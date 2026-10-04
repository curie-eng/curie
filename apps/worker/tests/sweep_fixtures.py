"""Shared helpers for the long scheduled sweep tests (ADR-0160, #2878).

Not a conftest: plain helpers a test calls, imported the way
``queue_fixtures.py`` is (a ``sys.path`` insert, because importlib import mode
does not add the test directory to ``sys.path``).

Only external services are faked here. ``FakeStateApi`` stands in for the API's
memory state routes; ``FakeTriggers`` stands in for the bundle store behind
``BundleTriggerSource``. Postgres and Valkey are always the real ones.

``curie_worker.sweep`` is imported lazily inside the helpers that need it, so a
test that uses them fails at its own call site (``ModuleNotFoundError`` before
the source lands) instead of the whole module failing to collect.
"""

from __future__ import annotations

import asyncio
import contextlib
import re
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import unquote

import httpx
from aci_protocol import (
    HookRunRef,
    OutboundEvent,
    QueuedTurn,
    ReplyHandle,
    SideEffectFlag,
    TextDelta,
    TurnSource,
)
from aci_protocol.service_config import STREAM_PAYLOAD_FIELD
from channel_protocol import hook_conversation_id
from channel_protocol.reply import ReplyUpdate
from curie_worker.delivery_lease import DeliveryBudget, DeliveryLease, DeliveryLeaseStore
from redis.exceptions import ResponseError
from sqlalchemy import text

API_BASE = "http://api.test"
PLATFORM_KEY = "platform-key-for-tests"
PROMPT = "sweep slack, github and notes; keep one sweep-checkpoint fact"
# ``make_hook_run`` seeds slot 2026-09-22T03:00Z. In America/New_York that slot
# is still the evening of 2026-09-21, which is the sweep date ADR-0160 keys a
# checkpoint by.
SWEEP_ZONE = "America/New_York"
SWEEP_DATE = "2026-09-21"
UTC_DATE = "2026-09-22"
NOTICE_PREFIX = "Scheduled sweep "


# --- the state API fake ---------------------------------------------------------


def _rfc3339(moment: datetime) -> str:
    """The wire form both the runner and the API use: RFC3339 UTC with ``Z``.

    ``runner/src/curie_runner/memory_facts.py::_now`` and
    ``apps/api/src/curie_api/routers/state.py::_stamp_author`` both write
    ``datetime.now(UTC).isoformat().replace("+00:00", "Z")``; pydantic renders
    ``StateEntryOut.updated_at`` the same way.
    """
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


def fact(
    statement: object,
    *,
    author: str,
    stated_at: datetime,
    updated_at: datetime | None = None,
    key: str | None = None,
    version: int = 1,
    session_id: str = "sess-sweep",
) -> dict[str, Any]:
    """One memory entry exactly as ``GET .../state/memory`` returns it.

    The envelope is ``StateEntryOut`` (``apps/api/src/curie_api/schemas/state.py``:
    ``namespace``, ``key``, ``value``, ``version``, ``updated_at``). The value is
    the runner's fact shape (``runner/src/curie_runner/memory_facts.py::_fact_value``:
    ``statement``, ``author``, ``stated_at``, ``session_id``) AFTER the API's
    ``_stamp_author`` (``apps/api/src/curie_api/routers/state.py``) replaced
    ``author`` with the per-turn credential's sender and ``stated_at`` with the
    server clock. A test passes the stamped values it wants the API to have
    produced, including wrong ones.
    """
    return {
        "namespace": "memory",
        "key": key if key is not None else f"fact-{uuid.uuid4().hex}",
        "value": {
            "statement": statement,
            "author": author,
            "stated_at": _rfc3339(stated_at),
            "session_id": session_id,
        },
        "version": version,
        "updated_at": _rfc3339(updated_at or stated_at),
    }


def checkpoint_text(
    *,
    hook: str,
    sweep_date: str = SWEEP_DATE,
    covered: Sequence[str] = ("slack",),
    uncovered: Sequence[str] = ("github", "notes"),
    sweep: str = "daily-plan",
) -> str:
    """The documented ``sweep-checkpoint`` statement, one key per line."""
    return (
        "sweep-checkpoint\n"
        f"sweep: {sweep}\n"
        f"date: {sweep_date}\n"
        f"hook: {hook}\n"
        f"covered: {', '.join(covered) if covered else 'none'}\n"
        f"uncovered: {', '.join(uncovered) if uncovered else 'none'}"
    )


_AGENT_PATH = re.compile(r"/agents/(?P<agent>[^/]+)/state/memory")
_CHANNEL_PATH = re.compile(
    r"/agents/(?P<agent>[^/]+)/state/bindings/(?P<kind>[^/]+)/(?P<address>[^/]+)/memory"
)

Failure = int | str  # an HTTP status, or "timeout", "non-json", "non-list", "redirect"


class FakeStateApi:
    """The API's two memory list routes, behind an ``httpx.MockTransport``.

    ``GET /agents/{agent_id}/state/memory`` and
    ``GET /agents/{agent_id}/state/bindings/{kind}/{address}/memory`` (both
    ``response_model=list[StateEntryOut]`` in ``routers/state.py``). Each scope
    can be told to fail on its own: a status code, a transport timeout, a
    non-JSON body, a JSON body that is not a list, or a redirect. Every request
    is recorded with its raw (still percent-encoded) path and headers.
    """

    def __init__(self, agent_id: uuid.UUID | str) -> None:
        self.agent_id = str(agent_id)
        self.agent_memory: list[dict[str, Any]] = []
        self.channel_memory: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self.agent_failure: Failure | None = None
        self.channel_failure: Failure | None = None
        self.requests: list[httpx.Request] = []
        self.raw_paths: list[str] = []
        # Awaited on every request before it is answered, so a test can act at
        # the exact moment the worker reads coverage (a fence bump, an order log).
        self.on_request: Callable[[httpx.Request], Awaitable[None]] | None = None

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=self.transport())

    def add_agent(self, entry: dict[str, Any]) -> None:
        self.agent_memory.append(entry)

    def add_channel(self, kind: str, address: str, entry: dict[str, Any]) -> None:
        self.channel_memory.setdefault((kind, address), []).append(entry)

    def clear(self) -> None:
        self.agent_memory.clear()
        self.channel_memory.clear()

    async def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        raw = request.url.raw_path.decode("ascii").split("?", 1)[0]
        self.raw_paths.append(raw)
        if self.on_request is not None:
            await self.on_request(request)
        channel = _CHANNEL_PATH.search(raw)
        agent = None if channel is not None else _AGENT_PATH.search(raw)
        if channel is not None:
            if channel["agent"] != self.agent_id:
                return httpx.Response(404, json={"detail": "agent not found"})
            failure = self.channel_failure
            key = (unquote(channel["kind"]), unquote(channel["address"]))
            entries = self.channel_memory.get(key, [])
        elif agent is not None:
            if agent["agent"] != self.agent_id:
                return httpx.Response(404, json={"detail": "agent not found"})
            failure = self.agent_failure
            entries = self.agent_memory
        else:
            return httpx.Response(404, json={"detail": "not found"})
        if failure is not None:
            return self._fail(request, failure)
        return httpx.Response(200, json=list(entries))

    @staticmethod
    def _fail(request: httpx.Request, failure: Failure) -> httpx.Response:
        if failure == "timeout":
            raise httpx.ReadTimeout("state API read timed out", request=request)
        if failure == "non-json":
            return httpx.Response(
                200, content=b"<html>gateway</html>", headers={"content-type": "text/html"}
            )
        if failure == "non-list":
            return httpx.Response(200, json={"items": [], "detail": "not a list"})
        if failure == "redirect":
            return httpx.Response(307, headers={"location": f"{API_BASE}/redirected/memory"})
        assert isinstance(failure, int)
        return httpx.Response(failure, json={"detail": "refused"})


class FakeTriggers:
    """A ``TriggerSource`` (``cron_loop.TriggerSource``) over canned triggers.

    Stands in for ``BundleTriggerSource``, whose real input is a stored bundle
    in object storage. Records every bundle ref it is asked about.
    """

    def __init__(self, triggers: list[dict[str, Any]] | None = None) -> None:
        self.by_ref: dict[str, list[dict[str, Any]]] = {}
        self.default = list(triggers or [])
        self.calls: list[str] = []
        self.raises: BaseException | None = None

    def triggers(self, bundle_ref: str) -> list[dict[str, Any]]:
        self.calls.append(bundle_ref)
        if self.raises is not None:
            raise self.raises
        return list(self.by_ref.get(bundle_ref, self.default))


def cron_trigger(name: str, *, timezone: str | None = SWEEP_ZONE) -> dict[str, Any]:
    trigger: dict[str, Any] = {
        "type": "cron",
        "name": name,
        "schedule": "0 23 * * *",
        "target": "C1",
        "prompt": PROMPT,
    }
    if timezone is not None:
        trigger["timezone"] = timezone
    return trigger


async def seed_bundle_ref(run: Any, bundle_ref: str | None = "bundles/sweep.tgz") -> None:
    async with run.engine.begin() as conn:
        await conn.execute(
            text("UPDATE curie.agent_versions SET bundle_ref = :ref WHERE id = :id"),
            {"ref": bundle_ref, "id": run.version_id},
        )


def coverage(
    run: Any,
    triggers: FakeTriggers,
    client: httpx.AsyncClient,
    **overrides: Any,
) -> Any:
    """A ``SweepCoverage`` on the seed's real engine, the fake API and triggers."""
    from curie_worker.sweep import SweepCoverage

    return SweepCoverage(
        engine=run.engine,
        db_schema="curie",
        trigger_source=triggers,
        client=client,
        api_base_url=API_BASE,
        api_key=PLATFORM_KEY,
        **overrides,
    )


def sweep_factory(
    run: Any, triggers: FakeTriggers, client: httpx.AsyncClient
) -> Callable[[Any, Any], Any]:
    """The ``make_harness(sweep_factory=...)`` value for one seeded run."""

    def factory(_redis: Any, _config: Any) -> Any:
        return coverage(run, triggers, client)

    return factory


# --- cron events, leases and the budget cut ------------------------------------


def cron_event(
    run: Any,
    *,
    event_id: str | None = None,
    targeted: bool = True,
    received_at: str = "2026-09-22T03:00:01+00:00",
) -> QueuedTurn:
    """A cron delivery shaped exactly as ``cron_loop._enqueue`` mints it."""
    ref: HookRunRef = run.ref
    return QueuedTurn(
        event_id=event_id or f"cron:{ref.agent_id}:{ref.name}:{ref.slot_utc}",
        conversation_id=hook_conversation_id(uuid.UUID(ref.agent_id), ref.name),
        author=f"cron:{ref.name}",
        text=PROMPT,
        source=TurnSource.CRON,
        reply_handle=(
            ReplyHandle(kind="slack", channel="C1", placeholder=None) if targeted else None
        ),
        received_at=received_at,
        hook_run=ref,
    )


@dataclass
class Delivery:
    """One stream entry this test's consumer read and holds a lease on."""

    event: QueuedTurn
    entry_id: str
    lease: DeliveryLease


async def ensure_group(h: Any) -> None:
    try:
        await h.async_redis.xgroup_create(
            h.config.stream, h.config.consumer_group, id="0", mkstream=True
        )
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def _first_entry(resp: Any) -> tuple[str, dict[str, str]] | None:
    if not resp:
        return None
    if isinstance(resp, dict):
        batches = list(resp.values())
        entries = batches[0][0] if batches and batches[0] else []
    else:
        entries = resp[0][1]
    if not entries:
        return None
    entry_id, fields = entries[0]
    return str(entry_id), dict(fields)


async def acquire(h: Any, entry_id: str) -> DeliveryLease:
    return await DeliveryLeaseStore(h.async_redis, h.config).acquire(
        h.config.stream,
        h.config.consumer_group,
        entry_id,
        consumer=h.config.consumer_name,
    )


async def read_next(h: Any) -> Delivery | None:
    """XREADGROUP the next entry and take the delivery lease on it."""
    await ensure_group(h)
    got = _first_entry(
        await h.async_redis.xreadgroup(
            h.config.consumer_group,
            h.config.consumer_name,
            {h.config.stream: ">"},
            count=1,
        )
    )
    if got is None:
        return None
    entry_id, fields = got
    event = QueuedTurn.model_validate_json(fields[STREAM_PAYLOAD_FIELD])
    return Delivery(event, entry_id, await acquire(h, entry_id))


async def enqueue(h: Any, event: QueuedTurn) -> Delivery:
    """Publish ``event`` on the runs stream the way the producers do, then read it."""
    await ensure_group(h)
    await h.async_redis.xadd(h.config.stream, {STREAM_PAYLOAD_FIELD: event.model_dump_json()})
    delivery = await read_next(h)
    assert delivery is not None
    assert delivery.event.event_id == event.event_id
    return delivery


async def redeliver(h: Any, delivery: Delivery) -> Delivery:
    """Release the old owner's lease and take the entry again (generation + 1)."""
    await DeliveryLeaseStore(h.async_redis, h.config).release(
        h.config.stream,
        h.config.consumer_group,
        delivery.entry_id,
        owner=delivery.lease.owner,
        resume_event_id=None,
    )
    lease = await acquire(h, delivery.entry_id)
    assert lease.generation > delivery.lease.generation
    return Delivery(delivery.event, delivery.entry_id, lease)


async def force_deadline(h: Any, delivery: Delivery, *, in_ms: int) -> None:
    """Move this delivery's ADR-0131 deadline to ``in_ms`` from the server clock."""
    seconds, microseconds = await h.async_redis.time()
    now_ms = int(seconds) * 1000 + int(microseconds) // 1000
    deadline_ms = now_ms + in_ms
    await h.async_redis.hset(
        h.config.delivery_state_key(h.config.stream, h.config.consumer_group, delivery.entry_id),
        mapping={"deadline_ms": str(deadline_ms)},
    )
    delivery.lease.budget = DeliveryBudget(
        deadline_ms=deadline_ms,
        anchor_server_ms=now_ms,
        anchor_monotonic=time.monotonic(),
    )


async def bump_fence(h: Any, delivery: Delivery) -> None:
    """Simulate a transfer: another owner took the delivery's fencing generation."""
    await h.async_redis.hincrby(
        h.config.delivery_state_key(h.config.stream, h.config.consumer_group, delivery.entry_id),
        "gen",
        1,
    )


def arm_started(kernel: Any) -> asyncio.Event:
    """Latch once the kernel applies the slice's ``started`` text delta.

    A cron turn posts no partial, so the sink cannot signal that the stream has
    begun (same latch as ``test_hook_runs._arm_started_frame``).
    """
    applied = asyncio.Event()
    original = kernel._apply_frame

    async def spy(
        frame: object, acc: object, reply: object, qevent: object, agent_id: object = None
    ) -> None:
        await original(frame, acc, reply, qevent, agent_id)
        if isinstance(frame, TextDelta) and frame.text == "started":
            applied.set()

    kernel._apply_frame = spy
    return applied


async def run_slice_to_budget_cut(
    h: Any,
    delivery: Delivery,
    *,
    at_cut: Callable[[], Awaitable[None]] | None = None,
    side_effect: bool = False,
    leading: Sequence[OutboundEvent] = (),
) -> None:
    """Drive one slice until the delivery budget cuts it.

    The slice streams ``started`` and then hangs. Once it is live, ``at_cut``
    runs (write the checkpoint, close the row, ...), and the delivery deadline
    is moved to 100 ms out. The client's 1 s per-request ceiling then expires,
    the worker POSTs ``/v1/timeout`` (FakeRunner answers ``timeout_status``),
    and the attempt ends ``runner-timeout`` with no budget left.
    """
    hold = asyncio.Event()
    h.runner.hold = hold
    h.runner.tail = []
    frames: list[OutboundEvent] = list(leading)
    if side_effect:
        frames.append(SideEffectFlag(tool="Bash", detail="curl the notes service"))
    frames.append(TextDelta(text="started"))
    h.runner.turn_scripts = [frames]
    started = arm_started(h.kernel)
    task = asyncio.create_task(h.kernel.process_event(delivery.event, lease=delivery.lease))
    try:
        await asyncio.wait_for(started.wait(), timeout=5.0)
        if at_cut is not None:
            await at_cut()
        await force_deadline(h, delivery, in_ms=100)
        await asyncio.wait_for(asyncio.shield(task), timeout=20.0)
    finally:
        hold.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    task.result()


async def stream_turns(h: Any) -> list[QueuedTurn]:
    out: list[QueuedTurn] = []
    for _entry_id, fields in await h.async_redis.xrange(h.config.stream):
        payload = fields.get(STREAM_PAYLOAD_FIELD)
        if payload is None:
            continue
        try:
            out.append(QueuedTurn.model_validate_json(payload))
        except ValueError:
            continue
    return out


async def successors(h: Any) -> list[QueuedTurn]:
    """Every continuation the kernel published on the runs stream."""
    return [turn for turn in await stream_turns(h) if ":sweep:" in turn.event_id]


def notices(h: Any) -> list[ReplyUpdate]:
    """Every coverage notice the sink received, in order."""
    return [
        event
        for event, _route, _best in h.sink.events
        if isinstance(event, ReplyUpdate)
        and event.message is None
        and (event.text or "").startswith(NOTICE_PREFIX)
    ]


def reply_texts(h: Any) -> list[str]:
    """Every reply.update text the sink received (no card edits), in order."""
    return [
        event.text or ""
        for event, _route, _best in h.sink.events
        if isinstance(event, ReplyUpdate) and event.message is None
    ]


async def run_outcome(run: Any) -> str | None:
    state = await run.state()
    assert state is not None
    return state[0]


async def set_outcome(run: Any, outcome: str) -> None:
    async with run.engine.begin() as conn:
        await conn.execute(
            text("UPDATE curie.hook_runs SET outcome = :outcome, ended_at = now() WHERE id = :id"),
            {"outcome": outcome, "id": run.run_id},
        )


async def pause_hook(run: Any) -> None:
    async with run.engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO curie.schedule_controls "
                "(agent_id, name, paused_at) VALUES (:agent_id, :name, now())"
            ),
            {"agent_id": run.agent_id, "name": run.ref.name},
        )


async def started_at(run: Any) -> datetime:
    async with run.engine.connect() as conn:
        value = (
            await conn.execute(
                text("SELECT started_at FROM curie.hook_runs WHERE id = :id"),
                {"id": run.run_id},
            )
        ).scalar_one()
    assert isinstance(value, datetime)
    return value


def now() -> datetime:
    return datetime.now(UTC)


# --- one seeded sweep: run row, fake API, triggers and a kernel harness ----------


@dataclass
class SweepCase:
    """Everything one sweep test drives: the real run row and harness, the fakes."""

    run: Any
    h: Any
    api: FakeStateApi
    triggers: FakeTriggers
    client: httpx.AsyncClient

    def checkpoint(
        self,
        *,
        covered: Sequence[str] = ("slack",),
        uncovered: Sequence[str] = ("github", "notes"),
        where: str = "agent",
    ) -> dict[str, Any]:
        """Save a checkpoint fact the way the runner memory tool plus the API would."""
        entry = fact(
            checkpoint_text(hook=self.run.ref.name, covered=covered, uncovered=uncovered),
            author=f"cron:{self.run.ref.name}",
            stated_at=now(),
        )
        if where == "agent":
            self.api.add_agent(entry)
        else:
            self.api.add_channel("slack", "C1", entry)
        return entry


@contextlib.asynccontextmanager
async def sweep_case(
    make_hook_run: Callable[..., Any],
    make_harness: Callable[..., Any],
    *,
    with_sweep: bool = True,
    production_factory: bool = False,
    wrap_recorder: Callable[[Any], Any] | None = None,
    **harness_overrides: Any,
) -> AsyncIterator[SweepCase]:
    """A seeded hook run with a zoned trigger, the fake API, and a kernel harness.

    The harness defaults to the budget-cut recipe: a 60 s delivery budget and a
    1 s per-request runner ceiling.
    """
    async with make_hook_run() as run:
        await seed_bundle_ref(run)
        api = FakeStateApi(run.agent_id)
        triggers = FakeTriggers([cron_trigger(run.ref.name)])
        async with api.client() as client:
            recorder = run.recorder()
            if wrap_recorder is not None:
                recorder = wrap_recorder(recorder)
            kwargs: dict[str, Any] = {
                "hook_runs": recorder,
                "delivery_budget_s": 60.0,
                "runner_total_timeout_s": 1.0,
            }
            if production_factory:

                def factory(_redis: Any, config: Any) -> Any:
                    from curie_worker import run as run_module

                    return run_module.build_sweep_coverage(
                        config, engine=run.engine, trigger_source=triggers, client=client
                    )

                kwargs["sweep_factory"] = factory
            elif with_sweep:
                kwargs["sweep_factory"] = sweep_factory(run, triggers, client)
            kwargs.update(harness_overrides)
            async with make_harness(**kwargs) as h:
                yield SweepCase(run=run, h=h, api=api, triggers=triggers, client=client)
