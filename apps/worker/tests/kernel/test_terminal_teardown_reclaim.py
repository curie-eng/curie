"""Teardown re-claims under the thread lock before it stops anything (#5377).

Plan R2: a termination claim on a terminal row leaves its runtime epoch alone,
so a claimant that stalled could otherwise record, interrupt or delete after a
peer finished the teardown and the owner resumed on a fresh runtime. Both
kernel teardown paths (the terminate wake and the owner's heartbeat stop)
therefore re-claim while holding the thread lock, immediately before the
interrupt and ``terminate_thread``; a refusal stops nothing, and the record
names the re-claim's epoch and request version.

Boundary: the real kernel, the real Valkey ``ThreadLock``, the real
``SandboxSubstrate`` over the suite's fake Kubernetes ``SandboxClient`` (per
apps/worker/CLAUDE.md), the real fake runner for interrupts, and the real
``WorkItemDispatchClient`` over httpx. Only the API is a double, as in
test_work_item_settling.py: ``TeardownApi`` serves the termination routes'
contract (owner lease, epoch, request-version compare and set). The API's own
refusals are proved against Postgres by the API suite.

The owner's stop is also driven end to end through ``Kernel.process_event``.
The live owner's re-claim keeps the epoch, so the turn it interrupts
still finishes as itself, ``not_running``, and settles its delivery.

The thread lock's lease is fenced after the re-claim and after the interrupt.
A lease that expired while either was in flight, with a peer's real
``ThreadLock`` holding the key since, stops the teardown before anything more is
interrupted, deleted or recorded, and the peer's lock is left as it is.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from aci_protocol import Final, SessionStatus
from curie_worker.binding import FactoryWake
from curie_worker.kernel import routing
from curie_worker.threadlock import ThreadLock
from curie_worker.workitem_dispatch import (
    WorkItemAcquireGrant,
    WorkItemDispatchClient,
    WorkItemRun,
)

from apps.worker.tests.queue_fixtures import factory_context

from .test_work_item_early_stop import _Binding as _ExecuteBinding
from .test_work_item_early_stop import _PublicationApi
from .test_work_item_early_stop import _Workspace as _ExecuteWorkspace
from .test_work_item_settling import Api, ApiClock, _event, _until
from .test_work_item_workspace import (
    AGENT_ID,
    CHANNEL,
    _Binding,
    _NoExistingPublication,
    _TerminationLease,
    _turn,
    _Workspace,
)

OWNER = "test-consumer"  # the harness kernel's consumer name
PEER = "worker-b"
_TERMINAL = ("completed", "failed", "expired")


@dataclass(kw_only=True)
class _Row(_TerminationLease):
    status: str
    teardown_pending: bool
    claim_name: str | None
    sandbox_name: str | None
    terminal_cause: str | None = None
    observation: str | None = None


@dataclass
class TeardownApi:
    """The worker-token termination routes, as the #5377 contract defines them.

    Claim: a ``cancellation_requested`` row, or a terminal row with teardown
    pending, is claimable when no other owner holds a live lease, and the claim
    renews the lease under ``_TerminationLease``'s rule (the API's
    ``claim_termination``): on a ``cancellation_requested`` row only a takeover
    advances the epoch and the version never moves; on a terminal row only the
    version advances. Record: the epoch and the expected request version must
    both be current and the observation nonblank; it advances the version, and
    a terminal row's confirmation also clears its owner.
    """

    request_id: uuid.UUID
    row: _Row
    redis: Any
    lock_key: str
    runner: Any
    claims: list[dict[str, Any]] = field(default_factory=list)
    records: list[dict[str, Any]] = field(default_factory=list)
    read_gate: asyncio.Event | None = None
    read_entered: asyncio.Event = field(default_factory=asyncio.Event)
    # Awaited while a claim made under the thread lock is being answered, as a
    # delayed response would be.
    on_locked_claim: Callable[[], Awaitable[None]] | None = None

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        base = f"/v1/internal/work-items/requests/{self.request_id}"
        path = request.url.path
        if request.method == "GET" and path == base:
            self.read_entered.set()
            if self.read_gate is not None:
                await self.read_gate.wait()
            return httpx.Response(
                200,
                json={
                    "status": self.row.status,
                    "runtime_epoch": self.row.runtime_epoch,
                    "runtime_claim_name": self.row.claim_name,
                    "runtime_sandbox_name": self.row.sandbox_name,
                },
            )
        body = json.loads(request.content)
        if path == f"{base}/termination/claim":
            return await self._claim(body["owner"])
        if path == f"{base}/termination":
            return self._record(body)
        if path == f"{base}/start":
            return httpx.Response(
                200,
                json={
                    "runtime_epoch": self.row.runtime_epoch,
                    "execution_deadline": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
                    "remaining_s": 3600,
                    "heartbeat_interval_s": 3600,
                },
            )
        raise AssertionError(f"unexpected API request {request.method} {path}")

    async def _claim(self, owner: str) -> httpx.Response:
        row = self.row
        seen = {
            "owner": owner,
            "lock_held": bool(await self.redis.exists(self.lock_key)),
            "interrupts_before": self.runner.interrupts,
        }
        if seen["lock_held"] and self.on_locked_claim is not None:
            await self.on_locked_claim()
        eligible = row.status == "cancellation_requested" or (
            row.status in _TERMINAL and row.teardown_pending
        )
        if not eligible:
            self.claims.append({**seen, "outcome": "not_running"})
            return httpx.Response(409, json={"detail": {"code": "not_running"}})
        if not row.claim(owner, terminal=row.status in _TERMINAL):
            self.claims.append({**seen, "outcome": "duplicate"})
            return httpx.Response(409, json={"detail": {"code": "duplicate"}})
        self.claims.append(
            {**seen, "outcome": "claimed", "epoch": row.runtime_epoch, "version": row.version}
        )
        return httpx.Response(
            200, json={"runtime_epoch": row.runtime_epoch, "request_version": row.version}
        )

    def _record(self, body: dict[str, Any]) -> httpx.Response:
        row = self.row
        if body["runtime_epoch"] != row.runtime_epoch:
            return httpx.Response(409, json={"detail": {"code": "stale_owner"}})
        if body["expected_request_version"] != row.version:
            return httpx.Response(409, json={"detail": {"code": "stale_version"}})
        if not body["observation"].strip():
            return httpx.Response(
                409, json={"detail": {"code": "termination_observation_required"}}
            )
        if row.status == "cancellation_requested":
            # The stop's record keeps the owner and its lease, as the API does.
            row.status = "cancelled"
        else:
            row.runtime_owner = None
        row.teardown_pending = False
        row.version += 1
        row.observation = body["observation"]
        self.records.append(body)
        return httpx.Response(200, json={})


def _harness(make_harness: Any) -> Any:
    return make_harness(
        binding=_Binding(),
        workspace_factory=_Workspace,
        publication_creator=_NoExistingPublication(),
    )


def _client(http: httpx.AsyncClient) -> WorkItemDispatchClient:
    return WorkItemDispatchClient(
        api_base_url="http://api.example", worker_token="example-worker-token", client=http
    )


async def _claim_thread(h: Any, thread_key: str) -> Any:
    """A runtime on the thread, claimed under its lock as a delivery claims it."""

    async with h.kernel._lock.hold(h.config.lock_key(thread_key)):
        return await asyncio.to_thread(h.substrate.claim, thread_key)


def test_delayed_duplicate_teardown_never_touches_a_resumed_successor(make_harness) -> None:
    """Claimant A stalls; B tears down and records; a successor runs; A is refused."""

    async def go() -> None:
        async with _harness(make_harness) as h:
            request_id = uuid.uuid4()
            terminate = _turn(f"work-item-{request_id}-terminate", "terminate")
            thread_key = routing._thread_key_for(terminate)
            old = await _claim_thread(h, thread_key)
            # A normally finished personal request whose generation was fenced:
            # terminal, teardown pending, so no claim moves its epoch.
            api = TeardownApi(
                request_id,
                _Row(
                    status="completed",
                    runtime_epoch=3,
                    version=5,
                    teardown_pending=True,
                    claim_name=old.claim_name,
                    sandbox_name=old.sandbox_name,
                ),
                h.async_redis,
                h.config.lock_key(thread_key),
                h.runner,
            )
            api.read_gate = asyncio.Event()
            async with httpx.AsyncClient(transport=httpx.MockTransport(api)) as http:
                h.kernel._work_items = _client(http)
                # A claims, then stalls reading the request's stored names.
                stalled = asyncio.create_task(h.kernel.process_event(terminate))
                await asyncio.wait_for(api.read_entered.wait(), timeout=5)
                assert [c["outcome"] for c in api.claims] == ["claimed"]

                # A's lease lapses on the API's clock; B takes the teardown over,
                # under the thread lock, as B's kernel would.
                api.row.lease_live = False
                peer = _client(http)
                taken = await peer.claim_termination(request_id, owner=PEER)
                assert taken.runtime_epoch == 3
                async with h.kernel._lock.hold(h.config.lock_key(thread_key)):
                    gone = await asyncio.to_thread(
                        h.substrate.terminate_thread,
                        thread_key,
                        claim_name=old.claim_name,
                        sandbox_name=old.sandbox_name,
                        observer=PEER,
                    )
                assert gone is not None
                await peer.record_termination(
                    request_id,
                    runtime_epoch=taken.runtime_epoch,
                    expected_request_version=taken.request_version,
                    observation=gone.render(),
                )
                assert api.row.teardown_pending is False

                # The owner revalidated and resumed: a fresh runtime now owns the thread.
                successor = await _claim_thread(h, thread_key)
                assert successor.claim_name != old.claim_name
                interrupts = h.runner.interrupts

                api.read_gate.set()
                await asyncio.wait_for(stalled, timeout=10)

            # A's re-claim, under the thread lock, was refused, so A stopped nothing.
            assert [(c["owner"], c["outcome"]) for c in api.claims] == [
                (OWNER, "claimed"),
                (PEER, "claimed"),
                (OWNER, "not_running"),
            ]
            assert api.claims[-1]["lock_held"] is True
            assert h.runner.interrupts == interrupts
            assert successor.claim_name in h.fake_k8s.claims
            assert successor.claim_name not in h.fake_k8s.deleted_claims
            assert await asyncio.to_thread(h.substrate.lookup, thread_key) == successor
            assert [r["expected_request_version"] for r in api.records] == [taken.request_version]

    asyncio.run(go())


@pytest.mark.parametrize(
    ("status", "recorded_epoch", "recorded_version"),
    # A cancellation's gating claim takes the ownerless runtime over (epoch 4)
    # and the re-claim renews that lease; a terminal row's claims move only the
    # version.
    [("cancellation_requested", 4, 5), ("failed", 3, 7)],
    ids=["cancellation", "terminal"],
)
def test_terminate_records_with_the_epoch_and_version_of_its_reclaim(
    make_harness, status: str, recorded_epoch: int, recorded_version: int
) -> None:
    async def go() -> None:
        async with _harness(make_harness) as h:
            request_id = uuid.uuid4()
            terminate = _turn(f"work-item-{request_id}-terminate", "terminate")
            thread_key = routing._thread_key_for(terminate)
            old = await _claim_thread(h, thread_key)
            api = TeardownApi(
                request_id,
                _Row(
                    status=status,
                    runtime_epoch=3,
                    version=5,
                    teardown_pending=status != "cancellation_requested",
                    claim_name=old.claim_name,
                    sandbox_name=old.sandbox_name,
                ),
                h.async_redis,
                h.config.lock_key(thread_key),
                h.runner,
            )
            async with httpx.AsyncClient(transport=httpx.MockTransport(api)) as http:
                h.kernel._work_items = _client(http)
                await h.kernel.process_event(terminate)

            # The gating claim runs unlocked; the re-claim holds the thread lock
            # and precedes the interrupt.
            assert [(c["outcome"], c["lock_held"]) for c in api.claims] == [
                ("claimed", False),
                ("claimed", True),
            ]
            assert [c["interrupts_before"] for c in api.claims] == [0, 0]
            assert h.runner.interrupts == 1
            [record] = api.records
            assert record["runtime_epoch"] == recorded_epoch == api.claims[-1]["epoch"]
            assert (
                record["expected_request_version"] == api.claims[-1]["version"] == recorded_version
            )
            assert old.claim_name in record["observation"]
            assert old.claim_name not in h.fake_k8s.claims
            assert h.fake_k8s.deleted_claims == [old.claim_name]

    asyncio.run(go())


async def _owned_run(h: Any, api: TeardownApi, http: httpx.AsyncClient, thread_key: str) -> Any:
    handle = await _claim_thread(h, thread_key)
    api.row.claim_name = handle.claim_name
    api.row.sandbox_name = handle.sandbox_name
    wake = FactoryWake(
        kind="slack", address=CHANNEL, adapter=None, conversation_id="1700000000.000001"
    )
    run = WorkItemRun(
        client=_client(http),
        request_id=api.request_id,
        owner=OWNER,
        grant=WorkItemAcquireGrant(
            generation=1,
            work_item_id=uuid.uuid4(),
            conversation_id="slack:example",
            wait_deadline=datetime.now(UTC) + timedelta(hours=1),
            repository_path="acme-corp/widgets",
            context=factory_context(AGENT_ID, wake),
        ),
        wake=wake,
        event_id=f"work-item-{api.request_id}-execute-1",
        thread_key=thread_key,
        on_stop=h.kernel._stop_owned_work_item,
        on_stale=h.kernel._abandon_stale_work_item,
    )
    await run.start(claim_name=handle.claim_name, sandbox_name=handle.sandbox_name)
    return run, handle


def test_owner_stop_whose_reclaim_is_refused_neither_interrupts_nor_deletes(
    make_harness,
) -> None:
    """The heartbeat saw cancellation, but a peer holds the teardown lease."""

    async def go() -> None:
        async with _harness(make_harness) as h:
            thread_key = f"teardown-{uuid.uuid4().hex}"
            api = TeardownApi(
                uuid.uuid4(),
                _Row(
                    status="cancellation_requested",
                    runtime_epoch=3,
                    version=5,
                    teardown_pending=False,
                    claim_name=None,
                    sandbox_name=None,
                    runtime_owner=PEER,
                    lease_live=True,
                ),
                h.async_redis,
                h.config.lock_key(thread_key),
                h.runner,
            )
            async with httpx.AsyncClient(transport=httpx.MockTransport(api)) as http:
                h.kernel._work_items = _client(http)
                run, handle = await _owned_run(h, api, http, thread_key)
                try:
                    await h.kernel._stop_owned_work_item(thread_key, run)
                finally:
                    await run.close()

            assert [(c["owner"], c["outcome"], c["lock_held"]) for c in api.claims] == [
                (OWNER, "duplicate", True)
            ]
            assert h.runner.interrupts == 0
            assert handle.claim_name in h.fake_k8s.claims
            assert api.records == []
            assert run.finished is False

    asyncio.run(go())


def test_owner_stop_records_with_the_epoch_and_version_of_its_reclaim(make_harness) -> None:
    async def go() -> None:
        async with _harness(make_harness) as h:
            thread_key = f"teardown-{uuid.uuid4().hex}"
            api = TeardownApi(
                uuid.uuid4(),
                _Row(
                    status="cancellation_requested",
                    runtime_epoch=3,
                    version=5,
                    teardown_pending=False,
                    claim_name=None,
                    sandbox_name=None,
                    runtime_owner=OWNER,
                    lease_live=True,
                ),
                h.async_redis,
                h.config.lock_key(thread_key),
                h.runner,
            )
            async with httpx.AsyncClient(transport=httpx.MockTransport(api)) as http:
                h.kernel._work_items = _client(http)
                run, handle = await _owned_run(h, api, http, thread_key)
                try:
                    await h.kernel._stop_owned_work_item(thread_key, run)
                finally:
                    await run.close()

            assert [(c["owner"], c["outcome"], c["lock_held"]) for c in api.claims] == [
                (OWNER, "claimed", True)
            ]
            assert api.claims[0]["interrupts_before"] == 0
            assert h.runner.interrupts == 1
            # The live owner's re-claim renews its lease: the epoch its turn
            # started with and the version both stay.
            [record] = api.records
            assert record["runtime_epoch"] == run.runtime_epoch == 3
            assert record["expected_request_version"] == 5
            assert handle.claim_name not in h.fake_k8s.claims
            assert h.fake_k8s.deleted_claims == [handle.claim_name]
            assert api.row.status == "cancelled"
            assert run.finished is True

    asyncio.run(go())


# --- a lease lost mid-teardown stops it ----------------------------------------


async def _peer_takes_the_lock(h: Any, lock_key: str) -> str:
    """This worker's lease expires and a peer worker's real ``ThreadLock`` acquires the key."""

    await h.async_redis.delete(lock_key)  # the expiry, without waiting out the TTL
    peer = ThreadLock(
        h.async_redis,
        ttl_ms=h.config.lock_ttl_ms,
        acquire_timeout_s=1.0,
        poll_interval_s=0.01,
        owner=PEER,
    )
    return await peer.acquire(lock_key)


@dataclass
class _Teardown:
    """What ``_halt_work_item_runtime`` returned, and every ``terminate_thread`` call."""

    halts: list[object] = field(default_factory=list)
    terminated: list[str] = field(default_factory=list)


def _observe_teardown(h: Any, monkeypatch: pytest.MonkeyPatch) -> _Teardown:
    seen = _Teardown()
    halt = h.kernel._halt_work_item_runtime
    terminate = h.substrate.terminate_thread

    async def observed_halt(*args: Any, **kwargs: Any) -> Any:
        try:
            result = await halt(*args, **kwargs)
        except Exception as exc:
            seen.halts.append(exc)
            raise
        seen.halts.append(result)
        return result

    def observed_terminate(thread_key: str, **kwargs: Any) -> Any:
        seen.terminated.append(thread_key)
        return terminate(thread_key, **kwargs)

    monkeypatch.setattr(h.kernel, "_halt_work_item_runtime", observed_halt)
    monkeypatch.setattr(h.substrate, "terminate_thread", observed_terminate)
    return seen


async def _raised(teardown: Awaitable[object]) -> Exception | None:
    """What a teardown entry point raised; callers assert it after its effects."""

    try:
        await teardown
    except Exception as exc:  # noqa: BLE001 - the caller asserts it is None
        return exc
    return None


def _terminal_api(h: Any, request_id: uuid.UUID, lock_key: str, old: Any) -> TeardownApi:
    return TeardownApi(
        request_id,
        _Row(
            status="failed",
            runtime_epoch=3,
            version=5,
            teardown_pending=True,
            claim_name=old.claim_name,
            sandbox_name=old.sandbox_name,
        ),
        h.async_redis,
        lock_key,
        h.runner,
    )


def test_terminate_whose_lock_is_lost_during_its_reclaim_stops_nothing(
    make_harness: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The re-claim is granted only after the lease expired and a peer took the lock."""

    async def go() -> None:
        async with _harness(make_harness) as h:
            request_id = uuid.uuid4()
            terminate = _turn(f"work-item-{request_id}-terminate", "terminate")
            thread_key = routing._thread_key_for(terminate)
            lock_key = h.config.lock_key(thread_key)
            old = await _claim_thread(h, thread_key)
            api = _terminal_api(h, request_id, lock_key, old)
            peer_tokens: list[str] = []

            async def lose_the_lock() -> None:
                peer_tokens.append(await _peer_takes_the_lock(h, lock_key))

            api.on_locked_claim = lose_the_lock
            seen = _observe_teardown(h, monkeypatch)
            async with httpx.AsyncClient(transport=httpx.MockTransport(api)) as http:
                h.kernel._work_items = _client(http)
                raised = await _raised(h.kernel.process_event(terminate))

            assert [(c["outcome"], c["lock_held"]) for c in api.claims] == [
                ("claimed", False),
                ("claimed", True),
            ]
            assert h.runner.interrupts == 0
            assert seen.terminated == []
            assert old.claim_name in h.fake_k8s.claims
            assert old.claim_name not in h.fake_k8s.deleted_claims
            assert api.records == []
            assert seen.halts == [None]
            [peer_token] = peer_tokens
            assert await h.async_redis.get(lock_key) == peer_token
            assert raised is None

    asyncio.run(go())


def test_owner_stop_whose_lock_is_lost_during_its_reclaim_stops_nothing(
    make_harness: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The heartbeat's stop: the same loss during its only re-claim."""

    async def go() -> None:
        async with _harness(make_harness) as h:
            thread_key = f"teardown-{uuid.uuid4().hex}"
            lock_key = h.config.lock_key(thread_key)
            api = TeardownApi(
                uuid.uuid4(),
                _Row(
                    status="cancellation_requested",
                    runtime_epoch=3,
                    version=5,
                    teardown_pending=False,
                    claim_name=None,
                    sandbox_name=None,
                    runtime_owner=OWNER,
                    lease_live=True,
                ),
                h.async_redis,
                lock_key,
                h.runner,
            )
            peer_tokens: list[str] = []

            async def lose_the_lock() -> None:
                peer_tokens.append(await _peer_takes_the_lock(h, lock_key))

            api.on_locked_claim = lose_the_lock
            seen = _observe_teardown(h, monkeypatch)
            async with httpx.AsyncClient(transport=httpx.MockTransport(api)) as http:
                h.kernel._work_items = _client(http)
                run, handle = await _owned_run(h, api, http, thread_key)
                try:
                    raised = await _raised(h.kernel._stop_owned_work_item(thread_key, run))
                finally:
                    await run.close()

            assert [(c["owner"], c["outcome"], c["lock_held"]) for c in api.claims] == [
                (OWNER, "claimed", True)
            ]
            assert h.runner.interrupts == 0
            assert seen.terminated == []
            assert handle.claim_name in h.fake_k8s.claims
            assert handle.claim_name not in h.fake_k8s.deleted_claims
            assert api.records == []
            assert run.finished is False
            assert seen.halts == [None]
            [peer_token] = peer_tokens
            assert await h.async_redis.get(lock_key) == peer_token
            assert raised is None

    asyncio.run(go())


def test_terminate_whose_lock_is_lost_during_its_interrupt_deletes_and_records_nothing(
    make_harness: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The lease is held through the re-claim and lost while the interrupt runs."""

    async def go() -> None:
        async with _harness(make_harness) as h:
            request_id = uuid.uuid4()
            terminate = _turn(f"work-item-{request_id}-terminate", "terminate")
            thread_key = routing._thread_key_for(terminate)
            lock_key = h.config.lock_key(thread_key)
            old = await _claim_thread(h, thread_key)
            api = _terminal_api(h, request_id, lock_key, old)
            peer_tokens: list[str] = []
            interrupt = h.kernel.interrupt_thread

            async def interrupt_and_lose_the_lock(
                interrupted: str, reason: str, **kwargs: Any
            ) -> bool:
                signalled = await interrupt(interrupted, reason, **kwargs)
                peer_tokens.append(await _peer_takes_the_lock(h, lock_key))
                return bool(signalled)

            monkeypatch.setattr(h.kernel, "interrupt_thread", interrupt_and_lose_the_lock)
            seen = _observe_teardown(h, monkeypatch)
            async with httpx.AsyncClient(transport=httpx.MockTransport(api)) as http:
                h.kernel._work_items = _client(http)
                raised = await _raised(h.kernel.process_event(terminate))

            assert [(c["outcome"], c["lock_held"]) for c in api.claims] == [
                ("claimed", False),
                ("claimed", True),
            ]
            # The interrupt went out while the lease was still held.
            assert h.runner.interrupts == 1
            assert seen.terminated == []
            assert old.claim_name in h.fake_k8s.claims
            assert old.claim_name not in h.fake_k8s.deleted_claims
            assert api.records == []
            assert seen.halts == [None]
            [peer_token] = peer_tokens
            assert await h.async_redis.get(lock_key) == peer_token
            assert raised is None

    asyncio.run(go())


# --- the owner's stop settles the turn it interrupts ----------------------------


class _OwnerStopApi:
    """The settling suite's dispatch ``Api`` with one request row behind the stop.

    Acquire, start and the issue read are ``Api``'s; start makes the starting
    owner the row's live lease holder. Heartbeat, finish, the request read and
    the termination routes read and write ``teardown.row`` in the order the API
    checks them (apps/api/src/curie_api/workitem_dispatch.py): a heartbeat
    presenting another epoch is ``stale_owner``; a finish presenting another
    epoch, or after the lease lapsed, is ``stale_owner`` before a request that
    is no longer running answers ``not_running`` (``_map_finish_conflict``).
    A record waits until the turn's own finish has been answered, the order a
    real stop takes while ``terminate_thread`` polls for absence.
    """

    def __init__(self, request_id: uuid.UUID, clock: ApiClock, teardown: TeardownApi) -> None:
        self.request_id = request_id
        self.dispatch = Api(request_id, clock)
        self.teardown = teardown
        self.finish_answers: list[tuple[int, str]] = []
        self.finish_answered = asyncio.Event()

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        base = f"/v1/internal/work-items/requests/{self.request_id}"
        path = request.url.path
        row = self.teardown.row
        if path == f"{base}/termination":
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.finish_answered.wait(), timeout=5)
        if request.method == "GET" or path.startswith(f"{base}/termination"):
            return await self.teardown(request)
        if path == f"{base}/heartbeat":
            if json.loads(request.content)["runtime_epoch"] != row.runtime_epoch:
                return httpx.Response(409, json={"detail": {"code": "stale_owner"}})
            return httpx.Response(
                200,
                json={
                    "status": row.status,
                    "terminal_cause": row.terminal_cause,
                    "work_item_cancelled": False,
                },
            )
        if path == f"{base}/finish":
            return self._finish(json.loads(request.content))
        response = self.dispatch(request)
        if path == f"{base}/start":
            assert self.dispatch.start_grant is not None
            row.status = "running"
            row.runtime_epoch = self.dispatch.start_grant["runtime_epoch"]
            row.runtime_owner = self.dispatch.owner
            row.lease_live = True
        return response

    def _finish(self, body: dict[str, Any]) -> httpx.Response:
        row = self.teardown.row
        code: str | None = None
        if body["runtime_epoch"] != row.runtime_epoch or not row.lease_live:
            code = "stale_owner"
        elif row.status == "cancellation_requested" and row.terminal_cause in (
            "issue_cancelled",
            "issue_deleted",
        ):
            code = "work_item_cancelled"
        elif row.status != "running":
            code = "not_running"
        self.finish_answers.append((body["runtime_epoch"], code or "finished"))
        self.finish_answered.set()
        if code is not None:
            return httpx.Response(409, json={"detail": {"code": code}})
        row.status = body["outcome"]
        return httpx.Response(200, json={})


def test_owner_stop_lets_the_interrupted_turn_finish_and_settle_its_delivery(
    make_harness, monkeypatch
) -> None:
    """The heartbeat sees the fence; the turn it interrupts still finishes as itself.

    Driven through ``Kernel.process_event``: the execute turn starts, the runner
    holds it open and the run heartbeats every 10 ms. The API then fences the
    request to ``cancellation_requested`` with the owner's lease still live.
    The heartbeat's stop re-claims under the thread lock and interrupts the
    turn, whose own finish presents the epoch it started with. The live owner's
    re-claim kept that epoch, so the finish is answered ``not_running`` and the
    delivery writes its done marker and ``turn.completed`` and releases the
    finished run's sandbox. Were the re-claim to advance the epoch, the finish
    would be ``stale_owner`` and the delivery would end with no marker.
    """

    from curie_worker import api_retry
    from curie_worker import workitem_dispatch as dispatch_module

    async def go() -> None:
        clock = ApiClock()
        request_id = uuid.uuid4()
        event = _event(request_id)
        thread_key = routing._thread_key_for(event)
        monkeypatch.setattr(dispatch_module, "_MIN_HEARTBEAT_INTERVAL_S", 0.01)
        monkeypatch.setattr(api_retry, "_clock", clock)
        monkeypatch.setattr(api_retry, "_sleep", clock.sleep)
        async with make_harness(
            binding=_ExecuteBinding(),
            workspace_factory=_ExecuteWorkspace,
            publication_creator=_PublicationApi(),
        ) as h:
            teardown = TeardownApi(
                request_id,
                _Row(
                    status="waiting",
                    runtime_epoch=0,
                    version=5,
                    teardown_pending=False,
                    claim_name=None,
                    sandbox_name=None,
                ),
                h.async_redis,
                h.config.lock_key(thread_key),
                h.runner,
            )
            api = _OwnerStopApi(request_id, clock, teardown)
            api.dispatch.kernel = h.kernel
            api.dispatch.heartbeat_interval_s = 0.01
            release = h.kernel._release_work_item_sandbox
            releases: list[str] = []

            async def observed_release(run: Any) -> None:
                releases.append(run.thread_key)
                await release(run)

            monkeypatch.setattr(h.kernel, "_release_work_item_sandbox", observed_release)
            h.runner.hold = asyncio.Event()
            h.runner.default_script = []
            # What the runner streams once its turn is interrupted (curie_runner.session).
            h.runner.tail = [
                Final(text="run interrupted", status=SessionStatus.IDLE_AWAITING_INPUT)
            ]
            async with httpx.AsyncClient(transport=httpx.MockTransport(api)) as http:
                h.kernel._work_items = _client(http)
                turn = asyncio.create_task(h.kernel.process_event(event))
                await _until(
                    lambda: (
                        h.runner.turn_active
                        and api.dispatch.run is not None
                        and api.dispatch.run.heartbeat_running
                    )
                )
                run = api.dispatch.run
                start_epoch = run.runtime_epoch
                own_claim = run.claim_name
                assert run.thread_key == thread_key
                assert (teardown.row.runtime_owner, teardown.row.runtime_epoch) == (
                    OWNER,
                    start_epoch,
                )

                # The enrollment fence stops the request; the owner's lease stays live.
                teardown.row.status = "cancellation_requested"
                teardown.row.terminal_cause = "enrollment_fenced"
                await asyncio.wait_for(turn, timeout=10)

            # The interrupted turn's own finish named its start epoch and was
            # answered as a stopped run, not a stale owner.
            assert api.finish_answers == [(start_epoch, "not_running")]
            # So the delivery settled: the done marker, turn.completed, a
            # finished run and its sandbox released.
            assert await h.kernel._markers.is_terminal(event.event_id)
            assert [completion.outcome for completion in h.sink.completions] == ["delivered"]
            assert run.finished
            assert releases == [thread_key]
            assert own_claim not in h.fake_k8s.claims
            assert h.substrate._affinity.get(thread_key) is None
            assert not h.kernel.owns_work_item(request_id)
            # The stop re-claimed under the thread lock before its one interrupt,
            # and the live owner's re-claim kept the epoch.
            assert [
                (c["owner"], c["outcome"], c["lock_held"], c["interrupts_before"])
                for c in teardown.claims
            ] == [(OWNER, "claimed", True, 0)]
            assert h.runner.interrupts == 1
            assert teardown.row.runtime_epoch == start_epoch
            # The record named the re-claim's epoch and version.
            [record] = teardown.records
            assert (record["runtime_epoch"], record["expected_request_version"]) == (
                start_epoch,
                5,
            )
            assert own_claim is not None and own_claim in record["observation"]

    asyncio.run(go())
