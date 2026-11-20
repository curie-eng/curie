"""Real API, SQL, stream, reconciler and runner wiring for factory retries (#5424).

One case owns an API under uvicorn, a runs stream with the real consumer on it,
a reconciler the test drives by hand, and a scripted NDJSON runner. Wakes reach
the kernel only through a reconciler pass and consumer delivery. The model, the
code host, the tracker surface, the Kubernetes control plane and the object
store are the only doubles.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import socket
import uuid
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg
import boto3
import httpx
import pytest
import redis
import redis.asyncio as aredis
from aci_protocol import (
    PROTOCOL_VERSION,
    STREAM_PAYLOAD_FIELD,
    ErrorEvent,
    Final,
    QueuedTurn,
    ReplyHandle,
    SessionStatus,
    TurnSource,
)
from aiohttp import web
from aiohttp.test_utils import TestServer
from alembic import command
from alembic.config import Config
from botocore.config import Config as S3Config
from botocore.exceptions import ClientError
from channel_protocol.work_item_events import CI_ROUND_KEY_PREFIX, ci_event_id, execute_event_id
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock
from curie_api import models
from curie_api.config import get_settings
from curie_api.workitem_reconciler import WorkItemReconciler
from curie_internal.provider_scope import ProviderScope
from curie_internal.turn_queue import to_stream_fields
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.fake import FakeModelSession
from curie_runner.issue_read import resolve_issue_read
from curie_runner.memory_rules_context import MemoryRulesContextGate
from curie_runner.progress import PROGRESS_TOKEN_ENV, PROGRESS_URL_ENV
from curie_runner.requester_context import RequesterContextGate
from curie_runner.server import create_app as create_runner_app
from curie_runner.session import SessionRunner
from curie_runner.tracker_result import bind_completion_contract
from curie_runner.verification import preflight_workspace_verification
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from curie_worker.binding import FactoryWake
from curie_worker.consumer import Consumer, refuse_unconfigured_reminder
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.sandbox.types import DeliveredLimits
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import async_sessionmaker

from apps.worker.tests.kernel.conftest import FakeRunner
from apps.worker.tests.queue_fixtures import wait_until
from apps.worker.tests.start_wait_fixtures import (
    ADDRESS,
    OPERATOR_KEY,
    REPOSITORY,
    ROOT,
    WORKER_TOKEN,
    StartWaitCase,
)

UNOBSERVED_LIMITS = DeliveredLimits(
    memory_bytes=None, ephemeral_storage_bytes=None, scratch_bytes={}, max_turns=None
).to_wire()


VECTORS_PATH = ROOT / "tests/vectors/factory-retry-dispositions.json"
REQUESTER = "U0REQUEST1"
WAIT_S = 20.0
_REQUEST_ROUTES = "/v1/internal/work-items/requests/"
_FINISH_FACTS = ("cause", "retry_safety", "retry_condition")


def load_vectors() -> dict[str, Any]:
    """The shared retry vectors; a file with nothing for the worker is an error."""

    vectors: dict[str, Any] = json.loads(VECTORS_PATH.read_text())
    if not [case for case in vectors["cases"] if case.get("worker")]:
        raise RuntimeError(f"{VECTORS_PATH} carries no worker cases")
    if not vectors["malformed_hints"]:
        raise RuntimeError(f"{VECTORS_PATH} carries no malformed hints")
    return vectors


@dataclass(frozen=True)
class Call:
    """One worker POST to a request route and the API's answer to it."""

    route: str
    request_id: uuid.UUID
    body: dict[str, Any]
    status: int
    code: str | None

    @property
    def facts(self) -> dict[str, Any]:
        """The cause, safety fact and carrier a finish reported."""

        return {key: self.body[key] for key in _FINISH_FACTS}


@dataclass(frozen=True)
class Item:
    """One admitted WorkItem and the conversation its wakes arrive on."""

    request_id: uuid.UUID
    work_item_id: uuid.UUID
    conversation: str
    thread: str


@dataclass(frozen=True)
class Line:
    """A raw NDJSON line the scripted runner writes exactly as it stands."""

    text: str

    def model_dump_json(self) -> str:
        return self.text


@dataclass
class _Ending:
    """First entry of every scripted turn: it sets how that turn ends.

    The scripted runner writes each entry's JSON and a newline, and the client
    skips blank lines, so this entry reaches the worker as nothing. It runs
    when its own turn starts, so one turn's ending never leaks into the next.
    """

    runner: FakeRunner
    end: str
    tail: tuple[Any, ...]
    timeout_status: int
    fail_next_open: bool

    def model_dump_json(self) -> str:
        self.runner.abort_after_frames = self.end == "drop"
        self.runner.hold = asyncio.Event() if self.end == "stall" else None
        self.runner.tail = list(self.tail)
        self.runner.timeout_status = self.timeout_status
        if self.fail_next_open:
            self.runner.event_fail_times = 1
        return ""


@dataclass
class RealRunner:
    """One real runner app on its own port and the paths it was asked for."""

    port: int
    paths: list[str]

    @property
    def turns(self) -> int:
        return self.paths.count("/v1/event")


def frame_line(error_frame: dict[str, Any]) -> Line:
    """A vector's error frame as a runner would write it, decodable or not."""

    return Line(json.dumps({**error_frame, "version": PROTOCOL_VERSION}))


def clean_failure(classification: str = "model-unreachable") -> tuple[Any, ...]:
    """A failed turn with no side effect that the runner itself ended."""

    return (
        ErrorEvent(message="Example provider error.", classification=classification),
        Final(text="The run failed.", status=SessionStatus.CLASSIFIED_FAILURE),
    )


def _provider_unreachable() -> list[Any]:
    """The model turn the real runner reports as an unreachable provider."""

    refused = "API Error: Connection refused (ECONNREFUSED)"
    return [
        AssistantMessage(
            content=[TextBlock(text=refused)], model="fake-model", error="server_error"
        ),
        ResultMessage(
            subtype="error_during_execution",
            duration_ms=1,
            duration_api_ms=1,
            is_error=True,
            num_turns=1,
            session_id="fake-session",
            result=refused,
            usage=None,
        ),
    ]


def _real_runner_app(
    index: int,
    paths: list[str],
    boot_env: Callable[[], dict[str, str] | None],
    script: Callable[[], list[Any]] = _provider_unreachable,
    provider_scope: ProviderScope | None = None,
    preflight: tuple[Path, Path] | None = None,
) -> web.Application:
    """The production runner app over a scripted model, recording each path asked.

    ``boot_env`` answers the environment of the claim that took this runner.
    ``script`` answers each model turn's SDK messages, and ``provider_scope`` is
    the boot credential scope the runner's own entry point would derive.
    ``preflight`` is the mounted workspace and the bundle directory the boot
    runs its verification preflight over; None skips the preflight.
    """

    session = FakeModelSession(script_factory=script)
    gate = RequesterContextGate()
    rules_gate = MemoryRulesContextGate()
    booting = asyncio.Lock()
    app = create_runner_app(
        SessionRunner(
            max_usd_per_day=None,
            held_secrets=frozenset(),
            session_factory=lambda: session,
            ceiling=10_000,
            tracer=RunTracer(None),
            classifier=SideEffectClassifier(),
            trace_name=f"factory-retry-{index}",
            provider_scope=provider_scope,
        ),
        requester_context=gate,
        memory_rules_context=rules_gate,
    )

    @web.middleware
    async def record(request: web.Request, handler: Any) -> web.StreamResponse:
        paths.append(request.path)
        async with booting:
            if not gate.bound:
                # The runner process boots after its claim: it reports its
                # verification preflight when asked to, then reads its
                # completion contract through the issue read capability in its
                # environment and serves the requester context and the learned
                # rules of that execution.
                env = boot_env() or {}
                if preflight is not None:
                    workspace, bundle = preflight
                    await preflight_workspace_verification(
                        workspace, bundle, env[PROGRESS_URL_ENV], env[PROGRESS_TOKEN_ENV]
                    )
                contract = await bind_completion_contract(resolve_issue_read(env))
                gate.bind(contract.contract.execution)
                rules_gate.bind(contract.contract.execution)
        response: web.StreamResponse = await handler(request)
        return response

    app.middlewares.append(record)
    return app


def _request_route(request: httpx.Request) -> tuple[uuid.UUID, str] | None:
    path = request.url.path
    if request.method != "POST" or not path.startswith(_REQUEST_ROUTES):
        return None
    head, _, route = path[len(_REQUEST_ROUTES) :].partition("/")
    try:
        return uuid.UUID(head), route
    except ValueError:
        return None


@pytest.fixture(scope="module")
def factory_retry_database() -> Iterator[str]:
    previous = os.environ.get("DATABASE_URL")
    base = make_url(get_settings().database_url)
    database = f"curie_test_factory_retry_{uuid.uuid4().hex}"

    async def admin(statement: str, *arguments: str, read: bool = False) -> Any:
        connection = await asyncpg.connect(
            user=base.username,
            password=base.password,
            host=base.host,
            port=base.port,
            database="postgres",
        )
        try:
            if read:
                return await connection.fetchval(statement, *arguments)
            return await connection.execute(statement)
        finally:
            await connection.close()

    asyncio.run(admin(f'CREATE DATABASE "{database}"'))
    try:
        database_url = base.set(database=database).render_as_string(hide_password=False)
        os.environ["DATABASE_URL"] = database_url
        get_settings.cache_clear()
        config = Config()
        config.set_main_option("script_location", str(ROOT / "apps/api/alembic"))
        command.upgrade(config, "head")
        yield database_url
    finally:
        asyncio.run(admin(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'))
        if previous is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous
        get_settings.cache_clear()
        remaining = asyncio.run(
            admin("SELECT count(*) FROM pg_database WHERE datname = $1", database, read=True)
        )
        assert remaining == 0


class FactoryRetryCase(StartWaitCase):
    """A start-wait case whose wakes travel the stream and whose passes are real."""

    def __init__(self, root: Path, database_url: str, options: dict[str, Any]) -> None:
        super().__init__(root, "normal", database_url)
        self.options = options
        self.calls: list[Call] = []
        self.items: list[Item] = []
        self.threads: list[str] = []
        self.finish_fault: Exception | None = None
        self.termination_gate: asyncio.Event | None = None
        self.termination_reached = asyncio.Event()
        self.runner_servers: list[TestServer] = []
        self.issue = 5155
        self.consumer: Consumer | None = None
        self.consuming: asyncio.Task[None] | None = None
        self.reconciler_valkey: aredis.Redis | None = None

    def harness_options(self) -> dict[str, Any]:
        # The production attempt bound, so one attempt per started factory
        # execute turn is the kernel's rule and not this harness's setting.
        return {"max_attempts": 3, **self.options}

    async def setup(self) -> None:
        await super().setup()
        self.item = Item(
            self.request_id,
            uuid.UUID(str(self.initial["work_item_id"])),
            self.conversation,
            self.thread,
        )
        self.items.append(self.item)
        self.http.event_hooks["request"].append(self._before_request)
        self.http.event_hooks["response"].append(self._after_response)
        # A turn no test scripted ends at once, with a cause no policy retries.
        self.script_default(*clean_failure("unclassified"))
        self.reconciler_valkey = aredis.Redis(
            host=VALKEY_HOST,
            port=VALKEY_PORT,
            password=VALKEY_PW or None,
            decode_responses=True,
        )
        self.reconciler = WorkItemReconciler(
            async_sessionmaker(self.engine, expire_on_commit=False),
            self.reconciler_valkey,
            get_settings(),
        )
        self.consumer = Consumer(
            redis=self.h.async_redis,
            kernel=self.h.kernel,
            config=self.h.config,
            leases=DeliveryLeaseStore(self.h.async_redis, self.h.config),
            reminders=refuse_unconfigured_reminder,
        )
        await self.consumer.ensure_group()
        self.consuming = asyncio.create_task(self.consumer.run())

    # -- the worker's HTTP calls, observed at the client -------------------------

    async def _before_request(self, request: httpx.Request) -> None:
        parsed = _request_route(request)
        gate = self.termination_gate
        if parsed is not None and parsed[1] == "termination" and gate is not None:
            self.termination_reached.set()
            await gate.wait()

    async def _after_response(self, response: httpx.Response) -> None:
        parsed = _request_route(response.request)
        if parsed is None:
            return
        request_id, route = parsed
        code: str | None = None
        if response.status_code == 409:
            await response.aread()
            detail = response.json().get("detail")
            code = detail.get("code") if isinstance(detail, dict) else detail
        content = response.request.content
        self.calls.append(
            Call(
                route,
                request_id,
                json.loads(content) if content else {},
                response.status_code,
                code,
            )
        )
        fault = self.finish_fault
        if route == "finish" and fault is not None:
            # The API has settled the finish; only its answer is lost.
            self.finish_fault = None
            raise fault

    def calls_to(self, route: str, request_id: uuid.UUID | None = None) -> list[Call]:
        return [
            call
            for call in self.calls
            if call.route == route and (request_id is None or call.request_id == request_id)
        ]

    async def finished(self, request_id: uuid.UUID) -> Call:
        """Wait for the one finish the API accepted for this request."""

        def accepted() -> list[Call]:
            return [call for call in self.calls_to("finish", request_id) if call.status == 200]

        await wait_until(
            lambda: bool(accepted()), f"an accepted finish for {request_id}", timeout=WAIT_S
        )
        (call,) = accepted()
        return call

    # -- SQL -------------------------------------------------------------------

    async def rows(
        self, statement: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        async with self.sessions() as session:
            result = await session.execute(text(statement), params or {})
            return [dict(row) for row in result.mappings().all()]

    async def execute(
        self, statement: str, params: dict[str, Any] | None = None, *, guarded: bool = False
    ) -> None:
        """Write rows directly; ``guarded`` lifts the immutable field guard for this write."""

        async with self.sessions() as session:
            if guarded:
                await session.execute(
                    text("ALTER TABLE curie.execution_requests DISABLE TRIGGER USER")
                )
            await session.execute(text(statement), params or {})
            if guarded:
                await session.execute(
                    text("ALTER TABLE curie.execution_requests ENABLE TRIGGER USER")
                )
            await session.commit()

    async def row(self, request_id: uuid.UUID) -> dict[str, Any]:
        (found,) = await self.rows(
            "SELECT * FROM curie.execution_requests WHERE id = :id", {"id": request_id}
        )
        return found

    async def requests(self, work_item_id: uuid.UUID) -> list[dict[str, Any]]:
        return await self.rows(
            "SELECT * FROM curie.execution_requests WHERE work_item_id = :id ORDER BY sequence",
            {"id": work_item_id},
        )

    async def successor(self, request_id: uuid.UUID) -> dict[str, Any] | None:
        found = await self.rows(
            "SELECT * FROM curie.execution_requests WHERE retry_of_request_id = :id",
            {"id": request_id},
        )
        assert len(found) <= 1, found
        return found[0] if found else None

    async def used(self, work_item_id: uuid.UUID) -> int:
        (found,) = await self.rows(
            "SELECT automatic_retries_used FROM curie.work_items WHERE id = :id",
            {"id": work_item_id},
        )
        return int(found["automatic_retries_used"])

    async def make_due(self, request_id: uuid.UUID) -> None:
        # A guarded row (migration 0182) is due by its guarded time and keeps
        # its waiting deadline where an older dispatcher's claim reads.
        await self.execute(
            "UPDATE curie.execution_requests SET "
            "guarded_dispatch_not_before = CASE WHEN guarded_dispatch_not_before IS NULL "
            "THEN NULL ELSE clock_timestamp() - interval '1 second' END, "
            "dispatch_not_before = CASE WHEN guarded_dispatch_not_before IS NULL "
            "THEN clock_timestamp() - interval '1 second' ELSE wait_deadline END "
            "WHERE id = :id AND status = 'waiting'",
            {"id": request_id},
            guarded=True,
        )

    async def lapse_heartbeat(self, request_id: uuid.UUID) -> None:
        elapsed = timedelta(seconds=get_settings().work_item_runtime_ttl_seconds + 5)
        await self.execute(
            "UPDATE curie.execution_requests "
            "SET runtime_heartbeat_expires_at = clock_timestamp() - CAST(:elapsed AS interval) "
            "WHERE id = :id AND status IN ('running', 'cancellation_requested')",
            {"id": request_id, "elapsed": elapsed},
        )

    # -- the stream and the reconciler -------------------------------------------

    async def reconcile(self) -> None:
        """One real reconciler pass. It publishes every request that is due."""

        await self.reconciler.run_once()

    def stream_event_ids(self, stream: str | None = None) -> list[str]:
        entries: Any = self.redis.xrange(stream or self.names["stream"])
        return [
            json.loads(fields[STREAM_PAYLOAD_FIELD])["event_id"]
            for _entry_id, fields in entries
            if STREAM_PAYLOAD_FIELD in fields
        ]

    def wakes_for(self, request_id: uuid.UUID) -> list[str]:
        prefix = f"work-item-{request_id}-"
        return [event_id for event_id in self.stream_event_ids() if event_id.startswith(prefix)]

    async def publish_wake(self, request_id: uuid.UUID) -> str:
        """Make the request due and let one pass publish its execute wake."""

        await self.make_due(request_id)
        generation = int((await self.row(request_id))["dispatch_generation"])
        await self.reconcile()
        event_id = execute_event_id(request_id, generation)
        assert event_id in self.stream_event_ids(), (event_id, await self.row(request_id))
        return event_id

    def enqueue(self, turn: QueuedTurn) -> None:
        self.redis.xadd(self.names["stream"], to_stream_fields(turn))

    def republish(self, event_id: str) -> None:
        """Deliver a stored stream entry again, field for field."""

        entries: Any = self.redis.xrange(self.names["stream"])
        (fields,) = [
            fields
            for _entry_id, fields in entries
            if STREAM_PAYLOAD_FIELD in fields
            and json.loads(fields[STREAM_PAYLOAD_FIELD])["event_id"] == event_id
        ]
        self.redis.xadd(self.names["stream"], fields)

    def drained(self) -> bool:
        """Every stream entry was delivered and none is still pending."""

        stream = self.names["stream"]
        info: Any = self.redis.xinfo_stream(stream)
        groups: Any = self.redis.xinfo_groups(stream)
        for group in groups:
            if group["name"] == self.names["group"]:
                return bool(
                    group["pending"] == 0
                    and group["last-delivered-id"] == info["last-generated-id"]
                )
        return False

    async def settle(self) -> None:
        await wait_until(self.drained, "the consumer to finish every delivery", timeout=WAIT_S)

    # -- WorkItems and turns -----------------------------------------------------

    async def admit(self) -> Item:
        """Admit one more WorkItem on a conversation of its own."""

        self.issue += 1
        conversation = f"1700000000.{self.issue:06d}"
        thread = f"slack:{ADDRESS}:{conversation}"
        async with self.sessions() as session:
            session.add(
                models.ThreadWorkspace(
                    agent_id=self.agent_id,
                    selected_by_deployment_id=self.deployment_id,
                    conversation_id=thread,
                    repo_full_name=REPOSITORY,
                    selected_by=REQUESTER,
                )
            )
            await session.commit()
        response = await self.http.post(
            "/v1/internal/work-items/admissions",
            headers={"X-Curie-Worker-Token": WORKER_TOKEN},
            json={
                "agent_id": str(self.agent_id),
                "kind": "slack",
                "address": ADDRESS,
                "reply_conversation_id": conversation,
                "tracker": {
                    "kind": "github",
                    "host": "github.com",
                    "scope_id": "101",
                    "issue_id": str(self.issue),
                },
                "repository": {
                    "code_host_kind": "github",
                    "host": "github.com",
                    "project_id": "101",
                    "path": REPOSITORY,
                },
                "code_host_installation_id": 202,
                "request_id": str(uuid.uuid4()),
                "objective": "Implement the admitted work item",
                "requester": REQUESTER,
            },
        )
        assert response.status_code == 200, response.text
        body = response.json()
        item = Item(
            uuid.UUID(body["request"]["id"]),
            uuid.UUID(body["work_item"]["id"]),
            conversation,
            thread,
        )
        self.items.append(item)
        self.threads.append(thread)
        return item

    def _turn(
        self,
        event_id: str,
        conversation: str,
        text_: str,
        source: TurnSource,
        placeholder: str | None = None,
    ) -> QueuedTurn:
        return QueuedTurn(
            event_id=event_id,
            conversation_id=conversation,
            author=REQUESTER,
            text=text_,
            reply_handle=ReplyHandle(
                kind="slack", channel=ADDRESS, adapter="default", placeholder=placeholder
            ),
            received_at=datetime.now(UTC).isoformat(),
            source=source,
        )

    def ci_fix_turn(self, item: Item, round_: int = 2) -> QueuedTurn:
        """A CI fix round's turn, in the shape the reconciler enqueues."""

        return self._turn(
            ci_event_id(item.request_id, round_),
            item.conversation,
            "Fix the failing checks on the pull request.",
            TurnSource.WEBHOOK,
        )

    def approval_resume_turn(self, item: Item) -> QueuedTurn:
        """An approval resume on the request's conversation, in the resume queue's shape."""

        return self._turn(
            f"approval-{uuid.uuid4()}-resolved",
            item.conversation,
            "The request was approved. Continue.",
            TurnSource.SLACK,
        )

    async def interactive_turn(self) -> QueuedTurn:
        """A person's Slack message on a conversation no WorkItem owns.

        The conversation has a selected repository, as a person's thread on
        this deployment does. Without one the worker refuses the turn before
        it reaches a runner.
        """

        self.issue += 1
        conversation = f"1700000001.{self.issue:06d}"
        thread = f"slack:{ADDRESS}:{conversation}"
        async with self.sessions() as session:
            session.add(
                models.ThreadWorkspace(
                    agent_id=self.agent_id,
                    selected_by_deployment_id=self.deployment_id,
                    conversation_id=thread,
                    repo_full_name=REPOSITORY,
                    selected_by=REQUESTER,
                )
            )
            await session.commit()
        self.threads.append(thread)
        return self._turn(
            uuid.uuid4().hex,
            conversation,
            "Summarize the open incidents.",
            TurnSource.SLACK,
            placeholder="p-example",
        )

    async def start_directly(self, item: Item) -> int:
        """Leave a request as its finished execute wake would: running and held.

        The request is acquired and started through the dispatch routes and its
        lease is held to the deadline, which is how a request stands while it
        waits on an approval or on CI. Returns the runtime epoch.
        """

        generation = int((await self.row(item.request_id))["dispatch_generation"])
        owner = self.h.config.consumer_name
        grant = await self.dispatch.acquire(
            item.request_id,
            owner=owner,
            generation=generation,
            wake=FactoryWake(
                kind="slack", address=ADDRESS, adapter="default", conversation_id=item.conversation
            ),
            context=None,
        )
        claim_name = f"curie-thread-{item.request_id.hex[:12]}"
        started = await self.dispatch.start(
            item.request_id,
            owner=owner,
            generation=generation,
            claim_name=claim_name,
            sandbox_name=f"sbx-{claim_name}",
            context=grant.context,
            delivered_limits=UNOBSERVED_LIMITS,
        )
        await self.dispatch.hold_for_approval(item.request_id, runtime_epoch=started.runtime_epoch)
        return started.runtime_epoch

    async def lose_owner(self, request_id: uuid.UUID) -> None:
        """Lapse the heartbeat and let one pass request the owner lost stop.

        Returns once the worker has torn the runtime down and is about to
        record it. The record waits for ``record_teardown``, so the pass has
        ended before any successor can exist.
        """

        self.termination_reached = asyncio.Event()
        self.termination_gate = asyncio.Event()
        await self.lapse_heartbeat(request_id)
        await self.reconcile()
        row = await self.row(request_id)
        assert (row["status"], row["terminal_cause"]) == ("cancellation_requested", "owner_lost")
        await wait_until(
            self.termination_reached.is_set,
            "the worker to observe the teardown",
            timeout=WAIT_S,
        )

    async def record_teardown(self, request_id: uuid.UUID) -> None:
        """Let the held teardown record through and wait for the API to accept it."""

        gate = self.termination_gate
        assert gate is not None
        self.termination_gate = None
        gate.set()
        await wait_until(
            lambda: any(call.status == 200 for call in self.calls_to("termination", request_id)),
            f"the teardown record for {request_id}",
            timeout=WAIT_S,
        )

    async def control_admits_one(self, item: Item | None = None) -> dict[str, Any]:
        """Liveness control: a clean transient failure here admits one successor."""

        assert not self.h.runner.turn_scripts, "an earlier scripted turn never ran"
        target = item if item is not None else await self.admit()
        self.script(*clean_failure())
        await self.publish_wake(target.request_id)
        finish = await self.finished(target.request_id)
        assert finish.facts == {
            "cause": "model_unreachable",
            "retry_safety": "no_effects",
            "retry_condition": None,
        }
        await self.settle()
        successor = await self.successor(target.request_id)
        assert successor is not None and successor["status"] == "waiting", successor
        assert await self.used(target.work_item_id) == 1
        return successor

    # -- the scripted runner -----------------------------------------------------

    def script(
        self,
        *frames: Any,
        end: str = "eof",
        tail: Sequence[Any] = (),
        timeout_status: int = 200,
        fail_next_open: bool = False,
    ) -> None:
        """Queue one runner turn: its frames, then how its stream ends.

        ``eof`` closes the stream, ``drop`` cuts the connection, and ``stall``
        holds the turn open until it is released and then writes ``tail``.
        ``timeout_status`` is the runner's answer to a timeout stop, and
        ``fail_next_open`` makes the runner refuse the turn after this one.
        """

        assert end in {"eof", "drop", "stall"}, end
        runner = self.h.runner
        turn: list[Any] = [
            _Ending(runner, end, tuple(tail), timeout_status, fail_next_open),
            *frames,
        ]
        runner.turn_scripts.append(turn)

    def script_default(self, *frames: Any) -> None:
        """What every turn without a script of its own answers."""

        runner = self.h.runner
        turn: list[Any] = [_Ending(runner, "eof", (), 200, False), *frames]
        runner.default_script = turn

    async def held(self) -> None:
        """Wait until a stalled turn is open on the runner."""

        runner = self.h.runner

        def stalled() -> bool:
            hold = runner.hold
            return isinstance(hold, asyncio.Event) and not hold.is_set()

        await wait_until(stalled, "the runner turn to stall", timeout=WAIT_S)

    def release_turn(self) -> None:
        hold = self.h.runner.hold
        assert isinstance(hold, asyncio.Event)
        hold.set()

    async def real_runners(
        self,
        count: int,
        *,
        script: Callable[[], list[Any]] = _provider_unreachable,
        provider_scope: ProviderScope | None = None,
        preflight_workspace: Path | None = None,
    ) -> list[RealRunner]:
        """Serve real runner apps, one for each sandbox the case claims next.

        Each runs the production session over a scripted model. By default its
        turn ends in the provider error the runner reports as an unreachable
        model; ``script`` replaces those SDK messages, and ``provider_scope``
        is the scope the runner names on its retry conditions. With
        ``preflight_workspace``, each boot first runs the production
        verification preflight over that checkout, with no bundle declaration,
        and reports it on the claim's progress capability.
        """

        preflight: tuple[Path, Path] | None = None
        if preflight_workspace is not None:
            # An empty bundle directory: the repository declaration decides.
            bundle = self.root / "runner-bundle"
            bundle.mkdir(exist_ok=True)
            preflight = (preflight_workspace, bundle)

        served: list[RealRunner] = []
        cluster = self.h.fake_k8s
        # Every claim from here on takes the next pooled runner, in order.
        unpooled = len(cluster.claim_envs) - len(cluster.assigned_ports)
        for index in range(count):
            paths: list[str] = []
            position = unpooled + len(cluster.runner_ports)
            server = TestServer(
                _real_runner_app(
                    index,
                    paths,
                    lambda position=position: cluster.claim_envs[position],
                    script,
                    provider_scope,
                    preflight,
                )
            )
            await server.start_server()
            assert server.port is not None
            self.runner_servers.append(server)
            self.h.fake_k8s.runner_ports.append(server.port)
            served.append(RealRunner(server.port, paths))
        return served

    # -- teardown ----------------------------------------------------------------

    async def close(self) -> None:
        request_ids: list[uuid.UUID] = []
        try:
            if self.termination_gate is not None:
                self.termination_gate.set()
            h = getattr(self, "h", None)
            if h is not None and isinstance(h.runner.hold, asyncio.Event):
                h.runner.hold.set()
            if self.consumer is not None and self.consuming is not None:
                self.consumer.request_stop()
                await asyncio.wait_for(self.consuming, 30)
                assert self.consuming.done()
            for server in self.runner_servers:
                await server.close()
            if self.reconciler_valkey is not None:
                await self.reconciler_valkey.aclose()
            if h is not None:
                for thread in self.threads:
                    await asyncio.to_thread(h.substrate.release, thread)
                    self.workspace.release(thread)
                for item in self.items:
                    request_ids.extend(row["id"] for row in await self.requests(item.work_item_id))
        finally:
            await super().close()
        # A request a test left running on purpose still has its heartbeat
        # loop. Nothing of the case may outlive the case's event loop.
        leftover = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
        for task in leftover:
            task.cancel()
        await asyncio.gather(*leftover, return_exceptions=True)
        self._assert_valkey_clean(request_ids)
        self._assert_bucket_deleted()
        with socket.socket() as probe:
            probe.settimeout(1)
            assert probe.connect_ex(("127.0.0.1", int(self.api_url.rsplit(":", 1)[1]))) != 0

    def _assert_valkey_clean(self, request_ids: list[uuid.UUID]) -> None:
        """Delete the stream's own keys and the CI round keys, then assert none is left.

        The base case covers the worker and sandbox prefixes. The stream
        pattern also covers the dead letter stream and the consumer's liveness
        keys.
        """

        client = redis.Redis(
            host=VALKEY_HOST,
            port=VALKEY_PORT,
            password=VALKEY_PW or None,
            decode_responses=True,
        )
        try:
            patterns = [f"{self.names['stream']}*"]
            patterns.extend(f"{CI_ROUND_KEY_PREFIX}{request_id}:*" for request_id in request_ids)
            for pattern in patterns:
                keys = list(client.scan_iter(match=pattern))
                if keys:
                    client.delete(*keys)
                assert not list(client.scan_iter(match=pattern))
        finally:
            client.close()

    def _assert_bucket_deleted(self) -> None:
        client = boto3.client(
            "s3",
            endpoint_url=os.environ.get("TEST_S3_ENDPOINT_URL")
            or os.environ.get("S3_ENDPOINT_URL", "http://127.0.0.1:29000"),
            aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "rustfs"),
            aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "rustfssecret"),
            region_name="us-east-1",
            config=S3Config(connect_timeout=3, read_timeout=5, retries={"max_attempts": 0}),
        )
        try:
            with pytest.raises(ClientError):
                client.head_bucket(Bucket=self.bucket)
        finally:
            client.close()


@pytest.fixture
def factory_retry(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    factory_retry_database: str,
) -> Iterator[FactoryRetryCase]:
    for executable in ("git", "openssl"):
        if shutil.which(executable) is None:
            raise RuntimeError(f"required executable unavailable: {executable}")
    root = tmp_path / "factory-retry"
    root.mkdir()
    case = FactoryRetryCase(root, factory_retry_database, dict(getattr(request, "param", {})))
    for key, value in {
        "INTERNAL_WORKER_TOKEN": WORKER_TOKEN,
        "API_KEY": OPERATOR_KEY,
        "RESUME_RECONCILER_ENABLED": "false",
        # Each test runs the real reconciler pass itself.
        "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
        "CURIE_DEPLOY_NOTICE_RECONCILER_ENABLED": "false",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        "GITHUB_REPO_ALLOWLIST": '["acme-corp/*"]',
        "S3_ACCESS_KEY": os.environ.get("S3_ACCESS_KEY", "rustfs"),
        "S3_SECRET_KEY": os.environ.get("S3_SECRET_KEY", "rustfssecret"),
        # The API and the reconciler publish to the stream this case consumes.
        "RUNS_STREAM": case.names["stream"],
        "CURIE_CONSUMER_GROUP": case.names["group"],
        "VALKEY_HOST": VALKEY_HOST,
        "VALKEY_PORT": str(VALKEY_PORT),
        "VALKEY_PASSWORD": VALKEY_PW,
    }.items():
        monkeypatch.setenv(key, value)
    for key in ("CURIE_STREAM", "RUNS_CONSUMER_GROUP", "VALKEY_URL"):
        monkeypatch.delenv(key, raising=False)
    get_settings.cache_clear()
    try:
        case.run(case.setup())
        yield case
    finally:
        try:
            case.run(case.close())
        finally:
            case.loop.close()
            get_settings.cache_clear()
            shutil.rmtree(root)
            assert not root.exists()
