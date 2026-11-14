"""Requester coding instructions, worker to runner (ADR 0278 sections 3 and 6, #5592).

Three parts, all through real components. The joined part at the end runs the
boundary path below against a runner built by the production boot with the real
Claude Code CLI, and reads what reached the model and the trace exporter.

The kernel half drives the real kernel (real Valkey lock and markers, the fake
Kubernetes control plane, the in-process fake runner): a WorkItem execute
delivery reads its context envelope and posts it to the runner after the
request starts and before its first turn. Only an acknowledgment the API
accepts lets the turn open; every other outcome fails the started request with
a visible reason, opens no turn (so no model request) and still releases the
claim.

The boundary half is black box from the worker's delivery boundary, with
nothing inside the changed components faked:

1. The real API (``curie_api.main.create_app`` under uvicorn) on an owned,
   migrated Postgres database. The request is admitted, acquired and started
   through the real dispatch routes, then pinned the way admission pins a
   selection (SQL, as an earlier admission wrote it).
2. The runner's execution binding comes from the real completion contract route,
   read with the real issue read capability the worker mints, through the
   runner's own contract client. As in a cluster, the runner reads it when it
   boots, after the acquire and before the worker starts the request.
3. The worker's ``requester_context.deliver`` reads the envelope over the real
   ``WorkItemDispatchClient``, posts it through the real ``RunnerClient`` to the
   real runner app (``curie_runner.server.create_app``) holding a real
   ``RequesterContextGate``, and relays the acknowledgment to the real API.

Injected faults sit outside the changed code: a pass-through wrapper around the
runner client that rewrites the envelope bytes in flight, a runner bound to
another fence, and a runner app served without the route. Each failure shows
the gate decided ``failed`` (so the runner never starts its session: zero model
requests), the API recorded evidence ``failed``, and the request finished
``failed`` through the real finish route with the reason in its detail. The
instruction text never reaches a log.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import os
import shutil
import sys
import uuid
from collections.abc import Iterator
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import claude_agent_sdk
import httpx
import pytest
from aci_protocol import Event, Final
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_api.config import get_settings
from curie_internal.requester_coding_instructions import text_sha256
from curie_runner import RunnerConfig, RunTracer, SideEffectClassifier
from curie_runner import __main__ as runner_boot
from curie_runner.__main__ import build_runner
from curie_runner.fake import FakeModelSession
from curie_runner.issue_read import ISSUE_READ_TOKEN_ENV, ISSUE_READ_URL_ENV
from curie_runner.mcp_tool_capability import McpToolCapabilityProbe
from curie_runner.requester_context import (
    BLOCKED_EXPLANATION,
    PREFLIGHT_BLOCKED,
    Applied,
    Failed,
    NoneSelected,
    RequesterContextGate,
)
from curie_runner.server import bind_status_attestation
from curie_runner.server import create_app as create_runner_app
from curie_runner.session import SessionRunner
from curie_runner.tracker_result import fetch_completion_contract, sibling_urls
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from curie_worker.binding import FactoryWake
from curie_worker.requester_context import (
    PREFLIGHT_BLOCKED_EVIDENCE,
    RequesterContextRefused,
    deliver,
)
from curie_worker.runner_client import RunnerClient
from curie_worker.workitem_dispatch import WorkItemDispatchClient, WorkItemRun
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from apps.worker.tests.test_workspace_recovery_storage import (
    REPOSITORY_PATH,
    _owned_database,
    _require_postgres,
    _served_api,
    _settings_env,
)

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_work_item_workspace import (  # noqa: E402
    ISSUE_URL,
    _Binding,
    _NoExistingPublication,
    _turn,
    _WorkItems,
    _Workspace,
)

RUNNER_TOKEN = "requester-context-runner-token"
CANARY = "CANARY-REQUESTER-BOUNDARY-5592-c41f"
TEXT = f"{CANARY}\nName tests after the behavior they prove.\n"
DIGEST = text_sha256(TEXT)
WORKER_HEADER = "X-Curie-Worker-Token"


@dataclass(frozen=True)
class _Services:
    db_url: str
    api_url: str
    worker_token: str


@pytest.fixture(scope="module")
def services() -> Iterator[_Services]:
    _require_postgres()
    with ExitStack() as stack:
        db_url = stack.enter_context(_owned_database())
        api_env = {
            "DATABASE_URL": db_url,
            "GITHUB_REPO_ALLOWLIST": '["acme-corp/*"]',
            "RESUME_RECONCILER_ENABLED": "false",
            "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
            "APPROVAL_SWEEP_INTERVAL_S": "0",
            "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        }
        for name, value in (
            ("VALKEY_HOST", VALKEY_HOST),
            ("VALKEY_PORT", str(VALKEY_PORT)),
            ("VALKEY_PASSWORD", VALKEY_PW),
        ):
            api_env.setdefault(name, os.environ.get(name, value))
        stack.enter_context(_settings_env(api_env))
        api_url = stack.enter_context(_served_api())
        yield _Services(
            db_url=db_url, api_url=api_url, worker_token=get_settings().internal_worker_token
        )


class _Stage:
    """One admitted, started request and the runner its claim would dial."""

    def __init__(self, services: _Services, http: httpx.AsyncClient) -> None:
        self.services = services
        self.http = http
        self.engine = create_async_engine(services.db_url, poolclass=NullPool)
        self.dispatch = WorkItemDispatchClient(
            api_base_url=services.api_url, worker_token=services.worker_token, client=http
        )
        self.agent_id = uuid.uuid4()
        self.address = f"C0EXAMPLE{uuid.uuid4().hex[:6].upper()}"
        self.wire = f"{uuid.uuid4().int % 10**10}.000100"
        self.sessions: list[FakeModelSession] = []
        self.bindings: dict[uuid.UUID, Any] = {}
        self.servers: list[TestServer] = []

    async def sql(self, statement: str, **params: Any) -> list[dict[str, Any]]:
        async with self.engine.begin() as conn:
            result = await conn.execute(text(statement), params)
            return [dict(row) for row in result.mappings()] if result.returns_rows else []

    async def seed(self) -> None:
        await self.sql(
            "INSERT INTO curie.agents (id, name, repo_full_name) VALUES (:id, :name, :repo)",
            id=self.agent_id,
            name=f"acme-requester-{self.agent_id.hex[:10]}",
            repo=REPOSITORY_PATH,
        )
        await self.sql(
            "INSERT INTO curie.agent_channels (id, agent_id, kind, address, adapter) "
            "VALUES (:id, :agent, 'slack', :address, 'default')",
            id=uuid.uuid4(),
            agent=self.agent_id,
            address=self.address,
        )

    @property
    def wake(self) -> FactoryWake:
        return FactoryWake(
            kind="slack", address=self.address, adapter="default", conversation_id=self.wire
        )

    async def start(self, *, selected: bool) -> WorkItemRun:
        run = await self.acquired(selected=selected)
        # The runner boots and reads its contract before the worker starts it.
        self.bindings[run.request_id] = await self.read_binding(run.request_id)
        await self.begin(run)
        return run

    async def begin(self, run: WorkItemRun) -> None:
        await run.start(claim_name=f"claim-{run.request_id.hex[:8]}", sandbox_name="sbx-requester")
        assert run.runtime_epoch is not None

    async def acquired(self, *, selected: bool) -> WorkItemRun:
        """An admitted request the worker acquired and has not started yet."""

        request_id = uuid.uuid4()
        admitted = await self.http.post(
            f"{self.services.api_url}/v1/internal/work-items/admissions",
            headers={WORKER_HEADER: self.services.worker_token},
            json={
                "agent_id": str(self.agent_id),
                "kind": "slack",
                "address": self.address,
                "reply_conversation_id": self.wire,
                "tracker": {
                    "kind": "github",
                    "host": "github.com",
                    "scope_id": "101",
                    "issue_id": str(uuid.uuid4().int % 10**9),
                },
                "repository": {
                    "code_host_kind": "github",
                    "host": "github.com",
                    "project_id": "101",
                    "path": REPOSITORY_PATH,
                },
                "code_host_installation_id": 202,
                "objective": "Fix the reported widget bug",
                "requester": "U0EXAMPLE1",
                "request_id": str(request_id),
            },
        )
        assert admitted.status_code == 200, admitted.text
        (row,) = await self.sql(
            "SELECT work_item_id, dispatch_generation FROM curie.execution_requests WHERE id = :id",
            id=request_id,
        )
        if selected:
            await self.pin(row["work_item_id"], request_id)
        owner = f"worker-{uuid.uuid4().hex[:8]}"
        grant = await self.dispatch.acquire(
            request_id,
            owner=owner,
            generation=int(row["dispatch_generation"]),
            wake=self.wake,
            context=None,
        )

        async def _stop(*_args: Any) -> None:
            return None

        run = WorkItemRun(
            client=self.dispatch,
            request_id=request_id,
            owner=owner,
            grant=grant,
            wake=self.wake,
            event_id=f"work-item-{request_id}-execute-{grant.generation}",
            thread_key=f"requester-{uuid.uuid4().hex}",
            on_stop=_stop,
            on_stale=_stop,
        )
        return run

    async def pin(self, work_item_id: uuid.UUID, request_id: uuid.UUID) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(text("SET LOCAL session_replication_role = replica"))
            await conn.execute(
                text(
                    "UPDATE curie.work_items SET requester_principal_id = :p, "
                    "coding_instructions_state = 'selected', "
                    "coding_instructions_absence_reason = NULL, coding_instructions_revision = 1, "
                    "coding_instructions_sha256 = :sha, coding_instructions_format_version = 1 "
                    "WHERE id = :w"
                ),
                {"p": uuid.uuid4(), "sha": DIGEST, "w": work_item_id},
            )
            await conn.execute(
                text(
                    "INSERT INTO curie.work_item_coding_snapshots (work_item_id, text) "
                    "VALUES (:w, :t)"
                ),
                {"w": work_item_id, "t": TEXT},
            )
            await conn.execute(
                text(
                    "UPDATE curie.execution_requests SET coding_instructions_selection_id = :w, "
                    "coding_instructions_evidence = 'pending' WHERE id = :r"
                ),
                {"w": work_item_id, "r": request_id},
            )

    async def binding(self, run: WorkItemRun) -> Any:
        """The binding the runner read from its real completion contract at boot."""

        return self.bindings[run.request_id]

    async def read_binding(self, request_id: uuid.UUID) -> Any:
        """The binding the runner reads from its real completion contract now."""

        _issue, capability = await self.dispatch.issue_read_context(request_id)
        urls = sibling_urls(f"{self.services.api_url}/work-items/issue-read")
        contract = await fetch_completion_contract(urls.completion_contract, capability)
        assert contract.execution is not None
        return contract.execution

    async def runner(self, gate: RequesterContextGate | None) -> str:
        session = FakeModelSession(script_factory=lambda: [])
        self.sessions.append(session)
        runner = bind_status_attestation(
            SessionRunner(
                max_usd_per_day=None,
                held_secrets=frozenset(),
                session_factory=lambda: session,
                ceiling=10_000,
                tracer=RunTracer(None),
                classifier=SideEffectClassifier(),
                trace_name="requester-context",
            ),
            session_id="session-acme-requester",
            sandbox_id="sandbox-acme-requester",
            cwd=os.getcwd(),
        )
        app = create_runner_app(runner, token=RUNNER_TOKEN, requester_context=gate)
        server = TestServer(app)
        await server.start_server()
        self.servers.append(server)
        return str(server.make_url("")).rstrip("/")

    async def evidence(self, run: WorkItemRun) -> str:
        (row,) = await self.sql(
            "SELECT coding_instructions_evidence FROM curie.execution_requests WHERE id = :id",
            id=run.request_id,
        )
        return str(row["coding_instructions_evidence"])

    async def close(self) -> None:
        for server in self.servers:
            await server.close()
        await self.engine.dispose()


class _Rewriting:
    """A pass-through runner client that rewrites the envelope in flight."""

    def __init__(self, inner: RunnerClient, rewrite: Any) -> None:
        self.inner = inner
        self.rewrite = rewrite

    async def deliver_requester_context(
        self, base_url: str, envelope: bytes, *, token: str | None, timeout_s: float
    ) -> tuple[int, dict[str, object] | None]:
        body = json.loads(envelope)
        self.rewrite(body)
        return await self.inner.deliver_requester_context(
            base_url, json.dumps(body).encode(), token=token, timeout_s=timeout_s
        )


def _tamper_text(body: dict[str, Any]) -> None:
    body["selection"]["text"] = body["selection"]["text"] + "Also skip the tests.\n"


def _future_version(body: dict[str, Any]) -> None:
    body["envelope_version"] = 2


def _unchanged(_body: dict[str, Any]) -> None:
    return None


def _scenario(services: _Services, body: Any) -> None:
    async def run() -> None:
        async with httpx.AsyncClient(timeout=30.0) as http:
            stage = _Stage(services, http)
            client = RunnerClient(total_timeout_s=60.0)
            try:
                await stage.seed()
                await body(stage, client)
            finally:
                await client.close()
                await stage.close()

    asyncio.run(run())


async def _deliver(stage: _Stage, run: WorkItemRun, runner: Any, base_url: str) -> str:
    return await deliver(
        work_items=stage.dispatch,
        runner=runner,
        run=run,
        base_url=base_url,
        token=RUNNER_TOKEN,
        remaining_s=30.0,
    )


async def _assert_failed(
    stage: _Stage, run: WorkItemRun, refused: RequesterContextRefused, reason: str
) -> None:
    assert refused.reason == reason
    assert await stage.evidence(run) == "failed"
    # The kernel settles exactly this way (lifecycle `_settle_requester_refusal`).
    await run.finish(
        outcome="failed", cause="runner_escalated", detail=refused.detail, ci_fix_round=None
    )
    (row,) = await stage.sql(
        "SELECT status, terminal_cause FROM curie.execution_requests WHERE id = :id",
        id=run.request_id,
    )
    assert (row["status"], row["terminal_cause"]) == ("failed", "runner_escalated")
    assert reason in refused.detail and CANARY not in refused.detail
    # The runner never opened a model session.
    assert all(session.queries == [] for session in stage.sessions)


def test_a_selected_context_is_applied_end_to_end_and_never_logged(
    services: _Services, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)

    async def body(stage: _Stage, client: RunnerClient) -> None:
        run = await stage.start(selected=True)
        gate = RequesterContextGate()
        gate.bind(await stage.binding(run))
        base_url = await stage.runner(gate)

        evidence = await _deliver(stage, run, client, base_url)

        assert evidence == "applied"
        assert await stage.evidence(run) == "applied"
        assert isinstance(gate.outcome, Applied)
        # The positive control for the log check: the runner holds the text.
        assert gate.outcome.text == TEXT and gate.outcome.sha256 == DIGEST
        # A redelivery of the same bytes (a retried start) is accepted again.
        assert await _deliver(stage, run, client, base_url) == "applied"

    _scenario(services, body)
    assert CANARY not in caplog.text


def test_an_absent_selection_is_acknowledged_none(services: _Services) -> None:
    async def body(stage: _Stage, client: RunnerClient) -> None:
        run = await stage.start(selected=False)
        gate = RequesterContextGate()
        gate.bind(await stage.binding(run))
        base_url = await stage.runner(gate)

        assert await _deliver(stage, run, client, base_url) == "none"
        assert await stage.evidence(run) == "none"
        assert isinstance(gate.outcome, NoneSelected)

    _scenario(services, body)


@pytest.mark.parametrize(
    ("rewrite", "reason"),
    [(_tamper_text, "digest_mismatch"), (_future_version, "unsupported_version")],
)
def test_an_envelope_altered_in_flight_fails_before_model_work(
    services: _Services, rewrite: Any, reason: str
) -> None:
    async def body(stage: _Stage, client: RunnerClient) -> None:
        run = await stage.start(selected=True)
        gate = RequesterContextGate()
        gate.bind(await stage.binding(run))
        base_url = await stage.runner(gate)

        with pytest.raises(RequesterContextRefused) as refused:
            await _deliver(stage, run, _Rewriting(client, rewrite), base_url)

        assert isinstance(gate.outcome, Failed) and gate.outcome.reason == reason
        await _assert_failed(stage, run, refused.value, reason)

    _scenario(services, body)


def test_the_pass_through_wrapper_alone_changes_nothing(services: _Services) -> None:
    # The control for the in-flight faults: the same wrapper, rewriting nothing.
    async def body(stage: _Stage, client: RunnerClient) -> None:
        run = await stage.start(selected=True)
        gate = RequesterContextGate()
        gate.bind(await stage.binding(run))
        base_url = await stage.runner(gate)

        assert await _deliver(stage, run, _Rewriting(client, _unchanged), base_url) == "applied"

    _scenario(services, body)


def test_a_runner_bound_to_another_fence_refuses(services: _Services) -> None:
    async def body(stage: _Stage, client: RunnerClient) -> None:
        run = await stage.start(selected=True)
        binding = await stage.binding(run)
        gate = RequesterContextGate()
        gate.bind(
            type(binding)(**{**vars(binding), "execution_fence": binding.execution_fence + 1})
        )
        base_url = await stage.runner(gate)

        with pytest.raises(RequesterContextRefused) as refused:
            await _deliver(stage, run, client, base_url)

        await _assert_failed(stage, run, refused.value, "wrong_fence")

    _scenario(services, body)


def test_a_runner_without_the_route_never_acknowledges(services: _Services) -> None:
    async def body(stage: _Stage, client: RunnerClient) -> None:
        run = await stage.start(selected=False)
        base_url = await stage.runner(None)

        with pytest.raises(RequesterContextRefused) as refused:
            await _deliver(stage, run, client, base_url)

        await _assert_failed(stage, run, refused.value, "runner_unacknowledged")

    _scenario(services, body)


def test_a_runner_that_boots_after_the_start_binds_the_same_execution(
    services: _Services,
) -> None:
    async def body(stage: _Stage, client: RunnerClient) -> None:
        run = await stage.start(selected=True)
        late = await stage.read_binding(run.request_id)
        assert late == await stage.binding(run)
        gate = RequesterContextGate()
        gate.bind(late)
        base_url = await stage.runner(gate)

        assert await _deliver(stage, run, client, base_url) == "applied"

    _scenario(services, body)


@pytest.mark.parametrize(("selected", "kept"), [(False, "none"), (True, "pending")])
def test_a_preflight_blocked_runner_leaves_the_evidence(
    services: _Services, selected: bool, kept: str
) -> None:
    async def body(stage: _Stage, client: RunnerClient) -> None:
        run = await stage.start(selected=selected)
        gate = RequesterContextGate()
        # The start found a declared check it cannot run: blocked before any session.
        gate.block()
        base_url = await stage.runner(gate)

        assert await _deliver(stage, run, client, base_url) == PREFLIGHT_BLOCKED_EVIDENCE

        # Absent stays absent; a selection is never acknowledged nor falsely failed.
        assert await stage.evidence(run) == kept
        assert gate.outcome is None

    _scenario(services, body)


def test_an_envelope_never_delivered_fails_the_request(services: _Services) -> None:
    async def body(stage: _Stage, client: RunnerClient) -> None:
        run = await stage.start(selected=True)
        gate = RequesterContextGate()
        gate.bind(await stage.binding(run))
        base_url = await stage.runner(gate)

        # The start gives up waiting: it is blocked, no model session opens.
        outcome = await gate.wait(0.2)
        assert isinstance(outcome, Failed) and outcome.reason == "missing_envelope"
        assert await stage.evidence(run) == "pending"
        # The envelope that arrives afterwards learns that reason.
        with pytest.raises(RequesterContextRefused) as refused:
            await _deliver(stage, run, client, base_url)

        await _assert_failed(stage, run, refused.value, "missing_envelope")

    _scenario(services, body)


def test_a_tenant_that_disabled_the_capability_fails_the_request(services: _Services) -> None:
    async def body(stage: _Stage, client: RunnerClient) -> None:
        run = await stage.start(selected=True)
        gate = RequesterContextGate()
        gate.bind(await stage.binding(run))
        base_url = await stage.runner(gate)
        (tenant,) = await stage.sql(
            "SELECT tenant_id FROM curie.agents WHERE id = :id", id=stage.agent_id
        )
        await stage.sql(
            "INSERT INTO curie.requester_coding_policies (tenant_id, enabled) VALUES (:t, false) "
            "ON CONFLICT (tenant_id) DO UPDATE SET enabled = false",
            t=tenant["tenant_id"],
        )
        try:
            with pytest.raises(RequesterContextRefused) as refused:
                await _deliver(stage, run, client, base_url)
            # Nothing reached the runner.
            assert gate.outcome is None
            await _assert_failed(stage, run, refused.value, "tenant_disabled")
        finally:
            await stage.sql(
                "UPDATE curie.requester_coding_policies SET enabled = true WHERE tenant_id = :t",
                t=tenant["tenant_id"],
            )

    _scenario(services, body)


class _DisablingFirst:
    """A pass-through runner client that runs ``before`` once the envelope was read."""

    def __init__(self, inner: RunnerClient, before: Any) -> None:
        self.inner = inner
        self.before = before

    async def deliver_requester_context(
        self, base_url: str, envelope: bytes, *, token: str | None, timeout_s: float
    ) -> tuple[int, dict[str, object] | None]:
        await self.before()
        return await self.inner.deliver_requester_context(
            base_url, envelope, token=token, timeout_s=timeout_s
        )


def test_a_tenant_that_disables_the_capability_after_the_read_fails_the_request(
    services: _Services,
) -> None:
    async def body(stage: _Stage, client: RunnerClient) -> None:
        run = await stage.start(selected=True)
        gate = RequesterContextGate()
        gate.bind(await stage.binding(run))
        base_url = await stage.runner(gate)
        (tenant,) = await stage.sql(
            "SELECT tenant_id FROM curie.agents WHERE id = :id", id=stage.agent_id
        )

        async def disable() -> None:
            # The envelope was read while enabled; the runner is still to answer.
            await stage.sql(
                "INSERT INTO curie.requester_coding_policies (tenant_id, enabled) "
                "VALUES (:t, false) ON CONFLICT (tenant_id) DO UPDATE SET enabled = false",
                t=tenant["tenant_id"],
            )

        try:
            with pytest.raises(RequesterContextRefused) as refused:
                await _deliver(stage, run, _DisablingFirst(client, disable), base_url)
            # The runner validated the envelope; the API refused to record it applied.
            assert isinstance(gate.outcome, Applied)
            await _assert_failed(stage, run, refused.value, "tenant_disabled")
            # The pinned snapshot stays with the WorkItem.
            (kept,) = await stage.sql(
                "SELECT count(*) AS n FROM curie.work_item_coding_snapshots s "
                "JOIN curie.execution_requests r ON r.work_item_id = s.work_item_id "
                "WHERE r.id = :id",
                id=run.request_id,
            )
            assert kept["n"] == 1
        finally:
            await stage.sql(
                "UPDATE curie.requester_coding_policies SET enabled = true WHERE tenant_id = :t",
                t=tenant["tenant_id"],
            )

    _scenario(services, body)


# --- The joined path: worker, API and a booted runner with the real CLI ------------
#
# The runner here is the production boot (``build_runner``), not a test session:
# it reads its execution from the real completion contract when it boots, waits
# for the envelope, and only then lets the real Claude Code CLI reach a model.
# The model is a local Messages stand-in that reads the request's committed
# evidence on every model request it receives, and the trace exporter posts to a
# local OTLP collector stand-in, so AC7 reads what the runner actually exported.

_CLI_AVAILABLE = (
    Path(claude_agent_sdk.__file__).parent / "_bundled" / "claude"
).is_file() or shutil.which("claude") is not None
requires_cli = pytest.mark.skipif(
    not _CLI_AVAILABLE, reason="requires the Claude Code CLI the SDK spawns"
)
_PROBE = McpToolCapabilityProbe(
    complete=True,
    has_potential_write_tool=True,
    tool_count=1,
    observed_tools=frozenset(),
    readonly_tools=frozenset(),
)
# The model's reply is exported on the generation span (the user prompt never
# is): the positive control that makes the absence of the instruction text in
# the same exported bytes meaningful.
OUTPUT_CONTROL = "OUTPUT-CONTROL-5592-7d2e"
JOINED_SESSION = "session-acme-requester-joined"


def _sse(event: str, data: dict[str, Any]) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()


class _Model:
    """A Messages stand-in recording the committed evidence at each request."""

    def __init__(self, stage: _Stage, run: WorkItemRun) -> None:
        self.stage = stage
        self.run = run
        self.evidence: list[str] = []
        self.systems: list[str] = []

    async def messages(self, request: web.Request) -> web.StreamResponse:
        body = await request.json()
        self.evidence.append(await self.stage.evidence(self.run))
        self.systems.append(json.dumps(body.get("system")))
        response = web.StreamResponse(headers={"content-type": "text/event-stream"})
        await response.prepare(request)
        message = {
            "id": "msg_stand_in",
            "type": "message",
            "role": "assistant",
            "model": body.get("model", "stand-in"),
            "content": [],
            "stop_reason": None,
            "stop_sequence": None,
            "usage": {"input_tokens": 1, "output_tokens": 1},
        }
        for event, data in (
            ("message_start", {"type": "message_start", "message": message}),
            (
                "content_block_start",
                {
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {"type": "text", "text": ""},
                },
            ),
            (
                "content_block_delta",
                {
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "text_delta", "text": OUTPUT_CONTROL},
                },
            ),
            ("content_block_stop", {"type": "content_block_stop", "index": 0}),
            (
                "message_delta",
                {
                    "type": "message_delta",
                    "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                    "usage": {"output_tokens": 1},
                },
            ),
            ("message_stop", {"type": "message_stop"}),
        ):
            await response.write(_sse(event, data))
        await response.write_eof()
        return response

    async def other(self, _request: web.Request) -> web.Response:
        return web.json_response({"input_tokens": 1})

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_post("/v1/messages", self.messages)
        app.router.add_route("*", "/{tail:.*}", self.other)
        return app


class _Collector:
    """An OTLP/HTTP collector stand-in keeping every exported trace body."""

    def __init__(self) -> None:
        self.bodies: list[bytes] = []

    async def traces(self, request: web.Request) -> web.Response:
        raw = await request.read()
        if request.headers.get("content-encoding") == "gzip":
            raw = gzip.decompress(raw)
        self.bodies.append(raw)
        return web.Response(body=b"", content_type="application/x-protobuf")

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_post("/{tail:.*}", self.traces)
        return app

    @property
    def exported(self) -> bytes:
        return b"".join(self.bodies)


@dataclass
class _Joined:
    final: Final
    model: _Model
    collector: _Collector
    refused: RequesterContextRefused | None
    run: WorkItemRun


def _joined(
    services: _Services,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    rewrite: Any = None,
) -> _Joined:
    """Acquire, boot the runner, start, deliver, then open the first turn."""

    for name in list(os.environ):
        # The API under test keeps its own CURIE_ settings; the runner's model
        # and exporter come only from the config below.
        if name.startswith(("OTEL_", "ANTHROPIC_", "CLAUDE_CODE_")):
            monkeypatch.delenv(name)
    plugin = tmp_path / "bundle"
    (plugin / ".claude-plugin").mkdir(parents=True)
    (plugin / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "implementer", "version": "0.1.0", "description": "t"}),
        encoding="utf-8",
    )
    providers: list[Any] = []
    real_provider = runner_boot.build_tracer_provider

    def keep_provider(*args: Any, **kwargs: Any) -> Any:
        provider = real_provider(*args, **kwargs)
        providers.append(provider)
        return provider

    monkeypatch.setattr(runner_boot, "build_tracer_provider", keep_provider)
    result: dict[str, Any] = {}

    async def body(stage: _Stage, client: RunnerClient) -> None:
        run = await stage.acquired(selected=True)
        model = _Model(stage, run)
        collector = _Collector()
        _issue, capability = await stage.dispatch.issue_read_context(run.request_id)
        monkeypatch.setenv(ISSUE_READ_URL_ENV, f"{stage.services.api_url}/work-items/issue-read")
        monkeypatch.setenv(ISSUE_READ_TOKEN_ENV, capability)
        async with (
            TestServer(model.app(), host="127.0.0.1") as model_server,
            TestServer(collector.app(), host="127.0.0.1") as collector_server,
        ):
            config = RunnerConfig.from_env(
                {
                    "CURIE_PLUGIN_DIR": str(plugin),
                    "CURIE_SESSION_ID": JOINED_SESSION,
                    "CURIE_SANDBOX_ID": "sandbox-acme-requester-joined",
                    "CURIE_BUDGET": '{"max_output_tokens_per_run": 64, "max_usd_per_day": 1.0}',
                    "CURIE_MODEL": "claude-sonnet-5",
                    "OTEL_EXPORTER_OTLP_ENDPOINT": str(collector_server.make_url("")).rstrip("/"),
                    "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
                }
            )
            runner = build_runner(
                config,
                sdk_env={
                    "ANTHROPIC_BASE_URL": str(model_server.make_url("")).rstrip("/"),
                    "ANTHROPIC_API_KEY": "sk-ant-placeholder",
                    "ANTHROPIC_AUTH_TOKEN": "",
                    "CLAUDE_CODE_OAUTH_TOKEN": "",
                    "CLAUDE_CONFIG_DIR": str(tmp_path / "claude-config"),
                    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
                    "DISABLE_TELEMETRY": "1",
                    "HTTP_PROXY": "",
                    "HTTPS_PROXY": "",
                    "ALL_PROXY": "",
                    "NO_PROXY": "127.0.0.1,localhost",
                },
                mcp_capability=_PROBE,
            )
            # The runner boots (and reads its execution) after the acquire...
            server = TestServer(create_runner_app(runner, token=RUNNER_TOKEN), host="127.0.0.1")
            await server.start_server()
            stage.servers.append(server)
            base_url = str(server.make_url("")).rstrip("/")
            # ...and the worker starts the request, then delivers the envelope.
            await stage.begin(run)
            sender: Any = client if rewrite is None else _Rewriting(client, rewrite)
            refused: RequesterContextRefused | None = None
            try:
                assert await _deliver(stage, run, sender, base_url) == "applied"
            except RequesterContextRefused as exc:
                refused = exc
            assert model.evidence == [], "a model request came before the acknowledgment"
            event = Event(
                kind="event", type="message", text="start the issue", user="U0EXAMPLE1", ts="1"
            )
            finals: list[Final] = []
            async with asyncio.timeout(120):
                async with await client.start_turn(base_url, event, token=RUNNER_TOKEN) as stream:
                    async for frame in stream:
                        if isinstance(frame, Final):
                            finals.append(frame)
            (provider,) = providers
            assert provider is not None, "the runner built no trace exporter"
            assert await asyncio.to_thread(provider.force_flush, 30_000)
            result["joined"] = _Joined(finals[0], model, collector, refused, run)
            if refused is not None:
                await _assert_failed(stage, run, refused, refused.reason)
            (row,) = await stage.sql(
                "SELECT row_to_json(r)::text AS row FROM curie.execution_requests r WHERE id = :id",
                id=run.request_id,
            )
            result["row"] = row["row"]
            await server.close()
            stage.servers.remove(server)

    _scenario(services, body)
    joined: _Joined = result["joined"]
    # Errors: neither the request row (its terminal detail) nor the final carries the text.
    assert CANARY not in result["row"]
    assert CANARY not in (joined.final.text or "")
    return joined


@requires_cli
def test_the_booted_runner_reaches_the_model_only_after_the_acknowledgment_commits(
    services: _Services,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    joined = _joined(services, tmp_path, monkeypatch)

    assert joined.refused is None
    assert joined.model.evidence, "the applied run made no model request"
    assert set(joined.model.evidence) == {"applied"}
    # The section reached the model: the text is in the system prompt it sent.
    assert any(CANARY in system for system in joined.model.systems)
    assert joined.final.text != BLOCKED_EXPLANATION
    # Traces: the model's reply was exported (the control), the instructions never.
    exported = joined.collector.exported
    assert JOINED_SESSION.encode() in exported and OUTPUT_CONTROL.encode() in exported
    assert CANARY.encode() not in exported
    assert CANARY not in caplog.text


@requires_cli
@pytest.mark.parametrize(
    ("rewrite", "reason"),
    [(_tamper_text, "digest_mismatch"), (_future_version, "unsupported_version")],
)
def test_a_refused_envelope_never_reaches_the_model_through_the_booted_runner(
    services: _Services,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    rewrite: Any,
    reason: str,
) -> None:
    caplog.set_level(logging.DEBUG)
    joined = _joined(services, tmp_path, monkeypatch, rewrite=rewrite)

    assert joined.refused is not None and joined.refused.reason == reason
    assert joined.model.evidence == []
    assert joined.final.text == BLOCKED_EXPLANATION
    exported = joined.collector.exported
    assert JOINED_SESSION.encode() in exported
    assert CANARY.encode() not in exported
    assert CANARY not in caplog.text
    assert CANARY not in joined.refused.detail


# --- The kernel half ---------------------------------------------------------------

SELECTED_DIGEST = "a" * 64


class _RecordingWorkItems(_WorkItems):
    """The dispatch double, recording what the worker read and relayed."""

    def __init__(self, *, selected: bool = False, read_refusal: str | None = None) -> None:
        super().__init__()
        self.selected = selected
        self.read_refusal = read_refusal
        self.reads: list[tuple[uuid.UUID, int]] = []
        self.relayed: list[tuple[dict[str, Any] | None, str | None]] = []

    async def requester_context(self, request_id: uuid.UUID, *, runtime_epoch: int) -> Any:
        from curie_worker.workitem_dispatch import WorkItemConflict

        self.calls.append("requester_context")
        self.reads.append((request_id, runtime_epoch))
        if self.read_refusal is not None:
            raise WorkItemConflict(self.read_refusal)
        envelope = await super().requester_context(request_id, runtime_epoch=runtime_epoch)
        if self.selected:
            envelope["selection"] = {
                "kind": "selected",
                "principal_id": str(uuid.uuid4()),
                "revision": 1,
                "sha256": SELECTED_DIGEST,
                "format_version": 1,
                "text": "Prefer early returns.",
            }
        return envelope

    async def acknowledge_requester_context(
        self,
        request_id: uuid.UUID,
        *,
        runtime_epoch: int,
        acknowledgment: dict[str, Any] | None,
        reason: str | None,
    ) -> str:
        self.calls.append("acknowledge_requester_context")
        self.relayed.append((acknowledgment, reason))
        return await super().acknowledge_requester_context(
            request_id, runtime_epoch=runtime_epoch, acknowledgment=acknowledgment, reason=reason
        )


async def _execute(h: Any, work_items: _RecordingWorkItems) -> uuid.UUID:
    h.kernel._work_items = work_items
    request_id = uuid.uuid4()
    await h.kernel.process_event(_turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}"))
    return request_id


@pytest.mark.parametrize("selected", [False, True])
def test_an_acknowledged_context_opens_the_first_turn(make_harness, selected: bool) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
        ) as h:
            work_items = _RecordingWorkItems(selected=selected)

            request_id = await _execute(h, work_items)

            # Delivered once, after start and before the turn, for the started fence.
            assert work_items.reads == [(request_id, 1)]
            assert work_items.calls.index("start") < work_items.calls.index("requester_context")
            assert len(h.runner.requester_contexts) == 1
            assert h.runner.requester_contexts[0]["execution_fence"] == 1
            ((ack, reason),) = work_items.relayed
            assert ack is not None and reason is None
            assert ack["outcome"] == ("applied" if selected else "none")
            assert h.runner.opened, "the acknowledged request never opened its turn"
            # It ends on its own merits (here: no pull request), not on the context.
            ((finish),) = work_items.finishes
            assert finish["cause"] != "runner_escalated"

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("answer", "relayed_reason"),
    [
        # A runner without the route, as an older image answers.
        ((404, {"error": "not found"}), "runner_unacknowledged"),
        # A runner that expected no envelope.
        ((409, {"error": "not_expected"}), "runner_unacknowledged"),
        # A 200 without an acknowledgment.
        ((200, {}), "runner_unacknowledged"),
    ],
)
def test_a_runner_that_never_acknowledges_fails_the_request_before_model_work(
    make_harness, answer: tuple[int, dict[str, Any]], relayed_reason: str
) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
        ) as h:
            h.runner.requester_context_answer = answer
            work_items = _RecordingWorkItems(selected=True)

            await _execute(h, work_items)

            assert work_items.relayed == [(None, relayed_reason)]
            _assert_failed_before_model_work(h, work_items, relayed_reason)

    asyncio.run(exercise())


@pytest.mark.parametrize("selected", [False, True])
def test_a_preflight_blocked_runner_still_answers_its_explanation_turn(
    make_harness, selected: bool
) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
        ) as h:
            h.runner.requester_context_answer = (409, {"error": PREFLIGHT_BLOCKED})
            work_items = _RecordingWorkItems(selected=selected)

            await _execute(h, work_items)

            # Posted once, nothing relayed: the evidence stays as admission left it.
            assert len(h.runner.requester_contexts) == 1
            assert work_items.relayed == []
            # The blocked start's turn opens and answers its own explanation.
            assert h.runner.opened, "the blocked request never reached its explanation turn"
            ((finish),) = work_items.finishes
            assert finish["cause"] != "runner_escalated"

    asyncio.run(exercise())


def test_a_runner_refusal_is_relayed_with_its_reason(make_harness) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
        ) as h:
            work_items = _RecordingWorkItems(selected=True)
            h.kernel._work_items = work_items
            request_id = uuid.uuid4()
            work_item_id = str(uuid.uuid5(uuid.UUID(int=0), str(request_id)))
            h.runner.requester_context_answer = (
                200,
                {
                    "acknowledgment": {
                        "envelope_version": 1,
                        "work_item_id": work_item_id,
                        "execution_request_id": str(request_id),
                        "execution_fence": 1,
                        "outcome": "failed",
                    },
                    "reason": "digest_mismatch",
                },
            )

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            ((ack, reason),) = work_items.relayed
            assert ack is not None and ack["outcome"] == "failed"
            assert reason == "digest_mismatch"
            _assert_failed_before_model_work(h, work_items, "digest_mismatch")

    asyncio.run(exercise())


@pytest.mark.parametrize("refusal", ["tenant_disabled", "coding_instructions_unavailable"])
def test_an_envelope_the_api_refuses_fails_the_request_without_posting(
    make_harness, refusal: str
) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
        ) as h:
            work_items = _RecordingWorkItems(selected=True, read_refusal=refusal)

            await _execute(h, work_items)

            # Nothing reached the runner, and the failure was relayed for evidence.
            assert h.runner.requester_contexts == []
            assert work_items.relayed == [(None, refusal)]
            _assert_failed_before_model_work(h, work_items, refusal)

    asyncio.run(exercise())


def _assert_failed_before_model_work(h: Any, work_items: _WorkItems, reason: str) -> None:
    assert h.runner.opened == [], "a turn opened without an acknowledged context"
    assert h.runner.queried == []
    ((finish),) = work_items.finishes
    assert finish["outcome"] == "failed"
    assert finish["cause"] == "runner_escalated"
    assert reason in str(finish["detail"])
    # The claim is released, as for any other settled request.
    assert h.fake_k8s.claims == {}
