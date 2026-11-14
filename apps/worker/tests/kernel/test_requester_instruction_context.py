"""Requester coding instructions, worker to runner (ADR 0278 sections 3 and 6, #5592).

Two halves, both through real components.

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
   runner's own contract client.
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
import json
import logging
import os
import sys
import uuid
from collections.abc import Iterator
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest
from aiohttp.test_utils import TestServer
from curie_api.config import get_settings
from curie_internal.requester_coding_instructions import text_sha256
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.fake import FakeModelSession
from curie_runner.requester_context import (
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
from curie_worker.requester_context import RequesterContextRefused, deliver
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
        await run.start(claim_name=f"claim-{request_id.hex[:8]}", sandbox_name="sbx-requester")
        assert run.runtime_epoch is not None
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
        """The binding the runner reads from its real completion contract."""

        _issue, capability = await self.dispatch.issue_read_context(run.request_id)
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
