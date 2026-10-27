"""A factory work-item execution checks out its WorkItem repository (#2992).

The objective of a factory execution is an issue URL. That URL is the
assignment, not a repository picker: the API bound the repository to the
WorkItem from the signed delivery, and the acquire grant carries it. An
ordinary chat message naming the same issue URL still selects nothing.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from aci_protocol import (
    ErrorEvent,
    Event,
    Final,
    PublicationContext,
    QueuedTurn,
    ReplyHandle,
    SessionStatus,
    TextDelta,
    ToolNote,
    TurnSource,
)
from channel_protocol.reply import ReplyAck, ReplyEvent
from curie_worker.approvals import (
    ApprovalBackendError,
    ApprovalClient,
    ApprovalRequest,
    CreatedApproval,
    PublicationCreateRequest,
    PublicationLineage,
)
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.binding import (
    AmbiguousRoute,
    FactoryBindingRefused,
    FactoryContext,
    FactoryWake,
    HttpIdentityBinding,
)
from curie_worker.config import WorkerConfig
from curie_worker.consumer_liveness import ConsumerLivenessStore, ThreadLockOwnerLiveness
from curie_worker.kernel import failures
from curie_worker.kernel.failures import ThreadBusyError
from curie_worker.reply_sink import ReplySink, TargetRoute, build_reply_sink
from curie_worker.runner_client import RunnerWorkspaceSnapshot
from curie_worker.sandbox import SandboxError, SuspendedThreadError
from curie_worker.workitem_dispatch import (
    WorkItemAcquireGrant,
    WorkItemConflict,
    WorkItemHeartbeat,
    WorkItemRequestView,
    WorkItemRun,
    WorkItemRunning,
    WorkItemStartGrant,
    WorkItemStartRefused,
    WorkItemTransportError,
)
from curie_worker.workspace import WorkspacePreparationError, WorkspaceRepositoryNotAllowed
from redis.exceptions import ResponseError

from apps.worker.tests.queue_fixtures import factory_context, factory_conversation, wait_until

AGENT_ID = uuid.UUID("11111111-1111-4111-8111-111111111111")
DEPLOYMENT_ID = uuid.UUID("22222222-2222-4222-8222-222222222222")
CHANNEL = "C0EXAMPLE1"
WORK_ITEM_REPO = "acme-corp/widgets"
ISSUE_URL = f"https://github.com/{WORK_ITEM_REPO}/issues/123"


class _Binding:

    async def identity_binding(
        self, kind: str, adapter: str | None, address: str,
    ) -> HttpIdentityBinding | None:
        """@spec HTTP-INVOCATION-EXECUTION-18: explicit local fixture classification."""
        agent_id = AGENT_ID
        return HttpIdentityBinding(
            tenant_id=uuid.UUID("00000000-0000-4000-8000-000000000001"),
            agent_id=agent_id,
            binding_id=uuid.uuid5(uuid.NAMESPACE_URL, repr((kind, adapter, address))),
            binding_generation=1, http_identity_required=False,
        )

    def _deployment(self) -> object:
        return SimpleNamespace(
            agent_id=AGENT_ID,
            agent_name="acme-bot",
            deployment_id=DEPLOYMENT_ID,
            endpoint=None,
            adapter=None,
            approval_routes=None,
        )

    async def resolve(self, kind: str, adapter: str | None, channel: str) -> object:
        return self._deployment()

    async def resolve_factory(self, context: FactoryContext) -> object:
        assert context.agent_id == AGENT_ID
        return self._deployment()

    def boot_env(self, _resolved: object, thread_key: str, **_: object) -> dict[str, str]:
        return {
            "CURIE_SESSION_ID": f"work-item-{thread_key}",
            "CURIE_RUNNER_TOKEN": "example-work-item-runner-token",
        }

    def packs_for(self, _resolved: object) -> BehaviorPacks:
        return BehaviorPacks()


class _Workspace:
    def __init__(self, substrate: object) -> None:
        self.substrate = substrate
        self.selections: list[object] = []
        self.claimed: list[object] = []

    def select_repository(self, **kwargs: object) -> object:
        self.selections.append(kwargs["repo_full_name"])
        return kwargs["repo_full_name"]

    def claim_or_resume_with_handle(self, **kwargs: object) -> object:
        self.claimed.append(kwargs.get("repo_full_name"))
        if kwargs.get("replace_handle") is not None:
            handoff = self.substrate.handoff(  # type: ignore[attr-defined]
                str(kwargs["thread_key"]),
                expected=kwargs["replace_handle"],
                env=dict(kwargs.get("env") or {}),
                workspace_repo=kwargs.get("repo_full_name"),
                agent_name=kwargs.get("agent_name"),
                caller_run=kwargs.get("caller_run"),
            )
            return SimpleNamespace(handle=handoff, prepared=None)
        try:
            handle = self.substrate.claim(  # type: ignore[attr-defined]
                str(kwargs["thread_key"]),
                env=kwargs.get("env"),
                agent_name=kwargs.get("agent_name"),
                workspace_repo=kwargs.get("repo_full_name"),
                caller_run=kwargs.get("caller_run"),
            )
        except SuspendedThreadError:
            handle = self.substrate.resume(  # type: ignore[attr-defined]
                str(kwargs["thread_key"]),
                env=kwargs.get("env"),
                agent_name=kwargs.get("agent_name"),
                workspace_repo=kwargs.get("repo_full_name"),
                caller_run=kwargs.get("caller_run"),
            )
        return SimpleNamespace(handle=handle, prepared=None)

    def touch(self, _thread_key: str, *, ttl_seconds: int) -> bool:
        return True

    def release(self, _thread_key: str) -> None:
        return None


class _WorkItems:
    """Dispatch double: the acquire grant carries the WorkItem repository."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.after_finish: Callable[[], Awaitable[None]] | None = None
        self.finishes: list[dict[str, object]] = []
        self.acquisitions: dict[uuid.UUID, WorkItemAcquireGrant] = {}
        self.starts: dict[uuid.UUID, WorkItemStartGrant] = {}
        self.terminal_outcomes: dict[uuid.UUID, str] = {}
        self.deadline_holds: set[uuid.UUID] = set()

    async def acquire(
        self,
        request_id: uuid.UUID,
        *,
        owner: str,
        generation: int,
        wake: FactoryWake,
        context: FactoryContext | None,
    ) -> WorkItemAcquireGrant:
        self.calls.append("acquire")
        if request_id in self.starts or request_id in self.terminal_outcomes:
            raise WorkItemConflict("not_dispatchable")
        existing = self.acquisitions.get(request_id)
        if existing is not None:
            assert existing.generation == generation
            assert context is None or context == existing.context
            return existing
        grant = WorkItemAcquireGrant(
            generation=generation,
            work_item_id=request_id,
            conversation_id=factory_conversation(wake),
            wait_deadline=datetime.now(UTC) + timedelta(hours=1),
            repository_path=WORK_ITEM_REPO,
            context=factory_context(AGENT_ID, wake),
        )
        self.acquisitions[request_id] = grant
        return grant

    async def running_for_conversation(
        self, conversation_id: str, *, wake: FactoryWake
    ) -> WorkItemRunning | None:
        self.calls.append("running_for_conversation")
        matches = [
            (request_id, grant)
            for request_id, grant in self.acquisitions.items()
            if grant.conversation_id == conversation_id
        ]
        if not matches:
            return None
        running = [
            (request_id, grant, self.starts[request_id])
            for request_id, grant in matches
            if request_id in self.starts
            and request_id not in self.terminal_outcomes
            and self.starts[request_id].execution_deadline > datetime.now(UTC)
        ]
        if not running:
            raise WorkItemConflict("execution_ended")
        assert len(running) == 1
        request_id, grant, started = running[0]
        if factory_conversation(wake) != grant.conversation_id:
            raise WorkItemConflict("factory_wake_mismatch")
        return WorkItemRunning(
            request_id=request_id,
            work_item_id=grant.work_item_id,
            agent_id=grant.agent_id,
            generation=grant.generation,
            conversation_id=grant.conversation_id,
            wait_deadline=grant.wait_deadline,
            repository_path=grant.repository_path,
            runtime_epoch=started.runtime_epoch,
            execution_deadline=started.execution_deadline,
            lease_held_to_deadline=request_id in self.deadline_holds,
            context=grant.context,
            refusal=None,
        )

    async def start(
        self, request_id: uuid.UUID, *, context: FactoryContext, **_: object
    ) -> WorkItemStartGrant:
        self.calls.append("start")
        acquired = self.acquisitions[request_id]
        assert context == acquired.context
        assert request_id not in self.starts
        assert request_id not in self.terminal_outcomes
        started = WorkItemStartGrant(
            runtime_epoch=1,
            execution_deadline=datetime.now(UTC) + timedelta(hours=1),
            remaining_s=3600.0,
            heartbeat_interval_s=60.0,
        )
        self.starts[request_id] = started
        return started

    async def hold_for_approval(
        self, request_id: uuid.UUID, *, runtime_epoch: int, **_: object,
    ) -> None:
        self.calls.append("hold_for_approval")
        assert runtime_epoch == self.starts[request_id].runtime_epoch
        assert request_id not in self.terminal_outcomes
        self.deadline_holds.add(request_id)

    async def hold_for_ci_fix(self, request_id: uuid.UUID, *, runtime_epoch: int) -> None:
        self.calls.append("hold_for_ci_fix")
        assert runtime_epoch == self.starts[request_id].runtime_epoch
        assert request_id not in self.terminal_outcomes
        self.deadline_holds.add(request_id)

    async def heartbeat(self, request_id: uuid.UUID, *, runtime_epoch: int) -> WorkItemHeartbeat:
        self.calls.append("heartbeat")
        assert runtime_epoch == self.starts[request_id].runtime_epoch
        assert request_id not in self.terminal_outcomes
        self.deadline_holds.discard(request_id)
        return WorkItemHeartbeat(status="running", terminal_cause=None, work_item_cancelled=False)

    async def issue_read_context(self, request_id: uuid.UUID) -> tuple[str, str]:
        self.calls.append("issue_read_context")
        return f"{WORK_ITEM_REPO}#7", f"wir.capability-for-{request_id}"

    async def finish(self, request_id: uuid.UUID, **kwargs: object) -> None:
        self.calls.append("finish")
        started = self.starts[request_id]
        assert kwargs["runtime_epoch"] == started.runtime_epoch
        assert request_id not in self.terminal_outcomes
        outcome = kwargs["outcome"]
        assert outcome in {"completed", "failed"}
        assert isinstance(outcome, str)
        self.terminal_outcomes[request_id] = outcome
        self.deadline_holds.discard(request_id)
        self.finishes.append(kwargs)
        if self.after_finish is not None:
            await self.after_finish()

    async def get_request(self, request_id: uuid.UUID) -> WorkItemRequestView:
        self.calls.append("get_request")
        if request_id not in self.acquisitions:
            raise WorkItemConflict("not_found")
        started = self.starts.get(request_id)
        status = self.terminal_outcomes.get(
            request_id, "running" if started is not None else "waiting"
        )
        return WorkItemRequestView(
            status=status,
            runtime_epoch=started.runtime_epoch if started is not None else 0,
            runtime_claim_name=None,
            runtime_sandbox_name=None,
        )

    def __getattr__(self, name: str):  # type: ignore[no-untyped-def]
        async def record(*_args: object, **_kwargs: object) -> None:
            self.calls.append(name)

        return record


class _Approvals:
    async def create(self, _request: ApprovalRequest, *, budget_s: float = 120) -> CreatedApproval:
        return CreatedApproval(id="appr-1", status="pending")


class _NoExistingPublication:
    async def get_publication_lineage(self, *_args: object) -> None:
        return None

    async def get_publication_precheck_context(self, **_kwargs: object) -> None:
        return None


class _HttpPrecheckApi(_NoExistingPublication):
    """Serve the API refusal shape from #4299 to the real worker client."""

    def __init__(self, responses: list[tuple[int, dict[str, object] | None]]) -> None:
        self.mints: list[httpx.Request] = []

        def answer(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v1/internal/publications/precheck/context"
            assert request.headers["X-Curie-Worker-Token"] == "example-worker-token"
            self.mints.append(request)
            status, body = responses[min(len(self.mints) - 1, len(responses) - 1)]
            return httpx.Response(status, json=body) if body is not None else httpx.Response(status)

        self.http = httpx.AsyncClient(transport=httpx.MockTransport(answer))
        self.client = ApprovalClient(
            api_base_url="http://api.example.test",
            api_key="",
            client=self.http,
            read_timeout_s=5.0,
            worker_token="example-worker-token",
        )

    async def get_publication_precheck_context(self, **kwargs: object) -> PublicationContext | None:
        return await self.client.get_publication_precheck_context(**kwargs)  # type: ignore[arg-type]

    async def aclose(self) -> None:
        await self.http.aclose()


_PR_NOT_ADOPTED_RESPONSE: tuple[int, dict[str, object]] = (
    409,
    {
        "detail": {
            "code": "pull_request_not_adopted",
            "message": "an earlier pull request could not be continued",
        }
    },
)


def test_real_precheck_client_raises_the_dedicated_existing_pr_refusal() -> None:
    from curie_worker.approvals import PullRequestNotAdopted

    async def exercise() -> None:
        publications = _HttpPrecheckApi([_PR_NOT_ADOPTED_RESPONSE])
        try:
            with pytest.raises(PullRequestNotAdopted) as caught:
                await publications.get_publication_precheck_context(
                    deployment_id=DEPLOYMENT_ID,
                    work_item_id=uuid.uuid4(),
                    execution_request_id=uuid.uuid4(),
                    runtime_epoch=1,
                    queued_event_id="work-item-context-example",
                )
            assert type(caught.value) is PullRequestNotAdopted
            assert len(publications.mints) == 1
        finally:
            await publications.aclose()

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("status", "body"),
    [
        (409, {"detail": {"code": "invalid_context"}}),
        (503, {"detail": {"code": "precheck_unavailable"}}),
        (503, {"detail": {"code": "pull_request_not_adopted"}}),
        (409, {"detail": "pull_request_not_adopted"}),
    ],
)
def test_real_precheck_client_keeps_other_refusals_as_backend_errors(
    status: int, body: dict[str, object]
) -> None:
    async def exercise() -> None:
        publications = _HttpPrecheckApi([(status, body)])
        try:
            with pytest.raises(ApprovalBackendError) as caught:
                await publications.get_publication_precheck_context(
                    deployment_id=DEPLOYMENT_ID,
                    work_item_id=uuid.uuid4(),
                    execution_request_id=uuid.uuid4(),
                    runtime_epoch=1,
                    queued_event_id="work-item-context-example",
                )
            assert type(caught.value) is ApprovalBackendError
            assert len(publications.mints) == 1
        finally:
            await publications.aclose()

    asyncio.run(exercise())


class _RecordingSink:
    def __init__(self, delegate: ReplySink) -> None:
        self.delegate = delegate
        self.events: list[str] = []

    async def emit(
        self,
        event: ReplyEvent,
        *,
        route: TargetRoute,
        best_effort_unreachable: bool = False,
    ) -> ReplyAck:
        self.events.append(event.event)
        return await self.delegate.emit(
            event,
            route=route,
            best_effort_unreachable=best_effort_unreachable,
        )


def _thread_key(conversation_id: str = "1700000000.000001") -> str:
    """The worker-internal route key for a turn built by ``_turn`` below."""

    return f"slack:{CHANNEL}:{conversation_id}"


def _turn(
    event_id: str,
    text: str,
    *,
    kind: str = "slack",
    placeholder: str | None = None,
    conversation_id: str = "1700000000.000001",
) -> QueuedTurn:
    return QueuedTurn(
        event_id=event_id,
        conversation_id=conversation_id,
        author="U0EXAMPLE1",
        text=text,
        reply_handle=ReplyHandle(
            kind=kind,
            channel=WORK_ITEM_REPO if kind == "github" else CHANNEL,
            placeholder=placeholder,
            endpoint=None,
            adapter=None,
        ),
        received_at="2026-09-23T01:00:00+00:00",
        source=TurnSource.SLACK,
    )


def test_work_item_execution_checks_out_the_work_item_repository(make_harness) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
        ) as h:
            work_items = _WorkItems()
            h.kernel._work_items = work_items
            h.runner.default_script = [
                TextDelta(text="Working. "),
                Final(text="Working. Done.", status=SessionStatus.DONE),
            ]
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            assert "acquire" in work_items.calls
            assert h.kernel._workspace.selections == [WORK_ITEM_REPO]
            assert h.kernel._workspace.claimed == [WORK_ITEM_REPO]
            assert h.sink.updates == []

    asyncio.run(exercise())


@pytest.mark.parametrize("mint_mode", ["context", "absent", "error"])
def test_factory_turn_receives_only_authoritative_publication_context(
    make_harness, mint_mode: str
) -> None:
    async def exercise() -> None:
        request_id = uuid.uuid4()
        expected = PublicationContext(
            agent_id=AGENT_ID,
            deployment_id=DEPLOYMENT_ID,
            work_item_id=request_id,
            execution_request_id=request_id,
            runtime_epoch=1,
            conversation_id=f"work-item-{request_id}",
            lineage_id=uuid.uuid4(),
            lineage_version=2,
            expected_head="a" * 40,
            queued_event_id=f"work-item-{request_id}-execute-1",
            precheck_url="https://api.example.com/publications/precheck",
            capability="ppc.example.signature",
            observed_title="Existing pull request",
            observed_body_sha256="b" * 64,
            observed_at=datetime.now(UTC),
        )

        class PublicationApi:
            def __init__(self) -> None:
                self.mint_calls: list[dict[str, object]] = []

            async def get_publication_lineage(self, *_args: object) -> PublicationLineage | None:
                if mint_mode == "absent":
                    return None
                return PublicationLineage(
                    id=expected.lineage_id,
                    deployment_id=DEPLOYMENT_ID,
                    conversation_id=expected.conversation_id,
                    repo_full_name=WORK_ITEM_REPO,
                    base_sha="c" * 40,
                    branch="curie/thread-example",
                    pr_number=7,
                    pr_url=f"https://github.com/{WORK_ITEM_REPO}/pull/7",
                    head_sha=expected.expected_head,
                    state="open",
                    version=2,
                    latest_revision=1,
                    has_pending_revision=False,
                    has_pending_outcome=False,
                    visible_outcome_revision=1,
                )

            async def get_publication_precheck_context(
                self, **kwargs: object
            ) -> PublicationContext | None:
                self.mint_calls.append(kwargs)
                if mint_mode == "error":
                    raise RuntimeError("publication context mint unavailable")
                return expected if mint_mode == "context" else None

        publication_api = PublicationApi()
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=publication_api,
            runner_api_base_url="http://runner-api.example.com:8000",
        ) as h:
            h.kernel._work_items = _WorkItems()
            h.runner.default_script = [Final(text="Done.", status=SessionStatus.DONE)]
            captured: list[Event] = []
            start_turn = h.kernel._runner.start_turn

            async def capture_turn(base_url: str, event: Event, *args: object, **kwargs: object):
                captured.append(event)
                return await start_turn(base_url, event, *args, **kwargs)

            h.kernel._runner.start_turn = capture_turn  # type: ignore[method-assign]
            with contextlib.suppress(RuntimeError):
                await h.kernel.process_event(
                    _turn(expected.queued_event_id, f"Resolve {ISSUE_URL}")
                )

            assert publication_api.mint_calls
            if mint_mode == "error":
                assert captured == []
            else:
                assert len(captured) == 2
                assert len(publication_api.mint_calls) == len(captured)
                for turn in captured:
                    assert turn.publication_context == (
                        expected.model_copy(
                            update={
                                "precheck_url": (
                                    f"{h.config.runner_facing_api_base_url.rstrip('/')}"
                                    "/publications/precheck"
                                )
                            }
                        )
                        if mint_mode == "context"
                        else None
                    )
                    assert expected.capability not in turn.text

    asyncio.run(exercise())


def test_existing_pr_refusal_finishes_execute_once_without_starting_the_runner(
    make_harness, caplog: pytest.LogCaptureFixture
) -> None:
    async def exercise() -> None:
        publications = _HttpPrecheckApi([_PR_NOT_ADOPTED_RESPONSE])
        try:
            async with make_harness(
                binding=_Binding(),
                workspace_factory=_Workspace,
                publication_creator=publications,
            ) as h:
                items = _WorkItems()
                h.kernel._work_items = items
                event = _turn(f"work-item-{uuid.uuid4()}-execute-1", f"Resolve {ISSUE_URL}")

                await h.kernel.process_event(event)

                assert len(publications.mints) == 1
                assert h.runner.opened == []
                assert items.calls.count("start") == 1
                assert items.calls.count("finish") == 1
                assert items.finishes[0]["outcome"] == "failed"
                assert items.finishes[0]["cause"] == "pull_request_not_adopted"
                assert await h.kernel._markers.is_terminal(event.event_id)
                assert all("runner_escalated" not in text for text in _escalations(caplog))
                assert any("pull-request-not-adopted" in text for text in _escalations(caplog))
        finally:
            await publications.aclose()

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("status", "code"), [(409, "invalid_context"), (503, "precheck_unavailable")]
)
def test_other_precheck_backend_errors_exhaust_the_existing_execute_retry_budget(
    make_harness, status: int, code: str
) -> None:
    async def exercise() -> None:
        publications = _HttpPrecheckApi([(status, {"detail": {"code": code}})])
        try:
            async with make_harness(
                binding=_Binding(),
                workspace_factory=_Workspace,
                publication_creator=publications,
            ) as h:
                items = _WorkItems()
                h.kernel._work_items = items

                await h.kernel.process_event(
                    _turn(f"work-item-{uuid.uuid4()}-execute-1", f"Resolve {ISSUE_URL}")
                )

                assert len(publications.mints) == h.config.max_attempts
                assert h.runner.opened == []
                assert items.calls.count("finish") == 1
                assert items.finishes[0]["outcome"] == "failed"
                assert items.finishes[0]["cause"] == "runner_escalated"
        finally:
            await publications.aclose()

    asyncio.run(exercise())


def test_a_transient_generic_precheck_refusal_still_allows_a_factory_turn(
    make_harness,
) -> None:
    async def exercise() -> None:
        publications = _HttpPrecheckApi(
            [(409, {"detail": {"code": "invalid_context"}}), (204, None)]
        )
        try:
            async with make_harness(
                binding=_Binding(),
                workspace_factory=_Workspace,
                approvals=_Approvals(),
                publication_creator=publications,
            ) as h:
                items = _WorkItems()
                h.kernel._work_items = items
                h.runner.default_script = [
                    Final(
                        text="Requesting approval.",
                        status=SessionStatus.AWAITING_APPROVAL,
                        approval_summary="Run the requested command",
                        approval_gate_kind="permission",
                        approval_granted_tool="Bash",
                    )
                ]

                await h.kernel.process_event(
                    _turn(f"work-item-{uuid.uuid4()}-execute-1", f"Resolve {ISSUE_URL}")
                )

                assert len(publications.mints) == 2
                assert len(h.runner.opened) == 1
                assert items.finishes == []
                assert items.calls.count("hold_for_approval") == 1
        finally:
            await publications.aclose()

    asyncio.run(exercise())


def test_existing_pr_refusal_finishes_approval_resume_without_a_new_runner_turn(
    make_harness, caplog: pytest.LogCaptureFixture
) -> None:
    async def exercise() -> None:
        publications = _HttpPrecheckApi([(204, None), _PR_NOT_ADOPTED_RESPONSE])
        try:
            async with make_harness(
                binding=_Binding(),
                workspace_factory=_Workspace,
                approvals=_Approvals(),
                publication_creator=publications,
            ) as h:
                items = _WorkItems()
                h.kernel._work_items = items
                h.runner.default_script = [
                    Final(
                        text="Requesting approval.",
                        status=SessionStatus.AWAITING_APPROVAL,
                        approval_summary="Run the requested command",
                        approval_gate_kind="permission",
                        approval_granted_tool="Bash",
                    )
                ]
                await h.kernel.process_event(
                    _turn(f"work-item-{uuid.uuid4()}-execute-1", f"Resolve {ISSUE_URL}")
                )
                assert len(publications.mints) == 1
                assert len(h.runner.opened) == 1
                assert items.finishes == []
                before = len(publications.mints)
                resumed = _turn(
                    f"approval-{uuid.uuid4()}-resolved",
                    "[approval resolved] approved",
                    placeholder="approval-placeholder",
                )

                await h.kernel.process_event(resumed)

                assert len(publications.mints) - before == 1
                assert len(h.runner.opened) == 1
                assert items.calls.count("finish") == 1
                assert items.finishes[0]["outcome"] == "failed"
                assert items.finishes[0]["cause"] == "pull_request_not_adopted"
                assert await h.kernel._markers.is_terminal(resumed.event_id)
                assert all("runner_escalated" not in text for text in _escalations(caplog))
                assert any("pull-request-not-adopted" in text for text in _escalations(caplog))
        finally:
            await publications.aclose()

    asyncio.run(exercise())


@pytest.mark.parametrize("completion_write_fails", [False, True])
def test_work_item_approval_resume_emits_no_requesting_turn_reply(
    make_harness, completion_write_fails: bool
) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            approvals=_Approvals(),
            publication_creator=_NoExistingPublication(),
        ) as h:
            work_items = _WorkItems()
            h.kernel._work_items = work_items
            request_id = uuid.uuid4()
            h.runner.default_script = [
                Final(
                    text="Requesting approval.",
                    status=SessionStatus.AWAITING_APPROVAL,
                    approval_summary="Run the requested publication",
                    approval_gate_kind="permission",
                    approval_granted_tool="Bash",
                )
            ]

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )
            assert h.sink.updates == []

            h.runner.default_script = [
                TextDelta(text="Resuming. "),
                Final(text="Resuming. Done.", status=SessionStatus.DONE),
            ]
            resumed = _turn(
                f"approval-{uuid.uuid4()}-resolved",
                "[approval resolved] approved",
                placeholder="approval-placeholder",
            )
            if completion_write_fails:

                async def corrupt_completion_after_finish() -> None:
                    await h.async_redis.set(h.config.completion_key(resumed.event_id), "wrong-type")

                work_items.after_finish = corrupt_completion_after_finish
                with pytest.raises(ResponseError, match="WRONGTYPE"):
                    await h.kernel.process_event(resumed)
                await h.kernel.notify_turn_not_started(resumed)
            else:
                await h.kernel.process_event(resumed)

            assert h.sink.updates == []
            assert work_items.calls.count("start") == 1
            assert work_items.calls.count("finish") == 1

    asyncio.run(exercise())


def test_github_work_item_reaches_the_model_without_chat_replies(make_harness) -> None:
    async def exercise() -> None:
        sink = build_reply_sink(WorkerConfig())
        github = _RecordingSink(sink._adapters["github"])
        sink._adapters["github"] = github
        try:
            async with make_harness(
                binding=_Binding(),
                workspace_factory=_Workspace,
                sink=sink,
                publication_creator=_NoExistingPublication(),
            ) as h:
                work_items = _WorkItems()
                h.kernel._work_items = work_items
                h.runner.default_script = [
                    TextDelta(text="Working. "),
                    Final(text="Working. Done.", status=SessionStatus.DONE),
                ]
                request_id = uuid.uuid4()

                await h.kernel.process_event(
                    _turn(
                        f"work-item-{request_id}-execute-1",
                        f"Resolve {ISSUE_URL}",
                        kind="github",
                    )
                )

                assert h.runner.opened
                assert "start" in work_items.calls
                assert "reply.update" not in github.events
        finally:
            await sink.aclose()

    asyncio.run(exercise())


def test_issue_url_in_ordinary_chat_selects_no_repository(make_harness) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
        ) as h:
            h.runner.default_script = [Final(text="Noted.", status=SessionStatus.DONE)]

            await h.kernel.process_event(
                _turn(f"slack-{uuid.uuid4()}", f"Please look at {ISSUE_URL}")
            )

            assert h.kernel._workspace.selections == [None]

    asyncio.run(exercise())


class _BarrierWorkItems(_WorkItems):
    """Holds every acquire until all concurrent executions have acquired."""

    def __init__(self, parties: int) -> None:
        super().__init__()
        self.barrier = asyncio.Barrier(parties)
        self.started: list[uuid.UUID] = []

    async def acquire(
        self,
        request_id: uuid.UUID,
        *,
        owner: str,
        generation: int,
        wake: FactoryWake,
        context: FactoryContext | None,
    ) -> WorkItemAcquireGrant:
        grant = await super().acquire(
            request_id, owner=owner, generation=generation, wake=wake, context=context
        )
        await self.barrier.wait()
        return grant

    async def start(self, request_id: uuid.UUID, **kwargs: object) -> WorkItemStartGrant:
        self.started.append(request_id)
        return await super().start(request_id, **kwargs)


def test_concurrent_work_items_each_start_their_own_request(make_harness) -> None:
    """#3069: a turn never starts under another execution's request."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
        ) as h:
            request_ids = [uuid.uuid4() for _ in range(3)]
            work_items = _BarrierWorkItems(len(request_ids))
            h.kernel._work_items = work_items
            h.runner.default_script = [
                Final(text="Working. Done.", status=SessionStatus.DONE),
            ]

            await asyncio.gather(
                *(
                    h.kernel.process_event(
                        _turn(
                            f"work-item-{request_id}-execute-1",
                            f"Resolve {ISSUE_URL}",
                            conversation_id=f"1700000000.00000{index}",
                        )
                    )
                    for index, request_id in enumerate(request_ids, start=2)
                )
            )

            assert work_items.calls.count("acquire") == len(request_ids)
            assert sorted(work_items.started) == sorted(request_ids)
            assert work_items.calls.count("finish") == len(request_ids)

    asyncio.run(exercise())


def test_credit_exhausted_escalation_finishes_with_its_cause_and_message(
    make_harness,
) -> None:
    """#3073: the issue names the real cause, not ``runner_escalated``."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
        ) as h:
            work_items = _WorkItems()
            h.kernel._work_items = work_items
            message = "model error: unknown: API Error: 402 This request requires more credits"
            h.runner.default_script = [
                ErrorEvent(message=message, classification="model-credit-exhausted"),
                Final(text="", status=SessionStatus.CLASSIFIED_FAILURE),
            ]
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            assert work_items.calls.count("finish") == 1
            finish = work_items.finishes[0]
            assert finish["outcome"] == "failed"
            assert finish["cause"] == "model_credit_exhausted"
            assert finish["detail"] == message

    asyncio.run(exercise())


def test_model_unreachable_on_every_attempt_finishes_with_its_cause_and_message(
    make_harness,
) -> None:
    """#4333: an outage longer than the retries names the cause, not ``unclassified``."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
            max_attempts=3,
        ) as h:
            work_items = _WorkItems()
            h.kernel._work_items = work_items
            message = "model error: server_error: API Error: Connection refused (ECONNREFUSED)"
            h.runner.default_script = [
                ErrorEvent(message=message, classification="model-unreachable"),
                Final(text="", status=SessionStatus.CLASSIFIED_FAILURE),
            ]
            request_id = uuid.uuid4()
            prompt = f"Resolve {ISSUE_URL}"

            await h.kernel.process_event(_turn(f"work-item-{request_id}-execute-1", prompt))

            # Retryable with no side effect: every attempt is spent first.
            assert h.runner.opened == [prompt, prompt, prompt]
            assert work_items.calls.count("finish") == 1
            finish = work_items.finishes[0]
            assert finish["outcome"] == "failed"
            assert finish["cause"] == "model_unreachable"
            assert isinstance(finish["detail"], str)
            assert "API Error: Connection refused (ECONNREFUSED)" in finish["detail"]

    asyncio.run(exercise())


def test_workspace_retry_fault_finishes_with_redacted_bounded_stage_and_reason(
    make_harness,
) -> None:
    """#4367: work-item finish consumes safe detail before reply formatting."""

    reason = (
        "API returned HTTP 500: -----BEGIN PRIVATE KEY-----\n"
        f"{'SYNTHETIC-PRIVATE-MATERIAL' * 30}\n-----END PRIVATE KEY----- "
        f"remaining diagnosis {'x' * 1000}"
    )

    class WorkspaceFaultOnRetry(_Workspace):
        def select_repository(self, **kwargs: object) -> object:
            selected = super().select_repository(**kwargs)
            if len(self.selections) > 1:
                raise WorkspacePreparationError("repository-selection", reason)
            return selected

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=WorkspaceFaultOnRetry,
            publication_creator=_NoExistingPublication(),
            max_attempts=3,
        ) as h:
            work_items = _WorkItems()
            h.kernel._work_items = work_items
            # Start the real execution before the workspace retry fails. A
            # fault before start defers the request and has no finish reader.
            h.runner.default_script = [
                ErrorEvent(message="retry this clean attempt", classification="runner-error"),
                Final(text="", status=SessionStatus.CLASSIFIED_FAILURE),
            ]
            request_id = uuid.uuid4()
            prompt = f"Resolve {ISSUE_URL}"

            await h.kernel.process_event(_turn(f"work-item-{request_id}-execute-1", prompt))

            assert h.runner.opened == [prompt]
            assert h.kernel._workspace.selections == [WORK_ITEM_REPO] * 3
            assert work_items.calls.count("start") == 1
            assert work_items.calls.count("finish") == 1
            finish = work_items.finishes[0]
            assert finish["outcome"] == "failed"
            assert finish["cause"] == "workspace_error"
            detail = finish["detail"]
            assert isinstance(detail, str)
            assert detail.startswith(
                "workspace repository-selection failed: API returned HTTP 500: "
                "[REDACTED:pem_private_key]"
            )
            assert len(detail) <= 300
            assert "remaining diagnosis" in detail
            assert "SYNTHETIC-PRIVATE-MATERIAL" not in detail
            assert "-----BEGIN PRIVATE KEY-----" not in detail
            assert "-----END PRIVATE KEY-----" not in detail

    asyncio.run(exercise())


class _DeferRecordingWorkItems(_WorkItems):
    """Records every ``defer`` with its full keyword set (#4930)."""

    def __init__(self) -> None:
        super().__init__()
        self.defers: list[tuple[uuid.UUID, dict[str, object]]] = []

    async def defer(self, request_id: uuid.UUID, **kwargs: object) -> str | None:
        self.calls.append("defer")
        self.defers.append((request_id, kwargs))
        return None


def test_pre_start_archive_fault_defers_with_its_redacted_bounded_stage_and_reason(
    make_harness,
) -> None:
    """#4930: an unstarted workspace fault carries its safe diagnostic to defer.

    The archive stage fails before ``start``; the request stays waiting, so the
    only durable reader of the cause is the deferral. The diagnostic is the
    same redacted, whitespace-collapsed, 300-char ``TurnOutcome.error_message``
    the #4367 finish path carries.
    """

    token = "ghp_" + "A" * 36
    reason = f"archive exceeds 1048576 bytes; token {token} {'x' * 1000}"

    class ArchiveTooLarge(_Workspace):
        def claim_or_resume_with_handle(self, **kwargs: object) -> object:
            self.claimed.append(kwargs.get("repo_full_name"))
            raise WorkspacePreparationError("archive-size", reason)

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=ArchiveTooLarge,
            publication_creator=_NoExistingPublication(),
            max_attempts=2,
        ) as h:
            work_items = _DeferRecordingWorkItems()
            h.kernel._work_items = work_items
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            assert h.kernel._workspace.claimed, work_items.calls
            assert h.runner.opened == []
            assert "start" not in work_items.calls
            assert work_items.finishes == []
            assert len(work_items.defers) == 1, work_items.calls
            deferred_id, defer = work_items.defers[0]
            assert deferred_id == request_id
            assert defer["reason"] == "not_started:classified_failure"
            assert defer["capacity"] is False
            detail = defer["start_failure_detail"]
            assert isinstance(detail, str)
            assert detail.startswith(
                "workspace archive-size failed: archive exceeds 1048576 bytes; token "
            )
            assert len(detail) <= 300
            assert "ghp_A" not in detail
            assert "[REDACTED:github_pat]" in detail

    asyncio.run(exercise())


def test_pre_start_non_workspace_failure_defers_without_a_diagnostic(make_harness) -> None:
    """#4930: only a ``workspace-error`` fault is a start-failure diagnostic.

    A sandbox fault before start (here the claim itself raises) still defers
    ``not_started:``, with an explicit ``None`` detail on the wire.
    """

    class ClaimFails(_Workspace):
        def claim_or_resume_with_handle(self, **kwargs: object) -> object:
            self.claimed.append(kwargs.get("repo_full_name"))
            raise SandboxError("sandbox claim failed: example control plane fault")

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=ClaimFails,
            publication_creator=_NoExistingPublication(),
            max_attempts=2,
        ) as h:
            work_items = _DeferRecordingWorkItems()
            h.kernel._work_items = work_items
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            assert h.kernel._workspace.claimed, work_items.calls
            assert "start" not in work_items.calls
            assert len(work_items.defers) == 1, work_items.calls
            _deferred_id, defer = work_items.defers[0]
            assert str(defer["reason"]).startswith("not_started:")
            assert defer["capacity"] is False
            assert "start_failure_detail" in defer
            assert defer["start_failure_detail"] is None

    asyncio.run(exercise())


class _PublicationPendingWorkItems(_WorkItems):
    """The publication loop, not this finish, owns the WorkItem terminus."""

    async def finish(self, _request_id: uuid.UUID, **_: object) -> None:
        self.calls.append("finish")
        raise WorkItemConflict("publication_pending")


@pytest.mark.parametrize("publication_pending", [False, True])
def test_finished_work_item_deletes_its_sandbox_claim(
    make_harness, publication_pending: bool
) -> None:
    """A WorkItem run that settles terminally leaves no claim or route behind (#3075)."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
        ) as h:
            work_items = _PublicationPendingWorkItems() if publication_pending else _WorkItems()
            h.kernel._work_items = work_items
            h.runner.default_script = [
                TextDelta(text="Working. "),
                Final(text="Working. Done.", status=SessionStatus.DONE),
            ]
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            assert "finish" in work_items.calls
            assert h.fake_k8s.deleted_claims
            assert h.fake_k8s.claims == {}

    asyncio.run(exercise())


class _StartRefusedWorkItems(_WorkItems):
    """The API refuses ``start``: the request settled before the turn could open."""

    def __init__(self, code: str) -> None:
        super().__init__()
        self.code = code

    async def start(self, _request_id: uuid.UUID, **_: object) -> WorkItemStartGrant:
        self.calls.append("start")
        raise WorkItemStartRefused(self.code)


class _FinishRefusedWorkItems(_WorkItems):
    """The label came off mid-turn: ``finish`` is refused ``work_item_cancelled``."""

    async def finish(self, _request_id: uuid.UUID, **_: object) -> None:
        self.calls.append("finish")
        raise WorkItemConflict("work_item_cancelled")


@pytest.mark.parametrize(
    "code",
    ["work_item_cancelled", "waiting_deadline_elapsed", "not_found"],
)
def test_start_refused_on_a_terminal_code_releases_the_claimed_sandbox(
    make_harness, code: str
) -> None:
    """#3208: a start refusal on a terminal code ends the execution, so the
    claim the delivery just made must not hold quota until the route TTL lapses.
    A request cancelled from ``waiting`` gets no terminate wake and carries no
    teardown flag, so the worker is the only one that can release it."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
        ) as h:
            work_items = _StartRefusedWorkItems(code)
            h.kernel._work_items = work_items
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            assert "start" in work_items.calls
            assert h.fake_k8s.deleted_claims
            assert h.fake_k8s.claims == {}
            assert h.substrate._affinity.get(_thread_key()) is None

    asyncio.run(exercise())


def test_start_refused_not_dispatchable_keeps_its_sandbox_claim(make_harness) -> None:
    """#3208's allowlist has an opposite: ``not_dispatchable`` can mean a lapsed
    acquire lease, where a replacement re-acquires the same generation and
    adopts this thread's route, so the refused delivery must leave the claim
    standing rather than yank it out from under the next owner."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
        ) as h:
            work_items = _StartRefusedWorkItems("not_dispatchable")
            h.kernel._work_items = work_items
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            assert "start" in work_items.calls
            assert h.fake_k8s.deleted_claims == []
            assert len(h.fake_k8s.claims) == 1
            assert h.substrate._affinity.get(_thread_key()) is not None

    asyncio.run(exercise())


def test_finish_refused_as_cancelled_releases_the_sandbox_claim(make_harness) -> None:
    """#3208: the label came off mid-turn, so the request is already settled
    ``cancelled`` and ``finish`` is refused. The run must still count as
    settled locally so the delivery releases its claim instead of holding
    quota until the terminate-wake backstop catches up."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
        ) as h:
            work_items = _FinishRefusedWorkItems()
            h.kernel._work_items = work_items
            h.runner.default_script = [
                TextDelta(text="Working. "),
                Final(text="Working. Done.", status=SessionStatus.DONE),
            ]
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            assert "finish" in work_items.calls
            assert h.fake_k8s.deleted_claims
            assert h.fake_k8s.claims == {}
            assert h.substrate._affinity.get(_thread_key()) is None

    asyncio.run(exercise())


# --- #4191: a publication refused because the run was cancelled settles ---------

PUBLISH_TOOL = "mcp__curie__publish_changes"
_CANCELLED_PUBLICATION_BODY = {
    "detail": {
        "code": "publication.work_item_cancelled",
        "message": "the work item request was cancelled",
    }
}
# A FastAPI request-validation 422: the detail is a list, not a coded dict.
_UNPROCESSABLE_PUBLICATION_BODY = {
    "detail": [{"loc": ["body", "base_sha"], "msg": "field required", "type": "missing"}]
}


class _HttpPublicationApi(_NoExistingPublication):
    """Publication creator whose create goes through the real ``ApprovalClient``.

    The API answer is served by an ``httpx.MockTransport``, so the
    ``ApprovalBackendError`` and its ``refusal`` are built by
    ``ApprovalClient.create_publication`` in curie_worker/approvals.py exactly as
    a live 409 or 422 builds them.
    """

    def __init__(self, status: int, body: dict[str, object]) -> None:
        self.creates = 0
        self._http = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _req: httpx.Response(status, json=body))
        )
        self._client = ApprovalClient(
            api_base_url="http://api.example.test",
            api_key="",
            client=self._http,
            read_timeout_s=5.0,
            worker_token="example-worker-token",
        )

    async def create_publication(
        self, request: PublicationCreateRequest, *, budget_s: float = 120
    ) -> object:
        self.creates += 1
        return await self._client.create_publication(request, budget_s=budget_s)

    async def aclose(self) -> None:
        await self._http.aclose()


class _CodedFinishRefusal(_WorkItems):
    """``finish`` is refused with ``code``; the claims live at that moment are kept."""

    def __init__(self, code: str, fake_k8s: object) -> None:
        super().__init__()
        self.code = code
        self.fake_k8s = fake_k8s
        self.claims_at_finish: list[str] = []

    async def finish(self, _request_id: uuid.UUID, **kwargs: object) -> None:
        self.calls.append("finish")
        self.finishes.append(kwargs)
        self.claims_at_finish = list(self.fake_k8s.claims)  # type: ignore[attr-defined]
        raise WorkItemConflict(self.code)


def _patch_publication_snapshot(h: object, monkeypatch: pytest.MonkeyPatch) -> None:
    async def snapshot(*_args: object, **_kwargs: object) -> RunnerWorkspaceSnapshot:
        return RunnerWorkspaceSnapshot(
            repo_full_name=WORK_ITEM_REPO,
            base_sha="a1" * 20,
            patch=b"diff --git a/src/widget.py b/src/widget.py\n",
            changed_paths=("src/widget.py",),
            contains_workflow_files=False,
            publication_title="Fix the widget parser",
            publication_body="Fixes the parser.",
        )

    monkeypatch.setattr(h.kernel._runner, "snapshot", snapshot)  # type: ignore[attr-defined]
    monkeypatch.setattr(
        "curie_worker.kernel.attempt.validate_snapshot_against_base",
        lambda *_args, **_kwargs: None,
    )


_PUBLISH_TURN = [
    ToolNote(text=f"running tool {PUBLISH_TOOL}", tool=PUBLISH_TOOL),
    Final(
        text="Ready to publish",
        status=SessionStatus.AWAITING_APPROVAL,
        approval_summary="Publish the change",
        approval_gate_kind="permission",
        approval_granted_tool=PUBLISH_TOOL,
    ),
]


def _escalations(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if "escalating event" in r.getMessage()]


def test_publication_refused_as_cancelled_settles_and_releases_the_claim(
    make_harness, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """#4191 AC1: the API refuses the publication 409 work_item_cancelled and then
    refuses the approval_create_failed finish the same way. The delivery settles
    on this pass, releases the run's claim, and flags nobody."""

    caplog.set_level("INFO", logger="curie_worker.kernel")

    async def exercise() -> None:
        publications = _HttpPublicationApi(409, _CANCELLED_PUBLICATION_BODY)
        try:
            async with make_harness(
                binding=_Binding(),
                workspace_factory=_Workspace,
                publication_creator=publications,
            ) as h:
                work_items = _CodedFinishRefusal("work_item_cancelled", h.fake_k8s)
                h.kernel._work_items = work_items
                _patch_publication_snapshot(h, monkeypatch)
                h.runner.turn_scripts = [list(_PUBLISH_TURN)]
                h.runner.default_script = [Final(text="must not run", status=SessionStatus.DONE)]
                request_id = uuid.uuid4()

                await h.kernel.process_event(
                    _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
                )

                assert publications.creates == 1
                assert work_items.calls.count("finish") == 1
                assert work_items.finishes[0]["cause"] == "approval_create_failed"
                assert len(work_items.claims_at_finish) == 1
                assert work_items.claims_at_finish[0] in h.fake_k8s.deleted_claims
                assert h.fake_k8s.claims == {}
                assert _escalations(caplog) == []
                assert any(
                    "approval create refused for cancelled work item" in r.getMessage()
                    for r in caplog.records
                )
        finally:
            await publications.aclose()

    asyncio.run(exercise())


def test_publication_refused_as_cancelled_still_raises_on_a_stale_owner_finish(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#4191 AC2: only work_item_cancelled and publication_pending settle the
    approval_create_failed finish; any other refusal still leaves the entry."""

    async def exercise() -> None:
        publications = _HttpPublicationApi(409, _CANCELLED_PUBLICATION_BODY)
        try:
            async with make_harness(
                binding=_Binding(),
                workspace_factory=_Workspace,
                publication_creator=publications,
            ) as h:
                work_items = _CodedFinishRefusal("stale_owner", h.fake_k8s)
                h.kernel._work_items = work_items
                _patch_publication_snapshot(h, monkeypatch)
                h.runner.turn_scripts = [list(_PUBLISH_TURN)]
                h.runner.default_script = [Final(text="must not run", status=SessionStatus.DONE)]

                with pytest.raises(WorkItemConflict) as raised:
                    await h.kernel.process_event(
                        _turn(f"work-item-{uuid.uuid4()}-execute-1", f"Resolve {ISSUE_URL}")
                    )

                assert raised.value.code == "stale_owner"
                assert work_items.calls.count("finish") == 1
        finally:
            await publications.aclose()

    asyncio.run(exercise())


def test_publication_unprocessable_still_escalates_approval_create_failed(
    make_harness, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """#4191 AC2: a 422 publication create is a real failure, still flagged."""

    caplog.set_level("INFO", logger="curie_worker.kernel")

    async def exercise() -> None:
        publications = _HttpPublicationApi(422, _UNPROCESSABLE_PUBLICATION_BODY)
        try:
            async with make_harness(
                binding=_Binding(),
                workspace_factory=_Workspace,
                publication_creator=publications,
            ) as h:
                work_items = _WorkItems()
                h.kernel._work_items = work_items
                _patch_publication_snapshot(h, monkeypatch)
                h.runner.turn_scripts = [list(_PUBLISH_TURN)]
                h.runner.default_script = [Final(text="must not run", status=SessionStatus.DONE)]

                await h.kernel.process_event(
                    _turn(f"work-item-{uuid.uuid4()}-execute-1", f"Resolve {ISSUE_URL}")
                )

                assert publications.creates == 1
                assert [f["cause"] for f in work_items.finishes] == ["approval_create_failed"]
                escalations = _escalations(caplog)
                assert escalations, "a 422 publication create must still be flagged"
                assert any("approval-create-failed" in text for text in escalations)
        finally:
            await publications.aclose()

    asyncio.run(exercise())


def test_approval_hold_keeps_its_sandbox_claim(make_harness) -> None:
    """Awaiting approval is not a terminus: the resume turn still needs the route."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            approvals=_Approvals(),
            publication_creator=_NoExistingPublication(),
        ) as h:
            h.kernel._work_items = _WorkItems()
            h.runner.default_script = [
                Final(
                    text="Requesting approval.",
                    status=SessionStatus.AWAITING_APPROVAL,
                    approval_summary="Run the requested publication",
                    approval_gate_kind="permission",
                    approval_granted_tool="Bash",
                )
            ]
            await h.kernel.process_event(
                _turn(f"work-item-{uuid.uuid4()}-execute-1", f"Resolve {ISSUE_URL}")
            )

            assert len(h.fake_k8s.claims) == 1
            assert h.fake_k8s.deleted_claims == []

    asyncio.run(exercise())


@pytest.mark.parametrize("path", ["owner_stop", "reconciler_terminate"])
def test_cancelled_work_item_deletes_its_suspended_sandbox_claim(make_harness, path: str) -> None:
    """A run held for approval idles into a SUSPENDED route, then is cancelled.
    Both termination paths delete the suspended claim and drop the route, so
    the orphan reaper's routed-claim skip cannot strand it (#3075)."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            approvals=_Approvals(),
            publication_creator=_NoExistingPublication(),
        ) as h:
            work_items = _WorkItems()
            h.kernel._work_items = work_items
            h.runner.default_script = [
                Final(
                    text="Requesting approval.",
                    status=SessionStatus.AWAITING_APPROVAL,
                    approval_summary="Run the requested publication",
                    approval_gate_kind="permission",
                    approval_granted_tool="Bash",
                )
            ]
            request_id = uuid.uuid4()
            execute = _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            await h.kernel.process_event(execute)
            [(thread_key, run)] = list(h.kernel._held_work_items.items())
            await asyncio.to_thread(h.substrate.suspend, thread_key, history_ref="hist-1")
            assert len(h.fake_k8s.claims) == 1

            if path == "owner_stop":
                await h.kernel._stop_owned_work_item(thread_key, run)
            else:

                async def get_request(_request_id: uuid.UUID) -> object:
                    return SimpleNamespace(
                        runtime_claim_name=run.claim_name,
                        runtime_sandbox_name=run.sandbox_name,
                    )

                work_items.get_request = get_request  # type: ignore[attr-defined]
                await h.kernel._terminate_work_item(execute, request_id)

            assert "record_termination" in work_items.calls
            assert h.fake_k8s.claims == {}
            assert h.substrate._affinity.get(thread_key) is None
            # Termination ends the held run too, so the sweeper may reclaim it
            # (#3564).
            assert h.kernel._held_work_items == {}
            assert h.kernel.owns_work_item(request_id) is False

    asyncio.run(exercise())


def test_work_item_boot_env_carries_the_configured_turn_budget(make_harness) -> None:
    """#3071: a factory execution runs with the worker's work-item turn budget,
    not the runner's short chat default."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            work_item_max_turns=5,
            publication_creator=_NoExistingPublication(),
        ) as h:
            h.kernel._work_items = _WorkItems()
            h.runner.default_script = [Final(text="Done.", status=SessionStatus.DONE)]
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            envs = h.fake_k8s.claim_envs
            assert len(envs) == 1 and envs[0] is not None
            assert envs[0].get("CURIE_MAX_TURNS") == "5"

    asyncio.run(exercise())


def test_ordinary_chat_boot_env_carries_no_turn_budget(make_harness) -> None:
    """#3071: only a work-item delivery raises the turn budget; chat keeps the
    runner default by carrying no CURIE_MAX_TURNS at all."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            work_item_max_turns=5,
            publication_creator=_NoExistingPublication(),
        ) as h:
            h.runner.default_script = [Final(text="Noted.", status=SessionStatus.DONE)]

            await h.kernel.process_event(
                _turn(f"slack-{uuid.uuid4()}", f"Please look at {ISSUE_URL}")
            )

            envs = h.fake_k8s.claim_envs
            assert len(envs) == 1 and envs[0] is not None
            assert "CURIE_MAX_TURNS" not in envs[0]

    asyncio.run(exercise())


def _max_turns_script() -> list:
    return [
        ErrorEvent(message="reached max turns", classification="max-turns"),
        Final(text="f", status=SessionStatus.CLASSIFIED_FAILURE),
    ]


def test_work_item_max_turns_escalation_names_the_work_item_budget(
    make_harness, caplog: pytest.LogCaptureFixture
) -> None:
    """#3403: a work item that exhausts its budget names the work-item setting
    and the value it actually ran under."""

    caplog.set_level("WARNING", logger="curie_worker.kernel")

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            work_item_max_turns=5,
            publication_creator=_NoExistingPublication(),
        ) as h:
            h.kernel._work_items = _WorkItems()
            h.runner.default_script = _max_turns_script()
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            # A work item has no chat sink; the escalation is the kernel's
            # escalation record.
            text = " ".join(
                r.getMessage() for r in caplog.records if "escalating event" in r.getMessage()
            )
            assert "max-turns" in text, text
            assert "CURIE_WORK_ITEM_MAX_TURNS, currently 5" in text, text
            assert "through runner.extraEnv" not in text, text

    asyncio.run(exercise())


def test_chat_max_turns_escalation_names_the_runner_budget(make_harness) -> None:
    """#3403: a chat delivery runs under the runner's CURIE_MAX_TURNS, so its
    escalation must name that setting, not the work-item budget it never had."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            work_item_max_turns=5,
            publication_creator=_NoExistingPublication(),
        ) as h:
            h.runner.default_script = _max_turns_script()

            await h.kernel.process_event(
                _turn(f"slack-{uuid.uuid4()}", f"Please look at {ISSUE_URL}")
            )

            text = h.sink.last_text
            assert text is not None and "max-turns" in text, text
            assert "raise CURIE_MAX_TURNS through runner.extraEnv" in text, text
            assert "runner default 20 when unset" in text, text
            assert "CURIE_WORK_ITEM_MAX_TURNS" not in text, text
            assert "currently 5" not in text, text

    asyncio.run(exercise())


def test_work_item_replaces_a_chat_sandbox_booted_without_its_turn_budget(
    make_harness,
) -> None:
    """#3071: CURIE_MAX_TURNS binds at boot, so a work item on a thread whose
    live sandbox was claimed by chat gets a fresh runner carrying its budget."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            work_item_max_turns=5,
            publication_creator=_NoExistingPublication(),
        ) as h:
            h.kernel._work_items = _WorkItems()
            h.runner.default_script = [Final(text="Done.", status=SessionStatus.DONE)]

            await h.kernel.process_event(_turn(f"slack-{uuid.uuid4()}", "hello there"))
            await h.kernel.process_event(
                _turn(f"work-item-{uuid.uuid4()}-execute-1", f"Resolve {ISSUE_URL}")
            )

            envs = h.fake_k8s.claim_envs
            assert len(envs) == 2
            assert "CURIE_MAX_TURNS" not in (envs[0] or {})
            assert (envs[1] or {}).get("CURIE_MAX_TURNS") == "5"

    asyncio.run(exercise())


def test_chat_replaces_a_work_item_sandbox_booted_with_the_factory_budget(
    make_harness,
) -> None:
    """#3071: an ordinary turn after a work item on the same thread must not
    inherit the factory turn budget from the reused sandbox."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            work_item_max_turns=5,
            publication_creator=_NoExistingPublication(),
        ) as h:
            h.kernel._work_items = _WorkItems()
            h.runner.default_script = [Final(text="Done.", status=SessionStatus.DONE)]

            await h.kernel.process_event(
                _turn(f"work-item-{uuid.uuid4()}-execute-1", f"Resolve {ISSUE_URL}")
            )
            await h.kernel.process_event(_turn(f"slack-{uuid.uuid4()}", "hello there"))

            envs = h.fake_k8s.claim_envs
            assert len(envs) == 2
            assert (envs[0] or {}).get("CURIE_MAX_TURNS") == "5"
            assert "CURIE_MAX_TURNS" not in (envs[1] or {})

    asyncio.run(exercise())


def _fail_settled_release(h: object) -> None:
    """Make the settled work item's best-effort sandbox release fail (#3075).

    A settled work item normally deletes its claim, so the only way its live,
    factory-budget sandbox survives to meet the next delivery on the thread is
    a failed release, which the kernel logs and tolerates. That surviving
    route is exactly what the #3071 turn budget fence guards.
    """

    def release_if_claim(_thread_key: str, _claim_name: str) -> bool:
        raise RuntimeError("control plane unavailable")

    h.substrate.release_if_claim = release_if_claim  # type: ignore[attr-defined]


def test_consecutive_work_items_replace_the_sandbox_even_with_the_same_budget(
    make_harness,
) -> None:
    """ADR 0178: two work items are two runs, so a matching turn budget does
    not let the second adopt the first's runner. The first release fails, the
    live sandbox survives, and the next run still claims a fresh one."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            work_item_max_turns=5,
            publication_creator=_NoExistingPublication(),
        ) as h:
            h.kernel._work_items = _WorkItems()
            h.runner.default_script = [Final(text="Done.", status=SessionStatus.DONE)]
            _fail_settled_release(h)

            for _ in range(2):
                await h.kernel.process_event(
                    _turn(f"work-item-{uuid.uuid4()}-execute-1", f"Resolve {ISSUE_URL}")
                )

            envs = h.fake_k8s.claim_envs
            assert len(envs) == 2
            assert (envs[0] or {}).get("CURIE_MAX_TURNS") == "5"
            assert (envs[1] or {}).get("CURIE_MAX_TURNS") == "5"
            # Each execution ends without publishing, so each gets its one
            # continuation turn (#3128) on the runner booted for that run.
            assert len(h.runner.opened) == 4
            assert h.runner.opened[0] == f"Resolve {ISSUE_URL}"
            assert h.runner.opened[2] == f"Resolve {ISSUE_URL}"

    asyncio.run(exercise())


def test_chat_steers_a_live_work_item_turn_instead_of_replacing_it(make_harness) -> None:
    """#3071: the turn budget fence applies only to a new turn. A message that
    arrives while the work item turn is live steers it (one live session)."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            work_item_max_turns=5,
            publication_creator=_NoExistingPublication(),
        ) as h:
            h.kernel._work_items = _WorkItems()
            h.runner.default_script = [Final(text="Done.", status=SessionStatus.DONE)]
            _fail_settled_release(h)

            await h.kernel.process_event(
                _turn(f"work-item-{uuid.uuid4()}-execute-1", f"Resolve {ISSUE_URL}")
            )
            assert len(h.fake_k8s.claims) == 1
            h.runner.turn_active = True
            await h.kernel.process_event(_turn(f"slack-{uuid.uuid4()}", "hello there"))

            assert h.runner.steers == ["hello there"]
            assert len(h.fake_k8s.claim_envs) == 1

    asyncio.run(exercise())


def test_chat_never_opens_a_turn_on_a_runner_with_the_factory_budget(
    make_harness,
) -> None:
    """#3071 finish race: liveness said busy, but the work item turn ended
    before the steer (409). The chat turn must not open on the old runner with
    the factory budget; it is retried and the retry replaces the runner."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            work_item_max_turns=5,
            publication_creator=_NoExistingPublication(),
        ) as h:
            h.kernel._work_items = _WorkItems()
            h.runner.default_script = [Final(text="Done.", status=SessionStatus.DONE)]
            _fail_settled_release(h)

            await h.kernel.process_event(
                _turn(f"work-item-{uuid.uuid4()}-execute-1", f"Resolve {ISSUE_URL}")
            )
            assert len(h.fake_k8s.claims) == 1
            opened_before = list(h.runner.opened)

            async def active_then_finished(*_args: object, **_kwargs: object) -> bool:
                h.runner.turn_active = False  # the turn ends before the steer lands
                return True

            h.kernel._turn_active = active_then_finished  # type: ignore[method-assign]
            chat = _turn(f"slack-{uuid.uuid4()}", "hello there")
            with pytest.raises(ThreadBusyError):
                await h.kernel.process_event(chat)

            assert h.runner.opened == opened_before
            assert len(h.fake_k8s.claim_envs) == 1

            del h.kernel._turn_active
            await h.kernel.process_event(chat)
            envs = h.fake_k8s.claim_envs
            assert len(envs) == 2
            assert "CURIE_MAX_TURNS" not in (envs[1] or {})

    asyncio.run(exercise())


# --- #3076: a restarted worker tears down an orphan it never ran -----------


class _OrphanWorkItems:
    """Dispatch double for a request a previous incarnation started."""

    CLAIM = "curie-thread-orphan-claim"
    SANDBOX = "sbx-curie-thread-orphan-claim"

    def __init__(self) -> None:
        self.claimed: list[tuple[uuid.UUID, str]] = []
        self.recorded: list[tuple[uuid.UUID, int, str]] = []

    async def claim_termination(self, request_id: uuid.UUID, *, owner: str) -> int:
        self.claimed.append((request_id, owner))
        return 7

    async def get_request(self, _request_id: uuid.UUID) -> object:
        return SimpleNamespace(
            status="cancellation_requested",
            runtime_epoch=7,
            runtime_claim_name=self.CLAIM,
            runtime_sandbox_name=self.SANDBOX,
        )

    async def record_termination(
        self, request_id: uuid.UUID, *, runtime_epoch: int, observation: str
    ) -> None:
        self.recorded.append((request_id, runtime_epoch, observation))


def test_terminate_wake_for_an_orphan_tears_down_its_stored_claim(make_harness) -> None:
    from curie_worker.workitem_dispatch import TerminationObservation

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
        ) as h:
            work_items = _OrphanWorkItems()
            h.kernel._work_items = work_items
            terminated: list[dict[str, object]] = []

            def terminate_thread(thread_key: str, **kwargs: object) -> object:
                terminated.append(kwargs)
                return TerminationObservation(
                    claims=(str(kwargs["claim_name"]),),
                    sandboxes=(str(kwargs["sandbox_name"]),),
                    observed_at=datetime.now(UTC),
                    observer=str(kwargs["observer"]),
                )

            h.substrate.terminate_thread = terminate_thread  # type: ignore[method-assign]
            request_id = uuid.uuid4()

            await h.kernel.process_event(_turn(f"work-item-{request_id}-terminate", "terminate"))

            assert [c[0] for c in work_items.claimed] == [request_id]
            assert len(terminated) == 1
            assert terminated[0]["claim_name"] == _OrphanWorkItems.CLAIM
            assert terminated[0]["sandbox_name"] == _OrphanWorkItems.SANDBOX
            assert [(r[0], r[1]) for r in work_items.recorded] == [(request_id, 7)]
            assert _OrphanWorkItems.CLAIM in work_items.recorded[0][2]

    asyncio.run(exercise())


def test_owns_work_item_tracks_live_and_held_runs(make_harness) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            approvals=_Approvals(),
            publication_creator=_NoExistingPublication(),
        ) as h:
            work_items = _WorkItems()
            h.kernel._work_items = work_items
            running = uuid.uuid4()
            seen_during_run: list[bool] = []

            async def observe() -> None:
                seen_during_run.append(h.kernel.owns_work_item(running))

            work_items.after_finish = observe
            h.runner.default_script = [Final(text="Done.", status=SessionStatus.DONE)]
            assert h.kernel.owns_work_item(running) is False
            await h.kernel.process_event(
                _turn(f"work-item-{running}-execute-1", f"Resolve {ISSUE_URL}")
            )
            assert seen_during_run == [True]
            assert h.kernel.owns_work_item(running) is False

            work_items.after_finish = None
            h.runner.default_script = [
                Final(
                    text="Requesting approval.",
                    status=SessionStatus.AWAITING_APPROVAL,
                    approval_summary="Run the requested publication",
                    approval_gate_kind="permission",
                    approval_granted_tool="Bash",
                )
            ]
            held = uuid.uuid4()
            await h.kernel.process_event(
                _turn(
                    f"work-item-{held}-execute-1",
                    f"Resolve {ISSUE_URL}",
                    conversation_id="1700000000.000002",
                )
            )
            assert h.kernel._held_work_items
            assert h.kernel.owns_work_item(held) is True
            assert h.kernel.owns_work_item(uuid.uuid4()) is False

    asyncio.run(exercise())


# --- #3564: a held run never outlives its deadline or its end -------------


_APPROVAL_FINAL = Final(
    text="Requesting approval.",
    status=SessionStatus.AWAITING_APPROVAL,
    approval_summary="Run the requested publication",
    approval_gate_kind="permission",
    approval_granted_tool="Bash",
)


async def _park_for_approval(h: object) -> tuple[uuid.UUID, str]:
    """Run one execute wake that parks for approval; return its request and thread."""

    h.kernel._work_items = _WorkItems()  # type: ignore[attr-defined]
    h.runner.default_script = [_APPROVAL_FINAL]  # type: ignore[attr-defined]
    request_id = uuid.uuid4()
    await h.kernel.process_event(  # type: ignore[attr-defined]
        _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
    )
    [thread_key] = list(h.kernel._held_work_items)  # type: ignore[attr-defined]
    assert h.kernel.owns_work_item(request_id) is True  # type: ignore[attr-defined]
    return request_id, thread_key


class _OwnerRows:
    """Orphan-sweeper client double listing one runtime-owner row."""

    def __init__(self, request_id: uuid.UUID, owner: str) -> None:
        self.rows = [SimpleNamespace(request_id=request_id, runtime_owner=owner, runtime_epoch=1)]
        self.declared: list[tuple[uuid.UUID, str, int]] = []

    async def runtime_owners(self, after: uuid.UUID | None = None) -> list[SimpleNamespace]:
        return [] if after is not None else self.rows

    async def declare_owner_lost(
        self, request_id: uuid.UUID, *, owner: str, runtime_epoch: int
    ) -> None:
        self.declared.append((request_id, owner, runtime_epoch))


@pytest.mark.parametrize("expired", [True, False])
def test_orphan_sweeper_reclaims_a_held_run_only_past_its_deadline(
    make_harness, expired: bool
) -> None:
    """A continuation that never arrives must not pin the request forever: past
    the execution deadline the held run is evicted and the sweeper declares it
    lost. Before the deadline it stays ours (#3564)."""

    from curie_worker.workitem_orphans import WorkItemOrphanSweeper

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            approvals=_Approvals(),
            publication_creator=_NoExistingPublication(),
        ) as h:
            request_id, thread_key = await _park_for_approval(h)
            held = h.kernel._held_work_items[thread_key]
            offset = timedelta(seconds=-1) if expired else timedelta(hours=1)
            held.execution_deadline = datetime.now(UTC) + offset
            self_name = h.kernel._config.consumer_name
            client = _OwnerRows(request_id, self_name)

            sweeper = WorkItemOrphanSweeper(
                client,
                ThreadLockOwnerLiveness(
                    ConsumerLivenessStore(h.async_redis),
                    stream=h.config.stream,
                    group=h.config.consumer_group,
                ),
                self_name=self_name,
                locally_owned=h.kernel.owns_work_item,
                absence_proof_s=60.0,
                interval_s=60.0,
            )

            declared = await sweeper.sweep()

            if expired:
                assert declared == 1
                assert client.declared == [(request_id, self_name, 1)]
                assert h.kernel._held_work_items == {}
            else:
                assert declared == 0
                assert client.declared == []
                assert h.kernel.owns_work_item(request_id) is True

    asyncio.run(exercise())


def test_kill_drops_the_held_run_of_that_agent_only(make_harness) -> None:
    """A kill ends a run parked for approval as well as live turns (#3564)."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            approvals=_Approvals(),
            publication_creator=_NoExistingPublication(),
        ) as h:
            request_id, _thread_key = await _park_for_approval(h)

            await h.kernel.interrupt_agent(uuid.uuid4())
            assert h.kernel.owns_work_item(request_id) is True

            await h.kernel.interrupt_agent(AGENT_ID)
            assert h.kernel.owns_work_item(request_id) is False

    asyncio.run(exercise())


def test_operator_release_drops_the_held_run_on_that_thread(make_harness) -> None:
    """An operator release of the thread ends its parked run (#3564)."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            approvals=_Approvals(),
            publication_creator=_NoExistingPublication(),
        ) as h:
            request_id, thread_key = await _park_for_approval(h)

            await h.kernel.release_thread("some-other-thread")
            assert h.kernel.owns_work_item(request_id) is True

            await h.kernel.release_thread(thread_key)
            assert h.kernel.owns_work_item(request_id) is False

    asyncio.run(exercise())


# --- ADR-0168 decision 7: a runner booted without a caller token -----------


class _CallerKeyBinding(_Binding):
    """Boots carry a caller token once the install holds a caller key."""

    def __init__(self, *, keyed: bool) -> None:
        self.keyed = keyed

    def boot_env(self, resolved: object, thread_key: str, **kwargs: object) -> dict[str, str]:
        env = super().boot_env(resolved, thread_key, **kwargs)
        if self.keyed:
            env["CURIE_CONNECTOR_CALLER_TOKEN"] = "cct.payload.signature"
        return env


# @spec ADR-0168 d7
def test_a_runner_booted_before_the_caller_key_is_replaced_on_its_next_turn(
    make_harness,
) -> None:
    """Every hosted connector's proxy refuses a runner with no caller token,
    and the route TTL slides on every turn, so waiting it out is no bound."""

    async def exercise() -> None:
        binding = _CallerKeyBinding(keyed=False)
        async with make_harness(
            binding=binding,
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
        ) as h:
            h.runner.default_script = [Final(text="Noted.", status=SessionStatus.DONE)]

            await h.kernel.process_event(_turn(f"slack-{uuid.uuid4()}", "hello there"))
            binding.keyed = True
            await h.kernel.process_event(_turn(f"slack-{uuid.uuid4()}", "and again"))

            envs = h.fake_k8s.claim_envs
            assert len(envs) == 2
            assert "CURIE_CONNECTOR_CALLER_TOKEN" not in (envs[0] or {})
            assert (envs[1] or {}).get("CURIE_CONNECTOR_CALLER_TOKEN") == "cct.payload.signature"

    asyncio.run(exercise())


# @spec ADR-0168 d7
@pytest.mark.parametrize(("first", "then"), [(True, True), (True, False), (False, False)])
def test_a_runner_whose_caller_token_still_fits_is_adopted(
    make_harness, first: bool, then: bool
) -> None:
    """Only a missing token forces a fresh runner. A runner that carries one
    keeps working after the key is removed, because no proxy is rendered then."""

    async def exercise() -> None:
        binding = _CallerKeyBinding(keyed=first)
        async with make_harness(
            binding=binding,
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
        ) as h:
            h.runner.default_script = [Final(text="Noted.", status=SessionStatus.DONE)]

            await h.kernel.process_event(_turn(f"slack-{uuid.uuid4()}", "hello there"))
            binding.keyed = then
            await h.kernel.process_event(_turn(f"slack-{uuid.uuid4()}", "and again"))

            assert len(h.fake_k8s.claim_envs) == 1

    asyncio.run(exercise())


def test_usage_limited_factory_run_finishes_with_its_cause_without_retry(
    make_harness,
) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
        ) as h:
            work_items = _WorkItems()
            h.kernel._work_items = work_items
            message = "You've hit your session limit · resets 3pm (UTC)"
            h.runner.default_script = [
                ErrorEvent(message=message, classification="model-usage-limited"),
                Final(text="", status=SessionStatus.CLASSIFIED_FAILURE),
            ]
            request_id = uuid.uuid4()
            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )
            assert len(h.runner.opened) == 1, "a subscription reset window must not be retried"
            assert work_items.calls.count("finish") == 1
            assert work_items.finishes[0]["outcome"] == "failed"
            assert work_items.finishes[0]["cause"] == "model_usage_limited"
            assert work_items.finishes[0]["detail"] == message

    asyncio.run(exercise())


async def _park_factory_request(
    h: Any,
    request_id: uuid.UUID,
    *,
    kind: str = "slack",
    conversation_id: str = "1700000000.000001",
) -> WorkItemRun:
    h.runner.default_script = [
        Final(
            text="Requesting approval.", status=SessionStatus.AWAITING_APPROVAL,
            approval_summary="Run the requested command", approval_gate_kind="permission",
            approval_granted_tool="Bash",
        )
    ]
    await h.kernel.process_event(
        _turn(
            f"work-item-{request_id}-execute-1", ISSUE_URL,
            kind=kind, conversation_id=conversation_id,
        )
    )
    [held] = [
        run for run in h.kernel._held_work_items.values() if run.request_id == request_id
    ]
    assert held.request_id == request_id
    assert held.held and held.started and not held.finished
    assert held.claim_name is not None and held.sandbox_name is not None
    return held


@pytest.mark.parametrize("continuation", ["ci", "approval"])
def test_continuation_refuses_a_persisted_repository_removed_from_the_allowlist(
    make_harness, caplog: pytest.LogCaptureFixture, continuation: str,
) -> None:
    class RevokedWorkspace(_Workspace):
        def __init__(self, substrate: object) -> None:
            super().__init__(substrate)
            self.allowed = True

        def select_repository(self, **kwargs: object) -> object:
            selected = super().select_repository(**kwargs)
            if selected is not None and not self.allowed:
                raise WorkspaceRepositoryNotAllowed("Repository access was removed.")
            return selected

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(), workspace_factory=RevokedWorkspace, approvals=_Approvals(),
            publication_creator=_NoExistingPublication(),
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            held = await _park_factory_request(h, uuid.uuid4())
            workspace = h.kernel._workspace
            workspace.allowed = False
            claims_before = list(h.fake_k8s.claim_envs)
            incoming = _turn(
                f"work-item-{held.request_id}-ci-2" if continuation == "ci"
                else f"approval-{uuid.uuid4()}-resolved",
                "Continue the current request", placeholder="approval-placeholder",
            )
            h.runner.default_script = [Final(text="Must not run", status=SessionStatus.DONE)]

            with caplog.at_level("INFO", logger="curie_worker.kernel"):
                await h.kernel.process_event(incoming)

            assert workspace.selections == [WORK_ITEM_REPO, WORK_ITEM_REPO]
            assert workspace.claimed == [WORK_ITEM_REPO]
            assert h.runner.opened == [ISSUE_URL]
            assert h.runner.steers == []
            assert h.fake_k8s.claim_envs == claims_before
            assert items.calls.count("acquire") == 1
            assert items.calls.count("start") == 1
            assert items.calls.count("finish") == 1
            assert items.terminal_outcomes == {held.request_id: "failed"}
            assert items.finishes[0]["runtime_epoch"] == held.runtime_epoch
            assert items.finishes[0]["cause"] == (
                "ci_fix_unpublished" if continuation == "ci" else "no_pull_request"
            )
            assert items.finishes[0]["ci_fix_round"] == (2 if continuation == "ci" else None)
            assert any(
                "workspace selection refused" in message
                and "Repository access was removed." in message
                for message in caplog.messages
            )
            assert await h.kernel._markers.is_terminal(incoming.event_id)
            assert not h.kernel.owns_work_item(held.request_id)
            assert held.thread_key not in h.kernel._held_work_items
            assert held.claim_name in h.fake_k8s.deleted_claims
            assert h.substrate._affinity.get(held.thread_key) is None
            assert not h.fake_k8s.claims

    asyncio.run(exercise())


@pytest.mark.parametrize("continuation", ["ci", "approval"])
def test_cold_continuation_reselects_persisted_repository_after_workspace_state_is_lost(
    make_harness, continuation: str,
) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(), workspace_factory=_Workspace, approvals=_Approvals(),
            publication_creator=_NoExistingPublication(),
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            held = await _park_factory_request(h, uuid.uuid4())
            assert h.kernel._workspace.selections == [WORK_ITEM_REPO]
            assert await asyncio.to_thread(h.substrate.release, held.thread_key)
            assert h.substrate._affinity.get(held.thread_key) is None
            del h.kernel._held_work_items[held.thread_key]
            workspace = _Workspace(h.substrate)
            h.kernel._workspace = workspace
            assert workspace.selections == [] and workspace.claimed == []
            claimed_routes: list[Any] = []

            async def observe_route_before_finish_cleanup() -> None:
                route = h.substrate._affinity.get(held.thread_key)
                assert route is not None
                claimed_routes.append(route.handle)

            items.after_finish = observe_route_before_finish_cleanup
            incoming = _turn(
                f"work-item-{held.request_id}-ci-2" if continuation == "ci"
                else f"approval-{uuid.uuid4()}-resolved",
                "Continue the current request", placeholder="approval-placeholder",
            )
            h.runner.default_script = [Final(text="No change published", status=SessionStatus.DONE)]

            await h.kernel.process_event(incoming)

            assert workspace.selections == [WORK_ITEM_REPO]
            assert workspace.claimed == [WORK_ITEM_REPO]
            [route] = claimed_routes
            assert route.workspace_repo == WORK_ITEM_REPO
            assert route.caller_run == str(held.request_id)
            assert route.claim_name != held.claim_name
            assert h.runner.opened == [ISSUE_URL, incoming.text]
            assert h.runner.steers == []
            assert items.calls.count("acquire") == 1
            assert items.calls.count("start") == 1
            assert items.calls.count("running_for_conversation") == 1
            assert items.calls.count("finish") == 1
            assert items.finishes[0]["runtime_epoch"] == held.runtime_epoch
            assert items.finishes[0]["cause"] == (
                "ci_fix_unpublished" if continuation == "ci" else "no_pull_request"
            )
            assert items.finishes[0]["ci_fix_round"] == (2 if continuation == "ci" else None)
            assert items.terminal_outcomes == {held.request_id: "failed"}
            assert await h.kernel._markers.is_terminal(incoming.event_id)
            assert not h.kernel.owns_work_item(held.request_id)
            assert h.substrate._affinity.get(held.thread_key) is None
            assert not h.fake_k8s.claims

    asyncio.run(exercise())


class _SharedLocationBinding(_Binding):
    def __init__(self) -> None:
        self.admitted = self._deployment()
        self.sibling = SimpleNamespace(
            agent_id=uuid.UUID("44444444-4444-4444-8444-444444444444"),
            agent_name="acme-neighbor",
            deployment_id=uuid.UUID("55555555-5555-4555-8555-555555555555"),
        )
        self.ordinary_calls: list[tuple[str, str | None, str]] = []
        self.factory_contexts: list[FactoryContext] = []

    async def resolve(self, kind: str, adapter: str | None, channel: str) -> object:
        self.ordinary_calls.append((kind, adapter, channel))
        assert (kind, adapter, channel) == ("github", None, WORK_ITEM_REPO)
        raise AmbiguousRoute(kind, channel, [AGENT_ID, self.sibling.agent_id])

    async def resolve_factory(self, context: FactoryContext) -> object:
        assert (context.agent_id, context.kind, context.address, context.adapter) == (
            AGENT_ID, "github", WORK_ITEM_REPO, None,
        )
        self.factory_contexts.append(context)
        return self.admitted

    def boot_env(self, resolved: Any, thread_key: str, **kwargs: object) -> dict[str, str]:
        return {
            **super().boot_env(resolved, thread_key, **kwargs),
            "CURIE_TEST_AGENT_ID": str(resolved.agent_id),
            "CURIE_TEST_DEPLOYMENT_ID": str(resolved.deployment_id),
        }


def test_approval_resume_uses_admitted_binding_at_shared_location(make_harness) -> None:
    async def exercise() -> None:
        binding = _SharedLocationBinding()
        async with make_harness(
            binding=binding, workspace_factory=_Workspace, approvals=_Approvals(),
            publication_creator=_NoExistingPublication(),
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            held = await _park_factory_request(
                h, uuid.uuid4(), kind="github", conversation_id="issue-123",
            )
            incoming = _turn(
                f"approval-{uuid.uuid4()}-resolved", "[approval resolved] approved",
                kind="github", conversation_id="issue-123",
            )
            h.runner.default_script = [Final(text="No change published", status=SessionStatus.DONE)]

            await h.kernel.process_event(incoming)

            assert h.runner.opened == [ISSUE_URL, incoming.text]
            assert binding.ordinary_calls == []
            assert binding.factory_contexts == [held.context, held.context]
            assert h.fake_k8s.claim_envs
            for env in h.fake_k8s.claim_envs:
                assert env["CURIE_TEST_AGENT_ID"] == str(AGENT_ID)
                assert env["CURIE_TEST_DEPLOYMENT_ID"] == str(DEPLOYMENT_ID)
            assert items.calls.count("start") == 1
            assert items.calls.count("running_for_conversation") == 1
            assert items.terminal_outcomes == {held.request_id: "failed"}
            assert items.finishes[0]["cause"] == "no_pull_request"
            assert items.finishes[0]["runtime_epoch"] == held.runtime_epoch
            assert await h.kernel._markers.is_terminal(incoming.event_id)
            assert not h.kernel.owns_work_item(held.request_id)
            assert not h.fake_k8s.claims

    asyncio.run(exercise())


@pytest.mark.parametrize("parking_phase", ["before_hold", "held_before_finally"])
def test_resume_during_parking_stays_pending_then_adopts_the_same_epoch(
    make_harness, monkeypatch: pytest.MonkeyPatch, parking_phase: str,
) -> None:
    async def exercise() -> None:
        entered = asyncio.Event()
        finish_parking = asyncio.Event()

        class ApprovalCreateBarrier(_Approvals):
            async def create(
                self, request: ApprovalRequest, *, budget_s: float = 120,
            ) -> CreatedApproval:
                created = await super().create(request, budget_s=budget_s)
                if parking_phase == "before_hold":
                    entered.set()
                    await finish_parking.wait()
                return created

        binding = _SharedLocationBinding()
        async with make_harness(
            binding=binding, workspace_factory=_Workspace, approvals=ApprovalCreateBarrier(),
            publication_creator=_NoExistingPublication(),
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            request_id = uuid.uuid4()
            execute_id = f"work-item-{request_id}-execute-1"
            if parking_phase == "held_before_finally":
                complete = h.kernel._complete

                async def complete_before_parking_finally(
                    qevent: QueuedTurn, route: TargetRoute, *args: Any, **kwargs: Any,
                ) -> None:
                    await complete(qevent, route, *args, **kwargs)
                    if qevent.event_id == execute_id:
                        assert h.kernel._work_item_runs[request_id].held
                        entered.set()
                        await finish_parking.wait()

                monkeypatch.setattr(h.kernel, "_complete", complete_before_parking_finally)
            parking = asyncio.create_task(
                _park_factory_request(h, request_id, kind="github", conversation_id="issue-123")
            )
            try:
                await asyncio.wait_for(entered.wait(), timeout=5)
                active = h.kernel._work_item_runs[request_id]
                epoch = active.runtime_epoch
                assert epoch is not None and active.started and not active.finished
                assert active.held is (parking_phase == "held_before_finally")
                assert (request_id in items.deadline_holds) is (
                    parking_phase == "held_before_finally"
                )
                incoming = _turn(
                    f"approval-{uuid.uuid4()}-resolved", "[approval resolved] approved",
                    kind="github", conversation_id="issue-123",
                )

                with pytest.raises(failures._FactoryContinuationPending):
                    await asyncio.wait_for(h.kernel.process_event(incoming), timeout=5)

                assert not await h.kernel._markers.is_terminal(incoming.event_id)
                assert h.kernel._work_item_runs[request_id] is active
                assert active.event_id == execute_id and active.runtime_epoch == epoch
                assert h.kernel._held_work_items == {}
                assert h.runner.opened == [ISSUE_URL]
                assert h.runner.steers == []
                assert items.finishes == []
                assert items.calls.count("acquire") == 1
                assert items.calls.count("start") == 1
                assert binding.factory_contexts == [active.context]
                assert binding.ordinary_calls == []

                finish_parking.set()
                held = await asyncio.wait_for(parking, timeout=5)
                assert held is active and held.runtime_epoch == epoch
                assert request_id not in h.kernel._work_item_runs
                assert h.kernel._held_work_items == {held.thread_key: held}
                assert request_id in items.deadline_holds
                h.runner.default_script = [
                    Final(text="No change published", status=SessionStatus.DONE),
                ]
                await h.kernel.process_event(incoming)
                assert h.runner.opened == [ISSUE_URL, incoming.text]
                assert binding.factory_contexts == [held.context, held.context]
                assert binding.ordinary_calls == []
                assert items.calls.count("running_for_conversation") == 2
                assert items.calls.count("start") == 1
                assert items.calls.count("finish") == 1
                assert items.finishes[0]["runtime_epoch"] == epoch
                assert items.finishes[0]["cause"] == "no_pull_request"
                assert items.terminal_outcomes == {request_id: "failed"}
                assert await h.kernel._markers.is_terminal(incoming.event_id)
                assert not h.kernel.owns_work_item(request_id)
                assert not h.fake_k8s.claims
            finally:
                finish_parking.set()
                if not parking.done():
                    parking.cancel()
                await asyncio.gather(parking, return_exceptions=True)

    asyncio.run(exercise())


@pytest.mark.parametrize("bookkeeping", ["held", "cold"])
@pytest.mark.parametrize("decision", ["resolved", "expired"])
def test_predecessor_approval_resume_adopts_a_ci_held_successor_as_the_accepted_residual(
    make_harness, bookkeeping: str, decision: str,
) -> None:
    async def exercise() -> None:
        binding = _SharedLocationBinding()
        async with make_harness(
            binding=binding, workspace_factory=_Workspace, approvals=_Approvals(),
            publication_creator=_NoExistingPublication(),
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            predecessor = await _park_factory_request(
                h, uuid.uuid4(), kind="github", conversation_id="issue-123",
            )
            await items.finish(
                predecessor.request_id, runtime_epoch=predecessor.runtime_epoch,
                outcome="failed", cause="owner_lost", detail=None, ci_fix_round=None,
            )
            assert await asyncio.to_thread(h.substrate.release, predecessor.thread_key)
            successor = await _park_factory_request(
                h, uuid.uuid4(), kind="github", conversation_id="issue-123",
            )
            assert successor.runtime_epoch is not None
            await items.heartbeat(successor.request_id, runtime_epoch=successor.runtime_epoch)
            assert successor.request_id not in items.deadline_holds
            await items.hold_for_ci_fix(
                successor.request_id, runtime_epoch=successor.runtime_epoch,
            )
            assert successor.request_id in items.deadline_holds
            if bookkeeping == "held":
                h.kernel._held_work_items[successor.thread_key] = predecessor
            else:
                del h.kernel._held_work_items[successor.thread_key]
            incoming = _turn(
                f"approval-{uuid.uuid4()}-resolved",
                "[approval expired] Do not perform the gated action."
                if decision == "expired" else "[approval resolved] approved",
                kind="github", conversation_id="issue-123",
            )
            h.runner.default_script = [Final(text="No change published", status=SessionStatus.DONE)]
            finishes_before = list(items.finishes)

            await h.kernel.process_event(incoming)

            assert h.runner.opened == [ISSUE_URL, ISSUE_URL, incoming.text]
            assert binding.factory_contexts == [
                predecessor.context, successor.context, successor.context,
            ]
            assert binding.ordinary_calls == []
            assert items.calls.count("hold_for_ci_fix") == 1
            assert items.calls.count("start") == 2
            assert items.calls.count("acquire") == 2
            assert items.finishes[:-1] == finishes_before
            assert len(items.finishes) == len(finishes_before) + 1
            assert items.finishes[-1]["runtime_epoch"] == successor.runtime_epoch
            assert items.finishes[-1]["cause"] == "no_pull_request"
            assert items.terminal_outcomes == {
                predecessor.request_id: "failed", successor.request_id: "failed",
            }
            assert await h.kernel._markers.is_terminal(incoming.event_id)
            assert not h.kernel.owns_work_item(successor.request_id)
            assert not h.fake_k8s.claims

    asyncio.run(exercise())


@pytest.mark.parametrize("bookkeeping", ["held", "cold"])
@pytest.mark.parametrize("decision", ["resolved", "expired"])
def test_stale_approval_resume_cannot_steer_or_finish_an_active_successor(
    make_harness, bookkeeping: str, decision: str,
) -> None:
    async def exercise() -> None:
        binding = _SharedLocationBinding()
        async with make_harness(
            binding=binding, workspace_factory=_Workspace, approvals=_Approvals(),
            publication_creator=_NoExistingPublication(),
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            predecessor = await _park_factory_request(
                h, uuid.uuid4(), kind="github", conversation_id="issue-123",
            )
            await items.finish(
                predecessor.request_id, runtime_epoch=predecessor.runtime_epoch,
                outcome="failed", cause="owner_lost", detail=None, ci_fix_round=None,
            )
            assert await asyncio.to_thread(h.substrate.release, predecessor.thread_key)
            successor = await _park_factory_request(
                h, uuid.uuid4(), kind="github", conversation_id="issue-123",
            )
            assert successor.runtime_epoch is not None
            await items.heartbeat(successor.request_id, runtime_epoch=successor.runtime_epoch)
            assert successor.request_id not in items.deadline_holds
            if bookkeeping == "held":
                h.kernel._held_work_items[successor.thread_key] = predecessor
            else:
                del h.kernel._held_work_items[successor.thread_key]
            h.runner.turn_active = True
            incoming = _turn(
                f"approval-{uuid.uuid4()}-resolved",
                "[approval expired] Do not perform the gated action."
                if decision == "expired" else "[approval resolved] approved",
                kind="github", conversation_id="issue-123",
            )
            finishes_before = list(items.finishes)
            envs_before = list(h.fake_k8s.claim_envs)
            held_before = dict(h.kernel._held_work_items)
            route_before = h.substrate._affinity.get(successor.thread_key)
            assert route_before is not None

            for _ in range(2):
                with pytest.raises(failures._FactoryContinuationPending):
                    await h.kernel.process_event(incoming)

            assert h.runner.opened == [ISSUE_URL, ISSUE_URL]
            assert h.runner.steers == []
            assert h.runner.turn_active
            assert items.finishes == finishes_before
            assert items.terminal_outcomes == {predecessor.request_id: "failed"}
            assert items.calls.count("start") == 2
            assert items.calls.count("acquire") == 2
            assert binding.factory_contexts == [predecessor.context, successor.context]
            assert binding.ordinary_calls == []
            assert h.fake_k8s.claim_envs == envs_before
            assert h.substrate._affinity.get(successor.thread_key) == route_before
            assert h.kernel._work_item_runs == {}
            assert h.kernel._held_work_items == held_before
            assert not await h.kernel._markers.is_terminal(incoming.event_id)

            h.runner.turn_active = False
            await items.hold_for_ci_fix(
                successor.request_id, runtime_epoch=successor.runtime_epoch,
            )
            assert successor.request_id in items.deadline_holds
            h.runner.default_script = [_APPROVAL_FINAL]
            await h.kernel.process_event(incoming)
            replayed = h.kernel._held_work_items[successor.thread_key]
            assert replayed.event_id == incoming.event_id
            assert replayed.request_id == successor.request_id
            assert replayed.runtime_epoch == successor.runtime_epoch
            assert replayed.agent_id == successor.agent_id == AGENT_ID
            assert replayed.context == successor.context
            assert h.runner.opened == [ISSUE_URL, ISSUE_URL, incoming.text]
            assert h.runner.steers == []
            assert items.finishes == finishes_before
            assert items.terminal_outcomes == {predecessor.request_id: "failed"}
            assert items.calls.count("hold_for_ci_fix") == 1
            assert items.calls.count("start") == 2
            assert items.calls.count("acquire") == 2
            assert binding.factory_contexts == [
                predecessor.context, successor.context, successor.context,
            ]
            assert binding.ordinary_calls == []
            assert await h.kernel._markers.is_terminal(incoming.event_id)

            h.runner.default_script = [Final(text="No change published", status=SessionStatus.DONE)]
            valid_ci = _turn(
                f"work-item-{successor.request_id}-ci-2", "Continue the current CI request",
                kind="github", conversation_id="issue-123",
            )
            await h.kernel.process_event(valid_ci)
            assert h.runner.opened == [ISSUE_URL, ISSUE_URL, incoming.text, valid_ci.text]
            assert items.finishes[:-1] == finishes_before
            assert items.finishes[-1]["runtime_epoch"] == successor.runtime_epoch
            assert items.finishes[-1]["cause"] == "ci_fix_unpublished"
            assert items.finishes[-1]["ci_fix_round"] == 2
            assert items.calls.count("start") == 2
            assert items.calls.count("acquire") == 2
            assert binding.factory_contexts == [
                predecessor.context, successor.context, successor.context, successor.context,
            ]
            assert binding.ordinary_calls == []
            assert await h.kernel._markers.is_terminal(valid_ci.event_id)
            assert await h.kernel._markers.is_terminal(incoming.event_id)
            assert not h.fake_k8s.claims

    asyncio.run(exercise())


@pytest.mark.parametrize("continuation", ["resolved", "expired", "ci"])
def test_continuation_cannot_overwrite_a_live_local_approval_turn(
    make_harness, continuation: str,
) -> None:
    async def exercise() -> None:
        binding = _SharedLocationBinding()
        async with make_harness(
            binding=binding, workspace_factory=_Workspace, approvals=_Approvals(),
            publication_creator=_NoExistingPublication(),
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            held = await _park_factory_request(
                h, uuid.uuid4(), kind="github", conversation_id="issue-123",
            )
            first_resume = _turn(
                f"approval-{uuid.uuid4()}-resolved", "Continue the approved request",
                kind="github", conversation_id="issue-123",
            )
            gate = asyncio.Event()
            h.runner.hold = gate
            h.runner.default_script = [TextDelta(text="Working on the approved request.")]
            h.runner.tail = [Final(text="No change published", status=SessionStatus.DONE)]
            task = asyncio.create_task(h.kernel.process_event(first_resume))
            try:
                await wait_until(
                    lambda: len(h.runner.queried) == 2 and h.runner.turn_ready,
                    "the admitted approval continuation to start",
                )
                live = h.kernel._work_item_runs[held.request_id]
                assert live.started and not live.finished
                assert live.event_id == first_resume.event_id
                assert held.request_id in items.deadline_holds
                claim_name, sandbox_name = live.claim_name, live.sandbox_name
                heartbeat_running = live.heartbeat_running
                incoming = _turn(
                    f"work-item-{held.request_id}-ci-2" if continuation == "ci"
                    else f"approval-{uuid.uuid4()}-resolved",
                    "[approval expired] Do not perform the gated action."
                    if continuation == "expired" else "Continue another delivery",
                    kind="github", conversation_id="issue-123",
                )

                with pytest.raises(failures._FactoryContinuationPending):
                    await asyncio.wait_for(h.kernel.process_event(incoming), timeout=5)

                assert h.kernel._work_item_runs[held.request_id] is live
                assert live.event_id == first_resume.event_id
                assert (live.claim_name, live.sandbox_name) == (claim_name, sandbox_name)
                assert live.heartbeat_running is heartbeat_running
                assert not live.finished and not task.done()
                assert h.kernel.owns_work_item(held.request_id)
                assert h.runner.opened == [ISSUE_URL, first_resume.text]
                assert h.runner.steers == []
                assert items.finishes == []
                assert items.calls.count("start") == 1
                assert binding.factory_contexts == [held.context, held.context]
                assert binding.ordinary_calls == []
                assert not await h.kernel._markers.is_terminal(incoming.event_id)

                gate.set()
                await asyncio.wait_for(task, timeout=5)
                assert items.calls.count("finish") == 1
                assert items.finishes[0]["runtime_epoch"] == held.runtime_epoch
                assert items.finishes[0]["cause"] == "no_pull_request"
                assert await h.kernel._markers.is_terminal(first_resume.event_id)
                assert not h.kernel.owns_work_item(held.request_id)
                assert not h.fake_k8s.claims
                await h.kernel.process_event(incoming)
                assert await h.kernel._markers.is_terminal(incoming.event_id)
                assert items.calls.count("finish") == 1
                assert h.runner.opened == [ISSUE_URL, first_resume.text]
            finally:
                gate.set()
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(exercise())


@pytest.mark.parametrize("replace_slot", [False, True])
def test_ended_lookup_clears_only_the_held_entry_it_observed(
    make_harness, replace_slot: bool,
) -> None:
    class EndedLookup(_WorkItems):
        def __init__(self) -> None:
            super().__init__()
            self.lookup_entered = asyncio.Event()
            self.return_ended = asyncio.Event()

        async def running_for_conversation(
            self, conversation_id: str, *, wake: FactoryWake,
        ) -> WorkItemRunning | None:
            try:
                return await super().running_for_conversation(conversation_id, wake=wake)
            except WorkItemConflict as exc:
                if exc.code != "execution_ended":
                    raise
                self.lookup_entered.set()
                await self.return_ended.wait()
                raise

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(), workspace_factory=_Workspace, approvals=_Approvals(),
            publication_creator=_NoExistingPublication(),
        ) as h:
            items = EndedLookup()
            h.kernel._work_items = items
            predecessor = await _park_factory_request(h, uuid.uuid4())
            await items.finish(
                predecessor.request_id, runtime_epoch=predecessor.runtime_epoch,
                outcome="failed", cause="owner_lost", detail=None, ci_fix_round=None,
            )
            wake = FactoryWake(
                kind="slack", address=CHANNEL, adapter=None,
                conversation_id="1700000000.000001",
            )
            pending = asyncio.create_task(
                h.kernel._adopt_resumed_work_item(
                    f"approval-{uuid.uuid4()}-resolved", predecessor.thread_key, wake=wake,
                )
            )
            try:
                await asyncio.wait_for(items.lookup_entered.wait(), timeout=5)
                assert await asyncio.to_thread(h.substrate.release, predecessor.thread_key)
                neighbor = None
                if replace_slot:
                    neighbor = await _park_factory_request(h, uuid.uuid4())
                items.return_ended.set()
                with pytest.raises(failures._FactoryExecutionEnded):
                    await asyncio.wait_for(pending, timeout=5)
                assert not h.kernel.owns_work_item(predecessor.request_id)
                if neighbor is None:
                    assert h.kernel._held_work_items == {}
                    neighbor = await _park_factory_request(h, uuid.uuid4())
                else:
                    assert h.kernel._held_work_items == {neighbor.thread_key: neighbor}
                    assert h.kernel.owns_work_item(neighbor.request_id)
                assert len(items.finishes) == 1
                assert items.terminal_outcomes == {predecessor.request_id: "failed"}
                valid = _turn(
                    f"approval-{uuid.uuid4()}-resolved", "Continue the valid held request",
                )
                h.runner.default_script = [
                    Final(text="No change published", status=SessionStatus.DONE),
                ]
                await h.kernel.process_event(valid)
                assert h.runner.opened == [ISSUE_URL, ISSUE_URL, valid.text]
                assert items.finishes[-1]["runtime_epoch"] == neighbor.runtime_epoch
                assert items.finishes[-1]["cause"] == "no_pull_request"
                assert not h.kernel._held_work_items
                assert not h.fake_k8s.claims
            finally:
                items.return_ended.set()
                if not pending.done():
                    pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)

    asyncio.run(exercise())


@pytest.mark.parametrize("bookkeeping", ["missing", "mismatched_event"])
def test_owned_resume_with_invalid_bookkeeping_cannot_resolve_ordinary_binding(
    make_harness, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
    bookkeeping: str,
) -> None:
    async def exercise() -> None:
        binding = _SharedLocationBinding()
        async with make_harness(
            binding=binding, workspace_factory=_Workspace, approvals=_Approvals(),
            publication_creator=_NoExistingPublication(),
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            held = await _park_factory_request(
                h, uuid.uuid4(), kind="github", conversation_id="issue-123",
            )
            incoming = _turn(
                f"approval-{uuid.uuid4()}-resolved", "[approval resolved] approved",
                kind="github", conversation_id="issue-123",
            )
            finalize = h.kernel._finalize_settled_card
            interrupted_runs: list[WorkItemRun] = []

            async def lose_bookkeeping(qevent: QueuedTurn, route: TargetRoute) -> None:
                await finalize(qevent, route)
                assert qevent.event_id == incoming.event_id
                run = h.kernel._work_item_runs[held.request_id]
                interrupted_runs.append(run)
                if bookkeeping == "missing":
                    del h.kernel._work_item_runs[held.request_id]
                else:
                    run.event_id = f"approval-{uuid.uuid4()}-resolved"
                await run.close()

            monkeypatch.setattr(h.kernel, "_finalize_settled_card", lose_bookkeeping)
            envs_before = list(h.fake_k8s.claim_envs)
            with caplog.at_level("INFO", logger="curie_worker.kernel"):
                await h.kernel.process_event(incoming)

            assert len(interrupted_runs) == 1
            assert h.runner.opened == [ISSUE_URL]
            assert binding.ordinary_calls == []
            assert binding.factory_contexts == [held.context]
            assert h.fake_k8s.claim_envs == envs_before
            assert items.calls.count("start") == 1
            assert items.finishes == []
            assert items.terminal_outcomes == {}
            assert any("factory_wake_mismatch" in message for message in caplog.messages)

            monkeypatch.setattr(h.kernel, "_finalize_settled_card", finalize)
            h.kernel._work_item_runs.pop(held.request_id, None)
            valid = _turn(
                f"approval-{uuid.uuid4()}-resolved", "Continue the admitted request",
                kind="github", conversation_id="issue-123",
            )
            h.runner.default_script = [Final(text="No change published", status=SessionStatus.DONE)]
            await h.kernel.process_event(valid)
            assert h.runner.opened == [ISSUE_URL, valid.text]
            assert binding.ordinary_calls == []
            assert binding.factory_contexts == [held.context, held.context]
            assert items.finishes[0]["runtime_epoch"] == held.runtime_epoch
            assert items.finishes[0]["cause"] == "no_pull_request"
            assert await h.kernel._markers.is_terminal(valid.event_id)
            assert not h.fake_k8s.claims

    asyncio.run(exercise())


@pytest.mark.parametrize("end_action", ["kill", "terminate"])
@pytest.mark.parametrize("replace_slot", [False, True])
def test_refused_resume_finish_transport_failure_cannot_restore_ended_held_run(
    make_harness, end_action: str, replace_slot: bool,
) -> None:
    class FinishUnavailable(_WorkItems):
        def __init__(self, refused_request_id: uuid.UUID) -> None:
            super().__init__()
            self.refused_request_id = refused_request_id
            self.finish_entered = asyncio.Event()
            self.fail_finish = asyncio.Event()
            self.runtime_claims: dict[uuid.UUID, tuple[str, str]] = {}
            self.finish_attempts: list[dict[str, object]] = []
            self.terminations: list[uuid.UUID] = []

        async def start(
            self, request_id: uuid.UUID, *, claim_name: str, sandbox_name: str,
            context: FactoryContext, **kwargs: object,
        ) -> WorkItemStartGrant:
            started = await super().start(request_id, context=context, **kwargs)
            self.runtime_claims[request_id] = (claim_name, sandbox_name)
            return started

        async def running_for_conversation(
            self, conversation_id: str, *, wake: FactoryWake,
        ) -> WorkItemRunning | None:
            running = await super().running_for_conversation(conversation_id, wake=wake)
            if running is not None and running.request_id == self.refused_request_id:
                return replace(running, context=None, refusal="factory_binding_missing")
            return running

        async def finish(self, request_id: uuid.UUID, **kwargs: object) -> None:
            if request_id != self.refused_request_id:
                await super().finish(request_id, **kwargs)
                return
            assert kwargs["runtime_epoch"] == self.starts[request_id].runtime_epoch
            assert kwargs["outcome"] == "failed"
            assert kwargs["cause"] == "runner_escalated"
            self.finish_attempts.append(kwargs)
            self.finish_entered.set()
            await self.fail_finish.wait()
            raise WorkItemTransportError("finish response unavailable")

        async def claim_termination(self, request_id: uuid.UUID, *, owner: str) -> int:
            assert request_id == self.refused_request_id
            assert owner == "test-consumer"
            return self.starts[request_id].runtime_epoch

        async def get_request(self, request_id: uuid.UUID) -> WorkItemRequestView:
            view = await super().get_request(request_id)
            claim_name, sandbox_name = self.runtime_claims[request_id]
            return replace(view, runtime_claim_name=claim_name, runtime_sandbox_name=sandbox_name)

        async def record_termination(
            self, request_id: uuid.UUID, *, runtime_epoch: int, observation: str,
        ) -> None:
            assert runtime_epoch == self.starts[request_id].runtime_epoch
            assert self.runtime_claims[request_id][0] in observation
            self.terminal_outcomes[request_id] = "cancelled"
            self.terminations.append(request_id)

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(), workspace_factory=_Workspace, approvals=_Approvals(),
            publication_creator=_NoExistingPublication(),
        ) as h:
            request_id = uuid.uuid4()
            items = FinishUnavailable(request_id)
            h.kernel._work_items = items
            held = await _park_factory_request(h, request_id)
            pending = asyncio.create_task(
                h.kernel._adopt_resumed_work_item(
                    f"approval-{uuid.uuid4()}-resolved", held.thread_key,
                    wake=FactoryWake(
                        kind="slack", address=CHANNEL, adapter=None,
                        conversation_id="1700000000.000001",
                    ),
                )
            )
            try:
                await asyncio.wait_for(items.finish_entered.wait(), timeout=5)
                if end_action == "kill":
                    items.terminal_outcomes[request_id] = "cancelled"
                    await h.kernel.interrupt_agent(AGENT_ID)
                else:
                    await h.kernel._terminate_work_item(
                        _turn(f"work-item-{request_id}-terminate", "terminate"), request_id,
                    )
                    assert items.terminations == [request_id]
                    assert held.claim_name in h.fake_k8s.deleted_claims
                assert not h.kernel.owns_work_item(request_id)
                assert held.thread_key not in h.kernel._held_work_items

                neighbor = await _park_factory_request(
                    h, uuid.uuid4(),
                    conversation_id=(
                        "1700000000.000001" if replace_slot else "1700000000.000002"
                    ),
                )
                assert (neighbor.thread_key == held.thread_key) is replace_slot
                items.fail_finish.set()
                with pytest.raises(WorkItemTransportError, match="finish response unavailable"):
                    await asyncio.wait_for(pending, timeout=5)

                assert len(items.finish_attempts) == 1
                assert h.kernel._held_work_items == {neighbor.thread_key: neighbor}
                assert not h.kernel.owns_work_item(request_id)
                assert h.kernel.owns_work_item(neighbor.request_id)
                assert h.runner.opened == [ISSUE_URL, ISSUE_URL]

                valid = _turn(
                    f"approval-{uuid.uuid4()}-resolved", "Continue the valid neighbor",
                    conversation_id=(
                        "1700000000.000001" if replace_slot else "1700000000.000002"
                    ),
                )
                h.runner.default_script = [
                    Final(text="No change published", status=SessionStatus.DONE),
                ]
                await h.kernel.process_event(valid)
                assert h.runner.opened == [ISSUE_URL, ISSUE_URL, valid.text]
                assert items.terminal_outcomes == {
                    request_id: "cancelled", neighbor.request_id: "failed",
                }
                assert items.finishes[0]["runtime_epoch"] == neighbor.runtime_epoch
                assert items.finishes[0]["cause"] == "no_pull_request"
                assert not h.kernel._held_work_items
                assert not h.kernel.owns_work_item(neighbor.request_id)
                assert await h.kernel._markers.is_terminal(valid.event_id)
            finally:
                items.fail_finish.set()
                if not pending.done():
                    pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)
                await asyncio.to_thread(h.substrate.release, held.thread_key)

    asyncio.run(exercise())


@pytest.mark.parametrize("continuation", ["ci", "approval"])
def test_valid_successor_continues_beside_stale_held_predecessor(
    make_harness, caplog: pytest.LogCaptureFixture, continuation: str,
) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(), workspace_factory=_Workspace, approvals=_Approvals(),
            publication_creator=_NoExistingPublication(),
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            predecessor_id, successor_id = uuid.uuid4(), uuid.uuid4()
            predecessor = await _park_factory_request(h, predecessor_id)
            await items.finish(
                predecessor_id, runtime_epoch=predecessor.runtime_epoch,
                outcome="failed", cause="owner_lost", detail=None, ci_fix_round=None,
            )
            assert await asyncio.to_thread(h.substrate.release, predecessor.thread_key)
            successor = await _park_factory_request(h, successor_id)
            assert successor.claim_name != predecessor.claim_name
            h.kernel._held_work_items[successor.thread_key] = predecessor
            finishes_before = list(items.finishes)
            incoming = _turn(
                f"work-item-{successor_id}-ci-2" if continuation == "ci"
                else f"approval-{uuid.uuid4()}-resolved",
                "Continue the current request", placeholder="approval-placeholder",
            )
            h.runner.default_script = [Final(text="No change published", status=SessionStatus.DONE)]
            with caplog.at_level("INFO", logger="curie_worker.kernel"):
                await h.kernel.process_event(incoming)
            assert h.runner.opened == [ISSUE_URL, ISSUE_URL, incoming.text]
            assert items.finishes[:-1] == finishes_before
            assert len(items.finishes) == len(finishes_before) + 1
            assert items.terminal_outcomes == {predecessor_id: "failed", successor_id: "failed"}
            assert items.finishes[-1]["runtime_epoch"] == successor.runtime_epoch
            assert items.finishes[-1]["cause"] == (
                "ci_fix_unpublished" if continuation == "ci" else "no_pull_request"
            )
            assert items.finishes[-1]["ci_fix_round"] == (2 if continuation == "ci" else None)
            assert any("discarded stale held bookkeeping" in line for line in caplog.messages)
            assert incoming.event_id not in {predecessor.event_id, successor.event_id}
            assert await h.kernel._markers.is_terminal(incoming.event_id)
            assert successor.thread_key not in h.kernel._held_work_items
            assert not h.fake_k8s.claims
            assert h.substrate._affinity.get(successor.thread_key) is None

    asyncio.run(exercise())


@pytest.mark.parametrize("response", ["delivered", "lost", "stale_owner"])
def test_refused_held_resume_releases_its_claim_only_when_the_finish_committed(
    make_harness, response: str,
) -> None:
    """A refused held continuation finishes its epoch and frees its sandbox.

    The client retries a settlement whose response was lost; the retry of a
    committed finish is refused ``not_running``. That refusal still means the
    request ended, so the recorded claim is released exactly as when the
    response arrives. A different conflict leaves the claim to its owner.
    """

    class RefusedHold(_WorkItems):
        def __init__(self) -> None:
            super().__init__()
            self.refused: uuid.UUID | None = None

        async def running_for_conversation(
            self, conversation_id: str, *, wake: FactoryWake,
        ) -> WorkItemRunning | None:
            running = await super().running_for_conversation(conversation_id, wake=wake)
            if running is not None and running.request_id == self.refused:
                return replace(running, context=None, refusal="factory_binding_missing")
            return running

        async def finish(self, request_id: uuid.UUID, **kwargs: object) -> None:
            if response == "stale_owner":
                self.calls.append("finish")
                raise WorkItemConflict("stale_owner")
            await super().finish(request_id, **kwargs)
            if response == "lost":
                raise WorkItemConflict("not_running")

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(), workspace_factory=_Workspace, approvals=_Approvals(),
            publication_creator=_NoExistingPublication(),
        ) as h:
            items = RefusedHold()
            h.kernel._work_items = items
            request_id = uuid.uuid4()
            held = await _park_factory_request(h, request_id)
            items.refused = request_id
            try:
                with pytest.raises(failures._FactoryExecutionEnded):
                    await h.kernel._adopt_resumed_work_item(
                        f"approval-{uuid.uuid4()}-resolved", held.thread_key,
                        wake=FactoryWake(
                            kind="slack", address=CHANNEL, adapter=None,
                            conversation_id="1700000000.000001",
                        ),
                    )
                assert items.calls.count("finish") == 1
                assert held.thread_key not in h.kernel._held_work_items
                assert not h.kernel.owns_work_item(request_id)
                assert h.runner.opened == [ISSUE_URL]
                if response == "stale_owner":
                    assert request_id not in items.terminal_outcomes
                    assert held.claim_name not in h.fake_k8s.deleted_claims
                    assert held.claim_name in h.fake_k8s.claims
                else:
                    assert items.terminal_outcomes == {request_id: "failed"}
                    assert items.finishes[0]["cause"] == "runner_escalated"
                    assert held.claim_name in h.fake_k8s.deleted_claims
                    assert h.substrate._affinity.get(held.thread_key) is None
            finally:
                await asyncio.to_thread(h.substrate.release, held.thread_key)

    asyncio.run(exercise())


@pytest.mark.parametrize("response", ["delivered", "lost", "stale_owner"])
def test_binding_refused_after_held_adoption_releases_claim_only_when_finish_committed(
    make_harness, response: str,
) -> None:
    """The binding is removed after a held continuation is adopted.

    The delivery's factory resolution refuses, so the adopted run finishes its
    epoch as escalated. A retried finish whose first response was lost is
    refused ``not_running`` and still ends the request, so the recorded claim
    is released. A different conflict leaves the claim to its owner.
    """

    class RemovedBinding(_Binding):
        removed = False

        async def resolve_factory(self, context: FactoryContext) -> object:
            if self.removed:
                raise FactoryBindingRefused("factory_binding_missing")
            return await super().resolve_factory(context)

    class FinishOutcome(_WorkItems):
        async def finish(self, request_id: uuid.UUID, **kwargs: object) -> None:
            if response == "stale_owner":
                self.calls.append("finish")
                raise WorkItemConflict("stale_owner")
            await super().finish(request_id, **kwargs)
            if response == "lost":
                raise WorkItemConflict("not_running")

    async def exercise() -> None:
        binding = RemovedBinding()
        async with make_harness(
            binding=binding, workspace_factory=_Workspace, approvals=_Approvals(),
            publication_creator=_NoExistingPublication(),
        ) as h:
            items = FinishOutcome()
            h.kernel._work_items = items
            request_id = uuid.uuid4()
            held = await _park_factory_request(h, request_id)
            binding.removed = True
            incoming = _turn(
                f"approval-{uuid.uuid4()}-resolved", "[approval resolved] approved",
            )
            try:
                await h.kernel.process_event(incoming)
                assert items.calls.count("finish") == 1
                assert h.runner.opened == [ISSUE_URL]
                assert held.thread_key not in h.kernel._held_work_items
                assert not h.kernel.owns_work_item(request_id)
                assert await h.kernel._markers.is_terminal(incoming.event_id)
                if response == "stale_owner":
                    assert request_id not in items.terminal_outcomes
                    assert held.claim_name not in h.fake_k8s.deleted_claims
                    assert held.claim_name in h.fake_k8s.claims
                else:
                    assert items.terminal_outcomes == {request_id: "failed"}
                    assert items.finishes[0]["cause"] == "runner_escalated"
                    assert held.claim_name in h.fake_k8s.deleted_claims
                    assert h.substrate._affinity.get(held.thread_key) is None
            finally:
                await asyncio.to_thread(h.substrate.release, held.thread_key)

    asyncio.run(exercise())


@pytest.mark.parametrize("successor_route", [False, True])
def test_matching_held_resume_early_exit_releases_only_its_recorded_claim(
    make_harness, monkeypatch: pytest.MonkeyPatch, successor_route: bool,
) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(), workspace_factory=_Workspace, approvals=_Approvals(),
            publication_creator=_NoExistingPublication(),
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            held = await _park_factory_request(h, uuid.uuid4())
            assert held.claim_name is not None
            successor = None
            if successor_route:
                assert h.substrate._affinity.delete_if_claim(held.thread_key, held.claim_name)
                successor = await asyncio.to_thread(h.substrate.claim, held.thread_key, env={})
                assert successor.claim_name != held.claim_name
            releases: list[tuple[str, str]] = []
            release_if_claim = h.substrate.release_if_claim

            def release(thread_key: str, claim_name: str) -> bool:
                releases.append((thread_key, claim_name))
                return release_if_claim(thread_key, claim_name)

            monkeypatch.setattr(h.substrate, "release_if_claim", release)
            incoming = _turn(
                f"approval-{uuid.uuid4()}-resolved", "[approval resolved] approved",
                placeholder="approval-placeholder",
            )
            await h.kernel._markers.mark_side_effect(incoming.event_id)
            await h.kernel.process_event(incoming)
            assert h.runner.opened == [ISSUE_URL]
            assert items.calls.count("start") == 1
            assert items.calls.count("finish") == 1
            assert items.finishes[0]["runtime_epoch"] == held.runtime_epoch
            assert items.finishes[0]["cause"] == "runner_escalated"
            assert releases == [(held.thread_key, held.claim_name)]
            assert await h.kernel._markers.is_terminal(incoming.event_id)
            assert held.thread_key not in h.kernel._held_work_items
            if successor is None:
                assert held.claim_name in h.fake_k8s.deleted_claims
                assert h.substrate._affinity.get(held.thread_key) is None
                assert not h.fake_k8s.claims
            else:
                route = h.substrate._affinity.get(held.thread_key)
                assert route is not None and route.handle.claim_name == successor.claim_name
                assert successor.claim_name in h.fake_k8s.claims
                assert successor.claim_name not in h.fake_k8s.deleted_claims
                assert held.claim_name not in h.fake_k8s.deleted_claims
                assert await asyncio.to_thread(h.substrate.release, held.thread_key)
                h.fake_k8s.delete_claim(held.claim_name, request_timeout_seconds=1.0)

    asyncio.run(exercise())
