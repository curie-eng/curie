"""Approval-gate lifecycle tests (#244, ADR-0010): suspend on pending, resume
on resolve, durable across a worker restart.

Same harness discipline as the other kernel suites: real Valkey, the real
substrate over a fake Kubernetes client, an in-process fake ACI runner. Only
Slack, the model, and the approval API (a recording fake at the
``ApprovalCreator`` seam) are faked.
"""

from __future__ import annotations

import asyncio
import functools
import json
import logging
import sys
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import aiohttp
import httpx
import pytest
from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus, TextDelta
from aiohttp import web
from aiohttp.test_utils import TestServer
from channel_protocol import ConfirmIntent, ReplyUpdate
from channel_protocol.reply import ReplyAck, ReplyEvent, ReplyPost
from curie_worker.approvals import (
    ApprovalBackendError,
    ApprovalClient,
    ApprovalRequest,
    CreatedApproval,
    PublicationCreateRequest,
    SettledApproval,
)
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.binding import GRANT_TOOL_ENV
from curie_worker.kernel import _WorkspaceInferenceCarry
from curie_worker.reply_sink import TargetRoute, _ClusterMessageReplyAdapter
from curie_worker.runner_client import RunnerError
from curie_worker.sandbox.types import RouteState
from curie_worker.workspace import WorkspaceSelectionRefused
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags, TraceState

# importlib import mode does not add the tests directory to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))
from queue_fixtures import qevent  # noqa: E402

DONE = SessionStatus.DONE
AWAITING = SessionStatus.AWAITING_APPROVAL

_HTTP_TRACE_ID = int("3123456789abcdef0123456789abcdef", 16)
_HTTP_SPAN_ID = int("3123456789abcdef", 16)
_HTTP_TRACEPARENT = "00-3123456789abcdef0123456789abcdef-3123456789abcdef-01"


@contextmanager
def _approval_http_parent() -> Iterator[None]:
    parent = SpanContext(
        trace_id=_HTTP_TRACE_ID,
        span_id=_HTTP_SPAN_ID,
        is_remote=True,
        trace_flags=TraceFlags.SAMPLED,
        trace_state=TraceState(),
    )
    token = otel_context.attach(trace.set_span_in_context(NonRecordingSpan(parent)))
    try:
        yield
    finally:
        otel_context.detach(token)


class RecordingApprovals:
    """An ApprovalCreator fake that records requests and mints stable ids."""

    def __init__(self, *, fail: bool = False) -> None:
        self.requests: list[ApprovalRequest] = []
        self.create_calls = 0
        self.fail = fail

    async def create(self, request: ApprovalRequest, *, budget_s: float = 120) -> CreatedApproval:
        self.create_calls += 1
        if self.fail:
            raise ApprovalBackendError("approval API unavailable")
        self.requests.append(request)
        return CreatedApproval(id=f"appr-{len(self.requests)}", status="pending")


class RecordingReader:
    """An ApprovalReader fake: hands back settled records in order, records the reads.

    Takes one or more responses and returns them in turn, the last one repeating
    for every further read, so ``RecordingReader(record)`` is a fixed record and
    ``RecordingReader(None, record)`` is a first read that came back empty and
    then recovered.

    That leading ``None`` is what a transient failure actually looks like at this
    seam (#1199): ``ApprovalReader.get`` never raises -- on an ``httpx.HTTPError``
    or a 503 it logs and hands back ``None`` -- so a blip is indistinguishable at
    the call site from "no such record", and raising here would exercise a path
    the production reader cannot produce.

    Separate from ``RecordingApprovals`` for the same reason the kernel takes two
    parameters (#1084): most tests need only the create half, and a combined fake
    would make every one of them carry a read they never exercise.

    Those responses answer only for an approval the test has resolved. Until
    then the record reads back ``pending``, as the API's does, because the pause
    reads its own record back once its card is registered (#3637). A test that
    resolves before a resume calls ``resolve`` first, which is the API's order:
    the row is resolved, then the resume is enqueued.
    """

    def __init__(self, *records: SettledApproval | None) -> None:
        # Loud here rather than an IndexError on the first ``get``: with no
        # records there is nothing to hand back, and ``RecordingReader(None)``
        # is how you spell "the read came back empty".
        assert records, "RecordingReader needs at least one record; use RecordingReader(None)"
        self.records = records
        # Every read, pending or not, in order.
        self.reads: list[str] = []
        self.resolved: set[str] = set()
        self._settled_reads = 0

    def resolve(self, approval_id: str) -> None:
        self.resolved.add(approval_id)

    async def get(self, approval_id: str) -> SettledApproval | None:
        self.reads.append(approval_id)
        if approval_id not in self.resolved:
            return SettledApproval(status="pending", resolved_by=None, resolution_note=None)
        self._settled_reads += 1
        return self.records[min(self._settled_reads, len(self.records)) - 1]


_qevent = functools.partial(qevent, thread="th-appr", received_at="2026-07-14T00:00:00+00:00")


def _thread_key(thread: str, *, kind: str = "slack") -> str:
    return f"{kind}:C1:{thread}"


def _awaiting_script(summary: str) -> list:
    return [
        TextDelta(text="Requesting sign-off"),
        Final(text="Requesting sign-off", status=AWAITING, approval_summary=summary),
    ]


def _awaiting_script_with_display(summary: str, display: str) -> list:
    return [
        TextDelta(text="Requesting sign-off"),
        Final(
            text="Requesting sign-off",
            status=AWAITING,
            approval_summary=summary,
            approval_display=display,
            approval_gate_kind="permission",
            approval_granted_tool="Bash",
        ),
    ]


class _LineageFenceRunner:
    def __init__(self, status: dict[str, object]) -> None:
        self._status = status
        self.reads = 0

    async def status(self, *_args: object, **_kwargs: object) -> dict[str, object]:
        self.reads += 1
        return dict(self._status)


def _lineage_route() -> object:
    return SimpleNamespace(
        base_url="http://runner.example.test",
        token="route-token",
    )


def test_completed_lineage_can_replace_an_idle_durable_awaiting_runner() -> None:
    """A failed publication suspend must not strand the dirty pre-publish route."""

    async def go() -> None:
        from curie_worker.kernel import Kernel

        kernel = object.__new__(Kernel)
        kernel._factory_work_item_events = set()  # type: ignore[attr-defined]
        kernel._work_item_runs = {}  # type: ignore[attr-defined]
        kernel._runner = _LineageFenceRunner(  # type: ignore[attr-defined]
            {
                "status": SessionStatus.AWAITING_APPROVAL.value,
                "turn_active": False,
                "history_durable": True,
            }
        )

        assert await kernel._workspace_handoff_ready(
            _lineage_route(),
            lineage_reconciliation=True,
            pending_publication_approval=False,
        )

    asyncio.run(go())


@pytest.mark.parametrize(
    ("status", "pending", "reason"),
    [
        (
            {
                "status": SessionStatus.AWAITING_APPROVAL.value,
                "turn_active": True,
                "history_durable": True,
            },
            False,
            "active-turn",
        ),
        (
            {
                "status": SessionStatus.AWAITING_APPROVAL.value,
                "turn_active": False,
                "history_durable": False,
            },
            False,
            "non-durable-history",
        ),
        (
            {
                "status": SessionStatus.AWAITING_APPROVAL.value,
                "turn_active": False,
                "history_durable": True,
            },
            True,
            "pending-approval",
        ),
    ],
)
def test_lineage_handoff_keeps_busy_and_durability_fences(
    status: dict[str, object], pending: bool, reason: str
) -> None:
    async def go() -> None:
        from curie_worker.kernel import Kernel

        kernel = object.__new__(Kernel)
        kernel._factory_work_item_events = set()  # type: ignore[attr-defined]
        kernel._work_item_runs = {}  # type: ignore[attr-defined]
        kernel._runner = _LineageFenceRunner(status)  # type: ignore[attr-defined]

        assert not await kernel._workspace_handoff_ready(
            _lineage_route(),
            lineage_reconciliation=True,
            pending_publication_approval=pending,
        ), reason

    asyncio.run(go())


def test_non_lineage_handoff_still_refuses_awaiting_approval() -> None:
    """The AWAITING_APPROVAL waiver belongs only to completed publication lineage."""

    async def go() -> None:
        from curie_worker.kernel import Kernel

        kernel = object.__new__(Kernel)
        kernel._factory_work_item_events = set()  # type: ignore[attr-defined]
        kernel._work_item_runs = {}  # type: ignore[attr-defined]
        kernel._runner = _LineageFenceRunner(  # type: ignore[attr-defined]
            {
                "status": SessionStatus.AWAITING_APPROVAL.value,
                "turn_active": False,
                "history_durable": True,
            }
        )

        assert not await kernel._workspace_handoff_ready(
            _lineage_route(),
            lineage_reconciliation=False,
            pending_publication_approval=False,
        )

    asyncio.run(go())


def test_open_lineage_bypasses_same_repo_adoption_and_surfaces_route_cas_loss() -> None:
    async def go() -> None:
        from curie_worker.kernel import Kernel

        old_route = object()

        class Substrate:
            def adopt(self, _thread_key: str) -> object:
                raise AssertionError("open lineage must not adopt the dirty live route")

        class Workspace:
            def claim_or_resume_with_handle(self, **kwargs: object) -> object:
                assert kwargs["replace_handle"] is old_route
                assert kwargs["lineage_branch"] == "curie/thread-lineage-example"
                assert kwargs["lineage_head"] == "a" * 40
                raise RuntimeError("late workspace handoff lost its route fence")

        kernel = object.__new__(Kernel)
        kernel._factory_work_item_events = set()  # type: ignore[attr-defined]
        kernel._work_item_runs = {}  # type: ignore[attr-defined]
        kernel._substrate = Substrate()  # type: ignore[attr-defined]
        kernel._workspace = Workspace()  # type: ignore[attr-defined]

        with pytest.raises(RuntimeError, match="lost its route fence"):
            await kernel._claim_or_resume(
                "slack:C0EXAMPLE1:1700000000.000100",
                {},
                workspace_deployment_id=uuid.UUID("11111111-1111-4111-8111-111111111111"),
                workspace_repo="acme-corp/acme-bot",
                replace_handle=old_route,
                lineage_branch="curie/thread-lineage-example",
                lineage_head="a" * 40,
                force_lineage_replacement=True,
                agent_name="acme-bot",
            )

    asyncio.run(go())


def test_route_cas_loss_never_steers_or_starts_the_old_lineage_runner() -> None:
    async def go() -> None:
        from curie_worker.kernel import Kernel
        from curie_worker.sandbox.types import SandboxHandle

        thread_key = "slack:C0EXAMPLE1:1700000000.000100"
        old_route = SandboxHandle(
            thread_key=thread_key,
            claim_name="claim-old",
            sandbox_name="sandbox-old",
            namespace="test-ns",
            service_fqdn="old-runner.example.test",
            port=8080,
            session_id="session-old",
            token="route-token",
            workspace_repo="acme-corp/acme-bot",
        )

        class Substrate:
            adopt_calls = 0

            def lookup(self, _thread_key: str) -> SandboxHandle:
                return old_route

            def adopt(self, _thread_key: str) -> object:
                self.adopt_calls += 1
                raise AssertionError("open lineage must bypass same-repo adoption")

        class Workspace:
            handoffs = 0

            def select_repository(self, **_kwargs: object) -> str:
                return "acme-corp/acme-bot"

            def claim_or_resume_with_handle(self, **kwargs: object) -> object:
                self.handoffs += 1
                assert kwargs["replace_handle"] == old_route
                raise RuntimeError("late workspace handoff lost its route fence")

        class Runner(_LineageFenceRunner):
            def __init__(self) -> None:
                super().__init__(
                    {
                        "status": SessionStatus.AWAITING_APPROVAL.value,
                        "turn_active": False,
                        "history_durable": True,
                    }
                )
                self.steers: list[object] = []
                self.starts: list[object] = []

            async def steer(self, *args: object, **kwargs: object) -> bool:
                self.steers.append((args, kwargs))
                return False

            async def start_turn(self, *args: object, **kwargs: object) -> object:
                self.starts.append((args, kwargs))
                raise AssertionError("model start crossed a lost route fence")

        substrate = Substrate()
        workspace = Workspace()
        runner = Runner()
        kernel = object.__new__(Kernel)
        kernel._factory_work_item_events = set()  # type: ignore[attr-defined]
        kernel._work_item_runs = {}  # type: ignore[attr-defined]
        kernel._substrate = substrate  # type: ignore[attr-defined]
        kernel._workspace = workspace  # type: ignore[attr-defined]
        kernel._runner = runner  # type: ignore[attr-defined]

        with pytest.raises(RuntimeError, match="lost its route fence"):
            await kernel._route_and_start(
                thread_key,
                SimpleNamespace(
                    text="Continue https://github.com/acme-corp/acme-bot",
                    user="U0REQUEST1",
                ),
                {},
                queued_event_id="test-event",
                workspace_deployment_id=uuid.UUID("11111111-1111-4111-8111-111111111111"),
                agent_name="acme-bot",
                lineage_branch="curie/thread-lineage-example",
                lineage_head="a" * 40,
                force_lineage_replacement=True,
                pending_publication_approval=False,
                workspace_inference=_WorkspaceInferenceCarry(),
            )

        assert substrate.adopt_calls == 0
        assert workspace.handoffs == 1
        assert runner.steers == []
        assert runner.starts == []

    asyncio.run(go())


@pytest.mark.parametrize(
    ("route_head", "lineage_head", "route_outcome_revision"),
    [
        (None, "b" * 40, 1),
        ("c" * 40, "b" * 40, 1),
        ("b" * 40, "b" * 40, 0),
        ("a" * 40, None, 0),
    ],
)
def test_verified_lineage_with_mismatched_route_state_cold_reconciles(
    route_head: str | None,
    lineage_head: str | None,
    route_outcome_revision: int,
) -> None:
    async def go() -> None:
        from curie_worker.approvals import PublicationLineage
        from curie_worker.kernel import Kernel
        from curie_worker.sandbox.types import SandboxHandle

        thread_key = "slack:C0EXAMPLE1:1700000000.000100"
        deployment_id = uuid.UUID("11111111-1111-4111-8111-111111111111")
        old_route = SandboxHandle(
            thread_key=thread_key,
            claim_name="claim-old",
            sandbox_name="sandbox-old",
            namespace="test-ns",
            service_fqdn="old-runner.example.test",
            port=8080,
            session_id="session-old",
            token="route-token",
            workspace_repo="acme-corp/acme-bot",
            workspace_materialized_head=route_head,
            publication_visible_outcome_revision=route_outcome_revision,
        )

        class PublicationApi:
            async def get_publication_lineage(
                self, requested_deployment: uuid.UUID, conversation: str, repo: str
            ) -> PublicationLineage:
                assert (requested_deployment, conversation, repo) == (
                    deployment_id,
                    thread_key,
                    "acme-corp/acme-bot",
                )
                return PublicationLineage(
                    id=uuid.UUID("55555555-5555-4555-8555-555555555555"),
                    deployment_id=deployment_id,
                    conversation_id=conversation,
                    repo_full_name=repo,
                    base_sha="1" * 40,
                    branch="curie/thread-lineage-example",
                    pr_number=42 if lineage_head is not None else None,
                    pr_url=(
                        "https://github.com/acme-corp/acme-bot/pull/42"
                        if lineage_head is not None
                        else None
                    ),
                    head_sha=lineage_head,
                    state="open",
                    version=2,
                    latest_revision=1,
                    has_pending_revision=False,
                    has_pending_outcome=False,
                    visible_outcome_revision=1,
                )

        class Substrate:
            def lookup(self, _thread_key: str) -> SandboxHandle:
                return old_route

            def adopt(self, _thread_key: str) -> object:
                raise AssertionError("mismatched lineage head must not reuse the route")

        class Workspace:
            def select_repository(self, **_kwargs: object) -> str:
                return "acme-corp/acme-bot"

            def claim_or_resume_with_handle(self, **kwargs: object) -> object:
                assert kwargs["replace_handle"] == old_route
                assert kwargs["lineage_branch"] == (
                    "curie/thread-lineage-example" if lineage_head is not None else None
                )
                assert kwargs["lineage_head"] == lineage_head
                assert kwargs["lineage_base_sha"] == ("1" * 40 if lineage_head is None else None)
                assert kwargs["publication_visible_outcome_revision"] == 1
                raise RuntimeError("captured cold lineage reconciliation")

        kernel = object.__new__(Kernel)
        kernel._factory_work_item_events = set()  # type: ignore[attr-defined]
        kernel._work_item_runs = {}  # type: ignore[attr-defined]
        kernel._substrate = Substrate()  # type: ignore[attr-defined]
        kernel._workspace = Workspace()  # type: ignore[attr-defined]
        kernel._publication_creator = PublicationApi()  # type: ignore[attr-defined]
        kernel._runner = _LineageFenceRunner(  # type: ignore[attr-defined]
            {
                "status": SessionStatus.DONE.value,
                "turn_active": False,
                "history_durable": True,
            }
        )

        with pytest.raises(RuntimeError, match="captured cold lineage reconciliation"):
            await kernel._route_and_start(
                thread_key,
                SimpleNamespace(
                    text="Continue https://github.com/acme-corp/acme-bot",
                    user="U0REQUEST1",
                ),
                {},
                queued_event_id="test-event",
                workspace_deployment_id=deployment_id,
                agent_name="acme-bot",
                workspace_inference=_WorkspaceInferenceCarry(),
            )

    asyncio.run(go())


@pytest.mark.parametrize("turn_active", [False, True])
@pytest.mark.parametrize("lineage_head", ["b" * 40, None])
def test_verified_lineage_at_materialized_route_head_reuses_existing_session(
    turn_active: bool,
    lineage_head: str | None,
) -> None:
    async def go() -> None:
        from curie_worker.approvals import PublicationLineage
        from curie_worker.kernel import Kernel
        from curie_worker.sandbox.types import SandboxHandle

        thread_key = "slack:C0EXAMPLE1:1700000000.000100"
        deployment_id = uuid.UUID("11111111-1111-4111-8111-111111111111")
        route = SandboxHandle(
            thread_key=thread_key,
            claim_name="claim-lineage",
            sandbox_name="sandbox-lineage",
            namespace="test-ns",
            service_fqdn="lineage-runner.example.test",
            port=8080,
            session_id="session-lineage",
            token="route-token",
            workspace_repo="acme-corp/acme-bot",
            workspace_materialized_head=lineage_head or "1" * 40,
            publication_visible_outcome_revision=1,
        )

        class PublicationApi:
            async def get_publication_lineage(
                self, requested_deployment: uuid.UUID, conversation: str, repo: str
            ) -> PublicationLineage:
                return PublicationLineage(
                    id=uuid.UUID("55555555-5555-4555-8555-555555555555"),
                    deployment_id=requested_deployment,
                    conversation_id=conversation,
                    repo_full_name=repo,
                    base_sha="1" * 40,
                    branch="curie/thread-lineage-example",
                    pr_number=42 if lineage_head is not None else None,
                    pr_url=(
                        "https://github.com/acme-corp/acme-bot/pull/42"
                        if lineage_head is not None
                        else None
                    ),
                    head_sha=lineage_head,
                    state="open",
                    version=2,
                    latest_revision=1,
                    has_pending_revision=False,
                    has_pending_outcome=False,
                    visible_outcome_revision=1,
                )

        class Substrate:
            adopt_calls = 0

            def lookup(self, _thread_key: str) -> SandboxHandle:
                return route

            def adopt(self, _thread_key: str) -> SandboxHandle:
                self.adopt_calls += 1
                return route

        class Workspace:
            touches: list[tuple[str, int]] = []

            def select_repository(self, **_kwargs: object) -> str:
                return "acme-corp/acme-bot"

            def touch(self, requested_thread: str, *, ttl_seconds: int) -> bool:
                self.touches.append((requested_thread, ttl_seconds))
                return True

            def claim_or_resume_with_handle(self, **_kwargs: object) -> object:
                raise AssertionError("matching lineage head must reuse the live route")

        class Runner:
            status_reads = 0
            steers = 0
            starts = 0

            async def status(self, *_args: object, **_kwargs: object) -> dict[str, object]:
                self.status_reads += 1
                return {"turn_active": turn_active}

            async def steer(self, *_args: object, **_kwargs: object) -> bool:
                self.steers += 1
                return turn_active

            async def start_turn(self, *_args: object, **_kwargs: object) -> object:
                self.starts += 1
                return SimpleNamespace()

        substrate = Substrate()
        workspace = Workspace()
        runner = Runner()
        kernel = object.__new__(Kernel)
        kernel._factory_work_item_events = set()  # type: ignore[attr-defined]
        kernel._work_item_runs = {}  # type: ignore[attr-defined]
        kernel._substrate = substrate  # type: ignore[attr-defined]
        kernel._workspace = workspace  # type: ignore[attr-defined]
        kernel._publication_creator = PublicationApi()  # type: ignore[attr-defined]
        kernel._runner = runner  # type: ignore[attr-defined]
        kernel._route_ttl_seconds = 60  # type: ignore[attr-defined]

        result = await kernel._route_and_start(
            thread_key,
            SimpleNamespace(
                text="Continue https://github.com/acme-corp/acme-bot",
                user="U0REQUEST1",
                tool_access=None,
            ),
            {},
            queued_event_id="test-event",
            workspace_deployment_id=deployment_id,
            agent_name="acme-bot",
            workspace_inference=_WorkspaceInferenceCarry(),
        )

        assert substrate.adopt_calls == 1
        assert workspace.touches == [(thread_key, 60)]
        assert runner.steers == 1
        assert result.steered is turn_active
        assert runner.starts == (0 if turn_active else 1)

    asyncio.run(go())


def test_headless_visible_outcome_cold_reconciles_once_then_live_followup_steers() -> None:
    async def go() -> None:
        from curie_worker.approvals import PublicationLineage
        from curie_worker.kernel import Kernel
        from curie_worker.sandbox.types import SandboxHandle

        thread_key = "slack:C0EXAMPLE1:1700000000.000100"
        deployment_id = uuid.UUID("11111111-1111-4111-8111-111111111111")
        base_sha = "1" * 40
        dirty_route = SandboxHandle(
            thread_key=thread_key,
            claim_name="claim-dirty",
            sandbox_name="sandbox-dirty",
            namespace="test-ns",
            service_fqdn="dirty-runner.example.test",
            port=8080,
            session_id="session-lineage",
            token="route-token",
            workspace_repo="acme-corp/acme-bot",
            workspace_materialized_head="d" * 40,
        )
        reconciled_route = SandboxHandle(
            thread_key=thread_key,
            claim_name="claim-reconciled",
            sandbox_name="sandbox-reconciled",
            namespace="test-ns",
            service_fqdn="reconciled-runner.example.test",
            port=8080,
            session_id="session-lineage",
            token="route-token",
            workspace_repo="acme-corp/acme-bot",
            workspace_materialized_head=base_sha,
            publication_visible_outcome_revision=1,
            generation=1,
        )

        class PublicationApi:
            async def get_publication_lineage(
                self, requested_deployment: uuid.UUID, conversation: str, repo: str
            ) -> PublicationLineage:
                return PublicationLineage(
                    id=uuid.UUID("55555555-5555-4555-8555-555555555555"),
                    deployment_id=requested_deployment,
                    conversation_id=conversation,
                    repo_full_name=repo,
                    base_sha=base_sha,
                    branch="curie/thread-lineage-example",
                    pr_number=None,
                    pr_url=None,
                    head_sha=None,
                    state="open",
                    version=1,
                    latest_revision=1,
                    has_pending_revision=False,
                    has_pending_outcome=False,
                    visible_outcome_revision=1,
                )

        class Substrate:
            current = dirty_route
            adopts = 0

            def lookup(self, _thread_key: str) -> SandboxHandle:
                return self.current

            def adopt(self, _thread_key: str) -> SandboxHandle:
                self.adopts += 1
                return self.current

        class Workspace:
            replacements = 0
            touches = 0

            def select_repository(self, **_kwargs: object) -> str:
                return "acme-corp/acme-bot"

            def claim_or_resume_with_handle(self, **kwargs: object) -> object:
                self.replacements += 1
                assert kwargs["replace_handle"] == dirty_route
                assert kwargs["lineage_branch"] is None
                assert kwargs["lineage_head"] is None
                assert kwargs["lineage_base_sha"] == base_sha
                substrate.current = reconciled_route
                return SimpleNamespace(handle=reconciled_route)

            def touch(self, _thread_key: str, *, ttl_seconds: int) -> bool:
                assert ttl_seconds == 60
                self.touches += 1
                return True

        class Runner:
            live = False
            starts = 0
            steers = 0

            async def status(self, handle_url: str, **_kwargs: object) -> dict[str, object]:
                if handle_url == dirty_route.base_url:
                    return {
                        "status": SessionStatus.DONE.value,
                        "turn_active": False,
                        "history_durable": True,
                    }
                return {"turn_active": self.live}

            async def steer(self, *_args: object, **_kwargs: object) -> bool:
                self.steers += 1
                return self.live

            async def start_turn(self, *_args: object, **_kwargs: object) -> object:
                self.starts += 1
                return SimpleNamespace()

        substrate = Substrate()
        workspace = Workspace()
        runner = Runner()
        kernel = object.__new__(Kernel)
        kernel._factory_work_item_events = set()  # type: ignore[attr-defined]
        kernel._work_item_runs = {}  # type: ignore[attr-defined]
        kernel._substrate = substrate  # type: ignore[attr-defined]
        kernel._workspace = workspace  # type: ignore[attr-defined]
        kernel._publication_creator = PublicationApi()  # type: ignore[attr-defined]
        kernel._runner = runner  # type: ignore[attr-defined]
        kernel._route_ttl_seconds = 60  # type: ignore[attr-defined]
        event = SimpleNamespace(
            text="Continue https://github.com/acme-corp/acme-bot",
            user="U0REQUEST1",
            tool_access=None,
        )

        first = await kernel._route_and_start(
            thread_key,
            event,
            {},
            queued_event_id="test-event",
            workspace_deployment_id=deployment_id,
            agent_name="acme-bot",
            workspace_inference=_WorkspaceInferenceCarry(),
        )
        runner.live = True
        second = await kernel._route_and_start(
            thread_key,
            event,
            {},
            queued_event_id="test-event",
            workspace_deployment_id=deployment_id,
            agent_name="acme-bot",
            workspace_inference=_WorkspaceInferenceCarry(),
        )

        assert first.steered is False
        assert second.steered is True
        assert workspace.replacements == 1
        assert workspace.touches == 1
        assert substrate.adopts == 1
        assert runner.starts == 1
        assert runner.steers == 2

    asyncio.run(go())


@pytest.mark.parametrize(
    ("has_pending_revision", "has_pending_outcome", "lineage_head"),
    [(True, False, "b" * 40), (False, True, None)],
)
def test_api_pending_publication_work_is_fenced_before_lineage_handoff_probe(
    has_pending_revision: bool,
    has_pending_outcome: bool,
    lineage_head: str | None,
) -> None:
    async def go() -> None:
        from curie_worker.approvals import PublicationLineage
        from curie_worker.kernel import Kernel, ThreadBusyError
        from curie_worker.sandbox.types import SandboxHandle

        thread_key = "slack:C0EXAMPLE1:1700000000.000100"
        deployment_id = uuid.UUID("11111111-1111-4111-8111-111111111111")
        old_route = SandboxHandle(
            thread_key=thread_key,
            claim_name="claim-old",
            sandbox_name="sandbox-old",
            namespace="test-ns",
            service_fqdn="old-runner.example.test",
            port=8080,
            session_id="session-old",
            token="route-token",
            workspace_repo="acme-corp/acme-private",
        )

        class PublicationApi:
            reads: list[tuple[uuid.UUID, str, str]] = []

            async def get_publication_lineage(
                self, requested_deployment: uuid.UUID, conversation: str, repo: str
            ) -> PublicationLineage:
                self.reads.append((requested_deployment, conversation, repo))
                return PublicationLineage(
                    id=uuid.UUID("55555555-5555-4555-8555-555555555555"),
                    deployment_id=deployment_id,
                    conversation_id=conversation,
                    repo_full_name=repo,
                    base_sha="a" * 40,
                    branch="curie/thread-lineage-example",
                    pr_number=123 if lineage_head is not None else None,
                    pr_url=(
                        "https://github.com/acme-corp/acme-private/pull/123"
                        if lineage_head is not None
                        else None
                    ),
                    head_sha=lineage_head,
                    state="open",
                    version=3,
                    latest_revision=2,
                    has_pending_revision=has_pending_revision,
                    has_pending_outcome=has_pending_outcome,
                    visible_outcome_revision=0,
                )

        class Workspace:
            handoffs = 0

            def select_repository(self, **_kwargs: object) -> str:
                return "acme-corp/acme-private"

            def claim_or_resume_with_handle(self, **_kwargs: object) -> object:
                self.handoffs += 1
                raise AssertionError("pending revision crossed the lineage fence")

        class Substrate:
            def lookup(self, _thread_key: str) -> SandboxHandle:
                return old_route

        publication_api = PublicationApi()
        workspace = Workspace()
        runner = _LineageFenceRunner(
            {
                "status": SessionStatus.AWAITING_APPROVAL.value,
                "turn_active": False,
                "history_durable": True,
            }
        )
        kernel = object.__new__(Kernel)
        kernel._factory_work_item_events = set()  # type: ignore[attr-defined]
        kernel._work_item_runs = {}  # type: ignore[attr-defined]
        kernel._workspace = workspace  # type: ignore[attr-defined]
        kernel._substrate = Substrate()  # type: ignore[attr-defined]
        kernel._runner = runner  # type: ignore[attr-defined]
        kernel._publication_creator = publication_api  # type: ignore[attr-defined]

        with pytest.raises(ThreadBusyError, match="pending publication revision"):
            await kernel._route_and_start(
                thread_key,
                SimpleNamespace(
                    text="Continue https://github.com/acme-corp/acme-private",
                    user="U0REQUEST1",
                    ts="1700000000.000100",
                ),
                {"CURIE_RUNNER_TOKEN": "runner-token"},
                queued_event_id="test-event",
                workspace_deployment_id=deployment_id,
                agent_name="acme-bot",
                workspace_inference=_WorkspaceInferenceCarry(),
            )

        assert publication_api.reads == [
            (
                deployment_id,
                "slack:C0EXAMPLE1:1700000000.000100",
                "acme-corp/acme-private",
            )
        ]
        assert workspace.handoffs == 0
        assert runner.reads == 0

    asyncio.run(go())


def test_pending_first_revision_is_a_terminal_reply_before_dirty_route_adoption(
    make_harness,
) -> None:
    """A headless first revision fences the failed-suspend runner immediately."""

    from curie_worker.approvals import PublicationLineage
    from curie_worker.sandbox.types import SandboxHandle

    deployment_id = uuid.UUID("11111111-1111-4111-8111-111111111111")
    thread = "1700000000.000100"
    thread_key = f"slack:C1:{thread}"
    old_route = SandboxHandle(
        thread_key=thread_key,
        claim_name="claim-old",
        sandbox_name="sandbox-old",
        namespace="test-ns",
        service_fqdn="old-runner.example.test",
        port=8080,
        session_id="session-old",
        token="route-token",
        workspace_repo="acme-corp/acme-private",
    )

    class Binding(GrantBinding):
        async def resolve(self, kind: str, adapter: str | None, channel: str):  # noqa: ANN201
            from curie_worker.binding import ResolvedDeployment

            return ResolvedDeployment(
                agent_id=self.agent_id,
                agent_name="acme-bot",
                deployment_id=deployment_id,
                workspace_enabled=True,
                version_id=uuid.uuid4(),
                version_label="v1",
                bundle_ref=None,
                max_usd_per_day=None,
                max_output_tokens_per_run=None,
            )

    class PublicationApi:
        reads: list[tuple[uuid.UUID, str, str]] = []

        async def get_publication_lineage(
            self, requested_deployment: uuid.UUID, conversation: str, repo: str
        ) -> PublicationLineage:
            self.reads.append((requested_deployment, conversation, repo))
            return PublicationLineage(
                id=uuid.UUID("55555555-5555-4555-8555-555555555555"),
                deployment_id=deployment_id,
                conversation_id=conversation,
                repo_full_name=repo,
                base_sha="a" * 40,
                branch="curie/thread-lineage-example",
                pr_number=None,
                pr_url=None,
                head_sha=None,
                state="open",
                version=1,
                latest_revision=1,
                has_pending_revision=True,
                has_pending_outcome=False,
                visible_outcome_revision=0,
            )

    class Workspace:
        def select_repository(self, **_kwargs: object) -> str:
            return "acme-corp/acme-private"

        def claim_or_resume_with_handle(self, **_kwargs: object) -> object:
            raise AssertionError("pending first revision reached workspace preparation")

    class Substrate:
        lookup_calls = 0
        adopt_calls = 0

        def lookup(self, _thread_key: str) -> SandboxHandle:
            self.lookup_calls += 1
            return old_route

        def adopt(self, _thread_key: str) -> object:
            self.adopt_calls += 1
            raise AssertionError("pending first revision adopted the dirty live route")

    async def go() -> None:
        publication_api = PublicationApi()
        substrate = Substrate()
        binding = Binding(grant_event_id="unused", grant_tool="unused")
        async with make_harness(binding=binding, publication_creator=publication_api) as h:
            h.runner.session_status = SessionStatus.AWAITING_APPROVAL.value
            h.kernel._workspace = Workspace()  # type: ignore[assignment]
            h.kernel._substrate = substrate  # type: ignore[assignment]

            await h.kernel.process_event(
                _qevent(
                    "Continue https://github.com/acme-corp/acme-private",
                    thread=thread,
                )
            )

            assert publication_api.reads == [(deployment_id, thread_key, "acme-corp/acme-private")]
            assert substrate.lookup_calls == 0
            assert substrate.adopt_calls == 0
            assert h.runner.opened == []
            assert h.sink.last_text == (
                "This thread already has a publication awaiting approval or completion. "
                "Resolve it before continuing."
            )
            assert len(h.sink.completions) == 1

    asyncio.run(go())


def test_api_stale_head_conflict_stops_before_workspace_or_model_for_private_repo() -> None:
    async def go() -> None:
        from curie_worker.kernel import Kernel

        deployment_id = uuid.UUID("11111111-1111-4111-8111-111111111111")

        class PublicationApi:
            async def get_publication_lineage(self, *_args: object) -> object:
                raise ApprovalBackendError(
                    "publication lineage remote head conflicts with the stored head"
                )

        class Workspace:
            def select_repository(self, **_kwargs: object) -> str:
                return "acme-corp/acme-private"

            def claim_or_resume_with_handle(self, **_kwargs: object) -> object:
                raise AssertionError("stale API truth reached workspace preparation")

        class Substrate:
            def lookup(self, _thread_key: str) -> object:
                raise AssertionError("stale API truth reached sandbox routing")

        kernel = object.__new__(Kernel)
        kernel._factory_work_item_events = set()  # type: ignore[attr-defined]
        kernel._work_item_runs = {}  # type: ignore[attr-defined]
        kernel._workspace = Workspace()  # type: ignore[attr-defined]
        kernel._substrate = Substrate()  # type: ignore[attr-defined]
        kernel._runner = _LineageFenceRunner({})  # type: ignore[attr-defined]
        kernel._publication_creator = PublicationApi()  # type: ignore[attr-defined]

        with pytest.raises(ApprovalBackendError, match="remote head conflicts"):
            await kernel._route_and_start(
                "slack:C0EXAMPLE1:1700000000.000100",
                SimpleNamespace(
                    text="Continue https://github.com/acme-corp/acme-private",
                    user="U0REQUEST1",
                    ts="1700000000.000100",
                ),
                {},
                queued_event_id="test-event",
                workspace_deployment_id=deployment_id,
                agent_name="acme-bot",
                workspace_inference=_WorkspaceInferenceCarry(),
            )

    asyncio.run(go())


@pytest.mark.parametrize(
    ("response_status", "detail", "message"),
    [
        (
            409,
            {
                "code": "publication.lineage_stale",
                "message": "GitHub pull request head differs from the stored lineage; "
                "restore the expected head or start a new thread.",
            },
            "GitHub pull request head differs from the stored lineage; restore the "
            "expected head or start a new thread.",
        ),
        (
            409,
            {
                "code": "publication.lineage_terminal",
                "message": "The pull request for this thread is merged or closed; "
                "start a new thread.",
            },
            "The pull request for this thread is merged or closed; start a new thread.",
        ),
        (
            503,
            {
                "code": "publication.github_unavailable",
                "message": "GitHub could not verify this thread's pull request. "
                "Try again later; no model turn or publication was started.",
            },
            "GitHub could not verify this thread's pull request. Try again later; "
            "no model turn or publication was started.",
        ),
        (
            409,
            "conversation has no selected repository workspace",
            "conversation has no selected repository workspace",
        ),
        (
            409,
            "publication repository differs from the thread workspace",
            "publication repository differs from the thread workspace",
        ),
        (
            409,
            "thread workspace repository is no longer allowed",
            "thread workspace repository is no longer allowed",
        ),
    ],
)
def test_lineage_api_refusal_is_a_user_visible_terminal_process_event(
    make_harness,
    response_status: int,
    detail: object,
    message: str,
) -> None:
    """A safe API refusal answers the requester instead of retrying to dead-letter."""

    deployment_id = uuid.UUID("11111111-1111-4111-8111-111111111111")

    class Binding(GrantBinding):
        async def resolve(self, kind: str, adapter: str | None, channel: str):  # noqa: ANN201
            from curie_worker.binding import ResolvedDeployment

            return ResolvedDeployment(
                agent_id=self.agent_id,
                agent_name="acme-bot",
                deployment_id=deployment_id,
                workspace_enabled=True,
                version_id=uuid.uuid4(),
                version_label="v1",
                bundle_ref=None,
                max_usd_per_day=None,
                max_output_tokens_per_run=None,
            )

    async def go() -> None:
        thread = "1700000000.000100"
        thread_key = f"slack:C1:{thread}"

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v1/internal/publications/lineage"
            assert request.url.params["conversation_id"] == thread_key
            return httpx.Response(response_status, json={"detail": detail})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            publication_api = ApprovalClient(
                api_base_url="https://api.example.com",
                api_key="",
                worker_token="worker-token",
                client=client,
                read_timeout_s=1,
            )
            binding = Binding(grant_event_id="unused", grant_tool="unused")
            async with make_harness(binding=binding, publication_creator=publication_api) as h:

                class Substrate:
                    lookup_calls = 0
                    adopt_calls = 0

                    def lookup(self, _thread_key: str) -> object:
                        self.lookup_calls += 1
                        raise AssertionError("lineage refusal reached route lookup")

                    def adopt(self, _thread_key: str) -> object:
                        self.adopt_calls += 1
                        raise AssertionError("lineage refusal reached route adoption")

                class Workspace:
                    def select_repository(self, **kwargs: object) -> str:
                        assert kwargs["thread_key"] == thread_key
                        return "acme-corp/acme-private"

                    def claim_or_resume_with_handle(self, **_kwargs: object) -> object:
                        raise AssertionError("lineage refusal reached workspace preparation")

                substrate = Substrate()
                h.kernel._workspace = Workspace()  # type: ignore[assignment]
                h.kernel._substrate = substrate  # type: ignore[assignment]
                await h.kernel.process_event(
                    _qevent(
                        "Continue https://github.com/acme-corp/acme-private",
                        thread=thread,
                    )
                )

                assert h.runner.opened == []
                assert h.fake_k8s.claim_envs == []
                assert substrate.lookup_calls == 0
                assert substrate.adopt_calls == 0
                assert h.sink.last_text == message
                assert h.sink.completions[0].target.conversation_id == thread

    asyncio.run(go())


@dataclass
class _PublicationRun:
    """What a publication-approval scenario hands back to its test."""

    h: object
    publication_api: object
    workspace: object


@asynccontextmanager
async def _publication_run(
    make_harness,  # noqa: ANN001
    monkeypatch: pytest.MonkeyPatch,
    *,
    deployment_id: uuid.UUID,
    thread: str,
    approval_routes: dict | None = None,
    approval_route: str | None = None,
    publication_creator: object | None = None,
    lease: object | None = None,
) -> AsyncIterator[_PublicationRun]:
    """Drive one Slack turn that ends in a publication approval gate.

    The shared setup of the publication tests: a workspace-enabled binding, a
    recording publication API with no prior lineage, a fixed private repository,
    a prepared README snapshot, and one ``process_event`` of a publish request
    in ``C0EXAMPLE1``. The harness stays open while the test asserts.
    ``publication_creator`` replaces the recording API, and ``lease`` is passed
    to ``process_event`` as the turn's delivery lease.
    """
    from curie_worker.approvals import CreatedPublication
    from curie_worker.runner_client import RunnerWorkspaceSnapshot

    class Binding(GrantBinding):
        async def resolve(self, kind: str, adapter: str | None, channel: str):  # noqa: ANN201
            from curie_worker.binding import ResolvedDeployment

            extra = {} if approval_routes is None else {"approval_routes": approval_routes}
            return ResolvedDeployment(
                agent_id=self.agent_id,
                agent_name="acme-bot",
                deployment_id=deployment_id,
                workspace_enabled=True,
                version_id=uuid.uuid4(),
                version_label="v1",
                bundle_ref=None,
                max_usd_per_day=None,
                max_output_tokens_per_run=None,
                **extra,
            )

    class PublicationApi:
        def __init__(self) -> None:
            self.reads: list[tuple[uuid.UUID, str, str]] = []
            self.creates: list[PublicationCreateRequest] = []

        async def get_publication_lineage(
            self, requested_deployment: uuid.UUID, conversation: str, repo: str
        ) -> None:
            self.reads.append((requested_deployment, conversation, repo))
            return None

        async def create_publication(
            self, request: PublicationCreateRequest, *, budget_s: float = 120
        ) -> CreatedPublication:
            self.creates.append(request)
            return CreatedPublication(
                id="publication-example",
                approval_id="approval-example",
                status="pending",
            )

    class Workspace:
        def __init__(self) -> None:
            self.substrate = None
            self.selected: list[str] = []
            self.released: list[str] = []

        def select_repository(self, **kwargs: object) -> str:
            self.selected.append(str(kwargs["thread_key"]))
            return "acme-corp/acme-private"

        def claim_or_resume_with_handle(self, **kwargs: object) -> object:
            assert self.substrate is not None
            return SimpleNamespace(
                handle=self.substrate.claim(
                    str(kwargs["thread_key"]),
                    env=kwargs["env"],
                    agent_name=kwargs["agent_name"],
                    workspace_repo=kwargs["repo_full_name"],
                )
            )

        def release(self, thread_identity: str) -> None:
            self.released.append(thread_identity)

        def touch(self, _thread_identity: str, *, ttl_seconds: int) -> None:
            del ttl_seconds

    publication_api = PublicationApi() if publication_creator is None else publication_creator
    workspace = Workspace()
    binding = Binding(grant_event_id="unused", grant_tool="unused")
    async with make_harness(binding=binding, publication_creator=publication_api) as h:
        workspace.substrate = h.substrate
        h.kernel._workspace = workspace  # type: ignore[assignment]
        route = {} if approval_route is None else {"approval_route": approval_route}
        h.runner.default_script = [
            Final(
                text="Ready to publish",
                status=AWAITING,
                approval_summary="Publish the prepared changes",
                approval_gate_kind="permission",
                approval_granted_tool="mcp__curie__publish_changes",
                **route,
            )
        ]

        async def snapshot(*_args: object, **_kwargs: object) -> RunnerWorkspaceSnapshot:
            return RunnerWorkspaceSnapshot(
                repo_full_name="acme-corp/acme-private",
                base_sha="a" * 40,
                patch=b"diff --git a/README.md b/README.md\n",
                changed_paths=("README.md",),
                contains_workflow_files=False,
                publication_title="Update README",
                publication_body="Prepared by acme-bot.",
            )

        monkeypatch.setattr(h.kernel._runner, "snapshot", snapshot)
        monkeypatch.setattr(
            "curie_worker.kernel.validate_snapshot_against_base",
            lambda *_args, **_kwargs: None,
        )
        await h.kernel.process_event(
            _qevent(
                "Publish https://github.com/acme-corp/acme-private",
                thread=thread,
                channel="C0EXAMPLE1",
            ),
            lease=lease,
        )
        yield _PublicationRun(h=h, publication_api=publication_api, workspace=workspace)


def test_slack_publication_ownership_uses_scoped_key_but_replies_use_bare_thread(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lineage ownership is canonical; the adapter thread id remains Slack-shaped."""

    deployment_id = uuid.UUID("11111111-1111-4111-8111-111111111111")
    thread = "1700000000.000100"
    thread_key = f"slack:C0EXAMPLE1:{thread}"

    async def go() -> None:
        async with _publication_run(
            make_harness, monkeypatch, deployment_id=deployment_id, thread=thread
        ) as run:
            creates = run.publication_api.creates
            assert run.workspace.selected == [thread_key]
            assert run.publication_api.reads == [
                (deployment_id, thread_key, "acme-corp/acme-private")
            ]
            assert [request.conversation_id for request in creates] == [thread_key]
            assert [request.reply_conversation_id for request in creates] == [thread]
            assert run.workspace.released == [thread_key]
            assert run.h.sink.completions[0].target.conversation_id == thread

    asyncio.run(go())


# --- #2659: the inferred repository line sits above the approval notice --------


def test_ordinary_approval_notice_keeps_the_announcement_above_it(make_harness) -> None:
    """The announcement is its own block before the notice, never in the record."""

    deployment_id = uuid.UUID("22222222-2222-4222-8222-222222222659")

    class Binding(GrantBinding):
        async def resolve(self, kind: str, adapter: str | None, channel: str):  # noqa: ANN201
            from curie_worker.binding import ResolvedDeployment

            return ResolvedDeployment(
                agent_id=self.agent_id,
                agent_name="acme-bot",
                deployment_id=deployment_id,
                workspace_enabled=True,
                version_id=uuid.uuid4(),
                version_label="v1",
                bundle_ref=None,
                max_usd_per_day=None,
                max_output_tokens_per_run=None,
            )

    class Workspace:
        def __init__(self) -> None:
            self.substrate = None
            self.selections: list[object] = []

        def select_repository(self, **kwargs: object) -> str | None:
            self.selections.append(kwargs["repo_full_name"])
            return "acme-corp/acme-bot"

        def claim_or_resume_with_handle(self, **kwargs: object) -> object:
            assert self.substrate is not None
            return SimpleNamespace(
                handle=self.substrate.claim(
                    str(kwargs["thread_key"]),
                    env=kwargs["env"],
                    agent_name=kwargs["agent_name"],
                    workspace_repo=kwargs["repo_full_name"],
                ),
                prepared=None,
            )

        def release(self, _thread_identity: str) -> None:
            return None

        def touch(self, _thread_identity: str, *, ttl_seconds: int) -> bool:
            return ttl_seconds > 0

    async def go() -> None:
        approvals = RecordingApprovals()
        workspace = Workspace()
        binding = Binding(grant_event_id="unused", grant_tool="unused")
        async with make_harness(binding=binding, approvals=approvals) as h:
            workspace.substrate = h.substrate
            h.kernel._workspace = workspace  # type: ignore[assignment]
            h.runner.default_script = _awaiting_script("Give ACME a 20% discount")

            await h.kernel.process_event(
                _qevent(
                    "Make a change in https://github.com/acme-corp/acme-bot: discount",
                    thread="tAnnounceApproval",
                )
            )

            assert workspace.selections == ["acme-corp/acme-bot"]
            assert len(approvals.requests) == 1
            assert approvals.requests[0].summary == "Give ACME a 20% discount"
            assert "Working in" not in approvals.requests[0].summary
            assert h.sink.last_text is not None
            assert h.sink.last_text.split("\n\n") == [
                "Requesting sign-off",
                "Working in acme-corp/acme-bot, from the repository named in your message.",
                # The card follows in this thread, so the notice is one line
                # (ADR-0179 decision 2); the announcement still precedes it.
                _SHORT_NOTICE,
            ]

    asyncio.run(go())


def test_publication_notice_keeps_the_announcement_above_it(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The demo path: the publication notice keeps the announcement as its own block."""

    from curie_worker.approvals import CreatedPublication

    deployment_id = uuid.UUID("11111111-1111-4111-8111-111111112659")

    class Binding(GrantBinding):
        async def resolve(self, kind: str, adapter: str | None, channel: str):  # noqa: ANN201
            from curie_worker.binding import ResolvedDeployment

            return ResolvedDeployment(
                agent_id=self.agent_id,
                agent_name="acme-bot",
                deployment_id=deployment_id,
                workspace_enabled=True,
                version_id=uuid.uuid4(),
                version_label="v1",
                bundle_ref=None,
                max_usd_per_day=None,
                max_output_tokens_per_run=None,
            )

    class PublicationApi:
        def __init__(self) -> None:
            self.creates: list[PublicationCreateRequest] = []

        async def get_publication_lineage(
            self, requested_deployment: uuid.UUID, conversation: str, repo: str
        ) -> None:
            return None

        async def create_publication(
            self, request: PublicationCreateRequest, *, budget_s: float = 120
        ) -> CreatedPublication:
            self.creates.append(request)
            return CreatedPublication(
                id="publication-example",
                approval_id="approval-example",
                status="pending",
            )

    class Workspace:
        def __init__(self) -> None:
            self.substrate = None

        def select_repository(self, **kwargs: object) -> str:
            return "acme-corp/acme-private"

        def claim_or_resume_with_handle(self, **kwargs: object) -> object:
            assert self.substrate is not None
            return SimpleNamespace(
                handle=self.substrate.claim(
                    str(kwargs["thread_key"]),
                    env=kwargs["env"],
                    agent_name=kwargs["agent_name"],
                    workspace_repo=kwargs["repo_full_name"],
                )
            )

        def release(self, _thread_identity: str) -> None:
            return None

        def touch(self, _thread_identity: str, *, ttl_seconds: int) -> None:
            del ttl_seconds

    async def go() -> None:
        async with _publication_run(
            make_harness,
            monkeypatch,
            deployment_id=uuid.UUID("11111111-1111-4111-8111-111111112659"),
            thread="1700000000.002659",
        ) as run:
            h, publication_api = run.h, run.publication_api
            assert len(publication_api.creates) == 1
            assert "Working in" not in publication_api.creates[0].summary
            notices = [
                text
                for _address, _ref, text in h.sink.updates
                if "Awaiting approval (approval-example)" in text
            ]
            assert notices, h.sink.updates
            blocks = notices[-1].split("\n\n")
            marker = next(
                index
                for index, block in enumerate(blocks)
                if block.startswith("Awaiting approval (approval-example)")
            )
            assert marker >= 1, blocks
            assert blocks[marker - 1] == (
                "Working in acme-corp/acme-private, from the repository named in your message."
            )

    asyncio.run(go())


def test_private_lineage_head_reaches_handoff_without_a_publication_credential() -> None:
    async def go() -> None:
        from curie_worker.approvals import PublicationLineage
        from curie_worker.kernel import Kernel
        from curie_worker.sandbox.types import SandboxHandle

        thread_key = "slack:C0EXAMPLE1:1700000000.000100"
        deployment_id = uuid.UUID("11111111-1111-4111-8111-111111111111")
        old_route = SandboxHandle(
            thread_key=thread_key,
            claim_name="claim-old",
            sandbox_name="sandbox-old",
            namespace="test-ns",
            service_fqdn="old-runner.example.test",
            port=8080,
            session_id="session-old",
            token="route-token",
            workspace_repo="acme-corp/acme-private",
        )

        class PublicationApi:
            async def get_publication_lineage(
                self, _deployment: uuid.UUID, conversation: str, repo: str
            ) -> PublicationLineage:
                return PublicationLineage(
                    id=uuid.UUID("55555555-5555-4555-8555-555555555555"),
                    deployment_id=deployment_id,
                    conversation_id=conversation,
                    repo_full_name=repo,
                    base_sha="a" * 40,
                    branch="curie/thread-lineage-example",
                    pr_number=123,
                    pr_url="https://github.com/acme-corp/acme-private/pull/123",
                    head_sha="b" * 40,
                    state="open",
                    version=3,
                    latest_revision=2,
                    has_pending_revision=False,
                    has_pending_outcome=False,
                    visible_outcome_revision=2,
                )

        class Workspace:
            claim: dict[str, object] | None = None

            def select_repository(self, **_kwargs: object) -> str:
                return "acme-corp/acme-private"

            def claim_or_resume_with_handle(self, **kwargs: object) -> object:
                self.claim = dict(kwargs)
                raise RuntimeError("captured safe lineage handoff")

        class Substrate:
            def lookup(self, _thread_key: str) -> SandboxHandle:
                return old_route

            def adopt(self, _thread_key: str) -> object:
                raise AssertionError("open lineage reused the dirty same-repo route")

        workspace = Workspace()
        kernel = object.__new__(Kernel)
        kernel._factory_work_item_events = set()  # type: ignore[attr-defined]
        kernel._work_item_runs = {}  # type: ignore[attr-defined]
        kernel._workspace = workspace  # type: ignore[attr-defined]
        kernel._substrate = Substrate()  # type: ignore[attr-defined]
        kernel._runner = _LineageFenceRunner(  # type: ignore[attr-defined]
            {
                "status": SessionStatus.AWAITING_APPROVAL.value,
                "turn_active": False,
                "history_durable": True,
            }
        )
        kernel._publication_creator = PublicationApi()  # type: ignore[attr-defined]

        with pytest.raises(RuntimeError, match="captured safe lineage handoff"):
            await kernel._route_and_start(
                thread_key,
                SimpleNamespace(
                    text="Continue https://github.com/acme-corp/acme-private",
                    user="U0REQUEST1",
                    ts="1700000000.000100",
                ),
                {"CURIE_RUNNER_TOKEN": "runner-token"},
                queued_event_id="test-event",
                workspace_deployment_id=deployment_id,
                agent_name="acme-bot",
                workspace_inference=_WorkspaceInferenceCarry(),
            )

        assert workspace.claim is not None
        assert workspace.claim["lineage_branch"] == "curie/thread-lineage-example"
        assert workspace.claim["lineage_head"] == "b" * 40
        assert workspace.claim["replace_handle"] == old_route
        serialized = json.dumps(workspace.claim, default=str).casefold()
        assert "authorization" not in serialized
        assert "publication-write" not in serialized
        assert "github_pat" not in serialized

    asyncio.run(go())


# The settled record a resolved approval reads back as: the one verdict the
# card-stamping tests below all pin their assertions to.
_APPROVED = SettledApproval(status="approved", resolved_by="U9", resolution_note="approved for Q3")


async def _pause_awaiting_approval(h, thread: str) -> None:
    """Run a turn to the approval pause on ``thread``.

    The live card is posted and its location remembered -- the worker is the only
    component that knows where it went -- which is the precondition every
    card-settling test below starts from, hence the exists-assert on the ref.
    """

    h.runner.default_script = _awaiting_script("Refund order 42")
    await h.kernel.process_event(_qevent("refund?", thread=thread))
    assert await h.async_redis.exists(h.config.approval_card_key("appr-1"))
    assert not await h.async_redis.exists(h.config.approval_card_key(thread))


async def _peek_card_ref(h, approval_id: str) -> dict | None:
    """Read a remembered card reference without consuming it."""

    raw = await h.async_redis.get(h.config.approval_card_key(approval_id))
    return None if raw is None else json.loads(raw)


def test_worker_approval_http_requests_carry_the_active_turn_parent() -> None:
    """Create, read, and publication siblings must retain the worker turn."""

    async def go() -> None:
        requests: list[httpx.Request] = []

        def handle(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if request.method == "GET":
                return httpx.Response(
                    200,
                    json={
                        "status": "approved",
                        "resolved_by": "U0APPROVER1",
                        "resolution_note": None,
                    },
                )
            if request.url.path == "/v1/internal/publications":
                return httpx.Response(
                    201,
                    json={
                        "id": str(uuid.uuid4()),
                        "approval_id": str(uuid.uuid4()),
                        "status": "pending",
                    },
                )
            return httpx.Response(
                201,
                json={"id": str(uuid.uuid4()), "status": "pending"},
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            client = ApprovalClient(
                api_base_url="https://api.example.test",
                api_key="platform-test-key",
                client=http,
                read_timeout_s=1.0,
                worker_token="worker-test-token",
            )
            approval = ApprovalRequest(
                conversation_id="thread-example",
                author="U0REQUEST1",
                summary="Approve the bounded action",
                reply_kind="slack",
                reply_channel="C0EXAMPLE1",
                reply_placeholder="1700000000.000001",
                dedupe_key="event-example",
            )
            publication = PublicationCreateRequest(
                deployment_id=uuid.uuid4(),
                conversation_id="thread-example",
                repo_full_name="acme-corp/acme-bot",
                author="U0REQUEST1",
                summary="Publish the bounded change",
                reply_kind="slack",
                reply_channel="C0EXAMPLE1",
                reply_placeholder="1700000000.000001",
                reply_endpoint=None,
                reply_adapter=None,
                dedupe_key="publication-example",
                base_sha="0123456789abcdef0123456789abcdef01234567",
                patch=b"diff --git a/README.md b/README.md\n",
                changed_paths=("README.md",),
                expires_in_seconds=600,
                title="Publish the bounded change",
                body="Approved platform publication.",
            )

            with _approval_http_parent():
                await client.create(approval)
                await client.get("00000000-0000-0000-0000-000000000001")
                await client.create_publication(publication)

        assert [request.method for request in requests] == ["POST", "GET", "POST"]
        assert all(request.headers.get("traceparent") == _HTTP_TRACEPARENT for request in requests)

    asyncio.run(go())


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # The API serializes the row's naive UTC ``resolved_at`` with no offset.
        ("2026-09-21T14:13:20", datetime(2026, 9, 21, 14, 13, 20, tzinfo=UTC)),
        (None, None),
        ("garbage", None),
    ],
)
def test_the_approval_read_carries_the_decision_time(
    raw: str | None, expected: datetime | None
) -> None:
    """ADR-0179 decision 1: the worker's settle path gets the time the click saw."""

    async def go() -> SettledApproval | None:
        def handle(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "status": "approved",
                    "resolved_by": "U0APPROVER1",
                    "resolution_note": None,
                    "resolved_at": raw,
                },
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            client = ApprovalClient(
                api_base_url="https://api.example.test",
                api_key="platform-test-key",
                client=http,
                read_timeout_s=1.0,
            )
            return await client.get("00000000-0000-0000-0000-000000000001")

    record = asyncio.run(go())
    assert record is not None
    assert record.resolved_at == expected


def test_private_lineage_truth_is_read_from_api_without_publication_credentials() -> None:
    """Routing gets refreshed PR truth from the trusted API, never GitHub directly."""

    async def go() -> None:
        seen: list[httpx.Request] = []
        deployment_id = uuid.UUID("11111111-1111-4111-8111-111111111111")

        def handle(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            assert request.url.path == "/v1/internal/publications/lineage"
            return httpx.Response(
                200,
                json={
                    "id": "55555555-5555-4555-8555-555555555555",
                    "deployment_id": str(deployment_id),
                    "conversation_id": "1700000000.000100",
                    "repo_full_name": "acme-corp/acme-private",
                    "base_sha": "a" * 40,
                    "branch": "curie/thread-lineage-example",
                    "pr_number": 123,
                    "pr_url": "https://github.com/acme-corp/acme-private/pull/123",
                    "head_sha": "b" * 40,
                    "state": "open",
                    "version": 3,
                    "latest_revision": 2,
                    "has_pending_revision": True,
                    "has_pending_outcome": True,
                    "visible_outcome_revision": 1,
                },
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            client = ApprovalClient(
                api_base_url="https://api.example.test",
                api_key="platform-test-key",
                client=http,
                read_timeout_s=1.0,
                worker_token="worker-test-token",
            )
            lineage = await client.get_publication_lineage(
                deployment_id,
                "1700000000.000100",
                "acme-corp/acme-private",
            )

        assert lineage is not None
        assert lineage.state == "open"
        assert lineage.head_sha == "b" * 40
        assert lineage.has_pending_revision is True
        assert lineage.has_pending_outcome is True
        assert lineage.visible_outcome_revision == 1
        assert not hasattr(lineage, "authorization_header")
        assert len(seen) == 1
        request = seen[0]
        assert request.headers["X-Curie-Worker-Token"] == "worker-test-token"
        assert "Authorization" not in request.headers
        assert "credential" not in request.url.query.decode().casefold()

    asyncio.run(go())


@pytest.mark.parametrize(
    "code",
    ["publication.lineage_stale", "publication.lineage_terminal"],
)
def test_private_lineage_conflict_is_translated_to_a_user_actionable_refusal(
    code: str,
) -> None:
    async def go() -> None:
        from curie_worker.workspace import WorkspaceSelectionRefused

        message = "Restore the expected head or start a new thread."

        def handle(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                409,
                json={"detail": {"code": code, "message": message}},
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            client = ApprovalClient(
                api_base_url="https://api.example.test",
                api_key="",
                client=http,
                read_timeout_s=1.0,
                worker_token="worker-test-token",
            )
            with pytest.raises(WorkspaceSelectionRefused) as excinfo:
                await client.get_publication_lineage(
                    uuid.UUID("11111111-1111-4111-8111-111111111111"),
                    "1700000000.000100",
                    "acme-corp/acme-private",
                )

        assert excinfo.value.public_detail == message

    asyncio.run(go())


@pytest.mark.parametrize(
    "message",
    [
        "conversation has no selected repository workspace",
        "publication repository differs from the thread workspace",
        "thread workspace repository is no longer allowed",
    ],
)
def test_publication_create_workspace_409_is_a_terminal_refusal(message: str) -> None:
    """Workspace authorization failures must not retry an already-run model turn."""

    async def go() -> None:
        from curie_worker.workspace import WorkspaceSelectionRefused

        def handle(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(409, json={"detail": message})

        request = PublicationCreateRequest(
            deployment_id=uuid.UUID("11111111-1111-4111-8111-111111111111"),
            conversation_id="slack:C0EXAMPLE1:1700000000.000100",
            repo_full_name="acme-corp/acme-private",
            author="U0REQUEST1",
            summary="Publish the bounded change",
            reply_kind="slack",
            reply_channel="C0EXAMPLE1",
            reply_placeholder="1700000000.000001",
            reply_endpoint=None,
            reply_adapter=None,
            dedupe_key="publication-example",
            base_sha="a" * 40,
            patch=b"diff --git a/README.md b/README.md\n",
            changed_paths=("README.md",),
            expires_in_seconds=600,
            title="Update README",
            body="Approved platform publication.",
        )
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            client = ApprovalClient(
                api_base_url="https://api.example.test",
                api_key="",
                client=http,
                read_timeout_s=1.0,
                worker_token="worker-test-token",
            )
            with pytest.raises(WorkspaceSelectionRefused) as excinfo:
                await client.create_publication(request)

        assert excinfo.value.public_detail == message

    asyncio.run(go())


def _refusal_publication_request() -> PublicationCreateRequest:
    return PublicationCreateRequest(
        deployment_id=uuid.UUID("11111111-1111-4111-8111-111111111111"),
        conversation_id="slack:C0EXAMPLE1:1700000000.000100",
        repo_full_name="acme-corp/acme-private",
        author="U0REQUEST1",
        summary="Publish the bounded change",
        reply_kind="slack",
        reply_channel="C0EXAMPLE1",
        reply_placeholder="1700000000.000001",
        reply_endpoint=None,
        reply_adapter=None,
        dedupe_key="publication-example",
        base_sha="a" * 40,
        patch=b"diff --git a/unitconv/convert.py b/unitconv/convert.py\n",
        changed_paths=("unitconv/convert.py",),
        expires_in_seconds=600,
        title="Update converter",
        body="Approved platform publication.",
    )


@contextmanager
def _fake_retry_time() -> Iterator[list[float]]:
    """Replace the API retry helper's clock and sleep; yield the recorded sleeps.

    Each fake sleep advances the fake clock by its delay, so a retry window
    (#4174, #4180) is spent without waiting for real.
    """

    from curie_worker import api_retry

    now = 0.0
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        nonlocal now
        sleeps.append(delay)
        now += delay
        await asyncio.sleep(0)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(api_retry, "_clock", lambda: now)
        patch.setattr(api_retry, "_sleep", sleep)
        yield sleeps


def _publication_client(http: httpx.AsyncClient) -> ApprovalClient:
    return ApprovalClient(
        api_base_url="https://api.example.test",
        api_key="",
        client=http,
        read_timeout_s=1.0,
        worker_token="worker-test-token",
    )


async def _create_publication_error(response: httpx.Response) -> Any:
    from curie_worker.approvals import ApprovalBackendError

    # A persistent 5xx is retried for the whole window (#4180); the fake clock
    # spends it at once.
    with _fake_retry_time():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _request: response)
        ) as http:
            client = _publication_client(http)
            with pytest.raises(ApprovalBackendError) as excinfo:
                await client.create_publication(_refusal_publication_request())
    return excinfo.value


@pytest.mark.parametrize(
    ("status", "code", "message"),
    [
        (
            409,
            "publication.required_python_ci_unselected",
            "required Python CI does not select unitconv/convert.py",
        ),
        (
            409,
            "publication.verification_preflight_missing",
            "verification preflight observation is missing",
        ),
        (422, "publication.patch_too_large", "patch exceeds the limit"),
    ],
)
def test_a_coded_publication_refusal_carries_its_code_and_message(
    status: int, code: str, message: str
) -> None:
    """#3617: the API's refusal reaches the factory run instead of a generic cause."""

    error = asyncio.run(
        _create_publication_error(
            httpx.Response(status, json={"detail": {"code": code, "message": message}})
        )
    )

    assert error.refusal == f"{code}: {message}"


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(500, text="internal error"),
        httpx.Response(409, json={"detail": {"code": 7, "message": "not a code"}}),
    ],
    ids=["plain-500", "non-string-code"],
)
def test_an_uncoded_publication_failure_has_no_refusal(response: httpx.Response) -> None:
    error = asyncio.run(_create_publication_error(response))

    assert error.refusal is None


def test_a_string_detail_api_refusal_is_carried_as_the_refusal() -> None:
    message = "publication patch exceeds the 1048576-byte limit"

    error = asyncio.run(_create_publication_error(httpx.Response(413, json={"detail": message})))

    assert error.refusal == message


def test_a_long_refusal_is_redacted_whole_before_it_is_clipped() -> None:
    """A secret whose closing delimiter lies past any clip must still redact.

    The refusal reaches a public GitHub issue, so a clip that runs before
    redaction would cut the PEM end line and leak the key body.
    """

    from curie_worker.kernel import _ApprovalPause

    body = "A" * 900
    pem = f"-----BEGIN RSA PRIVATE KEY-----\n{body}\n-----END RSA PRIVATE KEY-----"
    error = asyncio.run(
        _create_publication_error(
            httpx.Response(
                409,
                json={"detail": {"code": "publication.example", "message": f"x {pem}"}},
            )
        )
    )

    pause = _ApprovalPause.refused(error.refusal)

    assert pause.failure_detail is not None
    assert "publication.example" in pause.failure_detail
    assert "AAAAAAAAAA" not in pause.failure_detail


# --- #4180: publication create retries transient faults within the budget -----
#
# The create route is replay safe on ``dedupe_key``: an exact replay is adopted
# and answered 200 instead of 201, so a retry cannot mint a second card or a
# second publication. API replay contract:
# apps/api/src/curie_api/routers/publications.py::create_publication.

_CREATED_PUBLICATION = {
    "id": "publication-example",
    "approval_id": "approval-example",
    "status": "pending",
}


def _assert_identical_replays(seen: list[httpx.Request], count: int) -> None:
    assert len(seen) == count
    assert all(request.method == "POST" for request in seen)
    assert all(request.url.path == "/v1/internal/publications" for request in seen)
    assert len({request.content for request in seen}) == 1
    assert {json.loads(request.content)["dedupe_key"] for request in seen} == {
        "publication-example"
    }


def test_publication_create_retries_a_transport_fault_and_a_500_then_returns_201() -> None:
    """#4180 AC1: ConnectError, then 500, then 201 returns the publication.

    Every replay carries the same body and ``dedupe_key``, which is what lets the
    API adopt it (publications.py::create_publication).
    """

    async def go() -> None:
        seen: list[httpx.Request] = []

        def handle(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            if len(seen) == 1:
                raise httpx.ConnectError("API restarting", request=request)
            if len(seen) == 2:
                return httpx.Response(500, text="Internal Server Error")
            return httpx.Response(201, json=_CREATED_PUBLICATION)

        with _fake_retry_time() as sleeps:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
                published = await _publication_client(http).create_publication(
                    _refusal_publication_request()
                )

        assert (published.id, published.approval_id, published.status) == (
            "publication-example",
            "approval-example",
            "pending",
        )
        _assert_identical_replays(seen, 3)
        assert sleeps == [0.5, 1.0]

    asyncio.run(go())


def test_publication_create_adopts_a_200_replay_after_a_read_timeout() -> None:
    """#4180 AC2: the first POST may have committed before its response was lost.

    The replay is answered 200 by publications.py::create_publication and is
    the created publication, not an error.
    """

    async def go() -> None:
        seen: list[httpx.Request] = []

        def handle(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            if len(seen) == 1:
                raise httpx.ReadTimeout("response lost", request=request)
            return httpx.Response(200, json=_CREATED_PUBLICATION)

        with _fake_retry_time():
            async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
                published = await _publication_client(http).create_publication(
                    _refusal_publication_request()
                )

        assert published.approval_id == "approval-example"
        assert published.status == "pending"
        _assert_identical_replays(seen, 2)

    asyncio.run(go())


@pytest.mark.parametrize(
    ("response", "raised", "detail"),
    [
        (
            httpx.Response(
                409,
                json={
                    "detail": {
                        "code": "publication.metadata_stale",
                        "message": "pull request metadata changed since it was observed",
                    }
                },
            ),
            ApprovalBackendError,
            "publication.metadata_stale: pull request metadata changed since it was observed",
        ),
        (
            httpx.Response(422, json={"detail": "invalid publication request"}),
            ApprovalBackendError,
            "invalid publication request",
        ),
        (
            httpx.Response(
                409, json={"detail": "conversation has no selected repository workspace"}
            ),
            WorkspaceSelectionRefused,
            "conversation has no selected repository workspace",
        ),
    ],
    ids=["metadata-stale-409", "plain-422", "workspace-409"],
)
def test_publication_create_never_retries_a_4xx_refusal(
    response: httpx.Response, raised: type[Exception], detail: str
) -> None:
    """#4180 AC3: a 4xx is a real refusal, so one POST and today's handling.

    Only a transport fault or a 5xx is retried; publications.py::create_publication
    answers a refusal with a 4xx whose reason the issue must show.
    """

    async def go() -> None:
        seen: list[httpx.Request] = []

        def handle(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return response

        with _fake_retry_time() as sleeps:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
                with pytest.raises(raised) as excinfo:
                    await _publication_client(http).create_publication(
                        _refusal_publication_request()
                    )

        assert len(seen) == 1
        assert sleeps == []
        error = excinfo.value
        if isinstance(error, WorkspaceSelectionRefused):
            assert error.public_detail == detail
        else:
            assert isinstance(error, ApprovalBackendError)
            assert error.refusal == detail

    asyncio.run(go())


def test_publication_create_stops_retrying_when_its_budget_is_spent() -> None:
    """#4180 AC4: a dead API raises ApprovalBackendError within ``budget_s``.

    The retries replay one body under one ``dedupe_key``, safe per
    publications.py::create_publication, but never past the budget.
    """

    async def go() -> None:
        seen: list[httpx.Request] = []

        def handle(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            raise httpx.ConnectError("API unreachable", request=request)

        with _fake_retry_time() as sleeps:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
                with pytest.raises(ApprovalBackendError) as excinfo:
                    await _publication_client(http).create_publication(
                        _refusal_publication_request(), budget_s=3
                    )

        assert str(excinfo.value).startswith("publication create failed:")
        assert len(seen) > 1
        assert sum(sleeps) <= 3
        _assert_identical_replays(seen, len(seen))

    asyncio.run(go())


def test_publication_turn_rides_out_two_503s_and_pauses_awaiting_approval(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """#4180 AC5: a kernel publication turn retries 503, 503, then 201.

    The POST goes through the real ``ApprovalClient.create_publication``; only
    the lineage read is the recording fake's. The replays are adopted by
    publications.py::create_publication, so the turn pauses with no escalation.
    """

    from curie_worker.approvals import PublicationLineage

    class LineageFreeClient(ApprovalClient):
        async def get_publication_lineage(
            self, deployment_id: uuid.UUID, conversation_id: str, repo_full_name: str
        ) -> PublicationLineage | None:
            return None

    async def go() -> None:
        seen: list[httpx.Request] = []

        def handle(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            if len(seen) <= 2:
                return httpx.Response(503, text="Service Unavailable")
            return httpx.Response(201, json=_CREATED_PUBLICATION)

        with _fake_retry_time() as sleeps, caplog.at_level(logging.DEBUG):
            async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
                client = LineageFreeClient(
                    api_base_url="https://api.example.test",
                    api_key="",
                    client=http,
                    read_timeout_s=1.0,
                    worker_token="worker-test-token",
                )
                async with _publication_run(
                    make_harness,
                    monkeypatch,
                    deployment_id=uuid.UUID("11111111-1111-4111-8111-111111114180"),
                    thread="1700000000.004180",
                    publication_creator=client,
                ) as run:
                    outcomes = [c.outcome for c in run.h.sink.completions]
                    assert outcomes == ["awaiting-approval"]

        assert len(seen) == 3
        assert all(request.url.path == "/v1/internal/publications" for request in seen)
        assert len({request.content for request in seen}) == 1
        assert sleeps == [0.5, 1.0]
        assert "approval create failed" not in caplog.text
        assert "approval-create-failed" not in caplog.text

    asyncio.run(go())


@pytest.mark.parametrize("remaining", [None, 40.0])
def test_publication_create_budget_is_bounded_by_the_delivery_lease(
    make_harness, monkeypatch: pytest.MonkeyPatch, remaining: float | None
) -> None:
    """#4180 AC5: the kernel passes ``min(120, lease.remaining_s())``, else 120.

    The same rule as ``ApprovalClient.create``: a replayed publication
    (publications.py::create_publication) must never outlive its delivery.
    """

    import time

    from curie_worker.approvals import CreatedPublication
    from curie_worker.delivery_lease import DeliveryBudget, unfenced_lease

    class BudgetRecorder:
        def __init__(self) -> None:
            self.budgets: list[float] = []

        async def get_publication_lineage(
            self, requested_deployment: uuid.UUID, conversation: str, repo: str
        ) -> None:
            return None

        async def create_publication(
            self, request: PublicationCreateRequest, *, budget_s: float = 120
        ) -> CreatedPublication:
            self.budgets.append(budget_s)
            return CreatedPublication(**_CREATED_PUBLICATION)

    async def go() -> None:
        recorder = BudgetRecorder()
        lease = None
        if remaining is not None:
            lease = unfenced_lease()
            lease.budget = DeliveryBudget(
                deadline_ms=int(remaining * 1000),
                anchor_server_ms=0,
                anchor_monotonic=time.monotonic(),
            )
        async with _publication_run(
            make_harness,
            monkeypatch,
            deployment_id=uuid.UUID("11111111-1111-4111-8111-111111114181"),
            thread="1700000000.004181",
            publication_creator=recorder,
            lease=lease,
        ) as run:
            assert [c.outcome for c in run.h.sink.completions] == ["awaiting-approval"]

        expected = 120 if remaining is None else 40
        assert recorder.budgets == [pytest.approx(expected, abs=0.5)]

    asyncio.run(go())


def test_worker_approval_http_does_not_fabricate_a_parent() -> None:
    """A legacy/root call without an active trace stays a clean HTTP root."""

    async def go() -> None:
        seen: list[httpx.Request] = []

        def handle(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(
                200,
                json={
                    "status": "approved",
                    "resolved_by": "U0APPROVER1",
                    "resolution_note": None,
                },
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            client = ApprovalClient(
                api_base_url="https://api.example.test",
                api_key="platform-test-key",
                client=http,
                read_timeout_s=1.0,
            )
            await client.get("00000000-0000-0000-0000-000000000001")

        assert len(seen) == 1
        assert "traceparent" not in seen[0].headers

    asyncio.run(go())


_RERAISE_VECTOR = (
    Path(__file__).resolve().parents[4] / "tests" / "vectors" / "approval-reraise-refusal.json"
)
_RERAISE_MESSAGE = (
    'Not requesting approval again: "File the quarterly form" (approval '
    "00000000-0000-4000-8000-000000000001) was rejected by U0REJECT01 at "
    "2026-09-18 17:07 UTC, and nobody has asked for it since. Nothing was done. "
    "To raise it again, a person must ask for it in this thread."
)


def _reraise_code() -> str:
    vector = json.loads(_RERAISE_VECTOR.read_text(encoding="utf-8"))
    assert set(vector) == {"comment", "refusal_code"}
    code = vector["refusal_code"]
    assert isinstance(code, str) and code
    return code


def _approval_create_client(response: httpx.Response) -> tuple[httpx.AsyncClient, ApprovalClient]:
    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: response))
    client = ApprovalClient(
        api_base_url="https://api.example.test",
        api_key="platform-test-key",
        client=http,
        read_timeout_s=1.0,
    )
    return http, client


def _gate_request() -> ApprovalRequest:
    return ApprovalRequest(
        conversation_id="1700000000.000100",
        author="U0REJECT01",
        summary="File the quarterly form",
        reply_kind="slack",
        reply_channel="C0EXAMPLE1",
        reply_placeholder="1700000000.000001",
        dedupe_key="approval-00000000-0000-4000-8000-000000000001-resolved",
        route="filings",
    )


def test_rejected_reraise_409_is_a_terminal_refusal() -> None:
    """#2885: the API's re-raise refusal becomes a refusal carrying its message.

    Only the frozen code does; any other 409 is still a backend failure, so an
    unrelated conflict cannot post an API body into the thread.
    """

    from curie_worker.approvals import ApprovalRefused

    async def go() -> None:
        refused = httpx.Response(
            409,
            json={
                "detail": {
                    "code": _reraise_code(),
                    "message": _RERAISE_MESSAGE,
                    "approval_id": "00000000-0000-4000-8000-000000000001",
                    "rejected_by": "U0REJECT01",
                    "rejected_at": "2026-09-18T17:07:00",
                }
            },
        )
        http, client = _approval_create_client(refused)
        async with http:
            with pytest.raises(ApprovalRefused) as excinfo:
                await client.create(_gate_request())
        assert excinfo.value.public_detail == _RERAISE_MESSAGE

        other = httpx.Response(
            409, json={"detail": {"code": "approval.other", "message": "do not post me"}}
        )
        http, client = _approval_create_client(other)
        async with http:
            with pytest.raises(ApprovalBackendError):
                await client.create(_gate_request())

    asyncio.run(go())


class RefusingApprovals:
    """An ApprovalCreator fake standing in for the API's re-raise refusal."""

    def __init__(self) -> None:
        self.create_calls = 0

    async def create(self, request: ApprovalRequest, *, budget_s: float = 120) -> CreatedApproval:
        from curie_worker.approvals import ApprovalRefused

        self.create_calls += 1
        raise ApprovalRefused(_RERAISE_MESSAGE)


def test_refused_reraise_reports_the_rejection_instead_of_pausing(make_harness) -> None:
    """#2885: a refused re-raise ends the turn with the refusal, not a pause.

    The thread reads the refusal (which approval, who rejected it, how to ask
    again), nothing is suspended, no card is posted, and the turn is done rather
    than escalated or retried.
    """

    async def go() -> None:
        approvals = RefusingApprovals()
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting_script("File the quarterly form")
            ev = _qevent(
                "[approval resolved] The request was rejected by U0REJECT01.",
                event_id="approval-00000000-0000-4000-8000-000000000001-resolved",
            )
            await h.kernel.process_event(ev)

            assert approvals.create_calls == 1
            assert h.sink.last_text == _RERAISE_MESSAGE
            assert "could not be created" not in (h.sink.last_text or "")
            modes = [s.operating_mode for s in h.fake_k8s.sandboxes.values()]
            assert modes == ["Running"]
            assert h.sink.posts == []
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_awaiting_approval_creates_record_and_suspends(make_harness) -> None:
    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting_script("Give ACME a 20% discount")
            ev = _qevent("please discount", event_id="ev-appr-1")
            await h.kernel.process_event(ev)

            # The durable record was created with the turn's identity: the
            # dedupe key is the event id and the reply handle rides along so a
            # resolution can resume into the same placeholder.
            assert len(approvals.requests) == 1
            req = approvals.requests[0]
            assert req.summary == "Give ACME a 20% discount"
            assert req.dedupe_key == "ev-appr-1"
            assert req.conversation_id == ev.conversation_id
            assert req.reply_channel == "C1"
            assert req.reply_placeholder == "p-1"
            assert req.author == "U1"

            # The sandbox was suspended and the route flipped to SUSPENDED.
            modes = [s.operating_mode for s in h.fake_k8s.sandboxes.values()]
            assert modes == ["Suspended"]
            record = h.substrate._affinity.get(_thread_key(ev.conversation_id))
            assert record is not None and record.state is RouteState.SUSPENDED

            # The placeholder carries the one-line notice above the card, which
            # holds the summary and the record id (ADR-0179 decision 2), and the
            # event is done (no retry loop).
            assert h.sink.last_text is not None
            assert h.sink.last_text.endswith(_SHORT_NOTICE)
            card_message = h.sink.posts[0][1]
            assert card_message.text == "Give ACME a 20% discount"
            assert isinstance(card_message.interaction, ConfirmIntent)
            assert card_message.interaction.id == "appr-1"
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_templated_display_reaches_the_notice_and_card_not_the_record(
    make_harness,
) -> None:
    """#2565: the sentence is what a person reads; the record keeps the machine string."""

    sentence = "File FY26Q1: 11 workbooks into Approved, 25 cells going out blank. Approve?"
    machine = (
        "Tool call awaiting approval: mcp__plugin_demo_files__approve_batch "
        '{"expected": {"a.xlsx": "aaa"}}'
    )

    async def go() -> None:
        approvals = RecordingApprovals()
        # Routed, so the requester's thread carries the full notice: an in-thread
        # card gets the one line instead (ADR-0179 decision 2).
        binding = RoutedBinding({"managers": _resolution_route()})
        async with make_harness(approvals=approvals, binding=binding) as h:
            script = _awaiting_script_with_display(machine, sentence)
            script[-1] = script[-1].model_copy(update={"approval_route": "managers"})
            h.runner.default_script = script
            ev = _qevent("please file", event_id="ev-appr-display")
            await h.kernel.process_event(ev)

            assert len(approvals.requests) == 1
            assert approvals.requests[0].summary == machine

            assert h.sink.last_text is not None
            notice_lines = [
                line
                for line in h.sink.last_text.splitlines()
                if line.startswith("Awaiting approval (")
            ]
            assert notice_lines == [f"Awaiting approval (appr-1): {sentence}"]
            assert machine not in h.sink.last_text

            assert h.sink.posts
            card_message = h.sink.posts[0][1]
            assert card_message.text == sentence
            assert isinstance(card_message.interaction, ConfirmIntent)
            assert card_message.interaction.prompt == sentence

    asyncio.run(go())


def test_the_created_record_carries_the_turns_kind_and_adapter(make_harness) -> None:
    """T-A12, worker half / AC2 (plan EB-A17, finding 2).

    The durable record is what the RESUME is rebuilt from, days later, possibly
    after the binding moved (T-A8). So the kind and the egress-credential
    selector have to be copied off THIS turn's reply handle at creation time --
    the one moment both facts are known and true.

    Mutation this catches: leave `kernel.py`'s `ApprovalRequest(...)` producer
    unchanged. Everything still compiles, the approval is still created, the
    Slack lane is unaffected, and only a resumed EMAIL turn -- the rarest path in
    the system -- reveals the loss.

    Both fields are asserted on the same request rather than in two tests,
    because dropping either one alone produces the same silent shape.
    """

    async def go() -> None:
        approvals = RecordingApprovals()
        # An email approval is created only for a route that lists approver
        # emails (ADR-0177 amendment); a routeless one escalates.
        binding = RoutedBinding({"approve": _LISTED_EMAIL_ROUTE})
        async with make_harness(approvals=approvals, binding=binding) as h:
            h.runner.default_script = _awaiting_routed_script("Send the quote to ACME", "approve")
            ev = _qevent(
                "send it",
                event_id="ev-appr-kind",
                kind="email",
                adapter="agentmail-sandbox",
            )
            await h.kernel.process_event(ev)

            assert len(approvals.requests) == 1
            req = approvals.requests[0]
            assert req.reply_kind == "email"
            assert req.reply_adapter == "agentmail-sandbox"
            # The pre-existing reply-handle fields are still copied, so a
            # producer that replaced the block rather than extending it fails.
            assert req.reply_channel == "C1"
            assert req.reply_placeholder == "p-1"

    asyncio.run(go())


def test_session_approval_carries_a_finite_deadline(make_harness) -> None:
    """A session approval the worker creates has a finite SLA so the existing
    sweeper can wake a turn nobody resolves.
    """

    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting_script("Give ACME a 20% discount")
            await h.kernel.process_event(_qevent("discount?", event_id="ev-appr-deadline"))

            assert len(approvals.requests) == 1
            req = approvals.requests[0]
            assert req.expires_in_seconds is not None
            assert req.expires_in_seconds == 24 * 60 * 60

    asyncio.run(go())


def test_a_slack_turns_record_carries_slack_and_no_adapter(make_harness) -> None:
    """T-A12, the sibling lane. Slack legitimately has no adapter (its route is
    the worker's configured origin, D4.4), so its record must persist NULL rather
    than borrow the email lane's slug or a placeholder string -- a fabricated
    adapter would send the platform's egress credential for somebody else's
    adapter on resume.
    """

    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting_script("Give ACME a 20% discount")
            await h.kernel.process_event(_qevent("discount?", event_id="ev-appr-slack"))

            assert len(approvals.requests) == 1
            req = approvals.requests[0]
            assert req.reply_kind == "slack"
            assert req.reply_adapter is None

    asyncio.run(go())


def test_null_placeholder_turn_persists_its_approval_before_any_delivery(
    make_harness,
) -> None:
    """A placeholder-less turn can pause for approval like any other (ADR-0079).

    #2721: the durable row is created with whatever ref the turn already holds
    (None here) BEFORE any delivery. A delivery that precedes persistence strands
    the approval whenever the transport is down, so the reply ref is no longer
    minted ahead of the record.
    """

    async def go() -> None:
        approvals = RecordingApprovals()
        binding = RoutedBinding({"approve": _LISTED_EMAIL_ROUTE})
        async with make_harness(approvals=approvals, binding=binding) as h:
            h.runner.default_script = _awaiting_routed_script("Give ACME a 20% discount", "approve")
            event = _qevent(
                "please discount",
                placeholder=None,
                endpoint="http://adapter.example.test/",
                kind="email",
                adapter="agentmail",
            )

            await h.kernel.process_event(event)

            assert h.runner.opened == ["please discount"]
            assert approvals.create_calls == 1
            req = approvals.requests[0]
            assert req.reply_kind == "email"
            assert req.reply_adapter == "agentmail"
            # Whatever ref the turn held at persistence: a booting post may
            # already have minted one, or none exists yet.
            minted = h.sink.text_posts[0][1] if h.sink.text_posts else None
            assert req.reply_placeholder in (None, minted)

    asyncio.run(go())


def _gate_only_script(summary: str) -> list:
    # No TextDelta before the Final: pre-gate streamed prose is the turn's own
    # delivery during the model turn, before the kernel can know a gate fired,
    # and is out of scope for #2721.
    return [Final(text="Requesting sign-off", status=AWAITING, approval_summary=summary)]


def _record_create_calls_at_emit(h, approvals: RecordingApprovals) -> list[int]:  # noqa: ANN001
    seen: list[int] = []
    original_emit = h.sink.emit

    async def recording_emit(
        reply_event: ReplyEvent,
        *,
        route: TargetRoute,
        best_effort_unreachable: bool = False,
    ) -> ReplyAck:
        # The booting caption precedes the model turn, so no approval can
        # exist yet; every delivery after it is the pausing turn's own.
        if getattr(reply_event, "text", None) != h.config.booting_text:
            seen.append(approvals.create_calls)
        return await original_emit(
            reply_event,
            route=route,
            best_effort_unreachable=best_effort_unreachable,
        )

    h.sink.emit = recording_emit
    return seen


def test_placeholderless_approval_row_precedes_every_delivery_attempt(
    make_harness,
) -> None:
    """#2721 AC1: on a placeholderless Slack turn, no delivery is attempted
    before the durable approval row exists."""

    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _gate_only_script("Give ACME a 20% discount")
            seen = _record_create_calls_at_emit(h, approvals)

            await h.kernel.process_event(
                _qevent("please discount", thread="th_row_first", placeholder=None)
            )

            assert approvals.create_calls == 1
            assert seen, "the pausing turn delivered nothing"
            assert seen == [1] * len(seen), seen

    asyncio.run(go())


def _install_dead_transport(h) -> list[str]:  # noqa: ANN001
    attempts: list[str] = []

    async def not_authed(
        reply_event: ReplyEvent,
        *,
        route: TargetRoute,
        best_effort_unreachable: bool = False,
    ) -> ReplyAck:
        attempts.append(reply_event.event)
        raise RuntimeError("not_authed")

    h.sink.emit = not_authed
    return attempts


def test_approval_without_a_slack_surface_still_suspends_and_completes(
    make_harness,
) -> None:
    """#2721 AC2: every delivery failing (no Slack) must not strand the approval.

    The event completes awaiting approval exactly once: one row, the sandbox
    suspended, the done marker set, and no exception back to the consumer.
    """

    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _gate_only_script("Give ACME a 20% discount")
            _install_dead_transport(h)
            event = _qevent("please discount", thread="th_no_slack", placeholder=None)

            await h.kernel.process_event(event)

            assert approvals.create_calls == 1
            modes = [s.operating_mode for s in h.fake_k8s.sandboxes.values()]
            assert modes == ["Suspended"]
            assert await h.async_redis.exists(h.config.done_key(event.event_id))

    asyncio.run(go())


def test_approval_without_a_slack_surface_is_exactly_once_and_resumes_once(
    make_harness,
) -> None:
    """#2721 AC3: redelivery of the pausing event creates no second row and opens
    no second model turn; the resolution resumes exactly once, even redelivered."""

    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _gate_only_script("Give ACME a 20% discount")
            working_emit = h.sink.emit
            _install_dead_transport(h)
            thread = "th_no_slack_once"
            event = _qevent("please discount", thread=thread, placeholder=None)

            await h.kernel.process_event(event)
            await h.kernel.process_event(event)

            assert approvals.create_calls == 1
            assert h.runner.opened == ["please discount"]

            h.sink.emit = working_emit
            h.runner.default_script = [Final(text="Discount applied.", status=DONE)]
            resolution = _qevent(
                "[approval resolved] approved by U9",
                thread=thread,
                event_id="approval-appr-1-resolved",
                placeholder=None,
            )

            await h.kernel.process_event(resolution)
            assert h.runner.opened == [
                "please discount",
                "[approval resolved] approved by U9",
            ]

            await h.kernel.process_event(resolution)
            assert h.runner.opened == [
                "please discount",
                "[approval resolved] approved by U9",
            ]
            assert approvals.create_calls == 1

    asyncio.run(go())


def test_no_edit_placeholderless_approval_resumes_onto_the_minted_message(
    make_harness,
) -> None:
    """#1640 under the #2721 contract: the row is persisted before the notice
    mints a ref, so it carries None, and the API replays a None placeholder on
    the resolution turn. The resumed answer must still edit the single message
    the pending notice minted, never post a second one."""

    async def go() -> None:
        approvals = RecordingApprovals()
        # Routed, so no card follows the notice in this thread and the resume
        # answers on the notice. An in-thread card answers below itself instead
        # (ADR-0179 decision 3, test_a_placeholderless_resume_answers_below_the_card).
        binding = RoutedBinding({"managers": _resolution_route()})
        async with make_harness(
            approvals=approvals,
            binding=binding,
            slack_no_edit_streaming=True,
        ) as h:
            h.runner.default_script = _awaiting_routed_script(
                "Give ACME a 20% discount", "managers"
            )
            thread = "th_no_edit_approval"
            event = _qevent(
                "please discount",
                thread=thread,
                placeholder=None,
            )

            await h.kernel.process_event(event)

            assert len(h.sink.text_posts) == 1, h.sink.text_posts
            minted = h.sink.text_posts[0][1]
            request = approvals.requests[0]
            assert request.reply_kind == "slack"
            assert request.reply_endpoint is None
            assert request.reply_adapter is None
            assert request.reply_placeholder in (None, minted)
            assert h.sink.updates
            assert {ref for _, ref, _ in h.sink.updates} == {minted}
            assert "Awaiting approval (appr-1)" in h.sink.updates[-1][2]

            pending_update_count = len(h.sink.updates)
            h.runner.default_script = [
                Final(text="Discount applied.", status=DONE),
            ]
            resolution = _qevent(
                "[approval resolved] approved by U9",
                thread=thread,
                event_id="approval-appr-1-resolved",
                # The API replays the row's ref; under #2721 that may be None.
                placeholder=None,
            )

            await h.kernel.process_event(resolution)

            assert len(h.sink.text_posts) == 1, h.sink.text_posts
            assert len(h.sink.updates) == pending_update_count + 1
            assert h.sink.updates[-1][1] == minted
            assert h.sink.updates[-1][2] == "Discount applied."

    asyncio.run(go())


def test_no_edit_placeholderless_approval_tolerates_a_notice_without_a_ref(
    make_harness,
) -> None:
    """#2721: a notice acknowledged with no ref no longer fails the turn. The
    row already exists, so the turn completes awaiting approval."""

    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(
            approvals=approvals,
            slack_no_edit_streaming=True,
        ) as h:
            h.runner.default_script = _awaiting_script("Give ACME a 20% discount")
            event = _qevent(
                "please discount",
                thread="th_no_edit_missing_ref",
                placeholder=None,
            )
            original_emit = h.sink.emit
            missing_ref_deliveries = 0

            async def omit_minted_ref(
                reply_event: ReplyEvent,
                *,
                route: TargetRoute,
                best_effort_unreachable: bool = False,
            ) -> ReplyAck:
                nonlocal missing_ref_deliveries
                if reply_event.target.reply_ref is None:
                    missing_ref_deliveries += 1
                    return ReplyAck(ref=None)
                return await original_emit(
                    reply_event,
                    route=route,
                    best_effort_unreachable=best_effort_unreachable,
                )

            h.sink.emit = omit_minted_ref

            await h.kernel.process_event(event)

            assert missing_ref_deliveries >= 1
            assert approvals.create_calls == 1
            modes = [s.operating_mode for s in h.fake_k8s.sandboxes.values()]
            assert modes == ["Suspended"]
            assert await h.async_redis.exists(h.config.done_key(event.event_id))

    asyncio.run(go())


def test_stream_minted_ref_survives_a_booting_delivery_failure(make_harness) -> None:
    async def go() -> None:
        approvals = RecordingApprovals()
        binding = RoutedBinding({"approve": _LISTED_EMAIL_ROUTE})
        async with make_harness(approvals=approvals, binding=binding) as h:
            h.runner.default_script = _awaiting_routed_script("Give ACME a 20% discount", "approve")
            booting = h.config.booting_text
            original_emit = h.sink.emit
            booting_failures = 0

            async def fail_booting_once(
                event: ReplyEvent,
                *,
                route: TargetRoute,
                best_effort_unreachable: bool = False,
            ) -> ReplyAck:
                nonlocal booting_failures
                if getattr(event, "text", None) == booting and booting_failures == 0:
                    booting_failures += 1
                    raise RuntimeError("injected booting delivery failure")
                return await original_emit(
                    event,
                    route=route,
                    best_effort_unreachable=best_effort_unreachable,
                )

            h.sink.emit = fail_booting_once
            event = _qevent(
                "please discount",
                placeholder=None,
                endpoint="http://adapter.example.test/",
                kind="email",
                adapter="agentmail",
            )

            await h.kernel.process_event(event)

            assert booting_failures == 1
            assert len(h.sink.text_posts) == 1, h.sink.text_posts
            minted = h.sink.text_posts[0][1]
            assert approvals.requests[0].reply_placeholder == minted
            assert len(h.sink.updates) >= 2, h.sink.updates
            assert {ref for _, ref, _ in h.sink.updates} == {minted}
            assert h.sink.updates[-1][2].endswith(_SHORT_NOTICE)

    asyncio.run(go())


def test_multiparagraph_summary_yields_a_single_block_parseable_notice(
    make_harness,
) -> None:
    """A model-authored multi-paragraph summary must not break the CLI notice
    parse (#817).

    The notice is a control string the CLI splits on blank lines, requiring the
    marker-leading block (cli/src/chat.rs parse_approval_id, the #766
    keep-alive). A blank line inside the summary would strand the resumed reply
    (or, on the route-bound path, report the raw notice as a false success). The
    kernel collapses the interpolated summary to one logical line, so the notice
    stays a single block whose trailing ``\\n\\n``-split segment starts with the
    marker -- while the durable record keeps the original summary."""

    async def go() -> None:
        approvals = RecordingApprovals()
        # Routed: the CLI parses the notice only where the card is not in its
        # thread (ADR-0179 decision 2), so that is where the parse must hold.
        binding = RoutedBinding({"managers": _resolution_route()})
        async with make_harness(approvals=approvals, binding=binding) as h:
            summary = "First paragraph of the summary.\n\nSecond paragraph.\nThird line."
            h.runner.default_script = _awaiting_routed_script(summary, "managers")
            ev = _qevent("please discount", event_id="ev-appr-multi")
            await h.kernel.process_event(ev)

            # The durable record keeps the original multi-paragraph summary; only
            # the notice display is collapsed.
            assert len(approvals.requests) == 1
            assert approvals.requests[0].summary == summary

            # The placeholder notice is a single logical block: splitting on the
            # blank-line delimiter, the trailing block is the marker-leading
            # notice, exactly what the CLI parser anchors on.
            text = h.sink.last_text
            assert text is not None
            blocks = text.split("\n\n")
            notice = blocks[-1]
            assert notice.startswith("Awaiting approval (appr-1)")
            assert "The session is paused" in notice
            # The summary's own blank line is gone; it reads as one line.
            summary_line, _, _ = notice.partition("\n")
            assert "First paragraph of the summary." in summary_line
            assert "Second paragraph." in summary_line
            assert "Third line." in summary_line

    asyncio.run(go())


def test_pending_state_survives_worker_restart_and_resumes_on_resolve(
    make_harness,
) -> None:
    """The epic's acceptance shape: suspend, replace every worker-side object
    (a fresh harness over the same Valkey routes), then deliver the resolution
    turn and watch the session resume and complete."""

    async def go() -> None:
        approvals = RecordingApprovals()
        thread = "th-restart"
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting_script("Refund order 42")
            await h.kernel.process_event(_qevent("refund?", thread=thread))
            record = h.substrate._affinity.get(_thread_key(thread))
            assert record is not None and record.state is RouteState.SUSPENDED

        # "Restart": a brand-new kernel/substrate/runner (nothing in-process
        # survives) over the same Valkey affinity keys. The suspended route is
        # still there because it lives in Valkey, not worker memory.
        async with make_harness(approvals=approvals) as h2:
            record = h2.substrate._affinity.get(_thread_key(thread))
            assert record is not None and record.state is RouteState.SUSPENDED

            # The resolution turn (what the API enqueues on resolve): the
            # kernel must resume the suspended thread, boot a replacement
            # sandbox WITH the bound boot env, and run the turn to done.
            h2.runner.default_script = [Final(text="Refund processed.", status=DONE)]
            resume_turn = _qevent(
                "[approval resolved] approved by U9", thread=thread, event_id="ev-resolve-1"
            )
            await h2.kernel.process_event(resume_turn)

            # The suspended claim was retired and a fresh one created; the
            # route is LIVE again and the reply landed.
            record = h2.substrate._affinity.get(_thread_key(thread))
            assert record is not None and record.state is RouteState.LIVE
            assert h2.sink.last_text == "Refund processed."
            assert h2.runner.opened == ["[approval resolved] approved by U9"]

    asyncio.run(go())


def test_resume_injects_boot_env_into_replacement_claim(make_harness) -> None:
    """The dormant-path fix: a resume must boot the replacement sandbox with
    the same bound env a fresh claim gets (bundle ref, budget), not a generic
    env -- the suspended pod is gone (ADR-0003) and env is all a boot has."""

    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            thread = "th-envmerge"
            h.runner.default_script = _awaiting_script("Ship it")
            await h.kernel.process_event(_qevent("ship?", thread=thread))

            h.runner.default_script = [Final(text="Shipped.", status=DONE)]
            boot_env = {
                "CURIE_BUNDLE_REF": "bundles/agent-v7.tgz",
                "CURIE_BUDGET": '{"max_output_tokens_per_run": 1, "max_usd_per_day": 1.0}',
            }
            handle = await h.kernel._claim_or_resume(_thread_key(thread), boot_env)
            assert handle is not None

            resumed_env = h.fake_k8s.claim_envs[-1]
            assert resumed_env is not None
            assert resumed_env["CURIE_BUNDLE_REF"] == "bundles/agent-v7.tgz"
            assert "CURIE_BUDGET" in resumed_env
            # The substrate still guarantees session identity and a fresh
            # runner token on the replacement claim.
            assert resumed_env.get("CURIE_SESSION_ID")
            assert resumed_env.get("CURIE_RUNNER_TOKEN")

    asyncio.run(go())


class GrantBinding:
    """A binding stand-in that answers approval_grant_tool by event id (#430).

    resolve/boot_env behave like the routed double; approval_grant_tool returns
    the granted tool ONLY for the one resume event id it was configured with,
    mirroring the worker's real derivation from durable approval state.
    """

    def __init__(self, *, grant_event_id: str, grant_tool: str, decision: str = "approved") -> None:
        self.grant_event_id = grant_event_id
        self.grant_tool = grant_tool
        self.decision = decision
        self.agent_id = uuid.uuid4()

    async def resolve(self, kind: str, adapter: str | None, channel: str):  # noqa: ANN201
        from curie_worker.binding import ResolvedDeployment

        return ResolvedDeployment(
            agent_id=self.agent_id,
            agent_name="test-agent",
            version_id=uuid.uuid4(),
            version_label="v1",
            bundle_ref=None,
            max_usd_per_day=None,
            max_output_tokens_per_run=None,
        )

    def packs_for(self, resolved):  # noqa: ANN001, ANN201
        from curie_worker.behaviorpacks import BehaviorPacks

        return BehaviorPacks.from_config(None)

    def budget_for(self, resolved):  # noqa: ANN001, ANN201
        from aci_protocol import Budget

        return Budget(max_output_tokens_per_run=1000, max_usd_per_day=1.0)

    def boot_env(self, resolved, thread_key, *, kind=None, address=None, **_: object):  # noqa: ANN001, ANN201
        return {"CURIE_SESSION_ID": f"s-{thread_key}"}

    async def approval_grant_tool(self, event_id: str, agent_id):  # noqa: ANN001, ANN201
        return self.grant_tool if event_id == self.grant_event_id else None

    async def approval_grant_arguments(self, event_id: str, agent_id):  # noqa: ANN001, ANN201
        if event_id != self.grant_event_id:
            return None
        return {"title": "Ship the fix"}

    async def approval_decision(self, event_id: str, agent_id):  # noqa: ANN001, ANN201
        return self.decision if event_id == self.grant_event_id else None


def test_resume_claim_injects_approval_grant_tool_env(make_harness) -> None:
    """#430: a resume claim for an approved permission-gate approval injects
    CURIE_APPROVAL_GRANT_TOOL into the boot env passed to the replacement
    claim; a fresh (non-approval) mention injects nothing (the gate re-arms)."""

    async def go() -> None:
        from curie_api.resumequeue import resume_event_id

        grant_event = resume_event_id(uuid.uuid4())
        binding = GrantBinding(grant_event_id=grant_event, grant_tool="mcp__github__create_issue")
        async with make_harness(binding=binding) as h:
            # The resume turn carries the approval resume event id -> the grant
            # for the approved tool lands in the boot env of the fresh claim.
            h.runner.default_script = [Final(text="Issue created.", status=DONE)]
            await h.kernel.process_event(
                _qevent(
                    "proceed with the approved action",
                    thread="th-grant",
                    event_id=grant_event,
                )
            )
            resumed_env = h.fake_k8s.claim_envs[-1]
            assert resumed_env is not None
            assert resumed_env.get("CURIE_APPROVAL_GRANT_TOOL") == "mcp__github__create_issue"
            assert resumed_env.get("CURIE_APPROVAL_GRANT_ARGUMENTS") == '{"title":"Ship the fix"}'
            assert resumed_env.get("CURIE_APPROVAL_DECISION") == "approved"

            # A fresh, unrelated mention has a different event id -> no grant env
            # (re-armed), so an adopted/warm follow-up cannot inherit an allowance.
            await h.kernel.process_event(
                _qevent("hello there", thread="th-fresh", event_id="ev-fresh-1")
            )
            fresh_env = h.fake_k8s.claim_envs[-1]
            assert fresh_env is not None
            assert "CURIE_APPROVAL_GRANT_TOOL" not in fresh_env
            assert "CURIE_APPROVAL_GRANT_ARGUMENTS" not in fresh_env
            assert "CURIE_APPROVAL_DECISION" not in fresh_env

    asyncio.run(go())


def test_a_read_only_turn_never_boots_with_an_approval_grant(make_harness) -> None:
    """WORKER-TOOL-ACCESS-4: even carrying an approval resume's event id.

    A resume event id is platform-minted, so a read-only turn wearing one is
    forged or misrouted; either way no grant reaches its boot.
    """

    # @spec WORKER-TOOL-ACCESS-4
    async def go() -> None:
        from aci_protocol import ToolAccess
        from curie_api.resumequeue import resume_event_id

        grant_event = resume_event_id(uuid.uuid4())
        binding = GrantBinding(grant_event_id=grant_event, grant_tool="mcp__github__create_issue")
        async with make_harness(binding=binding) as h:
            h.runner.default_script = [Final(text="read it", status=DONE)]
            h.runner.tool_access_enforced = ["read-only"]
            await h.kernel.process_event(
                _qevent(
                    "proceed with the approved action",
                    thread="th-grant-read-only",
                    event_id=grant_event,
                ).model_copy(update={"tool_access": ToolAccess.READ_ONLY})
            )
            env = h.fake_k8s.claim_envs[-1]
            assert env is not None
            assert "CURIE_APPROVAL_GRANT_TOOL" not in env
            assert "CURIE_APPROVAL_GRANT_ARGUMENTS" not in env

    asyncio.run(go())


@pytest.mark.parametrize(
    ("approvals", "reason"),
    [
        pytest.param(None, "no approval backend", id="no-backend-wired"),
        pytest.param(RecordingApprovals(fail=True), "could not be created", id="backend-fails"),
    ],
)
def test_approval_that_cannot_be_created_escalates_and_does_not_suspend(
    make_harness, approvals: RecordingApprovals | None, reason: str
) -> None:
    async def go() -> None:
        kwargs = {} if approvals is None else {"approvals": approvals}
        async with make_harness(**kwargs) as h:
            h.runner.default_script = _awaiting_script("Anything")
            ev = _qevent("gate this")
            await h.kernel.process_event(ev)

            assert h.sink.last_text is not None
            assert reason in h.sink.last_text
            # Not suspended: a pause nothing could resume would strand the thread.
            modes = [s.operating_mode for s in h.fake_k8s.sandboxes.values()]
            assert modes == ["Running"]
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_unknown_gate_kind_escalates_instead_of_stranding_the_turn(make_harness) -> None:
    """#492/#544: ``gate_kind`` is authority-bearing, so the shared wire model
    rejects an unrecognized value rather than degrading it to None (which would
    route it through the prefix fallback and silently widen authority).

    The ACI ``final`` frame types the field as a bare ``str``, so a runner can
    emit anything and the rejection lands at the worker, at construction. Before
    the model was shared this same value was rejected by the API with a 422,
    surfacing as ``ApprovalBackendError`` and escalating; the local raise must
    escalate identically. If it escaped ``_pause_for_approval`` the consumer
    would leave the entry pending, redeliver it until the delivery cap, and
    dead-letter it -- a full LLM re-run per redelivery and silence for the user.
    The done marker is the proof it did not: it is only written once the turn is
    terminally handled."""

    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = [
                TextDelta(text="Requesting sign-off"),
                Final(
                    text="Requesting sign-off",
                    status=AWAITING,
                    approval_summary="Anything",
                    approval_gate_kind="not-a-real-gate",
                ),
            ]
            ev = _qevent("gate this", thread="th-bad-gate")
            await h.kernel.process_event(ev)

            # Escalated to a human, exactly as the 422 path did.
            assert h.sink.last_text is not None
            assert "could not be created" in h.sink.last_text
            # No record was created from the rejected payload.
            assert approvals.requests == []
            # Not suspended: a session no resolution could ever wake.
            modes = [s.operating_mode for s in h.fake_k8s.sandboxes.values()]
            assert modes == ["Running"]
            # Terminally handled, so the entry is acked rather than redelivered.
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_publication_snapshot_inherits_the_attempts_remaining_delivery_budget(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The publication snapshot is part of the same delivery as the turn.

    Pin the handoff at the kernel boundary: a snapshot must not silently regain
    the configured runner ceiling after the streamed request has consumed part
    of the delivery deadline.
    """

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [
                Final(
                    text="Ready to publish",
                    status=AWAITING,
                    approval_summary="Publish the prepared changes",
                    approval_gate_kind="permission",
                    approval_granted_tool="mcp__curie__publish_changes",
                )
            ]
            observed_remaining: list[float] = []

            async def snapshot_spy(
                _base_url: str,
                token: str | None = None,
                *,
                remaining_s: float,
            ) -> None:
                observed_remaining.append(remaining_s)
                # Stop after capturing the boundary; _attempt deliberately
                # converts runner snapshot failures into a publication error.
                raise RunnerError("captured snapshot deadline")

            monkeypatch.setattr(h.kernel._runner, "snapshot", snapshot_spy)
            outcome = await h.kernel._attempt(
                _qevent("publish", thread="th-publication-deadline"),
                TargetRoute(),
                lambda: None,
                remaining_s=17.25,
                pressure_retried=False,
                workspace_inference=_WorkspaceInferenceCarry(),
            )

            assert outcome.status is AWAITING
            assert observed_remaining == [17.25]

    asyncio.run(go())


def test_pause_emits_a_confirm_intent_for_the_approval_card(make_harness) -> None:
    """#246, ADR-0020: pausing emits a channel-neutral Confirm intent into the
    approval's thread whose confirm/cancel actions carry the record id (the Slack
    adapter renders it into Block Kit buttons -- see test_slack_sink.py), alongside
    the placeholder notice. The kernel never builds Block Kit itself."""

    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting_script("Give ACME a 20% discount")
            ev = _qevent("please discount", thread="th-card")
            await h.kernel.process_event(ev)

            assert len(h.sink.posts) == 1
            channel, message, requested_by, thread_ts, _endpoint = h.sink.posts[0]
            assert channel == "C1"
            assert thread_ts == "th-card"
            assert requested_by == "U1"
            # The mandatory text fallback carries the summary; the adapter derives
            # the "Approval required: ..." card fallback from it below the seam.
            assert message.text == "Give ACME a 20% discount"
            # A Confirm intent, not buttons: the record id rides both actions so a
            # click resolves exactly this approval.
            intent = message.interaction
            assert isinstance(intent, ConfirmIntent)
            assert intent.id == "appr-1"
            assert intent.confirm.value == "appr-1"
            assert intent.cancel.value == "appr-1"
            assert (intent.confirm.label, intent.cancel.label) == ("Approve", "Reject")
            # #1053: the decision may carry a reason. Asserted at the KERNEL,
            # not only at the renderer, because this is the field that decides
            # whether the real product path offers a note at all -- a renderer
            # test alone would stay green with the kernel emitting the default.
            #
            # #1076: and asserted UNCONDITIONALLY, because the value being the
            # same for every card is itself the decision. Nothing here reads
            # config, so if a toggle is ever added this assertion is where the
            # change surfaces, rather than the always-on behavior quietly
            # becoming sometimes-on.
            assert intent.allow_free_text is True

    asyncio.run(go())


def test_every_posted_card_carries_the_note_variant(make_harness) -> None:
    """#1076: always-on is the decision, so pin it across card SHAPES.

    ``allow_free_text`` is set at one call site with no config behind it, which
    is easy to read as an accident of that call site. This drives the two shapes
    that differ -- the in-thread card of an UNROUTED approval and the top-level
    card of a ROUTED one -- and asserts both carry it. A change that made it
    conditional on the routing branch would fail here rather than surfacing only
    as a UX difference nobody tests.
    """

    async def go() -> None:
        # Unrouted: the card joins the requesting thread.
        async with make_harness(approvals=RecordingApprovals()) as h:
            h.runner.default_script = _awaiting_script("Give ACME a 20% discount")
            await h.kernel.process_event(_qevent("please discount", thread="th-unrouted"))
            unrouted = h.sink.posts[0]

        # Routed: the card posts top-level in the bound channel.
        binding = RoutedBinding({"finance": _resolution_route("C0EXAMPLE2")})
        async with make_harness(approvals=RecordingApprovals(), binding=binding) as h:
            h.runner.default_script = _awaiting_routed_script("Approve the invoice", "finance")
            await h.kernel.process_event(_qevent("please invoice", thread="th-routed"))
            routed = h.sink.posts[0]

        # The two really are different shapes, or this test proves nothing.
        assert unrouted[3] == "th-unrouted", "the unrouted card must be in-thread"
        assert routed[3] is None, "the routed card must be top-level"

        for label, (channel, message, _by, _ts, _endpoint) in (
            ("unrouted", unrouted),
            ("routed", routed),
        ):
            intent = message.interaction
            assert isinstance(intent, ConfirmIntent)
            assert intent.allow_free_text is True, (
                f"the {label} card for {channel} came without the note variant; "
                "every card carries it (#1076)"
            )

    asyncio.run(go())


def test_escalation_paths_post_no_card(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:  # no approvals backend wired
            h.runner.default_script = _awaiting_script("Anything")
            await h.kernel.process_event(_qevent("gate this"))
            assert h.sink.posts == []

    asyncio.run(go())


def _awaiting_routed_script(summary: str, route: str) -> list:
    return [
        TextDelta(text="Requesting sign-off"),
        Final(
            text="Requesting sign-off",
            status=AWAITING,
            approval_summary=summary,
            approval_route=route,
        ),
    ]


class RoutedBinding:
    """A minimal binding stand-in: one channel -> one agent with route bindings."""

    def __init__(self, routes: dict | None) -> None:
        self.routes = routes
        self.agent_id = uuid.uuid4()

    async def resolve(self, kind: str, adapter: str | None, channel: str):  # noqa: ANN201
        from curie_worker.binding import ResolvedDeployment

        return ResolvedDeployment(
            agent_id=self.agent_id,
            agent_name="test-agent",
            version_id=uuid.uuid4(),
            version_label="v1",
            bundle_ref=None,
            max_usd_per_day=None,
            max_output_tokens_per_run=None,
            approval_routes=self.routes,
        )

    def packs_for(self, resolved):  # noqa: ANN001, ANN201
        from curie_worker.behaviorpacks import BehaviorPacks

        return BehaviorPacks.from_config(None)

    def budget_for(self, resolved):  # noqa: ANN001, ANN201
        from aci_protocol import Budget

        return Budget(max_output_tokens_per_run=1000, max_usd_per_day=1.0)

    def boot_env(self, resolved, thread_key, *, kind=None, address=None, **_: object):  # noqa: ANN001, ANN201
        return {"CURIE_SESSION_ID": f"s-{thread_key}"}


_NOTIFICATION_ENDPOINT = "https://adapter.example.com/replies"


def _resolution_route(address: str = "C0EXAMPLE1") -> dict:
    return {"resolution": {"kind": "slack", "address": address}}


def _notification_route(**changes) -> dict:
    notification = {
        "kind": "email",
        "address": "approvals@example.com",
        "endpoint": _NOTIFICATION_ENDPOINT,
        "adapter": "mail",
    }
    notification.update(changes)
    return {**_resolution_route(), "notification": notification}


def _split_approval_routes() -> dict:
    return {"managers": _notification_route()}


_MALFORMED_NOTIFICATION_OVERRIDES = [
    ("extra-key", {"unexpected": True}),
    ("kind-whitespace", {"kind": " email"}),
    ("kind-uppercase", {"kind": "Email"}),
    ("address-whitespace", {"address": "approvals team@example.com"}),
    (
        "slack-name-is-not-an-id",
        {"kind": "slack", "address": "#notify", "endpoint": None, "adapter": None},
    ),
    ("adapter-whitespace", {"adapter": "mail adapter"}),
    ("adapter-uppercase", {"adapter": "Mail"}),
    (
        "same-as-resolution",
        {"kind": "slack", "address": "C0EXAMPLE1", "endpoint": None, "adapter": None},
    ),
    ("half-configured-transport", {"adapter": None}),
    ("non-slack-needs-transport", {"endpoint": None, "adapter": None}),
    ("endpoint-http-only", {"endpoint": "ftp://adapter.example.com/replies"}),
    ("endpoint-needs-host", {"endpoint": "https:///replies"}),
    ("endpoint-no-userinfo", {"endpoint": "https://user@adapter.example.com/replies"}),
    (
        # ADR-0168 decision 3: a Slack notification names its identity and
        # carries no transport; an endpoint is the retired custom-transport form.
        "slack-notification-with-an-endpoint",
        {
            "kind": "slack",
            "address": "C0EXAMPLE2",
            "endpoint": "https://adapter.example.com/replies",
            "adapter": "some-slug",
        },
    ),
]


def test_routed_approval_cards_go_to_the_bound_channel(make_harness) -> None:
    """#247: the manifest route resolves through the agent's bindings; the card
    lands in the bound channel (top-level, no foreign thread) and the record
    carries route + card_channel so the authorizer counts THAT channel. #451:
    the triggering turn has no per-turn endpoint (a Slack-triggered turn), so
    the card also rides the worker's default Slack transport (``None``)."""

    async def go() -> None:
        approvals = RecordingApprovals()
        binding = RoutedBinding({"managers": _resolution_route()})
        async with make_harness(approvals=approvals, binding=binding) as h:
            h.runner.default_script = _awaiting_routed_script("Discount for ACME", "managers")
            await h.kernel.process_event(_qevent("discount?", thread="th-routed"))

            req = approvals.requests[0]
            assert req.route == "managers"
            assert req.card_channel == "C0EXAMPLE1"
            # Card posted to the bound channel, top-level (no thread there).
            channel, message, _requested_by, thread_ts, endpoint = h.sink.posts[0]
            assert channel == "C0EXAMPLE1"
            assert thread_ts is None
            assert isinstance(message.interaction, ConfirmIntent)
            assert endpoint is None

    asyncio.run(go())


def test_routed_approval_posts_one_interactive_resolution_card_and_one_text_only_notification(
    make_harness,
) -> None:
    """The split adds visibility without adding a second resolver.

    The resolution target owns the only ``ConfirmIntent`` and the only durable
    card ref. The notification directs readers back to that configured surface,
    but carries neither its identifier nor an interaction payload.
    """

    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(
            approvals=approvals, binding=RoutedBinding(_split_approval_routes())
        ) as h:
            h.runner.default_script = _awaiting_routed_script("Discount for ACME", "managers")
            await h.kernel.process_event(_qevent("discount?", thread="th-split"))

            assert len(approvals.requests) == 1
            assert approvals.requests[0].card_channel == "C0EXAMPLE1"

            posts = [
                (event, route)
                for event, route, _best_effort in h.sink.events
                if isinstance(event, ReplyPost)
            ]
            assert len(posts) == 2
            (card, card_route), (notification, notification_route) = posts

            assert card.target.kind == "slack"
            assert card.target.address == "C0EXAMPLE1"
            assert card.target.conversation_id is None
            assert isinstance(card.message.interaction, ConfirmIntent)
            assert card_route.endpoint is None
            assert card_route.adapter is None

            assert notification.target.kind == "email"
            assert notification.target.address == "approvals@example.com"
            assert notification.target.conversation_id is None
            assert notification.message.interaction is None
            assert notification.message.text == (
                "Approval appr-1 requires review: Discount for ACME. "
                "Resolve in the configured approval channel."
            )
            assert "C0EXAMPLE1" not in notification.message.text
            assert notification_route.endpoint == _NOTIFICATION_ENDPOINT
            assert notification_route.adapter == "mail"

            card_keys = [
                key async for key in h.async_redis.scan_iter(match=h.config.approval_card_key("*"))
            ]
            assert card_keys == [h.config.approval_card_key("appr-1")]

    asyncio.run(go())


def test_a_slack_notification_naming_an_identity_with_no_endpoint_parses(make_harness) -> None:
    """ADR-0168 decision 3: a Slack notification target may carry an identity
    in ``adapter`` with NO ``endpoint`` -- a complete, resolvable route on the
    worker's own Slack transport (D4.4), not the half-configured shape the
    both-or-neither rule refuses.
    """

    async def go() -> None:
        approvals = RecordingApprovals()
        routes = {
            "managers": {
                **_resolution_route(),
                "notification": {"kind": "slack", "address": "C0EXAMPLE2", "adapter": "second"},
            }
        }
        async with make_harness(approvals=approvals, binding=RoutedBinding(routes)) as h:
            h.runner.default_script = _awaiting_routed_script("Discount for ACME", "managers")
            await h.kernel.process_event(_qevent("discount?", thread="th-slack-identity"))

            assert len(approvals.requests) == 1

            posts = [
                (event, route)
                for event, route, _best_effort in h.sink.events
                if isinstance(event, ReplyPost)
            ]
            assert len(posts) == 2
            (_card, _card_route), (notification, notification_route) = posts

            assert notification.target.kind == "slack"
            assert notification.target.address == "C0EXAMPLE2"
            assert notification_route.endpoint is None
            assert notification_route.adapter == "second"

    asyncio.run(go())


def test_a_notification_on_the_resolutions_own_pair_under_another_identity_is_not_a_duplicate(
    make_harness,
) -> None:
    """Duplicate detection keys on IDENTITY (ADR-0168 decision 3), not merely
    the ``(kind, address)`` pair: a notification aimed at the resolution's own
    Slack channel but under a DIFFERENT bot identity is a distinct route, not
    the resolution card posted again, so it must parse rather than be refused
    as a same-as-resolution duplicate.
    """

    async def go() -> None:
        approvals = RecordingApprovals()
        routes = {
            "managers": {
                **_resolution_route(),
                "notification": {"kind": "slack", "address": "C0EXAMPLE1", "adapter": "second"},
            }
        }
        async with make_harness(approvals=approvals, binding=RoutedBinding(routes)) as h:
            h.runner.default_script = _awaiting_routed_script("Discount for ACME", "managers")
            await h.kernel.process_event(_qevent("discount?", thread="th-same-pair-identity"))

            assert len(approvals.requests) == 1

            posts = [
                (event, route)
                for event, route, _best_effort in h.sink.events
                if isinstance(event, ReplyPost)
            ]
            assert len(posts) == 2
            (_card, _card_route), (notification, notification_route) = posts

            assert notification.target.kind == "slack"
            assert notification.target.address == "C0EXAMPLE1"
            assert notification_route.adapter == "second"

    asyncio.run(go())


def test_notification_failure_leaves_resolution_card_and_durable_pause_intact(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(
            approvals=approvals, binding=RoutedBinding(_split_approval_routes())
        ) as h:
            original_emit = h.sink.emit
            notification_attempted = False

            async def fail_notification(event, **kwargs):
                nonlocal notification_attempted
                if isinstance(event, ReplyPost) and event.message.interaction is None:
                    notification_attempted = True
                    raise RuntimeError("injected notification delivery failure")
                return await original_emit(event, **kwargs)

            monkeypatch.setattr(h.sink, "emit", fail_notification)
            h.runner.default_script = _awaiting_routed_script("Discount for ACME", "managers")
            await h.kernel.process_event(_qevent("discount?", thread="th-notification-failure"))

            assert notification_attempted
            assert len(approvals.requests) == 1
            assert len(h.sink.posts) == 1
            assert isinstance(h.sink.posts[0][1].interaction, ConfirmIntent)
            remembered = await _peek_card_ref(h, "appr-1")
            assert remembered is not None
            assert remembered["channel"] == "C0EXAMPLE1"
            assert [s.operating_mode for s in h.fake_k8s.sandboxes.values()] == ["Suspended"]

    asyncio.run(go())


def test_resolution_card_transport_failure_still_posts_text_notification_and_preserves_durable_pause(  # noqa: E501
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(
            approvals=approvals, binding=RoutedBinding(_split_approval_routes())
        ) as h:
            original_emit = h.sink.emit
            card_attempted = False

            async def fail_resolution_card(event, **kwargs):
                nonlocal card_attempted
                if isinstance(event, ReplyPost) and isinstance(
                    event.message.interaction, ConfirmIntent
                ):
                    card_attempted = True
                    raise RuntimeError("injected resolution card delivery failure")
                return await original_emit(event, **kwargs)

            monkeypatch.setattr(h.sink, "emit", fail_resolution_card)
            h.runner.default_script = _awaiting_routed_script("Discount for ACME", "managers")
            await h.kernel.process_event(_qevent("discount?", thread="th-card-failure"))

            assert card_attempted
            assert len(approvals.requests) == 1
            assert approvals.requests[0].card_channel == "C0EXAMPLE1"
            assert [s.operating_mode for s in h.fake_k8s.sandboxes.values()] == ["Suspended"]
            assert len(h.sink.posts) == 1
            channel, message, _requested_by, thread_ts, endpoint = h.sink.posts[0]
            assert channel == "approvals@example.com"
            assert message.interaction is None
            assert "appr-1" in message.text
            assert "C0EXAMPLE1" not in message.text
            assert "configured approval channel" in message.text
            assert thread_ts is None
            assert endpoint == _NOTIFICATION_ENDPOINT
            assert await _peek_card_ref(h, "appr-1") is None

    asyncio.run(go())


def test_unbound_route_escalates_instead_of_routing_to_the_requesting_channel(
    make_harness,
) -> None:
    """(19, #544 Decision B / AC2) A named but UNBOUND route escalates loudly:
    no approval is created and no card is posted, so authority never widens to
    the requesting channel. This deliberately REVERSES #247's silent
    channel-fallback (the behavior this test used to assert) -- the fallback was
    the same silent widening from the other end.
    """

    async def go() -> None:
        approvals = RecordingApprovals()
        binding = RoutedBinding(None)  # agent has no bindings at all
        async with make_harness(approvals=approvals, binding=binding) as h:
            h.runner.default_script = _awaiting_routed_script("Anything", "managers")
            ev = _qevent("gate", thread="th-unbound")
            await h.kernel.process_event(ev)

            # No approval was created for the unresolvable route ...
            assert approvals.requests == []
            # ... and no card was posted anywhere (never widened to a channel).
            assert h.sink.posts == []
            # The human-visible escalation names the unbound route.
            assert h.sink.last_text is not None
            assert "managers" in h.sink.last_text
            # The event is terminally handled (done), not left to retry.
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


@pytest.mark.parametrize(
    "route_binding",
    [
        pytest.param(
            {"resolution": {"kind": "slack", "address": "#finance"}},
            id="slack-name-is-not-an-id",
        ),
        pytest.param(
            {"resolution": {"kind": "email", "address": "review@example.com"}},
            id="non-slack-resolution",
        ),
        pytest.param(
            {
                "resolution": {
                    "kind": "slack",
                    "address": "C0EXAMPLE3",
                    "unexpected": True,
                }
            },
            id="resolution-extra-key",
        ),
        pytest.param({"channel": "C0EXAMPLE3"}, id="retired-channel-key"),
        pytest.param(
            {
                "resolution": {"mode": "requesting_surface"},
                "notification": {"kind": "slack", "address": "C0EXAMPLE3"},
            },
            id="mode-with-notification",
        ),
        pytest.param(
            {
                "resolution": {
                    "mode": "requesting_surface",
                    "kind": "slack",
                    "address": "C0EXAMPLE3",
                }
            },
            id="mode-mixed-with-a-fixed-target",
        ),
        pytest.param({"resolution": {"mode": "anywhere"}}, id="mode-unknown"),
        *[
            pytest.param(_notification_route(**overrides), id=f"notification-{case}")
            for case, overrides in _MALFORMED_NOTIFICATION_OVERRIDES
        ],
    ],
)
def test_malformed_route_target_escalates_without_creating_an_approval(
    make_harness, route_binding: dict
) -> None:
    """Out-of-band JSONB cannot widen or invent a resolution surface.

    The worker independently pins the API's target identity, transport pair,
    endpoint, strict-envelope, and retired-key rules. The database migration is
    the only legacy-shape translator.
    """

    async def go() -> None:
        approvals = RecordingApprovals()
        binding = RoutedBinding({"managers": route_binding})
        async with make_harness(approvals=approvals, binding=binding) as h:
            h.runner.default_script = _awaiting_routed_script("Anything", "managers")
            ev = _qevent("gate", thread="th-malformed-route")
            await h.kernel.process_event(ev)

            assert approvals.requests == []
            assert h.sink.posts == []
            assert h.sink.last_text is not None
            assert "managers" in h.sink.last_text
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_routeless_approval_keeps_prior_behavior(make_harness) -> None:
    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting_script("Plain request")
            await h.kernel.process_event(_qevent("gate", thread="th-plain"))

            req = approvals.requests[0]
            assert req.route is None
            assert req.card_channel == "C1"

    asyncio.run(go())


# --- Card transport follows the card's channel, not the trigger (#451) --------

_CLI_STUB = "http://localhost:8155"


def test_routed_card_ignores_the_triggering_turns_endpoint(make_harness) -> None:
    """#451: the card's channel is policy (the manifest route binding), so its
    transport must be too. A CLI-triggered turn carries a local stub endpoint;
    delivering a route-bound card through it posts the card at the stub instead
    of the real Slack workspace, so the bound channel never sees it. ``None``
    means the worker's default Slack transport."""

    async def go() -> None:
        approvals = RecordingApprovals()
        binding = RoutedBinding({"managers": _resolution_route()})
        async with make_harness(approvals=approvals, binding=binding) as h:
            h.runner.default_script = _awaiting_routed_script("Discount for ACME", "managers")
            await h.kernel.process_event(
                _qevent("discount?", thread="th-cli-routed", endpoint=_CLI_STUB)
            )

            channel, _message, _requested_by, thread_ts, endpoint = h.sink.posts[0]
            assert channel == "C0EXAMPLE1"
            assert thread_ts is None
            assert endpoint is None

    asyncio.run(go())


def test_card_routed_to_requesting_channel_keeps_the_trigger_endpoint(
    make_harness,
) -> None:
    """The inverse of the routed case: when the route binds back to the channel
    that asked, the card belongs to that conversation -- it threads under it and
    rides the same transport the trigger arrived on, so a CLI-stub turn's card
    stays at the stub."""

    async def go() -> None:
        approvals = RecordingApprovals()
        # The policy target is the requesting channel itself.
        binding = RoutedBinding({"managers": _resolution_route("C0EXAMPLE4")})
        async with make_harness(approvals=approvals, binding=binding) as h:
            h.runner.default_script = _awaiting_routed_script("Ship it", "managers")
            await h.kernel.process_event(
                _qevent(
                    "ship?",
                    thread="th-self-routed",
                    endpoint=_CLI_STUB,
                    channel="C0EXAMPLE4",
                )
            )

            channel, _message, _requested_by, thread_ts, endpoint = h.sink.posts[0]
            assert channel == "C0EXAMPLE4"
            assert thread_ts == "th-self-routed"
            assert endpoint == _CLI_STUB

    asyncio.run(go())


# --- Expired-approval card teardown (#419) ------------------------------------


def _resume_turn(text: str, *, thread: str, approval_id: str, author: str) -> QueuedTurn:
    """The API's approval resume turn: the deterministic ``approval-<id>-resolved``
    event id both the resolve and expiry paths stamp, replayed into the same
    placeholder. The expiry path authors it as "system"; a resolve names the
    resolver."""

    return QueuedTurn(
        event_id=f"approval-{approval_id}-resolved",
        conversation_id=thread,
        author=author,
        text=text,
        reply_handle=ReplyHandle(kind="slack", channel="C1", placeholder="p-1", endpoint=None),
        received_at="2026-07-14T00:00:00+00:00",
    )


# ADR-0179 decision 2: the one line left above a card posted in the requester's
# own thread. No approval id and no session vocabulary; the card says the rest.
_SHORT_NOTICE = "Approval requested. See the card below."


def _card_post_index(h) -> int:
    """Where the approval card sits in the neutral emit log."""

    return next(
        i
        for i, (event, _route, _best) in enumerate(h.sink.events)
        if isinstance(event, ReplyPost) and isinstance(event.message.interaction, ConfirmIntent)
    )


def test_an_in_thread_card_leaves_one_plain_line_above_it(make_harness) -> None:
    """ADR-0179 decision 2: the notice points at the card instead of repeating it.

    The card already shows the summary and the requester to the person it
    addresses, so the placeholder above it says only that approval was asked.
    The model's own text before the pause still comes first.
    """

    async def go() -> None:
        async with make_harness(approvals=RecordingApprovals()) as h:
            h.runner.default_script = _awaiting_script("Refund order 42")
            await h.kernel.process_event(_qevent("refund?", thread="th-short"))

            placeholder_texts = [text for _, ref, text in h.sink.updates if ref == "p-1"]
            assert placeholder_texts[-1].split("\n\n") == ["Requesting sign-off", _SHORT_NOTICE]
            assert "appr-1" not in placeholder_texts[-1]
            # And the card is the message after it, in the same thread.
            _channel, _message, _requested_by, thread_ts, _endpoint = h.sink.posts[0]
            assert thread_ts == "th-short"

    asyncio.run(go())


def test_a_routed_card_keeps_the_full_notice_with_its_id(make_harness) -> None:
    """The negative of decision 2: no card follows the notice in this thread.

    A routed card goes to its bound channel, so the requester's thread gets the
    full notice, and the CLI reads the id from it (``parse_approval_id``).
    """

    async def go() -> None:
        binding = RoutedBinding({"managers": _resolution_route()})
        async with make_harness(approvals=RecordingApprovals(), binding=binding) as h:
            h.runner.default_script = _awaiting_routed_script("Discount for ACME", "managers")
            await h.kernel.process_event(_qevent("discount?", thread="th-routed-notice"))

            assert h.sink.last_text is not None
            assert "Awaiting approval (appr-1): Discount for ACME" in h.sink.last_text
            assert _SHORT_NOTICE not in h.sink.last_text

    asyncio.run(go())


def test_the_resumed_answer_is_posted_below_an_in_thread_card(make_harness) -> None:
    """ADR-0179 decision 3: the thread reads request, card, answer.

    The resume turn does not edit the placeholder above the card. Its first
    delivery posts a new message after the card, and the rest of the turn edits
    that message; the placeholder keeps the one line.
    """

    async def go() -> None:
        reader = RecordingReader(_APPROVED)
        thread = "th-order"
        async with make_harness(approvals=RecordingApprovals(), approval_reader=reader) as h:
            await _pause_awaiting_approval(h, thread)
            reader.resolve("appr-1")
            card_index = _card_post_index(h)
            paused_events = len(h.sink.events)

            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval resolved] approved by U9",
                    thread=thread,
                    approval_id="appr-1",
                    author="U9",
                )
            )

            resumed_text = [
                event
                for event, _route, _best in h.sink.events[paused_events:]
                if isinstance(event, ReplyUpdate) and event.message is None
            ]
            assert resumed_text, "the resume delivered no text"
            assert resumed_text[0].target.reply_ref is None, "the first delivery must post"
            assert paused_events > card_index
            minted = h.sink.text_posts[-1][1]
            assert {e.target.reply_ref for e in resumed_text[1:]} <= {minted}
            assert h.sink.updates[-1] == ("C1", minted, "Refunded.")
            placeholder_texts = [text for _, ref, text in h.sink.updates if ref == "p-1"]
            assert placeholder_texts[-1].endswith(_SHORT_NOTICE)

    asyncio.run(go())


def test_the_answer_goes_below_the_card_even_after_the_card_ref_is_consumed(
    make_harness,
) -> None:
    """The choice is remembered at pause time, not read from the card ref.

    Settling the card consumes its ref, so a redelivered resume would otherwise
    find nothing and answer above the card after the first try answered below.
    """

    async def go() -> None:
        reader = RecordingReader(_APPROVED)
        thread = "th-order-consumed"
        async with make_harness(approvals=RecordingApprovals(), approval_reader=reader) as h:
            await _pause_awaiting_approval(h, thread)
            reader.resolve("appr-1")
            await h.async_redis.delete(h.config.approval_card_key("appr-1"))

            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval resolved] approved by U9",
                    thread=thread,
                    approval_id="appr-1",
                    author="U9",
                )
            )

            minted = h.sink.text_posts[-1][1]
            assert h.sink.updates[-1] == ("C1", minted, "Refunded.")

    asyncio.run(go())


def test_a_placeholderless_resume_answers_below_the_card(make_harness) -> None:
    """Decision 3 on the #2721 path: the minted notice is above the card too.

    A turn with no placeholder mints its notice message, and the resume would
    adopt that ref. With the card below the notice, it posts after the card
    instead and leaves the notice's one line alone.
    """

    async def go() -> None:
        async with make_harness(approvals=RecordingApprovals(), slack_no_edit_streaming=True) as h:
            h.runner.default_script = _awaiting_script("Give ACME a 20% discount")
            thread = "th_no_edit_in_thread"
            await h.kernel.process_event(
                _qevent("please discount", thread=thread, placeholder=None)
            )
            notice_ref = h.sink.text_posts[0][1]
            assert h.sink.updates[-1] == (
                "C1",
                notice_ref,
                f"Requesting sign-off\n\n{_SHORT_NOTICE}",
            )

            h.runner.default_script = [Final(text="Discount applied.", status=DONE)]
            await h.kernel.process_event(
                _qevent(
                    "[approval resolved] approved by U9",
                    thread=thread,
                    event_id="approval-appr-1-resolved",
                    placeholder=None,
                )
            )

            assert len(h.sink.text_posts) == 2, h.sink.text_posts
            answer_ref = h.sink.text_posts[1][1]
            assert answer_ref != notice_ref
            assert h.sink.updates[-1] == ("C1", answer_ref, "Discount applied.")

    asyncio.run(go())


def test_a_resume_without_the_memory_edits_the_notice_as_before(make_harness) -> None:
    """The fallback of decision 3: a pause from before this change, or a lapsed TTL."""

    async def go() -> None:
        reader = RecordingReader(_APPROVED)
        thread = "th-order-forgotten"
        async with make_harness(approvals=RecordingApprovals(), approval_reader=reader) as h:
            await _pause_awaiting_approval(h, thread)
            reader.resolve("appr-1")
            assert await h.async_redis.exists(h.config.approval_reply_below_card_key("appr-1"))
            await h.async_redis.delete(h.config.approval_reply_below_card_key("appr-1"))

            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval resolved] approved by U9",
                    thread=thread,
                    approval_id="appr-1",
                    author="U9",
                )
            )

            assert h.sink.updates[-1] == ("C1", "p-1", "Refunded.")

    asyncio.run(go())


def test_a_card_acknowledged_without_a_ref_keeps_todays_reply(make_harness) -> None:
    """A channel with no message to address (email acks with no ref) keeps its reply.

    The notice is still the one line, because it is chosen before the card is
    posted, but nothing is remembered, so the resumed answer edits the turn's own
    message exactly as it did before.
    """

    async def go() -> None:
        reader = RecordingReader(_APPROVED)
        thread = "th-order-refless"
        async with make_harness(approvals=RecordingApprovals(), approval_reader=reader) as h:
            original_emit = h.sink.emit

            async def refless_card(
                event: ReplyEvent,
                *,
                route: TargetRoute,
                best_effort_unreachable: bool = False,
            ) -> ReplyAck:
                ack = await original_emit(
                    event, route=route, best_effort_unreachable=best_effort_unreachable
                )
                return ReplyAck(ref=None) if isinstance(event, ReplyPost) else ack

            h.sink.emit = refless_card
            h.runner.default_script = _awaiting_script("Refund order 42")
            await h.kernel.process_event(_qevent("refund?", thread=thread))
            assert not await h.async_redis.exists(h.config.approval_reply_below_card_key("appr-1"))
            reader.resolve("appr-1")

            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval resolved] approved by U9",
                    thread=thread,
                    approval_id="appr-1",
                    author="U9",
                )
            )

            assert h.sink.updates[-1] == ("C1", "p-1", "Refunded.")

    asyncio.run(go())


def test_expiry_resume_disables_the_approval_card(make_harness) -> None:
    """#419: an EXPIRED approval's resume turn (author "system", enqueued by the
    #412 sweeper or a past-SLA resolve attempt) disables the live card in place --
    buttons gone, an expiry line in their stead -- mirroring the resolved-card
    edit, since no click will ever arrive to do it."""

    async def go() -> None:
        approvals = RecordingApprovals()
        thread = "th-expire-card"
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting_script("Give ACME a 20% discount")
            await h.kernel.process_event(_qevent("please discount", thread=thread))

            # The live card was posted and its location remembered, because an
            # expiry (unlike a resolve) carries no click to locate the card.
            assert len(h.sink.posts) == 1
            assert await h.async_redis.exists(h.config.approval_card_key("appr-1"))
            assert not await h.async_redis.exists(h.config.approval_card_key(thread))
            card_ts = "posted-1"  # the FakeSink's returned ts for the first post

            # The expiry resume turn the sweeper enqueues (author "system").
            h.runner.default_script = [Final(text="Acknowledged the expiry.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval expired] not approved in time",
                    thread=thread,
                    approval_id="appr-1",
                    author="system",
                )
            )

            # The card was edited in place: same ts, and a channel-neutral message
            # carrying the remembered summary. The buttonless expired-card render
            # (no actions block, an expiry line) is the adapter's job below the
            # seam -- asserted in test_slack_sink.py.
            assert len(h.sink.card_updates) == 1
            channel, ts, message, endpoint, settled = h.sink.card_updates[0]
            # Expiry means nobody decided, so the settled outcome carries no
            # decision and the adapter renders the expired form (#1084).
            assert settled is not None and settled.decision is None
            assert (channel, ts) == ("C1", card_ts)
            assert endpoint is None
            assert message.text == "Give ACME a 20% discount"

            # The memory was consumed after delivery, so a redelivery no-ops.
            assert not await h.async_redis.exists(h.config.approval_card_key("appr-1"))

            # The continuation still streamed into the placeholder.
            assert h.sink.last_text == "Acknowledged the expiry."

    asyncio.run(go())


def test_resolve_resume_stamps_the_card_from_the_record(make_harness) -> None:
    """#1084: a RESOLVE resume settles the card, it no longer leaves it live.

    This asserted the opposite until #1084, on the premise that "the dispatcher
    already did from the click". That holds only when there WAS a click: a
    resolution through ``POST /approvals/{id}/resolve`` or ``curie <tier>
    approvals --resolve`` never touches Slack, so the card kept its buttons and
    every later click earned a 409. The worker is the only component that still
    knows where the card is, so settling it belongs here.

    The verdict comes from the durable record, not from the platform-authored
    resume prose -- that sentence is written for a model, and rebuilding a
    decision out of it by regex is how the card would start lying after a
    wording change.
    """

    async def go() -> None:
        reader = RecordingReader(_APPROVED)
        thread = "th-resolve-card"
        async with make_harness(approvals=RecordingApprovals(), approval_reader=reader) as h:
            await _pause_awaiting_approval(h, thread)
            reader.resolve("appr-1")

            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval resolved] approved by U9",
                    thread=thread,
                    approval_id="appr-1",
                    author="U9",
                )
            )

            # The card was settled, with the outcome read off the record.
            assert len(h.sink.card_updates) == 1
            _channel, _ts, message, _endpoint, settled = h.sink.card_updates[0]
            assert message.text == "Refund order 42", "the summary must survive"
            assert settled is not None
            assert settled.decision == "approved"
            assert settled.resolver == "U9"
            assert settled.note == "approved for Q3"
            # And the requester the live card named is carried into the rebuild,
            # so the settled card is not missing a line the original had.
            # "U1" is the author `_qevent` stamps on the triggering turn, which
            # is exactly what `approval_card` rendered as "Requested by".
            assert settled.requested_by == "U1"

            # The pause read its record back once the card was registered and
            # found it pending (#3637); the resume's read was keyed off the
            # resume turn's deterministic event id.
            assert reader.reads == ["appr-1", "appr-1"]

            # The memory is still consumed, so a later approval cannot collide.
            assert not await h.async_redis.exists(h.config.approval_card_key("appr-1"))

    asyncio.run(go())


@pytest.mark.parametrize("timed", [True, False], ids=["with-time", "without-time"])
def test_a_resolve_resume_carries_the_records_decision_time_to_the_card(
    make_harness, timed: bool
) -> None:
    """ADR-0179 decision 1: the settled card says when, read off the record.

    The reply wire's ``SettledOutcome`` is decoded strictly by out-of-process
    adapters, so the instant travels in the settle message's existing ``fields``
    list, which every adapter already accepts. A record with no time carries no
    field rather than a guessed one.
    """

    from curie_worker.approvals import decided_at

    decided = datetime(2026, 9, 21, 14, 13, 20, tzinfo=UTC)
    record = (
        SettledApproval(
            status="approved", resolved_by="U9", resolution_note=None, resolved_at=decided
        )
        if timed
        else _APPROVED
    )

    async def go() -> None:
        reader = RecordingReader(record)
        thread = "th-decided"
        async with make_harness(approvals=RecordingApprovals(), approval_reader=reader) as h:
            await _pause_awaiting_approval(h, thread)
            reader.resolve("appr-1")
            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval resolved] approved by U9",
                    thread=thread,
                    approval_id="appr-1",
                    author="U9",
                )
            )

            _channel, _ts, message, _endpoint, settled = h.sink.card_updates[0]
            assert settled is not None and settled.decision == "approved"
            if timed:
                assert decided_at(message) == decided
            else:
                assert message.fields == []

    asyncio.run(go())


def test_a_resolve_resume_leaves_the_card_alone_when_the_record_cannot_be_read(
    make_harness,
) -> None:
    """No record, no stamp (#1084).

    The kernel would have to invent a decision to render anything, and a card
    stating a verdict nobody confirmed is worse than one still showing buttons:
    the buttons are at least honest about the platform not having told Slack
    yet, and the next click gets the real answer from the API.

    #1199 flipped the key assertion below: the ref must SURVIVE a pass that
    stamped nothing.
    """

    async def go() -> None:
        reader = RecordingReader(None)
        thread = "th-unreadable-record"
        async with make_harness(approvals=RecordingApprovals(), approval_reader=reader) as h:
            await _pause_awaiting_approval(h, thread)
            reader.resolve("appr-1")

            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval resolved] approved by U9",
                    thread=thread,
                    approval_id="appr-1",
                    author="U9",
                )
            )

            assert h.sink.card_updates == []
            # #1199: the ref SURVIVES, because nothing was stamped. It is the
            # only pointer the platform still holds to the posted card, and a
            # failed record read is routinely transient (a 503, a connection
            # blip), so consuming it here would permanently strand a card with
            # live-looking Approve/Reject buttons that no later pass can settle.
            # The ref is only spent once a stamp is actually attempted.
            assert await h.async_redis.exists(h.config.approval_card_key("appr-1"))

    asyncio.run(go())


def test_a_resolve_resume_with_no_reader_configured_still_resumes(make_harness) -> None:
    """Progressive enhancement, per ADR-0020: a deployment with nothing to read
    the record with settles no card and continues the run normally. The stamp is
    an enrichment on a decision that already happened."""

    async def go() -> None:
        thread = "th-no-reader"
        async with make_harness(approvals=RecordingApprovals()) as h:
            await _pause_awaiting_approval(h, thread)

            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval resolved] approved by U9",
                    thread=thread,
                    approval_id="appr-1",
                    author="U9",
                )
            )

            assert h.sink.card_updates == []
            # The run itself continued: the resumed reply was delivered.
            assert h.sink.updates, "the resume must still produce a reply"

    asyncio.run(go())


def test_resolve_authored_by_a_system_named_actor_does_not_expire_the_card(
    make_harness,
) -> None:
    """#419 hardening: the expiry-vs-resolve discriminator is the platform text
    marker, NOT the author. A resolver whose identity is literally "system" (the
    codebase's reserved machine-actor name) must not get its RESOLVED card wrongly
    stamped expired -- the ``[approval resolved]`` text keeps it off the expiry
    path."""

    async def go() -> None:
        approvals = RecordingApprovals()
        thread = "th-system-resolver"
        async with make_harness(approvals=approvals) as h:
            await _pause_awaiting_approval(h, thread)

            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval resolved] approved by system",
                    thread=thread,
                    approval_id="appr-1",
                    author="system",  # a resolver literally named "system"
                )
            )

            # Author is "system" but the text says RESOLVED: the card is left to
            # the dispatcher, never stamped expired by the worker. That is the
            # load-bearing assertion here.
            assert h.sink.card_updates == []
            # #1199: no reader is configured, so no stamp was even attempted, so
            # the ref survives. It is the only pointer to the posted card, and
            # spending it on a pass that settled nothing would strand a card
            # whose buttons still look live. (This asserted the opposite until
            # #1199, when the pop moved behind the record read.)
            assert await h.async_redis.exists(h.config.approval_card_key("appr-1"))

    asyncio.run(go())


# --- A transient record read must not destroy the card ref (#1199) ------------


def test_a_transient_record_read_leaves_the_ref_for_a_later_pass(make_harness) -> None:
    """#1199: an unreadable record leaves the ref for a reclaimed pass.

    The durable outcome is read before the card ref. A transient empty result
    cannot consume the only pointer to the posted card, so a reclaimed delivery
    can retry and settle it once the record is readable.

    THE MUTATION THIS CATCHES: consuming the ref after the first empty read
    leaves the recovered pass unable to stamp the card.
    """

    async def go() -> None:
        # The blip, then the recovery: one ``None`` read, then the real record.
        reader = RecordingReader(None, _APPROVED)
        thread = "th-transient-read"
        async with make_harness(approvals=RecordingApprovals(), approval_reader=reader) as h:
            await _pause_awaiting_approval(h, thread)
            reader.resolve("appr-1")

            resume = _resume_turn(
                "[approval resolved] approved by U9",
                thread=thread,
                approval_id="appr-1",
                author="U9",
            )

            # Pass 1: the record read comes back None (the blip).
            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(resume)

            # Nothing was stamped, which is correct: the kernel must not guess a
            # verdict. But the ref SURVIVES, which is the whole point of #1199.
            assert h.sink.card_updates == []
            assert await h.async_redis.exists(h.config.approval_card_key("appr-1"))

            # Pass 2 is the bounded-reclaim redelivery (ADR-0039, #505): the
            # worker died before ``mark_done``, so the entry is redelivered and
            # the kernel's done short-circuit does not fire. Clearing the done key
            # reproduces that crash-before-done state; it is not a test shortcut
            # around the idempotence guard, it IS the state reclaim delivers.
            await h.async_redis.delete(h.config.done_key(resume.event_id))
            await h.kernel.process_event(resume)

            # The recovered pass settles the card, with the outcome read off the
            # now-reachable record.
            assert len(h.sink.card_updates) == 1
            _channel, _ts, message, _endpoint, settled = h.sink.card_updates[0]
            assert message.text == "Refund order 42", "the summary must survive"
            assert settled is not None
            assert settled.decision == "approved"
            assert settled.resolver == "U9"
            assert settled.note == "approved for Q3"
            # "U1" is the author `_qevent` stamps on the triggering turn, which is
            # what the live card rendered as "Requested by"; it rides the ref.
            assert settled.requested_by == "U1"

            # And NOW the ref is consumed, because a stamp was made.
            assert not await h.async_redis.exists(h.config.approval_card_key("appr-1"))

    asyncio.run(go())


def test_a_failed_card_edit_keeps_the_ref_and_a_reclaimed_pass_settles(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#1206: delivery must succeed before the remembered ref is consumed.

    THE MUTATION THIS CATCHES: consuming before emit makes the original ref
    assertion fail after the injected card edit error.
    """

    async def go() -> None:
        reader = RecordingReader(_APPROVED)
        thread = "th-card-edit-recovery"
        async with make_harness(approvals=RecordingApprovals(), approval_reader=reader) as h:
            await _pause_awaiting_approval(h, thread)
            reader.resolve("appr-1")
            key = h.config.approval_card_key("appr-1")
            original_raw = await h.async_redis.get(key)
            assert original_raw is not None

            original_emit = h.sink.emit
            failed_once = False

            async def fail_first_settled(event, **kwargs):
                nonlocal failed_once
                if isinstance(event, ReplyUpdate) and event.settled is not None and not failed_once:
                    failed_once = True
                    raise RuntimeError("injected settled card edit failure")
                return await original_emit(event, **kwargs)

            monkeypatch.setattr(h.sink, "emit", fail_first_settled)
            resume = _resume_turn(
                "[approval resolved] approved by U9",
                thread=thread,
                approval_id="appr-1",
                author="U9",
            )
            h.runner.default_script = [Final(text="Refunded.", status=DONE)]

            await h.kernel.process_event(resume)

            assert failed_once
            assert h.sink.card_updates == []
            assert await h.async_redis.get(key) == original_raw
            assert h.sink.last_text == "Refunded."
            assert await h.async_redis.exists(h.config.done_key(resume.event_id))

            await h.async_redis.delete(h.config.done_key(resume.event_id))
            await h.kernel.process_event(resume)

            assert len(h.sink.card_updates) == 1
            assert not await h.async_redis.exists(key)

            await h.async_redis.delete(h.config.done_key(resume.event_id))
            await h.kernel.process_event(resume)

            assert len(h.sink.card_updates) == 1
            assert not await h.async_redis.exists(key)

    asyncio.run(go())


def test_a_hanging_approval_read_does_not_hold_the_same_thread_order_lock_for_30_seconds(
    make_harness,
) -> None:
    """#1208: the approval GET has its own short timeout inside thread ordering.

    THE MUTATION THIS CATCHES: removing the per read timeout leaves both tasks
    blocked past the outer deadline.
    """

    async def go() -> None:
        request_started = asyncio.Event()
        release_response = asyncio.Event()
        resolved = False

        async def hang(_request: web.Request) -> web.Response:
            # Before the resolve, the pause's read after card registration
            # (#3637) finds the record pending, as the API answers it.
            if not resolved:
                return web.json_response({"status": "pending"})
            request_started.set()
            await release_response.wait()
            return web.json_response(
                {
                    "status": "approved",
                    "resolved_by": "U9",
                    "resolution_note": "approved for Q3",
                }
            )

        app = web.Application()
        app.router.add_get("/approvals/appr-1", hang)
        server = TestServer(app)
        await server.start_server()
        try:
            async with httpx.AsyncClient(timeout=30.0) as http:
                reader = ApprovalClient(
                    api_base_url=str(server.make_url("/")),
                    api_key="",
                    client=http,
                    read_timeout_s=0.05,
                )
                async with make_harness(
                    approvals=RecordingApprovals(), approval_reader=reader
                ) as h:
                    thread = "th-bounded-approval-read"
                    await _pause_awaiting_approval(h, thread)
                    resolved = True
                    key = h.config.approval_card_key("appr-1")
                    original_raw = await h.async_redis.get(key)
                    assert original_raw is not None

                    resume = _resume_turn(
                        "[approval resolved] approved by U9",
                        thread=thread,
                        approval_id="appr-1",
                        author="U9",
                    )
                    follower = _qevent(
                        "same thread follower",
                        thread=thread,
                        event_id="after-bounded-approval-read",
                    )
                    h.runner.default_script = [Final(text="Continued.", status=DONE)]

                    resume_task = asyncio.create_task(h.kernel.process_event(resume))
                    await asyncio.wait_for(request_started.wait(), timeout=0.5)
                    follower_task = asyncio.create_task(h.kernel.process_event(follower))
                    await asyncio.sleep(0)
                    assert not follower_task.done()

                    await asyncio.wait_for(asyncio.gather(resume_task, follower_task), timeout=3.0)

                    assert await h.async_redis.exists(h.config.done_key(resume.event_id))
                    assert await h.async_redis.exists(h.config.done_key(follower.event_id))
                    assert await h.async_redis.get(key) == original_raw
                    assert h.sink.card_updates == []
        finally:
            release_response.set()
            await server.close()

    asyncio.run(go())


# --- A verdict that lands before the card is registered (#3637) ----------------

_REJECTED = SettledApproval(status="rejected", resolved_by="U9", resolution_note="not this quarter")
_EXPIRED = SettledApproval(status="expired", resolved_by=None, resolution_note=None)


def _hold_the_card_post(h, monkeypatch: pytest.MonkeyPatch) -> tuple[asyncio.Event, asyncio.Event]:  # noqa: ANN001
    """The external Slack barrier: hold the card post's acknowledgement.

    ``posting`` is set once the card post has been handed to the transport.
    Until the test sets ``release`` the kernel has no ref for it, so it cannot
    have registered the card.
    """

    posting = asyncio.Event()
    release = asyncio.Event()
    original_emit = h.sink.emit

    async def held(event: ReplyEvent, **kwargs: Any) -> ReplyAck:
        if isinstance(event, ReplyPost) and isinstance(event.message.interaction, ConfirmIntent):
            posting.set()
            await release.wait()
        return await original_emit(event, **kwargs)

    monkeypatch.setattr(h.sink, "emit", held)
    return posting, release


def _runs_of(h, text: str) -> int:  # noqa: ANN001
    """How many times the runner was handed a turn carrying ``text``."""

    return sum(text in opened for opened in h.runner.opened)


@pytest.mark.parametrize(
    ("record", "resume_text", "author", "decision"),
    [
        (_APPROVED, "[approval resolved] approved by U9", "U9", "approved"),
        (_REJECTED, "[approval resolved] rejected by U9", "U9", "rejected"),
        (_EXPIRED, "[approval expired] not approved in time", "system", None),
    ],
    ids=["approved", "rejected", "expired"],
)
def test_resolve_before_card_registration_settles_the_card(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
    record: SettledApproval,
    resume_text: str,
    author: str,
    decision: str | None,
) -> None:
    """#3637: a verdict recorded before the card is registered still settles it.

    The row is durable before any delivery, so an operator can resolve it (or the
    sweeper expire it) while the card's post is still waiting on Slack. The resume
    then runs, finds no card ref, and finishes. The card is posted and registered
    afterwards, so the pause itself must settle it from the durable record.

    THE MUTATION THIS CATCHES: registering the card without reading its record
    back leaves Approve and Reject live on a decided approval, with its ref
    stored until the TTL.
    """

    async def go() -> None:
        reader = RecordingReader(record)
        thread = f"th-settled-before-card-{record.status}"
        async with make_harness(approvals=RecordingApprovals(), approval_reader=reader) as h:
            key = h.config.approval_card_key("appr-1")
            posting, release = _hold_the_card_post(h, monkeypatch)
            h.runner.default_script = _awaiting_script("Refund order 42")
            pause = asyncio.create_task(h.kernel.process_event(_qevent("refund?", thread=thread)))
            await asyncio.wait_for(posting.wait(), timeout=5.0)

            # The session is suspended and its row exists, but its card is not
            # registered. The verdict lands now and its resume runs to the end.
            assert not await h.async_redis.exists(key)
            reader.resolve("appr-1")
            resume = _resume_turn(resume_text, thread=thread, approval_id="appr-1", author=author)
            h.runner.default_script = [Final(text="Continued.", status=DONE)]
            await asyncio.wait_for(h.kernel.process_event(resume), timeout=10.0)
            assert await h.async_redis.exists(h.config.done_key(resume.event_id))
            assert h.sink.card_updates == [], "the resume had no registered card to settle"

            # Slack acknowledges the post; the pause registers the card.
            release.set()
            await asyncio.wait_for(pause, timeout=10.0)

            assert len(h.sink.card_updates) == 1
            channel, ts, message, endpoint, settled = h.sink.card_updates[0]
            assert (channel, ts, endpoint) == ("C1", "posted-1", None)
            assert message.text == "Refund order 42"
            assert settled is not None
            assert settled.decision == decision
            assert settled.requested_by == "U1"
            assert settled.resolver == record.resolved_by
            assert settled.note == record.resolution_note
            assert not await h.async_redis.exists(key)
            assert _runs_of(h, resume_text) == 1

            # A redelivered resume stops at its done marker: no second edit and
            # no second continuation.
            await h.kernel.process_event(resume)
            assert len(h.sink.card_updates) == 1
            assert _runs_of(h, resume_text) == 1

    asyncio.run(go())


def test_registration_before_resolve_still_settles_once(make_harness) -> None:
    """#3637, the ordinary order: a card registered while pending stays live.

    The pause's read back finds the record pending and leaves the card for the
    resume, which settles it exactly once.

    THE MUTATION THIS CATCHES: settling on anything but a decided record stamps
    a card nobody has decided, or edits it twice.
    """

    async def go() -> None:
        reader = RecordingReader(_APPROVED)
        thread = "th-card-before-resolve"
        async with make_harness(approvals=RecordingApprovals(), approval_reader=reader) as h:
            await _pause_awaiting_approval(h, thread)
            assert h.sink.card_updates == [], "a pending card must stay live"
            live = await _peek_card_ref(h, "appr-1")
            assert live is not None and live["ts"] == "posted-1"

            reader.resolve("appr-1")
            resume_text = "[approval resolved] approved by U9"
            resume = _resume_turn(resume_text, thread=thread, approval_id="appr-1", author="U9")
            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(resume)

            assert len(h.sink.card_updates) == 1
            _channel, ts, _message, _endpoint, settled = h.sink.card_updates[0]
            assert ts == "posted-1"
            assert settled is not None and settled.decision == "approved"
            assert not await h.async_redis.exists(h.config.approval_card_key("appr-1"))
            assert _runs_of(h, resume_text) == 1

            # The crash-before-done redelivery finds no ref to settle again.
            await h.async_redis.delete(h.config.done_key(resume.event_id))
            await h.kernel.process_event(resume)
            assert len(h.sink.card_updates) == 1

    asyncio.run(go())


def test_a_redelivery_after_a_successful_stamp_still_finds_nothing(make_harness) -> None:
    """A successful card edit consumes its exact ref before redelivery.

    THE MUTATION THIS CATCHES: retaining the ref after a successful edit makes
    redelivery emit a second card update.
    """

    async def go() -> None:
        reader = RecordingReader(_APPROVED)
        thread = "th-redelivered-stamp"
        async with make_harness(approvals=RecordingApprovals(), approval_reader=reader) as h:
            await _pause_awaiting_approval(h, thread)
            reader.resolve("appr-1")

            resume = _resume_turn(
                "[approval resolved] approved by U9",
                thread=thread,
                approval_id="appr-1",
                author="U9",
            )

            # The healthy pass: the card is settled and the ref is spent.
            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(resume)
            assert len(h.sink.card_updates) == 1
            assert not await h.async_redis.exists(h.config.approval_card_key("appr-1"))

            # The redelivery, in the same crash-before-done shape the reclaim loop
            # delivers, so the done short-circuit cannot mask the assertion.
            await h.async_redis.delete(h.config.done_key(resume.event_id))
            await h.kernel.process_event(resume)

            # Nothing further happened to the card: exactly one stamp across both
            # passes, and the ref stays absent.
            assert len(h.sink.card_updates) == 1
            assert not await h.async_redis.exists(h.config.approval_card_key("appr-1"))

    asyncio.run(go())


def test_another_approval_id_cannot_read_or_consume_this_cards_ref(
    make_harness,
) -> None:
    """#1207: approval identity is the key, not a tag inside a thread entry.

    THE MUTATION THIS CATCHES: keying by thread lets appr 99 read and consume
    appr 1 ref.
    """

    async def go() -> None:
        thread = "th-approval-id-isolation"
        async with make_harness(approvals=RecordingApprovals()) as h:
            await _pause_awaiting_approval(h, thread)
            own_key = h.config.approval_card_key("appr-1")
            wrong_key = h.config.approval_card_key("appr-99")
            old_thread_key = h.config.approval_card_key(thread)

            original_raw = await h.async_redis.get(own_key)
            assert original_raw is not None
            assert not await h.async_redis.exists(old_thread_key)
            assert not await h.async_redis.exists(wrong_key)
            assert "approval_id" not in json.loads(original_raw)

            h.runner.default_script = [Final(text="Ignored another expiry.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval expired] not approved in time",
                    thread=thread,
                    approval_id="appr-99",
                    author="system",
                )
            )

            assert h.sink.card_updates == []
            assert await h.async_redis.get(own_key) == original_raw
            assert not await h.async_redis.exists(wrong_key)

            h.runner.default_script = [Final(text="Acknowledged the expiry.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval expired] not approved in time",
                    thread=thread,
                    approval_id="appr-1",
                    author="system",
                )
            )

            assert len(h.sink.card_updates) == 1
            _channel, ts, message, _endpoint, settled = h.sink.card_updates[0]
            assert ts == "posted-1"
            assert message.text == "Refund order 42"
            assert settled is not None and settled.decision is None
            assert not await h.async_redis.exists(own_key)
            assert not await h.async_redis.exists(wrong_key)

    asyncio.run(go())


def test_the_remembered_approval_id_matches_the_resume_events_approval_id() -> None:
    """#1207: the writer and resume parser must agree on the key string.

    The key is a raw string across the api and worker seam. The worker
    remembers ``str(created.id)`` at the pause; the resuming id is the middle of
    the API's ``resume_event_id(approval.id)``. Every other test in this file
    fakes BOTH sides (``RecordingApprovals`` mints ``appr-<n>`` and
    ``_resume_turn`` interpolates whatever string it is handed), so a drift in
    either representation would make every resume look under the wrong key.

    This drives a real ``uuid.UUID`` through the API's builder and the worker's
    parser, which is why it needs no harness: the seam IS the unit.

    THE MUTATION THIS CATCHES: either side changing its id formatting -- e.g.
    ``resumequeue.resume_event_id`` emitting ``approval_id.hex``, or the
    worker's parser normalizing the middle it recovers.
    """

    from curie_api.resumequeue import resume_event_id
    from curie_worker.kernel import _approval_id_from_resume_event

    approval_id = uuid.UUID("6f1c8b3e-9c2a-4f5d-8a71-2b3c4d5e6f70")
    assert _approval_id_from_resume_event(resume_event_id(approval_id)) == str(approval_id)
    # Not a property of this one literal: a freshly minted id round-trips too.
    minted = uuid.uuid4()
    assert _approval_id_from_resume_event(resume_event_id(minted)) == str(minted)


def test_a_resolve_with_no_reader_leaves_the_ref_and_stamps_nothing(
    make_harness,
) -> None:
    """#1199: a PERMANENTLY unstampable resolve pass leaves the ref behind.

    "No reader configured" can never recover within this deployment, yet the
    #1199 ordering treats it like a transient blip: the record read returns
    nothing, so the card read is never reached and the ref survives to its TTL.
    This deserves its own test because it was pinned only as a side assertion on
    ``test_resolve_authored_by_a_system_named_actor_does_not_expire_the_card``,
    whose real subject is the expiry versus resolve discriminator, so a change to
    this ordering would have surfaced as a failure in a test named after
    something else entirely.

    Reading or consuming the ref before the record read makes this assertion
    fail because a pass that stamped nothing destroys the ref.
    """

    async def go() -> None:
        thread = "th-permanent-no-reader"
        async with make_harness(approvals=RecordingApprovals()) as h:  # no reader wired
            await _pause_awaiting_approval(h, thread)

            h.runner.default_script = [Final(text="Refunded.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval resolved] approved by U9",
                    thread=thread,
                    approval_id="appr-1",
                    author="U9",
                )
            )

            # Nothing was stamped: with no reader there is no verdict to state,
            # and guessing one is worse than a card that still shows buttons.
            assert h.sink.card_updates == []
            # The ref survives because no stamp was attempted.
            assert await h.async_redis.exists(h.config.approval_card_key("appr-1"))
            # The run continued regardless: the stamp is an enrichment.
            assert h.sink.updates, "the resume must still produce a reply"

    asyncio.run(go())


def test_post_success_consume_does_not_delete_a_replaced_same_approval_ref(
    make_harness,
) -> None:
    """#1206: consume removes only the exact raw value that was delivered.

    THE MUTATION THIS CATCHES: an unconditional delete removes the replacement
    even though its raw value differs.
    """

    async def go() -> None:
        async with make_harness() as h:
            store = h.card_store
            await store.remember(
                "appr-1",
                channel="C-alpha",
                ts="ts-alpha",
                summary="Refund order 42",
                endpoint=None,
                requested_by="U1",
            )
            first = await store.read("appr-1")
            assert first is not None
            first_ref, first_raw = first
            assert (first_ref.channel, first_ref.ts) == ("C-alpha", "ts-alpha")

            await store.remember(
                "appr-1",
                channel="C-beta",
                ts="ts-beta",
                summary="Delete the prod bucket",
                endpoint=None,
                requested_by="U2",
            )

            assert await store.consume("appr-1", first_raw) is False

            replacement_entry = await store.read("appr-1")
            assert replacement_entry is not None
            replacement_ref, replacement_raw = replacement_entry
            assert replacement_raw != first_raw
            assert (replacement_ref.channel, replacement_ref.ts) == (
                "C-beta",
                "ts-beta",
            )
            assert replacement_ref.summary == "Delete the prod bucket"
            assert replacement_ref.requested_by == "U2"

    asyncio.run(go())


def test_remember_refuses_to_write_an_empty_approval_id(make_harness) -> None:
    """#1207: an empty id must not collapse writes onto the shared bare key.

    THE MUTATION THIS CATCHES: removing the empty id guard writes the
    degenerate key.
    """

    async def go() -> None:
        async with make_harness() as h:
            with pytest.raises(ValueError, match="approval_id"):
                await h.card_store.remember(
                    "",
                    channel="C1",
                    ts="posted-1",
                    summary="Refund order 42",
                    endpoint=None,
                    requested_by="U1",
                )

            degenerate_key = h.config.approval_card_key("")
            assert not await h.async_redis.exists(degenerate_key)
            assert await h.card_store.read("") is None

    asyncio.run(go())


# --- Best-effort resume reply when the CLI stub endpoint is dead (#708) --------


def test_resume_reply_best_effort_completes_offline_when_endpoint_is_dead(
    make_harness,
) -> None:
    """AC-708-1/2 (#708, PRIMARY): a resolved approval's resume turn whose per-turn
    reply endpoint is the now-dead CLI stub, delivered on a worker with NO distinct
    default transport (the pure-offline local loop), must still COMPLETE -- the
    granted tool executes exactly once and the turn reaches terminal ACK -- instead
    of dead-lettering because the reply cannot be delivered.

    Today the reply-delivery ``update`` raises the aiohttp transport error
    ``_with_transport_fallback`` re-raises when there is no distinct default (#530
    only rescues the has-default case), so ``process_event`` escapes; the consumer
    then leaves the entry pending, redelivers it to the delivery cap, and
    dead-letters it (#505) -- a full re-run per redelivery and the resolved approval
    never completes. The done marker is the proof it did NOT: it is written only
    once the turn is terminally handled (the consumer then acks it).

    The fix makes a resume turn's reply best-effort: the kernel gates the new
    ``best_effort_unreachable`` flag on ``_is_approval_resume(event_id)`` for the
    reply-delivery ``update`` calls (streaming edits + final reply). The resume
    ``event_id`` shape (``approval-<uuid>-resolved``) is authored by
    ``resumequeue.resume_event_id`` -- the format authority the worker recognizer
    keys off across the api/worker seam.
    """

    async def go() -> None:
        from curie_api.resumequeue import resume_event_id

        grant_event = resume_event_id(uuid.uuid4())
        binding = GrantBinding(grant_event_id=grant_event, grant_tool="mcp__github__create_issue")
        async with make_harness(binding=binding) as h:
            # Offline local loop: the reply endpoint (the CLI stub) is dead, and the
            # worker sink has NO distinct default transport to fall back to.
            h.sink.dead_endpoints.add(_CLI_STUB)

            # The granted tool runs to done in the runner during the resume turn.
            h.runner.default_script = [Final(text="Issue created.", status=DONE)]
            resume_turn = QueuedTurn(
                event_id=grant_event,
                conversation_id="th-offline-resume",
                author="U9",
                text="[approval resolved] approved by U9",
                reply_handle=ReplyHandle(
                    kind="slack", channel="C1", placeholder="p-1", endpoint=_CLI_STUB
                ),
                received_at="2026-07-14T00:00:00+00:00",
            )

            # Must NOT raise: a dead reply endpoint on a resume turn no longer
            # dead-letters the resolved approval.
            await h.kernel.process_event(resume_turn)

            # Terminal ACK, not dead-letter.
            assert await h.async_redis.exists(h.config.done_key(grant_event))

            # The granted tool executed exactly once: the resume turn opened a
            # single runner turn, and that claim carried the one-shot #430 grant.
            assert h.runner.opened == ["[approval resolved] approved by U9"]
            resumed_env = h.fake_k8s.claim_envs[-1]
            assert resumed_env is not None
            assert resumed_env.get("CURIE_APPROVAL_GRANT_TOOL") == "mcp__github__create_issue"

    asyncio.run(go())


def test_normal_turn_reply_stays_loud_when_endpoint_is_dead(make_harness) -> None:
    """AC-708-4 (#708): the best-effort swallow is scoped to resume turns. A NORMAL
    (non ``approval-<uuid>-resolved``) turn hitting the same dead endpoint + no
    distinct default must STILL fail loudly -- a fresh local turn whose stub crashed
    mid-turn is a genuine failure that must surface, not silently complete. Extends
    ``test_no_fallback_when_no_default_is_configured``'s intent to the kernel.

    The transport error propagates out of ``process_event`` (leaving the entry
    pending for reclaim), so the turn is NOT marked done -- the inverse of the
    resume case above."""

    async def go() -> None:
        async with make_harness() as h:  # no binding -> a plain, non-resume turn
            h.sink.dead_endpoints.add(_CLI_STUB)
            h.runner.default_script = [Final(text="done", status=DONE)]
            ev = _qevent(
                "hello",
                thread="th-normal-dead",
                event_id="ev-normal-1",  # not the resume shape
                endpoint=_CLI_STUB,
            )

            with pytest.raises(aiohttp.ClientError):
                await h.kernel.process_event(ev)

            # Not silently completed: no done marker was written.
            assert not await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_a_legacy_thread_keyed_card_is_settled_after_the_boot_migration(
    make_harness,
) -> None:
    """#1751: the stranding a worker roll used to create is gone.

    #1723 rekeyed the card pointer from the thread to the approval id with no
    dual read, so an approval that was ALREADY PENDING when the workers rolled
    left its ref under the old thread key where ``_finalize_settled_card``
    could never see it. A resolve click still healed such a card from its
    interaction payload; an EXPIRY has no click, so the buttons stayed live for
    the rest of the 14 day TTL.

    This is the end-to-end shape of the fix: seed the pre-#1723 entry exactly
    as the old worker wrote it (thread-keyed, ``approval_id`` in the payload),
    run the one-shot boot migration, then drive the expiry resume turn the #412
    sweeper enqueues and assert the card is actually settled in place.
    """

    async def go() -> None:
        thread = "th-legacy-roll"
        async with make_harness(approvals=RecordingApprovals()) as h:
            # Written by the PREVIOUS worker version: keyed by thread, with the
            # approval id inline (#1199) -- the only reason it is recoverable.
            legacy_key = f"{h.config.key_prefix}:approval-card:{thread}"
            await h.async_redis.set(
                legacy_key,
                json.dumps(
                    {
                        "channel": "C1",
                        "ts": "posted-legacy",
                        "summary": "Give ACME a 20% discount",
                        "endpoint": None,
                        "requested_by": "U1",
                        "kind": "slack",
                        "adapter": None,
                        "approval_id": "appr-1",
                    }
                ),
                ex=3600,
            )
            # Boot: the one-shot pass ``run._run`` makes before any consumer reads.
            assert (await h.card_store.migrate_legacy_thread_keyed_refs()).migrated == 1
            assert not await h.async_redis.exists(legacy_key)

            h.runner.default_script = [Final(text="Acknowledged the expiry.", status=DONE)]
            await h.kernel.process_event(
                _resume_turn(
                    "[approval expired] not approved in time",
                    thread=thread,
                    approval_id="appr-1",
                    author="system",
                )
            )

            # The buttons are gone: the card was edited in place at the address
            # the LEGACY entry remembered, with its remembered summary.
            assert len(h.sink.card_updates) == 1
            channel, ts, message, endpoint, settled = h.sink.card_updates[0]
            assert (channel, ts) == ("C1", "posted-legacy")
            assert endpoint is None
            assert message.text == "Give ACME a 20% discount"
            assert settled is not None and settled.decision is None
            assert settled.requested_by == "U1"

            # And the migrated memory was consumed, so a redelivery no-ops.
            assert not await h.async_redis.exists(h.config.approval_card_key("appr-1"))

    asyncio.run(go())


def test_publication_with_bound_route_is_created_and_does_not_escalate_unexpected_route(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from curie_worker.approvals import CreatedPublication

    deployment_id = uuid.UUID("11111111-1111-4111-8111-111111112705")

    class Binding(GrantBinding):
        async def resolve(self, kind: str, adapter: str | None, channel: str):  # noqa: ANN201
            from curie_worker.binding import ResolvedDeployment

            return ResolvedDeployment(
                agent_id=self.agent_id,
                agent_name="acme-bot",
                deployment_id=deployment_id,
                workspace_enabled=True,
                version_id=uuid.uuid4(),
                version_label="v1",
                bundle_ref=None,
                max_usd_per_day=None,
                max_output_tokens_per_run=None,
                approval_routes={
                    "managers": {
                        **_notification_route(),
                        "approvers": {"users": ["U0EXAMPLE1"]},
                    }
                },
            )

    class PublicationApi:
        def __init__(self) -> None:
            self.creates: list[PublicationCreateRequest] = []

        async def get_publication_lineage(
            self, requested_deployment: uuid.UUID, conversation: str, repo: str
        ) -> None:
            return None

        async def create_publication(
            self, request: PublicationCreateRequest, *, budget_s: float = 120
        ) -> CreatedPublication:
            self.creates.append(request)
            return CreatedPublication(
                id="publication-example",
                approval_id="approval-example",
                status="pending",
            )

    class Workspace:
        def __init__(self) -> None:
            self.substrate = None

        def select_repository(self, **kwargs: object) -> str:
            return "acme-corp/acme-private"

        def claim_or_resume_with_handle(self, **kwargs: object) -> object:
            assert self.substrate is not None
            return SimpleNamespace(
                handle=self.substrate.claim(
                    str(kwargs["thread_key"]),
                    env=kwargs["env"],
                    agent_name=kwargs["agent_name"],
                    workspace_repo=kwargs["repo_full_name"],
                )
            )

        def release(self, _thread_identity: str) -> None:
            return None

        def touch(self, _thread_identity: str, *, ttl_seconds: int) -> None:
            del ttl_seconds

    async def go() -> None:
        async with _publication_run(
            make_harness,
            monkeypatch,
            deployment_id=uuid.UUID("11111111-1111-4111-8111-111111112705"),
            thread="1700000000.002705",
            approval_routes={
                "managers": {
                    **_notification_route(),
                    "approvers": {"users": ["U0EXAMPLE1"]},
                }
            },
            approval_route="managers",
        ) as run:
            h, publication_api = run.h, run.publication_api
            assert publication_api.creates, h.sink.last_text
            assert getattr(publication_api.creates[0], "route", None) == "managers"
            assert h.sink.last_text is not None
            assert "unexpected approval route" not in h.sink.last_text
            assert h.sink.posts == []
            assert [
                event
                for event, _route, _best_effort in h.sink.events
                if isinstance(event, ReplyPost) and event.message.interaction is None
            ] == []

    asyncio.run(go())


def test_publication_with_named_unbound_route_escalates_and_creates_nothing(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from curie_worker.approvals import CreatedPublication

    deployment_id = uuid.UUID("22222222-2222-4222-8222-222222222705")

    class Binding(GrantBinding):
        async def resolve(self, kind: str, adapter: str | None, channel: str):  # noqa: ANN201
            from curie_worker.binding import ResolvedDeployment

            return ResolvedDeployment(
                agent_id=self.agent_id,
                agent_name="acme-bot",
                deployment_id=deployment_id,
                workspace_enabled=True,
                version_id=uuid.uuid4(),
                version_label="v1",
                bundle_ref=None,
                max_usd_per_day=None,
                max_output_tokens_per_run=None,
                approval_routes={"operators": _resolution_route()},
            )

    class PublicationApi:
        def __init__(self) -> None:
            self.creates: list[PublicationCreateRequest] = []

        async def get_publication_lineage(
            self, requested_deployment: uuid.UUID, conversation: str, repo: str
        ) -> None:
            return None

        async def create_publication(
            self, request: PublicationCreateRequest, *, budget_s: float = 120
        ) -> CreatedPublication:
            self.creates.append(request)
            return CreatedPublication(
                id="publication-example",
                approval_id="approval-example",
                status="pending",
            )

    class Workspace:
        def __init__(self) -> None:
            self.substrate = None

        def select_repository(self, **kwargs: object) -> str:
            return "acme-corp/acme-private"

        def claim_or_resume_with_handle(self, **kwargs: object) -> object:
            assert self.substrate is not None
            return SimpleNamespace(
                handle=self.substrate.claim(
                    str(kwargs["thread_key"]),
                    env=kwargs["env"],
                    agent_name=kwargs["agent_name"],
                    workspace_repo=kwargs["repo_full_name"],
                )
            )

        def release(self, _thread_identity: str) -> None:
            return None

        def touch(self, _thread_identity: str, *, ttl_seconds: int) -> None:
            del ttl_seconds

    async def go() -> None:
        async with _publication_run(
            make_harness,
            monkeypatch,
            deployment_id=uuid.UUID("22222222-2222-4222-8222-222222222705"),
            thread="1700000000.002706",
            approval_routes={"operators": _resolution_route()},
            approval_route="managers",
        ) as run:
            h = run.h
            assert run.publication_api.creates == []
            assert h.sink.last_text is not None
            assert "route is not bound" in h.sink.last_text
            assert "unexpected approval route" not in h.sink.last_text

    asyncio.run(go())


# --- Approval resume must not re-derive a repository from its own text (#2828) --


class _WorkspacelessBinding:
    """A deployment with repository workspaces off, as `curie cluster message`
    installs it: a deployment id (so selection runs) and no bound repository."""

    def __init__(self, deployment_id: uuid.UUID) -> None:
        self.deployment_id = deployment_id
        self.grant_reads: list[str] = []

    async def approval_grant_tool(self, event_id: str, _agent_id: uuid.UUID) -> str | None:
        # The resolver's answer once an operator principal has approved the
        # persisted row: only that approval's resume event carries the one-shot
        # grant for the gated tool (binding.approval_grant_tool, ADR-0035).
        self.grant_reads.append(event_id)
        if event_id == _RESUME_EVENT_ID:
            return _GATED_TOOL
        return None

    async def approval_grant_arguments(
        self, event_id: str, _agent_id: uuid.UUID
    ) -> dict[str, str] | None:
        if event_id == _RESUME_EVENT_ID:
            return {"command": "printf ok"}
        return None

    async def resolve(self, _kind: str, _adapter: str | None, _channel: str) -> object:
        return SimpleNamespace(
            agent_id=uuid.UUID("22222222-2222-4222-8222-222222222828"),
            agent_name="sre-bot",
            deployment_id=self.deployment_id,
            workspace_enabled=False,
            endpoint=None,
            adapter=None,
        )

    def boot_env(
        self,
        _resolved: object,
        _thread_key: str,
        *,
        kind: str | None = None,
        address: str | None = None,
        **_: object,
    ) -> dict[str, str]:
        return {}

    def packs_for(self, _resolved: object) -> BehaviorPacks:
        return BehaviorPacks()


_GATED_TOOL = "mcp__kubernetes__resources_delete"
_RESUME_APPROVAL_ID = uuid.UUID("a1057a26-0000-4000-8000-000000002828")
_RESUME_EVENT_ID = f"approval-{_RESUME_APPROVAL_ID}-resolved"

_ALLOWLIST_REFUSAL = (
    "That repository is not in api.githubRepoAllowlist for this installation; "
    "allow `owner/repo` or `owner/*` in the chart values."
)


class _ApiShapedWorkspace:
    """The worker coordinator against the real API contract: a null repository
    with no selection is a 200 with nothing selected, a sticky selection is
    returned for a null request, and any named repository outside the
    allowlist (or different from the sticky one) is refused."""

    def __init__(self, *, selected: str | None = None) -> None:
        self.selected = selected
        self.requested: list[str | None] = []

    def select_repository(
        self,
        *,
        thread_key: str,
        deployment_id: uuid.UUID,
        author: str,
        repo_full_name: str | None,
    ) -> str | None:
        self.requested.append(repo_full_name)
        if repo_full_name is None:
            return self.selected
        if self.selected is not None and repo_full_name != self.selected:
            raise WorkspaceSelectionRefused(
                "This thread is already bound to a different repository."
            )
        raise WorkspaceSelectionRefused(_ALLOWLIST_REFUSAL)

    def touch(self, thread_key: str, *, ttl_seconds: int) -> bool:
        return True


def _kubernetes_resume_turn(thread: str, *, placeholder: str | None) -> QueuedTurn:
    """The resume the API really enqueues for the #2828 repro: its text embeds
    the gated tool's arguments, and ``apiVersion: batch/v1`` is shaped exactly
    like a bare ``owner/repo``."""

    from curie_api.resumequeue import build_resume_turn

    summary = (
        "Tool call awaiting approval: mcp__kubernetes__resources_delete "
        '{"apiVersion": "batch/v1", "kind": "Job", '
        '"name": "curie-preflight-gvisor", "namespace": "curie"}'
    )
    turn = build_resume_turn(
        SimpleNamespace(  # type: ignore[arg-type]
            id=_RESUME_APPROVAL_ID,
            conversation_id=thread,
            status="approved",
            summary=summary,
            resolved_by="U0ACC2570OP",
            resolution_note=None,
            reply_kind="slack",
            reply_channel="C0LOCALDEV",
            reply_placeholder=placeholder,
            reply_endpoint=None,
            reply_adapter=None,
        )
    )
    assert "batch/v1" in turn.text
    assert turn.event_id == _RESUME_EVENT_ID
    return turn


@pytest.mark.parametrize("coordinator", ["api", "off"])
def test_approval_resume_with_slash_argument_runs_the_approved_turn_once(
    make_harness,
    coordinator: str,
) -> None:
    """#2828: an operator-approved gate raised by a cluster-message turn on an
    install with workspaces off must resume into the runner exactly once. The
    resume text carries ``batch/v1``; it must never become a repository
    selection (403 from the API, or the workspaces-off refusal)."""

    async def go() -> None:
        approvals = RecordingApprovals()
        binding = _WorkspacelessBinding(uuid.UUID("77777777-7777-4777-8777-000000002828"))
        async with make_harness(binding=binding, approvals=approvals) as h:
            probe = _ApiShapedWorkspace() if coordinator == "api" else None
            h.kernel._workspace = probe  # type: ignore[assignment]
            thread = f"th_2828_{coordinator}"
            h.runner.default_script = _awaiting_script(
                "mcp__kubernetes__resources_delete batch/v1 Job"
            )
            ask = "Delete the failed Job curie-preflight-gvisor in namespace curie."
            await h.kernel.process_event(_qevent(ask, thread=thread, channel="C0LOCALDEV"))
            assert approvals.create_calls == 1
            assert h.runner.opened == [ask]

            h.runner.default_script = [
                Final(text="Deleted Job curie-preflight-gvisor.", status=DONE)
            ]
            resume = _kubernetes_resume_turn(thread, placeholder="p-1")
            await h.kernel.process_event(resume)
            # Redelivery of the same resume must not run the tool again.
            await h.kernel.process_event(resume)

            assert h.runner.opened == [ask, resume.text]
            # The approved tool reaches the runner exactly once: one claim carries
            # the one-shot grant, and redelivery claims nothing further.
            grant_claims = [env for env in h.fake_k8s.claim_envs if (env or {}).get(GRANT_TOOL_ENV)]
            assert len(grant_claims) == 1
            assert grant_claims[0][GRANT_TOOL_ENV] == _GATED_TOOL
            assert _RESUME_EVENT_ID in binding.grant_reads
            assert h.sink.last_text == "Deleted Job curie-preflight-gvisor."
            assert await h.async_redis.exists(h.config.done_key(resume.event_id))
            if probe is not None:
                assert "batch/v1" not in probe.requested
                assert all(repo is None for repo in probe.requested)

    asyncio.run(go())


def test_genuine_repository_message_on_workspaceless_install_is_still_refused(
    make_harness,
) -> None:
    """#2683 is untouched: a PERSON naming a repository on this install is
    still refused before any claim or model turn."""

    async def go() -> None:
        binding = _WorkspacelessBinding(uuid.UUID("77777777-7777-4777-8777-000000002829"))
        async with make_harness(binding=binding) as h:
            probe = _ApiShapedWorkspace()
            h.kernel._workspace = probe  # type: ignore[assignment]
            await h.kernel.process_event(
                _qevent("Change acme-corp/acme-bot", thread="th_2828_person")
            )

            assert probe.requested == ["acme-corp/acme-bot"]
            assert h.runner.opened == []
            assert h.sink.last_text == _ALLOWLIST_REFUSAL

    asyncio.run(go())


_CLUSTER_MESSAGE_REF = "123e4567-e89b-42d3-a456-426614174883"


@pytest.mark.parametrize(
    ("adapter", "placeholder", "expected_ref"),
    [
        # #2883: the relay addresses the caller's session bucket by the turn's
        # ref, so a tool-approval card must carry it or the adapter refuses it.
        ("curie-cluster-message", _CLUSTER_MESSAGE_REF, _CLUSTER_MESSAGE_REF),
        # Every other transport posts a fresh card message: no ref, or the
        # post would be read as an edit of the turn's own reply.
        (None, "p-1", None),
    ],
)
def test_tool_approval_card_reaches_the_cluster_message_caller(
    make_harness,
    caplog: pytest.LogCaptureFixture,
    adapter: str | None,
    placeholder: str,
    expected_ref: str | None,
) -> None:
    """#2883: a non-publication approval raised by a ``cluster message`` turn
    delivers its card into the caller's relay bucket, like #2757 did for
    publication cards, instead of failing with ``reply_ref is required``."""

    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting_script(
                "mcp__kubernetes__resources_scale apps/v1 Deployment"
            )
            with caplog.at_level(logging.WARNING, logger="curie_worker.kernel"):
                await h.kernel.process_event(
                    _qevent(
                        "scale it down",
                        thread="th-2883",
                        placeholder=placeholder,
                        adapter=adapter,
                    )
                )

            assert approvals.create_calls == 1
            cards = [
                (event, route)
                for event, route, _ in h.sink.events
                if isinstance(event, ReplyPost)
                and isinstance(event.message.interaction, ConfirmIntent)
            ]
            assert len(cards) == 1
            card, route = cards[0]
            assert card.target.reply_ref == expected_ref
            assert route.adapter == adapter
            assert card.message.interaction.id == "appr-1"
            assert "approval card post failed" not in caplog.text
            if expected_ref is not None:
                # The real relay adapter addresses this card to the caller's
                # bucket instead of refusing it (the #2883 log line).
                relay = _ClusterMessageReplyAdapter("http://api.example.test:8000", "worker-token")
                endpoint, ref = relay._endpoint_for(card.target.reply_ref)
                assert ref == expected_ref
                assert endpoint.endswith(f"/cluster-message-replies/{expected_ref}")
                # The CLI waiter prints reply.update text, so the caller also
                # sees the awaiting-approval notice in that same bucket.
                assert any(
                    ref == expected_ref and "Awaiting approval (appr-1)" in text
                    for _, ref, text in h.sink.updates
                )

    asyncio.run(go())


# --- ADR-0177: a route may show its card where the request was asked ----------

_REQUESTING_SURFACE = {"resolution": {"mode": "requesting_surface"}}
# ADR-0177 amendment: an email card needs a route that lists approver emails, or the
# approval escalates when raised.
_LISTED_EMAIL_ROUTE = {**_REQUESTING_SURFACE, "approvers": {"emails": ["approver@example.com"]}}
_MAIL_ENDPOINT = "http://curie-mail-adapter:8080/"
_MAIL_ADAPTER = "agentmail-sandbox"
_MAIL_INBOX = "bot@example.com"


def _email_qevent(text: str, *, thread: str) -> QueuedTurn:
    return _qevent(
        text,
        thread=thread,
        kind="email",
        channel=_MAIL_INBOX,
        endpoint=_MAIL_ENDPOINT,
        adapter=_MAIL_ADAPTER,
        placeholder=None,
    )


def test_a_requesting_surface_route_shows_the_card_in_the_email_thread_that_asked(
    make_harness,
) -> None:
    """ADR-0177 decision 1: the card joins the conversation that asked, over
    that conversation's own transport, and the record says so, which is what
    the API's served check and approver email list read back (ADR-0177 amendment)."""

    async def go() -> None:
        approvals = RecordingApprovals()
        binding = RoutedBinding({"confirm": _LISTED_EMAIL_ROUTE})
        async with make_harness(approvals=approvals, binding=binding) as h:
            h.runner.default_script = _awaiting_routed_script("Send the quote", "confirm")
            await h.kernel.process_event(_email_qevent("send it", thread="th-mail"))

            req = approvals.requests[0]
            assert req.route == "confirm"
            assert (req.reply_kind, req.reply_channel) == ("email", _MAIL_INBOX)
            assert req.card_channel == _MAIL_INBOX

            # One card, in the asking thread, carrying the Approve/Reject intent.
            assert len(h.sink.posts) == 1
            address, message, requested_by, conversation_id, endpoint = h.sink.posts[0]
            assert (address, conversation_id, endpoint) == (_MAIL_INBOX, "th-mail", _MAIL_ENDPOINT)
            assert requested_by == "U1"
            assert isinstance(message.interaction, ConfirmIntent)
            assert message.interaction.id == "appr-1"
            (card_event, card_route, _) = next(
                entry for entry in h.sink.events if entry[0].event == "reply.post"
            )
            assert card_event.target.kind == "email"
            assert card_route.adapter == _MAIL_ADAPTER

            # The card ref is remembered with the email destination, so the
            # resume settles THIS card (decision 6).
            h.runner.default_script = [Final(text="Sent.", status=DONE)]
            await h.kernel.process_event(
                QueuedTurn(
                    event_id="approval-appr-1-resolved",
                    conversation_id="th-mail",
                    author="requester@example.com",
                    text="[approval resolved] approved",
                    reply_handle=ReplyHandle(
                        kind="email",
                        channel=_MAIL_INBOX,
                        placeholder=None,
                        endpoint=_MAIL_ENDPOINT,
                        adapter=_MAIL_ADAPTER,
                    ),
                    received_at="2026-07-14T00:00:00+00:00",
                )
            )
            assert len(h.sink.card_updates) == 0  # no reader: no verdict to stamp

    asyncio.run(go())


def test_an_email_card_names_every_listed_approver_and_a_slack_card_names_none(
    make_harness,
) -> None:
    """ADR-0177 amendment A5: the email card carries the route's listed addresses,
    so the mail adapter can tell the requester who can approve. Every address,
    lowercased, once, in list order. A Slack card on a route that also lists
    emails carries none: an address means nothing on Slack."""

    async def go() -> None:
        route = {
            **_REQUESTING_SURFACE,
            "approvers": {
                "emails": [
                    "approver@example.com",
                    "Second.Approver@Example.com",
                    "approver@example.com",
                ],
                "users": ["U0EXAMPLE1"],
            },
        }
        binding = RoutedBinding({"confirm": route})
        async with make_harness(approvals=RecordingApprovals(), binding=binding) as h:
            h.runner.default_script = _awaiting_routed_script("Send the quote", "confirm")
            await h.kernel.process_event(_email_qevent("send it", thread="th-mail-names"))
            (_address, message, _by, _conversation, _endpoint) = h.sink.posts[0]
            assert [(f.label, f.value) for f in message.fields] == [
                ("Approver", "approver@example.com"),
                ("Approver", "second.approver@example.com"),
            ]

        async with make_harness(approvals=RecordingApprovals(), binding=binding) as h:
            h.runner.default_script = _awaiting_routed_script("Send the quote", "confirm")
            await h.kernel.process_event(_qevent("send it", thread="th-slack-names"))
            (address, message, _by, _conversation, _endpoint) = h.sink.posts[0]
            assert address == "C1"
            assert message.fields == []

    asyncio.run(go())


def test_a_settled_email_card_is_sent_to_the_thread_with_its_outcome(make_harness) -> None:
    """ADR-0177 decision 6: whatever ends the approval, the resume settles the
    one card. For email the adapter turns the settled update into a follow-up,
    so the update must reach the email thread, over its transport, with the
    outcome read from the record."""

    async def go() -> None:
        reader = RecordingReader(_APPROVED)
        binding = RoutedBinding({"confirm": _LISTED_EMAIL_ROUTE})
        async with make_harness(
            approvals=RecordingApprovals(), approval_reader=reader, binding=binding
        ) as h:
            h.runner.default_script = _awaiting_routed_script("Send the quote", "confirm")
            await h.kernel.process_event(_email_qevent("send it", thread="th-mail-settle"))
            assert len(h.sink.posts) == 1
            reader.resolve("appr-1")

            h.runner.default_script = [Final(text="Sent.", status=DONE)]
            await h.kernel.process_event(
                QueuedTurn(
                    event_id="approval-appr-1-resolved",
                    conversation_id="th-mail-settle",
                    author="U9",
                    text="[approval resolved] approved by U9",
                    reply_handle=ReplyHandle(
                        kind="email",
                        channel=_MAIL_INBOX,
                        placeholder=None,
                        endpoint=_MAIL_ENDPOINT,
                        adapter=_MAIL_ADAPTER,
                    ),
                    received_at="2026-07-14T00:00:00+00:00",
                )
            )

            assert len(h.sink.card_updates) == 1
            address, ref, _message, endpoint, settled = h.sink.card_updates[0]
            assert (address, ref, endpoint) == (_MAIL_INBOX, "posted-1", _MAIL_ENDPOINT)
            assert settled is not None and settled.decision == "approved"
            update = next(
                event
                for event, _route, _ in h.sink.events
                if event.event == "reply.update" and getattr(event, "settled", None) is not None
            )
            assert update.target.kind == "email"
            assert update.target.conversation_id == "th-mail-settle"

    asyncio.run(go())


def test_a_requesting_surface_route_asked_in_slack_joins_the_slack_thread(
    make_harness,
) -> None:
    async def go() -> None:
        approvals = RecordingApprovals()
        binding = RoutedBinding({"confirm": _REQUESTING_SURFACE})
        async with make_harness(approvals=approvals, binding=binding) as h:
            h.runner.default_script = _awaiting_routed_script("Refund order 42", "confirm")
            await h.kernel.process_event(_qevent("refund?", thread="th-slack-mode"))

            req = approvals.requests[0]
            assert (req.route, req.card_channel) == ("confirm", "C1")
            address, _message, _by, conversation_id, _endpoint = h.sink.posts[0]
            assert (address, conversation_id) == ("C1", "th-slack-mode")

    asyncio.run(go())


@pytest.mark.parametrize(
    ("route", "routes"),
    [
        # Routeless: no binding, so no list (ADR-0177 amendment A3).
        (None, None),
        # The mode alone lists nobody; the requester is not a default.
        ("confirm", {"confirm": _REQUESTING_SURFACE}),
        # Slack users nobody on an email thread can prove to be (ADR-0177).
        ("confirm", {"confirm": {**_REQUESTING_SURFACE, "approvers": {"users": ["U0EXAMPLE1"]}}}),
        # An empty list written around the API, and a malformed one.
        ("confirm", {"confirm": {**_REQUESTING_SURFACE, "approvers": {"emails": []}}}),
        ("confirm", {"confirm": {**_REQUESTING_SURFACE, "approvers": {"emails": "a@example.com"}}}),
    ],
)
def test_an_email_approval_nobody_is_listed_for_escalates_at_raise_time(
    make_harness, route: str | None, routes: dict | None
) -> None:
    """ADR-0177 amendment A3: on email only an address on the route's approver
    list may answer. Rather than create an approval nobody there can answer,
    the turn escalates and says why."""

    async def go() -> None:
        approvals = RecordingApprovals()
        binding = RoutedBinding(routes)
        async with make_harness(approvals=approvals, binding=binding) as h:
            h.runner.default_script = (
                _awaiting_script("Send the quote")
                if route is None
                else _awaiting_routed_script("Send the quote", route)
            )
            ev = _email_qevent("send it", thread=f"th-mail-nobody-{route}")
            await h.kernel.process_event(ev)

            assert approvals.requests == []
            assert h.sink.posts == []
            assert h.sink.last_text is not None
            assert "approval-no-email-approvers" in h.sink.last_text
            assert "approver list" in h.sink.last_text
            if route is not None:
                assert repr(route) in h.sink.last_text
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_slack_approvers_on_a_route_that_lands_on_another_channel_still_escalate(
    make_harness,
) -> None:
    """ADR-0177 decision 3 for a non-Slack channel that has no email list."""

    async def go() -> None:
        approvals = RecordingApprovals()
        binding = RoutedBinding(
            {"confirm": {**_REQUESTING_SURFACE, "approvers": {"users": ["U0EXAMPLE1"]}}}
        )
        async with make_harness(approvals=approvals, binding=binding) as h:
            h.runner.default_script = _awaiting_routed_script("Send the quote", "confirm")
            ev = _qevent(
                "send it",
                thread="th-webchat-approvers",
                kind="webchat",
                channel="chat-room-1",
                endpoint=_MAIL_ENDPOINT,
                adapter="webchat-adapter",
                placeholder=None,
            )
            await h.kernel.process_event(ev)

            assert approvals.requests == []
            assert h.sink.last_text is not None
            assert "approval-approvers-unverifiable" in h.sink.last_text

    asyncio.run(go())


def test_approvers_on_a_requesting_surface_route_asked_in_slack_still_apply(
    make_harness,
) -> None:
    async def go() -> None:
        approvals = RecordingApprovals()
        binding = RoutedBinding(
            {"confirm": {**_REQUESTING_SURFACE, "approvers": {"users": ["U0EXAMPLE1"]}}}
        )
        async with make_harness(approvals=approvals, binding=binding) as h:
            h.runner.default_script = _awaiting_routed_script("Refund order 42", "confirm")
            await h.kernel.process_event(_qevent("refund?", thread="th-slack-approvers"))

            assert [r.route for r in approvals.requests] == ["confirm"]
            assert len(h.sink.posts) == 1

    asyncio.run(go())


@pytest.mark.parametrize(
    "metadata,requester,known",
    [
        ({}, None, False),
        ({"requested_by": None}, None, True),
        ({"requested_by": "U0EXAMPLE1"}, "U0EXAMPLE1", True),
    ],
)
def test_approval_create_decoder_distinguishes_unavailable_from_older_api(
    metadata: dict, requester: str | None, known: bool
) -> None:
    async def go() -> None:
        def handle(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/approvals"
            return httpx.Response(201, json={"id": "appr-1", "status": "pending", **metadata})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            client = ApprovalClient(
                api_base_url="https://api.example.com",
                api_key="test-key",
                client=http,
                read_timeout_s=1.0,
            )
            created = await client.create(
                ApprovalRequest(
                    conversation_id="thread-example",
                    author="U0EXAMPLE2",
                    summary="Next action",
                    reply_kind="slack",
                    reply_channel="C0EXAMPLE1",
                    reply_placeholder=None,
                    dedupe_key="approval-11111111-1111-4111-8111-111111111111-resolved",
                )
            )
            assert created.requested_by == requester
            assert created.requester_known is known

    asyncio.run(go())


class DerivedRequesterApprovals(RecordingApprovals):
    def __init__(self, requester: str | None, *, known: bool = True) -> None:
        super().__init__()
        self.requester = requester
        self.known = known

    async def create(self, request: ApprovalRequest, *, budget_s: float = 120) -> CreatedApproval:
        created = await super().create(request)
        if not self.known:
            return created  # The real old API shape has no attribution metadata.
        return CreatedApproval(
            id=created.id,
            status=created.status,
            requested_by=self.requester,
            requester_known=self.known,
        )


def test_three_gate_requester_survives_restart_and_consumed_cards(make_harness) -> None:
    # The API fixture supplies independently tested durable attribution. Real
    # Valkey carries card identity; each harness replaces all in-memory state.
    async def go() -> None:
        approvals = DerivedRequesterApprovals("U0EXAMPLE1")
        thread = "th-requester-chain"
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting_script("First action")
            await h.kernel.process_event(
                _qevent("Do three actions", thread=thread).model_copy(
                    update={"author": "U0EXAMPLE1"}
                )
            )
            assert h.sink.posts[0][2] == "U0EXAMPLE1"
        for index, actor in enumerate(("U0EXAMPLE2", "U0EXAMPLE3"), 1):
            reader = RecordingReader(
                SettledApproval(
                    status="approved",
                    resolved_by=actor,
                    resolution_note="U0EXAMPLE4 requested it",
                    resolved_at=datetime(2026, 9, 30, 10, index, tzinfo=UTC),
                )
            )
            # Only the approval this resume answers is resolved. The one its
            # continuation raises next is still pending when its card lands.
            reader.resolve(f"appr-{index}")
            async with make_harness(approvals=approvals, approval_reader=reader) as h:
                h.runner.default_script = _awaiting_script(f"Next action {index}")
                await h.kernel.process_event(
                    _resume_turn(
                        "Untrusted display prose names U0EXAMPLE4",
                        thread=thread,
                        approval_id=f"appr-{index}",
                        author=actor,
                    )
                )
                assert approvals.requests[-1].author == actor
                assert h.sink.posts[-1][2] == "U0EXAMPLE1"
                assert len(h.sink.card_updates) == 1
                settled = h.sink.card_updates[0][-1]
                assert settled is not None
                assert settled.requested_by == "U0EXAMPLE1"
                assert settled.resolver == actor
                from curie_worker.approvals import decided_at

                assert decided_at(h.sink.card_updates[0][2]) == reader.records[0].resolved_at
                assert not await h.async_redis.exists(h.config.approval_card_key(f"appr-{index}"))
                ref = await _peek_card_ref(h, f"appr-{index + 1}")
                assert ref is not None and ref["requested_by"] == "U0EXAMPLE1"

    asyncio.run(go())


@pytest.mark.parametrize("known", [True, False])
@pytest.mark.parametrize("actor", ["U0EXAMPLE2", "system"])
def test_unavailable_or_legacy_resume_never_labels_resolver_as_requester(
    make_harness, known: bool, actor: str
) -> None:
    async def go() -> None:
        approvals = DerivedRequesterApprovals(None, known=known)
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting_script("Next action")
            await h.kernel.process_event(
                _resume_turn(
                    "U0EXAMPLE1 supposedly asked",
                    thread="th-unknown-requester",
                    approval_id="appr-prior",
                    author=actor,
                )
            )
            assert approvals.requests[0].author == actor
            assert h.sink.posts[0][2] == ""
            assert isinstance(h.sink.posts[0][1].interaction, ConfirmIntent)
            ref = await _peek_card_ref(h, "appr-1")
            assert ref is not None and ref["requested_by"] == ""

    asyncio.run(go())


@pytest.mark.parametrize("known,expected", [(False, "U0EXAMPLE4"), (True, "")])
def test_fresh_human_request_fallback_only_for_older_api(make_harness, known, expected) -> None:
    async def go() -> None:
        async with make_harness(approvals=DerivedRequesterApprovals(None, known=known)) as h:
            h.runner.default_script = _awaiting_script("Fresh action")
            await h.kernel.process_event(
                _qevent("New ask", thread="th-fresh-requester").model_copy(
                    update={"author": "U0EXAMPLE4"}
                )
            )
            assert h.sink.posts[0][2] == expected
            assert isinstance(h.sink.posts[0][1].interaction, ConfirmIntent)

    asyncio.run(go())


def test_routed_requester_is_used_in_card_memory_and_notification_metadata(make_harness) -> None:
    async def go() -> None:
        approvals = DerivedRequesterApprovals("U0EXAMPLE1")
        async with make_harness(
            approvals=approvals, binding=RoutedBinding(_split_approval_routes())
        ) as h:
            h.runner.default_script = _awaiting_routed_script("Next bounded action", "managers")
            await h.kernel.process_event(
                _resume_turn(
                    "approved",
                    thread="th-requester-notification",
                    approval_id="appr-prior",
                    author="U0EXAMPLE2",
                )
            )
            posts = [
                event
                for event, _route, _best_effort in h.sink.events
                if isinstance(event, ReplyPost)
            ]
            assert len(posts) == 2
            assert [event.requested_by for event in posts] == ["U0EXAMPLE1", "U0EXAMPLE1"]
            assert posts[0].message.interaction is not None
            assert posts[1].message.interaction is None
            assert approvals.requests[0].author == "U0EXAMPLE2"

    asyncio.run(go())


def test_missing_display_uses_plain_notice_and_card_without_rewriting_record(make_harness) -> None:
    tool = "mcp__plugin_acme_files__file_attachment"
    machine = "Tool call awaiting approval: " + tool + ' {"file_name": "example.pdf"}'
    sentence = "Approve file attachment. File name: example.pdf"

    async def go() -> None:
        approvals = RecordingApprovals()
        binding = RoutedBinding({"managers": _resolution_route()})
        async with make_harness(approvals=approvals, binding=binding) as h:
            script = _awaiting_script_with_display(machine, "")
            script[-1] = script[-1].model_copy(
                update={
                    "approval_display": None,
                    "approval_route": "managers",
                    "approval_granted_tool": tool,
                    "approval_granted_arguments": {"file_name": "example.pdf"},
                }
            )
            h.runner.default_script = script
            await h.kernel.process_event(_qevent("please attach", event_id="ev-plain-display"))
            assert approvals.requests[0].summary == machine
            assert h.sink.last_text is not None
            assert sentence in h.sink.last_text and tool not in h.sink.last_text
            assert h.sink.posts[0][1].text == sentence
            assert isinstance(h.sink.posts[0][1].interaction, ConfirmIntent)
            assert h.sink.posts[0][1].interaction.prompt == sentence

    asyncio.run(go())


def test_rendered_approval_display_preserves_exact_argument_tokens(make_harness) -> None:
    tool = "mcp__plugin_acme_files__file_attachment"
    filename = "mcp__acme__file_attachment"
    sentence = f"Attach {filename}"
    machine = f'Tool call awaiting approval: {tool} {{"filename": "{filename}"}}'

    async def go() -> None:
        approvals = RecordingApprovals()
        binding = RoutedBinding({"managers": _resolution_route()})
        async with make_harness(approvals=approvals, binding=binding) as h:
            script = _awaiting_script_with_display(machine, sentence)
            script[-1] = script[-1].model_copy(
                update={
                    "approval_route": "managers",
                    "approval_granted_tool": tool,
                    "approval_granted_arguments": {"filename": filename},
                }
            )
            h.runner.default_script = script
            await h.kernel.process_event(_qevent("please attach", event_id="ev-exact-display"))
            assert approvals.requests[0].summary == machine
            assert approvals.requests[0].granted_arguments == {"filename": filename}
            assert h.sink.last_text is not None and sentence in h.sink.last_text
            assert h.sink.posts[0][1].text == sentence
            assert isinstance(h.sink.posts[0][1].interaction, ConfirmIntent)
            assert h.sink.posts[0][1].interaction.prompt == sentence

    asyncio.run(go())


def test_missing_display_sentence_boundaries_reach_notice_and_card(make_harness) -> None:
    """@spec plain-approval-wording: actual fallback notice and card retain grants."""
    import json
    from pathlib import Path

    cases = json.loads(
        (Path(__file__).resolve().parents[4] / "tests/vectors/user-action-wording.json").read_text()
    )["metadata_references"]

    async def go() -> None:
        for index, case in enumerate(cases):
            arguments = {"file_name": case["tool"] + ".json"}
            approvals = RecordingApprovals()
            binding = RoutedBinding({"managers": _resolution_route()})
            async with make_harness(approvals=approvals, binding=binding) as h:
                script = _awaiting_script_with_display(case["summary"], "")
                script[-1] = script[-1].model_copy(
                    update={
                        "approval_display": None,
                        "approval_route": "managers",
                        "approval_granted_tool": case["tool"],
                        "approval_granted_arguments": arguments,
                    }
                )
                h.runner.default_script = script
                await h.kernel.process_event(
                    _qevent("please review", event_id=f"ev-period-display-{index}")
                )
                assert approvals.requests[0].summary == case["summary"]
                assert approvals.requests[0].granted_tool == case["tool"]
                assert approvals.requests[0].granted_arguments == arguments
                # The notice control string compacts whitespace (#817); the card stays literal.
                notice = " ".join(case["display"].split())
                assert h.sink.last_text is not None and notice in h.sink.last_text
                assert h.sink.posts[0][1].text == case["display"]
                assert isinstance(h.sink.posts[0][1].interaction, ConfirmIntent)
                assert h.sink.posts[0][1].interaction.prompt == case["display"]

    asyncio.run(go())


@pytest.mark.parametrize("refused", [False, True])
# API response and replay contract: apps/api/src/curie_api/routers/approvals.py::create_approval.
def test_approval_api_outage_retries_before_pausing_and_422_escalates(
    make_harness, monkeypatch, refused
) -> None:
    from curie_worker import api_retry

    async def go() -> None:
        now = 0.0
        seen: list[httpx.Request] = []
        sleeps: list[float] = []

        async def sleep(delay: float) -> None:
            nonlocal now
            sleeps.append(delay)
            now += delay
            await asyncio.sleep(0)

        monkeypatch.setattr(api_retry, "_clock", lambda: now)
        monkeypatch.setattr(api_retry, "_sleep", sleep)

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            if refused:
                return httpx.Response(422, json={"detail": "invalid approval"})
            if len(seen) == 1:
                raise httpx.ConnectError("API restarting", request=request)
            return httpx.Response(201, json={"id": "approval-1", "status": "pending"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            approvals = ApprovalClient(
                api_base_url="http://api", api_key="example", client=http, read_timeout_s=1
            )
            async with make_harness(approvals=approvals) as h:
                h.runner.default_script = _awaiting_script("Approve a deployment")
                event = _qevent("please deploy")
                await h.kernel.process_event(event)
                if refused:
                    assert len(seen) == 1
                    assert sleeps == []
                    assert [completion.outcome for completion in h.sink.completions] == [
                        "escalated"
                    ]
                    assert all(s.operating_mode == "Running" for s in h.fake_k8s.sandboxes.values())
                else:
                    assert len(seen) == 2
                    assert sleeps == [0.5]
                    assert seen[0].content == seen[1].content
                    assert json.loads(seen[0].content)["dedupe_key"] == event.event_id
                    assert [completion.outcome for completion in h.sink.completions] == [
                        "awaiting-approval"
                    ]
                    assert all(
                        s.operating_mode == "Suspended" for s in h.fake_k8s.sandboxes.values()
                    )
                assert h.runner.opened == ["please deploy"]
                assert await h.kernel._markers.is_terminal(event.event_id)

    asyncio.run(go())
