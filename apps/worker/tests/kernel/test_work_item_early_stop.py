"""A factory execution that ends without publishing gets one continuation (#3128).

A factory execute turn that ends ``done`` or ``idle`` without calling
``publish_changes`` is re-prompted ONCE in the same session. If it still does not
publish, the request finishes as ``early_stop`` (no progress report, no work tool,
no publish) or ``no_pull_request`` (it did something), and the agent's final
message rides on the terminal record as ``detail``, redacted before it is clipped.

The fake runner below is the kernel conftest runner: one scripted frame list per
``/v1/event``, every prompt recorded in ``h.runner.opened``. The WorkItems dispatch
double is the boundary outside the box.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from aci_protocol import (
    ErrorEvent,
    Final,
    OutboundEvent,
    QueuedTurn,
    ReplyHandle,
    SessionStatus,
    TextDelta,
    ToolNote,
    TurnSource,
)
from curie_worker.approvals import ApprovalRequest, CreatedApproval
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.workitem_dispatch import (
    WorkItemAcquireGrant,
    WorkItemConflict,
    WorkItemRequestView,
    WorkItemStartGrant,
    WorkItemTransportError,
)

AGENT_ID = uuid.UUID("11111111-1111-4111-8111-111111111111")
DEPLOYMENT_ID = uuid.UUID("22222222-2222-4222-8222-222222222222")
CHANNEL = "C0EXAMPLE1"
WORK_ITEM_REPO = "acme-corp/widgets"
ISSUE_URL = f"https://github.com/{WORK_ITEM_REPO}/issues/123"
ISSUE_PROMPT = f"Resolve {ISSUE_URL}"
PUBLISH_TOOL = "mcp__curie__publish_changes"
PROGRESS_TOOL = "mcp__curie__report_progress"
# A credential shape the shared redaction policy (curie_telemetry.redact
# ``github_pat``) replaces. Assembled so no literal token sits in the source.
GITHUB_TOKEN = "gh" + "p_" + "A1b2C3d4" * 5


class _Binding:
    async def resolve(self, kind: str, adapter: str | None, channel: str) -> object:
        return SimpleNamespace(
            agent_id=AGENT_ID,
            agent_name="acme-bot",
            deployment_id=DEPLOYMENT_ID,
            endpoint=None,
            adapter=None,
            approval_routes=None,
        )

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

    def select_repository(self, **kwargs: object) -> object:
        return kwargs["repo_full_name"]

    def claim_or_resume_with_handle(self, **kwargs: object) -> object:
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
        handle = self.substrate.claim(  # type: ignore[attr-defined]
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
    """Dispatch double: records every finish the worker posts."""

    def __init__(
        self,
        *,
        deadline_s: float = 3600.0,
        request_reads: list[str | float | BaseException] | None = None,
    ) -> None:
        self.deadline_s = deadline_s
        self.calls: list[str] = []
        self.finishes: list[dict[str, object]] = []
        self.after_finish: Callable[[], Awaitable[None]] | None = None
        # What each ``get_request`` answers, in order: a status, or an error to
        # raise, or a number of seconds to hang before reading as ``running``.
        # Once exhausted, the request reads as ``running``.
        self.request_reads = list(request_reads or [])

    async def acquire(
        self, request_id: uuid.UUID, *, owner: str, generation: int
    ) -> WorkItemAcquireGrant:
        self.calls.append("acquire")
        return WorkItemAcquireGrant(
            generation=generation,
            work_item_id=request_id,
            conversation_id=f"work-item-{request_id}",
            wait_deadline=(datetime.now(UTC) + timedelta(hours=1)).isoformat(),
            repo_full_name=WORK_ITEM_REPO,
        )

    async def start(self, request_id: uuid.UUID, **_: object) -> WorkItemStartGrant:
        self.calls.append("start")
        return WorkItemStartGrant(
            runtime_epoch=1,
            execution_deadline=datetime.now(UTC) + timedelta(seconds=self.deadline_s),
            remaining_s=self.deadline_s,
            heartbeat_interval_s=60.0,
        )

    async def finish(self, _request_id: uuid.UUID, **kwargs: object) -> None:
        self.calls.append("finish")
        self.finishes.append(kwargs)
        if self.after_finish is not None:
            await self.after_finish()

    async def issue_read_context(self, request_id: uuid.UUID) -> tuple[str, str]:
        return "acme widgets issue 7", f"wir.capability-for-{request_id}"

    async def get_request(self, _request_id: uuid.UUID) -> WorkItemRequestView:
        self.calls.append("get_request")
        read = self.request_reads.pop(0) if self.request_reads else "running"
        if isinstance(read, BaseException):
            raise read
        if isinstance(read, float):
            await asyncio.sleep(read)
            read = "running"
        return WorkItemRequestView(
            status=read,
            runtime_epoch=1,
            runtime_claim_name=None,
            runtime_sandbox_name=None,
        )

    def __getattr__(self, name: str):  # type: ignore[no-untyped-def]
        async def record(*_args: object, **_kwargs: object) -> None:
            self.calls.append(name)

        return record


class _PublicationPendingWorkItems(_WorkItems):
    """The publication loop, not this finish, owns the WorkItem terminus."""

    async def finish(self, _request_id: uuid.UUID, **kwargs: object) -> None:
        self.calls.append("finish")
        self.finishes.append(kwargs)
        raise WorkItemConflict("publication_pending")


class _Approvals:
    async def create(self, _request: ApprovalRequest, *, budget_s: float = 120) -> CreatedApproval:
        return CreatedApproval(id="appr-1", status="pending")


def _turn(
    event_id: str,
    text: str,
    *,
    placeholder: str | None = None,
    conversation_id: str = "1700000000.000001",
) -> QueuedTurn:
    return QueuedTurn(
        event_id=event_id,
        conversation_id=conversation_id,
        author="U0EXAMPLE1",
        text=text,
        reply_handle=ReplyHandle(
            kind="slack",
            channel=CHANNEL,
            placeholder=placeholder,
            endpoint=None,
            adapter=None,
        ),
        received_at="2026-09-25T01:00:00+00:00",
        source=TurnSource.SLACK,
    )


def _tool(name: str) -> ToolNote:
    return ToolNote(text=f"running tool {name}", tool=name)


def _done(text: str) -> Final:
    return Final(text=text, status=SessionStatus.DONE)


def _publish_final() -> Final:
    return Final(
        text="Ready to publish",
        status=SessionStatus.AWAITING_APPROVAL,
        approval_summary="Publish the change",
        approval_gate_kind="permission",
        approval_granted_tool=PUBLISH_TOOL,
    )


# A zero-work first turn: it read the issue and replied with text.
ZERO_WORK_TURN: list[OutboundEvent] = [
    _tool("mcp__github__get_issue"),
    TextDelta(text="I read the issue."),
    _done("I read the issue."),
]


async def _run_execute(
    make_harness,
    scripts: list[list[OutboundEvent]],
    *,
    work_items: _WorkItems | None = None,
    **harness_kwargs: object,
) -> tuple[list[str], _WorkItems, object]:
    """Drive one factory execute event; return the prompts, the double, the event."""

    harness_kwargs.setdefault("publication_creator", _PublicationApi())
    async with make_harness(
        binding=_Binding(), workspace_factory=_Workspace, **harness_kwargs
    ) as h:
        items = work_items if work_items is not None else _WorkItems()
        h.kernel._work_items = items
        h.runner.turn_scripts = [list(script) for script in scripts]
        h.runner.default_script = [_done("default script must not run")]
        event = _turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT)

        await h.kernel.process_event(event)

        terminal = await h.kernel._markers.is_terminal(event.event_id)
        return list(h.runner.opened), items, terminal


# --- W1: zero-work turn -> one continuation -> early_stop -------------------------


def test_a_zero_work_turn_is_reprompted_once_then_ends_as_early_stop(make_harness) -> None:
    async def exercise() -> None:
        opened, items, _ = await _run_execute(
            make_harness,
            [ZERO_WORK_TURN, [_done("I will not continue.")]],
        )

        assert len(opened) == 2
        assert opened[0] == ISSUE_PROMPT
        assert opened[1] != opened[0]
        assert "publish_changes" in opened[1]
        assert len(items.finishes) == 1
        finish = items.finishes[0]
        assert finish["outcome"] == "failed"
        assert finish["cause"] == "early_stop"
        assert finish["detail"] == "I will not continue."

    asyncio.run(exercise())


# --- W2: worked, never published -> continuation -> no_pull_request ---------------


def test_a_turn_that_reported_progress_ends_as_no_pull_request(make_harness) -> None:
    async def exercise() -> None:
        opened, items, _ = await _run_execute(
            make_harness,
            [
                [
                    _tool("mcp__github__get_issue"),
                    _tool(PROGRESS_TOOL),
                    TextDelta(text="Working."),
                    _done("Working."),
                ],
                [_done("The fix needs a design decision, so I did not publish.")],
            ],
        )

        assert len(opened) == 2
        assert "publish_changes" in opened[1]
        assert len(items.finishes) == 1
        assert items.finishes[0]["outcome"] == "failed"
        assert items.finishes[0]["cause"] == "no_pull_request"
        assert (
            items.finishes[0]["detail"] == "The fix needs a design decision, so I did not publish."
        )

    asyncio.run(exercise())


def test_early_stop_and_unpublished_turns_get_different_continuation_prompts(
    make_harness,
) -> None:
    async def exercise() -> None:
        early, _, _ = await _run_execute(make_harness, [ZERO_WORK_TURN, [_done("no")]])
        worked, _, _ = await _run_execute(
            make_harness,
            [[_tool("Bash"), _done("edited")], [_done("no")]],
        )

        assert early[1] != worked[1]
        assert "No changes needed:" in early[1]
        assert "No changes needed:" in worked[1]

    asyncio.run(exercise())


# --- W2b / W2c: work evidence without report_progress -----------------------------


def test_work_tools_without_a_progress_report_end_as_no_pull_request(make_harness) -> None:
    """The 60-call shape: Bash and a comment write, but no report_progress."""

    async def exercise() -> None:
        first = [
            _tool("mcp__github__get_issue"),
            *[_tool("Bash") for _ in range(30)],
            _tool("mcp__github__add_issue_comment"),
            _done("Commented on the issue."),
        ]
        opened, items, _ = await _run_execute(
            make_harness, [first, [_done("Still not publishing.")]]
        )

        assert len(opened) == 2
        assert items.finishes[0]["cause"] == "no_pull_request"
        assert items.finishes[0]["detail"] == "Still not publishing."

    asyncio.run(exercise())


def test_only_context_reads_end_as_early_stop(make_harness) -> None:
    async def exercise() -> None:
        first = [
            _tool("Read"),
            _tool("Grep"),
            _tool("mcp__github__list_issues"),
            _done("I looked around."),
        ]
        opened, items, _ = await _run_execute(make_harness, [first, [_done("Stopping.")]])

        assert len(opened) == 2
        assert items.finishes[0]["cause"] == "early_stop"

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("tools", "cause"),
    [
        (frozenset(), "early_stop"),
        (frozenset({"mcp__github__get_issue"}), "early_stop"),
        (frozenset({"Read", "Grep", "mcp__github__list_issues"}), "early_stop"),
        (frozenset({"Glob", "LS", "WebFetch", "WebSearch", "ToolSearch"}), "early_stop"),
        (frozenset({"TodoRead", "TodoWrite", "NotebookRead"}), "early_stop"),
        (frozenset({"mcp__github__search_code", "mcp__github__read_file"}), "early_stop"),
        (frozenset({"mcp__curie__request_approval"}), "early_stop"),
        (frozenset({PROGRESS_TOOL}), "no_pull_request"),
        (frozenset({"Bash"}), "no_pull_request"),
        (frozenset({"Edit"}), "no_pull_request"),
        (frozenset({"Write"}), "no_pull_request"),
        (frozenset({"mcp__github__add_issue_comment"}), "no_pull_request"),
        (frozenset({"mcp__github__create_branch", "Read"}), "no_pull_request"),
        (frozenset({PUBLISH_TOOL}), "no_pull_request"),
    ],
)
def test_the_unpublished_cause_follows_the_work_evidence(tools: frozenset[str], cause: str) -> None:
    from curie_worker.kernel import _unpublished_cause

    assert _unpublished_cause(tools) == cause


# --- W3 / W10: publication ends the loop -------------------------------------------


class _PublicationApi:
    def __init__(self) -> None:
        self.creates: list[object] = []

    async def get_publication_lineage(self, *_args: object) -> None:
        return None

    async def get_publication_precheck_context(self, **_kwargs: object) -> None:
        return None

    async def create_publication(self, request: object, *, budget_s: float = 120) -> object:
        from curie_worker.approvals import CreatedPublication

        self.creates.append(request)
        return CreatedPublication(id="publication-1", approval_id="approval-1", status="pending")


def _patch_snapshot(h: object, monkeypatch: pytest.MonkeyPatch) -> None:
    from curie_worker.runner_client import RunnerWorkspaceSnapshot

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
        "curie_worker.kernel.validate_snapshot_against_base",
        lambda *_args, **_kwargs: None,
    )


@pytest.mark.parametrize("publish_on", ["first", "continuation"])
def test_a_publication_is_held_for_approval_and_never_finished(
    make_harness, monkeypatch: pytest.MonkeyPatch, publish_on: str
) -> None:
    async def exercise() -> None:
        publications = _PublicationApi()
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=publications,
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            _patch_snapshot(h, monkeypatch)
            if publish_on == "first":
                h.runner.turn_scripts = [[_tool(PUBLISH_TOOL), _publish_final()]]
            else:
                h.runner.turn_scripts = [
                    list(ZERO_WORK_TURN),
                    [_tool(PUBLISH_TOOL), _publish_final()],
                ]
            h.runner.default_script = [_done("default script must not run")]

            await h.kernel.process_event(_turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT))

            assert len(h.runner.opened) == (1 if publish_on == "first" else 2)
            assert len(publications.creates) == 1
            assert items.finishes == []
            assert "hold_for_approval" in items.calls

    asyncio.run(exercise())


def test_failed_publication_approval_finishes_factory_request_immediately(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_worker.approvals import ApprovalBackendError

    class FailedPublicationApi(_PublicationApi):
        async def create_publication(self, request: object, *, budget_s: float = 120) -> object:
            self.creates.append(request)
            raise ApprovalBackendError("publication snapshot failed")

    async def exercise() -> None:
        publications = FailedPublicationApi()
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=publications,
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            _patch_snapshot(h, monkeypatch)
            h.runner.turn_scripts = [[_tool(PUBLISH_TOOL), _publish_final()]]
            await h.kernel.process_event(_turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT))
            assert len(publications.creates) == 1
            assert [finish["cause"] for finish in items.finishes] == ["approval_create_failed"]
            assert "hold_for_approval" not in items.calls

    asyncio.run(exercise())


def test_a_coded_publication_refusal_names_its_cause_on_the_factory_run(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#3617: the API's refusal code and message reach run.finish as the detail."""

    from curie_worker.approvals import ApprovalBackendError

    code = "publication.required_python_ci_unselected"
    message = "required Python CI does not select unitconv/convert.py"

    class RefusedPublicationApi(_PublicationApi):
        async def create_publication(self, request: object, *, budget_s: float = 120) -> object:
            self.creates.append(request)
            error = ApprovalBackendError(f"publication create failed: HTTP 409: {message}")
            error.refusal = f"{code}: {message}"
            raise error

    async def exercise() -> None:
        publications = RefusedPublicationApi()
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=publications,
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            _patch_snapshot(h, monkeypatch)
            h.runner.turn_scripts = [[_tool(PUBLISH_TOOL), _publish_final()]]
            await h.kernel.process_event(_turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT))
            assert len(items.finishes) == 1
            finish = items.finishes[0]
            assert finish["cause"] == "approval_create_failed"
            assert finish["outcome"] == "failed"
            detail = finish["detail"]
            assert isinstance(detail, str)
            assert code in detail
            assert message in detail

    asyncio.run(exercise())


def test_a_thread_refusal_code_keeps_its_message_on_the_factory_run(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#3617: lineage refusals used to finish with detail=None."""

    from curie_worker.workspace import WorkspaceSelectionRefused

    message = "GitHub pull request head differs from the stored lineage"

    class StalePublicationApi(_PublicationApi):
        async def create_publication(self, request: object, *, budget_s: float = 120) -> object:
            self.creates.append(request)
            raise WorkspaceSelectionRefused(message)

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=StalePublicationApi(),
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            _patch_snapshot(h, monkeypatch)
            h.runner.turn_scripts = [[_tool(PUBLISH_TOOL), _publish_final()]]
            await h.kernel.process_event(_turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT))
            assert len(items.finishes) == 1
            finish = items.finishes[0]
            assert finish["cause"] == "approval_create_failed"
            detail = finish["detail"]
            assert isinstance(detail, str)
            assert message in detail

    asyncio.run(exercise())


def test_a_string_api_refusal_keeps_its_message_on_the_factory_run(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_worker.approvals import ApprovalBackendError

    message = "publication patch exceeds the 1048576-byte limit"

    class TooLargePublicationApi(_PublicationApi):
        async def create_publication(self, request: object, *, budget_s: float = 120) -> object:
            self.creates.append(request)
            error = ApprovalBackendError("publication create failed: HTTP 413")
            error.refusal = message
            raise error

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=TooLargePublicationApi(),
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            _patch_snapshot(h, monkeypatch)
            h.runner.turn_scripts = [[_tool(PUBLISH_TOOL), _publish_final()]]
            await h.kernel.process_event(_turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT))
            finish = items.finishes[0]
            assert finish["cause"] == "approval_create_failed"
            assert isinstance(finish["detail"], str)
            assert message in finish["detail"]

    asyncio.run(exercise())


def test_a_snapshot_base_mismatch_names_both_commits_on_the_factory_run(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # #4121: the real validator's message reaches the work item's detail.
    from curie_worker.runner_client import RunnerWorkspaceSnapshot

    snapshot_sha = "0123456789abcdef0123456789abcdef01234567"
    retained_sha = "fedcba9876543210fedcba9876543210fedcba98"

    class RetainedBaseWorkspace(_Workspace):
        def current(self, _thread_key: str) -> object:
            return SimpleNamespace(repo_full_name=WORK_ITEM_REPO, base_sha=retained_sha)

    async def exercise() -> None:
        publications = _PublicationApi()
        async with make_harness(
            binding=_Binding(),
            workspace_factory=RetainedBaseWorkspace,
            publication_creator=publications,
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items

            async def snapshot(*_args: object, **_kwargs: object) -> RunnerWorkspaceSnapshot:
                return RunnerWorkspaceSnapshot(
                    repo_full_name=WORK_ITEM_REPO,
                    base_sha=snapshot_sha,
                    patch=b"diff --git a/src/widget.py b/src/widget.py\n",
                    changed_paths=("src/widget.py",),
                    contains_workflow_files=False,
                    publication_title="Fix the widget parser",
                    publication_body="Fixes the parser.",
                )

            monkeypatch.setattr(h.kernel._runner, "snapshot", snapshot)
            h.runner.turn_scripts = [[_tool(PUBLISH_TOOL), _publish_final()]]
            await h.kernel.process_event(_turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT))
            assert len(items.finishes) == 1
            finish = items.finishes[0]
            assert finish["outcome"] == "failed"
            assert finish["cause"] == "approval_create_failed"
            detail = finish["detail"]
            assert isinstance(detail, str)
            assert detail
            assert "snapshot commit" in detail
            assert snapshot_sha[:12] in detail
            assert retained_sha[:12] in detail
            assert publications.creates == []

    asyncio.run(exercise())


def _patch_snapshot_backoff(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    delays: list[float] = []
    real_sleep = asyncio.sleep

    async def without_backoff(delay: float, result: object = None) -> object:
        if delay in {1.0, 2.0}:
            delays.append(delay)
            return await real_sleep(0, result)
        return await real_sleep(delay, result)

    monkeypatch.setattr("curie_worker.kernel.asyncio.sleep", without_backoff)
    return delays


def test_snapshot_read_retries_then_creates_exactly_one_publication_approval(
    make_harness, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from curie_worker.delivery_lease import unfenced_lease
    from curie_worker.runner_client import RunnerSnapshotReadError

    delays = _patch_snapshot_backoff(monkeypatch)

    async def exercise() -> None:
        publications = _PublicationApi()
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=publications,
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            _patch_snapshot(h, monkeypatch)
            valid_snapshot = h.kernel._runner.snapshot
            calls = 0
            remaining_budgets: list[float] = []
            validations: list[object] = []

            async def snapshot(*args: object, **kwargs: object) -> object:
                nonlocal calls
                calls += 1
                remaining = kwargs["remaining_s"]
                assert isinstance(remaining, float)
                remaining_budgets.append(remaining)
                if calls <= 2:
                    raise RunnerSnapshotReadError(
                        "/v1/snapshot returned an invalid bounded payload after 4096 bytes: "
                        "JSONDecodeError: incomplete JSON"
                    )
                return await valid_snapshot(*args, **kwargs)

            monkeypatch.setattr(h.kernel._runner, "snapshot", snapshot)
            monkeypatch.setattr(
                "curie_worker.kernel.validate_snapshot_against_base",
                lambda *_args, **kwargs: validations.append(kwargs["snapshot"]),
            )
            h.runner.turn_scripts = [[_tool(PUBLISH_TOOL), _publish_final()]]
            event = _turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT)
            with caplog.at_level(logging.WARNING, logger="curie_worker.kernel"):
                await h.kernel.process_event(event, lease=unfenced_lease())

            assert calls == 3
            assert remaining_budgets[0] > remaining_budgets[1] > remaining_budgets[2] > 5.0
            assert len(validations) == 1
            assert len(publications.creates) == 1
            assert items.calls.count("hold_for_approval") == 1
            assert items.finishes == []
            warnings = [
                record.getMessage()
                for record in caplog.records
                if record.levelno == logging.WARNING and event.event_id in record.getMessage()
            ]
            for attempt in (1, 2):
                matching = [warning for warning in warnings if f"attempt {attempt} of 3" in warning]
                assert len(matching) == 1
                assert "JSONDecodeError" in matching[0]

    asyncio.run(exercise())
    assert delays == [1.0, 2.0]


@pytest.mark.parametrize("failure_kind", ["payload", "timeout", "client", "refusal"])
def test_snapshot_read_failure_finishes_with_a_named_cause_and_bounded_retries(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    failure_kind: str,
) -> None:
    from aiohttp import ClientPayloadError
    from curie_worker.runner_client import RunnerError, RunnerSnapshotReadError

    delays = _patch_snapshot_backoff(monkeypatch)
    payload_message = (
        "/v1/snapshot returned an invalid bounded payload after 4096 bytes: "
        "JSONDecodeError: incomplete JSON"
    )
    errors = {
        "payload": RunnerSnapshotReadError(payload_message),
        "timeout": TimeoutError(),
        "client": ClientPayloadError("Response payload is not completed"),
        "refusal": RunnerError("/v1/snapshot -> 403: forbidden"),
    }
    expected_failure_text = {
        "payload": payload_message,
        "timeout": "TimeoutError",
        "client": "ClientPayloadError: Response payload is not completed",
        "refusal": "/v1/snapshot -> 403: forbidden",
    }[failure_kind]
    expected_calls = 1 if failure_kind == "refusal" else 3

    async def exercise() -> None:
        publications = _PublicationApi()
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=publications,
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            calls = 0
            validations: list[object] = []

            async def snapshot(*_args: object, **_kwargs: object) -> object:
                nonlocal calls
                calls += 1
                raise errors[failure_kind]

            monkeypatch.setattr(h.kernel._runner, "snapshot", snapshot)
            monkeypatch.setattr(
                "curie_worker.kernel.validate_snapshot_against_base",
                lambda *_args, **kwargs: validations.append(kwargs["snapshot"]),
            )
            h.runner.turn_scripts = [[_tool(PUBLISH_TOOL), _publish_final()]]
            event = _turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT)
            with caplog.at_level(logging.WARNING, logger="curie_worker.kernel"):
                await h.kernel.process_event(event)

            assert calls == expected_calls
            assert validations == []
            assert publications.creates == []
            assert "hold_for_approval" not in items.calls
            assert len(items.finishes) == 1
            finish = items.finishes[0]
            assert finish["outcome"] == "failed"
            assert finish["cause"] == "approval_create_failed"
            assert finish["detail"] == (
                f"publication snapshot could not be read after {expected_calls} attempt(s): "
                f"{expected_failure_text}"
            )
            warnings = [
                record.getMessage()
                for record in caplog.records
                if record.levelno == logging.WARNING and event.event_id in record.getMessage()
            ]
            for attempt in range(1, expected_calls + 1):
                matching = [warning for warning in warnings if f"attempt {attempt} of 3" in warning]
                assert len(matching) == 1
                assert expected_failure_text in matching[0]

    asyncio.run(exercise())
    assert delays == ([] if failure_kind == "refusal" else [1.0, 2.0])


@pytest.mark.parametrize("remaining_after_failure", [5.0, 4.0])
def test_snapshot_read_starts_no_retry_with_five_seconds_or_less_remaining(
    make_harness, monkeypatch: pytest.MonkeyPatch, remaining_after_failure: float
) -> None:
    from curie_worker import kernel as kernel_module
    from curie_worker.delivery_lease import DeliveryBudget, unfenced_lease
    from curie_worker.runner_client import RunnerSnapshotReadError

    _patch_snapshot_backoff(monkeypatch)

    async def exercise() -> None:
        lease = unfenced_lease()
        lease.budget = DeliveryBudget(
            deadline_ms=30_000,
            anchor_server_ms=0,
            anchor_monotonic=time.monotonic(),
        )
        elapsed = 0.0
        # Move only the kernel's elapsed-time view. The event loop's clock and
        # the real Valkey transport keep running normally.
        monkeypatch.setattr(
            kernel_module,
            "time",
            SimpleNamespace(monotonic=lambda: time.monotonic() + elapsed, time=time.time),
        )
        publications = _PublicationApi()
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=publications,
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            calls: list[float] = []

            async def snapshot(
                *_args: object, remaining_s: float | None = None, **_kwargs: object
            ) -> object:
                nonlocal elapsed
                assert remaining_s is not None and remaining_s > 5.0
                calls.append(remaining_s)
                elapsed += remaining_s - remaining_after_failure
                lease.budget = DeliveryBudget(
                    deadline_ms=int(remaining_after_failure * 1000),
                    anchor_server_ms=0,
                    anchor_monotonic=time.monotonic(),
                )
                raise RunnerSnapshotReadError("/v1/snapshot -> 503: unavailable")

            monkeypatch.setattr(h.kernel._runner, "snapshot", snapshot)
            h.runner.turn_scripts = [[_tool(PUBLISH_TOOL), _publish_final()]]
            await h.kernel.process_event(
                _turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT), lease=lease
            )

            assert lease.remaining_s() <= 5.0
            assert len(calls) == 1
            assert publications.creates == []
            assert len(items.finishes) == 1
            assert items.finishes[0]["cause"] == "approval_create_failed"
            detail = items.finishes[0]["detail"]
            assert isinstance(detail, str)
            assert detail.startswith("publication snapshot could not be read after 1 attempt(s): ")

    asyncio.run(exercise())


@pytest.mark.parametrize("remaining_before_snapshot", [5.0, 4.0])
def test_snapshot_read_starts_no_attempt_after_the_turn_spends_its_delivery_budget(
    make_harness, monkeypatch: pytest.MonkeyPatch, remaining_before_snapshot: float
) -> None:
    from curie_worker import kernel as kernel_module
    from curie_worker.delivery_lease import DeliveryBudget, unfenced_lease

    delays = _patch_snapshot_backoff(monkeypatch)

    async def exercise() -> None:
        lease = unfenced_lease()
        lease.budget = DeliveryBudget(
            deadline_ms=30_000,
            anchor_server_ms=0,
            anchor_monotonic=time.monotonic(),
        )
        elapsed = 0.0
        monkeypatch.setattr(
            kernel_module,
            "time",
            SimpleNamespace(monotonic=lambda: time.monotonic() + elapsed, time=time.time),
        )
        publications = _PublicationApi()
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=publications,
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            _patch_snapshot(h, monkeypatch)
            valid_snapshot = h.kernel._runner.snapshot
            real_continue = h.kernel._continue_unpublished
            calls = 0

            async def spend_turn_budget(*args: object, **kwargs: object) -> object:
                nonlocal elapsed
                outcome = await real_continue(*args, **kwargs)
                remaining = kwargs["remaining_s"]
                assert isinstance(remaining, float) and remaining > 5.0
                elapsed += remaining - remaining_before_snapshot
                lease.budget = DeliveryBudget(
                    deadline_ms=int(remaining_before_snapshot * 1000),
                    anchor_server_ms=0,
                    anchor_monotonic=time.monotonic(),
                )
                return outcome

            async def snapshot(*args: object, **kwargs: object) -> object:
                nonlocal calls
                calls += 1
                return await valid_snapshot(*args, **kwargs)

            monkeypatch.setattr(h.kernel, "_continue_unpublished", spend_turn_budget)
            monkeypatch.setattr(h.kernel._runner, "snapshot", snapshot)
            h.runner.turn_scripts = [[_tool(PUBLISH_TOOL), _publish_final()]]
            await h.kernel.process_event(
                _turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT), lease=lease
            )

            assert lease.remaining_s() <= 5.0
            assert calls == 0
            assert publications.creates == []
            assert len(items.finishes) == 1
            finish = items.finishes[0]
            assert finish["cause"] == "approval_create_failed"
            detail = finish["detail"]
            assert isinstance(detail, str)
            assert detail.startswith("publication snapshot could not be read after 0 attempt(s): ")

    asyncio.run(exercise())
    assert delays == []


@pytest.mark.parametrize(
    ("remaining_after_failures", "expected_delays"),
    [
        pytest.param((5.5,), [], id="first-backoff-would-cross-floor"),
        pytest.param((6.0,), [], id="first-backoff-would-reach-floor"),
        pytest.param((20.0, 6.5), [1.0], id="second-backoff-would-cross-floor"),
    ],
)
def test_snapshot_read_skips_backoff_that_would_spend_its_five_second_reserve(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
    remaining_after_failures: tuple[float, ...],
    expected_delays: list[float],
) -> None:
    from curie_worker import kernel as kernel_module
    from curie_worker.delivery_lease import DeliveryBudget, unfenced_lease
    from curie_worker.runner_client import RunnerSnapshotReadError

    delays = _patch_snapshot_backoff(monkeypatch)

    async def exercise() -> None:
        lease = unfenced_lease()
        lease.budget = DeliveryBudget(
            deadline_ms=30_000,
            anchor_server_ms=0,
            anchor_monotonic=time.monotonic(),
        )
        elapsed = 0.0
        monkeypatch.setattr(
            kernel_module,
            "time",
            SimpleNamespace(monotonic=lambda: time.monotonic() + elapsed, time=time.time),
        )
        publications = _PublicationApi()
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=publications,
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            calls = 0

            async def snapshot(
                *_args: object, remaining_s: float | None = None, **_kwargs: object
            ) -> object:
                nonlocal calls, elapsed
                assert remaining_s is not None and remaining_s > 5.0
                assert calls < len(remaining_after_failures), "unexpected snapshot retry"
                remaining_after_failure = remaining_after_failures[calls]
                calls += 1
                elapsed += remaining_s - remaining_after_failure
                lease.budget = DeliveryBudget(
                    deadline_ms=int(remaining_after_failure * 1000),
                    anchor_server_ms=0,
                    anchor_monotonic=time.monotonic(),
                )
                raise RunnerSnapshotReadError("/v1/snapshot -> 503: unavailable")

            monkeypatch.setattr(h.kernel._runner, "snapshot", snapshot)
            h.runner.turn_scripts = [[_tool(PUBLISH_TOOL), _publish_final()]]
            await h.kernel.process_event(
                _turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT), lease=lease
            )

            assert lease.remaining_s() > 5.0
            assert calls == len(remaining_after_failures)
            assert publications.creates == []
            assert len(items.finishes) == 1
            finish = items.finishes[0]
            assert finish["cause"] == "approval_create_failed"
            detail = finish["detail"]
            assert isinstance(detail, str)
            assert detail.startswith(
                f"publication snapshot could not be read after {calls} attempt(s): "
            )

    asyncio.run(exercise())
    assert delays == expected_delays


def test_snapshot_validation_refusal_is_not_retried_and_keeps_its_prefix(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_worker.workspace import WorkspacePreparationError

    delays = _patch_snapshot_backoff(monkeypatch)

    async def exercise() -> None:
        publications = _PublicationApi()
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=publications,
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            _patch_snapshot(h, monkeypatch)
            valid_snapshot = h.kernel._runner.snapshot
            calls = 0
            validations = 0

            async def snapshot(*args: object, **kwargs: object) -> object:
                nonlocal calls
                calls += 1
                return await valid_snapshot(*args, **kwargs)

            def refuse(*_args: object, **_kwargs: object) -> None:
                nonlocal validations
                validations += 1
                raise WorkspacePreparationError("publication-validation", "protected path blocked")

            monkeypatch.setattr(h.kernel._runner, "snapshot", snapshot)
            monkeypatch.setattr("curie_worker.kernel.validate_snapshot_against_base", refuse)
            h.runner.turn_scripts = [[_tool(PUBLISH_TOOL), _publish_final()]]
            await h.kernel.process_event(_turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT))

            assert calls == 1
            assert validations == 1
            assert publications.creates == []
            assert len(items.finishes) == 1
            finish = items.finishes[0]
            assert finish["cause"] == "approval_create_failed"
            detail = finish["detail"]
            assert isinstance(detail, str)
            assert detail.startswith("publication snapshot failed: ")
            assert "protected path blocked" in detail
            assert "could not be read" not in detail

    asyncio.run(exercise())
    assert delays == []


# --- W4: bounded to one ------------------------------------------------------------


def test_the_continuation_is_bounded_to_one(make_harness) -> None:
    async def exercise() -> None:
        opened, items, _ = await _run_execute(
            make_harness,
            [ZERO_WORK_TURN, [_done("second")], [_done("third must never run")]],
        )

        assert len(opened) == 2
        assert len(items.finishes) == 1
        assert items.finishes[0]["detail"] == "second"

    asyncio.run(exercise())


# --- W-T1 (#3873): a preflight-blocked runner answers every turn offline -----------------

# Replays the runner's PreflightBlockedSession shape (runner/src/curie_runner/
# preflight_blocked.py, #3873) as seen through SessionRunner translation: a text
# delta and a DONE final carrying the same explanation, and no ToolNote, on EVERY
# turn, including the kernel's continuation. The kernel already ends this shape as
# early_stop; this pins the terminus the runner change relies on.
BLOCKED_EXPLANATION = (
    "Could not complete: in-sandbox verification is unavailable before "
    "implementation, so no model round ran. Blocked check: check-1 "
    "(package_registry)."
)


def test_a_preflight_blocked_runner_ends_as_early_stop_and_releases_the_claim(
    make_harness,
) -> None:
    from curie_worker.kernel import _EARLY_STOP_PROMPT

    def blocked_turn() -> list[OutboundEvent]:
        return [TextDelta(text=BLOCKED_EXPLANATION), _done(BLOCKED_EXPLANATION)]

    async def exercise() -> None:
        publications = _PublicationApi()
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=publications,
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            h.runner.turn_scripts = [blocked_turn(), blocked_turn()]
            h.runner.default_script = [_done("a third turn must never open")]

            await h.kernel.process_event(_turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT))

            assert len(h.runner.opened) == 2
            assert h.runner.opened[0] == ISSUE_PROMPT
            assert _EARLY_STOP_PROMPT in h.runner.opened[1]
            assert len(items.finishes) == 1
            finish = items.finishes[0]
            assert finish["outcome"] == "failed"
            assert finish["cause"] == "early_stop"
            assert finish["detail"] == BLOCKED_EXPLANATION
            assert publications.creates == []
            assert "hold_for_approval" not in items.calls
            assert h.fake_k8s.deleted_claims
            assert h.fake_k8s.claims == {}

    asyncio.run(exercise())


# Liveness sibling: a turn that calls a work tool and ends without publishing
# finishing as no_pull_request is already covered by
# test_work_tools_without_a_progress_report_end_as_no_pull_request (W2b), so none
# is added here.


# --- W5 / W6 / W6b: the detail on the terminal record --------------------------------


def test_an_empty_continuation_reply_keeps_the_first_turns_message(make_harness) -> None:
    async def exercise() -> None:
        opened, items, _ = await _run_execute(make_harness, [ZERO_WORK_TURN, [_done("")]])

        assert len(opened) == 2
        assert items.finishes[0]["cause"] == "early_stop"
        assert items.finishes[0]["detail"] == "I read the issue."

    asyncio.run(exercise())


def test_a_long_final_message_is_clipped_to_the_finish_limit(make_harness) -> None:
    async def exercise() -> None:
        _, items, _ = await _run_execute(make_harness, [ZERO_WORK_TURN, [_done("x" * 5000)]])

        detail = items.finishes[0]["detail"]
        assert isinstance(detail, str)
        assert len(detail) <= 4000
        assert detail.endswith("...")
        assert detail.startswith("x" * 100)

    asyncio.run(exercise())


def test_the_final_message_is_redacted_before_it_is_clipped(make_harness) -> None:
    """A credential straddling the 4000-char clip never reaches the finish raw."""

    async def exercise() -> None:
        text = "y " * 1995 + GITHUB_TOKEN + " tail " + "z" * 500
        _, items, _ = await _run_execute(make_harness, [ZERO_WORK_TURN, [_done(text)]])

        detail = items.finishes[0]["detail"]
        assert isinstance(detail, str)
        assert len(detail) <= 4000
        assert GITHUB_TOKEN[:10] not in detail
        assert "gh" + "p_" not in detail

    asyncio.run(exercise())


# --- W7: the continuation fails -----------------------------------------------------


def test_a_failed_continuation_escalates_with_its_own_cause(make_harness) -> None:
    async def exercise() -> None:
        message = "model error: unknown: API Error: 402 This request requires more credits"
        opened, items, _ = await _run_execute(
            make_harness,
            [
                ZERO_WORK_TURN,
                [
                    ErrorEvent(message=message, classification="model-credit-exhausted"),
                    Final(text="", status=SessionStatus.CLASSIFIED_FAILURE),
                ],
            ],
        )

        assert len(opened) == 2
        assert len(items.finishes) == 1
        assert items.finishes[0]["cause"] == "model_credit_exhausted"
        assert items.finishes[0]["detail"] == message

    asyncio.run(exercise())


# --- W8: no budget left, no continuation --------------------------------------------


def test_no_continuation_opens_near_the_execution_deadline(make_harness) -> None:
    async def exercise() -> None:
        opened, items, _ = await _run_execute(
            make_harness,
            [ZERO_WORK_TURN, [_done("must not run")]],
            work_items=_WorkItems(deadline_s=3.0),
        )

        assert len(opened) == 1
        assert len(items.finishes) == 1
        assert items.finishes[0]["cause"] == "early_stop"
        assert items.finishes[0]["detail"] == "I read the issue."

    asyncio.run(exercise())


# --- W9: publication owns the terminus ------------------------------------------------


def test_publication_pending_on_an_early_stop_finish_settles_the_event(
    make_harness,
) -> None:
    async def exercise() -> None:
        opened, items, terminal = await _run_execute(
            make_harness,
            [ZERO_WORK_TURN, [_done("stopping")]],
            work_items=_PublicationPendingWorkItems(),
        )

        assert len(opened) == 2
        assert items.calls.count("finish") == 1
        assert items.finishes[0]["cause"] == "early_stop"
        assert terminal

    asyncio.run(exercise())


# --- W12: an approval-resume turn is never continued ---------------------------------


def test_an_approval_resume_turn_is_not_continued(make_harness) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            approvals=_Approvals(),
            publication_creator=_PublicationApi(),
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            h.runner.turn_scripts = [
                [
                    _tool("Bash"),
                    Final(
                        text="Requesting approval.",
                        status=SessionStatus.AWAITING_APPROVAL,
                        approval_summary="Run the requested command",
                        approval_gate_kind="permission",
                        approval_granted_tool="Bash",
                    ),
                ],
                [TextDelta(text="Resumed. "), _done("Resumed. Nothing to publish.")],
            ]
            h.runner.default_script = [_done("default script must not run")]

            await h.kernel.process_event(_turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT))
            await h.kernel.process_event(
                _turn(
                    f"approval-{uuid.uuid4()}-resolved",
                    "[approval resolved] approved",
                    placeholder="approval-placeholder",
                )
            )

            assert len(h.runner.opened) == 2
            assert len(items.finishes) == 1
            assert items.finishes[0]["cause"] == "no_pull_request"
            assert items.finishes[0]["detail"] == "Resumed. Nothing to publish."

    asyncio.run(exercise())


# --- ordinary chat is untouched --------------------------------------------------------


def test_an_ordinary_chat_turn_is_never_continued(make_harness) -> None:
    async def exercise() -> None:
        async with make_harness(binding=_Binding(), workspace_factory=_Workspace) as h:
            h.runner.turn_scripts = [[_done("Noted.")], [_done("must not run")]]

            await h.kernel.process_event(_turn(f"slack-{uuid.uuid4()}", "hello there"))

            assert h.runner.opened == ["hello there"]

    asyncio.run(exercise())


# --- review r1: a continuation that cannot start is a runner failure ---------------


def test_a_continuation_the_runner_refuses_keeps_the_runner_failure(make_harness) -> None:
    """The agent never saw the continuation prompt, so the ending is not the
    agent's: the kernel's runner failure policy decides, not early_stop."""

    from curie_worker.runner_client import RunnerError

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_PublicationApi(),
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            h.runner.turn_scripts = [list(ZERO_WORK_TURN)]
            h.runner.default_script = [_done("later attempts are refused before this")]
            real_start = h.kernel._runner.start_turn
            opened: list[str] = []

            async def start_turn(base_url: str, event: object, *args: object, **kwargs: object):
                opened.append(event.text)  # type: ignore[attr-defined]
                if len(opened) > 1:
                    raise RunnerError("runner refused the continuation turn")
                return await real_start(base_url, event, *args, **kwargs)

            h.kernel._runner.start_turn = start_turn  # type: ignore[method-assign]

            await h.kernel.process_event(_turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT))

            assert len(opened) >= 2, "the continuation must have been attempted"
            assert "publish_changes" in opened[1]
            assert len(items.finishes) == 1
            finish = items.finishes[0]
            assert finish["outcome"] == "failed"
            assert finish["cause"] not in {"early_stop", "no_pull_request"}
            assert finish["cause"] in {"runner_failed", "runner_escalated"}

    asyncio.run(exercise())


# --- #4191: a cancelled request is not re-prompted or flagged -----------------------


class _CountingPrecheckApi(_PublicationApi):
    """Counts precheck-context reads; raises from read ``fail_from`` (1-based) on."""

    def __init__(self, *, fail_from: int | None = None) -> None:
        super().__init__()
        self.prechecks = 0
        self.fail_from = fail_from

    async def get_publication_precheck_context(self, **_kwargs: object) -> None:
        from curie_worker.approvals import ApprovalBackendError

        self.prechecks += 1
        if self.fail_from is not None and self.prechecks >= self.fail_from:
            raise ApprovalBackendError("publication precheck context failed: HTTP 409")
        return None


# The first turn worked (Bash) and never published, so the continuation, when it
# opens, carries the unpublished prompt.
WORKED_TURN: list[OutboundEvent] = [_tool("Bash"), _done("Edited the parser.")]


def _continuation_noise(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if "continuation failed to start" in r.getMessage() or "escalating event" in r.getMessage()
    ]


def test_a_cancelled_request_is_not_continued(
    make_harness, caplog: pytest.LogCaptureFixture
) -> None:
    """#4191 AC3: the request reads ``cancellation_requested`` before the
    continuation, so it is skipped: no second turn, no precheck, no flag."""

    caplog.set_level("INFO", logger="curie_worker.kernel")

    async def exercise() -> None:
        publications = _CountingPrecheckApi()
        opened, items, _ = await _run_execute(
            make_harness,
            [WORKED_TURN, [_done("a continuation must not open")]],
            work_items=_WorkItems(request_reads=["cancellation_requested"]),
            publication_creator=publications,
        )

        assert opened == [ISSUE_PROMPT]
        # The one precheck read is the first turn's own; the continuation made none.
        assert publications.prechecks == 1
        assert items.calls.count("get_request") == 1
        assert _continuation_noise(caplog) == []
        assert any(
            "work-item continuation skipped" in r.getMessage()
            and "cancellation_requested" in r.getMessage()
            for r in caplog.records
        )

    asyncio.run(exercise())


def test_a_cancel_landing_during_the_continuation_precheck_stops_quietly(
    make_harness, caplog: pytest.LogCaptureFixture
) -> None:
    """#4191 AC4: the request was running at the first read, the cancel landed
    before the precheck, and the precheck was refused. The continuation stops
    without a traceback or a flag."""

    caplog.set_level("INFO", logger="curie_worker.kernel")

    async def exercise() -> None:
        publications = _CountingPrecheckApi(fail_from=2)
        opened, items, _ = await _run_execute(
            make_harness,
            [WORKED_TURN, [_done("a continuation must not open")]],
            work_items=_WorkItems(request_reads=["running", "cancellation_requested"]),
            publication_creator=publications,
        )

        assert opened == [ISSUE_PROMPT]
        assert publications.prechecks == 2
        assert items.calls.count("get_request") == 2
        continuation_records = [r for r in caplog.records if "continuation" in r.getMessage()]
        assert continuation_records
        assert all(r.exc_info is None for r in continuation_records)
        assert _continuation_noise(caplog) == []
        assert any(
            "work-item continuation stopped" in r.getMessage()
            and "cancellation_requested" in r.getMessage()
            for r in caplog.records
        )

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "case", ["read_unreachable", "read_hangs", "running", "running_precheck_fails"]
)
def test_a_running_or_unreadable_request_continues_as_before(
    make_harness, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, case: str
) -> None:
    """#4191 AC5: an unreadable or running request continues exactly as before;
    a running request whose precheck fails is still today's runner failure."""

    from curie_worker.kernel import _UNPUBLISHED_PROMPT

    caplog.set_level("INFO", logger="curie_worker.kernel")
    reads: list[str | float | BaseException] = []
    if case == "read_unreachable":
        reads = [WorkItemTransportError("work-item dispatch endpoint is unreachable")]
    elif case == "read_hangs":
        monkeypatch.setattr("curie_worker.kernel._REQUEST_STATUS_READ_TIMEOUT_S", 0.05)
        reads = [30.0]

    async def exercise() -> None:
        publications = _CountingPrecheckApi(
            fail_from=2 if case == "running_precheck_fails" else None
        )
        opened, items, _ = await _run_execute(
            make_harness,
            [WORKED_TURN, [_done("Still not publishing.")]],
            work_items=_WorkItems(request_reads=reads),
            publication_creator=publications,
        )

        if case == "running_precheck_fails":
            assert opened[0] == ISSUE_PROMPT
            assert any(
                "continuation failed to start" in r.getMessage() and r.exc_info is not None
                for r in caplog.records
            )
            escalations = [
                r.getMessage() for r in caplog.records if "escalating event" in r.getMessage()
            ]
            assert any("runner-error" in text for text in escalations), escalations
            assert len(items.finishes) == 1
            assert items.finishes[0]["cause"] in {"runner_failed", "runner_escalated"}
        else:
            assert len(opened) == 2
            assert opened[0] == ISSUE_PROMPT
            assert _UNPUBLISHED_PROMPT in opened[1]
            assert len(items.finishes) == 1
            assert items.finishes[0]["cause"] == "no_pull_request"
            assert items.finishes[0]["detail"] == "Still not publishing."

    asyncio.run(exercise())
