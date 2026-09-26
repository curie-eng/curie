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
    WorkItemStartGrant,
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
    async def resolve(self, kind: str, channel: str) -> object:
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
            )
            return SimpleNamespace(handle=handoff, prepared=None)
        handle = self.substrate.claim(  # type: ignore[attr-defined]
            str(kwargs["thread_key"]),
            env=kwargs.get("env"),
            agent_name=kwargs.get("agent_name"),
            workspace_repo=kwargs.get("repo_full_name"),
        )
        return SimpleNamespace(handle=handle, prepared=None)

    def touch(self, _thread_key: str, *, ttl_seconds: int) -> bool:
        return True

    def release(self, _thread_key: str) -> None:
        return None


class _WorkItems:
    """Dispatch double: records every finish the worker posts."""

    def __init__(self, *, deadline_s: float = 3600.0) -> None:
        self.deadline_s = deadline_s
        self.calls: list[str] = []
        self.finishes: list[dict[str, object]] = []
        self.after_finish: Callable[[], Awaitable[None]] | None = None

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
    async def create(self, _request: ApprovalRequest) -> CreatedApproval:
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

    async def create_publication(self, request: object) -> object:
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

            await h.kernel.process_event(
                _turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT)
            )

            assert len(opened) >= 2, "the continuation must have been attempted"
            assert "publish_changes" in opened[1]
            assert len(items.finishes) == 1
            finish = items.finishes[0]
            assert finish["outcome"] == "failed"
            assert finish["cause"] not in {"early_stop", "no_pull_request"}
            assert finish["cause"] in {"runner_failed", "runner_escalated"}

    asyncio.run(exercise())
