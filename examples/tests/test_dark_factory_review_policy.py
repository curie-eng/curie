"""The dark factory's review round policy, enforced by the review gate hook.

One advisory plan review, five counted diff rounds, the execution's own
timing, findings kept for every refusal, and a run that continues across
prompts in the same session. Drives ``examples/dark-factory/hooks/review_gate.py``
the way Claude Code does (one subprocess per hook event, JSON on stdin) on real
Git checkouts, through the helpers of ``test_dark_factory_review_gate.py``.
Exact cutoff instants run the hook in process with its UTC clock replaced.

The name prefixes select the acceptance criteria: ``test_advisory_plan_*``
(AC1), ``test_diff_round_*`` (AC2), ``test_subagent_type_*`` (AC3),
``test_timing_*`` (AC4), ``test_unchanged_candidate_*`` (AC5),
``test_continuation_*``, ``test_retained_findings_*`` and
``test_approval_after_edit_*`` (AC6). ``test_protected_path_*`` keeps the root
``.github`` refused through symlinks.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from curie_api import factory_ci
from curie_runner.sender_frame import frame_user_turn


def _load_gate_helpers() -> Any:
    """The review gate test module, loaded by path: ``examples/tests`` is no package."""
    path = Path(__file__).resolve().with_name("test_dark_factory_review_gate.py")
    spec = importlib.util.spec_from_file_location("dark_factory_review_policy_helpers", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    previous = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = previous
    return module


gate = _load_gate_helpers()
Session = gate.Session
PLAN, DIFF, PUBLISH, REPORT = gate.PLAN, gate.DIFF, gate.PUBLISH, gate.REPORT
SOURCE, TEST, ISSUE = gate.SOURCE, gate.TEST, gate.ISSUE
FAR_SECONDS = gate.FAR_SECONDS

STATE_KEY = "sess-1"
PLAN_INPUT = {"subagent_type": PLAN, "description": "plan-reviewer", "prompt": "Review the plan"}
DIFF_INPUT = {"subagent_type": DIFF, "description": "diff-reviewer", "prompt": "Review the diff"}
# A ticket body that must never reach the pod log.
TICKET = "# Fix add\n\nTICKET-BODY-7f3a: add(2, 3) returns -1."
TICKET_MARK = "TICKET-BODY-7f3a"
FINDING = "AC2: tests/test_calc.py never checks add(-1, 1)"
FAILING_CHECK = "lint-and-types"
# The fixed deadline of the in process cutoff tests, in epoch seconds.
DEADLINE = 2_000_000_000


# --- helpers -------------------------------------------------------------------


def _now() -> int:
    return int(datetime.now(UTC).timestamp())


def _at(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch, UTC)


def _epoch(text: str) -> int:
    return int(datetime.fromisoformat(text).timestamp())


def _session(tmp_path: Path, *, auto_issue_read: bool = True) -> Any:
    s = Session(tmp_path, auto_issue_read=auto_issue_read)
    # The Bash default timeout is read from this variable; keep the ambient one out.
    s.env.pop("BASH_DEFAULT_TIMEOUT_MS", None)
    return s


def _prompted(tmp_path: Path, *, auto_issue_read: bool = True) -> Any:
    """A fresh run on its first prompt, the workflow skill loaded."""
    s = _session(tmp_path, auto_issue_read=auto_issue_read)
    assert gate.contract() in gate.context_of(s.fire("UserPromptSubmit", prompt=ISSUE))
    s.load_workflow()
    return s


def _timed(tmp_path: Path) -> Any:
    """A fresh run that read its issue once, with the far deadline, by itself."""
    s = _prompted(tmp_path, auto_issue_read=False)
    s.read_issue()
    return s


def _plan_approved(s: Any) -> None:
    """Plan review approved and implement reported: source edits open."""
    _, context = s.review(PLAN, gate.reply(PLAN, "APPROVE"))
    assert "APPROVED" in context, context
    s.report("implement", 1)


def changes(kind: str, finding: str) -> str:
    """A reviewer CHANGES reply whose one blocking finding is ``finding``."""
    name = kind.split(":")[1]
    return f"REVIEWER: {name}\nVERDICT: CHANGES\n- {finding}\nOPEN QUESTIONS:\n- none\n"


def _diff_changes(s: Any, finding: str) -> str:
    _, context = s.review(DIFF, changes(DIFF, finding))
    return str(context)


def _edit_source(s: Any, label: str) -> None:
    """One accepted source edit: the hook allows it, the file changes, the edit completes."""
    path = s.cwd / SOURCE
    tool_input = {
        "file_path": str(path),
        "content": f"def add(a, b):\n    return a + b  # {label}\n",
    }
    assert gate.allowed(s.edit_with("Write", tool_input))
    path.write_text(tool_input["content"])
    s.fire(
        "PostToolUse",
        tool_name="Write",
        tool_input=tool_input,
        tool_response={"filePath": str(path), "success": True},
    )


def _capped(s: Any, last: str) -> str:
    """Five diff CHANGES on five candidates, the last carrying ``last``; its context."""
    for round_ in range(1, 5):
        _diff_changes(s, f"finding of round {round_}")
        _edit_source(s, f"rework {round_}")
    return _diff_changes(s, last)


def _read_timing(s: Any, *, deadline_in: int, seconds: int) -> int:
    """Record execution timing whose deadline is ``deadline_in`` seconds away; its epoch."""
    deadline = _now() + deadline_in
    s.read_issue(_at(deadline), seconds)
    return deadline


def _state_path(s: Any, key: str = STATE_KEY) -> Path:
    return Path(s.env["DARK_FACTORY_STATE_DIR"]) / f"{key}.json"


def _state(s: Any, key: str = STATE_KEY) -> dict[str, Any]:
    state: dict[str, Any] = json.loads(_state_path(s, key).read_text())
    return state


def _without_seq(state: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in state.items() if k != "seq"}


def _open_round_earlier(s: Any, seconds: int = FAR_SECONDS) -> None:
    """Move the open round's start back by ``seconds``.

    The hook stamps ``round_opened`` with the real UTC clock. A test that
    brings the deadline closer, instead of waiting for the clock to reach it,
    moves that stamp back too, so the round still opened before every cutoff.
    """
    path = _state_path(s)
    state = json.loads(path.read_text())
    opened = state.get("round_opened")
    assert isinstance(opened, int) and not isinstance(opened, bool), opened
    state["round_opened"] = opened - seconds
    path.write_text(json.dumps(state))


def _gates(s: Any, name: str) -> list[dict[str, Any]]:
    return [e for e in s.gate_events() if e["curie_gate"] == name]


def _timing_refusals(s: Any) -> list[tuple[str, str]]:
    return [(e["action"], e["cause"]) for e in _gates(s, "timing_refused")]


def _review_refusals(s: Any) -> list[tuple[str, str | None]]:
    return [(e["cause"], e.get("stage")) for e in _gates(s, "review_refused")]


def _deny(decision: dict[str, Any]) -> str:
    """A PreToolUse decision that must deny without a rewrite; its reason."""
    assert decision["permissionDecision"] == "deny", decision
    assert "updatedInput" not in decision, decision
    return str(decision["permissionDecisionReason"])


def _ci_text(round_: int = 2) -> str:
    """The platform's real wait_ci continuation, ``FAILING_CHECK`` failing."""
    detail = gate._ci_detail(
        gate.CI_SHA, gate._ci_run("unit-tests"), gate._ci_run(FAILING_CHECK, "failure")
    )
    text: str = factory_ci.continuation_text(
        ISSUE, gate.PR, gate.CI_SHA, round_, gate._view(detail), diagnostics=True
    )
    return text


# --- AC1: one advisory plan review ---------------------------------------------


def test_advisory_plan_changes_completes_planning_and_carries_its_findings(
    tmp_path: Path,
) -> None:
    s = _prompted(tmp_path)
    finding = "criterion 2 has no failing test planned for the empty input"

    _, context = s.review(PLAN, changes(PLAN, finding))

    assert not context.startswith("STOP.")
    assert "round 1: CHANGES" in context
    assert finding in context
    # Advisory: nothing sends the agent back to plan for another review.
    assert "Go back to phase plan" not in context
    assert "round 2 of" not in context
    state = _state(s)
    assert state["plan"] == {"round": 1, "verdict": "CHANGES"}
    assert state["stage"] == "failing_test"
    assert state["stopped"] is None
    assert gate.stages(s) == [("plan_review", 1, "CHANGES")]
    # Test edits open now; source edits wait for the implement report.
    assert gate.allowed(s.edit("Write", TEST))
    assert "failing_test" in gate.deny_reason(s.edit("Edit", SOURCE))
    s.report("implement", 1)
    assert gate.allowed(s.edit("Edit", SOURCE))
    assert finding not in s.log.read_text()


def test_advisory_plan_approve_completes_planning(tmp_path: Path) -> None:
    s = _prompted(tmp_path)

    _, context = s.review(PLAN, gate.reply(PLAN, "APPROVE"))

    assert "round 1: APPROVED" in context
    state = _state(s)
    assert state["plan"] == {"round": 1, "verdict": "APPROVE"}
    assert state["stage"] == "failing_test"
    assert gate.allowed(s.edit("Write", TEST))
    assert not gate.allowed(s.edit("Edit", SOURCE))


@pytest.mark.parametrize("verdict", ["APPROVE", "CHANGES"])
def test_advisory_plan_second_review_is_refused(tmp_path: Path, verdict: str) -> None:
    s = _prompted(tmp_path)
    s.review(PLAN, gate.reply(PLAN, verdict))
    phases = s.phases()

    named = _deny(s.pre("Agent", PLAN_INPUT))
    inferred = _deny(s.pre("Agent", {"description": "Plan review round 2", "prompt": "p"}))

    for reason in (named, inferred):
        assert not reason.startswith("STOP."), reason
    assert s.phases() == phases
    assert _review_refusals(s) == [("planning_complete", "plan_review")] * 2
    state = _state(s)
    assert state["plan"] == {"round": 1, "verdict": verdict}
    assert state["stopped"] is None
    # The run goes on to the failing test.
    assert gate.allowed(s.edit("Write", TEST))


_INCOMPLETE_REPLIES = {
    "empty_reply": "",
    "wrong_reviewer": "REVIEWER: diff-reviewer\nVERDICT: APPROVE\n",
    "no_verdict": "REVIEWER: plan-reviewer\nI could not decide.\n",
}


@pytest.mark.parametrize("case", ["tool_failure", *_INCOMPLETE_REPLIES])
def test_advisory_plan_incomplete_review_grants_nothing_until_a_retry(
    tmp_path: Path, case: str
) -> None:
    s = _prompted(tmp_path)
    pre = s.pre("Agent", PLAN_INPUT)
    assert pre["permissionDecision"] == "allow", pre
    if case == "tool_failure":
        out = s.fire(
            "PostToolUseFailure",
            tool_name="Agent",
            tool_input=pre["updatedInput"],
            error="model is not available",
        )
    else:
        out = s.fire(
            "PostToolUse",
            tool_name="Agent",
            tool_input=pre["updatedInput"],
            tool_response={"content": [{"type": "text", "text": _INCOMPLETE_REPLIES[case]}]},
        )

    assert out is not None
    assert not out["hookSpecificOutput"]["additionalContext"].startswith("STOP.")
    incomplete = _gates(s, "review_incomplete")
    assert [(e["stage"], e["round"]) for e in incomplete] == [("plan_review", 1)]
    state = _state(s)
    assert state["plan"] == {"round": 0, "verdict": None}
    assert state["stopped"] is None
    assert gate.classified(s) == []
    # Nothing is granted: no edit, no diff review, no publication.
    assert not gate.allowed(s.edit("Write", TEST))
    _deny(s.pre("Agent", DIFF_INPUT))
    assert s.pre(PUBLISH)["permissionDecision"] == "deny"

    # A retried plan review completes and opens the test edit.
    _, context = s.review(PLAN, gate.reply(PLAN, "APPROVE"))
    assert "round 1: APPROVED" in context
    assert _state(s)["plan"] == {"round": 1, "verdict": "APPROVE"}
    assert gate.allowed(s.edit("Write", TEST))


def test_advisory_plan_changes_alone_never_allows_publish(tmp_path: Path) -> None:
    s = _prompted(tmp_path)
    s.review(PLAN, changes(PLAN, "the plan skips criterion 3"))
    assert s.pre(PUBLISH)["permissionDecision"] == "deny"

    s.apply_edit("Write", TEST)
    s.report("implement", 1)
    _edit_source(s, "fix")
    assert s.pre(PUBLISH)["permissionDecision"] == "deny"

    # Only a diff approval publishes.
    _, context = s.review(DIFF, gate.reply(DIFF, "APPROVE"))
    assert "APPROVED" in context
    assert s.pre(PUBLISH)["permissionDecision"] == "allow"


# --- AC2: five counted diff rounds ---------------------------------------------


def test_diff_round_four_changes_then_an_approve_on_the_fifth_publishes(tmp_path: Path) -> None:
    s = _prompted(tmp_path)
    _plan_approved(s)

    for round_ in range(1, 5):
        context = _diff_changes(s, f"finding of round {round_}")
        assert not context.startswith("STOP."), context
        assert f"round {round_ + 1} of 5" in context
        assert s.pre(PUBLISH)["permissionDecision"] == "deny"
        _edit_source(s, f"rework {round_}")
    _, context = s.review(DIFF, gate.reply(DIFF, "APPROVE"))

    assert "round 5: APPROVED" in context
    assert s.pre(PUBLISH)["permissionDecision"] == "allow"
    assert _state(s)["diff"] == {"round": 5, "verdict": "APPROVE"}
    assert [p for p in s.phases() if p[0] == "review_diff"] == [
        ("review_diff", n) for n in range(1, 6)
    ]
    assert [r for r in gate.stages(s) if r[0] == "review_diff"] == [
        *(("review_diff", n, "CHANGES") for n in range(1, 5)),
        ("review_diff", 5, "APPROVE"),
    ]


def test_diff_round_five_changes_stop_the_run_with_the_last_findings(tmp_path: Path) -> None:
    s = _prompted(tmp_path)
    _plan_approved(s)

    context = _capped(s, FINDING)

    assert context.startswith("STOP.")
    assert "Could not complete:" in context
    assert FINDING in context
    state = _state(s)
    assert state["stopped"]
    assert state["diff"] == {"round": 5, "verdict": "CHANGES"}
    # A sixth admission is refused, even on a new candidate, and nothing publishes.
    (s.cwd / SOURCE).write_text("def add(a, b):\n    return b + a\n")
    sixth = _deny(s.pre("Agent", DIFF_INPUT))
    assert sixth.startswith("STOP.") and FINDING in sixth
    assert s.pre(PUBLISH)["permissionDecision"] == "deny"
    assert [p for p in s.phases() if p[0] == "review_diff"] == [
        ("review_diff", n) for n in range(1, 6)
    ]
    assert FINDING not in s.log.read_text()


def test_diff_round_an_incomplete_call_between_rounds_does_not_count(tmp_path: Path) -> None:
    s = _prompted(tmp_path)
    _plan_approved(s)
    _diff_changes(s, "first finding")
    _edit_source(s, "rework 1")

    pre = s.pre("Agent", DIFF_INPUT)
    assert pre["permissionDecision"] == "allow", pre
    out = s.fire(
        "PostToolUseFailure",
        tool_name="Agent",
        tool_input=pre["updatedInput"],
        error="the reviewer timed out",
    )

    assert out is not None
    assert not out["hookSpecificOutput"]["additionalContext"].startswith("STOP.")
    assert [(e["stage"], e["round"]) for e in _gates(s, "review_incomplete")] == [
        ("review_diff", 2)
    ]
    state = _state(s)
    assert state["diff"]["round"] == 1
    assert state["stopped"] is None
    # The retry on the same checkout is round 2 again, and it counts.
    context = _diff_changes(s, "second finding")
    assert "round 3 of 5" in context
    assert _state(s)["diff"] == {"round": 2, "verdict": "CHANGES"}
    assert [p for p in s.phases() if p[0] == "review_diff"] == [
        ("review_diff", 1),
        ("review_diff", 2),
        ("review_diff", 2),
    ]
    assert [r for r in gate.stages(s) if r[0] == "review_diff"] == [
        ("review_diff", 1, "CHANGES"),
        ("review_diff", 2, "CHANGES"),
    ]


def test_diff_round_a_call_the_runner_denied_does_not_count(tmp_path: Path) -> None:
    """The runner's own foreground guard may deny a call the hook admitted:
    no result event follows, and no round is spent."""
    s = _prompted(tmp_path)
    _plan_approved(s)
    _diff_changes(s, "first finding")
    _edit_source(s, "rework 1")

    assert s.pre("Agent", DIFF_INPUT)["permissionDecision"] == "allow"
    assert _state(s)["diff"]["round"] == 1

    _, context = s.review(DIFF, gate.reply(DIFF, "APPROVE"))
    assert "round 2: APPROVED" in context
    assert _state(s)["diff"] == {"round": 2, "verdict": "APPROVE"}
    assert s.pre(PUBLISH)["permissionDecision"] == "allow"


def test_diff_round_after_an_approve_the_same_checkout_is_not_reviewed_again(
    tmp_path: Path,
) -> None:
    s = _prompted(tmp_path)
    _plan_approved(s)
    _edit_source(s, "fix")
    s.review(DIFF, gate.reply(DIFF, "APPROVE"))
    phases = s.phases()

    reason = _deny(s.pre("Agent", DIFF_INPUT))

    assert "go to publish" in reason.lower()
    assert not reason.startswith("STOP.")
    assert _review_refusals(s) == [("already_approved", "review_diff")]
    assert s.phases() == phases
    state = _state(s)
    assert state["diff"] == {"round": 1, "verdict": "APPROVE"}
    assert state["stopped"] is None
    assert s.pre(PUBLISH)["permissionDecision"] == "allow"


def test_diff_round_ci_rounds_keep_their_three_round_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = gate._load_hook(monkeypatch, tmp_path, "dark_factory_review_policy_ci_marker")
    assert module._CI_ROUND.pattern == r"^Curie wait_ci round ([23]) of 3: "
    s = _session(tmp_path)

    context = gate.context_of(s.fire("UserPromptSubmit", prompt=_ci_text(2)))

    assert context.startswith("CI fix round")
    (line,) = [line for line in s.lines() if line.get("curie_phase") == "wait_ci"]
    assert line["round"] == 2
    assert "of 3" in line["note"]


# --- AC3: only the reviewers run as sub-agents ---------------------------------


@pytest.mark.parametrize("tool", ["Agent", "Task"])
@pytest.mark.parametrize("subagent_type", ["Explore", "general-purpose"])
@pytest.mark.parametrize("description", ["Plan review round 1", "plan the change"])
def test_subagent_type_a_named_non_reviewer_is_denied(
    tmp_path: Path, tool: str, subagent_type: str, description: str
) -> None:
    s = _prompted(tmp_path)
    before = _without_seq(_state(s))

    reason = _deny(
        s.pre(
            tool,
            {
                "subagent_type": subagent_type,
                "description": description,
                "prompt": "Review the plan, then the diff, for issue 7.",
                "run_in_background": False,
            },
        )
    )

    assert PLAN in reason
    assert s.phases() == []
    assert _review_refusals(s) == [("not_a_reviewer", None)]
    assert _without_seq(_state(s)) == before


def test_subagent_type_a_named_non_reviewer_is_denied_after_planning(tmp_path: Path) -> None:
    s = _prompted(tmp_path)
    _plan_approved(s)
    phases = s.phases()
    before = _without_seq(_state(s))

    _deny(
        s.pre(
            "Agent",
            {"subagent_type": "Explore", "description": "Diff review round 1", "prompt": "diff"},
        )
    )

    assert s.phases() == phases
    assert _review_refusals(s) == [("not_a_reviewer", None)]
    assert _without_seq(_state(s)) == before


@pytest.mark.parametrize(
    ("alias", "kind"),
    [
        ("plan-reviewer", PLAN),
        ("dark-factory:plan-reviewer", PLAN),
        ("diff-reviewer", DIFF),
        ("dark-factory:diff-reviewer", DIFF),
    ],
)
def test_subagent_type_reviewer_aliases_route(tmp_path: Path, alias: str, kind: str) -> None:
    s = _prompted(tmp_path)
    if kind == DIFF:
        _plan_approved(s)

    out = s.pre(
        "Agent",
        {
            "subagent_type": alias,
            "description": "review",
            "prompt": "Review it",
            "isolation": "worktree",
            "model": "sonnet",
            "run_in_background": True,
        },
    )

    assert out["permissionDecision"] == "allow", out
    updated = out["updatedInput"]
    assert updated["subagent_type"] == kind
    assert updated["run_in_background"] is False
    assert "isolation" not in updated and "model" not in updated
    assert updated["prompt"].startswith("Review it\n\n")
    assert updated["prompt"].endswith(gate.contract())
    phase = "plan_review" if kind == PLAN else "review_diff"
    assert s.phases()[-1] == (phase, 1)


@pytest.mark.parametrize("subagent_type", [None, "", "   "], ids=["omitted", "empty", "blank"])
def test_subagent_type_omitted_or_blank_still_infers_the_plan_reviewer(
    tmp_path: Path, subagent_type: str | None
) -> None:
    s = _prompted(tmp_path)
    tool_input: dict[str, Any] = {"description": "Plan review round 1", "prompt": "the diff"}
    if subagent_type is not None:
        tool_input["subagent_type"] = subagent_type

    out = s.pre("Agent", tool_input)

    assert out["permissionDecision"] == "allow", out
    assert out["updatedInput"]["subagent_type"] == PLAN
    assert s.phases() == [("plan_review", 1)]
    assert _gates(s, "review_refused") == []


# --- AC4: the execution's own timing -------------------------------------------


def test_timing_issue_read_reaches_the_hook_through_hooks_json(tmp_path: Path) -> None:
    s = _session(tmp_path, auto_issue_read=False)
    run = gate.HooksJsonRuntime(s)
    (out,) = run.dispatch("UserPromptSubmit", prompt=ISSUE)
    assert gate.contract() in gate.context_of(out)
    run.post("PostToolUse", "Skill", tool_input={"skill": gate.WORKFLOW_SKILL}, tool_response="ok")
    early = run.pre("Agent", {"description": "Plan review round 1", "prompt": "p"})
    assert early is not None
    assert "mcp__curie__get_issue" in _deny(early["hookSpecificOutput"])

    # A wiring gap (no PostToolUse matcher for the tool) fails inside read_issue.
    run.read_issue()

    (recorded,) = _gates(s, "timing_recorded")
    assert recorded["execution_seconds"] == FAR_SECONDS
    assert "APPROVED" in run.review(PLAN, "APPROVE")


def test_timing_missing_refuses_plan_review_until_an_issue_read(tmp_path: Path) -> None:
    s = _prompted(tmp_path, auto_issue_read=False)

    reason = _deny(s.pre("Agent", PLAN_INPUT))

    assert "mcp__curie__get_issue" in reason
    assert not reason.startswith("STOP.")
    assert _timing_refusals(s) == [("plan_review", "timing_missing")]
    assert s.phases() == []
    state = _state(s)
    assert state["plan"] == {"round": 0, "verdict": None}
    assert state["stopped"] is None
    assert state.get("timing") is None

    # Recovery: the issue read records the timing and the plan review runs.
    s.read_issue()
    _, context = s.review(PLAN, gate.reply(PLAN, "APPROVE"))
    assert "round 1: APPROVED" in context
    assert s.phases() == [("plan_review", 1)]


def test_timing_missing_refuses_a_cold_ci_round_until_an_issue_read(tmp_path: Path) -> None:
    s = _session(tmp_path, auto_issue_read=False)
    assert "CI fix round" in gate.context_of(s.fire("UserPromptSubmit", prompt=_ci_text(2)))

    # Edits, the diff review and publication all wait for the timing.
    edit = gate.deny_reason(s.edit("Edit", SOURCE))
    diff = _deny(s.pre("Agent", DIFF_INPUT))
    assert s.pre(PUBLISH)["permissionDecision"] == "deny"

    for reason in (edit, diff):
        assert "mcp__curie__get_issue" in reason and not reason.startswith("STOP."), reason
    assert _timing_refusals(s) == [
        ("edit", "timing_missing"),
        ("review_diff", "timing_missing"),
        ("publish", "timing_missing"),
    ]
    assert _gates(s, "edit_refused") == []
    state = _state(s)
    assert state["stopped"] is None
    assert state["diff"]["round"] == 0
    assert s.phases() == [("wait_ci", 2)]

    s.read_issue()
    assert gate.allowed(s.edit("Edit", SOURCE))
    _, context = s.review(DIFF, gate.reply(DIFF, "APPROVE"))
    assert "round 1: APPROVED" in context
    assert s.pre(PUBLISH)["permissionDecision"] == "allow"


def test_timing_missing_leaves_bash_as_it_is(tmp_path: Path) -> None:
    s = _prompted(tmp_path, auto_issue_read=False)

    timed = s.pre("Bash", {"command": "ls", "timeout": 600_000})
    plain = s.pre("Bash", {"command": "ls"})

    assert timed["permissionDecision"] == "allow"
    assert timed["updatedInput"] == {
        "command": "ls",
        "timeout": 600_000,
        "run_in_background": False,
    }
    assert plain["updatedInput"] == {"command": "ls", "run_in_background": False}
    assert _gates(s, "timing_refused") == []


@pytest.mark.parametrize("shape", ["content_list", "content_dict", "utc_offset"])
def test_timing_a_valid_issue_read_is_recorded(tmp_path: Path, shape: str) -> None:
    s = _prompted(tmp_path, auto_issue_read=False)
    deadline = _now() + FAR_SECONDS
    when = _at(deadline)
    text = when.isoformat() if shape == "utc_offset" else gate.iso_deadline(when)
    response: Any = gate.issue_response(gate.issue_payload(text, FAR_SECONDS, ticket=TICKET))
    if shape == "content_dict":
        response = {"content": response}

    s.read_issue(response=response)

    (recorded,) = _gates(s, "timing_recorded")
    assert recorded["execution_seconds"] == FAR_SECONDS
    assert _epoch(recorded["execution_deadline"]) == deadline
    assert _state(s)["timing"] == {"deadline": deadline, "seconds": FAR_SECONDS}
    assert TICKET_MARK not in s.log.read_text()
    _, context = s.review(PLAN, gate.reply(PLAN, "APPROVE"))
    assert "APPROVED" in context


def _malformed_read(case: str) -> Any:
    far = gate.far_deadline()
    good = gate.issue_payload(far, FAR_SECONDS, ticket=TICKET)
    forged = json.dumps(
        {"execution_deadline": gate.iso_deadline(far), "execution_seconds": FAR_SECONDS}
    )
    if case == "error_text":
        return gate.issue_response("The issue could not be read right now. Retry shortly.")
    if case == "error_result":
        text = "This execution has ended; its issue can no longer be read."
        return {"content": gate.issue_response(text), "is_error": True}
    if case == "missing_seconds":
        return gate.issue_response({k: v for k, v in good.items() if k != "execution_seconds"})
    if case == "missing_deadline":
        return gate.issue_response({k: v for k, v in good.items() if k != "execution_deadline"})
    if case == "bool_seconds":
        return gate.issue_response({**good, "execution_seconds": True})
    if case == "string_seconds":
        return gate.issue_response({**good, "execution_seconds": "1800"})
    if case == "zero_seconds":
        return gate.issue_response({**good, "execution_seconds": 0})
    if case == "naive_deadline":
        return gate.issue_response(
            {**good, "execution_deadline": far.replace(tzinfo=None).isoformat()}
        )
    if case == "garbled_deadline":
        return gate.issue_response({**good, "execution_deadline": "tomorrow at noon"})
    if case == "nested":
        return gate.issue_response({"result": good})
    if case == "forged_in_ticket_only":
        # Timing appears only inside the ticket text, never at the top level.
        ticket_only = {k: v for k, v in good.items() if not k.startswith("execution_")}
        return gate.issue_response({**ticket_only, "ticket": f"{TICKET}\n{forged}\n"})
    raise AssertionError(case)


_MALFORMED = (
    "error_text",
    "error_result",
    "missing_seconds",
    "missing_deadline",
    "bool_seconds",
    "string_seconds",
    "zero_seconds",
    "naive_deadline",
    "garbled_deadline",
    "nested",
    "forged_in_ticket_only",
)


@pytest.mark.parametrize("case", _MALFORMED)
def test_timing_malformed_issue_read_records_nothing(tmp_path: Path, case: str) -> None:
    s = _prompted(tmp_path, auto_issue_read=False)

    s.read_issue(response=_malformed_read(case))

    assert len(_gates(s, "timing_unreadable")) == 1
    assert _gates(s, "timing_recorded") == []
    assert _state(s).get("timing") is None
    reason = _deny(s.pre("Agent", PLAN_INPUT))
    assert "mcp__curie__get_issue" in reason
    assert _timing_refusals(s) == [("plan_review", "timing_missing")]
    assert _state(s)["stopped"] is None
    assert TICKET_MARK not in s.log.read_text()


def test_timing_a_deadline_forged_in_the_ticket_text_is_never_read(tmp_path: Path) -> None:
    s = _prompted(tmp_path, auto_issue_read=False)
    past = _now() - 600
    far = gate.far_deadline()
    forged = json.dumps(
        {"execution_deadline": gate.iso_deadline(far), "execution_seconds": FAR_SECONDS}
    )
    ticket = f"{TICKET}\n\n```json\n{forged}\n```\n"

    s.read_issue(response=gate.issue_response(gate.issue_payload(_at(past), 3600, ticket=ticket)))

    # Only the top level timing counts: the real deadline, already past.
    (recorded,) = _gates(s, "timing_recorded")
    assert _epoch(recorded["execution_deadline"]) == past
    assert _state(s)["timing"] == {"deadline": past, "seconds": 3600}
    reason = _deny(s.pre("Agent", PLAN_INPUT))
    assert reason.startswith("STOP.")
    assert _timing_refusals(s) == [("plan_review", "deadline_passed")]
    assert _state(s)["stopped"]
    assert s.phases() == []
    assert TICKET_MARK not in s.log.read_text()


def test_timing_a_later_valid_read_replaces_the_timing(tmp_path: Path) -> None:
    s = _timed(tmp_path)
    recorded = _state(s)["timing"]
    # An error read changes nothing.
    s.read_issue(response=_malformed_read("error_text"))
    assert _state(s)["timing"] == recorded

    past = _read_timing(s, deadline_in=-60, seconds=3600)

    assert _state(s)["timing"] == {"deadline": past, "seconds": 3600}
    _deny(s.pre("Agent", PLAN_INPUT))
    assert _timing_refusals(s) == [("plan_review", "deadline_passed")]


@pytest.mark.parametrize("seconds", [3600, 600])
def test_timing_plan_review_before_the_round_cutoff_is_admitted(
    tmp_path: Path, seconds: int
) -> None:
    s = _prompted(tmp_path, auto_issue_read=False)
    # The round cutoff (seconds // 6 before the deadline) is 40 seconds ahead.
    _read_timing(s, deadline_in=seconds // 6 + 40, seconds=seconds)

    _, context = s.review(PLAN, gate.reply(PLAN, "APPROVE"))

    assert "round 1: APPROVED" in context
    assert _gates(s, "timing_refused") == []
    assert gate.allowed(s.edit("Write", TEST))


@pytest.mark.parametrize("seconds", [3600, 600])
def test_timing_plan_review_after_the_round_cutoff_stops_the_run(
    tmp_path: Path, seconds: int
) -> None:
    s = _prompted(tmp_path, auto_issue_read=False)
    # The round cutoff passed 40 seconds ago; the deadline is still ahead.
    _read_timing(s, deadline_in=seconds // 6 - 40, seconds=seconds)

    reason = _deny(s.pre("Agent", PLAN_INPUT))

    assert reason.startswith("STOP.") and "Could not complete:" in reason
    assert _timing_refusals(s) == [("plan_review", "round_cutoff")]
    assert s.phases() == []
    state = _state(s)
    assert state["stopped"]
    assert state["plan"] == {"round": 0, "verdict": None}
    assert not gate.allowed(s.edit("Write", TEST))
    assert s.pre(PUBLISH)["permissionDecision"] == "deny"


def test_timing_a_diff_changes_after_the_round_cutoff_stops_instead_of_opening_a_round(
    tmp_path: Path,
) -> None:
    s = _timed(tmp_path)
    _plan_approved(s)
    _edit_source(s, "fix")
    _open_round_earlier(s)
    # The round cutoff passed 60 seconds ago; wrap up is 240 seconds ahead.
    _read_timing(s, deadline_in=600 - 60, seconds=3600)

    context = _diff_changes(s, FINDING)

    assert context.startswith("STOP.") and "Could not complete:" in context
    assert FINDING in context
    assert "round 2 of 5" not in context
    state = _state(s)
    assert state["diff"] == {"round": 1, "verdict": "CHANGES"}
    assert state["stopped"]
    assert ("open_round", "round_cutoff") in _timing_refusals(s)
    assert s.pre(PUBLISH)["permissionDecision"] == "deny"


@pytest.mark.parametrize("seconds", [3600, 600])
def test_timing_diff_review_and_edits_before_wrap_up_are_admitted(
    tmp_path: Path, seconds: int
) -> None:
    s = _timed(tmp_path)
    _plan_approved(s)
    _open_round_earlier(s)
    # Wrap up (seconds // 12 before the deadline) is 40 seconds ahead.
    _read_timing(s, deadline_in=seconds // 12 + 40, seconds=seconds)

    _edit_source(s, "fix")
    _, context = s.review(DIFF, gate.reply(DIFF, "APPROVE"))

    assert "round 1: APPROVED" in context
    assert _gates(s, "timing_refused") == []


@pytest.mark.parametrize("seconds", [3600, 600])
def test_timing_edits_after_wrap_up_are_refused_without_stopping(
    tmp_path: Path, seconds: int
) -> None:
    s = _timed(tmp_path)
    _plan_approved(s)
    _open_round_earlier(s)
    # Wrap up started 30 seconds ago; the deadline is still ahead.
    _read_timing(s, deadline_in=seconds // 12 - 30, seconds=seconds)

    reason = gate.deny_reason(s.edit("Edit", SOURCE))

    assert not reason.startswith("STOP.")
    assert _timing_refusals(s) == [("edit", "wrap_up")]
    assert _gates(s, "edit_refused") == []
    assert _state(s)["stopped"] is None


@pytest.mark.parametrize("seconds", [3600, 600])
def test_timing_diff_review_after_wrap_up_stops_the_run(tmp_path: Path, seconds: int) -> None:
    s = _timed(tmp_path)
    _plan_approved(s)
    _edit_source(s, "fix")
    _open_round_earlier(s)
    # Wrap up started 30 seconds ago; the deadline is still ahead.
    _read_timing(s, deadline_in=seconds // 12 - 30, seconds=seconds)

    reason = _deny(s.pre("Agent", DIFF_INPUT))

    assert reason.startswith("STOP.")
    assert _timing_refusals(s) == [("review_diff", "wrap_up")]
    assert [p for p in s.phases() if p[0] == "review_diff"] == []
    state = _state(s)
    assert state["stopped"]
    assert state["diff"]["round"] == 0


def test_timing_an_approved_checkout_publishes_after_wrap_up_before_the_deadline(
    tmp_path: Path,
) -> None:
    s = _timed(tmp_path)
    _plan_approved(s)
    _edit_source(s, "fix")
    s.review(DIFF, gate.reply(DIFF, "APPROVE"))
    _open_round_earlier(s)
    # Wrap up started 60 seconds ago; the deadline is 240 seconds ahead.
    _read_timing(s, deadline_in=300 - 60, seconds=3600)

    assert s.pre(PUBLISH)["permissionDecision"] == "allow"
    assert _gates(s, "timing_refused") == []


def test_timing_publish_after_the_deadline_is_refused(tmp_path: Path) -> None:
    s = _timed(tmp_path)
    _plan_approved(s)
    _edit_source(s, "fix")
    s.review(DIFF, gate.reply(DIFF, "APPROVE"))
    _open_round_earlier(s)
    _read_timing(s, deadline_in=-60, seconds=3600)

    reason = _deny(s.pre(PUBLISH))

    assert reason.startswith("STOP.")
    assert _timing_refusals(s) == [("publish", "deadline_passed")]
    assert _state(s)["stopped"]


def test_timing_bash_before_wrap_up_is_bounded_by_it(tmp_path: Path) -> None:
    s = _prompted(tmp_path, auto_issue_read=False)
    # Wrap up is 900 seconds ahead: no command may run past it.
    wrap = _read_timing(s, deadline_in=300 + 900, seconds=3600) - 300

    started = _now()
    long = s.pre("Bash", {"command": "uv run pytest -q", "timeout": 3_600_000})
    ended = _now()
    short = s.pre("Bash", {"command": "ls", "timeout": 60_000})
    plain = s.pre("Bash", {"command": "ls"})

    assert long["permissionDecision"] == "allow", long
    assert long["updatedInput"]["run_in_background"] is False
    timeout = long["updatedInput"]["timeout"]
    assert (wrap - ended) * 1000 - 3000 <= timeout <= (wrap - started) * 1000 + 3000
    # A shorter request is kept, and the default (120 s) already ends before wrap up.
    assert short["updatedInput"]["timeout"] == 60_000
    assert "timeout" not in plain["updatedInput"]
    assert _gates(s, "timing_refused") == []


def test_timing_bash_omitted_timeout_never_outlasts_wrap_up(tmp_path: Path) -> None:
    s = _prompted(tmp_path, auto_issue_read=False)
    # Wrap up is 60 seconds ahead, inside the 120 s default timeout.
    wrap = _read_timing(s, deadline_in=300 + 60, seconds=3600) - 300

    started = _now()
    out = s.pre("Bash", {"command": "uv run pytest -q"})
    ended = _now()

    assert out["permissionDecision"] == "allow", out
    timeout = out["updatedInput"]["timeout"]
    assert (wrap - ended) * 1000 - 3000 <= timeout <= (wrap - started) * 1000 + 3000
    assert timeout < 120_000


def test_timing_bash_after_wrap_up_is_refused_without_stopping(tmp_path: Path) -> None:
    s = _prompted(tmp_path, auto_issue_read=False)
    # Wrap up started 30 seconds ago; the deadline is still ahead.
    _read_timing(s, deadline_in=300 - 30, seconds=3600)

    reason = _deny(s.pre("Bash", {"command": "ls"}))

    assert not reason.startswith("STOP.")
    assert _timing_refusals(s) == [("bash", "wrap_up")]
    assert _state(s)["stopped"] is None


def test_timing_a_warm_ci_round_after_the_round_cutoff_stops_with_the_failing_checks(
    tmp_path: Path,
) -> None:
    s = _timed(tmp_path)
    _plan_approved(s)
    _edit_source(s, "fix")
    s.review(DIFF, gate.reply(DIFF, "APPROVE"))
    assert s.pre(PUBLISH)["permissionDecision"] == "allow"
    _open_round_earlier(s)
    # CI comes back after the round cutoff: no new round may open.
    _read_timing(s, deadline_in=600 - 60, seconds=3600)

    context = gate.context_of(s.fire("UserPromptSubmit", prompt=_ci_text(2)))

    assert context.startswith("STOP.")
    assert FAILING_CHECK in context
    assert ("open_round", "round_cutoff") in _timing_refusals(s)
    assert _state(s)["stopped"]
    assert not gate.allowed(s.edit("Edit", SOURCE))
    # The untrusted CI data never reaches the pod log.
    assert FAILING_CHECK not in s.log.read_text()


def test_timing_a_cold_ci_round_opened_after_the_round_cutoff_stops(tmp_path: Path) -> None:
    s = _session(tmp_path, auto_issue_read=False)
    assert "CI fix round" in gate.context_of(s.fire("UserPromptSubmit", prompt=_ci_text(2)))
    # The issue read arrives after the round opened; its round cutoff passed 60 s ago.
    _read_timing(s, deadline_in=600 - 60, seconds=3600)

    reason = gate.deny_reason(s.edit("Edit", SOURCE))

    assert reason.startswith("STOP.")
    assert FAILING_CHECK in reason
    assert "round_cutoff" in {cause for _, cause in _timing_refusals(s)}
    assert _state(s)["stopped"]
    assert _deny(s.pre("Agent", DIFF_INPUT)).startswith("STOP.")


# Exact cutoff instants, in process: the hook's UTC clock is set to the second.
# (execution seconds, seconds before the deadline)
_ROUND_CUTOFFS = [(3600, 600), (600, 100)]
_WRAP_CUTOFFS = [(3600, 300), (600, 50)]


class _Hook:
    """The hook in process on a real checkout, its UTC clock under test control."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, seconds: int) -> None:
        self.module = gate._load_hook(
            monkeypatch, tmp_path, f"dark_factory_review_policy_clock_{seconds}"
        )
        monkeypatch.delenv("BASH_DEFAULT_TIMEOUT_MS", raising=False)
        assert callable(getattr(self.module, "_utc_now", None)), "the hook reads UTC via _utc_now"
        self.now = DEADLINE - seconds
        monkeypatch.setattr(self.module, "_utc_now", lambda: self.now)
        self.seconds = seconds
        self.repo = gate.make_repo(tmp_path / "repo")
        self.log = tmp_path / "pod.log"
        self.state_dir = tmp_path / "state"

    def fire(self, session_id: str, event: str, **fields: Any) -> dict[str, Any] | None:
        out: dict[str, Any] | None = self.module.handle(
            {"session_id": session_id, "hook_event_name": event, "cwd": str(self.repo), **fields}
        )
        return out

    def decision(self, session_id: str, tool: str, tool_input: dict[str, Any]) -> dict[str, Any]:
        out = self.fire(session_id, "PreToolUse", tool_name=tool, tool_input=tool_input)
        assert out is not None, (tool, tool_input)
        decision: dict[str, Any] = out["hookSpecificOutput"]
        return decision

    def start(self, session_id: str) -> None:
        """A fresh run at the start of the execution: prompt, issue read, workflow."""
        self.now = DEADLINE - self.seconds
        prompt = self.fire(session_id, "UserPromptSubmit", prompt=ISSUE)
        assert gate.contract() in gate.context_of(prompt)
        payload = gate.issue_payload(_at(DEADLINE), self.seconds)
        self.fire(
            session_id,
            "PostToolUse",
            tool_name=gate.ISSUE_READ,
            tool_input={},
            tool_response=gate.issue_response(payload),
        )
        self.fire(
            session_id, "PostToolUse", tool_name="Skill", tool_input={"skill": gate.WORKFLOW_SKILL}
        )

    def review(self, session_id: str, kind: str, text: str) -> str:
        pre = self.decision(
            session_id, "Agent", {"subagent_type": kind, "description": "r", "prompt": "p"}
        )
        assert pre["permissionDecision"] == "allow", pre
        out = self.fire(
            session_id,
            "PostToolUse",
            tool_name="Agent",
            tool_input=pre["updatedInput"],
            tool_response={"content": [{"type": "text", "text": text}]},
        )
        assert out is not None
        return str(out["hookSpecificOutput"]["additionalContext"])

    def implementing(self, session_id: str) -> None:
        self.start(session_id)
        assert "APPROVED" in self.review(session_id, PLAN, gate.reply(PLAN, "APPROVE"))
        report = {"phase": "implement", "round": 1}
        assert self.fire(session_id, "PreToolUse", tool_name=REPORT, tool_input=report) is None

    def state(self, session_id: str) -> dict[str, Any]:
        state: dict[str, Any] = json.loads((self.state_dir / f"{session_id}.json").read_text())
        return state

    def timing_refusals(self) -> list[tuple[str, str]]:
        lines = [json.loads(line) for line in self.log.read_text().splitlines()]
        return [
            (line["action"], line["cause"])
            for line in lines
            if line.get("curie_gate") == "timing_refused"
        ]


@pytest.mark.parametrize(("seconds", "before_deadline"), _ROUND_CUTOFFS)
def test_timing_plan_review_at_the_round_cutoff_stops_the_run(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, seconds: int, before_deadline: int
) -> None:
    hook = _Hook(monkeypatch, tmp_path, seconds)
    hook.start("before")
    hook.now = DEADLINE - before_deadline - 1
    assert hook.decision("before", "Agent", PLAN_INPUT)["permissionDecision"] == "allow"

    hook.start("at")
    hook.now = DEADLINE - before_deadline
    reason = _deny(hook.decision("at", "Agent", PLAN_INPUT))

    assert reason.startswith("STOP.")
    assert hook.timing_refusals() == [("plan_review", "round_cutoff")]
    state = hook.state("at")
    assert state["stopped"]
    assert state["plan"] == {"round": 0, "verdict": None}


@pytest.mark.parametrize(("seconds", "before_deadline"), _WRAP_CUTOFFS)
def test_timing_diff_review_and_edits_at_wrap_up(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, seconds: int, before_deadline: int
) -> None:
    hook = _Hook(monkeypatch, tmp_path, seconds)
    edit = gate.edit_input("Edit", str(hook.repo / SOURCE))
    for session_id in ("before", "at"):
        hook.implementing(session_id)

    hook.now = DEADLINE - before_deadline - 1
    assert hook.fire("before", "PreToolUse", tool_name="Edit", tool_input=edit) is None
    assert hook.decision("before", "Agent", DIFF_INPUT)["permissionDecision"] == "allow"

    hook.now = DEADLINE - before_deadline
    edit_reason = gate.deny_reason(hook.fire("at", "PreToolUse", tool_name="Edit", tool_input=edit))
    assert not edit_reason.startswith("STOP.")
    assert hook.state("at")["stopped"] is None
    diff_reason = _deny(hook.decision("at", "Agent", DIFF_INPUT))
    assert diff_reason.startswith("STOP.")
    assert hook.timing_refusals() == [("edit", "wrap_up"), ("review_diff", "wrap_up")]


@pytest.mark.parametrize(("seconds", "before_deadline"), _WRAP_CUTOFFS)
def test_timing_bash_at_wrap_up_is_refused_and_bounded_just_before_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, seconds: int, before_deadline: int
) -> None:
    hook = _Hook(monkeypatch, tmp_path, seconds)
    hook.start("run")

    hook.now = DEADLINE - before_deadline - 1
    requested = hook.decision("run", "Bash", {"command": "ls", "timeout": 600_000})
    omitted = hook.decision("run", "Bash", {"command": "ls"})
    assert requested["permissionDecision"] == "allow"
    assert requested["updatedInput"]["timeout"] == 1000
    assert omitted["updatedInput"]["timeout"] == 1000

    hook.now = DEADLINE - before_deadline
    reason = _deny(hook.decision("run", "Bash", {"command": "ls"}))
    assert not reason.startswith("STOP.")
    assert hook.timing_refusals() == [("bash", "wrap_up")]
    assert hook.state("run")["stopped"] is None


@pytest.mark.parametrize("seconds", [3600, 600])
def test_timing_publish_at_the_deadline_is_refused(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, seconds: int
) -> None:
    hook = _Hook(monkeypatch, tmp_path, seconds)
    for session_id in ("before", "at"):
        hook.implementing(session_id)
        assert "APPROVED" in hook.review(session_id, DIFF, gate.reply(DIFF, "APPROVE"))

    hook.now = DEADLINE - 1
    assert hook.decision("before", PUBLISH, {})["permissionDecision"] == "allow"
    hook.now = DEADLINE
    reason = _deny(hook.decision("at", PUBLISH, {}))

    assert reason.startswith("STOP.")
    assert hook.timing_refusals() == [("publish", "deadline_passed")]
    assert hook.state("at")["stopped"]


# --- AC5: an unchanged candidate is not reviewed twice -------------------------


def test_unchanged_candidate_re_review_is_refused_without_spending_a_round(
    tmp_path: Path,
) -> None:
    s = _prompted(tmp_path)
    _plan_approved(s)
    _edit_source(s, "fix")
    assert "round 2 of 5" in _diff_changes(s, FINDING)
    phases = s.phases()

    reason = _deny(s.pre("Agent", DIFF_INPUT))

    assert not reason.startswith("STOP.")
    assert FINDING in reason
    assert _review_refusals(s) == [("no_progress", "review_diff")]
    assert s.phases() == phases
    state = _state(s)
    assert state["diff"] == {"round": 1, "verdict": "CHANGES"}
    assert state["stopped"] is None
    assert FINDING not in s.log.read_text()


def test_unchanged_candidate_after_a_real_change_is_reviewed_and_counts(tmp_path: Path) -> None:
    s = _prompted(tmp_path)
    _plan_approved(s)
    _edit_source(s, "fix")
    _diff_changes(s, FINDING)

    _edit_source(s, "rework")
    _, context = s.review(DIFF, gate.reply(DIFF, "APPROVE"))

    assert "round 2: APPROVED" in context
    assert _state(s)["diff"] == {"round": 2, "verdict": "APPROVE"}
    assert _gates(s, "review_refused") == []
    assert s.pre(PUBLISH)["permissionDecision"] == "allow"


def test_unchanged_candidate_restored_to_the_rejected_bytes_is_refused_again(
    tmp_path: Path,
) -> None:
    s = _prompted(tmp_path)
    _plan_approved(s)
    _edit_source(s, "fix")
    rejected = (s.cwd / SOURCE).read_text()
    _diff_changes(s, FINDING)

    _edit_source(s, "another attempt")
    _edit_source(s, "fix")
    assert (s.cwd / SOURCE).read_text() == rejected
    reason = _deny(s.pre("Agent", DIFF_INPUT))

    assert FINDING in reason
    assert _review_refusals(s) == [("no_progress", "review_diff")]
    assert _state(s)["diff"]["round"] == 1
    # The other candidate is still reviewable.
    _edit_source(s, "another attempt")
    assert s.pre("Agent", DIFF_INPUT)["permissionDecision"] == "allow"


# --- AC6: findings kept for every refusal ---------------------------------------


@pytest.mark.parametrize(
    "cause", ["round_cap", "round_cutoff", "wrap_up", "deadline_passed", "no_progress"]
)
def test_retained_findings_reach_every_refusal(tmp_path: Path, cause: str) -> None:
    s = _timed(tmp_path)
    _plan_approved(s)
    _edit_source(s, "fix")
    if cause == "round_cap":
        assert FINDING in _capped(s, FINDING)
        (s.cwd / SOURCE).write_text("def add(a, b):\n    return b + a\n")
    elif cause == "no_progress":
        _diff_changes(s, FINDING)
    else:
        _diff_changes(s, FINDING)
        _edit_source(s, "rework")
        if cause == "round_cutoff":
            # The open round began after the cutoff, which passed 60 seconds ago.
            _read_timing(s, deadline_in=600 - 60, seconds=3600)
        elif cause == "wrap_up":
            _open_round_earlier(s)
            _read_timing(s, deadline_in=300 - 60, seconds=3600)
        else:
            _open_round_earlier(s)
            _read_timing(s, deadline_in=-60, seconds=3600)

    reason = _deny(s.pre("Agent", DIFF_INPUT))

    assert FINDING in reason
    if cause == "no_progress":
        assert not reason.startswith("STOP.")
    else:
        assert reason.startswith("STOP.") and "Could not complete:" in reason
    if cause in ("wrap_up", "deadline_passed"):
        assert _timing_refusals(s) == [("review_diff", cause)]
    if cause == "round_cutoff":
        assert "round_cutoff" in {c for _, c in _timing_refusals(s)}
    assert FINDING not in s.log.read_text()


def test_retained_findings_from_the_plan_review_reach_a_later_stop(tmp_path: Path) -> None:
    s = _timed(tmp_path)
    plan_finding = "the plan never names the negative input case"
    s.review(PLAN, changes(PLAN, plan_finding))
    _open_round_earlier(s)
    _read_timing(s, deadline_in=-60, seconds=3600)

    reason = _deny(s.pre(PUBLISH))

    assert reason.startswith("STOP.")
    assert plan_finding in reason
    assert plan_finding not in s.log.read_text()


def test_retained_findings_are_resolved_by_a_diff_approval(tmp_path: Path) -> None:
    s = _timed(tmp_path)
    _plan_approved(s)
    _edit_source(s, "fix")
    _diff_changes(s, FINDING)
    _edit_source(s, "rework")
    _, context = s.review(DIFF, gate.reply(DIFF, "APPROVE"))
    assert "APPROVED" in context
    assert _state(s)["findings"] is None
    _open_round_earlier(s)
    _read_timing(s, deadline_in=-60, seconds=3600)

    reason = _deny(s.pre(PUBLISH))

    assert reason.startswith("STOP.")
    assert FINDING not in reason


# --- AC6: the run continues across prompts in one session -----------------------


@pytest.mark.parametrize("framed", [True, False], ids=["framed", "unframed"])
def test_continuation_a_new_message_keeps_rounds_planning_and_findings(
    tmp_path: Path, framed: bool
) -> None:
    s = _timed(tmp_path)
    s.review(PLAN, changes(PLAN, "the plan skips criterion 3"))
    s.report("implement", 1)
    _edit_source(s, "fix")
    _diff_changes(s, FINDING)
    before = _state(s)
    text = "Please also keep add(0, 0) at zero."
    prompt = (
        frame_user_turn("message", "jira_cloud:account:Reviewer", text, "jira") if framed else text
    )

    context = gate.context_of(s.fire("UserPromptSubmit", prompt=prompt))

    assert gate.contract() in context and not context.startswith("STOP.")
    after = _state(s)
    for key in ("plan", "diff", "stage", "stopped", "findings", "timing", "fingerprint"):
        assert after[key] == before[key], key
    # Planning stays complete, and the rejected candidate stays rejected.
    _deny(s.pre("Agent", PLAN_INPUT))
    assert FINDING in _deny(s.pre("Agent", DIFF_INPUT))
    assert _review_refusals(s) == [
        ("planning_complete", "plan_review"),
        ("no_progress", "review_diff"),
    ]
    _edit_source(s, "rework")
    assert "round 3 of 5" in _diff_changes(s, "second finding")


def test_continuation_a_stopped_run_stays_stopped_with_its_findings(tmp_path: Path) -> None:
    s = _prompted(tmp_path)
    _plan_approved(s)
    _capped(s, FINDING)
    before = _state(s)

    context = gate.context_of(s.fire("UserPromptSubmit", prompt="Try once more."))

    assert context.startswith("STOP.") and FINDING in context
    after = _state(s)
    for key in ("stopped", "findings", "diff"):
        assert after[key] == before[key], key
    (s.cwd / SOURCE).write_text("def add(a, b):\n    return b + a\n")
    assert _deny(s.pre("Agent", DIFF_INPUT)).startswith("STOP.")
    assert s.pre(PUBLISH)["permissionDecision"] == "deny"


def test_continuation_a_ci_marker_keeps_a_stop_and_its_findings(tmp_path: Path) -> None:
    s = _prompted(tmp_path)
    _plan_approved(s)
    _capped(s, FINDING)
    before = _state(s)

    context = gate.context_of(s.fire("UserPromptSubmit", prompt=_ci_text(2)))

    assert context.startswith("STOP.") and FINDING in context
    after = _state(s)
    for key in ("stopped", "findings", "diff"):
        assert after[key] == before[key], key
    assert s.pre(PUBLISH)["permissionDecision"] == "deny"


def test_continuation_a_ci_marker_opens_one_repair_round_in_the_same_session(
    tmp_path: Path,
) -> None:
    s = _prompted(tmp_path)
    _plan_approved(s)
    _edit_source(s, "fix")
    s.review(DIFF, gate.reply(DIFF, "APPROVE"))
    assert s.pre(PUBLISH)["permissionDecision"] == "allow"

    context = gate.context_of(s.fire("UserPromptSubmit", prompt=_ci_text(2)))

    assert context.startswith("CI fix round") and gate.contract() in context
    state = _state(s)
    assert state["diff"] == {"round": 0, "verdict": None}
    assert state["stopped"] is None
    assert "wait_ci" in state["findings"]["source"]
    assert FAILING_CHECK in state["findings"]["text"]
    assert [p for p in s.phases() if p[0] == "wait_ci"] == [("wait_ci", 2)]
    assert s.pre(PUBLISH)["permissionDecision"] == "deny"
    _edit_source(s, "ci fix")
    _, context = s.review(DIFF, gate.reply(DIFF, "APPROVE"))
    assert "round 1: APPROVED" in context
    assert FAILING_CHECK not in s.log.read_text()


def test_continuation_follows_the_curie_session_across_a_rotated_sdk_session(
    tmp_path: Path,
) -> None:
    s = _session(tmp_path)
    s.env["CURIE_SESSION_ID"] = "curie-run-1"
    assert gate.contract() in gate.context_of(s.fire("UserPromptSubmit", prompt=ISSUE))
    s.load_workflow()
    _plan_approved(s)
    _edit_source(s, "fix")
    _diff_changes(s, FINDING)
    before = _state(s, "curie-run-1")

    # The runner rotated the SDK session on a reset; the Curie session is the same.
    context = gate.context_of(
        s.fire("UserPromptSubmit", prompt="carry on", session_id="sdk-rotated")
    )

    assert gate.contract() in context
    after = _state(s, "curie-run-1")
    for key in ("plan", "diff", "stopped", "findings", "fingerprint"):
        assert after[key] == before[key], key
    state_dir = Path(s.env["DARK_FACTORY_STATE_DIR"])
    for sdk_session in (STATE_KEY, "sdk-rotated"):
        assert not (state_dir / f"{sdk_session}.json").exists(), sdk_session
    refused = s.fire("PreToolUse", tool_name="Agent", tool_input=PLAN_INPUT, session_id="sdk-r2")
    assert refused is not None
    _deny(refused["hookSpecificOutput"])
    assert _review_refusals(s) == [("planning_complete", "plan_review")]


def test_continuation_a_blank_curie_session_keys_on_the_hook_session(tmp_path: Path) -> None:
    s = _session(tmp_path)
    s.env["CURIE_SESSION_ID"] = "   "

    assert gate.contract() in gate.context_of(s.fire("UserPromptSubmit", prompt=ISSUE))

    state_dir = Path(s.env["DARK_FACTORY_STATE_DIR"])
    assert _state(s)["workspace"] == os.path.realpath(s.cwd)
    assert not (state_dir / "___.json").exists()


def test_continuation_a_new_curie_session_starts_fresh(tmp_path: Path) -> None:
    s = _session(tmp_path)
    s.env["CURIE_SESSION_ID"] = "curie-run-1"
    assert gate.contract() in gate.context_of(s.fire("UserPromptSubmit", prompt=ISSUE))
    s.load_workflow()
    _plan_approved(s)
    _edit_source(s, "fix")
    s.review(DIFF, gate.reply(DIFF, "APPROVE"))
    assert s.pre(PUBLISH)["permissionDecision"] == "allow"

    s.env["CURIE_SESSION_ID"] = "curie-run-2"
    assert gate.contract() in gate.context_of(s.fire("UserPromptSubmit", prompt=ISSUE))

    state = _state(s, "curie-run-2")
    assert state["plan"] == {"round": 0, "verdict": None}
    assert state["diff"] == {"round": 0, "verdict": None}
    assert state["stopped"] is None
    assert s.pre(PUBLISH)["permissionDecision"] == "deny"
    assert "run the plan review first" in _deny(s.pre("Agent", DIFF_INPUT))
    s.load_workflow()
    assert s.pre("Agent", PLAN_INPUT)["permissionDecision"] == "allow"
    assert s.phases()[-1] == ("plan_review", 1)


def test_continuation_a_prompt_in_another_repository_starts_fresh(tmp_path: Path) -> None:
    s = _prompted(tmp_path)
    _plan_approved(s)
    _edit_source(s, "fix")
    s.review(DIFF, gate.reply(DIFF, "APPROVE"))
    other = gate.make_repo(tmp_path / "other")

    context = gate.context_of(s.fire("UserPromptSubmit", prompt=ISSUE, cwd=str(other)))

    assert gate.contract() in context
    state = _state(s)
    assert state["workspace"] == os.path.realpath(other)
    assert state["plan"] == {"round": 0, "verdict": None}
    assert state["diff"] == {"round": 0, "verdict": None}
    assert "run the plan review first" in _deny(s.pre("Agent", DIFF_INPUT))


def test_continuation_a_prompt_in_a_subdirectory_of_the_workspace_continues(
    tmp_path: Path,
) -> None:
    s = _prompted(tmp_path)
    _plan_approved(s)
    before = _state(s)

    context = gate.context_of(s.fire("UserPromptSubmit", prompt="go on", cwd=str(s.cwd / "pkg")))

    assert gate.contract() in context
    after = _state(s)
    assert after["workspace"] == before["workspace"] == os.path.realpath(s.cwd)
    assert after["plan"] == before["plan"] == {"round": 1, "verdict": "APPROVE"}
    _deny(s.pre("Agent", PLAN_INPUT))
    assert _review_refusals(s) == [("planning_complete", "plan_review")]


def test_continuation_a_run_started_in_a_subdirectory_keeps_its_stop_at_the_checkout_root(
    tmp_path: Path,
) -> None:
    s = _session(tmp_path)
    first = s.fire("UserPromptSubmit", prompt=ISSUE, cwd=str(s.cwd / "pkg"))
    assert gate.contract() in gate.context_of(first)
    # The run belongs to the checkout, not to the directory its first prompt came from.
    assert _state(s)["workspace"] == os.path.realpath(s.cwd)
    s.load_workflow()
    _plan_approved(s)
    assert _capped(s, FINDING).startswith("STOP.")

    context = gate.context_of(s.fire("UserPromptSubmit", prompt="go on", cwd=str(s.cwd)))

    assert context.startswith("STOP.") and FINDING in context
    state = _state(s)
    assert state["workspace"] == os.path.realpath(s.cwd)
    assert "others" not in state
    assert state["stopped"]
    assert state["diff"]["round"] == 5
    assert state["plan"] == {"round": 1, "verdict": "APPROVE"}
    assert FINDING in state["findings"]["text"]
    diff = _deny(s.pre("Agent", DIFF_INPUT))
    assert diff.startswith("STOP.") and FINDING in diff


def test_continuation_a_sibling_directory_of_the_same_checkout_continues(tmp_path: Path) -> None:
    s = _session(tmp_path)
    s.fire("UserPromptSubmit", prompt=ISSUE, cwd=str(s.cwd / "pkg"))
    s.load_workflow()
    _plan_approved(s)
    _edit_source(s, "fix")
    _diff_changes(s, FINDING)

    context = gate.context_of(s.fire("UserPromptSubmit", prompt="go on", cwd=str(s.cwd / "tests")))

    assert gate.contract() in context and not context.startswith("STOP.")
    state = _state(s)
    assert state["workspace"] == os.path.realpath(s.cwd)
    assert state["plan"] == {"round": 1, "verdict": "APPROVE"}
    assert state["diff"] == {"round": 1, "verdict": "CHANGES"}
    assert FINDING in state["findings"]["text"]
    # The rejected candidate stays rejected; a real change is the next round.
    assert FINDING in _deny(s.pre("Agent", DIFF_INPUT))
    assert _review_refusals(s)[-1] == ("no_progress", "review_diff")
    _edit_source(s, "rework")
    assert "round 3 of 5" in _diff_changes(s, "second finding")


def _prompt_from(s: Any, checkout: Path, prompt: str) -> str:
    """A prompt whose cwd is ``checkout``; later hook events come from there too."""
    s.cwd = checkout
    return str(gate.context_of(s.fire("UserPromptSubmit", prompt=prompt)))


def _nested_checkout(outer: Path) -> Path:
    """A checkout inside ``outer`` that ``outer`` ignores, so both stay fingerprintable."""
    (outer / ".git" / "info" / "exclude").write_text("nested/\n")
    return Path(gate.make_repo(outer / "nested"))


def test_continuation_a_nested_checkout_keeps_its_own_stop(tmp_path: Path) -> None:
    s = _session(tmp_path)
    outer = s.cwd
    nested = _nested_checkout(outer)
    assert gate.contract() in _prompt_from(s, nested, ISSUE)
    s.load_workflow()
    _plan_approved(s)
    assert _capped(s, FINDING).startswith("STOP.")

    # The enclosing checkout is another checkout: a fresh run of its own.
    context = _prompt_from(s, outer, ISSUE)
    assert gate.contract() in context and not context.startswith("STOP.")
    state = _state(s)
    assert state["workspace"] == os.path.realpath(outer)
    assert state["plan"] == {"round": 0, "verdict": None}
    assert state["stopped"] is None

    # Back in the nested checkout: its stop, spent rounds and findings, not the outer run.
    context = _prompt_from(s, nested, "go on")
    assert context.startswith("STOP.") and FINDING in context
    state = _state(s)
    assert state["workspace"] == os.path.realpath(nested)
    assert state["stopped"]
    assert state["diff"]["round"] == 5
    assert FINDING in state["findings"]["text"]
    diff = _deny(s.pre("Agent", DIFF_INPUT))
    assert diff.startswith("STOP.") and FINDING in diff


def test_continuation_a_nested_checkout_never_continues_its_parents_run(tmp_path: Path) -> None:
    s = _session(tmp_path)
    outer = s.cwd
    nested = _nested_checkout(outer)
    assert gate.contract() in _prompt_from(s, outer, ISSUE)
    s.load_workflow()
    _plan_approved(s)
    assert _capped(s, FINDING).startswith("STOP.")

    context = _prompt_from(s, nested, ISSUE)

    assert gate.contract() in context and not context.startswith("STOP.")
    state = _state(s)
    assert state["workspace"] == os.path.realpath(nested)
    assert state["plan"] == {"round": 0, "verdict": None}
    assert state["stopped"] is None
    # The parent's stop is kept for its own checkout.
    context = _prompt_from(s, outer, "go on")
    assert context.startswith("STOP.") and FINDING in context
    assert _state(s)["diff"]["round"] == 5


def test_continuation_recovers_a_missing_fingerprint_before_planning(tmp_path: Path) -> None:
    s = _session(tmp_path)
    readable = s.env["PATH"]
    gate._prepend_git_shim(s, "import sys\nsys.exit(1)\n")
    context = gate.context_of(s.fire("UserPromptSubmit", prompt=ISSUE))
    assert context.startswith("STOP.") and "new prompt" in context.lower()

    # Git is still unreadable: a later prompt keeps the stop.
    context = gate.context_of(s.fire("UserPromptSubmit", prompt="retry"))
    assert context.startswith("STOP.") and "new prompt" in context.lower()
    assert _state(s)["stopped"]

    s.env["PATH"] = readable
    context = gate.context_of(s.fire("UserPromptSubmit", prompt="retry"))

    assert gate.contract() in context and not context.startswith("STOP.")
    state = _state(s)
    assert state["stopped"] is None
    assert state["fingerprint"] is not None
    s.load_workflow()
    _, context = s.review(PLAN, gate.reply(PLAN, "APPROVE"))
    assert "round 1: APPROVED" in context


def test_continuation_each_workspace_keeps_its_own_run_in_one_session(tmp_path: Path) -> None:
    s = _prompted(tmp_path)
    repo_a = s.cwd
    repo_b = gate.make_repo(tmp_path / "other")
    _plan_approved(s)
    assert _capped(s, FINDING).startswith("STOP.")

    # Workspace B in the same session: a fresh run of its own.
    context = gate.context_of(s.fire("UserPromptSubmit", prompt=ISSUE, cwd=str(repo_b)))
    assert gate.contract() in context and not context.startswith("STOP.")
    state = _state(s)
    assert state["workspace"] == os.path.realpath(repo_b)
    assert state["stopped"] is None
    assert state["plan"] == {"round": 0, "verdict": None}
    s.load_workflow()
    _, context = s.review(PLAN, gate.reply(PLAN, "APPROVE"))
    assert "round 1: APPROVED" in context

    # Back to A: its stop, its spent rounds and its findings are restored.
    context = gate.context_of(s.fire("UserPromptSubmit", prompt="back to A", cwd=str(repo_a)))
    assert context.startswith("STOP.") and FINDING in context
    state = _state(s)
    assert state["workspace"] == os.path.realpath(repo_a)
    assert state["stopped"]
    assert state["diff"]["round"] == 5
    assert FINDING in state["findings"]["text"]
    (repo_a / SOURCE).write_text("def add(a, b):\n    return b + a\n")
    diff = _deny(s.pre("Agent", DIFF_INPUT))
    assert diff.startswith("STOP.") and FINDING in diff
    edit = gate.deny_reason(s.edit("Edit", SOURCE))
    assert edit.startswith("STOP.") and FINDING in edit

    # Back to B: its completed plan review is restored, and nothing of A's stop.
    context = gate.context_of(s.fire("UserPromptSubmit", prompt="back to B", cwd=str(repo_b)))
    assert gate.contract() in context and not context.startswith("STOP.")
    state = _state(s)
    assert state["workspace"] == os.path.realpath(repo_b)
    assert state["plan"] == {"round": 1, "verdict": "APPROVE"}
    assert state["stopped"] is None
    _deny(s.pre("Agent", PLAN_INPUT))
    assert ("planning_complete", "plan_review") in _review_refusals(s)


def test_continuation_returning_to_a_workspace_restores_its_spent_rounds(tmp_path: Path) -> None:
    s = _prompted(tmp_path)
    repo_a = s.cwd
    repo_b = gate.make_repo(tmp_path / "other")
    s.review(PLAN, changes(PLAN, "the plan skips criterion 3"))
    s.report("implement", 1)
    _edit_source(s, "fix")
    _diff_changes(s, FINDING)

    s.fire("UserPromptSubmit", prompt=ISSUE, cwd=str(repo_b))
    assert _state(s)["plan"] == {"round": 0, "verdict": None}
    context = gate.context_of(s.fire("UserPromptSubmit", prompt="back to A", cwd=str(repo_a)))

    assert gate.contract() in context and not context.startswith("STOP.")
    state = _state(s)
    assert state["workspace"] == os.path.realpath(repo_a)
    assert state["plan"] == {"round": 1, "verdict": "CHANGES"}
    assert state["diff"] == {"round": 1, "verdict": "CHANGES"}
    assert state["stopped"] is None
    assert FINDING in state["findings"]["text"]
    # Planning stays complete and the rejected candidate stays rejected.
    _deny(s.pre("Agent", PLAN_INPUT))
    assert FINDING in _deny(s.pre("Agent", DIFF_INPUT))
    assert _review_refusals(s)[-2:] == [
        ("planning_complete", "plan_review"),
        ("no_progress", "review_diff"),
    ]
    _edit_source(s, "rework")
    assert "round 3 of 5" in _diff_changes(s, "second finding")


# --- AC6: an accepted edit after approval clears it -----------------------------


def test_approval_after_edit_clears_it_until_a_fresh_review(tmp_path: Path) -> None:
    s = _prompted(tmp_path)
    _plan_approved(s)
    _edit_source(s, "fix")
    s.review(DIFF, gate.reply(DIFF, "APPROVE"))
    assert s.pre(PUBLISH)["permissionDecision"] == "allow"

    # An accepted edit, even one that writes the same bytes back.
    _edit_source(s, "fix")

    assert [e["cause"] for e in _gates(s, "diff_approval_cleared")] == ["edit"]
    assert _state(s)["diff"]["verdict"] is None
    assert s.pre(PUBLISH)["permissionDecision"] == "deny"
    _, context = s.review(DIFF, gate.reply(DIFF, "APPROVE"))
    assert "round 2: APPROVED" in context
    assert s.pre(PUBLISH)["permissionDecision"] == "allow"


def test_approval_after_edit_requested_but_never_run_keeps_it(tmp_path: Path) -> None:
    s = _prompted(tmp_path)
    _plan_approved(s)
    _edit_source(s, "fix")
    s.review(DIFF, gate.reply(DIFF, "APPROVE"))
    tool_input = {"file_path": str(s.cwd / SOURCE), "content": "def add(a, b):\n    return 0\n"}

    # Allowed by the hook, then refused by the runner: no PostToolUse follows.
    assert gate.allowed(s.edit_with("Write", tool_input))

    assert _gates(s, "diff_approval_cleared") == []
    assert _state(s)["diff"] == {"round": 1, "verdict": "APPROVE"}
    assert s.pre(PUBLISH)["permissionDecision"] == "allow"


# --- The root .github stays protected through symlinks --------------------------
# A path is refused when resolving it one component at a time, through any
# symlink, workspace alias or ``..``, steps into ``.github`` directly under the
# real workspace, whether that entry is a directory or a symlink.


def _assert_github_stop(s: Any, reason: str) -> None:
    assert reason.startswith("STOP.") and "Could not complete:" in reason, reason
    assert len(_gates(s, "github_path_refused")) == 1
    assert _state(s)["stopped"]
    assert [p for p in s.phases() if p[0] == "review_diff"] == []
    assert s.pre(PUBLISH)["permissionDecision"] == "deny"


@pytest.mark.parametrize("symlink", ["untracked", "retargeted"])
def test_protected_path_a_github_symlink_stops_the_diff_review(
    tmp_path: Path, symlink: str
) -> None:
    s = _session(tmp_path)
    github = s.cwd / ".github"
    if symlink == "retargeted":
        # The base commits a .github symlink into pkg; the run then retargets it.
        os.symlink("pkg", github)
        gate._git(s.cwd, "add", ".github")
        gate._git(s.cwd, "commit", "-q", "-m", "metadata symlink")
    assert gate.contract() in gate.context_of(s.fire("UserPromptSubmit", prompt=ISSUE))
    s.load_workflow()
    _plan_approved(s)
    if symlink == "retargeted":
        github.unlink()
        os.symlink("tests", github)
    else:
        os.symlink("pkg", github)

    reason = _deny(s.pre("Agent", DIFF_INPUT))

    _assert_github_stop(s, reason)


@pytest.mark.parametrize("tool", ["Edit", "Write"])
@pytest.mark.parametrize("stage", ["failing_test", "implement"])
def test_protected_path_an_edit_through_a_github_symlink_is_refused(
    tmp_path: Path, tool: str, stage: str
) -> None:
    s = _prompted(tmp_path)
    _, context = s.review(PLAN, gate.reply(PLAN, "APPROVE"))
    assert "APPROVED" in context
    allowed_path = TEST
    if stage == "implement":
        s.report("implement", 1)
        allowed_path = SOURCE
    # The same edit by its real path is allowed in this stage.
    assert gate.allowed(s.edit_with(tool, gate.edit_input(tool, s.abspath(allowed_path))))
    os.symlink("pkg", s.cwd / ".github")

    out = s.edit_with(tool, gate.edit_input(tool, str(s.cwd / ".github" / "calc.py")))

    _assert_github_stop(s, gate.deny_reason(out))


def test_protected_path_a_symlinked_workspace_prefix_still_guards_github(tmp_path: Path) -> None:
    s = _session(tmp_path)
    repo = s.cwd
    link = tmp_path / "link"
    link.symlink_to(repo, target_is_directory=True)
    # The run is started from the symlinked path; the hook stores the real one.
    s.cwd = link
    assert gate.contract() in gate.context_of(s.fire("UserPromptSubmit", prompt=ISSUE))
    s.load_workflow()
    _plan_approved(s)
    assert _state(s)["workspace"] == os.path.realpath(repo)

    # Ordinary source edits are allowed through the link and directly.
    for path in (link / SOURCE, repo / SOURCE):
        assert gate.allowed(s.edit_with("Edit", gate.edit_input("Edit", str(path))))
    assert _gates(s, "github_path_refused") == []

    workflow = link / ".github" / "workflows" / "ci.yaml"
    out = s.edit_with("Edit", gate.edit_input("Edit", str(workflow)))

    _assert_github_stop(s, gate.deny_reason(out))


@pytest.mark.parametrize("route", ["workspace alias", "link back to the root"])
def test_protected_path_combined_symlinks_never_escape_the_github_check(
    tmp_path: Path, route: str
) -> None:
    s = _prompted(tmp_path)
    _plan_approved(s)
    repo = s.cwd
    # The root .github is itself a symlink into an ordinary directory.
    os.symlink("pkg", repo / ".github")
    if route == "workspace alias":
        base = tmp_path / "alias"
        base.symlink_to(repo, target_is_directory=True)
    else:
        # A link inside the checkout that leads back to its root.
        os.symlink("..", repo / "pkg" / "up")
        base = repo / "pkg" / "up"

    # Ordinary source edits on the same route, and directly, stay allowed.
    for path in (base / SOURCE, repo / SOURCE):
        assert gate.allowed(s.edit_with("Edit", gate.edit_input("Edit", str(path))))
    assert _gates(s, "github_path_refused") == []

    out = s.edit_with("Edit", gate.edit_input("Edit", str(base / ".github" / "calc.py")))

    _assert_github_stop(s, gate.deny_reason(out))


def test_protected_path_a_link_inside_github_cannot_hide_a_path_that_lands_in_it(
    tmp_path: Path,
) -> None:
    s = _prompted(tmp_path)
    _plan_approved(s)
    github = s.cwd / ".github"
    (github / "nested" / "deep").mkdir(parents=True)
    (github / "workflows").mkdir()
    # Lexically ``workflows/ci.yaml``; resolved through the link, ``.github/workflows/ci.yaml``.
    os.symlink("nested/deep", github / "jump")

    # The same link leading out of .github to an ordinary file stays allowed.
    outward = github / "jump" / ".." / ".." / ".." / SOURCE
    assert gate.allowed(s.edit_with("Edit", gate.edit_input("Edit", str(outward))))
    assert _gates(s, "github_path_refused") == []

    inward = github / "jump" / ".." / ".." / "workflows" / "ci.yaml"
    out = s.edit_with("Edit", gate.edit_input("Edit", str(inward)))

    _assert_github_stop(s, gate.deny_reason(out))


def test_protected_path_a_github_symlink_whose_target_holds_a_link_cannot_hide_a_path(
    tmp_path: Path,
) -> None:
    s = _prompted(tmp_path)
    _plan_approved(s)
    repo = s.cwd
    os.symlink("pkg", repo / ".github")
    (repo / "pkg" / "nested" / "deep").mkdir(parents=True)
    os.symlink("nested/deep", repo / "pkg" / "jump")

    # Lexically leaving .github, through the same link to a file outside its target: allowed,
    # as is the target's file by its own path.
    outward = repo / ".github" / "jump" / ".." / ".." / ".." / TEST
    for path in (outward, repo / SOURCE):
        assert gate.allowed(s.edit_with("Edit", gate.edit_input("Edit", str(path))))
    assert _gates(s, "github_path_refused") == []

    # Lexically ``calc.py``; really inside the directory .github resolves to.
    inward = repo / ".github" / "jump" / ".." / ".." / "calc.py"
    out = s.edit_with("Edit", gate.edit_input("Edit", str(inward)))

    _assert_github_stop(s, gate.deny_reason(out))
