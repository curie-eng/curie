#!/usr/bin/env python3
"""Review gate for the dark factory: the two reviewer loops, enforced in code.

The main loop runs on a model that does not reliably follow the review
protocol. It drops ``subagent_type`` (so a "review" silently runs as a
general-purpose agent on the main model), adds ``isolation: worktree`` (which
the sandbox refuses), and omits ``run_in_background`` (which CLI 2.1.280
treats as a background launch that stalls the turn). It also forgets to report
phases and rounds. This hook takes all of that out of the model's hands.

One script handles every hook event the bundle registers:

``PostToolUse`` on ``Skill``
    Loading ``dark-factory:implement-issue`` marks the workflow as loaded.

``PreToolUse`` on ``Edit``/``Write``/``MultiEdit``/``NotebookEdit``
    No repository edit before the workflow is loaded and the plan reviewer
    approved. Right after approval (phase ``failing_test``) only test edits may
    run: an edit to a test file, or an edit that adds a test (an inline Rust
    ``#[test]`` module, a ``def test_`` function, and so on): its inserted text
    holds more test markers than the text it replaces (for ``Write``, the file
    on disk), and a notebook cell outside a test path counts only when
    inserted. Write the failing test, run it with ``Bash``, then call
    ``report_progress`` with phase ``implement``, then edit source. When no
    test is feasible, report ``implement`` directly and say why. No ``Bash``
    command opens source edits by itself. An allowed edit gets no decision, so
    the hook never grants a permission the runner would not. Every refusal
    writes a ``curie_gate: edit_refused`` line.

``PostToolUse`` on ``Edit``/``Write``/``MultiEdit``/``NotebookEdit``
    A completed test edit during ``failing_test`` is the evidence: the first one
    writes the ``failing_test`` phase line. Only an edit to a test path, or one
    the ``PreToolUse`` gate allowed as a test edit, counts; a requested, failed
    or never-allowed edit counts for nothing.

``PreToolUse`` on ``Bash``
    Refuses background shell commands. Run the command in the foreground and
    wait for its result before continuing or ending the turn.

``PostToolUse`` / ``PostToolUseFailure`` on ``Bash``
    Evidence only, never a transition. During ``failing_test``, after a
    completed test edit, each finished command increments
    ``bash_after_test_edit``, and a failed one also increments
    ``bash_failed_after_test_edit``. No command text is recorded anywhere:
    the pod log can leak secrets.

``PreToolUse`` on ``Agent``/``Task``
    Only ``plan-reviewer`` and ``diff-reviewer`` may run. The type is kept when
    it names a reviewer, otherwise inferred from the description and prompt.
    ``isolation`` and ``model`` are stripped and ``run_in_background`` is forced
    to ``false``. The call is refused out of order: no diff review before the
    plan is approved, and no plan review after it (a diff rejection returns to
    implement, not to plan). The hook numbers the round itself and writes the
    ``plan_review`` or ``review_diff`` phase line to the pod log.

``PostToolUse`` / ``PostToolUseFailure`` on ``Agent``/``Task``
    Reads the reviewer's ``REVIEWER:`` and ``VERDICT:`` lines. A reply without
    them, or a call that errored, fails the run. ``VERDICT: CHANGES`` on round
    ``MAX_ROUNDS`` caps the loop. A plan ``APPROVE`` is refused, and the run
    stops, when the checkout fingerprint differs from the one taken at the
    prompt or cannot be computed.

``PreToolUse`` on ``publish_changes``
    Allowed only after the diff reviewer approved, and never after a failed or
    capped review.

``PreToolUse`` on ``report_progress``
    Never decided. Writes a ``curie_reported`` line with the reported phase,
    the hook's last observed phase, and ``late`` when the report trails it.
    Reporting ``implement`` during ``failing_test`` is the one event that
    opens source edits. The hook's ``implement`` phase line records the
    evidence it saw (``test_edited``, ``bash_after_test_edit``,
    ``bash_failed_after_test_edit``) in its note and as fields. The hook never
    claims a test ran.

``UserPromptSubmit``
    A new message starts a new run: the reset state is saved, then the checkout
    fingerprint is taken under a deadline that also bounds hashing untracked
    files. A CI fix round starts at ``implement``.

Hook observed phases go out as ``curie_phase`` lines with ``source: "hook"``;
``curie_phase``, ``curie_reported`` and ``curie_gate`` lines share one ``seq``
counter per run.
"""

from __future__ import annotations

import datetime
import fnmatch
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

MAX_ROUNDS = 3
PLAN, DIFF = "dark-factory:plan-reviewer", "dark-factory:diff-reviewer"
PHASE = {PLAN: "plan_review", DIFF: "review_diff"}
LOOP = {PLAN: "plan", DIFF: "diff"}
BACK_TO = {PLAN: "plan", DIFF: "implement"}
AGENT_TOOLS = {"Agent", "Task"}
EDIT_TOOLS = {"Edit", "Write", "MultiEdit", "NotebookEdit"}
WORKFLOW_SKILL = "dark-factory:implement-issue"
# The phase ids of progress/phases.json, in order.
PHASE_ORDER = (
    "read_issue",
    "pin_criteria",
    "plan",
    "plan_review",
    "failing_test",
    "implement",
    "review_diff",
    "publish",
    "wait_ci",
)
TEST_DIRS = {"test", "tests", "spec", "specs", "__tests__", "testing"}
TEST_FILES = ("test_*", "*_test.*", "*.test.*", "*.spec.*", "*_spec.*", "conftest.py")
# The hook itself has 10 seconds; the git calls share most of it.
GIT_BUDGET_SECONDS = 7.0
GIT_CALL_SECONDS = 3.0

_VERDICT = re.compile(r"^\s*VERDICT:\s*(APPROVE|CHANGES)\b", re.MULTILINE)
_CI_ROUND = re.compile(r"^Curie wait_ci round ([23]) of 3: ")
# Inserted text that adds a test, for test modules that live inside a source file.
_ADDS_TEST = re.compile(
    r"#\[(?:tokio::)?test\]|#\[cfg\(test\)\]|\bdef test_\w+\(|\bfunc Test\w+\("
    r"|@Test\b|\b(?:it|test|describe)\(['\"]"
)


def _fresh(prompt: str = "") -> dict[str, Any]:
    """Fresh state for a new run, or for a CI fix round.

    A CI fix round is detected from the prompt's second line only (the
    hook's own marker, written by the platform); a match anywhere else,
    including inside the untrusted CI JSON that follows, has no effect. A CI
    fix round starts with the plan already approved (plan review already
    happened), the workflow loaded, the stage at ``implement``, and the diff
    loop at round 0.
    """
    lines = prompt.split("\n")
    ci_match = _CI_ROUND.match(lines[1]) if len(lines) > 1 else None
    ci_round = int(ci_match.group(1)) if ci_match else None
    plan = {"round": 0, "verdict": "APPROVE"} if ci_round else {"round": 0, "verdict": None}
    return {
        "plan": plan,
        "diff": {"round": 0, "verdict": None},
        "stopped": None,
        "ci_round": ci_round,
        "workflow": bool(ci_round),
        "stage": "implement" if ci_round else None,
        "test_edited": False,
        "test_edit_paths": [],
        "bash_after_test_edit": 0,
        "bash_failed_after_test_edit": 0,
        "observed": None,
        "seq": 0,
        "workspace": "",
        "fingerprint": None,
    }


def state_path(session_id: str) -> Path:
    base = os.environ.get("DARK_FACTORY_STATE_DIR") or os.path.join(
        tempfile.gettempdir(), "dark-factory-review"
    )
    name = re.sub(r"[^A-Za-z0-9_.-]", "_", session_id or "default")
    return Path(base) / f"{name}.json"


def load(path: Path) -> dict[str, Any]:
    try:
        state: dict[str, Any] = json.loads(path.read_text())
    except (OSError, ValueError):
        return _fresh()
    return state


def save(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state))
    tmp.replace(path)


def _emit(state: dict[str, Any], record: dict[str, Any]) -> None:
    """Write one evidence line where the status card reads it: the pod log.

    Our stderr is swallowed by the CLI that runs the hook, so the line goes to
    the runner container's PID 1 stderr. ``DARK_FACTORY_PROGRESS_LOG`` redirects
    it (tests, local runs).
    """
    state["seq"] += 1
    record["seq"] = state["seq"]
    record["ts"] = datetime.datetime.now(datetime.UTC).isoformat()
    line = json.dumps(record)
    target = os.environ.get("DARK_FACTORY_PROGRESS_LOG") or "/proc/1/fd/2"
    try:
        with open(target, "a") as log:
            log.write(line + "\n")
    except OSError:
        sys.stderr.write(line + "\n")


def emit_phase(state: dict[str, Any], phase: str, round_: int, note: str, **fields: Any) -> None:
    """A phase the hook itself observed."""
    state["observed"] = phase
    _emit(
        state,
        {"curie_phase": phase, "round": round_, "note": note, "source": "hook", **fields},
    )


def emit_gate(state: dict[str, Any], gate: str, **fields: Any) -> None:
    _emit(state, {"curie_gate": gate, **fields})


def emit_reported(state: dict[str, Any], tool_input: dict[str, Any]) -> None:
    """A phase the model reported, set against the hook's last observed phase."""
    phase = str(tool_input.get("phase") or "")
    observed = state["observed"]
    late = (
        phase in PHASE_ORDER
        and observed in PHASE_ORDER
        and PHASE_ORDER.index(phase) < PHASE_ORDER.index(observed)
    )
    _emit(
        state,
        {
            "curie_reported": phase,
            "round": tool_input.get("round"),
            "observed": observed,
            "late": late,
        },
    )


def _git(top: str, deadline: float, *args: str) -> bytes:
    timeout = min(GIT_CALL_SECONDS, deadline - time.monotonic())
    if timeout <= 0:
        raise subprocess.TimeoutExpired(["git", *args], 0)
    done = subprocess.run(
        ["git", "--no-optional-locks", "-C", top, *args],
        capture_output=True,
        timeout=timeout,
        check=True,
    )
    return done.stdout


def fingerprint(workspace: str) -> str | None:
    """A sha256 over the checkout's HEAD, status, diff and untracked files.

    ``None`` when any part cannot be read in time: the caller treats that as an
    unverifiable checkout, never as an unchanged one.
    """
    if not workspace:
        return None
    deadline = time.monotonic() + GIT_BUDGET_SECONDS
    digest = hashlib.sha256()
    try:
        top, head = _git(workspace, deadline, "rev-parse", "--show-toplevel", "HEAD").split()
        root = top.decode()
        digest.update(head)
        digest.update(
            _git(root, deadline, "status", "--porcelain=v1", "-z", "--untracked-files=all")
        )
        digest.update(_git(root, deadline, "diff", "HEAD", "--binary"))
        untracked = _git(root, deadline, "ls-files", "--others", "--exclude-standard", "-z")
        for name in filter(None, untracked.split(b"\0")):
            path = Path(root) / os.fsdecode(name)
            digest.update(name + b"\0")
            if time.monotonic() >= deadline:
                return None
            if path.is_symlink():
                digest.update(os.fsencode(os.readlink(path)))
            elif path.is_file():
                with path.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(1 << 20), b""):
                        digest.update(chunk)
                        if time.monotonic() >= deadline:
                            return None
    except (OSError, ValueError, subprocess.SubprocessError):
        return None
    return digest.hexdigest()


def is_test_path(path: str, workspace: str) -> bool:
    candidate = Path(path)
    if workspace and candidate.is_relative_to(workspace):
        candidate = candidate.relative_to(workspace)
    if any(part in TEST_DIRS for part in candidate.parent.parts):
        return True
    return any(fnmatch.fnmatch(candidate.name, pattern) for pattern in TEST_FILES)


def _markers(text: Any) -> int:
    return len(_ADDS_TEST.findall(text)) if isinstance(text, str) else 0


def adds_test(tool: str, tool_input: dict[str, Any]) -> bool:
    """Whether an edit adds a test, wherever the file lives.

    The test markers in the inserted text must outnumber those in the text it
    replaces, so a production change that keeps or rewrites an existing test
    adds none. A ``Write`` replaces the file on disk (a missing file holds no
    markers). A notebook cell replace cannot say what it replaced, so only an
    inserted cell counts.
    """
    if tool == "NotebookEdit":
        return (
            tool_input.get("edit_mode") == "insert" and _markers(tool_input.get("new_source")) > 0
        )
    if tool == "Write":
        try:
            old = Path(edit_path(tool_input)).read_text(errors="replace")
        except OSError:
            old = ""
        return _markers(tool_input.get("content")) > _markers(old)
    edits = [tool_input, *(tool_input.get("edits") or [])]
    added = sum(
        _markers(edit.get("new_string")) - _markers(edit.get("old_string")) for edit in edits
    )
    return added > 0


def is_test_edit(state: dict[str, Any], tool: str, path: str, tool_input: dict[str, Any]) -> bool:
    return is_test_path(path, state["workspace"]) or adds_test(tool, tool_input)


def edit_path(tool_input: dict[str, Any]) -> str:
    return str(tool_input.get("file_path") or tool_input.get("notebook_path") or "")


def reviewer_kind(tool_input: dict[str, Any]) -> str | None:
    kind = str(tool_input.get("subagent_type") or "").strip()
    if kind in (PLAN, "plan-reviewer"):
        return PLAN
    if kind in (DIFF, "diff-reviewer"):
        return DIFF
    # The description names the review ("Plan review round 1"); the prompt
    # quotes the issue, which may mention either word, so it is only a fallback.
    for text in (
        str(tool_input.get("description", "")),
        str(tool_input.get("prompt", ""))[:400],
    ):
        text = text.lower()
        if "diff" in text:
            return DIFF
        if "plan" in text:
            return PLAN
    return None


def _text(value: Any) -> str:
    """Every string inside a tool response, joined: the reply's shape varies."""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return "\n".join(_text(v) for v in value.values())
    if isinstance(value, list):
        return "\n".join(_text(v) for v in value)
    return ""


def parse_verdict(kind: str, response: Any) -> str | None:
    """``APPROVE`` or ``CHANGES`` from a real reviewer reply, else ``None``."""
    text = _text(response)
    name = kind.split(":", 1)[1]
    if not re.search(rf"^\s*REVIEWER:\s*{re.escape(name)}\s*$", text, re.MULTILINE):
        return None
    match = _VERDICT.search(text)
    return match.group(1) if match else None


def _deny(reason: str) -> dict[str, Any]:
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": reason,
        }
    }


def _allow(reason: str, updated: dict[str, Any] | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {
        "hookEventName": "PreToolUse",
        "permissionDecision": "allow",
        "permissionDecisionReason": reason,
    }
    if updated is not None:
        out["updatedInput"] = updated
    return {"hookSpecificOutput": out}


def _context(event: str, text: str) -> dict[str, Any]:
    return {"hookSpecificOutput": {"hookEventName": event, "additionalContext": text}}


def _stop_text(reason: str) -> str:
    return (
        f"STOP. {reason} Do not publish. End your reply with `Could not complete:` "
        "and the reviewer's unresolved findings and open questions as a list; "
        "the platform posts that reply on the issue."
    )


def pre_agent(state: dict[str, Any], tool_input: dict[str, Any]) -> dict[str, Any]:
    kind = reviewer_kind(tool_input)
    if kind is None:
        return _deny(
            "Only the reviewers may run as sub-agents: pass subagent_type "
            f"{PLAN!r} (phase plan_review) or {DIFF!r} (phase review_diff)."
        )
    if state["stopped"]:
        return _deny(_stop_text(state["stopped"]))
    plan, diff = state["plan"], state["diff"]
    if kind == DIFF and plan["verdict"] != "APPROVE":
        return _deny("The plan reviewer has not approved the plan yet; run the plan review first.")
    if kind == PLAN and plan["verdict"] == "APPROVE":
        return _deny(
            "The plan is already approved. A diff review rejection returns to "
            "implement, not to plan: fix the diff and call the diff reviewer."
        )
    if kind == DIFF and diff["verdict"] == "APPROVE":
        return _deny("The diff reviewer already approved; go to publish.")
    loop = state[LOOP[kind]]
    round_ = loop["round"] + 1
    if round_ > MAX_ROUNDS:
        state["stopped"] = f"The {PHASE[kind]} loop hit its {MAX_ROUNDS} round cap."
        return _deny(_stop_text(state["stopped"]))
    loop["round"] = round_
    loop["verdict"] = None
    emit_phase(state, PHASE[kind], round_, f"{kind.split(':', 1)[1]} round {round_}")
    updated = dict(tool_input)
    updated["subagent_type"] = kind
    updated.pop("isolation", None)
    updated.pop("model", None)
    # An omitted run_in_background launches the agent async and the main loop
    # stalls waiting for a notification. Always run the reviewer in the foreground.
    updated["run_in_background"] = False
    return _allow(f"routed to {kind}, round {round_}", updated)


def pre_bash(state: dict[str, Any], tool_input: dict[str, Any]) -> dict[str, Any]:
    if tool_input.get("run_in_background") is True:
        return _deny(
            "Run Bash in the foreground. Do not set run_in_background to true. "
            "Wait for the command to finish before you end your turn."
        )
    updated = dict(tool_input)
    updated["run_in_background"] = False
    return _allow("Bash runs in the foreground", updated)


def pre_edit(state: dict[str, Any], tool: str, tool_input: dict[str, Any]) -> dict[str, Any] | None:
    path = edit_path(tool_input)
    if state["stopped"]:
        reason = _stop_text(state["stopped"])
    elif not state["workflow"]:
        reason = (
            "No repository edit before the workflow is loaded. Call the Skill tool "
            "with skill 'dark-factory:implement-issue' and follow it."
        )
    elif state["plan"]["verdict"] != "APPROVE":
        reason = (
            "No repository edit before plan review approves the plan. Finish phase "
            f"plan and call {PLAN!r}; edits open after its VERDICT: APPROVE."
        )
    elif state["stage"] == "failing_test" and not is_test_edit(state, tool, path, tool_input):
        reason = (
            "Phase failing_test comes first: write the failing test (a test file, or "
            "an inline test that adds a test), run it with Bash, then call "
            "report_progress with phase implement, then edit source. When no test is "
            "feasible (docs, config), report phase implement directly and say why."
        )
    else:
        # A Write has overwritten the file by PostToolUse, so the test edit is
        # judged here and remembered for post_edit.
        if state["stage"] == "failing_test":
            state["test_edit_paths"].append(path)
        return None
    emit_gate(state, "edit_refused", tool=tool, path=path)
    return _deny(reason)


def post_edit(state: dict[str, Any], tool_input: dict[str, Any]) -> None:
    if state["stage"] != "failing_test" or state["test_edited"]:
        return
    path = edit_path(tool_input)
    if is_test_path(path, state["workspace"]) or path in state["test_edit_paths"]:
        state["test_edited"] = True
        emit_phase(state, "failing_test", 1, "first test edit after plan approval")


def post_bash(state: dict[str, Any], event: str) -> None:
    """Count finished commands after a test edit. Never the command text."""
    if state["stage"] != "failing_test" or not state["test_edited"]:
        return
    state["bash_after_test_edit"] += 1
    if event == "PostToolUseFailure":
        state["bash_failed_after_test_edit"] += 1


def pre_report(state: dict[str, Any], tool_input: dict[str, Any]) -> None:
    emit_reported(state, tool_input)
    if state["stage"] == "failing_test" and tool_input.get("phase") == "implement":
        state["stage"] = "implement"
        evidence = {
            "test_edited": bool(state["test_edited"]),
            "bash_after_test_edit": state["bash_after_test_edit"],
            "bash_failed_after_test_edit": state["bash_failed_after_test_edit"],
        }
        note = "; ".join(["reported implement", *(f"{k}={v}" for k, v in evidence.items())])
        emit_phase(state, "implement", 1, note, **evidence)


def post_skill(state: dict[str, Any], tool_input: dict[str, Any]) -> None:
    if str(tool_input.get("skill") or "") == WORKFLOW_SKILL and not state["workflow"]:
        state["workflow"] = True
        emit_gate(state, "workflow_loaded", skill=tool_input["skill"])


def refuse_approval(state: dict[str, Any]) -> str | None:
    """The stop cause when the checkout moved before plan approval, else ``None``."""
    now = fingerprint(state["workspace"])
    if state["fingerprint"] is None or now is None:
        cause = "checkout_unverifiable"
    elif now != state["fingerprint"]:
        cause = "checkout_changed_before_plan_approval"
    else:
        return None
    emit_gate(state, "approval_refused", cause=cause)
    return cause


def post_agent(
    state: dict[str, Any], event: str, tool_input: dict[str, Any], response: Any
) -> dict[str, Any] | None:
    kind = reviewer_kind(tool_input)
    if kind is None:
        return None
    loop = state[LOOP[kind]]
    round_ = loop["round"]
    verdict = None if event == "PostToolUseFailure" else parse_verdict(kind, response)
    if verdict is None:
        state["stopped"] = (
            f"The {PHASE[kind]} call failed on round {round_}: no reviewer verdict came back."
        )
        return _context(event, _stop_text(state["stopped"]))
    if kind == PLAN and verdict == "APPROVE":
        cause = refuse_approval(state)
        if cause is not None:
            state["stopped"] = (
                f"The plan approval on round {round_} is refused ({cause}): the "
                "repository checkout must not change before the plan reviewer approves."
            )
            return _context(event, _stop_text(state["stopped"]))
        state["stage"] = "failing_test"
    loop["verdict"] = verdict
    if verdict == "APPROVE":
        nxt = "failing_test" if kind == PLAN else "publish"
        return _context(event, f"{PHASE[kind]} round {round_}: APPROVED. Go to phase {nxt}.")
    if round_ >= MAX_ROUNDS:
        state["stopped"] = (
            f"The {PHASE[kind]} loop hit its {MAX_ROUNDS} round cap without approval."
        )
        return _context(event, _stop_text(state["stopped"]))
    return _context(
        event,
        f"{PHASE[kind]} round {round_}: CHANGES. Go back to phase {BACK_TO[kind]}, "
        f"round {round_ + 1} of {MAX_ROUNDS}, and address every finding.",
    )


def pre_publish(state: dict[str, Any]) -> dict[str, Any]:
    if state["stopped"]:
        return _deny(_stop_text(state["stopped"]))
    if state["diff"]["verdict"] != "APPROVE":
        return _deny("Nothing is published without a diff reviewer VERDICT: APPROVE.")
    return _allow("diff review approved")


def handle(data: dict[str, Any]) -> dict[str, Any] | None:
    event = data.get("hook_event_name", "")
    path = state_path(str(data.get("session_id") or ""))
    if event == "UserPromptSubmit":
        state = _fresh(str(data.get("prompt") or ""))
        state["workspace"] = os.environ.get("DARK_FACTORY_WORKSPACE") or str(data.get("cwd") or "")
        ci_round = state["ci_round"]
        if not ci_round:
            # Saved first: a hook killed mid-fingerprint must not leave the last
            # run's approvals in place.
            save(path, state)
            state["fingerprint"] = fingerprint(state["workspace"])
        else:
            emit_phase(
                state, "wait_ci", ci_round, f"checks failed; fix round {ci_round} of {MAX_ROUNDS}"
            )
        save(path, state)
        if ci_round:
            return _context(
                "UserPromptSubmit",
                "CI fix round: go to phase implement, fix what the failing checks show, "
                "call the diff reviewer, then publish to the same pull request.",
            )
        return None
    tool = str(data.get("tool_name") or "")
    tool_input = dict(data.get("tool_input") or {})
    state = load(path)
    out: dict[str, Any] | None = None
    if tool in AGENT_TOOLS:
        if event == "PreToolUse":
            out = pre_agent(state, tool_input)
        elif event in ("PostToolUse", "PostToolUseFailure"):
            response = data.get("tool_response", data.get("error"))
            out = post_agent(state, event, tool_input, response)
    elif event == "PostToolUse" and tool == "Skill":
        post_skill(state, tool_input)
    elif event == "PostToolUse" and tool in EDIT_TOOLS:
        post_edit(state, tool_input)
    elif event in ("PostToolUse", "PostToolUseFailure") and tool == "Bash":
        post_bash(state, event)
    elif event == "PreToolUse" and tool in EDIT_TOOLS:
        out = pre_edit(state, tool, tool_input)
    elif event == "PreToolUse" and tool == "Bash":
        out = pre_bash(state, tool_input)
    elif event == "PreToolUse" and tool.endswith("report_progress"):
        pre_report(state, tool_input)
    elif event == "PreToolUse" and tool.endswith("publish_changes"):
        out = pre_publish(state)
    save(path, state)
    return out


def main() -> int:
    out = handle(json.load(sys.stdin))
    if out is not None:
        print(json.dumps(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
