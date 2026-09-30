#!/usr/bin/env python3
"""Review gate for the dark factory: the two reviewer loops, enforced in code.

The main loop runs on a model that does not reliably follow the review
protocol. It drops ``subagent_type`` (so a "review" silently runs as a
general-purpose agent on the main model), adds ``isolation: worktree`` (which
the sandbox refuses), and omits ``run_in_background`` (which CLI 2.1.280
treats as a background launch that stalls the turn). It also forgets to report
phases and rounds. This hook takes all of that out of the model's hands.

One script handles every hook event the bundle registers:

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
    ``MAX_ROUNDS`` caps the loop.

``PreToolUse`` on ``publish_changes``
    Allowed only after the diff reviewer approved, and never after a failed or
    capped review.

``PreToolUse`` / ``PostToolUse`` on ``add_issue_comment``
    Allowed only after a failed or capped review, only on the run's own issue,
    and only until one comment succeeds (at most ``MAX_COMMENT_ATTEMPTS``
    tries), so the unresolved findings reach the issue. Any other comment is
    refused.

``UserPromptSubmit``
    A new message starts a new run: the state resets, and the first GitHub
    issue or pull request link in the message becomes the run's issue.
"""

from __future__ import annotations

import datetime
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any

MAX_ROUNDS = 3
MAX_COMMENT_ATTEMPTS = 2
PLAN, DIFF = "dark-factory:plan-reviewer", "dark-factory:diff-reviewer"
PHASE = {PLAN: "plan_review", DIFF: "review_diff"}
LOOP = {PLAN: "plan", DIFF: "diff"}
BACK_TO = {PLAN: "plan", DIFF: "implement"}
AGENT_TOOLS = {"Agent", "Task"}

_ISSUE_LINK = re.compile(r"github\.com/([\w.-]+)/([\w.-]+)/(?:issues|pull)/(\d+)")
_VERDICT = re.compile(r"^\s*VERDICT:\s*(APPROVE|CHANGES)\b", re.MULTILINE)
_CI_ROUND = re.compile(r"^Curie wait_ci round ([23]) of 3: ")


def _fresh(prompt: str = "") -> dict[str, Any]:
    """Fresh state for a new run, or for a CI fix round.

    A CI fix round is detected from the prompt's second line only (the
    hook's own marker, written by the platform); a match anywhere else,
    including inside the untrusted CI JSON that follows, has no effect. A CI
    fix round starts with the plan already approved (plan review already
    happened) and the diff loop at round 0.
    """
    link = _ISSUE_LINK.search(prompt)
    lines = prompt.split("\n")
    ci_match = _CI_ROUND.match(lines[1]) if len(lines) > 1 else None
    ci_round = int(ci_match.group(1)) if ci_match else None
    plan = {"round": 0, "verdict": "APPROVE"} if ci_round else {"round": 0, "verdict": None}
    return {
        "plan": plan,
        "diff": {"round": 0, "verdict": None},
        "stopped": None,
        "issue": [link.group(1).lower(), link.group(2).lower(), int(link.group(3))]
        if link
        else None,
        "comment_attempts": 0,
        "commented": False,
        "ci_round": ci_round,
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


def emit_phase(phase: str, round_: int, note: str) -> None:
    """Write one phase line where the status card reads it: the pod log.

    Our stderr is swallowed by the CLI that runs the hook, so the line goes to
    the runner container's PID 1 stderr. ``DARK_FACTORY_PROGRESS_LOG`` redirects
    it (tests, local runs).
    """
    line = json.dumps(
        {
            "curie_phase": phase,
            "round": round_,
            "note": note,
            "ts": datetime.datetime.now(datetime.UTC).isoformat(),
        }
    )
    target = os.environ.get("DARK_FACTORY_PROGRESS_LOG") or "/proc/1/fd/2"
    try:
        with open(target, "a") as log:
            log.write(line + "\n")
    except OSError:
        sys.stderr.write(line + "\n")


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
        f"STOP. {reason} Do not publish. Post the reviewer's unresolved findings "
        "and open questions on the issue with add_issue_comment, then end your "
        "reply with `Could not complete:` and the same list."
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
    emit_phase(PHASE[kind], round_, f"{kind.split(':', 1)[1]} round {round_}")
    updated = dict(tool_input)
    updated["subagent_type"] = kind
    updated.pop("isolation", None)
    updated.pop("model", None)
    # An omitted run_in_background launches the agent async and the main loop
    # stalls waiting for a notification. Always run the reviewer in the foreground.
    updated["run_in_background"] = False
    return _allow(f"routed to {kind}, round {round_}", updated)


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


def _comment_target(tool_input: dict[str, Any]) -> list[Any] | None:
    try:
        return [
            str(tool_input["owner"]).lower(),
            str(tool_input["repo"]).lower(),
            int(tool_input["issue_number"]),
        ]
    except (KeyError, TypeError, ValueError):
        return None


def pre_comment(state: dict[str, Any], tool_input: dict[str, Any]) -> dict[str, Any]:
    if not state["stopped"]:
        return _deny(
            "Issue comments are only for posting unresolved review findings after a "
            "failed or capped review."
        )
    if state["commented"]:
        return _deny("The findings are already posted; end with `Could not complete:`.")
    if state["issue"] is None or _comment_target(tool_input) != state["issue"]:
        return _deny(
            "The findings comment may only go on this run's issue "
            f"({state['issue'] or 'unknown'}); end with `Could not complete:` instead."
        )
    if state["comment_attempts"] >= MAX_COMMENT_ATTEMPTS:
        return _deny("The findings comment failed twice; end with `Could not complete:`.")
    state["comment_attempts"] += 1
    return _allow("posting unresolved review findings")


def handle(data: dict[str, Any]) -> dict[str, Any] | None:
    event = data.get("hook_event_name", "")
    path = state_path(str(data.get("session_id") or ""))
    if event == "UserPromptSubmit":
        state = _fresh(str(data.get("prompt") or ""))
        save(path, state)
        ci_round = state.get("ci_round")
        if ci_round:
            emit_phase(
                "wait_ci", ci_round, f"checks failed; fix round {ci_round} of {MAX_ROUNDS}"
            )
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
    elif event == "PreToolUse" and tool.endswith("publish_changes"):
        out = pre_publish(state)
    elif tool.endswith("add_issue_comment"):
        if event == "PreToolUse":
            out = pre_comment(state, tool_input)
        elif event == "PostToolUse" and "#issuecomment-" in _text(data.get("tool_response")):
            # A posted comment comes back with its html_url; an error does not.
            state["commented"] = True
    save(path, state)
    return out


def main() -> int:
    out = handle(json.load(sys.stdin))
    if out is not None:
        print(json.dumps(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
