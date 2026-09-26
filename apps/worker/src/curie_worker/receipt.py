"""What a turn tells the person who asked that it did to the world (ADR-0117).

Curie already classified every tool absent from a read-only allowlist as
side-effecting and recorded what each call did. This is the half that gets that
back to a human: a turn that changed anything ends by listing each action and
whether the platform can put it back.

Channel-neutral, and appended to the turn's reply rather than posted as a card.
A card carrying a working undo control needs an interaction type on the channel
protocol AND something able to perform a restore, and neither belongs in a change
that has nothing for the control to do -- a button that authorizes a restore
which never runs is the platform telling a user an action was put back when it
was not.

The line still says which actions could be put back. That is the thing an
operator is buying: not a bot that cannot make mistakes, but a platform that
knows which mistakes it can take back.
"""

from __future__ import annotations

import re
from typing import Any

# A connector's summary is not a size this platform controls, and a receipt is
# read in a chat client beneath an answer someone actually asked for.
_SUMMARY_MAX = 96

_HEADER = "_What I changed:_"
# The receipt sits beneath the answer; past this many lines it lists the first
# ones and counts the rest.
_MAX_LINES = 10

# Said when an action reported nothing at all. Deliberately distinct from a
# connector's own sentence: an undeclared third-party tool and a tool that
# explained itself are both not-undoable, and flattening them to one line would
# hide which happened.
_UNDECLARED = "cannot be undone: nothing reported a prior state"
_GENERIC_BASH_DETAILS = {
    None,
    "non-idempotent tool completed",
    "non-idempotent tool executed",
    "tool result too large to record",
}
_READ_ONLY_COMMANDS = (
    r"pwd",
    r"ls(?: -[alh]+)?(?: [\w./-]+)*",
    r"cat [\w./-]+(?: [\w./-]+)*",
    r"rg(?: -[nSi]+)? [\w./:][\w./:-]*(?: [\w./][\w./-]*)*",
    r"sed -n '[0-9,$]+p' [\w./-]+",
    r"git status(?: --short)?",
    r"git diff(?: --stat)?",
)


def _clamp(text: str) -> str:
    text = " ".join(str(text).split())
    encoded = text.encode("utf-8")
    if len(encoded) <= _SUMMARY_MAX:
        return text
    budget = _SUMMARY_MAX - len("…".encode())
    return encoded[:budget].decode("utf-8", "ignore").rstrip() + "…"


def _described(action: dict[str, Any]) -> str:
    """The connector's own summary, or the tool's name when it offered none."""

    result = action.get("result")
    summary = result.get("summary") if isinstance(result, dict) else None
    if isinstance(summary, str) and summary.strip():
        return _clamp(summary)
    return f"called `{_clamp(action.get('tool') or 'a tool')}`"


def _verdict(action: dict[str, Any]) -> str:
    if action.get("status") == "failed":
        # "It may have happened" is the state a human most needs told: the call
        # reported failure, and a failed write is not the same as no write.
        return "failed — check before retrying"
    if action.get("undoable"):
        return "can be undone"
    detail = action.get("detail")
    if isinstance(detail, str) and detail.strip():
        return _clamp(detail)
    return _UNDECLARED


def _generic_bash(action: dict[str, Any]) -> bool:
    if action.get("tool") != "Bash" or action.get("status") != "succeeded":
        return False
    if action.get("undoable") or action.get("detail") not in _GENERIC_BASH_DETAILS:
        return False
    result = action.get("result")
    summary = result.get("summary") if isinstance(result, dict) else None
    return not (isinstance(summary, str) and summary.strip())


def _read_only_bash(action: dict[str, Any]) -> bool:
    """Suppress only plain commands whose stored arguments show a read."""

    if not _generic_bash(action):
        return False
    arguments = action.get("arguments")
    command = arguments.get("command") if isinstance(arguments, dict) else None
    if not isinstance(command, str) or not command.strip():
        return False
    return any(re.fullmatch(pattern, command.strip()) for pattern in _READ_ONLY_COMMANDS)


def render_receipt(actions: list[dict[str, Any]]) -> str | None:
    """One line per action, or None when the turn changed nothing.

    Most turns are reads, and a receipt on every one of them is noise. This
    returns None rather than an empty section so the caller has nothing to
    decide.

    Both kinds of line are here on purpose. A receipt listing only the undoable
    actions would hide the ones that matter most: the value of showing
    "restarting pods cannot be undone" beside "scaled 3 to 10, can be undone" is
    that an operator sees the system knows the difference.
    """

    visible = [action for action in actions if not _read_only_bash(action)]
    if not visible:
        return None
    lines: list[str] = []
    counts: list[int] = []
    failures: list[int] = []
    grouped: dict[str, int] = {}
    for action in visible:
        generic_bash = _generic_bash(action)
        line = (
            "• Bash calls; changes not described"
            if generic_bash
            else f"• {_described(action)} — {_verdict(action)}"
        )
        if action.get("status") == "failed":
            failures.append(len(lines))
        elif line in grouped:
            counts[grouped[line]] += 1
            continue
        else:
            grouped[line] = len(lines)
        lines.append(line)
        counts.append(1)

    for i, line in enumerate(lines):
        if line == "• Bash calls; changes not described":
            noun = "call" if counts[i] == 1 else "calls"
            lines[i] = f"• {counts[i]} Bash {noun}; changes not described"
        elif counts[i] > 1:
            lines[i] = f"{line} ({counts[i]} calls)"
    if len(lines) > _MAX_LINES:
        # A turn with a hundred calls used to end with a hundred lines, and the
        # reply plus receipt passed the channel's size limit, so the answer
        # itself was lost (#3064). A failed call is the line a person most needs,
        # so failures are kept first, then the rest in the order they ran.
        order = failures + [i for i in range(len(lines)) if i not in failures]
        kept = sorted(order[:_MAX_LINES])
        omitted = sum(counts) - sum(counts[i] for i in kept)
        lines = [lines[i] for i in kept] + [f"• …and {omitted} more actions not listed"]
    return "\n".join([_HEADER, *lines])
