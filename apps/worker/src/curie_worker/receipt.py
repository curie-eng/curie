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

The line still says which actions the ledger holds restore information for.
That is the thing an operator is buying: not a bot that cannot make mistakes, but
a platform that knows which mistakes it could take back. It does not say "can be
undone": nothing executes a restore yet (#1867), so that would promise what no
part of the platform can deliver.
"""

from __future__ import annotations

import re
from typing import Any, Literal

# What the receipt shows, chosen per install (ADR-0180). ``WorkerConfig`` reads
# it from ``CURIE_TURN_RECEIPT``, and the chart schema offers the same three.
TurnReceiptMode = Literal["all", "failures", "off"]

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
_GENERIC_DETAILS = {
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

# The platform memory tools (#1461, ADR-0167). Saving, changing or forgetting a
# remembered fact is not a change to the world the person asked about, so the
# action is still recorded but never announced: a receipt line per save would
# be noise under every answer. Live names, as the runner's ``curie`` server
# publishes them.
_UNANNOUNCED_TOOLS = frozenset({"mcp__curie__remember", "mcp__curie__update", "mcp__curie__forget"})


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
        # The ledger holds what a restore needs; nothing performs one yet (#1867).
        return "restore information recorded"
    detail = action.get("detail")
    # Runner bookkeeping is not a connector explanation of irreversibility.
    if isinstance(detail, str) and detail.strip() and detail not in _GENERIC_DETAILS:
        return _clamp(detail)
    result = action.get("result")
    if action.get("prior_state") is not None or (
        isinstance(result, dict) and result.get("prior") is not None
    ):
        return "cannot be undone: undo information is incomplete"
    return _UNDECLARED


def _generic_native(action: dict[str, Any]) -> bool:
    if action.get("tool") not in {"Bash", "Skill"} or action.get("status") != "succeeded":
        return False
    if action.get("undoable") or action.get("detail") not in _GENERIC_DETAILS:
        return False
    result = action.get("result")
    summary = result.get("summary") if isinstance(result, dict) else None
    return not (isinstance(summary, str) and summary.strip())


def _read_only_bash(action: dict[str, Any]) -> bool:
    """Suppress only plain commands whose stored arguments show a read."""

    if action.get("tool") != "Bash" or not _generic_native(action):
        return False
    arguments = action.get("arguments")
    command = arguments.get("command") if isinstance(arguments, dict) else None
    if not isinstance(command, str) or not command.strip():
        return False
    return any(re.fullmatch(pattern, command.strip()) for pattern in _READ_ONLY_COMMANDS)


def render_receipt(actions: list[dict[str, Any]], mode: TurnReceiptMode = "all") -> str | None:
    """One line per action, or None when the turn changed nothing.

    Most turns are reads, and a receipt on every one of them is noise. This
    returns None rather than an empty section so the caller has nothing to
    decide.

    Both kinds of line are here on purpose. A receipt listing only the undoable
    actions would hide the ones that matter most: the value of showing
    "restarting pods cannot be undone" beside "scaled 3 to 10, restore
    information recorded" is that an operator sees the system knows the
    difference.

    ``mode`` is the install's choice (ADR-0180): ``failures`` renders this same
    receipt for the failed actions alone, and ``off`` renders none. It decides
    only what the person is shown; the caller has already recorded every action.
    """

    if mode == "off":
        return None
    if mode == "failures":
        actions = [action for action in actions if action.get("status") == "failed"]
    visible = [
        action
        for action in actions
        if action.get("tool") not in _UNANNOUNCED_TOOLS and not _read_only_bash(action)
    ]
    if not visible:
        return None
    lines: list[str] = []
    counts: list[int] = []
    failures: list[int] = []
    grouped: dict[str, int] = {}
    for action in visible:
        generic_native = _generic_native(action)
        request = "Shell" if action.get("tool") == "Bash" else "Instruction"
        line = (
            f"• {request} request completed; changes were not summarized "
            "and undo information is incomplete"
            if generic_native
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
        if counts[i] > 1:
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
