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

from typing import Any

# A connector's summary is not a size this platform controls, and a receipt is
# read in a chat client beneath an answer someone actually asked for.
_SUMMARY_MAX = 160

_HEADER = "_What I changed:_"
# The receipt sits beneath the answer; past this many lines it lists the first
# ones and counts the rest.
_MAX_LINES = 10

# Said when an action reported nothing at all. Deliberately distinct from a
# connector's own sentence: an undeclared third-party tool and a tool that
# explained itself are both not-undoable, and flattening them to one line would
# hide which happened.
_UNDECLARED = "cannot be undone: nothing reported a prior state"


def _clamp(text: str) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= _SUMMARY_MAX else text[: _SUMMARY_MAX - 1].rstrip() + "…"


def _described(action: dict[str, Any]) -> str:
    """The connector's own summary, or the tool's name when it offered none."""

    result = action.get("result")
    summary = result.get("summary") if isinstance(result, dict) else None
    if isinstance(summary, str) and summary.strip():
        return _clamp(summary)
    return f"called `{action.get('tool') or 'a tool'}`"


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

    if not actions:
        return None
    lines = [f"• {_described(action)} — {_verdict(action)}" for action in actions]
    if len(lines) > _MAX_LINES:
        # A turn with a hundred calls used to end with a hundred lines, and the
        # reply plus receipt passed the channel's size limit, so the answer
        # itself was lost (#3064). A failed call is the line a person most needs,
        # so failures are kept first, then the rest in the order they ran.
        failed = [i for i, a in enumerate(actions) if a.get("status") == "failed"]
        order = failed + [i for i in range(len(lines)) if i not in failed]
        kept = sorted(order[:_MAX_LINES])
        omitted = len(lines) - len(kept)
        lines = [lines[i] for i in kept] + [f"• …and {omitted} more actions not listed"]
    return "\n".join([_HEADER, *lines])
