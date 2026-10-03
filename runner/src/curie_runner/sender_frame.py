"""Platform sender frame around one user turn (#3818).

The frame is what the model is queried with. It is not part of the system
prompt. The boundary token is chosen so it does not occur in the user text,
the user id, or the channel kind, which keeps a copied sender block inside
the message off the platform line.
"""

from __future__ import annotations

_BOUNDARY_BASE = "curie-sender-boundary"

_EVENT_ROLE = {
    "message": "person",
    "job": "scheduled run",
    "eval_case": "eval sender",
}


def frame_user_turn(
    event_type: str,
    user: str,
    text: str,
    channel_kind: str | None,
) -> str:
    """Fence ``text`` with the platform sender for this event type.

    ``message``, ``job``, and ``eval_case`` name that event. Any other type
    is ``unknown`` and does not repeat the raw user id. The user id is not
    parsed; a newline in it is escaped so the person field stays one line.
    """

    boundary = _boundary_token(text, user, channel_kind)
    if event_type in _EVENT_ROLE:
        event = event_type
        role = _EVENT_ROLE[event_type]
        channel = _channel_value(channel_kind, boundary)
        person = "none" if event_type == "job" else _person_value(user)
    else:
        event = "unknown"
        role = "unknown sender"
        channel = "none"
        person = "none"
    return (
        f"[platform-sender {boundary}]\n"
        f"event: {event}\n"
        f"channel: {channel}\n"
        f"person: {person}\n"
        f"role: {role}\n"
        f"[user-message {boundary}]\n"
        f"{text}\n"
        f"[end-user-message {boundary}]"
    )


def _boundary_token(text: str, user: str, channel_kind: str | None) -> str:
    haystack = f"{text}{user}{channel_kind or ''}"
    token = _BOUNDARY_BASE
    suffix = 0
    while token in haystack:
        suffix += 1
        token = f"{_BOUNDARY_BASE}-{suffix}"
    return token


def _channel_value(channel_kind: str | None, boundary: str) -> str:
    if not channel_kind:
        return "none"
    stripped = channel_kind.strip()
    if not stripped or "\n" in stripped or "\r" in stripped or boundary in stripped:
        return "none"
    return stripped


def _person_value(user: str) -> str:
    stripped = user.strip()
    if not stripped:
        return "none"
    return stripped.replace("\r", "\\n").replace("\n", "\\n")
