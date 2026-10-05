"""Time window, page size and cursor rules for a channel read (ADR 0100).

A window is ``[oldest, latest)`` in UTC and at most seven days wide. The first
page of a read may omit ``latest``, which then becomes the request time and is
fixed into the cursor, so a later page can never widen what page one asked
for. A cursor is HMAC signed and binds the agent, the logical turn, the
resolved channel, the operation, the thread and the window; it is compared
against the request, never used to pick a channel.
"""

from __future__ import annotations

import hmac
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

from curie_internal import sandbox_token
from curie_internal.channel_read_ledger import turn_key
from curie_internal.sandbox_token import b64url, b64url_decode

from .errors import ChannelReadRefused

Operation = Literal["history", "thread", "message"]

MAX_WINDOW = timedelta(days=7)
DEFAULT_LIMIT = 50
MAX_LIMIT = 100
_CURSOR_PREFIX = "chc"
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class ResolvedWindow:
    oldest: datetime
    latest: datetime


@dataclass(frozen=True)
class CursorState:
    op: Operation
    thread_id: str | None
    window: ResolvedWindow
    limit: int
    # Slack's own cursor, or the time boundary (epoch microseconds) a page
    # continues from when Slack offered none. Exactly one is set.
    provider_cursor: str | None
    boundary: int | None = None


def _window_invalid(message: str) -> ChannelReadRefused:
    return ChannelReadRefused(422, "window_invalid", message)


def _cursor_invalid() -> ChannelReadRefused:
    return ChannelReadRefused(
        422, "cursor_invalid", "the cursor is not one this turn issued for this read"
    )


def epoch_micros(moment: datetime) -> int:
    delta = moment - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def from_epoch_micros(value: int) -> datetime:
    return _EPOCH + timedelta(microseconds=value)


def parse_rfc3339(value: str) -> datetime:
    """An offset carrying timestamp in UTC; a naive or unparseable one is refused."""

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise _window_invalid("timestamps must be RFC 3339 with an offset") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _window_invalid("timestamps must be RFC 3339 with an offset")
    return parsed.astimezone(UTC)


def resolve_window(
    op: Operation, oldest: str | None, latest: str | None, now: datetime
) -> ResolvedWindow | None:
    """The first page's window, or None for ``message``, which takes none."""

    if op == "message":
        if oldest is not None or latest is not None:
            raise _window_invalid("a message read takes no window")
        return None
    if oldest is None:
        raise _window_invalid("history and thread reads need oldest")
    start = parse_rfc3339(oldest)
    end = parse_rfc3339(latest) if latest is not None else now
    if start > now:
        raise _window_invalid("oldest is in the future")
    if end <= start:
        raise _window_invalid("latest must be after oldest")
    if end - start > MAX_WINDOW:
        raise ChannelReadRefused(422, "window_too_wide", "a window spans at most seven days")
    return ResolvedWindow(oldest=start, latest=end)


def resolve_limit(op: Operation, limit: int | None) -> int:
    if op == "message":
        if limit is not None:
            raise ChannelReadRefused(422, "limit_invalid", "a message read takes no limit")
        return 1
    if limit is None:
        return DEFAULT_LIMIT
    if not 1 <= limit <= MAX_LIMIT:
        raise ChannelReadRefused(422, "limit_invalid", f"limit must be 1 to {MAX_LIMIT}")
    return limit


def mint_cursor(
    api_key: str,
    *,
    agent: uuid.UUID,
    turn: str,
    kind: str,
    address: str,
    state: CursorState,
) -> str:
    body = {
        "agent": str(agent),
        "turn": turn_key(turn),
        "kind": kind,
        "address": address,
        "op": state.op,
        "thread": state.thread_id,
        "oldest": epoch_micros(state.window.oldest),
        "latest": epoch_micros(state.window.latest),
        "limit": state.limit,
        "provider": state.provider_cursor,
        "boundary": state.boundary,
    }
    payload = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    signed = f"{_CURSOR_PREFIX}.{b64url(payload)}"
    return f"{signed}.{sandbox_token.signature(api_key, signed)}"


def verify_cursor(
    api_key: str, cursor: str, *, agent: uuid.UUID, turn: str, kind: str, address: str
) -> CursorState:
    """The state a cursor carries, or ``cursor_invalid`` for any tamper or other binding."""

    try:
        prefix, payload, signature = cursor.split(".")
        if prefix != _CURSOR_PREFIX or not hmac.compare_digest(
            signature, sandbox_token.signature(api_key, f"{prefix}.{payload}")
        ):
            raise _cursor_invalid()
        body = json.loads(b64url_decode(payload))
        bound = (body["agent"], body["turn"], body["kind"], body["address"])
        op = body["op"]
        thread = body["thread"]
        oldest = int(body["oldest"])
        latest = int(body["latest"])
        limit = int(body["limit"])
        provider = body["provider"]
        boundary = body["boundary"]
    except (ValueError, TypeError, KeyError):
        raise _cursor_invalid() from None
    if bound != (str(agent), turn_key(turn), kind, address):
        raise _cursor_invalid()
    if op not in ("history", "thread"):
        raise _cursor_invalid()
    if (provider is None) == (boundary is None):
        raise _cursor_invalid()
    if provider is not None and not isinstance(provider, str):
        raise _cursor_invalid()
    if boundary is not None and (not isinstance(boundary, int) or not oldest < boundary <= latest):
        raise _cursor_invalid()
    if thread is not None and not isinstance(thread, str):
        raise _cursor_invalid()
    return CursorState(
        op=op,
        thread_id=thread,
        window=ResolvedWindow(from_epoch_micros(oldest), from_epoch_micros(latest)),
        limit=limit,
        provider_cursor=provider,
        boundary=boundary,
    )


def reconcile(
    cursor: CursorState,
    *,
    op: Operation | None,
    oldest: str | None,
    latest: str | None,
    thread_id: str | None,
    message_id: str | None,
) -> None:
    """Any value supplied beside a cursor must equal the cursor's own."""

    if op is not None and op != cursor.op:
        raise _cursor_invalid()
    if thread_id is not None and thread_id != cursor.thread_id:
        raise _cursor_invalid()
    if message_id is not None:
        raise _cursor_invalid()
    for supplied, kept in ((oldest, cursor.window.oldest), (latest, cursor.window.latest)):
        if supplied is None:
            continue
        try:
            moment = parse_rfc3339(supplied)
        except ChannelReadRefused:
            raise _cursor_invalid() from None
        if moment != kept:
            raise _cursor_invalid()
