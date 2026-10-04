"""Slack history reads, and nothing else (ADR 0100, #2877).

The only place the API reads Slack conversation content. Canvas reads (#3819)
belong in a sibling module, not here. The caller has already authorized the
channel; nothing in this module selects one from a message or thread id.

Methods and shapes:
https://docs.slack.dev/reference/methods/conversations.history
https://docs.slack.dev/reference/methods/conversations.replies
https://docs.slack.dev/reference/methods/auth.test
https://docs.slack.dev/apis/web-api/rate-limits
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from .errors import ChannelReadRefused
from .window import ResolvedWindow, epoch_micros, from_epoch_micros

SLACK_API = "https://slack.com/api/"
TEXT_LIMIT = 4000
TRUNCATION_MARKER = " [truncated]"

_TS = re.compile(r"^\d{9,10}\.\d{6}$")
_MESSAGE_ID = re.compile(r"^(\d{9,10}\.\d{6})(?::(\d{9,10}\.\d{6}))?$")
_NOT_MEMBER = frozenset({"not_in_channel", "channel_not_found"})
# Workspace URL per token digest; it is fixed for an install's lifetime.
_PERMALINK_BASES: dict[str, str] = {}


@dataclass(frozen=True)
class ChannelMessage:
    id: str
    thread_id: str | None
    timestamp: str
    author: str
    text: str
    truncated: bool
    provenance: str
    reply_count: int | None


HISTORY_METHOD = "conversations.history"
REPLIES_METHOD = "conversations.replies"


@dataclass(frozen=True)
class ProviderPage:
    """One page, and how to continue it: Slack's own cursor, or, when Slack
    says there is more but offers none, the ``boundary`` timestamp (epoch
    microseconds) the next page stops before (history) or starts after (thread).
    """

    messages: list[ChannelMessage]
    next_cursor: str | None = None
    boundary: int | None = None

    @property
    def has_more(self) -> bool:
        return self.next_cursor is not None or self.boundary is not None


def provider_method(op: str, message_id: str | None) -> str:
    """The Slack method an operation calls, which is what a cooldown is keyed by."""

    if op == "thread":
        return REPLIES_METHOD
    if op == "message" and message_id is not None:
        parsed = parse_message_id(message_id)
        if parsed is not None and parsed[0] is not None:
            return REPLIES_METHOD
    return HISTORY_METHOD


def valid_slack_ts(value: str) -> bool:
    return bool(_TS.match(value))


def parse_message_id(value: str) -> tuple[str | None, str] | None:
    """``<ts>`` is ``(None, ts)``; ``<thread_ts>:<ts>`` is ``(thread_ts, ts)``."""

    matched = _MESSAGE_ID.match(value)
    if matched is None:
        return None
    first, second = matched.group(1), matched.group(2)
    return (None, first) if second is None else (first, second)


def record_id(ts: str, thread_ts: str | None) -> str:
    return f"{thread_ts}:{ts}" if thread_ts and thread_ts != ts else ts


def _slack_ts(moment: datetime) -> str:
    micros = epoch_micros(moment)
    return f"{micros // 1_000_000}.{micros % 1_000_000:06d}"


def _ts_micros(ts: str) -> int:
    seconds, _, fraction = ts.partition(".")
    return int(seconds) * 1_000_000 + int((fraction + "000000")[:6])


def _rfc3339(ts: str) -> str:
    moment = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=_ts_micros(ts))
    return moment.isoformat().replace("+00:00", "Z")


def _provider_error() -> ChannelReadRefused:
    return ChannelReadRefused(502, "provider_error", "Slack could not answer the read")


def _retry_after(response: httpx.Response) -> int | None:
    try:
        return max(0, int(response.headers["Retry-After"]))
    except (KeyError, ValueError):
        return None


class SlackChannelReader:
    def __init__(self, http: httpx.AsyncClient, tokens: Mapping[str, str]) -> None:
        self._http = http
        self._tokens = tokens

    def has_identity(self, identity: str) -> bool:
        return bool(self._tokens.get(identity))

    def identity_key(self, identity: str) -> str:
        """A stable, non secret name for the bot credential behind ``identity``."""

        return hashlib.sha256(self._token(identity).encode()).hexdigest()[:24]

    def _token(self, identity: str) -> str:
        token = self._tokens.get(identity)
        if not token:
            raise ChannelReadRefused(
                503, "provider_unconfigured", "no Slack credential serves this binding"
            )
        return token

    async def _call(
        self, token: str, method: str, params: Mapping[str, str], *, scope: str
    ) -> dict[str, Any]:
        try:
            response = await self._http.get(
                SLACK_API + method,
                params=dict(params),
                headers={"Authorization": f"Bearer {token}"},
            )
        except httpx.HTTPError:
            raise _provider_error() from None
        if response.status_code == 429:
            raise ChannelReadRefused(
                429,
                "provider_rate_limited",
                "Slack is rate limiting reads; retry later",
                retry_after=_retry_after(response),
            )
        if response.status_code != 200:
            raise _provider_error()
        try:
            body = response.json()
        except ValueError:
            raise _provider_error() from None
        if not isinstance(body, dict):
            raise _provider_error()
        if body.get("ok") is True:
            return body
        error = body.get("error")
        if error == "ratelimited":
            raise ChannelReadRefused(
                429,
                "provider_rate_limited",
                "Slack is rate limiting reads; retry later",
                retry_after=_retry_after(response),
            )
        if error in _NOT_MEMBER:
            raise ChannelReadRefused(
                403, "not_member", "the Slack app is not a member of this channel"
            )
        if error == "thread_not_found" and scope == "thread":
            raise ChannelReadRefused(
                404, "thread_not_in_channel", "the thread is not in this channel"
            )
        if error == "thread_not_found" and scope == "message":
            raise ChannelReadRefused(404, "message_not_found", "no such message in this channel")
        raise _provider_error()

    async def _permalink_base(self, token: str) -> str:
        digest = hashlib.sha256(token.encode()).hexdigest()
        cached = _PERMALINK_BASES.get(digest)
        if cached is not None:
            return cached
        body = await self._call(token, "auth.test", {}, scope="auth")
        url = body.get("url")
        if not isinstance(url, str) or not url.startswith("https://"):
            raise _provider_error()
        base = url if url.endswith("/") else url + "/"
        _PERMALINK_BASES[digest] = base
        return base

    @staticmethod
    def _raw_messages(body: Mapping[str, Any]) -> list[dict[str, Any]]:
        raw = body.get("messages")
        if not isinstance(raw, list):
            raise _provider_error()
        records: list[dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict) or not valid_slack_ts(str(item.get("ts", ""))):
                raise _provider_error()
            records.append(item)
        return records

    @staticmethod
    def _next_cursor(body: Mapping[str, Any]) -> str | None:
        # Read whatever has_more says: a cursor Slack hands back is the way on.
        metadata = body.get("response_metadata")
        cursor = metadata.get("next_cursor") if isinstance(metadata, dict) else None
        return cursor if isinstance(cursor, str) and cursor else None

    @staticmethod
    def _continuation(
        body: Mapping[str, Any], seen: list[int], window: ResolvedWindow, *, backwards: bool
    ) -> tuple[str | None, int | None]:
        """Slack's cursor, else a time boundary when Slack says more but gives none.

        The boundary only ever narrows the window, so a later page can neither
        widen the read nor repeat a record.
        """

        cursor = SlackChannelReader._next_cursor(body)
        if cursor is not None or body.get("has_more") is not True or not seen:
            return cursor, None
        if backwards:
            boundary = min(seen)
            return None, boundary if boundary > epoch_micros(window.oldest) else None
        boundary = max(seen) + 1
        inside = epoch_micros(window.oldest) < boundary < epoch_micros(window.latest)
        return None, boundary if inside else None

    @staticmethod
    def _record(raw: Mapping[str, Any], *, channel: str, base: str) -> ChannelMessage:
        ts = str(raw["ts"])
        thread_ts = raw.get("thread_ts")
        if not isinstance(thread_ts, str) or not valid_slack_ts(thread_ts):
            thread_ts = None
        reply = thread_ts is not None and thread_ts != ts
        text = raw.get("text")
        text = text if isinstance(text, str) else ""
        truncated = len(text) > TEXT_LIMIT
        if truncated:
            text = text[:TEXT_LIMIT] + TRUNCATION_MARKER
        author = raw.get("user") or raw.get("bot_id") or "unknown"
        provenance = f"{base}archives/{channel}/p{ts.replace('.', '')}"
        if reply:
            provenance += f"?thread_ts={thread_ts}&cid={channel}"
        reply_count = raw.get("reply_count")
        return ChannelMessage(
            id=record_id(ts, thread_ts),
            thread_id=thread_ts if reply else None,
            timestamp=_rfc3339(ts),
            author=str(author),
            text=text,
            truncated=truncated,
            provenance=provenance,
            reply_count=reply_count if isinstance(reply_count, int) else None,
        )

    @staticmethod
    def _window_params(window: ResolvedWindow, limit: int, cursor: str | None) -> dict[str, str]:
        # Both ends are sent inclusive; the end is dropped afterwards, so a
        # boundary page sends the microsecond before its exclusive end.
        params = {
            "oldest": _slack_ts(window.oldest),
            "latest": _slack_ts(window.latest),
            "inclusive": "true",
            "limit": str(limit),
        }
        if cursor:
            params["cursor"] = cursor
        return params

    @staticmethod
    def _inside(ts: str, window: ResolvedWindow) -> bool:
        # Slack's inclusive flag covers both ends; the end is dropped here so
        # the interval is [oldest, latest).
        value = _ts_micros(ts)
        return epoch_micros(window.oldest) <= value < epoch_micros(window.latest)

    async def history(
        self,
        *,
        identity: str,
        channel: str,
        window: ResolvedWindow,
        limit: int,
        cursor: str | None,
        boundary: int | None = None,
    ) -> ProviderPage:
        token = self._token(identity)
        if boundary is not None:
            window = ResolvedWindow(window.oldest, from_epoch_micros(boundary))
        sent = ResolvedWindow(window.oldest, from_epoch_micros(epoch_micros(window.latest) - 1))
        body = await self._call(
            token,
            HISTORY_METHOD,
            {
                "channel": channel,
                **self._window_params(sent if boundary is not None else window, limit, cursor),
            },
            scope="history",
        )
        raw = self._raw_messages(body)
        next_cursor, next_boundary = self._continuation(
            body, [_ts_micros(str(m["ts"])) for m in raw], window, backwards=True
        )
        # History is parents only: a reply, a thread_broadcast included, is
        # read through its thread.
        parents = [
            m
            for m in raw
            if self._inside(str(m["ts"]), window) and m.get("thread_ts") in (None, m["ts"])
        ]
        base = await self._permalink_base(token)
        return ProviderPage(
            messages=[self._record(m, channel=channel, base=base) for m in parents],
            next_cursor=next_cursor,
            boundary=next_boundary,
        )

    async def thread(
        self,
        *,
        identity: str,
        channel: str,
        thread_ts: str,
        window: ResolvedWindow,
        limit: int,
        cursor: str | None,
        boundary: int | None = None,
    ) -> ProviderPage:
        token = self._token(identity)
        if boundary is not None:
            window = ResolvedWindow(from_epoch_micros(boundary), window.latest)
        # Slack counts the parent, always the first element, toward the
        # limit, so one more is asked for and the page is trimmed below. A
        # page holding only the parent, with more promised and no way on, is
        # asked once more with room for a reply before it is called incomplete.
        for extra in (1, 2):
            body = await self._call(
                token,
                REPLIES_METHOD,
                {
                    "channel": channel,
                    "ts": thread_ts,
                    **self._window_params(window, limit + extra, cursor),
                },
                scope="thread",
            )
            replies, seen = self._replies(body, thread_ts, window)
            next_cursor, next_boundary = self._continuation(
                body, seen, window, backwards=False
            )
            stuck = (
                not replies
                and body.get("has_more") is True
                and next_cursor is None
                and next_boundary is None
            )
            if not stuck:
                break
        else:
            raise ChannelReadRefused(
                502,
                "provider_incomplete",
                "Slack promised more replies but returned none and no way to continue",
            )
        replies.sort(key=lambda m: _ts_micros(str(m["ts"])))
        if len(replies) > limit:
            # The trimmed reply is read next, from just after the last one kept.
            replies = replies[:limit]
            next_cursor, next_boundary = None, _ts_micros(str(replies[-1]["ts"])) + 1
        base = await self._permalink_base(token)
        return ProviderPage(
            messages=[self._record(m, channel=channel, base=base) for m in replies],
            next_cursor=next_cursor,
            boundary=next_boundary,
        )

    def _replies(
        self, body: Mapping[str, Any], thread_ts: str, window: ResolvedWindow
    ) -> tuple[list[dict[str, Any]], list[int]]:
        """The in-window replies of one replies page, and every ts that moves paging on."""

        replies: list[dict[str, Any]] = []
        seen: list[int] = []
        for message in self._raw_messages(body):
            if message["ts"] == thread_ts:
                # Never returned, but when Slack counts the parent toward the
                # limit a page can hold nothing else, so it still moves the
                # boundary on. Every reply is later than its parent.
                if self._inside(str(message["ts"]), window):
                    seen.append(_ts_micros(str(message["ts"])))
                continue
            seen.append(_ts_micros(str(message["ts"])))
            # A reply from any other thread is never returned, whatever Slack sent.
            if message.get("thread_ts") != thread_ts:
                raise _provider_error()
            if self._inside(str(message["ts"]), window):
                replies.append(message)
        return replies, seen

    async def message(
        self, *, identity: str, channel: str, message_id: str
    ) -> ChannelMessage | None:
        parsed = parse_message_id(message_id)
        if parsed is None:
            return None
        thread_ts, ts = parsed
        token = self._token(identity)
        params = {"channel": channel, "oldest": ts, "latest": ts, "inclusive": "true"}
        if thread_ts is None:
            body = await self._call(
                token, HISTORY_METHOD, {**params, "limit": "1"}, scope="message"
            )
        else:
            body = await self._call(
                token, REPLIES_METHOD, {**params, "ts": thread_ts}, scope="message"
            )
        for raw in self._raw_messages(body):
            if raw["ts"] != ts:
                continue
            if thread_ts is not None and raw.get("thread_ts") != thread_ts:
                continue
            base = await self._permalink_base(token)
            return self._record(raw, channel=channel, base=base)
        return None
