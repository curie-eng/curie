"""Read and display voice feedback only in the Slack thread that supplied its clip."""

from __future__ import annotations

import asyncio
import hashlib
import html
import json
import re
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID, uuid5

import httpx
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from .files import (
    FileResponse,
    SlackFileClient,
    SlackFileError,
    SlackVoiceFile,
    file_transport,
    private_voice_request,
)
from .inbound_attachments import hosted_voice_clip_id
from .slack_text import split_verbatim_answer

MAX_VOICE_BYTES = 10 * 1024 * 1024
_MAX_WEB_API_BYTES = 256 * 1024
type VoiceReadReason = Literal[
    "source_changed", "forbidden", "media_limit", "invalid_media", "unavailable", "expired"
]
VOICE_ERROR_STATUS: dict[VoiceReadReason, int] = {
    "source_changed": 404,
    "forbidden": 403,
    "media_limit": 413,
    "invalid_media": 422,
    "unavailable": 503,
    "expired": 410,
}


class SlackVoiceReadError(Exception):
    def __init__(self, reason: VoiceReadReason) -> None:
        self.reason = reason
        super().__init__(reason)


class VoiceClipSource(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    team_id: str = Field(pattern=r"^T[A-Z0-9]+$")
    app_id: str = Field(pattern=r"^A[A-Z0-9]+$")
    channel: str = Field(pattern=r"^[CG][A-Z0-9]+$")
    root_ts: str = Field(pattern=r"^[0-9]+\.[0-9]+$")
    message_ts: str = Field(pattern=r"^[0-9]+\.[0-9]+$")
    author: str = Field(pattern=r"^[UW][A-Z0-9]+$")
    file_id: str = Field(pattern=r"^F[A-Z0-9]+$")
    content_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class VoiceDraftPost(VoiceClipSource):
    text: str = Field(min_length=1, max_length=70_000, repr=False)
    delivery_id: UUID
    expires_at: AwareDatetime | None


class SlackVoiceGateway:
    def __init__(
        self,
        http: httpx.AsyncClient,
        tokens: Mapping[str, str],
        *,
        base_url: str | None = None,
        file_transport: Callable[..., FileResponse] = file_transport,
    ) -> None:
        self._http = http
        self._tokens = tokens
        self._base = (base_url or "https://slack.com/api").rstrip("/")
        self._file_transport = file_transport

    async def _call(
        self,
        token: str,
        method: str,
        *,
        params: dict[str, str] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        verb = "POST" if method == "auth.test" or payload is not None else "GET"
        try:
            with private_voice_request():
                async with self._http.stream(
                    verb,
                    f"{self._base}/{method}",
                    params=params,
                    json=payload,
                    headers={"Authorization": f"Bearer {token}", "Accept-Encoding": "identity"},
                    follow_redirects=False,
                    timeout=10,
                ) as response:
                    if response.status_code != 200:
                        raise SlackVoiceReadError("unavailable")
                    if response.headers.get("content-encoding", "identity") != "identity":
                        raise SlackVoiceReadError("unavailable")
                    body = bytearray()
                    async for chunk in response.aiter_bytes(8192):
                        if len(chunk) > _MAX_WEB_API_BYTES - len(body):
                            raise SlackVoiceReadError("unavailable")
                        body.extend(chunk)
                    answer = json.loads(body)
        except (httpx.HTTPError, ValueError, RecursionError):
            raise SlackVoiceReadError("unavailable") from None
        if not isinstance(answer, dict):
            raise SlackVoiceReadError("unavailable")
        if answer.get("ok") is True:
            return answer
        error = answer.get("error")
        if error in ("thread_not_found", "message_not_found"):
            raise SlackVoiceReadError("source_changed")
        if error in ("not_in_channel", "channel_not_found", "no_permission"):
            raise SlackVoiceReadError("forbidden")
        raise SlackVoiceReadError("unavailable")

    async def _source_token(self, identity: str, source: VoiceClipSource) -> str:
        token = self._tokens.get(identity)
        if not token:
            raise SlackVoiceReadError("unavailable")
        if source.root_ts == source.message_ts:
            raise SlackVoiceReadError("source_changed")
        auth = await self._call(token, "auth.test")
        if auth.get("team_id") != source.team_id:
            raise SlackVoiceReadError("forbidden")
        bot_id, bot_user = auth.get("bot_id"), auth.get("user_id")
        if (
            not isinstance(bot_id, str)
            or not bot_id
            or not isinstance(bot_user, str)
            or not bot_user
        ):
            raise SlackVoiceReadError("unavailable")
        answer = await self._call(token, "bots.info", params={"bot": bot_id})
        bot = answer.get("bot")
        if not isinstance(bot, dict) or bot.get("id") != bot_id:
            raise SlackVoiceReadError("unavailable")
        if bot.get("app_id") != source.app_id:
            raise SlackVoiceReadError("forbidden")
        replies = await self._call(
            token,
            "conversations.replies",
            params={
                "channel": source.channel,
                "ts": source.root_ts,
                "oldest": source.message_ts,
                "latest": source.message_ts,
                "inclusive": "true",
                "limit": "2",
            },
        )
        messages = replies.get("messages")
        if not isinstance(messages, list) or not all(isinstance(m, dict) for m in messages):
            raise SlackVoiceReadError("unavailable")
        matches = [m for m in messages if m.get("ts") == source.message_ts]
        if not matches and (
            replies.get("has_more") or (replies.get("response_metadata") or {}).get("next_cursor")
        ):
            raise SlackVoiceReadError("unavailable")
        if len(matches) != 1:
            raise SlackVoiceReadError("source_changed")
        message = matches[0]
        text = message.get("text")
        if (
            message.get("type") != "message"
            or message.get("thread_ts") != source.root_ts
            or message.get("channel", source.channel) != source.channel
            or message.get("user") != source.author
            or source.author == bot_user
            or any(key in message for key in ("app_id", "bot_id", "bot_profile"))
            or message.get("subtype") not in (None, "file_share")
            or hosted_voice_clip_id(message) != source.file_id
            or not isinstance(text, str)
            or hashlib.sha256(text.encode()).hexdigest() != source.content_sha256
            or re.search(rf"<@{re.escape(bot_user)}(?:\|[^>]*)?>", text) is None
        ):
            raise SlackVoiceReadError("source_changed")
        return token

    async def read(
        self,
        identity: str,
        source: VoiceClipSource,
        max_bytes: int,
    ) -> SlackVoiceFile:
        async with asyncio.timeout(30):
            token = await self._source_token(identity, source)
        files = SlackFileClient(token=token, api_url=self._base, transport=self._file_transport)
        download = asyncio.create_task(
            asyncio.to_thread(
                files.fetch_voice,
                source.file_id,
                source.author,
                max_bytes=max_bytes,
                max_duration_ms=300_000,
            )
        )
        try:
            return await asyncio.shield(download)
        except asyncio.CancelledError:
            await asyncio.gather(download, return_exceptions=True)
            raise
        except SlackFileError as exc:
            if exc.status == 413:
                raise SlackVoiceReadError("media_limit") from None
            if exc.status == 404:
                raise SlackVoiceReadError("source_changed") from None
            if exc.status in (401, 403):
                raise SlackVoiceReadError("forbidden") from None
            if exc.status == 429 or (exc.status is not None and exc.status >= 500):
                raise SlackVoiceReadError("unavailable") from None
            raise SlackVoiceReadError("invalid_media") from None

    async def post(self, identity: str, draft: VoiceDraftPost) -> None:
        async with asyncio.timeout(30):
            token = await self._source_token(identity, draft)
            for index, part in enumerate(split_verbatim_answer(draft.text)):
                if draft.expires_at is not None and datetime.now(UTC) >= draft.expires_at:
                    raise SlackVoiceReadError("expired")
                result = await self._call(
                    token,
                    "chat.postMessage",
                    payload={
                        "channel": draft.channel,
                        "thread_ts": draft.root_ts,
                        "text": html.escape(part.content, quote=False),
                        "blocks": [
                            {
                                "type": "section",
                                "text": {
                                    "type": "plain_text",
                                    "text": part.content,
                                    "emoji": False,
                                },
                            }
                        ],
                        "client_msg_id": str(uuid5(draft.delivery_id, str(index))),
                        "parse": "none",
                        "link_names": False,
                        "mrkdwn": False,
                        "unfurl_links": False,
                        "unfurl_media": False,
                        "reply_broadcast": False,
                    },
                )
                if result.get("channel") != draft.channel or not result.get("ts"):
                    raise SlackVoiceReadError("unavailable")
