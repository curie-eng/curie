"""Durable factory transcript capture before replay reduction (#5608)."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime
from typing import Any

import aiohttp
from claude_agent_sdk import (
    AssistantMessage,
    ResultMessage,
    SystemMessage,
    TaskNotificationMessage,
    TaskUpdatedMessage,
    UserMessage,
)

from .adapter import _content_block_to_dict
from .redact import redact_text

TRANSCRIPT_URL_ENV = "CURIE_FACTORY_TRANSCRIPT_URL"
TRANSCRIPT_TOKEN_ENV = "CURIE_FACTORY_TRANSCRIPT_TOKEN"
MAX_DIRECT_CONTENT_BYTES = 3 * 1024 * 1024
CHUNK_CHARACTERS = 400_000


class TranscriptCaptureError(RuntimeError):
    """A runner-visible event could not be durably recorded."""


def visible_message(message: object) -> tuple[str, dict[str, Any]] | None:
    """Keep SDK messages, including subagent messages omitted from replay."""

    if isinstance(message, UserMessage | AssistantMessage):
        if isinstance(message.content, str):
            content: str | list[dict[str, Any]] = message.content
        else:
            content = [
                part
                for block in message.content
                if (part := _content_block_to_dict(block)) is not None
            ]
        return "message", {
            "role": "user" if isinstance(message, UserMessage) else "assistant",
            "content": content,
            "parent_tool_use_id": message.parent_tool_use_id,
        }
    if isinstance(message, ResultMessage):
        return "result", {
            "subtype": message.subtype,
            "is_error": message.is_error,
            "result": message.result,
            "num_turns": message.num_turns,
        }
    if isinstance(message, SystemMessage):
        return "control", {"subtype": message.subtype, "data": message.data}
    if isinstance(message, TaskUpdatedMessage):
        return "control", {
            "subtype": message.subtype,
            "task_id": message.task_id,
            "patch": message.patch,
        }
    if isinstance(message, TaskNotificationMessage):
        return "control", {
            "subtype": message.subtype,
            "task_id": message.task_id,
            "status": message.status,
            "summary": message.summary,
            "output_file": message.output_file,
        }
    return None


def _scrub(value: Any) -> Any:
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, list):
        return [_scrub(part) for part in value]
    if isinstance(value, dict):
        return {redact_text(key): _scrub(part) for key, part in value.items()}
    return value


def _event_parts(kind: str, content: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Split an oversized logical event without dropping any of its content."""

    safe = _scrub(content)
    serialized = json.dumps(safe, separators=(",", ":"), ensure_ascii=False)
    if len(serialized.encode()) <= MAX_DIRECT_CONTENT_BYTES:
        return [(kind, safe)]
    group_id = str(uuid.uuid4())
    fragments = [
        serialized[offset : offset + CHUNK_CHARACTERS]
        for offset in range(0, len(serialized), CHUNK_CHARACTERS)
    ]
    return [
        (
            "chunk",
            {
                "group_id": group_id,
                "source_kind": kind,
                "index": index,
                "count": len(fragments),
                "json_fragment": fragment,
            },
        )
        for index, fragment in enumerate(fragments)
    ]


class FactoryTranscriptRecorder:
    """Append each observed event under an idempotent per-boot sequence."""

    def __init__(self, url: str, token: str) -> None:
        if not url or not token:
            raise ValueError("factory transcript URL and token must be provided together")
        self._url = url
        self._token = token
        self._boot_id = uuid.uuid4()
        self._sequence = 0
        self._lock = asyncio.Lock()

    async def record(self, kind: str, content: dict[str, Any]) -> None:
        async with self._lock:
            for part_kind, part_content in _event_parts(kind, content):
                await self._record_one(part_kind, part_content)

    async def _record_one(self, kind: str, content: dict[str, Any]) -> None:
        self._sequence += 1
        payload = {
            "event_id": str(uuid.uuid4()),
            "boot_id": str(self._boot_id),
            "sequence": self._sequence,
            "occurred_at": datetime.now(UTC).isoformat(),
            "kind": kind,
            "content": content,
        }
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as client:
            for attempt in range(3):
                try:
                    async with client.post(
                        self._url,
                        data=body.encode(),
                        headers={
                            "X-API-Key": self._token,
                            "Content-Type": "application/json",
                        },
                    ) as response:
                        if response.status == 201:
                            return
                        if response.status < 500:
                            raise TranscriptCaptureError(
                                f"factory transcript write refused ({response.status})"
                            )
                except (aiohttp.ClientError, TimeoutError) as exc:
                    if attempt == 2:
                        raise TranscriptCaptureError(
                            "factory transcript write unavailable"
                        ) from exc
                if attempt < 2:
                    await asyncio.sleep(0.2 * (attempt + 1))
        raise TranscriptCaptureError("factory transcript write unavailable")
