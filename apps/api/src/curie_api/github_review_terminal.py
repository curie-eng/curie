"""Read exact worker terminal evidence; absence never means completion."""

import json
from typing import Any

import redis.asyncio as redis

from .config import Settings


async def worker_event_is_terminal(
    valkey: redis.Redis, settings: Settings, event_id: str
) -> bool:
    """Mirror Markers.is_terminal without becoming another terminal writer.

    The worker alone records terminal completion under its delivery fence. A
    missing queue entry or an expired lease supplies no terminal evidence.
    Executable parity tests cover default/custom prefixes, the current/stale
    fence, and the independently retained completion-outbox flag.
    """
    async with valkey.pipeline(transaction=False) as pipe:
        pipe.exists(f"{settings.worker_key_prefix}:done:{event_id}")
        pipe.hget(f"{settings.worker_key_prefix}:completion:{event_id}", "done")
        marker, flag = await pipe.execute()
    return bool(marker) or flag in ("1", b"1")


async def read_review_dead_letter(
    valkey: redis.Redis,
    settings: Settings,
    *,
    stream_id: str,
    turn: dict[str, Any],
    cursor: str | None,
) -> tuple[bool, str | None]:
    """Observe at most 128 existing graveyard rows; the caller owns SQL cursor CAS.

    Missing or trimmed records remain unknown. Malformed data naming the exact
    original delivery raises before returning a cursor, so the caller cannot
    commit progress past a matching record it failed to process.
    """
    entries = await valkey.xrange(
        settings.dead_letter_stream_name(), min=f"({cursor}" if cursor else "-", count=128
    ) or []
    observed = cursor
    for entry_id, fields in entries:
        if entry_id is None or fields is None:
            raise ValueError("review dead-letter record is unreadable")
        record_id = entry_id.decode() if isinstance(entry_id, bytes) else entry_id
        # The API uses byte responses; worker test clients may decode them.
        original_id = fields.get("dl_original_id", fields.get(b"dl_original_id"))
        if original_id in (stream_id, stream_id.encode()):
            try:
                candidate = json.loads(fields.get("payload", fields.get(b"payload", "")))
            except (ValueError, TypeError):
                raise ValueError("matching review dead-letter payload is unreadable") from None
            if candidate == turn:
                return True, record_id
        observed = record_id
    return False, observed
