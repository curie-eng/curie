"""Private, append-only factory execution transcript events (#5608)."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from curie_telemetry.redact import redact_text
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from .models import ExecutionRequest, FactoryTranscriptEvent

RETENTION = timedelta(days=30)
MAX_EVENT_BYTES = 4 * 1024 * 1024
WRITE_SCOPE = "work_item.transcript.write"
EventKind = Literal[
    "prompt",
    "message",
    "result",
    "control",
    "capture_complete",
    "capture_gap",
    "chunk",
]


class TranscriptEventIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: uuid.UUID
    boot_id: uuid.UUID
    sequence: int = Field(ge=1)
    occurred_at: datetime
    kind: EventKind
    content: dict[str, Any]


def _redact(value: Any) -> tuple[Any, bool]:
    if isinstance(value, str):
        safe = redact_text(value)
        return safe, safe != value
    if isinstance(value, list):
        list_parts = [_redact(part) for part in value]
        return [part for part, _ in list_parts], any(changed for _, changed in list_parts)
    if isinstance(value, dict):
        output: dict[str, Any] = {}
        changed = False
        for key, part in value.items():
            safe_key = redact_text(key)
            safe_part, part_changed = _redact(part)
            output[safe_key] = safe_part
            changed = changed or safe_key != key or part_changed
        return output, changed
    return value, False


async def record_event(
    session: AsyncSession, request_id: uuid.UUID, event: TranscriptEventIn
) -> Literal["recorded", "duplicate", "request_not_found", "event_conflict", "too_large"]:
    """Insert once per event ID; reject a retry that changes its content."""

    request = await session.get(ExecutionRequest, request_id)
    if request is None:
        return "request_not_found"
    safe, redacted = _redact(event.content)
    if len(json.dumps(safe, separators=(",", ":"), ensure_ascii=False).encode()) > MAX_EVENT_BYTES:
        return "too_large"
    existing = await session.scalar(
        select(FactoryTranscriptEvent).where(
            FactoryTranscriptEvent.execution_request_id == request_id,
            FactoryTranscriptEvent.event_id == event.event_id,
        )
    )
    if existing is not None:
        return (
            "duplicate"
            if existing.boot_id == event.boot_id
            and existing.sequence == event.sequence
            and existing.kind == event.kind
            and existing.content == safe
            else "event_conflict"
        )
    statement = (
        insert(FactoryTranscriptEvent)
        .values(
            execution_request_id=request_id,
            event_id=event.event_id,
            boot_id=event.boot_id,
            sequence=event.sequence,
            occurred_at=event.occurred_at,
            kind=event.kind,
            content=safe,
            redacted=redacted,
            expires_at=request.created_at + RETENTION,
        )
        .on_conflict_do_nothing()
        .returning(FactoryTranscriptEvent.id)
    )
    inserted = await session.scalar(statement)
    if inserted is None:
        # A concurrent retry may have inserted the same event after our first
        # read. Classify it by its persisted value before reporting conflict.
        existing = await session.scalar(
            select(FactoryTranscriptEvent).where(
                FactoryTranscriptEvent.execution_request_id == request_id,
                FactoryTranscriptEvent.event_id == event.event_id,
            )
        )
        return (
            "duplicate"
            if existing is not None
            and existing.boot_id == event.boot_id
            and existing.sequence == event.sequence
            and existing.kind == event.kind
            and existing.content == safe
            else "event_conflict"
        )
    await session.commit()
    return "recorded"


async def page_events(
    session: AsyncSession, request: ExecutionRequest, *, after: int, limit: int
) -> dict[str, Any]:
    """Return a stable cursor page and run-level capture status."""

    request_id = request.id
    rows = list(
        await session.scalars(
            select(FactoryTranscriptEvent)
            .where(
                FactoryTranscriptEvent.execution_request_id == request_id,
                FactoryTranscriptEvent.id > after,
                FactoryTranscriptEvent.expires_at > func.now(),
            )
            .order_by(FactoryTranscriptEvent.id)
            .limit(limit + 1)
        )
    )
    visible = rows[:limit]
    all_markers = list(
        await session.scalars(
            select(FactoryTranscriptEvent)
            .where(
                FactoryTranscriptEvent.execution_request_id == request_id,
                FactoryTranscriptEvent.expires_at > func.now(),
            )
            .order_by(FactoryTranscriptEvent.id)
        )
    )
    by_boot: dict[uuid.UUID, list[int]] = {}
    for event in all_markers:
        by_boot.setdefault(event.boot_id, []).append(event.sequence)
    gaps = [
        {"boot_id": str(boot_id), "after_sequence": previous, "before_sequence": current}
        for boot_id, sequences in by_boot.items()
        for previous, current in zip([0, *sequences[:-1]], sequences, strict=True)
        if current != previous + 1
    ]
    complete = (
        request.terminal_at is not None
        and bool(all_markers)
        and all_markers[-1].kind == "capture_complete"
        and not any(event.kind == "capture_gap" for event in all_markers)
        and not gaps
    )
    status = (
        "expired"
        if request.created_at + RETENTION <= datetime.now(UTC)
        else "unavailable"
        if not all_markers
        else "complete"
        if complete
        else "incomplete"
    )
    return {
        "status": status,
        "gaps": gaps,
        "redacted": any(event.redacted for event in all_markers),
        "expires_at": (request.created_at + RETENTION).isoformat(),
        "events": [
            {
                "cursor": row.id,
                "event_id": str(row.event_id),
                "boot_id": str(row.boot_id),
                "sequence": row.sequence,
                "occurred_at": row.occurred_at.isoformat(),
                "kind": row.kind,
                "content": row.content,
                "redacted": row.redacted,
            }
            for row in visible
        ],
        "next_cursor": visible[-1].id if len(rows) > limit else None,
    }


async def expire_events(session: AsyncSession, *, limit: int = 1000) -> int:
    """Remove one bounded batch; every API replica may sweep safely."""

    expired = (
        select(FactoryTranscriptEvent.id)
        .where(FactoryTranscriptEvent.expires_at <= func.now())
        .order_by(FactoryTranscriptEvent.expires_at, FactoryTranscriptEvent.id)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    rows = await session.execute(
        delete(FactoryTranscriptEvent)
        .where(FactoryTranscriptEvent.id.in_(expired))
        .returning(FactoryTranscriptEvent.id)
    )
    count = len(rows.all())
    await session.commit()
    return count
