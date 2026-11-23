"""Restricted steering input and its first consuming execution, under the WorkItem lock."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .factory_confirmed_steering import (
    AppliedSteering,
    QueuedSteering,
    RefusedSteering,
    SteeringDecision,
)


@dataclass(frozen=True, kw_only=True)
class SteeringEntry:
    steering_id: UUID
    work_item_id: UUID
    source_identity: str
    source_channel: str
    source_root_ts: str
    source_facts: dict[str, Any] = field(repr=False)
    first_delivery_id: str
    source_content_sha256: str
    accepted_text_sha256: str
    accepted_text: str = field(repr=False)
    state: str
    execution_request_id: UUID | None
    objective_embedded: bool
    created_at: datetime
    applied_at: datetime | None
    refused_at: datetime | None
    refusal_code: str | None

    def decision(self, *, replayed: bool) -> SteeringDecision:
        if self.state == "refused":
            assert self.refusal_code is not None
            return RefusedSteering(reason=self.refusal_code)
        if self.state == "applied":
            assert self.execution_request_id is not None
            return AppliedSteering(
                steering_id=self.steering_id,
                execution_request_id=self.execution_request_id,
                replayed=replayed,
            )
        return QueuedSteering(
            steering_id=self.steering_id,
            execution_request_id=self.execution_request_id,
            replayed=replayed,
        )


def digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


async def get(session: AsyncSession, steering_id: UUID) -> SteeringEntry | None:
    row = (
        (
            await session.execute(
                text("SELECT * FROM curie.factory_steering_ledger WHERE steering_id = :id"),
                {"id": steering_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    return None if row is None else SteeringEntry(**row)


async def record(
    session: AsyncSession,
    *,
    steering_id: UUID,
    work_item_id: UUID,
    source_identity: str,
    source_channel: str,
    source_root_ts: str,
    source_facts: dict[str, Any],
    first_delivery_id: str,
    source_content_sha256: str,
    accepted_text: str,
    execution_request_id: UUID | None,
) -> None:
    await session.execute(
        text("""
        INSERT INTO curie.factory_steering_ledger
            (steering_id, work_item_id, source_identity, source_channel, source_root_ts,
             source_facts, first_delivery_id, source_content_sha256, accepted_text_sha256,
             accepted_text, execution_request_id, objective_embedded)
        VALUES (:id, :item, :identity, :channel, :root, CAST(:facts AS jsonb), :delivery,
                :source_hash, :text_hash, :body, :request, :embedded)
    """),
        {
            "id": steering_id,
            "item": work_item_id,
            "identity": source_identity,
            "channel": source_channel,
            "root": source_root_ts,
            "facts": json.dumps(source_facts),
            "delivery": first_delivery_id,
            "source_hash": source_content_sha256,
            "text_hash": digest(accepted_text),
            "body": accepted_text,
            "request": execution_request_id,
            "embedded": execution_request_id is not None,
        },
    )


async def pending(session: AsyncSession, work_item_id: UUID) -> list[SteeringEntry]:
    result = await session.execute(
        text("""
        SELECT * FROM curie.factory_steering_ledger
        WHERE work_item_id = :id AND state = 'queued' AND execution_request_id IS NULL
        ORDER BY created_at, steering_id
    """),
        {"id": work_item_id},
    )
    return [SteeringEntry(**row) for row in result.mappings()]


async def assigned(session: AsyncSession, request_id: UUID) -> list[SteeringEntry]:
    result = await session.execute(
        text("""
        SELECT * FROM curie.factory_steering_ledger WHERE execution_request_id = :id
        ORDER BY created_at, steering_id
    """),
        {"id": request_id},
    )
    return [SteeringEntry(**row) for row in result.mappings()]


async def assign(
    session: AsyncSession,
    work_item_id: UUID,
    request_id: UUID,
    ids: Sequence[UUID],
    *,
    objective_embedded: bool = False,
) -> None:
    changed = await session.scalars(
        text("""
        UPDATE curie.factory_steering_ledger
        SET execution_request_id = :request, objective_embedded = :embedded
        WHERE steering_id = ANY(CAST(:ids AS uuid[])) AND work_item_id = :item
          AND state = 'queued' AND execution_request_id IS NULL
        RETURNING steering_id
    """),
        {
            "ids": list(ids),
            "item": work_item_id,
            "request": request_id,
            "embedded": objective_embedded,
        },
    )
    if set(changed) != set(ids):
        raise ValueError("steering_assignment_changed")


async def apply(
    session: AsyncSession, work_item_id: UUID, request_id: UUID, ids: Sequence[UUID]
) -> None:
    entries = await assigned(session, request_id)
    if set(ids) != {entry.steering_id for entry in entries} or any(
        entry.work_item_id != work_item_id or entry.state == "refused" for entry in entries
    ):
        raise ValueError("steering_selection_changed")
    await session.execute(
        text("""
        UPDATE curie.factory_steering_ledger SET state = 'applied', applied_at = clock_timestamp()
        WHERE work_item_id = :item AND execution_request_id = :request AND state = 'queued'
    """),
        {"item": work_item_id, "request": request_id},
    )


async def refuse(
    session: AsyncSession, work_item_id: UUID, ids: Sequence[UUID], reason: str
) -> None:
    await session.execute(
        text("""
        UPDATE curie.factory_steering_ledger
        SET state = 'refused', refused_at = clock_timestamp(), refusal_code = :reason
        WHERE work_item_id = :item AND steering_id = ANY(CAST(:ids AS uuid[])) AND state = 'queued'
    """),
        {"item": work_item_id, "ids": list(ids), "reason": reason},
    )
