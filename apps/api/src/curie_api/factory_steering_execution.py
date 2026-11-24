"""Seal Slack guidance into one execution's input and settle it at the fenced start."""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID

import httpx
from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from . import factory_steering_ledger as ledger
from .config import Settings
from .models import Agent, ExecutionRequest, ThreadPublicationLineage, WorkItem

_GUIDANCE_HEADER = "\n\nAdditional guidance:\n"
_EXPIRY = timedelta(hours=24)
_MAX_ENTRIES = 8
_MAX_INPUT = 65_536


async def selected_input(
    session: AsyncSession, request: ExecutionRequest
) -> tuple[str | None, list[ledger.SteeringEntry]]:
    if request.objective is None or request.execution_kind != "implementation":
        return request.objective, []
    entries = await ledger.selection(session, request)
    if not entries:
        return request.objective, []
    if any(entry.objective_embedded and entry.state == "refused" for entry in entries):
        # The revision's original objective contains the refused command itself.
        return None, entries
    objective = request.objective
    if request.retry_origin_request_id is not None:
        origin = await session.get(ExecutionRequest, request.retry_origin_request_id)
        assert origin is not None and origin.work_item_id == request.work_item_id
        assert origin.objective is not None
        objective = origin.objective
    return objective + "".join(
        _GUIDANCE_HEADER + entry.accepted_text
        for entry in entries
        if not entry.objective_embedded and entry.state != "refused"
    ), entries


async def execute_text(session: AsyncSession, request: ExecutionRequest) -> str | None:
    objective, _ = await selected_input(session, request)
    return objective


async def _select_pending(
    session: AsyncSession, item: WorkItem, request: ExecutionRequest, now: datetime, length: int
) -> list[UUID]:
    entries = await ledger.pending(session, item.id)
    if not entries:
        return []
    if item.publication_lineage_id is not None:
        lineage = await session.get(ThreadPublicationLineage, item.publication_lineage_id)
        if lineage is None or lineage.status != "open" or lineage.pr_number is not None:
            return []
    if not await session.scalar(
        select(Agent.factory_public_slack_channel)
        .where(Agent.id == item.agent_id)
        .with_for_update(read=True)
    ):
        return []
    selected: list[UUID] = []
    for entry in entries[:_MAX_ENTRIES]:
        if entry.created_at + _EXPIRY <= now:
            await ledger.refuse(session, item.id, [entry.steering_id], "guidance_expired")
            continue
        additional = len(_GUIDANCE_HEADER) + len(entry.accepted_text)
        if length + additional > _MAX_INPUT:
            await ledger.refuse(session, item.id, [entry.steering_id], "objective_too_large")
            continue
        length += additional
        selected.append(entry.steering_id)
    if selected:
        await ledger.assign(session, item.id, request.id, selected)
    return selected


async def requires_redispatch(
    session: AsyncSession,
    item: WorkItem,
    request: ExecutionRequest,
    *,
    already_acquired: bool,
    generation: int,
    digest: str | None,
    now: datetime,
    deadline: datetime,
    backoff_seconds: int,
) -> bool:
    if request.execution_kind != "implementation" or request.objective is None:
        return False
    objective, assigned = await selected_input(session, request)
    selected: list[UUID] = []
    if request.steering_sealed_at is None:
        request.steering_sealed_at = now
        if (
            objective is not None
            and not already_acquired
            and not any(entry.execution_request_id == request.id for entry in assigned)
        ):
            selected = await _select_pending(session, item, request, now, len(objective))
    if not selected and (
        not assigned or (objective is not None and ledger.digest(objective) == digest)
    ):
        return False
    due = min(
        now if selected else now + timedelta(seconds=backoff_seconds),
        deadline - timedelta(microseconds=1),
    )
    timing = (
        {"dispatch_not_before": due}
        if request.guarded_dispatch_not_before is None
        else {"guarded_dispatch_not_before": due}
    )
    changed = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request.id,
            ExecutionRequest.work_item_id == item.id,
            ExecutionRequest.status == "waiting",
            ExecutionRequest.dispatch_generation == generation,
        )
        .values(
            dispatch_generation=generation + 1,
            dispatch_owner=None,
            dispatch_lease_expires_at=None,
            acquired_generation=None,
            acquire_owner=None,
            acquire_expires_at=None,
            updated_at=func.clock_timestamp(),
            **timing,
        )
        .returning(ExecutionRequest.id)
    )
    if changed is None:
        raise RuntimeError("locked steering selection lost its dispatch generation")
    return True


_LIVE_RETRY = """
    SELECT FROM curie.execution_requests retry
    WHERE retry.work_item_id = assigned.work_item_id AND retry.sequence > assigned.sequence
      AND retry.retry_origin_request_id = COALESCE(assigned.retry_origin_request_id, assigned.id)
      AND retry.execution_kind = 'implementation'
      AND retry.status IN ('queued', 'waiting', 'running', 'cancellation_requested')
"""


async def due(session: AsyncSession, *, limit: int, after: UUID) -> list[tuple[UUID, UUID]]:
    rows = await session.execute(
        text(f"""
            SELECT entry.work_item_id, entry.steering_id
            FROM curie.factory_steering_ledger entry
            JOIN curie.work_items item ON item.id = entry.work_item_id
            LEFT JOIN curie.execution_requests assigned ON assigned.id = entry.execution_request_id
            LEFT JOIN curie.thread_publication_lineages lineage
                ON lineage.id = item.publication_lineage_id
            WHERE entry.state = 'queued' AND (
                item.cancelled_at IS NOT NULL OR lineage.status IN ('closed', 'merged')
                OR (assigned.status IN ('completed', 'failed', 'expired', 'cancelled')
                    AND NOT EXISTS ({_LIVE_RETRY}))
                OR (entry.execution_request_id IS NULL AND (
                    entry.created_at <= clock_timestamp() - interval '24 hours'
                    OR (item.readmit_request_id IS NULL AND NOT EXISTS (
                        SELECT 1 FROM curie.execution_requests active
                        WHERE active.work_item_id = item.id
                        AND active.status IN (
                            'queued', 'waiting', 'running', 'cancellation_requested'
                        )
                    ))
                ))
            )
            ORDER BY (entry.steering_id <= :after), entry.steering_id
            LIMIT :limit
        """),
        {"after": after, "limit": limit},
    )
    return [(row.work_item_id, row.steering_id) for row in rows]


async def redrive(
    session: AsyncSession,
    work_item_id: UUID,
    steering_id: UUID,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
) -> None:
    from . import factory_slack
    from .workitems.lifecycle import database_now, lock_work_item

    item = await lock_work_item(session, work_item_id)
    entry = await ledger.get(session, steering_id)
    if item is None or entry is None or entry.state != "queued" or entry.work_item_id != item.id:
        return
    lineage = (
        await session.get(ThreadPublicationLineage, item.publication_lineage_id)
        if item.publication_lineage_id is not None
        else None
    )
    reason = None
    if item.cancelled_at is not None:
        reason = "work_item_cancelled"
    elif lineage is not None and lineage.status != "open":
        reason = "lineage_closed"
    elif entry.execution_request_id is not None:
        if await session.scalar(
            text(f"""
                SELECT assigned.status IN ('queued', 'waiting', 'running', 'cancellation_requested')
                       OR EXISTS ({_LIVE_RETRY})
                FROM curie.execution_requests assigned
                WHERE assigned.id = :request AND assigned.work_item_id = :item
            """),
            {"request": entry.execution_request_id, "item": work_item_id},
        ):
            return
        reason = "execution_ended"
    elif entry.created_at + _EXPIRY <= await database_now(session):
        reason = "guidance_expired"
    elif item.readmit_request_id is not None or await session.scalar(
        select(ExecutionRequest.id)
        .where(
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.status.in_(("queued", "waiting", "running", "cancellation_requested")),
        )
        .limit(1)
    ):
        return
    elif lineage is None or lineage.pr_number is None:
        reason = "no_pull_request"
    if reason is not None:
        await ledger.refuse(session, work_item_id, [steering_id], reason)
    else:
        # The follow-up revalidates provider authority outside all row locks.
        await session.rollback()
        await factory_slack.redrive_pending(session, entry, settings=settings, client=client)
    await session.commit()
