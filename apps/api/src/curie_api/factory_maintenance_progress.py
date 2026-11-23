"""Append maintenance outcomes while the caller holds the WorkItem lock."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .config import get_settings
from .factory_maintenance import MaintenanceObservation
from .models import ExecutionRequest, FactoryMaintenanceAdmission, FactoryMaintenanceEvent, WorkItem


async def record_event(
    session: AsyncSession,
    observation: MaintenanceObservation | FactoryMaintenanceAdmission,
    outcome: str,
    *,
    repair_request_id: UUID | None = None,
) -> None:
    source_request_id = (
        observation.source_request_id
        if isinstance(observation, FactoryMaintenanceAdmission)
        else observation.request_id
    )
    previous = await session.scalar(
        select(FactoryMaintenanceEvent)
        .where(
            FactoryMaintenanceEvent.work_item_id == observation.work_item_id,
            FactoryMaintenanceEvent.lineage_id == observation.lineage_id,
            FactoryMaintenanceEvent.reason == observation.reason,
        )
        .order_by(FactoryMaintenanceEvent.id.desc())
        .limit(1)
    )
    if (
        previous is not None
        and previous.head_sha == observation.head_sha
        and previous.outcome == outcome
        and (
            outcome not in {"admitted", "base_changed"}
            or (
                previous.base_ref == observation.base_ref
                and previous.base_sha == observation.base_sha
            )
        )
    ):
        return
    session.add(
        FactoryMaintenanceEvent(
            work_item_id=observation.work_item_id,
            source_request_id=source_request_id,
            lineage_id=observation.lineage_id,
            head_sha=observation.head_sha,
            base_ref=observation.base_ref,
            base_sha=observation.base_sha,
            reason=observation.reason,
            outcome=outcome,
            repair_request_id=repair_request_id,
        )
    )
    await session.flush()


async def record_ci_settlement(
    session: AsyncSession,
    item: WorkItem,
    request: ExecutionRequest,
    *,
    head_sha: str,
    green: bool,
) -> None:
    """The successful exact-head CI settlement owns this event, once per request."""
    if item.publication_lineage_id is None:
        return
    session.add(
        FactoryMaintenanceEvent(
            work_item_id=item.id,
            source_request_id=request.id,
            lineage_id=item.publication_lineage_id,
            head_sha=head_sha,
            base_ref=item.base_branch,
            base_sha=item.base_commit,
            reason="ci_settlement",
            outcome="green" if green else "no_ci",
            notify_slack=get_settings().factory_maintenance_repair_enabled,
        )
    )
