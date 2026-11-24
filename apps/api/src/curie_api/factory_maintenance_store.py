"""Database authority and dispatch for maintenance on an existing factory PR."""

from __future__ import annotations

import uuid
from datetime import timedelta

from sqlalchemy import exists, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from . import factory_maintenance_progress
from .config import get_settings
from .factory_base_reconcile import transition_blocks
from .factory_maintenance import (
    MaintenanceAuthority,
    MaintenanceObservation,
    MaintenanceRefusal,
    MaintenanceRepair,
    RefusalReason,
)
from .factory_maintenance_reasons import maintenance_instructions
from .models import (
    ExecutionRequest,
    FactoryBaseTransition,
    FactoryMaintenanceAdmission,
    FactoryMaintenanceLineage,
    Publication,
    PublicationReviewReservation,
    ThreadPublicationLineage,
    WorkItem,
)
from .personal_execution import OwnerContext, OwnerTuple, compare_owner, verify_owner_context
from .workitems import lifecycle


def _owns_pr(item: WorkItem, lineage: ThreadPublicationLineage) -> bool:
    return (
        item.publication_lineage_id == lineage.id
        and item.agent_id == lineage.agent_id
        and item.conversation_id == lineage.conversation_id
        and item.repository == lineage.repository
        and item.repository_path.casefold() == lineage.repo_full_name.casefold()
        and item.code_host_installation_id == lineage.code_host_installation_id
        and bool(lineage.code_host_pr_id and lineage.pr_number and lineage.pr_url)
    )


async def pin_origin(
    session: AsyncSession, item: WorkItem, request: ExecutionRequest, *, head_sha: str, green: bool
) -> None:
    """Record successful exact-head settlement while its caller holds the WorkItem lock."""
    if (
        item.publication_lineage_id is None
        or item.cancelled_at is not None
        or request.work_item_id != item.id
        or request.status != "completed"
        or request.terminal_cause != "completed"
        or request.terminal_at is None
    ):
        return
    lineage = await session.get(ThreadPublicationLineage, item.publication_lineage_id)
    if (
        lineage is None
        or not _owns_pr(item, lineage)
        or lineage.status != "open"
        or lineage.head_sha != head_sha
    ):
        return
    window = await session.get(FactoryMaintenanceLineage, lineage.id)
    if window is None:
        window = FactoryMaintenanceLineage(
            lineage_id=lineage.id,
            work_item_id=item.id,
            origin_request_id=request.id,
            origin_head_sha=head_sha,
            origin_completed_at=request.terminal_at,
            expires_at=request.terminal_at + timedelta(hours=24),
        )
        session.add(window)
    window.latest_settled_request_id = request.id
    window.latest_settled_head_sha = head_sha
    if green:
        window.latest_green_head_sha = head_sha
        window.latest_green_at = request.terminal_at


async def replay(
    session: AsyncSession, observation: MaintenanceObservation
) -> MaintenanceRepair | None:
    receipt = await session.scalar(
        select(FactoryMaintenanceAdmission.repair_request_id)
        .join(
            ThreadPublicationLineage,
            ThreadPublicationLineage.id == FactoryMaintenanceAdmission.lineage_id,
        )
        .where(
            FactoryMaintenanceAdmission.work_item_id == observation.work_item_id,
            FactoryMaintenanceAdmission.lineage_id == observation.lineage_id,
            FactoryMaintenanceAdmission.code_host_kind == ThreadPublicationLineage.code_host_kind,
            FactoryMaintenanceAdmission.code_host_host == ThreadPublicationLineage.code_host_host,
            FactoryMaintenanceAdmission.repository_project_id
            == ThreadPublicationLineage.repository_project_id,
            FactoryMaintenanceAdmission.code_host_pr_id == ThreadPublicationLineage.code_host_pr_id,
            FactoryMaintenanceAdmission.head_sha == observation.head_sha,
            FactoryMaintenanceAdmission.base_sha == observation.base_sha,
            FactoryMaintenanceAdmission.reason == observation.reason,
            exists().where(
                ExecutionRequest.work_item_id == observation.work_item_id,
                ExecutionRequest.id == observation.request_id,
            ),
        )
    )
    return MaintenanceRepair(status="replayed", request_id=receipt) if receipt is not None else None


async def _owner_refusal(
    session: AsyncSession, item: WorkItem, request: ExecutionRequest
) -> RefusalReason | None:
    owner = OwnerTuple.of(item)
    mismatch = compare_owner(owner, OwnerTuple.of(request))
    if mismatch == "enrollment_generation_mismatch":
        return "enrollment_fenced"
    if mismatch or owner.tenant_id is None:
        return "owner_changed"
    if owner.is_personal:
        assert owner.owner_principal_id is not None and owner.enrollment_generation is not None
        refusal = await verify_owner_context(
            session,
            OwnerContext(owner.tenant_id, owner.owner_principal_id, owner.enrollment_generation),
            agent_id=item.agent_id,
        )
        if refusal in ("enrollment_inactive", "enrollment_generation_mismatch"):
            return "enrollment_fenced"
        if refusal:
            return "owner_changed"
    return None


async def _local_refusal(
    session: AsyncSession,
    observation: MaintenanceObservation,
    item: WorkItem,
    request: ExecutionRequest,
    lineage: ThreadPublicationLineage,
    window: FactoryMaintenanceLineage | None,
    provider_refusal: RefusalReason | None,
) -> RefusalReason | None:
    if not get_settings().factory_maintenance_repair_enabled:
        return "repair_disabled"
    if item.cancelled_at is not None:
        return "work_item_cancelled"
    if lineage.status != "open":
        return "pr_closed"
    if not _owns_pr(item, lineage):
        return "identity_unverified"
    if item.readmit_request_id is not None or await session.scalar(
        select(
            exists().where(
                ExecutionRequest.work_item_id == item.id,
                ExecutionRequest.sequence > request.sequence,
            )
        )
    ):
        return "superseded"
    if (
        request.status != "completed"
        or request.terminal_cause != "completed"
        or request.terminal_at is None
    ):
        return "source_not_completed"
    if (item.version, request.version, lineage.version) != (
        observation.work_item_version,
        observation.request_version,
        observation.lineage_version,
    ):
        return "snapshot_changed"
    if lineage.head_sha != observation.head_sha:
        return "foreign_head"
    if lineage.base_ref != observation.base_ref or (
        item.base_branch is not None and item.base_branch != observation.base_ref
    ):
        return "base_changed"
    if await session.scalar(
        select(
            or_(
                exists().where(
                    Publication.lineage_id == lineage.id,
                    Publication.status.in_(lifecycle.IN_FLIGHT_PUBLICATION),
                ),
                exists().where(
                    PublicationReviewReservation.lineage_id == lineage.id,
                    PublicationReviewReservation.status == "reserved",
                ),
            )
        )
    ):
        return "superseded"
    if (
        item.readmit_base_settle_until is not None
        or item.base_requested_branch is not None
        or item.base_requested_source is not None
        or item.base_label_ignored is not None
    ):
        return "retarget_pending"
    if await session.scalar(
        select(
            exists().where(
                FactoryBaseTransition.work_item_id == item.id,
                transition_blocks(),
            )
        )
    ):
        return "retarget_pending"
    if provider_refusal is not None:
        return provider_refusal
    if window is None:
        return (
            "prior_green_missing" if observation.reason == "late_ci_red" else "source_not_completed"
        )
    if window.work_item_id != item.id:
        return "identity_unverified"
    if (
        window.latest_settled_request_id != request.id
        or window.latest_settled_head_sha != observation.head_sha
    ):
        return "source_not_completed"
    if observation.reason == "late_ci_red" and window.latest_green_head_sha != observation.head_sha:
        return "prior_green_missing"
    if await lifecycle.database_now(session) >= window.expires_at:
        return "window_expired"
    if window.repairs_used:
        return "budget_exhausted"
    if not all(
        (
            request.objective,
            request.requester,
            request.reply_kind,
            request.reply_address,
            request.reply_conversation_id,
        )
    ):
        return "snapshot_changed"
    return await _owner_refusal(session, item, request)


async def commit_observation(
    session: AsyncSession,
    observation: MaintenanceObservation,
    *,
    objective: str | None,
    refusal: RefusalReason | None,
) -> MaintenanceRepair | MaintenanceRefusal:
    """Commit a verified provider decision, its first runnable wake and its single debit."""
    item = await lifecycle.lock_work_item(session, observation.work_item_id)
    prior = await replay(session, observation)
    if prior is not None:
        await session.commit()
        return prior
    request = await lifecycle.lock_request(
        session, work_item_id=observation.work_item_id, request_id=observation.request_id
    )
    if item is None or request is None or item.publication_lineage_id != observation.lineage_id:
        await session.commit()
        return MaintenanceRefusal(reason="identity_unverified")
    lineage = await session.get(
        ThreadPublicationLineage,
        observation.lineage_id,
        with_for_update=True,
        populate_existing=True,
    )
    if lineage is None:
        await session.commit()
        return MaintenanceRefusal(reason="identity_unverified")
    window = await session.get(
        FactoryMaintenanceLineage, lineage.id, with_for_update=True, populate_existing=True
    )
    refusal = await _local_refusal(session, observation, item, request, lineage, window, refusal)
    repair_id = uuid.uuid4()
    instructions = (
        maintenance_instructions(
            MaintenanceAuthority(
                repair_request_id=repair_id,
                reason=observation.reason,
                head_sha=observation.head_sha,
                base_ref=observation.base_ref,
                base_sha=observation.base_sha,
            ),
            objective=objective or "",
        )
        if refusal is None
        else None
    )
    if refusal is None:
        if instructions is None:
            refusal = "reason_unavailable"
        elif (
            not objective
            or not instructions.objective.strip()
            or len(instructions.objective) > 65536
        ):
            refusal = "evidence_unavailable"
    if refusal is not None:
        await factory_maintenance_progress.record_event(session, observation, refusal)
        await session.commit()
        return MaintenanceRefusal(reason=refusal)
    assert window is not None and instructions is not None
    now = await lifecycle.database_now(session)
    if now >= window.expires_at:
        await factory_maintenance_progress.record_event(session, observation, "window_expired")
        await session.commit()
        return MaintenanceRefusal(reason="window_expired")
    allocated = await lifecycle.insert_waiting_request(
        session,
        item,
        request_id=repair_id,
        wait_deadline=now + timedelta(seconds=get_settings().work_item_wait_budget_seconds),
        expected_work_item_version=observation.work_item_version,
        reserve_automatic_retry=False,
        columns={
            "objective": instructions.objective,
            "requester": request.requester,
            "reply_kind": request.reply_kind,
            "reply_address": request.reply_address,
            "reply_conversation_id": request.reply_conversation_id,
            "reply_target_kind": request.reply_target_kind,
            "reply_target_pr_number": request.reply_target_pr_number,
            "reply_target_comment_id": request.reply_target_comment_id,
            "reply_target_url": request.reply_target_url,
        },
    )
    assert allocated
    window.repairs_used += 1
    session.add(
        FactoryMaintenanceAdmission(
            lineage_id=lineage.id,
            work_item_id=item.id,
            source_request_id=request.id,
            repair_request_id=repair_id,
            code_host_kind=lineage.code_host_kind,
            code_host_host=lineage.code_host_host,
            repository_project_id=lineage.repository_project_id,
            code_host_pr_id=lineage.code_host_pr_id,
            pr_number=lineage.pr_number,
            head_sha=observation.head_sha,
            base_ref=observation.base_ref,
            base_sha=observation.base_sha,
            reason=observation.reason,
            mode="successor",
            admitted_at=now,
            **OwnerTuple.of(item).columns(),
        )
    )
    await factory_maintenance_progress.record_event(
        session, observation, "admitted", repair_request_id=repair_id
    )
    await session.commit()
    return MaintenanceRepair(status="admitted", request_id=repair_id)
