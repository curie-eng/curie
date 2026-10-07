"""Completing approved undo approvals whose ruling never committed.

@spec AUTOMATED-REMEDIATION-19 @spec AUTOMATED-REMEDIATION-16

Approving an undo approval (``remediation_escalation``) commits the approval's
claim and its ``resolved`` audit row before the undo ruling runs. A failure in
between (the restore's insert failing, the process dying) leaves an approved
undo approval with no ruling. ``reconcile_undo_approvals``, run by the API's
approval sweeper beside ``reconcile_remediation_approvals``, finds each such
approval and runs the same ruling again under the approving principal: the
subject, channel and kind its ``resolved`` audit row recorded when the resolver
authenticated it. The ruling re-authorizes that principal against the route as
it stands now, so a revoked approver completes nothing.

Exactly once. A ruling driven by an approval names it in the evidence of every
audit row it writes and keys its restore ``restore:<action>:approval:<approval>``
(``routers/actions.py::rule_undo``). An approval is owed only while no
``undo_ruling`` audit row of its record names it; a granted ruling, a refusal
and a concurrent duplicate (refused on the key) each end it. Commits per
approval; a failure is logged and retried on the next pass.
"""

from __future__ import annotations

import logging
import uuid
from typing import cast

import httpx
from fastapi import HTTPException
from sqlalchemy import String, and_, select
from sqlalchemy import cast as sql_cast
from sqlalchemy.ext.asyncio import AsyncSession

from .approval_auth import AuthenticatedApprovalPrincipal, AuthenticatedPrincipalKind
from .approvers import ApproverSetSelector
from .config import get_settings
from .models import (
    ActionAuditEntry,
    Approval,
    ApprovalAuditEntry,
    ApprovalStatus,
    RemediationEscalation,
)
from .remediation_approvals import REMEDIATION_PURPOSE
from .remediation_escalation import UNDO_DEDUPE_PREFIX, undo_subject
from .routers.actions import rule_undo
from .slack_approvers import build_approver_set_selector

logger = logging.getLogger(__name__)

_PRINCIPAL_KINDS: frozenset[str] = frozenset({"chat", "console", "operator", "adapter"})
_HTTP_TIMEOUT_S = 10.0


async def _approving_principal(
    session: AsyncSession, approval: Approval
) -> AuthenticatedApprovalPrincipal | None:
    """The principal the approval's winning resolution authenticated, or None."""

    entry = await session.scalar(
        select(ApprovalAuditEntry)
        .where(
            ApprovalAuditEntry.approval_id == approval.id,
            ApprovalAuditEntry.action == "resolved",
            ApprovalAuditEntry.authorized.is_(True),
            ApprovalAuditEntry.authenticated.is_(True),
            ApprovalAuditEntry.actor == approval.resolved_by,
        )
        .order_by(ApprovalAuditEntry.created_at.desc())
        .limit(1)
    )
    if entry is None or entry.principal_kind not in _PRINCIPAL_KINDS:
        return None
    return AuthenticatedApprovalPrincipal(
        subject=entry.actor,
        kind=cast(AuthenticatedPrincipalKind, entry.principal_kind),
        actor_channel=entry.actor_channel,
        adapter=entry.principal_subject,
    )


async def _owed(session: AsyncSession, limit: int) -> list[uuid.UUID]:
    """Approved undo approvals no ``undo_ruling`` audit row of their record names."""

    ruled = (
        select(ActionAuditEntry.id)
        .where(
            ActionAuditEntry.action_id == RemediationEscalation.action_id,
            ActionAuditEntry.actor_kind == "undo_ruling",
            ActionAuditEntry.evidence["approval_id"].astext == sql_cast(Approval.id, String),
        )
        .exists()
    )
    rows = (
        await session.scalars(
            select(Approval.id)
            .join(RemediationEscalation, RemediationEscalation.undo_approval_id == Approval.id)
            .where(
                and_(
                    Approval.purpose == REMEDIATION_PURPOSE,
                    Approval.dedupe_key.startswith(UNDO_DEDUPE_PREFIX, autoescape=True),
                    Approval.status == ApprovalStatus.approved,
                    RemediationEscalation.action_id.is_not(None),
                    ~ruled,
                )
            )
            .order_by(Approval.resolved_at)
            .limit(limit)
        )
    ).all()
    await session.commit()
    return list(rows)


async def reconcile_undo_approvals(
    session: AsyncSession,
    *,
    limit: int = 100,
    approver_sets: ApproverSetSelector | None = None,
) -> int:
    """Rerun the undo ruling of every approved undo approval it never decided.

    @spec AUTOMATED-REMEDIATION-19. ``approver_sets`` defaults to the API's own
    selector, built for the pass. Returns how many rulings this pass decided
    (granted or refused); every other approval is retried on the next pass.
    """

    owed = await _owed(session, limit)
    if not owed:
        return 0
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT_S) as http:
        selector = approver_sets or build_approver_set_selector(http, get_settings())
        decided = 0
        for approval_id in owed:
            decided += await _rerun(session, approval_id, selector)
    return decided


async def _rerun(
    session: AsyncSession, approval_id: uuid.UUID, approver_sets: ApproverSetSelector
) -> int:
    try:
        approval = await session.get(Approval, approval_id, populate_existing=True)
        if approval is None or approval.status != ApprovalStatus.approved:
            await session.commit()
            return 0
        subject = await undo_subject(session, approval)
        principal = await _approving_principal(session, approval)
        if subject is None or principal is None:
            await session.commit()
            logger.warning(
                "approved undo approval %s cannot be rerun: no record or no approving principal",
                approval_id,
            )
            return 0
        try:
            await rule_undo(
                session,
                subject,
                principal=principal,
                approver_sets=approver_sets,
                store=None,
                approval_id=approval_id,
            )
        except HTTPException as refused:
            # The ruling refused and recorded why, naming the approval: decided.
            logger.warning(
                "approved undo approval %s refused on rerun: %s", approval_id, refused.status_code
            )
            return 1
        logger.info("approved undo approval %s ruled on rerun", approval_id)
        return 1
    except Exception:  # noqa: BLE001 - one approval must not stop the pass
        await session.rollback()
        logger.exception("approved undo approval %s rerun failed", approval_id)
        return 0


__all__ = ["reconcile_undo_approvals"]
