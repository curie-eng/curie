"""The approver-DM resolution mode and its registered card (ADR 0272).

A route whose ``resolution`` is ``{"mode": "approver_dm", "identity": <name>}``
delivers its one card to the selected approver's DM through that Slack
identity. The card's registration writes one ``card_registered`` audit row
whose evidence records the identity, installation, namespace, recipient and DM
channel; that record, not the current route, decides how an answer and an
approved resume are judged. @spec APPROVER-DM-1 APPROVER-DM-2 APPROVER-DM-3.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Approval, ApprovalAuditEntry
from .schemas.approvals import APPROVER_DM_MODE, ApprovalApproverDmTarget, ApprovalRouteBinding

__all__ = [
    "APPROVER_DM_MODE", "CARD_REGISTERED", "EVIDENCE_KIND", "DmCard", "card", "dm_route",
]
CARD_REGISTERED = "card_registered"
EVIDENCE_KIND = "approver_dm_card"


@dataclass(frozen=True)
class DmCard:
    """The registered DM card of one approval. @spec APPROVER-DM-2 APPROVER-DM-3."""

    identity: str
    provider_installation_id: str
    namespace_id: str
    recipient: str
    channel: str


def dm_route(binding: Any) -> str | None:
    """The identity of a route binding's approver-DM resolution, else None.

    Takes the raw stored binding or a parsed ``ApprovalRouteBinding``.
    @spec APPROVER-DM-1.
    """
    if isinstance(binding, ApprovalRouteBinding):
        target = binding.resolution
        return target.identity if isinstance(target, ApprovalApproverDmTarget) else None
    resolution = binding.get("resolution") if isinstance(binding, Mapping) else None
    if not isinstance(resolution, Mapping) or resolution.get("mode") != APPROVER_DM_MODE:
        return None
    identity = resolution.get("identity")
    return identity if isinstance(identity, str) else None


async def card(session: AsyncSession, approval: Approval) -> DmCard | None:
    """The approval's latest ``card_registered`` record, or None when it has none.

    A marker whose evidence does not parse raises ``ValueError``: a recorded DM
    card is never silently read as an ordinary row. @spec APPROVER-DM-3.
    """
    entry = await session.scalar(
        select(ApprovalAuditEntry)
        .where(
            ApprovalAuditEntry.approval_id == approval.id,
            ApprovalAuditEntry.action == CARD_REGISTERED,
        )
        .order_by(ApprovalAuditEntry.created_at.desc(), ApprovalAuditEntry.id.desc())
        .limit(1)
    )
    if entry is None:
        return None
    evidence = entry.evidence
    fields = ("identity", "provider_installation_id", "namespace_id", "recipient", "channel")
    if (
        not isinstance(evidence, Mapping)
        or evidence.get("kind") != EVIDENCE_KIND
        or not all(isinstance(evidence.get(name), str) for name in fields)
    ):
        raise ValueError("approver DM card record is malformed")
    return DmCard(*(str(evidence[name]) for name in fields))
