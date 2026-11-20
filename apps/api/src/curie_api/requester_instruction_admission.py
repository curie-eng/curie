"""Admission-only requester selection and metadata projections (ADR 0278)."""

from __future__ import annotations

import uuid
from typing import Any, cast

from curie_internal.requester_coding_instructions import (
    EvidenceState,
    SelectedCodingInstructions,
    select_coding_instructions,
)
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from .config import get_settings
from .models import Agent, Principal, Tenant, WorkItem, WorkItemCodingSnapshot
from .schemas.workitems import CodingInstructionsMetadata


async def requester_is_active(
    session: AsyncSession, *, agent_id: uuid.UUID, principal_id: uuid.UUID
) -> bool:
    return (
        await session.scalar(
            select(Principal.id)
            .join(Tenant, Tenant.id == Principal.tenant_id)
            .join(Agent, Agent.tenant_id == Tenant.id)
            .where(
                Agent.id == agent_id,
                Tenant.status == "active",
                Principal.id == principal_id,
                Principal.status == "active",
            )
            .with_for_update(read=True, of=(Principal, Tenant))
        )
        is not None
    )


async def selection_columns(
    session: AsyncSession, *, agent_id: uuid.UUID, principal_id: uuid.UUID | None
) -> tuple[dict[str, Any], SelectedCodingInstructions | None]:
    tenant_id = await session.scalar(select(Agent.tenant_id).where(Agent.id == agent_id))
    assert tenant_id is not None
    selection = await select_coding_instructions(
        session,
        tenant_id=tenant_id,
        principal_id=principal_id,
        application_available=get_settings().requester_coding_instructions_apply,
    )
    selected = selection if isinstance(selection, SelectedCodingInstructions) else None
    return {
        "requester_principal_id": principal_id,
        "coding_instructions_state": selection.kind,
        "coding_instructions_absence_reason": selection.reason
        if not isinstance(selection, SelectedCodingInstructions)
        else None,
        "coding_instructions_revision": selected.revision if selected else None,
        "coding_instructions_sha256": selected.sha256 if selected else None,
        "coding_instructions_format_version": selected.format_version if selected else None,
    }, selected


class CodingInstructionsUnavailable(RuntimeError):
    pass


async def add_snapshot(
    session: AsyncSession, *, work_item_id: uuid.UUID, selection: SelectedCodingInstructions | None
) -> None:
    if selection is not None:
        try:
            session.add(WorkItemCodingSnapshot(work_item_id=work_item_id, text=selection.text))
            await session.flush()
        except DBAPIError:
            raise CodingInstructionsUnavailable("coding_instructions_unavailable") from None


def metadata(item: WorkItem) -> CodingInstructionsMetadata:
    return CodingInstructionsMetadata.model_validate(
        {
            "state": item.coding_instructions_state,
            "absence_reason": item.coding_instructions_absence_reason,
            "revision": item.coding_instructions_revision,
            "sha256": item.coding_instructions_sha256,
            "format_version": item.coding_instructions_format_version,
        }
    )


def request_selection_columns(item: WorkItem) -> dict[str, Any]:
    return {
        "coding_instructions_selection_id": item.id,
        "coding_instructions_evidence": "pending"
        if item.coding_instructions_state == "selected"
        else "none",
    }


def evidence(value: str) -> EvidenceState:
    return cast(EvidenceState, value)
