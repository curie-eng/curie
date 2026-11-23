"""Resolve conflict publication authority from a successor's immutable admission."""

from __future__ import annotations

import hashlib
import re
import uuid
from dataclasses import dataclass
from typing import Literal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .factory_maintenance import MaintenanceAuthority
from .factory_maintenance_reasons import maintenance_instructions
from .models import (
    ExecutionRequest,
    FactoryMaintenanceAdmission,
    Publication,
    ThreadPublicationLineage,
    WorkItem,
)
from .personal_execution import OwnerTuple
from .schemas.publications import PublicationCreate

_SHA = re.compile(r"[0-9a-f]{40}")


class ConflictAuthorityMismatch(Exception):
    """A conflict admission no longer belongs to this execution or open pull request."""


class ConflictPublicationSettled(ConflictAuthorityMismatch):
    """The publication outcome is durable and the request needs no further model work."""


@dataclass(frozen=True)
class ConflictPublicationAuthority:
    lineage_id: uuid.UUID
    head_sha: str
    base_sha: str
    publication_mode: Literal["base_conflict_merge"]

    def approval_arguments(self, patch: bytes) -> dict[str, str]:
        return {
            "publication_mode": self.publication_mode,
            "expected_prior_head": self.head_sha,
            "base_sha": self.base_sha,
            "patch_sha256": hashlib.sha256(patch).hexdigest(),
        }


async def is_conflict_successor(session: AsyncSession, request_id: uuid.UUID) -> bool:
    reason = await session.scalar(
        select(FactoryMaintenanceAdmission.reason).where(
            FactoryMaintenanceAdmission.repair_request_id == request_id
        )
    )
    return reason == "base_conflict"


async def conflict_publication_authority(
    session: AsyncSession, request: ExecutionRequest
) -> ConflictPublicationAuthority | None:
    row = await session.scalar(
        select(FactoryMaintenanceAdmission).where(
            FactoryMaintenanceAdmission.repair_request_id == request.id
        )
    )
    if row is None or row.reason == "late_ci_red":
        return None
    if (
        row.reason != "base_conflict"
        or row.mode != "successor"
        or row.work_item_id != request.work_item_id
        or request.execution_kind != "implementation"
        or not request.objective
    ):
        raise ConflictAuthorityMismatch
    source = await session.get(ExecutionRequest, row.source_request_id)
    if (
        source is None
        or source.work_item_id != request.work_item_id
        or source.sequence >= request.sequence
        or source.status != "completed"
        or source.terminal_cause != "completed"
        or source.terminal_at is None
    ):
        raise ConflictAuthorityMismatch
    item = await session.get(WorkItem, request.work_item_id)
    lineage = await session.scalar(
        select(ThreadPublicationLineage)
        .where(ThreadPublicationLineage.id == row.lineage_id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    if await session.scalar(
        select(Publication.id)
        .where(
            Publication.execution_request_id == request.id,
            Publication.lineage_id == row.lineage_id,
            Publication.publication_mode == "base_conflict_merge",
            Publication.expected_prior_head == row.head_sha,
            Publication.base_sha == row.base_sha,
            Publication.status.in_(("succeeded", "failed", "denied", "expired")),
        )
        .limit(1)
    ):
        raise ConflictPublicationSettled
    if (
        item is None
        or item.cancelled_at is not None
        or lineage is None
        or lineage.status != "open"
        or item.publication_lineage_id != lineage.id
        or item.agent_id != lineage.agent_id
        or item.conversation_id != lineage.conversation_id
        or item.repository != lineage.repository
        or item.repository_path.casefold() != lineage.repo_full_name.casefold()
        or item.code_host_installation_id != lineage.code_host_installation_id
        or (lineage.code_host_kind, lineage.code_host_host, lineage.repository_project_id)
        != (row.code_host_kind, row.code_host_host, row.repository_project_id)
        or not lineage.code_host_pr_id
        or lineage.code_host_pr_id != row.code_host_pr_id
        or lineage.pr_number is None
        or lineage.pr_number != row.pr_number
        or lineage.head_sha != row.head_sha
        or not lineage.base_ref
        or lineage.base_ref != row.base_ref
        or item.base_branch != lineage.base_ref
        or item.base_requested_branch not in (None, lineage.base_ref)
        or item.readmit_base_branch not in (None, lineage.base_ref)
        or row.head_sha == row.base_sha
        or not _SHA.fullmatch(row.head_sha)
        or not _SHA.fullmatch(row.base_sha)
    ):
        raise ConflictAuthorityMismatch
    owner = OwnerTuple.of(request)
    if (
        owner != OwnerTuple.of(item)
        or owner.tenant_id != row.tenant_id
        or owner.model_auth_mode != row.model_auth_mode
        or owner.owner_principal_id != row.owner_principal_id
        or owner.enrollment_generation != row.enrollment_generation
    ):
        raise ConflictAuthorityMismatch
    authority = MaintenanceAuthority(
        repair_request_id=request.id,
        reason="base_conflict",
        head_sha=row.head_sha,
        base_ref=row.base_ref,
        base_sha=row.base_sha,
    )
    instructions = maintenance_instructions(authority, objective=request.objective)
    if instructions is None or instructions.publication_mode != "base_conflict_merge":
        raise ConflictAuthorityMismatch
    return ConflictPublicationAuthority(
        lineage.id, authority.head_sha, authority.base_sha, instructions.publication_mode
    )


async def conflict_snapshot_authority(
    session: AsyncSession,
    request: ExecutionRequest | None,
    lineage: ThreadPublicationLineage | None,
    snapshot: PublicationCreate,
) -> ConflictPublicationAuthority | None:
    if request is None:
        claimed = snapshot.work_item_request_id
        if claimed is not None and await is_conflict_successor(session, claimed):
            raise ConflictAuthorityMismatch
        return None
    conflict = await conflict_publication_authority(session, request)
    if conflict is not None and (
        snapshot.work_item_request_id != request.id
        or request.status != "running"
        or snapshot.work_item_runtime_epoch != request.runtime_epoch
        or lineage is None
        or lineage.id != conflict.lineage_id
        or snapshot.base_sha != conflict.base_sha
        or not snapshot.changed_paths
    ):
        raise ConflictAuthorityMismatch
    return conflict
