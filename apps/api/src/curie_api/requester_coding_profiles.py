"""Transactional owner profile storage, independent of enrollment lifetime."""

from __future__ import annotations

from uuid import UUID

from curie_internal.requester_coding_instructions import (
    MAX_REVISION,
    lock_principal,
    normalize_text,
    text_sha256,
    validate_expected_revision,
)
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from .models import RequesterCodingPolicy, RequesterCodingProfile, Tenant
from .schemas.coding_instructions import CodingInstructionsOut


class ProfileRefusal(ValueError):
    def __init__(self, code: str, *, current_revision: int | None = None) -> None:
        self.code = code
        self.current_revision = current_revision
        super().__init__(code)


async def tenant_enabled(session: AsyncSession, tenant_id: UUID) -> bool:
    found = await session.scalar(
        select(RequesterCodingPolicy.enabled).where(RequesterCodingPolicy.tenant_id == tenant_id)
    )
    return True if found is None else found


async def read_profile(
    session: AsyncSession, *, tenant_id: UUID, principal_id: UUID
) -> CodingInstructionsOut:
    row = await session.scalar(
        select(RequesterCodingProfile)
        .where(
            RequesterCodingProfile.tenant_id == tenant_id,
            RequesterCodingProfile.principal_id == principal_id,
        )
        .execution_options(populate_existing=True)
    )
    enabled = await tenant_enabled(session, tenant_id)
    return CodingInstructionsOut(
        format_version=1,
        application="unavailable",
        revision=0 if row is None else row.revision,
        sha256=None if row is None else row.sha256,
        text=None if row is None else row.text,
        byte_length=0 if row is None or row.text is None else len(row.text.encode("utf-8")),
        updated_at=None if row is None else row.updated_at,
        tenant_enabled=enabled,
    )


async def write_profile(
    session: AsyncSession,
    *,
    tenant_id: UUID,
    principal_id: UUID,
    value: str | bytes,
    expected_revision: int,
) -> CodingInstructionsOut:
    """Mutate in the caller's transaction after locking the owner identity row."""

    validate_expected_revision(expected_revision)
    normalized = normalize_text(value)
    if not await lock_principal(
        session, tenant_id=tenant_id, principal_id=principal_id, writing=True
    ):
        raise ProfileRefusal("principal_not_found")
    current = await read_profile(session, tenant_id=tenant_id, principal_id=principal_id)
    if current.revision != expected_revision:
        raise ProfileRefusal("stale_revision", current_revision=current.revision)
    if current.revision == MAX_REVISION:
        raise ProfileRefusal("revision_exhausted")
    stored = normalized or None
    statement = insert(RequesterCodingProfile).values(
        tenant_id=tenant_id,
        principal_id=principal_id,
        revision=current.revision + 1,
        sha256=None if stored is None else text_sha256(stored),
        text=stored,
        format_version=1,
    )
    statement = statement.on_conflict_do_update(
        index_elements=[RequesterCodingProfile.tenant_id, RequesterCodingProfile.principal_id],
        set_={
            "revision": statement.excluded.revision,
            "sha256": statement.excluded.sha256,
            "text": statement.excluded.text,
            "format_version": 1,
            "updated_at": statement.excluded.updated_at,
        },
    )
    await session.execute(statement)
    return await read_profile(session, tenant_id=tenant_id, principal_id=principal_id)


async def require_tenant(session: AsyncSession, tenant_id: UUID) -> None:
    if await session.scalar(select(Tenant.id).where(Tenant.id == tenant_id)) is None:
        raise ProfileRefusal("tenant_not_found")


async def write_policy(session: AsyncSession, *, tenant_id: UUID, enabled: bool) -> None:
    await require_tenant(session, tenant_id)
    statement = insert(RequesterCodingPolicy).values(tenant_id=tenant_id, enabled=enabled)
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=[RequesterCodingPolicy.tenant_id], set_={"enabled": enabled}
        )
    )
