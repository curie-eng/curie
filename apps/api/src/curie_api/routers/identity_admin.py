"""Bounded tenant identity inspection, and operator code host links, for platform operators.

The writes live under ``/identity``, so the identity writer fence
(`identity_fence.protects`) serializes them with every other identity mutation
and with readers holding `identity_fence.lock_identity_reader`.
"""

import uuid
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from .. import identity_audit
from ..auth import require_platform_key
from ..deps import SessionDep
from ..identity.service import find_or_create_namespace
from ..models import IdentityLink, IdentityNamespace, Principal
from ..schemas.identity import CodeHostLinkIn, CodeHostLinkOut, IdentityLinkRevokedOut

GITHUB_HOST = "github_host"
_AUDIT_ACTOR = "platform"

router = APIRouter(
    prefix="/identity/admin",
    tags=["identity-admin"],
    dependencies=[Depends(require_platform_key)],
)


@router.get("/{tenant_id}/{resource}")
async def identity_view(
    tenant_id: uuid.UUID,
    resource: Literal["people", "links", "namespaces"],
    session: SessionDep,
    limit: int = Query(default=100, ge=1, le=200),
    after: uuid.UUID | None = None,
) -> dict[str, Any]:
    model: Any = {"people": Principal, "links": IdentityLink, "namespaces": IdentityNamespace}[
        resource
    ]
    statement = select(model).where(model.tenant_id == tenant_id).order_by(model.id).limit(limit)
    if after is not None:
        statement = statement.where(model.id > after)
    rows = list(await session.scalars(statement))
    fields = {
        "people": (
            "id",
            "tenant_id",
            "idp_issuer",
            "idp_subject",
            "type",
            "status",
            "authorization_version",
        ),
        "links": (
            "id",
            "tenant_id",
            "identity_namespace_id",
            "provider_native_id",
            "principal_id",
            "verification_source",
            "revoked_at",
        ),
        "namespaces": ("id", "tenant_id", "provider", "authority", "kind", "key", "status"),
    }[resource]

    def serialize(row: Any) -> dict[str, Any]:
        return {
            name: str(value) if isinstance(value, uuid.UUID) else value
            for name in fields
            for value in (getattr(row, name),)
        }

    return {
        "items": [serialize(row) for row in rows],
        "next_cursor": str(rows[-1].id) if len(rows) == limit else None,
    }


async def _active_principal(
    session: SessionDep, tenant_id: uuid.UUID, principal_id: uuid.UUID
) -> Principal | None:
    principal = await session.scalar(
        select(Principal).where(Principal.tenant_id == tenant_id, Principal.id == principal_id)
    )
    return principal if principal is not None and principal.status == "active" else None


@router.post(
    "/{tenant_id}/code-host-links",
    status_code=status.HTTP_201_CREATED,
    response_model=CodeHostLinkOut,
)
async def create_code_host_link(
    tenant_id: uuid.UUID, data: CodeHostLinkIn, session: SessionDep
) -> CodeHostLinkOut:
    """Map a principal to one GitHub account by its numeric id (#5414, ADR 0198).

    The link is ``admin_mapped`` in the host's ``github_host`` namespace. A
    person may hold several accounts; one account belongs to one active link.
    """

    host = data.host
    principal = await _active_principal(session, tenant_id, data.principal_id)
    creator = await session.scalar(
        select(Principal.id).where(
            Principal.tenant_id == tenant_id, Principal.id == data.created_by_principal_id
        )
    )
    if principal is None or creator is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, {"code": "principal_not_found"})
    namespace = await find_or_create_namespace(
        session,
        tenant_id=tenant_id,
        provider=data.code_host_kind,
        authority=host,
        kind=GITHUB_HOST,
        key=host,
    )
    existing = await session.scalar(
        select(IdentityLink.id).where(
            IdentityLink.identity_namespace_id == namespace.id,
            IdentityLink.provider_native_id == data.account_id,
            IdentityLink.revoked_at.is_(None),
        )
    )
    if existing is not None:
        await session.rollback()
        raise HTTPException(status.HTTP_409_CONFLICT, {"code": "link_exists"})
    link = IdentityLink(
        tenant_id=tenant_id,
        identity_namespace_id=namespace.id,
        provider_native_id=data.account_id,
        principal_id=principal.id,
        created_by_principal_id=data.created_by_principal_id,
        verification_source="admin_mapped",
    )
    session.add(link)
    try:
        await session.flush()
    except IntegrityError:
        await session.rollback()
        raise HTTPException(status.HTTP_409_CONFLICT, {"code": "link_exists"}) from None
    identity_audit.record(
        session,
        tenant_id=tenant_id,
        event_type="identity.code_host_link_created",
        actor=_AUDIT_ACTOR,
        principal_id=principal.id,
        details={"link_id": str(link.id), "namespace_id": str(namespace.id)},
    )
    response = CodeHostLinkOut(link_id=link.id, identity_namespace_id=namespace.id)
    await session.commit()
    return response


@router.post("/{tenant_id}/links/{link_id}/revoke", response_model=IdentityLinkRevokedOut)
async def revoke_link(
    tenant_id: uuid.UUID, link_id: uuid.UUID, session: SessionDep
) -> IdentityLinkRevokedOut:
    """Revoke one link of any provider. Revocation is set once and kept."""

    link = await session.scalar(
        select(IdentityLink)
        .where(IdentityLink.tenant_id == tenant_id, IdentityLink.id == link_id)
        .with_for_update()
    )
    if link is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, {"code": "link_not_found"})
    if link.revoked_at is None:
        link.revoked_at = await session.scalar(select(func.now()))
        identity_audit.record(
            session,
            tenant_id=tenant_id,
            event_type="identity.link_revoked",
            actor=_AUDIT_ACTOR,
            principal_id=link.principal_id,
            details={"link_id": str(link.id)},
        )
    revoked_at = link.revoked_at
    assert revoked_at is not None
    await session.commit()
    return IdentityLinkRevokedOut(link_id=link.id, revoked_at=revoked_at)
