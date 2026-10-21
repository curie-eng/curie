"""Bounded, read-only tenant identity inspection for platform operators."""

import uuid
from typing import Any, Literal

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select

from ..auth import require_platform_key
from ..deps import SessionDep
from ..models import IdentityLink, IdentityNamespace, Principal

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
