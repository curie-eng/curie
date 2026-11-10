"""Append identity evidence inside the caller's transaction (ADR 0220)."""
import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from .models import IdentityAuditEvent


def record(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    event_type: str,
    actor: str,
    principal_id: uuid.UUID | None = None,
    team_id: uuid.UUID | None = None,
    details: dict[str, Any] | None = None,
) -> IdentityAuditEvent:
    """Do not commit or store unbounded provider payloads/credentials as evidence."""
    row = IdentityAuditEvent(
        tenant_id=tenant_id,
        event_type=event_type,
        actor=actor,
        principal_id=principal_id,
        team_id=team_id,
        details=details or {},
    )
    session.add(row)
    return row
