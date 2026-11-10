"""Read a tenant's teams with their directory sync state (#4584, ADR 0220).

Read only. Applying snapshots and recording attempts belong to the directory
adapter path that follows this change.
"""

import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Team
from .schemas.teams import TeamSyncOut


async def list_team_sync(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID | None = None,
    now: datetime | None = None,
) -> list[TeamSyncOut]:
    query = select(Team).order_by(Team.name, Team.id)
    if tenant_id is not None:
        query = query.where(Team.tenant_id == tenant_id)
    at = now or datetime.now(UTC)
    rows = (await session.scalars(query)).all()
    return [
        TeamSyncOut(
            id=team.id,
            tenant_id=team.tenant_id,
            source=team.source,  # type: ignore[arg-type]
            external_id=team.external_id,
            name=team.name,
            managed_by=team.managed_by,
            epoch=team.epoch,
            member_count=team.member_count,
            digest=team.digest,
            last_synced_at=team.last_synced_at,
            last_attempt_at=team.last_attempt_at,
            last_error=team.last_error,
            synced=team.epoch > 0,
            sync_age_seconds=(
                max(0.0, (at - team.last_synced_at).total_seconds())
                if team.last_synced_at is not None
                else None
            ),
        )
        for team in rows
    ]
