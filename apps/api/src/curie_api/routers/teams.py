"""Team directory sync status for operators (#4584, ADR 0220 section 6).

The platform key only, via ``require_platform_key``, like the other identity
administration routes. Read only: the response says which snapshot each team
holds and how the last sync attempt went, never who is in it.
"""

import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, Response
from sqlalchemy import func, select

from .. import teams
from ..auth import require_platform_key
from ..deps import SessionDep
from ..models import IdentityAuditEvent, Team
from ..schemas.teams import TeamSyncOut

router = APIRouter(
    prefix="/teams",
    tags=["teams"],
    dependencies=[Depends(require_platform_key)],
)


@router.get("", response_model=list[TeamSyncOut])
async def list_team_sync_status(
    session: SessionDep, tenant_id: uuid.UUID | None = None
) -> list[TeamSyncOut]:
    return await teams.list_team_sync(session, tenant_id=tenant_id)


@router.get("/metrics", response_class=Response)
async def directory_metrics(session: SessionDep, tenant_id: uuid.UUID | None = None) -> Response:
    """Export current age and durable failure counts; never expire membership."""
    at = datetime.now(UTC)
    query = select(Team).where(Team.source == "idp_group").order_by(Team.id)
    if tenant_id is not None:
        query = query.where(Team.tenant_id == tenant_id)
    failures = select(IdentityAuditEvent.team_id, func.count()).where(
        IdentityAuditEvent.event_type == "identity.directory_sync_failed"
    )
    if tenant_id is not None:
        failures = failures.where(IdentityAuditEvent.tenant_id == tenant_id)
    counts = {
        team_id: count
        for team_id, count in (
            await session.execute(failures.group_by(IdentityAuditEvent.team_id))
        ).all()
    }
    lines = [
        "# HELP curie_directory_sync_age_seconds Seconds since last complete snapshot.",
        "# TYPE curie_directory_sync_age_seconds gauge",
        "# HELP curie_directory_sync_failed_attempts_total Durable recorded failed sync attempts.",
        "# TYPE curie_directory_sync_failed_attempts_total counter",
    ]
    for team in await session.scalars(query):
        label = f'{{team_id="{team.id}"}}'
        if team.last_synced_at is not None:
            age = max(0.0, (at - team.last_synced_at).total_seconds())
            lines.append(f"curie_directory_sync_age_seconds{label} {age}")
        lines.append(f"curie_directory_sync_failed_attempts_total{label} {counts.get(team.id, 0)}")
    return Response(
        "\n".join(lines) + "\n",
        media_type="text/plain; version=0.0.4",
        headers={"Cache-Control": "no-store"},
    )
