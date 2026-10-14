"""Team directory sync status for operators (#4584, ADR 0220 section 6).

The platform key only, via ``require_platform_key``, like the other identity
administration routes. Read only: the response says which snapshot each team
holds and how the last sync attempt went, never who is in it.
"""

import uuid

from fastapi import APIRouter, Depends

from .. import teams
from ..auth import require_platform_key
from ..deps import SessionDep
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
