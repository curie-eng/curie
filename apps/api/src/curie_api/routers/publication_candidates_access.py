"""Authenticated operator metadata for private publication candidates (#5428).

Only ids, statuses, timestamps, counts, sizes and digests: never source,
titles, bodies, paths, object keys, URLs, credentials or error text.
"""

import uuid

from fastapi import APIRouter, Depends, HTTPException

from .. import publication_candidates as ledger
from ..auth import require_api_key
from ..deps import SessionDep
from ..schemas import publication_candidates as wire

router = APIRouter(
    prefix="/work-items/{work_item_id}/publication-candidates",
    tags=["publication-candidates"],
    dependencies=[Depends(require_api_key)],
)


@router.get("", response_model=wire.CandidatePublicList)
async def list_candidates(work_item_id: uuid.UUID, session: SessionDep) -> wire.CandidatePublicList:
    try:
        result = await ledger.list_metadata(session, work_item_id)
    except ledger.CandidateRefusal as refusal:
        await session.rollback()
        raise HTTPException(refusal.status, {"code": refusal.code}) from None
    await session.commit()
    return result
