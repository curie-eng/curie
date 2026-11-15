"""Worker-only private publication candidate metadata operations (#5428)."""

from collections.abc import Awaitable

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from .. import publication_candidates as ledger
from ..auth import require_internal_worker_token
from ..config import get_settings
from ..deps import SessionDep
from ..schemas import publication_candidates as wire

router = APIRouter(
    prefix="/v1/internal/publication-candidates",
    tags=["internal-publication-candidates"],
    dependencies=[Depends(require_internal_worker_token)],
)


async def _transaction[Result: BaseModel](
    session: AsyncSession, operation: Awaitable[Result]
) -> Result:
    try:
        response = await operation
    except ledger.CandidateRefusal as refusal:
        # A recorded refusal keeps its outcome row; nothing else was written.
        if refusal.recorded:
            await session.commit()
        else:
            await session.rollback()
        raise HTTPException(refusal.status, {"code": refusal.code}) from None
    await session.commit()
    return response


@router.post("/context", response_model=wire.CandidateCaptureContext)
async def context(
    body: wire.CandidateCaptureAuthority, session: SessionDep
) -> wire.CandidateCaptureContext:
    return await _transaction(session, ledger.context(session, get_settings(), body))


@router.post("/outcome", response_model=wire.CandidateOutcomeRecorded)
async def outcome(
    body: wire.CandidateOutcome, session: SessionDep
) -> wire.CandidateOutcomeRecorded:
    return await _transaction(session, ledger.outcome(session, get_settings(), body))


@router.post("/reserve", response_model=wire.CandidateReservation)
async def reserve(body: wire.CandidateReserve, session: SessionDep) -> wire.CandidateReservation:
    return await _transaction(session, ledger.reserve(session, get_settings(), body))


@router.post("/complete", response_model=wire.CandidateCompletion)
async def complete(body: wire.CandidateComplete, session: SessionDep) -> wire.CandidateCompletion:
    return await _transaction(session, ledger.complete(session, body))


@router.post("/fail", response_model=wire.CandidateCaptureFailure)
async def fail(body: wire.CandidateFail, session: SessionDep) -> wire.CandidateCaptureFailure:
    return await _transaction(session, ledger.fail(session, body))


@router.post("/acquire-reader", response_model=wire.CandidateReader)
async def acquire_reader(
    body: wire.CandidateAcquireReader, session: SessionDep
) -> wire.CandidateReader:
    return await _transaction(session, ledger.acquire_reader(session, get_settings(), body))


@router.post("/release-reader", response_model=wire.CandidateAcknowledged)
async def release_reader(
    body: wire.CandidateReleaseReader, session: SessionDep
) -> wire.CandidateAcknowledged:
    return await _transaction(session, ledger.release_reader(session, body))


@router.post("/claim-collection", response_model=wire.CandidateCollectionClaims)
async def claim_collection(
    body: wire.CandidateClaimCollection, session: SessionDep
) -> wire.CandidateCollectionClaims:
    return await _transaction(session, ledger.claim_collection(session, body))


@router.post("/finalize-collection", response_model=wire.CandidateFinalized)
async def finalize_collection(
    body: wire.CandidateFinalizeCollection, session: SessionDep
) -> wire.CandidateFinalized:
    return await _transaction(session, ledger.finalize_collection(session, body))
