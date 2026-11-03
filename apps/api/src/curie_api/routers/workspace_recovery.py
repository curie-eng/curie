"""Worker-only private recovery metadata operations."""

from collections.abc import Awaitable

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from .. import workspace_recovery as ledger
from ..auth import require_internal_worker_token
from ..config import get_settings
from ..deps import SessionDep
from ..schemas import workspace_recovery as wire

router = APIRouter(
    prefix="/v1/internal/workspace-recovery",
    tags=["internal-workspace-recovery"],
    dependencies=[Depends(require_internal_worker_token)],
)


async def _transaction[Result: BaseModel](
    session: AsyncSession, operation: Awaitable[Result]
) -> Result:
    try:
        response = await operation
    except ledger.RecoveryRefusal as refusal:
        if refusal.recorded:
            await session.commit()
        else:
            await session.rollback()
        raise HTTPException(refusal.status, {"code": refusal.code}) from None
    await session.commit()
    return response


@router.post("/context", response_model=wire.RecoveryCaptureContext)
async def context(
    body: wire.RecoveryCaptureAuthority, session: SessionDep
) -> wire.RecoveryCaptureContext:
    return await _transaction(session, ledger.context(session, get_settings(), body))


@router.post("/outcome", response_model=wire.RecoveryOutcomeRecorded)
async def outcome(body: wire.RecoveryOutcome, session: SessionDep) -> wire.RecoveryOutcomeRecorded:
    return await _transaction(session, ledger.outcome(session, get_settings(), body))


@router.post("/reserve", response_model=wire.RecoveryReservation)
async def reserve(body: wire.RecoveryReserve, session: SessionDep) -> wire.RecoveryReservation:
    return await _transaction(session, ledger.reserve(session, get_settings(), body))


@router.post("/complete", response_model=wire.RecoveryCompletion)
async def complete(body: wire.RecoveryComplete, session: SessionDep) -> wire.RecoveryCompletion:
    return await _transaction(session, ledger.complete(session, body))


@router.post("/fail", response_model=wire.RecoveryCaptureFailure)
async def fail(body: wire.RecoveryFail, session: SessionDep) -> wire.RecoveryCaptureFailure:
    return await _transaction(session, ledger.fail(session, body))


@router.post("/acquire-reader", response_model=wire.RecoveryReader)
async def acquire_reader(
    body: wire.RecoveryAcquireReader, session: SessionDep
) -> wire.RecoveryReader:
    return await _transaction(session, ledger.acquire_reader(session, get_settings(), body))


@router.post("/release-reader", response_model=wire.RecoveryAcknowledged)
async def release_reader(
    body: wire.RecoveryReleaseReader, session: SessionDep
) -> wire.RecoveryAcknowledged:
    return await _transaction(session, ledger.release_reader(session, body))


@router.post("/claim-collection", response_model=wire.RecoveryCollectionClaims)
async def claim_collection(
    body: wire.RecoveryClaimCollection, session: SessionDep
) -> wire.RecoveryCollectionClaims:
    return await _transaction(session, ledger.claim_collection(session, body))


@router.post("/finalize-collection", response_model=wire.RecoveryFinalized)
async def finalize_collection(
    body: wire.RecoveryFinalizeCollection, session: SessionDep
) -> wire.RecoveryFinalized:
    return await _transaction(session, ledger.finalize_collection(session, body))
