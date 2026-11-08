"""Authenticated administrator access to retained recovery artifacts."""

import uuid

from fastapi import APIRouter, Depends, HTTPException, Request
from starlette.responses import StreamingResponse

from .. import workspace_recovery as ledger
from .. import workspace_recovery_access as access
from ..auth import require_api_key
from ..config import get_settings
from ..deps import SessionDep
from ..schemas import workspace_recovery as wire

router = APIRouter(
    prefix="/work-items/{work_item_id}/recovery",
    tags=["workspace-recovery"],
    dependencies=[Depends(require_api_key)],
)


@router.get("", response_model=wire.RecoveryPublicList)
async def list_checkpoints(
    work_item_id: uuid.UUID,
    request: Request,
    session: SessionDep,
) -> wire.RecoveryPublicList:
    try:
        return await access.list_metadata(
            session,
            get_settings(),
            request.app.state.forges,
            request.app.state.http_client,
            work_item_id,
        )
    except ledger.RecoveryRefusal as refusal:
        await session.rollback()
        raise HTTPException(refusal.status, {"code": refusal.code}) from None


@router.get("/{checkpoint_id}", response_model=wire.RecoveryPublicCheckpoint)
async def checkpoint_status(
    work_item_id: uuid.UUID,
    checkpoint_id: uuid.UUID,
    request: Request,
    session: SessionDep,
) -> wire.RecoveryPublicCheckpoint:
    try:
        return await access.checkpoint_metadata(
            session,
            get_settings(),
            request.app.state.forges,
            request.app.state.http_client,
            work_item_id,
            checkpoint_id,
        )
    except ledger.RecoveryRefusal as refusal:
        await session.rollback()
        raise HTTPException(refusal.status, {"code": refusal.code}) from None


@router.get(
    "/{checkpoint_id}/objects/{kind}",
    response_class=StreamingResponse,
    responses={
        200: {
            "content": {
                "application/octet-stream": {
                    "schema": {"type": "string", "format": "binary"},
                }
            }
        }
    },
)
async def object_download(
    work_item_id: uuid.UUID,
    checkpoint_id: uuid.UUID,
    kind: access.ObjectKind,
    request: Request,
    session: SessionDep,
) -> access.VerifiedRecoveryResponse:
    try:
        return await access.download(
            session,
            get_settings(),
            request.app.state.forges,
            request.app.state.http_client,
            request.app.state.sessionmaker,
            work_item_id,
            checkpoint_id,
            kind,
        )
    except ledger.RecoveryRefusal as refusal:
        await session.rollback()
        raise HTTPException(refusal.status, {"code": refusal.code}) from None
