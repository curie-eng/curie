"""Worker-only current-authority check for durable human GitHub feedback."""

import uuid

from aci_protocol import QueuedTurn
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from ..auth import require_internal_worker_token
from ..config import get_settings
from ..deps import SessionDep
from ..github_review_events import FeedbackIgnored, FeedbackUnavailable
from ..github_review_store import (
    record_feedback_refusal,
    reserve_queued_feedback,
    verify_queued_feedback,
)

router = APIRouter(
    prefix="/v1/internal/github/reviews",
    tags=["internal-github-reviews"],
    dependencies=[Depends(require_internal_worker_token)],
)


class ReviewVerificationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    turn: QueuedTurn
    deployment_id: uuid.UUID


class ReviewVerificationOut(BaseModel):
    head_sha: str
    agent_id: uuid.UUID
    sender: str
    receipt: str
    origin_key: str
    lineage_version: int
    reservation_id: uuid.UUID | None


class ReviewReservationRequest(ReviewVerificationRequest):
    expected_lineage_version: int = Field(ge=1, strict=True)
    expected_head_sha: str = Field(pattern=r"^[0-9a-f]{40}$")


class ReviewReservationOut(BaseModel):
    origin_key: str
    reservation_id: uuid.UUID


@router.post("/{event_id}/verify", response_model=ReviewVerificationOut)
async def verify_review(
    event_id: str,
    payload: ReviewVerificationRequest,
    request: Request,
    response: Response,
    session: SessionDep,
) -> ReviewVerificationOut:
    response.headers["Cache-Control"] = "no-store"
    if event_id != payload.turn.event_id:
        raise HTTPException(
            409, {"code": "feedback_turn_mismatch"}, headers={"Cache-Control": "no-store"}
        )
    try:
        result = await verify_queued_feedback(
            session,
            payload.turn,
            payload.deployment_id,
            settings=get_settings(),
            client=request.app.state.http_client,
        )
    except FeedbackUnavailable as exc:
        raise HTTPException(
            503, {"code": exc.code}, headers={"Cache-Control": "no-store"}
        ) from None
    except FeedbackIgnored as exc:
        await session.rollback()
        await record_feedback_refusal(session, payload.turn, exc.code)
        await session.commit()
        raise HTTPException(
            409, {"code": exc.code}, headers={"Cache-Control": "no-store"}
        ) from None
    return ReviewVerificationOut.model_validate(result)


@router.post("/{event_id}/reserve", response_model=ReviewReservationOut)
async def reserve_review(
    event_id: str,
    payload: ReviewReservationRequest,
    response: Response,
    session: SessionDep,
) -> ReviewReservationOut:
    response.headers["Cache-Control"] = "no-store"
    if event_id != payload.turn.event_id:
        raise HTTPException(409, {"code": "feedback_turn_mismatch"})
    try:
        reservation_id = await reserve_queued_feedback(
            session,
            payload.turn,
            payload.deployment_id,
            expected_lineage_version=payload.expected_lineage_version,
            expected_head_sha=payload.expected_head_sha,
            settings=get_settings(),
        )
        await session.commit()
    except FeedbackUnavailable as exc:
        raise HTTPException(
            503, {"code": exc.code}, headers={"Cache-Control": "no-store"}
        ) from None
    except FeedbackIgnored as exc:
        await session.rollback()
        await record_feedback_refusal(session, payload.turn, exc.code)
        await session.commit()
        raise HTTPException(
            409, {"code": exc.code}, headers={"Cache-Control": "no-store"}
        ) from None
    return ReviewReservationOut(origin_key=event_id, reservation_id=reservation_id)
