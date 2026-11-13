"""Request-scoped factory transcript ingress and operator read (#5608)."""

from __future__ import annotations

import uuid
from typing import Annotated, Any

from curie_internal import sandbox_token
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response, status
from sqlalchemy import select

from ..auth import require_api_key
from ..config import get_settings
from ..deps import SessionDep
from ..factory_transcripts import WRITE_SCOPE, TranscriptEventIn, page_events, record_event
from ..models import ExecutionRequest, WorkItem

router = APIRouter(tags=["factory-transcripts"])


async def require_transcript_writer(
    request_id: uuid.UUID,
    x_api_key: Annotated[str | None, Header()] = None,
) -> None:
    if not x_api_key or not sandbox_token.verify(
        x_api_key, get_settings().api_key, agent=str(request_id), scope=WRITE_SCOPE
    ):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing or invalid transcript token")


@router.post(
    "/v1/work-item-transcripts/{request_id}/events",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_transcript_writer)],
)
async def write_transcript_event(
    request_id: uuid.UUID, body: TranscriptEventIn, session: SessionDep
) -> dict[str, Any]:
    result = await record_event(
        session,
        request_id,
        body,
        retention_days=get_settings().factory_transcript_retention_days,
    )
    if result == "request_not_found":
        raise HTTPException(status.HTTP_404_NOT_FOUND, {"code": result})
    if result == "event_conflict":
        raise HTTPException(status.HTTP_409_CONFLICT, {"code": result})
    if result == "expired":
        raise HTTPException(status.HTTP_410_GONE, {"code": result})
    if result == "too_large":
        raise HTTPException(status.HTTP_413_CONTENT_TOO_LARGE, {"code": result})
    return {"recorded": result == "recorded", "duplicate": result == "duplicate"}


@router.get(
    "/work-items/runs/{request_id}/transcript", dependencies=[Depends(require_api_key)]
)
async def read_transcript(
    request_id: uuid.UUID,
    session: SessionDep,
    response: Response,
    agent_id: uuid.UUID | None = None,
    after: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
) -> dict[str, Any]:
    response.headers["Cache-Control"] = "no-store"
    request = await session.scalar(
        select(ExecutionRequest)
        .join(WorkItem, WorkItem.id == ExecutionRequest.work_item_id)
        .where(
            ExecutionRequest.id == request_id,
            *(() if agent_id is None else (WorkItem.agent_id == agent_id,)),
        )
    )
    if request is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            {"code": "not_found"},
            headers={"Cache-Control": "no-store"},
        )
    return await page_events(session, request, after=after, limit=limit)
