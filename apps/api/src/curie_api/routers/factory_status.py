"""Factory run progress ingress and the live status card (#3077).

``POST /v1/work-item-progress/{request_id}`` accepts exactly one credential: a
``work_item.progress`` sandbox token bound to that request id. The platform key
is refused here, so the route has one credential.

``GET /v1/factory/cards/{token}.svg`` carries no auth dependency: GitHub's image
proxy sends no credential, so the 64-hex card token is the capability. The
response is never cached, so the image stays live.
"""

from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from fastapi.responses import JSONResponse
from sqlalchemy import select

from .. import sandbox_token
from ..config import get_settings
from ..deps import SessionDep
from ..factory_card import CardInput, render_card
from ..factory_notices import cause_text
from ..factory_progress import (
    PROGRESS_SCOPE,
    ProgressReport,
    phase_view,
    record_report,
)
from ..models import (
    ExecutionRequest,
    ExecutionRequestPhaseReport,
    FactoryStatusComment,
    Publication,
    ThreadPublicationLineage,
    WorkItem,
)

router = APIRouter(tags=["factory-status"])

_CARD_TOKEN = re.compile(r"[0-9a-f]{64}")
_PUBLISHING_STATUSES = ("pending", "approved", "launching", "running")
_CARD_HEADERS = {
    "Cache-Control": "no-cache, max-age=0, must-revalidate",
    "X-Content-Type-Options": "nosniff",
    "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'",
    "Referrer-Policy": "no-referrer",
}
_STATUS_CODES = {
    "request_not_found": status.HTTP_404_NOT_FOUND,
    "no_active_request": status.HTTP_409_CONFLICT,
    "declaration_changed": status.HTTP_409_CONFLICT,
    "report_limit": status.HTTP_429_TOO_MANY_REQUESTS,
}


async def require_progress_token(
    request_id: uuid.UUID,
    x_api_key: Annotated[str | None, Header()] = None,
) -> None:
    """Accept only a progress token bound to the path's request id."""

    if not x_api_key or not sandbox_token.verify(
        x_api_key, get_settings().api_key, agent=str(request_id), scope=PROGRESS_SCOPE
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid progress token",
        )


@router.post(
    "/v1/work-item-progress/{request_id}",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_progress_token)],
)
async def report_work_item_progress(
    request_id: uuid.UUID, body: ProgressReport, session: SessionDep
) -> Any:
    result = await record_report(session, token_request_id=request_id, body=body)
    if result.outcome != "recorded":
        return JSONResponse(
            status_code=_STATUS_CODES[result.outcome], content={"code": result.outcome}
        )
    return {"recorded": True, "request_id": str(result.request_id)}


@router.get(
    "/v1/factory/cards/{token}.svg",
    response_class=Response,
    responses={200: {"content": {"image/svg+xml": {}}}},
)
async def factory_status_card(token: str, session: SessionDep) -> Response:
    if not _CARD_TOKEN.fullmatch(token):
        return Response(status_code=status.HTTP_404_NOT_FOUND)
    row = await session.scalar(
        select(FactoryStatusComment).where(FactoryStatusComment.card_token == token)
    )
    if row is None:
        return Response(status_code=status.HTTP_404_NOT_FOUND)
    request = await session.get(ExecutionRequest, row.execution_request_id)
    work_item = await session.get(WorkItem, row.work_item_id)
    if request is None or work_item is None:
        return Response(status_code=status.HTTP_404_NOT_FOUND)
    reports = list(
        await session.scalars(
            select(ExecutionRequestPhaseReport)
            .where(ExecutionRequestPhaseReport.execution_request_id == request.id)
            .order_by(ExecutionRequestPhaseReport.id)
        )
    )
    publishing = (
        await session.scalar(
            select(Publication.id)
            .where(
                Publication.execution_request_id == request.id,
                Publication.status.in_(_PUBLISHING_STATUSES),
            )
            .limit(1)
        )
    ) is not None
    revision_pr: int | None = None
    if request.sequence > 1 and work_item.publication_lineage_id is not None:
        revision_pr = await session.scalar(
            select(ThreadPublicationLineage.pr_number).where(
                ThreadPublicationLineage.id == work_item.publication_lineage_id
            )
        )
    terminal = request.terminal_cause if request.terminal_at is not None else None
    body = render_card(
        CardInput(
            repo=work_item.repo_full_name,
            issue_number=work_item.github_issue_number,
            title=row.subject_title,
            revision_pr=revision_pr,
            status=request.status,
            publishing=publishing,
            started_at=request.started_at,
            terminal_at=request.terminal_at,
            now=datetime.now(UTC),
            activity=row.activity,
            note=reports[-1].note if reports else None,
            phase_view=phase_view(
                row.declaration or {"phases": [], "loops": []},
                reports,
                request.status,
                request.terminal_cause,
            ),
            cause_text=(
                cause_text(terminal) if terminal and terminal != "completed" else None
            ),
        )
    )
    return Response(
        content=body,
        media_type="image/svg+xml; charset=utf-8",
        headers=_CARD_HEADERS,
    )
