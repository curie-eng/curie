"""Operator read surface for factory work item outcomes (#2577).

Read-only, platform key only. State and cause come from
``workitem_outcomes.derive_outcome``; this router only scopes and serializes.
A missing work item and one scoped to another agent return the same 404 body,
so the route is not an existence oracle across agents.
"""

from __future__ import annotations

import uuid

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Response, status

from .. import crud, workitem_outcomes
from ..auth import require_api_key
from ..config import get_settings
from ..deps import SessionDep
from ..schemas import WorkItemOutcomeList, WorkItemOutcomeOut

router = APIRouter(
    prefix="/work-items",
    tags=["work-items"],
    dependencies=[Depends(require_api_key)],
)

_NOT_FOUND = {"code": "not_found"}


@router.get("", response_model=WorkItemOutcomeList)
async def list_work_items(
    session: SessionDep,
    response: Response,
    agent_id: uuid.UUID | None = None,
    limit: int = Query(50, ge=1, le=200),
) -> WorkItemOutcomeList:
    response.headers["Cache-Control"] = "no-store"
    if agent_id is not None and await crud.get_agent(session, agent_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, _NOT_FOUND)
    items, truncated = await workitem_outcomes.load_outcomes(
        session, agent_id=agent_id, limit=limit, settings=get_settings()
    )
    return WorkItemOutcomeList(items=items, limit=limit, truncated=truncated)


@router.get("/{work_item_id}", response_model=WorkItemOutcomeOut)
async def get_work_item(
    work_item_id: uuid.UUID,
    session: SessionDep,
    response: Response,
    agent_id: uuid.UUID | None = None,
) -> WorkItemOutcomeOut:
    response.headers["Cache-Control"] = "no-store"
    settings = get_settings()
    loaded = await workitem_outcomes.load_outcome(
        session, work_item_id, agent_id=agent_id, settings=settings
    )
    if loaded is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, _NOT_FOUND)
    view, item, lineage = loaded
    async with httpx.AsyncClient() as client:
        view.ci = await workitem_outcomes.observe_ci(lineage, item, settings, client)
    return view
