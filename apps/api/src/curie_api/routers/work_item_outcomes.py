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

from curie_api.crud import agents as crud_agents
from curie_api.schemas.workitems import WorkItemOutcomeList, WorkItemOutcomeOut, WorkItemUsageOut

from .. import factory_usage, workitem_outcomes
from ..auth import require_api_key
from ..config import get_settings
from ..deps import SessionDep
from ..forges.hosts import code_host_for

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
    if agent_id is not None and await crud_agents.get_agent(session, agent_id) is None:
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
        view.ci = await workitem_outcomes.observe_ci(
            code_host_for(settings, client), settings, lineage, item
        )
    return view


@router.get("/{work_item_id}/usage", response_model=WorkItemUsageOut)
async def get_work_item_usage(
    work_item_id: uuid.UUID,
    session: SessionDep,
    response: Response,
    agent_id: uuid.UUID | None = None,
) -> WorkItemUsageOut:
    """Token usage and estimated cost over every round of the work item (#3223)."""

    response.headers["Cache-Control"] = "no-store"
    loaded = await workitem_outcomes.load_outcome(
        session, work_item_id, agent_id=agent_id, settings=get_settings()
    )
    if loaded is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, _NOT_FOUND)
    _view, _item, lineage = loaded
    usage = await factory_usage.work_item_usage(session, work_item_id)
    if lineage is not None:
        usage.pr_number = lineage.pr_number
        usage.pr_url = lineage.pr_url
    return usage
