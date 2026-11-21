"""Operator read surface for factory work item outcomes (#2577).

Read-only, platform key only. State and cause come from
``workitem_outcomes.derive_outcome``; this router only scopes and serializes.
A missing work item and one scoped to another agent return the same 404 body,
so the route is not an existence oracle across agents.
"""

from __future__ import annotations

import uuid

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from sqlalchemy import select

from curie_api.crud import agents as crud_agents
from curie_api.schemas.workitems import (
    FactoryRunReportOut,
    WorkItemOutcomeList,
    WorkItemOutcomeOut,
    WorkItemUsageOut,
)

from .. import factory_ci, factory_run_reports, factory_usage, workitem_outcomes
from ..auth import require_api_key
from ..code_host_trust import code_host_verify
from ..config import get_settings
from ..deps import LangfuseDep, SessionDep
from ..forges.errors import Unavailable
from ..forges.ports import CodeHost
from ..forges.registry import ForgeRegistry
from ..forges.trackers import code_host_for_repository, stored_repository
from ..models import ExecutionRequest, WorkItem

router = APIRouter(
    prefix="/work-items",
    tags=["work-items"],
    dependencies=[Depends(require_api_key)],
)


def _not_found() -> HTTPException:
    """A miss is as uncacheable as a hit, and identical for an unknown id and
    another agent's WorkItem (#5376)."""

    return HTTPException(
        status.HTTP_404_NOT_FOUND, {"code": "not_found"}, headers={"Cache-Control": "no-store"}
    )


@router.get("", response_model=WorkItemOutcomeList)
async def list_work_items(
    session: SessionDep,
    response: Response,
    agent_id: uuid.UUID | None = None,
    limit: int = Query(50, ge=1, le=200),
) -> WorkItemOutcomeList:
    response.headers["Cache-Control"] = "no-store"
    if agent_id is not None and await crud_agents.get_agent(session, agent_id) is None:
        raise _not_found()
    items, truncated = await workitem_outcomes.load_outcomes(
        session, agent_id=agent_id, limit=limit, settings=get_settings()
    )
    return WorkItemOutcomeList(items=items, limit=limit, truncated=truncated)


@router.get("/{work_item_id}", response_model=WorkItemOutcomeOut)
async def get_work_item(
    work_item_id: uuid.UUID,
    request: Request,
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
        raise _not_found()
    view, item, lineage = loaded
    registry: ForgeRegistry = request.app.state.forges
    # The code host entry the lineage (or the WorkItem alone) recorded, never
    # the install's GitHub accessor (#4358 plan D14).
    repository = stored_repository(
        settings,
        kind=(lineage.code_host_kind if lineage is not None else None) or item.code_host_kind,
        host=(lineage.code_host_host if lineage is not None else None) or item.code_host_host,
        path=(lineage.repo_full_name if lineage is not None else None) or item.repository_path,
        project_id=(lineage.repository_project_id if lineage is not None else None)
        or item.repository_project_id,
    )
    async with httpx.AsyncClient(verify=code_host_verify(settings)) as client:
        code_host: CodeHost | None
        try:
            code_host = code_host_for_repository(
                registry,
                client,
                repository,
                installations=factory_ci.stored_installation(lineage, item),
            )
        except Unavailable:
            code_host = None
        view.ci = await workitem_outcomes.observe_ci(
            code_host,
            settings,
            lineage,
            item,
            repository=repository,
        )
    return view


@router.get("/{work_item_id}/runs/{request_id}/report", response_model=FactoryRunReportOut)
async def get_run_report(
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    session: SessionDep,
    response: Response,
    langfuse: LangfuseDep,
    agent_id: uuid.UUID | None = None,
) -> FactoryRunReportOut:
    """Read one terminal execution's retained diagnostic record (#5601)."""

    response.headers["Cache-Control"] = "no-store"
    owned = (
        await session.execute(
            select(ExecutionRequest, WorkItem)
            .join(WorkItem, WorkItem.id == ExecutionRequest.work_item_id)
            .where(
                ExecutionRequest.id == request_id,
                ExecutionRequest.work_item_id == work_item_id,
                *(() if agent_id is None else (WorkItem.agent_id == agent_id,)),
            )
        )
    ).one_or_none()
    if owned is None:
        raise _not_found()
    report = await factory_run_reports.get_report(session, work_item_id, request_id)
    if report is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            {"code": "report_unavailable", "reason": "not_captured_or_expired"},
            headers={"Cache-Control": "no-store"},
        )
    request_row, item = owned
    report = dict(report)
    report["sources"] = dict(report["sources"])
    if request_row.started_at is not None and request_row.terminal_at is not None:
        session_id = f"agent-{item.agent_id}-thread-{item.conversation_id}"
        try:
            trace_ids, trace_status = await langfuse.list_factory_traces(
                session_id, request_row.created_at, request_row.terminal_at
            )
        except (httpx.HTTPError, ValueError):
            trace_ids, trace_status = [], "unavailable"
        report["trace_ids"] = trace_ids
        report["sources"]["traces"] = trace_status
    return FactoryRunReportOut.model_validate(report)


@router.get("/runs/{request_id}/report", response_model=FactoryRunReportOut)
async def get_run_report_by_id(
    request_id: uuid.UUID,
    session: SessionDep,
    response: Response,
    langfuse: LangfuseDep,
    agent_id: uuid.UUID | None = None,
) -> FactoryRunReportOut:
    """Inspect a run directly when its request ID is all the operator has."""

    work_item_id = await session.scalar(
        select(ExecutionRequest.work_item_id)
        .join(WorkItem, WorkItem.id == ExecutionRequest.work_item_id)
        .where(
            ExecutionRequest.id == request_id,
            *(() if agent_id is None else (WorkItem.agent_id == agent_id,)),
        )
    )
    if work_item_id is None:
        raise _not_found()
    return await get_run_report(work_item_id, request_id, session, response, langfuse, agent_id)


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
        raise _not_found()
    _view, _item, lineage = loaded
    usage = await factory_usage.work_item_usage(session, work_item_id)
    if lineage is not None:
        usage.pr_number = lineage.pr_number
        usage.pr_url = lineage.pr_url
    return usage
