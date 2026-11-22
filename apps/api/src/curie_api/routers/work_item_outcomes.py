"""Operator surface for factory work item outcomes (#2577).

Administrative authentication (``require_api_key``). The reads take state and
cause from ``workitem_outcomes.derive_outcome``; this router only scopes and
serializes. A missing work item and one scoped to another agent return the
same 404 body, so the route is not an existence oracle across agents.

The one write is ``POST /work-items/{id}/retry`` (#5431), which reruns the
automatic successor admission for a failed WorkItem through
``workitems.operator_retry``. Every answer it gives, refusals, validation
errors and authentication failures included, is ``Cache-Control: no-store``.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Coroutine
from typing import Annotated, Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from fastapi.exception_handlers import (
    http_exception_handler,
    request_validation_exception_handler,
)
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute

from curie_api.crud import agents as crud_agents
from curie_api.schemas.workitems import (
    WorkItemOutcomeList,
    WorkItemOutcomeOut,
    WorkItemRetryIn,
    WorkItemRetryOut,
    WorkItemUsageOut,
)

from .. import factory_ci, factory_usage, workitem_outcomes
from ..auth import AdministrativeActor, require_administrator, require_api_key
from ..code_host_trust import code_host_verify
from ..config import get_settings
from ..deps import SessionDep
from ..forges.errors import Unavailable
from ..forges.ports import CodeHost
from ..forges.registry import ForgeRegistry
from ..forges.trackers import code_host_for_repository, stored_repository
from ..workitems import operator_retry

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


class _NoStoreRoute(APIRoute):
    """Mark every answer of a route uncacheable, including the ones FastAPI
    renders itself: the 401 from authentication and the 422 from validation."""

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def no_store_handler(request: Request) -> Response:
            try:
                response = await handler(request)
            except RequestValidationError as exc:
                response = await request_validation_exception_handler(request, exc)
            except HTTPException as exc:
                response = await http_exception_handler(request, exc)
            response.headers["Cache-Control"] = "no-store"
            return response

        return no_store_handler


async def retry_work_item(
    work_item_id: uuid.UUID,
    body: WorkItemRetryIn,
    session: SessionDep,
    actor: Annotated[AdministrativeActor, Depends(require_administrator)],
) -> WorkItemRetryOut:
    """Admit one fresh request for a failed WorkItem, as automatic recovery would (#5431).

    Reruns the automatic successor admission against the newest failed
    request, inside the same shared allowance. ``request_id`` makes the call
    idempotent; ``dry_run`` answers the exact plan and writes nothing. The
    actor is the authenticated credential, never the body. Refusals are 409
    with a named ``code``; a missing WorkItem is 404.
    """

    try:
        return await operator_retry.retry_work_item(
            session, work_item_id=work_item_id, body=body, actor=actor
        )
    except operator_retry.RetryRefused as refused:
        raise HTTPException(refused.status_code, refused.detail()) from None


router.add_api_route(
    "/{work_item_id}/retry",
    retry_work_item,
    methods=["POST"],
    response_model=WorkItemRetryOut,
    route_class_override=_NoStoreRoute,
)
