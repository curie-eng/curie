"""Internal worker-token WorkItem admission and dispatch routes."""

from __future__ import annotations

import uuid
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from .. import workitem_dispatch
from ..auth import require_internal_worker_token
from ..config import get_settings
from ..deps import SessionDep
from ..workitem_dispatch import DispatchConflict
from ..workitems import WorkItemConflict, WorkItemOutcome

router = APIRouter(
    prefix="/v1/internal/work-items",
    tags=["internal-work-items"],
    dependencies=[Depends(require_internal_worker_token)],
)


class AdmissionFacts(BaseModel):
    agent_id: uuid.UUID
    kind: str = Field(min_length=1)
    address: str = Field(min_length=1)
    reply_conversation_id: str = Field(min_length=1)
    repo_full_name: str = Field(min_length=1)
    github_repository_id: int = Field(gt=0)
    github_issue_number: int = Field(gt=0)
    github_installation_id: int = Field(gt=0)
    objective: str = Field(min_length=1, max_length=65536)
    requester: str = Field(min_length=1)
    request_id: uuid.UUID


class CancelBody(BaseModel):
    expected_version: int = Field(ge=1)


class AcquireBody(BaseModel):
    owner: str = Field(min_length=1)
    generation: int = Field(ge=1)


class DeferBody(BaseModel):
    owner: str = Field(min_length=1)
    generation: int = Field(ge=1)
    reason: str = Field(min_length=1)
    capacity: bool


class StartBody(BaseModel):
    owner: str = Field(min_length=1)
    generation: int = Field(ge=1)
    claim_name: str = Field(min_length=1)
    sandbox_name: str = Field(min_length=1)


class HeartbeatBody(BaseModel):
    runtime_epoch: int = Field(ge=1)


class OwnerLostBody(BaseModel):
    owner: str = Field(min_length=1)
    runtime_epoch: int = Field(ge=1)


class FinishBody(BaseModel):
    runtime_epoch: int = Field(ge=1)
    outcome: Literal["completed", "failed"]
    cause: str = Field(min_length=1)
    # The provider's own failure message (#3073). The API redacts and clips it.
    detail: str | None = Field(default=None, max_length=4000)


class TerminationClaimBody(BaseModel):
    owner: str = Field(min_length=1)


class TerminationBody(BaseModel):
    runtime_epoch: int = Field(ge=1)
    observation: str = Field(min_length=1)


def _raise_conflict(result: object) -> None:
    code = getattr(result, "code", None)
    if isinstance(code, str):
        raise HTTPException(status.HTTP_409_CONFLICT, {"code": code})


def _admission_body(result: WorkItemOutcome) -> dict[str, Any]:
    assert result.request is not None
    return {
        "work_item": workitem_dispatch.work_item_view(result.work_item),
        "request": workitem_dispatch.request_snapshot_view(result.request),
        "replayed": result.replayed,
    }


@router.post("/admissions")
async def admit_work_item(
    facts: AdmissionFacts, session: SessionDep
) -> dict[str, Any]:
    result = await workitem_dispatch.admit(session, facts)
    if isinstance(result, WorkItemOutcome):
        if result.request is None:
            raise HTTPException(status.HTTP_409_CONFLICT, {"code": "not_found"})
        return _admission_body(result)
    _raise_conflict(result)
    raise HTTPException(status.HTTP_409_CONFLICT, {"code": "not_found"})


@router.get("/running")
async def running_work_item_request(
    conversation_id: str, session: SessionDep
) -> dict[str, Any]:
    state, row = await workitem_dispatch.running_for_conversation(
        session, conversation_id
    )
    if state == "absent" or row is None or row.runtime_epoch is None:
        if state == "ended":
            raise HTTPException(status.HTTP_409_CONFLICT, {"code": "execution_ended"})
        raise HTTPException(status.HTTP_404_NOT_FOUND, {"code": "not_found"})
    if row.execution_deadline is None:
        raise HTTPException(status.HTTP_409_CONFLICT, {"code": "execution_ended"})
    return {
        "request_id": str(row.id),
        "runtime_epoch": row.runtime_epoch,
        "execution_deadline": row.execution_deadline.isoformat(),
        "status": row.status,
    }


@router.get("/runtime-owners")
async def list_work_item_runtime_owners(
    session: SessionDep, after: uuid.UUID | None = None
) -> dict[str, Any]:
    rows = await workitem_dispatch.list_runtime_owners(
        session, limit=get_settings().work_item_batch_limit, after=after
    )
    return {
        "requests": [
            {
                "request_id": str(row.request_id),
                "runtime_owner": row.runtime_owner,
                "runtime_epoch": row.runtime_epoch,
            }
            for row in rows
        ]
    }


@router.get("/requests/{request_id}")
async def get_work_item_request(
    request_id: uuid.UUID, session: SessionDep
) -> dict[str, Any]:
    row = await workitem_dispatch.get_request(session, request_id)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, {"code": "not_found"})
    return workitem_dispatch.request_view(row)


@router.post("/{work_item_id}/cancel")
async def cancel_work_item(
    work_item_id: uuid.UUID, body: CancelBody, session: SessionDep
) -> dict[str, Any]:
    result = await workitem_dispatch.cancel(
        session, work_item_id=work_item_id, expected_version=body.expected_version
    )
    if isinstance(result, WorkItemConflict):
        _raise_conflict(result)
    assert isinstance(result, WorkItemOutcome)
    return {
        "work_item": workitem_dispatch.work_item_view(result.work_item),
        "request": (
            workitem_dispatch.request_snapshot_view(result.request)
            if result.request is not None
            else None
        ),
    }


@router.post("/requests/{request_id}/acquire")
async def acquire_work_item_request(
    request_id: uuid.UUID, body: AcquireBody, session: SessionDep
) -> dict[str, Any]:
    result = await workitem_dispatch.acquire(
        session, request_id, owner=body.owner, generation=body.generation
    )
    if isinstance(result, DispatchConflict):
        _raise_conflict(result)
    assert not isinstance(result, DispatchConflict)
    return {
        "generation": result.generation,
        "work_item_id": result.work_item_id,
        "conversation_id": result.conversation_id,
        "wait_deadline": result.wait_deadline,
        "repo_full_name": result.repo_full_name,
    }


@router.post("/requests/{request_id}/defer")
async def defer_work_item_request(
    request_id: uuid.UUID, body: DeferBody, session: SessionDep
) -> dict[str, Any]:
    result = await workitem_dispatch.defer(
        session,
        request_id,
        owner=body.owner,
        generation=body.generation,
        reason=body.reason,
        capacity=body.capacity,
    )
    if isinstance(result, DispatchConflict):
        _raise_conflict(result)
    assert not isinstance(result, DispatchConflict)
    return {
        "dispatch_generation": result.dispatch_generation,
        "not_before": result.not_before,
    }


@router.post("/requests/{request_id}/start")
async def start_work_item_request(
    request_id: uuid.UUID, body: StartBody, session: SessionDep
) -> dict[str, Any]:
    result = await workitem_dispatch.start(
        session,
        request_id,
        owner=body.owner,
        generation=body.generation,
        claim_name=body.claim_name,
        sandbox_name=body.sandbox_name,
    )
    if isinstance(result, DispatchConflict):
        _raise_conflict(result)
    assert not isinstance(result, DispatchConflict)
    return {
        "runtime_epoch": result.runtime_epoch,
        "execution_deadline": result.execution_deadline,
        "remaining_s": result.remaining_s,
        "heartbeat_interval_s": result.heartbeat_interval_s,
    }


@router.post("/requests/{request_id}/heartbeat")
async def heartbeat_work_item_request(
    request_id: uuid.UUID, body: HeartbeatBody, session: SessionDep
) -> dict[str, Any]:
    result = await workitem_dispatch.heartbeat(
        session, request_id, runtime_epoch=body.runtime_epoch
    )
    if isinstance(result, DispatchConflict):
        _raise_conflict(result)
    assert not isinstance(result, DispatchConflict)
    return {
        "status": result.status,
        "terminal_cause": result.terminal_cause,
        "work_item_cancelled": result.work_item_cancelled,
    }


@router.post("/requests/{request_id}/hold-approval")
async def hold_work_item_for_approval(
    request_id: uuid.UUID, body: HeartbeatBody, session: SessionDep
) -> dict[str, Any]:
    result = await workitem_dispatch.hold_for_approval(
        session, request_id, runtime_epoch=body.runtime_epoch
    )
    if isinstance(result, DispatchConflict):
        _raise_conflict(result)
    assert not isinstance(result, DispatchConflict)
    return {
        "status": result.status,
        "terminal_cause": result.terminal_cause,
        "work_item_cancelled": result.work_item_cancelled,
    }


@router.post("/requests/{request_id}/owner-lost")
async def declare_work_item_owner_lost(
    request_id: uuid.UUID, body: OwnerLostBody, session: SessionDep
) -> dict[str, Any]:
    result = await workitem_dispatch.declare_owner_lost(
        session, request_id, owner=body.owner, runtime_epoch=body.runtime_epoch
    )
    if isinstance(result, DispatchConflict):
        _raise_conflict(result)
    assert not isinstance(result, DispatchConflict)
    return {"status": result.status, "terminal_cause": result.terminal_cause}


@router.post("/requests/{request_id}/finish")
async def finish_work_item_request(
    request_id: uuid.UUID, body: FinishBody, session: SessionDep
) -> dict[str, Any]:
    result = await workitem_dispatch.finish(
        session,
        request_id,
        runtime_epoch=body.runtime_epoch,
        outcome=body.outcome,
        cause=body.cause,
        detail=body.detail,
    )
    if isinstance(result, DispatchConflict):
        _raise_conflict(result)
    assert isinstance(result, WorkItemOutcome)
    row = await workitem_dispatch.get_request(session, request_id)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, {"code": "not_found"})
    return workitem_dispatch.request_view(row)


@router.post("/requests/{request_id}/termination/claim")
async def claim_work_item_termination(
    request_id: uuid.UUID, body: TerminationClaimBody, session: SessionDep
) -> dict[str, Any]:
    result = await workitem_dispatch.claim_termination(
        session, request_id, owner=body.owner
    )
    if isinstance(result, DispatchConflict):
        _raise_conflict(result)
    assert not isinstance(result, DispatchConflict)
    return {"runtime_epoch": result.runtime_epoch}


@router.post("/requests/{request_id}/termination")
async def record_work_item_termination(
    request_id: uuid.UUID, body: TerminationBody, session: SessionDep
) -> dict[str, Any]:
    result = await workitem_dispatch.record_termination(
        session,
        request_id,
        runtime_epoch=body.runtime_epoch,
        observation=body.observation,
    )
    if isinstance(result, DispatchConflict):
        _raise_conflict(result)
    row = await workitem_dispatch.get_request(session, request_id)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, {"code": "not_found"})
    return workitem_dispatch.request_view(row)
