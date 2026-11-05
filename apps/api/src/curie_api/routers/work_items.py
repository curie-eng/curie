"""Internal worker-token WorkItem admission and dispatch routes."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Literal, Self

from channel_protocol.work_item_events import CI_FIRST_FIX_ROUND, CI_MAX_ROUNDS
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field, StrictBool, model_validator

from curie_api.workitems.lifecycle import WorkItemConflict, WorkItemOutcome

from .. import personal_execution, workitem_dispatch
from ..auth import require_internal_worker_token
from ..config import get_settings
from ..deps import LivenessSessionDep, SessionDep
from ..forges.types import RepositoryRef, TrackerIssueRef
from ..personal_execution import ModelAuthMode, OwnerContext
from ..workitem_dispatch import DispatchConflict

router = APIRouter(
    prefix="/v1/internal/work-items",
    tags=["internal-work-items"],
    dependencies=[Depends(require_internal_worker_token)],
)


class TrackerIssueIn(BaseModel):
    """The tracker issue that keys the WorkItem (ADR 0197 identity rule 1)."""

    kind: str = Field(min_length=1)
    host: str = Field(min_length=1)
    scope_id: str = Field(min_length=1)
    issue_id: str = Field(min_length=1)
    display_key: str | None = Field(default=None, min_length=1)


class RepositoryIn(BaseModel):
    """The repository frozen on the WorkItem (ADR 0197 identity rules 2 and 3)."""

    code_host_kind: str = Field(min_length=1)
    host: str = Field(min_length=1)
    project_id: str = Field(min_length=1)
    path: str = Field(min_length=1)


class OwnerContextIn(BaseModel):
    """The verified owner a personal execution runs as (#5376, ADR 0268).

    Accepted only here, from the trusted worker: a principal of the agent's
    tenant and the enrollment generation it is bound to.
    """

    tenant_id: uuid.UUID
    principal_id: uuid.UUID
    enrollment_generation: int = Field(ge=1)


class AdmissionFacts(BaseModel):
    agent_id: uuid.UUID
    kind: str = Field(min_length=1)
    address: str = Field(min_length=1)
    reply_conversation_id: str = Field(min_length=1)
    tracker: TrackerIssueIn
    repository: RepositoryIn
    code_host_installation_id: int | None = Field(default=None, gt=0)
    objective: str = Field(min_length=1, max_length=65536)
    requester: str = Field(min_length=1)
    request_id: uuid.UUID
    model_auth_mode: ModelAuthMode = "service"
    owner: OwnerContextIn | None = None

    def admission(self) -> workitem_dispatch.Admission:
        tracker, repository, owner = self.tracker, self.repository, self.owner
        return workitem_dispatch.Admission(
            agent_id=self.agent_id,
            kind=self.kind,
            address=self.address,
            reply_conversation_id=self.reply_conversation_id,
            issue=TrackerIssueRef(
                tracker.kind,
                tracker.host,
                tracker.scope_id,
                tracker.issue_id,
                tracker.display_key,
            ),
            repository=RepositoryRef(
                repository.code_host_kind, repository.host, repository.project_id, repository.path
            ),
            code_host_installation_id=self.code_host_installation_id,
            objective=self.objective,
            requester=self.requester,
            request_id=self.request_id,
            model_auth_mode=self.model_auth_mode,
            owner=(
                OwnerContext(
                    tenant_id=owner.tenant_id,
                    principal_id=owner.principal_id,
                    enrollment_generation=owner.enrollment_generation,
                )
                if owner is not None
                else None
            ),
        )


class CancelBody(BaseModel):
    expected_version: int = Field(ge=1)


class AcquireBody(BaseModel):
    owner: str = Field(min_length=1)
    generation: int = Field(ge=1)
    wake: workitem_dispatch.FactoryWake
    context: workitem_dispatch.FactoryContext | None


class DeferBody(BaseModel):
    owner: str = Field(min_length=1)
    generation: int = Field(ge=1)
    reason: str = Field(min_length=1)
    capacity: bool
    # #4930: a pre-start workspace preparation diagnostic. Stored on
    # non-capacity `not_started:` and `workspace_timeout` defers, redacted
    # and clipped by the API.
    start_failure_detail: str | None = None


class StartBody(BaseModel):
    owner: str = Field(min_length=1)
    generation: int = Field(ge=1)
    claim_name: str = Field(min_length=1)
    sandbox_name: str = Field(min_length=1)
    context: workitem_dispatch.FactoryContext


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
    # The CI fix round whose turn ended unpublished (#4330). Required for
    # ``ci_fix_unpublished`` and refused for every other cause.
    ci_fix_round: int | None = Field(default=None, ge=CI_FIRST_FIX_ROUND, le=CI_MAX_ROUNDS)

    @model_validator(mode="after")
    def _round_matches_cause(self) -> Self:
        unpublished = self.cause.strip() == "ci_fix_unpublished"
        if unpublished and self.ci_fix_round is None:
            raise ValueError("ci_fix_unpublished requires ci_fix_round")
        if not unpublished and self.ci_fix_round is not None:
            raise ValueError("ci_fix_round is only valid with cause ci_fix_unpublished")
        return self


class TerminationClaimBody(BaseModel):
    owner: str = Field(min_length=1)


class TerminationBody(BaseModel):
    runtime_epoch: int = Field(ge=1)
    observation: str = Field(min_length=1)


class RunningWorkItemRequest(BaseModel):
    lease_held_to_deadline: StrictBool
    work_item_id: uuid.UUID
    request_id: uuid.UUID
    runtime_epoch: int
    execution_deadline: datetime
    status: Literal["running"]
    agent_id: uuid.UUID
    generation: int
    conversation_id: str
    wait_deadline: datetime
    repository_path: str
    context: workitem_dispatch.FactoryContext | None
    refusal: workitem_dispatch.RefusalCode | None


class PersonalContextRequestOut(BaseModel):
    """The WorkItem's latest request and the generation it snapshotted."""

    id: uuid.UUID
    sequence: int
    status: str
    version: int
    enrollment_generation: int | None


class PersonalContextEnrollmentOut(BaseModel):
    """The owner's enrollment header: its generation, state and CAS version."""

    generation: int
    active: bool
    version: int


class PersonalContextOut(BaseModel):
    """A personal WorkItem's owner, bound generation and versions (#5376).

    Token free: identifiers, generations and versions only. ``enrollment`` is
    null when the owner has no enrollment row.
    """

    work_item_id: uuid.UUID
    agent_id: uuid.UUID
    tenant_id: uuid.UUID
    model_auth_mode: ModelAuthMode
    owner_principal_id: uuid.UUID
    enrollment_generation: int
    work_item_version: int
    request: PersonalContextRequestOut | None
    enrollment: PersonalContextEnrollmentOut | None


class RevalidationBody(BaseModel):
    """The versions the caller read, compared and set in one transaction."""

    tenant_id: uuid.UUID
    agent_id: uuid.UUID
    owner_principal_id: uuid.UUID
    request_id: uuid.UUID
    expected_work_item_version: int = Field(ge=1)
    expected_request_version: int = Field(ge=1)
    expected_enrollment_version: int = Field(ge=1)


class RevalidationOut(BaseModel):
    """The generation the WorkItem is now bound to and the versions it moved to."""

    work_item_id: uuid.UUID
    request_id: uuid.UUID
    previous_generation: int
    enrollment_generation: int
    work_item_version: int
    request_version: int
    enrollment_version: int


# One body for a missing, other-tenant, other-agent, other-owner or service item.
_NOT_FOUND = {"code": "not_found"}


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
    result = await workitem_dispatch.admit(session, facts.admission())
    if isinstance(result, WorkItemOutcome):
        if result.request is None:
            raise HTTPException(status.HTTP_409_CONFLICT, {"code": "not_found"})
        return _admission_body(result)
    _raise_conflict(result)
    raise HTTPException(status.HTTP_409_CONFLICT, {"code": "not_found"})


@router.get("/running", response_model=RunningWorkItemRequest)
async def running_work_item_request(
    conversation_id: str,
    session: SessionDep,
    wake_kind: str,
    wake_address: str,
    wake_conversation_id: str,
    wake_adapter: str | None = Query(...),
) -> RunningWorkItemRequest:
    state, row = await workitem_dispatch.running_for_conversation(
        session,
        conversation_id,
        wake=workitem_dispatch.FactoryWake(
            kind=wake_kind,
            address=wake_address,
            adapter=wake_adapter or None,
            conversation_id=wake_conversation_id,
        ),
    )
    if isinstance(row, DispatchConflict):
        _raise_conflict(row)
    assert not isinstance(row, DispatchConflict)
    if state == "absent" or row is None:
        if state == "ended":
            raise HTTPException(status.HTTP_409_CONFLICT, {"code": "execution_ended"})
        raise HTTPException(status.HTTP_404_NOT_FOUND, {"code": "not_found"})
    return RunningWorkItemRequest(
        lease_held_to_deadline=row.lease_held_to_deadline,
        work_item_id=row.work_item_id,
        request_id=row.request_id,
        runtime_epoch=row.runtime_epoch,
        execution_deadline=row.execution_deadline,
        status="running",
        agent_id=row.agent_id,
        generation=row.generation,
        conversation_id=row.conversation_id,
        wait_deadline=row.wait_deadline,
        repository_path=row.repository_path,
        context=row.context,
        refusal=row.refusal,
    )


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


@router.get("/{work_item_id}/personal-context", response_model=PersonalContextOut)
async def read_personal_context(
    work_item_id: uuid.UUID,
    tenant_id: uuid.UUID,
    agent_id: uuid.UUID,
    owner_principal_id: uuid.UUID,
    session: SessionDep,
) -> PersonalContextOut:
    """The token-free facts a worker revalidates a personal WorkItem from (#5376)."""

    context = await personal_execution.read_context(
        session,
        work_item_id=work_item_id,
        tenant_id=tenant_id,
        agent_id=agent_id,
        owner_principal_id=owner_principal_id,
    )
    if context is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, _NOT_FOUND)
    request, enrollment = context.request, context.enrollment
    return PersonalContextOut(
        work_item_id=context.work_item_id,
        agent_id=context.agent_id,
        tenant_id=context.tenant_id,
        model_auth_mode=context.model_auth_mode,
        owner_principal_id=context.owner_principal_id,
        enrollment_generation=context.enrollment_generation,
        work_item_version=context.work_item_version,
        request=(
            PersonalContextRequestOut(
                id=request.id,
                sequence=request.sequence,
                status=request.status,
                version=request.version,
                enrollment_generation=request.enrollment_generation,
            )
            if request is not None
            else None
        ),
        enrollment=(
            PersonalContextEnrollmentOut(
                generation=enrollment.generation,
                active=enrollment.active,
                version=enrollment.version,
            )
            if enrollment is not None
            else None
        ),
    )


@router.post("/{work_item_id}/personal-context/revalidate", response_model=RevalidationOut)
async def revalidate_personal_context(
    work_item_id: uuid.UUID, body: RevalidationBody, session: SessionDep
) -> RevalidationOut:
    """Bind a personal WorkItem to its owner's current enrollment generation.

    Compare-and-set on the WorkItem, request and enrollment versions; a stale
    version, another owner, an inactive enrollment, a live runtime or an
    unchanged generation is 409 with its code (#5376).
    """

    result = await personal_execution.revalidate(
        session,
        work_item_id=work_item_id,
        tenant_id=body.tenant_id,
        agent_id=body.agent_id,
        owner_principal_id=body.owner_principal_id,
        request_id=body.request_id,
        expected_work_item_version=body.expected_work_item_version,
        expected_request_version=body.expected_request_version,
        expected_enrollment_version=body.expected_enrollment_version,
    )
    if isinstance(result, personal_execution.ContextConflict):
        if result.code == "not_found":
            raise HTTPException(status.HTTP_404_NOT_FOUND, _NOT_FOUND)
        raise HTTPException(status.HTTP_409_CONFLICT, {"code": result.code})
    return RevalidationOut(
        work_item_id=result.work_item_id,
        request_id=result.request_id,
        previous_generation=result.previous_generation,
        enrollment_generation=result.enrollment_generation,
        work_item_version=result.work_item_version,
        request_version=result.request_version,
        enrollment_version=result.enrollment_version,
    )


@router.post("/requests/{request_id}/acquire")
async def acquire_work_item_request(
    request_id: uuid.UUID, body: AcquireBody, session: SessionDep, request: Request
) -> dict[str, Any]:
    result = await workitem_dispatch.acquire(
        session,
        request_id,
        owner=body.owner,
        generation=body.generation,
        wake=body.wake,
        context=body.context,
        client=request.app.state.http_client,
    )
    if isinstance(result, DispatchConflict):
        _raise_conflict(result)
    assert not isinstance(result, DispatchConflict)
    return {
        "generation": result.generation,
        "work_item_id": result.work_item_id,
        "conversation_id": result.conversation_id,
        "wait_deadline": result.wait_deadline,
        "repository_path": result.repository_path,
        "context": result.context,
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
        start_failure_detail=body.start_failure_detail,
    )
    if isinstance(result, DispatchConflict):
        _raise_conflict(result)
    assert not isinstance(result, DispatchConflict)
    return {
        "dispatch_generation": result.dispatch_generation,
        "not_before": result.not_before,
        "terminal_cause": result.terminal_cause,
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
        context=body.context,
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
    request_id: uuid.UUID, body: HeartbeatBody, session: LivenessSessionDep
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
        ci_fix_round=body.ci_fix_round,
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
