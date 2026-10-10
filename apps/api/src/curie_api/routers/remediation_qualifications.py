"""Remediation qualification routes, @spec AUTOMATED-REMEDIATION-22.

Exactly three routes under ``/agents/{agent_id}/remediation-qualifications``:

* ``PUT /{qualification_id}`` writes a qualification record after checking its
  evidence by state and digest (``remediation_qualifications.record_qualification``);
* ``POST /{qualification_id}/verifier-runs`` starts a verifier evaluation of the
  declared verifier against a literal member of the action's allowed targets;
  the body is exactly ``{"hook", "action", "target"}`` and it creates only
  ``read`` executions with ``authority_kind`` ``qualification``;
* ``GET /{qualification_id}/verifier-runs/{run_id}`` reads a run and its outcome.

No route here, or anywhere, lets an administrator request a forward write:
forward evidence comes only from ordinary remediation approvals.

Authentication is ``require_api_key``; both writes also need an ADR 0106
operator principal in ``X-Curie-Approval-Principal``, verified exactly as the
policy writes verify it (``remediation_policy.require_operator_principal``) and
recorded as the actor, because a record enables automatic execution (the
2026-10-07 principal ruling). Every handler response carries
``Cache-Control: no-store`` and every refusal is ``{"detail": {"code": ...}}``.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.responses import JSONResponse

from ..auth import require_api_key
from ..deps import SessionDep, StoreDep
from ..hook_source_policy_schemas import SourceUuid
from ..models import RemediationQualification, RemediationQualificationVerifierRun
from ..remediation_qualifications import (
    QualificationRefused,
    read_verifier_run,
    record_qualification,
    start_verifier_run,
)
from ..schemas.remediation_policy import RemediationPolicyRefusal
from ..schemas.remediation_qualifications import (
    RemediationQualificationOut,
    RemediationQualificationWrite,
    RemediationVerifierRunOut,
    RemediationVerifierRunStart,
)
from .remediation_policy import OperatorDep

router = APIRouter(
    prefix="/agents",
    tags=["remediation-qualifications"],
    dependencies=[Depends(require_api_key)],
)

_NO_STORE = {"Cache-Control": "no-store"}
_BASE = "/{agent_id}/remediation-qualifications/{qualification_id}"

AgentPath = Annotated[SourceUuid, Path()]
QualificationPath = Annotated[uuid.UUID, Path()]

_REFUSAL: dict[str, Any] = {"model": RemediationPolicyRefusal, "description": "Named refusal"}
_SHAPE_OR_REFUSAL: dict[str, Any] = {
    "description": "Validation error or named refusal",
    "content": {
        "application/json": {
            "schema": {
                "anyOf": [
                    {"$ref": "#/components/schemas/HTTPValidationError"},
                    {"$ref": "#/components/schemas/RemediationPolicyRefusal"},
                ]
            }
        }
    },
}
_WRITE_REFUSALS: dict[int | str, dict[str, Any]] = {
    403: _REFUSAL,
    409: _REFUSAL,
    422: _SHAPE_OR_REFUSAL,
}


def _refusal(error: QualificationRefused) -> JSONResponse:
    """@spec AUTOMATED-REMEDIATION-22."""
    return JSONResponse(
        status_code=error.status_code,
        content={"detail": {"code": error.code, "message": error.message}},
        headers=_NO_STORE,
    )


def _record_out(record: RemediationQualification) -> JSONResponse:
    body = RemediationQualificationOut(
        id=str(record.id),
        agent_id=str(record.agent_id),
        hook=record.hook,
        action=record.action,
        generation=str(record.generation) if record.generation is not None else None,
        connector=record.connector,
        tool=record.tool,
        connector_digest=record.connector_digest,
        verifier_sha256=record.verifier_sha256,
        reversibility=record.reversibility,
        recorded_by=record.recorded_by,
        worst_case=record.worst_case,
        evidence=dict(record.evidence),
        created_at=record.created_at,
    )
    return JSONResponse(content=body.model_dump(mode="json"), headers=_NO_STORE)


def _run_out(run: RemediationQualificationVerifierRun, status_code: int = 200) -> JSONResponse:
    body = RemediationVerifierRunOut(
        id=str(run.id),
        qualification_id=str(run.qualification_id),
        hook=run.hook,
        action=run.action,
        target=run.target,
        generation=str(run.generation),
        started_by=run.started_by,
        started_at=run.started_at,
        outcome=run.outcome,
        decided_at=run.decided_at,
    )
    return JSONResponse(
        status_code=status_code, content=body.model_dump(mode="json"), headers=_NO_STORE
    )


@router.put(_BASE, response_model=RemediationQualificationOut, responses=_WRITE_REFUSALS)
async def put_remediation_qualification(
    agent_id: AgentPath,
    qualification_id: QualificationPath,
    body: RemediationQualificationWrite,
    session: SessionDep,
    store: StoreDep,
    principal: OperatorDep,
) -> JSONResponse:
    """Record a qualification of one action declaration, recorded by the operator principal.

    The evidence references are checked by state and digest; any refusal writes
    nothing. A replay answers the record; another body under the same id is
    ``qualification_conflict``.
    \f
    @spec AUTOMATED-REMEDIATION-22 @spec AUTOMATED-REMEDIATION-23.
    """
    try:
        record = await record_qualification(
            session,
            store,
            agent_id=uuid.UUID(agent_id),
            qualification_id=qualification_id,
            hook=body.hook,
            action=body.action,
            generation=int(body.generation),
            evidence=body.evidence,
            worst_case=body.worst_case,
            principal=principal,
        )
    except QualificationRefused as error:
        await session.rollback()
        return _refusal(error)
    return _record_out(record)


@router.post(
    f"{_BASE}/verifier-runs",
    response_model=RemediationVerifierRunOut,
    status_code=201,
    responses=_WRITE_REFUSALS,
)
async def start_remediation_verifier_run(
    agent_id: AgentPath,
    qualification_id: QualificationPath,
    body: RemediationVerifierRunStart,
    session: SessionDep,
    store: StoreDep,
    principal: OperatorDep,
) -> JSONResponse:
    """Start a verifier evaluation of the declared verifier for a qualification.

    Only ``read`` executions of the declared verifier are created, against a
    target that is a literal member of the action's allowed list.
    \f
    @spec AUTOMATED-REMEDIATION-22 @spec AUTOMATED-REMEDIATION-18.
    """
    try:
        run = await start_verifier_run(
            session,
            store,
            agent_id=uuid.UUID(agent_id),
            qualification_id=qualification_id,
            hook=body.hook,
            action=body.action,
            target=body.target,
            principal=principal,
        )
    except QualificationRefused as error:
        await session.rollback()
        return _refusal(error)
    return _run_out(run, status_code=201)


@router.get(
    f"{_BASE}/verifier-runs/{{run_id}}",
    response_model=RemediationVerifierRunOut,
    responses={404: {"description": "No such verifier run"}},
)
async def get_remediation_verifier_run(
    agent_id: AgentPath,
    qualification_id: QualificationPath,
    run_id: Annotated[uuid.UUID, Path()],
    session: SessionDep,
) -> JSONResponse:
    """A qualification verifier run and its outcome.

    \f
    @spec AUTOMATED-REMEDIATION-22.
    """
    run = await read_verifier_run(session, uuid.UUID(agent_id), qualification_id, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="verifier run not found", headers=_NO_STORE)
    return _run_out(run)
