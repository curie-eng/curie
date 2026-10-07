"""Remediation policy administration routes, @spec AUTOMATED-REMEDIATION-3.

``GET``, ``PUT`` and ``DELETE`` on ``/agents/{agent_id}/hooks/{hook}/remediation-policy``
and ``POST .../arm`` and ``POST .../disarm``. Authentication is the same
``require_api_key`` dependency as the source policy routes; a hook signature or
the hook's scoped key never authenticates, so the hook ingress, the support
probe and the delivery body have no path to these tables.

Every write also requires an ADR 0106 operator principal in
``X-Curie-Approval-Principal``, verified as the approval resolver verifies an
operator token (signed by the platform key, approval scope), and records its
subject as ``bound_by`` on the generation it writes. A write without one is
refused ``operator_principal_required``. Reads need no principal.

Every handler response, refusals included, carries ``Cache-Control: no-store``,
and every refusal is ``{"detail": {"code": ...}}``. The routes stay readable and
writable with ``CURIE_REMEDIATION_ENABLED`` off so a policy can be staged before
activation (AUTOMATED-REMEDIATION-1).
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException, Path, Query
from fastapi.responses import JSONResponse

from .. import approval_principal
from ..approval_auth import APPROVAL_PRINCIPAL_HEADER
from ..auth import require_api_key
from ..config import get_settings
from ..deps import SessionDep, StoreDep
from ..hook_source_policy_schemas import SourceHook, SourceUuid
from ..remediation_policy_document import PolicyRefused, validate_document
from ..remediation_policy_store import PolicyGeneration, Verb, read_policy, write_policy
from ..remediation_verifier import NOT_INDEPENDENT, independence_refusal
from ..schemas.remediation_policy import (
    RemediationPolicyMutation,
    RemediationPolicyOut,
    RemediationPolicyRefusal,
    RemediationPolicyWrite,
)

router = APIRouter(
    prefix="/agents", tags=["remediation-policy"], dependencies=[Depends(require_api_key)]
)

_NO_STORE = {"Cache-Control": "no-store"}
_BASE = "/{agent_id}/hooks/{hook}/remediation-policy"

AgentPath = Annotated[SourceUuid, Path()]
HookPath = Annotated[SourceHook, Path()]

_REFUSAL: dict[str, Any] = {"model": RemediationPolicyRefusal, "description": "Policy refusal"}
_SHAPE_OR_POLICY: dict[str, Any] = {
    "description": "Validation error or named policy refusal",
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
_READ_REFUSALS: dict[int | str, dict[str, Any]] = {404: _REFUSAL}
_WRITE_REFUSALS: dict[int | str, dict[str, Any]] = {
    403: _REFUSAL,
    404: _REFUSAL,
    409: _REFUSAL,
    422: _SHAPE_OR_POLICY,
}


async def require_operator_principal(
    x_curie_approval_principal: Annotated[
        str | None, Header(alias=APPROVAL_PRINCIPAL_HEADER)
    ] = None,
) -> str:
    """The subject of a valid ADR 0106 operator principal, else a refusal.

    Verified as the approval resolver verifies an operator token: signed by the
    platform key with the approval scope and unexpired. A chat attestation, an
    adapter credential or no principal at all is ``operator_principal_required``.
    \f
    @spec AUTOMATED-REMEDIATION-3.
    """
    claims = None
    token = x_curie_approval_principal
    if token is not None and approval_principal.unverified_kind(token) == "operator":
        claims = approval_principal.verify_claims(
            token, get_settings().api_key, scope=approval_principal.APPROVE_SCOPE
        )
    if claims is None or claims.kind != "operator":
        raise HTTPException(
            status_code=403,
            detail={
                "code": "operator_principal_required",
                "message": (
                    f"policy writes need an operator principal in {APPROVAL_PRINCIPAL_HEADER}"
                ),
            },
            headers=_NO_STORE,
        )
    return claims.subject


OperatorDep = Annotated[str, Depends(require_operator_principal)]


def _refusal(error: PolicyRefused) -> JSONResponse:
    """@spec AUTOMATED-REMEDIATION-2."""
    detail: dict[str, Any] = {"code": error.code}
    if error.path is not None:
        detail["path"] = error.path
    if error.message is not None:
        detail["message"] = error.message
    return JSONResponse(
        status_code=error.status_code, content={"detail": detail}, headers=_NO_STORE
    )


def _ok(policy: PolicyGeneration) -> JSONResponse:
    """@spec AUTOMATED-REMEDIATION-3."""
    body = RemediationPolicyOut(
        agent_id=str(policy.agent_id),
        hook=policy.hook,
        generation=str(policy.generation),
        armed=policy.armed,
        active=policy.active,
        bound_by=policy.bound_by,
        policy=policy.document,
        updated_at=policy.created_at,
    )
    return JSONResponse(content=body.model_dump(mode="json"), headers=_NO_STORE)


async def _write(
    session: SessionDep,
    agent_id: str,
    hook: str,
    verb: Verb,
    cas: RemediationPolicyMutation,
    principal: str,
    document: dict[str, Any] | None = None,
) -> JSONResponse:
    """@spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3."""
    try:
        policy = await write_policy(
            session,
            agent_id=uuid.UUID(agent_id),
            hook=hook,
            verb=verb,
            expected_generation=int(cas.expected_generation),
            operation_id=uuid.UUID(cas.operation_id),
            principal=principal,
            document=document,
        )
    except PolicyRefused as error:
        return _refusal(error)
    return _ok(policy)


@router.get(_BASE, response_model=RemediationPolicyOut, responses=_READ_REFUSALS)
async def get_remediation_policy(
    agent_id: AgentPath, hook: HookPath, session: SessionDep
) -> JSONResponse:
    """The current remediation policy generation of a hook.

    \f
    @spec AUTOMATED-REMEDIATION-3.
    """
    try:
        policy = await read_policy(session, uuid.UUID(agent_id), hook)
    except PolicyRefused as error:
        return _refusal(error)
    return _ok(policy)


@router.put(_BASE, response_model=RemediationPolicyOut, responses=_WRITE_REFUSALS)
async def put_remediation_policy(
    agent_id: AgentPath,
    hook: HookPath,
    body: RemediationPolicyWrite,
    session: SessionDep,
    store: StoreDep,
    principal: OperatorDep,
) -> JSONResponse:
    """Bind, tighten or widen a protected hook's remediation policy.

    A new binding starts disarmed; a later write keeps the armed flag. Each
    write creates a generation recorded with the operator principal.
    \f
    @spec AUTOMATED-REMEDIATION-1 @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3.
    @spec AUTOMATED-REMEDIATION-17: every verifier is independent of its acting
    connector in the agent's in-force version, or the write is refused
    ``verifier_not_independent`` and creates no generation.
    """
    try:
        document = validate_document(body.policy)
    except PolicyRefused as error:
        return _refusal(error)
    for index, action in enumerate(document.get("actions") or []):
        if isinstance(action, dict) and await independence_refusal(
            session, uuid.UUID(agent_id), action, store=store
        ):
            return _refusal(
                PolicyRefused(
                    NOT_INDEPENDENT,
                    path=f"/actions/{index}/verifier/connector",
                    message=(
                        "the verifier must read through its own connector and credential, "
                        "not the acting connector's"
                    ),
                )
            )
    # The check only read; the store opens its own transaction for the write.
    await session.rollback()
    return await _write(session, agent_id, hook, "bind", body, principal, document)


@router.delete(_BASE, response_model=RemediationPolicyOut, responses=_WRITE_REFUSALS)
async def delete_remediation_policy(
    agent_id: AgentPath,
    hook: HookPath,
    cas: Annotated[RemediationPolicyMutation, Query()],
    session: SessionDep,
    principal: OperatorDep,
) -> JSONResponse:
    """Remove the policy: a new generation, inactive, disarmed and with no actions.

    \f
    @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3.
    """
    return await _write(session, agent_id, hook, "remove", cas, principal)


@router.post(f"{_BASE}/arm", response_model=RemediationPolicyOut, responses=_WRITE_REFUSALS)
async def arm_remediation_policy(
    agent_id: AgentPath,
    hook: HookPath,
    body: RemediationPolicyMutation,
    session: SessionDep,
    principal: OperatorDep,
) -> JSONResponse:
    """Arm the current policy as a new generation.

    \f
    @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3.
    """
    return await _write(session, agent_id, hook, "arm", body, principal)


@router.post(f"{_BASE}/disarm", response_model=RemediationPolicyOut, responses=_WRITE_REFUSALS)
async def disarm_remediation_policy(
    agent_id: AgentPath,
    hook: HookPath,
    body: RemediationPolicyMutation,
    session: SessionDep,
    principal: OperatorDep,
) -> JSONResponse:
    """Disarm the current policy as a new generation.

    \f
    @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3.
    """
    return await _write(session, agent_id, hook, "disarm", body, principal)
