"""Source policy administration routes, @spec PROTECTED-HOOK-SOURCE-3/6/7/10.

``GET``, ``PUT`` and ``DELETE`` on ``/agents/{agent_id}/hooks/{hook}/source-policy``,
``POST .../rotate`` and ``GET .../secret``. Authentication is the same
``require_api_key`` dependency as the legacy hook secret route (the platform key
or a live console session with console origin enforcement); a hook signature
never authenticates. Authentication and request shape (FastAPI's ordinary 422
list) run before any handler. Handlers take no request database session: every
source database connection comes from the gate pool first and the work pool
second. Every refusal raised by the source services has the body
``{"detail": {"code": ..., "committed_generation": ...}}`` and every handler
response carries ``Cache-Control: no-store``.

Mutations follow one order: an unset runtime directory is 503
``runtime_unavailable``; then an administrative slot, else 503
``broker_unavailable``; then the runtime files, read once on that slot, else
503 ``runtime_unavailable``; then for PUT the deployment's one runtime, else
422 ``unknown_source_reference``; then the gate, the coordinator's checks and
the registration, broker and SQL effects. A committed protected PUT or rotate
then publishes its active protected record once a reader bracketed evaluation
in its publication phase accepts (``accept`` or ``admission_closed``) and
answers 200 ``active``; otherwise 503 with the committed generation and the
outcome code, ``source_reservation_lost`` or ``broker_unavailable``. The source
secret route serves the scoped key only for a protected row whose active
protected record one reader session shows.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Annotated, Any

from curie_protected_hooks.source_policy_sql import SourceGate, SourcePolicySnapshot
from fastapi import APIRouter, Depends, Path, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncEngine

from .. import hook_source_signing
from ..auth import require_api_key
from ..config import get_settings
from ..hook_source_admin import SourceAdminError, SourceAdminService, policy_out
from ..hook_source_broker import ProvisionedSourceAuthority, admin_slot, source_activation
from ..hook_source_mutation import SourceMutationCoordinator
from ..hook_source_policy_schemas import (
    HookSourcePolicyMutation,
    HookSourcePolicyOut,
    HookSourcePolicyWrite,
    HookSourceRefusal,
    HookSourceSecretOut,
    SourceHook,
    SourceUuid,
)
from ..protected_runtime_files import RuntimeFilesInvalid, load_administration

router = APIRouter(
    prefix="/agents", tags=["hook-source-policy"], dependencies=[Depends(require_api_key)]
)

_NO_STORE = {"Cache-Control": "no-store"}
_PROTECTED_TARGET = "protected"

AgentPath = Annotated[SourceUuid, Path()]
HookPath = Annotated[SourceHook, Path()]

_REFUSAL: dict[str, Any] = {"model": HookSourceRefusal, "description": "Source refusal"}
# A request shape 422 is FastAPI's validation list; a reference 422 is a source refusal.
_SHAPE_OR_REFERENCE: dict[str, Any] = {
    "description": "Validation error or unknown source reference",
    "content": {
        "application/json": {
            "schema": {
                "anyOf": [
                    {"$ref": "#/components/schemas/HTTPValidationError"},
                    {"$ref": "#/components/schemas/HookSourceRefusal"},
                ]
            }
        }
    },
}
_READ_REFUSALS: dict[int | str, dict[str, Any]] = {404: _REFUSAL, 503: _REFUSAL}
_REFUSALS: dict[int | str, dict[str, Any]] = {**_READ_REFUSALS, 409: _REFUSAL}
_REFERENCE_REFUSALS: dict[int | str, dict[str, Any]] = {**_REFUSALS, 422: _SHAPE_OR_REFERENCE}


def _refusal(error: SourceAdminError) -> JSONResponse:
    """The SOURCE-3 refusal body; it names a generation, never a key.

    @spec PROTECTED-HOOK-SOURCE-3.
    """
    return JSONResponse(
        status_code=error.status_code,
        content={
            "detail": {"code": error.code, "committed_generation": error.committed_generation}
        },
        headers=_NO_STORE,
    )


def _ok(body: HookSourcePolicyOut) -> JSONResponse:
    """@spec PROTECTED-HOOK-SOURCE-3."""
    return JSONResponse(content=body.model_dump(mode="json"), headers=_NO_STORE)


def _stores(request: Request) -> tuple[SourceGate, AsyncEngine]:
    """The app's source gate and work engine, @spec PROTECTED-HOOK-SOURCE-2/3."""
    gate = getattr(request.app.state, "source_gate", None)
    engine = getattr(request.app.state, "engine", None)
    if not isinstance(gate, SourceGate) or not isinstance(engine, AsyncEngine):
        raise SourceAdminError("source_state_unavailable", 503)
    return gate, engine


async def _mutate(
    request: Request,
    run: Callable[[SourceMutationCoordinator], Awaitable[SourcePolicySnapshot]],
    references: tuple[str, str, str] | None = None,
) -> SourcePolicySnapshot:
    """Steps 2 through 7 of the SOURCE-3 mutation order, @spec PROTECTED-HOOK-SOURCE-3/6/10."""
    directory = get_settings().protected_runtime_dir
    if not directory:
        raise SourceAdminError("runtime_unavailable", 503)
    async with admin_slot() as slot:
        try:
            runtime = await slot.run(load_administration, directory)
        except RuntimeFilesInvalid:
            raise SourceAdminError("runtime_unavailable", 503) from None
        authority = ProvisionedSourceAuthority(runtime, slot)
        if references is not None:
            authority.check_references(*references)
        gate, engine = _stores(request)
        coordinator = SourceMutationCoordinator(
            gate,
            engine,
            authority_resolver=authority,
            target_check=authority.check_target,
        )
        return await run(coordinator)


@router.get(
    "/{agent_id}/hooks/{hook}/source-policy",
    response_model=HookSourcePolicyOut,
    responses=_READ_REFUSALS,
)
async def get_source_policy(agent_id: AgentPath, hook: HookPath, request: Request) -> JSONResponse:
    """Committed source policy and its publication activation.

    \f
    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-6/10.
    """
    directory = get_settings().protected_runtime_dir
    try:
        gate, engine = _stores(request)
        body = await SourceAdminService(gate, engine).read_policy(
            agent_id, hook, lambda policy: source_activation(policy, directory)
        )
    except SourceAdminError as error:
        return _refusal(error)
    return _ok(body)


@router.put(
    "/{agent_id}/hooks/{hook}/source-policy",
    response_model=HookSourcePolicyOut,
    responses=_REFERENCE_REFUSALS,
)
async def put_source_policy(
    agent_id: AgentPath, hook: HookPath, body: HookSourcePolicyWrite, request: Request
) -> JSONResponse:
    """Target mandatory read-only protected delivery under the deployment's runtime.

    The commit happens and the agent's legacy counter may advance; the row is
    then published once current runtime evidence is confirmed, answering 200
    ``active``. A refusal after the commit answers 503 with the committed
    generation and the source stays closed.
    \f
    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-5/6/7/10.
    """
    target = {
        "mode": _PROTECTED_TARGET,
        "tool_access": "read-only",
        "runtime_id": body.runtime_id,
        "qualification_id": body.qualification_id,
        "bundle_digest": body.bundle_digest,
    }
    try:
        policy = await _mutate(
            request,
            lambda coordinator: coordinator.mutate(
                agent_id, hook, body.expected_generation, body.operation_id, target
            ),
            (body.runtime_id, body.qualification_id, body.bundle_digest),
        )
    except SourceAdminError as error:
        return _refusal(error)
    return _ok(_active(policy, hook))


@router.delete(
    "/{agent_id}/hooks/{hook}/source-policy",
    response_model=HookSourcePolicyOut,
    responses=_REFUSALS,
)
async def delete_source_policy(
    agent_id: AgentPath,
    hook: HookPath,
    cas: Annotated[HookSourcePolicyMutation, Query()],
    request: Request,
) -> JSONResponse:
    """Commit and publish the ordinary tombstone.

    Pending history without a row commits a fresh tombstone above every
    attempt without rotating the legacy counter; an absent row without history
    is 409 ``source_not_configured``. \f
    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-5/6/7/10.
    """
    try:
        policy = await _mutate(
            request,
            lambda coordinator: coordinator.remove(
                agent_id, hook, cas.expected_generation, cas.operation_id
            ),
        )
    except SourceAdminError as error:
        return _refusal(error)
    return _ok(_active(policy, hook))


@router.post(
    "/{agent_id}/hooks/{hook}/source-policy/rotate",
    response_model=HookSourcePolicyOut,
    responses=_REFERENCE_REFUSALS,
)
async def rotate_source_policy(
    agent_id: AgentPath, hook: HookPath, body: HookSourcePolicyMutation, request: Request
) -> JSONResponse:
    """Allocate and publish a fresh generation for the current protected target.

    \f
    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-6/7/10.
    """
    try:
        policy = await _mutate(
            request,
            lambda coordinator: coordinator.rotate(
                agent_id, hook, body.expected_generation, body.operation_id
            ),
        )
    except SourceAdminError as error:
        return _refusal(error)
    return _ok(_active(policy, hook))


@router.get(
    "/{agent_id}/hooks/{hook}/source-policy/secret",
    response_model=HookSourceSecretOut,
    responses=_REFUSALS,
)
async def get_source_secret(agent_id: AgentPath, hook: HookPath, request: Request) -> JSONResponse:
    """The scoped source key of an active protected source; writes nothing.

    Served only when one reader session, after the gate is released, shows the
    row's active protected record; that attests publication, not current
    readiness. A rotation committing after that read makes the returned key
    already revoked. Every other state is refused with no key. \f
    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-4/6.
    """
    directory = get_settings().protected_runtime_dir
    try:
        gate, engine = _stores(request)
        policy = await SourceAdminService(gate, engine).refuse_secret(
            agent_id, hook, lambda row: source_activation(row, directory)
        )
        body = HookSourceSecretOut(
            agent_id=str(policy.agent_id),
            hook=policy.hook,
            generation=str(policy.generation),
            secret=hook_source_signing.derive(
                get_settings().api_key,
                agent_id=str(policy.agent_id),
                hook=policy.hook,
                generation=policy.generation,
            ),
        )
    except SourceAdminError as error:
        return _refusal(error)
    return JSONResponse(content=body.model_dump(mode="json"), headers=_NO_STORE)


def _active(policy: SourcePolicySnapshot, hook: str) -> HookSourcePolicyOut:
    """The DTO of a committed and published row, @spec PROTECTED-HOOK-SOURCE-3/6."""
    return policy_out(
        policy.agent_id,
        hook,
        policy,
        legacy_generation=policy.legacy_generation,
        activation="active",
        refusal_reason=None,
    )
