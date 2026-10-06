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
the registration, broker and SQL effects. Protected publication stays
unavailable until the LANE-4 ingress admission change, so a committed protected
PUT or rotate answers 503 ``source_publication_deferred`` with its committed
generation. The source secret route refuses every state in this slice.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Annotated, Any

from curie_protected_hooks.source_policy_sql import SourceGate, SourcePolicySnapshot
from fastapi import APIRouter, Depends, Path, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncEngine

from ..auth import require_api_key
from ..config import get_settings
from ..hook_source_admin import SourceAdminError, SourceAdminService, policy_out
from ..hook_source_broker import ProvisionedSourceAuthority, admin_slot, tombstone_activation
from ..hook_source_mutation import SourceMutationCoordinator
from ..hook_source_policy_schemas import (
    HookSourcePolicyMutation,
    HookSourcePolicyOut,
    HookSourcePolicyWrite,
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

_REFUSAL_SCHEMA: dict[str, Any] = {
    "description": "Source refusal",
    "content": {
        "application/json": {
            "schema": {
                "type": "object",
                "required": ["detail"],
                "properties": {
                    "detail": {
                        "type": "object",
                        "required": ["code", "committed_generation"],
                        "properties": {
                            "code": {"type": "string"},
                            "committed_generation": {"type": ["string", "null"]},
                        },
                    }
                },
            }
        }
    },
}
_REFUSALS: dict[int | str, dict[str, Any]] = {
    404: _REFUSAL_SCHEMA,
    409: _REFUSAL_SCHEMA,
    503: _REFUSAL_SCHEMA,
}


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
    responses=_REFUSALS,
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
            agent_id, hook, lambda policy: tombstone_activation(policy, directory)
        )
    except SourceAdminError as error:
        return _refusal(error)
    return _ok(body)


@router.put(
    "/{agent_id}/hooks/{hook}/source-policy",
    response_model=HookSourcePolicyOut,
    responses={**_REFUSALS, 422: _REFUSAL_SCHEMA},
)
async def put_source_policy(
    agent_id: AgentPath, hook: HookPath, body: HookSourcePolicyWrite, request: Request
) -> JSONResponse:
    """Target mandatory read-only protected delivery under the deployment's runtime.

    A committed protected PUT answers 503 ``source_publication_deferred`` with
    its committed generation: the commit happened, the agent's legacy counter
    may have advanced, and the source stays closed until the LANE-4 change.
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
    # Unreachable while protected publication is deferred; kept for the LANE-4 change.
    return _refusal(_deferred(policy))


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
                agent_id, hook, cas.expected_generation, cas.operation_id, refuse_unconfigured=True
            ),
        )
        body = policy_out(
            policy.agent_id,
            hook,
            policy,
            legacy_generation=policy.legacy_generation,
            activation="active",
            refusal_reason=None,
        )
    except SourceAdminError as error:
        return _refusal(error)
    return _ok(body)


@router.post(
    "/{agent_id}/hooks/{hook}/source-policy/rotate",
    response_model=HookSourcePolicyOut,
    responses={**_REFUSALS, 422: _REFUSAL_SCHEMA},
)
async def rotate_source_policy(
    agent_id: AgentPath, hook: HookPath, body: HookSourcePolicyMutation, request: Request
) -> JSONResponse:
    """Allocate a fresh generation for the current protected target.

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
    # Unreachable while protected publication is deferred; kept for the LANE-4 change.
    return _refusal(_deferred(policy))


@router.get(
    "/{agent_id}/hooks/{hook}/source-policy/secret",
    response_model=HookSourceSecretOut,
    responses=_REFUSALS,
)
async def get_source_secret(agent_id: AgentPath, hook: HookPath, request: Request) -> JSONResponse:
    """Refuses every state in this slice and writes nothing.

    \f
    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-6.
    """
    try:
        gate, engine = _stores(request)
        await SourceAdminService(gate, engine).refuse_secret(agent_id, hook)
    except SourceAdminError as error:
        return _refusal(error)
    return _refusal(SourceAdminError("source_publication_deferred", 503))


def _deferred(policy: SourcePolicySnapshot) -> SourceAdminError:
    """@spec PROTECTED-HOOK-SOURCE-3."""
    error = SourceAdminError("source_publication_deferred", 503)
    error.committed_generation = str(policy.generation)
    return error
