"""Platform operator administration of HTTP authorities and issuer group filters."""

import hashlib
import json
import uuid
from collections.abc import Callable, Coroutine
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api import agent_role_policy, identity_audit, principal_authority
from curie_api.auth import require_platform_key
from curie_api.deps import IdentityRuntimeDep, SessionDep
from curie_api.identity.http_credential_registry import ResolvedProviderCredential
from curie_api.identity.http_invocation import HttpInvocationRuntime
from curie_api.models import AgentAccessPolicy, IdentityAuditEvent, TenantIdentityProvider
from curie_api.schemas.agent_access import SetRoleFilterV1
from curie_api.schemas.http_authority import AuthorityConfigV1, HttpSelectionV1
from curie_api.schemas.http_identity import GroupV1


class OperatorRoute(APIRoute):
    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def redacted(request: Request) -> Response:
            try:
                response = await handler(request)
            except (RequestValidationError, ValidationError):
                raise _refusal("invalid_identity_operator_request", 422) from None
            except HTTPException as error:
                error.headers = {**(error.headers or {}), "Cache-Control": "private, no-store"}
                raise
            response.headers["Cache-Control"] = "private, no-store"
            return response

        return redacted


router = APIRouter(
    prefix="/identity",
    tags=["identity-operator"],
    route_class=OperatorRoute,
    dependencies=[Depends(require_platform_key)],
)


class OperatorMutation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: uuid.UUID
    expected_version: int = Field(ge=0, strict=True)


class AuthorityMutation(OperatorMutation):
    expected_lifecycle_generation: int = Field(ge=0, strict=True)


class AuthoritySelectionMutation(AuthorityMutation):
    selection: HttpSelectionV1


class GroupFilterMutation(OperatorMutation):
    groups: list[GroupV1] = Field(min_length=1, max_length=128)


def _refusal(code: str, status: int = 409) -> HTTPException:
    return HTTPException(status, code, headers={"Cache-Control": "private, no-store"})


def _result(row: TenantIdentityProvider) -> dict[str, Any]:
    return {
        "configuration": principal_authority.stored_configuration(row).model_dump(mode="json"),
        "lifecycle_generation": row.lifecycle_generation,
    }


def _digest(operation: str, target: str, body: OperatorMutation) -> str:
    raw = json.dumps([operation, target, body.model_dump(mode="json")], sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()


async def _replay(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    body: OperatorMutation,
    digest: str,
) -> dict[str, Any] | None:
    event = await session.scalar(
        select(IdentityAuditEvent).where(
            IdentityAuditEvent.tenant_id == tenant_id,
            IdentityAuditEvent.event_type == "identity.operator_request",
            IdentityAuditEvent.details["request_id"].astext == str(body.request_id),
        )
    )
    if event is None:
        return None
    if event.details["digest"] != digest:
        raise _refusal("operator_request_conflict")
    result: dict[str, Any] = event.details["result"]
    return result


async def _record(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    body: OperatorMutation,
    digest: str,
    operation: str,
    result: dict[str, Any],
) -> dict[str, Any]:
    identity_audit.record(
        session,
        tenant_id=tenant_id,
        event_type="identity.operator_request",
        actor="platform",
        details={
            "request_id": str(body.request_id),
            "digest": digest,
            "operation": operation,
            "result": result,
        },
    )
    await session.commit()
    return result


@router.get("/authorities")
async def list_authorities(
    session: SessionDep,
    limit: int = Query(default=100, ge=1, le=200),
    after: uuid.UUID | None = None,
) -> dict[str, Any]:
    statement = (
        select(TenantIdentityProvider)
        .where(
            TenantIdentityProvider.http_selection.is_not(None),
        )
        .order_by(TenantIdentityProvider.tenant_id)
        .limit(limit)
    )
    if after is not None:
        statement = statement.where(TenantIdentityProvider.tenant_id > after)
    rows = list(await session.scalars(statement))
    try:
        return {
            "items": [_result(row) for row in rows],
            "next_cursor": str(rows[-1].tenant_id) if len(rows) == limit else None,
        }
    except principal_authority.PrincipalAuthorityRefusal as error:
        raise _refusal(error.code) from None


@router.get("/authorities/{tenant_id}")
async def get_authority(tenant_id: uuid.UUID, session: SessionDep) -> dict[str, Any]:
    try:
        _, row = await principal_authority.lock_scope(session, tenant_id)
        if row is None or row.http_selection is None:
            raise _refusal("http_authority_required", 404)
        return _result(row)
    except principal_authority.PrincipalAuthorityRefusal as error:
        raise _refusal(error.code) from None


async def _write_authority(
    tenant_id: uuid.UUID,
    body: AuthorityMutation,
    session: AsyncSession,
    operation: Literal["adopt", "replace", "retire"],
    runtime: HttpInvocationRuntime | None,
    dry_run: bool = False,
) -> dict[str, Any]:
    try:
        _, row = await principal_authority.lock_scope(session, tenant_id)
        if isinstance(body, AuthoritySelectionMutation):
            candidate = AuthorityConfigV1(
                schema_version=1,
                tenant_id=str(tenant_id),
                version=body.expected_version + 1,
                selection=body.selection,
            )
            if runtime is None or not isinstance(
                runtime._providers.resolve(candidate),
                ResolvedProviderCredential,
            ):
                raise _refusal("provider_credential_unavailable")
        digest = _digest(operation, str(tenant_id), body)
        replay = await _replay(session, tenant_id, body, digest)
        if replay is not None:
            return {"dry_run": True, **replay} if dry_run else replay
        if operation == "retire":
            if row is None or row.http_selection is None:
                raise _refusal("http_authority_required")
            configuration = principal_authority.stored_configuration(row)
            if row.version != body.expected_version:
                raise _refusal("stale_authority_version")
            if row.lifecycle_generation != body.expected_lifecycle_generation:
                raise _refusal("stale_lifecycle_generation")
            if not isinstance(configuration.selection, HttpSelectionV1):
                raise _refusal("http_authority_required")
            revoked = await principal_authority.revoke_authority_sessions(
                session,
                tenant_id=tenant_id,
                issuer=configuration.selection.issuer,
            )
            row.http_selection = {"mode": "disabled"}
            row.version += 1
            row.lifecycle_generation += 1
            identity_audit.record(
                session,
                tenant_id=tenant_id,
                event_type="identity.http_authority_retired",
                actor="platform",
                details={
                    "authority_version": row.version,
                    "lifecycle_generation": row.lifecycle_generation,
                    "revoked_session_count": revoked,
                },
            )
        else:
            assert isinstance(body, AuthoritySelectionMutation)
            write = (
                principal_authority.adopt_http_authority
                if operation == "adopt"
                else principal_authority.replace_http_configuration
            )
            await write(
                session,
                tenant_id=tenant_id,
                expected_version=body.expected_version,
                expected_lifecycle_generation=body.expected_lifecycle_generation,
                selection=body.selection,
            )
            row = await session.get(TenantIdentityProvider, tenant_id)
            assert row is not None
        result = _result(row)
        if dry_run:
            await session.rollback()
            return {"dry_run": True, **result}
        return await _record(session, tenant_id, body, digest, operation, result)
    except principal_authority.PrincipalAuthorityRefusal as error:
        raise _refusal(error.code) from None


@router.put("/authorities/{tenant_id}")
async def adopt_authority(
    tenant_id: uuid.UUID,
    body: AuthoritySelectionMutation,
    session: SessionDep,
    runtime: IdentityRuntimeDep,
    dry_run: bool = False,
) -> dict[str, Any]:
    return await _write_authority(tenant_id, body, session, "adopt", runtime, dry_run)


@router.post("/authorities/{tenant_id}/replace")
async def replace_authority(
    tenant_id: uuid.UUID,
    body: AuthoritySelectionMutation,
    session: SessionDep,
    runtime: IdentityRuntimeDep,
    dry_run: bool = False,
) -> dict[str, Any]:
    return await _write_authority(tenant_id, body, session, "replace", runtime, dry_run)


@router.post("/authorities/{tenant_id}/retire")
async def retire_authority(
    tenant_id: uuid.UUID,
    body: AuthorityMutation,
    session: SessionDep,
) -> dict[str, Any]:
    return await _write_authority(tenant_id, body, session, "retire", None)


@router.get("/roles/{tenant_id}/{agent_id}")
async def list_role_filters(
    tenant_id: uuid.UUID,
    agent_id: uuid.UUID,
    session: SessionDep,
) -> dict[str, Any]:
    try:
        await session.begin()
        policy = await agent_role_policy.read_role_policy(
            session,
            tenant_id=tenant_id,
            agent_id=agent_id,
        )
        if policy is not None:
            return policy.model_dump(mode="json")
        row = await session.get(AgentAccessPolicy, agent_id)
        return {
            "tenant_id": str(tenant_id),
            "agent_id": str(agent_id),
            "version": 0 if row is None else row.version,
            "roles": {},
        }
    except agent_role_policy.RolePolicyRefusal as error:
        raise _refusal(error.code) from None


async def _write_filter(
    tenant_id: uuid.UUID,
    agent_id: uuid.UUID,
    role: Literal["invocation", "approver"],
    body: OperatorMutation,
    session: AsyncSession,
) -> dict[str, Any]:
    try:
        _, authority = await principal_authority.lock_scope(session, tenant_id)
        if isinstance(body, GroupFilterMutation):
            if authority is None:
                raise _refusal("http_authority_required")
            configuration = principal_authority.stored_configuration(authority)
            if not isinstance(configuration.selection, HttpSelectionV1):
                raise _refusal("http_authority_required")
            if any(group.issuer != configuration.selection.issuer for group in body.groups):
                raise _refusal("authority_scope_mismatch")
        operation = "set_filter" if isinstance(body, GroupFilterMutation) else "remove_filter"
        digest = _digest(operation, f"{tenant_id}/{agent_id}/{role}", body)
        replay = await _replay(session, tenant_id, body, digest)
        if replay is not None:
            return replay
        role_filter = (
            None
            if not isinstance(body, GroupFilterMutation)
            else {
                "entries": [
                    {"kind": "issuer_group", "group": group.model_dump(mode="json")}
                    for group in body.groups
                ],
            }
        )
        change = SetRoleFilterV1.model_validate(
            {
                "schema_version": 1,
                "tenant_id": str(tenant_id),
                "agent_id": str(agent_id),
                "role": role,
                "operation": "set_filter",
                "expected_version": body.expected_version,
                "filter": role_filter,
            }
        )
        policy = await agent_role_policy.apply_role_change(session, change=change)
        return await _record(
            session,
            tenant_id,
            body,
            digest,
            operation,
            policy.model_dump(mode="json"),
        )
    except (
        principal_authority.PrincipalAuthorityRefusal,
        agent_role_policy.RolePolicyRefusal,
    ) as error:
        raise _refusal(error.code) from None


@router.put("/roles/{tenant_id}/{agent_id}/{role}")
async def set_group_filter(
    tenant_id: uuid.UUID,
    agent_id: uuid.UUID,
    role: Literal["invocation", "approver"],
    body: GroupFilterMutation,
    session: SessionDep,
) -> dict[str, Any]:
    return await _write_filter(tenant_id, agent_id, role, body, session)


@router.delete("/roles/{tenant_id}/{agent_id}/{role}")
async def remove_group_filter(
    tenant_id: uuid.UUID,
    agent_id: uuid.UUID,
    role: Literal["invocation", "approver"],
    body: OperatorMutation,
    session: SessionDep,
) -> dict[str, Any]:
    return await _write_filter(tenant_id, agent_id, role, body, session)
