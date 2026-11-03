"""Resume current checks and exact grant scope. @spec HTTP-APPROVAL-CONTINUITY-12."""

from __future__ import annotations

import hashlib
import uuid
from typing import Literal

from curie_internal import identity_approval as wire
from curie_internal import identity_context as codec
from curie_internal.identity_authority import TurnAuthorityRequestV1, TurnAuthorityResponseV1
from fastapi import HTTPException, Request
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api import identity_fence
from curie_api.identity.http_approval import canonical_json, verified_capsule
from curie_api.identity.http_approval_answer import check_http_approval_actor
from curie_api.identity.http_invocation import (
    HttpInvocationRuntime,
    check_http_turn_authority,
    current_source_authority,
    invocation_source,
    turn_authority_response,
)
from curie_api.identity.http_transport import HttpIdentityClient
from curie_api.models import Approval

# Each capsule carries its own sealing key; every other continuity field must match.
_KEY_FIELDS = frozenset({"purpose", "key_id", "key_generation"})


def _original_invocation(resume: wire.ResumeContextV1) -> codec.InvocationContextV1:
    """The verified original invoker, never resealed or published.

    @spec HTTP-APPROVAL-CONTINUITY-8/12.
    """
    return codec.InvocationContextV1(
        schema_version=1, purpose="invocation", key_id=resume.key_id,
        key_generation=resume.key_generation, event_id=resume.original_event_id,
        original_event_id=resume.original_event_id, scope=resume.scope,
        receiver_id=resume.receiver_id, receiver_generation=resume.receiver_generation,
        independent_human=resume.independent_human, provider_issuer=resume.provider_issuer,
        provider_version=resume.provider_version,
        provider_credential_id=resume.provider_credential_id,
        provider_credential_generation=resume.provider_credential_generation,
        lifecycle_generation=resume.lifecycle_generation,
        role_policy_version=resume.role_policy_version, outcome=resume.outcome,
        canonical=resume.canonical, principal=resume.principal,
        payload_sha256=resume.original_payload_sha256,
    )


def _grant_matches(row: Approval, grant: wire.ApprovalGrantExpectationV1) -> bool:
    """Every effect field against the committed row. @spec HTTP-APPROVAL-CONTINUITY-12."""
    arguments = row.granted_arguments
    try:
        stored = None if arguments is None else canonical_json(arguments)
    except (ValueError, TypeError, RecursionError):
        return False
    if row.gate_kind == "permission" and (not row.granted_tool or arguments is None):
        return False
    return (
        row.status == "approved" and grant.approval_id == str(row.id)
        and row.agent_id is not None and grant.agent_id == str(row.agent_id)
        and grant.conversation_id == row.conversation_id and grant.gate_kind == row.gate_kind
        and grant.granted_tool == row.granted_tool
        and grant.granted_arguments_json == stored
    )


def _row_matches(
    runtime: HttpInvocationRuntime, row: Approval | None, resume: wire.ResumeContextV1,
    grant: wire.ApprovalGrantExpectationV1 | None,
) -> bool:
    """Current protected row, its seal and the resume capsule agree exactly.

    @spec HTTP-APPROVAL-CONTINUITY-5/6/12.
    """
    if row is None:
        return False
    saved = verified_capsule(runtime, row)
    if saved is None or (
        row.status != resume.status or row.agent_id is None
        or str(row.agent_id) != resume.scope.agent_id
        or row.conversation_id != resume.conversation_id
        or row.dedupe_key != resume.source_event_id
    ):
        return False
    for name in wire.ApprovalContinuityV1.model_fields:
        if name not in _KEY_FIELDS and getattr(saved, name) != getattr(resume, name):
            return False
    return grant is None or _grant_matches(row, grant)


async def _read_row(session: AsyncSession, approval_id: str) -> Approval | None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: a fresh read, never an identity-map copy."""
    return (
        await session.execute(
            select(Approval).where(Approval.id == uuid.UUID(approval_id))
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()


async def _check_resume(
    request: Request, check: TurnAuthorityRequestV1,
    grant: wire.ApprovalGrantExpectationV1 | None,
) -> TurnAuthorityResponseV1:
    """One current boundary for a selected resume. @spec HTTP-APPROVAL-CONTINUITY-8/12."""
    runtime = getattr(request.app.state, "http_identity_runtime", None)
    client = getattr(request.app.state, "http_identity_client", None)
    if type(runtime) is not HttpInvocationRuntime or not isinstance(client, HttpIdentityClient):
        return turn_authority_response(check, "unavailable", "authority_unavailable")
    resume = wire.open_resume_context(check.context, runtime._context_keys)
    if not isinstance(resume, wire.ResumeContextV1):
        return turn_authority_response(check, "refuse", "invalid_context")
    scope = resume.scope
    if (
        check.approval_id != resume.approval_id or check.event_id != resume.event_id
        or check.payload_sha256 != resume.payload_sha256
        or check.scope.tenant_id != scope.tenant_id or check.scope.agent_id != scope.agent_id
        or check.scope.binding_id != scope.binding_id
        or check.scope.binding_generation != scope.binding_generation
    ):
        return turn_authority_response(check, "refuse", "invalid_context")
    try:
        original = _original_invocation(resume)
        registration, source = invocation_source(runtime, original)
    except (ValueError, TypeError, HTTPException):
        return turn_authority_response(check, "refuse", "invalid_context")
    factory = request.app.state.sessionmaker
    try:
        async with factory() as session, session.begin():
            await identity_fence.lock_identity_reader(session)
            row = await _read_row(session, resume.approval_id)
            if not _row_matches(runtime, row, resume, grant):
                return turn_authority_response(check, "refuse", "invalid_context")
    except (SQLAlchemyError, RedisError, identity_fence.IdentityWriterUnavailable):
        return turn_authority_response(check, "unavailable", "authority_unavailable")
    if resume.status == "approved":
        # The preserved answering actor needs fresh approver authority of its own,
        # independently of the original invoker checked below. @spec HTTP-APPROVAL-CONTINUITY-8/12.
        try:
            await check_http_approval_actor(
                request, uuid.UUID(resume.approval_id),
                approver_sets=request.app.state.approver_sets,
            )
        except HTTPException as error:
            if error.status_code == 403 and error.detail == "caller_not_allowed":
                return turn_authority_response(check, "refuse", "caller_not_allowed")
            if error.status_code == 403:
                return turn_authority_response(check, "refuse", "unsupported_resume")
            return turn_authority_response(check, "unavailable", "authority_unavailable")
    reason: Literal["current_authority", "invalid_context"] = "invalid_context"
    try:
        async with current_source_authority(
            request, runtime=runtime, client=client, registration=registration,
            source=source, original=original,
        ) as current:
            # The final reader still holds the unchanged protected row.
            row = await _read_row(current.session, resume.approval_id)
            if _row_matches(runtime, row, resume, grant):
                reason = "current_authority"
    except HTTPException as error:
        if error.status_code == 401:
            return turn_authority_response(check, "refuse", "invalid_context")
        if error.status_code == 403:
            return turn_authority_response(check, "refuse", "caller_not_allowed")
        return turn_authority_response(check, "unavailable", "authority_unavailable")
    except (SQLAlchemyError, RedisError, identity_fence.IdentityWriterUnavailable):
        return turn_authority_response(check, "unavailable", "authority_unavailable")
    if reason == "current_authority":
        return turn_authority_response(check, "continue", "current_authority")
    return turn_authority_response(check, "refuse", "invalid_context")


async def check_current_turn_authority(
    request: Request, check: TurnAuthorityRequestV1,
) -> TurnAuthorityResponseV1:
    """Invocation contexts need approval_id=null; resume contexts name it exactly.

    @spec HTTP-INVOCATION-EXECUTION-16 HTTP-APPROVAL-CONTINUITY-12.
    """
    runtime = getattr(request.app.state, "http_identity_runtime", None)
    if check.approval_id is None or type(runtime) is not HttpInvocationRuntime or isinstance(
        codec.open_invocation_context(check.context, runtime._context_keys),
        codec.InvocationContextV1,
    ):
        return await check_http_turn_authority(request, check)
    return await _check_resume(request, check, None)


async def check_http_approval_grant(
    request: Request, body: wire.ApprovalGrantAuthorityRequestV1,
) -> wire.ApprovalGrantAuthorityResponseV1:
    """A continue applies only to this exact submitted snapshot and boundary.

    @spec HTTP-APPROVAL-CONTINUITY-12.
    """
    check = body.check
    runtime = getattr(request.app.state, "http_identity_runtime", None)
    if type(runtime) is HttpInvocationRuntime and isinstance(
        codec.open_invocation_context(check.context, runtime._context_keys),
        codec.InvocationContextV1,
    ):
        result = await check_http_turn_authority(request, check)
    else:
        result = await _check_resume(request, check, body.grant)
    return wire.ApprovalGrantAuthorityResponseV1(
        schema_version=1, check=result,
        grant_sha256=hashlib.sha256(
            canonical_json(body.grant.model_dump(mode="json")).encode("utf-8"),
        ).hexdigest(),
    )
