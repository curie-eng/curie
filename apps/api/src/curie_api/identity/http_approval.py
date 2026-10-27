"""Creation continuity in existing approval owners. @spec HTTP-APPROVAL-CONTINUITY-1/3/4/5/9."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any, NoReturn

from aci_protocol import ApprovalRequest, QueuedTurn
from curie_internal import identity_approval as wire
from curie_internal import identity_context as codec
from curie_telemetry import TRACEPARENT_STREAM_FIELD, canonicalize_traceparent
from fastapi import HTTPException, Request
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api import identity_fence, principal_authority
from curie_api.crud import approvals as approvals_crud
from curie_api.crud import channels as channels_crud
from curie_api.models import Agent, Approval, TenantIdentityProvider
from curie_api.schemas.approvals import ApprovalResolutionTarget, ApprovalRouteBinding
from curie_api.schemas.channels import BUILTIN_CLUSTER_MESSAGE_ADAPTER
from curie_api.schemas.http_authority import HttpSelectionV1

from .http_invocation import (
    HttpInvocationRuntime,
    current_http_approval_caller,
    revalidate_http_approval_caller,
)

_RECORD_FIELDS = (
    "id",
    "agent_id",
    "conversation_id",
    "author",
    "summary",
    "reply_kind",
    "reply_channel",
    "reply_placeholder",
    "reply_endpoint",
    "reply_adapter",
    "reply_identity",
    "route",
    "card_channel",
    "dedupe_key",
    "status",
    "expires_at",
    "resolved_by",
    "resolution_note",
    "created_at",
    "resolved_at",
    "gate_kind",
    "granted_tool",
    "granted_arguments",
    "purpose",
    "policy_identity",
    "policy_version",
)


def _refuse(status: int, code: str) -> NoReturn:
    """@spec HTTP-APPROVAL-CONTINUITY-3."""
    raise HTTPException(status, code, headers={"Cache-Control": "no-store"})


def _canonical(value: Any) -> str:
    """@spec HTTP-APPROVAL-CONTINUITY-2/5."""

    def scalar(item: Any) -> str:
        """@spec HTTP-APPROVAL-CONTINUITY-2."""
        if isinstance(item, uuid.UUID):
            return str(item)
        if isinstance(item, datetime):
            moment: datetime = item
            if moment.tzinfo is None:
                moment = moment.replace(tzinfo=UTC)
            return moment.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
        raise ValueError("invalid_approval_record")

    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
        default=scalar,
    )


def decode_creation_json(data: bytes, *, max_bytes: int) -> Any:
    """Finite numbers, unique keys, strict UTF-8. @spec HTTP-APPROVAL-CONTINUITY-2."""

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        """@spec HTTP-APPROVAL-CONTINUITY-2."""
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    def nonfinite(_value: str) -> NoReturn:
        """@spec HTTP-APPROVAL-CONTINUITY-2."""
        raise ValueError("nonfinite number")

    if type(data) is not bytes or len(data) > max_bytes:
        raise ValueError("invalid JSON bytes")
    value = json.loads(data.decode("utf-8"), object_pairs_hook=pairs, parse_constant=nonfinite)
    # Escaped lone surrogates decode to text that is not UTF-8.
    json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
    return value


def _digest(value: str) -> str:
    """@spec HTTP-APPROVAL-CONTINUITY-5."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def approval_record_digest(approval: Approval) -> str:
    """@spec HTTP-APPROVAL-CONTINUITY-5."""
    return _digest(_canonical({name: getattr(approval, name) for name in _RECORD_FIELDS}))


async def _http_tenant(session: AsyncSession, tenant_id: uuid.UUID) -> bool:
    """SQL absence is distinct from malformed JSON. @spec HTTP-APPROVAL-CONTINUITY-1."""
    result = (
        await session.execute(
            select(
                TenantIdentityProvider,
                TenantIdentityProvider.http_selection.is_(None),
            ).where(TenantIdentityProvider.tenant_id == tenant_id)
        )
    ).one_or_none()
    if result is None:
        return False
    row, sql_null = result
    if not sql_null and row.http_selection is None:
        _refuse(503, "authority_unavailable")
    configuration = principal_authority.stored_configuration(row)
    return isinstance(configuration.selection, HttpSelectionV1)


async def require_legacy_approval_creation(session: AsyncSession, data: ApprovalRequest) -> None:
    """No provenance-free adopted creation. @spec HTTP-APPROVAL-CONTINUITY-1/5."""
    try:
        await identity_fence.lock_identity_reader(session)
        existing = await approvals_crud.get_approval_by_dedupe_key(session, data.dedupe_key)
        if existing is not None and existing.identity_context is not None:
            _refuse(409, "unsupported_approval")
        inputs: list[ApprovalRequest | Approval] = [data]
        if existing is not None:
            inputs.append(existing)
        for source in inputs:
            agent = None if source.agent_id is None else await session.get(Agent, source.agent_id)
            if agent is not None and await _http_tenant(session, agent.tenant_id):
                _refuse(409, "unsupported_approval")
            adapter = (
                source.reply_identity
                if source.reply_kind == "slack"
                and source.reply_adapter == BUILTIN_CLUSTER_MESSAGE_ADAPTER
                else source.reply_adapter
            )
            # Every candidate, so an ambiguous route cannot hide an adopted one. A tenant
            # without HTTP selection keeps agent/route disagreement and ambiguity.
            for binding in await channels_crud.bindings_for_route(
                session,
                source.reply_kind,
                adapter,
                source.reply_channel,
            ):
                if await _http_tenant(session, binding.tenant_id):
                    _refuse(409, "unsupported_approval")
    except HTTPException:
        raise
    except Exception:  # noqa: BLE001 - classification failure cannot select legacy.
        _refuse(503, "authority_unavailable")


def _original(
    request: Request,
    body: wire.ApprovalCreationRequestV1,
) -> tuple[HttpInvocationRuntime, codec.InvocationContextV1]:
    """Authenticate the exact queue bytes before lookup. @spec HTTP-APPROVAL-CONTINUITY-4."""
    runtime = getattr(request.app.state, "http_identity_runtime", None)
    if type(runtime) is not HttpInvocationRuntime:
        _refuse(503, "authority_unavailable")
    context = codec.open_invocation_context(body.identity.context, runtime._context_keys)
    if not isinstance(context, codec.InvocationContextV1):
        _refuse(422, "invalid_approval_context")
    data, scope, expected = body.approval, context.scope, body.identity.scope
    if (
        body.approval.agent_id is None
        or str(data.agent_id) != scope.agent_id
        or expected.tenant_id != scope.tenant_id
        or expected.agent_id != scope.agent_id
        or expected.binding_id != scope.binding_id
        or expected.binding_generation != scope.binding_generation
        or _digest(body.identity.payload) != context.payload_sha256
        or data.dedupe_key != context.event_id
    ):
        _refuse(422, "invalid_approval_context")
    try:
        # The receiver registration is current permission, checked by the caller phase;
        # an identical historical retry never reasserts it.
        decoded = decode_creation_json(body.identity.payload.encode("utf-8"), max_bytes=262_144)
        turn = QueuedTurn.model_validate(decoded)
        handle = turn.reply_handle
        if (
            handle is None
            or turn.event_id != context.event_id
            or turn.conversation_id != data.conversation_id
            or turn.author != data.author
            or handle.kind != data.reply_kind
            or handle.channel != data.reply_channel
            or handle.adapter != data.reply_adapter
            or handle.identity != data.reply_identity
            or handle.endpoint != data.reply_endpoint
            or handle.kind != "slack"
        ):
            raise ValueError
    except (ValueError, TypeError, HTTPException):
        _refuse(422, "invalid_approval_context")
    return runtime, context


def _effect(data: ApprovalRequest) -> None:
    """Explicit gate fields, never summary authority. @spec HTTP-APPROVAL-CONTINUITY-4."""
    if data.gate_kind not in {"permission", "policy"}:
        _refuse(409, "unsupported_approval")
    if data.gate_kind == "permission" and (not data.granted_tool or data.granted_arguments is None):
        _refuse(409, "unsupported_approval")
    try:
        if data.granted_tool is not None and not 1 <= len(data.granted_tool) <= 256:
            raise ValueError
        if (
            data.granted_arguments is not None
            and len(_canonical(data.granted_arguments).encode("utf-8")) > 16_384
        ):
            raise ValueError
    except (ValueError, TypeError, RecursionError):
        _refuse(409, "unsupported_approval")


async def _route_snapshot(
    session: AsyncSession, data: ApprovalRequest, *, expected: str | None = None,
) -> str:
    """Current route owner, independently captured. @spec HTTP-APPROVAL-CONTINUITY-4/9."""
    agent = await session.get(Agent, data.agent_id, populate_existing=True)
    if agent is None:
        _refuse(422, "invalid_approval_context")
    snapshot = _canonical(agent.approval_routes)
    if expected is not None and snapshot != expected:
        _refuse(503, "authority_unavailable")
    target = data.reply_channel
    if data.route:
        if not isinstance(agent.approval_routes, dict) or data.route not in agent.approval_routes:
            _refuse(409, "unsupported_approval")
        try:
            binding = ApprovalRouteBinding.model_validate(agent.approval_routes[data.route])
        except (TypeError, ValueError):
            _refuse(409, "unsupported_approval")
        if isinstance(binding.resolution, ApprovalResolutionTarget):
            target = binding.resolution.address
    if data.card_channel != target:
        _refuse(409, "unsupported_approval")
    return snapshot


def _response(
    body: wire.ApprovalCreationRequestV1,
    row: Approval,
    *,
    duplicate: bool,
) -> wire.ApprovalCreationResponseV1:
    """@spec HTTP-APPROVAL-CONTINUITY-3."""
    return wire.ApprovalCreationResponseV1.model_validate({
        "schema_version": 1, "request_id": body.request_id, "id": str(row.id),
        "status": row.status, "duplicate": duplicate, "requested_by": body.approval.author,
    })


def _historical(
    runtime: HttpInvocationRuntime,
    body: wire.ApprovalCreationRequestV1,
    original: codec.InvocationContextV1,
    row: Approval,
) -> wire.ApprovalCreationResponseV1:
    """A matching historical receipt is not permission. @spec HTTP-APPROVAL-CONTINUITY-5."""
    if row.identity_context is None:
        _refuse(409, "approval_provenance_conflict")
    saved = wire.open_approval_context(row.identity_context, runtime._context_keys)
    if not isinstance(saved, wire.ApprovalContextV1) or (
        saved.approval_id != str(row.id)
        or saved.status != row.status
        or saved.approval_record_sha256 != approval_record_digest(row)
        or saved.creation_request_sha256
        != _digest(_canonical(body.approval.model_dump(mode="json")))
        or saved.source_context_sha256 != _digest(body.identity.context)
        or saved.source_payload_sha256 != _digest(body.identity.payload)
        or saved.source_event_id != body.approval.dedupe_key
        or saved.conversation_id != body.approval.conversation_id
        or saved.scope != original.scope
        or saved.original_event_id != original.original_event_id
        or saved.canonical != original.canonical
        or saved.principal != original.principal
    ):
        _refuse(409, "approval_provenance_conflict")
    return _response(body, row, duplicate=True)


def _seal(
    runtime: HttpInvocationRuntime,
    body: wire.ApprovalCreationRequestV1,
    original: codec.InvocationContextV1,
    row: Approval,
) -> str:
    """Retain original association in the private row. @spec HTTP-APPROVAL-CONTINUITY-5."""
    values = {
        name: getattr(original, name)
        for name in wire.ApprovalContextV1.model_fields
        if hasattr(original, name)
    }
    values.update(
        {name: None for name in wire.ApprovalContextV1.model_fields if name.startswith("actor_")}
    )
    values.update(
        purpose="approval",
        key_id=runtime._signing_key[0],
        key_generation=runtime._signing_key[1],
        approval_id=str(row.id),
        source_event_id=original.event_id,
        conversation_id=body.approval.conversation_id,
        status="pending",
        creation_request_sha256=_digest(_canonical(body.approval.model_dump(mode="json"))),
        source_context_sha256=_digest(body.identity.context),
        source_payload_sha256=_digest(body.identity.payload),
        original_payload_sha256=original.payload_sha256,
        approval_record_sha256=approval_record_digest(row),
    )
    return wire.seal_approval_context(
        wire.ApprovalContextV1.model_validate(values), runtime._context_keys
    )


async def create_http_approval(
    request: Request,
    body: wire.ApprovalCreationRequestV1,
) -> wire.ApprovalCreationResponseV1:
    """Separate acknowledged lifecycle and creation commits. @spec HTTP-APPROVAL-CONTINUITY-9."""
    runtime, original = _original(request, body)
    factory, valkey = request.app.state.sessionmaker, request.app.state.valkey
    try:
        async with factory() as session, session.begin():
            await identity_fence.lock_identity_reader(session)
            existing = await approvals_crud.get_approval_by_dedupe_key(
                session, body.approval.dedupe_key
            )
            if existing is not None:
                return _historical(runtime, body, original, existing)
            _effect(body.approval)
            route_snapshot = await _route_snapshot(session, body.approval)
            if await approvals_crud.find_rejected_reraise(session, body.approval) is not None:
                _refuse(409, "unsupported_approval")
        async with current_http_approval_caller(request, original) as caller:
            if await _route_snapshot(
                caller.session, body.approval, expected=route_snapshot,
            ) != route_snapshot:
                _refuse(503, "authority_unavailable")
        # The read-only caller boundary is closed before acquiring the explicit write owner.
        async with identity_fence.mutation(factory, valkey) as writer:
            async with writer.begin():
                await revalidate_http_approval_caller(request, writer, caller)
                if await _route_snapshot(
                    writer, body.approval, expected=route_snapshot,
                ) != route_snapshot:
                    _refuse(503, "authority_unavailable")
                existing = await approvals_crud.get_approval_by_dedupe_key(
                    writer, body.approval.dedupe_key
                )
                if existing is not None:
                    result = _historical(runtime, body, original, existing)
                else:
                    if (
                        await approvals_crud.find_rejected_reraise(writer, body.approval)
                        is not None
                    ):
                        _refuse(409, "unsupported_approval")
                    row = await approvals_crud.insert_approval(
                        writer, body.approval, approval_id=uuid.uuid4(),
                        traceparent=canonicalize_traceparent(
                            request.headers.get(TRACEPARENT_STREAM_FIELD),
                        )
                    )
                    # Seal the row as stored: JSONB normalizes some numbers.
                    await writer.flush()
                    await writer.refresh(row)
                    row.identity_context = _seal(runtime, body, original, row)
                    await writer.flush()
                    result = _response(body, row, duplicate=False)
                    await writer.commit()
        return result
    except IntegrityError:
        try:
            async with factory() as session:
                winner = await approvals_crud.get_approval_by_dedupe_key(
                    session, body.approval.dedupe_key,
                )
                if winner is not None:
                    return _historical(runtime, body, original, winner)
        except HTTPException:
            raise
        except Exception:  # noqa: BLE001 - the race reread is unavailable, never definitive.
            _refuse(503, "authority_unavailable")
        # A constraint failure without a stored winner proves no conflict.
        _refuse(503, "authority_unavailable")
    except HTTPException as error:
        if error.status_code == 401:
            _refuse(422, "invalid_approval_context")
        if error.status_code == 403:
            _refuse(403, "caller_not_allowed")
        if error.status_code == 503:
            _refuse(503, "authority_unavailable")
        raise
    except Exception:  # noqa: BLE001 - private provider/DB details never cross the boundary.
        _refuse(503, "authority_unavailable")
