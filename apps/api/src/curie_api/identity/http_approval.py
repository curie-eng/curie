"""Creation continuity in existing approval owners. @spec HTTP-APPROVAL-CONTINUITY-1/3/4/5/6/9."""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, NoReturn

from aci_protocol import READER_CONTEXT, ApprovalRequest, QueuedTurn
from curie_internal import identity_approval as wire
from curie_internal import identity_context as codec
from curie_internal.approver_ticket import ApprovalTicket
from curie_telemetry import TRACEPARENT_STREAM_FIELD, canonicalize_traceparent
from fastapi import HTTPException, Request
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api import approver_dm, approver_tickets, deploy, identity_fence, principal_authority
from curie_api.crud import approvals as approvals_crud
from curie_api.crud import channels as channels_crud
from curie_api.models import (
    Agent,
    AgentChannel,
    Approval,
    ApprovalAuditEntry,
    ApprovalStatus,
    TenantIdentityProvider,
)
from curie_api.schemas.approvals import (
    ApprovalApproverDmTarget,
    ApprovalResolutionTarget,
    ApprovalRouteBinding,
)
from curie_api.schemas.channels import BUILTIN_CLUSTER_MESSAGE_ADAPTER
from curie_api.schemas.http_authority import HttpSelectionV1
from curie_api.schemas.http_identity import LookupRequestV1, ScopeV1, SenderV1, ValidatedFacts
from curie_api.storage import ObjectStore

from .delivery_validity import identity_delivery_window
from .http_credential_registry import ResolvedProviderCredential
from .http_invocation import (
    HttpInvocationRuntime,
    ReceiverRegistrationV1,
    binding_requires_http_identity,
    current_http_approval_caller,
    current_slack_scope,
    revalidate_http_approval_caller,
    source_facts,
)
from .http_transport import HttpIdentityClient, HttpIdentityObservation

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
# ADR 0271 decision 4 (plan D11): covered only on a dynamic row, so every
# ordinary record digest, and every pending seal over one, stays byte for byte.
_SELECTION_FIELDS = (
    "selection_mode",
    "selection_requester",
    "selection_connector",
    "selection_tool",
    "selection_key_id",
    "selection_nonce",
    "selection_arguments_sha256",
    "selected_approvers",
)


# Reply handle kind and sealed sender provider an approval may originate from.
# @spec HTTP-APPROVAL-CONTINUITY-4.
_ORIGINS = frozenset({("slack", "slack"), ("email", "email")})


def _refuse(status: int, code: str) -> NoReturn:
    """@spec HTTP-APPROVAL-CONTINUITY-3."""
    raise HTTPException(status, code, headers={"Cache-Control": "no-store"})


def canonical_json(value: Any) -> str:
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
    """@spec HTTP-APPROVAL-CONTINUITY-5 REQUEST-APPROVAL-4."""
    fields: tuple[str, ...] = _RECORD_FIELDS
    if approval.selection_mode == "dynamic":
        fields = _RECORD_FIELDS + _SELECTION_FIELDS
    return _digest(canonical_json({name: getattr(approval, name) for name in fields}))


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


async def _selected(
    session: AsyncSession, runtime: HttpInvocationRuntime | None,
    source: ApprovalRequest | Approval,
) -> bool:
    """Stored provenance, then every route candidate by binding.

    A candidate in an adopted tenant selects when it requires HTTP identity
    (the one per-binding owner), or when it is not the approval agent's own
    binding in the agent's tenant, which is every candidate of an agent-less
    approval. An agent in an adopted tenant is selected
    unless its route resolves to exactly one binding, its own there: only that
    shows its approver role is legacy-evaluable, and an ambiguous route is
    refused strictly. A tenant without HTTP selection keeps
    agent/route disagreement and ambiguity. @spec HTTP-APPROVAL-CONTINUITY-1/14.
    """
    if isinstance(source, Approval) and source.identity_context is not None:
        return True
    agent = None if source.agent_id is None else await session.get(Agent, source.agent_id)
    adapter = (
        source.reply_identity
        if source.reply_kind == "slack" and source.reply_adapter == BUILTIN_CLUSTER_MESSAGE_ADAPTER
        else source.reply_adapter
    )

    def own(binding: AgentChannel) -> bool:
        """@spec HTTP-APPROVAL-CONTINUITY-14."""
        return agent is not None and (binding.agent_id, binding.tenant_id) == (
            agent.id, agent.tenant_id,
        )

    # Every candidate, so an ambiguous route cannot hide an adopted one.
    candidates = await channels_crud.bindings_for_route(
        session, source.reply_kind, adapter, source.reply_channel,
    )
    for binding in candidates:
        if not await _http_tenant(session, binding.tenant_id):
            continue
        # Without an agent there is no approver role to read: any adopted candidate selects.
        if not own(binding) or await binding_requires_http_identity(
            session, runtime, binding,
        ):
            return True
    if agent is None or not await _http_tenant(session, agent.tenant_id):
        return False
    # Strict: an ambiguous route refuses even when no candidate needs HTTP facts.
    return len(candidates) != 1 or not own(candidates[0])


async def approval_is_selected(
    session: AsyncSession, approval: Approval, *, runtime: HttpInvocationRuntime | None,
) -> bool:
    """Classify a stored row before any legacy mutation or wake.

    Null provenance is not legacy authority; unavailable classification raises
    the fixed 503 rather than selecting legacy. Runtime None registers no
    identity. @spec HTTP-APPROVAL-CONTINUITY-1/10/14.
    """
    try:
        return await _selected(session, runtime, approval)
    except HTTPException:
        raise
    except Exception:  # noqa: BLE001 - classification failure cannot select legacy.
        _refuse(503, "authority_unavailable")


async def require_legacy_approval_creation(
    session: AsyncSession, data: ApprovalRequest, *, runtime: HttpInvocationRuntime | None,
) -> None:
    """No provenance-free selected creation. @spec HTTP-APPROVAL-CONTINUITY-1/5/14."""
    try:
        await identity_fence.lock_identity_reader(session)
        existing = await approvals_crud.get_approval_by_dedupe_key(session, data.dedupe_key)
        for source in (data,) if existing is None else (data, existing):
            if await _selected(session, runtime, source):
                _refuse(409, "unsupported_approval")
    except HTTPException:
        raise
    except Exception:  # noqa: BLE001 - classification failure cannot select legacy.
        _refuse(503, "authority_unavailable")


def identity_runtime(state: Any) -> HttpInvocationRuntime | None:
    """The app's HTTP invocation runtime for classification; absent registers nothing.

    create_app validates the stored value, and anything else fails the
    classification closed. @spec HTTP-APPROVAL-CONTINUITY-14.
    """
    runtime: HttpInvocationRuntime | None = getattr(state, "http_identity_runtime", None)
    return runtime


def verified_capsule(
    runtime: HttpInvocationRuntime, row: Approval,
) -> wire.ApprovalContextV1 | None:
    """The stored seal names exactly this row as it is. @spec HTTP-APPROVAL-CONTINUITY-5/10."""
    if row.identity_context is None:
        return None
    saved = wire.open_approval_context(row.identity_context, runtime._context_keys)
    if not isinstance(saved, wire.ApprovalContextV1) or (
        saved.approval_id != str(row.id)
        or saved.status != row.status
        or saved.approval_record_sha256 != approval_record_digest(row)
    ):
        return None
    return saved


async def expire_http_approval(
    factory: Any, valkey: Any, runtime: Any, approval_id: uuid.UUID,
) -> Approval | None:
    """Expire a lapsed selected row; status, seal and audit commit together.

    Expiry invents no actor and needs no caller lookup. A row whose stored seal
    does not name it exactly is left pending. @spec HTTP-APPROVAL-CONTINUITY-9/10.
    """
    if type(runtime) is not HttpInvocationRuntime:
        return None
    now = datetime.now(UTC).replace(tzinfo=None)

    def expirable(row: Approval | None) -> bool:
        """@spec HTTP-APPROVAL-CONTINUITY-10."""
        return (
            row is not None and row.status == ApprovalStatus.pending
            and row.expires_at is not None and row.expires_at <= now
            and verified_capsule(runtime, row) is not None
        )

    # A plain read first: the global identity writer is taken only for real work.
    async with factory() as session:
        if not expirable(await session.get(Approval, approval_id)):
            return None
    async with identity_fence.mutation(factory, valkey) as writer:
        async with writer.begin():
            row = (
                await writer.execute(
                    select(Approval)
                    .where(Approval.id == approval_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if row is None or not expirable(row):
                return None
            saved = verified_capsule(runtime, row)
            assert saved is not None
            row.status = ApprovalStatus.expired
            row.next_remind_at = None
            row.resolved_at = now
            await writer.flush()
            await writer.refresh(row)
            resealed = saved.model_dump()
            resealed.update(
                status="expired",
                key_id=runtime._signing_key[0],
                key_generation=runtime._signing_key[1],
                approval_record_sha256=approval_record_digest(row),
            )
            row.identity_context = wire.seal_approval_context(
                wire.ApprovalContextV1.model_validate(resealed), runtime._context_keys,
            )
            writer.add(ApprovalAuditEntry(
                approval_id=row.id, action="expired", actor="system", actor_channel=None,
                principal_kind=None, authenticated=False, principal_subject=None, decision="",
                authorizer="HttpApprovalExpiry", authorized=True,
                reason=f"approval expired at {row.expires_at}", evidence=None,
            ))
            await writer.flush()
            await writer.commit()
    return row


def http_expiry_owner(state: Any) -> Callable[[uuid.UUID], Awaitable[Approval | None]]:
    """The sealing expiry owner, bound to the app's stores. @spec HTTP-APPROVAL-CONTINUITY-10."""

    async def expire(approval_id: uuid.UUID) -> Approval | None:
        """@spec HTTP-APPROVAL-CONTINUITY-10."""
        return await expire_http_approval(
            state.sessionmaker, state.valkey,
            getattr(state, "http_identity_runtime", None), approval_id,
        )

    return expire


def original_invocation(resume: wire.ResumeContextV1) -> codec.InvocationContextV1:
    """The verified original invoker a resume carries, never resealed or published.

    @spec HTTP-APPROVAL-CONTINUITY-4/6/8.
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


# Each capsule carries its own sealing key; every other continuity field must match.
_KEY_FIELDS = frozenset({"purpose", "key_id", "key_generation"})


def resume_row_matches(
    runtime: HttpInvocationRuntime, row: Approval | None, resume: wire.ResumeContextV1,
) -> bool:
    """The named row, its stored seal and the resume capsule agree exactly.

    @spec HTTP-APPROVAL-CONTINUITY-4/5/6.
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
    return all(
        getattr(saved, name) == getattr(resume, name)
        for name in wire.ApprovalContinuityV1.model_fields
        if name not in _KEY_FIELDS
    )


async def _require_resume_row(
    session: AsyncSession, runtime: HttpInvocationRuntime, resume: wire.ResumeContextV1 | None,
) -> None:
    """A fresh read of the approval a resume names. @spec HTTP-APPROVAL-CONTINUITY-4."""
    if resume is None:
        return
    row = (
        await session.execute(
            select(Approval).where(Approval.id == uuid.UUID(resume.approval_id))
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if not resume_row_matches(runtime, row, resume):
        _refuse(422, "invalid_approval_context")


def _original(
    request: Request,
    body: wire.ApprovalCreationRequestV1,
) -> tuple[HttpInvocationRuntime, codec.InvocationContextV1, wire.ResumeContextV1 | None]:
    """Authenticate the exact queue bytes before lookup.

    A resumed turn suspends again with its resume context; the original invoker
    comes from that verified continuity, never from the answering actor.
    @spec HTTP-APPROVAL-CONTINUITY-4.
    """
    runtime = getattr(request.app.state, "http_identity_runtime", None)
    if type(runtime) is not HttpInvocationRuntime:
        _refuse(503, "authority_unavailable")
    token = body.identity.context
    resume: wire.ResumeContextV1 | None = None
    opened = codec.open_invocation_context(token, runtime._context_keys)
    if isinstance(opened, codec.InvocationContextV1):
        context, event_id, payload_sha256 = opened, opened.event_id, opened.payload_sha256
    else:
        candidate = wire.open_resume_context(token, runtime._context_keys)
        if not isinstance(candidate, wire.ResumeContextV1):
            _refuse(422, "invalid_approval_context")
        try:
            context = original_invocation(candidate)
        except (ValueError, TypeError):
            _refuse(422, "invalid_approval_context")
        resume, event_id, payload_sha256 = candidate, candidate.event_id, candidate.payload_sha256
    data, scope, expected = body.approval, context.scope, body.identity.scope
    if (
        body.approval.agent_id is None
        or str(data.agent_id) != scope.agent_id
        or expected.tenant_id != scope.tenant_id
        or expected.agent_id != scope.agent_id
        or expected.binding_id != scope.binding_id
        or expected.binding_generation != scope.binding_generation
        or _digest(body.identity.payload) != payload_sha256
        or data.dedupe_key != event_id
    ):
        _refuse(422, "invalid_approval_context")
    try:
        # The receiver registration is current permission, checked by the caller phase;
        # an identical historical retry never reasserts it.
        decoded = decode_creation_json(body.identity.payload.encode("utf-8"), max_bytes=262_144)
        turn = QueuedTurn.model_validate(decoded, context=READER_CONTEXT)
        handle = turn.reply_handle
        if (
            handle is None
            or turn.event_id != event_id
            or turn.conversation_id != data.conversation_id
            or turn.author != data.author
            or handle.kind != data.reply_kind
            or handle.channel != data.reply_channel
            or handle.adapter != data.reply_adapter
            or handle.identity != data.reply_identity
            or handle.endpoint != data.reply_endpoint
            # A Slack turn under a Slack context, or an email turn under an
            # email receiver's context (ADR 0267); nothing else.
            or (handle.kind, context.scope.provider) not in _ORIGINS
        ):
            raise ValueError
    except (ValueError, TypeError, HTTPException):
        _refuse(422, "invalid_approval_context")
    return runtime, context, resume


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
            and len(canonical_json(data.granted_arguments).encode("utf-8")) > 16_384
        ):
            raise ValueError
    except (ValueError, TypeError, RecursionError):
        _refuse(409, "unsupported_approval")


async def _route_snapshot(
    session: AsyncSession, data: ApprovalRequest, *, expected: str | None = None,
) -> str:
    """Current route owner, independently captured. @spec HTTP-APPROVAL-CONTINUITY-4/9.

    An approver-DM route (ADR 0272) needs its selector registration and a null
    ``card_channel``. @spec APPROVER-DM-1/2.
    """
    agent = await session.get(Agent, data.agent_id, populate_existing=True)
    if agent is None:
        _refuse(422, "invalid_approval_context")
    snapshot = canonical_json(agent.approval_routes)
    if expected is not None and snapshot != expected:
        _refuse(503, "authority_unavailable")
    target: str | None = data.reply_channel
    if data.route:
        if not isinstance(agent.approval_routes, dict) or data.route not in agent.approval_routes:
            _refuse(409, "unsupported_approval")
        try:
            binding = ApprovalRouteBinding.model_validate(agent.approval_routes[data.route])
        except (TypeError, ValueError):
            _refuse(409, "unsupported_approval")
        if isinstance(binding.resolution, ApprovalResolutionTarget):
            target = binding.resolution.address
        elif isinstance(binding.resolution, ApprovalApproverDmTarget):
            # ADR 0272: only a dynamic route has one selected person to deliver
            # to, and the worker never names the DM card's channel: the API
            # stores the one the provider returned at registration.
            if binding.approver_ticket is None:
                _refuse(409, APPROVER_DM_REQUIRES_DYNAMIC)
            target = None
    if data.card_channel != target:
        _refuse(409, "unsupported_approval")
    return snapshot


async def _selection_snapshot(
    session: AsyncSession, agent_id: uuid.UUID,
) -> tuple[str, list[tuple[str, str | None]]]:
    """The requester flag, its version and the serving versions, independently captured.

    With the route snapshot this is the dynamic creation snapshot (plan D9): a
    registration, key, flag, deployment or bundle attach landing between the
    phases changes it. @spec REQUEST-APPROVAL-2/4.
    """
    found = (
        await session.execute(
            select(Agent.connector_requester_identity, Agent.connector_requester_identity_version)
            .where(Agent.id == agent_id)
        )
    ).one_or_none()
    if found is None:
        _refuse(422, "invalid_approval_context")
    serving = await approver_tickets.serving_versions(session, agent_id)
    return canonical_json([bool(found[0]), int(found[1]), serving]), serving


def _ticket_refusal(error: approver_tickets.ApproverTicketRefusal) -> NoReturn:
    """@spec REQUEST-APPROVAL-2/4."""
    _refuse(error.status, error.code)


async def _serving_gate(
    request: Request,
    registration: approver_tickets.StoredRegistration,
    serving: list[tuple[str, str | None]],
) -> None:
    """Every serving bundle still gates the pair on its route (plan D9a).

    Outside every transaction and fence: the object store is external I/O.
    Fail closed: no serving bundle, a missing bundle object, a poison map or a
    moved gate refuses; an unavailable store answers unavailable.
    @spec REQUEST-APPROVAL-2.
    """
    connector, tool = registration.pair
    if not serving:
        _refuse(409, approver_tickets.BUNDLE_GATE_MISMATCH)
    store: ObjectStore = request.app.state.bundle_store
    for _, bundle_ref in serving:
        if bundle_ref is None:
            _refuse(409, approver_tickets.BUNDLE_GATE_MISMATCH)
        try:
            route_map = await deploy.stored_route_map(store, bundle_ref)
        except Exception:  # noqa: BLE001 - an unreadable store is never a gate verdict.
            _refuse(503, "authority_unavailable")
        if not approver_tickets.gates_pair(route_map, registration.route, connector, tool):
            _refuse(409, approver_tickets.BUNDLE_GATE_MISMATCH)


async def _admit_selection(
    writer: AsyncSession,
    registration: approver_tickets.StoredRegistration,
    ticket: ApprovalTicket,
    data: ApprovalRequest,
) -> approver_tickets.ApprovalSelection:
    """Final-reader checks inside the identity writer, before insert (plan D9).

    @spec REQUEST-APPROVAL-1/4.
    """
    agent = None if data.agent_id is None else await writer.get(Agent, data.agent_id)
    if agent is None:
        _refuse(422, "invalid_approval_context")
    try:
        await approver_tickets.check_selected_approvers(
            writer, registration, ticket, tenant_id=agent.tenant_id,
        )
    except approver_tickets.ApproverTicketRefusal as error:
        _ticket_refusal(error)
    if await approver_tickets.nonce_spent(writer, agent.id, ticket.nonce):
        _refuse(409, approver_tickets.REPLAYED)
    return approver_tickets.selection_for(registration, ticket)


def _created_audit(
    row: Approval, registration: approver_tickets.StoredRegistration, ticket: ApprovalTicket,
) -> ApprovalAuditEntry:
    """The created entry with its selection evidence; never the ticket text.

    @spec REQUEST-APPROVAL-4.
    """
    return ApprovalAuditEntry(
        approval_id=row.id, action="created", actor="system", actor_channel=None,
        principal_kind=None, authenticated=False, principal_subject=None, decision="",
        authorizer="ApproverTicketCreation", authorized=True,
        reason="dynamic approval created from a verified approver ticket",
        evidence=approver_tickets.creation_evidence(registration, ticket),
    )


CreationReceipt = wire.ApprovalCreationResponseV1 | wire.ApprovalCreationResponseV2

# --- ADR 0272: approver-DM creation -----------------------------------------------------

APPROVER_DM_REQUIRES_DYNAMIC = "approver_dm_requires_dynamic"
APPROVER_DM_RECIPIENT_COUNT = "approver_dm_recipient_count"
APPROVER_DM_ACCOUNT_MISSING = "approver_dm_account_missing"
APPROVER_DM_WRONG_INSTALLATION = "approver_dm_wrong_installation"
APPROVER_DM_ACCOUNT_UNRESOLVED = "approver_dm_account_unresolved"
APPROVER_DM_RECEIVER_MISSING = "approver_dm_receiver_missing"


def dm_receiver(
    runtime: HttpInvocationRuntime, tenant_id: str, identity: str,
) -> ReceiverRegistrationV1 | None:
    """The registered Slack receiver of one bot identity in one tenant, or None.

    Several generations of one identity's registration must agree on its
    identity row, installation and namespace; the newest is chosen, so the
    choice is deterministic. Disagreement is no receiver. @spec APPROVER-DM-1.
    """
    found = sorted(
        (
            registration for registration in runtime._receivers.values()
            if registration.tenant_id == tenant_id
            and registration.channel_identity_name == identity
        ),
        key=lambda registration: (registration.generation, registration.receiver_id),
    )
    places = {
        (
            registration.channel_identity_id, registration.provider_installation_id,
            canonical_json(registration.namespace.model_dump(mode="json")),
        )
        for registration in found
    }
    return found[-1] if len(places) == 1 else None


def dm_serves_origin(
    registration: ReceiverRegistrationV1, provider: str, binding_id: str,
) -> bool:
    """Whether a DM card delivered by ``registration`` can be answered for its origin.

    The same rule the answer applies (``http_approval_answer._serves_origin``):
    a Slack origin's binding must be one the receiver explicitly serves. No
    Slack registration lists an email binding, so an email origin is bound to
    its stored card instead and needs no listing here. @spec APPROVER-DM-1.
    """
    return provider == "email" or binding_id in registration.binding_ids


def dm_account(
    registration: ReceiverRegistrationV1, accounts: Any,
) -> dict[str, Any] | None:
    """The one recipient account on the receiver's installation and namespace.

    ``accounts`` is a selected approver's ``recipient_accounts`` as stored on
    the row (plain JSON). @spec APPROVER-DM-1.
    """
    namespace = registration.namespace.model_dump(mode="json")
    matches = [
        account for account in (accounts if isinstance(accounts, list) else ())
        if isinstance(account, dict)
        and account.get("provider") == "slack"
        and account.get("installation_id") == registration.provider_installation_id
        and account.get("namespace") == namespace
        and isinstance(account.get("account_id"), str)
    ]
    return matches[0] if len(matches) == 1 else None


def _delivery(
    identity: str, installation_id: str, recipient: str,
) -> wire.ApprovalDmDeliveryV1:
    """@spec APPROVER-DM-2."""
    return wire.ApprovalDmDeliveryV1.model_validate({
        "mode": "approver_dm", "identity": identity,
        "provider_installation_id": installation_id, "recipient": recipient,
    })


@dataclass(frozen=True)
class _DmCapture:
    """Phase one's recipient capture, rechecked unchanged in the writer (plan R2).

    @spec APPROVER-DM-1.
    """

    identity: str
    tenant_id: uuid.UUID
    registration: ReceiverRegistrationV1 = field(repr=False)
    configuration_version: int
    credential_ref: tuple[str, int]
    issuer: str
    credential: ResolvedProviderCredential = field(repr=False)
    approver: tuple[str, str]
    account: dict[str, Any]
    origin: tuple[str, str]

    @property
    def delivery(self) -> wire.ApprovalDmDeliveryV1:
        """@spec APPROVER-DM-2."""
        return _delivery(
            self.identity, self.registration.provider_installation_id,
            str(self.account["account_id"]),
        )


@dataclass(frozen=True)
class _DmRecipient:
    """The recipient lookup made outside every transaction (plan R2/R2b).

    @spec APPROVER-DM-1.
    """

    lookup: LookupRequestV1 = field(repr=False)
    response_bytes: bytes = field(repr=False)


def _dm_route_identity(agent: Agent | None, route: str | None) -> str | None:
    """The identity of ``route``'s approver-DM resolution on ``agent``, if any.

    @spec APPROVER-DM-1.
    """
    routes = None if agent is None else agent.approval_routes
    if not route or not isinstance(routes, dict):
        return None
    return approver_dm.dm_route(routes.get(route))


async def _route_recipient(
    session: AsyncSession, runtime: HttpInvocationRuntime, row: Approval,
) -> tuple[Agent, str, ReceiverRegistrationV1, dict[str, Any]] | None:
    """A dynamic row's current DM route identity, that identity's receiver and
    the one selected approver's account on it, or None when any is missing.

    @spec APPROVER-DM-2.
    """
    agent = None if row.agent_id is None else await session.get(
        Agent, row.agent_id, populate_existing=True,
    )
    identity = _dm_route_identity(agent, row.route)
    selected = row.selected_approvers
    if (
        agent is None or row.selection_mode != "dynamic" or identity is None
        or not isinstance(selected, list) or len(selected) != 1
        or not isinstance(selected[0], dict)
    ):
        return None
    registration = dm_receiver(runtime, str(agent.tenant_id), identity)
    account = None if registration is None else dm_account(
        registration, selected[0].get("recipient_accounts"),
    )
    if registration is None or account is None:
        return None
    return agent, identity, registration, account


async def _provider_configuration(
    session: AsyncSession, tenant_id: uuid.UUID,
) -> tuple[int, tuple[str, int], str, Any]:
    """The tenant's current HTTP provider configuration, or unavailable.

    @spec APPROVER-DM-1.
    """
    row = await session.scalar(
        select(TenantIdentityProvider)
        .where(TenantIdentityProvider.tenant_id == tenant_id)
        .execution_options(populate_existing=True)
    )
    if row is None or row.http_selection is None:
        _refuse(503, "authority_unavailable")
    configuration = principal_authority.stored_configuration(row)
    selection = configuration.selection
    if not isinstance(selection, HttpSelectionV1):
        _refuse(503, "authority_unavailable")
    reference = (selection.credential_ref.id, selection.credential_ref.generation)
    return configuration.version, reference, selection.issuer, configuration


async def _capture_dm(
    session: AsyncSession, runtime: HttpInvocationRuntime, agent: Agent, identity: str,
    ticket: ApprovalTicket, origin: tuple[str, str],
) -> _DmCapture:
    """Phase one: one selected person, a current receiver serving the origin
    (``origin`` is the turn's provider and binding id), one account on it.

    @spec APPROVER-DM-1.
    """
    if len(ticket.approvers) != 1:
        _refuse(422, APPROVER_DM_RECIPIENT_COUNT)
    tenant = agent.tenant_id
    registration = dm_receiver(runtime, str(tenant), identity)
    if registration is None or not dm_serves_origin(registration, *origin):
        _refuse(409, APPROVER_DM_RECEIVER_MISSING)
    try:
        await current_slack_scope(session, registration, tenant)
    except HTTPException:
        _refuse(409, APPROVER_DM_RECEIVER_MISSING)
    approver = ticket.approvers[0]
    accounts = [
        account.model_dump(mode="json") for account in approver.recipient_accounts or ()
    ]
    if not accounts:
        _refuse(422, APPROVER_DM_ACCOUNT_MISSING)
    account = dm_account(registration, accounts)
    if account is None:
        _refuse(422, APPROVER_DM_WRONG_INSTALLATION)
    version, reference, issuer, configuration = await _provider_configuration(session, tenant)
    credential = runtime._providers.resolve(configuration)
    if not isinstance(credential, ResolvedProviderCredential):
        _refuse(503, "authority_unavailable")
    return _DmCapture(
        identity, tenant, registration, version, reference, issuer, credential,
        (approver.issuer, approver.subject), account, origin,
    )


def _recipient_facts(capture: _DmCapture, recipient: _DmRecipient) -> ValidatedFacts:
    """The recipient is the selected person, an active human, inside the delivery window.

    Valid facts naming anyone else, or no one, refuse
    ``approver_dm_account_unresolved``; invalid, expired or unavailable facts
    are unavailable. @spec APPROVER-DM-1.
    """
    try:
        facts = source_facts(recipient.response_bytes, recipient.lookup, capture.credential)
    except HTTPException as error:
        if error.status_code == 403:
            _refuse(422, APPROVER_DM_ACCOUNT_UNRESOLVED)
        _refuse(503, "authority_unavailable")
    identity = facts.identity
    if (
        facts.outcome != "resolved" or identity is None or identity.kind != "human"
        or facts.sender_kind != "human" or identity.account_status != "active"
        or (identity.issuer, identity.subject) != capture.approver
    ):
        _refuse(422, APPROVER_DM_ACCOUNT_UNRESOLVED)
    try:
        window = identity_delivery_window(facts, capture.credential.limits)
    except (ValueError, TypeError):
        _refuse(503, "authority_unavailable")
    now = datetime.now(UTC)
    instant = (int(now.timestamp()), now.microsecond)
    lower = (window.lower_seconds, window.lower_microseconds)
    if not lower <= instant <= (window.upper_seconds, window.upper_microseconds):
        _refuse(503, "authority_unavailable")
    return facts


async def _lookup_recipient(request: Request, capture: _DmCapture) -> _DmRecipient:
    """Look the recipient account up through the HTTP identity owner, outside every
    transaction, with purpose ``approval``. The result is never applied, so no
    lifecycle generation moves. @spec APPROVER-DM-1.
    """
    client = getattr(request.app.state, "http_identity_client", None)
    if not isinstance(client, HttpIdentityClient):
        _refuse(503, "authority_unavailable")
    registration = capture.registration
    credential = capture.credential
    lookup = LookupRequestV1(
        schema_version=1, request_id=str(uuid.uuid4()), purpose="approval",
        authority_version=capture.configuration_version,
        scope=ScopeV1(
            tenant_id=registration.tenant_id, expected_issuer=credential.target.expected_issuer,
            sender=SenderV1(
                provider="slack", channel_identity_id=registration.channel_identity_id,
                provider_installation_id=registration.provider_installation_id,
                namespace=registration.namespace, native_id=str(capture.account["account_id"]),
            ),
        ),
    )
    observation = await client.lookup(
        target=credential.target, bearer=credential.bearer, request=lookup,
        limits=credential.limits,
    )
    if not isinstance(observation, HttpIdentityObservation):
        _refuse(503, "authority_unavailable")
    recipient = _DmRecipient(lookup, observation.response_bytes)
    _recipient_facts(capture, recipient)
    return recipient


async def _recheck_dm(
    request: Request, writer: AsyncSession, runtime: HttpInvocationRuntime,
    capture: _DmCapture, recipient: _DmRecipient,
    selection: approver_tickets.ApprovalSelection | None,
) -> None:
    """The writer's recheck of the phase-one capture (plan R2, R2b).

    The same registration object is still registered for the identity, its
    rows are current, the provider configuration and credential are the
    captured ones, the stored selection still names the recipient, and the
    recipient facts are still valid and inside their delivery window now. The
    tenant lifecycle generation is never compared: the caller phase's own
    lookup moves it. Any change is unavailable. @spec APPROVER-DM-1.
    """
    # ``dm_receiver`` picks from ``runtime._receivers``: the same pick proves it is registered.
    if (
        request.app.state.http_identity_runtime is not runtime
        or dm_receiver(runtime, str(capture.tenant_id), capture.identity)
        is not capture.registration
    ):
        _refuse(503, "authority_unavailable")
    if not dm_serves_origin(capture.registration, *capture.origin):
        _refuse(503, "authority_unavailable")
    try:
        await current_slack_scope(writer, capture.registration, capture.tenant_id)
    except HTTPException:
        _refuse(503, "authority_unavailable")
    version, reference, issuer, _ = await _provider_configuration(writer, capture.tenant_id)
    if (version, reference, issuer) != (
        capture.configuration_version, capture.credential_ref, capture.issuer,
    ):
        _refuse(503, "authority_unavailable")
    if selection is None or len(selection.approvers) != 1:
        _refuse(503, "authority_unavailable")
    chosen = selection.approvers[0]
    if (
        (chosen.get("issuer"), chosen.get("subject")) != capture.approver
        or dm_account(capture.registration, chosen.get("recipient_accounts")) != capture.account
    ):
        _refuse(503, "authority_unavailable")
    try:
        _recipient_facts(capture, recipient)
    except HTTPException:
        _refuse(503, "authority_unavailable")


async def _stored_delivery(
    session: AsyncSession, runtime: HttpInvocationRuntime, row: Approval,
) -> wire.ApprovalDmDeliveryV1 | None:
    """A duplicate's delivery, recomputed from the stored row (ADR 0272).

    A registered DM card answers from its registration record; a dynamic row
    whose card is still unset answers from its route's DM identity, that
    identity's receiver and the one selected recipient. Every other row
    carries none. @spec APPROVER-DM-2.
    """
    if row.selection_mode != "dynamic":
        return None
    card = await approver_dm.card(session, row)
    if card is not None:
        return _delivery(card.identity, card.provider_installation_id, card.recipient)
    found = None if row.card_channel is not None else await _route_recipient(
        session, runtime, row,
    )
    if found is None:
        return None
    _, identity, registration, account = found
    return _delivery(identity, registration.provider_installation_id, str(account["account_id"]))


def _response(
    body: wire.ApprovalCreationRequestV1,
    row: Approval,
    *,
    duplicate: bool,
    delivery: wire.ApprovalDmDeliveryV1 | None = None,
) -> CreationReceipt:
    """V2 for a dynamic row, from the stored row; an ordinary row keeps exact V1.

    A dynamic row's receipt carries what was persisted, the ticket included,
    and its selected people projected to tenant, issuer and subject, plus an
    approver-DM row's delivery when one is given. A stored value the V2 model
    refuses fails the whole creation, never a silent V1.
    @spec HTTP-APPROVAL-CONTINUITY-3 REQUEST-APPROVAL-4 APPROVER-DM-2.
    """
    values: dict[str, Any] = {
        "schema_version": 1, "request_id": body.request_id, "id": str(row.id),
        "status": row.status, "duplicate": duplicate, "requested_by": body.approval.author,
    }
    if row.selection_mode != "dynamic":
        return wire.ApprovalCreationResponseV1.model_validate(values)
    if delivery is not None:
        values["delivery"] = delivery
    return wire.ApprovalCreationResponseV2.model_validate({
        **values,
        "schema_version": 2,
        "granted_tool": row.granted_tool,
        "granted_arguments": canonical_json(row.granted_arguments),
        "selected_approvers": [
            {name: person[name] for name in ("tenant_id", "issuer", "subject")}
            for person in row.selected_approvers or ()
        ],
    })


async def _historical(
    session: AsyncSession,
    runtime: HttpInvocationRuntime,
    body: wire.ApprovalCreationRequestV1,
    original: codec.InvocationContextV1,
    row: Approval,
) -> CreationReceipt:
    """A matching historical receipt is not permission. @spec HTTP-APPROVAL-CONTINUITY-5.

    An approver-DM row's receipt carries its delivery, recomputed from the
    stored row (@spec APPROVER-DM-2).
    """
    if row.identity_context is None:
        _refuse(409, "approval_provenance_conflict")
    saved = wire.open_approval_context(row.identity_context, runtime._context_keys)
    if not isinstance(saved, wire.ApprovalContextV1) or (
        saved.approval_id != str(row.id)
        or saved.status != row.status
        or saved.approval_record_sha256 != approval_record_digest(row)
        or saved.creation_request_sha256
        != _digest(canonical_json(body.approval.model_dump(mode="json")))
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
    return _response(
        body, row, duplicate=True, delivery=await _stored_delivery(session, runtime, row),
    )


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
        # The incoming invocation or resume event; original fields stay the root's.
        source_event_id=body.approval.dedupe_key,
        conversation_id=body.approval.conversation_id,
        status="pending",
        creation_request_sha256=_digest(canonical_json(body.approval.model_dump(mode="json"))),
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
) -> CreationReceipt:
    """Separate acknowledged lifecycle and creation commits. @spec HTTP-APPROVAL-CONTINUITY-9.

    ADR 0271 (@spec REQUEST-APPROVAL-2/4): the route lock runs before every
    other creation check, so a registered connector tool never takes a looser
    route whatever ``gate_kind`` says. A dynamic request is verified once in
    phase one, its serving bundles are rechecked outside every transaction,
    and its selected people, nonce, row, seal and created audit entry are
    judged and committed together in the identity writer against an unchanged
    snapshot. An identical retry stays historical before any of it. A dynamic
    request also needs the original requester, freshly resolved, active and
    unchanged, judged in the caller phase and again in the writer, never the
    raw-account exception of ordinary admission.

    ADR 0272 (@spec APPROVER-DM-1/2): on an approver-DM route phase one also
    captures the one selected recipient, its receiver and the provider
    configuration; the recipient is looked up outside every transaction; the
    writer rechecks that capture and the facts' freshness before insert. The
    response then carries the server-verified ``delivery``.
    """
    runtime, original, resume = _original(request, body)
    factory, valkey = request.app.state.sessionmaker, request.app.state.valkey
    data = body.approval
    registration: approver_tickets.StoredRegistration | None = None
    ticket: ApprovalTicket | None = None
    selection_snapshot: str | None = None
    serving: list[tuple[str, str | None]] = []
    dm: _DmCapture | None = None
    try:
        async with factory() as session, session.begin():
            await identity_fence.lock_identity_reader(session)
            existing = await approvals_crud.get_approval_by_dedupe_key(
                session, data.dedupe_key
            )
            if existing is not None:
                return await _historical(session, runtime, body, original, existing)
            await _require_resume_row(session, runtime, resume)
            agent = await session.get(Agent, data.agent_id, populate_existing=True)
            if agent is None:
                _refuse(422, "invalid_approval_context")
            agent_id = agent.id
            try:
                registration = approver_tickets.check_tool_route(agent, data)
            except approver_tickets.ApproverTicketRefusal as error:
                _ticket_refusal(error)
            _effect(data)
            route_snapshot = await _route_snapshot(session, data)
            if await approvals_crud.find_rejected_reraise(session, data) is not None:
                _refuse(409, "unsupported_approval")
            if registration is not None:
                try:
                    ticket = approver_tickets.verify_creation(
                        registration, agent=agent, data=data, original=original,
                        clock=lambda: int(time.time()),
                    )
                except approver_tickets.ApproverTicketRefusal as error:
                    _ticket_refusal(error)
                selection_snapshot, serving = await _selection_snapshot(session, agent_id)
            identity = _dm_route_identity(agent, data.route)
            if identity is not None:
                # ADR 0272 decision 1: one selected person, one current receiver
                # and one recipient account on its installation and namespace.
                if registration is None or ticket is None:
                    _refuse(409, APPROVER_DM_REQUIRES_DYNAMIC)
                dm = await _capture_dm(
                    session, runtime, agent, identity, ticket,
                    (original.scope.provider, original.scope.binding_id),
                )
        if registration is not None:
            await _serving_gate(request, registration, serving)
        recipient = None if dm is None else await _lookup_recipient(request, dm)

        async def unchanged(session: AsyncSession) -> None:
            """The route and selection snapshots are exactly the phase-one capture."""
            if await _route_snapshot(session, data, expected=route_snapshot) != route_snapshot:
                _refuse(503, "authority_unavailable")
            if selection_snapshot is not None and (
                await _selection_snapshot(session, agent_id)
            )[0] != selection_snapshot:
                _refuse(503, "authority_unavailable")

        async with current_http_approval_caller(request, original) as caller:
            if registration is not None:
                # Ordinary admission's raw-account exception is not enough here.
                caller.require_original_requester()
            await unchanged(caller.session)
        # The read-only caller boundary is closed before acquiring the explicit write owner.
        async with identity_fence.mutation(factory, valkey) as writer:
            async with writer.begin():
                await revalidate_http_approval_caller(
                    request, writer, caller, original_requester=registration is not None,
                )
                # Held to the commit: a registration cannot land between this
                # judgement and the insert (@spec REQUEST-APPROVAL-2).
                try:
                    locked = await approver_tickets.locked_tool_route(writer, data)
                except approver_tickets.ApproverTicketRefusal as error:
                    _ticket_refusal(error)
                if locked != registration:
                    _refuse(503, "authority_unavailable")
                await unchanged(writer)
                existing = await approvals_crud.get_approval_by_dedupe_key(
                    writer, data.dedupe_key
                )
                if existing is not None:
                    result = await _historical(writer, runtime, body, original, existing)
                else:
                    await _require_resume_row(writer, runtime, resume)
                    if (
                        await approvals_crud.find_rejected_reraise(writer, data)
                        is not None
                    ):
                        _refuse(409, "unsupported_approval")
                    selection = None
                    if registration is not None and ticket is not None:
                        selection = await _admit_selection(writer, registration, ticket, data)
                    if dm is not None:
                        assert recipient is not None
                        await _recheck_dm(request, writer, runtime, dm, recipient, selection)
                    row = await approvals_crud.insert_approval(
                        writer, data, approval_id=uuid.uuid4(),
                        traceparent=canonicalize_traceparent(
                            request.headers.get(TRACEPARENT_STREAM_FIELD),
                        ),
                        selection=selection,
                    )
                    # Seal the row as stored: JSONB normalizes some numbers.
                    await writer.flush()
                    await writer.refresh(row)
                    if selection is not None and not approver_tickets.arguments_stable(
                        row.granted_arguments, data.granted_arguments,
                    ):
                        # The signed arguments would not be the ones a grant is
                        # minted from; the writer rolls back on the way out.
                        _refuse(422, approver_tickets.ARGUMENTS_UNSTABLE)
                    row.identity_context = _seal(runtime, body, original, row)
                    if registration is not None and ticket is not None:
                        writer.add(_created_audit(row, registration, ticket))
                    await writer.flush()
                    result = _response(
                        body, row, duplicate=False,
                        delivery=None if dm is None else dm.delivery,
                    )
                    await writer.commit()
        return result
    except IntegrityError:
        try:
            async with factory() as session:
                winner = await approvals_crud.get_approval_by_dedupe_key(
                    session, data.dedupe_key,
                )
                if winner is not None:
                    return await _historical(session, runtime, body, original, winner)
                if ticket is not None and (
                    await approver_tickets.nonce_spent(session, agent_id, ticket.nonce)
                ):
                    _refuse(409, approver_tickets.REPLAYED)
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


# --- ADR 0272: approver-DM card registration ---------------------------------------------

APPROVAL_CARD_MISMATCH = "approval_card_mismatch"
APPROVAL_CARD_CONFLICT = "approval_card_conflict"


def _card_response(
    body: wire.ApprovalCardRegistrationRequestV1, row: Approval, *, duplicate: bool,
) -> wire.ApprovalCardRegistrationResponseV1:
    """@spec APPROVER-DM-2."""
    return wire.ApprovalCardRegistrationResponseV1.model_validate({
        "schema_version": 1, "request_id": body.request_id, "approval_id": str(row.id),
        "status": row.status, "card_channel": row.card_channel, "duplicate": duplicate,
    })


def _registration_association(
    runtime: HttpInvocationRuntime, body: wire.ApprovalCardRegistrationRequestV1,
    approval_id: uuid.UUID, row: Approval | None,
) -> None:
    """The body names this approval and its original creation context exactly.

    The sealed context's source digests must be those of the identity block's
    context and payload, and its scope that block's scope.
    @spec APPROVER-DM-2.
    """
    if row is None or body.approval_id != str(approval_id):
        _refuse(422, "invalid_approval_context")
    if row.dedupe_key != body.dedupe_key or row.identity_context is None:
        _refuse(422, "invalid_approval_context")
    saved = wire.open_approval_context(row.identity_context, runtime._context_keys)
    expected = body.identity.scope
    if not isinstance(saved, wire.ApprovalContextV1) or (
        saved.approval_id != str(row.id)
        or saved.source_context_sha256 != _digest(body.identity.context)
        or saved.source_payload_sha256 != _digest(body.identity.payload)
        or saved.scope.tenant_id != expected.tenant_id
        or saved.scope.agent_id != expected.agent_id
        or saved.scope.binding_id != expected.binding_id
        or saved.scope.binding_generation != expected.binding_generation
    ):
        _refuse(422, "invalid_approval_context")


async def _first_registration(
    writer: AsyncSession, runtime: HttpInvocationRuntime, row: Approval,
    body: wire.ApprovalCardRegistrationRequestV1, saved: wire.ApprovalContextV1,
) -> ReceiverRegistrationV1:
    """The current route still delivers this row to the named bot and recipient,
    through a receiver that serves the row's origin (``saved`` is its seal).

    Consulted only before any registration commits (plan R4).
    @spec APPROVER-DM-2.
    """
    found = await _route_recipient(writer, runtime, row)
    if found is None:
        _refuse(409, APPROVAL_CARD_MISMATCH)
    agent, identity, registration, account = found
    if (
        identity != body.card_identity or account["account_id"] != body.recipient
        or not dm_serves_origin(registration, saved.scope.provider, saved.scope.binding_id)
    ):
        _refuse(409, APPROVAL_CARD_MISMATCH)
    try:
        await current_slack_scope(writer, registration, agent.tenant_id)
    except HTTPException:
        _refuse(409, APPROVAL_CARD_MISMATCH)
    return registration


async def register_approval_card(
    request: Request, approval_id: uuid.UUID, body: wire.ApprovalCardRegistrationRequestV1,
) -> wire.ApprovalCardRegistrationResponseV1:
    """Store the provider-returned DM channel of one approver-DM card, once.

    Under the identity writer: authenticate the original creation association;
    judge a retry against the committed registration record, never the
    current route (plan R4); return a terminal row's status without writing;
    verify the stored seal still names the row before any mutation (plan R3);
    then, for a first registration only, require the current route's DM
    identity and the one selected recipient on its receiver. The channel, the
    resealed continuity context (only ``approval_record_sha256`` changes, so
    the creation fingerprint and requester association survive) and one
    ``card_registered`` audit row commit together. @spec APPROVER-DM-2.
    """
    runtime = getattr(request.app.state, "http_identity_runtime", None)
    if type(runtime) is not HttpInvocationRuntime:
        _refuse(503, "authority_unavailable")
    factory, valkey = request.app.state.sessionmaker, request.app.state.valkey
    try:
        async with identity_fence.mutation(factory, valkey) as writer:
            async with writer.begin():
                row = (
                    await writer.execute(
                        select(Approval)
                        .where(Approval.id == approval_id)
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one_or_none()
                _registration_association(runtime, body, approval_id, row)
                assert row is not None
                committed = await approver_dm.card(writer, row)
                if committed is not None:
                    if (
                        committed.channel == body.channel == row.card_channel
                        and committed.identity == body.card_identity
                        and committed.recipient == body.recipient
                    ):
                        return _card_response(body, row, duplicate=True)
                    _refuse(409, APPROVAL_CARD_CONFLICT)
                if row.status != ApprovalStatus.pending:
                    return _card_response(body, row, duplicate=False)
                saved = verified_capsule(runtime, row)
                if saved is None:
                    _refuse(409, "approval_provenance_conflict")
                if row.card_channel is not None:
                    _refuse(409, APPROVAL_CARD_MISMATCH)
                registration = await _first_registration(writer, runtime, row, body, saved)
                row.card_channel = body.channel
                await writer.flush()
                await writer.refresh(row)
                resealed = saved.model_dump()
                resealed["approval_record_sha256"] = approval_record_digest(row)
                row.identity_context = wire.seal_approval_context(
                    wire.ApprovalContextV1.model_validate(resealed), runtime._context_keys,
                )
                writer.add(ApprovalAuditEntry(
                    approval_id=row.id, action=approver_dm.CARD_REGISTERED, actor="system",
                    actor_channel=None, principal_kind=None, authenticated=False,
                    principal_subject=None, decision="", authorizer="ApproverDmRegistration",
                    authorized=True, reason="approver DM card channel registered",
                    evidence={
                        "kind": approver_dm.EVIDENCE_KIND, "identity": body.card_identity,
                        "provider_installation_id": registration.provider_installation_id,
                        "namespace_id": registration.namespace.id,
                        "recipient": body.recipient, "channel": body.channel,
                    },
                ))
                await writer.flush()
                result = _card_response(body, row, duplicate=False)
                await writer.commit()
        return result
    except HTTPException:
        raise
    except Exception:  # noqa: BLE001 - private provider/DB details never cross the boundary.
        _refuse(503, "authority_unavailable")
