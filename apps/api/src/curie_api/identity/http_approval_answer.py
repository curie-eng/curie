"""Authenticated answers to HTTP-selected approvals.

The answering actor is authenticated from the existing chat attestation plus a
receiver-signed proof over the exact submitted bytes, then the original invoker
and the actor are checked independently and the operation commits through one
explicit owner. @spec HTTP-APPROVAL-CONTINUITY-7/8/9/10.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal, NoReturn

from curie_internal import identity_approval as wire
from curie_internal import identity_context as codec
from fastapi import HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api import identity_fence, principal_authority
from curie_api.approval_auth import AuthenticatedApprovalPrincipal
from curie_api.approval_teams import authenticated_actor_evidence, team_restriction
from curie_api.authorizer import authorize_approval
from curie_api.crud import approvals as approvals_crud
from curie_api.models import Approval, ApprovalAuditEntry, ApprovalStatus, Principal
from curie_api.schemas.http_authority import HttpSelectionV1
from curie_api.schemas.http_identity import LookupRequestV1, SenderV1, ValidatedFacts
from curie_api.slack_approvers import SlackUserGroupMembers, approval_token_identity

from .http_approval import approval_record_digest, verified_capsule
from .http_credential_registry import ResolvedProviderCredential
from .http_invocation import (
    AuthenticatedSource,
    HttpInvocationRuntime,
    ReceiverRegistrationV1,
    SourceCapture,
    capture_source,
    invocation_source,
    lookup_request,
    source_facts,
    source_permission,
)
from .http_transport import HttpIdentityClient, HttpIdentityObservation
from .slack import INTERACTION_DELIVERIES, SlackEvidence, derive_registered_sender

PROOF_HEADER = "X-Curie-Approval-Receiver-Proof"
_INVALID_PRINCIPAL = "missing or invalid approval principal"
_RECOVERY_AUDIT_ACTION = approvals_crud.RECOVERY_AUDIT_ACTION
_RECOVERY_AUTHORIZER = "approval_recovery"
_AUTHORIZER = "HttpApprovalAnswer"


def _refuse(status: int, code: str) -> NoReturn:
    """Fixed details only; no submitted value is echoed. @spec HTTP-APPROVAL-CONTINUITY-7/10."""
    raise HTTPException(status, code, headers={"Cache-Control": "no-store"})


def _invalid() -> NoReturn:
    """@spec HTTP-APPROVAL-CONTINUITY-7."""
    _refuse(401, _INVALID_PRINCIPAL)


def _deny() -> NoReturn:
    """@spec HTTP-APPROVAL-CONTINUITY-8."""
    _refuse(403, "caller_not_allowed")


def _unavailable() -> NoReturn:
    """@spec HTTP-APPROVAL-CONTINUITY-9."""
    _refuse(503, "authority_unavailable")


def _sha256(data: bytes) -> str:
    """@spec HTTP-APPROVAL-CONTINUITY-7."""
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class ApprovalAnswer:
    """One authenticated resolve or recover request, as submitted.

    @spec HTTP-APPROVAL-CONTINUITY-7/10.
    """

    operation: Literal["resolve", "recover"]
    decision: Literal["approved", "rejected"]
    note: str | None
    body: bytes
    attestation: str
    proof: str
    recovery_key: str | None = None
    recovery_facts: tuple[str, ...] = ()


class ApprovalSettled(Exception):  # noqa: N818 - a settled outcome, not a failure.
    """The row was no longer pending at the CAS. @spec HTTP-APPROVAL-CONTINUITY-10."""

    def __init__(self, status: str, resolved_by: str | None, expires_at: datetime | None) -> None:
        super().__init__(status)
        self.status = status
        self.resolved_by = resolved_by
        self.expires_at = expires_at


@dataclass(frozen=True)
class _Actor:
    """Receiving scope of an authenticated answering actor. @spec HTTP-APPROVAL-CONTINUITY-7."""

    registration: ReceiverRegistrationV1
    source: AuthenticatedSource
    channel: str


def _authenticate_actor(
    runtime: HttpInvocationRuntime,
    row: Approval,
    saved: wire.ApprovalContextV1,
    principal: AuthenticatedApprovalPrincipal,
    answer: ApprovalAnswer,
) -> _Actor:
    """Exact bytes, both signatures and equal Slack evidence. @spec HTTP-APPROVAL-CONTINUITY-7."""
    slack = principal.slack
    if (
        principal.kind != "chat" or slack is None or principal.actor_channel is None
        or slack.delivery not in INTERACTION_DELIVERIES or not slack.interaction_user_team_id
        or type(answer.attestation) is not str or type(answer.body) is not bytes
    ):
        _invalid()
    proof = wire.verify_approval_answer_proof(answer.proof, runtime._receiver_keys)
    if not isinstance(proof, wire.ApprovalAnswerProofV1):
        _invalid()
    registration = runtime._receivers.get((proof.receiver_id, proof.receiver_generation))
    try:
        attestation = _sha256(answer.attestation.encode("ascii"))
        signed = SlackEvidence.model_validate(proof.slack.model_dump(mode="json"))
    except (UnicodeEncodeError, ValueError):
        _invalid()
    target = row.card_channel or row.reply_channel
    if (
        registration is None
        or proof.operation != answer.operation or proof.approval_id != str(row.id)
        or not hmac.compare_digest(proof.body_sha256, _sha256(answer.body))
        or not hmac.compare_digest(proof.attestation_sha256, attestation)
        or proof.channel_identity_id != registration.channel_identity_id
        or proof.provider_installation_id != registration.provider_installation_id
        or proof.native_channel_id != principal.actor_channel or proof.native_channel_id != target
        or signed != slack or signed.user_id != principal.subject
        # The answering receiver may differ from the invoker's; the server-resolved
        # approval target names the identity it must be, and the originating
        # binding must be one this registration explicitly serves.
        or registration.channel_identity_name != approval_token_identity(row)
        or registration.tenant_id != saved.scope.tenant_id
        or saved.scope.binding_id not in registration.binding_ids
    ):
        _invalid()
    # The registered-sender owner: an ordinary workspace keeps the existing
    # derivation; a Grid workspace resolves in its organisation and only from
    # its own workspace (@spec HTTP-INVOCATION-EXECUTION-21).
    sender = derive_registered_sender(
        signed,
        namespace_kind=registration.namespace.kind,
        namespace_authority=registration.namespace.authority,
        namespace_key=registration.namespace.key,
        installation_account=registration.installation_account,
    )
    if (
        not isinstance(sender, tuple) or sender[0] != registration.namespace.key
        or sender[1] != principal.subject
    ):
        _invalid()
    return _Actor(
        registration,
        AuthenticatedSource(
            saved.scope.binding_id, saved.scope.binding_generation,
            SenderV1(
                provider="slack", channel_identity_id=registration.channel_identity_id,
                provider_installation_id=registration.provider_installation_id,
                namespace=registration.namespace, native_id=sender[1],
            ),
            agent_id=saved.scope.agent_id,
        ),
        proof.native_channel_id,
    )


@dataclass(frozen=True)
class _Subject:
    """One subject's captured scope and external observation. @spec HTTP-APPROVAL-CONTINUITY-9."""

    role: Literal["invocation", "approver"]
    registration: ReceiverRegistrationV1
    source: AuthenticatedSource
    capture: SourceCapture
    credential: ResolvedProviderCredential
    lookup: LookupRequestV1
    response_bytes: bytes = b""

    def independent_human(self) -> bool:
        """Registered evidence only, never inferred. @spec HTTP-APPROVAL-CONTINUITY-7/8."""
        return self.source.sender.native_id in self.registration.human_native_ids


async def _prepare(
    session: AsyncSession, runtime: HttpInvocationRuntime,
    role: Literal["invocation", "approver"], registration: ReceiverRegistrationV1,
    source: AuthenticatedSource,
) -> _Subject:
    """Phase 1 for one subject: capture and a fresh role-purpose lookup request.

    @spec HTTP-APPROVAL-CONTINUITY-8/9.
    """
    captured, _ = await capture_source(
        session, registration, source, role=role, binding_identity=role == "invocation",
    )
    credential = runtime._providers.resolve(captured.authority.configuration)
    if not isinstance(credential, ResolvedProviderCredential):
        _unavailable()
    purpose: Literal["invocation", "approval"] = (
        "invocation" if role == "invocation" else "approval"
    )
    return _Subject(
        role, registration, source, captured, credential,
        lookup_request(registration, captured, credential, purpose=purpose),
    )


async def _recapture(
    session: AsyncSession, subject: _Subject, *, writer: bool,
) -> tuple[SourceCapture, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-8/9."""
    return await capture_source(
        session, subject.registration, subject.source,
        writer_agent=subject.capture.agent_id if writer else None, role=subject.role,
        binding_identity=subject.role == "invocation",
    )


async def _check_principal(
    session: AsyncSession, subject: _Subject, facts: ValidatedFacts,
    application: principal_authority.PrincipalApplication,
) -> None:
    """The applied principal is still the observed human. @spec HTTP-APPROVAL-CONTINUITY-9."""
    if application.principal_id is None:
        return
    principal = await session.get(Principal, application.principal_id, populate_existing=True)
    if (
        principal is None or facts.identity is None or principal.type != "human"
        or str(principal.tenant_id) != subject.registration.tenant_id
        or principal.idp_issuer != facts.identity.issuer
        or principal.idp_subject != facts.identity.subject
        or principal.status != facts.identity.account_status
        or principal.authorization_version != application.authorization_version
    ):
        _unavailable()


async def _route_decision(
    row: Approval, route_binding: Any, actor: _Subject, approver_sets: Any,
    subject: str, channel: str,
) -> dict[str, Any] | None:
    """Explicit route restrictions through their existing owner, freshly.

    A migrated approver role does not acquire the legacy card-channel default as
    an additional restriction; a nonmigrated role keeps it. @spec HTTP-APPROVAL-CONTINUITY-8.
    """
    role = None if actor.capture.policy is None else actor.capture.policy.roles.approver
    if role is not None and role.legacy_state == "migrated" and not team_restriction(
        row, route_binding,
    ):
        return None
    approver_set = approver_sets(row, route_binding)
    if isinstance(approver_set, SlackUserGroupMembers):
        approver_set = approver_set.fresh()
    _, decision = await authorize_approval(
        row, subject, channel, approver_set=approver_set, principal_kind="chat",
    )
    if not decision.allowed:
        if isinstance(decision.evidence, dict) and decision.evidence.get("lookup_failed"):
            _unavailable()
        _deny()
    return decision.evidence


def _settled(row: Approval) -> NoReturn:
    """@spec HTTP-APPROVAL-CONTINUITY-10."""
    raise ApprovalSettled(str(row.status), row.resolved_by, row.expires_at)


def _pending(
    row: Approval | None, digest: str | None = None, *, status: str = ApprovalStatus.pending,
) -> Approval:
    """Pending, unexpired and unchanged since capture; a read-only check of a
    settled row instead requires its status unchanged. @spec HTTP-APPROVAL-CONTINUITY-9/10.
    """
    if row is None:
        _unavailable()
    if status != ApprovalStatus.pending:
        if row.status != status or (
            digest is not None and approval_record_digest(row) != digest
        ):
            _unavailable()
        return row
    if row.status != ApprovalStatus.pending:
        _settled(row)
    if row.expires_at is not None and row.expires_at <= datetime.now(UTC).replace(tzinfo=None):
        raise ApprovalSettled("expired", None, row.expires_at)
    if digest is not None and approval_record_digest(row) != digest:
        _unavailable()
    return row


def _reseal(
    runtime: HttpInvocationRuntime, saved: wire.ApprovalContextV1, row: Approval,
    actor: _Subject, channel: str, facts: ValidatedFacts,
    application: principal_authority.PrincipalApplication, lifecycle: int,
) -> str:
    """Status, actor provenance and row digest together. @spec HTTP-APPROVAL-CONTINUITY-6/9."""
    config = actor.capture.authority.configuration
    selection = config.selection
    assert isinstance(selection, HttpSelectionV1)
    sender = actor.source.sender
    identity = facts.identity
    values = saved.model_dump()
    values.update(
        status=str(row.status),
        key_id=runtime._signing_key[0],
        key_generation=runtime._signing_key[1],
        approval_record_sha256=approval_record_digest(row),
        actor_scope=wire.ApprovalActorScopeV1(
            tenant_id=actor.registration.tenant_id,
            channel_identity_id=sender.channel_identity_id,
            provider_installation_id=sender.provider_installation_id,
            provider=sender.provider, native_id=sender.native_id, actor_channel=channel,
            namespace=codec.ContextNamespaceV1.model_validate(sender.namespace.model_dump()),
        ),
        actor_receiver_id=actor.registration.receiver_id,
        actor_receiver_generation=actor.registration.generation,
        actor_independent_human=actor.independent_human(),
        actor_provider_issuer=selection.issuer,
        actor_provider_version=config.version,
        actor_provider_credential_id=selection.credential_ref.id,
        actor_provider_credential_generation=selection.credential_ref.generation,
        actor_lifecycle_generation=lifecycle,
        actor_role_policy_version=actor.capture.policy_version,
        actor_outcome=application.outcome,
        actor_canonical=None if identity is None or application.outcome == "unresolved"
        else codec.ContextCanonicalV1(issuer=identity.issuer, subject=identity.subject),
        actor_principal=None if (
            application.principal_id is None or application.authorization_version is None
        ) else codec.ContextPrincipalV1(
            principal_id=str(application.principal_id),
            authorization_version=application.authorization_version,
        ),
    )
    return wire.seal_approval_context(
        wire.ApprovalContextV1.model_validate(values), runtime._context_keys,
    )


@dataclass(frozen=True)
class _PreservedAssociation:
    """A sealed subject's canonical association, checked like the invoker's.

    @spec HTTP-APPROVAL-CONTINUITY-8.
    """

    scope: codec.InvocationScopeV1
    receiver_id: str
    receiver_generation: int
    independent_human: bool
    canonical: codec.ContextCanonicalV1 | None


@dataclass
class _Composition:
    """Phases 2 to 4 for the required subjects; phase 5 belongs to the operation.

    Route restrictions are evaluated for the approver subject only.
    @spec HTTP-APPROVAL-CONTINUITY-8/9.
    """

    request: Request
    runtime: HttpInvocationRuntime
    client: HttpIdentityClient
    factory: Any
    valkey: Any
    approval_id: uuid.UUID
    snapshot: str
    row: Approval
    route_binding: Any
    approver_sets: Any
    actor_native: str
    actor_channel: str
    subjects: list[_Subject]
    associations: dict[str, Any]
    applications: dict[str, principal_authority.PrincipalApplication] = field(
        default_factory=dict,
    )
    final: int = 0
    route_evidence: dict[str, Any] | None = None

    async def _observe(self, subject: _Subject) -> _Subject:
        """Phase 2 for one subject. @spec HTTP-APPROVAL-CONTINUITY-9."""
        observation = await self.client.lookup(
            target=subject.credential.target, bearer=subject.credential.bearer,
            request=subject.lookup, limits=subject.credential.limits,
        )
        if not isinstance(observation, HttpIdentityObservation):
            _unavailable()
        return _Subject(
            subject.role, subject.registration, subject.source, subject.capture,
            subject.credential, subject.lookup, observation.response_bytes,
        )

    async def observe_and_apply(self) -> None:
        """Phases 2 and 3: lookups outside SQL, then chained lifecycle application.

        @spec HTTP-APPROVAL-CONTINUITY-8/9.
        """
        if len({subject.capture.authority for subject in self.subjects}) != 1:
            _unavailable()
        actor = next((item for item in self.subjects if item.role == "approver"), None)
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(self._observe(subject)) for subject in self.subjects]
            route = None if actor is None else group.create_task(_route_decision(
                self.row, self.route_binding, actor, self.approver_sets,
                self.actor_native, self.actor_channel,
            ))
        self.subjects = [task.result() for task in tasks]
        self.route_evidence = None if route is None else route.result()
        async with identity_fence.mutation(self.factory, self.valkey) as writer:
            async with writer.begin():
                for subject in self.subjects:
                    current, _ = await _recapture(writer, subject, writer=True)
                    if current != subject.capture:
                        _unavailable()
                observed: dict[tuple[str, str], Any] = {}
                for subject in self.subjects:
                    facts = source_facts(subject.response_bytes, subject.lookup, subject.credential)
                    key = (subject.source.sender.native_id, subject.source.sender.namespace.id)
                    # Contradictory canonical observations for one sender refuse.
                    if observed.setdefault(key, facts.identity) != facts.identity:
                        _unavailable()
                lifecycle = self.subjects[0].capture.authority
                for subject in self.subjects:
                    application = await principal_authority.apply_http_principal(
                        writer, capture=lifecycle, request=subject.lookup,
                        response_bytes=subject.response_bytes,
                    )
                    self.applications[subject.role] = application
                    lifecycle = principal_authority.AuthorityCapture(
                        lifecycle.configuration, application.lifecycle_generation,
                    )
                _pending(
                    await writer.get(Approval, self.approval_id, populate_existing=True),
                    self.snapshot, status=self.row.status,
                )
                await writer.commit()
        self.final = lifecycle.lifecycle_generation

    async def verify(self, session: AsyncSession, *, writer: bool) -> dict[str, ValidatedFacts]:
        """Phases 4 and 5 repeat every comparison and each permission.

        Requester equality neither grants nor vetoes: each subject is judged by
        its own role against its own preserved association, if any.
        @spec HTTP-APPROVAL-CONTINUITY-8/9.
        """
        current_facts: dict[str, ValidatedFacts] = {}
        state = self.request.app.state
        for subject in self.subjects:
            current, binding = await _recapture(session, subject, writer=writer)
            if (
                current.fingerprint != subject.capture.fingerprint
                or current.authority.lifecycle_generation != self.final
                or state.http_identity_runtime is not self.runtime
                or state.http_identity_client is not self.client
            ):
                _unavailable()
            observed = source_facts(subject.response_bytes, subject.lookup, subject.credential)
            await _check_principal(session, subject, observed, self.applications[subject.role])
            source_permission(
                current, binding, observed, subject.independent_human(),
                self.associations.get(subject.role), role_name=subject.role,
            )
            current_facts[subject.role] = observed
        if await approvals_crud.get_approval_route_binding(session, self.row) != self.route_binding:
            _unavailable()
        return current_facts

    async def recheck(self) -> None:
        """Phase 4: a clean read-only reader. @spec HTTP-APPROVAL-CONTINUITY-9."""
        async with self.factory() as session, session.begin():
            await identity_fence.lock_identity_reader(session)
            await self.verify(session, writer=False)
            _pending(
                await session.get(Approval, self.approval_id, populate_existing=True),
                self.snapshot, status=self.row.status,
            )


def _preserved_actor(
    runtime: HttpInvocationRuntime, row: Approval, saved: wire.ApprovalContextV1,
) -> tuple[_Actor, _PreservedAssociation]:
    """The sealed answering actor, under its still-current registration.

    @spec HTTP-APPROVAL-CONTINUITY-7/8.
    """
    scope = saved.actor_scope
    registration = None if scope is None or saved.actor_receiver_id is None else (
        runtime._receivers.get((saved.actor_receiver_id, saved.actor_receiver_generation or 0))
    )
    if (
        scope is None or registration is None
        or registration.tenant_id != scope.tenant_id or scope.tenant_id != saved.scope.tenant_id
        or registration.channel_identity_id != scope.channel_identity_id
        or registration.provider_installation_id != scope.provider_installation_id
        or registration.namespace.model_dump() != scope.namespace.model_dump()
        or registration.channel_identity_name != approval_token_identity(row)
        or saved.scope.binding_id not in registration.binding_ids
        or scope.provider != "slack"
        or (scope.native_id in registration.human_native_ids) != saved.actor_independent_human
    ):
        _deny()
    return _Actor(
        registration,
        AuthenticatedSource(
            saved.scope.binding_id, saved.scope.binding_generation,
            SenderV1(
                provider="slack", channel_identity_id=registration.channel_identity_id,
                provider_installation_id=registration.provider_installation_id,
                namespace=registration.namespace, native_id=scope.native_id,
            ),
            agent_id=saved.scope.agent_id,
        ),
        scope.actor_channel,
    ), _PreservedAssociation(
        saved.scope, registration.receiver_id, registration.generation,
        scope.native_id in registration.human_native_ids, saved.actor_canonical,
    )


async def answer_http_approval(
    request: Request,
    approval_id: uuid.UUID,
    *,
    principal: AuthenticatedApprovalPrincipal,
    answer: ApprovalAnswer,
    approver_sets: Any,
) -> Approval:
    """The operation-specific approval commit owner for resolve and recover.

    Phases, in order: a short reader capture; independent bounded lookups and
    route membership outside SQL; lifecycle application under the ordered
    boundary; a clean read-only recheck; then one mutation that repeats every
    comparison before the pending-only CAS, reseal and audit commit together.
    No external call runs under a transaction or fence, and the wake stays owed.
    @spec HTTP-APPROVAL-CONTINUITY-8/9/10.
    """
    runtime = getattr(request.app.state, "http_identity_runtime", None)
    client = getattr(request.app.state, "http_identity_client", None)
    if type(runtime) is not HttpInvocationRuntime or not isinstance(client, HttpIdentityClient):
        _unavailable()
    factory, valkey = request.app.state.sessionmaker, request.app.state.valkey
    try:
        return await _answer(
            request, runtime, client, factory, valkey, approval_id,
            principal=principal, answer=answer, approver_sets=approver_sets,
        )
    except ApprovalSettled:
        raise
    except HTTPException as error:
        # The shared phases name their own refusals; this route keeps fixed ones.
        if error.detail == "invalid_http_receiver":
            _deny()
        if error.status_code == 503:
            _unavailable()
        raise
    except Exception:  # noqa: BLE001 - provider, credential and SQL details never cross.
        _unavailable()


async def _answer(
    request: Request, runtime: HttpInvocationRuntime, client: HttpIdentityClient,
    factory: Any, valkey: Any, approval_id: uuid.UUID, *,
    principal: AuthenticatedApprovalPrincipal, answer: ApprovalAnswer, approver_sets: Any,
) -> Approval:
    """@spec HTTP-APPROVAL-CONTINUITY-9."""
    # Phase 1: short reader capture of every required source.
    async with factory() as session, session.begin():
        await identity_fence.lock_identity_reader(session)
        row = _pending(await session.get(Approval, approval_id, populate_existing=True))
        saved = verified_capsule(runtime, row)
        if saved is None or saved.status != "pending" or row.purpose != "session":
            _refuse(403, "unsupported_approval")
        actor_scope = _authenticate_actor(runtime, row, saved, principal, answer)
        try:
            caller_registration, caller_source = invocation_source(runtime, saved)
        except HTTPException:
            _deny()
        caller = await _prepare(
            session, runtime, "invocation", caller_registration, caller_source,
        )
        actor = await _prepare(
            session, runtime, "approver", actor_scope.registration, actor_scope.source,
        )
        route_binding = await approvals_crud.get_approval_route_binding(session, row)
        snapshot = approval_record_digest(row)
    composition = _Composition(
        request, runtime, client, factory, valkey, approval_id, snapshot, row, route_binding,
        approver_sets, principal.subject, actor_scope.channel, [caller, actor],
        {"invocation": saved, "approver": None},
    )
    await composition.observe_and_apply()
    await composition.recheck()
    actor = composition.subjects[1]

    # Phase 5: the one mutation; no external call here.
    async with identity_fence.mutation(factory, valkey) as writer:
        async with writer.begin():
            current_facts = await composition.verify(writer, writer=True)
            locked = (await writer.execute(
                select(Approval).where(Approval.id == approval_id).with_for_update()
                .execution_options(populate_existing=True)
            )).scalar_one_or_none()
            locked = _pending(locked, snapshot)
            locked.status = ApprovalStatus(answer.decision)
            locked.resolved_by = principal.subject
            locked.resolution_note = answer.note
            locked.resolved_at = datetime.now(UTC).replace(tzinfo=None)
            await writer.flush()
            await writer.refresh(locked)
            locked.identity_context = _reseal(
                runtime, saved, locked, actor, actor_scope.channel, current_facts["approver"],
                composition.applications["approver"], composition.final,
            )
            authenticated = authenticated_actor_evidence(principal)
            recovery = answer.operation == "recover"
            writer.add(ApprovalAuditEntry(
                approval_id=locked.id,
                action=_RECOVERY_AUDIT_ACTION if recovery else "resolved",
                actor=principal.subject, actor_channel=principal.actor_channel,
                principal_kind=principal.kind, authenticated=True, principal_subject=None,
                decision=answer.decision,
                authorizer=_RECOVERY_AUTHORIZER if recovery else _AUTHORIZER, authorized=True,
                reason=answer.note,
                evidence={
                    "kind": "administrative_recovery", "recovery_key": answer.recovery_key,
                    "facts": list(answer.recovery_facts), "authenticated_actor": authenticated,
                } if recovery else {
                    "kind": "http_approval_answer", "authenticated_actor": authenticated,
                    "route": composition.route_evidence,
                },
            ))
            await writer.flush()
            await writer.commit()
    return locked


async def check_http_approval_actor(
    request: Request, approval_id: uuid.UUID, *, approver_sets: Any,
) -> None:
    """Independent current approver authority of the preserved answering actor.

    For a current boundary after resolution, such as approved-resume publication
    or a grant check: purpose=approval and role=approver, against the actor and
    canonical association sealed with the approved row, never the requester.
    Read-only phases 1 to 4; phase 5 is absent. Returns None on current
    authority. Raises ``HTTPException``: 403 ``caller_not_allowed`` when the
    actor is denied or its registration no longer matches, 403
    ``unsupported_approval`` when the row is not a sealed approved selected
    session approval, 503 ``authority_unavailable`` otherwise.
    @spec HTTP-APPROVAL-CONTINUITY-8/9.
    """
    runtime = getattr(request.app.state, "http_identity_runtime", None)
    client = getattr(request.app.state, "http_identity_client", None)
    if type(runtime) is not HttpInvocationRuntime or not isinstance(client, HttpIdentityClient):
        _unavailable()
    factory, valkey = request.app.state.sessionmaker, request.app.state.valkey
    try:
        async with factory() as session, session.begin():
            await identity_fence.lock_identity_reader(session)
            row = await session.get(Approval, approval_id, populate_existing=True)
            saved = None if row is None else verified_capsule(runtime, row)
            if (
                row is None or saved is None or row.status != ApprovalStatus.approved
                or saved.status != "approved" or row.purpose != "session"
            ):
                _refuse(403, "unsupported_approval")
            actor, association = _preserved_actor(runtime, row, saved)
            subject = await _prepare(
                session, runtime, "approver", actor.registration, actor.source,
            )
            route_binding = await approvals_crud.get_approval_route_binding(session, row)
            snapshot = approval_record_digest(row)
        composition = _Composition(
            request, runtime, client, factory, valkey, approval_id, snapshot, row,
            route_binding, approver_sets, actor.source.sender.native_id, actor.channel,
            [subject], {"approver": association},
        )
        await composition.observe_and_apply()
        await composition.recheck()
    except HTTPException as error:
        if error.detail == "invalid_http_receiver":
            _deny()
        if error.status_code == 503:
            _unavailable()
        raise
    except Exception:  # noqa: BLE001 - provider, credential and SQL details never cross.
        _unavailable()
