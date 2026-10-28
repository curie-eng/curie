"""Current HTTP invocation composition. @spec HTTP-INVOCATION-EXECUTION-1/2/4/5/6/15."""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Annotated, Any, Literal, Never, Protocol, Self, SupportsIndex

from aci_protocol import PrincipalRef
from curie_internal import identity_context as codec
from curie_internal.identity_authority import TurnAuthorityRequestV1, TurnAuthorityResponseV1
from fastapi import HTTPException, Request
from pydantic import Field, model_validator
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api import (
    admission,
    agent_role_policy,
    channel_token,
    identity_fence,
    principal_authority,
)
from curie_api.agent_role_evaluation import RoleFilterContext, evaluate_role_filter_layer
from curie_api.config import get_settings
from curie_api.delivery import (
    BacklogReservation,
    DeliveryEnqueueGuard,
    DeliveryIdentityContext,
    DeliveryValidityWindow,
)
from curie_api.identity.delivery_validity import identity_delivery_window
from curie_api.identity.http_credential_registry import (
    ProviderCredentialRegistry,
    ResolvedProviderCredential,
)
from curie_api.identity.http_transport import HttpIdentityClient, HttpIdentityObservation
from curie_api.identity.slack import (
    SLACK_ENTERPRISE_KIND,
    SLACK_WORKSPACE_KIND,
    SlackEvidence,
    derive_registered_sender,
)
from curie_api.models import (
    Agent,
    AgentAccessPolicy,
    AgentChannel,
    ChannelIdentity,
    IdentityNamespace,
    Principal,
    ProviderInstallation,
    Team,
    Tenant,
    TenantIdentityProvider,
)
from curie_api.schemas.agent_access import AgentRolePolicyV1, SurfaceAccountSelectorV1
from curie_api.schemas.http_authority import HttpSelectionV1
from curie_api.schemas.http_identity import (
    CanonicalUUID,
    FactsModel,
    FactsRefusal,
    LookupRequestV1,
    NamespaceV1,
    ScopeV1,
    SenderV1,
    ValidatedFacts,
    validate_lookup,
)


class ReceiverRegistrationV1(FactsModel):
    """@spec HTTP-INVOCATION-EXECUTION-1/15."""

    schema_version: codec.Version
    receiver_id: CanonicalUUID
    generation: codec.Generation
    tenant_id: CanonicalUUID
    channel_identity_id: CanonicalUUID
    channel_identity_name: codec.Identifier
    provider_installation_id: CanonicalUUID
    namespace: NamespaceV1
    installation_account: codec.Identifier
    bot_user_id: codec.Identifier
    binding_ids: Annotated[tuple[CanonicalUUID, ...], Field(min_length=1, max_length=256)]
    human_native_ids: Annotated[tuple[codec.Identifier, ...], Field(max_length=256)]

    @model_validator(mode="after")
    def consistent_scope(self) -> Self:
        """@spec HTTP-INVOCATION-EXECUTION-15/21."""
        namespace = self.namespace
        ordinary = (
            namespace.kind == SLACK_WORKSPACE_KIND and namespace.key == self.installation_account
        )
        grid = (
            namespace.kind == SLACK_ENTERPRISE_KIND and namespace.key != self.installation_account
        )
        if (
            namespace.authority != "" or not (ordinary or grid)
            or len(set(self.binding_ids)) != len(self.binding_ids)
            or len(set(self.human_native_ids)) != len(self.human_native_ids)
        ):
            raise ValueError("invalid_receiver_registration")
        return self


class HttpInvocationRuntime:
    """@spec HTTP-INVOCATION-EXECUTION-1/15."""

    __slots__ = ("_providers", "_receivers", "_receiver_keys", "_context_keys", "_signing_key")
    _providers: ProviderCredentialRegistry
    _receivers: Mapping[tuple[str, int], ReceiverRegistrationV1]
    _receiver_keys: codec.IdentityKeyring
    _context_keys: codec.IdentityKeyring
    _signing_key: tuple[str, int]

    def __new__(cls, *args: object, **kwargs: object) -> Self:
        """@spec HTTP-INVOCATION-EXECUTION-15."""
        raise TypeError("http_invocation_runtime_not_public")

    def __setattr__(self, name: str, value: object) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-15."""
        raise AttributeError("immutable_http_invocation_runtime")

    def __delattr__(self, name: str) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-15."""
        raise AttributeError("immutable_http_invocation_runtime")

    def __getstate__(self) -> Never:
        """@spec HTTP-INVOCATION-EXECUTION-15."""
        raise TypeError("http_invocation_runtime_not_serializable")

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """@spec HTTP-INVOCATION-EXECUTION-15."""
        raise TypeError("http_invocation_runtime_not_serializable")

    def __repr__(self) -> str:
        """@spec HTTP-INVOCATION-EXECUTION-15."""
        return "HttpInvocationRuntime()"

    def __bool__(self) -> Never:
        """@spec HTTP-INVOCATION-EXECUTION-15."""
        raise TypeError("http_invocation_runtime_is_not_permission")


def build_http_invocation_runtime(
    providers: ProviderCredentialRegistry,
    receiver_registrations: tuple[tuple[ReceiverRegistrationV1, bytes], ...],
    context_keys: tuple[tuple[str, int, bytes], ...],
    *,
    signing_key: tuple[str, int],
) -> HttpInvocationRuntime:
    """@spec HTTP-INVOCATION-EXECUTION-1/15."""
    try:
        if (
            type(providers) is not ProviderCredentialRegistry
            or type(receiver_registrations) is not tuple or len(receiver_registrations) > 256
            or type(context_keys) is not tuple or type(signing_key) is not tuple
            or len(signing_key) != 2
        ):
            raise ValueError
        receivers = {}
        receiver_keys = []
        materials = set()
        for item in receiver_registrations:
            if type(item) is not tuple or len(item) != 2:
                raise ValueError
            supplied, material = item
            if type(supplied) is not ReceiverRegistrationV1:
                raise ValueError
            registration = ReceiverRegistrationV1.model_validate(supplied.model_dump())
            pair = (registration.receiver_id, registration.generation)
            if pair in receivers or type(material) is not bytes or len(material) != 32:
                raise ValueError
            if material in materials:
                raise ValueError
            materials.add(material)
            receivers[pair] = registration
            receiver_keys.append((*pair, material))
        context_ring = codec.build_identity_keyring("context", context_keys)
        references = set()
        for key_id, generation, material in context_keys:
            if material in materials:
                raise ValueError
            materials.add(material)
            references.add((key_id, generation))
        if (
            type(signing_key[0]) is not str or type(signing_key[1]) is not int
            or signing_key not in references
        ):
            raise ValueError
        runtime = object.__new__(HttpInvocationRuntime)
        object.__setattr__(runtime, "_providers", providers)
        object.__setattr__(runtime, "_receivers", MappingProxyType(receivers))
        object.__setattr__(runtime, "_receiver_keys", codec.build_identity_keyring(
            "receiver", tuple(receiver_keys),
        ))
        object.__setattr__(runtime, "_context_keys", context_ring)
        object.__setattr__(runtime, "_signing_key", signing_key)
        return runtime
    except Exception:  # noqa: BLE001 - initializer failures never disclose key material.
        raise ValueError("invalid_http_invocation_runtime") from None


def _invalid_receiver() -> Never:
    """@spec HTTP-INVOCATION-EXECUTION-2/15."""
    raise HTTPException(401, "invalid_http_receiver")


def _unavailable() -> Never:
    """@spec HTTP-INVOCATION-EXECUTION-4/15."""
    raise HTTPException(503, "identity_authority_unavailable")


def _deny() -> Never:
    """@spec HTTP-INVOCATION-EXECUTION-5/15."""
    raise HTTPException(403, "caller_not_allowed")


class OriginalAssociation(Protocol):
    """Preserved invoker provenance a later phase checks. @spec HTTP-APPROVAL-CONTINUITY-8."""

    @property
    def scope(self) -> codec.InvocationScopeV1: ...
    @property
    def receiver_id(self) -> str: ...
    @property
    def receiver_generation(self) -> int: ...
    @property
    def independent_human(self) -> bool: ...
    @property
    def canonical(self) -> codec.ContextCanonicalV1 | None: ...


@dataclass(frozen=True)
class SourceCapture:
    """@spec HTTP-INVOCATION-EXECUTION-4/15."""

    authority: principal_authority.AuthorityCapture
    agent_id: uuid.UUID
    fingerprint: str = field(repr=False)
    sender: SenderV1
    policy: AgentRolePolicyV1 | None
    policy_version: int | None
    retained_group: tuple[str, str] | None


@dataclass(frozen=True)
class AuthenticatedSource:
    """Core-authenticated source, never worker claims. @spec HTTP-INVOCATION-EXECUTION-2/8."""

    binding_id: str
    binding_generation: int
    sender: SenderV1
    agent_id: str | None = None
    address: str | None = None


def _receiver_source(
    registration: ReceiverRegistrationV1, proof: codec.ReceiverProofV1,
    claims: channel_token.ChannelClaims, body: Any,
) -> AuthenticatedSource:
    """@spec HTTP-INVOCATION-EXECUTION-2/15/21."""
    if (
        proof.binding_id != claims.channel_id or proof.binding_generation != claims.generation
        or proof.channel_identity_id != registration.channel_identity_id
        or proof.provider_installation_id != registration.provider_installation_id
        or proof.slack.delivery != "app_mention" or proof.slack.is_ext_shared_channel is not False
        or body.kind != "slack" or body.address != proof.native_channel_id
        or body.delivery_id != proof.delivery_id or body.conversation_id != proof.conversation_id
    ):
        _invalid_receiver()
    evidence = SlackEvidence.model_validate(proof.slack.model_dump(mode="json"))
    sender = derive_registered_sender(
        evidence,
        namespace_kind=registration.namespace.kind,
        namespace_authority=registration.namespace.authority,
        namespace_key=registration.namespace.key,
        installation_account=registration.installation_account,
    )
    if (
        not isinstance(sender, tuple) or sender[0] != registration.namespace.key
        or sender[1] != body.author
    ):
        _invalid_receiver()
    return AuthenticatedSource(
        claims.channel_id, claims.generation,
        SenderV1(
            provider="slack", channel_identity_id=registration.channel_identity_id,
            provider_installation_id=registration.provider_installation_id,
            namespace=registration.namespace, native_id=sender[1],
        ),
        address=proof.native_channel_id,
    )


Role = Literal["invocation", "approver"]


async def capture_source(
    session: AsyncSession,
    registration: ReceiverRegistrationV1,
    source: AuthenticatedSource,
    *,
    writer_agent: uuid.UUID | None = None,
    role: Role = "invocation",
    binding_identity: bool = True,
) -> tuple[SourceCapture, AgentChannel]:
    """One source's current scope for one role.

    An answering actor's receiver may differ from the binding's identity: its
    required identity comes from the server-resolved approval target, checked by
    the answer owner, so ``binding_identity`` is false there.
    @spec HTTP-INVOCATION-EXECUTION-2/4/5/15 HTTP-APPROVAL-CONTINUITY-8.
    """
    tenant_id = uuid.UUID(registration.tenant_id)
    if writer_agent is not None:
        await principal_authority.lock_scope(session, tenant_id)
        await agent_role_policy.lock_policy_rows(
            session, tenant_id=tenant_id, agent_id=writer_agent, require_active=True,
        )
    tenant = await session.get(Tenant, tenant_id, populate_existing=True)
    idp_result = (await session.execute(select(
        TenantIdentityProvider, TenantIdentityProvider.http_selection.is_(None),
    ).where(TenantIdentityProvider.tenant_id == tenant_id))).one_or_none()
    if tenant is None or tenant.status != "active":
        _deny()
    if idp_result is None or idp_result[1] or idp_result[0].http_selection is None:
        _unavailable()
    idp = idp_result[0]
    config = principal_authority.stored_configuration(idp)
    if not isinstance(config.selection, HttpSelectionV1):
        _unavailable()
    binding_query = select(AgentChannel).where(AgentChannel.id == uuid.UUID(source.binding_id))
    if writer_agent is not None:
        binding_query = binding_query.with_for_update()
    binding = await session.scalar(binding_query.execution_options(populate_existing=True))
    if (
        binding is None or str(binding.id) not in registration.binding_ids
        or binding.tenant_id != tenant_id or binding.generation != source.binding_generation
        or binding.kind != "slack"
        or (source.address is not None and source.address != binding.address)
        or (source.agent_id is not None and source.agent_id != str(binding.agent_id))
        or (binding_identity and (binding.adapter or "default")
            != registration.channel_identity_name)
        or binding.endpoint is not None
        or source.sender.provider != "slack"
        or source.sender.channel_identity_id != registration.channel_identity_id
        or source.sender.provider_installation_id != registration.provider_installation_id
        or source.sender.namespace != registration.namespace
    ):
        _invalid_receiver()
    agent = await session.get(Agent, binding.agent_id, populate_existing=True)
    if agent is None or agent.tenant_id != tenant_id or agent.status != "active":
        _deny()
    policy_result = (await session.execute(select(
        AgentAccessPolicy, AgentAccessPolicy.role_configuration.is_(None),
    ).where(AgentAccessPolicy.agent_id == agent.id))).one_or_none()
    policy_row, sql_null = (None, True) if policy_result is None else policy_result
    policy = agent_role_policy.stored_snapshot(
        policy_row, sql_null, tenant_id=tenant_id, agent_id=agent.id,
    )
    identity = await session.get(
        ChannelIdentity, uuid.UUID(registration.channel_identity_id), populate_existing=True,
    )
    installation = await session.get(
        ProviderInstallation, uuid.UUID(registration.provider_installation_id),
        populate_existing=True,
    )
    namespace = await session.get(
        IdentityNamespace, uuid.UUID(registration.namespace.id), populate_existing=True,
    )
    if (
        identity is None or identity.tenant_id != tenant_id or identity.provider != "slack"
        or identity.name != registration.channel_identity_name or identity.status != "active"
        or identity.installation_mismatch
        or str(identity.provider_installation_id) != registration.provider_installation_id
        or installation is None or installation.tenant_id != tenant_id
        or installation.provider != "slack" or installation.status != "connected"
        or installation.authority != registration.namespace.authority
        or installation.external_account_id != registration.installation_account
        or namespace is None or namespace.tenant_id != tenant_id or namespace.provider != "slack"
        or namespace.status != "active" or namespace.authority != registration.namespace.authority
        or namespace.kind != registration.namespace.kind
        or namespace.key != registration.namespace.key
    ):
        _invalid_receiver()
    role_config = None if policy is None else getattr(policy.roles, role)
    team_id = None if policy_row is None else (
        policy_row.invocation_team_id if role == "invocation" else policy_row.approver_team_id
    )
    group = None
    team_state = None
    if team_id is not None and (role_config is None or role_config.legacy_state != "migrated"):
        team = await session.get(Team, team_id, populate_existing=True)
        if (
            team is None or team.tenant_id != tenant_id or team.source != "idp_group"
            or not team.external_id or not idp.managed_by or team.managed_by != idp.managed_by
            or team.epoch < 1 or team.last_synced_at is None
        ):
            _deny()
        group = (idp.issuer, team.external_id)
        team_state = (str(team.id), team.managed_by, team.external_id, team.epoch, team.digest,
                      team.last_synced_at.isoformat())
    fingerprint = json.dumps({
        "role": role,
        "configuration": config.model_dump(mode="json"),
        "managed_by": idp.managed_by, "governed_domains": idp.governed_domains,
        "agent": str(agent.id), "binding": str(binding.id), "generation": binding.generation,
        "kind": binding.kind, "address": binding.address, "adapter": binding.adapter,
        "endpoint": binding.endpoint, "topic": str(binding.topic_id),
        "allowed_callers": binding.allowed_callers,
        "policy": None if policy is None else policy.model_dump(mode="json"),
        "policy_version": None if policy_row is None else policy_row.version,
        "invocation_team": str(team_id),
        "approver_team": None if policy_row is None else str(policy_row.approver_team_id),
        "sql_null": sql_null, "team": team_state,
        "registration": registration.model_dump(mode="json"),
    }, sort_keys=True, separators=(",", ":"))
    return SourceCapture(
        principal_authority.AuthorityCapture(config, idp.lifecycle_generation), agent.id,
        fingerprint, source.sender, policy,
        None if policy_row is None else policy_row.version, group,
    ), binding


def lookup_request(
    registration: ReceiverRegistrationV1, captured: SourceCapture,
    credential: ResolvedProviderCredential, *, purpose: Literal["invocation", "approval"],
) -> LookupRequestV1:
    """A fresh bounded lookup for one source and provider purpose.

    @spec HTTP-INVOCATION-EXECUTION-4 HTTP-APPROVAL-CONTINUITY-8.
    """
    return LookupRequestV1(
        schema_version=1, request_id=str(uuid.uuid4()), purpose=purpose,
        authority_version=captured.authority.configuration.version,
        scope=ScopeV1(
            tenant_id=registration.tenant_id, expected_issuer=credential.target.expected_issuer,
            sender=captured.sender,
        ),
    )


def source_facts(
    response_bytes: bytes, lookup: LookupRequestV1, credential: ResolvedProviderCredential,
) -> ValidatedFacts:
    """@spec HTTP-INVOCATION-EXECUTION-4/5."""
    facts = validate_lookup(response_bytes, lookup, credential.limits, now=datetime.now(UTC))
    if isinstance(facts, FactsRefusal):
        _unavailable()
    if facts.sender_kind in {"service", "agent"}:
        _deny()
    if facts.outcome != "unresolved" and (
        facts.identity is None or facts.identity.kind != "human" or facts.sender_kind != "human"
    ):
        _deny()
    return facts


def source_permission(
    capture: SourceCapture, binding: AgentChannel, facts: ValidatedFacts, independent_human: bool,
    original: codec.InvocationContextV1 | OriginalAssociation | None = None,
    *, role_name: Role = "invocation",
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-5/15 HTTP-APPROVAL-CONTINUITY-8."""
    if not admission.admit(binding, [capture.sender.native_id]).allowed:
        _deny()
    role = None if capture.policy is None else getattr(capture.policy.roles, role_name)
    raw = role is not None and role.legacy_state == "migrated" and role.filter is not None and any(
        isinstance(entry, SurfaceAccountSelectorV1)
        and entry.provider == capture.sender.provider
        and entry.namespace == capture.sender.namespace
        and entry.native_id == capture.sender.native_id for entry in role.filter.entries
    )
    if facts.outcome != "resolved":
        if not (raw and independent_human) or capture.retained_group is not None:
            _deny()
    elif role is not None:
        result = evaluate_role_filter_layer(
            capture.policy, facts,
            context=RoleFilterContext(
                tenant_id=capture.authority.configuration.tenant_id, agent_id=str(capture.agent_id),
                role=role_name, request=facts.request,
            ),
            now=datetime.now(UTC),
        )
        if result.status != "match":
            _deny()
    if original is not None and facts.outcome == "resolved":
        identity = facts.identity
        assert identity is not None
        association_matches = original.canonical is not None and (
            original.canonical.issuer == identity.issuer
            and original.canonical.subject == identity.subject
        )
        if not association_matches and not (raw and independent_human):
            _deny()
    if capture.retained_group is not None and (
        facts.groups is None
        or capture.retained_group not in {(group.issuer, group.id) for group in facts.groups}
    ):
        _deny()
    # The v1 provenance grammar has no absence representation for this version.
    if capture.policy_version is None:
        _unavailable()


@dataclass(frozen=True)
class CurrentHttpInvocation:
    """Live boundary only. @spec HTTP-INVOCATION-EXECUTION-4/6/15."""

    row: AgentChannel = field(repr=False)
    session: AsyncSession = field(repr=False)
    intent_digest: str = field(repr=False)
    principal_ref: PrincipalRef | None
    _runtime: HttpInvocationRuntime = field(repr=False)
    _registration: ReceiverRegistrationV1 = field(repr=False)
    _capture: SourceCapture = field(repr=False)
    _facts: ValidatedFacts = field(repr=False)
    _application: principal_authority.PrincipalApplication = field(repr=False)
    _stable: str = field(repr=False)
    _validity: DeliveryValidityWindow = field(repr=False)
    _response_bytes: bytes = field(repr=False)
    _lookup: LookupRequestV1 = field(repr=False)
    _credential: ResolvedProviderCredential = field(repr=False)
    _source: AuthenticatedSource = field(repr=False)
    _client: HttpIdentityClient = field(repr=False)
    _original: codec.InvocationContextV1 | None = field(repr=False)
    _sealable: bool = field(default=True, repr=False)
    _open: bool = field(default=True, repr=False)

    def publication_window(self) -> tuple[str, DeliveryValidityWindow]:
        """The stable fence read under this open reader and its T1 window.

        Valid only while the boundary that yielded it is open; a closed or
        released reader is unavailable. @spec HTTP-APPROVAL-CONTINUITY-11.
        """
        if not self._open or not self.session.in_transaction():
            _unavailable()
        return self._stable, self._validity

    def seal_payload(
        self, *, payload: str, event_id: str, original_invocation_id: str,
        reservation: BacklogReservation, receipt_key: str,
    ) -> tuple[DeliveryIdentityContext, DeliveryEnqueueGuard]:
        """@spec HTTP-INVOCATION-EXECUTION-6/15."""
        if not self._sealable or not self._open or not self.session.in_transaction():
            _unavailable()
        config = self._capture.authority.configuration
        selection = config.selection
        assert isinstance(selection, HttpSelectionV1)
        identity = self._facts.identity
        sender = self._capture.sender
        principal = self.principal_ref
        policy_version = self._capture.policy_version
        if policy_version is None:
            _unavailable()
        context = codec.InvocationContextV1(
            schema_version=1, purpose="invocation", key_id=self._runtime._signing_key[0],
            key_generation=self._runtime._signing_key[1], event_id=event_id,
            original_event_id=original_invocation_id,
            scope=codec.InvocationScopeV1(
                tenant_id=config.tenant_id, agent_id=str(self.row.agent_id),
                binding_id=str(self.row.id), binding_generation=self.row.generation,
                channel_identity_id=sender.channel_identity_id,
                provider_installation_id=sender.provider_installation_id, provider=sender.provider,
                native_id=sender.native_id,
                namespace=codec.ContextNamespaceV1.model_validate(sender.namespace.model_dump()),
            ),
            receiver_id=self._registration.receiver_id,
            receiver_generation=self._registration.generation,
            independent_human=sender.native_id in self._registration.human_native_ids,
            provider_issuer=selection.issuer, provider_version=config.version,
            provider_credential_id=selection.credential_ref.id,
            provider_credential_generation=selection.credential_ref.generation,
            lifecycle_generation=self._application.lifecycle_generation,
            role_policy_version=policy_version,
            outcome=self._application.outcome,
            canonical=None if identity is None else codec.ContextCanonicalV1(
                issuer=identity.issuer, subject=identity.subject,
            ),
            principal=None if principal is None else codec.ContextPrincipalV1(
                principal_id=principal.principal_id,
                authorization_version=principal.authorization_version,
            ),
            payload_sha256=hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        )
        return (
            DeliveryIdentityContext(
                codec.seal_invocation_context(context, self._runtime._context_keys),
                self.intent_digest, receipt_key,
            ),
            DeliveryEnqueueGuard(
                identity_fence.FENCE_KEY, self._stable, self._validity, reservation,
            ),
        )


@asynccontextmanager
async def current_http_invocation(
    request: Request, *, body: Any, submitted_bytes: bytes,
    channel_credential: str, receiver_proof: str,
) -> AsyncIterator[CurrentHttpInvocation]:
    """@spec HTTP-INVOCATION-EXECUTION-2/4/5/6/15."""
    if not channel_credential or not receiver_proof:
        _invalid_receiver()
    runtime = getattr(request.app.state, "http_identity_runtime", None)
    client = getattr(request.app.state, "http_identity_client", None)
    if type(runtime) is not HttpInvocationRuntime or not isinstance(client, HttpIdentityClient):
        _unavailable()
    claims = channel_token.verify_claims(
        channel_credential, get_settings().api_key, scope=channel_token.CHANNEL_ENQUEUE_SCOPE,
    )
    proof = codec.verify_receiver_proof(receiver_proof, runtime._receiver_keys)
    if claims is None or not isinstance(proof, codec.ReceiverProofV1):
        _invalid_receiver()
    registration = runtime._receivers.get((proof.receiver_id, proof.receiver_generation))
    if registration is None or proof.body_sha256 != hashlib.sha256(submitted_bytes).hexdigest():
        _invalid_receiver()
    source = _receiver_source(registration, proof, claims, body)
    async with current_source_authority(
        request, runtime=runtime, client=client, registration=registration, source=source,
        intent_digest=hashlib.sha256(receiver_proof.encode("ascii")).hexdigest(),
    ) as current:
        yield current


@asynccontextmanager
async def current_source_authority(
    request: Request, *, runtime: HttpInvocationRuntime, client: HttpIdentityClient,
    registration: ReceiverRegistrationV1, source: AuthenticatedSource,
    intent_digest: str = "", original: codec.InvocationContextV1 | None = None,
) -> AsyncIterator[CurrentHttpInvocation]:
    """One caller's lookup/application/reader owner, purpose and role invocation.

    Two-subject approval operations compose the same phases in
    ``http_approval_answer``. @spec HTTP-INVOCATION-EXECUTION-4/5/8 HTTP-APPROVAL-CONTINUITY-8.
    """
    factory = request.app.state.sessionmaker
    valkey = request.app.state.valkey
    try:
        async with factory() as session, session.begin():
            await identity_fence.lock_identity_reader(session)
            captured, _ = await capture_source(session, registration, source)
            credential = runtime._providers.resolve(captured.authority.configuration)
            if not isinstance(credential, ResolvedProviderCredential):
                _unavailable()
        lookup = lookup_request(registration, captured, credential, purpose="invocation")
        observation = await client.lookup(
            target=credential.target, bearer=credential.bearer, request=lookup,
            limits=credential.limits,
        )
        if not isinstance(observation, HttpIdentityObservation):
            _unavailable()
        async with identity_fence.mutation(factory, valkey) as writer:
            async with writer.begin():
                current, _ = await capture_source(
                    writer, registration, source, writer_agent=captured.agent_id,
                )
                if current != captured:
                    _unavailable()
                source_facts(observation.response_bytes, lookup, credential)
                application = await principal_authority.apply_http_principal(
                    writer, capture=captured.authority, request=lookup,
                    response_bytes=observation.response_bytes,
                )
                await writer.commit()
        # Leaving the writer successfully acknowledges stable settlement as well as COMMIT.
    except HTTPException:
        raise
    except Exception:  # noqa: BLE001 - authority failures carry no provider or credential data.
        _unavailable()
    async with factory() as session, session.begin():
        try:
            await identity_fence.lock_identity_reader(session)
            current, binding = await capture_source(session, registration, source)
            if (
                current.fingerprint != captured.fingerprint
                or current.authority.lifecycle_generation != application.lifecycle_generation
                or request.app.state.http_identity_runtime is not runtime
                or request.app.state.http_identity_client is not client
            ):
                _unavailable()
            facts = source_facts(observation.response_bytes, lookup, credential)
            if application.principal_id is not None:
                principal = await session.get(Principal, application.principal_id)
                if (
                    principal is None or str(principal.tenant_id) != registration.tenant_id
                    or facts.identity is None or principal.idp_issuer != facts.identity.issuer
                    or principal.idp_subject != facts.identity.subject or principal.type != "human"
                    or principal.status != facts.identity.account_status
                    or principal.authorization_version != application.authorization_version
                ):
                    _unavailable()
            source_permission(
                current, binding, facts,
                current.sender.native_id in registration.human_native_ids, original,
            )
            stable = await valkey.get(identity_fence.FENCE_KEY)
            if isinstance(stable, bytes):
                stable = stable.decode("ascii")
            if type(stable) is not str or not stable.startswith("stable:"):
                _unavailable()
            uuid.UUID(stable.removeprefix("stable:"))
            validity = identity_delivery_window(facts, credential.limits)
            principal_ref = None
            if application.principal_id is not None:
                assert application.authorization_version is not None
                principal_ref = PrincipalRef(
                    principal_id=str(application.principal_id),
                    authorization_version=application.authorization_version,
                )
            live = CurrentHttpInvocation(
                binding, session, intent_digest,
                principal_ref, runtime, registration, current, facts, application, stable, validity,
                observation.response_bytes, lookup, credential, source, client, original,
                _sealable=original is None,
            )
        except HTTPException:
            raise
        except Exception:  # noqa: BLE001 - fail closed without disclosing authority inputs.
            _unavailable()
        try:
            yield live
        finally:
            object.__setattr__(live, "_open", False)


def turn_authority_response(
    check: TurnAuthorityRequestV1,
    decision: Literal["continue", "refuse", "unavailable"],
    reason: Literal[
        "current_authority", "caller_not_allowed", "invalid_context", "unsupported_resume",
        "authority_unavailable",
    ],
) -> TurnAuthorityResponseV1:
    """@spec HTTP-INVOCATION-EXECUTION-8/16."""
    return TurnAuthorityResponseV1(
        schema_version=1, request_id=check.request_id, stage=check.stage,
        event_id=check.event_id,
        context_sha256=hashlib.sha256(check.context.encode("ascii")).hexdigest(),
        scope=check.scope, decision=decision, reason=reason,
    )


def invocation_source(
    runtime: HttpInvocationRuntime, context: codec.InvocationContextV1 | OriginalAssociation,
) -> tuple[ReceiverRegistrationV1, AuthenticatedSource]:
    """Authenticated source scope only. @spec HTTP-APPROVAL-CONTINUITY-4/9."""
    scope = context.scope
    registration = runtime._receivers.get((context.receiver_id, context.receiver_generation))
    if (
        registration is None or registration.tenant_id != scope.tenant_id
        or scope.binding_id not in registration.binding_ids or scope.provider != "slack"
        or registration.channel_identity_id != scope.channel_identity_id
        or registration.provider_installation_id != scope.provider_installation_id
        or registration.namespace.model_dump() != scope.namespace.model_dump()
        or context.independent_human != (scope.native_id in registration.human_native_ids)
    ):
        _invalid_receiver()
    source = AuthenticatedSource(
        scope.binding_id, scope.binding_generation,
        SenderV1(
            provider=scope.provider, channel_identity_id=scope.channel_identity_id,
            provider_installation_id=scope.provider_installation_id,
            namespace=registration.namespace, native_id=scope.native_id,
        ),
        agent_id=scope.agent_id,
    )
    return registration, source


async def check_http_turn_authority(
    request: Request, check: TurnAuthorityRequestV1,
) -> TurnAuthorityResponseV1:
    """@spec HTTP-INVOCATION-EXECUTION-5/8/16."""
    runtime = getattr(request.app.state, "http_identity_runtime", None)
    client = getattr(request.app.state, "http_identity_client", None)
    if type(runtime) is not HttpInvocationRuntime or not isinstance(client, HttpIdentityClient):
        return turn_authority_response(check, "unavailable", "authority_unavailable")
    context = codec.open_invocation_context(check.context, runtime._context_keys)
    if not isinstance(context, codec.InvocationContextV1):
        return turn_authority_response(check, "refuse", "invalid_context")
    scope = context.scope
    if (
        check.payload_sha256 != context.payload_sha256 or check.event_id != context.event_id
        or check.scope.tenant_id != scope.tenant_id or check.scope.agent_id != scope.agent_id
        or check.scope.binding_id != scope.binding_id
        or check.scope.binding_generation != scope.binding_generation
    ):
        return turn_authority_response(check, "refuse", "invalid_context")
    try:
        registration, source = invocation_source(runtime, context)
    except HTTPException:
        return turn_authority_response(check, "refuse", "invalid_context")
    if check.approval_id is not None:
        return turn_authority_response(check, "refuse", "unsupported_resume")
    try:
        async with current_source_authority(
            request, runtime=runtime, client=client, registration=registration,
            source=source, original=context,
        ):
            result = turn_authority_response(check, "continue", "current_authority")
        return result
    except HTTPException as error:
        if error.status_code == 401:
            return turn_authority_response(check, "refuse", "invalid_context")
        if error.status_code == 403:
            return turn_authority_response(check, "refuse", "caller_not_allowed")
        return turn_authority_response(check, "unavailable", "authority_unavailable")
    except (SQLAlchemyError, RedisError, identity_fence.IdentityWriterUnavailable):
        return turn_authority_response(check, "unavailable", "authority_unavailable")


@asynccontextmanager
async def current_http_approval_caller(
    request: Request, context: codec.InvocationContextV1,
) -> AsyncIterator[CurrentHttpInvocation]:
    """Read-only preparation for the separate insert owner. @spec HTTP-APPROVAL-CONTINUITY-9."""
    runtime = getattr(request.app.state, "http_identity_runtime", None)
    client = getattr(request.app.state, "http_identity_client", None)
    if type(runtime) is not HttpInvocationRuntime or not isinstance(client, HttpIdentityClient):
        _unavailable()
    registration, source = invocation_source(runtime, context)
    async with current_source_authority(
        request, runtime=runtime, client=client, registration=registration,
        source=source, original=context,
    ) as caller:
        yield caller


async def revalidate_http_approval_caller(
    request: Request, writer: AsyncSession, caller: CurrentHttpInvocation,
) -> None:
    """No external I/O; only the separate borrowed writer. @spec HTTP-APPROVAL-CONTINUITY-9."""
    if caller._open or not writer.in_transaction():
        _unavailable()
    if (
        request.app.state.http_identity_runtime is not caller._runtime
        or request.app.state.http_identity_client is not caller._client
    ):
        _unavailable()
    current, binding = await capture_source(
        writer, caller._registration, caller._source, writer_agent=caller._capture.agent_id,
    )
    if current != caller._capture:
        _unavailable()
    facts = source_facts(caller._response_bytes, caller._lookup, caller._credential)
    if caller._application.principal_id is not None:
        principal = await writer.get(
            Principal, caller._application.principal_id, populate_existing=True,
        )
        if (
            principal is None or facts.identity is None or principal.type != "human"
            or principal.tenant_id != uuid.UUID(caller._registration.tenant_id)
            or principal.idp_issuer != facts.identity.issuer
            or principal.idp_subject != facts.identity.subject
            or principal.status != facts.identity.account_status
            or principal.authorization_version != caller._application.authorization_version
        ):
            _unavailable()
    source_permission(
        current, binding, facts,
        current.sender.native_id in caller._registration.human_native_ids, caller._original,
    )
