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
from typing import Annotated, Any, Never, Self, SupportsIndex

from aci_protocol import PrincipalRef
from curie_internal import identity_context as codec
from fastapi import HTTPException, Request
from pydantic import Field, model_validator
from sqlalchemy import select
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
from curie_api.identity.slack import AUTH_TEST_KEY, SlackEvidence, derive
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
        """@spec HTTP-INVOCATION-EXECUTION-15."""
        namespace = self.namespace
        if (
            namespace.authority != "" or namespace.kind != "slack_workspace"
            or namespace.key != self.installation_account
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


@dataclass(frozen=True)
class _Capture:
    """@spec HTTP-INVOCATION-EXECUTION-4/15."""

    authority: principal_authority.AuthorityCapture
    agent_id: uuid.UUID
    fingerprint: str = field(repr=False)
    sender: SenderV1
    policy: AgentRolePolicyV1 | None
    policy_version: int | None
    retained_group: tuple[str, str] | None


async def _capture(
    session: AsyncSession,
    registration: ReceiverRegistrationV1,
    proof: codec.ReceiverProofV1,
    claims: channel_token.ChannelClaims,
    body: Any,
    *,
    writer_agent: uuid.UUID | None = None,
) -> tuple[_Capture, AgentChannel]:
    """@spec HTTP-INVOCATION-EXECUTION-2/4/5/15."""
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
    binding_query = select(AgentChannel).where(AgentChannel.id == uuid.UUID(claims.channel_id))
    if writer_agent is not None:
        binding_query = binding_query.with_for_update()
    binding = await session.scalar(binding_query.execution_options(populate_existing=True))
    if (
        binding is None or str(binding.id) not in registration.binding_ids
        or binding.tenant_id != tenant_id or binding.generation != claims.generation
        or proof.binding_id != claims.channel_id or proof.binding_generation != claims.generation
        or binding.kind != "slack" or body.kind != binding.kind
        or body.address != binding.address or proof.native_channel_id != binding.address
        or (binding.adapter or "default") != registration.channel_identity_name
        or binding.endpoint is not None
        or body.delivery_id != proof.delivery_id or body.conversation_id != proof.conversation_id
        or proof.channel_identity_id != registration.channel_identity_id
        or proof.provider_installation_id != registration.provider_installation_id
        or proof.slack.delivery != "app_mention" or proof.slack.is_ext_shared_channel is not False
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
    # This evidence comes from the independently registered receiver's connection,
    # never the platform-editable identity report.
    evidence = SlackEvidence.model_validate(proof.slack.model_dump(mode="json"))
    sender = derive(
        evidence,
        identity_attributes={AUTH_TEST_KEY: {
            "team_id": registration.installation_account, "user_id": registration.bot_user_id,
            "enterprise_id": None, "is_enterprise_install": False,
        }},
        installation_authority=installation.authority,
        installation_account=installation.external_account_id,
    )
    if (
        not isinstance(sender, tuple) or sender[0] != namespace.key
        or sender[1] != body.author
    ):
        _invalid_receiver()
    source = SenderV1(
        provider="slack", channel_identity_id=str(identity.id),
        provider_installation_id=str(installation.id), namespace=registration.namespace,
        native_id=sender[1],
    )
    role = None if policy is None else policy.roles.invocation
    team_id = None if policy_row is None else policy_row.invocation_team_id
    group = None
    team_state = None
    if team_id is not None and (role is None or role.legacy_state != "migrated"):
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
    return _Capture(
        principal_authority.AuthorityCapture(config, idp.lifecycle_generation), agent.id,
        fingerprint, source, policy, None if policy_row is None else policy_row.version, group,
    ), binding


def _facts(
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


def _permission(
    capture: _Capture, binding: AgentChannel, facts: ValidatedFacts, independent_human: bool,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-5/15."""
    if not admission.admit(binding, [capture.sender.native_id]).allowed:
        _deny()
    role = None if capture.policy is None else capture.policy.roles.invocation
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
                role="invocation", request=facts.request,
            ),
            now=datetime.now(UTC),
        )
        if result.status != "match":
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
    _capture: _Capture = field(repr=False)
    _facts: ValidatedFacts = field(repr=False)
    _application: principal_authority.PrincipalApplication = field(repr=False)
    _stable: str = field(repr=False)
    _validity: DeliveryValidityWindow = field(repr=False)
    _open: bool = field(default=True, repr=False)

    def seal_payload(
        self, *, payload: str, event_id: str, original_invocation_id: str,
        reservation: BacklogReservation, receipt_key: str,
    ) -> tuple[DeliveryIdentityContext, DeliveryEnqueueGuard]:
        """@spec HTTP-INVOCATION-EXECUTION-6/15."""
        if not self._open or not self.session.in_transaction():
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
    factory = request.app.state.sessionmaker
    valkey = request.app.state.valkey
    try:
        async with factory() as session, session.begin():
            await identity_fence.lock_identity_reader(session)
            captured, _ = await _capture(session, registration, proof, claims, body)
            credential = runtime._providers.resolve(captured.authority.configuration)
            if not isinstance(credential, ResolvedProviderCredential):
                _unavailable()
        lookup = LookupRequestV1(
            schema_version=1, request_id=str(uuid.uuid4()), purpose="invocation",
            authority_version=captured.authority.configuration.version,
            scope=ScopeV1(
                tenant_id=registration.tenant_id, expected_issuer=credential.target.expected_issuer,
                sender=captured.sender,
            ),
        )
        observation = await client.lookup(
            target=credential.target, bearer=credential.bearer, request=lookup,
            limits=credential.limits,
        )
        if not isinstance(observation, HttpIdentityObservation):
            _unavailable()
        async with identity_fence.mutation(factory, valkey) as writer:
            async with writer.begin():
                current, _ = await _capture(
                    writer, registration, proof, claims, body, writer_agent=captured.agent_id,
                )
                if current != captured:
                    _unavailable()
                _facts(observation.response_bytes, lookup, credential)
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
            current, binding = await _capture(session, registration, proof, claims, body)
            if (
                current.fingerprint != captured.fingerprint
                or current.authority.lifecycle_generation != application.lifecycle_generation
                or request.app.state.http_identity_runtime is not runtime
                or request.app.state.http_identity_client is not client
            ):
                _unavailable()
            facts = _facts(observation.response_bytes, lookup, credential)
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
            _permission(
                current, binding, facts,
                current.sender.native_id in registration.human_native_ids,
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
                binding, session, hashlib.sha256(receiver_proof.encode("ascii")).hexdigest(),
                principal_ref, runtime, registration, current, facts, application, stable, validity,
            )
        except HTTPException:
            raise
        except Exception:  # noqa: BLE001 - fail closed without disclosing authority inputs.
            _unavailable()
        try:
            yield live
        finally:
            object.__setattr__(live, "_open", False)
