"""Private continuity, never a permit. @spec HTTP-APPROVAL-CONTINUITY-2/3/6/7/12."""

from __future__ import annotations

import json
from typing import Annotated, Any, Literal, Never, Self

from aci_protocol import ApprovalRequest
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, model_validator

from curie_internal import identity_context as codec
from curie_internal.identity_authority import (
    TurnAuthorityRequestV1,
    TurnAuthorityResponseV1,
    TurnAuthorityScopeV1,
)


class ApprovalWireModel(BaseModel):
    """@spec HTTP-APPROVAL-CONTINUITY-2."""

    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", revalidate_instances="always",
    )

    def __bool__(self) -> Never:
        """@spec HTTP-APPROVAL-CONTINUITY-2."""
        raise TypeError("approval_provenance_is_not_permission")


def _context(value: object) -> str:
    """@spec HTTP-APPROVAL-CONTINUITY-2."""
    if type(value) is not str or not 1 <= len(value) <= 16_384:
        raise ValueError("invalid_approval_context")
    value.encode("ascii")
    return value


def _payload(value: object) -> str:
    """@spec HTTP-APPROVAL-CONTINUITY-2/4."""
    if type(value) is not str or not 1 <= len(value.encode("utf-8")) <= 262_144:
        raise ValueError("invalid_approval_payload")
    return value


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-2/12."""
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError("invalid_approval_json")
        result[key] = value
    return result


def _constant(value: str) -> Never:
    """@spec HTTP-APPROVAL-CONTINUITY-2/12."""
    raise ValueError("invalid_approval_json")


def _arguments(value: object) -> str:
    """@spec HTTP-APPROVAL-CONTINUITY-12."""
    try:
        if type(value) is not str or len(value.encode("utf-8")) > 16_384:
            raise ValueError
        parsed = json.loads(value, object_pairs_hook=_pairs, parse_constant=_constant)
        if type(parsed) is not dict:
            raise ValueError
        canonical = json.dumps(
            parsed, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False,
        )
        canonical.encode("utf-8")
        if canonical != value:
            raise ValueError
        return value
    except (ValueError, TypeError, RecursionError):
        raise ValueError("invalid_approval_arguments") from None


class ApprovalCreationIdentityV1(ApprovalWireModel):
    """@spec HTTP-APPROVAL-CONTINUITY-3/4."""

    schema_version: codec.Version
    context: Annotated[str, BeforeValidator(_context)] = Field(repr=False)
    payload: Annotated[str, BeforeValidator(_payload)] = Field(repr=False)
    scope: TurnAuthorityScopeV1


class ApprovalCreationRequestV1(ApprovalWireModel):
    """@spec HTTP-APPROVAL-CONTINUITY-3."""

    schema_version: codec.Version
    request_id: codec.CanonicalUUID
    approval: ApprovalRequest = Field(repr=False)
    identity: ApprovalCreationIdentityV1 = Field(repr=False)


class ApprovalCreationResponseV1(ApprovalWireModel):
    """@spec HTTP-APPROVAL-CONTINUITY-3."""

    schema_version: codec.Version
    request_id: codec.CanonicalUUID
    id: codec.CanonicalUUID
    status: Literal["pending", "approved", "rejected", "expired"]
    duplicate: bool
    requested_by: codec.Identifier | None


class ApprovalActorScopeV1(ApprovalWireModel):
    """@spec HTTP-APPROVAL-CONTINUITY-6."""

    tenant_id: codec.CanonicalUUID
    channel_identity_id: codec.CanonicalUUID
    provider_installation_id: codec.CanonicalUUID
    provider: codec.Identifier
    native_id: codec.Identifier
    actor_channel: codec.Identifier
    namespace: codec.ContextNamespaceV1


class ApprovalContinuityV1(ApprovalWireModel):
    """@spec HTTP-APPROVAL-CONTINUITY-6."""

    schema_version: codec.Version
    key_id: codec.KeyId
    key_generation: codec.Generation
    approval_id: codec.CanonicalUUID
    original_event_id: codec.Identifier
    source_event_id: codec.Identifier
    conversation_id: codec.Conversation
    creation_request_sha256: codec.Digest
    source_context_sha256: codec.Digest
    source_payload_sha256: codec.Digest
    original_payload_sha256: codec.Digest
    approval_record_sha256: codec.Digest
    scope: codec.InvocationScopeV1
    receiver_id: codec.CanonicalUUID
    receiver_generation: codec.Generation
    independent_human: bool
    provider_issuer: codec.Issuer
    provider_version: codec.Generation
    provider_credential_id: codec.Identifier
    provider_credential_generation: codec.Generation
    lifecycle_generation: codec.Generation
    role_policy_version: codec.Generation
    outcome: Literal["resolved", "inactive", "unresolved"]
    canonical: codec.ContextCanonicalV1 | None
    principal: codec.ContextPrincipalV1 | None
    status: Literal["pending", "approved", "rejected", "expired"]
    actor_scope: ApprovalActorScopeV1 | None
    actor_receiver_id: codec.CanonicalUUID | None
    actor_receiver_generation: codec.Generation | None
    actor_independent_human: bool | None
    actor_provider_issuer: codec.Issuer | None
    actor_provider_version: codec.Generation | None
    actor_provider_credential_id: codec.Identifier | None
    actor_provider_credential_generation: codec.Generation | None
    actor_lifecycle_generation: codec.Generation | None
    actor_role_policy_version: codec.Generation | None
    actor_outcome: Literal["resolved", "inactive", "unresolved"] | None
    actor_canonical: codec.ContextCanonicalV1 | None
    actor_principal: codec.ContextPrincipalV1 | None

    @model_validator(mode="after")
    def coherent_associations(self) -> Self:
        """@spec HTTP-APPROVAL-CONTINUITY-6."""
        if self.outcome == "unresolved":
            if self.canonical is not None or self.principal is not None:
                raise ValueError("invalid_approval_association")
        elif self.canonical is None:
            raise ValueError("invalid_approval_association")
        actor_names = tuple(name for name in type(self).model_fields if name.startswith("actor_"))
        if self.status in {"pending", "expired"}:
            if any(getattr(self, name) is not None for name in actor_names):
                raise ValueError("invalid_approval_actor")
        else:
            if any(
                getattr(self, name) is None for name in actor_names
                if name not in {"actor_canonical", "actor_principal"}
            ):
                raise ValueError("invalid_approval_actor")
            if self.actor_scope is None or self.actor_scope.tenant_id != self.scope.tenant_id:
                raise ValueError("invalid_approval_actor")
            if self.actor_outcome == "unresolved":
                if self.actor_canonical is not None or self.actor_principal is not None:
                    raise ValueError("invalid_approval_actor")
            elif self.actor_canonical is None:
                raise ValueError("invalid_approval_actor")
        return self


class ApprovalContextV1(ApprovalContinuityV1):
    """@spec HTTP-APPROVAL-CONTINUITY-6."""

    purpose: Literal["approval"]


class ResumeContextV1(ApprovalContinuityV1):
    """@spec HTTP-APPROVAL-CONTINUITY-6."""

    purpose: Literal["resume"]
    status: Literal["approved", "rejected", "expired"]
    event_id: codec.Identifier
    payload_sha256: codec.Digest

    @model_validator(mode="after")
    def exact_resume_event(self) -> Self:
        """@spec HTTP-APPROVAL-CONTINUITY-6."""
        if self.event_id != f"approval-{self.approval_id}-resolved":
            raise ValueError("invalid_approval_resume")
        return self


class ApprovalAnswerProofV1(ApprovalWireModel):
    """@spec HTTP-APPROVAL-CONTINUITY-7."""

    schema_version: codec.Version
    purpose: Literal["approval_answer"]
    operation: Literal["resolve", "recover"]
    receiver_id: codec.CanonicalUUID
    receiver_generation: codec.Generation
    channel_identity_id: codec.CanonicalUUID
    provider_installation_id: codec.CanonicalUUID
    approval_id: codec.CanonicalUUID
    body_sha256: codec.Digest
    attestation_sha256: codec.Digest
    native_channel_id: codec.Identifier
    slack: codec.ReceiverSlackEvidenceV1


class ApprovalGrantExpectationV1(ApprovalWireModel):
    """@spec HTTP-APPROVAL-CONTINUITY-12."""

    approval_id: codec.CanonicalUUID
    agent_id: codec.CanonicalUUID
    conversation_id: codec.Conversation
    gate_kind: Literal["permission", "policy"]
    granted_tool: codec.Identifier | None
    granted_arguments_json: Annotated[str, BeforeValidator(_arguments)] | None = Field(repr=False)


class ApprovalGrantAuthorityRequestV1(ApprovalWireModel):
    """@spec HTTP-APPROVAL-CONTINUITY-12."""

    schema_version: codec.Version
    check: TurnAuthorityRequestV1
    grant: ApprovalGrantExpectationV1

    @model_validator(mode="after")
    def coherent_grant(self) -> Self:
        """@spec HTTP-APPROVAL-CONTINUITY-12."""
        if self.check.stage != "prepare" or self.check.approval_id != self.grant.approval_id:
            raise ValueError("invalid_approval_grant_scope")
        return self


class ApprovalGrantAuthorityResponseV1(ApprovalWireModel):
    """@spec HTTP-APPROVAL-CONTINUITY-12."""

    schema_version: codec.Version
    check: TurnAuthorityResponseV1
    grant_sha256: codec.Digest


def seal_approval_context(value: ApprovalContextV1, keyring: codec.IdentityKeyring) -> str:
    """@spec HTTP-APPROVAL-CONTINUITY-6."""
    return codec.seal_approval_context(value, keyring)


def open_approval_context(
    token: str, keyring: codec.IdentityKeyring,
) -> ApprovalContextV1 | codec.ContextRefusal:
    """@spec HTTP-APPROVAL-CONTINUITY-6."""
    return codec.open_approval_context(token, keyring)


def seal_resume_context(value: ResumeContextV1, keyring: codec.IdentityKeyring) -> str:
    """@spec HTTP-APPROVAL-CONTINUITY-6."""
    return codec.seal_resume_context(value, keyring)


def open_resume_context(
    token: str, keyring: codec.IdentityKeyring,
) -> ResumeContextV1 | codec.ContextRefusal:
    """@spec HTTP-APPROVAL-CONTINUITY-6."""
    return codec.open_resume_context(token, keyring)


def sign_approval_answer_proof(value: ApprovalAnswerProofV1, keyring: codec.IdentityKeyring) -> str:
    """@spec HTTP-APPROVAL-CONTINUITY-7."""
    return codec.sign_approval_answer_proof(value, keyring)


def verify_approval_answer_proof(
    token: str, keyring: codec.IdentityKeyring,
) -> ApprovalAnswerProofV1 | codec.ContextRefusal:
    """@spec HTTP-APPROVAL-CONTINUITY-7."""
    return codec.verify_approval_answer_proof(token, keyring)
