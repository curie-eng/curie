"""Boundary observations. @spec HTTP-INVOCATION-EXECUTION-8/16.

Version 2 of the current-check answer adds the verified original requester for
an opted-in agent. @spec CONNECTOR-REQUESTER-1.
"""

from __future__ import annotations

from typing import Annotated, Literal, Never, Self

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, StrictBool, model_validator

from curie_internal.identity_context import (
    CanonicalUUID,
    ChannelGeneration,
    Digest,
    Identifier,
    Issuer,
    Version,
)

Stage = Literal["prepare", "claim_or_resume", "start", "steer"]

# How long the dispatcher receiver and the worker turn-authority client wait for
# the API's current-authority answer. The API derives its composition deadline
# from this value so that it always gives up before its callers do.
IDENTITY_CALLER_WAIT_SECONDS = 5.0


def _ascii_context(value: object) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-16."""
    if type(value) is not str or not 1 <= len(value) <= 16_384:
        raise ValueError("invalid_turn_authority_context")
    value.encode("ascii")
    return value


class _Observation(BaseModel):
    """@spec HTTP-INVOCATION-EXECUTION-16."""

    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", revalidate_instances="always",
    )

    def __bool__(self) -> Never:
        """@spec HTTP-INVOCATION-EXECUTION-16."""
        raise TypeError("turn_authority_is_not_offline_permission")


class TurnAuthorityScopeV1(_Observation):
    """@spec HTTP-INVOCATION-EXECUTION-16."""

    tenant_id: CanonicalUUID
    agent_id: CanonicalUUID
    binding_id: CanonicalUUID
    binding_generation: ChannelGeneration


class TurnAuthorityRequestV1(_Observation):
    """@spec HTTP-INVOCATION-EXECUTION-16."""

    schema_version: Version
    request_id: CanonicalUUID
    stage: Stage
    context: Annotated[str, BeforeValidator(_ascii_context)] = Field(repr=False)
    payload_sha256: Digest
    event_id: Identifier
    scope: TurnAuthorityScopeV1
    approval_id: CanonicalUUID | None


Decision = Literal["continue", "refuse", "unavailable"]
Reason = Literal[
    "current_authority", "caller_not_allowed", "invalid_context", "unsupported_resume",
    "authority_unavailable",
]
_REASONS: dict[str, frozenset[str]] = {
    "continue": frozenset({"current_authority"}),
    "refuse": frozenset({"caller_not_allowed", "invalid_context", "unsupported_resume"}),
    "unavailable": frozenset({"authority_unavailable"}),
}


def _version_two(value: object) -> int:
    """No reader guesses between versions. @spec CONNECTOR-REQUESTER-1."""
    if type(value) is not int or value != 2:
        raise ValueError("invalid_turn_authority_version")
    return value


class TurnAuthorityResponseV1(_Observation):
    """@spec HTTP-INVOCATION-EXECUTION-16."""

    schema_version: Version
    request_id: CanonicalUUID
    stage: Stage
    event_id: Identifier
    context_sha256: Digest
    scope: TurnAuthorityScopeV1
    decision: Decision
    reason: Reason

    @model_validator(mode="after")
    def coherent_decision(self) -> Self:
        """@spec HTTP-INVOCATION-EXECUTION-16."""
        if self.reason not in _REASONS[self.decision]:
            raise ValueError("invalid_turn_authority_decision")
        return self


class TurnRequesterV1(_Observation):
    """The verified original requester: tenant and canonical issuer and subject only.

    Compared exactly; never normalized. @spec CONNECTOR-REQUESTER-1.
    """

    tenant_id: CanonicalUUID
    issuer: Issuer
    subject: Identifier


class TurnAuthorityResponseV2(_Observation):
    """Every version 1 field plus the required-nullable requester projection.

    The API answers version 2 only for a continue on an agent that opted in to
    connector requester identity; a null requester means no verified human. A
    requester is bound to this answer's own checked scope and never rides a
    refused or unavailable answer. @spec CONNECTOR-REQUESTER-1.
    """

    schema_version: Annotated[Literal[2], BeforeValidator(_version_two)]
    request_id: CanonicalUUID
    stage: Stage
    event_id: Identifier
    context_sha256: Digest
    scope: TurnAuthorityScopeV1
    decision: Decision
    reason: Reason
    requester: TurnRequesterV1 | None

    @model_validator(mode="after")
    def coherent_projection(self) -> Self:
        """@spec CONNECTOR-REQUESTER-1 HTTP-INVOCATION-EXECUTION-16."""
        if self.reason not in _REASONS[self.decision]:
            raise ValueError("invalid_turn_authority_decision")
        requester = self.requester
        if requester is not None and (
            self.decision != "continue" or requester.tenant_id != self.scope.tenant_id
        ):
            raise ValueError("invalid_turn_authority_requester")
        return self


class BindingClassificationRequestV1(_Observation):
    """@spec HTTP-INVOCATION-EXECUTION-22."""

    schema_version: Version
    request_id: CanonicalUUID
    scope: TurnAuthorityScopeV1


class BindingClassificationResponseV1(_Observation):
    """Classification, not permission. @spec HTTP-INVOCATION-EXECUTION-22."""

    schema_version: Version
    request_id: CanonicalUUID
    scope: TurnAuthorityScopeV1
    http_identity_required: StrictBool
