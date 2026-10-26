"""Boundary observations. @spec HTTP-INVOCATION-EXECUTION-8/16."""

from __future__ import annotations

from typing import Annotated, Literal, Never, Self

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, model_validator

from curie_internal.identity_context import (
    CanonicalUUID,
    ChannelGeneration,
    Digest,
    Identifier,
    Version,
)

Stage = Literal["prepare", "claim_or_resume", "start", "steer"]


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


class TurnAuthorityResponseV1(_Observation):
    """@spec HTTP-INVOCATION-EXECUTION-16."""

    schema_version: Version
    request_id: CanonicalUUID
    stage: Stage
    event_id: Identifier
    context_sha256: Digest
    scope: TurnAuthorityScopeV1
    decision: Literal["continue", "refuse", "unavailable"]
    reason: Literal[
        "current_authority", "caller_not_allowed", "invalid_context", "unsupported_resume",
        "authority_unavailable",
    ]

    @model_validator(mode="after")
    def coherent_decision(self) -> Self:
        """@spec HTTP-INVOCATION-EXECUTION-16."""
        reasons = {
            "continue": {"current_authority"},
            "refuse": {"caller_not_allowed", "invalid_context", "unsupported_resume"},
            "unavailable": {"authority_unavailable"},
        }
        if self.reason not in reasons[self.decision]:
            raise ValueError("invalid_turn_authority_decision")
        return self
