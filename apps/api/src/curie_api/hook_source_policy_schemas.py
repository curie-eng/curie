"""@spec PROTECTED-HOOK-SOURCE-3."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal, Self

from curie_protected_hooks.source_policy_records import (
    canonical_bundle_digest,
    canonical_decimal,
    canonical_hook,
    canonical_uuid,
)
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, field_validator, model_validator

SourceDecimal = Annotated[str, AfterValidator(canonical_decimal)]
SourceUuid = Annotated[str, AfterValidator(canonical_uuid)]
SourceHook = Annotated[str, AfterValidator(canonical_hook)]
SourceDigest = Annotated[str, AfterValidator(canonical_bundle_digest)]


class _SourceModel(BaseModel):
    """@spec PROTECTED-HOOK-SOURCE-3."""

    model_config = ConfigDict(strict=True, extra="forbid")


class HookSourcePolicyMutation(_SourceModel):
    """@spec PROTECTED-HOOK-SOURCE-3."""

    expected_generation: SourceDecimal
    operation_id: SourceUuid


class HookSourcePolicyWrite(HookSourcePolicyMutation):
    """@spec PROTECTED-HOOK-SOURCE-3."""

    runtime_id: SourceUuid
    qualification_id: SourceUuid
    bundle_digest: SourceDigest


class HookSourcePolicyOut(_SourceModel):
    """@spec PROTECTED-HOOK-SOURCE-3."""

    agent_id: SourceUuid
    hook: SourceHook
    generation: SourceDecimal
    mode: Literal["ordinary", "protected"]
    tool_access: Literal["read-only"] | None
    runtime_id: SourceUuid | None
    qualification_id: SourceUuid | None
    bundle_digest: SourceDigest | None
    legacy_generation: SourceDecimal
    activation: Literal["closed", "active"]
    updated_at: datetime | None
    refusal_reason: Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]{0,62}$")] | None = None

    @field_validator("updated_at")
    @classmethod
    def _utc(cls, value: datetime | None) -> datetime | None:
        """@spec PROTECTED-HOOK-SOURCE-3."""
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("invalid_audit_time")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _policy_shape(self) -> Self:
        """@spec PROTECTED-HOOK-SOURCE-3."""
        references = (self.tool_access, self.runtime_id, self.qualification_id, self.bundle_digest)
        if self.mode == "ordinary":
            if any(value is not None for value in references):
                raise ValueError("invalid_ordinary_policy")
        elif any(value is None for value in references):
            raise ValueError("invalid_protected_policy")
        if self.generation == "0":
            if (
                self.mode != "ordinary"
                or self.updated_at is not None
                or self.activation != "closed"
            ):
                raise ValueError("invalid_absent_policy")
        elif self.updated_at is None:
            raise ValueError("invalid_audit_time")
        return self


class HookSourceSecretOut(_SourceModel):
    """@spec PROTECTED-HOOK-SOURCE-3."""

    agent_id: SourceUuid
    hook: SourceHook
    generation: SourceDecimal
    secret: Annotated[str, Field(min_length=1, repr=False)]

    @field_validator("generation")
    @classmethod
    def _positive(cls, value: str) -> str:
        """@spec PROTECTED-HOOK-SOURCE-3."""
        return canonical_decimal(value, positive=True)
