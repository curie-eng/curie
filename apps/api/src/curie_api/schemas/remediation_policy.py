"""Request and response bodies of the remediation policy routes.

Shapes follow the source policy routes (``curie_api.hook_source_policy_schemas``):
generations travel as canonical decimal strings, ``expected_generation`` ``"0"``
binds a hook with no policy, and a refusal is ``{"detail": {"code": ...}}``. The
policy document itself is carried as a JSON object and validated by
``curie_api.remediation_policy_document`` so that every refusal is a named code
rather than FastAPI's validation list.

@spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field

from ..hook_source_policy_schemas import SourceDecimal, SourceHook, SourceUuid

_CODE = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]{0,62}$")]


class RemediationModel(BaseModel):
    """@spec AUTOMATED-REMEDIATION-2."""

    model_config = ConfigDict(strict=True, extra="forbid")


class RemediationPolicyMutation(RemediationModel):
    """Compare and swap plus idempotency for arm, disarm and removal.

    @spec AUTOMATED-REMEDIATION-2.
    """

    expected_generation: SourceDecimal
    operation_id: SourceUuid


class RemediationPolicyWrite(RemediationPolicyMutation):
    """Bind or replace the policy document of a protected hook.

    @spec AUTOMATED-REMEDIATION-2.
    """

    policy: dict[str, Any] = Field(
        description=(
            "The closed policy document: route, limits and actions "
            "(AUTOMATED-REMEDIATION-2). Unknown keys are refused."
        )
    )


class RemediationPolicyOut(RemediationModel):
    """One committed remediation policy generation.

    @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3.
    """

    agent_id: SourceUuid
    hook: SourceHook
    generation: SourceDecimal
    armed: bool
    active: bool
    bound_by: str = Field(description="The operator principal that wrote this generation.")
    policy: dict[str, Any]
    updated_at: datetime


class RemediationPolicyRefusalDetail(RemediationModel):
    """A stable refusal code and, where one applies, the offending path.

    @spec AUTOMATED-REMEDIATION-2.
    """

    code: _CODE
    path: str | None = None
    message: str | None = None


class RemediationPolicyRefusal(RemediationModel):
    """@spec AUTOMATED-REMEDIATION-2."""

    detail: RemediationPolicyRefusalDetail


class RemediationBreakerClose(RemediationModel):
    """Why an operator closes a breaker; exactly ``{"reason"}``.

    @spec AUTOMATED-REMEDIATION-11.
    """

    reason: str = Field(
        min_length=1,
        max_length=1000,
        pattern=r"\S",
        description="Why the breaker is closed, recorded with the operator principal.",
    )


class RemediationBreakerOut(RemediationModel):
    """One breaker on an agent's connector, tool and target key.

    @spec AUTOMATED-REMEDIATION-11.
    """

    id: SourceUuid
    agent_id: SourceUuid
    connector: str
    tool: str
    target: str = Field(description="The target key: the connector and the canonical target.")
    opened_at: datetime
    closed_at: datetime | None
    closed_by: str | None = Field(description="The operator principal that closed it.")
    close_reason: str | None
