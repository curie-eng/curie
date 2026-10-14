"""Request and response bodies of the remediation qualification routes.

Shapes follow the remediation policy routes: identifiers and generations travel
as canonical strings, bodies are closed (``extra="forbid"``), and a refusal is
``{"detail": {"code": ...}}``. The evidence references and the worst case
statement are carried as given and checked by
``curie_api.remediation_qualifications`` so that every refusal is a named code.
The verifier-run body is exactly ``{"hook", "action", "target"}``: no tool,
argument, connector or verifier field is accepted.

@spec AUTOMATED-REMEDIATION-22
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import Field, StrictBool, StrictFloat, StrictInt, StrictStr

from ..hook_source_policy_schemas import SourceDecimal, SourceHook, SourceUuid
from .remediation_policy import RemediationModel

# A literal member of an action's ``target.allowed``: a JSON scalar.
TargetValue = StrictStr | StrictInt | StrictFloat | StrictBool


class RemediationQualificationWrite(RemediationModel):
    """One qualification record: the declaration it qualifies and its evidence.

    @spec AUTOMATED-REMEDIATION-22.
    """

    hook: SourceHook
    action: str = Field(min_length=1, max_length=128)
    generation: SourceDecimal = Field(
        description="The policy generation whose action declaration and bounds were qualified."
    )
    evidence: dict[str, Any] = Field(
        description=(
            "References to rows of this installation: restore_execution_id and "
            "conflict_execution_id (reversible) or forward_execution_ids (idempotent), "
            "and verified_run_id and not_recovered_run_id."
        )
    )
    worst_case: str = Field(description="The worst case statement, 1 to 2000 characters.")


class RemediationQualificationOut(RemediationModel):
    """One qualification record. @spec AUTOMATED-REMEDIATION-22."""

    id: SourceUuid
    agent_id: SourceUuid
    hook: str | None
    action: str | None
    generation: SourceDecimal | None
    connector: str
    tool: str
    connector_digest: str = Field(description="The acting connector's in-force digest at write.")
    verifier_sha256: str = Field(description="SHA-256 of the canonical verifier declaration.")
    reversibility: str
    recorded_by: str = Field(description="The operator principal that recorded it.")
    worst_case: str
    evidence: dict[str, Any]
    created_at: datetime


class RemediationVerifierRunStart(RemediationModel):
    """Start a qualification verifier run; exactly ``{"hook", "action", "target"}``.

    @spec AUTOMATED-REMEDIATION-22.
    """

    hook: SourceHook
    action: str = Field(min_length=1, max_length=128)
    target: TargetValue = Field(description="A literal member of the action's target.allowed list.")


class RemediationVerifierRunOut(RemediationModel):
    """One qualification verifier run. @spec AUTOMATED-REMEDIATION-22."""

    id: SourceUuid
    qualification_id: SourceUuid
    hook: str
    action: str
    target: TargetValue
    generation: SourceDecimal
    started_by: str = Field(description="The operator principal that started it.")
    started_at: datetime
    outcome: str | None = Field(
        description="verified, not-recovered or verifier-unavailable once decided."
    )
    decided_at: datetime | None
