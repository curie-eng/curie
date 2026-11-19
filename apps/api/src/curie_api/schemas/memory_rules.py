"""Learned rule list and decision wire shapes (#5797)."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

# The states a person can see. Candidate, superseded and discarded rows never
# leave the API.
ListedRuleState = Literal["proposed", "accepted", "rejected", "removed"]
RuleScopeKind = Literal["agent", "binding", "repository"]
# `memory_rules.version` is a Postgres integer.
MAX_VERSION = 2_147_483_647


class MemoryRuleDecision(BaseModel):
    """The body of an accept, reject or remove: the version the person saw."""

    model_config = ConfigDict(strict=True, extra="forbid", hide_input_in_errors=True)

    expected_version: int = Field(ge=1, le=MAX_VERSION)


class MemoryRuleDecider(BaseModel):
    principal_id: UUID
    display_name: str | None


class MemoryRuleOut(BaseModel):
    id: UUID
    number: int
    label: str
    scope_kind: RuleScopeKind
    scope_key: str
    scope_label: str
    state: ListedRuleState
    text: str
    reason: str
    source_url: str | None
    proposed_by: str
    proposed_at: datetime
    accepted_by: MemoryRuleDecider | None
    accepted_at: datetime | None
    rejected_by: MemoryRuleDecider | None
    rejected_at: datetime | None
    removed_by: MemoryRuleDecider | None
    removed_at: datetime | None
    version: int


class MemoryRuleScopeOut(BaseModel):
    scope_kind: RuleScopeKind
    scope_key: str
    scope_label: str
    in_force_count: int


class MemoryRulesOut(BaseModel):
    rules: list[MemoryRuleOut]
    scopes: list[MemoryRuleScopeOut]
    in_force_limit: int


class MemoryRuleDecisionOut(BaseModel):
    rule: MemoryRuleOut
    applies_from: Literal["next_ticket"] = Field(
        description=(
            "The decision is recorded now and takes effect at the next ticket's "
            "admission once admission reads learned rules (#5798); a running request "
            "keeps the rules it was admitted with."
        )
    )


class MemoryRuleRefusalDetail(BaseModel):
    """A coded refusal: ``code`` is ``memory_rules.<name>``."""

    code: str
    message: str
    fix: str
    # Only on ``memory_rules.stale_version``.
    current_version: int | None = None
    # Only on ``memory_rules.scope_full``.
    limit: int | None = None
    # Only on ``memory_rules.snapshot_full``.
    limit_bytes: int | None = None


class MemoryRuleRefusal(BaseModel):
    """Every coded learned rule refusal, a malformed request's 422 included."""

    detail: MemoryRuleRefusalDetail


class MemoryRuleAuthRefusal(BaseModel):
    """A refused credential or console origin: one plain reason string."""

    detail: str
