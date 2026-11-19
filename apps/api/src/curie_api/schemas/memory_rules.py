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
    applies_from: Literal["next_ticket"]
