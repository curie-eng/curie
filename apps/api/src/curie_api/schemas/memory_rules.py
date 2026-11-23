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
    can_decide: bool | None = Field(
        description=(
            "Whether the requesting person could accept, dismiss or remove a rule of this "
            "scope right now, by the same check a decision makes. `false`: the check "
            "refused with `memory_rules.write_access_required`. `null`: not known (a "
            "request that is not a principal's console session, a code host that cannot "
            "be reached or verified). The decision routes still check at click time."
        )
    )


class MemoryRulesOut(BaseModel):
    rules: list[MemoryRuleOut]
    scopes: list[MemoryRuleScopeOut]
    in_force_limit: int


class MemoryRuleDecisionOut(BaseModel):
    rule: MemoryRuleOut
    applies_from: Literal["next_ticket", "next_session"] = Field(
        description=(
            "When the decision takes effect. `next_ticket` for a repository scope rule: "
            "at the next ticket's admission; a running request keeps the rules it was "
            "admitted with. `next_session` for an agent or binding scope rule: at the "
            "next session start of the agent; a running session keeps the rules it "
            "loaded (#5801)."
        )
    )


class ChannelRuleProposalIn(BaseModel):
    """A non-factory turn's one learned rule proposal (#5801, ADR 0287 decision 7).

    Exactly these fields; any other body is refused 422 ``invalid_body`` with
    nothing echoed. ``scope`` picks the credential's own channel (the default)
    or the whole agent. The scope key, the proposer, the source turn and the
    state come from the per turn credential and the server, never from the body.
    """

    model_config = ConfigDict(strict=True, extra="forbid", hide_input_in_errors=True)

    rule: str = Field(description="The rule, one line of at most 240 UTF-8 bytes.")
    reason: str = Field(description="Why it should always apply, at most 240 UTF-8 bytes.")
    scope: Literal["channel", "agent"] = Field(
        default="channel",
        description="`channel`: the credential's own binding. `agent`: every channel.",
    )


class ChannelRuleOut(BaseModel):
    """One accepted rule a session loads at its start."""

    id: UUID
    scope_kind: Literal["agent", "binding"]
    scope_key: str
    scope_label: str
    number: int
    text: str
    accepted_at: datetime


class ChannelRulesOut(BaseModel):
    """The accepted rules a non-factory session loads at its start (#5801)."""

    proposals_enabled: bool = Field(
        description="Whether the agent may propose rules (`memory_rule_proposals`)."
    )
    rules: list[ChannelRuleOut] = Field(
        description=(
            "Accepted rules of the agent scope, then of the credential's binding scope, "
            "each ordered by acceptance, at most 16 per scope."
        )
    )


class ChannelRuleRefusalDetail(BaseModel):
    """A coded refusal of the state router's learned rule routes."""

    code: str
    message: str
    fix: str


class ChannelRuleRefusal(BaseModel):
    detail: ChannelRuleRefusalDetail


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
