"""A non-factory agent's learned rules: one proposal per turn, and the session start load.

ADR 0287 decision 7 (#5801) amends ADR 0188: the per turn memory write
credential may also propose one learned rule per turn, in its own binding scope
or in the agent scope. The state router (``routers/state.py``) authenticates the
credential and derives every value but the text from it: the scope key from the
``binding`` claim (``curie_internal.memory_rules.binding_claim_scope_key``) or
the agent scope key, the source turn from the ``turn`` claim and the proposer
from the ``sender`` claim. This module stores the proposal and reads the
accepted rules back; it never reads the request body itself.

There is no pull request to wait for, so the rule is stored ``proposed`` at once
with the next free number of its scope. Lock order, as for every learned rule
write (``memory_rule_lifecycle``): the agent row ``FOR KEY SHARE`` first
(``lock_rule_owner``), then the scope's number lock (``rule_number_lock_key``).
The unique ``(agent_id, source_turn)`` index refuses a second proposal of one
turn even when two race.

A database failure is answered ``ProposalUnavailable``, which carries no text:
the engine displays bound parameters in its error messages, so the SQLAlchemy
error, which would carry the rule and the reason, is dropped here.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Final, Literal

from curie_internal.memory_rules import (
    AGENT_SCOPE_KEY,
    FORMAT_VERSION,
    MAX_ACCEPTED_PER_SCOPE,
    InvalidRule,
    RuleState,
    rule_number_lock_key,
    validate_reason,
    validate_rule_text,
)
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError, StatementError
from sqlalchemy.ext.asyncio import AsyncSession

from .memory_rule_lifecycle import (
    ALREADY_PROPOSED,
    NOT_ELIGIBLE,
    ProposalRefused,
    ProposalUnavailable,
    lock_rule_owner,
)
from .models import Agent, MemoryRule, MemoryRuleEvent

ChannelScopeKind = Literal["agent", "binding"]
_SOURCE_TURN_KEY: Final = "memory_rules_source_turn_key"


@dataclass(frozen=True)
class ChannelScope:
    """The scope a proposal lands in, derived from the credential only.

    ``label`` is the binding claim for a binding scope; for the agent scope it
    is None and the agent's name is used.
    """

    kind: ChannelScopeKind
    key: str
    label: str | None


@dataclass(frozen=True)
class ProposedRule:
    """The stored proposal, read back inside its own transaction."""

    id: uuid.UUID
    scope_kind: ChannelScopeKind
    scope_key: str
    scope_label: str
    number: int
    state: RuleState
    text: str
    reason: str
    proposed_by: str
    proposed_at: datetime
    version: int


@dataclass(frozen=True)
class LoadedRule:
    id: uuid.UUID
    scope_kind: ChannelScopeKind
    scope_key: str
    scope_label: str
    number: int
    text: str
    accepted_at: datetime


@dataclass(frozen=True)
class LoadedRules:
    proposals_enabled: bool
    rules: list[LoadedRule]


def _constraint(exc: IntegrityError) -> str | None:
    return getattr(exc.orig.__cause__, "constraint_name", None) if exc.orig else None


async def _rollback_quietly(session: AsyncSession) -> None:
    """Roll back after a database failure; a dead connection may refuse even that."""

    try:
        await session.rollback()
    except (SQLAlchemyError, OSError):
        pass


async def propose_channel_rule(
    session: AsyncSession,
    *,
    agent_id: uuid.UUID,
    scope: ChannelScope,
    source_turn: str,
    proposed_by: str,
    rule: str,
    reason: str,
    ensure_open: Callable[[], Awaitable[None]],
) -> ProposedRule:
    """Store the turn's one rule as ``proposed`` with its ``propose`` event, committed.

    ``ensure_open`` is awaited immediately before the row is added, after every
    lock and read this function waits on, and raises to refuse a turn the worker
    has reported ended meanwhile (the exception propagates after a rollback).

    Raises ``ProposalRefused`` (``rule_proposal_not_eligible`` when the agent is
    gone or its ``memory_rule_proposals`` is off; ``rule_already_proposed`` for
    a second proposal of ``source_turn``) or ``InvalidRule`` (``invalid_text``,
    ``too_large``, ``reserved_text``); every refusal rolls back and stores
    nothing. A database failure rolls back as far as it can and raises
    ``ProposalUnavailable``. No exception carries the submitted text.
    """

    try:
        # Agent first (module docstring): an agent deleted meanwhile is gone.
        if not await lock_rule_owner(session, agent_id):
            raise ProposalRefused(NOT_ELIGIBLE)
        agent = (
            await session.execute(
                select(Agent.tenant_id, Agent.name, Agent.memory_rule_proposals).where(
                    Agent.id == agent_id
                )
            )
        ).one()
        tenant_id, agent_name, switch = agent
        if switch is not True:
            raise ProposalRefused(NOT_ELIGIBLE)
        body = validate_rule_text(rule)
        why = validate_reason(reason)
        taken = await session.scalar(
            select(MemoryRule.id).where(
                MemoryRule.agent_id == agent_id, MemoryRule.source_turn == source_turn
            )
        )
        if taken is not None:
            raise ProposalRefused(ALREADY_PROPOSED)
        await session.execute(
            text("SELECT pg_advisory_xact_lock(:key)"),
            {"key": rule_number_lock_key(agent_id, scope.kind, scope.key)},
        )
        highest = await session.scalar(
            select(func.max(MemoryRule.number)).where(
                MemoryRule.agent_id == agent_id,
                MemoryRule.scope_kind == scope.kind,
                MemoryRule.scope_key == scope.key,
            )
        )
        try:
            await ensure_open()
        except BaseException:
            await _rollback_quietly(session)
            raise
        state: RuleState = "proposed"
        number = (highest or 0) + 1
        row = MemoryRule(
            id=uuid.uuid4(),
            tenant_id=tenant_id,
            agent_id=agent_id,
            scope_kind=scope.kind,
            scope_key=scope.key,
            scope_label=scope.label if scope.label is not None else agent_name,
            number=number,
            state=state,
            text=body,
            sha256=hashlib.sha256(body.encode("utf-8")).hexdigest(),
            format_version=FORMAT_VERSION,
            reason=why,
            proposed_by=proposed_by,
            proposed_at=func.clock_timestamp(),
            source_turn=source_turn,
        )
        session.add(row)
        await session.flush()
        session.add(
            MemoryRuleEvent(
                rule_id=row.id,
                tenant_id=tenant_id,
                agent_id=agent_id,
                event="propose",
                from_state=None,
                to_state=state,
                actor_kind="agent",
                actor=str(agent_id),
            )
        )
        await session.flush()
        await session.refresh(row)
        stored = ProposedRule(
            id=row.id,
            scope_kind=scope.kind,
            scope_key=row.scope_key,
            scope_label=row.scope_label,
            number=number,
            state=state,
            text=row.text,
            reason=row.reason,
            proposed_by=row.proposed_by,
            proposed_at=row.proposed_at,
            version=row.version,
        )
        await session.commit()
    except IntegrityError as exc:
        await _rollback_quietly(session)
        if _constraint(exc) == _SOURCE_TURN_KEY:
            raise ProposalRefused(ALREADY_PROPOSED) from None
        # Its message displays the bound rule and reason: never re-raised.
        raise ProposalUnavailable from None
    except StatementError:
        await _rollback_quietly(session)
        raise ProposalUnavailable from None
    except (ProposalRefused, InvalidRule):
        await _rollback_quietly(session)
        raise
    return stored


async def load_channel_rules(
    session: AsyncSession, *, agent_id: uuid.UUID, binding_scope_key: str | None
) -> LoadedRules | None:
    """The accepted rules a session of the agent loads at its start, or None
    when the agent does not exist.

    The agent scope's rules, then those of ``binding_scope_key`` (none when it
    is None), each ordered by ``accepted_at`` then ``id`` and at most
    ``MAX_ACCEPTED_PER_SCOPE`` per scope; only ``accepted`` rows. Reads only.
    """

    switch = await session.scalar(select(Agent.memory_rule_proposals).where(Agent.id == agent_id))
    if switch is None:
        return None
    scopes: list[tuple[ChannelScopeKind, str]] = [("agent", AGENT_SCOPE_KEY)]
    if binding_scope_key is not None:
        scopes.append(("binding", binding_scope_key))
    rules: list[LoadedRule] = []
    for kind, key in scopes:
        rows = await session.execute(
            select(
                MemoryRule.id,
                MemoryRule.scope_label,
                MemoryRule.number,
                MemoryRule.text,
                MemoryRule.accepted_at,
            )
            .where(
                MemoryRule.agent_id == agent_id,
                MemoryRule.scope_kind == kind,
                MemoryRule.scope_key == key,
                MemoryRule.state == "accepted",
            )
            .order_by(MemoryRule.accepted_at, MemoryRule.id)
            .limit(MAX_ACCEPTED_PER_SCOPE)
        )
        for rule_id, label, number, body, accepted_at in rows.tuples().all():
            # An accepted rule always carries its number and acceptance time.
            if number is None or accepted_at is None:
                continue
            rules.append(
                LoadedRule(
                    id=rule_id,
                    scope_kind=kind,
                    scope_key=key,
                    scope_label=label,
                    number=number,
                    text=body,
                    accepted_at=accepted_at,
                )
            )
    return LoadedRules(proposals_enabled=bool(switch), rules=rules)
