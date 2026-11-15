"""When a blocked provider gate may release its factory successor (#5426, ADR 0269).

A failed request whose provider refused it (an invalid credential, depleted
credit or a usage limit without a typed reset time) leaves its automatic
successor waiting, blocked on ``provider_credential``, ``provider_credit`` or
``provider_usage_reset``. The worker's provider probe writes what a
non-generating read showed into the shared provider gate ledger
(``curie_internal.provider_gate_ledger``); this module decides from those
observations and the predecessor's persisted condition whether the block may
lift. ``clearance`` is pure; ``observe`` reads the ledger for one candidate and
``candidates`` lists the blocked successors worth reading.

Every rule fails closed. A missing scope, a typed reset time, an unknown or
unprobed provider, a stale, rejected or unreadable probe, an unchanged
credential, or an allowance that did not grow by a strictly greater limit all
keep the block, and the successor expires visibly as
``retry_condition_expired``. Nothing here logs.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Final

from curie_internal.provider_gate_ledger import (
    ScopeObservation,
    find_provider,
    read_current,
    read_scope,
)
from curie_internal.provider_scope import PROBED_PROVIDERS
from redis.asyncio import Redis
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from .models import ExecutionRequest, WorkItem

PROVIDER_CREDENTIAL: Final = "provider_credential"
PROVIDER_CREDIT: Final = "provider_credit"
PROVIDER_USAGE_RESET: Final = "provider_usage_reset"
# The blocked conditions a provider probe can release. Every other condition
# (``provider_rate_limit``, ``provider_service``, the diagnosis and limit waits)
# is never released here.
PROVIDER_GATES: Final[frozenset[str]] = frozenset(
    {PROVIDER_CREDENTIAL, PROVIDER_CREDIT, PROVIDER_USAGE_RESET}
)
# How old a probe may be, either side of now, and still count.
PROBE_FRESH_S: Final[int] = 120

# The predecessor's condition reasons each gate accepts.
_GATE_REASONS: Final[dict[str, frozenset[str]]] = {
    PROVIDER_CREDENTIAL: frozenset({"credential_rejected"}),
    PROVIDER_CREDIT: frozenset({"credit_exhausted", "usage_reset"}),
    PROVIDER_USAGE_RESET: frozenset({"usage_reset"}),
}


@dataclass(frozen=True)
class GateFacts:
    """The blocked successor's condition and its predecessor's persisted facts."""

    blocked_on: str | None
    reason: str | None
    retry_at: datetime | None
    scope_key: str | None
    terminal_at: datetime | None


@dataclass(frozen=True)
class GateObservation:
    """What the ledger showed for one candidate.

    ``provider`` holds the failed scope, ``current_scope`` is that provider's
    current scope key, and ``observation`` is the scope the gate relies on: the
    current scope for a credential gate, the failed scope otherwise.
    """

    provider: str | None
    current_scope: str | None
    observation: ScopeObservation | None


_UNOBSERVED: Final = GateObservation(provider=None, current_scope=None, observation=None)


@dataclass(frozen=True)
class GateCandidate:
    request_id: uuid.UUID
    work_item_id: uuid.UUID
    request_version: int
    work_item_version: int
    facts: GateFacts


def _ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _fresh(observation: ScopeObservation | None, provider: str, after_ms: int, now_ms: int) -> bool:
    """An authenticated probe of ``provider`` after ``after_ms`` and near ``now_ms``."""

    if observation is None or observation.provider != provider:
        return False
    probed = observation.probed_at_ms
    return (
        observation.outcome == "authenticated"
        and probed is not None
        and probed > after_ms
        and abs(now_ms - probed) <= PROBE_FRESH_S * 1000
    )


def clearance(facts: GateFacts, observed: GateObservation, now: datetime) -> bool:
    """Whether ``observed`` verifies the repair or allowance the gate waits on."""

    blocked_on = facts.blocked_on
    if blocked_on is None or blocked_on not in PROVIDER_GATES:
        return False
    if facts.reason not in _GATE_REASONS[blocked_on]:
        return False
    if facts.scope_key is None or facts.retry_at is not None or facts.terminal_at is None:
        return False
    provider = observed.provider
    if provider is None or provider not in PROBED_PROVIDERS:
        return False
    terminal_ms = _ms(facts.terminal_at)
    now_ms = _ms(now)
    observation = observed.observation
    if not _fresh(observation, provider, terminal_ms, now_ms):
        return False
    assert observation is not None
    if blocked_on == PROVIDER_CREDENTIAL:
        # A replaced credential: the provider now runs with another scope, and
        # that scope authenticated after the failure.
        return observed.current_scope is not None and observed.current_scope != facts.scope_key
    # Added allowance on the same credential: still the current scope, a numeric
    # limit raised after the failure, and something left under it.
    increased = observation.allowance_increased_at_ms
    remaining = observation.limit_remaining
    return (
        observed.current_scope == facts.scope_key
        and observation.limit is not None
        and remaining is not None
        and remaining > 0
        and increased is not None
        and increased > terminal_ms
    )


async def observe(redis: Redis, prefix: str, facts: GateFacts) -> GateObservation:
    """Read the ledger values ``clearance`` needs for one candidate."""

    if (
        facts.blocked_on not in PROVIDER_GATES
        or facts.scope_key is None
        or facts.retry_at is not None
    ):
        return _UNOBSERVED
    provider = await find_provider(redis, prefix, facts.scope_key)
    if provider is None or provider not in PROBED_PROVIDERS:
        return GateObservation(provider=provider, current_scope=None, observation=None)
    current = await read_current(redis, prefix, provider)
    relied = current if facts.blocked_on == PROVIDER_CREDENTIAL else facts.scope_key
    observation = None if relied is None else await read_scope(redis, prefix, provider, relied)
    return GateObservation(provider=provider, current_scope=current, observation=observation)


async def cleared(redis: Redis, prefix: str, facts: GateFacts, now: datetime) -> bool:
    """A fresh ledger read and ``clearance`` for one candidate."""

    return clearance(facts, await observe(redis, prefix, facts), now)


async def candidates(session: AsyncSession, limit: int) -> list[GateCandidate]:
    """Waiting, unstarted successors blocked on a provider gate, with their facts.

    Only a predecessor that names a scope and no reset time can ever clear, so
    the rest are not listed. The caller ends the read transaction.
    """

    predecessor = aliased(ExecutionRequest)
    successor = ExecutionRequest
    rows = (
        await session.execute(
            select(
                successor.id,
                successor.work_item_id,
                successor.version,
                WorkItem.version.label("work_item_version"),
                successor.blocked_on,
                predecessor.retry_condition_reason,
                predecessor.retry_condition_retry_at,
                predecessor.retry_condition_scope_key,
                predecessor.terminal_at,
            )
            .join(WorkItem, WorkItem.id == successor.work_item_id)
            .join(predecessor, predecessor.id == successor.retry_of_request_id)
            .where(
                successor.status == "waiting",
                successor.started_at.is_(None),
                successor.blocked_on.in_(sorted(PROVIDER_GATES)),
                successor.wait_deadline > func.clock_timestamp(),
                WorkItem.cancelled_at.is_(None),
                WorkItem.readmit_request_id.is_(None),
                predecessor.work_item_id == successor.work_item_id,
                predecessor.retry_condition_scope_key.is_not(None),
                predecessor.retry_condition_retry_at.is_(None),
            )
            .order_by(successor.wait_deadline, successor.id)
            .limit(limit)
        )
    ).all()
    return [
        GateCandidate(
            request_id=row.id,
            work_item_id=row.work_item_id,
            request_version=row.version,
            work_item_version=row.work_item_version,
            facts=GateFacts(
                blocked_on=row.blocked_on,
                reason=row.retry_condition_reason,
                retry_at=row.retry_condition_retry_at,
                scope_key=row.retry_condition_scope_key,
                terminal_at=row.terminal_at,
            ),
        )
        for row in rows
    ]


__all__ = [
    "PROBE_FRESH_S",
    "PROVIDER_CREDENTIAL",
    "PROVIDER_CREDIT",
    "PROVIDER_GATES",
    "PROVIDER_USAGE_RESET",
    "GateCandidate",
    "GateFacts",
    "GateObservation",
    "candidates",
    "clearance",
    "cleared",
    "observe",
]
