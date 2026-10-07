"""Limit reservations and breakers: the rows admission counts and consults.

@spec AUTOMATED-REMEDIATION-10 @spec AUTOMATED-REMEDIATION-11

Kept apart from ``remediation_admission`` so the executor routes and the
verifier, which admission itself imports, can release a reservation or open a
breaker without an import cycle. Nothing here commits.

* A reservation (``remediation_reservations``) is taken by admission under the
  per-agent admission lock when a nomination reaches its precondition read
  (check 11). It counts toward the policy's and the action's rolling hour, the
  turn and, while its nomination is live, the target, until it is released;
  ``release_reservation`` releases it whenever the nomination does not execute
  (the precondition or its re-check sends it to approval, the claim-time
  re-check refuses it, or its forward execution is refused before any write).
* A breaker (``remediation_breakers``) is keyed by agent, connector, tool and
  target key. ``open_breaker`` opens one on any verification outcome other than
  ``verified`` written for a remediation, whatever authorized the action; it is
  idempotent while one is open. Only ``close_breaker``, called by the policy's
  administrative route with an operator principal, closes it.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Final

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from .models import RemediationBreaker, RemediationNomination, RemediationReservation

logger = logging.getLogger(__name__)

VERIFIED: Final = "verified"


async def release_reservation(session: AsyncSession, nomination_id: uuid.UUID) -> None:
    """Release the nomination's reservation, if it holds one. @spec AUTOMATED-REMEDIATION-10."""

    await session.execute(
        update(RemediationReservation)
        .where(
            RemediationReservation.nomination_id == nomination_id,
            RemediationReservation.released_at.is_(None),
        )
        .values(released_at=func.now())
        .execution_options(synchronize_session=False)
    )


async def open_breaker(
    session: AsyncSession,
    *,
    agent_id: uuid.UUID,
    connector: str,
    tool: str,
    target: str,
) -> None:
    """Open the breaker for this key unless one is open. @spec AUTOMATED-REMEDIATION-11."""

    opened = await session.scalar(
        insert(RemediationBreaker)
        .values(
            id=uuid.uuid4(),
            agent_id=agent_id,
            connector=connector,
            tool=tool,
            target=target,
        )
        .on_conflict_do_nothing(
            index_elements=["agent_id", "connector", "tool", "target"],
            index_where=RemediationBreaker.closed_at.is_(None),
        )
        .returning(RemediationBreaker.id)
    )
    if opened is not None:
        logger.info("remediation breaker opened breaker=%s agent=%s", opened, agent_id)


async def outcome_written(
    session: AsyncSession,
    nomination_id: uuid.UUID,
    outcome: str,
    *,
    connector: str | None,
    tool: str | None,
) -> None:
    """Open the breaker of a remediation whose outcome is not ``verified``.

    @spec AUTOMATED-REMEDIATION-11: "opens on any verification outcome other
    than ``verified`` ... whether the action ran under the policy or under an
    approval, and on an execution that ended ``failed`` or ``indeterminate``"
    (those finish ``not-recovered``). Called in the transaction that writes the
    outcome, with the forward execution's connector and tool.
    """

    if outcome == VERIFIED or connector is None or tool is None:
        return
    nomination = await session.get(RemediationNomination, nomination_id)
    if nomination is None or nomination.target is None:
        return
    await open_breaker(
        session,
        agent_id=nomination.agent_id,
        connector=connector,
        tool=tool,
        target=nomination.target,
    )


async def breaker_open(
    session: AsyncSession, agent_id: uuid.UUID, connector: str, tool: str, target: str
) -> bool:
    """Whether a breaker is open for this key. @spec AUTOMATED-REMEDIATION-11."""

    found = await session.scalar(
        select(RemediationBreaker.id)
        .where(
            RemediationBreaker.agent_id == agent_id,
            RemediationBreaker.connector == connector,
            RemediationBreaker.tool == tool,
            RemediationBreaker.target == target,
            RemediationBreaker.closed_at.is_(None),
        )
        .limit(1)
    )
    return found is not None


async def close_breaker(
    session: AsyncSession,
    *,
    agent_id: uuid.UUID,
    hook_targets: frozenset[tuple[str, str]],
    breaker_id: uuid.UUID,
    principal: str,
    reason: str,
    now: datetime | None = None,
) -> RemediationBreaker | None:
    """Close one open breaker of the agent, recording who closed it and why.

    @spec AUTOMATED-REMEDIATION-11. ``hook_targets`` are the (connector, tool)
    pairs the hook's policy declares: a breaker is closed through the policy
    that declares its action, never another hook's. Returns the breaker as it
    now stands (an already closed one unchanged), or None when the agent has no
    such breaker under this policy.
    """

    breaker = await session.scalar(
        select(RemediationBreaker)
        .where(RemediationBreaker.id == breaker_id, RemediationBreaker.agent_id == agent_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if breaker is None or (breaker.connector, breaker.tool) not in hook_targets:
        return None
    if breaker.closed_at is None:
        breaker.closed_at = now if now is not None else await session.scalar(select(func.now()))
        breaker.closed_by = principal
        breaker.close_reason = reason
        logger.info("remediation breaker closed breaker=%s agent=%s", breaker.id, agent_id)
    return breaker


__all__ = [
    "breaker_open",
    "close_breaker",
    "open_breaker",
    "outcome_written",
    "release_reservation",
]
