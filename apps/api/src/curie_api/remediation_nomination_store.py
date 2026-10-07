"""Record a protected event's nomination submission, once.

@spec AUTOMATED-REMEDIATION-6 @spec AUTOMATED-REMEDIATION-7.

One transaction per submission:

1. claim the event: insert its submission row keyed on ``event_id`` with the
   SHA-256 of the submitted bytes. A second submission finds the row (waiting
   for a concurrent first one to commit) and is answered from it: the same bytes
   return the first answer, different bytes are ``nomination_conflict``.
2. read the hook's current remediation policy generation (none when unbound);
3. parse the block with the one production parser and write one
   ``remediation_nominations`` row per entry, or one ``nomination_malformed``
   row for a malformed block.

A well-formed entry the policy declares, with arguments of the declared shape,
is written ``received`` with its kind and target key, for admission (tasks 9 and
10) to advance; every parse refusal is written ``refused`` with its code and
``decided_at``, and creates no approval or execution.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Mapping
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from .models import (
    Agent,
    RemediationNomination,
    RemediationNominationSubmission,
    RemediationPolicy,
    RemediationPolicyGeneration,
)
from .remediation_binding import ProtectedEvent
from .remediation_nominations import (
    MALFORMED_CODE,
    NominationMalformed,
    find_action,
    parse_nomination_block,
    target_key,
    validate_entry,
)

RECEIVED = "received"
REFUSED = "refused"


class NominationConflict(Exception):
    """The event already has a different accepted submission. @spec AUTOMATED-REMEDIATION-6."""


class EventAgentMissing(Exception):
    """The binding names an agent that no longer exists. @spec AUTOMATED-REMEDIATION-6."""


def block_sha256(block: str) -> str:
    """SHA-256 hex of the submitted block's exact bytes. @spec AUTOMATED-REMEDIATION-6."""
    return hashlib.sha256(block.encode("utf-8", "surrogatepass")).hexdigest()


async def _nomination_ids(session: AsyncSession, event_id: str) -> list[uuid.UUID]:
    rows = await session.scalars(
        select(RemediationNomination.id)
        .where(RemediationNomination.event_id == event_id)
        .order_by(RemediationNomination.created_at, RemediationNomination.id)
    )
    return list(rows)


async def _policy(
    session: AsyncSession, event: ProtectedEvent
) -> tuple[int | None, Mapping[str, Any] | None]:
    """The current generation and its document, or none. @spec AUTOMATED-REMEDIATION-7."""
    current = await session.get(RemediationPolicy, (event.agent_id, event.hook))
    if current is None:
        return None, None
    row = await session.get(
        RemediationPolicyGeneration, (event.agent_id, event.hook, current.generation)
    )
    return current.generation, (row.document if row is not None else None)


def _rows(
    event: ProtectedEvent,
    block: str,
    generation: int | None,
    document: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    """The nomination rows of one submission, in block order. @spec AUTOMATED-REMEDIATION-7."""
    base: dict[str, Any] = {
        "agent_id": event.agent_id,
        "hook": event.hook,
        "event_id": event.event_id,
        "admitted_generation": event.admitted_generation,
        "current_generation": generation,
    }
    try:
        entries = parse_nomination_block(block)
    except NominationMalformed:
        return [
            {**base, "state": REFUSED, "refusal_code": MALFORMED_CODE, "decided_at": func.now()}
        ]
    rows: list[dict[str, Any]] = []
    for entry in entries:
        action = find_action(document, entry.action)
        refusal = entry.refusal or validate_entry(action, entry.value)
        row: dict[str, Any] = {
            **base,
            "action": entry.action,
            "arguments": entry.arguments,
            "arguments_sha256": entry.arguments_sha256,
            "reason": entry.reason,
        }
        if refusal is not None:
            row.update(state=REFUSED, refusal_code=refusal, decided_at=func.now())
        else:
            assert action is not None  # validate_entry refuses an unknown action
            row.update(
                state=RECEIVED,
                kind=action.get("kind"),
                target=target_key(action, entry.value),
            )
        rows.append(row)
    return rows


async def record_submission(
    session: AsyncSession, event: ProtectedEvent, block: str
) -> list[uuid.UUID]:
    """Record the event's first submission, or answer a replay of it.

    Returns the event's nomination ids in block order. Raises
    ``NominationConflict`` for different bytes and ``EventAgentMissing`` when
    the agent is gone. The caller commits. @spec AUTOMATED-REMEDIATION-6.
    """
    digest = block_sha256(block)
    if await session.get(Agent, event.agent_id) is None:
        raise EventAgentMissing()
    claimed = await session.scalar(
        insert(RemediationNominationSubmission)
        .values(
            event_id=event.event_id,
            agent_id=event.agent_id,
            hook=event.hook,
            block_sha256=digest,
        )
        .on_conflict_do_nothing(index_elements=["event_id"])
        .returning(RemediationNominationSubmission.event_id)
    )
    if claimed is None:
        existing = await session.scalar(
            select(RemediationNominationSubmission.block_sha256).where(
                RemediationNominationSubmission.event_id == event.event_id
            )
        )
        if existing != digest:
            raise NominationConflict()
        return await _nomination_ids(session, event.event_id)
    generation, document = await _policy(session, event)
    rows = _rows(event, block, generation, document)
    # Sorted ids keep block order among rows that share a timestamp.
    ids = sorted(uuid.uuid4() for _ in rows)
    for row_id, row in zip(ids, rows, strict=True):
        session.add(RemediationNomination(id=row_id, **row))
        await session.flush()
    return ids
