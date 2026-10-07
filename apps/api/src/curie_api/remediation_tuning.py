"""Alert rule tuning requests: the declared reads behind the card, and no write.

@spec AUTOMATED-REMEDIATION-24 @spec AUTOMATED-REMEDIATION-25

docs/superpowers/specs/2026-10-07-automated-remediation.md, with the maintainer
ruling of 2026-10-07: tuning stops at the nomination and the approval card; an
approved tuning request ends with ``tune_execution_not_automated`` and no write
call; automated rule-owner change requests need a later Draft ADR before any
execution path is built.

A ``tune`` nomination is never automatic (admission check 5 sends it to
approval ``not_automatic``). When it raises a new approval, the same
transaction schedules the nominated rule's declared reads as ``read``
executions under the policy authority, keyed
``remediation:<nomination id>:tune:current:<field>`` (the rule's current value
of the nominated field, when the action declares that read) and
``remediation:<nomination id>:tune:evidence:<name>`` (every evidence read the
rule declares). Nothing else is scheduled: no other rule's reads, and never the
rule owner's write tool. A nomination that attaches to a pending identical
request schedules nothing, so a recorded series yields one request and one set
of reads.

The worker renders the card from those reads once they have ended
(``curie_worker.remediation_cards``): the diff from the structured change and
the current-value read, the evidence from the evidence reads. The model's
reason is shown only as unverified model text.

Approving the request records the decision and ends every nomination of it
``refused`` with ``tune_execution_not_automated``; no forward execution is
created
(``remediation_approvals.execute_approved``, and ``create_remediation_forward``
refuses a tune action as a second guard).
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from .models import ActionExecution, RemediationNomination
from .remediation_forward import (
    IDEMPOTENCY_PREFIX,
    POLICY_AUTHORITY,
    in_force_connector_digest,
    policy_ref,
)
from .remediation_reads import ReadRefused, scheduled_read
from .storage import ObjectStore

logger = logging.getLogger(__name__)

TUNE_KIND: Final = "tune"
# @spec AUTOMATED-REMEDIATION-25 (maintainer ruling, 2026-10-07).
TUNE_EXECUTION_NOT_AUTOMATED: Final = "tune_execution_not_automated"
CURRENT: Final = "current"
EVIDENCE: Final = "evidence"
_TUNE: Final = "tune"


def is_tune(action: Mapping[str, Any] | None) -> bool:
    """Whether the declared action is a ``tune`` action. @spec AUTOMATED-REMEDIATION-25."""

    return action is not None and action.get("kind") == TUNE_KIND


def tune_read_prefix(nomination_id: uuid.UUID) -> str:
    """``remediation:<nomination id>:tune:``, the prefix of every tune read's key."""

    return f"{IDEMPOTENCY_PREFIX}{nomination_id}:{_TUNE}:"


@dataclass(frozen=True)
class TuneRead:
    """One declared read of the nominated rule. @spec AUTOMATED-REMEDIATION-25."""

    role: str
    name: str
    connector: str
    tool: str
    arguments: Mapping[str, Any]
    pointer: str


def declared_tune_reads(action: Mapping[str, Any], arguments: Mapping[str, Any]) -> list[TuneRead]:
    """The reads a tune nomination may schedule: the nominated rule's only.

    @spec AUTOMATED-REMEDIATION-25: the current-value read of the nominated
    field (when declared) and every evidence read of the nominated rule. A
    declaration the validator would refuse yields nothing for that read.
    """

    rules = action.get("rules")
    rule, field = arguments.get("rule"), arguments.get("field")
    declared = rules.get(rule) if isinstance(rules, Mapping) and isinstance(rule, str) else None
    if not isinstance(declared, Mapping):
        return []
    wanted: list[tuple[str, str, Any]] = []
    current = declared.get(CURRENT)
    if isinstance(current, Mapping) and isinstance(field, str) and field in current:
        wanted.append((CURRENT, field, current[field]))
    evidence = declared.get(EVIDENCE)
    if isinstance(evidence, Mapping):
        wanted.extend((EVIDENCE, name, read) for name, read in evidence.items())
    reads: list[TuneRead] = []
    for role, name, read in wanted:
        if not isinstance(read, Mapping):
            continue
        connector, tool = read.get("connector"), read.get("tool")
        read_arguments, pointer = read.get("arguments", {}), read.get("pointer")
        if (
            isinstance(connector, str)
            and isinstance(tool, str)
            and isinstance(read_arguments, Mapping)
            and isinstance(pointer, str)
        ):
            reads.append(TuneRead(role, name, connector, tool, read_arguments, pointer))
    return reads


async def tune_read_rows(
    session: AsyncSession,
    store: ObjectStore,
    nomination: RemediationNomination,
    generation: int,
    action: Mapping[str, Any],
    arguments: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """The insert values of the nominated rule's declared reads, due now.

    @spec AUTOMATED-REMEDIATION-25 @spec AUTOMATED-REMEDIATION-12. Each read's
    connector runs at the agent's in-force digest; a read whose connector has
    none, or that the read seam refuses, is left out (the card names it not
    read). Writes nothing; the caller inserts the rows with the approval.
    """

    reads = declared_tune_reads(action, arguments)
    if not reads:
        return []
    now = await session.scalar(select(func.now()))
    if not isinstance(now, datetime):
        return []
    digests: dict[str, str | None] = {}
    ref = policy_ref(nomination.agent_id, nomination.hook, generation, nomination.id)
    prefix = tune_read_prefix(nomination.id)
    rows: list[dict[str, Any]] = []
    for read in reads:
        if read.connector not in digests:
            digests[read.connector] = await in_force_connector_digest(
                session, store, nomination.agent_id, read.connector
            )
        digest = digests[read.connector]
        if digest is None:
            logger.warning(
                "tune read not scheduled, no in-force digest nomination=%s role=%s",
                nomination.id,
                read.role,
            )
            continue
        try:
            rows.append(
                scheduled_read(
                    agent_id=nomination.agent_id,
                    connector=read.connector,
                    connector_digest=digest,
                    tool=read.tool,
                    arguments=read.arguments,
                    pointer=read.pointer,
                    authority_kind=POLICY_AUTHORITY,
                    authority_ref=f"{ref}:{_TUNE}",
                    idempotency_key=f"{prefix}{read.role}:{read.name}",
                    not_before=now,
                )
            )
        except ReadRefused:
            logger.warning("tune read refused nomination=%s role=%s", nomination.id, read.role)
    return rows


async def insert_tune_reads(session: AsyncSession, rows: list[dict[str, Any]]) -> None:
    """Insert scheduled tune reads; a replay adopts the existing rows. Commits nothing."""

    for row in rows:
        await session.execute(
            insert(ActionExecution)
            .values(row)
            .on_conflict_do_nothing(constraint="uq_action_executions_agent_idempotency_key")
        )


__all__ = [
    "TUNE_EXECUTION_NOT_AUTOMATED",
    "TUNE_KIND",
    "TuneRead",
    "declared_tune_reads",
    "insert_tune_reads",
    "is_tune",
    "tune_read_prefix",
    "tune_read_rows",
]
