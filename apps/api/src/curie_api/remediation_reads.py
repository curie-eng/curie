"""Read executions: one remediation sample per execution (AUTOMATED-REMEDIATION-12).

@spec AUTOMATED-REMEDIATION-12 (executor amendments E1, E2, E5 and E9)

Precondition and verifier reads run through the executor, never as a direct
client in the API or the worker. Each sample is its own ``read`` execution,
created here by the remediation producers (admission's precondition, the
verifier, the qualification verifier run), never by an HTTP route (E1):

* the authority is ``policy``, ``approval`` or ``qualification``, and its
  reference names the nomination or verifier run; anything else, a connector,
  digest or tool outside its grammar, arguments without a canonical form or an
  invalid RFC 6901 pointer is ``ReadRefused`` and writes nothing;
* the row is ``requested`` with its ``not_before`` time, the bound tool, the
  canonical arguments (``forward_arguments``, with their digest) and the
  pointer; the claim route hands it out once due (E9);
* a replay of the same agent and idempotency key adopts the existing row in
  whatever state it is (``created`` false); the same key naming another read is
  ``arguments_mismatch`` and the first stands.

A read never enters ``dispatched``: it runs ``list`` and its one ``read`` in
``claimed`` and ends ``confirmed`` through ``POST /action-executions/{id}/samples``
or ``refused`` (E5). ``missed_samples`` is the one definition of a sample
series the claim route uses to record a missed sample ``skipped``.
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

from sqlalchemy import ColumnElement, and_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from .action_forward import arguments_sha256, canonical_arguments
from .models import ActionExecution, Agent, ExecutionKind, ExecutionState
from .remediation_policy_document import valid_pointer
from .schemas.action_executions import CONNECTOR_MAX_LENGTH, CONNECTOR_PATTERN, DIGEST_PATTERN

# @spec AUTOMATED-REMEDIATION-12: the authorities a read runs under.
READ_AUTHORITY_KINDS: Final = frozenset({"policy", "approval", "qualification"})
# @spec AUTOMATED-REMEDIATION-18 (executor amendment E3): an observe-only
# execution is a read of the acting connector's ``observe_version`` with no
# pointer; it is the only read that carries none.
OBSERVE_TOOL: Final = "observe_version"

_CONNECTOR = re.compile(CONNECTOR_PATTERN)
_DIGEST = re.compile(DIGEST_PATTERN)
_TOOL_MAX_LENGTH: Final = 128
_REF_MAX_LENGTH: Final = 512
_KEY_MAX_LENGTH: Final = 512


@dataclass(frozen=True)
class ReadCreated:
    """The created or adopted read execution: identity, state, whether it is new."""

    execution_id: uuid.UUID
    state: str
    created: bool


class ReadRefused(Exception):
    """A refusal that created no row. ``code`` is an ACTION-EXECUTOR-20 code."""

    def __init__(self, code: str, reason: str) -> None:
        super().__init__(f"{code}: {reason}")
        self.code = code
        self.reason = reason


def _well_formed(
    *,
    connector: str,
    connector_digest: str,
    tool: str,
    authority_kind: str,
    authority_ref: str,
    idempotency_key: str,
    not_before: datetime,
) -> bool:
    return (
        authority_kind in READ_AUTHORITY_KINDS
        and isinstance(connector, str)
        and len(connector) <= CONNECTOR_MAX_LENGTH
        and _CONNECTOR.fullmatch(connector) is not None
        and isinstance(connector_digest, str)
        and _DIGEST.fullmatch(connector_digest) is not None
        and isinstance(tool, str)
        and 0 < len(tool) <= _TOOL_MAX_LENGTH
        and tool.strip() == tool
        and isinstance(authority_ref, str)
        and 0 < len(authority_ref) <= _REF_MAX_LENGTH
        and isinstance(idempotency_key, str)
        and 0 < len(idempotency_key) <= _KEY_MAX_LENGTH
        and isinstance(not_before, datetime)
        and not_before.tzinfo is not None
    )


def _pointer_allowed(tool: str, pointer: str | None) -> bool:
    """A valid RFC 6901 pointer, or none for an observe-only execution (E3)."""

    if pointer is None:
        return tool == OBSERVE_TOOL
    return valid_pointer(pointer)


def scheduled_read(
    *,
    agent_id: uuid.UUID,
    connector: str,
    connector_digest: str,
    tool: str,
    arguments: Mapping[str, Any],
    pointer: str | None,
    authority_kind: str,
    authority_ref: str,
    idempotency_key: str,
    not_before: datetime,
) -> dict[str, Any]:
    """The insert values of one ``requested`` read, checked like ``create_read_execution``.

    @spec AUTOMATED-REMEDIATION-12 @spec AUTOMATED-REMEDIATION-18. For a producer
    that schedules a whole series inside its own transaction (the verifier):
    nothing is written or committed here. Raises ``ReadRefused``.
    """

    if not _well_formed(
        connector=connector,
        connector_digest=connector_digest,
        tool=tool,
        authority_kind=authority_kind,
        authority_ref=authority_ref,
        idempotency_key=idempotency_key,
        not_before=not_before,
    ) or not _pointer_allowed(tool, pointer):
        raise ReadRefused("authority_unavailable", "no remediation authority names this read")
    try:
        text = canonical_arguments(arguments)
    except (TypeError, ValueError):
        raise ReadRefused("arguments_mismatch", "the arguments have no canonical form") from None
    return {
        "id": uuid.uuid4(),
        "kind": ExecutionKind.read.value,
        "agent_id": agent_id,
        "connector": connector,
        "tool": tool,
        "subject_action_id": None,
        "connector_digest": connector_digest,
        "arguments_sha256": arguments_sha256(arguments),
        "forward_arguments": json.loads(text),
        "pointer": pointer,
        "authority_kind": authority_kind,
        "authority_ref": authority_ref,
        "idempotency_key": idempotency_key,
        "not_before": not_before,
        "state": ExecutionState.requested.value,
        "attempt": 0,
    }


def _same_read(
    execution: ActionExecution,
    *,
    connector: str,
    connector_digest: str,
    tool: str,
    sha256: str,
    pointer: str | None,
    authority_kind: str,
    authority_ref: str,
) -> bool:
    return (
        execution.kind == ExecutionKind.read
        and execution.connector == connector
        and execution.connector_digest == connector_digest
        and execution.tool == tool
        and execution.arguments_sha256 == sha256
        and execution.pointer == pointer
        and execution.authority_kind == authority_kind
        and execution.authority_ref == authority_ref
    )


async def create_read_execution(
    session: AsyncSession,
    *,
    agent_id: uuid.UUID,
    connector: str,
    connector_digest: str,
    tool: str,
    arguments: Mapping[str, Any],
    pointer: str | None,
    authority_kind: str,
    authority_ref: str,
    idempotency_key: str,
    not_before: datetime,
) -> ReadCreated:
    """Create, or adopt on replay, one scheduled sample. Commits on success.

    @spec AUTOMATED-REMEDIATION-12. Raises ``ReadRefused``
    (``authority_unavailable``, ``arguments_mismatch``) having written nothing.
    """

    if not _well_formed(
        connector=connector,
        connector_digest=connector_digest,
        tool=tool,
        authority_kind=authority_kind,
        authority_ref=authority_ref,
        idempotency_key=idempotency_key,
        not_before=not_before,
    ) or not _pointer_allowed(tool, pointer):
        await session.rollback()
        raise ReadRefused("authority_unavailable", "no remediation authority names this read")
    try:
        text = canonical_arguments(arguments)
    except (TypeError, ValueError):
        await session.rollback()
        raise ReadRefused("arguments_mismatch", "the arguments have no canonical form") from None
    sha256 = arguments_sha256(arguments)
    if await session.get(Agent, agent_id) is None:
        await session.rollback()
        raise ReadRefused("authority_unavailable", "no agent names this read")

    same: dict[str, Any] = {
        "connector": connector,
        "connector_digest": connector_digest,
        "tool": tool,
        "sha256": sha256,
        "pointer": pointer,
        "authority_kind": authority_kind,
        "authority_ref": authority_ref,
    }
    while True:
        existing = await session.scalar(
            select(ActionExecution)
            .where(
                ActionExecution.agent_id == agent_id,
                ActionExecution.idempotency_key == idempotency_key,
            )
            .execution_options(populate_existing=True)
        )
        if existing is not None:
            await session.commit()
            if not _same_read(existing, **same):
                raise ReadRefused(
                    "arguments_mismatch",
                    "an execution under this key already names another call",
                )
            return ReadCreated(execution_id=existing.id, state=existing.state, created=False)
        created = await session.scalar(
            insert(ActionExecution)
            .values(
                id=uuid.uuid4(),
                kind=ExecutionKind.read.value,
                agent_id=agent_id,
                connector=connector,
                tool=tool,
                subject_action_id=None,
                connector_digest=connector_digest,
                arguments_sha256=sha256,
                forward_arguments=json.loads(text),
                pointer=pointer,
                authority_kind=authority_kind,
                authority_ref=authority_ref,
                idempotency_key=idempotency_key,
                not_before=not_before,
                state=ExecutionState.requested.value,
                attempt=0,
            )
            .on_conflict_do_nothing(constraint="uq_action_executions_agent_idempotency_key")
            .returning(ActionExecution.id)
        )
        await session.commit()
        if created is not None:
            return ReadCreated(
                execution_id=created, state=ExecutionState.requested.value, created=True
            )
        # A concurrent creation took this key first: read it again and adopt it.


def series_successor_due(now: datetime) -> ColumnElement[bool]:
    """A requested read has a later sample of its series that is already due.

    @spec AUTOMATED-REMEDIATION-12: "A sample that cannot be claimed before its
    next sample is due is recorded skipped". A series is the read executions of
    one agent with the same ``authority_ref``, connector, tool, arguments and
    pointer.
    """

    later = aliased(ActionExecution)
    return (
        select(later.id)
        .where(
            later.kind == ExecutionKind.read,
            later.id != ActionExecution.id,
            later.agent_id == ActionExecution.agent_id,
            later.authority_ref == ActionExecution.authority_ref,
            later.connector == ActionExecution.connector,
            later.tool == ActionExecution.tool,
            later.arguments_sha256 == ActionExecution.arguments_sha256,
            # An observe-only series has no pointer: NULL matches NULL here.
            later.pointer.is_not_distinct_from(ActionExecution.pointer),
            later.not_before > ActionExecution.not_before,
            later.not_before <= now,
        )
        .exists()
    )


def missed_samples(now: datetime) -> ColumnElement[bool]:
    """Requested, due reads whose series successor is due too: never claimed."""

    return and_(
        ActionExecution.kind == ExecutionKind.read,
        ActionExecution.state == ExecutionState.requested,
        ActionExecution.not_before <= now,
        series_successor_due(now),
    )


__all__ = [
    "OBSERVE_TOOL",
    "READ_AUTHORITY_KINDS",
    "ReadCreated",
    "ReadRefused",
    "create_read_execution",
    "missed_samples",
    "scheduled_read",
    "series_successor_due",
]
