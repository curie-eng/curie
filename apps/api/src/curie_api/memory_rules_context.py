"""Deliver and acknowledge pinned learned rules, and name them on the pull request (#5798).

The worker reads the running request's version 1 learned rules envelope here,
posts it to the runner beside the requester envelope, and relays the runner's
acknowledgment back. The API accepts an acknowledgment only for the current
request and execution fence, and moves the request's ``memory_rules_evidence``
from ``pending`` to ``applied`` or ``failed``, or keeps ``none``.

Nothing here logs or returns the rule text except the envelope itself, which
only the internal worker credential can read. The pull request line names the
rule numbers and digest only.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Literal

from curie_internal.memory_rules import (
    AbsentMemoryRules,
    MemoryRulesAcknowledgmentV1,
    MemoryRulesContextV1,
    MemoryRulesSelectedV1,
)
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from .factory_tracker_result_runtime import CONTRACT_IMPLEMENTATION
from .models import Agent, ExecutionRequest, WorkItem, WorkItemMemoryRuleSnapshot

ContextRefusal = Literal[
    "not_found",
    "stale_owner",
    "memory_rules_unavailable",
    "acknowledgment_mismatch",
    "evidence_conflict",
]

# Worker-side failure reasons relayed without a runner acknowledgment.
WorkerFailure = Literal[
    "memory_rules_unavailable",
    "runner_unacknowledged",
    "runner_refused",
]


@dataclass(frozen=True)
class Refused:
    code: ContextRefusal


class AcknowledgmentBody(BaseModel):
    """What the worker relays: the runner's acknowledgment, or its own failure."""

    model_config = ConfigDict(extra="forbid")

    runtime_epoch: int = Field(ge=1)
    acknowledgment: dict[str, Any] | None = None
    # The runner's refusal reason or the worker's own failure; a short code.
    reason: str | None = Field(default=None, max_length=64, pattern=r"^[a-z_]+$")


class ContextBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    runtime_epoch: int = Field(ge=1)


async def _running(
    session: AsyncSession, request_id: uuid.UUID, runtime_epoch: int, *, lock: bool
) -> tuple[ExecutionRequest, WorkItem] | Refused:
    query = select(ExecutionRequest).where(ExecutionRequest.id == request_id)
    if lock:
        query = query.with_for_update()
    request = await session.scalar(query.execution_options(populate_existing=True))
    if request is None:
        return Refused("not_found")
    if request.status != "running" or request.runtime_epoch != runtime_epoch:
        return Refused("stale_owner")
    item = await session.scalar(select(WorkItem).where(WorkItem.id == request.work_item_id))
    if item is None:
        return Refused("not_found")
    return request, item


def _delivers(request: ExecutionRequest) -> bool:
    """Whether the request's session receives the pinned rules (the implementer only)."""

    return request.completion_contract == CONTRACT_IMPLEMENTATION


async def read_envelope(
    session: AsyncSession, request_id: uuid.UUID, *, runtime_epoch: int
) -> dict[str, Any] | Refused:
    """The version 1 learned rules envelope for the running request at ``runtime_epoch``."""

    found = await _running(session, request_id, runtime_epoch, lock=False)
    if isinstance(found, Refused):
        return found
    request, item = found
    tenant = await session.scalar(select(Agent.tenant_id).where(Agent.id == item.agent_id))
    if tenant is None:
        return Refused("memory_rules_unavailable")
    selection: MemoryRulesSelectedV1 | AbsentMemoryRules
    if item.memory_rules_state == "selected" and _delivers(request):
        snapshot = await session.scalar(
            select(WorkItemMemoryRuleSnapshot).where(
                WorkItemMemoryRuleSnapshot.work_item_id == item.id
            )
        )
        if snapshot is None or snapshot.sha256 != item.memory_rules_sha256:
            # A missing snapshot, or one that no longer matches the pin, is a
            # visible failure, never absence.
            return Refused("memory_rules_unavailable")
        try:
            # Validated, not trusted: the stored rows are revalidated against
            # the digest and every bound before they are served.
            selection = MemoryRulesSelectedV1.model_validate(
                {
                    "rule_numbers": [int(number) for number in snapshot.rule_numbers],
                    "sha256": item.memory_rules_sha256,
                    "format_version": item.memory_rules_format_version,
                    "text": snapshot.text,
                }
            )
        except (ValidationError, ValueError, TypeError):
            return Refused("memory_rules_unavailable")
        if len(selection.rule_numbers) != item.memory_rules_count:
            return Refused("memory_rules_unavailable")
    else:
        selection = AbsentMemoryRules(reason="no_rules")
    envelope = MemoryRulesContextV1(
        tenant_id=str(tenant),
        work_item_id=str(item.id),
        execution_request_id=str(request.id),
        execution_fence=request.runtime_epoch,
        selection=selection,
    )
    return envelope.model_dump(mode="json")


async def accept_acknowledgment(
    session: AsyncSession, request_id: uuid.UUID, body: AcknowledgmentBody
) -> str | Refused:
    """Record the relayed outcome for the current fence; the resulting evidence."""

    found = await _running(session, request_id, body.runtime_epoch, lock=True)
    if isinstance(found, Refused):
        await session.rollback()
        return found
    request, item = found
    ack: MemoryRulesAcknowledgmentV1 | None = None
    if body.acknowledgment is None and body.reason is None:
        # Neither a runner acknowledgment nor a worker failure: nothing to record.
        await session.rollback()
        return Refused("acknowledgment_mismatch")
    if body.acknowledgment is not None:
        try:
            if "envelope_version" not in body.acknowledgment:
                # The model defaults the version; an acknowledgment must name it.
                raise ValueError("missing envelope_version")
            ack = MemoryRulesAcknowledgmentV1.model_validate(body.acknowledgment)
        except (ValidationError, ValueError):
            await session.rollback()
            return Refused("acknowledgment_mismatch")
        if (
            ack.execution_request_id != str(request.id)
            or ack.work_item_id != str(item.id)
            or ack.execution_fence != request.runtime_epoch
        ):
            await session.rollback()
            return Refused("acknowledgment_mismatch")
    current = request.memory_rules_evidence
    selected = item.memory_rules_state == "selected" and _delivers(request)
    if ack is not None and ack.outcome == "applied":
        if not selected or ack.sha256 != item.memory_rules_sha256:
            await session.rollback()
            return Refused("acknowledgment_mismatch")
        if current not in ("pending", "applied"):
            await session.rollback()
            return Refused("evidence_conflict")
        evidence = "applied"
    elif ack is not None and ack.outcome == "none":
        if selected:
            await session.rollback()
            return Refused("acknowledgment_mismatch")
        if current != "none":
            await session.rollback()
            return Refused("evidence_conflict")
        evidence = "none"
    else:
        if current == "applied":
            # An applied pin is never downgraded by a later failure.
            await session.rollback()
            return Refused("evidence_conflict")
        # A failure marks a pinned selection failed. An absent pin carries no rules to
        # fail, so its evidence stays ``none``: the refusal is the request's terminal
        # detail (the worker records it), never a rewrite of the evidence.
        evidence = "failed" if selected else current
    if evidence != current:
        await session.execute(
            update(ExecutionRequest)
            .where(
                ExecutionRequest.id == request.id,
                ExecutionRequest.runtime_epoch == body.runtime_epoch,
            )
            .values(memory_rules_evidence=evidence)
        )
    await session.commit()
    return evidence


@dataclass(frozen=True)
class AppliedRules:
    rule_numbers: tuple[int, ...]
    sha256: str


async def applied_rules(session: AsyncSession, request_id: uuid.UUID) -> AppliedRules | None:
    """The acknowledged pin a publication of this request names, else None."""

    row = (
        await session.execute(
            select(
                ExecutionRequest.memory_rules_evidence,
                WorkItem.memory_rules_count,
                WorkItem.memory_rules_sha256,
                WorkItemMemoryRuleSnapshot.rule_numbers,
                WorkItemMemoryRuleSnapshot.sha256,
            )
            .join(WorkItem, WorkItem.id == ExecutionRequest.work_item_id)
            .outerjoin(
                WorkItemMemoryRuleSnapshot,
                WorkItemMemoryRuleSnapshot.work_item_id == WorkItem.id,
            )
            .where(ExecutionRequest.id == request_id)
        )
    ).one_or_none()
    if row is None:
        return None
    evidence, count, digest, numbers, snapshot_digest = row
    if evidence != "applied" or count < 1 or not digest or not numbers:
        return None
    if snapshot_digest != digest or len(numbers) != count:
        return None
    return AppliedRules(rule_numbers=tuple(int(number) for number in numbers), sha256=str(digest))


ANNOTATION_PREFIX = "Learned rules:"
_REQUESTER_PREFIX = "Requester preferences:"


def applied_line(applied: AppliedRules) -> str:
    """``Learned rules: in force; rules:R1,R4; sha256:<digest>``, in canonical order."""

    numbers = ",".join(f"R{number}" for number in applied.rule_numbers)
    return f"{ANNOTATION_PREFIX} in force; rules:{numbers}; sha256:{applied.sha256}"


def carries_annotation(body: str | None) -> bool:
    """Whether a submitted body already holds a line the platform reserves."""

    return body is not None and any(
        line.startswith(ANNOTATION_PREFIX) for line in body.splitlines()
    )


def annotate(body: str, applied: AppliedRules) -> str:
    """``body`` with the learned rules line immediately before the requester line.

    The requester line, when the platform wrote one, stays last; without it the
    learned rules line is last. No rule text is ever written.
    """

    line = applied_line(applied)
    lines = body.rstrip().splitlines()
    if lines and lines[-1].startswith(_REQUESTER_PREFIX):
        return "\n".join([*lines[:-1], line, lines[-1]])
    return f"{body.rstrip()}\n\n{line}" if body.strip() else line
