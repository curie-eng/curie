"""Deliver and acknowledge pinned requester coding instructions (ADR 0278 section 3, 6).

The worker reads the running request's version 1 context envelope here, posts
it to the runner, and relays the runner's acknowledgment back. The API accepts
an acknowledgment only for the current request and execution fence, and moves
the request's evidence from ``pending`` to ``applied`` or ``failed``, or keeps
``none``. A selection whose tenant disabled the capability after pinning is
refused ``tenant_disabled`` and the snapshot is kept.

Nothing here logs or returns the text except the envelope itself, which only
the internal worker credential can read.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Literal

from curie_internal.requester_coding_instructions import (
    AbsentCodingInstructions,
    CodingInstructionsAcknowledgmentV1,
    CodingInstructionsContextV1,
    SelectedCodingInstructions,
)
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Agent, ExecutionRequest, RequesterCodingPolicy, WorkItem, WorkItemCodingSnapshot
from .schemas.workitems import ExecutionBindingOut

ContextRefusal = Literal[
    "not_found",
    "stale_owner",
    "tenant_disabled",
    "coding_instructions_unavailable",
    "acknowledgment_mismatch",
    "evidence_conflict",
]

# Worker-side failure reasons relayed without a runner acknowledgment.
WorkerFailure = Literal[
    "tenant_disabled",
    "coding_instructions_unavailable",
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


async def _tenant_of(session: AsyncSession, item: WorkItem) -> uuid.UUID | None:
    tenant: uuid.UUID | None = await session.scalar(
        select(Agent.tenant_id).where(Agent.id == item.agent_id)
    )
    return tenant


async def execution_binding(
    session: AsyncSession, item: WorkItem, request: ExecutionRequest
) -> ExecutionBindingOut | None:
    """The binding the request's runner checks its envelope against.

    A runner reads it when it boots, which is after the worker acquired the
    request and before the worker starts it: the start raises the fence by one
    (``workitem_dispatch.start``), so an acquired request is bound to the fence
    its start assigns. A runner that boots later reads the running fence.
    """

    tenant = await _tenant_of(session, item)
    if tenant is None:
        return None
    if request.status == "running" and request.runtime_epoch >= 1:
        fence = request.runtime_epoch
    elif request.status == "waiting" and request.acquire_owner is not None:
        fence = request.runtime_epoch + 1
    else:
        return None
    return ExecutionBindingOut(
        tenant_id=tenant,
        work_item_id=item.id,
        execution_request_id=request.id,
        execution_fence=fence,
    )


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


async def read_envelope(
    session: AsyncSession, request_id: uuid.UUID, *, runtime_epoch: int
) -> dict[str, Any] | Refused:
    """The version 1 envelope for the running request at ``runtime_epoch``."""

    found = await _running(session, request_id, runtime_epoch, lock=False)
    if isinstance(found, Refused):
        return found
    request, item = found
    tenant = await _tenant_of(session, item)
    if tenant is None:
        return Refused("coding_instructions_unavailable")
    selection: SelectedCodingInstructions | AbsentCodingInstructions
    if item.coding_instructions_state == "selected":
        enabled = await session.scalar(
            select(RequesterCodingPolicy.enabled).where(RequesterCodingPolicy.tenant_id == tenant)
        )
        if enabled is False:
            return Refused("tenant_disabled")
        text = await session.scalar(
            select(WorkItemCodingSnapshot.text).where(
                WorkItemCodingSnapshot.work_item_id == item.id
            )
        )
        try:
            # Validated, not trusted: a stored row that no longer matches its
            # digest or bounds is refused below.
            selection = SelectedCodingInstructions.model_validate(
                {
                    "principal_id": str(item.requester_principal_id),
                    "revision": item.coding_instructions_revision,
                    "sha256": item.coding_instructions_sha256,
                    "format_version": item.coding_instructions_format_version,
                    "text": text,
                }
            )
        except (ValidationError, ValueError, TypeError):
            # A missing or tampered snapshot is a visible failure, never absence.
            return Refused("coding_instructions_unavailable")
    else:
        selection = AbsentCodingInstructions.model_validate(
            {"reason": item.coding_instructions_absence_reason or "historical"}
        )
    envelope = CodingInstructionsContextV1(
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
    ack: CodingInstructionsAcknowledgmentV1 | None = None
    if body.acknowledgment is not None:
        try:
            ack = CodingInstructionsAcknowledgmentV1.model_validate(body.acknowledgment)
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
    current = request.coding_instructions_evidence
    selected = item.coding_instructions_state == "selected"
    if ack is not None and ack.outcome == "applied":
        if not selected or ack.sha256 != item.coding_instructions_sha256:
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
            # An accepted selection is never downgraded by a later failure.
            await session.rollback()
            return Refused("evidence_conflict")
        evidence = "failed"
    if evidence != current:
        await session.execute(
            update(ExecutionRequest)
            .where(
                ExecutionRequest.id == request.id,
                ExecutionRequest.runtime_epoch == body.runtime_epoch,
            )
            .values(
                coding_instructions_selection_id=request.coding_instructions_selection_id
                or item.id,
                coding_instructions_evidence=evidence,
            )
        )
    await session.commit()
    return evidence


async def applied_digest(session: AsyncSession, request_id: uuid.UUID) -> str | None:
    """The acknowledged digest a publication of this request carries, else None."""

    row = (
        await session.execute(
            select(
                ExecutionRequest.coding_instructions_evidence, WorkItem.coding_instructions_sha256
            )
            .join(WorkItem, WorkItem.id == ExecutionRequest.work_item_id)
            .where(ExecutionRequest.id == request_id)
        )
    ).one_or_none()
    if row is None or row[0] != "applied" or not row[1]:
        return None
    return str(row[1])


ANNOTATION_PREFIX = "Requester preferences:"


def carries_annotation(body: str | None) -> bool:
    """Whether a submitted body already holds a line the platform reserves."""

    return body is not None and any(
        line.startswith(ANNOTATION_PREFIX) for line in body.splitlines()
    )


def annotate(body: str, digest: str) -> str:
    """``body`` with the one annotation line last; no name, principal or text."""

    line = f"{ANNOTATION_PREFIX} applied; sha256:{digest}"
    return f"{body.rstrip()}\n\n{line}" if body.strip() else line
