"""An operator's retry of a failed factory WorkItem (#5431, ADR 0269).

One administrative operation in one transaction. It never builds its own
admission: it reruns the automatic successor admission
(``lifecycle.admit_automatic_successor``) against the WorkItem's newest
request, with the disposition ``factory_retry_policy.decide`` derives from that
request's stored typed facts. So an operator can admit now exactly what
automatic recovery would admit now, inside the same shared allowance, fences,
backoff, publication only preference and candidate transfer. What it adds is a
fresh evaluation once a transient fence cleared, a durable idempotency record,
an audit row, and named refusals. An ordinary failure that settled before
automatic recovery existed carries no safety fact and is refused
``side_effects_unproven``.

The operator's request UUID is the idempotency key. An identical replay
(same WorkItem, expected version, stripped reason and server derived actor)
answers the stored result even after the version advanced; the same UUID with
other facts conflicts. Only an admission is recorded. Every refusal and every
dry run rolls the whole transaction back, so nothing of an admission survives
it and the UUID stays free. The operation writes no tracker label, posts no
notice and grants no allowance.
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import replace
from typing import Any, Literal, cast

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from .. import factory_retry_policy
from ..auth import AdministrativeActor
from ..factory_retry_policy import Disposition, DispositionKind, RetryFacts, RetrySafety
from ..factory_tracker_result_runtime import CONTRACT_TRACKER_RESULT
from ..models import ExecutionRequest, WorkItemOperatorRetry
from ..schemas.workitems import WorkItemRetryIn, WorkItemRetryOut
from . import lifecycle
from .lifecycle import AUTOMATIC_RETRY_LIMIT, admit_automatic_successor

logger = logging.getLogger(__name__)

RetryRefusalCode = Literal[
    "not_found",
    "idempotency_conflict",
    "stale_version",
    "work_item_cancelled",
    "readmit_pending",
    "active_execution",
    "no_failed_request",
    "teardown_unconfirmed",
    "allowance_exhausted",
    "not_retryable",
    "side_effects_unproven",
    "admission_refused",
]

_MESSAGES: dict[str, str] = {
    "not_found": "No such work item.",
    "idempotency_conflict": (
        "This request id was already used for a different retry; use a new request id."
    ),
    "stale_version": (
        "The work item changed since it was read; read it again and pass its current version."
    ),
    "work_item_cancelled": "The work item is cancelled, so it is never retried.",
    "readmit_pending": "A relabel readmission is already pending for this work item.",
    "active_execution": "The work item already has an active request; wait for it to settle.",
    "no_failed_request": "The newest request of this work item did not fail, so nothing retries.",
    "teardown_unconfirmed": (
        "The failed request's runtime teardown is not observed yet; retry once it is."
    ),
    "allowance_exhausted": (
        "Every automatic retry of this work item is used; this command grants no more."
    ),
    "not_retryable": "This failure's cause never authorizes another attempt.",
    "side_effects_unproven": (
        "The failed request may have had external effects; only a proven clean failure retries."
    ),
    "admission_refused": (
        "Automatic recovery refuses this retry now: a publication may still be in flight, "
        "the pull request lineage closed, a newer revision queued, or no exact retained "
        "candidate exists."
    ),
}


class RetryRefused(Exception):
    """A named refusal; the transaction is already rolled back and wrote nothing."""

    def __init__(self, code: RetryRefusalCode, **facts: Any) -> None:
        super().__init__(code)
        self.code = code
        self.facts = facts

    @property
    def status_code(self) -> int:
        return 404 if self.code == "not_found" else 409

    def detail(self) -> dict[str, Any]:
        if self.code == "not_found":
            return {"code": "not_found"}
        facts = {
            key: str(value) if isinstance(value, uuid.UUID) else value
            for key, value in self.facts.items()
        }
        return {"code": self.code, "message": self.message(), **facts}

    def message(self) -> str:
        """The operator sentence; the exhausted allowance names its count and cause."""

        facts = self.facts
        if self.code == "allowance_exhausted":
            return (
                f"All {facts['retries_used']} of {facts['retry_limit']} automatic retries of "
                f"this work item are used; the newest request failed with {facts['cause']}. "
                "This command grants no more."
            )
        if self.code == "not_retryable":
            return f"A failure with cause {facts['cause']} never authorizes another attempt."
        if self.code == "side_effects_unproven":
            return (
                f"The request that failed with {facts['cause']} reported retry safety "
                f"{facts['retry_safety'] or 'none'}; only a proven clean failure retries."
            )
        return _MESSAGES[self.code]


def facts_sha256(work_item_id: uuid.UUID, body: WorkItemRetryIn, actor: AdministrativeActor) -> str:
    """The canonical facts a request UUID is bound to; ``dry_run`` is not one."""

    canonical = json.dumps(
        {
            "work_item_id": str(work_item_id),
            "expected_version": body.expected_version,
            "reason": body.reason,
            "actor_kind": actor.kind,
            "actor_subject": actor.subject,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


async def _refuse(
    session: AsyncSession,
    code: RetryRefusalCode,
    *,
    work_item_id: uuid.UUID,
    actor: AdministrativeActor,
    **facts: Any,
) -> RetryRefused:
    await session.rollback()
    # Never the reason text: it is operator prose.
    logger.info(
        "operator retry refused",
        extra={"work_item_id": str(work_item_id), "code": code, "actor_kind": actor.kind},
    )
    return RetryRefused(code, **facts)


def _safety(request: ExecutionRequest) -> RetrySafety | None:
    value = request.retry_safety
    if value in ("no_effects", "flagged", "uncertain"):
        return cast(RetrySafety, value)
    return None


def _facts(request: ExecutionRequest) -> RetryFacts:
    """The stored typed facts, read the way automatic settlement reads them."""

    return RetryFacts(
        cause=request.terminal_cause or "",
        safety=_safety(request),
        condition_reason=request.retry_condition_reason,
        condition_retry_at=request.retry_condition_retry_at,
        condition_scope_key=request.retry_condition_scope_key,
    )


async def retry_work_item(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    body: WorkItemRetryIn,
    actor: AdministrativeActor,
) -> WorkItemRetryOut:
    """Admit, or plan in a dry run, one fresh request for a failed WorkItem.

    Raises ``RetryRefused`` with the transaction rolled back. On success the
    transaction is committed (or, in a dry run, rolled back).
    """

    async def refuse(code: RetryRefusalCode, **facts: Any) -> RetryRefused:
        return await _refuse(session, code, work_item_id=work_item_id, actor=actor, **facts)

    work_item = await lifecycle.lock_work_item(session, work_item_id)
    if work_item is None:
        raise await refuse("not_found")
    digest = facts_sha256(work_item_id, body, actor)
    # The WorkItem lock serializes identical calls, so a second one reads the
    # first's committed record here (READ COMMITTED, a fresh statement snapshot).
    recorded = await session.scalar(
        select(WorkItemOperatorRetry)
        .where(WorkItemOperatorRetry.request_id == body.request_id)
        .execution_options(populate_existing=True)
    )
    if recorded is not None:
        if recorded.work_item_id != work_item_id or recorded.facts_sha256 != digest:
            raise await refuse("idempotency_conflict", request_id=body.request_id)
        replayed = WorkItemRetryOut.model_validate({**recorded.result, "replayed": True})
        await session.rollback()
        return replayed
    if work_item.version != body.expected_version:
        raise await refuse("stale_version", current_version=work_item.version)
    if work_item.cancelled_at is not None:
        raise await refuse("work_item_cancelled")
    if work_item.readmit_request_id is not None:
        raise await refuse("readmit_pending")
    active = await lifecycle.lock_active_request(session, work_item_id)
    if active is None:
        active = await session.scalar(
            select(ExecutionRequest)
            .where(
                ExecutionRequest.work_item_id == work_item_id,
                ExecutionRequest.status == "queued",
            )
            .limit(1)
        )
    if active is not None:
        raise await refuse("active_execution", request_id=active.id, status=active.status)
    predecessor = await session.scalar(
        select(ExecutionRequest)
        .where(ExecutionRequest.work_item_id == work_item_id)
        .order_by(ExecutionRequest.sequence.desc())
        .limit(1)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if predecessor is None or predecessor.status != "failed":
        raise await refuse(
            "no_failed_request",
            latest_status=None if predecessor is None else predecessor.status,
            cause=None if predecessor is None else predecessor.terminal_cause,
        )
    cause = predecessor.terminal_cause or ""
    if predecessor.teardown_unconfirmed_at is not None:
        raise await refuse("teardown_unconfirmed", request_id=predecessor.id)
    if work_item.automatic_retries_used >= AUTOMATIC_RETRY_LIMIT:
        raise await refuse(
            "allowance_exhausted",
            retries_used=work_item.automatic_retries_used,
            retry_limit=AUTOMATIC_RETRY_LIMIT,
            cause=cause,
        )
    origin: Literal["owner_loss", "ordinary"]
    disposition: Disposition | None
    if cause == "owner_lost":
        # ADR 0206's teardown based exception, exactly as the teardown path
        # calls it: no safety fact and no policy disposition.
        origin, disposition = "owner_loss", None
    else:
        origin = "ordinary"
        facts = _facts(predecessor)
        safety = facts.safety
        disposition = factory_retry_policy.decide(facts)
        if predecessor.completion_contract == CONTRACT_TRACKER_RESULT or cause == "budget_exceeded":
            raise await refuse("not_retryable", cause=cause, disposition=disposition.kind.value)
        if not disposition.successor:
            # A cause that would admit after a proven clean run names the
            # missing proof, not the cause.
            clean = factory_retry_policy.decide(replace(facts, safety="no_effects"))
            if clean.successor and safety != "no_effects":
                raise await refuse("side_effects_unproven", cause=cause, retry_safety=safety)
            raise await refuse("not_retryable", cause=cause, disposition=disposition.kind.value)
        if disposition.kind == DispositionKind.PUBLICATION_RECOVERY:
            if safety == "uncertain":
                raise await refuse("side_effects_unproven", cause=cause, retry_safety=safety)
        elif predecessor.execution_kind == "publication_recovery":
            # A publication failure never buys an implementation run (#5429).
            raise await refuse("not_retryable", cause=cause, disposition=disposition.kind.value)
        elif safety != "no_effects":
            raise await refuse("side_effects_unproven", cause=cause, retry_safety=safety)

    admitted = await admit_automatic_successor(
        session, work_item, predecessor, origin=origin, disposition=disposition
    )
    successor = await lifecycle.automatic_successor_of(session, predecessor) if admitted else None
    if successor is None:
        raise await refuse(
            "admission_refused",
            cause=cause,
            disposition="owner_loss" if disposition is None else disposition.kind.value,
        )
    after = await lifecycle.reload_work_item(session, work_item_id)
    kind = cast(Literal["implementation", "publication_recovery"], successor.execution_kind)
    plan: dict[str, Any] = {
        "work_item_id": work_item_id,
        "request_id": body.request_id,
        "dry_run": body.dry_run,
        "replayed": False,
        "disposition": kind,
        "predecessor_request_id": predecessor.id,
        "predecessor_cause": cause,
        "successor_dispatch_not_before": successor.dispatch_not_before,
        "successor_blocked_on": successor.blocked_on,
        "work_item_version": after.version,
        "retries_used": after.automatic_retries_used,
        "retry_limit": AUTOMATIC_RETRY_LIMIT,
        "actor_kind": actor.kind,
    }
    if body.dry_run:
        # The successor's id would not survive the rollback.
        result = WorkItemRetryOut(**plan, successor_request_id=None, recorded_at=None)
        await session.rollback()
        return result
    recorded_at = await session.scalar(select(func.now()))
    result = WorkItemRetryOut(**plan, successor_request_id=successor.id, recorded_at=recorded_at)
    try:
        async with session.begin_nested():
            session.add(
                WorkItemOperatorRetry(
                    request_id=body.request_id,
                    work_item_id=work_item_id,
                    actor_kind=actor.kind,
                    actor_subject=actor.subject,
                    reason=body.reason,
                    expected_version=body.expected_version,
                    facts_sha256=digest,
                    disposition=kind,
                    predecessor_request_id=predecessor.id,
                    successor_request_id=successor.id,
                    result=result.model_dump(mode="json"),
                    created_at=recorded_at,
                )
            )
            await session.flush()
    except IntegrityError:
        # The same UUID committed for another WorkItem since the read above.
        raise await refuse("idempotency_conflict", request_id=body.request_id) from None
    await session.commit()
    logger.info(
        "operator retry admitted",
        extra={
            "work_item_id": str(work_item_id),
            "successor_request_id": str(successor.id),
            "disposition": kind,
            "actor_kind": actor.kind,
        },
    )
    return result


__all__ = ["RetryRefused", "RetryRefusalCode", "facts_sha256", "retry_work_item"]
