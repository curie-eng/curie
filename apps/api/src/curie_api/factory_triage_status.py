"""The API's reads of a tracker-result request's delivered result (#5586 decisions 11, 12).

A tracker-result request's ending comes from its delivered receipt: a receipt
row exists exactly when the request's one delivery reached ``delivered``. The
status comment renders it through ``curie_internal.factory_triage`` and the
state label shows ``curie-factory:triaged`` while it exists, whatever the
execution status, except a cancelled request, which shows no state label.
Implementation requests never have a receipt and keep their mapping unchanged.
"""

from __future__ import annotations

import uuid
from typing import Any

from curie_internal.factory_triage import TRIAGED_LABEL, HandoffOutcome, TriageResult
from sqlalchemy import and_, exists, literal, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from .models import (
    ExecutionRequest,
    FactoryStatusComment,
    FactoryTrackerResultReceipt,
    FactoryTriageHandoff,
    WorkItem,
)

_NEEDS_HUMAN = "curie-factory:needs-human"
# The live statuses keep the implementation mapping (queued, running).
_TERMINAL_WITHOUT_RESULT = ("completed", "failed", "expired")


def _tracker_result() -> str:
    # Imported at call time: the runtime module sits above the notices module.
    from .factory_tracker_result_runtime import CONTRACT_TRACKER_RESULT

    return CONTRACT_TRACKER_RESULT


def is_tracker_result(request: ExecutionRequest) -> bool:
    return bool(request.completion_contract == _tracker_result())


def triage_delivered_sql() -> Any:
    """True for the outer ``ExecutionRequest`` row when its result was delivered."""

    return (
        exists()
        .where(FactoryTrackerResultReceipt.execution_request_id == ExecutionRequest.id)
        .correlate(ExecutionRequest)
    )


def triage_label_sql() -> Any:
    """The ``case`` arms that decide a tracker-result request's label, before any other arm.

    Each arm is guarded by the contract, so an implementation request falls
    through every one of them to the unchanged mapping. A cancelled WorkItem
    (its trigger label removed) shows no state label even when its request
    completed and delivered, so the query using these arms joins ``WorkItem``.
    """

    contract = ExecutionRequest.completion_contract == _tracker_result()
    return (
        (
            and_(
                contract,
                or_(
                    ExecutionRequest.status == "cancelled",
                    WorkItem.cancelled_at.is_not(None),
                ),
            ),
            literal(""),
        ),
        (and_(contract, triage_delivered_sql()), literal(TRIAGED_LABEL)),
        (
            and_(contract, ExecutionRequest.status.in_(_TERMINAL_WITHOUT_RESULT)),
            literal(_NEEDS_HUMAN),
        ),
    )


def triage_label(status: str, *, delivered: bool, work_item_cancelled: bool = False) -> str | None:
    """The label of a tracker-result request, or None to use the live status mapping.

    A completed request without a delivered result cannot happen (completion
    requires a verified receipt); it shows needs-human rather than pr-open.
    """

    if status == "cancelled" or work_item_cancelled:
        return ""
    if delivered:
        return TRIAGED_LABEL
    if status in _TERMINAL_WITHOUT_RESULT:
        return _NEEDS_HUMAN
    return None


async def delivered_triage(
    session: AsyncSession, execution_request_id: uuid.UUID
) -> TriageResult | None:
    """The delivered result of a request, or None while none is delivered."""

    row = (
        await session.execute(
            select(
                FactoryTrackerResultReceipt.result_kind,
                FactoryTrackerResultReceipt.reason,
                FactoryTrackerResultReceipt.revision,
                FactoryTrackerResultReceipt.output_url,
                FactoryTrackerResultReceipt.result_comment_url,
            )
            .where(FactoryTrackerResultReceipt.execution_request_id == execution_request_id)
            .order_by(FactoryTrackerResultReceipt.delivered_at)
            .limit(1)
        )
    ).one_or_none()
    if row is None:
        return None
    kind, reason, revision, output_url, comment_url = row
    return TriageResult(
        result_kind=kind,
        reason=reason,
        revision=revision,
        ticket_url=output_url,
        result_comment_url=comment_url,
    )


async def recorded_handoff(
    session: AsyncSession, execution_request_id: uuid.UUID
) -> HandoffOutcome | None:
    """The settled implementation handoff of a delivered ticket (#5588), or None.

    None while no handoff exists (a source admitted before #5588) or while it
    is still unsettled; the ending then shows the ticket as published.
    """

    row = (
        await session.execute(
            select(
                FactoryTriageHandoff.outcome,
                FactoryTriageHandoff.reason,
                FactoryTriageHandoff.output_url,
            ).where(
                FactoryTriageHandoff.source_execution_request_id == execution_request_id,
                FactoryTriageHandoff.outcome.is_not(None),
            )
        )
    ).one_or_none()
    if row is None:
        return None
    outcome, reason, output_url = row
    admitted = outcome == "admitted"
    return HandoffOutcome(
        admitted=admitted,
        # The implementation run's status lives on the published ticket.
        implementation_status_url=output_url if admitted else None,
        refusal_reason=None if admitted else reason,
    )


async def on_tracker_result_settled(session: AsyncSession, execution_request_id: uuid.UUID) -> None:
    """Let the status sync render a request again once its delivery settled.

    Call it in the transaction that records a receipt or fails a delivery. A
    finalized comment is reopened, and a sync holding the lease is flagged so
    its writeback leaves the row due; the label follows from the receipt on
    the next pass. A refused comment stays refused.
    """

    await session.execute(
        update(FactoryStatusComment)
        .where(
            FactoryStatusComment.execution_request_id == execution_request_id,
            or_(
                FactoryStatusComment.finalized_at.is_not(None),
                FactoryStatusComment.sync_owner.is_not(None),
            ),
            FactoryStatusComment.refused_at.is_(None),
        )
        .values(
            finalized_at=None,
            sync_invalidated=FactoryStatusComment.sync_owner.is_not(None),
        )
        .execution_options(synchronize_session=False)
    )


__all__ = [
    "delivered_triage",
    "is_tracker_result",
    "on_tracker_result_settled",
    "recorded_handoff",
    "triage_delivered_sql",
    "triage_label",
    "triage_label_sql",
]
