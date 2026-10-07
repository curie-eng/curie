"""Approval SLA expiry sweeper (#412): resume the session an expiry stranded.

A pending approval whose ``expires_at`` lapses used to dead-end the suspended
session. The only prior expiry path lived inside the resolve endpoint (#244,
ADR-0010): a late resolver flipped the record to ``expired`` (410) and enqueued
NOTHING, so if no resolver ever arrived the session waited forever. This module
closes that gap with a periodic sweeper that (1) flips lapsed ``pending``
approvals to ``expired`` via the existing compare-and-set, (2) appends an
``expired`` audit row consistent with #247, and (3) enqueues a platform-authored
resume turn onto the same runs stream the resolve path uses, so the suspended
session resumes down its timeout branch (ADR-0003) and the channel placeholder
updates.

Concurrency and idempotency (why unattended sweeping is safe): the flip is
``crud.approvals.expire_approval``'s conditional UPDATE guarded on ``status = pending``, so
for one record exactly one writer wins -- one replica's sweeper, or a racing
resolver -- and every loser gets None back and neither audits nor enqueues. This
pending-guarded CAS is what guarantees a single wakeup: only the flip winner
ever enqueues.

A successful enqueue is recorded with ``crud.approvals.mark_approval_resumed`` (#418), the
same enqueue-first-then-mark ordering the resolve path uses: a NULL
``resumed_at`` on a flipped record means the wake never reached the stream, and
the resume reconciler (#411) re-enqueues it: on its next pass when this
process saw the enqueue fail (#4016), otherwise past its grace horizon. That is the
only recovery path for an expiry wake, because a flipped record is no longer
``pending`` and so is never re-selected by a later sweep. Marking before the
enqueue would write the wake off as delivered and strand the session for good.

Warning for anyone adding a re-enqueue path (retry, durable outbox, another
reconciler): the worker's done-marker (``markers.py``) only skips an event that
was already handled to a TERMINAL point (streamed to a final, or escalated). It
does NOT collapse a duplicate that lands while the resumed turn is still in
flight -- that duplicate would steer the live turn instead of being absorbed.
The shared ``resume_event_id`` (see ``resumequeue.build_expiry_resume_turn``)
prevents a redelivery of an already-finished turn from re-running; it does not
make a re-enqueue free, so do not rely on it as a mid-turn dedupe. What keeps
the reconciler's re-enqueue safe is its grace horizon (longer than the worker's
maximum turn), not the shared key; the one wake it retries sooner is one whose
XADD was observed failing (#4016), where no turn is in flight to collide with.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime, timedelta

from curie_telemetry import operation_span, record_metric
from opentelemetry.trace import SpanKind, StatusCode
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from curie_api.crud import approvals as crud_approvals
from curie_api.crud import publications as crud_publications

from .config import get_settings
from .models import ApprovalStatus
from .remediation_approvals import (
    REMEDIATION_PURPOSE,
    reconcile_remediation_approvals,
    settle_remediation_approval,
)
from .resumequeue import (
    ResumeQueue,
    approval_trace_context,
    build_expiry_resume_turn,
)

logger = logging.getLogger(__name__)
_monotonic = time.monotonic


async def observe_pending_approvals(
    session: AsyncSession, *, now: datetime | None = None
) -> None:
    """Publish authoritative fleet inventory; observation never gates a sweep."""

    try:
        count, oldest = await crud_approvals.pending_approval_inventory(session)
    except Exception as exc:  # noqa: BLE001 - existing broad catch retained
        logger.warning("pending approval telemetry observation failed (%s)", type(exc).__name__)
        return
    observed_at = now or datetime.now(UTC).replace(tzinfo=None)
    attributes = {
        "service.name": "curie-api",
        "operation": "observe",
        "outcome": "pending",
    }
    record_metric("curie.approval.pending", count, attributes=attributes)
    age = 0.0 if oldest is None else max(0.0, (observed_at - oldest).total_seconds())
    record_metric("curie.approval.pending.age", age, attributes=attributes)


async def sweep_expired_approvals(
    session: AsyncSession,
    resume_queue: ResumeQueue,
    *,
    now: datetime | None = None,
    limit: int = 100,
) -> int:
    """One sweep pass: expire every lapsed pending approval and wake its session.

    ``now`` defaults to naive UTC (matching ``expires_at`` and the router's
    ``_expired`` comparison); tests pass an explicit ``now`` to avoid real
    sleeps. Returns the number of records this pass flipped AND landed a resume
    turn on the stream for. The count is taken at the enqueue rather than after
    the mark below: at that point the wake is delivered, so a failed mark must
    not subtract from a wake the session already got.

    Per record, the order is flip -> audit -> enqueue, each guarded by a
    per-record try/except so one poisoned record cannot block the rest of the
    batch. Flip-first makes the DB the single arbiter of the flip/resolve race:
    only a successful CAS (``expire_approval`` returning the record) audits and
    enqueues; a None means a concurrent resolver or another replica's sweeper
    already claimed it, so this pass skips it silently.
    """

    now = now or datetime.now(UTC).replace(tzinfo=None)
    lapsed = await crud_approvals.list_expired_pending_approvals(session, now=now, limit=limit)
    # Read ids and private parents into plain values up front: the per-record
    # rollback below expires every ORM instance in this shared session, so
    # reading record fields lazily in a later iteration would trigger an
    # implicit reload (MissingGreenlet under the async session).
    approval_work = [
        (record.id, approval_trace_context(record)) for record in lapsed
    ]

    flipped = 0
    for approval_id, stored_parent in approval_work:
        try:
            with operation_span(
                "curie.approval.expire",
                kind=SpanKind.INTERNAL,
                parent=stored_parent,
                attributes={
                    "service.name": "curie-api",
                    "operation": "expire",
                },
            ):
                expired = await crud_approvals.expire_approval(session, approval_id)
            if expired is None:
                # A concurrent resolver or another replica's sweeper won the CAS;
                # the winner owns the audit and enqueue. No side effects here.
                continue
            await crud_approvals.append_approval_audit(
                session,
                approval_id=expired.id,
                action="expired",
                actor="system",
                actor_channel=None,
                decision="",
                authorizer="ExpirySweeper",
                authorized=True,
                reason=f"approval expired at {expired.expires_at}",
            )
            if expired.purpose == REMEDIATION_PURPOSE:
                # AUTOMATED-REMEDIATION-16: no execution and no model wake; the
                # nominations end expired.
                await settle_remediation_approval(session, expired.id, ApprovalStatus.expired)
            if expired.purpose in crud_approvals.NO_WAKE_PURPOSES:
                flipped += 1
                continue
            stream_id = await resume_queue.enqueue(
                build_expiry_resume_turn(expired), parent=stored_parent
            )
            flipped += 1
            # Enqueue-first-then-mark: only a wake that actually reached the
            # stream is written off. The mark's ``resumed_at IS NULL`` guard
            # makes a race with the reconciler a no-op rather than a conflict.
            await crud_approvals.mark_approval_resumed(session, expired.id)
            record_metric(
                "curie.approval.lifecycle",
                attributes={
                    "service.name": "curie-api",
                    "operation": "expire",
                    "outcome": "expired",
                },
            )
            logger.info(
                "approval %s expired by sweeper; resume turn enqueued (%s)",
                expired.id,
                stream_id,
            )
        except Exception:
            # Reset the shared session so a failed commit on this record cannot
            # poison the rest of the batch (PendingRollbackError); mirrors the
            # rollback-after-DB-error convention in the routers. If the flip
            # already committed, this record is left expired with ``resumed_at``
            # NULL, which is exactly the owed-wake shape the resume reconciler
            # (#411) re-enqueues (next pass for a failed enqueue, #4016; past
            # its grace horizon for a failed mark) -- so the wakeup is
            # retried rather than dropped, provided that backstop is enabled.
            await session.rollback()
            # The log must not name the enqueue as the failure: this except also
            # catches a failed mark, in which case the wake DID reach the stream.
            # An operator reads this line while deciding whether a session is
            # stranded, so it states the uncertainty instead of guessing.
            retry = (
                "the reconciler will re-enqueue it past its grace horizon (a "
                "redundant wake if it did land, which is the safe direction)"
                if get_settings().resume_reconciler_enabled
                else "nothing will retry it (resume reconciler disabled) and "
                "the wakeup may be lost"
            )
            logger.exception(
                "expiry sweep failed for approval %s after the flip; the resume "
                "turn may or may not have reached the stream, so %s",
                approval_id,
                retry,
            )
            continue
    # AUTOMATED-REMEDIATION-16: complete resolved remediation approvals whose
    # post-claim step (execution, nominations) did not commit.
    try:
        await reconcile_remediation_approvals(session, limit=limit)
    except Exception:
        await session.rollback()
        logger.exception("remediation approval reconciliation pass failed")
    await observe_pending_approvals(session, now=now)
    return flipped


async def run_expiry_sweeper(
    sessionmaker: async_sessionmaker[AsyncSession],
    resume_queue: ResumeQueue,
    interval_s: float,
    stop: asyncio.Event,
    *,
    publication_patch_retention_seconds: int = 3600,
) -> None:
    """Periodic loop driving ``sweep_expired_approvals`` until ``stop`` is set.

    Mirrors the worker heartbeat's sleep-or-stop shape (an
    ``asyncio.wait_for(stop.wait(), timeout=interval_s)`` that wakes early on
    shutdown) but INVERTS it to wait-FIRST: no sweep at t=0. That is deliberate,
    both boot hygiene (no DB query racing app startup) and a test-safety
    guarantee -- with a double-digit-second interval, no sweep fires inside any
    integration test's window, so the sweeper cannot leak into the frozen
    resolve-path expiry contract.

    A maintenance loop must never take down the API, so each pass runs inside a
    broad try/except: a failed pass (DB down, etc.) is logged and retried next
    interval rather than crashing the process.
    """

    last_success_monotonic: float | None = None
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
        except TimeoutError:
            pass
        if stop.is_set():
            break
        attributes = {
            "service.name": "curie-api",
            "operation": "approval-sweeper",
            "role": "background",
        }
        error: Exception | None = None
        with operation_span(
            "curie.background.approval-sweeper",
            kind=SpanKind.INTERNAL,
            attributes=attributes,
        ) as span:
            try:
                async with sessionmaker() as session:
                    await sweep_expired_approvals(session, resume_queue)
                    await crud_publications.reap_terminal_publication_patches(
                        session,
                        terminal_before=datetime.now(UTC).replace(tzinfo=None)
                        - timedelta(seconds=publication_patch_retention_seconds),
                        limit=100,
                    )
            except Exception as exc:  # noqa: BLE001 - existing broad catch retained
                error = exc
                if hasattr(span, "set_status"):
                    span.set_status(StatusCode.ERROR)
                span.add_event(
                    "background.pass.failed",
                    {"outcome": "failure", "error.class": type(exc).__name__},
                )
            else:
                span.add_event("background.pass.completed", {"outcome": "success"})
        outcome = "failure" if error is not None else "success"
        record_metric(
            "curie.background.loop",
            attributes={**attributes, "outcome": outcome},
        )
        now = _monotonic()
        if error is None:
            last_success_monotonic = now
        if last_success_monotonic is not None:
            record_metric(
                "curie.background.last_success.age",
                max(0.0, now - last_success_monotonic),
                attributes=attributes,
            )
        if error is not None:
            logger.error(
                "expiry sweep pass failed; retrying next interval (%s)",
                type(error).__name__,
            )
