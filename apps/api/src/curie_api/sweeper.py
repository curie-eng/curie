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
``crud.expire_approval``'s conditional UPDATE guarded on ``status = pending``, so
for one record exactly one writer wins -- one replica's sweeper, or a racing
resolver -- and every loser gets None back and neither audits nor enqueues. This
pending-guarded CAS is what guarantees a single wakeup: only the flip winner
ever enqueues.

A successful enqueue is recorded with ``crud.mark_approval_resumed`` (#418), the
same enqueue-first-then-mark ordering the resolve path uses: a NULL
``resumed_at`` on a flipped record means the wake never reached the stream, and
the resume reconciler (#411) re-enqueues it past its grace horizon. That is the
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
maximum turn), not the shared key.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from . import crud
from .config import get_settings
from .resumequeue import ResumeQueue, build_expiry_resume_turn

logger = logging.getLogger(__name__)


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
    lapsed = await crud.list_expired_pending_approvals(session, now=now, limit=limit)
    # Read the ids into plain values up front: the per-record rollback below
    # expires every ORM instance in this shared session, so reading record.id
    # lazily in a later iteration would trigger an implicit reload (MissingGreenlet
    # under the async session).
    approval_ids = [record.id for record in lapsed]

    flipped = 0
    for approval_id in approval_ids:
        try:
            expired = await crud.expire_approval(session, approval_id)
            if expired is None:
                # A concurrent resolver or another replica's sweeper won the CAS;
                # the winner owns the audit and enqueue. No side effects here.
                continue
            await crud.append_approval_audit(
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
            stream_id = await resume_queue.enqueue(build_expiry_resume_turn(expired))
            flipped += 1
            # Enqueue-first-then-mark: only a wake that actually reached the
            # stream is written off. The mark's ``resumed_at IS NULL`` guard
            # makes a race with the reconciler a no-op rather than a conflict.
            await crud.mark_approval_resumed(session, expired.id)
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
            # (#411) re-enqueues past its grace horizon -- so the wakeup is
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
                else "nothing will retry it (resume reconciler disabled) and the wakeup may be lost"
            )
            logger.exception(
                "expiry sweep failed for approval %s after the flip; the resume "
                "turn may or may not have reached the stream, so %s",
                approval_id,
                retry,
            )
            continue
    return flipped


async def run_expiry_sweeper(
    sessionmaker: async_sessionmaker[AsyncSession],
    resume_queue: ResumeQueue,
    interval_s: float,
    stop: asyncio.Event,
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

    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
        except TimeoutError:
            pass
        if stop.is_set():
            break
        try:
            async with sessionmaker() as session:
                await sweep_expired_approvals(session, resume_queue)
        except Exception:
            logger.exception("expiry sweep pass failed; retrying next interval")
