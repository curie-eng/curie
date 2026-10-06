"""Resume reconciler (#411): the backstop that re-enqueues owed wakes.

The resolve endpoint commits the resolve-once CAS claim and audit row, then
enqueues the resume turn. If that enqueue fails (a Valkey blip), the record is
left ``resolved_at`` set but ``resumed_at`` NULL -- a stranded suspended
session. ``ResumeReconciler`` sweeps those rows on an interval and re-enqueues
the resume turn, setting ``resumed_at`` only AFTER a successful enqueue
(enqueue-first-then-mark), so a failed enqueue is retried on the next pass
rather than lost.

An inline enqueue that raised in this process is retried on the very next pass
(#4016), not after the grace horizon below. ``ResumeQueue`` records every resume
turn whose XADD raised (the resolve endpoint, the resolve-path expiry branch,
the expiry sweeper, and administrative recovery all enqueue through it), and
each pass drains that record first. Before #4016 a resolve that hit a
just-restarted Valkey waited out the full grace, three hours under the chart.

An ``expired`` record is an owed wake on the same terms (#418): since #412 both
expiry paths enqueue a wake of their own, so a NULL ``resumed_at`` there means
the same failed enqueue -- and, unlike a resolved record, the flipped row is no
longer ``pending`` and so is never re-selected by the sweeper, which made an
expiry wake the one permanently unrecoverable case. Which turn a candidate owes
follows from its status, so the reconciler defers to ``resume_turn_for`` rather
than carrying the mapping itself.

Each pass is two steps (#532): ``reopen_dead_lettered_resumes`` first re-opens
any approval whose DELIVERED resume turn (``resumed_at`` set) died at the
worker's delivery cap and was dead-lettered -- a case the NULL-gated finder
above cannot re-select -- then ``reconcile_once`` re-enqueues every owed wake,
so a row re-opened this pass is re-enqueued in the same pass.

Three qualifications shape the design:

- **Grace window (load-bearing).** Apart from the recorded undelivered
  resumes, ``reconcile_once`` only considers records resolved at least
  ``grace_seconds`` ago. Helm derives this value from the worker's delivery
  budget plus its delivery-shutdown reserve, giving the resume delivered inline
  time to finish before the backstop retries it. The grace still governs every
  row whose enqueue was not observed failing in this process: a wake that may
  have landed (a failed mark after a successful enqueue), and any failure the
  record lost to a process restart or refused past its cap. A recorded failure
  bypasses it because the producer saw the XADD raise; if an ambiguous XADD
  (a timeout after the server applied it) did land, the expedited copy is a
  redundant wake that the worker's deterministic event-id claim and done marker
  absorb. Callers outside Helm should use a conservative grace that covers the
  worker's configured turn lifecycle. The worker serializes concurrent copies of a
  resume event with an active claim, so a duplicate does not enter the live
  turn. The two clocks it compares (``resolved_at`` is the DB ``func.now()``,
  ``resolved_before`` is this pod's clock) only add a small skew margin on top
  of a large grace, not a correctness dependency.
- **Absorption is TTL-bounded.** A duplicate enqueue is safe because the resume
  turn's ``event_id`` is deterministic per approval. The worker's active event
  claim prevents concurrent copies from entering the turn. The losing stream
  entry is acknowledged as redundant, while the original delivery remains
  pending under its delivery lease. After that delivery finishes, the done
  marker absorbs later enqueues. The marker only lasts for
  ``idempotency_ttl_s`` (default 24h). In steady state the
  reconciler retries on the interval (seconds), far inside that window. If a
  worker crashes, its claim may outlive its delivery lease. A new claimant
  detects that the recorded winner's lease is gone or its PEL row is absent,
  then atomically replaces the stale claim so the original delivery can be
  recovered. Neither mechanism guarantees exactly
  once external side effects if a worker crashes after performing one but
  before writing the terminal marker. The TTL bound also matters for later
  enqueues delayed beyond the marker lifetime. Pre-fix historical rows are
  excluded from the work-list by migration backfills
  (``resumed_at = resolved_at``): 0011 for resolved rows and 0012 for expired
  rows that #418's widened work-list first made candidates.
- **Concurrency.** The done marker CANNOT dedupe a concurrent double enqueue
  (it is written only post-terminal). Three guards cover distinct races:
  (1) *reconciler vs reconciler* (``api.replicas > 1``): a per-row
  ``SELECT ... FOR UPDATE SKIP LOCKED`` claim locks each candidate in its own
  short transaction, so two replicas do not grab the same row; (2) *inline
  resolver vs reconciler*: the grace lets the original delivery finish before
  the backstop retries, and only a wake whose XADD raised skips it; and
  (3) *duplicate worker deliveries*: the worker claims the deterministic
  resume event id under the active delivery lease.
  The claim is renewed with that lease. A duplicate entry that loses the claim
  is acknowledged as redundant, while the original delivery stays pending and
  recoverable. If the holder crashes, a new claimant detects that the recorded
  winner's lease is gone or its PEL row is absent, then atomically replaces the
  stale claim so that original delivery can be recovered. After terminal
  completion, the done marker absorbs
  later enqueues within its TTL. This serializes execution while a claim is live,
  but does not guarantee exactly once external side effects across a crash or
  after the marker TTL expires. No leader election.
"""

import asyncio
import enum
import logging
import time
import uuid
from datetime import UTC, datetime, timedelta

from aci_protocol import parse_queued_turn
from curie_telemetry import operation_span, record_metric
from opentelemetry.trace import SpanKind, StatusCode
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from curie_api.crud import approvals as crud_approvals

from .resumequeue import (
    ResumeQueue,
    approval_trace_context,
    parse_resume_event_id,
    resume_turn_for,
)

logger = logging.getLogger(__name__)


def _parse_dead_lettered_resume(
    fields: dict[str, str],
) -> tuple[uuid.UUID, datetime] | None:
    """Decode a graveyard row into ``(approval_id, dead_lettered_at)`` or None.

    Returns None for any row the scan cannot act on: no ``payload`` field, a
    payload that is not a valid ``QueuedTurn``, an ``event_id`` that is not a
    resume key, a missing ``dl_dead_lettered_at``, or one that does not parse.
    The timestamp is normalized to naive UTC to compare against the naive
    ``resumed_at`` column.
    """

    payload = fields.get("payload")
    if payload is None:
        return None
    try:
        # Tolerant decode (#625): this reads a queue-boundary payload the
        # worker produced, the same consumer-side case parse_queued_turn
        # already covers, so an unknown field a newer producer added must not
        # sink a dead-lettered row that would otherwise be recovered.
        turn = parse_queued_turn(payload)
    except ValidationError:
        return None
    approval_id = parse_resume_event_id(turn.event_id)
    if approval_id is None:
        return None
    raw = fields.get("dl_dead_lettered_at")
    if raw is None:
        return None
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    dt_naive = dt.astimezone(UTC).replace(tzinfo=None) if dt.tzinfo else dt
    return approval_id, dt_naive


class _Outcome(enum.Enum):
    """What one per-row re-enqueue attempt did."""

    ENQUEUED = "enqueued"
    NOT_CLAIMED = "not_claimed"
    FAILED = "failed"


class ResumeReconciler:
    """Periodically re-enqueue resume turns for resolved-but-unresumed approvals."""

    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        resume_queue: ResumeQueue,
        *,
        interval_seconds: int,
        grace_seconds: int,
        batch_limit: int,
        dead_letter_scan_limit: int = 1000,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._resume_queue = resume_queue
        self._interval_seconds = interval_seconds
        self._grace_seconds = grace_seconds
        self._batch_limit = batch_limit
        self._dead_letter_scan_limit = dead_letter_scan_limit
        self._last_success_monotonic: float | None = None

    async def reconcile_once(self) -> int:
        """Run one measured pass without changing the reconciler's result shape."""

        result = 0
        error: Exception | None = None
        attributes = {
            "service.name": "curie-api",
            "operation": "resume-reconciler",
            "role": "background",
        }
        with operation_span(
            "curie.background.resume-reconciler",
            kind=SpanKind.INTERNAL,
            attributes=attributes,
        ) as span:
            try:
                result = await self._reconcile_once()
            except Exception as exc:  # noqa: BLE001 - existing broad catch retained
                error = exc
                if hasattr(span, "set_status"):
                    span.set_status(StatusCode.ERROR)
                span.add_event("background.pass.failed", {"outcome": "failure"})
            else:
                span.add_event("background.pass.completed", {"outcome": "success"})

        outcome = "failure" if error is not None else "success"
        record_metric(
            "curie.background.loop",
            attributes={**attributes, "outcome": outcome},
        )
        now = time.monotonic()
        if error is None:
            self._last_success_monotonic = now
        if self._last_success_monotonic is not None:
            record_metric(
                "curie.background.last_success.age",
                max(0.0, now - self._last_success_monotonic),
                attributes=attributes,
            )
        if error is not None:
            raise error
        return result

    async def _reconcile_once(self) -> int:
        """Re-enqueue every owed wake this pass may retry; return the count.

        Two sources, in order. First, every approval whose resume enqueue was
        observed failing in this process (``ResumeQueue.undelivered_resumes``,
        #4016), regardless of the grace window: that wake is
        known not to have landed, so there is no in-flight delivery for the
        grace to protect. Second, the unchanged grace query: candidates read once
        (unlocked) past the grace horizon, skipping ids the first step already
        handled this pass.

        Each id goes through ``_reenqueue_one``'s own short transaction, claimed
        via ``claim_resume_row`` (``SELECT ... FOR UPDATE SKIP LOCKED``), so a row
        a concurrent replica already holds is skipped and two replicas never both
        enqueue one record. Per-record failure is isolated: a single-record Valkey
        blip rolls that row's transaction back (``resumed_at`` stays NULL for the
        next pass, preserving enqueue-first-then-mark durability) without
        aborting the batch, and the row lock is never held across the batch.
        """

        count = 0
        handled: set[uuid.UUID] = set()
        for approval_id in self._resume_queue.undelivered_resumes():
            handled.add(approval_id)
            outcome = await self._reenqueue_one(approval_id)
            if outcome is _Outcome.ENQUEUED:
                # enqueue discarded the id from the record on success.
                count += 1
            elif outcome is _Outcome.NOT_CLAIMED:
                # SKIP LOCKED returns None both when the row no longer owes a wake
                # (gone, already resumed, not resumable, or a publication) and
                # when a peer transaction merely holds its lock. Re-read without a
                # lock: forget the id only in the first case. A locked row that
                # still owes a wake stays recorded, so if the holder rolls back the
                # next pass retries it instead of waiting out the full grace.
                # A failed read keeps the id, isolating it like a failed enqueue.
                try:
                    async with self._sessionmaker() as session:
                        still_owed = await crud_approvals.approval_owes_resume(session, approval_id)
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "approval %s owed-wake check failed, will retry next pass",
                        approval_id,
                        exc_info=True,
                    )
                    still_owed = True
                if not still_owed:
                    self._resume_queue.forget_undelivered(approval_id)
            # _Outcome.FAILED: enqueue re-recorded the id; it stays owed.

        resolved_before = datetime.now(UTC).replace(tzinfo=None) - timedelta(
            seconds=self._grace_seconds
        )
        async with self._sessionmaker() as session:
            candidate_ids = await crud_approvals.list_resolved_unresumed(
                session, resolved_before=resolved_before, limit=self._batch_limit
            )

        for approval_id in candidate_ids:
            if approval_id in handled:
                continue
            if await self._reenqueue_one(approval_id) is _Outcome.ENQUEUED:
                count += 1
        return count

    async def _reenqueue_one(self, approval_id: uuid.UUID) -> _Outcome:
        """Claim one owed-wake row, enqueue its resume turn, then mark it.

        Enqueue-first-then-mark inside ``session.begin()``: a failed enqueue or
        mark rolls back, leaving ``resumed_at`` NULL for a later pass.
        """

        async with self._sessionmaker() as session:
            try:
                async with session.begin():
                    approval = await crud_approvals.claim_resume_row(session, approval_id)
                    if approval is None:
                        # Another replica holds it, or it is already resumed;
                        # exit the txn block cleanly, releasing any lock.
                        return _Outcome.NOT_CLAIMED
                    turn = resume_turn_for(approval)
                    await self._resume_queue.enqueue(
                        turn, parent=approval_trace_context(approval)
                    )
                    approval.resumed_at = datetime.now(UTC).replace(tzinfo=None)
                # session.begin() committed here, releasing the row lock.
            except Exception:  # noqa: BLE001
                logger.warning(
                    "approval %s resume re-enqueue failed, will retry next pass",
                    approval_id,
                    exc_info=True,
                )
                return _Outcome.FAILED
        logger.info("approval %s resume turn re-enqueued", approval_id)
        return _Outcome.ENQUEUED

    async def reopen_dead_lettered_resumes(self) -> int:
        """Re-open approvals whose DELIVERED resume turn was dead-lettered (#532).

        The gap: a resume turn that reached the runs stream (so ``resumed_at`` was
        marked by the inline path) can still die at the worker's delivery cap
        (#505). The worker moves it to the ``<runs>:dead`` graveyard and acks it
        off, so the sandbox never woke -- yet ``resumed_at`` is SET, and the
        NULL-gated finder (``list_resolved_unresumed``) never re-selects it. Such
        a row is stranded forever without this pass.

        The signal is a graveyard row whose payload ``event_id`` decodes back to
        an approval id via ``parse_resume_event_id``. For each such row this pass
        clears ``resumed_at`` (re-opens the approval) and DEFERS the actual
        re-enqueue to the standard ``reconcile_once`` pass -- ``run_forever``
        runs this immediately before it each cycle, so a row re-opened here is
        re-enqueued in the same cycle.

        Eviction / best-effort contract: the graveyard is bounded by an
        approximate MAXLEN, so a row is transient -- act only while it exists, and
        never assume permanence. A row beyond the scan cap is picked up on a later
        pass as the graveyard trims.

        Idempotency: ``crud.approvals.reopen_dead_lettered_resume`` only fires when the
        currently-marked ``resumed_at`` predates the row's dead-letter time, so a
        row that persists across passes cannot re-open a row already re-enqueued
        (its new ``resumed_at`` is newer). And a row already re-opened
        (``resumed_at`` NULL) is owned by the standard NULL-gated finder, never
        this path. A Valkey read failure logs and returns the count so far -- a
        graveyard read blip never kills the pass (mirrors ``reconcile_once``'s
        per-record isolation).
        """

        count = 0
        try:
            entries = await self._resume_queue.read_dead_letter(
                count=self._dead_letter_scan_limit
            )
        except Exception:
            logger.warning(
                "resume dead-letter scan read failed, skipping this pass",
                exc_info=True,
            )
            return count

        for _entry_id, fields in entries:
            parsed = _parse_dead_lettered_resume(fields)
            if parsed is None:
                continue
            approval_id, dead_lettered_at = parsed
            async with self._sessionmaker() as session:
                reopened = await crud_approvals.reopen_dead_lettered_resume(
                    session, approval_id, dead_lettered_after=dead_lettered_at
                )
            if reopened:
                count += 1
                logger.info(
                    "approval %s resume turn was dead-lettered; re-opened as an "
                    "owed wake",
                    approval_id,
                )
        return count

    async def run_forever(self) -> None:
        """Reconcile on the interval forever; never die from a reconcile error.

        Each pass is two steps: first ``reopen_dead_lettered_resumes`` re-opens
        any approval whose delivered resume turn was dead-lettered (#532), then
        ``reconcile_once`` re-enqueues every owed wake -- so a row re-opened this
        pass is re-enqueued in the same pass.

        The loop sleeps before each pass (including the first): the inline path
        handles the common case, so a just-started pod need not sweep instantly,
        and delaying the first pass keeps the backstop from firing inside a
        sub-interval process lifetime.
        """

        while True:
            await asyncio.sleep(self._interval_seconds)
            try:
                await self.reopen_dead_lettered_resumes()
                await self.reconcile_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("resume reconciler pass failed")
