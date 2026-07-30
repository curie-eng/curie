"""Resume reconciler (#411): the backstop that re-enqueues owed wakes.

The resolve endpoint commits the resolve-once CAS claim and audit row, then
enqueues the resume turn. If that enqueue fails (a Valkey blip), the record is
left ``resolved_at`` set but ``resumed_at`` NULL -- a stranded suspended
session. ``ResumeReconciler`` sweeps those rows on an interval and re-enqueues
the resume turn, setting ``resumed_at`` only AFTER a successful enqueue
(enqueue-first-then-mark), so a failed enqueue is retried on the next pass
rather than lost.

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

- **Grace window (load-bearing).** ``reconcile_once`` only considers records
  resolved at least ``grace_seconds`` ago. This is NOT approximate: the grace
  must exceed the worker's maximum single-turn processing time
  (``runner_total_timeout_s``, 600s) so the reconciler never re-enqueues while an
  inline-delivered resume turn is still live. The worker writes its done-marker
  only post-terminal, so a duplicate landing mid-turn is steered into that live
  turn and re-runs the approved action -- a too-small grace re-introduces exactly
  that. The two clocks it compares (``resolved_at`` is the DB ``func.now()``,
  ``resolved_before`` is this pod's clock) only add a small skew margin on top of
  a large grace, not a correctness dependency.
- **Absorption is TTL-bounded.** A duplicate enqueue is safe because the resume
  turn's ``event_id`` is deterministic per approval and the worker's done-marker
  dedupes it -- but only within ``idempotency_ttl_s`` (default 24h). In steady
  state the reconciler retries on the interval (seconds), far inside that
  window. The bound only bites for pre-fix historical rows, which the migration
  backfills (``resumed_at = resolved_at``) exclude from the work-list: 0011 for
  the resolved rows, 0012 for the expired rows that #418's widened work-list
  first made candidates.
- **Concurrency.** The done-marker CANNOT dedupe a concurrent double-enqueue
  (it is written only post-terminal), so two overlapping copies steer into one
  live turn. Two races, two guards: (1) *reconciler vs reconciler* (``api.replicas
  > 1``) -- a per-row ``SELECT ... FOR UPDATE SKIP LOCKED`` claim locks each
  candidate in its own short transaction, so two replicas never grab the same
  row; (2) *inline resolver vs reconciler* -- the grace above outlasts the max
  worker turn, so an inline-delivered turn is terminal (done-marked) before the
  reconciler would re-enqueue. Residual, NOT unconditional exactly-once: a worker
  retry loop can keep a turn live past the grace after an inline mark-failure; a
  fully airtight guarantee needs a worker-side in-flight lease (follow-up). The
  done-marker still dedupes strictly-sequential redeliveries within the 24h TTL.
  No leader election.
"""

import asyncio
import logging
import uuid
from datetime import UTC, datetime, timedelta

from aci_protocol import parse_queued_turn
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from . import crud
from .resumequeue import ResumeQueue, parse_resume_event_id, resume_turn_for

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

    async def reconcile_once(self) -> int:
        """Re-enqueue every owed wake past the grace horizon; return the count.

        Candidates are read once (unlocked) past the grace horizon; then each is
        claimed atomically in its OWN short transaction via ``claim_resume_row``
        (``SELECT ... FOR UPDATE SKIP LOCKED``). A row a concurrent replica
        already holds is skipped this pass and retried next -- two replicas never
        both enqueue one record. Per-record failure is isolated: the enqueue and
        mark live inside ``session.begin()``, so a single-record Valkey blip
        rolls that row's transaction back (``resumed_at`` stays NULL for the next
        pass, preserving enqueue-first-then-mark durability) without aborting the
        batch. The row lock is held only for that one record's brief enqueue+mark,
        never across the whole batch.
        """

        resolved_before = datetime.now(UTC).replace(tzinfo=None) - timedelta(
            seconds=self._grace_seconds
        )
        async with self._sessionmaker() as session:
            candidate_ids = await crud.list_resolved_unresumed(
                session, resolved_before=resolved_before, limit=self._batch_limit
            )

        count = 0
        for approval_id in candidate_ids:
            async with self._sessionmaker() as session:
                try:
                    async with session.begin():
                        approval = await crud.claim_resume_row(session, approval_id)
                        if approval is None:
                            # Another replica holds it, or it is already resumed;
                            # exit the txn block cleanly, releasing any lock.
                            continue
                        turn = resume_turn_for(approval)
                        await self._resume_queue.enqueue(turn)
                        approval.resumed_at = datetime.now(UTC).replace(tzinfo=None)
                    # session.begin() committed here, releasing the row lock.
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "approval %s resume re-enqueue failed, will retry next pass",
                        approval_id,
                        exc_info=True,
                    )
                    continue
                count += 1
                logger.info("approval %s resume turn re-enqueued", approval_id)
        return count

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

        Idempotency: ``crud.reopen_dead_lettered_resume`` only fires when the
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
            entries = await self._resume_queue.read_dead_letter(count=self._dead_letter_scan_limit)
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
                reopened = await crud.reopen_dead_lettered_resume(
                    session, approval_id, dead_lettered_after=dead_lettered_at
                )
            if reopened:
                count += 1
                logger.info(
                    "approval %s resume turn was dead-lettered; re-opened as an owed wake",
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
