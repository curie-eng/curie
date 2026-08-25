"""The Valkey Streams consumer: read, dispatch to the kernel, ack, and recover.

Uses a consumer group on the dispatcher's stream so multiple worker replicas
share the load and every entry is delivered to exactly one consumer. New entries
are read with ``XREADGROUP ... >``; entries that a consumer took but never acked
(a crash mid-run) are reclaimed with ``XAUTOCLAIM`` after an idle timeout and
reprocessed. Reprocessing is safe because the kernel is idempotent (the done
marker) and the side-effect marker blocks auto-retry of a half-run action.

Entries are dispatched concurrently across threads (bounded by a semaphore); the
kernel serializes within a thread. A successfully handled entry is acked; an
entry that raises is left pending for the next reclaim.

Delivery is **bounded** (#505). Reclaim-and-retry is not infinite: an entry that
has already been delivered ``max_delivery`` times and still failed is moved to a
dead-letter stream (``<stream>:dead`` by default) with its original fields plus
``dl_*`` failure metadata, then acked off the group. Without that cap a
permanently-failing entry (the motivating case: a reply POST to a CLI stub URL
that was persisted into a durable approval and is now a dead port) is reclaimed
and re-dispatched forever, silently stalling the whole consumer group. An
unparseable entry takes the same route (``reason="unparseable"``) so poison is
observable instead of being silently acked into the void.

The graveyard itself is capped (approximate ``MAXLEN``, ``dead_letter_maxlen``):
the unparseable path fires per INBOUND entry, so an unbounded graveyard would
grow at full ingest rate on the Valkey the kernel's locks and markers live on.
Dead-letter rows are therefore best-effort -- bounded loss traded against a
platform-wide OOM.

The delivery count is read from Valkey's pending-entries list on every pass and
is NEVER tracked in a process-local dict: the PEL counter is durable, so a
restarted or replacement worker still sees the accumulated count and still caps.
A process-local counter would reset on restart and let a crash-looping worker
retry a poison entry forever -- the exact stall this cap exists to end.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime

from curie_dispatcher.queue import from_stream_fields
from curie_telemetry import (
    TRACEPARENT_STREAM_FIELD,
    extract_trace_context,
    operation_span,
    record_metric,
)
from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode
from redis.asyncio import Redis

from .config import WorkerConfig
from .kernel import Kernel
from .stream_consumer import DeliverySpec, ReadLoopSpec, StreamConsumer

logger = logging.getLogger(__name__)

# Pause before retrying the blocking stream read after a transient transport
# error, so a briefly-unreachable Valkey does not spin the read loop hot.
_READ_ERROR_BACKOFF_S = 0.5

# XPENDING page size for the over-cap scan in ``_dead_letter_over_cap``.
# Deliberately NOT ``read_count``: that knob bounds how many entries the READ
# loop dispatches concurrently, whereas this scan dispatches nothing -- it is a
# pure metadata read that discards every under-cap row. Paging it at dispatch
# granularity costs one sequential round-trip per ``read_count`` entries, so a
# large backlog pages for no reason. A healthy tick is unaffected either way:
# the IDLE filter plus the empty-page early-out make it a single round-trip.
_CAP_SCAN_PAGE = 1000

# An operator-requested thread reset (#713): the API SADDs a thread_key here
# (`apps/api/src/curie_api/threadreset.py`) and the maintenance tick SPOPs
# it to force-release that thread's sandbox. Not a stream -- a one-shot
# administrative signal has no ordering/redelivery/dead-letter needs, so a
# plain Valkey SET is enough.
THREAD_RESET_SET = "curie:thread-reset-requests"

# Claimed-but-not-yet-released thread-reset requests (#812). The drain SPOPs a
# request off THREAD_RESET_SET (the atomic claim, so no second replica/tick
# double-releases it) and immediately SADDs it here; it is SREMoved only after
# `release_thread` actually completes. The API's `is_pending` reads the UNION of
# both sets, so the observable "reset still outstanding" signal the CLI polls on
# flips to done only when the sandbox is truly released -- not at claim time, and
# NOT at all if the release raises or times out (the key is left here, so the CLI
# reports the reset as unconfirmed rather than a false success). Mirrored verbatim
# in `apps/api/src/curie_api/threadreset.py`, the same cross-service-constant
# pattern as THREAD_RESET_SET itself.
THREAD_RESET_INFLIGHT_SET = "curie:thread-reset-inflight"

# Per-tick time budget for draining THREAD_RESET_SET (#743, follow-up to
# #739). #739 bounded the courtesy interrupt to 5s per *request*, but the
# drain itself is a serial `while True` over the whole set, inline in
# `_maintenance_loop` alongside `_reclaim_once`/`reap_orphans` -- so N wedged
# resets in one tick still cost N x the per-request bound of no stream
# reclaim and no orphan reaping, just scaled by operator batch size instead
# of by the runner's timeout. Once this budget is spent, the drain stops for
# this tick; `SPOP` only removes what it actually pops, so anything still in
# the set is picked up on the next tick rather than blocking the rest of the
# maintenance work behind an arbitrarily large batch.
_THREAD_RESET_DRAIN_BUDGET_S = 30.0


class Consumer(StreamConsumer):
    """Runs the read loop and the periodic reclaim/reap maintenance loop."""

    def __init__(
        self,
        *,
        redis: Redis,
        kernel: Kernel,
        config: WorkerConfig,
        max_concurrency: int = 16,
    ) -> None:
        super().__init__(redis)
        # The base class narrows self._redis to the StreamBroker port (stream
        # verbs only, by design -- a second broker implementation need not
        # support anything else). The thread-reset drain (#713) needs a plain
        # Valkey SET (SADD/SPOP), which isn't part of that port's contract, so
        # it gets its own concretely-typed handle onto the same connection
        # rather than widening StreamBroker for one unrelated feature.
        self._valkey: Redis = redis
        self._kernel = kernel
        self._config = config
        self._sem = asyncio.Semaphore(max_concurrency)
        self._inflight: set[asyncio.Task[None]] = set()
        # The reclaim/dead-letter knobs the shared base machinery reads. Built
        # after self._config is stored; handler is the bound self._dispatch. The
        # over-cap reason and the success-log format string are load-bearing:
        # dead_letter_log MUST stay byte-identical to
        # dead_letter_alert._DEAD_LETTER_MESSAGE and logger MUST be this module's
        # logger, or the CRITICAL dead-letter alert silently stops firing.
        self._delivery = DeliverySpec(
            stream=config.stream,
            group=config.consumer_group,
            consumer=config.consumer_name,
            dead_letter_target=config.dead_letter_stream_name(),
            over_cap_reason="max-delivery-exceeded",
            max_delivery=config.max_delivery,
            dead_letter_maxlen=config.dead_letter_maxlen,
            reclaim_min_idle_ms=config.reclaim_min_idle_ms,
            read_count=config.read_count,
            cap_scan_page=_CAP_SCAN_PAGE,
            telemetry_source="worker",
            handler=self._dispatch,
            logger=logger,
            dead_letter_log="dead-lettered entry %s after %d deliveries (reason=%s) -> %s",
            dead_letter_fail_log="dead-lettering entry %s failed; left pending, not dispatched",
        )

    async def ensure_group(self) -> None:
        """Create the consumer group (and the stream) if it does not exist.

        Created at ``$`` (the stream's current tail) so a first boot against a
        stream that already carries entries does NOT replay the whole backlog:
        a persistent Valkey that accumulated stale Slack mentions while no worker
        ran would otherwise storm every one of them into a live turn the moment
        the group is created. Only entries produced after the group exists are
        delivered; crash-recovery of in-flight entries is unaffected (it works
        off the pending list, not the group's start id). An existing group is
        left untouched.
        """
        await self._ensure_group(
            self._config.stream, self._config.consumer_group, start_id="$"
        )

    async def run(self) -> None:
        await self.ensure_group()
        # The startup sweep covers the case redelivery can NEVER reach: a turn
        # whose stream entry was already acked owes no redelivery, so a
        # completion left owed by the crash that stopped the last process would
        # otherwise sit in the outbox forever.
        #
        # It runs CONCURRENTLY with consumption, never ahead of it. The backlog
        # it drains is exactly the one an outage left behind, and each record is
        # an HTTP attempt against an adapter that may still be down -- awaiting
        # it here made a healthy worker's ability to read turns at all wait on
        # the recovery of the thing that broke. Owed completions are recovered on
        # their own timeline; live traffic does not queue behind them.
        await asyncio.gather(
            self._startup_completion_sweep(),
            self._read_loop(),
            self._maintenance_loop(),
        )
        if self._inflight:
            await asyncio.gather(*self._inflight, return_exceptions=True)

    async def _startup_completion_sweep(self) -> None:
        try:
            await self._kernel.sweep_pending_completions()
        except Exception:
            logger.exception("startup completion sweep failed")

    # -- read loop ------------------------------------------------------------

    async def _read_loop(self) -> None:
        await self._consume(
            ReadLoopSpec(
                stream=self._config.stream,
                group=self._config.consumer_group,
                consumer=self._config.consumer_name,
                count=self._config.read_count,
                block_ms=self._config.read_block_ms,
                backoff_s=_READ_ERROR_BACKOFF_S,
                timeout_msg="stream read timed out (idle); retrying: %s",
                connection_msg="stream read failed transiently; retrying: %s",
                logger=logger,
            ),
            self._dispatch,
        )

    async def _dispatch(self, entry_id: str, fields: dict[str, str]) -> None:
        if entry_id in self._inflight_ids:
            return  # already being handled by this consumer
        # Acquire a capacity slot BEFORE spawning the handler so a burst larger
        # than max_concurrency exerts backpressure on the read loop instead of
        # claiming the whole backlog into this consumer's local queue (which would
        # starve other replicas and make a crash wait out the reclaim window).
        await self._sem.acquire()
        self._inflight_ids.add(entry_id)
        task = asyncio.create_task(self._handle(entry_id, fields))
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)

    async def _handle(self, entry_id: str, fields: dict[str, str]) -> None:
        started = time.monotonic()
        parent = extract_trace_context(fields)
        parent_is_valid = trace.get_current_span(parent).get_span_context().is_valid
        malformed_carrier = TRACEPARENT_STREAM_FIELD in fields and not parent_is_valid
        metric_attributes = {
            "service.name": "curie-worker",
            "source": "worker",
        }
        process_outcome = "failure"
        try:
            with operation_span(
                "curie.queue.process",
                kind=SpanKind.CONSUMER,
                parent=parent,
                attributes=metric_attributes,
            ) as span:
                if malformed_carrier:
                    logger.warning("malformed stream trace context ignored; processing as a root")
                    span.add_event("trace.context.malformed", {"outcome": "failure"})
                elif TRACEPARENT_STREAM_FIELD not in fields:
                    span.add_event("trace.context.missing", {"outcome": "success"})
                try:
                    qevent = from_stream_fields(fields)
                except Exception as exc:
                    if hasattr(span, "set_status"):
                        span.set_status(StatusCode.ERROR)
                    span.add_event(
                        "queue.message.unparseable",
                        {"outcome": "failure", "error.class": type(exc).__name__},
                    )
                    record_metric(
                        "curie.queue.process",
                        attributes={**metric_attributes, "outcome": "failure"},
                    )
                    logger.exception("unparseable stream entry %s; dead-lettering", entry_id)
                    await self._dead_letter(
                        entry_id,
                        fields,
                        reason="unparseable",
                        delivery_count=await self._pending_delivery_count(entry_id),
                    )
                    return

                age = self._message_age_seconds(qevent.received_at)
                for name in (
                    "curie.queue.wait.duration",
                    "curie.queue.message.age",
                ):
                    record_metric(
                        name,
                        age,
                        attributes={**metric_attributes, "outcome": "success"},
                    )
                record_metric(
                    "curie.turn.accepted",
                    attributes={
                        "service.name": "curie-worker",
                        "source": "worker",
                        "outcome": "accepted",
                    },
                )
                try:
                    await self._kernel.process_event(qevent)
                except Exception as exc:
                    # Leave the entry pending: XAUTOCLAIM will reclaim and retry.
                    if hasattr(span, "set_status"):
                        span.set_status(StatusCode.ERROR)
                    span.add_event(
                        "queue.processing.failed",
                        {"outcome": "failure", "error.class": type(exc).__name__},
                    )
                    record_metric(
                        "curie.queue.process",
                        attributes={**metric_attributes, "outcome": "failure"},
                    )
                    record_metric(
                        "curie.queue.settle",
                        attributes={**metric_attributes, "outcome": "pending"},
                    )
                    logger.exception("processing failed for entry %s; left pending", entry_id)
                    return
                await self._ack(entry_id)
                process_outcome = "success"
                span.add_event("queue.message.acked", {"outcome": "ack"})
                record_metric(
                    "curie.queue.process",
                    attributes={**metric_attributes, "outcome": "success"},
                )
                record_metric(
                    "curie.queue.settle",
                    attributes={**metric_attributes, "outcome": "ack"},
                )
        finally:
            record_metric(
                "curie.queue.process.duration",
                max(0.0, time.monotonic() - started),
                attributes={**metric_attributes, "outcome": process_outcome},
            )
            self._inflight_ids.discard(entry_id)
            self._sem.release()

    @staticmethod
    def _message_age_seconds(received_at: str) -> float:
        try:
            received = datetime.fromisoformat(received_at)
        except ValueError:
            return 0.0
        if received.tzinfo is None:
            received = received.replace(tzinfo=UTC)
        return max(0.0, (datetime.now(UTC) - received.astimezone(UTC)).total_seconds())

    async def _observe_queue_state(self) -> None:
        """Publish bounded queue gauges; observation failure never gates delivery."""

        try:
            pending_raw = await self._valkey.xpending(
                self._config.stream, self._config.consumer_group
            )
            pending = float(pending_raw.get("pending", 0))
            depth = float(await self._valkey.xlen(self._config.stream))
            lag = 0.0
            for group in await self._valkey.xinfo_groups(self._config.stream):
                name = group.get("name")
                if isinstance(name, bytes):
                    name = name.decode()
                if name == self._config.consumer_group:
                    lag = float(group.get("lag") or 0)
                    break
        except Exception as exc:
            logger.warning("queue telemetry observation failed (%s)", type(exc).__name__)
            return

        attributes = {
            "service.name": "curie-worker",
            "source": "worker",
            "outcome": "pending",
        }
        record_metric("curie.queue.pending", pending, attributes=attributes)
        record_metric("curie.queue.lag", lag, attributes=attributes)
        record_metric("curie.queue.depth", depth, attributes=attributes)

    async def _pending_delivery_count(self, entry_id: str) -> int:
        """This entry's CURRENT delivery count, read from the PEL.

        The unparseable path reaches here from the read loop OR from a reclaim:
        an entry can be delivered, have its worker crash before it ever parses,
        and be reclaimed -- so its count is 2+ by the time it is dead-lettered.
        Hardcoding 1 would fabricate the graveyard's ``dl_delivery_count``
        precisely during crash recovery, when the evidence matters most. Read
        from the PEL, never a process-local counter, for the same durability
        reason the cap does. Falls back to 1 only if the row has vanished (it was
        delivered to us at least once), which is the honest floor rather than a
        guess.
        """
        rows = await self._redis.xpending_range(
            self._config.stream,
            self._config.consumer_group,
            min=entry_id,
            max=entry_id,
            count=1,
        )
        return int(rows[0]["times_delivered"]) if rows else 1

    # -- maintenance loop -----------------------------------------------------

    async def _maintenance_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self._reclaim_once()
                await self._kernel.reap_orphans()
                await self._kernel.sweep_pending_completions()
                await self._drain_thread_reset_requests()
            except Exception:
                logger.exception("maintenance tick failed")
            # Queue inventory is operational sampling, not message settlement.
            # Keep its three Valkey reads on the bounded maintenance cadence so
            # a slow telemetry read cannot retain a per-message concurrency slot
            # after the message is already acked (or deliberately left pending).
            # It still runs when an unrelated maintenance action fails; the
            # observer contains and logs its own failures.
            await self._observe_queue_state()
            await self._sleep_or_stop(self._config.reclaim_interval_s)

    async def _drain_thread_reset_requests(self) -> None:
        """Force-release any thread whose sandbox an operator requested reset
        for (#713). ``THREAD_RESET_SET`` mirrors
        ``apps/api/src/curie_api/threadreset.py``'s constant verbatim (same
        cross-service-constant pattern the kill switch already uses, since
        the API and worker are separate deployables that do not import each
        other's package).

        ``SPOP`` (not ``SMEMBERS``) so a request is CLAIMED and removed from
        the request set atomically -- a concurrent tick (this worker's own next
        iteration, or a second replica) can never double-process it or run
        ``release_thread`` for the same key twice.

        But the claim must not itself be the "reset done" signal (#812, was #806
        incomplete): the API's ``is_pending`` -- which the CLI's ``reset-thread``
        poll gates its "sandbox released" report on -- must stay True until the
        release ACTUALLY lands, not flip the instant the request is SPOPped
        (before, and independent of whether, ``release_thread`` succeeds; #777
        widened that release to several seconds). So a claimed request is moved
        into ``THREAD_RESET_INFLIGHT_SET`` (which ``is_pending`` also reads) for
        the duration of the release, and cleared from it only on SUCCESS.

        A release that raises or times out is logged and LEFT in the in-progress
        set: ``is_pending`` therefore stays True and the CLI reports the reset as
        unconfirmed rather than a false success (scenario B). It is deliberately
        NOT re-added to the request set -- re-claiming a permanently-failing
        release every tick would hot-loop the drain -- so, as before, a release
        that fails needs a fresh operator request to retry (acceptable for a
        manual action, unlike the queue's bounded-retry delivery guarantee). One
        failed release does not block the rest of the batch.

        The loop is also bounded by ``_THREAD_RESET_DRAIN_BUDGET_S`` (#743): a
        large operator-populated batch of wedged resets stops draining once
        the budget is spent, rather than serially paying every request's
        release bound inline in this tick. Members not yet popped simply stay
        in ``THREAD_RESET_SET`` and are drained on a later tick -- safe
        because ``SPOP`` never removes a member without this loop taking
        ownership of it in the same step."""
        start = time.monotonic()
        while True:
            raw = await self._valkey.spop(THREAD_RESET_SET)
            if raw is None:
                return
            # SPOP with no count (as called here) always returns a single
            # bare member, never the set-of-members shape its overload
            # allows with an explicit count -- narrow away that shape for
            # the type checker rather than the client's imprecise overload.
            assert isinstance(raw, (str, bytes)), f"unexpected SPOP shape: {raw!r}"
            thread_key = raw.decode("utf-8") if isinstance(raw, bytes) else raw
            # Mark the claim in-progress BEFORE releasing, so `is_pending` (the
            # union of the request and in-progress sets) stays True for the whole
            # release rather than flipping at claim time (#812).
            await self._valkey.sadd(THREAD_RESET_INFLIGHT_SET, thread_key)
            try:
                released = await self._kernel.release_thread(thread_key)
            except Exception:
                # Release failed/timed out: leave the key in the in-progress set
                # so `is_pending` stays True and the CLI does not report a false
                # "released" (#812). Not re-queued -- a fresh request re-drives it.
                logger.exception("thread reset failed for %s", thread_key)
            else:
                # Release landed: clear the in-progress marker so `is_pending`
                # flips to done -- only now, after the teardown actually
                # completed.
                await self._valkey.srem(THREAD_RESET_INFLIGHT_SET, thread_key)
                logger.info(
                    "thread reset: released sandbox for %s (route existed: %s)",
                    thread_key,
                    released,
                )
            if time.monotonic() - start >= _THREAD_RESET_DRAIN_BUDGET_S:
                logger.warning(
                    "thread reset drain: per-tick budget (%.0fs) spent; "
                    "deferring any remaining requests to the next maintenance tick",
                    _THREAD_RESET_DRAIN_BUDGET_S,
                )
                return
