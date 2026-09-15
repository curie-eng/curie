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
    channel_event_id_scope,
    extract_trace_context,
    operation_span,
    record_metric,
)
from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode
from redis.asyncio import Redis

from .completion_health import observe_completion_outbox
from .config import WorkerConfig
from .consumer_liveness import ConsumerLivenessStore
from .delivery_lease import DeliveryLeaseStore, LeaseLostError
from .kernel import Kernel, _thread_key_for
from .markers import Markers
from .stream_consumer import DeliverySpec, ReadLoopSpec, StreamConsumer
from .upgrade_drain import UpgradeDrainGate

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
# (`apps/api/src/curie_api/threadreset.py`) and the maintenance tick claims
# it to force-release that thread's sandbox. `curie local eval` /
# `curie cluster eval` SADDs each eval-owned conversation_id onto the same
# set (#1534) so the next turn's `_handle` (and the tick) release that
# case's sandbox instead of pinning quota until routeTtlSeconds. Frozen
# with the CLI copy in tests/vectors/thread-reset-set.json. Not a stream --
# a one-shot administrative signal has no ordering/redelivery/dead-letter
# needs, so a plain Valkey SET is enough.
THREAD_RESET_SET = "curie:thread-reset-requests"

# Claimed-but-not-yet-released thread-reset requests (#812). The drain MOVES a
# request off THREAD_RESET_SET into this set in one atomic server-side step
# (`_THREAD_RESET_CLAIM_LUA`, #855) -- the claim and the in-progress mark are the
# same operation, so no second replica/tick double-releases it AND that claim
# transition is never observable as a member missing from both sets. It is
# SREMoved only after `release_thread` actually completes. The API's
# `is_pending` reads the UNION of both sets, so the observable "reset still
# outstanding" signal the CLI polls on flips to done only when the sandbox is
# truly released -- not at claim time, and NOT at all if the release raises or
# times out (the key is left here, so the CLI reports the reset as unconfirmed
# rather than a false success). Mirrored verbatim in
# `apps/api/src/curie_api/threadreset.py`, the same cross-service-constant
# pattern as THREAD_RESET_SET itself.
#
# The marker itself carries no claim identity, though: if a second operator
# request for the same thread lands while an earlier release is still
# in-flight, both claims collapse onto this one SADDed key, and the earlier
# release's unconditional SREM on completion empties it (and the union) while
# the later release is still running. That duplicate-request gap predates
# this change and is adjacent to #734 -- not something the atomic claim step
# closes.
THREAD_RESET_INFLIGHT_SET = "curie:thread-reset-inflight"

# Claim a thread-reset request and mark it in-progress as ONE atomic unit (#855).
# Valkey runs a script to completion with nothing interleaved, so the claim
# transition -- moving the key from the request set to the in-progress set --
# is never observable as absent from both: there is no window where the API's
# `is_pending` (the UNION of both) reads False for a live request. A
# client-side SPOP-then-SADD pair cannot offer that: the RTT between the two
# commands is a real gap in which the key belongs to neither set, and a poll
# landing in it reads "released" before `release_thread` has even been
# invoked (#812's exact user-visible failure). (This says nothing about a
# repeat request for a thread whose earlier release is still in flight --
# see the identity-less-marker note above THREAD_RESET_INFLIGHT_SET.)
# Returns the claimed member, or Lua `false` (Python ``None``) when the request
# set is empty -- the same bare-member reply shape SPOP itself has.
_THREAD_RESET_CLAIM_LUA = """
local claimed = redis.call('SPOP', KEYS[1])
if claimed then
  redis.call('SADD', KEYS[2], claimed)
end
return claimed
"""

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
        leases: DeliveryLeaseStore | None = None,
        drain: UpgradeDrainGate | None = None,
    ) -> None:
        # The delivery-ownership fence (ADR-0131) lives entirely in the shared
        # base: acquisition, the background heartbeat, release, and the reclaim
        # liveness guards. This lane supplies only the store and the lane-specific
        # way to stop a runner when the fence moves, so runs and evals share one
        # implementation by construction. ``leases=None`` keeps every pre-ADR-0131
        # construction (and the tests that use it) behaving exactly as before.
        super().__init__(
            redis,
            leases=leases,
            on_lease_lost=self._interrupt_on_lease_lost,
            drain=drain,
            liveness_store=ConsumerLivenessStore(redis),
        )
        # The base class narrows self._redis to the StreamBroker port (stream
        # verbs only, by design -- a second broker implementation need not
        # support anything else). The thread-reset drain (#713) needs a plain
        # Valkey SET (SADD/SPOP), which isn't part of that port's contract, so
        # it gets its own concretely-typed handle onto the same connection
        # rather than widening StreamBroker for one unrelated feature.
        self._valkey: Redis = redis
        self._kernel = kernel
        self._config = config
        self._max_concurrency = max_concurrency
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
            dead_consumer_idle_ms=config.dead_consumer_idle_ms,
            heartbeat_ttl_ms=config.consumer_heartbeat_ttl_ms,
            capability_ttl_ms=config.consumer_capability_ttl_ms,
            read_count=config.read_count,
            cap_scan_page=_CAP_SCAN_PAGE,
            telemetry_source="worker",
            handler=self._dispatch,
            logger=logger,
            dead_letter_log="dead-lettered entry %s after %d deliveries (reason=%s) -> %s",
            dead_letter_fail_log="dead-lettering entry %s failed; left pending, not dispatched",
            lease_expired_idle_ms=config.lease_expired_idle_ms_value(),
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
        await self._run_consumer_generation(
            {
                "startup-completion-sweep": self._startup_completion_sweep,
                "read": self._read_loop,
                "maintenance": self._maintenance_loop,
                "prompt-reclaim": self._prompt_reclaim_loop,
            },
            may_complete=frozenset({"startup-completion-sweep"}),
        )

    def _generation_inflight_tasks(self) -> set[asyncio.Task[None]]:
        return set(self._inflight)

    def _reset_generation_resources(self) -> None:
        self._inflight.clear()
        self._sem = asyncio.Semaphore(self._max_concurrency)

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

    def _transfer_capacity(self) -> int | None:
        # Mirrors the semaphore rather than reading it: ``_dispatch`` adds to
        # ``_inflight_ids`` immediately after ``self._sem.acquire()`` and
        # ``_handle``'s finally discards it beside ``self._sem.release()``, so the
        # two move together and this is the free-slot count without touching
        # asyncio.Semaphore internals.
        return max(0, self._max_concurrency - len(self._inflight_ids))

    async def _dispatch(self, entry_id: str, fields: dict[str, str]) -> None:
        if entry_id in self._inflight_ids:
            return  # already being handled by this consumer
        # Acquire a capacity slot BEFORE spawning the handler so a burst larger
        # than max_concurrency exerts backpressure on the read loop instead of
        # claiming the whole backlog into this consumer's local queue (which would
        # starve other replicas and make a crash wait out the reclaim window).
        await self._sem.acquire()
        if self._should_stop():
            self._sem.release()
            return
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

                # ADR-0131: hold delivery AUTHORITY for the whole body. Owning
                # the PEL row is necessary but not sufficient -- a peer replica
                # that took the row (a reclaim, an XAUTOCLAIM) is refused here
                # rather than allowed to run the same turn a second time. The
                # lifecycle (acquire, background heartbeat, release) lives in the
                # shared base so this lane and the eval lane cannot diverge.
                async with self._delivery_lease(entry_id, fields) as lease:
                    if lease is None:
                        # Another owner holds it. Return WITHOUT acking: the
                        # entry stays pending for whoever legitimately owns it.
                        # A refusal is a normal early return, never an exception
                        # -- an exception here would be caught by ``_consume``'s
                        # isolation guard and logged as a failure, which is a
                        # stack trace per entry for the routine case of losing a
                        # race.
                        return
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

                    # The ``curie.queue.process`` span opened before the parse,
                    # so the id is attached post-hoc. This mutates only THIS
                    # turn's own span, so it is safe ahead of the drain below.
                    if qevent.event_id:
                        span.set_attribute("event_id", qevent.event_id)

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
                    # Eval (and operator reset-thread) SADDs onto THREAD_RESET_SET
                    # when a sandbox should be released. Drain those before this
                    # turn claims so a following eval case or cluster message does
                    # not wait for the maintenance tick with quota already full.
                    # A failed drain must not fail the turn; the next handle or
                    # maintenance tick retries the remaining requests (#1534).
                    try:
                        if await self._valkey.scard(THREAD_RESET_SET):
                            await self._drain_thread_reset_requests()
                    except Exception:
                        logger.exception(
                            "thread-reset drain before turn %s failed; continuing",
                            entry_id,
                        )
                    # Opened AFTER the thread-reset drain on purpose: the
                    # drain tears down OTHER threads (release_thread opens
                    # curie.runner.rpc, curie.thread.lock and
                    # curie.sandbox.release, the last via asyncio.to_thread,
                    # which COPIES this context). Opening the scope earlier
                    # would stamp another thread's teardown with this turn's
                    # event id -- silent wrong correlation. Do not move it up.
                    # Likewise, do not add teardown or maintenance work for a
                    # DIFFERENT thread inside this scope: it would be stamped
                    # with this turn's event id and land in this request's trace.
                    with channel_event_id_scope(qevent.event_id):
                        try:
                            if self._leases is None:
                                # No fence configured: call the kernel EXACTLY as it
                                # was called before ADR-0131. The base yields a
                                # permissive sentinel so this handler body stays
                                # uniform, but forwarding that sentinel would claim
                                # an authority nobody holds -- and ``process_event``
                                # keeps its lease optional for precisely this caller.
                                await self._kernel.process_event(qevent)
                            else:
                                await self._kernel.process_event(qevent, lease=lease)
                        except Exception as exc:
                            # Leave the entry pending: the lease-expiry reclaim pass
                            # picks it up one lease TTL after this handler released
                            # its lease, with XAUTOCLAIM still the 15 minute backstop
                            # behind that (#2433).
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
                            logger.exception(
                                "processing failed for entry %s; left pending", entry_id
                            )
                            # Best-effort, and belt and braces: the kernel method
                            # already swallows its own failures, but an exception
                            # escaping THIS branch is the #673 shape exactly, and a
                            # notice may never change the settlement outcome of a
                            # delivery. The return below stays unconditional.
                            try:
                                # This branch can run AFTER lease loss and sits ahead
                                # of the pre-ACK guard below, so terminality alone is
                                # not permission to edit. A replacement that holds the
                                # fence now owns this thread and will speak for it;
                                # talking over it is worse than saying nothing. On the
                                # leaseless sentinel this never raises, so a base-only
                                # consumer still notifies.
                                lease.raise_if_lost()
                                await self._kernel.notify_turn_not_started(qevent, lease=lease)
                            except LeaseLostError:
                                logger.warning(
                                    "skipping the not-started notice for entry %s: this "
                                    "owner lost the delivery lease, and the current owner "
                                    "speaks for the thread",
                                    entry_id,
                                )
                            except Exception:
                                logger.warning(
                                    "not-started notice failed for entry %s",
                                    entry_id,
                                    exc_info=True,
                                )
                            return
                        try:
                            # A stale owner may not ACK. Checked immediately before
                            # the ack, because the fence can move at any point during
                            # a long turn and the ack is the irreversible one: it
                            # takes the entry off the group, out from under the
                            # replacement that now owns it.
                            lease.raise_if_lost()
                        except LeaseLostError:
                            logger.warning(
                                "refusing to ack entry %s: this owner lost the delivery "
                                "lease mid-turn; leaving it pending for the current owner",
                                entry_id,
                            )
                            record_metric(
                                "curie.queue.process",
                                attributes={**metric_attributes, "outcome": "failure"},
                            )
                            record_metric(
                                "curie.queue.settle",
                                attributes={**metric_attributes, "outcome": "pending"},
                            )
                            return
                        await self._ack(entry_id)
                        # Terminal acknowledgement: remove the delivery state as well
                        # as the lease (the base's release drops only the lease). The
                        # state's one-day retention is the backstop for a crash
                        # between the ack and here, not the normal way it goes away --
                        # without this a dead-lettered-and-redelivered event id
                        # accumulates state keys until that TTL. Best-effort: the ack
                        # above already happened.
                        await self._settle_delivery_best_effort(entry_id)
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
                        return
        finally:
            record_metric(
                "curie.queue.process.duration",
                max(0.0, time.monotonic() - started),
                attributes={**metric_attributes, "outcome": process_outcome},
            )
            self._inflight_ids.discard(entry_id)
            self._sem.release()

    async def _interrupt_on_lease_lost(self, entry_id: str, fields: dict[str, str]) -> None:
        """Stop the live runner for a delivery whose fence has moved (ADR-0131).

        Wired as the base's ``on_lease_lost``. It uses ``interrupt_thread`` --
        the EXISTING bounded control path -- and nothing else. Cancelling the
        handler task instead would look tidier and be wrong: a bare cancel skips
        the runner-side stop and leaves a turn producing effects on a sandbox
        this process no longer owns, which is exactly what the replacement's
        reclaim preflight then has to clean up.

        Every failure is logged and swallowed. Failing to interrupt must not mask
        the lease loss itself, which ``lease.lost`` has already recorded and which
        the pre-ack fence enforces on its own; and the replacement's preflight is
        the second, independent guard against a turn that keeps running here.
        """
        try:
            qevent = from_stream_fields(fields)
            await self._kernel.interrupt_thread(
                _thread_key_for(qevent), "delivery lease lost"
            )
        except Exception:
            logger.exception(
                "could not interrupt the runner for entry %s after its delivery "
                "lease was lost; the fence still refuses every terminal write",
                entry_id,
            )

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
        while not self._should_stop():
            try:
                # A reclaim is a NEW claim: it moves a peer's pending entry into
                # this consumer and dispatches it. During an upgrade drain
                # (#2010) that is exactly the theft the gate exists to stop --
                # a replacement pod coming up mid-roll would otherwise take the
                # delivery a still-draining replica is settling, see the
                # side-effect marker, and escalate work that was about to
                # complete. The rest of the tick is unaffected: reaping orphans
                # and sweeping owed completions create no claims and stay
                # useful while a drain is in progress.
                if not await self._claims_paused():
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
            await self._observe_completion_outbox()
            await self._sleep_or_stop(self._config.reclaim_interval_s)

    async def _observe_completion_outbox(self) -> None:
        """Publish completion-outbox gauges; observation never settles records."""

        markers = Markers(self._valkey, self._config)
        await observe_completion_outbox(markers, self._valkey, self._config)

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

        The claim and that in-progress mark are ONE server-side step
        (``_THREAD_RESET_CLAIM_LUA``, #855), not an ``SPOP`` followed by a
        separate ``SADD`` -- see that constant's comment for why the two-round-trip
        version would reopen #812's failure at RTT scale.

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
        because the atomic claim never removes a member from the request set
        without this loop taking ownership of it, and marking it in-progress,
        in the same step."""
        start = time.monotonic()
        while True:
            # Claim the request AND mark it in-progress in one atomic step, so
            # `is_pending` (the union of the request and in-progress sets) stays
            # True across the whole claim rather than briefly reading False in
            # the gap between two round trips (#812, #855).
            raw = await self._valkey.eval(
                _THREAD_RESET_CLAIM_LUA,
                2,
                THREAD_RESET_SET,
                THREAD_RESET_INFLIGHT_SET,
            )
            if raw is None:
                return
            # The script returns the SPOP reply verbatim, and SPOP with no count
            # always returns a single bare member, never the set-of-members shape
            # its overload allows with an explicit count -- narrow away that
            # shape for the type checker rather than the client's imprecise
            # overload.
            assert isinstance(raw, (str, bytes)), f"unexpected claim shape: {raw!r}"
            thread_key = raw.decode("utf-8") if isinstance(raw, bytes) else raw
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
