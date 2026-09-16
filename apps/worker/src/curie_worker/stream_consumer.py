"""Shared Valkey consumer-group read-loop mechanics.

The runs consumer (``consumer.py``) and the eval consumer (``eval/stream.py``)
both drive the same Valkey ``XREADGROUP`` loop: ensure the group exists, do a
blocking group read, skip an empty/timeout response, survive a transient
transport fault (a blocking-read ``TimeoutError`` is the routine idle case ->
DEBUG; a ``ConnectionError`` is a real fault -> WARNING), back off and retry, and
ack a handled entry. That shared plumbing lives here once; each consumer keeps
its own stream/group/consumer names, backoff constant, log-message prefixes, and
per-message business logic and passes them in.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from curie_telemetry import operation_span, record_metric
from opentelemetry.trace import SpanKind, StatusCode
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
)
from redis.exceptions import (
    ResponseError,
)
from redis.exceptions import (
    TimeoutError as RedisTimeoutError,
)

from .broker import StreamBroker
from .consumer_liveness import ConsumerLivenessStore
from .delivery_lease import (
    DeliveryLease,
    DeliveryLeaseStore,
    LeaseRefused,
    unfenced_lease,
)
from .upgrade_drain import UpgradeDrainGate

# How often a paused read loop re-checks the upgrade quiesce flag (#2010). Short
# relative to any rollout, so a released fleet resumes claiming promptly, and far
# longer than one EXISTS, so the poll costs nothing measurable while idle.
_QUIESCE_POLL_S = 1.0

# One stream entry as redis returns it with decode_responses=True.
StreamEntry = tuple[str, dict[str, str]]

# An async per-message handler: (entry_id, fields) -> None.
EntryHandler = Callable[[str, dict[str, str]], Awaitable[None]]

# Called once when a delivery's lease is lost mid-flight: (entry_id, fields).
LeaseLostHandler = Callable[[str, dict[str, str]], Awaitable[None]]


@dataclass(frozen=True)
class ReadLoopSpec:
    """The per-consumer knobs the shared read loop needs. Everything here is
    deliberately passed in (not hard-coded) so each consumer's stream, group,
    backoff, and log output stay exactly what they were before the extraction."""

    stream: str
    group: str
    consumer: str
    count: int
    block_ms: int
    backoff_s: float
    # Log messages kept per-consumer so log output is byte-identical; each takes
    # a single ``%s`` for the exception. The logger is the owning module's logger
    # so records carry that module's name (tests assert on it).
    timeout_msg: str
    connection_msg: str
    logger: logging.Logger


@dataclass(frozen=True)
class DeliverySpec:
    """The per-consumer knobs the shared reclaim/dead-letter machinery needs;
    passed in so each lane stays byte-identical after the extraction."""

    stream: str
    group: str
    consumer: str
    dead_letter_target: str
    over_cap_reason: str
    max_delivery: int
    dead_letter_maxlen: int
    reclaim_min_idle_ms: int
    dead_consumer_idle_ms: int
    heartbeat_ttl_ms: int
    capability_ttl_ms: int
    read_count: int
    cap_scan_page: int
    telemetry_source: str
    handler: EntryHandler
    logger: logging.Logger
    dead_letter_log: str
    dead_letter_fail_log: str
    # The idle threshold for the lease-expiry reclaim pass (ADR-0131, #2433).
    # ``None`` means the lane opted out and the pass is inert, which is also what
    # a base-only consumer with no lease store gets for free. Last in the
    # dataclass because it is the first defaulted field; both lanes construct
    # ``DeliverySpec`` with keyword arguments only, so appending is safe.
    lease_expired_idle_ms: int | None = None


class ConsumerLivenessExpired(RuntimeError):
    """The local consumer could no longer renew its alive lease safely."""


class StreamConsumer:
    """Base for a Valkey consumer-group reader.

    Owns the transport-level plumbing (group create, blocking read loop with the
    Timeout->DEBUG / Connection->WARNING split and backoff, ack, stop-aware
    sleep). Subclasses supply their stream config via a :class:`ReadLoopSpec`,
    their per-message handler, and their own maintenance/reclaim loops and
    business logic.
    """

    def __init__(
        self,
        redis: StreamBroker,
        delivery: DeliverySpec | None = None,
        *,
        leases: DeliveryLeaseStore | None = None,
        on_lease_lost: LeaseLostHandler | None = None,
        drain: UpgradeDrainGate | None = None,
        liveness_store: ConsumerLivenessStore | None = None,
    ) -> None:
        # The stream broker behind the port (#284). ``redis.asyncio.Redis`` is the
        # one backing today and structurally satisfies ``StreamBroker``; a second
        # broker is a drop-in. Named ``_redis`` still so the sacred consumer.py
        # subclass (which reads ``self._redis`` for XAUTOCLAIM) is untouched.
        self._redis: StreamBroker = redis
        # Process stop is permanent (SIGTERM/operator shutdown). Generation stop
        # is replaced for every supervised ``run()`` so a terminal lease failure
        # can tear one generation down and restart cleanly without making the
        # process look gracefully stopped.
        self._stop = asyncio.Event()
        self._generation_stop = asyncio.Event()
        # Entry ids currently being handled by THIS consumer. XAUTOCLAIM would
        # otherwise reclaim our own long-running (still-pending) entries and
        # re-dispatch a duplicate handler that steers the same prompt into its
        # own live turn; skipping these ids prevents that self-reclaim.
        self._inflight_ids: set[str] = set()
        # The reclaim/dead-letter knobs, or None for a base-only reader that
        # exercises just ``_consume`` (no reclaim machinery).
        self._delivery = delivery
        # Delivery ownership leases (ADR-0131). Like ``consumer.py``'s
        # ``self._valkey``, this collaborator is built from the CONCRETE
        # ``redis.asyncio.Redis`` rather than the ``StreamBroker`` port above:
        # the fence needs Lua scripting, server ``TIME``, and string-key verbs
        # the port deliberately does not carry (see delivery_lease.py's module
        # docstring). Optional, and None for a base-only consumer or a second
        # broker implementation -- that construction must keep working exactly
        # as it did before the lease existed.
        self._leases: DeliveryLeaseStore | None = leases
        # Leases this process is holding RIGHT NOW, keyed by entry id, populated
        # and cleared by ``_delivery_lease``. It is the ONE registry of in-flight
        # leases -- a lane that kept its own second copy would have to stay in
        # lockstep with this one by hand, and the two fences reading different
        # dicts is exactly how a fence silently stops seeing its lease.
        #
        # It exists so a caller that did not receive the lease can still find it
        # without threading one through a shared signature. ``_dead_letter`` uses
        # it to tell which of its two callers it is serving: a handler path is
        # registered here, the maintenance scan is not (see
        # ``_dead_letter_refusal`` for what the fence asks in each case). The
        # eval lane reads it the same way at its report fence and its budget.
        #
        # Only a REAL lease is registered; the permissive ``unfenced_lease()``
        # sentinel is not, so absence here means either "not in a handler" or
        # "this consumer has no lease store at all", and every reader must
        # disambiguate on ``self._leases`` rather than on absence alone.
        self._held_leases: dict[str, DeliveryLease] = {}
        # Fired once when a lease is lost mid-flight, so the owning lane can
        # stop its runner through its own bounded control path. The base never
        # cancels a handler task itself: a bare cancel skips the runner-side
        # stop and leaves a turn producing effects on a sandbox we no longer own.
        self._on_lease_lost: LeaseLostHandler | None = on_lease_lost
        # The pre-upgrade drain gate (#2010), or None for a consumer that has no
        # platform to be rolled by (compose, the base-only unit tests). While it
        # reports a drain in progress this consumer takes NO new work -- neither
        # a fresh read nor a reclaim -- so the deliveries already accepted can
        # reach their terminal outcome before the roll interrupts them, and the
        # replacement pods that come up mid-roll do not reclaim them out from
        # under a replica that is still draining.
        self._drain: UpgradeDrainGate | None = drain
        self._liveness_store = liveness_store
        # The first absent observation for each capable peer. It is scoped to a
        # run generation and cleared on restoration/disappearance/restart.
        self._peer_absent_since: dict[str, float] = {}
        # Prompt and 15-minute recovery both select PEL ownership through this
        # lock. It never covers handler execution or a capacity-semaphore wait.
        self._reclaim_lock = asyncio.Lock()
        self._last_liveness_renewal: float | None = None

    @property
    def _spec(self) -> DeliverySpec:
        assert self._delivery is not None, (
            "reclaim/dead-letter machinery used without a DeliverySpec"
        )
        return self._delivery

    def request_stop(self) -> None:
        self._stop.set()

    async def _claims_paused(self) -> bool:
        """Is the fleet quiesced for an upgrade drain (#2010)?

        Checked before every new claim -- a blocking read and a reclaim alike.
        Deliveries already in flight are untouched: pausing is about not taking
        on MORE work, and cutting a live turn short is the failure the drain
        exists to prevent.

        Fails OPEN. An unreadable flag degrades to the behavior this consumer
        had before the gate existed, which is a working worker. Failing closed
        would idle every replica in the release on a transient Valkey blip --
        turning the one read that is supposed to make upgrades safer into a
        fleet-wide stall, and doing it at exactly the moment Valkey is least
        healthy.
        """
        if self._drain is None:
            return False
        try:
            return await self._drain.is_quiescing()
        except Exception:
            logging.getLogger(__name__).debug(
                "quiesce flag read failed; claiming normally", exc_info=True
            )
            return False

    def _should_stop(self) -> bool:
        return self._stop.is_set() or self._generation_stop.is_set()

    async def _ensure_group(self, stream: str, group: str, *, start_id: str) -> None:
        """Create the consumer group (and the stream) if it does not exist.

        ``start_id`` is the group's read start position and is per-consumer (see
        each subclass's ``ensure_group`` for why it picks ``$`` vs ``0``). An
        existing group is left untouched.
        """
        try:
            await self._redis.xgroup_create(stream, group, id=start_id, mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _consume(self, spec: ReadLoopSpec, handler: EntryHandler) -> None:
        """Blocking-read loop: read the group, dispatch each entry to ``handler``,
        and survive transient transport faults until stop is requested."""
        while not self._should_stop():
            if await self._claims_paused():
                # Take nothing new while an upgrade drains. The in-flight
                # handlers this consumer already owns keep running under their
                # own leases and settle normally; only the read is held back.
                await self._sleep_or_stop(_QUIESCE_POLL_S)
                continue
            try:
                resp = await self._redis.xreadgroup(
                    spec.group,
                    spec.consumer,
                    {spec.stream: ">"},
                    count=spec.count,
                    block=spec.block_ms,
                )
            except RedisTimeoutError as exc:
                # A blocking-read timeout is the routine idle case (no entries
                # arrived within block_ms plus the socket timeout margin), not a
                # fault -- log at DEBUG so an idle worker doesn't flood WARNING.
                # Still back off + retry rather than letting it kill the loop.
                spec.logger.debug(spec.timeout_msg, exc)
                await self._sleep_or_stop(spec.backoff_s)
                continue
            except RedisConnectionError as exc:
                # A real connection fault (a Valkey failover, pod-to-pod blip):
                # transient but worth a WARNING. Back off and retry; redis-py
                # reconnects on the next attempt.
                spec.logger.warning(spec.connection_msg, exc)
                await self._sleep_or_stop(spec.backoff_s)
                continue
            if not resp:
                continue
            streams = cast("list[tuple[str, list[StreamEntry]]]", resp)
            for _stream, entries in streams:
                for entry_id, fields in entries:
                    try:
                        await handler(entry_id, fields)
                    except Exception:
                        # A handler-internal error must not escape this loop. The
                        # realistic trigger is a transient transport fault (a Valkey
                        # failover/blip) hit while a poison-pill entry is being
                        # dead-lettered via XADD to ``<stream>:dead`` (#585 widened
                        # this with a second eval dead-letter site). This loop shares
                        # one event loop with the other consumers (runs, evals,
                        # killswitch, heartbeat) under the top-level gather, so an
                        # escaping exception would tear its siblings down (#673).
                        # Log and continue: the entry is left un-acked in the PEL, so
                        # the reclaim loop re-delivers it (and dead-letters it once the
                        # delivery cap is hit) rather than being lost. ``CancelledError``
                        # is a ``BaseException`` and still propagates, so cooperative
                        # shutdown is unaffected.
                        spec.logger.exception(
                            "handler failed for entry %s on stream %s; "
                            "left pending for reclaim",
                            entry_id,
                            spec.stream,
                        )

    async def _sleep_or_stop(self, seconds: float) -> None:
        """Sleep until the process/generation stops or the timeout elapses."""

        process_stop = asyncio.create_task(self._stop.wait())
        generation_stop = asyncio.create_task(self._generation_stop.wait())
        try:
            await asyncio.wait(
                {process_stop, generation_stop},
                timeout=seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for task in (process_stop, generation_stop):
                if not task.done():
                    task.cancel()
            await asyncio.gather(process_stop, generation_stop, return_exceptions=True)

    async def _sleep_generation(self, seconds: float) -> None:
        """Sleep for a lease cadence, ignoring process stop during drain."""

        try:
            await asyncio.wait_for(self._generation_stop.wait(), timeout=seconds)
        except TimeoutError:
            pass

    # -- consumer liveness + structured run generations ---------------------

    def _generation_inflight_tasks(self) -> set[asyncio.Task[None]]:
        """Tasks whose handlers own PEL entries in this generation."""

        return set()

    def _transfer_capacity(self) -> int | None:
        """How many rows the lease-expiry pass may claim this tick, or None.

        ``None``, the base default, means unbounded, which is what a base-only
        consumer with no local dispatch accounting can honestly report.

        Why a bound exists at all (#2433): every row this pass claims sits
        XCLAIMed but UNLEASED until its dispatched handler acquires the delivery
        lease. A lane whose dispatch can BLOCK (the runs semaphore, the eval
        inline await) therefore parks claimed-but-unowned rows for the whole
        wait, where every other replica reads them as expired-lease candidates.
        Claiming only what can be dispatched now also routes the surplus to a
        replica that has capacity, which is the behavior a saturated node should
        have.

        Consulted ONLY by ``_select_lease_expired_entries``. The proven-dead
        transfer path is deliberately unbounded and unchanged.
        """

        return None

    def _reset_generation_resources(self) -> None:
        """Subclass hook for semaphore/accounting reset after forced teardown."""

    def _reset_generation(self) -> None:
        self._generation_stop = asyncio.Event()
        self._peer_absent_since.clear()
        self._inflight_ids.clear()
        self._last_liveness_renewal = None

    def _liveness_timeout_s(self) -> float:
        # One renewal attempt may consume at most one sixth of the lease, so a
        # timed-out attempt plus a retry still fit before the pre-expiry guard.
        # The
        # 1ms floor keeps deliberately tiny integration-test leases usable.
        return max(0.001, self._spec.heartbeat_ttl_ms / 6000)

    async def _publish_liveness(self) -> None:
        if self._liveness_store is None:
            raise RuntimeError("consumer generation has no liveness store")
        async with asyncio.timeout(self._liveness_timeout_s()):
            await self._liveness_store.publish(
                stream=self._spec.stream,
                group=self._spec.group,
                consumer=self._spec.consumer,
                heartbeat_ttl_ms=self._spec.heartbeat_ttl_ms,
                capability_ttl_ms=self._spec.capability_ttl_ms,
            )
        self._last_liveness_renewal = time.monotonic()

    async def _liveness_refresh_loop(self) -> None:
        """Renew alive/capability until this run generation ends.

        One transient failure is not terminal. What matters is monotonic time
        since the last confirmed renewal: before the alive lease can silently
        expire, this task raises and structured teardown leaves owned entries in
        the PEL for a replacement generation/process.
        """

        assert self._liveness_store is not None
        ttl_s = self._spec.heartbeat_ttl_ms / 1000
        refresh_s = ttl_s / 3
        expiry_guard_s = max(0.001, ttl_s - refresh_s)
        retry_s = max(0.001, min(refresh_s / 4, 0.25))
        failed_once = False

        await self._sleep_generation(refresh_s)
        while not self._generation_stop.is_set():
            last = self._last_liveness_renewal
            now = time.monotonic()
            elapsed = 0.0 if last is None else now - last
            remaining_guard = expiry_guard_s - elapsed
            remaining_lease = ttl_s - elapsed
            # Guard minus 1ms on the first pass so a hang-forever attempt can
            # still retry. Once that window is thinner than retry_s, remaining
            # key TTL keeps a recovered SET alive instead of a 1ms floor.
            # Cap by leftover TTL minus retry_s so a late refresh sleep still
            # leaves room for one retry before the key expires.
            if remaining_guard - 0.001 >= retry_s:
                computed = remaining_guard - 0.001
            else:
                computed = max(0.001, remaining_lease)
            attempt_timeout = min(computed, max(0.001, ttl_s - elapsed - retry_s))
            try:
                async with asyncio.timeout(attempt_timeout):
                    await self._liveness_store.renew(
                        stream=self._spec.stream,
                        group=self._spec.group,
                        consumer=self._spec.consumer,
                        heartbeat_ttl_ms=self._spec.heartbeat_ttl_ms,
                        capability_ttl_ms=self._spec.capability_ttl_ms,
                    )
            except Exception as exc:
                # ``CancelledError`` remains a BaseException and propagates.
                last = self._last_liveness_renewal
                elapsed = float("inf") if last is None else time.monotonic() - last
                # A first timeout that crossed the pre-expiry guard still retries
                # if the key TTL has not elapsed. Raise after a second failure
                # in that window, or once the key itself is past TTL.
                if elapsed >= ttl_s or (elapsed >= expiry_guard_s and failed_once):
                    raise ConsumerLivenessExpired(
                        "consumer liveness renewal could not be confirmed before "
                        f"lease expiry for {self._spec.consumer}"
                    ) from exc
                failed_once = True
                self._spec.logger.warning(
                    "consumer liveness renewal failed transiently for %s; retrying",
                    self._spec.consumer,
                    exc_info=True,
                )
                leftover = expiry_guard_s - elapsed
                # Do not sleep past the guard; retry immediately when leftover
                # is too small for retry_s.
                if leftover > retry_s:
                    await self._sleep_generation(retry_s)
                continue
            failed_once = False
            self._last_liveness_renewal = time.monotonic()
            await self._sleep_generation(refresh_s)

    async def _cleanup_alive(self) -> None:
        if self._liveness_store is None:
            return
        try:
            await self._liveness_store.cleanup_alive(
                stream=self._spec.stream,
                group=self._spec.group,
                consumer=self._spec.consumer,
                timeout_s=self._liveness_timeout_s(),
            )
        except Exception:
            # Cleanup is best effort. The short TTL remains the authoritative
            # bound if Valkey is unavailable while this generation terminates.
            self._spec.logger.warning(
                "consumer alive-lease cleanup failed for %s; awaiting TTL",
                self._spec.consumer,
                exc_info=True,
            )

    async def _cancel_and_join(self, tasks: set[asyncio.Task[None]]) -> None:
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_consumer_generation(
        self,
        factories: dict[str, Callable[[], Coroutine[Any, Any, None]]],
        *,
        may_complete: frozenset[str] = frozenset(),
    ) -> None:
        """Run one supervised consumer generation with structured teardown.

        ``request_stop`` is graceful: stop/cancel new-work loops, drain existing
        handlers while the lease refresher stays alive, then remove the alive
        lease. A child/refresher failure is terminal: cancel and join every loop
        and handler, reset generation-local ownership/accounting, and re-raise so
        the process supervisor starts exactly one clean generation.
        """

        self._reset_generation()
        reserved = factories.keys() & {"bootstrap", "liveness"}
        if reserved:
            raise ValueError(f"consumer generation factories use reserved names: {reserved}")
        await self._publish_liveness()  # ordered publication precedes every read

        # A prior generation in this same pod can have been canceled after its
        # alive lease became unverifiable. Recover rows still owned by this
        # stable consumer name before reading new work. The refresher is already
        # running while eval's inline handler drains this bootstrap.
        tasks: dict[str, asyncio.Task[None]] = {}
        tasks["liveness"] = asyncio.create_task(
            self._liveness_refresh_loop(), name="consumer:liveness"
        )
        tasks["bootstrap"] = asyncio.create_task(
            self._recover_local_pending_once(), name="consumer:bootstrap"
        )
        stop_waiter = asyncio.create_task(self._stop.wait(), name="consumer:process-stop")

        failure: BaseException | None = None
        graceful = False
        try:
            while tasks:
                done, _pending = await asyncio.wait(
                    {*tasks.values(), stop_waiter},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stop_waiter in done:
                    graceful = True
                    break
                for name, task in list(tasks.items()):
                    if task not in done:
                        continue
                    del tasks[name]
                    try:
                        task.result()
                    except BaseException as exc:
                        failure = exc
                        break
                    if name == "bootstrap":
                        tasks.update(
                            {
                                child_name: asyncio.create_task(
                                    factory(), name=f"consumer:{child_name}"
                                )
                                for child_name, factory in factories.items()
                            }
                        )
                        continue
                    if name not in may_complete:
                        failure = RuntimeError(
                            f"consumer generation task {name!r} exited unexpectedly"
                        )
                        break
                if failure is not None:
                    break

            if graceful:
                # No new ownership after this point. Eval dispatch shields its
                # inline handler; runs handlers already live in the in-flight set.
                background = {
                    task for name, task in tasks.items() if name != "liveness"
                }
                await self._cancel_and_join(background)

                # Snapshot after producer loops are joined, so no new handler can
                # appear while graceful drain is in progress.
                inflight = self._generation_inflight_tasks()
                if inflight:
                    await asyncio.gather(*inflight, return_exceptions=True)

                self._generation_stop.set()
                liveness = tasks.get("liveness")
                if liveness is not None:
                    await asyncio.gather(liveness, return_exceptions=True)
                return

            # A terminal liveness or child failure must leave no zombie loop or
            # handler that could race the replacement generation.
            self._generation_stop.set()
            await self._cancel_and_join(set(tasks.values()))
            await self._cancel_and_join(self._generation_inflight_tasks())
            self._reset_generation_resources()
            # All old-generation work is joined. Replace the event and clear
            # ownership/absence state before the supervisor's restart backoff so
            # the next ``run()`` cannot inherit a terminal generation.
            self._reset_generation()
            if failure is None:
                failure = RuntimeError("consumer generation exited without a terminal cause")
            raise failure
        finally:
            if not stop_waiter.done():
                stop_waiter.cancel()
            await asyncio.gather(stop_waiter, return_exceptions=True)
            await self._cleanup_alive()

    async def _xack(self, stream: str, group: str, entry_id: str) -> None:
        await self._redis.xack(stream, group, entry_id)

    async def _ack(self, entry_id: str) -> None:
        await self._redis.xack(self._spec.stream, self._spec.group, entry_id)

    async def _settle_delivery(self, entry_id: str) -> None:
        """Remove this delivery's lease AND its state after a terminal settlement.

        ADR-0131: delivery state is "removed after terminal acknowledgement or
        dead-letter settlement". The base's lease release drops only the lease,
        so without this a dead-lettered-and-redelivered entry id accumulates
        state keys until their retention TTL expires them.

        RAISES THROUGH on failure. It is the caller that knows whether a settle
        failure is recoverable, and the two shapes genuinely differ: a caller
        that is already inside its own error handling (``_dead_letter``, which
        must mark the span and the metric and re-raise) needs the exception, and
        a caller that has already acked wants
        :meth:`_settle_delivery_best_effort` instead. Both lanes read the
        delivery off ``self._spec``, which is exactly the runs/eval difference
        (``stream`` vs ``eval_stream``) each lane used to spell out itself.
        """
        if self._leases is None:
            return
        await self._leases.settle(self._spec.stream, self._spec.group, entry_id)

    async def _settle_delivery_best_effort(self, entry_id: str) -> None:
        """:meth:`_settle_delivery` for a caller that has ALREADY acked.

        Best-effort by construction: the entry is already off the group by the
        time this runs, so raising here would only turn a settled turn or suite
        into a logged failure. The state key's own retention is the backstop for
        a crash between the ack and here, not the normal way it goes away.
        """
        if self._delivery is None or self._leases is None:
            # Nothing to settle without both a bound DeliverySpec and a lease
            # store -- ``_settle_delivery`` would no-op on a missing lease
            # store anyway, and a missing DeliverySpec must be handled here,
            # before the try, because the except below reads ``self._spec``
            # for its logger and that property raises on a None delivery too.
            # Swallowing is this method's whole contract, so bail out early
            # rather than let that second read escape uncaught.
            return
        try:
            await self._settle_delivery(entry_id)
        except Exception:
            self._spec.logger.warning(
                "settling the delivery state for entry %s on stream %s failed; "
                "leaving it to its retention TTL",
                entry_id,
                self._spec.stream,
                exc_info=True,
            )

    async def _entry_fields(self, entry_id: str) -> dict[str, str] | None:
        """The original entry's fields, or None if it was already trimmed off the
        stream (then a metadata-only graveyard row is written)."""
        rows = await self._redis.xrange(self._spec.stream, min=entry_id, max=entry_id)
        return dict(rows[0][1]) if rows else None

    async def _lease_is_live(self, entry_id: str) -> bool:
        """Does some owner hold this delivery right now?

        FAIL CLOSED on an unreadable answer. Every caller uses this to decide
        whether it may dead-letter or dispatch a delivery, and ADR-0131 is
        explicit that "loss of the ownership store cannot be treated as
        permission to continue producing effects" -- so a Valkey blip reads as
        "somebody owns it", which costs one skipped maintenance pass, rather
        than as "nobody owns it", which costs a duplicate turn or a healthy
        turn's dead-letter.

        With no lease store configured at all there is no fence to consult and
        the answer is False, leaving the pre-ADR-0131 behavior intact.
        """
        if self._leases is None:
            return False
        try:
            return await self._leases.is_live(
                self._spec.stream, self._spec.group, entry_id
            )
        except Exception:
            self._spec.logger.warning(
                "lease liveness unreadable for entry %s on stream %s; "
                "treating it as OWNED and skipping this pass",
                entry_id,
                self._spec.stream,
                exc_info=True,
            )
            return True

    async def _has_delivery_state(self, entry_id: str) -> bool:
        """Was a delivery lease ever granted for this entry?

        The mirror image of :meth:`_lease_is_live`'s posture. That one fails
        closed as OWNED; this one fails closed as ABSENT, meaning "no evidence".
        Both answers refuse the claim, which is the point: failing open here
        would let one Valkey blip manufacture a duplicate dispatch of a turn
        somebody may still be running.

        Absence is what keeps the mixed-version rule true (#2433). A pre-lease or
        pre-marker consumer's pending entry carries no evidence a lease was ever
        granted, so "the lease is gone" cannot be told apart from "there never
        was one", and such an entry stays on the unchanged XAUTOCLAIM backstop.
        With no lease store configured there is no evidence to read either.
        """
        if self._leases is None:
            return False
        try:
            return await self._leases.has_state(
                self._spec.stream, self._spec.group, entry_id
            )
        except Exception:
            self._spec.logger.warning(
                "delivery state unreadable for entry %s on stream %s; treating it "
                "as ABSENT and leaving it on the backstop",
                entry_id,
                self._spec.stream,
                exc_info=True,
            )
            return False

    @asynccontextmanager
    async def _delivery_lease(
        self, entry_id: str, fields: dict[str, str]
    ) -> AsyncIterator[DeliveryLease | None]:
        """Hold delivery authority for the body of a handler (ADR-0131).

        Lives HERE, once, rather than in each lane, because the ADR requires the
        runs and eval consumers to share the lease implementation by
        construction: "a fix on only one consumer lane is incomplete."

        Yields the lease on success and ``None`` when acquisition was refused --
        a refusal is a normal early return from the handler, never an exception.
        An exception would be caught by ``_consume``'s isolation guard and
        logged as a failure, which is correct but noisy for the routine case of
        losing a race to a peer that is already working the entry.

        With no lease store, yields the permissive sentinel so a base-only
        consumer is unchanged. That is the one place a missing lease is read as
        permission; nowhere else may it be.
        """
        if self._leases is None:
            yield unfenced_lease()
            return

        spec = self._spec
        try:
            lease = await self._leases.acquire(
                spec.stream, spec.group, entry_id, consumer=spec.consumer
            )
        except LeaseRefused as refused:
            if refused.reason == "held":
                # Routine under contention: a peer holds a live lease and is
                # working this entry. Return without acking; it stays pending
                # for its true owner.
                spec.logger.debug(
                    "entry %s on stream %s is leased elsewhere; not dispatching",
                    entry_id,
                    spec.stream,
                )
            else:
                # The PEL and our belief have diverged -- we were about to fence
                # a delivery Valkey never gave us. Not routine.
                spec.logger.warning(
                    "refused the delivery lease for entry %s on stream %s (%s); "
                    "this consumer does not hold the pending entry",
                    entry_id,
                    spec.stream,
                    refused.reason,
                )
            yield None
            return

        heartbeat = asyncio.create_task(self._heartbeat_lease(lease, entry_id, fields))
        # Registered BEFORE the body runs, so the dead-letter fence can find this
        # lease from anywhere inside the handler -- including the unparseable
        # path, which reaches the graveyard without ever naming its lease.
        self._held_leases[entry_id] = lease
        try:
            yield lease
        finally:
            # Deregistered FIRST: once the body is done this process is no longer
            # a holder, and a later maintenance-scan dead-letter of the same id
            # must fall through to the "does anybody else own it" question rather
            # than consulting a lease we have already finished with.
            self._held_leases.pop(entry_id, None)
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat
            try:
                await self._leases.release(
                    spec.stream, spec.group, entry_id, owner=lease.owner
                )
            except Exception:
                # Releasing is an optimization: the lease expires on its own at
                # ``delivery_lease_ttl_s``. Raising out of this ``finally`` would
                # mask whatever the handler was already failing with.
                spec.logger.warning(
                    "releasing the delivery lease for entry %s failed; "
                    "leaving it to expire",
                    entry_id,
                    exc_info=True,
                )

    async def _heartbeat_lease(
        self, lease: DeliveryLease, entry_id: str, fields: dict[str, str]
    ) -> None:
        """Renew the lease until it is lost or the handler finishes.

        Sleeps with a PLAIN ``asyncio.sleep``, never ``_sleep_or_stop``. A
        SIGTERM sets ``_stop`` while the worker is deliberately draining its
        in-flight turns, and a stop-aware sleep here would drop every in-flight
        lease the instant the signal landed -- turning a graceful rollout into a
        fleet-wide fence-out, the exact opposite of draining. The platform
        termination grace is sized to cover the budget plus the shutdown
        reserve precisely so this loop may keep running through shutdown.

        Any refusal OR any exception is lease-lost. ADR-0131: "if Valkey cannot
        confirm renewal, the owner fails closed as lease-lost."
        """
        assert self._leases is not None
        spec = self._spec
        while True:
            await asyncio.sleep(self._leases.heartbeat_interval_s)
            try:
                budget = await self._leases.heartbeat(
                    spec.stream,
                    spec.group,
                    entry_id,
                    consumer=spec.consumer,
                    owner=lease.owner,
                    generation=lease.generation,
                )
            except Exception as exc:  # noqa: BLE001 - any failure is lease-lost
                budget = None
                reason = f"renewal raised {type(exc).__name__}: {exc}"
            else:
                reason = "renewal refused by Valkey"
            if budget is None:
                lease.lost.set()
                spec.logger.warning(
                    "delivery lease LOST for entry %s on stream %s "
                    "(owner=%s generation=%d): %s; this owner may no longer ack, "
                    "dead-letter, or emit a terminal result",
                    entry_id,
                    spec.stream,
                    lease.owner,
                    lease.generation,
                    reason,
                )
                if self._on_lease_lost is not None:
                    try:
                        await self._on_lease_lost(entry_id, fields)
                    except Exception:
                        # A failure to stop the runner must not mask the loss
                        # itself, which ``lease.lost`` has already recorded and
                        # which the settle boundaries enforce on their own.
                        spec.logger.exception(
                            "lease-lost handler failed for entry %s on stream %s",
                            entry_id,
                            spec.stream,
                        )
                return
            # Re-anchor in place so the holder always reads the freshest
            # observation. The DEADLINE never moves: only the anchor does.
            lease.budget = budget

    async def _dead_letter_refusal(self, entry_id: str) -> str | None:
        """Why this process may NOT dead-letter ``entry_id``, or None if it may.

        ADR-0131 names dead-letter as one of the four verbs a fenced-out owner
        loses: *"A stale owner that fails a renewal immediately loses authority
        ... and may not ACK, dead-letter, clear an outbox record, or emit a
        terminal result."* Dead-letter is a terminal settlement -- it ACKs the
        delivery off the group and then deletes the lease and delivery state --
        so an unfenced one lets a stale process settle a delivery that now
        belongs to somebody else, and delete the CURRENT owner's keys on the way
        out.

        The two callers arrive from opposite sides of the fence, so the question
        asked is different at each:

        - **A handler path** (the unparseable/poison entry) is INSIDE
          ``_delivery_lease`` and is registered in ``_held_leases``. The question
          is whether our own lease is still ours: a heartbeat that failed while
          the graveyard row was being prepared has already set ``lost``, and a
          lost lease is authority we no longer have.
        - **``_dead_letter_over_cap``** holds no lease -- it got here precisely
          because ``_lease_is_live`` said nobody did. The question is whether
          that is STILL true: a new owner may have acquired in the window since
          that read, and its delivery must not be settled underneath it. The
          re-read fails closed on an unreadable answer, like every other use.

        With no lease store there is no fence to consult and nothing is ever
        refused, so a consumer built without leases dead-letters exactly as it
        did before ADR-0131.
        """
        if self._leases is None:
            return None
        held = self._held_leases.get(entry_id)
        if held is not None:
            if held.lost.is_set():
                return (
                    "this owner's delivery lease was lost mid-flight "
                    f"(owner={held.owner}, generation={held.generation})"
                )
            return None
        if await self._lease_is_live(entry_id):
            return "another owner now holds the delivery lease"
        return None

    async def _dead_letter(
        self,
        entry_id: str,
        fields: dict[str, str] | None,
        *,
        reason: str,
        delivery_count: int,
    ) -> None:
        """Move an entry to the graveyard and ack it off the main group.

        ``fields`` are the original entry's fields, kept verbatim so a human or a
        replay tool can inspect them; ``None`` writes a metadata-only row (the
        entry was pending but its message had been trimmed off the stream).

        The ``dl_`` prefix is a CONVENTION, not a guarantee: the unparseable path
        accepts arbitrary malformed field maps, so an entry may already carry its
        own ``dl_original_id``. A plain copy-then-update would let the payload
        forge (or be silently clobbered by) the graveyard's own metadata. Original
        keys already starting with ``dl_`` are therefore escaped by doubling the
        prefix (``dl_reason`` -> ``dl_dl_reason``) before the metadata is written
        last. The escape is injective -- escaped keys always start with ``dl_dl_``
        and so can never equal a metadata key, and un-escaping strips exactly one
        ``dl_`` -- so the metadata always wins AND the original is fully
        recoverable.

        Fenced (ADR-0131): dead-letter is a terminal settlement, so it is gated on
        this process still holding delivery authority. ``_dead_letter_refusal``
        explains what that means for each caller; a refusal returns having
        written nothing and acked nothing, leaving the entry pending for whoever
        now owns it.

        XADD before XACK, deliberately: a crash between the two leaves the entry
        pending, so it is re-reclaimed and re-dead-lettered -- a duplicate
        graveyard row, which is strictly safer than the XACK-first ordering's
        failure mode (a lost entry). Two replicas racing the same over-cap entry
        produce the same acceptable duplicate.

        The XADD is bounded by an approximate ``dead_letter_maxlen``, so
        graveyard rows are BEST-EFFORT: under a flood the oldest rows are evicted
        and those failures are lost. That loss is deliberate. The unparseable
        path dead-letters per inbound entry, so a wire-format drift would grow an
        unbounded graveyard at full ingest rate on the same Valkey that holds the
        kernel's locks and side-effect markers -- bounded record loss is traded
        against a platform-wide OOM. ``approximate=True`` lets Valkey trim on
        node boundaries, so the stream is bounded at *at least* the configured
        length, not exactly it.
        """
        attributes = {
            "service.name": "curie-worker",
            "source": self._spec.telemetry_source,
        }
        # The fence, asked as a PRECONDITION (ADR-0131; see
        # ``_dead_letter_refusal``). Asked here rather than further down so a
        # refusal costs nothing: no graveyard row is written for a delivery we
        # are not going to settle, and the XADD-before-XACK ordering below is
        # untouched -- this is a new gate in front of the sequence, not a
        # reordering of it. Refusal returns rather than raising: losing the fence
        # is the routine outcome of a peer taking over, and an exception would
        # reach ``_consume``'s isolation guard as a per-entry stack trace.
        refusal = await self._dead_letter_refusal(entry_id)
        if refusal is not None:
            self._spec.logger.warning(
                "refusing to dead-letter entry %s on stream %s (%s); leaving it "
                "pending for its current owner and touching no lease or state key",
                entry_id,
                self._spec.stream,
                refusal,
            )
            record_metric(
                "curie.queue.settle",
                attributes={**attributes, "outcome": "pending"},
            )
            return

        target = self._spec.dead_letter_target
        # Escape the original's own ``dl_*`` keys (see above) so the metadata
        # written last always wins and the original stays recoverable.
        payload: dict[str, str] = {
            (f"dl_{k}" if k.startswith("dl_") else k): v for k, v in (fields or {}).items()
        }
        payload.update(
            {
                "dl_original_id": entry_id,
                "dl_delivery_count": str(delivery_count),
                "dl_reason": reason,
                "dl_dead_lettered_at": datetime.now(UTC).isoformat(),
            }
        )
        error: Exception | None = None
        with operation_span(
            "curie.queue.dead-letter",
            kind=SpanKind.INTERNAL,
            attributes=attributes,
        ) as span:
            try:
                await self._redis.xadd(
                    target,
                    payload,
                    maxlen=self._spec.dead_letter_maxlen,
                    approximate=True,
                )
                self._spec.logger.error(
                    self._spec.dead_letter_log,
                    entry_id,
                    delivery_count,
                    reason,
                    target,
                )
                # The XADD above is a round trip, so a heartbeat can fail while
                # it is in flight. Re-read the fence immediately before the ACK
                # to close that window -- but only the FREE in-process flag a
                # held lease already carries, never a second Valkey read, so the
                # ordering is unchanged and the maintenance path (which holds no
                # lease) pays nothing. Fails closed by raising: the graveyard row
                # is already written, so unlike the precondition above there is
                # no clean early return left, and a duplicate row on the true
                # owner's later dead-letter is the ordering's accepted cost.
                held = self._held_leases.get(entry_id)
                if held is not None:
                    held.raise_if_lost()
                await self._ack(entry_id)
                # ADR-0131: delivery state is "removed after terminal
                # acknowledgement or dead-letter settlement". After the ACK and
                # inside the same try, and deliberately the RAISING settle rather
                # than the best-effort one the post-ack handler paths use: here a
                # settle failure must be caught by the handler below and reported
                # as a failed dead-letter, not swallowed into a half-settled
                # entry. The XADD-before-XACK ordering above is untouched.
                await self._settle_delivery(entry_id)
            except Exception as exc:
                error = exc
                if hasattr(span, "set_status"):
                    span.set_status(StatusCode.ERROR)
                span.add_event(
                    "queue.dead-letter.failed",
                    {"outcome": "failure", "error.class": type(exc).__name__},
                )
            else:
                span.add_event("queue.dead-lettered", {"outcome": "dead-letter"})

        outcome = "failure" if error is not None else "success"
        record_metric(
            "curie.queue.dead_letter",
            attributes={**attributes, "outcome": outcome},
        )
        record_metric(
            "curie.queue.settle",
            attributes={
                **attributes,
                "outcome": "pending" if error is not None else "dead-letter",
            },
        )
        if error is not None:
            raise error

    async def _dead_letter_over_cap(self) -> set[str]:
        """Dead-letter pending entries that have exhausted their delivery budget.

        Returns every over-cap id seen this pass -- including ones whose
        dead-letter failed -- so the caller never re-dispatches them.

        Read the delivery count with XPENDING *before* XAUTOCLAIM rather than
        after, because XAUTOCLAIM increments the counter as it claims: the
        pre-claim value is the number of deliveries ALREADY made, so an entry at
        ``>= max_delivery`` has had its full budget and must not be claimed
        again. Reading post-claim would fold in the current claim's own bump and
        kill the entry one delivery early.

        The scan PAGES THROUGH THE WHOLE pending list (``min`` advanced past the
        last id seen), because XAUTOCLAIM below pages through all of it too: a
        single ``COUNT cap_scan_page`` page would cap-check only the head of the
        list while XAUTOCLAIM happily claimed and dispatched the over-cap tail,
        so the bound would silently not hold at backlog scale.

        The IDLE filter matches XAUTOCLAIM's ``min_idle_time`` so both see the
        same candidate set and an entry that is not yet reclaim-eligible is never
        prematurely dead-lettered.

        A failure to dead-letter ONE entry is logged and isolated: it must not
        stop the other entries being cap-checked, nor XAUTOCLAIM, nor
        ``reap_orphans``. This is the first await of the maintenance tick, and an
        unguarded raise here would kill crash recovery for the whole group on
        every tick -- #505's own stall class.
        """
        over_cap: set[str] = set()
        page_size = self._spec.cap_scan_page
        cursor = "-"
        while True:
            pending = await self._redis.xpending_range(
                self._spec.stream,
                self._spec.group,
                min=cursor,
                max="+",
                count=page_size,
                idle=self._spec.reclaim_min_idle_ms,
            )
            if not pending:
                break
            for row in pending:
                entry_id = str(row["message_id"])
                # An entry in flight on THIS consumer is not an orphan: it is
                # being worked right now, and its count must not be judged. This
                # guard stays ahead of the cap check.
                if entry_id in self._inflight_ids:
                    continue
                # ADR-0131: "a live lease is checked before cap evaluation so a
                # healthy long turn cannot be dead-lettered." This is the
                # cross-replica sibling of the guard above -- that one protects
                # our OWN in-flight entry, this one protects a peer's. It sits
                # ahead of the cap check for the same reason: an entry somebody
                # is actively working has not spent its budget on failures, and
                # its count must not be judged.
                if await self._lease_is_live(entry_id):
                    continue
                delivered = int(row["times_delivered"])
                if delivered < self._spec.max_delivery:
                    continue
                over_cap.add(entry_id)
                try:
                    fields = await self._entry_fields(entry_id)
                    await self._dead_letter(
                        entry_id,
                        fields,
                        reason=self._spec.over_cap_reason,
                        delivery_count=delivered,
                    )
                except Exception:
                    self._spec.logger.exception(
                        self._spec.dead_letter_fail_log,
                        entry_id,
                    )
            if len(pending) < page_size:
                break
            # Exclusive lower bound: resume after the last id of this page.
            cursor = f"({pending[-1]['message_id']}"
        return over_cap

    async def _select_dead_consumer_entries(
        self, over_cap: set[str]
    ) -> list[StreamEntry]:
        """Select/transfer entries only after sustained lease absence.

        Caller holds ``_reclaim_lock``. Consumer idle is merely a cheap
        candidate threshold. A peer is proven dead only when it advertised the
        liveness protocol and its alive lease is absent on two observations at
        least one full heartbeat TTL apart. Unknown/pre-marker peers remain on
        the unchanged XAUTOCLAIM backstop. Even after that proof, ADR-0131's
        delivery lease remains authoritative: a live delivery fence is skipped
        before cap evaluation and again after transfer.
        """

        if self._liveness_store is None:
            return []
        try:
            consumers = await self._redis.xinfo_consumers(
                self._spec.stream, self._spec.group
            )
        except ResponseError:
            self._peer_absent_since.clear()
            return []

        current_names = {str(info["name"]) for info in consumers}
        for missing in self._peer_absent_since.keys() - current_names:
            self._peer_absent_since.pop(missing, None)

        now = time.monotonic()
        required_absence_s = self._spec.heartbeat_ttl_ms / 1000
        selected: list[StreamEntry] = []
        for info in consumers:
            if self._should_stop():
                break
            name = str(info["name"])
            if (
                name == self._spec.consumer
                or int(info.get("pending") or 0) <= 0
                or int(info.get("idle") or 0) < self._spec.dead_consumer_idle_ms
            ):
                self._peer_absent_since.pop(name, None)
                continue

            try:
                capable = await self._liveness_store.is_capable(
                    stream=self._spec.stream,
                    group=self._spec.group,
                    consumer=name,
                )
                if not capable:
                    self._peer_absent_since.pop(name, None)
                    continue
                alive = await self._liveness_store.is_alive(
                    stream=self._spec.stream,
                    group=self._spec.group,
                    consumer=name,
                )
            except Exception:
                # An uncertain observation cannot contribute to proof of death.
                self._peer_absent_since.pop(name, None)
                self._spec.logger.warning(
                    "consumer liveness observation failed for %s; prompt reclaim skipped",
                    name,
                    exc_info=True,
                )
                continue

            if alive:
                self._peer_absent_since.pop(name, None)
                continue
            first_absent = self._peer_absent_since.setdefault(name, now)
            if now - first_absent < required_absence_s:
                continue
            token: str | None = None
            try:
                # Every surviving replica reaches this proof point at roughly
                # the same time. A Valkey NX lease chooses one transfer owner so
                # racing XCLAIM calls cannot burn several delivery attempts.
                token = await self._liveness_store.try_acquire_reclaim(
                    stream=self._spec.stream,
                    group=self._spec.group,
                    consumer=name,
                    ttl_ms=max(self._spec.heartbeat_ttl_ms * 2, 60_000),
                )
                if token is None:
                    continue
                # The alive lease can be republished between the observation
                # above and winning arbitration. Re-check inside the lease so a
                # stale absence never transfers work from a restarted owner.
                if await self._liveness_store.is_alive(
                    stream=self._spec.stream,
                    group=self._spec.group,
                    consumer=name,
                ):
                    self._peer_absent_since.pop(name, None)
                    continue
                selected.extend(
                    await self._claim_consumer_pending_locked(name, over_cap)
                )
            except Exception:
                self._spec.logger.exception(
                    "dead-consumer reclaim failed for %s on stream %s; left pending",
                    name,
                    self._spec.stream,
                )
            finally:
                if token is not None:
                    try:
                        await self._liveness_store.release_reclaim(
                            stream=self._spec.stream,
                            group=self._spec.group,
                            consumer=name,
                            token=token,
                        )
                    except Exception:
                        # The lease TTL is authoritative. Never discard entries
                        # already transferred merely because release failed.
                        self._spec.logger.warning(
                            "dead-consumer reclaim lease release failed for %s",
                            name,
                            exc_info=True,
                        )
        return selected

    async def _recover_local_pending_once(self) -> None:
        """Recover rows canceled by an earlier generation with this same name."""

        if self._liveness_store is None:
            raise RuntimeError("consumer liveness store is required")
        retry_s = min(max(0.001, self._spec.heartbeat_ttl_ms / 3000), 1.0)
        while not self._should_stop():
            token: str | None = None
            entries: list[StreamEntry] = []
            async with self._reclaim_lock:
                try:
                    # A peer can begin transferring this stable consumer name
                    # while the local supervisor is between generations.
                    # Bootstrap must contend on the same distributed lease or
                    # both XCLAIM calls can dispatch the row and spend two
                    # delivery attempts.
                    token = await self._liveness_store.try_acquire_reclaim(
                        stream=self._spec.stream,
                        group=self._spec.group,
                        consumer=self._spec.consumer,
                        ttl_ms=max(self._spec.heartbeat_ttl_ms * 2, 60_000),
                    )
                    if token is not None:
                        entries = await self._claim_consumer_pending_locked(
                            self._spec.consumer, set()
                        )
                finally:
                    if token is not None:
                        try:
                            await self._liveness_store.release_reclaim(
                                stream=self._spec.stream,
                                group=self._spec.group,
                                consumer=self._spec.consumer,
                                token=token,
                            )
                        except Exception:
                            self._spec.logger.warning(
                                "local pending recovery lease release failed for %s",
                                self._spec.consumer,
                                exc_info=True,
                            )
            if token is not None:
                await self._dispatch_reclaimed(entries)
                return
            await self._sleep_generation(retry_s)

    async def _claim_consumer_pending_locked(
        self, name: str, over_cap: set[str]
    ) -> list[StreamEntry]:
        """Transfer one proven-dead peer's PEL, enforcing cap before XCLAIM.

        Caller holds ``_reclaim_lock``. At/over-cap entries are dead-lettered
        directly without XCLAIM's delivery-count increment. Handler dispatch is
        returned to the caller and always occurs after the lock is released.
        """

        selected: list[StreamEntry] = []
        cursor = "-"
        page_size = self._spec.read_count
        while not self._should_stop():
            rows = await self._redis.xpending_range(
                self._spec.stream,
                self._spec.group,
                min=cursor,
                max="+",
                count=page_size,
                consumername=name,
            )
            if not rows:
                break
            # ``min_idle_ms=None`` means compare-and-claim against each row's own
            # observed idle. Never 0: a stalled claim here must lose to a pass
            # that already took the row. See ``_claim_pending_page``.
            selected.extend(
                await self._claim_pending_page(rows, over_cap, min_idle_ms=None)
            )
            if len(rows) < page_size:
                break
            cursor = f"({rows[-1]['message_id']}"
        return selected

    async def _claim_pending_page(
        self,
        rows: list[Any],
        over_cap: set[str],
        *,
        min_idle_ms: int | None,
        require_delivery_state: bool = False,
        budget: int | None = None,
    ) -> list[StreamEntry]:
        """Transfer one page of pending rows, enforcing cap before XCLAIM.

        The ONE transfer primitive, shared by the proven-dead path and the
        lease-expiry pass, so ADR-0039's rules (the pre-claim delivery count, the
        cap at ``>=``, the ``_inflight_ids`` skip ahead of the cap, the lease
        re-read after the claim) live in one place instead of drifting in two.

        The claim's min-idle is ALWAYS the idle the caller observed for the row,
        never 0 (#2433). ``min_idle_ms=None`` means each row's own
        ``time_since_delivered`` from the pre-claim XPENDING read, which is what
        the proven-dead path and local recovery pass; an int means that value for
        every row on the page, which is what the lease-expiry pass passes, equal
        to its own scan filter. That makes XCLAIM an atomic compare-and-claim on
        the row's idle clock: any competing transfer resets that clock to about
        zero the instant it wins, so a stalled caller's claim simply returns
        nothing rather than stealing a row another pass already took. An empty
        return is the intended outcome of losing that race, not an error.

        Caller holds ``_reclaim_lock``. At/over-cap entries are dead-lettered
        directly without XCLAIM's delivery-count increment. ``budget``, when set,
        stops the page once that many rows have been selected; the rest are left
        pending with no XCLAIM at all, so a replica with capacity picks them up.
        """

        selected: list[StreamEntry] = []
        for row in rows:
            entry_id = str(row["message_id"])
            if entry_id in self._inflight_ids or entry_id in over_cap:
                continue
            if budget is not None and len(selected) >= budget:
                break
            if await self._lease_is_live(entry_id):
                continue
            if require_delivery_state and not await self._has_delivery_state(entry_id):
                continue
            delivered = int(row["times_delivered"])
            if delivered >= self._spec.max_delivery:
                over_cap.add(entry_id)
                try:
                    await self._dead_letter(
                        entry_id,
                        await self._entry_fields(entry_id),
                        reason=self._spec.over_cap_reason,
                        delivery_count=delivered,
                    )
                except Exception:
                    self._spec.logger.exception(
                        self._spec.dead_letter_fail_log,
                        entry_id,
                    )
                continue
            idle_for_row = (
                min_idle_ms
                if min_idle_ms is not None
                # Floored to 1: a just-delivered row's own
                # ``time_since_delivered`` can read 0, and XCLAIM with min-idle
                # 0 always matches regardless of ownership, which would break
                # the compare-and-claim guarantee above.
                else max(1, int(row["time_since_delivered"]))
            )
            claimed = await self._redis.xclaim(
                self._spec.stream,
                self._spec.group,
                self._spec.consumer,
                idle_for_row,
                [entry_id],
            )
            for claimed_id, fields in cast("list[StreamEntry]", claimed or []):
                if claimed_id in self._inflight_ids or claimed_id in over_cap:
                    continue
                # Re-checked after the XCLAIM: the window between the filter
                # above and the claim is exactly long enough for a
                # replacement to acquire the lease.
                if await self._lease_is_live(claimed_id):
                    continue
                selected.append((claimed_id, dict(fields)))
        return selected

    async def _select_lease_expired_entries(
        self, over_cap: set[str], *, already_selected: int = 0
    ) -> list[StreamEntry]:
        """Transfer rows whose delivery lease has expired, including a live peer's.

        Caller holds ``_reclaim_lock``. The rule is ADR-0131's: "a replacement may
        transfer a delivery only after the lease has expired." The failure it
        closes (#2433) is a LIVE consumer's own PEL row: a handler that raised
        released its lease and left the entry pending, and no prompt path looks at
        a peer that is not dead, so before this pass such an entry waited out the
        900 second XAUTOCLAIM backstop.

        Inert without a lease store or without a configured threshold, so a
        base-only consumer keeps its pre-ADR-0131 behavior exactly.
        """

        idle_ms = self._spec.lease_expired_idle_ms
        if self._leases is None or idle_ms is None:
            return []
        # Claim only what this process can dispatch NOW. Everything claimed here
        # sits XCLAIMed but UNLEASED until its handler acquires the delivery
        # lease, and the runs lane's ``_dispatch`` waits on a semaphore before it
        # even gets that far, so an unbounded pass on a saturated node parks rows
        # a peer will legitimately compare-and-claim one lease TTL later.
        # ``already_selected`` is what the proven-dead pass already took this same
        # tick: nothing is dispatched until every pass has finished selecting, so
        # without it a concurrency-one consumer would still see a free slot here.
        # ``None`` capacity means unbounded (the base default).
        capacity = self._transfer_capacity()
        budget = None if capacity is None else max(0, capacity - already_selected)
        if budget is not None and budget <= 0:
            return []

        selected: list[StreamEntry] = []
        page_size = self._spec.cap_scan_page
        cursor = "-"
        while not self._should_stop():
            rows = await self._redis.xpending_range(
                self._spec.stream,
                self._spec.group,
                min=cursor,
                max="+",
                count=page_size,
                idle=idle_ms,
            )
            if not rows:
                break
            # Compare-and-claim: the min-idle handed to XCLAIM is exactly the IDLE
            # this scan filtered on, so a row any competing transfer path already
            # took (which reset its idle to about zero) no longer matches and this
            # pass simply does not win it. Never 0.
            claimed = await self._claim_pending_page(
                rows,
                over_cap,
                min_idle_ms=idle_ms,
                require_delivery_state=True,
                budget=None if budget is None else budget - len(selected),
            )
            selected.extend(claimed)
            if budget is not None and len(selected) >= budget:
                break
            if len(rows) < page_size:
                break
            # Exclusive lower bound: a single page would leave the tail unscanned
            # at backlog scale.
            cursor = f"({rows[-1]['message_id']}"
        return selected

    async def _dispatch_reclaimed(self, entries: list[StreamEntry]) -> int:
        dispatched = 0
        for entry_id, fields in entries:
            if self._should_stop():
                break
            if entry_id in self._inflight_ids:
                continue
            record_metric(
                "curie.queue.retry",
                attributes={
                    "service.name": "curie-worker",
                    "source": self._spec.telemetry_source,
                    "retry_class": "redelivery",
                },
            )
            await self._spec.handler(entry_id, fields)
            dispatched += 1
        return dispatched

    async def _reclaim_dead_consumers(self) -> int:
        """Prompt-only compatible-peer reclaim, with dispatch outside lock."""

        async with self._reclaim_lock:
            entries = await self._select_dead_consumer_entries(set())
        return await self._dispatch_reclaimed(entries)

    async def _prompt_reclaim_once(self) -> int:
        """Observe capable peers and promptly recover only proven-dead owners."""

        if await self._claims_paused():
            # A prompt transfer is a new claim just as surely as XREADGROUP is.
            # Return before liveness observation, pending selection, arbitration,
            # or XCLAIM so a drain cannot charge or move a peer's delivery.
            return 0
        return await self._reclaim_dead_consumers()

    async def _prompt_reclaim_loop(self) -> None:
        """Dedicated lease-observation cadence, isolated from heavy maintenance."""

        interval_s = self._spec.heartbeat_ttl_ms / 3000
        while not self._should_stop():
            try:
                await self._prompt_reclaim_once()
            except Exception:
                self._spec.logger.exception(
                    "prompt consumer reclaim tick failed on stream %s",
                    self._spec.stream,
                )
            await self._sleep_or_stop(interval_s)

    async def _reclaim_once(self) -> int:
        """Reclaim entries pending too long from any (dead) consumer and retry.

        Entries that have already exhausted their delivery budget are
        dead-lettered first, so they are never claimed or re-dispatched again.
        XAUTOCLAIM still claims an over-cap entry whose dead-letter failed (it is
        still pending), so the ids it reports are skipped rather than dispatched:
        the cap binds even when the graveyard is unwritable.

        The prompt capable-peer path shares this selection lock, while unknown
        peers retain this 15-minute compatibility backstop unchanged.

        Between the proven-dead pass and that backstop runs the lease-expiry pass
        (#2433), which recovers a row whose delivery lease has expired, including
        a LIVE consumer's own, long before the backstop. It is hosted here rather
        than on the faster prompt cadence so it inherits the upgrade-drain gate
        and can reuse the ``over_cap`` set this tick already produced.
        """
        if await self._claims_paused():
            # Runs already skips this entire maintenance tick at its outer loop.
            # Keep the shared entrypoint guarded too so eval and direct callers
            # return before cap scans, dead-letter writes, or any transfer.
            return 0
        selected: list[StreamEntry] = []
        async with self._reclaim_lock:
            over_cap = await self._dead_letter_over_cap()
            selected.extend(await self._select_dead_consumer_entries(over_cap))
            # ``already_selected`` is load-bearing rather than tidiness: nothing is
            # dispatched until every pass has finished selecting, so omitting it
            # lets a concurrency-one consumer claim a second row that then waits
            # behind the first.
            selected.extend(
                await self._select_lease_expired_entries(
                    over_cap, already_selected=len(selected)
                )
            )
            cursor: str = "0-0"
            while not self._should_stop():
                raw = await self._redis.xautoclaim(
                    self._spec.stream,
                    self._spec.group,
                    self._spec.consumer,
                    min_idle_time=self._spec.reclaim_min_idle_ms,
                    start_id=cursor,
                    count=self._spec.read_count,
                )
                cursor = str(raw[0])
                entries = cast("list[StreamEntry]", raw[1])
                for entry_id, fields in entries:
                    if entry_id in self._inflight_ids:
                        continue  # still being processed here; not an orphan
                    if entry_id in over_cap:
                        continue  # budget spent; never dispatch it again
                    if await self._lease_is_live(entry_id):
                        continue  # another delivery owner is still active
                    selected.append((entry_id, fields))
                if cursor in ("0-0", "0"):
                    break
        return await self._dispatch_reclaimed(selected)
