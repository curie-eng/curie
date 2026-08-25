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
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

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

# One stream entry as redis returns it with decode_responses=True.
StreamEntry = tuple[str, dict[str, str]]

# An async per-message handler: (entry_id, fields) -> None.
EntryHandler = Callable[[str, dict[str, str]], Awaitable[None]]


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
    read_count: int
    cap_scan_page: int
    telemetry_source: str
    handler: EntryHandler
    logger: logging.Logger
    dead_letter_log: str
    dead_letter_fail_log: str


class StreamConsumer:
    """Base for a Valkey consumer-group reader.

    Owns the transport-level plumbing (group create, blocking read loop with the
    Timeout->DEBUG / Connection->WARNING split and backoff, ack, stop-aware
    sleep). Subclasses supply their stream config via a :class:`ReadLoopSpec`,
    their per-message handler, and their own maintenance/reclaim loops and
    business logic.
    """

    def __init__(self, redis: StreamBroker, delivery: DeliverySpec | None = None) -> None:
        # The stream broker behind the port (#284). ``redis.asyncio.Redis`` is the
        # one backing today and structurally satisfies ``StreamBroker``; a second
        # broker is a drop-in. Named ``_redis`` still so the sacred consumer.py
        # subclass (which reads ``self._redis`` for XAUTOCLAIM) is untouched.
        self._redis: StreamBroker = redis
        self._stop = asyncio.Event()
        # Entry ids currently being handled by THIS consumer. XAUTOCLAIM would
        # otherwise reclaim our own long-running (still-pending) entries and
        # re-dispatch a duplicate handler that steers the same prompt into its
        # own live turn; skipping these ids prevents that self-reclaim.
        self._inflight_ids: set[str] = set()
        # The reclaim/dead-letter knobs, or None for a base-only reader that
        # exercises just ``_consume`` (no reclaim machinery).
        self._delivery = delivery

    @property
    def _spec(self) -> DeliverySpec:
        assert self._delivery is not None, (
            "reclaim/dead-letter machinery used without a DeliverySpec"
        )
        return self._delivery

    def request_stop(self) -> None:
        self._stop.set()

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
        while not self._stop.is_set():
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
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except TimeoutError:
            pass

    async def _xack(self, stream: str, group: str, entry_id: str) -> None:
        await self._redis.xack(stream, group, entry_id)

    async def _ack(self, entry_id: str) -> None:
        await self._redis.xack(self._spec.stream, self._spec.group, entry_id)

    async def _entry_fields(self, entry_id: str) -> dict[str, str] | None:
        """The original entry's fields, or None if it was already trimmed off the
        stream (then a metadata-only graveyard row is written)."""
        rows = await self._redis.xrange(self._spec.stream, min=entry_id, max=entry_id)
        return dict(rows[0][1]) if rows else None

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
        attributes = {
            "service.name": "curie-worker",
            "source": self._spec.telemetry_source,
        }
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
                await self._ack(entry_id)
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

    async def _reclaim_once(self) -> int:
        """Reclaim entries pending too long from any (dead) consumer and retry.

        Entries that have already exhausted their delivery budget are
        dead-lettered first, so they are never claimed or re-dispatched again.
        XAUTOCLAIM still claims an over-cap entry whose dead-letter failed (it is
        still pending), so the ids it reports are skipped rather than dispatched:
        the cap binds even when the graveyard is unwritable.
        """
        over_cap = await self._dead_letter_over_cap()
        reclaimed = 0
        cursor: str = "0-0"
        while not self._stop.is_set():
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
                reclaimed += 1
                record_metric(
                    "curie.queue.retry",
                    attributes={
                        "service.name": "curie-worker",
                        "source": self._spec.telemetry_source,
                        "retry_class": "redelivery",
                    },
                )
                await self._spec.handler(entry_id, fields)
            if cursor in ("0-0", "0"):
                break
        return reclaimed
