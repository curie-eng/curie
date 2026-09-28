"""Dead-letter tests: a permanently-failing entry is bounded by a delivery cap
instead of being reclaimed and re-dispatched forever (#505).

Against the real Valkey stream + consumer group, like ``test_consumer.py``.

Valkey delivery-count semantics, pinned empirically against the live Valkey
(localhost:26379) before these tests were written -- the Implementer MUST build
to the same understanding:

  * ``XADD`` alone puts nothing in the PEL; ``times_delivered`` does not exist
    until an entry is delivered to a consumer.
  * The FIRST ``XREADGROUP ... >`` delivery creates the PEL entry with
    ``times_delivered == 1``. So the counter is a count of deliveries ALREADY
    MADE, not of retries remaining.
  * Every ``XAUTOCLAIM`` (and ``XREADGROUP ... 0`` pending-replay) INCREMENTS
    ``times_delivered`` by exactly 1, and ``XPENDING`` reports the POST-claim
    value. Observed: XREADGROUP -> 1, XAUTOCLAIM -> 2, -> 3, -> 4.

The consequence for the cap: reading ``times_delivered`` via ``XPENDING``
*before* the claim yields the number of deliveries already made, so the cap
check is ``times_delivered >= max_delivery`` -> dead-letter without claiming.
Reading it *after* an ``XAUTOCLAIM`` would already include the current claim's
bump and needs ``>``. These tests are written against the XPENDING-first shape
the plan recommends, but they assert only on the OBSERVABLE contract -- the
number of times the handler is invoked -- so either implementation shape passes
as long as the boundary is right.

The contract these tests pin: ``max_delivery`` is the maximum number of times an
entry may be DELIVERED to a handler. Once an entry has been delivered
``max_delivery`` times and still failed, the next reclaim dead-letters it
instead of re-dispatching.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import uuid
from typing import Any

import curie_worker.consumer as consumer_module
import pytest
from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus
from curie_dispatcher.queue import to_stream_fields
from curie_worker.config import WorkerConfig
from curie_worker.consumer import Consumer
from curie_worker.dead_letter_alert import install_dead_letter_alerting
from curie_worker.delivery_lease import DeliveryLeaseStore
from pydantic import ValidationError

from .conftest import _pending_rows, _updates_for

DONE = SessionStatus.DONE


def _qevent(text: str, *, thread: str, event_id: str) -> QueuedTurn:
    return QueuedTurn(
        event_id=event_id,
        conversation_id=thread,
        author="U1",
        text=text,
        # The #505 shape: a reply endpoint that is durably persisted but dead.
        reply_handle=ReplyHandle(
            kind="slack", channel="C1", placeholder="p-1", endpoint="http://localhost:8155/api/"
        ),
        received_at="2026-07-05T00:00:00+00:00",
    )


def _dead_stream(config: WorkerConfig) -> str:
    """The graveyard name, asked of the config rather than re-derived here.

    Re-deriving ``<stream>:dead`` in the test would mirror the implementation, so
    a change to the derivation would move both sides together and the suite would
    never notice. Calling the same helper the consumer calls means these tests
    fail loudly if the name changes.
    """
    return config.dead_letter_stream_name()


async def _deliver_new(consumer: Consumer, h) -> int:
    """Take initial delivery as THIS consumer and dispatch, exactly as the read
    loop does -- so every delivery corresponds to one handler invocation.

    Driving the read loop by hand (rather than ``consumer.run()``) keeps the
    reclaim count deterministic: ``run()``'s maintenance loop would reclaim on
    its own timer and race the assertions.
    """
    delivered = await h.async_redis.xreadgroup(
        h.config.consumer_group,
        h.config.consumer_name,
        {h.config.stream: ">"},
        count=10,
    )
    n = 0
    for _stream, entries in delivered or []:
        for entry_id, fields in entries:
            await consumer._dispatch(entry_id, fields)
            n += 1
    await asyncio.gather(*list(consumer._inflight))
    return n


async def _reclaim_and_settle(consumer: Consumer) -> None:
    await consumer._reclaim_once()
    await asyncio.gather(*list(consumer._inflight))


async def _pending_ids(h) -> set[str]:
    return set(await _pending_rows(h))


async def _dead_rows(h) -> list[tuple[str, dict[str, str]]]:
    return await h.async_redis.xrange(_dead_stream(h.config))


def test_permanently_failing_entry_is_dead_lettered_at_the_cap_and_group_progresses(
    make_harness,
) -> None:
    """A poison entry is dead-lettered after exactly ``max_delivery`` handler
    invocations, and a healthy entry alongside it still completes (the
    head-of-line proof: the poison never stalls the group).
    """

    async def go() -> None:
        async with make_harness(max_delivery=3, reclaim_min_idle_ms=0) as h:
            h.runner.default_script = [Final(text="answer", status=DONE)]
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            # The controllable-failure seam: process_event raises forever for the
            # poison event (the #505 dead-endpoint shape -- the reply POST to the
            # long-gone CLI stub raises out of the kernel), and delegates to the
            # real kernel for the healthy one.
            real_process = h.kernel.process_event
            calls: dict[str, int] = {"poison": 0, "healthy": 0}

            async def counting(qevent: QueuedTurn, *, lease: Any) -> None:
                calls[qevent.event_id] += 1
                if qevent.event_id == "poison":
                    raise RuntimeError("simulated dead reply endpoint")
                await real_process(qevent, lease=lease)

            h.kernel.process_event = counting  # type: ignore[method-assign,assignment]

            poison = _qevent("poison turn", thread="tdl-poison", event_id="poison")
            healthy = _qevent("healthy turn", thread="tdl-healthy", event_id="healthy")
            poison_id = await h.async_redis.xadd(h.config.stream, to_stream_fields(poison))
            await h.async_redis.xadd(h.config.stream, to_stream_fields(healthy))

            try:
                # Delivery #1 for both: the healthy entry succeeds and acks; the
                # poison entry raises and is left pending.
                assert await _deliver_new(consumer, h) == 2

                # Reclaim until the poison entry leaves the main group's PEL --
                # bounded well above the cap so a missing cap fails the test by
                # assertion, not by hanging forever.
                for _ in range(12):
                    if poison_id not in await _pending_ids(h):
                        break
                    await _reclaim_and_settle(consumer)

                # AC-1: the poison entry is on the graveyard, with the original
                # payload preserved verbatim plus namespaced failure metadata.
                rows = await _dead_rows(h)
                assert len(rows) == 1, f"expected exactly one dead-letter row, got {rows}"
                _dl_id, dl = rows[0]
                assert dl["dl_original_id"] == poison_id
                assert dl["dl_delivery_count"] == "3"
                assert dl["dl_reason"]
                assert dl["dl_dead_lettered_at"]
                # The original entry fields survive so a human can inspect/replay.
                # The frozen #7 wire encoding is a single JSON blob under
                # "payload" (curie_dispatcher.queue.to_stream_fields), not
                # flat top-level fields, so decode it before asserting.
                payload = json.loads(dl["payload"])
                assert payload["event_id"] == "poison"
                assert payload["text"] == "poison turn"

                # AC-1: and it is gone from the main group's PEL, so it is never
                # reclaimed again.
                assert poison_id not in await _pending_ids(h)

                # AC-5 / the exact boundary: the handler ran exactly max_delivery
                # times -- not one fewer (dying early would break crash recovery),
                # not one more (an off-by-one in the cap).
                assert calls["poison"] == 3

                # AC-2, the head-of-line proof and the issue's core severity: the
                # healthy entry enqueued alongside the poison one was processed to
                # completion, and the group's PEL is now empty -- forward progress,
                # no stall.
                assert h.runner.opened == ["healthy turn"]
                assert _updates_for(h, "tdl-healthy")[-1] == "answer"
                assert calls["healthy"] == 1

                # AC1 (#2433): the person on the POISON thread was told on every
                # failed delivery, and the placeholder still carries that line
                # after the dead-letter rather than sitting on the dispatcher's
                # original text forever. The notice is an EDIT of the same
                # placeholder, so no additional channel notification is produced
                # however many times the entry is retried -- which is what makes
                # a faster retry cadence safe for the person.
                #
                # F1 (a distinct terminal notice once the cap is spent) is the
                # named limit of this copy, not a reason to skip the assertion:
                # "if no answer appears here shortly, please send it again" is
                # true both before the cap and at it.
                poison_updates = _updates_for(h, "tdl-poison")
                assert poison_updates == [h.config.turn_not_started_text] * 3, (
                    "the poison thread was left silent, or told a different "
                    f"number of times than it was delivered: {poison_updates}"
                )
                summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
                assert summary["pending"] == 0
            finally:
                await h.async_redis.delete(_dead_stream(h.config))

    asyncio.run(go())


def test_transient_failure_reclaims_and_acks_without_dead_lettering(
    make_harness,
) -> None:
    """The cap must not break ADR-0013 crash recovery: a failure that stops
    failing is retried by reclaim and eventually acked, never dead-lettered.

    Deliberately set at the boundary (``max_delivery=3``, succeeding on delivery
    #3) so this pins the cap from the opposite side to test 1: the LAST permitted
    delivery must still happen. An implementation that reads the delivery count
    AFTER ``XAUTOCLAIM``'s own bump and still compares ``>=`` would kill the entry
    one delivery early and fail here, while test 1 (which catches a cap that runs
    one delivery too long) would still pass. Together they nail the off-by-one.
    """

    async def go() -> None:
        async with make_harness(max_delivery=3, reclaim_min_idle_ms=0) as h:
            h.runner.default_script = [Final(text="recovered", status=DONE)]
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            # Fails the first two deliveries (a worker crash / transient blip),
            # then the real kernel handles it on delivery #3 -- the last delivery
            # max_delivery=3 permits.
            real_process = h.kernel.process_event
            calls = {"n": 0}

            async def flaky(qevent: QueuedTurn, *, lease: Any) -> None:
                calls["n"] += 1
                if calls["n"] <= 2:
                    raise RuntimeError("simulated transient failure")
                await real_process(qevent, lease=lease)

            h.kernel.process_event = flaky  # type: ignore[method-assign,assignment]

            qe = _qevent("orphan", thread="tdl-transient", event_id="transient")
            entry_id = await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))

            try:
                assert await _deliver_new(consumer, h) == 1  # delivery #1: fails

                for _ in range(8):
                    if entry_id not in await _pending_ids(h):
                        break
                    await _reclaim_and_settle(consumer)

                # It got there on the third delivery, by reclaim -- exactly the
                # crash-recovery behavior the module exists for.
                assert calls["n"] == 3
                assert h.runner.opened == ["orphan"]
                assert h.sink.last_text == "recovered"

                # Acked off the main stream...
                summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
                assert summary["pending"] == 0
                # ...and NOT dead-lettered. A cap that fired here would have
                # thrown away a turn that was going to succeed.
                assert await _dead_rows(h) == []
            finally:
                await h.async_redis.delete(_dead_stream(h.config))

    asyncio.run(go())


def test_unparseable_entry_is_dead_lettered_not_silently_dropped(
    make_harness,
) -> None:
    """An entry the frozen queue contract cannot parse goes to the graveyard so
    it is observable, instead of being silently acked into the void.
    """

    async def go() -> None:
        async with make_harness(reclaim_min_idle_ms=0) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            token = uuid.uuid4().hex[:8]
            entry_id = await h.async_redis.xadd(
                h.config.stream, {"garbage": "x", "trace": token}
            )

            try:
                assert await _deliver_new(consumer, h) == 1

                rows = await _dead_rows(h)
                assert len(rows) == 1, f"unparseable entry never reached the graveyard: {rows}"
                _dl_id, dl = rows[0]
                assert dl["dl_original_id"] == entry_id
                assert "unparseable" in dl["dl_reason"]
                assert dl["dl_dead_lettered_at"]
                # The raw fields are kept verbatim -- the whole point is that a
                # human can see WHAT failed to parse.
                assert dl["garbage"] == "x"
                assert dl["trace"] == token

                # Acked off the main group: a poison entry must not be reclaimed
                # forever either.
                summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
                assert summary["pending"] == 0
                # And the kernel never saw it.
                assert h.runner.opened == []
            finally:
                await h.async_redis.delete(_dead_stream(h.config))

    asyncio.run(go())


def test_cap_binds_beyond_the_first_pending_page(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every reclaim candidate is cap-checked, not just the first XPENDING page.

    ``XAUTOCLAIM`` pages through the ENTIRE pending list via its cursor, so a
    cap check that inspects only one ``COUNT _CAP_SCAN_PAGE`` page lets every
    entry past the head of the list be claimed and re-dispatched over its budget
    -- the bounded-delivery guarantee silently not holding at backlog scale.

    The scan's page size is a module constant (1000), not a config knob, so it is
    pinned to 1 here: three poison entries then span three pages and the paging
    is actually exercised. Without this the backlog would fit in one page and the
    test would pass against a single-page scan -- i.e. prove nothing.
    """
    monkeypatch.setattr(consumer_module, "_CAP_SCAN_PAGE", 1)

    async def go() -> None:
        async with make_harness(max_delivery=2, reclaim_min_idle_ms=0) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            calls: dict[str, int] = {"p0": 0, "p1": 0, "p2": 0}

            async def always_fails(qevent: QueuedTurn, *, lease: Any) -> None:
                calls[qevent.event_id] += 1
                raise RuntimeError("simulated dead reply endpoint")

            h.kernel.process_event = always_fails  # type: ignore[method-assign,assignment]

            ids: dict[str, str] = {}
            for name in ("p0", "p1", "p2"):
                qe = _qevent(f"{name} turn", thread=f"tdl-page-{name}", event_id=name)
                ids[name] = await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))

            try:
                # Delivery #1 for all three; every one fails and stays pending.
                assert await _deliver_new(consumer, h) == 3
                # Reclaim #1: all are at 1 < max_delivery=2, so all are claimed
                # and re-dispatched -- delivery #2, the last one permitted.
                await _reclaim_and_settle(consumer)
                assert calls == {"p0": 2, "p1": 2, "p2": 2}

                # Reclaim #2: all three are now over cap. Every one must be
                # dead-lettered; none may be dispatched a third time. Against the
                # single-page scan only p0 (the first page) is cap-checked, while
                # XAUTOCLAIM's own paging still claims and dispatches p1 and p2.
                await _reclaim_and_settle(consumer)

                assert calls == {"p0": 2, "p1": 2, "p2": 2}, (
                    "an entry beyond the first pending page was dispatched over the cap"
                )
                rows = await _dead_rows(h)
                dead_originals = {dl["dl_original_id"] for _dl_id, dl in rows}
                assert dead_originals == set(ids.values()), (
                    f"not every over-cap entry reached the graveyard: {rows}"
                )
                for _dl_id, dl in rows:
                    assert dl["dl_delivery_count"] == "2"
                # ...and the whole group's PEL is drained: no stall left behind.
                summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
                assert summary["pending"] == 0
            finally:
                await h.async_redis.delete(_dead_stream(h.config))

    asyncio.run(go())


def test_a_failing_dead_letter_does_not_kill_the_rest_of_the_tick(
    make_harness,
    caplog: Any,
) -> None:
    """A graveyard XADD that fails for ONE entry is isolated.

    ``_dead_letter_over_cap`` is the first, unguarded await of the maintenance
    tick. If a single entry's XADD raises (an unwritable dead stream, a
    WRONGTYPE key), an unisolated failure propagates out of ``_reclaim_once`` --
    so XAUTOCLAIM never runs and ``reap_orphans`` never runs, on EVERY tick.
    That is #505's own stall class: one bad entry silently killing crash
    recovery for the whole group.
    """

    async def go() -> None:
        async with make_harness(max_delivery=2, reclaim_min_idle_ms=0) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            calls: dict[str, int] = {"bad": 0, "good": 0}

            async def always_fails(qevent: QueuedTurn, *, lease: Any) -> None:
                calls[qevent.event_id] += 1
                raise RuntimeError("simulated dead reply endpoint")

            h.kernel.process_event = always_fails  # type: ignore[method-assign,assignment]

            reaped = {"n": 0}
            real_reap = h.kernel.reap_orphans

            async def counting_reap() -> None:
                reaped["n"] += 1
                await real_reap()

            h.kernel.reap_orphans = counting_reap  # type: ignore[method-assign,assignment]

            # "bad" is XADDed first, so it is the first over-cap id the scan
            # reaches: an unisolated raise there never gets to "good".
            bad_id = await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("bad turn", thread="tdl-iso-bad", event_id="bad")),
            )
            good_id = await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("good turn", thread="tdl-iso-good", event_id="good")),
            )

            # The graveyard is unwritable for exactly one entry.
            dead = _dead_stream(h.config)
            real_xadd = h.async_redis.xadd

            async def failing_xadd(name: str, fields: dict[str, str], **kw: Any) -> Any:
                if name == dead and fields.get("dl_original_id") == bad_id:
                    raise RuntimeError("graveyard unwritable")
                return await real_xadd(name, fields, **kw)

            consumer._redis.xadd = failing_xadd  # type: ignore[method-assign,assignment]

            try:
                assert await _deliver_new(consumer, h) == 2  # delivery #1: both fail
                await _reclaim_and_settle(consumer)  # delivery #2: both fail
                assert calls == {"bad": 2, "good": 2}

                # The tick, exactly as _maintenance_loop runs it. Pre-fix, the
                # raise from bad's XADD escapes _reclaim_once and reap never runs.
                with caplog.at_level(logging.ERROR):
                    await _reclaim_and_settle(consumer)
                    await h.kernel.reap_orphans()

                # The failure is loud, and names the entry and the cause.
                assert any(
                    bad_id in r.getMessage() and r.exc_info for r in caplog.records
                ), "the failed dead-letter was not logged with the entry id"

                # The OTHER entry was still cap-checked and dead-lettered.
                rows = await _dead_rows(h)
                assert [dl["dl_original_id"] for _dl_id, dl in rows] == [good_id]

                # XAUTOCLAIM still ran on that same tick: it claimed the still-
                # pending bad entry, bumping its PEL delivery count to 3...
                assert (await _pending_rows(h)).get(bad_id) == 3
                # ...but the cap still binds -- it was never dispatched again.
                assert calls == {"bad": 2, "good": 2}
                # ...and reap_orphans still ran.
                assert reaped["n"] == 1
            finally:
                consumer._redis.xadd = real_xadd  # type: ignore[method-assign]
                await h.async_redis.delete(_dead_stream(h.config))

    asyncio.run(go())


def test_graveyard_is_bounded_by_dead_letter_maxlen(
    make_harness,
) -> None:
    """The graveyard XADD is capped, so poison at ingest rate cannot OOM Valkey.

    The unparseable path dead-letters per INBOUND entry, so a wire-format drift
    that makes entries unparseable en masse grows the graveyard as fast as the
    dispatcher produces -- on the same Valkey that holds the kernel's per-thread
    locks and side-effect markers. This drives a flood well past a tiny
    ``dead_letter_maxlen`` and asserts the graveyard LENGTH actually stays
    bounded; an implementation that merely reads the knob and still XADDs
    unbounded ends at ``flood`` rows and fails.

    ``approximate=True`` lets Valkey trim on whole-node boundaries, so the bound
    is "at least maxlen", not exactly it -- hence a generous ceiling rather than
    an exact count (observed: 44 rows for this flood). The signal is
    bounded-vs-linear growth, not the precise trim point.
    """

    flood = 400
    # Comfortably above the observed trimmed length yet far below the unbounded
    # one, so the assertion distinguishes the two without pinning Valkey's
    # internal node size.
    ceiling = 200

    async def go() -> None:
        async with make_harness(dead_letter_maxlen=1, reclaim_min_idle_ms=0) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            for i in range(flood):
                # Unparseable: the per-inbound-entry dead-letter path.
                await h.async_redis.xadd(h.config.stream, {"garbage": str(i)})

            try:
                # _deliver_new reads a bounded page, so drain to empty.
                total = 0
                while (n := await _deliver_new(consumer, h)) > 0:
                    total += n
                assert total == flood

                dead_len = await h.async_redis.xlen(_dead_stream(h.config))
                assert dead_len < flood, (
                    f"graveyard grew unbounded: {dead_len} rows from {flood} "
                    "dead-letters -- the maxlen bound is not applied"
                )
                assert dead_len <= ceiling, (
                    f"graveyard length {dead_len} exceeds the expected bound for "
                    f"dead_letter_maxlen=1 after {flood} dead-letters"
                )

                # The bound must not cost the ack: every entry is still off the
                # group, even the ones whose graveyard row was evicted.
                summary = await h.async_redis.xpending(
                    h.config.stream, h.config.consumer_group
                )
                assert summary["pending"] == 0
            finally:
                await h.async_redis.delete(_dead_stream(h.config))

    asyncio.run(go())


def test_unparseable_entry_records_its_real_reclaimed_delivery_count(
    make_harness,
) -> None:
    """An unparseable entry reclaimed after a crash records its ACTUAL count.

    An entry can be delivered, have its worker die before it ever parses, and be
    reclaimed -- so by the time it is dead-lettered the PEL says 2+, not 1. A
    hardcoded 1 fabricates ``dl_delivery_count`` precisely during crash recovery,
    which is when the graveyard's operational evidence matters most. Here the
    first delivery is taken WITHOUT dispatching (the crashed worker), so the
    reclaim that follows is delivery #2.
    """

    async def go() -> None:
        async with make_harness(reclaim_min_idle_ms=0) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            entry_id = await h.async_redis.xadd(h.config.stream, {"garbage": "x"})

            try:
                # Delivery #1, taken and then "crashed": claimed into the PEL but
                # never dispatched, so it was never parsed and never acked.
                await h.async_redis.xreadgroup(
                    h.config.consumer_group,
                    h.config.consumer_name,
                    {h.config.stream: ">"},
                    count=10,
                )
                assert (await _pending_rows(h))[entry_id] == 1

                # Delivery #2, by reclaim: now it parses, fails, and dead-letters.
                await _reclaim_and_settle(consumer)

                rows = await _dead_rows(h)
                assert len(rows) == 1, f"expected one dead-letter row, got {rows}"
                _dl_id, dl = rows[0]
                assert dl["dl_original_id"] == entry_id
                assert "unparseable" in dl["dl_reason"]
                # The real reclaimed count, not a fabricated 1.
                assert dl["dl_delivery_count"] == "2", (
                    "the graveyard fabricated the delivery count of a reclaimed "
                    "unparseable entry instead of reading the PEL"
                )
                summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
                assert summary["pending"] == 0
            finally:
                await h.async_redis.delete(_dead_stream(h.config))

    asyncio.run(go())


def test_unparseable_entry_cannot_clobber_or_forge_its_own_dl_metadata(
    make_harness,
) -> None:
    """The graveyard's ``dl_*`` metadata is the consumer's, never the payload's.

    The unparseable path stores an ARBITRARY malformed field map, so the ``dl_``
    prefix is a convention the payload is under no obligation to respect. An
    entry carrying its own ``dl_original_id`` must not be able to overwrite the
    real one (forging the record) nor have its own value silently destroyed (the
    graveyard then no longer preserves the original verbatim). The metadata wins;
    the original survives under a doubled prefix.
    """

    async def go() -> None:
        async with make_harness(reclaim_min_idle_ms=0) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            entry_id = await h.async_redis.xadd(
                h.config.stream,
                {
                    "dl_original_id": "forged-id",
                    "dl_reason": "forged-reason",
                    "dl_delivery_count": "999",
                    "dl_dead_lettered_at": "forged-time",
                    "garbage": "x",
                },
            )

            try:
                assert await _deliver_new(consumer, h) == 1

                rows = await _dead_rows(h)
                assert len(rows) == 1, f"expected one dead-letter row, got {rows}"
                _dl_id, dl = rows[0]

                # The consumer's metadata wins outright -- the payload cannot forge it.
                assert dl["dl_original_id"] == entry_id
                assert "unparseable" in dl["dl_reason"]
                assert dl["dl_delivery_count"] == "1"
                assert dl["dl_dead_lettered_at"] != "forged-time"

                # ...and the original is still preserved verbatim, recoverable by
                # stripping exactly one "dl_" from the escaped key.
                assert dl["dl_dl_original_id"] == "forged-id"
                assert dl["dl_dl_reason"] == "forged-reason"
                assert dl["dl_dl_delivery_count"] == "999"
                assert dl["dl_dl_dead_lettered_at"] == "forged-time"
                # Non-colliding fields are untouched.
                assert dl["garbage"] == "x"
            finally:
                await h.async_redis.delete(_dead_stream(h.config))

    asyncio.run(go())


def test_over_cap_entry_whose_message_was_trimmed_is_dead_lettered_and_acked(
    make_harness,
) -> None:
    """An id can sit in the PEL while its message is gone from the stream.

    A trim (MAXLEN) or an XDEL removes the message but NOT the pending entry, so
    the over-cap path's XRANGE comes back empty. It must still write a
    metadata-only graveyard row and -- the load-bearing half -- still XACK. Skip
    the XACK and the id stays pending forever: exactly the #505 stall the cap
    exists to end, now permanent because it can never be dispatched again either.

    ``_dead_letter_over_cap`` is driven directly rather than via
    ``_reclaim_once`` so the assertion sees the XACK and nothing else: a later
    XAUTOCLAIM purges PEL ids whose messages were deleted, which would mask a
    missing XACK entirely.
    """

    async def go() -> None:
        async with make_harness(max_delivery=2, reclaim_min_idle_ms=0) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            calls = {"n": 0}

            async def always_fails(qevent: QueuedTurn, *, lease: Any) -> None:
                calls["n"] += 1
                raise RuntimeError("simulated dead reply endpoint")

            h.kernel.process_event = always_fails  # type: ignore[method-assign,assignment]

            qe = _qevent("trimmed turn", thread="tdl-trimmed", event_id="trimmed")
            entry_id = await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))

            try:
                assert await _deliver_new(consumer, h) == 1  # delivery #1: fails
                await _reclaim_and_settle(consumer)  # delivery #2: fails, at cap
                assert calls["n"] == 2

                # The message is trimmed off the stream while its id stays pending.
                assert await h.async_redis.xdel(h.config.stream, entry_id) == 1
                assert await h.async_redis.xrange(h.config.stream, min=entry_id, max=entry_id) == []
                assert entry_id in await _pending_ids(h)

                over_cap = await consumer._dead_letter_over_cap()
                assert over_cap == {entry_id}

                # A metadata-only row: everything the operator can still know.
                rows = await _dead_rows(h)
                assert len(rows) == 1, (
                    f"the trimmed over-cap entry never reached the graveyard: {rows}"
                )
                _dl_id, dl = rows[0]
                assert dl["dl_original_id"] == entry_id
                assert dl["dl_delivery_count"] == "2"
                assert dl["dl_reason"]
                assert dl["dl_dead_lettered_at"]
                # Metadata-ONLY: there is no original payload left to preserve.
                assert set(dl) == {
                    "dl_original_id",
                    "dl_delivery_count",
                    "dl_reason",
                    "dl_dead_lettered_at",
                }

                # The XACK happened: the PEL is drained, so the stall is gone.
                assert entry_id not in await _pending_ids(h)
                summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
                assert summary["pending"] == 0
                # ...and it was never dispatched again on the way out.
                assert calls["n"] == 2
            finally:
                await h.async_redis.delete(_dead_stream(h.config))

    asyncio.run(go())


def test_dead_letter_is_logged_loudly_with_the_operational_facts(
    make_harness,
    caplog: Any,
) -> None:
    """Dead-lettering is silent data loss unless it is loud.

    The row itself is best-effort (the graveyard is MAXLEN-bounded and has no
    consumer group), so the ERROR log is the only durable trace an operator is
    guaranteed to see. It must carry the entry id, the delivery count, the
    reason, and where the row went.
    """

    async def go() -> None:
        async with make_harness(max_delivery=2, reclaim_min_idle_ms=0) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            async def always_fails(qevent: QueuedTurn, *, lease: Any) -> None:
                raise RuntimeError("simulated dead reply endpoint")

            h.kernel.process_event = always_fails  # type: ignore[method-assign,assignment]

            qe = _qevent("poison turn", thread="tdl-log", event_id="poison")
            entry_id = await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))

            try:
                assert await _deliver_new(consumer, h) == 1  # delivery #1: fails
                await _reclaim_and_settle(consumer)  # delivery #2: fails, at cap

                with caplog.at_level(logging.ERROR):
                    await _reclaim_and_settle(consumer)  # dead-letters

                dead = _dead_stream(h.config)
                # ONE record must carry every fact -- an operator reading a single
                # line has to be able to act on it, not correlate across lines.
                # (The per-delivery "left pending" ERRORs are also captured here;
                # none of them is this record.)
                messages = [
                    r.getMessage() for r in caplog.records if r.levelno == logging.ERROR
                ]
                assert any(
                    entry_id in m
                    and "2" in m
                    and "max-delivery-exceeded" in m
                    and dead in m
                    for m in messages
                ), (
                    "no single ERROR log carried the entry id, delivery count, "
                    f"reason and target stream; got: {messages}"
                )
            finally:
                await h.async_redis.delete(_dead_stream(h.config))

    asyncio.run(go())


def test_dead_letter_emits_one_retention_independent_critical_alert(
    make_harness,
    caplog: pytest.LogCaptureFixture,
) -> None:
    source_logger = logging.getLogger("curie_worker.consumer")
    original_handlers = list(source_logger.handlers)
    original_propagate = source_logger.propagate
    for handler in original_handlers:
        source_logger.removeHandler(handler)
    source_logger.propagate = False

    async def go() -> None:
        async with make_harness(max_delivery=2, reclaim_min_idle_ms=0) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            async def always_fails(qevent: QueuedTurn, *, lease: Any) -> None:
                raise RuntimeError("simulated dead reply endpoint")

            h.kernel.process_event = always_fails  # type: ignore[method-assign,assignment]

            qe = _qevent("poison turn", thread="tdl-alert", event_id="poison-alert")
            entry_id = await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))
            dead = _dead_stream(h.config)

            try:
                assert await _deliver_new(consumer, h) == 1
                await _reclaim_and_settle(consumer)
                assert entry_id in await _pending_ids(h)
                await _reclaim_and_settle(consumer)

                assert await h.async_redis.delete(dead) == 1

                alerts = [
                    record
                    for record in caplog.records
                    if record.name == "curie_worker.alerts.dead_letter"
                    and record.levelno == logging.CRITICAL
                ]
                assert len(alerts) == 1, f"expected one dead letter alert, got {alerts}"
                assert alerts[0].getMessage() == (
                    f"event=curie.dead_letter entry_id={entry_id} delivery_count=2 "
                    f"reason=max-delivery-exceeded dead_stream={dead}"
                )

                caplog.clear()
                source_logger.error("unrelated consumer error for entry %s", entry_id)
                source_logger.error(
                    "dead-lettered entry %s after %d deliveries reason=%s -> %s",
                    entry_id,
                    2,
                    "max-delivery-exceeded",
                    dead,
                )
                source_logger.error(
                    "dead-lettered entry %s after %d deliveries (reason=%s) -> %s",
                    entry_id,
                    2,
                    "max-delivery-exceeded",
                )
                assert not [
                    record
                    for record in caplog.records
                    if record.name == "curie_worker.alerts.dead_letter"
                    and record.levelno == logging.CRITICAL
                ]

                caplog.clear()
                child_logger = logging.getLogger("curie_worker.consumer.child")
                child_logger.error(
                    "dead-lettered entry %s after %d deliveries (reason=%s) -> %s",
                    entry_id,
                    2,
                    "max-delivery-exceeded",
                    dead,
                )
                assert not [
                    record
                    for record in caplog.records
                    if record.name == "curie_worker.alerts.dead_letter"
                    and record.levelno == logging.CRITICAL
                ]

                caplog.clear()
                source_logger.critical(
                    "dead-lettered entry %s after %d deliveries (reason=%s) -> %s",
                    entry_id,
                    2,
                    "max-delivery-exceeded",
                    dead,
                )
                assert not [
                    record
                    for record in caplog.records
                    if record.name == "curie_worker.alerts.dead_letter"
                    and record.levelno == logging.CRITICAL
                ]

                caplog.clear()
                source_logger.error(
                    "dead-lettered entry %s after %d deliveries (reason=%s) -> %s",
                    entry_id,
                    "two",
                    "max-delivery-exceeded",
                    dead,
                )
                assert not [
                    record
                    for record in caplog.records
                    if record.name == "curie_worker.alerts.dead_letter"
                    and record.levelno == logging.CRITICAL
                ]
            finally:
                await h.async_redis.delete(dead)

    caplog.clear()
    try:
        install_dead_letter_alerting()
        install_dead_letter_alerting()
        with caplog.at_level(logging.ERROR):
            asyncio.run(go())
    finally:
        for handler in list(source_logger.handlers):
            source_logger.removeHandler(handler)
        for handler in original_handlers:
            source_logger.addHandler(handler)
        source_logger.propagate = original_propagate


def test_max_delivery_below_the_floor_is_rejected() -> None:
    """``max_delivery=1`` dead-letters every ordinary worker crash on its first
    reclaim -- ADR-0013 crash recovery relies on a reclaim actually retrying.

    The ``ge=2`` floor is the only thing standing between a config typo and a
    consumer that throws away every recoverable turn, so pin it: a future edit
    dropping the constraint fails here instead of shipping.
    """
    with pytest.raises(ValidationError):
        WorkerConfig(stream="curie:runs", max_delivery=1)
    with pytest.raises(ValidationError):
        WorkerConfig(stream="curie:runs", max_delivery=0)

    # The floor itself, and the default above it, stay valid.
    assert WorkerConfig(stream="curie:runs", max_delivery=2).max_delivery == 2
    assert WorkerConfig(stream="curie:runs").max_delivery >= 2


def test_dead_letter_stream_equal_to_source_stream_is_rejected() -> None:
    """A graveyard pointed at its own source stream is a config error, not a
    runtime surprise.

    ``_dead_letter`` XADDs the payload to the target and only then XACKs it, so
    target == source re-queues every failure onto the stream it came from: a
    valid failure is re-consumed under a fresh id, and an unparseable one spins a
    hot loop -- the exact permanent stall the delivery cap exists to prevent.
    Rejecting at construction means an operator learns at boot, not mid-incident.
    """
    with pytest.raises(ValidationError, match="must not equal CURIE_STREAM"):
        WorkerConfig(stream="curie:runs", dead_letter_stream="curie:runs")

    # The derived default can never collide, so it stays valid...
    assert WorkerConfig(stream="curie:runs").dead_letter_stream == ""
    # ...and so does any genuinely distinct override.
    assert (
        WorkerConfig(
            stream="curie:runs", dead_letter_stream="curie:runs:dead"
        ).dead_letter_stream
        == "curie:runs:dead"
    )


# --- ADR-0131: a live lease is checked BEFORE cap evaluation ------------------


def test_a_live_lease_holds_off_the_cap_and_releasing_it_dead_letters_normally(
    make_harness,
) -> None:
    """R4. "A live lease is checked before cap evaluation so a healthy long turn
    cannot be dead-lettered" (ADR-0131), directly.

    The entry is driven to ``times_delivered >= max_delivery`` -- the exact state
    that dead-letters today -- while a peer replica holds a LIVE lease on it. The
    cap scan must skip it. ``_inflight_ids`` is asserted empty throughout, so the
    pre-existing same-process guard cannot be what saved it: only the new
    cross-replica lease check can.

    Red on reverting the ``is_live`` guard inserted between the ``_inflight_ids``
    skip and the ``delivered = int(row["times_delivered"])`` read in
    ``_dead_letter_over_cap``: a healthy long turn on a peer is dead-lettered
    mid-run, its reply is lost, and the graveyard reports it as poison.

    The positive control is the second pass: once the lease is released the very
    same entry dead-letters normally, so the skip above is the fence and not a
    cap that stopped working. The cap itself, its ``>=`` boundary and its
    PEL-backed count are untouched -- weakening ``max_delivery`` is the #505
    total-stall regression, not a simplification.
    """
    # Imported inside the test on purpose: ``delivery_lease`` does not exist
    # until this ticket lands, and a module-level import would fail COLLECTION
    # for this whole file, turning every unrelated test in it red.
    from curie_worker.delivery_lease import DeliveryLeaseStore

    async def go() -> None:
        async with make_harness(
            max_delivery=2,
            reclaim_min_idle_ms=0,
            delivery_budget_s=60.0,
            delivery_lease_ttl_s=1.0,
            delivery_lease_heartbeat_s=0.3,
            runner_total_timeout_s=30.0,
        ) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()

            qe = _qevent("healthy long turn", thread="tcap", event_id="cap-live")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))

            # Delivery #1 (XREADGROUP) then #2 (XCLAIM) -> at the cap of 2, and
            # pending under a PEER, so this consumer's own in-flight guard is not
            # in play.
            rows = await h.async_redis.xreadgroup(
                h.config.consumer_group, "peer-worker", {h.config.stream: ">"}, count=1
            )
            entry_id = rows[0][1][0][0]
            await h.async_redis.xclaim(
                h.config.stream, h.config.consumer_group, "peer-worker", 0, [entry_id]
            )
            assert (await _pending_rows(h))[entry_id] >= h.config.max_delivery

            lease = await store.acquire(
                h.config.stream, h.config.consumer_group, entry_id, consumer="peer-worker"
            )
            assert await store.is_live(h.config.stream, h.config.consumer_group, entry_id)
            assert consumer._inflight_ids == set(), (
                "the in-flight guard would mask the lease guard under test"
            )

            over_cap = await consumer._dead_letter_over_cap()
            assert over_cap == set(), "a healthy long turn was judged against the cap"
            assert await _dead_rows(h) == [], "a healthy long turn was dead-lettered"
            assert entry_id in await _pending_ids(h)

            # POSITIVE CONTROL: with the lease gone the same entry, at the same
            # count, dead-letters on the next pass.
            assert (
                await store.release(
                    h.config.stream, h.config.consumer_group, entry_id, owner=lease.owner,
                    resume_event_id=None,
                )
                is True
            )
            over_cap = await consumer._dead_letter_over_cap()
            assert over_cap == {entry_id}
            rows = await _dead_rows(h)
            assert len(rows) == 1
            assert rows[0][1]["dl_original_id"] == entry_id
            assert entry_id not in await _pending_ids(h)

    asyncio.run(go())


# --- ADR-0131: dead-letter is FENCED (the four terminal verbs) ----------------
#
# ADR-0131: a fenced-out owner "may not ACK, dead-letter, clear an outbox
# record, or emit a terminal result." ``_dead_letter`` settles a delivery three
# ways at once -- it XADDs the graveyard row, XACKs the entry off the group, and
# then deletes the lease and delivery-state keys -- so an unfenced one lets a
# stale process ack somebody else's delivery AND delete the current owner's
# keys. ``_dead_letter_refusal`` is the precondition that stops it, and it asks
# a DIFFERENT question of each of its two callers; the tests below pin both
# branches with a positive control each, so a fence that refused everything
# (equally broken: a graveyard that never fills is #505's stall) fails too.


async def _park_over_cap(h, consumer_name: str) -> str:
    """One entry, pending under ``consumer_name`` at ``times_delivered >= cap``.

    The exact state ``_dead_letter_over_cap`` hands to ``_dead_letter``: read
    once by XREADGROUP (delivery #1) then claimed once (delivery #2).
    """
    qe = _qevent("long turn", thread="tfence", event_id=f"fence-{uuid.uuid4().hex[:8]}")
    await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))
    rows = await h.async_redis.xreadgroup(
        h.config.consumer_group, consumer_name, {h.config.stream: ">"}, count=1
    )
    entry_id = rows[0][1][0][0]
    await h.async_redis.xclaim(
        h.config.stream, h.config.consumer_group, consumer_name, 0, [entry_id]
    )
    assert (await _pending_rows(h))[entry_id] >= h.config.max_delivery
    return entry_id


def _lease_keys(h, entry_id: str) -> tuple[str, str]:
    return (
        h.config.delivery_lease_key(h.config.stream, h.config.consumer_group, entry_id),
        h.config.delivery_state_key(h.config.stream, h.config.consumer_group, entry_id),
    )


# Short lease clocks so a real fence loss is observed in-test without touching a
# clock. ``delivery_budget_s`` floors at 60.0 and a validator rejects
# ``runner_total_timeout_s`` above it, so the runner ceiling comes down too --
# that is the whole compression lever here. ``max_delivery`` is NOT touched:
# weakening the ADR-0039 cap is the #505 regression, not a test convenience.
_FENCE_CONFIG: dict[str, object] = {
    "max_delivery": 2,
    "reclaim_min_idle_ms": 0,
    "delivery_budget_s": 60.0,
    "runner_total_timeout_s": 30.0,
    "delivery_lease_ttl_s": 1.0,
    "delivery_lease_heartbeat_s": 0.3,
}


def test_the_maintenance_scan_refuses_to_dead_letter_once_another_owner_acquires(
    make_harness,
) -> None:
    """A lease taken in the window between the cap read and the settle wins.

    ``_dead_letter_over_cap``'s own pre-cap ``_lease_is_live`` check cannot cover
    this: it is a READ, and a new owner may acquire in the window between it and
    the terminal writes. So ``_dead_letter`` is called directly here, exactly as
    the scan calls it, with a live peer lease in place -- the only way to reach
    the second line of defense the review asked for.

    All three settle effects must be absent: nothing on ``<stream>:dead``, the
    entry NOT acked (still pending, delivery count untouched), and neither the
    lease key nor the delivery-state key deleted out from under the live owner.

    Red on removing the ``_dead_letter_refusal`` precondition from
    ``_dead_letter``: a stale scan acks the new owner's delivery off the group,
    writes it to the graveyard as poison, and deletes the keys the owner is
    heartbeating -- ADR-0131's split brain, from the maintenance side.
    """
    from curie_worker.delivery_lease import DeliveryLeaseStore

    async def go() -> None:
        async with make_harness(**_FENCE_CONFIG) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()

            try:
                entry_id = await _park_over_cap(h, "peer-worker")
                lease_key, state_key = _lease_keys(h, entry_id)

                await store.acquire(
                    h.config.stream, h.config.consumer_group, entry_id, consumer="peer-worker"
                )
                # The maintenance branch, not the handler branch: this process
                # holds no lease of its own, so the refusal must come from the
                # re-read of somebody ELSE's liveness.
                assert consumer._held_leases == {}

                before = (await _pending_rows(h))[entry_id]
                await consumer._dead_letter(
                    entry_id,
                    await consumer._entry_fields(entry_id),
                    reason="max-delivery-exceeded",
                    delivery_count=before,
                )

                assert await _dead_rows(h) == [], "settled a delivery another owner holds"
                assert entry_id in await _pending_ids(h), "acked another owner's delivery"
                assert (await _pending_rows(h))[entry_id] == before
                assert await h.async_redis.exists(lease_key) == 1, "deleted the owner's lease"
                assert await h.async_redis.exists(state_key) == 1, "deleted the owner's state"
            finally:
                await h.async_redis.delete(_dead_stream(h.config))

    asyncio.run(go())


def test_the_maintenance_scan_dead_letters_normally_when_nobody_owns_the_entry(
    make_harness,
) -> None:
    """POSITIVE CONTROL for the two maintenance refusals above and below.

    Byte-for-byte the same setup and the same direct ``_dead_letter`` call, with
    the single difference that no live lease exists and the store is readable.
    Without this test a fence that refused unconditionally -- or a
    ``_dead_letter`` broken outright -- would leave the refusal tests green
    while the graveyard silently stopped filling, which is #505's stall wearing
    the fence's clothes.

    All three settle effects must happen: the graveyard row, the ACK, and the
    ``settle`` that removes BOTH the lease and delivery-state keys (ADR-0131:
    delivery state is "removed after terminal acknowledgement or dead-letter
    settlement").

    Red on a fence that refuses when ``_lease_is_live`` says False, or on
    dropping the ``self._leases.settle(...)`` call after the ACK.
    """
    from curie_worker.delivery_lease import DeliveryLeaseStore

    async def go() -> None:
        async with make_harness(**_FENCE_CONFIG) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()

            try:
                entry_id = await _park_over_cap(h, "peer-worker")
                lease_key, state_key = _lease_keys(h, entry_id)

                # Acquired and RELEASED, so the delivery-state key exists (release
                # deliberately preserves it) and only ``settle`` can remove it.
                lease = await store.acquire(
                    h.config.stream, h.config.consumer_group, entry_id, consumer="peer-worker"
                )
                assert (
                    await store.release(
                        h.config.stream, h.config.consumer_group, entry_id, owner=lease.owner,
                        resume_event_id=None,
                    )
                    is True
                )
                live = await store.is_live(h.config.stream, h.config.consumer_group, entry_id)
                assert live is False
                assert await h.async_redis.exists(state_key) == 1
                assert consumer._held_leases == {}

                delivered = (await _pending_rows(h))[entry_id]
                await consumer._dead_letter(
                    entry_id,
                    await consumer._entry_fields(entry_id),
                    reason="max-delivery-exceeded",
                    delivery_count=delivered,
                )

                rows = await _dead_rows(h)
                assert len(rows) == 1, f"an unowned over-cap entry was not dead-lettered: {rows}"
                assert rows[0][1]["dl_original_id"] == entry_id
                assert rows[0][1]["dl_delivery_count"] == str(delivered)
                assert entry_id not in await _pending_ids(h), "dead-lettered but never acked"
                assert await h.async_redis.exists(lease_key) == 0
                assert await h.async_redis.exists(state_key) == 0, "delivery state outlived settle"
            finally:
                await h.async_redis.delete(_dead_stream(h.config))

    asyncio.run(go())


# --- The settle asymmetry: RAISE at the dead-letter, SWALLOW after the ACK ----
#
# ``_settle_delivery`` (raises through) and ``_settle_delivery_best_effort``
# (catches and logs a WARNING) are deliberately two methods on the shared base,
# and WHICH ONE a call site picks is the entire point of the split. The two
# tests below drive a real ``DeliveryLeaseStore`` whose ``settle`` cannot
# complete and assert OPPOSITE outcomes at the two call sites, so swapping
# either one for the other variant turns exactly one of them red.


def test_a_failed_settle_makes_the_dead_letter_report_failure_not_a_clean_row(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The dead-letter settle RAISES THROUGH: a half-settled entry says so.

    ``_dead_letter`` finishes with ``_settle_delivery`` -- the raising variant --
    from inside the try that marks the span, records ``curie.queue.dead_letter``
    and re-raises. A settle that fails there leaves the entry HALF settled: the
    graveyard row is written and the entry is acked off the group, but its
    delivery state is still on the box. That must be reported as a failed
    dead-letter; swallowing it would hand the caller, the alerting and the
    metric a dead-letter that looks clean while the state key silently outlives
    it to its retention TTL.

    Red on flipping that call to ``_settle_delivery_best_effort``, and equally
    red on re-introducing a subclass override of ``_settle_delivery`` that
    swallows -- the shape both lanes carried before the base consolidated them,
    and the reason the raising variant is the overridable base method rather
    than a private one.

    The store is real and so is the failure: ONLY ``settle`` is replaced, so the
    acquire/release that created the delivery-state key ran for real, and that
    key surviving is the observable proof the settle genuinely did not happen
    (an injection that silently no-opped would leave this test green on nothing).

    ``record_metric`` is wrapped rather than replaced so the real declaration
    check still runs on every point this path emits.
    """
    from curie_worker import stream_consumer as stream_consumer_module
    from curie_worker.delivery_lease import DeliveryLeaseStore

    points: list[tuple[str, dict[str, str]]] = []
    real_record_metric = stream_consumer_module.record_metric

    def recording_metric(
        name: str, value: float = 1, *, attributes: dict[str, str] | None = None
    ) -> None:
        points.append((name, dict(attributes or {})))
        real_record_metric(name, value, attributes=attributes)

    def outcomes(name: str) -> list[str]:
        return [attrs.get("outcome", "") for point, attrs in points if point == name]

    async def go() -> None:
        async with make_harness(**_FENCE_CONFIG) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()

            try:
                entry_id = await _park_over_cap(h, "peer-worker")
                lease_key, state_key = _lease_keys(h, entry_id)

                # Acquired and RELEASED, exactly as the positive control above:
                # the fence permits, the state key exists, and only ``settle``
                # can remove it.
                lease = await store.acquire(
                    h.config.stream, h.config.consumer_group, entry_id, consumer="peer-worker"
                )
                await store.release(
                    h.config.stream, h.config.consumer_group, entry_id, owner=lease.owner,
                    resume_event_id=None,
                )
                assert await h.async_redis.exists(state_key) == 1
                assert consumer._held_leases == {}

                boom = RuntimeError("valkey refused the terminal settle")

                async def exploding_settle(*_args: object, **_kwargs: object) -> None:
                    raise boom

                monkeypatch.setattr(store, "settle", exploding_settle)
                monkeypatch.setattr(
                    stream_consumer_module, "record_metric", recording_metric
                )

                delivered = (await _pending_rows(h))[entry_id]
                with pytest.raises(RuntimeError) as raised:
                    await consumer._dead_letter(
                        entry_id,
                        await consumer._entry_fields(entry_id),
                        reason="max-delivery-exceeded",
                        delivery_count=delivered,
                    )
                assert raised.value is boom, (
                    "the settle failure was swallowed and something else raised"
                )

                # Reported as a FAILED dead-letter, not a successful one.
                assert outcomes("curie.queue.dead_letter") == ["failure"], (
                    "a dead-letter whose settle failed was recorded as clean: "
                    f"{points}"
                )
                assert outcomes("curie.queue.settle") == ["pending"]

                # ...and the two writes that precede the settle DID happen, in
                # that order: the graveyard row first (the neighbouring ordering
                # test pins XADD-before-XACK against a graveyard that cannot be
                # written), then the ACK. A failed settle must not be mistaken
                # for a refused dead-letter, which writes neither.
                rows = await _dead_rows(h)
                assert len(rows) == 1, f"the graveyard row was rolled back: {rows}"
                assert rows[0][1]["dl_original_id"] == entry_id
                assert entry_id not in await _pending_ids(h), "the entry was never acked"
                assert await h.async_redis.exists(lease_key) == 0, (
                    "the lease key vanished, so the injected settle did not fire"
                )
                assert await h.async_redis.exists(state_key) == 1, (
                    "the delivery state was removed, so the settle under test ran"
                )
            finally:
                await h.async_redis.delete(_dead_stream(h.config))

    asyncio.run(go())


def test_a_failed_settle_after_the_ack_is_warned_about_and_the_turn_still_succeeds(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The MIRROR of the test above, at the post-ack call site: it SWALLOWS.

    ``Consumer._handle`` settles after its terminal ACK, and by then the entry
    is already off the group: raising there would turn a turn that ran, replied
    and acked into a logged handler failure, and there is nothing left to retry
    -- the state key's own retention is the backstop. So this site calls
    ``_settle_delivery_best_effort``, which catches and logs one WARNING.

    Same real store, same injected ``settle`` failure, opposite assertion: the
    handler task completes normally, the turn is acked and answered, and the
    only trace is a WARNING carrying the entry id and the exception. Red on
    flipping this call site to the raising ``_settle_delivery`` (the handler
    task ends in an exception and the success metric is never recorded), and red
    on downgrading the log to silence.
    """
    from curie_worker.delivery_lease import DeliveryLeaseStore

    async def go() -> None:
        async with make_harness(**_FENCE_CONFIG) as h:
            h.runner.default_script = [Final(text="answered", status=DONE)]
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()

            async def exploding_settle(*_args: object, **_kwargs: object) -> None:
                raise RuntimeError("valkey refused the terminal settle")

            monkeypatch.setattr(store, "settle", exploding_settle)

            try:
                qe = _qevent(
                    "post-ack settle",
                    thread="tdl-postack",
                    event_id=f"postack-{uuid.uuid4().hex[:8]}",
                )
                await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))
                rows = await h.async_redis.xreadgroup(
                    h.config.consumer_group,
                    h.config.consumer_name,
                    {h.config.stream: ">"},
                    count=1,
                )
                entry_id, fields = rows[0][1][0]

                with caplog.at_level(logging.DEBUG, logger="curie_worker.consumer"):
                    await consumer._dispatch(entry_id, dict(fields))
                    handlers = list(consumer._inflight)
                    results = await asyncio.gather(*handlers, return_exceptions=True)

                assert [r for r in results if isinstance(r, BaseException)] == [], (
                    "a settle failure AFTER the ack escaped the handler and "
                    "failed a turn that had already succeeded"
                )
                assert h.runner.opened == ["post-ack settle"]
                assert h.sink.last_text == "answered"
                assert entry_id not in await _pending_ids(h), "the turn never acked"
                assert await _dead_rows(h) == [], "a settled turn reached the graveyard"

                settle_logs = [
                    r for r in caplog.records if "settling the delivery state" in r.getMessage()
                ]
                assert len(settle_logs) == 1, (
                    f"expected exactly one settle-failure log, got: "
                    f"{[r.getMessage() for r in settle_logs]}"
                )
                record = settle_logs[0]
                assert record.levelno == logging.WARNING, (
                    f"the post-ack settle failure was logged at {record.levelname}, not WARNING"
                )
                assert entry_id in record.getMessage()
                assert record.exc_info is not None, (
                    "the swallowed exception was not attached, so an operator "
                    "cannot tell WHY the state key was left behind"
                )
            finally:
                await h.async_redis.delete(_dead_stream(h.config))

    asyncio.run(go())


def test_the_settle_split_survives_on_the_consumer_subclass_itself(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The same asymmetry asserted where the REGRESSION would land: on ``Consumer``.

    The two tests above drive the two call sites. This one drives the two
    METHODS, on a real ``Consumer``, because the likeliest regression is not a
    changed call site at all: the method deleted from ``consumer.py`` when the
    base consolidated the three copies had exactly the name ``_settle_delivery``
    and exactly the SWALLOWING shape. ``Consumer`` now inherits three coupled
    things -- ``_settle_delivery`` (raises), ``_settle_delivery_best_effort``
    (swallows) and ``_dead_letter``, which deliberately calls the raising one --
    so a future "make settle best-effort everywhere" cleanup that re-adds
    ``Consumer._settle_delivery`` would not only change the post-ack site: it
    would silently convert the INHERITED dead-letter's settle into a swallow.
    A half-settled dead-letter then reports as a clean one -- the CRITICAL alert
    fires as normal, no failure metric is recorded, and the lease and
    delivery-state keys leak to their retention TTL with no signal anywhere.

    Deliberately Valkey-free: a fake store is enough to make ``settle`` fail, and
    an override anywhere must be caught on every box, not only where the compose
    stack happens to be up.
    """

    class _RefusingLeaseStore:
        """A lease store whose terminal settle cannot complete.

        Records its calls so a variant that never reached the store at all --
        the other way this test could go quietly vacuous -- is visible.
        """

        def __init__(self) -> None:
            self.calls: list[tuple[str, str, str]] = []

        async def settle(self, stream: str, group: str, entry_id: str) -> None:
            self.calls.append((stream, group, entry_id))
            raise RuntimeError("valkey refused the terminal settle")

    config = WorkerConfig(
        stream="curie:runs", consumer_group="curie-workers", consumer_name="worker-1"
    )
    store = _RefusingLeaseStore()
    consumer = Consumer(
        redis=None,  # type: ignore[arg-type]
        kernel=None,  # type: ignore[arg-type]
        config=config,
        leases=store,  # type: ignore[arg-type]
    )
    entry_id = "1700000000000-0"

    # The dead-letter's variant: raises through, so ``_dead_letter``'s own
    # handler can mark the span, record the failure metric and re-raise.
    with pytest.raises(RuntimeError, match="refused the terminal settle"):
        asyncio.run(consumer._settle_delivery(entry_id))

    # The post-ack variant: same failure, same store, returns normally.
    with caplog.at_level(logging.DEBUG, logger="curie_worker.consumer"):
        asyncio.run(consumer._settle_delivery_best_effort(entry_id))

    assert store.calls == [
        (config.stream, config.consumer_group, entry_id),
        (config.stream, config.consumer_group, entry_id),
    ], f"a variant never reached the lease store at all: {store.calls}"

    settle_logs = [r for r in caplog.records if "settling the delivery state" in r.getMessage()]
    assert len(settle_logs) == 1, (
        "the swallowed settle failure must leave exactly one trace; got "
        f"{[r.getMessage() for r in settle_logs]}"
    )
    record = settle_logs[0]
    assert record.name == "curie_worker.consumer", (
        f"the settle warning came from {record.name}; the dead-letter alerting and "
        "the operator's log filters both key off this logger"
    )
    assert record.levelno == logging.WARNING, (
        f"the swallowed settle failure was logged at {record.levelname}, not WARNING"
    )
    assert entry_id in record.getMessage()
    assert record.exc_info is not None, (
        "the swallowed exception was not attached, so an operator cannot tell WHY "
        "the delivery state was left behind"
    )


def test_the_maintenance_scan_fails_closed_when_lease_liveness_is_unreadable(
    make_harness,
) -> None:
    """An unreadable ownership store reads as OWNED, never as permission.

    ADR-0131: "loss of the ownership store cannot be treated as permission to
    continue producing effects." The store here is a REAL ``DeliveryLeaseStore``
    over a real client pointed at a dead port -- no mock -- so ``is_live``
    raises for real and ``_lease_is_live``'s ``except`` arm is what answers.

    The cost of failing closed is one skipped maintenance pass: the entry stays
    pending and is dead-lettered on a later tick once Valkey answers again. The
    cost of failing OPEN is a healthy turn's delivery settled underneath its
    owner during a blip -- unrecoverable.

    Red on removing the ``_dead_letter_refusal`` precondition, and equally red
    on flipping ``_lease_is_live``'s exception handler to ``return False``. The
    positive control is the test above: a READABLE store with no live lease
    proceeds, so this is fail-closed and not a fence that refuses everything.
    """
    from curie_worker.delivery_lease import DeliveryLeaseStore
    from redis.asyncio import Redis as AsyncRedis
    from redis.exceptions import ConnectionError as RedisConnectionError

    async def go() -> None:
        async with make_harness(**_FENCE_CONFIG) as h:
            # A real client that cannot reach anything: port 1 is never a Valkey.
            unreachable = AsyncRedis(
                host="127.0.0.1", port=1, socket_connect_timeout=0.5, decode_responses=True
            )
            store = DeliveryLeaseStore(unreachable, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()

            try:
                entry_id = await _park_over_cap(h, "peer-worker")
                with pytest.raises(RedisConnectionError):
                    await store.is_live(h.config.stream, h.config.consumer_group, entry_id)

                before = (await _pending_rows(h))[entry_id]
                await consumer._dead_letter(
                    entry_id,
                    await consumer._entry_fields(entry_id),
                    reason="max-delivery-exceeded",
                    delivery_count=before,
                )

                assert await _dead_rows(h) == [], "settled a delivery it could not vouch for"
                assert entry_id in await _pending_ids(h)
                assert (await _pending_rows(h))[entry_id] == before
            finally:
                with contextlib.suppress(Exception):
                    await unreachable.aclose()
                await h.async_redis.delete(_dead_stream(h.config))

    asyncio.run(go())


def test_a_handler_that_lost_its_lease_mid_flight_may_not_dead_letter(
    make_harness,
) -> None:
    """The handler branch: our OWN lease going lost is authority we no longer have.

    The unparseable path reaches the graveyard from INSIDE ``_delivery_lease``,
    so its lease is registered in ``_held_leases`` and the fence asks
    ``held.lost`` rather than re-reading liveness. The loss here is genuine: a
    peer XCLAIMs the PEL row, the real heartbeat's next renewal comes back
    ``not-owner``, and ``lost`` is set by production code -- no clock is patched
    and nothing is mocked.

    A lost lease means the entry now belongs to whoever holds the fence, so the
    refusal must leave it PENDING for them and write nothing.

    Red on removing the ``_dead_letter_refusal`` precondition from
    ``_dead_letter``: the graveyard row is written (the later
    ``held.raise_if_lost()`` guard fires only AFTER the XADD, by design), so a
    fenced-out owner poisons a delivery the new owner is still working.
    """
    from curie_worker.delivery_lease import DeliveryLeaseStore

    async def go() -> None:
        async with make_harness(**_FENCE_CONFIG) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()

            try:
                fields = {"garbage": "x", "trace": uuid.uuid4().hex[:8]}
                await h.async_redis.xadd(h.config.stream, fields)
                rows = await h.async_redis.xreadgroup(
                    h.config.consumer_group,
                    h.config.consumer_name,
                    {h.config.stream: ">"},
                    count=1,
                )
                entry_id = rows[0][1][0][0]

                async with consumer._delivery_lease(entry_id, fields) as lease:
                    assert lease is not None
                    assert consumer._held_leases.get(entry_id) is lease, (
                        "the handler branch of the fence is not the one under test"
                    )

                    # The real fence move: a peer takes the PEL row, so our next
                    # renewal is refused and the heartbeat marks us lost.
                    await h.async_redis.xclaim(
                        h.config.stream, h.config.consumer_group, "peer-worker", 0, [entry_id]
                    )
                    await asyncio.wait_for(lease.lost.wait(), timeout=10.0)

                    await consumer._dead_letter(
                        entry_id, fields, reason="unparseable", delivery_count=1
                    )

                    assert await _dead_rows(h) == [], "a fenced-out owner wrote a graveyard row"
                    assert entry_id in await _pending_ids(h), (
                        "a fenced-out owner acked the current owner's delivery"
                    )
            finally:
                await h.async_redis.delete(_dead_stream(h.config))

    asyncio.run(go())


def test_a_handler_holding_a_healthy_lease_dead_letters_normally(
    make_harness,
) -> None:
    """POSITIVE CONTROL for the handler-branch refusal above.

    Same path, same registered lease, same ``_dead_letter`` call -- the only
    difference is that nothing fenced us out, so the unparseable entry must
    reach the graveyard and be acked exactly as it always has. Without this,
    a ``_dead_letter_refusal`` that returned a reason for every held lease
    would leave the refusal test green while silently disabling the unparseable
    path, and poison would go back to being reclaimed forever.

    Red on a handler-branch fence that refuses a lease whose ``lost`` is clear.
    """
    from curie_worker.delivery_lease import DeliveryLeaseStore

    async def go() -> None:
        async with make_harness(**_FENCE_CONFIG) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()

            try:
                token = uuid.uuid4().hex[:8]
                fields = {"garbage": "x", "trace": token}
                await h.async_redis.xadd(h.config.stream, fields)
                rows = await h.async_redis.xreadgroup(
                    h.config.consumer_group,
                    h.config.consumer_name,
                    {h.config.stream: ">"},
                    count=1,
                )
                entry_id = rows[0][1][0][0]
                lease_key, state_key = _lease_keys(h, entry_id)

                async with consumer._delivery_lease(entry_id, fields) as lease:
                    assert lease is not None
                    assert not lease.lost.is_set()
                    assert consumer._held_leases.get(entry_id) is lease

                    await consumer._dead_letter(
                        entry_id, fields, reason="unparseable", delivery_count=1
                    )

                    dead = await _dead_rows(h)
                    assert len(dead) == 1, f"a healthy owner could not dead-letter: {dead}"
                    assert dead[0][1]["dl_original_id"] == entry_id
                    assert dead[0][1]["dl_reason"] == "unparseable"
                    assert dead[0][1]["trace"] == token
                    assert entry_id not in await _pending_ids(h)
                    assert await h.async_redis.exists(lease_key) == 0
                    assert await h.async_redis.exists(state_key) == 0
            finally:
                await h.async_redis.delete(_dead_stream(h.config))

    asyncio.run(go())


def test_the_graveyard_write_precedes_the_ack_so_a_failed_write_leaves_it_pending(
    make_harness,
) -> None:
    """XADD before XACK, observed through a graveyard that cannot be written.

    The fence added above is a gate in FRONT of this sequence, not a reordering
    of it, so the ordering rationale still has to hold: a crash (or a failing
    write) between the two must cost a DUPLICATE graveyard row on the retry,
    never a lost entry. The target is turned into a plain string key, so the
    real XADD raises WRONGTYPE against real Valkey -- no patched client.

    With XADD first, the failure happens before the ACK: the entry is still
    pending and will be re-reclaimed and re-dead-lettered. Reverse the two and
    this test goes red the interesting way -- the entry is acked off the group
    with no graveyard row anywhere, which is the silent-loss failure mode the
    ordering exists to avoid. The delivery-state key surviving is the same
    proof from the settle side: nothing terminal ran.
    """
    from curie_worker.delivery_lease import DeliveryLeaseStore
    from redis.exceptions import ResponseError

    async def go() -> None:
        async with make_harness(**_FENCE_CONFIG) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()
            dead = _dead_stream(h.config)

            try:
                entry_id = await _park_over_cap(h, "peer-worker")
                _lease_key, state_key = _lease_keys(h, entry_id)
                # Give the delivery a state key to watch, then leave it unowned so
                # the fence permits: what stops this dead-letter is the write.
                lease = await store.acquire(
                    h.config.stream, h.config.consumer_group, entry_id, consumer="peer-worker"
                )
                await store.release(
                    h.config.stream, h.config.consumer_group, entry_id, owner=lease.owner,
                    resume_event_id=None,
                )

                await h.async_redis.set(dead, "not-a-stream")
                before = (await _pending_rows(h))[entry_id]

                with pytest.raises(ResponseError):
                    await consumer._dead_letter(
                        entry_id,
                        await consumer._entry_fields(entry_id),
                        reason="max-delivery-exceeded",
                        delivery_count=before,
                    )

                assert entry_id in await _pending_ids(h), (
                    "the entry was acked without a graveyard row: XACK ran before XADD"
                )
                assert (await _pending_rows(h))[entry_id] == before
                assert await h.async_redis.exists(state_key) == 1
            finally:
                await h.async_redis.delete(dead)

    asyncio.run(go())
