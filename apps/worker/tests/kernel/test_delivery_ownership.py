"""Fenced delivery ownership in the RUNS lane (ADR-0131, #1971).

The regressions R1, R2, R3, R5, R8 from the plan's test strategy, plus the
terminal-ACK fence, driven through the real ``Consumer`` against **real
Valkey**. Valkey is never mocked here and that is load-bearing rather than
stylistic: the fence *is* Valkey semantics -- atomic ``EVAL``, server ``TIME``,
key expiry, ``XPENDING`` ownership and ``XCLAIM ... JUSTID``'s refusal to bump
the delivery counter. A mocked store would assert only that we wrote the Lua we
wrote.

**Time is compressed by CONFIGURING short lease clocks, never by patching a
clock.** ``_LEASE_KNOBS`` below keeps every ratio the ``WorkerConfig``
validators enforce (TTL >= 3x heartbeat, reclaim interval < TTL, runner ceiling
<= budget) while running in seconds. The budget stays at its configurable floor
of 60s because nothing in this file waits the budget out -- only the lease
clocks need to be small. The one thing deliberately NOT compressed or stubbed
is the Valkey server ``TIME`` read: that the deadline comes from the server is
the property under test.

Every test here carries a negative control. Where a guard is asserted to
refuse, the same path is also asserted to succeed once the guard should let it
through -- otherwise the test passes just as happily when the whole path is
dead.

The eval-lane twins of R1-R5 live in ``tests/eval/test_stream.py`` and are
deliberately NOT a shared parametrized body: the two lanes have genuinely
different handler shapes (task-spawned here, inline there) and one body would
hide exactly the difference that matters.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import replace
from typing import Any

from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus, TextDelta
from curie_dispatcher.queue import to_stream_fields
from curie_worker import kernel as kernel_module
from curie_worker.consumer import Consumer
from curie_worker.consumer_liveness import ConsumerLivenessStore, consumer_heartbeat_key
from curie_worker.delivery_lease import DeliveryLeaseStore

from .conftest import _failing_process_event, _pending_rows, _ProcessEventSpy

DONE = SessionStatus.DONE

# The compressed lease clocks. Every ratio the config validators enforce is
# preserved: TTL (1.0) >= 3 * heartbeat (0.3); the harness's reclaim interval
# (0.05) < TTL; the runner ceiling (30) <= the budget (60, its configurable
# floor).
_TTL_S = 1.0
_HEARTBEAT_S = 0.3
_BUDGET_S = 60.0

_LEASE_KNOBS: dict[str, object] = {
    "delivery_budget_s": _BUDGET_S,
    "delivery_lease_ttl_s": _TTL_S,
    "delivery_lease_heartbeat_s": _HEARTBEAT_S,
    "runner_total_timeout_s": 30.0,
}


def _qevent(text: str, *, thread: str = "th-1", event_id: str | None = None) -> QueuedTurn:
    return QueuedTurn(
        event_id=event_id or uuid.uuid4().hex,
        conversation_id=thread,
        author="U1",
        text=text,
        reply_handle=ReplyHandle(kind="slack", channel="C1", placeholder="p-1"),
        received_at="2026-07-05T00:00:00+00:00",
    )


async def _wait_until(pred: Callable[[], bool], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met within timeout")


async def _read_one(h: Any, consumer_name: str) -> tuple[str, dict[str, str]]:
    """Take the next new entry into ``consumer_name``'s PEL, as the read loop does."""
    rows = await h.async_redis.xreadgroup(
        h.config.consumer_group, consumer_name, {h.config.stream: ">"}, count=1
    )
    assert rows, "expected an entry to read"
    entry_id, fields = rows[0][1][0]
    return entry_id, dict(fields)


async def _settle(consumer: Consumer) -> None:
    await asyncio.gather(*list(consumer._inflight), return_exceptions=True)


# --- R1: ownership ------------------------------------------------------------


def test_a_second_replica_is_refused_while_the_first_holds_a_live_lease(
    make_harness,
) -> None:
    """R1, the observed defect directly: two replicas, one entry.

    Red on revert of the live-lease refusal in ``_ACQUIRE_LUA`` (or of the
    ``async with self._delivery_lease(...)`` wrapper in ``Consumer._handle``):
    without the fence BOTH handlers enter and the same turn runs twice on two
    replicas. The refused replica must return WITHOUT acking, so the entry stays
    pending for whoever legitimately holds it.

    The negative control is the refusal; the positive control is the SECOND
    entry, delivered to the very same refused consumer and carried all the way
    to an ACK. Without it this test would pass just as well against a consumer
    whose whole handler was dead.
    """

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            cfg_a = h.config.model_copy(update={"consumer_name": "worker-a"})
            cfg_b = h.config.model_copy(update={"consumer_name": "worker-b"})
            consumer_a = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=cfg_a, leases=store
            )
            consumer_b = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=cfg_b, leases=store
            )
            await consumer_a.ensure_group()
            spy = _ProcessEventSpy(h.kernel)

            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=DONE)]

            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("first", thread="own-a", event_id="own-1")),
            )
            entry_id, fields = await _read_one(h, "worker-a")
            await consumer_a._dispatch(entry_id, fields)
            await _wait_until(lambda: h.runner.turn_active)
            assert await store.is_live(h.config.stream, h.config.consumer_group, entry_id)

            # B takes the PEL row exactly as XAUTOCLAIM does on the reclaim path,
            # so the ONLY thing standing between B and the handler is the lease.
            await h.async_redis.xclaim(
                h.config.stream, h.config.consumer_group, "worker-b", 0, [entry_id]
            )
            await consumer_b._dispatch(entry_id, dict(fields))
            await _settle(consumer_b)

            assert len(spy.leases_for("own-1")) == 1, (
                "the refused replica entered the kernel: both owners ran the same turn"
            )
            assert h.runner.opened == ["first"], "the refused replica opened a second turn"
            # Refused means "return without acking": the entry stays pending.
            assert entry_id in await _pending_rows(h)

            # POSITIVE CONTROL: the same consumer B, an entry it legitimately
            # owns, all the way to the ACK. The refusal above was the fence and
            # not a dead handler.
            h.runner.hold = None
            h.runner.default_script = [Final(text="second answer", status=DONE)]
            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("second", thread="own-b", event_id="own-2")),
            )
            entry_b, fields_b = await _read_one(h, "worker-b")
            await consumer_b._dispatch(entry_b, fields_b)
            await _settle(consumer_b)

            assert len(spy.leases_for("own-2")) == 1
            assert spy.leases_for("own-2")[0] is not None, "the kernel was handed no lease"
            assert entry_b not in await _pending_rows(h), "the granted delivery never acked"

            hold.set()
            await _settle(consumer_a)

    asyncio.run(go())


# --- R2: heartbeat renewal ----------------------------------------------------


def test_a_heartbeating_handler_holds_its_lease_without_burning_a_delivery(
    make_harness,
) -> None:
    """R2. Two independent reverts, both red here.

    Dropping the background heartbeat expires a healthy long turn's lease
    mid-run; dropping ``JUSTID`` from the same-owner ``XCLAIM`` burns one
    delivery of the ADR-0039 budget per heartbeat and dead-letters a healthy turn
    in under a minute. The delivery count stays PEL-backed and is never reset.

    The negative control is the sibling entry leased directly and never
    renewed: it expires inside the SAME window, so "the lease was still live"
    is about the heartbeat and not about a TTL that silently never expires.
    """

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()

            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=DONE)]

            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("long turn", thread="hb-1", event_id="hb-1")),
            )
            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("abandoned", thread="hb-2", event_id="hb-2")),
            )
            renewed_id, renewed_fields = await _read_one(h, h.config.consumer_name)
            abandoned_id, _abandoned_fields = await _read_one(h, h.config.consumer_name)

            # The negative control: same consumer, same window, NO heartbeats.
            await store.acquire(
                h.config.stream,
                h.config.consumer_group,
                abandoned_id,
                consumer=h.config.consumer_name,
            )

            before = (await _pending_rows(h))[renewed_id]
            await consumer._dispatch(renewed_id, renewed_fields)
            await _wait_until(lambda: h.runner.turn_active)

            # ~3x the lease TTL and ~10 heartbeat periods.
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline:
                assert await store.is_live(
                    h.config.stream, h.config.consumer_group, renewed_id
                ), "a healthy in-flight turn lost its lease: the heartbeat is not renewing"
                await asyncio.sleep(0.1)

            assert (
                await store.is_live(h.config.stream, h.config.consumer_group, abandoned_id)
                is False
            ), "the un-renewed sibling never expired, so the lease TTL is not real"

            after = (await _pending_rows(h))[renewed_id]
            assert after == before, (
                "the same-owner XCLAIM must use JUSTID: it reset PEL idle but "
                f"burned {after - before} deliveries of the ADR-0039 budget"
            )

            hold.set()
            await _settle(consumer)
            assert renewed_id not in await _pending_rows(h)

    asyncio.run(go())


# --- R3: dead-owner reclaim ---------------------------------------------------


def test_a_dead_owners_delivery_transfers_only_after_expiry_and_keeps_its_deadline(
    make_harness,
) -> None:
    """R3, force-kill recovery.

    Owner A acquires and dies without releasing (a SIGKILLed process runs no
    ``finally``, which is why the lease is taken through the store directly here
    rather than by cancelling a task -- cancelling would run the graceful
    release path and prove nothing about expiry).

    Red on two reverts: dropping the ``EXISTS`` gate in ``_ACQUIRE_LUA`` lets the
    replacement steal the delivery immediately; turning the ``HSETNX`` on
    ``deadline_ms`` into an ``HSET`` hands the replacement a fresh budget.

    Negative control: refused before expiry. Positive control: granted after it,
    with the generation incremented and the deadline inherited.
    """

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()
            spy = _ProcessEventSpy(h.kernel)
            h.runner.default_script = [Final(text="recovered", status=DONE)]

            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("crashed turn", thread="dead-1", event_id="dead-1")),
            )
            entry_id, fields = await _read_one(h, "dead-owner")
            lease_a = await store.acquire(
                h.config.stream, h.config.consumer_group, entry_id, consumer="dead-owner"
            )
            assert lease_a.generation == 1

            # The replacement takes the PEL row (what XAUTOCLAIM does) but the
            # lease has not expired yet.
            await h.async_redis.xclaim(
                h.config.stream, h.config.consumer_group, h.config.consumer_name, 0, [entry_id]
            )
            await consumer._dispatch(entry_id, dict(fields))
            await _settle(consumer)

            assert spy.leases_for("dead-1") == [], (
                "a replacement ran a delivery whose lease was still live"
            )
            assert h.runner.opened == []
            assert entry_id in await _pending_rows(h)

            await asyncio.sleep(_TTL_S + 0.4)
            assert (
                await store.is_live(h.config.stream, h.config.consumer_group, entry_id) is False
            )

            await consumer._dispatch(entry_id, dict(fields))
            await _settle(consumer)

            leases = spy.leases_for("dead-1")
            assert len(leases) == 1, "the replacement never ran after the lease expired"
            replacement = leases[0]
            assert replacement is not None, "the kernel was handed no lease"
            assert replacement.generation == 2, (
                "the fencing generation did not increment on the change of authority"
            )
            assert replacement.budget.deadline_ms == lease_a.budget.deadline_ms, (
                "the replacement minted a FRESH deadline: reclaim multiplied the budget"
            )
            assert h.runner.opened == ["crashed turn"]

    asyncio.run(go())


# --- R5: rollout / termination ------------------------------------------------


def test_request_stop_stops_the_read_loop_but_never_the_heartbeat(make_harness) -> None:
    """R5, the voluntary-rollout half.

    ``request_stop()`` (what SIGTERM triggers via ``run.py:_stop``) must stop the
    read loop taking NEW entries while the in-flight handler keeps renewing its
    lease and runs to completion.

    Red on reverting the "the heartbeat sleeps with a plain ``asyncio.sleep``,
    never ``self._sleep_or_stop``" decision: a drain would then drop every
    in-flight lease the instant SIGTERM landed -- a silent, high-frequency
    regression no other test in this suite would catch.

    ``reclaim_min_idle_ms`` is parked at its production value so the maintenance
    tick cannot reclaim anything underneath the assertions; this test is about
    the drain, not about reclaim.
    """

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS, reclaim_min_idle_ms=900000) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()
            spy = _ProcessEventSpy(h.kernel)

            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="drained", status=DONE)]

            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("in flight", thread="drain-1", event_id="drain-1")),
            )
            task = asyncio.create_task(consumer.run())
            await _wait_until(lambda: h.runner.turn_active)
            inflight_ids = set(consumer._inflight_ids)
            assert len(inflight_ids) == 1
            entry_id = inflight_ids.pop()

            # The negative control: the SAME entry pending under a second
            # consumer group, leased directly and never renewed. A second group
            # (rather than a second entry on this one) keeps it entirely out of
            # the live read loop's ">" -- otherwise the loop and this test would
            # race for it -- while still being a real PEL row, which ``acquire``
            # requires. It must expire across the very same post-stop window, so
            # "the in-flight lease survived" is about the heartbeat.
            sibling_group = f"{h.config.consumer_group}-sib"
            await h.async_redis.xgroup_create(
                h.config.stream, sibling_group, id="0", mkstream=True
            )
            sibling_rows = await h.async_redis.xreadgroup(
                sibling_group, "sib-owner", {h.config.stream: ">"}, count=1
            )
            sibling_id = sibling_rows[0][1][0][0]
            await store.acquire(
                h.config.stream, sibling_group, sibling_id, consumer="sib-owner"
            )

            consumer.request_stop()
            # The read loop can still be parked inside a blocking XREADGROUP when
            # the stop is set, so give it more than ``read_block_ms`` to unblock
            # and re-check. Without this the entry below would be a coin flip
            # rather than a test.
            await asyncio.sleep(3 * h.config.read_block_ms / 1000)

            # Nothing new is taken after the stop...
            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("too late", thread="drain-3", event_id="drain-3")),
            )
            # ...while the in-flight lease is renewed across ~3 TTLs.
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline:
                assert await store.is_live(
                    h.config.stream, h.config.consumer_group, entry_id
                ), "request_stop() dropped the in-flight lease: the drain cannot finish"
                await asyncio.sleep(0.1)

            assert await store.is_live(h.config.stream, sibling_group, sibling_id) is False, (
                "the un-renewed sibling never expired, so the survival above is vacuous"
            )
            assert "drain-3" not in [eid for eid, _ in spy.calls], (
                "the read loop took a new entry after request_stop()"
            )
            lease = spy.leases_for("drain-1")[0]
            assert lease is not None
            assert not lease.lost.is_set(), "a draining owner was declared lease-lost"

            hold.set()
            await asyncio.wait_for(task, timeout=10.0)

            # The in-flight handler ran to completion and acked.
            assert h.sink.last_text == "drained"
            assert entry_id not in await _pending_rows(h)

    asyncio.run(go())


def test_a_hard_killed_owner_leaves_its_lease_to_expire_before_a_replacement_runs(
    make_harness,
) -> None:
    """R5, the force-kill half.

    A SIGKILL runs no ``finally``, so the lease is neither released nor renewed
    and ownership becomes transferable only by EXPIRY. Modelled by taking the
    lease through the store and abandoning it: cancelling a task would instead
    run the context manager's graceful release, which is the other half of R5
    and proves nothing about expiry.

    Red on revert of the expiry gate: the replacement would run immediately,
    beside a runner the killed pod may still have live.
    """

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()
            spy = _ProcessEventSpy(h.kernel)
            h.runner.default_script = [Final(text="after the kill", status=DONE)]

            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("killed", thread="kill-1", event_id="kill-1")),
            )
            entry_id, fields = await _read_one(h, "killed-worker")
            await store.acquire(
                h.config.stream, h.config.consumer_group, entry_id, consumer="killed-worker"
            )
            await h.async_redis.xclaim(
                h.config.stream, h.config.consumer_group, h.config.consumer_name, 0, [entry_id]
            )

            # Half a TTL in, the replacement is still refused.
            await asyncio.sleep(_TTL_S / 2)
            await consumer._dispatch(entry_id, dict(fields))
            await _settle(consumer)
            assert spy.leases_for("kill-1") == []
            assert h.runner.opened == []

            # Past expiry it is granted -- the positive control that keeps the
            # refusal above from being a dead path.
            await asyncio.sleep(_TTL_S + 0.4)
            await consumer._dispatch(entry_id, dict(fields))
            await _settle(consumer)
            assert len(spy.leases_for("kill-1")) == 1
            assert h.runner.opened == ["killed"]
            assert entry_id not in await _pending_rows(h)

    asyncio.run(go())


# --- R6, the ACK half: a fenced-out owner may not settle ----------------------


def test_an_owner_that_loses_its_lease_refuses_the_terminal_ack(make_harness) -> None:
    """AC4: lease loss prevents a stale owner ACKing.

    The ownership store is made to disagree with the owner mid-turn (the lease
    key is dropped, which is what a peer's post-expiry acquisition looks like
    from this owner's side). The heartbeat then fails CLOSED, the lost lease
    drives the existing bounded interrupt path, and the handler must NOT ack --
    the entry stays pending for whoever now holds the fence.

    Red on reverting ``lease.raise_if_lost()`` before ``self._ack(entry_id)`` in
    ``Consumer._handle``: the fenced-out owner acks the delivery out from under
    its replacement.

    The sibling entry is the positive control: an unmolested delivery on the
    same consumer acks normally, so the refusal above is the fence and not a
    consumer that stopped acking altogether.
    """

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS, reclaim_min_idle_ms=900000) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()
            spy = _ProcessEventSpy(h.kernel)

            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=DONE)]

            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("fenced out", thread="fence-1", event_id="fence-1")),
            )
            entry_id, fields = await _read_one(h, h.config.consumer_name)
            await consumer._dispatch(entry_id, fields)
            await _wait_until(lambda: h.runner.turn_active)

            lease = spy.leases_for("fence-1")[0]
            assert lease is not None, "the kernel was handed no lease"

            # The ownership store no longer agrees this process is the owner.
            await h.async_redis.delete(
                h.config.delivery_lease_key(h.config.stream, h.config.consumer_group, entry_id)
            )
            await _wait_until(lease.lost.is_set, timeout=10.0)
            # Lease loss goes through the EXISTING bounded control path, never a
            # bare task cancel (a cancel skips the runner-side stop and leaves a
            # turn producing effects on a sandbox we no longer own).
            await _wait_until(lambda: h.runner.interrupts >= 1, timeout=10.0)

            hold.set()
            await _settle(consumer)
            assert entry_id in await _pending_rows(h), (
                "a fenced-out owner acked the delivery"
            )

            # POSITIVE CONTROL: an untouched delivery on the same consumer acks.
            h.runner.hold = None
            h.runner.default_script = [Final(text="clean", status=DONE)]
            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("clean", thread="fence-2", event_id="fence-2")),
            )
            clean_id, clean_fields = await _read_one(h, h.config.consumer_name)
            await consumer._dispatch(clean_id, clean_fields)
            await _settle(consumer)
            assert clean_id not in await _pending_rows(h)

    asyncio.run(go())


# --- R8: the budget survives reclaim ------------------------------------------


def test_a_transferred_delivery_inherits_the_remaining_budget_not_a_fresh_one(
    make_harness,
) -> None:
    """R8, scaled statement of AC2: "multiple attempts cannot multiply a
    1,800-second budget into 5,400 seconds."

    Red on reverting the ``HSETNX 'deadline_ms'`` create-if-absent semantics to a
    plain ``HSET``: each transfer would then mint a fresh deadline and three
    attempts would triple the configured budget.

    The negative control is the SECOND entry: a genuinely first delivery does get
    a fresh, strictly later deadline, so the deadline equality asserted for the
    transfers is not the trivial "every deadline is the same" case.
    """

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()
            spy = _ProcessEventSpy(h.kernel)
            h.runner.default_script = [Final(text="ok", status=DONE)]

            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("budgeted", thread="bud-1", event_id="bud-1")),
            )
            entry_id, fields = await _read_one(h, "first-owner")
            lease_1 = await store.acquire(
                h.config.stream, h.config.consumer_group, entry_id, consumer="first-owner"
            )
            first_remaining = lease_1.remaining_s()
            assert 0.0 < first_remaining <= _BUDGET_S

            # Two more changes of authority, each only after expiry.
            await asyncio.sleep(_TTL_S + 0.4)
            lease_2 = await store.acquire(
                h.config.stream, h.config.consumer_group, entry_id, consumer="first-owner"
            )
            await asyncio.sleep(_TTL_S + 0.4)
            lease_3 = await store.acquire(
                h.config.stream, h.config.consumer_group, entry_id, consumer="first-owner"
            )

            assert [lease_1.generation, lease_2.generation, lease_3.generation] == [1, 2, 3]
            assert lease_2.budget.deadline_ms == lease_1.budget.deadline_ms
            assert lease_3.budget.deadline_ms == lease_1.budget.deadline_ms
            # Not ordered against each other: ``remaining_s()`` is "budget left as
            # of right now", and both of these are read on the same line, at the
            # same instant, against the SAME inherited deadline -- so they are two
            # readings of one quantity and agree to within clock noise. Ordering
            # them would be a coin flip. What proves the budget is CONSUMED is the
            # margin against ``first_remaining``, captured two lease TTLs earlier,
            # which the assertions below quantify.
            assert abs(lease_3.remaining_s() - lease_2.remaining_s()) < 0.05
            assert lease_2.remaining_s() < first_remaining
            assert lease_3.remaining_s() < first_remaining
            # Three attempts never exceeded the configured budget.
            assert first_remaining - lease_3.remaining_s() >= 2 * _TTL_S
            assert lease_3.remaining_s() <= _BUDGET_S - 2 * _TTL_S

            # And the kernel receives the inherited budget, not a fresh one: the
            # replacement's delivery is what actually runs the turn.
            await store.release(
                h.config.stream, h.config.consumer_group, entry_id, owner=lease_3.owner
            )
            await h.async_redis.xclaim(
                h.config.stream, h.config.consumer_group, h.config.consumer_name, 0, [entry_id]
            )
            await consumer._dispatch(entry_id, dict(fields))
            await _settle(consumer)

            handed = spy.leases_for("bud-1")
            assert len(handed) == 1
            assert handed[0] is not None, "the kernel was handed no lease"
            assert handed[0].generation == 4
            assert handed[0].budget.deadline_ms == lease_1.budget.deadline_ms
            assert handed[0].remaining_s() < first_remaining - 2 * _TTL_S

            # NEGATIVE CONTROL: a first delivery of a DIFFERENT entry mints its
            # own, strictly later deadline.
            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("fresh", thread="bud-2", event_id="bud-2")),
            )
            other_id, _other_fields = await _read_one(h, "first-owner")
            fresh = await store.acquire(
                h.config.stream, h.config.consumer_group, other_id, consumer="first-owner"
            )
            assert fresh.generation == 1
            assert fresh.budget.deadline_ms > lease_1.budget.deadline_ms

    asyncio.run(go())


def test_an_already_expired_delivery_escalates_once_records_deadline_halted_and_acks(
    make_harness,
    monkeypatch,
) -> None:
    """#2278: recovering an already-expired delivery must escalate once, emit
    one terminal completion, record both turn metrics through the REAL shared
    ``record_metric`` validator, and ACK so the entry does not remain pending.

    Red on omitting ``deadline_halted`` from ``_TURN_OUTCOMES``: ``record_metric``
    raises after settlement, the consumer leaves the entry pending, and a turn
    that already completed stays visibly stuck.

    The sibling entry is the failure-negative: a first delivery with a live
    budget still completes as ``done`` and ACKs, so the halt is the expired
    deadline and not a consumer that stopped settling.
    """

    recorded: list[tuple[str, dict[str, str]]] = []
    real_record_metric = kernel_module.record_metric

    def spy(
        name: str, value: float = 1, *, attributes: dict[str, str] | None = None
    ) -> None:
        recorded.append((name, dict(attributes or {})))
        real_record_metric(name, value, attributes=attributes)

    monkeypatch.setattr(kernel_module, "record_metric", spy)

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS, reclaim_min_idle_ms=900000) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()
            h.runner.default_script = [Final(text="should-not-run", status=DONE)]

            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(
                    _qevent("expired", thread="deadline-1", event_id="deadline-1")
                ),
            )
            entry_id, fields = await _read_one(h, h.config.consumer_name)
            seconds, microseconds = await h.async_redis.time()
            now_ms = int(seconds) * 1000 + int(microseconds) // 1000
            await h.async_redis.hset(
                h.config.delivery_state_key(
                    h.config.stream, h.config.consumer_group, entry_id
                ),
                mapping={"deadline_ms": str(now_ms - 1000)},
            )

            await consumer._dispatch(entry_id, fields)
            await _settle(consumer)

            assert h.runner.opened == [], (
                "an already-expired delivery must not start a runner attempt"
            )
            assert h.sink.last_text is not None
            assert "delivery deadline" in h.sink.last_text.lower()
            assert "human" in h.sink.last_text.lower()
            assert [event.event for event, _route, _best in h.sink.events].count(
                "turn.completed"
            ) == 1
            assert [completion.outcome for completion in h.sink.completions] == [
                "escalated"
            ]
            assert await h.async_redis.exists(h.config.done_key("deadline-1"))
            assert entry_id not in await _pending_rows(h), (
                "an expired delivery that already completed was left pending"
            )

            completed = [
                attrs
                for name, attrs in recorded
                if name == "curie.turn.completed"
            ]
            durations = [
                attrs for name, attrs in recorded if name == "curie.turn.duration"
            ]
            assert [attrs["outcome"] for attrs in completed] == ["deadline_halted"]
            assert [attrs["outcome"] for attrs in durations] == ["deadline_halted"]

            recorded.clear()
            h.runner.default_script = [Final(text="fresh-ok", status=DONE)]
            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(
                    _qevent("fresh", thread="deadline-2", event_id="deadline-2")
                ),
            )
            fresh_id, fresh_fields = await _read_one(h, h.config.consumer_name)
            await consumer._dispatch(fresh_id, fresh_fields)
            await _settle(consumer)

            assert h.sink.last_text == "fresh-ok"
            assert [completion.outcome for completion in h.sink.completions][-1] == (
                "delivered"
            )
            assert fresh_id not in await _pending_rows(h)
            assert [
                attrs["outcome"]
                for name, attrs in recorded
                if name == "curie.turn.completed"
            ] == ["done"]

    asyncio.run(go())



def test_a_deadline_halted_thread_accepts_a_followup_turn(make_harness) -> None:
    """#2425: exceeding the delivery deadline must leave the conversation usable.

    The halted event settles once and does not start a runner. A later event on
    the SAME thread opens a fresh turn and completes. Red on a halt that keeps
    the thread lock, leaves a live turn, or makes the follow-up steer into a
    dead session.

    The negative control is the halt itself: the first event must not reach the
    runner. The independent path is the follow-up's own event id completing.
    """

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS, reclaim_min_idle_ms=900000) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()
            h.runner.default_script = [Final(text="should-not-run", status=DONE)]

            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(
                    _qevent("expired", thread="deadline-follow", event_id="halted-1")
                ),
            )
            entry_id, fields = await _read_one(h, h.config.consumer_name)
            seconds, microseconds = await h.async_redis.time()
            now_ms = int(seconds) * 1000 + int(microseconds) // 1000
            await h.async_redis.hset(
                h.config.delivery_state_key(
                    h.config.stream, h.config.consumer_group, entry_id
                ),
                mapping={"deadline_ms": str(now_ms - 1000)},
            )

            await consumer._dispatch(entry_id, dict(fields))
            await _settle(consumer)

            assert h.runner.opened == [], (
                "an already-expired delivery must not start a runner attempt"
            )
            assert await h.async_redis.exists(h.config.done_key("halted-1"))
            assert entry_id not in await _pending_rows(h)

            h.runner.default_script = [Final(text="follow-up-ok", status=DONE)]
            follow = _qevent("follow", thread="deadline-follow", event_id="follow-1")
            await h.kernel.process_event(follow)

            assert h.runner.opened == ["follow"], h.runner.opened
            assert h.runner.steers == [], (
                "a follow-up after a halted turn must open a new turn, not steer"
            )
            assert h.sink.last_text == "follow-up-ok"
            assert await h.async_redis.exists(h.config.done_key("follow-1"))
            pending = await h.async_redis.scard(h.config.completions_pending_key())
            assert int(pending) == 0, (
                "the follow-up left owed completions on the delivery plane"
            )

    asyncio.run(go())


# --- AC2: the lease-expiry reclaim pass ---------------------------------------
#
# The gap #1532 and #1971 deliberately left open: a LIVE consumer's own pending
# row. A handler that RAISED released its delivery lease and left the entry
# pending, but no prompt path looks at a peer that is not dead, so the row waited
# out the 900 second XAUTOCLAIM backstop.
#
# Every test below pins ``reclaim_min_idle_ms`` at its PRODUCTION 900000,
# deliberately not shortened the way the harness default is. With the backstop
# out of reach, any redelivery observed can only have come from the new pass.
# That is the whole measurement.
#
# PEL idle is driven by ``XCLAIM ... IDLE ... JUSTID`` rather than by sleeping.
# Two properties make that the right lever, both observed against the live
# Valkey (see ``.projects/plans/task-2433-lease-expiry-reclaim.valkey-probe.md``
# and the XCLAIM documentation): ``JUSTID`` returns ids only and does NOT
# increment ``times_delivered``, so arming a boundary does not spend the
# ADR-0039 budget these tests measure; and passing the row's CURRENT owner as
# the consumer name leaves ownership untouched, so only idle moves.

# The threshold under test: one lease TTL, which is both the derived default and
# the floor the config validator allows.
_EXPIRY_IDLE_MS = int(_TTL_S * 1000)

_EXPIRY_KNOBS: dict[str, object] = {
    **_LEASE_KNOBS,
    "lease_expired_idle_ms": _EXPIRY_IDLE_MS,
    # NOT shortened. See the banner above: this is the measurement.
    "reclaim_min_idle_ms": 900000,
}

# The proven-dead peer recipe needs a sustained-absence window short enough to
# sleep through. ``consumer_capability_ttl_ms`` keeps its 1800000 default, which
# the ``_capability_outlives_reclaim_backstop`` validator requires to exceed the
# 900000 backstop above.
_DEAD_PEER_KNOBS: dict[str, object] = {
    **_EXPIRY_KNOBS,
    "dead_consumer_idle_ms": 0,
    "consumer_heartbeat_ttl_ms": 200,
}


async def _arm_pel_idle(h: Any, entry_id: str, *, owner: str, idle_ms: int) -> None:
    """Set one PEL row's idle explicitly, spending no delivery and moving no owner.

    ``justid=True`` is load-bearing: a plain XCLAIM bumps ``times_delivered``
    (probe: 1 -> 2), which is the very number these tests measure. Passing the
    row's current ``owner`` keeps the PEL consumer unchanged, so this moves the
    idle clock and nothing else.
    """
    claimed = await h.async_redis.xclaim(
        h.config.stream,
        h.config.consumer_group,
        owner,
        0,
        [entry_id],
        idle=idle_ms,
        justid=True,
    )
    assert claimed, f"arming idle for {entry_id} claimed nothing"


async def _pel_owners(h: Any) -> dict[str, str]:
    """Every pending entry id -> the consumer that currently owns its PEL row."""
    rows = await h.async_redis.xpending_range(
        h.config.stream, h.config.consumer_group, min="-", max="+", count=50
    )
    return {str(row["message_id"]): str(row["consumer"]) for row in rows}


async def _fenced_failing_consumer(h: Any) -> tuple[Consumer, DeliveryLeaseStore, list[str]]:
    """A fenced ``Consumer`` (leases=store) plus the incident's failing handler.

    Five tests below open with exactly this: a ``DeliveryLeaseStore``, a
    single ``Consumer`` bound to it via ``leases=store``, its group ensured, and
    ``process_event`` replaced with the raiser. Kept here rather than repeated so
    the shape of a fenced consumer cannot quietly drift between call sites; a
    test whose consumer varies (a second replica, ``max_concurrency``, no lease
    store at all) builds its own ``Consumer`` instead of calling this.
    """
    store = DeliveryLeaseStore(h.async_redis, h.config)
    consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store)
    await consumer.ensure_group()
    attempts = _failing_process_event(h)
    return consumer, store, attempts


async def _lease_expired_row(
    h: Any, store: DeliveryLeaseStore, *, event_id: str, owner: str
) -> tuple[str, dict[str, str]]:
    """A pending row with delivery state present and its lease gone.

    Taken through the store rather than by cancelling a real handler, because
    that asymmetry IS the signal the new pass keys on: ``release`` drops only the
    lease key and deliberately leaves the state hash, so a handler that raised
    looks exactly like this. Only ``settle`` removes the state.
    """
    await h.async_redis.xadd(
        h.config.stream,
        to_stream_fields(_qevent(event_id, thread=event_id, event_id=event_id)),
    )
    entry_id, fields = await _read_one(h, owner)
    lease = await store.acquire(
        h.config.stream, h.config.consumer_group, entry_id, consumer=owner
    )
    await store.release(
        h.config.stream, h.config.consumer_group, entry_id, owner=lease.owner
    )
    return entry_id, fields


async def _prove_peer_dead(h: Any, consumer: Consumer, peer: str) -> None:
    """Make ``peer`` proven-dead to ``consumer``'s prompt path (#1961).

    The proof is deliberately expensive: the peer must have advertised the
    liveness protocol, and its alive lease must be absent on TWO observations at
    least one heartbeat TTL apart. Call this BEFORE arming any idle: the priming
    observation is a full ``_reclaim_once`` and would otherwise let the expiry
    pass claim the very row the test is about to set up.
    """
    liveness = ConsumerLivenessStore(h.async_redis)
    await liveness.publish(
        stream=h.config.stream,
        group=h.config.consumer_group,
        consumer=peer,
        heartbeat_ttl_ms=1,
        capability_ttl_ms=h.config.consumer_capability_ttl_ms,
    )
    alive_key = consumer_heartbeat_key(h.config.stream, h.config.consumer_group, peer)
    deadline = time.monotonic() + 5.0
    while await h.async_redis.exists(alive_key):
        if time.monotonic() > deadline:
            raise AssertionError(f"the alive lease for {peer} never expired")
        await asyncio.sleep(0.005)
    assert await consumer._reclaim_once() == 0, (
        "the first absent observation transferred work: the sustained-absence "
        "proof is not being required"
    )
    await asyncio.sleep(h.config.consumer_heartbeat_ttl_ms / 1000 + 0.02)


def _synchronize_first_eligibility_read(
    consumer: Consumer, barrier: asyncio.Barrier
) -> None:
    """Barrier the FIRST ``_lease_is_live`` per consumer; the re-read runs free.

    A plain ``asyncio.gather`` does not force the interleaving the AC4 tests are
    about: one replica can finish its whole pass before the other starts, and the
    test then passes just as happily against a ``min_idle_time=0`` claim.
    Barriering EVERY call instead deadlocks a CORRECT implementation, because the
    mandatory post-claim re-read would wait for a second participant that has
    already left. One-shot is the shape that forces the read and leaves the
    revalidation alone.
    """
    inner = consumer._lease_is_live
    state = {"armed": True}

    async def wrapped(entry_id: str) -> bool:
        result = await inner(entry_id)
        if state["armed"]:
            state["armed"] = False
            await barrier.wait()
        return result

    consumer._lease_is_live = wrapped  # type: ignore[method-assign,assignment]


class _ClaimGate:
    """Delegates to the real client, ordering ONE replica's ``XCLAIM`` against another.

    A rendezvous alone leaves the claim order free, and if the dead pass claims
    first its competitor's positive min-idle correctly rejects it and the test
    passes for the wrong reason. This makes the order an input.
    """

    def __init__(
        self,
        inner: Any,
        *,
        before: asyncio.Event | None = None,
        after: asyncio.Event | None = None,
    ) -> None:
        self._inner = inner
        self._before = before
        self._after = after

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def xclaim(self, *args: Any, **kwargs: Any) -> Any:
        if self._before is not None:
            await asyncio.wait_for(self._before.wait(), timeout=20.0)
        result = await self._inner.xclaim(*args, **kwargs)
        if self._after is not None:
            self._after.set()
        return result


def test_a_failed_turn_is_reclaimed_after_one_lease_not_after_the_backstop(
    make_harness,
) -> None:
    """AC2, the boundary measurement, on a LIVE consumer's own pending row.

    A real turn is driven into ``Consumer._handle``'s ``except`` branch, so the
    row's shape is the incident's: this consumer still owns the PEL entry, its
    delivery state survives, and its lease is gone.

    Red on reverting EB-9/EB-10: nothing is redelivered at all, which is the
    reported symptom. The negative control is the pass BELOW the threshold, which
    keeps the positive from passing against an implementation that reclaims
    everything on sight. ``reclaim_min_idle_ms`` is pinned at 900000 throughout,
    so the redelivery cannot have come from the backstop.

    The exact ``+1`` on ``times_delivered`` is the ADR-0039 half: one logical
    retry charges exactly one delivery of the budget, never two.

    Twin: ``test_the_running_consumer_redelivers_a_failed_turn_without_a_manual_reclaim``
    observes the same behavior through the real ``Consumer.run()`` maintenance
    tick with no reclaim helper called at all.
    """

    async def go() -> None:
        async with make_harness(**_EXPIRY_KNOBS) as h:
            consumer, store, attempts = await _fenced_failing_consumer(h)

            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("expired", thread="exp-1", event_id="exp-1")),
            )
            entry_id, fields = await _read_one(h, h.config.consumer_name)
            before = (await _pending_rows(h))[entry_id]
            await consumer._dispatch(entry_id, fields)
            await _settle(consumer)
            assert attempts == ["exp-1"], "the first delivery never reached the kernel"

            # NEGATIVE CONTROL: under the threshold the pass selects nothing.
            assert await consumer._reclaim_once() == 0
            assert attempts == ["exp-1"]
            assert (await _pending_rows(h))[entry_id] == before

            await _arm_pel_idle(
                h,
                entry_id,
                owner=h.config.consumer_name,
                idle_ms=_EXPIRY_IDLE_MS + 200,
            )
            assert await consumer._reclaim_once() == 1
            await _settle(consumer)

            assert attempts == ["exp-1", "exp-1"], "the retry never reached the kernel"
            assert (await _pending_rows(h))[entry_id] == before + 1, (
                "one logical retry charged more than one delivery of the budget"
            )

    asyncio.run(go())


def test_a_live_lease_is_never_reclaimed_by_the_expiry_pass(make_harness) -> None:
    """AC2's primary fence: a live delivery is never transferred, however idle.

    The peer's lease is kept alive with a bare ``PEXPIRE``, never
    ``store.heartbeat``: a real heartbeat also resets PEL idle with
    ``XCLAIM ... JUSTID`` (probe: JUSTID resets idle and does not bump the
    delivery count), which would take the row out of the scan's IDLE filter
    entirely and let this test pass without ever exercising the lease check.

    Red on dropping the ``_lease_is_live`` guard from the pass. The positive
    control -- the same row, the same pass, with the lease key deleted -- is what
    keeps the refusal from being a dead code path.
    """

    async def go() -> None:
        async with make_harness(**_EXPIRY_KNOBS) as h:
            consumer, store, attempts = await _fenced_failing_consumer(h)

            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("live", thread="live-1", event_id="live-1")),
            )
            entry_id, _fields = await _read_one(h, "live-peer")
            await store.acquire(
                h.config.stream, h.config.consumer_group, entry_id, consumer="live-peer"
            )
            lease_key = h.config.delivery_lease_key(
                h.config.stream, h.config.consumer_group, entry_id
            )
            await h.async_redis.pexpire(lease_key, 60000)
            before = (await _pending_rows(h))[entry_id]
            await _arm_pel_idle(
                h, entry_id, owner="live-peer", idle_ms=_EXPIRY_IDLE_MS + 200
            )

            assert await consumer._reclaim_once() == 0
            assert attempts == []
            assert (await _pending_rows(h))[entry_id] == before, (
                "the row was XCLAIMed, so a delivery was burnt off a live turn"
            )

            # POSITIVE CONTROL: the fence, not a dead pass.
            await h.async_redis.delete(lease_key)
            assert await consumer._reclaim_once() == 1
            await _settle(consumer)
            assert attempts == ["live-1"]
            assert (await _pending_rows(h))[entry_id] == before + 1

    asyncio.run(go())


def test_the_expiry_pass_is_inert_without_a_lease_store(make_harness) -> None:
    """A base-only consumer keeps its pre-ADR-0131 behavior exactly.

    Red on dereferencing ``self._leases`` unconditionally in the new pass, and
    red on keying the pass on the CONFIGURED threshold alone: the config carries
    a threshold here, and the leaseless consumer must still do nothing with it,
    because with no lease store there is no evidence to key on.

    The fenced consumer on the same row is the positive control.
    """

    async def go() -> None:
        async with make_harness(**_EXPIRY_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            leaseless = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await leaseless.ensure_group()
            attempts = _failing_process_event(h)

            entry_id, _fields = await _lease_expired_row(
                h, store, event_id="inert-1", owner="peer-one"
            )
            before = (await _pending_rows(h))[entry_id]
            await _arm_pel_idle(
                h, entry_id, owner="peer-one", idle_ms=_EXPIRY_IDLE_MS + 200
            )

            assert await leaseless._reclaim_once() == 0
            assert attempts == []
            assert (await _pending_rows(h))[entry_id] == before

            # POSITIVE CONTROL: the same row, a fenced consumer.
            fenced = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            assert await fenced._reclaim_once() == 1
            await _settle(fenced)
            assert attempts == ["inert-1"]

    asyncio.run(go())


def test_the_runs_lane_carries_the_configured_lease_expiry_threshold(
    make_harness,
) -> None:
    """AC5, the runs half of the parity seam.

    Both lanes must read the SAME resolver, which is what makes ADR-0131's
    runs/eval parity structural rather than a review promise. Red on hard-coding
    the threshold anywhere in the reclaim machinery instead of carrying it on the
    ``DeliverySpec``.

    Twin: ``tests/eval/test_stream.py``'s
    ``test_the_eval_lane_carries_the_configured_lease_expiry_threshold``.
    """

    async def go() -> None:
        async with make_harness(**_EXPIRY_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )

            assert h.config.lease_expired_idle_ms_value() == _EXPIRY_IDLE_MS
            assert consumer._delivery.lease_expired_idle_ms == _EXPIRY_IDLE_MS

    asyncio.run(go())


# --- AC3: no delivery state means the unchanged backstop still applies --------


def test_an_entry_with_no_delivery_state_stays_on_the_backstop(make_harness) -> None:
    """AC3, the mixed-version rule, as one control pair in a single pass.

    A pre-lease or pre-marker consumer's pending entry carries no evidence a
    lease was ever granted, so "the lease is gone" cannot be told apart from
    "there never was one". Such a row stays on the unchanged 900 second backstop.
    Its lease-aware sibling, armed identically in the same pass, is redelivered.

    The legacy row is asserted to really BE the legacy shape (``peek`` returns
    ``{}``) before anything else, so a change that silently started writing state
    on read would fail here rather than making the pair vacuous.

    Red on dropping the ``has_state`` guard: every legacy consumer's pending
    entry becomes reclaimable by a replica that never had any evidence about it.
    """

    async def go() -> None:
        async with make_harness(**_EXPIRY_KNOBS) as h:
            consumer, store, attempts = await _fenced_failing_consumer(h)

            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(
                    _qevent("legacy", thread="legacy-1", event_id="legacy-1")
                ),
            )
            legacy_id, _legacy_fields = await _read_one(h, "legacy-peer")
            assert (
                await store.peek(h.config.stream, h.config.consumer_group, legacy_id)
                == {}
            ), "the legacy row carries delivery state, so it is not the legacy shape"

            new_id, _new_fields = await _lease_expired_row(
                h, store, event_id="new-1", owner="lease-aware-peer"
            )
            before = await _pending_rows(h)
            await _arm_pel_idle(
                h, legacy_id, owner="legacy-peer", idle_ms=_EXPIRY_IDLE_MS + 200
            )
            await _arm_pel_idle(
                h, new_id, owner="lease-aware-peer", idle_ms=_EXPIRY_IDLE_MS + 200
            )

            assert await consumer._reclaim_once() == 1
            await _settle(consumer)

            assert attempts == ["new-1"]
            after = await _pending_rows(h)
            assert after[legacy_id] == before[legacy_id], (
                "a row with no delivery state was XCLAIMed off the backstop"
            )
            assert after[new_id] == before[new_id] + 1

    asyncio.run(go())


def test_an_unreadable_delivery_state_read_leaves_the_entry_on_the_backstop(
    make_harness, caplog
) -> None:
    """AC3's fail-closed posture, which is the MIRROR of ``_lease_is_live``'s.

    ``_lease_is_live`` fails closed as OWNED and ``_has_delivery_state`` fails
    closed as ABSENT. Both answers refuse the claim: failing open here would let
    one Valkey blip manufacture a duplicate dispatch of a turn somebody may still
    be running.

    Red on ``return True`` in the ``except``, and red on swallowing the failure
    with no WARNING. The positive control restores the read and reclaims the very
    same row in the very same pass shape.
    """

    async def go() -> None:
        async with make_harness(**_EXPIRY_KNOBS) as h:
            consumer, store, attempts = await _fenced_failing_consumer(h)

            entry_id, _fields = await _lease_expired_row(
                h, store, event_id="unreadable-1", owner="peer-one"
            )
            before = (await _pending_rows(h))[entry_id]
            await _arm_pel_idle(
                h, entry_id, owner="peer-one", idle_ms=_EXPIRY_IDLE_MS + 200
            )

            readable = store.has_state

            async def unreadable(stream: str, group: str, entry: str) -> bool:
                raise ConnectionError("injected delivery-state read failure")

            store.has_state = unreadable  # type: ignore[method-assign,assignment]
            with caplog.at_level(logging.WARNING, logger="curie_worker.consumer"):
                assert await consumer._reclaim_once() == 0
            assert attempts == []
            assert (await _pending_rows(h))[entry_id] == before
            assert any(entry_id in message for message in caplog.messages), (
                "an unreadable delivery state was swallowed silently"
            )

            # POSITIVE CONTROL: the read, not a dead pass.
            store.has_state = readable  # type: ignore[method-assign]
            assert await consumer._reclaim_once() == 1
            await _settle(consumer)
            assert attempts == ["unreadable-1"]
            assert (await _pending_rows(h))[entry_id] == before + 1

    asyncio.run(go())


# --- AC4: two transfer paths cannot both move one row -------------------------


def test_two_replicas_reclaiming_the_same_expired_row_dispatch_it_once(
    make_harness,
) -> None:
    """AC4, same-path: two expiry passes, one row, one dispatch.

    ``_reclaim_lock`` is per-process and proves nothing across replicas, and this
    pass takes no ``try_acquire_reclaim`` arbitration lease (that one is keyed per
    DEAD consumer, which does not apply when the owner is alive). The only
    serializer is the claim itself: ``XCLAIM`` with a positive min-idle is an
    atomic compare-and-claim on the row's idle clock, and the winner resets that
    clock to about zero (probe: replica B's ``XCLAIM min-idle=1000`` on a row A
    just claimed returns ``[]``).

    The interleaving is FORCED, because a plain gather lets one replica finish
    before the other starts and would pass against a broken implementation.

    Mutation map: this test goes red, and only red, on ``min_idle_ms=0`` in the
    EXPIRY pass. It says nothing about the proven-dead pass, which is the
    cross-path test's job. The two assertions isolate different failures:
    ``sum(dispatched) == 1`` catches "both claims succeed", and the exact ``+1``
    on ``times_delivered`` catches the silent form where the loser steals the row,
    the winner is fenced out on its next heartbeat, and nobody runs the turn while
    a delivery was still charged.
    """

    async def go() -> None:
        async with make_harness(**_EXPIRY_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            cfg_a = h.config.model_copy(update={"consumer_name": "worker-a"})
            cfg_b = h.config.model_copy(update={"consumer_name": "worker-b"})
            consumer_a = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=cfg_a, leases=store
            )
            consumer_b = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=cfg_b, leases=store
            )
            await consumer_a.ensure_group()
            attempts = _failing_process_event(h)

            entry_id, _fields = await _lease_expired_row(
                h, store, event_id="race-1", owner="departed-peer"
            )
            before = (await _pending_rows(h))[entry_id]
            await _arm_pel_idle(
                h, entry_id, owner="departed-peer", idle_ms=_EXPIRY_IDLE_MS + 200
            )

            barrier = asyncio.Barrier(2)
            _synchronize_first_eligibility_read(consumer_a, barrier)
            _synchronize_first_eligibility_read(consumer_b, barrier)

            async with asyncio.timeout(30):
                dispatched = await asyncio.gather(
                    consumer_a._reclaim_once(), consumer_b._reclaim_once()
                )
            await _settle(consumer_a)
            await _settle(consumer_b)

            assert sum(dispatched) == 1, f"both replicas dispatched: {dispatched}"
            assert attempts == ["race-1"]
            assert (await _pending_rows(h))[entry_id] == before + 1, (
                "two transfer paths both moved one row: a delivery was charged "
                "for a turn nobody ran"
            )

    asyncio.run(go())


def test_a_dead_consumer_pass_does_not_steal_a_row_the_expiry_pass_just_claimed(
    make_harness,
) -> None:
    """AC4, cross-path: the pre-existing zero-idle competitor is the real hazard.

    One row is eligible for BOTH passes: its owner is proven dead to replica A,
    and its lease has expired with its state intact, which is replica B's expiry
    candidate. The interleaving under test is the one that costs two deliveries
    and runs the turn zero times: B claims and acquires the lease, A's stale
    ``XCLAIM`` steals the PEL row anyway, B's next heartbeat fails its PEL-owner
    guard and B is fenced out mid-turn, and A skips dispatch on B's now-live
    lease.

    Both halves of the ordering are inputs, not luck: the one-shot barrier makes
    A's eligibility read provably precede B's claim (which is what makes A's
    observed idle stale), and the claim gate makes B claim first.

    Mutation map: this test goes red, and only red, on the proven-dead pass
    reverting to a batched ``min_idle_time=0``. The last assertion catches the
    silent form: with the counts alone a reviewer cannot tell a clean handoff
    from a fenced-out winner, so the winner is required to settle.
    """

    async def go() -> None:
        async with make_harness(**_DEAD_PEER_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            cfg_a = h.config.model_copy(update={"consumer_name": "worker-a"})
            cfg_b = h.config.model_copy(update={"consumer_name": "worker-b"})
            consumer_a = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=cfg_a, leases=store
            )
            consumer_b = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=cfg_b, leases=store
            )
            await consumer_a.ensure_group()
            spy = _ProcessEventSpy(h.kernel)

            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="one run", status=DONE)]

            entry_id, _fields = await _lease_expired_row(
                h, store, event_id="steal-1", owner="dying-peer"
            )
            before = (await _pending_rows(h))[entry_id]

            await _prove_peer_dead(h, consumer_a, "dying-peer")
            await _arm_pel_idle(
                h, entry_id, owner="dying-peer", idle_ms=_EXPIRY_IDLE_MS + 200
            )

            b_claimed = asyncio.Event()
            consumer_a._redis = _ClaimGate(  # type: ignore[assignment]
                h.async_redis, before=b_claimed
            )
            consumer_b._redis = _ClaimGate(  # type: ignore[assignment]
                h.async_redis, after=b_claimed
            )
            barrier = asyncio.Barrier(2)
            _synchronize_first_eligibility_read(consumer_a, barrier)
            _synchronize_first_eligibility_read(consumer_b, barrier)

            async with asyncio.timeout(60):
                dispatched = await asyncio.gather(
                    consumer_a._reclaim_once(), consumer_b._reclaim_once()
                )

            assert sum(dispatched) == 1, f"both passes moved the row: {dispatched}"
            await _wait_until(lambda: h.runner.turn_active)
            assert (await _pending_rows(h))[entry_id] == before + 1, (
                "the dead pass stole a row the expiry pass had already claimed: "
                "two deliveries charged for one logical retry"
            )

            hold.set()
            await _settle(consumer_a)
            await _settle(consumer_b)

            assert spy.entries_for("steal-1") == 1
            winner = spy.leases_for("steal-1")[0]
            assert winner is not None, "the kernel was handed no lease"
            assert not winner.lost.is_set(), (
                "the winner was fenced out mid-turn by a competing claim"
            )
            assert entry_id not in await _pending_rows(h), (
                "nobody settled the row: the turn was charged and never finished"
            )

    asyncio.run(go())


def test_a_delayed_handoff_loses_the_row_to_a_peer_without_a_double_run(
    make_harness, caplog
) -> None:
    """The ACCEPTED residual, pinned as a bounded number rather than left as prose.

    Selection completes before any dispatch, and the runs lane's ``_dispatch``
    then waits on a semaphore. If that wait exceeds one lease TTL, the row this
    process claimed has been idle long enough to become a legitimate expiry-pass
    candidate for a peer. The peer compare-and-claims it, and this process's late
    ``acquire`` is refused ``not-owner`` and returns WITHOUT acking.

    The cost is exactly one extra charged delivery. Never a double run, never a
    stranded turn. The ``+2`` is deliberately exact: it documents the residual as
    a known number, so a future change that made the cost three goes red here
    rather than being absorbed silently.

    Red on a residual that grows, and red on a late handler that runs anyway
    (which would be the double run this whole design exists to prevent).
    """

    async def go() -> None:
        async with make_harness(**_EXPIRY_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            cfg_a = h.config.model_copy(update={"consumer_name": "worker-a"})
            cfg_b = h.config.model_copy(update={"consumer_name": "worker-b"})
            consumer_a = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=cfg_a, leases=store
            )
            consumer_b = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=cfg_b, leases=store
            )
            await consumer_a.ensure_group()
            attempts = _failing_process_event(h)

            entry_id, _fields = await _lease_expired_row(
                h, store, event_id="delayed-1", owner="departed-peer"
            )
            before = (await _pending_rows(h))[entry_id]
            await _arm_pel_idle(
                h, entry_id, owner="departed-peer", idle_ms=_EXPIRY_IDLE_MS + 200
            )

            released = asyncio.Event()
            dispatch_a = consumer_a._delivery.handler

            async def held(held_id: str, fields: dict[str, str]) -> None:
                await released.wait()
                await dispatch_a(held_id, fields)

            consumer_a._delivery = replace(consumer_a._delivery, handler=held)

            with caplog.at_level(logging.WARNING, logger="curie_worker.consumer"):
                task_a = asyncio.create_task(consumer_a._reclaim_once())
                deadline = time.monotonic() + 10.0
                while (await _pel_owners(h)).get(entry_id) != "worker-a":
                    if time.monotonic() > deadline:
                        raise AssertionError("replica A never claimed the row")
                    await asyncio.sleep(0.01)

                # A's dispatch is still parked, so its claimed row is unleased and
                # ages back past the threshold, exactly as a saturated node's does.
                await _arm_pel_idle(
                    h, entry_id, owner="worker-a", idle_ms=_EXPIRY_IDLE_MS + 200
                )
                assert await consumer_b._reclaim_once() == 1
                await _settle(consumer_b)

                released.set()
                await asyncio.wait_for(task_a, timeout=20.0)
                await _settle(consumer_a)

            assert attempts == ["delayed-1"], (
                f"the delayed handoff ran the turn more than once: {attempts}"
            )
            assert any(
                "refused the delivery lease for entry" in message
                for message in caplog.messages
            ), "the late handler was not refused; it may have run beside the peer"
            assert entry_id in await _pending_rows(h), (
                "the late owner acked a delivery it no longer held"
            )
            assert (await _pending_rows(h))[entry_id] == before + 2, (
                "the accepted residual is exactly one EXTRA charged delivery"
            )

    asyncio.run(go())


# --- The capacity bound: claim only what this process can dispatch now --------


def test_a_saturated_consumer_claims_no_more_expired_rows_than_it_can_dispatch(
    make_harness,
) -> None:
    """The capacity budget, and why an unbounded pass is worse than a slow one.

    Every row this pass claims sits XCLAIMed but UNLEASED until its dispatched
    handler acquires the delivery lease, and ``_dispatch`` waits on the semaphore
    before it gets that far. On a saturated node an unbounded pass therefore parks
    claimed-but-unowned rows for the whole wait, where every other replica reads
    them as expired-lease candidates.

    Neither row being XCLAIMed AT ALL is the assertion that matters: it proves the
    pass stopped before claiming rather than claiming and discarding. The passes
    after the slot frees prove the bound is a DEFERRAL, not a refusal, and that
    the surplus is routed rather than dropped.

    Red on an unbounded expiry pass.
    """

    async def go() -> None:
        async with make_harness(**_EXPIRY_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                max_concurrency=1,
                leases=store,
            )
            await consumer.ensure_group()

            real_process = h.kernel.process_event
            attempts: list[str] = []

            async def routed(qevent: QueuedTurn, *, lease: Any = None) -> None:
                attempts.append(qevent.event_id)
                if qevent.event_id != "held-1":
                    raise RuntimeError("simulated handler failure")
                if lease is None:
                    await real_process(qevent)
                else:
                    await real_process(qevent, lease=lease)

            h.kernel.process_event = routed  # type: ignore[method-assign,assignment]

            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=DONE)]

            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("held", thread="sat-held", event_id="held-1")),
            )
            held_id, held_fields = await _read_one(h, h.config.consumer_name)
            await consumer._dispatch(held_id, held_fields)
            await _wait_until(lambda: h.runner.turn_active)
            assert len(consumer._inflight_ids) == 1, "the only slot is not occupied"

            first_id, _first = await _lease_expired_row(
                h, store, event_id="sat-1", owner="peer-one"
            )
            second_id, _second = await _lease_expired_row(
                h, store, event_id="sat-2", owner="peer-two"
            )
            before = await _pending_rows(h)
            await _arm_pel_idle(
                h, first_id, owner="peer-one", idle_ms=_EXPIRY_IDLE_MS + 200
            )
            await _arm_pel_idle(
                h, second_id, owner="peer-two", idle_ms=_EXPIRY_IDLE_MS + 200
            )

            assert await consumer._reclaim_once() == 0
            after = await _pending_rows(h)
            assert after[first_id] == before[first_id]
            assert after[second_id] == before[second_id]
            owners = await _pel_owners(h)
            assert owners[first_id] == "peer-one", "a row was claimed then discarded"
            assert owners[second_id] == "peer-two", "a row was claimed then discarded"

            hold.set()
            await _settle(consumer)
            assert not consumer._inflight_ids

            # One free slot means exactly one claim, and the deferred row follows
            # on the next tick rather than being dropped.
            assert await consumer._reclaim_once() == 1
            await _settle(consumer)

            # Exactly one row was deferred, and it is re-armed explicitly so the
            # next tick cannot pick the row this one already took: the claim
            # above reset that row's idle, and which of the two was claimed is
            # not this test's business.
            owners_now = await _pel_owners(h)
            deferred = [
                row
                for row in (first_id, second_id)
                if owners_now[row] != h.config.consumer_name
            ]
            assert len(deferred) == 1, f"the bound claimed {2 - len(deferred)} rows"
            await _arm_pel_idle(
                h,
                deferred[0],
                owner=owners_now[deferred[0]],
                idle_ms=_EXPIRY_IDLE_MS + 200,
            )
            assert await consumer._reclaim_once() == 1
            await _settle(consumer)

            final = await _pending_rows(h)
            assert final[first_id] == before[first_id] + 1
            assert final[second_id] == before[second_id] + 1
            assert sorted(attempts) == ["held-1", "sat-1", "sat-2"]

    asyncio.run(go())


def test_the_expiry_pass_leaves_no_room_for_a_row_the_dead_pass_already_took(
    make_harness,
) -> None:
    """``already_selected``: the two passes share one tick's capacity, not two.

    Nothing is dispatched until every pass has finished selecting, so a
    concurrency-one consumer whose proven-dead pass just took a row still sees a
    free slot when the expiry pass runs. Without subtracting what the dead pass
    already selected, it claims a second row that then waits behind the first,
    which is the very parking the budget exists to prevent.

    The dead-pass row carries NO delivery state on purpose, so only the dead pass
    can select it and the two passes cannot be confused for one another.

    Red on dropping ``already_selected=len(selected)`` from the ``_reclaim_once``
    call: both rows are claimed and both are charged.
    """

    async def go() -> None:
        async with make_harness(**_DEAD_PEER_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                max_concurrency=1,
                leases=store,
            )
            await consumer.ensure_group()
            attempts = _failing_process_event(h)

            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(
                    _qevent("dead row", thread="budget-dead", event_id="dead-1")
                ),
            )
            dead_id, _dead_fields = await _read_one(h, "dying-peer")
            expiry_id, _expiry_fields = await _lease_expired_row(
                h, store, event_id="budget-1", owner="other-peer"
            )
            before = await _pending_rows(h)

            await _prove_peer_dead(h, consumer, "dying-peer")
            await _arm_pel_idle(
                h, expiry_id, owner="other-peer", idle_ms=_EXPIRY_IDLE_MS + 200
            )

            assert await consumer._reclaim_once() == 1
            await _settle(consumer)

            assert attempts == ["dead-1"]
            after = await _pending_rows(h)
            assert after[dead_id] == before[dead_id] + 1
            assert after[expiry_id] == before[expiry_id], (
                "the expiry pass claimed a row the tick had no capacity to dispatch"
            )

    asyncio.run(go())


# --- The Fix pin: the same behavior through the real maintenance tick ---------


def test_the_running_consumer_redelivers_a_failed_turn_without_a_manual_reclaim(
    make_harness,
) -> None:
    """The Fix pin (#2433). No reclaim helper is called by this test at all.

    Every other AC2 test invokes ``_reclaim_once()`` by hand, which proves the
    algorithm and nothing about the deployed worker ever scheduling it. This one
    drives the real ``Consumer.run()``, whose maintenance loop calls the pass on
    ``reclaim_interval_s``, and observes AC1's placeholder edit and AC2's
    redelivery together through one running consumer, which is the shape the
    incident had.

    ``reclaim_min_idle_ms`` stays pinned at 900000, so the redelivery still
    cannot have come from the backstop.

    Red on wiring the pass anywhere the maintenance tick does not reach, red on a
    pass that only works when a test calls it directly, and red on a retry that
    charges more than one delivery of the ADR-0039 budget.
    """

    async def go() -> None:
        async with make_harness(**_EXPIRY_KNOBS) as h:
            consumer, store, attempts = await _fenced_failing_consumer(h)

            entry_id = await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(
                    _qevent("lifecycle", thread="life-1", event_id="life-1")
                ),
            )

            task = asyncio.create_task(consumer.run())
            try:
                await _wait_until(lambda: len(attempts) >= 2, timeout=30.0)
            finally:
                consumer.request_stop()
                await asyncio.wait_for(task, timeout=20.0)
                await _settle(consumer)

            delivered = list(attempts)
            rows = await _pending_rows(h)

            assert delivered == ["life-1"] * len(delivered)
            assert len(delivered) >= 2, "the running consumer never redelivered"
            assert entry_id in rows, "the redelivered turn was acked or dead-lettered"
            assert rows[entry_id] == len(delivered), (
                "each redelivery must charge exactly one delivery of the budget"
            )
            assert h.sink.updates == [
                ("C1", "p-1", h.config.turn_not_started_text)
            ] * len(delivered), (
                "AC1 and AC2 must be observable together: the person is told on "
                "every failed delivery while the redelivery is waited out"
            )

    asyncio.run(go())
