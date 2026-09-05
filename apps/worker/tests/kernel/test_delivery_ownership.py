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
import time
import uuid
from collections.abc import Callable
from typing import Any

from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus, TextDelta
from curie_dispatcher.queue import to_stream_fields
from curie_worker import kernel as kernel_module
from curie_worker.consumer import Consumer
from curie_worker.delivery_lease import DeliveryLeaseStore

from .conftest import _pending_rows, _ProcessEventSpy

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
