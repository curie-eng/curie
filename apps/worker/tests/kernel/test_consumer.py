"""Consumer tests: end-to-end stream consumption and crash-recovery reclaim,
against the real Valkey stream + consumer group.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Callable

import redis.exceptions
from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus, TextDelta
from curie_dispatcher.queue import to_stream_fields
from curie_worker import consumer as consumer_module
from curie_worker import kernel as kernel_module
from curie_worker.consumer import (
    THREAD_RESET_INFLIGHT_SET,
    THREAD_RESET_SET,
    Consumer,
)

DONE = SessionStatus.DONE


def _qevent(text: str, *, thread: str = "th-1", event_id: str | None = None) -> QueuedTurn:
    return QueuedTurn(
        event_id=event_id or uuid.uuid4().hex,
        conversation_id=thread,
        author="U1",
        text=text,
        reply_handle=ReplyHandle(kind="slack", channel="C1", placeholder="p-1"),
        received_at="2026-07-05T00:00:00+00:00",
    )


def _thread_key(thread: str) -> str:
    return f"slack:C1:{thread}"


async def _wait_until(pred: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met within timeout")


def test_consumes_stream_entry_end_to_end_and_acks(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [TextDelta(text="hi "), Final(text="answer", status=DONE)]
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()

            qe = _qevent("hello", thread="tc1", event_id="c1")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))

            task = asyncio.create_task(consumer.run())
            await _wait_until(lambda: h.sink.last_text == "answer")
            consumer.request_stop()
            await task

            assert h.runner.opened == ["hello"]
            summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            assert summary["pending"] == 0  # the entry was acked

    asyncio.run(go())


def test_reclaim_skips_this_consumers_own_inflight_entry(make_harness) -> None:
    async def go() -> None:
        async with make_harness(reclaim_min_idle_ms=0) as h:
            # A turn that hangs, so its stream entry stays pending (unacked, in
            # flight) while streaming.
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=DONE)]
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()

            qe = _qevent("hello", thread="ti1", event_id="i1")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))
            task = asyncio.create_task(consumer.run())
            await _wait_until(lambda: h.runner.turn_active)

            # A reclaim pass while the turn is still in flight must NOT re-dispatch
            # our own entry (which would steer the same prompt into its own turn).
            reclaimed = await consumer._reclaim_once()
            assert reclaimed == 0
            assert h.runner.opened == ["hello"]  # no duplicate turn

            hold.set()
            await _wait_until(lambda: h.sink.last_text == "done")
            consumer.request_stop()
            await task

    asyncio.run(go())


def test_dispatch_applies_backpressure_at_capacity(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            # A hanging turn holds the single capacity slot; the next dispatch must
            # block (backpressure) rather than claim the entry into a local queue.
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="w")]
            h.runner.tail = [Final(text="done", status=DONE)]
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, max_concurrency=1
            )
            await consumer.ensure_group()

            first = to_stream_fields(_qevent("a", thread="ta", event_id="a"))
            await consumer._dispatch("1-0", first)
            await _wait_until(lambda: h.runner.turn_active)  # slot taken, turn hanging

            second_fields = to_stream_fields(_qevent("b", thread="tb", event_id="b"))
            second = asyncio.create_task(consumer._dispatch("2-0", second_fields))
            await asyncio.sleep(0.1)
            assert not second.done()  # blocked: capacity is full

            hold.set()  # first turn finishes, frees the slot
            await second  # second dispatch now proceeds
            await asyncio.gather(*list(consumer._inflight))

    asyncio.run(go())


def test_ensure_group_does_not_replay_preexisting_backlog(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            # A stale entry already on the stream BEFORE the group is created (a
            # persistent Valkey carrying a backlog from a prior deploy). Creating
            # the group at "$" must skip it; creating at "0" would storm it.
            stale = _qevent("stale", thread="tb1", event_id="b1")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(stale))

            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()

            # An entry produced AFTER the group exists must still be delivered.
            fresh = _qevent("fresh", thread="tb2", event_id="b2")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(fresh))
            h.runner.default_script = [Final(text="answer", status=DONE)]

            task = asyncio.create_task(consumer.run())
            await _wait_until(lambda: h.sink.last_text == "answer")
            consumer.request_stop()
            await task

            # Only the post-group entry ran; the stale backlog was never opened.
            assert h.runner.opened == ["fresh"]

    asyncio.run(go())


def test_read_loop_survives_transient_redis_timeout(make_harness, caplog) -> None:
    async def go() -> None:
        async with make_harness() as h:
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()

            # The first blocking read raises a transient redis TimeoutError (the
            # routine idle case) and the second a ConnectionError (a real fault).
            # The loop must survive both and process the next read; an unguarded
            # read would kill the worker. The two are logged at different levels:
            # an idle timeout is DEBUG (not log-worthy every idle interval), a
            # connection blip stays WARNING.
            real = h.async_redis.xreadgroup
            calls = {"n": 0}

            async def flaky(*args: object, **kwargs: object) -> object:
                calls["n"] += 1
                if calls["n"] == 1:
                    raise redis.exceptions.TimeoutError("simulated blocking-read timeout")
                if calls["n"] == 2:
                    raise redis.exceptions.ConnectionError("simulated connection blip")
                return await real(*args, **kwargs)

            consumer._redis.xreadgroup = flaky  # type: ignore[method-assign,assignment]

            h.runner.default_script = [Final(text="answer", status=DONE)]
            qe = _qevent("hello", thread="tt1", event_id="t1")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))

            with caplog.at_level(logging.DEBUG, logger="curie_worker.consumer"):
                task = asyncio.create_task(consumer.run())
                await _wait_until(lambda: h.sink.last_text == "answer")
                consumer.request_stop()
                await task

            assert calls["n"] >= 3  # it retried after both injected faults
            assert h.runner.opened == ["hello"]

            recs = [r for r in caplog.records if r.name == "curie_worker.consumer"]
            timeout_recs = [r for r in recs if "simulated blocking-read timeout" in r.getMessage()]
            conn_recs = [r for r in recs if "simulated connection blip" in r.getMessage()]
            assert timeout_recs and all(r.levelno == logging.DEBUG for r in timeout_recs)
            assert conn_recs and all(r.levelno == logging.WARNING for r in conn_recs)

    asyncio.run(go())


def test_reclaims_and_reprocesses_a_dead_consumers_pending_entry(make_harness) -> None:
    async def go() -> None:
        async with make_harness(reclaim_min_idle_ms=0) as h:
            h.runner.default_script = [Final(text="recovered", status=DONE)]
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()

            qe = _qevent("orphan", thread="tr1", event_id="r1")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))

            # A different (now "dead") consumer takes delivery but never acks,
            # leaving the entry pending — the crash mid-run case.
            dead = await h.async_redis.xreadgroup(
                h.config.consumer_group, "dead-consumer", {h.config.stream: ">"}, count=1
            )
            assert dead

            # Our consumer reclaims the pending entry and reprocesses it.
            reclaimed = await consumer._reclaim_once()
            assert reclaimed == 1
            await _wait_until(lambda: h.sink.last_text == "recovered")
            await asyncio.gather(*list(consumer._inflight))

            assert h.runner.opened == ["orphan"]
            summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            assert summary["pending"] == 0  # reclaimed entry acked after reprocessing

    asyncio.run(go())


def test_maintenance_tick_drains_pending_thread_reset_requests(make_harness) -> None:
    """#713: an operator-requested thread reset (the API SADDs the thread_key
    into THREAD_RESET_SET) is picked up and applied by the maintenance tick,
    releasing that thread's sandbox and popping it off the pending set."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tDrain"))
            thread_key = _thread_key("tDrain")
            assert h.substrate.lookup(thread_key) is not None

            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await h.async_redis.sadd(THREAD_RESET_SET, thread_key)

            await consumer._drain_thread_reset_requests()

            assert h.substrate.lookup(thread_key) is None  # released
            assert await h.async_redis.scard(THREAD_RESET_SET) == 0  # popped, not left behind
            # #812: the in-progress marker is cleared only after the release
            # actually lands, so a successful drain leaves nothing pending.
            assert not await h.async_redis.sismember(THREAD_RESET_INFLIGHT_SET, thread_key)

    asyncio.run(go())


def test_maintenance_tick_thread_reset_failed_release_keeps_the_signal_pending(
    make_harness, caplog
) -> None:
    """#812 (was #806 incomplete): the observable "reset outstanding" signal --
    membership of THREAD_RESET_SET UNION THREAD_RESET_INFLIGHT_SET, which the
    API's ``is_pending`` and therefore the CLI's ``reset-thread`` poll read --
    must NOT flip to done when ``release_thread`` raises or times out. The drain
    SPOPs the request (the atomic claim) and moves it into the in-progress set,
    clearing it only on SUCCESS; a failed release leaves the key in the
    in-progress set, so the signal stays pending and the CLI reports the reset as
    unconfirmed rather than a false ``released: true`` (scenario B)."""

    async def go() -> None:
        async with make_harness() as h:
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await h.async_redis.sadd(THREAD_RESET_SET, "tFailRelease")

            async def boom_release(thread_key: str) -> bool:
                raise RuntimeError("injected release failure")

            h.kernel.release_thread = boom_release  # type: ignore[method-assign]

            with caplog.at_level(logging.ERROR):
                await consumer._drain_thread_reset_requests()

            # Claimed off the request set (atomic SPOP: no second replica double-releases)...
            assert await h.async_redis.scard(THREAD_RESET_SET) == 0
            # ...but the in-progress marker is STILL set: the pending signal the
            # CLI gates on stays True, so it never reports a false success.
            assert await h.async_redis.sismember(THREAD_RESET_INFLIGHT_SET, "tFailRelease")
            assert any("tFailRelease" in r.getMessage() for r in caplog.records)

    asyncio.run(go())


def test_maintenance_tick_thread_reset_is_not_stalled_by_a_wedged_runner(
    make_harness, monkeypatch
) -> None:
    """#739: the maintenance tick runs stream reclaim, orphan reaping, and the
    thread-reset drain in one pass, so a reset whose runner never answers the
    courtesy interrupt would otherwise block all three for the runner client's
    full 600s request timeout -- and the request is already SPOPped off the set,
    so it is lost rather than retried on the next tick. The drain must therefore
    finish in seconds and the sandbox must actually be gone afterwards."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tWedgedDrain"))
            thread_key = _thread_key("tWedgedDrain")
            assert h.substrate.lookup(thread_key) is not None

            monkeypatch.setattr(kernel_module, "_RESET_INTERRUPT_TIMEOUT_S", 0.2)

            wedged = asyncio.Event()  # never set

            async def never_answers(base_url: str, reason: str, token: str | None = None) -> None:
                await wedged.wait()

            monkeypatch.setattr(h.kernel._runner, "interrupt", never_answers)

            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await h.async_redis.sadd(THREAD_RESET_SET, thread_key)

            await asyncio.wait_for(consumer._drain_thread_reset_requests(), timeout=2.0)

            assert h.substrate.lookup(thread_key) is None  # the reset was not lost

    asyncio.run(go())


def test_maintenance_tick_thread_reset_is_a_noop_when_nothing_pending(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer._drain_thread_reset_requests()  # must not raise

    asyncio.run(go())


def test_maintenance_tick_thread_reset_one_failure_does_not_block_the_rest(
    make_harness, caplog
) -> None:
    """A release failure for one requested thread (e.g. a transient substrate
    error) is logged and does not prevent the rest of the batch from being
    drained -- an operator resetting several stuck threads at once should not
    have one bad apple silently strand the others unprocessed."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tOk"))

            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await h.async_redis.sadd(THREAD_RESET_SET, "tBoom", "tOk")

            original_release_thread = h.kernel.release_thread

            async def flaky_release_thread(thread_key: str) -> bool:
                if thread_key == "tBoom":
                    raise RuntimeError("injected substrate failure")
                return await original_release_thread(thread_key)

            h.kernel.release_thread = flaky_release_thread  # type: ignore[method-assign]

            with caplog.at_level(logging.ERROR):
                await consumer._drain_thread_reset_requests()

            assert h.substrate.lookup("tOk") is None  # still processed despite tBoom's failure
            assert await h.async_redis.scard(THREAD_RESET_SET) == 0  # both popped either way
            assert any("tBoom" in r.getMessage() for r in caplog.records)

    asyncio.run(go())


def test_maintenance_tick_reset_drain_has_a_per_tick_budget_and_defers_the_rest(
    make_harness, monkeypatch
) -> None:
    """#743: a large operator-populated batch of wedged resets must not cost
    N x the per-request release bound inline in one maintenance tick -- that
    re-crosses the same multi-hundred-second stall #739 set out to eliminate,
    just scaled by batch size instead of by the runner's HTTP timeout. The
    drain now stops once its per-tick time budget is spent and leaves
    whatever is left in THREAD_RESET_SET for a later tick, so one call to
    ``_drain_thread_reset_requests`` never blocks proportionally to N."""

    async def go() -> None:
        async with make_harness() as h:
            budget_s = 0.2
            monkeypatch.setattr(consumer_module, "_THREAD_RESET_DRAIN_BUDGET_S", budget_s)

            processed: list[str] = []

            async def slow_release_thread(thread_key: str) -> bool:
                processed.append(thread_key)
                await asyncio.sleep(0.05)  # each request "wedged" for a while
                return True

            h.kernel.release_thread = slow_release_thread  # type: ignore[method-assign]

            # A batch large enough that draining it all at 0.05s/request would
            # take roughly 1s -- five times the budget.
            keys = [f"tBatch{i}" for i in range(20)]
            await h.async_redis.sadd(THREAD_RESET_SET, *keys)

            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            start = time.monotonic()
            await asyncio.wait_for(consumer._drain_thread_reset_requests(), timeout=2.0)
            elapsed = time.monotonic() - start

            # Bounded by the budget (plus slack for the one in-flight request
            # that pushed the check past it), not by N * per-request cost.
            assert elapsed < 0.6
            assert len(processed) < len(keys)  # did not drain the whole batch in one pass
            remaining = await h.async_redis.scard(THREAD_RESET_SET)
            assert remaining > 0  # the rest is left for the next tick, not lost

            # A later tick picks up where this one left off: draining again
            # (with the budget restored to a generous value) finishes the batch.
            monkeypatch.setattr(consumer_module, "_THREAD_RESET_DRAIN_BUDGET_S", 30.0)
            await asyncio.wait_for(consumer._drain_thread_reset_requests(), timeout=5.0)
            assert await h.async_redis.scard(THREAD_RESET_SET) == 0
            assert len(processed) == len(keys)

    asyncio.run(go())


def test_maintenance_tick_thread_reset_is_not_stalled_by_a_hanging_substrate_release(
    make_harness, monkeypatch
) -> None:
    """#743: the courtesy interrupt bound (#739) only covers a wedged runner.
    `release_thread`'s own substrate release runs on a bare `asyncio.to_thread`
    with no timeout, so a hang in the K8s control plane -- a claim delete that
    never returns -- would stall the tick just as unboundedly. The release
    call must be bounded the same way the interrupt already is."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tHangRelease"))
            thread_key = _thread_key("tHangRelease")
            assert h.substrate.lookup(thread_key) is not None

            monkeypatch.setattr(kernel_module, "_RESET_RELEASE_TIMEOUT_S", 0.2)

            def hanging_release(thread_key: str) -> bool:
                time.sleep(5.0)  # never returns within the test's window
                return True

            monkeypatch.setattr(h.substrate, "release", hanging_release)

            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await h.async_redis.sadd(THREAD_RESET_SET, thread_key)

            # Must finish well under the 5s hang, bounded instead by the
            # (monkeypatched) release timeout.
            await asyncio.wait_for(consumer._drain_thread_reset_requests(), timeout=2.0)

            # The request was popped either way; a fresh reset is needed to retry.
            assert await h.async_redis.scard(THREAD_RESET_SET) == 0

    asyncio.run(go())
