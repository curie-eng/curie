"""Park retry stays at 5000 ms and a delivered queued notice is not repeated."""

from __future__ import annotations

import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Any

from aci_protocol import Final, SessionStatus
from curie_dispatcher.queue import to_stream_fields
from curie_worker.capacity_wait import CapacityWaitStore
from curie_worker.consumer import Consumer
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.sandbox import QuotaRejection

sys.path.insert(0, str(Path(__file__).parent.parent))

from queue_fixtures import qevent as _qevent  # noqa: E402


def _ms(parts: tuple[int, int]) -> int:
    return int(parts[0]) * 1000 + int(parts[1]) // 1000


def _consumer(h: Any) -> Consumer:
    return Consumer(
        redis=h.async_redis,
        kernel=h.kernel,
        config=h.config,
        leases=DeliveryLeaseStore(h.async_redis, h.config),
    )


def _refusal() -> QuotaRejection:
    return QuotaRejection(
        quota_name="curie-sandbox-quota",
        requested={"pods": "1"},
        used={"pods": "2"},
        hard={"pods": "2"},
    )


async def _claim(h: Any, leases: DeliveryLeaseStore) -> tuple[str, dict[str, str], Any]:
    rows = await h.async_redis.xreadgroup(
        h.config.consumer_group,
        h.config.consumer_name,
        {h.config.stream: ">"},
        count=1,
    )
    entry_id, fields = rows[0][1][0]
    lease = await leases.acquire(
        h.config.stream,
        h.config.consumer_group,
        entry_id,
        consumer=h.config.consumer_name,
    )
    return entry_id, fields, lease


async def _park_once(h: Any, store: CapacityWaitStore, leases: DeliveryLeaseStore, event_id: str):
    entry_id, fields, lease = await _claim(h, leases)
    before = _ms(await h.async_redis.time())
    try:
        record = await store.park(entry_id, fields, event_id, lease)
    finally:
        await leases.release(
            h.config.stream,
            h.config.consumer_group,
            entry_id,
            owner=lease.owner,
            resume_event_id=None,
        )
    after = _ms(await h.async_redis.time())
    return record, before, after


def test_park_delay_is_capped_for_every_deferral(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            consumer = _consumer(h)
            await consumer.ensure_group()
            store = CapacityWaitStore(h.async_redis, h.config)
            leases = DeliveryLeaseStore(h.async_redis, h.config)
            event = _qevent("delay bound", thread="delay-thread", event_id="delay-turn")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
            first_wait = None
            deadline = None
            for deferral in range(1, 13):
                record, before, after = await _park_once(h, store, leases, event.event_id)
                if first_wait is None:
                    first_wait = record.first_wait_ms
                    deadline = record.deadline_ms
                assert record.deferrals == deferral
                assert record.first_wait_ms == first_wait
                assert record.deadline_ms == deadline
                low = min(deadline, before + 5000)
                high = min(deadline, after + 5000)
                assert low <= record.due_ms <= high
                await h.async_redis.zadd(store._due, {event.event_id: 0})
                assert await store.wake_due() == 1
            before_short = _ms(await h.async_redis.time())
            await h.async_redis.hset(
                store._record(event.event_id), "deadline_ms", str(before_short + 1000)
            )
            record, before, after = await _park_once(h, store, leases, event.event_id)
            assert record.first_wait_ms == first_wait
            assert record.deadline_ms - before < 5000
            assert record.due_ms == record.deadline_ms
            assert min(record.deadline_ms, before + 5000) <= record.due_ms
            assert record.due_ms <= min(record.deadline_ms, after + 5000)

    asyncio.run(go())


def test_parked_turn_starts_within_bound_after_capacity_frees(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
            reclaim_interval_s=30,
        ) as h:
            h.fake_k8s.quota_rejection = _refusal()
            h.runner.default_script = [
                Final(text="answer after capacity", status=SessionStatus.DONE)
            ]
            consumer = _consumer(h)
            await consumer.ensure_group()
            event = _qevent("held", thread="bound-thread", event_id="bound-turn")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
            task = asyncio.create_task(consumer.run())
            try:
                for deferral in range(1, 6):
                    deadline = time.monotonic() + 30
                    while time.monotonic() < deadline:
                        record = await consumer._waits.get(event.event_id)
                        if (
                            record is not None
                            and record.state == "waiting"
                            and record.deferrals == deferral
                        ):
                            break
                        await asyncio.sleep(0.02)
                    else:
                        raise AssertionError(f"park {deferral} was not observed")
                    await h.async_redis.zadd(consumer._waits._due, {event.event_id: 0})
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    record = await consumer._waits.get(event.event_id)
                    if record is not None and record.state == "waiting" and record.deferrals == 6:
                        break
                    await asyncio.sleep(0.02)
                else:
                    raise AssertionError("sixth park was not observed")
                h.fake_k8s.quota_rejection = None
                started = time.monotonic()
                while time.monotonic() < started + 12:
                    if h.sink.last_text == "answer after capacity":
                        break
                    await asyncio.sleep(0.05)
                assert h.sink.last_text == "answer after capacity"
                assert time.monotonic() - started <= 12
            finally:
                consumer.request_stop()
                await task

    asyncio.run(go())


async def _queued(h: Any) -> int:
    return sum(1 for _channel, _ref, text in h.sink.updates if "queued" in text.lower())


async def _wait_deferrals(consumer: Consumer, event_id: str, deferral: int) -> Any:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        record = await consumer._waits.get(event_id)
        if record is not None and record.state == "waiting" and record.deferrals == deferral:
            return record
        await asyncio.sleep(0.02)
    raise AssertionError(f"deferral {deferral} was not observed")


def test_reparks_do_not_resend_a_delivered_queued_notice(make_harness, caplog) -> None:
    async def delivered(h: Any) -> None:
        h.fake_k8s.quota_rejection = _refusal()
        consumer = _consumer(h)
        await consumer.ensure_group()
        event = _qevent("notice", thread="notice-thread", event_id="notice-turn")
        await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
        task = asyncio.create_task(consumer.run())
        try:
            await _wait_deferrals(consumer, event.event_id, 1)
            deadline = time.monotonic() + 10
            while await _queued(h) < 1 and time.monotonic() < deadline:
                await asyncio.sleep(0.02)
            assert await _queued(h) == 1
            record = await consumer._waits.get(event.event_id)
            assert record is not None and record.notice_pending is False
            for deferral in range(1, 7):
                await h.async_redis.zadd(consumer._waits._due, {event.event_id: 0})
                await _wait_deferrals(consumer, event.event_id, deferral + 1)
            assert await _queued(h) == 1
            h.fake_k8s.quota_rejection = None
            h.runner.default_script = [Final(text="done notice", status=SessionStatus.DONE)]
            await h.async_redis.zadd(consumer._waits._due, {event.event_id: 0})
            deadline = time.monotonic() + 20
            while h.sink.last_text != "done notice" and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
            assert h.sink.last_text == "done notice"
            finished = await consumer._waits.get(event.event_id)
            assert finished is not None and finished.reply_ref
        finally:
            consumer.request_stop()
            await task

    async def failed_first(h: Any) -> None:
        h.fake_k8s.quota_rejection = _refusal()
        h.sink.fail_events.add("reply.update")
        consumer = _consumer(h)
        await consumer.ensure_group()
        event = _qevent("retry notice", thread="retry-thread", event_id="retry-turn")
        await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
        task = asyncio.create_task(consumer.run())
        try:
            await _wait_deferrals(consumer, event.event_id, 1)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if "capacity wait notice failed" in caplog.text:
                    break
                await asyncio.sleep(0.02)
            else:
                raise AssertionError("the first queued notice did not fail")
            record = await consumer._waits.get(event.event_id)
            assert record is not None and record.notice_pending
            assert await _queued(h) == 0
            h.sink.fail_events.discard("reply.update")
            await h.async_redis.zadd(consumer._waits._due, {event.event_id: 0})
            await _wait_deferrals(consumer, event.event_id, 2)
            deadline = time.monotonic() + 10
            while await _queued(h) < 1 and time.monotonic() < deadline:
                await asyncio.sleep(0.02)
            assert await _queued(h) == 1
        finally:
            consumer.request_stop()
            await task

    async def flagged(h: Any, field: str, event_id: str) -> None:
        h.fake_k8s.quota_rejection = _refusal()
        consumer = _consumer(h)
        await consumer.ensure_group()
        event = _qevent(field, thread=event_id, event_id=event_id)
        await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
        task = asyncio.create_task(consumer.run())
        try:
            await _wait_deferrals(consumer, event.event_id, 1)
            deadline = time.monotonic() + 10
            while await _queued(h) < 1 and time.monotonic() < deadline:
                await asyncio.sleep(0.02)
            await h.async_redis.hset(consumer._waits._record(event.event_id), field, "1")
            await h.async_redis.zadd(consumer._waits._due, {event.event_id: 0})
            await _wait_deferrals(consumer, event.event_id, 2)
            deadline = time.monotonic() + 10
            while await _queued(h) < 2 and time.monotonic() < deadline:
                await asyncio.sleep(0.02)
            assert await _queued(h) >= 2
        finally:
            consumer.request_stop()
            await task

    async def go() -> None:
        caplog.set_level(logging.WARNING)
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
            reclaim_interval_s=0.05,
        ) as h:
            await delivered(h)
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
            reclaim_interval_s=0.05,
        ) as h:
            await failed_first(h)
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
            reclaim_interval_s=0.05,
        ) as h:
            await flagged(h, "grant_confirmed", "grant-turn")
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
            reclaim_interval_s=0.05,
        ) as h:
            await flagged(h, "grant_unknown", "unknown-turn")

    asyncio.run(go())
