"""A deleted stream entry is not a live successor (#3349).

The runs consumer holds the delivery open inside ``process_event`` while the
real lease heartbeat runs against real Valkey. Deleting that stream entry must
graveyard it as ``broker-entry-vanished`` and edit the placeholder. Moving the
PEL row to another consumer must do neither, and must not ack.
"""

from __future__ import annotations

import asyncio
import functools
import sys
import time
from pathlib import Path
from typing import Any

from aci_protocol import QueuedTurn
from curie_dispatcher.queue import to_stream_fields
from curie_worker.consumer import Consumer
from curie_worker.delivery_lease import DeliveryLease, DeliveryLeaseStore

from .conftest import _pending_rows

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from queue_fixtures import qevent  # noqa: E402
from queue_fixtures import wait_until as _wait_until  # noqa: E402

_qevent = functools.partial(qevent, event_id="notice-1")

# TTL (1.0) is at least 3x the heartbeat (0.2). Reclaim interval (0.5) stays
# strictly under the TTL. The runner ceiling and budget stay at their defaults,
# which already satisfy runner_total_timeout_s <= delivery_budget_s.
_LEASE_KNOBS: dict[str, object] = {
    "delivery_lease_heartbeat_s": 0.2,
    "delivery_lease_ttl_s": 1.0,
    "reclaim_interval_s": 0.5,
}


def _stall(kernel: Any) -> tuple[asyncio.Event, asyncio.Event, dict[str, DeliveryLease]]:
    """Replace ``process_event`` with a hold the test releases.

    The consumer still calls this wrapper, so ``_delivery_lease`` keeps the
    heartbeat running for the whole hold. The real kernel body is not entered:
    a turn started after the entry is gone would post a second message.
    """
    entered = asyncio.Event()
    release = asyncio.Event()
    held: dict[str, DeliveryLease] = {}

    async def stalled(qevent: QueuedTurn, *, lease: DeliveryLease | None = None) -> None:
        del qevent
        if lease is not None:
            held["lease"] = lease
        entered.set()
        await release.wait()

    kernel.process_event = stalled  # type: ignore[method-assign,assignment]
    return entered, release, held


async def _settle(consumer: Consumer) -> None:
    await asyncio.gather(*list(consumer._inflight))


async def _vanished_rows(redis: Any, stream: str, entry_id: str) -> list[dict[str, str]]:
    rows = await redis.xrange(stream)
    return [
        dict(fields)
        for _row_id, fields in rows
        if fields.get("dl_original_id") == entry_id
        and fields.get("dl_reason") == "broker-entry-vanished"
    ]


async def _wait_vanished(
    redis: Any, stream: str, entry_id: str, *, timeout: float = 3.0
) -> list[dict[str, str]]:
    deadline = time.monotonic() + timeout
    found: list[dict[str, str]] = []
    while time.monotonic() < deadline:
        found = await _vanished_rows(redis, stream, entry_id)
        if found:
            return found
        await asyncio.sleep(0.05)
    return found


async def _deliver(h: Any, consumer: Consumer, qevent: QueuedTurn) -> tuple[str, dict[str, str]]:
    await h.async_redis.xadd(h.config.stream, to_stream_fields(qevent))
    rows = await h.async_redis.xreadgroup(
        h.config.consumer_group, h.config.consumer_name, {h.config.stream: ">"}, count=1
    )
    assert rows, "expected an entry to read"
    entry_id, fields = rows[0][1][0]
    await consumer._dispatch(entry_id, dict(fields))
    return entry_id, dict(fields)


def test_a_deleted_stream_entry_graveyards_as_vanished_and_edits_the_placeholder(
    make_harness: Any,
) -> None:
    """XDEL under a live owner is not a successor. The graveyard records it and
    the placeholder is edited once. A new post would tell the thread twice.
    """

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS) as h:
            entered, release, _held = _stall(h.kernel)
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()
            try:
                entry_id, _fields = await _deliver(
                    h, consumer, _qevent("hello", thread="vanished-1", event_id="vanished-1")
                )
                await _wait_until(entered.is_set)
                await h.async_redis.xdel(h.config.stream, entry_id)
                rows = await _wait_vanished(
                    h.async_redis, h.config.dead_letter_stream_name(), entry_id
                )
                assert len(rows) == 1
                assert rows[0]["dl_reason"] == "broker-entry-vanished"
                assert rows[0]["dl_original_id"] == entry_id
                assert h.sink.updates == [("C1", "p-1", h.config.turn_not_started_text)]
                assert h.sink.text_posts == []
            finally:
                release.set()
                await _settle(consumer)
                await h.async_redis.delete(h.config.dead_letter_stream_name())

    asyncio.run(go())


def test_a_pel_row_moved_to_another_consumer_is_not_a_vanished_entry(
    make_harness: Any,
) -> None:
    """XCLAIM leaves the entry pending for the new owner. That refusal must not
    graveyard it or edit the placeholder, and this owner must not ack.
    """

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS) as h:
            entered, release, held = _stall(h.kernel)
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()
            other = "other-owner"
            entry_id: str | None = None
            try:
                entry_id, _fields = await _deliver(
                    h, consumer, _qevent("hello", thread="claimed-1", event_id="claimed-1")
                )
                await _wait_until(entered.is_set)
                lease = held["lease"]
                # A heartbeat already inside its renewal can XCLAIM the row back.
                # Keep moving it until that renewal refuses and the loop stops.
                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline and not lease.lost.is_set():
                    await h.async_redis.xclaim(
                        h.config.stream,
                        h.config.consumer_group,
                        other,
                        0,
                        [entry_id],
                    )
                    await asyncio.sleep(0.05)
                assert lease.lost.is_set()
                await h.async_redis.xclaim(
                    h.config.stream,
                    h.config.consumer_group,
                    other,
                    0,
                    [entry_id],
                )
                await asyncio.sleep(h.config.delivery_lease_heartbeat_s + 0.05)
                rows = await _vanished_rows(
                    h.async_redis, h.config.dead_letter_stream_name(), entry_id
                )
                assert rows == []
                assert h.config.turn_not_started_text not in {
                    text for _channel, _ref, text in h.sink.updates
                }
                release.set()
                await _settle(consumer)
                pending = await _pending_rows(h)
                assert entry_id in pending
            finally:
                release.set()
                await _settle(consumer)
                await h.async_redis.delete(h.config.dead_letter_stream_name())

    asyncio.run(go())
