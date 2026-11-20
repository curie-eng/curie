"""Diagnostic probes for two kernel tests that failed in one local parallel run.

``test_consumer.py::test_runner_acceptance_boundary_respects_wait_deadline``
raised IndexError on an empty XREADGROUP right after ``wake_due() == 1``, and
``test_delivery_ownership.py::test_an_owner_that_loses_its_lease_refuses_the_terminal_ack``
timed out waiting for ``lease.lost`` while ``finish_order`` refused with
"thread order release lost its delivery". Neither cause is established.

These probes do not repeat those tests. Each changes one input or adds the
observation the failures lacked, records what it saw in its assertion
messages, and asserts only what the production contract promises.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

from curie_internal.turn_queue import to_stream_fields
from curie_worker.sandbox.types import QuotaRejection

from .test_consumer import _capacity_consumer, _wait_capacity_state

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from queue_fixtures import qevent as _qevent  # noqa: E402

# The ownership suite's compressed lease clocks (test_delivery_ownership.py).


def _text(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode()
    if isinstance(value, (list, tuple)):
        return [_text(item) for item in value]
    if isinstance(value, dict):
        return {_text(key): _text(item) for key, item in value.items()}
    return value


async def _queue_state(h: Any) -> dict[str, Any]:
    """The stream, its group, its readers and its PEL, read straight from Valkey."""

    redis = h.async_redis
    stream, group = h.config.stream, h.config.consumer_group
    clients = _text(await redis.client_list())
    return {
        "blocked_readers": [
            {key: client.get(key) for key in ("id", "name", "cmd", "flags", "age", "idle")}
            for client in clients
            if str(client.get("cmd", "")).startswith("xread")
        ],
        "tasks": sorted(
            repr(task.get_coro()).split(" at ")[0]
            for task in asyncio.all_tasks()
            if not task.done() and task is not asyncio.current_task()
        ),
        "entries": _text(await redis.xrange(stream, "-", "+")),
        "groups": _text(await redis.xinfo_groups(stream)),
        "consumers": _text(await redis.xinfo_consumers(stream, group)),
        "pending": _text(await redis.xpending_range(stream, group, min="-", max="+", count=100)),
    }


def test_a_due_capacity_wake_is_a_new_entry_for_the_group(make_harness) -> None:
    """Where the woken entry is, observed before the read that came back empty.

    Same park as the acceptance boundary test, with changed inputs: the parked
    consumer is stopped as soon as the wait is recorded, its run task is
    confirmed finished, and the stream, the group, the PEL, the server's
    blocked readers and this loop's live tasks are read before and after
    ``wake_due``. The woken entry is then read with ``XREADGROUP >``.

    Asserted: one due wake appends exactly one stream entry for the parked
    event, and the group holds it exactly once, either as a new entry for
    ``>`` or already in the PEL of this consumer name. The second happens when
    the stopped generation's cancelled blocking read is still blocked on the
    server (redis-py closes the socket on cancel; the server sees it later),
    so that read takes the entry. Which one happened is in the messages.
    """

    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
            capacity_wait_budget_s=1.0,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            event = _qevent("hello", thread="probe-wake", event_id="probe-wake-turn")
            first = _capacity_consumer(h)
            await first.ensure_group()
            await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
            first_task = asyncio.create_task(first.run())
            try:
                await _wait_capacity_state(first, event.event_id, "waiting")
            finally:
                first.request_stop()
                await first_task
            assert first_task.done() and first_task.exception() is None
            assert not first._inflight, f"parked consumer left work in flight: {first._inflight}"

            h.fake_k8s.quota_rejection = None
            replacement = _capacity_consumer(h)
            await replacement.ensure_group()
            before = await _queue_state(h)
            await h.async_redis.zadd(replacement._waits._due, {event.event_id: 0})
            woke = await replacement._waits.wake_due()
            after = await _queue_state(h)
            rows = await h.async_redis.xreadgroup(
                h.config.consumer_group,
                h.config.consumer_name,
                {h.config.stream: ">"},
                count=1,
            )
            observed = {"woke": woke, "before": before, "after": after, "read": _text(rows)}

            assert woke == 1, observed
            assert len(after["entries"]) == len(before["entries"]) + 1, observed
            wake_id, wake_fields = after["entries"][-1]
            assert any(event.event_id in str(value) for value in wake_fields.values()), observed
            read_ids = [entry_id for _, entries in _text(rows) for entry_id, _ in entries]
            pel_ids = [row["message_id"] for row in after["pending"]]
            assert (read_ids == [wake_id]) != (wake_id in pel_ids), observed
            if wake_id in pel_ids:
                (row,) = [row for row in after["pending"] if row["message_id"] == wake_id]
                assert row["consumer"] == h.config.consumer_name, observed
                print(f"wake probe: the woken entry was already delivered: {observed}")

    asyncio.run(go())
