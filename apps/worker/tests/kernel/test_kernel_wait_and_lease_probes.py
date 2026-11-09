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
import contextlib
import functools
import sys
from pathlib import Path
from typing import Any

import pytest
from aci_protocol import Final, SessionStatus, TextDelta
from curie_dispatcher.queue import to_stream_fields
from curie_worker.capacity_wait import CapacityWaitStore
from curie_worker.sandbox.types import QuotaRejection

from .conftest import _pending_rows, _ProcessEventSpy
from .test_consumer import _capacity_consumer, _wait_capacity_state

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from queue_fixtures import qevent as _qevent  # noqa: E402
from queue_fixtures import wait_until  # noqa: E402

_wait_until = functools.partial(wait_until, timeout=10.0)

DONE = SessionStatus.DONE

# The ownership suite's compressed lease clocks (test_delivery_ownership.py).
_LEASE_KNOBS: dict[str, object] = {
    "delivery_budget_s": 60.0,
    "delivery_lease_ttl_s": 1.0,
    "delivery_lease_heartbeat_s": 0.3,
    "runner_total_timeout_s": 30.0,
}


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


@pytest.mark.parametrize("deleted", ["before_order_release", "after_order_release"])
def test_ownership_loss_around_the_post_start_order_release(
    make_harness, monkeypatch: pytest.MonkeyPatch, deleted: str
) -> None:
    """Lose the lease on a known side of ``finish_order`` and observe what follows.

    The ownership test deleted the lease once the fake runner set
    ``turn_active``, which happens before the worker's post-start thread order
    release (``finish_order`` in ``attempt.py``). This probe observes that
    release through a pass-through wrapper on the production
    ``CapacityWaitStore.finish_order`` and deletes the lease either while the
    release is held just before it runs, or after it returned while the runner
    holds the open turn.

    Both sides must keep the stale owner from acking, and an untouched delivery
    on the same consumer must still ack. After the release, the lost lease must
    be seen and must stop the turn through the bounded interrupt path. Before
    the release, the observed sequence is reported in the assertion messages.
    """

    releases: list[dict[str, Any]] = []
    entered = asyncio.Event()
    proceed = asyncio.Event()
    original = CapacityWaitStore.finish_order

    async def observed_finish_order(
        self: CapacityWaitStore, thread_key: str, event_id: str, lease: Any, *, started: bool
    ) -> None:
        record: dict[str, Any] = {"event_id": event_id, "started": started}
        releases.append(record)
        if event_id == "probe-fence-1":
            entered.set()
            if deleted == "before_order_release":
                await proceed.wait()
        try:
            await original(self, thread_key, event_id, lease, started=started)
        except BaseException as exc:
            record["raised"] = repr(exc)
            raise
        record["returned"] = True

    monkeypatch.setattr(CapacityWaitStore, "finish_order", observed_finish_order)

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS, reclaim_min_idle_ms=900000) as h:
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            spy = _ProcessEventSpy(h.kernel)

            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=DONE)]

            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(
                    _qevent("fenced out", thread="probe-fence-1", event_id="probe-fence-1")
                ),
            )
            rows = await h.async_redis.xreadgroup(
                h.config.consumer_group, h.config.consumer_name, {h.config.stream: ">"}, count=1
            )
            entry_id, fields = rows[0][1][0]
            await consumer._dispatch(entry_id, dict(fields))
            await _wait_until(entered.is_set)
            if deleted == "after_order_release":
                # TextDelta frames never reach the reply sink (worker config),
                # so the post-release point is the production release having
                # returned while the runner holds the open turn.
                await _wait_until(lambda: releases[0].get("returned") is True)
                assert h.runner.queried == ["fenced out"] and h.runner.turn_active

            lease = spy.leases_for("probe-fence-1")[0]
            assert lease is not None, "the kernel was handed no lease"
            await h.async_redis.delete(
                h.config.delivery_lease_key(h.config.stream, h.config.consumer_group, entry_id)
            )
            proceed.set()

            # Observed, not required here: the after-release case asserts it below.
            with contextlib.suppress(AssertionError):
                await _wait_until(lambda: lease.lost.is_set() and h.runner.interrupts >= 1)
            observed = {
                "deleted": deleted,
                "releases": list(releases),
                "lease_lost": lease.lost.is_set(),
                "interrupts": h.runner.interrupts,
                "turn_active": h.runner.turn_active,
                "inflight": len(consumer._inflight),
            }

            hold.set()
            await asyncio.gather(*list(consumer._inflight), return_exceptions=True)
            pending = await _pending_rows(h)
            assert entry_id in pending, f"a fenced-out owner acked the delivery: {observed}"
            if deleted == "after_order_release":
                assert observed["lease_lost"], observed
                assert observed["interrupts"] >= 1, observed

            # POSITIVE CONTROL: an untouched delivery on the same consumer acks.
            h.runner.hold = None
            h.runner.default_script = [Final(text="clean", status=DONE)]
            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(
                    _qevent("clean", thread="probe-fence-2", event_id="probe-fence-2")
                ),
            )
            rows = await h.async_redis.xreadgroup(
                h.config.consumer_group, h.config.consumer_name, {h.config.stream: ">"}, count=1
            )
            clean_id, clean_fields = rows[0][1][0]
            await consumer._dispatch(clean_id, dict(clean_fields))
            await asyncio.gather(*list(consumer._inflight), return_exceptions=True)
            assert clean_id not in await _pending_rows(h), observed
            if deleted == "before_order_release":
                # Reported, not judged: what the stale owner did once its
                # release ran against a lease it no longer held.
                print(f"ownership probe before order release: {observed}")

    asyncio.run(go())
