"""A protected delivery can wait for admission without spending its retry budget.

These tests use the kernel suite's real Valkey consumer-group fixture. They pin
the shared StreamConsumer boundary: an entry deferred by the read-only
eligibility callback stays pending in place, while ordinary entries retain the
existing cap and compare-and-claim behavior.
"""

from __future__ import annotations

import asyncio
import functools
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest
from aci_protocol import Final, SessionStatus
from curie_dispatcher.queue import to_stream_fields
from curie_worker import stream_consumer as stream_consumer_module
from curie_worker.consumer import Consumer
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.stream_consumer import DeliverySpec

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from queue_fixtures import qevent  # noqa: E402

DONE = SessionStatus.DONE
_qevent = functools.partial(
    qevent,
    kind="email",
    channel="agent@example.test",
    placeholder="msg_upstream",
    endpoint="https://adapter.example/hook",
    adapter="agentmail-sandbox",
)


def test_deferred_over_cap_entry_is_left_untouched_until_eligible(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A parked protected entry is not capped, transferred, or reported as retry."""

    async def go() -> None:
        metric_calls: list[tuple[str, dict[str, object]]] = []

        def record_metric(name: str, *, attributes: dict[str, object]) -> None:
            metric_calls.append((name, attributes))

        monkeypatch.setattr(stream_consumer_module, "record_metric", record_metric)

        async with make_harness(
            max_delivery=2,
            reclaim_min_idle_ms=0,
        ) as h:
            eligibility_calls: list[str] = []

            async def entry_eligibility(entry_id: str, fields: dict[str, str] | None) -> bool:
                eligibility_calls.append(entry_id)
                assert fields is None or isinstance(fields, dict)
                return False

            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            consumer._delivery = replace(
                cast(DeliverySpec, consumer._delivery),
                entry_eligibility=entry_eligibility,
            )
            await consumer.ensure_group()
            event = _qevent("wait for admission", thread="tProtectedWait", event_id="wait")
            entry_id = await h.async_redis.xadd(h.config.stream, to_stream_fields(event))

            # Establish one real PEL delivery owned by a different consumer.
            rows = await h.async_redis.xreadgroup(
                h.config.consumer_group,
                "previous-owner",
                {h.config.stream: ">"},
                count=1,
            )
            assert rows and rows[0][1][0][0] == entry_id
            # Spend the second allowed delivery before the measured defer.
            claimed = await h.async_redis.xclaim(
                h.config.stream,
                h.config.consumer_group,
                "previous-owner",
                0,
                [entry_id],
            )
            assert claimed and claimed[0][0] == entry_id
            before = await h.async_redis.xpending_range(
                h.config.stream,
                h.config.consumer_group,
                min=entry_id,
                max=entry_id,
                count=1,
            )
            assert len(before) == 1
            before_count = int(before[0]["times_delivered"])
            assert before_count == 2
            before_owner = str(before[0]["consumer"])
            before_idle = int(before[0]["time_since_delivered"])
            before_lease = await h.async_redis.get(
                h.config.delivery_lease_key(h.config.stream, h.config.consumer_group, entry_id)
            )
            assert before_lease is None

            # The callback must suppress every transfer path, including the
            # over-cap scan and the unconditional compatibility XAUTOCLAIM pass.
            await consumer._reclaim_once()
            await consumer._reclaim_once()

            after = await h.async_redis.xpending_range(
                h.config.stream,
                h.config.consumer_group,
                min=entry_id,
                max=entry_id,
                count=1,
            )
            assert len(after) == 1, "deferred work must remain pending"
            assert int(after[0]["times_delivered"]) == before_count, (
                "waiting for admission must not spend a delivery attempt"
            )
            assert str(after[0]["consumer"]) == before_owner, (
                "waiting must not transfer PEL ownership"
            )
            assert int(after[0]["time_since_delivered"]) >= before_idle
            assert (
                await h.async_redis.get(
                    h.config.delivery_lease_key(h.config.stream, h.config.consumer_group, entry_id)
                )
                == before_lease
            ), "eligibility probing must not create or change a lease"
            assert await h.async_redis.xrange(h.config.dead_letter_stream_name()) == []
            assert eligibility_calls and set(eligibility_calls) == {entry_id}
            assert not any(name == "curie.queue.retry" for name, _ in metric_calls)

    asyncio.run(go())


def test_default_consumer_still_applies_the_existing_over_cap_rule(make_harness) -> None:
    """Omitting the optional callback preserves the ordinary delivery cap."""

    async def go() -> None:
        async with make_harness(max_delivery=2, reclaim_min_idle_ms=0) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()
            event = _qevent("ordinary retry", thread="tOrdinary", event_id="ordinary")
            entry_id = await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
            rows = await h.async_redis.xreadgroup(
                h.config.consumer_group,
                "previous-owner",
                {h.config.stream: ">"},
                count=1,
            )
            assert rows and rows[0][1][0][0] == entry_id
            claimed = await h.async_redis.xclaim(
                h.config.stream,
                h.config.consumer_group,
                "previous-owner",
                0,
                [entry_id],
            )
            assert claimed and claimed[0][0] == entry_id

            await consumer._reclaim_once()

            assert (
                await h.async_redis.xpending_range(
                    h.config.stream,
                    h.config.consumer_group,
                    min=entry_id,
                    max=entry_id,
                    count=1,
                )
                == []
            )
            dead = await h.async_redis.xrange(h.config.dead_letter_stream_name())
            assert len(dead) == 1
            assert dead[0][1]["dl_original_id"] == entry_id

    asyncio.run(go())


def test_compare_and_claim_defers_one_entry_without_blocking_eligible_sibling(
    make_harness,
) -> None:
    """Candidate filtering preserves positive-idle claims and sibling progress."""

    async def go() -> None:
        async with make_harness(max_delivery=4, reclaim_min_idle_ms=0) as h:
            decisions: list[str] = []

            async def entry_eligibility(entry_id: str, fields: dict[str, str] | None) -> bool:
                decisions.append(entry_id)
                assert fields is None or isinstance(fields, dict)
                return entry_id != deferred_id

            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            consumer._delivery = replace(
                cast(DeliverySpec, consumer._delivery),
                entry_eligibility=entry_eligibility,
            )
            await consumer.ensure_group()
            deferred = await h.async_redis.xadd(h.config.stream, {"payload": "deferred"})
            eligible = await h.async_redis.xadd(h.config.stream, {"payload": "eligible"})
            deferred_id = str(deferred)
            eligible_id = str(eligible)
            rows = await h.async_redis.xreadgroup(
                h.config.consumer_group,
                "previous-owner",
                {h.config.stream: ">"},
                count=2,
            )
            assert rows and {row[0] for row in rows[0][1]} == {deferred_id, eligible_id}
            await asyncio.sleep(0.02)
            pending = await h.async_redis.xpending_range(
                h.config.stream,
                h.config.consumer_group,
                min="-",
                max="+",
                count=10,
            )
            by_id = {str(row["message_id"]): row for row in pending}
            assert set(by_id) == {deferred_id, eligible_id}

            observed_idle = int(by_id[eligible_id]["time_since_delivered"])
            claim_calls: list[tuple[int, list[str]]] = []
            real_xclaim = h.async_redis.xclaim

            async def record_xclaim(
                name: str,
                group: str,
                consumername: str,
                min_idle_time: int,
                message_ids: list[str],
                **kwargs: object,
            ) -> Any:
                claim_calls.append((min_idle_time, list(message_ids)))
                return await real_xclaim(
                    name,
                    group,
                    consumername,
                    min_idle_time,
                    message_ids,
                    **kwargs,
                )

            h.async_redis.xclaim = record_xclaim  # type: ignore[method-assign]
            selected = await consumer._claim_pending_page(
                pending,
                set(),
                min_idle_ms=None,
            )

            assert [entry_id for entry_id, _fields in selected] == [eligible_id]
            assert len(claim_calls) == 1
            assert claim_calls[0][0] >= max(1, observed_idle)
            assert claim_calls[0][1] == [eligible_id]
            assert set(decisions) == {deferred_id, eligible_id}
            after = await h.async_redis.xpending_range(
                h.config.stream,
                h.config.consumer_group,
                min="-",
                max="+",
                count=10,
            )
            after_by_id = {str(row["message_id"]): row for row in after}
            assert int(after_by_id[deferred_id]["times_delivered"]) == int(
                by_id[deferred_id]["times_delivered"]
            )
            assert str(after_by_id[deferred_id]["consumer"]) == "previous-owner"
            assert str(after_by_id[eligible_id]["consumer"]) == h.config.consumer_name

    asyncio.run(go())


def test_eligible_entry_is_reclaimed_and_committed_after_waiting(
    make_harness,
) -> None:
    """A parked row becomes ordinary reclaimable work when eligibility opens."""

    async def go() -> None:
        async with make_harness(max_delivery=4, reclaim_min_idle_ms=0) as h:
            h.runner.default_script = [Final(text="resumed", status=DONE)]
            admitted = False
            eligibility_calls: list[str] = []

            async def entry_eligibility(entry_id: str, fields: dict[str, str] | None) -> bool:
                eligibility_calls.append(entry_id)
                assert fields is None or isinstance(fields, dict)
                return admitted

            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            consumer._delivery = replace(
                cast(DeliverySpec, consumer._delivery),
                entry_eligibility=entry_eligibility,
            )
            await consumer.ensure_group()
            event = _qevent("wait then resume", thread="tProtectedResume", event_id="resume")
            entry_id = await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
            rows = await h.async_redis.xreadgroup(
                h.config.consumer_group,
                "previous-owner",
                {h.config.stream: ">"},
                count=1,
            )
            assert rows and rows[0][1][0][0] == entry_id

            # While admission is preparing, the protected entry stays pending.
            await consumer._reclaim_once()
            pending = await h.async_redis.xpending_range(
                h.config.stream,
                h.config.consumer_group,
                min=entry_id,
                max=entry_id,
                count=1,
            )
            assert len(pending) == 1
            parked_count = int(pending[0]["times_delivered"])
            assert str(pending[0]["consumer"]) == "previous-owner"

            admitted = True
            await consumer._reclaim_once()
            await asyncio.gather(*list(consumer._inflight))

            assert (
                await h.async_redis.xpending_range(
                    h.config.stream,
                    h.config.consumer_group,
                    min=entry_id,
                    max=entry_id,
                    count=1,
                )
                == []
            )
            assert await h.async_redis.exists(h.config.done_key(event.event_id)) == 1
            assert h.runner.opened == ["wait then resume"]
            assert [completion.outcome for completion in h.sink.completions] == ["delivered"]
            assert len(eligibility_calls) >= 2
            assert parked_count == 1

    asyncio.run(go())


def test_slow_eligibility_refreshes_delivery_cap_before_claim(make_harness) -> None:
    """A competing transfer during verification cannot spend past max_delivery."""

    async def go() -> None:
        async with make_harness(max_delivery=2, reclaim_min_idle_ms=0) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()
            entry_id = await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("slow verification", thread="tSlowGate", event_id="slow")),
            )
            rows = await h.async_redis.xreadgroup(
                h.config.consumer_group,
                "previous-owner",
                {h.config.stream: ">"},
                count=1,
            )
            assert rows and rows[0][1][0][0] == entry_id

            claim_owners: list[str] = []
            real_xclaim = h.async_redis.xclaim

            async def competing_transfer(_entry_id: str, _fields: dict[str, str] | None) -> bool:
                if not claim_owners:
                    claimed = await real_xclaim(
                        h.config.stream,
                        h.config.consumer_group,
                        "other-worker",
                        0,
                        [entry_id],
                    )
                    assert claimed and claimed[0][0] == entry_id
                    claim_owners.append("other-worker")
                return True

            consumer._delivery = replace(
                cast(DeliverySpec, consumer._delivery),
                entry_eligibility=competing_transfer,
            )
            await consumer._reclaim_once()

            assert claim_owners == ["other-worker"]
            assert await h.async_redis.xpending_range(
                h.config.stream,
                h.config.consumer_group,
                min=entry_id,
                max=entry_id,
                count=1,
            ) == []
            dead = await h.async_redis.xrange(h.config.dead_letter_stream_name())
            assert len(dead) == 1
            assert dead[0][1]["dl_delivery_count"] == "2"

    asyncio.run(go())
