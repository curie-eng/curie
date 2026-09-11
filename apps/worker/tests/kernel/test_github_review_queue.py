"""Durable queue outcomes for authenticated GitHub review turns."""

from __future__ import annotations

import asyncio
import uuid

from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus
from channel_protocol.reply import REPLY_WIRE_VERSION, ReplyTarget, TurnCompleted
from curie_dispatcher.queue import to_stream_fields
from curie_worker.consumer import Consumer
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.markers import CompletionRecord, Markers
from curie_worker.reply_sink import TargetRoute

from apps.worker.tests.kernel.test_github_reviews import (
    ReviewBinding,
    ReviewPublicationApi,
    ReviewWorkspace,
    _claim_matching_route,
    _review_turn,
    _wait_until,
)


def test_busy_verified_review_delivery_stays_pending_then_runs_once_idle(
    make_harness,
) -> None:
    async def exercise() -> None:
        turn = _review_turn()
        api = ReviewPublicationApi(turn)
        async with make_harness(
            binding=ReviewBinding(),
            publication_creator=api,
            workspace_factory=ReviewWorkspace,
            reclaim_min_idle_ms=1,
        ) as h:
            _claim_matching_route(h, turn)
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()
            entry_id = await h.async_redis.xadd(h.config.stream, to_stream_fields(turn))
            rows = await h.async_redis.xreadgroup(
                h.config.consumer_group,
                h.config.consumer_name,
                {h.config.stream: ">"},
                count=1,
            )
            assert rows
            queued_id, fields = rows[0][1][0]
            assert queued_id == entry_id

            h.runner.turn_active = True
            await consumer._dispatch(entry_id, dict(fields))
            await _wait_until(lambda: entry_id not in consumer._inflight_ids)

            assert len(
                await h.async_redis.xpending_range(
                    h.config.stream,
                    h.config.consumer_group,
                    entry_id,
                    entry_id,
                    1,
                )
            ) == 1
            assert api.reserve_calls == []
            assert h.runner.steer_headers == []
            assert h.runner.event_headers == []

            h.runner.turn_active = False
            h.runner.default_script = [
                Final(text="review complete", status=SessionStatus.DONE)
            ]
            await consumer._dispatch(entry_id, dict(fields))
            await _wait_until(lambda: entry_id not in consumer._inflight_ids)

            assert await h.async_redis.xpending_range(
                h.config.stream,
                h.config.consumer_group,
                entry_id,
                entry_id,
                1,
            ) == []
            assert len(api.verify_calls) == 2
            assert len(api.reserve_calls) == 1
            assert h.runner.steer_headers == []
            assert h.runner.opened == [turn.text]

    asyncio.run(exercise())


def test_review_terminal_observer_requires_exact_current_fenced_completion(
    make_harness,
) -> None:
    from curie_api.config import Settings
    from curie_api.github_review_terminal import worker_event_is_terminal

    async def exercise() -> None:
        async with make_harness() as h:
            settings = Settings(KEY_PREFIX=h.config.key_prefix)
            markers = Markers(h.async_redis, h.config)
            leases = DeliveryLeaseStore(h.async_redis, h.config)
            event_id = f"github-feedback-{uuid.uuid4()}"
            await h.async_redis.xgroup_create(
                h.config.stream,
                h.config.consumer_group,
                id="0",
                mkstream=True,
            )
            turn = QueuedTurn(
                event_id=event_id,
                conversation_id="review-terminal",
                author="github:41:example-reviewer",
                text="Review fixture",
                reply_handle=ReplyHandle(
                    kind="slack",
                    channel="C0EXAMPLE1",
                    placeholder=None,
                ),
                received_at="2026-09-05T00:00:00+00:00",
            )
            entry_id = await h.async_redis.xadd(
                h.config.stream,
                {"payload": turn.model_dump_json()},
            )
            pending = await h.async_redis.xreadgroup(
                h.config.consumer_group,
                "old",
                {h.config.stream: ">"},
                count=1,
            )
            assert pending[0][1][0][0] == entry_id
            stale = await leases.acquire(
                h.config.stream,
                h.config.consumer_group,
                entry_id,
                consumer="old",
            )
            assert await leases.release(
                h.config.stream,
                h.config.consumer_group,
                entry_id,
                owner=stale.owner,
            )
            await h.async_redis.xclaim(
                h.config.stream,
                h.config.consumer_group,
                "current",
                min_idle_time=0,
                message_ids=[entry_id],
            )
            current = await leases.acquire(
                h.config.stream,
                h.config.consumer_group,
                entry_id,
                consumer="current",
            )
            assert current.generation == stale.generation + 1
            record = CompletionRecord(
                event_id=event_id,
                event=TurnCompleted(
                    version=REPLY_WIRE_VERSION,
                    event="turn.completed",
                    target=ReplyTarget(
                        kind="slack",
                        address="C0EXAMPLE1",
                        conversation_id="review-terminal",
                        reply_ref=None,
                    ),
                    event_id=event_id,
                    outcome="delivered",
                ),
                route=TargetRoute(),
                created_at=1.0,
            )

            async def settle(lease) -> object:  # noqa: ANN001
                return await markers.settle_fenced(
                    event_id,
                    record,
                    stream=lease.stream,
                    group=lease.group,
                    entry_id=lease.entry_id,
                    owner=lease.owner,
                    generation=lease.generation,
                )

            assert await settle(stale) is None
            assert not await worker_event_is_terminal(h.async_redis, settings, event_id)
            assert await settle(current) is not None
            assert await worker_event_is_terminal(h.async_redis, settings, event_id)
            assert not await worker_event_is_terminal(
                h.async_redis,
                settings,
                event_id + "-other",
            )
            # The completion outbox remains independently terminal after the
            # shorter ordinary done marker has gone away.
            await h.async_redis.delete(h.config.done_key(event_id))
            assert await worker_event_is_terminal(h.async_redis, settings, event_id)
            await h.async_redis.hset(h.config.completion_key(event_id), "done", "0")
            assert not await worker_event_is_terminal(h.async_redis, settings, event_id)

    asyncio.run(exercise())
