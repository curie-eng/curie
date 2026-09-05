"""Bounded route proof; real API provenance and durable ingress are separate.

The verified-head argument is supplied at the kernel boundary in this test.
It proves that a verified review cannot disappear into an unrelated active
turn; it does not claim that a forged webhook acquired that authority.
"""

import asyncio
import uuid

import pytest
from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus, TextDelta
from channel_protocol import scoped_conversation_id
from curie_worker.kernel import ThreadBusyError


def test_verified_review_defers_while_ordinary_slack_still_steers(make_harness) -> None:
    async def exercise() -> None:
        async with make_harness() as h:
            first = QueuedTurn(
                event_id=f"Ev-{uuid.uuid4()}",
                conversation_id="1700000000.000051",
                author="U0REQUEST1",
                text="Continue the existing task.",
                reply_handle=ReplyHandle(kind="slack", channel="C0EXAMPLE1", placeholder=None),
                received_at="2026-09-05T00:00:00+00:00",
            )
            thread_key = scoped_conversation_id(
                first.reply_handle.kind, first.reply_handle.channel, first.conversation_id
            )
            # Exercise a materialized route at the SAME accepted head. Without
            # this premise, the old workspace-handoff guard already defers and
            # would conceal the review-steering bug this regression must catch.
            head = "a" * 40
            handle = await asyncio.to_thread(
                h.substrate.claim,
                thread_key,
                env={"CURIE_RUNNER_TOKEN": "example-review-route-token"},
                workspace_materialized_head=head,
                publication_visible_outcome_revision=0,
            )
            assert handle.workspace_materialized_head == head
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="finished", status=SessionStatus.DONE)]
            active = asyncio.create_task(h.kernel.process_event(first))
            try:
                async with asyncio.timeout(3):
                    while not h.runner.turn_active:
                        await asyncio.sleep(0.01)
                ordinary = first.model_copy(
                    update={"event_id": f"Ev-{uuid.uuid4()}", "text": "Also cover the edge case."}
                )
                await h.kernel.process_event(ordinary)
                assert h.runner.steers == [ordinary.text]
                assert h.runner.opened == [first.text]
                assert h.runner.turn_active
                review = first.model_copy(
                    update={
                        "event_id": f"github-feedback-{uuid.uuid4()}",
                        "author": "github:41:example-reviewer",
                        "text": "Please address this review in its own revision.",
                    }
                )
                async with h.kernel._lock.hold(h.config.lock_key(thread_key)):
                    with pytest.raises(ThreadBusyError):
                        await h.kernel._route_and_start(
                            thread_key,
                            h.kernel._to_event(review),
                            None,
                            source=review.source,
                            lineage_head=head,
                            required_review_head=head,
                            publication_visible_outcome_revision=0,
                        )
                assert h.runner.steers == [ordinary.text]
                assert h.runner.opened == [first.text]
            finally:
                hold.set()
                await active

    asyncio.run(exercise())


def test_review_terminal_observer_requires_exact_current_fenced_completion(make_harness) -> None:
    from channel_protocol.reply import REPLY_WIRE_VERSION, ReplyTarget, TurnCompleted
    from curie_api.config import Settings
    from curie_api.github_review_terminal import worker_event_is_terminal
    from curie_worker.delivery_lease import DeliveryLeaseStore
    from curie_worker.markers import CompletionRecord, Markers
    from curie_worker.reply_sink import TargetRoute

    async def exercise() -> None:
        async with make_harness() as h:
            settings = Settings(KEY_PREFIX=h.config.key_prefix)
            markers = Markers(h.async_redis, h.config)
            leases = DeliveryLeaseStore(h.async_redis, h.config)
            event_id = f"github-feedback-{uuid.uuid4()}"
            await h.async_redis.xgroup_create(
                h.config.stream, h.config.consumer_group, id="0", mkstream=True
            )
            entry_id = await h.async_redis.xadd(
                h.config.stream,
                {"payload": QueuedTurn(
                    event_id=event_id, conversation_id="review-terminal",
                    author="github:41:example-reviewer", text="Review fixture",
                    reply_handle=ReplyHandle(kind="slack", channel="C0EXAMPLE1", placeholder=None),
                    received_at="2026-09-05T00:00:00+00:00",
                ).model_dump_json()},
            )
            pending = await h.async_redis.xreadgroup(
                h.config.consumer_group, "old", {h.config.stream: ">"}, count=1
            )
            assert pending[0][1][0][0] == entry_id
            stale = await leases.acquire(
                h.config.stream, h.config.consumer_group, entry_id, consumer="old"
            )
            assert await leases.release(
                h.config.stream, h.config.consumer_group, entry_id, owner=stale.owner
            )
            await h.async_redis.xclaim(
                h.config.stream, h.config.consumer_group, "current",
                min_idle_time=0, message_ids=[entry_id],
            )
            current = await leases.acquire(
                h.config.stream, h.config.consumer_group, entry_id, consumer="current"
            )
            assert current.generation == stale.generation + 1
            record = CompletionRecord(
                event_id=event_id,
                event=TurnCompleted(
                    version=REPLY_WIRE_VERSION,
                    event="turn.completed",
                    target=ReplyTarget(
                        kind="slack", address="C0EXAMPLE1", conversation_id="review-terminal",
                        reply_ref=None,
                    ),
                    event_id=event_id,
                    outcome="delivered",
                ),
                route=TargetRoute(),
                created_at=1.0,
            )

            async def settle(lease):
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
            assert not await worker_event_is_terminal(h.async_redis, settings, event_id + "-other")
            # The production completion outbox remains independently terminal
            # after the shorter ordinary done marker is absent.
            await h.async_redis.delete(h.config.done_key(event_id))
            assert await worker_event_is_terminal(h.async_redis, settings, event_id)
            await h.async_redis.hset(h.config.completion_key(event_id), "done", "0")
            assert not await worker_event_is_terminal(h.async_redis, settings, event_id)

    asyncio.run(exercise())


def test_review_refuses_idle_runner_until_its_history_is_durable(make_harness) -> None:
    async def exercise() -> None:
        async with make_harness() as h:
            turn = QueuedTurn(
                event_id=f"github-feedback-{uuid.uuid4()}",
                conversation_id="1700000000.000061", author="github:41:example-reviewer",
                text="Review the completed turn only after its history is durable.",
                reply_handle=ReplyHandle(kind="slack", channel="C0EXAMPLE1", placeholder=None),
                received_at="2026-09-05T00:00:00+00:00",
            )
            thread_key = scoped_conversation_id(
                turn.reply_handle.kind, turn.reply_handle.channel, turn.conversation_id
            )
            head = "a" * 40
            await asyncio.to_thread(
                h.substrate.claim, thread_key,
                env={"CURIE_RUNNER_TOKEN": "example-review-route-token"},
                workspace_materialized_head=head, publication_visible_outcome_revision=0,
            )
            assert h.runner.turn_active is False
            h.runner.history_durable = False
            async with h.kernel._lock.hold(h.config.lock_key(thread_key)):
                with pytest.raises(ThreadBusyError, match="queued review revision"):
                    await h.kernel._route_and_start(
                        thread_key, h.kernel._to_event(turn), None,
                        source=turn.source, lineage_head=head, required_review_head=head,
                        publication_visible_outcome_revision=0,
                    )
            assert h.runner.opened == [] and h.runner.steers == []

    asyncio.run(exercise())
