"""Reminder poison is bounded by the shared stream owner before turn admission."""

import asyncio
import hashlib
import json
import uuid
from datetime import UTC, datetime

import pytest
from aci_protocol import Final, SessionStatus
from curie_dispatcher.queue import to_stream_fields
from curie_internal.approval_reminder import APPROVAL_REMINDER_FIELD, ApprovalReminderV1
from curie_worker.approval_reminders import deliver_reminder
from curie_worker.approvals import CreatedApproval
from curie_worker.consumer import Consumer
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.reply_sink import CLUSTER_MESSAGE_ADAPTER

from ..queue_fixtures import qevent
from .test_approval_lifecycle import RecordingReader, RoutedBinding, _awaiting_routed_script
from .test_http_identity_authority import CAPSULE, Binding, Core


@pytest.mark.parametrize(
    "adapter,identity,resolution,post_count,card_adapter",
    [
        (CLUSTER_MESSAGE_ADAPTER, None, {"kind": "slack", "address": "C0EXAMPLE2"}, 2, "default"),
        (
            CLUSTER_MESSAGE_ADAPTER,
            "support-bot",
            {"kind": "slack", "address": "C0EXAMPLE2"},
            2,
            "support-bot",
        ),
        (
            CLUSTER_MESSAGE_ADAPTER,
            None,
            {"mode": "requesting_surface"},
            1,
            CLUSTER_MESSAGE_ADAPTER,
        ),
        ("support-bot", None, {"kind": "slack", "address": "C0EXAMPLE2"}, 2, "support-bot"),
    ],
)
def test_policy_card_reminder_uses_slack_identity_from_a_relay_turn(
    make_harness, adapter, identity, resolution, post_count, card_adapter
):
    async def run():
        approval_id = str(uuid.uuid4())
        creates = []

        class Approvals:
            async def create(self, request, *, budget_s=120):
                creates.append(request)
                return CreatedApproval(id=approval_id, status="pending")

        reader = RecordingReader(None)
        binding = RoutedBinding({"review": {"resolution": resolution, "remind_every_s": 60}})
        async with make_harness(
            approvals=Approvals(), approval_reader=reader, binding=binding
        ) as h:
            h.runner.default_script = _awaiting_routed_script("Approve bounded work", "review")
            turn = qevent(
                "Request a decision",
                channel="C0EXAMPLE1",
                adapter=adapter,
                identity=identity,
                placeholder=str(uuid.uuid4()),
            )
            await h.kernel.process_event(turn)
            await deliver_reminder(
                ApprovalReminderV1(
                    version=1,
                    approval_id=approval_id,
                    claimed_due_at=datetime.now(UTC),
                ),
                reader=reader,
                cards=h.card_store,
                sink=h.sink,
            )
            assert len(creates) == 1
            assert len(h.sink.posts) == post_count
            stored = await h.card_store.read(approval_id)
            assert stored is not None
            assert stored[0].adapter == card_adapter
            if post_count == 2:
                card, reminder = [
                    (event, route)
                    for event, route, _ in h.sink.events
                    if event.event == "reply.post"
                ]
                assert card[0].message.interaction is not None
                assert reminder[0].message.interaction is None
                assert reminder[0].target.address == "C0EXAMPLE2"
                assert reminder[0].target.conversation_id == stored[0].ts
                assert card[1].adapter == reminder[1].adapter == card_adapter

    asyncio.run(run())


def test_normal_identity_turn_still_runs_through_reminder_capable_consumer(make_harness):
    async def run():
        authority = Core()
        async with make_harness(binding=Binding(), identity_authority=authority) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()
            fields = to_stream_fields(qevent("ordinary identity turn"))
            fields["curie_identity_context_v1"] = CAPSULE
            await h.async_redis.xadd(h.config.stream, fields)
            entries = await h.async_redis.xreadgroup(
                h.config.consumer_group,
                h.config.consumer_name,
                {h.config.stream: ">"},
                count=1,
            )
            entry_id, observed = entries[0][1][0]
            await consumer._dispatch(entry_id, observed)
            await asyncio.gather(*list(consumer._inflight))
            assert h.runner.opened == ["ordinary identity turn"]
            assert authority.requests[-1].stage == "start"
            assert all(
                request.context == CAPSULE
                and request.payload_sha256 == hashlib.sha256(fields["payload"].encode()).hexdigest()
                for request in authority.requests
            )
            assert (await h.async_redis.xpending(h.config.stream, h.config.consumer_group))[
                "pending"
            ] == 0
            assert await h.async_redis.xlen(h.config.dead_letter_stream_name()) == 0

    asyncio.run(run())


@pytest.mark.parametrize("poison", ["malformed", "mixed", "identity", "version"])
def test_poison_reminder_dead_letters_without_turn_execution_and_group_progresses(
    make_harness, poison
):
    async def run():
        async with make_harness(max_delivery=2, reclaim_min_idle_ms=0) as h:
            h.runner.default_script = [Final(text="healthy", status=SessionStatus.DONE)]
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()
            reminder = ApprovalReminderV1(
                version=1, approval_id=str(uuid.uuid4()), claimed_due_at=datetime.now(UTC)
            )
            fields = {APPROVAL_REMINDER_FIELD: reminder.model_dump_json()}
            if poison == "malformed":
                fields[APPROVAL_REMINDER_FIELD] = "{"
            elif poison == "mixed":
                fields.update(to_stream_fields(qevent("must never execute", event_id="poison")))
            elif poison == "identity":
                fields["curie_identity_context_v1"] = "untrusted"
            else:
                raw = json.loads(reminder.model_dump_json())
                raw["version"] = 2
                fields[APPROVAL_REMINDER_FIELD] = json.dumps(raw)
            await h.async_redis.xadd(h.config.stream, fields)
            await h.async_redis.xadd(
                h.config.stream, to_stream_fields(qevent("healthy", event_id="healthy"))
            )
            entries = await h.async_redis.xreadgroup(
                h.config.consumer_group,
                h.config.consumer_name,
                {h.config.stream: ">"},
                count=10,
            )
            for _, batch in entries:
                for entry_id, payload in batch:
                    await consumer._dispatch(entry_id, payload)
            await asyncio.gather(*list(consumer._inflight))
            dead = await h.async_redis.xrange(h.config.dead_letter_stream_name())
            assert len(dead) == 1
            assert dead[0][1]["dl_reason"] == "unparseable"
            assert h.runner.opened == ["healthy"]
            assert (await h.async_redis.xpending(h.config.stream, h.config.consumer_group))[
                "pending"
            ] == 0

    asyncio.run(run())
