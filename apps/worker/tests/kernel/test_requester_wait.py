"""A different requester's deferred delivery waits durably, not in the PEL.

ADR 0270 decision 3: another requester never steers a live turn; it waits for
a subsequent execution at the durable handoff boundary. Through the consumer,
a delivery that can park (a person's message with a reply handle, not an
approval resume or a factory turn) is parked in the capacity wait store with
reason ``requester_handoff``. The stream entry is acked, so ``max_delivery``
never dead-letters it, and the person gets no "send it again" notice. It is
parked without a thread order entry, so the first person's own follow-ups
still steer their live turn. Each due wake re-runs it: while the other turn is
live it re-parks on the same record, and once that turn ends it runs on a new
sandbox booted for its own requester and the wait ends completed. A delivery
that cannot park keeps the pending ``RequesterHandoffPending`` deferral.

Real Valkey, the real kernel, consumer and substrate, in-process fake runners;
the identity core and the binding are the labelled doubles of
``test_requester_handoff``. No sleeps: waits are events and bounded polls, and
a parked delivery is made due by moving its due score, the store's own clock.

@spec CONNECTOR-REQUESTER-3.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

import pytest
from curie_worker.capacity_wait import CapacityWaitRequested, CapacityWaitStore
from curie_worker.consumer import Consumer
from curie_worker.delivery_lease import DeliveryLeaseStore

from ..queue_fixtures import qevent, wait_until
from .test_http_identity_authority import (
    CAPSULE,
    pending_entry,
    process,
    require_execution_interface,
)
from .test_requester_claim import require_requester_interface
from .test_requester_handoff import (
    _LEASE_KNOBS,
    EventCore,
    all_interrupts,
    all_opened,
    all_steers,
    any_turn_active,
    cct_claims,
    cct_of,
    complete_turns,
    event_parties,
    handoff_pending,
    handoff_wait,
    hold_turns,
    is_done,
    logged,
    pending_count,
    principal_dict,
    recorded,
    release,
    requester_digest,
    resume_of,
    route,
    runner_at,
    thread_key,
)

_NO_FIELD = "<CapacityWaitRecord has no such field>"
_RETURNED = "returned"


# -- Helpers ----------------------------------------------------------------------------------


def consumer_for(h: Any) -> Consumer:
    """The production consumer over the harness kernel. @spec CONNECTOR-REQUESTER-3."""
    return Consumer(
        redis=h.async_redis,
        kernel=h.kernel,
        config=h.config,
        leases=DeliveryLeaseStore(h.async_redis, h.config),
    )


def observe_outcomes(h: Any) -> dict[str, list[object]]:
    """Record what each ``process_event`` call ended with, per event id.

    ``"returned"`` or the exception class the real kernel raised to the
    consumer; the call itself is delegated unchanged. @spec CONNECTOR-REQUESTER-3.
    """
    real = h.kernel.process_event
    outcomes: dict[str, list[object]] = {}

    async def observed(turn: Any, **kwargs: Any) -> None:
        try:
            await real(turn, **kwargs)
        except BaseException as exc:
            outcomes.setdefault(turn.event_id, []).append(type(exc))
            raise
        outcomes.setdefault(turn.event_id, []).append(_RETURNED)

    h.kernel.process_event = observed
    return outcomes


async def deliver(
    h: Any, consumer: Consumer, core: EventCore, event: Any, requester: dict[str, str]
) -> str:
    """Hand one HTTP-context entry to the consumer, its V2 answers naming ``requester``.

    @spec CONNECTOR-REQUESTER-3.
    """
    core.by_event[event.event_id] = requester
    fields = {"payload": event.model_dump_json(), "curie_identity_context_v1": CAPSULE}
    entry_id, observed = await pending_entry(h, consumer, fields)
    await consumer._handle(entry_id, observed)
    return entry_id


async def wake(h: Any, waits: CapacityWaitStore, event_id: str) -> tuple[str, dict[str, str]]:
    """Make the parked delivery due and read the entry its wake appends.

    @spec CONNECTOR-REQUESTER-3.
    """
    await h.async_redis.zadd(waits._due, {event_id: 0})
    assert await waits.wake_due() == 1, "the parked delivery did not wake"
    rows = await h.async_redis.xreadgroup(
        h.config.consumer_group, h.config.consumer_name, {h.config.stream: ">"}, count=1
    )
    entry_id, fields = rows[0][1][0]
    return entry_id, dict(fields)


def pending_line(event_id: str, key: str) -> str:
    """The deferral log line, verbatim. @spec CONNECTOR-REQUESTER-3."""
    return (
        f"requester handoff pending for event {event_id} on thread {key}: "
        "a different requester's turn is live; deferring without steering"
    )


def not_started_notices(h: Any) -> list[str]:
    """Every "problem ... send it again" edit or post the sink saw.

    @spec CONNECTOR-REQUESTER-3.
    """
    notice = h.config.turn_not_started_text
    texts = [text for _a, _r, text in h.sink.updates] + [text for _a, _r, text in h.sink.text_posts]
    return [text for text in texts if notice in text]


async def dead_letters(h: Any) -> int:
    """@spec CONNECTOR-REQUESTER-3."""
    return int(await h.async_redis.xlen(h.config.dead_letter_stream_name()))


# -- Parking and the first person's follow-ups ------------------------------------------------


def test_a_parkable_deferral_parks_durably_while_the_first_human_keeps_steering(
    make_harness, caplog: pytest.LogCaptureFixture
) -> None:
    """@spec CONNECTOR-REQUESTER-3: Bob's delivery waits in the store, Alice still steers.

    Alice's turn is live. The consumer handles Bob's message on the same
    thread: the kernel raises ``RequesterHandoffWaitRequested`` (after the
    verbatim deferral line), and the consumer parks it with reason
    ``requester_handoff``, logs ``cause=requester_handoff``, and acks the
    entry: nothing stays in the PEL for ``max_delivery`` to dead-letter,
    nothing reaches either runner, and no not-started notice is sent. Alice's
    own follow-up, delivered through the same consumer, still steers into her
    live turn instead of waiting behind Bob's parked delivery.
    """
    require_execution_interface()
    require_requester_interface()
    wait_cls = handoff_wait()
    caplog.set_level(logging.DEBUG, logger="curie_worker")

    async def run() -> None:
        binding, alice, bob, core = event_parties()
        async with make_harness(
            binding=binding,
            identity_authority=core,
            per_sandbox_runners=2,
            max_delivery=2,
            **_LEASE_KNOBS,
        ) as h:
            consumer = consumer_for(h)
            waits = CapacityWaitStore(h.async_redis, h.config)
            outcomes = observe_outcomes(h)
            holds = hold_turns(h)
            alice_turn = qevent("alice is working", thread="park-steer")
            key = thread_key(alice_turn)
            opener = asyncio.create_task(process(h, alice_turn))
            bob_turn = qevent("bob asks", thread="park-steer")
            follow_up = qevent("alice follows up", thread="park-steer")
            try:
                await wait_until(lambda: any_turn_active(h), "alice's turn is live")
                alice_handle = route(h, key)
                alice_runner = runner_at(h, alice_handle)

                bob_entry = await deliver(h, consumer, core, bob_turn, bob)

                assert outcomes.get(bob_turn.event_id) == [wait_cls]
                assert "bob asks" not in all_opened(h)
                assert all_steers(h) == [] and all_interrupts(h) == 0
                assert alice_runner.turn_active and not opener.done()
                assert len(h.fake_k8s.claim_envs) == 1
                assert route(h, key) == alice_handle
                assert await pending_count(h) == 0, "the parked entry was left in the PEL"
                assert await dead_letters(h) == 0
                assert not await is_done(h, bob_turn)
                parked = await waits.get(bob_turn.event_id)
                assert parked is not None and parked.state == "waiting"
                assert parked.deferrals == 1
                assert getattr(parked, "reason", _NO_FIELD) == "requester_handoff"
                assert logged(caplog, pending_line(bob_turn.event_id, key))
                assert logged(
                    caplog,
                    f"capacity wait parked event_id={bob_turn.event_id} cause=requester_handoff",
                )
                assert not logged(caplog, f"processing failed for entry {bob_entry}")
                assert not_started_notices(h) == []

                await deliver(h, consumer, core, follow_up, alice)

                assert alice_runner.steers == ["alice follows up"], (
                    "Alice's follow-up waited behind Bob's parked delivery"
                )
                assert all_steers(h) == ["alice follows up"]
                assert outcomes.get(follow_up.event_id) == [_RETURNED]
                assert await waits.get(follow_up.event_id) is None
                assert await pending_count(h) == 0
                still = await waits.get(bob_turn.event_id)
                assert still is not None and still.state == "waiting"
                assert still.deferrals == 1
                assert all_opened(h) == ["alice is working"]
                assert all_interrupts(h) == 0
            finally:
                release(holds)
                await opener

    asyncio.run(run())


# -- Re-park while live, then a turn of its own ---------------------------------------------


def test_a_parked_deferral_reparks_while_live_then_runs_on_its_own_sandbox(
    make_harness, caplog: pytest.LogCaptureFixture
) -> None:
    """@spec CONNECTOR-REQUESTER-3: each wake re-parks, the last one runs once.

    Bob's parked delivery is woken twice while Alice's turn is still live.
    Each wake asks again for a requester handoff wait, and re-parks on the
    same record outside the thread order (same first wait and deadline, a
    newer generation, one more deferral), always acked: three deliveries
    against a ``max_delivery`` of two dead-letter nothing. Liveness: once her
    turn ends, the next wake runs Bob's turn once on a new sandbox whose cct
    names Bob, and the wait record ends terminal, ``done``, logged
    ``cause=completed``.
    """
    require_execution_interface()
    require_requester_interface()
    wait_cls = handoff_wait()
    caplog.set_level(logging.DEBUG, logger="curie_worker")

    async def run() -> None:
        binding, _alice, bob, core = event_parties()
        async with make_harness(
            binding=binding,
            identity_authority=core,
            per_sandbox_runners=2,
            max_delivery=2,
            **_LEASE_KNOBS,
        ) as h:
            consumer = consumer_for(h)
            waits = CapacityWaitStore(h.async_redis, h.config)
            outcomes = observe_outcomes(h)
            holds = hold_turns(h)
            alice_turn = qevent("alice is working", thread="repark")
            key = thread_key(alice_turn)
            opener = asyncio.create_task(process(h, alice_turn))
            bob_turn = qevent("bob asks", thread="repark")
            try:
                await wait_until(lambda: any_turn_active(h), "alice's turn is live")
                alice_handle = route(h, key)
                await deliver(h, consumer, core, bob_turn, bob)
                first = await waits.get(bob_turn.event_id)
                assert first is not None and first.state == "waiting"

                previous = first
                for deferral in (2, 3):
                    woken_id, woken = await wake(h, waits, bob_turn.event_id)
                    await consumer._handle(woken_id, woken)
                    again = await waits.get(bob_turn.event_id)
                    assert again is not None and again.state == "waiting"
                    assert again.deferrals == deferral
                    assert again.first_wait_ms == first.first_wait_ms
                    assert again.deadline_ms == first.deadline_ms
                    assert again.generation > previous.generation
                    assert await pending_count(h) == 0
                    previous = again

                seen = outcomes[bob_turn.event_id]
                assert len(seen) == 3
                assert seen[0] is wait_cls
                assert all(kind is wait_cls for kind in seen[1:]), seen
                assert getattr(previous, "reason", _NO_FIELD) == "requester_handoff"
                assert all_opened(h) == ["alice is working"]
                assert all_steers(h) == [] and all_interrupts(h) == 0
                assert len(h.fake_k8s.claim_envs) == 1
                assert await dead_letters(h) == 0
                assert not_started_notices(h) == []
            finally:
                release(holds)
                await opener

            complete_turns(h)
            woken_id, woken = await wake(h, waits, bob_turn.event_id)
            await consumer._handle(woken_id, woken)

            assert outcomes[bob_turn.event_id][-1] == _RETURNED
            assert len(h.fake_k8s.claim_envs) == 2
            bob_handle = route(h, key)
            assert bob_handle.claim_name != alice_handle.claim_name
            claims = cct_claims(cct_of(h.fake_k8s.claim_envs[1]))
            assert claims["principal"] == principal_dict(bob)
            assert recorded(bob_handle) == (requester_digest(bob), claims["exp"])
            assert runner_at(h, bob_handle).opened == ["bob asks"]
            assert runner_at(h, alice_handle).opened == ["alice is working"]
            assert all_opened(h).count("bob asks") == 1
            assert all_steers(h) == [] and all_interrupts(h) == 0
            assert await is_done(h, bob_turn)
            settled = await waits.get(bob_turn.event_id)
            assert settled is not None and settled.state == "done"
            assert logged(
                caplog, f"capacity wait terminal event_id={bob_turn.event_id} cause=completed"
            )
            assert await pending_count(h) == 0
            assert await dead_letters(h) == 0
            assert not_started_notices(h) == []

    asyncio.run(run())


# -- Regressions -----------------------------------------------------------------------------


def test_a_capacity_wait_that_meets_another_requesters_live_turn_becomes_a_handoff_wait(
    make_harness, caplog: pytest.LogCaptureFixture
) -> None:
    """@spec CONNECTOR-REQUESTER-3 (regression): an ordinary wait changes its reason.

    Bob's delivery is already parked as an ordinary capacity wait, registered
    in the thread's order, when Alice's turn goes live. Its wake meets her live
    turn: the kernel asks for a requester handoff wait, so the consumer
    releases Bob's order slot and re-parks the same record with reason
    ``requester_handoff``. Alice's own follow-up then steers her live turn
    instead of parking behind Bob. Liveness: once her turn ends, Bob's next
    wake runs once on a new sandbox whose cct names Bob.
    """
    require_execution_interface()
    require_requester_interface()
    wait_cls = handoff_wait()
    caplog.set_level(logging.DEBUG, logger="curie_worker")

    async def run() -> None:
        binding, alice, bob, core = event_parties()
        async with make_harness(
            binding=binding,
            identity_authority=core,
            per_sandbox_runners=2,
            max_delivery=2,
            **_LEASE_KNOBS,
        ) as h:
            consumer = consumer_for(h)
            leases = DeliveryLeaseStore(h.async_redis, h.config)
            waits = CapacityWaitStore(h.async_redis, h.config)
            outcomes = observe_outcomes(h)
            holds = hold_turns(h)
            alice_turn = qevent("alice is working", thread="capacity-handoff")
            key = thread_key(alice_turn)
            bob_turn = qevent("bob asks", thread="capacity-handoff")
            follow_up = qevent("alice follows up", thread="capacity-handoff")
            order_key = waits._thread_order(key)

            def ordered(event_id: str) -> bool:
                return any(
                    (member.decode() if isinstance(member, bytes) else str(member)).endswith(
                        "|" + event_id
                    )
                    for member in members
                )

            core.by_event[bob_turn.event_id] = bob
            fields = {"payload": bob_turn.model_dump_json(), "curie_identity_context_v1": CAPSULE}
            entry_id, observed = await pending_entry(h, consumer, fields)
            lease = await leases.acquire(
                h.config.stream,
                h.config.consumer_group,
                entry_id,
                consumer=h.config.consumer_name,
            )
            try:
                first = await waits.park(
                    entry_id, observed, bob_turn.event_id, lease, thread_key=key
                )
            finally:
                await leases.release(
                    h.config.stream,
                    h.config.consumer_group,
                    entry_id,
                    owner=lease.owner,
                    resume_event_id=None,
                )
            assert first.state == "waiting" and getattr(first, "reason", _NO_FIELD) == ""
            members = await h.async_redis.zrange(order_key, 0, -1)
            assert ordered(bob_turn.event_id), "the capacity park was not in the thread order"

            opener = asyncio.create_task(process(h, alice_turn))
            try:
                await wait_until(lambda: any_turn_active(h), "alice's turn is live")
                alice_handle = route(h, key)
                alice_runner = runner_at(h, alice_handle)

                woken_id, woken = await wake(h, waits, bob_turn.event_id)
                await consumer._handle(woken_id, woken)

                assert outcomes.get(bob_turn.event_id) == [wait_cls]
                again = await waits.get(bob_turn.event_id)
                assert again is not None and again.state == "waiting"
                assert getattr(again, "reason", _NO_FIELD) == "requester_handoff"
                assert again.first_wait_ms == first.first_wait_ms
                assert again.deadline_ms == first.deadline_ms
                members = await h.async_redis.zrange(order_key, 0, -1)
                assert not ordered(bob_turn.event_id), "bob's wait kept its thread order slot"
                assert logged(
                    caplog,
                    f"capacity wait parked event_id={bob_turn.event_id} cause=requester_handoff",
                )
                assert all_steers(h) == [] and all_interrupts(h) == 0
                assert await pending_count(h) == 0

                await deliver(h, consumer, core, follow_up, alice)

                assert alice_runner.steers == ["alice follows up"], (
                    "Alice's follow-up waited behind Bob's wait"
                )
                assert outcomes.get(follow_up.event_id) == [_RETURNED]
                assert await waits.get(follow_up.event_id) is None
            finally:
                release(holds)
                await opener

            complete_turns(h)
            woken_id, woken = await wake(h, waits, bob_turn.event_id)
            await consumer._handle(woken_id, woken)

            assert outcomes[bob_turn.event_id][-1] == _RETURNED
            bob_handle = route(h, key)
            assert bob_handle.claim_name != alice_handle.claim_name
            claims = cct_claims(cct_of(h.fake_k8s.claim_envs[-1]))
            assert claims["principal"] == principal_dict(bob)
            assert runner_at(h, bob_handle).opened == ["bob asks"]
            assert all_opened(h).count("bob asks") == 1
            assert await is_done(h, bob_turn)
            assert await dead_letters(h) == 0

    asyncio.run(run())


def test_a_delivery_that_cannot_park_keeps_the_pending_deferral(make_harness) -> None:
    """@spec CONNECTOR-REQUESTER-3 (regression): an approval resume cannot park.

    Bob's approval resume meets Alice's live turn. It is no capacity wait: it
    keeps ``RequesterHandoffPending``, so it stays pending for redelivery, with
    no wait record, no steer and no interrupt. Liveness: once Alice's turn
    ends, the same resume runs once on a new sandbox whose cct names Bob.
    """
    require_execution_interface()
    require_requester_interface()
    pending_cls = handoff_pending()

    async def run() -> None:
        binding, _alice, bob, core = event_parties()
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=2
        ) as h:
            waits = CapacityWaitStore(h.async_redis, h.config)
            holds = hold_turns(h)
            alice_turn = qevent("alice is working", thread="no-park")
            key = thread_key(alice_turn)
            opener = asyncio.create_task(process(h, alice_turn))
            resumed = resume_of(str(uuid.uuid4()), "no-park")
            core.by_event[resumed.event_id] = bob
            try:
                await wait_until(lambda: any_turn_active(h), "alice's turn is live")
                alice_handle = route(h, key)
                with pytest.raises(pending_cls) as raised:
                    await process(h, resumed)
                assert not isinstance(raised.value, CapacityWaitRequested)
                assert all_steers(h) == [] and all_interrupts(h) == 0
                assert all_opened(h) == ["alice is working"]
                assert len(h.fake_k8s.claim_envs) == 1
                assert route(h, key) == alice_handle
                assert await waits.get(resumed.event_id) is None
                assert not await is_done(h, resumed)
            finally:
                release(holds)
                await opener

            complete_turns(h)
            await process(h, resumed)

            bob_handle = route(h, key)
            assert bob_handle.claim_name != alice_handle.claim_name
            assert recorded(bob_handle)[0] == requester_digest(bob)
            claims = cct_claims(cct_of(h.fake_k8s.claim_envs[-1]))
            assert claims["principal"] == principal_dict(bob)
            assert runner_at(h, bob_handle).opened == [resumed.text]
            assert all_opened(h).count(resumed.text) == 1
            assert await is_done(h, resumed)

    asyncio.run(run())


def test_a_capacity_park_records_no_reason(make_harness, caplog: pytest.LogCaptureFixture) -> None:
    """@spec CONNECTOR-REQUESTER-3 (regression): ``reason`` is "" on every other record.

    A capacity park through the store's existing signature keeps its log line
    and reads back an empty reason, and so does a record written before the
    field existed (the field removed from the stored hash).
    """
    caplog.set_level(logging.DEBUG, logger="curie_worker")

    async def run() -> None:
        async with make_harness() as h:
            consumer = consumer_for(h)
            leases = DeliveryLeaseStore(h.async_redis, h.config)
            waits = CapacityWaitStore(h.async_redis, h.config)
            event = qevent("capacity park", thread="capacity-reason")
            entry_id, observed = await pending_entry(
                h, consumer, {"payload": event.model_dump_json()}
            )
            lease = await leases.acquire(
                h.config.stream,
                h.config.consumer_group,
                entry_id,
                consumer=h.config.consumer_name,
            )
            try:
                record = await waits.park(entry_id, observed, event.event_id, lease)
            finally:
                await leases.release(
                    h.config.stream,
                    h.config.consumer_group,
                    entry_id,
                    owner=lease.owner,
                    resume_event_id=None,
                )
            assert record.state == "waiting"
            assert getattr(record, "reason", _NO_FIELD) == ""
            assert logged(caplog, f"capacity wait parked event_id={event.event_id} cause=capacity")

            await h.async_redis.hdel(waits._record(event.event_id), "reason")
            legacy = await waits.get(event.event_id)
            assert legacy is not None and legacy.state == "waiting"
            assert getattr(legacy, "reason", _NO_FIELD) == ""

    asyncio.run(run())
