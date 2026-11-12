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
from curie_worker import protected_recovery
from curie_worker.capacity_wait import (
    REQUESTER_HANDOFF_REASON,
    WAIT_REASON_ADMISSION,
    WAIT_REASON_PREDECESSOR,
    CapacityWaitRefused,
    CapacityWaitRequested,
    CapacityWaitStore,
    durable_wait_notice,
)
from curie_worker.consumer import Consumer, refuse_unconfigured_reminder
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
        reminders=refuse_unconfigured_reminder,
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

    A capacity park through the store's existing signature keeps its log line,
    reads back an empty reason, and stores no ``reason`` field at all: its hash
    keeps the field set the protected wait store admits before it schedules a
    wake (a stored empty field made every protected wait unschedulable). That
    hash is also the shape of a record written before the field existed.
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

            stored = await h.async_redis.hgetall(waits._record(event.event_id))
            assert "reason" not in stored
            assert set(stored) <= protected_recovery._WAIT_HASH_FIELDS

    asyncio.run(run())


# -- The notice cause and the order reason are independent (forward merge) ---


def _text(value: object) -> str:
    """A Valkey reply as text. @spec CONNECTOR-REQUESTER-3."""
    return value.decode() if isinstance(value, bytes) else str(value)


async def order_count(h: Any, key: str, event_id: str) -> int:
    """How many thread order members (``<rank>|<event_id>``) name ``event_id``.

    @spec CONNECTOR-REQUESTER-3.
    """
    members = await h.async_redis.zrange(h.config.capacity_thread_order_key(key), 0, -1)
    return sum(1 for member in members if _text(member).endswith("|" + event_id))


async def stored_hash(h: Any, waits: CapacityWaitStore, event_id: str) -> dict[str, str]:
    """The wait record hash as stored. @spec CONNECTOR-REQUESTER-3."""
    stored = await h.async_redis.hgetall(waits._record(event_id))
    return {_text(name): _text(value) for name, value in stored.items()}


async def stored_cause(h: Any, waits: CapacityWaitStore, event_id: str) -> str | None:
    """The durable notice cause, from its own key outside the hash. @spec CONNECTOR-REQUESTER-3."""
    raw = await h.async_redis.get(waits._reason(event_id))
    return None if raw is None else _text(raw)


def test_a_handoff_wait_keeps_its_notice_cause_and_its_order_reason_apart(
    make_harness,
) -> None:
    """@spec CONNECTOR-REQUESTER-3 (forward merge): two fields, two park arguments.

    Alice's turn is live and Bob's delivery parks through the real consumer
    as a requester handoff. The one record carries both fields apart: the
    order ``reason`` is ``requester_handoff`` in the hash, and the notice
    cause ``wait_reason`` is ``sandbox_admission`` in its own key. Bob never
    joins the thread order. A wake while Alice is still live re-parks with
    both kept (same deadline, one more deferral), and Alice's follow-up still
    steers her turn. Liveness: once her turn ends, Bob runs once on a sandbox
    recorded for his requester, and the wait ends ``done``.

    Catches the park script reading the notice cause as the order reason
    (``local reason = ARGV[15]``): the handoff would store and read back
    reason ``sandbox_admission`` instead of ``requester_handoff``, losing the
    sticky marker that keeps its wakes out of the thread order.
    """
    require_execution_interface()
    require_requester_interface()
    wait_cls = handoff_wait()

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
            alice_turn = qevent("alice is working", thread="cause-apart")
            key = thread_key(alice_turn)
            opener = asyncio.create_task(process(h, alice_turn))
            bob_turn = qevent("bob asks", thread="cause-apart")
            follow_up = qevent("alice follows up", thread="cause-apart")
            try:
                await wait_until(lambda: any_turn_active(h), "alice's turn is live")
                alice_handle = route(h, key)
                alice_runner = runner_at(h, alice_handle)

                await deliver(h, consumer, core, bob_turn, bob)

                assert outcomes.get(bob_turn.event_id) == [wait_cls]
                parked = await waits.get(bob_turn.event_id)
                assert parked is not None and parked.state == "waiting"
                assert parked.reason == REQUESTER_HANDOFF_REASON
                assert parked.wait_reason == WAIT_REASON_ADMISSION
                assert await stored_cause(h, waits, bob_turn.event_id) == WAIT_REASON_ADMISSION
                stored = await stored_hash(h, waits, bob_turn.event_id)
                assert stored.get("reason") == REQUESTER_HANDOFF_REASON
                assert await order_count(h, key, bob_turn.event_id) == 0, (
                    "a handoff wait joined the thread order"
                )
                # The notice cause resolves to a known notice. Bob's delivery
                # carries an HTTP identity context, so that courtesy is
                # suppressed and never reaches the sink.
                notice = durable_wait_notice(parked.wait_reason)
                sink_texts = [text for _a, _r, text in h.sink.updates] + [
                    text for _a, _r, text in h.sink.text_posts
                ]
                assert all(notice not in text for text in sink_texts)
                assert all_steers(h) == [] and all_interrupts(h) == 0

                woken_id, woken = await wake(h, waits, bob_turn.event_id)
                await consumer._handle(woken_id, woken)

                assert outcomes[bob_turn.event_id] == [wait_cls, wait_cls]
                again = await waits.get(bob_turn.event_id)
                assert again is not None and again.state == "waiting"
                assert again.reason == REQUESTER_HANDOFF_REASON
                assert again.wait_reason == WAIT_REASON_ADMISSION
                assert await stored_cause(h, waits, bob_turn.event_id) == WAIT_REASON_ADMISSION
                stored = await stored_hash(h, waits, bob_turn.event_id)
                assert stored.get("reason") == REQUESTER_HANDOFF_REASON
                assert again.deadline_ms == parked.deadline_ms
                assert again.deferrals == parked.deferrals + 1
                assert await order_count(h, key, bob_turn.event_id) == 0, (
                    "a re-parked handoff wait joined the thread order"
                )
                assert await pending_count(h) == 0

                await deliver(h, consumer, core, follow_up, alice)

                assert alice_runner.steers == ["alice follows up"], (
                    "Alice's follow-up waited behind Bob's handoff wait"
                )
                assert all_steers(h) == ["alice follows up"]
                assert all_interrupts(h) == 0
                assert outcomes.get(follow_up.event_id) == [_RETURNED]
                assert await waits.get(follow_up.event_id) is None
                assert all_opened(h) == ["alice is working"]
            finally:
                release(holds)
                await opener

            complete_turns(h)
            woken_id, woken = await wake(h, waits, bob_turn.event_id)
            await consumer._handle(woken_id, woken)

            assert outcomes[bob_turn.event_id][-1] == _RETURNED
            bob_handle = route(h, key)
            assert bob_handle.claim_name != alice_handle.claim_name
            assert recorded(bob_handle)[0] == requester_digest(bob)
            assert runner_at(h, bob_handle).opened == ["bob asks"]
            assert all_opened(h).count("bob asks") == 1
            assert all_steers(h) == ["alice follows up"] and all_interrupts(h) == 0
            assert await is_done(h, bob_turn)
            settled = await waits.get(bob_turn.event_id)
            assert settled is not None and settled.state == "done"
            assert await pending_count(h) == 0
            assert await dead_letters(h) == 0

    asyncio.run(run())


def test_an_ordered_wait_stores_its_cause_without_a_reason_field(make_harness) -> None:
    """@spec CONNECTOR-REQUESTER-3 (forward merge): an ordered wait's cause is not a reason.

    A delivery parked into the thread order with the notice cause
    ``thread_predecessor`` reads back that cause, an empty order reason, no
    ``reason`` hash field, and exactly one thread order member. Woken and
    re-parked with the cause ``sandbox_admission`` on the same thread, the new
    cause is stored and re-arms the notice, and the record still has no
    ``reason`` field and one order member. An ordered park writes its
    ``order_token`` into the hash; the protected lane never parks with a thread
    key, so its field allowlist is not asserted here.

    Catches the park script reading the notice cause as the order reason
    (``local reason = ARGV[15]``): every park would write ``reason`` =
    ``thread_predecessor`` or ``sandbox_admission`` into the hash.
    """

    async def run() -> None:
        async with make_harness() as h:
            consumer = consumer_for(h)
            leases = DeliveryLeaseStore(h.async_redis, h.config)
            waits = CapacityWaitStore(h.async_redis, h.config)
            event = qevent("ordered wait", thread="ordered-cause")
            key = thread_key(event)
            entry_id, observed = await pending_entry(
                h, consumer, {"payload": event.model_dump_json()}
            )

            async def park_owned(entry: str, fields: dict[str, str], wait_reason: str) -> Any:
                lease = await leases.acquire(
                    h.config.stream,
                    h.config.consumer_group,
                    entry,
                    consumer=h.config.consumer_name,
                )
                try:
                    return await waits.park(
                        entry,
                        fields,
                        event.event_id,
                        lease,
                        thread_key=key,
                        wait_reason=wait_reason,
                    )
                finally:
                    await leases.release(
                        h.config.stream,
                        h.config.consumer_group,
                        entry,
                        owner=lease.owner,
                        resume_event_id=None,
                    )

            first = await park_owned(entry_id, observed, WAIT_REASON_PREDECESSOR)

            assert first.state == "waiting"
            assert first.reason == ""
            assert first.wait_reason == WAIT_REASON_PREDECESSOR
            assert await stored_cause(h, waits, event.event_id) == WAIT_REASON_PREDECESSOR
            stored = await stored_hash(h, waits, event.event_id)
            assert "reason" not in stored
            assert await order_count(h, key, event.event_id) == 1

            woken_id, woken = await wake(h, waits, event.event_id)
            again = await park_owned(woken_id, woken, WAIT_REASON_ADMISSION)

            assert again.state == "waiting"
            assert again.generation > first.generation
            assert again.reason == ""
            assert again.wait_reason == WAIT_REASON_ADMISSION
            assert again.notice_pending is True
            assert await stored_cause(h, waits, event.event_id) == WAIT_REASON_ADMISSION
            stored = await stored_hash(h, waits, event.event_id)
            assert "reason" not in stored
            assert await order_count(h, key, event.event_id) == 1

    asyncio.run(run())


def test_an_order_reason_is_refused_as_a_notice_cause_before_any_write(make_harness) -> None:
    """@spec CONNECTOR-REQUESTER-3 (forward merge): ``requester_handoff`` is no notice cause.

    Parking an owned delivery with ``wait_reason=requester_handoff`` raises
    ``CapacityWaitRefused`` before Valkey is touched: no wait record, no cause
    key, no thread order member, and the entry is still pending in the
    consumer group. Liveness: the same entry under the same lease then parks
    with the cause ``sandbox_admission`` and reads back waiting with it.

    Catches a merge that swaps the two park arguments or drops the
    ``wait_reason`` validation, so the order reason lands in the notice cause
    key, where ``durable_wait_notice`` cannot resolve it, and the entry is
    acked. It does not catch the ``ARGV[15]`` script revert, which the other
    tests in this section do.
    """

    async def run() -> None:
        async with make_harness() as h:
            consumer = consumer_for(h)
            leases = DeliveryLeaseStore(h.async_redis, h.config)
            waits = CapacityWaitStore(h.async_redis, h.config)
            event = qevent("refused cause", thread="refused-cause")
            key = thread_key(event)
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
                with pytest.raises(CapacityWaitRefused):
                    await waits.park(
                        entry_id,
                        observed,
                        event.event_id,
                        lease,
                        thread_key=key,
                        wait_reason=REQUESTER_HANDOFF_REASON,
                    )

                assert await waits.get(event.event_id) is None
                assert await h.async_redis.exists(waits._reason(event.event_id)) == 0
                assert await h.async_redis.exists(waits._record(event.event_id)) == 0
                assert await order_count(h, key, event.event_id) == 0
                rows = await h.async_redis.xpending_range(
                    h.config.stream,
                    h.config.consumer_group,
                    min=entry_id,
                    max=entry_id,
                    count=1,
                )
                assert len(rows) == 1, "the refused park acked the entry"

                record = await waits.park(
                    entry_id,
                    observed,
                    event.event_id,
                    lease,
                    thread_key=key,
                    wait_reason=WAIT_REASON_ADMISSION,
                )
            finally:
                await leases.release(
                    h.config.stream,
                    h.config.consumer_group,
                    entry_id,
                    owner=lease.owner,
                    resume_event_id=None,
                )

            assert record.state == "waiting"
            assert record.wait_reason == WAIT_REASON_ADMISSION
            assert record.reason == ""
            parked = await waits.get(event.event_id)
            assert parked is not None and parked.state == "waiting"
            assert parked.wait_reason == WAIT_REASON_ADMISSION
            assert await stored_cause(h, waits, event.event_id) == WAIT_REASON_ADMISSION
            assert await order_count(h, key, event.event_id) == 1
            assert await pending_count(h) == 0

    asyncio.run(run())


def test_a_handoff_wait_reparked_as_ordered_capacity_keeps_out_of_the_thread_order(
    make_harness,
) -> None:
    """@spec CONNECTOR-REQUESTER-3 (forward merge): the sticky handoff reason outranks a key.

    Bob's delivery parks as a requester handoff with no thread key, as the
    consumer parks one. Woken, it re-parks the way the consumer's plain
    capacity path does: with the thread's key, the cause
    ``thread_predecessor`` and no reason. The stored ``requester_handoff``
    reason is sticky, so the record keeps it (hash field included) and stays
    out of the thread order, while the new notice cause is stored and re-arms
    the notice. Control: a second delivery on the same thread, parked with
    that key and no reason, joins the order exactly once.

    Catches the reverted park script (``local reason = ARGV[15]`` and
    ``reason ~= ARGV[16]``): the re-park reads the notice cause as the order
    reason, which is not the handoff constant, so the wait joins the thread
    order and the live requester's own follow-ups queue behind a different
    requester.
    """

    async def run() -> None:
        async with make_harness() as h:
            consumer = consumer_for(h)
            leases = DeliveryLeaseStore(h.async_redis, h.config)
            waits = CapacityWaitStore(h.async_redis, h.config)
            handoff = qevent("bob asks", thread="handoff-ordered")
            other = qevent("carol asks", thread="handoff-ordered")
            key = thread_key(handoff)

            async def park_owned(
                entry: str, fields: dict[str, str], event_id: str, **knobs: Any
            ) -> Any:
                lease = await leases.acquire(
                    h.config.stream,
                    h.config.consumer_group,
                    entry,
                    consumer=h.config.consumer_name,
                )
                try:
                    return await waits.park(entry, fields, event_id, lease, **knobs)
                finally:
                    await leases.release(
                        h.config.stream,
                        h.config.consumer_group,
                        entry,
                        owner=lease.owner,
                        resume_event_id=None,
                    )

            entry_id, observed = await pending_entry(
                h, consumer, {"payload": handoff.model_dump_json()}
            )
            first = await park_owned(
                entry_id,
                observed,
                handoff.event_id,
                thread_key=None,
                reason=REQUESTER_HANDOFF_REASON,
            )
            assert first.state == "waiting"
            assert first.reason == REQUESTER_HANDOFF_REASON
            assert first.wait_reason == WAIT_REASON_ADMISSION
            assert await order_count(h, key, handoff.event_id) == 0

            woken_id, woken = await wake(h, waits, handoff.event_id)
            record = await park_owned(
                woken_id,
                woken,
                handoff.event_id,
                thread_key=key,
                wait_reason=WAIT_REASON_PREDECESSOR,
                reason="",
            )

            assert record.state == "waiting"
            assert record.generation > first.generation
            assert record.reason == REQUESTER_HANDOFF_REASON, "the handoff reason was not sticky"
            stored = await stored_hash(h, waits, handoff.event_id)
            assert stored.get("reason") == REQUESTER_HANDOFF_REASON
            assert record.wait_reason == WAIT_REASON_PREDECESSOR
            assert await stored_cause(h, waits, handoff.event_id) == WAIT_REASON_PREDECESSOR
            assert record.notice_pending is True
            assert await order_count(h, key, handoff.event_id) == 0, (
                "a re-parked handoff wait joined the thread order"
            )

            other_entry, other_fields = await pending_entry(
                h, consumer, {"payload": other.model_dump_json()}
            )
            control = await park_owned(other_entry, other_fields, other.event_id, thread_key=key)
            assert control.state == "waiting" and control.reason == ""
            assert await order_count(h, key, other.event_id) == 1
            assert await order_count(h, key, handoff.event_id) == 0
            assert await pending_count(h) == 0

    asyncio.run(run())
