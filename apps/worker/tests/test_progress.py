"""The worker's durable progress store and delivery outbox (ADR 0130, #3463).

Against **real Valkey**, never a mock: every rule below is a property of one
Lua script's atomicity, and a mocked ``EVAL`` would only assert that we wrote
the Lua we wrote. The rules themselves are in the worker README's "Deliberate
progress (ADR 0130)" section; each test names the one it provokes.

The API this file pins, for the implementer of ``progress.py``:

    progress_id_for(thread_key, root_event_id) -> str
    card_delivery_id(pid) / card_update_delivery_id(pid, revision)
      / milestone_delivery_id(pid, ordinal) -> str
    progress_ttl_s(config) -> int

    ProgressStore(redis, config)
      .open_chain(thread_key, root_event_id, *, answer_ref=None) -> pid
      .chain_for_turn(thread_key, event_id, *, resume, answer_ref=None) -> pid | None
      .link_resume(resume_event_id, pid) -> bool
      .resolve_chain(event_id) -> pid | None
      .read(pid) -> ProgressRecord | None
      .apply_model_command(pid, command, *, epoch, seq, route, target) -> ProgressOutcome
      .apply_platform_update(pid, *, update_id, state, summary, epoch, route,
                             target, lease) -> ProgressOutcome
      .ack(delivery_id, *, generation, card_ref=None) -> bool
      .pending(limit) -> set[str]
      .read_delivery(delivery_id) -> StoredProgressDelivery | None
      .charge_attempt / .dead_letter / .drop_pending_member

    sweep_pending_progress(store, *, deliver, batch, budget_s, grace_s,
                           max_attempts) -> ProgressSweep
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from channel_protocol import (
    MilestoneClass,
    ProgressCard,
    ProgressCommand,
    ProgressMilestone,
    ProgressState,
)
from channel_protocol.reply import DeliveryId, ReplyTarget
from curie_test_support.valkey import (
    VALKEY_HOST as _VALKEY_HOST,
)
from curie_test_support.valkey import (
    VALKEY_PORT as _VALKEY_PORT,
)
from curie_test_support.valkey import (
    VALKEY_PW as _VALKEY_PW,
)
from curie_worker.config import WorkerConfig
from curie_worker.delivery_lease import DeliveryLease, DeliveryLeaseStore, unfenced_lease
from curie_worker.progress import (
    MAX_PROGRESS_UPDATES,
    MODEL_PROGRESS_STATES,
    PROGRESS_ID_NAMESPACE,
    ProgressStore,
    StoredProgressDelivery,
    card_delivery_id,
    card_update_delivery_id,
    milestone_delivery_id,
    progress_id_for,
    progress_ttl_s,
    sweep_pending_progress,
)
from curie_worker.reply_sink import TargetRoute
from pydantic import TypeAdapter
from redis.asyncio import Redis as AsyncRedis

# ``sync_redis`` and ``names`` (per-test-unique stream, group and key prefix on
# the shared Valkey) live in ``tests/conftest.py``.

_THREAD = "slack:C0EXAMPLE1:1700000000.000100"
_ROOT = "Ev0EXAMPLE1"
_RESUME = "approval-6f1c3f0e-3c1b-4c55-9e57-1d2a7d7b2f10-resolved"
_ROUTE = TargetRoute(adapter="acme-bot")
_TARGET = ReplyTarget(
    kind="slack",
    address="C0EXAMPLE1",
    conversation_id="1700000000.000100",
    reply_ref="1700000000.000200",
)
_PLATFORM_ONLY = (
    ProgressState.QUEUED,
    ProgressState.AWAITING_APPROVAL,
    ProgressState.COMPLETE,
    ProgressState.FAILED,
    ProgressState.CANCELLED,
)
_MODEL_STATES = (
    ProgressState.INVESTIGATING,
    ProgressState.PREPARING_WORKSPACE,
    ProgressState.TESTING,
    ProgressState.PUBLISHING,
)
_DELIVERY_ID = TypeAdapter(DeliveryId)


def _config(names: dict[str, str], **overrides: object) -> WorkerConfig:
    base: dict[str, object] = {
        "valkey_host": _VALKEY_HOST,
        "valkey_port": _VALKEY_PORT,
        "valkey_password": _VALKEY_PW,
        "stream": names["stream"],
        "consumer_group": names["group"],
        "key_prefix": names["prefix"],
        # Under the per-test prefix, so the ``names`` teardown removes it.
        "dead_letter_stream": f"{names['prefix']}:dead",
    }
    base.update(overrides)
    return WorkerConfig(**base)


def _client() -> AsyncRedis:
    return AsyncRedis(
        host=_VALKEY_HOST,
        port=_VALKEY_PORT,
        password=_VALKEY_PW or None,
        decode_responses=True,
    )


@contextlib.asynccontextmanager
async def _store(
    names: dict[str, str], **overrides: object
) -> AsyncIterator[tuple[ProgressStore, WorkerConfig, AsyncRedis]]:
    """A progress store on a live async client, torn down on every exit path."""
    config = _config(names, **overrides)
    client = _client()
    try:
        yield ProgressStore(client, config), config, client
    finally:
        with contextlib.suppress(Exception):
            await client.aclose()


def _command(
    update_id: str,
    *,
    state: ProgressState = ProgressState.INVESTIGATING,
    summary: str = "Reading the ledger",
    milestone: MilestoneClass | None = None,
) -> ProgressCommand:
    return ProgressCommand(
        version="1.0",
        update_id=update_id,
        state=state,
        summary=summary,
        milestone=milestone,
    )


async def _model(
    store: ProgressStore,
    pid: str,
    command: ProgressCommand,
    *,
    epoch: int = 1,
    seq: int,
) -> Any:
    return await store.apply_model_command(
        pid, command, epoch=epoch, seq=seq, route=_ROUTE, target=_TARGET
    )


async def _lease(client: AsyncRedis, config: WorkerConfig, consumer: str) -> DeliveryLease:
    """A real ADR-0131 lease over a real pending entry this consumer holds."""
    with contextlib.suppress(Exception):
        await client.xgroup_create(config.stream, config.consumer_group, id="0", mkstream=True)
    entry = await client.xadd(config.stream, {"payload": "p"})
    await client.xreadgroup(config.consumer_group, consumer, {config.stream: ">"}, count=1)
    return await DeliveryLeaseStore(client, config).acquire(
        config.stream, config.consumer_group, entry, consumer=consumer
    )


async def _platform(
    store: ProgressStore,
    pid: str,
    lease: DeliveryLease,
    *,
    update_id: str,
    state: ProgressState,
    summary: str = "Closed by the platform",
    epoch: int = 1,
) -> Any:
    return await store.apply_platform_update(
        pid,
        update_id=update_id,
        state=state,
        summary=summary,
        epoch=epoch,
        route=_ROUTE,
        target=_TARGET,
        lease=lease,
    )


async def _pending(client: AsyncRedis, config: WorkerConfig) -> set[str]:
    return set(await client.smembers(config.progress_pending_key()))


async def _graveyard(client: AsyncRedis, config: WorkerConfig) -> list[dict[str, str]]:
    rows = await client.xrange(config.dead_letter_stream_name())
    return [fields for _entry_id, fields in rows]


# --- pure identity helpers -----------------------------------------------------


def test_the_progress_id_is_the_documented_uuid5() -> None:
    """A redelivered root event must reopen its own record, so the id is derived,
    and the namespace is pinned: changing it would orphan every live chain."""
    assert PROGRESS_ID_NAMESPACE == uuid.UUID("d22277ad-b64c-43b9-a404-8141dda9859b")
    pid = progress_id_for(_THREAD, _ROOT)
    assert pid == str(uuid.uuid5(PROGRESS_ID_NAMESPACE, _THREAD + "\0" + _ROOT))
    assert progress_id_for(_THREAD, _ROOT) == pid
    # The separator is what keeps two different (thread, root) pairs apart.
    assert progress_id_for(_THREAD + "a", "b") != progress_id_for(_THREAD, "ab")
    assert progress_id_for(_THREAD, "Ev0EXAMPLE2") != pid


def test_delivery_ids_are_derived_canonical_uuids() -> None:
    pid = progress_id_for(_THREAD, _ROOT)
    base = uuid.UUID(pid)
    assert card_delivery_id(pid) == str(uuid.uuid5(base, "card"))
    assert card_update_delivery_id(pid, 7) == str(uuid.uuid5(base, "card:7"))
    assert milestone_delivery_id(pid, 2) == str(uuid.uuid5(base, "milestone:2"))
    ids = [card_delivery_id(pid)]
    ids += [card_update_delivery_id(pid, r) for r in range(2, 6)]
    ids += [milestone_delivery_id(pid, n) for n in range(1, 4)]
    assert len(set(ids)) == len(ids)
    for delivery_id in ids:
        # The reply wire's own DeliveryId type is the judge of "canonical".
        assert _DELIVERY_ID.validate_python(delivery_id) == delivery_id


def test_the_model_writes_exactly_the_four_working_states() -> None:
    assert frozenset(_MODEL_STATES) == MODEL_PROGRESS_STATES
    assert MODEL_PROGRESS_STATES.isdisjoint(_PLATFORM_ONLY)
    assert MODEL_PROGRESS_STATES | frozenset(_PLATFORM_ONLY) == frozenset(ProgressState)


def test_every_key_lives_at_least_as_long_as_an_approval_card(names) -> None:  # noqa: ANN001
    fourteen_days = 14 * 24 * 60 * 60
    assert progress_ttl_s(_config(names)) == fourteen_days
    thirty_days = 30 * 24 * 60 * 60
    assert progress_ttl_s(_config(names, completion_max_retention_s=thirty_days)) == thirty_days

    async def go() -> None:
        async with _store(names) as (store, config, client):
            pid = await store.open_chain(_THREAD, _ROOT)
            await _model(store, pid, _command("u1", milestone=MilestoneClass.EVIDENCE), seq=1)
            assert await store.link_resume(_RESUME, pid) is True
            keys = [
                config.progress_key(pid),
                config.progress_pending_key(),
                config.progress_chain_key(_RESUME),
                config.progress_delivery_key(card_delivery_id(pid)),
                config.progress_delivery_key(milestone_delivery_id(pid, 1)),
            ]
            for key in keys:
                ttl = await client.ttl(key)
                assert fourteen_days - 60 <= ttl <= fourteen_days, (key, ttl)

    asyncio.run(go())


# --- idempotency, ordering, and terminal monotonicity --------------------------


def test_the_same_update_twice_is_a_no_op(names) -> None:  # noqa: ANN001
    async def go() -> None:
        async with _store(names) as (store, config, client):
            pid = await store.open_chain(_THREAD, _ROOT)
            first = await _model(store, pid, _command("u1"), seq=1)
            assert first.status == "applied"
            assert first.revision == 1
            assert first.deliveries == (card_delivery_id(pid),)
            stored = await store.read_delivery(card_delivery_id(pid))
            assert stored is not None

            again = await _model(store, pid, _command("u1"), seq=1)
            # An ingress retry that was handed a fresh seq is still the same update.
            retried = await _model(store, pid, _command("u1"), seq=2)

            for outcome in (again, retried):
                assert outcome.status == "duplicate"
                assert outcome.reason is None
                assert outcome.deliveries == ()
            record = await store.read(pid)
            assert record is not None
            assert (record.revision, record.update_count, record.last_seq) == (1, 1, 1)
            assert await _pending(client, config) == {card_delivery_id(pid)}
            unchanged = await store.read_delivery(card_delivery_id(pid))
            assert unchanged is not None
            assert unchanged.generation == stored.generation

    asyncio.run(go())


def test_a_stale_update_cannot_overwrite_a_newer_one(names) -> None:  # noqa: ANN001
    """The (epoch, seq) order: late, equal and older-epoch commands are refused
    and leave the newer state in place; a newer epoch restarts the sequence."""

    async def go() -> None:
        async with _store(names) as (store, _config, _client):
            pid = await store.open_chain(_THREAD, _ROOT)
            assert (await _model(store, pid, _command("u1"), seq=1)).status == "applied"
            newer = await _model(store, pid, _command("u2", state=ProgressState.TESTING), seq=3)
            assert (newer.status, newer.revision) == ("applied", 2)

            late = await _model(store, pid, _command("u3", state=ProgressState.PUBLISHING), seq=2)
            equal = await _model(store, pid, _command("u4", state=ProgressState.PUBLISHING), seq=3)
            assert (late.status, late.reason) == ("refused", "stale-seq")
            assert (equal.status, equal.reason) == ("refused", "stale-seq")
            record = await store.read(pid)
            assert record is not None
            assert (record.state, record.revision, record.last_seq) == (
                ProgressState.TESTING,
                2,
                3,
            )

            next_epoch = await _model(
                store, pid, _command("u5", state=ProgressState.PUBLISHING), epoch=2, seq=1
            )
            assert (next_epoch.status, next_epoch.revision) == ("applied", 3)
            older_epoch = await _model(
                store, pid, _command("u6", state=ProgressState.INVESTIGATING), epoch=1, seq=99
            )
            assert (older_epoch.status, older_epoch.reason) == ("refused", "stale-epoch")
            record = await store.read(pid)
            assert record is not None
            assert (record.state, record.revision, record.epoch, record.last_seq) == (
                ProgressState.PUBLISHING,
                3,
                2,
                1,
            )

    asyncio.run(go())


def test_a_stale_revision_read_writes_nothing_and_is_retried(names) -> None:  # noqa: ANN001
    """The compare-and-set: the store builds the card payload and its delivery id
    from the revision it READ. When another update moves the revision between
    that read and the script, the script must refuse the stale payload, or the
    newer revision's card would be enqueued under the older revision's identity."""

    async def go() -> None:
        async with _store(names) as (store, config, client):
            pid = await store.open_chain(_THREAD, _ROOT)
            assert (await _model(store, pid, _command("u1"), seq=1)).revision == 1
            real_read = store._read_counts
            stale_reads = 0

            async def stale_then_real(progress_id: str) -> tuple[int, int]:
                nonlocal stale_reads
                if stale_reads == 0:
                    stale_reads += 1
                    # What a concurrent writer's interleaving looks like from
                    # here: a read taken before that writer's revision 2 landed.
                    await _model(
                        ProgressStore(client, config),
                        pid,
                        _command("u2", state=ProgressState.TESTING),
                        seq=2,
                    )
                    return (1, 0)
                return await real_read(progress_id)

            store._read_counts = stale_then_real  # type: ignore[method-assign]
            outcome = await _model(
                store, pid, _command("u3", state=ProgressState.PUBLISHING), seq=3
            )
            assert stale_reads == 1
            assert (outcome.status, outcome.revision) == ("applied", 3)
            assert outcome.deliveries == (card_update_delivery_id(pid, 3),)

            second = await store.read_delivery(card_update_delivery_id(pid, 2))
            third = await store.read_delivery(card_update_delivery_id(pid, 3))
            assert second is not None
            assert third is not None
            assert isinstance(second.delivery.progress, ProgressCard)
            assert isinstance(third.delivery.progress, ProgressCard)
            assert (second.delivery.progress.revision, second.delivery.progress.state) == (
                2,
                ProgressState.TESTING,
            )
            assert (third.delivery.progress.revision, third.delivery.progress.state) == (
                3,
                ProgressState.PUBLISHING,
            )
            assert await _pending(client, config) == {
                card_delivery_id(pid),
                card_update_delivery_id(pid, 2),
                card_update_delivery_id(pid, 3),
            }

    asyncio.run(go())


def test_terminal_is_monotonic(names) -> None:  # noqa: ANN001
    async def go() -> None:
        async with _store(names) as (store, config, client):
            pid = await store.open_chain(_THREAD, _ROOT)
            lease = await _lease(client, config, "worker-a")
            await _model(store, pid, _command("u1"), seq=1)
            closed = await _platform(
                store, pid, lease, update_id="terminal", state=ProgressState.COMPLETE
            )
            assert (closed.status, closed.revision) == ("applied", 2)
            card = await store.read_delivery(card_update_delivery_id(pid, 2))
            assert card is not None
            assert isinstance(card.delivery.progress, ProgressCard)
            assert card.delivery.progress.terminal is True

            late_model = await _model(
                store, pid, _command("u2", state=ProgressState.TESTING), seq=2
            )
            late_newer_epoch = await _model(
                store, pid, _command("u3", state=ProgressState.TESTING), epoch=5, seq=1
            )
            late_failed = await _platform(
                store, pid, lease, update_id="fail", state=ProgressState.FAILED, epoch=5
            )
            late_queued = await _platform(
                store, pid, lease, update_id="requeue", state=ProgressState.QUEUED, epoch=5
            )
            for outcome in (late_model, late_newer_epoch, late_failed, late_queued):
                assert (outcome.status, outcome.reason) == ("refused", "terminal")
            again = await _platform(
                store, pid, lease, update_id="terminal", state=ProgressState.COMPLETE
            )
            assert again.status == "duplicate"

            record = await store.read(pid)
            assert record is not None
            assert (record.state, record.terminal, record.revision, record.update_count) == (
                ProgressState.COMPLETE,
                True,
                2,
                2,
            )
            assert await _pending(client, config) == {
                card_delivery_id(pid),
                card_update_delivery_id(pid, 2),
            }

    asyncio.run(go())


@pytest.mark.parametrize("state", _PLATFORM_ONLY, ids=lambda s: s.value)
def test_a_model_command_naming_a_platform_only_state_is_refused(
    names,  # noqa: ANN001
    state: ProgressState,
) -> None:
    async def go() -> None:
        async with _store(names) as (store, config, client):
            pid = await store.open_chain(_THREAD, _ROOT)
            outcome = await _model(
                store, pid, _command("u1", state=state, milestone=MilestoneClass.EVIDENCE), seq=1
            )
            assert (outcome.status, outcome.reason) == ("refused", "platform-only-state")
            record = await store.read(pid)
            assert record is not None
            assert (record.state, record.revision, record.update_count) == (None, 0, 0)
            assert record.milestones_used == 0
            assert await _pending(client, config) == set()
            # The refused id is not recorded, so it is not a duplicate later.
            accepted = await _model(store, pid, _command("u1"), seq=1)
            assert accepted.status == "applied"

    asyncio.run(go())


@pytest.mark.parametrize("state", _MODEL_STATES, ids=lambda s: s.value)
def test_a_model_command_naming_a_working_state_is_accepted(
    names,  # noqa: ANN001
    state: ProgressState,
) -> None:
    """The positive control for the refusal above."""

    async def go() -> None:
        async with _store(names) as (store, _config, _client):
            pid = await store.open_chain(_THREAD, _ROOT)
            outcome = await _model(store, pid, _command("u1", state=state), seq=1)
            assert (outcome.status, outcome.revision) == ("applied", 1)
            record = await store.read(pid)
            assert record is not None
            assert record.state == state

    asyncio.run(go())


def test_an_update_without_a_record_is_refused_and_creates_none(names) -> None:  # noqa: ANN001
    async def go() -> None:
        async with _store(names) as (store, config, client):
            pid = progress_id_for(_THREAD, _ROOT)
            outcome = await _model(store, pid, _command("u1"), seq=1)
            assert (outcome.status, outcome.reason) == ("refused", "no-chain")
            assert await store.read(pid) is None
            assert await client.exists(config.progress_key(pid)) == 0
            assert await _pending(client, config) == set()

    asyncio.run(go())


def test_an_unchanged_update_is_accepted_without_a_new_revision(names) -> None:  # noqa: ANN001
    """Revision counts changes to the card; update_count counts accepted updates."""

    async def go() -> None:
        async with _store(names) as (store, config, client):
            pid = await store.open_chain(_THREAD, _ROOT)
            await _model(store, pid, _command("u1"), seq=1)
            same = await _model(store, pid, _command("u2"), seq=2)
            assert (same.status, same.revision, same.deliveries) == ("applied", 1, ())
            record = await store.read(pid)
            assert record is not None
            assert (record.revision, record.update_count, record.last_seq) == (1, 2, 2)
            assert await _pending(client, config) == {card_delivery_id(pid)}

    asyncio.run(go())


def test_the_fifty_first_update_is_refused_and_the_terminal_write_still_lands(
    names,  # noqa: ANN001
) -> None:
    async def go() -> None:
        async with _store(names) as (store, config, client):
            assert MAX_PROGRESS_UPDATES == 50
            pid = await store.open_chain(_THREAD, _ROOT)
            for n in range(1, MAX_PROGRESS_UPDATES + 1):
                outcome = await _model(store, pid, _command(f"u{n}", summary=f"Step {n}"), seq=n)
                assert outcome.status == "applied", n
            over = await _model(
                store,
                pid,
                _command("u51", summary="Step 51", milestone=MilestoneClass.SCOPE),
                seq=51,
            )
            assert (over.status, over.reason) == ("refused", "update-cap")
            record = await store.read(pid)
            assert record is not None
            assert (record.update_count, record.revision, record.milestones_used) == (50, 50, 0)
            assert record.summary == "Step 50"

            # The cap bounds the chain, not its ability to close its card.
            lease = await _lease(client, config, "worker-a")
            closed = await _platform(
                store, pid, lease, update_id="terminal", state=ProgressState.FAILED
            )
            assert (closed.status, closed.revision) == ("applied", 51)
            record = await store.read(pid)
            assert record is not None
            assert (record.state, record.terminal, record.update_count) == (
                ProgressState.FAILED,
                True,
                51,
            )

    asyncio.run(go())


# --- milestones ----------------------------------------------------------------


def test_concurrent_milestone_requests_reserve_exactly_three(names) -> None:  # noqa: ANN001
    """Eight requests at once, each on its own connection and its own store.

    Arrival order at Valkey is not controllable, and the (epoch, seq) order
    refuses whichever request arrives after a higher seq, so how many of one
    wave are accepted varies. What cannot vary is the budget. Each wave accepts
    at least its first arrival, so waves run until at least four milestone
    requests have been accepted, one more than the budget, and at most four
    waves are needed.
    """

    async def go() -> None:
        async with _store(names) as (store, config, client):
            pid = await store.open_chain(_THREAD, _ROOT)
            clients = [_client() for _ in range(8)]
            try:
                stores = [ProgressStore(c, config) for c in clients]
                await asyncio.gather(*(c.ping() for c in clients))
                accepted: list[Any] = []
                for wave in range(4):
                    outcomes = await asyncio.gather(
                        *(
                            _model(
                                stores[i],
                                pid,
                                _command(
                                    f"w{wave}-{i}",
                                    summary=f"Evidence {wave}-{i}",
                                    milestone=MilestoneClass.EVIDENCE,
                                ),
                                seq=wave * 8 + i + 1,
                            )
                            for i in range(8)
                        )
                    )
                    wave_accepted = [o for o in outcomes if o.status == "applied"]
                    assert wave_accepted, f"wave {wave} accepted nothing"
                    assert all(
                        o.reason == "stale-seq" for o in outcomes if o.status != "applied"
                    )
                    accepted += wave_accepted
                    if len(accepted) > 3:
                        break
            finally:
                for c in clients:
                    with contextlib.suppress(Exception):
                        await c.aclose()

            ordinals = sorted(o.milestone_ordinal for o in accepted if o.milestone_ordinal)
            assert ordinals == [1, 2, 3]
            assert sum(1 for o in accepted if o.milestone_refused) == len(accepted) - 3
            record = await store.read(pid)
            assert record is not None
            assert record.milestones_used == 3
            milestones = {milestone_delivery_id(pid, n) for n in (1, 2, 3)}
            assert milestones <= await _pending(client, config)
            assert await client.exists(
                config.progress_delivery_key(milestone_delivery_id(pid, 4))
            ) == 0
            for n in (1, 2, 3):
                stored = await store.read_delivery(milestone_delivery_id(pid, n))
                assert stored is not None
                assert isinstance(stored.delivery.progress, ProgressMilestone)
                assert stored.delivery.progress.ordinal == n

    asyncio.run(go())


def test_a_fourth_milestone_is_refused_while_its_update_still_applies(
    names,  # noqa: ANN001
) -> None:
    async def go() -> None:
        async with _store(names) as (store, config, client):
            pid = await store.open_chain(_THREAD, _ROOT)
            classes = (MilestoneClass.EVIDENCE, MilestoneClass.SCOPE, MilestoneClass.VERIFICATION)
            for n, cls in enumerate(classes, start=1):
                outcome = await _model(
                    store, pid, _command(f"m{n}", summary=f"Milestone {n}", milestone=cls), seq=n
                )
                assert (outcome.milestone_ordinal, outcome.milestone_refused) == (n, False)
                assert milestone_delivery_id(pid, n) in outcome.deliveries

            fourth = await _model(
                store,
                pid,
                _command(
                    "m4",
                    state=ProgressState.TESTING,
                    summary="Running the suite",
                    milestone=MilestoneClass.VERIFICATION,
                ),
                seq=4,
            )
            assert fourth.status == "applied"
            assert (fourth.milestone_ordinal, fourth.milestone_refused) == (None, True)
            assert fourth.revision == 4
            assert fourth.deliveries == (card_update_delivery_id(pid, 4),)
            record = await store.read(pid)
            assert record is not None
            assert (record.state, record.summary, record.milestones_used) == (
                ProgressState.TESTING,
                "Running the suite",
                3,
            )
            assert await client.exists(
                config.progress_delivery_key(milestone_delivery_id(pid, 4))
            ) == 0
            # Its id is recorded, so a retry is a duplicate, not a second try.
            retry = await _model(
                store,
                pid,
                _command(
                    "m4",
                    state=ProgressState.TESTING,
                    summary="Running the suite",
                    milestone=MilestoneClass.VERIFICATION,
                ),
                seq=5,
            )
            assert retry.status == "duplicate"

    asyncio.run(go())


def test_the_milestone_cap_survives_a_new_store_instance(names) -> None:  # noqa: ANN001
    async def go() -> None:
        async with _store(names) as (first, _config, _client):
            pid = await first.open_chain(_THREAD, _ROOT)
            for n in (1, 2, 3):
                outcome = await _model(
                    first,
                    pid,
                    _command(f"m{n}", summary=f"M{n}", milestone=MilestoneClass.EVIDENCE),
                    seq=n,
                )
                assert outcome.milestone_ordinal == n
        # A restarted worker: a new connection, a new store, nothing in memory.
        async with _store(names) as (second, _config, _client):
            reopened = await second.open_chain(_THREAD, _ROOT)
            assert reopened == pid
            record = await second.read(pid)
            assert record is not None
            assert record.milestones_used == 3
            fourth = await _model(
                second, pid, _command("m4", summary="M4", milestone=MilestoneClass.SCOPE), seq=4
            )
            assert (fourth.status, fourth.milestone_ordinal, fourth.milestone_refused) == (
                "applied",
                None,
                True,
            )

    asyncio.run(go())


# --- the chain pointer ---------------------------------------------------------


def test_an_approval_resume_continues_the_chain_and_its_budget(names) -> None:  # noqa: ANN001
    async def go() -> None:
        async with _store(names) as (store, config, client):
            pid = await store.chain_for_turn(_THREAD, _ROOT, resume=False)
            assert pid == progress_id_for(_THREAD, _ROOT)
            for n in (1, 2):
                await _model(
                    store,
                    pid,
                    _command(f"m{n}", summary=f"M{n}", milestone=MilestoneClass.EVIDENCE),
                    seq=n,
                )
            assert await store.link_resume(_RESUME, pid) is True
            lease = await _lease(client, config, "worker-a")
            waiting = await _platform(
                store,
                pid,
                lease,
                update_id="await-1",
                state=ProgressState.AWAITING_APPROVAL,
                summary="Waiting for approval",
                epoch=2,
            )
            assert waiting.status == "applied"

            # A late command from the suspended session is fenced out.
            late = await _model(store, pid, _command("late", summary="Late"), epoch=1, seq=3)
            assert (late.status, late.reason) == ("refused", "stale-epoch")

            resumed = await store.chain_for_turn(_THREAD, _RESUME, resume=True)
            assert resumed == pid
            third = await _model(
                store,
                pid,
                _command("m3", summary="M3", milestone=MilestoneClass.VERIFICATION),
                epoch=2,
                seq=1,
            )
            fourth = await _model(
                store,
                pid,
                _command("m4", summary="M4", milestone=MilestoneClass.VERIFICATION),
                epoch=2,
                seq=2,
            )
            assert (third.status, third.milestone_ordinal) == ("applied", 3)
            assert (fourth.status, fourth.milestone_refused) == ("applied", True)
            # The resume never opened a record of its own.
            assert await store.read(progress_id_for(_THREAD, _RESUME)) is None

    asyncio.run(go())


def test_an_expired_resume_pointer_yields_no_chain(names) -> None:  # noqa: ANN001
    async def go() -> None:
        async with _store(names) as (store, config, client):
            pid = await store.open_chain(_THREAD, _ROOT)
            assert await store.link_resume(_RESUME, pid) is True
            assert await store.resolve_chain(_RESUME) == pid
            # A real expiry, not a delete.
            await client.pexpire(config.progress_chain_key(_RESUME), 1)
            await asyncio.sleep(0.05)

            assert await store.resolve_chain(_RESUME) is None
            assert await store.chain_for_turn(_THREAD, _RESUME, resume=True) is None
            # No record was derived from the resume's own id, so no budget either.
            assert await store.read(progress_id_for(_THREAD, _RESUME)) is None
            # A resume whose chain never linked is silent in the same way.
            assert await store.chain_for_turn(_THREAD, "approval-x-resolved", resume=True) is None
            # A fresh turn still opens its own record.
            fresh = await store.chain_for_turn(_THREAD, "Ev0EXAMPLE3", resume=False)
            assert fresh == progress_id_for(_THREAD, "Ev0EXAMPLE3")
            assert await store.read(fresh) is not None

    asyncio.run(go())


def test_a_resume_cannot_link_to_a_chain_that_does_not_exist(names) -> None:  # noqa: ANN001
    async def go() -> None:
        async with _store(names) as (store, config, client):
            missing = progress_id_for(_THREAD, "never-opened")
            assert await store.link_resume(_RESUME, missing) is False
            assert await client.exists(config.progress_chain_key(_RESUME)) == 0
            pid = await store.open_chain(_THREAD, _ROOT)
            other = await store.open_chain(_THREAD, "Ev0EXAMPLE2")
            assert await store.link_resume(_RESUME, pid) is True
            # The first link wins; a second chain cannot take the resume over.
            assert await store.link_resume(_RESUME, other) is False
            assert await store.resolve_chain(_RESUME) == pid

    asyncio.run(go())


# --- the outbox: identity, ack, and the fence ----------------------------------


def test_a_redelivered_turn_reuses_the_same_delivery_ids(names) -> None:  # noqa: ANN001
    async def go() -> None:
        async with _store(names) as (store, config, client):
            pid = await store.open_chain(_THREAD, _ROOT)
            first = await _model(
                store, pid, _command("u1", milestone=MilestoneClass.EVIDENCE), seq=1
            )
            assert set(first.deliveries) == {card_delivery_id(pid), milestone_delivery_id(pid, 1)}
            # A redelivered root event reopens the same record and replays.
            assert await store.open_chain(_THREAD, _ROOT) == pid
            again = await _model(
                store, pid, _command("u1", milestone=MilestoneClass.EVIDENCE), seq=1
            )
            assert again.status == "duplicate"
            assert await _pending(client, config) == set(first.deliveries)
            card = await store.read_delivery(card_delivery_id(pid))
            milestone = await store.read_delivery(milestone_delivery_id(pid, 1))
            assert card is not None
            assert milestone is not None
            assert (card.delivery.operation, card.delivery.delivery_id) == (
                "post",
                card_delivery_id(pid),
            )
            assert card.delivery.progress == ProgressCard(
                kind="card",
                state=ProgressState.INVESTIGATING,
                summary="Reading the ledger",
                revision=1,
                terminal=False,
            )
            assert milestone.delivery.progress == ProgressMilestone(
                kind="milestone",
                milestone=MilestoneClass.EVIDENCE,
                summary="Reading the ledger",
                ordinal=1,
            )
            assert card.route == _ROUTE
            assert card.delivery.target == _TARGET
            assert card.attempts == 0

    asyncio.run(go())


def test_ack_clears_only_its_own_generation(names) -> None:  # noqa: ANN001
    async def go() -> None:
        async with _store(names) as (store, config, client):
            pid = await store.open_chain(_THREAD, _ROOT, answer_ref="1700000000.000200")
            await _model(store, pid, _command("u1"), seq=1)
            card_id = card_delivery_id(pid)
            written = await store.read_delivery(card_id)
            assert written is not None

            assert await store.ack(card_id, generation="not-this-one") is False
            assert await store.read_delivery(card_id) is not None
            assert card_id in await _pending(client, config)

            # The chain expires and a redelivered root reopens it: the same
            # delivery id is written again, under a new generation.
            await client.delete(config.progress_key(pid), config.progress_delivery_key(card_id))
            await store.open_chain(_THREAD, _ROOT, answer_ref="1700000000.000200")
            await _model(store, pid, _command("u1"), seq=1)
            rewritten = await store.read_delivery(card_id)
            assert rewritten is not None
            assert rewritten.generation != written.generation

            # The late acknowledgement of the first write clears nothing.
            assert (
                await store.ack(card_id, generation=written.generation, card_ref="1700000000.1")
                is False
            )
            assert await store.read_delivery(card_id) is not None
            record = await store.read(pid)
            assert record is not None
            assert record.card_ref is None

            assert (
                await store.ack(card_id, generation=rewritten.generation, card_ref="1700000000.2")
                is True
            )
            assert await store.read_delivery(card_id) is None
            assert card_id not in await _pending(client, config)
            record = await store.read(pid)
            assert record is not None
            assert record.card_ref == "1700000000.2"
            assert record.answer_ref == "1700000000.000200"

            # An edit's acknowledgement never moves the adopted card ref.
            await _model(store, pid, _command("u2", state=ProgressState.TESTING), seq=2)
            edit = await store.read_delivery(card_update_delivery_id(pid, 2))
            assert edit is not None
            assert await store.ack(
                edit.delivery.delivery_id, generation=edit.generation, card_ref="1700000000.3"
            )
            record = await store.read(pid)
            assert record is not None
            assert record.card_ref == "1700000000.2"

    asyncio.run(go())


def test_a_platform_write_with_a_lost_lease_writes_nothing(names) -> None:  # noqa: ANN001
    async def go() -> None:
        async with _store(names) as (store, config, client):
            pid = await store.open_chain(_THREAD, _ROOT)
            await _model(store, pid, _command("u1"), seq=1)
            before = await store.read(pid)
            lease_a = await _lease(client, config, "worker-a")
            leases = DeliveryLeaseStore(client, config)

            # A's lease expires and B takes the delivery: token and generation move.
            await client.delete(
                config.delivery_lease_key(lease_a.stream, lease_a.group, lease_a.entry_id)
            )
            await client.xclaim(
                lease_a.stream, lease_a.group, "worker-b", 0, [lease_a.entry_id]
            )
            lease_b = await leases.acquire(
                lease_a.stream, lease_a.group, lease_a.entry_id, consumer="worker-b"
            )
            assert lease_b.generation == lease_a.generation + 1

            # The same token under a stale generation is refused too.
            stale_generation = DeliveryLease(
                stream=lease_b.stream,
                group=lease_b.group,
                entry_id=lease_b.entry_id,
                owner=lease_b.owner,
                generation=lease_a.generation,
                budget=lease_b.budget,
            )
            for lost in (lease_a, stale_generation, unfenced_lease()):
                outcome = await _platform(
                    store, pid, lost, update_id="terminal", state=ProgressState.COMPLETE
                )
                assert (outcome.status, outcome.reason) == ("refused", "lease-lost")
                assert outcome.deliveries == ()
                assert await store.read(pid) == before
                assert await _pending(client, config) == {card_delivery_id(pid)}

            # Positive control: the current owner's identical write lands.
            closed = await _platform(
                store, pid, lease_b, update_id="terminal", state=ProgressState.COMPLETE
            )
            assert (closed.status, closed.revision) == ("applied", 2)
            record = await store.read(pid)
            assert record is not None
            assert (record.state, record.terminal) == (ProgressState.COMPLETE, True)

    asyncio.run(go())


def test_a_model_update_id_cannot_pre_empt_a_platform_write(names) -> None:  # noqa: ANN001
    async def go() -> None:
        async with _store(names) as (store, config, client):
            pid = await store.open_chain(_THREAD, _ROOT)
            await _model(store, pid, _command("terminal"), seq=1)
            lease = await _lease(client, config, "worker-a")
            closed = await _platform(
                store, pid, lease, update_id="terminal", state=ProgressState.CANCELLED
            )
            assert closed.status == "applied"
            record = await store.read(pid)
            assert record is not None
            assert record.state == ProgressState.CANCELLED

    asyncio.run(go())


# --- the sweeper ---------------------------------------------------------------


async def _one_delivery(store: ProgressStore) -> tuple[str, str]:
    pid = await store.open_chain(_THREAD, _ROOT)
    await _model(store, pid, _command("u1"), seq=1)
    return pid, card_delivery_id(pid)


def test_the_sweeper_retries_with_the_same_delivery_id(names) -> None:  # noqa: ANN001
    async def go() -> None:
        async with _store(names) as (store, config, client):
            pid, card_id = await _one_delivery(store)
            seen: list[tuple[str, str]] = []

            async def flaky(stored: StoredProgressDelivery) -> str | None:
                seen.append((stored.delivery.delivery_id, stored.delivery.model_dump_json()))
                if len(seen) < 3:
                    raise ConnectionError("adapter unreachable")
                return "1700000000.000300"

            for _ in range(3):
                await sweep_pending_progress(store, deliver=flaky, grace_s=0.0)
            assert [delivery_id for delivery_id, _ in seen] == [card_id] * 3
            assert len({body for _, body in seen}) == 1
            assert await store.read_delivery(card_id) is None
            assert await _pending(client, config) == set()
            record = await store.read(pid)
            assert record is not None
            assert record.card_ref == "1700000000.000300"

    asyncio.run(go())


def test_the_sweeper_dead_letters_after_its_attempt_budget(names) -> None:  # noqa: ANN001
    async def go() -> None:
        async with _store(names) as (store, config, client):
            pid, card_id = await _one_delivery(store)
            stored = await store.read_delivery(card_id)
            assert stored is not None
            calls = 0

            async def down(_stored: StoredProgressDelivery) -> str | None:
                nonlocal calls
                calls += 1
                raise ConnectionError("adapter unreachable")

            for _ in range(5):
                await sweep_pending_progress(store, deliver=down, grace_s=0.0, max_attempts=3)
            assert calls == 3
            assert await store.read_delivery(card_id) is None
            assert await _pending(client, config) == set()
            rows = await _graveyard(client, config)
            assert len(rows) == 1
            row = rows[0]
            assert row["dl_source"] == "progress-outbox"
            assert row["dl_reason"] == "max-attempts-exceeded"
            assert row["dl_delivery_count"] == "3"
            assert row["delivery_id"] == card_id
            assert row["progress_id"] == pid
            assert json.loads(row["event"]) == json.loads(stored.delivery.model_dump_json())
            assert "dl_dead_lettered_at" in row
            assert "dl_original_id" not in row

    asyncio.run(go())


def test_the_sweeper_quarantines_a_malformed_record(
    names,  # noqa: ANN001
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def go() -> None:
        async with _store(names) as (store, config, client):
            _pid, card_id = await _one_delivery(store)
            bad_id = str(uuid.uuid4())
            bad_key = config.progress_delivery_key(bad_id)
            await client.hset(bad_key, mapping={"event": "{not json", "gen": "g", "attempts": "0"})
            await client.sadd(config.progress_pending_key(), bad_id)
            delivered: list[str] = []

            async def ok(stored: StoredProgressDelivery) -> str | None:
                delivered.append(stored.delivery.delivery_id)
                return None

            with caplog.at_level(logging.ERROR, logger="curie_worker.progress"):
                first = await sweep_pending_progress(store, deliver=ok, grace_s=0.0)
            assert first.quarantined == 1
            assert delivered == [card_id]
            assert bad_id not in await _pending(client, config)
            # The payload stays for inspection; only the index membership goes.
            assert await client.exists(bad_key) == 1
            assert any(
                "quarantined" in r.getMessage() and bad_id in r.getMessage()
                for r in caplog.records
            )
            second = await sweep_pending_progress(store, deliver=ok, grace_s=0.0)
            assert second.quarantined == 0
            assert delivered == [card_id]

    asyncio.run(go())


def test_the_sweeper_drops_a_member_whose_delivery_has_expired(names) -> None:  # noqa: ANN001
    async def go() -> None:
        async with _store(names) as (store, config, client):
            _pid, card_id = await _one_delivery(store)
            await client.delete(config.progress_delivery_key(card_id))
            result = await sweep_pending_progress(store, deliver=None)
            assert result.dropped == 1
            assert await _pending(client, config) == set()

    asyncio.run(go())


def test_the_sweeper_is_bounded_by_its_batch(names) -> None:  # noqa: ANN001
    async def go() -> None:
        async with _store(names) as (store, config, client):
            for n in range(5):
                pid = await store.open_chain(_THREAD, f"Ev{n}")
                await _model(store, pid, _command("u1"), seq=1)
            assert len(await _pending(client, config)) == 5
            calls = 0

            async def ok(_stored: StoredProgressDelivery) -> str | None:
                nonlocal calls
                calls += 1
                return None

            await sweep_pending_progress(store, deliver=ok, grace_s=0.0, batch=2)
            assert calls == 2
            assert len(await _pending(client, config)) == 3

    asyncio.run(go())


def test_the_sweeper_is_bounded_by_its_time_budget(names) -> None:  # noqa: ANN001
    async def go() -> None:
        async with _store(names) as (store, config, client):
            for n in range(3):
                pid = await store.open_chain(_THREAD, f"Ev{n}")
                await _model(store, pid, _command("u1"), seq=1)
            calls = 0

            async def hangs(_stored: StoredProgressDelivery) -> str | None:
                nonlocal calls
                calls += 1
                await asyncio.sleep(3600)
                return None

            started = time.monotonic()
            await sweep_pending_progress(store, deliver=hangs, grace_s=0.0, budget_s=0.3)
            assert time.monotonic() - started < 2.0
            assert calls == 1
            # The abandoned attempt was charged, and the delivery is still owed.
            pending = await _pending(client, config)
            assert len(pending) == 3
            attempts = [
                stored.attempts
                for stored in [await store.read_delivery(d) for d in pending]
                if stored is not None
            ]
            assert sorted(attempts) == [0, 0, 1]

    asyncio.run(go())


def test_the_sweeper_leaves_a_delivery_inside_its_grace_window(names) -> None:  # noqa: ANN001
    async def go() -> None:
        async with _store(names) as (store, config, client):
            _pid, card_id = await _one_delivery(store)
            calls = 0

            async def ok(_stored: StoredProgressDelivery) -> str | None:
                nonlocal calls
                calls += 1
                return None

            await sweep_pending_progress(store, deliver=ok, grace_s=60.0)
            assert calls == 0
            stored = await store.read_delivery(card_id)
            assert stored is not None
            assert stored.attempts == 0

    asyncio.run(go())


def test_the_sweeper_without_a_deliverer_charges_nothing(names) -> None:  # noqa: ANN001
    """The production mode of this change: nothing delivers progress yet, so the
    tick's sweeper may quarantine, drop and dead-letter, but must not spend a
    budget a replica that can deliver would need."""

    async def go() -> None:
        async with _store(names) as (store, config, client):
            _pid, fresh = await _one_delivery(store)
            spent_pid = await store.open_chain(_THREAD, "Ev0EXAMPLE2")
            await _model(store, spent_pid, _command("u1"), seq=1)
            spent = card_delivery_id(spent_pid)
            await client.hset(config.progress_delivery_key(spent), "attempts", "5")

            result = await sweep_pending_progress(store, deliver=None, grace_s=0.0)
            assert (result.dead_lettered, result.delivered, result.failed) == (1, 0, 0)
            still_owed = await store.read_delivery(fresh)
            assert still_owed is not None
            assert still_owed.attempts == 0
            assert await _pending(client, config) == {fresh}
            rows = await _graveyard(client, config)
            assert [row["delivery_id"] for row in rows] == [spent]
            assert rows[0]["dl_delivery_count"] == "5"

    asyncio.run(go())
