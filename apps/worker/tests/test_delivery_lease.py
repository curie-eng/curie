"""Per-script contract tests for the delivery ownership lease (ADR-0131, #1971).

Against **real Valkey**, never a mock. The repo rule is "Valkey is never mocked"
and here it is load-bearing rather than stylistic: the fence *is* Valkey
semantics -- atomic ``EVAL``, server ``TIME``, key expiry, ``XPENDING``
ownership, and ``XCLAIM ... JUSTID``'s refusal to bump the delivery counter. A
mocked ``EVAL`` would assert only that we wrote the Lua we wrote.

Time is compressed by CONFIGURING short lease clocks (TTL 1.0s, heartbeat 0.3s),
never by patching a clock. The one thing deliberately NOT compressed or stubbed
is the Valkey server ``TIME`` read: the whole point of the deadline is that it
comes from the server, so a test that faked it would prove nothing about the
property under test. The *budget* is left at its 60s floor because nothing in
this file waits on the budget -- only the lease clocks need to be small.

The API this file pins, for the implementer of ``delivery_lease.py``:

    DeliveryLeaseStore(redis: Redis, config: WorkerConfig)
      .acquire(stream, group, entry_id, *, consumer)  -> DeliveryLease
          raises LeaseRefused, whose ``.reason`` is "held" (a live lease exists)
          or "not-owner" (we do not hold the PEL row)
      .heartbeat(stream, group, entry_id, *, consumer, owner, generation,
                 resume_event_id)
          -> DeliveryBudget | None   (None == fail-closed refusal, lease lost)
      .release(stream, group, entry_id, *, owner, resume_event_id) -> bool
          (compare-and-delete)
      .settle(stream, group, entry_id) -> None    (lease AND state key removed)
      .is_live(stream, group, entry_id) -> bool   (the cheap reclaim-path read)
      .peek(stream, group, entry_id) -> dict[str, str]   ({} when absent)

    DeliveryLease: .owner .generation .budget .lost .remaining_s() .raise_if_lost()
    DeliveryBudget: .deadline_ms .anchor_server_ms .anchor_monotonic
                    .remaining_s() .reanchor(server_ms)
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
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
from curie_worker.delivery_lease import (
    DeliveryBudget,
    DeliveryLeaseStore,
    LeaseRefused,
)
from redis.asyncio import Redis as AsyncRedis

# The compressed lease clocks. Every ratio the config validators enforce is
# preserved: TTL (1.0) >= 3 * heartbeat (0.3), reclaim (0.5) < TTL (1.0), and
# the runner ceiling (30) <= the budget (60, its configurable floor).
_TTL_S = 1.0
_HEARTBEAT_S = 0.3
_BUDGET_S = 60.0
# A lease TTL longer than any runner stall, for a test that must observe a
# lease stay live across real heartbeats rather than wait for one to lapse.
_STALL_PROOF_TTL_S = 300.0


# ``sync_redis`` and ``names`` (the per-test-unique stream / group / key
# prefix on the shared Valkey) live in ``tests/conftest.py``, shared with
# ``tests/kernel``'s ``make_harness``.


def _config(names: dict[str, str], **overrides: object) -> WorkerConfig:
    base: dict[str, object] = {
        "valkey_host": _VALKEY_HOST,
        "valkey_port": _VALKEY_PORT,
        "valkey_password": _VALKEY_PW,
        "stream": names["stream"],
        "consumer_group": names["group"],
        "key_prefix": names["prefix"],
        "delivery_budget_s": _BUDGET_S,
        "delivery_lease_ttl_s": _TTL_S,
        "delivery_lease_heartbeat_s": _HEARTBEAT_S,
        "reclaim_interval_s": 0.5,
        "runner_total_timeout_s": 30.0,
    }
    base.update(overrides)
    return WorkerConfig(**base)


@contextlib.asynccontextmanager
async def _store(names: dict[str, str], **overrides: object) -> AsyncIterator[
    tuple[DeliveryLeaseStore, WorkerConfig, AsyncRedis]
]:
    """A lease store on a live async client, torn down on every exit path."""
    config = _config(names, **overrides)
    client: AsyncRedis = AsyncRedis(
        host=_VALKEY_HOST,
        port=_VALKEY_PORT,
        password=_VALKEY_PW or None,
        decode_responses=True,
    )
    try:
        yield DeliveryLeaseStore(client, config), config, client
    finally:
        with contextlib.suppress(Exception):
            await client.aclose()


async def _pending(
    client: AsyncRedis, config: WorkerConfig, consumer: str, *, count: int = 1
) -> list[str]:
    """Add ``count`` entries and read them into ``consumer``'s PEL.

    The PEL row is what ``acquire`` verifies: the lease is authority, but a
    consumer that does not hold the pending entry has no standing to take it.
    """
    with contextlib.suppress(Exception):
        await client.xgroup_create(config.stream, config.consumer_group, id="0", mkstream=True)
    ids = [await client.xadd(config.stream, {"payload": f"p{i}"}) for i in range(count)]
    read = await client.xreadgroup(
        config.consumer_group, consumer, {config.stream: ">"}, count=count
    )
    delivered = [entry_id for _stream, entries in read for entry_id, _fields in entries]
    assert delivered == ids, f"expected {ids} in {consumer}'s PEL, got {delivered}"
    return ids


async def _times_delivered(
    client: AsyncRedis, config: WorkerConfig, entry_id: str
) -> int:
    rows: Any = await client.xpending_range(
        config.stream, config.consumer_group, min=entry_id, max=entry_id, count=1
    )
    assert rows, f"entry {entry_id} is not pending"
    return int(rows[0]["times_delivered"])


# --- acquire: the single authority point -------------------------------------


def test_acquire_refuses_a_replacement_while_the_lease_is_live(names) -> None:  # noqa: ANN001
    """The observed defect, directly: without the live-lease refusal BOTH owners
    enter the handler and the same turn runs twice on two replicas."""

    async def go() -> None:
        async with _store(names) as (store, config, client):
            (entry,) = await _pending(client, config, "worker-a")
            lease_a = await store.acquire(
                config.stream, config.consumer_group, entry, consumer="worker-a"
            )
            assert lease_a.generation == 1
            assert await store.is_live(config.stream, config.consumer_group, entry) is True

            # B takes the PEL row, exactly as XAUTOCLAIM does on the reclaim path,
            # so the ONLY thing standing between B and the handler is the lease.
            await client.xclaim(
                config.stream, config.consumer_group, "worker-b", 0, [entry]
            )
            with pytest.raises(LeaseRefused) as exc_info:
                await store.acquire(
                    config.stream, config.consumer_group, entry, consumer="worker-b"
                )
            assert exc_info.value.reason == "held"

            # Positive control: the refusal above was the fence, not a dead path.
            # Once A's lease is gone, the very same call is granted.
            assert (
                await store.release(
                    config.stream, config.consumer_group, entry, owner=lease_a.owner,
                    resume_event_id=None,
                )
                is True
            )
            lease_b = await store.acquire(
                config.stream, config.consumer_group, entry, consumer="worker-b"
            )
            assert lease_b.generation == 2
            assert lease_b.owner != lease_a.owner

    asyncio.run(go())


def test_acquire_refuses_a_consumer_that_does_not_hold_the_pel_row(names) -> None:  # noqa: ANN001
    """The lease is authority AND the PEL row is a precondition. A consumer that
    never read the entry has no standing to fence it; reverting the ownership
    check lets any replica mint authority over a delivery it was never given."""

    async def go() -> None:
        async with _store(names) as (store, config, client):
            (entry,) = await _pending(client, config, "worker-a")

            with pytest.raises(LeaseRefused) as exc_info:
                await store.acquire(
                    config.stream, config.consumer_group, entry, consumer="worker-c"
                )
            assert exc_info.value.reason == "not-owner"
            # Nothing was minted on the refused path.
            assert await store.is_live(config.stream, config.consumer_group, entry) is False
            assert await store.peek(config.stream, config.consumer_group, entry) == {}

            # Positive control: the true PEL owner is granted.
            lease = await store.acquire(
                config.stream, config.consumer_group, entry, consumer="worker-a"
            )
            assert lease.generation == 1

    asyncio.run(go())


def test_acquire_after_expiry_transfers_and_increments_the_generation(names) -> None:  # noqa: ANN001
    """Force-kill recovery. A replacement may take a delivery ONLY after the
    lease expires, and the generation increments monotonically on every change of
    authority -- which is what makes the old owner's late heartbeat refusable."""

    async def go() -> None:
        async with _store(names) as (store, config, client):
            (entry,) = await _pending(client, config, "worker-a")
            lease_a = await store.acquire(
                config.stream, config.consumer_group, entry, consumer="worker-a"
            )
            # A dies without releasing: the lease is left to expire.
            await client.xclaim(
                config.stream, config.consumer_group, "worker-b", 0, [entry]
            )

            # Before expiry the replacement is refused.
            with pytest.raises(LeaseRefused) as exc_info:
                await store.acquire(
                    config.stream, config.consumer_group, entry, consumer="worker-b"
                )
            assert exc_info.value.reason == "held"

            await asyncio.sleep(_TTL_S + 0.4)
            assert await store.is_live(config.stream, config.consumer_group, entry) is False

            lease_b = await store.acquire(
                config.stream, config.consumer_group, entry, consumer="worker-b"
            )
            assert lease_b.generation == lease_a.generation + 1
            assert lease_b.generation == 2

    asyncio.run(go())


def test_a_replacement_inherits_the_original_deadline_not_a_fresh_budget(names) -> None:  # noqa: ANN001
    """The anti-budget-multiplication property (create-if-absent on
    ``deadline_ms``). Reverting HSETNX to HSET lets three transfers turn a
    configured 1,800-second budget into 5,400 seconds of execution."""

    async def go() -> None:
        async with _store(names) as (store, config, client):
            (entry,) = await _pending(client, config, "worker-a")
            lease_a = await store.acquire(
                config.stream, config.consumer_group, entry, consumer="worker-a"
            )
            first_deadline = int(
                (await store.peek(config.stream, config.consumer_group, entry))["deadline_ms"]
            )
            assert lease_a.budget.deadline_ms == first_deadline

            await client.xclaim(
                config.stream, config.consumer_group, "worker-b", 0, [entry]
            )
            await asyncio.sleep(_TTL_S + 0.4)
            lease_b = await store.acquire(
                config.stream, config.consumer_group, entry, consumer="worker-b"
            )

            # The generation moved, so a real re-acquisition happened -- the
            # deadline equality below is not the trivial "nothing changed" case.
            assert lease_b.generation == 2
            assert lease_b.budget.deadline_ms == first_deadline
            assert (
                int((await store.peek(config.stream, config.consumer_group, entry))["deadline_ms"])
                == first_deadline
            )
            # The replacement's remaining budget is what is LEFT, not the whole
            # budget: at least the lease TTL has burned since the first owner.
            assert lease_b.remaining_s() < _BUDGET_S - _TTL_S

    asyncio.run(go())


# --- heartbeat: three independent fail-closed guards --------------------------


def test_heartbeat_refuses_a_wrong_owner_token(names) -> None:  # noqa: ANN001
    """Fail closed on an owner token that is not the one in the lease key.
    Reverting the compare lets any process renew any lease."""

    async def go() -> None:
        async with _store(names) as (store, config, client):
            (entry,) = await _pending(client, config, "worker-a")
            lease = await store.acquire(
                config.stream, config.consumer_group, entry, consumer="worker-a"
            )

            refused = await store.heartbeat(
                config.stream,
                config.consumer_group,
                entry,
                consumer="worker-a",
                owner="not-the-owner-token",
                generation=lease.generation,
                resume_event_id=None,
            )
            assert refused is None

            # Positive control: the true owner renews, so the refusal above was
            # the owner compare and not a heartbeat path that never works.
            renewed = await store.heartbeat(
                config.stream,
                config.consumer_group,
                entry,
                consumer="worker-a",
                owner=lease.owner,
                generation=lease.generation,
                resume_event_id=None,
            )
            assert renewed is not None

    asyncio.run(go())


def test_heartbeat_refuses_a_stale_fencing_generation(names) -> None:  # noqa: ANN001
    """The slow-heartbeat case: Valkey answers after the lease already expired and
    was taken. The owner token alone cannot catch it, which is precisely why the
    generation is checked in the heartbeat and not only at acquisition."""

    async def go() -> None:
        async with _store(names) as (store, config, client):
            (entry,) = await _pending(client, config, "worker-a")
            lease_a = await store.acquire(
                config.stream, config.consumer_group, entry, consumer="worker-a"
            )
            await client.xclaim(
                config.stream, config.consumer_group, "worker-b", 0, [entry]
            )
            await asyncio.sleep(_TTL_S + 0.4)
            lease_b = await store.acquire(
                config.stream, config.consumer_group, entry, consumer="worker-b"
            )
            assert lease_b.generation == 2

            # CORRECT owner token, STALE generation: only the generation check can
            # refuse this, so the assertion isolates that guard alone.
            refused = await store.heartbeat(
                config.stream,
                config.consumer_group,
                entry,
                consumer="worker-b",
                owner=lease_b.owner,
                generation=lease_a.generation,
                resume_event_id=None,
            )
            assert refused is None

            # Positive control: the current generation renews.
            renewed = await store.heartbeat(
                config.stream,
                config.consumer_group,
                entry,
                consumer="worker-b",
                owner=lease_b.owner,
                generation=lease_b.generation,
                resume_event_id=None,
            )
            assert renewed is not None

    asyncio.run(go())


def test_heartbeat_refuses_once_the_pel_row_is_owned_by_someone_else(names) -> None:  # noqa: ANN001
    """A renewal must verify PEL ownership as well as the lease. Without it an
    owner whose entry has been claimed away keeps renewing authority over a
    delivery Valkey has already handed to another consumer."""

    async def go() -> None:
        async with _store(names) as (store, config, client):
            (entry,) = await _pending(client, config, "worker-a")
            lease = await store.acquire(
                config.stream, config.consumer_group, entry, consumer="worker-a"
            )

            # Positive control FIRST, while the PEL row is still ours.
            assert (
                await store.heartbeat(
                    config.stream,
                    config.consumer_group,
                    entry,
                    consumer="worker-a",
                    owner=lease.owner,
                    generation=lease.generation,
                    resume_event_id=None,
                )
                is not None
            )

            await client.xclaim(
                config.stream, config.consumer_group, "worker-b", 0, [entry]
            )

            refused = await store.heartbeat(
                config.stream,
                config.consumer_group,
                entry,
                consumer="worker-a",
                owner=lease.owner,
                generation=lease.generation,
                resume_event_id=None,
            )
            assert refused is None

    asyncio.run(go())


def test_heartbeat_extends_the_lease_without_bumping_times_delivered(names) -> None:  # noqa: ANN001
    """Two independent reverts, both caught here. Dropping the renewal expires a
    healthy long turn's lease; dropping ``JUSTID`` from the same-owner ``XCLAIM``
    burns one delivery per heartbeat and dead-letters a healthy turn in under a
    minute. The delivery count stays PEL-backed and is never reset.

    The renewal is read from the lease key's absolute server expiry
    (``PEXPIRETIME``) under a TTL no runner stall can outlast (#3861). Racing
    six 0.3s beats against a 1.0s TTL failed whenever one beat stalled 0.7s.
    No assertion needs the un-renewed sibling to survive the loop either."""

    async def go() -> None:
        async with _store(names, delivery_lease_ttl_s=_STALL_PROOF_TTL_S) as (
            store,
            config,
            client,
        ):
            renewed_entry, abandoned_entry = await _pending(client, config, "worker-a", count=2)
            renewed = await store.acquire(
                config.stream, config.consumer_group, renewed_entry, consumer="worker-a"
            )
            # The negative control: same consumer, same window, NO heartbeats.
            await store.acquire(
                config.stream, config.consumer_group, abandoned_entry, consumer="worker-a"
            )
            renewed_key = config.delivery_lease_key(
                config.stream, config.consumer_group, renewed_entry
            )
            abandoned_key = config.delivery_lease_key(
                config.stream, config.consumer_group, abandoned_entry
            )
            abandoned_expiry = await client.pexpiretime(abandoned_key)
            # A positive absolute expiry: the lease is a TTL that does lapse,
            # not a key that silently never expires.
            assert abandoned_expiry > 0

            before = await _times_delivered(client, config, renewed_entry)
            expiry = await client.pexpiretime(renewed_key)

            for _ in range(6):
                await asyncio.sleep(_HEARTBEAT_S)
                anchor = await store.heartbeat(
                    config.stream,
                    config.consumer_group,
                    renewed_entry,
                    consumer="worker-a",
                    owner=renewed.owner,
                    generation=renewed.generation,
                    resume_event_id=None,
                )
                assert anchor is not None
                # Every successful renewal re-anchors on a fresh server TIME, so
                # a worker with a skewed clock still measures elapsed correctly.
                assert anchor.deadline_ms == renewed.budget.deadline_ms
                assert anchor.anchor_server_ms >= renewed.budget.anchor_server_ms
                # The beat pushed the lease's server-side expiry forward.
                renewed_expiry = await client.pexpiretime(renewed_key)
                assert renewed_expiry > expiry, "the heartbeat did not renew the lease"
                expiry = renewed_expiry

            assert (
                await store.is_live(config.stream, config.consumer_group, renewed_entry) is True
            )
            # ...and the un-renewed sibling's expiry never moved over the SAME
            # window, so the renewal above is the heartbeat's doing and not a
            # side effect of acquiring under the same consumer. A renewal could
            # only move it later; an expired key reads -2, which also passes.
            assert await client.pexpiretime(abandoned_key) <= abandoned_expiry
            assert expiry > abandoned_expiry

            after = await _times_delivered(client, config, renewed_entry)
            assert after == before, (
                "the same-owner XCLAIM must use JUSTID: it reset PEL idle but "
                f"burned {after - before} deliveries of the ADR-0039 budget"
            )

    asyncio.run(go())


# --- release and settle -------------------------------------------------------


def test_release_is_compare_and_delete_so_a_stale_token_frees_nothing(names) -> None:  # noqa: ANN001
    """A late ``__aexit__`` from an owner that already lost the fence must not
    free the CURRENT owner's lease -- that would hand the delivery to a third
    process while the real owner is still executing."""

    async def go() -> None:
        async with _store(names) as (store, config, client):
            (entry,) = await _pending(client, config, "worker-a")
            lease = await store.acquire(
                config.stream, config.consumer_group, entry, consumer="worker-a"
            )

            assert (
                await store.release(
                    config.stream, config.consumer_group, entry, owner="stale-owner-token",
                    resume_event_id=None,
                )
                is False
            )
            assert await store.is_live(config.stream, config.consumer_group, entry) is True

            assert (
                await store.release(
                    config.stream, config.consumer_group, entry, owner=lease.owner,
                    resume_event_id=None,
                )
                is True
            )
            assert await store.is_live(config.stream, config.consumer_group, entry) is False

            # Release drops the LEASE only. The delivery state survives, because
            # its absence must keep meaning "first delivery" -- a released-then-
            # reacquired entry that minted a fresh deadline would multiply the
            # budget exactly as a reverted HSETNX does.
            state = await store.peek(config.stream, config.consumer_group, entry)
            assert state.get("deadline_ms")
            assert state.get("gen") == "1"

    asyncio.run(go())


def test_settle_removes_both_the_lease_and_the_delivery_state(names) -> None:  # noqa: ANN001
    """Terminal ACK and dead-letter settlement remove the delivery state. Without
    it, a dead-lettered-and-redelivered event id accumulates state keys until
    their one-day retention TTL."""

    async def go() -> None:
        async with _store(names) as (store, config, client):
            (entry,) = await _pending(client, config, "worker-a")
            await store.acquire(
                config.stream, config.consumer_group, entry, consumer="worker-a"
            )
            lease_key = config.delivery_lease_key(config.stream, config.consumer_group, entry)
            state_key = config.delivery_state_key(config.stream, config.consumer_group, entry)
            assert await client.exists(lease_key) == 1
            assert await client.exists(state_key) == 1

            await store.settle(config.stream, config.consumer_group, entry)

            assert await client.exists(lease_key) == 0
            assert await client.exists(state_key) == 0
            assert await store.is_live(config.stream, config.consumer_group, entry) is False
            assert await store.peek(config.stream, config.consumer_group, entry) == {}

    asyncio.run(go())


# --- the substrate property the whole deadline rests on -----------------------


def test_server_time_inside_eval_is_a_plausible_epoch(names) -> None:  # noqa: ANN001
    """``redis.call('TIME')`` inside ``EVAL`` must return real epoch time, not
    zero and not process uptime. Every deadline in this feature is computed from
    it inside the acquire script, so if this property does not hold the budget is
    meaningless -- and it would fail silently as an instantly-expired deadline."""

    async def go() -> None:
        async with _store(names) as (store, config, client):
            raw = await client.eval(
                "local t = redis.call('TIME') "
                "return tostring(t[1] * 1000 + math.floor(t[2] / 1000))",
                0,
            )
            server_ms = int(raw)
            wall_ms = time.time() * 1000
            # After 2023-11-14: rules out 0, a small uptime counter, and seconds
            # mistakenly returned as milliseconds.
            assert server_ms > 1_700_000_000_000
            assert abs(server_ms - wall_ms) < 5 * 60 * 1000

            # And the deadline the acquire script actually writes is anchored on
            # it: roughly "now plus the configured budget".
            (entry,) = await _pending(client, config, "worker-a")
            lease = await store.acquire(
                config.stream, config.consumer_group, entry, consumer="worker-a"
            )
            expected_ms = wall_ms + _BUDGET_S * 1000
            assert abs(lease.budget.deadline_ms - expected_ms) < 60_000
            assert 0.0 < lease.remaining_s() <= _BUDGET_S

    asyncio.run(go())


# --- DeliveryBudget: monotonic anchoring (pure unit, no Valkey) ---------------


def test_remaining_s_is_driven_by_the_monotonic_anchor_not_the_wall_clock() -> None:
    """ADR-0131: "elapsed-time enforcement uses a monotonic clock anchored to the
    last Valkey-time observation so wall-clock adjustment cannot extend the
    budget."

    The server anchor here is deliberately ANCIENT (2001-09-09). A ``remaining_s``
    that consulted the wall clock -- ``time.time()`` against these absolute
    milliseconds -- would read this budget as having expired about 25 years ago,
    so every assertion below is red on that revert. Faking the monotonic source
    is done by choosing ``anchor_monotonic``, which is the whole reason it is a
    field rather than an implicit "now".
    """
    ancient_server_ms = 1_000_000_000_000  # 2001-09-09, ~25 years before now
    monotonic_now = time.monotonic()

    fresh = DeliveryBudget(
        deadline_ms=ancient_server_ms + 10_000,
        anchor_server_ms=ancient_server_ms,
        anchor_monotonic=monotonic_now,
    )
    assert 9.5 < fresh.remaining_s() <= 10.0

    # The SAME server-time facts, anchored four monotonic seconds ago: the only
    # thing that moved is the monotonic anchor, and remaining moves with it.
    aged = DeliveryBudget(
        deadline_ms=ancient_server_ms + 10_000,
        anchor_server_ms=ancient_server_ms,
        anchor_monotonic=monotonic_now - 4.0,
    )
    assert 5.5 < aged.remaining_s() <= 6.0

    # Re-anchoring on a later server observation (the shape a renewal preserves;
    # the production heartbeat builds an equivalent budget directly) keeps the
    # deadline and re-bases elapsed on "now".
    reanchored = aged.reanchor(ancient_server_ms + 4_000)
    assert reanchored.deadline_ms == aged.deadline_ms
    assert 5.5 < reanchored.remaining_s() <= 6.0
    assert reanchored.anchor_server_ms == ancient_server_ms + 4_000

    # An exhausted budget reads as non-positive rather than wrapping around.
    spent = DeliveryBudget(
        deadline_ms=ancient_server_ms + 1_000,
        anchor_server_ms=ancient_server_ms,
        anchor_monotonic=monotonic_now - 30.0,
    )
    assert spent.remaining_s() < 0.0


def test_delivery_lease_module_never_reads_the_wall_clock() -> None:
    """``time.time()`` anywhere in this module is the defect: an NTP step or a
    manual clock correction would then extend or destroy a live budget. The
    monotonic anchor is the only in-process clock, and Valkey server ``TIME`` is
    the only absolute one.

    This inspects actual ``time.X(...)`` call expressions via ``ast`` rather
    than scanning raw text, so mentioning ``time.time()`` in a comment or
    docstring (e.g. to document this very rule) cannot trip the assertion --
    only a real call would."""
    import ast

    source = (
        Path(__file__).resolve().parents[1]
        / "src/curie_worker/delivery_lease.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)

    def time_attr_calls(attr: str) -> list[ast.Call]:
        return [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == attr
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "time"
        ]

    assert time_attr_calls("time") == []
    # Proves the scan actually sees this module's clock reads, rather than
    # passing vacuously against an empty or unparsed source.
    assert len(time_attr_calls("monotonic")) > 0
