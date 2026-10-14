"""The channel read ledger both services share (ADR 0100, #2877).

The API mints, renews and charges against these keys; the worker revokes by
deleting the active key directly, without the API. One definition in
`curie_internal.channel_read_ledger` is what keeps the two from drifting, so
these tests drive the API's ledger and the worker's revoke against the same
real Valkey and check they agree on the keys.
"""

from __future__ import annotations

import asyncio
import hashlib
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

PREFIX = f"test:2877:{uuid.uuid4().hex[:8]}"
TTL_S = 3600


def _with_valkey(exercise: Callable[[Redis, uuid.UUID, str], Awaitable[None]]) -> None:
    async def run() -> None:
        client = Redis(
            host=VALKEY_HOST,
            port=VALKEY_PORT,
            password=VALKEY_PW or None,
            retry=Retry(NoBackoff(), 0),
        )
        agent = uuid.uuid4()
        turn = f"evt-{uuid.uuid4().hex}"
        try:
            await client.ping()
            await exercise(client, agent, turn)
        finally:
            try:
                stale = [key async for key in client.scan_iter(match=f"{PREFIX}:*")]
                if stale:
                    await client.delete(*stale)
            finally:
                await client.aclose()

    asyncio.run(run())


def _ledger(client: Redis) -> Any:
    from curie_api.channel_read.ledger import ChannelReadLedger

    return ChannelReadLedger(client, PREFIX)


def _keys(agent: uuid.UUID, turn: str) -> tuple[str, str, str, str]:
    from curie_internal.channel_read_ledger import ledger_keys, turn_key

    return ledger_keys(PREFIX, agent, turn_key(turn))


def test_key_layout_is_under_the_configured_prefix_in_one_slot() -> None:
    from curie_internal.channel_read_ledger import (
        LEDGER_TTL_S,
        MAX_PAGES_PER_TURN,
        ledger_keys,
        turn_key,
    )

    assert MAX_PAGES_PER_TURN == 8
    assert LEDGER_TTL_S == 7 * 24 * 60 * 60
    digest = turn_key("approval-x-resolved")
    assert digest == hashlib.sha256(b"approval-x-resolved").hexdigest()[:32]
    agent = uuid.UUID("00000000-0000-4000-8000-000000000001")
    keys = ledger_keys("acme:custom", agent, digest)
    tag = f"{{{agent}:{digest}}}"
    assert keys == (
        f"acme:custom:channel-read:{tag}:gen",
        f"acme:custom:channel-read:{tag}:active",
        f"acme:custom:channel-read:{tag}:pages",
        f"acme:custom:channel-read:{tag}:marker",
    )
    # No `curie:` literal leaks in when the prefix is something else.
    assert not any("curie" in key for key in keys)


def test_open_writes_the_keys_the_shared_layout_names() -> None:
    from curie_internal.channel_read_ledger import LEDGER_TTL_S

    async def exercise(client: Redis, agent: uuid.UUID, turn: str) -> None:
        gen_key, active_key, pages_key, marker_key = _keys(agent, turn)
        gen = await _ledger(client).open(agent, turn, "owner-aaaaaaaaaaaaaaaa", TTL_S, resume=False)
        assert gen == 1
        assert await client.get(active_key) == b"owner-aaaaaaaaaaaaaaaa:1"
        assert await client.get(pages_key) == b"0"
        assert await client.exists(marker_key) == 1
        assert int(await client.get(gen_key)) == 1
        assert 0 < await client.ttl(active_key) <= TTL_S
        for key in (gen_key, pages_key, marker_key):
            assert TTL_S < await client.ttl(key) <= LEDGER_TTL_S

    _with_valkey(exercise)


def test_revoke_is_owner_checked() -> None:
    from curie_internal.channel_read_ledger import revoke_owner, turn_key

    async def exercise(client: Redis, agent: uuid.UUID, turn: str) -> None:
        ledger = _ledger(client)
        gen = await ledger.open(agent, turn, "owner-aaaaaaaaaaaaaaaa", TTL_S, resume=False)
        digest = turn_key(turn)
        assert await revoke_owner(client, PREFIX, agent, digest, "owner-bbbbbbbbbbbbbbbb") is False
        assert await ledger.is_current(agent, turn, gen) is True
        assert await revoke_owner(client, PREFIX, agent, digest, "owner-aaaaaaaaaaaaaaaa") is True
        assert await ledger.is_current(agent, turn, gen) is False
        # The budget and the marker outlive the revoke, so a resume continues it.
        _, _, pages_key, marker_key = _keys(agent, turn)
        assert await client.exists(pages_key, marker_key) == 2
        # Revoking twice is a no-op, not an error.
        assert await revoke_owner(client, PREFIX, agent, digest, "owner-aaaaaaaaaaaaaaaa") is False

    _with_valkey(exercise)


def test_a_later_opener_is_not_killed_by_the_earlier_owner() -> None:
    from curie_internal.channel_read_ledger import revoke_owner, turn_key

    async def exercise(client: Redis, agent: uuid.UUID, turn: str) -> None:
        ledger = _ledger(client)
        await ledger.open(agent, turn, "owner-aaaaaaaaaaaaaaaa", TTL_S, resume=False)
        resumed = await ledger.open(agent, turn, "owner-bbbbbbbbbbbbbbbb", TTL_S, resume=True)
        assert resumed == 2
        digest = turn_key(turn)
        assert await revoke_owner(client, PREFIX, agent, digest, "owner-aaaaaaaaaaaaaaaa") is False
        assert await ledger.is_current(agent, turn, resumed) is True
        assert await ledger.is_current(agent, turn, 1) is False

    _with_valkey(exercise)


def test_steer_bumps_only_with_an_active_key_and_keeps_the_owner() -> None:
    from curie_internal.channel_read_ledger import revoke_owner, turn_key

    async def exercise(client: Redis, agent: uuid.UUID, turn: str) -> None:
        ledger = _ledger(client)
        # Nothing opened: a steer gets nothing and writes nothing.
        assert await ledger.steer(agent, turn, TTL_S) is None
        assert await client.exists(*_keys(agent, turn)) == 0

        gen = await ledger.open(agent, turn, "owner-aaaaaaaaaaaaaaaa", TTL_S, resume=False)
        steered = await ledger.steer(agent, turn, TTL_S)
        assert steered is not None
        new_gen, remaining_ms = steered
        assert new_gen == gen + 1
        assert 0 < remaining_ms <= TTL_S * 1000
        assert await ledger.is_current(agent, turn, gen) is False
        assert await ledger.is_current(agent, turn, new_gen) is True
        # The opener's revoke still kills the steer generation.
        assert await revoke_owner(client, PREFIX, agent, turn_key(turn), "owner-aaaaaaaaaaaaaaaa")
        assert await ledger.is_current(agent, turn, new_gen) is False
        assert await ledger.steer(agent, turn, TTL_S) is None

    _with_valkey(exercise)


def test_reserve_caps_at_eight_and_release_returns_a_page() -> None:
    async def exercise(client: Redis, agent: uuid.UUID, turn: str) -> None:
        ledger = _ledger(client)
        gen = await ledger.open(agent, turn, "owner-aaaaaaaaaaaaaaaa", TTL_S, resume=False)
        outcomes = await asyncio.gather(*(ledger.reserve(agent, turn, gen) for _ in range(12)))
        assert outcomes.count("reserved") == 8
        assert outcomes.count("exhausted") == 4
        await ledger.release(agent, turn)
        assert await ledger.reserve(agent, turn, gen) == "reserved"
        assert await ledger.reserve(agent, turn, gen) == "exhausted"
        # Release never drives the count below zero.
        fresh = f"{turn}-fresh"
        fresh_gen = await ledger.open(agent, fresh, "owner-cccccccccccccccc", TTL_S, resume=False)
        await ledger.release(agent, fresh)
        _, _, pages_key, _ = _keys(agent, fresh)
        assert await client.get(pages_key) == b"0"
        assert await ledger.reserve(agent, fresh, fresh_gen) == "reserved"

    _with_valkey(exercise)


def test_reserve_refuses_a_stale_generation_and_an_expired_budget() -> None:
    async def exercise(client: Redis, agent: uuid.UUID, turn: str) -> None:
        ledger = _ledger(client)
        gen = await ledger.open(agent, turn, "owner-aaaaaaaaaaaaaaaa", TTL_S, resume=False)
        steered = await ledger.steer(agent, turn, TTL_S)
        assert steered is not None
        assert await ledger.reserve(agent, turn, gen) == "inactive"
        assert await ledger.reserve(agent, turn, steered[0]) == "reserved"
        _, _, pages_key, _ = _keys(agent, turn)
        await client.delete(pages_key)
        assert await ledger.reserve(agent, turn, steered[0]) == "expired"

    _with_valkey(exercise)


def test_resume_without_the_marker_is_expired_and_writes_nothing() -> None:
    async def exercise(client: Redis, agent: uuid.UUID, turn: str) -> None:
        ledger = _ledger(client)
        assert await ledger.open(agent, turn, "owner-aaaaaaaaaaaaaaaa", TTL_S, resume=True) == (
            "expired"
        )
        assert await client.exists(*_keys(agent, turn)) == 0
        # Liveness: the first open of a fresh turn is not a resume and succeeds.
        assert await ledger.open(agent, turn, "owner-aaaaaaaaaaaaaaaa", TTL_S, resume=False) == 1
        assert await ledger.open(agent, turn, "owner-bbbbbbbbbbbbbbbb", TTL_S, resume=True) == 2

    _with_valkey(exercise)


# A lost mint response (review round 1): the API commits the open, the worker
# never sees it, and the worker still revokes at attempt end through an owner
# index the open script writes in the same script.


def test_revoke_by_owner_removes_only_that_owners_active_entries() -> None:
    from curie_internal.channel_read_ledger import revoke_by_owner

    async def exercise(client: Redis, agent: uuid.UUID, turn: str) -> None:
        ledger = _ledger(client)
        # One attempt may open two logical turns (a work item continuation).
        first = await ledger.open(agent, turn, "owner-aaaaaaaaaaaaaaaa", TTL_S, resume=False)
        second_turn = f"{turn}-continued"
        second = await ledger.open(
            agent, second_turn, "owner-aaaaaaaaaaaaaaaa", TTL_S, resume=False
        )
        other_turn = f"{turn}-other"
        other = await ledger.open(agent, other_turn, "owner-bbbbbbbbbbbbbbbb", TTL_S, resume=False)

        assert await revoke_by_owner(client, PREFIX, agent, "owner-aaaaaaaaaaaaaaaa")
        assert await ledger.is_current(agent, turn, first) is False
        assert await ledger.is_current(agent, second_turn, second) is False
        assert await ledger.is_current(agent, other_turn, other) is True
        # The budget and the marker outlive it, as with ``revoke_owner``.
        _, _, pages_key, marker_key = _keys(agent, turn)
        assert await client.exists(pages_key, marker_key) == 2
        # Twice is a no-op, not an error.
        assert not await revoke_by_owner(client, PREFIX, agent, "owner-aaaaaaaaaaaaaaaa")
        assert await ledger.is_current(agent, other_turn, other) is True

    _with_valkey(exercise)


def test_revoke_by_owner_is_a_no_op_for_a_newer_owner() -> None:
    from curie_internal.channel_read_ledger import revoke_by_owner

    async def exercise(client: Redis, agent: uuid.UUID, turn: str) -> None:
        ledger = _ledger(client)
        await ledger.open(agent, turn, "owner-aaaaaaaaaaaaaaaa", TTL_S, resume=False)
        resumed = await ledger.open(agent, turn, "owner-bbbbbbbbbbbbbbbb", TTL_S, resume=True)

        assert not await revoke_by_owner(client, PREFIX, agent, "owner-aaaaaaaaaaaaaaaa")
        assert await ledger.is_current(agent, turn, resumed) is True
        # A steer under the newer owner is not the earlier owner's either.
        steered = await ledger.steer(agent, turn, TTL_S)
        assert steered is not None
        assert not await revoke_by_owner(client, PREFIX, agent, "owner-aaaaaaaaaaaaaaaa")
        assert await ledger.is_current(agent, turn, steered[0]) is True
        assert await revoke_by_owner(client, PREFIX, agent, "owner-bbbbbbbbbbbbbbbb")
        assert await ledger.is_current(agent, turn, steered[0]) is False

    _with_valkey(exercise)


def test_an_expired_resume_writes_no_owner_index() -> None:
    from curie_internal.channel_read_ledger import revoke_by_owner

    async def exercise(client: Redis, agent: uuid.UUID, turn: str) -> None:
        ledger = _ledger(client)
        assert await ledger.open(agent, turn, "owner-aaaaaaaaaaaaaaaa", TTL_S, resume=True) == (
            "expired"
        )
        assert [key async for key in client.scan_iter(match=f"{PREFIX}:*")] == []
        assert not await revoke_by_owner(client, PREFIX, agent, "owner-aaaaaaaaaaaaaaaa")

    _with_valkey(exercise)


# Review round 2: the active entry is a lease the opener renews, and a settled
# owner is tombstoned so an open committed after settlement is refused.


def test_the_active_entry_lives_on_the_lease_not_the_capability_ttl() -> None:
    from curie_internal.channel_read_ledger import LEASE_TTL_S

    async def exercise(client: Redis, agent: uuid.UUID, turn: str) -> None:
        assert LEASE_TTL_S == 90
        ledger = _ledger(client)
        _, active_key, _, _ = _keys(agent, turn)
        await ledger.open(agent, turn, "owner-aaaaaaaaaaaaaaaa", TTL_S, resume=False)
        assert 0 < await client.ttl(active_key) <= LEASE_TTL_S
        # A steer keeps the owner and the remaining lease.
        await client.expire(active_key, 20)
        steered = await ledger.steer(agent, turn, TTL_S)
        assert steered is not None
        assert 0 < await client.ttl(active_key) <= 20

    _with_valkey(exercise)


def test_refresh_is_owner_checked_and_never_extends_a_successor() -> None:
    from curie_internal.channel_read_ledger import LEASE_TTL_S, refresh_owner, turn_key

    async def exercise(client: Redis, agent: uuid.UUID, turn: str) -> None:
        ledger = _ledger(client)
        digest = turn_key(turn)
        _, active_key, _, _ = _keys(agent, turn)
        await ledger.open(agent, turn, "owner-aaaaaaaaaaaaaaaa", TTL_S, resume=False)
        await ledger.open(agent, turn, "owner-bbbbbbbbbbbbbbbb", TTL_S, resume=True)
        await client.expire(active_key, 10)

        assert not await refresh_owner(client, PREFIX, agent, digest, "owner-aaaaaaaaaaaaaaaa")
        assert 0 < await client.ttl(active_key) <= 10
        # A prefix of the owner is not the owner.
        assert not await refresh_owner(client, PREFIX, agent, digest, "owner-bbbb")
        assert 0 < await client.ttl(active_key) <= 10

        assert await refresh_owner(client, PREFIX, agent, digest, "owner-bbbbbbbbbbbbbbbb")
        assert 10 < await client.ttl(active_key) <= LEASE_TTL_S
        assert await client.get(active_key) == b"owner-bbbbbbbbbbbbbbbb:2"
        # Nothing to refresh once revoked.
        await client.delete(active_key)
        assert not await refresh_owner(client, PREFIX, agent, digest, "owner-bbbbbbbbbbbbbbbb")
        assert await client.exists(active_key) == 0

    _with_valkey(exercise)


def test_a_tombstoned_owner_cannot_open() -> None:
    from curie_internal.channel_read_ledger import tombstone_owner

    async def exercise(client: Redis, agent: uuid.UUID, turn: str) -> None:
        ledger = _ledger(client)
        _, active_key, _, _ = _keys(agent, turn)
        await tombstone_owner(client, PREFIX, agent, "owner-aaaaaaaaaaaaaaaa", TTL_S)

        late = await ledger.open(agent, turn, "owner-aaaaaaaaaaaaaaaa", TTL_S, resume=False)
        assert not (isinstance(late, int) and late > 0), late
        assert await client.exists(active_key) == 0
        # Positive control: a live owner opens the same turn.
        live = await ledger.open(agent, turn, "owner-bbbbbbbbbbbbbbbb", TTL_S, resume=False)
        assert isinstance(live, int) and live >= 1
        assert (await client.get(active_key)).startswith(b"owner-bbbbbbbbbbbbbbbb:")

    _with_valkey(exercise)
