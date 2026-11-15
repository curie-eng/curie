"""The provider gate ledger against a real Valkey (#5426).

Each test runs on its own random prefix and deletes its keys. An unreachable
Valkey skips locally and fails when ``CI_REQUIRE_VALKEY_TESTS`` is set.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable, Iterator
from decimal import Decimal
from typing import Literal, Protocol, TypeVar

import pytest
import redis
from curie_internal.provider_gate_ledger import (
    CURRENT_TTL_S,
    SCOPE_TTL_S,
    ScopeObservation,
    claim_probe,
    current_key,
    find_provider,
    lease_key,
    read_current,
    read_scope,
    record_probe,
    register_current,
    scope_hash_key,
)
from curie_internal.provider_scope import AllowanceReading, ProviderScope, scope_key
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW, connect_or_skip
from redis.asyncio import Redis as AsyncRedis

OPENROUTER = "openrouter"
SCOPE = ProviderScope(
    provider=OPENROUTER,
    host="openrouter.ai",
    scope_key=scope_key(OPENROUTER, "openrouter.ai", "sk-or-v1-" + "0" * 64),
)
OTHER_SCOPE = scope_key(OPENROUTER, "openrouter.ai", "sk-or-v1-" + "1" * 64)


def _dec(value: str | None) -> Decimal | None:
    return None if value is None else Decimal(value)


def _auth(limit: str | None, remaining: str | None = "1") -> AllowanceReading:
    return AllowanceReading("authenticated", _dec(remaining), _dec(limit))


@pytest.fixture
def valkey() -> Iterator[redis.Redis]:
    client = connect_or_skip(decode_responses=True)
    yield client
    client.close()


@pytest.fixture
def prefix(valkey: redis.Redis) -> Iterator[str]:
    value = f"test-provider-gate-{uuid.uuid4().hex}"
    yield value
    keys = list(valkey.scan_iter(match=f"{value}:*"))
    if keys:
        valkey.delete(*keys)


T = TypeVar("T")


class Run(Protocol):
    def __call__(self, body: Callable[[AsyncRedis], Awaitable[T]]) -> T: ...


@pytest.fixture(params=[True, False], ids=["decoded", "bytes"])
def run(valkey: redis.Redis, request: pytest.FixtureRequest) -> Run:
    """Run one coroutine on a fresh async client in either response mode."""

    decode = bool(request.param)

    def runner(body: Callable[[AsyncRedis], Awaitable[T]]) -> T:
        async def main() -> T:
            client = AsyncRedis(
                host=VALKEY_HOST,
                port=VALKEY_PORT,
                password=VALKEY_PW or None,
                decode_responses=decode,
            )
            try:
                return await body(client)
            finally:
                await client.aclose()

        return asyncio.run(main())

    return runner


def test_register_current_then_read(run: Run, prefix: str, valkey: redis.Redis) -> None:
    async def body(
        client: AsyncRedis,
    ) -> tuple[str | None, str | None, ScopeObservation | None]:
        await register_current(client, prefix, SCOPE, 1000)
        return (
            await read_current(client, prefix, OPENROUTER),
            await find_provider(client, prefix, SCOPE.scope_key),
            await read_scope(client, prefix, OPENROUTER, SCOPE.scope_key),
        )

    current, provider, observation = run(body)
    assert current == SCOPE.scope_key
    assert provider == OPENROUTER
    assert observation == ScopeObservation(OPENROUTER, 1000, None, None, None, None, None)
    assert 0 < valkey.ttl(current_key(prefix, OPENROUTER)) <= CURRENT_TTL_S
    assert (
        CURRENT_TTL_S
        < valkey.ttl(scope_hash_key(prefix, OPENROUTER, SCOPE.scope_key))
        <= SCOPE_TTL_S
    )


def test_absent_reads_are_none(run: Run, prefix: str) -> None:
    async def body(
        client: AsyncRedis,
    ) -> tuple[str | None, str | None, ScopeObservation | None]:
        return (
            await read_current(client, prefix, OPENROUTER),
            await find_provider(client, prefix, OTHER_SCOPE),
            await read_scope(client, prefix, OPENROUTER, OTHER_SCOPE),
        )

    assert run(body) == (None, None, None)


def test_find_provider_refuses_an_ambiguous_scope(run: Run, prefix: str) -> None:
    async def body(client: AsyncRedis) -> str | None:
        await register_current(client, prefix, SCOPE, 1)
        await register_current(
            client, prefix, ProviderScope("anthropic", "api.anthropic.com", SCOPE.scope_key), 1
        )
        return await find_provider(client, prefix, SCOPE.scope_key)

    assert run(body) is None


def test_claim_probe_is_exclusive(run: Run, prefix: str, valkey: redis.Redis) -> None:
    async def body(client: AsyncRedis) -> tuple[str | None, str | None]:
        return (
            await claim_probe(client, prefix, OPENROUTER, SCOPE.scope_key, "owner-a"),
            await claim_probe(client, prefix, OPENROUTER, SCOPE.scope_key, "owner-b"),
        )

    first, second = run(body)
    assert first is not None
    assert first.startswith("owner-a:")
    assert second is None
    assert valkey.get(lease_key(prefix, OPENROUTER, SCOPE.scope_key)) == first


def test_each_claim_mints_a_fresh_token(run: Run, prefix: str) -> None:
    async def body(client: AsyncRedis) -> tuple[str | None, str | None]:
        first = await claim_probe(client, prefix, OPENROUTER, SCOPE.scope_key, "owner-a")
        await client.delete(lease_key(prefix, OPENROUTER, SCOPE.scope_key))
        second = await claim_probe(client, prefix, OPENROUTER, SCOPE.scope_key, "owner-a")
        return first, second

    first, second = run(body)
    assert first is not None
    assert second is not None
    assert first != second
    assert first.startswith("owner-a:")
    assert second.startswith("owner-a:")


async def _lapse_lease(client: AsyncRedis, prefix: str) -> None:
    lease = lease_key(prefix, OPENROUTER, SCOPE.scope_key)
    await client.pexpire(lease, 1)
    # Valkey expires a key on access, so this ends within a few reads.
    while await client.exists(lease):
        pass


def test_crashed_owner_lease_lapses(run: Run, prefix: str) -> None:
    async def body(
        client: AsyncRedis,
    ) -> tuple[str | None, str | None, bool, bool, ScopeObservation | None]:
        first = await claim_probe(client, prefix, OPENROUTER, SCOPE.scope_key, "owner-a")
        assert first is not None
        await _lapse_lease(client, prefix)
        second = await claim_probe(client, prefix, OPENROUTER, SCOPE.scope_key, "owner-b")
        assert second is not None
        late = await record_probe(
            client, prefix, OPENROUTER, SCOPE.scope_key, first, 5, _auth("10", "1")
        )
        valid = await record_probe(
            client, prefix, OPENROUTER, SCOPE.scope_key, second, 6, _auth("10", "2")
        )
        return (
            first,
            second,
            late,
            valid,
            await read_scope(client, prefix, OPENROUTER, SCOPE.scope_key),
        )

    first, second, late, valid, observation = run(body)
    assert first is not None
    assert second is not None
    assert (late, valid) == (False, True)
    assert observation == ScopeObservation(
        OPENROUTER, None, 6, "authenticated", Decimal("10"), Decimal("2"), None
    )


def test_a_restarted_owner_cannot_commit_its_lapsed_claim(run: Run, prefix: str) -> None:
    # The same owner name claims, its lease lapses (a crash), and it claims
    # again after a restart. Only the second claim's token can record.
    async def body(
        client: AsyncRedis,
    ) -> tuple[bool, bool, ScopeObservation | None]:
        first = await claim_probe(client, prefix, OPENROUTER, SCOPE.scope_key, "owner-a")
        assert first is not None
        await _lapse_lease(client, prefix)
        second = await claim_probe(client, prefix, OPENROUTER, SCOPE.scope_key, "owner-a")
        assert second is not None
        assert second != first
        late = await record_probe(
            client, prefix, OPENROUTER, SCOPE.scope_key, first, 5, _auth("99", "99")
        )
        valid = await record_probe(
            client, prefix, OPENROUTER, SCOPE.scope_key, second, 6, _auth("10", "2")
        )
        return late, valid, await read_scope(client, prefix, OPENROUTER, SCOPE.scope_key)

    late, valid, observation = run(body)
    assert (late, valid) == (False, True)
    assert observation == ScopeObservation(
        OPENROUTER, None, 6, "authenticated", Decimal("10"), Decimal("2"), None
    )


def test_non_owner_record_writes_nothing(run: Run, prefix: str, valkey: redis.Redis) -> None:
    async def body(client: AsyncRedis) -> tuple[str | None, bool]:
        token = await claim_probe(client, prefix, OPENROUTER, SCOPE.scope_key, "owner-a")
        refused = await record_probe(
            client, prefix, OPENROUTER, SCOPE.scope_key, "owner-a", 5, _auth("9")
        )
        return token, refused

    token, refused = run(body)
    assert refused is False
    assert not valkey.exists(scope_hash_key(prefix, OPENROUTER, SCOPE.scope_key))
    assert valkey.get(lease_key(prefix, OPENROUTER, SCOPE.scope_key)) == token


def test_record_without_any_lease_is_refused(run: Run, prefix: str, valkey: redis.Redis) -> None:
    async def body(client: AsyncRedis) -> bool:
        return await record_probe(
            client, prefix, OPENROUTER, SCOPE.scope_key, "owner-a:0", 5, _auth("9")
        )

    assert run(body) is False
    assert not valkey.exists(scope_hash_key(prefix, OPENROUTER, SCOPE.scope_key))


async def _probe(
    client: AsyncRedis, prefix: str, now_ms: int, reading: AllowanceReading
) -> ScopeObservation:
    token = await claim_probe(client, prefix, OPENROUTER, SCOPE.scope_key, f"owner-{now_ms}")
    assert token is not None
    assert await record_probe(client, prefix, OPENROUTER, SCOPE.scope_key, token, now_ms, reading)
    observation = await read_scope(client, prefix, OPENROUTER, SCOPE.scope_key)
    assert observation is not None
    return observation


def test_allowance_increase_tracking(run: Run, prefix: str, valkey: redis.Redis) -> None:
    async def body(client: AsyncRedis) -> list[ScopeObservation]:
        await register_current(client, prefix, SCOPE, 50)
        return [
            await _probe(client, prefix, 100, _auth("5", "0")),
            await _probe(client, prefix, 200, _auth("5", "0")),
            await _probe(client, prefix, 300, _auth("4.5", "0")),
            await _probe(client, prefix, 400, _auth("4.75", "0.25")),
            await _probe(client, prefix, 500, _auth("4.75", "0.25")),
            await _probe(client, prefix, 600, _auth("1", "0")),
        ]

    first, equal, lower, higher, equal_again, lower_again = run(body)
    assert first == ScopeObservation(
        OPENROUTER, 50, 100, "authenticated", Decimal("5"), Decimal("0"), None
    )
    assert equal.allowance_increased_at_ms is None
    assert lower.allowance_increased_at_ms is None
    assert lower.limit == Decimal("4.5")
    assert higher == ScopeObservation(
        OPENROUTER, 50, 400, "authenticated", Decimal("4.75"), Decimal("0.25"), 400
    )
    assert equal_again.allowance_increased_at_ms == 400
    assert lower_again.allowance_increased_at_ms == 400
    assert lower_again.limit == Decimal("1")
    assert not valkey.exists(lease_key(prefix, OPENROUTER, SCOPE.scope_key))


def test_rising_remaining_under_an_equal_limit_is_not_an_increase(run: Run, prefix: str) -> None:
    # A usage fall, a refund or a period rollover raises limit_remaining with
    # the limit unchanged; none of them is added allowance.
    async def body(client: AsyncRedis) -> tuple[ScopeObservation, ScopeObservation]:
        await _probe(client, prefix, 100, _auth("10", "0"))
        risen = await _probe(client, prefix, 200, _auth("10", "10"))
        risen_again = await _probe(client, prefix, 300, _auth("10.0", "7.5"))
        return risen, risen_again

    risen, risen_again = run(body)
    assert risen == ScopeObservation(
        OPENROUTER, None, 200, "authenticated", Decimal("10"), Decimal("10"), None
    )
    assert risen_again.allowance_increased_at_ms is None
    assert risen_again.limit_remaining == Decimal("7.5")


def test_a_null_new_limit_is_not_an_increase(run: Run, prefix: str) -> None:
    async def body(client: AsyncRedis) -> tuple[ScopeObservation, ScopeObservation]:
        await _probe(client, prefix, 100, _auth("10", "0"))
        unlimited = await _probe(client, prefix, 200, _auth(None, None))
        numeric = await _probe(client, prefix, 300, _auth("20", "10"))
        return unlimited, numeric

    unlimited, numeric = run(body)
    assert unlimited == ScopeObservation(OPENROUTER, None, 200, "authenticated", None, None, None)
    # A numeric limit after a stored empty one is a new baseline, not an increase.
    assert numeric == ScopeObservation(
        OPENROUTER, None, 300, "authenticated", Decimal("20"), Decimal("10"), None
    )


@pytest.mark.parametrize("outcome", ["rejected", "unreadable"])
def test_rejected_and_unreadable_keep_the_baseline(
    run: Run, prefix: str, outcome: Literal["rejected", "unreadable"]
) -> None:
    async def body(client: AsyncRedis) -> tuple[ScopeObservation, ScopeObservation]:
        await _probe(client, prefix, 100, _auth("5", "0"))
        kept = await _probe(client, prefix, 200, AllowanceReading(outcome, None, None))
        after = await _probe(client, prefix, 300, _auth("6", "1"))
        return kept, after

    kept, after = run(body)
    assert kept == ScopeObservation(
        OPENROUTER, None, 200, outcome, Decimal("5"), Decimal("0"), None
    )
    assert after == ScopeObservation(
        OPENROUTER, None, 300, "authenticated", Decimal("6"), Decimal("1"), 300
    )


@pytest.mark.parametrize("outcome", ["rejected", "unreadable"])
def test_rejected_and_unreadable_keep_a_recorded_increase(
    run: Run, prefix: str, outcome: Literal["rejected", "unreadable"]
) -> None:
    async def body(client: AsyncRedis) -> ScopeObservation:
        await _probe(client, prefix, 100, _auth("5", "0"))
        await _probe(client, prefix, 200, _auth("6", "1"))
        return await _probe(client, prefix, 300, AllowanceReading(outcome, None, None))

    assert run(body) == ScopeObservation(
        OPENROUTER, None, 300, outcome, Decimal("6"), Decimal("1"), 200
    )


def test_unknown_limit_is_not_a_baseline(run: Run, prefix: str) -> None:
    async def body(
        client: AsyncRedis,
    ) -> tuple[ScopeObservation, object, object, ScopeObservation]:
        unknown = await _probe(client, prefix, 100, _auth(None, None))
        hash_key = scope_hash_key(prefix, OPENROUTER, SCOPE.scope_key)
        stored_limit = await client.hget(hash_key, "limit")
        stored_remaining = await client.hget(hash_key, "limit_remaining")
        numeric = await _probe(client, prefix, 200, _auth("10", "4"))
        return unknown, stored_limit, stored_remaining, numeric

    unknown, stored_limit, stored_remaining, numeric = run(body)
    assert unknown == ScopeObservation(OPENROUTER, None, 100, "authenticated", None, None, None)
    assert stored_limit in ("", b"")
    assert stored_remaining in ("", b"")
    assert numeric == ScopeObservation(
        OPENROUTER, None, 200, "authenticated", Decimal("10"), Decimal("4"), None
    )


def test_decimal_is_stored_without_exponent(run: Run, prefix: str, valkey: redis.Redis) -> None:
    async def body(client: AsyncRedis) -> ScopeObservation:
        return await _probe(client, prefix, 100, _auth("2E+1", "1E-7"))

    observation = run(body)
    assert observation.limit == Decimal("20")
    assert observation.limit_remaining == Decimal("0.0000001")
    hash_key = scope_hash_key(prefix, OPENROUTER, SCOPE.scope_key)
    assert valkey.hget(hash_key, "limit") == "20"
    assert valkey.hget(hash_key, "limit_remaining") == "0.0000001"
    assert 0 < valkey.ttl(hash_key) <= SCOPE_TTL_S


def test_malformed_stored_numbers_read_as_none(run: Run, prefix: str, valkey: redis.Redis) -> None:
    valkey.hset(
        scope_hash_key(prefix, OPENROUTER, SCOPE.scope_key),
        mapping={
            "provider": OPENROUTER,
            "current_at_ms": "soon",
            "probed_at_ms": "",
            "outcome": "authenticated",
            "limit": "lots",
            "limit_remaining": "plenty",
            "allowance_increased_at_ms": "1.5",
        },
    )

    async def body(client: AsyncRedis) -> ScopeObservation | None:
        return await read_scope(client, prefix, OPENROUTER, SCOPE.scope_key)

    assert run(body) == ScopeObservation(OPENROUTER, None, None, "authenticated", None, None, None)
