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


def _auth(value: str | None) -> AllowanceReading:
    return AllowanceReading("authenticated", None if value is None else Decimal(value))


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
    assert observation == ScopeObservation(OPENROUTER, 1000, None, None, None, None)
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


def test_claim_probe_is_exclusive(run: Run, prefix: str) -> None:
    async def body(client: AsyncRedis) -> tuple[bool, bool]:
        return (
            await claim_probe(client, prefix, OPENROUTER, SCOPE.scope_key, "owner-a"),
            await claim_probe(client, prefix, OPENROUTER, SCOPE.scope_key, "owner-b"),
        )

    assert run(body) == (True, False)


def test_crashed_owner_lease_lapses(run: Run, prefix: str) -> None:
    async def body(
        client: AsyncRedis,
    ) -> tuple[bool, bool, bool, bool, ScopeObservation | None]:
        first = await claim_probe(client, prefix, OPENROUTER, SCOPE.scope_key, "owner-a")
        lease = lease_key(prefix, OPENROUTER, SCOPE.scope_key)
        await client.pexpire(lease, 1)
        # Valkey expires a key on access, so this ends within a few reads.
        while await client.exists(lease):
            pass
        second = await claim_probe(client, prefix, OPENROUTER, SCOPE.scope_key, "owner-b")
        late = await record_probe(
            client, prefix, OPENROUTER, SCOPE.scope_key, "owner-a", 5, _auth("1")
        )
        valid = await record_probe(
            client, prefix, OPENROUTER, SCOPE.scope_key, "owner-b", 6, _auth("2")
        )
        return (
            first,
            second,
            late,
            valid,
            await read_scope(client, prefix, OPENROUTER, SCOPE.scope_key),
        )

    first, second, late, valid, observation = run(body)
    assert (first, second, late, valid) == (True, True, False, True)
    assert observation == ScopeObservation(OPENROUTER, None, 6, "authenticated", Decimal("2"), None)


def test_non_owner_record_writes_nothing(run: Run, prefix: str, valkey: redis.Redis) -> None:
    async def body(client: AsyncRedis) -> bool:
        await claim_probe(client, prefix, OPENROUTER, SCOPE.scope_key, "owner-a")
        return await record_probe(
            client, prefix, OPENROUTER, SCOPE.scope_key, "owner-b", 5, _auth("9")
        )

    assert run(body) is False
    assert not valkey.exists(scope_hash_key(prefix, OPENROUTER, SCOPE.scope_key))
    assert valkey.get(lease_key(prefix, OPENROUTER, SCOPE.scope_key)) == "owner-a"


def test_record_without_any_lease_is_refused(run: Run, prefix: str, valkey: redis.Redis) -> None:
    async def body(client: AsyncRedis) -> bool:
        return await record_probe(
            client, prefix, OPENROUTER, SCOPE.scope_key, "owner-a", 5, _auth("9")
        )

    assert run(body) is False
    assert not valkey.exists(scope_hash_key(prefix, OPENROUTER, SCOPE.scope_key))


async def _probe(
    client: AsyncRedis, prefix: str, now_ms: int, reading: AllowanceReading
) -> ScopeObservation:
    owner = f"owner-{now_ms}"
    assert await claim_probe(client, prefix, OPENROUTER, SCOPE.scope_key, owner)
    assert await record_probe(client, prefix, OPENROUTER, SCOPE.scope_key, owner, now_ms, reading)
    observation = await read_scope(client, prefix, OPENROUTER, SCOPE.scope_key)
    assert observation is not None
    return observation


def test_allowance_increase_tracking(run: Run, prefix: str, valkey: redis.Redis) -> None:
    async def body(client: AsyncRedis) -> list[ScopeObservation]:
        await register_current(client, prefix, SCOPE, 50)
        return [
            await _probe(client, prefix, 100, _auth("5")),
            await _probe(client, prefix, 200, _auth("5")),
            await _probe(client, prefix, 300, _auth("4.5")),
            await _probe(client, prefix, 400, _auth("4.75")),
            await _probe(client, prefix, 500, _auth("4.75")),
            await _probe(client, prefix, 600, _auth("1")),
        ]

    first, equal, lower, higher, equal_again, lower_again = run(body)
    assert first == ScopeObservation(OPENROUTER, 50, 100, "authenticated", Decimal("5"), None)
    assert equal.allowance_increased_at_ms is None
    assert lower.allowance_increased_at_ms is None
    assert lower.limit_remaining == Decimal("4.5")
    assert higher == ScopeObservation(OPENROUTER, 50, 400, "authenticated", Decimal("4.75"), 400)
    assert equal_again.allowance_increased_at_ms == 400
    assert lower_again.allowance_increased_at_ms == 400
    assert lower_again.limit_remaining == Decimal("1")
    assert not valkey.exists(lease_key(prefix, OPENROUTER, SCOPE.scope_key))


@pytest.mark.parametrize("outcome", ["rejected", "unreadable"])
def test_rejected_and_unreadable_keep_the_baseline(
    run: Run, prefix: str, outcome: Literal["rejected", "unreadable"]
) -> None:
    async def body(client: AsyncRedis) -> tuple[ScopeObservation, ScopeObservation]:
        await _probe(client, prefix, 100, _auth("5"))
        kept = await _probe(client, prefix, 200, AllowanceReading(outcome, None))
        after = await _probe(client, prefix, 300, _auth("6"))
        return kept, after

    kept, after = run(body)
    assert kept == ScopeObservation(OPENROUTER, None, 200, outcome, Decimal("5"), None)
    assert after == ScopeObservation(OPENROUTER, None, 300, "authenticated", Decimal("6"), 300)


def test_unknown_remaining_is_not_a_baseline(run: Run, prefix: str) -> None:
    async def body(client: AsyncRedis) -> tuple[ScopeObservation, object, ScopeObservation]:
        unknown = await _probe(client, prefix, 100, _auth(None))
        stored = await client.hget(
            scope_hash_key(prefix, OPENROUTER, SCOPE.scope_key), "limit_remaining"
        )
        numeric = await _probe(client, prefix, 200, _auth("10"))
        return unknown, stored, numeric

    unknown, stored, numeric = run(body)
    assert unknown == ScopeObservation(OPENROUTER, None, 100, "authenticated", None, None)
    assert stored in ("", b"")
    assert numeric == ScopeObservation(OPENROUTER, None, 200, "authenticated", Decimal("10"), None)


def test_decimal_is_stored_without_exponent(run: Run, prefix: str, valkey: redis.Redis) -> None:
    async def body(client: AsyncRedis) -> ScopeObservation:
        return await _probe(client, prefix, 100, _auth("1E-7"))

    observation = run(body)
    assert observation.limit_remaining == Decimal("0.0000001")
    assert (
        valkey.hget(scope_hash_key(prefix, OPENROUTER, SCOPE.scope_key), "limit_remaining")
        == "0.0000001"
    )
    assert 0 < valkey.ttl(scope_hash_key(prefix, OPENROUTER, SCOPE.scope_key)) <= SCOPE_TTL_S


def test_malformed_stored_numbers_read_as_none(run: Run, prefix: str, valkey: redis.Redis) -> None:
    valkey.hset(
        scope_hash_key(prefix, OPENROUTER, SCOPE.scope_key),
        mapping={
            "provider": OPENROUTER,
            "current_at_ms": "soon",
            "probed_at_ms": "",
            "outcome": "authenticated",
            "limit_remaining": "plenty",
            "allowance_increased_at_ms": "1.5",
        },
    )

    async def body(client: AsyncRedis) -> ScopeObservation | None:
        return await read_scope(client, prefix, OPENROUTER, SCOPE.scope_key)

    assert run(body) == ScopeObservation(OPENROUTER, None, None, "authenticated", None, None)
