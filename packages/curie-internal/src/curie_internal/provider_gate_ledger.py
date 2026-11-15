"""The provider gate ledger the worker writes and the API reads (#5426).

All keys live under the caller's worker key prefix. Per scope, one hash tag
``{<provider>:<scope_key>}`` keeps two keys in one cluster slot:

``provider-scope``        a hash: ``provider``, ``current_at_ms``,
                          ``probed_at_ms``, ``outcome``, ``limit_remaining``
                          (decimal string, empty when unknown) and
                          ``allowance_increased_at_ms``. Lives ``SCOPE_TTL_S``,
                          refreshed on every write.
``provider-probe-lease``  the owner token of the one worker allowed to probe
                          the scope, alive ``PROBE_LEASE_MS``.

Per provider, ``provider-current`` holds the scope key a worker currently
runs with, alive ``CURRENT_TTL_S``. A lost key only delays a release, so every
reader fails closed. Nothing here logs.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Final

from redis.asyncio import Redis

from curie_internal.provider_scope import SUPPORTED_PROVIDERS, AllowanceReading, ProviderScope

SCOPE_TTL_S: Final[int] = 259200
CURRENT_TTL_S: Final[int] = 120
PROBE_LEASE_MS: Final[int] = 30000


def scope_hash_key(prefix: str, provider: str, scope: str) -> str:
    """The observation hash of one provider scope."""

    return f"{prefix}:provider-scope:{{{provider}:{scope}}}"


def lease_key(prefix: str, provider: str, scope: str) -> str:
    """The probe lease of one provider scope, in the same slot as its hash."""

    return f"{prefix}:provider-probe-lease:{{{provider}:{scope}}}"


def current_key(prefix: str, provider: str) -> str:
    """The scope key a worker currently runs with for ``provider``."""

    return f"{prefix}:provider-current:{provider}"


# KEYS: lease, scope hash. ARGV: owner, provider, now ms, outcome,
# limit_remaining (decimal string, empty for unknown), scope ttl s. Refuses (0)
# unless the caller holds the lease. Only an authenticated reading touches the
# allowance fields; a strictly greater numeric value over a numeric baseline
# marks an increase at now.
RECORD_LUA: Final[str] = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
redis.call('HSET', KEYS[2], 'provider', ARGV[2], 'probed_at_ms', ARGV[3], 'outcome', ARGV[4])
if ARGV[4] == 'authenticated' then
    local stored = tonumber(redis.call('HGET', KEYS[2], 'limit_remaining') or '')
    local fresh = tonumber(ARGV[5])
    if stored and fresh and fresh > stored then
        redis.call('HSET', KEYS[2], 'allowance_increased_at_ms', ARGV[3])
    end
    redis.call('HSET', KEYS[2], 'limit_remaining', ARGV[5])
end
redis.call('DEL', KEYS[1])
redis.call('EXPIRE', KEYS[2], ARGV[6])
return 1
"""


@dataclass(frozen=True)
class ScopeObservation:
    """What the ledger holds for one provider scope."""

    provider: str
    current_at_ms: int | None
    probed_at_ms: int | None
    outcome: str | None
    limit_remaining: Decimal | None
    allowance_increased_at_ms: int | None


def _text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def _int(value: object) -> int | None:
    text = _text(value)
    if text is None:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _decimal(value: object) -> Decimal | None:
    text = _text(value)
    if not text:
        return None
    try:
        number = Decimal(text)
    except InvalidOperation:
        return None
    return number if number.is_finite() else None


async def register_current(redis: Redis, prefix: str, scope: ProviderScope, now_ms: int) -> None:
    """Record ``scope`` as the provider's current scope, seen at ``now_ms``."""

    hash_key = scope_hash_key(prefix, scope.provider, scope.scope_key)
    async with redis.pipeline(transaction=False) as pipe:
        pipe.hset(hash_key, mapping={"provider": scope.provider, "current_at_ms": str(now_ms)})
        pipe.expire(hash_key, SCOPE_TTL_S)
        pipe.set(current_key(prefix, scope.provider), scope.scope_key, ex=CURRENT_TTL_S)
        await pipe.execute()


async def claim_probe(redis: Redis, prefix: str, provider: str, scope_key: str, owner: str) -> bool:
    """Take the scope's probe lease for ``owner``; False when another holds it."""

    claimed = await redis.set(
        lease_key(prefix, provider, scope_key), owner, nx=True, px=PROBE_LEASE_MS
    )
    return bool(claimed)


async def record_probe(
    redis: Redis,
    prefix: str,
    provider: str,
    scope_key: str,
    owner: str,
    now_ms: int,
    reading: AllowanceReading,
) -> bool:
    """Store a probe reading and release the lease; False when ``owner`` lost it."""

    remaining = "" if reading.limit_remaining is None else format(reading.limit_remaining, "f")
    recorded = await redis.eval(
        RECORD_LUA,
        2,
        lease_key(prefix, provider, scope_key),
        scope_hash_key(prefix, provider, scope_key),
        owner,
        provider,
        str(now_ms),
        reading.outcome,
        remaining,
        str(SCOPE_TTL_S),
    )
    return int(recorded) == 1


async def read_scope(
    redis: Redis, prefix: str, provider: str, scope_key: str
) -> ScopeObservation | None:
    """The scope's observation, None when the ledger holds none."""

    raw = await redis.hgetall(scope_hash_key(prefix, provider, scope_key))
    if not raw:
        return None
    fields = {_text(key): value for key, value in raw.items()}
    return ScopeObservation(
        provider=_text(fields.get("provider")) or provider,
        current_at_ms=_int(fields.get("current_at_ms")),
        probed_at_ms=_int(fields.get("probed_at_ms")),
        outcome=_text(fields.get("outcome")) or None,
        limit_remaining=_decimal(fields.get("limit_remaining")),
        allowance_increased_at_ms=_int(fields.get("allowance_increased_at_ms")),
    )


async def read_current(redis: Redis, prefix: str, provider: str) -> str | None:
    """The provider's current scope key, None when no worker registered one lately."""

    return _text(await redis.get(current_key(prefix, provider))) or None


async def find_provider(redis: Redis, prefix: str, scope_key: str) -> str | None:
    """The one supported provider holding a hash for ``scope_key``, else None."""

    found = [
        provider
        for provider in sorted(SUPPORTED_PROVIDERS)
        if await redis.exists(scope_hash_key(prefix, provider, scope_key))
    ]
    return found[0] if len(found) == 1 else None
