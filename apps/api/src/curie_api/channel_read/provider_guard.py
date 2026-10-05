"""Limits on calls to the provider itself, apart from the page budget (ADR 0100).

Two pieces of API owned Valkey state, beside the shared ledger but in their
own ``api`` namespace:

1. A per logical turn attempt count. Every read that reaches the provider
   spends one, whether it succeeds or not, so a turn that keeps failing (a
   missing message, a refused channel) cannot call it without bound even though
   failures give their page back.
2. A cooldown per credential and provider method, set from ``Retry-After``.
   It is shared by every agent the credential serves, so one turn's 429 stops
   all of them from calling that method until the provider's window passes.
"""

from __future__ import annotations

import math
import uuid
from typing import Final

from curie_internal.channel_read_ledger import LEDGER_TTL_S, turn_key
from redis.asyncio import Redis
from redis.exceptions import RedisError

from .ledger import LedgerUnavailable

MAX_PROVIDER_ATTEMPTS_PER_TURN: Final[int] = 24
# Applied when the provider rate limits without saying for how long.
DEFAULT_COOLDOWN_S: Final[int] = 30

# KEYS: attempts. ARGV: cap, ttl. Returns 1 when an attempt was taken.
_CHARGE_LUA: Final[str] = """
local spent = tonumber(redis.call('GET', KEYS[1]) or '0')
if spent >= tonumber(ARGV[1]) then
    return 0
end
redis.call('INCR', KEYS[1])
if spent == 0 then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return 1
"""

# KEYS: cooldown. ARGV: milliseconds. Never shortens a longer cooldown.
_COOL_LUA: Final[str] = """
if redis.call('PTTL', KEYS[1]) < tonumber(ARGV[1]) then
    redis.call('SET', KEYS[1], '1', 'PX', ARGV[1])
end
return 0
"""


class ProviderGuard:
    def __init__(self, client: Redis, prefix: str) -> None:
        self._client = client
        self._base = f"{prefix}:channel-read:api"

    def _attempts_key(self, agent: uuid.UUID, turn: str) -> str:
        return f"{self._base}:{{{agent}:{turn_key(turn)}}}:attempts"

    def _cooldown_key(self, identity_key: str, method: str) -> str:
        return f"{self._base}:cooldown:{identity_key}:{method}"

    async def charge_attempt(self, agent: uuid.UUID, turn: str) -> bool:
        try:
            taken = await self._client.eval(
                _CHARGE_LUA,
                1,
                self._attempts_key(agent, turn),
                MAX_PROVIDER_ATTEMPTS_PER_TURN,
                LEDGER_TTL_S,
            )
        except RedisError:
            raise LedgerUnavailable from None
        return int(taken) == 1

    async def cooldown_remaining(self, identity_key: str, method: str) -> int | None:
        """Whole seconds left on the method's cooldown, or None when it is open."""

        try:
            remaining_ms = await self._client.pttl(self._cooldown_key(identity_key, method))
        except RedisError:
            raise LedgerUnavailable from None
        if remaining_ms is None or remaining_ms <= 0:
            return None
        return max(1, math.ceil(remaining_ms / 1000))

    async def cool_down(self, identity_key: str, method: str, seconds: int | None) -> None:
        duration_ms = max(1, DEFAULT_COOLDOWN_S if seconds is None else seconds) * 1000
        try:
            await self._client.eval(
                _COOL_LUA, 1, self._cooldown_key(identity_key, method), duration_ms
            )
        except RedisError:
            raise LedgerUnavailable from None
