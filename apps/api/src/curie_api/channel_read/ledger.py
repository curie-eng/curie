"""The API's view of the shared channel read ledger (ADR 0100).

A thin wrapper over ``curie_internal.channel_read_ledger``: the keys and the
scripts are imported, never copied, so the worker's revoke and the API's
reads always address the same state. Every Valkey failure becomes
``LedgerUnavailable``, which the route answers with ``channel_read.unavailable``.
"""

from __future__ import annotations

import uuid
from typing import Literal, cast

from curie_internal.channel_read_ledger import (
    LEASE_TTL_S,
    LEDGER_TTL_S,
    MAX_PAGES_PER_TURN,
    OPEN_LUA,
    RELEASE_LUA,
    RESERVE_LUA,
    STEER_LUA,
    ledger_keys,
    owner_index_key,
    revoke_owner,
    tombstone_key,
    turn_key,
)
from redis.asyncio import Redis
from redis.exceptions import RedisError

__all__ = ["LEDGER_TTL_S", "MAX_PAGES_PER_TURN", "ChannelReadLedger", "LedgerUnavailable"]

Reservation = Literal["reserved", "exhausted", "inactive", "expired"]
_RESERVATIONS: frozenset[str] = frozenset({"reserved", "exhausted", "inactive", "expired"})


class LedgerUnavailable(RuntimeError):
    """Valkey could not answer; the read or mint is refused, never guessed."""


def _text(value: object) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


class ChannelReadLedger:
    def __init__(self, client: Redis, prefix: str) -> None:
        self._client = client
        self._prefix = prefix

    def _keys(self, agent: uuid.UUID, turn: str) -> tuple[str, str, str, str]:
        return ledger_keys(self._prefix, agent, turn_key(turn))

    async def open(
        self, agent: uuid.UUID, turn: str, owner: str, ttl_s: int, *, resume: bool
    ) -> int | Literal["expired"]:
        """Start ``owner``'s generation; a resume whose ledger expired gets nothing."""

        try:
            gen = await self._client.eval(
                OPEN_LUA,
                6,
                *self._keys(agent, turn),
                owner_index_key(self._prefix, agent, owner),
                tombstone_key(self._prefix, agent, owner),
                owner,
                min(ttl_s, LEASE_TTL_S),
                LEDGER_TTL_S,
                "1" if resume else "0",
                turn_key(turn),
                ttl_s,
            )
        except RedisError:
            raise LedgerUnavailable from None
        value = int(gen)
        return "expired" if value < 0 else value

    async def steer(self, agent: uuid.UUID, turn: str, ttl_s: int) -> tuple[int, int] | None:
        """Bump the live opener's generation; ``(gen, remaining ms)`` or None when ended."""

        gen_key, active, _, _ = self._keys(agent, turn)
        try:
            bumped = await self._client.eval(STEER_LUA, 2, gen_key, active)
        except RedisError:
            raise LedgerUnavailable from None
        if not bumped:
            return None
        gen, remaining_ms = (int(part) for part in bumped)
        return gen, min(remaining_ms, ttl_s * 1000)

    async def is_current(self, agent: uuid.UUID, turn: str, gen: int) -> bool:
        _, active, _, _ = self._keys(agent, turn)
        try:
            value = await self._client.get(active)
        except RedisError:
            raise LedgerUnavailable from None
        if value is None:
            return False
        _, _, current = _text(value).rpartition(":")
        return current == str(gen)

    async def reserve(self, agent: uuid.UUID, turn: str, gen: int) -> Reservation:
        """Take one page atomically, rechecking the generation in the same script."""

        _, active, pages, _ = self._keys(agent, turn)
        try:
            outcome = _text(
                await self._client.eval(RESERVE_LUA, 2, active, pages, gen, MAX_PAGES_PER_TURN)
            )
        except RedisError:
            raise LedgerUnavailable from None
        if outcome not in _RESERVATIONS:
            raise LedgerUnavailable
        return cast(Reservation, outcome)

    async def release(self, agent: uuid.UUID, turn: str) -> None:
        """Give back a page whose provider call failed; never below zero."""

        _, _, pages, _ = self._keys(agent, turn)
        try:
            await self._client.eval(RELEASE_LUA, 1, pages)
        except RedisError:
            raise LedgerUnavailable from None

    async def revoke(self, agent: uuid.UUID, turn: str, owner: str) -> bool:
        try:
            return await revoke_owner(self._client, self._prefix, agent, turn_key(turn), owner)
        except RedisError:
            raise LedgerUnavailable from None
