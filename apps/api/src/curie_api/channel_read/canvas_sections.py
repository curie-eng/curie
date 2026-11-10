"""The canvas cells a logical turn has read, which are the only ones it may edit (ADR 0200).

One Valkey set per agent, logical turn and canvas, in the API's own
``channel-read:api`` namespace and in the same cluster slot as the turn's
provider guard. It holds section ids only, never text. A read replaces the set;
an edit checks membership before any provider call. It lives as long as the
turn's read ledger, so a read before an approval suspension still counts after
the resume, and a new logical turn starts empty.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable

from curie_internal.channel_read_ledger import LEDGER_TTL_S, turn_key
from redis.asyncio import Redis
from redis.exceptions import RedisError

from .ledger import LedgerUnavailable


class CanvasSections:
    def __init__(self, client: Redis, prefix: str) -> None:
        self._client = client
        self._base = f"{prefix}:channel-read:api"

    def _key(self, agent: uuid.UUID, turn: str, canvas_id: str) -> str:
        return f"{self._base}:{{{agent}:{turn_key(turn)}}}:canvas:{canvas_id}"

    async def record(
        self, agent: uuid.UUID, turn: str, canvas_id: str, section_ids: Iterable[str]
    ) -> None:
        """Replace what this turn has read of the canvas with ``section_ids``."""

        key = self._key(agent, turn, canvas_id)
        members = sorted(set(section_ids))
        try:
            async with self._client.pipeline(transaction=True) as pipe:
                pipe.delete(key)
                if members:
                    pipe.sadd(key, *members)
                    pipe.expire(key, LEDGER_TTL_S)
                await pipe.execute()
        except RedisError:
            raise LedgerUnavailable from None

    async def was_read(self, agent: uuid.UUID, turn: str, canvas_id: str, section_id: str) -> bool:
        try:
            found = await self._client.sismember(self._key(agent, turn, canvas_id), section_id)
        except RedisError:
            raise LedgerUnavailable from None
        return bool(found)
