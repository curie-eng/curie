"""Trim settled entries off the consumed streams (ADR 0184, #1523).

Nothing else removes an acknowledged entry from ``curie:runs`` or
``curie:evals``, and Valkey runs ``noeviction``, so without this pass every
settled turn stays in memory until the store refuses writes.

The floor is computed and applied in one script (``_TRIM_SCRIPT``), so no
delivery or acknowledgement can land between reading the groups and trimming.
Only the entries every group has already acknowledged, and that are older than
the lag window, fall below it.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from typing import Any

from redis.asyncio import Redis

from .config import WorkerConfig

logger = logging.getLogger(__name__)

# KEYS[1] stream; ARGV[1] lag window in milliseconds. Returns {trimmed, floor}.
#
# A group's floor is its oldest pending id, or the id after its
# last-delivered-id when nothing is pending: everything below it was delivered
# to that group and acknowledged. The stream's floor is the minimum over every
# group, and never later than now minus the window. No group means nothing is
# proven settled, so nothing is trimmed.
#
# Ids are compared as (ms, seq) numbers. A sequence at or past 2^53 cannot be
# incremented exactly as a Lua number, so that group keeps its last delivered
# entry instead; keeping one settled entry is the safe side.
_TRIM_SCRIPT = """
local stream = KEYS[1]
if redis.call('EXISTS', stream) == 0 then return {0, ''} end
local groups = redis.call('XINFO', 'GROUPS', stream)
if #groups == 0 then return {0, ''} end
local function parse(id)
  local dash = string.find(id, '-', 1, true)
  return tonumber(string.sub(id, 1, dash - 1)), tonumber(string.sub(id, dash + 1))
end
local floor_ms, floor_seq
local function consider(ms, seq)
  if floor_ms == nil or ms < floor_ms or (ms == floor_ms and seq < floor_seq) then
    floor_ms, floor_seq = ms, seq
  end
end
for _, group in ipairs(groups) do
  local name, last
  for i = 1, #group, 2 do
    if group[i] == 'name' then name = group[i + 1] end
    if group[i] == 'last-delivered-id' then last = group[i + 1] end
  end
  local summary = redis.call('XPENDING', stream, name)
  if tonumber(summary[1]) > 0 then
    consider(parse(summary[2]))
  else
    local ms, seq = parse(last)
    if seq < 9007199254740992 then seq = seq + 1 end
    consider(ms, seq)
  end
end
local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
consider(now_ms - tonumber(ARGV[1]), 0)
local floor = string.format('%.0f-%.0f', floor_ms, floor_seq)
return {redis.call('XTRIM', stream, 'MINID', floor), floor}
"""


async def trim_settled(redis: Redis, stream: str, *, min_age_s: float) -> int:
    """Trim ``stream`` below its settled floor; return how many entries went.

    A missing stream, or one with no consumer group, is left alone and
    returns 0.
    """

    result: Any = await redis.eval(
        _TRIM_SCRIPT, 1, stream, int(min_age_s * 1000)
    )
    return int(result[0])


class StreamRetention:
    """The supervised worker loop that runs ``trim_settled`` on each stream."""

    def __init__(
        self,
        redis: Redis,
        streams: Sequence[str],
        *,
        min_age_s: float,
        interval_s: float,
    ) -> None:
        self._redis = redis
        self.streams = tuple(streams)
        self.min_age_s = min_age_s
        self._interval_s = interval_s

    async def trim_once(self) -> dict[str, int]:
        """One pass over every stream; a failing stream never stops the rest."""

        trimmed: dict[str, int] = {}
        for stream in self.streams:
            try:
                trimmed[stream] = await trim_settled(
                    self._redis, stream, min_age_s=self.min_age_s
                )
            except Exception as exc:  # noqa: BLE001 - existing broad catch retained
                logger.warning(
                    "stream retention pass failed on %s (%s: %s); retrying next tick",
                    stream,
                    type(exc).__name__,
                    exc,
                )
                continue
            if trimmed[stream]:
                logger.debug("trimmed %d settled entries from %s", trimmed[stream], stream)
        return trimmed

    async def run_forever(self, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            await self.trim_once()
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=self._interval_s)
            except TimeoutError:
                pass


def build_stream_retention(config: WorkerConfig, redis: Redis) -> StreamRetention:
    """The pass over every stream this worker consumes through a group."""

    return StreamRetention(
        redis,
        (config.stream, config.eval_stream),
        min_age_s=config.stream_retention_min_age_s,
        interval_s=config.stream_retention_interval_s,
    )
