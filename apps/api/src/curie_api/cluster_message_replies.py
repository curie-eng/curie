"""Atomic, bounded Valkey handoff for disconnected cluster-message replies."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, cast

import redis.asyncio as redis
from curie_internal.keyspace import cluster_reply_keys


class ReplyBucketFullError(RuntimeError):
    """The bucket already contains its configured maximum event count."""


class ReplyBucketBytesExceededError(RuntimeError):
    """Appending an event would exceed the bucket's aggregate byte bound."""


@dataclass(frozen=True)
class ReplyPage:
    events: list[dict[str, Any]]
    next_cursor: int
    terminal: bool


# All mutation, including retry dedupe and the three bounds, happens in one
# script.  Checking dedupe first is load-bearing: retrying the event that filled
# a bucket must still acknowledge successfully rather than report overflow.
_APPEND_SCRIPT = """
if redis.call('SISMEMBER', KEYS[2], ARGV[2]) == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[6])
  redis.call('EXPIRE', KEYS[2], ARGV[6])
  redis.call('EXPIRE', KEYS[3], ARGV[6])
  redis.call('EXPIRE', KEYS[4], ARGV[6])
  return 0
end

local count = redis.call('LLEN', KEYS[1])
if count >= tonumber(ARGV[4]) then
  return -1
end

local current_bytes = tonumber(redis.call('GET', KEYS[3]) or '0')
local next_bytes = current_bytes + tonumber(ARGV[3])
if next_bytes > tonumber(ARGV[5]) then
  return -2
end

redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('SADD', KEYS[2], ARGV[2])
redis.call('SET', KEYS[3], next_bytes)
if ARGV[7] == '1' then
  redis.call('SET', KEYS[4], '1')
elseif redis.call('EXISTS', KEYS[4]) == 0 then
  redis.call('SET', KEYS[4], '0')
end
redis.call('EXPIRE', KEYS[1], ARGV[6])
redis.call('EXPIRE', KEYS[2], ARGV[6])
redis.call('EXPIRE', KEYS[3], ARGV[6])
redis.call('EXPIRE', KEYS[4], ARGV[6])
return 1
"""


# Read the cursor, current length, and terminal flag from one Valkey snapshot.
# The reply list is short by construction, so LRANGE from the caller's cursor is
# bounded by max_events and requires no separate enumeration/index structure.
_READ_SCRIPT = """
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {}
end
local count = redis.call('LLEN', KEYS[1])
local after = tonumber(ARGV[1])
if after > count then
  after = count
end
local stored = redis.call('LRANGE', KEYS[1], after, -1)
local result = {tostring(count), redis.call('GET', KEYS[2]) or '0'}
for _, event in ipairs(stored) do
  table.insert(result, event)
end
return result
"""


class ClusterMessageReplyStore:
    """Store isolated per-ref reply buckets with no scan/list operation."""

    def __init__(
        self,
        client: redis.Redis,
        *,
        ttl_s: int,
        max_events: int,
        max_bytes: int,
    ) -> None:
        self._client = client
        self._ttl_s = ttl_s
        self._max_events = max_events
        self._max_bytes = max_bytes

    @staticmethod
    def _keys(reply_ref: str) -> tuple[str, str, str, str]:
        # Curly braces keep one bucket in one Redis Cluster hash slot while the
        # fixed UUID-only suffix prevents key injection.
        return cluster_reply_keys(reply_ref)

    async def append(
        self,
        reply_ref: str,
        event: dict[str, Any],
        *,
        terminal: bool,
    ) -> None:
        encoded = json.dumps(event, sort_keys=True, separators=(",", ":"))
        event_bytes = len(encoded.encode("utf-8"))
        # Fast refusal for a single oversize event; the Lua script repeats the
        # aggregate decision atomically for races with other writers.
        if event_bytes > self._max_bytes:
            raise ReplyBucketBytesExceededError
        digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        keys = self._keys(reply_ref)
        result = await self._client.eval(
            _APPEND_SCRIPT,
            len(keys),
            *keys,
            encoded,
            digest,
            str(event_bytes),
            str(self._max_events),
            str(self._max_bytes),
            str(self._ttl_s),
            "1" if terminal else "0",
        )
        code = int(result)
        if code == -1:
            raise ReplyBucketFullError
        if code == -2:
            raise ReplyBucketBytesExceededError

    async def read(self, reply_ref: str, *, after: int) -> ReplyPage | None:
        events_key, _digests_key, _bytes_key, terminal_key = self._keys(reply_ref)
        raw = await self._client.eval(
            _READ_SCRIPT,
            2,
            events_key,
            terminal_key,
            str(after),
        )
        if not raw:
            return None
        values = cast(list[bytes | str], raw)
        next_cursor = int(values[0])
        terminal_raw = values[1]
        terminal = terminal_raw == b"1" or terminal_raw == "1"
        events: list[dict[str, Any]] = []
        for value in values[2:]:
            decoded = json.loads(value)
            if not isinstance(decoded, dict):  # defensive against store corruption
                raise ValueError("stored cluster-message reply is not an object")
            events.append(cast(dict[str, Any], decoded))
        return ReplyPage(events=events, next_cursor=next_cursor, terminal=terminal)
