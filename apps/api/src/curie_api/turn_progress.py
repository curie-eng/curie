"""The deliberate progress ingress: the chain inbox and the per-token limit (ADR 0130).

The runner's ``progress`` tool posts one ``ProgressCommand`` per call, with the
worker-issued ``generation`` and runner ``seq``, to
``POST /v1/turn-progress/{progress_id}``
(``routers/turn_progress.py``). This module is what that route does once the
token is verified: refuse a token past its rate, and append the command to the
chain's inbox stream in Valkey, where the worker's per-turn pump reads it. The
API never touches the progress record itself; ordering, idempotency and the
milestone budget are the worker store's (``curie_worker.progress``).

The scope, the route, the inbox key and its entry fields are frozen with the
worker and the runner in ``tests/vectors/turn-progress-capability.json``: the
three ship in different images; the API and worker use shared key builders.
"""

from __future__ import annotations

from typing import Annotated, Any, Final

from channel_protocol.progress import ProgressCommand
from pydantic import Field
from redis.asyncio import Redis

TURN_PROGRESS_SCOPE: Final = "turn.progress"
TURN_PROGRESS_PATH: Final = "/v1/turn-progress/{progress_id}"
TOKEN_REQUEST_HEADER: Final = "X-API-Key"

# A chain accepts at most 50 updates (the worker store's cap), so an inbox
# holding more than twice that is a runaway, not a backlog. Exact trimming,
# because the stream is small and a pump must be able to count on the bound.
INBOX_MAXLEN: Final = 128
# A chain lives across the approval it suspends for; the store keeps its record
# for the approval card's lifetime, and an entry nobody applied by then has no
# record left to apply to.
INBOX_TTL_S: Final = 14 * 24 * 60 * 60

# One update a second sustained, a burst of five, per token. A token is minted
# per turn, so this bounds one turn, and the model is told to report only
# material transitions.
RATE_PER_S: Final = 1.0
RATE_BURST: Final = 5
_RATE_TTL_MS: Final = 60_000

# generation and seq travel through the worker store's Lua, whose numbers are doubles.
_MAX_POSITION: Final = 2**53 - 1

Position = Annotated[int, Field(strict=True, ge=1, le=_MAX_POSITION)]


class TurnProgressBody(ProgressCommand):
    """A ``ProgressCommand`` and the worker/runner position for it.

    Closed like the command: an unknown field is refused. ``generation`` is
    allocated durably by the worker and ``seq`` orders commands within it.
    """

    generation: Position
    seq: Position


def inbox_fields(body: TurnProgressBody) -> dict[str, str]:
    """One inbox entry: the command as JSON, and its position."""

    command = ProgressCommand.model_validate(body.model_dump(exclude={"generation", "seq"}))
    return {
        "command": command.model_dump_json(exclude_none=True),
        "generation": str(body.generation),
        "seq": str(body.seq),
    }


# Fence, append, index and renew expiry in one step. A 202 can therefore never
# be orphaned from the maintenance drainer by an API crash between writes.
_APPEND_LUA = """
if redis.call('HGET', KEYS[3], 'active_generation') ~= ARGV[4] then return false end
local active_until_ms = tonumber(redis.call('HGET', KEYS[3], 'active_until_ms'))
if active_until_ms == nil then return false end
local now_parts = redis.call('TIME')
local now_ms = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)
if now_ms >= active_until_ms then return false end
local id = redis.call('XADD', KEYS[1], 'MAXLEN', ARGV[1], '*',
  'command', ARGV[3], 'generation', ARGV[4], 'seq', ARGV[5])
redis.call('SADD', KEYS[2], ARGV[6])
redis.call('EXPIRE', KEYS[1], ARGV[2])
redis.call('EXPIRE', KEYS[2], ARGV[2])
return id
"""

# A token bucket on the server's clock, so API replicas with skewed clocks
# share one limit. tokens refill continuously at ARGV[1] per millisecond up to
# ARGV[2]; a request takes one whole token or is refused.
_TAKE_LUA = """
local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local stored = redis.call('HMGET', KEYS[1], 'tokens', 'at')
local tokens = tonumber(stored[1])
local at = tonumber(stored[2])
if tokens == nil or at == nil then
  tokens = burst
  at = now
end
if now > at then
  tokens = math.min(burst, tokens + (now - at) * rate)
  at = now
end
local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'at', tostring(at))
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return allowed
"""


async def take_rate_token(valkey: Redis, key: str) -> bool:
    """Spend one of the token's updates; False when it is past its rate."""

    allowed: Any = await valkey.eval(
        _TAKE_LUA,
        1,
        key,
        str(RATE_PER_S / 1000.0),
        str(RATE_BURST),
        str(_RATE_TTL_MS),
    )
    return int(allowed) == 1


async def append_to_inbox(
    valkey: Redis,
    *,
    key: str,
    pending_key: str,
    record_key: str,
    progress_id: str,
    fields: dict[str, str],
) -> str | None:
    """Append and index one active-generation entry; None when fenced."""

    entry_id: Any = await valkey.eval(
        _APPEND_LUA,
        3,
        key,
        pending_key,
        record_key,
        str(INBOX_MAXLEN),
        str(INBOX_TTL_S),
        fields["command"],
        fields["generation"],
        fields["seq"],
        progress_id,
    )
    if entry_id is None:
        return None
    return entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)
