"""Durable capacity waits for interactive turns."""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from curie_telemetry import TRACEPARENT_STREAM_FIELD, inject_trace_context
from redis.asyncio import Redis

from .config import WorkerConfig
from .delivery_lease import DeliveryLease

logger = logging.getLogger(__name__)

WAIT_GENERATION_FIELD = "curie_capacity_wait_generation"
WAIT_REPLY_REF_FIELD = "curie_capacity_wait_reply_ref"
WAIT_TERMINAL_ONLY_FIELD = "curie_capacity_wait_terminal_only"

_NOTICE_RETRY_MS = 30_000
_NOTICE_LOCK_MS = 60_000
_NOTICE_SEND_TIMEOUT_S = 30.0
_BATCH = 64


class CapacityWaitRequested(Exception):
    """The kernel could not start an interactive turn because capacity is absent."""


class CapacityWaitRefused(RuntimeError):
    """The delivery or wait generation no longer owns this transition."""


class CapacityWaitExpired(Exception):
    """The fixed wait deadline passed before a sandbox could start."""


@dataclass(frozen=True)
class CapacityWaitRecord:
    event_id: str
    fields: dict[str, str]
    state: str
    generation: int
    first_wait_ms: int
    deadline_ms: int
    due_ms: int
    deferrals: int
    cause: str
    notice_pending: bool
    expiry_notice_pending: bool
    reply_ref: str | None
    grant_epoch: str | None
    grant_confirmed: bool
    grant_unknown: bool


_CURRENT_WAIT: ContextVar[tuple[CapacityWaitStore, str, int, DeliveryLease] | None] = ContextVar(
    "curie_current_capacity_wait", default=None
)


@contextmanager
def wait_delivery_scope(
    store: CapacityWaitStore, event_id: str, generation: int, lease: DeliveryLease
) -> Iterator[None]:
    token = _CURRENT_WAIT.set((store, event_id, generation, lease))
    try:
        yield
    finally:
        _CURRENT_WAIT.reset(token)


def current_wait() -> tuple[CapacityWaitStore, str, int, DeliveryLease] | None:
    return _CURRENT_WAIT.get()


# Every guard precedes the first write. In particular, the exact PEL owner and
# the delivery token are checked inside the script that acknowledges the entry.
_PARK_LUA = """
local record = KEYS[1]
local due = KEYS[2]
local waiting = KEYS[3]
local active = KEYS[4]
local notices = KEYS[5]
local done = KEYS[6]
local completion = KEYS[7]
local lease = KEYS[8]
local delivery = KEYS[9]
local flight = KEYS[10]
local stream = ARGV[1]
local group = ARGV[2]
local entry = ARGV[3]
local consumer = ARGV[4]
local owner = ARGV[5]
local lease_gen = ARGV[6]
local expected_wait_gen = ARGV[7]
local payload = ARGV[8]
local budget_ms = tonumber(ARGV[9])
local retention_ms = tonumber(ARGV[10])

local pending = redis.call('XPENDING', stream, group, 'IDLE', 0, entry, entry, 1)
if #pending == 0 then return {0, 'not_pending'} end
if pending[1][2] ~= consumer then return {0, 'not_owner'} end
if redis.call('GET', lease) ~= owner then return {0, 'lease_lost'} end
if redis.call('HGET', delivery, 'gen') ~= lease_gen then return {0, 'lease_generation'} end
if redis.call('EXISTS', done) == 1 or redis.call('HGET', completion, 'done') == '1' then
  return {0, 'terminal'}
end
local state = redis.call('HGET', record, 'state')
local generation = 1
if state then
  if state ~= 'woken' or redis.call('HGET', record, 'generation') ~= expected_wait_gen then
    return {0, 'wait_generation'}
  end
  generation = tonumber(expected_wait_gen) + 1
elseif expected_wait_gen ~= '' then
  return {0, 'missing_wait'}
end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local first = tonumber(redis.call('HGET', record, 'first_wait_ms')) or now
local deadline = tonumber(redis.call('HGET', record, 'deadline_ms')) or (now + budget_ms)
local deferrals = tonumber(redis.call('HGET', record, 'deferrals')) or 0
deferrals = deferrals + 1
local delay = math.min(120000, 10000 * math.pow(2, math.min(deferrals - 1, 7)))
local next_due = math.min(deadline, now + delay)
local retained_until = deadline + retention_ms
if not state then redis.call('HSET', record, 'fields', payload) end
redis.call('HSET', record,
  'state', 'waiting', 'generation', generation, 'first_wait_ms', first,
  'deadline_ms', deadline, 'due_ms', next_due, 'deferrals', deferrals,
  'cause', '', 'notice_pending', '1')
redis.call('PEXPIREAT', record, retained_until)
redis.call('ZADD', due, next_due, ARGV[11])
redis.call('ZADD', waiting, retained_until, ARGV[11])
redis.call('ZADD', notices, now, ARGV[11])
redis.call('ZREM', active, ARGV[11])
redis.call('ZREM', flight, ARGV[11])
redis.call('XACK', stream, group, entry)
return {1, generation, first, deadline, next_due, deferrals}
"""


_WAKE_LUA = """
local record = KEYS[1]
local due = KEYS[2]
local waiting = KEYS[3]
local notices = KEYS[4]
local notice_lock = KEYS[5]
local done = KEYS[6]
local completion = KEYS[7]
local flight = KEYS[8]
local stream = ARGV[1]
local event_id = ARGV[2]
local generation_field = ARGV[3]
local reply_ref_field = ARGV[4]
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
if redis.call('HGET', record, 'state') ~= 'waiting' then
  redis.call('ZREM', due, event_id)
  redis.call('ZREM', waiting, event_id)
  redis.call('ZREM', notices, event_id)
  return {0, 'not_waiting'}
end
if redis.call('EXISTS', done) == 1 or
   redis.call('HGET', completion, 'done') == '1' then
  redis.call('HSET', record, 'state', 'done')
  redis.call('ZREM', due, event_id)
  redis.call('ZREM', waiting, event_id)
  redis.call('ZREM', notices, event_id)
  return {0, 'terminal'}
end
local score = redis.call('ZSCORE', due, event_id)
if not score or tonumber(score) > now then return {0, 'not_due'} end
if redis.call('EXISTS', notice_lock) == 1 then return {0, 'notice_inflight'} end
local original = redis.call('HGET', record, 'fields')
if not original then return {0, 'missing_fields'} end
local fields = cjson.decode(original)
if ARGV[5] ~= '' and ARGV[6] ~= '' then fields[ARGV[5]] = ARGV[6] end
local generation = tonumber(redis.call('HGET', record, 'generation')) + 1
local deadline = redis.call('HGET', record, 'deadline_ms')
fields[generation_field] = tostring(generation)
local reply_ref = redis.call('HGET', record, 'reply_ref')
if reply_ref and reply_ref ~= '' then fields[reply_ref_field] = reply_ref end
local args = {stream, '*'}
for field, value in pairs(fields) do
  table.insert(args, field)
  table.insert(args, value)
end
local wake_entry = redis.call('XADD', unpack(args))
redis.call('HSET', record, 'state', 'woken', 'generation', generation, 'wake_entry', wake_entry)
redis.call('HDEL', record, 'terminal_only')
redis.call('ZREM', due, event_id)
redis.call('ZREM', waiting, event_id)
redis.call('ZREM', notices, event_id)
redis.call('ZADD', flight, tonumber(deadline), event_id)
return {1, generation, wake_entry}
"""


_RECONCILE_LUA = """
local record = KEYS[1]
local flight = KEYS[2]
local done = KEYS[3]
local completion = KEYS[4]
local active = KEYS[5]
local event_id = ARGV[1]
local stream = ARGV[2]
local group = ARGV[3]
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local state = redis.call('HGET', record, 'state')
if state ~= 'woken' and state ~= 'active' then
  redis.call('ZREM', flight, event_id)
  return {0, 'not_flying'}
end
local deadline = tonumber(redis.call('HGET', record, 'deadline_ms'))
if not deadline or now < deadline then return {0, 'not_due'} end
if redis.call('EXISTS', done) == 1 or
   redis.call('HGET', completion, 'done') == '1' then
  return {2, redis.call('HGET', record, 'generation')}
end
local entry = redis.call('HGET', record, 'wake_entry')
if not entry then return {0, 'missing_entry'} end
local pending = redis.call('XPENDING', stream, group, 'IDLE', 0, entry, entry, 1)
if #pending > 0 then
  redis.call('ZADD', flight, now + 5000, event_id)
  return {0, 'pending'}
end
if redis.call('HGET', record, 'terminal_only') == '1' then
  local entries = redis.call('XRANGE', stream, entry, entry, 'COUNT', 1)
  if #entries > 0 then
    local last_delivered = nil
    for _, info in ipairs(redis.call('XINFO', 'GROUPS', stream)) do
      local name = nil
      local candidate = nil
      for i = 1, #info, 2 do
        if info[i] == 'name' then name = info[i + 1] end
        if info[i] == 'last-delivered-id' then candidate = info[i + 1] end
      end
      if name == group then last_delivered = candidate; break end
    end
    if not last_delivered then return {0, 'group_missing'} end
    local entry_ms, entry_seq = string.match(entry, '^(%d+)%-(%d+)$')
    local delivered_ms, delivered_seq = string.match(last_delivered, '^(%d+)%-(%d+)$')
    if not entry_ms or not delivered_ms then return {0, 'invalid_stream_id'} end
    if tonumber(delivered_ms) < tonumber(entry_ms) or
       (delivered_ms == entry_ms and tonumber(delivered_seq) < tonumber(entry_seq)) then
      redis.call('ZADD', flight, now + 5000, event_id)
      return {0, 'unread_terminal'}
    end
  end
end
local original = redis.call('HGET', record, 'fields')
if not original then return {0, 'missing_fields'} end
local fields = cjson.decode(original)
if ARGV[7] ~= '' and ARGV[8] ~= '' then fields[ARGV[7]] = ARGV[8] end
local generation = tonumber(redis.call('HGET', record, 'generation')) + 1
local cause = redis.call('HGET', record, 'cause')
if cause ~= 'delivery_exhausted' and cause ~= 'capacity_wait_expired' then
  if redis.call('HGET', record, 'ever_granted') == '1' then
    cause = 'delivery_exhausted'
  elseif redis.call('HGET', record, 'grant_unknown') == '1' then
    cause = 'grant_unknown'
  else
    cause = 'capacity_wait_expired'
  end
end
fields[ARGV[4]] = tostring(generation)
fields[ARGV[5]] = '1'
local reply_ref = redis.call('HGET', record, 'reply_ref')
if reply_ref and reply_ref ~= '' then fields[ARGV[6]] = reply_ref end
local args = {stream, '*'}
for field, value in pairs(fields) do
  table.insert(args, field)
  table.insert(args, value)
end
local terminal_entry = redis.call('XADD', unpack(args))
redis.call('HSET', record, 'state', 'woken', 'generation', generation,
  'cause', cause, 'wake_entry', terminal_entry, 'terminal_only', '1')
redis.call('ZADD', flight, now + 5000, event_id)
redis.call('ZREM', active, event_id)
return {1, generation, cause, terminal_entry}
"""


_NOTICE_CLAIM_LUA = """
if redis.call('HGET', KEYS[1], 'state') ~= 'waiting' then return 0 end
if redis.call('HGET', KEYS[1], 'generation') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'notice_pending') ~= '1' then return 0 end
if redis.call('EXISTS', KEYS[3]) == 1 or
   redis.call('HGET', KEYS[4], 'done') == '1' then return 0 end
if redis.call('SET', KEYS[2], ARGV[2], 'NX', 'PX', ARGV[3]) == false then return 0 end
return 1
"""


_NOTICE_FINISH_LUA = """
if redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
if redis.call('HGET', KEYS[1], 'state') == 'waiting' and
   redis.call('HGET', KEYS[1], 'generation') == ARGV[1] then
  if ARGV[3] == '1' then
    redis.call('HSET', KEYS[1], 'notice_pending', '0')
    if ARGV[4] ~= '' then redis.call('HSET', KEYS[1], 'reply_ref', ARGV[4]) end
    redis.call('ZREM', KEYS[3], ARGV[5])
  else
    local t = redis.call('TIME')
    local now = t[1] * 1000 + math.floor(t[2] / 1000)
    redis.call('ZADD', KEYS[3], now + tonumber(ARGV[6]), ARGV[5])
  end
end
redis.call('DEL', KEYS[2])
return 1
"""


_EXPIRY_NOTICE_CLAIM_LUA = """
if redis.call('HGET', KEYS[1], 'state') ~= 'expired' then return 0 end
if redis.call('HGET', KEYS[1], 'generation') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'expiry_notice_pending') ~= '1' then return 0 end
if redis.call('EXISTS', KEYS[3]) == 0 and
   redis.call('HGET', KEYS[4], 'done') ~= '1' then return 0 end
if redis.call('SET', KEYS[2], ARGV[2], 'NX', 'PX', ARGV[3]) == false then return 0 end
return 1
"""


_EXPIRY_NOTICE_FINISH_LUA = """
if ARGV[2] ~= '' and redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
if redis.call('HGET', KEYS[1], 'state') == 'expired' and
   redis.call('HGET', KEYS[1], 'generation') == ARGV[1] then
  if ARGV[3] == '1' then
    redis.call('HSET', KEYS[1], 'expiry_notice_pending', '0')
    if ARGV[4] ~= '' then redis.call('HSET', KEYS[1], 'reply_ref', ARGV[4]) end
    redis.call('ZREM', KEYS[3], ARGV[5])
  else
    local t = redis.call('TIME')
    local now = t[1] * 1000 + math.floor(t[2] / 1000)
    redis.call('ZADD', KEYS[3], now + tonumber(ARGV[6]), ARGV[5])
  end
end
if ARGV[2] ~= '' then redis.call('DEL', KEYS[2]) end
return 1
"""


_STATE_LUA = """
local state = redis.call('HGET', KEYS[1], 'state')
if state ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'generation') ~= ARGV[2] then return 0 end
if redis.call('EXISTS', KEYS[5]) == 0 and
   redis.call('HGET', KEYS[6], 'done') ~= '1' then return 0 end
if state == ARGV[3] then return 2 end
redis.call('HSET', KEYS[1], 'state', ARGV[3], 'cause', ARGV[6])
redis.call('ZREM', KEYS[2], ARGV[4])
redis.call('ZREM', KEYS[3], ARGV[4])
redis.call('ZREM', KEYS[8], ARGV[4])
if ARGV[3] == 'expired' then redis.call('ZADD', KEYS[4], ARGV[5], ARGV[4]) end
if ARGV[3] == 'expired' then
  redis.call('HSET', KEYS[1], 'expiry_notice_pending', '1')
  local t = redis.call('TIME')
  local now = t[1] * 1000 + math.floor(t[2] / 1000)
  redis.call('ZADD', KEYS[7], now, ARGV[4])
end
return 1
"""


_BEGIN_GRANT_LUA = """
local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'woken' and state ~= 'active' then return 0 end
if redis.call('HGET', KEYS[1], 'generation') ~= ARGV[1] then return 0 end
if redis.call('EXISTS', KEYS[3]) == 1 or
   redis.call('HGET', KEYS[4], 'done') == '1' then return 0 end
local pending = redis.call('XPENDING', ARGV[4], ARGV[5], 'IDLE', 0, ARGV[6], ARGV[6], 1)
if #pending == 0 or pending[1][2] ~= ARGV[7] then return 0 end
if redis.call('GET', KEYS[5]) ~= ARGV[8] then return 0 end
if redis.call('HGET', KEYS[6], 'gen') ~= ARGV[9] then return 0 end
if state == 'active' then
  if redis.call('HGET', KEYS[1], 'grant_confirmed') ~= '1' then return 4 end
else
  local t = redis.call('TIME')
  local now = t[1] * 1000 + math.floor(t[2] / 1000)
  local deadline = tonumber(redis.call('HGET', KEYS[1], 'deadline_ms'))
  if not deadline or now >= deadline then return 3 end
  redis.call('HSET', KEYS[1], 'state', 'active')
  redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
end
redis.call('HSET', KEYS[1], 'grant_epoch', ARGV[10],
  'grant_confirmed', '0', 'grant_unknown', '0')
return 1
"""


_CONFIRM_GRANT_LUA = """
local state = redis.call('HGET', KEYS[1], 'state')
if (state ~= 'active' and not
    (state == 'woken' and redis.call('HGET', KEYS[1], 'terminal_only') == '1')) or
   redis.call('HGET', KEYS[1], 'generation') ~= ARGV[1] or
   redis.call('HGET', KEYS[1], 'grant_epoch') ~= ARGV[2] then return 0 end
if redis.call('EXISTS', KEYS[2]) == 1 or
   redis.call('HGET', KEYS[3], 'done') == '1' then return 0 end
local pending = redis.call('XPENDING', ARGV[3], ARGV[4], 'IDLE', 0, ARGV[5], ARGV[5], 1)
if #pending == 0 or pending[1][2] ~= ARGV[6] then return 0 end
if redis.call('GET', KEYS[4]) ~= ARGV[7] then return 0 end
if redis.call('HGET', KEYS[5], 'gen') ~= ARGV[8] then return 0 end
redis.call('HSET', KEYS[1], 'grant_confirmed', '1',
  'ever_granted', '1', 'grant_unknown', '0')
if state == 'woken' then redis.call('HSET', KEYS[1], 'cause', 'delivery_exhausted') end
return 1
"""


_GRANT_UNKNOWN_LUA = """
local state = redis.call('HGET', KEYS[1], 'state')
if (state ~= 'active' and not
    (state == 'woken' and redis.call('HGET', KEYS[1], 'terminal_only') == '1')) or
   redis.call('HGET', KEYS[1], 'generation') ~= ARGV[1] or
   redis.call('HGET', KEYS[1], 'grant_epoch') ~= ARGV[2] or
   redis.call('HGET', KEYS[1], 'grant_confirmed') == '1' then return 0 end
redis.call('HSET', KEYS[1], 'grant_unknown', '1')
return 1
"""


_FLAG_EXPIRY_LUA = """
local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'woken' and not
   (state == 'active' and redis.call('HGET', KEYS[1], 'grant_confirmed') ~= '1') then
  return 0
end
if redis.call('HGET', KEYS[1], 'generation') ~= ARGV[1] then return 0 end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local deadline = tonumber(redis.call('HGET', KEYS[1], 'deadline_ms'))
if not deadline or now < deadline then return 0 end
local cause = 'capacity_wait_expired'
if redis.call('HGET', KEYS[1], 'ever_granted') == '1' then
  cause = 'delivery_exhausted'
elseif redis.call('HGET', KEYS[1], 'grant_unknown') == '1' then
  cause = 'grant_unknown'
end
redis.call('HSET', KEYS[1], 'cause', cause)
return 1
"""


_REQUEST_BUDGET_LUA = """
if redis.call('HGET', KEYS[1], 'generation') ~= ARGV[1] then return {0, 0} end
if redis.call('EXISTS', KEYS[2]) == 1 or
   redis.call('HGET', KEYS[3], 'done') == '1' then return {0, 0} end
local state = redis.call('HGET', KEYS[1], 'state')
if state == 'active' then
  if redis.call('HGET', KEYS[1], 'grant_confirmed') == '1' then return {2, 0} end
  local t = redis.call('TIME')
  local now = t[1] * 1000 + math.floor(t[2] / 1000)
  local deadline = tonumber(redis.call('HGET', KEYS[1], 'deadline_ms'))
  if not deadline or now >= deadline then return {3, 0} end
  return {4, 0}
end
if state ~= 'woken' then return {0, 0} end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local deadline = tonumber(redis.call('HGET', KEYS[1], 'deadline_ms'))
if not deadline or now >= deadline then return {3, 0} end
return {1, deadline - now}
"""


class CapacityWaitStore:
    def __init__(self, redis: Redis, config: WorkerConfig) -> None:
        self._redis = redis
        self._config = config
        prefix = f"{config.key_prefix}:capacity_wait"
        self._prefix = prefix
        self._due = f"{prefix}:due"
        self._waiting = f"{prefix}:waiting"
        self._active = f"{prefix}:active"
        self._expired = f"{prefix}:expired"
        self._notices = f"{prefix}:notices"
        self._expiry_notices = f"{prefix}:expiry_notices"
        self._flight = f"{prefix}:flight"
        self._retention_ms = int(config.completion_max_retention_s * 1000)

    def _record(self, event_id: str) -> str:
        return f"{self._prefix}:record:{event_id}"

    def _notice_lock(self, event_id: str) -> str:
        return f"{self._prefix}:notice_lock:{event_id}"

    @staticmethod
    def _notice_lock_ttl_ms() -> int:
        # The queued send must end before this lock can expire. Otherwise a
        # wake can finish the turn while the older queued edit is still sending.
        if _NOTICE_LOCK_MS < 2 * _NOTICE_SEND_TIMEOUT_S * 1000:
            raise ValueError("capacity notice lock must outlast its send bound")
        return _NOTICE_LOCK_MS

    async def get(self, event_id: str) -> CapacityWaitRecord | None:
        stored = await self._redis.hgetall(self._record(event_id))
        if not stored:
            return None
        raw = {
            key.decode() if isinstance(key, bytes) else str(key): value
            for key, value in stored.items()
        }
        def val(name: str, default: str = "") -> str:
            value = raw.get(name, default)
            return value.decode() if isinstance(value, bytes) else str(value)
        return CapacityWaitRecord(
            event_id=event_id,
            fields=json.loads(val("fields", "{}")),
            state=val("state"),
            generation=int(val("generation", "0")),
            first_wait_ms=int(val("first_wait_ms", "0")),
            deadline_ms=int(val("deadline_ms", "0")),
            due_ms=int(val("due_ms", "0")),
            deferrals=int(val("deferrals", "0")),
            cause=val("cause"),
            notice_pending=val("notice_pending") == "1",
            expiry_notice_pending=val("expiry_notice_pending") == "1",
            reply_ref=val("reply_ref") or None,
            grant_epoch=val("grant_epoch") or None,
            grant_confirmed=val("grant_confirmed") == "1",
            grant_unknown=val("grant_unknown") == "1",
        )

    async def park(
        self, entry_id: str, fields: dict[str, str], event_id: str, lease: DeliveryLease
    ) -> CapacityWaitRecord:
        if not lease.owner or not lease.stream or not lease.group:
            raise CapacityWaitRefused("a real delivery lease is required")
        lease.raise_if_lost()
        raw = await self._redis.eval(
            _PARK_LUA,
            10,
            self._record(event_id), self._due, self._waiting, self._active,
            self._notices, self._config.done_key(event_id),
            self._config.completion_key(event_id),
            self._config.delivery_lease_key(lease.stream, lease.group, entry_id),
            self._config.delivery_state_key(lease.stream, lease.group, entry_id),
            self._flight,
            lease.stream, lease.group, entry_id, self._config.consumer_name,
            lease.owner, str(lease.generation), fields.get(WAIT_GENERATION_FIELD, ""),
            json.dumps(fields, separators=(",", ":")),
            str(int(self._config.capacity_wait_budget_s * 1000)),
            str(self._retention_ms), event_id,
        )
        if int(raw[0]) != 1:
            raise CapacityWaitRefused(str(raw[1]))
        logger.info("capacity wait parked event_id=%s cause=capacity", event_id)
        record = await self.get(event_id)
        assert record is not None
        return record

    async def wake_due(self, limit: int = _BATCH) -> int:
        due = await self._redis.zrangebyscore(
            self._due, "-inf", "+inf", start=0, num=min(limit, _BATCH)
        )
        count = 0
        for item in due:
            event_id = item.decode() if isinstance(item, bytes) else str(item)
            carrier: dict[str, str] = {}
            inject_trace_context(carrier)
            raw = await self._redis.eval(
                _WAKE_LUA, 8, self._record(event_id), self._due, self._waiting,
                self._notices, self._notice_lock(event_id),
                self._config.done_key(event_id), self._config.completion_key(event_id),
                self._flight,
                self._config.stream, event_id, WAIT_GENERATION_FIELD,
                WAIT_REPLY_REF_FIELD,
                TRACEPARENT_STREAM_FIELD if TRACEPARENT_STREAM_FIELD in carrier else "",
                carrier.get(TRACEPARENT_STREAM_FIELD, ""),
            )
            if int(raw[0]) == 1:
                count += 1
                logger.info("capacity wait woke event_id=%s cause=due", event_id)
        return count

    async def reconcile_lost_wakes(self, limit: int = _BATCH) -> int:
        t = await self._redis.time()
        now = int(t[0]) * 1000 + int(t[1]) // 1000
        ids = await self._redis.zrangebyscore(
            self._flight, "-inf", now, start=0, num=min(limit, _BATCH)
        )
        appended = 0
        for item in ids:
            event_id = item.decode() if isinstance(item, bytes) else str(item)
            carrier: dict[str, str] = {}
            inject_trace_context(carrier)
            raw = await self._redis.eval(
                _RECONCILE_LUA, 5,
                self._record(event_id), self._flight,
                self._config.done_key(event_id),
                self._config.completion_key(event_id), self._active,
                event_id, self._config.stream, self._config.consumer_group,
                WAIT_GENERATION_FIELD, WAIT_TERMINAL_ONLY_FIELD,
                WAIT_REPLY_REF_FIELD,
                TRACEPARENT_STREAM_FIELD if TRACEPARENT_STREAM_FIELD in carrier else "",
                carrier.get(TRACEPARENT_STREAM_FIELD, ""),
            )
            if int(raw[0]) == 1:
                appended += 1
                logger.warning(
                    "capacity wait terminal delivery queued event_id=%s cause=%s",
                    event_id,
                    raw[2],
                )
            elif int(raw[0]) == 2:
                await self.mark_terminal(event_id, int(raw[1]))
        return appended

    async def claim_notice(self, event_id: str, generation: int) -> str | None:
        token = uuid.uuid4().hex
        claimed = await self._redis.eval(
            _NOTICE_CLAIM_LUA, 4, self._record(event_id), self._notice_lock(event_id),
            self._config.done_key(event_id), self._config.completion_key(event_id),
            str(generation), token, str(self._notice_lock_ttl_ms()),
        )
        return token if int(claimed) == 1 else None

    async def finish_notice(
        self, event_id: str, generation: int, token: str, *,
        delivered: bool, reply_ref: str | None = None,
    ) -> None:
        await self._redis.eval(
            _NOTICE_FINISH_LUA, 3, self._record(event_id),
            self._notice_lock(event_id), self._notices,
            str(generation), token, "1" if delivered else "0", reply_ref or "",
            event_id, str(_NOTICE_RETRY_MS),
        )

    async def notices_due(self, limit: int = _BATCH) -> list[CapacityWaitRecord]:
        t = await self._redis.time()
        now = int(t[0]) * 1000 + int(t[1]) // 1000
        ids = await self._redis.zrangebyscore(
            self._notices, "-inf", now, start=0, num=min(limit, _BATCH)
        )
        result = []
        for item in ids:
            event_id = item.decode() if isinstance(item, bytes) else str(item)
            record = await self.get(event_id)
            if record is not None and record.state == "waiting" and record.notice_pending:
                result.append(record)
            else:
                await self._redis.zrem(self._notices, event_id)
        return result

    async def expiry_notices_due(self, limit: int = _BATCH) -> list[CapacityWaitRecord]:
        t = await self._redis.time()
        now = int(t[0]) * 1000 + int(t[1]) // 1000
        ids = await self._redis.zrangebyscore(
            self._expiry_notices, "-inf", now, start=0, num=min(limit, _BATCH)
        )
        result = []
        for item in ids:
            event_id = item.decode() if isinstance(item, bytes) else str(item)
            record = await self.get(event_id)
            if (
                record is not None
                and record.state == "expired"
                and record.expiry_notice_pending
            ):
                result.append(record)
            else:
                await self._redis.zrem(self._expiry_notices, event_id)
        return result

    async def claim_expiry_notice(self, event_id: str, generation: int) -> str | None:
        token = uuid.uuid4().hex
        claimed = await self._redis.eval(
            _EXPIRY_NOTICE_CLAIM_LUA, 4,
            self._record(event_id), self._notice_lock(event_id),
            self._config.done_key(event_id), self._config.completion_key(event_id),
            str(generation), token, str(self._notice_lock_ttl_ms()),
        )
        return token if int(claimed) == 1 else None

    async def finish_expiry_notice(
        self, event_id: str, generation: int, token: str | None, *,
        delivered: bool, reply_ref: str | None = None,
    ) -> None:
        await self._redis.eval(
            _EXPIRY_NOTICE_FINISH_LUA, 3,
            self._record(event_id), self._notice_lock(event_id), self._expiry_notices,
            str(generation), token or "", "1" if delivered else "0",
            reply_ref or "", event_id, str(_NOTICE_RETRY_MS),
        )

    async def check_delivery(self, event_id: str, generation: int) -> str:
        record = await self.get(event_id)
        if (
            record is None
            or record.generation != generation
            or record.state not in {"woken", "active"}
        ):
            return "stale"
        if record.state == "active":
            if record.grant_confirmed:
                return "ready"
            t = await self._redis.time()
            now = int(t[0]) * 1000 + int(t[1]) // 1000
            return "expired" if now >= record.deadline_ms else "pending"
        t = await self._redis.time()
        now = int(t[0]) * 1000 + int(t[1]) // 1000
        return "expired" if now >= record.deadline_ms else "ready"

    async def remaining_before_request(self, event_id: str, generation: int) -> float | None:
        raw = await self._redis.eval(
            _REQUEST_BUDGET_LUA, 3,
            self._record(event_id), self._config.done_key(event_id),
            self._config.completion_key(event_id), str(generation),
        )
        status = int(raw[0])
        if status == 1:
            return int(raw[1]) / 1000
        if status == 2:
            return None
        if status == 3:
            raise CapacityWaitExpired()
        if status == 4:
            raise CapacityWaitRefused("runner grant has not been confirmed")
        raise CapacityWaitRefused("wait generation changed before runner request")

    async def mark_active(
        self, event_id: str, generation: int, lease: DeliveryLease, epoch: str
    ) -> str:
        if not lease.owner or not lease.stream or not lease.group:
            raise CapacityWaitRefused("a real delivery lease is required")
        lease.raise_if_lost()
        record = await self.get(event_id)
        if record is None:
            return "stale"
        retained_until = record.deadline_ms + self._retention_ms
        raw = await self._redis.eval(
            _BEGIN_GRANT_LUA, 6, self._record(event_id), self._active,
            self._config.done_key(event_id), self._config.completion_key(event_id),
            self._config.delivery_lease_key(lease.stream, lease.group, lease.entry_id),
            self._config.delivery_state_key(lease.stream, lease.group, lease.entry_id),
            str(generation), event_id, str(retained_until), lease.stream, lease.group,
            lease.entry_id, self._config.consumer_name, lease.owner,
            str(lease.generation), epoch,
        )
        if int(raw) == 1:
            logger.info("capacity wait grant pending event_id=%s", event_id)
            return "active"
        if int(raw) == 3:
            return "expired"
        return "stale"

    async def confirm_grant(
        self, event_id: str, generation: int, lease: DeliveryLease, epoch: str
    ) -> bool:
        lease.raise_if_lost()
        raw = await self._redis.eval(
            _CONFIRM_GRANT_LUA, 5,
            self._record(event_id), self._config.done_key(event_id),
            self._config.completion_key(event_id),
            self._config.delivery_lease_key(lease.stream, lease.group, lease.entry_id),
            self._config.delivery_state_key(lease.stream, lease.group, lease.entry_id),
            str(generation), epoch, lease.stream, lease.group, lease.entry_id,
            self._config.consumer_name, lease.owner, str(lease.generation),
        )
        if int(raw) == 1:
            logger.info("capacity wait active event_id=%s cause=started", event_id)
            return True
        return False

    async def mark_grant_unknown(
        self, event_id: str, generation: int, epoch: str
    ) -> bool:
        raw = await self._redis.eval(
            _GRANT_UNKNOWN_LUA, 1, self._record(event_id), str(generation), epoch
        )
        return int(raw) == 1

    async def flag_expiry(self, event_id: str, generation: int) -> bool:
        raw = await self._redis.eval(
            _FLAG_EXPIRY_LUA, 1, self._record(event_id), str(generation)
        )
        return int(raw) == 1

    async def mark_terminal(self, event_id: str, generation: int) -> bool:
        record = await self.get(event_id)
        if record is None:
            return False
        expired = record.cause in {
            "capacity_wait_expired", "delivery_exhausted", "grant_unknown"
        }
        raw = await self._redis.eval(
            _STATE_LUA, 8, self._record(event_id), self._active,
            self._waiting, self._expired, self._config.done_key(event_id),
            self._config.completion_key(event_id), self._expiry_notices, self._flight,
            record.state, str(generation), "expired" if expired else "done",
            event_id, str(record.deadline_ms + self._retention_ms),
            record.cause if expired else "",
        )
        if int(raw) == 2:
            return True
        if int(raw) == 1:
            cause = record.cause if expired else "completed"
            logger.info(
                "capacity wait terminal event_id=%s cause=%s",
                event_id, cause,
            )
            return True
        return False

    async def snapshot(self) -> dict[str, int]:
        t = await self._redis.time()
        now = int(t[0]) * 1000 + int(t[1]) // 1000
        async with self._redis.pipeline(transaction=False) as pipe:
            pipe.zcount(self._waiting, now + 1, "+inf")
            pipe.zcount(self._active, now + 1, "+inf")
            pipe.zcount(self._expired, now + 1, "+inf")
            counts = await pipe.execute()
        return dict(zip(("waiting", "active", "expired"), map(int, counts), strict=True))

    async def prune(self, limit: int = _BATCH) -> None:
        t = await self._redis.time()
        now = int(t[0]) * 1000 + int(t[1]) // 1000
        for index in (self._waiting, self._active, self._expired):
            ids = await self._redis.zrangebyscore(
                index, "-inf", now, start=0, num=min(limit, _BATCH)
            )
            if ids:
                await self._redis.zrem(index, *ids)
