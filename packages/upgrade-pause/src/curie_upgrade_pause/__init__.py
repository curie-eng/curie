"""Shared upgrade pause identity and atomic, extend-only renewal."""

from typing import Literal

from redis.asyncio import Redis

PAUSE_LEASE_S = 300


def legacy_key(prefix: str) -> str:
    """The release-wide marker used by standalone workers and the bridge."""

    return f"{prefix}:upgrade:quiesce"


def authoritative_key(prefix: str, installation_id: str) -> str:
    """The single pause authority shared by an installation's replicas."""

    key = legacy_key(prefix)
    return f"{key}:{installation_id}" if installation_id else key


def marker_keys(prefix: str, installation_id: str, bridge: bool) -> tuple[str, ...]:
    """Keep installation authority first, followed by an optional legacy key."""

    authoritative = authoritative_key(prefix, installation_id)
    if installation_id and bridge:
        return (authoritative, legacy_key(prefix))
    return (authoritative,)


# Only KEYS[1] grants renewal authority. A legacy bridge may contain the old
# unversioned marker, so its bytes are deliberately retained without parsing.
# PTTL -1 is an unbounded lifetime and -2 is absence; neither may be changed.
_RENEW_PAUSE_LUA = """
local read_ok, raw = pcall(redis.call, 'GET', KEYS[1])
if not read_ok then return -1 end
if not raw then return 0 end
if not string.match(raw, '^%s*{') then return -1 end
local parse_ok, marker = pcall(cjson.decode, raw)
if not parse_ok or type(marker) ~= 'table' then return -1 end
local revision = marker['revision']
if type(revision) ~= 'number' or math.abs(revision) == math.huge
    or revision ~= math.floor(revision)
    or revision ~= tonumber(ARGV[1]) then
  return -1
end

local requested_ttl = tonumber(ARGV[2])
for _, key in ipairs(KEYS) do
  local current_ttl = redis.call('PTTL', key)
  if current_ttl >= 0 and current_ttl < requested_ttl then
    redis.call('PEXPIRE', key, requested_ttl)
  end
end
return 1
"""


async def renew_pause(
    redis: Redis, keys: tuple[str, ...], revision: int, ttl_ms: int
) -> Literal["renewed", "absent", "foreign"]:
    """Extend existing markers only when their authority belongs to revision."""

    if not keys:
        raise ValueError("pause renewal requires an authoritative key")
    result = await redis.eval(_RENEW_PAUSE_LUA, len(keys), *keys, str(revision), ttl_ms)
    if result == 1:
        return "renewed"
    if result == 0:
        return "absent"
    return "foreign"
