"""Source authority CAS, @spec PROTECTED-HOOK-SOURCE-1/6/7.

Decimal strings preserve BIGINT precision through Lua and cjson. The caller
supplies the role-scoped client; this module acquires no credentials.
"""

from __future__ import annotations

import json
import re
from typing import Any, TypedDict
from uuid import UUID

from redis import Redis

_MAX_GENERATION = 2**63 - 1
# @spec PROTECTED-HOOK-SOURCE-1: same decoded name grammar as hook ingress.
_HOOK_NAME = re.compile(r"^[a-z0-9][a-z0-9._-]{0,62}$")
_FINGERPRINT = re.compile(r"[0-9a-f]{64}")
_STORED_GENERATION = re.compile(r"[1-9][0-9]*")

# @spec PROTECTED-HOOK-SOURCE-1/6
_VALIDATE_SOURCE = """
-- @spec PROTECTED-HOOK-SOURCE-1/6
local function valid_generation(value)
    -- @spec PROTECTED-HOOK-SOURCE-1
    if type(value) ~= 'string' or not string.match(value, '^[1-9][0-9]*$') then
        return false
    end
    local maximum = '9223372036854775807'
    return #value < #maximum or (#value == #maximum and value <= maximum)
end
local function valid_uuid(value)
    -- @spec PROTECTED-HOOK-SOURCE-1/6
    if type(value) ~= 'string' or #value ~= 36 then return false end
    for _, index in ipairs({9, 14, 19, 24}) do
        if string.sub(value, index, index) ~= '-' then return false end
    end
    local hex, separators = string.gsub(value, '-', '')
    return separators == 4 and #hex == 32 and string.match(hex, '^[0-9a-f]+$') ~= nil
end
local function decode_source(raw)
    -- @spec PROTECTED-HOOK-SOURCE-1/6
    local ok, source = pcall(cjson.decode, raw)
    if not ok or type(source) ~= 'table' or not valid_generation(source.floor)
       or not valid_uuid(source.operation_id) then return nil end
    if source.active == cjson.null then return source end
    local active = source.active
    if type(active) ~= 'table' or not valid_generation(active.generation)
       or active.generation ~= source.floor or active.operation_id ~= source.operation_id
       or (active.mode ~= 'ordinary' and active.mode ~= 'protected')
       or type(active.policy_fingerprint) ~= 'string' or #active.policy_fingerprint ~= 64
       or not string.match(active.policy_fingerprint, '^[0-9a-f]+$') then return nil end
    return source
end
"""

# @spec PROTECTED-HOOK-SOURCE-1/6/7
_RESERVE = (
    _VALIDATE_SOURCE
    + """
-- @spec PROTECTED-HOOK-SOURCE-1/6/7
local function greater(a, b)
    -- @spec PROTECTED-HOOK-SOURCE-1
    if #a ~= #b then return #a > #b end
    return a > b
end
local function increment(value)
    -- @spec PROTECTED-HOOK-SOURCE-1
    local result = ''
    local carry = 1
    for i = #value, 1, -1 do
        local digit = string.byte(value, i) - 48 + carry
        if digit == 10 then digit = 0; carry = 1 else carry = 0 end
        result = string.char(48 + digit) .. result
    end
    if carry == 1 then result = '1' .. result end
    return result
end
local raw = redis.call('GET', KEYS[1])
local current = {floor = '0', operation_id = cjson.null, active = cjson.null}
if raw then
    current = decode_source(raw)
    if not current then return {'invalid'} end
end
if current.operation_id == ARGV[2] then return {'ok', current.floor} end
if current.floor ~= ARGV[1] then return {'conflict'} end
local floor = current.floor
if greater(ARGV[3], floor) then floor = ARGV[3] end
if floor == '9223372036854775807' then return {'exhausted'} end
local generation = increment(floor)
redis.call('SET', KEYS[1], cjson.encode({
    floor = generation, operation_id = ARGV[2], active = cjson.null
}))
return {'ok', generation}
"""
)

# @spec PROTECTED-HOOK-SOURCE-3/6/7
_PUBLISH_ORDINARY = (
    _VALIDATE_SOURCE
    + """
-- @spec PROTECTED-HOOK-SOURCE-3/6/7
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local current = decode_source(raw)
if not current then return -1 end
if current.floor ~= ARGV[1] or current.operation_id ~= ARGV[2] then return 0 end
if current.active ~= cjson.null and current.active ~= nil then
    local active = current.active
    if active.generation == ARGV[1] and active.operation_id == ARGV[2]
       and active.mode == 'ordinary' and active.policy_fingerprint == ARGV[3] then
        return 1
    end
    return 0
end
current.active = {
    generation = ARGV[1], operation_id = ARGV[2], mode = 'ordinary',
    policy_fingerprint = ARGV[3]
}
redis.call('SET', KEYS[1], cjson.encode(current))
return 1
"""
)

# @spec PROTECTED-HOOK-SOURCE-6: the protected sibling. It sets the active
# protected record only when floor and operation equal the committed row and no
# other active record exists, and it is idempotent for the same record.
_PUBLISH_PROTECTED = (
    _VALIDATE_SOURCE
    + """
-- @spec PROTECTED-HOOK-SOURCE-6
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local current = decode_source(raw)
if not current then return -1 end
if current.floor ~= ARGV[1] or current.operation_id ~= ARGV[2] then return 0 end
if current.active ~= cjson.null and current.active ~= nil then
    local active = current.active
    if active.generation == ARGV[1] and active.operation_id == ARGV[2]
       and active.mode == 'protected' and active.policy_fingerprint == ARGV[3] then
        return 1
    end
    return 0
end
current.active = {
    generation = ARGV[1], operation_id = ARGV[2], mode = 'protected',
    policy_fingerprint = ARGV[3]
}
redis.call('SET', KEYS[1], cjson.encode(current))
return 1
"""
)


class SourceFenceConflict(Exception):
    """Reservation CAS refused, @spec PROTECTED-HOOK-SOURCE-6/7."""


class SourceFenceExhausted(Exception):
    """BIGINT generation space exhausted, @spec PROTECTED-HOOK-SOURCE-1."""


class SourceFenceInvalid(ValueError):
    """Malformed persisted authority refused, @spec PROTECTED-HOOK-SOURCE-1/6."""


class ActiveSource(TypedDict):
    """Published authority, @spec PROTECTED-HOOK-SOURCE-6."""

    generation: int
    operation_id: str
    mode: str
    policy_fingerprint: str


class SourceState(TypedDict):
    """Current source authority, @spec PROTECTED-HOOK-SOURCE-6."""

    floor: int
    operation_id: str | None
    active: ActiveSource | None


def _canonical_uuid(value: str) -> str:
    """Canonicalize source identity, @spec PROTECTED-HOOK-SOURCE-1/6."""
    if not isinstance(value, str):
        raise ValueError("source identity must be a UUID string")
    return str(UUID(value))


def _source_key(agent_id: str, hook: str) -> str:
    """Validate decoded key components, @spec PROTECTED-HOOK-SOURCE-1/6."""
    agent_id = _canonical_uuid(agent_id)
    if not isinstance(hook, str) or not _HOOK_NAME.fullmatch(hook):
        raise ValueError("invalid decoded hook name")
    return f"protected:source:{agent_id}:{hook}"


def _generation(value: int) -> str:
    """Validate exact BIGINT input before effects, @spec PROTECTED-HOOK-SOURCE-1/6."""
    if type(value) is not int or value < 0:
        raise ValueError("source generation must be a nonnegative integer")
    if value > _MAX_GENERATION:
        raise SourceFenceExhausted("source generation exceeds BIGINT")
    return str(value)


def _valid_stored_generation(value: object) -> bool:
    """Validate the persisted decimal representation, @spec PROTECTED-HOOK-SOURCE-1/6."""
    if not isinstance(value, str) or not _STORED_GENERATION.fullmatch(value):
        return False
    maximum = str(_MAX_GENERATION)
    return len(value) < len(maximum) or (len(value) == len(maximum) and value <= maximum)


def _valid_stored_uuid(value: object) -> bool:
    """Refuse noncanonical persisted operations, @spec PROTECTED-HOOK-SOURCE-1/6."""
    if not isinstance(value, str):
        return False
    try:
        return str(UUID(value)) == value
    except ValueError:
        return False


def _decode_source(raw: str | bytes) -> SourceState:
    """Validate stored authority before conversion, @spec PROTECTED-HOOK-SOURCE-1/6."""
    try:
        state = json.loads(raw)
    except ValueError as error:
        raise SourceFenceInvalid("invalid stored source authority") from error
    if (
        not isinstance(state, dict)
        or not _valid_stored_generation(state.get("floor"))
        or not _valid_stored_uuid(state.get("operation_id"))
        or "active" not in state
    ):
        raise SourceFenceInvalid("invalid stored source authority")
    active = state["active"]
    published: ActiveSource | None = None
    if active is not None:
        if (
            not isinstance(active, dict)
            or not _valid_stored_generation(active.get("generation"))
            or active["generation"] != state["floor"]
            or active.get("operation_id") != state["operation_id"]
            or active.get("mode") not in ("ordinary", "protected")
            or not isinstance(active.get("policy_fingerprint"), str)
            or not _FINGERPRINT.fullmatch(active["policy_fingerprint"])
        ):
            raise SourceFenceInvalid("invalid stored active source authority")
        published = {
            "generation": int(active["generation"]),
            "operation_id": active["operation_id"],
            "mode": active["mode"],
            "policy_fingerprint": active["policy_fingerprint"],
        }
    return {
        "floor": int(state["floor"]),
        "operation_id": state["operation_id"],
        "active": published,
    }


class SourceFence:
    """Source writer operations only, @spec PROTECTED-HOOK-SOURCE-6/7."""

    def __init__(self, client: Redis) -> None:
        """Use the supplied role-specific client, @spec PROTECTED-HOOK-SOURCE-6."""
        self._client = client

    def reserve_and_revoke(
        self,
        agent_id: str,
        hook: str,
        expected_floor: int,
        operation_id: str,
        min_generation: int,
    ) -> int:
        """Reserve the next source revision, @spec PROTECTED-HOOK-SOURCE-1/6/7."""
        key = _source_key(agent_id, hook)
        expected = _generation(expected_floor)
        operation = _canonical_uuid(operation_id)
        minimum = _generation(min_generation)
        result: Any = self._client.eval(_RESERVE, 1, key, expected, operation, minimum)
        status = result[0]
        if status in ("invalid", b"invalid"):
            raise SourceFenceInvalid("invalid stored source authority")
        if status in ("conflict", b"conflict"):
            raise SourceFenceConflict("source reservation is no longer current")
        if status in ("exhausted", b"exhausted"):
            raise SourceFenceExhausted("source generation space exhausted")
        if status not in ("ok", b"ok"):
            raise ValueError("invalid source reservation response")
        return int(result[1])

    def read(self, agent_id: str, hook: str) -> SourceState:
        """Read authority metadata, @spec PROTECTED-HOOK-SOURCE-6."""
        raw: Any = self._client.get(_source_key(agent_id, hook))
        if raw is None:
            return {"floor": 0, "operation_id": None, "active": None}
        return _decode_source(raw)

    def publish_ordinary(
        self,
        agent_id: str,
        hook: str,
        generation: int,
        operation_id: str,
        policy_fingerprint: str,
    ) -> bool:
        """Publish the exact ordinary reservation, @spec PROTECTED-HOOK-SOURCE-3/6/7."""
        key = _source_key(agent_id, hook)
        revision = _generation(generation)
        if generation == 0:
            raise ValueError("published generation must be positive")
        operation = _canonical_uuid(operation_id)
        if not isinstance(policy_fingerprint, str) or not _FINGERPRINT.fullmatch(
            policy_fingerprint
        ):
            raise ValueError("policy fingerprint must be lowercase SHA256 hex")
        result: Any = self._client.eval(
            _PUBLISH_ORDINARY, 1, key, revision, operation, policy_fingerprint
        )
        if result == -1:
            raise SourceFenceInvalid("invalid stored source authority")
        return bool(result == 1)

    def publish_protected(
        self,
        agent_id: str,
        hook: str,
        generation: int,
        operation_id: str,
        policy_fingerprint: str,
    ) -> bool:
        """Publish the exact protected reservation, @spec PROTECTED-HOOK-SOURCE-6/7.

        The caller has confirmed current runtime evidence with the control
        reader first; that check is not atomic with this CAS, which is safe
        because every delivery repeats the full evaluation atomically.
        """
        key = _source_key(agent_id, hook)
        revision = _generation(generation)
        if generation == 0:
            raise ValueError("published generation must be positive")
        operation = _canonical_uuid(operation_id)
        if not isinstance(policy_fingerprint, str) or not _FINGERPRINT.fullmatch(
            policy_fingerprint
        ):
            raise ValueError("policy fingerprint must be lowercase SHA256 hex")
        result: Any = self._client.eval(
            _PUBLISH_PROTECTED, 1, key, revision, operation, policy_fingerprint
        )
        if result == -1:
            raise SourceFenceInvalid("invalid stored source authority")
        return bool(result == 1)
