"""The at-least-once delivery machinery every inbound ingress route shares.

Extracted from ``routers/channels.py`` when ADR-0079's hook ingress became the
SECOND route needing it (issue #269). Until then it was one implementation and
belonged where it was used; the abstraction is written now because there are two
callers, not in anticipation of them.

Copying it instead would have put two versions of the same idempotency argument
in the tree, and the parts most worth not duplicating are the ones a reader
cannot check by eye: the three-armed enqueue script, the reason a receipt has no
expiry, and the reason the quota is counted only after a claim is won. Two copies
of that drift silently, and the symptom is a correspondent answered twice.

**Callers own their key names.** Every function here takes a fully-built key,
because the namespace is the caller's decision and the two routes deliberately do
not share one: a channel delivery is identified by ``(binding row, delivery id)``
and a hook delivery by ``(agent, hook, delivery id)``. Handing both a shared
prefix would let two different id spaces collide on one key and swallow each
other's turns.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import secrets
import time
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import redis.asyncio as redis
from fastapi import Response

# The API's Valkey client is built without `decode_responses`, so values come
# back as bytes; `text` is the package's named, documented decode for exactly
# that.
from curie_api.graveyardwatcher import text

logger = logging.getLogger(__name__)

# Bound cleanup waits so an unavailable Valkey cannot hold the original error.
_FAILED_DELIVERY_CLEANUP_TIMEOUT_S = 5.0

# The owner-checked enqueue, as ONE script so the ownership check and the XADD
# are a single atomic step (Valkey runs a script single-threaded, which is
# exactly the guarantee needed here). Three exhaustive outcomes:
#
#   (a) we still own the lease        -> XADD, and the claim becomes the receipt
#   (b) the lease lapsed, NO successor -> re-claim and XADD in-script
#   (c) a foreign token or a stream id -> no XADD; name the current owner
#
# (b) is not a nicety: without it a winner that resumed after its lease expired
# but BEFORE any successor claimed would find an empty key, take the lease-lost
# arm, and report a duplicate for a delivery that was NEVER enqueued -- and the
# caller retries only transport failures, so that delivery is silently dropped.
# (c)'s TOKEN comparison is what makes the lease OWNED: a merely SLOW winner
# whose lease was re-claimed must not XADD on top of the retry's entry.
#
# The claim key has TWO phases with OPPOSITE expiry contracts, and BOTH enqueue
# arms below write the second one: `pending:<token>` carries the lease TTL (what
# makes a dead winner's delivery recoverable), while the stream id that replaces
# it is a RECEIPT and is written with NO expiry at all. An expiry there lets the
# same `delivery_id` win a fresh `SET NX` once it lapses, enqueue a second time,
# and answer the correspondent twice -- silently, and only for the deliveries a
# caller happens to retry after the window.
# @spec GUARDED-DELIVERY-V1-4, GUARDED-DELIVERY-V1-7.
_DELIVERY_WINDOW_LUA = """
local function delivery_limb(raw, minimum, maximum, canonical, width)
  if type(raw) ~= 'string' or #raw == 0 or #raw > width
     or not string.match(raw, '^%-?%d+$') then return nil end
  local n = tonumber(raw)
  if not n or n < minimum or n > maximum then return nil end
  if canonical and raw ~= '0' and not string.match(raw, '^[1-9]%d*$')
     and not string.match(raw, '^%-[1-9]%d*$') then return nil end
  return n
end
local function delivery_window_bounds(ls, lu, us, uu)
  local a = delivery_limb(ls, -62135596800, 253402300799, true, 13)
  local b = delivery_limb(lu, 0, 999999, true, 6)
  local c = delivery_limb(us, -62135596800, 253402300799, true, 13)
  local d = delivery_limb(uu, 0, 999999, true, 6)
  if not a or not b or not c or not d or a > c or (a == c and b > d) then
    return nil
  end
  return {a, b, c, d}
end
local function delivery_window_contains(bounds, seconds, micros)
  local s = delivery_limb(seconds, -62135596800, 253402300799, false, 13)
  local u = delivery_limb(micros, 0, 999999, false, 6)
  if not s or not u then return -5 end
  if s < bounds[1] or (s == bounds[1] and u < bounds[2])
     or s > bounds[3] or (s == bounds[3] and u > bounds[4]) then return -3 end
  return 1
end
"""

_IDENTITY_RECEIPT_LUA = """
-- @spec HTTP-INVOCATION-EXECUTION-7, HTTP-INVOCATION-EXECUTION-14.
local function identity_receipt(cur, metadata, intent)
  if not cur then
    if metadata then return -6, '' end
    return 0, ''
  end
  if #cur == 40 and string.match(cur, '^pending:[0-9a-f]+$') then
    if metadata then return -6, '' end
    return 0, cur
  end
  if not string.match(cur, '^[0-9]+%-[0-9]+$')
     or not metadata or #metadata > 512 then return -6, '' end
  local context, saved_intent, stream = string.match(metadata,
    '^{"context_digest":"([0-9a-f]+)","intent_digest":"([0-9a-f]+)","schema_version":1,"stream_id":"([0-9]+%-[0-9]+)"}$')
  if not context or #context ~= 64 or #saved_intent ~= 64
     or saved_intent ~= intent or stream ~= cur then return -6, '' end
  return 1, cur
end
"""

_IDENTITY_RECEIPT_READ = _IDENTITY_RECEIPT_LUA + """
local status, value = identity_receipt(
  redis.call('GET', KEYS[1]), redis.call('GET', KEYS[2]), ARGV[1])
return {status, value}
"""

_ENQUEUE_SCRIPT = _DELIVERY_WINDOW_LUA + _IDENTITY_RECEIPT_LUA + """
-- @spec GUARDED-DELIVERY-V1-4, GUARDED-DELIVERY-V1-5.
local mode = ARGV[8] or ''
local identity = mode == 'identity_guard_v2'
local guarded = mode == 'identity_guard_v1' or identity
local function invalid_protocol()
  if identity then return {-7, ''} end
  return redis.error_reply('invalid guarded delivery protocol')
end
local bounds = nil
if guarded then
  local key_count = identity and 5 or 4
  local arg_count = identity and 17 or 14
  if #KEYS ~= key_count or #ARGV ~= arg_count or ARGV[7] ~= '' then
    return invalid_protocol()
  end
  for i = 1, key_count do
    if KEYS[i] == '' then return invalid_protocol() end
    for j = i + 1, key_count do
      if KEYS[i] == KEYS[j] then
        return invalid_protocol()
      end
    end
  end
  if not string.match(ARGV[1], '^pending:[0-9a-f]+$') or #ARGV[1] ~= 40
     or string.sub(ARGV[9], 1, 7) ~= 'stable:' or #ARGV[9] <= 7
     or not string.match(ARGV[10], '^[0-9a-f]+$') or #ARGV[10] ~= 32 then
    return invalid_protocol()
  end
  bounds = delivery_window_bounds(ARGV[11], ARGV[12], ARGV[13], ARGV[14])
  if not bounds then return invalid_protocol() end
  if identity then
    if #ARGV[15] > 16384 then return invalid_protocol() end
    local body, signature = string.match(ARGV[15],
      '^curie%-context%-v1%.([%w_-]+)%.([%w_-]+)$')
    if not body or #signature ~= 43
       or #ARGV[16] ~= 64 or not string.match(ARGV[16], '^[0-9a-f]+$')
       or #ARGV[17] ~= 64 or not string.match(ARGV[17], '^[0-9a-f]+$')
       or (ARGV[5] ~= '' and ARGV[5] ~= 'traceparent')
       or (ARGV[5] == '') ~= (ARGV[6] == '')
       or ARGV[3] == '' or ARGV[3] == 'curie_identity_context_v1'
       or ARGV[3] == ARGV[5] then
      return invalid_protocol()
    end
  end
elseif mode ~= '' or #KEYS >= 4 then
  return redis.error_reply('invalid guarded delivery protocol')
end
local function append()
  if identity then
    if ARGV[5] ~= '' then
      return redis.call('XADD', KEYS[2], '*', ARGV[3], ARGV[2],
        ARGV[5], ARGV[6], 'curie_identity_context_v1', ARGV[15])
    end
    return redis.call('XADD', KEYS[2], '*', ARGV[3], ARGV[2],
      'curie_identity_context_v1', ARGV[15])
  end
  if ARGV[5] ~= '' then
    return redis.call('XADD', KEYS[2], '*', ARGV[3], ARGV[2], ARGV[5], ARGV[6])
  end
  return redis.call('XADD', KEYS[2], '*', ARGV[3], ARGV[2])
end
-- @spec HOOK-RECEIPT-1, HOOK-RECEIPT-5. Optional metadata never touches legacy callers.
local with_receipt = not guarded and #KEYS == 3
if with_receipt then
  local kind = redis.call('TYPE', KEYS[1]).ok
  if kind ~= 'none' and kind ~= 'string' then return {-1, ''} end
  if kind == 'string' and redis.call('STRLEN', KEYS[1]) > 128 then
    return {-1, ''}
  end
end
local cur = redis.call('GET', KEYS[1])
if identity then
  local metadata = redis.call('GET', KEYS[5])
  if cur ~= ARGV[1] or metadata then
    local status, value = identity_receipt(cur, metadata, ARGV[16])
    if status == -6 then return {-6, ''} end
    if cur then return {0, value} end
  end
end
if cur == ARGV[1] or not cur then
  if guarded then
    if redis.call('GET', KEYS[3]) ~= ARGV[9] then return {-2, ''} end
    if redis.call('GET', KEYS[4]) ~= ARGV[10] then return {-4, ''} end
    local now = redis.call('TIME')
    if type(now) ~= 'table' or #now ~= 2 then return {-5, ''} end
    local valid = delivery_window_contains(bounds, now[1], now[2])
    if valid ~= 1 then return {valid, ''} end
  end
  if with_receipt and redis.call('EXISTS', KEYS[3]) == 1 then return {-1, ''} end
  if not cur then redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[4]) end
  local id = append()
  if identity then
    local metadata = '{"context_digest":"' .. ARGV[17]
      .. '","intent_digest":"' .. ARGV[16]
      .. '","schema_version":1,"stream_id":"' .. id .. '"}'
    redis.call('MSET', KEYS[1], id, KEYS[5], metadata)
  elseif with_receipt then
    -- All validation precedes XADD. MSET publishes both retained keys together;
    -- there is no post-append JSON parser or second independent metadata write.
    redis.call('MSET', KEYS[1], id, KEYS[3], ARGV[7] .. id .. '"}')
  else
    redis.call('SET', KEYS[1], id)
  end
  return {1, id}
else
  return {0, cur}
end
"""

# @spec HOOK-RECEIPT-4: reject orphan evidence before claim or quota mutation.
_RECEIPT_CLAIM_SCRIPT = """
local kind = redis.call('TYPE', KEYS[1]).ok
if kind ~= 'none' and kind ~= 'string' then return -1 end
if kind == 'string' and redis.call('STRLEN', KEYS[1]) > 128 then return -1 end
local cur = redis.call('GET', KEYS[1])
if redis.call('EXISTS', KEYS[2]) == 1 then
  if not cur or string.sub(cur, 1, 8) == 'pending:' then return -1 end
end
if cur then return 0 end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
return 1
"""


class DeliveryReceiptConflict(ValueError):
    """Unverifiable retained evidence. @spec HOOK-RECEIPT-3, HOOK-RECEIPT-4, HOOK-RECEIPT-5."""


# Record the attempt token with the increment so cleanup can verify a charge
# even when the request never observed the result. Both keys expire together.
_QUOTA_SCRIPT = """
local ttl = redis.call('PTTL', KEYS[1])
if ttl == -2 then
  ttl = tonumber(ARGV[1]) * 1000
end
if ttl == -1 then
  redis.call('SET', KEYS[2], ARGV[2])
else
  redis.call('SET', KEYS[2], ARGV[2], 'PX', math.max(ttl, 1))
end
local n = redis.pcall('INCR', KEYS[1])
if type(n) == 'table' and n.err then
  redis.call('DEL', KEYS[2])
  return n
end
if n == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
ttl = redis.call('PTTL', KEYS[1])
if ttl == -1 then
  redis.call('PERSIST', KEYS[2])
else
  redis.call('PEXPIRE', KEYS[2], ttl)
end
return n
"""

# Claim inspection and quota refund must be one atomic operation. An enqueue
# may have committed even when its result was lost, and its receipt preserves
# both the stream entry and the charge. Foreign pending owners are untouched.
# A successor's receipt also preserves this attempt's charge.
_SETTLE_SCRIPT = """
local cur = redis.call('GET', KEYS[1])
if ARGV[3] == '1' then
  if cur == ARGV[1] then
    redis.call('DEL', KEYS[1])
  end
  if redis.call('GET', KEYS[3]) == ARGV[2] then
    redis.call('DEL', KEYS[3])
  end
  return 1
end
if cur and string.sub(cur, 1, 8) ~= 'pending:' then
  return 1
end
if cur == ARGV[1] then
  redis.call('DEL', KEYS[1])
end
if redis.call('GET', KEYS[3]) == ARGV[2] then
  redis.call('DEL', KEYS[3])
  if redis.call('EXISTS', KEYS[2]) == 1 then
    redis.call('DECR', KEYS[2])
  end
end
return 1
"""


@dataclass(frozen=True)
class BacklogReservation:
    """One attempt's quota identity, fixed before any acquisition await."""

    counter_key: str
    token: str
    window_s: int

    @property
    def token_key(self) -> str:
        """The marker recording whether this attempt charged its counter."""

        return f"{self.counter_key}:attempt:{self.token}"


@dataclass(frozen=True)
class DeliveryValidityWindow:
    """Inclusive server time bounds. @spec GUARDED-DELIVERY-V1-2."""

    lower_seconds: int
    lower_microseconds: int
    upper_seconds: int
    upper_microseconds: int

    def __post_init__(self) -> None:
        """@spec GUARDED-DELIVERY-V1-2."""
        seconds = (self.lower_seconds, self.upper_seconds)
        micros = (self.lower_microseconds, self.upper_microseconds)
        if (
            any(type(value) is not int for value in (*seconds, *micros))
            or any(not -62135596800 <= value <= 253402300799 for value in seconds)
            or any(not 0 <= value <= 999999 for value in micros)
            or (self.lower_seconds, self.lower_microseconds)
            > (self.upper_seconds, self.upper_microseconds)
        ):
            raise ValueError("invalid_delivery_validity")


@dataclass(frozen=True)
class DeliveryEnqueueGuard:
    """Conditional write inputs, not permission. @spec GUARDED-DELIVERY-V1-2."""

    fence_key: str = field(repr=False)
    expected_stable: str = field(repr=False)
    validity: DeliveryValidityWindow = field(repr=False)
    reservation: BacklogReservation = field(repr=False)

    def __post_init__(self) -> None:
        """@spec GUARDED-DELIVERY-V1-2."""
        try:
            if (
                type(self.fence_key) is not str or not self.fence_key
                or type(self.expected_stable) is not str
                or not self.expected_stable.startswith("stable:")
                or type(self.validity) is not DeliveryValidityWindow
                or type(self.reservation) is not BacklogReservation
            ):
                raise ValueError
            UUID(self.expected_stable.removeprefix("stable:"))
            DeliveryValidityWindow.__post_init__(self.validity)
            reservation = self.reservation
            if (
                type(reservation.counter_key) is not str or not reservation.counter_key
                or type(reservation.token) is not str
                or re.fullmatch(r"[0-9a-f]{32}", reservation.token) is None
                or type(reservation.window_s) is not int or reservation.window_s <= 0
            ):
                raise ValueError
        except (ValueError, TypeError, AttributeError):
            raise ValueError("invalid_delivery_guard") from None


class DeliveryGuardRefusal(RuntimeError):
    """Bounded conditional append refusal. @spec GUARDED-DELIVERY-V1-6."""

    def __init__(self, code: str) -> None:
        """@spec GUARDED-DELIVERY-V1-6."""
        if type(code) is not str or code not in {
            "fence_changed", "outside_validity", "reservation_lost", "clock_unavailable",
            "intent_conflict",
        }:
            raise ValueError("invalid_delivery_guard_refusal")
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class DeliveryIdentityContext:
    """@spec HTTP-INVOCATION-EXECUTION-7 HTTP-INVOCATION-EXECUTION-14."""

    capsule: str = field(repr=False)
    intent_digest: str = field(repr=False)
    receipt_key: str = field(repr=False)

    def __post_init__(self) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-14."""
        try:
            if (
                type(self.capsule) is not str or len(self.capsule) > 16_384
                or re.fullmatch(
                    r"curie-context-v1\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{43}", self.capsule,
                ) is None
                or type(self.intent_digest) is not str
                or re.fullmatch(r"[0-9a-f]{64}", self.intent_digest) is None
                or type(self.receipt_key) is not str or not self.receipt_key
            ):
                raise ValueError
            self.capsule.encode("ascii")
            self.receipt_key.encode("utf-8")
        except (ValueError, TypeError, AttributeError):
            raise ValueError("invalid_delivery_context") from None

    @property
    def context_digest(self) -> str:
        """@spec HTTP-INVOCATION-EXECUTION-14."""
        self.__post_init__()
        return hashlib.sha256(self.capsule.encode("ascii")).hexdigest()


async def read_identity_delivery_receipt(
    client: redis.Redis, *, key: str, receipt_key: str, intent_digest: str,
) -> str | None:
    """@spec HTTP-INVOCATION-EXECUTION-14."""
    try:
        if (
            any(type(value) is not str or not value for value in (key, receipt_key))
            or key == receipt_key or type(intent_digest) is not str
            or re.fullmatch(r"[0-9a-f]{64}", intent_digest) is None
        ):
            raise ValueError
        key.encode("utf-8")
        receipt_key.encode("utf-8")
    except (ValueError, TypeError, AttributeError):
        raise ValueError("invalid_delivery_context") from None
    result = await client.eval_ro(_IDENTITY_RECEIPT_READ, 2, key, receipt_key, intent_digest)
    if type(result) not in (list, tuple) or len(result) != 2 or type(result[0]) is not int:
        raise RuntimeError("invalid_identity_receipt_result")
    if result[0] == -6:
        raise DeliveryGuardRefusal("intent_conflict")
    if not isinstance(result[1], (str, bytes, bytearray)):
        raise RuntimeError("invalid_identity_receipt_result")
    try:
        current = text(result[1])
    except UnicodeError:
        raise RuntimeError("invalid_identity_receipt_result") from None
    if result[0] == 0 and not current:
        return None
    if (
        result[0] == 0 and re.fullmatch(r"pending:[0-9a-f]{32}", current)
        or result[0] == 1 and re.fullmatch(r"[0-9]+-[0-9]+", current)
    ):
        return current
    raise RuntimeError("invalid_identity_receipt_result")


def _guarded_enqueue_result(result: Any) -> tuple[bool, str]:
    """Decode only the guarded protocol. @spec GUARDED-DELIVERY-V1-6."""
    if (
        type(result) not in (list, tuple) or len(result) != 2
        or type(result[0]) is not int
    ):
        raise RuntimeError("invalid_guarded_delivery_result")
    status, current = result
    refusals = {
        -2: "fence_changed", -3: "outside_validity",
        -4: "reservation_lost", -5: "clock_unavailable",
    }
    if status in refusals:
        raise DeliveryGuardRefusal(refusals[status])
    if status not in (0, 1) or not isinstance(current, (str, bytes, bytearray)):
        raise RuntimeError("invalid_guarded_delivery_result")
    try:
        value = text(current)
    except UnicodeError:
        raise RuntimeError("invalid_guarded_delivery_result") from None
    if not value:
        raise RuntimeError("invalid_guarded_delivery_result")
    return status == 1, value


def _guard_arguments(
    guard: DeliveryEnqueueGuard, *, key: str, stream: str, owner: str,
    receipt_key: str | None, receipt_prefix: str | None,
) -> tuple[list[str], list[str]]:
    """Validate again before the first await. @spec GUARDED-DELIVERY-V1-2."""
    try:
        if type(guard) is not DeliveryEnqueueGuard:
            raise ValueError
        DeliveryEnqueueGuard.__post_init__(guard)
        if receipt_key is not None or receipt_prefix is not None:
            raise ValueError
        if type(owner) is not str or re.fullmatch(r"pending:[0-9a-f]{32}", owner) is None:
            raise ValueError
        keys = [key, stream, guard.fence_key, guard.reservation.token_key]
        all_keys = [*keys, guard.reservation.counter_key]
        if any(type(value) is not str or not value for value in all_keys):
            raise ValueError
        for value in all_keys:
            value.encode("utf-8")
        if len(set(all_keys)) != len(all_keys):
            raise ValueError
        validity = guard.validity
        arguments = [
            "identity_guard_v1", guard.expected_stable, guard.reservation.token,
            str(validity.lower_seconds), str(validity.lower_microseconds),
            str(validity.upper_seconds), str(validity.upper_microseconds),
        ]
        return keys, arguments
    except (ValueError, TypeError, AttributeError):
        raise ValueError("invalid_delivery_guard") from None


def backlog_reservation(*, key_prefix: str, window_s: int) -> BacklogReservation:
    """Fix the window and token before taking a claim or quota slot."""

    window = int(time.time()) // window_s
    return BacklogReservation(
        counter_key=f"{key_prefix}:{window}",
        token=secrets.token_hex(16),
        window_s=window_s,
    )


def sha16(delivery_id: str) -> str:
    """The short digest both of a delivery's derived names are built from.

    Args:
        delivery_id: The upstream's id for this delivery.

    Returns:
        The first 16 hex characters of its SHA-256.
    """

    return hashlib.sha256(delivery_id.encode()).hexdigest()[:16]


async def claim_delivery(
    client: redis.Redis, key: str, owner: str, lease_s: int, *, receipt_key: str | None = None
) -> bool:
    """Claim one delivery for THIS request: ``SET NX EX``, first writer wins.

    The structural sibling of the dispatcher's ``already_enqueued`` guard, with
    two deliberate differences: the dispatcher DROPS a retry silently while
    ingress ANSWERS it (an HTTP caller needs a response), and the value here is
    an owner TOKEN that later becomes the stream id rather than a bare ``1``,
    which is what makes that answer possible.

    ``SET NX`` is the only atomic step available: a read-then-write guard (the
    kernel's ``is_done`` check, read long before ``mark_done`` is written) admits
    two winners, which is why idempotency is settled here, at ingress.

    Args:
        client: The Valkey client.
        key: This delivery's claim key, in the caller's namespace.
        owner: This request's opaque owner token.
        lease_s: How long an unfinished claim survives.

    Returns:
        True when this request took the claim.
    """

    # @spec HOOK-RECEIPT-4. Callers without metadata retain their original path.
    if receipt_key is not None:
        if receipt_key == key:
            raise ValueError("receipt and claim keys must differ")
        result = await client.eval(_RECEIPT_CLAIM_SCRIPT, 2, key, receipt_key, owner, lease_s)
        if result == -1:
            raise DeliveryReceiptConflict("invalid hook receipt evidence")
        return bool(result)
    return bool(await client.set(key, owner, nx=True, ex=lease_s))


async def enqueue_owned(
    client: redis.Redis,
    *,
    key: str,
    stream: str,
    owner: str,
    payload: str,
    payload_field: str,
    lease_s: int,
    transport_field: str | None = None,
    transport_value: str | None = None,
    receipt_key: str | None = None,
    receipt_prefix: str | None = None,
    guard: DeliveryEnqueueGuard | None = None,
    identity_context: DeliveryIdentityContext | None = None,
) -> tuple[bool, str]:
    """Enqueue the payload if this request still owns the claim.

    @spec GUARDED-DELIVERY-V1-1, GUARDED-DELIVERY-V1-4.

    Args:
        client: The Valkey client.
        key: This delivery's claim key.
        stream: The runs stream to append to.
        owner: This request's owner token.
        payload: The serialized ``QueuedTurn``.
        payload_field: The stream field the payload rides in.
        lease_s: The lease used by the re-claim arm.
        transport_field: Optional transport-owned metadata field beside payload.
        transport_value: Value paired with ``transport_field``.

    Returns:
        ``(True, stream_id)`` when THIS request enqueued; ``(False, owner_value)``
        naming the current owner when it did not.
    """

    # @spec GUARDED-DELIVERY-V1-2, GUARDED-DELIVERY-V1-4.
    guarded_arguments: list[str] = []
    if identity_context is not None:
        if (
            type(identity_context) is not DeliveryIdentityContext or guard is None
            or receipt_key is not None or receipt_prefix is not None
        ):
            raise ValueError("invalid_delivery_context")
        DeliveryIdentityContext.__post_init__(identity_context)
        if (
            transport_field not in (None, "traceparent")
            or (transport_field is None) != (transport_value is None)
            or payload_field in ("curie_identity_context_v1", transport_field, "")
        ):
            raise ValueError("invalid_delivery_context")
    if guard is not None:
        keys, guarded_arguments = _guard_arguments(
            guard, key=key, stream=stream, owner=owner,
            receipt_key=receipt_key, receipt_prefix=receipt_prefix,
        )
    else:
        keys = [key, stream] if receipt_key is None else [key, stream, receipt_key]
    if identity_context is not None:
        assert guard is not None
        if identity_context.receipt_key in (*keys, guard.reservation.counter_key):
            raise ValueError("invalid_delivery_context")
        keys.append(identity_context.receipt_key)
        guarded_arguments[0] = "identity_guard_v2"
        guarded_arguments.extend([
            identity_context.capsule, identity_context.intent_digest,
            identity_context.context_digest,
        ])
    # @spec HOOK-RECEIPT-1, HOOK-RECEIPT-5. The hook codec validates and bounds this prefix
    # before claiming. Lua only appends the server's canonical stream ID.
    if (receipt_key is None) != (receipt_prefix is None):
        raise ValueError("receipt key and prefix must be supplied together")
    if receipt_key in (key, stream):
        raise ValueError("receipt key must be separate from claim and stream")
    result: Any = await client.eval(
        _ENQUEUE_SCRIPT,
        len(keys),
        *keys,
        owner,
        payload,
        payload_field,
        str(lease_s),
        transport_field or "",
        transport_value or "",
        receipt_prefix or "",
        *guarded_arguments,
    )
    if guard is not None:
        if (
            identity_context is not None and type(result) in (list, tuple) and len(result) == 2
            and type(result[0]) is int and result[0] == -6
        ):
            raise DeliveryGuardRefusal("intent_conflict")
        return _guarded_enqueue_result(result)
    enqueued, current = result
    if enqueued == -1:
        raise DeliveryReceiptConflict("invalid hook receipt evidence")
    return bool(enqueued), text(current)


async def take_backlog_slot(
    client: redis.Redis, *, reservation: BacklogReservation, limit: int
) -> bool:
    """Count ONE new delivery against this caller's window, atomically.

    Called only once a claim is won, so a duplicate the upstream is retrying
    costs nothing -- the cap is on NEW work, which is the thing a compromised or
    runaway source can make unbounded.

    Args:
        client: The Valkey client.
        reservation: This attempt's fixed counter window and token marker.
        limit: The most new deliveries allowed per window.

    Returns:
        True when this delivery fits inside the window's allowance.
    """

    count: Any = await client.eval(
        _QUOTA_SCRIPT,
        2,
        reservation.counter_key,
        reservation.token_key,
        str(reservation.window_s),
        reservation.token,
    )
    return int(count) <= limit


async def settle_failed_delivery(
    client: redis.Redis,
    *,
    key: str,
    owner: str,
    reservation: BacklogReservation,
    preserve_quota: bool,
) -> None:
    """Release unfinished work without replacing the request's original error.

    A receipt preserves the claim and quota, including when enqueue committed
    before raising or cancellation. A pending or missing claim allows only this
    reservation's token to refund its charge. Normal quota refusal removes this
    attempt's marker and owned claim but retains the counter increment.

    Args:
        client: The Valkey client.
        key: This delivery's claim key.
        owner: This request's owner token, compared before the delete.
        reservation: The original quota window and this attempt's token.
        preserve_quota: Whether this request observed a normal quota refusal.
    """

    try:
        cleanup = asyncio.create_task(
            asyncio.wait_for(
                client.eval(
                    _SETTLE_SCRIPT,
                    3,
                    key,
                    reservation.counter_key,
                    reservation.token_key,
                    owner,
                    reservation.token,
                    "1" if preserve_quota else "0",
                ),
                timeout=_FAILED_DELIVERY_CLEANUP_TIMEOUT_S,
            )
        )
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                continue
        cleanup.result()
    except BaseException:
        logger.error("ingress delivery cleanup failed for claim %s", key, exc_info=True)


def duplicate_stream_id(current: str, response: Response) -> str | None:
    """Read a claim someone else owns, and set the status that describes it.

    A ``pending:`` value means another request is mid-flight and there is no
    stream id yet, so the answer is 202 ("come back"); anything else IS the
    stream id of the entry that request enqueued, so the answer stays 200 with
    that id. Either way the calling request issues no XADD.

    Returns the id rather than a response model because the two routes answer
    with different receipt shapes; the status code and the None-vs-id decision
    are the parts that must not diverge, and those are made here.

    Args:
        current: The claim key's current value.
        response: The FastAPI response, whose status this may set to 202.

    Returns:
        The stream id, or None while another request still holds the claim.
    """

    if current.startswith("pending:"):
        response.status_code = 202
        return None
    return current
