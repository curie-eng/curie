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
import secrets
import time
from dataclasses import dataclass
from typing import Any

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
_ENQUEUE_SCRIPT = """
local function append()
  if ARGV[5] ~= '' then
    return redis.call('XADD', KEYS[2], '*', ARGV[3], ARGV[2], ARGV[5], ARGV[6])
  end
  return redis.call('XADD', KEYS[2], '*', ARGV[3], ARGV[2])
end
local cur = redis.call('GET', KEYS[1])
if cur == ARGV[1] then
  local id = append()
  redis.call('SET', KEYS[1], id)
  return {1, id}
elseif not cur then
  redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[4])
  local id = append()
  redis.call('SET', KEYS[1], id)
  return {1, id}
else
  return {0, cur}
end
"""

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


async def claim_delivery(client: redis.Redis, key: str, owner: str, lease_s: int) -> bool:
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
) -> tuple[bool, str]:
    """Enqueue the payload if this request still owns the claim.

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

    result: Any = await client.eval(
        _ENQUEUE_SCRIPT,
        2,
        key,
        stream,
        owner,
        payload,
        payload_field,
        str(lease_s),
        transport_field or "",
        transport_value or "",
    )
    enqueued, current = result
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
