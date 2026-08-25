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

import hashlib
import logging
import time
from typing import Any

import redis.asyncio as redis
from fastapi import Response

# The API's Valkey client is built without `decode_responses`, so values come
# back as bytes; `_text` is the package's named, documented decode for exactly
# that.
from .graveyardwatcher import _text

logger = logging.getLogger(__name__)

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

# One caller's slot counter for the current window, taken atomically: INCR and
# the window's EXPIRE cannot be two round trips, or a process that dies between
# them leaves a counter that never expires and 429s that caller forever.
_QUOTA_SCRIPT = """
local n = redis.call('INCR', KEYS[1])
if n == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return n
"""

# Give a claim back, and ONLY our own: used when the quota refuses a delivery we
# have already claimed, so the refusal does not leave the `delivery_id` locked
# for a lease's duration. The compare is what keeps a slow caller from deleting
# a successor's claim.
_RELEASE_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('DEL', KEYS[1])
end
return 1
"""


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
    return bool(enqueued), _text(current)


async def take_backlog_slot(
    client: redis.Redis, *, key_prefix: str, limit: int, window_s: int
) -> bool:
    """Count ONE new delivery against this caller's window, atomically.

    Called only once a claim is won, so a duplicate the upstream is retrying
    costs nothing -- the cap is on NEW work, which is the thing a compromised or
    runaway source can make unbounded.

    Args:
        client: The Valkey client.
        key_prefix: The counter's namespace, without the window suffix.
        limit: The most new deliveries allowed per window.
        window_s: The window length in seconds.

    Returns:
        True when this delivery fits inside the window's allowance.
    """

    window = int(time.time()) // window_s
    key = f"{key_prefix}:{window}"
    count: Any = await client.eval(_QUOTA_SCRIPT, 1, key, str(window_s))
    return int(count) <= limit


async def release_claim(client: redis.Redis, key: str, owner: str) -> None:
    """Give back a claim this request took but will not enqueue.

    Args:
        client: The Valkey client.
        key: This delivery's claim key.
        owner: This request's owner token, compared before the delete.
    """

    await client.eval(_RELEASE_SCRIPT, 1, key, owner)


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
