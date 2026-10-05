"""The channel read ledger the API and the worker share (ADR 0100, #2877).

One logical turn owns four keys under the configured worker prefix, all in one
cluster slot through a hash tag:

``gen``     an INCR counter; a capability is current only while its ``gen``
            matches the one stored in ``active``.
``active``  ``<owner>:<gen>``, alive for the capability TTL. The opener names
            the owner; a steer bumps the generation and keeps the owner.
``pages``   the page budget spent so far, capped at ``MAX_PAGES_PER_TURN``.
``marker``  proof the logical turn opened, so an approval resume can tell a
            continued turn from one whose ledger has expired.

One more key per opener, ``owner_index_key``, is a set of the logical turns
an owner opened. The open script adds to it in the same script that sets the
active value, so a worker that never saw the mint response (a timeout, a
cancellation, a malformed body) still revokes everything it opened by owner.
Callers pass it as the open script's fifth KEYS entry, built with
``owner_index_key``, so every key a script touches is declared.

``pages``, ``marker`` and ``gen`` share ``LEDGER_TTL_S`` so the budget survives
an approval suspension. The API mints, renews and charges through these
scripts; the worker revokes by running ``revoke_owner`` directly, so a
terminal revoke holds while the API is down. Both import this module, which is
why the key layout and scripts exist exactly once.
"""

from __future__ import annotations

import hashlib
import uuid
from typing import Final

from redis.asyncio import Redis

LEDGER_TTL_S: Final[int] = 7 * 24 * 60 * 60
MAX_PAGES_PER_TURN: Final[int] = 8
# The active entry is a lease (review round 2): its opener renews it with
# ``refresh_owner`` while the attempt is live, so a worker that dies or cannot
# revoke leaves it to lapse within one lease, not at the token's expiry. Read at
# call time, not bound at import.
LEASE_TTL_S = 90


def turn_key(turn: str) -> str:
    """The fixed width digest a logical turn id is stored under."""

    return hashlib.sha256(turn.encode()).hexdigest()[:32]


def owner_index_key(prefix: str, agent_id: uuid.UUID, owner: str) -> str:
    """The set of logical turn keys ``owner`` opened for ``agent_id``.

    Passed to ``OPEN_LUA`` as KEYS[5] and read by ``revoke_by_owner``."""

    return f"{prefix}:channel-read-owner:{{{agent_id}}}:{owner}"


def tombstone_key(prefix: str, agent_id: uuid.UUID, owner: str) -> str:
    """Set when ``owner``'s attempt settled; ``OPEN_LUA`` then refuses that owner.

    Passed to ``OPEN_LUA`` as KEYS[6]."""

    return f"{prefix}:channel-read-tombstone:{{{agent_id}}}:{owner}"


def ledger_keys(prefix: str, agent_id: uuid.UUID, turn_key: str) -> tuple[str, str, str, str]:
    """The ``gen``, ``active``, ``pages`` and ``marker`` keys of one logical turn."""

    base = f"{prefix}:channel-read:{{{agent_id}:{turn_key}}}"
    return f"{base}:gen", f"{base}:active", f"{base}:pages", f"{base}:marker"


# KEYS: gen, active, pages, marker, owner index (``owner_index_key``),
# tombstone (``tombstone_key``). ARGV: owner, lease ttl, ledger ttl, resume
# flag, turn key, capability ttl. A tombstoned owner (its attempt already
# settled) writes nothing and returns -2; a resume with no marker writes
# nothing and returns -1. The active value lives one lease; the owner index
# gains this turn and lives as long as the capability could.
OPEN_LUA: Final[str] = """
if redis.call('EXISTS', KEYS[6]) == 1 then
    return -2
end
if ARGV[4] == '1' and redis.call('EXISTS', KEYS[4]) == 0 then
    return -1
end
redis.call('SET', KEYS[4], '1', 'NX', 'EX', ARGV[3])
redis.call('SET', KEYS[3], '0', 'NX', 'EX', ARGV[3])
local gen = redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], ARGV[3])
redis.call('SET', KEYS[2], ARGV[1] .. ':' .. gen, 'EX', ARGV[2])
redis.call('SADD', KEYS[5], ARGV[5])
if redis.call('TTL', KEYS[5]) < tonumber(ARGV[6]) then
    redis.call('EXPIRE', KEYS[5], ARGV[6])
end
return gen
"""

# KEYS: active. ARGV: owner, lease ttl. Renews only the named owner's lease.
REFRESH_LUA: Final[str] = """
local value = redis.call('GET', KEYS[1])
if not value or string.match(value, '^(.*):%d+$') ~= ARGV[1] then
    return 0
end
redis.call('EXPIRE', KEYS[1], ARGV[2])
return 1
"""

# KEYS: gen, active. Renews only a live opener: no active key, no generation.
# Returns {gen, remaining ms of the active key}.
STEER_LUA: Final[str] = """
local value = redis.call('GET', KEYS[2])
if not value then
    return false
end
local owner = string.match(value, '^(.*):%d+$')
if not owner then
    return false
end
local gen = redis.call('INCR', KEYS[1])
redis.call('SET', KEYS[2], owner .. ':' .. gen, 'KEEPTTL')
return {gen, redis.call('PTTL', KEYS[2])}
"""

# KEYS: active, pages. ARGV: generation, page cap. The generation is rechecked
# inside the script so a revoke racing the read still refuses.
RESERVE_LUA: Final[str] = """
local value = redis.call('GET', KEYS[1])
if not value or string.match(value, ':(%d+)$') ~= ARGV[1] then
    return 'inactive'
end
local spent = redis.call('GET', KEYS[2])
if not spent then
    return 'expired'
end
if tonumber(spent) >= tonumber(ARGV[2]) then
    return 'exhausted'
end
redis.call('INCR', KEYS[2])
return 'reserved'
"""

# KEYS: pages. Returns a page a failed provider call did not use.
RELEASE_LUA: Final[str] = """
local spent = redis.call('GET', KEYS[1])
if spent and tonumber(spent) > 0 then
    redis.call('DECR', KEYS[1])
end
return 0
"""

# KEYS: active. ARGV: owner. Deletes only the named owner's generation, so a
# late revoke from an earlier opener never kills a resume's capability.
REVOKE_LUA: Final[str] = """
local value = redis.call('GET', KEYS[1])
if not value or string.match(value, '^(.*):%d+$') ~= ARGV[1] then
    return 0
end
redis.call('DEL', KEYS[1])
return 1
"""


async def revoke_owner(
    client: Redis, prefix: str, agent_id: uuid.UUID, turn_key: str, owner: str
) -> bool:
    """End ``owner``'s capability for the logical turn; False when it does not hold it."""

    _, active, _, _ = ledger_keys(prefix, agent_id, turn_key)
    revoked = await client.eval(REVOKE_LUA, 1, active, owner)
    return int(revoked) == 1


async def refresh_owner(
    client: Redis, prefix: str, agent_id: uuid.UUID, turn_key: str, owner: str
) -> bool:
    """Renew ``owner``'s lease on the logical turn; False when it does not hold it."""

    _, active, _, _ = ledger_keys(prefix, agent_id, turn_key)
    refreshed = await client.eval(REFRESH_LUA, 1, active, owner, LEASE_TTL_S)
    return int(refreshed) == 1


async def tombstone_owner(
    client: Redis, prefix: str, agent_id: uuid.UUID, owner: str, ttl_s: int
) -> None:
    """Mark ``owner``'s attempt settled, so an open committed later is refused."""

    await client.set(tombstone_key(prefix, agent_id, owner), "1", ex=max(1, int(ttl_s)))


async def revoke_by_owner(client: Redis, prefix: str, agent_id: uuid.UUID, owner: str) -> bool:
    """End every capability ``owner`` opened for ``agent_id``, found by its index.

    For a worker that cannot name the logical turn (its mint response was
    lost). Each delete is the owner checked ``revoke_owner``, so a turn a newer
    owner reopened or steered is left alone. A turn leaves the index only after
    its revoke ran, so a failure partway keeps the rest for a retry. True only
    when something was removed."""

    index = owner_index_key(prefix, agent_id, owner)
    removed = False
    for member in await client.smembers(index):
        digest = member.decode() if isinstance(member, bytes) else str(member)
        if await revoke_owner(client, prefix, agent_id, digest, owner):
            removed = True
        await client.srem(index, member)
    return removed
