"""Valkey markers for idempotency, crash-safe side effects, and the completion outbox.

Three markers, all keyed by the event id (the idempotency key the ingress
assigned):

- ``done``: set once an event has been terminally handled (streamed to a final,
  or escalated). A redelivery (Slack retry that slipped the dispatcher guard, or
  a crash-recovery reclaim of an already-finished entry) sees it and is skipped.
- ``side_effect``: set the instant a ``side_effect_flag`` frame is observed, and
  therefore durable across a worker crash. The no-retry-after-side-effects rule
  needs this to survive process death: if a reclaimed event already executed a
  side effect but never reached ``done``, it must escalate, not silently re-run.
- ``completion``: the durable outbox for ``turn.completed`` (ADR-0096 EB-B6).

The completion outbox exists because ``done`` is a one-way door. Once it lands,
the already-done skip returns before anything else on every redelivery -- so a
crash or an HTTP failure between marking done and emitting the completion would
suppress the only ``turn.completed`` that will ever exist, and redelivery could
not retry it. The record is therefore written BEFORE ``done``, flagged done in
the SAME transaction as ``done``, and cleared only after a CONFIRMED emit.

Two structural choices carry that guarantee:

- **The pending index is a SET, not a SCAN over the keyspace.** The maintenance
  loop must not scan a production Valkey, and a redelivery-only sweep would never
  reach a turn whose stream entry was already acked.
- **The record has NO expiry.** A payload TTL shorter than the retention window
  leaves a set member pointing at an expired payload -- completion permanently
  lost, silently. Retention is a decision the sweeper makes out loud instead.

The outbox also carries a DEDUPE consequence, which is what ``is_terminal``
answers: a record proves its turn finished, so a turn that owns one must not be
rerun for as long as that record can exist. Completion emit is at-least-once;
turn SIDE EFFECTS are at-most-once for the whole outbox retention window, and
that is why ``mark_done`` widens the marker's own TTL to match.

Since ADR-0131 the terminal write has TWO forms, and they differ only by a
precondition. ``settle_fenced`` is the form a delivery OWNER uses: it verifies
the ownership lease and its fencing generation in the same script that writes
the record and the marker, so a fenced-out owner writes nothing at all.
``mark_completion_pending`` + ``mark_done`` remain the leaseless form, used by a
kernel called without a lease and by the sweeper -- neither of which is an owner
and neither of which has a fence to check. The ordering, the TTLs, and the
resulting state are identical in both; only the precondition is new.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from aci_protocol.service_config import STREAM_PAYLOAD_FIELD
from channel_protocol.reply import TurnCompleted
from curie_internal.channel_read_ledger import LEDGER_TTL_S as _CHANNEL_READ_PIN_TTL_S
from curie_internal.channel_read_ledger import refresh_owner as _refresh_channel_read_owner
from curie_internal.channel_read_ledger import revoke_by_owner as _revoke_channel_read_by_owner
from curie_internal.channel_read_ledger import revoke_owner as _revoke_channel_read_owner
from curie_internal.channel_read_ledger import tombstone_owner as _tombstone_channel_read_owner
from pydantic import BaseModel
from redis.asyncio import Redis

from .config import WorkerConfig
from .reply_sink import ProviderEgressRefusedError, TargetRoute

DoneMarkerValue = Literal["1", "history_capacity"]

# The longest a per-turn memory credential lives: ``binding.SANDBOX_TOKEN_TTL_SECONDS``
# (not imported: binding imports this package's config, and the value is tiny).
_MEMORY_STEER_TURNS_TTL_S = 24 * 60 * 60

# ADR 0100 (#2877): delete the live channel read record only when its owner
# is the caller, so an earlier attempt's late settlement never deletes the
# record a newer turn on the thread wrote. A value that is not JSON is left.
_TAKE_CHANNEL_READ_TURN_LUA = """
local value = redis.call('GET', KEYS[1])
if not value then return 0 end
local ok, record = pcall(cjson.decode, value)
if ok and type(record) == 'table' and record['owner'] == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
"""


@dataclass(frozen=True)
class LiveChannelReadTurn:
    """The live channel read logical turn on one thread (ADR 0100, #2877).

    Written by the attempt that opened it, read by a steer on any worker so it
    can renew the same logical turn with the opener's event id and default
    channel. Carries no token. ``default`` is None for a targetless turn.
    """

    agent_id: uuid.UUID
    deployment_id: uuid.UUID
    event_id: str
    owner: str
    default: tuple[str, str] | None

    def to_json(self) -> str:
        kind, address = self.default if self.default is not None else (None, None)
        return json.dumps(
            {
                "agent_id": str(self.agent_id),
                "deployment_id": str(self.deployment_id),
                "event_id": self.event_id,
                "owner": self.owner,
                "kind": kind,
                "address": address,
            },
            separators=(",", ":"),
        )

    @classmethod
    def from_json(cls, raw: str) -> LiveChannelReadTurn | None:
        try:
            data = json.loads(raw)
            kind, address = data.get("kind"), data.get("address")
            default = (
                (kind, address) if isinstance(kind, str) and isinstance(address, str) else None
            )
            event_id, owner = data["event_id"], data["owner"]
            if not isinstance(event_id, str) or not isinstance(owner, str):
                return None
            return cls(
                agent_id=uuid.UUID(data["agent_id"]),
                deployment_id=uuid.UUID(data["deployment_id"]),
                event_id=event_id,
                owner=owner,
                default=default,
            )
        except (ValueError, TypeError, KeyError, AttributeError):
            return None


@dataclass(frozen=True)
class ChannelReadPin:
    """The deployment one sandbox claim on a thread was booted for (ADR 0100)."""

    claim_name: str
    agent_id: uuid.UUID
    deployment_id: uuid.UUID
    bundle_ref: str | None


# Stored fields of the completion hash. The done flag is its OWN field rather
# than a value inside the record JSON so it can be set in the same MULTI as the
# done marker: a read-modify-write of the JSON could not be atomic with it, and
# the two diverging is precisely the loss this outbox exists to prevent.
_RECORD_FIELD = "record"
_DONE_FIELD = "done"
# The record's identity, minted per write. A clear is compare-and-checked
# against it so an emitter can only ever clear the record IT read: a concurrent
# retry that rewrote the record for the same event id owns a different identity,
# and clearing that one would discard a completion nobody has delivered.
_GENERATION_FIELD = "gen"
_CAUSE_FIELD = "cause"

# Set the done marker and flag the completion record done in ONE round trip.
# The record is only touched when it still exists, so a sweeper that cleared it
# concurrently is never resurrected as a payload-less key with no expiry.
_MARK_DONE_LUA = """
if ARGV[3] == 'history_capacity' then
  redis.call('SET', KEYS[1], ARGV[3])
else
  redis.call('SET', KEYS[1], ARGV[3], 'EX', ARGV[1])
end
if redis.call('EXISTS', KEYS[2]) == 1 then
  redis.call('HSET', KEYS[2], ARGV[2], '1')
end
return 1
"""

# The fencing generation field inside the DELIVERY STATE hash. Owned by
# ``delivery_lease.py`` (which HINCRBYs it on every change of authority); named
# here because ``_SETTLE_FENCED_LUA`` below reads it, and a literal buried in Lua
# is exactly the kind of cross-module constant that drifts silently.
_DELIVERY_GENERATION_FIELD = "gen"

# The FENCED terminal settlement (ADR-0131): the ownership check and the whole
# terminal write, indivisibly.
#
# This is the ADR's "one atomic operation verifies the current lease, writes the
# done marker and completion outbox, and identifies the winning owner". It fuses
# what ``mark_completion_pending`` + ``mark_done`` do in two calls, and the
# fusion is the point: two calls cannot be atomic with a fence check, so a slow
# owner could pass the check and then write after a replacement had already
# taken authority.
#
# It does NOT reorder anything. The record is still written BEFORE/WITH the done
# marker, for the reason the module docstring gives, and is still cleared only
# after a CONFIRMED emit (by ``clear_completion``, which is untouched). The fence
# adds a PRECONDITION in front of the same ordering.
#
# Two guards, both before any write, both fail-closed:
#   - the lease key must still hold OUR owner token; and
#   - the delivery state's fencing generation must still be the one we hold.
# ``_ACQUIRE_LUA`` installs the token and increments the generation in one
# script, so the two move together; checking both means a hand-rolled or
# partially-applied change of authority cannot slip between them.
#
# ``_MARK_DONE_LUA`` above serves the leaseless path (a kernel called without a
# lease) and the completion sweeper, neither of which is a delivery owner.
_SETTLE_FENCED_LUA = """
if redis.call('GET', KEYS[4]) ~= ARGV[7] then return 0 end
if redis.call('HGET', KEYS[5], ARGV[8]) ~= ARGV[9] then return 0 end
redis.call('HDEL', KEYS[2], ARGV[11])
redis.call('HSET', KEYS[2], ARGV[3], ARGV[5], ARGV[2], '1', ARGV[4], ARGV[6])
redis.call('SADD', KEYS[3], ARGV[10])
if ARGV[12] == 'history_capacity' then
  redis.call('SET', KEYS[1], ARGV[12])
else
  redis.call('SET', KEYS[1], ARGV[12], 'EX', ARGV[1])
end
return 1
"""

# The fenced settlement that also publishes the next slice of a long scheduled
# sweep (ADR-0160, #2878): ``_SETTLE_FENCED_LUA`` exactly, plus five lines placed
# right after both guards. Built from the shared text so the guards cannot
# drift: a fenced-out owner writes nothing and publishes nothing.
#
# These run before the record writes. A replay (the production client retries
# EVAL after a lost reply) finds the event's published marker (``KEYS[7]``)
# holding THIS call's record generation and returns success without writing or
# publishing again. The marker, not the completion record, is the evidence: an
# outbox clear deletes the record but never the marker. Otherwise the successor
# is XADDed FIRST, with the marker set beside it, before the record, the index
# and the done marker, so a failing XADD aborts the script with nothing
# written: the slice is never settled without its successor.
_SETTLE_FENCED_AND_PUBLISH_LUA = _SETTLE_FENCED_LUA.replace(
    "\nredis.call('HDEL', KEYS[2], ARGV[11])\n",
    "\nif redis.call('GET', KEYS[7]) == ARGV[6] then"
    "\n  return 1"
    "\nend"
    "\nredis.call('XADD', KEYS[6], '*', ARGV[13], ARGV[14])"
    "\nredis.call('SET', KEYS[7], ARGV[6], 'EX', ARGV[1])"
    "\nredis.call('HDEL', KEYS[2], ARGV[11])\n",
)

# The fenced MARKER-ONLY settlement (#2963): a targetless cron turn owes no
# ``turn.completed`` (no adapter is waiting), so it writes no outbox record. The
# two guards are exactly ``_SETTLE_FENCED_LUA``'s, so a fenced-out owner writes
# nothing here either.
_SETTLE_FENCED_MARKER_LUA = """
if redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
if redis.call('HGET', KEYS[3], ARGV[3]) ~= ARGV[4] then return 0 end
redis.call('SET', KEYS[1], '1', 'EX', ARGV[1])
return 1
"""

# Clear the record and its set membership together, but only when the record is
# still the one the caller read. A stored generation that does not match -- a
# different one, or none at all -- means this is not the record the caller read;
# the safe answer is to touch nothing and let whoever owns it own its clear.
# There is no unconditional arm: a record with no generation is MALFORMED, never
# a legacy shape, because the outbox is introduced by this train and every
# writer sets the field. Clearing on an absent generation would let one pass
# delete a record it never read.
_CLEAR_COMPLETION_LUA = """
if redis.call('HGET', KEYS[1], ARGV[2]) == ARGV[1] then
  redis.call('DEL', KEYS[1])
  redis.call('SREM', KEYS[2], ARGV[3])
  return 1
end
return 0
"""


# Attribute or clear the cause only on the generation whose send observed it.
# A stale emitter cannot change a replacement record or resurrect a cleared one.
_UPDATE_COMPLETION_CAUSE_LUA = """
if redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] then return 0 end
if ARGV[4] == '' then
  redis.call('HDEL', KEYS[1], ARGV[3])
else
  redis.call('HSET', KEYS[1], ARGV[3], ARGV[4])
end
return 1
"""


# A terminal rejection is retained in the existing bounded graveyard BEFORE the
# owed record disappears. The generation fence covers both effects: a stale
# sweep can neither accuse nor clear the replacement record. An XADD failure
# leaves the outbox intact for the next sweep.
_DEAD_LETTER_COMPLETION_LUA = """
if redis.call('HGET', KEYS[1], ARGV[2]) ~= ARGV[1] then return 0 end
redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[4], '*',
  'event_id', ARGV[3], 'completion', ARGV[5], 'dl_reason', ARGV[6],
  'dl_delivery_count', '1', 'dl_source', 'completion-outbox',
  'dl_dead_lettered_at', ARGV[7])
redis.call('DEL', KEYS[1])
redis.call('SREM', KEYS[2], ARGV[3])
return 1
"""


class CompletionRecord(BaseModel):
    """A self-contained, separately retryable ``turn.completed``.

    Self-contained is the whole point: it carries the ALREADY-RESOLVED route, so
    any later emitter -- the already-done skip or a sweeper -- uses the stored
    route and never re-resolves. A sweeper draining an acked entry has no binding
    lookup available to it, and re-resolving would read a binding an operator may
    since have re-pointed.
    """

    event_id: str
    event: TurnCompleted
    route: TargetRoute
    created_at: float
    done: bool = False


class MalformedCompletionError(RuntimeError):
    """A stored completion record is missing a field every writer sets.

    The outbox is introduced by this train, so there is no pre-upgrade record to
    be tolerant of: a record without its done flag or its generation was
    corrupted or written by something that is not this code. Reading it as
    "not done yet" or as "safe to clear" both act on a record nobody can vouch
    for, so it is raised out instead and the caller quarantines it.
    """


@dataclass(frozen=True)
class StoredCompletion:
    """A completion record AS STORED, with the two facts the JSON cannot carry.

    Both are REQUIRED, which is what keeps the impossible legacy mode out by
    construction: ``done_flag`` is ``False`` while this worker is still
    mid-flight and ``True`` once the turn is durably done, and there is no third
    state for a guard to be lenient about. ``generation`` is the record's
    identity, used to compare-and-clear.
    """

    record: CompletionRecord
    done_flag: bool
    generation: str
    cause: str | None


class Markers:
    """Idempotency, side-effect and completion markers over Valkey."""

    def __init__(self, redis: Redis, config: WorkerConfig) -> None:
        self._redis = redis
        self._config = config

    async def push_steer_memory_turns(
        self, agent_id: uuid.UUID, live_turn: str, turns: Sequence[tuple[uuid.UUID, str]]
    ) -> None:
        """Hand a steer's memory turn claims to the live turn it joined (#3776).

        ``live_turn`` names the runner turn the steer landed in. The attempt
        that owns that turn drains them when it ends. The TTL is the longest a
        turn credential lives, so an owner that never drains leaves nothing
        behind once the credentials have expired anyway."""

        if not turns:
            return
        key = self._config.memory_steer_turns_key(str(agent_id), live_turn)
        values = [json.dumps([str(agent), turn]) for agent, turn in turns]
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.rpush(key, *values)
            pipe.expire(key, _MEMORY_STEER_TURNS_TTL_S)
            await pipe.execute()

    async def record_channel_read_turn(
        self, thread_key: str, record: LiveChannelReadTurn, ttl_s: int
    ) -> None:
        """Record the live channel read logical turn on a thread (ADR 0100).

        Expires with the capability it describes, so a record whose owner
        never settles leaves nothing behind once the token is dead anyway."""

        await self._redis.set(
            self._config.channel_read_turn_key(thread_key), record.to_json(), ex=max(1, ttl_s)
        )

    async def read_channel_read_turn(self, thread_key: str) -> LiveChannelReadTurn | None:
        """The live channel read logical turn on a thread, or None."""

        raw = _as_str(await self._redis.get(self._config.channel_read_turn_key(thread_key)))
        return None if raw is None else LiveChannelReadTurn.from_json(raw)

    async def take_channel_read_turn(self, thread_key: str, owner: str) -> bool:
        """Delete the thread's live record only if ``owner`` wrote it."""

        taken = await self._redis.eval(
            _TAKE_CHANNEL_READ_TURN_LUA,
            1,
            self._config.channel_read_turn_key(thread_key),
            owner,
        )
        return bool(taken)

    async def read_channel_read_pin(self, thread_key: str) -> ChannelReadPin | None:
        """The deployment the thread's current sandbox claim was booted for."""

        raw = _as_str(await self._redis.get(self._config.channel_read_pin_key(thread_key)))
        if raw is None:
            return None
        try:
            data = json.loads(raw)
            return ChannelReadPin(
                claim_name=str(data["claim_name"]),
                agent_id=uuid.UUID(data["agent_id"]),
                deployment_id=uuid.UUID(data["deployment_id"]),
                bundle_ref=data.get("bundle_ref")
                if isinstance(data.get("bundle_ref"), str)
                else None,
            )
        except (ValueError, TypeError, KeyError):
            return None

    async def pin_channel_read_deployment(
        self,
        thread_key: str,
        claim_name: str,
        agent_id: uuid.UUID,
        deployment_id: uuid.UUID,
        bundle_ref: str | None,
    ) -> None:
        """Record the deployment a freshly claimed sandbox was booted for."""

        await self._redis.set(
            self._config.channel_read_pin_key(thread_key),
            json.dumps(
                {
                    "claim_name": claim_name,
                    "agent_id": str(agent_id),
                    "deployment_id": str(deployment_id),
                    "bundle_ref": bundle_ref,
                },
                separators=(",", ":"),
            ),
            ex=_CHANNEL_READ_PIN_TTL_S,
        )

    async def refresh_channel_read_owner(
        self, agent_id: uuid.UUID, turn_key: str, owner: str
    ) -> bool:
        """Renew the opener's lease on a live logical turn (owner checked)."""

        return await _refresh_channel_read_owner(
            self._redis, self._config.key_prefix, agent_id, turn_key, owner
        )

    async def tombstone_channel_read_owner(
        self, agent_id: uuid.UUID, owner: str, ttl_s: int
    ) -> None:
        """Mark an attempt's owner settled, so a late open under it is refused."""

        await _tombstone_channel_read_owner(
            self._redis, self._config.key_prefix, agent_id, owner, ttl_s
        )

    async def revoke_channel_read_by_owner(self, agent_id: uuid.UUID, owner: str) -> bool:
        """Revoke every logical turn ``owner`` opened, through the ledger's index.

        For an attempt whose mint answer may have been lost. Owner checked per
        turn, so a newer owner's capability is untouched."""

        return await _revoke_channel_read_by_owner(
            self._redis, self._config.key_prefix, agent_id, owner
        )

    async def revoke_channel_read_owner(
        self, agent_id: uuid.UUID, turn_key: str, owner: str
    ) -> bool:
        """Revoke a logical turn's channel read capability as its opener.

        The owner checked delete runs directly in the Valkey the API reads, so
        revocation holds while the API is down. False when the active value
        belongs to another owner (a resume opener) or is already gone."""

        return await _revoke_channel_read_owner(
            self._redis, self._config.key_prefix, agent_id, turn_key, owner
        )

    async def drain_steer_memory_turns(
        self, agent_id: uuid.UUID, live_turn: str
    ) -> list[tuple[uuid.UUID, str]]:
        """Take every memory turn claim steered into one live turn."""

        key = self._config.memory_steer_turns_key(str(agent_id), live_turn)
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.lrange(key, 0, -1)
            pipe.delete(key)
            raw, _ = await pipe.execute()
        turns: list[tuple[uuid.UUID, str]] = []
        for item in raw or []:
            try:
                agent, turn = json.loads(_as_str(item) or "")
                turns.append((uuid.UUID(agent), str(turn)))
            except (ValueError, TypeError):
                continue
        return turns

    async def is_terminal(self, event_id: str) -> bool:
        """Has this event ALREADY been handled to a terminal state?

        The dedupe question the kernel actually needs answered, and it is wider
        than the done marker alone. ``done_key`` is one marker with one TTL; a turn that
        wrote an outbox record is also provably terminal, and that record is
        retained for ``completion_max_retention_s`` (7 days) rather than
        ``idempotency_ttl_s`` (1 day). Reading only the marker let a >24h outage
        rerun a finished turn: the startup sweep emits the record and clears it,
        and the stream entry that was never acked is then reclaimed with no
        marker and no record left to refuse it. So a DONE outbox record is
        terminal in its own right, and ``mark_done`` holds the marker itself for
        the full retention window whenever a record exists (below).

        One round trip, on the sacred path: the two reads are pipelined.
        """
        async with self._redis.pipeline(transaction=False) as pipe:
            pipe.exists(self._config.done_key(event_id))
            pipe.hget(self._config.completion_key(event_id), _DONE_FIELD)
            marker, flag = await pipe.execute()
        if marker:
            return True
        return _as_str(flag) == "1"

    async def mark_done(self, event_id: str, *, marker_value: DoneMarkerValue) -> None:
        """Mark the event durably done AND flag its outbox record, in one call.

        The two writes are ONE round trip so they cannot diverge: a record
        flagged done without the marker would let a sweeper emit for a turn that
        is about to rerun, and a marker without the flag would strand the record
        behind a guard that can never pass once ``done_key`` expires at
        ``idempotency_ttl_s``.

        This is the form for every turn that owes a completion. Every such
        terminal outcome goes through ``Kernel._complete``, which writes the
        outbox record for THIS event id first, so the record key is always this
        event's own -- the Lua below is a no-op on the record when the sweeper
        cleared it concurrently, which is the only case where there is nothing
        to flag.

        It also widens the MARKER's own TTL to the outbox retention window, for
        the reason ``is_terminal`` states: the outbox proves this turn finished
        for 7 days, so a dedupe state that lapses after 1 day can rerun a turn
        whose completion has already been delivered. A verified review that
        exceeded history capacity keeps its distinct marker without expiry
        until the API mirrors the refusal to SQL.
        """
        ttl_s = max(self._config.idempotency_ttl_s, int(self._config.completion_max_retention_s))
        await self._redis.eval(
            _MARK_DONE_LUA,
            2,
            self._config.done_key(event_id),
            self._config.completion_key(event_id),
            str(ttl_s),
            _DONE_FIELD,
            marker_value,
        )

    def _done_ttl_s(self) -> int:
        return max(self._config.idempotency_ttl_s, int(self._config.completion_max_retention_s))

    async def mark_done_without_completion(self, event_id: str) -> None:
        """Leaseless marker-only done, for a targetless turn (#2963).

        The one terminal outcome with no outbox record: no adapter is waiting
        for a ``turn.completed``. The marker keeps ``mark_done``'s TTL so the
        dedupe window does not depend on which form settled the turn.
        """
        await self._redis.set(self._config.done_key(event_id), "1", ex=self._done_ttl_s())

    async def settle_fenced_without_completion(
        self,
        event_id: str,
        *,
        stream: str,
        group: str,
        entry_id: str,
        owner: str,
        generation: int,
    ) -> bool:
        """The fenced sibling of ``mark_done_without_completion``.

        Same lease-token and generation fence as ``settle_fenced``; returns
        False when the fence refused, in which case nothing was written.
        """
        settled = await self._redis.eval(
            _SETTLE_FENCED_MARKER_LUA,
            3,
            self._config.done_key(event_id),
            self._config.delivery_lease_key(stream, group, entry_id),
            self._config.delivery_state_key(stream, group, entry_id),
            str(self._done_ttl_s()),
            owner,
            _DELIVERY_GENERATION_FIELD,
            str(generation),
        )
        return int(settled) == 1

    async def saw_side_effect(self, event_id: str) -> bool:
        return bool(await self._redis.exists(self._config.side_effect_key(event_id)))

    async def mark_side_effect(self, event_id: str) -> None:
        await self._redis.set(
            self._config.side_effect_key(event_id), "1", ex=self._config.idempotency_ttl_s
        )

    # -- the completion outbox ------------------------------------------------

    async def mark_completion_pending(self, event_id: str, record: CompletionRecord) -> str:
        """Write the record and index it, before the turn is marked done.

        Returns the record's GENERATION, which the writer keeps so its own clear
        can be compare-and-checked against the record it wrote (a concurrent
        retry that rewrote the record owns a different one).
        """
        generation = uuid.uuid4().hex
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.hdel(self._config.completion_key(event_id), _CAUSE_FIELD)
            pipe.hset(
                self._config.completion_key(event_id),
                mapping={
                    _RECORD_FIELD: record.model_dump_json(),
                    _DONE_FIELD: "1" if record.done else "0",
                    _GENERATION_FIELD: generation,
                },
            )
            pipe.sadd(self._config.completions_pending_key(), event_id)
            await pipe.execute()
        return generation

    async def settle_fenced(
        self,
        event_id: str,
        record: CompletionRecord,
        *,
        stream: str,
        group: str,
        entry_id: str,
        owner: str,
        generation: int,
        marker_value: DoneMarkerValue,
    ) -> str | None:
        """Settle this turn terminally, but only if this owner still holds the fence.

        The fenced sibling of ``mark_completion_pending`` + ``mark_done``, fused
        into one script so the ownership check and the terminal write cannot be
        separated (see ``_SETTLE_FENCED_LUA``). On success the outbox record is
        stored, indexed, and flagged done, and the done marker is set -- the same
        state, in the same order, the two-call path produces.

        The delivery triple is the caller's, taken straight off the
        ``DeliveryLease`` it was granted for, so the lease and state keys named
        here are EXACTLY the ones the fence was acquired on. There is no lookup:
        ADR-0131 keys a delivery by ``(stream, group, entry_id)``, the lease
        carries that triple, and the two key helpers on ``WorkerConfig`` are the
        single definition of how it becomes a key. A settle therefore costs one
        round trip and touches nothing but this delivery.

        Returns the record's GENERATION on success, so the caller can
        compare-and-clear the record it wrote, exactly as the leaseless path
        does. Returns ``None`` when the fence refused: this owner's lease has
        moved on, and per ADR-0131 it "may not ACK, dead-letter, clear an outbox
        record, or emit a terminal result". Nothing was written. The history
        capacity marker uses the same fence and has no expiry until SQL mirror.
        """
        lease_key = self._config.delivery_lease_key(stream, group, entry_id)
        state_key = self._config.delivery_state_key(stream, group, entry_id)
        record_generation = uuid.uuid4().hex
        ttl_s = max(self._config.idempotency_ttl_s, int(self._config.completion_max_retention_s))
        settled = await self._redis.eval(
            _SETTLE_FENCED_LUA,
            5,
            self._config.done_key(event_id),
            self._config.completion_key(event_id),
            self._config.completions_pending_key(),
            lease_key,
            state_key,
            str(ttl_s),
            _DONE_FIELD,
            _RECORD_FIELD,
            _GENERATION_FIELD,
            record.model_dump_json(),
            record_generation,
            owner,
            _DELIVERY_GENERATION_FIELD,
            str(generation),
            event_id,
            _CAUSE_FIELD,
            marker_value,
        )
        return record_generation if int(settled) == 1 else None

    async def settle_fenced_and_publish(
        self,
        event_id: str,
        record: CompletionRecord,
        *,
        stream: str,
        group: str,
        entry_id: str,
        owner: str,
        generation: int,
        marker_value: DoneMarkerValue,
        successor_stream: str,
        successor_payload: str,
        record_generation: str,
    ) -> str | None:
        """``settle_fenced``, plus publishing a sweep's next slice in the same script.

        The caller supplies ``record_generation`` so that, when the reply is
        lost, it can read ``sweep_published_generation`` and tell a committed
        script from one that never ran. Returns ``record_generation`` on success and
        None when the fence refused, in which case nothing was written and
        nothing was published.
        """
        lease_key = self._config.delivery_lease_key(stream, group, entry_id)
        state_key = self._config.delivery_state_key(stream, group, entry_id)
        ttl_s = max(self._config.idempotency_ttl_s, int(self._config.completion_max_retention_s))
        settled = await self._redis.eval(
            _SETTLE_FENCED_AND_PUBLISH_LUA,
            7,
            self._config.done_key(event_id),
            self._config.completion_key(event_id),
            self._config.completions_pending_key(),
            lease_key,
            state_key,
            successor_stream,
            self._config.sweep_published_key(event_id),
            str(ttl_s),
            _DONE_FIELD,
            _RECORD_FIELD,
            _GENERATION_FIELD,
            record.model_dump_json(),
            record_generation,
            owner,
            _DELIVERY_GENERATION_FIELD,
            str(generation),
            event_id,
            _CAUSE_FIELD,
            marker_value,
            STREAM_PAYLOAD_FIELD,
            successor_payload,
        )
        return record_generation if int(settled) == 1 else None

    async def sweep_published_generation(self, event_id: str) -> str | None:
        """The record generation of the settle that published this slice's
        successor, or None. Survives an outbox clear, unlike the record."""
        value = await self._redis.get(self._config.sweep_published_key(event_id))
        return _as_str(value)

    async def publish_successor(self, stream: str, payload: str) -> None:
        """Publish a sweep's next slice with no fence (the leaseless path only)."""
        await self._redis.xadd(stream, {STREAM_PAYLOAD_FIELD: payload})

    async def note_provider_egress_refusal(self, event_id: str, *, generation: str) -> bool:
        """Retain the fixed refusal cause only on the observed outbox generation."""
        return await self._update_completion_cause(
            event_id, generation=generation, cause=ProviderEgressRefusedError.reason
        )

    async def clear_completion_cause(self, event_id: str, *, generation: str) -> bool:
        """Remove an earlier refusal when this generation fails for another cause."""
        return await self._update_completion_cause(event_id, generation=generation, cause="")

    async def _update_completion_cause(self, event_id: str, *, generation: str, cause: str) -> bool:
        updated = await self._redis.eval(
            _UPDATE_COMPLETION_CAUSE_LUA,
            1,
            self._config.completion_key(event_id),
            _GENERATION_FIELD,
            generation,
            _CAUSE_FIELD,
            cause,
        )
        return bool(updated)

    async def read_completion(self, event_id: str) -> StoredCompletion | None:
        """The stored record AS STORED, or None when some emitter cleared it.

        Raises ``MalformedCompletionError`` when the hash exists but is missing
        the done flag or the generation. Both are written by
        ``mark_completion_pending`` on every path, so their absence is
        corruption, not an older shape to fall back for -- the outbox has no
        pre-upgrade records. The caller quarantines rather than guessing.
        """
        stored: dict[Any, Any] = await self._redis.hgetall(self._config.completion_key(event_id))
        return _parse_stored(event_id, stored)

    async def read_completions(
        self, event_ids: Sequence[str]
    ) -> dict[str, StoredCompletion | MalformedCompletionError | None]:
        """A whole sweep batch's records, read in ONE pipeline.

        The sweeper reads up to ``completion_sweep_batch`` (64) members per pass
        and used to pay a round trip per member before it could decide anything
        about any of them; the reads are independent, so they go out together.
        Not a transaction: these are plain reads, and MULTI would only add a
        blocking window on the same Valkey that holds the kernel's locks.

        A malformed record is returned AS the exception rather than raised,
        because one corrupt member must not abort the batch -- the caller
        quarantines that member and carries on with the rest, exactly as it does
        on the single-record path.
        """
        keys = [self._config.completion_key(event_id) for event_id in event_ids]
        if not keys:
            return {}
        async with self._redis.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.hgetall(key)
            stored_hashes = await pipe.execute()
        out: dict[str, StoredCompletion | MalformedCompletionError | None] = {}
        for event_id, stored in zip(event_ids, stored_hashes, strict=True):
            try:
                out[event_id] = _parse_stored(event_id, stored)
            except MalformedCompletionError as exc:
                out[event_id] = exc
        return out

    async def clear_completion(self, event_id: str, *, generation: str) -> bool:
        """Drop the payload and its set membership together, in one MULTI.

        Together, so the set and the payload can never diverge durably: a member
        without a payload is a completion nothing can reconstruct a route for.

        Compare-and-checked against ``generation``: the caller clears the record
        IT read, never whatever happens to be under the key now. Without that, a
        sweeper that read a stale record could delete the fresh one a concurrent
        retry wrote in between -- an undelivered completion discarded by a pass
        that never saw it. Returns whether anything was cleared.

        ``generation`` is required, and a stored record with none is never
        cleared here: that record is malformed and belongs in quarantine, not in
        a delete a caller cannot prove it owns.
        """
        cleared = await self._redis.eval(
            _CLEAR_COMPLETION_LUA,
            2,
            self._config.completion_key(event_id),
            self._config.completions_pending_key(),
            generation,
            _GENERATION_FIELD,
            event_id,
        )
        return bool(cleared)

    async def dead_letter_completion(
        self, record: CompletionRecord, *, generation: str, reason: str
    ) -> bool:
        """Atomically retain a terminal failure and clear only its own generation."""
        return bool(
            await self._redis.eval(
                _DEAD_LETTER_COMPLETION_LUA,
                3,
                self._config.completion_key(record.event_id),
                self._config.completions_pending_key(),
                self._config.dead_letter_stream_name(),
                generation,
                _GENERATION_FIELD,
                record.event_id,
                self._config.dead_letter_maxlen,
                record.event.model_dump_json(),
                reason,
                datetime.now(UTC).isoformat(),
            )
        )

    async def drop_pending_member(self, event_id: str) -> None:
        """Drop ONLY the set membership, leaving whatever key state exists.

        For the member whose payload is already gone: some emitter confirmed
        delivery and cleared it. Deleting the key here would destroy a record a
        concurrent retry may have just written under the same event id, so this
        removes the stale index entry and nothing else.

        Also the quarantine step for a MALFORMED record: the payload stays put
        for an operator to inspect, and only the index entry goes, so the sweeper
        stops re-reading a record it has already refused to act on.
        """
        await self._redis.srem(self._config.completions_pending_key(), event_id)

    async def pending_completions(self, limit: int) -> set[str]:
        """A BOUNDED batch of pending members, never the whole set.

        ``SRANDMEMBER`` with a count, not ``SMEMBERS``: one sweep pass must cost
        a bounded number of delivery attempts, because the startup sweep runs
        against exactly the backlog an outage left behind. Random sampling also
        keeps one poison record from head-of-lining every later pass -- the
        sweeper runs on the maintenance cadence, so the remainder is drained by
        the passes that follow.
        """
        # With a count, SRANDMEMBER answers with a list; the client's signature
        # also covers the countless single-member form, hence the narrowing.
        members: Any = await self._redis.srandmember(self._config.completions_pending_key(), limit)
        if not members:
            return set()
        if not isinstance(members, list):
            members = [members]
        return {str(_as_str(m)) for m in members}


def _parse_stored(event_id: str, stored: dict[Any, Any]) -> StoredCompletion | None:
    """One stored hash as a ``StoredCompletion``, or None when there is none.

    The single reading of the outbox hash, shared by the one-record and the
    batched read so the two can never drift on what counts as malformed.
    """
    raw = _as_str(stored.get(_RECORD_FIELD))
    if not raw:
        return None
    record = CompletionRecord.model_validate(json.loads(raw))
    flag = _as_str(stored.get(_DONE_FIELD))
    generation = _as_str(stored.get(_GENERATION_FIELD))
    if flag is None or generation is None:
        raise MalformedCompletionError(
            f"completion record {event_id} is missing "
            f"{'the done flag' if flag is None else 'its generation'}"
        )
    done_flag = flag == "1"
    return StoredCompletion(
        record=record.model_copy(update={"done": done_flag}),
        done_flag=done_flag,
        generation=generation,
        cause=_as_str(stored.get(_CAUSE_FIELD)),
    )


def _as_str(value: Any) -> str | None:
    """Hash values as ``str``, tolerating a client without ``decode_responses``."""
    if value is None:
        return None
    return value.decode() if isinstance(value, bytes) else str(value)
