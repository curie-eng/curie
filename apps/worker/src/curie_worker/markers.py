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
from typing import Any

from channel_protocol.reply import TurnCompleted
from pydantic import BaseModel
from redis.asyncio import Redis

from .config import WorkerConfig
from .reply_sink import ProviderEgressRefusedError, TargetRoute

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
redis.call('SET', KEYS[1], '1', 'EX', ARGV[1])
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
# ``_MARK_DONE_LUA`` above is deliberately NOT modified: it still serves the
# leaseless path (a kernel called without a lease) and the completion sweeper,
# neither of which is a delivery owner.
_SETTLE_FENCED_LUA = """
if redis.call('GET', KEYS[4]) ~= ARGV[7] then return 0 end
if redis.call('HGET', KEYS[5], ARGV[8]) ~= ARGV[9] then return 0 end
redis.call('HDEL', KEYS[2], ARGV[11])
redis.call('HSET', KEYS[2], ARGV[3], ARGV[5], ARGV[2], '1', ARGV[4], ARGV[6])
redis.call('SADD', KEYS[3], ARGV[10])
redis.call('SET', KEYS[1], '1', 'EX', ARGV[1])
return 1
"""

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

    async def mark_done(self, event_id: str) -> None:
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
        whose completion has already been delivered.
        """
        ttl_s = max(
            self._config.idempotency_ttl_s, int(self._config.completion_max_retention_s)
        )
        await self._redis.eval(
            _MARK_DONE_LUA,
            2,
            self._config.done_key(event_id),
            self._config.completion_key(event_id),
            str(ttl_s),
            _DONE_FIELD,
        )

    def _done_ttl_s(self) -> int:
        return max(
            self._config.idempotency_ttl_s, int(self._config.completion_max_retention_s)
        )

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
        record, or emit a terminal result". Nothing was written.
        """
        lease_key = self._config.delivery_lease_key(stream, group, entry_id)
        state_key = self._config.delivery_state_key(stream, group, entry_id)
        record_generation = uuid.uuid4().hex
        ttl_s = max(
            self._config.idempotency_ttl_s, int(self._config.completion_max_retention_s)
        )
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
        )
        return record_generation if int(settled) == 1 else None

    async def note_provider_egress_refusal(self, event_id: str, *, generation: str) -> bool:
        """Retain the fixed refusal cause only on the observed outbox generation."""
        return await self._update_completion_cause(
            event_id, generation=generation, cause=ProviderEgressRefusedError.reason
        )

    async def clear_completion_cause(self, event_id: str, *, generation: str) -> bool:
        """Remove an earlier refusal when this generation fails for another cause."""
        return await self._update_completion_cause(event_id, generation=generation, cause="")

    async def _update_completion_cause(
        self, event_id: str, *, generation: str, cause: str
    ) -> bool:
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
        stored: dict[Any, Any] = await self._redis.hgetall(
            self._config.completion_key(event_id)
        )
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
        members: Any = await self._redis.srandmember(
            self._config.completions_pending_key(), limit
        )
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
