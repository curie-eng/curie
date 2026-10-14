"""The renewable, fenced ownership lease for one stream delivery (ADR-0131, #1971).

A delivery is a ``(stream, group, entry_id)``. Owning its PEL row is *necessary
but not sufficient* to execute or settle it: the delivery also has a short Valkey
lease carrying an opaque owner token and a monotonically increasing fencing
generation, and only the holder of a live lease may run the handler, ACK,
dead-letter, clear an outbox record, or emit a terminal result.

Three design decisions are load-bearing enough to state here rather than in a
plan nobody reads at the call site.

**1. This store takes ``redis.asyncio.Redis`` directly, not the ``StreamBroker``
port.** The fence *is* Valkey semantics -- atomic ``EVAL``, server ``TIME``, key
expiry with ``PX``, entry-targeted ``XPENDING`` ownership, and ``XCLAIM ...
JUSTID``. Widening ``StreamBroker`` would force a hypothetical second broker to
implement Lua scripting and string-key verbs it has no reason to have, and would
churn ``docs/interfaces/queue-stream/INTERFACE.md`` -- a published interface
document -- for a worker-internal feature. The precedent already exists twice:
``markers.py`` takes a concrete ``Redis``, and ``Consumer.__init__`` keeps its own
``self._valkey: Redis`` for the thread-reset drain "rather than widening
StreamBroker for one unrelated feature".

**2. One script serves both acquisition and transfer.** ``acquire`` refuses if
and only if a *live* lease exists; when the previous owner's lease has expired,
that same call **is** the transfer. A separate ``transfer`` in the reclaim loop
followed by an ``acquire`` in the handler would open a window in which authority
is held by a process that is not yet executing. One script closes it by
construction, and the reclaim paths degrade to a cheap ``EXISTS`` filter
(:meth:`DeliveryLeaseStore.is_live`) rather than a second authority point.

**3. The fencing generation outlives the lease.** There are TWO keys per
delivery, and the split is not incidental:

- ``{prefix}:lease:{stream}:{group}:{entry_id}`` -- a STRING holding the opaque
  owner token, expiring at ``delivery_lease_ttl_s``. Short-lived by design: its
  expiry is exactly how a dead owner's delivery becomes recoverable.
- ``{prefix}:delivery:{stream}:{group}:{entry_id}`` -- a HASH holding
  ``deadline_ms`` (absolute, from Valkey server ``TIME``, written
  **create-if-absent**) and ``gen`` (the fencing generation, ``HINCRBY``'d on
  every change of authority), retained for ``idempotency_ttl_s``.

If the generation lived in the lease key, expiry would erase it and the next
acquisition would restart at 1 -- monotonicity broken, and a stale owner holding
generation 1 would validate against a fresh generation 1. The long retention also
makes "the state key is absent" unambiguously mean *first delivery*: minting a
fresh deadline for a delivery that already burned its budget is precisely the
budget multiplication ``HSETNX`` on ``deadline_ms`` exists to prevent.

An explicit renewal refusal drops authority immediately. An unconfirmed renewal
may retain authority only until the last confirmed send's conservative local
deadline (ADR-0207); an ownership-store outage never extends that deadline.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass

from redis.asyncio import Redis

from .config import WorkerConfig

# The permissive budget handed to a consumer built with NO lease store (the
# base-only degradation in ``StreamConsumer._delivery_lease``). It is finite,
# positive, and far beyond the configurable maximum budget (1800s) purely so the
# leaseless path behaves exactly as it did before ADR-0131: every
# ``min(per_request_ceiling, remaining)`` resolves to the ceiling, and no budget
# check ever trips. It is never compared against a real deadline.
_UNFENCED_BUDGET_S = 86_400.0

# Acquire, and transfer, in one authority point. KEYS: lease, delivery state.
# ARGV: 1 owner token, 2 consumer, 3 lease TTL ms, 4 budget ms, 5 state
# retention ms, 6 stream, 7 group, 8 entry id.
#
# The PEL check is ENTRY-TARGETED (``XPENDING ... IDLE 0 <entry> <entry> 1``) and
# compares the consumer name the range form returns. The summary form
# (``IDLE 0 - + 1 <consumer>``) looks equivalent and is not: it pages the
# consumer's whole pending list and ``COUNT 1`` answers about its OLDEST entry,
# not the one being fenced. The runs lane holds up to ``max_concurrency`` (16 by default)
# entries per consumer, so that form would grant a lease whenever the consumer
# owned ANY pending entry -- silently defeating the fence it exists to enforce.
#
# ``EXISTS`` on the lease key is the single refusal condition, and it is both
# "refuse a concurrent acquisition" and "refuse a premature transfer": a
# replacement may take a delivery only once the previous owner's lease expired.
#
# ``HSETNX`` on the deadline is create-if-absent so a replacement inherits the
# SAME deadline. A plain ``HSET`` here multiplies the configured budget by the
# number of transfers -- three reclaims would turn 1,800 seconds into 5,400.
_ACQUIRE_LUA = """
local lease_key = KEYS[1]
local state_key = KEYS[2]
local owner = ARGV[1]
local consumer = ARGV[2]
local stream = ARGV[6]
local group = ARGV[7]
local entry = ARGV[8]

local pending = redis.call('XPENDING', stream, group, 'IDLE', 0, entry, entry, 1)
if #pending == 0 then return {0, 'not-pending'} end
if pending[1][2] ~= consumer then return {0, 'not-owner'} end

if redis.call('EXISTS', lease_key) == 1 then return {0, 'held'} end

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('HSETNX', state_key, 'deadline_ms', string.format('%d', now + tonumber(ARGV[4])))
local gen = redis.call('HINCRBY', state_key, 'gen', 1)
local deadline = redis.call('HGET', state_key, 'deadline_ms')
redis.call('SET', lease_key, owner, 'PX', tonumber(ARGV[3]))
redis.call('PEXPIRE', state_key, tonumber(ARGV[5]))
return {1, gen, deadline, string.format('%d', now)}
"""

# Separate stream entries for one approval resume have separate PEL rows and
# delivery leases. The event claim closes that gap without moving ordinary
# deliveries onto an event id lease. It records its owner's lease key and entry
# so a later claimant can distinguish a live winner from a stale claim. It is
# granted only to a live, fenced PEL owner and expires on the same clock as that
# owner's delivery lease.
_CLAIM_RESUME_LUA = """
local lease_key = KEYS[1]
local state_key = KEYS[2]
local resume_key = KEYS[3]
local owner = ARGV[1]
local stream = ARGV[4]
local group = ARGV[5]
local entry = ARGV[6]
local consumer = ARGV[7]

local pending = redis.call('XPENDING', stream, group, 'IDLE', 0, entry, entry, 1)
if #pending == 0 then return {0, 'not-pending'} end
if pending[1][2] ~= consumer then return {0, 'not-owner'} end
if redis.call('GET', lease_key) ~= owner then return {0, 'not-owner-token'} end
if redis.call('HGET', state_key, 'gen') ~= ARGV[2] then
  return {0, 'stale-generation'}
end
local winner_owner = redis.call('HGET', resume_key, 'owner')
if winner_owner then
  local winner_lease_key = redis.call('HGET', resume_key, 'lease_key')
  local winner_entry = redis.call('HGET', resume_key, 'entry_id')
  if winner_owner == owner and winner_lease_key == lease_key and winner_entry == entry then
    redis.call('PEXPIRE', resume_key, tonumber(ARGV[3]))
    return {1, 'claimed'}
  end
  if winner_lease_key and winner_entry and
     redis.call('GET', winner_lease_key) == winner_owner then
    local winner_pending = redis.call(
      'XPENDING', stream, group, 'IDLE', 0, winner_entry, winner_entry, 1
    )
    if #winner_pending ~= 0 then return {0, 'held'} end
  end
end
redis.call('HSET', resume_key, 'owner', owner, 'lease_key', lease_key, 'entry_id', entry)
redis.call('PEXPIRE', resume_key, tonumber(ARGV[3]))
return {1, 'claimed'}
"""

# Renew, with three independent fail-closed guards. KEYS: lease, delivery state,
# and the approval resume claim when this delivery holds one (empty otherwise).
# ARGV: 1 owner token, 2 expected generation, 3 lease TTL ms, 4 state retention
# ms, 5 stream, 6 group, 7 entry id, 8 consumer.
#
# All three guards run BEFORE anything is written, and each catches a distinct
# way a stale process believes it is still the owner:
#   - the PEL row moved to another consumer (an XCLAIM/XAUTOCLAIM took it);
#   - the lease key holds a different token (our lease expired and was retaken);
#   - the generation moved (the slow-heartbeat case: Valkey answers after our
#     lease already expired AND was retaken by a process that happens to be us).
#
# ``XCLAIM ... JUSTID`` is what keeps a healthy long turn un-reclaimable: it
# resets the same-owner PEL idle clock WITHOUT incrementing ``times_delivered``.
# Dropping ``JUSTID`` burns one delivery of the ADR-0039 budget on every
# heartbeat and dead-letters a perfectly healthy turn in under a minute.
#
# A fresh ``TIME`` is read and returned so the caller re-anchors its monotonic
# clock on every successful renewal: the deadline never moves, but the anchor
# does, which is what makes elapsed-time enforcement immune to a wall-clock step.
_HEARTBEAT_LUA = """
local lease_key = KEYS[1]
local state_key = KEYS[2]
local resume_key = KEYS[3]
local owner = ARGV[1]
local stream = ARGV[5]
local group = ARGV[6]
local entry = ARGV[7]
local consumer = ARGV[8]

local pending = redis.call('XPENDING', stream, group, 'IDLE', 0, entry, entry, 1)
if #pending == 0 then return {0, 'not-pending'} end
if pending[1][2] ~= consumer then return {0, 'not-owner'} end
if redis.call('GET', lease_key) ~= owner then return {0, 'not-owner-token'} end
if redis.call('HGET', state_key, 'gen') ~= ARGV[2] then return {0, 'stale-generation'} end
if resume_key ~= '' and redis.call('HGET', resume_key, 'owner') ~= owner then
  return {0, 'resume-claim-lost'}
end

redis.call('PEXPIRE', lease_key, tonumber(ARGV[3]))
redis.call('PEXPIRE', state_key, tonumber(ARGV[4]))
if resume_key ~= '' then redis.call('PEXPIRE', resume_key, tonumber(ARGV[3])) end
redis.call('XCLAIM', stream, group, consumer, 0, entry, 'JUSTID')

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
return {1, string.format('%d', now), redis.call('HGET', state_key, 'deadline_ms')}
"""

# Compare-and-delete both claims independently. Terminal settlement can remove
# the delivery lease before this call, but the resume claim still needs prompt
# release. A late owner cannot delete either claim after another owner takes it.
_RELEASE_LUA = """
local released = 0
if redis.call('GET', KEYS[1]) == ARGV[1] then
  released = redis.call('DEL', KEYS[1])
end
if KEYS[2] ~= '' and redis.call('HGET', KEYS[2], 'owner') == ARGV[1] then
  released = released + redis.call('DEL', KEYS[2])
end
return released
"""


class LeaseRefused(Exception):
    """Acquisition was refused. ``reason`` says which guard refused it.

    The distinction is operational, not cosmetic, and callers log the two
    differently:

    - ``held`` -- another owner's lease is live. Normal and expected under
      contention and on every reclaim scan that races a healthy turn; the
      correct response is to return without acking and leave the entry pending.
    - ``not-owner`` / ``not-pending`` -- the PEL and our belief have diverged:
      the entry is owned by another consumer, or is not pending at all. Neither
      is routine, and both mean this process was about to fence a delivery it
      was never given, so they are worth a WARNING.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(f"delivery lease refused: {reason}")
        self.reason = reason


class LeaseLostError(RuntimeError):
    """This owner's lease could not be confirmed, so it has no authority left.

    Raised by :meth:`DeliveryLease.raise_if_lost` at the settle boundaries. A
    process that sees this may not ACK, dead-letter, clear an outbox record, or
    emit a terminal result: whoever now holds the fence owns those.
    """


@dataclass(frozen=True)
class DeliveryBudget:
    """The delivery's one deadline, measured with a monotonic clock.

    ADR-0131: *"Within one process, elapsed-time enforcement uses a monotonic
    clock anchored to the last Valkey-time observation so wall-clock adjustment
    cannot extend the budget."*

    ``deadline_ms`` and ``anchor_server_ms`` are both Valkey **server** time, so
    their difference is the budget remaining as of the anchor. Everything since
    is measured with ``time.monotonic()``, which no NTP step or manual clock
    correction can move. ``time.time()`` must never appear in this module: an
    absolute in-process clock read against these server-time milliseconds would
    silently extend or destroy a live budget the moment the two disagree.
    """

    deadline_ms: int
    anchor_server_ms: int
    anchor_monotonic: float

    def remaining_s(self) -> float:
        """Seconds left, negative once the budget is spent (never wrapped)."""
        at_anchor_s = (self.deadline_ms - self.anchor_server_ms) / 1000.0
        return at_anchor_s - (time.monotonic() - self.anchor_monotonic)

    def reanchor(self, server_ms: int) -> DeliveryBudget:
        """The same deadline, re-based on a fresh server observation.

        Not on the production heartbeat path -- ``DeliveryLeaseStore.heartbeat``
        constructs a fresh ``DeliveryBudget`` directly. Retained as test-only API,
        exercised by
        ``test_remaining_s_is_driven_by_the_monotonic_anchor_not_the_wall_clock``
        to demonstrate the ADR-0131 invariant that remaining time is driven by
        the monotonic anchor rather than the wall clock. The deadline is
        deliberately NOT recomputed: re-deriving it from ``now + budget`` is how
        a renewal would turn into a budget extension.
        """
        return DeliveryBudget(
            deadline_ms=self.deadline_ms,
            anchor_server_ms=server_ms,
            anchor_monotonic=time.monotonic(),
        )


class DeliveryLease:
    """One process's authority over one delivery, for as long as it is renewed.

    It carries the delivery it is a lease ON -- the ``(stream, group, entry_id)``
    triple ADR-0131 keys a delivery by -- alongside the owner token and the
    fencing generation. That is what lets a holder name its OWN lease and state
    keys at the terminal settle: the fence is checked against keys derived from
    the triple the lease was granted for, never against keys rediscovered by a
    search. On the leaseless sentinel (:func:`unfenced_lease`) the triple is
    empty, exactly as ``owner`` is, and nothing fenced ever reads it.

    ``budget`` is replaced in place by the heartbeat loop -- a fresh
    ``DeliveryBudget`` built from the Lua script's returned deadline and server
    time, not a call to :meth:`DeliveryBudget.reanchor` -- so a holder always
    reads the freshest anchor. The separate local ownership deadline advances
    only on a confirmed renewal; ``lost`` is set on refusal or local expiry and
    never cleared, since authority is not recoverable once fenced out.
    """

    def __init__(
        self,
        *,
        stream: str,
        group: str,
        entry_id: str,
        owner: str,
        generation: int,
        budget: DeliveryBudget,
    ) -> None:
        self.stream = stream
        self.group = group
        self.entry_id = entry_id
        self.owner = owner
        self.generation = generation
        self.budget = budget
        # Acquire stamps the conservative ownership window before exposing a
        # real lease. A directly constructed lease has no confirmed authority.
        self.local_deadline_monotonic = budget.anchor_monotonic
        self.resume_event_id: str | None = None
        self.lost = asyncio.Event()
        self.entry_vanished = asyncio.Event()
        self.settlement_lock = asyncio.Lock()
        self.acknowledged = asyncio.Event()

    def remaining_s(self) -> float:
        return self.budget.remaining_s()

    def raise_if_lost(self) -> None:
        """Refuse to settle when the fence has already moved on.

        Called immediately before an ACK or any other terminal write. Fail
        closed: a lost lease is not a warning to log past, it is a hard stop.
        """
        if self.lost.is_set():
            raise LeaseLostError(
                f"delivery lease lost (owner={self.owner}, generation={self.generation}); "
                "this owner may not ack, dead-letter, or emit a terminal result"
            )


def unfenced_lease() -> DeliveryLease:
    """The permissive sentinel for a consumer built with NO lease store.

    This is the ONE place a missing lease is read as permission, and it exists
    only so a base-only ``StreamConsumer`` (the second-broker port, the
    ``_FakeBroker`` unit tests) behaves exactly as it did before ADR-0131.
    Nowhere else may absence be treated as authority.

    Its budget is finite and positive on purpose. A sentinel reading as
    exhausted would make every budget check skip every attempt -- a total stall
    reached from the other side of the same mistake.
    """
    lease = DeliveryLease(
        stream="",
        group="",
        entry_id="",
        owner="",
        generation=0,
        budget=DeliveryBudget(
            deadline_ms=int(_UNFENCED_BUDGET_S * 1000),
            anchor_server_ms=0,
            anchor_monotonic=time.monotonic(),
        ),
    )
    lease.local_deadline_monotonic += _UNFENCED_BUDGET_S
    return lease


class DeliveryLeaseStore:
    """Scripted delivery-ownership leases over Valkey.

    Mirrors ``Markers.__init__``: a concrete async client plus the worker config
    that owns the key namespace and every clock in it.
    """

    def __init__(self, redis: Redis, config: WorkerConfig) -> None:
        self._redis = redis
        self._config = config

    @property
    def heartbeat_interval_s(self) -> float:
        """How often a holder must renew. The config validator guarantees the
        lease spans at least three of these, so two consecutive missed renewals
        still leave a healthy owner's lease live."""
        return self._config.delivery_lease_heartbeat_s

    @property
    def ownership_window_s(self) -> float:
        """Confirmed-send authority, reserving one heartbeat before server expiry."""
        return self._config.delivery_lease_ttl_s - self.heartbeat_interval_s

    def _keys(self, stream: str, group: str, entry_id: str) -> tuple[str, str]:
        return (
            self._config.delivery_lease_key(stream, group, entry_id),
            self._config.delivery_state_key(stream, group, entry_id),
        )

    def _resume_key(self, stream: str, group: str, event_id: str) -> str:
        return f"{self._config.key_prefix}:resume-inflight:{stream}:{group}:{event_id}"

    async def claim_resume(
        self, lease: DeliveryLease, event_id: str, *, consumer: str
    ) -> bool:
        """Claim one approval resume across distinct stream entries.

        A held claim means another entry is pending under a live delivery lease.
        A refused delivery fence means the caller has lost authority, even if
        its heartbeat has not noticed yet.
        """
        # Heartbeat must not observe the claim id before the atomic acquire has
        # installed its key. If the reply is lost, release can still clear a
        # claim the server granted because the id is recorded before the await.
        async with lease.settlement_lock:
            lease.raise_if_lost()
            lease.resume_event_id = event_id
            lease_key, state_key = self._keys(lease.stream, lease.group, lease.entry_id)
            raw = await self._redis.eval(
                _CLAIM_RESUME_LUA,
                3,
                lease_key,
                state_key,
                self._resume_key(lease.stream, lease.group, event_id),
                lease.owner,
                str(lease.generation),
                str(int(self._config.delivery_lease_ttl_s * 1000)),
                lease.stream,
                lease.group,
                lease.entry_id,
                consumer,
            )
            if int(raw[0]) != 1:
                if str(raw[1]) == "held":
                    lease.resume_event_id = None
                    return False
                lease.lost.set()
                raise LeaseLostError(f"approval resume claim refused: {raw[1]}")
            return True

    async def acquire(
        self, stream: str, group: str, entry_id: str, *, consumer: str
    ) -> DeliveryLease:
        """Take authority over this delivery, or refuse.

        This is also the transfer path (see decision #2 in the module docstring):
        once the previous owner's lease has expired the very same call grants,
        inheriting the original deadline and incrementing the fencing generation.

        Raises :class:`LeaseRefused`; never returns a lease it did not win.
        """
        lease_key, state_key = self._keys(stream, group, entry_id)
        owner = uuid.uuid4().hex
        # Anchored BEFORE the round trip, so the round trip itself counts
        # against the budget rather than being silently gifted back.
        anchor_monotonic = time.monotonic()
        raw = await self._redis.eval(
            _ACQUIRE_LUA,
            2,
            lease_key,
            state_key,
            owner,
            consumer,
            str(int(self._config.delivery_lease_ttl_s * 1000)),
            str(int(self._config.delivery_budget_s * 1000)),
            str(int(self._config.idempotency_ttl_s * 1000)),
            stream,
            group,
            entry_id,
        )
        if int(raw[0]) != 1:
            raise LeaseRefused(str(raw[1]))
        lease = DeliveryLease(
            stream=stream,
            group=group,
            entry_id=entry_id,
            owner=owner,
            generation=int(raw[1]),
            budget=DeliveryBudget(
                deadline_ms=int(raw[2]),
                anchor_server_ms=int(raw[3]),
                anchor_monotonic=anchor_monotonic,
            ),
        )
        lease.local_deadline_monotonic = anchor_monotonic + self.ownership_window_s
        return lease

    async def heartbeat(
        self,
        stream: str,
        group: str,
        entry_id: str,
        *,
        consumer: str,
        owner: str,
        generation: int,
        resume_event_id: str | None,
    ) -> DeliveryBudget | None:
        """Renew the lease and reset same-owner PEL idle, or refuse.

        Returns the re-anchored budget on success and ``None`` on ANY refusal --
        the caller treats ``None`` as immediate lease loss. A raising call is
        unconfirmed, rather than refused, and the caller retains only its prior
        local ownership deadline (ADR-0207).
        """
        lease_key, state_key = self._keys(stream, group, entry_id)
        resume_key = (
            self._resume_key(stream, group, resume_event_id) if resume_event_id else ""
        )
        anchor_monotonic = time.monotonic()
        raw = await self._redis.eval(
            _HEARTBEAT_LUA,
            3,
            lease_key,
            state_key,
            resume_key,
            owner,
            str(generation),
            str(int(self._config.delivery_lease_ttl_s * 1000)),
            str(int(self._config.idempotency_ttl_s * 1000)),
            stream,
            group,
            entry_id,
            consumer,
        )
        if int(raw[0]) != 1:
            return None
        return DeliveryBudget(
            deadline_ms=int(raw[2]),
            anchor_server_ms=int(raw[1]),
            anchor_monotonic=anchor_monotonic,
        )

    async def entry_vanished(
        self,
        stream: str,
        group: str,
        entry_id: str,
        *,
        owner: str,
    ) -> bool | None:
        """Whether this exact delivery is gone and no other owner holds it.

        True only when ``XRANGE`` of ``entry_id`` is empty, ``XPENDING`` of
        that same id is empty, and the lease key is missing or still ``owner``.
        False when the entry exists, the PEL row exists, or the lease token is
        some other owner. None when any of those reads raises.
        """
        try:
            rows = await self._redis.xrange(stream, min=entry_id, max=entry_id, count=1)
            if rows:
                return False
            pending = await self._redis.xpending_range(
                stream,
                group,
                min=entry_id,
                max=entry_id,
                count=1,
            )
            if pending:
                return False
            lease_key, _state_key = self._keys(stream, group, entry_id)
            token = await self._redis.get(lease_key)
            if token is None:
                return True
            if isinstance(token, bytes):
                token = token.decode()
            return token == owner
        except Exception:  # noqa: BLE001 - an unreadable probe is not a vanished entry
            return None

    async def release(
        self,
        stream: str,
        group: str,
        entry_id: str,
        *,
        owner: str,
        resume_event_id: str | None,
    ) -> bool:
        """Give up the lease, but only if it is still ours (compare-and-delete).

        The delivery STATE survives a release. Its absence must keep meaning
        "first delivery": a released-then-reacquired entry that minted a fresh
        deadline would multiply the budget exactly as a reverted ``HSETNX`` does.
        Only :meth:`settle` removes it.
        """
        lease_key, _state_key = self._keys(stream, group, entry_id)
        resume_key = (
            self._resume_key(stream, group, resume_event_id) if resume_event_id else ""
        )
        deleted = await self._redis.eval(_RELEASE_LUA, 2, lease_key, resume_key, owner)
        return int(deleted) > 0

    async def settle(self, stream: str, group: str, entry_id: str) -> None:
        """Terminal cleanup: remove the lease AND the delivery state.

        ADR-0131: internal delivery state is *"removed after terminal
        acknowledgement or dead-letter settlement"*. Called only from those two
        places -- the long retention on the state key is the backstop for a crash
        between the ACK and this call, not the normal way it goes away.
        """
        lease_key, state_key = self._keys(stream, group, entry_id)
        await self._redis.delete(lease_key, state_key)

    async def is_live(self, stream: str, group: str, entry_id: str) -> bool:
        """Is some owner currently holding this delivery?

        A bare ``EXISTS``: the cheap read every reclaim path makes before it
        cap-evaluates or dispatches. It deliberately does not say WHO holds the
        lease -- the reclaim paths only need to know that somebody does.
        """
        lease_key, _state_key = self._keys(stream, group, entry_id)
        return bool(await self._redis.exists(lease_key))

    async def has_state(self, stream: str, group: str, entry_id: str) -> bool:
        """Was a lease ever granted for this delivery?

        A bare ``EXISTS`` on the state key, the cheap counterpart of
        :meth:`is_live`'s ``EXISTS`` on the lease key. It exists rather than a
        truthiness test on :meth:`peek` because that would drag the whole hash
        back for a question that is one bit.

        What it is FOR: positive evidence. It separates "a lease-aware owner died
        or failed here", which is reclaimable once its lease is gone, from "a
        pre-lease or pre-marker consumer's entry", which has no evidence at all
        and stays on the unchanged XAUTOCLAIM backstop (the mixed-version rule in
        ``apps/worker/CLAUDE.md``). Absence is authoritative for the second case
        because only :meth:`settle` removes the state: a ``release`` leaves it, so
        a failed handler's entry still carries it long after its lease is gone.
        """
        _lease_key, state_key = self._keys(stream, group, entry_id)
        return bool(await self._redis.exists(state_key))

    async def peek(self, stream: str, group: str, entry_id: str) -> dict[str, str]:
        """The delivery state hash for diagnostics, ``{}`` when absent."""
        _lease_key, state_key = self._keys(stream, group, entry_id)
        state = await self._redis.hgetall(state_key)
        return {str(k): str(v) for k, v in (state or {}).items()}
