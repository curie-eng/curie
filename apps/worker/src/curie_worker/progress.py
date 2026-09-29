"""The worker coordinator's durable progress state and delivery outbox (ADR 0130).

One logical turn chain owns one progress record: its current task state, its
milestone budget, and the card and milestone deliveries it owes its channel.
The rules are specified in the worker README's "Deliberate progress (ADR 0130)"
section; this module is where they run. A model's command reaches it through
the API's ingress, the chain's inbox and the kernel's per-turn pump
(``curie_worker.turn_progress``); nothing delivers from the outbox yet, and the
maintenance tick sweeps it without a deliverer.

Three structural choices carry the ADR's guarantees:

- **Every rule is one Lua script.** Idempotency, the ``(epoch, seq)`` order,
  terminal monotonicity, the update cap, the milestone reservation and the
  enqueue of the delivery an accepted change owes all run in the script that
  writes the change, so no interleaving of replicas can pass a check and then
  write after another replica moved the record. A process-local counter would
  rearm after a restart, which ADR 0130 calls non-conforming.
- **Delivery ids are derived, never minted.** Each is a UUIDv5 of the record's
  ``progress_id`` and its slot, so a retry, a restart or a redelivered turn
  names the same externally visible operation again, and an adapter can
  deduplicate an ambiguous attempt by it.
- **The platform's write is fenced; the model's is ordered.** A platform update,
  terminal included, verifies the caller's ADR-0131 delivery lease with the two
  checks ``markers._SETTLE_FENCED_LUA`` makes, in its own script. A model
  command comes through an ingress that serves the running turn rather than a
  stream delivery, so its guard is the ``(epoch, seq)`` order instead.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, Any, Final, Literal, cast, get_args

from channel_protocol.progress import (
    MAX_PROGRESS_MILESTONES,
    TERMINAL_PROGRESS_STATES,
    MilestoneClass,
    ProgressCard,
    ProgressCommand,
    ProgressMilestone,
    ProgressState,
    ProgressSummary,
    UpdateId,
)
from channel_protocol.reply import DeliveryId, ReplyTarget
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError
from redis.asyncio import Redis

from .approval_cards import DEFAULT_CARD_TTL_S
from .config import WorkerConfig
from .delivery_lease import DeliveryLease
from .reply_sink import TargetRoute

logger = logging.getLogger(__name__)

# Pinned: every live record's id is derived from it, so a changed namespace
# would orphan every chain in flight and hand each a fresh milestone budget.
PROGRESS_ID_NAMESPACE: Final = uuid.UUID("d22277ad-b64c-43b9-a404-8141dda9859b")

MAX_PROGRESS_UPDATES: Final = 50

MODEL_PROGRESS_STATES: Final[frozenset[ProgressState]] = frozenset(
    {
        ProgressState.INVESTIGATING,
        ProgressState.PREPARING_WORKSPACE,
        ProgressState.TESTING,
        ProgressState.PUBLISHING,
    }
)

# One sweep pass is bounded in members, in wall time, and per delivery by the
# time left; the completion sweeper's defaults, for the same reason it gives.
# Fixed rather than configurable: nothing delivers progress yet, so there is no
# operating experience to tune a boot-env knob against.
PROGRESS_SWEEP_BATCH: Final = 64
PROGRESS_SWEEP_BUDGET_S: Final = 30.0
PROGRESS_SWEEP_GRACE_S: Final = 60.0
PROGRESS_MAX_ATTEMPTS: Final = 5

# The fencing generation field of the ADR-0131 delivery state hash, which
# ``delivery_lease.py`` HINCRBYs on every change of authority. The fenced-write
# tests' positive control fails if this ever stops matching it.
_DELIVERY_GENERATION_FIELD: Final = "gen"

# Model and platform update ids live in separate hash-field namespaces, so a
# model cannot pre-empt a platform write by submitting its id first.
_MODEL_UPDATE_FIELD: Final = "m:"
_PLATFORM_UPDATE_FIELD: Final = "p:"

# Each retry of an apply means another change moved the revision or the
# milestone count between the read and the script. Both are bounded (a
# revision per accepted update plus the terminal one, three reservations), so
# this many rounds cannot all be lost to real contention.
_MAX_APPLY_ROUNDS: Final = MAX_PROGRESS_UPDATES + MAX_PROGRESS_MILESTONES + 2

# epoch and seq travel through Lua, whose numbers are doubles.
_MAX_POSITION: Final = 2**53 - 1

ProgressRefusal = Literal[
    "no-chain",
    "platform-only-state",
    "terminal",
    "stale-epoch",
    "stale-seq",
    "update-cap",
    "lease-lost",
]

_REFUSALS: Final[frozenset[str]] = frozenset(get_args(ProgressRefusal))
_UPDATE_ID: TypeAdapter[str] = TypeAdapter(UpdateId)
_SUMMARY: TypeAdapter[str] = TypeAdapter(ProgressSummary)

_OPEN_LUA = """
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('HSET', KEYS[1],
  'state', '', 'summary', '', 'revision', '0', 'epoch', '0', 'last_seq', '0',
  'milestones_used', '0', 'card_ref', '', 'answer_ref', ARGV[2], 'terminal', '0',
  'inbox_cursor', '', 'update_count', '0')
redis.call('EXPIRE', KEYS[1], ARGV[1])
return 1
"""

# Link a resume event to a LIVE chain, first link wins. Refusing a missing
# record is what keeps an expired chain from being continued under a pointer.
_LINK_LUA = """
if redis.call('EXISTS', KEYS[2]) == 0 then return 0 end
local linked = redis.call('GET', KEYS[1])
if linked and linked ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
return 1
"""

# The platform prelude: the ADR-0131 fence, before anything is read or
# written. KEYS[5] is the lease key and KEYS[6] the delivery state hash, both
# derived from the caller's own lease triple; ARGV[24..26] are its owner token,
# the generation field name and its generation.
_PLATFORM_PRELUDE = """
if redis.call('GET', KEYS[5]) ~= ARGV[24] then return {'refused', 'lease-lost'} end
if redis.call('HGET', KEYS[6], ARGV[25]) ~= ARGV[26] then return {'refused', 'lease-lost'} end
local platform = true
"""

_MODEL_PRELUDE = """
local platform = false
"""

# One accepted update, and the deliveries it owes, in one atomic step.
#
# KEYS: 1 the record, 2 the pending set, 3 the card delivery, 4 the milestone
# delivery ('' when none was requested).
# ARGV: 1 ttl s, 2 update id field, 3 epoch, 4 seq (0 for platform), 5 state,
# 6 summary, 7 terminal, 8 max updates, 9 max milestones, 10 revision read,
# 11 milestones read, 12 milestone requested, 13-16 card id / event / slot /
# generation, 17-20 the same for the milestone, 21 route, 22 created_at,
# 23 progress id.
#
# The checks run in the README's order and the first to fail answers. The two
# "moved" answers are the compare-and-set: the card payload and the delivery
# ids were derived by the caller from the counts it read, so a script that
# finds either count moved writes nothing and the caller retries.
_APPLY_BODY = """
local record = KEYS[1]
local ttl = ARGV[1]
local epoch = tonumber(ARGV[3])
local terminal = ARGV[7] == '1'

if redis.call('EXISTS', record) == 0 then return {'refused', 'no-chain'} end
if redis.call('HEXISTS', record, ARGV[2]) == 1 then
  return {'duplicate', redis.call('HGET', record, 'revision')}
end
if redis.call('HGET', record, 'terminal') == '1' then return {'refused', 'terminal'} end

local current_epoch = tonumber(redis.call('HGET', record, 'epoch'))
local last_seq = redis.call('HGET', record, 'last_seq')
if epoch < current_epoch then return {'refused', 'stale-epoch'} end
local next_seq = ARGV[4]
if platform then
  if epoch == current_epoch then next_seq = last_seq else next_seq = '0' end
elseif epoch == current_epoch and tonumber(ARGV[4]) <= tonumber(last_seq) then
  return {'refused', 'stale-seq'}
end

local count = tonumber(redis.call('HGET', record, 'update_count'))
if not terminal and count >= tonumber(ARGV[8]) then return {'refused', 'update-cap'} end

local revision = tonumber(redis.call('HGET', record, 'revision'))
local changed = redis.call('HGET', record, 'state') ~= ARGV[5]
  or redis.call('HGET', record, 'summary') ~= ARGV[6]
local used = tonumber(redis.call('HGET', record, 'milestones_used'))
local reserve = ARGV[12] == '1' and used < tonumber(ARGV[9])
if changed and revision ~= tonumber(ARGV[10]) then return {'moved'} end
if reserve and used ~= tonumber(ARGV[11]) then return {'moved'} end

if changed then revision = revision + 1 end
local fields = {'epoch', ARGV[3], 'last_seq', next_seq,
  'update_count', tostring(count + 1), ARGV[2], tostring(revision)}
if changed then
  for _, value in ipairs({'state', ARGV[5], 'summary', ARGV[6],
      'revision', tostring(revision)}) do
    table.insert(fields, value)
  end
  if terminal then
    table.insert(fields, 'terminal')
    table.insert(fields, '1')
  end
end
local ordinal = 0
if reserve then
  ordinal = used + 1
  table.insert(fields, 'milestones_used')
  table.insert(fields, tostring(ordinal))
end
redis.call('HSET', record, unpack(fields))
redis.call('EXPIRE', record, ttl)

local function enqueue(key, id, event, slot, generation)
  redis.call('DEL', key)
  redis.call('HSET', key, 'event', event, 'route', ARGV[21], 'attempts', '0',
    'gen', generation, 'pid', ARGV[23], 'slot', slot, 'created_at', ARGV[22])
  redis.call('EXPIRE', key, ttl)
  redis.call('SADD', KEYS[2], id)
end
if changed then enqueue(KEYS[3], ARGV[13], ARGV[14], ARGV[15], ARGV[16]) end
if reserve then enqueue(KEYS[4], ARGV[17], ARGV[18], ARGV[19], ARGV[20]) end
if changed or reserve then redis.call('EXPIRE', KEYS[2], ttl) end

local refused_milestone = '0'
if ARGV[12] == '1' and not reserve then refused_milestone = '1' end
local enqueued_card = '0'
if changed then enqueued_card = '1' end
return {'applied', tostring(revision), tostring(ordinal), refused_milestone, enqueued_card}
"""

_APPLY_MODEL_LUA = _MODEL_PRELUDE + _APPLY_BODY
_APPLY_PLATFORM_LUA = _PLATFORM_PRELUDE + _APPLY_BODY

# Clear one delivery, only in the generation the caller read. On the card's
# first post it also adopts the adapter's ref, in the same step, so a crash
# cannot leave the post acknowledged with the ref lost. An existing ref is never
# replaced, and an expired record is never recreated by the adoption.
_ACK_LUA = """
if redis.call('HGET', KEYS[1], 'gen') ~= ARGV[1] then return 0 end
if ARGV[3] ~= '' and KEYS[3] ~= '' and redis.call('HGET', KEYS[1], 'slot') == 'card'
   and redis.call('HGET', KEYS[3], 'card_ref') == '' then
  redis.call('HSET', KEYS[3], 'card_ref', ARGV[3])
end
redis.call('DEL', KEYS[1])
redis.call('SREM', KEYS[2], ARGV[2])
return 1
"""

# Move the record's inbox cursor forward to a stream id, never back, and never
# onto an expired record. Stream ids compare as (milliseconds, sequence).
_ADVANCE_CURSOR_LUA = """
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
local nm, ns = string.match(ARGV[1], '^(%d+)-(%d+)$')
if not nm then return 0 end
local current = redis.call('HGET', KEYS[1], 'inbox_cursor')
if current and current ~= '' then
  local cm, cs = string.match(current, '^(%d+)-(%d+)$')
  if cm then
    nm, ns, cm, cs = tonumber(nm), tonumber(ns), tonumber(cm), tonumber(cs)
    if nm < cm or (nm == cm and ns <= cs) then return 0 end
  end
end
redis.call('HSET', KEYS[1], 'inbox_cursor', ARGV[1])
return 1
"""

# Charge one attempt BEFORE the delivery is tried, so a crash mid-attempt still
# counts against the budget. -1 when the generation moved or the record is gone.
_CHARGE_LUA = """
if redis.call('HGET', KEYS[1], 'gen') ~= ARGV[1] then return -1 end
return redis.call('HINCRBY', KEYS[1], 'attempts', 1)
"""

# ``markers._DEAD_LETTER_COMPLETION_LUA``'s shape: retain the row in the bounded
# graveyard BEFORE the owed record disappears, all under the generation fence,
# so a stale pass can neither accuse nor clear a record written after it.
_DEAD_LETTER_LUA = """
if redis.call('HGET', KEYS[1], 'gen') ~= ARGV[1] then return 0 end
local stored = redis.call('HMGET', KEYS[1], 'pid', 'event', 'attempts')
redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[3], '*',
  'delivery_id', ARGV[2], 'progress_id', stored[1] or '', 'event', stored[2] or '',
  'dl_reason', ARGV[4], 'dl_delivery_count', stored[3] or '0',
  'dl_source', 'progress-outbox', 'dl_dead_lettered_at', ARGV[5])
redis.call('DEL', KEYS[1])
redis.call('SREM', KEYS[2], ARGV[2])
return 1
"""


def progress_id_for(thread_key: str, root_event_id: str) -> str:
    """The record id of the chain a turn's root event starts."""
    return str(uuid.uuid5(PROGRESS_ID_NAMESPACE, thread_key + "\0" + root_event_id))


def card_delivery_id(progress_id: str) -> str:
    """The card's first post."""
    return str(uuid.uuid5(uuid.UUID(progress_id), "card"))


def card_update_delivery_id(progress_id: str, revision: int) -> str:
    """The card edit that carries ``revision``."""
    return str(uuid.uuid5(uuid.UUID(progress_id), f"card:{revision}"))


def milestone_delivery_id(progress_id: str, ordinal: int) -> str:
    """The milestone post holding slot ``ordinal``."""
    return str(uuid.uuid5(uuid.UUID(progress_id), f"milestone:{ordinal}"))


def progress_ttl_s(config: WorkerConfig) -> int:
    """Every progress key's lifetime: at least an approval card's, which a chain
    spans when it suspends for approval."""
    return max(int(config.completion_max_retention_s), DEFAULT_CARD_TTL_S)


class ProgressDelivery(BaseModel):
    """One owed delivery, self-contained like ``markers.CompletionRecord``.

    The payload is the semantic one; building the reply-wire body from it is
    the deliverer's. An edit is addressed to the record's ``card_ref``, which
    only exists once the first post is acknowledged, so it is not stored here.
    """

    model_config = ConfigDict(frozen=True)

    delivery_id: DeliveryId
    progress_id: str
    operation: Literal["post", "update"]
    progress: Annotated[ProgressCard | ProgressMilestone, Field(discriminator="kind")]
    target: ReplyTarget


@dataclass(frozen=True)
class StoredProgressDelivery:
    """A pending delivery as stored, with the fields its hash keeps beside it."""

    delivery: ProgressDelivery
    route: TargetRoute
    attempts: int
    generation: str
    created_at: float
    slot: str


@dataclass(frozen=True)
class ProgressRecord:
    """One chain's record as stored. ``state`` is None until the first update."""

    progress_id: str
    state: ProgressState | None
    summary: str
    revision: int
    epoch: int
    last_seq: int
    milestones_used: int
    card_ref: str | None
    answer_ref: str | None
    terminal: bool
    inbox_cursor: str
    update_count: int


@dataclass(frozen=True)
class ProgressOutcome:
    """What one update did. A refusal and a duplicate wrote nothing."""

    status: Literal["applied", "duplicate", "refused"]
    reason: ProgressRefusal | None = None
    revision: int = 0
    milestone_ordinal: int | None = None
    milestone_refused: bool = False
    deliveries: tuple[str, ...] = ()


class MalformedProgressError(RuntimeError):
    """A stored progress hash lacks or garbles a field every writer sets.

    Every key here is written by this module's scripts, so there is no older
    shape to be lenient about: the caller quarantines rather than guessing.
    """


class ProgressStore:
    """The durable progress record, its chain pointers, and its outbox."""

    def __init__(self, redis: Redis, config: WorkerConfig) -> None:
        self._redis = redis
        self._config = config

    @property
    def dead_letter_stream(self) -> str:
        """The graveyard a spent delivery is retained in."""
        return self._config.dead_letter_stream_name()

    # -- chains ---------------------------------------------------------------

    async def open_chain(
        self, thread_key: str, root_event_id: str, *, answer_ref: str | None = None
    ) -> str:
        """Create the record a root event derives, if absent, and return its id."""
        progress_id = progress_id_for(thread_key, root_event_id)
        await self._redis.eval(
            _OPEN_LUA,
            1,
            self._config.progress_key(progress_id),
            str(progress_ttl_s(self._config)),
            answer_ref or "",
        )
        return progress_id

    async def chain_for_turn(
        self,
        thread_key: str,
        event_id: str,
        *,
        resume: bool,
        answer_ref: str | None = None,
    ) -> str | None:
        """The record a turn updates, or None when it must render nothing.

        A resume only ever follows its pointer: deriving a record from its own
        event id would hand it a fresh milestone budget (ADR 0130 section 2).
        """
        if resume:
            return await self.resolve_chain(event_id)
        return await self.open_chain(thread_key, event_id, answer_ref=answer_ref)

    async def link_resume(self, resume_event_id: str, progress_id: str) -> bool:
        """Point a resume event at a live chain. False when the chain is gone or
        the resume is already linked to another."""
        linked = await self._redis.eval(
            _LINK_LUA,
            2,
            self._config.progress_chain_key(resume_event_id),
            self._config.progress_key(progress_id),
            progress_id,
            str(progress_ttl_s(self._config)),
        )
        return int(linked) == 1

    async def resolve_chain(self, event_id: str) -> str | None:
        """The live chain a resume event continues, or None."""
        progress_id = _as_str(await self._redis.get(self._config.progress_chain_key(event_id)))
        if not progress_id:
            return None
        if not await self._redis.exists(self._config.progress_key(progress_id)):
            return None
        return progress_id

    async def read(self, progress_id: str) -> ProgressRecord | None:
        stored: dict[Any, Any] = await self._redis.hgetall(self._config.progress_key(progress_id))
        if not stored:
            return None
        fields = {str(_as_str(k)): str(_as_str(v)) for k, v in stored.items()}
        try:
            state = fields["state"]
            return ProgressRecord(
                progress_id=progress_id,
                state=ProgressState(state) if state else None,
                summary=fields["summary"],
                revision=int(fields["revision"]),
                epoch=int(fields["epoch"]),
                last_seq=int(fields["last_seq"]),
                milestones_used=int(fields["milestones_used"]),
                card_ref=fields["card_ref"] or None,
                answer_ref=fields["answer_ref"] or None,
                terminal=fields["terminal"] == "1",
                inbox_cursor=fields["inbox_cursor"],
                update_count=int(fields["update_count"]),
            )
        except (KeyError, ValueError) as exc:
            raise MalformedProgressError(f"progress record {progress_id}: {exc!r}") from exc

    # -- updates --------------------------------------------------------------

    async def apply_model_command(
        self,
        progress_id: str,
        command: ProgressCommand,
        *,
        epoch: int,
        seq: int,
        route: TargetRoute,
        target: ReplyTarget,
    ) -> ProgressOutcome:
        """Apply one model command at its ingress position ``(epoch, seq)``."""
        if command.state not in MODEL_PROGRESS_STATES:
            return ProgressOutcome(status="refused", reason="platform-only-state")
        _require_position("epoch", epoch)
        _require_position("seq", seq)
        return await self._apply(
            _APPLY_MODEL_LUA,
            progress_id,
            update_field=_MODEL_UPDATE_FIELD + command.update_id,
            epoch=epoch,
            seq=seq,
            state=command.state,
            summary=command.summary,
            milestone=command.milestone,
            route=route,
            target=target,
            fence_keys=(),
            fence_args=(),
        )

    async def apply_platform_update(
        self,
        progress_id: str,
        *,
        update_id: str,
        state: ProgressState,
        summary: str,
        epoch: int,
        route: TargetRoute,
        target: ReplyTarget,
        lease: DeliveryLease,
    ) -> ProgressOutcome:
        """Apply a platform update, terminal included, only under a held lease.

        The fence keys are derived from the triple the lease was granted for, as
        ``Markers.settle_fenced`` derives them.
        """
        _require_position("epoch", epoch)
        return await self._apply(
            _APPLY_PLATFORM_LUA,
            progress_id,
            update_field=_PLATFORM_UPDATE_FIELD + _UPDATE_ID.validate_python(update_id),
            epoch=epoch,
            seq=0,
            state=state,
            summary=_SUMMARY.validate_python(summary),
            milestone=None,
            route=route,
            target=target,
            fence_keys=(
                self._config.delivery_lease_key(lease.stream, lease.group, lease.entry_id),
                self._config.delivery_state_key(lease.stream, lease.group, lease.entry_id),
            ),
            fence_args=(lease.owner, _DELIVERY_GENERATION_FIELD, str(lease.generation)),
        )

    async def _read_counts(self, progress_id: str) -> tuple[int, int]:
        """The revision and milestone count the next payloads are derived from."""
        revision, used = await self._redis.hmget(
            self._config.progress_key(progress_id), ["revision", "milestones_used"]
        )
        return int(_as_str(revision) or 0), int(_as_str(used) or 0)

    async def _apply(
        self,
        script: str,
        progress_id: str,
        *,
        update_field: str,
        epoch: int,
        seq: int,
        state: ProgressState,
        summary: str,
        milestone: MilestoneClass | None,
        route: TargetRoute,
        target: ReplyTarget,
        fence_keys: tuple[str, ...],
        fence_args: tuple[str, ...],
    ) -> ProgressOutcome:
        ttl_s = str(progress_ttl_s(self._config))
        terminal = state in TERMINAL_PROGRESS_STATES
        for _round in range(_MAX_APPLY_ROUNDS):
            revision, used = await self._read_counts(progress_id)
            card_revision = revision + 1
            if card_revision == 1:
                card_id, card_slot = card_delivery_id(progress_id), "card"
            else:
                card_id = card_update_delivery_id(progress_id, card_revision)
                card_slot = f"card:{card_revision}"
            card = ProgressDelivery(
                delivery_id=card_id,
                progress_id=progress_id,
                operation="post" if card_revision == 1 else "update",
                progress=ProgressCard(
                    kind="card",
                    state=state,
                    summary=summary,
                    revision=card_revision,
                    terminal=terminal,
                ),
                target=target,
            )
            milestone_key = ""
            milestone_args = ("", "", "", "")
            if milestone is not None and used < MAX_PROGRESS_MILESTONES:
                ordinal = used + 1
                milestone_id = milestone_delivery_id(progress_id, ordinal)
                milestone_key = self._config.progress_delivery_key(milestone_id)
                reserved = ProgressDelivery(
                    delivery_id=milestone_id,
                    progress_id=progress_id,
                    operation="post",
                    progress=ProgressMilestone(
                        kind="milestone", milestone=milestone, summary=summary, ordinal=ordinal
                    ),
                    target=target,
                )
                milestone_args = (
                    milestone_id,
                    reserved.model_dump_json(),
                    f"milestone:{ordinal}",
                    uuid.uuid4().hex,
                )
            raw = await self._redis.eval(
                script,
                4 + len(fence_keys),
                self._config.progress_key(progress_id),
                self._config.progress_pending_key(),
                self._config.progress_delivery_key(card_id),
                milestone_key,
                *fence_keys,
                ttl_s,
                update_field,
                str(epoch),
                str(seq),
                state.value,
                summary,
                "1" if terminal else "0",
                str(MAX_PROGRESS_UPDATES),
                str(MAX_PROGRESS_MILESTONES),
                str(revision),
                str(used),
                "1" if milestone is not None else "0",
                card_id,
                card.model_dump_json(),
                card_slot,
                uuid.uuid4().hex,
                *milestone_args,
                route.model_dump_json(),
                str(time.time()),
                progress_id,
                *fence_args,
            )
            answer = [str(_as_str(part)) for part in raw]
            if answer[0] == "moved":
                continue
            if answer[0] == "refused":
                return ProgressOutcome(status="refused", reason=_refusal(answer[1]))
            if answer[0] == "duplicate":
                return ProgressOutcome(status="duplicate", revision=int(answer[1]))
            ordinal_reserved = int(answer[2])
            deliveries = [card_id] if answer[4] == "1" else []
            if ordinal_reserved:
                deliveries.append(milestone_args[0])
            return ProgressOutcome(
                status="applied",
                revision=int(answer[1]),
                milestone_ordinal=ordinal_reserved or None,
                milestone_refused=answer[3] == "1",
                deliveries=tuple(deliveries),
            )
        raise RuntimeError(
            f"progress record {progress_id} moved on each of {_MAX_APPLY_ROUNDS} reads"
        )

    # -- the inbox ------------------------------------------------------------

    async def read_inbox(
        self, progress_id: str, *, after: str, count: int
    ) -> list[tuple[str, dict[str, str]]]:
        """Up to ``count`` inbox entries after the stream id ``after`` ('' for all)."""
        entries: Any = await self._redis.xrange(
            self._config.progress_inbox_key(progress_id),
            min=f"({after}" if after else "-",
            max="+",
            count=count,
        )
        return [
            (
                str(_as_str(entry_id)),
                {str(_as_str(key)): str(_as_str(value)) for key, value in fields.items()},
            )
            for entry_id, fields in entries
        ]

    async def advance_cursor(self, progress_id: str, entry_id: str) -> bool:
        """Record that the chain's inbox is applied through ``entry_id``.

        Only forward, and never onto an expired record. Returns whether it moved.
        """
        moved = await self._redis.eval(
            _ADVANCE_CURSOR_LUA, 1, self._config.progress_key(progress_id), entry_id
        )
        return int(moved) == 1

    # -- the outbox -----------------------------------------------------------

    async def discard_deliveries(self, delivery_ids: Sequence[str]) -> None:
        """Remove owed deliveries that nothing will make.

        For the pump while rendering is off: the record keeps its state and
        reservations, and no delivery is left for a later deliverer to replay.
        """
        if not delivery_ids:
            return
        async with self._redis.pipeline(transaction=True) as pipe:
            for delivery_id in delivery_ids:
                pipe.delete(self._config.progress_delivery_key(delivery_id))
            pipe.srem(self._config.progress_pending_key(), *delivery_ids)
            await pipe.execute()

    async def ack(
        self, delivery_id: str, *, generation: str, card_ref: str | None = None
    ) -> bool:
        """Clear a delivery the adapter answered, only in the generation read.

        ``card_ref`` is the adapter's ref for a post; on the card's first post
        it becomes the record's ``card_ref``. Returns whether anything cleared.
        """
        delivery_key = self._config.progress_delivery_key(delivery_id)
        record_key = ""
        if card_ref:
            progress_id = _as_str(await self._redis.hget(delivery_key, "pid"))
            if progress_id:
                record_key = self._config.progress_key(progress_id)
        cleared = await self._redis.eval(
            _ACK_LUA,
            3,
            delivery_key,
            self._config.progress_pending_key(),
            record_key,
            generation,
            delivery_id,
            card_ref or "",
        )
        return int(cleared) == 1

    async def charge_attempt(self, delivery_id: str, *, generation: str) -> int | None:
        """Count one attempt against the delivery's budget; the new count, or
        None when the delivery is gone or was rewritten since it was read."""
        charged = int(
            await self._redis.eval(
                _CHARGE_LUA, 1, self._config.progress_delivery_key(delivery_id), generation
            )
        )
        return None if charged < 0 else charged

    async def dead_letter(self, delivery_id: str, *, generation: str, reason: str) -> bool:
        """Retain the delivery in the graveyard and clear it, in its generation."""
        return bool(
            await self._redis.eval(
                _DEAD_LETTER_LUA,
                3,
                self._config.progress_delivery_key(delivery_id),
                self._config.progress_pending_key(),
                self.dead_letter_stream,
                generation,
                delivery_id,
                self._config.dead_letter_maxlen,
                reason,
                datetime.now(UTC).isoformat(),
            )
        )

    async def drop_pending_member(self, delivery_id: str) -> None:
        """Remove only the index membership, leaving any stored payload.

        For a member whose delivery expired or was cleared, and the quarantine
        of a malformed one, whose payload stays for an operator until it
        expires, as ``Markers.drop_pending_member`` does for the completion
        outbox.
        """
        await self._redis.srem(self._config.progress_pending_key(), delivery_id)

    async def pending(self, limit: int) -> set[str]:
        """A bounded random sample of pending delivery ids, never the whole set."""
        members: Any = await self._redis.srandmember(self._config.progress_pending_key(), limit)
        if not members:
            return set()
        if not isinstance(members, list):
            members = [members]
        return {str(_as_str(member)) for member in members}

    async def read_delivery(self, delivery_id: str) -> StoredProgressDelivery | None:
        """One delivery as stored; raises ``MalformedProgressError``."""
        stored: dict[Any, Any] = await self._redis.hgetall(
            self._config.progress_delivery_key(delivery_id)
        )
        return _parse_delivery(delivery_id, stored)

    async def read_deliveries(
        self, delivery_ids: Sequence[str]
    ) -> dict[str, StoredProgressDelivery | MalformedProgressError | None]:
        """A sweep batch in one pipeline; a malformed member is returned as its
        error so it cannot abort the rest of the batch."""
        if not delivery_ids:
            return {}
        async with self._redis.pipeline(transaction=False) as pipe:
            for delivery_id in delivery_ids:
                pipe.hgetall(self._config.progress_delivery_key(delivery_id))
            stored_hashes = await pipe.execute()
        out: dict[str, StoredProgressDelivery | MalformedProgressError | None] = {}
        for delivery_id, stored in zip(delivery_ids, stored_hashes, strict=True):
            try:
                out[delivery_id] = _parse_delivery(delivery_id, stored)
            except MalformedProgressError as exc:
                out[delivery_id] = exc
        return out


ProgressDeliver = Callable[[StoredProgressDelivery], Awaitable[str | None]]
"""Deliver one stored delivery, returning the adapter's ref for a post; raise on
failure. None is passed by the maintenance tick, which delivers nothing yet."""


@dataclass
class ProgressSweep:
    """What one sweep pass did, for its caller and its tests."""

    delivered: int = 0
    failed: int = 0
    dead_lettered: int = 0
    quarantined: int = 0
    dropped: int = 0


async def sweep_pending_progress(
    store: ProgressStore,
    *,
    deliver: ProgressDeliver | None = None,
    batch: int = PROGRESS_SWEEP_BATCH,
    budget_s: float = PROGRESS_SWEEP_BUDGET_S,
    grace_s: float = PROGRESS_SWEEP_GRACE_S,
    max_attempts: int = PROGRESS_MAX_ATTEMPTS,
) -> ProgressSweep:
    """One bounded pass over the progress outbox (README, "The outbox").

    Without a deliverer the pass charges no attempt: it only quarantines,
    drops and dead-letters, so a replica that cannot deliver never spends the
    budget of one that can.
    """
    result = ProgressSweep()
    started = time.monotonic()
    members = await store.pending(batch)
    stored_batch = await store.read_deliveries(sorted(members))
    for seen, (delivery_id, stored) in enumerate(stored_batch.items()):
        remaining = budget_s - (time.monotonic() - started)
        if remaining <= 0:
            logger.info(
                "progress sweep budget (%.0fs) reached after %d delivery(ies); "
                "the rest are left for the next pass",
                budget_s,
                seen,
            )
            break
        if isinstance(stored, MalformedProgressError):
            logger.error("quarantined malformed progress delivery %s: %s", delivery_id, stored)
            await store.drop_pending_member(delivery_id)
            result.quarantined += 1
            continue
        if stored is None:
            await store.drop_pending_member(delivery_id)
            result.dropped += 1
            continue
        if stored.attempts >= max_attempts:
            if await _dead_letter(store, stored, stored.attempts):
                result.dead_lettered += 1
            continue
        if deliver is None or time.time() - stored.created_at < grace_s:
            continue
        attempts = await store.charge_attempt(delivery_id, generation=stored.generation)
        if attempts is None:
            continue
        try:
            card_ref = await asyncio.wait_for(deliver(stored), timeout=remaining)
        except Exception as exc:  # noqa: BLE001 - one failed delivery must not end the pass
            result.failed += 1
            logger.warning(
                "progress delivery %s failed on attempt %d of %d: %r",
                delivery_id,
                attempts,
                max_attempts,
                exc,
            )
            if attempts >= max_attempts and await _dead_letter(store, stored, attempts):
                result.dead_lettered += 1
            continue
        if await store.ack(delivery_id, generation=stored.generation, card_ref=card_ref):
            result.delivered += 1
    return result


async def _dead_letter(store: ProgressStore, stored: StoredProgressDelivery, attempts: int) -> bool:
    delivery_id = stored.delivery.delivery_id
    retained = await store.dead_letter(
        delivery_id, generation=stored.generation, reason="max-attempts-exceeded"
    )
    if retained:
        logger.error(
            "dead-lettered progress delivery %s of %s after %d attempt(s) "
            "(reason=max-attempts-exceeded) -> %s",
            delivery_id,
            stored.delivery.progress_id,
            attempts,
            store.dead_letter_stream,
        )
    return retained


def _require_position(name: str, value: int) -> None:
    if not 1 <= value <= _MAX_POSITION:
        raise ValueError(f"{name} must be between 1 and {_MAX_POSITION}, got {value}")


def _refusal(reason: str) -> ProgressRefusal:
    if reason not in _REFUSALS:
        raise RuntimeError(f"progress script answered an unknown refusal {reason!r}")
    return cast(ProgressRefusal, reason)


def _parse_delivery(delivery_id: str, stored: dict[Any, Any]) -> StoredProgressDelivery | None:
    """The one reading of a delivery hash, shared by the single and batch reads."""
    if not stored:
        return None
    fields = {str(_as_str(k)): str(_as_str(v)) for k, v in stored.items()}
    try:
        delivery = ProgressDelivery.model_validate_json(fields["event"])
        parsed = StoredProgressDelivery(
            delivery=delivery,
            route=TargetRoute.model_validate_json(fields["route"]),
            attempts=int(fields["attempts"]),
            generation=fields["gen"],
            created_at=float(fields["created_at"]),
            slot=fields["slot"],
        )
    except (KeyError, ValueError, ValidationError) as exc:
        raise MalformedProgressError(f"progress delivery {delivery_id}: {exc!r}") from exc
    if delivery.delivery_id != delivery_id or not parsed.generation:
        raise MalformedProgressError(
            f"progress delivery {delivery_id} names {delivery.delivery_id} or has no generation"
        )
    return parsed


def _as_str(value: Any) -> str | None:
    """Values as ``str``, tolerating a client without ``decode_responses``."""
    if value is None:
        return None
    return value.decode() if isinstance(value, bytes) else str(value)
