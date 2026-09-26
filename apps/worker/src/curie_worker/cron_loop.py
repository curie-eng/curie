"""The cron scheduler loop (ADR-0099, #268).

Runs in the worker beside the connector reconcile loop. Each pass reads the
cron triggers every in-force deployment declares, resolves the latest slot that
came due since the previous pass, records that slot in ``hook_runs`` and, when
admitted, puts one CRON ``QueuedTurn`` on the runs stream. The kernel consumes
that turn like any other and closes the hook run from the turn's exit.

Four choices make this safe to run on every replica, unattended:

**The row is the claim.** ``hook_runs`` is unique on ``(agent_id, name,
slot_utc)``. Every replica computes the same slots, and exactly one INSERT
wins; the losers see no returned row and stop. No leader election. Replicas
whose watermarks differ can resolve different slots of one hook, so admission
also takes a transaction-scoped advisory lock per ``(agent, name)`` and treats
any other open row for that hook as in flight.

**A claim carries a lease.** Every in-flight row records when its lease
lapses, and the kernel renews it when it starts the turn, so time spent
queued on the stream never counts against a live run. A worker that dies
mid-turn never closes its row, so the hook's next fire closes any open row
past its lease as ``reclaimed`` and proceeds, the same recovery
``XAUTOCLAIM`` gives a stream delivery whose consumer died.

**Catch-up is bounded to one slot (#2930).** A pass covers (watermark, now],
reaching back to the hook's last recorded slot when that is earlier, so a
restarted worker still sees the slots it slept through. It fires only the
newest due slot and records every older one ``skipped``. The newest is skipped
too once it is older than the schedule's own interval or ``CATCH_UP_CEILING``,
whichever is shorter. A hook with no recorded slot never fires a slot from
before this worker started, and no hook reaches back past its deployment. The
reach back stops at ``_CATCH_UP_LOOKBACK`` and one pass records at most
``_MAX_SKIPPED_ROWS`` skipped slots per hook.

**One hook's failure ends with that hook.** An exception while reading one
agent's triggers, or resolving or admitting one hook, is caught and logged, and
the rest of the pass continues, for the same reason as the connector loop: the
order is arbitrary, so aborting would starve whichever hooks sorted later.

**A deferred fire is retried, within the catch-up bound.** The kernel records
``deferred`` when a targeted fire meets a live session on its thread, read under
the per-thread lock (ADR-0099, #2929). Each pass reopens such a slot and puts it
back on the stream under a fresh event id, until it runs or is past the same
catch-up age bound a missed slot uses. Past the bound the slot is recorded
``skipped``.

**A pass never kills the worker.** ``run_forever`` wraps the pass whole. The
loop shares a process with the kernel, so a scheduling problem fails loudly
and retries on the next tick instead of becoming a worker outage.

Slots are computed over NAIVE local wall-clock time in the hook's zone and then
mapped to instants. A wall time inside a spring-forward gap does not exist and
is dropped; an ambiguous wall time in a fall-back fold fires once, at its first
occurrence.
"""

from __future__ import annotations

import asyncio
import json
import logging
import tempfile
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from aci_protocol import HookRunRef, QueuedTurn, ReplyHandle, TurnSource
from aci_protocol.service_config import STREAM_PAYLOAD_FIELD
from channel_protocol import hook_conversation_id
from cronsim import CronSim
from plugin_format import resolve_manifest
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .bundle_store import BundleReader, extract_bundle
from .hook_runs import retry_event_id

logger = logging.getLogger(__name__)

# How far before the window's local start the cron iteration begins. It covers
# the widest UTC offset change a zone makes in one transition, so a slot whose
# wall time reads earlier than the window start (a fold) is still enumerated
# and then filtered on its instant.
_ITERATION_MARGIN = timedelta(hours=3)

# A missed slot older than this is skipped rather than fired, however coarse the
# schedule: a monthly hook four weeks late starts fresh.
CATCH_UP_CEILING = timedelta(hours=24)

# How far back a restarted worker looks for slots it slept through. It bounds
# the slot enumeration (a minute schedule enumerates about 50,000 slots at
# most) while a monthly hook down four weeks still gets its missed slot
# recorded.
_CATCH_UP_LOOKBACK = timedelta(days=35)

# The most skipped rows one pass writes for one hook, newest kept. A minute
# schedule down for days would otherwise write thousands of rows in one pass.
_MAX_SKIPPED_ROWS = 1000


def resolve_slots(
    schedule: str, zone: str, window_start: datetime, window_end: datetime
) -> list[datetime]:
    """Every cron slot in (window_start, window_end], as ascending aware UTC.

    Args:
        schedule: A five-field cron expression, read in ``zone``'s wall time.
        zone: An IANA zone name.
        window_start: Exclusive lower bound, an aware instant.
        window_end: Inclusive upper bound, an aware instant.

    Returns:
        The slot instants in UTC.
    """

    tz = ZoneInfo(zone)
    local_start = window_start.astimezone(tz).replace(tzinfo=None) - _ITERATION_MARGIN
    local_end = window_end.astimezone(tz).replace(tzinfo=None) + _ITERATION_MARGIN
    slots: list[datetime] = []
    for wall in CronSim(schedule, local_start):
        if wall > local_end:
            break
        instant = wall.replace(tzinfo=tz, fold=0)
        # A gap wall time does not round-trip: converting to UTC and back
        # lands on a different wall time. It never happened, so it is dropped.
        if instant.astimezone(UTC).astimezone(tz).replace(tzinfo=None) != wall:
            continue
        utc = instant.astimezone(UTC)
        if window_start < utc <= window_end:
            slots.append(utc)
    slots.sort()
    return slots


def catch_up_bound(
    schedule: str, zone: str, slot: datetime, ceiling: timedelta = CATCH_UP_CEILING
) -> timedelta:
    """How long after ``slot`` it may still fire: a missed slot on restart, or a
    deferred one on retry (#2929).

    The schedule's own interval at ``slot`` (the gap to the next slot, in the
    hook's zone), capped by ``ceiling`` for a coarse schedule.
    """

    following = resolve_slots(schedule, zone, slot, slot + ceiling)
    return following[0] - slot if following else ceiling


def slot_is_stale(
    schedule: str,
    zone: str,
    slot: datetime,
    now: datetime,
    ceiling: timedelta = CATCH_UP_CEILING,
) -> bool:
    """Whether ``slot`` is past the catch-up age bound at ``now``.

    The bound is the schedule's own interval at ``slot`` (the gap to the next
    slot, in the hook's zone), capped by ``ceiling`` for a coarse schedule.
    """

    return now - slot > catch_up_bound(schedule, zone, slot, ceiling)


def plan_catch_up(
    schedule: str, zone: str, due: list[datetime], now: datetime
) -> tuple[datetime | None, list[datetime]]:
    """The one slot to fire and the slots to record skipped.

    Args:
        schedule: The hook's cron expression.
        zone: The hook's IANA zone.
        due: Ascending unrecorded slots, as ``resolve_slots`` returns them.
        now: The pass instant.

    Returns:
        The newest due slot, or None when nothing is due or it is stale, and
        every other due slot, ascending.
    """

    if not due:
        return None, []
    newest = due[-1]
    if slot_is_stale(schedule, zone, newest, now):
        return None, list(due)
    return newest, list(due[:-1])


class TriggerSource(Protocol):
    def triggers(self, bundle_ref: str) -> list[dict[str, Any]]: ...


def read_bundle_triggers(root: Path) -> list[Any]:
    """``plugin.json`` ``triggers`` as stored under an extracted plugin root.

    Mirrors the API's ``bundles.read_manifest_triggers``: a missing manifest or
    key, unreadable JSON, or a non-list value is an empty list.
    """

    manifest_path = resolve_manifest(root)
    if manifest_path is None:
        return []
    try:
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []
    if not isinstance(raw, dict):
        return []
    triggers = raw.get("triggers")
    return triggers if isinstance(triggers, list) else []


class BundleTriggerSource:
    """Reads a version's declared triggers straight from its stored bundle.

    The bundle is the immutable record of what a version declares, so this
    needs no connector render route, release parameters, or API call; a bundle
    carrying any kind of connector lock reads the same. Synchronous on purpose:
    the loop runs it in a worker thread. Extraction goes through
    ``extract_bundle``, so the traversal and size guards apply here too.
    """

    def __init__(
        self,
        reader: BundleReader,
        *,
        max_uncompressed_bytes: int,
        max_compression_ratio: float,
        max_members: int,
    ) -> None:
        self._reader = reader
        self._max_uncompressed_bytes = max_uncompressed_bytes
        self._max_compression_ratio = max_compression_ratio
        self._max_members = max_members

    def triggers(self, bundle_ref: str) -> list[dict[str, Any]]:
        data = self._reader.get(bundle_ref)
        with tempfile.TemporaryDirectory(prefix="curie-cron-bundle-") as tmp:
            root = extract_bundle(
                data,
                Path(tmp),
                max_uncompressed_bytes=self._max_uncompressed_bytes,
                max_compression_ratio=self._max_compression_ratio,
                max_members=self._max_members,
            )
            declared = read_bundle_triggers(root)
        return [t for t in declared if isinstance(t, dict)]


# Ranked exactly as connector_loop._TARGETS_SQL (and binding.py's _RESOLVE_SQL):
# prod outranks dev, then most recent deployed_at, then id as a total-order
# tiebreak. The triggers that fire must belong to the version a sandbox would
# actually boot. The agent's budget columns ride along for the admission check.
_TARGETS_SQL = """
SELECT DISTINCT ON (a.id)
       a.id AS agent_id,
       a.name AS agent_name,
       v.id AS version_id,
       v.bundle_ref AS bundle_ref,
       d.deployed_at AS deployed_at,
       a.max_usd_per_day AS max_usd_per_day,
       a.max_output_tokens_per_run AS max_output_tokens_per_run
FROM {schema}.agents a
JOIN {schema}.deployments d ON d.agent_id = a.id AND d.status = 'active'
JOIN {schema}.agent_versions v ON v.id = d.version_id AND v.agent_id = a.id
ORDER BY a.id, (d.environment = 'prod') DESC, d.deployed_at DESC, d.id DESC
"""

_BINDINGS_SQL = """
SELECT kind, address, endpoint, adapter
FROM {schema}.agent_channels
WHERE agent_id = :agent_id AND address = :address
"""

_INSERT_SQL = """
INSERT INTO {schema}.hook_runs
       (id, agent_id, name, slot_utc, version_id, outcome, started_at, ended_at,
        lease_expires_at)
VALUES (:id, :agent_id, :name, :slot, :version_id, CAST(:outcome AS text), now(),
        CASE WHEN :terminal THEN now() END,
        CASE WHEN :terminal THEN NULL ELSE now() + make_interval(secs => :lease_s) END)
ON CONFLICT (agent_id, name, slot_utc) DO NOTHING
RETURNING id
"""

# Serializes admission per (agent, name) across replicas for the length of the
# admission transaction, so two passes resolving different slots of one hook
# cannot both see "nothing in flight" and both admit.
_LOCK_SQL = """
SELECT pg_advisory_xact_lock(hashtextextended(CAST(:agent_id AS text) || ':' || :name, 0))
"""

# The lease is compared on the database clock, the clock that set it. A claim
# with no lease (written before migration 0062, or by an older worker during a
# rollout) is held for one lease from its start.
_RECLAIM_SQL = """
UPDATE {schema}.hook_runs
SET outcome = 'reclaimed', ended_at = now()
WHERE agent_id = :agent_id AND name = :name AND outcome IS NULL
  AND slot_utc <> :slot
  AND COALESCE(lease_expires_at, started_at + make_interval(secs => :lease_s)) < now()
"""

# Any other open row for this hook blocks, whichever slot it holds: a later
# slot admitted by a replica with a newer watermark is just as much in flight.
_IN_FLIGHT_SQL = """
SELECT 1 FROM {schema}.hook_runs
WHERE agent_id = :agent_id AND name = :name AND outcome IS NULL AND slot_utc <> :slot
LIMIT 1
"""

_LAST_SLOT_SQL = """
SELECT max(slot_utc) FROM {schema}.hook_runs WHERE agent_id = :agent_id AND name = :name
"""

_CONTROL_SQL = """
SELECT paused_at, resume_from, generation FROM {schema}.schedule_controls
WHERE agent_id = :agent_id AND name = :name
"""

_CLEAR_RESUME_SQL = """
UPDATE {schema}.schedule_controls SET resume_from = NULL
WHERE agent_id = :agent_id AND name = :name AND paused_at IS NULL
  AND resume_from = :resume_from AND generation = :generation
"""

_SKIP_SQL = """
INSERT INTO {schema}.hook_runs
       (id, agent_id, name, slot_utc, version_id, outcome, started_at, ended_at)
VALUES (:id, :agent_id, :name, :slot, :version_id, 'skipped', now(), now())
ON CONFLICT (agent_id, name, slot_utc) DO NOTHING
"""

_DEFERRED_SQL = """
SELECT slot_utc FROM {schema}.hook_runs
WHERE agent_id = :agent_id AND name = :name AND outcome = 'deferred'
ORDER BY slot_utc
"""

_SETTLE_DEFERRED_SQL = """
UPDATE {schema}.hook_runs SET outcome = CAST(:outcome AS text), ended_at = now()
WHERE agent_id = :agent_id AND name = :name AND slot_utc = :slot AND outcome = 'deferred'
"""

# The reopen is a CAS on ``deferred``, so of two replicas retrying one slot
# exactly one gets the row back in flight and enqueues it.
_REOPEN_SQL = """
UPDATE {schema}.hook_runs SET outcome = NULL, ended_at = NULL, started_at = now(),
       lease_expires_at = now() + make_interval(secs => :lease_s)
WHERE agent_id = :agent_id AND name = :name AND slot_utc = :slot AND outcome = 'deferred'
RETURNING id
"""

_FAIL_RUN_SQL = """
UPDATE {schema}.hook_runs SET outcome = 'failed', ended_at = now()
WHERE id = :id AND outcome IS NULL
"""


class _UnboundTarget(Exception):
    """A hook's target matches no single binding of its agent."""


@dataclass(frozen=True)
class _Target:
    agent_id: uuid.UUID
    agent_name: str
    version_id: uuid.UUID
    bundle_ref: str | None
    deployed_at: datetime | None
    max_usd_per_day: float | None
    max_output_tokens_per_run: int | None


@dataclass
class CronPassSummary:
    """What one pass decided, one counter per slot outcome."""

    admitted: int = 0
    retried: int = 0
    blocked: int = 0
    skipped: int = 0
    failed: int = 0
    reclaimed: int = 0
    lost: int = 0

    @property
    def did_work(self) -> bool:
        return bool(
            self.admitted
            or self.retried
            or self.blocked
            or self.skipped
            or self.failed
            or self.reclaimed
        )


class CronSchedulerLoop:
    """Fires every deployed agent's cron triggers, forever."""

    def __init__(
        self,
        *,
        engine: AsyncEngine,
        redis: Redis,
        source: TriggerSource,
        is_killed: Callable[[uuid.UUID], Awaitable[bool]],
        db_schema: str,
        stream: str,
        interval_seconds: float,
        claim_lease_s: float,
        default_max_usd_per_day: float,
        default_max_output_tokens_per_run: int,
        started_at: datetime | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._engine = engine
        self._redis = redis
        self._source = source
        self._is_killed = is_killed
        self._stream = stream
        self._interval = interval_seconds
        self._claim_lease_s = claim_lease_s
        self._default_usd = default_max_usd_per_day
        self._default_tokens = default_max_output_tokens_per_run
        self._clock = clock or (lambda: datetime.now(UTC))
        self._watermark = started_at or self._clock()
        # A version's declared triggers are immutable, so one fetch per version.
        self._triggers: dict[tuple[uuid.UUID, str], list[dict[str, Any]]] = {}
        # Table identifiers are not user input; the schema comes from config.
        self._targets_sql = text(_TARGETS_SQL.format(schema=db_schema))
        self._bindings_sql = text(_BINDINGS_SQL.format(schema=db_schema))
        self._insert_sql = text(_INSERT_SQL.format(schema=db_schema))
        self._reclaim_sql = text(_RECLAIM_SQL.format(schema=db_schema))
        self._lock_sql = text(_LOCK_SQL)
        self._in_flight_sql = text(_IN_FLIGHT_SQL.format(schema=db_schema))
        self._fail_run_sql = text(_FAIL_RUN_SQL.format(schema=db_schema))
        self._last_slot_sql = text(_LAST_SLOT_SQL.format(schema=db_schema))
        self._control_sql = text(_CONTROL_SQL.format(schema=db_schema))
        self._clear_resume_sql = text(_CLEAR_RESUME_SQL.format(schema=db_schema))
        self._skip_sql = text(_SKIP_SQL.format(schema=db_schema))
        self._deferred_sql = text(_DEFERRED_SQL.format(schema=db_schema))
        self._settle_deferred_sql = text(_SETTLE_DEFERRED_SQL.format(schema=db_schema))
        self._reopen_sql = text(_REOPEN_SQL.format(schema=db_schema))

    async def _targets(self) -> list[_Target]:
        async with self._engine.connect() as conn:
            rows = (await conn.execute(self._targets_sql)).mappings().all()
        return [
            _Target(
                agent_id=row["agent_id"],
                agent_name=row["agent_name"],
                version_id=row["version_id"],
                bundle_ref=row["bundle_ref"],
                deployed_at=row["deployed_at"],
                max_usd_per_day=row["max_usd_per_day"],
                max_output_tokens_per_run=row["max_output_tokens_per_run"],
            )
            for row in rows
        ]

    async def _cron_triggers(self, target: _Target) -> list[dict[str, Any]]:
        if not target.bundle_ref:
            # No bundle attached yet; do not cache, so a later bundle attach
            # on this same active version is picked up on the next pass.
            return []
        cache_key = (target.version_id, target.bundle_ref)
        cached = self._triggers.get(cache_key)
        if cached is None:
            fetched = await asyncio.to_thread(self._source.triggers, target.bundle_ref)
            cached = [t for t in fetched if isinstance(t, dict) and t.get("type") == "cron"]
            self._triggers[cache_key] = cached
        return cached

    def _budget_spent(self, target: _Target) -> bool:
        # No daily-spend ledger exists in the worker, so the only spent budget
        # knowable here is one configured to nothing.
        usd = target.max_usd_per_day if target.max_usd_per_day is not None else self._default_usd
        tokens = (
            target.max_output_tokens_per_run
            if target.max_output_tokens_per_run is not None
            else self._default_tokens
        )
        return usd <= 0 or tokens <= 0

    async def _insert(
        self,
        conn: AsyncConnection,
        target: _Target,
        name: str,
        slot: datetime,
        outcome: str | None,
    ) -> uuid.UUID | None:
        row = (
            await conn.execute(
                self._insert_sql,
                {
                    "id": uuid.uuid4(),
                    "agent_id": target.agent_id,
                    "name": name,
                    "slot": slot,
                    "version_id": target.version_id,
                    "outcome": outcome,
                    # Terminal outcomes close the row at once; NULL is in flight.
                    "terminal": outcome is not None,
                    "lease_s": self._claim_lease_s,
                },
            )
        ).first()
        return None if row is None else row[0]

    async def _window_start(
        self,
        target: _Target,
        name: str,
        window_start: datetime,
        now: datetime,
        resume_from: datetime | None,
    ) -> datetime:
        """The pass window's start, reaching back to the last slot or resume.

        Whatever the start, it never passes the in-force deployment (a slot
        from before it belongs to whatever schedule was deployed then) or
        ``_CATCH_UP_LOOKBACK``.
        """

        async with self._engine.connect() as conn:
            last = (
                await conn.execute(self._last_slot_sql, {"agent_id": target.agent_id, "name": name})
            ).scalar()
        start = window_start
        if isinstance(last, datetime) and last < start:
            start = last
        if resume_from is not None and resume_from < start:
            start = resume_from
        floor = now - _CATCH_UP_LOOKBACK
        if target.deployed_at is not None:
            floor = max(floor, target.deployed_at)
        return max(start, floor)

    async def _control(
        self, conn: AsyncConnection, target: _Target, name: str
    ) -> tuple[datetime | None, datetime | None, int]:
        """Read durable control state without caching it across passes."""

        row = (
            await conn.execute(self._control_sql, {"agent_id": target.agent_id, "name": name})
        ).first()
        return (None, None, 0) if row is None else (row[0], row[1], row[2])

    async def _skip(
        self, target: _Target, name: str, slots: list[datetime], summary: CronPassSummary
    ) -> None:
        if not slots:
            return
        if len(slots) > _MAX_SKIPPED_ROWS:
            logger.warning(
                "cron hook %s for agent=%s missed %d slots; recording the newest %d skipped",
                name,
                target.agent_name,
                len(slots),
                _MAX_SKIPPED_ROWS,
            )
            slots = slots[-_MAX_SKIPPED_ROWS:]
        async with self._engine.begin() as conn:
            await conn.execute(
                self._skip_sql,
                [
                    {
                        "id": uuid.uuid4(),
                        "agent_id": target.agent_id,
                        "name": name,
                        "slot": slot,
                        "version_id": target.version_id,
                    }
                    for slot in slots
                ],
            )
        summary.skipped += len(slots)

    async def _admit(
        self,
        target: _Target,
        trigger: dict[str, Any],
        slot: datetime,
        summary: CronPassSummary,
        defer_if_busy: bool,
    ) -> None:
        name = str(trigger["name"])
        if await self._is_killed(target.agent_id) or self._budget_spent(target):
            async with self._engine.begin() as conn:
                await self._lock_and_reclaim(conn, target, name, slot, summary)
                await self._insert(conn, target, name, slot, "blocked")
            summary.blocked += 1
            return

        try:
            handle = await self._reply_handle(target, trigger)
        except _UnboundTarget:
            async with self._engine.begin() as conn:
                await self._lock_and_reclaim(conn, target, name, slot, summary)
                await self._insert(conn, target, name, slot, "failed")
            summary.failed += 1
            return

        async with self._engine.begin() as conn:
            await self._lock_and_reclaim(conn, target, name, slot, summary)
            paused_at, _, _ = await self._control(conn, target, name)
            if paused_at is not None:
                await self._insert(conn, target, name, slot, "deferred")
                return
            if await self._blocked_by_in_flight(conn, target, name, slot):
                await self._insert(
                    conn, target, name, slot, "deferred" if defer_if_busy else "skipped"
                )
                if not defer_if_busy:
                    summary.skipped += 1
                return
            run_id = await self._insert(conn, target, name, slot, None)
        if run_id is None:
            # Another replica recorded this slot first.
            summary.lost += 1
            return

        try:
            await self._enqueue(target, trigger, handle, slot, run_id)
        except Exception:
            summary.failed += 1
            raise
        summary.admitted += 1

    async def _lock_and_reclaim(
        self,
        conn: AsyncConnection,
        target: _Target,
        name: str,
        slot: datetime,
        summary: CronPassSummary,
    ) -> None:
        """Take the hook's admission lock and reclaim its claims past their lease.

        Every fire of the hook does this, whatever it records for its own slot,
        so a dead run is recorded ``reclaimed`` by the very next fire.
        """
        await conn.execute(self._lock_sql, {"agent_id": str(target.agent_id), "name": name})
        reclaimed = await conn.execute(
            self._reclaim_sql,
            {
                "agent_id": target.agent_id,
                "name": name,
                "slot": slot,
                "lease_s": self._claim_lease_s,
            },
        )
        if reclaimed.rowcount:
            logger.warning(
                "cron hook %s for agent=%s reclaimed %d claim(s) past their lease",
                name,
                target.agent_name,
                reclaimed.rowcount,
            )
            summary.reclaimed += reclaimed.rowcount

    async def _blocked_by_in_flight(
        self,
        conn: AsyncConnection,
        target: _Target,
        name: str,
        slot: datetime,
    ) -> bool:
        in_flight = (
            await conn.execute(
                self._in_flight_sql,
                {"agent_id": target.agent_id, "name": name, "slot": slot},
            )
        ).first()
        return in_flight is not None

    async def _reply_handle(self, target: _Target, trigger: dict[str, Any]) -> ReplyHandle | None:
        """The bound route a targeted hook replies on; None for a targetless one.

        Raises ``_UnboundTarget`` when the target matches no single binding.
        """

        name = str(trigger["name"])
        handle: ReplyHandle | None = None
        address = trigger.get("target")
        if address is not None:
            async with self._engine.connect() as conn:
                bindings = (
                    (
                        await conn.execute(
                            self._bindings_sql,
                            {"agent_id": target.agent_id, "address": str(address)},
                        )
                    )
                    .mappings()
                    .all()
                )
            if len(bindings) != 1:
                logger.warning(
                    "cron hook %s for agent=%s targets %r, which matches %d bindings; "
                    "recording failed",
                    name,
                    target.agent_name,
                    address,
                    len(bindings),
                )
                raise _UnboundTarget
            binding = bindings[0]
            handle = ReplyHandle(
                kind=binding["kind"],
                channel=binding["address"],
                placeholder=None,
                endpoint=binding["endpoint"],
                adapter=binding["adapter"],
            )
        return handle

    async def _enqueue(
        self,
        target: _Target,
        trigger: dict[str, Any],
        handle: ReplyHandle | None,
        slot: datetime,
        run_id: uuid.UUID,
        *,
        retry_expires_at: datetime | None = None,
    ) -> None:
        name = str(trigger["name"])
        agent = str(target.agent_id)
        slot_iso = slot.isoformat()
        event_id = f"cron:{agent}:{name}:{slot_iso}"
        if retry_expires_at is not None:
            event_id = retry_event_id(event_id, retry_expires_at)
        turn = QueuedTurn(
            event_id=event_id,
            conversation_id=hook_conversation_id(target.agent_id, name),
            # The author is the platform: no person sent this.
            author=f"cron:{name}",
            text=str(trigger["prompt"]),
            source=TurnSource.CRON,
            reply_handle=handle,
            received_at=datetime.now(UTC).isoformat(),
            hook_run=HookRunRef(agent_id=agent, name=name, slot_utc=slot_iso),
        )
        try:
            await self._redis.xadd(self._stream, {STREAM_PAYLOAD_FIELD: turn.model_dump_json()})
        except Exception:
            async with self._engine.begin() as conn:
                await conn.execute(self._fail_run_sql, {"id": run_id})
            raise

    async def _retry_deferred(
        self,
        target: _Target,
        trigger: dict[str, Any],
        zone: str,
        now: datetime,
        summary: CronPassSummary,
    ) -> None:
        """Reopen each deferred slot of this hook, or age it out as skipped."""

        name = str(trigger["name"])
        schedule = str(trigger["schedule"])
        async with self._engine.connect() as conn:
            slots = [
                row[0]
                for row in (
                    await conn.execute(
                        self._deferred_sql, {"agent_id": target.agent_id, "name": name}
                    )
                ).all()
            ]
        for slot in slots:
            key = {"agent_id": target.agent_id, "name": name, "slot": slot}
            expires_at = slot + catch_up_bound(schedule, zone, slot)
            if slot_is_stale(schedule, zone, slot, now):
                async with self._engine.begin() as conn:
                    await conn.execute(self._settle_deferred_sql, {**key, "outcome": "skipped"})
                summary.skipped += 1
                continue
            if await self._is_killed(target.agent_id) or self._budget_spent(target):
                async with self._engine.begin() as conn:
                    await conn.execute(self._settle_deferred_sql, {**key, "outcome": "blocked"})
                summary.blocked += 1
                continue
            try:
                handle = await self._reply_handle(target, trigger)
            except _UnboundTarget:
                async with self._engine.begin() as conn:
                    await conn.execute(self._settle_deferred_sql, {**key, "outcome": "failed"})
                summary.failed += 1
                continue
            async with self._engine.begin() as conn:
                await self._lock_and_reclaim(conn, target, name, slot, summary)
                paused_at, _, _ = await self._control(conn, target, name)
                if paused_at is not None:
                    # The pause raced the pass. Leave the row deferred for a
                    # later pass after resume.
                    return
                if await self._blocked_by_in_flight(conn, target, name, slot):
                    # Another fire of this hook is live; stay deferred until it
                    # settles or this slot ages out.
                    continue
                row = (
                    await conn.execute(self._reopen_sql, {**key, "lease_s": self._claim_lease_s})
                ).first()
            if row is None:
                # Another replica reopened or settled it first.
                summary.lost += 1
                continue
            try:
                await self._enqueue(
                    target, trigger, handle, slot, row[0], retry_expires_at=expires_at
                )
            except Exception:
                summary.failed += 1
                raise
            summary.retried += 1

    async def one_pass(self, now: datetime | None = None) -> CronPassSummary:
        """Schedule every deployed agent's cron triggers for (watermark, now]."""

        now = now or self._clock()
        window_start, self._watermark = self._watermark, now
        summary = CronPassSummary()
        for target in await self._targets():
            try:
                triggers = await self._cron_triggers(target)
            except Exception:
                summary.failed += 1
                logger.exception(
                    "cron trigger read raised for agent=%s; continuing with the rest",
                    target.agent_name,
                )
                continue
            for trigger in triggers:
                name, schedule, prompt = (
                    trigger.get("name"),
                    trigger.get("schedule"),
                    trigger.get("prompt"),
                )
                if not name or not schedule or not prompt:
                    continue
                try:
                    zone = str(trigger.get("timezone") or "UTC")
                    async with self._engine.connect() as conn:
                        paused_at, resume_from, generation = await self._control(
                            conn, target, str(name)
                        )
                    if paused_at is not None:
                        continue
                    await self._retry_deferred(target, trigger, zone, now, summary)
                    start = await self._window_start(
                        target, str(name), window_start, now, resume_from
                    )
                    due = resolve_slots(str(schedule), zone, start, now)
                    fire, skipped = plan_catch_up(str(schedule), zone, due, now)
                    await self._skip(target, str(name), skipped, summary)
                    if fire is not None:
                        await self._admit(
                            target, trigger, fire, summary, resume_from is not None
                        )
                    if resume_from is not None:
                        async with self._engine.begin() as conn:
                            await conn.execute(
                                self._clear_resume_sql,
                                {
                                    "agent_id": target.agent_id,
                                    "name": str(name),
                                    "resume_from": resume_from,
                                    "generation": generation,
                                },
                            )
                except Exception:
                    # Ends with this hook. The agent's other hooks, and every
                    # later agent, still run: the order is arbitrary.
                    summary.failed += 1
                    logger.exception(
                        "cron hook %s raised for agent=%s; continuing with the rest",
                        name,
                        target.agent_name,
                    )

        # A pass that only lost a slot race to another replica did no work,
        # but it is the one signal that replicas are contending for slots, so
        # it must stay visible at INFO (#3013). A pass where nothing happened
        # stays at DEBUG.
        log = logger.info if (summary.did_work or summary.lost) else logger.debug
        log(
            "cron pass: %d admitted, %d retried, %d blocked, %d skipped, %d failed, "
            "%d reclaimed, %d lost",
            summary.admitted,
            summary.retried,
            summary.blocked,
            summary.skipped,
            summary.failed,
            summary.reclaimed,
            summary.lost,
        )
        return summary

    async def run_forever(self, stop: asyncio.Event | None = None) -> None:
        """Schedule on an interval until told to stop.

        Never raises: a scheduling problem must not become a worker outage.
        """

        stop = stop or asyncio.Event()
        logger.info("cron scheduler loop started interval=%ss", self._interval)
        while not stop.is_set():
            try:
                await self.one_pass()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("cron pass failed; retrying next interval")
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._interval)
            except TimeoutError:
                continue
        logger.info("cron scheduler loop stopped")
