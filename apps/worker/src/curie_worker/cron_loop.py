"""The cron scheduler loop (ADR-0099, #268).

Runs in the worker beside the connector reconcile loop. Each pass reads the
cron triggers every in-force deployment declares, resolves the latest slot that
came due since the previous pass, records that slot in ``hook_runs`` and, when
admitted, puts one CRON ``QueuedTurn`` on the runs stream. The kernel consumes
that turn like any other and closes the hook run from the turn's exit.

Four choices make this safe to run on every replica, unattended:

**The row is the claim.** ``hook_runs`` is unique on ``(agent_id, name,
slot_utc)``. Every replica computes the same slots, and exactly one INSERT
wins; the losers see no returned row and stop. No leader election, no lock.

**Missed slots are slept through, not replayed.** A pass covers (watermark,
now] and considers only the latest due slot per hook. A worker that was down
for a day fires once when it returns, not once per missed slot, and a fresh
worker never fires a slot from before it started.

**One agent's failure ends with that agent.** An exception while scheduling one
agent is caught and logged, and the rest of the pass continues, for the same
reason as the connector loop: the order is arbitrary, so aborting would starve
whichever agents happened to sort later.

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
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from aci_protocol import HookRunRef, QueuedTurn, ReplyHandle, TurnSource
from aci_protocol.service_config import STREAM_PAYLOAD_FIELD
from channel_protocol import hook_conversation_id
from cronsim import CronSim
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

logger = logging.getLogger(__name__)

# How far before the window's local start the cron iteration begins. It covers
# the widest UTC offset change a zone makes in one transition, so a slot whose
# wall time reads earlier than the window start (a fold) is still enumerated
# and then filtered on its instant.
_ITERATION_MARGIN = timedelta(hours=3)

# A stale in-flight row is one whose turn has outlived every delivery deadline
# plus this slack; the kernel should have closed it long ago.
_STALE_SLACK_S = 60.0


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


class TriggerSource(Protocol):
    def triggers(self, agent_id: str, version_id: str) -> list[dict[str, Any]]: ...


# Ranked exactly as connector_loop._TARGETS_SQL (and binding.py's _RESOLVE_SQL):
# prod outranks dev, then most recent deployed_at, then id as a total-order
# tiebreak. The triggers that fire must belong to the version a sandbox would
# actually boot. The agent's budget columns ride along for the admission check.
_TARGETS_SQL = """
SELECT DISTINCT ON (a.id)
       a.id AS agent_id,
       a.name AS agent_name,
       v.id AS version_id,
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
       (id, agent_id, name, slot_utc, version_id, outcome, started_at, ended_at)
VALUES (:id, :agent_id, :name, :slot, :version_id, CAST(:outcome AS text), now(),
        CASE WHEN :terminal THEN now() END)
ON CONFLICT (agent_id, name, slot_utc) DO NOTHING
RETURNING id
"""

_CLOSE_STALE_SQL = """
UPDATE {schema}.hook_runs
SET outcome = 'failed', ended_at = now()
WHERE agent_id = :agent_id AND name = :name AND outcome IS NULL
  AND slot_utc < :slot AND started_at < :cutoff
"""

_IN_FLIGHT_SQL = """
SELECT 1 FROM {schema}.hook_runs
WHERE agent_id = :agent_id AND name = :name AND outcome IS NULL AND slot_utc < :slot
LIMIT 1
"""

_FAIL_RUN_SQL = """
UPDATE {schema}.hook_runs SET outcome = 'failed', ended_at = now()
WHERE id = :id AND outcome IS NULL
"""


@dataclass(frozen=True)
class _Target:
    agent_id: uuid.UUID
    agent_name: str
    version_id: uuid.UUID
    max_usd_per_day: float | None
    max_output_tokens_per_run: int | None


@dataclass
class CronPassSummary:
    """What one pass decided, one counter per slot outcome."""

    admitted: int = 0
    blocked: int = 0
    skipped: int = 0
    failed: int = 0
    lost: int = 0

    @property
    def did_work(self) -> bool:
        return bool(self.admitted or self.blocked or self.skipped or self.failed)


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
        delivery_budget_s: float,
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
        self._stale_after = timedelta(seconds=delivery_budget_s + _STALE_SLACK_S)
        self._default_usd = default_max_usd_per_day
        self._default_tokens = default_max_output_tokens_per_run
        self._clock = clock or (lambda: datetime.now(UTC))
        self._watermark = started_at or self._clock()
        # A version's declared triggers are immutable, so one fetch per version.
        self._triggers: dict[uuid.UUID, list[dict[str, Any]]] = {}
        # Table identifiers are not user input; the schema comes from config.
        self._targets_sql = text(_TARGETS_SQL.format(schema=db_schema))
        self._bindings_sql = text(_BINDINGS_SQL.format(schema=db_schema))
        self._insert_sql = text(_INSERT_SQL.format(schema=db_schema))
        self._close_stale_sql = text(_CLOSE_STALE_SQL.format(schema=db_schema))
        self._in_flight_sql = text(_IN_FLIGHT_SQL.format(schema=db_schema))
        self._fail_run_sql = text(_FAIL_RUN_SQL.format(schema=db_schema))

    async def _targets(self) -> list[_Target]:
        async with self._engine.connect() as conn:
            rows = (await conn.execute(self._targets_sql)).mappings().all()
        return [
            _Target(
                agent_id=row["agent_id"],
                agent_name=row["agent_name"],
                version_id=row["version_id"],
                max_usd_per_day=row["max_usd_per_day"],
                max_output_tokens_per_run=row["max_output_tokens_per_run"],
            )
            for row in rows
        ]

    async def _cron_triggers(self, target: _Target) -> list[dict[str, Any]]:
        cached = self._triggers.get(target.version_id)
        if cached is None:
            fetched = await asyncio.to_thread(
                self._source.triggers, str(target.agent_id), str(target.version_id)
            )
            cached = [t for t in fetched if isinstance(t, dict) and t.get("type") == "cron"]
            self._triggers[target.version_id] = cached
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
                },
            )
        ).first()
        return None if row is None else row[0]

    async def _admit(
        self,
        target: _Target,
        trigger: dict[str, Any],
        slot: datetime,
        now: datetime,
        summary: CronPassSummary,
    ) -> None:
        name = str(trigger["name"])
        if await self._is_killed(target.agent_id) or self._budget_spent(target):
            async with self._engine.begin() as conn:
                await self._insert(conn, target, name, slot, "blocked")
            summary.blocked += 1
            return

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
                async with self._engine.begin() as conn:
                    await self._insert(conn, target, name, slot, "failed")
                summary.failed += 1
                return
            binding = bindings[0]
            handle = ReplyHandle(
                kind=binding["kind"],
                channel=binding["address"],
                placeholder=None,
                endpoint=binding["endpoint"],
                adapter=binding["adapter"],
            )

        async with self._engine.begin() as conn:
            await conn.execute(
                self._close_stale_sql,
                {
                    "agent_id": target.agent_id,
                    "name": name,
                    "slot": slot,
                    "cutoff": now - self._stale_after,
                },
            )
            in_flight = (
                await conn.execute(
                    self._in_flight_sql,
                    {"agent_id": target.agent_id, "name": name, "slot": slot},
                )
            ).first()
            if in_flight is not None:
                await self._insert(conn, target, name, slot, "skipped")
                summary.skipped += 1
                return
            run_id = await self._insert(conn, target, name, slot, None)
        if run_id is None:
            # Another replica recorded this slot first.
            summary.lost += 1
            return

        agent = str(target.agent_id)
        slot_iso = slot.isoformat()
        turn = QueuedTurn(
            event_id=f"cron:{agent}:{name}:{slot_iso}",
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
            summary.failed += 1
            raise
        summary.admitted += 1

    async def one_pass(self, now: datetime | None = None) -> CronPassSummary:
        """Schedule every deployed agent's cron triggers for (watermark, now]."""

        now = now or self._clock()
        window_start, self._watermark = self._watermark, now
        summary = CronPassSummary()
        for target in await self._targets():
            try:
                for trigger in await self._cron_triggers(target):
                    name, schedule, prompt = (
                        trigger.get("name"),
                        trigger.get("schedule"),
                        trigger.get("prompt"),
                    )
                    if not name or not schedule or not prompt:
                        continue
                    zone = trigger.get("timezone") or "UTC"
                    slots = resolve_slots(str(schedule), str(zone), window_start, now)
                    if not slots:
                        continue
                    await self._admit(target, trigger, slots[-1], now, summary)
            except Exception:
                # Ends with this agent. Aborting would leave every later agent
                # unscheduled, and the order is arbitrary.
                summary.failed += 1
                logger.exception(
                    "cron scheduling raised for agent=%s; continuing with the rest",
                    target.agent_name,
                )

        log = logger.info if summary.did_work else logger.debug
        log(
            "cron pass: %d admitted, %d blocked, %d skipped, %d failed, %d lost",
            summary.admitted,
            summary.blocked,
            summary.skipped,
            summary.failed,
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
