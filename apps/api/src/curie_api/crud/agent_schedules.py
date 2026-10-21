"""Database access for agent-created schedules (ADR 0238).

One lock order everywhere a schedule changes: the per (agent, name)
transaction advisory lock the worker's admission takes (``lock``), then the
schedule row (``get(..., for_update=True)``). A create, which has no row yet,
holds a per-agent advisory lock instead, so the idempotency lookup and the
active count cannot race another create for the same agent. Every change
appends to ``agent_schedule_revisions`` through ``log``.
"""

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from ..agent_schedule_time import local_label, next_run
from ..db import SCHEMA
from ..models import (
    Agent,
    AgentChannel,
    AgentSchedule,
    AgentScheduleRevision,
    AgentScheduleRun,
    HookRun,
)
from ..schemas.schedules import (
    AgentScheduleDestinationOut,
    AgentScheduleOut,
    AgentScheduleState,
    HookRunReason,
    ScheduleOutcome,
)

NAME_PREFIX = "agent-schedule:"
ACTIVE = "active"
CANCELLED = "cancelled"

_LOCK_SQL = text(
    "SELECT pg_advisory_xact_lock(hashtextextended(CAST(:agent_id AS text) || ':' || :name, 0))"
)
# The create lock's name has no ``agent-schedule:`` prefix, so it never
# collides with one schedule's admission lock.
_CREATE_LOCK_NAME = "agent-schedules:create"


@dataclass(frozen=True)
class Stamp:
    """Provenance of a change, taken from the credential, never the body."""

    author: str
    source: str
    conversation: str | None = None
    turn: str | None = None


OPERATOR = Stamp("operator", "operator")


def schedule_name(schedule_id: uuid.UUID) -> str:
    """The trigger name the scheduler, hook_runs and schedule_controls use."""

    return f"{NAME_PREFIX}{schedule_id}"


def parse_name(name: str) -> uuid.UUID | None:
    """The schedule id in an ``agent-schedule:<id>`` name, else None. Only the
    canonical lowercase hyphenated spelling names a schedule."""

    if not name.startswith(NAME_PREFIX):
        return None
    raw = name.removeprefix(NAME_PREFIX)
    try:
        parsed = uuid.UUID(raw)
    except ValueError:
        return None
    return parsed if str(parsed) == raw else None


async def lock(session: AsyncSession, agent_id: uuid.UUID, name: str) -> None:
    await session.execute(_LOCK_SQL, {"agent_id": str(agent_id), "name": name})


async def lock_creates(session: AsyncSession, agent_id: uuid.UUID) -> None:
    await lock(session, agent_id, _CREATE_LOCK_NAME)


async def lock_schedule(
    session: AsyncSession, agent_id: uuid.UUID, schedule_id: uuid.UUID
) -> AgentSchedule | None:
    """Take the admission lock, then the row lock, and return the fresh row."""

    await lock(session, agent_id, schedule_name(schedule_id))
    return await get(session, agent_id, schedule_id, for_update=True)


async def agent_for_share(session: AsyncSession, agent_id: uuid.UUID) -> Agent | None:
    """The agent row, share-locked so a toggle-off waits for this change."""

    agent: Agent | None = await session.scalar(
        select(Agent)
        .where(Agent.id == agent_id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    return agent


async def get(
    session: AsyncSession, agent_id: uuid.UUID, schedule_id: uuid.UUID, *, for_update: bool = False
) -> AgentSchedule | None:
    query = select(AgentSchedule).where(
        AgentSchedule.id == schedule_id, AgentSchedule.agent_id == agent_id
    )
    if for_update:
        query = query.with_for_update().execution_options(populate_existing=True)
    found: AgentSchedule | None = await session.scalar(query)
    return found


async def by_key(session: AsyncSession, agent_id: uuid.UUID, key: str) -> AgentSchedule | None:
    found: AgentSchedule | None = await session.scalar(
        select(AgentSchedule).where(
            AgentSchedule.agent_id == agent_id, AgentSchedule.idempotency_key == key
        )
    )
    return found


async def active_count(session: AsyncSession, agent_id: uuid.UUID) -> int:
    """Stored-active schedules, paused ones included."""

    count = await session.scalar(
        select(func.count())
        .select_from(AgentSchedule)
        .where(AgentSchedule.agent_id == agent_id, AgentSchedule.state == ACTIVE)
    )
    return int(count or 0)


async def listed(
    session: AsyncSession, agent_id: uuid.UUID | None, *, include_all: bool
) -> Sequence[AgentSchedule]:
    """Schedules oldest first; only stored-active ones unless ``include_all``.
    ``agent_id`` None lists every agent's."""

    query = select(AgentSchedule)
    if agent_id is not None:
        query = query.where(AgentSchedule.agent_id == agent_id)
    if not include_all:
        query = query.where(AgentSchedule.state == ACTIVE)
    rows = await session.scalars(
        query.order_by(AgentSchedule.agent_id, AgentSchedule.created_at, AgentSchedule.id)
    )
    return rows.all()


async def destinations(
    session: AsyncSession, agent_id: uuid.UUID, kind: str, address: str
) -> Sequence[AgentChannel]:
    rows = await session.scalars(
        select(AgentChannel).where(
            AgentChannel.agent_id == agent_id,
            AgentChannel.kind == kind,
            AgentChannel.address == address,
        )
    )
    return rows.all()


async def revisions(
    session: AsyncSession, schedule_id: uuid.UUID
) -> Sequence[AgentScheduleRevision]:
    rows = await session.scalars(
        select(AgentScheduleRevision)
        .where(AgentScheduleRevision.schedule_id == schedule_id)
        .order_by(AgentScheduleRevision.seq)
    )
    return rows.all()


async def revision_config(
    session: AsyncSession, schedule_id: uuid.UUID, number: int
) -> AgentScheduleRevision | None:
    """The first log entry of configuration revision ``number``."""

    found: AgentScheduleRevision | None = await session.scalar(
        select(AgentScheduleRevision)
        .where(
            AgentScheduleRevision.schedule_id == schedule_id,
            AgentScheduleRevision.revision == number,
        )
        .order_by(AgentScheduleRevision.seq)
        .limit(1)
    )
    return found


async def has_fired(session: AsyncSession, schedule: AgentSchedule) -> bool:
    """Whether the scheduler has a run of this schedule other than one skipped
    because the schedule changed (manual test fires never count)."""

    found = await session.scalar(
        select(HookRun.id)
        .where(
            HookRun.agent_id == schedule.agent_id,
            HookRun.name == schedule_name(schedule.id),
            HookRun.source == "schedule",
            ~(
                (HookRun.outcome == "skipped")
                & (func.coalesce(HookRun.reason, "") == "schedule_changed")
            ),
        )
        .limit(1)
    )
    return found is not None


async def settle_deferred(session: AsyncSession, schedule: AgentSchedule) -> None:
    """Close every deferred run of a cancelled schedule as schedule_changed, so
    the deferred-retry path never fires it."""

    await session.execute(
        update(HookRun)
        .where(
            HookRun.agent_id == schedule.agent_id,
            HookRun.name == schedule_name(schedule.id),
            HookRun.outcome == "deferred",
        )
        .values(outcome="skipped", reason="schedule_changed", ended_at=func.now())
    )


async def is_paused(session: AsyncSession, schedule: AgentSchedule) -> bool:
    paused = await session.scalar(
        text(
            f"SELECT paused_at IS NOT NULL FROM {SCHEMA}.schedule_controls "
            "WHERE agent_id = :agent_id AND name = :name"
        ),
        {"agent_id": schedule.agent_id, "name": schedule_name(schedule.id)},
    )
    return bool(paused)


def read_state(schedule: AgentSchedule, paused: bool) -> AgentScheduleState:
    if schedule.state == ACTIVE:
        return "paused" if paused else "active"
    return cast(AgentScheduleState, schedule.state)


async def log(
    session: AsyncSession,
    schedule: AgentSchedule,
    *,
    action: str,
    state: AgentScheduleState,
    stamp: Stamp,
) -> AgentScheduleRevision:
    """Append the next log entry, recording the schedule's configuration as
    it now stands. A new schedule must already be flushed."""

    seq = await session.scalar(
        select(func.coalesce(func.max(AgentScheduleRevision.seq), 0)).where(
            AgentScheduleRevision.schedule_id == schedule.id
        )
    )
    entry = AgentScheduleRevision(
        schedule_id=schedule.id,
        seq=int(seq or 0) + 1,
        revision=schedule.revision,
        action=action,
        state=state,
        prompt=schedule.prompt,
        cron=schedule.cron,
        run_at=schedule.run_at,
        timezone=schedule.timezone,
        author=stamp.author,
        source=stamp.source,
        conversation=stamp.conversation,
        turn=stamp.turn,
        created_at=datetime.now(UTC),
    )
    session.add(entry)
    await session.flush()
    return entry


def snapshot(schedule: AgentSchedule, hook_run_id: uuid.UUID) -> AgentScheduleRun:
    """The immutable record of what one hook run fires."""

    return AgentScheduleRun(
        hook_run_id=hook_run_id,
        schedule_id=schedule.id,
        agent_id=schedule.agent_id,
        revision=schedule.revision,
        prompt=schedule.prompt,
        cron=schedule.cron,
        run_at=schedule.run_at,
        timezone=schedule.timezone,
        destination_kind=schedule.destination_kind,
        destination_address=schedule.destination_address,
        destination_adapter=schedule.destination_adapter,
        created_at=datetime.now(UTC),
    )


_OUTCOMES: dict[str, ScheduleOutcome] = {
    "ran": "ran",
    "deferred": "deferred",
    "skipped": "skipped",
    "blocked": "blocked",
    "reclaimed": "reclaimed",
    "failed": "failed",
}


async def views(
    session: AsyncSession,
    schedules: Sequence[AgentSchedule],
    *,
    platform: bool,
    now: datetime | None = None,
) -> list[AgentScheduleOut]:
    """Read-back JSON for ``schedules``: paused state, failure streak, newest
    scheduled run and next run. ``conversation`` and ``turn`` only when
    ``platform``; otherwise they are left unset for the routes to drop."""

    if not schedules:
        return []
    at = now or datetime.now(UTC)
    agent_ids = sorted({row.agent_id for row in schedules})
    names = [schedule_name(row.id) for row in schedules]
    params: dict[str, Any] = {"agents": agent_ids, "names": names}
    agent_names = {
        row.id: row.name
        for row in (
            await session.execute(select(Agent.id, Agent.name).where(Agent.id.in_(agent_ids)))
        )
    }
    paused = {
        (row[0], row[1])
        for row in await session.execute(
            text(
                f"SELECT agent_id, name FROM {SCHEMA}.schedule_controls "
                "WHERE agent_id = ANY(:agents) AND name = ANY(:names) AND paused_at IS NOT NULL"
            ),
            params,
        )
    }
    streaks = {
        (row[0], row[1]): int(row[2])
        for row in await session.execute(
            text(
                f"SELECT agent_id, name, consecutive FROM {SCHEMA}.schedule_failure_streaks "
                "WHERE agent_id = ANY(:agents) AND name = ANY(:names)"
            ),
            params,
        )
    }
    latest = {
        (row[0], row[1]): (row[2], row[3], row[4])
        for row in await session.execute(
            text(
                f"SELECT DISTINCT ON (agent_id, name) agent_id, name, slot_utc, outcome, reason "
                f"FROM {SCHEMA}.hook_runs WHERE agent_id = ANY(:agents) AND name = ANY(:names) "
                "AND source = 'schedule' ORDER BY agent_id, name, slot_utc DESC"
            ),
            params,
        )
    }
    out: list[AgentScheduleOut] = []
    for row in schedules:
        key = (row.agent_id, schedule_name(row.id))
        is_paused_now = key in paused
        state = read_state(row, is_paused_now)
        upcoming = None
        if state == "active":
            upcoming = next_run(row.cron, row.run_at, row.timezone, max(at, row.effective_from))
        fire = latest.get(key)
        reason = None if fire is None else fire[2]
        extra: dict[str, Any] = (
            {"conversation": row.created_conversation, "turn": row.created_turn} if platform else {}
        )
        out.append(
            AgentScheduleOut(
                id=row.id,
                agent=agent_names.get(row.agent_id, ""),
                agent_id=row.agent_id,
                revision=row.revision,
                state=state,
                cron=row.cron,
                run_at=row.run_at,
                timezone=row.timezone,
                prompt=row.prompt,
                created_by=row.created_by,
                created_source=cast(Any, row.created_source),
                destination=AgentScheduleDestinationOut(
                    kind=row.destination_kind,
                    address=row.destination_address,
                    adapter=row.destination_adapter,
                ),
                next_run_at=upcoming,
                next_run_local=None if upcoming is None else local_label(upcoming, row.timezone),
                paused=is_paused_now,
                consecutive_failures=streaks.get(key, 0),
                last_fire_at=None if fire is None else fire[0],
                last_outcome=None if fire is None else _OUTCOMES.get(str(fire[1] or "")),
                last_reason=cast(HookRunReason | None, reason),
                created_at=row.created_at,
                updated_at=row.updated_at,
                updated_by=row.updated_by,
                **extra,
            )
        )
    return out
