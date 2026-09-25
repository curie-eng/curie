"""List each in-force cron hook and the newest slot recorded for it (#2933).

The ranking matches the worker cron loop: prod outranks dev, then the newest
active deployment. Declarations are read from that version's bundle. ``hook_runs``
only supplies the newest slot for a name the bundle still declares.
"""

from __future__ import annotations

import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from .. import bundles, crud
from ..auth import require_api_key
from ..config import get_settings
from ..db import SCHEMA
from ..deps import SessionDep, StoreDep
from ..schemas import AgentSchedulesOut, ScheduleHookOut, ScheduleListOut, ScheduleOutcome
from ..storage import ObjectStore

_OUTCOMES: dict[str, ScheduleOutcome] = {
    "ran": "ran",
    "deferred": "deferred",
    "skipped": "skipped",
    "blocked": "blocked",
    "reclaimed": "reclaimed",
    "failed": "failed",
}

router = APIRouter(
    prefix="/schedules",
    tags=["schedules"],
    dependencies=[Depends(require_api_key)],
)

_UNREADABLE = "stored bundle could not be read"

_IN_FORCE_SQL = """
SELECT DISTINCT ON (a.id)
       a.id AS agent_id,
       a.name AS agent_name,
       v.bundle_ref AS bundle_ref
FROM {schema}.agents a
JOIN {schema}.deployments d ON d.agent_id = a.id AND d.status = 'active'
JOIN {schema}.agent_versions v ON v.id = d.version_id AND v.agent_id = a.id
{where}
ORDER BY a.id, (d.environment = 'prod') DESC, d.deployed_at DESC, d.id DESC
"""

_LATEST_SQL = """
SELECT DISTINCT ON (name)
       name, slot_utc, outcome
FROM {schema}.hook_runs
WHERE agent_id = :agent_id
ORDER BY name, slot_utc DESC
"""


def _cron_hooks(raw: list[Any]) -> list[tuple[str, str, str]]:
    found: list[tuple[str, str, str]] = []
    for item in raw:
        if not isinstance(item, dict) or item.get("type") != "cron":
            continue
        name = item.get("name")
        schedule = item.get("schedule")
        if not isinstance(name, str) or not name.strip():
            continue
        if not isinstance(schedule, str) or not schedule.strip():
            continue
        timezone = item.get("timezone")
        zone = timezone.strip() if isinstance(timezone, str) and timezone.strip() else "UTC"
        found.append((name.strip(), schedule.strip(), zone))
    found.sort(key=lambda row: row[0])
    return found


def _read_triggers(data: bytes) -> list[Any]:
    settings = get_settings()
    with tempfile.TemporaryDirectory() as tmp:
        bundles.extract_and_validate(
            data,
            Path(tmp),
            max_uncompressed_bytes=settings.bundle_max_uncompressed_bytes,
            max_compression_ratio=settings.bundle_max_compression_ratio,
            max_members=settings.bundle_max_members,
        )
        declared = bundles.read_manifest_triggers(Path(tmp))
    return declared if isinstance(declared, list) else []


async def _resolve_agent(session: AsyncSession, raw: str) -> Any:
    parsed: uuid.UUID | None
    try:
        parsed = uuid.UUID(raw)
    except ValueError:
        parsed = None
    agent = await crud.get_agent(session, parsed) if parsed is not None else None
    if agent is None:
        agent = await crud.get_agent_by_name(session, raw)
    if agent is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    return agent


async def _in_force(session: AsyncSession, agent_id: uuid.UUID | None) -> list[dict[str, Any]]:
    where = "" if agent_id is None else "WHERE a.id = :agent_id"
    statement = text(_IN_FORCE_SQL.format(schema=SCHEMA, where=where))
    params: dict[str, Any] = {} if agent_id is None else {"agent_id": agent_id}
    rows = (await session.execute(statement, params)).mappings().all()
    return [dict(row) for row in rows]


async def _latest(
    session: AsyncSession, agent_id: uuid.UUID
) -> dict[str, tuple[datetime, str | None]]:
    statement = text(_LATEST_SQL.format(schema=SCHEMA))
    rows = (await session.execute(statement, {"agent_id": agent_id})).mappings().all()
    latest: dict[str, tuple[datetime, str | None]] = {}
    for row in rows:
        name = row["name"]
        if isinstance(name, str):
            latest[name] = (row["slot_utc"], row["outcome"])
    return latest


async def _hooks_for(
    store: ObjectStore,
    session: AsyncSession,
    agent_id: uuid.UUID,
    bundle_ref: str | None,
) -> tuple[list[ScheduleHookOut], str | None]:
    if bundle_ref is None:
        return [], None
    try:
        data = await store.get(bundle_ref)
        declared = await run_in_threadpool(_read_triggers, data)
    except Exception:
        return [], _UNREADABLE
    latest = await _latest(session, agent_id)
    hooks: list[ScheduleHookOut] = []
    for name, schedule, zone in _cron_hooks(declared):
        slot = latest.get(name)
        last_fire_at = None if slot is None else slot[0]
        last_outcome = None if slot is None else _OUTCOMES.get(slot[1] or "")
        hooks.append(
            ScheduleHookOut(
                name=name,
                trigger="cron",
                schedule=schedule,
                zone=zone,
                last_fire_at=last_fire_at,
                last_outcome=last_outcome,
            )
        )
    return hooks, None


@router.get("", response_model=ScheduleListOut)
async def list_schedules(
    session: SessionDep,
    store: StoreDep,
    agent: str | None = None,
) -> ScheduleListOut:
    """Cron hooks on each in-force deployment, newest slot first in the record."""

    selected = None if agent is None else await _resolve_agent(session, agent)
    rows = await _in_force(session, None if selected is None else selected.id)
    if selected is not None and not rows:
        return ScheduleListOut(
            schedules=[
                AgentSchedulesOut(
                    agent=selected.name,
                    agent_id=selected.id,
                    bundle_error=None,
                    hooks=[],
                )
            ]
        )
    listed: list[AgentSchedulesOut] = []
    for row in rows:
        hooks, error = await _hooks_for(store, session, row["agent_id"], row["bundle_ref"])
        listed.append(
            AgentSchedulesOut(
                agent=row["agent_name"],
                agent_id=row["agent_id"],
                bundle_error=error,
                hooks=hooks,
            )
        )
    listed.sort(key=lambda item: item.agent)
    return ScheduleListOut(schedules=listed)
