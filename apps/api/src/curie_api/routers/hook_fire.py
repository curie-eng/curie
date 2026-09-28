"""Test-fire one declared cron hook, bypassing the schedule only (#2932).

A fire still claims a ``hook_runs`` row and still skips when that hook already
has a run in flight. The queued turn is the same shape the scheduler enqueues,
so the worker settles the row when the turn ends.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from aci_protocol import HookRunRef, QueuedTurn, ReplyHandle, TurnSource
from channel_protocol import hook_conversation_id
from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from ..auth import require_api_key
from ..db import SCHEMA
from ..deps import SessionDep, StoreDep
from ..schemas import HookFireOut, ScheduleOutcome
from .schedules import _read_triggers, _resolve_agent

router = APIRouter(dependencies=[Depends(require_api_key)])

_DEFAULT_USD = 10.0
_DEFAULT_TOKENS = 100_000
_OUTCOMES: dict[str, ScheduleOutcome] = {
    "ran": "ran",
    "deferred": "deferred",
    "skipped": "skipped",
    "blocked": "blocked",
    "reclaimed": "reclaimed",
    "failed": "failed",
}

_IN_FORCE_SQL = """
SELECT DISTINCT ON (a.id)
       a.id AS agent_id,
       a.name AS agent_name,
       v.id AS version_id,
       v.bundle_ref AS bundle_ref,
       a.max_usd_per_day AS max_usd_per_day,
       a.max_output_tokens_per_run AS max_output_tokens_per_run
FROM {schema}.agents a
JOIN {schema}.deployments d ON d.agent_id = a.id AND d.status = 'active'
JOIN {schema}.agent_versions v ON v.id = d.version_id AND v.agent_id = a.id
WHERE a.id = :agent_id
ORDER BY a.id, (d.environment = 'prod') DESC, d.deployed_at DESC, d.id DESC
"""

_BINDINGS_SQL = """
SELECT kind, address, endpoint, adapter
FROM {schema}.agent_channels
WHERE agent_id = :agent_id AND address = :address
"""

_LOCK_SQL = """
SELECT pg_advisory_xact_lock(hashtextextended(CAST(:agent_id AS text) || ':' || :name, 0))
"""

_IN_FLIGHT_SQL = """
SELECT 1 FROM {schema}.hook_runs
WHERE agent_id = :agent_id AND name = :name AND outcome IS NULL
LIMIT 1
"""

_INSERT_SQL = """
INSERT INTO {schema}.hook_runs
       (id, agent_id, name, slot_utc, version_id, outcome, started_at, ended_at)
VALUES (:id, :agent_id, :name, :slot, :version_id, CAST(:outcome AS text), now(),
        CASE WHEN :terminal THEN now() END)
RETURNING id, slot_utc, outcome, started_at, ended_at
"""

_FAIL_SQL = """
UPDATE {schema}.hook_runs
SET outcome = 'failed', ended_at = now()
WHERE id = :id AND outcome IS NULL
RETURNING id, slot_utc, outcome, started_at, ended_at
"""

_GET_SQL = """
SELECT r.id, r.slot_utc, r.outcome, r.started_at, r.ended_at, a.name AS agent_name
FROM {schema}.hook_runs r
JOIN {schema}.agents a ON a.id = r.agent_id
WHERE r.id = :id AND r.agent_id = :agent_id AND r.name = :name
"""


def _sql(statement: str) -> Any:
    return text(statement.format(schema=SCHEMA))


def _cron(raw: list[Any], name: str) -> dict[str, Any] | None:
    for item in raw:
        if not isinstance(item, dict) or item.get("type") != "cron":
            continue
        declared = item.get("name")
        if isinstance(declared, str) and declared.strip() == name:
            return item
    return None


def _budget_spent(usd: Any, tokens: Any) -> bool:
    spend = _DEFAULT_USD if usd is None else float(usd)
    limit = _DEFAULT_TOKENS if tokens is None else int(tokens)
    return spend <= 0 or limit <= 0


def _record(
    *,
    agent_id: uuid.UUID,
    agent: str,
    name: str,
    row: Any,
) -> HookFireOut:
    outcome = row["outcome"]
    return HookFireOut(
        id=row["id"],
        agent_id=agent_id,
        agent=agent,
        name=name,
        trigger="cron",
        slot_utc=row["slot_utc"],
        outcome=None if outcome is None else _OUTCOMES[str(outcome)],
        started_at=row["started_at"],
        ended_at=row["ended_at"],
    )


async def _insert(
    session: AsyncSession,
    *,
    agent_id: uuid.UUID,
    version_id: uuid.UUID,
    name: str,
    outcome: str | None,
) -> Any:
    slot = datetime.now(UTC)
    await session.execute(_sql(_LOCK_SQL), {"agent_id": str(agent_id), "name": name})
    if outcome is None:
        busy = (
            await session.execute(_sql(_IN_FLIGHT_SQL), {"agent_id": agent_id, "name": name})
        ).first()
        if busy is not None:
            outcome = "skipped"
    row = (
        (
            await session.execute(
                _sql(_INSERT_SQL),
                {
                    "id": uuid.uuid4(),
                    "agent_id": agent_id,
                    "name": name,
                    "slot": slot,
                    "version_id": version_id,
                    "outcome": outcome,
                    "terminal": outcome is not None,
                },
            )
        )
        .mappings()
        .one()
    )
    return outcome, row


@router.post("/agents/{agent_id}/hooks/{name}/fire", response_model=HookFireOut)
async def fire_hook(
    request: Request,
    session: SessionDep,
    store: StoreDep,
    agent_id: str,
    name: str,
) -> HookFireOut:
    """Run one named cron hook now and return its run record."""

    selected = await _resolve_agent(session, agent_id)
    deployment = (
        (await session.execute(_sql(_IN_FORCE_SQL), {"agent_id": selected.id})).mappings().first()
    )
    if deployment is None or deployment["bundle_ref"] is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent has no in-force bundle")
    try:
        data = await store.get(str(deployment["bundle_ref"]))
        declared = await run_in_threadpool(_read_triggers, data)
    except Exception as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, "stored bundle could not be read") from exc
    trigger = _cron(declared, name)
    if trigger is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "hook is not declared")
    prompt = trigger.get("prompt")
    if not isinstance(prompt, str) or not prompt.strip():
        raise HTTPException(status.HTTP_404_NOT_FOUND, "hook is not declared")

    killed = await request.app.state.kill_switch.is_killed(selected.id)
    terminal: str | None = None
    handle: ReplyHandle | None = None
    if killed or _budget_spent(
        deployment["max_usd_per_day"], deployment["max_output_tokens_per_run"]
    ):
        terminal = "blocked"
    else:
        address = trigger.get("target")
        if isinstance(address, str) and address.strip():
            bindings = (
                (
                    await session.execute(
                        _sql(_BINDINGS_SQL),
                        {"agent_id": selected.id, "address": address.strip()},
                    )
                )
                .mappings()
                .all()
            )
            if len(bindings) != 1:
                terminal = "failed"
            else:
                binding = bindings[0]
                handle = ReplyHandle(
                    kind=binding["kind"],
                    channel=binding["address"],
                    placeholder=None,
                    endpoint=binding["endpoint"],
                    adapter=binding["adapter"],
                )

    await session.commit()
    async with session.begin():
        outcome, row = await _insert(
            session,
            agent_id=selected.id,
            version_id=deployment["version_id"],
            name=name,
            outcome=terminal,
        )
    record = _record(agent_id=selected.id, agent=selected.name, name=name, row=row)
    if outcome is not None:
        return record

    slot_iso = row["slot_utc"].isoformat()
    turn = QueuedTurn(
        event_id=f"cron:{selected.id}:{name}:{slot_iso}",
        conversation_id=hook_conversation_id(selected.id, name),
        author=f"cron:{name}",
        text=prompt.strip(),
        source=TurnSource.CRON,
        reply_handle=handle,
        received_at=datetime.now(UTC).isoformat(),
        hook_run=HookRunRef(agent_id=str(selected.id), name=name, slot_utc=slot_iso),
    )
    try:
        await request.app.state.resume_queue.enqueue(turn)
    except Exception as exc:
        async with session.begin():
            failed = (await session.execute(_sql(_FAIL_SQL), {"id": row["id"]})).mappings().one()
        record = _record(agent_id=selected.id, agent=selected.name, name=name, row=failed)
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "hook fire could not be queued"
        ) from exc
    return record


@router.get(
    "/agents/{agent_id}/hooks/{name}/runs/{run_id}",
    response_model=HookFireOut,
)
async def get_hook_run(
    session: SessionDep,
    agent_id: str,
    name: str,
    run_id: uuid.UUID,
) -> HookFireOut:
    """Read one test-fire record, including a turn that has not settled."""

    selected = await _resolve_agent(session, agent_id)
    row = (
        (
            await session.execute(
                _sql(_GET_SQL),
                {"id": run_id, "agent_id": selected.id, "name": name},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "hook run not found")
    return _record(agent_id=selected.id, agent=row["agent_name"], name=name, row=row)
