"""Test-fire one declared cron hook, bypassing the schedule only (#2932).

A fire still claims a ``hook_runs`` row and still skips when that hook already
has a run in flight. The queued turn is the same shape the scheduler enqueues,
so the worker settles the row when the turn ends.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any, cast, get_args

from aci_protocol import HookRunRef, QueuedTurn, ReplyHandle, TurnSource
from aci_protocol.turn import DEFAULT_IDENTITY, SLACK_KIND, route_identity
from channel_protocol import hook_conversation_id
from curie_protected_hooks.source_policy_sql import (
    SourceAgentNotFound,
    SourceGate,
    SourceGateContext,
    SourceGateInvalid,
    SourceSnapshotUnavailable,
    ensure_source_gate_live,
    read_source_snapshot,
)
from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from curie_api.routers.schedules import read_triggers, resolve_agent
from curie_api.schemas.schedules import HookFireOut, HookRunReason, ScheduleOutcome

from ..auth import require_api_key
from ..db import SCHEMA
from ..deps import SessionDep, StoreDep
from ..hook_partition import HOOK_NAME
from ..models import Agent

router = APIRouter(dependencies=[Depends(require_api_key)])
logger = logging.getLogger(__name__)

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
       (id, agent_id, name, slot_utc, version_id, source, outcome, reason, started_at, ended_at)
VALUES (:id, :agent_id, :name, :slot, :version_id, 'manual', CAST(:outcome AS text),
        CASE WHEN :terminal THEN :reason ELSE NULL END, now(),
        CASE WHEN :terminal THEN now() END)
RETURNING id, source, slot_utc, outcome, reason, started_at, ended_at
"""

_FAIL_SQL = """
UPDATE {schema}.hook_runs
SET outcome = 'failed', reason = 'enqueue_failed', ended_at = now()
WHERE id = :id AND outcome IS NULL
RETURNING id, source, slot_utc, outcome, reason, started_at, ended_at
"""

_GET_SQL = """
SELECT r.id, r.source, r.slot_utc, r.outcome, r.reason, r.started_at, r.ended_at,
       a.name AS agent_name
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


def _reason(value: Any) -> HookRunReason | None:
    if value is None:
        return None
    if value not in get_args(HookRunReason):
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "hook run reason is unknown")
    return cast(HookRunReason, value)


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
        source=row["source"],
        slot_utc=row["slot_utc"],
        outcome=None if outcome is None else _OUTCOMES[str(outcome)],
        reason=_reason(row["reason"]),
        started_at=row["started_at"],
        ended_at=row["ended_at"],
    )


@asynccontextmanager
async def _source_locked_agent(
    request: Request, session: AsyncSession, agent_id: uuid.UUID
) -> AsyncIterator[tuple[Agent, SourceGateContext]]:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    try:
        await session.rollback()
        gate = getattr(request.app.state, "source_gate", None)
        if not isinstance(gate, SourceGate):
            raise SourceSnapshotUnavailable("source_gate_unavailable")
        async with gate.hold(agent_id) as held:
            agent: Agent | None = await session.scalar(
                select(Agent).where(Agent.id == agent_id).execution_options(populate_existing=True)
            )
            if agent is None:
                raise HTTPException(404, "agent not found")
            yield agent, held
    except (SourceAgentNotFound, SourceGateInvalid, SourceSnapshotUnavailable, SQLAlchemyError):
        raise HTTPException(503, "authority_unavailable") from None


async def _require_ordinary_source(
    session: AsyncSession, held: SourceGateContext, name: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    if HOOK_NAME.fullmatch(name):
        snapshot = await read_source_snapshot(held, await session.connection(), name)
        if not snapshot.never_configured:
            raise HTTPException(503, snapshot.refusal_reason or "authority_unavailable")
        return
    await ensure_source_gate_live(held)
    presence = (
        await session.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM curie.hook_source_policies "
                "WHERE agent_id=:agent AND hook=:hook) AS policy, "
                "EXISTS (SELECT 1 FROM curie.hook_source_operations "
                "WHERE agent_id=:agent AND hook=:hook) AS history"
            ),
            {"agent": held.agent_id, "hook": name},
        )
    ).one()
    await ensure_source_gate_live(held)
    if presence.policy or presence.history:
        raise HTTPException(503, "authority_unavailable" if presence.policy else "pending_history")


async def _insert(
    session: AsyncSession,
    *,
    held: SourceGateContext,
    agent_id: uuid.UUID,
    version_id: uuid.UUID,
    name: str,
    outcome: str | None,
    reason: str | None = None,
) -> Any:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    slot = datetime.now(UTC)
    await session.execute(_sql(_LOCK_SQL), {"agent_id": str(agent_id), "name": name})
    if outcome is None:
        busy = (
            await session.execute(_sql(_IN_FLIGHT_SQL), {"agent_id": agent_id, "name": name})
        ).first()
        if busy is not None:
            outcome = "skipped"
            reason = "run_in_flight"
    if outcome is None:
        reason = None
    await ensure_source_gate_live(held)
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
                    "reason": reason,
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
    """Run one named cron hook now and return its run record.
    \f
    @spec PROTECTED-HOOK-SOURCE-2/10.
    """

    selected = await resolve_agent(session, agent_id)
    selected_id = uuid.UUID(str(selected.id))
    async with _source_locked_agent(request, session, selected_id) as (selected, held):
        deployment = (
            (await session.execute(_sql(_IN_FORCE_SQL), {"agent_id": selected.id}))
            .mappings()
            .first()
        )
        if deployment is None or deployment["bundle_ref"] is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "agent has no in-force bundle")
        try:
            data = await store.get(str(deployment["bundle_ref"]))
            declared = await run_in_threadpool(read_triggers, data)
        except Exception as exc:
            raise HTTPException(
                status.HTTP_409_CONFLICT, "stored bundle could not be read"
            ) from exc
        trigger = _cron(declared, name)
        if trigger is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "hook is not declared")
        prompt = trigger.get("prompt")
        if not isinstance(prompt, str) or not prompt.strip():
            raise HTTPException(status.HTTP_404_NOT_FOUND, "hook is not declared")

        await _require_ordinary_source(session, held, name)

        killed = await request.app.state.kill_switch.is_killed(selected.id)
        terminal: str | None = None
        reason: str | None = None
        handle: ReplyHandle | None = None
        if killed:
            terminal = "blocked"
            reason = "agent_killed"
        elif _budget_spent(deployment["max_usd_per_day"], deployment["max_output_tokens_per_run"]):
            terminal = "blocked"
            reason = "budget_exhausted"
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
                candidates = list(bindings)
                slack_identities = len(candidates) > 1 and all(
                    b["kind"] == SLACK_KIND for b in candidates
                )
                if slack_identities:
                    # ADR-0168 decision 3: a trigger names an address, never an
                    # identity; several of this agent's identities there mean its
                    # default Slack one, as the cron loop reads the same target.
                    candidates = [
                        b
                        for b in candidates
                        if b["kind"] == SLACK_KIND
                        and route_identity(b["kind"], b["adapter"]) == DEFAULT_IDENTITY
                    ]
                if len(candidates) != 1:
                    terminal = "failed"
                    reason = "target_unbound"
                else:
                    binding = candidates[0]
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
                held=held,
                agent_id=selected.id,
                version_id=deployment["version_id"],
                name=name,
                outcome=terminal,
                reason=reason,
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
            await ensure_source_gate_live(held)
            await request.app.state.resume_queue.enqueue(turn)
        except Exception as exc:
            try:
                async with session.begin():
                    await session.connection()
                    await ensure_source_gate_live(held)
                    failed = (
                        (await session.execute(_sql(_FAIL_SQL), {"id": row["id"]})).mappings().one()
                    )
                record = _record(agent_id=selected.id, agent=selected.name, name=name, row=failed)
            except Exception:  # noqa: BLE001 - source cleanup diagnostics may contain credentials
                # The committed claim remains recoverable when cleanup has no authority.
                logger.warning("hook_fire_cleanup_unavailable")
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

    selected = await resolve_agent(session, agent_id)
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
