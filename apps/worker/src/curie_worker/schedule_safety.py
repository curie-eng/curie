"""Per-agent daily spend and consecutive scheduled-turn failures.

Spend is keyed by agent and UTC day. Failures are keyed by agent and trigger
name, which covers bundle hooks and agent-created schedules named
``agent-schedule:<id>``. The third consecutive failure pauses that trigger
through ``schedule_controls`` and records one notice.
"""

from __future__ import annotations

import logging
import uuid
from contextvars import ContextVar
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from .eval.pricing import cost_usd

logger = logging.getLogger(__name__)

FAILURES_BEFORE_PAUSE = 3
_LOCK = """
SELECT pg_advisory_xact_lock(hashtextextended(CAST(:agent_id AS text) || ':' || :name, 0))
"""
_reported: ContextVar[tuple[int | None, int | None] | None] = ContextVar(
    "schedule_reported_usage", default=None
)
_notice_sink: Any = None


def remember_reported_usage(input_tokens: int | None, output_tokens: int | None) -> None:
    """Hold the turn's reported token counts until the hook run closes."""

    if input_tokens is None and output_tokens is None:
        return
    _reported.set((input_tokens, output_tokens))


def take_reported_usage() -> tuple[int | None, int | None] | None:
    """Return and clear the usage remembered on this task."""

    usage = _reported.get()
    _reported.set(None)
    return usage


def bind_notice_sink(sink: Any) -> None:
    """The worker reply sink that posts a pause notice to the trigger channel."""

    global _notice_sink
    _notice_sink = sink


async def spent_usd(
    connection: AsyncConnection, schema: str, agent_id: uuid.UUID, day: date
) -> Decimal:
    """Spend recorded for this agent on the UTC day."""

    value = (
        await connection.execute(
            text(
                f"SELECT cost_usd FROM {schema}.schedule_daily_spend "
                "WHERE agent_id = :agent_id AND utc_day = :utc_day"
            ),
            {"agent_id": agent_id, "utc_day": day},
        )
    ).scalar()
    if value is None:
        return Decimal(0)
    return Decimal(value)


async def add_spend(
    connection: AsyncConnection,
    schema: str,
    agent_id: uuid.UUID,
    day: date,
    amount: Decimal,
) -> None:
    """Add one scheduled run's cost to that UTC day."""

    if amount <= 0:
        return
    await connection.execute(
        text(
            f"INSERT INTO {schema}.schedule_daily_spend (agent_id, utc_day, cost_usd) "
            "VALUES (:agent_id, :utc_day, :cost_usd) "
            "ON CONFLICT (agent_id, utc_day) DO UPDATE "
            "SET cost_usd = schedule_daily_spend.cost_usd + EXCLUDED.cost_usd"
        ),
        {"agent_id": agent_id, "utc_day": day, "cost_usd": amount},
    )


async def reset_streak(
    connection: AsyncConnection, schema: str, agent_id: uuid.UUID, name: str
) -> None:
    """A resume or a successful run clears the consecutive failure count."""

    await connection.execute(
        text(
            f"INSERT INTO {schema}.schedule_failure_streaks (agent_id, name, consecutive) "
            "VALUES (:agent_id, :name, 0) "
            "ON CONFLICT (agent_id, name) DO UPDATE SET consecutive = 0"
        ),
        {"agent_id": agent_id, "name": name},
    )


def _notice_body(name: str, agent_name: str) -> str:
    return (
        f"Schedule {name} for agent {agent_name} paused after "
        f"{FAILURES_BEFORE_PAUSE} consecutive failures. "
        f"Resume it with: curie schedules resume {agent_name} {name}"
    )


async def account_closed_run(
    connection: AsyncConnection,
    schema: str,
    *,
    agent_id: uuid.UUID,
    name: str,
    outcome: str,
    when: datetime,
    model: str | None,
    input_tokens: int | None,
    output_tokens: int | None,
) -> str | None:
    """Record cost and the failure streak. Return a notice body when pausing.

    The caller holds the transaction. A success resets the streak. Only a
    ``failed`` outcome increments it. The third failure pauses the trigger and
    inserts exactly one notice for that pause generation.
    """

    priced = cost_usd(model, input_tokens, output_tokens)
    if priced is not None and priced > 0:
        await add_spend(
            connection, schema, agent_id, when.astimezone(UTC).date(), Decimal(str(priced))
        )
    await connection.execute(text(_LOCK), {"agent_id": str(agent_id), "name": name})
    if outcome == "ran":
        await reset_streak(connection, schema, agent_id, name)
        return None
    if outcome != "failed":
        return None
    current = (
        await connection.execute(
            text(
                f"SELECT consecutive FROM {schema}.schedule_failure_streaks "
                "WHERE agent_id = :agent_id AND name = :name"
            ),
            {"agent_id": agent_id, "name": name},
        )
    ).scalar()
    count = int(current or 0) + 1
    await connection.execute(
        text(
            f"INSERT INTO {schema}.schedule_failure_streaks (agent_id, name, consecutive) "
            "VALUES (:agent_id, :name, :consecutive) "
            "ON CONFLICT (agent_id, name) DO UPDATE SET consecutive = EXCLUDED.consecutive"
        ),
        {"agent_id": agent_id, "name": name, "consecutive": count},
    )
    if count != FAILURES_BEFORE_PAUSE:
        return None
    await connection.execute(
        text(
            f"INSERT INTO {schema}.schedule_controls (agent_id, name, paused_at, resume_from) "
            "VALUES (:agent_id, :name, now(), NULL) "
            "ON CONFLICT (agent_id, name) DO UPDATE "
            "SET paused_at = COALESCE(schedule_controls.paused_at, now()), "
            "resume_from = schedule_controls.resume_from, "
            "generation = schedule_controls.generation "
            "+ CASE WHEN schedule_controls.paused_at IS NULL THEN 1 ELSE 0 END"
        ),
        {"agent_id": agent_id, "name": name},
    )
    generation = (
        await connection.execute(
            text(
                f"SELECT generation FROM {schema}.schedule_controls "
                "WHERE agent_id = :agent_id AND name = :name"
            ),
            {"agent_id": agent_id, "name": name},
        )
    ).scalar()
    agent_name = (
        await connection.execute(
            text(f"SELECT name FROM {schema}.agents WHERE id = :agent_id"),
            {"agent_id": agent_id},
        )
    ).scalar()
    body = _notice_body(name, str(agent_name or agent_id))
    inserted = (
        await connection.execute(
            text(
                f"INSERT INTO {schema}.schedule_pause_notices "
                "(agent_id, name, generation, body) "
                "VALUES (:agent_id, :name, :generation, :body) "
                "ON CONFLICT (agent_id, name, generation) DO NOTHING "
                "RETURNING generation"
            ),
            {
                "agent_id": agent_id,
                "name": name,
                "generation": int(generation or 0),
                "body": body,
            },
        )
    ).first()
    if inserted is None:
        return None
    return body


async def post_notice(
    connection: AsyncConnection, schema: str, agent_id: uuid.UUID, body: str
) -> None:
    """Post the pause notice to one of the agent's channels when a sink is bound."""

    sink = _notice_sink
    if sink is None:
        return
    found = await connection.execute(
        text(
            f"SELECT kind, address, endpoint, adapter FROM {schema}.agent_channels "
            "WHERE agent_id = :agent_id ORDER BY kind, address LIMIT 1"
        ),
        {"agent_id": agent_id},
    )
    row = found.mappings().first()
    if row is None:
        logger.warning("schedule pause notice has no channel for agent %s", agent_id)
        return
    from channel_protocol.models import MESSAGE_VERSION, OutboundMessage
    from channel_protocol.reply import ReplyPost, ReplyTarget

    from .reply_sink import TargetRoute

    event = ReplyPost(
        version="1.0",
        event="reply.post",
        target=ReplyTarget(
            kind=row["kind"],
            address=row["address"],
            conversation_id=None,
            reply_ref=None,
        ),
        message=OutboundMessage(version=MESSAGE_VERSION, text=body),
        requested_by="schedule-safety",
    )
    try:
        await sink.emit(
            event,
            route=TargetRoute(endpoint=row["endpoint"], adapter=row["adapter"]),
            best_effort_unreachable=True,
        )
    except Exception:
        logger.exception("schedule pause notice delivery failed")
