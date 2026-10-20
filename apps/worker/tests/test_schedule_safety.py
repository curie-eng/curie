"""Daily spend admission and consecutive-failure pause."""

from __future__ import annotations

import asyncio
import importlib.util
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from curie_worker.schedule_safety import account_closed_run, add_spend, reset_streak
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

_cron_path = Path(__file__).with_name("test_source_cron_integration.py")
_cron_spec = importlib.util.spec_from_file_location("test_source_cron_integration", _cron_path)
assert _cron_spec is not None and _cron_spec.loader is not None
_cron = importlib.util.module_from_spec(_cron_spec)
_cron_spec.loader.exec_module(_cron)
Campaign = _cron.Campaign
guard_setup = _cron.guard_setup

worker_templates = guard_setup.worker_templates
worker_db = guard_setup.worker_db


@pytest.fixture
def campaign(worker_db: Any) -> Any:
    support, _, url = worker_db
    actual = Campaign(support, url)
    try:
        yield actual
    finally:
        actual.close()


async def _upgrade(campaign: Campaign) -> None:
    engine = create_async_engine(campaign.url)
    statements = [
            """
            CREATE TABLE IF NOT EXISTS curie.schedule_daily_spend (
                agent_id uuid NOT NULL REFERENCES curie.agents(id) ON DELETE CASCADE,
                utc_day date NOT NULL,
                cost_usd numeric(14,6) NOT NULL CHECK (cost_usd >= 0),
                PRIMARY KEY (agent_id, utc_day)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS curie.schedule_failure_streaks (
                agent_id uuid NOT NULL REFERENCES curie.agents(id) ON DELETE CASCADE,
                name varchar NOT NULL,
                consecutive integer NOT NULL CHECK (consecutive >= 0),
                PRIMARY KEY (agent_id, name)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS curie.schedule_pause_notices (
                agent_id uuid NOT NULL REFERENCES curie.agents(id) ON DELETE CASCADE,
                name varchar NOT NULL,
                generation bigint NOT NULL,
                body text NOT NULL,
                PRIMARY KEY (agent_id, name, generation)
            )
            """,
    ]
    try:
        async with engine.begin() as connection:
            for statement in statements:
                await connection.execute(text(statement))
    finally:
        await engine.dispose()


async def _reasons(engine: Any, agent: uuid.UUID) -> list[tuple[Any, Any]]:
    async with engine.connect() as connection:
        rows = (
            await connection.execute(
                text(
                    "SELECT outcome, reason FROM curie.hook_runs "
                    "WHERE agent_id = :agent ORDER BY slot_utc"
                ),
                {"agent": agent},
            )
        ).all()
    return [(row[0], row[1]) for row in rows]


async def _close_open(engine: Any, agent: uuid.UUID) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "UPDATE curie.hook_runs SET outcome = 'ran', ended_at = now() "
                "WHERE agent_id = :agent AND outcome IS NULL"
            ),
            {"agent": agent},
        )


def test_daily_budget_skips_then_admits(campaign: Campaign) -> None:
    async def scenario() -> None:
        await _upgrade(campaign)
        async with campaign.runtime() as (loop, _, work, observer, _redis):
            async with work.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE curie.agents SET max_usd_per_day = :cap, model = :model "
                        "WHERE id = :id"
                    ),
                    {"cap": 0.000001, "model": "anthropic/claude-haiku-5.5", "id": campaign.agent},
                )
            first = await loop.one_pass(campaign.now)
            assert first.admitted == 1
            await _close_open(work, campaign.agent)
            async with work.begin() as connection:
                await add_spend(
                    connection,
                    "curie",
                    campaign.agent,
                    campaign.now.astimezone(UTC).date(),
                    Decimal("1"),
                )
            later = campaign.now + timedelta(minutes=2)
            skipped = await loop.one_pass(later)
            assert skipped.admitted == 0
            assert skipped.skipped >= 1
            reasons = await _reasons(observer, campaign.agent)
            assert ("skipped", "daily_budget") in reasons
            async with work.begin() as connection:
                await connection.execute(
                    text("UPDATE curie.agents SET max_usd_per_day = 10 WHERE id = :id"),
                    {"id": campaign.agent},
                )
            admitted = await loop.one_pass(later + timedelta(minutes=2))
            assert admitted.admitted == 1

    asyncio.run(scenario())


def test_utc_day_boundary(campaign: Campaign) -> None:
    async def scenario() -> None:
        await _upgrade(campaign)
        day = datetime(2026, 10, 20, 23, 50, tzinfo=UTC)
        nxt = datetime(2026, 10, 21, 0, 10, tzinfo=UTC)
        async with campaign.runtime() as (loop, _, work, _observer, _redis):
            async with work.begin() as connection:
                await connection.execute(
                    text("UPDATE curie.agents SET max_usd_per_day = :cap WHERE id = :id"),
                    {"cap": 0.01, "id": campaign.agent},
                )
            async with work.begin() as connection:
                await add_spend(connection, "curie", campaign.agent, day.date(), Decimal("1"))
            blocked = await loop.one_pass(day)
            assert blocked.admitted == 0
            opened = await loop.one_pass(nxt)
            assert opened.admitted == 1

    asyncio.run(scenario())


async def _fail(engine: Any, agent: uuid.UUID, name: str, n: int) -> str | None:
    async with engine.begin() as connection:
        return await account_closed_run(
            connection,
            "curie",
            agent_id=agent,
            name=name,
            outcome="failed",
            when=datetime(2026, 10, 20, 1, n, tzinfo=UTC),
            model=None,
            input_tokens=None,
            output_tokens=None,
        )


def test_three_failures_pause_one_notice(campaign: Campaign) -> None:
    async def scenario() -> None:
        await _upgrade(campaign)
        engine = create_async_engine(campaign.url)
        try:
            assert await _fail(engine, campaign.agent, "nightly", 1) is None
            assert await _fail(engine, campaign.agent, "nightly", 2) is None
            notice = await _fail(engine, campaign.agent, "nightly", 3)
            assert notice is not None and "nightly" in notice and "resume" in notice
            async with engine.connect() as connection:
                paused = (
                    await connection.execute(
                        text(
                            "SELECT paused_at IS NOT NULL FROM curie.schedule_controls "
                            "WHERE agent_id = :agent AND name = 'nightly'"
                        ),
                        {"agent": campaign.agent},
                    )
                ).scalar()
                notices = (
                    await connection.execute(
                        text(
                            "SELECT count(*) FROM curie.schedule_pause_notices "
                            "WHERE agent_id = :agent AND name = 'nightly'"
                        ),
                        {"agent": campaign.agent},
                    )
                ).scalar()
            assert paused is True
            assert notices == 1
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_two_replicas_one_notice(campaign: Campaign) -> None:
    async def scenario() -> None:
        await _upgrade(campaign)
        engine = create_async_engine(campaign.url, pool_size=2)
        try:
            await _fail(engine, campaign.agent, "nightly", 1)
            await _fail(engine, campaign.agent, "nightly", 2)

            async def third(minute: int) -> str | None:
                return await _fail(engine, campaign.agent, "nightly", minute)

            first, second = await asyncio.gather(third(3), third(4))
            bodies = [item for item in (first, second) if item]
            assert len(bodies) == 1
            async with engine.connect() as connection:
                notices = (
                    await connection.execute(
                        text(
                            "SELECT count(*) FROM curie.schedule_pause_notices "
                            "WHERE agent_id = :agent"
                        ),
                        {"agent": campaign.agent},
                    )
                ).scalar()
            assert notices == 1
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_success_resets_count(campaign: Campaign) -> None:
    async def scenario() -> None:
        await _upgrade(campaign)
        engine = create_async_engine(campaign.url)
        try:
            await _fail(engine, campaign.agent, "nightly", 1)
            await _fail(engine, campaign.agent, "nightly", 2)
            async with engine.begin() as connection:
                await account_closed_run(
                    connection,
                    "curie",
                    agent_id=campaign.agent,
                    name="nightly",
                    outcome="ran",
                    when=datetime(2026, 10, 20, 2, 0, tzinfo=UTC),
                    model="anthropic/claude-haiku-5.5",
                    input_tokens=10,
                    output_tokens=10,
                )
            notice = await _fail(engine, campaign.agent, "nightly", 5)
            assert notice is None
            async with engine.connect() as connection:
                count = (
                    await connection.execute(
                        text(
                            "SELECT consecutive FROM curie.schedule_failure_streaks "
                            "WHERE agent_id = :agent AND name = 'nightly'"
                        ),
                        {"agent": campaign.agent},
                    )
                ).scalar()
            assert count == 1
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_resume_clears_count(campaign: Campaign) -> None:
    async def scenario() -> None:
        await _upgrade(campaign)
        async with campaign.runtime() as (loop, _, work, _observer, _redis):
            await _fail(work, campaign.agent, "nightly", 1)
            await _fail(work, campaign.agent, "nightly", 2)
            await _fail(work, campaign.agent, "nightly", 3)
            async with work.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE curie.schedule_controls SET paused_at = NULL "
                        "WHERE agent_id = :agent AND name = 'nightly'"
                    ),
                    {"agent": campaign.agent},
                )
                await reset_streak(connection, "curie", campaign.agent, "nightly")
            async with work.connect() as connection:
                count = (
                    await connection.execute(
                        text(
                            "SELECT consecutive FROM curie.schedule_failure_streaks "
                            "WHERE agent_id = :agent AND name = 'nightly'"
                        ),
                        {"agent": campaign.agent},
                    )
                ).scalar()
            assert count == 0
            summary = await loop.one_pass(campaign.now + timedelta(minutes=3))
            assert summary.admitted == 1

    asyncio.run(scenario())
