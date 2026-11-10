"""Actual cron effect provenance, @spec PROTECTED-HOOK-SOURCE-2/10."""

from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path
from typing import Any

import pytest
from curie_protected_hooks.source_policy_sql import SourceGateInvalid
from curie_worker import cron_loop as cron_module
from curie_worker.cron_loop import CronPassSummary
from redis.exceptions import ResponseError
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import create_async_engine


def effect_support() -> Any:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    path = Path(__file__).with_name("test_source_cron_effect_faults.py")
    spec = importlib.util.spec_from_file_location("_source_cron_ownership_setup", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


support = effect_support()
worker_db = support.worker_db
worker_templates = support.worker_templates
campaign = support.campaign
HOOK = support.HOOK


@pytest.mark.parametrize("foreign", ["agent", "hook"])
def test_wrongtype_cleanup_cannot_modify_run_outside_authorized_identity(
    campaign: Any, foreign: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    other = support.core.Campaign(campaign.support, campaign.url) if foreign == "agent" else None

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with campaign.runtime() as (loop, guard, work, observer, redis):
            target = (await loop._targets())[0]
            # Another real active agent may be selected first; pin the authorized one.
            target = next(t for t in await loop._targets() if t.agent_id == campaign.agent)
            owner = other if other is not None else campaign
            run_id = await support.seed_run(owner, work, outcome=None)
            if foreign == "hook":
                async with work.begin() as connection:
                    await connection.execute(
                        text("UPDATE curie.hook_runs SET name='other-owned-hook' WHERE id=:id"),
                        dict(id=run_id),
                    )
            before = await owner.runs(observer)
            await redis.set(campaign.stream, "owned-wrongtype")
            async with guard.locked_snapshot(campaign.agent, HOOK) as context:
                with pytest.raises(ResponseError, match="WRONGTYPE"):
                    await loop._enqueue(
                        target,
                        campaign.trigger,
                        None,
                        campaign.slot,
                        run_id,
                        source_context=context,
                    )
            assert await owner.runs(observer) == before
            assert await redis.get(campaign.stream) in {"owned-wrongtype", b"owned-wrongtype"}

    try:
        asyncio.run(asyncio.wait_for(scenario(), 10))
    finally:
        if other is not None:
            other.close()


@pytest.mark.parametrize("effect", ["insert", "reclaim"])
def test_supplied_connection_from_other_actual_engine_refuses_before_sql(
    campaign: Any, effect: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with campaign.runtime() as (loop, guard, work, observer, redis):
            target = (await loop._targets())[0]
            await support.seed_run(campaign, work, outcome=None, old=True)
            before = await campaign.runs(observer)
            other_engine = create_async_engine(campaign.url)
            statements: list[str] = []

            def observe_sql(
                _connection: Any,
                _cursor: Any,
                statement: str,
                _parameters: Any,
                _context: Any,
                _executemany: bool,
            ) -> None:
                """Passive actual SQL observer, @spec PROTECTED-HOOK-SOURCE-2."""
                statements.append(statement)

            event.listen(other_engine.sync_engine, "before_cursor_execute", observe_sql)
            try:
                async with guard.locked_snapshot(campaign.agent, HOOK) as context:
                    async with other_engine.begin() as connection:
                        with pytest.raises(SourceGateInvalid):
                            if effect == "insert":
                                await loop._insert(
                                    connection,
                                    target,
                                    HOOK,
                                    campaign.slot,
                                    None,
                                    source_context=context,
                                )
                            else:
                                await loop._lock_and_reclaim(
                                    connection,
                                    target,
                                    HOOK,
                                    campaign.slot,
                                    CronPassSummary(),
                                    source_context=context,
                                )
                assert statements == [], "foreign connection must execute no SQL"
                assert await campaign.runs(observer) == before
                assert await redis.xlen(campaign.stream) == 0
            finally:
                event.remove(other_engine.sync_engine, "before_cursor_execute", observe_sql)
                await other_engine.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 10))


def test_failure_metric_exception_preserves_original_genuine_wrongtype(
    campaign: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only diagnostic sink injected, @spec PROTECTED-HOOK-SOURCE-2."""
    observed: list[str] = []

    def broken_diagnostic(outcome: str) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        observed.append(outcome)
        raise RuntimeError("test diagnostic sink unavailable")

    monkeypatch.setattr(cron_module, "_record_fire", broken_diagnostic)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with campaign.runtime() as (loop, guard, work, observer, redis):
            target = (await loop._targets())[0]
            await support.seed_run(campaign, work, outcome=None)
            rows = await campaign.runs(observer)
            await redis.set(campaign.stream, "owned-wrongtype")
            async with guard.locked_snapshot(campaign.agent, HOOK) as context:
                with pytest.raises(ResponseError, match="WRONGTYPE"):
                    await loop._enqueue(
                        target,
                        campaign.trigger,
                        None,
                        campaign.slot,
                        rows[0]["id"],
                        source_context=context,
                    )
            rows = await campaign.runs(observer)
            assert len(rows) == 1 and rows[0]["outcome"] == "failed"
            assert rows[0]["ended_at"] is not None
            assert observed == ["failed"], "diagnostic fault must follow actual committed cleanup"
            assert await redis.get(campaign.stream) in {"owned-wrongtype", b"owned-wrongtype"}

    asyncio.run(asyncio.wait_for(scenario(), 10))
