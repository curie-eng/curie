"""Actual pre/posteffect boundary faults, @spec PROTECTED-HOOK-SOURCE-2/10."""

from __future__ import annotations

import asyncio
import importlib.util
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from curie_protected_hooks.source_policy_sql import SourceGateInvalid, SourceSnapshotUnavailable
from curie_worker.cron_loop import CronPassSummary, CronSchedulerLoop
from curie_worker.hook_source_guard import CronHookSourceGuard
from redis.exceptions import ResponseError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


def core_support() -> Any:
    """Public fixture reuse, @spec PROTECTED-HOOK-SOURCE-2/10."""
    path = Path(__file__).with_name("test_source_cron_integration.py")
    spec = importlib.util.spec_from_file_location("_source_cron_fault_setup", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


core = core_support()
worker_db = core.worker_db
worker_templates = core.worker_templates
campaign = core.campaign
HOOK = core.HOOK


def test_genuine_bundle_parser_control_before_guarded_constructor(campaign: Any) -> None:
    """Independent actual S3 positive, @spec PROTECTED-HOOK-SOURCE-2."""
    source = core.BundleTriggerSource(
        campaign.store, max_uncompressed_bytes=1000000, max_compression_ratio=1000, max_members=10
    )
    assert source.triggers(campaign.key) == [campaign.trigger]


def test_loop_rejects_guard_for_other_actual_work_pool(campaign: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with campaign.runtime(guarded=False) as (loop, guard, work, _, redis):
            other_work = create_async_engine(campaign.url)
            try:
                other_guard = CronHookSourceGuard(guard.source_gate, other_work)
                with pytest.raises(SourceGateInvalid):
                    CronSchedulerLoop(
                        engine=work,
                        redis=redis,
                        source=loop._source,
                        is_killed=loop._is_killed,
                        db_schema="curie",
                        stream=campaign.stream,
                        interval_seconds=1,
                        claim_lease_s=60,
                        default_max_usd_per_day=1,
                        default_max_output_tokens_per_run=100,
                        source_guard=other_guard,
                    )
                assert work.pool.checkedout() == other_work.pool.checkedout() == 0
            finally:
                await other_work.dispose()

    asyncio.run(scenario())


async def gate_pid(observer: Any, agent: uuid.UUID) -> int | None:
    """Exact owned lock identity, @spec PROTECTED-HOOK-SOURCE-2."""
    async with observer.connect() as connection:
        value = await connection.scalar(
            text(
                "SELECT l.pid FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid "
                "CROSS JOIN (SELECT hashtextextended(:key,0) AS h) k "
                "WHERE a.datname=current_database() AND l.locktype='advisory' AND l.granted "
                "AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database()) "
                "AND l.classid::bigint=((k.h >> 32) & 4294967295) "
                "AND l.objid::bigint=(k.h & 4294967295)"
            ),
            dict(key="hook-source:" + str(agent)),
        )
    return None if value is None else int(value)


async def seed_run(
    campaign: Any, engine: Any, *, outcome: str | None, old: bool = False
) -> uuid.UUID:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    run_id = uuid.uuid4()
    slot = campaign.slot - timedelta(minutes=2) if old else campaign.slot
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "INSERT INTO curie.hook_runs "
                "(id,agent_id,name,slot_utc,version_id,outcome,started_at,"
                "ended_at,lease_expires_at) "
                "VALUES(:id,:agent,:name,:slot,:version,:outcome,:started,:ended,:lease)"
            ),
            dict(
                id=run_id,
                agent=campaign.agent,
                name=HOOK,
                slot=slot,
                version=campaign.version,
                outcome=outcome,
                started=datetime.now(UTC) - timedelta(minutes=5),
                ended=None if outcome is None else datetime.now(UTC),
                lease=datetime.now(UTC) - timedelta(minutes=1),
            ),
        )
    return run_id


async def kill_owned_gate(observer: Any, agent: uuid.UUID) -> int:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    pid = await gate_pid(observer, agent)
    assert pid is not None, "production must hold the exact source gate"
    async with observer.connect() as connection:
        assert await connection.scalar(text("SELECT pg_terminate_backend(:pid)"), dict(pid=pid))
    return pid


def isolate_current_due_slot(campaign: Any) -> None:
    """Actual daily archive avoids prior skipped slots, @spec PROTECTED-HOOK-SOURCE-2."""
    campaign.trigger["schedule"] = f"{campaign.slot.minute} {campaign.slot.hour} * * *"
    campaign.key = campaign.bundle([campaign.trigger])
    campaign.support.sql_dicts(
        "UPDATE curie.agent_versions SET bundle_ref=:ref WHERE id=:id",
        dict(ref=campaign.key, id=campaign.version),
    )


def test_gate_loss_while_inner_hook_lock_waits_preserves_aged_claim(campaign: Any) -> None:
    """Probe BEFORE reclaim/insert, @spec PROTECTED-HOOK-SOURCE-2."""
    isolate_current_due_slot(campaign)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with campaign.runtime() as (loop, _, work, observer, redis):
            await seed_run(campaign, work, outcome=None, old=True)
            before = await campaign.runs(observer)
            task = None
            try:
                async with observer.connect() as blocker, blocker.begin():
                    await blocker.execute(
                        text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"),
                        dict(key=str(campaign.agent) + ":" + HOOK),
                    )
                    task = asyncio.create_task(loop.one_pass(campaign.now))
                    await core.wait_advisory(observer, task)
                    await kill_owned_gate(observer, campaign.agent)
                summary = await task
                assert await campaign.runs(observer) == before
                assert summary.reclaimed == summary.admitted == 0
                assert await redis.xlen(campaign.stream) == 0
            finally:
                if task is not None and not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    asyncio.run(asyncio.wait_for(scenario(), 12))


async def install_gate_kill_trigger(campaign: Any, engine: Any, *, phase: str) -> tuple[str, str]:
    """Scoped real SQL fault, @spec PROTECTED-HOOK-SOURCE-2."""
    token = uuid.uuid4().hex
    function, trigger = "test_source_function_" + token, "test_source_trigger_" + token
    condition = {
        "reclaim": "OLD.outcome IS NULL AND NEW.outcome='reclaimed'",
        "insert_commit": "NEW.outcome IS NULL",
        "reopen_commit": "OLD.outcome='deferred' AND NEW.outcome IS NULL",
    }[phase]
    body = f"""CREATE FUNCTION curie.{function}() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE owned_pid integer; h bigint;
    BEGIN
      IF NEW.agent_id='{campaign.agent}'::uuid AND NEW.name='{HOOK}' AND ({condition}) THEN
        h := hashtextextended('hook-source:' || NEW.agent_id::text,0);
        SELECT l.pid INTO owned_pid FROM pg_locks l
        WHERE l.locktype='advisory' AND l.granted
          AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database())
          AND l.classid::bigint=((h >> 32) & 4294967295)
          AND l.objid::bigint=(h & 4294967295);
        IF owned_pid IS NULL THEN RAISE EXCEPTION 'missing owned source gate'; END IF;
        PERFORM pg_terminate_backend(owned_pid);
      END IF;
      RETURN NEW;
    END $$"""
    if phase == "reclaim":
        statement = (
            f"CREATE TRIGGER {trigger} AFTER UPDATE ON curie.hook_runs "
            f"FOR EACH ROW EXECUTE FUNCTION curie.{function}()"
        )
    else:
        event = "INSERT" if phase == "insert_commit" else "UPDATE"
        statement = (
            f"CREATE CONSTRAINT TRIGGER {trigger} AFTER {event} ON curie.hook_runs "
            "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
            f"EXECUTE FUNCTION curie.{function}()"
        )
    async with engine.begin() as connection:
        await connection.execute(text(body))
        await connection.execute(text(statement))
    return function, trigger


async def remove_trigger(engine: Any, identifiers: tuple[str, str]) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    function, trigger = identifiers
    async with engine.begin() as connection:
        await connection.execute(text(f"DROP TRIGGER IF EXISTS {trigger} ON curie.hook_runs"))
        await connection.execute(text(f"DROP FUNCTION IF EXISTS curie.{function}()"))


@pytest.mark.parametrize("phase", ["reclaim", "insert_commit", "reopen_commit"])
def test_real_statement_or_commit_gate_loss_has_honest_durable_counts(
    campaign: Any, phase: str
) -> None:
    """AFTER effects authorize only NEXT-boundary proof, @spec PROTECTED-HOOK-SOURCE-2."""
    if phase == "reclaim":
        isolate_current_due_slot(campaign)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with campaign.runtime() as (loop, _, work, observer, redis):
            if phase == "reclaim":
                await seed_run(campaign, work, outcome=None, old=True)
            elif phase == "reopen_commit":
                await seed_run(campaign, work, outcome="deferred")
            before = await campaign.runs(observer)
            identifiers = await install_gate_kill_trigger(campaign, work, phase=phase)
            try:
                summary = await loop.one_pass(campaign.now)
                rows = await campaign.runs(observer)
                assert await redis.xlen(campaign.stream) == 0
                assert summary.admitted == summary.retried == summary.reclaimed == 0
                if phase == "reclaim":
                    assert rows == before, "reclaim must roll back when next INSERT probe refuses"
                else:
                    assert (
                        len(rows) == 1
                        and rows[0]["outcome"] is None
                        and rows[0]["ended_at"] is None
                    )
                    if phase == "reopen_commit":
                        assert rows[0]["id"] == before[0]["id"]
            finally:
                await remove_trigger(work, identifiers)

    asyncio.run(asyncio.wait_for(scenario(), 12))


@pytest.mark.parametrize("effect", ["insert", "skip", "admit", "retry"])
def test_detected_dead_gate_refuses_every_direct_sql_entry(campaign: Any, effect: str) -> None:
    """No AFTER-trigger pre-effect claim, @spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with campaign.runtime() as (loop, guard, work, observer, redis):
            target = (await loop._targets())[0]
            if effect == "retry":
                await seed_run(campaign, work, outcome="deferred")
            before = await campaign.runs(observer)
            with pytest.raises((SourceGateInvalid, SourceSnapshotUnavailable)):
                async with guard.locked_snapshot(campaign.agent, HOOK) as context:
                    await kill_owned_gate(observer, campaign.agent)
                    summary = CronPassSummary()
                    if effect == "insert":
                        async with work.begin() as connection:
                            await loop._insert(
                                connection,
                                target,
                                HOOK,
                                campaign.slot,
                                None,
                                source_context=context,
                            )
                    elif effect == "skip":
                        await loop._skip(
                            target, HOOK, [campaign.slot], summary, source_context=context
                        )
                    elif effect == "admit":
                        await loop._admit(
                            target,
                            campaign.trigger,
                            campaign.slot,
                            summary,
                            False,
                            source_context=context,
                        )
                    else:
                        await loop._retry_deferred(
                            target,
                            campaign.trigger,
                            "UTC",
                            campaign.now,
                            summary,
                            source_context=context,
                        )
                    pytest.fail("dead source gate authorized SQL effect")
            assert (
                await campaign.runs(observer) == before and await redis.xlen(campaign.stream) == 0
            )

    asyncio.run(scenario())


@pytest.mark.parametrize("phase", ["reclaim", "insert_commit", "reopen_commit"])
def test_genuine_trigger_fault_controls_on_actual_guard_without_new_loop_constructor(
    campaign: Any, phase: str
) -> None:
    """Fixture phase proof, not cron integration, @spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with campaign.runtime(guarded=False) as (_, guard, work, observer, _):
            row = None
            if phase != "insert_commit":
                row = await seed_run(
                    campaign,
                    work,
                    outcome=None if phase == "reclaim" else "deferred",
                    old=phase == "reclaim",
                )
            identifiers = await install_gate_kill_trigger(campaign, work, phase=phase)
            try:
                with pytest.raises((SourceGateInvalid, SourceSnapshotUnavailable)):
                    async with guard.locked_snapshot(campaign.agent, HOOK) as context:
                        if phase == "insert_commit":
                            await seed_run(campaign, work, outcome=None)
                        else:
                            async with work.begin() as connection:
                                await connection.execute(
                                    text(
                                        "UPDATE curie.hook_runs SET outcome=:outcome, "
                                        "ended_at=:ended "
                                        "WHERE id=:id"
                                    ),
                                    dict(
                                        id=row,
                                        outcome="reclaimed" if phase == "reclaim" else None,
                                        ended=datetime.now(UTC) if phase == "reclaim" else None,
                                    ),
                                )
                        await context.ensure_before_effect(campaign.agent, HOOK)
                        pytest.fail(
                            "genuine scoped trigger did not terminate exact held source gate"
                        )
                assert await gate_pid(observer, campaign.agent) is None
            finally:
                await remove_trigger(work, identifiers)

    asyncio.run(asyncio.wait_for(scenario(), 10))


async def hold_work_until_waiter_then_kill(
    work: Any, observer: Any, agent: uuid.UUID, held: asyncio.Event
) -> None:
    """Real SQLAlchemy 2.0.52 queue evidence, @spec PROTECTED-HOOK-SOURCE-2."""
    async with work.connect():
        held.set()
        async with asyncio.timeout(5):
            while not work.pool._pool._queue._getters:
                await asyncio.sleep(0.005)
        await kill_owned_gate(observer, agent)


@pytest.mark.parametrize("dead_cleanup", [False, True])
def test_genuine_wrongtype_preserves_primary_and_fences_failed_cleanup(
    campaign: Any, dead_cleanup: bool
) -> None:
    """Actual XADD failure then SQL checkout, @spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with campaign.runtime() as (loop, guard, work, observer, redis):
            target = (await loop._targets())[0]
            run_id = await seed_run(campaign, work, outcome=None)
            await redis.set(campaign.stream, "owned-wrongtype")
            controller = None
            try:
                try:
                    async with guard.locked_snapshot(campaign.agent, HOOK) as context:
                        if dead_cleanup:
                            held = asyncio.Event()
                            controller = asyncio.create_task(
                                hold_work_until_waiter_then_kill(
                                    work, observer, campaign.agent, held
                                )
                            )
                            await held.wait()
                        with pytest.raises(ResponseError, match="WRONGTYPE"):
                            await loop._enqueue(
                                target,
                                campaign.trigger,
                                None,
                                campaign.slot,
                                run_id,
                                source_context=context,
                            )
                        if controller is not None:
                            await controller
                except SourceSnapshotUnavailable:
                    assert dead_cleanup, "live gate must not become unavailable"
                rows = await campaign.runs(observer)
                assert len(rows) == 1
                assert rows[0]["outcome"] == (None if dead_cleanup else "failed")
                assert (rows[0]["ended_at"] is None) == dead_cleanup
                assert await redis.get(campaign.stream) in {"owned-wrongtype", b"owned-wrongtype"}
            finally:
                if controller is not None and not controller.done():
                    controller.cancel()
                    await asyncio.gather(controller, return_exceptions=True)

    asyncio.run(asyncio.wait_for(scenario(), 10))


@pytest.mark.parametrize("branch", ["aged", "killed", "unbound"])
def test_dead_gate_after_work_checkout_wait_preserves_nonempty_deferred(
    campaign: Any, branch: str
) -> None:
    """Actual deferred branches retain rows, @spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with campaign.runtime() as (loop, guard, work, observer, redis):
            target = (await loop._targets())[0]
            row = await seed_run(campaign, work, outcome="deferred")
            trigger = dict(campaign.trigger)
            if branch == "aged":
                async with work.begin() as connection:
                    await connection.execute(
                        text("UPDATE curie.hook_runs SET slot_utc=:slot WHERE id=:id"),
                        dict(id=row, slot=campaign.slot - timedelta(hours=2)),
                    )
            elif branch == "killed":
                await redis.set(core.kill_key(campaign.agent), "1")
            else:
                trigger["target"] = "owned-missing-binding"
            before = await campaign.runs(observer)
            controller = None
            try:
                with pytest.raises((SourceGateInvalid, SourceSnapshotUnavailable)):
                    async with guard.locked_snapshot(campaign.agent, HOOK) as context:
                        held = asyncio.Event()
                        controller = asyncio.create_task(
                            hold_work_until_waiter_then_kill(work, observer, campaign.agent, held)
                        )
                        await held.wait()
                        await loop._retry_deferred(
                            target,
                            trigger,
                            "UTC",
                            campaign.now,
                            CronPassSummary(),
                            source_context=context,
                        )
                        pytest.fail("dead gate authorized deferred settlement")
                if controller is not None:
                    await controller
                assert await campaign.runs(observer) == before
                assert await redis.xlen(campaign.stream) == 0
            finally:
                if controller is not None and not controller.done():
                    controller.cancel()
                    await asyncio.gather(controller, return_exceptions=True)

    asyncio.run(asyncio.wait_for(scenario(), 10))


@pytest.mark.parametrize("boundary", ["live", "dead", "checkout_wait"])
def test_resume_cas_requires_live_context_after_actual_checkout(
    campaign: Any, boundary: str
) -> None:
    """Routine internal resume seam, @spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with campaign.runtime() as (loop, guard, work, observer, redis):
            target = (await loop._targets())[0]
            async with work.begin() as connection:
                await connection.execute(
                    text(
                        "INSERT INTO curie.schedule_controls(agent_id,name,resume_from,generation) "
                        "VALUES(:agent,:name,:resume,7)"
                    ),
                    dict(agent=campaign.agent, name=HOOK, resume=campaign.start),
                )
            controller = None
            try:
                try:
                    async with guard.locked_snapshot(campaign.agent, HOOK) as context:
                        if boundary == "dead":
                            await kill_owned_gate(observer, campaign.agent)
                        elif boundary == "checkout_wait":
                            held = asyncio.Event()
                            controller = asyncio.create_task(
                                hold_work_until_waiter_then_kill(
                                    work, observer, campaign.agent, held
                                )
                            )
                            await held.wait()
                        if boundary == "live":
                            await loop._clear_resume(
                                target, HOOK, campaign.start, 6, source_context=context
                            )
                            async with observer.connect() as connection:
                                assert (
                                    await connection.scalar(
                                        text(
                                            "SELECT resume_from FROM curie.schedule_controls "
                                            "WHERE agent_id=:agent AND name=:name"
                                        ),
                                        dict(agent=campaign.agent, name=HOOK),
                                    )
                                    == campaign.start
                                )
                            await loop._clear_resume(
                                target, HOOK, campaign.start, 7, source_context=context
                            )
                        else:
                            with pytest.raises((SourceGateInvalid, SourceSnapshotUnavailable)):
                                await loop._clear_resume(
                                    target, HOOK, campaign.start, 7, source_context=context
                                )
                            if controller is not None:
                                await controller
                except SourceSnapshotUnavailable:
                    assert boundary != "live"
                async with observer.connect() as connection:
                    value = await connection.scalar(
                        text(
                            "SELECT resume_from FROM curie.schedule_controls "
                            "WHERE agent_id=:agent AND name=:name"
                        ),
                        dict(agent=campaign.agent, name=HOOK),
                    )
                assert value == (None if boundary == "live" else campaign.start)
                assert await redis.xlen(campaign.stream) == 0
            finally:
                if controller is not None and not controller.done():
                    controller.cancel()
                    await asyncio.gather(controller, return_exceptions=True)

    asyncio.run(asyncio.wait_for(scenario(), 10))
