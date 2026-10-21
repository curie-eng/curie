"""Agent-created schedules through the cron loop (ADR 0238).

Every test drives ``CronSchedulerLoop.one_pass(now=...)`` against a real
migrated Postgres clone and real Valkey, as ``test_source_cron_integration``
does. The campaign's bundle declares no triggers, so the only hooks that can
fire are the ``agent_schedules`` rows each test inserts with SQL.
"""

from __future__ import annotations

import asyncio
import importlib.util
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from aci_protocol import HookRunRef, QueuedTurn
from curie_protected_hooks.source_policy_sql import SourceGate
from curie_worker.bundle_store import BundleStore
from curie_worker.cron_loop import BundleTriggerSource, CronSchedulerLoop
from curie_worker.hook_runs import HookRunRecorder
from curie_worker.hook_source_guard import CronHookSourceGuard
from curie_worker.killswitch import KillSwitch, kill_key
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

_cron_path = Path(__file__).with_name("test_source_cron_integration.py")
_cron_spec = importlib.util.spec_from_file_location("_agent_schedule_cron_setup", _cron_path)
assert _cron_spec is not None and _cron_spec.loader is not None
_cron = importlib.util.module_from_spec(_cron_spec)
_cron_spec.loader.exec_module(_cron)
Campaign = _cron.Campaign
guard_setup = _cron.guard_setup

worker_templates = guard_setup.worker_templates
worker_db = guard_setup.worker_db

PREFIX = "agent-schedule:"
PROMPT = "Post the open incident summary."
SENDER = "U0EXAMPLE1"


@pytest.fixture
def campaign(worker_db: Any) -> Iterator[Any]:
    support, clone, url = worker_db
    clone.at("selfimp_memory_0001")
    actual = Campaign(support, url)
    # No bundle triggers: only agent schedules can fire in these tests.
    empty = actual.bundle([])
    support.sql_dicts(
        "UPDATE curie.agent_versions SET bundle_ref=:key WHERE id=:id",
        dict(key=empty, id=actual.version),
    )
    support.sql_dicts(
        "UPDATE curie.agents SET agent_schedules=true WHERE id=:id",
        dict(id=actual.agent),
    )
    try:
        yield actual
    finally:
        actual.close()


@asynccontextmanager
async def runtime(
    campaign: Any, *, started_at: datetime | None = None
) -> AsyncIterator[tuple[CronSchedulerLoop, AsyncEngine, AsyncEngine, Redis]]:
    """One replica: its own engines, guard and loop on the shared DB and stream."""

    gate = create_async_engine(campaign.url, pool_size=2, max_overflow=0, pool_timeout=5)
    work = create_async_engine(campaign.url, pool_size=1, max_overflow=0, pool_timeout=5)
    observer = create_async_engine(campaign.url)
    redis = Redis(**campaign.config.valkey_client_kwargs())
    switch = KillSwitch(redis, on_kill=campaign.no_interrupt)
    source = BundleTriggerSource(
        BundleStore(campaign.config),
        max_uncompressed_bytes=1000000,
        max_compression_ratio=1000,
        max_members=10,
    )
    guard = CronHookSourceGuard(SourceGate(gate), work)
    try:
        loop = CronSchedulerLoop(
            engine=work,
            redis=redis,
            source=source,
            is_killed=switch.is_killed,
            db_schema="curie",
            stream=campaign.stream,
            interval_seconds=1,
            claim_lease_s=60,
            default_max_usd_per_day=1,
            default_max_output_tokens_per_run=100,
            started_at=started_at or campaign.start,
            source_guard=guard,
        )
        yield loop, work, observer, redis
    finally:
        source._reader._client.close()
        await redis.delete(campaign.stream, kill_key(campaign.agent))
        await redis.aclose()
        await gate.dispose()
        await work.dispose()
        await observer.dispose()


async def add_schedule(
    engine: AsyncEngine,
    campaign: Any,
    *,
    cron: str | None = None,
    run_at: datetime | None = None,
    prompt: str = PROMPT,
    effective_from: datetime | None = None,
    state: str = "active",
    revision: int = 1,
) -> uuid.UUID:
    schedule_id = uuid.uuid4()
    created = effective_from or campaign.slot - timedelta(minutes=2)
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "INSERT INTO curie.agent_schedules "
                "(id, agent_id, revision, state, prompt, cron, run_at, timezone, "
                "destination_kind, destination_address, destination_adapter, "
                "created_by, created_source, created_conversation, created_turn, "
                "idempotency_key, effective_from, created_at, updated_at, updated_by) "
                "VALUES (:id, :agent, :revision, :state, :prompt, :cron, :run_at, 'UTC', "
                "'slack', :address, 'default', :sender, 'agent', :conversation, :turn, "
                ":key, :effective_from, :created, :created, :sender)"
            ),
            dict(
                id=schedule_id,
                agent=campaign.agent,
                revision=revision,
                state=state,
                prompt=prompt,
                cron=cron,
                run_at=run_at,
                address=campaign.address,
                sender=SENDER,
                conversation=f"slack:{campaign.address}:1700000000.000100",
                turn="evt-0001#abcd1234",
                key=uuid.uuid4().hex,
                effective_from=created,
                created=created,
            ),
        )
    return schedule_id


async def execute(engine: AsyncEngine, sql: str, **params: Any) -> None:
    async with engine.begin() as connection:
        await connection.execute(text(sql), params)


async def runs(engine: AsyncEngine, campaign: Any, schedule_id: uuid.UUID) -> list[Any]:
    async with engine.connect() as connection:
        return list(
            (
                await connection.execute(
                    text(
                        "SELECT id, name, source, outcome, reason, slot_utc "
                        "FROM curie.hook_runs WHERE agent_id=:agent AND name=:name "
                        "ORDER BY slot_utc"
                    ),
                    dict(agent=campaign.agent, name=f"{PREFIX}{schedule_id}"),
                )
            )
            .mappings()
            .all()
        )


async def all_runs(engine: AsyncEngine, campaign: Any) -> list[Any]:
    async with engine.connect() as connection:
        return list(
            (
                await connection.execute(
                    text("SELECT name, outcome FROM curie.hook_runs WHERE agent_id=:agent"),
                    dict(agent=campaign.agent),
                )
            )
            .mappings()
            .all()
        )


async def schedule_state(engine: AsyncEngine, schedule_id: uuid.UUID) -> str:
    async with engine.connect() as connection:
        value = await connection.scalar(
            text("SELECT state FROM curie.agent_schedules WHERE id=:id"),
            dict(id=schedule_id),
        )
    assert isinstance(value, str)
    return value


async def snapshots(engine: AsyncEngine, schedule_id: uuid.UUID) -> list[Any]:
    async with engine.connect() as connection:
        return list(
            (
                await connection.execute(
                    text(
                        "SELECT s.hook_run_id, s.agent_id, s.revision, s.prompt, s.cron, "
                        "s.run_at, s.destination_kind, s.destination_address, "
                        "s.destination_adapter, r.slot_utc "
                        "FROM curie.agent_schedule_runs s "
                        "JOIN curie.hook_runs r ON r.id = s.hook_run_id "
                        "WHERE s.schedule_id=:id ORDER BY r.slot_utc"
                    ),
                    dict(id=schedule_id),
                )
            )
            .mappings()
            .all()
        )


async def queued(redis: Redis, stream: str) -> list[QueuedTurn]:
    return [
        QueuedTurn.model_validate_json(fields["payload"])
        for _, fields in await redis.xrange(stream)
    ]


def ref_of(campaign: Any, schedule_id: uuid.UUID, slot: datetime) -> HookRunRef:
    return HookRunRef(
        agent_id=str(campaign.agent),
        name=f"{PREFIX}{schedule_id}",
        slot_utc=slot.astimezone(UTC).isoformat(),
    )


async def start(engine: AsyncEngine, ref: HookRunRef) -> bool:
    """The runner-start revalidation the kernel performs, with no runner."""

    async with HookRunRecorder(engine, "curie").start_guard(ref) as allowed:
        return bool(allowed)


def test_recurring_schedule_fires_next_slot_with_snapshot_and_turn(campaign: Any) -> None:
    async def scenario() -> None:
        async with runtime(campaign) as (loop, work, observer, redis):
            sid = await add_schedule(work, campaign, cron="* * * * *")
            summary = await loop.one_pass(campaign.now)
            assert summary.admitted == 1

            rows = await runs(observer, campaign, sid)
            assert len(rows) == 1
            row = rows[0]
            assert row["name"] == f"{PREFIX}{sid}"
            assert row["source"] == "schedule"
            assert row["outcome"] is None
            assert row["slot_utc"] == campaign.slot

            snaps = await snapshots(observer, sid)
            assert len(snaps) == 1
            assert snaps[0]["hook_run_id"] == row["id"]
            assert snaps[0]["agent_id"] == campaign.agent
            assert snaps[0]["revision"] == 1
            assert snaps[0]["prompt"] == PROMPT
            assert snaps[0]["cron"] == "* * * * *"
            assert (
                snaps[0]["destination_kind"],
                snaps[0]["destination_address"],
                snaps[0]["destination_adapter"],
            ) == ("slack", campaign.address, "default")

            turns = await queued(redis, campaign.stream)
            assert len(turns) == 1
            turn = turns[0]
            assert turn.text == PROMPT
            assert turn.author == f"cron:{PREFIX}{sid}"
            assert turn.event_id.startswith(f"cron:{campaign.agent}:{PREFIX}{sid}:")
            assert turn.reply_handle is not None
            assert turn.reply_handle.kind == "slack"
            assert turn.reply_handle.channel == campaign.address
            assert turn.reply_handle.adapter == "default"
            assert turn.hook_run is not None
            assert turn.hook_run.name == f"{PREFIX}{sid}"
            assert datetime.fromisoformat(turn.hook_run.slot_utc) == campaign.slot

            # The same slot never fires twice.
            await loop.one_pass(campaign.now + timedelta(seconds=5))
            assert await redis.xlen(campaign.stream) == 1
            assert len(await runs(observer, campaign, sid)) == 1

    asyncio.run(scenario())


def test_no_slot_before_effective_from(campaign: Any) -> None:
    async def scenario() -> None:
        async with runtime(campaign) as (loop, work, observer, redis):
            # Created at hh:mm:30: the hh:mm slot predates it.
            sid = await add_schedule(
                work,
                campaign,
                cron="* * * * *",
                effective_from=campaign.slot + timedelta(seconds=30),
            )
            await loop.one_pass(campaign.slot + timedelta(seconds=40))
            assert await runs(observer, campaign, sid) == []
            assert await redis.xlen(campaign.stream) == 0

            await loop.one_pass(campaign.slot + timedelta(seconds=80))
            rows = await runs(observer, campaign, sid)
            assert [row["slot_utc"] for row in rows] == [campaign.slot + timedelta(minutes=1)]
            assert await redis.xlen(campaign.stream) == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["recurring", "one_shot"])
def test_two_replicas_admit_once(campaign: Any, kind: str) -> None:
    async def scenario() -> None:
        async with (
            runtime(campaign) as (first, work, observer, redis),
            runtime(campaign) as (second, _, _, _),
        ):
            if kind == "recurring":
                sid = await add_schedule(work, campaign, cron="* * * * *")
                now = campaign.now
            else:
                run_at = campaign.slot + timedelta(seconds=5)
                sid = await add_schedule(work, campaign, run_at=run_at)
                now = run_at + timedelta(seconds=10)
            a, b = await asyncio.gather(first.one_pass(now), second.one_pass(now))
            assert a.admitted + b.admitted == 1
            rows = await runs(observer, campaign, sid)
            assert len(rows) == 1 and rows[0]["outcome"] is None
            assert len(await snapshots(observer, sid)) == 1
            assert await redis.xlen(campaign.stream) == 1

    asyncio.run(scenario())


def test_one_shot_fires_once_then_done(campaign: Any) -> None:
    async def scenario() -> None:
        run_at = campaign.slot + timedelta(seconds=5)
        async with runtime(campaign) as (loop, work, observer, redis):
            sid = await add_schedule(work, campaign, run_at=run_at)

            early = await loop.one_pass(run_at - timedelta(seconds=3))
            assert early.admitted == 0
            assert await runs(observer, campaign, sid) == []

            fired = await loop.one_pass(run_at + timedelta(seconds=10))
            assert fired.admitted == 1
            rows = await runs(observer, campaign, sid)
            assert len(rows) == 1 and rows[0]["slot_utc"] == run_at
            assert rows[0]["source"] == "schedule"
            turns = await queued(redis, campaign.stream)
            assert len(turns) == 1 and turns[0].text == PROMPT
            assert turns[0].author == f"cron:{PREFIX}{sid}"

            # Kernel-style close of the run.
            assert await HookRunRecorder(work, "curie").close(ref_of(campaign, sid, run_at), "ran")

            await loop.one_pass(run_at + timedelta(seconds=20))
            assert await schedule_state(observer, sid) == "done"
            assert await redis.xlen(campaign.stream) == 1

            third = await loop.one_pass(run_at + timedelta(seconds=40))
            assert third.admitted == 0
            assert await redis.xlen(campaign.stream) == 1
            assert [row["outcome"] for row in await runs(observer, campaign, sid)] == ["ran"]

    asyncio.run(scenario())


def test_restart_between_commit_and_enqueue_reclaims_and_completes(campaign: Any) -> None:
    async def scenario() -> None:
        run_at = campaign.slot + timedelta(seconds=5)
        async with runtime(campaign) as (loop_a, work, observer, redis):
            sid = await add_schedule(work, campaign, run_at=run_at)

            async def die(*_: Any, **__: Any) -> Any:
                # Process death after the admission commit: BaseException, so
                # the enqueue failure bookkeeping never runs.
                raise asyncio.CancelledError

            redis.xadd = die  # type: ignore[method-assign]
            with pytest.raises(asyncio.CancelledError):
                await loop_a.one_pass(run_at + timedelta(seconds=10))
            del redis.xadd
            rows = await runs(observer, campaign, sid)
            assert len(rows) == 1 and rows[0]["outcome"] is None
            assert await redis.xlen(campaign.stream) == 0
            # The lease lapses (the worker is gone).
            await execute(
                work,
                "UPDATE curie.hook_runs SET lease_expires_at = now() - interval '1 second' "
                "WHERE id = :id",
                id=rows[0]["id"],
            )

        async with runtime(campaign, started_at=run_at + timedelta(minutes=2)) as (
            loop_b,
            _,
            observer,
            redis,
        ):
            await loop_b.one_pass(run_at + timedelta(minutes=2, seconds=5))
            rows = await runs(observer, campaign, sid)
            assert len(rows) == 1
            assert (rows[0]["outcome"], rows[0]["reason"]) == ("reclaimed", "claim_expired")
            assert await schedule_state(observer, sid) == "done"
            assert await redis.xlen(campaign.stream) == 0

            await loop_b.one_pass(run_at + timedelta(minutes=3))
            assert await redis.xlen(campaign.stream) == 0
            assert len(await runs(observer, campaign, sid)) == 1

    asyncio.run(scenario())


def test_loop_started_after_run_at_fires_once(campaign: Any) -> None:
    async def scenario() -> None:
        run_at = campaign.slot + timedelta(seconds=5)
        later = run_at + timedelta(minutes=30)
        async with runtime(campaign, started_at=later) as (loop, work, observer, redis):
            sid = await add_schedule(work, campaign, run_at=run_at)
            first = await loop.one_pass(later + timedelta(seconds=5))
            assert first.admitted == 1
            await loop.one_pass(later + timedelta(seconds=30))
            rows = await runs(observer, campaign, sid)
            assert len(rows) == 1 and rows[0]["slot_utc"] == run_at
            assert await redis.xlen(campaign.stream) == 1

    asyncio.run(scenario())


def test_stale_one_shot_is_skipped_and_done(campaign: Any) -> None:
    async def scenario() -> None:
        run_at = campaign.slot + timedelta(seconds=5)
        late = run_at + timedelta(hours=1, minutes=5)
        async with runtime(campaign, started_at=late) as (loop, work, observer, redis):
            sid = await add_schedule(work, campaign, run_at=run_at)
            summary = await loop.one_pass(late + timedelta(seconds=5))
            assert summary.admitted == 0
            rows = await runs(observer, campaign, sid)
            assert len(rows) == 1
            assert (rows[0]["outcome"], rows[0]["reason"]) == ("skipped", "catch_up_expired")
            assert await schedule_state(observer, sid) == "done"
            assert await redis.xlen(campaign.stream) == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("change", ["toggle_off", "cancelled", "paused"])
@pytest.mark.parametrize("kind", ["recurring", "one_shot"])
def test_off_cancelled_or_paused_never_fires(campaign: Any, change: str, kind: str) -> None:
    async def scenario() -> None:
        run_at = campaign.slot + timedelta(seconds=5)
        async with runtime(campaign) as (loop, work, observer, redis):
            sid = await add_schedule(
                work,
                campaign,
                cron="* * * * *" if kind == "recurring" else None,
                run_at=None if kind == "recurring" else run_at,
                state="cancelled" if change == "cancelled" else "active",
            )
            if change == "toggle_off":
                await execute(
                    work,
                    "UPDATE curie.agents SET agent_schedules=false WHERE id=:id",
                    id=campaign.agent,
                )
            elif change == "paused":
                await execute(
                    work,
                    "INSERT INTO curie.schedule_controls(agent_id,name,paused_at) "
                    "VALUES(:agent,:name,now())",
                    agent=campaign.agent,
                    name=f"{PREFIX}{sid}",
                )
            summary = await loop.one_pass(run_at + timedelta(seconds=15))
            assert summary.admitted == 0
            assert await redis.xlen(campaign.stream) == 0
            assert [
                row for row in await runs(observer, campaign, sid) if row["outcome"] is None
            ] == []
            assert await snapshots(observer, sid) == []

    asyncio.run(scenario())


def test_revision_change_after_admission_skips_at_start(campaign: Any) -> None:
    async def scenario() -> None:
        async with runtime(campaign) as (loop, work, observer, redis):
            sid = await add_schedule(work, campaign, cron="* * * * *")
            assert (await loop.one_pass(campaign.now)).admitted == 1
            (turn,) = await queued(redis, campaign.stream)
            assert turn.hook_run is not None
            await execute(
                work,
                "UPDATE curie.agent_schedules SET revision = revision + 1, "
                "prompt = 'A different prompt.' WHERE id=:id",
                id=sid,
            )

            assert await start(work, turn.hook_run) is False
            rows = await runs(observer, campaign, sid)
            assert (rows[0]["outcome"], rows[0]["reason"]) == ("skipped", "schedule_changed")

            # The kernel's later close is a no-op on the closed row.
            recorder = HookRunRecorder(work, "curie")
            assert await recorder.close(turn.hook_run, "deferred", "hook_paused") is False
            assert await recorder.close(turn.hook_run, "failed", "turn_error") is False
            rows = await runs(observer, campaign, sid)
            assert (rows[0]["outcome"], rows[0]["reason"]) == ("skipped", "schedule_changed")

    asyncio.run(scenario())


def test_ordinary_pause_after_admission_stays_open_and_resumable(campaign: Any) -> None:
    async def scenario() -> None:
        async with runtime(campaign) as (loop, work, observer, redis):
            sid = await add_schedule(work, campaign, cron="* * * * *")
            assert (await loop.one_pass(campaign.now)).admitted == 1
            (turn,) = await queued(redis, campaign.stream)
            assert turn.hook_run is not None
            await execute(
                work,
                "INSERT INTO curie.schedule_controls(agent_id,name,paused_at) "
                "VALUES(:agent,:name,now())",
                agent=campaign.agent,
                name=f"{PREFIX}{sid}",
            )
            assert await start(work, turn.hook_run) is False
            rows = await runs(observer, campaign, sid)
            assert (rows[0]["outcome"], rows[0]["reason"]) == (None, None)
            # Unchanged behavior: the kernel records it deferred, resumable.
            assert await HookRunRecorder(work, "curie").close(
                turn.hook_run, "deferred", "hook_paused"
            )
            rows = await runs(observer, campaign, sid)
            assert (rows[0]["outcome"], rows[0]["reason"]) == ("deferred", "hook_paused")

    asyncio.run(scenario())


@pytest.mark.parametrize("other_kind", [False, True])
def test_destination_removed_before_start_skips(campaign: Any, other_kind: bool) -> None:
    async def scenario() -> None:
        async with runtime(campaign) as (loop, work, observer, redis):
            sid = await add_schedule(work, campaign, cron="* * * * *")
            assert (await loop.one_pass(campaign.now)).admitted == 1
            (turn,) = await queued(redis, campaign.stream)
            assert turn.hook_run is not None
            if other_kind:
                # Same address, another kind: neither satisfies nor redirects.
                await execute(
                    work,
                    "INSERT INTO curie.agent_channels(id,agent_id,kind,address,endpoint,adapter) "
                    "VALUES(:id,:agent,'mail',:address,NULL,NULL)",
                    id=uuid.uuid4(),
                    agent=campaign.agent,
                    address=campaign.address,
                )
            await execute(
                work,
                "DELETE FROM curie.agent_channels WHERE agent_id=:agent AND kind='slack'",
                agent=campaign.agent,
            )
            assert await start(work, turn.hook_run) is False
            rows = await runs(observer, campaign, sid)
            assert (rows[0]["outcome"], rows[0]["reason"]) == ("skipped", "schedule_changed")

            await loop.one_pass(campaign.now + timedelta(minutes=1))
            turns = await queued(redis, campaign.stream)
            assert len(turns) == 1
            assert all(t.reply_handle is None or t.reply_handle.kind == "slack" for t in turns)
            assert [
                row for row in await runs(observer, campaign, sid) if row["outcome"] is None
            ] == []

    asyncio.run(scenario())


def test_one_shot_prompt_update_after_skip_fires_at_new_revision_slot(campaign: Any) -> None:
    async def scenario() -> None:
        run_at = campaign.slot + timedelta(seconds=5)
        async with runtime(campaign) as (loop, work, observer, redis):
            sid = await add_schedule(work, campaign, run_at=run_at)
            assert (await loop.one_pass(run_at + timedelta(seconds=10))).admitted == 1
            (first,) = await queued(redis, campaign.stream)
            assert first.hook_run is not None
            await execute(
                work,
                "UPDATE curie.agent_schedules SET revision = 2, prompt = 'Updated prompt.' "
                "WHERE id=:id",
                id=sid,
            )
            assert await start(work, first.hook_run) is False

            again = await loop.one_pass(run_at + timedelta(seconds=20))
            assert again.admitted == 1
            rows = await runs(observer, campaign, sid)
            assert [(r["slot_utc"], r["outcome"], r["reason"]) for r in rows] == [
                (run_at, "skipped", "schedule_changed"),
                (run_at + timedelta(microseconds=1), None, None),
            ]
            turns = await queued(redis, campaign.stream)
            assert len(turns) == 2
            assert turns[1].text == "Updated prompt."
            assert turns[1].hook_run is not None
            assert datetime.fromisoformat(turns[1].hook_run.slot_utc) == run_at + timedelta(
                microseconds=1
            )
            snaps = await snapshots(observer, sid)
            assert [(s["revision"], s["prompt"]) for s in snaps] == [
                (1, PROMPT),
                (2, "Updated prompt."),
            ]
            assert await schedule_state(observer, sid) == "active"

    asyncio.run(scenario())


def test_one_shot_toggle_off_then_on_without_revision_change_is_done(campaign: Any) -> None:
    async def scenario() -> None:
        run_at = campaign.slot + timedelta(seconds=5)
        async with runtime(campaign) as (loop, work, observer, redis):
            sid = await add_schedule(work, campaign, run_at=run_at)
            assert (await loop.one_pass(run_at + timedelta(seconds=10))).admitted == 1
            (first,) = await queued(redis, campaign.stream)
            assert first.hook_run is not None
            await execute(
                work,
                "UPDATE curie.agents SET agent_schedules=false WHERE id=:id",
                id=campaign.agent,
            )
            assert await start(work, first.hook_run) is False
            await execute(
                work,
                "UPDATE curie.agents SET agent_schedules=true WHERE id=:id",
                id=campaign.agent,
            )

            summary = await loop.one_pass(run_at + timedelta(seconds=20))
            assert summary.admitted == 0
            assert await schedule_state(observer, sid) == "done"
            rows = await runs(observer, campaign, sid)
            assert [(r["outcome"], r["reason"]) for r in rows] == [("skipped", "schedule_changed")]
            assert await redis.xlen(campaign.stream) == 1

    asyncio.run(scenario())


def test_manual_test_fire_does_not_consume_or_complete_one_shot(campaign: Any) -> None:
    async def scenario() -> None:
        run_at = campaign.slot + timedelta(seconds=5)
        async with runtime(campaign) as (loop, work, observer, redis):
            sid = await add_schedule(work, campaign, run_at=run_at)
            await execute(
                work,
                "INSERT INTO curie.hook_runs "
                "(id, agent_id, name, slot_utc, version_id, source, outcome, started_at, ended_at) "
                "VALUES (:id, :agent, :name, :slot, :version, 'manual', 'ran', now(), now())",
                id=uuid.uuid4(),
                agent=campaign.agent,
                name=f"{PREFIX}{sid}",
                slot=run_at - timedelta(minutes=2),
                version=campaign.version,
            )
            await loop.one_pass(run_at - timedelta(seconds=3))
            assert await schedule_state(observer, sid) == "active"
            assert await redis.xlen(campaign.stream) == 0

            fired = await loop.one_pass(run_at + timedelta(seconds=10))
            assert fired.admitted == 1
            rows = await runs(observer, campaign, sid)
            scheduled = [r for r in rows if r["source"] == "schedule"]
            assert len(scheduled) == 1 and scheduled[0]["slot_utc"] == run_at
            assert scheduled[0]["outcome"] is None
            assert await redis.xlen(campaign.stream) == 1

    asyncio.run(scenario())


def test_three_failed_closes_pause_the_schedule(campaign: Any) -> None:
    async def scenario() -> None:
        async with runtime(campaign) as (loop, work, observer, redis):
            sid = await add_schedule(work, campaign, cron="* * * * *")
            name = f"{PREFIX}{sid}"
            recorder = HookRunRecorder(work, "curie")
            for minute in range(3):
                now = campaign.now + timedelta(minutes=minute)
                assert (await loop.one_pass(now)).admitted == 1, minute
                slot = campaign.slot + timedelta(minutes=minute)
                assert await recorder.close(ref_of(campaign, sid, slot), "failed", "turn_error")

            async with observer.connect() as connection:
                paused = await connection.scalar(
                    text(
                        "SELECT paused_at IS NOT NULL FROM curie.schedule_controls "
                        "WHERE agent_id=:agent AND name=:name"
                    ),
                    dict(agent=campaign.agent, name=name),
                )
            assert paused is True

            fourth = await loop.one_pass(campaign.now + timedelta(minutes=3))
            assert fourth.admitted == 0
            assert await redis.xlen(campaign.stream) == 3
            outcomes = [row["outcome"] for row in await runs(observer, campaign, sid)]
            assert outcomes == ["failed", "failed", "failed"]
            # Stored state stays active; paused is the schedule_controls row.
            assert await schedule_state(observer, sid) == "active"
            assert [r for r in await all_runs(observer, campaign) if r["name"] != name] == []

    asyncio.run(scenario())


def test_unfired_recurring_converted_to_one_shot_fires_once_then_done(campaign: Any) -> None:
    """CR2-F1 liveness: the API lets an unfired recurring schedule become a
    one-shot. The loop fires it once at its slot and then marks it done."""

    async def scenario() -> None:
        run_at = campaign.slot + timedelta(seconds=5)
        # Revision 2's one-shot slot: run_at plus one microsecond per revision past 1.
        slot = run_at + timedelta(microseconds=1)
        async with runtime(campaign) as (loop, work, observer, redis):
            # Yearly on 1 January: live but with no slot near the campaign.
            sid = await add_schedule(work, campaign, cron="0 0 1 1 *")
            assert (await loop.one_pass(run_at - timedelta(seconds=4))).admitted == 0
            assert await runs(observer, campaign, sid) == []

            # The row the API's update writes: run_at set, cron cleared, a new
            # revision and a new effective_from.
            await execute(
                work,
                "UPDATE curie.agent_schedules SET run_at = :run_at, cron = NULL, "
                "revision = revision + 1, effective_from = :changed, updated_at = :changed "
                "WHERE id = :id",
                id=sid,
                run_at=run_at,
                changed=run_at - timedelta(seconds=3),
            )

            early = await loop.one_pass(run_at - timedelta(seconds=2))
            assert early.admitted == 0
            assert await schedule_state(observer, sid) == "active"

            fired = await loop.one_pass(run_at + timedelta(seconds=10))
            assert fired.admitted == 1
            rows = await runs(observer, campaign, sid)
            assert [(r["slot_utc"], r["source"], r["outcome"]) for r in rows] == [
                (slot, "schedule", None)
            ]
            snaps = await snapshots(observer, sid)
            assert [(s["revision"], s["cron"], s["run_at"]) for s in snaps] == [(2, None, run_at)]
            turns = await queued(redis, campaign.stream)
            assert len(turns) == 1 and turns[0].text == PROMPT

            assert await HookRunRecorder(work, "curie").close(ref_of(campaign, sid, slot), "ran")
            await loop.one_pass(run_at + timedelta(seconds=20))
            assert await schedule_state(observer, sid) == "done"

            later = await loop.one_pass(run_at + timedelta(seconds=40))
            assert later.admitted == 0
            assert await redis.xlen(campaign.stream) == 1
            assert [r["outcome"] for r in await runs(observer, campaign, sid)] == ["ran"]

    asyncio.run(scenario())
