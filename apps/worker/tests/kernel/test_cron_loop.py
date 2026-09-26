"""The worker cron scheduler against real Postgres and real Valkey (#268).

``CronSchedulerLoop.one_pass`` reads each active deployment's declared cron
triggers, resolves the latest due slot in its window, records the slot in
``curie.hook_runs`` and, when admitted, XADDs one CRON ``QueuedTurn`` onto the
runs stream. The unique ``(agent_id, name, slot_utc)`` row is the cross-replica
claim, so two loop instances must fire a slot exactly once.

Only the trigger source and the kill check are fakes. Every row seeded here is
created with fresh uuids and deleted by the test that made it; the stream name
is per test (``names``).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import redis
from aci_protocol import QueuedTurn, TurnSource
from curie_worker.cron_loop import CronSchedulerLoop, _Target
from redis.asyncio import Redis as AsyncRedis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .conftest import _DB_URL, _VALKEY_HOST, _VALKEY_PORT, _VALKEY_PW

HOOK = "nightly"
PROMPT = "Summarize.\n  Keep the  spacing verbatim."
BUDGET_S = 300.0
ENDPOINT = "http://curie-test-adapter:8080/"
ADAPTER = "test-adapter"


def _slot() -> datetime:
    """A slot one minute behind the real clock, so a loop comparing against
    either the passed ``now`` or the database clock sees the same picture."""
    return datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=1)


def _schedule(slot: datetime) -> str:
    return f"{slot.minute} {slot.hour} * * *"


@dataclass
class _Seed:
    engine: AsyncEngine
    agent_id: uuid.UUID
    version_id: uuid.UUID
    deployment_id: uuid.UUID
    address: str
    slot: datetime
    bundle_ref: str

    async def runs(self, name: str = HOOK) -> list[Any]:
        async with self.engine.connect() as conn:
            return list(
                (
                    await conn.execute(
                        text(
                            "SELECT id, outcome, slot_utc, ended_at FROM curie.hook_runs "
                            "WHERE agent_id = :a AND name = :n ORDER BY slot_utc"
                        ),
                        {"a": self.agent_id, "n": name},
                    )
                ).all()
            )

    async def set_control(
        self,
        name: str,
        *,
        paused_at: datetime | None,
        resume_from: datetime | None,
    ) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO curie.schedule_controls "
                    "(agent_id, name, paused_at, resume_from) "
                    "VALUES (:agent_id, :name, :paused_at, :resume_from) "
                    "ON CONFLICT (agent_id, name) DO UPDATE SET "
                    "paused_at = EXCLUDED.paused_at, resume_from = EXCLUDED.resume_from"
                ),
                {
                    "agent_id": self.agent_id,
                    "name": name,
                    "paused_at": paused_at,
                    "resume_from": resume_from,
                },
            )

    async def add_run(
        self, slot: datetime, started_at: datetime, outcome: str | None = None
    ) -> uuid.UUID:
        run_id = uuid.uuid4()
        async with self.engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO curie.hook_runs "
                    "(id, agent_id, name, slot_utc, version_id, outcome, started_at) "
                    "VALUES (:id, :a, :n, :slot, :v, :outcome, :started)"
                ),
                {
                    "id": run_id,
                    "a": self.agent_id,
                    "n": HOOK,
                    "slot": slot,
                    "v": self.version_id,
                    "started": started_at,
                    "outcome": outcome,
                },
            )
        return run_id


@contextlib.asynccontextmanager
async def _seed(*, max_usd_per_day: float | None = None) -> AsyncIterator[_Seed]:
    """One agent with one version, one active prod deployment and one binding."""
    engine = create_async_engine(_DB_URL)
    token = uuid.uuid4().hex
    agent_id, version_id, deployment_id, channel_id = (uuid.uuid4() for _ in range(4))
    address = f"C{token[:10].upper()}"
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO curie.agents (id, name, max_usd_per_day) "
                    "VALUES (:id, :name, :usd)"
                ),
                {"id": agent_id, "name": f"cron_agent_{token}", "usd": max_usd_per_day},
            )
            await conn.execute(
                text(
                    "INSERT INTO curie.agent_versions "
                    "(id, agent_id, version_label, bundle_ref, created_by) "
                    "VALUES (:id, :agent_id, :label, :ref, 'kernel-test')"
                ),
                {
                    "id": version_id,
                    "agent_id": agent_id,
                    "label": f"cron_{token}",
                    "ref": f"bundles/cron_{token}.tar.gz",
                },
            )
            await conn.execute(
                text(
                    "INSERT INTO curie.deployments "
                    "(id, agent_id, version_id, environment, status, deployed_at) "
                    "VALUES (:id, :agent_id, :version_id, "
                    "CAST('prod' AS curie.environment), 'active', now() - interval '30 days')"
                ),
                {"id": deployment_id, "agent_id": agent_id, "version_id": version_id},
            )
            await conn.execute(
                text(
                    "INSERT INTO curie.agent_channels "
                    "(id, agent_id, kind, address, endpoint, adapter) "
                    "VALUES (:id, :agent_id, 'slack', :address, :endpoint, :adapter)"
                ),
                {
                    "id": channel_id,
                    "agent_id": agent_id,
                    "address": address,
                    "endpoint": ENDPOINT,
                    "adapter": ADAPTER,
                },
            )
        yield _Seed(
            engine,
            agent_id,
            version_id,
            deployment_id,
            address,
            _slot(),
            f"bundles/cron_{token}.tar.gz",
        )
    finally:
        async with engine.begin() as conn:
            await conn.execute(
                text("DELETE FROM curie.hook_runs WHERE agent_id = :id"), {"id": agent_id}
            )
            await conn.execute(
                text("DELETE FROM curie.agent_channels WHERE id = :id"), {"id": channel_id}
            )
            await conn.execute(
                text("DELETE FROM curie.deployments WHERE id = :id"), {"id": deployment_id}
            )
            await conn.execute(
                text("DELETE FROM curie.agent_versions WHERE id = :id"), {"id": version_id}
            )
            await conn.execute(text("DELETE FROM curie.agents WHERE id = :id"), {"id": agent_id})
        await engine.dispose()


class _Triggers:
    """The fake trigger source: only this seed's version declares the hook."""

    def __init__(self, seed: _Seed, *triggers: dict[str, Any]) -> None:
        self._bundle_ref = seed.bundle_ref
        self._triggers = triggers

    def triggers(self, bundle_ref: str) -> list[dict[str, Any]]:
        return [dict(t) for t in self._triggers] if bundle_ref == self._bundle_ref else []


def _trigger(seed: _Seed, **overrides: Any) -> dict[str, Any]:
    trigger: dict[str, Any] = {
        "type": "cron",
        "name": HOOK,
        "schedule": _schedule(seed.slot),
        "prompt": PROMPT,
        "target": seed.address,
    }
    trigger.update(overrides)
    return {k: v for k, v in trigger.items() if v is not None}


def _not_killed() -> Callable[[uuid.UUID], Awaitable[bool]]:
    async def is_killed(_agent_id: uuid.UUID) -> bool:
        return False

    return is_killed


def _async_redis() -> AsyncRedis:
    return AsyncRedis(
        host=_VALKEY_HOST, port=_VALKEY_PORT, password=_VALKEY_PW or None, decode_responses=True
    )


def _loop(
    engine: AsyncEngine,
    client: AsyncRedis,
    source: object,
    stream: str,
    slot: datetime,
    *,
    is_killed: Callable[[uuid.UUID], Awaitable[bool]] | None = None,
    started_at: datetime | None = None,
) -> CronSchedulerLoop:
    return CronSchedulerLoop(
        engine=engine,
        redis=client,
        source=source,
        is_killed=is_killed or _not_killed(),
        db_schema="curie",
        stream=stream,
        interval_seconds=1.0,
        delivery_budget_s=BUDGET_S,
        default_max_usd_per_day=10.0,
        default_max_output_tokens_per_run=100_000,
        started_at=started_at or slot - timedelta(seconds=30),
    )


def _entries(sync_redis: redis.Redis, stream: str) -> list[QueuedTurn]:
    return [
        QueuedTurn.model_validate_json(fields["payload"])
        for _id, fields in sync_redis.xrange(stream)
    ]


async def _pass_once(
    seed: _Seed,
    stream: str,
    trigger: dict[str, Any],
    **kwargs: Any,
) -> None:
    client = _async_redis()
    try:
        loop = _loop(seed.engine, client, _Triggers(seed, trigger), stream, seed.slot, **kwargs)
        await loop.one_pass(now=seed.slot + timedelta(seconds=30))
    finally:
        await client.aclose()


async def _pass_triggers(
    seed: _Seed,
    stream: str,
    triggers: list[dict[str, Any]],
    *,
    started_at: datetime | None = None,
) -> None:
    client = _async_redis()
    try:
        loop = _loop(
            seed.engine,
            client,
            _Triggers(seed, *triggers),
            stream,
            seed.slot,
            started_at=started_at,
        )
        await loop.one_pass(now=seed.slot + timedelta(seconds=30))
    finally:
        await client.aclose()


def test_two_loops_sharing_one_db_and_stream_fire_a_slot_exactly_once(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    async def body() -> None:
        async with _seed() as seed:
            trigger = _trigger(seed)
            engine_b = create_async_engine(_DB_URL)
            client_a, client_b = _async_redis(), _async_redis()
            try:
                stream = names["stream"]
                a = _loop(seed.engine, client_a, _Triggers(seed, trigger), stream, seed.slot)
                b = _loop(engine_b, client_b, _Triggers(seed, trigger), stream, seed.slot)
                now = seed.slot + timedelta(seconds=30)
                await asyncio.gather(a.one_pass(now=now), b.one_pass(now=now))
            finally:
                await client_a.aclose()
                await client_b.aclose()
                await engine_b.dispose()

            rows = await seed.runs()
            assert len(rows) == 1
            assert rows[0].slot_utc == seed.slot
            assert rows[0].outcome is None  # admitted, in flight
            assert len(_entries(sync_redis, names["stream"])) == 1

    asyncio.run(body())


def test_a_pass_that_loses_the_slot_race_logs_the_loss_at_info(
    sync_redis: redis.Redis, names: dict[str, str], caplog: pytest.LogCaptureFixture
) -> None:
    async def body() -> None:
        async with _seed() as seed:
            trigger = _trigger(seed)
            now = seed.slot + timedelta(seconds=30)
            # The winner admits the slot; its pass logs at INFO either way.
            await _pass_once(seed, names["stream"], trigger)
            # A second replica then races the same slot. The in-flight check
            # only guards other slots (`slot_utc <> :slot`), so this pass
            # reaches the unique INSERT, loses it to the winner's row, and
            # ends the pass with lost=1 and nothing else done (#3013).
            client = _async_redis()
            try:
                loser = _loop(
                    seed.engine, client, _Triggers(seed, trigger), names["stream"], seed.slot
                )
                with caplog.at_level(logging.DEBUG, logger="curie_worker.cron_loop"):
                    caplog.clear()
                    await loser.one_pass(now=now)

                    [lost_line] = [
                        record
                        for record in caplog.records
                        if record.name == "curie_worker.cron_loop"
                        and "cron pass" in record.getMessage()
                    ]
                    assert lost_line.levelno == logging.INFO
                    assert "1 lost" in lost_line.getMessage()

                    # The watermark has moved to `now`, so this next pass has
                    # an empty window: no work and no lost slots. It must stay
                    # at DEBUG, or idle replicas would spam INFO (#3013).
                    caplog.clear()
                    await loser.one_pass(now=now)

                    [idle_line] = [
                        record
                        for record in caplog.records
                        if record.name == "curie_worker.cron_loop"
                        and "cron pass" in record.getMessage()
                    ]
                    assert idle_line.levelno == logging.DEBUG
            finally:
                await client.aclose()

    asyncio.run(body())


def test_killed_agent_records_blocked_and_enqueues_nothing(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    async def killed(_agent_id: uuid.UUID) -> bool:
        return True

    async def body() -> None:
        async with _seed() as seed:
            await _pass_once(seed, names["stream"], _trigger(seed), is_killed=killed)
            rows = await seed.runs()
            assert [(r.slot_utc, r.outcome) for r in rows] == [(seed.slot, "blocked")]
            assert rows[0].ended_at is not None
            assert _entries(sync_redis, names["stream"]) == []

    asyncio.run(body())


def test_spent_budget_records_blocked_and_enqueues_nothing(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    async def body() -> None:
        async with _seed(max_usd_per_day=0) as seed:
            await _pass_once(seed, names["stream"], _trigger(seed))
            rows = await seed.runs()
            assert [(r.slot_utc, r.outcome) for r in rows] == [(seed.slot, "blocked")]
            assert _entries(sync_redis, names["stream"]) == []

    asyncio.run(body())


def test_previous_fire_still_in_flight_skips_the_new_slot(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    async def body() -> None:
        async with _seed() as seed:
            now = seed.slot + timedelta(seconds=30)
            previous = seed.slot - timedelta(days=1)
            await seed.add_run(previous, now - timedelta(seconds=10))
            await _pass_once(seed, names["stream"], _trigger(seed))
            rows = await seed.runs()
            assert [(r.slot_utc, r.outcome) for r in rows] == [
                (previous, None),
                (seed.slot, "skipped"),
            ]
            assert _entries(sync_redis, names["stream"]) == []

    asyncio.run(body())


def test_stale_in_flight_row_is_failed_and_the_new_slot_admitted(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    async def body() -> None:
        async with _seed() as seed:
            now = seed.slot + timedelta(seconds=30)
            previous = seed.slot - timedelta(days=1)
            await seed.add_run(previous, now - timedelta(seconds=BUDGET_S + 120))
            await _pass_once(seed, names["stream"], _trigger(seed))
            rows = await seed.runs()
            assert [(r.slot_utc, r.outcome) for r in rows] == [
                (previous, "failed"),
                (seed.slot, None),
            ]
            assert rows[0].ended_at is not None
            assert len(_entries(sync_redis, names["stream"])) == 1

    asyncio.run(body())


def test_admitted_event_has_the_cron_turn_shape(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    async def body() -> None:
        async with _seed() as seed:
            await _pass_once(seed, names["stream"], _trigger(seed))
            [turn] = _entries(sync_redis, names["stream"])
            agent = str(seed.agent_id)
            assert turn.source == TurnSource.CRON
            assert turn.author == f"cron:{HOOK}"
            assert turn.text == PROMPT
            assert turn.conversation_id == f"hook:{agent}:{HOOK}"
            assert turn.event_id == f"cron:{agent}:{HOOK}:{seed.slot.isoformat()}"
            assert turn.hook_run is not None
            assert (turn.hook_run.agent_id, turn.hook_run.name, turn.hook_run.slot_utc) == (
                agent,
                HOOK,
                seed.slot.isoformat(),
            )
            handle = turn.reply_handle
            assert handle is not None
            assert handle.placeholder is None
            assert (handle.kind, handle.channel, handle.endpoint, handle.adapter) == (
                "slack",
                seed.address,
                ENDPOINT,
                ADAPTER,
            )

    asyncio.run(body())


def test_targetless_hook_mints_a_turn_without_a_reply_handle(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    async def body() -> None:
        async with _seed() as seed:
            await _pass_once(seed, names["stream"], _trigger(seed, target=None))
            [turn] = _entries(sync_redis, names["stream"])
            assert turn.reply_handle is None
            assert turn.hook_run is not None
            assert [r.outcome for r in await seed.runs()] == [None]

    asyncio.run(body())


def test_target_not_bound_to_the_agent_records_failed(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    async def body() -> None:
        async with _seed() as seed:
            unbound = f"C{uuid.uuid4().hex[:10].upper()}"
            await _pass_once(seed, names["stream"], _trigger(seed, target=unbound))
            rows = await seed.runs()
            assert [(r.slot_utc, r.outcome) for r in rows] == [(seed.slot, "failed")]
            assert _entries(sync_redis, names["stream"]) == []

    asyncio.run(body())


def test_loops_resolving_adjacent_slots_of_one_hook_admit_exactly_once(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    """Replicas with different watermarks resolve different slots of one hook.

    The per-hook advisory lock plus the any-other-open-row check must leave one
    admitted event and the other slot recorded skipped, whichever wins.
    """

    async def body() -> None:
        async with _seed() as seed:
            earlier = seed.slot - timedelta(minutes=1)
            trigger = _trigger(seed, schedule="* * * * *")
            engine_b = create_async_engine(_DB_URL)
            client_a, client_b = _async_redis(), _async_redis()
            try:
                stream = names["stream"]
                a = _loop(seed.engine, client_a, _Triggers(seed, trigger), stream, earlier)
                b = _loop(engine_b, client_b, _Triggers(seed, trigger), stream, seed.slot)
                await asyncio.gather(
                    a.one_pass(now=earlier + timedelta(seconds=30)),
                    b.one_pass(now=seed.slot + timedelta(seconds=30)),
                )
            finally:
                await client_a.aclose()
                await client_b.aclose()
                await engine_b.dispose()

            rows = await seed.runs()
            assert [r.slot_utc for r in rows] == [earlier, seed.slot]
            assert sorted(str(r.outcome) for r in rows) == ["None", "skipped"]
            assert len(_entries(sync_redis, names["stream"])) == 1

    asyncio.run(body())


def test_a_raising_hook_does_not_stop_the_agents_later_hooks(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    """``0 0 31 2 *`` passes validation but cronsim raises on it; the valid due
    hook after it on the same agent still fires."""

    async def body() -> None:
        async with _seed() as seed:
            bad = _trigger(seed, name="never", schedule="0 0 31 2 *")
            client = _async_redis()
            try:
                loop = _loop(
                    seed.engine,
                    client,
                    _Triggers(seed, bad, _trigger(seed)),
                    names["stream"],
                    seed.slot,
                )
                summary = await loop.one_pass(now=seed.slot + timedelta(seconds=30))
            finally:
                await client.aclose()
            assert summary.failed == 1
            assert summary.admitted == 1
            assert [(r.slot_utc, r.outcome) for r in await seed.runs()] == [(seed.slot, None)]
            assert len(_entries(sync_redis, names["stream"])) == 1

    asyncio.run(body())


def test_bundle_attached_later_is_read_on_the_next_pass(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    """A version with no bundle_ref must not cache an empty trigger list, or a
    bundle attached later to the same active version is never read (#268)."""

    async def body() -> None:
        async with _seed() as seed:
            calls: list[str] = []

            class _CountingSource:
                def triggers(self, bundle_ref: str) -> list[dict[str, Any]]:
                    calls.append(bundle_ref)
                    return [dict(_trigger(seed))]

            def _target(bundle_ref: str | None) -> _Target:
                return _Target(
                    agent_id=seed.agent_id,
                    agent_name="doesnotmatter",
                    version_id=seed.version_id,
                    bundle_ref=bundle_ref,
                    deployed_at=None,
                    max_usd_per_day=None,
                    max_output_tokens_per_run=None,
                )

            client = _async_redis()
            try:
                loop = _loop(seed.engine, client, _CountingSource(), names["stream"], seed.slot)
                assert await loop._cron_triggers(_target(None)) == []
                assert calls == []

                triggers = await loop._cron_triggers(_target(seed.bundle_ref))
                assert calls == [seed.bundle_ref]
                assert len(triggers) == 1
                assert triggers[0]["name"] == HOOK
            finally:
                await client.aclose()

    asyncio.run(body())


def test_restarted_loop_fires_the_newest_missed_slot_once_and_skips_the_older_ones(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    """#2930: a worker that slept through three hourly slots catches up one."""

    async def body() -> None:
        async with _seed() as seed:
            last = seed.slot - timedelta(hours=3)
            await seed.add_run(last, last)
            async with seed.engine.begin() as conn:
                await conn.execute(
                    text(
                        "UPDATE curie.hook_runs SET outcome = 'ran', ended_at = now() "
                        "WHERE agent_id = :a"
                    ),
                    {"a": seed.agent_id},
                )
            # A fresh process: its own watermark starts just before the newest slot.
            trigger = _trigger(seed, schedule=f"{seed.slot.minute} * * * *")
            await _pass_once(seed, names["stream"], trigger)
            rows = await seed.runs()
            assert [(r.slot_utc, r.outcome) for r in rows] == [
                (last, "ran"),
                (seed.slot - timedelta(hours=2), "skipped"),
                (seed.slot - timedelta(hours=1), "skipped"),
                (seed.slot, None),
            ]
            assert all(r.ended_at is not None for r in rows[1:3])
            entries = _entries(sync_redis, names["stream"])
            assert len(entries) == 1
            assert entries[0].hook_run is not None
            assert entries[0].hook_run.slot_utc == seed.slot.isoformat()

    asyncio.run(body())


def test_restarted_loop_fires_nothing_for_a_slot_past_the_age_bound(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    """A weekly hook down ten days: its newest missed slot is three days old,
    past the 24 h ceiling, so nothing fires and both missed slots are recorded."""

    async def body() -> None:
        async with _seed() as seed:
            missed = seed.slot - timedelta(days=10)
            last = missed - timedelta(weeks=1)
            await seed.add_run(last, last)
            async with seed.engine.begin() as conn:
                await conn.execute(
                    text(
                        "UPDATE curie.hook_runs SET outcome = 'ran', ended_at = now() "
                        "WHERE agent_id = :a"
                    ),
                    {"a": seed.agent_id},
                )
            dow = (missed.weekday() + 1) % 7  # cron: Sunday is 0
            trigger = _trigger(seed, schedule=f"{missed.minute} {missed.hour} * * {dow}")
            await _pass_once(seed, names["stream"], trigger)
            rows = await seed.runs()
            assert [(r.slot_utc, r.outcome) for r in rows] == [
                (last, "ran"),
                (missed, "skipped"),
                (missed + timedelta(weeks=1), "skipped"),
            ]
            assert _entries(sync_redis, names["stream"]) == []

    asyncio.run(body())


def test_catch_up_never_reaches_back_past_the_in_force_deployment(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    """A slot from before the deployment belonged to whatever schedule ran then."""

    async def body() -> None:
        async with _seed() as seed:
            last = seed.slot - timedelta(hours=3)
            await seed.add_run(last, last)
            async with seed.engine.begin() as conn:
                await conn.execute(
                    text(
                        "UPDATE curie.hook_runs SET outcome = 'ran', ended_at = now() "
                        "WHERE agent_id = :a"
                    ),
                    {"a": seed.agent_id},
                )
                await conn.execute(
                    text("UPDATE curie.deployments SET deployed_at = :at WHERE id = :id"),
                    {"at": seed.slot - timedelta(minutes=90), "id": seed.deployment_id},
                )
            trigger = _trigger(seed, schedule=f"{seed.slot.minute} * * * *")
            await _pass_once(seed, names["stream"], trigger)
            rows = await seed.runs()
            assert [(r.slot_utc, r.outcome) for r in rows] == [
                (last, "ran"),
                (seed.slot - timedelta(hours=1), "skipped"),
                (seed.slot, None),
            ]
            assert len(_entries(sync_redis, names["stream"])) == 1

    asyncio.run(body())


def test_a_deployment_after_the_watermark_fires_no_slot_from_before_it(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    """The deployment floor holds even for a hook with no recorded slot."""

    async def body() -> None:
        async with _seed() as seed:
            async with seed.engine.begin() as conn:
                await conn.execute(
                    text("UPDATE curie.deployments SET deployed_at = :at WHERE id = :id"),
                    {"at": seed.slot + timedelta(seconds=10), "id": seed.deployment_id},
                )
            await _pass_once(seed, names["stream"], _trigger(seed))
            assert await seed.runs() == []
            assert _entries(sync_redis, names["stream"]) == []

    asyncio.run(body())


async def _later_pass(seed: _Seed, stream: str, trigger: dict[str, Any], now: datetime) -> None:
    """A pass whose window starts after the deferred slot, so only the retry
    path, never first admission, can put that slot back on the stream."""
    client = _async_redis()
    try:
        loop = _loop(
            seed.engine,
            client,
            _Triggers(seed, trigger),
            stream,
            seed.slot,
            started_at=now - timedelta(seconds=30),
        )
        await loop.one_pass(now=now)
    finally:
        await client.aclose()


def test_deferred_fire_is_retried_on_the_next_tick(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    """#2929: a slot the kernel recorded ``deferred`` fires again on a later
    tick, as the same slot, once more in flight and under a fresh event id so
    the done marker of the deferred delivery cannot swallow it."""

    async def body() -> None:
        async with _seed() as seed:
            now = seed.slot + timedelta(minutes=5)
            await seed.add_run(seed.slot, seed.slot, outcome="deferred")
            await _later_pass(seed, names["stream"], _trigger(seed), now)

            rows = await seed.runs()
            assert [(r.slot_utc, r.outcome) for r in rows] == [(seed.slot, None)]
            assert rows[0].ended_at is None
            entries = _entries(sync_redis, names["stream"])
            assert len(entries) == 1
            turn = entries[0]
            assert turn.source is TurnSource.CRON
            assert turn.hook_run is not None
            assert turn.hook_run.slot_utc == seed.slot.isoformat()
            assert turn.event_id != f"cron:{seed.agent_id}:{HOOK}:{seed.slot.isoformat()}"
            assert turn.reply_handle is not None
            assert turn.reply_handle.channel == seed.address

    asyncio.run(body())


def test_deferred_fire_past_the_catch_up_ceiling_records_skipped(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    """A weekly hook whose thread stayed busy past the 24 h ceiling is not
    fired late: its slot is recorded ``skipped`` and nothing is enqueued."""

    async def body() -> None:
        async with _seed() as seed:
            now = seed.slot + timedelta(hours=25)
            dow = (seed.slot.weekday() + 1) % 7  # cron: Sunday is 0
            trigger = _trigger(seed, schedule=f"{seed.slot.minute} {seed.slot.hour} * * {dow}")
            await seed.add_run(seed.slot, seed.slot, outcome="deferred")
            await _later_pass(seed, names["stream"], trigger, now)

            rows = await seed.runs()
            assert [(r.slot_utc, r.outcome) for r in rows] == [(seed.slot, "skipped")]
            assert rows[0].ended_at is not None
            assert _entries(sync_redis, names["stream"]) == []

    asyncio.run(body())


def test_deferred_fire_past_its_own_interval_records_skipped(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    """A fine-grained schedule ages out at its own interval, well inside the
    ceiling: a five-minute hook deferred six minutes is superseded."""

    async def body() -> None:
        async with _seed() as seed:
            now = seed.slot + timedelta(minutes=6)
            await seed.add_run(seed.slot, seed.slot, outcome="deferred")
            trigger = _trigger(seed, schedule="*/5 * * * *")
            await _later_pass(seed, names["stream"], trigger, now)

            rows = await seed.runs()
            assert (seed.slot, "skipped") in [(r.slot_utc, r.outcome) for r in rows]
            assert all(
                t.hook_run is None or t.hook_run.slot_utc != seed.slot.isoformat()
                for t in _entries(sync_redis, names["stream"])
            )

    asyncio.run(body())


def test_paused_trigger_does_not_fire_while_another_hook_keeps_running(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    """A durable pause suppresses only its named hook on the same agent."""

    async def body() -> None:
        async with _seed() as seed:
            await seed.set_control(
                HOOK,
                paused_at=seed.slot - timedelta(minutes=30),
                resume_from=None,
            )
            paused = _trigger(seed)
            independent = _trigger(seed, name="independent")
            await _pass_triggers(seed, names["stream"], [paused, independent])

            assert await seed.runs(HOOK) == []
            other_rows = await seed.runs("independent")
            assert [(row.slot_utc, row.outcome) for row in other_rows] == [
                (seed.slot, None)
            ]
            entries = _entries(sync_redis, names["stream"])
            assert len(entries) == 1
            assert entries[0].hook_run is not None
            assert entries[0].hook_run.name == "independent"

    asyncio.run(body())


def test_resumed_hook_catches_up_one_slot_after_a_worker_restart(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    """The persisted resume floor exposes the gap to a fresh loop, which still
    fires only the newest eligible slot and records every older one skipped."""

    async def body() -> None:
        async with _seed() as seed:
            paused_at = seed.slot - timedelta(hours=3, minutes=30)
            await seed.set_control(HOOK, paused_at=None, resume_from=paused_at)
            trigger = _trigger(seed, schedule=f"{seed.slot.minute} * * * *")

            # This fresh worker starts after the older missed slots, so they
            # can only be found through the durable resume marker.
            await _pass_once(
                seed,
                names["stream"],
                trigger,
                started_at=seed.slot - timedelta(seconds=10),
            )

            assert [
                (row.slot_utc, row.outcome) for row in await seed.runs(HOOK)
            ] == [
                (seed.slot - timedelta(hours=3), "skipped"),
                (seed.slot - timedelta(hours=2), "skipped"),
                (seed.slot - timedelta(hours=1), "skipped"),
                (seed.slot, None),
            ]
            entries = _entries(sync_redis, names["stream"])
            assert len(entries) == 1
            assert entries[0].hook_run is not None
            assert entries[0].hook_run.slot_utc == seed.slot.isoformat()

    asyncio.run(body())


def test_old_pass_cannot_clear_a_newer_resume_gap() -> None:
    """A repeated pause and resume preserves work missed after a pass snapshot."""

    async def body() -> None:
        async with _seed() as seed:
            floor = seed.slot - timedelta(hours=2)
            await seed.set_control(HOOK, paused_at=None, resume_from=floor)
            async with seed.engine.begin() as conn:
                old_generation = (
                    await conn.execute(
                        text(
                            "SELECT generation FROM curie.schedule_controls "
                            "WHERE agent_id = :agent_id AND name = :name"
                        ),
                        {"agent_id": seed.agent_id, "name": HOOK},
                    )
                ).scalar_one()
                await conn.execute(
                    text(
                        "UPDATE curie.schedule_controls "
                            "SET generation = generation + 2 "
                            "WHERE agent_id = :agent_id AND name = :name"
                    ),
                    {"agent_id": seed.agent_id, "name": HOOK},
                )
            loop = _loop(seed.engine, _async_redis(), _Triggers(seed), "unused", seed.slot)
            try:
                async with seed.engine.begin() as conn:
                    result = await conn.execute(
                        loop._clear_resume_sql,
                        {
                            "agent_id": seed.agent_id,
                            "name": HOOK,
                            "resume_from": floor,
                            "generation": old_generation,
                        },
                    )
                assert result.rowcount == 0
            finally:
                await loop._redis.aclose()

    asyncio.run(body())


def test_pause_during_first_admission_keeps_slot_for_resume(
    names: dict[str, str]
) -> None:
    """A pause after the scheduler snapshot retains its first due slot."""

    async def body() -> None:
        async with _seed() as seed:
            client = _async_redis()
            try:
                trigger = _trigger(seed)
                loop = _loop(
                    seed.engine, client, _Triggers(seed, trigger), names["stream"], seed.slot
                )
                original = loop._admit

                async def pause_before_admit(*args: Any, **kwargs: Any) -> None:
                    await seed.set_control(
                        HOOK, paused_at=seed.slot + timedelta(seconds=1), resume_from=None
                    )
                    await original(*args, **kwargs)

                loop._admit = pause_before_admit
                await loop.one_pass(now=seed.slot + timedelta(seconds=30))
                assert [(row.slot_utc, row.outcome) for row in await seed.runs()] == [
                    (seed.slot, "deferred")
                ]
            finally:
                await client.aclose()

    asyncio.run(body())


def test_paused_queued_slot_retries_once_after_resume(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    """A queued fire held by pause is retried within its catch-up bound."""

    async def body() -> None:
        async with _seed() as seed:
            await seed.add_run(seed.slot, seed.slot, outcome="deferred")
            await seed.set_control(
                HOOK, paused_at=seed.slot + timedelta(seconds=1), resume_from=None
            )
            trigger = _trigger(seed)
            await _later_pass(
                seed, names["stream"], trigger, seed.slot + timedelta(seconds=20)
            )
            assert (await seed.runs())[0].outcome == "deferred"

            await seed.set_control(
                HOOK, paused_at=None, resume_from=seed.slot + timedelta(seconds=1)
            )
            await _later_pass(
                seed, names["stream"], trigger, seed.slot + timedelta(seconds=30)
            )
            assert (await seed.runs())[0].outcome is None
            entries = _entries(sync_redis, names["stream"])
            assert len(entries) == 1
            assert entries[0].hook_run is not None
            assert entries[0].hook_run.slot_utc == seed.slot.isoformat()

    asyncio.run(body())


def test_resume_slot_waits_for_a_pre_pause_fire_to_finish(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    """An earlier running fire cannot consume the resume catch-up slot."""

    async def body() -> None:
        async with _seed() as seed:
            earlier = seed.slot - timedelta(hours=1)
            earlier_id = await seed.add_run(earlier, seed.slot)
            await seed.set_control(
                HOOK, paused_at=None, resume_from=seed.slot - timedelta(minutes=30)
            )
            trigger = _trigger(seed, schedule=f"{seed.slot.minute} * * * *")
            await _later_pass(
                seed, names["stream"], trigger, seed.slot + timedelta(seconds=30)
            )
            assert [(row.slot_utc, row.outcome) for row in await seed.runs()] == [
                (earlier, None),
                (seed.slot, "deferred"),
            ]
            async with seed.engine.begin() as conn:
                await conn.execute(
                    text("UPDATE curie.hook_runs SET outcome = 'ran' WHERE id = :id"),
                    {"id": earlier_id},
                )
            await _later_pass(
                seed, names["stream"], trigger, seed.slot + timedelta(seconds=40)
            )
            assert (await seed.runs())[-1].outcome is None
            assert len(_entries(sync_redis, names["stream"])) == 1

    asyncio.run(body())
