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
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import redis
from aci_protocol import QueuedTurn, TurnSource
from curie_worker.cron_loop import CronSchedulerLoop
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
    address: str
    slot: datetime

    async def runs(self) -> list[Any]:
        async with self.engine.connect() as conn:
            return list(
                (
                    await conn.execute(
                        text(
                            "SELECT id, outcome, slot_utc, ended_at FROM curie.hook_runs "
                            "WHERE agent_id = :a AND name = :n ORDER BY slot_utc"
                        ),
                        {"a": self.agent_id, "n": HOOK},
                    )
                ).all()
            )

    async def add_run(self, slot: datetime, started_at: datetime) -> uuid.UUID:
        run_id = uuid.uuid4()
        async with self.engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO curie.hook_runs "
                    "(id, agent_id, name, slot_utc, version_id, outcome, started_at) "
                    "VALUES (:id, :a, :n, :slot, :v, NULL, :started)"
                ),
                {
                    "id": run_id,
                    "a": self.agent_id,
                    "n": HOOK,
                    "slot": slot,
                    "v": self.version_id,
                    "started": started_at,
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
                    "(id, agent_id, version_label, created_by) "
                    "VALUES (:id, :agent_id, :label, 'kernel-test')"
                ),
                {"id": version_id, "agent_id": agent_id, "label": f"cron_{token}"},
            )
            await conn.execute(
                text(
                    "INSERT INTO curie.deployments "
                    "(id, agent_id, version_id, environment, status, deployed_at) "
                    "VALUES (:id, :agent_id, :version_id, "
                    "CAST('prod' AS curie.environment), 'active', now())"
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
        yield _Seed(engine, agent_id, version_id, address, _slot())
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

    def __init__(self, seed: _Seed, trigger: dict[str, Any]) -> None:
        self._version = str(seed.version_id)
        self._trigger = trigger

    def triggers(self, agent_id: str, version_id: str) -> list[dict[str, Any]]:
        return [dict(self._trigger)] if version_id == self._version else []


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
        started_at=slot - timedelta(seconds=30),
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
