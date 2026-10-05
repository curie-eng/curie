"""Real cron producer fencing, @spec PROTECTED-HOOK-SOURCE-2/10."""

from __future__ import annotations

import asyncio
import importlib.util
import io
import json
import tarfile
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from aci_protocol import QueuedTurn
from curie_protected_hooks.source_policy_sql import (
    SourceGate,
    SourceGateInvalid,
    SourceSnapshotUnavailable,
)
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from curie_worker.bundle_store import BundleStore
from curie_worker.config import WorkerConfig
from curie_worker.cron_loop import BundleTriggerSource, CronPassSummary, CronSchedulerLoop
from curie_worker.hook_source_guard import CronHookSourceGuard
from curie_worker.killswitch import KillSwitch, kill_key
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


def public_guard_setup() -> Any:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    path = Path(__file__).with_name("test_source_cron_guard.py")
    spec = importlib.util.spec_from_file_location("_source_cron_producer_setup", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


guard_setup = public_guard_setup()
worker_db = guard_setup.worker_db
worker_templates = guard_setup.worker_templates
HOOK = "nightly"


class Campaign:
    """Actual fixture resources only, @spec PROTECTED-HOOK-SOURCE-2/10."""

    def __init__(self, support: Any, url: str) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        self.support, self.url = support, url
        self.agent, self.version, self.deployment = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        self.slot = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=1)
        self.now = self.slot + timedelta(seconds=20)
        self.start = self.slot - timedelta(seconds=1)
        self.config = WorkerConfig(
            database_url=url,
            valkey_host=VALKEY_HOST,
            valkey_port=VALKEY_PORT,
            valkey_password=VALKEY_PW,
        )
        self.store = BundleStore(self.config)
        self.keys: list[str] = []
        self.stream = "test:source-cron-integration:" + uuid.uuid4().hex
        self.address = "C" + uuid.uuid4().hex[:10].upper()
        self.trigger = dict(
            type="cron",
            name=HOOK,
            schedule="* * * * *",
            prompt="Read-only probe.",
            target=self.address,
        )
        self.key = self.bundle([self.trigger])
        self.support.sql_dicts(
            "INSERT INTO curie.agents(id,name,max_usd_per_day) VALUES(:id,:name,10)",
            dict(id=self.agent, name="source-cron-" + self.agent.hex),
        )
        self.add_version(self.version, self.key)
        self.support.sql_dicts(
            "INSERT INTO curie.deployments "
            "(id,agent_id,version_id,environment,status,deployed_at) "
            "VALUES(:id,:agent,:version,CAST('prod' AS curie.environment),'active',:at)",
            dict(
                id=self.deployment,
                agent=self.agent,
                version=self.version,
                at=self.slot - timedelta(days=30),
            ),
        )
        self.support.sql_dicts(
            "INSERT INTO curie.agent_channels(id,agent_id,kind,address,endpoint,adapter) "
            "VALUES(:id,:agent,'slack',:address,NULL,'default')",
            dict(id=uuid.uuid4(), agent=self.agent, address=self.address),
        )

    def bundle(self, triggers: list[dict[str, Any]]) -> str:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        body = json.dumps(dict(name="test-source-cron", triggers=triggers)).encode()
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
            info = tarfile.TarInfo("plugin.json")
            info.size = len(body)
            archive.addfile(info, io.BytesIO(body))
        key = "test/source-cron-integration/" + uuid.uuid4().hex + ".tar.gz"
        self.store._client.put_object(
            Bucket=self.config.bundle_bucket, Key=key, Body=buffer.getvalue()
        )
        self.keys.append(key)
        return key

    def add_version(self, version: uuid.UUID, key: str) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        self.support.sql_dicts(
            "INSERT INTO curie.agent_versions "
            "(id,agent_id,version_label,bundle_ref,created_by) "
            "VALUES(:id,:agent,:label,:ref,'test-source')",
            dict(id=version, agent=self.agent, label="test-" + version.hex, ref=key),
        )

    async def runs(self, engine: Any) -> list[Any]:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with engine.connect() as connection:
            return list(
                (
                    await connection.execute(
                        text(
                            "SELECT id,name,version_id,outcome,slot_utc,ended_at "
                            "FROM curie.hook_runs WHERE agent_id=:agent ORDER BY slot_utc"
                        ),
                        dict(agent=self.agent),
                    )
                )
                .mappings()
                .all()
            )

    @asynccontextmanager
    async def runtime(self, *, guarded: bool = True) -> AsyncIterator[Any]:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        gate = create_async_engine(self.url, pool_size=2, max_overflow=0, pool_timeout=2)
        work = create_async_engine(self.url, pool_size=1, max_overflow=0, pool_timeout=2)
        observer = create_async_engine(self.url)
        redis = Redis(**self.config.valkey_client_kwargs())
        switch = KillSwitch(redis, on_kill=self.no_interrupt)
        source = BundleTriggerSource(
            BundleStore(self.config),
            max_uncompressed_bytes=1000000,
            max_compression_ratio=1000,
            max_members=10,
        )
        guard = CronHookSourceGuard(SourceGate(gate), work)
        kwargs = dict(source_guard=guard) if guarded else {}
        try:
            loop = CronSchedulerLoop(
                engine=work,
                redis=redis,
                source=source,
                is_killed=switch.is_killed,
                db_schema="curie",
                stream=self.stream,
                interval_seconds=1,
                claim_lease_s=60,
                default_max_usd_per_day=1,
                default_max_output_tokens_per_run=100,
                started_at=self.start,
                **kwargs,
            )
            yield loop, guard, work, observer, redis
        finally:
            source._reader._client.close()
            await redis.delete(self.stream, kill_key(self.agent))
            await redis.aclose()
            await gate.dispose()
            await work.dispose()
            await observer.dispose()

    async def no_interrupt(self, _: uuid.UUID) -> None:
        """Unused callback; kill check is real Valkey, @spec PROTECTED-HOOK-SOURCE-2."""

    def close(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        for key in self.keys:
            self.store._client.delete_object(Bucket=self.config.bundle_bucket, Key=key)
        self.store._client.close()


@pytest.fixture
def campaign(worker_db: Any) -> Iterator[Campaign]:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    support, _, url = worker_db
    actual = Campaign(support, url)
    try:
        yield actual
    finally:
        actual.close()


async def wait_advisory(observer: Any, task: asyncio.Task[Any]) -> None:
    """Actual PG lock proof, @spec PROTECTED-HOOK-SOURCE-2."""
    async with asyncio.timeout(5):
        while True:
            assert not task.done(), "production source gate was not awaited"
            async with observer.connect() as connection:
                waiting = await connection.scalar(
                    text(
                        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity "
                        "WHERE datname=current_database() AND wait_event='advisory')"
                    )
                )
            if waiting:
                return
            await asyncio.sleep(0.01)


def test_actual_ordinary_bundle_claim_and_queue_preserve_payload(campaign: Campaign) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with campaign.runtime() as (loop, _, _, observer, redis):
            summary = await loop.one_pass(campaign.now)
            assert summary.admitted == 1
            rows = await campaign.runs(observer)
            assert (
                len(rows) == 1
                and rows[0]["outcome"] is None
                and rows[0]["version_id"] == campaign.version
            )
            queued = await redis.xrange(campaign.stream)
            assert len(queued) == 1
            turn = QueuedTurn.model_validate_json(queued[0][1]["payload"])
            assert turn.text == campaign.trigger["prompt"]
            assert turn.reply_handle.channel == campaign.address
            assert await loop.one_pass(campaign.now) is not None
            assert await redis.xlen(campaign.stream) == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["ordinary", "protected", None])
@pytest.mark.parametrize("raw_name", [HOOK, "Nightly Summary/legacy"])
def test_configured_exact_sources_cannot_mutate_or_enqueue(
    campaign: Campaign, mode: str | None, raw_name: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    if raw_name != HOOK:
        campaign.trigger["name"] = raw_name
        key = campaign.bundle([campaign.trigger])
        campaign.support.sql_dicts(
            "UPDATE curie.agent_versions SET bundle_ref=:key WHERE id=:id",
            dict(key=key, id=campaign.version),
        )
    guard_setup.seed(campaign.support, campaign.agent, raw_name, mode=mode)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        async with campaign.runtime() as (loop, _, _, observer, redis):
            summary = await loop.one_pass(campaign.now)
            assert not await campaign.runs(observer) and await redis.xlen(campaign.stream) == 0
            assert summary.admitted == summary.retried == summary.reclaimed == 0

    asyncio.run(scenario())


def test_missing_guard_sentinel_refuses_discovered_ordinary_source(campaign: Campaign) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with campaign.runtime(guarded=False) as (loop, _, _, observer, redis):
            await loop.one_pass(campaign.now)
            assert not await campaign.runs(observer) and await redis.xlen(campaign.stream) == 0

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "change", ["new_version", "remove_hook", "budget", "pause", "kill", "unbind", "pending_history"]
)
def test_outer_gate_wait_holds_no_work_and_uses_fresh_target_decisions(
    campaign: Campaign, change: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    fresh_version, fresh_key = None, None
    if change in {"new_version", "remove_hook"}:
        fresh_version = uuid.uuid4()
        triggers = (
            []
            if change == "remove_hook"
            else [dict(campaign.trigger, prompt="Fresh immutable version.")]
        )
        fresh_key = campaign.bundle(triggers)
        campaign.add_version(fresh_version, fresh_key)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        async with campaign.runtime() as (loop, guard, work, observer, redis):
            task = None
            try:
                async with guard.source_gate.hold(campaign.agent):
                    task = asyncio.create_task(loop.one_pass(campaign.now))
                    await wait_advisory(observer, task)
                    assert work.pool.checkedout() == 0
                    async with work.begin() as connection:
                        assert await connection.scalar(text("SELECT 1")) == 1
                        if change in {"new_version", "remove_hook"}:
                            await connection.execute(
                                text(
                                    "UPDATE curie.deployments SET version_id=:version WHERE id=:id"
                                ),
                                dict(version=fresh_version, id=campaign.deployment),
                            )
                        elif change == "budget":
                            await connection.execute(
                                text("UPDATE curie.agents SET max_usd_per_day=0 WHERE id=:id"),
                                dict(id=campaign.agent),
                            )
                        elif change == "pause":
                            await connection.execute(
                                text(
                                    "INSERT INTO curie.schedule_controls(agent_id,name,paused_at) "
                                    "VALUES(:agent,:name,now())"
                                ),
                                dict(agent=campaign.agent, name=HOOK),
                            )
                        elif change == "unbind":
                            await connection.execute(
                                text("DELETE FROM curie.agent_channels WHERE agent_id=:agent"),
                                dict(agent=campaign.agent),
                            )
                        elif change == "pending_history":
                            await connection.execute(
                                text(
                                    "INSERT INTO curie.hook_source_operations "
                                    "(agent_id,hook,operation_id,intent_sha256,status,generation) "
                                    "VALUES(:agent,:hook,:operation,:intent,'pending',1)"
                                ),
                                dict(
                                    agent=campaign.agent,
                                    hook=HOOK,
                                    operation=uuid.uuid4(),
                                    intent="a" * 64,
                                ),
                            )
                    if change == "kill":
                        await redis.set(kill_key(campaign.agent), "1")
                summary = await task
                rows, queue = await campaign.runs(observer), await redis.xrange(campaign.stream)
                if change == "new_version":
                    assert len(rows) == len(queue) == 1 and rows[0]["version_id"] == fresh_version
                    assert (
                        QueuedTurn.model_validate_json(queue[0][1]["payload"]).text
                        == "Fresh immutable version."
                    )
                elif change in {"budget", "kill"}:
                    assert len(rows) == 1 and rows[0]["outcome"] == "blocked" and not queue
                elif change == "unbind":
                    assert len(rows) == 1 and rows[0]["outcome"] == "failed" and not queue
                else:
                    assert not rows and not queue
                assert summary.reclaimed == 0
            finally:
                if task is not None and not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    asyncio.run(asyncio.wait_for(scenario(), 12))


@pytest.mark.parametrize("method", ["insert", "skip", "admit", "reclaim", "retry", "enqueue"])
def test_direct_effect_helpers_omit_context_and_refuse(campaign: Campaign, method: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with campaign.runtime(guarded=False) as (loop, _, work, observer, redis):
            target = (await loop._targets())[0]
            summary = CronPassSummary()
            with pytest.raises((SourceGateInvalid, SourceSnapshotUnavailable)):
                if method == "insert":
                    async with work.begin() as connection:
                        await loop._insert(connection, target, HOOK, campaign.slot, None)
                elif method == "skip":
                    await loop._skip(target, HOOK, [], summary)
                elif method == "admit":
                    await loop._admit(target, campaign.trigger, campaign.slot, summary, False)
                elif method == "reclaim":
                    async with work.begin() as connection:
                        await loop._lock_and_reclaim(
                            connection, target, HOOK, campaign.slot, summary
                        )
                elif method == "retry":
                    await loop._retry_deferred(
                        target, campaign.trigger, "UTC", campaign.now, summary
                    )
                else:
                    await loop._enqueue(target, campaign.trigger, None, campaign.slot, uuid.uuid4())
            assert not await campaign.runs(observer) and await redis.xlen(campaign.stream) == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("identity", ["other_guard", "wrong_name", "expired_scope", "other_task"])
def test_enqueue_requires_exact_real_context_identity(campaign: Campaign, identity: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with campaign.runtime() as (loop, guard, work, _, redis):
            target = (await loop._targets())[0]
            source_guard = (
                CronHookSourceGuard(guard.source_gate, work) if identity == "other_guard" else guard
            )
            scope = source_guard.locked_snapshot(campaign.agent, HOOK)
            async with scope as context:
                if identity == "expired_scope":
                    pass
                else:
                    trigger = (
                        dict(campaign.trigger, name="Nightly")
                        if identity == "wrong_name"
                        else campaign.trigger
                    )

                    async def attempt() -> None:
                        """@spec PROTECTED-HOOK-SOURCE-2."""
                        with pytest.raises(SourceGateInvalid):
                            await loop._enqueue(
                                target,
                                trigger,
                                None,
                                campaign.slot,
                                uuid.uuid4(),
                                source_context=context,
                            )

                    if identity == "other_task":
                        await asyncio.create_task(attempt())
                    else:
                        await attempt()
            if identity == "expired_scope":
                with pytest.raises(SourceGateInvalid):
                    await loop._enqueue(
                        target,
                        campaign.trigger,
                        None,
                        campaign.slot,
                        uuid.uuid4(),
                        source_context=context,
                    )
            assert await redis.xlen(campaign.stream) == 0

    asyncio.run(scenario())
