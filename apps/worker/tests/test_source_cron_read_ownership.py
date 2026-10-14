"""Actual S3 read ownership; TEST driver only, @spec PROTECTED-HOOK-SOURCE-2."""

from __future__ import annotations

import asyncio
import importlib.util
import io
import json
import os
import subprocess
import sys
import tarfile
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from curie_protected_hooks.source_policy_sql import SourceGate
from curie_test_support.valkey import connect_or_skip
from curie_worker.bundle_store import BundleStore
from curie_worker.config import WorkerConfig
from curie_worker.cron_loop import BundleTriggerSource, CronSchedulerLoop, _Target
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


def public_helper(name: str) -> Any:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    path = Path(__file__).with_name(name + ".py")
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


startup = public_helper("test_source_worker_startup")
relay_support = public_helper("_source_s3_body_relay")
worker_db = startup.worker_db
worker_templates = startup.worker_templates
CRON = {"type": "cron", "name": "read-only-test", "schedule": "0 3 * * *", "prompt": "Ask."}


@pytest.fixture
def stored_bundle() -> Any:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    config = WorkerConfig()
    store = BundleStore(config)
    key = "test/source-cron-read/" + uuid.uuid4().hex + ".tar.gz"
    data = json.dumps({"name": "test-read-only", "triggers": [CRON, {"type": "event"}]}).encode()
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        info = tarfile.TarInfo("plugin.json")
        info.size = len(data)
        archive.addfile(info, io.BytesIO(data))
    blob = buffer.getvalue()
    store._client.put_object(Bucket=config.bundle_bucket, Key=key, Body=blob)
    try:
        assert store.get(key) == blob
        yield config, key, blob
    finally:
        store._client.delete_object(Bucket=config.bundle_bucket, Key=key)
        store._client.close()


def actual_loop(config: WorkerConfig, engine: Any, redis: Redis) -> CronSchedulerLoop:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    source = BundleTriggerSource(
        BundleStore(config),
        max_uncompressed_bytes=1000000,
        max_compression_ratio=1000,
        max_members=10,
    )

    async def never_killed(_: uuid.UUID) -> bool:
        """Unused external kill boundary, @spec PROTECTED-HOOK-SOURCE-2."""
        return False

    return CronSchedulerLoop(
        engine=engine,
        redis=redis,
        source=source,
        is_killed=never_killed,
        db_schema="curie",
        stream="test:source-cron-read:" + uuid.uuid4().hex,
        interval_seconds=1,
        claim_lease_s=60,
        default_max_usd_per_day=1,
        default_max_output_tokens_per_run=1,
    )


def target(key: str) -> _Target:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    return _Target(uuid.uuid4(), "test", uuid.uuid4(), key, datetime.now(UTC), 1, 1)


def test_genuine_s3_relay_bytes_actual_cron_filter_and_cache(
    worker_db: Any, stored_bundle: Any
) -> None:
    """Normal network fixture control, @spec PROTECTED-HOOK-SOURCE-2."""
    _, _, url = worker_db
    original, key, blob = stored_bundle
    relay = relay_support.S3BodyRelay(
        original.s3_endpoint_url, "/" + original.bundle_bucket + "/" + key, hold=False
    )
    endpoint = relay.start()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        config = original.model_copy(update={"database_url": url, "s3_endpoint_url": endpoint})
        engine = create_async_engine(url)
        redis = Redis(**config.valkey_client_kwargs())
        loop = actual_loop(config, engine, redis)
        try:
            assert await asyncio.to_thread(loop._source._reader.get, key) == blob
            item = target(key)
            assert await loop._cron_triggers(item) == [CRON]
            assert await loop._cron_triggers(item) == [CRON]
            assert relay.requests == 2 and not relay.errors
        finally:
            loop._source._reader._client.close()
            await redis.aclose()
            await engine.dispose()

    try:
        asyncio.run(asyncio.wait_for(scenario(), 10))
    finally:
        relay.close()


def test_actual_s3_thread_stays_owned_on_cancel_until_genuine_body_released(
    worker_db: Any, stored_bundle: Any
) -> None:
    """Actual thread/gate only, @spec PROTECTED-HOOK-SOURCE-2."""
    _, _, url = worker_db
    original, key, _ = stored_bundle
    relay = relay_support.S3BodyRelay(
        original.s3_endpoint_url, "/" + original.bundle_bucket + "/" + key, hold=True
    )
    endpoint = relay.start()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        config = original.model_copy(update={"database_url": url, "s3_endpoint_url": endpoint})
        engine = create_async_engine(url, pool_size=1, max_overflow=0)
        observer = create_async_engine(url)
        redis = Redis(**config.valkey_client_kwargs())
        loop = actual_loop(config, engine, redis)
        pid = None

        async def read() -> None:
            """TEST driver with actual gate, @spec PROTECTED-HOOK-SOURCE-2."""
            nonlocal pid
            async with SourceGate(engine).hold(uuid.uuid4()) as held:
                pid = await held._connection.scalar(text("SELECT pg_backend_pid()"))
                await loop._cron_triggers(target(key))

        task = asyncio.create_task(read())
        try:
            assert await asyncio.to_thread(relay.body_held.wait, 4)
            assert not relay.errors and pid is not None
            task.cancel()
            done, _ = await asyncio.wait({task}, timeout=0.2)
            assert not done, "cancelled cron must retain ownership of actual live read thread"
            async with observer.connect() as connection:
                assert await connection.scalar(
                    text(
                        "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=:pid "
                        "AND locktype='advisory' AND granted)"
                    ),
                    {"pid": pid},
                ), "the source gate must remain held while the genuine body is blocked"
            relay.release.set()
            with pytest.raises(asyncio.CancelledError):
                async with asyncio.timeout(5):
                    await task
            await engine.dispose()
            async with observer.connect() as connection:
                assert not await connection.scalar(
                    text("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=:pid)"),
                    {"pid": pid},
                )
        finally:
            relay.release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            loop._source._reader._client.close()
            await redis.aclose()
            await engine.dispose()
            await observer.dispose()

    try:
        asyncio.run(asyncio.wait_for(scenario(), 12))
    finally:
        relay.close()


def test_owned_process_real_s3_read_uses_producer_fatal_deadline_not_detached_thread(
    worker_db: Any, stored_bundle: Any
) -> None:
    """Registry TEST driver, not whole Runtime, @spec PROTECTED-HOOK-SOURCE-2."""
    support, _, url = worker_db
    original, key, _ = stored_bundle
    relay = relay_support.S3BodyRelay(
        original.s3_endpoint_url, "/" + original.bundle_bucket + "/" + key, hold=True
    )
    endpoint = relay.start()
    client = connect_or_skip()
    assert client.ping()
    client.close()
    program = '''
import asyncio,json,os,uuid
from datetime import UTC,datetime
from curie_worker.config import WorkerConfig
from curie_worker.bundle_store import BundleStore
from curie_worker.cron_loop import BundleTriggerSource,CronSchedulerLoop,_Target
from curie_worker.worker_lifecycle import WorkerResources
from curie_protected_hooks.source_policy_sql import SourceGate
from redis.asyncio import Redis
from sqlalchemy import event,text
from sqlalchemy.ext.asyncio import create_async_engine
async def exercise():
    """@spec PROTECTED-HOOK-SOURCE-2."""
    config=WorkerConfig()
    gate=create_async_engine(config.database_url,pool_size=1,max_overflow=0)
    work=create_async_engine(config.database_url)
    redis=Redis(**config.valkey_client_kwargs())
    source=BundleTriggerSource(BundleStore(config),max_uncompressed_bytes=1000000,max_compression_ratio=1000,max_members=10)
    async def never_killed(_):
        """@spec PROTECTED-HOOK-SOURCE-2."""
        return False
    loop=CronSchedulerLoop(engine=work,redis=redis,source=source,is_killed=never_killed,db_schema='curie',stream='test:read-driver',interval_seconds=1,claim_lease_s=60,default_max_usd_per_day=1,default_max_output_tokens_per_run=1)
    owner=WorkerResources()
    def disposed(_):
        """@spec PROTECTED-HOOK-SOURCE-2."""
        print(json.dumps({'disposed':True}),flush=True)
    event.listen(gate.sync_engine,'engine_disposed',disposed)
    event.listen(work.sync_engine,'engine_disposed',disposed)
    owner.register_close('work',work.dispose,order=90)
    owner.register_close('gate',gate.dispose,order=100)
    async def read():
        """Actual S3/gate TEST driver, @spec PROTECTED-HOOK-SOURCE-2."""
        async with SourceGate(gate).hold(uuid.uuid4()) as held:
            pid=await held._connection.scalar(text('SELECT pg_backend_pid()'))
            print(json.dumps({'pid':pid}),flush=True)
            item=_Target(uuid.uuid4(),'test',uuid.uuid4(),os.environ['SOURCE_READ_KEY'],datetime.now(UTC),1,1)
            await loop._cron_triggers(item)
    task=asyncio.create_task(read())
    owner.register_task('actual-S3-read-driver',task)
    await asyncio.to_thread(__import__('sys').stdin.readline)
    await owner.aclose()
asyncio.run(exercise())
'''
    process = subprocess.Popen(
        [sys.executable, "-c", program],
        env=dict(os.environ, DATABASE_URL=url, S3_ENDPOINT_URL=endpoint, SOURCE_READ_KEY=key),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert relay.body_held.wait(5), "genuine owned S3 body must be blocked before fatal control"
        assert not relay.errors
        started = time.monotonic()
        stdout, _ = process.communicate("go\n", timeout=14)
        assert process.returncode == 1, (
            "live genuine S3 thread must keep producer pending to fatal exit"
        )
        assert time.monotonic() - started <= 11.5
        events = [json.loads(line) for line in stdout.splitlines()]
        assert not any(item.get("disposed") for item in events)
        pid = next(item["pid"] for item in events if "pid" in item)
        assert support.sql_dicts(
            "SELECT count(*) AS peers FROM pg_stat_activity WHERE pid=:pid", {"pid": pid}
        ) == [{"peers": 0}]
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=3)
        relay.close()
