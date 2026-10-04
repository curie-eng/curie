"""Actual owned lifecycle; TEST actors are explicit, @spec PROTECTED-HOOK-SOURCE-2."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
from curie_protected_hooks.source_policy_sql import SourceGate
from curie_worker import run
from curie_worker.config import WorkerConfig
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import create_async_engine


def startup_support() -> Any:
    """Public test setup reuse, @spec PROTECTED-HOOK-SOURCE-2/10."""
    path = Path(__file__).with_name("test_source_worker_startup.py")
    spec = importlib.util.spec_from_file_location("_source_worker_startup_tests", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


startup = startup_support()
worker_db = startup.worker_db
worker_templates = startup.worker_templates


def owner_class() -> Any:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    name = "curie_worker.worker_lifecycle"
    assert importlib.util.find_spec(name) is not None, "SOURCE-2 WorkerResources owner is missing"
    module = __import__(name, fromlist=["WorkerResources"])
    return module.WorkerResources


def test_gate_factory_has_bounded_separate_real_pool(worker_db: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    _, _, url = worker_db
    factory = getattr(run, "create_source_gate_engine", None)
    assert callable(factory), "SOURCE-2 worker dedicated gate engine factory is missing"

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        gate = factory(WorkerConfig(database_url=url))
        work = create_async_engine(url, pool_size=1, max_overflow=0)
        try:
            assert gate.pool is not work.pool
            assert gate.pool.size() == 4
            assert gate.pool._max_overflow == 0
            assert gate.pool.timeout() == 30
            assert gate.pool._pre_ping
            async with gate.connect() as locked, work.connect() as ordinary:
                assert await locked.scalar(
                    text("SELECT pg_backend_pid()")
                ) != await ordinary.scalar(text("SELECT pg_backend_pid()"))
        finally:
            await gate.dispose()
            await work.dispose()

    asyncio.run(scenario())


def test_owner_stops_real_gate_holder_before_disposal_and_closes_once(worker_db: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    _, _, url = worker_db
    cls = owner_class()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        engine = create_async_engine(url, pool_size=1, max_overflow=0)
        owner = cls()
        gate = SourceGate(engine)
        ready, stop = asyncio.Event(), asyncio.Event()
        observations: list[bool] = []

        async def holder() -> None:
            """Explicit TEST actor with actual gate, @spec PROTECTED-HOOK-SOURCE-2."""
            async with gate.hold(uuid.uuid4()):
                ready.set()
                await stop.wait()

        task = asyncio.create_task(holder())
        event.listen(
            engine.sync_engine, "engine_disposed", lambda _: observations.append(task.done())
        )
        owner.register_close("gate", engine.dispose, order=100)
        owner.register_task("TEST-real-gate-holder", task)
        owner.register_stop("TEST-stop", stop.set)
        try:
            async with asyncio.timeout(3):
                await ready.wait()
            async with asyncio.timeout(12):
                await owner.aclose()
            assert task.done() and not task.cancelled()
            assert observations == [True]
            await owner.aclose()
            assert observations == [True]
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.parametrize("primary_present", [False, True])
def test_test_actor_close_error_still_disposes_actual_backend_and_preserves_primary(
    worker_db: Any, primary_present: bool
) -> None:
    """Registry machinery only, @spec PROTECTED-HOOK-SOURCE-2."""
    _, _, url = worker_db
    cls = owner_class()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        engine = create_async_engine(url, pool_size=1, max_overflow=0)
        observer = create_async_engine(url, pool_size=1, max_overflow=0)
        owner = cls()
        async with engine.connect() as connection:
            pid = await connection.scalar(text("SELECT pg_backend_pid()"))
        disposed: list[bool] = []
        event.listen(engine.sync_engine, "engine_disposed", lambda _: disposed.append(True))

        async def fail() -> None:
            """Explicit TEST closer fault, @spec PROTECTED-HOOK-SOURCE-2."""
            raise RuntimeError("TEST-private-error-do-not-reflect")

        owner.register_close("TEST-error", fail, order=1)
        owner.register_close("actual-gate", engine.dispose, order=100)
        primary = ValueError("TEST-primary") if primary_present else None
        try:
            with pytest.raises(Exception) as caught:
                async with asyncio.timeout(7):
                    await owner.aclose(primary=primary)
            if primary is not None:
                assert caught.value is primary
            else:
                assert "TEST-private-error-do-not-reflect" not in str(caught.value)
            assert disposed == [True]
            async with observer.connect() as connection:
                assert not await connection.scalar(
                    text("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=:pid)"),
                    {"pid": pid},
                )
        finally:
            await engine.dispose()
            await observer.dispose()

    asyncio.run(scenario())


@pytest.mark.parametrize("actor", ["producer", "closer"])
def test_owned_process_fatal_deadline_for_test_actor_with_actual_backend(
    worker_db: Any, actor: str
) -> None:
    """Fatal machinery, not real cron qualification, @spec PROTECTED-HOOK-SOURCE-2."""
    support, _, url = worker_db
    owner_class()
    program = '''
import asyncio,json,os,uuid
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from curie_protected_hooks.source_policy_sql import SourceGate
from curie_worker.worker_lifecycle import WorkerResources
async def exercise():
    """@spec PROTECTED-HOOK-SOURCE-2."""
    engine=create_async_engine(os.environ['DATABASE_URL'],pool_size=1,max_overflow=0)
    owner=WorkerResources()
    ready=asyncio.Event()
    async def stubborn():
        """Explicit TEST actor, @spec PROTECTED-HOOK-SOURCE-2."""
        async with SourceGate(engine).hold(uuid.uuid4()) as held:
            pid=await held._connection.scalar(text('SELECT pg_backend_pid()'))
            print(json.dumps({'pid':pid}),flush=True)
            ready.set()
            while True:
                try: await asyncio.Event().wait()
                except asyncio.CancelledError: pass
    owner.register_close('actual-engine',engine.dispose,order=100)
    if os.environ['SOURCE_TEST_ACTOR']=='producer':
        task=asyncio.create_task(stubborn())
        owner.register_task('TEST-stubborn-producer',task)
        await ready.wait()
    else:
        owner.register_close('TEST-stubborn-closer',stubborn,order=1)
    await owner.aclose()
    print('unexpected-cleanup-success',flush=True)
asyncio.run(exercise())
'''
    started = time.monotonic()
    process = subprocess.Popen(
        [sys.executable, "-c", program],
        env=dict(os.environ, DATABASE_URL=url, SOURCE_TEST_ACTOR=actor),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        stdout, _ = process.communicate(timeout=14 if actor == "producer" else 9)
        assert process.returncode == 1, "unjoined TEST actor must fatal-exit owned process"
        elapsed = time.monotonic() - started
        assert elapsed <= (12 if actor == "producer" else 7)
        pid = json.loads(stdout)["pid"]
        assert support.sql_dicts(
            "SELECT count(*) AS peers FROM pg_stat_activity WHERE pid=:pid", {"pid": pid}
        ) == [{"peers": 0}]
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=3)
