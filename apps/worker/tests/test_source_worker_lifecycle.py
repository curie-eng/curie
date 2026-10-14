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
    worker_db: Any, actor: str, tmp_path: Path
) -> None:
    """Fatal machinery, not real cron qualification, @spec PROTECTED-HOOK-SOURCE-2."""
    assert_fatal_deadline(worker_db, actor, tmp_path)


def test_fatal_deadline_excludes_bounded_interpreter_startup(
    worker_db: Any, tmp_path: Path
) -> None:
    """Slow startup must not consume SOURCE-2's owned close budget."""
    assert_fatal_deadline(worker_db, "closer", tmp_path, startup_delay=3)


@pytest.mark.parametrize(
    ("fault", "message"),
    [("late_exit", "owned cleanup exceeded"), ("wrong_exit", "must fatal-exit")],
)
def test_fatal_deadline_still_rejects_broken_cleanup(
    worker_db: Any, tmp_path: Path, fault: str, message: str
) -> None:
    """Negative controls catch late or incorrect SOURCE-2 process exits."""
    with pytest.raises(AssertionError, match=message):
        assert_fatal_deadline(worker_db, "closer", tmp_path, fault=fault)


def assert_fatal_deadline(
    worker_db: Any,
    actor: str,
    tmp_path: Path,
    *,
    startup_delay: float = 0,
    fault: str = "",
) -> None:
    """Fatal machinery, not real cron qualification, @spec PROTECTED-HOOK-SOURCE-2."""
    support, _, url = worker_db
    owner_class()
    program = '''
import time,os
# Controlled startup delay, before worker imports and owned cleanup.
time.sleep(float(os.environ['SOURCE_TEST_STARTUP_DELAY']))
import asyncio,json,uuid
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from curie_protected_hooks.source_policy_sql import SourceGate
from curie_worker.worker_lifecycle import WorkerResources
from pathlib import Path
class TestOwner(WorkerResources):
    def _fatal(self, code):
        # Explicit negative controls; the actual owner is unchanged.
        if os.environ['SOURCE_TEST_FAULT']=='late_exit': time.sleep(3)
        if os.environ['SOURCE_TEST_FAULT']=='wrong_exit': os._exit(0)
        super()._fatal(code)
async def exercise():
    """@spec PROTECTED-HOOK-SOURCE-2."""
    engine=create_async_engine(os.environ['DATABASE_URL'],pool_size=1,max_overflow=0)
    owner=TestOwner()
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
    # SOURCE-2 budgets owned cleanup, independently of interpreter startup.
    marker=Path(os.environ['SOURCE_TEST_CLEANUP_MARKER'])
    pending=marker.with_suffix('.pending')
    pending.write_text(str(time.monotonic()))
    pending.replace(marker)
    await owner.aclose()
    print('unexpected-cleanup-success',flush=True)
asyncio.run(exercise())
'''
    marker = tmp_path / "cleanup-started"
    startup_deadline = time.monotonic() + 20
    process = subprocess.Popen(
        [sys.executable, "-c", program],
        env=dict(
            os.environ,
            DATABASE_URL=url,
            SOURCE_TEST_ACTOR=actor,
            SOURCE_TEST_STARTUP_DELAY=str(startup_delay),
            SOURCE_TEST_FAULT=fault,
            SOURCE_TEST_CLEANUP_MARKER=str(marker),
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        # Startup and owned cleanup each have an independent finite watchdog.
        while not marker.exists():
            assert process.poll() is None, "child exited before owned cleanup began"
            assert time.monotonic() < startup_deadline, "child startup watchdog expired"
            time.sleep(0.01)
        started = float(marker.read_text())
        assert started < startup_deadline, "child startup watchdog expired"
        stdout, _ = process.communicate(timeout=14 if actor == "producer" else 9)
        elapsed = time.monotonic() - started
        pid = json.loads(stdout)["pid"]
        assert support.sql_dicts(
            "SELECT count(*) AS peers FROM pg_stat_activity WHERE pid=:pid", {"pid": pid}
        ) == [{"peers": 0}]
        assert process.returncode == 1, "unjoined TEST actor must fatal-exit owned process"
        assert elapsed <= (12 if actor == "producer" else 7), "owned cleanup exceeded its budget"
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=3)
