"""Real Runtime ownership with no cluster requests, @spec PROTECTED-HOOK-SOURCE-2."""

from __future__ import annotations

import asyncio
import importlib.util
import uuid
from pathlib import Path
from typing import Any

import pytest
from curie_protected_hooks.source_policy_sql import SourceGate
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from curie_worker import run
from curie_worker.config import WorkerConfig
from curie_worker.sandbox.k8s import k8s_config
from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import create_async_engine


def startup_support() -> Any:
    """Public setup only, @spec PROTECTED-HOOK-SOURCE-2/10."""
    path = Path(__file__).with_name("test_source_worker_startup.py")
    spec = importlib.util.spec_from_file_location("_source_worker_composition_setup", path)
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
    return __import__(name, fromlist=["WorkerResources"]).WorkerResources


def config(url: str) -> WorkerConfig:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    token = uuid.uuid4().hex
    return WorkerConfig(
        database_url=url,
        valkey_host=VALKEY_HOST,
        valkey_port=VALKEY_PORT,
        valkey_password=VALKEY_PW,
        internal_worker_token="",
        workspace_enabled=False,
        publication_enabled=False,
        slack_bot_token="",
        stream="test:source-composition:runs:" + token,
        eval_stream="test:source-composition:evals:" + token,
        consumer_group="test-source-composition-" + token,
    )


def external_cluster_config_noop() -> None:
    """External configuration boundary only, @spec PROTECTED-HOOK-SOURCE-2."""


async def backend_gone(observer: Any, pid: int) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    async with asyncio.timeout(5):
        while True:
            async with observer.connect() as connection:
                if not await connection.scalar(
                    text("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=:pid)"),
                    {"pid": pid},
                ):
                    return
            await asyncio.sleep(0.01)


def test_actual_runtime_four_gate_slots_cancel_waiter_and_work_pool_stays_usable(
    worker_db: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No cluster operations are exercised, @spec PROTECTED-HOOK-SOURCE-2."""
    _, _, url = worker_db
    cls = owner_class()
    monkeypatch.setattr(k8s_config, "load_incluster_config", external_cluster_config_noop)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        owner = cls()
        runtime = run.build(config(url), {"CURIE_SANDBOX_SUBSTRATE": "kubernetes"}, resources=owner)
        assert runtime.resources is owner
        gate = runtime.source_gate
        assert isinstance(gate, SourceGate)
        assert gate.engine.pool is not runtime.engine.pool
        assert gate.engine.pool.size() == 4
        assert gate.engine.pool._max_overflow == 0 and gate.engine.pool.timeout() == 30
        release = asyncio.Event()
        ready = [asyncio.Event() for _ in range(4)]
        pids: list[int] = []
        tasks: list[asyncio.Task[Any]] = []
        observer = create_async_engine(url, pool_size=1, max_overflow=0)

        async def holder(index: int) -> None:
            """@spec PROTECTED-HOOK-SOURCE-2."""
            async with gate.hold(uuid.uuid4()) as held:
                pids.append(int(await held._connection.scalar(text("SELECT pg_backend_pid()"))))
                ready[index].set()
                await release.wait()

        waiting_started, admitted = asyncio.Event(), asyncio.Event()

        async def fifth() -> None:
            """@spec PROTECTED-HOOK-SOURCE-2."""
            waiting_started.set()
            async with gate.hold(uuid.uuid4()):
                admitted.set()

        try:
            tasks = [asyncio.create_task(holder(index)) for index in range(4)]
            async with asyncio.timeout(4):
                for signal in ready:
                    await signal.wait()
            assert gate.engine.pool.checkedout() == 4
            waiter = asyncio.create_task(fifth())
            tasks.append(waiter)
            await waiting_started.wait()
            assert not waiter.done() and not admitted.is_set()
            async with asyncio.timeout(2), runtime.engine.connect() as connection:
                assert await connection.scalar(text("SELECT 1")) == 1
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            release.set()
            await asyncio.gather(*tasks[:4])
            async with gate.hold(uuid.uuid4()):
                pass
            await owner.aclose()
            for pid in pids:
                await backend_gone(observer, pid)
        finally:
            release.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await owner.aclose()
            await observer.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 20))


def test_actual_early_constructor_error_has_immediately_owned_gate_disposal(worker_db: Any) -> None:
    """Lazy engine disposal uses actual event, @spec PROTECTED-HOOK-SOURCE-2."""
    _, _, url = worker_db
    cls = owner_class()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        owner = cls()
        disposed: list[Engine] = []

        def observe(engine: Engine) -> None:
            """@spec PROTECTED-HOOK-SOURCE-2."""
            disposed.append(engine)

        event.listen(Engine, "engine_disposed", observe)
        try:
            with pytest.raises(ValueError) as original:
                run.build(
                    config(url),
                    {"CURIE_SANDBOX_SUBSTRATE": "kubernetes", "CURIE_CLAIM_TIMEOUT_SECONDS": "0"},
                    resources=owner,
                )
            with pytest.raises(ValueError) as cleanup:
                await owner.aclose(primary=original.value)
            assert cleanup.value is original.value
            gates = [engine for engine in disposed if engine.pool.size() == 4]
            assert len(gates) == 1, (
                "early dedicated gate must be registered before later constructors"
            )
            await owner.aclose()
            assert [engine for engine in disposed if engine.pool.size() == 4] == gates
        finally:
            event.remove(Engine, "engine_disposed", observe)

    asyncio.run(asyncio.wait_for(scenario(), 10))


def test_actual_runtime_cron_relation_wait_joins_before_work_and_gate_disposal(
    worker_db: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Real cron lifecycle only, @spec PROTECTED-HOOK-SOURCE-2."""
    _, _, url = worker_db
    cls = owner_class()
    monkeypatch.setattr(k8s_config, "load_incluster_config", external_cluster_config_noop)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        owner = cls()
        runtime = run.build(config(url), {"CURIE_SANDBOX_SUBSTRATE": "kubernetes"}, resources=owner)
        assert runtime.cron_loop is not None
        blocker = create_async_engine(url, pool_size=1, max_overflow=0)
        observer = create_async_engine(url, pool_size=1, max_overflow=0)
        stop = asyncio.Event()
        task = None
        disposed: list[bool] = []

        def observe(_: Engine) -> None:
            """@spec PROTECTED-HOOK-SOURCE-2."""
            disposed.append(task is not None and task.done())

        event.listen(runtime.engine.sync_engine, "engine_disposed", observe)
        event.listen(runtime.source_gate.engine.sync_engine, "engine_disposed", observe)
        try:
            async with runtime.source_gate.engine.connect() as connection:
                gate_pid = int(await connection.scalar(text("SELECT pg_backend_pid()")))
            async with blocker.connect() as connection, connection.begin():
                await connection.execute(
                    text("LOCK TABLE curie.deployments IN ACCESS EXCLUSIVE MODE")
                )
                task = asyncio.create_task(runtime.cron_loop.run_forever(stop))
                owner.register_task("actual-cron", task)
                owner.register_stop("actual-cron-stop", stop.set)
                async with asyncio.timeout(4):
                    while True:
                        assert not task.done()
                        async with observer.connect() as observed:
                            pid = await observed.scalar(
                                text(
                                    "SELECT l.pid FROM pg_locks l "
                                    "JOIN pg_stat_activity a ON a.pid=l.pid "
                                    "WHERE a.datname=current_database() AND NOT l.granted "
                                    "AND l.relation='curie.deployments'::regclass "
                                    "AND a.wait_event='relation'"
                                )
                            )
                        if pid is not None:
                            break
                        await asyncio.sleep(0.01)
                async with asyncio.timeout(12):
                    await owner.aclose()
                assert task.done()
                assert disposed == [True, True]
                await backend_gone(observer, int(pid))
                await backend_gone(observer, gate_pid)
        finally:
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await owner.aclose()
            await blocker.dispose()
            await observer.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 25))
