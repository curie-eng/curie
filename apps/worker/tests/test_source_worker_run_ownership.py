"""Actual entrypoint ownership, @spec PROTECTED-HOOK-SOURCE-2."""

from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path
from typing import Any

import pytest
from curie_test_support.valkey import connect_or_skip
from curie_worker import run
from curie_worker.sandbox.k8s import k8s_client, k8s_config
from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import create_async_engine


def composition_support() -> Any:
    """Public test fixture reuse, @spec PROTECTED-HOOK-SOURCE-2/10."""
    path = Path(__file__).with_name("test_source_worker_composition.py")
    spec = importlib.util.spec_from_file_location("_source_worker_run_setup", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


composition = composition_support()
worker_db = composition.worker_db
worker_templates = composition.worker_templates


def test_actual_run_constructor_failure_disposes_immediately_owned_gate(worker_db: Any) -> None:
    """Lazy disposal uses event, not PID absence, @spec PROTECTED-HOOK-SOURCE-2."""
    _, _, url = worker_db

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        disposed: list[Engine] = []

        def observe(engine: Engine) -> None:
            """@spec PROTECTED-HOOK-SOURCE-2."""
            disposed.append(engine)

        event.listen(Engine, "engine_disposed", observe)
        try:
            with pytest.raises(ValueError):
                await run._run(
                    composition.config(url),
                    {"CURIE_SANDBOX_SUBSTRATE": "kubernetes", "CURIE_CLAIM_TIMEOUT_SECONDS": "0"},
                )
            assert len([engine for engine in disposed if engine.pool.size() == 4]) == 1
        finally:
            event.remove(Engine, "engine_disposed", observe)

    asyncio.run(asyncio.wait_for(scenario(), 12))


def external_empty_kubernetes_inventory(*args: Any, **kwargs: Any) -> dict[str, list[Any]]:
    """External API TEST reply only, @spec PROTECTED-HOOK-SOURCE-2."""
    return {"items": []}


def test_actual_run_boots_idle_then_joins_real_cron_wait_and_disposes_gate(
    worker_db: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """External config only faked; no cluster requests, @spec PROTECTED-HOOK-SOURCE-2."""
    _, _, url = worker_db
    monkeypatch.setattr(
        k8s_config, "load_incluster_config", composition.external_cluster_config_noop
    )
    monkeypatch.setattr(
        k8s_client.CustomObjectsApi,
        "list_namespaced_custom_object",
        external_empty_kubernetes_inventory,
    )
    config = composition.config(url).model_copy(
        update={
            "heartbeat_file": str(tmp_path / "owned-heartbeat"),
            "heartbeat_interval_s": 0.05,
            "read_block_ms": 100,
            "cron_tick_interval_s": 0.05,
        }
    )
    client = connect_or_skip()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        blocker = create_async_engine(url, pool_size=1, max_overflow=0)
        observer = create_async_engine(url, pool_size=1, max_overflow=0)
        disposed: list[Engine] = []
        task = None

        def observe(engine: Engine) -> None:
            """@spec PROTECTED-HOOK-SOURCE-2."""
            disposed.append(engine)

        event.listen(Engine, "engine_disposed", observe)
        try:
            async with blocker.connect() as locked, locked.begin():
                await locked.execute(text("LOCK TABLE curie.deployments IN ACCESS EXCLUSIVE MODE"))
                task = asyncio.create_task(
                    run._run(config, {"CURIE_SANDBOX_SUBSTRATE": "kubernetes"})
                )
                async with asyncio.timeout(10):
                    while True:
                        assert not task.done(), "actual worker failed before idle boot evidence"
                        if Path(config.heartbeat_file).exists() and client.exists(config.stream):
                            groups = client.xinfo_groups(config.stream)
                            if any(group["name"] == config.consumer_group for group in groups):
                                break
                        await asyncio.sleep(0.01)
                async with asyncio.timeout(5):
                    while True:
                        assert not task.done()
                        async with observer.connect() as connection:
                            pid = await connection.scalar(
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
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    async with asyncio.timeout(15):
                        await task
                await composition.backend_gone(observer, int(pid))
                gates = [engine for engine in disposed if engine.pool.size() == 4]
                assert len(gates) == 1, "actual entrypoint must dispose its registered gate"
                assert disposed[-1] is gates[0], "gate closes after the real work pool"
        finally:
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            event.remove(Engine, "engine_disposed", observe)
            await blocker.dispose()
            await observer.dispose()

    try:
        asyncio.run(asyncio.wait_for(scenario(), 35))
    finally:
        client.delete(config.stream, config.eval_stream)
        client.close()
