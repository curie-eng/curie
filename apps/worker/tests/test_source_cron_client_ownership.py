"""Genuine Runtime cron transport ownership, @spec PROTECTED-HOOK-SOURCE-2."""

from __future__ import annotations

import asyncio
import importlib.util
import uuid
from pathlib import Path
from typing import Any

import pytest
from botocore.client import BaseClient
from curie_worker import run
from curie_worker.sandbox.k8s import k8s_config
from curie_worker.worker_lifecycle import WorkerResources
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


def public_helper(name: str) -> Any:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + ".py"))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


reads = public_helper("test_source_cron_read_ownership")
composition = public_helper("test_source_worker_composition")
worker_db = reads.worker_db
worker_templates = reads.worker_templates
stored_bundle = reads.stored_bundle


def test_actual_runtime_joins_genuine_cron_read_before_client_close_once(
    worker_db: Any,
    stored_bundle: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TEST driver, no cluster requests or producer qualification, @spec PROTECTED-HOOK-SOURCE-2."""
    _, _, url = worker_db
    original, key, _ = stored_bundle
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text(
        "apiVersion: v1\nkind: Config\ncurrent-context: owned-test\n"
        "clusters:\n- name: owned-test\n  cluster:\n    server: https://example.invalid\n"
        "contexts:\n- name: owned-test\n  context:\n    cluster: owned-test\n    user: owned-test\n"
        "users:\n- name: owned-test\n  user: {}\n"
    )
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    monkeypatch.setattr(k8s_config.kube_config, "KUBE_CONFIG_DEFAULT_LOCATION", str(kubeconfig))
    relay = reads.relay_support.S3BodyRelay(
        original.s3_endpoint_url, "/" + original.bundle_bucket + "/" + key, hold=True
    )
    endpoint = relay.start()
    cron_client: Any = None
    driver: asyncio.Task[None] | None = None
    closes: list[bool] = []
    original_close = BaseClient.close

    def observe_close(client: Any) -> None:
        """Observation invokes the genuine provider close, @spec PROTECTED-HOOK-SOURCE-2."""
        if client is cron_client:
            closes.append(driver is not None and driver.done())
        original_close(client)

    monkeypatch.setattr(BaseClient, "close", observe_close)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        nonlocal cron_client, driver
        config = composition.config(url).model_copy(update={"s3_endpoint_url": endpoint})
        owner = WorkerResources()
        runtime = None
        cleanup: asyncio.Task[None] | None = None
        observer = create_async_engine(url, pool_size=1, max_overflow=0)
        pid = None

        async def read() -> None:
            """TEST driver holds an actual source gate, @spec PROTECTED-HOOK-SOURCE-2."""
            nonlocal pid
            assert runtime is not None
            async with runtime.source_gate.hold(uuid.uuid4()) as held:
                pid = int(await held._connection.scalar(text("SELECT pg_backend_pid()")))
                await runtime.cron_loop._cron_triggers(reads.target(key))

        try:
            runtime = run.build(config, {"CURIE_SANDBOX_SUBSTRATE": "kubernetes"}, resources=owner)
            assert runtime.resources is owner
            cron_client = runtime.cron_loop._source._reader._client
            manager = cron_client._endpoint.http_session._manager
            driver = asyncio.create_task(read())
            owner.register_task("TEST-cron-read", driver)
            assert await asyncio.to_thread(relay.body_held.wait, 4)
            assert pid is not None and not relay.errors and len(manager.pools) > 0
            driver.cancel()
            cleanup = asyncio.create_task(owner.aclose())
            done, _ = await asyncio.wait({cleanup, driver}, timeout=0.2)
            assert not done and not closes
            async with observer.connect() as connection:
                assert await connection.scalar(
                    text(
                        "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=:pid "
                        "AND locktype='advisory' AND granted)"
                    ),
                    {"pid": pid},
                )
            relay.release.set()
            async with asyncio.timeout(5):
                await cleanup
            assert driver.cancelled() and relay.requests == 1 and not relay.errors
            assert closes == [True], (
                "Runtime must own and close its genuine cron S3 client after join"
            )
            assert len(manager.pools) == 0, "the genuine provider close must clear its actual pool"
            await owner.aclose()
            assert closes == [True], "repeated cleanup must not close the cron client twice"
            async with observer.connect() as connection:
                assert not await connection.scalar(
                    text("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=:pid)"),
                    {"pid": pid},
                )
        finally:
            relay.release.set()
            if driver is not None:
                driver.cancel()
                await asyncio.gather(driver, return_exceptions=True)
            if cleanup is not None:
                await asyncio.gather(cleanup, return_exceptions=True)
            await owner.aclose()
            if cron_client is not None:
                original_close(cron_client)
            await observer.dispose()

    try:
        asyncio.run(asyncio.wait_for(scenario(), 15))
    finally:
        relay.close()
