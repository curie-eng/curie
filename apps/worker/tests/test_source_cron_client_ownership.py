"""Genuine Runtime cron transport ownership, @spec PROTECTED-HOOK-SOURCE-2."""

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


async def contained_scenario(settings: dict[str, str]) -> None:
    """Fatal-capable TEST actor runs only in its owned child, @spec PROTECTED-HOOK-SOURCE-2."""
    os.environ.pop("KUBERNETES_SERVICE_HOST", None)
    k8s_config.kube_config.KUBE_CONFIG_DEFAULT_LOCATION = settings["kubeconfig"]
    config = composition.config(settings["url"]).model_copy(
        update={"s3_endpoint_url": settings["endpoint"]}
    )
    owner = WorkerResources()
    runtime = None
    cleanup: asyncio.Task[None] | None = None
    driver: asyncio.Task[None] | None = None
    cron_client: Any = None
    closes: list[bool] = []
    observer = create_async_engine(settings["url"], pool_size=1, max_overflow=0)
    pid = None
    original_close = BaseClient.close

    def observe_close(client: Any) -> None:
        """Observation invokes the genuine provider close, @spec PROTECTED-HOOK-SOURCE-2."""
        if client is cron_client:
            closes.append(driver is not None and driver.done())
        original_close(client)

    BaseClient.close = observe_close

    async def read() -> None:
        """TEST driver holds an actual source gate, @spec PROTECTED-HOOK-SOURCE-2."""
        nonlocal pid
        assert runtime is not None
        async with runtime.source_gate.hold(uuid.uuid4()) as held:
            pid = int(await held._connection.scalar(text("SELECT pg_backend_pid()")))
            await runtime.cron_loop._cron_triggers(reads.target(settings["key"]))

    try:
        runtime = run.build(config, {"CURIE_SANDBOX_SUBSTRATE": "kubernetes"}, resources=owner)
        assert runtime.resources is owner
        cron_client = runtime.cron_loop._source._reader._client
        manager = cron_client._endpoint.http_session._manager
        driver = asyncio.create_task(read())
        owner.register_task("TEST-cron-read", driver)
        Path(settings["ready"]).touch()
        async with asyncio.timeout(4):
            while not Path(settings["held"]).exists():
                await asyncio.sleep(0.01)
        assert pid is not None and len(manager.pools) > 0
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
        Path(settings["release"]).touch()
        async with asyncio.timeout(5):
            await cleanup
        assert driver.cancelled()
        assert closes == [True], "Runtime must close its genuine cron S3 client after join"
        assert len(manager.pools) == 0, "genuine provider close must clear its actual pool"
        await owner.aclose()
        assert closes == [True], "repeated cleanup must not close the cron client twice"
        async with observer.connect() as connection:
            assert not await connection.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=:pid)"),
                {"pid": pid},
            )
    finally:
        if driver is not None:
            driver.cancel()
            await asyncio.gather(driver, return_exceptions=True)
        if cleanup is not None:
            await asyncio.gather(cleanup, return_exceptions=True)
        await owner.aclose()
        if cron_client is not None:
            original_close(cron_client)
        BaseClient.close = original_close
        await observer.dispose()


def test_actual_runtime_joins_genuine_cron_read_before_client_close_once(
    worker_db: Any, stored_bundle: Any, tmp_path: Path
) -> None:
    """Owned child contains every possible fatal deadline, @spec PROTECTED-HOOK-SOURCE-2."""
    _, _, url = worker_db
    original, key, _ = stored_bundle
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text(
        "apiVersion: v1\nkind: Config\ncurrent-context: owned-test\n"
        "clusters:\n- name: owned-test\n  cluster:\n    server: https://example.invalid\n"
        "contexts:\n- name: owned-test\n  context:\n    cluster: owned-test\n    user: owned-test\n"
        "users:\n- name: owned-test\n  user: {}\n"
    )
    relay = reads.relay_support.S3BodyRelay(
        original.s3_endpoint_url, "/" + original.bundle_bucket + "/" + key, hold=True
    )
    endpoint = relay.start()
    ready, held, release = [tmp_path / name for name in ("ready", "held", "release")]
    settings = dict(
        url=url,
        endpoint=endpoint,
        key=key,
        kubeconfig=str(kubeconfig),
        ready=str(ready),
        held=str(held),
        release=str(release),
    )
    program = """
import asyncio,importlib.util,json,os
spec=importlib.util.spec_from_file_location("owned_cron_client",os.environ["SOURCE_CRON_FILE"])
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
asyncio.run(asyncio.wait_for(module.contained_scenario(json.loads(os.environ["SOURCE_CRON_SETTINGS"])),15))
"""
    process = None
    with (
        (tmp_path / "child.stdout").open("w") as stdout,
        (tmp_path / "child.stderr").open("w") as stderr,
    ):
        os.chmod(stdout.name, 0o600)
        os.chmod(stderr.name, 0o600)
        try:
            process = subprocess.Popen(
                [sys.executable, "-c", program],
                env=dict(
                    os.environ,
                    SOURCE_CRON_FILE=str(Path(__file__).resolve()),
                    SOURCE_CRON_SETTINGS=json.dumps(settings),
                ),
                stdout=stdout,
                stderr=stderr,
            )
            started = time.monotonic()
            while not ready.exists() and process.poll() is None and time.monotonic() - started < 10:
                time.sleep(0.01)
            assert ready.exists(), "owned actor must construct genuine Runtime"
            assert relay.body_held.wait(4), "genuine S3 body must be held before cancellation"
            assert not relay.errors
            held.touch()
            started = time.monotonic()
            while (
                not release.exists() and process.poll() is None and time.monotonic() - started < 4
            ):
                time.sleep(0.01)
            assert release.exists(), (
                "actual gate and pending cleanup assertions must precede release"
            )
            relay.release.set()
            process.wait(timeout=10)
            assert process.returncode == 0, "owned cron-client actor must complete all invariants"
            assert relay.requests == 1 and not relay.errors
        finally:
            relay.release.set()
            if process is not None and process.poll() is None:
                process.kill()
                process.wait(timeout=3)
            relay.close()
