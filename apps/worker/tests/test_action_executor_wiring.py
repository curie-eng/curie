"""How ``run.py`` composes the executor loop (plan task 11).

@spec ACTION-EXECUTOR-1: closed by default; with the setting off the worker
claims nothing, so no loop is built. The executor routes take only the internal
worker token, so a worker without one builds none either. When built, the loop
runs as its own supervised task, ``action-executor``, beside the connector
reconcile loop, and stops with the worker's shutdown.
"""

from __future__ import annotations

import asyncio
import types
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from curie_worker import run
from curie_worker.action_executor_loop import ActionExecutorLoop
from curie_worker.config import WorkerConfig
from curie_worker.worker_lifecycle import WorkerResources

SEED = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="


def _config(**fields: Any) -> WorkerConfig:
    base: dict[str, Any] = {
        "action_executor_enabled": True,
        "internal_worker_token": "example-worker-token",
        "connector_reconcile_enabled": False,
        "connector_caller_signing_key": SEED,
    }
    return WorkerConfig(**{**base, **fields})


def _build(config: WorkerConfig) -> ActionExecutorLoop | None:
    return run._build_action_executor(
        config,
        object(),  # type: ignore[arg-type]
        substrate=types.SimpleNamespace(claim_timeout_seconds=90.0),  # type: ignore[arg-type]
        runner=object(),  # type: ignore[arg-type]
        killswitch=object(),  # type: ignore[arg-type]
        binding=object(),  # type: ignore[arg-type]
        client=object(),  # type: ignore[arg-type]
        bundles=object(),  # type: ignore[arg-type]
    )


def test_the_loop_is_not_built_with_the_executor_off() -> None:
    """@spec ACTION-EXECUTOR-1. Pins existing behaviour."""

    assert _build(_config(action_executor_enabled=False)) is None


def test_the_loop_is_not_built_without_a_worker_token() -> None:
    """Pins existing behaviour."""

    assert _build(_config(internal_worker_token="")) is None


def test_the_local_tier_loop_reads_no_deployment() -> None:
    """@spec ACTION-EXECUTOR-7: no reconciled Deployment and no proxy, so every
    grant-bound execution refuses. Pins existing behaviour.
    """

    loop = _build(_config())

    assert isinstance(loop, ActionExecutorLoop)
    assert loop._deployments is None


def test_the_lease_covers_a_sandbox_claim_and_the_dispatch_deadline() -> None:
    """@spec ACTION-EXECUTOR-17: a run that claims a sandbox and calls within its
    deadline fits inside one lease. Pins existing behaviour.
    """

    loop = _build(_config())

    assert loop is not None
    assert loop._lease_seconds >= 90 + loop._dispatch_deadline_s


# -- the supervised task ------------------------------------------------------


class _Stoppable:
    def __init__(self) -> None:
        self._stop = asyncio.Event()

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        await self._stop.wait()


class _CardStore:
    async def migrate_legacy_thread_keyed_refs(self) -> None:
        return None


class _Executor:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.stopped = False

    async def run_forever(self, shutdown: asyncio.Event) -> None:
        self.started.set()
        await shutdown.wait()
        self.stopped = True


class _Resources(WorkerResources):
    def __init__(self) -> None:
        super().__init__()
        self.stops: dict[str, Callable[[], None]] = {}
        self.task_names: list[str] = []

    def register_stop(self, name: str, callback: Callable[[], None]) -> None:
        super().register_stop(name, callback)
        self.stops[name] = callback

    def register_task(self, name: str, task: asyncio.Task[None]) -> None:
        super().register_task(name, task)
        self.task_names.append(name)


def _runtime(executor: _Executor | None) -> Any:
    return types.SimpleNamespace(
        consumer=_Stoppable(),
        killswitch=_Stoppable(),
        eval_consumer=_Stoppable(),
        deploy_notice_consumer=None,
        card_store=_CardStore(),
        orphan_sweeper=None,
        stream_retention=None,
        connector_loop=None,
        e2e_reaper=None,
        cron_loop=None,
        publication_loop=None,
        action_executor=executor,
    )


async def _run_until_started(executor: _Executor | None, tmp_path: Path) -> _Resources:
    config = _config(heartbeat_file=str(tmp_path / "heartbeat"))
    resources = _Resources()
    task = asyncio.create_task(run._run_runtime(_runtime(executor), config, resources))
    if executor is not None:
        await asyncio.wait_for(executor.started.wait(), timeout=5.0)
    else:
        await asyncio.sleep(0.2)
    resources.stops["shutdown"]()
    await asyncio.wait_for(task, timeout=10.0)
    return resources


@pytest.mark.anyio
async def test_a_built_loop_runs_as_its_own_task_and_stops_on_shutdown(tmp_path: Path) -> None:
    """Pins existing behaviour."""

    executor = _Executor()

    resources = await _run_until_started(executor, tmp_path)

    assert "action-executor" in resources.task_names
    assert executor.stopped is True


@pytest.mark.anyio
async def test_no_executor_task_without_a_loop(tmp_path: Path) -> None:
    """@spec ACTION-EXECUTOR-1. Pins existing behaviour."""

    resources = await _run_until_started(None, tmp_path)

    assert "action-executor" not in resources.task_names
    assert "connectors" not in resources.task_names
