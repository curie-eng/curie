"""Concurrent status helpers must fit beside the worker that owns them."""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import redis
import redis.asyncio
import yaml
from curie_worker.config import WorkerConfig
from curie_worker.upgrade_drain import UpgradeDrainGate
from kubernetes.utils.quantity import parse_quantity

_STATUS_HELPER = """
import resource
import runpy
import sys

sys.argv = ["curie_worker.upgrade_drain", "--mode", "status", "--json", "--with-ttl"]
try:
    runpy.run_module("curie_worker.upgrade_drain", run_name="__main__", alter_sys=True)
finally:
    if sys.platform == "linux":
        # VmHWM measures this exec's address space, without inherited ru_maxrss.
        # https://man7.org/linux/man-pages/man5/proc_pid_status.5.html
        with open("/proc/self/status") as status_file:
            for line in status_file:
                if line.startswith("VmHWM:"):
                    _, amount, unit = line.split()
                    if unit != "kB":
                        raise RuntimeError("unexpected process peak RSS unit")
                    peak = int(amount) * 1024
                    break
            else:
                raise RuntimeError("missing process peak RSS")
    elif sys.platform == "darwin":
        # Darwin getrusage(2) reports ru_maxrss in bytes.
        # https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/getrusage.2.html
        peak = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    else:
        raise RuntimeError("process peak RSS measurement requires Linux or macOS")
    sys.stderr.write(f"CURIE_HELPER_PEAK_RSS_BYTES={int(peak)}\\n")
"""
_RSS_PREFIX = "CURIE_HELPER_PEAK_RSS_BYTES="
_QUIESCE_TTL_S = 120


@pytest.fixture
def status_config(
    sync_redis: redis.Redis,
    names: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> WorkerConfig:
    connection = sync_redis.connection_pool.connection_kwargs
    settings = {
        "VALKEY_HOST": str(connection["host"]),
        "VALKEY_PORT": str(connection["port"]),
        "VALKEY_PASSWORD": str(connection.get("password") or ""),
        "VALKEY_DB": str(connection.get("db", 0)),
        "VALKEY_TLS": "false",
        "KEY_PREFIX": names["prefix"],
        "CURIE_STREAM": names["stream"],
        "CURIE_CONSUMER_GROUP": names["group"],
        "CURIE_EVAL_STREAM": f"{names['stream']}:evals",
        "CURIE_EVAL_CONSUMER_GROUP": f"{names['group']}-evals",
        "CURIE_INSTALLATION_ID": f"install-{names['group']}",
        "CURIE_UPGRADE_REVISION": "7",
        "CURIE_UPGRADE_LEGACY_QUIESCE": "false",
    }
    for name, value in settings.items():
        monkeypatch.setenv(name, value)
    config = WorkerConfig()
    request.addfinalizer(lambda: sync_redis.delete(config.upgrade_quiesce_key()))
    return config


async def _quiesce(config: WorkerConfig) -> dict[str, object]:
    client = redis.asyncio.Redis(**config.valkey_client_kwargs())
    try:
        gate = UpgradeDrainGate(client, config)
        await gate.request_quiesce(ttl_s=_QUIESCE_TTL_S)
        status = dict(await gate.claim_status())
        status.pop("ttl_seconds")
        return status
    finally:
        await client.aclose()


def _read_status(process: subprocess.Popen[str]) -> tuple[dict[str, object], int]:
    stdout, stderr = process.communicate(timeout=10)
    assert process.returncode == 0, f"status helper exited {process.returncode}"
    assert len(stdout.splitlines()) == 1
    status: dict[str, object] = json.loads(stdout)
    metrics = [
        line.removeprefix(_RSS_PREFIX)
        for line in stderr.splitlines()
        if line.startswith(_RSS_PREFIX)
    ]
    assert len(metrics) == 1, "status helper must report one peak RSS measurement"
    peak = int(metrics[0])
    assert peak > 0
    return status, peak


@pytest.mark.parametrize("quiescing", [False, True], ids=["claims-enabled", "quiescing"])
def test_six_status_helpers_fit_the_worker_memory_reserve(
    status_config: WorkerConfig, quiescing: bool,
) -> None:
    expected: dict[str, object] = {"state": "claims_enabled", "since": None, "revision": None}
    if quiescing:
        expected = asyncio.run(_quiesce(status_config))
        assert expected["state"] == "quiescing"
        assert expected["revision"] == status_config.upgrade_revision

    children: list[subprocess.Popen[str]] = []
    try:
        for _ in range(6):
            children.append(
                subprocess.Popen(
                    [sys.executable, "-c", _STATUS_HELPER],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
            )
        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(_read_status, children))
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
        for child in children:
            child.communicate(timeout=10)

    for status, _ in results:
        if quiescing:
            ttl = status.pop("ttl_seconds")
            assert isinstance(ttl, int) and not isinstance(ttl, bool)
            assert 0 < ttl <= _QUIESCE_TTL_S
        assert status == expected

    chart = Path(__file__).resolve().parents[3] / "charts/curie/values.yaml"
    resources = yaml.safe_load(chart.read_text())["worker"]["resources"]
    # The request reserves memory for the main worker; helpers share the remainder.
    budget = int(
        parse_quantity(resources["limits"]["memory"])
        - parse_quantity(resources["requests"]["memory"])
    )
    assert budget > 0
    peaks = [peak for _, peak in results]
    assert sum(peaks) <= budget, f"helper peaks {peaks} exceed worker reserve {budget} bytes"
