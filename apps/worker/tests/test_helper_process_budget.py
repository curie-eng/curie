"""Concurrent status helpers must fit beside the worker that owns them."""

from __future__ import annotations

import asyncio
import json
import os
import selectors
import subprocess
import sys
import time
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
import runpy
import sys

sys.argv = ["curie_worker.upgrade_drain", "--mode", "status", "--json", "--with-ttl"]
runpy.run_module("curie_worker.upgrade_drain", run_name="__main__", alter_sys=True)
"""
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


def _read_status(process: subprocess.Popen[str]) -> dict[str, object]:
    stdout, _ = process.communicate(timeout=10)
    assert process.returncode == 0, f"status helper exited {process.returncode}"
    assert len(stdout.splitlines()) == 1
    status: dict[str, object] = json.loads(stdout)
    return status


def _live_helper_rss(children: list[subprocess.Popen[str]]) -> int:
    live = [child for child in children if child.poll() is None]
    if not live:
        return 0
    if sys.platform == "darwin":
        # Darwin ps reports live RSS in 1024-byte units.
        # https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man1/ps.1.html
        sample = subprocess.run(
            ["ps", "-o", "rss=", "-p", ",".join(str(child.pid) for child in live)],
            capture_output=True, text=True, timeout=10, check=False,
        )
        if sample.returncode:
            if any(child.poll() is None for child in live):
                raise RuntimeError("live helper RSS observation failed")
            return 0
        return sum(int(value) for value in sample.stdout.split()) * 1024
    if sys.platform != "linux":
        raise RuntimeError("live helper RSS measurement requires Linux or macOS")
    page_size = os.sysconf("SC_PAGE_SIZE")
    if page_size <= 0:
        raise RuntimeError("invalid resident-page size")
    total = 0
    for child in live:
        # statm reports resident pages, including zero after the address space exits.
        # https://man7.org/linux/man-pages/man5/proc_pid_statm.5.html
        try:
            columns = Path(f"/proc/{child.pid}/statm").read_text(encoding="ascii").split()
        except (FileNotFoundError, ProcessLookupError):
            continue
        if len(columns) < 2 or any(not value.isdecimal() for value in columns[:2]):
            raise RuntimeError("invalid helper resident-page observation")
        total += int(columns[1]) * page_size
    return total


@pytest.mark.parametrize("helper_count", [6, 16])
@pytest.mark.parametrize("quiescing", [False, True], ids=["claims-enabled", "quiescing"])
def test_status_helpers_fit_the_live_worker_memory_reserve(
    status_config: WorkerConfig, helper_count: int, quiescing: bool,
) -> None:
    expected: dict[str, object] = {"state": "claims_enabled", "since": None, "revision": None}
    if quiescing:
        expected = asyncio.run(_quiesce(status_config))
        assert expected["state"] == "quiescing"
        assert expected["revision"] == status_config.upgrade_revision

    program = (
        "import sys\n"
        "sys.stderr.write('CURIE_HELPER_READY\\n')\n"
        "sys.stderr.flush()\n"
        "if sys.stdin.read(1) != '1':\n"
        "    raise RuntimeError('status helper start barrier was not released')\n"
        + _STATUS_HELPER
    )
    children: list[subprocess.Popen[str]] = []
    pool = ThreadPoolExecutor(max_workers=helper_count)
    peak = 0
    try:
        with selectors.DefaultSelector() as ready:
            for _ in range(helper_count):
                child = subprocess.Popen(
                    [sys.executable, "-c", program],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                children.append(child)
                assert child.stderr is not None
                ready.register(child.stderr, selectors.EVENT_READ, child)
                peak = max(peak, _live_helper_rss(children))
            ready_deadline = time.monotonic() + 10
            while ready.get_map():
                peak = max(peak, _live_helper_rss(children))
                if time.monotonic() >= ready_deadline:
                    raise TimeoutError("status interpreters did not reach the start barrier")
                for key, _ in ready.select(timeout=0.005):
                    stream = key.data.stderr
                    assert stream is not None
                    assert stream.readline() == "CURIE_HELPER_READY\n"
                    ready.unregister(stream)

        deadline = time.monotonic() + 10
        for child in children:
            assert child.stdin is not None
            child.stdin.write("1")
            child.stdin.flush()

        def observe(child: subprocess.Popen[str]) -> dict[str, object]:
            result = _read_status(child)
            assert time.monotonic() <= deadline, (
                "status command exceeded its 10s observation budget"
            )
            return result

        futures = [pool.submit(observe, child) for child in children]
        while any(not future.done() for future in futures):
            peak = max(peak, _live_helper_rss(children))
            time.sleep(0.005)
        results = [future.result() for future in futures]
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
        pool.shutdown(wait=True, cancel_futures=True)
        for child in children:
            child.communicate(timeout=10)

    for status in results:
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
    assert peak > 0, "live helper RSS was not observed"
    assert budget > 0
    assert peak <= budget, f"live helper RSS {peak} exceeds worker reserve {budget} bytes"
