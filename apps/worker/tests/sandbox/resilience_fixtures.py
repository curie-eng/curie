"""Fixtures and import wiring for the sandbox-substrate resilience E2E.

The cluster-touching fixtures (``substrate``, ``pool_ready``) are only requested
by the gated scenario in ``test_e2e_resilience.py``. The sandbox collector
excludes that scenario unless ``CURIE_SANDBOX_E2E=1``, so these fixtures never
run during the default offline suite. The pure-helper unit tests request none of
these fixtures and always run.
"""

from __future__ import annotations

import json
import sys
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

# importlib import mode does not add the test directory to sys.path, so make the
# sibling resilience helpers importable from every test module in this folder.
sys.path.insert(0, str(Path(__file__).parent))

from resilience_harness import ResilienceConfig, kubectl  # noqa: E402


@pytest.fixture(scope="session", name="cfg")
def resilience_cfg() -> ResilienceConfig:
    return ResilienceConfig.from_env()


@pytest.fixture(scope="session", name="substrate")
def resilience_substrate(cfg: ResilienceConfig) -> Iterator[object]:
    """A real substrate over the standing cluster and dev-stack Valkey.

    Mirrors the sandbox e2e template's ``substrate`` fixture: real ``redis``
    client, a scenario-scoped key prefix, and teardown that scans and deletes the
    prefix so a run leaves no route keys behind.
    """

    import redis
    from curie_worker.sandbox import (
        AffinityStore,
        KubernetesSandboxClient,
        SandboxSubstrate,
        SubstrateConfig,
    )

    client = redis.Redis(
        host=cfg.valkey_host,
        port=cfg.valkey_port,
        password=cfg.valkey_password,
    )
    client.ping()
    prefix = f"test:resilience:curie:sandbox:{uuid.uuid4().hex}"
    config = SubstrateConfig(
        namespace=cfg.namespace,
        warm_pool=cfg.pool,
        claim_timeout_seconds=120.0,
        poll_interval_seconds=0.05,
        key_prefix=prefix,
    )
    yield SandboxSubstrate(
        KubernetesSandboxClient(cfg.namespace),
        AffinityStore(client, key_prefix=prefix),
        config,
    )
    keys = list(client.scan_iter(match=f"{prefix}:*"))
    if keys:
        client.delete(*keys)
    client.close()


@pytest.fixture(name="pool_ready")
def resilience_pool_ready(cfg: ResilienceConfig) -> None:
    """Block until the warm pool has enough ready replicas for the run.

    The resilience scenario claims ``concurrency`` distinct threads plus a
    ``batch`` burst, so the pool must be able to hand out that many sandboxes
    without starving.
    """

    wanted = cfg.concurrency + cfg.batch
    deadline = time.monotonic() + 300.0
    while time.monotonic() < deadline:
        raw = kubectl(cfg, "get", "sandboxwarmpool", cfg.pool, "-o", "json")
        status = json.loads(raw).get("status") or {}
        if status.get("readyReplicas", 0) >= wanted:
            return
        time.sleep(2)
    raise AssertionError(
        f"warm pool {cfg.pool} never reached readyReplicas>={wanted}"
    )
