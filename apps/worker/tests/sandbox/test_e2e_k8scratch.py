"""End-to-end proof of the sandbox substrate against a real cluster.

Gated: runs only with ``CURIE_SANDBOX_E2E=1`` plus a reachable cluster
(``KUBECONFIG``) that has the agent-sandbox controller+extensions, the chart's
sandbox resources installed (``agentSandbox.deploy=true``), and the runner
image imported. Asserts the full G1 lifecycle against real machinery:

1. warm-pool claim binds a ready sandbox in under a second,
2. the claimed sandbox answers ``/healthz`` and round-trips an ACI event
   (fake-model NDJSON stream ending in a ``final``),
3. consecutive turns land on the SAME pod/process (the cache-affinity
   invariant the substrate owns: no rebind between turns),
4. suspend deletes the pod; resume creates a NEW claim whose pod carries
   ``CURIE_HISTORY_REF`` (the cold-restart rehydrate path, asserted from
   the injected env + a fresh healthy runner),
5. release deletes claim, sandbox, and pod (the reap path).

Out-of-cluster reachability uses ``kubectl port-forward`` to the sandbox pod
(the serviceFQDN is cluster-internal DNS).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import socket
import subprocess
import time
import urllib.request
from collections.abc import Iterator

import pytest
import redis
from curie_worker.sandbox import (
    HISTORY_ENV,
    AffinityStore,
    KubernetesSandboxClient,
    SandboxHandle,
    SandboxSubstrate,
    SubstrateConfig,
)
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.retry import Retry as AsyncRetry
from redis.backoff import NoBackoff
from redis.maint_notifications import MaintNotificationsConfig

pytestmark = pytest.mark.skipif(
    os.environ.get("CURIE_SANDBOX_E2E") != "1",
    reason="cluster e2e; set CURIE_SANDBOX_E2E=1 with KUBECONFIG + chart installed",
)

NAMESPACE = os.environ.get("CURIE_SANDBOX_E2E_NAMESPACE", "curie-g1")
# Matches what the chart renders for release curie-g1 (fullname collapses the
# repeated chart name): <fullname>-runner-pool.
POOL = os.environ.get("CURIE_SANDBOX_E2E_POOL", "curie-g1-runner-pool")


def _kubectl(*args: str) -> str:
    result = subprocess.run(
        ["kubectl", "-n", NAMESPACE, *args],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return result.stdout


def _pod_of_sandbox(sandbox_name: str) -> dict[str, object]:
    raw = _kubectl("get", "pod", sandbox_name, "-o", "json")
    return dict(json.loads(raw))


@contextlib.contextmanager
def _port_forward(pod: str, remote_port: int) -> Iterator[str]:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        local_port = probe.getsockname()[1]
    proc = subprocess.Popen(
        ["kubectl", "-n", NAMESPACE, "port-forward", f"pod/{pod}", f"{local_port}:{remote_port}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline()
        if "Forwarding from" not in line:
            raise RuntimeError(f"port-forward failed: {line}")
        yield f"http://127.0.0.1:{local_port}"
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def _get_json(base: str, path: str) -> dict[str, object]:
    with urllib.request.urlopen(f"{base}{path}", timeout=10) as resp:
        return dict(json.loads(resp.read()))


def _post_event(base: str, text: str) -> list[dict[str, object]]:
    body = json.dumps(
        {"kind": "event", "type": "message", "text": text, "user": "U-e2e", "ts": "1.0"}
    ).encode()
    request = urllib.request.Request(
        f"{base}/v1/event", data=body, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=60) as resp:
        return [json.loads(line) for line in resp.read().splitlines() if line.strip()]


@pytest.fixture(scope="module")
def substrate() -> Iterator[SandboxSubstrate]:
    client = redis.Redis(
        host=os.environ.get("TEST_VALKEY_HOST", "localhost"),
        port=int(os.environ.get("TEST_VALKEY_PORT", "26379")),
        password=os.environ.get("TEST_VALKEY_PW", "valkeypass") or None,
    )
    pressure_client = AsyncRedis(
        host=os.environ.get("TEST_VALKEY_HOST", "localhost"),
        port=int(os.environ.get("TEST_VALKEY_PORT", "26379")),
        password=os.environ.get("TEST_VALKEY_PW", "valkeypass") or None,
        socket_timeout=1.0,
        socket_connect_timeout=1.0,
        retry=AsyncRetry(NoBackoff(), 0),
        driver_info=None,
        maint_notifications_config=MaintNotificationsConfig(enabled=False),
    )
    client.ping()
    prefix = "e2e:curie:sandbox"
    config = SubstrateConfig(
        namespace=NAMESPACE,
        warm_pool=POOL,
        claim_timeout_seconds=120.0,
        poll_interval_seconds=0.02,
        key_prefix=prefix,
    )
    yield SandboxSubstrate(
        KubernetesSandboxClient(NAMESPACE),
        AffinityStore(client, pressure_client=pressure_client, key_prefix=prefix),
        config,
    )
    keys = list(client.scan_iter(match=f"{prefix}:*"))
    if keys:
        client.delete(*keys)
    client.close()
    asyncio.run(pressure_client.aclose())


def _await_pool_ready(timeout: float = 180.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        raw = _kubectl("get", "sandboxwarmpool", POOL, "-o", "json")
        status = json.loads(raw).get("status") or {}
        if status.get("readyReplicas", 0) >= 1:
            return
        time.sleep(2)
    raise AssertionError(f"warm pool {POOL} never became ready")


def test_full_lifecycle_on_cluster(substrate: SandboxSubstrate) -> None:
    _await_pool_ready()
    thread = f"e2e-{int(time.time())}"

    # 1. Warm claim binds sub-second.
    started = time.monotonic()
    handle: SandboxHandle = substrate.claim(thread)
    claim_latency = time.monotonic() - started
    print(f"\nEVIDENCE claim_latency_seconds={claim_latency:.3f}")
    assert claim_latency < 1.0, f"warm claim took {claim_latency:.3f}s (>= 1s)"
    assert handle.service_fqdn.endswith(f"{NAMESPACE}.svc.cluster.local")

    pod_before = _pod_of_sandbox(handle.sandbox_name)
    uid_before = pod_before["metadata"]["uid"]  # type: ignore[index]

    with _port_forward(handle.sandbox_name, handle.port) as base:
        # 2. Health + ACI event round-trip through the real runner image.
        assert _get_json(base, "/healthz") == {"ok": True}
        frames = _post_event(base, "hello from the G1 e2e")
        types = [f.get("type") for f in frames]
        print(f"EVIDENCE first_turn_frames={types}")
        assert types[-1] == "final"

        # 3. Consecutive turn, same claim -> same pod, same process.
        frames2 = _post_event(base, "second turn, same session")
        assert [f.get("type") for f in frames2][-1] == "final"
        again = substrate.claim(thread)
        assert again.sandbox_name == handle.sandbox_name
    pod_after_turns = _pod_of_sandbox(handle.sandbox_name)
    assert pod_after_turns["metadata"]["uid"] == uid_before  # type: ignore[index]
    print(f"EVIDENCE same_pod_across_turns uid={uid_before}")

    # 4. Suspend deletes the pod; resume rehydrates via a fresh claim with
    #    CURIE_HISTORY_REF injected.
    substrate.suspend(thread, history_ref="e2e-history-ref-123")
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            _pod_of_sandbox(handle.sandbox_name)
        except subprocess.CalledProcessError:
            break
        time.sleep(1)
    else:
        raise AssertionError("suspended sandbox pod was never deleted")
    print("EVIDENCE suspend_deleted_pod=true")

    resumed = substrate.resume(thread)
    assert resumed.claim_name != handle.claim_name
    assert resumed.history_ref == "e2e-history-ref-123"
    resumed_pod = _pod_of_sandbox(resumed.sandbox_name)
    env = {
        e.get("name"): e.get("value")
        for c in resumed_pod["spec"]["containers"]  # type: ignore[index,union-attr]
        for e in c.get("env", [])
    }
    assert env[HISTORY_ENV] == "e2e-history-ref-123"
    with _port_forward(resumed.sandbox_name, resumed.port) as base:
        assert _get_json(base, "/healthz") == {"ok": True}
    print(f"EVIDENCE resume_injected_history_ref pod={resumed.sandbox_name}")

    # 5. Release reaps claim, sandbox, pod.
    assert substrate.release(thread)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            _pod_of_sandbox(resumed.sandbox_name)
        except subprocess.CalledProcessError:
            break
        time.sleep(1)
    else:
        raise AssertionError("released sandbox pod was never deleted")
    assert substrate.lookup(thread) is None
    print("EVIDENCE release_reaped_pod=true")
