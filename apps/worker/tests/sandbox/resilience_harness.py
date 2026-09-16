"""Reusable helpers for the sandbox-substrate resilience E2E scenario.

Two layers live here:

- **Pure helpers** (``thread_hash``, ``unique_marker``, ``final_frame``,
  ``collected_text``, ``detect_cross_talk``, ``pod_identity_gone``,
  ``pod_ready``) have no cluster or subprocess dependency and are unit-tested offline in
  ``test_resilience_harness_unit.py``.
- **Cluster helpers** (``kubectl``, ``pod_of_sandbox``, ``pod_uid``,
  ``read_pod``, ``wait_pod_identity_gone``, ``wait_pod_ready``,
  ``port_forward``, ``get_json``, ``post_event``, ``final_frame`` consumers,
  ``live_sandboxclaims``) mirror ``apps/worker/tests/sandbox/test_e2e_k8scratch.py``
  and only run when a real cluster is configured.

The substrate seam is synchronous, so the scenario drives concurrency with a
``ThreadPoolExecutor``; nothing here is async.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import socket
import subprocess
import time
import urllib.request
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class ResilienceConfig:
    """Frozen knobs for one resilience E2E invocation, built from the environment.

    The namespace, pool, and Valkey defaults match the sandbox e2e template so
    the two suites can share a standing cluster and dev stack.

    ``history_base`` (``CURIE_SANDBOX_E2E_HISTORY_BASE``) is the cluster-reachable
    state-API transcript-key prefix the resume phase injects. The runner rejects
    any ``CURIE_HISTORY_REF`` that is not an ``http(s)://`` state-API URL
    (``runner/src/curie_runner/history.py``, ``resolve_history``), so a resumed
    pod cannot boot without one; the resume phase says so rather than guessing.
    """

    namespace: str
    pool: str
    valkey_host: str
    valkey_port: int
    valkey_password: str | None
    concurrency: int
    batch: int
    runs: int
    live_creds: bool
    history_base: str

    @classmethod
    def from_env(cls) -> ResilienceConfig:
        password = os.environ.get("TEST_VALKEY_PW", "valkeypass") or None
        live = bool(
            os.environ.get("CLAUDE_CODE_OAUTH_TOKEN") or os.environ.get("ANTHROPIC_API_KEY")
        )
        return cls(
            namespace=os.environ.get("CURIE_SANDBOX_E2E_NAMESPACE", "curie-g1"),
            pool=os.environ.get("CURIE_SANDBOX_E2E_POOL", "curie-g1-runner-pool"),
            valkey_host=os.environ.get("TEST_VALKEY_HOST", "localhost"),
            valkey_port=int(os.environ.get("TEST_VALKEY_PORT", "26379")),
            valkey_password=password,
            concurrency=int(os.environ.get("CURIE_SANDBOX_E2E_CONCURRENCY", "5")),
            batch=int(os.environ.get("CURIE_SANDBOX_E2E_BATCH", "3")),
            runs=int(os.environ.get("CURIE_SANDBOX_E2E_RUNS", "1")),
            live_creds=live,
            history_base=os.environ.get("CURIE_SANDBOX_E2E_HISTORY_BASE", ""),
        )


# -- pure helpers (offline-unit-testable) -----------------------------------


def thread_hash(thread_key: str) -> str:
    """The sha256[:10] thread hash the worker stamps on claim names and labels.

    Mirrors ``SubstrateConfig.claim_name_for`` and the ``curietech.ai/thread-hash``
    label so the scenario can select a thread's cluster-side resources by label.
    """

    return hashlib.sha256(thread_key.encode("utf-8")).hexdigest()[:10]


def unique_marker(prefix: str, seed: int) -> str:
    """A deterministic-per-(prefix, seed), collision-resistant content token.

    Deterministic so the offline unit tests can assert stability; unique across
    seeds so distinct threads carry distinct markers. No wall-clock input.
    """

    digest = hashlib.sha256(f"{prefix}:{seed}".encode()).hexdigest()[:8]
    return f"soakmark-{prefix}-{seed}-{digest}"


def final_frame(frames: Sequence[dict[str, object]]) -> dict[str, object] | None:
    """The last frame whose ``type`` is ``final``, or None if there is none."""

    for frame in reversed(frames):
        if frame.get("type") == "final":
            return frame
    return None


def collected_text(frames: Sequence[dict[str, object]]) -> str:
    """Concatenate the ``text`` field across every text-bearing frame.

    ACI outbound frames (``text_delta``, ``tool_note``, ``final``) all carry a
    ``text`` field; joining them yields the full assistant utterance for a turn.
    """

    parts: list[str] = []
    for frame in frames:
        value = frame.get("text")
        if isinstance(value, str) and value:
            parts.append(value)
    return " ".join(parts)


def pod_ready(pod: dict[str, object] | None) -> bool:
    """True when the pod reports ``Ready=True``.

    ``None`` (no such pod) and a pod with no Ready condition are both not-ready.
    """

    if pod is None:
        return False
    status = pod.get("status")
    if not isinstance(status, dict):
        return False
    conditions = status.get("conditions")
    if not isinstance(conditions, list):
        return False
    for condition in conditions:
        if isinstance(condition, dict) and condition.get("type") == "Ready":
            return condition.get("status") == "True"
    return False


def pod_identity_gone(pod: dict[str, object] | None, original_uid: str) -> bool:
    """True when the pod identity named by ``original_uid`` is no longer present.

    Two distinct cluster states both mean the original identity is gone:

    - the pod name resolves to nothing (``pod is None``), the ordinary case; and
    - the name resolves to a *different* ``metadata.uid``, which is what the
      sandbox controller produces when it recreates a deleted pod under the same
      name. A name-only check mistakes that replacement for a failed deletion.

    An unchanged UID means the original pod is still there and the caller must
    keep waiting.
    """

    if pod is None:
        return True
    return pod_uid(pod) != original_uid


def detect_cross_talk(marker: str, other_markers: Sequence[str], text: str) -> bool:
    """True if any foreign marker (a marker other than this thread's) is in ``text``.

    A thread's own ``marker`` is expected in its reply; a foreign marker leaking
    into this thread's reply is cross-talk between threads.
    """

    return any(other != marker and other in text for other in other_markers)


# -- cluster helpers (require a configured cluster) --------------------------


def kubectl(cfg: ResilienceConfig, *args: str) -> str:
    """Run a namespaced ``kubectl`` command and return stdout."""

    result = subprocess.run(
        ["kubectl", "-n", cfg.namespace, *args],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return result.stdout


def pod_of_sandbox(cfg: ResilienceConfig, name: str) -> dict[str, object]:
    """The pod object for a sandbox (pod name == sandbox name)."""

    raw = kubectl(cfg, "get", "pod", name, "-o", "json")
    return dict(json.loads(raw))


def pod_uid(pod: dict[str, object]) -> str:
    """The pod's ``metadata.uid`` (identity that changes on a rebuild)."""

    metadata = pod["metadata"]
    assert isinstance(metadata, dict)
    uid = metadata["uid"]
    assert isinstance(uid, str)
    return uid


@contextlib.contextmanager
def port_forward(cfg: ResilienceConfig, pod: str, remote_port: int) -> Iterator[str]:
    """Port-forward to a sandbox pod and yield the local base URL."""

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        local_port = probe.getsockname()[1]
    proc = subprocess.Popen(
        [
            "kubectl",
            "-n",
            cfg.namespace,
            "port-forward",
            f"pod/{pod}",
            f"{local_port}:{remote_port}",
        ],
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


def get_json(base: str, path: str) -> dict[str, object]:
    """GET ``{base}{path}`` and parse the JSON body."""

    with urllib.request.urlopen(f"{base}{path}", timeout=10) as resp:
        return dict(json.loads(resp.read()))


def post_event(
    base: str, text: str, *, token: str, user: str = "U-soak", ts: str = "1.0"
) -> list[dict[str, object]]:
    """POST an ACI ``message`` event and return the parsed NDJSON frames.

    ``token`` is the claim's per-claim runner token. The runner enforces a
    bearer on its POST routes exactly when the claim minted one and is a
    pass-through when it did not (``runner/src/curie_runner/server.py``,
    ``create_app``), so an empty token means "this claim carries no bearer",
    not "skip authentication".
    """

    body = json.dumps(
        {"kind": "event", "type": "message", "text": text, "user": user, "ts": ts}
    ).encode()
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(f"{base}/v1/event", data=body, headers=headers)
    with urllib.request.urlopen(request, timeout=90) as resp:
        return [json.loads(line) for line in resp.read().splitlines() if line.strip()]


def live_sandboxclaims(
    cfg: ResilienceConfig, thread_hash_value: str
) -> list[dict[str, object]]:
    """SandboxClaim objects tagged with the given thread hash label.

    Used to assert exactly one live claim survives a chaos kill and re-claim
    (no orphaned or duplicated claim for the thread).
    """

    raw = kubectl(
        cfg,
        "get",
        "sandboxclaims",
        "-l",
        f"curietech.ai/thread-hash={thread_hash_value}",
        "-o",
        "json",
    )
    items = json.loads(raw).get("items", [])
    return [dict(item) for item in items]


class PodReadError(RuntimeError):
    """A pod read failed for a reason that is not "the pod does not exist".

    Authorization failures, transport failures, and malformed API responses all
    raise this rather than being read as a deletion.
    """


def read_pod(cfg: ResilienceConfig, name: str) -> dict[str, object] | None:
    """The pod object for ``name``, or ``None`` when the API says NotFound.

    Only a genuine NotFound yields ``None``. Any other ``kubectl`` failure
    (RBAC, an unreachable API server, a timeout) and any unparseable or
    non-object body raise :class:`PodReadError`, so a broken read can never be
    mistaken for a deleted pod.
    """

    try:
        raw = kubectl(cfg, "get", "pod", name, "-o", "json")
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr or ""
        if "NotFound" in stderr or "not found" in stderr:
            return None
        raise PodReadError(f"pod read for {name} failed: {stderr.strip()}") from exc
    except subprocess.SubprocessError as exc:
        raise PodReadError(f"pod read for {name} failed: {exc}") from exc
    try:
        parsed = json.loads(raw)
    except ValueError as exc:
        raise PodReadError(f"pod read for {name} returned malformed JSON") from exc
    if not isinstance(parsed, dict) or "metadata" not in parsed:
        raise PodReadError(f"pod read for {name} returned an unexpected body")
    return dict(parsed)


def wait_pod_identity_gone(
    cfg: ResilienceConfig,
    sandbox_name: str,
    original_uid: str,
    *,
    timeout: float = 90.0,
    poll_interval: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, object] | None:
    """Block until the pod identity ``original_uid`` disappears; return what replaced it.

    Returns ``None`` if the name went away entirely, or the replacement pod
    object if the controller recreated it under the same name. Raises
    ``AssertionError`` if ``original_uid`` is still present at ``timeout``, and
    propagates :class:`PodReadError` for any read that did not answer the
    question.

    ``sleep`` and ``clock`` are injected so the offline unit tests can exercise
    every branch, including the timeout, without a cluster or real waiting.
    """

    deadline = clock() + timeout
    while True:
        pod = read_pod(cfg, sandbox_name)
        if pod_identity_gone(pod, original_uid):
            return pod
        if clock() >= deadline:
            raise AssertionError(
                f"pod {sandbox_name} still carries uid {original_uid} after {timeout}s"
            )
        sleep(poll_interval)


def wait_pod_ready(
    cfg: ResilienceConfig,
    sandbox_name: str,
    *,
    timeout: float = 120.0,
    poll_interval: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, object]:
    """Block until ``sandbox_name`` reports ``Ready=True`` and return that pod.

    A claim satisfied by a pod the controller has just rebuilt can be handed back
    before that pod serves, so the recovery assertions state readiness rather
    than racing a port-forward against container start. Failing to become ready
    inside ``timeout`` is a failure, never a pass: this waits for the assertion,
    it does not relax it. A read that cannot answer still raises
    :class:`PodReadError`.
    """

    deadline = clock() + timeout
    while True:
        pod = read_pod(cfg, sandbox_name)
        if pod_ready(pod):
            assert pod is not None
            return pod
        if clock() >= deadline:
            raise AssertionError(
                f"pod {sandbox_name} was not Ready within {timeout}s"
            )
        sleep(poll_interval)
