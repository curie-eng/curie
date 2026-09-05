"""Reusable helpers for the sandbox-substrate resilience E2E scenario.

Two layers live here:

- **Pure helpers** (``thread_hash``, ``unique_marker``, ``final_frame``,
  ``collected_text``, ``detect_cross_talk``) have no cluster or subprocess
  dependency and are unit-tested offline in ``test_resilience_harness_unit.py``.
- **Cluster helpers** (``kubectl``, ``pod_of_sandbox``, ``pod_uid``,
  ``port_forward``, ``get_json``, ``post_event``, ``final_frame`` consumers,
  ``live_sandboxclaims``) mirror ``apps/worker/tests/sandbox/test_e2e_k8scratch.py``
  and only run when a real cluster is configured.

The substrate seam is synchronous, so the scenario drives concurrency with a
``ThreadPoolExecutor``; nothing here is async.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import os
import re
import socket
import subprocess
import time
import urllib.request
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from urllib.parse import urlparse

from aci_protocol import Final, SessionStatus, parse_ndjson


@dataclass(frozen=True)
class ResilienceConfig:
    """Frozen knobs for one resilience E2E invocation, built from the environment.

    The namespace, pool, and Valkey defaults match the sandbox e2e template so
    the two suites can share a standing cluster and dev stack.
    """

    namespace: str
    pool: str
    valkey_host: str
    valkey_port: int
    valkey_password: str | None
    concurrency: int
    batch: int
    runs: int
    live_model: bool

    @classmethod
    def from_env(cls) -> ResilienceConfig:
        password = os.environ.get("TEST_VALKEY_PW", "valkeypass") or None
        # Credentials belong to the selected cluster pool, not necessarily to
        # this pytest process (CURIE_CREDENTIALS may come from a pod Secret).
        # An explicitly required live run must execute and fail on missing
        # prerequisites, never turn green through a host-credential skip.
        live = (
            os.environ.get("CURIE_SANDBOX_E2E_LIVE") == "1"
            or os.environ.get("CURIE_E2E_LIVE") == "1"
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
            live_model=live,
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


def assert_exact_recall(frames: Sequence[dict[str, object]], expected: str) -> None:
    """A substring in a UUID, tool note, or echoed context is not final recall."""
    final = final_frame(frames)
    assert final is not None and final.get("status") == "done", "successful final absent"
    text = final.get("text")
    assert isinstance(text, str) and text.strip() == expected, (
        "final did not recall the exact value"
    )


def required_history_fixture(env: Mapping[str, str]) -> tuple[str, str, str]:
    """Validate fixture shape, never claim authentication or a fetch from this check.

    Only a real authenticated runner recall can prove durable history. The fake
    lane remains environment-only. Decoding the scoped token prevents passing a
    raw platform key into a sandbox; the real state API still verifies its HMAC.
    """
    ref = env.get("CURIE_SANDBOX_E2E_HISTORY_REF", "")
    marker = env.get("CURIE_SANDBOX_E2E_HISTORY_MARKER", "")
    token = env.get("CURIE_SANDBOX_E2E_HISTORY_TOKEN", "")
    valid = False
    try:
        url = urlparse(ref)
        match = re.search(r"/agents/([^/]+)/state/transcript/[^/]+/?$", url.path)
        prefix, payload, signature = token.split(".")
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        valid = bool(
            url.scheme in {"http", "https"} and url.hostname
            and not url.username and not url.password and not url.query and not url.fragment
            and match and str(uuid.UUID(match.group(1))) == match.group(1)
            and marker and prefix == "sbx" and signature
            and isinstance(claims, dict) and claims.get("agent") == match.group(1)
            and claims.get("scope") == "state" and type(claims.get("exp")) is int
            and claims["exp"] > time.time()
        )
    except (ValueError, TypeError):
        pass
    assert valid, "configure a transcript-key URL, matching scoped state token and expected marker"
    return ref, token, marker


def release_claims(release: Callable[[str], None], keys: Sequence[str]) -> None:
    """Attempt every task-owned release, then fail if any cleanup failed."""
    failures: list[str] = []
    for key in keys:
        try:
            release(key)
        except Exception as exc:
            failures.append(type(exc).__name__)
    assert not failures, f"task-owned claim cleanup failed ({len(failures)} failures)"


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
    base: str,
    text: str,
    *,
    user: str = "U-soak",
    ts: str = "1.0",
    token: str = "",
    trace_id: str | None = None,
) -> list[dict[str, object]]:
    """Drive an authenticated ACI turn and require a successful terminal frame.

    HTTP 200 only means the stream opened. A classified failure, approval wait,
    missing final or malformed wire response must fail the scenario. Error
    diagnostics name types/status only; model text and bearer tokens stay out.
    """

    body = json.dumps(
        {"kind": "event", "type": "message", "text": text, "user": user, "ts": ts}
    ).encode()
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if trace_id:
        headers["traceparent"] = f"00-{trace_id}-{uuid.uuid4().hex[:16]}-01"
    request = urllib.request.Request(f"{base}/v1/event", data=body, headers=headers)
    with urllib.request.urlopen(request, timeout=90) as resp:
        raw = resp.read()
    try:
        frames = parse_ndjson(raw.decode())
    except Exception:
        # Decoder exceptions can include input_value and wire version reprs.
        # Neither model output nor hostile frame contents belong in diagnostics.
        raise AssertionError("runner emitted invalid NDJSON") from None
    assert frames and isinstance(frames[-1], Final), "turn did not end in a final frame"
    final = frames[-1]
    assert final.status == SessionStatus.DONE, f"runner terminal status was {final.status.value}"
    assert sum(isinstance(frame, Final) for frame in frames) == 1, "turn emitted multiple finals"
    assert not any(frame.type == "error" for frame in frames), "turn emitted an error frame"
    return [frame.model_dump(mode="json") for frame in frames]


def cache_reads_for_trace(observations: Sequence[dict[str, object]], trace_id: str) -> int:
    """Count only this turn's generation usage, never a global cached request.

    Langfuse documents flat usageDetails and normalized OTel cache buckets:
    https://langfuse.com/docs/observability/features/token-and-cost-tracking
    """

    total = 0
    for observation in observations:
        if observation.get("traceId") != trace_id or observation.get("type") != "GENERATION":
            continue
        usage = observation.get("usageDetails")
        if isinstance(usage, dict):
            count = usage.get("cache_read_input_tokens", usage.get("input_cached_tokens", 0))
            assert isinstance(count, int) and not isinstance(count, bool) and count >= 0, (
                "generation cache usage must be a nonnegative integer"
            )
            total += count
    return total


def trace_cache_reads(trace_id: str) -> int:
    """Read one exact trace from the real Langfuse v3 public API."""

    host = os.environ.get("LANGFUSE_HOST", "http://localhost:23000").rstrip("/")
    public_key = os.environ.get("LANGFUSE_PUBLIC_KEY", "pk-lf-curie-dev")
    secret_key = os.environ.get("LANGFUSE_SECRET_KEY", "sk-lf-curie-dev")
    bearer = base64.b64encode(f"{public_key}:{secret_key}".encode()).decode()
    # Same supported v3 query as curie_api.langfuse.LangfuseClient.observations.
    request = urllib.request.Request(
        f"{host}/api/public/observations?traceId={trace_id}&limit=100",
        headers={"Authorization": f"Basic {bearer}"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        payload = json.loads(response.read())
    return cache_reads_for_trace(payload["data"], trace_id)


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
