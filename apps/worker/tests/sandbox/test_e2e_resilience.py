"""Sandbox-substrate resilience E2E against a standing Curie cluster.

The scenario drives the worker's ``SandboxSubstrate`` plus Valkey plus
``kubectl`` directly, mirroring ``apps/worker/tests/sandbox/test_e2e_k8scratch.py``.
There is no REST thread or message API to drive: a turn is a ``kubectl
port-forward`` to the sandbox pod followed by ``POST /v1/event`` (NDJSON frames
ending in a ``final``).

It is opt-in: the sandbox collector excludes this module unless
``CURIE_SANDBOX_E2E=1`` and the module retains the same guard for direct
collection. The offline helpers in ``test_resilience_harness_unit.py`` remain in
the normal ``apps/worker/tests`` collection and need neither a cluster nor a dev
stack. To run the scenario three consecutive times against a standing cluster
and dev-stack Valkey:

``CURIE_SANDBOX_E2E=1 CURIE_SANDBOX_E2E_RUNS=3 uv run pytest
apps/worker/tests/sandbox/test_e2e_resilience.py -q``

Size the warm pool to at least ``CURIE_SANDBOX_E2E_CONCURRENCY +
CURIE_SANDBOX_E2E_BATCH`` ready replicas. ``pool_ready`` blocks until the pool
reports that capacity, so the concurrent claims and batch burst do not starve.
``CURIE_SANDBOX_E2E_NAMESPACE`` and ``CURIE_SANDBOX_E2E_POOL`` select the
standing-cluster resources; the namespace/pool and Valkey defaults match the
sandbox E2E template. ``CURIE_SANDBOX_E2E_HISTORY_BASE`` is required by Phase D:
the resumed pod boots its ``CURIE_HISTORY_REF``, and the runner accepts only an
``http(s)://`` state-API transcript-key URL, so the phase needs a
cluster-reachable prefix to hang each thread's marker off.

With a live Claude credential, reply-content isolation
assertions and the cache-token probe are also enabled. Without one, the
fake-model runner still proves every structural property but does not echo
markers, so content-level cross-talk assertions do not run.

Four phases share the module-scoped substrate and a set of held claims:

- Phase A: concurrent threads are isolated (distinct sandboxes) and affined
  (re-claim returns the same sandbox); with live creds, no content cross-talk.
- Phase B: a mid-thread batch burst runs while Phase-A threads are held, and
  leaves them undisturbed (same pod UIDs).
- Phase C: a sandbox is killed mid-run (unclean pod delete); the thread
  re-claims a fresh sandbox and exactly one live claim survives (no orphan or
  duplicate side effect at the substrate level). Waiting for the kill to land is
  UID-aware: the controller frequently recreates the pod under the same name, so
  the scenario waits for the *original* ``metadata.uid`` to disappear rather than
  for the name, and a failed read raises instead of counting as a deletion.
- Phase D: one thread is suspended and resumed under sustained load on the
  others; the resumed pod carries the injected history ref and the loaded
  threads keep their pods.

A cache-warmth proxy asserts pod-UID affinity across consecutive turns (the
ADR-0003 "same pod across turns" property that enables prompt-cache reuse); see
the documented assumptions below for why the direct
``cache_read_input_tokens`` signal is not cluster-observable today.

Documented assumptions and gaps:

1. **"Batch job" is interpreted as a concurrent burst under sustained load.**
   The batch phase launches ``CURIE_SANDBOX_E2E_BATCH`` additional threads while
   the Phase-A threads are still held claimed, and asserts the batch turns
   complete without disturbing the held threads. An alternative reading of
   "batch job" is an eval fan-out (an ``XADD`` to the ``curie:evals`` stream
   consumed by a separate consumer group). That path is not part of the sandbox
   substrate this scenario drives, so it is noted here as an alternative rather
   than exercised.

2. **"No duplicate side effects" is asserted at the substrate level (one live
   claim survives a kill), not as end-to-end side-effect idempotency.** The
   semantic invariant, a failed run that emitted a side effect escalates to a
   human instead of auto-retrying, is the kernel's fourth rule in
   ``apps/worker/CLAUDE.md`` and already has a provoking integration test in
   ``apps/worker/tests/kernel``. This scenario drives the ``SandboxSubstrate``
   seam directly, mirroring ``test_e2e_k8scratch.py``, and therefore asserts the
   observable substrate-level proxy: after an unclean kill and re-claim, exactly
   one live ``SandboxClaim`` remains for the thread hash (no orphaned or
   duplicated claim). Re-driving turns through the kernel path (a Valkey
   ``curie:runs`` producer plus a fake Slack sink plus the in-cluster consumer)
   to count actual side-effect executions is deliberately outside this
   footprint: it would duplicate the kernel suite's existing coverage and pull
   this scenario away from the substrate seam it is meant to stress.

3. **``cache_read_input_tokens`` is not observable at the cluster level, so the
   scenario asserts pod-UID affinity as the cache-warmth proxy.** The runner's
   OTel export (``runner/src/curie_runner/otel.py``,
   ``_GenerationSpan.record_usage``) exports only
   ``gen_ai.usage.input_tokens`` and ``output_tokens`` and drops the cache-token
   fields, so Langfuse never records ``cache_read_input_tokens``. It is asserted
   only at the SDK layer in ``runner/tests/test_live.py``. The scenario therefore
   asserts the cluster-observable property that enables cache reuse: the same
   pod (same pod UID) serves consecutive turns on a thread (ADR-0003 "same pod
   across turns"). The single ``xfail`` probe, ``test_cache_read_tokens_probe``,
   reads per-trace usage from Langfuse and would turn green if the OTel export is
   later extended.

Follow-ups outside this footprint:

- Extend ``runner/src/curie_runner/otel.py`` to export
  ``cache_read_input_tokens`` and ``cache_creation_input_tokens``; the ``xfail``
  probe becomes a real assertion then.
- Add an end-to-end side-effect-idempotency resilience scenario that drives
  turns through the kernel path (a Valkey ``curie:runs`` producer plus a fake
  Slack sink plus the in-cluster consumer), kills a sandbox after a
  side-effecting tool call fires, re-drives, and asserts the effect executed
  exactly once.
- If an eval-fanout batch interpretation is wanted, add a phase that drives the
  ``curie:evals`` stream and its consumer group.
- ``SandboxSubstrate.claim`` can hand back a handle whose pod the controller has
  just rebuilt and which is not serving yet; it waits for the serviceFQDN, not
  for readiness. The scenario waits for ``Ready`` itself (see Phase C). Whether
  the substrate should do that for its callers is a product question, filed
  separately rather than patched from a test.
"""

from __future__ import annotations

import os
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Protocol

import pytest

# importlib import mode does not add this test directory to sys.path.
sys.path.insert(0, str(Path(__file__).parent))

from resilience_fixtures import (  # noqa: E402, F401
    resilience_cfg,
    resilience_pool_ready,
    resilience_substrate,
)
from resilience_harness import (  # noqa: E402
    ResilienceConfig,
    collected_text,
    detect_cross_talk,
    final_frame,
    get_json,
    kubectl,
    live_sandboxclaims,
    pod_of_sandbox,
    pod_uid,
    port_forward,
    post_event,
    thread_hash,
    unique_marker,
    wait_pod_identity_gone,
    wait_pod_ready,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("CURIE_SANDBOX_E2E") != "1",
    reason=(
        "sandbox-substrate resilience E2E; set CURIE_SANDBOX_E2E=1 with a "
        "standing cluster + dev stack"
    ),
)

# Evaluated at import so parametrization reflects CURIE_SANDBOX_E2E_RUNS. When
# explicitly collected without the gate, this still yields a single skipped param.
_RUNS = ResilienceConfig.from_env().runs


class _Claimed(Protocol):
    """The part of ``SandboxHandle`` a turn needs: where to dial and how to authenticate."""

    sandbox_name: str
    port: int
    token: str


def _drive_turn(
    cfg: ResilienceConfig,
    handle: _Claimed,
    text: str,
    *,
    user: str,
    ts: str,
) -> list[dict[str, object]]:
    """Port-forward to a claim's sandbox, assert health, post one ACI turn.

    The claim's own token authenticates the turn: a resumed claim mints a
    per-claim runner bearer that a warm-pool binding does not, so driving every
    turn through the handle keeps both paths honest.
    """

    with port_forward(cfg, handle.sandbox_name, handle.port) as base:
        assert get_json(base, "/healthz") == {"ok": True}
        return post_event(base, text, token=handle.token, user=user, ts=ts)


def _assert_final(frames: Sequence[dict[str, object]]) -> None:
    final = final_frame(frames)
    types = [f.get("type") for f in frames]
    assert final is not None, f"turn did not end in a final frame: {types}"


@pytest.mark.parametrize("run", range(_RUNS))
def test_e2e_resilience(
    run: int, substrate: object, cfg: ResilienceConfig, pool_ready: None
) -> None:
    from curie_worker.sandbox import HISTORY_ENV, SandboxHandle, SandboxSubstrate

    assert isinstance(substrate, SandboxSubstrate)
    print(f"\nEVIDENCE resilience_run={run} concurrency={cfg.concurrency} batch={cfg.batch}")

    a_keys = [f"soak-a-{run}-{i}" for i in range(cfg.concurrency)]
    batch_keys = [f"soak-batch-{run}-{j}" for j in range(cfg.batch)]
    claimed: dict[str, SandboxHandle] = {}
    markers: dict[str, str] = {}
    uids: dict[str, str] = {}

    try:
        # -- Phase A: concurrent threads, isolation + affinity ----------------
        def _claim(key: str) -> tuple[str, SandboxHandle]:
            return key, substrate.claim(key)

        with ThreadPoolExecutor(max_workers=cfg.concurrency) as pool:
            for key, handle in pool.map(_claim, a_keys):
                claimed[key] = handle
        for idx, key in enumerate(a_keys):
            markers[key] = unique_marker(f"a-r{run}", idx)

        sandbox_names = {h.sandbox_name for h in claimed.values()}
        assert len(sandbox_names) == cfg.concurrency, "distinct threads shared a sandbox"
        print(f"EVIDENCE phase_a_distinct_sandboxes={len(sandbox_names)}")

        def _turn(key: str) -> tuple[str, list[dict[str, object]]]:
            handle = claimed[key]
            text = f"Please remember this token exactly: {markers[key]}"
            frames = _drive_turn(cfg, handle, text, user=key, ts="1.0")
            return key, frames

        replies: dict[str, list[dict[str, object]]] = {}
        with ThreadPoolExecutor(max_workers=cfg.concurrency) as pool:
            for key, frames in pool.map(_turn, a_keys):
                _assert_final(frames)
                replies[key] = frames

        # Affinity: re-claim returns the same sandbox, and record pod UIDs.
        for key in a_keys:
            again = substrate.claim(key)
            assert again.sandbox_name == claimed[key].sandbox_name, "affinity broke on re-claim"
            uids[key] = pod_uid(pod_of_sandbox(cfg, claimed[key].sandbox_name))
        print("EVIDENCE phase_a_affinity_stable=true")

        # Content-level no-cross-talk only when a real model is behind the runner.
        if cfg.live_creds:
            all_markers = list(markers.values())
            for key in a_keys:
                text = collected_text(replies[key])
                assert markers[key] in text, f"thread {key} reply dropped its own marker"
                assert not detect_cross_talk(markers[key], all_markers, text), (
                    f"thread {key} reply leaked a foreign marker"
                )
            print("EVIDENCE phase_a_no_content_cross_talk=true")

        # -- Phase B: mid-thread batch burst under sustained hold -------------
        # "batch job" is interpreted as a burst of concurrent threads launched
        # while the Phase-A threads are still held claimed. (An alternative
        # reading, an eval fan-out XADD to curie:evals, is a separate consumer
        # group not exercised by the sandbox substrate; see the module docstring.)
        def _batch_turn(key: str) -> tuple[str, list[dict[str, object]]]:
            handle = substrate.claim(key)
            claimed[key] = handle
            frames = _drive_turn(cfg, handle, "batch turn under load", user=key, ts="1.0")
            return key, frames

        with ThreadPoolExecutor(max_workers=max(cfg.batch, 1)) as pool:
            for _key, frames in pool.map(_batch_turn, batch_keys):
                _assert_final(frames)
        print(f"EVIDENCE phase_b_batch_turns_final={cfg.batch}")

        # Phase-A threads are undisturbed: same pod UIDs, follow-up lands same pod.
        for key in a_keys:
            assert pod_uid(pod_of_sandbox(cfg, claimed[key].sandbox_name)) == uids[key], (
                f"batch burst disturbed Phase-A thread {key}"
            )
        probe = a_keys[0]
        _, frames = _turn(probe)
        _assert_final(frames)
        assert pod_uid(pod_of_sandbox(cfg, claimed[probe].sandbox_name)) == uids[probe]
        print("EVIDENCE phase_b_phase_a_undisturbed=true")

        # -- Phase C: sandbox killed mid-run ---------------------------------
        # The kernel's no-auto-retry-after-side-effects escalation is unit-tested
        # in apps/worker/tests/kernel; here we assert the substrate-level proxy
        # for "no duplicate side effect": exactly one live claim survives a kill.
        victim = a_keys[-1]
        victim_hash = thread_hash(victim)
        victim_sandbox = claimed[victim].sandbox_name
        old_uid = uids[victim]
        kubectl(cfg, "delete", "pod", victim_sandbox, "--wait=false")
        kill_start = time.monotonic()
        replacement = wait_pod_identity_gone(cfg, victim_sandbox, old_uid)
        replacement_uid = pod_uid(replacement) if replacement is not None else None
        print(
            f"EVIDENCE phase_c_killed_pod uid={old_uid} "
            f"replacement_uid={replacement_uid} "
            f"gone_after_s={time.monotonic() - kill_start:.1f}"
        )

        fresh = substrate.claim(victim)
        claimed[victim] = fresh
        new_uid = pod_uid(pod_of_sandbox(cfg, fresh.sandbox_name))
        assert new_uid != old_uid, "re-claim returned the killed pod UID"
        # The replacement pod may be seconds old, so state the readiness the
        # recovery turn depends on instead of racing a port-forward against it.
        assert pod_uid(wait_pod_ready(cfg, fresh.sandbox_name)) == new_uid
        print(f"EVIDENCE phase_c_replacement_ready uid={new_uid}")
        frames = _drive_turn(cfg, fresh, "back after a kill", user=victim, ts="2.0")
        _assert_final(frames)
        uids[victim] = new_uid

        # Exactly one live claim for this thread (no orphan/duplicate). Allow a
        # brief settle for the evicted claim's deletion to finalize.
        deadline = time.monotonic() + 30
        live_count = 0
        while time.monotonic() < deadline:
            live_count = len(live_sandboxclaims(cfg, victim_hash))
            if live_count == 1:
                break
            time.sleep(2)
        assert live_count == 1, f"expected exactly one live claim, saw {live_count}"
        print("EVIDENCE phase_c_single_live_claim=true")

        # -- Phase D: resume-rehydrate under sustained load -------------------
        loaded = [k for k in a_keys if k != victim][: max(cfg.concurrency - 1, 1)]
        target = loaded[0]
        others = loaded[1:]
        # The resumed pod boots the ref, so it has to be a state-API transcript
        # URL; the per-thread marker rides in its path to keep the injected value
        # unique per thread.
        assert cfg.history_base, (
            "set CURIE_SANDBOX_E2E_HISTORY_BASE to a cluster-reachable state-API "
            "transcript-key prefix; the runner rejects a non-http history ref at boot"
        )
        target_marker = f"{cfg.history_base.rstrip('/')}/{markers[target]}"

        def _sustained(key: str) -> str:
            handle = claimed[key]
            frames = _drive_turn(cfg, handle, "sustained follow-up", user=key, ts="3.0")
            _assert_final(frames)
            return key

        original_claim = claimed[target].claim_name
        with ThreadPoolExecutor(max_workers=max(len(others), 1)) as pool:
            load = pool.map(_sustained, others) if others else iter(())
            substrate.suspend(target, history_ref=target_marker)
            suspended_uid = uids[target]
            wait_pod_identity_gone(cfg, claimed[target].sandbox_name, suspended_uid)
            resumed = substrate.resume(target)
            claimed[target] = resumed
            list(load)

        assert resumed.claim_name != original_claim, "resume reused the suspended claim"
        resumed_pod = pod_of_sandbox(cfg, resumed.sandbox_name)
        containers = resumed_pod["spec"]["containers"]  # type: ignore[index]
        assert isinstance(containers, list)
        env = {
            e.get("name"): e.get("value")
            for c in containers
            for e in c.get("env", [])
        }
        assert env.get(HISTORY_ENV) == target_marker, "resumed pod missing injected history ref"
        assert pod_uid(wait_pod_ready(cfg, resumed.sandbox_name)) == pod_uid(resumed_pod)
        frames = _drive_turn(cfg, resumed, "resumed and rehydrated", user=target, ts="4.0")
        _assert_final(frames)
        for key in others:
            assert pod_uid(pod_of_sandbox(cfg, claimed[key].sandbox_name)) == uids[key], (
                f"suspend/resume disturbed concurrently loaded thread {key}"
            )
        print(f"EVIDENCE phase_d_resume_injected_history_ref pod={resumed.sandbox_name}")

        # -- Cache-warmth proxy: same pod across consecutive turns ------------
        stable = loaded[-1] if len(loaded) > 1 else target
        first_uid = pod_uid(pod_of_sandbox(cfg, claimed[stable].sandbox_name))
        frames = _drive_turn(cfg, claimed[stable], "warmth turn one", user=stable, ts="5.0")
        _assert_final(frames)
        frames = _drive_turn(cfg, claimed[stable], "warmth turn two", user=stable, ts="6.0")
        _assert_final(frames)
        second_uid = pod_uid(pod_of_sandbox(cfg, claimed[stable].sandbox_name))
        assert second_uid == first_uid, "pod rebound between consecutive turns (cache lost)"
        print(f"EVIDENCE same_pod_across_turns uid={first_uid}")

    finally:
        for key in list(claimed):
            try:
                substrate.release(key)
            except Exception:
                pass


@pytest.mark.xfail(
    strict=False,
    reason=(
        "otel.py does not export cache_read_input_tokens; "
        "see runner/src/curie_runner/otel.py -- tracked follow-up"
    ),
)
@pytest.mark.skipif(
    not (os.environ.get("CLAUDE_CODE_OAUTH_TOKEN") or os.environ.get("ANTHROPIC_API_KEY")),
    reason="cache-token probe needs live creds so a real model reports usage",
)
def test_cache_read_tokens_probe(cfg: ResilienceConfig) -> None:
    """Probe: assert a per-trace ``cache_read_input_tokens`` is observable.

    This lights up green only if the runner's OTel export is later extended to
    carry the cache-token fields (today ``_GenerationSpan.record_usage`` drops
    them, so Langfuse never sees them). Reading is via the Langfuse public API
    using the dev-stack keys; if Langfuse is not reachable the probe skips.
    """

    host = os.environ.get("LANGFUSE_HOST", "http://localhost:23000")
    public_key = os.environ.get("LANGFUSE_PUBLIC_KEY", "pk-lf-curie-dev")
    secret_key = os.environ.get("LANGFUSE_SECRET_KEY", "sk-lf-curie-dev")

    import base64

    token = base64.b64encode(f"{public_key}:{secret_key}".encode()).decode()
    request = urllib.request.Request(
        f"{host}/api/public/observations?limit=50",
        headers={"Authorization": f"Basic {token}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as resp:
            import json

            payload = json.loads(resp.read())
    except (urllib.error.URLError, TimeoutError) as exc:
        pytest.skip(f"Langfuse not reachable for the cache-token probe: {exc}")

    observations = payload.get("data", [])
    cache_reads = [
        (o.get("usage") or {}).get("cache_read_input_tokens", 0) for o in observations
    ]
    assert any((count or 0) > 0 for count in cache_reads), (
        "no observation reported cache_read_input_tokens > 0 "
        "(otel.py drops the cache-token fields today)"
    )
