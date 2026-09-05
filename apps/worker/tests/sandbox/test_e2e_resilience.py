"""Sandbox-substrate resilience E2E on a disposable, task-owned cluster.

The scenario drives the production SandboxSubstrate, real Valkey and runner ACI.
Run three repetitions with ``CURIE_SANDBOX_E2E=1 CURIE_SANDBOX_E2E_RUNS=3 uv run
pytest apps/worker/tests/sandbox/test_e2e_resilience.py -q``. The ordinary
collector excludes this file unless the gate is enabled. Namespace, pool and
capacity are selected by the CURIE_SANDBOX_E2E environment family; provision at
least concurrency + batch ready replicas. Never target the permanent mail soak.

Phases A/B assert distinct sandboxes, conversation affinity and a concurrent
burst. Phase C replaces an idle sandbox and proves one surviving claim. Phase D
asserts cold suspend/resume environment and a successful authenticated turn.
Every ACI turn must end successfully: an HTTP 200 stream or a failed final does
not pass. Set CURIE_SANDBOX_E2E_LIVE=1 (or CURIE_E2E_LIVE=1) to require a live
pool, content isolation, durable recall and cache evidence. Credentials may live
only in the pool's Secret; missing prerequisites fail the live run, never skip it.
The whole scenario preflights its phase-D fixture before creating any claims:
CURIE_SANDBOX_E2E_HISTORY_REF must name a task-owned real state API transcript,
CURIE_SANDBOX_E2E_HISTORY_TOKEN supplies its scoped credential and
CURIE_SANDBOX_E2E_HISTORY_MARKER for the synthetic token seeded in that transcript.

These phases do not claim a worker process kill, a mid-tool pod kill, actual
side-effect idempotency, eval-stream delivery or cache reuse merely from pod
identity. The companion test_delivery_resilience.py counts durable subprocess
receipts across fresh kernel/runner instances and real PEL reclaim, with fake
Kubernetes and provider seams. Actual pod/worker crash and eval-fanout acceptance
still needs separate disposable-cluster execution.

The live cache probe drives two successful turns and queries their explicit
trace identity in Langfuse. A missing endpoint, missing usage or unrelated cached
observation cannot qualify. The runner already exports cache-token OTel fields;
a stale xfail on that export would mask a regression.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
import uuid
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

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
    assert_exact_recall,
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
    release_claims,
    required_history_fixture,
    thread_hash,
    trace_cache_reads,
    unique_marker,
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


def _drive_turn(
    cfg: ResilienceConfig,
    sandbox_name: str,
    port: int,
    text: str,
    *,
    user: str,
    ts: str,
    token: str = "",
    trace_id: str | None = None,
) -> list[dict[str, object]]:
    """Port-forward to a sandbox, assert health, post one ACI turn, return frames."""

    if cfg.live_model:
        # Read only the model-mode bit. Never dump the pod's credential env.
        mode = kubectl(
            cfg, "exec", sandbox_name, "-c", "runner", "--", "python", "-c",
            "import os; print('fake' if os.environ.get('CURIE_FAKE_MODEL','').lower() "
            "in {'1','true','yes'} else 'live')",
        ).strip()
        assert mode == "live", "required live run reached a fake-model or unverified pod"
    with port_forward(cfg, sandbox_name, port) as base:
        assert get_json(base, "/healthz") == {"ok": True}
        return post_event(base, text, user=user, ts=ts, token=token, trace_id=trace_id)


def _assert_final(frames: Sequence[dict[str, object]]) -> None:
    final = final_frame(frames)
    types = [f.get("type") for f in frames]
    assert final is not None, f"turn did not end in a final frame: {types}"


def _wait_pod_gone(cfg: ResilienceConfig, sandbox_name: str, timeout: float = 90.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            pod_of_sandbox(cfg, sandbox_name)
        except subprocess.CalledProcessError:
            return
        time.sleep(1)
    raise AssertionError(f"pod {sandbox_name} was never deleted within {timeout}s")


@pytest.mark.parametrize("run", range(_RUNS))
def test_e2e_resilience(
    run: int, substrate: object, cfg: ResilienceConfig, pool_ready: None
) -> None:
    from aci_protocol import BootEnv
    from curie_worker.sandbox import HISTORY_ENV, SandboxHandle, SandboxSubstrate

    assert isinstance(substrate, SandboxSubstrate)
    # The operator prepares an isolated transcript through the real state API.
    # An arbitrary marker is not a valid history ref: runner boot rejects it.
    history_ref, history_token, history_marker = required_history_fixture(os.environ)
    print(f"\nEVIDENCE resilience_run={run} concurrency={cfg.concurrency} batch={cfg.batch}")

    scope = uuid.uuid4().hex
    a_keys = [f"resilience-{scope}-a-{run}-{i}" for i in range(cfg.concurrency)]
    batch_keys = [f"resilience-{scope}-batch-{run}-{j}" for j in range(cfg.batch)]
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
            frames = _drive_turn(
                cfg, handle.sandbox_name, handle.port, text,
                user=key, ts="1.0", token=handle.token,
            )
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
        if cfg.live_model:
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
            frames = _drive_turn(
                cfg, handle.sandbox_name, handle.port, "batch turn under load",
                user=key, ts="1.0", token=handle.token,
            )
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

        # -- Phase C: idle sandbox replacement -------------------------------
        # One live claim proves substrate cleanup only. It does not count tool
        # side effects or claim to interrupt an active turn.
        victim = a_keys[-1]
        victim_hash = thread_hash(victim)
        victim_sandbox = claimed[victim].sandbox_name
        old_uid = uids[victim]
        kubectl(cfg, "delete", "pod", victim_sandbox, "--wait=false")
        _wait_pod_gone(cfg, victim_sandbox)
        print(f"EVIDENCE phase_c_killed_pod uid={old_uid}")

        fresh = substrate.claim(victim)
        claimed[victim] = fresh
        new_uid = pod_uid(pod_of_sandbox(cfg, fresh.sandbox_name))
        assert new_uid != old_uid, "re-claim returned the killed pod UID"
        frames = _drive_turn(
            cfg, fresh.sandbox_name, fresh.port, "back after a kill",
            user=victim, ts="2.0", token=fresh.token,
        )
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

        def _sustained(key: str) -> str:
            handle = claimed[key]
            frames = _drive_turn(
                cfg, handle.sandbox_name, handle.port, "sustained follow-up",
                user=key, ts="3.0", token=handle.token,
            )
            _assert_final(frames)
            return key

        original_claim = claimed[target].claim_name
        with ThreadPoolExecutor(max_workers=max(len(others), 1)) as pool:
            load = pool.map(_sustained, others) if others else iter(())
            substrate.suspend(target, history_ref=history_ref)
            _wait_pod_gone(cfg, claimed[target].sandbox_name)
            resumed = substrate.resume(
                target, env={BootEnv.env_key("history_token"): history_token},
            )
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
        actual_history_ref = env.get(HISTORY_ENV)
        history_token_matches = env.get(BootEnv.env_key("history_token")) == history_token
        del env, containers, resumed_pod, history_token
        assert actual_history_ref == history_ref, "resumed pod missing injected history ref"
        assert history_token_matches, "resumed pod missing matching scoped history credential"
        frames = _drive_turn(
            cfg, resumed.sandbox_name, resumed.port,
            "What exact verification token was recorded in the durable transcript? "
            "Reply only that token.",
            user=target, ts="4.0", token=resumed.token,
        )
        _assert_final(frames)
        if cfg.live_model:
            assert_exact_recall(frames, history_marker)
        for key in others:
            assert pod_uid(pod_of_sandbox(cfg, claimed[key].sandbox_name)) == uids[key], (
                f"suspend/resume disturbed concurrently loaded thread {key}"
            )
        proof = "authenticated_runner_recall" if cfg.live_model else "UNPROVED_environment_only"
        print(f"EVIDENCE phase_d_history={proof}")

        # -- Cache-warmth proxy: same pod across consecutive turns ------------
        stable = loaded[-1] if len(loaded) > 1 else target
        first_uid = pod_uid(pod_of_sandbox(cfg, claimed[stable].sandbox_name))
        frames = _drive_turn(
            cfg, claimed[stable].sandbox_name, claimed[stable].port, "warmth turn one",
            user=stable, ts="5.0", token=claimed[stable].token,
        )
        _assert_final(frames)
        frames = _drive_turn(
            cfg, claimed[stable].sandbox_name, claimed[stable].port, "warmth turn two",
            user=stable, ts="6.0", token=claimed[stable].token,
        )
        _assert_final(frames)
        second_uid = pod_uid(pod_of_sandbox(cfg, claimed[stable].sandbox_name))
        assert second_uid == first_uid, "pod rebound between consecutive turns (cache lost)"
        print(f"EVIDENCE same_pod_across_turns uid={first_uid}")

    finally:
        release_claims(substrate.release, list(claimed))


@pytest.mark.skipif(
    not ResilienceConfig.from_env().live_model,
    reason="cache is outside the fake tier; select CURIE_SANDBOX_E2E_LIVE=1 for the live probe",
)
def test_cache_read_tokens_probe(
    substrate: object, cfg: ResilienceConfig, pool_ready: None,
) -> None:
    """Attribute cache reads to an actual follow-up, with an absent-trace control."""

    from curie_worker.sandbox import SandboxSubstrate

    assert isinstance(substrate, SandboxSubstrate)
    key = f"cache-{uuid.uuid4().hex}"
    handle = substrate.claim(key)
    try:
        # A unique, substantial prompt makes the continuation useful for a
        # real cache probe. Two tiny replies alone may never reach a provider's
        # minimum cacheable prefix size.
        values = [unique_marker(key, i) for i in range(500)]
        context = "\n".join(f"Record {i}: {value}" for i, value in enumerate(values))
        _drive_turn(
            cfg, handle.sandbox_name, handle.port,
            f"Remember these records for the next turn. Reply only READY.\n{context}",
            user=key, ts="1.0", token=handle.token,
        )
        trace_id = uuid.uuid4().hex
        followup = _drive_turn(
            cfg, handle.sandbox_name, handle.port,
            "What is the exact value in Record 17? Reply only that value.",
            user=key, ts="2.0", token=handle.token, trace_id=trace_id,
        )
        assert_exact_recall(followup, values[17])
        deadline = time.monotonic() + 120
        while True:
            reads = trace_cache_reads(trace_id)
            if reads > 0:
                break
            assert time.monotonic() < deadline, "follow-up trace has no observed cache reads"
            time.sleep(2)
        assert trace_cache_reads(uuid.uuid4().hex) == 0, "unobserved trace reported cached tokens"
        print(f"EVIDENCE cache_followup_trace={trace_id} cache_read_input_tokens={reads}")
    finally:
        release_claims(substrate.release, [key])
