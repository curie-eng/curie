"""Offline unit tests for the pure sandbox-resilience helpers.

These run with no cluster and no dev stack: they exercise only the pure
functions in ``resilience_harness.py`` (``thread_hash``, ``unique_marker``,
``final_frame``, ``collected_text``, ``detect_cross_talk``). They are deliberately
not gated by ``CURIE_SANDBOX_E2E`` so the harness logic stays covered in default
CI collection.
"""

from __future__ import annotations

import hashlib
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from resilience_harness import (  # noqa: E402
    assert_exact_recall,
    cache_reads_for_trace,
    collected_text,
    detect_cross_talk,
    final_frame,
    release_claims,
    required_history_fixture,
    thread_hash,
    unique_marker,
)


def test_recall_requires_exact_successful_final_not_substring_or_tool_note() -> None:
    expected = "soakmark-cache-example-17-f00"
    assert_exact_recall([{"type": "final", "status": "done", "text": expected}], expected)
    for frames in (
        [{"type": "final", "status": "done", "text": "prefix " + expected}],
        [{"type": "tool_note", "text": expected}, {"type": "final", "text": "unrelated"}],
        [{"type": "final", "status": "classified-failure", "text": expected}],
        [{"type": "final", "status": "done", "text": "a-key-with-289-inside"}],
    ):
        with pytest.raises(AssertionError):
            assert_exact_recall(frames, expected)


def _history_env() -> dict[str, str]:
    from curie_worker.sandbox_token import mint

    agent = "00000000-0000-0000-0000-000000000001"
    return {
        "CURIE_SANDBOX_E2E_HISTORY_REF":
            f"https://api.example.com/agents/{agent}/state/transcript/example-thread",
        "CURIE_SANDBOX_E2E_HISTORY_MARKER": "synthetic-history-example-marker",
        "CURIE_SANDBOX_E2E_HISTORY_TOKEN": mint(
            "test-signing-key", agent=agent, scope="state", exp=int(time.time()) + 300,
        ),
    }


def test_history_fixture_requires_transcript_key_and_scoped_token_shape() -> None:
    env = _history_env()
    ref, token, marker = required_history_fixture(env)
    assert ref == env["CURIE_SANDBOX_E2E_HISTORY_REF"]
    assert token == env["CURIE_SANDBOX_E2E_HISTORY_TOKEN"]
    assert marker == env["CURIE_SANDBOX_E2E_HISTORY_MARKER"]


@pytest.mark.parametrize("key,value", [
    ("CURIE_SANDBOX_E2E_HISTORY_REF", "https://example.com/"),
    ("CURIE_SANDBOX_E2E_HISTORY_REF", "not-a-url"),
    ("CURIE_SANDBOX_E2E_HISTORY_REF",
     "https://api.example.com/agents/00000000-0000-0000-0000-000000000002/state/transcript/key"),
    ("CURIE_SANDBOX_E2E_HISTORY_TOKEN", ""),
    ("CURIE_SANDBOX_E2E_HISTORY_TOKEN", "platform-key-sentinel"),
    ("CURIE_SANDBOX_E2E_HISTORY_TOKEN", "sbx.not-json.signature"),
    ("CURIE_SANDBOX_E2E_HISTORY_MARKER", ""),
])
def test_invalid_history_fixture_is_refused_without_echo(key: str, value: str) -> None:
    env = _history_env()
    env[key] = value
    with pytest.raises(AssertionError) as caught:
        required_history_fixture(env)
    assert "platform-key-sentinel" not in str(caught.value)


def test_cleanup_attempts_every_owned_release_and_reports_failure_without_payload() -> None:
    calls: list[str] = []

    def release(key: str) -> None:
        calls.append(key)
        if key == "broken":
            raise RuntimeError("private-response-sentinel")

    with pytest.raises(AssertionError) as caught:
        release_claims(release, ["broken", "healthy"])
    assert calls == ["broken", "healthy"]
    assert "private-response-sentinel" not in str(caught.value)
    calls.clear()
    release_claims(release, ["healthy", "another"])
    assert calls == ["healthy", "another"]


def test_thread_hash_matches_sha256_prefix() -> None:
    key = "soak-thread-42"
    expected = hashlib.sha256(key.encode("utf-8")).hexdigest()[:10]
    assert thread_hash(key) == expected
    assert len(thread_hash(key)) == 10


def test_thread_hash_distinct_keys_distinct_hashes() -> None:
    assert thread_hash("soak-thread-1") != thread_hash("soak-thread-2")


def test_unique_marker_is_deterministic_per_seed() -> None:
    assert unique_marker("phase-a", 3) == unique_marker("phase-a", 3)


def test_unique_marker_is_unique_across_seeds() -> None:
    markers = {unique_marker("phase-a", seed) for seed in range(50)}
    assert len(markers) == 50


def test_unique_marker_format() -> None:
    marker = unique_marker("phase-a", 7)
    assert marker.startswith("soakmark-phase-a-7-")
    assert " " not in marker


def test_final_frame_picks_last_final() -> None:
    frames: list[dict[str, object]] = [
        {"type": "text_delta", "text": "thinking"},
        {"type": "final", "text": "first final"},
        {"type": "text_delta", "text": "more"},
        {"type": "final", "text": "second final"},
    ]
    result = final_frame(frames)
    assert result is not None
    assert result["text"] == "second final"


def test_final_frame_none_when_absent() -> None:
    frames: list[dict[str, object]] = [{"type": "text_delta", "text": "no final here"}]
    assert final_frame(frames) is None


def test_collected_text_concatenates_text_fields() -> None:
    frames: list[dict[str, object]] = [
        {"type": "text_delta", "text": "hello"},
        {"type": "tool_note", "text": "searching", "tool": "search"},
        {"type": "final", "text": "world", "status": "done"},
    ]
    assert collected_text(frames) == "hello searching world"


def test_collected_text_ignores_non_text_frames() -> None:
    frames: list[dict[str, object]] = [
        {"type": "final", "text": "only this", "status": "done"},
        {"type": "side_effect_flag"},
        {"type": "text_delta", "text": ""},
    ]
    assert collected_text(frames) == "only this"


def test_detect_cross_talk_true_when_foreign_marker_present() -> None:
    own = "soakmark-a-0-aaaa"
    others = [own, "soakmark-b-1-bbbb"]
    text = f"reply carrying {own} and leaked soakmark-b-1-bbbb"
    assert detect_cross_talk(own, others, text) is True


def test_detect_cross_talk_false_when_only_own_marker_present() -> None:
    own = "soakmark-a-0-aaaa"
    others = [own, "soakmark-b-1-bbbb"]
    text = f"clean reply carrying only {own}"
    assert detect_cross_talk(own, others, text) is False


def test_detect_cross_talk_false_when_no_markers_present() -> None:
    own = "soakmark-a-0-aaaa"
    others = [own, "soakmark-b-1-bbbb"]
    assert detect_cross_talk(own, others, "no markers at all") is False


def test_cache_reads_are_attributed_only_to_the_requested_generation_trace() -> None:
    observations: list[dict[str, object]] = [
        {"traceId": "ours", "type": "GENERATION", "usageDetails": {"input_cached_tokens": 27}},
        {"traceId": "ours", "type": "GENERATION", "usageDetails": {"cache_read_input_tokens": 3}},
        {"traceId": "foreign", "type": "GENERATION", "usageDetails": {"input_cached_tokens": 999}},
        {"traceId": "ours", "type": "SPAN", "usageDetails": {"input_cached_tokens": 999}},
    ]
    assert cache_reads_for_trace(observations, "ours") == 30
    assert cache_reads_for_trace(observations, "unobserved") == 0


@pytest.mark.parametrize("count", [-1, True, "27"])
def test_cache_probe_rejects_malformed_usage(count: object) -> None:
    with pytest.raises(AssertionError, match="nonnegative integer"):
        cache_reads_for_trace(
            [{
                "traceId": "ours", "type": "GENERATION",
                "usageDetails": {"input_cached_tokens": count},
            }],
            "ours",
        )


@pytest.mark.parametrize("runtime_mode", ["1", "true", "YES", "false", "0", ""])
def test_live_requirement_checks_effective_container_mode_before_forwarding(
    monkeypatch: pytest.MonkeyPatch, runtime_mode: str,
) -> None:
    import importlib.util
    import os
    import subprocess
    from dataclasses import replace

    from resilience_harness import ResilienceConfig

    spec = importlib.util.spec_from_file_location(
        "resilience_scenario_mode_test", Path(__file__).with_name("test_e2e_resilience.py"),
    )
    assert spec is not None and spec.loader is not None
    scenario = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(scenario)

    def external_kubectl(_cfg: object, *args: str) -> str:
        if args[0] == "get":
            # A Secret/envFrom-supplied bit has no direct pod .env[].value.
            return ""
        assert args[0] == "exec" and args[-2] == "-c"
        env = {**os.environ, "CURIE_FAKE_MODEL": runtime_mode,
               "PROVIDER_TOKEN": "test-private-sentinel"}
        result = subprocess.run(
            [sys.executable, "-c", args[-1]], env=env, capture_output=True, text=True, check=True,
        )
        assert "test-private-sentinel" not in result.stdout + result.stderr
        return result.stdout

    def external_forward(*_args: object) -> None:
        raise AssertionError("port-forward reached")

    monkeypatch.setattr(scenario, "kubectl", external_kubectl)
    monkeypatch.setattr(scenario, "port_forward", external_forward)
    expected = "fake-model" if runtime_mode.lower() in {"1", "true", "yes"} else "port-forward"
    with pytest.raises(AssertionError, match=expected):
        scenario._drive_turn(
            replace(ResilienceConfig.from_env(), live_model=True),
            "example-runner", 8000, "example", user="example", ts="1.0",
        )
