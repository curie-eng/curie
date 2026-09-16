"""Offline unit tests for the pure sandbox-resilience helpers.

These run with no cluster and no dev stack: they exercise the pure functions in
``resilience_harness.py`` (``thread_hash``, ``unique_marker``, ``final_frame``,
``collected_text``, ``detect_cross_talk``, ``pod_identity_gone``) plus the pod
read and wait helpers with ``kubectl`` stubbed out. They are deliberately not
gated by ``CURIE_SANDBOX_E2E`` so the harness logic stays covered in default CI
collection.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

import resilience_harness  # noqa: E402
from resilience_harness import (  # noqa: E402
    PodReadError,
    ResilienceConfig,
    collected_text,
    detect_cross_talk,
    final_frame,
    pod_identity_gone,
    read_pod,
    thread_hash,
    unique_marker,
    wait_pod_identity_gone,
)

OLD_UID = "11111111-1111-1111-1111-111111111111"
NEW_UID = "22222222-2222-2222-2222-222222222222"


def _pod(uid: str, name: str = "sbx-1") -> dict[str, object]:
    return {"metadata": {"name": name, "uid": uid}}


def _cfg() -> ResilienceConfig:
    return ResilienceConfig(
        namespace="test-ns",
        pool="test-pool",
        valkey_host="localhost",
        valkey_port=26379,
        valkey_password=None,
        concurrency=1,
        batch=1,
        runs=1,
        live_creds=False,
        history_base="http://history.test/t",
    )


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


# -- pod identity: the #2743 regression surface ------------------------------


def test_pod_identity_gone_when_name_resolves_to_nothing() -> None:
    assert pod_identity_gone(None, OLD_UID) is True


def test_pod_identity_gone_on_same_name_replacement() -> None:
    """The controller recreating the pod under the same name is a disappearance."""

    assert pod_identity_gone(_pod(NEW_UID), OLD_UID) is True


def test_pod_identity_not_gone_while_original_uid_present() -> None:
    assert pod_identity_gone(_pod(OLD_UID), OLD_UID) is False


def test_read_pod_returns_none_only_on_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_kubectl(cfg: object, *args: str) -> str:
        raise subprocess.CalledProcessError(
            1, args, stderr='Error from server (NotFound): pods "sbx-1" not found'
        )

    monkeypatch.setattr(resilience_harness, "kubectl", fake_kubectl)
    assert read_pod(_cfg(), "sbx-1") is None


@pytest.mark.parametrize(
    "stderr",
    [
        'Error from server (Forbidden): pods "sbx-1" is forbidden',
        "Unable to connect to the server: dial tcp 10.0.0.1:6443: i/o timeout",
        "error: You must be logged in to the server (Unauthorized)",
    ],
)
def test_read_pod_raises_on_authorization_and_transport_errors(
    monkeypatch: pytest.MonkeyPatch, stderr: str
) -> None:
    """A read that did not answer the question must never count as a deletion."""

    def fake_kubectl(cfg: object, *args: str) -> str:
        raise subprocess.CalledProcessError(1, args, stderr=stderr)

    monkeypatch.setattr(resilience_harness, "kubectl", fake_kubectl)
    with pytest.raises(PodReadError):
        read_pod(_cfg(), "sbx-1")


def test_read_pod_raises_on_subprocess_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_kubectl(cfg: object, *args: str) -> str:
        raise subprocess.TimeoutExpired(args, 60)

    monkeypatch.setattr(resilience_harness, "kubectl", fake_kubectl)
    with pytest.raises(PodReadError):
        read_pod(_cfg(), "sbx-1")


@pytest.mark.parametrize("body", ["not json at all", "[]", '{"items": []}'])
def test_read_pod_raises_on_malformed_response(
    monkeypatch: pytest.MonkeyPatch, body: str
) -> None:
    monkeypatch.setattr(resilience_harness, "kubectl", lambda cfg, *args: body)
    with pytest.raises(PodReadError):
        read_pod(_cfg(), "sbx-1")


def test_read_pod_parses_a_pod_body(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        resilience_harness, "kubectl", lambda cfg, *args: json.dumps(_pod(OLD_UID))
    )
    assert read_pod(_cfg(), "sbx-1") == _pod(OLD_UID)


def test_wait_returns_replacement_on_same_name_recreation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The v0.9.0 failure: same name, new UID, and the old wait hung until timeout."""

    reads = [_pod(OLD_UID), _pod(OLD_UID), _pod(NEW_UID)]
    monkeypatch.setattr(
        resilience_harness, "read_pod", lambda cfg, name: reads.pop(0)
    )
    ticks = iter([0.0, 1.0, 2.0, 3.0, 4.0, 5.0])
    result = wait_pod_identity_gone(
        _cfg(),
        "sbx-1",
        OLD_UID,
        timeout=30.0,
        sleep=lambda _s: None,
        clock=lambda: next(ticks),
    )
    assert result is not None
    assert result["metadata"]["uid"] == NEW_UID  # type: ignore[index]
    assert reads == []


def test_wait_returns_none_when_the_name_goes_away(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reads: list[object] = [_pod(OLD_UID), None]
    monkeypatch.setattr(
        resilience_harness, "read_pod", lambda cfg, name: reads.pop(0)
    )
    ticks = iter([0.0, 1.0, 2.0, 3.0])
    assert (
        wait_pod_identity_gone(
            _cfg(),
            "sbx-1",
            OLD_UID,
            timeout=30.0,
            sleep=lambda _s: None,
            clock=lambda: next(ticks),
        )
        is None
    )


def test_wait_times_out_while_the_original_uid_is_still_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An undeleted pod must still fail; the UID awareness does not soften the gate."""

    monkeypatch.setattr(
        resilience_harness, "read_pod", lambda cfg, name: _pod(OLD_UID)
    )
    ticks = iter([0.0, 1.0, 5.0, 40.0])
    with pytest.raises(AssertionError, match="still carries uid"):
        wait_pod_identity_gone(
            _cfg(),
            "sbx-1",
            OLD_UID,
            timeout=30.0,
            sleep=lambda _s: None,
            clock=lambda: next(ticks),
        )


def test_wait_propagates_a_failed_read(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(cfg: object, name: str) -> object:
        raise PodReadError("forbidden")

    monkeypatch.setattr(resilience_harness, "read_pod", boom)
    with pytest.raises(PodReadError):
        wait_pod_identity_gone(
            _cfg(),
            "sbx-1",
            OLD_UID,
            timeout=30.0,
            sleep=lambda _s: None,
            clock=lambda: 0.0,
        )


# -- readiness: the recovery turn's precondition ------------------------------


def _ready_pod(uid: str, status: str = "True") -> dict[str, object]:
    return {
        "metadata": {"name": "sbx-1", "uid": uid},
        "status": {"conditions": [{"type": "Initialized", "status": "True"},
                                  {"type": "Ready", "status": status}]},
    }


def test_pod_ready_reads_the_ready_condition() -> None:
    assert resilience_harness.pod_ready(_ready_pod(NEW_UID)) is True
    assert resilience_harness.pod_ready(_ready_pod(NEW_UID, status="False")) is False


@pytest.mark.parametrize("pod", [None, {"metadata": {}}, {"status": {}},
                                 {"status": {"conditions": []}}])
def test_pod_ready_false_without_a_ready_condition(pod: object) -> None:
    assert resilience_harness.pod_ready(pod) is False  # type: ignore[arg-type]


def test_wait_pod_ready_returns_the_ready_pod(monkeypatch: pytest.MonkeyPatch) -> None:
    reads: list[object] = [_ready_pod(NEW_UID, "False"), _ready_pod(NEW_UID)]
    monkeypatch.setattr(resilience_harness, "read_pod", lambda cfg, name: reads.pop(0))
    ticks = iter([0.0, 1.0, 2.0, 3.0])
    pod = resilience_harness.wait_pod_ready(
        _cfg(), "sbx-1", timeout=30.0, sleep=lambda _s: None, clock=lambda: next(ticks)
    )
    assert pod["metadata"]["uid"] == NEW_UID  # type: ignore[index]


def test_wait_pod_ready_fails_when_readiness_never_arrives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A never-ready pod must fail; readiness is asserted, not assumed."""

    monkeypatch.setattr(
        resilience_harness, "read_pod", lambda cfg, name: _ready_pod(NEW_UID, "False")
    )
    ticks = iter([0.0, 1.0, 5.0, 40.0])
    with pytest.raises(AssertionError, match="was not Ready"):
        resilience_harness.wait_pod_ready(
            _cfg(), "sbx-1", timeout=30.0, sleep=lambda _s: None, clock=lambda: next(ticks)
        )


def test_wait_pod_ready_propagates_a_failed_read(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(cfg: object, name: str) -> object:
        raise PodReadError("forbidden")

    monkeypatch.setattr(resilience_harness, "read_pod", boom)
    with pytest.raises(PodReadError):
        resilience_harness.wait_pod_ready(
            _cfg(), "sbx-1", timeout=30.0, sleep=lambda _s: None, clock=lambda: 0.0
        )


# -- ACI turn authentication --------------------------------------------------


def _captured_post_event(monkeypatch: pytest.MonkeyPatch, token: str) -> dict[str, str]:
    seen: dict[str, str] = {}

    class _Resp:
        def __enter__(self) -> _Resp:
            return self

        def __exit__(self, *_a: object) -> None:
            return None

        def read(self) -> bytes:
            return b'{"type": "final", "text": "ok"}'

    def fake_urlopen(request: object, timeout: float = 0) -> object:
        seen.update(request.headers)  # type: ignore[attr-defined]
        return _Resp()

    monkeypatch.setattr(resilience_harness.urllib.request, "urlopen", fake_urlopen)
    frames = resilience_harness.post_event("http://sbx", "hi", token=token)
    assert frames == [{"type": "final", "text": "ok"}]
    return seen


def test_post_event_sends_the_claim_bearer(monkeypatch: pytest.MonkeyPatch) -> None:
    """A resumed claim mints a runner token; an unauthenticated turn gets a 401."""

    headers = _captured_post_event(monkeypatch, "tok-abc")
    assert headers["Authorization"] == "Bearer tok-abc"


def test_post_event_sends_no_bearer_for_a_tokenless_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    headers = _captured_post_event(monkeypatch, "")
    assert "Authorization" not in headers
