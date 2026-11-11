"""Owner-scoped enrollment on the exact candidate (#5377 AC 5), gated by CURIE_PERSONAL_LIVE=1.

The caller supplies the scratch Docker context, the throwaway Kubernetes context,
a fresh namespace, the source-built CLI (CURIE_BIN), the address of this host the
cluster's pods can reach (CURIE_PERSONAL_RUNNER_API_HOST) and an evidence
directory outside the checkout. CURIE_PERSONAL_PROOF_PHASE=synthetic exercises
every unpaid observation with no grant, no provider call and no model session.
The default live phase also reads the grant (SUBSCRIPTION_TEST_GRANT_FILE) and a
usable service credential (CURIE_PERSONAL_SERVICE_CREDENTIAL_FILE) from private
0600 files, and needs a stable CURIE_PERSONAL_PROOF_RUN_ID for the durable
four-session ledger. Run it from a filtered shell (``env -i``, stdin from
/dev/null); nothing paid is retried.

The offline tests below cover the driver's own guards and run everywhere.
"""

from __future__ import annotations

import contextlib
import json
import os
import socket
import stat
import sys
import threading
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest

# importlib import mode does not add this test directory to sys.path.
sys.path.insert(0, str(Path(__file__).parent))

import personal_enrollment_cluster_support as support  # noqa: E402
from personal_enrollment_cluster_support import (  # noqa: E402
    ROOT,
    EnrollmentLedger,
    EnrollmentProof,
    ProofCheckError,
    TcpRelay,
    anonymize,
    leaks,
    oidc_harness,
    read_private_input,
    validate_inputs,
    wait_until,
)

__tracebackhide__ = True

_LIVE_INPUTS = (
    "SUBSCRIPTION_TEST_GRANT_FILE",
    "CURIE_PERSONAL_SERVICE_CREDENTIAL_FILE",
    "CURIE_PERSONAL_PROOF_RUN_ID",
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_BASE_URL",
)


def _inputs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for name in ("CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY", "CURIE_CREDENTIALS",
                 *_LIVE_INPUTS):
        monkeypatch.delenv(name, raising=False)
    values = {
        "DOCKER_CONTEXT": "scratch",
        "KUBECONFIG": str(tmp_path / "kubeconfig"),
        "THROWAWAY_CONTEXT": "throwaway",
        "CURIE_PERSONAL_TEST_NAMESPACE": "test-personal-enrollment-e2e",
        "CURIE_BIN": str(tmp_path / "curie"),
        "CURIE_PERSONAL_RUNNER_API_HOST": "192.0.2.10",
        "CURIE_PERSONAL_EVIDENCE_DIR": str(tmp_path / "evidence"),
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def _private(path: Path, content: bytes, mode: int = 0o600) -> Path:
    path.write_bytes(content)
    path.chmod(mode)
    return path


def test_ledger_admits_four_sessions_with_one_unpaid_and_refuses_a_fifth(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CURIE_PERSONAL_PROOF_STATE_DIR", str(tmp_path))
    ledger = EnrollmentLedger("enrollment-four")
    try:
        assert ledger.start("candidate") == (1, 1024)
        assert ledger.finish(1, 40, 1, 12)["sessions"] == 1
        assert ledger.start("candidate") == (2, 1024)
        refused = ledger.finish(2, 0, 0, 3)
        assert refused["output_tokens"] == 0 and refused["assistant_turns"] == 0
        assert ledger.start("candidate") == (3, 1024)
        assert ledger.finish(3, 300, 3, 150)["accounted_output_tokens"] == 340
        assert ledger.start("candidate") == (4, 1024)
        usage = ledger.finish(4, 20, 1, 9)
        assert usage == {
            "sessions": 4, "assistant_turns": 1, "output_tokens": 20,
            "accounted_output_tokens": 360, "unknown_reserved_output_tokens": 0,
            "elapsed_seconds": 9,
        }
        with pytest.raises(ProofCheckError, match="4-session"):
            ledger.start("candidate")
    finally:
        ledger.db.close()


def test_ledger_keeps_an_unsettled_session_reserved_across_processes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CURIE_PERSONAL_PROOF_STATE_DIR", str(tmp_path))
    first = EnrollmentLedger("enrollment-crash")
    assert first.start("candidate") == (1, 1024)
    first.db.close()
    second = EnrollmentLedger("enrollment-crash")
    try:
        assert second.start("candidate") == (2, 1024)
        usage = second.finish(2, 1000, 4, 100)
        assert usage["unknown_reserved_output_tokens"] == 1024
        assert usage["accounted_output_tokens"] == 2024
        assert second.start("candidate") == (3, 1024)
        second.finish(3, 1024, 4, 100)
        assert second.remaining() == 1024
        assert second.start("candidate") == (4, 1024)
        with pytest.raises(ProofCheckError, match="output allowance"):
            second.finish(4, 1025, 1, 1)
    finally:
        second.db.close()


@pytest.mark.parametrize(
    ("tokens", "turns", "elapsed", "message"),
    [
        (1025, 1, 1, "output allowance"),
        (10, 5, 1, "session bound"),
        (10, 1, 181, "session bound"),
    ],
)
def test_ledger_refuses_a_session_over_its_bounds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    tokens: int, turns: int, elapsed: float, message: str,
) -> None:
    monkeypatch.setenv("CURIE_PERSONAL_PROOF_STATE_DIR", str(tmp_path))
    ledger = EnrollmentLedger("enrollment-bounds")
    try:
        sequence, _ = ledger.start("candidate")
        with pytest.raises(ProofCheckError, match=message):
            ledger.finish(sequence, tokens, turns, elapsed)
    finally:
        ledger.db.close()


def test_private_input_is_read_without_its_trailing_newline(tmp_path: Path) -> None:
    path = _private(tmp_path / "grant", b"sk-ant-oat01-example\n")
    assert read_private_input(str(path), "grant input").reveal() == "sk-ant-oat01-example"
    windows = _private(tmp_path / "grant-crlf", b"sk-ant-oat01-example\r\n")
    assert read_private_input(str(windows), "grant input").reveal() == "sk-ant-oat01-example"


@pytest.mark.parametrize(
    ("content", "mode", "message"),
    [
        (b"token\n", 0o644, "mode 0600"),
        (b"", 0o600, "empty or malformed"),
        (b"\n", 0o600, "empty or malformed"),
        (b"two words\n", 0o600, "empty or malformed"),
        (b"token\n\n", 0o600, "empty or malformed"),
        ("töken".encode(), 0o600, "printable ASCII"),
    ],
)
def test_private_input_refuses_unsafe_or_malformed_files(
    tmp_path: Path, content: bytes, mode: int, message: str,
) -> None:
    path = _private(tmp_path / "grant", content, mode)
    with pytest.raises(ProofCheckError, match=message):
        read_private_input(str(path), "grant input")


def test_private_input_refuses_relative_missing_and_linked_paths(tmp_path: Path) -> None:
    with pytest.raises(ProofCheckError, match="absolute"):
        read_private_input("grant", "grant input")
    with pytest.raises(ProofCheckError, match="unavailable"):
        read_private_input(str(tmp_path / "absent"), "grant input")
    target = _private(tmp_path / "target", b"token\n")
    link = tmp_path / "link"
    link.symlink_to(target)
    with pytest.raises(ProofCheckError, match="regular file"):
        read_private_input(str(link), "grant input")
    assert stat.S_IMODE(target.stat().st_mode) == 0o600


def test_synthetic_inputs_are_accepted_and_name_the_grant_namespace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _inputs(monkeypatch, tmp_path)
    assert validate_inputs("synthetic") == {
        "namespace": "test-personal-enrollment-e2e",
        "grant_namespace": "test-personal-enrollment-e2e-grants",
    }


def test_live_phase_names_every_missing_private_input(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _inputs(monkeypatch, tmp_path)
    with pytest.raises(ProofCheckError) as refused:
        validate_inputs("live")
    for name in _LIVE_INPUTS:
        assert name in str(refused.value)


def test_live_proof_refuses_to_start_without_its_service_credential(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _inputs(monkeypatch, tmp_path)
    grant = _private(tmp_path / "grant", b"sk-ant-oat01-example\n")
    monkeypatch.setenv("SUBSCRIPTION_TEST_GRANT_FILE", str(grant))
    monkeypatch.setenv("CURIE_PERSONAL_PROOF_RUN_ID", "enrollment-run")
    monkeypatch.setenv("CURIE_PERSONAL_SERVICE_CREDENTIAL_FILE", str(tmp_path / "absent"))
    monkeypatch.setenv("ANTHROPIC_API_KEY", "service-placeholder")
    monkeypatch.setenv("ANTHROPIC_BASE_URL", "https://model.example.com")
    with pytest.raises(ProofCheckError, match="service credential input is unavailable"):
        EnrollmentProof("live")


def test_live_installation_inputs_are_held_without_reaching_child_environments(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _inputs(monkeypatch, tmp_path)
    grant = _private(tmp_path / "grant", b"sk-ant-oat01-example\n")
    service = _private(tmp_path / "service", b"service-placeholder\n")
    for name, value in {
        "SUBSCRIPTION_TEST_GRANT_FILE": str(grant),
        "CURIE_PERSONAL_SERVICE_CREDENTIAL_FILE": str(service),
        "CURIE_PERSONAL_PROOF_RUN_ID": "enrollment-run",
        "ANTHROPIC_API_KEY": "service-placeholder",
        "ANTHROPIC_BASE_URL": "https://model.example.com",
    }.items():
        monkeypatch.setenv(name, value)
    proof = EnrollmentProof("live")
    try:
        assert proof.service.reveal() == "service-placeholder"
        assert proof.service_url.reveal() == "https://model.example.com"
        assert "ANTHROPIC_API_KEY" not in proof.env
        assert "ANTHROPIC_BASE_URL" not in proof.env
        assert "ANTHROPIC_API_KEY" not in os.environ
        assert "ANTHROPIC_BASE_URL" not in os.environ
    finally:
        for held in proof.held():
            held.clear()


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("CLAUDE_CODE_OAUTH_TOKEN", "x", "model credential"),
        ("ANTHROPIC_API_KEY", "x", "model credential"),
        ("CURIE_PERSONAL_TEST_NAMESPACE", "Not_A_Label", "DNS-label"),
        ("CURIE_PERSONAL_TEST_NAMESPACE", "n" * 43, "DNS-label"),
        ("CURIE_PERSONAL_EVIDENCE_DIR", "relative/evidence", "absolute"),
        ("CURIE_PERSONAL_EVIDENCE_DIR", str(ROOT / "evidence"), "outside the checkout"),
        ("CURIE_PERSONAL_RUNNER_API_HOST", "127.0.0.1", "reachable from the cluster"),
        ("CURIE_PERSONAL_RUNNER_API_HOST", "runner.example.com", "IPv4"),
    ],
)
def test_unsafe_inputs_are_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str, value: str, message: str,
) -> None:
    _inputs(monkeypatch, tmp_path)
    monkeypatch.setenv(name, value)
    with pytest.raises(ProofCheckError, match=message):
        validate_inputs("synthetic")


def test_unknown_phase_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _inputs(monkeypatch, tmp_path)
    with pytest.raises(ProofCheckError, match="synthetic or live"):
        validate_inputs("paid")


def test_leaks_finds_raw_and_encoded_values() -> None:
    import base64

    held = "sk-ant-oat01-private-example"
    assert leaks(b"prefix " + held.encode(), [held])
    assert leaks(base64.b64encode(held.encode()).rstrip(b"="), [held])
    assert leaks(base64.urlsafe_b64encode(held.encode()), [held])
    assert not leaks(b"public output", [held, ""])


def test_anonymize_replaces_longest_targets_first() -> None:
    data = json.dumps(
        {"ns": "test-ns-grants", "base": "test-ns", "path": "/private/run/session"}
    ).encode()
    assert json.loads(anonymize(data, {
        "test-ns": "<namespace>", "test-ns-grants": "<grant-namespace>", "/private/run": "<p>",
    })) == {"ns": "<grant-namespace>", "base": "<namespace>", "path": "<p>/session"}


def test_wait_until_returns_the_first_value_and_fails_at_its_deadline() -> None:
    answers = iter([None, None, "ready"])
    assert wait_until(lambda: next(answers), timeout=5, interval=0.001, failure="x") == "ready"
    with pytest.raises(ProofCheckError, match="never ready"):
        wait_until(lambda: None, timeout=0.01, interval=0.001, failure="never ready")


def test_oidc_harness_loads_by_path_with_its_pinned_dex_image() -> None:
    harness = oidc_harness()
    assert harness.DEX_IMAGE.startswith("ghcr.io/dexidp/dex:") and "@sha256:" in harness.DEX_IMAGE
    assert callable(harness.request)


def test_relay_forwards_both_directions_and_stops() -> None:
    with socket.create_server(("127.0.0.1", 0)) as server:
        def echo() -> None:
            connection, _ = server.accept()
            with connection:
                connection.sendall(connection.recv(64).upper())

        thread = threading.Thread(target=echo, daemon=True)
        thread.start()
        relay = TcpRelay("127.0.0.1", int(server.getsockname()[1]))
        try:
            with socket.create_connection(("127.0.0.1", relay.port), timeout=5) as client:
                client.sendall(b"relay")
                assert client.recv(64) == b"RELAY"
        finally:
            relay.close()
        thread.join(timeout=5)
        assert not relay.alive


@pytest.fixture
def live_driver(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> EnrollmentProof:
    _inputs(monkeypatch, tmp_path)
    proof = EnrollmentProof("synthetic")
    proof.candidate = "candidate"
    proof.s3 = SimpleNamespace(pod="owned-pod", runner_token=support.HeldSecret("runner"))
    proof.ledger = SimpleNamespace(remaining=lambda: 1024, start=lambda _: (3, 1024))
    proof.headers_seen["restored"] = {"version": 3}
    monkeypatch.setattr(proof, "forward", lambda _: contextlib.nullcontext(1234))
    monkeypatch.setattr(proof, "observe_files", lambda _: {})
    monkeypatch.setattr(proof, "snapshot_replay", lambda _: None)
    monkeypatch.setattr(proof, "retain_transcript", lambda *args, **kwargs: None)
    monkeypatch.setattr(support, "live_transcript_identity", lambda _: ({"new"}, {support.MODEL}))
    monkeypatch.setattr(proof, "capture_pod", lambda _: None)
    monkeypatch.setattr(proof, "account", lambda *args, **kwargs: {})
    return proof


@pytest.mark.parametrize("outcome", ["interrupted", "eof", "error", "done", "inactive"])
def test_disconnect_requires_active_work_and_propagates_turn_errors(
    live_driver: EnrollmentProof, monkeypatch: pytest.MonkeyPatch, outcome: str,
) -> None:
    released = threading.Event()
    finished = threading.Event()
    writes: list[str] = []
    calls = 0

    def stream(
        *args: object, **kwargs: object,
    ) -> tuple[list[dict[str, object]], bytes, float, bool]:
        nonlocal calls
        calls += 1
        if calls == 1:
            return [{"type": "final", "status": "done", "text": live_driver.marker}], b"", 1, False
        callback = kwargs["on_event"]
        callback({"type": "tool_note"})
        if outcome != "inactive":
            assert released.wait(5), "disconnect never released the running turn"
        finished.set()
        if outcome == "error":
            raise RuntimeError("unexpected thread failure")
        if outcome in {"eof", "inactive"}:
            return [], b"", 1, True
        return [{"type": "final", "status": (
            "done" if outcome == "done" else "idle-awaiting-input"
        )}], b"", 1, False

    def write(action: str, **kwargs: object) -> dict[str, int]:
        writes.append(action)
        released.set()
        return {"version": 4}

    monkeypatch.setattr(live_driver, "stream", stream)
    monkeypatch.setattr(live_driver, "write", write)
    if outcome == "inactive":
        monkeypatch.setattr(live_driver, "observe_files", lambda _: finished.wait(5) or {})
    try:
        if outcome in {"interrupted", "eof"}:
            assert live_driver.live_disconnect_during_work() == {"version": 4}
        else:
            with pytest.raises(ProofCheckError):
                live_driver.live_disconnect_during_work()
        assert writes == ([] if outcome == "inactive" else ["disconnect"])
    finally:
        released.set()


@pytest.mark.parametrize("elapsed", [(100, 81), (100, 20)])
def test_session_deadline_spans_turns_and_kills_the_runner(
    live_driver: EnrollmentProof, monkeypatch: pytest.MonkeyPatch, elapsed: tuple[int, int],
) -> None:
    clock = [0.0]
    timers: list[object] = []
    stopped: list[str] = []

    class Timer:
        def __init__(self, seconds: float, callback: object) -> None:
            self.deadline = clock[0] + seconds
            self.callback = callback
            self.cancelled = False
            timers.append(self)

        def start(self) -> None:
            pass

        def cancel(self) -> None:
            self.cancelled = True

        def join(self) -> None:
            pass

    class Connection:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        def request(self, method: str, path: str, **kwargs: object) -> None:
            assert (method, path) == ("POST", "/v1/interrupt")
            stopped.append("interrupt")

        def getresponse(self) -> object:
            return SimpleNamespace(status=200, close=lambda: None)

        def close(self) -> None:
            pass

    def delete(pod: str, namespace: str, **kwargs: object) -> None:
        assert pod == "owned-pod" and kwargs["grace_period_seconds"] == 0
        stopped.append("delete")

    durations = iter(elapsed)

    def stream(*args: object, **kwargs: object) -> tuple[list[dict[str, object]], bytes, int, bool]:
        duration = next(durations)
        clock[0] += duration
        for timer in timers:
            if not timer.cancelled and clock[0] >= timer.deadline:
                timer.callback()
        return [{"type": "final", "status": "authentication-required"}], b"", duration, False

    live_driver.core = SimpleNamespace(delete_namespaced_pod=delete)
    monkeypatch.setattr(support.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(support.threading, "Timer", Timer)
    monkeypatch.setattr(support.http.client, "HTTPConnection", Connection)
    monkeypatch.setattr(live_driver, "stream", stream)
    if sum(elapsed) > 180:
        with pytest.raises(ProofCheckError, match="absolute deadline"):
            live_driver.live_session(live_driver.s3, "deadline", ["first", "second"],
                                     expect="authentication-required")
        assert stopped == ["interrupt", "delete"]
    else:
        live_driver.live_session(live_driver.s3, "deadline", ["first", "second"],
                                 expect="authentication-required")
        assert stopped == []


@pytest.mark.parametrize("leaked", [True, False])
def test_cleanup_checks_evidence_before_clearing_private_values(
    live_driver: EnrollmentProof, monkeypatch: pytest.MonkeyPatch, leaked: bool,
) -> None:
    sentinel = live_driver.grant.reveal()
    live_driver.evidence["sentinel"] = sentinel if leaked else "public outcome"

    def parent_cleanup(proof: EnrollmentProof) -> None:
        proof.grant.clear()
        proof.runner_auth.clear()
        proof.progress_auth.clear()

    monkeypatch.setattr(support.ClusterProof, "cleanup", parent_cleanup)
    if leaked:
        with pytest.raises(ProofCheckError, match="evidence carried a private value"):
            live_driver.cleanup()
    else:
        live_driver.cleanup()
    path = live_driver.evidence_dir / "personal-enrollment-run.json"
    document = json.loads(path.read_bytes())
    assert sentinel not in path.read_text()
    if leaked:
        assert document == {"withheld": "evidence carried a private value"}
    else:
        assert document["sentinel"] == "public outcome"
    assert all(not secret.reveal() for secret in live_driver.held())


def test_evidence_refuses_an_empty_private_value_list(live_driver: EnrollmentProof) -> None:
    for secret in live_driver.held():
        secret.clear()
    with pytest.raises(ProofCheckError, match="no private values"):
        live_driver.write_evidence()


@pytest.mark.parametrize("leaked", [False, True])
def test_retained_transcript_metadata_is_covered_by_the_secret_audit(
    live_driver: EnrollmentProof, leaked: bool,
) -> None:
    sentinel = live_driver.grant.reveal()
    live_driver.evidence["transcript_entries"] = [{
        "session": 1, "step": "a", "turn": 1,
        "entries": [{"index": 0, "turn": 1, "provenance": "new-response",
                     "model": sentinel if leaked else support.MODEL,
                     "usage": {"output_tokens": 3}}],
    }]
    if leaked:
        with pytest.raises(ProofCheckError, match="evidence carried a private value"):
            live_driver.write_evidence()
    else:
        live_driver.write_evidence()
    path = live_driver.evidence_dir / "personal-enrollment-transcript.json"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert sentinel not in path.read_text()
    document = json.loads(path.read_text())
    if leaked:
        assert document == {"withheld": "evidence carried a private value"}
    else:
        assert document["observations"] == live_driver.evidence["transcript_entries"]


def test_missing_grant_control_keeps_service_material_out_of_the_runner(
    live_driver: EnrollmentProof, monkeypatch: pytest.MonkeyPatch,
) -> None:
    live_driver.s4 = SimpleNamespace(claim_name="personal-claim")
    live_driver.core = SimpleNamespace(read_namespaced_secret=lambda *args, **kwargs: (
        SimpleNamespace(data={})
    ))
    monkeypatch.setattr(live_driver, "secret", lambda *args, **kwargs: None)
    monkeypatch.setattr(live_driver, "exec", lambda *args: b"[]")
    monkeypatch.setattr(live_driver, "capture_pod", lambda *args: None)
    monkeypatch.setattr(live_driver, "observe_files", lambda *args: {
        "provider_attempts": 0, "assistant_messages": [], "usage": [],
    })

    def create_pod(name: str, spec: dict[str, object]) -> tuple[str, dict[str, object]]:
        for container in [*spec["containers"], *spec["initContainers"]]:
            assert not container.get("envFrom")
            assert all(
                entry.get("valueFrom", {}).get("secretKeyRef", {}).get("name")
                != "installation-model"
                for entry in container.get("env", [])
            )
        return name, {}

    def stream(
        port: int, token: object, prompt: str, *, deadline: float,
    ) -> tuple[list[dict[str, str]], bytes, int, bool]:
        assert deadline > support.time.monotonic()
        return [
            {"type": "error", "message": "Authentication required"},
            {"type": "final", "status": "classified-failure"},
        ], b"", 0, False

    monkeypatch.setattr(live_driver, "create_pod", create_pod)
    monkeypatch.setattr(live_driver, "stream", stream)
    live_driver.step_f()
    assert live_driver.evidence["missing_grant_service_present"]["sessions_consumed"] == 0


def test_service_installation_template_has_unique_environment_names(
    live_driver: EnrollmentProof,
) -> None:
    # The Kubernetes API returned 422 FieldValueDuplicate for repeated env names
    # in the SandboxTemplate's map-list; each container needs unique name keys.
    objects: dict[tuple[str, str], dict] = {}

    def create(group: str, version: str, namespace: str, plural: str, body: dict,
               **kwargs: object) -> dict:
        objects[plural, body["metadata"]["name"]] = body
        return body

    def get(group: str, version: str, namespace: str, plural: str, name: str,
            **kwargs: object) -> dict:
        return objects[plural, name]

    live_driver.custom = SimpleNamespace(
        create_namespaced_custom_object=create, get_namespaced_custom_object=get,
    )
    live_driver.core = SimpleNamespace(
        create_namespaced_secret=lambda *args, **kwargs: None,
        create_namespaced_config_map=lambda *args, **kwargs: None,
        patch_namespaced_secret=lambda *args, **kwargs: None,
    )
    live_driver.create_inputs()
    pod = live_driver.source["podTemplate"]["spec"]
    for container in [*pod["containers"], *pod["initContainers"]]:
        names = [entry["name"] for entry in container["env"]]
        assert len(names) == len(set(names))
        assert {"ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL"} <= set(names)


@pytest.fixture
def enrollment_cluster(request: pytest.FixtureRequest) -> Iterator[EnrollmentProof]:
    __tracebackhide__ = True
    if os.environ.get("CURIE_PERSONAL_LIVE") != "1":
        pytest.skip("set CURIE_PERSONAL_LIVE=1 for the owned enrollment proof")
    phase = os.environ.get("CURIE_PERSONAL_PROOF_PHASE", "live")
    proof = EnrollmentProof(phase)

    def finish() -> None:
        __tracebackhide__ = True
        try:
            proof.cleanup()
        finally:
            reporter = request.config.pluginmanager.get_plugin("terminalreporter")
            if reporter is not None:
                reporter.write_line(json.dumps({
                    "stage": proof.evidence.get("stage"),
                    "failure": proof.evidence.get("failure"),
                    "cleanup": proof.evidence.get("cleanup"),
                }, sort_keys=True))

    request.addfinalizer(finish)
    try:
        proof.setup()
    except Exception as exc:  # noqa: BLE001 - provider errors must not expose private input
        proof.evidence["failure"] = (
            str(exc) if isinstance(exc, ProofCheckError) else type(exc).__name__
        )
        proof.retain_failure(exc)
        raise RuntimeError(
            "personal enrollment proof setup failed; private details withheld"
        ) from None
    yield proof


def test_owner_enrollment_lifecycle_on_exact_candidate(
    enrollment_cluster: EnrollmentProof,
) -> None:
    __tracebackhide__ = True
    try:
        enrollment_cluster.run()
    except Exception as exc:  # noqa: BLE001 - provider errors must not expose private input
        enrollment_cluster.evidence["failure"] = (
            str(exc) if isinstance(exc, ProofCheckError) else type(exc).__name__
        )
        enrollment_cluster.retain_failure(exc)
        raise AssertionError(
            "personal enrollment proof failed; private details withheld"
        ) from None
