"""Failure contracts for the disposable scripted factory driver.

These tests exercise driver validation and lifecycle bookkeeping. The real
kind scenario remains the proof that the product completes an issue.
"""

from __future__ import annotations

import fcntl
import json
import socket
import sqlite3
import subprocess
import threading
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import factory_e2e as fe
import pytest
import scripted_kind as sk

REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture(autouse=True)
def owned_scratch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    mkdtemp = sk.tempfile.mkdtemp
    monkeypatch.setattr(
        sk.tempfile, "mkdtemp", lambda *, prefix: mkdtemp(prefix=prefix, dir=scratch)
    )
    return scratch


def _refuse_external_command(*_args: Any, **_kwargs: Any) -> Any:
    raise AssertionError("configuration must refuse before an external command")


def test_missing_replay_transcript_refuses_before_resource_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    missing = tmp_path / "missing-transcript.json"
    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sk.subprocess, "run", _refuse_external_command)
    args = fe.parse_args(["scripted", "--transcript", str(missing)])

    assert sk.main(args) == fe.EXIT_CONFIG
    assert "transcript" in capsys.readouterr().err.lower()


def test_record_without_upstream_key_refuses_before_resource_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("CURIE_FACTORY_RECORD_KEY", raising=False)
    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sk.subprocess, "run", _refuse_external_command)
    args = fe.parse_args(["scripted", "--record", "--transcript", str(tmp_path / "record.json")])

    assert sk.main(args) == fe.EXIT_CONFIG
    assert "CURIE_FACTORY_RECORD_KEY" in capsys.readouterr().err


@pytest.mark.parametrize(
    "content",
    [
        '{"version": 1, "exchanges": [',
        json.dumps({"version": 1, "exchanges": [{"lane_key": "add Kelvin tests", "ordinal": 0}]}),
    ],
    ids=["invalid-json", "exchange-without-message"],
)
def test_malformed_replay_transcript_refuses_before_resource_mutation(
    content: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    malformed = tmp_path / "malformed-transcript.json"
    malformed.write_text(content)
    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sk.subprocess, "run", _refuse_external_command)
    args = fe.parse_args(["scripted", "--transcript", str(malformed)])

    assert sk.main(args) == fe.EXIT_CONFIG
    assert "transcript" in capsys.readouterr().err.lower()


def test_well_formed_replay_transcript_passes_configuration(tmp_path: Path) -> None:
    transcript = tmp_path / "transcript.json"
    transcript.write_text(
        json.dumps(
            {
                "version": 1,
                "exchanges": [
                    {
                        "lane_key": "add Kelvin tests",
                        "ordinal": 0,
                        "message": {"type": "message", "content": []},
                    }
                ],
            }
        )
    )
    args = fe.parse_args(["scripted", "--transcript", str(transcript)])

    sk.validate_configuration(args)


def _replay_args(tmp_path: Path) -> Any:
    transcript = tmp_path / "transcript.json"
    transcript.write_text(json.dumps({"version": 1, "exchanges": []}))
    return fe.parse_args(["scripted", "--transcript", str(transcript)])


def _evidence(tmp_path: Path) -> dict[str, Any]:
    files = list((tmp_path / "target/factory-e2e").glob("scripted-*.json"))
    assert len(files) == 1
    return json.loads(files[0].read_text())


@pytest.mark.parametrize("failure_stage", ["registry", "cluster"])
def test_partial_resource_creation_failure_deletes_every_attempted_owned_resource(
    failure_stage: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A command can fail after creating its resource, so deletion must follow."""

    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sk.ScriptedRun, "prerequisites", lambda _self: None)
    calls: list[tuple[str, ...]] = []

    def command(argv: tuple[str, ...], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        output = ""
        if argv[:2] == ("docker", "run"):
            if failure_stage == "registry":
                return subprocess.CompletedProcess(argv, 1, "", "connection lost after create")
        elif argv[:2] == ("docker", "inspect"):
            output = json.dumps(
                [{"NetworkSettings": {"Ports": {"5000/tcp": [{"HostPort": "35000"}]}}}]
            )
        elif argv[:3] == ("kind", "create", "cluster"):
            return subprocess.CompletedProcess(argv, 1, "", "connection lost after create")
        elif argv[:3] not in {
            ("kind", "delete", "cluster"),
            ("kind", "get", "clusters"),
            ("docker", "rm", "-f"),
            ("docker", "ps", "-aq"),
        } and argv[:2] != ("docker", "images"):
            raise AssertionError(f"unexpected external command: {argv}")
        return subprocess.CompletedProcess(argv, 0, output, "")

    monkeypatch.setattr(sk.subprocess, "run", command)

    assert sk.main(_replay_args(tmp_path)) == fe.EXIT_FAILED
    evidence = _evidence(tmp_path)
    assert evidence["result"] == "failed"
    assert "connection lost after create" in evidence["error"]
    assert evidence["cleanup_verified"] is True
    assert evidence["cleanup_errors"] == []
    assert ("docker", "rm", "-f", evidence["registry"]) in calls
    cluster_delete = ("kind", "delete", "cluster", "--name", evidence["cluster"])
    assert (cluster_delete in calls) is (failure_stage == "cluster")
    assert not list((tmp_path / "scratch").iterdir())


def _attempted_resources(run: sk.ScriptedRun) -> None:
    """Exercise teardown independently of the product's scenario execution."""

    run.cluster_attempted = True
    run.registry_attempted = True


def test_remaining_owned_cluster_overrides_a_successful_scenario(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sk.ScriptedRun, "execute", _attempted_resources)
    calls: list[tuple[str, ...]] = []
    cluster: str | None = None

    def command(argv: tuple[str, ...], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        nonlocal cluster
        calls.append(argv)
        output = ""
        if argv[:3] == ("kind", "delete", "cluster"):
            cluster = argv[-1]
        elif argv[:3] == ("kind", "get", "clusters"):
            assert cluster is not None
            output = cluster + "\n"
        elif argv[:3] == ("docker", "ps", "-aq"):
            if "label=io.x-k8s.kind.cluster=" in argv[-1]:
                output = "owned-node-container\n"
        elif argv[:3] != ("docker", "rm", "-f"):
            raise AssertionError(f"unexpected external command: {argv}")
        return subprocess.CompletedProcess(argv, 0, output, "")

    monkeypatch.setattr(sk.subprocess, "run", command)

    assert sk.main(_replay_args(tmp_path)) == fe.EXIT_FAILED
    evidence = _evidence(tmp_path)
    assert evidence["result"] == "failed"
    assert evidence["cleanup_verified"] is False
    assert "owned kind cluster remains after teardown" in evidence["cleanup_errors"]
    assert "owned kind node container remains after teardown" in evidence["cleanup_errors"]
    assert ("docker", "rm", "-f", evidence["registry"]) in calls


def test_cluster_delete_error_does_not_prevent_registry_deletion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sk.ScriptedRun, "execute", _attempted_resources)
    calls: list[tuple[str, ...]] = []

    def command(argv: tuple[str, ...], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        if argv[:3] == ("kind", "delete", "cluster"):
            return subprocess.CompletedProcess(argv, 1, "", "owned cluster deletion failed")
        if argv[:3] not in {("docker", "rm", "-f"), ("docker", "ps", "-aq")}:
            raise AssertionError(f"unexpected external command: {argv}")
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(sk.subprocess, "run", command)

    assert sk.main(_replay_args(tmp_path)) == fe.EXIT_FAILED
    evidence = _evidence(tmp_path)
    assert evidence["cleanup_verified"] is False
    assert any("owned cluster deletion failed" in error for error in evidence["cleanup_errors"])
    assert ("docker", "rm", "-f", evidence["registry"]) in calls


def test_unexpected_real_model_request_fails_and_records_evidence_after_endpoint_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the real HTTP replay endpoint; no model or teardown fact is supplied."""

    model_module = sk._module(
        "factory_test_model_script", REPO_ROOT / "tools/model-script/model_script.py"
    )
    args = _replay_args(tmp_path)
    model = model_module.ModelScript(Path(args.transcript))
    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)

    def execute(run: sk.ScriptedRun) -> None:
        run.model = model
        run.install_started = time.monotonic()
        run.deadline = run.install_started + 10
        run.evidence["model_port"] = model.port
        model.start()
        request = Request(
            model.base_url + "/v1/messages",
            data=json.dumps(
                {"messages": [{"role": "user", "content": "add Kelvin tests"}]}
            ).encode(),
            headers={"Content-Type": "application/json"},
        )
        with pytest.raises(HTTPError) as refused:
            with urlopen(request, timeout=5) as response:
                response.read()
        assert refused.value.code == 422
        refused.value.close()
        run.wait_for_final()

    monkeypatch.setattr(sk.ScriptedRun, "execute", execute)
    try:
        assert sk.main(args) == fe.EXIT_FAILED
        evidence = _evidence(tmp_path)
        assert evidence["result"] == "failed"
        assert "unexpected model request" in evidence["error"]
        assert len(evidence["unexpected_model_requests"]) == 1
        assert evidence["unexpected_model_requests"][0]["path"] == "/v1/messages"
        # The refused request is the scenario failure, recorded above. Teardown
        # itself completed, so it must not also read as an incomplete cleanup.
        assert evidence["cleanup_errors"] == []
        assert evidence["cleanup_verified"] is True
        with socket.socket() as probe:
            probe.settimeout(1)
            assert probe.connect_ex((model.host, model.port)) != 0
    finally:
        try:
            model.close()
        except model_module.UnexpectedRequest:
            pass


def _cleanup_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> sk.ScriptedRun:
    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)
    run = sk.ScriptedRun(_replay_args(tmp_path))
    run.cluster_attempted = True
    run.registry_attempted = True
    run.workdir = tmp_path / "owned-workdir"
    run.workdir.mkdir()
    return run


def _successful_cleanup_commands(
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[str, ...]]:
    calls: list[tuple[str, ...]] = []

    def command(argv: tuple[str, ...], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        if argv[:3] not in {
            ("kind", "delete", "cluster"),
            ("kind", "get", "clusters"),
            ("docker", "rm", "-f"),
            ("docker", "ps", "-aq"),
        }:
            raise AssertionError(f"unexpected external command: {argv}")
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(sk.subprocess, "run", command)
    return calls


def _assert_remaining_teardown_attempted(
    run: sk.ScriptedRun,
    calls: list[tuple[str, ...]],
) -> None:
    assert ("kind", "delete", "cluster", "--name", run.cluster) in calls
    assert ("docker", "rm", "-f", run.registry) in calls
    assert run.workdir is not None and not run.workdir.exists()


def test_forward_termination_error_does_not_interrupt_remaining_teardown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = _cleanup_run(tmp_path, monkeypatch)
    calls = _successful_cleanup_commands(monkeypatch)

    class BrokenForward:
        stdout = None

        def terminate(self) -> None:
            raise OSError("owned forward termination failed")

        def kill(self) -> None:
            raise OSError("owned forward forced termination failed")

        def wait(self, *, timeout: float) -> int:
            return 0

    run.forward = BrokenForward()  # type: ignore[assignment]

    errors = run.cleanup()
    assert any("owned forward" in error for error in errors)
    assert run.evidence["cleanup_verified"] is False
    _assert_remaining_teardown_attempted(run, calls)


def test_endpoint_probe_error_does_not_interrupt_cluster_and_registry_teardown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model_module = sk._module(
        "factory_cleanup_model_script", REPO_ROOT / "tools/model-script/model_script.py"
    )
    run = _cleanup_run(tmp_path, monkeypatch)
    model = model_module.ModelScript(Path(run.args.transcript))
    run.model = model
    calls = _successful_cleanup_commands(monkeypatch)
    real_socket = socket.socket

    def failed_socket_probe(*_args: Any, **_kwargs: Any) -> Any:
        raise OSError("owned endpoint probe failed")

    monkeypatch.setattr(sk.socket, "socket", failed_socket_probe)
    try:
        errors = run.cleanup()
        assert any("owned endpoint probe failed" in error for error in errors)
        assert run.evidence["cleanup_verified"] is False
        _assert_remaining_teardown_attempted(run, calls)
        with real_socket() as probe:
            probe.settimeout(1)
            assert probe.connect_ex((model.host, model.port)) != 0
    finally:
        model.close()


def test_failed_registry_delete_and_verification_still_remove_owned_workdir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = _cleanup_run(tmp_path, monkeypatch)
    run.cluster_attempted = False
    calls: list[tuple[str, ...]] = []

    def command(argv: tuple[str, ...], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        if argv[:3] == ("docker", "rm", "-f"):
            return subprocess.CompletedProcess(argv, 1, "", "owned registry deletion failed")
        if argv[:3] == ("docker", "ps", "-aq"):
            return subprocess.CompletedProcess(argv, 1, "", "owned registry verification failed")
        raise AssertionError(f"unexpected external command: {argv}")

    monkeypatch.setattr(sk.subprocess, "run", command)

    errors = run.cleanup()
    assert any("owned registry verification failed" in error for error in errors)
    assert run.evidence["cleanup_verified"] is False
    assert ("docker", "rm", "-f", run.registry) in calls
    assert run.workdir is not None and not run.workdir.exists()


def test_registry_publishes_the_explicit_port_its_mirror_names(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Joining the kind network reassigned an ephemeral port on a real host."""

    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sk.ScriptedRun, "prerequisites", lambda _self: None)
    published: list[str] = []
    mirrors: list[str] = []

    def command(argv: tuple[str, ...], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        if argv[:2] == ("docker", "run"):
            published.append(argv[argv.index("-p") + 1])
        elif argv[:3] == ("kind", "create", "cluster"):
            mirrors.append(Path(argv[argv.index("--config") + 1]).read_text())
            return subprocess.CompletedProcess(argv, 1, "", "stop after create")
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(sk.subprocess, "run", command)

    assert sk.main(_replay_args(tmp_path)) == fe.EXIT_FAILED
    assert len(published) == 1
    host, port, container = published[0].split(":")
    assert (host, container) == ("127.0.0.1", "5000")
    assert 0 < int(port) < 32768
    assert f'registry.mirrors."localhost:{port}"' in mirrors[0]


def _recording_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    run_id: str = "example-run",
    remaining: float = 50,
) -> sk.ScriptedRun:
    monkeypatch.setenv("CURIE_FACTORY_RECORD_KEY", "example-upstream-key")
    monkeypatch.setenv("FACTORY_RUN_ID", run_id)
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    monkeypatch.delenv("CURIE_FACTORY_RECORD_BASELINE_REMAINING", raising=False)
    args = fe.parse_args(["scripted", "--record", "--transcript", str(tmp_path / "record.json")])
    run = sk.ScriptedRun(args)
    monkeypatch.setattr(run, "provider_remaining", lambda: remaining)
    monkeypatch.setattr(run, "command", lambda *_args, **_kwargs: str(tmp_path / "repo/.git"))
    run.prepare_recording()
    return run


def test_failures_before_model_calls_leave_all_three_run_attempts_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for _ in range(4):
        run = _recording_run(tmp_path, monkeypatch)
        assert run.cleanup() == []
        assert "recording_attempt" not in run.evidence
        assert run.evidence["recording_model_calls"] == 0
    for attempt in range(1, 4):
        run = _recording_run(tmp_path, monkeypatch)
        run.record_model_call()
        assert run.evidence["recording_attempt"] == attempt
        assert run.cleanup() == []
    with pytest.raises(fe.ConfigError, match="3 model-reaching attempts per run"):
        _recording_run(tmp_path, monkeypatch)


def test_attempt_limit_resets_for_a_new_run_while_provider_budget_stays_shared(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for _ in range(3):
        run = _recording_run(tmp_path, monkeypatch, remaining=50)
        run.record_model_call()
        assert run.cleanup() == []
    next_run = _recording_run(tmp_path, monkeypatch, run_id="next-run", remaining=35)
    next_run.record_model_call()
    assert next_run.evidence["recording_attempt"] == 1
    assert next_run.evidence["recording_budget_start_remaining_usd"] == 50
    assert next_run.cleanup() == []
    assert next_run.evidence["recording_total_usd"] == 15
    with pytest.raises(fe.ConfigError, match="total 20 USD provider budget"):
        _recording_run(tmp_path, monkeypatch, run_id="third-run", remaining=30)


def test_in_flight_attempt_reserves_the_last_run_slot_before_forwarding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _recording_run(tmp_path, monkeypatch)
    second = _recording_run(tmp_path, monkeypatch)
    third = _recording_run(tmp_path, monkeypatch)
    pending = _recording_run(tmp_path, monkeypatch)
    for run in (first, second, third):
        run.record_model_call()
    with pytest.raises(fe.ConfigError, match="3 model-reaching attempts per run"):
        pending.record_model_call()
    first.record_model_call()
    assert first.evidence["recording_model_calls"] == 2
    assert first.evidence["recording_attempt"] == 1


def test_budget_is_rechecked_before_each_model_call_and_after_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = _recording_run(tmp_path, monkeypatch)
    run.record_model_call()
    monkeypatch.setattr(run, "provider_remaining", lambda: 30)
    with pytest.raises(fe.ConfigError, match="total 20 USD provider budget"):
        run.record_model_call()
    assert run.evidence["recording_model_calls"] == 1
    monkeypatch.setattr(run, "provider_remaining", lambda: 29)
    assert run.cleanup() == ["recording exceeded its total 20 USD provider budget"]
    assert run.evidence["recording_total_usd"] == 21
    assert run.evidence["recording_start_remaining_usd"] == 50
    assert run.evidence["recording_end_remaining_usd"] == 29


@pytest.mark.parametrize("provider_status", [200, 503])
def test_real_proxy_counts_provider_requests_including_provider_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    provider_status: int,
) -> None:
    """Drive the real recording endpoint, replacing only the external provider."""

    model_module = sk._module(
        "factory_accounting_model_script", REPO_ROOT / "tools/model-script/model_script.py"
    )
    run = _recording_run(tmp_path, monkeypatch)
    model = model_module.ModelScript(Path(run.args.transcript), record=True)
    run.model = model
    run.count_recording_requests(model_module)
    provider_calls: list[str] = []
    message = {"type": "message", "content": [{"type": "text", "text": "done"}]}

    class ProviderResponse:
        status = provider_status
        headers = {"Content-Type": "application/json"}

        def __enter__(self) -> ProviderResponse:
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(message if provider_status == 200 else {"type": "error"}).encode()

    def provider(request: Request, **_kwargs: Any) -> ProviderResponse:
        provider_calls.append(request.full_url)
        if provider_status >= 400:
            raise HTTPError(request.full_url, provider_status, "provider unavailable", {}, None)
        return ProviderResponse()

    monkeypatch.setattr(model_module, "urlopen", provider)
    model.start()
    try:
        invalid = Request(
            model.base_url + "/v1/messages",
            data=b"{}",
            headers={"Content-Type": "application/json"},
        )
        with pytest.raises(HTTPError) as refused:
            urlopen(invalid, timeout=5)
        refused.value.close()
        assert run.evidence["recording_model_calls"] == 0
        assert provider_calls == []
        request = Request(
            model.base_url + "/v1/messages",
            data=json.dumps({"messages": [{"role": "user", "content": "convert units"}]}).encode(),
            headers={"Content-Type": "application/json"},
        )
        if provider_status >= 400:
            with pytest.raises(HTTPError) as unavailable:
                urlopen(request, timeout=5)
            assert unavailable.value.code == provider_status
            unavailable.value.close()
        else:
            with urlopen(request, timeout=5) as response:
                assert json.load(response)["type"] == "message"
        assert len(provider_calls) == 1
        assert run.evidence["recording_attempt"] == 1
        assert run.evidence["recording_model_calls"] == 1
        monkeypatch.setattr(run, "provider_remaining", lambda: 30)
        with pytest.raises(HTTPError) as budget_refused:
            urlopen(request, timeout=5)
        assert budget_refused.value.code == 422
        budget_refused.value.close()
        assert len(provider_calls) == 1
        assert run.record_error == "recording reached its total 20 USD provider budget"
        assert run.record_ledger is not None
        with sqlite3.connect(run.record_ledger) as database:
            assert database.execute("SELECT model_calls FROM attempts").fetchone() == (1,)
        assert b"example-upstream-key" not in run.record_ledger.read_bytes()
    finally:
        with pytest.raises(model_module.UnexpectedRequest):
            model.close()


@pytest.mark.parametrize(
    ("remaining", "admitted"),
    [(31.8, False), (32.5, True)],
    ids=["under-two-usd-left-refused", "over-two-usd-left-admitted"],
)
def test_real_proxy_admits_a_request_only_while_two_usd_of_budget_remain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    remaining: float,
    admitted: bool,
) -> None:
    """One request can spend past the completed-drop check, so reserve 2 USD."""

    model_module = sk._module(
        "factory_reserve_model_script", REPO_ROOT / "tools/model-script/model_script.py"
    )
    run = _recording_run(tmp_path, monkeypatch, remaining=50)
    assert run.record_budget_start == 50
    model = model_module.ModelScript(Path(run.args.transcript), record=True)
    run.model = model
    run.count_recording_requests(model_module)
    provider_calls: list[str] = []
    message = {"type": "message", "content": [{"type": "text", "text": "done"}]}

    class ProviderResponse:
        status = 200
        headers = {"Content-Type": "application/json"}

        def __enter__(self) -> ProviderResponse:
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(message).encode()

    def provider(request: Request, **_kwargs: Any) -> ProviderResponse:
        provider_calls.append(request.full_url)
        return ProviderResponse()

    monkeypatch.setattr(model_module, "urlopen", provider)
    monkeypatch.setattr(run, "provider_remaining", lambda: remaining)
    model.start()
    request = Request(
        model.base_url + "/v1/messages",
        data=json.dumps({"messages": [{"role": "user", "content": "convert units"}]}).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        if admitted:
            with urlopen(request, timeout=5) as response:
                assert json.load(response)["type"] == "message"
            assert len(provider_calls) == 1
            assert run.record_error is None
            assert run.evidence["recording_model_calls"] == 1
        else:
            with pytest.raises(HTTPError) as refused:
                urlopen(request, timeout=5)
            assert refused.value.code == 422
            refused.value.close()
            assert provider_calls == []
            assert run.record_error is not None
            assert "20 USD" in run.record_error
            assert run.evidence["recording_model_calls"] == 0
            assert len(model.unexpected) == 1
    finally:
        try:
            model.close()
        except model_module.UnexpectedRequest:
            assert not admitted


def _concurrent_proxy_requests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    remaining_after: list[float],
    module_name: str,
) -> tuple[sk.ScriptedRun, Any, Any, list[int], dict[str, int]]:
    """Send two client requests at once through the real recording proxy.

    The fake provider holds each upstream request briefly and records how
    many were open at once. The provider balance drops to the next value in
    ``remaining_after`` each time an upstream response completes.
    """

    model_module = sk._module(module_name, REPO_ROOT / "tools/model-script/model_script.py")
    run = _recording_run(tmp_path, monkeypatch, remaining=50)
    assert run.record_budget_start == 50
    model = model_module.ModelScript(Path(run.args.transcript), record=True)
    run.model = model
    run.count_recording_requests(model_module)
    counts = {"open": 0, "max_open": 0, "completed": 0}
    counts_lock = threading.Lock()
    message = {"type": "message", "content": [{"type": "text", "text": "done"}]}

    class ProviderResponse:
        status = 200
        headers = {"Content-Type": "application/json"}

        def __enter__(self) -> ProviderResponse:
            return self

        def __exit__(self, *_args: Any) -> None:
            with counts_lock:
                counts["open"] -= 1
                counts["completed"] += 1

        def read(self) -> bytes:
            return json.dumps(message).encode()

    def provider(_request: Request, **_kwargs: Any) -> ProviderResponse:
        with counts_lock:
            counts["open"] += 1
            counts["max_open"] = max(counts["max_open"], counts["open"])
        time.sleep(0.5)
        return ProviderResponse()

    def remaining() -> float:
        with counts_lock:
            return remaining_after[min(counts["completed"], len(remaining_after) - 1)]

    monkeypatch.setattr(model_module, "urlopen", provider)
    monkeypatch.setattr(run, "provider_remaining", remaining)
    model.start()
    statuses: list[int] = []
    barrier = threading.Barrier(2)

    def client(text: str) -> None:
        request = Request(
            model.base_url + "/v1/messages",
            data=json.dumps({"messages": [{"role": "user", "content": text}]}).encode(),
            headers={"Content-Type": "application/json"},
        )
        barrier.wait(timeout=5)
        try:
            with urlopen(request, timeout=10) as response:
                assert json.load(response)["type"] == "message"
                statuses.append(response.status)
        except HTTPError as exc:
            statuses.append(exc.code)
            exc.close()

    clients = [
        threading.Thread(target=client, args=(text,), daemon=True)
        for text in ("convert units", "convert currency")
    ]
    for thread in clients:
        thread.start()
    for thread in clients:
        thread.join(timeout=15)
    assert not any(thread.is_alive() for thread in clients)
    return run, model_module, model, statuses, counts


def test_real_proxy_admits_concurrent_requests_one_upstream_call_at_a_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second credit check must observe the balance after the first call.

    Budget start is 50. The first request is admitted at a 17 USD drop. Once
    its upstream call completes the drop is 19, so with the 2 USD reserve the
    second request must be refused. Admitting both on the stale balance lets
    two requests spend past the 20 USD budget.
    """

    run, model_module, model, statuses, counts = _concurrent_proxy_requests(
        tmp_path, monkeypatch, [33.0, 31.0], "factory_serial_refuse_model_script"
    )
    try:
        assert counts["max_open"] == 1
        assert counts["completed"] == 1
        assert sorted(statuses) == [200, 422]
        assert run.record_error == "recording reached its total 20 USD provider budget"
        assert run.evidence["recording_model_calls"] == 1
        assert len(model.unexpected) == 1
    finally:
        try:
            model.close()
        except model_module.UnexpectedRequest:
            pass
        else:
            raise AssertionError("the refused request must be reported at close")


def test_real_proxy_forwards_concurrent_requests_serially_with_ample_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Serial forwarding must not refuse or drop a request that fits the budget."""

    run, model_module, model, statuses, counts = _concurrent_proxy_requests(
        tmp_path, monkeypatch, [45.0], "factory_serial_admit_model_script"
    )
    try:
        assert counts["max_open"] == 1
        assert counts["completed"] == 2
        assert statuses == [200, 200]
        assert run.record_error is None
        assert run.evidence["recording_model_calls"] == 2
        assert model.unexpected == []
    finally:
        model.close()


def _cross_run_proxy_requests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    remaining_after: list[float],
    module_name: str,
) -> tuple[list[sk.ScriptedRun], Any, list[Any], dict[str, int], dict[str, int]]:
    """Send one request through each of two recording runs at once.

    The runs are separate ScriptedRun instances with their own proxies,
    run namespaces and forward locks, sharing only the SQLite ledger under
    one XDG_STATE_HOME and repository, as two worktrees recording at once
    would. The fake provider records how many upstream calls were open at
    once across both runs, and its balance drops to the next value in
    ``remaining_after`` each time an upstream response completes.
    """

    model_module = sk._module(module_name, REPO_ROOT / "tools/model-script/model_script.py")
    runs = [
        _recording_run(tmp_path, monkeypatch, run_id=run_id, remaining=50)
        for run_id in ("first-run", "second-run")
    ]
    assert runs[0].record_ledger == runs[1].record_ledger
    assert all(run.record_budget_start == 50 for run in runs)
    counts = {"open": 0, "max_open": 0, "completed": 0}
    counts_lock = threading.Lock()
    message = {"type": "message", "content": [{"type": "text", "text": "done"}]}

    class ProviderResponse:
        status = 200
        headers = {"Content-Type": "application/json"}

        def __enter__(self) -> ProviderResponse:
            return self

        def __exit__(self, *_args: Any) -> None:
            with counts_lock:
                counts["open"] -= 1
                counts["completed"] += 1

        def read(self) -> bytes:
            return json.dumps(message).encode()

    def provider(_request: Request, **_kwargs: Any) -> ProviderResponse:
        with counts_lock:
            counts["open"] += 1
            counts["max_open"] = max(counts["max_open"], counts["open"])
        time.sleep(0.5)
        return ProviderResponse()

    def remaining() -> float:
        with counts_lock:
            return remaining_after[min(counts["completed"], len(remaining_after) - 1)]

    monkeypatch.setattr(model_module, "urlopen", provider)
    models = []
    for index, run in enumerate(runs):
        model = model_module.ModelScript(tmp_path / f"record-{index}.json", record=True)
        run.model = model
        run.count_recording_requests(model_module)
        monkeypatch.setattr(run, "provider_remaining", remaining)
        model.start()
        models.append(model)
    statuses: dict[str, int] = {}
    barrier = threading.Barrier(2)

    def client(name: str, base_url: str) -> None:
        request = Request(
            base_url + "/v1/messages",
            data=json.dumps({"messages": [{"role": "user", "content": name}]}).encode(),
            headers={"Content-Type": "application/json"},
        )
        barrier.wait(timeout=5)
        try:
            with urlopen(request, timeout=10) as response:
                assert json.load(response)["type"] == "message"
                statuses[name] = response.status
        except HTTPError as exc:
            statuses[name] = exc.code
            exc.close()

    clients = [
        threading.Thread(target=client, args=(name, model.base_url), daemon=True)
        for name, model in zip(("first-run", "second-run"), models, strict=True)
    ]
    for thread in clients:
        thread.start()
    for thread in clients:
        thread.join(timeout=15)
    assert not any(thread.is_alive() for thread in clients)
    return runs, model_module, models, statuses, counts


def _close_models(model_module: Any, models: list[Any], refused: set[int]) -> None:
    for index, model in enumerate(models):
        try:
            model.close()
        except model_module.UnexpectedRequest:
            assert index in refused, "an admitted run must close without a refusal"
        else:
            assert index not in refused, "the refused request must be reported at close"


def test_recording_runs_sharing_a_ledger_admit_one_upstream_call_at_a_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Admission across worktrees must see the credit left by the other run.

    Budget start is 50. Whichever run is admitted first does so at a 17 USD
    drop. Once its upstream call completes the drop is 19, so with the 2 USD
    reserve the other run must be refused. A per-run lock lets both runs pass
    the credit check on the stale balance and spend past the 20 USD budget.
    """

    runs, model_module, models, statuses, counts = _cross_run_proxy_requests(
        tmp_path, monkeypatch, [33.0, 31.0], "factory_cross_run_refuse_model_script"
    )
    refused = {index for index, run in enumerate(runs) if run.record_error is not None}
    try:
        assert counts["max_open"] == 1
        assert counts["completed"] == 1
        assert sorted(statuses.values()) == [200, 422]
        assert len(refused) == 1
        (refused_run,) = (runs[index] for index in refused)
        assert refused_run.record_error is not None
        assert "20 USD" in refused_run.record_error
        assert statuses[refused_run.record_namespace] == 422
        assert sum(run.evidence["recording_model_calls"] for run in runs) == 1
    finally:
        _close_models(model_module, models, refused)


def test_recording_runs_sharing_a_ledger_forward_serially_with_ample_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cross-run lock must not refuse or drop a request that fits the budget."""

    runs, model_module, models, statuses, counts = _cross_run_proxy_requests(
        tmp_path, monkeypatch, [45.0], "factory_cross_run_admit_model_script"
    )
    try:
        assert counts["max_open"] == 1
        assert counts["completed"] == 2
        assert statuses == {"first-run": 200, "second-run": 200}
        assert all(run.record_error is None for run in runs)
        assert [run.evidence["recording_model_calls"] for run in runs] == [1, 1]
        assert all(model.unexpected == [] for model in models)
    finally:
        _close_models(model_module, models, set())


def test_driver_pins_the_local_docker_engine_over_an_inherited_remote_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """kind, docker and the registry must all talk to the same local engine."""

    monkeypatch.setenv("DOCKER_CONTEXT", "remote-box")
    run = sk.ScriptedRun(_replay_args(tmp_path))
    assert run.env["DOCKER_CONTEXT"] == "default"


def test_driver_pins_the_default_docker_context_and_keeps_an_inherited_docker_host(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default context is the engine DOCKER_HOST names, so keep DOCKER_HOST."""

    monkeypatch.delenv("DOCKER_CONTEXT", raising=False)
    monkeypatch.setenv("DOCKER_HOST", "unix:///run/example/docker.sock")
    run = sk.ScriptedRun(_replay_args(tmp_path))
    assert run.env["DOCKER_CONTEXT"] == "default"
    assert run.env["DOCKER_HOST"] == "unix:///run/example/docker.sock"


@pytest.mark.parametrize("remaining", [None, -1, "nan", "inf"])
def test_provider_balance_requires_an_observed_finite_remaining_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    remaining: Any,
) -> None:
    run = sk.ScriptedRun(_replay_args(tmp_path))
    monkeypatch.setenv("CURIE_FACTORY_RECORD_KEY", "example-upstream-key")

    class BalanceResponse:
        def __enter__(self) -> BalanceResponse:
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

        def read(self) -> str:
            # OpenRouter documents limit_remaining separately from usage:
            # https://openrouter.ai/docs/api-reference/authentication
            return json.dumps({"data": {"usage": 1, "limit": 50, "limit_remaining": remaining}})

    monkeypatch.setattr(sk, "urlopen", lambda *_args, **_kwargs: BalanceResponse())
    with pytest.raises(fe.ConfigError, match="provider credit could not be verified"):
        run.provider_remaining()


def test_provider_balance_uses_remaining_limit_instead_of_usage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = sk.ScriptedRun(_replay_args(tmp_path))
    monkeypatch.setenv("CURIE_FACTORY_RECORD_KEY", "example-upstream-key")

    class BalanceResponse:
        def __enter__(self) -> BalanceResponse:
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

        def read(self) -> str:
            return json.dumps({"data": {"usage": 999, "limit_remaining": 35}})

    monkeypatch.setattr(sk, "urlopen", lambda *_args, **_kwargs: BalanceResponse())
    assert run.provider_remaining() == 35


@pytest.mark.parametrize("release_lock", [True, False], ids=["lock-released", "lock-still-held"])
def test_request_queued_on_the_recording_lock_is_refused_once_teardown_begins(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    release_lock: bool,
) -> None:
    """A request admitted after teardown begins would spend credit cleanup never measures.

    Another run sharing the ledger holds recording.lock, so this run's request
    queues behind it. Once teardown begins, the queued request must be refused
    without reaching the provider or being charged, whether the other run
    releases the lock or keeps holding it: lock waiting must not stall teardown.
    """

    model_module = sk._module(
        "factory_teardown_model_script", REPO_ROOT / "tools/model-script/model_script.py"
    )
    run = _recording_run(tmp_path, monkeypatch, remaining=50)
    model = model_module.ModelScript(Path(run.args.transcript), record=True)
    run.model = model
    run.count_recording_requests(model_module)
    provider_calls: list[str] = []

    def provider(request: Request, **_kwargs: Any) -> Any:
        provider_calls.append(request.full_url)
        raise AssertionError("a request queued past teardown must not reach the provider")

    monkeypatch.setattr(model_module, "urlopen", provider)
    model.start()
    assert run.record_ledger is not None
    other_run_lock = open(run.record_ledger.with_name("recording.lock"), "a")
    fcntl.flock(other_run_lock, fcntl.LOCK_EX)
    held = True
    statuses: list[int] = []

    def client() -> None:
        request = Request(
            model.base_url + "/v1/messages",
            data=json.dumps({"messages": [{"role": "user", "content": "convert units"}]}).encode(),
            headers={"Content-Type": "application/json"},
        )
        try:
            with urlopen(request, timeout=10) as response:
                statuses.append(response.status)
        except HTTPError as exc:
            statuses.append(exc.code)
            exc.close()
        except OSError:
            statuses.append(0)

    thread = threading.Thread(target=client, daemon=True)
    thread.start()
    try:
        time.sleep(0.5)
        assert thread.is_alive(), "the request must queue behind the other run's lock"
        run.begin_model_teardown()
        if release_lock:
            fcntl.flock(other_run_lock, fcntl.LOCK_UN)
            held = False
        thread.join(timeout=8)
        assert not thread.is_alive(), "a queued request must not block teardown"
        assert statuses == [422]
        assert provider_calls == []
        assert run.evidence["recording_model_calls"] == 0
        with sqlite3.connect(run.record_ledger) as database:
            assert database.execute("SELECT model_calls FROM attempts").fetchone() == (0,)
    finally:
        if held:
            fcntl.flock(other_run_lock, fcntl.LOCK_UN)
        other_run_lock.close()
        thread.join(timeout=10)
        try:
            model.close()
        except model_module.UnexpectedRequest:
            pass


_READ_ONLY_PREREQUISITES = {
    ("kind", "version"),
    ("docker", "info"),
    ("helm", "version"),
}


def _image_check_commands(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    missing: str | None,
) -> tuple[list[tuple[str, ...]], list[tuple[str, ...]]]:
    """Answer read-only prerequisite probes; record anything that would mutate.

    Returns every command seen and the commands outside the read-only probes
    and image inspection. A mutating command fails, so the run stops there.
    """

    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sk.shutil, "which", lambda binary: f"/usr/bin/{Path(binary).name}")
    monkeypatch.setattr(sk.ScriptedRun, "check_disk", lambda _self, _stage: None)
    calls: list[tuple[str, ...]] = []
    mutating: list[tuple[str, ...]] = []

    def command(argv: tuple[str, ...], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        if argv[:2] in _READ_ONLY_PREREQUISITES:
            output = "kind v0.24.0 go1.22 linux/amd64" if argv[0] == "kind" else ""
            return subprocess.CompletedProcess(argv, 0, output, "")
        if argv[:3] == ("docker", "image", "inspect"):
            if argv[3] == missing:
                return subprocess.CompletedProcess(argv, 1, "", f"Error: No such image: {missing}")
            return subprocess.CompletedProcess(argv, 0, "[]", "")
        mutating.append(argv)
        if argv[:3] in {("docker", "rm", "-f"), ("docker", "ps", "-aq")}:
            return subprocess.CompletedProcess(argv, 0, "", "")
        return subprocess.CompletedProcess(argv, 1, "", "stop at first resource command")

    monkeypatch.setattr(sk.subprocess, "run", command)
    return calls, mutating


@pytest.mark.parametrize("missing", sk.IMAGES)
def test_missing_local_image_refuses_as_configuration_before_resource_creation(
    missing: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without --build-images a missing candidate image is an operator setup error."""

    calls, mutating = _image_check_commands(tmp_path, monkeypatch, missing)

    assert sk.main(_replay_args(tmp_path)) == fe.EXIT_CONFIG
    assert ("docker", "image", "inspect", missing) in calls
    assert mutating == [], "no cluster, registry or build command may run"
    evidence = _evidence(tmp_path)
    assert missing in evidence["error"]
    assert "--build-images" in evidence["error"]
    assert evidence["cleanup_verified"] is True


def test_present_local_images_pass_the_check_and_reach_resource_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls, mutating = _image_check_commands(tmp_path, monkeypatch, None)

    assert sk.main(_replay_args(tmp_path)) == fe.EXIT_FAILED
    inspected = [argv[3] for argv in calls if argv[:3] == ("docker", "image", "inspect")]
    assert sorted(inspected) == sorted(sk.IMAGES)
    assert mutating, "the run must proceed past the image check"
    assert mutating[0][:2] in {("docker", "run"), ("kind", "create")}
    first_mutation = calls.index(mutating[0])
    assert all(
        calls.index(("docker", "image", "inspect", image)) < first_mutation for image in sk.IMAGES
    )
    assert "stop at first resource command" in _evidence(tmp_path)["error"]


def test_failed_publication_evidence_survives_verdict_without_transport_or_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = sk.ScriptedRun(fe.parse_args(["scripted"]))
    request_id = "00000000-0000-0000-0000-000000000001"
    run.evidence["execution_request_id"] = request_id
    run.evidence["final_comment"] = {"body": f"<!-- curie-execution-request:{request_id} -->"}
    run.secrets = ["example-sensitive-token"]
    queries: list[str] = []

    def durable_observation(query: str) -> dict[str, Any]:
        queries.append(query)
        return {
            "preflights": [{"outcome": "passed"}],
            "publications": [{
                "changed_paths": ["src/unitconv/core.py", "tests/test_core.py"],
                "status": "failed",
                "error": "credential refused example-sensitive-token https://stub.example.test/token",
                "result_delivery_error": None,
                "resource_cleanup_error": None,
            }],
        }

    monkeypatch.setattr(run, "sql", durable_observation)
    monkeypatch.setattr(sk, "assert_observed", lambda *_: None)
    run.stub = type("EmptyPulls", (), {"_rows": lambda self, _: []})()
    with pytest.raises(sk.ScriptedScenarioError, match="exactly one stub pull request"):
        run.observe()
    outcome = run.evidence["publication_outcomes"][0]
    assert outcome["status"] == "failed"
    assert outcome["error"] == "credential refused [REDACTED] https://example.com"
    assert "patch_bytes" not in queries[0]
    assert "'error', error" in queries[0]
    assert "example-sensitive-token" not in json.dumps(run.evidence)


@pytest.mark.parametrize("publication_namespace", ["example-publication", ""])
def test_publication_trust_uses_actual_worker_namespace_or_refuses(
    publication_namespace: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    run = sk.ScriptedRun(fe.parse_args(["scripted"]))
    ca_file = tmp_path / "ca.pem"
    run.stub = type("StubCa", (), {"ca_file": ca_file})()
    calls: list[tuple[str, ...]] = []

    def kubectl(*args: str, timeout: float = 300) -> str:
        calls.append(args)
        if "deployment/curie-worker" in args:
            assert args[:2] == ("-n", run.namespace)
            assert timeout == 10
            return publication_namespace
        assert args == (
            "-n", publication_namespace, "create", "configmap", "github-stub-ca",
            f"--from-file=ca.pem={ca_file}",
        )
        return ""

    monkeypatch.setattr(run, "kubectl", kubectl)
    if publication_namespace:
        run.provision_publication_trust()
        assert len(calls) == 2
    else:
        with pytest.raises(sk.ScriptedScenarioError, match="publication namespace is absent"):
            run.provision_publication_trust()
        assert len(calls) == 1


def test_publication_runtime_keeps_bounded_redacted_pod_failure_after_removal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = sk.ScriptedRun(fe.parse_args(["scripted"]))
    run.secrets = ["example-sensitive-token"]
    calls: list[tuple[str, ...]] = []
    remaining = True

    def kubectl(*args: str, timeout: float) -> str:
        calls.append(args)
        assert timeout <= 10
        if "deployment/curie-worker" in args:
            assert args[:2] == ("-n", run.namespace)
            return "example-publication-namespace"
        assert args[:2] == ("-n", "example-publication-namespace")
        if "get" in args:
            return json.dumps({"items": [{
                "kind": "Pod", "metadata": {"name": "example-publication"},
                "spec": {"env": [{"value": "example-sensitive-token"}]},
                "status": {"phase": "Pending", "containerStatuses": [{
                    "restartCount": 0, "state": {"waiting": {
                        "reason": "ImagePullBackOff",
                        "message": "cannot pull https://registry.example.test/image",
                    }},
                }]},
            }] if remaining else []})
        assert "--tail=40" in args and "--limit-bytes=4096" in args
        return "example-sensitive-token failed HTTPS https://stub.example.test/path"

    monkeypatch.setattr(run, "kubectl", kubectl)
    run.capture_publication_runtime()
    snapshot = run.evidence["publication_runtime"][0]
    assert snapshot["phase"] == "Pending"
    assert snapshot["containers"][0]["state"]["waiting"]["reason"] == "ImagePullBackOff"
    assert snapshot["logs"] == "[REDACTED] failed HTTPS https://example.com"
    assert "spec" not in snapshot
    remaining = False
    run.capture_publication_runtime()
    assert run.evidence["publication_runtime"] == [snapshot]
    assert all(
        "curietech.ai/component=publication" in call for call in calls if "jobs,pods" in call
    )
