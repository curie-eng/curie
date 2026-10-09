"""Failure contracts for the disposable scripted factory driver.

These tests exercise driver validation and lifecycle bookkeeping. The real
kind scenario remains the proof that the product completes an issue.
"""

from __future__ import annotations

import json
import socket
import sqlite3
import subprocess
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
