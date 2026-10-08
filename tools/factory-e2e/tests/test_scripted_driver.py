"""Failure contracts for the disposable scripted factory driver.

These tests exercise driver validation and lifecycle bookkeeping. The real
kind scenario remains the proof that the product completes an issue.
"""

from __future__ import annotations

import json
import socket
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
    assert not list((tmp_path / ".projects/factory-scripted").iterdir())


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
