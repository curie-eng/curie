"""Failure contracts for the disposable scripted factory driver.

These tests exercise driver validation and lifecycle bookkeeping. The real
kind scenario remains the proof that the product completes an issue.
"""

from __future__ import annotations

import base64
import dataclasses
import fcntl
import json
import os
import socket
import sqlite3
import subprocess
import threading
import time
from pathlib import Path
from subprocess import run as subprocess_run
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import factory_e2e as fe
import kind_images
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
    args = fe.parse_args(["scripted", "--scenario", "publication", "--transcript", str(missing)])

    assert sk.main(args) == fe.EXIT_CONFIG
    assert "transcript" in capsys.readouterr().err.lower()


def test_facts_configuration_needs_no_model_transcript(tmp_path: Path) -> None:
    args = fe.parse_args(
        ["scripted", "--scenario", "facts", "--transcript", str(tmp_path / "missing.json")]
    )
    sk.validate_configuration(args)
    assert sk.ScriptedRun(args).model is None


def test_facts_recording_refuses_before_resource_mutation(
    monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    monkeypatch.setattr(sk.subprocess, "run", _refuse_external_command)
    args = fe.parse_args(["scripted", "--scenario", "facts", "--record"])
    assert sk.main(args) == fe.EXIT_CONFIG
    assert "never records" in capsys.readouterr().err


def test_recording_another_scenario_refuses_before_resource_mutation(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(sk.subprocess, "run", _refuse_external_command)
    args = fe.parse_args(["scripted", "--scenario", "jira-one-ticket", "--record"])
    assert sk.main(args) == fe.EXIT_CONFIG
    assert "only the publication scenario supports recording" in capsys.readouterr().err


@pytest.mark.parametrize("violation", [None, "admission", "request", "tool", "route"])
def test_facts_scenario_executes_assertions_and_refuses_violations(
    tmp_path: Path, violation: str | None
) -> None:
    import copy
    import types

    sample = json.loads((REPO_ROOT / "tests/vectors/factory-facts-v1.examples.json").read_text())[0]
    sample["label_route"]["route"] = "github_user_via_app"
    sample["request"]["comment_id"] = "4"
    if violation == "admission":
        sample["admission"] = "requested"
    if violation == "request":
        sample["request"]["comment_id"] = "3"
    if violation == "tool":
        sample["executables"][0]["state"] = "present"
    if violation == "route":
        sample["label_route"]["route"] = "github_user"
    args = fe.parse_args(["scripted", "--scenario", "facts"])
    driver = sk.ScriptedRun(args)
    calls: list[Any] = []

    class Stub:
        request_log: list[Any] = []

        def __init__(self) -> None:
            self.rows: list[dict[str, Any]] = []

        def add_issue_comment(
            self, number: int, body: str, *, author: Any, app: Any = None
        ) -> dict[str, Any]:
            row = {"id": len(self.rows) + 1}
            self.rows.append(row)
            return row

    def kubectl(*args: Any, **kwargs: Any) -> str:
        calls.append(args)
        return '{"items":[]}'

    def api(method: str, path: str, body: Any = None) -> Any:
        calls.append((method, path))
        return {"items": []}

    def wait(request: str) -> dict[str, Any]:
        calls.append(("wait", request))
        return copy.deepcopy(sample)

    driver.stub = Stub()
    driver.kubectl = kubectl
    driver.facts_api = api
    driver.wait_facts = wait
    driver.sql = lambda query: {"work_items": 0, "execution_requests": 0, "deliveries": 2}
    driver.run_tool_facts = lambda: calls.append(("tool-facts",))
    github = types.SimpleNamespace(HUMAN={"type": "User"}, BOT={"type": "Bot"})
    if violation is None:
        driver.run_facts(github)
        assert driver.evidence["model_requests"] == []
        assert driver.evidence["api_work_items"] == driver.evidence["api_execution_requests"] == 0
        assert any("restart" in call for call in calls)
    else:
        with pytest.raises(sk.ScriptedScenarioError):
            driver.run_facts(github)


def test_record_without_upstream_key_refuses_before_resource_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("CURIE_FACTORY_RECORD_KEY", raising=False)
    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sk.subprocess, "run", _refuse_external_command)
    args = fe.parse_args(
        ["scripted", "--scenario", "publication", "--record",
         "--transcript", str(tmp_path / "record.json")]
    )

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
    args = fe.parse_args(["scripted", "--scenario", "publication", "--transcript", str(malformed)])

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
    args = fe.parse_args(["scripted", "--scenario", "publication", "--transcript", str(transcript)])

    sk.validate_configuration(args)


def _replay_args(tmp_path: Path, *, scenario: str = "publication") -> Any:
    transcript = tmp_path / "transcript.json"
    transcript.write_text(json.dumps({"version": 1, "exchanges": []}))
    return fe.parse_args(["scripted", "--scenario", scenario, "--transcript", str(transcript)])


def _evidence(tmp_path: Path) -> dict[str, Any]:
    files = list((tmp_path / "target/factory-e2e").glob("scripted-*.json"))
    assert len(files) == 1
    return json.loads(files[0].read_text())


@pytest.fixture
def scripted_decisions_bundle(tmp_path: Path) -> Path:
    bundle = tmp_path / "bundle"
    hooks = bundle / "hooks"
    hooks.mkdir(parents=True)
    helper = REPO_ROOT / "examples/dark-factory/hooks/decisions_section.py"
    (hooks / helper.name).write_bytes(helper.read_bytes())
    return bundle


@pytest.mark.parametrize(
    "case",
    json.loads((REPO_ROOT / "tests/vectors/factory-decisions-section.json").read_text())["cases"],
    ids=lambda case: case["id"],
)
def test_scripted_publication_decisions_match_the_shared_contract(
    case: dict[str, Any], scripted_decisions_bundle: Path,
) -> None:
    pull = {"body": case["body"]}
    if case["valid"]:
        assert sk.assert_pull_request_decisions(scripted_decisions_bundle, pull) == len(
            case["entries"]
        )
    else:
        with pytest.raises(sk.ScriptedScenarioError, match="invalid Decisions section"):
            sk.assert_pull_request_decisions(scripted_decisions_bundle, pull)


def test_replayed_publication_contains_valid_decisions(scripted_decisions_bundle: Path) -> None:
    transcript = json.loads(sk.TRANSCRIPT.read_text())
    publications = [
        block["input"]
        for exchange in transcript["exchanges"]
        for block in exchange["message"]["content"]
        if block.get("name") == "mcp__curie__publish_changes"
    ]
    assert len(publications) == 1
    assert sk.assert_pull_request_decisions(scripted_decisions_bundle, publications[0]) > 0


def test_scripted_driver_records_the_curie_bin_it_used(
    candidate_curie: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)

    def execute(run: sk.ScriptedRun) -> None:
        assert run.curie == str(candidate_curie)
        monkeypatch.setenv("CURIE_BIN", str(tmp_path / "changed-selection"))
        run.env["PATH"] = ""
        assert run.command(run.curie, "build").strip() == str(candidate_curie)

    monkeypatch.setattr(sk.ScriptedRun, "execute", execute)
    # This test is about the binary; its stubbed install has no scenario to run.
    monkeypatch.setattr(sk.ScriptedRun, "runs_one_ticket", False)

    assert sk.main(_replay_args(tmp_path)) == 0
    evidence = _evidence(tmp_path)
    assert evidence["curie_bin"] == str(candidate_curie)
    assert evidence["curie_version"] == "curie 0.0.0-test"
    assert evidence["cleanup_verified"] is True
    assert (
        f"factory-e2e: curie binary {candidate_curie} (curie 0.0.0-test)"
        in capsys.readouterr().err
    )


@pytest.mark.parametrize("selection", ["relative", "path-name", "default"])
def test_scripted_driver_resolves_relative_and_path_binaries(
    selection: str,
    candidate_curie: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PATH", str(candidate_curie.parent))
    if selection == "default":
        monkeypatch.delenv("CURIE_BIN")
    else:
        monkeypatch.setenv("CURIE_BIN", "./bin/curie" if selection == "relative" else "curie")

    def execute(run: sk.ScriptedRun) -> None:
        assert run.command(run.curie, "build").strip() == str(candidate_curie)

    monkeypatch.setattr(sk.ScriptedRun, "execute", execute)
    # This test is about the binary; its stubbed install has no scenario to run.
    monkeypatch.setattr(sk.ScriptedRun, "runs_one_ticket", False)

    assert sk.main(_replay_args(tmp_path)) == 0
    evidence = _evidence(tmp_path)
    assert evidence["curie_bin"] == str(candidate_curie)
    assert evidence["curie_version"] == "curie 0.0.0-test"


@pytest.mark.parametrize(
    "failure",
    [
        "missing",
        "not-executable",
        "version-failed",
        "empty-version",
        "symlink-loop",
        "invalid-version",
    ],
)
def test_scripted_driver_refuses_invalid_curie_before_external_actions(
    failure: str,
    candidate_curie: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)
    if failure == "missing":
        monkeypatch.setenv("CURIE_BIN", str(tmp_path / "missing-curie"))
    elif failure == "not-executable":
        candidate_curie.chmod(0o644)
    elif failure == "version-failed":
        candidate_curie.write_text("#!/bin/sh\nexit 9\n")
    elif failure == "empty-version":
        candidate_curie.write_text("#!/bin/sh\nexit 0\n")
    elif failure == "symlink-loop":
        candidate_curie.unlink()
        candidate_curie.symlink_to(candidate_curie.name)
    else:
        candidate_curie.write_text("#!/bin/sh\nprintf '\\377\\n'\n")
    monkeypatch.setattr(sk.ScriptedRun, "execute", _refuse_external_command)
    monkeypatch.setattr(fe, "http_json", _refuse_external_command)

    assert sk.main(_replay_args(tmp_path)) == fe.EXIT_CONFIG
    assert "CURIE_BIN" in capsys.readouterr().err


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
        if len(argv) == 2 and argv[1] == "--version":
            return subprocess_run(argv, capture_output=True, text=True, check=False)
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
    assert ("docker", "rm", "-f", "-v", evidence["registry"]) in calls
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
        if len(argv) == 2 and argv[1] == "--version":
            return subprocess_run(argv, capture_output=True, text=True, check=False)
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
    assert ("docker", "rm", "-f", "-v", evidence["registry"]) in calls


def test_cluster_delete_error_does_not_prevent_registry_deletion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sk.ScriptedRun, "execute", _attempted_resources)
    calls: list[tuple[str, ...]] = []

    def command(argv: tuple[str, ...], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        if len(argv) == 2 and argv[1] == "--version":
            return subprocess_run(argv, capture_output=True, text=True, check=False)
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
    assert ("docker", "rm", "-f", "-v", evidence["registry"]) in calls


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
    assert ("docker", "rm", "-f", "-v", run.registry) in calls
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


def test_closed_endpoint_self_connection_does_not_fail_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model_module = sk._module(
        "factory_cleanup_self_connection_model", REPO_ROOT / "tools/model-script/model_script.py"
    )
    run = _cleanup_run(tmp_path, monkeypatch)
    model = model_module.ModelScript(Path(run.args.transcript))
    model.start()
    run.model = model
    calls = _successful_cleanup_commands(monkeypatch)
    connections: list[tuple[Any, Any]] = []

    class SelfConnectingSocket(socket.socket):
        def connect_ex(self, address: Any) -> int:
            # Force Linux's ephemeral source-port allocation to choose the
            # just-closed listener's port. The TCP connection itself is real.
            self.bind(address)
            result = super().connect_ex(address)
            assert result == 0
            connections.append((self.getsockname(), self.getpeername()))
            return result

    monkeypatch.setattr(sk.socket, "socket", SelfConnectingSocket)
    try:
        errors = run.cleanup()
        assert connections == [((model.host, model.port), (model.host, model.port))]
        assert errors == []
        assert run.evidence["cleanup_verified"] is True
        _assert_remaining_teardown_attempted(run, calls)
    finally:
        model.close()


def test_listening_endpoint_fails_cleanup_and_remaining_teardown_still_runs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model_module = sk._module(
        "factory_cleanup_listening_model", REPO_ROOT / "tools/model-script/model_script.py"
    )
    run = _cleanup_run(tmp_path, monkeypatch)
    model = model_module.ModelScript(Path(run.args.transcript))
    model.start()
    run.model = model
    calls = _successful_cleanup_commands(monkeypatch)
    close = model.close
    monkeypatch.setattr(model, "close", lambda: None)
    try:
        assert run.cleanup() == [f"owned endpoint still listens on port {model.port}"]
        assert run.evidence["cleanup_verified"] is False
        _assert_remaining_teardown_attempted(run, calls)
    finally:
        close()


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
    assert ("docker", "rm", "-f", "-v", run.registry) in calls
    assert run.workdir is not None and not run.workdir.exists()


def test_teardown_removes_anonymous_volumes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Teardown removes anonymous volumes with the containers that own them."""

    run = _cleanup_run(tmp_path, monkeypatch)
    calls: list[tuple[str, ...]] = []
    labelled_listings = 0

    def command(argv: tuple[str, ...], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        nonlocal labelled_listings
        calls.append(argv)
        output = ""
        if argv[:3] == ("docker", "ps", "-aq") and any(
            "label=curie.factory-scripted=" in arg for arg in argv
        ):
            labelled_listings += 1
            if labelled_listings == 1:
                output = "deadbeef\n"
        elif argv[:3] not in {
            ("kind", "delete", "cluster"),
            ("kind", "get", "clusters"),
            ("docker", "rm", "-f"),
            ("docker", "ps", "-aq"),
        }:
            raise AssertionError(f"unexpected external command: {argv}")
        return subprocess.CompletedProcess(argv, 0, output, "")

    monkeypatch.setattr(sk.subprocess, "run", command)

    assert run.cleanup() == []
    # docker rm --volumes removes anonymous volumes associated with the container.
    # https://docs.docker.com/reference/cli/docker/container/rm/
    rm_argv = [argv for argv in calls if argv[:2] == ("docker", "rm")]
    assert ("docker", "rm", "-f", "-v", run.registry) in rm_argv
    assert ("docker", "rm", "-f", "-v", "deadbeef") in rm_argv
    assert all("-v" in argv for argv in rm_argv)


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
        if len(argv) == 2 and argv[1] == "--version":
            return subprocess_run(argv, capture_output=True, text=True, check=False)
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
    args = fe.parse_args(
        ["scripted", "--scenario", "publication", "--record",
         "--transcript", str(tmp_path / "record.json")]
    )
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


@pytest.mark.parametrize("inherited_host", [None, ""])
@pytest.mark.parametrize(
    "endpoint",
    [
        "ssh://builder@engine.example.com",
        "tcp://engine.example.com:2376",
        "unix:///run/example/docker.sock",
        "npipe:////./pipe/docker_engine",
    ],
)
def test_driver_resolves_the_selected_docker_context_once_for_every_engine(
    inherited_host: str | None,
    endpoint: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DOCKER_CONTEXT", "example-builder")
    if inherited_host is None:
        monkeypatch.delenv("DOCKER_HOST", raising=False)
    else:
        monkeypatch.setenv("DOCKER_HOST", inherited_host)
    calls: list[tuple[tuple[str, ...], dict[str, str]]] = []

    def command(argv: tuple[str, ...], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append((argv, dict(kwargs["env"])))
        if argv[:3] == ("docker", "context", "inspect"):
            # Docker exposes the endpoint through context inspect's Go template:
            # https://docs.docker.com/reference/cli/docker/context/inspect/
            assert argv == (
                "docker", "context", "inspect", "example-builder",
                "--format", "{{json .Endpoints.docker.Host}}",
            )
            return subprocess.CompletedProcess(argv, 0, json.dumps(endpoint) + "\n", "")
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(sk.subprocess, "run", command)
    run = sk.ScriptedRun(_replay_args(tmp_path))
    run.command("docker", "info")
    run.command("kind", "get", "clusters")
    run.command("docker", "buildx", "ls")

    assert len(calls) == 4
    assert calls[0][1]["DOCKER_CONTEXT"] == "example-builder"
    assert calls[0][1].get("DOCKER_HOST") == inherited_host
    assert [argv for argv, _env in calls[1:]] == [
        ("docker", "info"), ("kind", "get", "clusters"), ("docker", "buildx", "ls"),
    ]
    for _argv, env in calls[1:]:
        assert env["DOCKER_CONTEXT"] == "default"
        assert env["DOCKER_HOST"] == endpoint
        assert env["BUILDX_BUILDER"] == "default"


@pytest.mark.parametrize("context", [None, "default", "example-builder"])
def test_driver_preserves_an_explicit_docker_host_without_inspecting_context(
    context: str | None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if context is None:
        monkeypatch.delenv("DOCKER_CONTEXT", raising=False)
    else:
        monkeypatch.setenv("DOCKER_CONTEXT", context)
    endpoint = "ssh://builder@engine.example.com"
    monkeypatch.setenv("DOCKER_HOST", endpoint)
    environments: list[dict[str, str]] = []

    def command(argv: tuple[str, ...], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        assert argv == ("docker", "info")
        environments.append(dict(kwargs["env"]))
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(sk.subprocess, "run", command)
    run = sk.ScriptedRun(_replay_args(tmp_path))
    assert environments == []
    run.command("docker", "info")
    assert len(environments) == 1
    assert environments[0]["DOCKER_CONTEXT"] == "default"
    assert environments[0]["DOCKER_HOST"] == endpoint


@pytest.mark.parametrize("context", [None, "", "default"])
def test_driver_keeps_default_docker_selection_without_inspecting_context(
    context: str | None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if context is None:
        monkeypatch.delenv("DOCKER_CONTEXT", raising=False)
    else:
        monkeypatch.setenv("DOCKER_CONTEXT", context)
    monkeypatch.delenv("DOCKER_HOST", raising=False)
    environments: list[dict[str, str]] = []

    def command(argv: tuple[str, ...], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        assert argv == ("docker", "info")
        environments.append(dict(kwargs["env"]))
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(sk.subprocess, "run", command)
    run = sk.ScriptedRun(_replay_args(tmp_path))
    assert environments == []
    run.command("docker", "info")
    assert len(environments) == 1
    assert environments[0]["DOCKER_CONTEXT"] == "default"
    assert "DOCKER_HOST" not in environments[0]


@pytest.mark.parametrize(
    "resolution",
    [
        "failed-inspect", "missing-docker", "timeout", "malformed-json", "null",
        '""', '"ssh://"', '"not-an-endpoint"', '"ssh://engine.example.com with spaces"',
    ],
)
def test_unresolvable_docker_context_refuses_before_resource_mutation(
    resolution: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("DOCKER_CONTEXT", "example-builder")
    monkeypatch.delenv("DOCKER_HOST", raising=False)
    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sk.ScriptedRun, "execute", _refuse_external_command)
    monkeypatch.setattr(sk.ScriptedRun, "cleanup", _refuse_external_command)
    calls: list[tuple[str, ...]] = []

    def command(argv: tuple[str, ...], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        if len(argv) == 2 and argv[1] == "--version":
            return subprocess_run(argv, capture_output=True, text=True, check=False)
        calls.append(argv)
        assert argv[:4] == ("docker", "context", "inspect", "example-builder")
        if resolution == "missing-docker":
            raise FileNotFoundError("docker is unavailable")
        if resolution == "timeout":
            raise subprocess.TimeoutExpired(argv, 30)
        if resolution == "failed-inspect":
            return subprocess.CompletedProcess(argv, 1, "", "context is unavailable")
        return subprocess.CompletedProcess(argv, 0, resolution, "")

    monkeypatch.setattr(sk.subprocess, "run", command)

    assert sk.main(_replay_args(tmp_path)) == fe.EXIT_CONFIG
    assert "example-builder" in capsys.readouterr().err
    assert len(calls) == 1


@pytest.mark.parametrize("docker_host", [None, "", "unix:///var/run/docker.sock"])
def test_local_engine_keeps_the_kind_gateway_for_in_process_servers(
    docker_host: str | None,
) -> None:
    """A local engine's pods reach this process through the kind gateway."""

    assert (
        sk.in_process_callback_host(
            docker_host=docker_host,
            stub_host="192.0.2.10",
            gateway="172.18.0.1",
        )
        == "172.18.0.1"
    )


def test_remote_engine_binds_in_process_servers_to_the_stub_host() -> None:
    """Pods on a remote engine cannot use that host's kind gateway."""

    assert (
        sk.in_process_callback_host(
            docker_host="ssh://builder@engine.example",
            stub_host="192.0.2.10",
            gateway="172.18.0.1",
        )
        == "192.0.2.10"
    )


def test_remote_engine_without_a_stub_host_is_refused() -> None:
    with pytest.raises(sk.ScriptedScenarioError, match="CURIE_LOCAL_STUB_HOST"):
        sk.in_process_callback_host(
            docker_host="tcp://10.0.0.8:2375",
            stub_host=None,
            gateway="172.18.0.1",
        )


def test_registry_publish_retries_when_the_engine_already_holds_the_port(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The client probe cannot see a port held on a remote engine."""

    run = sk.ScriptedRun(_replay_args(tmp_path))
    monkeypatch.setattr(sk, "registry_port_candidates", lambda: [24111, 24112])
    monkeypatch.setattr(sk, "client_loopback_port_is_free", lambda _port: True)
    published: list[str] = []

    def command(*argv: str, timeout: float = 900) -> str:
        del timeout
        mapping = argv[argv.index("-p") + 1]
        if mapping.startswith("127.0.0.1:24111:"):
            raise sk.ScriptedScenarioError("docker run failed: port is already allocated")
        published.append(mapping)
        return "registry-id"

    monkeypatch.setattr(run, "command", command)
    port, ref = run.start_registry()
    assert port == 24112
    assert ref == "localhost:24112"
    assert published == ["127.0.0.1:24112:5000"]


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
    monkeypatch.setattr(sk.ScriptedRun, "check_disk", lambda _self, _stage, **_kwargs: None)
    calls: list[tuple[str, ...]] = []
    mutating: list[tuple[str, ...]] = []

    def command(argv: tuple[str, ...], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        if len(argv) == 2 and argv[1] == "--version":
            return subprocess_run(argv, capture_output=True, text=True, check=False)
        calls.append(argv)
        if argv[:2] in _READ_ONLY_PREREQUISITES:
            output = "kind v0.24.0 go1.22 linux/amd64" if argv[0] == "kind" else ""
            return subprocess.CompletedProcess(argv, 0, output, "")
        if argv[:3] == ("docker", "image", "inspect"):
            if argv[3] == missing:
                return subprocess.CompletedProcess(argv, 1, "", f"Error: No such image: {missing}")
            return subprocess.CompletedProcess(
                argv, 0, json.dumps([{"Size": 1024} for _ in argv[3:]]), ""
            )
        mutating.append(argv)
        if argv[:3] in {("docker", "rm", "-f"), ("docker", "ps", "-aq")}:
            return subprocess.CompletedProcess(argv, 0, "", "")
        return subprocess.CompletedProcess(argv, 1, "", "stop at first resource command")

    monkeypatch.setattr(sk.subprocess, "run", command)
    return calls, mutating


# The publication replay needs the dispatcher image for the channel gateway.
GATEWAY_IMAGES = (*sk.IMAGES, sk.GATEWAY_IMAGE)


@pytest.mark.parametrize("missing", GATEWAY_IMAGES)
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
    inspected = {
        image for argv in calls if argv[:3] == ("docker", "image", "inspect")
        for image in argv[3:]
    }
    assert sorted(inspected) == sorted(GATEWAY_IMAGES)
    assert mutating, "the run must proceed past the image check"
    assert mutating[0][:2] in {("docker", "run"), ("kind", "create")}
    first_mutation = calls.index(mutating[0])
    assert all(
        calls.index(("docker", "image", "inspect", image)) < first_mutation
        for image in GATEWAY_IMAGES
    )
    assert "stop at first resource command" in _evidence(tmp_path)["error"]



def test_admitted_candidate_map_keeps_image_refs_through_budget_and_import(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, candidate_curie: Path,
) -> None:
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parent))
    from c4_admitted_kind import CANDIDATE_CLI_ENV, CANDIDATE_IMAGES_ENV, AdmittedKind

    monkeypatch.setenv("DOCKER_HOST", "tcp://docker.example.com:2376")

    monkeypatch.setenv(CANDIDATE_CLI_ENV, str(candidate_curie))
    monkeypatch.setenv(CANDIDATE_IMAGES_ENV, "curie-{component}:candidate")
    monkeypatch.setenv("CURIE_LOCAL_STUB_HOST", "192.0.2.10")
    monkeypatch.setattr(sk.shutil, "which", lambda binary: f"/usr/bin/{binary}")
    monkeypatch.setattr(sk.shutil, "disk_usage", lambda _path: type("Usage", (), {"free": 2**40})())
    monkeypatch.setattr(sk, "client_loopback_port_is_free", lambda _port: True)
    run = AdmittedKind(tmp_path, candidate_curie)
    expected = tuple(run.images.values())
    inspected: list[tuple[str, ...]] = []
    imported: list[str] = []

    class ImportsComplete(Exception):
        pass

    def command(argv: tuple[str, ...], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        output = ""
        if argv[:3] == ("docker", "image", "inspect"):
            refs = argv[3:]
            assert all(ref in expected for ref in refs), refs
            inspected.append(refs)
            output = json.dumps([{"Id": "sha256:" + "a" * 64, "Size": 1024} for _ in refs])
        elif argv[:2] == ("docker", "inspect"):
            output = json.dumps([
                {"NetworkSettings": {"Networks": {"kind": {"Gateway": "192.0.2.1"}}}}
            ])
        elif argv[:2] == ("docker", "version"):
            output = "linux/amd64"
        elif argv[:3] == ("kind", "get", "nodes"):
            output = "acme-control-plane"
        elif argv[:2] == ("docker", "exec"):
            output = '[plugins."io.containerd.grpc.v1.cri".containerd]\nsnapshotter="overlayfs"\n'
        elif argv[:3] == ("bash", "-o", "pipefail"):
            imported.append(argv[-1])
            if len(imported) == len(expected):
                raise ImportsComplete
        return subprocess.CompletedProcess(argv, 0, output, "")

    monkeypatch.setattr(sk.subprocess, "run", command)
    with pytest.raises(ImportsComplete):
        run.execute()
    assert expected in inspected
    assert tuple(imported) == expected

def test_failed_publication_evidence_survives_verdict_without_transport_or_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = sk.ScriptedRun(fe.parse_args(["scripted", "--scenario", "publication"]))
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
    run = sk.ScriptedRun(fe.parse_args(["scripted", "--scenario", "publication"]))
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
    run = sk.ScriptedRun(fe.parse_args(["scripted", "--scenario", "publication"]))
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


# Observed Docker image inspect Size values from a fresh main candidate build.
# The dispatcher figure is an estimate of the same order as the API image.
_CANDIDATE_IMAGE_BYTES = {
    "curie-api:local": 716245847,
    "curie-worker:local": 708133868,
    "curie-ui:local": 74690291,
    "curie-runner:latest": 1065367629,
    sk.GATEWAY_IMAGE: 700 * 1024**2,
}


@pytest.mark.parametrize("free_gib", [20, 8])
def test_runtime_disk_budget_reserves_candidate_import_and_workloads(
    free_gib: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sk.shutil, "which", lambda binary: f"/usr/bin/{binary}")
    monkeypatch.setattr(
        sk.shutil, "disk_usage",
        lambda _path: type("Usage", (), {"free": free_gib * 1024**3})(),
    )
    run = sk.ScriptedRun(_replay_args(tmp_path))

    def command(*argv: str, **_kwargs: Any) -> str:
        if argv[:2] == ("kind", "version"):
            return "kind v0.24.0"
        if argv[:3] == ("docker", "image", "inspect"):
            return json.dumps([
                {"Size": _CANDIDATE_IMAGE_BYTES[image]}
                for image in argv[3:]
            ])
        assert argv[:2] in _READ_ONLY_PREREQUISITES
        return ""

    monkeypatch.setattr(run, "command", command)
    if free_gib == 8:
        with pytest.raises(fe.ConfigError, match="free disk"):
            run.prerequisites()
    else:
        run.prerequisites()



def test_image_builds_retain_the_existing_disk_reserve(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sk.shutil, "which", lambda binary: f"/usr/bin/{binary}")
    monkeypatch.setattr(
        sk.shutil, "disk_usage", lambda _path: type("Usage", (), {"free": 20 * 1024**3})(),
    )
    args = _replay_args(tmp_path)
    args.build_images = True
    run = sk.ScriptedRun(args)
    monkeypatch.setattr(run, "command", _refuse_external_command)
    with pytest.raises(fe.ConfigError, match=str(30 * 1024**3)):
        run.prerequisites()


# Hosted candidate sizes at the runner-layer check: the runner is about
# 745 MiB and each other candidate is about 150 MiB. The free-byte figure
# is the hosted runner that refused this stage.
_LAYER_STAGE_IMAGE_BYTES = {
    "curie-api:local": 150 * 1024**2,
    "curie-worker:local": 150 * 1024**2,
    "curie-ui:local": 150 * 1024**2,
    sk.GATEWAY_IMAGE: 150 * 1024**2,
    "curie-runner:latest": 745 * 1024**2,
}
_OBSERVED_HOSTED_FREE_BYTES = 26525540352
_RUNNER_LAYER_BUDGET_BYTES = (
    2 * _LAYER_STAGE_IMAGE_BYTES["curie-runner:latest"] + kind_images.RUNTIME_RESERVE_BYTES
)


def _layer_image_size(image: str) -> int:
    if image in _LAYER_STAGE_IMAGE_BYTES:
        return _LAYER_STAGE_IMAGE_BYTES[image]
    if image.endswith("/curie-runner:latest"):
        return _LAYER_STAGE_IMAGE_BYTES["curie-runner:latest"]
    raise AssertionError(image)


class _TokenResponse:
    def __enter__(self) -> _TokenResponse:
        return self

    def __exit__(self, *_args: object) -> bool:
        return False

    def read(self, _size: int = -1) -> bytes:
        return b'{"token":"example-token"}'


class _PortForward:
    def __init__(self) -> None:
        read_fd, write_fd = os.pipe()
        os.write(write_fd, b"Forwarding from 127.0.0.1:18080 -> 8000\n")
        os.close(write_fd)
        self.stdout = os.fdopen(read_fd, "r")


def _drive_runner_layer(
    free_bytes: int,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[sk.ScriptedRun, list[tuple[str, ...]]]:
    monkeypatch.setattr(
        sk.shutil,
        "disk_usage",
        lambda _path: type("Usage", (), {"free": free_bytes})(),
    )
    monkeypatch.setattr(sk.ssl, "create_default_context", lambda **_kwargs: object())
    monkeypatch.setattr(sk, "urlopen", lambda *_args, **_kwargs: _TokenResponse())
    monkeypatch.setattr(sk.subprocess, "Popen", lambda *_args, **_kwargs: _PortForward())
    monkeypatch.setattr(
        sk.fe,
        "http_json",
        lambda *_args, **_kwargs: (201, {"id": "agent-example"}),
    )
    run = sk.ScriptedRun(_replay_args(tmp_path))
    run.workdir = tmp_path / "work"
    run.workdir.mkdir()
    run.stub = type(
        "Stub",
        (),
        {"ca_file": tmp_path / "ca.pem", "base_url": "https://stub.example.test"},
    )()
    (tmp_path / "ca.pem").write_text("placeholder\n")
    encoded = base64.b64encode(b"example-key").decode()

    def kubectl(*_args: str, **_kwargs: Any) -> str:
        return json.dumps({"data": {"apiKey": encoded}})

    builds: list[tuple[str, ...]] = []

    def command(*argv: str, **_kwargs: Any) -> str:
        if argv[:3] == ("docker", "image", "inspect"):
            return json.dumps([{"Size": _layer_image_size(image)} for image in argv[3:]])
        if len(argv) > 1 and argv[1] == "build" and "--plugin-dir" in argv:
            builds.append(argv)
            return ""
        if len(argv) > 1 and argv[1] == "cluster":
            raise sk.ScriptedScenarioError("layer-admitted")
        raise AssertionError(argv[0:2])

    monkeypatch.setattr(run, "kubectl", kubectl)
    monkeypatch.setattr(run, "command", command)
    return run, builds


@pytest.mark.parametrize(
    ("free_bytes", "admitted"),
    [
        (_OBSERVED_HOSTED_FREE_BYTES, True),
        (_RUNNER_LAYER_BUDGET_BYTES - 1, False),
    ],
    ids=["observed-hosted-free", "below-measured"],
)
def test_runner_layer_disk_budget_follows_the_runner_image(
    free_bytes: int,
    admitted: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    run, builds = _drive_runner_layer(free_bytes, tmp_path, monkeypatch)
    if admitted:
        with pytest.raises(sk.ScriptedScenarioError, match="layer-admitted"):
            run.deploy("localhost:5000")
        assert len(builds) == 1
        err = capsys.readouterr().err
        for stage in ("building the runner layer", "built runner layer"):
            assert (
                f"{stage} disk_free_bytes={free_bytes} "
                f"disk_required_bytes={_RUNNER_LAYER_BUDGET_BYTES}"
            ) in err
    else:
        with pytest.raises(fe.ConfigError, match=str(_RUNNER_LAYER_BUDGET_BYTES)):
            run.deploy("localhost:5000")
        assert builds == []


# Per-agent model lanes and the Slack recorder, offline. Each capability has a
# negative control that fails when the capability is removed.

import scripted_lanes as lanes  # noqa: E402
import scripted_scenario as sso  # noqa: E402

MODEL_SCRIPT = REPO_ROOT / "tools/model-script/model_script.py"
RECORDED = REPO_ROOT / "tools/model-script/transcripts/unitconv-issue.json"
SHA = "0123456789abcdef0123456789abcdef01234567"
REVIEWER = "Reviewer agent."


@pytest.fixture
def model_module() -> Any:
    return sk._module("factory_model_script_lanes", MODEL_SCRIPT)


def _frame(issue: int) -> str:
    return (
        "[platform-sender curie-sender-boundary]\nevent: job\nchannel: github\n"
        "person: none\nrole: scheduled run\n[user-message curie-sender-boundary]\n"
        f"https://192.0.2.1:8443/acme-corp/acme-bot/issues/{issue}\n"
        "[end-user-message curie-sender-boundary]"
    )


def _request(
    issue: int, *, reviewer: bool = False, turns: list[dict[str, Any]] | None = None
) -> bytes:
    system = REVIEWER if reviewer else "Dark factory."
    messages = [{"role": "user", "content": [{"type": "text", "text": _frame(issue)}]}]
    messages += turns or []
    return json.dumps(
        {"system": [{"type": "text", "text": f"Platform preamble.\n\n{system}"}],
         "messages": messages, "stream": True}
    ).encode()


def _tool_turn(output: str) -> list[dict[str, Any]]:
    return [
        {"role": "assistant", "content": [{"type": "tool_use", "id": "toolu_a", "name": "Bash",
                                           "input": {}}]},
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "toolu_a",
                                      "content": output}]},
    ]


def _text(exchange: dict[str, Any] | None) -> str:
    assert exchange is not None
    return json.dumps(exchange["message"])


def _two_agent_router(model_module: Any) -> tuple[Any, lanes.LaneRouter]:
    """The recorded implementation lane and a reviewer lane that shares its lane key."""

    script = model_module.ModelScript(RECORDED)
    recorded = model_module.load_transcript(RECORDED)
    reviewer = [dict(recorded[0], message={"content": [{"type": "text", "text": "review ok"}]})]
    router = lanes.LaneRouter({"reviewer": REVIEWER})
    router.add(lanes.Lane("implementation", lanes.IMPLEMENTATION, 3815, recorded))
    router.add(lanes.Lane("reviewer", "reviewer", 3815, reviewer))
    router.install(script, model_module)
    return script, router


def test_agents_on_one_issue_collapse_without_lanes(model_module: Any) -> None:
    """Negative control: the scripted model alone gives both agents one lane key."""
    plain = model_module.ModelScript(RECORDED)
    try:
        reviewer = plain._match("POST", "/v1/messages", _request(3815, reviewer=True))
        assert "Skill" in _text(reviewer)
    finally:
        plain._server.server_close()


def test_router_answers_each_agent_from_its_own_lane(model_module: Any) -> None:
    script, router = _two_agent_router(model_module)
    try:
        reviewer = script._match("POST", "/v1/messages", _request(3815, reviewer=True))
        assert "review ok" in _text(reviewer)
        implementation = script._match("POST", "/v1/messages", _request(3815))
        assert "Skill" in _text(implementation) and "review ok" not in _text(implementation)
        evidence = router.evidence()
        assert evidence["reviewer"]["served_ordinals"] == [0]
        assert evidence["implementation"]["served_ordinals"] == [0]
        assert evidence["unrouted"] == []
    finally:
        script._server.server_close()


def test_a_second_session_of_a_lane_is_not_answered(model_module: Any) -> None:
    script, router = _two_agent_router(model_module)
    try:
        assert script._match("POST", "/v1/messages", _request(3815, reviewer=True))
        assert script._match("POST", "/v1/messages", _request(3815, reviewer=True)) is None
        assert router.unrouted and router.unrouted[-1]["lane"] == "reviewer"
    finally:
        script._server.server_close()


def test_a_non_default_agent_never_borrows_the_implementation_transcript(
    model_module: Any,
) -> None:
    script, router = _two_agent_router(model_module)
    try:
        assert script._match("POST", "/v1/messages", _request(3999, reviewer=True)) is None
        assert router.unrouted == [{"agent": "reviewer", "issue": None}]
    finally:
        script._server.server_close()


def test_an_answer_carries_the_revision_the_sandbox_printed(model_module: Any) -> None:
    script = model_module.ModelScript(RECORDED)
    recorded = model_module.load_transcript(RECORDED)
    second = next(e for e in recorded if e["ordinal"] == 1)
    placeholder = dict(
        second, message={"content": [{"type": "text", "text": lanes.REVISION_PLACEHOLDER}]}
    )
    router = lanes.LaneRouter({})
    router.add(lanes.Lane("implementation", lanes.IMPLEMENTATION, 3815, [recorded[0], placeholder]))
    router.install(script, model_module)
    try:
        assert script._match("POST", "/v1/messages", _request(3815))
        # No printed commit, no answer: the negative control.
        bare = _tool_turn("Exit code 2\nerror: unknown unit k")
        assert script._match("POST", "/v1/messages", _request(3815, turns=bare)) is None
        assert router.unrouted[-1] == {"lane": "implementation", "missing": "observed_revision"}
        turn = _tool_turn(f"Exit code 2\n{SHA}\nerror: unknown unit k")
        answer = script._match("POST", "/v1/messages", _request(3815, turns=turn))
        assert SHA in _text(answer) and lanes.REVISION_PLACEHOLDER not in _text(answer)
        assert router.evidence()["implementation"]["observed_revisions"] == [SHA]
    finally:
        script._server.server_close()


def test_a_resolver_supplies_the_lane_of_an_issue_the_platform_creates(
    model_module: Any,
) -> None:
    script = model_module.ModelScript(RECORDED)
    recorded = model_module.load_transcript(RECORDED)
    router = lanes.LaneRouter({})
    router.resolvers.append(
        lambda agent, issue: lanes.Lane(
            "implementation-created", agent, issue, lanes.retarget(recorded, 3815, issue)
        )
        if issue == 3904
        else None
    )
    router.install(script, model_module)
    try:
        answer = script._match("POST", "/v1/messages", _request(3904))
        assert "issues/3904" in _text(answer) and "3815" not in _text(answer)
        assert router.lanes[(lanes.IMPLEMENTATION, 3904)].served == [0]
        # Another issue has no resolver: it falls back to the recorded transcript.
        assert "Skill" in _text(script._match("POST", "/v1/messages", _request(3815)))
    finally:
        script._server.server_close()


def test_retarget_rewrites_whole_issue_numbers_only() -> None:
    exchanges = [{"lane_key": "issue 3815 port 38150", "ordinal": 0,
                  "message": {"content": [{"type": "text", "text": "Closes #3815"}]}}]
    (moved,) = lanes.retarget(exchanges, 3815, 3904)
    assert moved["lane_key"] == "issue 3904 port 38150"
    assert moved["message"]["content"][0]["text"] == "Closes #3904"
    with pytest.raises(lanes.LaneError):
        lanes.retarget(exchanges, 1234, 3904)


def _slack_call(recorder: lanes.SlackRecorder, method: str, body: bytes, kind: str) -> Any:
    request = Request(
        recorder.base_url + method, data=body, method="POST",
        headers={"Content-Type": kind, "Authorization": "Bearer xoxb-example"},
    )
    with urlopen(request, timeout=5) as response:
        return json.load(response)


def test_slack_recorder_records_posts_and_answers_like_slack() -> None:
    recorder = lanes.SlackRecorder()
    recorder.start()
    try:
        root = _slack_call(recorder, "chat.postMessage",
                           json.dumps({"channel": sso.SLACK_CHANNEL, "text": "root"}).encode(),
                           "application/json")
        assert root["ok"] and root["channel"] == sso.SLACK_CHANNEL
        reply = _slack_call(
            recorder, "chat.postMessage",
            f"channel={sso.SLACK_CHANNEL}&text=line&thread_ts={root['ts']}".encode(),
            "application/x-www-form-urlencoded",
        )
        assert reply["ts"] != root["ts"]
        _slack_call(recorder, "auth.test", b"{}", "application/json")
        posts = recorder.posts()
        assert [(p.text, p.thread_ts) for p in posts] == [("root", None), ("line", root["ts"])]
        assert recorder.unauthenticated == 0
        assert "xoxb-example" not in json.dumps(recorder.calls)
    finally:
        recorder.close()


def test_slack_recorder_negative_control_counts_only_message_posts() -> None:
    recorder = lanes.SlackRecorder()
    recorder.start()
    try:
        _slack_call(recorder, "conversations.info", b"{}", "application/json")
        assert recorder.posts() == []
        assert [call["method"] for call in recorder.calls] == ["conversations.info"]
    finally:
        recorder.close()


def test_scripted_values_route_slack_to_the_recorder_only_when_asked() -> None:
    common = dict(model_base_url="http://192.0.2.1:9", github_api="https://192.0.2.1:8/api/v3",
                  clone_base="https://192.0.2.1:8", ca_configmap="ca", runner_image="r")
    values = sk.scripted_values(**common, slack_api_base_url="http://192.0.2.1:7/api/")
    assert values["worker"]["slackApiBaseUrl"] == "http://192.0.2.1:7/api/"
    assert values["dispatcher"]["slack"]["botToken"] == sk.SCRIPTED_SLACK_BOT_TOKEN
    assert values["dispatcher"]["deploy"] is False
    plain = sk.scripted_values(**common)
    assert "slackApiBaseUrl" not in plain["worker"]
    assert "slack" not in plain["dispatcher"]

# The channel gateway runs on the dispatcher image, so a run that enables it
# builds, inspects and imports that image into its cluster.


def test_enabled_gateway_builds_and_imports_the_dispatcher_image_the_chart_names(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    args = _replay_args(tmp_path)
    args.build_images = True
    run = sk.ScriptedRun(args)
    assert run.gateway_enabled()
    values = sk.scripted_values(
        model_base_url="http://192.0.2.1:9",
        github_api="https://192.0.2.1:8443/api/v3",
        clone_base="https://192.0.2.1:8443",
        ca_configmap="ca",
        runner_image="localhost:5000/curie-runner",
        slack_api_base_url="http://192.0.2.1:7/api/",
    )
    assert values["dispatcher"]["slack"]["botToken"], "the gateway is enabled"
    image = values["dispatcher"]["image"]
    chart_image = f"{image['repository']}:{image['tag']}"
    assert chart_image == sk.GATEWAY_IMAGE

    monkeypatch.setattr(sk.shutil, "which", lambda binary: f"/usr/bin/{binary}")
    monkeypatch.setattr(sk.ScriptedRun, "check_disk", lambda _self, _stage, **_kwargs: None)
    monkeypatch.setattr(sk.tempfile, "mkdtemp", lambda **_kwargs: str(tmp_path / "work"))
    monkeypatch.setattr(run, "start_registry", lambda: (5000, "localhost:5000"))
    (tmp_path / "work").mkdir()
    builds: dict[str, str] = {}
    inspected: set[str] = set()
    imported: list[str] = []

    def command(*argv: str, **_kwargs: Any) -> str:
        if argv[:2] == ("kind", "version"):
            return "kind v0.24.0"
        if argv[:3] == ("docker", "image", "inspect"):
            inspected.update(argv[3:])
            return json.dumps([{"Size": 1024} for _ in argv[3:]])
        if argv[:2] == ("docker", "build"):
            builds[argv[argv.index("-t") + 1]] = argv[argv.index("-f") + 1]
        if argv[:2] == ("docker", "inspect"):
            return json.dumps(
                [{"NetworkSettings": {"Networks": {"kind": {"Gateway": "172.18.0.1"}}}}]
            )
        return ""

    def load_images(_command: Any, cluster: str, images: Any) -> None:
        assert cluster == run.cluster
        imported.extend(images)
        raise sk.ScriptedScenarioError("imported")

    monkeypatch.setattr(run, "command", command)
    monkeypatch.setattr(sk.kind_images, "load_images", load_images)
    with pytest.raises(sk.ScriptedScenarioError, match="imported"):
        run.execute()
    assert builds[chart_image] == "apps/dispatcher/Dockerfile"
    assert chart_image in inspected
    assert chart_image in imported


def _gateway_argv(scenario: str, *extra: str) -> Any:
    return fe.parse_args(["scripted", "--scenario", scenario, *extra])


def test_gateway_runs_only_on_the_publication_install() -> None:
    publication = sk.ScriptedRun(_gateway_argv("publication"))
    assert publication.gateway_enabled()
    assert sk.GATEWAY_IMAGE in publication.image_refs

    class NoSlack(sk.ScriptedRun):
        SLACK_PROGRESS = False

    for run in (
        sk.ScriptedRun(_gateway_argv("facts")),
        sk.ScriptedRun(_gateway_argv("jira-one-ticket")),
        sk.ScriptedRun(_gateway_argv("publication", "--record")),
        NoSlack(_gateway_argv("publication")),
    ):
        assert not run.gateway_enabled()
        assert sk.GATEWAY_IMAGE not in run.image_refs


def test_every_subclass_that_drives_its_own_scenario_runs_no_gateway(tmp_path: Path) -> None:
    """A subclass left on the default would start the gateway on its own install."""

    from c4_admitted_kind import AdmittedKind
    from test_c4_held_cancellation import HeldRun

    assert AdmittedKind(tmp_path, Path("curie")).gateway_enabled() is False
    held = HeldRun.__new__(HeldRun)
    held.args = _gateway_argv("publication")
    assert held.gateway_enabled() is False


def _post(method: str, text: str | None, *, thread_ts: str | None, ts: str,
          channel: str = sso.SLACK_CHANNEL) -> lanes.SlackPost:
    return lanes.SlackPost(method, channel, text, thread_ts, ts)


PULL_REQUEST = "https://192.0.2.1:8443/acme-corp/acme-bot/pull/2"
ROOT_TEXT = sso.SLACK_ROOT_PREFIX + ": started"


def _slack_run(
    ready: str | None = "root", ready_channel: str = sso.SLACK_CHANNEL, extra_card: bool = False
) -> list[lanes.SlackPost]:
    """Posts as a real SlackRecorder holds them: one root, one card, a ready reply.

    ``ready`` is the thread the ready reply lands in: ``root`` (correct), another
    thread's ts, or None for no ready reply at all.
    """

    recorder = lanes.SlackRecorder()
    recorder.start()

    def call(method: str, body: dict[str, Any], channel: str = sso.SLACK_CHANNEL) -> Any:
        body = {"channel": channel, **body}
        return _slack_call(recorder, method, json.dumps(body).encode(), "application/json")

    try:
        root = call("chat.postMessage", {"text": ROOT_TEXT})
        card = call("chat.postMessage", {"text": "card", "thread_ts": root["ts"]})
        call("chat.update", {"ts": card["ts"], "text": "card, finished"})
        # Another scenario's thread: ignored.
        call("chat.postMessage", {"text": "Factory run: other", "thread_ts": "9.9"})
        if extra_card:
            call("chat.postMessage", {"text": "card again", "thread_ts": root["ts"]})
        if ready is not None:
            call("chat.postMessage", {
                "text": f"Opened {PULL_REQUEST}",
                "thread_ts": root["ts"] if ready == "root" else ready,
            }, ready_channel)
        return recorder.posts()
    finally:
        recorder.close()


def test_slack_progress_accepts_root_card_and_ready() -> None:
    counts = sso.assert_slack_progress(_slack_run(), sso.SLACK_CHANNEL, PULL_REQUEST)
    assert counts == {"root": 1, "card": 1, "ready": 1}


def test_slack_progress_refuses_a_missing_or_misthreaded_ready_post() -> None:
    for posts in (_slack_run(ready=None), _slack_run(ready="9.9")):
        with pytest.raises(sso.ScenarioAssertionError, match="ready reply"):
            sso.assert_slack_progress(posts, sso.SLACK_CHANNEL, PULL_REQUEST)


class _RecordedSlack:
    """Stands in for the run's recorder with the posts a real SlackRecorder held."""

    def __init__(self, posts: list[lanes.SlackPost]) -> None:
        self._posts = posts

    def posts(self) -> list[lanes.SlackPost]:
        return self._posts


def _publication_run(posts: list[lanes.SlackPost]) -> sk.ScriptedRun:
    run = sk.ScriptedRun(_gateway_argv("publication"))
    run.slack = _RecordedSlack(posts)  # type: ignore[assignment]
    run.evidence["pull_request"] = {"html_url": PULL_REQUEST}
    return run


def test_the_recorder_stamps_each_post_with_its_receive_time() -> None:
    before = time.monotonic()
    posts = _slack_run()
    after = time.monotonic()
    assert posts and all(before <= post.received_at <= after for post in posts)


def test_the_run_records_the_slack_post_counts_in_its_evidence() -> None:
    run = _publication_run(_slack_run())
    run.wait_for_slack_progress()
    assert run.evidence["slack_posts"] == {"root": 1, "card": 1, "ready": 1}


def test_the_run_fails_when_the_ready_reply_lands_outside_the_root_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sk, "SLACK_PROGRESS_SECONDS", 0)
    run = _publication_run(_slack_run(ready="9.9"))
    with pytest.raises(sk.ScriptedScenarioError, match="Slack progress: .*ready reply"):
        run.wait_for_slack_progress()
    assert "slack_posts" not in run.evidence


def test_a_run_without_the_gateway_does_not_wait_for_slack() -> None:
    run = sk.ScriptedRun(_gateway_argv("jira-one-ticket"))
    run.wait_for_slack_progress()
    assert "slack_posts" not in run.evidence


def test_the_run_binds_the_factory_agent_to_the_slack_channel_before_the_label(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = _publication_run([])
    run.evidence["agent_id"] = "agent-example"
    run.env.update(CURIE_API_URL="http://api.example.com", CURIE_API_KEY="example-key")
    calls: list[tuple[str, str, Any]] = []

    def http_json(method: str, url: str, **kwargs: Any) -> tuple[int, dict[str, Any]]:
        calls.append((method, url, kwargs["body"]))
        return 200, {}

    monkeypatch.setattr(fe, "http_json", http_json)
    monkeypatch.setattr(run, "kubectl", lambda *_args, **_kwargs: "")
    run.bind_slack()
    assert calls == [
        ("POST", "http://api.example.com/agents/agent-example/channels",
         {"kind": "slack", "address": sso.SLACK_CHANNEL}),
        ("PATCH", "http://api.example.com/agents/agent-example",
         {"factory_slack_channel": sso.SLACK_CHANNEL}),
    ]
    monkeypatch.setattr(fe, "http_json", lambda *_a, **_k: (409, {}))
    with pytest.raises(sk.ScriptedScenarioError, match="Slack binding returned HTTP 409"):
        run.bind_slack()


def test_slack_progress_refuses_a_ready_reply_in_another_channel_or_a_second_card() -> None:
    other = "C0EXAMPLE8"
    with pytest.raises(sso.ScenarioAssertionError, match="ready reply"):
        sso.assert_slack_progress(
            _slack_run(ready_channel=other), sso.SLACK_CHANNEL, PULL_REQUEST
        )
    with pytest.raises(sso.ScenarioAssertionError, match="one card"):
        sso.assert_slack_progress(_slack_run(extra_card=True), sso.SLACK_CHANNEL, PULL_REQUEST)


def test_slack_progress_ignores_an_update_of_the_card_in_another_channel() -> None:
    posts = [
        lanes.SlackPost(p.method, "C0EXAMPLE8" if p.method == "chat.update" else p.channel,
                        p.text, p.thread_ts, p.ts)
        for p in _slack_run()
    ]
    with pytest.raises(sso.ScenarioAssertionError, match="never updated"):
        sso.assert_slack_progress(posts, sso.SLACK_CHANNEL, PULL_REQUEST)


def _received_at(posts: list[lanes.SlackPost], seconds: float) -> list[lanes.SlackPost]:
    """The same posts as the recorder received them ``seconds`` after the final comment."""

    return [dataclasses.replace(post, received_at=seconds) for post in posts]


def _clock(monkeypatch: pytest.MonkeyPatch, now: float) -> None:
    monkeypatch.setattr(
        sk, "time", type("Clock", (), {"monotonic": staticmethod(lambda: now),
                                       "sleep": staticmethod(lambda _s: None)})
    )


def test_a_settled_result_after_the_bound_is_late(monkeypatch: pytest.MonkeyPatch) -> None:
    run = _publication_run(_received_at(_slack_run(), 181.0))
    run.final_comment_at = 0.0
    _clock(monkeypatch, 182.0)
    with pytest.raises(sk.ScriptedScenarioError, match="settled after 180 seconds"):
        run.wait_for_slack_progress()
    assert "slack_posts" not in run.evidence


def test_posts_received_on_time_pass_however_late_the_run_looks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The recorder had everything one second after the final comment; the check ran at 181."""

    run = _publication_run(_received_at(_slack_run(), 1.0))
    run.final_comment_at = 0.0
    _clock(monkeypatch, 181.0)
    run.wait_for_slack_progress()
    assert run.evidence["slack_posts"] == {"root": 1, "card": 1, "ready": 1}


def test_identical_posts_are_judged_by_their_receive_time_not_the_check_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for received, passes in ((179.0, True), (181.0, False)):
        run = _publication_run(_received_at(_slack_run(), received))
        run.final_comment_at = 0.0
        _clock(monkeypatch, 400.0)
        if passes:
            run.wait_for_slack_progress()
            assert "slack_posts" in run.evidence
        else:
            with pytest.raises(sk.ScriptedScenarioError, match="settled after 180 seconds"):
                run.wait_for_slack_progress()


def test_the_bound_starts_at_the_final_comment_not_at_the_slack_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = _publication_run(_received_at(_slack_run(), 190.0))
    run.final_comment_at = 0.0
    _clock(monkeypatch, 190.0)
    with pytest.raises(sk.ScriptedScenarioError, match="settled after 180 seconds"):
        run.wait_for_slack_progress()


def test_a_duplicate_card_that_arrives_after_the_first_check_fails_the_run() -> None:
    run = _publication_run(_slack_run())
    run.wait_for_slack_progress()
    assert run.evidence["slack_posts"] == {"root": 1, "card": 1, "ready": 1}
    run.slack = _RecordedSlack(_slack_run(extra_card=True))  # type: ignore[assignment]
    with pytest.raises(sk.ScriptedScenarioError, match="end of the run: .*one card"):
        run.verify_slack_progress()


def test_the_publication_install_fails_without_its_recorder() -> None:
    run = sk.ScriptedRun(_gateway_argv("publication"))
    run.evidence["pull_request"] = {"html_url": PULL_REQUEST}
    with pytest.raises(sk.ScriptedScenarioError, match="recorder did not start"):
        run.wait_for_slack_progress()
    with pytest.raises(sk.ScriptedScenarioError, match="recorder did not start"):
        run.bind_slack()


def test_the_publication_binds_slack_then_labels_then_waits_then_verifies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = sk.ScriptedRun(_gateway_argv("publication"))
    calls: list[str] = []
    monkeypatch.setattr(run, "bind_slack", lambda: calls.append("bind"))
    monkeypatch.setattr(run, "label_issue", lambda _github: calls.append("label"))
    monkeypatch.setattr(run, "wait_for_final", lambda: calls.append("wait"))
    monkeypatch.setattr(run, "observe", lambda: calls.append("observe"))
    monkeypatch.setattr(run, "wait_for_slack_progress", lambda: calls.append("slack"))
    monkeypatch.setattr(run, "verify_slack_progress", lambda: calls.append("slack-final"))
    monkeypatch.setattr(run, "run_one_ticket", lambda _github: calls.append("one-ticket"))
    run.router = lanes.LaneRouter({})
    run.run_publication(None)
    assert calls == ["bind", "label", "wait", "observe", "slack", "one-ticket", "slack-final"]


def test_slack_progress_refuses_a_card_that_was_never_updated() -> None:
    posts = [p for p in _slack_run() if p.method != "chat.update"]
    with pytest.raises(sso.ScenarioAssertionError, match="never updated"):
        sso.assert_slack_progress(posts, sso.SLACK_CHANNEL, PULL_REQUEST)


def test_slack_progress_refuses_a_second_root_post() -> None:
    posts = _slack_run()
    posts.append(_post("chat.postMessage", ROOT_TEXT, thread_ts=None, ts="7.7"))
    with pytest.raises(sso.ScenarioAssertionError, match="one Slack root post"):
        sso.assert_slack_progress(posts, sso.SLACK_CHANNEL, PULL_REQUEST)


# one-ticket (#5788): the scenario runs on every replayed install, and its
# verdict is the evidence file's ``one_ticket.result``.

import one_ticket_run  # noqa: E402
import one_ticket_scenario as ots  # noqa: E402


@pytest.mark.parametrize("outcome", ["passed", "failed"])
def test_the_one_ticket_verdict_reaches_the_evidence_file_and_the_exit_code(
    outcome: str, candidate_curie: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)

    def one_ticket_execute(self: one_ticket_run.OneTicket, _github: Any) -> None:
        if outcome == "failed":
            raise ots.OneTicketAssertionError("a second request after the bot comment")
        self.evidence["result"] = "passed"

    def execute(run: sk.ScriptedRun) -> None:
        run.router = lanes.LaneRouter({})
        run.run_one_ticket(None)

    monkeypatch.setattr(one_ticket_run.OneTicket, "execute", one_ticket_execute)
    monkeypatch.setattr(sk.ScriptedRun, "execute", execute)

    assert sk.main(_replay_args(tmp_path)) == (0 if outcome == "passed" else fe.EXIT_FAILED)
    evidence = _evidence(tmp_path)
    assert evidence["one_ticket"]["result"] == outcome
    assert evidence["result"] == outcome
    if outcome == "failed":
        assert "second request after the bot comment" in evidence["error"]
        assert evidence["one_ticket"]["error"] == "a second request after the bot comment"


def test_the_scripted_command_fails_when_the_one_ticket_scenario_did_not_run(
    candidate_curie: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sk.ScriptedRun, "execute", lambda _run: None)

    assert sk.main(_replay_args(tmp_path)) == fe.EXIT_FAILED

    evidence = _evidence(tmp_path)
    assert evidence["result"] == "failed"
    assert evidence["error"] == "the one-ticket scenario did not run"
    assert "one_ticket" not in evidence


def test_the_scripted_command_fails_when_the_one_ticket_scenario_did_not_pass(
    candidate_curie: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)

    def execute(run: sk.ScriptedRun) -> None:
        run.one_ticket = one_ticket_run.OneTicket(run, lanes.LaneRouter({}))

    monkeypatch.setattr(sk.ScriptedRun, "execute", execute)

    assert sk.main(_replay_args(tmp_path)) == fe.EXIT_FAILED
    assert _evidence(tmp_path)["error"] == "the one-ticket scenario did not pass"


@pytest.mark.parametrize(
    ("argv", "required"),
    [
        (["scripted", "--scenario", "publication"], True),
        (["scripted", "--scenario", "publication", "--record"], False),
        (["scripted", "--scenario", "facts"], False),
    ],
)
def test_the_one_ticket_verdict_is_owed_by_every_replay_of_an_install_and_no_other_run(
    argv: list[str], required: bool
) -> None:
    run = sk.ScriptedRun(fe.parse_args(argv))

    assert run.one_ticket_required() is required
    assert (run.one_ticket_verdict_error() is not None) is required


def test_the_scenarios_after_the_publication_scenario_call_the_one_ticket_scenario(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The call site itself: removing the call from ``run_scenarios`` fails this test."""

    run = sk.ScriptedRun(fe.parse_args(["scripted", "--scenario", "publication"]))
    run.router = lanes.LaneRouter({})
    calls: list[str] = []
    monkeypatch.setattr(run, "observe", lambda: calls.append("observe"))
    monkeypatch.setattr(run, "wait_for_slack_progress", lambda: calls.append("slack"))
    monkeypatch.setattr(run, "verify_slack_progress", lambda: calls.append("slack-final"))
    monkeypatch.setattr(run, "run_one_ticket", lambda _github: calls.append("one-ticket"))

    run.run_scenarios(None)

    assert calls == ["observe", "slack", "one-ticket", "slack-final"]


def test_a_fixture_that_owns_its_agents_does_not_run_the_one_ticket_scenario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from c4_admitted_kind import AdmittedKind

    run = AdmittedKind(tmp_path, Path("curie"))
    run.router = lanes.LaneRouter({})
    calls: list[str] = []
    monkeypatch.setattr(run, "observe", lambda: calls.append("observe"))
    monkeypatch.setattr(run, "run_one_ticket", lambda _github: calls.append("one-ticket"))

    run.run_scenarios(None)

    assert calls == ["observe"]
    assert run.one_ticket_required() is False


def test_only_the_scripted_command_runs_the_one_ticket_scenario_on_its_install() -> None:
    # The c4 fixtures subclass ScriptedRun to provision their own agents and
    # drive their own tickets; the one-ticket flow needs the agent execute makes
    # and a free fixture repository, so it is theirs to skip.
    from c4_admitted_kind import AdmittedKind
    from test_c4_held_cancellation import HeldRun

    assert sk.ScriptedRun.runs_one_ticket is True
    assert AdmittedKind.runs_one_ticket is False
    assert HeldRun.runs_one_ticket is False


def test_the_publication_scenario_installs_the_one_ticket_lane_and_the_gateway() -> None:
    run = sk.ScriptedRun(fe.parse_args(["scripted", "--scenario", "publication"]))
    assert run.gateway_enabled()
    assert run.one_ticket is None
    model = sk._module("factory_model_script_one_ticket", MODEL_SCRIPT)
    lane = one_ticket_run.build_lane(model)
    assert (lane.agent, lane.issue) == (lanes.IMPLEMENTATION, ots.ISSUE)
    assert lane.exchanges, "the transcript holds both sessions"


# The learned rules chain (#5799): its configuration, its driver and its recording.

import learned_rules_lanes as lrl  # noqa: E402
import learned_rules_run as lrr  # noqa: E402


def _chain_transcripts(tmp_path: Path, *, skip: str | None = None) -> Path:
    first = tmp_path / "chain" / "learned-rules-chain-a.json"
    first.parent.mkdir(exist_ok=True)
    for letter, path in lrl.transcript_paths(first).items():
        if letter != skip:
            path.write_text(json.dumps({"version": 1, "exchanges": []}))
    return first


def _chain_args(transcript: Path, *extra: str) -> Any:
    return fe.parse_args(
        ["scripted", "--scenario", "learned-rules", "--transcript", str(transcript), *extra]
    )


@pytest.mark.parametrize("missing", ["b", "c"])
def test_learned_rules_replay_without_a_sibling_refuses_before_any_external_command(
    missing: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sk.subprocess, "run", _refuse_external_command)
    first = _chain_transcripts(tmp_path, skip=missing)

    assert sk.main(_chain_args(first)) == fe.EXIT_CONFIG
    assert f"learned-rules-chain-{missing}.json is missing" in capsys.readouterr().err


def test_learned_rules_replay_refuses_a_malformed_sibling_and_an_unpaired_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sk.subprocess, "run", _refuse_external_command)
    first = _chain_transcripts(tmp_path)
    first.with_name("learned-rules-chain-c.json").write_text('{"version": 1, "exchanges": [')
    assert sk.main(_chain_args(first)) == fe.EXIT_CONFIG
    assert "learned-rules-chain-c.json is malformed" in capsys.readouterr().err

    unpaired = tmp_path / "chain" / "transcript.json"
    unpaired.write_text(json.dumps({"version": 1, "exchanges": []}))
    assert sk.main(_chain_args(unpaired)) == fe.EXIT_CONFIG
    assert "is not named <stem>-a.json" in capsys.readouterr().err


def test_learned_rules_replay_with_all_three_transcripts_passes_configuration(
    tmp_path: Path,
) -> None:
    sk.validate_configuration(_chain_args(_chain_transcripts(tmp_path)))


def test_learned_rules_may_record_with_the_key_and_writes_the_chain_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CURIE_FACTORY_RECORD_KEY", "sk-or-example")
    recording = fe.parse_args(["scripted", "--scenario", "learned-rules", "--record"])
    sk.validate_configuration(recording)
    run = lrr.LearnedRulesRun(recording)
    assert run.transcripts["a"] == lrr.RECORD_TARGET
    assert run.transcripts["b"].name == "learned-rules-chain-b.json"
    # A named target must be an A file, since the B and C files sit beside it.
    sk.validate_configuration(_chain_args(tmp_path / "x-a.json", "--record"))
    with pytest.raises(fe.ConfigError, match="<stem>-a.json"):
        sk.validate_configuration(_chain_args(tmp_path / "x.json", "--record"))


def test_learned_rules_recording_without_the_key_refuses_before_any_external_command(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("CURIE_FACTORY_RECORD_KEY", raising=False)
    monkeypatch.setattr(sk.subprocess, "run", _refuse_external_command)
    args = fe.parse_args(["scripted", "--scenario", "learned-rules", "--record"])
    assert sk.main(args) == fe.EXIT_CONFIG
    assert "CURIE_FACTORY_RECORD_KEY" in capsys.readouterr().err


def test_main_runs_the_learned_rules_scenario_in_its_own_driver(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)
    ran: list[type] = []

    def execute(run: sk.ScriptedRun) -> None:
        ran.append(type(run))

    monkeypatch.setattr(sk.ScriptedRun, "execute", execute)
    assert sk.main(_chain_args(_chain_transcripts(tmp_path))) == 0
    assert ran == [lrr.LearnedRulesRun]
    evidence = _evidence(tmp_path)
    assert evidence["learned_rules"]["scenario"] == "learned-rules"


def test_the_learned_rules_scenario_owes_no_one_ticket_verdict_and_the_others_still_do(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    chain = lrr.LearnedRulesRun(_chain_args(_chain_transcripts(tmp_path)))
    assert chain.one_ticket_required() is False
    assert chain.one_ticket_verdict_error() is None
    assert not chain.gateway_enabled() and sk.GATEWAY_IMAGE not in chain.image_refs
    assert chain.router_fallback() is False
    run = sk.ScriptedRun(_replay_args(tmp_path, scenario="publication"))
    assert run.one_ticket_required() is True
    assert run.one_ticket_verdict_error() == "the one-ticket scenario did not run"
    monkeypatch.setenv("CURIE_FACTORY_RECORD_KEY", "sk-or-example")
    recording = fe.parse_args(["scripted", "--scenario", "publication", "--record"])
    assert sk.ScriptedRun(recording).one_ticket_required() is False


def test_the_default_hooks_route_the_existing_scenarios_as_before(tmp_path: Path) -> None:
    model = sk._module("factory_model_script_hooks", MODEL_SCRIPT)
    run = sk.ScriptedRun(_replay_args(tmp_path))
    assert run.model_options(model) == {}
    assert run.router_fallback() is True
    router = run.make_router(model)
    assert router is not None and router.agent_markers == {}
    assert [lane.name for lane in router.lanes.values()] == ["one-ticket"]
    values: dict[str, Any] = {"api": {"extraEnv": [{"name": "X", "value": "1"}]}}
    run.extend_install_values(values)
    assert values == {"api": {"extraEnv": [{"name": "X", "value": "1"}]}}


def test_learned_rules_install_values_add_the_oidc_settings_by_secret_reference(
    tmp_path: Path,
) -> None:
    run = lrr.LearnedRulesRun(_chain_args(_chain_transcripts(tmp_path)))
    values: dict[str, Any] = {
        "api": {"extraEnv": [{"name": "KEEP", "value": "1"}, {"name": "CURIE_OIDC_ISSUER"}]}
    }
    run.extend_install_values(values)
    env = {entry["name"]: entry for entry in values["api"]["extraEnv"]}
    assert env["KEEP"] == {"name": "KEEP", "value": "1"}
    assert env["CURIE_OIDC_ISSUER"]["value"] == run.issuer
    assert run.issuer.endswith(".svc.cluster.local:5556/dex")
    assert env["CURIE_OIDC_CLIENT_SECRET"] == {
        "name": "CURIE_OIDC_CLIENT_SECRET",
        "valueFrom": {"secretKeyRef": {"name": lrr.OIDC_SECRET, "key": lrr.OIDC_SECRET_KEY}},
    }
    assert env["CURIE_OIDC_REDIRECT_URI"]["value"] == run.redirect_uri
    assert env["CURIE_OIDC_ADMIT_ALL_AUTHENTICATED"]["value"] == "true"
    assert len([e for e in values["api"]["extraEnv"] if e["name"] == "CURIE_OIDC_ISSUER"]) == 1


class _RecordedStub:
    host = "192.0.2.10"
    port = 34567
    base_url = "https://192.0.2.10:34567"


class _RecordedModel:
    def __init__(self, exchanges: list[dict[str, Any]], tags: list[str | None]) -> None:
        self._exchanges = exchanges
        self.recorded_tags = tags
        self._lock = threading.Lock()


def _recorded(lane: str, text: str) -> dict[str, Any]:
    return {
        "lane_key": lane,
        "ordinal": 0,
        "message": {"type": "message", "content": [{"type": "text", "text": text}]},
    }


def test_a_learned_rules_recording_is_split_by_ticket_and_scrubbed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CURIE_FACTORY_RECORD_KEY", "sk-or-example")
    first = tmp_path / "chain" / "learned-rules-chain-a.json"
    run = lrr.LearnedRulesRun(_chain_args(first, "--record"))
    run.stub = _RecordedStub()
    run.model = _RecordedModel(
        [
            _recorded("a0", "cloned https://192.0.2.10:34567/acme-corp/acme-bot.git"),
            _recorded("b0", "host 192.0.2.10:34567"),
            _recorded("a1", "pushed to 192.0.2.10"),
            _recorded("c0", "done"),
        ],
        ["a", "b", "a", "c"],
    )
    # A committed A file from an earlier recording; ModelScript dumps elsewhere.
    first.parent.mkdir()
    first.write_text(json.dumps({"version": 1, "exchanges": [{"stale": True}]}))
    run.workdir = tmp_path / "work"
    assert run.model_transcript() == run.workdir / "recording-raw.json"
    assert run.model_transcript() not in run.transcripts.values()
    run.write_recording()

    model = sk._module("factory_model_script_split", MODEL_SCRIPT)
    written = {letter: model.load_transcript(path) for letter, path in run.transcripts.items()}
    assert [e["lane_key"] for e in written["a"]] == ["a0", "a1"]
    assert [e["lane_key"] for e in written["b"]] == ["b0"]
    assert [e["lane_key"] for e in written["c"]] == ["c0"]
    texts = json.dumps(written)
    assert "192.0.2.10" not in texts
    assert "https://stub.example.test/acme-corp/acme-bot.git" in texts
    assert "host stub.example.test" in texts
    assert run.evidence["recorded_exchanges"] == {"a": 2, "b": 1, "c": 1}

    committed = {letter: path.read_text() for letter, path in run.transcripts.items()}
    run.model = _RecordedModel([_recorded("x", "cloned 192.0.2.10:34567")], [None])
    with pytest.raises(lanes.LaneError, match="no known ticket tag"):
        run.write_recording()
    # A failed split leaves every committed file as it was.
    assert {letter: path.read_text() for letter, path in run.transcripts.items()} == committed


@pytest.mark.parametrize(
    ("exchanges", "tags", "passed"),
    [
        # A failure after ticket A's model calls: the split alone would succeed and
        # replace B and C with empty transcripts.
        ([_recorded("a0", "one"), _recorded("a1", "two")], ["a", "a"], False),
        # A failure after the model was set up and before its first call.
        ([], [], False),
        # The control: a chain the judge passed writes all three files.
        (
            [_recorded("a0", "one"), _recorded("b0", "two"), _recorded("c0", "three")],
            list("abc"),
            True,
        ),
    ],
    ids=["failed-after-a", "failed-before-first-call", "passed-chain-writes"],
)
def test_cleanup_replaces_the_replay_fixtures_only_after_a_passed_chain(
    exchanges: list[dict[str, Any]],
    tags: list[str | None],
    passed: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CURIE_FACTORY_RECORD_KEY", "sk-or-example")
    # The cluster, registry and endpoint teardown is not under test here.
    monkeypatch.setattr(sk.ScriptedRun, "cleanup", lambda _self: [])
    first = tmp_path / "chain" / "learned-rules-chain-a.json"
    first.parent.mkdir()
    run = lrr.LearnedRulesRun(_chain_args(first, "--record"))
    for path in run.transcripts.values():
        path.write_text(json.dumps({"version": 1, "exchanges": [{"committed": path.name}]}))
    committed = {letter: path.read_text() for letter, path in run.transcripts.items()}
    run.stub = _RecordedStub()
    run.model = _RecordedModel(list(exchanges), list(tags))
    run.workdir = tmp_path / "work"
    if passed:
        run.chain["result"] = "passed"
    run.cleanup()
    after = {letter: path.read_text() for letter, path in run.transcripts.items()}
    assert (after != committed) is passed
    if passed:
        assert [e["lane_key"] for e in json.loads(after["b"])["exchanges"]] == ["b0"]
