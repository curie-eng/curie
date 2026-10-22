"""Offline checks for the live daemon lifecycle driver (ADR 0216, ADR 0217)."""

from __future__ import annotations

import copy
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import daemon_lifecycle
import pytest

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "fixtures" / "daemon-lifecycle"
MODEL_SCRIPT = ROOT.parent / "model-script" / "model_script.py"
NONCE = "0123abcd"
TURNS = ["success", "daemon_death", "retained_next", "cancel", "worker_loss"]
ROLES = ["daemon", "service", "ingress", "server-secret", "client-secret"]
VALID_ARGV = [
    "--context", "ctx",
    "--namespace", f"fd-proof-{NONCE}",
    "--daemon-namespace", f"fd-proof-d-{NONCE}",
    "--release", f"fd-proof-{NONCE}",
    "--candidate", "HEAD",
    "--work-dir", f".projects/daemon-proof-{NONCE}",
]  # fmt: skip
VALID_ENV = {"CURIE_LOCAL_STUB_HOST": "192.0.2.10"}


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _load_kind():
    return _load("curie_scripted_kind", ROOT / "scripted_kind.py")


@pytest.fixture
def envelope() -> dict[str, Any]:
    return copy.deepcopy(json.loads((FIXTURES / "passing-envelope.json").read_text()))


def _reasons(envelope: dict[str, Any]) -> list[str]:
    return daemon_lifecycle.judge(envelope)


def _assert_token(envelope: dict[str, Any], token: str) -> None:
    reasons = _reasons(envelope)
    assert any(token in reason for reason in reasons), (token, reasons)


def test_passing_envelope_judges_clean(envelope: dict[str, Any]) -> None:
    assert _reasons(envelope) == []


def test_judge_ignores_result_field(envelope: dict[str, Any]) -> None:
    envelope["result"] = "failed"
    assert _reasons(envelope) == []


def test_wrong_schema(envelope: dict[str, Any]) -> None:
    envelope["schema"] = "daemon-lifecycle/v0"
    _assert_token(envelope, "schema")


# identity


def test_runner_docker_id_differs_from_daemon_id(envelope: dict[str, Any]) -> None:
    envelope["turns"]["success"]["runner_docker"]["id"] = "ZZZZ:YYYY:XXXX"
    _assert_token(envelope, "identity_mismatch")


def test_daemon_docker_name_differs_from_pod(envelope: dict[str, Any]) -> None:
    envelope["turns"]["success"]["daemon"]["docker_name"] = "other-daemon"
    _assert_token(envelope, "identity_mismatch")


def test_runner_docker_name_differs_from_pod(envelope: dict[str, Any]) -> None:
    envelope["turns"]["cancel"]["runner_docker"]["name"] = "other-daemon"
    _assert_token(envelope, "identity_mismatch")


def test_wrong_container_nonce(envelope: dict[str, Any]) -> None:
    envelope["turns"]["success"]["runner_docker"]["container_nonce"] = "ffffffff"
    _assert_token(envelope, "identity_mismatch")


# claim labels


@pytest.mark.parametrize("turn", ["success", "retained_next", "worker_loss"])
def test_wrong_claim_label_on_turn_object(envelope: dict[str, Any], turn: str) -> None:
    envelope["turns"][turn]["objects"][1]["claim_label"] = "factory-e2e-other"
    _assert_token(envelope, "wrong_claim_label")


def test_wrong_claim_label_on_orphan_object(envelope: dict[str, Any]) -> None:
    envelope["orphan"]["objects"][0]["claim_label"] = "factory-e2e-other"
    _assert_token(envelope, "wrong_claim_label")


# TLS


def test_cross_claim_tls_accepted(envelope: dict[str, Any]) -> None:
    envelope["tls"]["independent_client_refused"] = False
    _assert_token(envelope, "cross_claim_tls_accepted")


def test_missing_tls(envelope: dict[str, Any]) -> None:
    del envelope["tls"]
    _assert_token(envelope, "missing_observation")


# cleanup and timing


@pytest.mark.parametrize("turn", TURNS)
def test_turn_objects_still_present(envelope: dict[str, Any], turn: str) -> None:
    envelope["turns"][turn]["objects_absent"] = False
    _assert_token(envelope, "incomplete_cleanup")


def test_settle_seconds_over_limit(envelope: dict[str, Any]) -> None:
    envelope["turns"]["cancel"]["settle_seconds"] = 301
    _assert_token(envelope, "incomplete_cleanup")


def test_settle_seconds_missing(envelope: dict[str, Any]) -> None:
    del envelope["turns"]["cancel"]["settle_seconds"]
    _assert_token(envelope, "incomplete_cleanup")


@pytest.mark.parametrize("role", ROLES)
def test_turn_missing_a_role(envelope: dict[str, Any], role: str) -> None:
    turn = envelope["turns"]["success"]
    turn["objects"] = [obj for obj in turn["objects"] if obj["role"] != role]
    _assert_token(envelope, "missing_observation")


def test_orphan_objects_still_present(envelope: dict[str, Any]) -> None:
    envelope["orphan"]["objects_absent"] = False
    _assert_token(envelope, "incomplete_cleanup")


def test_orphan_settle_seconds_over_limit(envelope: dict[str, Any]) -> None:
    envelope["orphan"]["settle_seconds"] = 301
    _assert_token(envelope, "incomplete_cleanup")


def test_orphan_removed_while_young(envelope: dict[str, Any]) -> None:
    envelope["orphan"]["kept_while_young"] = False
    _assert_token(envelope, "age_gate")


@pytest.mark.parametrize(
    "key",
    [
        "helm_uninstalled",
        "release_objects_absent",
        "fixture_cleanup_complete",
        "namespaces_absent",
        "docker_assets_absent",
    ],
)
def test_cleanup_boolean_false(envelope: dict[str, Any], key: str) -> None:
    envelope["cleanup"][key] = False
    _assert_token(envelope, "teardown_unverified")


# dead daemon reuse


def test_retained_next_reuses_claim(envelope: dict[str, Any]) -> None:
    turns = envelope["turns"]
    turns["retained_next"]["claim"] = turns["daemon_death"]["claim"]
    _assert_token(envelope, "dead_daemon_reused")


def test_retained_next_reuses_daemon_pod_uid(envelope: dict[str, Any]) -> None:
    turns = envelope["turns"]
    turns["retained_next"]["daemon"]["pod_uid"] = turns["daemon_death"]["daemon"]["pod_uid"]
    _assert_token(envelope, "dead_daemon_reused")


def test_retained_next_on_another_thread(envelope: dict[str, Any]) -> None:
    envelope["turns"]["retained_next"]["thread"] = "thread-other"
    _assert_token(envelope, "retained_thread_mismatch")


# provenance


def test_candidate_missing(envelope: dict[str, Any]) -> None:
    del envelope["provenance"]["candidate"]
    _assert_token(envelope, "provenance")


@pytest.mark.parametrize("candidate", ["main", "HEAD", "xyz" + "0" * 37, "0123abcd"])
def test_candidate_not_hex(envelope: dict[str, Any], candidate: str) -> None:
    envelope["provenance"]["candidate"] = candidate
    _assert_token(envelope, "provenance")


def test_image_ref_without_digest(envelope: dict[str, Any]) -> None:
    image = envelope["provenance"]["images"]["worker"]
    image["ref"] = image["ref"].split("@", 1)[0] + ":latest"
    _assert_token(envelope, "provenance")


def test_image_ref_digest_differs_from_recorded(envelope: dict[str, Any]) -> None:
    image = envelope["provenance"]["images"]["api"]
    image["ref"] = image["ref"].split("@", 1)[0] + "@sha256:" + "e" * 64
    _assert_token(envelope, "provenance")


def test_catalog_builder_missing(envelope: dict[str, Any]) -> None:
    del envelope["provenance"]["catalog_builder"]
    _assert_token(envelope, "provenance")


def test_forged_runner_image_id(envelope: dict[str, Any]) -> None:
    envelope["turns"]["success"]["runner"]["image_id"] = "sha256:" + "9" * 64
    _assert_token(envelope, "forged_provenance")


def test_forged_daemon_image_id(envelope: dict[str, Any]) -> None:
    envelope["turns"]["worker_loss"]["daemon"]["image_id"] = "sha256:" + "9" * 64
    _assert_token(envelope, "forged_provenance")


def test_runner_image_id_may_be_the_manifest(envelope: dict[str, Any]) -> None:
    runner = envelope["provenance"]["images"]["runner"]
    envelope["turns"]["success"]["runner"]["image_id"] = runner["manifest"]
    assert _reasons(envelope) == []


# control


def test_control_runner_has_docker_env(envelope: dict[str, Any]) -> None:
    envelope["control"]["runner"]["docker_env"] = True
    _assert_token(envelope, "control_docker")


def test_control_has_daemon_objects(envelope: dict[str, Any]) -> None:
    envelope["control"]["daemon_objects"] = 1
    _assert_token(envelope, "control_docker")


def test_control_has_client_docker_files(envelope: dict[str, Any]) -> None:
    envelope["control"]["client_docker_files"] = True
    _assert_token(envelope, "control_docker")


def test_control_did_not_complete(envelope: dict[str, Any]) -> None:
    envelope["control"]["completed"] = False
    _assert_token(envelope, "missing_observation")


# missing sections


@pytest.mark.parametrize("turn", TURNS)
def test_missing_turn(envelope: dict[str, Any], turn: str) -> None:
    del envelope["turns"][turn]
    _assert_token(envelope, "missing_observation")


# stricter judging

MISSING = object()


def _set(container: dict[str, Any], key: str, value: Any) -> None:
    if value is MISSING:
        container.pop(key, None)
    else:
        container[key] = value


@pytest.mark.parametrize("turn", TURNS)
def test_turn_leaked_objects(envelope: dict[str, Any], turn: str) -> None:
    envelope["turns"][turn]["leaked"] = ["Secret/x"]
    _assert_token(envelope, "incomplete_cleanup")


@pytest.mark.parametrize("value", [False, MISSING], ids=["false", "missing"])
@pytest.mark.parametrize("turn", ["success", "retained_next", "daemon_death"])
def test_turn_not_delivered(envelope: dict[str, Any], turn: str, value: Any) -> None:
    _set(envelope["turns"][turn], "turn_delivered", value)
    _assert_token(envelope, "missing_observation")


@pytest.mark.parametrize("value", [False, MISSING], ids=["false", "missing"])
def test_control_turn_not_delivered(envelope: dict[str, Any], value: Any) -> None:
    _set(envelope["control"], "turn_delivered", value)
    _assert_token(envelope, "missing_observation")


def test_cancel_request_failed(envelope: dict[str, Any]) -> None:
    envelope["turns"]["cancel"]["request_state"] = "failed"
    _assert_token(envelope, "missing_observation")


@pytest.mark.parametrize("value", [False, MISSING], ids=["false", "missing"])
def test_daemon_death_not_released_before_next(envelope: dict[str, Any], value: Any) -> None:
    _set(envelope["turns"]["daemon_death"], "released_before_next", value)
    _assert_token(envelope, "retained_thread_mismatch")


@pytest.mark.parametrize("value", [301, MISSING], ids=["slow", "missing"])
def test_worker_loss_recovery_seconds(envelope: dict[str, Any], value: Any) -> None:
    _set(envelope["turns"]["worker_loss"], "recovery_seconds", value)
    _assert_token(envelope, "incomplete_cleanup")


@pytest.mark.parametrize("value", [False, MISSING], ids=["false", "missing"])
def test_worker_loss_successor_not_cleared(envelope: dict[str, Any], value: Any) -> None:
    _set(envelope["turns"]["worker_loss"], "successor_cleared", value)
    _assert_token(envelope, "incomplete_cleanup")


@pytest.mark.parametrize("value", [130, MISSING], ids=["young", "missing"])
def test_orphan_removed_after_seconds(envelope: dict[str, Any], value: Any) -> None:
    _set(envelope["orphan"], "removed_after_seconds", value)
    _assert_token(envelope, "age_gate")


@pytest.mark.parametrize("key", ["claim_client_accepted", "claim_client_accepted_after"])
@pytest.mark.parametrize("value", [False, MISSING], ids=["false", "missing"])
def test_tls_claim_client_not_accepted(envelope: dict[str, Any], key: str, value: Any) -> None:
    _set(envelope["tls"], key, value)
    _assert_token(envelope, "tls_control_failed")


@pytest.mark.parametrize("value", ["", MISSING], ids=["empty", "missing"])
@pytest.mark.parametrize("field", ["kind", "name", "namespace", "uid"])
def test_turn_object_missing_identity_field(
    envelope: dict[str, Any], field: str, value: Any
) -> None:
    _set(envelope["turns"]["success"]["objects"][1], field, value)
    _assert_token(envelope, "missing_observation")


def test_daemon_pod_uid_differs_from_daemon_object_uid(envelope: dict[str, Any]) -> None:
    envelope["turns"]["success"]["daemon"]["pod_uid"] = "00000000-0000-0000-0000-000000000000"
    _assert_token(envelope, "identity_mismatch")


def test_daemon_pod_differs_from_daemon_object_name(envelope: dict[str, Any]) -> None:
    turn = envelope["turns"]["success"]
    turn["daemon"]["pod"] = "curie-thread-other"
    turn["runner_docker"]["name"] = "curie-thread-other"
    turn["daemon"]["docker_name"] = "curie-thread-other"
    _assert_token(envelope, "identity_mismatch")


# CLI


def test_help_names_the_contract(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exit_info:
        daemon_lifecycle.main(["--help"])
    assert exit_info.value.code == 0
    out = capsys.readouterr().out
    for token in [
        "run",
        "--context",
        "--namespace fd-proof-<nonce>",
        "--daemon-namespace",
        "--release",
        "--candidate HEAD",
        "--work-dir .projects/daemon-proof-<nonce>",
        "owner=factory-tools",
        "CURIE_LOCAL_STUB_HOST",
        "catalog",
        "teardown",
    ]:
        assert token in out, token


def _forbid_processes(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("an unsafe intake reached a subprocess")

    monkeypatch.setattr(subprocess, "run", refuse)
    monkeypatch.setattr(subprocess, "Popen", refuse)


def _replace(argv: list[str], flag: str, value: str) -> list[str]:
    out = list(argv)
    out[out.index(flag) + 1] = value
    return out


UNSAFE = [
    pytest.param(_replace(VALID_ARGV, "--namespace", "fd-other-zz"), VALID_ENV, id="namespace"),
    pytest.param(_replace(VALID_ARGV, "--candidate", "main"), VALID_ENV, id="candidate"),
    pytest.param(VALID_ARGV, {}, id="stub-host-unset"),
    pytest.param(VALID_ARGV, {"CURIE_LOCAL_STUB_HOST": "stub.example"}, id="stub-host-name"),
]


@pytest.mark.parametrize(("argv", "env"), UNSAFE)
def test_run_refuses_unsafe_intake_before_any_subprocess(
    monkeypatch: pytest.MonkeyPatch, argv: list[str], env: dict[str, str]
) -> None:
    _forbid_processes(monkeypatch)
    monkeypatch.delenv("CURIE_LOCAL_STUB_HOST", raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    try:
        code = daemon_lifecycle.main(["run", *argv])
    except SystemExit as exit_info:
        code = exit_info.code
    assert code not in (0, None)


@pytest.mark.parametrize(("argv", "env"), UNSAFE)
def test_parse_run_raises_intake_error(argv: list[str], env: dict[str, str]) -> None:
    with pytest.raises(daemon_lifecycle.IntakeError):
        daemon_lifecycle.parse_run(argv, env)


def test_parse_run_accepts_valid_intake(monkeypatch: pytest.MonkeyPatch) -> None:
    _forbid_processes(monkeypatch)
    args = daemon_lifecycle.parse_run(VALID_ARGV, VALID_ENV)
    assert args.nonce == NONCE
    assert args.callback_host == "192.0.2.10"


# transcript


def test_transcript_loads_and_holds_on_the_signal_files() -> None:
    assert daemon_lifecycle.TRANSCRIPT.is_file(), "committed transcript is missing"
    model_script = _load("curie_model_script", MODEL_SCRIPT)
    exchanges = model_script.load_transcript(daemon_lifecycle.TRANSCRIPT)
    assert exchanges
    bash = 0
    for exchange in exchanges:
        content = exchange["message"].get("content")
        for block in content if isinstance(content, list) else []:
            if block.get("type") == "tool_use" and block.get("name") == "Bash":
                bash += 1
                assert block["input"]["timeout"] == 600000
                command = block["input"]["command"]
                for needle in [
                    "/tmp/fd-go",
                    "factory-tools/nonce=",
                    "docker info --format",
                    "/tmp/fd-release",
                ]:
                    assert needle in command, needle
    assert bash
    lanes = {exchange["lane_key"] for exchange in exchanges}
    for lane in lanes:
        assert any(
            exchange["lane_key"] == lane and exchange["message"].get("stop_reason") == "end_turn"
            for exchange in exchanges
        ), lane


# values


IMAGE_NAMES = ["daemon", "platform", "catalog", "runner", "api", "worker"]


def _images() -> dict[str, dict[str, str]]:
    out = {}
    for index, name in enumerate(IMAGE_NAMES):
        digest = "sha256:" + format(index + 1, "x") * 64
        out[name] = {
            "ref": f"localhost:5000/fd-{NONCE}/{name}@{digest}",
            "digest": digest,
            "manifest": "sha256:" + format(index + 8, "x") * 64,
            "config": "sha256:" + "f" * 64,
        }
    return out


def _base() -> dict[str, Any]:
    return _load_kind().scripted_values(
        model_base_url="http://192.0.2.20:8080",
        github_api="https://198.51.100.7:8443/api/v3",
        clone_base="https://198.51.100.7:8443",
        ca_configmap="github-stub-ca",
        runner_image="localhost:24111/curie-runner",
    )


def _values(base: dict[str, Any], *, docker: bool) -> dict[str, Any]:
    return daemon_lifecycle.lifecycle_values(
        base,
        _images(),
        nonce=NONCE,
        daemon_namespace=f"fd-proof-d-{NONCE}",
        release=f"fd-proof-{NONCE}",
        docker=docker,
    )


def _single_repo(values: dict[str, Any]) -> dict[str, Any]:
    bindings = values["factory"]["bindings"]
    assert len(bindings) == 1
    assert len(bindings[0]["repos"]) == 1
    repo = bindings[0]["repos"][0]
    assert repo["path"] == "acme-corp/acme-bot"
    return repo


def test_lifecycle_values_opt_one_repository_into_docker() -> None:
    base = _base()
    snapshot = copy.deepcopy(base)
    images = _images()
    values = _values(base, docker=True)
    assert base == snapshot
    assert values is not base
    docker = values["factory"]["docker"]
    assert docker["daemonImage"] == images["daemon"]["ref"]
    assert docker["namespace"] == f"fd-proof-d-{NONCE}"
    repo = _single_repo(values)
    assert repo["runner"]["image"] == images["runner"]["ref"]
    assert repo["runner"]["base"] == images["platform"]["digest"]
    assert "docker-cli" in repo["runner"]["tools"]
    assert repo["docker"] == {"enabled": True}
    assert values["agentSandbox"]["runner"]["digest"] == images["platform"]["digest"]
    assert values["ui"]["deploy"] is False


def test_control_values_drop_the_docker_opt_in() -> None:
    base = _base()
    snapshot = copy.deepcopy(base)
    values = _values(base, docker=False)
    assert base == snapshot
    repo = _single_repo(values)
    assert "docker" not in repo
    assert repo["runner"]["image"] == _images()["runner"]["ref"]
    assert "docker-cli" in repo["runner"]["tools"]
