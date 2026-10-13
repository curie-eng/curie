"""Contracts for the pre-runtime repository runner scenario judge.

The fixture samples describe synthetic recorded shapes. These tests prove the
judge accepts or refuses those shapes, not that any live component implements
repository runner routing, a manifest, or a model request.
"""

from __future__ import annotations

import copy
import json
import subprocess
from pathlib import Path
from typing import Any

import factory_e2e as fe
import pytest
import repo_runner

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "fixtures" / "repo-runner"
REPO_X = "acme-corp/repo-x"
REPO_Y = "acme-corp/repo-y"


def _passing() -> dict[str, Any]:
    return json.loads((FIXTURES / "passing.json").read_text())


def _node(observation: dict[str, Any], path: tuple[Any, ...]) -> Any:
    node: Any = observation
    for part in path:
        node = node[part]
    return node


def _set(observation: dict[str, Any], path: tuple[Any, ...], value: Any) -> None:
    _node(observation, path[:-1])[path[-1]] = value


def _manifest(observation: dict[str, Any], turn: int = 0) -> dict[str, Any]:
    return json.loads(observation["turns"][turn]["manifest_output"])


def test_manifest_probe_uses_the_fixed_image_manifest_path() -> None:
    assert repo_runner.MANIFEST_COMMAND == "cat /etc/curie/tool-manifest.json"


def test_scenario_values_copy_inputs_and_bind_repositories_to_the_factory_agent() -> None:
    base = {
        "factory": {"enabled": True, "codeHosts": [{"name": "fixture"}]},
        "worker": {"extraEnv": [{"name": "FIXTURE_SETTING", "value": "value"}]},
    }
    bindings = [{"path": path, "runner": config} for path, config in _passing()["bindings"].items()]
    original_base, original_bindings = copy.deepcopy(base), copy.deepcopy(bindings)

    values = repo_runner.scenario_values(base, bindings)

    assert values["factory"]["bindings"] == [{"agent": "factory", "repos": bindings}]
    assert values["factory"]["enabled"] is True
    assert values["factory"]["codeHosts"] == original_base["factory"]["codeHosts"]
    assert values["worker"] == original_base["worker"]
    assert (base, bindings) == (original_base, original_bindings)
    values["factory"]["bindings"][0]["repos"][0]["runner"]["tools"].append("pnpm")
    values["worker"]["extraEnv"][0]["value"] = "changed"
    assert (base, bindings) == (original_base, original_bindings)


def test_scenario_values_add_factory_to_a_base_without_it() -> None:
    bindings = [{"path": REPO_X, "runner": _passing()["bindings"][REPO_X]}]
    assert repo_runner.scenario_values({}, bindings)["factory"]["bindings"] == [
        {"agent": "factory", "repos": bindings}
    ]


@pytest.mark.parametrize(("sample", "passes"), [("passing.json", True), ("failing.json", False)])
def test_canonical_synthetic_samples(sample: str, passes: bool) -> None:
    observation = json.loads((FIXTURES / sample).read_text())
    assert "synthetic" in observation["provenance"]
    assert "not observed live evidence" in observation["provenance"]
    failures = repo_runner.judge_repo_runner(observation)
    assert isinstance(failures, list)
    assert all(isinstance(failure, str) and failure for failure in failures)
    assert (failures == []) is passes


@pytest.mark.parametrize("turn", [0, 1])
@pytest.mark.parametrize("digest_key", ["index", "linux/amd64", "linux/arm64"])
@pytest.mark.parametrize("identity_kind", ["docker-pullable", "containerd"])
def test_index_and_each_platform_digest_are_valid_runtime_image_identities(
    turn: int, digest_key: str, identity_kind: str
) -> None:
    observation = _passing()
    config = observation["bindings"][observation["turns"][turn]["repository"]]
    digest = config["image"].split("@", 1)[1] if digest_key == "index" else config[
        "platformDigests"
    ][digest_key]
    reference = config["image"].split("@", 1)[0]
    observation["turns"][turn]["image_id"] = (
        f"docker-pullable://{reference}@{digest}"
        if identity_kind == "docker-pullable"
        else f"containerd://{digest}"
    )
    assert repo_runner.judge_repo_runner(observation) == []


def test_manifest_tool_order_does_not_change_the_declared_tool_set() -> None:
    observation = _passing()
    manifest = _manifest(observation)
    manifest["tools"].reverse()
    observation["turns"][0]["manifest_output"] = json.dumps(manifest)
    assert repo_runner.judge_repo_runner(observation) == []


def test_a_declared_catalog_tool_and_an_additional_probe_can_share_the_manifest() -> None:
    observation = _passing()
    manifest = _manifest(observation)
    manifest["tools"].append(
        {"name": "acme-check", "version": "1.0", "path": "/opt/bin/acme-check", "source": "probe"}
    )
    observation["turns"][0]["manifest_output"] = json.dumps(manifest)
    observation["turns"][0]["boot_log"] = "tool manifest loaded: 3 tools\n"
    assert repo_runner.judge_repo_runner(observation) == []


def test_boot_log_may_contain_other_startup_lines() -> None:
    observation = _passing()
    observation["turns"][0]["boot_log"] = (
        "runner starting\n" + observation["turns"][0]["boot_log"] + "runner ready\n"
    )
    assert repo_runner.judge_repo_runner(observation) == []


@pytest.mark.parametrize("observation", [None, [], "", 1, True, {}, {"bindings": {}, "turns": []}])
def test_missing_or_non_mapping_observation_fails_closed(observation: Any) -> None:
    assert repo_runner.judge_repo_runner(observation)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        pytest.param(("bindings",), {}, id="empty-bindings"),
        pytest.param(("bindings",), [], id="non-mapping-bindings"),
        pytest.param(("turns",), [], id="empty-turns"),
        pytest.param(("turns",), {}, id="non-list-turns"),
        pytest.param(("bindings", REPO_X), None, id="missing-runner-config"),
        pytest.param(("bindings", REPO_X, "tools"), [], id="empty-declared-tools"),
        pytest.param(("bindings", REPO_X, "tools"), "uv", id="non-list-declared-tools"),
        pytest.param(("bindings", REPO_X, "tools"), ["uv", "uv"], id="duplicate-declared-tools"),
        pytest.param(("bindings", REPO_X, "tools"), ["made-up-tool"], id="unknown-catalog-tool"),
        pytest.param(("bindings", REPO_X, "image"), "runner:latest", id="mutable-image"),
        pytest.param(("bindings", REPO_X, "image"), "runner@sha256:abc", id="short-index-digest"),
        pytest.param(("bindings", REPO_X, "base"), "sha256:abc", id="malformed-base-digest"),
        pytest.param(("bindings", REPO_X, "platformDigests"), {}, id="empty-platform-digests"),
        pytest.param(
            ("bindings", REPO_X, "platformDigests", "linux/amd64"),
            "sha256:abc",
            id="malformed-platform-digest",
        ),
        pytest.param(("turns", 0), None, id="non-mapping-turn"),
        pytest.param(("turns", 0, "repository"), "acme-corp/unbound", id="unbound-repository"),
        pytest.param(("turns", 0, "thread"), "", id="empty-thread"),
        pytest.param(("turns", 0, "sandbox"), "", id="empty-sandbox"),
        pytest.param(("turns", 0, "pod_uid"), "", id="empty-pod-uid"),
        pytest.param(("turns", 0, "image_id"), "", id="empty-image-id"),
        pytest.param(("turns", 0, "image_id"), "containerd://sha256:abc", id="short-image-digest"),
        pytest.param(
            ("turns", 0, "image_id"),
            "containerd://sha256:" + "9" * 64,
            id="unrelated-image-digest",
        ),
        pytest.param(
            ("turns", 0, "image_id"),
            "containerd://sha256:" + "b" * 64 + "unrelated",
            id="digest-prefix-is-not-exact-identity",
        ),
        pytest.param(("turns", 0, "boot_log"), "", id="absent-boot-log"),
        pytest.param(("turns", 0, "boot_log"), "runner ready", id="no-manifest-boot-line"),
        pytest.param(
            ("turns", 0, "boot_log"), "tool manifest loaded: 1 tools\n", id="wrong-boot-count"
        ),
        pytest.param(("turns", 0, "manifest_output"), "", id="empty-manifest-output"),
        pytest.param(("turns", 0, "manifest_output"), "{", id="invalid-manifest-json"),
        pytest.param(("turns", 0, "manifest_output"), "null", id="null-manifest"),
        pytest.param(("turns", 0, "manifest_output"), "[]", id="non-object-manifest"),
        pytest.param(("turns", 0, "status"), "running", id="nonterminal-turn"),
        pytest.param(("turns", 0, "status"), "failed", id="failed-turn"),
        pytest.param(("turns", 0, "model_requests"), 0, id="no-model-requests"),
        pytest.param(("turns", 0, "model_requests"), 1, id="only-one-model-request"),
        pytest.param(("turns", 0, "model_requests"), "2", id="noninteger-model-count"),
        pytest.param(("turns", 0, "model_requests"), True, id="boolean-model-count"),
        pytest.param(("turns", 1, "thread"), "other-thread", id="repository-change-outside-thread"),
        pytest.param(("turns", 1, "sandbox"), "fixture-sandbox-x", id="sandbox-reused-across-repos"),
        pytest.param(
            ("turns", 1, "pod_uid"),
            "00000000-0000-4000-8000-000000000001",
            id="pod-reused-across-repos",
        ),
    ],
)
def test_each_observation_shortfall_fails_closed(path: tuple[Any, ...], value: Any) -> None:
    observation = _passing()
    _set(observation, path, value)
    assert repo_runner.judge_repo_runner(observation), path


@pytest.mark.parametrize(
    "path",
    [
        ("bindings",),
        ("turns",),
        ("bindings", REPO_Y),
        ("bindings", REPO_X, "tools"),
        ("bindings", REPO_X, "image"),
        ("bindings", REPO_X, "base"),
        ("bindings", REPO_X, "platformDigests"),
        ("bindings", REPO_X, "platformDigests", "linux/arm64"),
        ("turns", 0, "repository"),
        ("turns", 0, "thread"),
        ("turns", 0, "sandbox"),
        ("turns", 0, "pod_uid"),
        ("turns", 0, "image_id"),
        ("turns", 0, "boot_log"),
        ("turns", 0, "manifest_output"),
        ("turns", 0, "status"),
        ("turns", 0, "model_requests"),
    ],
)
def test_each_required_observation_field_is_required(path: tuple[Any, ...]) -> None:
    observation = _passing()
    del _node(observation, path[:-1])[path[-1]]
    assert repo_runner.judge_repo_runner(observation), path


@pytest.mark.parametrize(
    ("path", "value"),
    [
        pytest.param(("schema_version",), 2, id="wrong-schema-version"),
        pytest.param(("schema_version",), "1", id="string-schema-version"),
        pytest.param(("schema_version",), True, id="boolean-schema-version"),
        pytest.param(("tools",), [], id="manifest-missing-declared-tools"),
        pytest.param(("tools",), {}, id="manifest-tools-not-list"),
        pytest.param(("tools", 0), "uv", id="manifest-tool-not-object"),
        pytest.param(("tools", 0, "name"), "pnpm", id="wrong-declared-tool"),
        pytest.param(("tools", 0, "version"), "", id="blank-tool-version"),
        pytest.param(("tools", 0, "path"), "", id="blank-tool-path"),
        pytest.param(("tools", 0, "source"), "other", id="unknown-tool-source"),
        pytest.param(("tools", 0, "source"), "probe", id="declared-catalog-tool-is-only-probe"),
        pytest.param(("docker_cli",), "false", id="docker-cli-not-boolean"),
    ],
)
def test_each_manifest_shortfall_fails_closed(path: tuple[Any, ...], value: Any) -> None:
    observation = _passing()
    manifest = _manifest(observation)
    _set(manifest, path, value)
    observation["turns"][0]["manifest_output"] = json.dumps(manifest)
    assert repo_runner.judge_repo_runner(observation), path


@pytest.mark.parametrize(
    "path", [("schema_version",), ("tools",), ("docker_cli",)]
    + [("tools", 0, field) for field in ("name", "version", "path", "source")],
)
def test_each_manifest_field_is_required(path: tuple[Any, ...]) -> None:
    observation = _passing()
    manifest = _manifest(observation)
    del _node(manifest, path[:-1])[path[-1]]
    observation["turns"][0]["manifest_output"] = json.dumps(manifest)
    assert repo_runner.judge_repo_runner(observation), path


@pytest.mark.parametrize("extra_tool", ["catalog", "duplicate"])
def test_manifest_cannot_add_an_undeclared_catalog_tool_or_duplicate(extra_tool: str) -> None:
    observation = _passing()
    manifest = _manifest(observation)
    tool = copy.deepcopy(manifest["tools"][0])
    if extra_tool == "catalog":
        tool["name"] = "pnpm"
    manifest["tools"].append(tool)
    observation["turns"][0]["manifest_output"] = json.dumps(manifest)
    observation["turns"][0]["boot_log"] = "tool manifest loaded: 3 tools\n"
    assert repo_runner.judge_repo_runner(observation)


@pytest.mark.parametrize("turn", [0, 1, 2, 3])
def test_each_success_and_refusal_case_is_required(turn: int) -> None:
    observation = _passing()
    del observation["turns"][turn]
    assert repo_runner.judge_repo_runner(observation)


@pytest.mark.parametrize("turn", [2, 3], ids=["missing-pool", "stale-base"])
@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("status", "completed", id="completion-does-not-prove-refusal"),
        pytest.param("reason", "some_other_refusal", id="wrong-refusal-reason"),
        pytest.param("reason", "", id="absent-refusal-reason"),
        pytest.param("sandbox", "fallback-agent-sandbox", id="refusal-created-sandbox"),
        pytest.param("pod_uid", "fallback-pod", id="refusal-created-pod"),
        pytest.param(
            "image_id", "containerd://sha256:" + "e" * 64, id="refusal-used-agent-image"
        ),
        pytest.param("model_requests", 1, id="refusal-contacted-model"),
        pytest.param("model_requests", False, id="refusal-model-count-not-integer"),
        pytest.param("new_claims", ["fallback-agent-sandbox"], id="refusal-created-claim"),
        pytest.param("new_claims", None, id="refusal-claim-evidence-unavailable"),
    ],
)
def test_each_refusal_must_precede_any_model_request_or_sandbox(
    turn: int, field: str, value: Any
) -> None:
    observation = _passing()
    observation["turns"][turn][field] = value
    assert repo_runner.judge_repo_runner(observation), (turn, field)


@pytest.mark.parametrize("turn", [2, 3], ids=["missing-pool", "stale-base"])
@pytest.mark.parametrize("field", ["status", "reason", "model_requests", "new_claims"])
def test_refusal_requires_explicit_terminal_and_no_side_effect_evidence(turn: int, field: str) -> None:
    observation = _passing()
    del observation["turns"][turn][field]
    assert repo_runner.judge_repo_runner(observation), (turn, field)


def test_repo_runner_is_named_by_the_factory_parser_and_its_help(
    capsys: pytest.CaptureFixture[str],
) -> None:
    args = fe.parse_args(["run", "--scenario", "repo-runner"])
    assert args.mode == "run" and args.scenario == "repo-runner"
    assert callable(fe.resolve_scenario("repo-runner"))
    with pytest.raises(SystemExit) as exit_info:
        fe.parse_args(["run", "--help"])
    assert exit_info.value.code == 0
    assert "repo-runner" in capsys.readouterr().out


def test_public_judge_parser_takes_an_observation_path() -> None:
    sample = FIXTURES / "failing.json"
    args = fe.parse_args(["judge-repo-runner", "--observation", str(sample)])
    assert args.mode == "judge-repo-runner"
    assert args.observation == sample


def test_public_judge_requires_an_observation_path() -> None:
    with pytest.raises(SystemExit) as exit_info:
        fe.parse_args(["judge-repo-runner"])
    assert exit_info.value.code != 0


@pytest.mark.parametrize(("sample", "code"), [("passing.json", 0), ("failing.json", 1)])
def test_standalone_judge_main_returns_the_sample_verdict(sample: str, code: int) -> None:
    assert repo_runner.main(["judge", str(FIXTURES / sample)]) == code


@pytest.mark.parametrize("contents", ["{", "null", "[]", "{}"])
def test_standalone_judge_fails_closed_on_unreadable_observation(
    contents: str, tmp_path: Path
) -> None:
    observation = tmp_path / "observation.json"
    observation.write_text(contents)
    assert repo_runner.main(["judge", str(observation)]) == 1


@pytest.mark.parametrize(("sample", "code"), [("passing.json", 0), ("failing.json", 1)])
def test_real_judge_command_exits_with_the_sample_verdict(sample: str, code: int) -> None:
    result = subprocess.run(
        ["python3", str(ROOT / "repo_runner.py"), "judge", str(FIXTURES / sample)],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == code, result.stdout + result.stderr


@pytest.mark.parametrize(("sample", "code"), [("passing.json", 0), ("failing.json", 1)])
def test_public_factory_judge_command_exits_with_the_sample_verdict(sample: str, code: int) -> None:
    result = subprocess.run(
        [
            "python3",
            str(ROOT / "factory_e2e.py"),
            "judge-repo-runner",
            "--observation",
            str(FIXTURES / sample),
        ],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == code, result.stdout + result.stderr
