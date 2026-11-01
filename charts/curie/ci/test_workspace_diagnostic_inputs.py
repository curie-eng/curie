"""Exercise the workspace scenario's input and shared-cluster boundaries."""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent / "runtime/workspace-dependency-diagnostic-runtime.sh"
INPUTS = (
    "CURIE_BIN",
    "CURIE_SCENARIO_CONTEXT",
    "CURIE_SCENARIO_RUN_ID",
    "CURIE_SCENARIO_API_IMAGE",
    "CURIE_SCENARIO_WORKER_IMAGE",
    "CURIE_SCENARIO_RUNNER_IMAGE",
    "CURIE_SCENARIO_NAMESPACE",
    "CURIE_SCENARIO_RELEASE",
)
TOOL_STUB = r"""
import os
import shlex
import sys
from pathlib import Path

with Path(os.environ["WSDIAG_CALL_LOG"]).open("a") as log:
    log.write(shlex.join([Path(sys.argv[0]).name, *sys.argv[1:]]) + "\n")
raise SystemExit(91)
"""


@dataclass
class Scenario:
    environment: dict[str, str]
    log: Path
    binary: Path

    def run(self, script: Path = SCRIPT) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(script)],
            env=self.environment,
            capture_output=True,
            text=True,
            check=False,
            timeout=20,
        )

    def calls(self) -> list[list[str]]:
        return [shlex.split(line) for line in self.log.read_text().splitlines()]



@pytest.fixture
def scenario(tmp_path: Path) -> Scenario:
    tools = tmp_path / "tools"
    tools.mkdir()
    log = tmp_path / "calls.log"
    log.touch()
    body = f"#!{sys.executable}\n" + TOOL_STUB
    for name in ("kubectl", "helm", "kind", "docker", "cargo", "curie"):
        executable = tools / name
        executable.write_text(body)
        executable.chmod(0o755)
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text(
        "apiVersion: v1\nkind: Config\ncurrent-context: kind-acme-test\n"
        "clusters:\n  - name: acme-test\n    cluster:\n      server: https://127.0.0.1:6443\n"
        "contexts:\n  - name: kind-acme-test\n    context:\n      cluster: acme-test\n"
        "      user: acme-test\nusers:\n  - name: acme-test\n    user: {}\n"
    )
    environment = {
        "PATH": str(tools) + os.pathsep + os.environ["PATH"],
        "HOME": str(tmp_path),
        "KUBECONFIG": str(kubeconfig),
        "WSDIAG_CALL_LOG": str(log),
        "CURIE_BIN": str(tools / "curie"),
        "CURIE_SCENARIO_CONTEXT": "kind-acme-test",
        "CURIE_SCENARIO_RUN_ID": "12345-1",
        "CURIE_SCENARIO_NAMESPACE": "test-workload-12345-1",
        "CURIE_SCENARIO_RELEASE": "workload-12345-1",
        "CURIE_SCENARIO_API_IMAGE": "curie-api:wsdiag-acme",
        "CURIE_SCENARIO_WORKER_IMAGE": "curie-worker:wsdiag-acme",
        "CURIE_SCENARIO_RUNNER_IMAGE": "curie-runner:wsdiag-acme",
    }
    return Scenario(environment, log, tools / "curie")


def assert_input_rejected(scenario: Scenario, variable: str) -> None:
    completed = scenario.run()
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, output
    assert "FAIL:" in output and variable in output, output
    assert scenario.calls() == [], "invalid inputs must be rejected before any external tool runs"


@pytest.mark.parametrize("variable", INPUTS)
def test_unset_input_fails_before_any_tool_call(scenario: Scenario, variable: str) -> None:
    del scenario.environment[variable]
    assert_input_rejected(scenario, variable)


@pytest.mark.parametrize("variable", INPUTS)
def test_empty_input_fails_before_any_tool_call(scenario: Scenario, variable: str) -> None:
    scenario.environment[variable] = ""
    assert_input_rejected(scenario, variable)


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("CURIE_SCENARIO_CONTEXT", "kind-acme-test\nother-context"),
        ("CURIE_SCENARIO_CONTEXT", "kind-absent"),
        ("CURIE_SCENARIO_NAMESPACE", "Uppercase"),
        ("CURIE_SCENARIO_NAMESPACE", "with/slash"),
        ("CURIE_SCENARIO_NAMESPACE", "a" * 64),
        ("CURIE_SCENARIO_RELEASE", "Uppercase"),
        ("CURIE_SCENARIO_RELEASE", "with/slash"),
        ("CURIE_SCENARIO_RELEASE", "a" * 54),
        ("CURIE_SCENARIO_RUN_ID", "Uppercase"),
        ("CURIE_SCENARIO_RUN_ID", "-leading"),
        ("CURIE_SCENARIO_RUN_ID", "with/slash"),
        ("CURIE_SCENARIO_RUN_ID", "a" * 26),
        ("CURIE_SCENARIO_API_IMAGE", "curie-api"),
        ("CURIE_SCENARIO_API_IMAGE", "curie-api:"),
        ("CURIE_SCENARIO_WORKER_IMAGE", "curie-worker:bad tag"),
        ("CURIE_SCENARIO_WORKER_IMAGE", ":candidate"),
        ("CURIE_SCENARIO_RUNNER_IMAGE", "curie-runner@sha256:" + "a" * 64),
        ("CURIE_SCENARIO_RUNNER_IMAGE", "curie-runner:candidate\nother:image"),
    ],
)
def test_malformed_input_fails_before_any_tool_call(
    scenario: Scenario, variable: str, value: str
) -> None:
    scenario.environment[variable] = value
    assert_input_rejected(scenario, variable)


@pytest.mark.parametrize("binary_kind", ["missing", "directory", "not-executable"])
def test_invalid_binary_fails_before_any_tool_call(
    scenario: Scenario, tmp_path: Path, binary_kind: str
) -> None:
    path = tmp_path / "invalid-curie"
    if binary_kind == "directory":
        path.mkdir()
    elif binary_kind == "not-executable":
        path.write_text("#!/bin/sh\nexit 0\n")
        path.chmod(0o644)
    scenario.environment["CURIE_BIN"] = str(path)
    assert_input_rejected(scenario, "CURIE_BIN")



def test_valid_shared_install_reaches_workload_without_platform_mutation(scenario: Scenario) -> None:
    completed = scenario.run()
    assert completed.returncode != 0, "the workload boundary stub must stop the scenario"
    calls = scenario.calls()
    assert calls, completed.stdout + completed.stderr
    assert all(call[0] == "kubectl" for call in calls), calls
    assert all("--context" in call and "kind-acme-test" in call for call in calls), calls
    assert all("test-workload-12345-1" in call for call in calls), calls
    assert all(not any(action in call for action in ("create", "apply", "delete")) for call in calls), calls


@pytest.mark.parametrize(
    "script_name",
    [
        "idle-capacity-reclamation-runtime.sh",
        "workspace-dependency-diagnostic-runtime.sh",
        "connector-secret-rotation-runtime.sh",
        "connector-apply-race-runtime.sh",
        "factory-resource-profile-runtime.sh",
    ],
)
@pytest.mark.parametrize("variable", ["CURIE_SCENARIO_NAMESPACE", "CURIE_SCENARIO_RELEASE"])
@pytest.mark.parametrize("value", [None, ""])
def test_every_shared_consumer_requires_its_install_identity_before_tool_calls(
    scenario: Scenario, script_name: str, variable: str, value: str | None
) -> None:
    if value is None:
        del scenario.environment[variable]
    else:
        scenario.environment[variable] = value
    completed = scenario.run(SCRIPT.with_name(script_name))
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, output
    assert any(line.startswith("FAIL:") and variable in line for line in output.splitlines()), output
    assert scenario.calls() == [], "an incomplete shared install identity must fail before tool calls"
