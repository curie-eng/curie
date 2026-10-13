"""Exercise the quota scenario's caller-supplied input boundary."""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent / "runtime/idle-capacity-reclamation-runtime.sh"
INPUTS = (
    "CURIE_BIN",
    "CURIE_SCENARIO_CONTEXT",
    "CURIE_SCENARIO_RUN_ID",
    "CURIE_SCENARIO_API_IMAGE",
    "CURIE_SCENARIO_WORKER_IMAGE",
    "CURIE_SCENARIO_RUNNER_IMAGE",
)
IMAGE_INPUTS = INPUTS[3:]
SHARED_OBJECTS = (
    "namespace/agent-sandbox-system",
    "clusterrole/agent-sandbox-controller",
    "clusterrole/agent-sandbox-controller-extensions",
    "clusterrole/agent-sandbox-controller-networkpolicies-read",
    "clusterrolebinding/agent-sandbox-controller",
    "clusterrolebinding/agent-sandbox-controller-extensions",
    "clusterrolebinding/agent-sandbox-controller-networkpolicies-read",
    "priorityclass/curie-platform",
    "priorityclass/curie-sandbox",
)

# Recording tools normally fail on their first call. Inventory mode returns
# one occupied object or an empty cluster, then fails the candidate install.
# Neither mode touches a cluster.
TOOL_STUB = r"""
import os
import shlex
import sys
from pathlib import Path

tool = Path(sys.argv[0]).name
args = sys.argv[1:]
with Path(os.environ["QUOTA_CALL_LOG"]).open("a") as log:
    log.write(shlex.join([tool, *args]) + "\n")
if os.environ.get("QUOTA_INVENTORY_MODE") != "1":
    raise SystemExit(91)
if tool == "kubectl":
    if "get" in args:
        tail = args[args.index("get") + 1:]
        kind, separator, name = tail[0].partition("/")
        if not separator:
            name = tail[1] if len(tail) > 1 and not tail[1].startswith("-") else ""
        if f"{kind}/{name}" == os.environ.get("QUOTA_OCCUPIED_OBJECT"):
            print(f"{kind}/{name}")
        raise SystemExit(0)
    if any(command in args for command in ("delete", "wait", "logs")):
        raise SystemExit(0)
if tool == "curie":
    raise SystemExit(93)
if tool == "helm" and any(command in args for command in ("uninstall", "list")):
    raise SystemExit(0)
raise SystemExit(94)
"""


@dataclass
class Scenario:
    environment: dict[str, str]
    log: Path

    def run(self) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(SCRIPT)],
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
    for name in ("kubectl", "helm", "kind", "docker", "cargo", "curie"):
        executable = tools / name
        executable.write_text(f"#!{sys.executable}\n" + TOOL_STUB)
        executable.chmod(0o755)
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text(
        "apiVersion: v1\nkind: Config\ncurrent-context: kind-acme-test\n"
        "contexts:\n  - name: kind-acme-test\n    context:\n      cluster: acme-test\n"
        "      user: acme-test\n"
    )
    return Scenario(
        {
            "PATH": str(tools) + os.pathsep + os.environ["PATH"],
            "HOME": str(tmp_path),
            "KUBECONFIG": str(kubeconfig),
            "QUOTA_CALL_LOG": str(log),
            "CURIE_BIN": str(tools / "curie"),
            "CURIE_SCENARIO_CONTEXT": "kind-acme-test",
            "CURIE_SCENARIO_RUN_ID": "12345-1",
            "CURIE_SCENARIO_API_IMAGE": "curie-api:wsdiag-acme",
            "CURIE_SCENARIO_WORKER_IMAGE": "curie-worker:wsdiag-acme",
            "CURIE_SCENARIO_RUNNER_IMAGE": "curie-runner:wsdiag-acme",
        },
        log,
    )


def assert_input_rejected(scenario: Scenario, variable: str) -> None:
    completed = scenario.run()
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, output
    assert any(
        line.startswith("FAIL:") and variable in line for line in output.splitlines()
    ), output
    assert scenario.calls() == [], "invalid inputs must be rejected before any tool runs"


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
        ("CURIE_SCENARIO_CONTEXT", "kind-acme-test\t"),
        ("CURIE_SCENARIO_CONTEXT", "--context"),
        ("CURIE_SCENARIO_CONTEXT", "kind-absent"),
        ("CURIE_SCENARIO_RUN_ID", "Uppercase"),
        ("CURIE_SCENARIO_RUN_ID", "-leading"),
        ("CURIE_SCENARIO_RUN_ID", "with/slash"),
        ("CURIE_SCENARIO_RUN_ID", "12345-1\n"),
        ("CURIE_SCENARIO_RUN_ID", "a" * 26),
    ],
)
def test_malformed_context_or_run_id_fails_before_any_tool_call(
    scenario: Scenario, variable: str, value: str
) -> None:
    scenario.environment[variable] = value
    assert_input_rejected(scenario, variable)


@pytest.mark.parametrize("variable", IMAGE_INPUTS)
@pytest.mark.parametrize(
    "value",
    [
        "curie-api",
        "curie-api:",
        ":candidate",
        "curie-api:bad tag",
        "curie-api:candidate\nother:image",
        "curie-api@sha256:" + "a" * 64,
        "curie-api:" + "a" * 129,
    ],
)
def test_malformed_image_fails_before_any_tool_call(
    scenario: Scenario, variable: str, value: str
) -> None:
    scenario.environment[variable] = value
    assert_input_rejected(scenario, variable)


@pytest.mark.parametrize("binary_kind", ["missing", "directory", "not-executable"])
def test_invalid_binary_fails_before_any_tool_call(
    scenario: Scenario, tmp_path: Path, binary_kind: str
) -> None:
    binary = tmp_path / "invalid-curie"
    if binary_kind == "directory":
        binary.mkdir()
    elif binary_kind == "not-executable":
        binary.write_text("#!/bin/sh\nexit 0\n")
        binary.chmod(0o644)
    scenario.environment["CURIE_BIN"] = str(binary)
    assert_input_rejected(scenario, "CURIE_BIN")


@pytest.mark.parametrize("run_id", ["a", "a" * 25, "12345-1"])
def test_valid_run_id_reaches_cluster_inventory(scenario: Scenario, run_id: str) -> None:
    scenario.environment["CURIE_SCENARIO_RUN_ID"] = run_id
    assert_reaches_cluster_inventory(scenario)


@pytest.mark.parametrize("variable", IMAGE_INPUTS)
@pytest.mark.parametrize(
    "image",
    [
        "curie-api:12345",
        "registry.example.com:5000/acme-corp/curie-api:Candidate_1.0",
        "[::1]:5000/acme-corp/curie-api:candidate",
        "curie-api:" + "a" * 128,
    ],
)
def test_valid_image_reaches_cluster_inventory(
    scenario: Scenario, variable: str, image: str
) -> None:
    scenario.environment[variable] = image
    assert_reaches_cluster_inventory(scenario)


def assert_reaches_cluster_inventory(scenario: Scenario) -> None:
    completed = scenario.run()
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, "the cluster inventory stub must stop the scenario"
    calls = scenario.calls()
    assert len(calls) == 1, (output, calls)
    assert calls[0][0] == "kubectl" and "get" in calls[0], (output, calls)
    assert "--context" in calls[0] and "kind-acme-test" in calls[0], calls
    assert not any(variable in output for variable in INPUTS), output


@pytest.mark.parametrize(
    "occupied_object", [*SHARED_OBJECTS, "namespace/test-4368-idle-capacity-12345-1"]
)
def test_occupied_shared_or_private_object_refuses_before_install_or_cleanup(
    scenario: Scenario, occupied_object: str
) -> None:
    scenario.environment["QUOTA_INVENTORY_MODE"] = "1"
    scenario.environment["QUOTA_OCCUPIED_OBJECT"] = occupied_object
    completed = scenario.run()
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, output
    name = occupied_object.split("/", maxsplit=1)[1]
    assert any(
        line.startswith("FAIL:") and name in line for line in output.splitlines()
    ), output
    calls = scenario.calls()
    assert calls, output
    assert all(call[0] == "kubectl" and "get" in call for call in calls), calls
    assert all("--context" in call and "kind-acme-test" in call for call in calls), calls
    assert not any(
        command in call for call in calls for command in ("delete", "apply", "uninstall", "wait")
    ), calls


def test_empty_inventory_advances_to_install_and_cleans_up_failed_install(
    scenario: Scenario,
) -> None:
    scenario.environment["QUOTA_INVENTORY_MODE"] = "1"
    completed = scenario.run()
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, "the failed candidate install must remain a failed scenario"
    assert "candidate cluster install failed" in output, output
    calls = scenario.calls()
    installs = [call for call in calls if call[0] == "curie"]
    assert len(installs) == 1, (output, calls)
    install = installs[0]
    assert "cluster" in install and "up" in install, install
    assert "kind-acme-test" in install, install
    assert "test-4368-idle-capacity-12345-1" in install, install
    assert "idlecap-12345-1" in install, install
    install_index = calls.index(install)
    assert all(
        call[0] == "kubectl" and "get" in call for call in calls[:install_index]
    ), calls
    cleanup = calls[install_index + 1 :]
    assert any(call[0] == "helm" and "uninstall" in call for call in cleanup), cleanup
    assert any(
        call[0] == "kubectl"
        and "delete" in call
        and any(argument.endswith("test-4368-idle-capacity-12345-1") for argument in call)
        for call in cleanup
    ), cleanup
    assert all(call[0] not in {"kind", "docker", "cargo"} for call in calls), calls


def test_candidate_stack_provisioning_header_is_declared_once_in_first_twenty_lines() -> None:
    header = SCRIPT.read_text().splitlines()[:20]
    assert header.count("# fix-pin-provision: candidate-stack") == 1
