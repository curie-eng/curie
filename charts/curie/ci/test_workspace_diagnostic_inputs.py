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
)
CRDS = (
    "sandboxes.agents.x-k8s.io",
    "sandboxclaims.extensions.agents.x-k8s.io",
    "sandboxtemplates.extensions.agents.x-k8s.io",
    "sandboxwarmpools.extensions.agents.x-k8s.io",
)
CLUSTER_OBJECTS = (
    ("namespace", "agent-sandbox-system"),
    ("clusterrole", "agent-sandbox-controller"),
    ("clusterrole", "agent-sandbox-controller-extensions"),
    ("clusterrole", "agent-sandbox-controller-networkpolicies-read"),
    ("clusterrolebinding", "agent-sandbox-controller"),
    ("clusterrolebinding", "agent-sandbox-controller-extensions"),
    ("clusterrolebinding", "agent-sandbox-controller-networkpolicies-read"),
    ("priorityclass", "curie-platform"),
    ("priorityclass", "curie-sandbox"),
)

# These executables replace external tools only. Run the real scenario so its
# validation, preflight, and EXIT trap remain the paths under test.
TOOL_STUB = r"""
import os
import shlex
import sys
from pathlib import Path

tool = Path(sys.argv[0]).name
args = sys.argv[1:]
with Path(os.environ["WSDIAG_CALL_LOG"]).open("a") as log:
    log.write(shlex.join([tool, *args]) + "\n")
if os.environ.get("WSDIAG_MODE") != "cluster":
    raise SystemExit(91)
if tool == "kubectl":
    if "config" in args:
        if "current-context" in args or "get-contexts" in args:
            print(os.environ["CURIE_SCENARIO_CONTEXT"])
        raise SystemExit(0)
    if "get" in args:
        tail = args[args.index("get") + 1:]
        aliases = {
            "ns": "namespace", "namespaces": "namespace",
            "clusterroles": "clusterrole", "clusterrolebindings": "clusterrolebinding",
            "priorityclasses": "priorityclass",
            "crd": "customresourcedefinition", "crds": "customresourcedefinition",
            "customresourcedefinitions": "customresourcedefinition",
        }
        kind, separator, name = tail[0].partition("/")
        kind = aliases.get(kind, kind)
        if not separator:
            name = tail[1] if len(tail) > 1 and not tail[1].startswith("-") else ""
        identity = f"{kind}/{name}"
        occupied = os.environ.get("WSDIAG_OCCUPIED_OBJECT", "")
        retained = os.environ.get("WSDIAG_EXISTING_CRDS", "").split(",")
        remains = os.environ.get("WSDIAG_REMAINING_OBJECT", "")
        installed = Path(os.environ["WSDIAG_INSTALL_MARKER"]).exists()
        present = identity == occupied or (installed and identity == remains)
        present |= kind == "customresourcedefinition" and name in retained
        if present:
            print(identity)
            raise SystemExit(0)
        ignore_missing = "--ignore-not-found" in args or "--ignore-not-found=true" in args
        raise SystemExit(0 if ignore_missing else 1)
    if "delete" in args or "wait" in args:
        raise SystemExit(0)
    raise SystemExit(92)
if tool == "curie":
    Path(os.environ["WSDIAG_INSTALL_MARKER"]).touch()
    raise SystemExit(93)
if tool == "helm":
    if "uninstall" in args:
        raise SystemExit(0)
    if "list" in args:
        print("[]" if "json" in args or "-o=json" in args or "--output=json" in args else "")
        raise SystemExit(0)
    if "status" in args:
        raise SystemExit(1)
    raise SystemExit(94)
raise SystemExit(95)
"""


@dataclass
class Scenario:
    environment: dict[str, str]
    log: Path
    binary: Path

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
        "WSDIAG_INSTALL_MARKER": str(tmp_path / "installed"),
        "CURIE_BIN": str(tools / "curie"),
        "CURIE_SCENARIO_CONTEXT": "kind-acme-test",
        "CURIE_SCENARIO_RUN_ID": "12345-1",
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


@pytest.mark.parametrize(("kind", "name"), CLUSTER_OBJECTS)
def test_occupied_controller_or_priority_class_fails_before_install(
    scenario: Scenario, kind: str, name: str
) -> None:
    scenario.environment["WSDIAG_MODE"] = "cluster"
    scenario.environment["WSDIAG_OCCUPIED_OBJECT"] = f"{kind}/{name}"
    completed = scenario.run()
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, output
    assert "FAIL:" in output and name in output, output
    calls = scenario.calls()
    assert calls and all(call[0] == "kubectl" for call in calls), calls
    assert all("get" in call or "config" in call for call in calls), calls


@pytest.mark.parametrize("existing_crd", [None, *CRDS])
def test_failed_install_cleans_owned_objects_and_preserves_existing_crds(
    scenario: Scenario, existing_crd: str | None
) -> None:
    scenario.environment["WSDIAG_MODE"] = "cluster"
    scenario.environment["WSDIAG_EXISTING_CRDS"] = existing_crd or ""
    completed = scenario.run()
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, "a failed install must remain a failed scenario"
    assert "candidate cluster install failed" in output, output
    calls = scenario.calls()
    install_index = next(index for index, call in enumerate(calls) if call[0] == "curie")
    install = calls[install_index]
    assert "kind-acme-test" in install and "test-4367-wsdiag-12345-1" in install, install
    assert "wsdiag-12345-1" in install, install
    assert all(call[0] not in {"cargo", "docker", "kind"} for call in calls), calls
    assert any(call[0] == "helm" and "uninstall" in call for call in calls), calls
    assert any("delete" in call and "test-4367-wsdiag-12345-1" in call for call in calls), calls
    assert any("wait" in call and "--for=delete" in call for call in calls), calls
    for crd in CRDS:
        inventory = [
            index
            for index, call in enumerate(calls)
            if "get" in call and any(arg == crd or arg.endswith("/" + crd) for arg in call)
        ]
        assert any(index < install_index for index in inventory), (crd, calls)
        deletions = [
            call
            for call in calls
            if "delete" in call and any(arg == crd or arg.endswith("/" + crd) for arg in call)
        ]
        assert bool(deletions) == (crd != existing_crd), (crd, deletions)
        if crd != existing_crd:
            assert any(index > install_index for index in inventory), (crd, calls)
