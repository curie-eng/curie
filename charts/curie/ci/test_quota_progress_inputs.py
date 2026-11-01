"""Exercise quota scenario inputs and pod observations."""

from __future__ import annotations

import ast
import json
import os
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
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
    "CURIE_SCENARIO_NAMESPACE",
    "CURIE_SCENARIO_RELEASE",
)
IMAGE_INPUTS = INPUTS[3:6]
TOOL_STUB = r"""
import os
import shlex
import sys
from pathlib import Path

with Path(os.environ["QUOTA_CALL_LOG"]).open("a") as log:
    log.write(shlex.join([Path(sys.argv[0]).name, *sys.argv[1:]]) + "\n")
raise SystemExit(91)
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
            "CURIE_SCENARIO_NAMESPACE": "test-workload-12345-1",
            "CURIE_SCENARIO_RELEASE": "workload-12345-1",
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
        ("CURIE_SCENARIO_NAMESPACE", "Uppercase"),
        ("CURIE_SCENARIO_NAMESPACE", "with/slash"),
        ("CURIE_SCENARIO_NAMESPACE", "a" * 64),
        ("CURIE_SCENARIO_RELEASE", "Uppercase"),
        ("CURIE_SCENARIO_RELEASE", "with/slash"),
        ("CURIE_SCENARIO_RELEASE", "a" * 54),
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
def test_valid_run_id_reaches_shared_install(scenario: Scenario, run_id: str) -> None:
    scenario.environment["CURIE_SCENARIO_RUN_ID"] = run_id
    assert_reaches_shared_install(scenario)


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
def test_valid_image_reaches_shared_install(
    scenario: Scenario, variable: str, image: str
) -> None:
    scenario.environment[variable] = image
    assert_reaches_shared_install(scenario)


def assert_reaches_shared_install(scenario: Scenario) -> None:
    completed = scenario.run()
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, "the shared install boundary stub must stop the scenario"
    calls = scenario.calls()
    assert calls, (output, calls)
    assert all(call[0] == "kubectl" for call in calls), (output, calls)
    assert all("--context" in call and "kind-acme-test" in call for call in calls), calls
    assert all("test-workload-12345-1" in call for call in calls), calls
    for call in calls:
        if "delete" in call:
            targets = call[call.index("delete") + 1:]
            assert targets[0] in {"sandboxclaims", "sandboxes"}, call
    assert not any(variable in output for variable in INPUTS), output


def test_candidate_stack_provisioning_header_is_declared_once_in_first_twenty_lines() -> None:
    header = SCRIPT.read_text().splitlines()[:20]
    assert header.count("# fix-pin-provision: candidate-stack") == 1


@pytest.mark.parametrize(
    ("phase", "deletion_grace", "created", "fetch_finished", "accepted"),
    [
        ("Running", 30, 990, 1031, True),
        ("Running", 30, 990, 1029, False),
        ("Running", 30, 990, 1030, False),
        ("Running", 0, 990, 1001, True),
        ("Running", None, 990, 1031, False),
        ("Pending", None, 1001, 1031, True),
        ("Pending", None, 1000, 1031, False),
        ("Succeeded", None, 990, 1031, True),
        ("Failed", None, 990, 1031, True),
    ],
    ids=[
        "grace-expires-during-fetch",
        "within-grace",
        "at-deadline",
        "zero-grace",
        "live-running",
        "fresh-live-pending",
        "expired-live-pending",
        "succeeded",
        "failed",
    ],
)
def test_burst_quota_snapshot_at_fetch_completion(
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    deletion_grace: int | None,
    created: int,
    fetch_finished: int,
    accepted: bool,
) -> None:
    # Replay Kubernetes v1.31 QuotaV1Pod's terminal and deletion-grace rules:
    # https://github.com/kubernetes/kubernetes/blob/v1.31.0/pkg/quota/v1/evaluator/core/pods.go
    source = SCRIPT.read_text().split("cat >\"$WORKDIR/driver.py\" <<'PY'\n", 1)[1]
    driver = ast.parse(source.split("\nPY\n", 1)[0])
    helpers = ast.Module(
        body=[
            node
            for node in ast.walk(driver)
            if isinstance(node, ast.FunctionDef)
            and node.name in {"run", "get", "counts_against_quota", "quota_pods"}
        ],
        type_ignores=[],
    )
    namespace = dict(
        json=json,
        subprocess=subprocess,
        sys=sys,
        time=time,
        datetime=datetime,
        kube=["kubectl", "--context", "acme", "-n", "acme"],
        fullname="acme",
    )
    exec(compile(helpers, "<quota-scenario-driver>", "exec"), namespace)
    clock = [1000]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    retired = {
        "metadata": {
            "uid": "retired",
            "creationTimestamp": datetime.fromtimestamp(created, UTC).isoformat(),
        },
        "spec": {"priorityClassName": "sandbox"},
        "status": {"phase": phase},
    }
    if deletion_grace is not None:
        retired["metadata"].update(
            deletionTimestamp="1970-01-01T00:16:40Z", deletionGracePeriodSeconds=deletion_grace
        )
    pods = [
        retired,
        *[
            {
                "metadata": {"uid": uid},
                "spec": {"priorityClassName": "sandbox"},
                "status": {"phase": "Running"},
            }
            for uid in ("retained", "replacement")
        ],
        {
            "metadata": {"uid": "platform"},
            "spec": {"priorityClassName": "platform"},
            "status": {"phase": "Running"},
        },
    ]
    calls = []

    def kubectl(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(args[6])
        if args[6] == "resourcequota":
            payload = {
                "spec": {
                    "hard": {"pods": "2"},
                    "scopeSelector": {"matchExpressions": [{"values": ["sandbox"]}]},
                },
                "status": {"used": {"pods": "2"}},
            }
        else:
            assert args[6] == "pods", args
            clock[0] = fetch_finished
            payload = {"items": pods}
        return subprocess.CompletedProcess(args, 0, json.dumps(payload), "")

    monkeypatch.setattr(subprocess, "run", kubectl)
    if accepted:
        namespace["quota_pods"]()
    else:
        error = (
            "excess pre-runtime Pending sandbox pod exceeded 30 seconds"
            if phase == "Pending"
            else "observed more than two chargeable sandbox pods holding runtime"
        )
        with pytest.raises(AssertionError, match=f"^{error}$"):
            namespace["quota_pods"]()
    assert calls == ["resourcequota", "pods"]
