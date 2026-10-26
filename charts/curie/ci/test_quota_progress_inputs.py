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
from datetime import datetime
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
    assert not any(
        argument.split("=", 1)[0] == "--set-string" for argument in args
    )
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


@pytest.mark.parametrize(
    "images",
    [
        ("curie-api:wsdiag-acme", "curie-worker:wsdiag-acme", "curie-runner:wsdiag-acme"),
        (
            "registry.example.com:5000/acme-corp/curie-api:12345",
            "registry.example.com:5000/acme-corp/curie-worker:23456",
            "registry.example.com:5000/acme-corp/curie-runner:34567",
        ),
    ],
    ids=["ordinary-tags", "numeric-tags-and-registry-ports"],
)
def test_empty_inventory_advances_to_install_and_cleans_up_failed_install(
    scenario: Scenario, images: tuple[str, str, str]
) -> None:
    scenario.environment["QUOTA_INVENTORY_MODE"] = "1"
    for variable, image in zip(IMAGE_INPUTS, images, strict=True):
        scenario.environment[variable] = image
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
    assert not any(
        argument.split("=", 1)[0] == "--set-string" for argument in install
    ), install
    # This pins CLI argv preservation, including numeric tags and registry
    # ports. It does not assert how Helm later interprets those tag strings.
    sets = [
        install[index + 1] for index, argument in enumerate(install) if argument == "--set"
    ]
    for image, repository_key, tag_key in zip(
        images,
        ("api.image.repository", "worker.image.repository", "agentSandbox.runner.image"),
        ("api.image.tag", "worker.image.tag", "agentSandbox.runner.tag"),
        strict=True,
    ):
        repository, tag = image.rsplit(":", 1)
        for key, value in ((repository_key, repository), (tag_key, tag)):
            assert [setting for setting in sets if setting.startswith(key + "=")] == [
                f"{key}={value}"
            ], install
    install_index = calls.index(install)
    assert all(
        call[0] == "kubectl" and "get" in call for call in calls[:install_index]
    ), calls
    cleanup = calls[install_index + 1 :]
    namespace = "test-4368-idle-capacity-12345-1"
    claims = [
        call
        for call in cleanup
        if call[0] == "kubectl" and "delete" in call
        and "sandboxclaims" in call
    ]
    sandboxes = [
        call
        for call in cleanup
        if call[0] == "kubectl" and "delete" in call
        and "sandboxes" in call
    ]
    uninstalls = [call for call in cleanup if call[0] == "helm" and "uninstall" in call]
    pods = [
        call
        for call in cleanup
        if call[0] == "kubectl" and "delete" in call and "pods" in call
    ]
    namespace_deletions = [
        call
        for call in cleanup
        if call[0] == "kubectl" and "delete" in call
        and "namespace" in call and namespace in call
    ]
    assert len(claims) == len(sandboxes) == len(uninstalls) == 1, cleanup
    assert len(pods) == len(namespace_deletions) == 1, cleanup
    assert cleanup.index(claims[0]) < cleanup.index(sandboxes[0]), cleanup
    assert cleanup.index(sandboxes[0]) < cleanup.index(uninstalls[0]), cleanup
    assert cleanup.index(uninstalls[0]) < cleanup.index(pods[0]), cleanup
    assert cleanup.index(pods[0]) < cleanup.index(namespace_deletions[0]), cleanup
    for deletion in (claims[0], sandboxes[0]):
        assert "--all" in deletion and "--wait=true" in deletion, deletion
        assert "--timeout=90s" in deletion, deletion
        assert "-n" in deletion, deletion
        assert "test-4368-idle-capacity-12345-1" in deletion, deletion
    for flag in ("--all", "--ignore-not-found", "--wait=true", "--timeout=60s"):
        assert flag in pods[0], pods[0]
    assert "-n" in pods[0] and namespace in pods[0], pods[0]
    fullname = "idlecap-12345-1-curie"
    publication_namespace = f"{namespace}-{fullname}-publication"[:63].rstrip("-")
    for deleted_namespace in (namespace, "agent-sandbox-system", publication_namespace):
        waits = [
            call
            for call in cleanup
            if call[0] == "kubectl" and "wait" in call
            and f"namespace/{deleted_namespace}" in call
        ]
        assert len(waits) == 1, (deleted_namespace, cleanup)
        assert cleanup.index(namespace_deletions[0]) < cleanup.index(waits[0]), cleanup
        assert "--for=delete" in waits[0] and "--timeout=180s" in waits[0], waits[0]
    for kind in ("validatingadmissionpolicy", "validatingadmissionpolicybinding"):
        assert any(
            call[0] == "kubectl"
            and "get" in call
            and f"{kind}/{fullname}-runner-resources" in call
            for call in cleanup
        ), (kind, cleanup)
    assert all(call[0] not in {"kind", "docker", "cargo"} for call in calls), calls


def test_candidate_stack_provisioning_header_is_declared_once_in_first_twenty_lines() -> None:
    header = SCRIPT.read_text().splitlines()[:20]
    assert header.count("# fix-pin-provision: candidate-stack") == 1


@pytest.mark.parametrize(
    ("phase", "deletion_grace", "fetch_finished", "accepted"),
    [
        ("Running", 30, 1031, True),
        ("Running", 30, 1029, False),
        ("Running", 30, 1030, False),
        ("Running", 0, 1001, True),
        ("Running", None, 1031, False),
        ("Pending", None, 1031, False),
        ("Succeeded", None, 1031, True),
        ("Failed", None, 1031, True),
    ],
    ids=[
        "grace-expires-during-fetch",
        "within-grace",
        "at-deadline",
        "zero-grace",
        "live-running",
        "live-pending",
        "succeeded",
        "failed",
    ],
)
def test_burst_quota_snapshot_at_fetch_completion(
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    deletion_grace: int | None,
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
        "metadata": {"uid": "retired"},
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
        with pytest.raises(AssertionError, match="more than two chargeable sandbox pods"):
            namespace["quota_pods"]()
    assert calls == ["resourcequota", "pods"]
