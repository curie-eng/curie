"""Exercise failure diagnostics through the real scenario and external CLI stubs."""

from __future__ import annotations

import json
import os
import re
import shlex
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import pytest

SCRIPT = Path(__file__).resolve().parent / "runtime/failed-first-install-retry-runtime.sh"
CONTEXT = "acme-diagnostics"
NAMESPACE = "test-4716-retry-acme-diag"
CRDS = (
    "sandboxes.agents.x-k8s.io",
    "sandboxclaims.extensions.agents.x-k8s.io",
    "sandboxtemplates.extensions.agents.x-k8s.io",
    "sandboxwarmpools.extensions.agents.x-k8s.io",
)
RESOURCES = ("pods", "deployments", "events", "nodes")
ALIASES = {"pod": "pods", "deployment": "deployments", "event": "events", "node": "nodes"}

# Only the external command boundary is replaced. No store or retry is simulated:
# the first up fails, and Helm cannot supply a readable release history.
TOOL_STUB = r"""
import json
import os
import sqlite3
import sys
from pathlib import Path

tool, args = Path(sys.argv[0]).name, sys.argv[1:]
with Path(os.environ["RETRY_CALL_LOG"]).open("a") as log:
    log.write(json.dumps([tool, *args]) + "\n")
canaries = json.loads(os.environ["RETRY_CANARIES"])
db = sqlite3.connect(os.environ["RETRY_STATE"])
owned = os.environ["RETRY_NAMESPACE"]

def option(*flags):
    for index, arg in enumerate(args):
        for flag in flags:
            if arg == flag:
                return args[index + 1]
            if arg.startswith(flag + "="):
                return arg.split("=", 1)[1]
    return None

def noise(code):
    print(canaries["stdout"])
    print(canaries["stderr"], file=sys.stderr)
    raise SystemExit(code)

assert option("--context", "--kube-context") == os.environ["CURIE_SCENARIO_CONTEXT"]
namespace = option("--namespace", "-n")
if tool == "curie":
    assert "cluster" in args and "up" in args and namespace == owned
    assert db.execute("SELECT present FROM namespaces WHERE name = ?", (owned,)).fetchone() == (1,)
    assert db.execute("SELECT count(*) FROM pvcs WHERE namespace = ?", (owned,)).fetchone() == (1,)
    with db:
        db.execute("UPDATE namespaces SET pods = 1 WHERE name = ?", (owned,))
    noise(37)
if tool == "helm":
    assert namespace == owned
    if "history" in args:
        noise(23)
    if "uninstall" in args:
        noise(0)
    if "list" in args:
        raise SystemExit(0)
    raise SystemExit(91)
assert tool == "kubectl"
action = next(arg for arg in args if arg in {"get", "create", "delete", "wait"})
tail = args[args.index(action) + 1:]
if action == "create" and "-f" in tail:
    assert namespace == owned
    claim = json.load(sys.stdin)
    assert claim["kind"] == "PersistentVolumeClaim"
    assert claim["metadata"]["namespace"] == owned
    assert claim["metadata"]["name"] == "unrelated-pre-retry-acme-diag"
    with db:
        db.execute("INSERT INTO pvcs VALUES (?, ?)", (claim["metadata"]["name"], owned))
    claim["metadata"]["uid"] = "acme-pre-pvc-uid"
    print(json.dumps(claim))
    raise SystemExit(0)
target = next(arg for arg in tail if not arg.startswith("-"))
kind, separator, name = target.partition("/")
if not separator:
    name = tail[1] if len(tail) > 1 and not tail[1].startswith("-") else ""
kind = {"ns": "namespace", "namespaces": "namespace", "pod": "pods",
        "deployment": "deployments", "event": "events", "node": "nodes",
        "crds": "crd", "customresourcedefinitions": "crd"}.get(kind, kind)
if action == "get" and kind in {"pods", "deployments", "events", "nodes"}:
    assert option("-o", "--output") == "json"
    assert namespace == owned or (kind == "nodes" and namespace is None)
    assert "-A" not in args and "--all-namespaces" not in args
    row = db.execute("SELECT present, pods FROM namespaces WHERE name = ?", (owned,)).fetchone()
    assert row == (1, 1)
    print(canaries["stderr"], file=sys.stderr)
    if kind == os.environ.get("RETRY_BROKEN_RESOURCE"):
        mode = os.environ["RETRY_BROKEN_MODE"]
        if mode == "read-error":
            print(canaries["stdout"] * 2000)
            print(canaries["stderr"] * 2000, file=sys.stderr)
            raise SystemExit(61)
        if mode == "invalid-json":
            print("{" + canaries["stdout"] * 2000)
        else:
            print(json.dumps({"items": {"message": canaries["status_message"]}}))
    else:
        print(json.dumps(json.loads(os.environ["RETRY_SNAPSHOTS"])[kind]))
    raise SystemExit(0)
if kind == "namespace":
    if action == "create":
        assert name == owned and namespace == owned
        with db:
            db.execute("INSERT INTO namespaces VALUES (?, 1, 0)", (name,))
        noise(0)
    row = db.execute("SELECT present, pods FROM namespaces WHERE name = ?", (name,)).fetchone()
    if action == "get":
        if row and row[0]:
            print("namespace/" + name)
        raise SystemExit(0)
    assert name == owned and namespace == owned
    if action == "delete":
        assert row == (1, 0), "pods must drain before namespace deletion"
        with db:
            db.execute("DELETE FROM pvcs WHERE namespace = ?", (owned,))
            db.execute("DELETE FROM namespaces WHERE name = ?", (owned,))
        noise(0)
    if action == "wait":
        assert row is None and "--for=delete" in args
        raise SystemExit(0)
if kind == "crd" and action == "get":
    if db.execute("SELECT name FROM crds WHERE name = ?", (name,)).fetchone():
        print("crd/" + name)
    raise SystemExit(0)
if kind == "pods" and action == "delete":
    assert namespace == owned and "--all" in args and "--wait=true" in args
    with db:
        db.execute("UPDATE namespaces SET pods = 0 WHERE name = ?", (owned,))
    noise(0)
raise SystemExit(92)
"""


def snapshots(canary: dict[str, str]) -> dict[str, dict]:
    metadata = {
        "name": canary["metadata"],
        "labels": {"acme-private": canary["metadata"]},
        "annotations": {"acme-private": canary["metadata"]},
    }

    def waiting(reason: str) -> dict:
        return {"waiting": {"reason": reason, "message": canary["status_message"]}}

    def container_status(
        name: str, state: dict, restarts: int = 0, previous: dict | None = None
    ) -> dict:
        return {
            "name": name,
            "image": "registry.invalid/acme:missing",
            "imageID": "",
            "ready": False,
            "restartCount": restarts,
            "state": state,
            "lastState": previous or {},
        }

    # core/v1 PodStatus has distinct init/app statuses and current/previous states.
    # https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#PodStatus
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": metadata,
        "spec": {
            "nodeName": canary["node_name"],
            "initContainers": [{"name": "init", "image": "registry.invalid/acme:missing"}],
            "containers": [
                {
                    "name": name,
                    "image": "registry.invalid/acme:missing",
                    "env": [{"name": "ACME_SECRET", "value": canary["container_env"]}],
                }
                for name in ("api", "worker", "runner", "unknown")
            ],
        },
        "status": {
            "phase": "Pending",
            "reason": canary["unknown_reason"],
            "message": canary["status_message"],
            "initContainerStatuses": [container_status("init", waiting("ErrImageNeverPull"))],
            "containerStatuses": [
                container_status(
                    "api",
                    waiting("CrashLoopBackOff"),
                    7,
                    {
                        "terminated": {
                            "reason": "OOMKilled",
                            "exitCode": 137,
                            "message": canary["error_message"],
                        }
                    },
                ),
                container_status("worker", waiting("ImagePullBackOff")),
                container_status("runner", waiting("ErrImagePull")),
                container_status("unknown", waiting(canary["unknown_reason"])),
            ],
        },
    }
    # apps/v1 DeploymentStatus.conditions includes type, status, reason, message.
    # https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/deployment-v1/#DeploymentStatus
    deployment = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": metadata,
        "status": {
            "replicas": 1,
            "availableReplicas": 0,
            "conditions": [
                {
                    "type": "Progressing",
                    "status": "False",
                    "reason": "ProgressDeadlineExceeded",
                    "message": canary["status_message"],
                },
                {
                    "type": "Available",
                    "status": "False",
                    "reason": canary["unknown_reason"],
                    "message": canary["error_message"],
                },
            ],
        },
    }
    # core/v1 Event reason/message are free text, including for Warning events.
    # https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/event-v1/
    event = {
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": metadata,
        "type": "Warning",
        "reason": "FailedScheduling",
        "message": canary["event_message"],
        "involvedObject": {"kind": "Pod", "name": canary["metadata"], "namespace": NAMESPACE},
    }
    # https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/node-v1/#NodeStatus
    node = {
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {"name": canary["node_name"]},
        "status": {
            "conditions": [
                {
                    "type": "MemoryPressure",
                    "status": "True",
                    "reason": canary["unknown_reason"],
                    "message": canary["status_message"],
                },
                {"type": "DiskPressure", "status": "False", "message": canary["status_message"]},
            ]
        },
    }
    items = {
        "pods": [pod],
        "deployments": [deployment],
        "nodes": [node],
        "events": [event, {**event, "reason": canary["unknown_reason"]}],
    }
    return {
        resource: {"apiVersion": "v1", "kind": "List", "items": rows}
        for resource, rows in items.items()
    }


@dataclass
class Scenario:
    environment: dict[str, str]
    log: Path
    database: Path
    canaries: dict[str, str]

    def run(self) -> tuple[subprocess.CompletedProcess[str], list[list[str]]]:
        completed = subprocess.run(
            ["bash", str(SCRIPT)],
            env=self.environment,
            capture_output=True,
            text=True,
            check=False,
            timeout=20,
        )
        return completed, [json.loads(line) for line in self.log.read_text().splitlines()]


@pytest.fixture
def scenario(tmp_path: Path) -> Scenario:
    tools = tmp_path / "tools"
    tools.mkdir()
    for name in ("kubectl", "helm", "curie"):
        executable = tools / name
        executable.write_text(f"#!{sys.executable}\n" + TOOL_STUB)
        executable.chmod(0o755)
    # The embedded script must use the caller's Python, including its PyYAML.
    python = tools / "python3"
    python.write_text("#!/bin/sh\nexec " + shlex.quote(sys.executable) + ' "$@"\n')
    python.chmod(0o755)
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text(
        "apiVersion: v1\nkind: Config\ncurrent-context: acme-diagnostics\n"
        "clusters:\n  - name: acme\n    cluster:\n      server: https://127.0.0.1:6443\n"
        "contexts:\n  - name: acme-diagnostics\n    context:\n      cluster: acme\n"
        "      user: acme\nusers:\n  - name: acme\n    user: {}\n"
    )
    log = tmp_path / "calls.log"
    log.touch()
    database = tmp_path / "cluster.sqlite3"
    with sqlite3.connect(database) as db:
        db.executescript("""
            CREATE TABLE namespaces (name TEXT PRIMARY KEY, present INTEGER, pods INTEGER);
            CREATE TABLE pvcs (name TEXT PRIMARY KEY, namespace TEXT);
            CREATE TABLE crds (name TEXT PRIMARY KEY);
            INSERT INTO namespaces VALUES ('acme-existing', 1, 1);
            INSERT INTO pvcs VALUES ('acme-existing-claim', 'acme-existing');
        """)
        db.executemany("INSERT INTO crds VALUES (?)", [(name,) for name in CRDS])
    canaries = {
        field: "acme-canary-" + uuid4().hex
        for field in (
            "container_env",
            "status_message",
            "error_message",
            "unknown_reason",
            "event_message",
            "stdout",
            "stderr",
            "metadata",
            "node_name",
        )
    }
    environment = {
        "PATH": str(tools) + os.pathsep + os.defpath,
        "HOME": str(tmp_path),
        "TMPDIR": str(tmp_path),
        "KUBECONFIG": str(kubeconfig),
        "HELM_DRIVER": "secret",
        "RETRY_CALL_LOG": str(log),
        "RETRY_STATE": str(database),
        "RETRY_NAMESPACE": NAMESPACE,
        "RETRY_CANARIES": json.dumps(canaries),
        "RETRY_SNAPSHOTS": json.dumps(snapshots(canaries)),
        "CURIE_BIN": str(tools / "curie"),
        "CURIE_SCENARIO_CONTEXT": CONTEXT,
        "CURIE_SCENARIO_RUN_ID": "acme-diag",
        "CURIE_SCENARIO_API_IMAGE": "curie-api:acme-diag",
        "CURIE_SCENARIO_WORKER_IMAGE": "curie-worker:acme-diag",
        "CURIE_SCENARIO_RUNNER_IMAGE": "curie-runner:acme-diag",
    }
    return Scenario(environment, log, database, canaries)


def assert_failure_and_cleanup(
    scenario: Scenario,
    completed: subprocess.CompletedProcess[str],
    calls: list[list[str]],
) -> str:
    output = completed.stdout + completed.stderr
    assert completed.returncode == 1, output
    assert re.search(r"first cluster up[^\n]*history", output, re.I), output
    assert re.search(r"\b37\b", output) and re.search(r"\b23\b", output), output
    assert "PASS:" not in output and "Traceback" not in output, output
    assert len(output) < 8000, "diagnostic output must remain bounded"
    for field, canary in scenario.canaries.items():
        assert canary not in output, f"unsafe {field} reached printed output"
    assert sum(call[0] == "curie" for call in calls) == 1, calls
    uninstall = next(i for i, call in enumerate(calls) if call[0] == "helm" and "uninstall" in call)
    drain = next(i for i, call in enumerate(calls) if "delete" in call and "pods" in call)
    delete = next(i for i, call in enumerate(calls) if "delete" in call and "namespace" in call)
    wait = next(i for i, call in enumerate(calls) if "wait" in call and "--for=delete" in call)
    assert uninstall < drain < delete < wait, calls
    assert any("get" in call and f"namespace/{NAMESPACE}" in call for call in calls[wait + 1 :]), (
        calls
    )
    assert any(call[0] == "helm" and "list" in call for call in calls[wait + 1 :]), calls
    for call in calls:
        if call[0] == "kubectl":
            assert "--context" in call and call[call.index("--context") + 1] == CONTEXT, call
            assert "--all-namespaces" not in call and "-A" not in call, call
            action = next((arg for arg in call if arg in {"get", "create", "delete", "wait"}), None)
            assert action is not None, f"unexpected cluster operation: {call}"
            if action in {"create", "delete"}:
                target = call[call.index(action) + 1 :]
                assert target[:2] == ["namespace", NAMESPACE] or target[0] in {"-f", "pods"}, call
        if call[0] == "curie" or any(verb in call for verb in ("create", "delete", "uninstall")):
            assert "--namespace" in call and call[call.index("--namespace") + 1] == NAMESPACE, call
            assert not any("crd/" in arg or arg in {"crd", "crds"} for arg in call), call
    with sqlite3.connect(scenario.database) as db:
        assert db.execute("SELECT * FROM namespaces").fetchall() == [("acme-existing", 1, 1)]
        assert db.execute("SELECT * FROM pvcs").fetchall() == [
            ("acme-existing-claim", "acme-existing")
        ]
        assert {row[0] for row in db.execute("SELECT name FROM crds")} == set(CRDS)
    return output


def assert_observations_before_cleanup(calls: list[list[str]]) -> None:
    first_cleanup = next(
        i for i, call in enumerate(calls) if "uninstall" in call or "delete" in call
    )
    history = next(i for i, call in enumerate(calls) if call[0] == "helm" and "history" in call)
    observed = set()
    for index, call in enumerate(calls):
        if call[0] != "kubectl" or "get" not in call:
            continue
        resource = call[call.index("get") + 1]
        resource = ALIASES.get(resource, resource)
        if resource in RESOURCES:
            assert history < index < first_cleanup, call
            observed.add(resource)
    assert observed == set(RESOURCES), (
        f"missing diagnostic observations: {set(RESOURCES) - observed}"
    )


def test_failed_install_prints_safe_structural_diagnostics_before_cleanup(
    scenario: Scenario,
) -> None:
    completed, calls = scenario.run()
    output = assert_failure_and_cleanup(scenario, completed, calls)
    assert_observations_before_cleanup(calls)
    for value in (
        "Pending",
        "CrashLoopBackOff",
        "OOMKilled",
        "ErrImageNeverPull",
        "ErrImagePull",
        "ImagePullBackOff",
        "ProgressDeadlineExceeded",
        "Warning",
        "FailedScheduling",
    ):
        assert re.search(r"\b" + value + r"\b", output), f"missing useful diagnostic value: {value}"
    assert re.search(r"\b137\b", output), "missing previous termination exit code"
    assert re.search(r"\b7\b", output), "missing container restart count"
    for condition, status in (("MemoryPressure", "True"), ("DiskPressure", "False")):
        assert re.search(condition + r"[\s\S]{0,100}\b" + status + r"\b", output, re.I), output


@pytest.mark.parametrize("resource", RESOURCES)
@pytest.mark.parametrize("mode", ("read-error", "invalid-json", "wrong-shape"))
def test_unavailable_diagnostic_preserves_failure_and_cleanup(
    scenario: Scenario,
    resource: str,
    mode: str,
) -> None:
    scenario.environment.update(RETRY_BROKEN_RESOURCE=resource, RETRY_BROKEN_MODE=mode)
    completed, calls = scenario.run()
    output = assert_failure_and_cleanup(scenario, completed, calls)
    assert_observations_before_cleanup(calls)
    assert re.search(r"\bunavailable\b", output, re.I), output


def test_invalid_input_owns_no_namespace_and_never_queries_diagnostics(scenario: Scenario) -> None:
    scenario.environment["CURIE_SCENARIO_RUN_ID"] = "Invalid/Run"
    completed, calls = scenario.run()
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0 and "CURIE_SCENARIO_RUN_ID" in output, output
    assert calls == [], "input rejection must precede all observations and cluster mutations"
    with sqlite3.connect(scenario.database) as db:
        assert db.execute("SELECT * FROM namespaces").fetchall() == [("acme-existing", 1, 1)]
        assert db.execute("SELECT * FROM pvcs").fetchall() == [
            ("acme-existing-claim", "acme-existing")
        ]
        assert {row[0] for row in db.execute("SELECT name FROM crds")} == set(CRDS)
