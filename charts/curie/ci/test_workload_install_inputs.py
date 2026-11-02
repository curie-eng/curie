"""Exercise the job-owned workload installation and teardown boundaries."""

from __future__ import annotations

import json
import os
import shlex
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent / "scenarios/workload-install.sh"
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
CRDS = (
    "sandboxes.agents.x-k8s.io",
    "sandboxclaims.extensions.agents.x-k8s.io",
    "sandboxtemplates.extensions.agents.x-k8s.io",
    "sandboxwarmpools.extensions.agents.x-k8s.io",
)
CLUSTER_OBJECTS = (
    ("namespace", "test-workload-12345-1"),
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
# Observed Kubernetes API snapshots showed a terminating namespace retaining
# its pods-remaining condition after all pods and PVCs had disappeared. The
# stateful mode models the pod-grace estimate and delayed reconciliation in:
# https://github.com/kubernetes/kubernetes/blob/v1.31.0/pkg/controller/namespace/deletion/namespaced_resources_deleter.go
# https://github.com/kubernetes/kubernetes/blob/v1.31.0/pkg/controller/namespace/namespace_controller.go
TOOL_STUB = r"""
import json
import os
import shlex
import sqlite3
import sys
from pathlib import Path

tool = Path(sys.argv[0]).name
args = sys.argv[1:]
with Path(os.environ["WORKLOAD_TEST_CALL_LOG"]).open("a") as log:
    log.write(shlex.join([tool, *args]) + "\n")
if os.environ.get("WORKLOAD_TEST_MODE") != "cluster":
    raise SystemExit(91)
state = None
if os.environ.get("WORKLOAD_TEST_MODE") == "cluster":
    state = sqlite3.connect(os.environ["WORKLOAD_TEST_CLUSTER_STATE"])
    state.executescript('''
        CREATE TABLE IF NOT EXISTS objects (identity TEXT PRIMARY KEY, document TEXT);
        CREATE TABLE IF NOT EXISTS namespaces (
            name TEXT PRIMARY KEY,
            namespace_present INTEGER,
            pods_present INTEGER,
            pod_delete_requested INTEGER,
            namespace_delete_requested INTEGER
        );
        CREATE TABLE IF NOT EXISTS events (
            sequence INTEGER PRIMARY KEY,
            action TEXT,
            namespace TEXT,
            pods_present INTEGER
        );
        INSERT OR IGNORE INTO namespaces VALUES ('acme-existing', 1, 1, 0, 0);
    ''')
    state.commit()


def record(action, namespace, pods_present):
    state.execute(
        "INSERT INTO events(action, namespace, pods_present) VALUES (?, ?, ?)",
        (action, namespace, pods_present),
    )
    state.commit()


namespace = next(
    (args[index + 1] for index in range(len(args) - 2, -1, -1)
     if args[index] in {"-n", "--namespace"}),
    "default",
)
if tool == "kubectl":
    if "create" in args or "apply" in args:
        import yaml
        filename = args[args.index("-f") + 1]
        with open(filename) as source:
            documents = list(yaml.safe_load_all(source))
        for document in documents:
            kind = document["kind"].lower()
            metadata = document["metadata"]
            name = metadata["name"]
            state.execute(
                "INSERT OR REPLACE INTO objects VALUES (?, ?)",
                (f"{kind}/{name}", json.dumps(document)),
            )
            if kind == "namespace":
                state.execute("INSERT OR REPLACE INTO namespaces VALUES (?, 1, 0, 0, 0)", (name,))
        state.commit()
        raise SystemExit(0)
    if "label" in args or "annotate" in args:
        action = "label" if "label" in args else "annotate"
        tail = args[args.index(action) + 1:]
        kind, separator, name = tail[0].partition("/")
        if not separator:
            name = tail[1]
        row = state.execute(
            "SELECT document FROM objects WHERE identity = ?", (f"{kind}/{name}",)
        ).fetchone()
        if row is None:
            raise SystemExit(1)
        document = json.loads(row[0])
        key = "labels" if action == "label" else "annotations"
        mapping = document["metadata"].setdefault(key, {})
        for setting in tail[1:]:
            if "=" in setting and not setting.startswith("-"):
                key, value = setting.split("=", 1)
                mapping[key] = value
        state.execute(
            "UPDATE objects SET document = ? WHERE identity = ?",
            (json.dumps(document), f"{kind}/{name}"),
        )
        state.commit()
        raise SystemExit(0)
    if "config" in args:
        if "current-context" in args or "get-contexts" in args:
            print(os.environ["CURIE_SCENARIO_CONTEXT"])
        raise SystemExit(0)
    if state is not None:
        action = next((argument for argument in args if argument in {"get", "delete", "wait"}), "")
        if action:
            tail = args[args.index(action) + 1:]
            target = next((argument for argument in tail if not argument.startswith("-")), "")
            kind, separator, name = target.partition("/")
            if not separator:
                index = tail.index(target) if target else -1
                name = (
                    tail[index + 1]
                    if index >= 0 and len(tail) > index + 1
                    and not tail[index + 1].startswith("-") else ""
                )
            if kind in {"pod", "pods"} and action in {"delete", "wait"}:
                targets = (
                    [row[0] for row in state.execute("SELECT name FROM namespaces")]
                    if "--all-namespaces" in args or "-A" in args else [namespace]
                )
                for target_namespace in targets:
                    row = state.execute(
                        "SELECT pods_present, pod_delete_requested FROM namespaces WHERE name = ?",
                        (target_namespace,),
                    ).fetchone()
                    if row is None:
                        continue
                    if action == "wait" and row[0] and not row[1]:
                        raise SystemExit(1)
                    waits = action == "wait" or "--wait=false" not in args
                    state.execute(
                        "UPDATE namespaces SET pod_delete_requested = 1, pods_present = ? "
                        "WHERE name = ?",
                        (0 if waits else row[0], target_namespace),
                    )
                    if os.environ.get("WORKLOAD_TEST_POD_DRAIN_FAILURE") == "1":
                        # The API accepted deletion, but the client's watch failed.
                        record("pod-drain-failed", target_namespace, 0 if waits else row[0])
                        raise SystemExit(97)
                    record(
                        "pod-drained" if waits else "pod-delete-requested",
                        target_namespace,
                        0 if waits else row[0],
                    )
                raise SystemExit(0)
            if kind in {"ns", "namespace", "namespaces"} and name:
                row = state.execute(
                    "SELECT namespace_present, pods_present, namespace_delete_requested "
                    "FROM namespaces WHERE name = ?",
                    (name,),
                ).fetchone()
                if row is not None:
                    if action == "delete":
                        state.execute(
                            "DELETE FROM objects WHERE identity = ?", (f"namespace/{name}",)
                        )
                        # Content can disappear while the pod-grace estimate
                        # still postpones the namespace's next reconciliation.
                        state.execute(
                            "UPDATE namespaces SET namespace_present = ?, pods_present = 0, "
                            "namespace_delete_requested = 1 WHERE name = ?",
                            (row[0] if row[2] else int(bool(row[1])), name),
                        )
                        record("namespace-delete", name, row[1])
                        raise SystemExit(0)
                    if action == "wait":
                        record("namespace-wait", name, row[1])
                        raise SystemExit(1 if row[0] else 0)
                    if row[0]:
                        if "json" in args or "-o=json" in args:
                            document = state.execute(
                                "SELECT document FROM objects WHERE identity = ?",
                                (f"namespace/{name}",),
                            ).fetchone()
                            payload = (
                                document[0] if document
                                else json.dumps({"metadata": {"name": name}})
                            )
                            print(payload)
                        else:
                            print(f"namespace/{name}")
                        raise SystemExit(0)
                    ignore_missing = (
                        "--ignore-not-found" in args or "--ignore-not-found=true" in args
                    )
                    raise SystemExit(0 if ignore_missing else 1)
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
        occupied = os.environ.get("WORKLOAD_TEST_OCCUPIED_OBJECT", "")
        retained = os.environ.get("WORKLOAD_TEST_EXISTING_CRDS", "").split(",")
        remains = os.environ.get("WORKLOAD_TEST_REMAINING_OBJECT", "")
        installed = Path(os.environ["WORKLOAD_TEST_INSTALL_MARKER"]).exists()
        present = identity == occupied or (installed and identity == remains)
        present |= kind == "customresourcedefinition" and name in retained
        row = state.execute(
            "SELECT document FROM objects WHERE identity = ?", (identity,)
        ).fetchone()
        if present or row:
            if "json" in args or "-o=json" in args:
                print(row[0] if row else json.dumps({"metadata": {"name": name}}))
            else:
                print(identity)
            raise SystemExit(0)
        ignore_missing = "--ignore-not-found" in args or "--ignore-not-found=true" in args
        raise SystemExit(0 if ignore_missing else 1)
    if "delete" in args:
        target = args[args.index("delete") + 1]
        kind, separator, name = target.partition("/")
        if not separator:
            index = args.index(target)
            name = args[index + 1] if len(args) > index + 1 else ""
        kind = {"crd": "customresourcedefinition"}.get(kind, kind)
        state.execute("DELETE FROM objects WHERE identity = ?", (f"{kind}/{name}",))
        state.commit()
        raise SystemExit(0)
    if "wait" in args:
        raise SystemExit(0)
    raise SystemExit(92)
if tool == "curie":
    if os.environ.get("WORKLOAD_TEST_INSTALL_OUTPUT"):
        print(os.environ["WORKLOAD_TEST_INSTALL_OUTPUT"], file=sys.stderr)
    if "--values-file" in args:
        import json
        import yaml
        values_file = args[args.index("--values-file") + 1]
        values = yaml.safe_load(Path(values_file).read_text())
        Path(os.environ["WORKLOAD_TEST_VALUES"]).write_text(json.dumps(values))
    Path(os.environ["WORKLOAD_TEST_INSTALL_MARKER"]).touch()
    if state is not None:
        release = args[args.index("--release") + 1]
        fullname = f"{release}-curie"
        annotations = {
            "meta.helm.sh/release-name": release,
            "meta.helm.sh/release-namespace": namespace,
        }
        identities = [
            "namespace/agent-sandbox-system",
            "clusterrole/agent-sandbox-controller",
            "clusterrole/agent-sandbox-controller-extensions",
            "clusterrole/agent-sandbox-controller-networkpolicies-read",
            "clusterrolebinding/agent-sandbox-controller",
            "clusterrolebinding/agent-sandbox-controller-extensions",
            "clusterrolebinding/agent-sandbox-controller-networkpolicies-read",
            "priorityclass/curie-platform", "priorityclass/curie-sandbox",
            f"namespace/{namespace}-{fullname}-publication",
            f"validatingadmissionpolicy/{fullname}-runner-resources",
            f"validatingadmissionpolicybinding/{fullname}-runner-resources",
        ]
        for crd in (
            "sandboxes.agents.x-k8s.io", "sandboxclaims.extensions.agents.x-k8s.io",
            "sandboxtemplates.extensions.agents.x-k8s.io",
            "sandboxwarmpools.extensions.agents.x-k8s.io",
        ):
            if crd not in os.environ.get("WORKLOAD_TEST_EXISTING_CRDS", "").split(","):
                identities.append(f"customresourcedefinition/{crd}")
        for identity in identities:
            kind, name = identity.split("/", 1)
            document = {"metadata": {"name": name, "annotations": annotations}}
            state.execute(
                "INSERT OR REPLACE INTO objects VALUES (?, ?)",
                (identity, json.dumps(document)),
            )
            if kind == "namespace":
                state.execute("INSERT OR REPLACE INTO namespaces VALUES (?, 1, 0, 0, 0)", (name,))
        state.execute("INSERT OR REPLACE INTO namespaces VALUES (?, 1, 1, 0, 0)", (namespace,))
        record("partial-install", namespace, 1)
    raise SystemExit(93)
if tool == "helm":
    if "uninstall" in args:
        if state is not None:
            row = state.execute(
                "SELECT pods_present FROM namespaces WHERE name = ?", (namespace,)
            ).fetchone()
            record("helm-uninstall", namespace, row[0] if row else 0)
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

    def run(self, action: str = "up") -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(SCRIPT), action],
            env=self.environment,
            capture_output=True,
            text=True,
            check=False,
            timeout=20,
        )

    def calls(self) -> list[list[str]]:
        return [shlex.split(line) for line in self.log.read_text().splitlines()]

    def cleanup_state(self) -> tuple[dict[str, tuple[int, int]], list[tuple[str, str, int]]]:
        with sqlite3.connect(self.environment["WORKLOAD_TEST_CLUSTER_STATE"]) as state:
            resources = {
                name: (namespace_present, pods_present)
                for name, namespace_present, pods_present in state.execute(
                    "SELECT name, namespace_present, pods_present FROM namespaces"
                )
            }
            events = list(
                state.execute(
                    "SELECT action, namespace, pods_present FROM events ORDER BY sequence"
                )
            )
        return resources, events


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
        "WORKLOAD_TEST_CALL_LOG": str(log),
        "WORKLOAD_TEST_INSTALL_MARKER": str(tmp_path / "installed"),
        "WORKLOAD_TEST_VALUES": str(tmp_path / "values.json"),
        "WORKLOAD_TEST_CLUSTER_STATE": str(tmp_path / "cluster-state.sqlite3"),
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


@pytest.fixture
def partial_install_with_pods(scenario: Scenario, tmp_path: Path) -> Scenario:
    scenario.environment["WORKLOAD_TEST_MODE"] = "cluster"
    scenario.environment["WORKLOAD_TEST_STATEFUL_CLEANUP"] = "1"
    scenario.environment["WORKLOAD_TEST_CLUSTER_STATE"] = str(tmp_path / "cluster-state.sqlite3")
    return scenario


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


@pytest.mark.parametrize(("kind", "name"), CLUSTER_OBJECTS)
def test_occupied_controller_or_priority_class_fails_before_install(
    scenario: Scenario, kind: str, name: str
) -> None:
    scenario.environment["WORKLOAD_TEST_MODE"] = "cluster"
    scenario.environment["WORKLOAD_TEST_OCCUPIED_OBJECT"] = f"{kind}/{name}"
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
    scenario.environment["WORKLOAD_TEST_MODE"] = "cluster"
    scenario.environment["WORKLOAD_TEST_EXISTING_CRDS"] = existing_crd or ""
    completed = scenario.run()
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, "a failed install must remain a failed scenario"
    assert "candidate cluster install failed" in output, output
    calls = scenario.calls()
    install_index = next(index for index, call in enumerate(calls) if call[0] == "curie")
    install = calls[install_index]
    assert "kind-acme-test" in install and "test-workload-12345-1" in install, install
    assert "workload-12345-1" in install, install
    assert all(call[0] not in {"cargo", "docker", "kind"} for call in calls), calls
    assert any(call[0] == "helm" and "uninstall" in call for call in calls), calls
    assert any("delete" in call and "test-workload-12345-1" in call for call in calls), calls
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


def test_partial_install_drains_owned_pods_before_namespace_deletion(
    partial_install_with_pods: Scenario,
) -> None:
    scenario = partial_install_with_pods
    completed = scenario.run()
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, "the primary install failure must remain a failed scenario"
    assert "candidate cluster install failed" in output, output
    assert (
        "OK: owned release, namespaces, controller resources, priority classes, "
        "and new CRDs are gone"
        in output
    ), output
    assert "owned namespace deletion did not complete" not in output, output
    assert "PASS:" not in output, output
    resources, events = scenario.cleanup_state()
    owned_namespace = "test-workload-12345-1"
    assert resources[owned_namespace] == (0, 0), resources
    assert resources["acme-existing"] == (1, 1), resources
    owned_events = [
        (action, pods) for action, namespace, pods in events if namespace == owned_namespace
    ]
    assert ("partial-install", 1) in owned_events, events
    assert ("namespace-delete", 0) in owned_events, events
    assert ("namespace-wait", 0) in owned_events, events
    uninstall_index = owned_events.index(("helm-uninstall", 1))
    drained_index = owned_events.index(("pod-drained", 0))
    namespace_delete_index = owned_events.index(("namespace-delete", 0))
    assert uninstall_index < drained_index < namespace_delete_index, events


def test_pod_drain_tool_failure_fails_cleanup_even_after_pods_disappear(
    partial_install_with_pods: Scenario,
) -> None:
    scenario = partial_install_with_pods
    scenario.environment["WORKLOAD_TEST_POD_DRAIN_FAILURE"] = "1"
    completed = scenario.run()
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, output
    assert "candidate cluster install failed" in output, output
    assert any(
        line.startswith("FAIL:") and "pod" in line.lower() for line in output.splitlines()
    ), output
    assert "OK: owned release" not in output, output
    assert "owned namespace deletion did not complete" not in output, output
    assert "PASS:" not in output, output
    resources, events = scenario.cleanup_state()
    owned_namespace = "test-workload-12345-1"
    assert resources[owned_namespace] == (0, 0), resources
    assert resources["acme-existing"] == (1, 1), resources
    assert ("pod-drain-failed", owned_namespace, 0) in events, events
    assert ("namespace-delete", owned_namespace, 0) in events, events


@pytest.mark.parametrize(
    "images",
    [
        ("curie-api:candidate", "curie-worker:candidate", "curie-runner:candidate"),
        (
            "registry.example.com:5000/acme-corp/curie-api:12345",
            "registry.example.com:5000/acme-corp/curie-worker:23456",
            "registry.example.com:5000/acme-corp/curie-runner:34567",
        ),
    ],
)
def test_install_preserves_candidate_image_tags_as_strings(
    scenario: Scenario, images: tuple[str, str, str]
) -> None:
    scenario.environment["WORKLOAD_TEST_MODE"] = "cluster"
    for component, image in zip(("API", "WORKER", "RUNNER"), images, strict=True):
        scenario.environment[f"CURIE_SCENARIO_{component}_IMAGE"] = image
    completed = scenario.run()
    assert completed.returncode != 0, completed.stdout + completed.stderr
    values = json.loads(Path(scenario.environment["WORKLOAD_TEST_VALUES"]).read_text())
    actual = [values[component]["image"] for component in ("api", "worker")]
    actual.append(values["agentSandbox"]["runner"])
    for settings, image in zip(actual, images, strict=True):
        repository, tag = image.rsplit(":", 1)
        assert settings.get("repository", settings.get("image")) == repository
        assert settings["tag"] == tag


@pytest.mark.parametrize("variable", ["CURIE_SCENARIO_NAMESPACE", "CURIE_SCENARIO_RELEASE"])
def test_down_refuses_missing_install_identity_before_tool_calls(
    scenario: Scenario, variable: str
) -> None:
    del scenario.environment[variable]
    completed = scenario.run("down")
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, output
    assert any(
        line.startswith("FAIL:") and variable in line for line in output.splitlines()
    ), output
    assert scenario.calls() == []



def test_failed_install_drains_claims_and_checks_publication_namespace_and_admission_policy(
    scenario: Scenario,
) -> None:
    scenario.environment["WORKLOAD_TEST_MODE"] = "cluster"
    completed = scenario.run()
    assert completed.returncode != 0, completed.stdout + completed.stderr
    calls = scenario.calls()
    install_index = next(index for index, call in enumerate(calls) if call[0] == "curie")
    cleanup = calls[install_index + 1:]
    ordered = []
    for tool, action, target in (
        ("kubectl", "delete", "sandboxclaims"),
        ("kubectl", "delete", "sandboxes"),
        ("helm", "uninstall", "workload-12345-1"),
        ("kubectl", "delete", "pods"),
        ("kubectl", "delete", "namespace"),
    ):
        matches = [
            call for call in cleanup if call[0] == tool and action in call and target in call
        ]
        if target == "pods":
            matches = [
                call for call in matches
                if [call[index + 1] for index, item in enumerate(call[:-1])
                    if item in {"-n", "--namespace"}][-1] == "test-workload-12345-1"
            ]
        assert len(matches) == 1, (tool, action, target, cleanup)
        ordered.append(cleanup.index(matches[0]))
        if action == "delete" and target in {"sandboxclaims", "sandboxes", "pods"}:
            assert "--all" in matches[0] and "--wait=true" in matches[0], matches[0]
            assert "test-workload-12345-1" in matches[0], matches[0]
    assert ordered == sorted(ordered), cleanup
    publication = "test-workload-12345-1-workload-12345-1-curie-publication"
    for namespace in ("test-workload-12345-1", "agent-sandbox-system", publication):
        matches = [call for call in cleanup if "wait" in call and f"namespace/{namespace}" in call]
        assert len(matches) == 1, (namespace, cleanup)
        assert "--for=delete" in matches[0], matches[0]
    for kind in ("validatingadmissionpolicy", "validatingadmissionpolicybinding"):
        assert any(
            "get" in call and f"{kind}/workload-12345-1-curie-runner-resources" in call
            for call in cleanup
        ), (kind, cleanup)


@pytest.mark.parametrize(
    "key",
    [
        "worker.claimTimeoutSeconds",
        "worker.routeTtlSeconds",
        "api.readinessProbe.failureThreshold",
        "api.livenessProbe.failureThreshold",
        "agentSandbox.runner.workspace.enabled",
        "worker.connectorReconciler.intervalSeconds",
    ],
)
def test_shared_install_leaves_scenario_timing_and_workspace_at_chart_defaults(
    scenario: Scenario, key: str,
) -> None:
    scenario.environment["WORKLOAD_TEST_MODE"] = "cluster"
    completed = scenario.run()
    assert completed.returncode != 0, "the install stub must stop at the owner boundary"
    installs = [call for call in scenario.calls() if call[0] == "curie"]
    assert len(installs) == 1, installs
    install = installs[0]
    settings = []
    for index, argument in enumerate(install):
        if argument in {"--set", "--set-string", "--set-json", "--set-file"}:
            settings.extend(install[index + 1].split(","))
        elif argument.startswith(("--set=", "--set-string=", "--set-json=", "--set-file=")):
            settings.extend(argument.split("=", 1)[1].split(","))
    assert not any(setting.partition("=")[0] == key for setting in settings), install
    values = json.loads(Path(scenario.environment["WORKLOAD_TEST_VALUES"]).read_text())
    value = values
    for part in key.split("."):
        if not isinstance(value, dict) or part not in value:
            break
        value = value[part]
    else:
        pytest.fail(f"shared installation overrides scenario-owned chart value {key}")


def test_failed_install_shows_sanitized_diagnostics_and_still_cleans_owned_resources(
    partial_install_with_pods: Scenario,
) -> None:
    scenario = partial_install_with_pods
    credentials = (
        "synthetic-api-credential",
        "synthetic-bearer-credential",
        "synthetic-database-credential",
    )
    scenario.environment["WORKLOAD_TEST_INSTALL_OUTPUT"] = (
        "install diagnostic: waiting for API rollout exceeded the deadline\n"
        f"CURIE_API_KEY={credentials[0]}\n"
        f"Authorization: Bearer {credentials[1]}\n"
        f"DATABASE_URL=postgresql://acme:{credentials[2]}@postgres.example.com/curie\n"
    )
    completed = scenario.run()
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0
    assert "candidate cluster install failed" in output, output
    assert "waiting for API rollout exceeded the deadline" in output, output
    assert all(credential not in output for credential in credentials), output
    resources, events = scenario.cleanup_state()
    assert resources["test-workload-12345-1"] == (0, 0), resources
    assert resources["acme-existing"] == (1, 1), resources
    assert ("namespace-delete", "test-workload-12345-1", 0) in events, events
    assert "OK: owned release" in output, output
