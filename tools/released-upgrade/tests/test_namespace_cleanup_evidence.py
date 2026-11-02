"""Independent bounded/sanitized namespace-cleanup diagnostic contract.

@spec NAMESPACE-CLEANUP-EVIDENCE-1 NAMESPACE-CLEANUP-EVIDENCE-2
@spec NAMESPACE-CLEANUP-EVIDENCE-3 NAMESPACE-CLEANUP-EVIDENCE-4.
"""

from __future__ import annotations

import importlib.util
import json
import socket
import subprocess
import sys
import time
import tomllib
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "tools/released-upgrade/namespace_cleanup_evidence.py"
WORKFLOW = ROOT / ".github/workflows/ci.yaml"
CONTEXT = "kind-owned-negative-control"
NAMESPACE = "owned-profile"
SECRET = "SyntheticCredentialMustNotAppear"


def load() -> Any:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-1: implementation absence is a body RED."""
    assert SOURCE.is_file(), "namespace cleanup evidence collector is not implemented"
    spec = importlib.util.spec_from_file_location("namespace_cleanup_evidence_tested", SOURCE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    assert callable(getattr(module, "collect_cleanup_evidence", None))
    assert callable(getattr(module, "_run", None))
    return module


def workflow_job() -> dict[str, Any]:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-4."""
    return yaml.safe_load(WORKFLOW.read_text())["jobs"]["e2e-released-upgrade-negative"]


def test_original_upgrade_assertions_wait_and_required_cleanup_remain() -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-4: a diagnostic must never waive failure."""
    job = workflow_job()
    steps = job["steps"]
    plain = next(
        step
        for step in steps
        if step.get("name", "").startswith(
            "Factory resource profile survives a plain retained-values upgrade"
        )
    )
    commands = plain["run"]
    assert "set -euo pipefail" in commands
    assert "released-upgrade-resource-profile-assertions.sh" in commands
    assert "--reset-then-reuse-values --timeout 15m" in commands
    assert 'helm uninstall "$release" -n "$ns" --wait --timeout 5m' in commands
    assert 'kubectl delete namespace "$ns" --wait=true --timeout=300s' in commands
    teardown = next(
        step
        for step in steps
        if step.get("name") == "Tear down the disposable negative control cluster"
    )
    assert teardown["if"] == "always()"
    assert "kind delete cluster --name curie-upgrade-negative" in teardown["run"]
    assert "--ignore-not-found --wait=false" in teardown["run"]
    required = yaml.safe_load(WORKFLOW.read_text())["jobs"]["e2e-required"]
    assert "e2e-released-upgrade-negative" in required["needs"]


def test_failure_only_safe_report_and_artifact_precede_owned_cluster_teardown() -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-4."""
    steps = workflow_job()["steps"]
    collectors = [
        (i, step)
        for i, step in enumerate(steps)
        if "namespace_cleanup_evidence" in step.get("run", "")
    ]
    assert len(collectors) == 1, "failure diagnostics do not collect namespace evidence"
    position, collector = collectors[0]
    assert "failure()" in collector.get("if", "")
    run = collector["run"]
    assert "--context" in run and "kind-curie-upgrade-negative" in run
    assert "--namespace" in run and "curie-negative" in run and "curie-profile" in run
    assert "current-context" not in run
    artifacts = [
        (i, step)
        for i, step in enumerate(steps)
        if "actions/upload-artifact@" in step.get("uses", "")
        and "cleanup" in step.get("with", {}).get("name", "")
    ]
    assert len(artifacts) == 1, "safe cleanup report has no dedicated artifact"
    artifact_position, artifact = artifacts[0]
    assert "failure()" in artifact.get("if", "")
    path = artifact["with"]["path"]
    assert path.endswith(".json") and "\n" not in path
    assert not any(character in path for character in "*?[]")
    assert Path(path).name in run
    teardown = next(
        i
        for i, step in enumerate(steps)
        if step.get("name") == "Tear down the disposable negative control cluster"
    )
    assert position < artifact_position < teardown


@pytest.mark.parametrize(
    ("context", "namespaces", "budget"),
    [
        ("", [NAMESPACE], 40),
        (None, [NAMESPACE], 40),
        (b"secret-context", [NAMESPACE], 40),
        (True, [NAMESPACE], 40),
        (CONTEXT, [], 40),
        (CONTEXT, ["--all"], 40),
        (CONTEXT, ["Uppercase"], 40),
        (CONTEXT, ["a.b"], 40),
        (CONTEXT, ["-leading"], 40),
        (CONTEXT, ["trailing-"], 40),
        (CONTEXT, ["a" * 64], 40),
        (CONTEXT, [None], 40),
        (CONTEXT, [NAMESPACE, NAMESPACE], 40),
        (CONTEXT, ["one", "two", "three", "four", "five"], 40),
        (CONTEXT, [NAMESPACE], True),
        (CONTEXT, [NAMESPACE], 0),
        (CONTEXT, [NAMESPACE], 61),
        (CONTEXT, [NAMESPACE], float("nan")),
        (CONTEXT, [NAMESPACE], float("inf")),
        (CONTEXT, [NAMESPACE], "40"),
    ],
)
def test_invalid_inputs_refuse_before_any_command_with_fixed_safe_error(
    context: Any,
    namespaces: Any,
    budget: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-1."""
    module = load()
    calls = []

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        """@spec NAMESPACE-CLEANUP-EVIDENCE-1."""
        calls.append((args, kwargs))
        raise AssertionError("invalid input reached external command")

    monkeypatch.setattr(module, "_run", forbidden)
    with pytest.raises(ValueError) as caught:
        module.collect_cleanup_evidence(context, namespaces, budget_s=budget)
    assert calls == []
    assert str(caught.value) == "invalid_cleanup_evidence_input"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None or caught.value.__suppress_context__


def flag(command: list[str], name: str) -> str | None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-1: permit equivalent kubectl flag forms."""
    for index, argument in enumerate(command):
        if argument == name:
            return command[index + 1] if index + 1 < len(command) else None
        if argument.startswith(name + "="):
            return argument.split("=", 1)[1]
    return None


class Kubernetes:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-1: synthetic external service only."""

    def __init__(self) -> None:
        """@spec NAMESPACE-CLEANUP-EVIDENCE-2."""
        self.discovery = ["secrets", "pods"]
        self.namespace = {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": NAMESPACE,
                "deletionTimestamp": "2030-01-01T00:00:00Z",
                "finalizers": ["example.test/metadata"],
                "annotations": {"secret": SECRET},
            },
            "spec": {"finalizers": ["kubernetes"], "secret": SECRET},
            "status": {
                "phase": "Terminating",
                "conditions": [
                    {
                        "type": "NamespaceContentRemaining",
                        "status": "True",
                        "reason": "SomeResourcesRemain",
                        "message": SECRET,
                    },
                ],
            },
        }
        self.services = {
            "apiVersion": "apiregistration.k8s.io/v1",
            "kind": "APIServiceList",
            "items": [
                {
                    "metadata": {"name": "v1.available.example.test"},
                    "spec": {"secret": SECRET},
                    "status": {
                        "conditions": [
                            {
                                "type": "Available",
                                "status": "True",
                                "reason": "Passed",
                                "message": SECRET,
                            }
                        ]
                    },
                },
                {
                    "metadata": {
                        "name": "v1.unavailable.example.test",
                        "annotations": {"x": SECRET},
                    },
                    "spec": {"secret": SECRET},
                    "status": {
                        "conditions": [
                            {
                                "type": "Available",
                                "status": "False",
                                "reason": "MissingEndpoints",
                                "message": SECRET,
                            }
                        ]
                    },
                },
                {
                    "metadata": {"name": "v1.unknown.example.test"},
                    "status": {
                        "conditions": [
                            {
                                "type": "Available",
                                "status": "Unknown",
                                "reason": "UnavailableProbe",
                                "message": SECRET,
                            }
                        ]
                    },
                },
            ],
        }
        self.projections = {
            "secrets": [
                {"finalizers": ["example.test/retain"], "deleting": True, "owner_kinds": ["Job"]},
                {"finalizers": ["example.test/retain"], "deleting": False, "owner_kinds": ["Job"]},
            ],
            "pods": [],
        }
        self.overrides: dict[str, Any] = {}
        self.calls: list[dict[str, Any]] = []
        self.clock: Any = None
        self.consume = 0.0

    def operation(self, command: list[str]) -> str:
        """@spec NAMESPACE-CLEANUP-EVIDENCE-1."""
        if "api-resources" in command:
            return "discovery"
        assert "get" in command, "collector requested a non-read kubectl operation"
        if any(value in command for value in ("namespace", "namespaces", "ns")):
            return "namespace"
        if any(
            value in command
            for value in (
                "apiservice",
                "apiservices",
                "apiservices.apiregistration.k8s.io",
            )
        ):
            return "api_services"
        resource = next((name for name in self.discovery if name in command), None)
        assert resource is not None, "collector used an undiscovered resource argument"
        return "resource:" + resource

    def run(self, command: list[str], timeout_s: float) -> subprocess.CompletedProcess[str]:
        """@spec NAMESPACE-CLEANUP-EVIDENCE-1 NAMESPACE-CLEANUP-EVIDENCE-3."""
        assert isinstance(command, list) and all(isinstance(value, str) for value in command)
        assert Path(command[0]).name == "kubectl"
        assert flag(command, "--context") == CONTEXT
        assert not any(
            value in command
            for value in (
                "config",
                "current-context",
                "patch",
                "delete",
                "create",
                "apply",
                "replace",
                "edit",
            )
        )
        assert 0 < timeout_s <= 5
        operation = self.operation(command)
        self.calls.append(
            {
                "command": command[:],
                "timeout": timeout_s,
                "operation": operation,
                "started": self.clock.value if self.clock else 0.0,
            }
        )
        if self.clock:
            self.clock.value += min(self.consume, timeout_s)
        value = self.overrides.get(operation)
        if isinstance(value, BaseException):
            raise value
        if callable(value):
            return value(command, timeout_s)
        if value is None:
            if operation == "discovery":
                value = "\n".join(self.discovery) + "\n"
            elif operation == "namespace":
                value = json.dumps(self.namespace)
            elif operation == "api_services":
                value = json.dumps(self.services)
            else:
                value = json.dumps(self.projections.get(operation.removeprefix("resource:"), []))
        if isinstance(value, subprocess.CompletedProcess):
            return value
        return subprocess.CompletedProcess(command, 0, stdout=value, stderr=SECRET)


class Clock:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-3: CI-only monotonic budget seam."""

    def __init__(self) -> None:
        """@spec NAMESPACE-CLEANUP-EVIDENCE-3."""
        self.value = 0.0

    def __call__(self) -> float:
        """@spec NAMESPACE-CLEANUP-EVIDENCE-3."""
        return self.value


def collect(module: Any, world: Kubernetes, monkeypatch: pytest.MonkeyPatch, **kwargs: Any) -> Any:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-1."""
    monkeypatch.setattr(module, "_run", world.run)
    return module.collect_cleanup_evidence(CONTEXT, [NAMESPACE], **kwargs)


def errors(report: dict[str, Any]) -> list[dict[str, Any]]:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-2."""
    return report["errors"] + [
        error for namespace in report["namespaces"] for error in namespace["errors"]
    ]


def safe(report: dict[str, Any]) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-2: reject free-form diagnostic escape."""
    assert set(report) == {"schema_version", "complete", "namespaces", "api_services", "errors"}
    assert report["schema_version"] == 1 and type(report["complete"]) is bool
    rendered = json.dumps(report)
    assert SECRET not in rendered
    assert "synthetic-workload-name" not in rendered
    assert "synthetic-owner-name" not in rendered
    for namespace in report["namespaces"]:
        assert set(namespace) == {
            "namespace",
            "phase",
            "deleting",
            "finalizers",
            "conditions",
            "resources",
            "errors",
        }
        for condition in namespace["conditions"]:
            assert set(condition) == {"type", "status", "reason"}
        for resource in namespace["resources"]:
            assert set(resource) == {"kind", "count", "deleting", "finalizers", "owner_kinds"}
    for service in report["api_services"]:
        assert set(service) == {"name", "status", "reason"}
    for error in errors(report):
        assert set(error) <= {"operation", "resource", "reason"}
        assert isinstance(error.get("reason"), str)
        assert len(error["reason"]) <= 256
        assert all(isinstance(value, str) and len(value) <= 256 for value in error.values())


def test_selected_namespace_resource_and_unavailable_api_metadata_are_safe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-1 NAMESPACE-CLEANUP-EVIDENCE-2."""
    module, world = load(), Kubernetes()
    report = collect(module, world, monkeypatch)
    safe(report)
    assert report["complete"] is True
    assert errors(report) == []
    namespace = report["namespaces"][0]
    assert namespace["namespace"] == NAMESPACE
    assert namespace["phase"] == "Terminating" and namespace["deleting"] is True
    assert set(namespace["finalizers"]) == {"kubernetes", "example.test/metadata"}
    assert namespace["conditions"] == [
        {
            "type": "NamespaceContentRemaining",
            "status": "True",
            "reason": "SomeResourcesRemain",
        }
    ]
    assert namespace["resources"] == [
        {
            "kind": "secrets",
            "count": 2,
            "deleting": 1,
            "finalizers": {"example.test/retain": 2},
            "owner_kinds": {"Job": 2},
        }
    ]
    assert sorted(report["api_services"], key=lambda service: service["name"]) == [
        {"name": "v1.unavailable.example.test", "status": "False", "reason": "MissingEndpoints"},
        {"name": "v1.unknown.example.test", "status": "Unknown", "reason": "UnavailableProbe"},
    ]


@pytest.fixture(autouse=True)
def forbid_real_processes_and_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-1: no real kubectl or cluster prerequisite."""

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        """@spec NAMESPACE-CLEANUP-EVIDENCE-1."""
        raise AssertionError("unit test attempted a real external process or network connection")

    monkeypatch.setattr(subprocess, "run", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)


def test_import_performs_no_external_command_or_network_io() -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-1."""
    # The autouse fixture is installed before the actual module executes.
    module = load()
    assert callable(module.collect_cleanup_evidence)


def test_root_pytest_collects_the_new_diagnostic_contract_tests() -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-4."""
    configured = tomllib.loads((ROOT / "pyproject.toml").read_text())
    paths = configured["tool"]["pytest"]["ini_options"]["testpaths"]
    owned = Path(__file__).resolve().relative_to(ROOT)
    assert any(owned.is_relative_to(Path(path)) for path in paths), (
        "root pytest testpaths do not collect released-upgrade diagnostic tests"
    )


def test_projection_limit_counts_utf8_bytes_before_parsing_or_symbol_folding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-3."""
    module, world = load(), Kubernetes()
    value = json.dumps(
        [
            {
                "finalizers": ["Ω" * (512 * 1024)],
                "deleting": False,
                "owner_kinds": [],
            }
        ],
        ensure_ascii=False,
    )
    assert len(value) < 1024 * 1024 < len(value.encode())
    world.overrides["resource:secrets"] = value
    report = collect(module, world, monkeypatch)
    safe(report)
    assert report["complete"] is False
    assert "oversized_projection" in {error["reason"] for error in errors(report)}


def test_absent_available_condition_is_reported_as_unknown_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-2."""
    module, world = load(), Kubernetes()
    world.services["items"] = [{"metadata": {"name": "v1.unobserved.example.test"}, "status": {}}]
    report = collect(module, world, monkeypatch)
    safe(report)
    assert len(report["api_services"]) == 1
    assert report["api_services"][0]["name"] == "v1.unobserved.example.test"
    assert report["api_services"][0]["status"] == "Unknown"


@pytest.mark.parametrize("field", ["namespace", "api_services"])
def test_malformed_top_level_selected_metadata_does_not_become_empty_success(
    field: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-2 NAMESPACE-CLEANUP-EVIDENCE-3."""
    module, world = load(), Kubernetes()
    world.overrides[field] = json.dumps({"items": SECRET, "metadata": {"finalizers": 1}})
    report = collect(module, world, monkeypatch)
    safe(report)
    assert report["complete"] is False and errors(report)


def test_multiple_valid_namespaces_are_reported_separately_without_merging_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-1 NAMESPACE-CLEANUP-EVIDENCE-2."""
    module, world = load(), Kubernetes()
    supplied = ["owned-first", "owned-second", "owned-third", "owned-fourth"]

    def namespace_response(command: list[str], _timeout: float) -> Any:
        """@spec NAMESPACE-CLEANUP-EVIDENCE-2."""
        requested = next(name for name in supplied if name in command)
        body = json.loads(json.dumps(world.namespace))
        body["metadata"]["name"] = requested
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps(body), stderr=SECRET)

    world.overrides["namespace"] = namespace_response
    monkeypatch.setattr(module, "_run", world.run)
    report = module.collect_cleanup_evidence(CONTEXT, supplied)
    safe(report)
    assert report["complete"] is True
    assert {namespace["namespace"] for namespace in report["namespaces"]} == set(supplied)
    assert all(namespace["resources"][0]["count"] == 2 for namespace in report["namespaces"])


def test_cli_partial_evidence_is_json_and_never_a_cleanup_success_claim(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-1 NAMESPACE-CLEANUP-EVIDENCE-3."""
    module, world = load(), Kubernetes()
    world.overrides["resource:secrets"] = subprocess.CompletedProcess(
        ["kubectl"],
        1,
        stdout=SECRET,
        stderr=SECRET,
    )
    monkeypatch.setattr(module, "_run", world.run)
    status = module.main(["--context", CONTEXT, "--namespace", NAMESPACE, "--budget-seconds", "1"])
    output = capsys.readouterr()
    assert type(status) is int
    assert SECRET not in output.out + output.err
    report = json.loads(output.out)
    safe(report)
    assert report["complete"] is False
    assert errors(report)


def test_workload_commands_request_only_projected_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-1 NAMESPACE-CLEANUP-EVIDENCE-2."""
    module, world = load(), Kubernetes()
    collect(module, world, monkeypatch)
    for call in world.calls:
        command = call["command"]
        if call["operation"] == "discovery":
            assert flag(command, "--namespaced") in {"true", None}
            assert "--namespaced" in command or flag(command, "--namespaced") == "true"
            assert flag(command, "--verbs") == "list"
        elif call["operation"].startswith("resource:"):
            assert flag(command, "-n") == NAMESPACE or flag(command, "--namespace") == NAMESPACE
            rendered = " ".join(command)
            assert "go-template" in rendered
            for forbidden in (
                ".metadata.name",
                ".metadata.namespace",
                ".name",
                ".uid",
                ".labels",
                ".annotations",
                ".spec",
                ".data",
                "{{.}}",
                "{{ . }}",
            ):
                assert forbidden not in rendered


@pytest.mark.parametrize("where", ["namespace", "api_services", "resource:secrets"])
def test_unknown_symbols_become_unrecognized_without_echoing_input(
    where: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-2."""
    module, world = load(), Kubernetes()
    invalid = SECRET + " Ω"
    if where == "namespace":
        world.namespace["metadata"]["finalizers"] = [invalid, "a" * 257]
        world.namespace["status"]["conditions"][0]["reason"] = invalid
    elif where == "api_services":
        world.services["items"][1]["metadata"]["name"] = invalid
        world.services["items"][1]["status"]["conditions"][0]["reason"] = "a" * 257
    else:
        world.projections["secrets"] = [
            {
                "finalizers": [invalid, "a" * 257],
                "deleting": False,
                "owner_kinds": [invalid],
            }
        ]
    report = collect(module, world, monkeypatch)
    safe(report)
    assert invalid not in json.dumps(report)
    assert "a" * 257 not in json.dumps(report)
    assert "unrecognized" in json.dumps(report)


@pytest.mark.parametrize(
    "value",
    [
        "not-json",
        "{}",
        '[{"finalizers":[],"deleting":"false","owner_kinds":[]}]',
        '[{"finalizers":{},"deleting":false,"owner_kinds":[]}]',
        '[{"finalizers":[],"deleting":0,"owner_kinds":[]}]',
        '[{"finalizers":[1],"deleting":false,"owner_kinds":[]}]',
        '[{"finalizers":[],"deleting":false,"owner_kinds":[true]}]',
    ],
)
def test_invalid_workload_projection_is_partial_not_coerced_or_reported_empty(
    value: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-2 NAMESPACE-CLEANUP-EVIDENCE-3."""
    module, world = load(), Kubernetes()
    world.overrides["resource:secrets"] = value
    report = collect(module, world, monkeypatch)
    safe(report)
    assert report["complete"] is False
    assert errors(report)
    assert "invalid_projection" in {error["reason"] for error in errors(report)}


@pytest.mark.parametrize(
    ("operation", "fault", "reason"),
    [
        ("namespace", "missing", "not_found"),
        ("resource:secrets", "error", "command_error"),
        ("api_services", "timeout", "timeout"),
        ("discovery", "error", "command_error"),
        ("namespace", "bad-json", "invalid_projection"),
        ("api_services", "bad-json", "invalid_projection"),
    ],
)
def test_external_failures_remain_safe_partial_evidence_without_retry(
    operation: str,
    fault: str,
    reason: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-2 NAMESPACE-CLEANUP-EVIDENCE-3."""
    module, world = load(), Kubernetes()
    if fault == "missing":
        world.overrides[operation] = subprocess.CompletedProcess(
            ["kubectl"],
            1,
            stdout=SECRET,
            stderr=(
                'Error from server (NotFound): namespaces "' + NAMESPACE + '" not found ' + SECRET
            ),
        )
    elif fault == "error":
        world.overrides[operation] = subprocess.CompletedProcess(
            ["kubectl"],
            1,
            stdout=SECRET,
            stderr=SECRET,
        )
    elif fault == "timeout":
        world.overrides[operation] = subprocess.TimeoutExpired(
            ["kubectl"],
            5,
            output=SECRET,
            stderr=SECRET,
        )
    else:
        world.overrides[operation] = SECRET
    report = collect(module, world, monkeypatch)
    safe(report)
    assert report["complete"] is False
    assert reason in {error["reason"] for error in errors(report)}
    assert sum(call["operation"] == operation for call in world.calls) == 1
    if operation == "namespace":
        assert report["namespaces"]
        assert report["namespaces"][0]["errors"]


@pytest.mark.parametrize(
    "name",
    [
        "--all",
        "pods;echo",
        "pods/exec",
        "pods --all",
        "pods..example.test",
        "a" * 257,
    ],
)
def test_discovery_names_never_become_unvalidated_command_arguments(
    name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-1 NAMESPACE-CLEANUP-EVIDENCE-3."""
    module, world = load(), Kubernetes()
    world.discovery = ["secrets"]
    world.overrides["discovery"] = name + "\nsecrets\n"
    report = collect(module, world, monkeypatch)
    safe(report)
    assert report["complete"] is False
    assert errors(report)
    assert all(name not in call["command"] for call in world.calls)
    assert name not in json.dumps(report)


@pytest.mark.parametrize("count", [128, 129])
def test_discovery_kind_limit_is_enforced_without_unbounded_queries(
    count: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-3."""
    module, world = load(), Kubernetes()
    world.discovery = [f"resources{number}.example.test" for number in range(count)]
    report = collect(module, world, monkeypatch)
    safe(report)
    queried = [call for call in world.calls if call["operation"].startswith("resource:")]
    assert len(queried) <= 128
    if count == 128:
        assert report["complete"] is True and len(queried) == 128
    else:
        assert report["complete"] is False
        assert "resource_limit" in {error["reason"] for error in errors(report)}


@pytest.mark.parametrize(("size", "complete"), [(1024 * 1024, True), (1024 * 1024 + 1, False)])
def test_projection_byte_bound_is_inclusive(
    size: int,
    complete: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-3."""
    module, world = load(), Kubernetes()
    world.overrides["resource:secrets"] = "[]" + " " * (size - 2)
    report = collect(module, world, monkeypatch)
    safe(report)
    assert report["complete"] is complete
    if not complete:
        assert "oversized_projection" in {error["reason"] for error in errors(report)}


@pytest.mark.parametrize(("count", "complete"), [(1024, True), (1025, False)])
def test_projection_item_bound_is_inclusive(
    count: int,
    complete: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-3."""
    module, world = load(), Kubernetes()
    world.projections["secrets"] = [
        {"finalizers": [], "deleting": False, "owner_kinds": []} for _ in range(count)
    ]
    report = collect(module, world, monkeypatch)
    safe(report)
    assert report["complete"] is complete
    if complete:
        assert report["namespaces"][0]["resources"][0]["count"] == count
    else:
        assert errors(report)


@pytest.mark.parametrize("field", ["finalizers", "owner_kinds"])
@pytest.mark.parametrize(("count", "complete"), [(64, True), (65, False)])
def test_per_object_metadata_list_bound_is_inclusive(
    field: str,
    count: int,
    complete: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-3."""
    module, world = load(), Kubernetes()
    item = {"finalizers": [], "deleting": False, "owner_kinds": []}
    item[field] = ["ExampleSymbol"] * count
    world.projections["secrets"] = [item]
    report = collect(module, world, monkeypatch)
    safe(report)
    assert report["complete"] is complete
    if not complete:
        assert errors(report)


def test_one_deadline_bounds_each_command_and_stops_further_collection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-3."""
    clock = Clock()
    monkeypatch.setattr(time, "monotonic", clock)
    module, world = load(), Kubernetes()
    world.clock, world.consume = clock, 5
    report = collect(module, world, monkeypatch, budget_s=7)
    safe(report)
    assert world.calls
    assert report["complete"] is False
    assert "budget_exhausted" in {error["reason"] for error in errors(report)}
    for call in world.calls:
        assert call["started"] < 7
        assert call["timeout"] <= min(5, 7 - call["started"])
    assert len(world.calls) <= 2


def test_exhaustion_during_failed_command_never_retries_or_starts_a_later_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-3."""
    clock = Clock()
    monkeypatch.setattr(time, "monotonic", clock)
    module, world = load(), Kubernetes()
    world.clock, world.consume = clock, 5
    timeout = subprocess.TimeoutExpired(["kubectl"], 1, output=SECRET, stderr=SECRET)
    for operation in (
        "namespace",
        "discovery",
        "api_services",
        "resource:secrets",
        "resource:pods",
    ):
        world.overrides[operation] = timeout
    report = collect(module, world, monkeypatch, budget_s=1)
    safe(report)
    assert report["complete"] is False
    assert len(world.calls) == 1
    assert world.calls[0]["timeout"] <= 1


def test_cli_returns_only_safe_json_for_actual_collector_protocol(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-1 NAMESPACE-CLEANUP-EVIDENCE-2."""
    module, world = load(), Kubernetes()
    monkeypatch.setattr(module, "_run", world.run)
    status = module.main(["--context", CONTEXT, "--namespace", NAMESPACE])
    output = capsys.readouterr()
    assert type(status) is int
    assert SECRET not in output.out + output.err
    report = json.loads(output.out)
    safe(report)
    assert report["complete"] is True


@pytest.mark.parametrize("operation", ["namespace", "api_services", "resource:secrets"])
def test_json_null_is_invalid_evidence_and_never_silent_complete_unknown_state(
    operation: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-2 NAMESPACE-CLEANUP-EVIDENCE-3."""
    module, world = load(), Kubernetes()
    world.overrides[operation] = "null"
    report = collect(module, world, monkeypatch)
    safe(report)
    assert report["complete"] is False, "JSON null silently became complete unknown/empty evidence"
    assert "invalid_projection" in {error["reason"] for error in errors(report)}
    if operation == "namespace":
        assert report["namespaces"][0]["errors"]


@pytest.mark.parametrize("operation", ["namespace", "api_services"])
def test_empty_json_object_cannot_attest_namespace_or_api_service_state(
    operation: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-2 NAMESPACE-CLEANUP-EVIDENCE-3."""
    module, world = load(), Kubernetes()
    world.overrides[operation] = "{}"
    report = collect(module, world, monkeypatch)
    safe(report)
    assert report["complete"] is False
    assert "invalid_projection" in {error["reason"] for error in errors(report)}


def test_unexpected_workload_projection_fields_refuse_without_sensitive_copy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-2: only the three projected fields are valid."""
    module, world = load(), Kubernetes()
    world.projections["secrets"][0]["data"] = {"credential": SECRET}
    report = collect(module, world, monkeypatch)
    safe(report)
    assert report["complete"] is False
    assert "invalid_projection" in {error["reason"] for error in errors(report)}


def test_enormous_integer_budget_uses_fixed_input_refusal_before_commands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-1: overflow must not escape validation."""
    module, world = load(), Kubernetes()
    monkeypatch.setattr(module, "_run", world.run)
    with pytest.raises(ValueError) as caught:
        module.collect_cleanup_evidence(CONTEXT, [NAMESPACE], budget_s=10**1000)
    assert str(caught.value) == "invalid_cleanup_evidence_input"
    assert world.calls == []
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None or caught.value.__suppress_context__


@pytest.mark.parametrize("timestamp", [False, 0, []])
def test_namespace_deletion_timestamp_presence_rejects_nonstring_shapes(
    timestamp: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-2 NAMESPACE-CLEANUP-EVIDENCE-3."""
    module, world = load(), Kubernetes()
    world.namespace["metadata"]["deletionTimestamp"] = timestamp
    report = collect(module, world, monkeypatch)
    safe(report)
    assert report["complete"] is False, "malformed deletion timestamp became complete evidence"
    assert report["namespaces"][0]["errors"]
    assert "invalid_projection" in {error["reason"] for error in errors(report)}
