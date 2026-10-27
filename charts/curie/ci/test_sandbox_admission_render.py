"""Pass the rendered sandbox admission contract to the worker's real parser."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml
from curie_worker.run import _substrate_config

CHART = Path(__file__).resolve().parents[1]
ENV = "CURIE_SANDBOX_ADMISSION"
HARD = {
    "sandboxPodCount": "10",
    "requestsCpu": "10",
    "requestsMemory": "10Gi",
    "limitsCpu": "20",
    "limitsMemory": "20Gi",
}


def render(
    tmp_path: Path,
    *,
    expected_error: str | None = None,
    **overrides: Any,
) -> list[dict[str, Any]]:
    values = {
        "fullnameOverride": "acme-admission",
        "priorityClasses": {"sandbox": {"name": "acme-sandbox"}},
        "worker": {"connectorReconciler": {"enabled": False}},
        "security": {},
        "agentSandbox": {
            "runner": {
                "resources": {
                    "requests": {"cpu": "1", "memory": "1Gi"},
                    "limits": {"cpu": "2", "memory": "2Gi"},
                }
            }
        },
        "resourceQuota": {"hard": dict(HARD), "capacityReport": False},
    }
    for key, value in overrides.items():
        values[key].update(value)
    completed = subprocess.run(
        [
            "helm",
            "template",
            "acme",
            str(CHART),
            "--namespace",
            "acme-admission",
            "--kube-version",
            "1.31.0",
            "--api-versions",
            "admissionregistration.k8s.io/v1/ValidatingAdmissionPolicy",
            "--output-dir",
            str(tmp_path),
            "-f",
            "-",
        ],
        input=yaml.safe_dump(values),
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    if expected_error is not None:
        assert completed.returncode != 0, "chart-owned extraEnv must be refused"
        assert expected_error in completed.stderr
        assert "worker.extraEnv" in completed.stderr
        return []
    assert completed.returncode == 0, completed.stderr
    return [
        doc
        for path in sorted(tmp_path.rglob("*.yaml"))
        for doc in yaml.safe_load_all(path.read_text())
        if isinstance(doc, dict)
    ]


def worker_env(docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    worker = next(
        doc
        for doc in docs
        if doc.get("kind") == "Deployment"
        and doc["metadata"]["labels"].get("app.kubernetes.io/component") == "worker"
    )
    container = next(
        item
        for item in worker["spec"]["template"]["spec"]["containers"]
        if item["name"] == "worker"
    )
    return container["env"]


def role_permissions(roles: list[dict[str, Any]]) -> dict[tuple[str, str], set[str]]:
    permissions: dict[tuple[str, str], set[str]] = {}
    for role in roles:
        for rule in role["rules"]:
            for group in rule["apiGroups"]:
                for resource in rule["resources"]:
                    permissions.setdefault((group, resource), set()).update(rule["verbs"])
    return permissions


def bound_worker_roles(docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    worker = next(
        doc
        for doc in docs
        if doc.get("kind") == "Deployment"
        and doc["metadata"]["labels"].get("app.kubernetes.io/component") == "worker"
    )
    account = worker["spec"]["template"]["spec"]["serviceAccountName"]
    namespace = "acme-admission"
    roles: list[dict[str, Any]] = []
    for binding in docs:
        if (
            binding.get("kind") != "RoleBinding"
            or binding["metadata"].get("namespace", namespace) != namespace
            or not any(
                subject.get("kind") == "ServiceAccount"
                and subject.get("name") == account
                and subject.get("namespace") == namespace
                for subject in binding.get("subjects", [])
            )
        ):
            continue
        reference = binding["roleRef"]
        assert reference["apiGroup"] == "rbac.authorization.k8s.io"
        assert reference["kind"] in {"Role", "ClusterRole"}
        referenced = [
            doc
            for doc in docs
            if doc.get("kind") == reference["kind"]
            and doc["metadata"]["name"] == reference["name"]
            and (
                reference["kind"] == "ClusterRole"
                or doc["metadata"].get("namespace", namespace) == namespace
            )
        ]
        assert len(referenced) == 1, "worker RoleBinding must resolve to one rendered role"
        roles.extend(referenced)
    return roles


@pytest.mark.parametrize(
    ("hard", "ceiling"),
    [
        ({"sandboxPodCount": "2"}, 2),
        ({"requestsCpu": "2500m"}, 2),
        ({"limitsCpu": "5"}, 2),
        ({"requestsMemory": "2500Mi"}, 2),
        ({"limitsMemory": "5Gi"}, 2),
        ({"sandboxPodCount": "0"}, 0),
    ],
    ids=["pods-two", "request-cpu", "limit-cpu", "request-memory", "limit-memory", "zero"],
)
def test_rendered_admission_reaches_worker_parser(
    tmp_path: Path,
    hard: dict[str, str],
    ceiling: int,
) -> None:
    docs = render(tmp_path, resourceQuota={"hard": HARD | hard})
    entries = [entry for entry in worker_env(docs) if entry["name"] == ENV]
    assert len(entries) == 1, "enabled quota must render one worker admission contract"
    quota = next(doc for doc in docs if doc.get("kind") == "ResourceQuota")
    contract = json.loads(entries[0]["value"])
    assert contract == {
        "quota_name": quota["metadata"]["name"],
        "priority_class": "acme-sandbox",
        "ceiling": ceiling,
        "hard": quota["spec"]["hard"],
    }
    assert quota["spec"]["scopeSelector"]["matchExpressions"][0]["values"] == [
        contract["priority_class"]
    ]
    config = _substrate_config(
        {entry["name"]: entry["value"] for entry in worker_env(docs) if "value" in entry}
    )
    assert config.namespace == "acme-admission"
    assert config.admission is not None
    assert config.admission.quota_name == contract["quota_name"]
    assert config.admission.priority_class == contract["priority_class"]
    assert config.admission.ceiling == ceiling
    assert dict(config.admission.hard) == contract["hard"]


def test_disabled_quota_omits_admission_contract(tmp_path: Path) -> None:
    docs = render(tmp_path, resourceQuota={"enabled": False})
    environment = {entry["name"]: entry["value"] for entry in worker_env(docs) if "value" in entry}
    assert ENV not in environment
    assert not any(doc.get("kind") == "ResourceQuota" for doc in docs)
    assert _substrate_config(environment).admission is None


def test_extra_env_cannot_replace_or_duplicate_admission(tmp_path: Path) -> None:
    neighbor = {"name": "ACME_ADMISSION_NEIGHBOR", "value": "retained"}
    docs = render(tmp_path / "valid", worker={"extraEnv": [neighbor]})
    entries = worker_env(docs)
    admission = [entry for entry in entries if entry["name"] == ENV]
    assert len(admission) == 1
    assert json.loads(admission[0]["value"])["quota_name"] == "acme-admission-sandbox-quota"
    assert neighbor in entries
    render(
        tmp_path / "collision",
        expected_error=ENV,
        worker={
            "extraEnv": [
                {"name": ENV, "value": admission[0]["value"]},
                {"name": "ACME_ADMISSION_NEIGHBOR", "value": "retained"},
            ]
        },
    )


def test_worker_admission_adds_only_namespaced_inventory_reads(tmp_path: Path) -> None:
    docs = render(tmp_path, security={"gvisor": {"mode": "off"}})
    baseline_role = next(
        doc
        for doc in docs
        if doc.get("kind") == "Role" and doc["metadata"]["name"] == "acme-admission-worker"
    )
    baseline_permissions = {
        ("extensions.agents.x-k8s.io", "sandboxclaims"): {"create", "get", "list", "delete"},
        ("agents.x-k8s.io", "sandboxes"): {"get", "patch"},
        ("", "resourcequotas"): {"get"},
        ("extensions.agents.x-k8s.io", "sandboxtemplates"): {
            "get",
            "create",
            "patch",
            "list",
            "delete",
        },
        ("extensions.agents.x-k8s.io", "sandboxwarmpools"): {"get", "create", "patch"},
        ("", "secrets"): {"create"},
        ("", "pods"): {"get"},
        ("", "pods/log"): {"get"},
        ("", "events"): {"list"},
    }
    assert role_permissions([baseline_role]) == baseline_permissions
    inventory_name = "acme-admission-worker-admission"
    inventory_roles = [
        doc
        for doc in docs
        if doc.get("kind") == "Role" and doc["metadata"]["name"] == inventory_name
    ]
    assert len(inventory_roles) == 1
    inventory_role = inventory_roles[0]
    assert inventory_role["metadata"].get("namespace", "acme-admission") == "acme-admission"
    assert inventory_role["metadata"]["labels"]["app.kubernetes.io/component"] == (
        "sandbox-admission"
    )
    assert inventory_role["rules"] == [
        {"apiGroups": [""], "resources": ["pods"], "verbs": ["list"]},
        {"apiGroups": ["agents.x-k8s.io"], "resources": ["sandboxes"], "verbs": ["list"]},
    ]
    inventory_bindings = [
        doc
        for doc in docs
        if doc.get("kind") == "RoleBinding" and doc["metadata"]["name"] == inventory_name
    ]
    assert len(inventory_bindings) == 1
    inventory_binding = inventory_bindings[0]
    assert inventory_binding["metadata"].get("namespace", "acme-admission") == "acme-admission"
    assert inventory_binding["metadata"]["labels"]["app.kubernetes.io/component"] == (
        "sandbox-admission"
    )
    assert inventory_binding["roleRef"] == {
        "apiGroup": "rbac.authorization.k8s.io",
        "kind": "Role",
        "name": inventory_name,
    }
    assert inventory_binding["subjects"] == [
        {
            "kind": "ServiceAccount",
            "name": "acme-admission-worker",
            "namespace": "acme-admission",
        }
    ]
    assert role_permissions(bound_worker_roles(docs)) == baseline_permissions | {
        ("", "pods"): {"get", "list"},
        ("agents.x-k8s.io", "sandboxes"): {"get", "patch", "list"},
    }
    assert not any(
        doc.get("kind") == "ClusterRoleBinding"
        and any(
            subject.get("name") == "acme-admission-worker" for subject in doc.get("subjects", [])
        )
        for doc in docs
    )


@pytest.mark.parametrize(
    "overrides",
    [
        {"resourceQuota": {"enabled": False}},
        {"agentSandbox": {"deploy": False}},
    ],
    ids=["quota-disabled", "substrate-disabled"],
)
def test_disabled_admission_omits_inventory_grants(
    tmp_path: Path, overrides: dict[str, dict[str, bool]]
) -> None:
    docs = render(tmp_path, security={"gvisor": {"mode": "off"}}, **overrides)
    assert not any(entry["name"] == ENV for entry in worker_env(docs))
    environment = {entry["name"]: entry["value"] for entry in worker_env(docs) if "value" in entry}
    assert _substrate_config(environment).admission is None
    assert not any(
        doc.get("kind") in {"Role", "RoleBinding", "ClusterRole", "ClusterRoleBinding"}
        and (
            doc["metadata"]["name"] == "acme-admission-worker-admission"
            or doc["metadata"].get("labels", {}).get("app.kubernetes.io/component")
            == "sandbox-admission"
        )
        for doc in docs
    )
    baseline_role = next(
        doc
        for doc in docs
        if doc.get("kind") == "Role" and doc["metadata"]["name"] == "acme-admission-worker"
    )
    for permissions in [
        role_permissions([baseline_role]),
        role_permissions(bound_worker_roles(docs)),
    ]:
        assert permissions[("", "pods")] == {"get"}
        assert permissions[("agents.x-k8s.io", "sandboxes")] == {"get", "patch"}
    assert not any(
        doc.get("kind") == "ClusterRoleBinding"
        and any(
            subject.get("name") == "acme-admission-worker" for subject in doc.get("subjects", [])
        )
        for doc in docs
    )


def test_worker_reads_only_the_selected_runtime_class(tmp_path: Path) -> None:
    docs = render(
        tmp_path,
        security={
            "gvisor": {
                "mode": "require",
                "runtimeClassName": "acme-runtime",
            }
        },
    )
    worker = next(
        doc
        for doc in docs
        if doc.get("kind") == "Deployment"
        and doc["metadata"]["labels"].get("app.kubernetes.io/component") == "worker"
    )
    account = worker["spec"]["template"]["spec"]["serviceAccountName"]
    bindings = [
        doc
        for doc in docs
        if doc.get("kind") == "ClusterRoleBinding"
        and any(
            subject.get("kind") == "ServiceAccount" and subject.get("name") == account
            for subject in doc.get("subjects", [])
        )
    ]
    assert len(bindings) == 1, "worker needs one narrowly scoped RuntimeClass grant"
    assert bindings[0]["subjects"] == [
        {
            "kind": "ServiceAccount",
            "name": account,
            "namespace": "acme-admission",
        }
    ]
    reference = bindings[0]["roleRef"]
    assert reference["kind"] == "ClusterRole"
    assert reference["apiGroup"] == "rbac.authorization.k8s.io"
    role = next(
        doc
        for doc in docs
        if doc.get("kind") == "ClusterRole" and doc["metadata"]["name"] == reference["name"]
    )
    assert role["rules"] == [
        {
            "apiGroups": ["node.k8s.io"],
            "resources": ["runtimeclasses"],
            "resourceNames": ["acme-runtime"],
            "verbs": ["get"],
        }
    ]
