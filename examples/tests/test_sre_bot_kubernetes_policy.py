"""The SRE bot's vanilla Kubernetes MCP policy and RBAC ceiling (#2169)."""

import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
BUNDLE = ROOT / "examples" / "sre-bot"

READ_TOOLS = {
    "events_list",
    "namespaces_list",
    "projects_list",
    "nodes_log",
    "nodes_stats_summary",
    "nodes_top",
    "pods_list",
    "pods_list_in_namespace",
    "pods_get",
    "pods_top",
    "pods_log",
    "resources_list",
    "resources_get",
}
MUTATING_TOOLS = {
    "pods_delete",
    "pods_exec",
    "pods_run",
    "resources_create_or_update",
    "resources_delete",
    "resources_scale",
}


def _manifest() -> dict:
    return json.loads((BUNDLE / ".claude-plugin" / "plugin.json").read_text())


def _connectors() -> dict:
    return yaml.safe_load((BUNDLE / "connectors.yaml").read_text())["connectors"]


def test_one_pinned_core_only_connector_replaces_bespoke_writers() -> None:
    connectors = _connectors()
    assert "k8s-write" not in connectors
    assert "k8s-scale" not in connectors
    kubernetes = connectors["kubernetes"]
    assert kubernetes["image"] == (
        "ghcr.io/containers/kubernetes-mcp-server@sha256:"
        "6d650f4bd6ac303ad82713c997e73a2d001602f9bf17392c9b9a0e30e29c6423"
    )
    args = kubernetes["args"]
    assert args == [
        "--port",
        "8000",
        "--bind-address",
        "0.0.0.0",
        "--toolsets",
        "core",
        "--disable-multi-cluster",
        "--stateless",
        "--kubeconfig",
        "/secrets/kubeconfig",
    ]
    assert "--read-only" not in args
    assert "--disable-destructive" not in args
    assert kubernetes["secret_files"] == {
        "K8S_KUBECONFIG": "/secrets/kubeconfig"
    }


def test_every_pinned_core_tool_is_classified_exactly_once() -> None:
    policy = _manifest()["toolPolicy"]
    assert policy["enforcement"] == "curie/mcp-tool-policy@1"
    kubernetes_allow = {
        entry.removeprefix("kubernetes/")
        for entry in policy["allow"]
        if entry.startswith("kubernetes/")
    }
    other_allow = {
        entry for entry in policy["allow"] if not entry.startswith("kubernetes/")
    }
    approval = {
        entry.removeprefix("kubernetes/")
        for entry in policy["approvalRequired"]
    }
    assert kubernetes_allow == READ_TOOLS
    surface = json.loads((BUNDLE / "supported-surface.json").read_text())
    expected_other = {
        f"{name}/{tool}"
        for name, spec in surface["connectors"].items()
        if name != "kubernetes"
        for tool in spec["tools"]
    }
    assert other_allow == expected_other
    assert approval == MUTATING_TOOLS
    assert not (kubernetes_allow & approval)
    assert policy["deny"] == []


def test_platform_upgrade_stays_a_separate_legacy_gate() -> None:
    gates = _manifest()["approvalPolicy"]["gates"]
    assert gates == [
        {
            "gate": "mcp__self-upgrade__upgrade_self",
            "route": "sre-approvals",
        },
        {
            "gate": "mcp__self-upgrade__upgrade_platform",
            "route": "sre-approvals",
        },
    ]


def test_kubernetes_rbac_has_one_identity_and_a_demo_namespace_write_ceiling() -> None:
    documents = list(
        yaml.safe_load_all(
            (BUNDLE / "manifests" / "kubernetes-access.yaml").read_text()
        )
    )
    service_accounts = [doc for doc in documents if doc.get("kind") == "ServiceAccount"]
    identities = [
        (doc["metadata"]["namespace"], doc["metadata"]["name"])
        for doc in service_accounts
    ]
    assert identities == [("curie", "sre-bot-kubernetes")]
    roles = [doc for doc in documents if doc.get("kind") == "Role"]
    assert len(roles) == 1
    assert roles[0]["metadata"]["namespace"] == "sre-demo"

    cluster_roles = [doc for doc in documents if doc.get("kind") == "ClusterRole"]
    assert len(cluster_roles) == 1
    assert all(
        set(rule["verbs"]) <= {"get", "list", "watch"}
        for rule in cluster_roles[0]["rules"]
    )

    forbidden = {
        "secrets",
        "serviceaccounts",
        "roles",
        "rolebindings",
        "clusterroles",
        "clusterrolebindings",
        "customresourcedefinitions",
        "mutatingwebhookconfigurations",
        "validatingwebhookconfigurations",
        "namespaces",
        "nodes",
    }
    assert not {
        resource
        for rule in roles[0]["rules"]
        for resource in rule["resources"]
    } & forbidden
    assert all("*" not in rule["resources"] for rule in roles[0]["rules"])
    assert all("*" not in rule["verbs"] for rule in roles[0]["rules"])
    assert not {
        resource
        for rule in cluster_roles[0]["rules"]
        for resource in rule["resources"]
    } & {
        "secrets",
        "serviceaccounts",
        "roles",
        "rolebindings",
        "clusterroles",
        "clusterrolebindings",
        "customresourcedefinitions",
        "mutatingwebhookconfigurations",
        "validatingwebhookconfigurations",
    }

    workload_resources = {
        resource
        for rule in roles[0]["rules"]
        for resource in rule["resources"]
    }
    assert workload_resources == {
        "pods",
        "pods/log",
        "pods/exec",
        "deployments",
        "deployments/scale",
        "statefulsets",
        "statefulsets/scale",
        "daemonsets",
        "replicasets",
        "jobs",
        "cronjobs",
    }

    write_bindings = [
        doc
        for doc in documents
        if doc.get("kind") == "RoleBinding"
    ]
    assert len(write_bindings) == 1
    assert write_bindings[0]["metadata"]["namespace"] == "sre-demo"
    assert write_bindings[0]["subjects"] == [
        {
            "kind": "ServiceAccount",
            "name": "sre-bot-kubernetes",
            "namespace": "curie",
        }
    ]
    assert write_bindings[0]["roleRef"]["name"] == "sre-bot-kubernetes-workloads"
    assert all(
        doc.get("roleRef", {}).get("name")
        not in {"sre-bot-upgrader", "curie-platform-upgrader"}
        for doc in documents
    )
