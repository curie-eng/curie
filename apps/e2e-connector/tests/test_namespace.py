"""env_create and env_destroy against a fake test cluster API."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from curie_e2e_connector.kube import ClusterApi, ClusterError
from curie_e2e_connector.namespace import (
    Install,
    create_environment,
    destroy_environment,
    require_caller,
)

RUN = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
OTHER = "bbbbbbbb-bbbb-4ccc-8ddd-eeeeeeeeeeee"
WORK = "11111111-2222-4333-8444-555555555555"
NOW = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)


def install() -> Install:
    return Install(
        namespace_prefix="curie-e2e-",
        owner_label_key="curietech.ai/e2e-owner",
        owner_label_value="acme",
        service_account="curie-e2e-connector",
        service_account_namespace="test-system",
        worker_cluster_role="curie-e2e-connector-namespace",
        ttl_seconds=3600,
        pod_security="baseline",
    )


class FakeCluster(ClusterApi):
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, Any] | None]] = []
        self.namespaces: dict[str, dict[str, Any]] = {}

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> tuple[int, dict[str, Any]]:
        self.calls.append((method, path, body))
        if method == "GET" and path.startswith("/api/v1/namespaces/") and path.count("/") == 4:
            name = path.rsplit("/", 1)[-1]
            found = self.namespaces.get(name)
            if found is None:
                return 404, {}
            return 200, found
        if method == "POST" and path == "/api/v1/namespaces":
            assert body is not None
            name = str(body["metadata"]["name"])
            self.namespaces[name] = body
            return 201, body
        if method == "DELETE" and path.startswith("/api/v1/namespaces/"):
            name = path.rsplit("/", 1)[-1]
            self.namespaces.pop(name, None)
            return 200, {}
        if method == "POST":
            return 201, body or {}
        return 500, {}


def _caller():
    return require_caller(RUN, WORK)


def test_env_create_builds_a_bounded_namespace_for_this_run() -> None:
    cluster = FakeCluster()
    created = create_environment(
        cluster, install(), _caller(), [{"cidr": "10.1.0.0/16", "port": 443}], None, now=NOW
    )

    assert created["namespace"] == f"curie-e2e-{RUN}"
    assert created["run"] == RUN
    assert created["work_item"] == WORK
    assert created["expires_at"] == "2026-10-02T13:00:00Z"
    body = cluster.namespaces[created["namespace"]]
    labels = body["metadata"]["labels"]
    assert labels["curietech.ai/e2e-owner"] == "acme"
    assert labels["curietech.ai/e2e-run"] == RUN
    assert labels["curietech.ai/e2e-work-item"] == WORK
    assert labels["pod-security.kubernetes.io/enforce"] == "baseline"
    kinds = [call[2]["kind"] for call in cluster.calls if call[0] == "POST" and call[2]]
    assert kinds == [
        "Namespace",
        "RoleBinding",
        "ResourceQuota",
        "LimitRange",
        "NetworkPolicy",
        "NetworkPolicy",
    ]
    policies = [
        call[2]
        for call in cluster.calls
        if call[2] and call[2].get("kind") == "NetworkPolicy"
    ]
    assert policies[0]["metadata"]["name"] == "default-deny"
    assert policies[0]["spec"]["policyTypes"] == ["Ingress", "Egress"]
    assert "egress" not in policies[0]["spec"]
    assert policies[1]["spec"]["egress"][0]["to"] == [{"ipBlock": {"cidr": "10.1.0.0/16"}}]
    binding = next(
        call[2] for call in cluster.calls if call[2] and call[2].get("kind") == "RoleBinding"
    )
    assert binding is not None
    assert binding["subjects"] == [
        {
            "kind": "ServiceAccount",
            "name": "curie-e2e-connector",
            "namespace": "test-system",
        }
    ]


def test_env_create_without_allow_rules_adds_no_permit() -> None:
    cluster = FakeCluster()
    create_environment(cluster, install(), _caller(), None, None, now=NOW)
    policies = [
        call[2]
        for call in cluster.calls
        if call[2] and call[2].get("kind") == "NetworkPolicy"
    ]
    assert [item["metadata"]["name"] for item in policies] == ["default-deny"]


def test_env_destroy_deletes_only_this_runs_namespace() -> None:
    cluster = FakeCluster()
    created = create_environment(cluster, install(), _caller(), None, None, now=NOW)
    cluster.calls.clear()
    result = destroy_environment(cluster, install(), _caller(), created["namespace"])
    assert result == {"namespace": created["namespace"], "deleted": True}
    assert ("DELETE", f"/api/v1/namespaces/{created['namespace']}", None) in cluster.calls


def test_env_destroy_refuses_another_run_without_deleting() -> None:
    cluster = FakeCluster()
    created = create_environment(cluster, install(), _caller(), None, None, now=NOW)
    cluster.calls.clear()
    other = require_caller(OTHER, WORK)
    with pytest.raises(ClusterError, match="e2e_namespace_not_owned"):
        destroy_environment(cluster, install(), other, created["namespace"])
    assert created["namespace"] in cluster.namespaces
    assert not any(call[0] == "DELETE" for call in cluster.calls)


def test_env_destroy_refuses_an_unprefixed_name_without_a_request() -> None:
    cluster = FakeCluster()
    with pytest.raises(ClusterError, match="e2e_namespace_not_owned"):
        destroy_environment(cluster, install(), _caller(), "kube-system")
    assert cluster.calls == []


def test_a_caller_without_a_run_is_refused_before_the_cluster() -> None:
    with pytest.raises(ClusterError, match="e2e_run_identity_required"):
        require_caller("", WORK)


def test_a_kubeconfig_token_never_appears_in_a_refusal(tmp_path) -> None:
    from curie_e2e_connector.kube import load_kubeconfig

    path = tmp_path / "kubeconfig"
    path.write_text(
        "clusters:\n- cluster: {server: http://example.test}\n  name: t\n"
        "users:\n- user: {token: super-secret-token}\n  name: u\n",
        encoding="utf-8",
    )
    with pytest.raises(ClusterError, match="https") as excinfo:
        load_kubeconfig(str(path))
    assert "super-secret-token" not in str(excinfo.value)
