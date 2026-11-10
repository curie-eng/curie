"""env_create and env_destroy against a fake test cluster API.

Since #3246 env_destroy runs image retention before the namespace delete. The
fake serves the ``e2e-images`` ledger ConfigMap (with
``metadata.resourceVersion``) and build Pods (``status.phase``) as in the
Kubernetes API reference (https://kubernetes.io/docs/reference/kubernetes-api/).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from curie_e2e_connector.contract import BUILD_LABEL, IMAGES_CONFIGMAP, IMAGES_CONFIGMAP_KEY
from curie_e2e_connector.kube import ClusterApi, ClusterError
from curie_e2e_connector.namespace import (
    Install,
    create_environment,
    destroy_environment,
    require_caller,
)
from curie_e2e_connector.registry import RegistryApi, RegistryError, RegistrySettings

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
        # Ledger JSON per namespace; absent means the ConfigMap does not exist.
        self.ledgers: dict[str, str] = {}
        self.versions: dict[str, int] = {}
        self.pods: dict[str, list[dict[str, Any]]] = {}

    def _configmap(self, ns: str) -> dict[str, Any]:
        return {
            "kind": "ConfigMap",
            "metadata": {
                "name": IMAGES_CONFIGMAP,
                "namespace": ns,
                "resourceVersion": str(self.versions.get(ns, 1)),
            },
            "data": {IMAGES_CONFIGMAP_KEY: self.ledgers[ns]},
        }

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> tuple[int, dict[str, Any]]:
        self.calls.append((method, path, body))
        parts = path.split("?")[0].split("/")
        if method == "GET" and path.startswith("/api/v1/namespaces/") and path.count("/") == 4:
            name = path.rsplit("/", 1)[-1]
            found = self.namespaces.get(name)
            if found is None:
                return 404, {}
            return 200, found
        if len(parts) == 7 and parts[5] == "configmaps" and parts[6] == IMAGES_CONFIGMAP:
            ns = parts[4]
            if method == "GET":
                if ns not in self.ledgers:
                    return 404, {"kind": "Status", "reason": "NotFound", "code": 404}
                return 200, self._configmap(ns)
            if method == "PUT":
                assert body is not None
                if body["metadata"].get("resourceVersion") != str(self.versions.get(ns, 1)):
                    return 409, {"kind": "Status", "reason": "Conflict", "code": 409}
                self.ledgers[ns] = body["data"][IMAGES_CONFIGMAP_KEY]
                self.versions[ns] = self.versions.get(ns, 1) + 1
                return 200, self._configmap(ns)
        if method == "POST" and len(parts) == 6 and parts[5] == "configmaps":
            assert body is not None
            ns = parts[4]
            if ns in self.ledgers:
                return 409, {"kind": "Status", "reason": "AlreadyExists", "code": 409}
            self.ledgers[ns] = body["data"][IMAGES_CONFIGMAP_KEY]
            return 201, self._configmap(ns)
        if method == "GET" and len(parts) == 6 and parts[5] == "pods":
            return 200, {"kind": "PodList", "items": self.pods.get(parts[4], [])}
        if method == "POST" and path == "/api/v1/namespaces":
            assert body is not None
            name = str(body["metadata"]["name"])
            self.namespaces[name] = body
            return 201, body
        if method == "DELETE" and path.startswith("/apis/batch/v1/namespaces/"):
            return 200, {"kind": "Status", "status": "Success"}
        if method == "DELETE" and path.startswith("/api/v1/namespaces/"):
            name = path.rsplit("/", 1)[-1]
            self.namespaces.pop(name, None)
            return 200, {}
        if method == "POST":
            return 201, body or {}
        return 500, {}


HOST = "registry.test.example"
REGISTRY_PREFIX = f"{HOST}/e2e"
DIGEST = "sha256:" + "a1" * 32


class FakeRegistry(RegistryApi):
    """Logs into the cluster's call list so order against the namespace delete shows."""

    def __init__(
        self,
        log: list[Any],
        tags: dict[str, dict[str, str]] | None = None,
        *,
        fail: bool = False,
    ) -> None:
        self.settings = RegistrySettings(prefix=REGISTRY_PREFIX, insecure=False, token_hosts=())
        self.log = log
        self.tags = tags or {}
        self.fail = fail
        self.deleted: list[tuple[str, str]] = []

    def list_tags(self, repo: str) -> list[str]:
        self.log.append(("REGISTRY", "list_tags", repo))
        if self.fail:
            raise RegistryError(f"{HOST}: the registry is unreachable")
        return sorted(self.tags.get(repo, {}))

    def resolve(self, repo: str, tag: str) -> str | None:
        self.log.append(("REGISTRY", "resolve", repo))
        return self.tags.get(repo, {}).get(tag)

    def delete_manifest(self, repo: str, digest: str) -> None:
        self.log.append(("REGISTRY", "delete_manifest", repo))
        self.deleted.append((repo, digest))


class FrozenClock:
    """A clock that moves only when the code under test sleeps."""

    def __init__(self, *, advance: bool = True) -> None:
        self.moment = NOW
        self.advance = advance
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self.moment

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        if self.advance:
            self.moment += timedelta(seconds=seconds)


def destroy(
    cluster: FakeCluster,
    namespace: str,
    *,
    caller: Any = None,
    registry: FakeRegistry | None = None,
    clock: FrozenClock | None = None,
) -> dict[str, Any]:
    tick = clock or FrozenClock()
    return destroy_environment(
        cluster,
        install(),
        caller or _caller(),
        namespace,
        registry=registry or FakeRegistry(cluster.calls),
        now=tick.now,
        sleep=tick.sleep,
    )


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
        call[2] for call in cluster.calls if call[2] and call[2].get("kind") == "NetworkPolicy"
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
        call[2] for call in cluster.calls if call[2] and call[2].get("kind") == "NetworkPolicy"
    ]
    assert [item["metadata"]["name"] for item in policies] == ["default-deny"]


def test_env_destroy_deletes_only_this_runs_namespace() -> None:
    cluster = FakeCluster()
    created = create_environment(cluster, install(), _caller(), None, None, now=NOW)
    cluster.calls.clear()
    result = destroy(cluster, created["namespace"])
    assert result == {"namespace": created["namespace"], "deleted": True}
    assert ("DELETE", f"/api/v1/namespaces/{created['namespace']}", None) in cluster.calls


def test_env_destroy_refuses_another_run_without_deleting() -> None:
    cluster = FakeCluster()
    created = create_environment(cluster, install(), _caller(), None, None, now=NOW)
    cluster.calls.clear()
    other = require_caller(OTHER, WORK)
    with pytest.raises(ClusterError, match="e2e_namespace_not_owned"):
        destroy(cluster, created["namespace"], caller=other)
    assert created["namespace"] in cluster.namespaces
    assert not any(call[0] == "DELETE" for call in cluster.calls)


def test_env_destroy_refuses_an_unprefixed_name_without_a_request() -> None:
    cluster = FakeCluster()
    with pytest.raises(ClusterError, match="e2e_namespace_not_owned"):
        destroy(cluster, "kube-system")
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


# env_destroy runs image retention first (#3246)


def _namespace_delete(cluster: FakeCluster, name: str) -> tuple[str, str, None]:
    return ("DELETE", f"/api/v1/namespaces/{name}", None)


def _created(cluster: FakeCluster) -> str:
    created = create_environment(cluster, install(), _caller(), None, None, now=NOW)
    cluster.calls.clear()
    return str(created["namespace"])


def _settled(cluster: FakeCluster, name: str, repositories: list[str]) -> None:
    cluster.ledgers[name] = json.dumps(
        {"repositories": repositories, "closing_at": "2026-10-02T11:00:00Z"}
    )


def test_env_destroy_deletes_images_before_the_namespace() -> None:
    cluster = FakeCluster()
    name = _created(cluster)
    repo = f"{REGISTRY_PREFIX}/{name}/app"
    _settled(cluster, name, [repo])
    registry = FakeRegistry(cluster.calls, {repo: {"build-0a1b2c3d": DIGEST}})

    result = destroy(cluster, name, registry=registry)

    assert result == {"namespace": name, "deleted": True}
    assert registry.deleted == [(repo, DIGEST)]
    delete_at = cluster.calls.index(_namespace_delete(cluster, name))
    registry_at = [i for i, call in enumerate(cluster.calls) if call[0] == "REGISTRY"]
    assert registry_at and max(registry_at) < delete_at


def test_env_destroy_closes_an_open_ledger_sleeps_the_settle_then_deletes() -> None:
    cluster = FakeCluster()
    name = _created(cluster)
    repo = f"{REGISTRY_PREFIX}/{name}/app"
    cluster.ledgers[name] = json.dumps({"repositories": [repo]})
    registry = FakeRegistry(cluster.calls, {repo: {"build-0a1b2c3d": DIGEST}})
    clock = FrozenClock()

    result = destroy(cluster, name, registry=registry, clock=clock)

    assert result == {"namespace": name, "deleted": True}
    assert clock.sleeps == [pytest.approx(10)]
    ledger = json.loads(cluster.ledgers[name])
    assert datetime.fromisoformat(ledger["closing_at"]) == NOW
    puts = [call for call in cluster.calls if call[0] == "PUT"]
    assert len(puts) == 1
    assert registry.deleted == [(repo, DIGEST)]
    assert _namespace_delete(cluster, name) in cluster.calls


def test_env_destroy_without_a_ledger_creates_it_closed_then_deletes() -> None:
    cluster = FakeCluster()
    name = _created(cluster)
    clock = FrozenClock()

    destroy(cluster, name, clock=clock)

    assert clock.sleeps == [pytest.approx(10)]
    assert json.loads(cluster.ledgers[name])["repositories"] == []
    assert _namespace_delete(cluster, name) in cluster.calls


def test_env_destroy_still_settling_after_the_sleep_is_refused() -> None:
    cluster = FakeCluster()
    name = _created(cluster)
    cluster.ledgers[name] = json.dumps({"repositories": []})
    clock = FrozenClock(advance=False)

    with pytest.raises(ClusterError, match="e2e_build_in_progress"):
        destroy(cluster, name, clock=clock)

    assert len(clock.sleeps) == 1
    assert _namespace_delete(cluster, name) not in cluster.calls


def test_env_destroy_with_a_running_build_is_refused_without_deleting() -> None:
    cluster = FakeCluster()
    name = _created(cluster)
    _settled(cluster, name, [])
    cluster.pods[name] = [
        {
            "metadata": {
                "name": "e2e-build-0a1b2c3d-x7k2p",
                "labels": {BUILD_LABEL: "e2e-build-0a1b2c3d"},
            },
            "status": {"phase": "Running"},
        }
    ]

    with pytest.raises(ClusterError, match="e2e_build_in_progress"):
        destroy(cluster, name)

    assert _namespace_delete(cluster, name) not in cluster.calls
    assert name in cluster.namespaces


def test_env_destroy_with_a_registry_failure_keeps_the_namespace() -> None:
    cluster = FakeCluster()
    name = _created(cluster)
    repo = f"{REGISTRY_PREFIX}/{name}/app"
    _settled(cluster, name, [repo])

    with pytest.raises(ClusterError):
        destroy(cluster, name, registry=FakeRegistry(cluster.calls, fail=True))

    assert _namespace_delete(cluster, name) not in cluster.calls
    assert name in cluster.namespaces


def test_env_destroy_refusal_sends_no_retention_call() -> None:
    cluster = FakeCluster()
    name = _created(cluster)
    other = require_caller(OTHER, WORK)
    with pytest.raises(ClusterError, match="e2e_namespace_not_owned"):
        destroy(cluster, name, caller=other)
    assert not any(call[0] in ("PUT", "POST", "DELETE", "REGISTRY") for call in cluster.calls)
