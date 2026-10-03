"""The platform reaper sweep against a fake test cluster API (#3245, ADR 0176 decision 4).

Since #3246 every reap first runs image retention: close admission on the
``e2e-images`` ledger ConfigMap, stop build Jobs, wait for build pods, then
delete every ledger repository's images. The fake serves the ledger as a
ConfigMap with ``metadata.resourceVersion`` and Pods with ``status.phase``,
per the Kubernetes API reference
(https://kubernetes.io/docs/reference/kubernetes-api/).
"""

from __future__ import annotations

import json
import urllib.parse
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from curie_e2e_connector.contract import (
    BUILD_LABEL,
    EXPIRES_ANNOTATION,
    IMAGES_CONFIGMAP,
    IMAGES_CONFIGMAP_KEY,
    OWNER_LABEL,
    RUN_LABEL,
)
from curie_e2e_connector.kube import (
    ClusterApi,
    ClusterError,
    client_from_kubeconfig_text,
    load_kubeconfig_text,
)
from curie_e2e_connector.reaper import (
    OVERDUE_GRACE_S,
    Scope,
    ScopedNamespace,
    SweepResult,
    SweepTarget,
    scoped_namespaces,
    sweep,
)
from curie_e2e_connector.registry import RegistryApi, RegistryError, RegistrySettings

NOW = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)
PAST = "2026-10-02T11:00:00Z"
FUTURE = "2026-10-02T13:00:00Z"
PREFIX = "curie-e2e-"
OWNER = "acme"

RUN_LIVE = "aaaaaaaa-bbbb-4ccc-8ddd-000000000001"
RUN_DONE = "aaaaaaaa-bbbb-4ccc-8ddd-000000000002"
RUN_OLD = "aaaaaaaa-bbbb-4ccc-8ddd-000000000003"
RUN_NOANN = "aaaaaaaa-bbbb-4ccc-8ddd-000000000004"
RUN_BAD = "aaaaaaaa-bbbb-4ccc-8ddd-000000000005"
RUN_TERM = "aaaaaaaa-bbbb-4ccc-8ddd-000000000006"
RUN_FOREIGN = "aaaaaaaa-bbbb-4ccc-8ddd-000000000007"

LIST_PATH = "/api/v1/namespaces?labelSelector=" + urllib.parse.quote(
    f"{OWNER_LABEL}={OWNER}", safe=""
)


def scope() -> Scope:
    return Scope(namespace_prefix=PREFIX, owner_label_key=OWNER_LABEL, owner_label_value=OWNER)


def namespace_item(
    name: str,
    *,
    labels: dict[str, str] | None = None,
    expires: str | None = FUTURE,
    phase: str = "Active",
    annotations_key: bool = True,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {"name": name, "labels": dict(labels or {})}
    if annotations_key:
        metadata["annotations"] = {} if expires is None else {EXPIRES_ANNOTATION: expires}
    return {"metadata": metadata, "status": {"phase": phase}}


def owned(run: str) -> dict[str, str]:
    return {OWNER_LABEL: OWNER, RUN_LABEL: run}


def child_paths(ns: str) -> list[str]:
    return [
        f"/apis/extensions.agents.x-k8s.io/v1beta1/namespaces/{ns}/sandboxclaims",
        f"/apis/batch/v1/namespaces/{ns}/jobs?propagationPolicy=Background",
        f"/api/v1/namespaces/{ns}/persistentvolumeclaims",
    ]


def ledger_path(ns: str) -> str:
    return f"/api/v1/namespaces/{ns}/configmaps/{IMAGES_CONFIGMAP}"


_SELECTOR = urllib.parse.quote(BUILD_LABEL, safe="")


def build_jobs_path(ns: str) -> str:
    query = f"labelSelector={_SELECTOR}&propagationPolicy=Background"
    return f"/apis/batch/v1/namespaces/{ns}/jobs?{query}"


def build_pods_path(ns: str) -> str:
    return f"/api/v1/namespaces/{ns}/pods?labelSelector={_SELECTOR}"


def retention_calls(ns: str) -> list[tuple[str, str]]:
    """A closed, settled ledger: read it, stop builds, check pods, read it again."""

    return [
        ("GET", ledger_path(ns)),
        ("DELETE", build_jobs_path(ns)),
        ("GET", build_pods_path(ns)),
        ("GET", ledger_path(ns)),
    ]


def reap_calls(ns: str) -> list[tuple[str, str]]:
    return [
        *retention_calls(ns),
        *[("DELETE", path) for path in [*child_paths(ns), f"/api/v1/namespaces/{ns}"]],
    ]


HOST = "registry.test.example"
REGISTRY_PREFIX = f"{HOST}/e2e"
# Closed well before NOW and holding no images: the steady state of a namespace
# whose teardown already settled.
SETTLED_LEDGER = json.dumps({"repositories": [], "closing_at": "2026-10-02T11:00:00Z"})


def repo(ns: str, name: str = "app") -> str:
    return f"{REGISTRY_PREFIX}/{ns}/{name}"


class FakeRegistry(RegistryApi):
    """A registry at the ``RegistryApi`` seam: ``repo -> {tag: digest}``.

    It logs into a shared call list so tests can order registry calls against
    the cluster's child and namespace deletes.
    """

    def __init__(
        self,
        log: list[Any] | None = None,
        tags: dict[str, dict[str, str]] | None = None,
        *,
        fail_repos: set[str] | None = None,
    ) -> None:
        self.settings = RegistrySettings(prefix=REGISTRY_PREFIX, insecure=False, token_hosts=())
        self.log = log if log is not None else []
        self.tags = tags or {}
        self.fail_repos = fail_repos or set()
        self.deleted: list[tuple[str, str]] = []

    def _check(self, repository: str) -> None:
        if repository in self.fail_repos:
            raise RegistryError(f"{HOST}: the registry is unreachable")

    def list_tags(self, repo: str) -> list[str]:
        self.log.append(("REGISTRY", "list_tags", repo))
        self._check(repo)
        return sorted(self.tags.get(repo, {}))

    def resolve(self, repo: str, tag: str) -> str | None:
        self.log.append(("REGISTRY", "resolve", repo))
        self._check(repo)
        return self.tags.get(repo, {}).get(tag)

    def delete_manifest(self, repo: str, digest: str) -> None:
        self.log.append(("REGISTRY", "delete_manifest", repo))
        self._check(repo)
        self.deleted.append((repo, digest))


class FakeCluster(ClusterApi):
    """Serves every namespace on any list query, like a server that ignored the selector.

    The real API server applies the label selector. This fake deliberately does
    not, so the client side re-check is what keeps foreign namespaces out.

    Each namespace's ledger defaults to a settled, empty one. ``ledgers`` maps a
    namespace to ledger JSON, or to None for a missing ConfigMap.
    """

    def __init__(self, items: list[dict[str, Any]]) -> None:
        self.items = items
        self.calls: list[Any] = []
        self.status: dict[tuple[str, str], int] = {}
        self.raises: set[tuple[str, str]] = set()
        self.list_status = 200
        self.ledgers: dict[str, str | None] = {}
        self.versions: dict[str, int] = {}
        self.pods: dict[str, list[dict[str, Any]]] = {}

    def _ledger_ns(self, path: str) -> str | None:
        parts = path.split("/")
        if len(parts) == 7 and parts[5] == "configmaps" and parts[6] == IMAGES_CONFIGMAP:
            return parts[4]
        return None

    def _configmap(self, ns: str) -> dict[str, Any]:
        return {
            "kind": "ConfigMap",
            "metadata": {
                "name": IMAGES_CONFIGMAP,
                "namespace": ns,
                "resourceVersion": str(self.versions.get(ns, 1)),
            },
            "data": {IMAGES_CONFIGMAP_KEY: self.ledgers.get(ns, SETTLED_LEDGER)},
        }

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> tuple[int, dict[str, Any]]:
        self.calls.append((method, path))
        if (method, path) in self.raises:
            raise ClusterError("the test cluster API is unreachable")
        if (method, path) in self.status and method != "DELETE":
            return self.status[(method, path)], {"kind": "Status"}
        if method == "GET" and path.startswith("/api/v1/namespaces?"):
            if self.list_status != 200:
                return self.list_status, {}
            return 200, {"kind": "NamespaceList", "apiVersion": "v1", "items": self.items}
        ledger_ns = self._ledger_ns(path)
        if ledger_ns is not None and method == "GET":
            if self.ledgers.get(ledger_ns, SETTLED_LEDGER) is None:
                return 404, {"kind": "Status", "reason": "NotFound", "code": 404}
            return 200, self._configmap(ledger_ns)
        if ledger_ns is not None and method == "PUT":
            assert body is not None
            if body["metadata"].get("resourceVersion") != str(self.versions.get(ledger_ns, 1)):
                return 409, {"kind": "Status", "reason": "Conflict", "code": 409}
            self.ledgers[ledger_ns] = body["data"][IMAGES_CONFIGMAP_KEY]
            self.versions[ledger_ns] = self.versions.get(ledger_ns, 1) + 1
            return 200, self._configmap(ledger_ns)
        if method == "POST" and path.endswith("/configmaps"):
            assert body is not None
            ns = path.split("/")[4]
            if self.ledgers.get(ns, SETTLED_LEDGER) is not None:
                return 409, {"kind": "Status", "reason": "AlreadyExists", "code": 409}
            self.ledgers[ns] = body["data"][IMAGES_CONFIGMAP_KEY]
            return 201, self._configmap(ns)
        if method == "GET" and "/pods?" in path:
            ns = path.split("/")[4]
            return 200, {"kind": "PodList", "items": self.pods.get(ns, [])}
        if method == "DELETE":
            return self.status.get((method, path), 200), {}
        return 500, {}


def standard_items() -> list[dict[str, Any]]:
    return [
        namespace_item(f"{PREFIX}{RUN_LIVE}", labels=owned(RUN_LIVE), expires=FUTURE),
        namespace_item(f"{PREFIX}{RUN_DONE}", labels=owned(RUN_DONE), expires=FUTURE),
        namespace_item(f"{PREFIX}{RUN_OLD}", labels=owned(RUN_OLD), expires=PAST),
        namespace_item(f"{PREFIX}{RUN_NOANN}", labels=owned(RUN_NOANN), annotations_key=False),
        namespace_item(f"{PREFIX}{RUN_BAD}", labels=owned(RUN_BAD), expires="tomorrow-ish"),
        namespace_item(
            f"{PREFIX}{RUN_TERM}", labels=owned(RUN_TERM), expires=PAST, phase="Terminating"
        ),
        # Out of scope: prefix but no owner label.
        namespace_item(f"{PREFIX}nolabel", labels={RUN_LABEL: RUN_FOREIGN}, expires=PAST),
        # Out of scope: owner label but another prefix.
        namespace_item("team-prod", labels=owned(RUN_FOREIGN), expires=PAST),
        # Out of scope: prefix and owner key, but another owner's value.
        namespace_item(
            f"{PREFIX}otherowner",
            labels={OWNER_LABEL: "someone-else", RUN_LABEL: RUN_FOREIGN},
            expires=PAST,
        ),
    ]


FOREIGN = (f"{PREFIX}nolabel", "team-prod", f"{PREFIX}otherowner")


def test_scoped_namespaces_lists_by_selector_and_rechecks_client_side() -> None:
    cluster = FakeCluster(standard_items())
    found = scoped_namespaces(cluster, scope())

    assert cluster.calls == [("GET", LIST_PATH)]
    assert [item.name for item in found] == [
        f"{PREFIX}{RUN_LIVE}",
        f"{PREFIX}{RUN_DONE}",
        f"{PREFIX}{RUN_OLD}",
        f"{PREFIX}{RUN_NOANN}",
        f"{PREFIX}{RUN_BAD}",
        f"{PREFIX}{RUN_TERM}",
    ]
    by_name = {item.name: item for item in found}
    live = by_name[f"{PREFIX}{RUN_LIVE}"]
    assert live == ScopedNamespace(
        name=f"{PREFIX}{RUN_LIVE}",
        run=RUN_LIVE,
        expires_at=datetime(2026, 10, 2, 13, 0, tzinfo=UTC),
        terminating=False,
    )
    assert by_name[f"{PREFIX}{RUN_NOANN}"].expires_at is None
    assert by_name[f"{PREFIX}{RUN_BAD}"].expires_at is None
    assert by_name[f"{PREFIX}{RUN_TERM}"].terminating is True
    assert by_name[f"{PREFIX}{RUN_OLD}"].terminating is False


def test_scoped_namespace_without_run_label_has_empty_run() -> None:
    cluster = FakeCluster(
        [namespace_item(f"{PREFIX}norun", labels={OWNER_LABEL: OWNER}, expires=FUTURE)]
    )
    [item] = scoped_namespaces(cluster, scope())
    assert item.run == ""


@pytest.mark.parametrize("status", [401, 403, 500])
def test_scoped_namespaces_raises_on_a_non_200_list(status: int) -> None:
    cluster = FakeCluster(standard_items())
    cluster.list_status = status
    with pytest.raises(ClusterError):
        scoped_namespaces(cluster, scope())


def test_an_expired_namespace_is_reaped_in_child_first_order() -> None:
    name = f"{PREFIX}{RUN_OLD}"
    cluster = FakeCluster([namespace_item(name, labels=owned(RUN_OLD), expires=PAST)])
    found = scoped_namespaces(cluster, scope())
    cluster.calls.clear()

    result = sweep(cluster, scope(), found, now=NOW, terminal_runs=set(), registry=FakeRegistry())

    assert cluster.calls == reap_calls(name)
    # An hour past its TTL is past the overdue grace too.
    assert result == SweepResult(reaped=[name], expired=1, failed=[], overdue=1)


def test_a_terminal_run_namespace_is_reaped_before_its_ttl() -> None:
    name = f"{PREFIX}{RUN_DONE}"
    cluster = FakeCluster([namespace_item(name, labels=owned(RUN_DONE), expires=FUTURE)])
    found = scoped_namespaces(cluster, scope())
    cluster.calls.clear()

    result = sweep(
        cluster, scope(), found, now=NOW, terminal_runs={RUN_DONE}, registry=FakeRegistry()
    )

    assert cluster.calls == reap_calls(name)
    assert result.reaped == [name]
    assert result.expired == 0
    assert result.failed == []


def test_a_live_namespace_receives_no_call_at_all() -> None:
    name = f"{PREFIX}{RUN_LIVE}"
    cluster = FakeCluster([namespace_item(name, labels=owned(RUN_LIVE), expires=FUTURE)])
    found = scoped_namespaces(cluster, scope())
    cluster.calls.clear()

    result = sweep(
        cluster, scope(), found, now=NOW, terminal_runs={RUN_DONE}, registry=FakeRegistry()
    )

    assert cluster.calls == []
    assert result == SweepResult(reaped=[], expired=0, failed=[], overdue=0)


def test_expiry_exactly_now_is_expired() -> None:
    name = f"{PREFIX}{RUN_OLD}"
    cluster = FakeCluster(
        [namespace_item(name, labels=owned(RUN_OLD), expires="2026-10-02T12:00:00Z")]
    )
    found = scoped_namespaces(cluster, scope())
    cluster.calls.clear()
    result = sweep(cluster, scope(), found, now=NOW, terminal_runs=set(), registry=FakeRegistry())
    assert result.reaped == [name]
    assert result.expired == 1


def test_missing_child_kinds_still_delete_the_namespace() -> None:
    name = f"{PREFIX}{RUN_OLD}"
    cluster = FakeCluster([namespace_item(name, labels=owned(RUN_OLD), expires=PAST)])
    for path in child_paths(name):
        cluster.status[("DELETE", path)] = 404
    found = scoped_namespaces(cluster, scope())
    cluster.calls.clear()

    result = sweep(cluster, scope(), found, now=NOW, terminal_runs=set(), registry=FakeRegistry())

    assert cluster.calls == reap_calls(name)
    assert result.reaped == [name]
    assert result.failed == []


def test_a_refused_child_delete_on_an_active_namespace_still_deletes_it_but_fails() -> None:
    """A 403 on an Active namespace means the grant is wrong, not that teardown is underway.

    The namespace delete is still sent, but the namespace is not reported reaped:
    the children the reaper could not delete may be what holds it.
    """

    name = f"{PREFIX}{RUN_OLD}"
    cluster = FakeCluster([namespace_item(name, labels=owned(RUN_OLD), expires=PAST)])
    for path in child_paths(name):
        cluster.status[("DELETE", path)] = 403
    found = scoped_namespaces(cluster, scope())
    cluster.calls.clear()

    result = sweep(cluster, scope(), found, now=NOW, terminal_runs=set(), registry=FakeRegistry())

    assert cluster.calls == reap_calls(name)
    assert result.reaped == []
    assert result.failed == [name]


def test_a_refused_child_delete_on_a_terminating_namespace_counts_as_reaped() -> None:
    """Namespace teardown removes the RoleBinding first, so a 403 there is expected."""

    name = f"{PREFIX}{RUN_TERM}"
    cluster = FakeCluster(
        [namespace_item(name, labels=owned(RUN_TERM), expires=PAST, phase="Terminating")]
    )
    for path in child_paths(name):
        cluster.status[("DELETE", path)] = 403
    found = scoped_namespaces(cluster, scope())
    cluster.calls.clear()

    result = sweep(cluster, scope(), found, now=NOW, terminal_runs=set(), registry=FakeRegistry())

    assert cluster.calls == reap_calls(name)
    assert result.reaped == [name]
    assert result.failed == []


def test_a_sandboxclaims_403_alone_does_not_stop_the_rest() -> None:
    name = f"{PREFIX}{RUN_OLD}"
    cluster = FakeCluster([namespace_item(name, labels=owned(RUN_OLD), expires=PAST)])
    cluster.status[("DELETE", child_paths(name)[0])] = 403
    found = scoped_namespaces(cluster, scope())
    cluster.calls.clear()

    result = sweep(cluster, scope(), found, now=NOW, terminal_runs=set(), registry=FakeRegistry())

    # Every later child and the namespace itself are still deleted, but the
    # namespace is Active, so the refusal fails it.
    assert cluster.calls == reap_calls(name)
    assert result.reaped == []
    assert result.failed == [name]


@pytest.mark.parametrize("ns_status", [200, 202, 404, 409])
def test_namespace_delete_accepts_gone_and_already_terminating(ns_status: int) -> None:
    name = f"{PREFIX}{RUN_OLD}"
    cluster = FakeCluster([namespace_item(name, labels=owned(RUN_OLD), expires=PAST)])
    cluster.status[("DELETE", f"/api/v1/namespaces/{name}")] = ns_status
    found = scoped_namespaces(cluster, scope())
    result = sweep(cluster, scope(), found, now=NOW, terminal_runs=set(), registry=FakeRegistry())
    assert result.reaped == [name]
    assert result.failed == []


def test_a_terminating_namespace_past_ttl_gets_its_child_deletes_reissued() -> None:
    name = f"{PREFIX}{RUN_TERM}"
    cluster = FakeCluster(
        [namespace_item(name, labels=owned(RUN_TERM), expires=PAST, phase="Terminating")]
    )
    found = scoped_namespaces(cluster, scope())
    cluster.calls.clear()
    result = sweep(cluster, scope(), found, now=NOW, terminal_runs=set(), registry=FakeRegistry())
    assert cluster.calls == reap_calls(name)
    assert result.expired == 1


def test_foreign_namespaces_never_receive_a_call_even_expired_and_terminal() -> None:
    cluster = FakeCluster(standard_items())
    found = scoped_namespaces(cluster, scope())
    result = sweep(
        cluster,
        scope(),
        found,
        now=NOW,
        terminal_runs={RUN_FOREIGN, RUN_DONE},
        registry=FakeRegistry(),
    )

    for method, path in cluster.calls:
        for foreign in FOREIGN:
            assert f"/{foreign}" not in path, (method, path)
    for foreign in FOREIGN:
        assert foreign not in result.reaped
        assert foreign not in result.failed


def test_sweep_rechecks_the_prefix_on_names_it_is_handed() -> None:
    cluster = FakeCluster([])
    handed = [
        ScopedNamespace(name="kube-system", run=RUN_DONE, expires_at=None, terminating=False),
        ScopedNamespace(name="curie-e2e", run=RUN_DONE, expires_at=None, terminating=False),
    ]
    result = sweep(
        cluster, scope(), handed, now=NOW, terminal_runs={RUN_DONE}, registry=FakeRegistry()
    )
    assert cluster.calls == []
    assert result.reaped == []
    assert result.failed == []


def test_missing_and_malformed_ttl_are_counted_expired_and_reaped() -> None:
    cluster = FakeCluster(standard_items())
    found = scoped_namespaces(cluster, scope())
    cluster.calls.clear()

    result = sweep(
        cluster, scope(), found, now=NOW, terminal_runs={RUN_DONE}, registry=FakeRegistry()
    )

    # Past TTL: RUN_OLD, RUN_TERM. Unreadable TTL: RUN_NOANN, RUN_BAD.
    assert result.expired == 4
    # All four are an hour past or unreadable, so all four are overdue.
    assert result.overdue == 4
    assert sorted(result.reaped) == sorted(
        [
            f"{PREFIX}{RUN_DONE}",
            f"{PREFIX}{RUN_OLD}",
            f"{PREFIX}{RUN_NOANN}",
            f"{PREFIX}{RUN_BAD}",
            f"{PREFIX}{RUN_TERM}",
        ]
    )
    assert result.failed == []
    for name in result.reaped:
        # Scan forward so a repeated call (the ledger is read twice) matches
        # its own later occurrence, not the first one.
        cursor = 0
        for call in reap_calls(name):
            cursor = cluster.calls.index(call, cursor) + 1
    assert not any(f"/{PREFIX}{RUN_LIVE}" in path for _, path in cluster.calls)


def test_a_failed_namespace_delete_is_recorded_and_the_sweep_continues() -> None:
    first = f"{PREFIX}{RUN_OLD}"
    second = f"{PREFIX}{RUN_BAD}"
    cluster = FakeCluster(
        [
            namespace_item(first, labels=owned(RUN_OLD), expires=PAST),
            namespace_item(second, labels=owned(RUN_BAD), expires=PAST),
        ]
    )
    cluster.status[("DELETE", f"/api/v1/namespaces/{first}")] = 500
    found = scoped_namespaces(cluster, scope())
    cluster.calls.clear()

    result = sweep(cluster, scope(), found, now=NOW, terminal_runs=set(), registry=FakeRegistry())

    assert result.failed == [first]
    assert result.reaped == [second]
    assert result.expired == 2
    assert cluster.calls == reap_calls(first) + reap_calls(second)


def test_a_child_transport_error_fails_that_namespace_only() -> None:
    first = f"{PREFIX}{RUN_OLD}"
    second = f"{PREFIX}{RUN_BAD}"
    cluster = FakeCluster(
        [
            namespace_item(first, labels=owned(RUN_OLD), expires=PAST),
            namespace_item(second, labels=owned(RUN_BAD), expires=PAST),
        ]
    )
    cluster.raises.add(("DELETE", child_paths(first)[1]))
    found = scoped_namespaces(cluster, scope())
    cluster.calls.clear()

    result = sweep(cluster, scope(), found, now=NOW, terminal_runs=set(), registry=FakeRegistry())

    assert result.failed == [first]
    assert result.reaped == [second]
    assert ("DELETE", f"/api/v1/namespaces/{second}") in cluster.calls


def test_a_child_500_fails_that_namespace() -> None:
    name = f"{PREFIX}{RUN_OLD}"
    cluster = FakeCluster([namespace_item(name, labels=owned(RUN_OLD), expires=PAST)])
    cluster.status[("DELETE", child_paths(name)[2])] = 500
    found = scoped_namespaces(cluster, scope())
    result = sweep(cluster, scope(), found, now=NOW, terminal_runs=set(), registry=FakeRegistry())
    assert result.failed == [name]
    assert result.reaped == []


def _expiring(seconds_before_now: float) -> str:
    return (NOW - timedelta(seconds=seconds_before_now)).isoformat()


def test_overdue_grace_is_ten_minutes() -> None:
    assert OVERDUE_GRACE_S == 600


def test_overdue_counts_only_namespaces_past_the_grace_or_without_a_ttl() -> None:
    """``expired`` is every namespace past its TTL; ``overdue`` is the ones the reaper is late on.

    A healthy reaper always sees a few namespaces just past their TTL, so the
    alert keys on ``overdue``, which leaves room for one sweep interval.
    """

    within = f"{PREFIX}within"
    edge = f"{PREFIX}edge"
    late = f"{PREFIX}late"
    noann = f"{PREFIX}noann"
    future = f"{PREFIX}future"
    terminating = f"{PREFIX}terminating"
    cluster = FakeCluster(
        [
            namespace_item(within, labels={OWNER_LABEL: OWNER}, expires=_expiring(599)),
            namespace_item(edge, labels={OWNER_LABEL: OWNER}, expires=_expiring(OVERDUE_GRACE_S)),
            namespace_item(late, labels={OWNER_LABEL: OWNER}, expires=_expiring(3600)),
            namespace_item(noann, labels={OWNER_LABEL: OWNER}, annotations_key=False),
            namespace_item(future, labels=owned(RUN_DONE), expires=FUTURE),
            namespace_item(
                terminating,
                labels={OWNER_LABEL: OWNER},
                expires=_expiring(3600),
                phase="Terminating",
            ),
            # Foreign namespaces are never counted, however late.
            namespace_item("team-prod", labels=owned(RUN_FOREIGN), expires=_expiring(3600)),
        ]
    )
    found = scoped_namespaces(cluster, scope())

    result = sweep(
        cluster, scope(), found, now=NOW, terminal_runs={RUN_DONE}, registry=FakeRegistry()
    )

    assert result.expired == 5
    # edge (exactly the grace), late, noann, terminating. Not within, not future.
    assert result.overdue == 4
    assert result.failed == []


def test_overdue_counts_a_namespace_whose_delete_failed() -> None:
    name = f"{PREFIX}{RUN_OLD}"
    cluster = FakeCluster([namespace_item(name, labels=owned(RUN_OLD), expires=PAST)])
    cluster.status[("DELETE", f"/api/v1/namespaces/{name}")] = 500
    found = scoped_namespaces(cluster, scope())

    result = sweep(cluster, scope(), found, now=NOW, terminal_runs=set(), registry=FakeRegistry())

    assert result == SweepResult(reaped=[], expired=1, failed=[name], overdue=1)


_TOKEN_KUBECONFIG = (
    "apiVersion: v1\n"
    "clusters:\n- cluster: {server: 'https://test-cluster.example:6443'}\n  name: t\n"
    "contexts:\n- context: {cluster: t, user: u}\n  name: c\n"
    "current-context: c\n"
    "users:\n- user: {token: super-secret-token}\n  name: u\n"
)

_EXEC_KUBECONFIG = (
    "apiVersion: v1\n"
    "clusters:\n- cluster: {server: 'https://test-cluster.example:6443'}\n  name: t\n"
    "users:\n- name: u\n  user:\n    exec:\n      apiVersion: client.authentication.k8s.io/v1\n"
    "      command: aws\n"
)


def test_load_kubeconfig_text_accepts_a_token_kubeconfig() -> None:
    server, token, verify = load_kubeconfig_text(_TOKEN_KUBECONFIG)
    assert server == "https://test-cluster.example:6443"
    assert token == "super-secret-token"
    assert verify is True


def test_load_kubeconfig_text_refuses_an_exec_plugin() -> None:
    with pytest.raises(ClusterError, match="exec plugin"):
        load_kubeconfig_text(_EXEC_KUBECONFIG)


def test_load_kubeconfig_text_refuses_plain_http_without_leaking_the_token() -> None:
    text = _TOKEN_KUBECONFIG.replace("https://", "http://")
    with pytest.raises(ClusterError, match="https") as excinfo:
        load_kubeconfig_text(text)
    assert "super-secret-token" not in str(excinfo.value)


def test_load_kubeconfig_text_refuses_unparseable_yaml() -> None:
    with pytest.raises(ClusterError, match="e2e_connector_misconfigured"):
        load_kubeconfig_text("clusters: [unclosed")


def test_client_from_kubeconfig_text_carries_server_and_bearer() -> None:
    client = client_from_kubeconfig_text(_TOKEN_KUBECONFIG, timeout=7.0)
    try:
        assert isinstance(client, httpx.Client)
        assert str(client.base_url).rstrip("/") == "https://test-cluster.example:6443"
        assert client.headers["Authorization"] == "Bearer super-secret-token"
        assert client.timeout.read == 7.0
    finally:
        client.close()


# Image retention before the namespace delete (#3246)

DIGEST_A = "sha256:" + "a1" * 32
DIGEST_S = "sha256:" + "5e" * 32


def build_pod(phase: str) -> dict[str, Any]:
    return {
        "metadata": {
            "name": "e2e-build-0a1b2c3d-x7k2p",
            "labels": {BUILD_LABEL: "e2e-build-0a1b2c3d"},
        },
        "status": {"phase": phase},
    }


def one_expired(run: str = RUN_OLD, *, phase: str = "Active") -> tuple[str, FakeCluster]:
    name = f"{PREFIX}{run}"
    cluster = FakeCluster([namespace_item(name, labels=owned(run), expires=PAST, phase=phase)])
    return name, cluster


def test_sweep_target_pairs_a_cluster_with_its_registry() -> None:
    cluster = FakeCluster([])
    registry = FakeRegistry()
    target = SweepTarget(cluster, registry)
    assert target.cluster is cluster
    assert target.registry is registry
    with pytest.raises(AttributeError):
        target.cluster = FakeCluster([])  # type: ignore[misc]


def test_images_are_deleted_before_children_and_the_namespace() -> None:
    name, cluster = one_expired()
    cluster.ledgers[name] = json.dumps(
        {"repositories": [repo(name)], "closing_at": "2026-10-02T11:00:00Z"}
    )
    registry = FakeRegistry(
        cluster.calls,
        {repo(name): {"staging-0a1b2c3d": DIGEST_S, "build-0a1b2c3d": DIGEST_A}},
    )
    found = scoped_namespaces(cluster, scope())
    cluster.calls.clear()

    result = sweep(cluster, scope(), found, now=NOW, terminal_runs=set(), registry=registry)

    assert result.reaped == [name]
    assert sorted(registry.deleted) == sorted([(repo(name), DIGEST_A), (repo(name), DIGEST_S)])
    registry_positions = [i for i, call in enumerate(cluster.calls) if call[0] == "REGISTRY"]
    child_positions = [
        cluster.calls.index(("DELETE", path))
        for path in [*child_paths(name), f"/api/v1/namespaces/{name}"]
    ]
    assert registry_positions
    assert max(registry_positions) < min(child_positions)
    assert cluster.calls.index(("GET", build_pods_path(name))) < min(registry_positions)


def test_a_running_build_pod_defers_the_namespace() -> None:
    name, cluster = one_expired()
    cluster.pods[name] = [build_pod("Running")]
    registry = FakeRegistry(cluster.calls)
    found = scoped_namespaces(cluster, scope())
    cluster.calls.clear()

    result = sweep(cluster, scope(), found, now=NOW, terminal_runs=set(), registry=registry)

    assert result.deferred == [name]
    assert result.failed == []
    assert result.reaped == []
    assert ("DELETE", f"/api/v1/namespaces/{name}") not in cluster.calls
    assert not any(("DELETE", path) in cluster.calls for path in child_paths(name))
    # Deferral is not a failure, but the namespace still counts as overdue.
    assert result.overdue == 1


def test_inside_the_settle_window_defers_then_a_later_pass_reaps() -> None:
    name, cluster = one_expired()
    cluster.ledgers[name] = json.dumps({"repositories": [repo(name)]})
    registry = FakeRegistry(cluster.calls, {repo(name): {"build-0a1b2c3d": DIGEST_A}})
    found = scoped_namespaces(cluster, scope())
    cluster.calls.clear()

    first = sweep(cluster, scope(), found, now=NOW, terminal_runs=set(), registry=registry)

    assert first.deferred == [name]
    assert first.failed == [] and first.reaped == []
    assert cluster.calls == [("GET", ledger_path(name)), ("PUT", ledger_path(name))]
    closed = json.loads(cluster.ledgers[name] or "")
    assert datetime.fromisoformat(closed["closing_at"]) == NOW
    assert closed["repositories"] == [repo(name)]

    cluster.calls.clear()
    later = NOW + timedelta(seconds=10)
    second = sweep(cluster, scope(), found, now=later, terminal_runs=set(), registry=registry)

    assert second.reaped == [name]
    assert second.deferred == []
    assert registry.deleted == [(repo(name), DIGEST_A)]
    assert cluster.calls[-1] == ("DELETE", f"/api/v1/namespaces/{name}")


def test_a_registry_failure_fails_the_namespace_without_deleting_it() -> None:
    name, cluster = one_expired()
    cluster.ledgers[name] = json.dumps(
        {"repositories": [repo(name)], "closing_at": "2026-10-02T11:00:00Z"}
    )
    registry = FakeRegistry(cluster.calls, fail_repos={repo(name)})
    found = scoped_namespaces(cluster, scope())
    cluster.calls.clear()

    result = sweep(cluster, scope(), found, now=NOW, terminal_runs=set(), registry=registry)

    assert result.failed == [name]
    assert result.reaped == [] and result.deferred == []
    assert ("DELETE", f"/api/v1/namespaces/{name}") not in cluster.calls
    assert not any(("DELETE", path) in cluster.calls for path in child_paths(name))


def test_one_namespaces_registry_outage_does_not_stop_the_next() -> None:
    first = f"{PREFIX}{RUN_OLD}"
    second = f"{PREFIX}{RUN_BAD}"
    cluster = FakeCluster(
        [
            namespace_item(first, labels=owned(RUN_OLD), expires=PAST),
            namespace_item(second, labels=owned(RUN_BAD), expires=PAST),
        ]
    )
    for name in (first, second):
        cluster.ledgers[name] = json.dumps(
            {"repositories": [repo(name)], "closing_at": "2026-10-02T11:00:00Z"}
        )
    registry = FakeRegistry(
        cluster.calls,
        {repo(second): {"build-0a1b2c3d": DIGEST_A}},
        fail_repos={repo(first)},
    )
    found = scoped_namespaces(cluster, scope())
    cluster.calls.clear()

    result = sweep(cluster, scope(), found, now=NOW, terminal_runs=set(), registry=registry)

    assert result.failed == [first]
    assert result.reaped == [second]
    assert registry.deleted == [(repo(second), DIGEST_A)]
    assert ("DELETE", f"/api/v1/namespaces/{first}") not in cluster.calls
    assert ("DELETE", f"/api/v1/namespaces/{second}") in cluster.calls


def test_a_malformed_ledger_fails_the_namespace() -> None:
    name, cluster = one_expired()
    cluster.ledgers[name] = json.dumps(
        {
            "repositories": [f"{REGISTRY_PREFIX}/{name}/../../victim/app"],
            "closing_at": "2026-10-02T11:00:00Z",
        }
    )
    registry = FakeRegistry(cluster.calls)
    found = scoped_namespaces(cluster, scope())
    cluster.calls.clear()

    result = sweep(cluster, scope(), found, now=NOW, terminal_runs=set(), registry=registry)

    assert result.failed == [name]
    assert not any(call[0] == "REGISTRY" for call in cluster.calls)
    assert ("DELETE", f"/api/v1/namespaces/{name}") not in cluster.calls


def test_a_terminating_namespace_with_a_refused_ledger_is_reaped() -> None:
    name, cluster = one_expired(RUN_TERM, phase="Terminating")
    cluster.status[("GET", ledger_path(name))] = 403
    cluster.status[("DELETE", build_jobs_path(name))] = 403
    cluster.status[("GET", build_pods_path(name))] = 403
    for path in child_paths(name):
        cluster.status[("DELETE", path)] = 403
    registry = FakeRegistry(cluster.calls)
    found = scoped_namespaces(cluster, scope())
    cluster.calls.clear()

    result = sweep(cluster, scope(), found, now=NOW, terminal_runs=set(), registry=registry)

    assert result.reaped == [name]
    assert result.failed == [] and result.deferred == []
    assert not any(call[0] == "REGISTRY" for call in cluster.calls)
    assert cluster.calls[-1] == ("DELETE", f"/api/v1/namespaces/{name}")


def test_a_live_namespace_gets_no_retention_call() -> None:
    name = f"{PREFIX}{RUN_LIVE}"
    cluster = FakeCluster([namespace_item(name, labels=owned(RUN_LIVE), expires=FUTURE)])
    cluster.ledgers[name] = json.dumps({"repositories": [repo(name)]})
    registry = FakeRegistry(cluster.calls, {repo(name): {"build-0a1b2c3d": DIGEST_A}})
    found = scoped_namespaces(cluster, scope())
    cluster.calls.clear()

    result = sweep(cluster, scope(), found, now=NOW, terminal_runs=set(), registry=registry)

    assert cluster.calls == []
    assert result == SweepResult(reaped=[], expired=0, failed=[], overdue=0, deferred=[])
