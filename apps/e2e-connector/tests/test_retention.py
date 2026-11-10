"""Teardown retention: close admission, stop builds, delete a run's images (#3246).

The cluster is faked at the ``ClusterApi`` seam. Shapes follow the Kubernetes
API reference (https://kubernetes.io/docs/reference/kubernetes-api/):

* ConfigMap ``data`` plus ``metadata.resourceVersion``; a PUT carrying a stale
  resourceVersion answers 409 Conflict
  (https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions).
* Pod ``status.phase`` (Pending, Running, Succeeded, Failed, Unknown)
  (https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#PodStatus).

The registry is a real ``DockerRegistry`` over an ``httpx.MockTransport``
shaped like distribution (https://distribution.github.io/distribution/spec/api/):
tags/list ``{"name", "tags"}``, HEAD with ``Docker-Content-Digest``, DELETE 202,
and 404 bodies with ``NAME_UNKNOWN`` / ``MANIFEST_UNKNOWN``.
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
    CLOSE_SETTLE_S,
    IMAGES_CONFIGMAP,
    IMAGES_CONFIGMAP_KEY,
)
from curie_e2e_connector.kube import ClusterApi, ClusterError
from curie_e2e_connector.registry import DockerRegistry, RegistryError, RegistrySettings
from curie_e2e_connector.retention import (
    BuildInProgress,
    CloseSettling,
    TeardownDeferred,
    prepare_teardown,
)

NOW = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)
HOST = "registry.test.example"
PREFIX = f"{HOST}/e2e"
NS = "curie-e2e-aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
REPO = f"{PREFIX}/{NS}/app"
REPO_PATH = f"e2e/{NS}/app"
LEDGER_PATH = f"/api/v1/namespaces/{NS}/configmaps/{IMAGES_CONFIGMAP}"
CONFIGMAPS_PATH = f"/api/v1/namespaces/{NS}/configmaps"

DIGEST_A = "sha256:" + "a1" * 32
DIGEST_B = "sha256:" + "b2" * 32
DIGEST_S = "sha256:" + "5e" * 32


def stamp(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def ledger(repositories: list[str], closing_at: datetime | None = None) -> str:
    body: dict[str, Any] = {"repositories": repositories}
    if closing_at is not None:
        body["closing_at"] = stamp(closing_at)
    return json.dumps(body)


def split(path: str) -> tuple[str, dict[str, str]]:
    parsed = urllib.parse.urlsplit(path)
    return parsed.path, dict(urllib.parse.parse_qsl(parsed.query))


class FakeTeardownCluster(ClusterApi):
    """A namespace's ledger ConfigMap, build Jobs and build pods."""

    def __init__(
        self,
        log: list[tuple[str, ...]],
        *,
        ledger_text: str | None,
        pods: list[dict[str, Any]] | None = None,
    ) -> None:
        self.log = log
        self.ledger_text = ledger_text
        self.resource_version = 7
        self.pods = pods or []
        # Scripted status overrides, consumed one per ledger GET.
        self.ledger_get_script: list[int | None] = []
        self.ledger_status: int | None = None
        self.put_conflicts = 0
        self.on_conflict: Any = None
        self.jobs_status = 200
        self.pods_status = 200
        self.puts: list[dict[str, Any]] = []
        self.posts: list[dict[str, Any]] = []

    def configmap(self) -> dict[str, Any]:
        return {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": IMAGES_CONFIGMAP,
                "namespace": NS,
                "resourceVersion": str(self.resource_version),
            },
            "data": {IMAGES_CONFIGMAP_KEY: self.ledger_text},
        }

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> tuple[int, dict[str, Any]]:
        self.log.append(("cluster", method, path))
        bare, query = split(path)
        if bare == LEDGER_PATH and method == "GET":
            if self.ledger_get_script:
                scripted = self.ledger_get_script.pop(0)
                if scripted is not None:
                    return scripted, {"kind": "Status", "code": scripted}
            if self.ledger_status is not None:
                return self.ledger_status, {"kind": "Status", "code": self.ledger_status}
            if self.ledger_text is None:
                return 404, {"kind": "Status", "reason": "NotFound", "code": 404}
            return 200, self.configmap()
        if bare == LEDGER_PATH and method == "PUT":
            assert body is not None
            self.puts.append(body)
            if self.put_conflicts:
                self.put_conflicts -= 1
                if self.on_conflict is not None:
                    self.on_conflict(self)
                self.resource_version += 1
                return 409, {"kind": "Status", "reason": "Conflict", "code": 409}
            if body["metadata"].get("resourceVersion") != str(self.resource_version):
                return 409, {"kind": "Status", "reason": "Conflict", "code": 409}
            self.ledger_text = body["data"][IMAGES_CONFIGMAP_KEY]
            self.resource_version += 1
            return 200, self.configmap()
        if bare == CONFIGMAPS_PATH and method == "POST":
            assert body is not None
            self.posts.append(body)
            if self.ledger_text is not None:
                return 409, {"kind": "Status", "reason": "AlreadyExists", "code": 409}
            self.ledger_text = body["data"][IMAGES_CONFIGMAP_KEY]
            return 201, self.configmap()
        if bare == f"/apis/batch/v1/namespaces/{NS}/jobs" and method == "DELETE":
            return self.jobs_status, {"kind": "Status", "status": "Success"}
        if bare == f"/api/v1/namespaces/{NS}/pods" and method == "GET":
            if self.pods_status != 200:
                return self.pods_status, {"kind": "Status", "code": self.pods_status}
            return 200, {"kind": "PodList", "apiVersion": "v1", "items": self.pods}
        return 500, {"kind": "Status", "code": 500}


class FakeDistribution:
    """A distribution registry holding ``repo path -> {tag: digest}``."""

    def __init__(self, log: list[tuple[str, ...]], tags: dict[str, dict[str, str]]) -> None:
        self.log = log
        self.tags = tags
        self.deleted: list[str] = []
        self.delete_status: dict[str, int] = {}

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.log.append(("registry", request.method, request.url.path))
        path = request.url.path
        assert path.startswith("/v2/")
        repo, _, rest = path[len("/v2/") :].partition("/tags/list")
        if path.endswith("/tags/list"):
            if repo not in self.tags:
                return httpx.Response(
                    404,
                    json={
                        "errors": [
                            {
                                "code": "NAME_UNKNOWN",
                                "message": "repository name not known to registry",
                                "detail": {},
                            }
                        ]
                    },
                )
            listed = sorted(self.tags[repo])
            return httpx.Response(200, json={"name": repo, "tags": listed or None})
        repo, _, reference = path[len("/v2/") :].partition("/manifests/")
        known = self.tags.get(repo, {})
        if request.method == "HEAD":
            if reference not in known:
                return httpx.Response(404)
            return httpx.Response(200, headers={"Docker-Content-Digest": known[reference]})
        if request.method == "DELETE":
            self.deleted.append(reference)
            status = self.delete_status.get(reference, 202)
            if status == 404:
                return httpx.Response(
                    404,
                    json={
                        "errors": [
                            {
                                "code": "MANIFEST_UNKNOWN",
                                "message": "manifest unknown",
                                "detail": {},
                            }
                        ]
                    },
                )
            return httpx.Response(status)
        return httpx.Response(405)


def registry_for(distribution: FakeDistribution, *, prefix: str = PREFIX) -> DockerRegistry:
    client = httpx.Client(transport=httpx.MockTransport(distribution), follow_redirects=False)
    return DockerRegistry(
        {}, settings=RegistrySettings(prefix=prefix, insecure=False, token_hosts=()), client=client
    )


def setup(
    *,
    ledger_text: str | None,
    tags: dict[str, dict[str, str]] | None = None,
    pods: list[dict[str, Any]] | None = None,
    prefix: str = PREFIX,
) -> tuple[list[tuple[str, ...]], FakeTeardownCluster, FakeDistribution, DockerRegistry]:
    log: list[tuple[str, ...]] = []
    cluster = FakeTeardownCluster(log, ledger_text=ledger_text, pods=pods)
    distribution = FakeDistribution(log, tags or {})
    return log, cluster, distribution, registry_for(distribution, prefix=prefix)


def registry_calls(log: list[tuple[str, ...]]) -> list[tuple[str, ...]]:
    return [entry for entry in log if entry[0] == "registry"]


def pod(phase: str, name: str = "e2e-build-0a1b2c3d-x7k2p") -> dict[str, Any]:
    return {
        "metadata": {"name": name, "labels": {BUILD_LABEL: "e2e-build-0a1b2c3d"}},
        "status": {"phase": phase},
    }


CLOSED_LONG_AGO = NOW - timedelta(minutes=5)


def test_teardown_exceptions_are_cluster_errors() -> None:
    assert issubclass(TeardownDeferred, ClusterError)
    assert issubclass(CloseSettling, TeardownDeferred)
    assert issubclass(BuildInProgress, TeardownDeferred)
    assert CLOSE_SETTLE_S == 10


# Order


def test_order_is_ledger_jobs_pods_ledger_then_registry() -> None:
    log, cluster, distribution, registry = setup(
        ledger_text=ledger([REPO], CLOSED_LONG_AGO),
        tags={REPO_PATH: {"staging-0a1b2c3d": DIGEST_S, "build-0a1b2c3d": DIGEST_A}},
        pods=[pod("Succeeded")],
    )

    prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)

    cluster_calls = [entry for entry in log if entry[0] == "cluster"]
    assert [(m, split(p)[0]) for _, m, p in cluster_calls] == [
        ("GET", LEDGER_PATH),
        ("DELETE", f"/apis/batch/v1/namespaces/{NS}/jobs"),
        ("GET", f"/api/v1/namespaces/{NS}/pods"),
        ("GET", LEDGER_PATH),
    ]
    _bare, jobs_query = split(cluster_calls[1][2])
    assert jobs_query == {"labelSelector": BUILD_LABEL, "propagationPolicy": "Background"}
    _bare, pods_query = split(cluster_calls[2][2])
    assert pods_query == {"labelSelector": BUILD_LABEL}
    # Every registry call follows every cluster call.
    first_registry = log.index(registry_calls(log)[0])
    assert all(log.index(entry) < first_registry for entry in cluster_calls)
    assert registry_calls(log)[0] == ("registry", "GET", f"/v2/{REPO_PATH}/tags/list")
    assert sorted(distribution.deleted) == sorted([DIGEST_A, DIGEST_S])
    # Nothing was written: the existing closing_at was kept.
    assert cluster.puts == [] and cluster.posts == []


# Close and settle


def test_an_open_ledger_is_closed_with_a_conditioned_put_then_settles() -> None:
    log, cluster, _distribution, registry = setup(ledger_text=ledger([REPO]))

    with pytest.raises(CloseSettling) as excinfo:
        prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)

    assert excinfo.value.remaining_s == pytest.approx(CLOSE_SETTLE_S)
    [put] = cluster.puts
    assert put["metadata"]["resourceVersion"] == "7"
    written = json.loads(put["data"][IMAGES_CONFIGMAP_KEY])
    assert written["repositories"] == [REPO]
    assert datetime.fromisoformat(written["closing_at"]) == NOW
    # Settling stops before builds are touched or the registry is called.
    assert [(m, p) for _, m, p in log] == [("GET", LEDGER_PATH), ("PUT", LEDGER_PATH)]


def test_a_missing_ledger_is_created_closed() -> None:
    log, cluster, _distribution, registry = setup(ledger_text=None)

    with pytest.raises(CloseSettling) as excinfo:
        prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)

    assert excinfo.value.remaining_s == pytest.approx(CLOSE_SETTLE_S)
    [post] = cluster.posts
    assert post["metadata"]["name"] == IMAGES_CONFIGMAP
    written = json.loads(post["data"][IMAGES_CONFIGMAP_KEY])
    assert written["repositories"] == []
    assert datetime.fromisoformat(written["closing_at"]) == NOW
    assert registry_calls(log) == []
    assert not any(m == "DELETE" for _, m, _p in log)


def test_an_existing_closing_at_is_kept() -> None:
    closed = NOW - timedelta(seconds=3)
    _log, cluster, _distribution, registry = setup(ledger_text=ledger([REPO], closed))

    with pytest.raises(CloseSettling):
        prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)

    assert cluster.puts == []
    assert cluster.posts == []
    assert json.loads(cluster.ledger_text or "")["closing_at"] == stamp(closed)


@pytest.mark.parametrize(
    ("age_s", "remaining"),
    [(9, 1.0), (0, 10.0), (-30, 10.0)],
    ids=["nine-seconds-old", "just-closed", "future-clock-skew"],
)
def test_inside_the_settle_window_raises_close_settling(age_s: int, remaining: float) -> None:
    log, cluster, _distribution, registry = setup(
        ledger_text=ledger([REPO], NOW - timedelta(seconds=age_s))
    )
    with pytest.raises(CloseSettling) as excinfo:
        prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)
    assert excinfo.value.remaining_s == pytest.approx(remaining)
    assert registry_calls(log) == []
    assert not any(m == "DELETE" for _, m, _p in log)


def test_exactly_the_settle_age_proceeds() -> None:
    log, cluster, distribution, registry = setup(
        ledger_text=ledger([REPO], NOW - timedelta(seconds=CLOSE_SETTLE_S)),
        tags={REPO_PATH: {"build-0a1b2c3d": DIGEST_A}},
    )
    prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)
    assert distribution.deleted == [DIGEST_A]


def test_a_409_on_the_close_put_rereads_and_retries() -> None:
    other = f"{PREFIX}/{NS}/worker"
    _log, cluster, _distribution, registry = setup(ledger_text=ledger([REPO]))
    cluster.put_conflicts = 1

    def concurrent_build_appends(fake: FakeTeardownCluster) -> None:
        fake.ledger_text = ledger([REPO, other])

    cluster.on_conflict = concurrent_build_appends

    with pytest.raises(CloseSettling):
        prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)

    assert len(cluster.puts) == 2
    assert cluster.puts[1]["metadata"]["resourceVersion"] == "8"
    final = json.loads(cluster.ledger_text or "")
    assert final["repositories"] == [REPO, other]
    assert datetime.fromisoformat(final["closing_at"]) == NOW


def test_endless_close_conflicts_give_up() -> None:
    _log, cluster, _distribution, registry = setup(ledger_text=ledger([REPO]))
    cluster.put_conflicts = 100
    with pytest.raises(ClusterError) as excinfo:
        prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)
    assert not isinstance(excinfo.value, TeardownDeferred)
    assert len(cluster.puts) <= 5


@pytest.mark.parametrize("status", [403, 500])
def test_an_unreadable_ledger_on_an_active_namespace_raises(status: int) -> None:
    log, cluster, _distribution, registry = setup(ledger_text=ledger([REPO], CLOSED_LONG_AGO))
    cluster.ledger_status = status
    with pytest.raises(ClusterError) as excinfo:
        prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)
    assert not isinstance(excinfo.value, TeardownDeferred)
    assert registry_calls(log) == []


def test_an_unparseable_closing_at_raises() -> None:
    text = json.dumps({"repositories": [REPO], "closing_at": "soon"})
    log, cluster, _distribution, registry = setup(ledger_text=text)
    with pytest.raises(ClusterError) as excinfo:
        prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)
    assert not isinstance(excinfo.value, TeardownDeferred)
    assert registry_calls(log) == []


# Stopping builds


def test_a_running_build_pod_defers() -> None:
    log, cluster, _distribution, registry = setup(
        ledger_text=ledger([REPO], CLOSED_LONG_AGO),
        tags={REPO_PATH: {"build-0a1b2c3d": DIGEST_A}},
        pods=[pod("Succeeded", "e2e-build-old-1"), pod("Running")],
    )
    with pytest.raises(BuildInProgress, match="e2e_build_in_progress"):
        prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)
    assert registry_calls(log) == []


def test_a_pending_build_pod_defers() -> None:
    log, cluster, _distribution, registry = setup(
        ledger_text=ledger([REPO], CLOSED_LONG_AGO), pods=[pod("Pending")]
    )
    with pytest.raises(BuildInProgress):
        prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)
    assert registry_calls(log) == []


def test_finished_build_pods_do_not_defer() -> None:
    _log, cluster, distribution, registry = setup(
        ledger_text=ledger([REPO], CLOSED_LONG_AGO),
        tags={REPO_PATH: {"build-0a1b2c3d": DIGEST_A}},
        pods=[pod("Succeeded", "e2e-build-a-1"), pod("Failed", "e2e-build-b-1")],
    )
    prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)
    assert distribution.deleted == [DIGEST_A]


def test_a_refused_job_delete_on_an_active_namespace_raises() -> None:
    log, cluster, _distribution, registry = setup(ledger_text=ledger([REPO], CLOSED_LONG_AGO))
    cluster.jobs_status = 403
    with pytest.raises(ClusterError) as excinfo:
        prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)
    assert not isinstance(excinfo.value, TeardownDeferred)
    assert registry_calls(log) == []


def test_a_job_delete_404_continues() -> None:
    _log, cluster, distribution, registry = setup(
        ledger_text=ledger([REPO], CLOSED_LONG_AGO),
        tags={REPO_PATH: {"build-0a1b2c3d": DIGEST_A}},
    )
    cluster.jobs_status = 404
    prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)
    assert distribution.deleted == [DIGEST_A]


# Images


def test_every_tag_digest_is_deleted_once_and_a_404_still_completes() -> None:
    log, cluster, distribution, registry = setup(
        ledger_text=ledger([REPO], CLOSED_LONG_AGO),
        tags={
            REPO_PATH: {
                "staging-0a1b2c3d": DIGEST_S,
                "build-0a1b2c3d": DIGEST_A,
                "staging-0a1b2c3e": DIGEST_S,
                "build-0a1b2c3e": DIGEST_B,
            }
        },
    )
    distribution.delete_status[DIGEST_A] = 404

    prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)

    assert sorted(distribution.deleted) == sorted([DIGEST_A, DIGEST_B, DIGEST_S])
    heads = [entry for entry in registry_calls(log) if entry[1] == "HEAD"]
    assert len(heads) == 4


def test_an_unknown_repository_is_done() -> None:
    log, cluster, distribution, registry = setup(ledger_text=ledger([REPO], CLOSED_LONG_AGO))
    prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)
    assert registry_calls(log) == [("registry", "GET", f"/v2/{REPO_PATH}/tags/list")]
    assert distribution.deleted == []


def test_every_ledger_repository_is_listed() -> None:
    worker = f"{PREFIX}/{NS}/worker"
    _log, cluster, distribution, registry = setup(
        ledger_text=ledger([REPO, worker], CLOSED_LONG_AGO),
        tags={
            REPO_PATH: {"build-0a1b2c3d": DIGEST_A},
            f"e2e/{NS}/worker": {"build-0a1b2c3e": DIGEST_B},
        },
    )
    prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)
    assert sorted(distribution.deleted) == sorted([DIGEST_A, DIGEST_B])


def test_a_registry_delete_refusal_propagates() -> None:
    _log, cluster, distribution, registry = setup(
        ledger_text=ledger([REPO], CLOSED_LONG_AGO),
        tags={REPO_PATH: {"build-0a1b2c3d": DIGEST_A}},
    )
    distribution.delete_status[DIGEST_A] = 405
    with pytest.raises(RegistryError, match="405"):
        prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)


# Ledger


def test_a_ledger_gone_on_the_reread_makes_no_registry_call() -> None:
    log, cluster, _distribution, registry = setup(
        ledger_text=ledger([REPO], CLOSED_LONG_AGO),
        tags={REPO_PATH: {"build-0a1b2c3d": DIGEST_A}},
    )
    cluster.ledger_get_script = [None, 404]
    prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)
    assert registry_calls(log) == []


def test_a_403_ledger_on_a_terminating_namespace_proceeds() -> None:
    log, cluster, _distribution, registry = setup(
        ledger_text=ledger([REPO]), tags={REPO_PATH: {"build-0a1b2c3d": DIGEST_A}}
    )
    # Namespace teardown removed the RoleBinding: every namespaced read is refused.
    cluster.ledger_status = 403
    cluster.jobs_status = 403
    cluster.pods_status = 403

    prepare_teardown(cluster, registry, NS, terminating=True, now=NOW)

    assert registry_calls(log) == []
    assert cluster.puts == [] and cluster.posts == []


def test_a_403_ledger_on_an_active_namespace_raises() -> None:
    log, cluster, _distribution, registry = setup(ledger_text=ledger([REPO], CLOSED_LONG_AGO))
    cluster.ledger_get_script = [None, 403]
    with pytest.raises(ClusterError) as excinfo:
        prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)
    assert not isinstance(excinfo.value, TeardownDeferred)
    assert registry_calls(log) == []


@pytest.mark.parametrize(
    "text",
    [
        "{not json",
        json.dumps(["not", "an", "object"]),
        json.dumps({"repositories": REPO, "closing_at": stamp(CLOSED_LONG_AGO)}),
        json.dumps({"repositories": [7], "closing_at": stamp(CLOSED_LONG_AGO)}),
    ],
    ids=["not-json", "not-object", "repositories-not-list", "entry-not-string"],
)
def test_a_malformed_ledger_raises(text: str) -> None:
    log, cluster, _distribution, registry = setup(ledger_text=text)
    with pytest.raises(ClusterError) as excinfo:
        prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)
    assert not isinstance(excinfo.value, TeardownDeferred)
    assert registry_calls(log) == []


@pytest.mark.parametrize(
    "entry",
    [
        pytest.param(f"{PREFIX}/{NS}/../../victim/app", id="traversal"),
        pytest.param(f"{PREFIX}/{NS}/%2e%2e", id="percent"),
        pytest.param(f"{PREFIX}/{NS}/app?x", id="query"),
        pytest.param(f"{PREFIX}/{NS}/app#x", id="fragment"),
        pytest.param(f"evil.example/e2e/{NS}/app", id="other-host"),
        pytest.param(f"{PREFIX}/curie-e2e-someone-else/app", id="other-namespace"),
        pytest.param(f"{PREFIX}/{NS}/app/extra", id="extra-level"),
    ],
)
def test_a_tampered_ledger_entry_is_refused_before_any_registry_call(entry: str) -> None:
    log, cluster, _distribution, registry = setup(
        ledger_text=ledger([REPO, entry], CLOSED_LONG_AGO),
        tags={REPO_PATH: {"build-0a1b2c3d": DIGEST_A}},
    )
    with pytest.raises(ClusterError, match="e2e image ledger is malformed"):
        prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)
    assert registry_calls(log) == []


def test_an_empty_ledger_makes_no_registry_call_even_unconfigured() -> None:
    log, cluster, _distribution, registry = setup(
        ledger_text=ledger([], CLOSED_LONG_AGO), prefix=""
    )
    prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)
    assert registry_calls(log) == []


def test_a_non_empty_ledger_with_no_registry_configured_raises() -> None:
    log, cluster, _distribution, registry = setup(
        ledger_text=ledger([REPO], CLOSED_LONG_AGO), prefix=""
    )
    with pytest.raises(RegistryError, match="registry is not configured"):
        prepare_teardown(cluster, registry, NS, terminating=False, now=NOW)
    assert registry_calls(log) == []
