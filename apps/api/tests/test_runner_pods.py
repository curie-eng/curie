"""Runner pod-list endpoint: no-cluster 503, success, and cluster-error 502.

The K8s access is injected, so the paths are exercised with fakes; the default
app (no cluster configured) proves the 503 degradation. Mirrors the runner-logs
proxy's error contract (503 no cluster, 502 other), minus the per-pod 404 that
listing has no notion of.
"""

from typing import Any

from curie_api.deps import get_pod_lister
from curie_api.k8s import NullPodLister, PodLogError
from curie_api.main import create_app
from fastapi.testclient import TestClient

URL = "/observability/runners"


class FakeLister:
    def list_runner_pods(self, namespace: str, label_selector: str) -> list[str]:
        # Echo the args back through the pod names so the test can assert them.
        return [f"{namespace}:{label_selector}:runner-a", "runner-b"]


class ClusterErrorLister:
    def list_runner_pods(self, namespace: str, label_selector: str) -> list[str]:
        raise PodLogError("connection refused", status=None)


def _client_with(lister: object) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pod_lister] = lambda: lister
    return TestClient(app)


def test_no_cluster_configured_degrades_to_503(auth_headers: dict[str, str]) -> None:
    with _client_with(NullPodLister()) as client:
        resp = client.get(URL, headers=auth_headers)
    assert resp.status_code == 503
    assert "no kubernetes cluster" in resp.json()["detail"]


def test_lists_runner_pods_with_the_default_namespace_and_selector(
    auth_headers: dict[str, str],
) -> None:
    with _client_with(FakeLister()) as client:
        resp = client.get(URL, headers=auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["namespace"] == "curie"  # settings default
    # The lister was called with the release namespace + runner-sandbox selector.
    assert body["pods"][0] == ("curie:app.kubernetes.io/component=runner-sandbox:runner-a")
    assert "runner-b" in body["pods"]


def test_namespace_override_is_passed_through(
    auth_headers: dict[str, str],
) -> None:
    with _client_with(FakeLister()) as client:
        resp = client.get(URL, params={"namespace": "preview-pr-7"}, headers=auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["namespace"] == "preview-pr-7"
    assert body["pods"][0].startswith("preview-pr-7:")


def test_cluster_error_maps_to_502(auth_headers: dict[str, str]) -> None:
    with _client_with(ClusterErrorLister()) as client:
        resp = client.get(URL, headers=auth_headers)
    assert resp.status_code == 502


def test_runner_pods_require_api_key(client: Any) -> None:
    assert client.get(URL).status_code == 401
