"""Runner-logs proxy: namespace/pod scoping, no-cluster degradation, error mapping.

The log endpoint is scoped to the configured runner namespace + runner-pod label
selector (issue #64): an out-of-scope namespace or a pod that is not a genuine
runner pod is rejected with 403 *before* the pod-log reader is ever invoked. The
K8s access is injected (both the pod lister and the log reader), so the scoping
gate, success, and error paths are all exercised with fakes; the default app (no
cluster configured) proves the 503 degradation.
"""

import logging
from typing import Any

import pytest
from curie_api.deps import get_pod_lister, get_pod_log_reader
from curie_api.k8s import (
    LazyPodLogReader,
    NullPodLister,
    NullPodLogReader,
    PodLogError,
    PodLogReader,
    build_pod_log_reader,
)
from curie_api.main import create_app
from fastapi.testclient import TestClient

# runner-abc lives in the configured runner namespace ("curie"); the old URL
# used "preview-pr-1", which is now out of scope and would be a 403.
URL = "/observability/runners/curie/runner-abc/logs"


class FakeReader:
    def read(
        self,
        namespace: str,
        pod: str,
        container: str | None,
        tail_lines: int | None,
        previous: bool,
    ) -> str:
        return f"logs for {namespace}/{pod}"


class SpyReader:
    """Records every .read() call so tests can assert it is NEVER reached on 403.

    Optionally raises a configured error to exercise the 404/502 mappings once
    execution legitimately reaches the reader (a valid runner pod).
    """

    def __init__(self, *, raises: Exception | None = None) -> None:
        self.calls: list[tuple[str, str, str | None, int | None, bool]] = []
        self._raises = raises

    def read(
        self,
        namespace: str,
        pod: str,
        container: str | None,
        tail_lines: int | None,
        previous: bool,
    ) -> str:
        self.calls.append((namespace, pod, container, tail_lines, previous))
        if self._raises is not None:
            raise self._raises
        return f"logs for {namespace}/{pod}"


class FakeLister:
    """Returns a configurable set of runner-pod names; records list calls.

    Membership of the requested pod in this list is what the endpoint checks to
    decide whether the pod is a genuine runner pod.
    """

    def __init__(self, pods: list[str], *, raises: Exception | None = None) -> None:
        self._pods = pods
        self._raises = raises
        self.calls: list[tuple[str, str]] = []

    def list_runner_pods(self, namespace: str, label_selector: str) -> list[str]:
        self.calls.append((namespace, label_selector))
        if self._raises is not None:
            raise self._raises
        return list(self._pods)


def _client_with(reader: object, lister: object) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pod_log_reader] = lambda: reader
    app.dependency_overrides[get_pod_lister] = lambda: lister
    return TestClient(app)


def test_non_runner_namespace_returns_403(auth_headers: dict[str, str]) -> None:
    # A lister that WOULD return the requested pod, proving the namespace gate
    # short-circuits *before* any cluster call (no list, no read).
    reader = SpyReader()
    lister = FakeLister(["some-pod"])
    with _client_with(reader, lister) as client:
        resp = client.get(
            "/observability/runners/kube-system/some-pod/logs",
            headers=auth_headers,
        )
    assert resp.status_code == 403
    assert reader.calls == []  # the log reader must not run for an out-of-scope ns
    assert lister.calls == []  # the check is cheap: it precedes any cluster call


def test_pod_not_carrying_runner_selector_returns_403(
    auth_headers: dict[str, str],
) -> None:
    # In-scope namespace, but the pod is not among the genuine runner pods.
    reader = SpyReader()
    lister = FakeLister(["runner-xyz", "runner-def"])  # excludes runner-abc
    with _client_with(reader, lister) as client:
        resp = client.get(URL, headers=auth_headers)
    assert resp.status_code == 403
    assert reader.calls == []  # rejected before ever reading logs
    assert lister.calls  # membership was verified against the runner-pod list


def test_returns_logs_for_valid_runner_pod(auth_headers: dict[str, str]) -> None:
    # In-scope namespace and the pod IS a genuine runner pod -> logs are served.
    reader = SpyReader()
    lister = FakeLister(["runner-abc", "runner-old"])  # includes runner-abc
    with _client_with(reader, lister) as client:
        resp = client.get(URL, headers=auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["namespace"] == "curie"
    assert body["pod"] == "runner-abc"
    assert body["container"] is None
    assert body["logs"] == "logs for curie/runner-abc"
    assert lister.calls  # membership verified first
    assert reader.calls  # then the reader was invoked


def test_no_cluster_configured_degrades_to_503(
    auth_headers: dict[str, str],
) -> None:
    # With no cluster the lister raises NoClusterConfigured, which the endpoint
    # turns into a 503-with-reason rather than crashing; the reader never runs.
    reader = SpyReader()
    with _client_with(reader, NullPodLister()) as client:
        resp = client.get(URL, headers=auth_headers)
    assert resp.status_code == 503
    assert "no kubernetes cluster" in resp.json()["detail"]
    assert reader.calls == []


def test_pod_not_found_maps_to_404(auth_headers: dict[str, str]) -> None:
    # A valid runner pod whose logs the cluster reports as gone -> 404.
    reader = SpyReader(raises=PodLogError("pod not found", status=404))
    lister = FakeLister(["runner-abc"])
    with _client_with(reader, lister) as client:
        resp = client.get(URL, headers=auth_headers)
    assert resp.status_code == 404
    assert reader.calls  # execution reached the reader (pod was a valid runner)


def test_other_cluster_error_maps_to_502(auth_headers: dict[str, str]) -> None:
    # A valid runner pod but a non-404 cluster error -> 502.
    reader = SpyReader(raises=PodLogError("boom", status=500))
    lister = FakeLister(["runner-abc"])
    with _client_with(reader, lister) as client:
        resp = client.get(URL, headers=auth_headers)
    assert resp.status_code == 502
    assert reader.calls


def test_runner_logs_require_api_key(client: Any) -> None:
    assert client.get(URL).status_code == 401


# --- k8s.py internals (do not hit the endpoint) ---------------------------


def test_build_reader_degrades_to_null_when_config_unloadable() -> None:
    # An explicit but unloadable kubeconfig path degrades to the null reader.
    reader = build_pod_log_reader("/nonexistent/kubeconfig-path.yaml")
    assert isinstance(reader, NullPodLogReader)


def test_lazy_reader_defers_resolution_until_first_read() -> None:
    # The lazy reader must not build the real reader (which loads a kubeconfig and
    # resolves credentials) until the first read is actually served -- that is
    # what keeps app startup free of the exec-credential ERROR.
    calls = {"n": 0}

    def factory() -> PodLogReader:
        calls["n"] += 1
        return FakeReader()

    reader = LazyPodLogReader(factory)
    assert calls["n"] == 0  # constructing it resolves nothing

    first = reader.read("ns", "pod", None, None, False)
    assert first == "logs for ns/pod"
    assert calls["n"] == 1  # resolved on first read

    reader.read("ns", "pod2", None, None, False)
    assert calls["n"] == 1  # cached: not rebuilt per read


def test_build_reader_warns_not_errors_when_creds_absent(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # An unloadable/credential-less cluster config must degrade with a single WARN
    # from our code, never an ERROR (the scary boot log the audit flagged).
    with caplog.at_level(logging.WARNING):
        reader = build_pod_log_reader("/nonexistent/kubeconfig-path.yaml")
    assert isinstance(reader, NullPodLogReader)
    records = [r for r in caplog.records if r.name == "curie_api.k8s"]
    assert len(records) == 1
    assert records[0].levelno == logging.WARNING
    assert not any(r.levelno >= logging.ERROR for r in caplog.records)


def test_app_startup_emits_no_exec_credential_error(
    _disposable_db: Any, caplog: pytest.LogCaptureFixture
) -> None:
    # Booting the app must not eagerly resolve the cluster: on a host whose
    # kubeconfig auths via an exec plugin with expired creds, eager init logged
    # `exec: process returned ...` at ERROR at startup. With lazy init the
    # lifespan is clean; any exec-credential noise is deferred to a real pod-log
    # read (and even then suppressed in favor of a single WARN).
    with caplog.at_level(logging.DEBUG):
        with TestClient(create_app()):
            pass
    offenders = [
        r
        for r in caplog.records
        if r.levelno >= logging.ERROR and "exec: process returned" in r.getMessage()
    ]
    assert offenders == [], f"startup logged exec-credential error(s): {offenders}"
