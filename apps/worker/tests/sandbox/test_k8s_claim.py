"""KubernetesSandboxClient.create_claim payload shape.

The agent-sandbox controller injects per-claim env with no ``containerName`` into
only the first main container. The bundle ref must ALSO be targeted at the init
containers by name, or a Kubernetes runner boots an empty plugin dir. These tests
assert the emitted SandboxClaim env, so the fix is mutation-honest: dropping the
named entries fails ``test_bundle_ref_targets_init_containers_by_name``.
"""

from __future__ import annotations

import copy
import json
import math
import os
import secrets
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from curie_worker.sandbox import QuotaRejection
from curie_worker.sandbox import k8s as k8s_module
from curie_worker.sandbox.k8s import (
    BUNDLE_INIT_CONTAINERS,
    WORKSPACE_INIT_CONTAINERS,
    KubernetesSandboxClient,
    _claim_view,
)
from curie_worker.sandbox.substrate import REAP_GRACE_MARGIN_SECONDS
from curie_worker.sandbox.types import KubeTransientError, SubstrateConfig
from kubernetes.client import ApiException
from urllib3.exceptions import MaxRetryError, ReadTimeoutError

# Captured live with `kubectl get sandboxclaims` in JSON form. The controller's
# direct Pod admission failure emitted no Warning quota Event, so this Ready
# condition is the machine readable cluster evidence for the rejection.
LIVE_QUOTA_REJECTED_CLAIM: dict[str, Any] = {
    "apiVersion": "extensions.agents.x-k8s.io/v1beta1",
    "kind": "SandboxClaim",
    "metadata": {
        "annotations": {
            "agents.x-k8s.io/controller-first-observed-at": "2026-08-19T10:24:42.828003465Z"
        },
        "creationTimestamp": "2026-08-19T10:24:42Z",
        "generation": 1,
        "name": "acme-claim",
        "namespace": "acme-quota",
        "resourceVersion": "12345",
        "uid": "00000000-0000-0000-0000-000000000000",
    },
    "status": {
        "conditions": [
            {
                "lastTransitionTime": "2026-08-19T10:24:42Z",
                "message": (
                    'Error seen: pods "acme-claim" is forbidden: exceeded quota: '
                    "acme-sandbox-quota, requested: limits.cpu=1, used: limits.cpu=0, "
                    "limited: limits.cpu=1m"
                ),
                "observedGeneration": 1,
                "reason": "ReconcilerError",
                "status": "False",
                "type": "Ready",
            }
        ],
        "sandbox": {"name": "acme-claim"},
    },
}

ISSUE_QUOTA_REJECTION_MESSAGE = (
    'Error seen: pods "curie-thread-example" is forbidden: exceeded quota: '
    "curie-sandbox-quota, requested: limits.cpu=1, used: limits.cpu=8, "
    "limited: limits.cpu=8"
)


_LEGACY_STUB_PLURALS = ("sandboxclaims", "sandboxes")


def _selector_matches(labels: dict[str, str], selector: str | None) -> bool:
    """Equality (``k=v``) and existence (``k``) terms, comma-joined, as kube does."""

    if not selector:
        return True
    for term in selector.split(","):
        key, sep, value = term.partition("=")
        if sep:
            if labels.get(key) != value:
                return False
        elif key not in labels:
            return False
    return True


def _merge_patch(target: dict[str, Any], patch: dict[str, Any]) -> None:
    """RFC 7386 JSON merge patch, the content type a dict body is sent as."""

    for key, value in patch.items():
        if value is None:
            target.pop(key, None)
        elif isinstance(value, dict) and isinstance(target.get(key), dict):
            _merge_patch(target[key], value)
        else:
            target[key] = copy.deepcopy(value)


def _json_patch(target: dict[str, Any], ops: list[dict[str, Any]]) -> None:
    """The add/replace subset of RFC 6902, the content type a list body is sent as."""

    for op in ops:
        assert op["op"] in {"add", "replace"}, op
        parts = [p.replace("~1", "/").replace("~0", "~") for p in op["path"].split("/")[1:]]
        node: Any = target
        for part in parts[:-1]:
            node = node.setdefault(part, {}) if isinstance(node, dict) else node[int(part)]
        last = parts[-1]
        if isinstance(node, list):
            if last == "-":
                node.append(copy.deepcopy(op["value"]))
            else:
                node.insert(int(last), copy.deepcopy(op["value"]))
        else:
            node[last] = copy.deepcopy(op["value"])


class _FakeApi:
    """CustomObjectsApi + CoreV1Api as the API server answers them.

    Created objects are stored and come back carrying ``metadata.uid`` and a
    ``creationTimestamp``; a read or delete of an absent object is a 404
    ``ApiException``; a duplicate create is a 409; and deleting an object
    removes every stored object whose ``ownerReferences`` name its uid,
    recursively, which is what the garbage collector does with background
    propagation. ``failures`` keyed by ``(verb, plural)`` injects an API error
    on that call (``plural`` is ``"secrets"`` for the core Secret create);
    ``delete_failures`` keyed by object name does the same for one delete.

    ``calls`` records ``(verb, plural, kwargs)`` for every write and read, so a
    test can check the transport bound each call carried. A read of an absent
    plural in ``stub_plurals`` answers a legacy stub instead of a 404; clear it
    for a test that needs the API server's real 404 on an absent claim.
    """

    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []
        self.objects: dict[tuple[str, str], dict[str, Any]] = {}
        self.secrets: dict[str, dict[str, Any]] = {}
        self.secret_namespaces: list[str] = []
        self.patches: list[tuple[str, str, Any]] = []
        self.deletes: list[tuple[str, str]] = []
        self.lists: list[tuple[str, str | None]] = []
        self.failures: dict[tuple[str, str], BaseException] = {}
        self.delete_failures: dict[str, BaseException] = {}
        self._uid = 0
        self.request_timeouts: list[tuple[str, float]] = []
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        self.stub_plurals: set[str] = set(_LEGACY_STUB_PLURALS)
        self.quota: object | None = None
        self.quota_error: BaseException | None = None
        self.pod: object | None = None
        self.pod_error: BaseException | None = None
        self.events: list[object] = []
        self.event_reads: list[tuple[str, str | None, int, float]] = []

    # -- seeding (what the chart rendered before the worker ran) ------------

    def _next_uid(self) -> str:
        self._uid += 1
        return f"00000000-0000-0000-0000-{self._uid:012d}"

    def seed(
        self,
        plural: str,
        name: str,
        spec: dict[str, Any],
        *,
        labels: dict[str, str] | None = None,
        created: str | None = "2026-10-01T00:00:00Z",
    ) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "name": name,
            "namespace": "test-ns",
            "uid": self._next_uid(),
        }
        if created is not None:
            metadata["creationTimestamp"] = created
        if labels is not None:
            metadata["labels"] = dict(labels)
        obj = {"metadata": metadata, "spec": copy.deepcopy(spec)}
        self.objects[(plural, name)] = obj
        return obj

    def _fail(self, verb: str, plural: str) -> None:
        error = self.failures.get((verb, plural))
        if error is not None:
            raise error

    # -- CustomObjectsApi ------------------------------------------------------

    def create_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        body: dict[str, Any],
        **kwargs: Any,
    ) -> dict[str, Any]:
        del group, version
        assert namespace == "test-ns"
        self.calls.append(("create", plural, dict(kwargs)))
        self._fail("create", plural)
        name = body["metadata"]["name"]
        if (plural, name) in self.objects:
            raise k8s_module.k8s_client.ApiException(status=409, reason="AlreadyExists")
        self.created.append(body)
        stored = copy.deepcopy(body)
        stored["metadata"]["namespace"] = namespace
        stored["metadata"]["uid"] = self._next_uid()
        stored["metadata"]["creationTimestamp"] = "2026-10-03T12:00:00Z"
        self.objects[(plural, name)] = stored
        return copy.deepcopy(stored)

    def get_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        name: str,
        *,
        _request_timeout: float | None = None,
    ) -> dict[str, Any]:
        del group, version, namespace
        self.calls.append(
            (
                "get",
                plural,
                {} if _request_timeout is None else {"_request_timeout": _request_timeout},
            )
        )
        if _request_timeout is not None:
            self.request_timeouts.append((f"get:{plural}:{name}", _request_timeout))
        self._fail("get", plural)
        stored = self.objects.get((plural, name))
        if stored is not None:
            return copy.deepcopy(stored)
        if plural == "sandboxclaims" and plural in self.stub_plurals:
            return {"metadata": {"name": name}}
        if plural == "sandboxes" and plural in self.stub_plurals:
            return {
                "metadata": {"name": name},
                "spec": {"operatingMode": "Running"},
                "status": {},
            }
        raise k8s_module.k8s_client.ApiException(status=404, reason="NotFound")

    def patch_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        name: str,
        body: Any,
        **kwargs: Any,
    ) -> dict[str, Any]:
        del group, version, namespace
        self.calls.append(("patch", plural, dict(kwargs)))
        self._fail("patch", plural)
        stored = self.objects.get((plural, name))
        if stored is None:
            raise k8s_module.k8s_client.ApiException(status=404, reason="NotFound")
        self.patches.append((plural, name, copy.deepcopy(body)))
        if isinstance(body, list):
            _json_patch(stored, body)
        else:
            _merge_patch(stored, body)
        return copy.deepcopy(stored)

    def delete_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        name: str,
        *,
        _request_timeout: float | None = None,
        **kwargs: Any,
    ) -> object:
        del group, version, namespace
        self.calls.append(
            (
                "delete",
                plural,
                {
                    **kwargs,
                    **({} if _request_timeout is None else {"_request_timeout": _request_timeout}),
                },
            )
        )
        if _request_timeout is not None:
            self.request_timeouts.append((f"delete:{plural}:{name}", _request_timeout))
        self.deletes.append((plural, name))
        self._fail("delete", plural)
        error = self.delete_failures.get(name)
        if error is not None:
            raise error
        stored = self.objects.pop((plural, name), None)
        if stored is None:
            raise k8s_module.k8s_client.ApiException(status=404, reason="NotFound")
        self._collect(stored["metadata"]["uid"])
        return {"kind": "Status", "status": "Success"}

    def list_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        *,
        label_selector: str | None = None,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        del group, version, namespace
        self.lists.append((plural, label_selector))
        self._fail("list", plural)
        items = [
            copy.deepcopy(obj)
            for (kind, _name), obj in self.objects.items()
            if kind == plural
            and _selector_matches(obj["metadata"].get("labels") or {}, label_selector)
        ]
        return {"items": items}

    def _collect(self, owner_uid: str) -> None:
        """Garbage-collect every dependent of ``owner_uid``, recursively."""

        def owned(obj: dict[str, Any]) -> bool:
            refs = obj.get("metadata", {}).get("ownerReferences") or []
            return any(ref.get("uid") == owner_uid for ref in refs)

        for key, obj in list(self.objects.items()):
            if key in self.objects and owned(obj):
                del self.objects[key]
                self._collect(obj["metadata"]["uid"])
        for name, secret in list(self.secrets.items()):
            if owned(secret):
                del self.secrets[name]

    # -- CoreV1Api ---------------------------------------------------------------

    def create_namespaced_secret(self, namespace: str, body: Any, **kwargs: Any) -> Any:
        self.calls.append(("create", "secrets", dict(kwargs)))
        self._fail("create", "secrets")
        self.secret_namespaces.append(namespace)
        # A dict or a V1Secret model, normalized to the wire JSON the API
        # server would store (camelCase, ``stringData``).
        wire = k8s_module.k8s_client.ApiClient().sanitize_for_serialization(body)
        name = wire["metadata"]["name"]
        if name in self.secrets:
            raise k8s_module.k8s_client.ApiException(status=409, reason="AlreadyExists")
        wire["metadata"]["uid"] = self._next_uid()
        self.secrets[name] = wire
        return k8s_module.k8s_client.V1Secret(
            metadata=k8s_module.k8s_client.V1ObjectMeta(
                name=name, namespace=namespace, uid=wire["metadata"]["uid"]
            ),
            type=wire.get("type"),
        )

    def read_namespaced_resource_quota(
        self,
        name: str,
        namespace: str,
        *,
        _request_timeout: float,
    ) -> object:
        self.request_timeouts.append((f"get:resourcequotas:{namespace}:{name}", _request_timeout))
        if self.quota_error is not None:
            raise self.quota_error
        assert self.quota is not None
        return self.quota

    def read_namespaced_pod(
        self,
        name: str,
        namespace: str,
        *,
        _request_timeout: float,
    ) -> object:
        self.request_timeouts.append((f"get:pods:{namespace}:{name}", _request_timeout))
        if self.pod_error is not None:
            raise self.pod_error
        return self.pod

    def list_namespaced_event(
        self,
        namespace: str,
        *,
        field_selector: str | None = None,
        limit: int,
        _request_timeout: float,
    ) -> SimpleNamespace:
        self.event_reads.append((namespace, field_selector, limit, _request_timeout))
        return SimpleNamespace(items=self.events)


def _client(api: _FakeApi) -> KubernetesSandboxClient:
    client = KubernetesSandboxClient.__new__(KubernetesSandboxClient)
    client._api = api  # type: ignore[attr-defined]
    client._core_api = api  # type: ignore[attr-defined]
    client._namespace = "test-ns"  # type: ignore[attr-defined]
    return client


class _LogApi(_FakeApi):
    """The CoreV1Api boundary, with one response per attempted log read."""

    def __init__(self, responses: list[object], *, elapsed: list[float] | None = None) -> None:
        super().__init__()
        self.responses = responses
        self.elapsed = elapsed or [0.0] * len(responses)
        self.now = 100.0
        self.log_reads: list[dict[str, object]] = []

    def read_namespaced_pod_log(
        self,
        name: str,
        namespace: str,
        *,
        container: str,
        previous: bool,
        tail_lines: int,
        limit_bytes: int,
        _preload_content: bool,
        _request_timeout: float,
    ) -> object:
        # Kubernetes documents previous as the previous terminated container's
        # log and bounds by both lines and bytes. The generated Python API
        # exposes the urllib3 response when _preload_content is false:
        # https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#read-log
        # https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/CoreV1Api.md#read_namespaced_pod_log
        self.log_reads.append(
            {
                "name": name,
                "namespace": namespace,
                "container": container,
                "previous": previous,
                "tail_lines": tail_lines,
                "limit_bytes": limit_bytes,
                "preload_content": _preload_content,
                "request_timeout": _request_timeout,
            }
        )
        self.now += self.elapsed.pop(0)
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        (
            SimpleNamespace(data=b"runner boot\ntraceback: \xff\n"),
            "runner boot\ntraceback: \ufffd\n",
        ),
        (b"plain bytes\n", "plain bytes\n"),
        ("plain text\n", "plain text\n"),
    ],
    ids=["http-response-invalid-utf8", "raw-bytes", "text"],
)
def test_pod_log_tail_reads_previous_runner_output_with_bounds(
    response: object, expected: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    api = _LogApi([response])
    monkeypatch.setattr(k8s_module.time, "monotonic", lambda: api.now)

    assert _client(api).pod_log_tail("runner-pod", request_timeout_seconds=5.0) == expected
    assert api.log_reads == [
        {
            "name": "runner-pod",
            "namespace": "test-ns",
            "container": "runner",
            "previous": True,
            "tail_lines": 200,
            "limit_bytes": 8192,
            "preload_content": False,
            "request_timeout": 5.0,
        }
    ]


def test_pod_log_tail_falls_back_to_current_only_when_previous_returns_400(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = _LogApi(
        [k8s_module.k8s_client.ApiException(status=400), SimpleNamespace(data=b"current\n")],
        elapsed=[2.0, 0.0],
    )
    monkeypatch.setattr(k8s_module.time, "monotonic", lambda: api.now)

    assert _client(api).pod_log_tail("runner-pod", request_timeout_seconds=5.0) == "current\n"
    assert [read["previous"] for read in api.log_reads] == [True, False]
    assert [read["request_timeout"] for read in api.log_reads] == [5.0, 3.0]
    assert all(
        read["name"] == "runner-pod"
        and read["namespace"] == "test-ns"
        and read["container"] == "runner"
        and read["tail_lines"] == 200
        and read["limit_bytes"] == 8192
        and read["preload_content"] is False
        for read in api.log_reads
    )


@pytest.mark.parametrize(
    "error",
    [
        k8s_module.k8s_client.ApiException(status=403),
        k8s_module.k8s_client.ApiException(status=404),
        k8s_module.k8s_client.ApiException(status=500),
        TimeoutError("log read timed out"),
        OSError("log transport failed"),
    ],
    ids=["forbidden", "missing", "server-error", "timeout", "transport"],
)
def test_pod_log_tail_errors_return_none_without_retry(error: BaseException) -> None:
    api = _LogApi([error])

    assert _client(api).pod_log_tail("runner-pod", request_timeout_seconds=5.0) is None
    assert len(api.log_reads) == 1
    assert api.log_reads[0]["previous"] is True


@pytest.mark.parametrize(
    "error",
    [k8s_module.k8s_client.ApiException(status=400), TimeoutError("current log timed out")],
    ids=["current-400", "current-timeout"],
)
def test_pod_log_tail_current_failure_returns_none_and_never_retries_again(
    error: BaseException,
) -> None:
    api = _LogApi([k8s_module.k8s_client.ApiException(status=400), error])

    assert _client(api).pod_log_tail("runner-pod", request_timeout_seconds=5.0) is None
    assert [read["previous"] for read in api.log_reads] == [True, False]


def test_pod_log_tail_exhausted_previous_read_does_not_start_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = _LogApi([k8s_module.k8s_client.ApiException(status=400)], elapsed=[5.1])
    monkeypatch.setattr(k8s_module.time, "monotonic", lambda: api.now)

    assert _client(api).pod_log_tail("runner-pod", request_timeout_seconds=5.0) is None
    assert len(api.log_reads) == 1


def _resource_quota(
    *,
    name: str = "curie-sandbox-quota",
    namespace: str = "test-ns",
    spec_hard: dict[str, str],
    status_hard: dict[str, str] | None = None,
    status_used: dict[str, str],
) -> SimpleNamespace:
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, namespace=namespace),
        spec=SimpleNamespace(hard=spec_hard),
        status=SimpleNamespace(
            hard=spec_hard if status_hard is None else status_hard,
            used=status_used,
        ),
    )


def _claim_body(api: _FakeApi) -> dict[str, Any]:
    """The one SandboxClaim body the client sent (per-claim objects come first)."""

    claims = [body for body in api.created if body.get("kind") == "SandboxClaim"]
    assert len(claims) == 1, [body.get("kind") for body in api.created]
    return claims[0]


def _env_entries(api: _FakeApi) -> list[dict[str, str]]:
    return _claim_body(api)["spec"]["env"]


# DRIVER observation from installed kubernetes 36.0.3 against an actual local
# stalled HTTP endpoint: get_claim, get_sandbox, and delete_claim each
# requested 0.15 seconds, raised MaxRetryError after 0.151 seconds, and made
# exactly one HTTP request. RESTClientObject.request maps scalar
# _request_timeout to urllib3.Timeout(total=...), while RESTClientObject.__init__
# forwards Configuration.retries when set.
def test_pressure_reads_and_delete_forward_the_required_transport_bounds() -> None:
    api = _FakeApi()
    client = _client(api)

    assert client.get_claim("claim-bound", request_timeout_seconds=0.75) is not None
    assert client.get_sandbox("sandbox-bound", request_timeout_seconds=0.5) is not None
    client.delete_claim("claim-bound", request_timeout_seconds=0.25)

    assert api.request_timeouts == [
        ("get:sandboxclaims:claim-bound", 0.75),
        ("get:sandboxes:sandbox-bound", 0.5),
        ("delete:sandboxclaims:claim-bound", 0.25),
    ]


def test_kubernetes_client_disables_sdk_transport_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(k8s_module.k8s_config, "load_incluster_config", lambda: None)

    client = KubernetesSandboxClient("test-ns")

    assert client._api.api_client.configuration.retries == 0  # noqa: SLF001
    assert client._core_api.api_client is client._api.api_client  # noqa: SLF001


@pytest.mark.parametrize(
    ("rejection", "spec_hard", "status_used"),
    [
        (
            QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"limits.cpu": "1"},
                used={"limits.cpu": "8"},
                hard={"limits.cpu": "8"},
            ),
            {"limits.cpu": "8000m"},
            {"limits.cpu": "7"},
        ),
        (
            QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"limits.memory": "512Mi"},
                used={"limits.memory": "1Gi"},
                hard={"limits.memory": "1Gi"},
            ),
            {"limits.memory": "1024Mi"},
            {"limits.memory": "512Mi"},
        ),
        (
            QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            ),
            {"pods": "2"},
            {"pods": "1"},
        ),
        (
            QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={
                    "limits.cpu": "500m",
                    "limits.memory": "512Mi",
                    "pods": "1",
                },
                used={
                    "limits.cpu": "1",
                    "limits.memory": "1Gi",
                    "pods": "2",
                },
                hard={
                    "limits.cpu": "1",
                    "limits.memory": "1Gi",
                    "pods": "2",
                },
            ),
            {
                "limits.cpu": "1000m",
                "limits.memory": "1Gi",
                "pods": "2",
                "requests.cpu": "4",
            },
            {
                "limits.cpu": "500m",
                "limits.memory": "512Mi",
                "pods": "1",
                "requests.cpu": "3",
            },
        ),
    ],
    ids=["cpu", "memory", "pods", "combined"],
)
def test_quota_headroom_reads_exact_quota_and_requires_every_resource(
    rejection: QuotaRejection,
    spec_hard: dict[str, str],
    status_used: dict[str, str],
) -> None:
    # Kubernetes defines status hard as the enforced limits and status used as
    # current observed namespace usage. See the ResourceQuota v1 API reference:
    # https://kubernetes.io/docs/reference/kubernetes-api/core/resource-quota-v1/
    api = _FakeApi()
    api.quota = _resource_quota(spec_hard=spec_hard, status_used=status_used)
    client = _client(api)

    assert client.quota_has_headroom(rejection, request_timeout_seconds=0.75)
    assert api.request_timeouts == [("get:resourcequotas:test-ns:curie-sandbox-quota", 0.75)]


@pytest.mark.parametrize(
    "rejection",
    [
        QuotaRejection(
            quota_name="INVALID_NAME",
            requested={"pods": "1"},
            used={"pods": "2"},
            hard={"pods": "2"},
        ),
        QuotaRejection(quota_name="curie-sandbox-quota", requested={}, used={}, hard={}),
        QuotaRejection(
            quota_name="curie-sandbox-quota",
            requested={"pods": "1"},
            used={"limits.cpu": "2"},
            hard={"pods": "2"},
        ),
        QuotaRejection(
            quota_name="curie-sandbox-quota",
            requested={"pods": "NaN"},
            used={"pods": "2"},
            hard={"pods": "2"},
        ),
        QuotaRejection(
            quota_name="curie-sandbox-quota",
            requested={"pods": "Infinity"},
            used={"pods": "2"},
            hard={"pods": "2"},
        ),
        QuotaRejection(
            quota_name="curie-sandbox-quota",
            requested={"pods": "sNaNm"},
            used={"pods": "2"},
            hard={"pods": "2"},
        ),
        QuotaRejection(
            quota_name="curie-sandbox-quota",
            requested={"pods": "1e999999999k"},
            used={"pods": "2"},
            hard={"pods": "2"},
        ),
        QuotaRejection(
            quota_name="curie-sandbox-quota",
            requested={"pods": "1_0"},
            used={"pods": "20"},
            hard={"pods": "20"},
        ),
        QuotaRejection(
            quota_name="curie-sandbox-quota",
            requested={"pods": " 1"},
            used={"pods": "2"},
            hard={"pods": "2"},
        ),
        QuotaRejection(
            quota_name="curie-sandbox-quota",
            requested={"pods": "0"},
            used={"pods": "2"},
            hard={"pods": "2"},
        ),
        QuotaRejection(
            quota_name="curie-sandbox-quota",
            requested={"pods": "1"},
            used={"pods": "-1"},
            hard={"pods": "2"},
        ),
        QuotaRejection(
            quota_name="curie-sandbox-quota",
            requested={"pods": "1"},
            used={"pods": "1"},
            hard={"pods": "-2"},
        ),
        QuotaRejection(
            quota_name="curie-sandbox-quota",
            requested={"pods": "1"},
            used={"pods": "1"},
            hard={"pods": "2"},
        ),
        QuotaRejection(
            quota_name="curie-sandbox-quota",
            requested={"limits.cpu": "0.1"},
            used={"limits.cpu": "1." + "1" * 128},
            hard={"limits.cpu": "1"},
        ),
    ],
    ids=[
        "quota_name",
        "empty",
        "unequal_keys",
        "nan",
        "infinity",
        "signaling_nan_suffix",
        "overflow",
        "underscore",
        "whitespace",
        "zero_request",
        "negative_used",
        "negative_hard",
        "not_over_quota",
        "rounded_arithmetic",
    ],
)
def test_invalid_quota_rejection_fails_before_core_api_read(
    rejection: QuotaRejection,
) -> None:
    api = _FakeApi()

    assert not _client(api).quota_has_headroom(
        rejection,
        request_timeout_seconds=1.0,
    )
    assert api.request_timeouts == []


def test_combined_quota_headroom_fails_when_one_resource_remains_full() -> None:
    rejection = QuotaRejection(
        quota_name="curie-sandbox-quota",
        requested={"limits.cpu": "500m", "limits.memory": "512Mi"},
        used={"limits.cpu": "1", "limits.memory": "1Gi"},
        hard={"limits.cpu": "1", "limits.memory": "1Gi"},
    )
    api = _FakeApi()
    api.quota = _resource_quota(
        spec_hard={"limits.cpu": "1", "limits.memory": "1Gi"},
        status_used={"limits.cpu": "1", "limits.memory": "512Mi"},
    )

    assert not _client(api).quota_has_headroom(
        rejection,
        request_timeout_seconds=1.0,
    )


@pytest.mark.parametrize(
    "quota",
    [
        _resource_quota(
            name="another-quota",
            spec_hard={"pods": "2"},
            status_used={"pods": "1"},
        ),
        _resource_quota(
            namespace="another-ns",
            spec_hard={"pods": "2"},
            status_used={"pods": "1"},
        ),
        _resource_quota(spec_hard={}, status_used={"pods": "1"}),
        _resource_quota(spec_hard={"pods": "2"}, status_hard={}, status_used={"pods": "1"}),
        _resource_quota(spec_hard={"pods": "2"}, status_used={}),
        _resource_quota(
            spec_hard={"pods": "2"},
            status_hard={"pods": "3"},
            status_used={"pods": "1"},
        ),
        _resource_quota(spec_hard={"pods": "NaN"}, status_used={"pods": "1"}),
        _resource_quota(spec_hard={"pods": "2"}, status_used={"pods": "-1"}),
    ],
    ids=[
        "name",
        "namespace",
        "spec_missing",
        "status_hard_missing",
        "used_missing",
        "stale_hard",
        "malformed",
        "negative_used",
    ],
)
def test_unknown_live_quota_state_fails_closed(quota: object) -> None:
    api = _FakeApi()
    api.quota = quota
    rejection = QuotaRejection(
        quota_name="curie-sandbox-quota",
        requested={"pods": "1"},
        used={"pods": "2"},
        hard={"pods": "2"},
    )

    assert not _client(api).quota_has_headroom(
        rejection,
        request_timeout_seconds=1.0,
    )


@pytest.mark.parametrize(
    "error",
    [
        k8s_module.k8s_client.ApiException(status=403),
        k8s_module.k8s_client.ApiException(status=404),
        TimeoutError("quota read timed out"),
        OSError("quota transport failed"),
    ],
    ids=["forbidden", "missing", "timeout", "transport"],
)
def test_quota_read_errors_fail_closed(error: BaseException) -> None:
    api = _FakeApi()
    api.quota_error = error
    rejection = QuotaRejection(
        quota_name="curie-sandbox-quota",
        requested={"pods": "1"},
        used={"pods": "2"},
        hard={"pods": "2"},
    )

    assert not _client(api).quota_has_headroom(
        rejection,
        request_timeout_seconds=1.0,
    )


def test_bundle_ref_targets_init_containers_by_name() -> None:
    api = _FakeApi()
    _client(api).create_claim(
        "claim-1",
        pool="pool",
        env={"CURIE_BUNDLE_REF": "bundles/x.tar.gz", "CURIE_BUDGET": "{}"},
    )
    entries = _env_entries(api)

    # The main runner still receives the ref (unnamed entry).
    assert {"name": "CURIE_BUNDLE_REF", "value": "bundles/x.tar.gz"} in entries

    # And each bundle init container receives it by explicit containerName.
    named = {(e["containerName"], e["name"]): e["value"] for e in entries if "containerName" in e}
    for container in BUNDLE_INIT_CONTAINERS:
        assert named[(container, "CURIE_BUNDLE_REF")] == "bundles/x.tar.gz"


def test_bundle_version_reaches_the_runner_not_the_init_containers() -> None:
    """#2174: the agent-readable version is runner env, not an object-store key.

    Init containers still receive only CURIE_BUNDLE_REF (the fetch key). The
    version_label is an unnamed main-container entry so the sandboxed agent
    can read it, matching the docker substrate's forward of the same key.
    """

    api = _FakeApi()
    _client(api).create_claim(
        "claim-1",
        pool="pool",
        env={
            "CURIE_BUNDLE_REF": "bundles/x.tar.gz",
            "CURIE_BUNDLE_VERSION": "abc123def456",
            "CURIE_BUDGET": "{}",
        },
    )
    entries = _env_entries(api)

    assert {"name": "CURIE_BUNDLE_VERSION", "value": "abc123def456"} in entries
    named = {(e["containerName"], e["name"]): e["value"] for e in entries if "containerName" in e}
    assert all(key[1] != "CURIE_BUNDLE_VERSION" for key in named)


def test_no_named_env_without_bundle_ref() -> None:
    api = _FakeApi()
    _client(api).create_claim(
        "claim-1", pool="pool", env={"CURIE_BUDGET": "{}", "CURIE_SESSION_ID": "s"}
    )
    entries = _env_entries(api)
    assert entries  # the main-container env is still present
    assert all("containerName" not in e for e in entries)


def test_workspace_capability_targets_only_workspace_init_containers() -> None:
    api = _FakeApi()
    workspace_ref = "opaque-presigned-workspace-reference"
    workspace_sha256 = "a" * 64
    _client(api).create_claim(
        "claim-workspace",
        pool="pool",
        env={
            "CURIE_BUDGET": "{}",
            "CURIE_WORKSPACE_REF": workspace_ref,
            "CURIE_WORKSPACE_SHA256": workspace_sha256,
        },
    )
    entries = _env_entries(api)

    unnamed = {entry["name"] for entry in entries if "containerName" not in entry}
    assert "CURIE_WORKSPACE_REF" not in unnamed
    assert "CURIE_WORKSPACE_SHA256" not in unnamed
    named = {
        (entry["containerName"], entry["name"]): entry["value"]
        for entry in entries
        if "containerName" in entry
    }
    for container in WORKSPACE_INIT_CONTAINERS:
        assert named[(container, "CURIE_WORKSPACE_REF")] == workspace_ref
        assert named[(container, "CURIE_WORKSPACE_SHA256")] == workspace_sha256


def test_credential_is_never_written_to_the_claim() -> None:
    # The SandboxClaim env is value-only, so the secret must not be persisted on
    # the claim; the template's secretKeyRef supplies it to the runner instead.
    api = _FakeApi()
    _client(api).create_claim(
        "claim-1",
        pool="pool",
        env={"CURIE_BUDGET": "{}", "CURIE_CREDENTIALS": "super-secret-token"},
    )
    entries = _env_entries(api)
    assert all(e.get("name") != "CURIE_CREDENTIALS" for e in entries)
    assert all("super-secret-token" not in e.get("value", "") for e in entries)
    # The rest of the boot env is still written.
    assert {"name": "CURIE_BUDGET", "value": "{}"} in entries


def test_host_credentials_are_never_written_to_the_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    denied_names = {
        "POSTGRES_PASSWORD",
        "DATABASE_URL",
        "VALKEY_PASSWORD",
        "SLACK_BOT_TOKEN",
        "S3_ACCESS_KEY",
        "S3_SECRET_KEY",
        "CURIE_API_KEY",
        "LANGFUSE_SECRET_KEY",
        "CURIE_ADAPTER_CREDENTIALS",
        "CURIE_SEALING_PRIVATE_KEY",
        "CURIE_SEALING_PREVIOUS_PRIVATE_KEY",
        "CURIE_CONNECTOR_CALLER_SIGNING_KEY",
    }
    for name in denied_names:
        monkeypatch.setenv(name, "placeholder")
    monkeypatch.setenv("CURIE_BUDGET", "{}")
    monkeypatch.setenv("CURIE_CREDENTIALS", "placeholder")
    monkeypatch.delenv("CURIE_CONNECTOR_SECRET_KEYS", raising=False)

    api = _FakeApi()
    _client(api).create_claim("claim-credentials", pool="pool", env=os.environ)

    claim_env_names = {entry["name"] for entry in _env_entries(api)}
    assert denied_names.isdisjoint(claim_env_names)
    assert "CURIE_BUDGET" in claim_env_names
    assert "CURIE_CREDENTIALS" not in claim_env_names


def test_the_caller_token_rides_the_claim_secret_and_its_signing_key_never_does() -> None:
    # ADR-0168 decision 7 made the caller token the sandbox's own short-lived
    # identity; #3842 moves it off the value-only claim, where any principal
    # with `get sandboxclaims` could read and replay it, into the per-claim
    # Secret the runner reads by secretKeyRef. The key that signs it is the
    # worker's, and would let a sandbox mint a token naming any agent, so it
    # reaches neither object.
    api = _FakeApi()
    _seed_chart(api, pool="pool", template="curie-runner")
    claim = _claim_name()
    _client(api).create_claim(
        claim,
        pool="pool",
        env={
            "CURIE_BUDGET": "{}",
            "CURIE_CONNECTOR_CALLER_TOKEN": "cct.payload.signature",
            "CURIE_CONNECTOR_CALLER_SIGNING_KEY": "placeholder-signing-key",
        },
    )
    entries = _env_entries(api)
    assert all(e.get("name") != "CURIE_CONNECTOR_CALLER_TOKEN" for e in entries)
    assert "cct." not in json.dumps(_claim_body(api))
    secret = _token_secret(api, claim)
    assert secret["stringData"] == {"CURIE_CONNECTOR_CALLER_TOKEN": "cct.payload.signature"}
    assert all(e.get("name") != "CURIE_CONNECTOR_CALLER_SIGNING_KEY" for e in entries)
    everything = json.dumps([api.created, list(api.secrets.values())])
    assert "CURIE_CONNECTOR_CALLER_SIGNING_KEY" not in everything
    assert "placeholder-signing-key" not in everything


def test_no_slack_identity_token_reaches_the_claim() -> None:
    """The k8s counterpart of
    `test_create_claim_excludes_every_slack_identity_token_from_child_env`:
    the same `filter_agent_child_env` backs both substrates, and this pins
    the k8s claim's call site against the indexed Slack token prefixes."""
    api = _FakeApi()
    _client(api).create_claim(
        "claim-slack-identities",
        pool="pool",
        env={
            "CURIE_SLACK_BOT_TOKEN__0": "placeholder",
            "CURIE_SLACK_BOT_TOKEN__1": "placeholder",
            "CURIE_SLACK_APP_TOKEN__0": "placeholder",
            "CURIE_SLACK_SIGNING_SECRET__0": "placeholder",
            "CURIE_SLACK_IDENTITIES": "[]",
        },
    )

    claim_env_names = {entry["name"] for entry in _env_entries(api)}
    assert claim_env_names.isdisjoint(
        {
            "CURIE_SLACK_BOT_TOKEN__0",
            "CURIE_SLACK_BOT_TOKEN__1",
            "CURIE_SLACK_APP_TOKEN__0",
            "CURIE_SLACK_SIGNING_SECRET__0",
        }
    )
    assert "CURIE_SLACK_IDENTITIES" in claim_env_names


def test_runner_token_rides_the_claim_secret_and_the_credential_is_excluded() -> None:
    # The per-sandbox runner token used to ride the claim in plain text only
    # because there was no secretKeyRef path for it. The per-claim Secret is
    # that path (#3842, H4): the token leaves the claim and reaches the runner
    # by reference, while the model credential stays excluded from both.
    api = _FakeApi()
    _seed_chart(api, pool="pool", template="curie-runner")
    claim = _claim_name()
    _client(api).create_claim(
        claim,
        pool="pool",
        env={
            "CURIE_BUDGET": "{}",
            "CURIE_RUNNER_TOKEN": "tok-26",
            "CURIE_CREDENTIALS": "super-secret-token",
        },
    )
    entries = _env_entries(api)
    assert all(e.get("name") != "CURIE_RUNNER_TOKEN" for e in entries)
    assert "tok-26" not in json.dumps(_claim_body(api))
    assert {"name": "CURIE_BUDGET", "value": "{}"} in entries
    secret = _token_secret(api, claim)
    assert secret["stringData"] == {"CURIE_RUNNER_TOKEN": "tok-26"}
    assert all(e.get("name") != "CURIE_CREDENTIALS" for e in entries)
    assert "super-secret-token" not in json.dumps([api.created, list(api.secrets.values())])


def test_claim_view_surfaces_the_creation_timestamp() -> None:
    # The reaper's bind-window grace is only as good as the age the real
    # cluster adapter reports. An adapter that never surfaced the timestamp
    # would leave every claim at unknown age, which spares every orphan and
    # disables reaping on the tier that actually runs in production -- and the
    # substrate's own tests, which drive an in-memory fake, would not see it.
    view = _claim_view(
        {"metadata": {"name": "claim-1", "creationTimestamp": "2026-08-16T12:00:00Z"}}
    )
    created = view.created_at
    assert created is not None
    assert created.utcoffset() == timedelta(0)  # tz-aware UTC, never naive
    assert created == datetime(2026, 8, 16, 12, 0, tzinfo=UTC)

    # A zoneless instant is read AS UTC, never as host-local: interpreting it
    # in the host's zone shifts the claim's age by that offset, and on a
    # west-of-UTC host that makes a young claim look old enough to reap.
    naive = _claim_view(
        {"metadata": {"name": "claim-3", "creationTimestamp": "2026-08-16T12:00:00"}}
    )
    assert naive.created_at == datetime(2026, 8, 16, 12, 0, tzinfo=UTC)

    # A claim the cluster gave no creation instant for reads as unknown age.
    absent = _claim_view({"metadata": {"name": "claim-2"}})
    assert absent.created_at is None

    # And an unparseable one, so one malformed object cannot raise inside the
    # maintenance tick and silently end reaping.
    malformed = _claim_view(
        {"metadata": {"name": "claim-4", "creationTimestamp": "not-a-timestamp"}}
    )
    assert malformed.created_at is None


def test_claim_view_classifies_live_resource_quota_condition() -> None:
    view = _claim_view(copy.deepcopy(LIVE_QUOTA_REJECTED_CLAIM))

    assert view.name == "acme-claim"
    assert view.ready is False
    assert view.sandbox_name == "acme-claim"
    assert view.created_at == datetime(2026, 8, 19, 10, 24, 42, tzinfo=UTC)
    assert view.quota_rejection == QuotaRejection(
        quota_name="acme-sandbox-quota",
        requested={"limits.cpu": "1"},
        used={"limits.cpu": "0"},
        hard={"limits.cpu": "1m"},
    )
    assert view.ready_reason == "ReconcilerError"
    assert view.ready_message == LIVE_QUOTA_REJECTED_CLAIM["status"]["conditions"][0]["message"]


def test_claim_view_classifies_issue_example_at_eight_of_eight() -> None:
    claim = copy.deepcopy(LIVE_QUOTA_REJECTED_CLAIM)
    claim["status"]["conditions"][0]["message"] = ISSUE_QUOTA_REJECTION_MESSAGE

    view = _claim_view(claim)

    assert view.quota_rejection == QuotaRejection(
        quota_name="curie-sandbox-quota",
        requested={"limits.cpu": "1"},
        used={"limits.cpu": "8"},
        hard={"limits.cpu": "8"},
    )
    assert view.ready_reason == "ReconcilerError"
    assert view.ready_message == ISSUE_QUOTA_REJECTION_MESSAGE


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("status", "True", id="status-true"),
        pytest.param("type", "Provisioned", id="type-provisioned"),
        pytest.param("reason", "ProvisioningFailed", id="another-reason"),
        pytest.param(
            "message",
            'Error seen: pods "curie-thread-example" is forbidden: User "system:serviceaccount:'
            'curie1572:worker" cannot create resource "pods"',
            id="reconciler-error-without-exceeded-quota-clause",
        ),
    ],
)
def test_quota_message_requires_failed_ready_condition(field: str, value: str) -> None:
    claim = copy.deepcopy(LIVE_QUOTA_REJECTED_CLAIM)
    claim["status"]["conditions"][0][field] = value

    assert _claim_view(claim).quota_rejection is None


@pytest.mark.parametrize(
    "message",
    [
        (
            'Error seen: pods "curie-thread-example" is forbidden: exceeded quota: '
            "curie-sandbox-quota, requested: limits.cpu=1, used: limits.cpu=8"
        ),
        (
            'Error seen: pods "curie-thread-example" is forbidden: exceeded quota: '
            "curie-sandbox-quota, requested: limits.cpu, used: limits.cpu=8, "
            "limited: limits.cpu=8"
        ),
    ],
    ids=["missing_map", "malformed_map"],
)
def test_incomplete_quota_maps_are_not_classified(message: str) -> None:
    claim = copy.deepcopy(LIVE_QUOTA_REJECTED_CLAIM)
    claim["status"]["conditions"][0]["message"] = message

    assert _claim_view(claim).quota_rejection is None


def test_quota_parser_preserves_every_resource_map_entry() -> None:
    # Kubernetes admission reports complete requested, used, and hard maps for
    # every exceeded resource. The upstream controller is the primary source:
    # https://github.com/kubernetes/kubernetes/blob/v1.32.6/staging/src/k8s.io/apiserver/pkg/admission/plugin/resourcequota/controller.go
    claim = copy.deepcopy(LIVE_QUOTA_REJECTED_CLAIM)
    claim["status"]["conditions"][0]["message"] = (
        'Error seen: pods "curie-thread-example" is forbidden: exceeded quota: '
        "curie-sandbox-quota, requested: requests.memory=1Gi,limits.cpu=1, "
        "used: limits.cpu=8,requests.memory=2Gi, limited: requests.memory=4Gi,limits.cpu=8"
    )

    assert _claim_view(claim).quota_rejection == QuotaRejection(
        quota_name="curie-sandbox-quota",
        requested={"requests.memory": "1Gi", "limits.cpu": "1"},
        used={"limits.cpu": "8", "requests.memory": "2Gi"},
        hard={"requests.memory": "4Gi", "limits.cpu": "8"},
    )


def test_quota_parser_preserves_unequal_resource_maps_for_guarding() -> None:
    claim = copy.deepcopy(LIVE_QUOTA_REJECTED_CLAIM)
    claim["status"]["conditions"][0]["message"] = (
        'Error seen: pods "curie-thread-example" is forbidden: exceeded quota: '
        "curie-sandbox-quota, requested: requests.cpu=1, used: limits.cpu=8, "
        "limited: limits.cpu=8"
    )

    assert _claim_view(claim).quota_rejection == QuotaRejection(
        quota_name="curie-sandbox-quota",
        requested={"requests.cpu": "1"},
        used={"limits.cpu": "8"},
        hard={"limits.cpu": "8"},
    )


def test_connector_secrets_are_never_written_to_the_claim() -> None:
    # Per-agent connector secrets (#429) ride the substrate-agnostic boot env by
    # value, but the value-only claim CR would persist them in plaintext in etcd.
    # The binding marks their keys in CURIE_CONNECTOR_SECRET_KEYS; the substrate
    # strips both the marker and every key it names (cluster delivery is #1488).
    api = _FakeApi()
    _client(api).create_claim(
        "claim-1",
        pool="pool",
        env={
            "CURIE_BUDGET": "{}",
            "GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_super_secret",
            "API_KEY": "k-secret",
            "CURIE_CONNECTOR_SECRET_KEYS": "API_KEY,GITHUB_PERSONAL_ACCESS_TOKEN",
        },
    )
    entries = _env_entries(api)
    # Neither the secret values nor the marker land on the claim.
    for leaked in ("ghp_super_secret", "k-secret"):
        assert all(leaked not in e.get("value", "") for e in entries)
    names = {e.get("name") for e in entries}
    assert "GITHUB_PERSONAL_ACCESS_TOKEN" not in names
    assert "API_KEY" not in names
    assert "CURIE_CONNECTOR_SECRET_KEYS" not in names
    # Non-secret boot env is still written.
    assert {"name": "CURIE_BUDGET", "value": "{}"} in entries
    body = _claim_body(api)
    assert "additionalPodMetadata" not in body["spec"]
    assert body["spec"]["warmPoolRef"]["name"] == "pool"


def test_claim_metadata_agent_label_is_not_additional_pod_metadata() -> None:
    # The adopted controller rejects spec.additionalPodMetadata.labels under
    # curietech.ai (Ready=False reason=InvalidMetadata). Claim object labels
    # are ordinary Kubernetes metadata and are the rotation selector.
    api = _FakeApi()
    _client(api).create_claim(
        "claim-1",
        pool="curie-agent-acme-a-runner-pool",
        labels={"curietech.ai/agent": "acme-a"},
        env={"CURIE_BUDGET": "{}"},
    )
    body = _claim_body(api)
    assert body["metadata"]["labels"]["curietech.ai/agent"] == "acme-a"
    assert "additionalPodMetadata" not in body["spec"]
    assert body["spec"]["warmPoolRef"]["name"] == "curie-agent-acme-a-runner-pool"


def _pod(*conditions: SimpleNamespace) -> SimpleNamespace:
    return SimpleNamespace(status=SimpleNamespace(conditions=list(conditions)))


def _condition(type_: str, status: str, reason: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        type=type_,
        status=status,
        reason=reason,
        message="0/1 nodes are available: 1 Insufficient cpu.",
    )


def test_unschedulable_pod_reports_the_scheduler_message() -> None:
    """#3169: PodScheduled=False with reason Unschedulable is the no-room signal."""

    api = _FakeApi()
    api.pod = _pod(_condition("PodScheduled", "False", "Unschedulable"))

    assert (
        _client(api).pod_unschedulable("sbx-1", request_timeout_seconds=0.5)
        == "0/1 nodes are available: 1 Insufficient cpu."
    )
    assert api.request_timeouts == [("get:pods:test-ns:sbx-1", 0.5)]


@pytest.mark.parametrize(
    "pod",
    [
        _pod(_condition("PodScheduled", "True")),
        _pod(_condition("PodScheduled", "False", "SchedulerError")),
        _pod(_condition("Ready", "False", "Unschedulable")),
        _pod(),
        SimpleNamespace(status=None),
        None,
    ],
    ids=["scheduled", "other_reason", "other_type", "no_conditions", "no_status", "none"],
)
def test_a_scheduled_or_unknown_pod_is_not_unschedulable(pod: object) -> None:
    api = _FakeApi()
    api.pod = pod

    assert _client(api).pod_unschedulable("sbx-1", request_timeout_seconds=0.5) is None


@pytest.mark.parametrize(
    "error",
    [
        k8s_module.k8s_client.ApiException(status=404),
        k8s_module.k8s_client.ApiException(status=403),
        TimeoutError("pod read timed out"),
    ],
    ids=["missing", "forbidden", "timeout"],
)
def test_an_unreadable_pod_is_not_unschedulable(error: BaseException) -> None:
    """Unknown pod state keeps today's claim-timeout failure, never a defer."""

    api = _FakeApi()
    api.pod_error = error

    assert _client(api).pod_unschedulable("sbx-1", request_timeout_seconds=0.5) is None


def test_evicted_pod_reports_its_status_reason_and_message() -> None:
    api = _FakeApi()
    message = (
        'Usage of EmptyDir volume "workspace" exceeds the limit "1Gi". '
        "token=exampleSecretValue123456 " + "x" * 400
    )
    api.pod = SimpleNamespace(
        metadata=SimpleNamespace(name="sbx-1", uid="pod-current"),
        status=SimpleNamespace(
            phase="Failed",
            reason="Evicted",
            message=message,
            container_statuses=[],
        ),
    )

    termination = _client(api).pod_termination(
        "sbx-1", request_timeout_seconds=0.5, since=datetime.now(UTC)
    )

    assert termination is not None
    assert termination.reason == "Evicted"
    assert termination.detail is not None
    assert 'EmptyDir volume "workspace" exceeds the limit "1Gi"' in termination.detail
    assert "exampleSecretValue123456" not in termination.detail
    assert "token=" not in termination.detail
    assert len(termination.detail) <= 256
    assert len(api.request_timeouts) == 1
    assert api.request_timeouts[0][0] == "get:pods:test-ns:sbx-1"
    assert 0 < api.request_timeouts[0][1] <= 0.5


def test_evicted_pod_does_not_publish_unstructured_status_message() -> None:
    api = _FakeApi()
    api.pod = SimpleNamespace(
        metadata=SimpleNamespace(name="sbx-1", uid="pod-current"),
        status=SimpleNamespace(
            phase="Failed",
            reason="Evicted",
            message="The key is exampleSecretValue123456; token = anotherSecretValue123456",
            container_statuses=[],
        ),
    )

    termination = _client(api).pod_termination(
        "sbx-1", request_timeout_seconds=0.5, since=datetime.now(UTC)
    )

    assert termination is not None
    assert termination.reason == "Evicted"
    assert termination.detail is None


def test_failed_pod_reports_other_termination_reason() -> None:
    api = _FakeApi()
    api.pod = SimpleNamespace(
        metadata=SimpleNamespace(name="sbx-1", uid="pod-current"),
        status=SimpleNamespace(
            phase="Failed", reason="NodeLost", message=None, container_statuses=[]
        ),
    )

    termination = _client(api).pod_termination(
        "sbx-1", request_timeout_seconds=0.5, since=datetime.now(UTC)
    )

    assert termination is not None
    assert termination.reason == "NodeLost"


def test_oom_killed_runner_reports_container_termination() -> None:
    api = _FakeApi()
    api.pod = SimpleNamespace(
        metadata=SimpleNamespace(name="sbx-1", uid="pod-current"),
        status=SimpleNamespace(
            phase="Running",
            reason=None,
            message=None,
            container_statuses=[
                SimpleNamespace(
                    name="runner",
                    state=SimpleNamespace(
                        terminated=SimpleNamespace(
                            reason="OOMKilled", message="Memory limit exceeded", exit_code=137
                        )
                    ),
                    last_state=None,
                )
            ],
        ),
    )

    termination = _client(api).pod_termination(
        "sbx-1", request_timeout_seconds=0.5, since=datetime.now(UTC)
    )

    assert termination is not None
    assert termination.reason == "OOMKilled"
    assert termination.detail is not None
    assert termination.detail == "exit code 137"
    assert len(api.request_timeouts) == 1
    assert api.request_timeouts[0][0] == "get:pods:test-ns:sbx-1"
    assert 0 < api.request_timeouts[0][1] <= 0.5


def test_pod_event_fallback_uses_only_the_current_pod_uid() -> None:
    api = _FakeApi()
    api.pod = SimpleNamespace(
        metadata=SimpleNamespace(name="sbx-1", uid="pod-current"),
        status=SimpleNamespace(phase="Failed", reason=None, message=None, container_statuses=[]),
    )
    api.events = [
        SimpleNamespace(
            type="Warning",
            reason="Evicted",
            message="An older pod was evicted.",
            involved_object=SimpleNamespace(kind="Pod", name="sbx-1", uid="pod-previous"),
            last_timestamp=datetime.now(UTC),
        ),
        SimpleNamespace(
            type="Warning",
            reason="OOMKilling",
            message="The current pod was killed for memory pressure.",
            involved_object=SimpleNamespace(kind="Pod", name="sbx-1", uid="pod-current"),
            last_timestamp=datetime.now(UTC),
        ),
    ]

    termination = _client(api).pod_termination(
        "sbx-1", request_timeout_seconds=0.5, since=datetime.now(UTC) - timedelta(seconds=5)
    )

    assert termination is not None
    assert termination.reason == "OOMKilling"
    assert termination.detail is None
    assert len(api.event_reads) == 1
    assert api.event_reads[0][0] == "test-ns"
    assert api.event_reads[0][1] == "involvedObject.kind=Pod,involvedObject.name=sbx-1"
    assert api.event_reads[0][2] == 20
    assert 0 < api.event_reads[0][3] <= 0.5


def test_stale_pod_event_cannot_supply_a_failed_pods_cause() -> None:
    api = _FakeApi()
    api.pod = SimpleNamespace(
        metadata=SimpleNamespace(name="sbx-1", uid="pod-current"),
        status=SimpleNamespace(phase="Failed", reason=None, message=None, container_statuses=[]),
    )
    api.events = [
        SimpleNamespace(
            type="Warning",
            reason="Evicted",
            involved_object=SimpleNamespace(kind="Pod", name="sbx-1", uid="pod-previous"),
            last_timestamp=datetime.now(UTC),
        )
    ]

    termination = _client(api).pod_termination(
        "sbx-1", request_timeout_seconds=0.5, since=datetime.now(UTC) - timedelta(seconds=5)
    )

    assert termination is not None
    assert termination.reason == "Failed"


def test_running_pod_without_termination_or_matching_event_has_no_cause() -> None:
    api = _FakeApi()
    since = datetime.now(UTC) - timedelta(seconds=5)
    api.pod = SimpleNamespace(
        metadata=SimpleNamespace(name="sbx-1", uid="pod-current"),
        status=SimpleNamespace(phase="Running", reason=None, message=None, container_statuses=[]),
    )
    api.events = [
        SimpleNamespace(
            type="Warning",
            reason="Evicted",
            message="A stale eviction event must not override a running pod.",
            involved_object=SimpleNamespace(kind="Pod", name="sbx-1", uid="pod-current"),
            last_timestamp=since - timedelta(seconds=1),
        )
    ]

    assert _client(api).pod_termination("sbx-1", request_timeout_seconds=0.5, since=since) is None


def test_recent_eviction_event_explains_a_still_running_pod() -> None:
    api = _FakeApi()
    api.pod = SimpleNamespace(
        metadata=SimpleNamespace(name="sbx-1", uid="pod-current"),
        status=SimpleNamespace(phase="Running", reason=None, container_statuses=[]),
    )
    api.events = [
        SimpleNamespace(
            reason="Evicted",
            involved_object=SimpleNamespace(kind="Pod", name="sbx-1", uid="pod-current"),
            last_timestamp=datetime.now(UTC),
        )
    ]

    termination = _client(api).pod_termination(
        "sbx-1", request_timeout_seconds=0.5, since=datetime.now(UTC) - timedelta(seconds=5)
    )
    assert termination is not None
    assert termination.reason == "Evicted"


def test_oom_event_alone_does_not_identify_runner_in_running_pod() -> None:
    api = _FakeApi()
    api.pod = SimpleNamespace(
        metadata=SimpleNamespace(name="sbx-1", uid="pod-current"),
        status=SimpleNamespace(phase="Running", reason=None, container_statuses=[]),
    )
    api.events = [
        SimpleNamespace(
            reason="OOMKilling",
            involved_object=SimpleNamespace(kind="Pod", name="sbx-1", uid="pod-current"),
            last_timestamp=datetime.now(UTC),
        )
    ]

    assert (
        _client(api).pod_termination(
            "sbx-1", request_timeout_seconds=0.5, since=datetime.now(UTC) - timedelta(seconds=5)
        )
        is None
    )


def test_terminated_sidecar_does_not_hide_runner_state() -> None:
    api = _FakeApi()
    api.pod = SimpleNamespace(
        metadata=SimpleNamespace(name="sbx-1", uid="pod-current"),
        status=SimpleNamespace(
            phase="Running",
            reason=None,
            container_statuses=[
                SimpleNamespace(
                    name="helper",
                    state=SimpleNamespace(terminated=SimpleNamespace(reason="Error", exit_code=1)),
                    last_state=None,
                ),
                SimpleNamespace(
                    name="runner",
                    state=SimpleNamespace(
                        terminated=SimpleNamespace(reason="OOMKilled", exit_code=137)
                    ),
                    last_state=None,
                ),
            ],
        ),
    )

    termination = _client(api).pod_termination(
        "sbx-1", request_timeout_seconds=0.5, since=datetime.now(UTC)
    )
    assert termination is not None
    assert termination.reason == "OOMKilled"


@pytest.mark.parametrize("recent", [True, False])
def test_only_recent_oom_last_state_explains_a_running_pod(recent: bool) -> None:
    api = _FakeApi()
    since = datetime.now(UTC) - timedelta(seconds=5)
    finished_at = since + timedelta(seconds=1 if recent else -1)
    api.pod = SimpleNamespace(
        metadata=SimpleNamespace(name="sbx-1", uid="pod-current"),
        status=SimpleNamespace(
            phase="Running",
            reason=None,
            message=None,
            container_statuses=[
                SimpleNamespace(
                    name="runner",
                    state=SimpleNamespace(terminated=None),
                    last_state=SimpleNamespace(
                        terminated=SimpleNamespace(
                            reason="OOMKilled", exit_code=137, finished_at=finished_at
                        )
                    ),
                )
            ],
        ),
    )

    termination = _client(api).pod_termination("sbx-1", request_timeout_seconds=0.5, since=since)

    if recent:
        assert termination is not None
        assert termination.reason == "OOMKilled"
        assert termination.detail == "exit code 137"
    else:
        assert termination is None


# ---------------------------------------------------------------------------
# #3842: claim-scoped tokens ride a per-claim Secret, never the claim (AC1).
#
# The label, suffixes and ownerReference shape below are spelled as literals on
# purpose: they are the contract with the chart's admission policies (the
# cleanup and worker-secrets ValidatingAdmissionPolicies key on them) and with
# the Kubernetes garbage collector, not internal names of the module under test.
# ---------------------------------------------------------------------------

_EXT_API_VERSION = "extensions.agents.x-k8s.io/v1beta1"
_MANAGED_BY = ("curietech.ai/managed-by", "curie-sandbox-substrate")
_CLAIM_LABEL = "curietech.ai/sandbox-claim"
_CONNECTOR_SECRET_REF = {
    "name": "GITHUB_PERSONAL_ACCESS_TOKEN",
    "valueFrom": {
        "secretKeyRef": {
            "name": "curie-agent-acme-a-connector-secrets",
            "key": "GITHUB_PERSONAL_ACCESS_TOKEN",
            "optional": False,
        }
    },
}
_OVERRIDE: dict[str, Any] = {
    "requests": {"cpu": "500m", "memory": "1Gi", "ephemeral-storage": "1Gi"},
    "limits": {"cpu": "1", "memory": "2Gi", "ephemeral-storage": "4Gi"},
}


def _claim_name(nonce: str = "abc123") -> str:
    """A claim name exactly as the substrate mints one (30 characters)."""

    return SubstrateConfig(namespace="test-ns", warm_pool="pool").claim_name_for("T1", nonce)


def _chart_template_spec(*, agent: bool = True) -> dict[str, Any]:
    """The runner SandboxTemplate spec as charts/curie/templates/agent-sandbox.yaml renders it."""

    resources = {
        "requests": {"cpu": "50m", "memory": "192Mi", "ephemeral-storage": "512Mi"},
        "limits": {"cpu": "1", "memory": "768Mi", "ephemeral-storage": "4Gi"},
    }
    runner_env: list[dict[str, Any]] = []
    if agent:
        runner_env.append(copy.deepcopy(_CONNECTOR_SECRET_REF))
    runner_env += [
        {
            "name": "CURIE_CREDENTIALS",
            "valueFrom": {"secretKeyRef": {"name": "curie", "key": "agentCredentials"}},
        },
        {"name": "CURIE_PLUGIN_DIR", "value": "/bundles/plugin"},
        {"name": "CURIE_SESSION_ID", "value": "warm-unbound"},
        {
            "name": "CURIE_SANDBOX_ID",
            "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}},
        },
    ]
    return {
        "service": True,
        "envVarsInjectionPolicy": "Overrides",
        "networkPolicyManagement": "Unmanaged",
        "podTemplate": {
            "metadata": {"labels": {"app.kubernetes.io/component": "runner"}},
            "spec": {
                "automountServiceAccountToken": False,
                "serviceAccountName": "curie-runner",
                "runtimeClassName": "gvisor",
                "volumes": [{"name": "bundles", "emptyDir": {"sizeLimit": "2Gi"}}],
                "containers": [
                    {
                        "name": "runner",
                        "image": "ghcr.io/curie-eng/curie-runner:dev",
                        "env": runner_env,
                        "resources": copy.deepcopy(resources),
                        "volumeMounts": [{"name": "bundles", "mountPath": "/bundles"}],
                    }
                ],
                "initContainers": [
                    {
                        "name": "bundle-fetch",
                        "image": "amazon/aws-cli:2",
                        "resources": copy.deepcopy(resources),
                    },
                    {
                        "name": "workspace-init",
                        "image": "ghcr.io/curie-eng/curie-runner:dev",
                        "resources": copy.deepcopy(resources),
                    },
                ],
            },
        },
    }


def _seed_chart(
    api: _FakeApi,
    *,
    pool: str = "curie-agent-acme-a-runner-pool",
    template: str = "curie-agent-acme-a-runner",
    spec: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Store the chart-rendered warm pool and the template its sandboxTemplateRef names."""

    template_spec = spec if spec is not None else _chart_template_spec()
    api.seed(
        "sandboxwarmpools",
        pool,
        {"replicas": 1, "sandboxTemplateRef": {"name": template}},
        labels={"app.kubernetes.io/managed-by": "Helm"},
    )
    api.seed(
        "sandboxtemplates",
        template,
        template_spec,
        labels={"app.kubernetes.io/managed-by": "Helm"},
    )
    return template_spec


def _token_secret(api: _FakeApi, claim: str) -> dict[str, Any]:
    secret = api.secrets.get(f"{claim}-tokens")
    assert secret is not None, sorted(api.secrets)
    return secret


def _runner_container(spec: dict[str, Any]) -> dict[str, Any]:
    containers = spec["podTemplate"]["spec"]["containers"]
    return next(c for c in containers if c["name"] == "runner")


def _scoped_token_env() -> dict[str, str]:
    return {
        "CURIE_RUNNER_TOKEN": secrets.token_urlsafe(32),
        "CURIE_HISTORY_TOKEN": "sbx.history.payload.sig",
        "CURIE_MEMORY_TOKEN": "sbx.memory.payload.sig",
        "CURIE_STATE_TOKEN": "sbx.state.payload.sig",
        "CURIE_PROGRESS_TOKEN": "sbx.progress.payload.sig",
        "CURIE_ISSUE_READ_TOKEN": "wir.payload.sig",
        "CURIE_CONNECTOR_CALLER_TOKEN": "cct.payload.signature",
    }


def _chart_objects_untouched(api: _FakeApi, pristine: dict[str, Any]) -> None:
    pool = api.objects[("sandboxwarmpools", "curie-agent-acme-a-runner-pool")]
    assert pool["spec"] == {
        "replicas": 1,
        "sandboxTemplateRef": {"name": "curie-agent-acme-a-runner"},
    }
    assert api.objects[("sandboxtemplates", "curie-agent-acme-a-runner")]["spec"] == pristine
    assert not any(
        plural == "sandboxtemplates" and name == "curie-agent-acme-a-runner"
        for plural, name, _ in api.patches
    )


def test_no_scoped_token_value_reaches_the_claim() -> None:
    """AC1: `kubectl get sandboxclaim -o yaml` shows no sbx. or cct. values."""

    api = _FakeApi()
    _seed_chart(api)
    claim = _claim_name()
    tokens = _scoped_token_env()

    _client(api).create_claim(
        claim,
        pool="curie-agent-acme-a-runner-pool",
        labels={"curietech.ai/agent": "acme-a"},
        agent_name="acme-a",
        env={
            "CURIE_BUDGET": "{}",
            "CURIE_SESSION_ID": "s-1",
            "CURIE_BUNDLE_REF": "bundles/x.tar.gz",
            **tokens,
        },
    )

    # Both what the worker sent and what the API server now holds.
    for claim_json in (
        json.dumps(_claim_body(api)),
        json.dumps(api.objects[("sandboxclaims", claim)]),
    ):
        for prefix in ("sbx.", "cct.", "wir."):
            assert prefix not in claim_json, prefix
        assert tokens["CURIE_RUNNER_TOKEN"] not in claim_json
        for key in tokens:
            assert key not in claim_json, key
    # The rest of the boot env still reaches the runner through the claim.
    entries = _env_entries(api)
    assert {"name": "CURIE_BUDGET", "value": "{}"} in entries
    assert {"name": "CURIE_SESSION_ID", "value": "s-1"} in entries
    # Liveness: the tokens were delivered, not dropped. Every one is in the
    # per-claim Secret, and the runner reads each from it by reference.
    assert _token_secret(api, claim)["stringData"] == tokens
    template = api.objects[("sandboxtemplates", f"{claim}-resources")]
    runner_env = _runner_container(template["spec"])["env"]
    for key in tokens:
        assert {
            "name": key,
            "valueFrom": {
                "secretKeyRef": {"name": f"{claim}-tokens", "key": key, "optional": False}
            },
        } in runner_env
    assert tokens["CURIE_RUNNER_TOKEN"] not in json.dumps(template)


def test_tokens_ride_a_secret_owned_by_the_claim_template() -> None:
    api = _FakeApi()
    _seed_chart(api)
    claim = _claim_name()
    tokens = {"CURIE_RUNNER_TOKEN": "f3b2c1d0", "CURIE_STATE_TOKEN": "sbx.state.sig"}

    _client(api).create_claim(
        claim,
        pool="curie-agent-acme-a-runner-pool",
        labels={"curietech.ai/agent": "acme-a"},
        env={"CURIE_BUDGET": "{}", **tokens},
    )

    template = api.objects[("sandboxtemplates", f"{claim}-resources")]
    template_uid = template["metadata"]["uid"]
    claim_obj = api.objects[("sandboxclaims", claim)]

    secret = _token_secret(api, claim)
    assert api.secret_namespaces == ["test-ns"]
    assert secret.get("type", "Opaque") == "Opaque"
    assert secret["stringData"] == tokens
    assert "data" not in secret or not secret["data"]
    labels = secret["metadata"]["labels"]
    assert labels[_MANAGED_BY[0]] == _MANAGED_BY[1]
    assert labels[_CLAIM_LABEL] == claim
    (secret_owner,) = secret["metadata"]["ownerReferences"]
    assert {k: secret_owner[k] for k in ("apiVersion", "kind", "name", "uid")} == {
        "apiVersion": _EXT_API_VERSION,
        "kind": "SandboxTemplate",
        "name": f"{claim}-resources",
        "uid": template_uid,
    }
    # No controller / blockOwnerDeletion: those need `finalizers` RBAC the
    # worker is not granted.
    assert not secret_owner.get("controller")
    assert not secret_owner.get("blockOwnerDeletion")

    pool = api.objects[("sandboxwarmpools", f"{claim}-resources-pool")]
    assert pool["spec"] == {"replicas": 0, "sandboxTemplateRef": {"name": f"{claim}-resources"}}
    assert pool["metadata"]["labels"][_CLAIM_LABEL] == claim
    assert pool["metadata"]["labels"][_MANAGED_BY[0]] == _MANAGED_BY[1]
    (pool_owner,) = pool["metadata"]["ownerReferences"]
    assert (pool_owner["kind"], pool_owner["name"], pool_owner["uid"]) == (
        "SandboxTemplate",
        f"{claim}-resources",
        template_uid,
    )

    assert claim_obj["spec"]["warmPoolRef"] == {"name": f"{claim}-resources-pool"}
    assert claim_obj["metadata"]["labels"]["curietech.ai/agent"] == "acme-a"

    assert template["metadata"]["labels"][_CLAIM_LABEL] == claim
    assert template["metadata"]["labels"][_MANAGED_BY[0]] == _MANAGED_BY[1]
    (template_owner,) = template["metadata"]["ownerReferences"]
    assert {k: template_owner[k] for k in ("apiVersion", "kind", "name", "uid")} == {
        "apiVersion": _EXT_API_VERSION,
        "kind": "SandboxClaim",
        "name": claim,
        "uid": claim_obj["metadata"]["uid"],
    }

    # The outcome the ownerReferences exist for: deleting the claim, by any
    # path, garbage-collects the template, the Secret and the pool.
    _client(api).delete_claim(claim, request_timeout_seconds=1.0)
    assert ("sandboxtemplates", f"{claim}-resources") not in api.objects
    assert ("sandboxwarmpools", f"{claim}-resources-pool") not in api.objects
    assert f"{claim}-tokens" not in api.secrets
    # And the chart's own objects are not dependents of anything the worker made.
    assert ("sandboxtemplates", "curie-agent-acme-a-runner") in api.objects
    assert ("sandboxwarmpools", "curie-agent-acme-a-runner-pool") in api.objects


def test_per_claim_template_copies_the_pool_source_template() -> None:
    # The source is the template the pool's sandboxTemplateRef names, not the
    # pool name minus "-pool". A decoy at that guessed name lacks the per-agent
    # connector secret, so reading it would silently drop the agent's
    # connector credentials (#1488).
    api = _FakeApi()
    source = _seed_chart(api, template="curie-agent-acme-a-runner-v7")
    api.seed("sandboxtemplates", "curie-agent-acme-a-runner", _chart_template_spec(agent=False))
    pristine = copy.deepcopy(source)
    claim = _claim_name()

    _client(api).create_claim(
        claim,
        pool="curie-agent-acme-a-runner-pool",
        env={"CURIE_BUDGET": "{}", "CURIE_RUNNER_TOKEN": "f3b2c1d0"},
    )

    template = api.objects[("sandboxtemplates", f"{claim}-resources")]
    copied = template["spec"]
    runner_env = _runner_container(copied)["env"]
    # #1488 regression guard: the per-agent connector secretKeyRef survives.
    assert _CONNECTOR_SECRET_REF in runner_env
    # Every pre-existing entry survives verbatim and in order.
    assert [e for e in runner_env if e["name"] != "CURIE_RUNNER_TOKEN"] == _runner_container(
        pristine
    )["env"]
    # Every other field of the spec is a copy of the source.
    without_env = copy.deepcopy(copied)
    _runner_container(without_env).pop("env")
    expected = copy.deepcopy(pristine)
    _runner_container(expected).pop("env")
    assert without_env == expected
    assert template["kind"] == "SandboxTemplate"
    assert template["apiVersion"] == _EXT_API_VERSION
    # The shared chart template was read, never written.
    assert api.objects[("sandboxtemplates", "curie-agent-acme-a-runner-v7")]["spec"] == pristine


def test_claim_create_failure_deletes_the_claim_template() -> None:
    api = _FakeApi()
    pristine = copy.deepcopy(_seed_chart(api))
    api.failures[("create", "sandboxclaims")] = k8s_module.k8s_client.ApiException(
        status=500, reason="InternalError"
    )
    claim = _claim_name()

    with pytest.raises(k8s_module.k8s_client.ApiException):
        _client(api).create_claim(
            claim,
            pool="curie-agent-acme-a-runner-pool",
            env={"CURIE_BUDGET": "{}", "CURIE_STATE_TOKEN": "sbx.state.sig"},
        )

    assert ("sandboxtemplates", f"{claim}-resources") in api.deletes
    assert ("sandboxtemplates", f"{claim}-resources") not in api.objects
    assert ("sandboxwarmpools", f"{claim}-resources-pool") not in api.objects
    assert f"{claim}-tokens" not in api.secrets, "the token Secret must not outlive the failure"
    assert ("sandboxclaims", claim) not in api.objects
    _chart_objects_untouched(api, pristine)


def test_secret_create_failure_deletes_the_claim_template_and_creates_no_claim() -> None:
    api = _FakeApi()
    pristine = copy.deepcopy(_seed_chart(api))
    api.failures[("create", "secrets")] = k8s_module.k8s_client.ApiException(
        status=403, reason="Forbidden"
    )
    claim = _claim_name()

    with pytest.raises(k8s_module.k8s_client.ApiException):
        _client(api).create_claim(
            claim,
            pool="curie-agent-acme-a-runner-pool",
            env={"CURIE_BUDGET": "{}", "CURIE_STATE_TOKEN": "sbx.state.sig"},
        )

    assert ("sandboxtemplates", f"{claim}-resources") not in api.objects
    assert ("sandboxwarmpools", f"{claim}-resources-pool") not in api.objects
    assert not [b for b in api.created if b.get("kind") == "SandboxClaim"]
    _chart_objects_untouched(api, pristine)


def test_owner_patch_failure_deletes_claim_and_template() -> None:
    api = _FakeApi()
    pristine = copy.deepcopy(_seed_chart(api))
    api.failures[("patch", "sandboxtemplates")] = k8s_module.k8s_client.ApiException(
        status=422, reason="Invalid"
    )
    claim = _claim_name()

    with pytest.raises(k8s_module.k8s_client.ApiException):
        _client(api).create_claim(
            claim,
            pool="curie-agent-acme-a-runner-pool",
            env={"CURIE_BUDGET": "{}", "CURIE_STATE_TOKEN": "sbx.state.sig"},
        )

    # The claim existed, so it is deleted, not left to boot a sandbox whose
    # template no longer exists or, worse, one nothing will ever clean up.
    assert ("sandboxclaims", claim) in api.deletes
    assert ("sandboxclaims", claim) not in api.objects
    assert ("sandboxtemplates", f"{claim}-resources") in api.deletes
    assert ("sandboxtemplates", f"{claim}-resources") not in api.objects
    assert ("sandboxwarmpools", f"{claim}-resources-pool") not in api.objects
    assert f"{claim}-tokens" not in api.secrets
    _chart_objects_untouched(api, pristine)


def _assert_no_writes(api: _FakeApi) -> None:
    assert api.created == []
    assert api.secrets == {}
    assert api.patches == []


def test_a_source_template_without_a_runner_container_is_refused_before_any_write() -> None:
    api = _FakeApi()
    spec = _chart_template_spec()
    _runner_container(spec)["name"] = "agent"
    _seed_chart(api, spec=spec)

    with pytest.raises(ValueError, match="runner container"):
        _client(api).create_claim(
            _claim_name(),
            pool="curie-agent-acme-a-runner-pool",
            env={"CURIE_BUDGET": "{}", "CURIE_RUNNER_TOKEN": "f3b2c1d0"},
        )
    _assert_no_writes(api)


def test_a_missing_source_pool_is_refused_before_any_write() -> None:
    api = _FakeApi()

    with pytest.raises(ValueError, match="missing"):
        _client(api).create_claim(
            _claim_name(),
            pool="curie-agent-acme-a-runner-pool",
            env={"CURIE_BUDGET": "{}", "CURIE_RUNNER_TOKEN": "f3b2c1d0"},
        )
    _assert_no_writes(api)


def test_a_missing_source_template_is_refused_before_any_write() -> None:
    api = _FakeApi()
    _seed_chart(api)
    del api.objects[("sandboxtemplates", "curie-agent-acme-a-runner")]

    with pytest.raises(ValueError, match="missing"):
        _client(api).create_claim(
            _claim_name(),
            pool="curie-agent-acme-a-runner-pool",
            env={"CURIE_BUDGET": "{}", "CURIE_RUNNER_TOKEN": "f3b2c1d0"},
        )
    _assert_no_writes(api)


def test_an_over_long_token_claim_name_is_refused_before_any_write() -> None:
    api = _FakeApi()
    _seed_chart(api)

    with pytest.raises(ValueError):
        _client(api).create_claim(
            "c" * 49,
            pool="curie-agent-acme-a-runner-pool",
            env={"CURIE_BUDGET": "{}", "CURIE_RUNNER_TOKEN": "f3b2c1d0"},
        )
    _assert_no_writes(api)


def test_invalid_runner_resources_on_a_token_claim_are_refused_before_any_write() -> None:
    api = _FakeApi()
    _seed_chart(api)

    with pytest.raises(ValueError):
        _client(api).create_claim(
            _claim_name(),
            pool="curie-agent-acme-a-runner-pool",
            agent_name="acme-a",
            runner_resources={"requests": _OVERRIDE["requests"]},
            env={"CURIE_BUDGET": "{}", "CURIE_RUNNER_TOKEN": "f3b2c1d0"},
        )
    _assert_no_writes(api)


def test_runner_resources_on_a_token_claim_land_on_the_per_claim_template() -> None:
    # Liveness pair for the refusal above: a valid override is applied on the
    # one per-claim copy, and the separate per-agent resources objects are not
    # also written (one template per claim, not two).
    api = _FakeApi()
    _seed_chart(api)
    claim = _claim_name()

    _client(api).create_claim(
        claim,
        pool="curie-agent-acme-a-runner-pool",
        agent_name="acme-a",
        runner_resources=_OVERRIDE,
        env={"CURIE_BUDGET": "{}", "CURIE_RUNNER_TOKEN": "f3b2c1d0"},
    )

    pod = api.objects[("sandboxtemplates", f"{claim}-resources")]["spec"]["podTemplate"]["spec"]
    for container in [*pod["containers"], *pod["initContainers"]]:
        assert container["resources"] == _OVERRIDE, container["name"]
    assert ("sandboxtemplates", "curie-agent-acme-a-resources") not in api.objects
    assert _claim_body(api)["spec"]["warmPoolRef"] == {"name": f"{claim}-resources-pool"}


def test_token_free_claim_keeps_the_existing_paths() -> None:
    # Secondary-path negative: a claim with no scoped token writes no Secret
    # and no per-claim template, and binds the chart pool exactly as before.
    api = _FakeApi()
    _seed_chart(api)
    claim = _claim_name()

    _client(api).create_claim(
        claim,
        pool="curie-agent-acme-a-runner-pool",
        env={"CURIE_BUDGET": "{}", "CURIE_SESSION_ID": "s-1", "CURIE_RUNNER_TOKEN": ""},
    )

    assert api.secrets == {}
    assert [b["kind"] for b in api.created] == ["SandboxClaim"]
    assert _claim_body(api)["spec"]["warmPoolRef"] == {"name": "curie-agent-acme-a-runner-pool"}
    assert not any(
        _CLAIM_LABEL in (obj["metadata"].get("labels") or {}) for obj in api.objects.values()
    )
    assert api.patches == []


def test_token_free_claim_with_runner_resources_keeps_the_agent_resources_path() -> None:
    api = _FakeApi()
    _seed_chart(api)
    claim = _claim_name()

    _client(api).create_claim(
        claim,
        pool="curie-agent-acme-a-runner-pool",
        agent_name="acme-a",
        runner_resources=_OVERRIDE,
        env={"CURIE_BUDGET": "{}"},
    )

    assert api.secrets == {}
    assert ("sandboxtemplates", f"{claim}-resources") not in api.objects
    assert ("sandboxtemplates", "curie-agent-acme-a-resources") in api.objects
    assert _claim_body(api)["spec"]["warmPoolRef"] == {"name": "curie-agent-acme-a-resources-pool"}
    assert not any(
        _CLAIM_LABEL in (obj["metadata"].get("labels") or {}) for obj in api.objects.values()
    )


def test_reap_claim_templates_deletes_only_unkept_old_labelled_templates() -> None:
    api = _FakeApi()
    api.stub_plurals.clear()  # an absent claim is a real 404 here
    cutoff = datetime(2026, 10, 3, 12, 0, tzinfo=UTC)
    old = "2026-10-03T11:00:00Z"
    young = "2026-10-03T12:00:01Z"

    def claim_template(name: str, claim: str, created: str | None, **extra: str) -> None:
        api.seed(
            "sandboxtemplates",
            name,
            _chart_template_spec(),
            labels={_MANAGED_BY[0]: _MANAGED_BY[1], _CLAIM_LABEL: claim, **extra},
            created=created,
        )

    claim_template("gone-old-resources", "gone-old", old)  # crash-window orphan
    claim_template("kept-resources", "kept", old)  # its claim still exists
    claim_template("gone-young-resources", "gone-young", young)  # inside the grace
    claim_template("gone-unknown-resources", "gone-unknown", None)  # unknown age
    claim_template("gone-raced-resources", "gone-raced", old)  # GC got there first
    api.delete_failures["gone-raced-resources"] = k8s_module.k8s_client.ApiException(
        status=404, reason="NotFound"
    )
    # The chart's own template: no claim label, old. Never a candidate.
    api.seed(
        "sandboxtemplates",
        "curie-agent-acme-a-runner",
        _chart_template_spec(),
        labels={"app.kubernetes.io/managed-by": "Helm"},
        created=old,
    )
    # Another manager's object carrying the claim label is not ours.
    api.seed(
        "sandboxtemplates",
        "foreign-resources",
        _chart_template_spec(),
        labels={_MANAGED_BY[0]: "someone-else", _CLAIM_LABEL: "foreign"},
        created=old,
    )

    deleted = _client(api).reap_claim_templates(keep={"kept"}, created_before=cutoff)

    assert set(deleted) - {"gone-raced-resources"} == {"gone-old-resources"}
    remaining = {name for plural, name in api.objects if plural == "sandboxtemplates"}
    assert remaining == {
        "kept-resources",
        "gone-young-resources",
        "gone-unknown-resources",
        "gone-raced-resources",
        "curie-agent-acme-a-runner",
        "foreign-resources",
    }
    # Only labelled templates were ever touched.
    assert {name for plural, name in api.deletes if plural == "sandboxtemplates"} <= {
        "gone-old-resources",
        "gone-raced-resources",
    }
    assert api.lists and all(plural == "sandboxtemplates" for plural, _ in api.lists)


_REAP_CUTOFF = datetime(2026, 10, 3, 12, 0, tzinfo=UTC)
_OLD = "2026-10-03T11:00:00Z"


def _orphan_template(api: _FakeApi, claim: str) -> str:
    """Seed one labelled per-claim template old enough to be a sweep candidate."""

    name = f"{claim}-resources"
    api.seed(
        "sandboxtemplates",
        name,
        _chart_template_spec(),
        labels={_MANAGED_BY[0]: _MANAGED_BY[1], _CLAIM_LABEL: claim},
        created=_OLD,
    )
    return name


def test_reap_spares_a_template_whose_claim_appeared_after_the_inventory() -> None:
    # Finding 1 interleaving: the substrate's claim inventory was listed before
    # an in-flight create_claim finished, so the claim is not in ``keep``, but
    # by the time the sweep reaches the template the claim exists. Deleting the
    # template then would strand a live claim with no template, Secret or pool.
    api = _FakeApi()
    api.stub_plurals.clear()
    claim = "inflight"
    template = _orphan_template(api, claim)
    api.seed("sandboxclaims", claim, {"warmPoolRef": {"name": f"{claim}-resources-pool"}})

    deleted = _client(api).reap_claim_templates(keep=set(), created_before=_REAP_CUTOFF)

    assert deleted == []
    assert ("sandboxtemplates", template) in api.objects
    assert ("sandboxtemplates", template) not in api.deletes
    # The spare came from a fresh read of that claim, not from luck.
    assert ("get", "sandboxclaims") in {(verb, plural) for verb, plural, _ in api.calls}


def test_reap_still_deletes_an_old_template_whose_claim_is_truly_gone() -> None:
    # Liveness pair for the recheck: a fresh claim read that 404s still lets
    # the crash-window orphan, and with it its Secret and pool, be collected.
    api = _FakeApi()
    api.stub_plurals.clear()
    claim = "gone"
    template = _orphan_template(api, claim)

    deleted = _client(api).reap_claim_templates(keep=set(), created_before=_REAP_CUTOFF)

    assert deleted == [template]
    assert ("sandboxtemplates", template) not in api.objects


def test_reap_spares_a_template_when_the_claim_recheck_fails() -> None:
    # Fail-safe direction: a claim read that errors with anything but 404 is no
    # evidence the claim is gone, so the template is kept for the next tick.
    api = _FakeApi()
    api.stub_plurals.clear()
    claim = "unknown"
    template = _orphan_template(api, claim)
    api.failures[("get", "sandboxclaims")] = k8s_module.k8s_client.ApiException(
        status=503, reason="ServiceUnavailable"
    )

    deleted = _client(api).reap_claim_templates(keep=set(), created_before=_REAP_CUTOFF)

    assert deleted == []
    assert ("sandboxtemplates", template) in api.objects
    assert ("sandboxtemplates", template) not in api.deletes


def _finite_positive(timeout: object) -> bool:
    values = timeout if isinstance(timeout, tuple) else (timeout,)
    return bool(values) and all(
        isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) and v > 0
        for v in values
    )


def _bound(timeout: object) -> float:
    values = timeout if isinstance(timeout, tuple) else (timeout,)
    return float(sum(values))  # type: ignore[arg-type]


def test_token_claim_preparation_bounds_every_kubernetes_call() -> None:
    # Finding 1 part (a): an unbounded call between creating the per-claim
    # template and creating its claim lets the template age past the reaper
    # grace mid-creation. Every call carries a finite positive transport bound.
    api = _FakeApi()
    _seed_chart(api)
    claim = _claim_name()

    _client(api).create_claim(
        claim,
        pool="curie-agent-acme-a-runner-pool",
        agent_name="acme-a",
        env={"CURIE_BUDGET": "{}", **_scoped_token_env()},
    )

    made = [(verb, plural) for verb, plural, _ in api.calls]
    for expected in (
        ("get", "sandboxwarmpools"),
        ("get", "sandboxtemplates"),
        ("create", "sandboxtemplates"),
        ("create", "secrets"),
        ("create", "sandboxwarmpools"),
        ("create", "sandboxclaims"),
        ("patch", "sandboxtemplates"),
    ):
        assert expected in made, (expected, made)
    unbounded = [
        (verb, plural, kwargs.get("_request_timeout"))
        for verb, plural, kwargs in api.calls
        if not _finite_positive(kwargs.get("_request_timeout"))
    ]
    assert unbounded == []
    # From the moment the template exists until its claim does, the summed
    # worst case stays under the reaper's margin, so the template cannot age
    # into a candidate while its creator is still working.
    start = made.index(("create", "sandboxtemplates"))
    end = made.index(("create", "sandboxclaims"))
    window = sum(_bound(kwargs["_request_timeout"]) for _, _, kwargs in api.calls[start : end + 1])
    assert window < REAP_GRACE_MARGIN_SECONDS, window


class _RaisingCustomObjectsApi:
    """The CustomObjectsApi boundary, failing every read with one exception."""

    def __init__(self, error: Exception) -> None:
        self.error = error

    def get_namespaced_custom_object(self, *args: object, **kwargs: object) -> dict[str, Any]:
        del args, kwargs
        raise self.error


def _raising_client(error: Exception) -> KubernetesSandboxClient:
    client = KubernetesSandboxClient.__new__(KubernetesSandboxClient)
    client._api = _RaisingCustomObjectsApi(error)
    client._namespace = "test-ns"
    return client


_READ_TIMEOUT = ReadTimeoutError(
    None,  # type: ignore[arg-type]
    "/apis/extensions.agents.x-k8s.io/v1beta1/namespaces/test-ns/sandboxclaims/c",
    "Read timed out. (read timeout=0.07)",
)


@pytest.mark.parametrize(
    "error",
    [
        _READ_TIMEOUT,
        MaxRetryError(None, "/apis/sandboxclaims/c", _READ_TIMEOUT),  # type: ignore[arg-type]
        ApiException(status=503),
    ],
    ids=["read-timeout", "max-retry", "503"],
)
def test_transient_kube_read_failures_raise_kube_transient_error(error: Exception) -> None:
    # #4181: a urllib3 transport error escaped _get as a non-SandboxError and
    # left the claim's stream entry pending.
    with pytest.raises(KubeTransientError, match="sandboxclaims/c") as excinfo:
        _raising_client(error).get_claim("c", request_timeout_seconds=1.0)

    assert excinfo.value.__cause__ is error


def test_kube_read_404_is_absence() -> None:
    client = _raising_client(ApiException(status=404))

    assert client.get_claim("c", request_timeout_seconds=1.0) is None


def test_kube_read_403_is_reraised_unchanged() -> None:
    error = ApiException(status=403)

    with pytest.raises(ApiException) as excinfo:
        _raising_client(error).get_claim("c", request_timeout_seconds=1.0)

    assert excinfo.value is error
