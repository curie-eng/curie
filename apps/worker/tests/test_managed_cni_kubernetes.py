"""The measuring identity's bounded API client, ADR 0238 (@spec MANAGED-CNI-5).

A mock transport stands in for the API server. These tests prove request
shapes and the closed path allowlist only.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest
from curie_worker.managed_cni_cases import ResourceAttributes
from curie_worker.managed_cni_kubernetes import MeasuringCredentials, MeasuringIdentityAPI
from curie_worker.managed_cni_probe_kit import ProbeKitRefused


def api(handler: Any) -> MeasuringIdentityAPI:
    client = MeasuringIdentityAPI.__new__(MeasuringIdentityAPI)
    client._namespaces = frozenset({"probe-a", "prot"})
    client._origin = "https://k8s.example:6443"
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return client


def test_credentials_refuse_anything_but_a_bare_https_origin() -> None:
    for origin in ("http://k8s:6443", "https://u:p@k8s:6443", "https://k8s:6443/x"):
        with pytest.raises(ProbeKitRefused):
            MeasuringCredentials(origin, "/nonexistent", "t").context()
    with pytest.raises(ProbeKitRefused):
        MeasuringCredentials("https://k8s:6443", "/nonexistent", "t\nx").context()
    assert "secret-token" not in repr(MeasuringCredentials("https://k8s", "/ca", "secret-token"))


def test_pods_only_in_the_allowed_namespaces() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.method + " " + request.url.path + "?" + request.url.query.decode())
        return httpx.Response(201, json={"metadata": {"uid": "u1"}})

    client = api(handler)
    asyncio.run(client.create_pod("prot", {"kind": "Pod"}))
    with pytest.raises(ProbeKitRefused):
        asyncio.run(client.create_pod("kube-system", {"kind": "Pod"}))
    with pytest.raises(ProbeKitRefused):
        asyncio.run(client.read_pod("prot", "../secrets"))
    assert seen == ["POST /api/v1/namespaces/prot/pods?"]


def test_dry_run_returns_the_refusal_message() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.query == b"dryRun=All"
        return httpx.Response(400, json={"message": 'admission webhook "g" denied the request'})

    status, message = asyncio.run(api(handler).dry_run_create_pod("prot", {"kind": "Pod"}))
    assert (status, message) == (400, 'admission webhook "g" denied the request')


def test_access_review_shape_and_answers() -> None:
    bodies: list[dict[str, Any]] = []
    answers = iter(
        [{"allowed": False}, {"allowed": True}, {"allowed": False, "evaluationError": "x"}]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/apis/authorization.k8s.io/v1/subjectaccessreviews"
        bodies.append(json.loads(request.content))
        return httpx.Response(201, json={"status": next(answers)})

    client = api(handler)
    attributes = ResourceAttributes("create", "", "pods", "prot", "ephemeralcontainers")
    results = [asyncio.run(client.access_review("u", ["g"], attributes)) for _ in range(3)]
    assert results == [False, True, None]
    assert bodies[0]["spec"] == {
        "user": "u",
        "groups": ["g"],
        "resourceAttributes": {
            "verb": "create",
            "group": "",
            "resource": "pods",
            "namespace": "prot",
            "subresource": "ephemeralcontainers",
        },
    }


def test_lists_follow_continue_tokens_and_refuse_unknown_collections() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if "continue=" in request.url.query.decode():
            return httpx.Response(200, json={"items": [{"n": 2}], "metadata": {}})
        return httpx.Response(200, json={"items": [{"n": 1}], "metadata": {"continue": "c"}})

    client = api(handler)
    assert asyncio.run(client.list_items("nodes")) == [{"n": 1}, {"n": 2}]
    assert asyncio.run(client.list_items("/apis/crd.projectcalico.org/v1/networkpolicies"))
    with pytest.raises(ProbeKitRefused):
        asyncio.run(client.list_items("/api/v1/secrets"))
    with pytest.raises(ProbeKitRefused):
        asyncio.run(client.list_items("secrets"))


def test_a_forbidden_list_reads_as_unavailable() -> None:
    client = api(lambda request: httpx.Response(403, json={"message": "forbidden"}))
    assert asyncio.run(client.list_items("clusterrolebindings")) is None
