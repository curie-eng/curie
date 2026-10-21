"""Local Cilium engine identity of the disposable local network producer, ADR 0236.

@spec PROTECTED-COMMISSIONING-5. A fake API stands in for a minikube profile
running Cilium. These tests prove only that the identity reads refuse every
state that cannot bind a behavioral measurement and that any engine change
changes the digest. Nothing here is an enforcement observation.
"""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from curie_worker.commissioning_local_cilium import (
    CILIUM_AGENT_OBJECTS,
    CILIUM_POLICY_COLLECTIONS,
    PROFILE_LABEL,
    CiliumIdentity,
    LocalCiliumAPI,
    observe_cilium,
)
from curie_worker.managed_cni_probe_kit import ProbeKitRefused

PROFILE = "d8-local"
NODES = ("d8-local", "d8-local-m02")
PROT = "prot"
DS_UID = "11111111-1111-4111-8111-111111111111"
ENVOY_UID = "22222222-2222-4222-8222-222222222222"
OP_UID = "33333333-3333-4333-8333-333333333333"
RS_UID = "44444444-4444-4444-8444-444444444444"
AGENT = "quay.io/cilium/cilium@sha256:" + "a" * 64
ENVOY = "quay.io/cilium/cilium-envoy@sha256:" + "b" * 64
OPERATOR = "quay.io/cilium/operator-generic@sha256:" + "c" * 64


def _node(name: str) -> dict[str, Any]:
    return {
        "metadata": {"name": name, "uid": "uid-" + name, "labels": {PROFILE_LABEL: PROFILE}},
        "status": {
            "conditions": [{"type": "Ready", "status": "True"}],
            "nodeInfo": {
                "kubeletVersion": "v1.35.1",
                "kernelVersion": "6.12.76",
                "containerRuntimeVersion": "docker://29.2.1",
                "osImage": "Debian GNU/Linux 12 (bookworm)",
                "architecture": "arm64",
                "operatingSystem": "linux",
            },
        },
    }


def _workload(kind: str, uid: str, image: str, **status: int) -> dict[str, Any]:
    return {
        "kind": kind,
        "metadata": {"uid": uid, "generation": 3},
        "spec": {"template": {"spec": {"containers": [{"name": "main", "image": image}]}}},
        "status": {"observedGeneration": 3, **status},
    }


def _daemonset(uid: str, image: str) -> dict[str, Any]:
    n = len(NODES)
    return _workload(
        "DaemonSet",
        uid,
        image,
        desiredNumberScheduled=n,
        numberReady=n,
        updatedNumberScheduled=n,
        numberAvailable=n,
    )


def _pod(name: str, node: str, owner_kind: str, owner_uid: str, image: str) -> dict[str, Any]:
    return {
        "metadata": {
            "name": name,
            "uid": "uid-" + name,
            "ownerReferences": [{"kind": owner_kind, "uid": owner_uid, "controller": True}],
        },
        "spec": {"nodeName": node},
        "status": {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}],
            "containerStatuses": [
                {
                    "name": "main",
                    "ready": True,
                    "restartCount": 0,
                    "state": {"running": {}},
                    "imageID": image,
                }
            ],
        },
    }


def installation() -> dict[str, Any]:
    return {
        "nodes": [_node(name) for name in NODES],
        "/api/v1/namespaces/kube-system/configmaps/cilium-config": {
            "metadata": {"uid": "cfg"},
            "data": {"enable-policy": "default", "enable-ipv4": "true", "enable-ipv6": "false"},
        },
        "/apis/apps/v1/namespaces/kube-system/daemonsets/cilium": _daemonset(DS_UID, AGENT),
        "/apis/apps/v1/namespaces/kube-system/daemonsets/cilium-envoy": _daemonset(
            ENVOY_UID, ENVOY
        ),
        "/apis/apps/v1/namespaces/kube-system/deployments/cilium-operator": _workload(
            "Deployment", OP_UID, OPERATOR, replicas=1, readyReplicas=1, updatedReplicas=1
        ),
        "/api/v1/namespaces/kube-system/pods?labelSelector=k8s-app%3Dcilium": {
            "items": [
                _pod("agent-" + str(i), node, "DaemonSet", DS_UID, AGENT)
                for i, node in enumerate(NODES)
            ]
        },
        "/api/v1/namespaces/kube-system/pods?labelSelector=k8s-app%3Dcilium-envoy": {
            "items": [
                _pod("envoy-" + str(i), node, "DaemonSet", ENVOY_UID, ENVOY)
                for i, node in enumerate(NODES)
            ]
        },
        "/api/v1/namespaces/kube-system/pods?labelSelector=io.cilium%2Fapp%3Doperator": {
            "items": [_pod("operator", NODES[0], "ReplicaSet", RS_UID, OPERATOR)]
        },
    }


class FakeReader:
    def __init__(self, state: dict[str, Any]) -> None:
        self.state = state

    async def read(self, path: str) -> dict[str, Any] | None:
        value = self.state.get(path)
        return copy.deepcopy(value)

    async def list_items(self, collection: str) -> list[dict[str, Any]] | None:
        assert collection == "nodes"
        value = self.state.get("nodes")
        return None if value is None else copy.deepcopy(value)


def observe(state: dict[str, Any]) -> CiliumIdentity:
    return asyncio.run(
        observe_cilium(
            FakeReader(state),
            profile=PROFILE,
            eligible_nodes=NODES,
        )
    )


def refused(change: Callable[[dict[str, Any]], None]) -> None:
    state = installation()
    change(state)
    with pytest.raises(ProbeKitRefused):
        observe(state)


def test_a_healthy_enforcing_installation_has_a_stable_identity() -> None:
    first, second = observe(installation()), observe(installation())
    assert first.families == ("ipv4",)
    assert first.digest == second.digest and len(first.digest) == 64
    assert "token" not in repr(first)


def test_policy_and_agent_objects_name_the_cilium_engine() -> None:
    assert CILIUM_POLICY_COLLECTIONS == (
        "/apis/cilium.io/v2/ciliumnetworkpolicies",
        "/apis/cilium.io/v2/ciliumclusterwidenetworkpolicies",
    )
    assert {(r.group, r.resource, r.namespace, r.name) for r in CILIUM_AGENT_OBJECTS} == {
        ("", "configmaps", "kube-system", "cilium-config"),
        ("apps", "daemonsets", "kube-system", "cilium"),
        ("apps", "daemonsets", "kube-system", "cilium-envoy"),
        ("apps", "deployments", "kube-system", "cilium-operator"),
    }


@pytest.mark.parametrize(
    "mode,families",
    [(("true", "false"), ("ipv4",)), (("false", "true"), ("ipv6",)), (("true", "true"), None)],
)
def test_families_come_from_the_agent_configuration(
    mode: tuple[str, str], families: tuple[str, ...] | None
) -> None:
    state = installation()
    data = state["/api/v1/namespaces/kube-system/configmaps/cilium-config"]["data"]
    data["enable-ipv4"], data["enable-ipv6"] = mode
    assert observe(state).families == (families or ("ipv4", "ipv6"))


def _config(state: dict[str, Any]) -> dict[str, Any]:
    return state["/api/v1/namespaces/kube-system/configmaps/cilium-config"]["data"]  # type: ignore[no-any-return]


@pytest.mark.parametrize(
    "change",
    [
        # Policy enforcement off, or audit mode, means policies do not drop traffic.
        lambda s: _config(s).update({"enable-policy": "never"}),
        lambda s: _config(s).pop("enable-policy"),
        lambda s: _config(s).update({"policy-audit-mode": "true"}),
        # Cilium would then ignore every Kubernetes NetworkPolicy object.
        lambda s: _config(s).update({"enable-k8s-networkpolicy": "false"}),
        lambda s: _config(s).update({"enable-ipv4": "false", "enable-ipv6": "false"}),
        lambda s: s.pop("/api/v1/namespaces/kube-system/configmaps/cilium-config"),
    ],
)
def test_a_non_enforcing_configuration_refuses(change: Callable[[dict[str, Any]], None]) -> None:
    refused(change)


def _agents(state: dict[str, Any]) -> list[dict[str, Any]]:
    return state["/api/v1/namespaces/kube-system/pods?labelSelector=k8s-app%3Dcilium"]["items"]  # type: ignore[no-any-return]


@pytest.mark.parametrize(
    "change",
    [
        # A node outside the profile, a node missing, a NotReady node.
        lambda s: s["nodes"][1]["metadata"]["labels"].update({PROFILE_LABEL: "other"}),
        lambda s: s["nodes"].pop(),
        lambda s: s["nodes"].append(_node("stranger")),
        lambda s: s["nodes"][0]["status"]["conditions"][0].update({"status": "False"}),
        lambda s: s.update({"nodes": None}),
        # Rollout incomplete or an agent missing, unready, restarted elsewhere.
        lambda s: s["/apis/apps/v1/namespaces/kube-system/daemonsets/cilium"]["status"].update(
            {"numberReady": 1}
        ),
        lambda s: s["/apis/apps/v1/namespaces/kube-system/daemonsets/cilium"]["status"].update(
            {"observedGeneration": 2}
        ),
        lambda s: _agents(s).pop(),
        lambda s: _agents(s).append(_pod("agent-x", NODES[0], "DaemonSet", DS_UID, AGENT)),
        lambda s: _agents(s)[0]["status"].update({"phase": "Pending"}),
        lambda s: _agents(s)[0]["status"]["containerStatuses"][0].update({"ready": False}),
        lambda s: _agents(s)[0]["status"]["containerStatuses"][0].update({"imageID": "cilium:v1"}),
        lambda s: _agents(s)[0]["metadata"]["ownerReferences"][0].update({"uid": "other"}),
        lambda s: _agents(s)[0]["metadata"].update({"deletionTimestamp": "2026-10-21T00:00:00Z"}),
        lambda s: s.pop("/apis/apps/v1/namespaces/kube-system/daemonsets/cilium-envoy"),
        lambda s: s["/apis/apps/v1/namespaces/kube-system/deployments/cilium-operator"][
            "status"
        ].update({"readyReplicas": 0}),
        lambda s: s["/api/v1/namespaces/kube-system/pods?labelSelector=io.cilium%2Fapp%3Doperator"][
            "items"
        ].clear(),
    ],
)
def test_an_engine_that_is_not_fully_rolled_out_refuses(
    change: Callable[[dict[str, Any]], None],
) -> None:
    refused(change)


@pytest.mark.parametrize(
    "change",
    [
        lambda s: _config(s).update({"tunnel-protocol": "geneve"}),
        lambda s: s["nodes"][0]["status"]["nodeInfo"].update({"kernelVersion": "6.13"}),
        lambda s: _agents(s)[0]["status"]["containerStatuses"][0].update({"restartCount": 1}),
        lambda s: _agents(s)[1]["metadata"].update({"uid": "uid-replaced"}),
        lambda s: s["/apis/apps/v1/namespaces/kube-system/daemonsets/cilium"]["spec"]["template"][
            "spec"
        ]["containers"][0].update({"image": "quay.io/cilium/cilium@sha256:" + "d" * 64}),
        lambda s: (
            s["/apis/apps/v1/namespaces/kube-system/daemonsets/cilium"]["metadata"].update(
                {"generation": 4}
            )
            or s["/apis/apps/v1/namespaces/kube-system/daemonsets/cilium"]["status"].update(
                {"observedGeneration": 4}
            )
        ),
    ],
)
def test_any_engine_change_changes_the_identity(change: Callable[[dict[str, Any]], None]) -> None:
    state = installation()
    change(state)
    assert observe(state).digest != observe(installation()).digest


def api(handler: Any) -> LocalCiliumAPI:
    client = LocalCiliumAPI.__new__(LocalCiliumAPI)
    client._namespaces = frozenset({"probe-a", PROT})
    client._origin = "https://127.0.0.1:6443"
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return client


def test_the_client_reads_only_the_fixed_cilium_paths() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path + "?" + request.url.query.decode())
        return httpx.Response(200, json={"items": []})

    client = api(handler)
    for path in installation():
        if path != "nodes":
            asyncio.run(client.read(path))
    for path in (
        "/api/v1/namespaces/kube-system/secrets",
        "/api/v1/namespaces/kube-system/configmaps/cilium-ca",
        "/apis/cilium.io/v2/namespaces/prot/ciliumendpoints",
        "/api/v1/namespaces/kube-system/pods?labelSelector=k8s-app%3Dcilium&x=1",
        "/apis/apps/v1/namespaces/kube-system/daemonsets/cilium/../../secrets",
    ):
        with pytest.raises(ProbeKitRefused):
            asyncio.run(client.read(path))
    assert len(seen) == len(installation()) - 1
