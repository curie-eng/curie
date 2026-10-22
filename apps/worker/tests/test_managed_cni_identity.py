"""The managed CNI identity fact and its invalidation, @spec MANAGED-CNI-1 / MANAGED-CNI-9.

A fake reader stands in for the API server with a Calico-shaped provider
entry: a plugin DaemonSet, a policy-agent Deployment, an enforcement-mode
environment variable, one add-on ConfigMap and kube-proxy. Every identity
input that changes refuses the next reobservation with the member that
changed; so does an added, removed or replaced eligible node. A node no
protected template can be scheduled on is not part of the identity.
"""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Callable
from typing import Any

import pytest
from curie_worker.managed_cni_identity import (
    IDENTITY_MEMBERS,
    CniIdentity,
    CniIdentityFact,
    ManagedIdentityAPI,
    ModeKey,
    Placement,
    ProviderEntry,
    WorkloadRef,
    cni_digest,
    identity_change,
    managed_cni_fact,
    observe_identity,
)
from curie_worker.managed_cni_probe_kit import (
    PRODUCER_CONSTANTS,
    PRODUCER_KIND,
    ObservationEntry,
    ProbeKitRefused,
    producer_constants_digest,
    sha256_canonical,
)

NODES = ("n1", "n2")
PLUGIN_UID = "11111111-1111-4111-8111-111111111111"
AGENT_UID = "22222222-2222-4222-8222-222222222222"
PROXY_UID = "33333333-3333-4333-8333-333333333333"
RS_UID = "44444444-4444-4444-8444-444444444444"
PLUGIN = "docker.io/calico/node@sha256:" + "a" * 64
AGENT = "docker.io/calico/kube-controllers@sha256:" + "b" * 64
PROXY = "registry.k8s.io/kube-proxy@sha256:" + "c" * 64
PLUGIN_DS = "/apis/apps/v1/namespaces/kube-system/daemonsets/calico-node"
PLUGIN_PODS = "/api/v1/namespaces/kube-system/pods?labelSelector=k8s-app%3Dcalico-node"
AGENT_DEPLOYMENT = "/apis/apps/v1/namespaces/kube-system/deployments/calico-kube-controllers"
AGENT_PODS = "/api/v1/namespaces/kube-system/pods?labelSelector=k8s-app%3Dcalico-kube-controllers"
CONFIG = "/api/v1/namespaces/kube-system/configmaps/calico-config"
PROXY_DS = "/apis/apps/v1/namespaces/kube-system/daemonsets/kube-proxy"
PROXY_PODS = "/api/v1/namespaces/kube-system/pods?labelSelector=k8s-app%3Dkube-proxy"
PROXY_CONFIG = "/api/v1/namespaces/kube-system/configmaps/kube-proxy"
CONTROL_PLANE = "node-role.kubernetes.io/control-plane"
GUARD = "d" * 64

PROVIDER = ProviderEntry(
    provider="calico-fixture",
    cni_plugin=(WorkloadRef("DaemonSet", "kube-system", "calico-node", "k8s-app=calico-node"),),
    policy_agent=(
        WorkloadRef(
            "Deployment",
            "kube-system",
            "calico-kube-controllers",
            "k8s-app=calico-kube-controllers",
        ),
    ),
    mode_keys=(
        ModeKey("calico-node", "calico-node", env="FELIX_DEFAULTENDPOINTTOHOSTACTION"),
        ModeKey("calico-kube-controllers", "calico-kube-controllers", flag="--enabled"),
    ),
    addon_configmaps=(("kube-system", "calico-config"),),
)
# The protected templates tolerate nothing and select Linux workers.
PLACEMENTS = (Placement.of({"nodeSelector": {"kubernetes.io/os": "linux"}}),)


def node(name: str, **changes: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "metadata": {"name": name, "uid": "uid-" + name, "labels": {"kubernetes.io/os": "linux"}},
        "spec": {"podCIDRs": ["10.244.0.0/24"]},
        "status": {
            "conditions": [{"type": "Ready", "status": "True"}],
            "addresses": [{"type": "InternalIP", "address": "192.168.49.2"}],
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
    value.update(changes)
    return value


def pod(
    name: str, node_name: str, owner_kind: str, owner_uid: str, container: str, image: str
) -> dict[str, Any]:
    return {
        "metadata": {
            "name": name,
            "uid": "uid-" + name,
            "labels": {"pod-template-hash": "abc"} if owner_kind == "ReplicaSet" else {},
            "ownerReferences": [{"kind": owner_kind, "uid": owner_uid, "controller": True}],
        },
        "spec": {"nodeName": node_name},
        "status": {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}],
            "containerStatuses": [
                {
                    "name": container,
                    "ready": True,
                    "restartCount": 0,
                    "state": {"running": {}},
                    "imageID": "docker-pullable://" + image,
                }
            ],
        },
    }


def workload(
    kind: str, uid: str, name: str, container: dict[str, Any], count: int
) -> dict[str, Any]:
    status: dict[str, Any] = {"observedGeneration": 2}
    spec: dict[str, Any] = {"template": {"spec": {"containers": [container]}}}
    if kind == "DaemonSet":
        status |= {
            "desiredNumberScheduled": count,
            "numberReady": count,
            "updatedNumberScheduled": count,
            "numberAvailable": count,
        }
    else:
        spec["replicas"] = count
        status |= {"replicas": count, "readyReplicas": count, "updatedReplicas": count}
    return {
        "kind": kind,
        "metadata": {"name": name, "uid": uid, "generation": 2},
        "spec": spec,
        "status": status,
    }


def installation() -> dict[str, Any]:
    plugin_container = {
        "name": "calico-node",
        "image": "docker.io/calico/node:v3.30.0",
        "env": [{"name": "FELIX_DEFAULTENDPOINTTOHOSTACTION", "value": "ACCEPT"}],
    }
    agent_container = {
        "name": "calico-kube-controllers",
        "image": "docker.io/calico/kube-controllers:v3.30.0",
        "args": ["--enabled=node,policy"],
    }
    proxy_container = {"name": "kube-proxy", "image": "registry.k8s.io/kube-proxy:v1.35.1"}
    return {
        "nodes": [node(name) for name in NODES],
        "/version": {"gitVersion": "v1.35.1", "gitCommit": "x", "platform": "linux/arm64"},
        PLUGIN_DS: workload("DaemonSet", PLUGIN_UID, "calico-node", plugin_container, 2),
        PLUGIN_PODS: {
            "items": [
                pod(f"calico-{n}", n, "DaemonSet", PLUGIN_UID, "calico-node", PLUGIN) for n in NODES
            ]
        },
        AGENT_DEPLOYMENT: workload(
            "Deployment", AGENT_UID, "calico-kube-controllers", agent_container, 1
        ),
        AGENT_PODS: {
            "items": [
                pod("controllers", "n1", "ReplicaSet", RS_UID, "calico-kube-controllers", AGENT)
            ]
        },
        CONFIG: {"metadata": {"uid": "cfg"}, "data": {"cni_network_config": "{}", "veth_mtu": "0"}},
        PROXY_DS: workload("DaemonSet", PROXY_UID, "kube-proxy", proxy_container, 2),
        PROXY_PODS: {
            "items": [
                pod(f"proxy-{n}", n, "DaemonSet", PROXY_UID, "kube-proxy", PROXY) for n in NODES
            ]
        },
        PROXY_CONFIG: {
            "metadata": {"uid": "proxy-cfg"},
            "data": {"config.conf": 'mode: "iptables"\nclusterCIDR: 10.244.0.0/16\n'},
        },
    }


class Reader:
    def __init__(self, state: dict[str, Any]) -> None:
        self.state = state
        self.paths: list[str] = []

    async def read(self, path: str) -> dict[str, Any] | None:
        self.paths.append(path)
        return copy.deepcopy(self.state.get(path))

    async def list_items(self, collection: str) -> list[dict[str, Any]] | None:
        assert collection == "nodes"
        value = self.state.get("nodes")
        return None if value is None else copy.deepcopy(value)


class Cloud:
    def __init__(self) -> None:
        self.addon: dict[str, Any] = {
            "addon": "vpc-cni",
            "configuration": {"enableNetworkPolicy": "true"},
        }
        self.images: dict[str, str] = {}

    async def describe(self) -> tuple[dict[str, Any], dict[str, str]]:
        return copy.deepcopy(self.addon), dict(self.images)


def observe(
    state: dict[str, Any], cloud: Cloud | None = None, placements: Any = PLACEMENTS
) -> CniIdentity:
    return asyncio.run(observe_identity(Reader(state), PROVIDER, placements, cloud=cloud))


def refuses(state: dict[str, Any], **options: Any) -> None:
    with pytest.raises(ProbeKitRefused):
        observe(state, **options)


# --- The identity object ------------------------------------------------------


def test_the_identity_holds_exactly_the_clause_1_members() -> None:
    identity = observe(installation())
    assert sorted(identity.document) == sorted(IDENTITY_MEMBERS)
    assert identity.digest == sha256_canonical(identity.document)
    doc = identity.document
    assert doc["cni_plugin_images"] == [
        {
            "workload": "calico-node",
            "container": "calico-node",
            "image_digest": "sha256:" + "a" * 64,
        }
    ]
    assert doc["policy_agent_images"] == [
        {
            "workload": "calico-kube-controllers",
            "container": "calico-kube-controllers",
            "image_digest": "sha256:" + "b" * 64,
        }
    ]
    assert {item["key"]: item["value"] for item in doc["enforcement_mode"]} == {
        "FELIX_DEFAULTENDPOINTTOHOSTACTION": "ACCEPT",
        "--enabled": "node,policy",
    }
    assert doc["dataplane_mode"]["kube_proxy"]["mode"] == "iptables"
    assert doc["dataplane_mode"]["kube_proxy"]["images"] == [
        {"container": "kube-proxy", "image_digest": "sha256:" + "c" * 64}
    ]
    assert doc["control_plane_version"] == {"gitVersion": "v1.35.1", "platform": "linux/arm64"}
    assert doc["ip_families"] == ["ipv4"]
    assert [item["node_name"] for item in doc["eligible_nodes"]] == list(NODES)
    assert doc["eligible_nodes"][0] == {
        "node_name": "n1",
        "node_uid": "uid-n1",
        "node_image": "Debian GNU/Linux 12 (bookworm)",
        "kernel_version": "6.12.76",
        "kubelet_version": "v1.35.1",
        "container_runtime_version": "docker://29.2.1",
    }


def test_the_identity_is_stable_and_ignores_pod_restarts_and_uids() -> None:
    state = installation()
    first = observe(state)
    pods = state[PLUGIN_PODS]["items"]
    pods[0]["status"]["containerStatuses"][0]["restartCount"] = 4
    pods[1]["metadata"]["uid"] = "uid-recreated"
    state[CONFIG]["metadata"]["uid"] = "cfg-recreated"
    assert observe(state).digest == first.digest


def test_the_reads_stay_inside_the_provider_entry() -> None:
    reader = Reader(installation())
    asyncio.run(observe_identity(reader, PROVIDER, PLACEMENTS))
    assert set(reader.paths) <= PROVIDER.paths()
    assert not any("replicasets" in path or "secrets" in path for path in reader.paths)


def test_the_cni_channel_digest_binds_kind_constants_and_identity() -> None:
    identity = observe(installation())
    assert cni_digest(identity.digest) == sha256_canonical(
        {
            "producer_kind": PRODUCER_KIND,
            "producer_constants_digest": producer_constants_digest(),
            "identity_digest": identity.digest,
        }
    )
    assert cni_digest(identity.digest) != cni_digest("0" * 64)


# --- Each identity input changing invalidates ---------------------------------


def _all(items: list[dict[str, Any]], change: Callable[[dict[str, Any]], None]) -> None:
    for item in items:
        change(item)


def _image(state: dict[str, Any], path: str, digit: str) -> None:
    def change(item: dict[str, Any]) -> None:
        status = item["status"]["containerStatuses"][0]
        status["imageID"] = status["imageID"].split("@")[0] + "@sha256:" + digit * 64

    _all(state[path]["items"], change)


def _info(key: str, value: str) -> Callable[[dict[str, Any]], None]:
    def change(state: dict[str, Any]) -> None:
        state["nodes"][1]["status"]["nodeInfo"][key] = value

    return change


def _env(value: str) -> Callable[[dict[str, Any]], None]:
    def change(state: dict[str, Any]) -> None:
        container = state[PLUGIN_DS]["spec"]["template"]["spec"]["containers"][0]
        container["env"][0]["value"] = value

    return change


INPUT_CHANGES: dict[str, tuple[Callable[[dict[str, Any]], None], str]] = {
    "plugin image": (lambda s: _image(s, PLUGIN_PODS, "e"), "cni_plugin_images_changed"),
    "agent image": (lambda s: _image(s, AGENT_PODS, "e"), "policy_agent_images_changed"),
    "mode env": (_env("DROP"), "enforcement_mode_changed"),
    "mode env removed": (
        lambda s: s[PLUGIN_DS]["spec"]["template"]["spec"]["containers"][0].update(env=[]),
        "enforcement_mode_changed",
    ),
    "mode flag": (
        lambda s: s[AGENT_DEPLOYMENT]["spec"]["template"]["spec"]["containers"][0].update(
            args=["--enabled", "node"]
        ),
        "enforcement_mode_changed",
    ),
    "addon config": (
        lambda s: s[CONFIG]["data"].update(veth_mtu="1450"),
        "addon_configuration_digest_changed",
    ),
    "kube-proxy mode": (
        lambda s: s[PROXY_CONFIG]["data"].update({"config.conf": 'mode: "ipvs"\n'}),
        "dataplane_mode_changed",
    ),
    "kube-proxy config": (
        lambda s: s[PROXY_CONFIG]["data"].update(extra="1"),
        "dataplane_mode_changed",
    ),
    "kube-proxy image": (lambda s: _image(s, PROXY_PODS, "f"), "dataplane_mode_changed"),
    "kube-proxy removed": (
        lambda s: (s.pop(PROXY_DS), s[PROXY_PODS].update(items=[])),
        "dataplane_mode_changed",
    ),
    "control plane version": (
        lambda s: s["/version"].update(gitVersion="v1.35.2"),
        "control_plane_version_changed",
    ),
    "control plane platform": (
        lambda s: s["/version"].update(platform="linux/amd64"),
        "control_plane_version_changed",
    ),
    "node kernel": (_info("kernelVersion", "6.13.0"), "eligible_nodes_changed"),
    "node kubelet": (_info("kubeletVersion", "v1.35.2"), "eligible_nodes_changed"),
    "node runtime": (_info("containerRuntimeVersion", "docker://30.0.0"), "eligible_nodes_changed"),
    "node image": (_info("osImage", "Debian GNU/Linux 13"), "eligible_nodes_changed"),
    "ip family": (
        lambda s: s["nodes"][0]["spec"]["podCIDRs"].append("fd00::/64"),
        "ip_families_changed",
    ),
}


@pytest.mark.parametrize("name", sorted(INPUT_CHANGES))
def test_each_identity_input_changing_invalidates(name: str) -> None:
    state = installation()
    before = observe(state)
    change, reason = INPUT_CHANGES[name]
    change(state)
    after = observe(state)
    assert after.digest != before.digest
    assert identity_change(before, after) == reason
    fact = Fact(state, before)
    with pytest.raises(ProbeKitRefused):
        fact.observe()
    assert fact.producer.refusal == reason


def test_a_cloud_reported_addon_or_node_image_change_invalidates() -> None:
    state, cloud = installation(), Cloud()
    before = observe(state, cloud)
    cloud.addon["configuration"]["enableNetworkPolicy"] = "false"
    assert identity_change(before, observe(state, cloud)) == "addon_configuration_digest_changed"
    cloud = Cloud()
    before = observe(state, cloud)
    cloud.images["n2"] = "ami-0123"
    after = observe(state, cloud)
    assert after.document["eligible_nodes"][1]["node_image"] == "ami-0123"
    assert identity_change(before, after) == "eligible_nodes_changed"


# --- The eligible node set ----------------------------------------------------


def _add_node(state: dict[str, Any], name: str, **changes: Any) -> None:
    state["nodes"].append(node(name, **changes))
    for path, owner, container, image in (
        (PLUGIN_PODS, PLUGIN_UID, "calico-node", PLUGIN),
        (PROXY_PODS, PROXY_UID, "kube-proxy", PROXY),
    ):
        state[path]["items"].append(
            pod(f"{container}-{name}", name, "DaemonSet", owner, container, image)
        )
    for path in (PLUGIN_DS, PROXY_DS):
        status = state[path]["status"]
        for key in (
            "desiredNumberScheduled",
            "numberReady",
            "updatedNumberScheduled",
            "numberAvailable",
        ):
            status[key] += 1


TAINT = [{"key": CONTROL_PLANE, "effect": "NoSchedule"}]

NODE_CHANGES: dict[str, tuple[Callable[[dict[str, Any]], None], str]] = {
    "added": (lambda s: _add_node(s, "n3"), "eligible_node_added"),
    "removed": (lambda s: s["nodes"].pop(), "eligible_node_removed"),
    "replaced": (
        lambda s: s["nodes"][1]["metadata"].update(uid="uid-n2-new"),
        "eligible_node_replaced",
    ),
    "tainted out": (lambda s: s["nodes"][1]["spec"].update(taints=TAINT), "eligible_node_removed"),
    "cordoned": (
        lambda s: s["nodes"][1]["spec"].update(unschedulable=True),
        "eligible_node_removed",
    ),
    "relabelled out": (
        lambda s: s["nodes"][1]["metadata"]["labels"].update({"kubernetes.io/os": "other"}),
        "eligible_node_removed",
    ),
}


@pytest.mark.parametrize("name", sorted(NODE_CHANGES))
def test_a_change_of_the_eligible_node_set_invalidates(name: str) -> None:
    state = installation()
    before = observe(state)
    change, reason = NODE_CHANGES[name]
    change(state)
    assert identity_change(before, observe(state)) == reason
    fact = Fact(state, before)
    with pytest.raises(ProbeKitRefused):
        fact.observe()
    assert fact.producer.refusal == reason


def test_a_node_no_protected_template_can_use_is_not_part_of_the_identity() -> None:
    state = installation()
    before = observe(state)
    _add_node(state, "cp", spec={"taints": TAINT, "podCIDRs": ["10.244.9.0/24"]})
    assert observe(state).digest == before.digest
    # Untainting it makes it a new, unmeasured eligible node.
    state["nodes"][-1]["spec"]["taints"] = []
    assert identity_change(before, observe(state)) == "eligible_node_added"


def test_a_tolerating_template_makes_a_tainted_node_eligible() -> None:
    state = installation()
    state["nodes"][1]["spec"]["taints"] = TAINT
    assert [n["node_name"] for n in observe(state).document["eligible_nodes"]] == ["n1"]
    tolerating = (
        Placement.of(
            {
                "nodeSelector": {"kubernetes.io/os": "linux"},
                "tolerations": [
                    {"key": CONTROL_PLANE, "operator": "Exists", "effect": "NoSchedule"}
                ],
            }
        ),
    )
    assert [
        n["node_name"] for n in observe(state, placements=tolerating).document["eligible_nodes"]
    ] == list(NODES)


def test_required_node_affinity_selects_eligible_nodes() -> None:
    state = installation()
    state["nodes"][0]["metadata"]["labels"]["pool"] = "protected"
    for operator, expected in (
        ("In", ["n1"]),
        ("NotIn", ["n2"]),
        ("Exists", ["n1"]),
        ("DoesNotExist", ["n2"]),
    ):
        expression: dict[str, Any] = {"key": "pool", "operator": operator}
        if operator in ("In", "NotIn"):
            expression["values"] = ["protected"]
        placement = Placement.of(
            {
                "affinity": {
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": [{"matchExpressions": [expression]}]
                        }
                    }
                }
            }
        )
        assert [
            n["node_name"]
            for n in observe(state, placements=(placement,)).document["eligible_nodes"]
        ] == (expected)


@pytest.mark.parametrize(
    "spec",
    [
        {
            "affinity": {
                "nodeAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": {
                        "nodeSelectorTerms": [{"matchFields": [{"key": "metadata.name"}]}]
                    }
                }
            }
        },
        {
            "affinity": {
                "nodeAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": {
                        "nodeSelectorTerms": [
                            {"matchExpressions": [{"key": "a", "operator": "Gt", "values": ["1"]}]}
                        ]
                    }
                }
            }
        },
        {"tolerations": [{"key": "a", "operator": "Lt"}]},
        {"nodeSelector": {"a": 1}},
    ],
)
def test_unsupported_scheduling_constraints_refuse(spec: dict[str, Any]) -> None:
    with pytest.raises(ProbeKitRefused):
        Placement.of(spec)


# --- Unbindable states refuse -------------------------------------------------


def _tag_only(s: dict[str, Any]) -> None:
    s[PLUGIN_PODS]["items"][0]["status"]["containerStatuses"][0]["imageID"] = (
        "docker.io/calico/node:v3"
    )


def _mixed(s: dict[str, Any]) -> None:
    status = s[PLUGIN_PODS]["items"][1]["status"]["containerStatuses"][0]
    status["imageID"] = "docker.io/calico/node@sha256:" + "9" * 64


UNBINDABLE: dict[str, Callable[[dict[str, Any]], Any]] = {
    "tag without digest": _tag_only,
    "mixed plugin digests": _mixed,
    "plugin missing on an eligible node": lambda s: s[PLUGIN_PODS]["items"].pop(),
    "plugin pod not ready": lambda s: s[PLUGIN_PODS]["items"][0]["status"]["conditions"][0].update(
        status="False"
    ),
    "plugin rollout incomplete": lambda s: s[PLUGIN_DS]["status"].update(updatedNumberScheduled=1),
    "agent surplus replica": lambda s: s[AGENT_DEPLOYMENT]["status"].update(replicas=2),
    "agent two templates": lambda s: (
        s[AGENT_PODS]["items"].append(copy.deepcopy(s[AGENT_PODS]["items"][0])),
        s[AGENT_PODS]["items"][1]["metadata"]["labels"].update({"pod-template-hash": "old"}),
    ),
    "agent missing": lambda s: s.pop(AGENT_DEPLOYMENT),
    "mode container missing": lambda s: s[PLUGIN_DS]["spec"]["template"]["spec"]["containers"][
        0
    ].update(name="other"),
    "addon config missing": lambda s: s.pop(CONFIG),
    "version missing": lambda s: s.pop("/version"),
    "nodes unavailable": lambda s: s.pop("nodes"),
    "windows eligible node": lambda s: s["nodes"][1]["status"]["nodeInfo"].update(
        operatingSystem="windows"
    ),
    "eligible node not ready": lambda s: s["nodes"][1]["status"]["conditions"][0].update(
        status="False"
    ),
    "no eligible node": lambda s: s.update(nodes=[]),
    "kube-proxy pods without daemonset": lambda s: s.pop(PROXY_DS),
    "kube-proxy two modes": lambda s: s[PROXY_CONFIG]["data"].update(other='mode: "ipvs"\n'),
    "terminating plugin": lambda s: s[PLUGIN_DS]["metadata"].update(deletionTimestamp="now"),
}


@pytest.mark.parametrize("name", sorted(UNBINDABLE))
def test_an_unbindable_identity_refuses(name: str) -> None:
    state = installation()
    UNBINDABLE[name](state)
    refuses(state)


def test_an_absent_kube_proxy_is_its_own_identity() -> None:
    state = installation()
    state.pop(PROXY_DS)
    state[PROXY_PODS]["items"] = []
    assert observe(state).document["dataplane_mode"] == {"kube_proxy": None}


# --- Reobservation on every guard call ----------------------------------------


class Fact:
    """A ``CniIdentityFact`` over a mutable fake state, run synchronously."""

    def __init__(self, state: dict[str, Any], campaign: CniIdentity) -> None:
        self.guard = GUARD
        self.least: str | None = None
        self.calls = 0

        async def guard() -> str:
            return self.guard

        async def least_privilege() -> str | None:
            self.calls += 1
            return self.least

        self.reader = Reader(state)
        self.producer = CniIdentityFact(
            self.reader,
            PROVIDER,
            PLACEMENTS,
            campaign=campaign,
            guard_config_digest=GUARD,
            guard=guard,
            least_privilege=least_privilege,
        )

    def observe(self) -> str:
        return asyncio.run(self.producer.observe())


def test_an_unchanged_installation_yields_the_cni_digest_on_every_call() -> None:
    state = installation()
    campaign = observe(state)
    fact = Fact(state, campaign)
    assert fact.producer.fact == "cni"
    assert fact.observe() == cni_digest(campaign.digest)
    assert fact.observe() == cni_digest(campaign.digest)
    # Never cached: each call reads again and reruns the least-privilege check.
    assert fact.calls == 2
    assert fact.reader.paths.count("/version") == 2


def test_a_changed_guard_configuration_refuses() -> None:
    state = installation()
    fact = Fact(state, observe(state))
    fact.guard = "e" * 64
    with pytest.raises(ProbeKitRefused):
        fact.observe()
    assert fact.producer.refusal == "guard_config_changed"


def test_an_over_privileged_measuring_identity_refuses_the_call() -> None:
    state = installation()
    fact = Fact(state, observe(state))
    fact.least = "measuring_review_allowed"
    with pytest.raises(ProbeKitRefused):
        fact.observe()
    assert fact.producer.refusal == "measuring_review_allowed"


def test_an_unreadable_identity_refuses_the_call() -> None:
    state = installation()
    fact = Fact(state, observe(state))
    state.pop(CONFIG)
    with pytest.raises(ProbeKitRefused):
        fact.observe()
    assert fact.producer.refusal == "cni_unavailable"


def test_a_missed_per_call_deadline_refuses(monkeypatch: pytest.MonkeyPatch) -> None:
    state = installation()
    fact = Fact(state, observe(state))

    async def slow() -> str | None:
        await asyncio.sleep(1)
        return None

    fact.producer._least_privilege = slow  # type: ignore[assignment]
    monkeypatch.setitem(PRODUCER_CONSTANTS, "per_call_deadline_ms", 50)
    with pytest.raises(ProbeKitRefused):
        fact.observe()
    assert fact.producer.refusal == "deadline_missed"


# --- The fact document --------------------------------------------------------


def entry(case: int, sub_case: str, outcome: str = "pass") -> ObservationEntry:
    return ObservationEntry(
        case, sub_case, "none", "none", "none", outcome, "reviews_denied", "0" * 64
    )  # type: ignore[arg-type]


def test_the_fact_document_has_exactly_the_clause_1_members() -> None:
    identity = observe(installation())
    fact = managed_cni_fact(
        runtime_id="rt-1",
        runtime_generation="3",
        guard_config_digest=GUARD,
        identity=identity,
        entries=[entry(8, "rbac_policy"), entry(2, "rbac_refusal")],
    )
    assert sorted(fact) == sorted(
        [
            "fact_kind",
            "fact_version",
            "producer_kind",
            "producer_constants_digest",
            "runtime_id",
            "runtime_generation",
            "guard_config_digest",
            "identity",
            "identity_digest",
            "observations",
        ]
    )
    assert fact["fact_kind"] == "managed_cni_fact" and fact["fact_version"] == 1
    assert fact["producer_kind"] == PRODUCER_KIND
    assert fact["identity_digest"] == identity.digest == sha256_canonical(fact["identity"])
    assert {item["sub_case"] for item in fact["observations"]} == {"rbac_policy", "rbac_refusal"}
    # Canonical encoding: hashable, no floats.
    sha256_canonical(fact)


@pytest.mark.parametrize(
    "changes",
    [
        {"entries": [entry(8, "rbac_policy"), entry(8, "rbac_policy", "fail")]},
        {"guard_config_digest": "G"},
        {"runtime_id": ""},
        {"runtime_generation": 3},
    ],
)
def test_an_invalid_fact_refuses(changes: dict[str, Any]) -> None:
    values: dict[str, Any] = {
        "runtime_id": "rt-1",
        "runtime_generation": "3",
        "guard_config_digest": GUARD,
        "identity": observe(installation()),
        "entries": [entry(8, "rbac_policy")],
    }
    with pytest.raises(ProbeKitRefused):
        managed_cni_fact(**(values | changes))


# --- The bounded client -------------------------------------------------------


def test_the_client_reads_only_the_provider_entrys_paths() -> None:
    client = ManagedIdentityAPI.__new__(ManagedIdentityAPI)
    client._paths = PROVIDER.paths()

    async def fake_json(method: str, path: str, value: Any = None) -> dict[str, Any]:
        return {"path": path}

    client._json = fake_json  # type: ignore[method-assign]
    assert asyncio.run(client.read(CONFIG)) == {"path": CONFIG}
    for path in (
        "/api/v1/namespaces/kube-system/secrets/x",
        "/api/v1/namespaces/kube-system/configmaps/other",
        "/apis/apps/v1/namespaces/kube-system/replicasets",
    ):
        with pytest.raises(ProbeKitRefused):
            asyncio.run(client.read(path))
    assert asyncio.run(client.read_role("ClusterRole", "", "reads")) == {
        "path": "/apis/rbac.authorization.k8s.io/v1/clusterroles/reads"
    }
    with pytest.raises(ProbeKitRefused):
        asyncio.run(client.read_role("ClusterRole", "kube-system", "reads"))
    with pytest.raises(ProbeKitRefused):
        asyncio.run(client.read_role("Secret", "kube-system", "reads"))


def test_the_client_lists_namespaces_for_the_least_privilege_probe_list() -> None:
    client = ManagedIdentityAPI.__new__(ManagedIdentityAPI)
    seen: list[str] = []

    async def fake_json(method: str, path: str, value: Any = None) -> dict[str, Any]:
        seen.append(path)
        return {"items": [{"metadata": {"name": "default"}}], "metadata": {}}

    client._json = fake_json  # type: ignore[method-assign]
    assert asyncio.run(client.list_items("namespaces")) == [{"metadata": {"name": "default"}}]
    assert seen == ["/api/v1/namespaces?limit=500"]


def test_every_configmap_read_is_one_the_provider_entry_declares() -> None:
    # Clause 5 grants ConfigMap reads only by the names the provider table lists.
    configmaps = {path for path in PROVIDER.paths() if "/configmaps/" in path}
    assert configmaps == {
        f"/api/v1/namespaces/{ns}/configmaps/{name}" for ns, name in PROVIDER.configmaps
    }
    assert ("kube-system", "kube-proxy") in PROVIDER.configmaps
    renamed = ProviderEntry(
        provider="renamed",
        cni_plugin=PROVIDER.cni_plugin,
        policy_agent=PROVIDER.policy_agent,
        mode_keys=PROVIDER.mode_keys,
        addon_configmaps=(),
        kube_proxy_config="kube-proxy-config",
    )
    state = installation()
    state["/api/v1/namespaces/kube-system/configmaps/kube-proxy-config"] = state.pop(PROXY_CONFIG)
    reader = Reader(state)
    asyncio.run(observe_identity(reader, renamed, PLACEMENTS))
    assert set(reader.paths) <= renamed.paths()
