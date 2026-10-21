"""Local Cilium engine identity for the disposable local network producer, ADR 0236.

@spec PROTECTED-COMMISSIONING-5. ADR 0236 makes the local Cilium producer the
first per-CNI network producer, for disposable local installations only. This
module reads the identity of the Cilium engine a behavioral measurement runs
against:
- every node of the named minikube profile, all of them eligible;
- the agent and Envoy DaemonSets and the operator, rolled out, with the image
  digest and restart count of every pod;
- the complete agent configuration, which must enforce policy, outside audit
  mode, including Kubernetes NetworkPolicy objects.

These reads bind and invalidate a measurement. They never count as an
enforcement observation and can only refuse: ``pass`` comes from the probe
kit's behavioral cases alone. Cilium 1.16 and later no longer publish
per-endpoint policy state in CiliumEndpoint status, so endpoint enforcement is
observed only behaviorally.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from .managed_cni_cases import ResourceRef
from .managed_cni_kubernetes import MeasuringIdentityAPI
from .managed_cni_probe_kit import refuse, require, sha256_canonical

PROFILE_LABEL = "minikube.k8s.io/name"
CILIUM_POLICY_COLLECTIONS = (
    "/apis/cilium.io/v2/ciliumnetworkpolicies",
    "/apis/cilium.io/v2/ciliumclusterwidenetworkpolicies",
)
CILIUM_AGENT_OBJECTS = (
    ResourceRef("", "configmaps", "kube-system", "cilium-config"),
    ResourceRef("apps", "daemonsets", "kube-system", "cilium"),
    ResourceRef("apps", "daemonsets", "kube-system", "cilium-envoy"),
    ResourceRef("apps", "deployments", "kube-system", "cilium-operator"),
)
_SYSTEM = "/api/v1/namespaces/kube-system/"
_APPS = "/apis/apps/v1/namespaces/kube-system/"
_CONFIG = _SYSTEM + "configmaps/cilium-config"
# Workload path, pod label selector and the pods' controller kind.
_WORKLOADS = {
    "agent": (_APPS + "daemonsets/cilium", "k8s-app%3Dcilium", "DaemonSet"),
    "envoy": (_APPS + "daemonsets/cilium-envoy", "k8s-app%3Dcilium-envoy", "DaemonSet"),
    "operator": (_APPS + "deployments/cilium-operator", "io.cilium%2Fapp%3Doperator", "ReplicaSet"),
}
_PODS = _SYSTEM + "pods?labelSelector="
_PATHS = frozenset(
    {_CONFIG}
    | {path for path, _selector, _kind in _WORKLOADS.values()}
    | {_PODS + selector for _path, selector, _kind in _WORKLOADS.values()}
)
_DIGEST_IMAGE = re.compile(r".+@sha256:[0-9a-f]{64}")
_NODE_INFO = (
    "kubeletVersion",
    "kernelVersion",
    "containerRuntimeVersion",
    "osImage",
    "architecture",
    "operatingSystem",
)


class CiliumReader(Protocol):
    """Fixed read-only paths plus the node list of ``ProbeCluster``."""

    async def read(self, path: str) -> dict[str, Any] | None: ...

    async def list_items(self, collection: str) -> list[dict[str, Any]] | None: ...


class LocalCiliumAPI(MeasuringIdentityAPI):
    """The measuring identity's client plus the fixed Cilium reads of this module.

    The extra rights are reads of the CNI's own workloads and configuration,
    which ADR 0238 clause 5 already grants the measuring identity.
    """

    async def read(self, path: str) -> dict[str, Any] | None:
        require(path in _PATHS)
        return await self._json("GET", path)


@dataclass(frozen=True, slots=True)
class CiliumIdentity:
    """What binds a measurement: the served families and a digest of the engine."""

    families: tuple[str, ...]
    digest: str
    document: dict[str, Any] = field(repr=False, compare=False)


def _live(value: dict[str, Any]) -> dict[str, Any]:
    require(type(value) is dict and value["metadata"].get("deletionTimestamp") is None)
    return value


def _nodes(items: list[dict[str, Any]] | None, profile: str, eligible: Sequence[str]) -> list[Any]:
    require(items is not None)
    assert items is not None
    found = []
    for node in items:
        meta = _live(node)["metadata"]
        require(meta.get("labels", {}).get(PROFILE_LABEL) == profile)
        ready = [c for c in node["status"]["conditions"] if c["type"] == "Ready"]
        require(len(ready) == 1 and ready[0]["status"] == "True")
        info = node["status"]["nodeInfo"]
        found.append({"name": meta["name"], "uid": meta["uid"]} | {k: info[k] for k in _NODE_INFO})
    # A disposable local installation measures every node; none is left out.
    require(sorted(item["name"] for item in found) == sorted(eligible))
    require(len(eligible) == len(set(eligible)))
    return sorted(found, key=lambda item: str(item["name"]))


def _families(config: dict[str, Any] | None) -> tuple[dict[str, Any], tuple[str, ...]]:
    require(config is not None)
    assert config is not None
    data = _live(config)["data"]
    require(type(data) is dict and all(type(v) is str for v in data.values()))
    # "never" disables enforcement; audit mode logs policy verdicts and drops nothing.
    require(data.get("enable-policy") in ("default", "always"))
    require(data.get("policy-audit-mode", "false") == "false")
    # Without it Cilium ignores the NetworkPolicy objects the layout relies on.
    require(data.get("enable-k8s-networkpolicy", "true") == "true")
    families = tuple(f for f in ("ipv4", "ipv6") if data.get("enable-" + f) == "true")
    require(bool(families))
    return {"uid": config["metadata"]["uid"], "data": data}, families


def _rolled_out(name: str, value: dict[str, Any] | None, nodes: int) -> dict[str, Any]:
    require(value is not None)
    assert value is not None
    meta, status = _live(value)["metadata"], value["status"]
    require(type(meta["generation"]) is int and status["observedGeneration"] == meta["generation"])
    if value["kind"] == "DaemonSet":
        counts = ("desiredNumberScheduled", "numberReady", "updatedNumberScheduled")
        require(all(status[key] == nodes for key in (*counts, "numberAvailable")))
    else:
        require(value["kind"] == "Deployment" and type(value["spec"].get("replicas", 1)) is int)
        wanted = value["spec"].get("replicas", 1)
        require(wanted >= 1)
        require(all(status.get(k) == wanted for k in ("readyReplicas", "updatedReplicas")))
    template = value["spec"]["template"]["spec"]
    images = [c["image"] for c in [*template.get("initContainers", []), *template["containers"]]]
    require(all(type(i) is str and _DIGEST_IMAGE.fullmatch(i) is not None for i in images))
    return {"name": name, "uid": meta["uid"], "generation": meta["generation"], "images": images}


def _pods(
    listed: dict[str, Any] | None, kind: str, owner: str | None, nodes: Sequence[str]
) -> list[dict[str, Any]]:
    require(listed is not None)
    assert listed is not None
    require(not listed.get("metadata", {}).get("continue"))
    pods = []
    for pod in listed["items"]:
        meta = _live(pod)["metadata"]
        owners = [r for r in meta.get("ownerReferences", []) if r.get("controller") is True]
        require(len(owners) == 1 and owners[0]["kind"] == kind)
        require(owner is None or owners[0]["uid"] == owner)
        require(pod["status"]["phase"] == "Running" and pod["spec"]["nodeName"] in nodes)
        ready = [c for c in pod["status"]["conditions"] if c["type"] == "Ready"]
        require(len(ready) == 1 and ready[0]["status"] == "True")
        containers = []
        for item in pod["status"]["containerStatuses"]:
            require(item["ready"] is True and set(item["state"]) == {"running"})
            require(type(item["imageID"]) is str and "@sha256:" in item["imageID"])
            containers.append([item["name"], item["imageID"], item["restartCount"]])
        pods.append(
            {
                "node": pod["spec"]["nodeName"],
                "uid": meta["uid"],
                "owner": owners[0]["uid"],
                "containers": sorted(containers),
            }
        )
    require(bool(pods))
    return sorted(pods, key=lambda item: (str(item["node"]), str(item["uid"])))


async def observe_cilium(
    reader: CiliumReader,
    *,
    profile: str,
    eligible_nodes: Sequence[str],
) -> CiliumIdentity:
    """Read the engine identity once; raise ``ProbeKitRefused`` on any unbindable state."""
    try:
        nodes = _nodes(await reader.list_items("nodes"), profile, eligible_nodes)
        names = [item["name"] for item in nodes]
        config, families = _families(await reader.read(_CONFIG))
        workloads, pods = [], {}
        for name, (path, selector, kind) in _WORKLOADS.items():
            workload = _rolled_out(name, await reader.read(path), len(names))
            workloads.append(workload)
            owner = workload["uid"] if kind == "DaemonSet" else None
            found = _pods(await reader.read(_PODS + selector), kind, owner, names)
            if kind == "DaemonSet":
                # Exactly one agent and one proxy on every node.
                require([item["node"] for item in found] == names)
            pods[name] = found
    except (KeyError, TypeError, AttributeError, IndexError):
        refuse()
    document = {
        "profile": profile,
        "nodes": nodes,
        "config": config,
        "workloads": workloads,
        "pods": pods,
    }
    return CiliumIdentity(families, sha256_canonical(document), document)
