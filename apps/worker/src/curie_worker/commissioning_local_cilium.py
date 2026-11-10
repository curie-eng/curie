"""Local Cilium engine identity for the disposable local network producer, ADR 0236.

@spec PROTECTED-COMMISSIONING-5. ADR 0236 makes the local Cilium producer the
first per-CNI network producer, for disposable local installations only. This
module reads the identity of the Cilium engine a behavioral measurement runs
against:
- every node of the named minikube profile, all of them eligible;
- the agent and Envoy DaemonSets and the operator, rolled out, with the image
  digest and restart count of every pod;
- the complete agent configuration, which must enforce policy, outside audit
  mode, including Kubernetes NetworkPolicy objects;
- the operator's lineage: its pods belong to the Deployment's current
  ReplicaSet;
- the API server version and platform;
- the kube-proxy dataplane: its DaemonSet, the digest every pod runs and its
  configuration, or that it is absent.

These reads bind and invalidate a measurement. They never count as an
enforcement observation and can only refuse: ``pass`` comes from the probe
kit's behavioral cases alone. Cilium 1.16 and later no longer publish
per-endpoint policy state in CiliumEndpoint status, so endpoint enforcement is
observed only behaviorally.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from .managed_cni_cases import ResourceRef
from .managed_cni_identity import (
    KUBE_PROXY,
    KUBE_PROXY_CONFIG,
    KUBE_PROXY_PODS,
    VERSION_PATH,
    WorkloadReader,
    api_version,
    live,
    observe_kube_proxy,
    rolled_out,
    running_pods,
)
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
_OPERATOR_SETS = _APPS + "replicasets?labelSelector=io.cilium%2Fapp%3Doperator"
_REVISION = "deployment.kubernetes.io/revision"
_PATHS = frozenset(
    {_CONFIG, VERSION_PATH, KUBE_PROXY, KUBE_PROXY_PODS, KUBE_PROXY_CONFIG, _OPERATOR_SETS}
    | {path for path, _selector, _kind in _WORKLOADS.values()}
    | {_PODS + selector for _path, selector, _kind in _WORKLOADS.values()}
)
_NODE_INFO = (
    "kubeletVersion",
    "kernelVersion",
    "containerRuntimeVersion",
    "osImage",
    "architecture",
    "operatingSystem",
)


# The local reads are the shared workload reader's fixed paths.
CiliumReader = WorkloadReader


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


def _nodes(items: list[dict[str, Any]] | None, profile: str, eligible: Sequence[str]) -> list[Any]:
    require(items is not None)
    assert items is not None
    found = []
    for node in items:
        meta = live(node)["metadata"]
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
    data = live(config)["data"]
    require(type(data) is dict and all(type(v) is str for v in data.values()))
    # "never" disables enforcement; audit mode logs policy verdicts and drops nothing.
    require(data.get("enable-policy") in ("default", "always"))
    require(data.get("policy-audit-mode", "false") == "false")
    # Without it Cilium ignores the NetworkPolicy objects the layout relies on.
    require(data.get("enable-k8s-networkpolicy", "true") == "true")
    families = tuple(f for f in ("ipv4", "ipv6") if data.get("enable-" + f) == "true")
    require(bool(families))
    return {"uid": config["metadata"]["uid"], "data": data}, families


def _operator_set(listed: dict[str, Any] | None, deployment: dict[str, Any]) -> dict[str, Any]:
    """The Deployment's one current ReplicaSet; operator pods of any other refuse."""
    require(listed is not None)
    assert listed is not None
    require(not listed.get("metadata", {}).get("continue"))
    revision = deployment["metadata"]["annotations"][_REVISION]
    current = []
    for item in listed["items"]:
        meta = live(item)["metadata"]
        owners = [r for r in meta.get("ownerReferences", []) if r.get("controller") is True]
        require(len(owners) == 1 and owners[0]["kind"] == "Deployment")
        require(owners[0]["uid"] == deployment["metadata"]["uid"])
        if meta.get("annotations", {}).get(_REVISION) == revision:
            current.append(meta["uid"])
    require(len(current) == 1)
    return {"uid": current[0], "revision": revision}


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
        version = api_version(await reader.read(VERSION_PATH))
        workloads, pods, operator_set = [], {}, None
        for name, (path, selector, kind) in _WORKLOADS.items():
            value = await reader.read(path)
            workload = rolled_out(name, value, len(names))
            workloads.append(workload)
            if kind == "DaemonSet":
                owner = workload["uid"]
            else:
                assert value is not None
                operator_set = _operator_set(await reader.read(_OPERATOR_SETS), value)
                owner = operator_set["uid"]
            found = running_pods(await reader.read(_PODS + selector), kind, owner, names)
            if kind == "DaemonSet":
                # Exactly one agent and one proxy on every node.
                require([item["node"] for item in found] == names)
            pods[name] = found
        kube_proxy = await observe_kube_proxy(reader, names)
    except (KeyError, TypeError, AttributeError, IndexError):
        refuse()
    document = {
        "profile": profile,
        "nodes": nodes,
        "config": config,
        "version": version,
        "workloads": workloads,
        "operator_set": operator_set,
        "pods": pods,
        "kube_proxy": kube_proxy,
    }
    return CiliumIdentity(families, sha256_canonical(document), document)
