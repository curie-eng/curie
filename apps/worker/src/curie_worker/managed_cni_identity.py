"""The CNI identity fact of the managed behavioral producer, ADR 0238.

@spec MANAGED-CNI-1 @spec MANAGED-CNI-9. Configuration never decides
enforcement; it only binds and invalidates a measurement. This module reads
the identity ADR 0238 lists under "What invalidates the qualification":

- the CNI plugin and policy-agent image digests, from the digests their pods
  actually run;
- the agent's enforcement mode, from the workload's arguments and environment;
- a digest of the managed add-on configuration;
- the kube-proxy dataplane: absent, or its image digests, mode and
  configuration;
- the API server's ``/version`` ``gitVersion`` and ``platform``;
- the served IP families;
- every node eligible for protected pods: name, UID, node image, kernel,
  kubelet and container runtime.

Eligibility is computed from Node labels and taints against the protected pod
templates' node selectors, required node affinity and tolerations. A Node's UID
is part of its identity, so a replaced node with the same name is a new,
unmeasured node.

``ManagedCniFact`` is internal evidence, not a closed record: no broker key
stores it, no production reader parses it and no record grammar gains a member
for it. Its CNI channel digest binds ``producer_kind``, the producer constants
digest and ``identity_digest``. ``CniIdentityFact`` reobserves the identity,
``sha256(G)`` and the measuring identity's least privilege on every call and
refuses on any change, including an added, removed or replaced eligible node.

The workload readers here are shared with the local Cilium producer. The reads
stay inside the measuring identity's clause 5 rights: Nodes, Pods, DaemonSets
and Deployments, the provider table's ConfigMaps by name, and ``/version``. No
ReplicaSet is read, so a Deployment's current pods are identified by one
``pod-template-hash`` across a fully rolled out Deployment.
"""

from __future__ import annotations

import asyncio
import ipaddress
import re
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol
from urllib.parse import quote

from .managed_cni_cases import ResourceAttributes
from .managed_cni_kubernetes import MeasuringIdentityAPI
from .managed_cni_probe_kit import (
    PRODUCER_CONSTANTS,
    PRODUCER_KIND,
    ObservationEntry,
    ProbeKitRefused,
    address_family,
    entries_document,
    producer_constants_digest,
    refuse,
    require,
    sha256_canonical,
)

_SYSTEM = "/api/v1/namespaces/kube-system/"
_APPS = "/apis/apps/v1/namespaces/kube-system/"
VERSION_PATH = "/version"
KUBE_PROXY = _APPS + "daemonsets/kube-proxy"
KUBE_PROXY_PODS = _SYSTEM + "pods?labelSelector=k8s-app%3Dkube-proxy"
KUBE_PROXY_CONFIG = _SYSTEM + "configmaps/kube-proxy"
# The other forms kube-proxy takes: a DaemonSet under another name, and static pods.
KUBE_PROXY_SETS = _APPS + "daemonsets?labelSelector=k8s-app%3Dkube-proxy"
KUBE_PROXY_STATIC = _SYSTEM + "pods?labelSelector=component%3Dkube-proxy"
_KUBE_PROXY_CHECKED = [
    "daemonset:kube-system/kube-proxy",
    "daemonsets:k8s-app=kube-proxy",
    "pods:component=kube-proxy",
    "pods:k8s-app=kube-proxy",
]
DIGEST_IMAGE = re.compile(r".+@sha256:[0-9a-f]{64}")
_IMAGE_DIGEST = re.compile(r"@(sha256:[0-9a-f]{64})$")
_NAME = re.compile(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?")
_ROLE = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._-]{0,252}")
_SELECTOR = re.compile(r"[A-Za-z0-9./_-]+=[A-Za-z0-9._-]+(?:,[A-Za-z0-9./_-]+=[A-Za-z0-9._-]+)*")
_MODE = re.compile(r'^\s*mode:\s*"?([A-Za-z]*)"?\s*$', re.MULTILINE)
_TEMPLATE_HASH = "pod-template-hash"
_BLOCKING = frozenset({"NoSchedule", "NoExecute"})
FACT_KIND = "managed_cni_fact"
FACT_VERSION = 1
IDENTITY_MEMBERS = (
    "cni_plugin_images",
    "policy_agent_images",
    "enforcement_mode",
    "addon_configuration_digest",
    "dataplane_mode",
    "control_plane_version",
    "ip_families",
    "eligible_nodes",
)


class WorkloadReader(Protocol):
    """Fixed read-only paths plus the node list of ``ProbeCluster``."""

    async def read(self, path: str) -> dict[str, Any] | None: ...

    async def list_items(self, collection: str) -> list[dict[str, Any]] | None: ...


# --- Workload readers shared with the local Cilium producer ------------------


def live(value: dict[str, Any]) -> dict[str, Any]:
    require(type(value) is dict and value["metadata"].get("deletionTimestamp") is None)
    return value


def rolled_out(
    name: str, value: dict[str, Any] | None, nodes: int | None, *, pinned: bool = True
) -> dict[str, Any]:
    """A DaemonSet or Deployment whose current template runs everywhere it is scheduled.

    ``nodes`` is the exact DaemonSet size, or ``None`` when only a consistent
    rollout is required.
    """
    require(value is not None)
    assert value is not None
    meta, status = live(value)["metadata"], value["status"]
    require(type(meta["generation"]) is int and status["observedGeneration"] == meta["generation"])
    if value["kind"] == "DaemonSet":
        counts = ("desiredNumberScheduled", "numberReady", "updatedNumberScheduled")
        wanted = status["desiredNumberScheduled"] if nodes is None else nodes
        require(type(wanted) is int and wanted >= 1)
        require(all(status[key] == wanted for key in (*counts, "numberAvailable")))
    else:
        require(value["kind"] == "Deployment" and type(value["spec"].get("replicas", 1)) is int)
        wanted = value["spec"].get("replicas", 1)
        require(wanted >= 1)
        require(all(status.get(k) == wanted for k in ("readyReplicas", "updatedReplicas")))
        if nodes is None:
            # Without a ReplicaSet read, no surplus pod of an older template may exist.
            require(status.get("replicas") == wanted)
    template = value["spec"]["template"]["spec"]
    images = [c["image"] for c in [*template.get("initContainers", []), *template["containers"]]]
    require(all(type(i) is str for i in images))
    # Cilium's templates pin digests. Other templates name tags; their pods' digests bind them.
    require(not pinned or all(DIGEST_IMAGE.fullmatch(i) is not None for i in images))
    return {"name": name, "uid": meta["uid"], "generation": meta["generation"], "images": images}


def running_pods(
    listed: dict[str, Any] | None, kind: str, owner: str | None, nodes: Sequence[str] | None
) -> list[dict[str, Any]]:
    """Every listed pod runs and is ready, under one controller of ``kind``.

    ``owner`` is the controller's UID, or ``None`` when the caller checks lineage
    itself; ``nodes`` bounds where pods may run, or ``None`` for anywhere.
    """
    require(listed is not None)
    assert listed is not None
    require(not listed.get("metadata", {}).get("continue"))
    pods = []
    for pod in listed["items"]:
        meta = live(pod)["metadata"]
        owners = [r for r in meta.get("ownerReferences", []) if r.get("controller") is True]
        require(len(owners) == 1 and owners[0]["kind"] == kind)
        require(owner is None or owners[0]["uid"] == owner)
        require(pod["status"]["phase"] == "Running")
        require(nodes is None or pod["spec"]["nodeName"] in nodes)
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


def api_version(value: dict[str, Any] | None) -> dict[str, Any]:
    require(value is not None)
    assert value is not None
    found = {key: value.get(key) for key in ("gitVersion", "gitCommit", "platform")}
    require(type(found["gitVersion"]) is str and type(found["platform"]) is str)
    return found


def _config_data(config: dict[str, Any] | None) -> dict[str, Any]:
    require(config is not None)
    assert config is not None
    data = live(config)["data"]
    require(type(data) is dict and all(type(v) is str for v in data.values()))
    return {"uid": config["metadata"]["uid"], "data": data}


async def observe_kube_proxy(reader: WorkloadReader, names: Sequence[str]) -> dict[str, Any] | None:
    """kube-proxy's dataplane on exactly ``names``, or ``None`` when neither it nor any of
    its pods exists. The local Cilium producer's reading; the managed identity reads
    every form kube-proxy takes (``_kube_proxy_dataplane``)."""
    daemonset = await reader.read(KUBE_PROXY)
    pods = await reader.read(KUBE_PROXY_PODS)
    if daemonset is None:
        require(pods is not None and not pods["items"])
        return None
    workload = rolled_out("kube-proxy", daemonset, len(names), pinned=False)
    found = running_pods(pods, "DaemonSet", workload["uid"], names)
    require([item["node"] for item in found] == list(names))
    return {
        "workload": workload,
        "pods": found,
        "config": _config_data(await reader.read(KUBE_PROXY_CONFIG)),
    }


# --- The provider table entry and eligibility --------------------------------


@dataclass(frozen=True, slots=True)
class WorkloadRef:
    """One CNI, policy-agent or dataplane workload the provider table names."""

    kind: Literal["DaemonSet", "Deployment"]
    namespace: str
    name: str
    selector: str  # the pods' label selector, ``key=value[,key=value]``
    # The containers that belong to this role; empty means every container.
    containers: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        require(self.kind in ("DaemonSet", "Deployment"))
        require(
            _NAME.fullmatch(self.namespace) is not None and _NAME.fullmatch(self.name) is not None
        )
        require(_SELECTOR.fullmatch(self.selector) is not None)

    @property
    def path(self) -> str:
        plural = "daemonsets" if self.kind == "DaemonSet" else "deployments"
        return f"/apis/apps/v1/namespaces/{self.namespace}/{plural}/{self.name}"

    @property
    def pods_path(self) -> str:
        return f"/api/v1/namespaces/{self.namespace}/pods?labelSelector=" + quote(
            self.selector, safe=""
        )


@dataclass(frozen=True, slots=True)
class ModeKey:
    """Where the agent declares its enforcement mode: one argument flag or one env name."""

    workload: str
    container: str
    flag: str = ""  # ``--enforcing-mode``, read as ``--flag=value`` or ``--flag value``
    env: str = ""

    def __post_init__(self) -> None:
        require(bool(self.flag) != bool(self.env))
        require(not self.flag or self.flag.startswith("--"))


@dataclass(frozen=True, slots=True)
class ProviderEntry:
    """One provider table row: the workloads and configuration to read."""

    provider: str
    cni_plugin: tuple[WorkloadRef, ...]
    policy_agent: tuple[WorkloadRef, ...]
    mode_keys: tuple[ModeKey, ...]
    addon_configmaps: tuple[tuple[str, str], ...]
    # kube-proxy's ConfigMap in kube-system; providers name it differently.
    kube_proxy_config: str = "kube-proxy"
    # Where the provider exposes them only through its cloud API, the add-on
    # configuration and the node image IDs come from the read-only describe
    # call. A declared source that is missing refuses; nothing falls back.
    cloud_addon: bool = False
    cloud_node_images: bool = False

    def __post_init__(self) -> None:
        require(bool(self.provider) and bool(self.cni_plugin) and bool(self.policy_agent))
        require(bool(self.mode_keys))
        names = {ref.name for ref in (*self.cni_plugin, *self.policy_agent)}
        require(all(key.workload in names for key in self.mode_keys))
        for namespace, name in self.configmaps:
            require(_NAME.fullmatch(namespace) is not None and _NAME.fullmatch(name) is not None)

    @property
    def configmaps(self) -> tuple[tuple[str, str], ...]:
        """Every ConfigMap this entry reads. Clause 5 grants these by name and no others,
        so the measuring identity's CNI ConfigMap Role is built from this list."""
        return (*self.addon_configmaps, ("kube-system", self.kube_proxy_config))

    def paths(self) -> frozenset[str]:
        """Every path ``observe_identity`` reads with this entry."""
        refs = (*self.cni_plugin, *self.policy_agent)
        return frozenset(
            {VERSION_PATH, KUBE_PROXY, KUBE_PROXY_PODS, KUBE_PROXY_SETS, KUBE_PROXY_STATIC}
            | {ref.path for ref in refs}
            | {ref.pods_path for ref in refs}
            | {f"/api/v1/namespaces/{ns}/configmaps/{name}" for ns, name in self.configmaps}
        )


@dataclass(frozen=True, slots=True)
class Toleration:
    key: str = ""
    operator: Literal["Equal", "Exists"] = "Equal"
    value: str = ""
    effect: str = ""

    def tolerates(self, taint: Mapping[str, Any]) -> bool:
        if self.effect and self.effect != taint.get("effect"):
            return False
        if self.operator == "Exists":
            return not self.key or self.key == taint.get("key")
        return self.key == taint.get("key") and self.value == taint.get("value", "")


@dataclass(frozen=True, slots=True)
class Requirement:
    key: str
    operator: Literal["In", "NotIn", "Exists", "DoesNotExist"]
    values: tuple[str, ...] = ()

    def matches(self, labels: Mapping[str, str]) -> bool:
        if self.operator == "Exists":
            return self.key in labels
        if self.operator == "DoesNotExist":
            return self.key not in labels
        if self.operator == "In":
            return labels.get(self.key) in self.values
        return labels.get(self.key) not in self.values


@dataclass(frozen=True, slots=True)
class Placement:
    """Where one protected pod template measured by G can be scheduled."""

    node_selector: Mapping[str, str]
    # Required node affinity: any term matches when all its requirements do.
    terms: tuple[tuple[Requirement, ...], ...]
    tolerations: tuple[Toleration, ...]

    @classmethod
    def of(cls, spec: Mapping[str, Any]) -> Placement:
        """Parse a pod spec's scheduling constraints; anything unsupported refuses."""
        try:
            selector = dict(spec.get("nodeSelector") or {})
            require(all(type(k) is str and type(v) is str for k, v in selector.items()))
            affinity = (spec.get("affinity") or {}).get("nodeAffinity") or {}
            required = affinity.get("requiredDuringSchedulingIgnoredDuringExecution") or {}
            terms = []
            for term in required.get("nodeSelectorTerms") or []:
                # Field selectors and numeric comparisons are not modelled: refuse.
                require(not term.get("matchFields"))
                expressions = term.get("matchExpressions") or []
                require(bool(expressions))
                terms.append(
                    tuple(
                        Requirement(e["key"], e["operator"], tuple(e.get("values") or ()))
                        for e in expressions
                    )
                )
            for term in terms:
                for item in term:
                    require(item.operator in ("In", "NotIn", "Exists", "DoesNotExist"))
            tolerations = tuple(
                Toleration(
                    t.get("key", ""),
                    t.get("operator", "Equal"),
                    t.get("value", ""),
                    t.get("effect", ""),
                )
                for t in spec.get("tolerations") or []
            )
            require(all(t.operator in ("Equal", "Exists") for t in tolerations))
        except (KeyError, TypeError, AttributeError):
            refuse()
        return cls(selector, tuple(terms), tolerations)

    def admits(self, node: Mapping[str, Any]) -> bool:
        labels = node["metadata"].get("labels") or {}
        if any(labels.get(key) != value for key, value in self.node_selector.items()):
            return False
        if self.terms and not any(all(r.matches(labels) for r in term) for term in self.terms):
            return False
        if node.get("spec", {}).get("unschedulable"):
            return False
        for taint in node.get("spec", {}).get("taints") or []:
            if taint.get("effect") in _BLOCKING and not any(
                t.tolerates(taint) for t in self.tolerations
            ):
                return False
        return True


def eligible_nodes(
    nodes: Iterable[dict[str, Any]], placements: Sequence[Placement]
) -> list[dict[str, Any]]:
    """The nodes any protected template can be scheduled on."""
    require(bool(placements))
    return [live(node) for node in nodes if any(p.admits(node) for p in placements)]


# --- The identity object ------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CniIdentity:
    """The clause 1 ``identity`` object and its digest."""

    document: dict[str, Any] = field(repr=False)
    digest: str

    @classmethod
    def of(cls, document: dict[str, Any]) -> CniIdentity:
        require(sorted(document) == sorted(IDENTITY_MEMBERS))
        return cls(document, sha256_canonical(document))

    @property
    def families(self) -> tuple[str, ...]:
        return tuple(self.document["ip_families"])


def _image_digest(image_id: str) -> str:
    match = _IMAGE_DIGEST.search(image_id)
    require(match is not None)
    assert match is not None
    return match[1]


def _pod_digests(
    items: Iterable[Mapping[str, Any]], containers: Sequence[str] = ()
) -> list[dict[str, str]]:
    """One digest per container across the pods, init and ephemeral containers included.

    A CNI often installs its plugin binary from an init container, so a rollout
    that changes only that image changes the identity. A completed init
    container must have succeeded; a sidecar init container runs.
    """
    digests: dict[str, set[str]] = {}
    for pod in items:
        status = pod["status"]
        for item in status.get("initContainerStatuses") or []:
            state = item["state"]
            require(
                state.get("terminated", {}).get("exitCode") == 0
                or (set(state) == {"running"} and item.get("ready") is True)
            )
        statuses = [
            *status["containerStatuses"],
            *(status.get("initContainerStatuses") or []),
            *(status.get("ephemeralContainerStatuses") or []),
        ]
        for item in statuses:
            require(type(item["imageID"]) is str)
            digests.setdefault(item["name"], set()).add(_image_digest(item["imageID"]))
    if containers:
        require(set(containers) <= set(digests))
        digests = {name: found for name, found in digests.items() if name in containers}
    # Every pod of a rolled-out workload runs one digest per container.
    require(bool(digests) and all(len(found) == 1 for found in digests.values()))
    return [
        {"container": name, "image_digest": next(iter(found))} for name, found in digests.items()
    ]


async def _workload_images(
    reader: WorkloadReader, ref: WorkloadRef, eligible: Sequence[str]
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    """One workload's per-container image digests, and its template for mode reads."""
    value = await reader.read(ref.path)
    workload = rolled_out(ref.name, value, None, pinned=False)
    assert value is not None
    require(value["metadata"].get("name", ref.name) == ref.name)
    listed = await reader.read(ref.pods_path)
    if ref.kind == "DaemonSet":
        pods = running_pods(listed, "DaemonSet", workload["uid"], None)
        # The agent runs on every node a protected pod can be scheduled on.
        require(set(eligible) <= {item["node"] for item in pods})
    else:
        pods = running_pods(listed, "ReplicaSet", None, None)
        assert listed is not None
        hashes = {
            item["metadata"].get("labels", {}).get(_TEMPLATE_HASH) for item in listed["items"]
        }
        require(len(hashes) == 1 and None not in hashes)
        require(len(pods) == value["spec"].get("replicas", 1))
    assert listed is not None
    images = [
        {"workload": ref.name} | item for item in _pod_digests(listed["items"], ref.containers)
    ]
    return images, value["spec"]["template"]["spec"]


def _mode_value(template: Mapping[str, Any], key: ModeKey) -> Any:
    containers = [c for c in template["containers"] if c["name"] == key.container]
    require(len(containers) == 1)
    container = containers[0]
    if key.env:
        found = [e for e in container.get("env") or [] if e["name"] == key.env]
        require(len(found) <= 1)
        if not found:
            return None
        return found[0]["value"] if "value" in found[0] else {"value_from": found[0]["valueFrom"]}
    args = [*(container.get("command") or []), *(container.get("args") or [])]
    values = []
    for index, arg in enumerate(args):
        if arg.startswith(key.flag + "="):
            values.append(arg.split("=", 1)[1])
        elif arg == key.flag:
            require(index + 1 < len(args))
            values.append(args[index + 1])
    require(len(values) <= 1)
    return values[0] if values else None


def _node_identity(
    node: Mapping[str, Any], images: Mapping[str, str], *, described: bool
) -> dict[str, Any]:
    meta, info = node["metadata"], node["status"]["nodeInfo"]
    # Serverless pod runtimes and Windows nodes refuse until a producer measures them.
    require(info["operatingSystem"] == "linux")
    ready = [c for c in node["status"]["conditions"] if c["type"] == "Ready"]
    require(len(ready) == 1 and ready[0]["status"] == "True")
    return {
        "node_name": meta["name"],
        "node_uid": meta["uid"],
        # A declared cloud node image never falls back to the Node's osImage.
        "node_image": images[meta["name"]] if described else info["osImage"],
        "kernel_version": info["kernelVersion"],
        "kubelet_version": info["kubeletVersion"],
        "container_runtime_version": info["containerRuntimeVersion"],
    }


def _served_families(nodes: Iterable[Mapping[str, Any]]) -> list[str]:
    """Clause 3 as the node objects show it: InternalIP and pod CIDR families."""
    served: set[str] = set()
    for node in nodes:
        for address in node["status"].get("addresses") or []:
            if address.get("type") == "InternalIP":
                served.add(address_family(address["address"]))
        for cidr in node.get("spec", {}).get("podCIDRs") or []:
            try:
                network = ipaddress.ip_network(cidr, strict=False)
            except ValueError:
                refuse()
            served.add("ipv4" if network.version == 4 else "ipv6")
    require(bool(served))
    return sorted(served)


class CloudDescriber(Protocol):
    """The optional read-only cloud principal of clause 5.

    ``describe`` returns the managed add-on configuration as the provider
    reports it, with secrets and provider request IDs already removed, and the
    provider's node image ID per node name where it gives one.
    """

    async def describe(self) -> tuple[dict[str, Any], dict[str, str]]: ...


async def _described(
    provider: ProviderEntry, cloud: CloudDescriber | None, eligible: Sequence[str]
) -> tuple[dict[str, Any], dict[str, str]]:
    """The cloud describe the provider entry declares: exactly that, or a refusal."""
    declared = provider.cloud_addon or provider.cloud_node_images
    require((cloud is not None) == declared)
    if cloud is None:
        return {}, {}
    try:
        addon, node_images = await cloud.describe()
    except Exception:  # noqa: BLE001 -- an unavailable describe is a refusal.
        refuse()
    require(type(addon) is dict and type(node_images) is dict)
    require(bool(addon) == provider.cloud_addon)
    if provider.cloud_node_images:
        require(set(eligible) <= set(node_images))
        require(all(type(node_images[name]) is str and node_images[name] for name in eligible))
    else:
        require(not node_images)
    return addon, node_images


def _proxy_modes(values: Iterable[str]) -> str:
    modes = set(values)
    require(len(modes) <= 1)
    return next(iter(modes), "")


def _flag_values(command: Sequence[str], flag: str) -> list[str]:
    found = []
    for index, arg in enumerate(command):
        if arg.startswith(flag + "="):
            found.append(arg.split("=", 1)[1])
        elif arg == flag:
            require(index + 1 < len(command))
            found.append(command[index + 1])
    return found


async def _kube_proxy_dataplane(
    reader: WorkloadReader, provider: ProviderEntry, eligible: Sequence[str]
) -> dict[str, Any]:
    """kube-proxy in whichever form it runs; ``absent`` only after every form was checked."""
    named = await reader.read(KUBE_PROXY)
    labelled = await reader.read(KUBE_PROXY_SETS)
    app_pods = await reader.read(KUBE_PROXY_PODS)
    static = await reader.read(KUBE_PROXY_STATIC)
    for listed in (labelled, app_pods, static):
        require(listed is not None and not listed.get("metadata", {}).get("continue"))
    assert labelled is not None and app_pods is not None and static is not None
    sets = {item["metadata"]["uid"]: item for item in labelled["items"]}
    if named is not None:
        sets[named["metadata"]["uid"]] = named
    require(len(sets) <= 1)
    if sets:
        # One form only: a DaemonSet beside static kube-proxy pods is ambiguous.
        require(not static["items"])
        workload = rolled_out("kube-proxy", next(iter(sets.values())), None, pinned=False)
        pods = running_pods(app_pods, "DaemonSet", workload["uid"], None)
        require(set(eligible) <= {item["node"] for item in pods})
        config = await reader.read(_SYSTEM + "configmaps/" + provider.kube_proxy_config)
        data = _config_data(config)["data"]
        return {
            "form": "daemonset",
            "images": _sorted(_pod_digests(app_pods["items"])),
            "mode": _proxy_modes(found for v in data.values() for found in _MODE.findall(v)),
            "configuration_digest": sha256_canonical(data),
        }
    if static["items"]:
        # Static pods are mirror pods owned by their Node; their flags are the configuration.
        uids = {item["metadata"]["uid"] for item in static["items"]}
        require(all(item["metadata"]["uid"] in uids for item in app_pods["items"]))
        pods = running_pods(static, "Node", None, None)
        require(set(eligible) <= {item["node"] for item in pods})
        commands = []
        for item in static["items"]:
            for container in item["spec"]["containers"]:
                commands.append([*(container.get("command") or []), *(container.get("args") or [])])
        return {
            "form": "static",
            "images": _sorted(_pod_digests(static["items"])),
            "mode": _proxy_modes(
                value for command in commands for value in _flag_values(command, "--proxy-mode")
            ),
            "configuration_digest": sha256_canonical(sorted(commands)),
        }
    require(not app_pods["items"])
    return {"form": "absent", "checked": list(_KUBE_PROXY_CHECKED)}


async def observe_identity(
    reader: WorkloadReader,
    provider: ProviderEntry,
    placements: Sequence[Placement],
    *,
    cloud: CloudDescriber | None = None,
) -> CniIdentity:
    """Read the identity once; raise ``ProbeKitRefused`` on any unbindable state."""
    try:
        listed = await reader.list_items("nodes")
        require(listed is not None)
        assert listed is not None
        eligible_items = eligible_nodes(listed, placements)
        require(bool(eligible_items))
        eligible = sorted(item["metadata"]["name"] for item in eligible_items)
        require(len(eligible) == len(set(eligible)))
        addon, node_images = await _described(provider, cloud, eligible)
        roles: dict[str, list[dict[str, str]]] = {
            "cni_plugin_images": [],
            "policy_agent_images": [],
        }
        templates: dict[str, Any] = {}
        for member, refs in (
            ("cni_plugin_images", provider.cni_plugin),
            ("policy_agent_images", provider.policy_agent),
        ):
            for ref in refs:
                images, templates[ref.name] = await _workload_images(reader, ref, eligible)
                roles[member] += images
        mode = [
            {
                "workload": key.workload,
                "container": key.container,
                "key": key.flag or key.env,
                "value": _mode_value(templates[key.workload], key),
            }
            for key in provider.mode_keys
        ]
        configmaps = []
        for namespace, name in provider.addon_configmaps:
            config = await reader.read(f"/api/v1/namespaces/{namespace}/configmaps/{name}")
            data = _config_data(config)["data"]
            configmaps.append({"namespace": namespace, "name": name, "data": data})
        dataplane = {"kube_proxy": await _kube_proxy_dataplane(reader, provider, eligible)}
        version = api_version(await reader.read(VERSION_PATH))
        document = {
            "cni_plugin_images": _sorted(roles["cni_plugin_images"]),
            "policy_agent_images": _sorted(roles["policy_agent_images"]),
            "enforcement_mode": _sorted(mode),
            "addon_configuration_digest": sha256_canonical(
                {"configmaps": configmaps, "cloud": addon}
            ),
            "dataplane_mode": dataplane,
            "control_plane_version": {
                "gitVersion": version["gitVersion"],
                "platform": version["platform"],
            },
            "ip_families": _served_families(eligible_items),
            "eligible_nodes": sorted(
                (
                    _node_identity(node, node_images, described=provider.cloud_node_images)
                    for node in eligible_items
                ),
                key=lambda item: str(item["node_name"]),
            ),
        }
    except (KeyError, TypeError, AttributeError, IndexError, ValueError):
        refuse()
    return CniIdentity.of(document)


def _sorted(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = [sha256_canonical(item) for item in items]
    require(len(keys) == len(set(keys)))
    return sorted(items, key=sha256_canonical)


def identity_change(before: CniIdentity, after: CniIdentity) -> str | None:
    """Why ``after`` invalidates a campaign measured under ``before``, or ``None``."""
    if before.digest == after.digest:
        return None
    old = {item["node_name"]: item for item in before.document["eligible_nodes"]}
    new = {item["node_name"]: item for item in after.document["eligible_nodes"]}
    if set(new) - set(old):
        return "eligible_node_added"
    if set(old) - set(new):
        return "eligible_node_removed"
    if any(old[name]["node_uid"] != new[name]["node_uid"] for name in old):
        return "eligible_node_replaced"
    for member in IDENTITY_MEMBERS:
        if before.document[member] != after.document[member]:
            return member + "_changed"
    refuse()  # equal members with different digests cannot happen


# --- The fact and its CNI channel digest -------------------------------------


def cni_digest(identity_digest: str) -> str:
    """The ADR 0236 CNI channel digest (clause 1)."""
    return sha256_canonical(
        {
            "producer_kind": PRODUCER_KIND,
            "producer_constants_digest": producer_constants_digest(),
            "identity_digest": identity_digest,
        }
    )


def managed_cni_fact(
    *,
    runtime_id: str,
    runtime_generation: str,
    guard_config_digest: str,
    identity: CniIdentity,
    entries: Iterable[ObservationEntry],
) -> dict[str, Any]:
    """The clause 1 ``ManagedCniFact`` document: internal evidence, never a record."""
    require(type(runtime_id) is str and bool(runtime_id))
    require(type(runtime_generation) is str and bool(runtime_generation))
    require(re.fullmatch(r"[0-9a-f]{64}", guard_config_digest) is not None)
    observations = entries_document(entries)
    keys = [
        tuple(item[k] for k in ("case", "sub_case", "family", "placement", "source_kind"))
        for item in observations
    ]
    require(len(keys) == len(set(keys)))
    return {
        "fact_kind": FACT_KIND,
        "fact_version": FACT_VERSION,
        "producer_kind": PRODUCER_KIND,
        "producer_constants_digest": producer_constants_digest(),
        "runtime_id": runtime_id,
        "runtime_generation": runtime_generation,
        "guard_config_digest": guard_config_digest,
        "identity": identity.document,
        "identity_digest": identity.digest,
        "observations": observations,
    }


class ManagedIdentityAPI(MeasuringIdentityAPI):
    """The measuring identity's client plus the identity and RBAC reads of clause 5."""

    def __init__(self, *args: Any, provider: ProviderEntry, **options: Any) -> None:
        super().__init__(*args, **options)
        self._paths = provider.paths()

    async def read(self, path: str) -> dict[str, Any] | None:
        require(path in self._paths)
        return await self._json("GET", path)

    async def review(
        self, user: str, groups: Sequence[str], attributes: Mapping[str, str]
    ) -> bool | None:
        return await self.access_review(user, groups, ResourceAttributes(**attributes))

    async def read_role(self, kind: str, namespace: str, name: str) -> dict[str, Any] | None:
        # RBAC names are path segments; Kubernetes' bootstrap roles carry ``:``.
        require(_ROLE.fullmatch(name) is not None)
        base = "/apis/rbac.authorization.k8s.io/v1/"
        if kind == "ClusterRole":
            require(namespace == "")
            return await self._json("GET", base + "clusterroles/" + name)
        require(kind == "Role" and _NAME.fullmatch(namespace) is not None)
        return await self._json("GET", base + f"namespaces/{namespace}/roles/{name}")


class CniIdentityFact:
    """The ADR 0236 ``cni`` fact, reobserved on every guard call; implements ``FactProducer``.

    Each call rereads the identity and refuses on any change from the campaign's,
    including the eligible node set; rereads ``sha256(G)``; and reruns the
    measuring identity's least-privilege self-check. All of it must finish
    within the producer's per-call deadline.
    """

    fact = "cni"

    def __init__(
        self,
        reader: WorkloadReader,
        provider: ProviderEntry,
        placements: Sequence[Placement],
        *,
        campaign: CniIdentity,
        guard_config_digest: str,
        guard: Callable[[], Awaitable[str]],
        least_privilege: Callable[[], Awaitable[str | None]],
        cloud: CloudDescriber | None = None,
    ) -> None:
        require(re.fullmatch(r"[0-9a-f]{64}", guard_config_digest) is not None)
        self._reader, self._provider, self._placements = reader, provider, tuple(placements)
        self._campaign, self._guard_digest = campaign, guard_config_digest
        self._guard, self._least_privilege, self._cloud = guard, least_privilege, cloud
        self.refusal: str | None = None

    async def _observe(self) -> str:
        try:
            current = await observe_identity(
                self._reader, self._provider, self._placements, cloud=self._cloud
            )
        except ProbeKitRefused:
            self.refusal = "cni_unavailable"
            raise
        self.refusal = identity_change(self._campaign, current)
        require(self.refusal is None)
        if await self._guard() != self._guard_digest:
            self.refusal = "guard_config_changed"
            refuse()
        self.refusal = await self._least_privilege()
        require(self.refusal is None)
        return cni_digest(current.digest)

    async def observe(self) -> str:
        self.refusal = None
        try:
            async with asyncio.timeout(PRODUCER_CONSTANTS["per_call_deadline_ms"] / 1000):
                return await self._observe()
        except ProbeKitRefused:
            raise
        except TimeoutError:
            self.refusal = "deadline_missed"
            refuse()
        except Exception:  # noqa: BLE001 -- an unavailable observation is a refusal.
            self.refusal = self.refusal or "cni_unavailable"
            refuse()
