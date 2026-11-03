"""The control observer: the ``control_profile`` fact, reobserved per call. ADR 0236.

@spec PROTECTED-COMMISSIONING-5. ADR 0236 lists the control observer among the
provisioner producers. On every call it reads the live control roster and
refuses unless the installation still is the one the profile renders:

- the API server reports a Kubernetes minor the profile pins;
- each roster object is present, not deleting, carries the UID its create
  response carried, and equals its rendering except for a closed list of
  server-set members, each at its measured default;
- exactly one guard Pod runs the profile's image with the rendered command,
  mounts and security context, is ready, and is the single ready endpoint of
  the guard Service on the guard port.

What this detects and what it cannot prevent: the webhook configuration
cannot be guarded by any admission webhook, so its removal or change, or a
replaced guard, is detected here on the next call and refuses; it is not
prevented. A request the API server admitted while the guard was absent is
not visible here. The fact carries no authority; the signed channel binds its
digest to one guard call.

The tolerated defaults were measured by server dry run on Kubernetes 1.35.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
import threading
from collections.abc import Callable, Mapping
from typing import Any, NoReturn, Protocol

from .commissioning_control_installation import (
    ControlInstallationRefused,
    InstalledControl,
    object_key,
)
from .commissioning_control_profile import (
    GUARD_SERVER_LABEL,
    GUARD_TARGET_PORT,
    ControlProfile,
    require_supported_version,
)
from .managed_cni_probe_kit import PRODUCER_CONSTANTS
from .protected_kubernetes_projection import Projection
from .protected_launch_records import canonical_json

__all__ = ["FACT", "ControlObserver", "ControlReader"]

FACT = "control_profile"
_CONTAINER = r"(?:containerd|cri-o|docker)://[0-9a-f]{64}"


def _deadline() -> float:
    return float(PRODUCER_CONSTANTS["per_call_deadline_ms"]) / 1000


def _refuse() -> NoReturn:
    raise ControlInstallationRefused() from None


def _require(condition: bool) -> None:
    if not condition:
        _refuse()


class ControlReader(Protocol):
    """Reads under the provisioner's observer identity; every reply is a fresh read.

    Each request gives up after ``request_timeout`` seconds on its own. The
    observer's deadline bounds the call, but a blocking read cannot be
    interrupted from outside its thread, so the reader's own timeout is what
    ends that thread.
    """

    @property
    def request_timeout(self) -> float:
        """Seconds after which one request raises; at most the per-call deadline."""
        ...

    def read(self, key: str) -> dict[str, Any] | None:
        """The live object at an ``object_key``, or ``None`` when absent."""
        ...

    def version(self) -> dict[str, Any]:
        """The API server's ``/version`` reply."""
        ...

    def pods(self, namespace: str, labels: Mapping[str, str]) -> list[dict[str, Any]]:
        """Every Pod in ``namespace`` whose labels include ``labels``."""
        ...

    def endpoint_slices(self, namespace: str, service: str) -> list[dict[str, Any]]:
        """Every EndpointSlice of ``service``."""
        ...


Allowed = Callable[[Any], bool]


def _any(_: Any) -> bool:
    return True


def _equals(value: Any) -> Allowed:
    return lambda observed: type(observed) is type(value) and observed == value


_METADATA: dict[tuple[str, ...], Allowed] = {
    ("metadata", "creationTimestamp"): _any,
    ("metadata", "uid"): _any,
    ("metadata", "resourceVersion"): _any,
    ("metadata", "generation"): _any,
    ("metadata", "managedFields"): _any,
}
# Members the API server sets on each roster kind, at their measured defaults.
_DEFAULTS: dict[str, dict[tuple[str, ...], Allowed]] = {
    "Namespace": {
        ("spec",): _equals({"finalizers": ["kubernetes"]}),
        ("status",): _equals({"phase": "Active"}),
    },
    "Secret": {},
    "Service": {
        ("spec", "clusterIP"): _any,
        ("spec", "clusterIPs"): _any,
        ("spec", "ipFamilies"): _any,
        ("spec", "ipFamilyPolicy"): _any,
        ("spec", "internalTrafficPolicy"): _equals("Cluster"),
        ("spec", "sessionAffinity"): _equals("None"),
        ("status",): _equals({"loadBalancer": {}}),
    },
    "Deployment": {
        # The controller records the template revision it rolled out.
        ("metadata", "annotations"): lambda value: (
            type(value) is dict and set(value) <= {"deployment.kubernetes.io/revision"}
        ),
        ("spec", "progressDeadlineSeconds"): _equals(600),
        ("spec", "revisionHistoryLimit"): _equals(10),
        ("spec", "strategy"): _equals(
            {"rollingUpdate": {"maxSurge": "25%", "maxUnavailable": "25%"}, "type": "RollingUpdate"}
        ),
        ("spec", "template", "spec", "containers", "*", "ports", "*", "protocol"): _equals("TCP"),
        ("spec", "template", "spec", "containers", "*", "readinessProbe", "failureThreshold"): (
            _equals(3)
        ),
        ("spec", "template", "spec", "containers", "*", "readinessProbe", "successThreshold"): (
            _equals(1)
        ),
        ("spec", "template", "spec", "containers", "*", "readinessProbe", "timeoutSeconds"): (
            _equals(1)
        ),
        ("spec", "template", "spec", "containers", "*", "resources"): _equals({}),
        ("spec", "template", "spec", "containers", "*", "terminationMessagePath"): _equals(
            "/dev/termination-log"
        ),
        ("spec", "template", "spec", "containers", "*", "terminationMessagePolicy"): _equals(
            "File"
        ),
        ("spec", "template", "spec", "dnsPolicy"): _equals("ClusterFirst"),
        ("spec", "template", "spec", "restartPolicy"): _equals("Always"),
        ("spec", "template", "spec", "schedulerName"): _equals("default-scheduler"),
        ("spec", "template", "spec", "terminationGracePeriodSeconds"): _equals(30),
        ("status",): _any,
    },
    # Every member is rendered, so the webhook configuration compares exactly.
    "ValidatingWebhookConfiguration": {},
}


_TEMPLATE = ("spec", "template", "spec")
# The guard Pod's spec is its template's, plus the template defaults above and
# what the scheduler and admission add to a controller-created Pod, measured on
# Kubernetes 1.35: the node, the default service account, the default
# priority and the not-ready and unreachable tolerations.
_POD_DEFAULTS: dict[tuple[str, ...], Allowed] = {
    path[len(_TEMPLATE) :]: allowed
    for path, allowed in _DEFAULTS["Deployment"].items()
    if path[: len(_TEMPLATE)] == _TEMPLATE
}
_POD_DEFAULTS.update(
    {
        ("nodeName",): lambda value: type(value) is str and 0 < len(value) <= 253,
        ("serviceAccount",): _equals("default"),
        ("serviceAccountName",): _equals("default"),
        ("priority",): _equals(0),
        ("preemptionPolicy",): _equals("PreemptLowerPriority"),
        ("tolerations",): _equals(
            [
                {
                    "effect": "NoExecute",
                    "key": key,
                    "operator": "Exists",
                    "tolerationSeconds": 300,
                }
                for key in ("node.kubernetes.io/not-ready", "node.kubernetes.io/unreachable")
            ]
        ),
    }
)
# A controller-created Pod's metadata members; any other member refuses.
_POD_METADATA = frozenset(
    {
        "creationTimestamp",
        "generateName",
        "generation",
        "labels",
        "managedFields",
        "name",
        "namespace",
        "ownerReferences",
        "resourceVersion",
        "uid",
    }
)


def _controller(meta: dict[str, Any], kind: str) -> dict[str, Any]:
    """The one owner reference: the controlling ``apps/v1`` object of ``kind``."""
    references: list[Any] = meta.get("ownerReferences") or []
    _require(type(references) is list and len(references) == 1)
    reference: dict[str, Any] = references[0]
    _require(
        reference.get("apiVersion") == "apps/v1"
        and reference.get("kind") == kind
        and reference.get("controller") is True
    )
    return reference


def _match(
    observed: Any,
    rendered: Any,
    defaults: Mapping[tuple[str, ...], Allowed],
    path: tuple[str, ...] = (),
) -> None:
    """``observed`` equals ``rendered`` but for members in ``defaults`` it may add."""
    if type(rendered) is dict:
        _require(type(observed) is dict)
        for key, value in observed.items():
            member = (*path, key)
            if key in rendered:
                _match(value, rendered[key], defaults, member)
            else:
                allowed = defaults.get(member)
                _require(allowed is not None and allowed(value))
        _require(set(rendered) <= set(observed))
    elif type(rendered) is list:
        _require(type(observed) is list and len(observed) == len(rendered))
        for item, expected in zip(observed, rendered, strict=True):
            _match(item, expected, defaults, (*path, "*"))
    else:
        _require(type(observed) is type(rendered) and observed == rendered)


class ControlObserver:
    """The ``control_profile`` fact producer over one installed control roster."""

    def __init__(
        self,
        reader: ControlReader,
        *,
        profile: ControlProfile,
        projection: Projection,
        roster: tuple[dict[str, Any], ...],
        installed: InstalledControl,
    ) -> None:
        try:
            _require(type(profile) is ControlProfile and type(projection) is Projection)
            _require(type(installed) is InstalledControl and type(roster) is tuple)
            keys = tuple(object_key(document) for document in roster)
            _require(tuple(key for key, _ in installed.uids) == keys)
            _require(all(document["kind"] in _DEFAULTS for document in roster))
            v = profile.as_dict()
            _require(v["projection_digest"] == projection.digest)
            deployments = [d for d in roster if d["kind"] == "Deployment"]
            _require(len(deployments) == 1)
            template = deployments[0]["spec"]["template"]
            _require(template["metadata"]["labels"] == {GUARD_SERVER_LABEL: profile.digest[:32]})
            _require(len(template["spec"]["containers"]) == 1)
            _require(template["spec"]["containers"][0]["image"] == v["guard"]["image"])
        except ControlInstallationRefused:
            raise
        except Exception:  # noqa: BLE001 - no document detail
            _refuse()
        timeout: Any = getattr(reader, "request_timeout", None)
        _require(type(timeout) in (int, float) and 0 < timeout <= _deadline())
        self._reader, self._profile, self._projection = reader, profile, projection
        self._roster, self._installed = roster, installed
        self._template = template
        self._busy = threading.Lock()

    @property
    def fact(self) -> str:
        return FACT

    async def observe(self) -> str:
        """Lowercase SHA-256 of this observation; refuses on any difference.

        Bounded by the producers' per-call deadline. The reads run in one
        worker thread that holds this observer while it lives: a call made
        while an earlier, timed-out read is still running refuses at once
        instead of starting another thread.
        """
        if not self._busy.acquire(blocking=False):
            _refuse()
        try:
            worker = asyncio.to_thread(self._observe_and_release)
        except BaseException:
            self._busy.release()
            raise
        try:
            async with asyncio.timeout(_deadline()):
                return await worker
        except Exception:  # noqa: BLE001 - an unavailable or late observation is a refusal
            _refuse()

    def _observe_and_release(self) -> str:
        try:
            return self._observe()
        finally:
            self._busy.release()

    def _observe(self) -> str:
        v = self._profile.as_dict()
        version = self._reader.version()
        require_supported_version(self._profile, version)
        for document in self._roster:
            key = object_key(document)
            live = self._reader.read(key)
            _require(type(live) is dict)
            assert live is not None
            meta: dict[str, Any] = live.get("metadata") or {}
            _require(meta.get("deletionTimestamp") is None)
            _require(meta.get("uid") == self._installed.uid(key))
            defaults = {**_METADATA, **_DEFAULTS[document["kind"]]}
            if document["kind"] == "Namespace":
                name = document["metadata"]["name"]
                defaults[("metadata", "labels", "kubernetes.io/metadata.name")] = _equals(name)
            _match(live, document, defaults)
        pod = self._guard_pod(v)
        self._guard_route(v, pod)
        status = pod["status"]["containerStatuses"][0]
        document = {
            "profile_sha256": self._profile.digest,
            "projection_sha256": self._projection.digest,
            "kubernetes_version": version["gitVersion"],
            "objects": [list(pair) for pair in self._installed.uids],
            "guard_pod_uid": pod["metadata"]["uid"],
            "guard_container_id": status["containerID"],
        }
        return hashlib.sha256(canonical_json(document)).hexdigest()

    def _guard_pod(self, v: dict[str, Any]) -> dict[str, Any]:
        """The one guard Pod: its template's spec, created by the installed Deployment.

        The spec is compared closed against the rendered template, like the
        roster: an added member such as ``hostNetwork`` refuses. The Pod's
        controller must be a ReplicaSet whose controller is the installed
        Deployment, so a Pod created by anyone else and adopted by label is
        refused unless it is indistinguishable from the template.
        """
        namespace = v["control_namespace"]
        selector = {GUARD_SERVER_LABEL: self._profile.digest[:32]}
        pods = self._reader.pods(namespace, selector)
        _require(type(pods) is list and len(pods) == 1)
        pod = pods[0]
        meta, status = pod["metadata"], pod["status"]
        _require(set(meta) <= _POD_METADATA and meta.get("namespace") == namespace)
        replica_set = _controller(meta, "ReplicaSet")
        owner = self._reader.read(
            "/".join(("apps/v1", "ReplicaSet", namespace, replica_set["name"]))
        )
        _require(type(owner) is dict)
        assert owner is not None
        owner_meta = owner["metadata"]
        _require(owner_meta.get("uid") == replica_set.get("uid"))
        _require(owner_meta.get("deletionTimestamp") is None)
        deployment = _controller(owner_meta, "Deployment")
        deployment_key = object_key(self._roster[4])
        _require(deployment.get("uid") == self._installed.uid(deployment_key))
        _require(deployment.get("name") == self._roster[4]["metadata"]["name"])
        template_hash = owner_meta["labels"]["pod-template-hash"]
        _require(meta.get("labels") == {**selector, "pod-template-hash": template_hash})
        _match(pod["spec"], self._template["spec"], _POD_DEFAULTS)
        _require(status.get("phase") == "Running")
        _require(not status.get("initContainerStatuses"))
        _require(not status.get("ephemeralContainerStatuses"))
        statuses = status.get("containerStatuses")
        _require(type(statuses) is list and len(statuses) == 1)
        observed = statuses[0]
        digest = v["guard"]["image"].rsplit("@", 1)[1]
        _require(observed.get("name") == "guard" and observed.get("ready") is True)
        _require(observed.get("restartCount") == 0)
        _require(type(observed.get("imageID")) is str and observed["imageID"].endswith(digest))
        _require(set(observed.get("state") or {}) == {"running"})
        _require(
            type(observed.get("containerID")) is str
            and re.fullmatch(_CONTAINER, observed["containerID"]) is not None
        )
        return pod

    def _guard_route(self, v: dict[str, Any], pod: dict[str, Any]) -> None:
        """The guard Service's only ready endpoint is this Pod on the guard port."""
        slices = self._reader.endpoint_slices(v["control_namespace"], v["guard"]["service_name"])
        _require(type(slices) is list and len(slices) >= 1)
        endpoints: list[dict[str, Any]] = []
        for item in slices:
            ports = item.get("ports") or []
            _require(ports == [{"name": "https", "port": GUARD_TARGET_PORT, "protocol": "TCP"}])
            endpoints.extend(item.get("endpoints") or [])
        _require(len(endpoints) == 1)
        endpoint = endpoints[0]
        conditions = endpoint.get("conditions") or {}
        _require(conditions.get("ready") is True and conditions.get("terminating") is not True)
        _require(endpoint.get("addresses") == [pod["status"]["podIP"]])
        ref = endpoint.get("targetRef") or {}
        _require(ref.get("kind") == "Pod" and ref.get("uid") == pod["metadata"]["uid"])
        _require(ref.get("name") == pod["metadata"]["name"])
        _require(ref.get("namespace") == v["control_namespace"])
