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
from .protected_kubernetes_projection import Projection
from .protected_launch_records import canonical_json

__all__ = ["FACT", "ControlObserver", "ControlReader"]

FACT = "control_profile"
_CONTAINER = r"(?:containerd|cri-o|docker)://[0-9a-f]{64}"


def _refuse() -> NoReturn:
    raise ControlInstallationRefused() from None


def _require(condition: bool) -> None:
    if not condition:
        _refuse()


class ControlReader(Protocol):
    """Reads under the provisioner's observer identity; every reply is a fresh read."""

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
        self._reader, self._profile, self._projection = reader, profile, projection
        self._roster, self._installed = roster, installed
        self._template = template

    @property
    def fact(self) -> str:
        return FACT

    async def observe(self) -> str:
        """Lowercase SHA-256 of this observation; refuses on any difference."""
        try:
            return await asyncio.to_thread(self._observe)
        except Exception:  # noqa: BLE001 - an unavailable observation is a refusal
            _refuse()

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
        """The one guard Pod, running the rendered container, ready and unrestarted."""
        selector = {GUARD_SERVER_LABEL: self._profile.digest[:32]}
        pods = self._reader.pods(v["control_namespace"], selector)
        _require(type(pods) is list and len(pods) == 1)
        pod = pods[0]
        meta, spec, status = pod["metadata"], pod["spec"], pod["status"]
        _require(meta.get("deletionTimestamp") is None and meta.get("labels") == selector)
        expected = self._template["spec"]
        _require(spec.get("automountServiceAccountToken") is False)
        _require(not spec.get("initContainers") and not spec.get("ephemeralContainers"))
        _require(spec["volumes"] == expected["volumes"])
        _require(spec["securityContext"] == expected["securityContext"])
        _require(len(spec["containers"]) == 1)
        container, rendered = spec["containers"][0], expected["containers"][0]
        for member in ("name", "image", "command", "volumeMounts", "securityContext"):
            _require(container.get(member) == rendered[member])
        _require(not container.get("args") and not container.get("env"))
        _require(not container.get("envFrom"))
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
