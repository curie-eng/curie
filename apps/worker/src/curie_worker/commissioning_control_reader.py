"""Provisioner-side control observer wiring, @spec PROTECTED-COMMISSIONING-5.

The observer uses an explicit owned cluster configuration, never the ambient
context. Its identity needs get on the roster, ReplicaSets and /version, and
list on guard Pods and EndpointSlices. In particular, get on the guard Secret
reads **tls.key**, its private serving key: this is privileged provisioner-side
RBAC, never a grant to the candidate or control Pod. No object body is logged.

The command transport's subprocess deadline bounds a complete read (including
streaming), unlike a socket inactivity timeout. ControlObserver runs these sync
reads in a thread and bounds the whole observation separately. The provisioner
must supply the observer's dedicated kubeconfig when composing LocalCluster.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any, NoReturn

from curie_protected_hooks.commissioning_local_installation import OWNER_LABEL, LocalCluster
from curie_protected_hooks.commissioning_local_ownership import OwnershipLedger

from .commissioning_control_installation import (
    ControlInstallationRefused,
    InstalledControl,
    object_key,
)
from .commissioning_control_observer import ControlObserver
from .commissioning_control_profile import ControlProfile
from .managed_cni_probe_kit import PRODUCER_CONSTANTS
from .protected_kubernetes_projection import Projection

_NAME = re.compile(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?")
_RESOURCES = {
    ("v1", "Namespace"): "namespaces",
    ("v1", "Secret"): "secrets",
    ("v1", "Service"): "services",
    ("apps/v1", "Deployment"): "deployments.apps",
    ("apps/v1", "ReplicaSet"): "replicasets.apps",
    (
        "admissionregistration.k8s.io/v1",
        "ValidatingWebhookConfiguration",
    ): "validatingwebhookconfigurations.admissionregistration.k8s.io",
}


def _refuse() -> NoReturn:
    raise ControlInstallationRefused() from None


def _require(value: bool) -> None:
    if not value:
        _refuse()


def _name(value: str) -> str:
    _require(type(value) is str and _NAME.fullmatch(value) is not None)
    return value


class KubernetesControlReader:
    """Fresh closed-roster reads, each with both API and hard process deadlines."""

    def __init__(self, cluster: LocalCluster) -> None:
        _require(type(cluster) is LocalCluster)
        self._cluster = cluster

    @property
    def request_timeout(self) -> float:
        return min(4.0, float(PRODUCER_CONSTANTS["per_call_deadline_ms"]) / 1000)

    def _read(self, *args: str, absent: bool = False) -> dict[str, Any] | None:
        try:
            raw = self._cluster.kubectl(
                *args,
                "--request-timeout=" + str(self.request_timeout) + "s",
                "--output=json",
                timeout=self.request_timeout,
            )
            if absent and not raw.strip():
                return None
            _require(len(raw) <= 1048576)
            value = json.loads(raw)
            _require(type(value) is dict)
            return dict(value)
        except Exception:  # noqa: BLE001 - no object, credential or command diagnostic escapes
            _refuse()

    def read(self, key: str) -> dict[str, Any] | None:
        try:
            api_version, kind, namespace, name = key.rsplit("/", 3)
            resource = _RESOURCES[(api_version, kind)]
            _require(
                bool(namespace) == (kind not in {"Namespace", "ValidatingWebhookConfiguration"})
            )
            ns = ("--namespace", _name(namespace)) if namespace else ()
            return self._read(
                "get", resource + "/" + _name(name), *ns, "--ignore-not-found", absent=True
            )
        except Exception:  # noqa: BLE001
            _refuse()

    def version(self) -> dict[str, Any]:
        value = self._read("version")
        _require(value is not None and type(value.get("serverVersion")) is dict)
        assert value is not None
        return dict(value["serverVersion"])

    def _list(self, resource: str, namespace: str, selector: str) -> list[dict[str, Any]]:
        value = self._read("get", resource, "--namespace", _name(namespace), "--selector", selector)
        _require(value is not None)
        assert value is not None
        # Never mistake a partial list for the entire live roster.
        _require(not (value.get("metadata") or {}).get("continue"))
        items = value.get("items")
        _require(type(items) is list and all(type(item) is dict for item in items))
        assert isinstance(items, list)
        return list(items)

    def pods(self, namespace: str, labels: Mapping[str, str]) -> list[dict[str, Any]]:
        _require(bool(labels))
        _require(
            all(
                re.fullmatch(r"[a-zA-Z0-9./_-]+", k) and re.fullmatch(r"[a-zA-Z0-9._-]+", v)
                for k, v in labels.items()
            )
        )
        return self._list(
            "pods", namespace, ",".join(k + "=" + v for k, v in sorted(labels.items()))
        )

    def endpoint_slices(self, namespace: str, service: str) -> list[dict[str, Any]]:
        return self._list(
            "endpointslices.discovery.k8s.io",
            namespace,
            "kubernetes.io/service-name=" + _name(service),
        )


def installed_control(
    ledger: OwnershipLedger, roster: tuple[dict[str, Any], ...]
) -> InstalledControl:
    """Only created ledger rows with this owner's recorded UIDs form the roster."""
    try:
        _require(type(ledger) is OwnershipLedger and type(roster) is tuple and bool(roster))
        entries = {entry.key: entry for entry in ledger.entries() if entry.kind == "kubernetes"}
        keys = [object_key(document) for document in roster]
        _require(len(set(keys)) == len(keys))
        uids = []
        for document, key in zip(roster, keys, strict=True):
            _require(document["metadata"]["labels"][OWNER_LABEL] == ledger.owner)
            entry = entries[key]
            _require(entry.state == "created" and entry.identity.get("owner") == ledger.owner)
            uid = entry.identity.get("uid")
            _require(type(uid) is str and 0 < len(uid) <= 128)
            assert uid is not None
            uids.append((key, uid))
        return InstalledControl(tuple(uids))
    except Exception:  # noqa: BLE001
        _refuse()


def control_observer(
    cluster: LocalCluster,
    *,
    profile: ControlProfile,
    projection: Projection,
    roster: tuple[dict[str, Any], ...],
) -> ControlObserver:
    """Build the provisioner's observer; never accept caller-supplied installed UIDs."""
    return ControlObserver(
        KubernetesControlReader(cluster),
        profile=profile,
        projection=projection,
        roster=roster,
        installed=installed_control(cluster.ledger, roster),
    )
