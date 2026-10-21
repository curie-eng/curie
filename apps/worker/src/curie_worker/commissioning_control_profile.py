"""Closed control profile: the admission policy the guard enforces, ADR 0236.

@spec PROTECTED-COMMISSIONING-8. The guard permits only the measured image,
service account, mounts, Secret references, network and ephemeral container
policy. This module is the closed record that fixes that policy for one
installation, and the pure rendering of the cluster objects that install it:
the two namespaces, the guard Service and the ValidatingWebhookConfiguration.

@spec PROTECTED-COMMISSIONING-2. The profile is measured into G: the
provisioner sets G's ``admission_policy_sha256`` to ``ControlProfile.digest``,
and M binds ``sha256(G)``. ``validate_control_profile_binding`` checks that
equality and the projection binding; it authenticates and authorizes nothing.

Rendering is source only. Whether the API server calls the guard, and whether
the live objects still equal this rendering, is observed elsewhere: the
webhook configuration cannot be guarded by any admission webhook, so its
removal or change is detected by reobservation, not prevented. The pinned
Kubernetes minors bound the cluster versions whose admitted object shapes the
guard is built for; any other version refuses. The measurement on each pinned
minor is recorded with the admission server.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any

from cryptography import x509
from cryptography.hazmat.primitives.serialization import Encoding
from curie_protected_hooks.authority_records import _scalar

from .protected_kubernetes_projection import Projection
from .protected_launch_records import (
    _decode,
    _pattern,
    _require,
    canonical_json,
    safe_validation,
)

# The Kubernetes minors whose admitted object shapes the guard is built for. A
# minor joins only together with a cluster measurement of the guard on it (the
# admission server's evidence), never by default.
SUPPORTED_KUBERNETES_MINORS = ("1.34", "1.35")
SLOT_POLICY = "launch-id-names-v1"
RECORD_TYPE = "commissioning_control_profile"
GUARD_PATH = "/validate"
GUARD_PORT = 443
GUARD_TARGET_PORT = 9443
# The API server sets this label from the namespace name and refuses changes,
# so a selector on it cannot be escaped by relabelling the namespace.
NAMESPACE_NAME_LABEL = "kubernetes.io/metadata.name"
GUARD_SERVER_LABEL = "curietech.ai/protected-guard"
_MAX_BYTES = 16384
_TOP = {
    "schema_version",
    "record_type",
    "projection_digest",
    "authority_domain_id",
    "kubernetes_minors",
    "workload_namespace",
    "control_namespace",
    "guard",
    "principals",
    "slot_policy",
}
_GUARD = {"name", "service_name", "image", "server_ca_sha256", "timeout_seconds"}
_PRINCIPALS = {
    "mediator",
    "provisioner",
    "endpoint_controllers",
    "pod_binders",
    "pod_status_controllers",
}
# The controllers that write a launch Service's Endpoints and EndpointSlices:
# per-controller service accounts, or the controller manager's own identity
# when it runs without service account credentials.
_KUBE_SYSTEM_SA = r"system:serviceaccount:kube-system:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
_CONTROLLER = _KUBE_SYSTEM_SA + r"|system:kube-controller-manager"
# The scheduler binds launch Pods to nodes and records scheduling conditions.
_BINDER = _KUBE_SYSTEM_SA + r"|system:kube-scheduler"
_LABEL = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
# A webhook name is a fully qualified DNS subdomain with at least three labels.
_WEBHOOK = r"(?=.{1,253}$)" + _LABEL + r"(?:\." + _LABEL + r"){2,}"
_IMAGE = r"[a-z0-9][a-z0-9./:_-]{0,1950}@sha256:[0-9a-f]{64}"
_USERNAME = r"[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}"
_RESERVED_USERS = ("system:anonymous", "system:apiserver", "system:kube-scheduler")


class ControlProfileInvalid(ValueError):
    """Fixed refusal; never echoes a profile member, file path or certificate."""

    def __init__(self) -> None:
        super().__init__("Invalid protected control profile")


def _closed(value: Any, keys: set[str]) -> dict[str, Any]:
    _require(type(value) is dict and value.keys() == keys)
    result: dict[str, Any] = value
    return result


def _validate(value: Any) -> None:
    v = _closed(value, _TOP)
    _scalar(v["schema_version"], "version")
    _require(type(v["record_type"]) is str and v["record_type"] == RECORD_TYPE)
    _require(type(v["slot_policy"]) is str and v["slot_policy"] == SLOT_POLICY)
    _scalar(v["projection_digest"], "sha256")
    _scalar(v["authority_domain_id"], "uuid")
    minors = v["kubernetes_minors"]
    _require(type(minors) is list and 1 <= len(minors) <= len(SUPPORTED_KUBERNETES_MINORS))
    _require(all(type(m) is str and m in SUPPORTED_KUBERNETES_MINORS for m in minors))
    _require(minors == sorted(set(minors), key=SUPPORTED_KUBERNETES_MINORS.index))
    for key in ("workload_namespace", "control_namespace"):
        _pattern(v[key], _LABEL)
    _require(v["workload_namespace"] != v["control_namespace"])
    guard = _closed(v["guard"], _GUARD)
    _pattern(guard["name"], _WEBHOOK)
    _pattern(guard["service_name"], _LABEL)
    _pattern(guard["image"], _IMAGE)
    _scalar(guard["server_ca_sha256"], "sha256")
    _require(type(guard["timeout_seconds"]) is int and 1 <= guard["timeout_seconds"] <= 10)
    principals = _closed(v["principals"], _PRINCIPALS)
    system: set[str] = set()
    for name, pattern, low in (
        ("endpoint_controllers", _CONTROLLER, 1),
        ("pod_binders", _BINDER, 1),
        ("pod_status_controllers", _CONTROLLER, 0),
    ):
        users = principals[name]
        _require(type(users) is list and low <= len(users) <= 4)
        for user in users:
            _pattern(user, pattern)
        _require(users == sorted(set(users)))
        system.update(users)
    for name in ("mediator", "provisioner"):
        user = principals[name]
        _pattern(user, _USERNAME)
        _require(user not in _RESERVED_USERS and not user.startswith("system:node:"))
        _require(user not in system)
    _require(principals["mediator"] != principals["provisioner"])


@dataclass(frozen=True, slots=True, repr=False)
class ControlProfile:
    """Canonical closed profile bytes; a digest is identity, not authentication."""

    _canonical_bytes: bytes = field(repr=False)

    @safe_validation(ControlProfileInvalid)
    def __post_init__(self) -> None:
        value = _decode(self._canonical_bytes, _MAX_BYTES)
        _validate(value)
        raw = canonical_json(value)
        _require(raw == self._canonical_bytes)
        object.__setattr__(self, "_canonical_bytes", raw)

    def __repr__(self) -> str:
        return "<ControlProfile>"

    @property
    def canonical_bytes(self) -> bytes:
        return self._canonical_bytes

    @property
    def digest(self) -> str:
        """The value G records as ``admission_policy_sha256``."""
        return hashlib.sha256(self._canonical_bytes).hexdigest()

    def as_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = json.loads(self._canonical_bytes)
        return value

    @property
    def guard_name(self) -> str:
        """The exact name the API server prints when this guard refuses."""
        name: str = self.as_dict()["guard"]["name"]
        return name


def parse_control_profile(raw: bytes) -> ControlProfile:
    """Parse canonical bytes only; no provisioner selection or authority."""
    return ControlProfile(raw)


@safe_validation(ControlProfileInvalid)
def validate_control_profile_binding(
    profile: ControlProfile, *, admission_policy_sha256: str, projection: Projection
) -> None:
    """G names this profile and the profile names this projection.

    ``admission_policy_sha256`` is G's member of that name, read by the caller
    from the parsed guard configuration. Equality binds; it proves no live
    enforcement.
    """
    _require(type(profile) is ControlProfile and type(projection) is Projection)
    _scalar(admission_policy_sha256, "sha256")
    _require(profile.digest == admission_policy_sha256)
    v, p = profile.as_dict(), projection.as_dict()
    _require(v["projection_digest"] == projection.digest)
    _require(v["workload_namespace"] == p["namespace"])
    _require(v["authority_domain_id"] == p["authority_domain_id"])
    runner = f"system:serviceaccount:{p['namespace']}:{p['service_account']}"
    _require(runner not in (v["principals"]["mediator"], v["principals"]["provisioner"]))


@safe_validation(ControlProfileInvalid)
def require_supported_version(profile: ControlProfile, version: object) -> None:
    """Refuse an API server ``/version`` reply outside the profile's minors."""
    _require(type(profile) is ControlProfile)
    if type(version) is not dict:
        raise ControlProfileInvalid()
    major, minor, git = (_string(version.get(key)) for key in ("major", "minor", "gitVersion"))
    _require(major == "1")
    # Some distributions append "+" to the minor; the git version is the authority.
    match = re.fullmatch(
        r"v1\.(0|[1-9][0-9]{0,3})\.(0|[1-9][0-9]{0,3})(?:[-+][\x21-\x7e]{1,128})?", git
    )
    if match is None:
        raise ControlProfileInvalid()
    _require(minor.rstrip("+") == match[1])
    _require(f"1.{match[1]}" in profile.as_dict()["kubernetes_minors"])


def _string(value: object) -> str:
    if type(value) is not str:
        raise ControlProfileInvalid()
    return value


def _ca_digest(pem: bytes) -> str:
    _require(type(pem) is bytes and 0 < len(pem) <= 16384)
    cert = x509.load_pem_x509_certificate(pem)
    _require(cert.public_bytes(Encoding.PEM) == pem)
    _require(cert.extensions.get_extension_for_class(x509.BasicConstraints).value.ca)
    return hashlib.sha256(cert.public_bytes(Encoding.DER)).hexdigest()


def server_ca_digest(pem: bytes) -> str:
    """The profile's ``server_ca_sha256`` of one canonical PEM CA certificate."""
    try:
        return _ca_digest(pem)
    except ControlProfileInvalid:
        raise
    except Exception:  # noqa: BLE001 - fixed refusal, no certificate detail
        raise ControlProfileInvalid() from None


def _rule(
    group: str,
    resources: list[str],
    operations: list[str],
    scope: str = "Namespaced",
    versions: str = "v1",
) -> dict[str, Any]:
    return {
        "apiGroups": [group],
        "apiVersions": [versions],
        "operations": operations,
        "resources": resources,
        "scope": scope,
    }


_CHANGE = ["CREATE", "UPDATE", "DELETE"]
# Every request that can place or change a workload, its credentials, its
# identity or its network in the workload namespace. The guard decides each;
# anything it does not recognize refuses.
_RULES = (
    _rule("", ["pods"], ["CREATE", "UPDATE"]),
    _rule("", ["pods/ephemeralcontainers", "pods/resize"], ["UPDATE"]),
    _rule("", ["pods/exec", "pods/attach", "pods/portforward"], ["CONNECT"]),
    _rule("", ["secrets", "services"], ["CREATE", "UPDATE"]),
    _rule("", ["serviceaccounts"], ["CREATE", "UPDATE", "DELETE"]),
    _rule("", ["serviceaccounts/token"], ["CREATE"]),
    _rule("networking.k8s.io", ["networkpolicies"], ["CREATE", "UPDATE", "DELETE"]),
    _rule("rbac.authorization.k8s.io", ["roles", "rolebindings"], ["CREATE", "UPDATE", "DELETE"]),
    _rule("", ["namespaces"], ["UPDATE"], "Cluster"),
    # A launch Service selects its runner by label, so whoever writes its
    # endpoints decides where the mediator dials. Only the endpoint controllers
    # named in the profile may.
    _rule("", ["endpoints"], ["CREATE", "UPDATE"]),
    _rule("discovery.k8s.io", ["endpointslices"], ["CREATE", "UPDATE"]),
    # Pod status carries the Pod IPs and readiness that steer the launch
    # Service, and a binding chooses the node: only the Pod's own node, the
    # named binders and the named status controllers may write them.
    _rule("", ["pods/status"], ["UPDATE"]),
    _rule("", ["pods/binding", "bindings"], ["CREATE"]),
    # Namespaced policy kinds of common CNIs change workload networking as
    # NetworkPolicy does; every write refuses. A CNI kind not listed here is
    # covered only by the network fact's reobservation of the policy set.
    _rule("cilium.io", ["ciliumnetworkpolicies"], _CHANGE, versions="*"),
    _rule("crd.projectcalico.org", ["networkpolicies"], _CHANGE, versions="*"),
    _rule("projectcalico.org", ["networkpolicies"], _CHANGE, versions="*"),
    _rule("vpcresources.k8s.aws", ["securitygrouppolicies"], _CHANGE, versions="*"),
)


def render_control_objects(
    profile: ControlProfile, *, projection: Projection, server_ca_pem: bytes
) -> tuple[dict[str, Any], ...]:
    """Render the namespaces, guard Service and webhook configuration.

    The webhook fails closed (``failurePolicy: Fail``): an unreachable,
    slow or erroring guard refuses every matched request. It selects the
    workload namespace by its immutable name label, never the control
    namespace, so the guard's own Pod is never subject to itself.
    """
    try:
        return _render(profile, projection, server_ca_pem)
    except ControlProfileInvalid:
        raise
    except Exception:  # noqa: BLE001 - fixed refusal, no input detail
        raise ControlProfileInvalid() from None


def _render(
    profile: ControlProfile, projection: Projection, server_ca_pem: bytes
) -> tuple[dict[str, Any], ...]:
    _require(type(profile) is ControlProfile and type(projection) is Projection)
    v = profile.as_dict()
    _require(v["projection_digest"] == projection.digest)
    _require(v["workload_namespace"] == projection.as_dict()["namespace"])
    guard = v["guard"]
    _require(_ca_digest(server_ca_pem) == guard["server_ca_sha256"])
    workload, control = v["workload_namespace"], v["control_namespace"]
    return (
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": workload}},
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": control}},
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": guard["service_name"], "namespace": control},
            "spec": {
                "type": "ClusterIP",
                "selector": {GUARD_SERVER_LABEL: profile.digest[:32]},
                "ports": [
                    {
                        "name": "https",
                        "port": GUARD_PORT,
                        "targetPort": GUARD_TARGET_PORT,
                        "protocol": "TCP",
                    }
                ],
            },
        },
        {
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingWebhookConfiguration",
            "metadata": {"name": guard["name"]},
            "webhooks": [
                {
                    "name": guard["name"],
                    "admissionReviewVersions": ["v1"],
                    "sideEffects": "None",
                    "failurePolicy": "Fail",
                    "matchPolicy": "Equivalent",
                    "timeoutSeconds": guard["timeout_seconds"],
                    "namespaceSelector": {
                        "matchExpressions": [
                            {
                                "key": NAMESPACE_NAME_LABEL,
                                "operator": "In",
                                "values": [workload],
                            }
                        ]
                    },
                    "objectSelector": {},
                    "clientConfig": {
                        "service": {
                            "namespace": control,
                            "name": guard["service_name"],
                            "port": GUARD_PORT,
                            "path": GUARD_PATH,
                        },
                        "caBundle": base64.b64encode(server_ca_pem).decode("ascii"),
                    },
                    "rules": copy.deepcopy(list(_RULES)),
                }
            ],
        },
    )
