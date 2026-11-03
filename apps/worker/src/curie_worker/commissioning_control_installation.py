"""The control installation: the guard's objects, owned before they exist. ADR 0236.

@spec PROTECTED-COMMISSIONING-5. The operator command owns a unique
installation and registers ownership of every resource before it is created.
@spec PROTECTED-COMMISSIONING-8. The guard permits only the measured image,
service account, mounts, Secret references, network and ephemeral container
policy.

This module renders the complete control roster of one installation from its
control profile, projection and guard serving material, and installs it in a
fixed order through an owning cluster: the workload and control namespaces,
the guard configuration Secret, the guard Service and Deployment, and only once
the guard serves, the webhook configuration. The webhook fails closed, so it
is created last; creating it first would refuse every workload request until
the guard answered.

Every create goes through ``InstallationCluster``, which registers ownership
before the create, refuses a same-name object and binds the UID from the
create response (the provisioner's local cluster does exactly that). This
module adds the order and the readiness wait and returns the bound UIDs, which
the control observer then requires on every reobservation. It grants no
admission, launch or campaign authority and decides nothing about the
installation's qualification.
"""

from __future__ import annotations

import base64
import datetime
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, NoReturn, Protocol

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import Encoding

from .commissioning_control_admission import render_guard_deployment
from .commissioning_control_profile import (
    ControlProfile,
    render_control_objects,
    server_ca_digest,
)
from .protected_kubernetes_projection import Projection
from .protected_launch_records import _pattern, canonical_json

__all__ = [
    "CONFIG_KEYS",
    "ControlInstallation",
    "ControlInstallationRefused",
    "ControlMaterial",
    "InstallationCluster",
    "InstalledControl",
    "config_secret_name",
    "object_key",
    "render_control_installation",
]

# The files the guard reads from its configuration directory.
CONFIG_KEYS = ("ca.crt", "profile.json", "projection.json", "tls.crt", "tls.key")
_LABEL_NAME = r"[A-Za-z0-9](?:[A-Za-z0-9._-]{0,61}[A-Za-z0-9])?"
_LABEL_KEY = r"(?:[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?/)?" + _LABEL_NAME
_LABEL_VALUE = "(?:" + _LABEL_NAME + ")?"


class ControlInstallationRefused(RuntimeError):
    """Fixed refusal; never echoes a document, key or certificate."""

    def __init__(self) -> None:
        super().__init__("Protected control installation refused")


def _refuse() -> NoReturn:
    raise ControlInstallationRefused() from None


def _require(condition: bool) -> None:
    if not condition:
        _refuse()


def object_key(document: Mapping[str, Any]) -> str:
    """``apiVersion/kind/namespace/name``; the namespace is empty for cluster objects."""
    metadata = document["metadata"]
    namespace = metadata.get("namespace", "")
    return "/".join((document["apiVersion"], document["kind"], namespace, metadata["name"]))


def config_secret_name(profile: ControlProfile) -> str:
    """The guard configuration Secret beside its Service in the control namespace."""
    return str(profile.as_dict()["guard"]["service_name"]) + "-config"


@dataclass(frozen=True, slots=True, repr=False)
class ControlMaterial:
    """The guard's serving material: the pinned CA and a key pair it issued."""

    server_ca_pem: bytes
    certificate_pem: bytes
    key_pem: bytes = field(repr=False)

    def __repr__(self) -> str:
        return "<ControlMaterial>"

    def check(self, profile: ControlProfile) -> None:
        """The CA is the profile's pin and directly issued a current serving certificate.

        The certificate names the guard Service's DNS name, which the API
        server dials, and its key is the one supplied.
        """
        try:
            _require(type(profile) is ControlProfile)
            v = profile.as_dict()
            pinned = v["guard"]["server_ca_sha256"]
            _require(server_ca_digest(self.server_ca_pem) == pinned)
            certificate = x509.load_pem_x509_certificate(self.certificate_pem)
            _require(certificate.public_bytes(Encoding.PEM) == self.certificate_pem)
            certificate.verify_directly_issued_by(
                x509.load_pem_x509_certificate(self.server_ca_pem)
            )
            now = datetime.datetime.now(datetime.UTC)
            _require(certificate.not_valid_before_utc <= now < certificate.not_valid_after_utc)
            names = certificate.extensions.get_extension_for_class(
                x509.SubjectAlternativeName
            ).value.get_values_for_type(x509.DNSName)
            _require(f"{v['guard']['service_name']}.{v['control_namespace']}.svc" in names)
            key = serialization.load_pem_private_key(self.key_pem, password=None)
            _require(
                key.public_key().public_bytes(
                    Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
                )
                == certificate.public_key().public_bytes(
                    Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
                )
            )
        except ControlInstallationRefused:
            raise
        except Exception:  # noqa: BLE001 - no certificate or key detail
            _refuse()


def _config_secret(
    profile: ControlProfile, projection: Projection, material: ControlMaterial
) -> dict[str, Any]:
    files = {
        "ca.crt": material.server_ca_pem,
        "profile.json": profile.canonical_bytes,
        "projection.json": projection.canonical_bytes,
        "tls.crt": material.certificate_pem,
        "tls.key": material.key_pem,
    }
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": config_secret_name(profile),
            "namespace": profile.as_dict()["control_namespace"],
        },
        "type": "Opaque",
        "immutable": True,
        "data": {key: base64.b64encode(files[key]).decode("ascii") for key in CONFIG_KEYS},
    }


def render_control_installation(
    profile: ControlProfile,
    *,
    projection: Projection,
    material: ControlMaterial,
    labels: Mapping[str, str],
) -> tuple[dict[str, Any], ...]:
    """The complete control roster in installation order; the webhook is last.

    ``labels`` (for example the provisioner's owner label) are added to every
    object's own metadata, never to the guard Pod template or a selector.
    """
    try:
        _require(type(projection) is Projection)
        material.check(profile)
        _require(isinstance(labels, Mapping) and 1 <= len(labels) <= 8)
        for key, value in labels.items():
            _pattern(key, _LABEL_KEY)
            _pattern(value, _LABEL_VALUE)
        workload, control, service, webhook = render_control_objects(
            profile, projection=projection, server_ca_pem=material.server_ca_pem
        )
        roster = (
            workload,
            control,
            _config_secret(profile, projection, material),
            service,
            render_guard_deployment(profile, config_secret=config_secret_name(profile)),
            webhook,
        )
        for document in roster:
            metadata = document["metadata"]
            _require("labels" not in metadata)
            metadata["labels"] = dict(labels)
        _require(len({object_key(d) for d in roster}) == len(roster))
        return roster
    except ControlInstallationRefused:
        raise
    except Exception:  # noqa: BLE001 - fixed refusal, no input detail
        _refuse()


class InstallationCluster(Protocol):
    """The owning cluster: ownership before create, no adoption, UID from the response."""

    def create(self, manifest: Mapping[str, Any], *, timeout: float = 60) -> str:
        """Register, create and bind one object; return its UID. Refuses a same-name object."""
        ...

    def read(self, key: str) -> dict[str, Any] | None:
        """The live object at an ``object_key``, or ``None`` when absent."""
        ...


@dataclass(frozen=True, slots=True)
class InstalledControl:
    """Each roster object's ``object_key`` and the UID its create response carried.

    An installation record, not a capability: the control observer requires
    these exact UIDs, so a deleted and recreated object is a different one.
    """

    uids: tuple[tuple[str, str], ...]

    def uid(self, key: str) -> str:
        found = [uid for name, uid in self.uids if name == key]
        _require(len(found) == 1)
        return found[0]

    def digest_document(self) -> bytes:
        return canonical_json([list(pair) for pair in self.uids])


def _guard_ready(document: dict[str, Any] | None, uid: str) -> bool:
    if document is None:
        return False
    # Readiness of the installed Deployment only, never of a replacement.
    _require(document["metadata"].get("uid") == uid)
    status = document.get("status") or {}
    generation = document["metadata"].get("generation")
    return (
        status.get("observedGeneration") == generation
        and status.get("readyReplicas") == 1
        and status.get("availableReplicas") == 1
        and status.get("updatedReplicas") == 1
        and status.get("replicas") == 1
    )


class ControlInstallation:
    """Install one rendered control roster through an owning cluster, in order."""

    def __init__(
        self,
        cluster: InstallationCluster,
        roster: tuple[dict[str, Any], ...],
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        _require(type(roster) is tuple and len(roster) == 6)
        kinds = [(d["apiVersion"], d["kind"]) for d in roster]
        _require(
            kinds
            == [
                ("v1", "Namespace"),
                ("v1", "Namespace"),
                ("v1", "Secret"),
                ("v1", "Service"),
                ("apps/v1", "Deployment"),
                ("admissionregistration.k8s.io/v1", "ValidatingWebhookConfiguration"),
            ]
        )
        self._cluster, self._roster = cluster, roster
        self._clock, self._sleep = clock, sleep

    def install(self, *, ready_timeout: float = 300) -> InstalledControl:
        """Create every object; the webhook only once the guard Deployment is ready.

        A failed create or a guard that never becomes ready refuses. What was
        created stays owned by the cluster's ledger for its own removal.
        """
        _require(type(ready_timeout) in (int, float) and 0 < ready_timeout <= 900)
        uids: list[tuple[str, str]] = []
        for document in self._roster[:-1]:
            uids.append((object_key(document), self._create(document)))
        deployment = object_key(self._roster[4])
        deadline = self._clock() + ready_timeout
        while not _guard_ready(self._cluster.read(deployment), uids[4][1]):
            _require(self._clock() < deadline)
            self._sleep(2)
        uids.append((object_key(self._roster[-1]), self._create(self._roster[-1])))
        return InstalledControl(tuple(uids))

    def _create(self, document: dict[str, Any]) -> str:
        try:
            uid = self._cluster.create(document)
        except Exception:  # noqa: BLE001 - the cluster's ledger records the ambiguity
            _refuse()
        _require(type(uid) is str and 0 < len(uid) <= 128)
        return uid
