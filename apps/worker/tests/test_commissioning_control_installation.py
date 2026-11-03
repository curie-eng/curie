"""Control installation roster and order. @spec PROTECTED-COMMISSIONING-5/8.

Source rendering and installation order against a recording cluster; no
cluster is measured here.
"""

from __future__ import annotations

import base64
import datetime
from copy import deepcopy
from typing import Any

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.x509.oid import NameOID
from curie_worker import commissioning_control_installation as ci
from curie_worker import commissioning_control_profile as cp

from .test_commissioning_control_profile import profile, profile_value, projection

OWNER = {"commissioning.curietech.ai/owner": "curie-commission-000000000d07"}
_NOW = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=1)


def _name(value: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, value)])


def _issue(subject: str, key: Any, issuer_key: Any, *, ca: bool) -> x509.Certificate:
    builder = (
        x509.CertificateBuilder()
        .subject_name(_name(subject))
        .issuer_name(_name("guard-ca"))
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(_NOW)
        .not_valid_after(_NOW + datetime.timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
    )
    if not ca:
        builder = builder.add_extension(
            x509.SubjectAlternativeName([x509.DNSName(subject)]), critical=False
        )
    return builder.sign(issuer_key, hashes.SHA256())


def material_and_profile() -> tuple[ci.ControlMaterial, cp.ControlProfile]:
    ca_key, key = ec.generate_private_key(ec.SECP256R1()), ec.generate_private_key(ec.SECP256R1())
    ca = _issue("guard-ca", ca_key, ca_key, ca=True).public_bytes(Encoding.PEM)
    leaf = _issue("acme-guard.acme-control.svc", key, ca_key, ca=False)
    material = ci.ControlMaterial(
        ca,
        leaf.public_bytes(Encoding.PEM),
        key.private_bytes(
            Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        ),
    )
    value = profile_value()
    value["guard"]["server_ca_sha256"] = cp.server_ca_digest(ca)
    return material, profile(value)


MATERIAL, PROFILE = material_and_profile()


def roster() -> tuple[dict[str, Any], ...]:
    return ci.render_control_installation(
        PROFILE, projection=projection(), material=MATERIAL, labels=OWNER
    )


def test_the_roster_is_the_profile_rendering_with_the_guard_workload_and_webhook_last():
    documents = roster()
    assert [ci.object_key(d) for d in documents] == [
        "v1/Namespace//acme-runtime",
        "v1/Namespace//acme-control",
        "v1/Secret/acme-control/acme-guard-config",
        "v1/Service/acme-control/acme-guard",
        "apps/v1/Deployment/acme-control/acme-guard",
        "admissionregistration.k8s.io/v1/ValidatingWebhookConfiguration//"
        "workloads.guard.example.com",
    ]
    rendered = cp.render_control_objects(
        PROFILE, projection=projection(), server_ca_pem=MATERIAL.server_ca_pem
    )
    for document, expected in zip(
        (documents[0], documents[1], documents[3], documents[5]), rendered, strict=True
    ):
        assert document["metadata"].pop("labels") == OWNER
        assert document == expected
    # Owner labels never reach the guard Pod template or its selector.
    deployment = documents[4]
    assert deployment["metadata"]["labels"] == OWNER
    assert OWNER.keys().isdisjoint(deployment["spec"]["template"]["metadata"]["labels"])
    volume = deployment["spec"]["template"]["spec"]["volumes"][0]
    assert volume["secret"]["secretName"] == "acme-guard-config"


def test_the_config_secret_holds_exactly_what_the_guard_reads():
    secret = roster()[2]
    assert secret["immutable"] is True and secret["type"] == "Opaque"
    data = {key: base64.b64decode(value) for key, value in secret["data"].items()}
    assert tuple(sorted(data)) == ci.CONFIG_KEYS
    assert data["profile.json"] == PROFILE.canonical_bytes
    assert data["projection.json"] == projection().canonical_bytes
    assert data["ca.crt"] == MATERIAL.server_ca_pem
    assert data["tls.crt"] == MATERIAL.certificate_pem
    assert data["tls.key"] == MATERIAL.key_pem
    assert "PRIVATE" not in repr(MATERIAL)


def test_material_must_be_the_pinned_ca_and_its_key_pair():
    other, _ = material_and_profile()
    for material in (
        ci.ControlMaterial(other.server_ca_pem, MATERIAL.certificate_pem, MATERIAL.key_pem),
        ci.ControlMaterial(MATERIAL.server_ca_pem, other.certificate_pem, other.key_pem),
        ci.ControlMaterial(MATERIAL.server_ca_pem, MATERIAL.certificate_pem, other.key_pem),
    ):
        with pytest.raises(ci.ControlInstallationRefused):
            ci.render_control_installation(
                PROFILE, projection=projection(), material=material, labels=OWNER
            )


@pytest.mark.parametrize("labels", [{}, {"bad key!": "x"}, {"a": "-x"}])
def test_owner_labels_are_required_and_valid(labels):
    with pytest.raises(ci.ControlInstallationRefused):
        ci.render_control_installation(
            PROFILE, projection=projection(), material=MATERIAL, labels=labels
        )


class Cluster:
    """Records creates; the guard Deployment becomes ready after ``ready_after`` reads."""

    def __init__(self, *, ready_after: int = 2, fail: str | None = None) -> None:
        self.created: list[str] = []
        self.objects: dict[str, dict[str, Any]] = {}
        self.reads = 0
        self.ready_after, self.fail = ready_after, fail

    def create(self, manifest: dict[str, Any], *, timeout: float = 60) -> str:
        key = ci.object_key(manifest)
        if key == self.fail or key in self.objects:
            raise RuntimeError("AlreadyExists")
        self.created.append(key)
        value = deepcopy(dict(manifest))
        value["metadata"]["uid"] = f"00000000-0000-4000-8000-{len(self.created):012d}"
        value["metadata"]["generation"] = 1
        self.objects[key] = value
        return str(value["metadata"]["uid"])

    def read(self, key: str) -> dict[str, Any] | None:
        self.reads += 1
        value = self.objects.get(key)
        if value is not None and value["kind"] == "Deployment" and self.reads > self.ready_after:
            value["status"] = dict.fromkeys(
                ("readyReplicas", "availableReplicas", "updatedReplicas", "replicas"), 1
            )
            value["status"]["observedGeneration"] = 1
        return deepcopy(value)


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def test_the_webhook_is_created_only_after_the_guard_is_ready():
    cluster, clock = Cluster(ready_after=3), Clock()
    installed = ci.ControlInstallation(cluster, roster(), clock=clock, sleep=clock.sleep).install()
    keys = [ci.object_key(d) for d in roster()]
    assert cluster.created == keys
    assert cluster.reads == 4 and clock.now == 6
    assert [key for key, _ in installed.uids] == keys
    assert installed.uid(keys[5]) == cluster.objects[keys[5]]["metadata"]["uid"]


def test_a_guard_that_never_serves_leaves_the_webhook_uncreated():
    cluster, clock = Cluster(ready_after=10**6), Clock()
    installation = ci.ControlInstallation(cluster, roster(), clock=clock, sleep=clock.sleep)
    with pytest.raises(ci.ControlInstallationRefused):
        installation.install(ready_timeout=30)
    assert not [key for key in cluster.created if "ValidatingWebhookConfiguration" in key]


def test_a_same_name_object_refuses_and_nothing_after_it_is_created():
    keys = [ci.object_key(d) for d in roster()]
    cluster = Cluster(fail=keys[3])
    with pytest.raises(ci.ControlInstallationRefused):
        ci.ControlInstallation(cluster, roster(), sleep=lambda _: None).install()
    assert cluster.created == keys[:3]


def test_a_reordered_or_partial_roster_refuses():
    documents = roster()
    for bad in (documents[::-1], documents[:-1], (*documents[:5], documents[0])):
        with pytest.raises(ci.ControlInstallationRefused):
            ci.ControlInstallation(Cluster(), bad)


def test_readiness_counts_only_the_installed_deployment():
    keys = [ci.object_key(d) for d in roster()]

    class Replaced(Cluster):
        def read(self, key: str) -> dict[str, Any] | None:
            value = super().read(key)
            if value is not None and key == keys[4]:
                value["metadata"]["uid"] = "99999999-0000-4000-8000-000000000001"
            return value

    cluster = Replaced(ready_after=0)
    with pytest.raises(ci.ControlInstallationRefused):
        ci.ControlInstallation(cluster, roster(), sleep=lambda _: None).install()
    assert keys[5] not in cluster.created


def _leaf(*, days: int, dns: str | None) -> ci.ControlMaterial:
    ca_key, key = ec.generate_private_key(ec.SECP256R1()), ec.generate_private_key(ec.SECP256R1())
    ca = _issue("guard-ca", ca_key, ca_key, ca=True)
    builder = (
        x509.CertificateBuilder()
        .subject_name(_name("leaf"))
        .issuer_name(_name("guard-ca"))
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(_NOW + datetime.timedelta(days=days))
        .not_valid_after(_NOW + datetime.timedelta(days=days + 2))
    )
    if dns is not None:
        builder = builder.add_extension(
            x509.SubjectAlternativeName([x509.DNSName(dns)]), critical=False
        )
    return ci.ControlMaterial(
        ca.public_bytes(Encoding.PEM),
        builder.sign(ca_key, hashes.SHA256()).public_bytes(Encoding.PEM),
        key.private_bytes(
            Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        ),
    )


@pytest.mark.parametrize(
    ("days", "dns", "accepted"),
    [
        (0, "acme-guard.acme-control.svc", True),
        (-30, "acme-guard.acme-control.svc", False),
        (10, "acme-guard.acme-control.svc", False),
        (0, "other.acme-control.svc", False),
        (0, None, False),
    ],
)
def test_the_serving_certificate_is_current_and_names_the_guard_service(days, dns, accepted):
    material = _leaf(days=days, dns=dns)
    value = profile_value()
    value["guard"]["server_ca_sha256"] = cp.server_ca_digest(material.server_ca_pem)
    if accepted:
        material.check(profile(value))
    else:
        with pytest.raises(ci.ControlInstallationRefused):
            material.check(profile(value))
