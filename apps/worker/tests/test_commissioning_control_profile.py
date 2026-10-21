"""Closed control profile vectors; source rendering, no cluster qualification."""

from __future__ import annotations

import base64
import datetime
import hashlib
import json
from copy import deepcopy

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.x509.oid import NameOID
from curie_worker import commissioning_control_profile as cp
from curie_worker.protected_kubernetes_projection import parse_projection

from .protected_configuration_vectors import canonical
from .test_protected_kubernetes_projection import projection_value


def certificate(*, ca: bool = True) -> bytes:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "guard-ca")])
    now = datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(1)
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return cert.public_bytes(Encoding.PEM)


CA = certificate()


def projection():
    return parse_projection(canonical(projection_value()))


def profile_value():
    p = projection()
    return {
        "schema_version": 1,
        "record_type": "commissioning_control_profile",
        "projection_digest": p.digest,
        "authority_domain_id": p.as_dict()["authority_domain_id"],
        "kubernetes_minors": ["1.34", "1.35"],
        "workload_namespace": "acme-runtime",
        "control_namespace": "acme-control",
        "guard": {
            "name": "workloads.guard.example.com",
            "service_name": "acme-guard",
            "image": "registry.example.com/acme-guard@sha256:" + "c" * 64,
            "server_ca_sha256": cp.server_ca_digest(CA),
            "timeout_seconds": 5,
        },
        "principals": {
            "mediator": "acme-mediator",
            "provisioner": "acme-provisioner",
            "endpoint_controllers": [
                "system:serviceaccount:kube-system:endpoint-controller",
                "system:serviceaccount:kube-system:endpointslice-controller",
            ],
            "pod_binders": ["system:kube-scheduler"],
            "pod_status_controllers": [
                "system:serviceaccount:kube-system:node-controller",
                "system:serviceaccount:kube-system:pod-garbage-collector",
            ],
        },
        "slot_policy": "launch-id-names-v1",
    }


def profile(value=None):
    return cp.parse_control_profile(canonical(profile_value() if value is None else value))


def test_pod_status_controllers_may_be_empty():
    value = profile_value()
    value["principals"]["pod_status_controllers"] = []
    assert profile(value).as_dict()["principals"]["pod_status_controllers"] == []


def test_cni_policy_kinds_are_routed_at_every_version():
    hook = cp.render_control_objects(profile(), projection=projection(), server_ca_pem=CA)[3][
        "webhooks"
    ][0]
    groups = {"cilium.io", "crd.projectcalico.org", "projectcalico.org", "vpcresources.k8s.aws"}
    for rule in hook["rules"]:
        if rule["apiGroups"][0] in groups:
            assert rule["apiVersions"] == ["*"]
            assert rule["operations"] == ["CREATE", "UPDATE", "DELETE"]


def test_controller_manager_without_service_account_credentials_is_accepted():
    value = profile_value()
    value["principals"]["endpoint_controllers"] = ["system:kube-controller-manager"]
    assert profile(value).as_dict()["principals"]["endpoint_controllers"] == [
        "system:kube-controller-manager"
    ]


def test_canonical_profile_digest_is_g_member_and_copies_are_fresh():
    parsed = profile()
    assert parsed.digest == hashlib.sha256(parsed.canonical_bytes).hexdigest()
    assert repr(parsed) == "<ControlProfile>"
    assert parsed.guard_name == "workloads.guard.example.com"
    copy = parsed.as_dict()
    copy["guard"]["name"] = "changed.guard.example.com"
    assert parsed.guard_name == "workloads.guard.example.com"
    with pytest.raises(cp.ControlProfileInvalid):
        cp.parse_control_profile(json.dumps(profile_value(), indent=1).encode())


def _set(path, value):
    def mutate(v):
        target = v
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value

    return mutate


def _drop(path):
    def mutate(v):
        target = v
        for key in path[:-1]:
            target = target[key]
        del target[path[-1]]

    return mutate


@pytest.mark.parametrize(
    "mutate",
    [
        _set(("extra",), 1),
        _drop(("guard",)),
        _set(("guard", "extra"), 1),
        _drop(("guard", "timeout_seconds")),
        _set(("principals", "observer"), "acme-observer"),
        _set(("schema_version",), 2),
        _set(("schema_version",), True),
        _set(("record_type",), "commissioning_permit"),
        _set(("slot_policy",), "provisioner-owned-exact-slots-v1"),
        _set(("projection_digest",), "A" * 64),
        _set(("authority_domain_id",), "not-a-uuid"),
        _set(("kubernetes_minors",), []),
        _set(("kubernetes_minors",), ["1.33"]),
        _set(("kubernetes_minors",), ["1.36"]),
        _set(("kubernetes_minors",), ["1.35", "1.34"]),
        _set(("kubernetes_minors",), ["1.34", "1.34"]),
        _set(("kubernetes_minors",), "1.34"),
        _set(("workload_namespace",), "Acme"),
        _set(("control_namespace",), "acme-runtime"),
        _set(("guard", "name"), "guard.example"),
        _set(("guard", "name"), "Guard.example.com"),
        _set(("guard", "service_name"), "acme.guard"),
        _set(("guard", "image"), "registry.example.com/acme-guard:latest"),
        _set(("guard", "image"), "registry.example.com/acme-guard@sha256:" + "C" * 64),
        _set(("guard", "server_ca_sha256"), "0" * 63),
        _set(("guard", "timeout_seconds"), 0),
        _set(("guard", "timeout_seconds"), 11),
        _set(("guard", "timeout_seconds"), 5.0),
        _set(("guard", "timeout_seconds"), True),
        _set(("principals", "mediator"), "acme-provisioner"),
        _set(("principals", "mediator"), "system:anonymous"),
        _set(("principals", "mediator"), "system:kube-scheduler"),
        _set(("principals", "provisioner"), "system:node:worker-1"),
        _set(("principals", "provisioner"), ""),
        _set(("principals", "mediator"), "system:serviceaccount:kube-system:endpoint-controller"),
        _set(("principals", "endpoint_controllers"), []),
        _set(("principals", "endpoint_controllers"), "system:kube-controller-manager"),
        _set(("principals", "endpoint_controllers"), ["acme-mediator"]),
        _set(("principals", "endpoint_controllers"), ["system:serviceaccount:acme-runtime:x"]),
        _set(("principals", "endpoint_controllers"), ["system:serviceaccount:kube-system:Bad"]),
        _set(
            ("principals", "endpoint_controllers"),
            [
                "system:serviceaccount:kube-system:endpointslice-controller",
                "system:serviceaccount:kube-system:endpoint-controller",
            ],
        ),
        _set(
            ("principals", "endpoint_controllers"),
            ["system:kube-controller-manager", "system:kube-controller-manager"],
        ),
        _set(
            ("principals", "endpoint_controllers"),
            [f"system:serviceaccount:kube-system:c{i}" for i in range(5)],
        ),
        _drop(("principals", "pod_binders")),
        _set(("principals", "pod_binders"), []),
        _set(("principals", "pod_binders"), ["system:kube-controller-manager"]),
        _set(("principals", "pod_binders"), ["acme-mediator"]),
        _set(
            ("principals", "pod_binders"),
            [
                "system:kube-scheduler",
                "system:serviceaccount:kube-system:a",
                "system:kube-scheduler",
            ],
        ),
        _drop(("principals", "pod_status_controllers")),
        _set(("principals", "pod_status_controllers"), ["system:node:worker-1"]),
        _set(("principals", "pod_status_controllers"), ["system:kube-scheduler"]),
        _set(
            ("principals", "pod_status_controllers"),
            [f"system:serviceaccount:kube-system:c{i}" for i in range(5)],
        ),
        _set(("principals", "mediator"), "system:kube-scheduler"),
    ],
)
def test_profile_refuses_every_closed_member_change(mutate):
    value = profile_value()
    mutate(value)
    with pytest.raises(cp.ControlProfileInvalid) as raised:
        profile(value)
    assert raised.value.__cause__ is None and raised.value.__context__ is None
    assert str(raised.value) == "Invalid protected control profile"


def test_duplicate_member_and_oversize_refuse():
    raw = canonical(profile_value())
    duplicated = raw.replace(b'"slot_policy"', b'"schema_version":1,"slot_policy"')
    for bad in (duplicated, raw + b" " * 16384, b"", b"[]"):
        with pytest.raises(cp.ControlProfileInvalid):
            cp.parse_control_profile(bad)


def test_binding_requires_the_g_member_and_the_projection():
    parsed, p = profile(), projection()
    cp.validate_control_profile_binding(parsed, admission_policy_sha256=parsed.digest, projection=p)
    with pytest.raises(cp.ControlProfileInvalid):
        cp.validate_control_profile_binding(parsed, admission_policy_sha256="0" * 64, projection=p)
    other = projection_value()
    other["runtime_class"] = "other-runtime"
    with pytest.raises(cp.ControlProfileInvalid):
        cp.validate_control_profile_binding(
            parsed,
            admission_policy_sha256=parsed.digest,
            projection=parse_projection(canonical(other)),
        )


@pytest.mark.parametrize(
    "mutate",
    [
        _set(("workload_namespace",), "other-runtime"),
        _set(("authority_domain_id",), "99999999-9999-4999-8999-999999999999"),
        _set(("principals", "mediator"), "system:serviceaccount:acme-runtime:acme-runner"),
    ],
)
def test_binding_refuses_each_projection_mismatch(mutate):
    value = profile_value()
    mutate(value)
    parsed = profile(value)
    with pytest.raises(cp.ControlProfileInvalid):
        cp.validate_control_profile_binding(
            parsed, admission_policy_sha256=parsed.digest, projection=projection()
        )


@pytest.mark.parametrize(
    ("git", "minor", "accepted"),
    [
        ("v1.34.4", "34", True),
        ("v1.35.1", "35", True),
        ("v1.35.1-eks-abcdef", "35+", True),
        ("v1.33.9", "33", False),
        ("v1.36.0", "36", False),
        ("v1.35.1", "34", False),
        ("1.35.1", "35", False),
    ],
)
def test_server_version_must_be_a_pinned_minor(git, minor, accepted):
    version = {"major": "1", "minor": minor, "gitVersion": git}
    if accepted:
        cp.require_supported_version(profile(), version)
    else:
        with pytest.raises(cp.ControlProfileInvalid):
            cp.require_supported_version(profile(), version)


def test_server_version_outside_a_narrowed_profile_refuses():
    value = profile_value()
    value["kubernetes_minors"] = ["1.35"]
    with pytest.raises(cp.ControlProfileInvalid):
        cp.require_supported_version(
            profile(value), {"major": "1", "minor": "34", "gitVersion": "v1.34.4"}
        )


def test_rendered_webhook_fails_closed_on_the_workload_namespace_only():
    objects = cp.render_control_objects(profile(), projection=projection(), server_ca_pem=CA)
    kinds = [o["kind"] for o in objects]
    assert kinds == ["Namespace", "Namespace", "Service", "ValidatingWebhookConfiguration"]
    vwc = objects[3]
    assert vwc["metadata"]["name"] == "workloads.guard.example.com"
    (hook,) = vwc["webhooks"]
    assert hook["name"] == "workloads.guard.example.com"
    assert hook["failurePolicy"] == "Fail"
    assert hook["sideEffects"] == "None"
    assert hook["matchPolicy"] == "Equivalent"
    assert hook["timeoutSeconds"] == 5
    assert hook["namespaceSelector"] == {
        "matchExpressions": [
            {"key": "kubernetes.io/metadata.name", "operator": "In", "values": ["acme-runtime"]}
        ]
    }
    assert hook["objectSelector"] == {}
    assert base64.b64decode(hook["clientConfig"]["caBundle"]) == CA
    assert hook["clientConfig"]["service"] == {
        "namespace": "acme-control",
        "name": "acme-guard",
        "port": 443,
        "path": "/validate",
    }
    service = objects[2]
    assert service["metadata"]["namespace"] == "acme-control"
    assert service["spec"]["selector"] == {"curietech.ai/protected-guard": profile().digest[:32]}


def test_rendered_rules_cover_every_container_list_and_fixed_object():
    hook = cp.render_control_objects(profile(), projection=projection(), server_ca_pem=CA)[3][
        "webhooks"
    ][0]
    covered = {
        (rule["apiGroups"][0], resource, operation)
        for rule in hook["rules"]
        for resource in rule["resources"]
        for operation in rule["operations"]
    }
    for expected in [
        ("", "pods", "CREATE"),
        ("", "pods", "UPDATE"),
        ("", "pods/ephemeralcontainers", "UPDATE"),
        ("", "pods/resize", "UPDATE"),
        ("", "pods/exec", "CONNECT"),
        ("", "pods/attach", "CONNECT"),
        ("", "pods/portforward", "CONNECT"),
        ("", "secrets", "CREATE"),
        ("", "secrets", "UPDATE"),
        ("", "services", "CREATE"),
        ("", "serviceaccounts", "DELETE"),
        ("", "serviceaccounts/token", "CREATE"),
        ("networking.k8s.io", "networkpolicies", "UPDATE"),
        ("networking.k8s.io", "networkpolicies", "DELETE"),
        ("rbac.authorization.k8s.io", "rolebindings", "CREATE"),
        ("", "namespaces", "UPDATE"),
        ("", "endpoints", "CREATE"),
        ("", "endpoints", "UPDATE"),
        ("discovery.k8s.io", "endpointslices", "CREATE"),
        ("discovery.k8s.io", "endpointslices", "UPDATE"),
        ("", "pods/status", "UPDATE"),
        ("", "pods/binding", "CREATE"),
        ("", "bindings", "CREATE"),
        ("cilium.io", "ciliumnetworkpolicies", "CREATE"),
        ("cilium.io", "ciliumnetworkpolicies", "UPDATE"),
        ("cilium.io", "ciliumnetworkpolicies", "DELETE"),
        ("crd.projectcalico.org", "networkpolicies", "CREATE"),
        ("projectcalico.org", "networkpolicies", "UPDATE"),
        ("vpcresources.k8s.aws", "securitygrouppolicies", "CREATE"),
    ]:
        assert expected in covered
    # The rendering is a copy: changing it cannot widen the next rendering.
    hook["rules"].clear()
    again = cp.render_control_objects(profile(), projection=projection(), server_ca_pem=CA)
    assert again[3]["webhooks"][0]["rules"]


# Stable IDs: certificates are generated per run, so they must not name the case.
CA_CASES = {
    "another_ca": lambda: certificate(),
    "not_a_ca": lambda: certificate(ca=False),
    "crlf_pem": lambda: CA.replace(b"\n", b"\r\n"),
    "empty": lambda: b"",
    "not_pem": lambda: b"not a pem",
}


@pytest.mark.parametrize("case", sorted(CA_CASES))
def test_render_refuses_a_ca_that_is_not_the_pinned_ca(case):
    with pytest.raises(cp.ControlProfileInvalid):
        cp.render_control_objects(
            profile(), projection=projection(), server_ca_pem=CA_CASES[case]()
        )


def test_render_refuses_another_projection():
    other = projection_value()
    other["service_account"] = "other-runner"
    with pytest.raises(cp.ControlProfileInvalid):
        cp.render_control_objects(
            profile(), projection=parse_projection(canonical(other)), server_ca_pem=CA
        )


def test_profile_is_not_a_projection_or_guard_config():
    value = deepcopy(profile_value())
    value["record_type"] = "commissioning_guard_config"
    with pytest.raises(cp.ControlProfileInvalid):
        profile(value)
    with pytest.raises(cp.ControlProfileInvalid):
        cp.parse_control_profile(canonical(projection_value()))
