"""Guard decisions over synthetic AdmissionReviews; no cluster qualification."""

from __future__ import annotations

import asyncio
import datetime
import json
import ssl
from copy import deepcopy

import aiohttp
import pytest
from aiohttp import web
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.x509.oid import NameOID
from curie_worker import commissioning_control_admission as ca
from curie_worker import commissioning_control_profile as cp
from curie_worker import commissioning_control_slots as cs
from curie_worker.managed_cni_probe_kit import judge_guard_dry_run
from curie_worker.protected_kubernetes_projection import parse_projection
from curie_worker.protected_launch_kubernetes import secret_document, service_document

from .protected_configuration_vectors import canonical
from .test_commissioning_control_profile import CA, certificate, profile_value
from .test_protected_kubernetes_projection import projection_value

LAUNCH = "7777aaaa-7777-4777-8777-777777777777"
UID = "11111111-2222-4333-8444-555555555555"
POD_UID = "88888888-8888-4888-8888-888888888888"
MEDIATOR, PROVISIONER = "acme-mediator", "acme-provisioner"
NS = "acme-runtime"
SLOT = cs.ControlSlot(LAUNCH)


def policy():
    projection = parse_projection(canonical(projection_value()))
    return ca.GuardPolicy(cp.parse_control_profile(canonical(profile_value())), projection)


def template_pod():
    slots = cs.PodSlots(SLOT, "session/example", "https://api.example.com/h")
    return cs.expected_pod(policy().projection, slots)


def admitted_pod():
    """The template as a validating webhook receives it on CREATE."""
    pod = template_pod()
    pod["metadata"].update(
        uid=POD_UID,
        creationTimestamp="2026-10-21T00:00:00Z",
        generation=1,
        managedFields=[
            {
                "manager": "mediator",
                "operation": "Update",
                "apiVersion": "v1",
                "time": "2026-10-21T00:00:00Z",
                "fieldsType": "FieldsV1",
                "fieldsV1": {"f:spec": {}},
            }
        ],
    )
    pod["status"] = {"phase": "Pending", "qosClass": "Guaranteed"}
    return pod


def request(
    obj,
    *,
    resource="pods",
    sub="",
    operation="CREATE",
    user=MEDIATOR,
    old=None,
    group="",
    namespace=NS,
    dry_run=False,
):
    q = {
        "uid": UID,
        "kind": {"group": group, "version": "v1", "kind": "Object"},
        "resource": {"group": group, "version": "v1", "resource": resource},
        "requestResource": {"group": group, "version": "v1", "resource": resource},
        "namespace": namespace,
        "operation": operation,
        "userInfo": {"username": user, "groups": ["system:authenticated"]},
        "object": obj,
        "oldObject": old,
        "dryRun": dry_run,
    }
    if sub:
        q["subResource"] = sub
        q["requestSubResource"] = sub
    if type(obj) is dict and "name" in obj.get("metadata", {}):
        q["name"] = obj["metadata"]["name"]
    return {"apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview", "request": q}


def allowed(review):
    response = ca.review(review, policy=policy())["response"]
    assert response["uid"] == (review["request"]["uid"] if type(review) is dict else "")
    if not response["allowed"]:
        assert response["status"] == {"code": 403, "message": ca.MESSAGE}
    return response["allowed"]


def test_measured_pod_create_by_the_mediator_is_allowed_dry_run_alike():
    assert allowed(request(admitted_pod()))
    assert allowed(request(template_pod()))
    assert allowed(request(admitted_pod(), dry_run=True))


def _container(pod):
    return pod["spec"]["containers"][0]


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: _container(p).update(image="registry.example.com/other@sha256:" + "0" * 64),
        lambda p: p["spec"].update(serviceAccountName="other-runner"),
        lambda p: p["spec"].update(serviceAccount="other-runner"),
        lambda p: p["spec"].update(automountServiceAccountToken=True),
        lambda p: p["spec"].update(runtimeClassName="other-runtime"),
        lambda p: p["spec"].update(hostNetwork=True),
        lambda p: p["spec"].update(hostPID=True),
        lambda p: p["spec"].update(dnsPolicy="None"),
        lambda p: p["spec"].update(nodeName="worker-1"),
        lambda p: p["spec"]["volumes"].append({"name": "host", "hostPath": {"path": "/"}}),
        lambda p: p["spec"]["volumes"][0].update(emptyDir={"medium": ""}),
        lambda p: _container(p)["volumeMounts"].append({"name": "tmp", "mountPath": "/etc"}),
        lambda p: _container(p)["volumeMounts"][0].update(readOnly=True),
        lambda p: _container(p)["env"][0]["valueFrom"]["secretKeyRef"].update(
            name=cs.SECRET_PREFIX + "8" * 32
        ),
        lambda p: _container(p)["env"][0]["valueFrom"]["secretKeyRef"].update(optional=True),
        lambda p: _container(p)["securityContext"].update(privileged=True),
        lambda p: _container(p)["securityContext"]["capabilities"].update(add=["NET_RAW"]),
        lambda p: _container(p).update(command=["/bin/sh"]),
        lambda p: _container(p)["ports"].append({"containerPort": 22, "protocol": "TCP"}),
        lambda p: p["spec"].update(initContainers=[deepcopy(_container(p))]),
        lambda p: p["spec"].update(ephemeralContainers=[{"name": "debug", "image": "busybox"}]),
        lambda p: p["spec"]["containers"].append(dict(deepcopy(_container(p)), name="second")),
        lambda p: p["spec"].update(imagePullSecrets=[{"name": "pull"}]),
        lambda p: p["spec"].update(shareProcessNamespace=True),
        lambda p: p["metadata"].update(annotations={"k": "v"}),
        lambda p: p["metadata"]["labels"].update(extra="1"),
        lambda p: p["metadata"].update(finalizers=[]),
        lambda p: p["metadata"].update(ownerReferences=[]),
        lambda p: p["metadata"].update(resourceVersion="7"),
        lambda p: p["metadata"].update(generation=2),
        lambda p: p["metadata"].update(uid="not-a-uuid"),
        lambda p: p["metadata"].update(namespace="other"),
        lambda p: p.update(status={"phase": "Running"}),
        lambda p: p.update(kind="Binding"),
    ],
)
def test_pod_off_the_template_is_refused(mutate):
    pod = admitted_pod()
    mutate(pod)
    assert not allowed(request(pod))
    assert not allowed(request(pod, dry_run=True))


def test_identical_init_container_is_refused_like_a_second_container():
    pod = admitted_pod()
    pod["spec"]["initContainers"] = [deepcopy(_container(pod))]
    assert not allowed(request(pod))


@pytest.mark.parametrize("user", [PROVISIONER, "system:serviceaccount:acme-runtime:acme-runner"])
def test_pod_create_by_anyone_but_the_mediator_is_refused(user):
    assert not allowed(request(admitted_pod(), user=user))


def test_pod_in_another_namespace_is_refused():
    pod = admitted_pod()
    assert not allowed(request(pod, namespace="acme-control"))


@pytest.mark.parametrize(
    ("resource", "sub", "operation"),
    [
        ("pods", "ephemeralcontainers", "UPDATE"),
        ("pods", "resize", "UPDATE"),
        ("pods", "exec", "CONNECT"),
        ("pods", "attach", "CONNECT"),
        ("pods", "portforward", "CONNECT"),
        ("pods", "binding", "CREATE"),
        ("pods", "", "DELETE"),
    ],
)
@pytest.mark.parametrize("user", [MEDIATOR, PROVISIONER])
def test_pod_subresources_always_refuse(resource, sub, operation, user):
    pod = admitted_pod()
    ephemeral = deepcopy(pod)
    ephemeral["spec"]["ephemeralContainers"] = [{"name": "debug", "image": "busybox"}]
    assert not allowed(
        request(ephemeral, resource=resource, sub=sub, operation=operation, user=user, old=pod)
    )


def _live(pod):
    pod = deepcopy(pod)
    pod["metadata"].update(resourceVersion="42", deletionTimestamp="2026-10-21T00:01:00Z")
    pod["metadata"]["deletionGracePeriodSeconds"] = 5
    pod["spec"]["nodeName"] = "worker-1"
    pod["status"] = {"phase": "Failed"}
    return pod


@pytest.mark.parametrize("user", [MEDIATOR, PROVISIONER])
def test_finalizer_only_removal_by_mediator_or_provisioner_is_allowed(user):
    old = _live(admitted_pod())
    new = deepcopy(old)
    del new["metadata"]["finalizers"]
    new["metadata"]["managedFields"] = []
    assert allowed(request(new, operation="UPDATE", user=user, old=old))
    new["metadata"]["finalizers"] = []
    assert allowed(request(new, operation="UPDATE", user=user, old=old))


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p["metadata"]["labels"].update(extra="1"),
        lambda p: p["metadata"].update(annotations={"k": "v"}),
        lambda p: p["metadata"].update(finalizers=["other"]),
        lambda p: p["spec"].update(activeDeadlineSeconds=5),
        lambda p: _container(p).update(image="registry.example.com/other@sha256:" + "0" * 64),
        lambda p: p["spec"]["tolerations"].append({"operator": "Exists"}),
        lambda p: p.update(status={"phase": "Succeeded"}),
    ],
)
def test_finalizer_removal_with_any_other_change_is_refused(mutate):
    old = _live(admitted_pod())
    new = deepcopy(old)
    del new["metadata"]["finalizers"]
    mutate(new)
    assert not allowed(request(new, operation="UPDATE", old=old))


def test_pod_update_without_finalizer_removal_or_by_others_is_refused():
    old = _live(admitted_pod())
    assert not allowed(request(deepcopy(old), operation="UPDATE", old=old))
    new = deepcopy(old)
    del new["metadata"]["finalizers"]
    assert not allowed(request(new, operation="UPDATE", user="system:node:worker-1", old=old))
    renamed_old, renamed_new = deepcopy(old), deepcopy(new)
    for pod in (renamed_old, renamed_new):
        pod["metadata"]["name"] = "ordinary-pod"
    assert not allowed(request(renamed_new, operation="UPDATE", old=renamed_old))
    assert not allowed(request(new, operation="UPDATE", old=None))


def secret():
    value = secret_document(
        NS, SLOT.secret_name, {"runner-token": "r", "history-token": "h", "model-credential": "m"}
    )
    value["metadata"].update(uid=POD_UID, creationTimestamp="2026-10-21T00:00:00Z")
    return value


def test_measured_secret_create_by_the_mediator_is_allowed():
    assert allowed(request(secret(), resource="secrets"))


@pytest.mark.parametrize(
    "mutate",
    [
        lambda s: s.update(immutable=False),
        lambda s: s.pop("immutable"),
        lambda s: s.update(type="kubernetes.io/service-account-token"),
        lambda s: s["data"].update(extra="eA=="),
        lambda s: s["data"].pop("history-token"),
        lambda s: s["data"].update({"runner-token": "not base64!"}),
        lambda s: s["data"].update({"runner-token": ""}),
        lambda s: s.update(stringData={"runner-token": "r"}),
        lambda s: s["metadata"].update(name=cs.POD_PREFIX + "7777aaaa777747778777777777777777"),
        lambda s: s["metadata"].update(name="ordinary-secret"),
        lambda s: s["metadata"].update(labels={"k": "v"}),
        lambda s: s["metadata"].update(annotations={"kubernetes.io/service-account.name": "x"}),
        lambda s: s["metadata"].update(ownerReferences=[]),
    ],
)
def test_secret_off_the_mechanics_shape_is_refused(mutate):
    value = secret()
    mutate(value)
    assert not allowed(request(value, resource="secrets"))


def test_secret_by_others_or_updated_is_refused():
    assert not allowed(request(secret(), resource="secrets", user=PROVISIONER))
    assert not allowed(request(secret(), resource="secrets", operation="UPDATE", old=secret()))


def service():
    value = service_document(NS, SLOT.service_name, LAUNCH)
    value["spec"].update(
        clusterIPs=["None"],
        ipFamilies=["IPv4"],
        ipFamilyPolicy="SingleStack",
        sessionAffinity="None",
        internalTrafficPolicy="Cluster",
    )
    value["status"] = {"loadBalancer": {}}
    return value


def test_measured_service_create_by_the_mediator_is_allowed():
    assert allowed(request(service(), resource="services"))
    assert allowed(request(service_document(NS, SLOT.service_name, LAUNCH), resource="services"))


@pytest.mark.parametrize(
    "mutate",
    [
        lambda s: s["spec"].update(selector={"app": "other"}),
        lambda s: s["spec"].update(type="NodePort"),
        lambda s: s["spec"].update(externalIPs=["10.0.0.1"]),
        lambda s: s["spec"].update(clusterIP="10.96.0.9"),
        lambda s: s["spec"].update(ipFamilies=["IPv4", "IPv6"]),
        lambda s: s["spec"]["ports"].append({"name": "x", "port": 22, "targetPort": 22}),
        lambda s: s["metadata"].update(name=cs.SECRET_PREFIX + "7777aaaa777747778777777777777777"),
    ],
)
def test_service_off_the_mechanics_shape_is_refused(mutate):
    value = service()
    mutate(value)
    assert not allowed(request(value, resource="services"))


@pytest.mark.parametrize(
    ("group", "resource", "sub", "operation"),
    [
        ("", "serviceaccounts", "", "CREATE"),
        ("", "serviceaccounts", "", "UPDATE"),
        ("", "serviceaccounts", "", "DELETE"),
        ("", "serviceaccounts", "token", "CREATE"),
        ("networking.k8s.io", "networkpolicies", "", "CREATE"),
        ("networking.k8s.io", "networkpolicies", "", "UPDATE"),
        ("networking.k8s.io", "networkpolicies", "", "DELETE"),
        ("rbac.authorization.k8s.io", "roles", "", "CREATE"),
        ("rbac.authorization.k8s.io", "rolebindings", "", "UPDATE"),
        ("rbac.authorization.k8s.io", "rolebindings", "", "DELETE"),
        ("", "services", "", "UPDATE"),
        ("", "configmaps", "", "CREATE"),
    ],
)
@pytest.mark.parametrize("user", [MEDIATOR, PROVISIONER])
def test_fixed_objects_never_change_under_the_guard(group, resource, sub, operation, user):
    obj = {"apiVersion": "v1", "kind": "Object", "metadata": {"name": "x", "namespace": NS}}
    assert not allowed(
        request(obj, group=group, resource=resource, sub=sub, operation=operation, user=user)
    )


def test_namespace_update_is_refused():
    obj = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS}}
    assert not allowed(
        request(obj, resource="namespaces", operation="UPDATE", namespace="", old=obj)
    )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda r: r.update(apiVersion="admission.k8s.io/v1beta1"),
        lambda r: r["request"].update(uid="not-a-uuid"),
        lambda r: r["request"].pop("userInfo"),
        lambda r: r["request"]["userInfo"].update(username=""),
        lambda r: r["request"].update(unknown=1),
        lambda r: r["request"].update(dryRun="false"),
        lambda r: r["request"]["resource"].update(version="v2"),
        lambda r: r["request"].update(requestSubResource="exec"),
        lambda r: r.update(extra=1),
    ],
)
def test_malformed_reviews_refuse(mutate):
    value = request(admitted_pod())
    mutate(value)
    response = ca.review(value, policy=policy())["response"]
    assert response["allowed"] is False
    assert response["status"]["message"] == ca.MESSAGE


def test_unusable_uid_is_never_reflected():
    for value in (None, [], {"request": {"uid": "<script>"}}, {"request": []}):
        response = ca.review(value, policy=policy())["response"]
        assert response == {
            "uid": "",
            "allowed": False,
            "status": {"code": 403, "message": ca.MESSAGE},
        }


def test_refusal_reads_as_the_named_guard_to_the_probe_kit():
    # The API server prefixes a webhook denial with the exact webhook name. The
    # managed-CNI kit's judge recognizes only that prefix; the guard reuses it.
    name = policy().profile.guard_name
    message = (
        f'pods "{SLOT.pod_name}" is forbidden: admission webhook "{name}" '
        f"denied the request: {ca.MESSAGE}"
    )
    assert judge_guard_dry_run(403, message, (name,)) == ("pass", "guard_refused")
    assert judge_guard_dry_run(403, message, ("other.guard.example.com",))[0] == "fail"


def test_policy_refuses_a_projection_the_profile_does_not_name():
    other = projection_value()
    other["runtime_class"] = "other-runtime"
    with pytest.raises(ca.ControlAdmissionInvalid):
        ca.GuardPolicy(
            cp.parse_control_profile(canonical(profile_value())),
            parse_projection(canonical(other)),
        )


def test_guard_deployment_has_no_api_credential_and_pins_the_image():
    profile = policy().profile
    deployment = ca.render_guard_deployment(profile, config_secret="acme-guard-config")
    spec = deployment["spec"]["template"]["spec"]
    assert deployment["metadata"]["namespace"] == "acme-control"
    assert spec["automountServiceAccountToken"] is False
    (container,) = spec["containers"]
    assert container["image"] == profile.as_dict()["guard"]["image"]
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert container["volumeMounts"] == [
        {"name": "config", "mountPath": ca.CONFIG_DIR, "readOnly": True}
    ]
    assert deployment["spec"]["template"]["metadata"]["labels"] == {
        cp.GUARD_SERVER_LABEL: profile.digest[:32]
    }
    for bad in ("Bad_Name", "", 7):
        with pytest.raises(ca.ControlAdmissionInvalid):
            ca.render_guard_deployment(profile, config_secret=bad)


def _issue(tmp_path, *, issuer_key=None, issuer=None):
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "acme-guard.acme-control.svc")])
    now = datetime.datetime.now(datetime.UTC)
    builder = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(issuer.subject if issuer else name)
        .public_key(key.public_key())
        .serial_number(2)
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
    )
    if issuer is not None:
        builder = builder.add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(issuer_key.public_key()),
            critical=False,
        )
    cert = builder.sign(issuer_key or key, hashes.SHA256())
    pem = cert.public_bytes(Encoding.PEM)
    (tmp_path / "tls.crt").write_bytes(pem)
    (tmp_path / "tls.key").write_bytes(
        key.private_bytes(
            Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        )
    )
    return pem


def _authority():
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "guard-ca")])
    now = datetime.datetime.now(datetime.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(1)
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(key, hashes.SHA256())
    )
    return key, cert


def _policy_for(ca_pem):
    value = profile_value()
    value["guard"]["server_ca_sha256"] = cp.server_ca_digest(ca_pem)
    return ca.GuardPolicy(
        cp.parse_control_profile(canonical(value)), parse_projection(canonical(projection_value()))
    )


def test_server_refuses_a_certificate_outside_the_pinned_ca(tmp_path):
    key, authority = _authority()
    ca_pem = authority.public_bytes(Encoding.PEM)
    pem = _issue(tmp_path)  # self-signed, not from the pinned CA
    with pytest.raises(ca.ControlAdmissionInvalid):
        ca.ControlAdmissionServer(
            _policy_for(ca_pem),
            certificate_pem=pem,
            certificate_file=str(tmp_path / "tls.crt"),
            key_file=str(tmp_path / "tls.key"),
            ca_pem=ca_pem,
        )
    pem = _issue(tmp_path, issuer_key=key, issuer=authority)
    with pytest.raises(ca.ControlAdmissionInvalid):
        ca.ControlAdmissionServer(
            _policy_for(CA),
            certificate_pem=pem,
            certificate_file=str(tmp_path / "tls.crt"),
            key_file=str(tmp_path / "tls.key"),
            ca_pem=ca_pem,
        )
    assert certificate()  # the shared CA helper stays importable for vectors


def test_server_answers_only_admission_posts_over_tls(tmp_path):
    key, authority = _authority()
    ca_pem = authority.public_bytes(Encoding.PEM)
    pem = _issue(tmp_path, issuer_key=key, issuer=authority)
    server = ca.ControlAdmissionServer(
        _policy_for(ca_pem),
        certificate_pem=pem,
        certificate_file=str(tmp_path / "tls.crt"),
        key_file=str(tmp_path / "tls.key"),
        ca_pem=ca_pem,
    )

    async def scenario():
        runner = server.runner()
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0, ssl_context=server.tls_context)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
        client_tls = ssl.create_default_context(cadata=ca_pem.decode())
        base = f"https://localhost:{port}"
        try:
            async with aiohttp.ClientSession() as session:
                good = request(admitted_pod())
                async with session.post(base + "/validate", json=good, ssl=client_tls) as r:
                    assert r.status == 200
                    body = await r.json()
                    assert body["response"] == {"uid": UID, "allowed": True}
                bad = request(admitted_pod(), user=PROVISIONER)
                async with session.post(base + "/validate", json=bad, ssl=client_tls) as r:
                    assert (await r.json())["response"]["allowed"] is False
                for method, path, data in (
                    ("GET", "/validate", None),
                    ("POST", "/other", json.dumps(good)),
                    ("POST", "/validate", "{not json"),
                    ("POST", "/validate", json.dumps({"request": {}})),
                ):
                    async with session.request(method, base + path, data=data, ssl=client_tls) as r:
                        assert r.status == 403
                        assert await r.text() == ca.MESSAGE
        finally:
            await runner.cleanup()

    asyncio.run(scenario())
