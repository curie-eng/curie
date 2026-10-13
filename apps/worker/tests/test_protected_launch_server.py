"""Owned mediator refusal and authenticated factual cleanup; no runtime qualification."""

from __future__ import annotations

import importlib
import importlib.util
from copy import deepcopy

import pytest


def product():
    name = "curie_worker.protected_launch_kubernetes"
    assert importlib.util.find_spec(name) is not None, "owned Kubernetes launch control absent"
    return importlib.import_module(name)


def terminated_pod():
    return {
        "metadata": {
            "name": "acme-pod",
            "namespace": "acme-runtime",
            "uid": "88888888-8888-4888-8888-888888888888",
            "resourceVersion": "7",
            "finalizers": ["curietech.ai/protected-launch"],
            "deletionTimestamp": "2026-10-12T00:00:00Z",
        },
        "status": {
            "containerStatuses": [
                {
                    "name": "runner",
                    "restartCount": 0,
                    "containerID": "containerd://" + "a" * 64,
                    "state": {
                        "terminated": {
                            "exitCode": 0,
                            "finishedAt": "2026-10-12T00:00:01Z",
                            "startedAt": "2026-10-12T00:00:00Z",
                            "containerID": "containerd://" + "a" * 64,
                        }
                    },
                }
            ]
        },
    }


def proof(pod):
    return product().termination_observation(
        pod,
        namespace="acme-runtime",
        pod_name="acme-pod",
        pod_uid="88888888-8888-4888-8888-888888888888",
        container_id="containerd://" + "a" * 64,
    )


def test_exact_terminated_runner_observation():
    observed = proof(terminated_pod())
    assert observed.resource_version == "7"
    assert observed.exit_code == 0
    assert observed.container_id == "containerd://" + "a" * 64


@pytest.mark.parametrize(
    "mutation", ["uid", "container", "restart", "finalizer", "running", "exit", "finished", "other"]
)
def test_retirement_refuses_missing_or_substituted_proof(mutation):
    p = deepcopy(terminated_pod())
    r = p["status"]["containerStatuses"][0]
    if mutation == "uid":
        p["metadata"]["uid"] = "99999999-9999-4999-8999-999999999999"
    elif mutation == "container":
        r["containerID"] = "containerd://" + "b" * 64
    elif mutation == "restart":
        r["restartCount"] = 1
    elif mutation == "finalizer":
        p["metadata"]["finalizers"] = []
    elif mutation == "running":
        r["state"] = {"running": {}}
    elif mutation == "other":
        r["name"] = "sidecar"
    else:
        del r["state"]["terminated"]["exitCode" if mutation == "exit" else "finishedAt"]
    with pytest.raises(product().LaunchControlUnavailable):
        proof(p)


def test_server_is_an_owned_entrypoint():
    name = "curie_worker.protected_launch_server"
    assert importlib.util.find_spec(name) is not None, "owned mediator server absent"
    m = importlib.import_module(name)
    assert hasattr(m, "ProtectedLaunchServer")
    assert hasattr(m, "serve")


def test_real_https_kubernetes_transport_and_assignment_substitution(tmp_path):
    import asyncio
    import base64

    from aiohttp import web

    from .protected_launch_endpoint_helpers import certificates, endpoint

    root = certificates(tmp_path / "tls")
    p = product()
    ns, name = "acme-runtime", "acme-secret"
    uid = "88888888-8888-4888-8888-888888888888"
    source = {
        "runner-token": "synthetic-runner-secret",
        "history-token": "synthetic-history",
        "model-credential": "synthetic-model",
    }
    document = p.secret_document(ns, name, source)
    document["metadata"].update(uid=uid, resourceVersion="1")
    calls = []

    async def external(request):
        assert request.transport.get_extra_info("ssl_object").getpeercert(binary_form=True)
        if request.headers.get("Authorization") != "Bearer synthetic-control":
            return web.Response(status=403)
        calls.append(request.method)
        return web.json_response(document)

    async def run():
        async with endpoint(root, external) as origin:
            api = p.KubernetesLaunchAPI(
                p.KubernetesTLS(
                    origin,
                    str(root / "ca.pem"),
                    str(root / "worker.pem"),
                    str(root / "worker.key"),
                    "synthetic-control",
                ),
                authority_domain_id="55555555-5555-4555-8555-555555555555",
            )
            try:
                observed = await api.read(ns, "secrets", name)
                p.verify_secret(
                    observed, namespace=ns, name=name, uid=uid, version="1", source=source
                )
                for mutation in ("immutable", "uid", "resourceVersion", "bytes", "keys"):
                    modified = deepcopy(observed)
                    if mutation == "immutable":
                        modified["immutable"] = False
                    elif mutation in {"uid", "resourceVersion"}:
                        modified["metadata"][mutation] = "substituted"
                    elif mutation == "bytes":
                        modified["data"]["model-credential"] = base64.b64encode(
                            b"substituted"
                        ).decode()
                    else:
                        modified["data"]["extra"] = "eA=="
                    with pytest.raises(p.LaunchControlUnavailable):
                        p.verify_secret(
                            modified, namespace=ns, name=name, uid=uid, version="1", source=source
                        )
                await api.delete(ns, "pods", "acme-pod", uid)
            finally:
                await api.close()
        assert calls == ["GET", "DELETE"]

    asyncio.run(run())


def test_kubernetes_transport_refuses_wrong_ca_name_missing_client_and_redirect(tmp_path):
    import asyncio
    import ssl

    import httpx
    from aiohttp import web

    from .protected_launch_endpoint_helpers import certificates, context, endpoint

    root = certificates(tmp_path / "tls")
    other = certificates(tmp_path / "other")
    p = product()

    async def external(request):
        return web.Response(status=302, headers={"Location": "https://example.com/forbidden"})

    async def run():
        async with endpoint(root, external) as origin:
            api = p.KubernetesLaunchAPI(
                p.KubernetesTLS(
                    origin,
                    str(root / "ca.pem"),
                    str(root / "worker.pem"),
                    str(root / "worker.key"),
                    "synthetic-control",
                ),
                authority_domain_id="55555555-5555-4555-8555-555555555555",
            )
            try:
                with pytest.raises(p.LaunchControlUnavailable):
                    await api.read("acme-runtime", "pods", "acme-pod")
            finally:
                await api.close()
            for ctx in (context(other), ssl.create_default_context(cafile=str(root / "ca.pem"))):
                async with httpx.AsyncClient(verify=ctx, trust_env=False) as client:
                    with pytest.raises(httpx.TransportError):
                        await client.get(origin)
            ctx = context(root)
            async with httpx.AsyncClient(verify=ctx, trust_env=False) as client:
                with pytest.raises(httpx.TransportError):
                    await client.get(origin, extensions={"sni_hostname": "wrong.example.com"})

    asyncio.run(run())


def test_server_tls_requires_actual_selected_certificate_pin(tmp_path):
    from cryptography import x509
    from cryptography.hazmat.primitives import serialization
    from curie_worker.protected_launch_server import ServerConfig, public_key_identity

    from .protected_launch_endpoint_helpers import certificates

    root = certificates(tmp_path / "tls")
    cert = x509.load_pem_x509_certificate((root / "server.pem").read_bytes())
    pin = public_key_identity(cert.public_bytes(serialization.Encoding.DER))
    valid = ServerConfig(
        "/private-registry",
        "127.0.0.1",
        8081,
        str(root / "ca.pem"),
        str(root / "server.pem"),
        str(root / "server.key"),
        pin,
        "http",
        "svc.cluster.local",
        None,
    )
    assert valid.tls_context().verify_mode.name == "CERT_REQUIRED"
    from dataclasses import replace

    with pytest.raises(product().LaunchControlUnavailable):
        replace(valid, public_key_sha256="0" * 64).tls_context()
    with pytest.raises(product().LaunchControlUnavailable):
        replace(valid, runner_scheme="ambient").tls_context()


def test_closed_bounded_external_observation_refuses_duplicate_fields():
    p = product()
    for raw in (b'{"status":{},"status":{"private":"marker"}}', b'{"status":NaN}', b"x" * 1048577):
        with pytest.raises((p.LaunchControlUnavailable, ValueError)):
            p.decode_observation(raw)
