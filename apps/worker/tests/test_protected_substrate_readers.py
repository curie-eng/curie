"""Offline control-plane plumbing; these fixtures do not qualify a runtime.

Shapes: https://docs.docker.com/reference/api/engine/version/v1.47/
and https://kubernetes.io/docs/reference/kubernetes-api/core/pod-v1/.
Only the narrow external control-plane transport is faked.
"""

from __future__ import annotations

import copy
import hashlib
import importlib
import importlib.util
from dataclasses import FrozenInstanceError, replace

import pytest
from aci_protocol import BootEnv
from curie_worker.sandbox.types import SandboxHandle

DOMAIN = "11111111-1111-4111-8111-111111111111"
CONTAINER = "c" * 64
pytestmark = pytest.mark.anyio

TOKEN = "synthetic-runner-token"
START = "2026-01-01T00:00:00Z"


def api():
    spec = importlib.util.find_spec("curie_worker.protected_substrate_readers")
    assert spec is not None, "owned substrate readers are absent"
    return importlib.import_module(spec.name)


def handle(kind="docker"):
    return SandboxHandle(
        thread_key="protected-session",
        claim_name="claim-example",
        sandbox_name="runner-example",
        namespace="example",
        service_fqdn="127.0.0.1"
        if kind == "docker"
        else "service-example.example.svc.cluster.local",
        port=18080 if kind == "docker" else 8080,
        session_id="session-example",
        token=TOKEN,
    )


def receipt(kind="docker"):
    return api().ProtectedLaunchReceipt(
        event_id="event-example",
        execution_session_key="protected-session",
        runtime_generation=1,
        manifest_digest="a" * 64,
        handle=handle(kind),
        authority_domain_id=DOMAIN,
        launch_identity="template-example",
        launch_config_sha256="b" * 64,
        workload_uid=CONTAINER if kind == "docker" else "pod-uid-example",
        container_id=CONTAINER if kind == "docker" else "containerd://" + CONTAINER,
        started_at=START,
        token_sha256=hashlib.sha256(TOKEN.encode()).hexdigest(),
        sandbox_uid=None if kind == "docker" else "sandbox-uid-example",
        service_name=None if kind == "docker" else "service-example",
        service_uid=None if kind == "docker" else "service-uid-example",
    )


class DockerPlane:
    authority_domain_id = DOMAIN
    endpoint_host = "127.0.0.1"

    def __init__(self):
        self.container = {
            "Id": CONTAINER,
            "Name": "/runner-example",
            "Image": "sha256:" + "d" * 64,
            "Config": {
                "Image": "acme-runner@sha256:" + "a" * 64,
                "Env": [BootEnv.env_key("runner_token") + "=" + TOKEN],
            },
            "HostConfig": {"NetworkMode": "bridge"},
            "RestartCount": 0,
            "State": {"Running": True, "Paused": False, "Restarting": False, "StartedAt": START},
            "NetworkSettings": {
                "Ports": {"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": "18080"}]}
            },
        }
        self.image = {
            "Id": "sha256:" + "d" * 64,
            "Os": "linux",
            "Architecture": "amd64",
            "RepoDigests": ["acme-runner@sha256:" + "a" * 64],
        }

    async def inspect_container(self, container_id):
        assert container_id == CONTAINER
        return copy.deepcopy(self.container)

    async def inspect_image(self, image_id):
        assert image_id == "sha256:" + "d" * 64
        return copy.deepcopy(self.image)


async def test_docker_reader_keeps_actual_identity_and_platform_separate_from_manifest():
    facts = await api().DockerSubstrateReader(DockerPlane()).observe(receipt(), handle=handle())
    assert facts.workload_uid == CONTAINER
    assert facts.image_id == "sha256:" + "d" * 64
    assert facts.declared_image == "acme-runner@sha256:" + "a" * 64
    assert facts.platform_os == "linux" and facts.platform_architecture == "amd64"
    assert facts.started_at == START
    assert not hasattr(facts, "qualified") and not hasattr(facts, "authorized")
    assert TOKEN not in repr(facts) and TOKEN not in repr(receipt())
    with pytest.raises(FrozenInstanceError):
        facts.image_id = "other"


@pytest.mark.parametrize(
    "case",
    [
        "authority",
        "handle-token",
        "handle-target",
        "object",
        "start",
        "restart",
        "env-token",
        "duplicate-token",
        "ordinary-name",
        "dial-target",
        "image-object",
        "missing-platform",
    ],
)
async def test_docker_reader_refuses_replacement_and_unowned_dial_target(case):
    plane, selected = DockerPlane(), handle()
    if case == "authority":
        plane.authority_domain_id = "other-domain"
    elif case == "handle-token":
        selected = replace(selected, token="other-token")
    elif case == "handle-target":
        selected = replace(selected, port=18081)
    elif case == "object":
        plane.container["Id"] = "e" * 64
    elif case == "start":
        plane.container["State"]["StartedAt"] = "2026-01-02T00:00:00Z"
    elif case == "restart":
        plane.container["RestartCount"] = 1
    elif case == "env-token":
        plane.container["Config"]["Env"] = [BootEnv.env_key("runner_token") + "=wrong"]
    elif case == "duplicate-token":
        plane.container["Config"]["Env"] *= 2
    elif case == "ordinary-name":
        plane.container["Name"] = "/ordinary-runner"
    elif case == "dial-target":
        plane.container["NetworkSettings"]["Ports"]["8080/tcp"][0]["HostPort"] = "18081"
    elif case == "image-object":
        plane.image["Id"] = "sha256:" + "e" * 64
    else:
        plane.image.pop("Architecture")
    with pytest.raises(api().SubstrateFactsUnavailable):
        await api().DockerSubstrateReader(plane).observe(receipt(), handle=selected)


class KubernetesPlane:
    authority_domain_id = DOMAIN

    def __init__(self):
        self.pod = {
            "metadata": {
                "namespace": "example",
                "name": "runner-example",
                "uid": "pod-uid-example",
                "ownerReferences": [
                    {
                        "apiVersion": "agents.x-k8s.io/v1beta1",
                        "kind": "Sandbox",
                        "name": "runner-example",
                        "uid": "sandbox-uid-example",
                        "controller": True,
                    }
                ],
            },
            "spec": {
                "nodeName": "node-example",
                "containers": [
                    {
                        "name": "runner",
                        "image": "acme-runner@sha256:" + "a" * 64,
                        "env": [{"name": BootEnv.env_key("runner_token"), "value": TOKEN}],
                    }
                ],
            },
            "status": {
                "phase": "Running",
                "podIP": "192.0.2.10",
                "containerStatuses": [
                    {
                        "name": "runner",
                        "imageID": "containerd://sha256:" + "d" * 64,
                        "containerID": "containerd://" + CONTAINER,
                        "restartCount": 0,
                        "ready": True,
                        "state": {"running": {"startedAt": START}},
                    }
                ],
            },
        }
        self.node = {
            "metadata": {"name": "node-example", "uid": "node-uid-example"},
            "status": {
                "nodeInfo": {
                    "operatingSystem": "linux",
                    "architecture": "amd64",
                    "containerRuntimeVersion": "containerd://2.0.0",
                }
            },
        }
        self.service = {
            "metadata": {
                "name": "service-example",
                "namespace": "example",
                "uid": "service-uid-example",
            },
            "spec": {
                "type": "ClusterIP",
                "clusterIP": "None",
                "ports": [{"port": 8080, "targetPort": 8080, "protocol": "TCP"}],
            },
        }
        self.slices = {
            "items": [
                {
                    "metadata": {
                        "namespace": "example",
                        "ownerReferences": [
                            {
                                "kind": "Service",
                                "name": "service-example",
                                "uid": "service-uid-example",
                                "controller": True,
                            }
                        ],
                    },
                    "ports": [{"port": 8080, "protocol": "TCP"}],
                    "endpoints": [
                        {
                            "addresses": ["192.0.2.10"],
                            "conditions": {"ready": True},
                            "targetRef": {
                                "kind": "Pod",
                                "namespace": "example",
                                "name": "runner-example",
                                "uid": "pod-uid-example",
                            },
                        }
                    ],
                }
            ]
        }

    async def read_pod(self, namespace, name):
        assert (namespace, name) == ("example", "runner-example")
        return copy.deepcopy(self.pod)

    async def read_node(self, name):
        assert name == "node-example"
        return copy.deepcopy(self.node)

    async def read_service(self, namespace, name):
        assert (namespace, name) == ("example", "service-example")
        return copy.deepcopy(self.service)

    async def list_endpoint_slices(self, namespace, service_name):
        assert (namespace, service_name) == ("example", "service-example")
        return copy.deepcopy(self.slices)


async def test_kubernetes_reader_binds_pod_start_owner_and_service_target():
    facts = await (
        api()
        .KubernetesSubstrateReader(KubernetesPlane())
        .observe(
            receipt("kubernetes"),
            handle=handle("kubernetes"),
        )
    )
    assert facts.workload_uid == "pod-uid-example"
    assert facts.container_id == "containerd://" + CONTAINER
    assert facts.image_id == "containerd://sha256:" + "d" * 64
    assert facts.runtime_profile == "containerd://2.0.0"
    assert facts.node_uid == "node-uid-example"


@pytest.mark.parametrize(
    "case",
    [
        "start",
        "owner",
        "token-reference",
        "token",
        "node",
        "service-uid",
        "slice-owner",
        "target-uid",
        "extra-target",
        "missing-target",
        "pagination",
        "deleting-service",
    ],
)
async def test_kubernetes_reader_refuses_missing_or_replaced_launch_provenance(case):
    plane = KubernetesPlane()
    if case == "start":
        plane.pod["status"]["containerStatuses"][0]["state"]["running"]["startedAt"] = "other"
    elif case == "owner":
        plane.pod["metadata"]["ownerReferences"][0]["uid"] = "ordinary-sandbox"
    elif case == "token-reference":
        plane.pod["spec"]["containers"][0]["env"] = [
            {
                "name": BootEnv.env_key("runner_token"),
                "valueFrom": {"secretKeyRef": {"name": "tokens", "key": "token"}},
            }
        ]
    elif case == "token":
        plane.pod["spec"]["containers"][0]["env"][0]["value"] = "other-token"
    elif case == "node":
        plane.node["metadata"]["name"] = "other-node"
    elif case == "service-uid":
        plane.service["metadata"]["uid"] = "other-service"
    elif case == "slice-owner":
        plane.slices["items"][0]["metadata"]["ownerReferences"][0]["uid"] = "other-service"
    elif case == "target-uid":
        plane.slices["items"][0]["endpoints"][0]["targetRef"]["uid"] = "ordinary-pod"
    elif case == "extra-target":
        plane.slices["items"][0]["endpoints"].append(
            copy.deepcopy(plane.slices["items"][0]["endpoints"][0])
        )
    elif case == "missing-target":
        plane.slices["items"][0]["endpoints"][0].pop("targetRef")
    elif case == "pagination":
        plane.slices["metadata"] = {"continue": "more"}
    else:
        plane.service["metadata"]["deletionTimestamp"] = START
    with pytest.raises(api().SubstrateFactsUnavailable):
        await (
            api()
            .KubernetesSubstrateReader(plane)
            .observe(receipt("kubernetes"), handle=handle("kubernetes"))
        )


async def test_transport_failure_is_redacted_without_source_exception_chain():
    class Unavailable(DockerPlane):
        async def inspect_container(self, container_id):
            raise RuntimeError("private-raw-snapshot " + TOKEN)

    with pytest.raises(api().SubstrateFactsUnavailable) as error:
        await api().DockerSubstrateReader(Unavailable()).observe(receipt(), handle=handle())
    assert TOKEN not in str(error.value)
    assert error.value.__cause__ is None and error.value.__suppress_context__


async def test_engine_client_refuses_missing_client_authentication():
    import ssl

    with pytest.raises(api().SubstrateFactsUnavailable):
        api().DockerEngineControlPlane(
            authority_domain_id=DOMAIN,
            endpoint="https://engine.example.com",
            tls_context=ssl.create_default_context(),
        )


@pytest.mark.parametrize("case", ["redirect", "oversized", "invalid-json", "error", "encoded"])
async def test_selected_kubernetes_transport_bounds_and_redacts_responses(monkeypatch, case):
    import ssl

    import httpx

    client_type = httpx.AsyncClient

    def transport(request):
        assert request.url.host == "api.example.com"
        assert request.url.path == "/api/v1/namespaces/example/pods/runner-example"
        assert request.headers["Authorization"] == "Bearer synthetic-api-token"
        if case == "redirect":
            return httpx.Response(302, headers={"Location": "https://other.example.com"})
        if case == "oversized":
            return httpx.Response(200, content=b"x" * 1_048_577)
        if case == "invalid-json":
            return httpx.Response(200, content=TOKEN)
        if case == "encoded":
            return httpx.Response(200, json={}, headers={"Content-Encoding": "unsupported"})
        raise httpx.ConnectError(TOKEN)

    def selected_client(**kwargs):
        assert kwargs["trust_env"] is False and kwargs["follow_redirects"] is False
        return client_type(**kwargs, transport=httpx.MockTransport(transport))

    monkeypatch.setattr(httpx, "AsyncClient", selected_client)
    control = api().KubernetesAPIControlPlane(
        authority_domain_id=DOMAIN,
        endpoint="https://api.example.com",
        tls_context=ssl.create_default_context(),
        token="synthetic-api-token",
    )
    try:
        with pytest.raises(api().SubstrateFactsUnavailable) as error:
            await control.read_pod("example", "runner-example")
        assert TOKEN not in str(error.value) and error.value.__suppress_context__
    finally:
        await control.aclose()


async def test_remote_daemon_cannot_attest_worker_loopback_dial_target():
    plane = DockerPlane()
    plane.endpoint_host = "engine.example.com"
    with pytest.raises(api().SubstrateFactsUnavailable):
        await api().DockerSubstrateReader(plane).observe(receipt(), handle=handle())


async def test_authenticated_engine_transport_keeps_image_and_container_reads_distinct(
    monkeypatch, tmp_path
):
    import ssl
    from datetime import UTC, datetime, timedelta

    import httpx
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    # Disposable local client identity; no provider or daemon is contacted.
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "synthetic-client")])
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(1)
        .not_valid_before(now)
        .not_valid_after(now + timedelta(hours=1))
        .sign(key, hashes.SHA256())
    )
    cert_file, key_file = tmp_path / "client.pem", tmp_path / "client.key"
    cert_file.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_file.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    key_file.chmod(0o600)
    plane = DockerPlane()
    client_type = httpx.AsyncClient

    def transport(request):
        assert request.url.host == "127.0.0.1"
        if request.url.path == "/v1.47/containers/" + CONTAINER + "/json":
            return httpx.Response(200, json=plane.container)
        if request.url.path == "/v1.47/images/sha256:" + "d" * 64 + "/json":
            return httpx.Response(200, json=plane.image)
        pytest.fail("unowned control-plane read")

    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: client_type(
            **kwargs,
            transport=httpx.MockTransport(transport),
        ),
    )
    control = api().DockerEngineControlPlane(
        authority_domain_id=DOMAIN,
        endpoint="https://127.0.0.1:2376",
        tls_context=ssl.create_default_context(),
        client_certificate_file=str(cert_file),
        client_key_file=str(key_file),
    )
    try:
        facts = await api().DockerSubstrateReader(control).observe(receipt(), handle=handle())
        assert facts.image_id == "sha256:" + "d" * 64
        assert facts.workload_uid == CONTAINER
    finally:
        await control.aclose()


async def test_authenticated_kubernetes_transport_reads_only_owned_objects(monkeypatch):
    import ssl

    import httpx

    plane = KubernetesPlane()
    client_type = httpx.AsyncClient
    snapshots = {
        "/api/v1/namespaces/example/pods/runner-example": plane.pod,
        "/api/v1/namespaces/example/services/service-example": plane.service,
        "/apis/discovery.k8s.io/v1/namespaces/example/endpointslices": plane.slices,
        "/api/v1/nodes/node-example": plane.node,
    }

    def transport(request):
        assert request.url.host == "api.example.com" and request.method == "GET"
        assert request.headers["Authorization"] == "Bearer synthetic-api-token"
        assert request.url.path in snapshots
        if request.url.path.endswith("endpointslices"):
            assert (
                request.url.params["labelSelector"] == "kubernetes.io/service-name=service-example"
            )
            assert request.url.params["limit"] == "16"
        return httpx.Response(200, json=snapshots[request.url.path])

    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: client_type(
            **kwargs,
            transport=httpx.MockTransport(transport),
        ),
    )
    control = api().KubernetesAPIControlPlane(
        authority_domain_id=DOMAIN,
        endpoint="https://api.example.com",
        tls_context=ssl.create_default_context(),
        token="synthetic-api-token",
    )
    try:
        facts = await (
            api()
            .KubernetesSubstrateReader(control)
            .observe(
                receipt("kubernetes"),
                handle=handle("kubernetes"),
            )
        )
        assert facts.workload_uid == "pod-uid-example"
        assert facts.node_uid == "node-uid-example"
    finally:
        await control.aclose()


async def test_headless_service_does_not_translate_service_port_to_endpoint_port():
    # https://kubernetes.io/docs/concepts/services-networking/service/#headless-services
    # Without a VIP/proxy a DNS answer points directly at the Pod's endpoint.
    plane = KubernetesPlane()
    selected = replace(handle("kubernetes"), port=18081)
    selected_receipt = replace(receipt("kubernetes"), handle=selected)
    plane.service["spec"]["ports"][0]["port"] = 18081
    with pytest.raises(api().SubstrateFactsUnavailable):
        await api().KubernetesSubstrateReader(plane).observe(selected_receipt, handle=selected)


def exception_material(error):
    material, pending, seen = [], [error], set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        material.extend([str(current), repr(current), str(getattr(current, "doc", ""))])
        pending.extend(
            value for value in (current.__cause__, current.__context__) if value is not None
        )
    return " ".join(material)


async def test_redacted_reader_exception_detaches_nested_source_context():
    import json

    class Failed(DockerPlane):
        async def inspect_container(self, container_id):
            try:
                json.loads(TOKEN)
            except json.JSONDecodeError:
                raise RuntimeError("source failed " + TOKEN) from None

    with pytest.raises(api().SubstrateFactsUnavailable) as error:
        await api().DockerSubstrateReader(Failed()).observe(receipt(), handle=handle())
    assert TOKEN not in exception_material(error.value)
    assert error.value.__context__ is None and error.value.__cause__ is None


@pytest.mark.parametrize("phase", ["headers", "body"])
async def test_request_deadline_cancels_header_and_body_acquisition(monkeypatch, phase):
    import asyncio
    import ssl
    import time

    import httpx

    finished = False

    class DelayedBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            nonlocal finished
            try:
                await asyncio.sleep(6.0)
                yield b"{}"
            finally:
                finished = True

    async def async_headers(request):
        nonlocal finished
        if phase == "body":
            return httpx.Response(200, stream=DelayedBody())
        try:
            await asyncio.sleep(6.0)
            return httpx.Response(200, json={})
        finally:
            finished = True

    client_type = httpx.AsyncClient
    transport = httpx.MockTransport(async_headers)
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: client_type(**kwargs, transport=transport),
    )
    control = api().KubernetesAPIControlPlane(
        authority_domain_id=DOMAIN,
        endpoint="https://api.example.com",
        tls_context=ssl.create_default_context(),
        token="synthetic-api-token",
    )
    started = time.monotonic()
    try:
        with pytest.raises(api().SubstrateFactsUnavailable):
            await control.read_pod("example", "runner-example")
        elapsed = time.monotonic() - started
        assert elapsed < 5.15, "request acquisition exceeded the entire request deadline"
        assert finished, "timed-out transport work was not joined"
    finally:
        await control.aclose()


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def test_parent_cancellation_is_preserved_and_transport_work_is_joined(monkeypatch):
    import asyncio
    import ssl

    import httpx

    entered, exited = asyncio.Event(), asyncio.Event()
    client_type = httpx.AsyncClient

    async def headers(request):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            exited.set()

    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: client_type(
            **kwargs,
            transport=httpx.MockTransport(headers),
        ),
    )
    control = api().KubernetesAPIControlPlane(
        authority_domain_id=DOMAIN,
        endpoint="https://api.example.com",
        tls_context=ssl.create_default_context(),
        token="synthetic-api-token",
    )
    task = asyncio.create_task(control.read_pod("example", "runner-example"))
    try:
        async with asyncio.timeout(1.0):
            await entered.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert exited.is_set()
    finally:
        task.cancel()
        await control.aclose()


async def test_malformed_json_is_detached_from_entire_exception_chain(monkeypatch):
    import ssl

    import httpx

    client_type = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: client_type(
            **kwargs,
            transport=httpx.MockTransport(lambda request: httpx.Response(200, content=TOKEN)),
        ),
    )
    control = api().KubernetesAPIControlPlane(
        authority_domain_id=DOMAIN,
        endpoint="https://api.example.com",
        tls_context=ssl.create_default_context(),
        token="synthetic-api-token",
    )
    try:
        with pytest.raises(api().SubstrateFactsUnavailable) as error:
            await control.read_pod("example", "runner-example")
        assert TOKEN not in exception_material(error.value)
        assert error.value.__context__ is None and error.value.__cause__ is None
    finally:
        await control.aclose()


async def test_reader_failure_does_not_attach_an_active_caller_exception():
    import json

    class Failed(DockerPlane):
        async def inspect_container(self, container_id):
            raise RuntimeError("control plane failed " + TOKEN)

    try:
        json.loads(TOKEN)
    except json.JSONDecodeError:
        with pytest.raises(api().SubstrateFactsUnavailable) as error:
            await api().DockerSubstrateReader(Failed()).observe(receipt(), handle=handle())
        assert TOKEN not in exception_material(error.value)
        assert error.value.__context__ is None and error.value.__cause__ is None
