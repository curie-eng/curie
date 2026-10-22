"""Provider gateway launch identity on a disposable minikube profile running Calico.

@spec PROTECTED-COMMISSIONING-8. Opt-in: it runs only with an explicit
kubeconfig file, context and minikube profile of a disposable local cluster
whose API server is on loopback and whose CNI is Calico (its IPAM honors a
requested Pod address, which makes address reuse observable). It never reads
the default kubeconfig or its current context.

The actual ``provider_identity`` and ``commissioning_provider_gateway``
modules run in a Pod behind a Service, with the gateway's own read-only Pod
identity. Only the upstream provider and the two authorities are stand-ins
that record what they would authorize. Launch Pods call the gateway's Service
DNS name over TLS. The test observes the cross Pod, ordinary worker, host
path, retained connection and IP reuse negatives; a negative it cannot
provoke fails the test, because that composition is then unavailable.
Local results qualify no other installation.
"""

from __future__ import annotations

import ipaddress
import json
import os
import secrets
import shutil
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest
import yaml
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from .protected_configuration_vectors import execution_profile

KUBECONFIG = os.environ.get("CURIE_GATEWAY_CLUSTER_KUBECONFIG", "")
CONTEXT = os.environ.get("CURIE_GATEWAY_CLUSTER_CONTEXT", "")
PROFILE = os.environ.get("CURIE_GATEWAY_CLUSTER_PROFILE", "")
EVIDENCE = os.environ.get("CURIE_GATEWAY_CLUSTER_EVIDENCE", "")
pytestmark = pytest.mark.skipif(
    not (KUBECONFIG and CONTEXT and PROFILE),
    reason="needs an explicit disposable local Calico cluster kubeconfig, context and profile",
)

BASE_IMAGE = (
    "python:3.13.15-slim@sha256:9d2e5553305c7c7b0097999bb17187c69b921ccd6bc9d40e4bb5ebe652c00285"
)
GATEWAY_NS, LAUNCH_NS, ORDINARY_NS = "curie-gw", "curie-launch", "curie-ordinary"
SERVICE = f"provider-gateway.{GATEWAY_NS}.svc"
URL = f"https://{SERVICE}:8443/v1/messages"
LAUNCH_A = "b0000000-0000-4000-8000-00000000000a"
LAUNCH_B = "b0000000-0000-4000-8000-00000000000b"
SOURCES = (
    "provider_identity.py",
    "commissioning_provider_gateway.py",
    "protected_configuration.py",
    "protected_launch_registry.py",
)

# The gateway process: the actual modules, a stand-in upstream and recording
# authorities, plus a loopback-only registration channel standing in for the
# in-process mediator registrar (no Service exposes it).
HARNESS = r"""
import asyncio, hashlib, json, ssl, sys
import httpx
from aiohttp import web
from curie_worker import commissioning_provider_gateway as gateway
from curie_worker import provider_identity as identity
from curie_worker.protected_configuration import parse_execution_config

SA = "/var/run/secrets/kubernetes.io/serviceaccount/"

class InCluster:
    def __init__(self):
        token = open(SA + "token").read().strip()
        self.client = httpx.AsyncClient(
            base_url="https://kubernetes.default.svc",
            verify=ssl.create_default_context(cafile=SA + "ca.crt"),
            headers={"Authorization": "Bearer " + token}, trust_env=False, timeout=5)
    async def read(self, namespace, kind, name):
        response = await self.client.get(f"/api/v1/namespaces/{namespace}/{kind}/{name}")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

class Manifest:
    def __init__(self, value, raw):
        self.value = {"runner_image_digest": value["launch"]["runner_image_digest"],
                      "substrate": value["launch"]["substrate"],
                      "execution_config_digest": hashlib.sha256(raw).hexdigest()}
    def as_dict(self):
        return dict(self.value)

def log(*parts):
    print(*parts, flush=True)

class Production:
    async def require_launch(self, launch, *, execution_config_digest):
        log("AUTHORIZED production", launch.launch_id)

class Campaign:
    async def require_launch(self, launch_id, *, namespace, pod_name, pod_uid):
        log("REQUIRE commissioning", launch_id)
    async def consume(self, operation_id, **kwargs):
        log("AUTHORIZED commissioning", kwargs["launch_id"])
        return True

class Upstream:
    async def post(self, raw):
        return gateway.UpstreamReply(200, "application/json", b'{"id":"msg"}')

async def main():
    raw = open("/config/execution_config.json", "rb").read()
    value = json.loads(raw)
    config = parse_execution_config(raw, manifest=Manifest(value, raw))
    prices = gateway.WorstCasePrices(3000000, 3750000, 6000000, 300000, 15000000)
    profile = gateway.GatewayProfile.measured(config, max_input_tokens=200000, prices=prices)
    peers = identity.PeerRegistry(identity.PodFacts(InCluster()))
    server = gateway.ProviderGateway(
        profile=profile, peers=peers,
        production=gateway.ProductionModelAuthorizer(Production(), profile),
        commissioning=gateway.CommissioningModelAuthorizer(Campaign()),
        upstream=Upstream())
    tls = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    tls.load_cert_chain("/tls/tls.crt", "/tls/tls.key")
    runner = server.runner()
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", 8443, ssl_context=tls).start()

    async def register(request):
        try:
            await peers.register(identity.LaunchPeer(**(await request.json())))
        except Exception as error:
            log("REGISTER refused", type(error).__name__)
            return web.json_response({"registered": False})
        return web.json_response({"registered": True})

    async def withdraw(request):
        peers.withdraw((await request.json())["launch_id"])
        return web.json_response({"withdrawn": True})

    admin = web.Application()
    admin.router.add_post("/register", register)
    admin.router.add_post("/withdraw", withdraw)
    admin_runner = web.AppRunner(admin, access_log=None)
    await admin_runner.setup()
    await web.TCPSite(admin_runner, "127.0.0.1", 9000).start()
    log("READY")
    await asyncio.Event().wait()

asyncio.run(main())
"""

ADMIN = r"""
import json, sys, urllib.request
request = urllib.request.Request("http://127.0.0.1:9000/" + sys.argv[1], data=sys.argv[2].encode(),
                                 headers={"Content-Type": "application/json"})
print(urllib.request.urlopen(request, timeout=10).read().decode())
"""

# A launch-side client: one request, or two on one retained TLS connection.
CLIENT = r"""
import http.client, json, os, ssl, sys, time
host, mode, spoof = sys.argv[1], sys.argv[2], sys.argv[3]
body = json.dumps({"model": "example-model", "max_tokens": 16, "thinking": {"type": "disabled"},
                   "messages": [{"role": "user", "content": "ping"}]})
headers = {"Content-Type": "application/json", "X-Forwarded-For": spoof,
           "Forwarded": "for=" + spoof, "X-Real-IP": spoof,
           "X-Curie-Launch": "b0000000-0000-4000-8000-00000000000a"}
context = ssl.create_default_context(cafile="/ca/ca.crt")

def send(connection):
    try:
        connection.request("POST", "/v1/messages", body=body, headers=headers)
        response = connection.getresponse()
        response.read()
        return {"status": response.status, "port": connection.sock.getsockname()[1]}
    except Exception as error:
        return {"error": type(error).__name__}

connection = http.client.HTTPSConnection(host, 8443, context=context, timeout=15)
print(json.dumps(send(connection)), flush=True)
if mode == "retained":
    deadline = time.time() + 120
    while not os.path.exists("/tmp/go") and time.time() < deadline:
        time.sleep(0.2)
    print(json.dumps(send(connection)), flush=True)
"""


def kubectl(*args: str, stdin: str | None = None, check: bool = True) -> str:
    return subprocess.run(
        ["kubectl", "--kubeconfig", KUBECONFIG, "--context", CONTEXT, *args],
        input=stdin,
        capture_output=True,
        text=True,
        check=check,
        timeout=300,
    ).stdout


def apply(documents: list[dict[str, Any]]) -> None:
    kubectl(
        "apply",
        "-f",
        "-",
        stdin=json.dumps({"apiVersion": "v1", "kind": "List", "items": documents}),
    )


def require_disposable_cluster() -> None:
    config = yaml.safe_load(kubectl("config", "view", "--raw", "--minify", "-o", "yaml"))
    origin = config["clusters"][0]["cluster"]["server"]
    assert urlsplit(origin).hostname in ("127.0.0.1", "localhost", "::1"), origin
    nodes = json.loads(kubectl("get", "nodes", "-o", "json"))["items"]
    assert [node["metadata"]["name"] for node in nodes] == [PROFILE], "one-node owned profile"
    calico = kubectl("get", "pods", "-n", "kube-system", "-l", "k8s-app=calico-node", "-o", "name")
    assert calico.strip(), "Calico is required to make address reuse observable"


def tls_material(root: Path) -> None:
    now = datetime.now(UTC)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "gateway-cluster-test-ca")])
    ca = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(hours=2))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    key = ec.generate_private_key(ec.SECP256R1())
    names = [SERVICE, SERVICE + ".cluster.local"]
    leaf = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, SERVICE)]))
        .issuer_name(ca_name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(hours=2))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(n) for n in names]), False)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), False)
        .sign(ca_key, hashes.SHA256())
    )
    (root / "ca.crt").write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    (root / "tls.crt").write_bytes(leaf.public_bytes(serialization.Encoding.PEM))
    (root / "tls.key").write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )


def build_image(root: Path) -> str:
    source = Path(__file__).resolve().parents[1] / "src" / "curie_worker"
    context = root / "image"
    package = context / "curie_worker"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    for name in SOURCES:
        shutil.copy(source / name, package / name)
    (context / "gateway_harness.py").write_text(HARNESS)
    (context / "client.py").write_text(CLIENT)
    (context / "admin.py").write_text(ADMIN)
    (context / "Dockerfile").write_text(
        f"FROM {BASE_IMAGE}\n"
        "RUN pip install --no-cache-dir aiohttp==3.14.3 httpx==0.28.1\n"
        "COPY . /app\nWORKDIR /app\nENV PYTHONPATH=/app\n"
    )
    tag = "curie-gateway-cluster-test:" + secrets.token_hex(6)
    subprocess.run(
        ["minikube", "-p", PROFILE, "image", "build", "-t", tag, str(context)],
        check=True,
        capture_output=True,
        timeout=900,
    )
    return tag


def pod(name: str, namespace: str, image: str, **spec: Any) -> dict[str, Any]:
    annotations = spec.pop("annotations", {})
    volumes = [{"name": "ca", "configMap": {"name": "gateway-ca"}}]
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": namespace, "annotations": annotations},
        "spec": {
            "terminationGracePeriodSeconds": 1,
            "containers": [
                {
                    "name": "main",
                    "image": image,
                    "imagePullPolicy": "Never",
                    "command": ["sleep", "infinity"],
                    "volumeMounts": [{"name": "ca", "mountPath": "/ca"}],
                }
            ],
            "volumes": volumes,
            **spec,
        },
    }


def installation(image: str, root: Path) -> list[dict[str, Any]]:
    def namespace(name: str) -> dict[str, Any]:
        return {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}}

    ca = (root / "ca.crt").read_text()
    documents: list[dict[str, Any]] = [namespace(n) for n in (GATEWAY_NS, LAUNCH_NS, ORDINARY_NS)]
    documents.append(
        {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {"name": "provider-gateway", "namespace": GATEWAY_NS},
        }
    )
    for watched in (LAUNCH_NS, ORDINARY_NS):
        meta = {"name": "provider-gateway-pod-reads", "namespace": watched}
        documents += [
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "Role",
                "metadata": meta,
                "rules": [{"apiGroups": [""], "resources": ["pods"], "verbs": ["get"]}],
            },
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": meta,
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "Role",
                    "name": "provider-gateway-pod-reads",
                },
                "subjects": [
                    {"kind": "ServiceAccount", "name": "provider-gateway", "namespace": GATEWAY_NS}
                ],
            },
        ]
        documents.append(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "gateway-ca", "namespace": watched},
                "data": {"ca.crt": ca},
            }
        )
    config = execution_profile()
    config["runner"]["model_base_url"] = f"https://{SERVICE}:8443"
    documents += [
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "gateway-config", "namespace": GATEWAY_NS},
            "data": {"execution_config.json": json.dumps(config)},
        },
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "gateway-tls", "namespace": GATEWAY_NS},
            "type": "kubernetes.io/tls",
            "stringData": {
                "tls.crt": (root / "tls.crt").read_text(),
                "tls.key": (root / "tls.key").read_text(),
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "provider-gateway",
                "namespace": GATEWAY_NS,
                "labels": {"app": "provider-gateway"},
            },
            "spec": {
                "serviceAccountName": "provider-gateway",
                "containers": [
                    {
                        "name": "gateway",
                        "image": image,
                        "imagePullPolicy": "Never",
                        "command": ["python", "-u", "gateway_harness.py"],
                        "ports": [{"containerPort": 8443}],
                        "readinessProbe": {"tcpSocket": {"port": 8443}, "periodSeconds": 1},
                        "volumeMounts": [
                            {"name": "tls", "mountPath": "/tls"},
                            {"name": "config", "mountPath": "/config"},
                        ],
                    }
                ],
                "volumes": [
                    {"name": "tls", "secret": {"secretName": "gateway-tls"}},
                    {"name": "config", "configMap": {"name": "gateway-config"}},
                ],
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "provider-gateway", "namespace": GATEWAY_NS},
            "spec": {
                "selector": {"app": "provider-gateway"},
                "ports": [{"port": 8443, "targetPort": 8443}],
            },
        },
    ]
    return documents


def observed(namespace: str, name: str) -> dict[str, Any]:
    return json.loads(kubectl("get", "pod", "-n", namespace, name, "-o", "json"))


def wait_ready(namespace: str, name: str) -> dict[str, Any]:
    kubectl("wait", "-n", namespace, "--for=condition=Ready", f"pod/{name}", "--timeout=240s")
    return observed(namespace, name)


def client(namespace: str, name: str, spoof: str = "203.0.113.7") -> dict[str, Any]:
    out = kubectl(
        "exec", "-n", namespace, name, "--", "python", "/app/client.py", SERVICE, "once", spoof
    )
    return json.loads(out.strip().splitlines()[-1])


def admin(verb: str, value: dict[str, Any]) -> dict[str, Any]:
    out = kubectl(
        "exec", "-n", GATEWAY_NS, "provider-gateway", "--",
        "python", "/app/admin.py", verb, json.dumps(value),
    )  # fmt: skip
    return json.loads(out)


def launch(authority: str, launch_id: str, item: dict[str, Any]) -> dict[str, Any]:
    return {
        "authority": authority,
        "launch_id": launch_id,
        "namespace": item["metadata"]["namespace"],
        "pod_name": item["metadata"]["name"],
        "pod_uid": item["metadata"]["uid"],
        "peer_ip": ipaddress.ip_address(item["status"]["podIP"]).compressed,
    }


# The gateway closes an unregistered peer's connection right after the TLS
# handshake; a resolution or routing failure is not that refusal.
CLOSED = {"RemoteDisconnected", "SSLEOFError", "ConnectionResetError", "SSLZeroReturnError"}


def closed(result: dict[str, Any]) -> bool:
    return result.get("error") in CLOSED


def gateway_log() -> str:
    return kubectl("logs", "-n", GATEWAY_NS, "provider-gateway")


def test_gateway_identity_negatives_on_the_actual_substrate(tmp_path: Path) -> None:
    require_disposable_cluster()
    tls_material(tmp_path)
    image = build_image(tmp_path)
    evidence: dict[str, Any] = {"profile": PROFILE, "image": image}
    try:
        apply(installation(image, tmp_path))
        wait_ready(GATEWAY_NS, "provider-gateway")
        apply(
            [
                pod("launch-a", LAUNCH_NS, image),
                pod("launch-b", LAUNCH_NS, image),
                pod("ordinary-worker", ORDINARY_NS, image),
                pod(
                    "host-path",
                    LAUNCH_NS,
                    image,
                    hostNetwork=True,
                    dnsPolicy="ClusterFirstWithHostNet",
                ),
            ]
        )
        a = wait_ready(LAUNCH_NS, "launch-a")
        b = wait_ready(LAUNCH_NS, "launch-b")
        wait_ready(ORDINARY_NS, "ordinary-worker")
        host = wait_ready(LAUNCH_NS, "host-path")

        # Positive: the registered launch Pod is authorized as itself.
        assert admin("register", launch("commissioning", LAUNCH_A, a)) == {"registered": True}
        evidence["positive"] = client(LAUNCH_NS, "launch-a")
        assert evidence["positive"]["status"] == 200
        assert f"AUTHORIZED commissioning {LAUNCH_A}" in gateway_log()

        # Cross Pod: forwarded headers naming launch A give another Pod nothing.
        a_ip = a["status"]["podIP"]
        evidence["cross_pod_unregistered"] = client(LAUNCH_NS, "launch-b", spoof=a_ip)
        assert closed(evidence["cross_pod_unregistered"]), evidence["cross_pod_unregistered"]
        assert admin("register", launch("production", LAUNCH_B, b)) == {"registered": True}
        before = gateway_log().count(f"AUTHORIZED commissioning {LAUNCH_A}")
        evidence["cross_pod_registered"] = client(LAUNCH_NS, "launch-b", spoof=a_ip)
        assert evidence["cross_pod_registered"]["status"] == 200
        log = gateway_log()
        assert f"AUTHORIZED production {LAUNCH_B}" in log
        assert log.count(f"AUTHORIZED commissioning {LAUNCH_A}") == before

        # Ordinary worker: a Pod outside the launch registrations is refused.
        evidence["ordinary_worker"] = client(ORDINARY_NS, "ordinary-worker", spoof=a_ip)
        assert closed(evidence["ordinary_worker"]), evidence["ordinary_worker"]

        # Host path: a host-network Pod connects as the node and cannot be registered.
        assert host["spec"]["hostNetwork"] is True
        assert host["status"]["podIP"] == host["status"]["hostIP"]
        evidence["host_path_registration"] = admin(
            "register", launch("commissioning", LAUNCH_B.replace("b", "c", 1), host)
        )
        assert evidence["host_path_registration"] == {"registered": False}
        evidence["host_path"] = client(LAUNCH_NS, "host-path", spoof=a_ip)
        assert closed(evidence["host_path"]), evidence["host_path"]

        # Retained connection: withdrawal refuses the next request on the same connection.
        process = subprocess.Popen(
            [
                "kubectl", "--kubeconfig", KUBECONFIG, "--context", CONTEXT,
                "exec", "-n", LAUNCH_NS, "launch-a", "--",
                "python", "/app/client.py", SERVICE, "retained", "203.0.113.7",
            ],
            stdout=subprocess.PIPE,
            text=True,
        )  # fmt: skip
        assert process.stdout is not None
        first = json.loads(process.stdout.readline())
        admin("withdraw", {"launch_id": LAUNCH_A})
        kubectl("exec", "-n", LAUNCH_NS, "launch-a", "--", "touch", "/tmp/go")
        second = json.loads(process.stdout.readline())
        process.wait(timeout=60)
        evidence["retained_connection"] = {"first": first, "second": second}
        assert first["status"] == 200 and second["status"] == 403
        assert first["port"] == second["port"], "the second request must reuse the connection"

        # IP reuse: a new Pod given launch A's address after A is gone is refused,
        # while the registry still maps that address to launch A.
        assert admin("register", launch("commissioning", LAUNCH_A, a)) == {"registered": True}
        kubectl("delete", "pod", "-n", LAUNCH_NS, "launch-a", "--wait=true", "--timeout=120s")
        reuse = pod(
            "reused-address",
            LAUNCH_NS,
            image,
            annotations={"cni.projectcalico.org/ipAddrs": json.dumps([a_ip])},
        )
        deadline = time.time() + 120
        while True:
            # Calico releases the address asynchronously after the Pod is gone.
            apply([reuse])
            try:
                reused = wait_ready(LAUNCH_NS, "reused-address")
                break
            except subprocess.CalledProcessError:
                kubectl("delete", "pod", "-n", LAUNCH_NS, "reused-address", "--wait=true")
                assert time.time() < deadline, "address reuse could not be provoked"
        evidence["ip_reuse_address"] = {"old": a_ip, "new": reused["status"]["podIP"]}
        assert reused["status"]["podIP"] == a_ip, "address reuse was not observed"
        assert reused["metadata"]["uid"] != a["metadata"]["uid"]
        before = gateway_log().count(f"AUTHORIZED commissioning {LAUNCH_A}")
        evidence["ip_reuse"] = client(LAUNCH_NS, "reused-address")
        assert evidence["ip_reuse"]["status"] == 403
        assert gateway_log().count(f"AUTHORIZED commissioning {LAUNCH_A}") == before
        evidence["result"] = "every negative observed"
    finally:
        evidence.setdefault("result", "incomplete")
        if EVIDENCE:
            Path(EVIDENCE).write_text(json.dumps(evidence, indent=2, sort_keys=True))
        for namespace in (LAUNCH_NS, ORDINARY_NS, GATEWAY_NS):
            kubectl("delete", "namespace", namespace, "--wait=false", check=False)
