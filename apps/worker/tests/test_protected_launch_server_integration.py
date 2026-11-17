"""Actual issuer/broker/SQLite/HTTPS tests; only external Kubernetes/runner are synthetic.

These source tests do not qualify any image, cluster, provider or deployment.
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import importlib.util
import json
import sys
import types
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import httpx
from aiohttp import web
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from curie_protected_hooks.artifact_measurement import ArtifactMeasurementDirectory
from curie_protected_hooks.authority_records import parse_qualification
from curie_protected_hooks.worker_transport import AuthenticatedWorkerClient
from curie_protected_hooks.worker_verification import verification_acl_rules
from curie_worker.protected_configuration import parse_execution_config
from curie_worker.protected_kubernetes_projection import ProjectionDirectory
from curie_worker.protected_launch_kubernetes import KubernetesLaunchAPI, KubernetesTLS
from curie_worker.protected_launch_server import (
    ProtectedLaunchServer,
    ProvisionedRuntime,
    ProvisionedWorker,
    ServerConfig,
    public_key_identity,
)

from .protected_configuration_vectors import canonical, execution_profile, manifest_for
from .protected_launch_endpoint_helpers import certificates, context, endpoint
from .test_protected_kubernetes_projection import projection_value

# Reuse the actual owned TLS fixture (no broker mock); imports remain test-only.
name = "protected_launch_broker_tests"
package = types.ModuleType(name)
package.__path__ = [str(Path(__file__).resolve().parents[3] / "packages/protected-hooks/tests")]
sys.modules[name] = package
wb = importlib.import_module(name + ".worker_broker")
ab = importlib.import_module(name + ".admission_broker")
pworker_service = wb.pworker_service
owned_worker = wb.owned_worker


class ScopedFixtureCredentials:
    def model_credential(self, reference):
        assert reference == {"id": "credential/example-model", "generation": "1"}
        return "synthetic-model-credential"

    def history_token(self, reference, *, agent_id, transcript_binding, expires_at):
        assert reference == {"id": "credential/example-history", "generation": "1"}
        assert agent_id == ab.AGENT and transcript_binding.startswith("@protected:")
        assert expires_at > 0
        return "synthetic-history-credential"


def inputs(b, root, *, execution_seconds=300, profile_override=None, admit=True):
    projection = projection_value()
    profile = execution_profile() if profile_override is None else deepcopy(profile_override)
    profile["runner"]["execution_deadline_seconds"] = execution_seconds
    profile["launch"]["substrate"]["kind"] = "kubernetes"
    profile["launch"]["substrate"]["launch_config_sha256"] = hashlib.sha256(
        canonical(projection)
    ).hexdigest()
    manifest = manifest_for(profile, broker_identity=b.manifest().as_dict()["broker_identity"])
    m = manifest.as_dict()
    # Real provisioner writes actual authority records to the isolated broker.
    existing = ab.seed(b)
    q = next(v for k, v in existing.items() if ":qualification:" in k)
    readiness = next(v for k, v in existing.items() if ":readiness:" in k)
    selection = next(v for k, v in existing.items() if ":selection:" in k)
    measurement = {
        "schema_version": 1,
        "kind": "protected-artifact-qualification",
        "manifest_digest": manifest.digest,
        "execution_config_digest": m["execution_config_digest"],
        "substrate": m["substrate"],
        "guard_identity": m["guard_identity"],
        "platform_mappings": [
            {
                **projection["platform"],
                "oci_digest": projection["image"]["digest"],
                "pinned_image_reference": projection["image"]["reference"],
                "observed_image_id": projection["image"]["reference"],
                "image_config_id": None,
            }
        ],
        "launch_projection": {
            "schema_version": 1,
            "format": projection["format"],
            "sha256": hashlib.sha256(canonical(projection)).hexdigest(),
        },
        "delivery_slots": projection["delivery_slots"],
        "enforcement_evidence_ref": "sha256:" + "9" * 64,
    }
    measurement_hash = hashlib.sha256(canonical(measurement)).hexdigest()
    for value in (q, readiness):
        for key in (
            "runtime_id",
            "runtime_generation",
            "qualification_id",
            "broker_identity",
            "guard_identity",
        ):
            value[key] = m[key]
        value["manifest_digest"] = manifest.digest
    q["execution_config_digest"] = m["execution_config_digest"]
    q["measurement_record_id"] = "sha256:" + measurement_hash
    selection["manifest_digest"] = manifest.digest
    for key, value in (
        ("protected:control:manifest:" + manifest.digest, m),
        ("protected:control:qualification:" + m["qualification_id"] + ":1", q),
        ("protected:control:readiness:" + m["runtime_id"] + ":1", readiness),
        ("protected:control:selection:" + m["runtime_id"], selection),
    ):
        b.command("SET", key, canonical(value))
    request = None
    if admit:
        records = ab.module("admission_records")
        original = (
            json.dumps(json.loads(ab.queued_payload()), indent=2, ensure_ascii=True).encode()
            + b"\n"
        )
        req = ab.request(records, payload=original)
        atomic = ab.module("atomic_admission").AtomicAdmission(
            b.admin, trusted_manifest=manifest, trusted_max_readiness_ms=60000, backlog_limit=64
        )
        assert atomic.admit(req).as_dict()["status"] == "accepted"
        entry = b.command("XRANGE", "curie:runs", "-", "+")[0]
        fields = {k.decode(): v.decode() for k, v in entry[1].items()}
        envelope = json.loads(fields["protected_envelope"])
        request = {
            "schema_version": 1,
            "request_id": str(uuid4()),
            **{
                k: envelope[k]
                for k in (
                    "runtime_id",
                    "runtime_generation",
                    "manifest_digest",
                    "event_id",
                    "payload_sha256",
                    "execution_session_key",
                )
            },
            "committed_stream_id": entry[0].decode(),
            "source_fields": fields,
        }
    measurements = root / "measurements"
    projections = root / "projections"
    measurements.mkdir(mode=0o700)
    projections.mkdir(mode=0o700)
    (measurements / (measurement_hash + ".json")).write_bytes(canonical(measurement))
    (projections / (hashlib.sha256(canonical(projection)).hexdigest() + ".json")).write_bytes(
        canonical(projection)
    )
    return manifest, profile, request, parse_qualification(canonical(q)), measurements, projections


def test_actual_server_pending_retry_trim_refusal_and_restart_cleanup(owned_worker, tmp_path):
    """One actual job, mTLS identity/capability, real retained proof after stream trim."""
    owned = owned_worker
    owned.rules = verification_acl_rules("worker")
    owned.install()
    root = certificates(tmp_path / "tls")
    registry = tmp_path / "registry"
    registry.mkdir(mode=0o700)
    manifest, profile, request, _, measurements, projections = inputs(owned.broker, tmp_path)
    calls = []
    create_entered = asyncio.Event()
    create_release = asyncio.Event()

    async def external(krequest):
        assert krequest.headers.get("Authorization") == "Bearer synthetic-kubernetes"
        calls.append((krequest.method, krequest.path))
        if krequest.method == "POST":
            doc = await krequest.json()
            create_entered.set()
            await create_release.wait()
            doc["metadata"].update(uid=str(uuid4()), resourceVersion="1")
            return web.json_response(doc)
        return web.Response(status=404)

    async def run():
        client = await AuthenticatedWorkerClient.connect(
            manifest, owned.credential(), owned.broker.ca_pem
        )
        measurement_loader, projection_loader = (
            ArtifactMeasurementDirectory(str(measurements)),
            ProjectionDirectory(str(projections)),
        )
        try:
            runtime = ProvisionedRuntime(
                manifest,
                client,
                parse_execution_config(canonical(profile), manifest=manifest),
                measurement_loader,
                projection_loader,
            )
            workers = tuple(
                ProvisionedWorker(
                    "worker/example",
                    public_key_identity(
                        x509.load_pem_x509_certificate(
                            (root / "worker.pem").read_bytes()
                        ).public_bytes(serialization.Encoding.DER)
                    ),
                    manifest.as_dict()["runtime_id"],
                    manifest.as_dict()["substrate"]["authority_domain_id"],
                    frozenset(
                        {
                            "create",
                            "observe",
                            "start",
                            "admit",
                            "deny",
                            "status",
                            "capacity-status",
                            "interrupt",
                            "timeout",
                            "retire",
                        }
                    ),
                )
                for _ in range(1)
            )
            certpin = public_key_identity(
                x509.load_pem_x509_certificate((root / "server.pem").read_bytes()).public_bytes(
                    serialization.Encoding.DER
                )
            )
            async with endpoint(root, external) as origin:
                kube = KubernetesLaunchAPI(
                    KubernetesTLS(
                        origin,
                        str(root / "ca.pem"),
                        str(root / "worker.pem"),
                        str(root / "worker.key"),
                        "synthetic-kubernetes",
                    ),
                    authority_domain_id=workers[0].authority_domain_id,
                )
                config = ServerConfig(
                    str(registry),
                    "127.0.0.1",
                    8081,
                    str(root / "ca.pem"),
                    str(root / "server.pem"),
                    str(root / "server.key"),
                    certpin,
                    "http",
                    "svc.cluster.local",
                    None,
                )
                server = ProtectedLaunchServer(
                    config,
                    runtimes=(runtime,),
                    workers=workers,
                    credentials=ScopedFixtureCredentials(),
                    kubernetes=kube,
                )
                app_runner = web.AppRunner(
                    server.application, access_log=None, handler_cancellation=True
                )
                await app_runner.setup()
                site = web.TCPSite(app_runner, "127.0.0.1", 0, ssl_context=server._tls)
                await site.start()
                url = "https://127.0.0.1:" + str(site._server.sockets[0].getsockname()[1])
                try:
                    async with httpx.AsyncClient(verify=context(root), trust_env=False) as http:
                        first = await http.post(url + "/v1/launches", content=canonical(request))
                        assert first.status_code == 202, first.json()
                        pending = first.json()
                        await asyncio.wait_for(create_entered.wait(), 3)
                        owned.broker.command("XTRIM", "curie:runs", "MAXLEN", "=", 0)
                        again = await http.post(url + "/v1/launches", content=canonical(request))
                        assert again.status_code == 202 and again.json() == pending
                        owned.broker.command(
                            "DEL", "protected:admission:binding:" + request["event_id"]
                        )
                        assert (
                            await http.post(url + "/v1/launches", content=canonical(request))
                        ).status_code == 503
                        whitespace_retry = await http.post(
                            url + "/v1/launches", content=b" " + canonical(request)
                        )
                        assert whitespace_retry.status_code == 409
                        assert len(calls) == 1
                        launch = url + "/v1/launches/" + pending["launch_id"]
                        cap = {"X-Curie-Launch-Capability": pending["control_capability"]}
                        assert (await http.get(launch)).status_code == 403
                        assert (await http.get(launch, headers=cap)).json()["state"] == "preparing"
                        assert (
                            await http.get(
                                launch,
                                headers=[
                                    ("X-Curie-Launch-Capability", pending["control_capability"]),
                                    ("X-Curie-Launch-Capability", pending["control_capability"]),
                                ],
                            )
                        ).status_code == 403
                        extra = dict(request, arbitrary_proxy="https://example.com")
                        assert (
                            await http.post(url + "/v1/launches", content=canonical(extra))
                        ).status_code == 400
                        duplicate = canonical(request).replace(
                            b'"schema_version":1', b'"schema_version":1,"schema_version":1'
                        )
                        assert (
                            await http.post(url + "/v1/launches", content=duplicate)
                        ).status_code == 400
                        changed = dict(request, event_id="event/substituted")
                        assert (
                            await http.post(url + "/v1/launches", content=canonical(changed))
                        ).status_code >= 400
                        start = {
                            "schema_version": 1,
                            "operation_id": str(uuid4()),
                            "expected_receipt_revision": pending["receipt_revision"],
                            "capacity_admission": True,
                            "event": {
                                "type": "message",
                                "text": "anonymous prompt",
                                "user": "author",
                                "ts": "thread",
                                "tool_access": "read-only",
                            },
                        }
                        assert (
                            await http.post(
                                launch + "/start", content=canonical(start), headers=cap
                            )
                        ).status_code >= 400
                        # Stop remains available after proof loss.
                        # Uncertain creation stays retiring.
                        stopped = await http.post(
                            launch + "/retire",
                            headers=cap,
                            content=canonical({"schema_version": 1, "operation_id": str(uuid4())}),
                        )
                        assert stopped.status_code == 202 and stopped.json()["state"] == "retiring"
                    async with httpx.AsyncClient(
                        verify=context(root, "unknown"), trust_env=False
                    ) as unknown:
                        assert (await unknown.get(launch, headers=cap)).status_code == 403
                finally:
                    create_release.set()
                    await app_runner.cleanup()
                # A real replacement issuer opens the same SQLite root only
                # after the previous app/network owner has fully stopped.
                replacement_kube = KubernetesLaunchAPI(
                    KubernetesTLS(
                        origin,
                        str(root / "ca.pem"),
                        str(root / "worker.pem"),
                        str(root / "worker.key"),
                        "synthetic-kubernetes",
                    ),
                    authority_domain_id=workers[0].authority_domain_id,
                )
                replacement = ProtectedLaunchServer(
                    config,
                    runtimes=(runtime,),
                    workers=workers,
                    credentials=ScopedFixtureCredentials(),
                    kubernetes=replacement_kube,
                )
                replacement_owner = web.AppRunner(replacement.application, access_log=None)
                await replacement_owner.setup()
                replacement_site = web.TCPSite(
                    replacement_owner, "127.0.0.1", 0, ssl_context=replacement._tls
                )
                await replacement_site.start()
                replacement_url = "https://127.0.0.1:" + str(
                    replacement_site._server.sockets[0].getsockname()[1]
                )
                try:
                    async with httpx.AsyncClient(verify=context(root), trust_env=False) as http:
                        old_launch = replacement_url + "/v1/launches/" + pending["launch_id"]
                        # Fresh broker proof was removed above. Exact old request
                        # still returns retired before any positive proof read.
                        result = await http.post(
                            replacement_url + "/v1/launches", content=canonical(request)
                        )
                        assert result.status_code == 409 and result.json()["code"] == "retired"
                        observed = (await http.get(old_launch)).json()
                        assert observed["state"] == "retiring" and observed["receipt"] is None
                        assert (
                            await http.post(
                                old_launch + "/start", headers=cap, content=canonical(start)
                            )
                        ).status_code == 409
                        stopped = await http.post(
                            old_launch + "/retire",
                            content=canonical({"schema_version": 1, "operation_id": str(uuid4())}),
                        )
                        assert stopped.status_code == 202
                finally:
                    await replacement_owner.cleanup()
                # SQLite and WAL never contain original prompt/envelope or credential bytes.
                for file in registry.iterdir():
                    if file.is_file():
                        data = file.read_bytes()
                        for excluded in (
                            request["source_fields"]["payload"],
                            request["source_fields"]["protected_envelope"],
                            pending["control_capability"],
                            "synthetic-history-credential",
                            "synthetic-model-credential",
                            "anonymous prompt",
                        ):
                            assert excluded.encode() not in data
        finally:
            measurement_loader.close()
            projection_loader.close()
            await client.close()

    asyncio.run(run())


class ExternalWorkload:
    """Only the authenticated external Kubernetes/runner control planes are simulated."""

    def __init__(self):
        self.objects = {}
        self.pod = None
        self.events = []
        self.epoch = None
        self.decision = "unknown"
        self.finish = asyncio.Event()
        self.server = None
        self.launch_id = None
        self.node_uid = "99999999-9999-4999-8999-999999999999"
        self.fault_mode = None
        self.start_entered = asyncio.Event()
        self.start_release = asyncio.Event()
        self.start_release.set()
        self.admit_entered = asyncio.Event()
        self.admit_release = asyncio.Event()
        self.admit_release.set()

    async def kubernetes(self, request):
        from copy import deepcopy

        assert request.headers.get("Authorization") == "Bearer synthetic-kubernetes"
        self.events.append((request.method, request.path))
        parts = request.path.split("/")
        if request.path.startswith("/api/v1/nodes/"):
            return web.json_response(
                {
                    "metadata": {"name": "acme-node", "uid": self.node_uid},
                    "status": {
                        "nodeInfo": {
                            "operatingSystem": "linux",
                            "architecture": "amd64",
                            "containerRuntimeVersion": "runtime/example",
                        }
                    },
                }
            )
        if parts[-1] == "endpointslices":
            pod = self.pod
            service = next(v for (k, _), v in self.objects.items() if k == "services")
            return web.json_response(
                {
                    "items": [
                        {
                            "metadata": {
                                "namespace": pod["metadata"]["namespace"],
                                "ownerReferences": [
                                    {
                                        "kind": "Service",
                                        "name": service["metadata"]["name"],
                                        "uid": service["metadata"]["uid"],
                                        "controller": True,
                                    }
                                ],
                            },
                            "ports": [{"name": "http", "port": 8080, "protocol": "TCP"}],
                            "endpoints": [
                                {
                                    "conditions": {"ready": True},
                                    "addresses": ["127.0.0.1"],
                                    "targetRef": {
                                        "kind": "Pod",
                                        "namespace": pod["metadata"]["namespace"],
                                        "name": pod["metadata"]["name"],
                                        "uid": pod["metadata"]["uid"],
                                    },
                                }
                            ],
                        }
                    ]
                }
            )
        if request.method == "POST":
            kind = parts[-1]
            doc = await request.json()
            doc["metadata"].update(uid=str(uuid4()), resourceVersion="1")
            self.objects[(kind, doc["metadata"]["name"])] = doc
            if kind == "pods":
                self.pod = doc
                doc["spec"]["nodeName"] = "acme-node"
                doc["status"] = {
                    "phase": "Running",
                    "podIP": "127.0.0.1",
                    "containerStatuses": [
                        {
                            "name": "runner",
                            "ready": True,
                            "restartCount": 0,
                            "containerID": "containerd://" + "a" * 64,
                            "imageID": doc["spec"]["containers"][0]["image"],
                            "state": {"running": {"startedAt": "2026-10-12T00:00:00Z"}},
                        }
                    ],
                }
                if self.fault_mode in {"delayed-ready", "creation-deadline"}:
                    doc["status"]["phase"] = "Pending"
                    doc["status"]["containerStatuses"][0]["ready"] = False
                if self.fault_mode == "creation-no-container":
                    doc["status"]["phase"] = "Pending"
                    doc["status"]["containerStatuses"] = []
            return web.json_response(doc, status=201)
        kind, name = parts[-2:]
        key = (kind, name)
        doc = self.objects.get(key)
        if doc is None:
            return web.Response(status=404)
        if request.method == "GET":
            return web.json_response(doc)
        if request.method == "DELETE":
            body = await request.json()
            assert body["preconditions"]["uid"] == doc["metadata"]["uid"]
            if kind == "pods" and self.fault_mode == "not-found":
                del self.objects[key]
                self.finish.set()
                return web.json_response(doc)
            if kind == "services" and self.fault_mode == "incomplete-service":
                return web.json_response(doc)
            if kind == "pods":
                if self.fault_mode == "creation-no-container":
                    doc["metadata"]["deletionTimestamp"] = "2026-10-12T00:00:01Z"
                    return web.json_response(doc)
                if self.fault_mode == "no-termination":
                    doc["metadata"]["deletionTimestamp"] = "2026-10-12T00:00:01Z"
                    return web.json_response(doc)
                doc["metadata"]["deletionTimestamp"] = "2026-10-12T00:00:01Z"
                c = doc["status"]["containerStatuses"][0]
                c["state"] = {
                    "terminated": {
                        "containerID": c["containerID"],
                        "exitCode": 0,
                        "startedAt": "2026-10-12T00:00:00Z",
                        "finishedAt": "2026-10-12T00:00:01Z",
                    }
                }
                c["ready"] = False
                self.finish.set()
            else:
                del self.objects[key]
            return web.json_response(doc)
        if request.method == "PATCH":
            # The real issuer registry must contain exact durable termination
            # before the external API observes a finalizer removal request.
            assert self.server is not None and self.launch_id is not None
            row = self.server._registry.get("worker/example", self.launch_id)
            assert row.state == "retiring" and row.termination is not None
            assert (
                row.termination.container_id == doc["status"]["containerStatuses"][0]["containerID"]
            )
            assert row.termination.pod_uid == doc["metadata"]["uid"]
            patches = await request.json()
            assert patches == [
                {"op": "test", "path": "/metadata/uid", "value": doc["metadata"]["uid"]},
                {
                    "op": "test",
                    "path": "/metadata/resourceVersion",
                    "value": doc["metadata"]["resourceVersion"],
                },
                {
                    "op": "test",
                    "path": "/metadata/finalizers",
                    "value": ["curietech.ai/protected-launch"],
                },
                {"op": "remove", "path": "/metadata/finalizers/0"},
            ]
            removed = deepcopy(doc)
            del self.objects[key]
            return web.json_response(removed)
        return web.Response(status=405)

    async def runner(self, request):
        import base64

        from aci_protocol import Final, to_ndjson_line
        from aci_protocol.events import SessionStatus

        secret = next(v for (k, _), v in self.objects.items() if k == "secrets")
        token = base64.b64decode(secret["data"]["runner-token"]).decode()
        if request.headers.get("Authorization") != "Bearer " + token:
            return web.Response(status=403)
        env = {v["name"]: v.get("value") for v in self.pod["spec"]["containers"][0]["env"]}
        if request.path == "/v1/status":
            status_hook = getattr(self, "status_hook", None)
            if status_hook is not None:
                self.status_hook = None
                status_hook()
            response = {
                "status": "idle-awaiting-input",
                "ready": True,
                "turn_active": self.epoch is not None,
                "history_durable": True,
                "tool_access": ["read-only"],
                "turn_epoch": self.epoch,
                "capacity_admission": True,
                "session_id": env["CURIE_SESSION_ID"],
                "sandbox_id": self.pod["metadata"]["name"],
                "managed_workspace": False,
                "cwd": None,
            }
            if request.headers.get("X-Curie-Turn-Epoch"):
                response["capacity_admission_result"] = self.decision
            return web.json_response(response)
        self.events.append(("RUNNER_POST", request.path))
        if request.path == "/v1/event":
            assert self.epoch is None
            self.start_entered.set()
            await self.start_release.wait()
            if self.fault_mode == "uncertain-start":
                return web.json_response({"ok": True})
            self.epoch = "a" * 43
            self.decision = "pending"
            response = web.StreamResponse(
                headers={"Content-Type": "application/x-ndjson", "X-Curie-Turn-Epoch": self.epoch}
            )
            await response.prepare(request)
            await self.finish.wait()
            await response.write(
                to_ndjson_line(Final(text="synthetic reply", status=SessionStatus.DONE)).encode()
            )
            await response.write_eof()
            return response
        assert request.headers.get("X-Curie-Turn-Epoch") == self.epoch
        if request.path == "/v1/turn-admit":
            assert self.decision == "pending"
            self.admit_entered.set()
            await self.admit_release.wait()
            self.decision = "granted" if (await request.json())["allow"] else "denied"
            if self.fault_mode == "uncertain-admit":
                return web.json_response({"unexpected": True})
        else:
            assert request.path in {"/v1/timeout", "/v1/interrupt"}
            self.finish.set()
        return web.json_response({"ok": True})
