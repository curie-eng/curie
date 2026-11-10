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
import pytest
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


@pytest.mark.parametrize(
    "mode",
    [
        "complete",
        "uncertain-start",
        "uncertain-admit",
        "no-termination",
        "missing-finalizer",
        "not-found",
        "incomplete-service",
        "retire-wins",
        "start-wins",
        "admit-wins",
        "deny-after-proof-loss",
        "delayed-ready",
        "creation-deadline",
        "creation-no-container",
        "stream-duration",
    ],
)
def test_actual_ready_assignment_start_admit_and_termination_journal(owned_worker, tmp_path, mode):
    """Actual issuer produces receipt and forwards actual external runner epoch/stream."""
    from copy import deepcopy

    from aci_protocol import Event

    owned = owned_worker
    owned.rules = verification_acl_rules("worker")
    owned.install()
    root = certificates(tmp_path / "tls")
    registry = tmp_path / "registry"
    registry.mkdir(mode=0o700)
    manifest, profile, request, _, measurements, projections = inputs(
        owned.broker,
        tmp_path,
        execution_seconds=2 if mode in {"creation-deadline", "creation-no-container"} else 300,
    )

    async def run():
        external = ExternalWorkload()
        external.fault_mode = mode
        broker = await AuthenticatedWorkerClient.connect(
            manifest, owned.credential(), owned.broker.ca_pem
        )
        ml, pl = (
            ArtifactMeasurementDirectory(str(measurements)),
            ProjectionDirectory(str(projections)),
        )
        runner_app = web.Application()
        runner_app.router.add_route("*", "/{tail:.*}", external.runner)
        runner_owner = web.AppRunner(runner_app, access_log=None)
        await runner_owner.setup()
        runner_site = web.TCPSite(runner_owner, "127.0.0.1", 8080)
        try:
            await runner_site.start()  # A collision is infrastructure failure, never skipped.
            async with endpoint(root, external.kubernetes) as origin:
                runtime = ProvisionedRuntime(
                    manifest,
                    broker,
                    parse_execution_config(canonical(profile), manifest=manifest),
                    ml,
                    pl,
                )
                worker_pin = public_key_identity(
                    x509.load_pem_x509_certificate((root / "worker.pem").read_bytes()).public_bytes(
                        serialization.Encoding.DER
                    )
                )
                server_pin = public_key_identity(
                    x509.load_pem_x509_certificate((root / "server.pem").read_bytes()).public_bytes(
                        serialization.Encoding.DER
                    )
                )
                worker = ProvisionedWorker(
                    "worker/example",
                    worker_pin,
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
                server = ProtectedLaunchServer(
                    ServerConfig(
                        str(registry),
                        "127.0.0.1",
                        8081,
                        str(root / "ca.pem"),
                        str(root / "server.pem"),
                        str(root / "server.key"),
                        server_pin,
                        "http",
                        "svc.cluster.local",
                        None,
                    ),
                    runtimes=(runtime,),
                    workers=(worker,),
                    credentials=ScopedFixtureCredentials(),
                    kubernetes=KubernetesLaunchAPI(
                        KubernetesTLS(
                            origin,
                            str(root / "ca.pem"),
                            str(root / "worker.pem"),
                            str(root / "worker.key"),
                            "synthetic-kubernetes",
                        ),
                        authority_domain_id=worker.authority_domain_id,
                    ),
                )
                external.server = server
                owner = web.AppRunner(
                    server.application, access_log=None, handler_cancellation=True
                )
                await owner.setup()
                site = web.TCPSite(owner, "127.0.0.1", 0, ssl_context=server._tls)
                await site.start()
                url = "https://127.0.0.1:" + str(site._server.sockets[0].getsockname()[1])
                try:
                    async with httpx.AsyncClient(verify=context(root), trust_env=False) as http:
                        first = await http.post(url + "/v1/launches", content=canonical(request))
                        assert first.status_code == 202, first.json()
                        pending = first.json()
                        external.launch_id = pending["launch_id"]
                        launch = url + "/v1/launches/" + pending["launch_id"]
                        headers = {"X-Curie-Launch-Capability": pending["control_capability"]}
                        if mode == "delayed-ready":
                            await asyncio.sleep(5.1)
                            assert (await http.get(launch, headers=headers)).json()[
                                "state"
                            ] == "preparing"
                            external.pod["status"]["phase"] = "Running"
                            external.pod["status"]["containerStatuses"][0]["ready"] = True
                        if mode == "creation-no-container":
                            await asyncio.wait_for(
                                asyncio.shield(server._live[pending["launch_id"]].job), 5
                            )
                            snapshot = server._registry.get(
                                worker.principal_id, pending["launch_id"]
                            )
                            assert snapshot.state == "retiring" and snapshot.termination is None
                            assert any(
                                m == "DELETE" and path.endswith(snapshot.objects.pod_name)
                                for m, path in external.events
                            )
                            assert not any(m == "PATCH" for m, path in external.events)
                            assert not any(m == "RUNNER_POST" for m, path in external.events)
                            return
                        if mode == "creation-deadline":
                            for _ in range(10):
                                expired = (await http.get(launch, headers=headers)).json()
                                if expired["state"] == "retired":
                                    break
                                await asyncio.sleep(0.5)
                            assert expired["state"] == "retired"
                            snapshot = server._registry.get(
                                worker.principal_id, pending["launch_id"]
                            )
                            assert snapshot.creation_budget_ms == 2000
                            assert not any(m == "RUNNER_POST" for m, p in external.events)
                            assert not external.objects
                            return
                        for _ in range(5):
                            observation = (await http.get(launch, headers=headers)).json()
                            if observation["state"] == "idle":
                                break
                            await asyncio.sleep(1)
                        assert observation["state"] == "idle", observation
                        assert observation["receipt"]["assignment"]["immutable"] is True
                        assert (
                            observation["receipt"]["workload"]["container_id"]
                            == "containerd://" + "a" * 64
                        )
                        again = await http.post(url + "/v1/launches", content=canonical(request))
                        assert again.status_code == 200
                        assert (
                            sum(
                                method == "POST" and path.endswith("/pods")
                                for method, path in external.events
                            )
                            == 1
                        )
                        payload = json.loads(request["source_fields"]["payload"])
                        event = Event(
                            type="job",
                            text=payload["text"],
                            user=payload["author"],
                            ts=payload["conversation_id"],
                            tool_access="read-only",
                        ).model_dump(mode="json")
                        start = {
                            "schema_version": 1,
                            "operation_id": str(uuid4()),
                            "expected_receipt_revision": observation["receipt_revision"],
                            "capacity_admission": True,
                            "event": event,
                        }
                        # Every changed assignment/platform/endpoint fact is refused
                        # by the actual issuer before any runner POST, then restored.
                        for mutation in (
                            "secret-bytes",
                            "secret-uid",
                            "secret-version",
                            "restart",
                            "image",
                            "node",
                            "env",
                        ):
                            before = deepcopy(external.objects)
                            secret = next(
                                v for (k, _), v in external.objects.items() if k == "secrets"
                            )
                            if mutation == "secret-bytes":
                                secret["data"]["runner-token"] = "c3Vic3RpdHV0ZWQ="
                            elif mutation == "secret-uid":
                                secret["metadata"]["uid"] = str(uuid4())
                            elif mutation == "secret-version":
                                secret["metadata"]["resourceVersion"] = "2"
                            elif mutation == "restart":
                                external.pod["status"]["containerStatuses"][0]["restartCount"] = 1
                            elif mutation == "image":
                                external.pod["status"]["containerStatuses"][0]["imageID"] = (
                                    "substituted"
                                )
                            elif mutation == "node":
                                external.node_uid = str(uuid4())
                            else:
                                external.pod["spec"]["containers"][0]["env"].append(
                                    {"name": "EXTRA", "value": "unsupported"}
                                )
                            refused = await http.post(
                                launch + "/start", headers=headers, content=canonical(start)
                            )
                            assert refused.status_code == 503, mutation
                            assert not any(method == "RUNNER_POST" for method, _ in external.events)
                            external.objects = before
                            external.pod = next(v for (k, _), v in before.items() if k == "pods")
                            external.node_uid = "99999999-9999-4999-8999-999999999999"
                        retire_body = canonical({"schema_version": 1, "operation_id": str(uuid4())})
                        if mode == "retire-wins":
                            stopped = await http.post(
                                launch + "/retire", headers=headers, content=retire_body
                            )
                            assert stopped.status_code == 200
                            assert (
                                await http.post(
                                    launch + "/start", headers=headers, content=canonical(start)
                                )
                            ).status_code >= 400
                            assert not any(
                                m == "RUNNER_POST" and p == "/v1/event" for m, p in external.events
                            )
                            return
                        if mode == "uncertain-start":
                            result = await http.post(
                                launch + "/start", headers=headers, content=canonical(start)
                            )
                            assert result.status_code == 503
                            for _ in range(5):
                                snapshot = server._registry.get(
                                    worker.principal_id, pending["launch_id"]
                                )
                                if snapshot.state == "retired":
                                    break
                                await asyncio.sleep(0.1)
                            assert snapshot.state == "retired"
                            assert (
                                server._registry.get_operation(
                                    worker.principal_id, pending["launch_id"], start["operation_id"]
                                ).status
                                == "uncertain"
                            )
                            assert (
                                sum(
                                    m == "RUNNER_POST" and p == "/v1/event"
                                    for m, p in external.events
                                )
                                == 1
                            )
                            return
                        if mode == "start-wins":
                            external.start_release.clear()
                            start_task = asyncio.create_task(
                                http.send(
                                    http.build_request(
                                        "POST",
                                        launch + "/start",
                                        headers=headers,
                                        content=canonical(start),
                                    ),
                                    stream=True,
                                )
                            )
                            await asyncio.wait_for(external.start_entered.wait(), 3)
                            retire_task = asyncio.create_task(
                                http.post(launch + "/retire", headers=headers, content=retire_body)
                            )
                            await asyncio.sleep(0.1)
                            assert not any(m == "DELETE" for m, p in external.events)
                            external.start_release.set()
                            streamed = await start_task
                            assert streamed.status_code == 200
                            result = await retire_task
                            assert result.status_code == 200 and result.json()["state"] == "retired"
                            assert ("RUNNER_POST", "/v1/timeout") in external.events
                            await streamed.aclose()
                            return
                        streamed = await http.send(
                            http.build_request(
                                "POST", launch + "/start", headers=headers, content=canonical(start)
                            ),
                            stream=True,
                        )
                        assert streamed.status_code == 200
                        epoch = streamed.headers["X-Curie-Turn-Epoch"]
                        assert epoch == external.epoch
                        if mode == "stream-duration":
                            await asyncio.sleep(5.1)
                            assert (
                                server._registry.get(
                                    worker.principal_id, pending["launch_id"]
                                ).state
                                == "waiting"
                            )
                            assert not any(m == "DELETE" for m, p in external.events)
                        waiting = (await http.get(launch, headers=headers)).json()
                        admit = {
                            "schema_version": 1,
                            "operation_id": str(uuid4()),
                            "expected_receipt_revision": waiting["receipt_revision"],
                            "epoch": epoch,
                            "allow": True,
                        }
                        wrong = dict(admit, epoch="b" * 43)
                        assert (
                            await http.post(
                                launch + "/admit", headers=headers, content=canonical(wrong)
                            )
                        ).status_code >= 400
                        if mode == "deny-after-proof-loss":
                            owned.broker.command(
                                "DEL", "protected:admission:binding:" + request["event_id"]
                            )
                            denied = await http.post(
                                launch + "/admit",
                                headers=headers,
                                content=canonical(dict(admit, allow=False)),
                            )
                            assert denied.status_code == 200
                            assert external.decision == "denied"
                            stopped = await http.post(
                                launch + "/retire", headers=headers, content=retire_body
                            )
                            assert stopped.status_code == 200
                            await streamed.aclose()
                            return
                        if mode == "admit-wins":
                            external.admit_release.clear()
                            admit_task = asyncio.create_task(
                                http.post(
                                    launch + "/admit", headers=headers, content=canonical(admit)
                                )
                            )
                            await asyncio.wait_for(external.admit_entered.wait(), 3)
                            retire_task = asyncio.create_task(
                                http.post(launch + "/retire", headers=headers, content=retire_body)
                            )
                            await asyncio.sleep(0.1)
                            assert not any(m == "DELETE" for m, p in external.events)
                            external.admit_release.set()
                            assert (await admit_task).status_code == 200
                            assert (await retire_task).status_code == 200
                            await streamed.aclose()
                            return
                        admitted = await http.post(
                            launch + "/admit", headers=headers, content=canonical(admit)
                        )
                        if mode == "uncertain-admit":
                            assert admitted.status_code == 503
                            for _ in range(5):
                                snapshot = server._registry.get(
                                    worker.principal_id, pending["launch_id"]
                                )
                                if snapshot.state == "retired":
                                    break
                                await asyncio.sleep(0.1)
                            assert snapshot.state == "retired"
                            assert (
                                server._registry.get_operation(
                                    worker.principal_id, pending["launch_id"], admit["operation_id"]
                                ).status
                                == "uncertain"
                            )
                            assert (
                                sum(
                                    m == "RUNNER_POST" and p == "/v1/turn-admit"
                                    for m, p in external.events
                                )
                                == 1
                            )
                            await streamed.aclose()
                            return
                        assert admitted.status_code == 200
                        if mode == "missing-finalizer":
                            external.pod["metadata"]["finalizers"] = []
                        # Proof loss stops later positives while preserving owned stop.
                        owned.broker.command(
                            "DEL", "protected:admission:binding:" + request["event_id"]
                        )
                        assert (
                            await http.post(
                                launch + "/start", headers=headers, content=canonical(start)
                            )
                        ).status_code >= 400
                        assert (
                            await http.post(launch + "/steer", headers=headers, content=b"{}")
                        ).status_code == 404
                        stopped = await http.post(
                            launch + "/retire",
                            headers=headers,
                            content=canonical({"schema_version": 1, "operation_id": str(uuid4())}),
                        )
                        if mode in {
                            "no-termination",
                            "missing-finalizer",
                            "not-found",
                            "incomplete-service",
                        }:
                            assert (
                                stopped.status_code == 202 and stopped.json()["state"] == "retiring"
                            )
                            snapshot = server._registry.get(
                                worker.principal_id, pending["launch_id"]
                            )
                            assert snapshot.retirement_outcome is None
                            if mode != "incomplete-service":
                                assert snapshot.termination is None
                                assert not any(m == "PATCH" for m, p in external.events)
                            await streamed.aclose()
                            return
                        assert (
                            stopped.status_code == 200 and stopped.json()["state"] == "retired"
                        ), stopped.json()
                        assert not external.objects
                        assert (
                            sum(m == "RUNNER_POST" and p == "/v1/event" for m, p in external.events)
                            == 1
                        )
                        await streamed.aclose()
                finally:
                    external.finish.set()
                    await owner.cleanup()
        finally:
            await runner_owner.cleanup()
            ml.close()
            pl.close()
            await broker.close()

    asyncio.run(run())
