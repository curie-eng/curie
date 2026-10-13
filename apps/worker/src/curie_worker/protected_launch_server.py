"""Single-process provisioner-owned HTTPS mediator, not an application API.

@spec PROTECTED-HOOK-LANE-2 PROTECTED-HOOK-LANE-7 PROTECTED-HOOK-LANE-8.
Every positive operation independently verifies retained broker authority. No
source prompt or credential is persisted. Source plumbing supplies no preventive
or actual runtime qualification; absent provisioner proof refuses execution.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import ipaddress
import re
import secrets
import ssl
import time
from dataclasses import dataclass, field
from typing import Any, cast
from uuid import UUID, uuid4

import httpx
from aci_protocol import BootEnv, parse_ndjson_line
from aiohttp import web
from channel_protocol import parse_scoped_conversation_id
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from curie_protected_hooks.artifact_measurement import ArtifactMeasurementDirectory
from curie_protected_hooks.authority_records import Manifest, parse_qualification, parse_readiness
from curie_protected_hooks.worker_artifact_selection import SelectedArtifact, select_artifact
from curie_protected_hooks.worker_transport import AuthenticatedWorkerClient
from curie_protected_hooks.worker_verification import Dispatchable, verify_delivery

from .protected_binding import ProtectedCredentialProvider
from .protected_configuration import ProtectedExecutionConfig, parse_execution_config
from .protected_history import protected_history_key
from .protected_kubernetes_projection import (
    Projection,
    ProjectionDirectory,
    render_delivery_environment,
    render_pod,
    validate_projection_binding,
)
from .protected_launch_kubernetes import (
    FINALIZER,
    KubernetesLaunchAPI,
    LaunchControlUnavailable,
    decode_observation,
    object_identity,
    observe_direct_pod,
    present,
    require,
    secret_document,
    service_document,
    sha256_text,
    termination_observation,
)
from .protected_launch_records import (
    IssuerPodBinding,
    LaunchObservation,
    LaunchPendingSuccess,
    LaunchReceipt,
    LaunchRequest,
    LaunchSuccess,
    canonical_json,
    parse_admit_request,
    parse_epoch_request,
    parse_launch_request,
    parse_retire_request,
    parse_start_request,
    validate_launch_request_delivery,
    validate_start_request_delivery,
)
from .protected_launch_registry import (
    AssignmentMetadata,
    CleanupReceipt,
    LaunchOwnership,
    LaunchSnapshot,
    OperationKind,
    ProtectedLaunchRegistry,
    RegistryConflict,
    RegistryInvalid,
    RegistryRetired,
    RegistryUnauthorized,
)

CAPABILITY_HEADER = "X-Curie-Launch-Capability"
EPOCH_HEADER = "X-Curie-Turn-Epoch"
_CONTROL_SECONDS = 5
_CONTROL_DEADLINE = web.RequestKey("protected_launch_control_deadline", asyncio.Timeout)


@dataclass(frozen=True, slots=True, repr=False)
class ProvisionedWorker:
    principal_id: str
    public_key_sha256: str
    runtime_id: str
    authority_domain_id: str
    allowed_operations: frozenset[str]

    def __post_init__(self) -> None:
        require(re.fullmatch(r"[0-9a-f]{64}", self.public_key_sha256) is not None)
        require(str(UUID(self.runtime_id)) == self.runtime_id)
        require(str(UUID(self.authority_domain_id)) == self.authority_domain_id)
        require(
            re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}", self.principal_id) is not None
        )
        require(
            type(self.allowed_operations) is frozenset
            and self.allowed_operations
            <= {
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
        )


@dataclass(frozen=True, slots=True, repr=False)
class ProvisionedRuntime:
    manifest: Manifest
    broker: AuthenticatedWorkerClient
    execution: ProtectedExecutionConfig
    measurements: ArtifactMeasurementDirectory
    projections: ProjectionDirectory

    def __post_init__(self) -> None:
        require(
            type(self.manifest) is Manifest
            and type(self.broker) is AuthenticatedWorkerClient
            and type(self.execution) is ProtectedExecutionConfig
            and type(self.measurements) is ArtifactMeasurementDirectory
            and type(self.projections) is ProjectionDirectory
        )
        parse_execution_config(self.execution.canonical_bytes, manifest=self.manifest)
        require(self.manifest.as_dict()["substrate"]["kind"] == "kubernetes")


@dataclass(frozen=True, slots=True, repr=False)
class ServerConfig:
    registry_root: str
    host: str
    port: int
    ca_file: str
    certificate_file: str
    key_file: str = field(repr=False)
    public_key_sha256: str
    # Explicit internal runner ingress. HTTP is protected by the qualified
    # exclusive network path plus issuer bearer; HTTPS additionally verifies CA.
    runner_scheme: str
    runner_dns_suffix: str
    runner_ca_file: str | None

    def tls_context(self) -> ssl.SSLContext:
        require(self.runner_scheme in {"http", "https"})
        require(re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,252}", self.runner_dns_suffix) is not None)
        require(type(self.port) is int and 1 <= self.port <= 65535 and bool(self.host))
        require(self.runner_scheme != "https" or bool(self.runner_ca_file))
        with open(self.certificate_file, "rb") as f:
            cert = x509.load_pem_x509_certificate(f.read(65536))
        require(
            public_key_identity(cert.public_bytes(serialization.Encoding.DER))
            == self.public_key_sha256
        )
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH, cafile=self.ca_file)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.verify_mode = ssl.CERT_REQUIRED
        context.load_cert_chain(self.certificate_file, self.key_file)
        return context


def public_key_identity(der: bytes) -> str:
    cert = x509.load_der_x509_certificate(der)
    spki = cert.public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    return hashlib.sha256(spki).hexdigest()


class _Refusal(Exception):
    def __init__(self, code: str, status: int) -> None:
        self.code, self.status = code, status
        super().__init__("Protected launch refused")


@dataclass(slots=True, repr=False)
class _LiveLaunch:
    worker: ProvisionedWorker
    runtime: ProvisionedRuntime
    request: LaunchRequest
    selected: SelectedArtifact
    projection: Projection
    platform: dict[str, Any]
    delivery: Dispatchable
    slots: dict[str, str]
    control_capability: str = field(repr=False)
    source: dict[str, str] = field(repr=False)
    expires_at_ms: int
    deadline: float
    receipt: LaunchReceipt | None = None
    binding: IssuerPodBinding | None = None
    job: asyncio.Task[None] | None = None
    watchdog: asyncio.Task[None] | None = None


class ProtectedLaunchServer:
    """Owns registry, jobs, issuer-only assignments, authenticated HTTP and mutexes.

    Construct only under the provisioner owner with exact preloaded dependencies.
    The OS lock is retained through shutdown cleanup; deployment/failover fencing
    must be independently established outside this single-host implementation.
    """

    def __init__(
        self,
        config: ServerConfig,
        *,
        runtimes: tuple[ProvisionedRuntime, ...],
        workers: tuple[ProvisionedWorker, ...],
        credentials: ProtectedCredentialProvider,
        kubernetes: KubernetesLaunchAPI,
    ) -> None:
        require(type(config) is ServerConfig and type(kubernetes) is KubernetesLaunchAPI)
        require(bool(runtimes) and bool(workers))
        self._tls = config.tls_context()
        self._config, self._credentials, self._kube = config, credentials, kubernetes
        self._workers = {w.public_key_sha256: w for w in workers}
        self._runtimes = {r.manifest.as_dict()["runtime_id"]: r for r in runtimes}
        require(len(self._workers) == len(workers) and len(self._runtimes) == len(runtimes))
        for w in workers:
            require(type(w) is ProvisionedWorker and w.runtime_id in self._runtimes)
            domain = self._runtimes[w.runtime_id].manifest.as_dict()["substrate"][
                "authority_domain_id"
            ]
            require(w.authority_domain_id == domain == kubernetes.authority_domain_id)
        self._boot_id = str(uuid4())
        self._registry = ProtectedLaunchRegistry.open(config.registry_root, boot_id=self._boot_id)
        self._live: dict[str, _LiveLaunch] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._create_lock = asyncio.Lock()
        self._closing = False
        self._cleanup_task: asyncio.Task[None] | None = None
        verify: ssl.SSLContext | bool = True
        if config.runner_ca_file:
            verify = ssl.create_default_context(cafile=config.runner_ca_file)
        self._runner = httpx.AsyncClient(
            verify=verify, trust_env=False, follow_redirects=False, timeout=5
        )
        self._app = web.Application(client_max_size=2097152, middlewares=[self._middleware])
        self._app.router.add_route("*", "/{tail:.*}", self._http)
        self._app.on_shutdown.append(self._shutdown)
        self._app.on_cleanup.append(self._close)

    @property
    def application(self) -> web.Application:
        return self._app

    def _mutex(self, launch_id: str) -> asyncio.Lock:
        return self._locks.setdefault(launch_id, asyncio.Lock())

    def _row(self, worker: ProvisionedWorker, launch_id: str) -> LaunchSnapshot:
        return self._registry.get(worker.principal_id, launch_id)

    def _owner(self, request: web.Request) -> ProvisionedWorker:
        ssl_object = request.transport.get_extra_info("ssl_object") if request.transport else None
        if ssl_object is None:
            raise _Refusal("unauthorized", 403)
        peer = ssl_object.getpeercert(binary_form=True)
        worker = self._workers.get(public_key_identity(peer)) if peer else None
        if worker is None:
            raise _Refusal("unauthorized", 403)
        return worker

    @web.middleware
    async def _middleware(self, request: web.Request, handler: Any) -> web.StreamResponse:
        try:
            async with asyncio.timeout(_CONTROL_SECONDS) as deadline:
                request[_CONTROL_DEADLINE] = deadline
                return cast(web.StreamResponse, await handler(request))
        except _Refusal as e:
            return self._json({"schema_version": 1, "code": e.code}, e.status)
        except RegistryConflict:
            return self._json({"schema_version": 1, "code": "conflict"}, 409)
        except RegistryRetired:
            return self._json({"schema_version": 1, "code": "retired"}, 409)
        except RegistryUnauthorized:
            return self._json({"schema_version": 1, "code": "unauthorized"}, 403)
        except (ValueError, TypeError, KeyError, RegistryInvalid, web.HTTPRequestEntityTooLarge):
            return self._json({"schema_version": 1, "code": "invalid"}, 400)
        except Exception:  # noqa: BLE001 - fixed fail-closed diagnostic
            return self._json({"schema_version": 1, "code": "unavailable"}, 503)

    @staticmethod
    def _json(value: Any, status: int = 200) -> web.Response:
        return web.Response(
            body=canonical_json(value), content_type="application/json", status=status
        )

    async def _body(self, request: web.Request, cap: int) -> bytes:
        # Reject transfer encodings/duplicate framing at HTTP parser, then bound
        # decoded bytes independently of Content-Length before canonical codecs.
        if request.headers.get("Content-Encoding", "identity") != "identity":
            raise _Refusal("invalid", 400)
        if request.content_length is not None and request.content_length > cap:
            raise _Refusal("invalid", 400)
        body = bytearray()
        async for chunk in request.content.iter_chunked(16384):
            if len(body) + len(chunk) > cap:
                raise _Refusal("invalid", 400)
            body.extend(chunk)
        return bytes(body)

    def _authorize(
        self, request: web.Request, worker: ProvisionedWorker, launch_id: str, operation: str
    ) -> LaunchSnapshot:
        if operation not in worker.allowed_operations:
            raise _Refusal("unauthorized", 403)
        row = self._row(worker, launch_id)
        if row.boot_id != self._boot_id:
            if operation not in {
                "retire",
                "interrupt",
                "timeout",
                "status",
                "capacity-status",
                "observe",
                "deny",
            }:
                raise _Refusal("retired", 409)
            return row
        live = self._live.get(launch_id)
        if len(request.headers.getall(CAPABILITY_HEADER, [])) != 1:
            raise _Refusal("unauthorized", 403)
        cap = request.headers.get(CAPABILITY_HEADER, "")
        if live is None or not hmac.compare_digest(cap.encode(), live.control_capability.encode()):
            raise _Refusal("unauthorized", 403)
        return row

    async def _http(self, request: web.Request) -> web.StreamResponse:
        worker = self._owner(request)
        if self._closing:
            raise _Refusal("unavailable", 503)
        if request.method == "POST" and request.path == "/v1/launches":
            if "create" not in worker.allowed_operations or request.query_string:
                raise _Refusal("unauthorized", 403)
            body = await self._body(request, 2097152)
            return await self._create(
                worker, parse_launch_request(body), request_digest=hashlib.sha256(body).hexdigest()
            )
        match = re.fullmatch(
            r"/v1/launches/([0-9a-f-]{36})(?:/(start|admit|status|capacity-status|interrupt|timeout|retire))?",
            request.path,
        )
        if match is None:
            raise _Refusal("invalid", 404)
        launch_id, op = match.groups()
        require(str(UUID(launch_id)) == launch_id)
        operation = op or "observe"
        if request.method != (
            "GET" if operation in {"observe", "status", "capacity-status"} else "POST"
        ):
            raise _Refusal("invalid", 405)
        if operation != "capacity-status" and request.query_string:
            raise _Refusal("invalid", 400)
        if operation == "admit":
            body = await self._body(request, 16384)
            parsed = parse_admit_request(body)
            row = self._authorize(
                request, worker, launch_id, "admit" if parsed.as_dict()["allow"] else "deny"
            )
            return await self._control(
                worker, row, "admit", parsed, hashlib.sha256(body).hexdigest()
            )
        row = self._authorize(request, worker, launch_id, operation)
        if operation == "observe":
            live = self._live.get(launch_id)
            value = {
                "schema_version": 1,
                "launch_id": launch_id,
                "receipt_revision": row.revision,
                "state": row.state,
                "receipt": live.receipt.as_dict() if live and live.receipt else None,
            }
            return self._json(LaunchObservation(canonical_json(value)).as_dict())
        if operation == "start":
            body = await self._body(request, 262144)
            return await self._start(
                request, worker, row, parse_start_request(body), hashlib.sha256(body).hexdigest()
            )
        if operation in {"interrupt", "timeout"}:
            body = await self._body(request, 16384)
            return await self._control(
                worker, row, operation, parse_epoch_request(body), hashlib.sha256(body).hexdigest()
            )
        if operation == "retire":
            body = await self._body(request, 16384)
            retire_request = parse_retire_request(body)
            async with self._mutex(launch_id):
                current = self._row(worker, launch_id)
                reservation = self._registry.begin_operation(
                    worker.principal_id,
                    launch_id,
                    current.revision,
                    retire_request.as_dict()["operation_id"],
                    "retire",
                    hashlib.sha256(body).hexdigest(),
                )
                if reservation.disposition == "new":
                    current = self._row(worker, launch_id)
                    self._registry.finish_operation(
                        worker.principal_id,
                        launch_id,
                        current.revision,
                        retire_request.as_dict()["operation_id"],
                        "applied",
                    )
            await self._retire(worker.principal_id, launch_id)
            state = self._row(worker, launch_id).state
            return self._json(
                {
                    "schema_version": 1,
                    "launch_id": launch_id,
                    "operation_id": retire_request.as_dict()["operation_id"],
                    "state": state,
                },
                200 if state == "retired" else 202,
            )
        epoch = None
        if operation == "capacity-status":
            require(set(request.query) == {"epoch"} and len(request.query.getall("epoch")) == 1)
            epoch = request.query["epoch"]
            require(row.owned_epoch == epoch)
        binding = await self._binding_for_stop(row)
        status = await self._runner_status(binding, epoch=epoch)
        return self._json(
            {
                "schema_version": 1,
                "launch_id": launch_id,
                "receipt_revision": row.revision,
                "runner_status": status,
            }
        )

    async def _verify(
        self, runtime: ProvisionedRuntime, request: LaunchRequest
    ) -> tuple[Dispatchable, SelectedArtifact, Projection, dict[str, Any]]:
        # Never reconstruct original payload/envelope or require the trimmed stream.
        verdict = await verify_delivery(
            runtime.broker, dict(request.source_fields), trusted_manifest=runtime.manifest
        )
        if type(verdict) is not Dispatchable:
            raise _Refusal("unavailable", 503)
        validate_launch_request_delivery(request, delivery=verdict, manifest=runtime.manifest)
        m = runtime.manifest.as_dict()
        raw = await runtime.broker.read_control(
            "protected:control:readiness:" + m["runtime_id"] + ":" + m["runtime_generation"]
        )
        require(raw is not None)
        readiness = parse_readiness(present(raw)).as_dict()
        rawq = await runtime.broker.read_control(
            "protected:control:qualification:"
            + m["qualification_id"]
            + ":"
            + readiness["qualification_generation"]
        )
        require(rawq is not None)
        qualification = parse_qualification(present(rawq))
        measurement = runtime.measurements.load(
            manifest=runtime.manifest, qualification=qualification
        )
        selected = await select_artifact(
            runtime.broker, trusted_manifest=runtime.manifest, measurement=measurement
        )
        execution = parse_execution_config(
            runtime.execution.canonical_bytes, manifest=runtime.manifest
        )
        projection = runtime.projections.load(
            manifest=runtime.manifest, measurement=measurement, execution=execution
        )
        platform = validate_projection_binding(
            projection, manifest=runtime.manifest, measurement=measurement, execution=execution
        )
        return verdict, selected, projection, platform

    async def _create(
        self, worker: ProvisionedWorker, request: LaunchRequest, *, request_digest: str
    ) -> web.Response:
        if request.as_dict()["runtime_id"] != worker.runtime_id:
            raise _Refusal("unauthorized", 403)
        prior = self._registry.find_request(worker.principal_id, request.as_dict()["request_id"])
        if prior is not None:
            if prior.request_digest != request_digest:
                raise _Refusal("conflict", 409)
            if prior.boot_id != self._boot_id or prior.state in {"retiring", "retired", "failed"}:
                raise _Refusal("retired", 409)
        runtime = self._runtimes[worker.runtime_id]
        delivery, selected, projection, platform = await self._verify(runtime, request)
        m, q, p = (
            runtime.manifest.as_dict(),
            selected.qualification.as_dict(),
            runtime.execution.as_dict(),
        )
        budget = min(120, p["runner"]["execution_deadline_seconds"])
        # Existing QueuedTurn deadline is optional seconds since Unix epoch.
        # Frozen delivery contracts currently have no execution-deadline field.
        # Admission proof expiry does not revoke accepted drain.
        deadline: float | None = None
        if deadline is not None:
            budget = min(budget, deadline - time.time())
        require(budget > 0)
        slots = render_delivery_environment(
            execution=runtime.execution, manifest=runtime.manifest, delivery=delivery
        )
        ownership = LaunchOwnership(
            m["runtime_id"],
            m["runtime_generation"],
            runtime.manifest.digest,
            q["qualification_id"],
            q["qualification_generation"],
            q["measurement_record_id"],
            m["execution_config_digest"],
            request.as_dict()["event_id"],
            request.as_dict()["committed_stream_id"],
            request.as_dict()["payload_sha256"],
            request.as_dict()["execution_session_key"],
            projection.digest,
            m["guard_identity"]["control_id"],
            m["guard_identity"]["revision"],
            m["guard_identity"]["config_sha256"],
        )
        async with self._create_lock:
            row = self._registry.create(
                worker_principal_id=worker.principal_id,
                request_id=request.as_dict()["request_id"],
                request_digest=request_digest,
                ownership=ownership,
                creation_budget_ms=max(1, int(budget * 1000)),
            )
            if row.launch_id in self._live:
                live = self._live[row.launch_id]
                if row.state not in {"preparing", "idle"}:
                    raise _Refusal("conflict", 409)
                return self._create_response(row, live, retry=True)
            if row.boot_id != self._boot_id or row.state != "preparing" or row.objects.pod_name:
                raise _Refusal("retired", 409)
            launch_id = row.launch_id
            suffix = UUID(launch_id).hex
            row = self._registry.plan_objects(
                worker.principal_id,
                launch_id,
                row.revision,
                namespace=projection.as_dict()["namespace"],
                pod_name="pl-p-" + suffix,
                service_name="pl-s-" + suffix,
                secret_name="pl-c-" + suffix,
            )
            try:
                capability, runner_token = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
                now_ms = int(time.time() * 1000)
                execution_budget = p["runner"]["execution_deadline_seconds"]
                if deadline is not None:
                    execution_budget = min(execution_budget, deadline - time.time())
                expiry_ms = ((now_ms + int(execution_budget * 1000)) // 1000) * 1000
                parsed = parse_scoped_conversation_id(protected_history_key(delivery))
                require(
                    parsed is not None and parsed.kind == "@protected" and parsed.identity is None
                )
                transcript = present(parsed).kind + ":" + present(parsed).address
                history = self._credentials.history_token(
                    p["bindings"]["history_signing_credential_ref"],
                    agent_id=p["agent_id"],
                    transcript_binding=transcript,
                    expires_at=expiry_ms // 1000,
                )
                model = self._credentials.model_credential(p["runner"]["model_credential_ref"])
                source = {
                    "runner-token": runner_token,
                    "history-token": history,
                    "model-credential": model,
                }
                secret_document(
                    present(row.objects.namespace), present(row.objects.secret_name), source
                )
                href, mref = (
                    p["bindings"]["history_signing_credential_ref"],
                    p["runner"]["model_credential_ref"],
                )
                self._registry.record_assignment(
                    worker.principal_id,
                    launch_id,
                    row.revision,
                    AssignmentMetadata(
                        sha256_text(capability),
                        sha256_text(runner_token),
                        sha256_text(transcript),
                        href["id"],
                        href["generation"],
                        str(expiry_ms),
                        mref["id"],
                        mref["generation"],
                    ),
                )
            except BaseException:
                current = self._row(worker, launch_id)
                self._registry.transition(
                    worker.principal_id, launch_id, current.revision, "retiring"
                )
                asyncio.create_task(self._retire(worker.principal_id, launch_id))
                raise
            live = _LiveLaunch(
                worker,
                runtime,
                request,
                selected,
                projection,
                platform,
                delivery,
                slots,
                capability,
                source,
                expiry_ms,
                asyncio.get_running_loop().time() + budget,
            )
            self._live[launch_id] = live
            # Cleanup ownership is already durable and registered before scheduling.
            live.job = asyncio.create_task(self._prepare(launch_id), name="protected-launch-create")
            return self._create_response(self._row(worker, launch_id), live, retry=False)

    def _create_response(
        self, row: LaunchSnapshot, live: _LiveLaunch, *, retry: bool
    ) -> web.Response:
        value = {
            "schema_version": 1,
            "launch_id": row.launch_id,
            "receipt_revision": row.revision,
            "state": row.state,
            "receipt": live.receipt.as_dict() if live.receipt else None,
            "control_capability": live.control_capability,
        }
        record = (
            LaunchPendingSuccess(canonical_json(value))
            if row.state == "preparing"
            else LaunchSuccess(canonical_json(value))
        )
        return self._json(
            record.as_dict(), 202 if row.state == "preparing" else (200 if retry else 201)
        )

    async def _prepare(self, launch_id: str) -> None:
        live = self._live[launch_id]
        owner = live.worker.principal_id
        try:
            async with asyncio.timeout_at(live.deadline):
                for kind in ("secrets", "services", "pods"):
                    async with self._mutex(launch_id):
                        row = self._registry.get(owner, launch_id)
                        require(row.state == "preparing" and not self._closing)
                        o = row.objects
                        if kind == "secrets":
                            document = secret_document(
                                present(o.namespace), present(o.secret_name), live.source
                            )
                        elif kind == "services":
                            document = service_document(
                                present(o.namespace), present(o.service_name), launch_id
                            )
                        else:
                            document = render_pod(
                                live.projection,
                                launch_id=launch_id,
                                pod_name=present(o.pod_name),
                                secret_name=present(o.secret_name),
                                session_id=live.slots[BootEnv.env_key("session_id")],
                                history_ref=live.slots[BootEnv.env_key("history_ref")],
                            )
                        observed = await self._kube.create(present(o.namespace), kind, document)
                        uid, version = object_identity(
                            observed,
                            namespace=present(o.namespace),
                            name=present(o.secret_name)
                            if kind == "secrets"
                            else present(o.service_name)
                            if kind == "services"
                            else present(o.pod_name),
                        )
                        self._registry.record_object_uid(
                            owner,
                            launch_id,
                            row.revision,
                            cast(
                                Any,
                                {"secrets": "secret", "services": "service", "pods": "pod"}[kind],
                            ),
                            uid,
                            version,
                        )
                while True:
                    async with self._mutex(launch_id):
                        row = self._registry.get(owner, launch_id)
                        require(row.state == "preparing" and not self._closing)
                        o = row.objects
                        binding = IssuerPodBinding(
                            present(o.namespace),
                            present(o.pod_name),
                            present(o.pod_uid),
                            present(o.service_name),
                            present(o.service_uid),
                            self._runner_origin(present(o.service_name), present(o.namespace)),
                            live.source["runner-token"],
                        )
                        live.binding = binding
                        try:
                            observation = await observe_direct_pod(
                                self._kube,
                                binding,
                                launch_id=launch_id,
                                projection=live.projection,
                                measured_platform=live.platform,
                                secret_name=present(o.secret_name),
                                secret_uid=present(o.secret_uid),
                                secret_version=present(o.secret_resource_version),
                                source=live.source,
                                session_id=live.slots[BootEnv.env_key("session_id")],
                                history_ref=live.slots[BootEnv.env_key("history_ref")],
                            )
                            binding = IssuerPodBinding(
                                binding.namespace,
                                binding.pod_name,
                                binding.pod_uid,
                                binding.service_name,
                                binding.service_uid,
                                self._pod_origin(observation.pod_ip),
                                binding.runner_token,
                            )
                            live.binding = binding
                            status = await self._runner_status(binding)
                            self._check_status(status, live, row, idle=True)
                        except LaunchControlUnavailable:
                            observation = None
                        if observation is not None:
                            row = self._registry.record_container_identity(
                                owner,
                                launch_id,
                                row.revision,
                                observation.container_id,
                                observation.node_uid,
                            )
                            live.receipt = self._receipt(row, live, observation.started_at)
                            self._registry.transition(owner, launch_id, row.revision, "idle")
                            live.watchdog = asyncio.create_task(
                                self._expire(launch_id), name="protected-launch-deadline"
                            )
                            return
                    await asyncio.sleep(1)
        except (Exception, asyncio.CancelledError):  # noqa: BLE001 - owned cleanup on all failures
            async with self._mutex(launch_id):
                row = self._registry.get(owner, launch_id)
                if row.state not in {"retiring", "retired"}:
                    self._registry.transition(owner, launch_id, row.revision, "retiring")
            # Never retry a create; owned recorded-name reconciliation only.
            await self._retire(owner, launch_id, cancel_job=False)

    def _runner_origin(self, service: str, namespace: str) -> str:
        return f"{self._config.runner_scheme}://{service}.{namespace}.{self._config.runner_dns_suffix}:8080"

    def _pod_origin(self, pod_ip: str) -> str:
        ip = ipaddress.ip_address(pod_ip)
        host = f"[{ip}]" if ip.version == 6 else str(ip)
        return f"{self._config.runner_scheme}://{host}:8080"

    async def _expire(self, launch_id: str) -> None:
        live = self._live[launch_id]
        await asyncio.sleep(max(0, (live.expires_at_ms - int(time.time() * 1000)) / 1000))
        await self._retire(live.worker.principal_id, launch_id)

    def _receipt(self, row: LaunchSnapshot, live: _LiveLaunch, started_at: str) -> LaunchReceipt:
        m, q, o, a = (
            live.runtime.manifest.as_dict(),
            live.selected.qualification.as_dict(),
            row.objects,
            row.assignment,
        )
        require(a is not None)
        value = {
            "schema_version": 1,
            "launch_id": row.launch_id,
            "authority_domain_id": m["substrate"]["authority_domain_id"],
            "runtime_id": m["runtime_id"],
            "runtime_generation": m["runtime_generation"],
            "manifest_digest": live.runtime.manifest.digest,
            "qualification_id": q["qualification_id"],
            "qualification_generation": q["qualification_generation"],
            "execution_config_digest": m["execution_config_digest"],
            "measurement_ref": q["measurement_record_id"],
            "launch_identity": m["substrate"]["launch_identity"],
            "projection_sha256": live.projection.digest,
            "guard_identity": m["guard_identity"],
            **{
                k: live.request.as_dict()[k]
                for k in (
                    "event_id",
                    "committed_stream_id",
                    "payload_sha256",
                    "execution_session_key",
                )
            },
            "workload": {
                "namespace": present(o.namespace),
                "pod_name": present(o.pod_name),
                "pod_uid": present(o.pod_uid),
                "container_id": present(o.container_id),
                "started_at": started_at,
                "node_uid": o.node_uid,
                "service_name": present(o.service_name),
                "service_uid": present(o.service_uid),
            },
            "assignment": {
                "secret_name": present(o.secret_name),
                "secret_uid": present(o.secret_uid),
                "secret_resource_version": present(o.secret_resource_version),
                "immutable": True,
                "runner_token_key": "runner-token",
                "runner_token_sha256": present(a).runner_token_sha256,
                "history_token_key": "history-token",
                "history_binding_sha256": present(a).history_binding_sha256,
                "history_signing_credential_ref": {
                    "id": present(a).history_signing_credential_id,
                    "generation": present(a).history_signing_credential_generation,
                },
                "history_expires_at_ms": present(a).history_expires_at_ms,
                "model_key": "model-credential",
                "model_credential_ref": {
                    "id": present(a).model_credential_id,
                    "generation": present(a).model_credential_generation,
                },
            },
            "platform": live.platform,
            "route": {
                "kind": "mediator-v1",
                "launch_id": row.launch_id,
                "worker_principal_id": row.worker_principal_id,
            },
        }
        return LaunchReceipt(canonical_json(value))

    async def _runner_status(
        self, binding: IssuerPodBinding, *, epoch: str | None = None
    ) -> dict[str, Any]:
        headers = {"Authorization": "Bearer " + binding.runner_token}
        if epoch is not None:
            headers[EPOCH_HEADER] = epoch
        async with asyncio.timeout(5):
            async with self._runner.stream(
                "GET", binding.runner_url + "/v1/status", headers=headers
            ) as response:
                require(response.status_code == 200)
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    require(len(body) + len(chunk) <= 16384)
                    body.extend(chunk)
                status = decode_observation(bytes(body))
                validate_runner_status(status)
                return status

    @staticmethod
    def _check_status(
        status: dict[str, Any], live: _LiveLaunch, row: LaunchSnapshot, *, idle: bool
    ) -> None:
        require(
            status["ready"] is True
            and "read-only" in status["tool_access"]
            and status.get("session_id") == live.slots[BootEnv.env_key("session_id")]
            and status.get("sandbox_id") == row.objects.pod_name
            and status.get("managed_workspace") is False
            and status.get("cwd") is None
        )
        if idle:
            require(
                status["status"] == "idle-awaiting-input"
                and status["turn_active"] is False
                and status["turn_epoch"] is None
                and row.owned_epoch is None
            )
        else:
            require(
                status["turn_active"] is True
                and status["turn_epoch"] == row.owned_epoch
                and status.get("capacity_admission_result") == "pending"
            )

    async def _positive(
        self, worker: ProvisionedWorker, row: LaunchSnapshot, *, idle: bool
    ) -> _LiveLaunch:
        require(row.boot_id == self._boot_id and row.state == ("idle" if idle else "waiting"))
        live = self._live[row.launch_id]
        require(
            live.receipt is not None
            and live.binding is not None
            and live.expires_at_ms > int(time.time() * 1000)
        )
        delivery, selected, projection, platform = await self._verify(live.runtime, live.request)
        require(
            selected.qualification.canonical_bytes == live.selected.qualification.canonical_bytes
            and projection.canonical_bytes == live.projection.canonical_bytes
            and platform == live.platform
        )
        o = row.objects
        observation = await observe_direct_pod(
            self._kube,
            present(live.binding),
            launch_id=row.launch_id,
            projection=projection,
            measured_platform=platform,
            secret_name=present(o.secret_name),
            secret_uid=present(o.secret_uid),
            secret_version=present(o.secret_resource_version),
            source=live.source,
            session_id=live.slots[BootEnv.env_key("session_id")],
            history_ref=live.slots[BootEnv.env_key("history_ref")],
            container_id=present(o.container_id),
            node_uid=o.node_uid,
        )
        require(observation.started_at == present(live.receipt).as_dict()["workload"]["started_at"])
        require(present(live.binding).runner_url == self._pod_origin(observation.pod_ip))
        self._check_status(
            await self._runner_status(
                present(live.binding), epoch=None if idle else row.owned_epoch
            ),
            live,
            row,
            idle=idle,
        )
        live.delivery = delivery
        return live

    async def _start(
        self,
        request: web.Request,
        worker: ProvisionedWorker,
        row: LaunchSnapshot,
        parsed: Any,
        request_digest: str,
    ) -> web.StreamResponse:
        launch_id, value = row.launch_id, parsed.as_dict()
        # Initial streaming acceptance stays inside the five-second HTTP deadline.
        # The stream itself has the independent execution deadline below.
        async with self._mutex(launch_id):
            row = self._row(worker, launch_id)
            live = await self._positive(worker, row, idle=True)
            validate_start_request_delivery(parsed, delivery=live.delivery)
            reservation = self._registry.begin_operation(
                worker.principal_id,
                launch_id,
                value["expected_receipt_revision"],
                value["operation_id"],
                "start",
                request_digest,
            )
            if reservation.disposition != "new":
                raise _Refusal("conflict", 409)
            binding = present(live.binding)
            require(binding is not None)
            headers = {"Authorization": "Bearer " + binding.runner_token}
            if value["capacity_admission"]:
                headers["X-Curie-Capacity-Admission"] = "wait"
            upstream = None
            try:
                upstream = await self._runner.send(
                    self._runner.build_request(
                        "POST",
                        binding.runner_url + "/v1/event",
                        content=canonical_json(value["event"]),
                        headers=headers,
                        extensions={
                            "timeout": {
                                "connect": 5,
                                "write": 5,
                                "pool": 5,
                                "read": max(
                                    0.001, (live.expires_at_ms - int(time.time() * 1000)) / 1000
                                ),
                            }
                        },
                    ),
                    stream=True,
                )
                require(upstream.status_code == 200)
                epoch = upstream.headers.get(EPOCH_HEADER)
                require(
                    type(epoch) is str and re.fullmatch(r"[A-Za-z0-9_-]{32,256}", epoch) is not None
                )
                current = self._row(worker, launch_id)
                self._registry.finish_operation(
                    worker.principal_id,
                    launch_id,
                    current.revision,
                    value["operation_id"],
                    "applied",
                    epoch=epoch,
                    state="waiting" if value["capacity_admission"] else "running",
                )
            except BaseException:
                if upstream:
                    await upstream.aclose()
                current = self._row(worker, launch_id)
                self._registry.finish_operation(
                    worker.principal_id,
                    launch_id,
                    current.revision,
                    value["operation_id"],
                    "uncertain",
                )
                asyncio.create_task(self._retire(worker.principal_id, launch_id))
                raise
            response = web.StreamResponse(
                headers={
                    "Content-Type": "application/x-ndjson",
                    "X-Curie-Launch-Id": launch_id,
                    "X-Curie-Operation-Id": value["operation_id"],
                    EPOCH_HEADER: epoch,
                }
            )
            try:
                await response.prepare(request)
                request[_CONTROL_DEADLINE].reschedule(None)
            except BaseException:
                await upstream.aclose()
                asyncio.create_task(self._retire(worker.principal_id, launch_id))
                raise
        # Return acceptance now; request middleware deadline must not truncate
        # the owned stream. aiohttp writes are completed by the tracked task.
        await self._forward_stream(request, response, upstream, live, launch_id)
        return response

    async def _forward_stream(
        self,
        request: web.Request,
        response: web.StreamResponse,
        upstream: httpx.Response,
        live: _LiveLaunch,
        launch_id: str,
    ) -> None:
        try:
            async with asyncio.timeout(
                max(0, (live.expires_at_ms - int(time.time() * 1000)) / 1000)
            ):
                pending = bytearray()
                async for chunk in upstream.aiter_bytes():
                    pending.extend(chunk)
                    while b"\n" in pending:
                        line, _, tail = pending.partition(b"\n")
                        require(len(line) <= 262144)
                        parse_ndjson_line(line.decode("utf-8"))
                        await response.write(line + b"\n")
                        pending = bytearray(tail)
                    require(len(pending) <= 262144)
                require(not pending)
            await response.write_eof()
        except BaseException:  # noqa: BLE001 - close streamed transport and retain cleanup
            asyncio.create_task(self._retire(live.worker.principal_id, launch_id))
            if request.transport:
                request.transport.close()
        finally:
            await upstream.aclose()

    async def _control(
        self,
        worker: ProvisionedWorker,
        row: LaunchSnapshot,
        operation: str,
        parsed: Any,
        request_digest: str,
    ) -> web.Response:
        value, launch_id = parsed.as_dict(), row.launch_id
        kind = "deny" if operation == "admit" and not value["allow"] else operation
        async with self._mutex(launch_id):
            row = self._row(worker, launch_id)
            if kind == "admit":
                await self._positive(worker, row, idle=False)
            require(row.owned_epoch == value["epoch"])
            reservation = self._registry.begin_operation(
                worker.principal_id,
                launch_id,
                value.get("expected_receipt_revision", row.revision),
                value["operation_id"],
                cast(OperationKind, kind),
                request_digest,
                epoch=value["epoch"],
            )
            if reservation.disposition == "existing":
                if reservation.operation.status != "applied":
                    raise _Refusal("conflict", 409)
            else:
                try:
                    binding = await self._binding_for_stop(row)
                    path = "/v1/turn-admit" if operation == "admit" else "/v1/" + operation
                    body = (
                        {"allow": value["allow"]}
                        if operation == "admit"
                        else (
                            {"type": "interrupt", "reason": "protected-launch-stop"}
                            if operation == "interrupt"
                            else None
                        )
                    )
                    await self._runner_control(binding, path, value["epoch"], body)
                    current = self._row(worker, launch_id)
                    self._registry.finish_operation(
                        worker.principal_id,
                        launch_id,
                        current.revision,
                        value["operation_id"],
                        "applied",
                        epoch=value["epoch"],
                        state="running" if kind == "admit" else None,
                    )
                except BaseException:
                    current = self._row(worker, launch_id)
                    self._registry.finish_operation(
                        worker.principal_id,
                        launch_id,
                        current.revision,
                        value["operation_id"],
                        "uncertain",
                    )
                    asyncio.create_task(self._retire(worker.principal_id, launch_id))
                    raise
        return self._json(
            {
                "schema_version": 1,
                "launch_id": launch_id,
                "operation_id": value["operation_id"],
                "epoch": value["epoch"],
                "result": "applied",
            }
        )

    async def _runner_control(
        self, binding: IssuerPodBinding, path: str, epoch: str, body: Any
    ) -> None:
        async with asyncio.timeout(5):
            async with self._runner.stream(
                "POST",
                binding.runner_url + path,
                headers={"Authorization": "Bearer " + binding.runner_token, EPOCH_HEADER: epoch},
                content=None if body is None else canonical_json(body),
            ) as response:
                require(response.status_code == 200)
                raw = bytearray()
                async for chunk in response.aiter_bytes():
                    require(len(raw) + len(chunk) <= 16384)
                    raw.extend(chunk)
                require(decode_observation(bytes(raw)) == {"ok": True})

    async def _binding_for_stop(self, row: LaunchSnapshot) -> IssuerPodBinding:
        live = self._live.get(row.launch_id)
        if live and live.binding:
            return live.binding
        o, a = row.objects, row.assignment
        require(a is not None and present(o.secret_uid) is not None)
        secret = await self._kube.read(present(o.namespace), "secrets", present(o.secret_name))
        require(
            object_identity(secret, namespace=present(o.namespace), name=present(o.secret_name))
            == (present(o.secret_uid), present(o.secret_resource_version))
        )
        require(
            secret is not None
            and secret.get("immutable") is True
            and secret.get("type") == "Opaque"
        )
        require(
            present(secret)["data"].keys() == {"runner-token", "history-token", "model-credential"}
        )
        token = base64.b64decode(present(secret)["data"]["runner-token"], validate=True).decode(
            "utf-8"
        )
        require(hmac.compare_digest(sha256_text(token), present(a).runner_token_sha256))
        pod = await self._kube.read(present(o.namespace), "pods", present(o.pod_name))
        require(pod is not None and pod["metadata"]["uid"] == present(o.pod_uid))
        return IssuerPodBinding(
            present(o.namespace),
            present(o.pod_name),
            present(o.pod_uid),
            present(o.service_name),
            present(o.service_uid),
            self._pod_origin(present(pod)["status"]["podIP"]),
            token,
        )

    async def _retire(self, owner: str, launch_id: str, *, cancel_job: bool = True) -> None:
        live = self._live.get(launch_id)
        async with self._mutex(launch_id):
            row = self._registry.get(owner, launch_id)
            if row.state == "retired":
                return
            if row.state != "retiring":
                self._registry.transition(owner, launch_id, row.revision, "retiring")
        # Cancel and join before cleanup, outside the shared effect mutex.
        current = asyncio.current_task()
        if live and cancel_job and live.job and live.job is not current and not live.job.done():
            live.job.cancel()
            await asyncio.gather(live.job, return_exceptions=True)
        if live and live.watchdog and live.watchdog is not current:
            live.watchdog.cancel()
        async with self._mutex(launch_id):
            try:
                row = self._registry.get(owner, launch_id)
                o = row.objects
                # Reconcile uncertain creation by exact durable planned name,
                # never labels. These objects remain quarantined/retiring.
                for kind in ("secret", "service", "pod"):
                    if getattr(o, kind + "_name") and getattr(o, kind + "_uid") is None:
                        doc = await self._kube.read(
                            present(o.namespace), kind + "s", getattr(o, kind + "_name")
                        )
                        if doc is not None:
                            uid, version = object_identity(
                                doc, namespace=present(o.namespace), name=getattr(o, kind + "_name")
                            )
                            row = self._registry.record_object_uid(
                                owner, launch_id, row.revision, kind, uid, version
                            )
                            o = row.objects
                require(
                    present(o.pod_uid) is not None
                    and present(o.service_uid) is not None
                    and present(o.secret_uid) is not None
                )
                if row.owned_epoch:
                    try:
                        await self._runner_control(
                            await self._binding_for_stop(row), "/v1/timeout", row.owned_epoch, None
                        )
                    except Exception:  # noqa: BLE001 - fixed fail-closed diagnostic
                        pass  # UID deletion is still attempted; never implies success.
                if row.termination is None:
                    pod = await self._kube.read(present(o.namespace), "pods", present(o.pod_name))
                    require(
                        pod is not None
                        and pod["metadata"].get("uid") == present(o.pod_uid)
                        and pod["metadata"].get("finalizers") == [FINALIZER]
                    )
                    # Fence the exact owned Pod even when it has not started
                    # a runner yet. Missing container proof prevents retirement
                    # success; it must not postpone the UID deletion request.
                    await self._kube.delete(
                        present(o.namespace), "pods", present(o.pod_name), present(o.pod_uid)
                    )
                    pod = await self._kube.read(present(o.namespace), "pods", present(o.pod_name))
                    require(pod is not None and pod["metadata"].get("uid") == present(o.pod_uid))
                    if o.container_id is None:
                        statuses = present(pod).get("status", {}).get("containerStatuses", [])
                        require(
                            len(statuses) == 1
                            and statuses[0]["name"] == "runner"
                            and statuses[0].get("restartCount") == 0
                        )
                        node = await self._kube.node(present(pod)["spec"]["nodeName"])
                        row = self._registry.record_container_identity(
                            owner,
                            launch_id,
                            row.revision,
                            statuses[0]["containerID"],
                            node["metadata"]["uid"],
                        )
                        o = row.objects
                    proof = termination_observation(
                        pod,
                        namespace=present(o.namespace),
                        pod_name=present(o.pod_name),
                        pod_uid=present(o.pod_uid),
                        container_id=present(o.container_id),
                    )
                    row = self._registry.record_termination_observation(
                        owner, launch_id, row.revision, proof
                    )
                pod = await self._kube.read(present(o.namespace), "pods", present(o.pod_name))
                if pod is not None:
                    require(
                        pod["metadata"]["uid"] == present(o.pod_uid) and row.termination is not None
                    )
                    finalizers = pod["metadata"].get("finalizers", [])
                    if finalizers:
                        require(finalizers == [FINALIZER])
                        await self._kube.remove_finalizer(
                            present(o.namespace),
                            present(o.pod_name),
                            present(o.pod_uid),
                            pod["metadata"]["resourceVersion"],
                        )
                    pod = await self._kube.read(present(o.namespace), "pods", present(o.pod_name))
                    require(pod is None)
                for kind in ("service", "secret"):
                    name, uid = getattr(o, kind + "_name"), getattr(o, kind + "_uid")
                    doc = await self._kube.read(present(o.namespace), kind + "s", name)
                    if doc is not None:
                        require(doc["metadata"]["uid"] == uid)
                        await self._kube.delete(present(o.namespace), kind + "s", name, uid)
                        require(
                            await self._kube.read(present(o.namespace), kind + "s", name) is None
                        )
                row = self._registry.get(owner, launch_id)
                self._registry.record_cleanup_receipt(
                    owner,
                    launch_id,
                    row.revision,
                    CleanupReceipt(
                        present(o.pod_uid), present(o.service_uid), present(o.secret_uid)
                    ),
                )
                if live:
                    live.source.clear()
                    live.binding = None
            except Exception:  # noqa: BLE001 - fixed fail-closed diagnostic
                return  # Incomplete proof remains durable retiring; no capacity refund.

    async def start_cleanup(self) -> None:
        require(self._cleanup_task is None)
        self._cleanup_task = asyncio.create_task(
            self._cleanup_loop(), name="protected-launch-cleanup"
        )

    async def _cleanup_loop(self) -> None:
        cursor = None
        while not self._closing:
            try:
                rows = self._registry.enumerate_retiring(limit=64, after_launch_id=cursor)
                for row in rows:
                    await self._retire(row.worker_principal_id, row.launch_id)
                cursor = rows[-1].launch_id if rows else None
            except Exception:  # noqa: BLE001 - retain ownership and retry bounded cleanup
                cursor = None
            await asyncio.sleep(1)

    async def _shutdown(self, _app: web.Application) -> None:
        self._closing = True
        if self._cleanup_task:
            self._cleanup_task.cancel()
            await asyncio.gather(self._cleanup_task, return_exceptions=True)
        for launch_id, live in tuple(self._live.items()):
            await self._retire(live.worker.principal_id, launch_id)
        tasks = [live.watchdog for live in self._live.values() if live.watchdog]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _close(self, _app: web.Application) -> None:
        await self._runner.aclose()
        await self._kube.close()
        self._registry.close()


async def serve(server: ProtectedLaunchServer, stop: asyncio.Event) -> None:
    """Actual HTTPS mTLS listener under the explicit provisioner configuration."""
    require(type(server) is ProtectedLaunchServer)
    runner = web.AppRunner(
        server.application, access_log=None, shutdown_timeout=5, handler_cancellation=True
    )
    await runner.setup()
    try:
        await server.start_cleanup()
        site = web.TCPSite(
            runner,
            server._config.host,
            server._config.port,
            ssl_context=server._tls,
            shutdown_timeout=5,
        )
        await site.start()
        await stop.wait()
    finally:
        await runner.cleanup()


def validate_runner_status(status: dict[str, Any]) -> None:
    """Existing runner status surface only; no status read grants execution."""
    required = {
        "status",
        "ready",
        "turn_active",
        "history_durable",
        "tool_access",
        "turn_epoch",
        "capacity_admission",
    }
    optional = {
        "channel_read",
        "capacity_admission_result",
        "session_id",
        "sandbox_id",
        "managed_workspace",
        "cwd",
    }
    require(required <= status.keys() and status.keys() <= required | optional)
    require(
        status["status"]
        in {"done", "idle-awaiting-input", "awaiting-approval", "classified-failure"}
    )
    require(all(type(status[k]) is bool for k in ("ready", "turn_active", "history_durable")))
    require(
        status["capacity_admission"] is True
        and type(status["tool_access"]) is list
        and all(v in {"read-only", "unrestricted"} for v in status["tool_access"])
    )
    epoch = status["turn_epoch"]
    require(
        epoch is None
        or (type(epoch) is str and re.fullmatch(r"[A-Za-z0-9_-]{32,256}", epoch) is not None)
    )
    if "capacity_admission_result" in status:
        require(status["capacity_admission_result"] in {"pending", "granted", "denied", "unknown"})
