"""Single-process provisioner-owned HTTPS mediator, not an application API.

@spec PROTECTED-HOOK-LANE-2 PROTECTED-HOOK-LANE-7 PROTECTED-HOOK-LANE-8.
Every positive operation independently verifies retained broker authority. No
source prompt or credential is persisted. Source plumbing supplies no preventive
or actual runtime qualification; absent provisioner proof refuses execution.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import re
import secrets
import ssl
import time
from dataclasses import dataclass, field
from typing import Any, cast, final
from uuid import UUID

from aiohttp import web
from channel_protocol import parse_scoped_conversation_id
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from curie_protected_hooks.artifact_measurement import ArtifactMeasurementDirectory
from curie_protected_hooks.authority_records import Manifest, parse_qualification, parse_readiness
from curie_protected_hooks.execution_namespace import derive_lane_namespace
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
    validate_projection_binding,
)
from .protected_launch_kubernetes import (
    KubernetesLaunchAPI,
    present,
    require,
    secret_document,
    sha256_text,
)
from .protected_launch_mechanics import CONTROL_DEADLINE as _CONTROL_DEADLINE
from .protected_launch_mechanics import LaunchAuthorizer, LiveLaunch, ProtectedLaunchMechanics
from .protected_launch_mechanics import LaunchRefusal as _Refusal
from .protected_launch_records import (
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
    LaunchOwnership,
    LaunchSnapshot,
    ProtectedLaunchRegistry,
    RegistryConflict,
    RegistryInvalid,
    RegistryRetired,
    RegistryUnauthorized,
)

CAPABILITY_HEADER = "X-Curie-Launch-Capability"
_CONTROL_SECONDS = 5


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


@dataclass(slots=True, repr=False)
class _ProductionGrant:
    worker: ProvisionedWorker
    runtime: ProvisionedRuntime
    request: LaunchRequest
    selected: SelectedArtifact
    delivery: Dispatchable


def _grant(live: LiveLaunch) -> _ProductionGrant:
    require(type(live.grant) is _ProductionGrant)
    return cast(_ProductionGrant, live.grant)


@final
class ProductionLaunchAuthorizer(LaunchAuthorizer):
    """Production launch authority: retained delivery, Readiness and Qualification."""

    __slots__ = ()

    def open_registry(self, root: str, *, boot_id: str) -> ProtectedLaunchRegistry:
        return ProtectedLaunchRegistry.open(root, boot_id=boot_id)

    async def verify(
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
        await self._require_unsettled(runtime, verdict)
        return verdict, selected, projection, platform

    @staticmethod
    async def _require_unsettled(runtime: ProvisionedRuntime, delivery: Dispatchable) -> None:
        # Retained admission proves origin, not permission to restart a settled
        # event. Any terminal key, including malformed or wrong-type evidence,
        # permanently fences positive execution without revoking recovery reads.
        key = (
            derive_lane_namespace(runtime.manifest).prefix + ":settlement:" + delivery.turn.event_id
        )
        try:
            async with asyncio.timeout(2):
                present_terminal = await runtime.broker.lane().runs.exists(key)
        except Exception:  # noqa: BLE001 - detach private broker diagnostics at the HTTP boundary
            raise _Refusal("unavailable", 503) from None
        if type(present_terminal) is not int or present_terminal != 0:
            raise _Refusal("unavailable", 503)

    async def reverify(self, live: LiveLaunch) -> tuple[Dispatchable, Projection, dict[str, Any]]:
        grant = _grant(live)
        delivery, selected, projection, platform = await self.verify(grant.runtime, grant.request)
        require(
            selected.qualification.canonical_bytes == grant.selected.qualification.canonical_bytes
            and projection.canonical_bytes == live.projection.canonical_bytes
            and platform == live.platform
        )
        return delivery, projection, platform

    async def accept_reverification(self, live: LiveLaunch, evidence: Any) -> None:
        grant = _grant(live)
        await self._require_unsettled(grant.runtime, evidence)
        grant.delivery = evidence

    async def require_unsettled(self, live: LiveLaunch) -> None:
        grant = _grant(live)
        await self._require_unsettled(grant.runtime, grant.delivery)

    def validate_start(self, parsed: Any, live: LiveLaunch) -> None:
        validate_start_request_delivery(parsed, delivery=_grant(live).delivery)

    def receipt(self, row: LaunchSnapshot, live: LiveLaunch, started_at: str) -> LaunchReceipt:
        grant = _grant(live)
        m, q, o, a = (
            grant.runtime.manifest.as_dict(),
            grant.selected.qualification.as_dict(),
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
            "manifest_digest": grant.runtime.manifest.digest,
            "qualification_id": q["qualification_id"],
            "qualification_generation": q["qualification_generation"],
            "execution_config_digest": m["execution_config_digest"],
            "measurement_ref": q["measurement_record_id"],
            "launch_identity": m["substrate"]["launch_identity"],
            "projection_sha256": live.projection.digest,
            "guard_identity": m["guard_identity"],
            **{
                k: grant.request.as_dict()[k]
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


class ProtectedLaunchServer:
    """Owns issuer-only assignments, authenticated HTTP and the production authorizer.

    Construct only under the provisioner owner with exact preloaded dependencies.
    The OS lock is retained through shutdown cleanup; deployment/failover fencing
    must be independently established outside this single-host implementation.
    Shared workload mechanics run only under ``ProductionLaunchAuthorizer``.
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
        self._config, self._credentials = config, credentials
        self._workers = {w.public_key_sha256: w for w in workers}
        self._runtimes = {r.manifest.as_dict()["runtime_id"]: r for r in runtimes}
        require(len(self._workers) == len(workers) and len(self._runtimes) == len(runtimes))
        for w in workers:
            require(type(w) is ProvisionedWorker and w.runtime_id in self._runtimes)
            domain = self._runtimes[w.runtime_id].manifest.as_dict()["substrate"][
                "authority_domain_id"
            ]
            require(w.authority_domain_id == domain == kubernetes.authority_domain_id)
        self._authorizer = ProductionLaunchAuthorizer()
        self._mechanics = ProtectedLaunchMechanics(
            self._authorizer,
            kubernetes=kubernetes,
            registry_root=config.registry_root,
            runner_scheme=config.runner_scheme,
            runner_dns_suffix=config.runner_dns_suffix,
            runner_ca_file=config.runner_ca_file,
        )
        self._boot_id = self._mechanics.boot_id
        self._registry: ProtectedLaunchRegistry = self._mechanics.registry
        self._live = self._mechanics.live
        self._app = web.Application(client_max_size=2097152, middlewares=[self._middleware])
        self._app.router.add_route("*", "/{tail:.*}", self._http)
        self._app.on_shutdown.append(self._mechanics.shutdown)
        self._app.on_cleanup.append(self._mechanics.close)

    @property
    def application(self) -> web.Application:
        return self._app

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
        if self._mechanics.closing:
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
            return self._json(
                await self._mechanics.control(
                    worker.principal_id, row, "admit", parsed, hashlib.sha256(body).hexdigest()
                )
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
            return await self._mechanics.start(
                request,
                worker.principal_id,
                row,
                parse_start_request(body),
                hashlib.sha256(body).hexdigest(),
            )
        if operation in {"interrupt", "timeout"}:
            body = await self._body(request, 16384)
            return self._json(
                await self._mechanics.control(
                    worker.principal_id,
                    row,
                    operation,
                    parse_epoch_request(body),
                    hashlib.sha256(body).hexdigest(),
                )
            )
        if operation == "retire":
            body = await self._body(request, 16384)
            retire_request = parse_retire_request(body)
            async with self._mechanics.mutex(launch_id):
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
            await self._mechanics.retire(worker.principal_id, launch_id)
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
        binding = await self._mechanics.binding_for_stop(row)
        status = await self._mechanics.runner_status(binding, epoch=epoch)
        return self._json(
            {
                "schema_version": 1,
                "launch_id": launch_id,
                "receipt_revision": row.revision,
                "runner_status": status,
            }
        )

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
        delivery, selected, projection, platform = await self._authorizer.verify(runtime, request)
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
        async with self._mechanics.create_lock:
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
                asyncio.create_task(self._mechanics.retire(worker.principal_id, launch_id))
                raise
            live = LiveLaunch(
                worker.principal_id,
                _ProductionGrant(worker, runtime, request, selected, delivery),
                projection,
                platform,
                slots,
                capability,
                source,
                expiry_ms,
                asyncio.get_running_loop().time() + budget,
            )
            self._live[launch_id] = live
            # Cleanup ownership is already durable and registered before scheduling.
            live.job = asyncio.create_task(
                self._mechanics.prepare(launch_id), name="protected-launch-create"
            )
            return self._create_response(self._row(worker, launch_id), live, retry=False)

    def _create_response(
        self, row: LaunchSnapshot, live: LiveLaunch, *, retry: bool
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

    async def start_cleanup(self) -> None:
        await self._mechanics.start_cleanup()


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
