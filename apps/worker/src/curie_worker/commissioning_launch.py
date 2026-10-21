"""Commissioning launch authorizer and mediator. @spec PROTECTED-COMMISSIONING-8.

The second closed authorizer of the shared launch mechanics. It cannot
construct a production launch request, receipt, Qualification or Readiness
type, and it reads no ``protected:control:*`` record. The mediator authenticates
the candidate worker over mTLS before any launch reservation: the client
certificate key must be the pinned candidate key and the certificate must expire
no later than the permit.

Campaign authority stays behind ``LaunchControl``, implemented over the
commissioning records outside this package: it verifies the campaign launch
request and its committed admission, consumes the launch reservation, and
records broker ownership ``creating`` with the deterministic owned namespace and
Pod name before any create. The observed Pod UID is bound after the create
response. Local cleanup facts live in ``CommissioningLaunchRegistry``.

An ambiguous create is resolved only by the exact recorded name, a known UID
and the guard-owned witness of the UID it admitted for that create. A same-name
object is never adopted: without a witness the ownership becomes ``uncertain``,
which blocks acceptance. This slice launches, observes and retires; starting a
turn, admission control and stop requests refuse until the supervisor and
provider gateway slices compose them.
"""

from __future__ import annotations

import asyncio
import hmac
import re
import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, cast, final
from uuid import UUID

from aci_protocol import BootEnv
from aiohttp import web
from cryptography import x509

from .commissioning_registry import (
    CampaignLaunchBinding,
    CommissioningLaunchRegistry,
    CommissioningLaunchSnapshot,
    GuardCreateWitness,
    object_names,
)
from .protected_kubernetes_projection import Projection
from .protected_launch_kubernetes import (
    KubernetesLaunchAPI,
    object_identity,
    present,
    require,
    secret_document,
    sha256_text,
)
from .protected_launch_mechanics import CONTROL_DEADLINE as _CONTROL_DEADLINE
from .protected_launch_mechanics import LaunchAuthorizer, LiveLaunch, ProtectedLaunchMechanics
from .protected_launch_mechanics import LaunchRefusal as _Refusal
from .protected_launch_records import canonical_json
from .protected_launch_registry import (
    AssignmentMetadata,
    RegistryConflict,
    RegistryInvalid,
    RegistryRetired,
    RegistryUnauthorized,
)
from .protected_launch_server import ServerConfig, public_key_identity

CAPABILITY_HEADER = "X-Curie-Launch-Capability"
OwnershipState = Literal["bound", "retiring", "retired", "uncertain"]
_KINDS = ("secret", "service", "pod")
_CONTROL_SECONDS = 5


class LaunchPlan(Protocol):
    """A verified campaign launch request; attributes only, never authority."""

    @property
    def launch_id(self) -> str: ...
    @property
    def launch_request_digest(self) -> str: ...
    @property
    def projection_digest(self) -> str: ...
    @property
    def launch_reservation_id(self) -> str: ...


class LaunchControl(Protocol):
    """Campaign launch authority over the commissioning broker records."""

    @property
    def permit_digest(self) -> str: ...
    @property
    def expires_at_ms(self) -> int: ...
    @property
    def worker_principal_id(self) -> str: ...
    @property
    def guard_identity(self) -> dict[str, str]:
        """The permit's guard identity: ``control_id``, ``revision`` and ``config_sha256``."""
        ...

    async def plan(self, raw: bytes) -> LaunchPlan:
        """Verify one campaign launch request and its committed admission."""
        ...

    async def own(self, plan: LaunchPlan, *, namespace: str, pod_name: str) -> None:
        """Consume the launch reservation and record ``creating`` ownership."""
        ...

    async def advance(self, launch_id: str, state: OwnershipState, pod_uid: str | None) -> None:
        """Move broker ownership to ``state``; a repeat of the current state is inert."""
        ...

    async def unsettled(self) -> tuple[str, ...]:
        """Launch IDs whose durable ownership is not ``retired``."""
        ...


class CreateWitness(Protocol):
    """The guard-owned identity of what it admitted for one recorded create."""

    async def admitted(
        self, *, kind: str, namespace: str, name: str, create_operation_id: str
    ) -> GuardCreateWitness | None: ...


@dataclass(frozen=True, slots=True, repr=False)
class CampaignRunnerAssignment:
    """Provisioner-owned runner slots and gateway-only credentials of one launch."""

    session_id: str
    history_ref: str
    history_token: str = field(repr=False)
    model_credential: str = field(repr=False)
    history_credential: tuple[str, str]
    model_credential_ref: tuple[str, str]


class CampaignAssignments(Protocol):
    async def assign(self, plan: LaunchPlan, *, expires_at_ms: int) -> CampaignRunnerAssignment: ...


@dataclass(frozen=True, slots=True, repr=False)
class CandidateWorker:
    principal_id: str
    public_key_sha256: str

    def __post_init__(self) -> None:
        require(re.fullmatch(r"[0-9a-f]{64}", self.public_key_sha256) is not None)
        require(
            re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}", self.principal_id) is not None
        )


@dataclass(frozen=True, slots=True, repr=False)
class MeasuredLaunch:
    """The provisioner's premeasured projection, platform row and execution budget."""

    projection: Projection
    platform: dict[str, Any]
    execution_seconds: int

    def __post_init__(self) -> None:
        require(type(self.projection) is Projection and type(self.platform) is dict)
        require(type(self.execution_seconds) is int and 1 <= self.execution_seconds <= 3600)


@dataclass(frozen=True, slots=True, repr=False)
class CommissioningLaunchReceipt:
    """Mediator-local observation of one started campaign workload; not a record."""

    launch_id: str
    namespace: str
    pod_name: str
    pod_uid: str
    container_id: str
    started_at: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "launch_id": self.launch_id,
            "workload": {
                "namespace": self.namespace,
                "pod_name": self.pod_name,
                "pod_uid": self.pod_uid,
                "container_id": self.container_id,
                "started_at": self.started_at,
            },
        }


@final
class CommissioningLaunchAuthorizer(LaunchAuthorizer):
    """Campaign launch authority: no production type, no positive turn in this slice."""

    __slots__ = ("_permit_digest",)

    def __init__(self, permit_digest: str) -> None:
        require(re.fullmatch(r"[0-9a-f]{64}", permit_digest) is not None)
        self._permit_digest = permit_digest

    def open_registry(self, root: str, *, boot_id: str) -> CommissioningLaunchRegistry:
        return CommissioningLaunchRegistry.open(
            root, boot_id=boot_id, permit_digest=self._permit_digest
        )

    async def reverify(self, live: LiveLaunch) -> tuple[Any, Projection, dict[str, Any]]:
        raise _Refusal("unavailable", 503)

    async def accept_reverification(self, live: LiveLaunch, evidence: Any) -> None:
        raise _Refusal("unavailable", 503)

    async def require_unsettled(self, live: LiveLaunch) -> None:
        raise _Refusal("unavailable", 503)

    def validate_start(self, parsed: Any, live: LiveLaunch) -> None:
        raise _Refusal("unavailable", 503)

    def receipt(
        self, row: CommissioningLaunchSnapshot, live: LiveLaunch, started_at: str
    ) -> CommissioningLaunchReceipt:
        require(type(row) is CommissioningLaunchSnapshot)
        o = row.objects
        return CommissioningLaunchReceipt(
            row.launch_id,
            present(o.namespace),
            present(o.pod_name),
            present(o.pod_uid),
            present(o.container_id),
            started_at,
        )


class CommissioningLaunchServer:
    """mTLS mediator for one campaign's candidate worker over the shared mechanics."""

    def __init__(
        self,
        config: ServerConfig,
        *,
        control: LaunchControl,
        worker: CandidateWorker,
        measured: MeasuredLaunch,
        assignments: CampaignAssignments,
        kubernetes: KubernetesLaunchAPI,
        witness: CreateWitness | None = None,
    ) -> None:
        require(type(config) is ServerConfig and type(kubernetes) is KubernetesLaunchAPI)
        require(type(worker) is CandidateWorker and type(measured) is MeasuredLaunch)
        require(worker.principal_id == control.worker_principal_id)
        self._tls = config.tls_context()
        self._config, self._control, self._worker = config, control, worker
        self._measured, self._assignments, self._witness = measured, assignments, witness
        self._kube = kubernetes
        self._mechanics = ProtectedLaunchMechanics(
            CommissioningLaunchAuthorizer(control.permit_digest),
            kubernetes=kubernetes,
            registry_root=config.registry_root,
            runner_scheme=config.runner_scheme,
            runner_dns_suffix=config.runner_dns_suffix,
            runner_ca_file=config.runner_ca_file,
        )
        self._registry: CommissioningLaunchRegistry = self._mechanics.registry
        self._live = self._mechanics.live
        self._settle_task: asyncio.Task[None] | None = None
        self._app = web.Application(client_max_size=65536, middlewares=[self._middleware])
        self._app.router.add_route("*", "/{tail:.*}", self._http)
        self._app.on_shutdown.append(self._shutdown)
        self._app.on_cleanup.append(self._mechanics.close)

    @property
    def application(self) -> web.Application:
        return self._app

    def _owner(self, request: web.Request) -> str:
        """Pinned candidate key, a certificate expiring by the permit, and a live permit."""
        ssl_object = request.transport.get_extra_info("ssl_object") if request.transport else None
        peer = ssl_object.getpeercert(binary_form=True) if ssl_object else None
        if not peer or not hmac.compare_digest(
            public_key_identity(peer), self._worker.public_key_sha256
        ):
            raise _Refusal("unauthorized", 403)
        not_after = x509.load_der_x509_certificate(peer).not_valid_after_utc.timestamp()
        if int(not_after * 1000) > self._control.expires_at_ms:
            raise _Refusal("unauthorized", 403)
        if int(time.time() * 1000) >= self._control.expires_at_ms:
            raise _Refusal("unauthorized", 403)
        return self._worker.principal_id

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

    async def _http(self, request: web.Request) -> web.StreamResponse:
        owner = self._owner(request)
        if self._mechanics.closing:
            raise _Refusal("unavailable", 503)
        if request.query_string:
            raise _Refusal("invalid", 400)
        if request.method == "POST" and request.path == "/v1/commissioning/launches":
            return await self._create(owner, await self._body(request, 32768))
        match = re.fullmatch(r"/v1/commissioning/launches/([0-9a-f-]{36})(/retire)?", request.path)
        if match is None:
            raise _Refusal("invalid", 404)
        launch_id, retire = match.groups()
        require(str(UUID(launch_id)) == launch_id)
        if request.method != ("POST" if retire else "GET"):
            raise _Refusal("invalid", 405)
        row = self._registry.get(owner, launch_id)
        live = self._live.get(launch_id)
        if row.boot_id == self._mechanics.boot_id:
            # The capability gates a live launch; an old boot's row is stop-only.
            cap = request.headers.getall(CAPABILITY_HEADER, [])
            if (
                live is None
                or len(cap) != 1
                or not hmac.compare_digest(cap[0].encode(), live.control_capability.encode())
            ):
                raise _Refusal("unauthorized", 403)
        elif not retire:
            raise _Refusal("retired", 409)
        if retire:
            if await self._body(request, 0):
                raise _Refusal("invalid", 400)
            await self._retire(owner, launch_id)
            row = self._registry.get(owner, launch_id)
            return self._json(self._view(row, None), 200 if row.state == "retired" else 202)
        return self._json(self._view(row, live))

    @staticmethod
    def _view(row: CommissioningLaunchSnapshot, live: LiveLaunch | None) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "launch_id": row.launch_id,
            "receipt_revision": row.revision,
            "state": row.state,
            "receipt": live.receipt.as_dict() if live and live.receipt else None,
        }

    async def _create(self, owner: str, raw: bytes) -> web.Response:
        plan = await self._control.plan(raw)
        projection = self._measured.projection
        if plan.projection_digest != projection.digest:
            raise _Refusal("unauthorized", 403)
        launch_id = plan.launch_id
        prior = self._registry.find(owner, launch_id)
        if prior is not None:
            live = self._live.get(launch_id)
            if live is None or prior.state not in {"preparing", "idle"}:
                raise _Refusal("retired", 409)
            if prior.binding.launch_request_digest != plan.launch_request_digest:
                raise _Refusal("conflict", 409)
            return self._created(prior, live, retry=True)
        namespace = projection.as_dict()["namespace"]
        pod_name = object_names(launch_id)[0]
        guard = self._control.guard_identity
        # Workload binding W's create operation is the consumed launch reservation.
        binding = CampaignLaunchBinding(
            self._control.permit_digest,
            plan.launch_request_digest,
            plan.projection_digest,
            plan.launch_reservation_id,
            plan.launch_reservation_id,
            guard["control_id"],
            guard["revision"],
            guard["config_sha256"],
        )
        budget = min(120, self._measured.execution_seconds)
        async with self._mechanics.create_lock:
            # Durable campaign ownership precedes the local row and every create.
            await self._control.own(plan, namespace=namespace, pod_name=pod_name)
            row = self._registry.create(
                worker_principal_id=owner,
                launch_id=launch_id,
                binding=binding,
                namespace=namespace,
                creation_budget_ms=budget * 1000,
            )
            try:
                capability, runner_token = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
                now_ms = int(time.time() * 1000)
                expiry_ms = min(
                    ((now_ms + self._measured.execution_seconds * 1000) // 1000) * 1000,
                    self._control.expires_at_ms,
                )
                require(expiry_ms > now_ms)
                assignment = await self._assignments.assign(plan, expires_at_ms=expiry_ms)
                require(type(assignment) is CampaignRunnerAssignment)
                source = {
                    "runner-token": runner_token,
                    "history-token": assignment.history_token,
                    "model-credential": assignment.model_credential,
                }
                secret_document(namespace, present(row.objects.secret_name), source)
                row = self._registry.record_assignment(
                    owner,
                    launch_id,
                    row.revision,
                    AssignmentMetadata(
                        sha256_text(capability),
                        sha256_text(runner_token),
                        sha256_text(assignment.session_id),
                        *assignment.history_credential,
                        str(expiry_ms),
                        *assignment.model_credential_ref,
                    ),
                )
            except BaseException:
                current = self._registry.get(owner, launch_id)
                self._registry.transition(owner, launch_id, current.revision, "retiring")
                asyncio.create_task(self._retire(owner, launch_id))
                raise
            live = LiveLaunch(
                owner,
                plan,
                projection,
                self._measured.platform,
                {
                    BootEnv.env_key("session_id"): assignment.session_id,
                    BootEnv.env_key("history_ref"): assignment.history_ref,
                },
                capability,
                source,
                expiry_ms,
                asyncio.get_running_loop().time() + budget,
            )
            self._live[launch_id] = live
            live.job = asyncio.create_task(
                self._prepare(owner, launch_id), name="commissioning-launch-create"
            )
            return self._created(row, live, retry=False)

    def _created(
        self, row: CommissioningLaunchSnapshot, live: LiveLaunch, *, retry: bool
    ) -> web.Response:
        value = self._view(row, live) | {"control_capability": live.control_capability}
        return self._json(value, 202 if row.state == "preparing" else (200 if retry else 201))

    async def _prepare(self, owner: str, launch_id: str) -> None:
        await self._mechanics.prepare(launch_id)
        row = self._registry.get(owner, launch_id)
        if row.state == "idle":
            try:
                await self._control.advance(launch_id, "bound", present(row.objects.pod_uid))
                return
            except Exception:  # noqa: BLE001 - unbound ownership never starts a turn
                await self._mechanics.retire(owner, launch_id, cancel_job=False)
        await self._settle(owner, launch_id)

    async def _retire(self, owner: str, launch_id: str) -> None:
        if self._registry.get(owner, launch_id).state != "uncertain":
            await self._mechanics.retire(owner, launch_id)
        await self._settle(owner, launch_id)

    def _detach(self, launch_id: str) -> None:
        """An uncertain launch keeps no live effect: no watchdog, no credentials."""
        live = self._live.get(launch_id)
        if live is None:
            return
        if live.watchdog and live.watchdog is not asyncio.current_task():
            live.watchdog.cancel()
        live.source.clear()
        live.binding = None

    @staticmethod
    def _unknown(row: CommissioningLaunchSnapshot) -> list[str]:
        return [kind for kind in _KINDS if getattr(row.objects, kind + "_uid") is None]

    async def _resolve(self, owner: str, row: CommissioningLaunchSnapshot) -> bool:
        """Resolve unknown UIDs by exact name plus the guard's witness; never adopt."""
        if self._witness is None:
            return False
        o = row.objects
        resolved = False
        for kind in self._unknown(row):
            name = getattr(o, kind + "_name")
            witness = await self._witness.admitted(
                kind=kind,
                namespace=present(o.namespace),
                name=name,
                create_operation_id=row.binding.create_operation_id,
            )
            if witness is None:
                continue
            require(type(witness) is GuardCreateWitness and witness.kind == kind)
            doc = await self._kube.read(present(o.namespace), kind + "s", name)
            if doc is None:
                continue
            uid, version = object_identity(doc, namespace=present(o.namespace), name=name)
            if uid != witness.uid:
                continue
            try:
                # The registry refuses a witness of another create operation or guard.
                row = self._registry.record_witnessed_uid(
                    owner, row.launch_id, row.revision, witness, version
                )
            except RegistryConflict:
                continue
            resolved = True
        return resolved

    async def _settle(self, owner: str, launch_id: str) -> None:
        """Bring broker ownership to the local facts; never ahead of them.

        A retirement that stopped on an unknown UID is an ambiguous create: it
        becomes ``uncertain``, and only the guard's witness moves it on. One
        NotFound never retires it.
        """
        row = self._registry.get(owner, launch_id)
        if row.state == "retiring" and self._unknown(row):
            row = self._registry.mark_uncertain(owner, launch_id, row.revision)
        if row.state == "uncertain":
            self._detach(launch_id)
        if row.state == "uncertain" and await self._resolve(owner, row):
            row = self._registry.get(owner, launch_id)
            if row.state == "retiring" and not self._unknown(row):
                await self._mechanics.retire(owner, launch_id)
            row = self._registry.get(owner, launch_id)
        state = cast(OwnershipState, row.state)
        if state in {"retired", "retiring", "uncertain"}:
            await self._control.advance(launch_id, state, row.objects.pod_uid)

    async def _settle_loop(self) -> None:
        while not self._mechanics.closing:
            try:
                for launch_id in await self._control.unsettled():
                    row = self._registry.find(self._worker.principal_id, launch_id)
                    if row is None:
                        # Ownership without a local row: no create is attributable.
                        await self._control.advance(launch_id, "uncertain", None)
                    elif launch_id not in self._live or row.state in {
                        "retiring",
                        "uncertain",
                        "retired",
                    }:
                        await self._settle(self._worker.principal_id, launch_id)
            except Exception:  # noqa: BLE001 - durable ownership is retried, never assumed
                pass
            await asyncio.sleep(1)

    async def start_cleanup(self) -> None:
        require(self._settle_task is None)
        await self._mechanics.start_cleanup()
        self._settle_task = asyncio.create_task(
            self._settle_loop(), name="commissioning-launch-settle"
        )

    async def _shutdown(self, app: web.Application) -> None:
        # The shared shutdown retires every live launch; uncertain ones wait for
        # the guard's witness instead, from durable ownership after restart.
        for launch_id in tuple(self._live):
            if self._registry.get(self._worker.principal_id, launch_id).state == "uncertain":
                self._detach(launch_id)
                self._live.pop(launch_id)
        await self._mechanics.shutdown(app)
        if self._settle_task:
            self._settle_task.cancel()
            await asyncio.gather(self._settle_task, return_exceptions=True)
        for launch_id in tuple(self._live):
            try:
                await self._settle(self._worker.principal_id, launch_id)
            except Exception:  # noqa: BLE001 - the next control process resumes from ownership
                pass
