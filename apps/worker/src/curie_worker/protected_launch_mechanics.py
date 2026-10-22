"""Shared owned-workload launch mechanics behind a closed authorizer seam.

@spec PROTECTED-HOOK-LANE-7 PROTECTED-HOOK-LANE-8 PROTECTED-COMMISSIONING-8.
Create, observe, start, stop and retire effects on recorded Kubernetes objects
and their runner. The mechanics grant no launch authority: every positive
decision, receipt and settlement fence belongs to one closed authorizer.
"""

from __future__ import annotations

import asyncio
import base64
import hmac
import ipaddress
import re
import ssl
import time
from dataclasses import dataclass, field
from typing import Any, cast
from uuid import uuid4

import httpx
from aci_protocol import BootEnv, parse_ndjson_line
from aiohttp import web

from .commissioning_registry import CommissioningLaunchSnapshot
from .protected_kubernetes_projection import Projection, render_pod
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
from .protected_launch_records import IssuerPodBinding, canonical_json
from .protected_launch_registry import (
    CleanupReceipt,
    LaunchSnapshot,
    OperationKind,
)

EPOCH_HEADER = "X-Curie-Turn-Epoch"
CONTROL_DEADLINE = web.RequestKey("protected_launch_control_deadline", asyncio.Timeout)

# The closed authorizer set. A new authorizer is a reviewed change to this set
# and to ``_authorizer_classes``. Names only filter class definitions; the
# mechanics admit an authorizer by its exact class object.
_AUTHORIZERS = frozenset(
    {
        "curie_worker.protected_launch_server.ProductionLaunchAuthorizer",
        "curie_worker.commissioning_launch.CommissioningLaunchAuthorizer",
    }
)

# One owned launch row under either authorizer's bookkeeping.
LaunchRow = LaunchSnapshot | CommissioningLaunchSnapshot


def _authorizer_classes() -> tuple[type[LaunchAuthorizer], ...]:
    # Imported here because the authorizer modules import these mechanics.
    from .commissioning_launch import CommissioningLaunchAuthorizer
    from .protected_launch_server import ProductionLaunchAuthorizer

    return (ProductionLaunchAuthorizer, CommissioningLaunchAuthorizer)


class LaunchRefusal(Exception):
    def __init__(self, code: str, status: int) -> None:
        self.code, self.status = code, status
        super().__init__("Protected launch refused")


@dataclass(slots=True, repr=False)
class LiveLaunch:
    owner: str
    # Authorizer-owned launch authority; opaque to the mechanics.
    grant: Any
    projection: Projection
    platform: dict[str, Any]
    slots: dict[str, str]
    control_capability: str = field(repr=False)
    source: dict[str, str] = field(repr=False)
    expires_at_ms: int
    deadline: float
    # Authorizer-owned receipt and the runner start it was issued for.
    receipt: Any = None
    started_at: str | None = None
    binding: IssuerPodBinding | None = None
    job: asyncio.Task[None] | None = None
    watchdog: asyncio.Task[None] | None = None


class LaunchAuthorizer:
    """Closed seam: only the reviewed authorizers in ``_AUTHORIZERS`` may exist."""

    __slots__ = ()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # An authorizer is never subclassed: @final is static only.
        if cls.__bases__ != (LaunchAuthorizer,) or (
            cls.__module__ + "." + cls.__qualname__ not in _AUTHORIZERS
        ):
            raise TypeError("Protected launch authorizers are closed")

    def open_registry(self, root: str, *, boot_id: str) -> Any:
        """Open this authorizer's own launch bookkeeping for the mechanics."""
        raise NotImplementedError

    async def reverify(self, live: LiveLaunch) -> tuple[Any, Projection, dict[str, Any]]:
        """Re-verify authority; return evidence plus the re-measured projection."""
        raise NotImplementedError

    async def accept_reverification(self, live: LiveLaunch, evidence: Any) -> None:
        raise NotImplementedError

    async def require_unsettled(self, live: LiveLaunch) -> None:
        raise NotImplementedError

    def validate_start(self, parsed: Any, live: LiveLaunch) -> None:
        raise NotImplementedError

    def receipt(self, row: Any, live: LiveLaunch, started_at: str) -> Any:
        raise NotImplementedError


class ProtectedLaunchMechanics:
    """Owns registry, jobs, runner transport and mutexes for one closed authorizer."""

    def __init__(
        self,
        authorizer: LaunchAuthorizer,
        *,
        kubernetes: KubernetesLaunchAPI,
        registry_root: str,
        runner_scheme: str,
        runner_dns_suffix: str,
        runner_ca_file: str | None,
    ) -> None:
        require(
            type(authorizer) in _authorizer_classes() and type(kubernetes) is KubernetesLaunchAPI
        )
        self._authorizer, self._kube = authorizer, kubernetes
        self._runner_scheme, self._runner_dns_suffix = runner_scheme, runner_dns_suffix
        self._boot_id = str(uuid4())
        # ProtectedLaunchRegistry under production, CommissioningLaunchRegistry
        # under commissioning; the two never share files or rows.
        self._registry: Any = authorizer.open_registry(registry_root, boot_id=self._boot_id)
        self._live: dict[str, LiveLaunch] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._create_lock = asyncio.Lock()
        self._closing = False
        self._cleanup_task: asyncio.Task[None] | None = None
        verify: ssl.SSLContext | bool = True
        if runner_ca_file:
            verify = ssl.create_default_context(cafile=runner_ca_file)
        self._runner = httpx.AsyncClient(
            verify=verify, trust_env=False, follow_redirects=False, timeout=5
        )

    @property
    def authorizer(self) -> LaunchAuthorizer:
        return self._authorizer

    @property
    def boot_id(self) -> str:
        return self._boot_id

    @property
    def registry(self) -> Any:
        return self._registry

    @property
    def live(self) -> dict[str, LiveLaunch]:
        return self._live

    @property
    def create_lock(self) -> asyncio.Lock:
        return self._create_lock

    @property
    def closing(self) -> bool:
        return self._closing

    def mutex(self, launch_id: str) -> asyncio.Lock:
        return self._locks.setdefault(launch_id, asyncio.Lock())

    def _row(self, owner: str, launch_id: str) -> LaunchRow:
        row: LaunchRow = self._registry.get(owner, launch_id)
        return row

    async def prepare(self, launch_id: str) -> None:
        live = self._live[launch_id]
        owner = live.owner
        try:
            async with asyncio.timeout_at(live.deadline):
                for kind in ("secrets", "services", "pods"):
                    async with self.mutex(launch_id):
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
                    async with self.mutex(launch_id):
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
                            status = await self.runner_status(binding)
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
                            live.receipt = self._authorizer.receipt(
                                row, live, observation.started_at
                            )
                            live.started_at = observation.started_at
                            self._registry.transition(owner, launch_id, row.revision, "idle")
                            live.watchdog = asyncio.create_task(
                                self._expire(launch_id), name="protected-launch-deadline"
                            )
                            return
                    await asyncio.sleep(1)
        except (Exception, asyncio.CancelledError):  # noqa: BLE001 - owned cleanup on all failures
            async with self.mutex(launch_id):
                row = self._registry.get(owner, launch_id)
                if row.state not in {"retiring", "retired"}:
                    self._registry.transition(owner, launch_id, row.revision, "retiring")
            # Never retry a create; owned recorded-name reconciliation only.
            await self.retire(owner, launch_id, cancel_job=False)

    def _runner_origin(self, service: str, namespace: str) -> str:
        return f"{self._runner_scheme}://{service}.{namespace}.{self._runner_dns_suffix}:8080"

    def _pod_origin(self, pod_ip: str) -> str:
        ip = ipaddress.ip_address(pod_ip)
        host = f"[{ip}]" if ip.version == 6 else str(ip)
        return f"{self._runner_scheme}://{host}:8080"

    async def _expire(self, launch_id: str) -> None:
        live = self._live[launch_id]
        await asyncio.sleep(max(0, (live.expires_at_ms - int(time.time() * 1000)) / 1000))
        await self.retire(live.owner, launch_id)

    async def runner_status(
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
        status: dict[str, Any], live: LiveLaunch, row: LaunchRow, *, idle: bool
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

    async def _positive(self, row: LaunchRow, *, idle: bool) -> LiveLaunch:
        require(row.boot_id == self._boot_id and row.state == ("idle" if idle else "waiting"))
        live = self._live[row.launch_id]
        require(
            live.receipt is not None
            and live.binding is not None
            and live.expires_at_ms > int(time.time() * 1000)
        )
        evidence, projection, platform = await self._authorizer.reverify(live)
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
        require(observation.started_at == present(live.started_at))
        require(present(live.binding).runner_url == self._pod_origin(observation.pod_ip))
        self._check_status(
            await self.runner_status(
                present(live.binding), epoch=None if idle else row.owned_epoch
            ),
            live,
            row,
            idle=idle,
        )
        await self._authorizer.accept_reverification(live, evidence)
        return live

    async def start(
        self,
        request: web.Request,
        owner: str,
        row: LaunchRow,
        parsed: Any,
        request_digest: str,
    ) -> web.StreamResponse:
        launch_id, value = row.launch_id, parsed.as_dict()
        # Initial streaming acceptance stays inside the five-second HTTP deadline.
        # The stream itself has the independent execution deadline below.
        async with self.mutex(launch_id):
            row = self._row(owner, launch_id)
            live = await self._positive(row, idle=True)
            self._authorizer.validate_start(parsed, live)
            reservation = self._registry.begin_operation(
                owner,
                launch_id,
                value["expected_receipt_revision"],
                value["operation_id"],
                "start",
                request_digest,
            )
            if reservation.disposition != "new":
                raise LaunchRefusal("conflict", 409)
            binding = present(live.binding)
            require(binding is not None)
            headers = {"Authorization": "Bearer " + binding.runner_token}
            if value["capacity_admission"]:
                headers["X-Curie-Capacity-Admission"] = "wait"
            upstream = None
            try:
                await self._authorizer.require_unsettled(live)
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
                current = self._row(owner, launch_id)
                self._registry.finish_operation(
                    owner,
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
                current = self._row(owner, launch_id)
                self._registry.finish_operation(
                    owner,
                    launch_id,
                    current.revision,
                    value["operation_id"],
                    "uncertain",
                )
                asyncio.create_task(self.retire(owner, launch_id))
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
                request[CONTROL_DEADLINE].reschedule(None)
            except BaseException:
                await upstream.aclose()
                asyncio.create_task(self.retire(owner, launch_id))
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
        live: LiveLaunch,
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
            asyncio.create_task(self.retire(live.owner, launch_id))
            if request.transport:
                request.transport.close()
        finally:
            await upstream.aclose()

    async def control(
        self,
        owner: str,
        row: LaunchRow,
        operation: str,
        parsed: Any,
        request_digest: str,
    ) -> dict[str, Any]:
        value, launch_id = parsed.as_dict(), row.launch_id
        kind = "deny" if operation == "admit" and not value["allow"] else operation
        async with self.mutex(launch_id):
            row = self._row(owner, launch_id)
            if kind == "admit":
                await self._positive(row, idle=False)
            require(row.owned_epoch == value["epoch"])
            reservation = self._registry.begin_operation(
                owner,
                launch_id,
                value.get("expected_receipt_revision", row.revision),
                value["operation_id"],
                cast(OperationKind, kind),
                request_digest,
                epoch=value["epoch"],
            )
            if reservation.disposition == "existing":
                if reservation.operation.status != "applied":
                    raise LaunchRefusal("conflict", 409)
            else:
                try:
                    binding = await self.binding_for_stop(row)
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
                    if kind == "admit":
                        live = self._live[launch_id]
                        await self._authorizer.require_unsettled(live)
                    await self._runner_control(binding, path, value["epoch"], body)
                    current = self._row(owner, launch_id)
                    self._registry.finish_operation(
                        owner,
                        launch_id,
                        current.revision,
                        value["operation_id"],
                        "applied",
                        epoch=value["epoch"],
                        state="running" if kind == "admit" else None,
                    )
                except BaseException:
                    current = self._row(owner, launch_id)
                    self._registry.finish_operation(
                        owner,
                        launch_id,
                        current.revision,
                        value["operation_id"],
                        "uncertain",
                    )
                    asyncio.create_task(self.retire(owner, launch_id))
                    raise
        return {
            "schema_version": 1,
            "launch_id": launch_id,
            "operation_id": value["operation_id"],
            "epoch": value["epoch"],
            "result": "applied",
        }

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

    async def binding_for_stop(self, row: LaunchRow) -> IssuerPodBinding:
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

    async def retire(self, owner: str, launch_id: str, *, cancel_job: bool = True) -> None:
        live = self._live.get(launch_id)
        async with self.mutex(launch_id):
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
        async with self.mutex(launch_id):
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
                            await self.binding_for_stop(row), "/v1/timeout", row.owned_epoch, None
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
                    await self.retire(row.worker_principal_id, row.launch_id)
                cursor = rows[-1].launch_id if rows else None
            except Exception:  # noqa: BLE001 - retain ownership and retry bounded cleanup
                cursor = None
            await asyncio.sleep(1)

    async def shutdown(self, _app: web.Application) -> None:
        self._closing = True
        if self._cleanup_task:
            self._cleanup_task.cancel()
            await asyncio.gather(self._cleanup_task, return_exceptions=True)
        for launch_id, live in tuple(self._live.items()):
            await self.retire(live.owner, launch_id)
        tasks = [live.watchdog for live in self._live.values() if live.watchdog]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def close(self, _app: web.Application) -> None:
        await self._runner.aclose()
        await self._kube.close()
        self._registry.close()


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
