"""Explicit pinned mTLS mediator client; parsed receipts are never authority.

The connector checks the peer after TLS handshake and before handing its transport
back to aiohttp's HTTP sender. Fresh connections avoid hidden GET replay. The
issuer independently rechecks every positive operation; this client also retains
private response ownership and reads current protected broker proof.
"""

from __future__ import annotations

import asyncio
import hashlib
import math
import re
import ssl
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from types import TracebackType
from typing import Any
from urllib.parse import urlsplit

import aiohttp
from aci_protocol import Event, SessionStatus
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from curie_protected_hooks.artifact_measurement import (
    ArtifactMeasurement,
    validate_artifact_selection,
)
from curie_protected_hooks.authority_records import Manifest, Qualification, _scalar
from curie_protected_hooks.worker_transport import AuthenticatedWorkerClient
from curie_protected_hooks.worker_verification import Dispatchable, verify_delivery

from .protected_binding import _CURRENT_DELIVERY
from .protected_configuration import ProtectedExecutionConfig
from .protected_kubernetes_projection import Projection, validate_projection_binding
from .protected_launch_records import (
    AdmitRequest,
    AppliedResponse,
    AuthenticatedLaunchObservation,
    EpochRequest,
    LaunchObservation,
    LaunchPendingSuccess,
    LaunchReceipt,
    LaunchRequest,
    LaunchSuccess,
    RetireRequest,
    RetireResponse,
    StartRequest,
    WorkerLaunchCapability,
    _decode,
    canonical_json,
    validate_launch_receipt_binding,
    validate_launch_request_delivery,
    validate_start_request_delivery,
)
from .protected_retirement import CleanupEvidence, OwnedPendingCleanupProof, OwnedRetirementProof
from .runner_client import RunnerError, TimeoutResult, TurnStream, _valid_turn_epoch


class LaunchClientError(RunnerError):
    """Fixed detached refusal; no server text, tokens, URL or source bytes."""

    def __init__(self) -> None:
        super().__init__("Protected launch request refused")


def _refuse() -> None:
    try:
        raise LaunchClientError() from None
    except LaunchClientError as error:
        error.__cause__ = None
        error.__context__ = None
        raise


def _budget(remaining_s: float | None, ceiling: float = 5.0) -> float:
    if remaining_s is None:
        return ceiling
    if not math.isfinite(remaining_s) or remaining_s <= 0:
        _refuse()
    return min(ceiling, remaining_s)


@dataclass(frozen=True, slots=True, repr=False)
class LaunchTLSConfig:
    origin: str
    ca_file: str
    server_name: str
    server_spki_sha256: str
    client_certificate: str
    client_key: str
    worker_principal_id: str

    def __post_init__(self) -> None:
        try:
            _scalar(self.worker_principal_id, "opaque_ref")
            url = urlsplit(self.origin)
            if (
                url.scheme != "https"
                or not url.hostname
                or url.username
                or url.password
                or url.path
                or url.query
                or url.fragment
                or not re.fullmatch(r"[0-9a-f]{64}", self.server_spki_sha256)
                or not self.server_name
                or any(c in self.server_name for c in "\r\n\x00")
                or not all((self.ca_file, self.client_certificate, self.client_key))
            ):
                _refuse()
        except Exception:  # noqa: BLE001 - detach untrusted transport/codec diagnostics
            _refuse()


class _PinnedConnector(aiohttp.TCPConnector):
    def __init__(self, context: ssl.SSLContext, pin: str) -> None:
        super().__init__(ssl=context, force_close=True, limit=64)
        self._pin = pin

    async def _wrap_create_connection(self, *args: Any, **kwargs: Any) -> Any:
        transport, protocol = await super()._wrap_create_connection(*args, **kwargs)
        try:
            peer = transport.get_extra_info("ssl_object")
            der = peer.getpeercert(binary_form=True) if peer else None
            if not der:
                _refuse()
            assert der is not None
            spki = (
                x509.load_der_x509_certificate(bytes(der))
                .public_key()
                .public_bytes(
                    serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
                )
            )
            if hashlib.sha256(spki).hexdigest() != self._pin:
                _refuse()
            return transport, protocol
        except BaseException:  # noqa: BLE001 - cancellation also closes owned transport
            transport.abort()
            raise


class _ProtectedTurnStream(TurnStream):
    """Existing ACI semantics plus bounded owned retirement on abandonment."""

    def __init__(
        self,
        response: aiohttp.ClientResponse,
        budget_s: float,
        deadline: float,
        turn_epoch: str,
        timeout_callback: Callable[[], Awaitable[TimeoutResult]] | None,
        retire_callback: Callable[[], Awaitable[None]],
    ) -> None:
        super().__init__(response, budget_s, deadline, turn_epoch, timeout_callback)
        self._retire_callback = retire_callback

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        try:
            await super().__aexit__(exc_type, exc, tb)
        finally:
            if exc_type is not None or not self._saw_final:
                await self._retire_callback()


class _LaunchTransport:
    def __init__(self, config: LaunchTLSConfig) -> None:
        try:
            if type(config) is not LaunchTLSConfig:
                _refuse()
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.minimum_version = ssl.TLSVersion.TLSv1_2
            context.load_verify_locations(cafile=config.ca_file)
            context.load_cert_chain(config.client_certificate, config.client_key)
            self._config = config
            self._session = aiohttp.ClientSession(
                connector=_PinnedConnector(context, config.server_spki_sha256),
                trust_env=False,
                auto_decompress=False,
                timeout=aiohttp.ClientTimeout(total=5, ceil_threshold=math.inf),
            )
            # aiohttp may replay idempotent requests on disconnected persistent
            # transports. This protocol explicitly forbids automatic replay.
            self._session._retry_connection = False
        except Exception:  # noqa: BLE001 - detach untrusted transport/codec diagnostics
            _refuse()

    async def request(
        self,
        method: str,
        path: str,
        body: bytes | None,
        headers: Mapping[str, str],
        remaining_s: float | None,
        *,
        stream_budget: float | None = None,
    ) -> aiohttp.ClientResponse:
        try:
            if not path.startswith("/v1/launches") or any(c in path for c in "\r\n#"):
                _refuse()
            limit = _budget(remaining_s)
            async with asyncio.timeout(limit):
                response = await self._session.request(
                    method,
                    self._config.origin + path,
                    data=body,
                    headers={"Content-Type": "application/json", **headers},
                    server_hostname=self._config.server_name,
                    allow_redirects=False,
                    proxy=None,
                    timeout=aiohttp.ClientTimeout(
                        total=stream_budget or limit,
                        connect=limit,
                        sock_read=stream_budget or limit,
                        ceil_threshold=math.inf,
                    ),
                )
            return response
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - detach untrusted transport/codec diagnostics
            _refuse()
        raise AssertionError("unreachable")

    async def control(
        self,
        method: str,
        path: str,
        body: bytes | None,
        headers: Mapping[str, str],
        remaining_s: float | None,
        *,
        maximum: int = 16384,
    ) -> tuple[int, bytes]:
        response = None
        try:
            async with asyncio.timeout(_budget(remaining_s)):
                response = await self.request(method, path, body, headers, remaining_s)
                if response.status not in {200, 201, 202} or response.headers.get(
                    "Content-Encoding"
                ):
                    _refuse()
                if response.content_length is not None and response.content_length > maximum:
                    _refuse()
                raw = bytearray()
                async for chunk in response.content.iter_chunked(16384):
                    raw.extend(chunk)
                    if len(raw) > maximum:
                        _refuse()
                _decode(bytes(raw), maximum)
                return response.status, bytes(raw)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - detach untrusted transport/codec diagnostics
            _refuse()
        finally:
            if response is not None:
                response.close()
        raise AssertionError("unreachable")

    async def close(self) -> None:
        await self._session.close()

    async def __aenter__(self) -> _LaunchTransport:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()


@dataclass(slots=True, repr=False)
class _OwnedLaunch:
    capability: WorkerLaunchCapability
    request: LaunchRequest
    delivery: Dispatchable
    observation: AuthenticatedLaunchObservation
    deadline: float
    epoch: str | None = None
    positive: bool = True
    retirement_proof: CleanupEvidence | None = None
    pending_identity: bytes | None = None


class ProtectedLaunchClient:
    """Own targets only after authenticated replies; never accept copied DTOs."""

    def __init__(
        self,
        tls: LaunchTLSConfig,
        *,
        broker: AuthenticatedWorkerClient,
        manifest: Manifest,
        qualification: Qualification,
        execution: ProtectedExecutionConfig,
        measurement: ArtifactMeasurement,
        projection: Projection,
    ) -> None:
        try:
            if type(broker) is not AuthenticatedWorkerClient:
                _refuse()
            validate_artifact_selection(measurement, manifest, qualification)
            validate_projection_binding(
                projection, manifest=manifest, execution=execution, measurement=measurement
            )
            self._tls, self._broker = tls, broker
            self._manifest, self._qualification = manifest, qualification
            self._execution, self._measurement, self._projection = (
                execution,
                measurement,
                projection,
            )
            self._transport = _LaunchTransport(tls)
            self._owned: dict[str, _OwnedLaunch] = {}
            # Retain uncertain submissions until client shutdown: an accepted
            # issuer effect without a registered response is not cleanup proof.
            self._creating: dict[int, Dispatchable] = {}
        except Exception:  # noqa: BLE001 - detach untrusted transport/codec diagnostics
            _refuse()

    def _target(self, capability: WorkerLaunchCapability) -> _OwnedLaunch:
        if type(capability) is not WorkerLaunchCapability:
            _refuse()
        owned = self._owned.get(capability.launch_id)
        if owned is None or owned.capability is not capability:
            _refuse()
        assert owned is not None
        return owned

    @staticmethod
    def _headers(capability: WorkerLaunchCapability) -> dict[str, str]:
        return {"X-Curie-Launch-Capability": capability.control_capability}

    async def _verify(self, delivery: Dispatchable, request: LaunchRequest) -> Dispatchable:
        try:
            context = _CURRENT_DELIVERY.get()
            if type(context) is not Dispatchable or context.source_fields != delivery.source_fields:
                _refuse()
            async with asyncio.timeout(5):
                current = await verify_delivery(
                    self._broker, dict(delivery.source_fields), trusted_manifest=self._manifest
                )
            if type(current) is not Dispatchable:
                _refuse()
            assert isinstance(current, Dispatchable)
            validate_launch_request_delivery(request, delivery=current, manifest=self._manifest)
            assert context is not None
            validate_launch_request_delivery(request, delivery=context, manifest=self._manifest)
            if current.intent.canonical_bytes != delivery.intent.canonical_bytes:
                _refuse()
            if (
                context.envelope.canonical_bytes != current.envelope.canonical_bytes
                or context.intent.canonical_bytes != current.intent.canonical_bytes
            ):
                _refuse()
            # Concrete parent guard checks actual lease/store fence, PEL owner,
            # broker generation and settlement absence. Missing composition
            # refuses; it is never replaced by a worker-provided true boolean.
            from . import protected_execution

            async with asyncio.timeout(5):
                await protected_execution.require_current_execution(self._broker, self._manifest)
            return current
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - detach untrusted transport/codec diagnostics
            _refuse()
        raise AssertionError("unreachable")

    def _receipt(self, owned: _OwnedLaunch, value: dict[str, Any]) -> None:
        if value["launch_id"] != owned.capability.launch_id or value["receipt"] is None:
            _refuse()
        prior = owned.observation.response.as_dict()
        if prior["receipt"] is not None and prior["receipt"] != value["receipt"]:
            _refuse()
        if int(value["receipt_revision"]) < int(prior["receipt_revision"]):
            _refuse()
        validate_launch_receipt_binding(
            LaunchReceipt(canonical_json(value["receipt"])),
            request=owned.request,
            manifest=self._manifest,
            qualification=self._qualification,
            projection=self._projection,
            execution=self._execution,
            measurement=self._measurement,
            worker_principal_id=self._tls.worker_principal_id,
        )

    async def create(
        self, request: LaunchRequest, *, delivery: Dispatchable, remaining_s: float
    ) -> WorkerLaunchCapability:
        owned = None
        submitted = False
        reserved = False
        try:
            if type(delivery) is not Dispatchable or id(delivery) in self._creating:
                _refuse()
            self._creating[id(delivery)] = delivery
            reserved = True
            budget = _budget(
                remaining_s,
                min(
                    120.0, float(self._execution.as_dict()["runner"]["execution_deadline_seconds"])
                ),
            )
            claim_deadline = asyncio.get_running_loop().time() + _budget(
                remaining_s,
                float(self._execution.as_dict()["runner"]["execution_deadline_seconds"]),
            )
            deadline = asyncio.get_running_loop().time() + budget
            async with asyncio.timeout_at(deadline):
                await self._verify(delivery, request)
                submitted = True
                status, raw = await self._transport.control(
                    "POST",
                    "/v1/launches",
                    request.canonical_bytes,
                    {},
                    deadline - asyncio.get_running_loop().time(),
                )
                response = LaunchPendingSuccess(raw) if status == 202 else LaunchSuccess(raw)
                value = response.as_dict()
                capability = WorkerLaunchCapability(
                    self._tls.origin,
                    value["launch_id"],
                    self._tls.worker_principal_id,
                    value["control_capability"],
                )
                if capability.launch_id in self._owned:
                    _refuse()
                observation = AuthenticatedLaunchObservation(
                    self._tls.origin, self._tls.worker_principal_id, response
                )
                owned = _OwnedLaunch(capability, request, delivery, observation, claim_deadline)
                if status == 202:
                    owned.pending_identity = canonical_json(
                        {key: value[key] for key in value if key != "control_capability"}
                    )
                self._owned[capability.launch_id] = owned
                if status != 202:
                    self._receipt(owned, value)
                    return capability
                for _ in range(120):
                    await asyncio.sleep(1)
                    observed = await self.observe(
                        capability, remaining_s=deadline - asyncio.get_running_loop().time()
                    )
                    value = observed.response.as_dict()
                    if value["state"] == "idle":
                        self._receipt(owned, value)
                        return capability
                    if value["state"] != "preparing" or value["receipt"] is not None:
                        _refuse()
                _refuse()
        except BaseException as error:  # noqa: BLE001 - cancellation retires owned target
            if owned is not None:
                owned.positive = False
                await self._retire_after_failure(owned.capability)
            if isinstance(error, asyncio.CancelledError):
                raise
            _refuse()
        finally:
            if reserved and (not submitted or owned is not None):
                self._creating.pop(id(delivery), None)
        raise AssertionError("unreachable")

    async def _retire_after_failure(self, capability: WorkerLaunchCapability) -> None:
        try:
            await asyncio.shield(self.retire(capability))
        except BaseException:  # noqa: BLE001 - cancellation also closes owned transport
            pass  # ownership remains retained; independent issuer deadline still applies

    async def observe(
        self, capability: WorkerLaunchCapability, *, remaining_s: float | None = None
    ) -> AuthenticatedLaunchObservation:
        owned = self._target(capability)
        try:
            status, raw = await self._transport.control(
                "GET",
                f"/v1/launches/{capability.launch_id}",
                None,
                self._headers(capability),
                remaining_s,
            )
            if status != 200:
                _refuse()
            response = LaunchObservation(raw)
            value = response.as_dict()
            if value["launch_id"] != capability.launch_id:
                _refuse()
            prior = owned.observation.response.as_dict()
            if int(value["receipt_revision"]) < int(prior["receipt_revision"]):
                _refuse()
            if prior["receipt"] is not None and value["receipt"] is None:
                _refuse()
            if value["receipt"] is not None:
                self._receipt(owned, value)
            observation = AuthenticatedLaunchObservation(
                self._tls.origin, self._tls.worker_principal_id, response
            )
            owned.observation = observation
            if value["state"] in {"retiring", "retired", "failed"}:
                owned.positive = False
            return observation
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - detach untrusted transport/codec diagnostics
            _refuse()
        raise AssertionError("unreachable")

    async def _positive(self, owned: _OwnedLaunch) -> Dispatchable:
        if not owned.positive or asyncio.get_running_loop().time() >= owned.deadline:
            _refuse()
        observation = await self.observe(
            owned.capability, remaining_s=owned.deadline - asyncio.get_running_loop().time()
        )
        self._receipt(owned, observation.response.as_dict())
        current = await self._verify(owned.delivery, owned.request)
        if asyncio.get_running_loop().time() >= owned.deadline:
            _refuse()
        return current

    def turn_deadline_s(self, remaining_s: float | None) -> float:
        return _budget(
            remaining_s, float(self._execution.as_dict()["runner"]["execution_deadline_seconds"])
        )

    async def start_turn(
        self,
        capability: WorkerLaunchCapability,
        event: Event,
        *,
        remaining_s: float | None = None,
        capacity_admission: bool = False,
    ) -> TurnStream:
        owned = self._target(capability)
        response = None
        try:
            current = await self._positive(owned)
            value = owned.observation.response.as_dict()
            if value["state"] != "idle" or owned.epoch is not None:
                _refuse()
            operation_id = str(uuid.uuid4())
            request = StartRequest(
                canonical_json(
                    {
                        "schema_version": 1,
                        "operation_id": operation_id,
                        "expected_receipt_revision": value["receipt_revision"],
                        "capacity_admission": capacity_admission,
                        "event": event.model_dump(mode="json"),
                    }
                )
            )
            validate_start_request_delivery(request, delivery=current)
            budget = min(
                self.turn_deadline_s(remaining_s),
                owned.deadline - asyncio.get_running_loop().time(),
            )
            _budget(budget)
            deadline = asyncio.get_running_loop().time() + budget
            response = await self._transport.request(
                "POST",
                f"/v1/launches/{capability.launch_id}/start",
                request.canonical_bytes,
                self._headers(capability),
                budget,
                stream_budget=budget,
            )
            epoch = response.headers.get("X-Curie-Turn-Epoch")
            if (
                response.status != 200
                or response.headers.get("X-Curie-Launch-Id") != capability.launch_id
                or response.headers.get("X-Curie-Operation-Id") != operation_id
                or not _valid_turn_epoch(epoch)
            ):
                _refuse()
            owned.epoch = epoch

            async def timeout() -> TimeoutResult:
                assert epoch is not None
                return await self.timeout_turn(capability, epoch)

            async def abandon() -> None:
                owned.positive = False
                await self._retire_after_failure(capability)

            assert epoch is not None
            return _ProtectedTurnStream(response, budget, deadline, epoch, timeout, abandon)
        except BaseException as error:  # noqa: BLE001 - cancellation retires owned target
            if response is not None:
                response.close()
            owned.positive = False
            await self._retire_after_failure(capability)
            if isinstance(error, asyncio.CancelledError):
                raise
            _refuse()
        raise AssertionError("unreachable")

    async def _applied(
        self,
        capability: WorkerLaunchCapability,
        route: str,
        request: AdmitRequest | EpochRequest,
        remaining_s: float | None = None,
    ) -> None:
        owned = self._target(capability)
        value = request.as_dict()
        if owned.epoch is None or value["epoch"] != owned.epoch:
            _refuse()
        positive = type(request) is AdmitRequest and value["allow"] is True
        deadline = None
        if positive:
            now = asyncio.get_running_loop().time()
            deadline = min(owned.deadline, now + _budget(remaining_s))
            remaining_s = _budget(deadline - now)
        async with asyncio.timeout_at(deadline):
            status, raw = await self._transport.control(
                "POST",
                f"/v1/launches/{capability.launch_id}/{route}",
                request.canonical_bytes,
                self._headers(capability),
                remaining_s,
            )
        result = AppliedResponse(raw).as_dict()
        if deadline is not None:
            _budget(deadline - asyncio.get_running_loop().time())
        if status != 200 or any(
            result[key] != expected
            for key, expected in (
                ("launch_id", capability.launch_id),
                ("operation_id", value["operation_id"]),
                ("epoch", owned.epoch),
            )
        ):
            _refuse()

    async def admit_turn(
        self,
        capability: WorkerLaunchCapability,
        turn_epoch: str,
        *,
        allow: bool,
        remaining_s: float | None = None,
    ) -> None:
        owned = self._target(capability)
        try:
            deadline = None
            if allow:
                now = asyncio.get_running_loop().time()
                deadline = owned.deadline
                if remaining_s is not None:
                    deadline = min(deadline, now + _budget(remaining_s, math.inf))
                _budget(deadline - now)
            async with asyncio.timeout_at(deadline):
                if allow:
                    await self._positive(owned)
                    if owned.observation.response.as_dict()["state"] != "waiting":
                        _refuse()
                    status = await self.capacity_status(
                        capability,
                        epoch=turn_epoch,
                        remaining_s=owned.deadline - asyncio.get_running_loop().time(),
                    )
                    if (
                        status.get("turn_epoch") != turn_epoch
                        or status.get("turn_active") is not True
                        or status.get("capacity_admission") is not True
                        or status.get("capacity_admission_result") != "pending"
                    ):
                        _refuse()
                    await self._verify(owned.delivery, owned.request)
                request = AdmitRequest(
                    canonical_json(
                        {
                            "schema_version": 1,
                            "operation_id": str(uuid.uuid4()),
                            "expected_receipt_revision": owned.observation.response.as_dict()[
                                "receipt_revision"
                            ],
                            "epoch": turn_epoch,
                            "allow": allow,
                        }
                    )
                )
                if deadline is not None:
                    remaining_s = _budget(deadline - asyncio.get_running_loop().time())
                await self._applied(capability, "admit", request, remaining_s)
                if deadline is not None:
                    _budget(deadline - asyncio.get_running_loop().time())
        except asyncio.CancelledError:
            # Cancellation can hide an accepted positive or stop operation.
            # Fence first and retain ownership even when cleanup is incomplete.
            owned.positive = False
            await self._retire_after_failure(capability)
            raise
        except Exception:  # noqa: BLE001 - detach untrusted transport/codec diagnostics
            if allow:
                owned.positive = False
                await self._retire_after_failure(capability)
            _refuse()

    async def interrupt(self, capability: WorkerLaunchCapability) -> None:
        owned = self._target(capability)
        if owned.epoch is None:
            await self.retire(capability)
            return
        await self._applied(
            capability,
            "interrupt",
            EpochRequest(
                canonical_json(
                    {"schema_version": 1, "operation_id": str(uuid.uuid4()), "epoch": owned.epoch}
                )
            ),
        )

    async def timeout_turn(
        self, capability: WorkerLaunchCapability, turn_epoch: str
    ) -> TimeoutResult:
        try:
            await self._applied(
                capability,
                "timeout",
                EpochRequest(
                    canonical_json(
                        {
                            "schema_version": 1,
                            "operation_id": str(uuid.uuid4()),
                            "epoch": turn_epoch,
                        }
                    )
                ),
            )
            return "accepted"
        except Exception:  # noqa: BLE001 - detach untrusted transport/codec diagnostics
            return "unconfirmed"

    def is_owned_retirement(self, proof: CleanupEvidence, *, delivery: Dispatchable | None) -> bool:
        """Copied records cannot authorize completion or accepted-slot release."""
        if (
            type(proof) not in (OwnedRetirementProof, OwnedPendingCleanupProof)
            or type(delivery) is not Dispatchable
        ):
            return False
        if self._creating.get(id(delivery)) is delivery:
            return False
        matches = [owned for owned in self._owned.values() if owned.delivery is delivery]
        if not matches or any(owned.retirement_proof is None for owned in matches):
            return False
        latest = matches[-1]
        if latest.retirement_proof is not proof:
            return False
        try:
            validate_launch_request_delivery(
                latest.request, delivery=delivery, manifest=self._manifest
            )
        except ValueError:
            return False
        return True

    def owned_retirement_for_delivery(
        self, delivery: Dispatchable | None
    ) -> CleanupEvidence | None:
        """Retain cleanup evidence across handle removal and settlement retries."""
        if type(delivery) is not Dispatchable:
            return None
        if self._creating.get(id(delivery)) is delivery:
            return None
        matches = [owned for owned in self._owned.values() if owned.delivery is delivery]
        if not matches or any(owned.retirement_proof is None for owned in matches):
            return None
        proof = matches[-1].retirement_proof
        if proof is None or not self.is_owned_retirement(proof, delivery=delivery):
            return None
        return proof

    async def retire(self, capability: WorkerLaunchCapability) -> bool:
        owned = self._target(capability)
        owned.positive = False
        try:
            request = RetireRequest(
                canonical_json({"schema_version": 1, "operation_id": str(uuid.uuid4())})
            )
            status, raw = await self._transport.control(
                "POST",
                f"/v1/launches/{capability.launch_id}/retire",
                request.canonical_bytes,
                self._headers(capability),
                None,
            )
            value = RetireResponse(raw).as_dict()
            if (
                value["launch_id"] != capability.launch_id
                or value["operation_id"] != request.as_dict()["operation_id"]
            ):
                _refuse()
            if (status, value["state"]) not in {(200, "retired"), (202, "retiring")}:
                _refuse()
            receipt = owned.observation.response.as_dict()["receipt"]
            if status == 200 and receipt is not None and owned.retirement_proof is None:
                context = owned.request.as_dict()
                owned.retirement_proof = OwnedRetirementProof(
                    canonical_json(
                        {
                            "schema_version": 1,
                            "event_id": context["event_id"],
                            "execution_session_key": context["execution_session_key"],
                            "manifest_digest": self._manifest.digest,
                            "worker_principal_id": self._tls.worker_principal_id,
                            "issuer_spki_sha256": self._tls.server_spki_sha256,
                            "issued_receipt_sha256": LaunchReceipt(canonical_json(receipt)).digest,
                            "retire_response": value,
                        }
                    )
                )
            elif (
                status == 200
                and owned.pending_identity is not None
                and owned.retirement_proof is None
            ):
                context = owned.request.as_dict()
                owned.retirement_proof = OwnedPendingCleanupProof(
                    canonical_json(
                        {
                            "schema_version": 1,
                            "event_id": context["event_id"],
                            "execution_session_key": context["execution_session_key"],
                            "manifest_digest": self._manifest.digest,
                            "worker_principal_id": self._tls.worker_principal_id,
                            "issuer_spki_sha256": self._tls.server_spki_sha256,
                            "request_id": context["request_id"],
                            "request_sha256": owned.request.digest,
                            "pending_response_identity": _decode(owned.pending_identity, 16384),
                            "pending_response_sha256": hashlib.sha256(
                                owned.pending_identity
                            ).hexdigest(),
                            "retire_response": value,
                        }
                    )
                )
            return bool(value["state"] == "retired")
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - detach untrusted transport/codec diagnostics
            _refuse()
        raise AssertionError("unreachable")

    async def retire_verified(self, capability: WorkerLaunchCapability) -> CleanupEvidence | None:
        """Require actual issuer cleanup for a privately registered pending or ready launch."""
        owned = self._target(capability)
        if not await self.retire(capability):
            return None
        return owned.retirement_proof

    async def status(
        self,
        capability: WorkerLaunchCapability,
        *,
        epoch: str | None = None,
        remaining_s: float | None = None,
    ) -> dict[str, object]:
        owned = self._target(capability)
        try:
            route = "status"
            if epoch is not None:
                if epoch != owned.epoch or not _valid_turn_epoch(epoch):
                    _refuse()
                route = "capacity-status?epoch=" + epoch
            status, raw = await self._transport.control(
                "GET",
                f"/v1/launches/{capability.launch_id}/{route}",
                None,
                self._headers(capability),
                remaining_s,
                maximum=1048576,
            )
            value = _decode(raw, 1048576)
            if (
                status != 200
                or type(value) is not dict
                or set(value)
                != {"schema_version", "launch_id", "receipt_revision", "runner_status"}
                or type(value["schema_version"]) is not int
                or value["schema_version"] != 1
                or value["launch_id"] != capability.launch_id
                or type(value["receipt_revision"]) is not str
                or not re.fullmatch(r"[1-9][0-9]*", value["receipt_revision"])
                or type(value["runner_status"]) is not dict
            ):
                _refuse()
            runner = value["runner_status"]
            if (
                runner.get("status") not in {status.value for status in SessionStatus}
                or any(
                    type(runner.get(name)) is not bool
                    for name in ("ready", "turn_active", "history_durable")
                )
                or type(runner.get("tool_access")) is not list
            ):
                _refuse()
            if epoch is not None and runner.get("turn_epoch") not in (None, epoch):
                _refuse()
            return dict(runner)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - detach untrusted transport/codec diagnostics
            _refuse()
        raise AssertionError("unreachable")

    async def capacity_status(
        self,
        capability: WorkerLaunchCapability,
        *,
        epoch: str | None = None,
        remaining_s: float | None = None,
    ) -> dict[str, object]:
        owned = self._target(capability)
        selected = epoch if epoch is not None else owned.epoch
        # The kernel checks capacity before starting a capacity-wait turn.
        # No owned epoch exists yet; observe the real general status only.
        # status() still rejects any supplied epoch that is not privately owned.
        return await self.status(capability, epoch=selected, remaining_s=remaining_s)

    async def steer(self, *_args: object, **_kwargs: object) -> None:
        _refuse()

    async def close(self) -> None:
        for owned in self._owned.values():
            await self._retire_after_failure(owned.capability)
        await self._transport.close()

    async def __aenter__(self) -> ProtectedLaunchClient:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()
