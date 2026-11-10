"""Closed mediator codecs. Parsed records never establish issued authority.

All records retain canonical private bytes and return fresh dictionaries. Original
source strings are preserved, not reconstructed. Authentication, registry ownership,
current broker proof and runtime qualification belong to the mediator/client.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from functools import wraps
from typing import TYPE_CHECKING, Any, ClassVar, ParamSpec, TypeVar

from aci_protocol import Event
from aci_protocol.events import READER_CONTEXT
from curie_protected_hooks.authority_records import (
    _CREDENTIAL,
    _GUARD,
    Manifest,
    Qualification,
    _scalar,
)
from curie_protected_hooks.worker_verification import Dispatchable

if TYPE_CHECKING:
    from curie_protected_hooks.artifact_measurement import ArtifactMeasurement

    from .protected_configuration import ProtectedExecutionConfig
    from .protected_kubernetes_projection import Projection

_METADATA_CAP = 16384
_LAUNCH_CAP = 2097152
_START_CAP = 262144
P = ParamSpec("P")
T = TypeVar("T")


class LaunchRecordInvalid(ValueError):
    """Fixed refusal, without payload, endpoint or credential diagnostics."""

    def __init__(self) -> None:
        super().__init__("Invalid protected launch record")


def safe_validation(error: type[ValueError]) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Detach the actual cause and context, including an ambient caller exception.

    Raising ``from None`` only hides context in formatting; it leaves sensitive
    JSON documents/paths reachable. Clear after raise and use bare re-raise so
    Python does not assign the ambient exception again.
    """

    def decorate(function: Callable[P, T]) -> Callable[P, T]:
        @wraps(function)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
            try:
                return function(*args, **kwargs)
            except (
                ValueError,
                TypeError,
                KeyError,
                AttributeError,
                OSError,
                OverflowError,
                RecursionError,
            ):
                try:
                    raise error() from None
                except ValueError as refusal:
                    refusal.__cause__ = None
                    refusal.__context__ = None
                    raise

        return wrapped

    return decorate


def _require(condition: bool) -> None:
    if not condition:
        raise LaunchRecordInvalid()


def _text(value: Any, maximum: int = 2048, *, empty: bool = False) -> None:
    _require(
        type(value) is str
        and (empty or bool(value))
        and len(value.encode("utf-8", errors="strict")) <= maximum
        and "\x00" not in value
    )


def _pattern(value: Any, pattern: str) -> None:
    _require(type(value) is str and re.fullmatch(pattern, value, re.ASCII) is not None)


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, value in pairs:
        _require(name not in result)
        result[name] = value
    return result


def _reject_constant(_value: str) -> Any:
    raise LaunchRecordInvalid()


def canonical_json(value: Any) -> bytes:
    """Canonical ASCII encoding; this is encoding, not permission."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")


def _decode(raw: bytes, maximum: int) -> Any:
    _require(type(raw) is bytes and 0 < len(raw) <= maximum)
    return json.loads(
        raw.decode("utf-8", errors="strict"),
        object_pairs_hook=_pairs,
        parse_constant=_reject_constant,
    )


def _timestamp(value: Any) -> None:
    _text(value, 64)
    _pattern(
        value,
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})",
    )
    _require(datetime.fromisoformat(value.replace("Z", "+00:00")).tzinfo is not None)


def _epoch(value: Any) -> None:
    # Existing runner server's bounded base64url spelling; no padding.
    _pattern(value, r"[A-Za-z0-9_-]{32,256}")


def _platform(value: Any) -> None:
    from curie_protected_hooks.artifact_measurement import _mapping

    _mapping(value)


def _event(value: Any) -> None:
    _require(type(value) is dict)
    # Existing ACI reader policy; the surrounding mediator authority stays closed.
    Event.model_validate_json(canonical_json(value), strict=True, context=READER_CONTEXT)
    _require(len(canonical_json(value)) <= _START_CAP)


def _constraint(value: Any, constraint: Any) -> None:
    if type(constraint) is dict:
        _validate_object(value, constraint)
    elif type(constraint) is tuple:
        _require(any(type(value) is type(item) and value == item for item in constraint))
    elif callable(constraint):
        constraint(value)
    elif constraint == "text":
        _text(value)
    elif constraint == "dns":
        _pattern(value, r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
    elif constraint == "epoch":
        _epoch(value)
    elif constraint == "execution":
        _pattern(value, r"protected:[0-9a-f-]{36}:[1-9][0-9]{0,18}:[0-9a-f]{64}")
        _, runtime, generation, _ = value.split(":")
        _scalar(runtime, "uuid")
        _scalar(generation, "generation")
    elif constraint == "stream":
        _pattern(value, r"(0|[1-9][0-9]{0,19})-(0|[1-9][0-9]{0,19})")
        _require(value != "0-0" and all(int(part) <= 2**64 - 1 for part in value.split("-")))
    elif constraint == "timestamp":
        _timestamp(value)
    elif constraint == "bool":
        _require(type(value) is bool)
    elif constraint == "capability":
        _pattern(value, r"[A-Za-z0-9_-]{43}")
        decoded = base64.urlsafe_b64decode(value + "=")
        _require(
            len(decoded) == 32
            and base64.urlsafe_b64encode(decoded).decode("ascii").rstrip("=") == value
        )
    else:
        _scalar(value, constraint)


def _validate_object(value: Any, schema: dict[str, Any]) -> None:
    _require(type(value) is dict and value.keys() == schema.keys())
    for key, constraint in schema.items():
        _constraint(value[key], constraint)


def _source_fields(value: Any) -> None:
    _require(type(value) is dict and value.keys() == {"payload", "protected_envelope"})
    _text(value["payload"], 262144)
    _text(value["protected_envelope"], 16384)


_REQUEST = {
    "schema_version": "version",
    "request_id": "uuid",
    "runtime_id": "uuid",
    "runtime_generation": "generation",
    "manifest_digest": "sha256",
    "event_id": "opaque_ref",
    "committed_stream_id": "stream",
    "payload_sha256": "sha256",
    "execution_session_key": "execution",
    "source_fields": _source_fields,
}
_WORKLOAD = {
    "namespace": "dns",
    "pod_name": "dns",
    "pod_uid": "uuid",
    "container_id": "text",
    "started_at": "timestamp",
    "node_uid": "uuid",
    "service_name": "dns",
    "service_uid": "uuid",
}
_ASSIGNMENT = {
    "secret_name": "dns",
    "secret_uid": "uuid",
    "secret_resource_version": "text",
    "immutable": (True,),
    "runner_token_key": ("runner-token",),
    "runner_token_sha256": "sha256",
    "history_token_key": ("history-token",),
    "history_binding_sha256": "sha256",
    "history_signing_credential_ref": _CREDENTIAL,
    "history_expires_at_ms": "millisecond",
    "model_key": ("model-credential",),
    "model_credential_ref": _CREDENTIAL,
}
_RECEIPT = {
    "schema_version": "version",
    "launch_id": "uuid",
    "authority_domain_id": "uuid",
    "runtime_id": "uuid",
    "runtime_generation": "generation",
    "manifest_digest": "sha256",
    "qualification_id": "uuid",
    "qualification_generation": "generation",
    "execution_config_digest": "sha256",
    "measurement_ref": "oci_digest",
    "launch_identity": "opaque_ref",
    "projection_sha256": "sha256",
    "guard_identity": _GUARD,
    "event_id": "opaque_ref",
    "committed_stream_id": "stream",
    "payload_sha256": "sha256",
    "execution_session_key": "execution",
    "workload": _WORKLOAD,
    "assignment": _ASSIGNMENT,
    "platform": _platform,
    "route": {"kind": ("mediator-v1",), "launch_id": "uuid", "worker_principal_id": "opaque_ref"},
}


def _receipt_or_null(value: Any) -> None:
    if value is not None:
        _validate_object(value, _RECEIPT)
        _receipt_crosscheck(value)


def _receipt_crosscheck(value: dict[str, Any]) -> None:
    _require(value["route"]["launch_id"] == value["launch_id"])
    _require(
        value["assignment"]["model_credential_ref"]
        != value["assignment"]["history_signing_credential_ref"]
    )
    _execution_crosscheck(value)


def _execution_crosscheck(value: dict[str, Any]) -> None:
    _, runtime, generation, _ = value["execution_session_key"].split(":")
    _require(runtime == value["runtime_id"] and generation == value["runtime_generation"])


@dataclass(frozen=True, slots=True, repr=False)
class _Record:
    _canonical_bytes: bytes = field(repr=False)
    _schema: ClassVar[dict[str, Any]]
    _maximum: ClassVar[int] = _METADATA_CAP

    @safe_validation(LaunchRecordInvalid)
    def __post_init__(self) -> None:
        value = _decode(self._canonical_bytes, self._maximum)
        _validate_object(value, self._schema)
        if self._schema is _REQUEST:
            _execution_crosscheck(value)
        if self._schema is _RECEIPT:
            _receipt_crosscheck(value)
        if "receipt" in self._schema and value["receipt"] is not None:
            _receipt_crosscheck(value["receipt"])
            _require(value["launch_id"] == value["receipt"]["launch_id"])
        raw = canonical_json(value)
        _require(len(raw) <= self._maximum)
        object.__setattr__(self, "_canonical_bytes", raw)

    def __repr__(self) -> str:
        return f"<{type(self).__name__}>"

    @property
    def canonical_bytes(self) -> bytes:
        return self._canonical_bytes

    @property
    def digest(self) -> str:
        return hashlib.sha256(self._canonical_bytes).hexdigest()

    def as_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = json.loads(self._canonical_bytes)
        return value


class LaunchRequest(_Record):
    __slots__ = ()
    _schema = _REQUEST
    _maximum = _LAUNCH_CAP

    @property
    def source_fields(self) -> tuple[tuple[str, str], ...]:
        return tuple(sorted(self.as_dict()["source_fields"].items()))


class LaunchReceipt(_Record):
    __slots__ = ()
    _schema = _RECEIPT


class LaunchSuccess(_Record):
    __slots__ = ()
    _schema = {
        "schema_version": "version",
        "launch_id": "uuid",
        "receipt_revision": "generation",
        "state": ("idle",),
        "receipt": _RECEIPT,
        "control_capability": "capability",
    }


class LaunchPendingSuccess(_Record):
    """Structural preparing response; its capability permits observation/stop only."""

    __slots__ = ()
    _schema = {
        "schema_version": "version",
        "launch_id": "uuid",
        "receipt_revision": "generation",
        "state": ("preparing",),
        "receipt": (None,),
        "control_capability": "capability",
    }


class LaunchObservation(_Record):
    __slots__ = ()
    _schema = {
        "schema_version": "version",
        "launch_id": "uuid",
        "receipt_revision": "generation",
        "state": ("preparing", "idle", "waiting", "running", "retiring", "retired", "failed"),
        "receipt": _receipt_or_null,
    }


class StartRequest(_Record):
    __slots__ = ()
    _schema = {
        "schema_version": "version",
        "operation_id": "uuid",
        "expected_receipt_revision": "generation",
        "capacity_admission": "bool",
        "event": _event,
    }
    _maximum = _START_CAP

    @property
    def event(self) -> Event:
        return Event.model_validate_json(
            canonical_json(self.as_dict()["event"]), strict=True, context=READER_CONTEXT
        )


class AdmitRequest(_Record):
    __slots__ = ()
    _schema = {
        "schema_version": "version",
        "operation_id": "uuid",
        "expected_receipt_revision": "generation",
        "epoch": "epoch",
        "allow": "bool",
    }


class EpochRequest(_Record):
    __slots__ = ()
    _schema = {"schema_version": "version", "operation_id": "uuid", "epoch": "epoch"}


class RetireRequest(_Record):
    __slots__ = ()
    _schema = {"schema_version": "version", "operation_id": "uuid"}


class AppliedResponse(_Record):
    __slots__ = ()
    _schema = {
        "schema_version": "version",
        "launch_id": "uuid",
        "operation_id": "uuid",
        "epoch": "epoch",
        "result": ("applied",),
    }


class RetireResponse(_Record):
    __slots__ = ()
    _schema = {
        "schema_version": "version",
        "launch_id": "uuid",
        "operation_id": "uuid",
        "state": ("retiring", "retired"),
    }


class LaunchErrorResponse(_Record):
    __slots__ = ()
    _schema = {
        "schema_version": "version",
        "code": ("invalid", "unauthorized", "conflict", "unavailable", "retired"),
    }


def parse_launch_request(raw: bytes) -> LaunchRequest:
    return LaunchRequest(raw)


def parse_launch_receipt(raw: bytes) -> LaunchReceipt:
    return LaunchReceipt(raw)


def parse_launch_success(raw: bytes) -> LaunchSuccess:
    return LaunchSuccess(raw)


def parse_launch_pending_success(raw: bytes) -> LaunchPendingSuccess:
    return LaunchPendingSuccess(raw)


def parse_launch_observation(raw: bytes) -> LaunchObservation:
    return LaunchObservation(raw)


def parse_start_request(raw: bytes) -> StartRequest:
    return StartRequest(raw)


def parse_admit_request(raw: bytes) -> AdmitRequest:
    return AdmitRequest(raw)


def parse_epoch_request(raw: bytes) -> EpochRequest:
    return EpochRequest(raw)


def parse_retire_request(raw: bytes) -> RetireRequest:
    return RetireRequest(raw)


def parse_applied_response(raw: bytes) -> AppliedResponse:
    return AppliedResponse(raw)


def parse_retire_response(raw: bytes) -> RetireResponse:
    return RetireResponse(raw)


def parse_launch_error_response(raw: bytes) -> LaunchErrorResponse:
    return LaunchErrorResponse(raw)


@safe_validation(LaunchRecordInvalid)
def validate_launch_request_delivery(
    request: LaunchRequest, *, delivery: Dispatchable, manifest: Manifest
) -> None:
    """Compare to independently verified original bytes; do not run broker proof."""
    _require(
        type(request) is LaunchRequest
        and type(delivery) is Dispatchable
        and type(manifest) is Manifest
    )
    value, envelope, m = request.as_dict(), delivery.envelope.as_dict(), manifest.as_dict()
    _require(dict(request.source_fields) == dict(delivery.source_fields))
    _require(value["manifest_digest"] == manifest.digest == envelope["manifest_digest"])
    for key in (
        "runtime_id",
        "runtime_generation",
        "event_id",
        "payload_sha256",
        "execution_session_key",
    ):
        _require(value[key] == envelope[key])
    _require(
        value["runtime_id"] == m["runtime_id"]
        and value["runtime_generation"] == m["runtime_generation"]
    )
    for key in ("qualification_id", "runner_image_digest", "execution_config_digest"):
        _require(envelope[key] == m[key])
    _require(envelope["bundle_digest"] == m["bundle_digest"]["sha256"])
    _require(value["event_id"] == delivery.turn.event_id)
    _require(value["committed_stream_id"] == delivery.intent.as_dict()["reserved_stream_id"])
    _require(
        hashlib.sha256(value["source_fields"]["payload"].encode("utf-8")).hexdigest()
        == value["payload_sha256"]
    )


@safe_validation(LaunchRecordInvalid)
def validate_start_request_delivery(request: StartRequest, *, delivery: Dispatchable) -> None:
    """Compare the unchanged existing Event to its accepted QueuedTurn separately."""
    _require(type(request) is StartRequest and type(delivery) is Dispatchable)
    turn = delivery.turn
    expected = Event(
        type="job" if turn.source.is_job else "message",
        text=turn.text,
        user=turn.author,
        ts=turn.conversation_id,
        tool_access=turn.tool_access,
    )
    _require(request.event.model_dump(mode="json") == expected.model_dump(mode="json"))


@dataclass(frozen=True, slots=True, repr=False)
class WorkerLaunchCapability:
    """Private opaque operation target, never a persisted receipt or authority proof.

    Only the authenticated explicit client may select its provisioned server
    origin and create/retain this target after checking the server response.
    Construction alone cannot authorize a call.
    """

    origin: str
    launch_id: str
    worker_principal_id: str
    control_capability: str = field(repr=False)

    @safe_validation(LaunchRecordInvalid)
    def __post_init__(self) -> None:
        _text(self.origin)
        _scalar(self.launch_id, "uuid")
        _scalar(self.worker_principal_id, "opaque_ref")
        _constraint(self.control_capability, "capability")


@dataclass(frozen=True, slots=True, repr=False)
class IssuerPodBinding:
    """Issuer-private actual Pod/dial identities; no fabricated Sandbox owner."""

    namespace: str
    pod_name: str
    pod_uid: str
    service_name: str
    service_uid: str
    runner_url: str
    runner_token: str = field(repr=False)

    @safe_validation(LaunchRecordInvalid)
    def __post_init__(self) -> None:
        for name in (self.namespace, self.pod_name, self.service_name):
            _constraint(name, "dns")
        for uid in (self.pod_uid, self.service_uid):
            _scalar(uid, "uuid")
        _text(self.runner_url)
        _text(self.runner_token, 8192)


@dataclass(frozen=True, slots=True, repr=False)
class AuthenticatedLaunchObservation:
    """Client-origin bound DTO; copied Python objects never confer permission.

    The actual transport/registry/private target map remains separately required.
    """

    origin: str
    worker_principal_id: str
    response: LaunchObservation | LaunchSuccess | LaunchPendingSuccess

    @safe_validation(LaunchRecordInvalid)
    def __post_init__(self) -> None:
        _text(self.origin)
        _scalar(self.worker_principal_id, "opaque_ref")
        _require(type(self.response) in (LaunchObservation, LaunchSuccess, LaunchPendingSuccess))
        receipt = self.response.as_dict()["receipt"]
        if receipt is not None:
            _require(receipt["route"]["worker_principal_id"] == self.worker_principal_id)


@safe_validation(LaunchRecordInvalid)
def validate_launch_receipt_binding(
    receipt: LaunchReceipt,
    *,
    request: LaunchRequest,
    manifest: Manifest,
    qualification: Qualification,
    projection: Projection,
    execution: ProtectedExecutionConfig,
    measurement: ArtifactMeasurement,
    worker_principal_id: str,
) -> None:
    """Compare authenticated response data to selected immutable inputs only.

    This pure comparison is not the authentication, registry, Secret-assignment,
    control-plane observation or live qualification needed for positive calls.
    The explicit client must retain its separately authenticated private target.
    """
    from curie_protected_hooks.artifact_measurement import validate_artifact_selection

    from .protected_kubernetes_projection import validate_projection_binding

    _require(type(receipt) is LaunchReceipt and type(request) is LaunchRequest)
    _scalar(worker_principal_id, "opaque_ref")
    validate_artifact_selection(measurement, manifest, qualification)
    row = validate_projection_binding(
        projection, manifest=manifest, execution=execution, measurement=measurement
    )
    r, expected, m, q = (
        receipt.as_dict(),
        request.as_dict(),
        manifest.as_dict(),
        qualification.as_dict(),
    )
    for key in (
        "runtime_id",
        "runtime_generation",
        "manifest_digest",
        "event_id",
        "committed_stream_id",
        "payload_sha256",
        "execution_session_key",
    ):
        _require(r[key] == expected[key])
    _require(r["manifest_digest"] == manifest.digest)
    for key in (
        "runtime_id",
        "runtime_generation",
        "qualification_id",
        "execution_config_digest",
        "guard_identity",
    ):
        _require(r[key] == m[key])
    _require(r["qualification_generation"] == q["qualification_generation"])
    _require(r["measurement_ref"] == q["measurement_record_id"] == "sha256:" + measurement.digest)
    _require(r["projection_sha256"] == projection.digest)
    for key in ("authority_domain_id", "launch_identity"):
        _require(r[key] == m["substrate"][key])
    _require(r["platform"] == row)
    _require(r["workload"]["namespace"] == projection.as_dict()["namespace"])
    _require(r["route"]["worker_principal_id"] == worker_principal_id)
    for key in ("model_credential_ref", "history_signing_credential_ref"):
        _require(r["assignment"][key] == projection.as_dict()[key])
