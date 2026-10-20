"""Closed commissioning campaign records, @spec PROTECTED-COMMISSIONING-7.

These are inert codecs and one pure ownership transition. A decoded record
authenticates nothing, observes nothing and authorizes no enqueue, claim,
provider request or workload start. No production reader decodes them.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, ClassVar, NoReturn

from .authority_records import (
    _GUARD,
    _MAX_BYTES,
    _MAX_MILLISECOND,
    _decimal,
    _object,
    _pairs,
    _reject_number,
    _scalar,
)

# @spec PROTECTED-COMMISSIONING-7
_CAMPAIGN: dict[str, Any] = {
    "campaign_id": "uuid",
    "campaign_generation": "generation",
    "permit_digest": "sha256",
    "bootstrap_digest": "sha256",
    "manifest_digest": "sha256",
    "runtime_id": "uuid",
    "runtime_generation": "generation",
    "broker_run_id": "run_id",
}
_DELIVERY: dict[str, Any] = {
    "delivery_id": "uuid",
    "attempt_id": "uuid",
    "reservation_id": "uuid",
    "request_digest": "sha256",
    "payload_digest": "sha256",
    "payload_bytes": "generation",
    "source": {"source_id": "uuid", "generation": "generation", "policy_fingerprint": "sha256"},
    "target_ref": "opaque_ref",
}
_MAX_PAYLOAD_BYTES = 32768
_DNS_LABEL = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
_STATES = ("creating", "bound", "retiring", "retired", "uncertain")
# Which workload tags each ownership state may carry.
_STATE_WORKLOADS = {
    "creating": ("unbound",),
    "bound": ("bound",),
    "retiring": ("unbound", "bound"),
    "retired": ("bound",),
    "uncertain": ("unbound", "bound"),
}
_TRANSITIONS = {
    "creating": ("bound", "retiring", "retired", "uncertain"),
    "bound": ("retiring", "uncertain"),
    "retiring": ("retired", "uncertain"),
    "retired": (),
    "uncertain": ("bound", "retiring", "retired"),
}


class CampaignRecordInvalid(ValueError):
    """Fixed refusal without record contents."""

    def __init__(self) -> None:
        """@spec PROTECTED-COMMISSIONING-7."""
        super().__init__("Invalid commissioning campaign record")


def _refuse() -> NoReturn:
    """Raise a detached refusal, even inside a caller's active exception."""
    error = CampaignRecordInvalid()
    try:
        raise error from None
    finally:
        error.__context__ = None


def _require(condition: bool) -> None:
    if not condition:
        _refuse()


def _label(value: Any) -> None:
    _require(type(value) is str and re.fullmatch(_DNS_LABEL, value, flags=re.ASCII) is not None)


def _stream_entry_id(value: Any) -> None:
    _require(type(value) is str and value.count("-") == 1)
    for component in value.split("-"):
        _decimal(component, _MAX_MILLISECOND, zero=True)


def _workload(value: Any) -> None:
    _require(type(value) is dict and value.get("kind") in ("unbound", "bound"))
    members = {"kind", "namespace", "pod_name", "create_operation_id"}
    _require(value.keys() == (members | {"pod_uid"} if value["kind"] == "bound" else members))
    _label(value["namespace"])
    _label(value["pod_name"])
    _scalar(value["create_operation_id"], "uuid")
    if value["kind"] == "bound":
        _scalar(value["pod_uid"], "uuid")


def _encode(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")


@dataclass(frozen=True, slots=True, repr=False)
class _CampaignRecord:
    """Immutable canonical bytes, @spec PROTECTED-COMMISSIONING-7."""

    _canonical_bytes: bytes
    _record_type: ClassVar[str]
    _schema: ClassVar[dict[str, Any]]
    _delivery: ClassVar[bool] = True

    def __post_init__(self) -> None:
        """Validate even direct construction, @spec PROTECTED-COMMISSIONING-7."""
        try:
            raw = self._canonical_bytes
            _require(type(raw) is bytes and len(raw) <= _MAX_BYTES)
            value = json.loads(
                raw.decode("utf-8"),
                object_pairs_hook=_pairs,
                parse_float=_reject_number,
                parse_constant=_reject_number,
            )
            _require(type(value) is dict and value.get("record_type") == self._record_type)
            schema: dict[str, Any] = {"schema_version": "version", "campaign": _CAMPAIGN}
            if self._delivery:
                schema["delivery"] = _DELIVERY
            schema |= self._schema
            special = {k for k, v in schema.items() if v is None}
            _require(value.keys() == schema.keys() | {"record_type"})
            _object(
                {k: value[k] for k in schema if k not in special},
                {k: v for k, v in schema.items() if k not in special},
            )
            if self._delivery:
                _require(int(value["delivery"]["payload_bytes"]) <= _MAX_PAYLOAD_BYTES)
            self._check(value)
            canonical = _encode(value)
            _require(len(canonical) <= _MAX_BYTES)
        except (ValueError, TypeError, OverflowError, RecursionError):
            _refuse()
        object.__setattr__(self, "_canonical_bytes", canonical)

    def _check(self, value: dict[str, Any]) -> None:
        """Record-specific members the scalar schema marks as ``None``."""

    def __repr__(self) -> str:
        return type(self).__name__ + "()"

    @property
    def canonical_bytes(self) -> bytes:
        """@spec PROTECTED-COMMISSIONING-7."""
        return self._canonical_bytes

    @property
    def digest(self) -> str:
        """sha256 of the canonical bytes, @spec PROTECTED-COMMISSIONING-7."""
        return hashlib.sha256(self._canonical_bytes).hexdigest()

    def as_dict(self) -> dict[str, Any]:
        """Fresh nested copy, @spec PROTECTED-COMMISSIONING-7."""
        value: dict[str, Any] = json.loads(self._canonical_bytes)
        return value

    @property
    def campaign(self) -> dict[str, Any]:
        """Fresh copy of campaign binding C."""
        value: dict[str, Any] = self.as_dict()["campaign"]
        return value


class _DeliveryRecord(_CampaignRecord):
    __slots__ = ()

    @property
    def delivery(self) -> dict[str, Any]:
        """Fresh copy of delivery binding D."""
        value: dict[str, Any] = self.as_dict()["delivery"]
        return value


class CampaignEnvelope(_DeliveryRecord):
    """@spec PROTECTED-COMMISSIONING-7."""

    __slots__ = ()
    _record_type = "commissioning_envelope"
    _schema: ClassVar[dict[str, Any]] = {"tool_access": None}

    def _check(self, value: dict[str, Any]) -> None:
        _require(value["tool_access"] == "read-only")

    @property
    def tool_access(self) -> str:
        """Always the literal ``read-only``."""
        value: str = self.as_dict()["tool_access"]
        return value


class CampaignAdmission(_DeliveryRecord):
    """@spec PROTECTED-COMMISSIONING-7."""

    __slots__ = ()
    _record_type = "commissioning_admission"
    _schema: ClassVar[dict[str, Any]] = {
        "envelope_digest": "sha256",
        "stream_entry_id": None,
        "accepted_at_ms": "millisecond",
    }

    def _check(self, value: dict[str, Any]) -> None:
        _stream_entry_id(value["stream_entry_id"])

    @property
    def stream_entry_id(self) -> str:
        """The committed campaign lane entry."""
        value: str = self.as_dict()["stream_entry_id"]
        return value


class CampaignLaunchRequest(_DeliveryRecord):
    """@spec PROTECTED-COMMISSIONING-7."""

    __slots__ = ()
    _record_type = "commissioning_launch_request"
    _schema: ClassVar[dict[str, Any]] = {
        "admission_digest": "sha256",
        "launch_id": "uuid",
        "launch_reservation_id": "uuid",
        "projection_digest": "sha256",
    }


class CampaignLaunchOwnership(_DeliveryRecord):
    """@spec PROTECTED-COMMISSIONING-7."""

    __slots__ = ()
    _record_type = "commissioning_launch_ownership"
    _schema: ClassVar[dict[str, Any]] = {
        "launch_id": "uuid",
        "launch_request_digest": "sha256",
        "projection_digest": "sha256",
        "workload": None,
        "state": None,
        "revision": "generation",
    }

    def _check(self, value: dict[str, Any]) -> None:
        _workload(value["workload"])
        _require(value["state"] in _STATES)
        _require(value["workload"]["kind"] in _STATE_WORKLOADS[value["state"]])

    @property
    def state(self) -> str:
        """One of the five ownership states."""
        value: str = self.as_dict()["state"]
        return value

    @property
    def workload(self) -> dict[str, Any]:
        """Fresh copy of workload binding W."""
        value: dict[str, Any] = self.as_dict()["workload"]
        return value


class CommissioningAcceptance(_CampaignRecord):
    """@spec PROTECTED-COMMISSIONING-7."""

    __slots__ = ()
    _record_type = "commissioning_acceptance"
    _delivery = False
    _schema: ClassVar[dict[str, Any]] = {
        "evidence_digest": "sha256",
        "qualification_id": "uuid",
        "qualification_generation": "generation",
        "accepted_at_ms": "millisecond",
    }

    def _check(self, value: dict[str, Any]) -> None:
        # A campaign ID never occupies a production qualification_id field.
        _require(value["qualification_id"] != value["campaign"]["campaign_id"])


def parse_campaign_envelope(raw: bytes) -> CampaignEnvelope:
    """@spec PROTECTED-COMMISSIONING-7."""
    return CampaignEnvelope(raw)


def parse_campaign_admission(raw: bytes) -> CampaignAdmission:
    """@spec PROTECTED-COMMISSIONING-7."""
    return CampaignAdmission(raw)


def parse_campaign_launch_request(raw: bytes) -> CampaignLaunchRequest:
    """@spec PROTECTED-COMMISSIONING-7."""
    return CampaignLaunchRequest(raw)


def parse_campaign_launch_ownership(raw: bytes) -> CampaignLaunchOwnership:
    """@spec PROTECTED-COMMISSIONING-7."""
    return CampaignLaunchOwnership(raw)


def parse_commissioning_acceptance(raw: bytes) -> CommissioningAcceptance:
    """@spec PROTECTED-COMMISSIONING-7."""
    return CommissioningAcceptance(raw)


@dataclass(frozen=True, slots=True, repr=False)
class UnambiguousNotFound:
    """Caller-supplied evidence that one exact created Pod no longer exists.

    It names the recorded namespace and Pod name, the UID the guard admitted
    for that create and the guard identity that observed it. Constructing it
    proves nothing; ``next_ownership`` only checks that it matches the record
    and the trusted guard. @spec PROTECTED-COMMISSIONING-7.
    """

    namespace: str
    pod_name: str
    pod_uid: str
    guard_identity: dict[str, Any]

    def __post_init__(self) -> None:
        try:
            _label(self.namespace)
            _label(self.pod_name)
            _scalar(self.pod_uid, "uuid")
            _object(self.guard_identity, _GUARD)
        except (ValueError, TypeError):
            _refuse()
        object.__setattr__(self, "guard_identity", dict(self.guard_identity))


def next_ownership(
    current: CampaignLaunchOwnership,
    proposed: CampaignLaunchOwnership,
    *,
    trusted_guard_identity: dict[str, Any],
    not_found: UnambiguousNotFound | None = None,
) -> CampaignLaunchOwnership:
    """Return ``proposed`` only if it is a permitted next ownership record.

    Pure and side-effect free. The launch, campaign, delivery and workload
    identities stay fixed, the revision advances by one, a bound UID never
    changes or unbinds, and ``retired`` is terminal. Every transition into
    ``retired``, from a bound or an unbound workload, needs an unambiguous
    NotFound matching the recorded name, the retired UID and the trusted guard,
    so no uncertain ownership reaches acceptance unobserved.
    @spec PROTECTED-COMMISSIONING-7.
    """
    _require(type(current) is CampaignLaunchOwnership)
    _require(type(proposed) is CampaignLaunchOwnership)
    try:
        _object(trusted_guard_identity, _GUARD)
    except (ValueError, TypeError):
        _refuse()
    before, after = current.as_dict(), proposed.as_dict()
    for member in (
        "campaign",
        "delivery",
        "launch_id",
        "launch_request_digest",
        "projection_digest",
    ):
        _require(before[member] == after[member])
    _require(int(after["revision"]) == int(before["revision"]) + 1)
    _require(after["state"] in _TRANSITIONS[before["state"]])
    old, new = before["workload"], after["workload"]
    for member in ("namespace", "pod_name", "create_operation_id"):
        _require(old[member] == new[member])
    if old["kind"] == "bound":
        _require(new["kind"] == "bound" and new["pod_uid"] == old["pod_uid"])
    if after["state"] == "retired":
        if type(not_found) is not UnambiguousNotFound:
            _refuse()
        _require(
            not_found.namespace == old["namespace"]
            and not_found.pod_name == old["pod_name"]
            and not_found.pod_uid == new["pod_uid"]
            and not_found.guard_identity == trusted_guard_identity
        )
    return proposed


def blocks_acceptance(ownership: object) -> bool:
    """Whether this launch ownership blocks campaign acceptance.

    Only a ``retired`` ownership record leaves acceptance unblocked; uncertain,
    live and unknown ownership all block. @spec PROTECTED-COMMISSIONING-7.
    """
    return not (type(ownership) is CampaignLaunchOwnership and ownership.state == "retired")
