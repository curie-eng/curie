"""Closed admission records, @spec PROTECTED-HOOK-ADMISSION-1/2/3."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, ClassVar

from aci_protocol.ndjson import parse_queued_turn
from aci_protocol.turn import TurnSource

from .authority_records import _scalar
from .source_policy_records import canonical_hook, canonical_uuid, policy_fingerprint

# @spec PROTECTED-HOOK-ADMISSION-2/3
_MAX_METADATA = 16384
_MAX_PAYLOAD = 262144
_POLICY_FIELDS = frozenset(
    {
        "agent_id",
        "hook",
        "generation",
        "operation_id",
        "legacy_generation",
        "mode",
        "tool_access",
        "runtime_id",
        "qualification_id",
        "bundle_digest",
    }
)
_IDENTITY = {"agent_id": "uuid", "hook": "hook", "delivery_id": "logical"}
_ENVELOPE = {
    "schema_version": "version",
    "event_id": "opaque_ref",
    "source_revision": "generation",
    "runtime_id": "uuid",
    "runtime_generation": "generation",
    "manifest_digest": "sha256",
    "qualification_id": "uuid",
    "runner_image_digest": "oci_digest",
    "bundle_digest": "sha256",
    "execution_config_digest": "sha256",
    "logical_conversation_key": "logical",
    "execution_session_key": "execution",
    "payload_sha256": "sha256",
}
_INTENT = {
    "schema_version": "version",
    "identity": "identity",
    "requested_tool_access": "requested",
    "effective_tool_access": "access",
    "request_body_sha256": "sha256",
    "source_generation": "generation",
    "source_operation_id": "uuid",
    "policy_fingerprint": "sha256",
    "manifest_digest": "sha256",
    "runtime_id": "uuid",
    "runtime_generation": "generation",
    "qualification_id": "uuid",
    "event_id": "opaque_ref",
    "conversation_id": "logical",
    "payload_sha256": "sha256",
    "envelope_sha256": "sha256",
    "reserved_stream_id": "stream",
    "created_at_ms": "millisecond",
    "deadline_ms": "millisecond",
}
_RECEIPT = {
    **{
        k: v
        for k, v in _INTENT.items()
        if k
        not in {
            "created_at_ms",
            "deadline_ms",
            "reserved_stream_id",
            "envelope_sha256",
        }
    },
    "stream_id": "stream",
    "acceptance_status": "accepted",
    "tool_access": "access",
}
_STATE = {
    "schema_version": "version",
    "status": "status",
    "recovery_attempts": "attempts",
    "reason": "terminal",
    "receipt": "receipt",
}
_TERMINAL = frozenset({"deadline", "attempts_exhausted", "stream_id_unappendable"})
_REFUSALS = frozenset(
    {
        "source_unavailable",
        "runtime_unavailable",
        "qualification_unavailable",
        "evidence_unavailable",
        "broker_identity_mismatch",
        "admission_closed",
        "quota_full",
    }
)


class AdmissionUnavailable(Exception):
    """@spec PROTECTED-HOOK-ADMISSION-1."""

    def __init__(self) -> None:
        """@spec PROTECTED-HOOK-ADMISSION-1."""
        super().__init__("protected admission unavailable")


def _require(condition: bool) -> None:
    """@spec PROTECTED-HOOK-ADMISSION-2/3."""
    if not condition:
        raise ValueError("invalid protected admission record")


def _encode(value: Any) -> bytes:
    """@spec PROTECTED-HOOK-ADMISSION-2/3 PROTECTED-HOOK-LANE-2."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """@spec PROTECTED-HOOK-ADMISSION-2 PROTECTED-HOOK-LANE-2."""
    output: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in output)
        output[key] = value
    return output


def _reject_number(value: str) -> Any:
    """@spec PROTECTED-HOOK-ADMISSION-2 PROTECTED-HOOK-LANE-2."""
    raise ValueError("invalid protected admission record")


def _decode(raw: bytes, maximum: int) -> Any:
    """@spec PROTECTED-HOOK-ADMISSION-2 PROTECTED-HOOK-LANE-2."""
    _require(type(raw) is bytes and len(raw) <= maximum)
    return json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=_pairs,
        parse_float=_reject_number,
        parse_constant=_reject_number,
    )


def _logical(value: Any) -> None:
    """@spec PROTECTED-HOOK-ADMISSION-2/3."""
    _require(type(value) is str and 0 < len(value.encode("utf-8")) <= 1024)
    _require(all(ord(c) >= 32 and ord(c) != 127 for c in value))


def _validate(value: Any, schema: dict[str, str]) -> None:
    """@spec PROTECTED-HOOK-ADMISSION-2/3 PROTECTED-HOOK-LANE-2."""
    _require(type(value) is dict and value.keys() == schema.keys())
    for field, kind in schema.items():
        item = value[field]
        if kind == "identity":
            _validate(item, _IDENTITY)
        elif kind == "logical":
            _logical(item)
        elif kind == "hook":
            canonical_hook(item)
        elif kind == "requested":
            _require(item is None or (type(item) is str and item == "read-only"))
        elif kind == "access":
            _require(type(item) is str and item == "read-only")
        elif kind == "accepted":
            _require(type(item) is str and item == "accepted")
        elif kind == "stream":
            _require(
                type(item) is str
                and re.fullmatch(r"(0|[1-9][0-9]*)-(0|[1-9][0-9]*)", item) is not None
            )
            _require(item != "0-0" and all(int(n) <= 2**64 - 1 for n in item.split("-")))
        elif kind == "execution":
            _require(type(item) is str)
        elif kind == "status":
            _require(type(item) is str and item in {"preparing", "committed", "failed"})
        elif kind == "attempts":
            _require(type(item) is int and 0 <= item <= 10)
        elif kind == "terminal":
            _require(item is None or (type(item) is str and item in _TERMINAL))
        elif kind == "receipt":
            if item is not None:
                _validate(item, _RECEIPT)
        else:
            _scalar(item, kind)
    if schema is _ENVELOPE:
        _require(
            value["execution_session_key"]
            == execution_session_key(
                value["runtime_id"], value["runtime_generation"], value["logical_conversation_key"]
            )
        )
    elif schema is _INTENT:
        _require(int(value["deadline_ms"]) == int(value["created_at_ms"]) + 300000)
    elif schema is _STATE:
        status, reason, receipt = value["status"], value["reason"], value["receipt"]
        _require(
            (status == "preparing" and reason is None and receipt is None)
            or (status == "committed" and reason is None and receipt is not None)
            or (status == "failed" and reason is not None and receipt is None)
        )


@dataclass(frozen=True, slots=True, repr=False)
class _Record:
    """@spec PROTECTED-HOOK-ADMISSION-2/3 PROTECTED-HOOK-LANE-2."""

    _canonical_bytes: bytes
    _schema: ClassVar[dict[str, str]]

    def __post_init__(self) -> None:
        """@spec PROTECTED-HOOK-ADMISSION-2/3."""
        try:
            value = _decode(self._canonical_bytes, _MAX_METADATA)
            _validate(value, self._schema)
            raw = _encode(value)
            _require(len(raw) <= _MAX_METADATA)
            object.__setattr__(self, "_canonical_bytes", raw)
        except (ValueError, TypeError, OverflowError, RecursionError):
            raise ValueError("invalid protected admission record") from None

    def __repr__(self) -> str:
        """@spec PROTECTED-HOOK-ADMISSION-1."""
        return f"<{type(self).__name__}>"

    @property
    def canonical_bytes(self) -> bytes:
        """@spec PROTECTED-HOOK-ADMISSION-2/3."""
        return self._canonical_bytes

    @property
    def digest(self) -> str:
        """@spec PROTECTED-HOOK-ADMISSION-2/3."""
        return hashlib.sha256(self._canonical_bytes).hexdigest()

    def as_dict(self) -> dict[str, Any]:
        """@spec PROTECTED-HOOK-ADMISSION-2/3."""
        value: dict[str, Any] = json.loads(self._canonical_bytes)
        return value


class Envelope(_Record):
    """@spec PROTECTED-HOOK-ADMISSION-3."""

    __slots__ = ()
    _schema = _ENVELOPE


class Intent(_Record):
    """@spec PROTECTED-HOOK-ADMISSION-3."""

    __slots__ = ()
    _schema = _INTENT


class State(_Record):
    """@spec PROTECTED-HOOK-ADMISSION-3."""

    __slots__ = ()
    _schema = _STATE


class Receipt(_Record):
    """@spec PROTECTED-HOOK-ADMISSION-3."""

    __slots__ = ()
    _schema = _RECEIPT


def parse_envelope(raw: bytes) -> Envelope:
    """@spec PROTECTED-HOOK-ADMISSION-3."""
    return Envelope(raw)


def parse_intent(raw: bytes) -> Intent:
    """@spec PROTECTED-HOOK-ADMISSION-3."""
    return Intent(raw)


def parse_state(raw: bytes) -> State:
    """@spec PROTECTED-HOOK-ADMISSION-3."""
    return State(raw)


def parse_receipt(raw: bytes) -> Receipt:
    """@spec PROTECTED-HOOK-ADMISSION-3."""
    return Receipt(raw)


@dataclass(frozen=True, slots=True, repr=False, kw_only=True)
class DeliveryIdentity:
    """@spec PROTECTED-HOOK-ADMISSION-2 PROTECTED-HOOK-SOURCE-1."""

    agent_id: str
    hook: str
    delivery_id: str

    def __post_init__(self) -> None:
        """@spec PROTECTED-HOOK-ADMISSION-2."""
        try:
            canonical_uuid(self.agent_id)
            canonical_hook(self.hook)
            _logical(self.delivery_id)
        except (ValueError, TypeError, OverflowError):
            raise ValueError("invalid protected admission identity") from None

    def __repr__(self) -> str:
        """@spec PROTECTED-HOOK-ADMISSION-1."""
        return "<DeliveryIdentity>"

    def as_dict(self) -> dict[str, Any]:
        """@spec PROTECTED-HOOK-ADMISSION-2."""
        return dict(agent_id=self.agent_id, hook=self.hook, delivery_id=self.delivery_id)


def delivery_digest(identity: DeliveryIdentity) -> str:
    """@spec PROTECTED-HOOK-ADMISSION-2."""
    _require(type(identity) is DeliveryIdentity)
    return hashlib.sha256(
        _encode([identity.agent_id, identity.hook, identity.delivery_id])
    ).hexdigest()


def execution_session_key(
    runtime_id: str, runtime_generation: str, logical_conversation_key: str
) -> str:
    """@spec PROTECTED-HOOK-ADMISSION-3 PROTECTED-HOOK-LANE-2."""
    try:
        _scalar(runtime_id, "uuid")
        _scalar(runtime_generation, "generation")
        _logical(logical_conversation_key)
        digest = hashlib.sha256(
            _encode([runtime_id, runtime_generation, logical_conversation_key])
        ).hexdigest()
        return f"protected:{runtime_id}:{runtime_generation}:{digest}"
    except (ValueError, TypeError, OverflowError):
        raise ValueError("invalid protected execution identity") from None


@dataclass(frozen=True, slots=True, repr=False, init=False)
class AdmissionRequest:
    """@spec PROTECTED-HOOK-ADMISSION-2 PROTECTED-HOOK-SOURCE-6/8."""

    identity: DeliveryIdentity
    _policy_bytes: bytes
    requested_tool_access: str | None
    request_body_sha256: str
    queued_payload: bytes

    def __init__(
        self,
        *,
        identity: DeliveryIdentity,
        source_policy: Mapping[str, Any],
        requested_tool_access: str | None,
        request_body_sha256: str,
        queued_payload: bytes,
    ) -> None:
        """@spec PROTECTED-HOOK-ADMISSION-2 PROTECTED-HOOK-SOURCE-6/8."""
        try:
            _require(type(identity) is DeliveryIdentity)
            _require(isinstance(source_policy, Mapping) and set(source_policy) == _POLICY_FIELDS)
            policy = dict(source_policy)
            policy_fingerprint(policy)
            _require(
                policy["mode"] == "protected"
                and policy["agent_id"] == identity.agent_id
                and policy["hook"] == identity.hook
            )
            _require(
                requested_tool_access is None
                or (type(requested_tool_access) is str and requested_tool_access == "read-only")
            )
            _scalar(request_body_sha256, "sha256")
            policy_bytes = _encode(policy)
            _require(len(policy_bytes) <= _MAX_METADATA)
            payload = _decode(queued_payload, _MAX_PAYLOAD)
            _require(type(payload) is dict)
            turn = parse_queued_turn(queued_payload)
            _require(
                turn.source in (TurnSource.WEBHOOK, TurnSource.CRON)
                and turn.tool_access == "read-only"
                and turn.reply_handle is not None
                and not turn.attachments
            )
            _scalar(turn.event_id, "opaque_ref")
            _logical(turn.conversation_id)
            if turn.source is TurnSource.CRON:
                _require(
                    turn.hook_run is not None
                    and turn.hook_run.agent_id == identity.agent_id
                    and turn.hook_run.name == identity.hook
                )
            for field, value in (
                ("identity", identity),
                ("_policy_bytes", policy_bytes),
                ("requested_tool_access", requested_tool_access),
                ("request_body_sha256", request_body_sha256),
                ("queued_payload", queued_payload),
            ):
                object.__setattr__(self, field, value)
        except (ValueError, TypeError, OverflowError, RecursionError):
            raise ValueError("invalid protected admission request") from None

    @property
    def source_policy(self) -> dict[str, Any]:
        """@spec PROTECTED-HOOK-ADMISSION-2."""
        value: dict[str, Any] = json.loads(self._policy_bytes)
        return value

    def __repr__(self) -> str:
        """@spec PROTECTED-HOOK-ADMISSION-1."""
        return "<AdmissionRequest>"


@dataclass(frozen=True, slots=True, repr=False, kw_only=True)
class AdmissionResult:
    """@spec PROTECTED-HOOK-ADMISSION-3."""

    status: str
    reason: str | None = None
    receipt: Receipt | dict[str, Any] | None = None

    def __post_init__(self) -> None:
        """@spec PROTECTED-HOOK-ADMISSION-3."""
        _require(type(self.status) is str)
        _require(self.reason is None or type(self.reason) is str)
        if type(self.receipt) is dict:
            object.__setattr__(self, "receipt", parse_receipt(_encode(self.receipt)))
        _require(self.receipt is None or type(self.receipt) is Receipt)
        _require(
            (
                self.status in {"accepted", "duplicate"}
                and self.reason is None
                and self.receipt is not None
            )
            or (self.status == "preparing" and self.reason is None and self.receipt is None)
            or (self.status == "failed" and self.reason in _TERMINAL and self.receipt is None)
            or (
                self.status == "conflict"
                and self.reason == "delivery_conflict"
                and self.receipt is None
            )
            or (self.status == "refused" and self.reason in _REFUSALS and self.receipt is None)
        )

    def as_dict(self) -> dict[str, Any]:
        """@spec PROTECTED-HOOK-ADMISSION-3."""
        return dict(
            status=self.status,
            reason=self.reason,
            receipt=self.receipt.as_dict() if isinstance(self.receipt, Receipt) else None,
        )

    def __repr__(self) -> str:
        """@spec PROTECTED-HOOK-ADMISSION-1."""
        return "<AdmissionResult>"
