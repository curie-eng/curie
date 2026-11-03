"""Closed retained broker records, not authority, @spec COMMISSIONING-SERIES-3.

The existing scripts write these formats without a schema header. Decoding
checks their closed grammar and original permit binding; it proves no causal
work, dispatch, successful cleanup or live campaign authorization.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, ClassVar

from .authority_records import _decimal, _object, _pairs, _reject_number, _scalar
from .commissioning_records import CommissioningPermit

__all__ = [
    "COUNTERS",
    "RetainedEvidenceInvalid",
    "RetainedJournal",
    "RetainedReservation",
    "parse_retained_journal",
    "parse_retained_reservation",
]
COUNTERS = ("deliveries", "launches", "model_calls", "output_tokens", "spend_microusd")
_MAX = "9223372036854775807"
_MAX_BYTES = 16384
_ERRORS = (ValueError, TypeError, OverflowError, RecursionError)
_JOURNAL = {
    "record_type": "opaque_ref",
    "permit_digest": "sha256",
    "broker_run_id": "run_id",
    "state": "opaque_ref",
    "generation": "generation",
}
_RESERVATION = {
    "record_type": "opaque_ref",
    "permit_digest": "sha256",
    "operation_id": "uuid",
    "kind": "opaque_ref",
    "request_digest": "sha256",
    "status": "opaque_ref",
}
_KIND_COUNTER = {"delivery": "deliveries", "launch": "launches", "model_call": "model_calls"}


class RetainedEvidenceInvalid(ValueError):
    """One fixed refusal; never echo a record or key, @spec COMMISSIONING-SERIES-3."""

    def __init__(self) -> None:
        super().__init__("Invalid retained commissioning evidence")


def _require(condition: bool) -> None:
    if not condition:
        raise RetainedEvidenceInvalid()


def _encode(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")


def _counts(value: Any) -> None:
    _require(type(value) is dict and value.keys() == set(COUNTERS))
    for count in value.values():
        _decimal(count, _MAX, zero=True)


def _decode(raw: bytes, kind: str) -> bytes:
    try:
        _require(type(raw) is bytes and len(raw) <= _MAX_BYTES)
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_pairs,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
        _require(type(value) is dict and value.get("record_type") == kind)
        if kind == "commissioning_journal":
            _require(value.keys() == _JOURNAL.keys() | {"counters", "exhausted"})
            _object({k: value[k] for k in _JOURNAL}, _JOURNAL)
            _require(value["state"] in {"prepared", "running", "closing", "closed", "failed"})
            _counts(value["counters"])
            exhausted = value["exhausted"]
            _require(type(exhausted) is str and exhausted in ("", *COUNTERS))
            if exhausted:
                _require(value["state"] not in {"prepared", "running"})
        else:
            _require(kind == "commissioning_reservation")
            _require(value.keys() == _RESERVATION.keys() | {"charge", "claim", "exhausts"})
            _object({k: value[k] for k in _RESERVATION}, _RESERVATION)
            _require(value["kind"] in _KIND_COUNTER)
            _require(value["status"] in {"reserved", "dispatched"})
            _counts(value["charge"])
            _require(value["charge"][_KIND_COUNTER[value["kind"]]] != "0")
            claim = value["claim"]
            if claim != "":
                _scalar(claim, "uuid")
                _require(value["kind"] == "launch" and value["status"] == "dispatched")
            _require(type(value["exhausts"]) is str and value["exhausts"] in {"0", "1"})
        result = _encode(value)
        _require(len(result) <= _MAX_BYTES)
        return result
    except _ERRORS:
        raise RetainedEvidenceInvalid() from None


@dataclass(frozen=True, slots=True, repr=False)
class _RetainedRecord:
    _canonical_bytes: bytes
    _kind: ClassVar[str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "_canonical_bytes", _decode(self._canonical_bytes, self._kind))

    def __repr__(self) -> str:
        return type(self).__name__ + "()"

    @property
    def canonical_bytes(self) -> bytes:
        """Canonical parsed state, not a broker grant, @spec COMMISSIONING-SERIES-3."""
        return self._canonical_bytes

    @property
    def digest(self) -> str:
        """Content identity of the decoded state, @spec COMMISSIONING-SERIES-3."""
        return hashlib.sha256(self._canonical_bytes).hexdigest()

    def as_dict(self) -> dict[str, Any]:
        """A fresh nested copy, @spec COMMISSIONING-SERIES-3."""
        result: dict[str, Any] = json.loads(self._canonical_bytes)
        return result


class RetainedJournal(_RetainedRecord):
    """Inert journal bytes; no authenticated-time claim, @spec COMMISSIONING-SERIES-3."""

    __slots__ = ()
    _kind = "commissioning_journal"


class RetainedReservation(_RetainedRecord):
    """Inert accounting state; never fresh dispatch authority, @spec COMMISSIONING-SERIES-3."""

    __slots__ = ()
    _kind = "commissioning_reservation"


def _limits(value: dict[str, Any], permit: CommissioningPermit, member: str) -> None:
    _require(type(permit) is CommissioningPermit and value["permit_digest"] == permit.digest)
    limits = permit.as_dict()["limits"]
    _require(all(int(value[member][k]) <= int(limits[k]) for k in COUNTERS))


def parse_retained_journal(raw: bytes, permit: CommissioningPermit) -> RetainedJournal:
    """Bind original permit state, including after expiry, @spec COMMISSIONING-SERIES-3."""
    try:
        result = RetainedJournal(raw)
        value = result.as_dict()
        _limits(value, permit, "counters")
        p = permit.as_dict()
        _require(value["broker_run_id"] == p["broker_identity"]["run_id"])
        if value["exhausted"]:
            counter = value["exhausted"]
            _require(value["counters"][counter] == p["limits"][counter])
        return result
    except _ERRORS:
        raise RetainedEvidenceInvalid() from None


def parse_retained_reservation(
    raw: bytes, permit: CommissioningPermit, operation_id: str
) -> RetainedReservation:
    """Bind an operation/charge without replay authority, @spec COMMISSIONING-SERIES-3."""
    try:
        _scalar(operation_id, "uuid")
        result = RetainedReservation(raw)
        value = result.as_dict()
        _limits(value, permit, "charge")
        _require(value["operation_id"] == operation_id)
        return result
    except _ERRORS:
        raise RetainedEvidenceInvalid() from None
