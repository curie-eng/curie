"""Explicit inert commissioning B v2, @spec COMMISSIONING-SERIES-3.

This decoder neither falls back to B v1 nor grants series authority. Consumers
must explicitly implement the complete v2 composition before admitting work.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from .authority_records import _CREDENTIAL, _MAX_BYTES, _object, _pairs, _reject_number
from .commissioning_encoding import (
    _CANDIDATES,
    _REFUSALS,
    CommissioningRecordInvalid,
    _canonical,
    _require,
    _source_scope,
    _target_scope,
)

__all__ = ["CommissioningRecordInvalid", "SeriesBootstrap", "parse_series_bootstrap"]

# Independent versioned inventory: v1 remains closed and is never coerced.
_FIELDS: dict[str, Any] = {
    "runtime_id": "uuid",
    "runtime_generation": "generation",
    "execution_config_digest": "sha256",
    "entry_points": {"production": "opaque_ref", "commissioning": "opaque_ref"},
    "production_credentials": {name: _CREDENTIAL for name in ("enqueue", "worker", "verifier")},
    "candidate_credentials": _CANDIDATES,
    "issuer_identity": _CREDENTIAL,
    "control_identity": _CREDENTIAL,
    "qualification_publisher_identity": _CREDENTIAL,
    "control_reader_identity": _CREDENTIAL,
    "source_writer_identity": _CREDENTIAL,
    "mediator_identity": _CREDENTIAL,
    "gateway_identity": _CREDENTIAL,
    "evidence_observer_identity": _CREDENTIAL,
    "max_permit_ms": "generation",
}
_IDENTITY_FIELDS = (
    "issuer_identity",
    "control_identity",
    "qualification_publisher_identity",
    "control_reader_identity",
    "source_writer_identity",
    "mediator_identity",
    "gateway_identity",
    "evidence_observer_identity",
)


def _bootstrap_bytes(raw: bytes) -> bytes:
    try:
        _require(type(raw) is bytes and len(raw) <= _MAX_BYTES)
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_pairs,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
        _require(type(value) is dict)
        _require(
            value.keys()
            == _FIELDS.keys()
            | {
                "schema_version",
                "record_type",
                "source_scope",
                "target_scope",
            }
        )
        _require(type(value["schema_version"]) is int and value["schema_version"] == 2)
        _require(value["record_type"] == "commissioning_bootstrap")
        _object({key: value[key] for key in _FIELDS}, _FIELDS)
        _source_scope(value["source_scope"])
        _target_scope(value["target_scope"])
        _require(value["entry_points"]["production"] != value["entry_points"]["commissioning"])
        identities = [
            *value["production_credentials"].values(),
            *value["candidate_credentials"].values(),
            *(value[field] for field in _IDENTITY_FIELDS),
        ]
        ids = [ref["id"] for ref in identities]
        _require(len(ids) == len(set(ids)))
        return _canonical(value)
    except _REFUSALS:
        raise CommissioningRecordInvalid() from None


@dataclass(frozen=True, slots=True, repr=False)
class SeriesBootstrap:
    """Immutable canonical B v2; parsing authenticates nothing, @spec COMMISSIONING-SERIES-3."""

    _canonical_bytes: bytes

    def __post_init__(self) -> None:
        """Validate direct construction as well, @spec COMMISSIONING-SERIES-3."""
        object.__setattr__(self, "_canonical_bytes", _bootstrap_bytes(self._canonical_bytes))

    def __repr__(self) -> str:
        """No scope/credential references in representations, @spec COMMISSIONING-SERIES-3."""
        return "SeriesBootstrap()"

    @property
    def canonical_bytes(self) -> bytes:
        """Canonical closed record bytes, @spec COMMISSIONING-SERIES-3."""
        return self._canonical_bytes

    @property
    def digest(self) -> str:
        """sha256 of the original v2 bytes, @spec COMMISSIONING-SERIES-3."""
        return hashlib.sha256(self._canonical_bytes).hexdigest()

    def as_dict(self) -> dict[str, Any]:
        """A fresh nested copy, @spec COMMISSIONING-SERIES-3."""
        value: dict[str, Any] = json.loads(self._canonical_bytes)
        return value


def parse_series_bootstrap(raw: bytes) -> SeriesBootstrap:
    """Closed structural validation only, @spec COMMISSIONING-SERIES-3."""
    return SeriesBootstrap(raw)
