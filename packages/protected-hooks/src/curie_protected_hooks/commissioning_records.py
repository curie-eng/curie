"""Closed commissioning permit and pure binding, @spec PROTECTED-COMMISSIONING-1.

Parsing and matching supplied facts authenticate nothing and authorize no work.
No production reader consumes these records; the production decoders refuse them.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from .authority_records import (
    _BROKER,
    _CREDENTIAL,
    _GUARD,
    _MAX_MILLISECOND,
    Manifest,
    _object,
)
from .commissioning_bootstrap import CommissioningBootstrap
from .commissioning_encoding import (
    _CANDIDATES,
    _REFUSALS,
    CommissioningRecordInvalid,
    _canonical,
    _decode,
    _encode,
    _require,
    _source_scope,
    _target_scope,
)

__all__ = [
    "BrokerObservation",
    "CommissioningPermit",
    "CommissioningRecordInvalid",
    "parse_commissioning_permit",
    "validate_commissioning_binding",
]

# @spec PROTECTED-COMMISSIONING-1
_FIXED: dict[str, Any] = {
    "schema_version": "version",
    "campaign_id": "uuid",
    "campaign_generation": "generation",
    "runtime_id": "uuid",
    "runtime_generation": "generation",
    "manifest_digest": "sha256",
    "bootstrap_digest": "sha256",
    "execution_config_digest": "sha256",
    "broker_identity": _BROKER,
    "guard_identity": _GUARD,
    "issuer_identity": _CREDENTIAL,
    "control_identity": _CREDENTIAL,
    "candidate_credentials": _CANDIDATES,
    "issued_at_ms": "millisecond",
    "expires_at_ms": "millisecond",
    "limits": {
        key: "generation"
        for key in ("deliveries", "launches", "model_calls", "output_tokens", "spend_microusd")
    },
}
_SCOPES = {"sources": _source_scope, "targets": _target_scope}


def _credential_ids(permit: dict[str, Any]) -> list[str]:
    """Issuer, control and both candidate IDs, @spec PROTECTED-COMMISSIONING-1."""
    return [
        permit["issuer_identity"]["id"],
        permit["control_identity"]["id"],
        *(ref["id"] for ref in permit["candidate_credentials"].values()),
    ]


def _permit_bytes(raw: Any) -> bytes:
    """@spec PROTECTED-COMMISSIONING-1."""
    try:
        value = _decode(raw, "commissioning_permit", _FIXED, _SCOPES)
        _require(len(set(_credential_ids(value))) == 4)
        _require(int(value["issued_at_ms"]) < int(value["expires_at_ms"]))
        return _canonical(value)
    except _REFUSALS:
        raise CommissioningRecordInvalid() from None


@dataclass(frozen=True, slots=True, repr=False)
class CommissioningPermit:
    """Immutable canonical bytes; not an authenticated grant, @spec PROTECTED-COMMISSIONING-1."""

    _canonical_bytes: bytes

    def __post_init__(self) -> None:
        """Validate even direct construction, @spec PROTECTED-COMMISSIONING-1."""
        object.__setattr__(self, "_canonical_bytes", _permit_bytes(self._canonical_bytes))

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-1."""
        return "CommissioningPermit()"

    @property
    def canonical_bytes(self) -> bytes:
        """@spec PROTECTED-COMMISSIONING-1."""
        return self._canonical_bytes

    @property
    def digest(self) -> str:
        """The permit's identity, @spec PROTECTED-COMMISSIONING-1."""
        return hashlib.sha256(self._canonical_bytes).hexdigest()

    def as_dict(self) -> dict[str, Any]:
        """Fresh nested copy, @spec PROTECTED-COMMISSIONING-1."""
        value: dict[str, Any] = json.loads(self._canonical_bytes)
        return value


def parse_commissioning_permit(raw: bytes) -> CommissioningPermit:
    """Closed structural validation only, @spec PROTECTED-COMMISSIONING-1."""
    return CommissioningPermit(raw)


@dataclass(frozen=True, slots=True, repr=False, init=False)
class BrokerObservation:
    """Broker identity, ``run_id`` and ``TIME`` as observed by the caller.

    This value checks shape only. The caller authenticates the broker session
    and reads its identity and time elsewhere (the authenticated control journal
    slice); constructing one proves nothing. @spec PROTECTED-COMMISSIONING-1.
    """

    _identity: bytes
    time_ms: int

    def __init__(self, *, identity: dict[str, Any], time_ms: int) -> None:
        """Copy a closed ``BrokerIdentity`` and broker time, @spec PROTECTED-COMMISSIONING-1."""
        try:
            _object(identity, _BROKER)
            _require(type(time_ms) is int and 0 <= time_ms <= int(_MAX_MILLISECOND))
            encoded = _encode(identity)
        except _REFUSALS:
            raise CommissioningRecordInvalid() from None
        object.__setattr__(self, "_identity", encoded)
        object.__setattr__(self, "time_ms", time_ms)

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-1."""
        return "BrokerObservation()"

    @property
    def identity(self) -> dict[str, Any]:
        """Fresh copy of the observed ``BrokerIdentity``, @spec PROTECTED-COMMISSIONING-1."""
        value: dict[str, Any] = json.loads(self._identity)
        return value

    @property
    def run_id(self) -> str:
        """@spec PROTECTED-COMMISSIONING-1."""
        run_id: str = self.identity["run_id"]
        return run_id


def validate_commissioning_binding(
    permit: CommissioningPermit,
    *,
    manifest: Manifest,
    bootstrap: CommissioningBootstrap,
    broker: BrokerObservation,
    trusted_issuer: dict[str, Any],
    trusted_control: dict[str, Any],
    max_permit_ms: int,
) -> None:
    """Match independently supplied facts; returns ``None`` and authorizes nothing.

    The caller authenticates the manifest, bootstrap and broker observation and
    holds the trusted references. The campaign generation floor and the journal
    are checked elsewhere. @spec PROTECTED-COMMISSIONING-1/2.
    """
    try:
        _require(type(permit) is CommissioningPermit)
        _require(type(manifest) is Manifest)
        _require(type(bootstrap) is CommissioningBootstrap)
        _require(type(broker) is BrokerObservation)
        _object(trusted_issuer, _CREDENTIAL)
        _object(trusted_control, _CREDENTIAL)
        _require(type(max_permit_ms) is int and max_permit_ms > 0)
        p, m, b = permit.as_dict(), manifest.as_dict(), bootstrap.as_dict()

        _require(p["manifest_digest"] == manifest.digest)
        _require(p["bootstrap_digest"] == bootstrap.digest)
        for field in ("runtime_id", "runtime_generation", "execution_config_digest"):
            _require(p[field] == m[field] == b[field])
        for field in ("broker_identity", "guard_identity"):
            _require(p[field] == m[field])
        _require(broker.identity == m["broker_identity"])

        _require(b["production_credentials"] == m["credential_refs"])
        _require(p["issuer_identity"] == b["issuer_identity"] == trusted_issuer)
        _require(p["control_identity"] == b["control_identity"] == trusted_control)
        _require(p["candidate_credentials"] == b["candidate_credentials"])
        # B's grammar already implies this; the binding states the separation
        # rule itself rather than rely on how B was parsed.
        production = {ref["id"] for ref in m["credential_refs"].values()}
        _require(production.isdisjoint(_credential_ids(p)))

        _require(all(source in b["source_scope"] for source in p["sources"]))
        _require(set(p["targets"]) <= set(b["target_scope"]))

        issued, expires = int(p["issued_at_ms"]), int(p["expires_at_ms"])
        _require(expires - issued <= min(max_permit_ms, int(b["max_permit_ms"])))
        _require(issued <= broker.time_ms < expires)
    except _REFUSALS:
        raise CommissioningRecordInvalid() from None
