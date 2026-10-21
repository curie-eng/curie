"""Closed commissioning bootstrap B, @spec PROTECTED-COMMISSIONING-2.

B predeclares the production and commissioning compositions and the
commissioning principals. Parsing never selects a composition, installs a
control or authenticates the provisioner. B holds no manifest digest, guard
digest or permit, so hashing it precedes G, M and every permit.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from .authority_records import _CREDENTIAL
from .commissioning_encoding import (
    _CANDIDATES,
    _REFUSALS,
    CommissioningRecordInvalid,
    _canonical,
    _decode,
    _require,
    _source_scope,
    _target_scope,
)

__all__ = ["CommissioningBootstrap", "CommissioningRecordInvalid", "parse_commissioning_bootstrap"]

# @spec PROTECTED-COMMISSIONING-2
_FIXED: dict[str, Any] = {
    "schema_version": "version",
    "runtime_id": "uuid",
    "runtime_generation": "generation",
    "execution_config_digest": "sha256",
    "entry_points": {"production": "opaque_ref", "commissioning": "opaque_ref"},
    "production_credentials": {
        "enqueue": _CREDENTIAL,
        "worker": _CREDENTIAL,
        "verifier": _CREDENTIAL,
    },
    "candidate_credentials": _CANDIDATES,
    "issuer_identity": _CREDENTIAL,
    "control_identity": _CREDENTIAL,
    "qualification_publisher_identity": _CREDENTIAL,
    "control_reader_identity": _CREDENTIAL,
    "mediator_identity": _CREDENTIAL,
    "gateway_identity": _CREDENTIAL,
    "max_permit_ms": "generation",
}
_SCOPES = {"source_scope": _source_scope, "target_scope": _target_scope}


def _bootstrap_bytes(raw: Any) -> bytes:
    """@spec PROTECTED-COMMISSIONING-2."""
    try:
        value = _decode(raw, "commissioning_bootstrap", _FIXED, _SCOPES)
        entry = value["entry_points"]
        _require(entry["production"] != entry["commissioning"])
        # Every credential ID in B is pairwise distinct: the production trio,
        # the four commissioning broker principals, the Qualification publisher
        # (design decision 8), the production control reader (design decision
        # 10) and the launch mediator and provider gateway. So no
        # commissioning or launch identity can equal a production ID.
        ids = [
            *(ref["id"] for ref in value["production_credentials"].values()),
            *(ref["id"] for ref in value["candidate_credentials"].values()),
            *(
                value[member]["id"]
                for member in (
                    "issuer_identity",
                    "control_identity",
                    "qualification_publisher_identity",
                    "control_reader_identity",
                    "mediator_identity",
                    "gateway_identity",
                )
            ),
        ]
        _require(len(set(ids)) == len(ids))
        return _canonical(value)
    except _REFUSALS:
        raise CommissioningRecordInvalid() from None


@dataclass(frozen=True, slots=True, repr=False)
class CommissioningBootstrap:
    """Immutable canonical B; not an authenticated selection, @spec PROTECTED-COMMISSIONING-2."""

    _canonical_bytes: bytes

    def __post_init__(self) -> None:
        """Validate even direct construction, @spec PROTECTED-COMMISSIONING-2."""
        object.__setattr__(self, "_canonical_bytes", _bootstrap_bytes(self._canonical_bytes))

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-2."""
        return "CommissioningBootstrap()"

    @property
    def canonical_bytes(self) -> bytes:
        """@spec PROTECTED-COMMISSIONING-2."""
        return self._canonical_bytes

    @property
    def digest(self) -> str:
        """sha256(B), the value G and every permit bind, @spec PROTECTED-COMMISSIONING-2."""
        return hashlib.sha256(self._canonical_bytes).hexdigest()

    def as_dict(self) -> dict[str, Any]:
        """Fresh nested copy, @spec PROTECTED-COMMISSIONING-2."""
        value: dict[str, Any] = json.loads(self._canonical_bytes)
        return value


def parse_commissioning_bootstrap(raw: bytes) -> CommissioningBootstrap:
    """Closed structural validation only, @spec PROTECTED-COMMISSIONING-2."""
    return CommissioningBootstrap(raw)
