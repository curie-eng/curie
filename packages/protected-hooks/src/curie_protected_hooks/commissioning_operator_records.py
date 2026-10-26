"""Closed inert v2 operator history, not evidence, @spec COMMISSIONING-SERIES-8."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, ClassVar

from .authority_records import _BROKER, _CREDENTIAL, _object, _scalar
from .commissioning_encoding import (
    _REFUSALS,
    CommissioningRecordInvalid,
    _canonical,
    _decode,
    _require,
)
from .commissioning_run_records import _ordinal

ROLES = frozenset(
    {
        "administrator",
        "evidence-observer",
        "acl-observer",
        "acceptance-publisher",
        "qualification-publisher",
        "activation-publisher",
    }
)
_PHASES = frozenset({"setup", "trial", "transition", "decision", "publication", "teardown"})
_IDENTITY = {
    "client_id": "generation",
    "broker_identity": _BROKER,
    "credential": _CREDENTIAL,
    "host_process": {
        "pid": "generation",
        "start_ticks": "generation",
        "pid_namespace_inode": "generation",
        "boot_id": "uuid",
    },
}


def _version(value: Any) -> None:
    _require(type(value) is int and value == 2)


def _roles(value: Any) -> None:
    _require(type(value) is list and len(value) == len(ROLES))
    for row in value:
        _object(
            row, {"role": "opaque_ref", "credential": _CREDENTIAL, "max_connections": "generation"}
        )
        _require(int(row["max_connections"]) <= 256)
    _require([row["role"] for row in value] == sorted(ROLES))
    _require(len({row["credential"]["id"] for row in value}) == len(ROLES))
    _require(sum(int(row["max_connections"]) for row in value) <= 1000)


def _nullable_digest(value: Any) -> None:
    if value is not None:
        _scalar(value, "sha256")


def _nullable_identity(value: Any) -> None:
    if value is not None:
        _object(value, _IDENTITY)


def _bytes(raw: bytes, kind: str) -> bytes:
    try:
        if kind == "commissioning_operator_recipe":
            value = _decode(
                raw,
                kind,
                {"provisioning_recipe_digest": "sha256"},
                {"schema_version": _version, "roles": _roles},
            )
        else:
            _require(kind == "commissioning_operator_event")
            value = _decode(
                raw,
                kind,
                {
                    "run_id": "uuid",
                    "revision": "generation",
                    "previous_digest": "sha256",
                    "elapsed_ms": "millisecond",
                    "kind": "opaque_ref",
                    "intent_id": "uuid",
                    "role": "opaque_ref",
                    "phase": "opaque_ref",
                },
                {
                    "schema_version": _version,
                    "ordinal": _ordinal,
                    "binding_digest": _nullable_digest,
                    "identity": _nullable_identity,
                },
            )
            _require(value["role"] in ROLES and value["phase"] in _PHASES)
            phase, role = value["phase"], value["role"]
            _require((phase in {"setup", "teardown"}) == (value["ordinal"] == "0"))
            if phase in {"decision", "publication"}:
                _require(value["ordinal"] == "10")
            if phase == "transition":
                _require(value["ordinal"] != "10")
            if role == "administrator":
                _require(phase in {"setup", "transition", "teardown"})
            elif role.endswith("publisher"):
                _require(phase == "publication")
            if value["kind"] == "intended":
                _require(value["identity"] is None and value["binding_digest"] is None)
            elif value["kind"] == "bound":
                _require(value["identity"] is not None and value["binding_digest"] is None)
            elif value["kind"] == "closed":
                _require(value["identity"] is None and value["binding_digest"] is not None)
            else:
                _require(value["kind"] == "uncertain" and value["identity"] is None)
            _require((value["revision"] == "1") == (value["previous_digest"] == "0" * 64))
            if value["revision"] == "1":
                _require(value["kind"] == "intended")
        return _canonical(value)
    except _REFUSALS:
        raise CommissioningRecordInvalid() from None


@dataclass(frozen=True, slots=True, repr=False)
class _OperatorRecord:
    _canonical_bytes: bytes
    _kind: ClassVar[str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "_canonical_bytes", _bytes(self._canonical_bytes, self._kind))

    def __repr__(self) -> str:
        return type(self).__name__ + "()"

    @property
    def canonical_bytes(self) -> bytes:
        """Closed retained bytes, @spec COMMISSIONING-SERIES-8."""
        return self._canonical_bytes

    @property
    def digest(self) -> str:
        """Content identity only, @spec COMMISSIONING-SERIES-8."""
        return hashlib.sha256(self._canonical_bytes).hexdigest()

    def as_dict(self) -> dict[str, Any]:
        """A detached nested copy, @spec COMMISSIONING-SERIES-8."""
        result: dict[str, Any] = json.loads(self._canonical_bytes)
        return result


class OperatorRecipe(_OperatorRecord):
    """Fixed inventory/ceilings, not a grant, @spec COMMISSIONING-SERIES-8."""

    __slots__ = ()
    _kind = "commissioning_operator_recipe"


class OperatorEvent(_OperatorRecord):
    """One retained event, not a closure proof, @spec COMMISSIONING-SERIES-8."""

    __slots__ = ()
    _kind = "commissioning_operator_event"


def parse_operator_recipe(raw: bytes) -> OperatorRecipe:
    """Parse an inert versioned inventory, @spec COMMISSIONING-SERIES-8."""
    return OperatorRecipe(raw)


def parse_operator_event(raw: bytes) -> OperatorEvent:
    """Parse closed history without authenticating it, @spec COMMISSIONING-SERIES-8."""
    return OperatorEvent(raw)
