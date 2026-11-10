"""Closed internal authority records, @spec PROTECTED-HOOK-LANE-2.

Matching supplied facts performs no authentication, provisioning or observation.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
from dataclasses import dataclass
from typing import Any, ClassVar
from uuid import UUID

_MAX_BYTES = 16384
_MAX_GENERATION = "9223372036854775807"
_MAX_MILLISECOND = "9007199254740991"


class AuthorityRecordInvalid(ValueError):
    """Safe structural or binding refusal, @spec PROTECTED-HOOK-LANE-2."""

    def __init__(self) -> None:
        """@spec PROTECTED-HOOK-LANE-2."""
        super().__init__("Invalid protected authority record")


def _require(condition: bool) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    if not condition:
        raise AuthorityRecordInvalid()


def _pattern(value: Any, pattern: str) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    _require(type(value) is str and re.fullmatch(pattern, value, flags=re.ASCII) is not None)


def _decimal(value: Any, maximum: str, *, zero: bool = False) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    _pattern(value, r"0|[1-9][0-9]*" if zero else r"[1-9][0-9]*")
    _require(len(value) < len(maximum) or (len(value) == len(maximum) and value <= maximum))


def _host(value: Any) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    _require(type(value) is str and value.isascii())
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        _require(not re.fullmatch(r"[0-9]+(?:\.[0-9]+)+", value))
        _require(0 < len(value) <= 253)
        for label in value.split("."):
            _pattern(label, r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
    else:
        _require(address.compressed == value and "%" not in value)


def _scalar(value: Any, kind: str) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    if kind == "uuid":
        _require(type(value) is str and str(UUID(value)) == value)
    elif kind == "generation":
        _decimal(value, _MAX_GENERATION)
    elif kind == "millisecond":
        _decimal(value, _MAX_MILLISECOND, zero=True)
    elif kind == "sha256":
        _pattern(value, r"[0-9a-f]{64}")
    elif kind == "oci_digest":
        _pattern(value, r"sha256:[0-9a-f]{64}")
    elif kind == "opaque_ref":
        _pattern(value, r"[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}")
    elif kind == "host":
        _host(value)
    elif kind == "run_id":
        _pattern(value, r"[0-9a-f]{40}")
    elif kind == "port":
        _require(type(value) is int and 1 <= value <= 65535)
    elif kind == "version":
        _require(type(value) is int and value == 1)
    elif kind == "database":
        _require(type(value) is int and value == 0)
    elif kind == "substrate":
        _require(type(value) is str and value in ("docker", "kubernetes"))
    else:
        raise AuthorityRecordInvalid()


# @spec PROTECTED-HOOK-LANE-2
_BROKER: dict[str, Any] = {
    "instance_id": "uuid",
    "endpoint": {"host": "host", "port": "port"},
    "tls_server_name": "host",
    "tls_spki_sha256": "sha256",
    "run_id": "run_id",
    "database": "database",
}
_CREDENTIAL: dict[str, Any] = {"id": "opaque_ref", "generation": "generation"}
_GUARD: dict[str, Any] = {
    "control_id": "uuid",
    "revision": "generation",
    "config_sha256": "sha256",
}
_MANIFEST: dict[str, Any] = {
    "schema_version": "version",
    "runtime_id": "uuid",
    "runtime_generation": "generation",
    "broker_identity": _BROKER,
    "worker_image_digest": "oci_digest",
    "runner_image_digest": "oci_digest",
    "bundle_digest": {"sha256": "sha256", "object_identity": "opaque_ref"},
    "execution_config_digest": "sha256",
    "qualification_id": "uuid",
    "substrate": {
        "kind": "substrate",
        "authority_domain_id": "uuid",
        "launch_identity": "opaque_ref",
        "launch_config_sha256": "sha256",
    },
    "guard_identity": _GUARD,
    "credential_refs": {"enqueue": _CREDENTIAL, "worker": _CREDENTIAL, "verifier": _CREDENTIAL},
}
_QUALIFICATION: dict[str, Any] = {
    "schema_version": "version",
    "qualification_id": "uuid",
    "qualification_generation": "generation",
    "runtime_id": "uuid",
    "runtime_generation": "generation",
    "manifest_digest": "sha256",
    "broker_identity": _BROKER,
    "execution_config_digest": "sha256",
    "guard_identity": _GUARD,
    "measurement_record_id": "opaque_ref",
}
_READINESS: dict[str, Any] = {
    "schema_version": "version",
    "manifest_digest": "sha256",
    "runtime_id": "uuid",
    "runtime_generation": "generation",
    "qualification_id": "uuid",
    "qualification_generation": "generation",
    "broker_identity": _BROKER,
    "guard_identity": _GUARD,
    "verifier_identity": _CREDENTIAL,
    "issued_at_ms": "millisecond",
    "expires_at_ms": "millisecond",
    "measurement_record_id": "opaque_ref",
}


def _object(value: Any, schema: dict[str, Any]) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    _require(type(value) is dict and value.keys() == schema.keys())
    for field, constraint in schema.items():
        if type(constraint) is dict:
            _object(value[field], constraint)
        else:
            _scalar(value[field], constraint)


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """@spec PROTECTED-HOOK-LANE-2."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result)
        result[key] = value
    return result


def _reject_number(_value: str) -> Any:
    """@spec PROTECTED-HOOK-LANE-2."""
    raise AuthorityRecordInvalid()


def _canonical(raw: bytes, schema: dict[str, Any]) -> bytes:
    """@spec PROTECTED-HOOK-LANE-2."""
    try:
        _require(type(raw) is bytes and len(raw) <= _MAX_BYTES)
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_pairs,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
        _object(value, schema)
        if schema is _MANIFEST:
            refs = value["credential_refs"]
            _require(len({refs[role]["id"] for role in refs}) == 3)
        if schema is _READINESS:
            _require(int(value["issued_at_ms"]) < int(value["expires_at_ms"]))
        canonical = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        _require(len(canonical) <= _MAX_BYTES)
        return canonical
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise AuthorityRecordInvalid() from None


@dataclass(frozen=True, slots=True)
class _Record:
    """Immutable byte storage, @spec PROTECTED-HOOK-LANE-2."""

    _canonical_bytes: bytes
    _schema: ClassVar[dict[str, Any]]

    def __post_init__(self) -> None:
        """Validate even direct construction, @spec PROTECTED-HOOK-LANE-2."""
        object.__setattr__(
            self, "_canonical_bytes", _canonical(self._canonical_bytes, self._schema)
        )

    @property
    def canonical_bytes(self) -> bytes:
        """@spec PROTECTED-HOOK-LANE-2."""
        return self._canonical_bytes

    @property
    def digest(self) -> str:
        """@spec PROTECTED-HOOK-LANE-2."""
        return hashlib.sha256(self._canonical_bytes).hexdigest()

    def as_dict(self) -> dict[str, Any]:
        """Fresh nested copy, @spec PROTECTED-HOOK-LANE-2."""
        value: dict[str, Any] = json.loads(self._canonical_bytes)
        return value


class Manifest(_Record):
    """@spec PROTECTED-HOOK-LANE-2."""

    __slots__ = ()
    _schema = _MANIFEST


class Qualification(_Record):
    """@spec PROTECTED-HOOK-LANE-2."""

    __slots__ = ()
    _schema = _QUALIFICATION


class Readiness(_Record):
    """@spec PROTECTED-HOOK-LANE-2."""

    __slots__ = ()
    _schema = _READINESS


def parse_manifest(raw: bytes) -> Manifest:
    """@spec PROTECTED-HOOK-LANE-2."""
    return Manifest(raw)


def parse_qualification(raw: bytes) -> Qualification:
    """@spec PROTECTED-HOOK-LANE-2."""
    return Qualification(raw)


def parse_readiness(raw: bytes) -> Readiness:
    """@spec PROTECTED-HOOK-LANE-2."""
    return Readiness(raw)


def validate_authority(
    manifest: Manifest,
    qualification: Qualification,
    readiness: Readiness,
    *,
    broker_identity: dict[str, Any],
    broker_now_ms: int,
    trusted_max_readiness_ms: int,
) -> None:
    """Match trusted supplied facts only, @spec PROTECTED-HOOK-LANE-2."""
    try:
        _require(type(manifest) is Manifest)
        _require(type(qualification) is Qualification)
        _require(type(readiness) is Readiness)
        _object(broker_identity, _BROKER)
        _require(type(broker_now_ms) is int and 0 <= broker_now_ms <= int(_MAX_MILLISECOND))
        _require(type(trusted_max_readiness_ms) is int and trusted_max_readiness_ms > 0)
        m, q, r = manifest.as_dict(), qualification.as_dict(), readiness.as_dict()
        for record in (q, r):
            _require(record["manifest_digest"] == manifest.digest)
            for field in (
                "runtime_id",
                "runtime_generation",
                "qualification_id",
                "broker_identity",
                "guard_identity",
            ):
                _require(record[field] == m[field])
        _require(q["execution_config_digest"] == m["execution_config_digest"])
        _require(r["qualification_generation"] == q["qualification_generation"])
        _require(r["verifier_identity"] == m["credential_refs"]["verifier"])
        _require(broker_identity == m["broker_identity"])
        issued, expires = int(r["issued_at_ms"]), int(r["expires_at_ms"])
        _require(issued <= broker_now_ms < expires)
        _require(expires - issued <= trusted_max_readiness_ms)
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise AuthorityRecordInvalid() from None
