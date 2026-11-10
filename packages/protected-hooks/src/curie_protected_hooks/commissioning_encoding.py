"""Shared closed commissioning encoding, @spec PROTECTED-COMMISSIONING-1/2.

Commissioning records reuse the LANE-2 encoding and scalar spellings. Decoding
authenticates nothing and authorizes no work.
"""

from __future__ import annotations

import json
from typing import Any

from .authority_records import (
    _CREDENTIAL,
    _MAX_BYTES,
    AuthorityRecordInvalid,
    _object,
    _pairs,
    _reject_number,
    _scalar,
)

_MAX_SCOPE = 64
# @spec PROTECTED-COMMISSIONING-1
_SOURCE: dict[str, Any] = {
    "source_id": "uuid",
    "generation": "generation",
    "policy_fingerprint": "sha256",
}
_CANDIDATES: dict[str, Any] = {"enqueue": _CREDENTIAL, "worker": _CREDENTIAL}


class CommissioningRecordInvalid(ValueError):
    """Redacted commissioning refusal, @spec PROTECTED-COMMISSIONING-1/2."""

    def __init__(self) -> None:
        """Never echo a member or value, @spec PROTECTED-COMMISSIONING-1."""
        super().__init__("Invalid protected commissioning record")


def _require(condition: bool) -> None:
    """@spec PROTECTED-COMMISSIONING-1."""
    if not condition:
        raise CommissioningRecordInvalid()


def _source_scope(value: Any) -> None:
    """1 to 64 sources sorted by distinct UUID, @spec PROTECTED-COMMISSIONING-1."""
    _require(type(value) is list and 1 <= len(value) <= _MAX_SCOPE)
    for source in value:
        _object(source, _SOURCE)
    ids = [source["source_id"] for source in value]
    _require(ids == sorted(set(ids)))


def _target_scope(value: Any) -> None:
    """1 to 64 distinct sorted opaque_ref values, @spec PROTECTED-COMMISSIONING-1."""
    _require(type(value) is list and 1 <= len(value) <= _MAX_SCOPE)
    for target in value:
        _scalar(target, "opaque_ref")
    _require(value == sorted(set(value)))


def _encode(value: dict[str, Any]) -> bytes:
    """Canonical ASCII JSON, @spec PROTECTED-COMMISSIONING-1."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")


def _decode(raw: Any, record_type: str, fixed: dict[str, Any], scopes: dict[str, Any]) -> Any:
    """Closed LANE-2 decoding of one record kind, @spec PROTECTED-COMMISSIONING-1/2.

    Returns the decoded value. The caller adds cross-member checks and encodes.
    """
    _require(type(raw) is bytes and len(raw) <= _MAX_BYTES)
    value = json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=_pairs,
        parse_float=_reject_number,
        parse_constant=_reject_number,
    )
    _require(type(value) is dict and value.keys() == fixed.keys() | scopes.keys() | {"record_type"})
    _require(type(value["record_type"]) is str and value["record_type"] == record_type)
    _object({key: value[key] for key in fixed}, fixed)
    for key, check in scopes.items():
        check(value[key])
    return value


def _canonical(value: dict[str, Any]) -> bytes:
    """@spec PROTECTED-COMMISSIONING-1."""
    result = _encode(value)
    _require(len(result) <= _MAX_BYTES)
    return result


# Every refusal from the LANE-2 helpers or the JSON decoder becomes one redacted error.
_REFUSALS = (AuthorityRecordInvalid, ValueError, TypeError, OverflowError, RecursionError)
