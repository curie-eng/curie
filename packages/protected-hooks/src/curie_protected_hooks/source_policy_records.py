"""@spec PROTECTED-HOOK-SOURCE-3/6/10."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Mapping

_DECIMAL = re.compile(r"(?:0|[1-9][0-9]{0,18})\Z")
_HOOK = re.compile(r"[a-z0-9][a-z0-9._-]{0,62}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_TARGET = frozenset({"mode", "tool_access", "runtime_id", "qualification_id", "bundle_digest"})
_POLICY = _TARGET | {"agent_id", "hook", "generation", "operation_id", "legacy_generation"}


class SourcePolicyRecordInvalid(ValueError):
    """@spec PROTECTED-HOOK-SOURCE-3/6/10."""


def canonical_decimal(value: str, *, positive: bool = False) -> str:
    """@spec PROTECTED-HOOK-SOURCE-3."""
    if type(value) is not str or not _DECIMAL.fullmatch(value):
        raise SourcePolicyRecordInvalid("invalid_generation")
    number = int(value)
    if number > 2**63 - 1 or (positive and number == 0):
        raise SourcePolicyRecordInvalid("invalid_generation")
    return value


def canonical_uuid(value: str) -> str:
    """@spec PROTECTED-HOOK-SOURCE-3."""
    if type(value) is not str or len(value) != 36:
        raise SourcePolicyRecordInvalid("invalid_identity")
    try:
        parsed = uuid.UUID(value)
    except ValueError:
        raise SourcePolicyRecordInvalid("invalid_identity") from None
    if str(parsed) != value:
        raise SourcePolicyRecordInvalid("invalid_identity")
    return value


def canonical_hook(value: str) -> str:
    """@spec PROTECTED-HOOK-SOURCE-3."""
    if type(value) is not str or not _HOOK.fullmatch(value):
        raise SourcePolicyRecordInvalid("invalid_hook")
    return value


def canonical_bundle_digest(value: str) -> str:
    """@spec PROTECTED-HOOK-SOURCE-3."""
    if type(value) is not str or not _DIGEST.fullmatch(value):
        raise SourcePolicyRecordInvalid("invalid_bundle_digest")
    return value


def _shape(
    value: Mapping[str, object], keys: frozenset[str], *, audit: bool = False
) -> dict[str, object]:
    """@spec PROTECTED-HOOK-SOURCE-6/10."""
    if not isinstance(value, Mapping) or set(value) not in (
        keys,
        keys | {"updated_at"} if audit else keys,
    ):
        raise SourcePolicyRecordInvalid("invalid_record_fields")
    return {key: value[key] for key in keys}


def _target(value: Mapping[str, object]) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6/10."""
    if type(value["mode"]) is not str:
        raise SourcePolicyRecordInvalid("invalid_policy_mode")
    if value["mode"] == "ordinary":
        if any(value[key] is not None for key in _TARGET - {"mode"}):
            raise SourcePolicyRecordInvalid("invalid_ordinary_policy")
        return
    if (
        value["mode"] != "protected"
        or type(value["tool_access"]) is not str
        or value["tool_access"] != "read-only"
    ):
        raise SourcePolicyRecordInvalid("invalid_protected_policy")
    for key in ("runtime_id", "qualification_id"):
        reference = value[key]
        if type(reference) is not str:
            raise SourcePolicyRecordInvalid("invalid_identity")
        canonical_uuid(reference)
    digest = value["bundle_digest"]
    if type(digest) is not str:
        raise SourcePolicyRecordInvalid("invalid_bundle_digest")
    canonical_bundle_digest(digest)


def _hash(value: Mapping[str, object]) -> str:
    """@spec PROTECTED-HOOK-SOURCE-6/10."""
    encoded = json.dumps(dict(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("ascii")).hexdigest()


def target_intent_sha256(target: Mapping[str, object]) -> str:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    selected = _shape(target, _TARGET)
    _target(selected)
    return _hash(selected)


def policy_fingerprint(record: Mapping[str, object]) -> str:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    selected = _shape(record, _POLICY, audit=True)
    _target(selected)
    for key, validator in (
        ("agent_id", canonical_uuid),
        ("operation_id", canonical_uuid),
        ("hook", canonical_hook),
    ):
        value = selected[key]
        if type(value) is not str:
            raise SourcePolicyRecordInvalid("invalid_record_binding")
        validator(value)
    for key in ("generation", "legacy_generation"):
        value = selected[key]
        if type(value) is not str:
            raise SourcePolicyRecordInvalid("invalid_generation")
        canonical_decimal(value, positive=key == "generation")
    return _hash(selected)
