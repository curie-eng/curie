"""Generic static compatibility declarations and exact input identity.

Contract identifiers are opaque. This module does not interpret coworker input,
prove readiness, or confer tracker authority.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

from .manifest import resolve_manifest

INPUT_CONTRACT_ENFORCEMENT = "curie/input-contracts@1"
_PREFIX = "<!-- curie-input@1 "
_ID = re.compile(r"[A-Za-z0-9._/:@-]{1,200}\Z", re.ASCII)
_HASH = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)


class InputContractsInvalid(ValueError):
    """Malformed, unsupported, unavailable or incompatible binding."""


@dataclass(frozen=True)
class InputContracts:
    interface: str
    accepts: tuple[str, ...]
    required: bool = True


@dataclass(frozen=True)
class InputEnvelope:
    contract: str
    deployment: str
    bundle_sha256: str
    body_sha256: str
    body: str


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise InputContractsInvalid("duplicate input contract key")
        result[key] = value
    return result


def _load(text: str) -> Any:
    try:
        return json.loads(text, object_pairs_hook=_unique)
    except (ValueError, RecursionError) as exc:
        raise InputContractsInvalid("invalid input contract JSON") from exc


def parse_input_contracts(raw: object) -> InputContracts:
    """Parse a strict declaration without interpreting its opaque IDs."""
    if not isinstance(raw, dict) or set(raw) != {"interface", "accepts", "required"}:
        raise InputContractsInvalid("inputContracts requires interface, accepts and required")
    if raw["interface"] != INPUT_CONTRACT_ENFORCEMENT or raw["required"] is not True:
        raise InputContractsInvalid("unsupported input contract interface or required flag")
    accepts = raw["accepts"]
    if not isinstance(accepts, list) or not 1 <= len(accepts) <= 32:
        raise InputContractsInvalid("inputContracts accepts must contain 1 to 32 identifiers")
    if any(not isinstance(value, str) or _ID.fullmatch(value) is None for value in accepts):
        raise InputContractsInvalid("invalid input contract identifier")
    if len(set(accepts)) != len(accepts):
        raise InputContractsInvalid("duplicate input contract identifier")
    return InputContracts(INPUT_CONTRACT_ENFORCEMENT, tuple(accepts))


def read_input_contracts(root: Path, *, enforcements: frozenset[str]) -> InputContracts | None:
    """Read only when the caller implements admission AND start enforcement."""
    manifest = resolve_manifest(root)
    if manifest is None:
        raise InputContractsInvalid("bundle manifest unavailable")
    try:
        raw = _load(manifest.read_text(encoding="utf-8"))
    except (OSError, UnicodeError) as exc:
        raise InputContractsInvalid("bundle manifest unreadable") from exc
    if not isinstance(raw, dict):
        raise InputContractsInvalid("bundle manifest must be an object")
    if "inputContracts" not in raw:
        return None
    declaration = parse_input_contracts(raw["inputContracts"])
    if INPUT_CONTRACT_ENFORCEMENT not in enforcements:
        raise InputContractsInvalid("input contract admission and start enforcement unavailable")
    return declaration


def parse_input_envelope(body: str) -> InputEnvelope:
    """Parse one bounded prefix and hash the exact remaining UTF-8 body."""
    if not isinstance(body, str) or not body.startswith(_PREFIX):
        raise InputContractsInvalid("input contract prefix missing or unsupported")
    end = body.find(" -->\n", len(_PREFIX), 2048)
    if end < 0:
        raise InputContractsInvalid("input contract prefix delimiter missing")
    boundary = end + len(" -->\n")
    try:
        if len(body[:boundary].encode("utf-8")) > 2048:
            raise InputContractsInvalid("input contract prefix exceeds limit")
        remainder = body[boundary:]
        digest = hashlib.sha256(remainder.encode("utf-8")).hexdigest()
    except UnicodeError as exc:
        raise InputContractsInvalid("input contract body is not UTF-8") from exc
    raw = _load(body[len(_PREFIX) : end])
    keys = {"contract", "deployment", "bundle_sha256", "body_sha256"}
    if (
        not isinstance(raw, dict)
        or set(raw) != keys
        or any(not isinstance(value, str) for value in raw.values())
    ):
        raise InputContractsInvalid("invalid input contract prefix fields")
    if _ID.fullmatch(raw["contract"]) is None:
        raise InputContractsInvalid("invalid input contract identifier")
    try:
        deployment = str(UUID(raw["deployment"]))
    except ValueError as exc:
        raise InputContractsInvalid("invalid input contract deployment") from exc
    if deployment != raw["deployment"]:
        raise InputContractsInvalid("input contract deployment must be canonical UUID")
    if any(_HASH.fullmatch(raw[key]) is None for key in ("bundle_sha256", "body_sha256")):
        raise InputContractsInvalid("invalid input contract digest")
    if raw["body_sha256"] != digest:
        raise InputContractsInvalid("input contract body digest mismatch")
    if remainder.startswith(_PREFIX):
        raise InputContractsInvalid("duplicate input contract prefix")
    return InputEnvelope(body=remainder, **raw)


def require_input_contract(
    body: str,
    declaration: InputContracts,
    *,
    deployment_id: str,
    bundle_sha256: str,
) -> InputEnvelope:
    """Match caller-authoritative deployment identity, never author authority."""
    envelope = parse_input_envelope(body)
    if declaration.interface != INPUT_CONTRACT_ENFORCEMENT or not declaration.required:
        raise InputContractsInvalid("unsupported input contract declaration")
    if envelope.contract not in declaration.accepts:
        raise InputContractsInvalid("input contract unsupported by deployment")
    if envelope.deployment != str(deployment_id) or envelope.bundle_sha256 != bundle_sha256:
        raise InputContractsInvalid("input contract deployment identity changed")
    return envelope
