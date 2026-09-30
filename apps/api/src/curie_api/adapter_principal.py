"""Scoped credential for a channel adapter principal (ADR-0154, #2806).

A channel adapter is a principal: it has a subject (the adapter's name) and a
set of binding rows it serves. This credential is what it presents instead of
the platform key, and it carries exactly the three scopes an adapter needs --
mint a ``chn`` token for a binding it serves, list approvals routed to those
bindings, and resolve them on behalf of a sender it authenticated.

Issuance is administrative (``require_platform_key``), so the token is signed
with the platform key, like an ``operator`` approval principal. The prefix
``adp`` is its own, so an ``apr``, ``chn`` or sandbox token never verifies as an
adapter credential and an adapter credential never verifies as any of them.

The claim set is strict: an unknown key, a scope list other than exactly the
three, or an empty binding set fails verification rather than being ignored.
"""

from __future__ import annotations

import hmac
import json
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass

from .sandbox_token import _b64url, _b64url_decode, _signature

_PREFIX = "adp"
_KIND = "adapter"

SCOPE_CHANNELS_TOKEN = "channels:token"
SCOPE_APPROVALS_READ = "approvals:read"
SCOPE_APPROVALS_RESOLVE = "approvals:resolve"
SCOPES: tuple[str, ...] = tuple(
    sorted((SCOPE_APPROVALS_READ, SCOPE_APPROVALS_RESOLVE, SCOPE_CHANNELS_TOKEN))
)

DEFAULT_TTL_SECONDS = 24 * 60 * 60
# The same ceiling a `chn` token has: the adapter rotates before expiry, and an
# unbounded lifetime would remove the only revocation stand-in it carries.
MAX_TTL_SECONDS = 7 * 24 * 60 * 60

_CLAIM_KEYS = frozenset({"sub", "kind", "bindings", "scopes", "exp"})


@dataclass(frozen=True)
class AdapterClaims:
    """What an authentic ``adp`` token asserts: one adapter, its bindings."""

    subject: str
    bindings: frozenset[uuid.UUID]
    exp: int


def mint(
    signing_key: str,
    *,
    subject: str,
    bindings: Iterable[uuid.UUID | str],
    exp: int,
) -> str:
    """Mint an adapter credential over ``bindings`` with absolute expiry ``exp``.

    Deterministic: bindings are normalized and sorted, so the wire form is a
    pure function of its inputs.
    """

    if not isinstance(subject, str) or not subject.strip():
        raise ValueError("adapter principal subject must be non-empty")
    if not isinstance(exp, int) or isinstance(exp, bool):
        raise ValueError("adapter principal expiry must be an integer")
    normalized = sorted({str(uuid.UUID(str(binding))) for binding in bindings})
    if not normalized:
        raise ValueError("adapter principal must serve at least one binding")

    payload = json.dumps(
        {
            "sub": subject,
            "kind": _KIND,
            "bindings": normalized,
            "scopes": list(SCOPES),
            "exp": exp,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    signing_input = f"{_PREFIX}.{_b64url(payload)}"
    return f"{signing_input}.{_signature(signing_key, signing_input)}"


def _parse_bindings(value: object) -> frozenset[uuid.UUID] | None:
    """The binding claim, or None unless it is a non-empty sorted list of
    distinct canonical UUID strings (exactly what ``mint`` writes)."""

    if not isinstance(value, list) or not value:
        return None
    parsed: list[uuid.UUID] = []
    for item in value:
        if not isinstance(item, str):
            return None
        try:
            binding = uuid.UUID(item)
        except ValueError:
            return None
        if str(binding) != item:
            return None
        parsed.append(binding)
    if [str(b) for b in parsed] != sorted({str(b) for b in parsed}):
        return None
    return frozenset(parsed)


def verify(
    token: str,
    signing_key: str,
    *,
    scope: str,
    now: int | None = None,
) -> AdapterClaims | None:
    """The claims of an authentic, unexpired adapter token carrying ``scope``.

    Returns None (never raises) on any malformed, tampered, wrong-key,
    wrong-shape, wrong-scope or expired input, so a caller answers 401 rather
    than 500.
    """

    try:
        prefix, payload_seg, sig_seg = token.split(".")
    except (ValueError, AttributeError, TypeError):
        return None
    if prefix != _PREFIX:
        return None
    expected_sig = _signature(signing_key, f"{_PREFIX}.{payload_seg}")
    try:
        signature_ok = hmac.compare_digest(sig_seg, expected_sig)
    except TypeError:
        return None
    if not signature_ok:
        return None
    try:
        payload = json.loads(_b64url_decode(payload_seg))
    except (ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict) or set(payload) != _CLAIM_KEYS:
        return None
    subject = payload["sub"]
    if not isinstance(subject, str) or not subject.strip():
        return None
    if payload["kind"] != _KIND or payload["scopes"] != list(SCOPES):
        return None
    if scope not in SCOPES:
        return None
    bindings = _parse_bindings(payload["bindings"])
    if bindings is None:
        return None
    # `bool` is an `int` in Python, so it is excluded explicitly.
    exp = payload["exp"]
    if not isinstance(exp, int) or isinstance(exp, bool):
        return None
    current = now if now is not None else int(time.time())
    if exp <= current:
        return None
    return AdapterClaims(subject=subject, bindings=bindings, exp=exp)
