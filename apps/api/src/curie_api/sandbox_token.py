"""Scoped, least-privilege sandbox state token (ADR-0033, issue #410).

The worker mints one of these per turn and forwards it into the sandbox in place
of the raw platform API key, so the runner can rehydrate memory and transcript
without holding a resolve-capable, platform-wide credential. The token is an
HMAC-SHA256 signature over its own claims, keyed by the shared ``api_key``: it
authenticates only against the state router, only for the one agent it names, and
can never be presented as the platform key (it is never equal to ``api_key``).

This module is duplicated byte-identically in ``apps/api`` and ``apps/worker``
(they share no internal library and the contract packages are frozen); a
committed byte-identical-source test keeps the two copies from drifting.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time

_PREFIX = "sbx"


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(seg: str) -> bytes:
    pad = "=" * (-len(seg) % 4)
    return base64.urlsafe_b64decode(seg + pad)


def _signature(api_key: str, signing_input: str) -> str:
    digest = hmac.new(api_key.encode(), signing_input.encode(), hashlib.sha256).digest()
    return _b64url(digest)


def mint(api_key: str, *, agent: str, scope: str, exp: int) -> str:
    """Mint a signed token binding ``agent`` and ``scope`` with absolute expiry
    ``exp`` (unix seconds). Deterministic: the caller supplies ``exp`` so the wire
    form is a pure function of its inputs."""

    payload = json.dumps(
        {"agent": agent, "scope": scope, "exp": exp},
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    payload_seg = _b64url(payload)
    signing_input = f"{_PREFIX}.{payload_seg}"
    return f"{signing_input}.{_signature(api_key, signing_input)}"


def verify(token: str, api_key: str, *, agent: str, scope: str, now: int | None = None) -> bool:
    """True only when ``token`` is a well-formed token signed by ``api_key`` that
    names exactly this ``agent`` and ``scope`` and has not expired. Returns False
    (never raises) on any malformed, tampered, wrong-key, wrong-claim, or expired
    input, so the caller can treat a failure as a plain 401."""

    try:
        prefix, payload_seg, sig_seg = token.split(".")
    except (ValueError, AttributeError):
        return False
    if prefix != _PREFIX:
        return False
    expected_sig = _signature(api_key, f"{_PREFIX}.{payload_seg}")
    try:
        signature_ok = hmac.compare_digest(sig_seg, expected_sig)
    except TypeError:
        return False
    if not signature_ok:
        return False
    try:
        payload = json.loads(_b64url_decode(payload_seg))
    except (ValueError, json.JSONDecodeError):
        return False
    if not isinstance(payload, dict):
        return False
    if payload.get("agent") != agent or payload.get("scope") != scope:
        return False
    exp = payload.get("exp")
    if not isinstance(exp, int) or isinstance(exp, bool):
        return False
    current = now if now is not None else int(time.time())
    return exp > current
