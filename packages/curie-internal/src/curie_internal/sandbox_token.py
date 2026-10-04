"""Scoped, least-privilege sandbox state token (ADR-0033, issue #410).

The worker mints these when it builds a sandbox's boot env (and, for memory
writes, once per turn; ADR-0188) and forwards them into the sandbox in place
of the raw platform API key, so the runner can rehydrate memory and transcript
without holding a resolve-capable, platform-wide credential. The token is an
HMAC-SHA256 signature over its own claims, keyed by the shared ``api_key``: it
authenticates only against the state router, only for the one agent it names, and
can never be presented as the platform key (it is never equal to ``api_key``).

Beyond the three core claims (``agent``, ``scope``, ``exp``) a token may carry
extra claims (ADR-0188: ``binding``, ``memory``, ``sender``, ``turn``) that narrow
what the api lets it do. ``verify`` checks only the core claims; ``decode``
returns the whole verified payload so the api can read the rest.

API verification and worker minting import this single implementation from
the shared internal package. The frozen protocol packages remain independent.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from collections.abc import Mapping
from typing import Any

_PREFIX = "sbx"


def b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def b64url_decode(seg: str) -> bytes:
    pad = "=" * (-len(seg) % 4)
    return base64.urlsafe_b64decode(seg + pad)


def signature(api_key: str, signing_input: str) -> str:
    digest = hmac.new(
        api_key.encode(), signing_input.encode(), hashlib.sha256
    ).digest()
    return b64url(digest)


_RESERVED_CLAIMS = frozenset({"agent", "scope", "exp"})


def mint(
    api_key: str,
    *,
    agent: str,
    scope: str,
    exp: int,
    claims: Mapping[str, str | None] | None = None,
) -> str:
    """Mint a signed token binding ``agent`` and ``scope`` with absolute expiry
    ``exp`` (unix seconds). Deterministic: the caller supplies ``exp`` so the wire
    form is a pure function of its inputs.

    ``claims`` adds extra string-or-null claims to the payload. It may not name a
    core claim (``agent``, ``scope``, ``exp``): ValueError. With ``claims=None``
    the payload is exactly the three-claim form."""

    body: dict[str, Any] = {"agent": agent, "scope": scope, "exp": exp}
    if claims is not None:
        clash = _RESERVED_CLAIMS.intersection(claims)
        if clash:
            raise ValueError(f"claims may not override {sorted(clash)}")
        body.update(claims)
    payload = json.dumps(
        body,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    payload_seg = b64url(payload)
    signing_input = f"{_PREFIX}.{payload_seg}"
    return f"{signing_input}.{signature(api_key, signing_input)}"


def decode(
    token: str, api_key: str, *, agent: str, scope: str, now: int | None = None
) -> dict[str, Any] | None:
    """The verified payload of ``token``, or None. None (never an exception) on
    any malformed, tampered, wrong-key, wrong-claim, or expired input; a payload
    only when ``token`` is signed by ``api_key``, names exactly this ``agent``
    and ``scope``, and has not expired."""

    try:
        prefix, payload_seg, sig_seg = token.split(".")
    except (ValueError, AttributeError):
        return None
    if prefix != _PREFIX:
        return None
    expected_sig = signature(api_key, f"{_PREFIX}.{payload_seg}")
    try:
        signature_ok = hmac.compare_digest(sig_seg, expected_sig)
    except TypeError:
        return None
    if not signature_ok:
        return None
    try:
        payload = json.loads(b64url_decode(payload_seg))
    except (ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("agent") != agent or payload.get("scope") != scope:
        return None
    exp = payload.get("exp")
    if not isinstance(exp, int) or isinstance(exp, bool):
        return None
    current = now if now is not None else int(time.time())
    if exp <= current:
        return None
    return payload


def verify(
    token: str, api_key: str, *, agent: str, scope: str, now: int | None = None
) -> bool:
    """True only when ``token`` is a well-formed token signed by ``api_key`` that
    names exactly this ``agent`` and ``scope`` and has not expired. Returns False
    (never raises) on any malformed, tampered, wrong-key, wrong-claim, or expired
    input, so the caller can treat a failure as a plain 401. Extra claims are
    ignored here; ``decode`` returns them."""

    return decode(token, api_key, agent=agent, scope=scope, now=now) is not None
