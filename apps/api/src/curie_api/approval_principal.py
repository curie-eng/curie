"""Authenticated principals for approval resolution (ADR-0106, #1531).

An approval principal is a short-lived HMAC credential, signed with the
platform API key but accepted only by the approval resolver.  Its subject and
proof kind are claims in the credential, never values supplied in a resolution
body.  ``chat`` principals also attest the channel Slack delivered with the
interaction; ``operator`` principals deliberately cannot assert a channel.
"""

from __future__ import annotations

import hmac
import json
import time
from dataclasses import dataclass
from typing import Literal

from .sandbox_token import _b64url, _b64url_decode, _signature

_PREFIX = "apr"
APPROVE_SCOPE = "approval.resolve"
OPERATOR_TOKEN_TTL_SECONDS = 12 * 60 * 60
PrincipalKind = Literal["chat", "operator"]


@dataclass(frozen=True)
class ApprovalPrincipalClaims:
    """The authenticated identity and evidence carried by one credential."""

    subject: str
    kind: PrincipalKind
    actor_channel: str | None


def mint(
    api_key: str,
    *,
    subject: str,
    kind: PrincipalKind,
    scope: str,
    exp: int,
    actor_channel: str | None = None,
) -> str:
    """Mint a strict approval-principal credential.

    A chat attestation is meaningful only with its channel.  An operator token
    carries no channel by design, so it can satisfy only an explicit-user
    approver set rather than manufacturing channel-membership evidence.
    """

    if not isinstance(subject, str) or not subject.strip():
        raise ValueError("approval principal subject must be non-empty")
    if kind not in ("chat", "operator"):
        raise ValueError("approval principal kind must be chat or operator")
    if not isinstance(exp, int) or isinstance(exp, bool):
        raise ValueError("approval principal expiry must be an integer")
    if kind == "chat":
        if not isinstance(actor_channel, str) or not actor_channel.strip():
            raise ValueError("chat approval principals must attest a channel")
    elif actor_channel is not None:
        raise ValueError("operator approval principals cannot attest a channel")

    payload = json.dumps(
        {
            "sub": subject,
            "kind": kind,
            "actor_channel": actor_channel,
            "scope": scope,
            "exp": exp,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    payload_seg = _b64url(payload)
    signing_input = f"{_PREFIX}.{payload_seg}"
    return f"{signing_input}.{_signature(api_key, signing_input)}"


def verify_claims(
    token: str,
    api_key: str,
    *,
    scope: str,
    now: int | None = None,
) -> ApprovalPrincipalClaims | None:
    """Return authenticated claims, or ``None`` for every invalid token."""

    try:
        prefix, payload_seg, sig_seg = token.split(".")
    except (ValueError, AttributeError, TypeError):
        return None
    if prefix != _PREFIX:
        return None
    expected_sig = _signature(api_key, f"{_PREFIX}.{payload_seg}")
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
    if not isinstance(payload, dict) or payload.get("scope") != scope:
        return None
    subject = payload.get("sub")
    kind = payload.get("kind")
    actor_channel = payload.get("actor_channel")
    exp = payload.get("exp")
    if not isinstance(subject, str) or not subject.strip():
        return None
    if kind not in ("chat", "operator"):
        return None
    if not isinstance(exp, int) or isinstance(exp, bool):
        return None
    current = now if now is not None else int(time.time())
    if exp <= current:
        return None
    if kind == "chat":
        if not isinstance(actor_channel, str) or not actor_channel.strip():
            return None
    elif actor_channel is not None:
        return None
    return ApprovalPrincipalClaims(
        subject=subject,
        kind=kind,
        actor_channel=actor_channel,
    )
