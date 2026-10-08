"""Authenticated principals for approval resolution (ADR-0106, #1531).

An approval principal is a short-lived HMAC credential accepted only by the
approval resolver.  Its subject and proof kind are claims in the credential,
never values supplied in a resolution body.  ``chat`` principals are signed by
the dispatcher's dedicated attester secret and bind the channel Slack delivered
to one approval.  ``operator`` principals are signed by the platform key and
deliberately carry neither a channel nor an approval binding.

The two keys are never interchangeable.  Callers may inspect the untrusted
``kind`` claim only to select the one verifier that is allowed for that kind;
signature verification then authenticates the complete, strict claim set.
"""

from __future__ import annotations

import hmac
import json
import time
from dataclasses import dataclass
from typing import Literal

from curie_internal.sandbox_token import b64url, b64url_decode, signature

_PREFIX = "apr"
APPROVE_SCOPE = "approval.resolve"
OPERATOR_TOKEN_TTL_SECONDS = 12 * 60 * 60
PrincipalKind = Literal["chat", "operator", "test_driver"]


@dataclass(frozen=True)
class ApprovalPrincipalClaims:
    """The authenticated identity and evidence carried by one credential."""

    subject: str
    kind: PrincipalKind
    actor_channel: str | None
    approval_id: str | None


def mint(
    signing_key: str,
    *,
    subject: str,
    kind: PrincipalKind,
    scope: str,
    exp: int,
    actor_channel: str | None = None,
    approval_id: str | None = None,
) -> str:
    """Mint a strict approval-principal credential.

    A chat attestation is meaningful only with its channel.  An operator token
    carries no channel by design, so it can satisfy only an explicit-user
    approver set rather than manufacturing channel-membership evidence.
    """

    if not isinstance(subject, str) or not subject.strip():
        raise ValueError("approval principal subject must be non-empty")
    if kind not in ("chat", "operator", "test_driver"):
        raise ValueError("approval principal kind must be chat, operator or test_driver")
    if not isinstance(exp, int) or isinstance(exp, bool):
        raise ValueError("approval principal expiry must be an integer")
    if kind in ("chat", "test_driver"):
        if not isinstance(actor_channel, str) or not actor_channel.strip():
            raise ValueError("chat approval principals must attest a channel")
        if not isinstance(approval_id, str) or not approval_id.strip():
            raise ValueError("chat approval principals must bind an approval")
    else:
        if actor_channel is not None:
            raise ValueError("operator approval principals cannot attest a channel")
        if approval_id is not None:
            raise ValueError("operator approval principals cannot bind an approval")

    payload = json.dumps(
        {
            "sub": subject,
            "kind": kind,
            "actor_channel": actor_channel,
            "approval_id": approval_id,
            "scope": scope,
            "exp": exp,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    payload_seg = b64url(payload)
    signing_input = f"{_PREFIX}.{payload_seg}"
    return f"{signing_input}.{signature(signing_key, signing_input)}"


def unverified_kind(token: str) -> PrincipalKind | None:
    """Peek at the untrusted kind solely to select the permitted signing key.

    No caller may use this value as authenticated identity or policy evidence.
    ``verify_claims`` reparses and authenticates the same payload before any
    claim leaves the API's credential boundary.
    """

    try:
        prefix, payload_seg, _sig_seg = token.split(".")
        payload = json.loads(b64url_decode(payload_seg))
    except (ValueError, AttributeError, TypeError, json.JSONDecodeError):
        return None
    if prefix != _PREFIX or not isinstance(payload, dict):
        return None
    kind = payload.get("kind")
    if kind == "chat":
        return "chat"
    if kind == "test_driver":
        return "test_driver"
    if kind == "operator":
        return "operator"
    return None


def verify_claims(
    token: str,
    signing_key: str,
    *,
    scope: str,
    approval_id: str | None = None,
    now: int | None = None,
) -> ApprovalPrincipalClaims | None:
    """Return authenticated claims, or ``None`` for every invalid token."""

    try:
        prefix, payload_seg, sig_seg = token.split(".")
    except (ValueError, AttributeError, TypeError):
        return None
    if prefix != _PREFIX:
        return None
    expected_sig = signature(signing_key, f"{_PREFIX}.{payload_seg}")
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
    if not isinstance(payload, dict) or set(payload) != {
        "sub",
        "kind",
        "actor_channel",
        "approval_id",
        "scope",
        "exp",
    }:
        return None
    if payload.get("scope") != scope:
        return None
    subject = payload.get("sub")
    kind = payload.get("kind")
    actor_channel = payload.get("actor_channel")
    claimed_approval_id = payload.get("approval_id")
    exp = payload.get("exp")
    if not isinstance(subject, str) or not subject.strip():
        return None
    if kind not in ("chat", "operator", "test_driver"):
        return None
    if not isinstance(exp, int) or isinstance(exp, bool):
        return None
    current = now if now is not None else int(time.time())
    if exp <= current or (kind == "test_driver" and exp > current + 60):
        return None
    if kind in ("chat", "test_driver"):
        if not isinstance(actor_channel, str) or not actor_channel.strip():
            return None
        if not isinstance(claimed_approval_id, str) or not claimed_approval_id.strip():
            return None
        if approval_id is None or not hmac.compare_digest(claimed_approval_id, approval_id):
            return None
    else:
        if actor_channel is not None or claimed_approval_id is not None:
            return None
        if approval_id is not None:
            return None
    return ApprovalPrincipalClaims(
        subject=subject,
        kind=kind,
        actor_channel=actor_channel,
        approval_id=claimed_approval_id,
    )
