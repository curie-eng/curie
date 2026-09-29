"""Check a connector caller token (ADR-0168 decision 7).

The verifying half of ``curie_worker.caller_token``. Both halves read the wire
frozen in ``tests/vectors/connector-caller-token.json``. The signature is
checked over the received ``cct.<payload>`` text before anything is parsed, and
nothing here re-serializes the claims.
"""

from __future__ import annotations

import base64
import binascii
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass

from nacl.exceptions import BadSignatureError
from nacl.signing import VerifyKey

PREFIX = "cct"
HEADER = "X-Curie-Caller"

MISSING = "missing"
INVALID = "invalid"
EXPIRED = "expired"
NOT_ADMITTED = "not_admitted"
REFUSALS = (MISSING, INVALID, EXPIRED, NOT_ADMITTED)

_KEY_BYTES = 32
_SIGNATURE_BYTES = 64
# Far above any minted token; bounds the work one header can ask for.
_MAX_TOKEN_CHARS = 4096
_SEGMENT = re.compile(r"[A-Za-z0-9_-]+")
_CLAIMS = frozenset({"agent", "exp"})


@dataclass(frozen=True)
class Decision:
    """Who a request claims to be from, and why it is refused when it is."""

    agent: str | None
    refusal: str | None

    @property
    def admitted(self) -> bool:
        return self.refusal is None


def public_key(text: str) -> VerifyKey:
    """The key a standard-base64 32-byte Ed25519 public key names."""

    try:
        raw = base64.b64decode(text.strip(), validate=True)
    except (binascii.Error, ValueError):
        raise ValueError("a caller public key is not standard base64") from None
    if len(raw) != _KEY_BYTES:
        raise ValueError(
            f"a caller public key decodes to {len(raw)} bytes; an Ed25519 public key "
            f"is {_KEY_BYTES}"
        )
    return VerifyKey(raw)


def _segment(text: str) -> bytes | None:
    if not _SEGMENT.fullmatch(text):
        return None
    try:
        raw = base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))
    except (binascii.Error, ValueError):
        return None
    # One spelling per byte string: a segment with stray trailing bits decodes
    # to the same bytes as the canonical one, and is refused.
    if base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii") != text:
        return None
    return raw


def _signed_by(keys: Sequence[VerifyKey], message: bytes, signature: bytes) -> bool:
    for key in keys:
        try:
            key.verify(message, signature)
        except BadSignatureError:
            continue
        return True
    return False


def claims(keys: Sequence[VerifyKey], token: str) -> tuple[str, int] | None:
    """The ``(agent, exp)`` a token carries when one of ``keys`` signed it."""

    if len(token) > _MAX_TOKEN_CHARS or not token.isascii():
        return None
    parts = token.split(".")
    if len(parts) != 3 or parts[0] != PREFIX:
        return None
    payload = _segment(parts[1])
    signature = _segment(parts[2])
    if payload is None or signature is None or len(signature) != _SIGNATURE_BYTES:
        return None
    if not _signed_by(keys, f"{parts[0]}.{parts[1]}".encode("ascii"), signature):
        return None
    try:
        parsed = json.loads(payload)
    except ValueError:
        return None
    if not isinstance(parsed, dict) or set(parsed) != _CLAIMS:
        return None
    agent, exp = parsed["agent"], parsed["exp"]
    if not isinstance(agent, str) or not agent or type(exp) is not int:
        return None
    return agent, exp


def decide(
    keys: Sequence[VerifyKey], token: str | None, *, admits: frozenset[str], now: int
) -> Decision:
    """Admit ``token`` or name the refusal. Never raises on its input."""

    if not token:
        return Decision(agent=None, refusal=MISSING)
    carried = claims(keys, token)
    if carried is None:
        return Decision(agent=None, refusal=INVALID)
    agent, exp = carried
    # Accept is `exp > now`, the comparison `sandbox_token.verify` makes.
    if exp <= now:
        return Decision(agent=agent, refusal=EXPIRED)
    # Exact: no case folding and no normalization, so a stored name outside
    # the bundle shape is admitted only through `self`.
    if agent not in admits:
        return Decision(agent=agent, refusal=NOT_ADMITTED)
    return Decision(agent=agent, refusal=None)
