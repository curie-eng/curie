"""The caller token a sandbox presents to its hosted connectors (ADR-0168 decision 7).

The worker signs one per boot, naming the agent the binding resolved, and the
runner sends it to each hosted connector in the ``X-Curie-Caller`` header. The
wire is frozen in ``tests/vectors/connector-caller-token.json``.

Ed25519 rather than the HMAC ``sandbox_token`` uses, because whatever checks
this token sits beside a third-party connector: it can hold a public key, and
it must never hold a key that can mint.
"""

from __future__ import annotations

import base64
import binascii
import json

from nacl.signing import SigningKey

PREFIX = "cct"

_SEED_BYTES = 32


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def signing_key(text: str) -> SigningKey:
    """The key a standard-base64 32-byte Ed25519 seed names.

    Raises ``ValueError`` without the text, so a refusal logged at boot does
    not carry the key it refused.
    """

    try:
        seed = base64.b64decode(text.strip(), validate=True)
    except (binascii.Error, ValueError):
        raise ValueError("the caller signing key is not standard base64") from None
    if len(seed) != _SEED_BYTES:
        raise ValueError(
            f"the caller signing key decodes to {len(seed)} bytes; an Ed25519 seed is "
            f"{_SEED_BYTES}"
        )
    return SigningKey(seed)


def mint(signing_key_text: str, *, agent: str, exp: int) -> str:
    """Sign ``agent`` with absolute expiry ``exp`` (unix seconds).

    Deterministic: Ed25519 signatures are, and the caller supplies ``exp``.
    """

    payload = json.dumps(
        {"agent": agent, "exp": exp}, separators=(",", ":"), sort_keys=True
    ).encode("ascii")
    signing_input = f"{PREFIX}.{_b64url(payload)}"
    signature = signing_key(signing_key_text).sign(signing_input.encode("ascii")).signature
    return f"{signing_input}.{_b64url(signature)}"
