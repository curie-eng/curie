"""Signing and verification for inbound hook deliveries (#269).

ADR-0079 decision 1 calls for a "per-agent hook secret" verified with the same
HMAC pattern the GitHub webhook already uses. This module answers where that
secret comes from, and the answer is that it is DERIVED rather than stored --
which is also why the module is not named for a secret it never holds.

**Why derived.** The obvious shape -- a secret column on ``agents`` -- puts a
credential that a third party also holds in plaintext in the control-plane
database, and this API has no encryption at rest (``agents.secrets`` is plain
JSONB). Deriving it with HMAC keyed by the platform ``api_key`` keeps nothing
secret in a row, inherits the production guard that already refuses to boot with
a default ``api_key``, and is reproducible, so an operator can be shown the
current value whenever they need to paste it into the upstream system.

**Why a rotation counter is still stored.** Derivation alone would make the only
way to revoke one compromised hook a rotation of the platform key, which revokes
every credential the platform has minted. ``agents.hook_generation`` is an
ordinary integer, not a secret: bumping it changes that one agent's derived
secret and nothing else. Storing the counter rather than the secret is the whole
trick.

This is HMAC used as a key-derivation step, which is what ``sandbox_token`` and
``channel_token`` already do with the same key; the primitives are imported from
the first of them rather than copied.
"""

from __future__ import annotations

import hashlib
import hmac

from .sandbox_token import _signature

# The header an upstream presents its signature in. Named for this platform
# rather than borrowing GitHub's ``X-Hub-Signature-256``: a hook source is any
# system, and reusing GitHub's spelling would suggest a GitHub payload shape.
SIGNATURE_HEADER = "X-Curie-Signature-256"

# The label that separates this derivation from every other use of ``api_key``.
# Without it a hook secret and some future token derived from the same key over
# the same inputs would be the same bytes, and holding one would grant the other.
_LABEL = "curie.hook.v1"


def derive(api_key: str, *, agent_id: str, generation: int) -> str:
    """The shared secret for one agent's hooks at one rotation generation.

    Args:
        api_key: The platform's shared signing key.
        agent_id: The agent's id, as a string.
        generation: The agent's ``hook_generation``; bumping it rotates.

    Returns:
        The secret, base64url-encoded, safe to hand to an operator verbatim.
    """

    return _signature(api_key, f"{_LABEL}:{agent_id}:{generation}")


def sign(secret: str, body: bytes) -> str:
    """The ``sha256=`` header value for ``body`` under ``secret``."""

    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def verify(secret: str, body: bytes, header: str | None) -> bool:
    """Constant-time check of the upstream's signature over the RAW body.

    The raw bytes are signed, never a re-serialization: any parse-then-dump round
    trip can change whitespace or key order, and a signature checked against
    re-serialized bytes either rejects honest deliveries or, worse, is quietly
    dropped as unworkable.

    Mirrors ``gitflow.verify_signature`` deliberately, including the ``sha256=``
    prefix, because an operator configuring a hook has almost certainly
    configured a GitHub webhook before and the two should not differ in shape for
    no reason.

    Args:
        secret: The derived per-agent secret.
        body: The exact request body bytes.
        header: The presented signature header, or None when absent.

    Returns:
        True only for a well-formed header that matches.
    """

    if not header or not header.startswith("sha256="):
        return False
    return hmac.compare_digest(sign(secret, body), header)
