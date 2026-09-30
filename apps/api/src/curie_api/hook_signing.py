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

**What a delivery signature covers (#3554).** The signed material is the
upstream's timestamp, its delivery id and the raw body, in that order:
``f"{timestamp}.{delivery_id}.".encode() + body``. A signature over the body
alone left the delivery id, which is the deduplication key, outside the
authenticated bytes, so a captured signed body resent under a fresh delivery id
was indistinguishable from a new delivery and ran the agent again. Binding the id
into the signature ties one signature to one dedupe key; binding a timestamp and
refusing any outside ``TOLERANCE_S`` bounds how long a captured request is worth
anything at all. There is one scheme and no body-only fallback: a verifier that
still accepted the old shape would reopen exactly what this closes.

The ``.`` is the delimiter, so both boundaries must be fixed for the material to
parse one way only. A digits-only timestamp fixes the first. A delivery id that
may contain ``.`` would leave the second movable, letting bytes shift between the
id and the body under one signature. A delivery id therefore may not contain
``.``; ``sign`` refuses one and ``verify`` rejects one.
"""

from __future__ import annotations

import hashlib
import hmac
import time

from .sandbox_token import _signature

# The header an upstream presents its signature in. Named for this platform
# rather than borrowing GitHub's ``X-Hub-Signature-256``: a hook source is any
# system, and reusing GitHub's spelling would suggest a GitHub payload shape.
SIGNATURE_HEADER = "X-Curie-Signature-256"

# The header an upstream names its delivery with, and the header carrying the
# integer unix-seconds time it signed at. Both are part of the signed material,
# so they live here beside the signature header where signers and the verifier
# share one spelling.
DELIVERY_HEADER = "X-Curie-Delivery-Id"
TIMESTAMP_HEADER = "X-Curie-Timestamp"

# How far, in seconds, a delivery's signed timestamp may sit from the server's
# clock in either direction. Five minutes absorbs ordinary clock skew and a
# retrying upstream's backoff without leaving a captured request replayable for
# long. Delivery receipts never expire once enqueued (``delivery``), so a
# retry inside the window that reuses its delivery id is still deduplicated.
TOLERANCE_S = 300

# The longest timestamp string ``verify`` will convert. Twelve digits covers unix
# seconds for tens of thousands of years; anything longer cannot be in the window,
# and an unbounded digit string would make ``int()`` or the float subtraction
# raise (a 500) instead of refusing with the uniform 401.
MAX_TIMESTAMP_DIGITS = 12

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


def _material(timestamp: str, delivery_id: str, body: bytes) -> bytes:
    """The exact bytes a delivery signature is computed over."""

    return f"{timestamp}.{delivery_id}.".encode() + body


def sign(secret: str, *, timestamp: str, delivery_id: str, body: bytes) -> str:
    """The ``sha256=`` header value for one delivery under ``secret``.

    Args:
        secret: The derived per-agent secret.
        timestamp: The integer unix-seconds time sent in ``TIMESTAMP_HEADER``.
        delivery_id: The id sent in ``DELIVERY_HEADER``.
        body: The exact request body bytes.

    Raises:
        ValueError: ``delivery_id`` contains ``.``, the material delimiter, which
            would let bytes shift between the id and the body.
    """

    if "." in delivery_id:
        raise ValueError("a hook delivery id may not contain '.'")
    digest = hmac.new(secret.encode(), _material(timestamp, delivery_id, body), hashlib.sha256)
    return "sha256=" + digest.hexdigest()


def verify(
    secret: str,
    *,
    timestamp: str | None,
    delivery_id: str,
    body: bytes,
    header: str | None,
    now: float | None = None,
) -> bool:
    """Constant-time check of the upstream's signature over one delivery.

    The signature covers the timestamp, the delivery id and the RAW body. The
    delivery id is signed because it is the deduplication key: left unsigned, a
    captured body could be resent under a new id and accepted as a new delivery.
    The timestamp is signed, and refused outside ``TOLERANCE_S`` of ``now``, so a
    captured request stops being usable at all once the window passes. A
    delivery id containing ``.`` is refused before any HMAC is computed: the dot
    is the material delimiter, so such an id would make the id and body boundary
    ambiguous.

    The raw bytes are signed, never a re-serialization: any parse-then-dump round
    trip can change whitespace or key order, and a signature checked against
    re-serialized bytes either rejects honest deliveries or, worse, is quietly
    dropped as unworkable.

    Keeps the ``sha256=`` prefix ``gitflow.verify_signature`` uses, because an
    operator configuring a hook has almost certainly configured a GitHub webhook
    before and the header should not differ in shape for no reason.

    Args:
        secret: The derived per-agent secret.
        timestamp: The presented ``TIMESTAMP_HEADER``, or None when absent.
        delivery_id: The presented delivery id, or ``""`` when absent; the
            caller still refuses a missing id after this check passes.
        body: The exact request body bytes.
        header: The presented signature header, or None when absent.
        now: The current unix time; injectable for tests, defaults to the clock.

    Returns:
        True only for a well-formed, in-window timestamp and a matching header.
    """

    if not header or not header.startswith("sha256="):
        return False
    # ASCII digits only: ``int()`` alone would also take a sign, whitespace,
    # underscores and non-ASCII digits, each a second spelling of one time.
    if not timestamp or not (timestamp.isascii() and timestamp.isdigit()):
        return False
    if len(timestamp) > MAX_TIMESTAMP_DIGITS:
        return False
    if "." in delivery_id:
        return False
    current = time.time() if now is None else now
    if abs(current - int(timestamp)) > TOLERANCE_S:
        return False
    expected = sign(secret, timestamp=timestamp, delivery_id=delivery_id, body=body)
    return hmac.compare_digest(expected, header)
