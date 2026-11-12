"""Wake keys, signed wake material and delivery reservations (ADR 0274).

The wire contract is ``docs/interfaces/triggers/INTERFACE.md``, section
"Declared webhook wake contract (ADR 0274)", subsections "Wake request and
exact signing bytes" and "Key derivation and administration"; the fixed
vectors are ``tests/vectors/hook-wake-signing.json``. ADR 0274 decision 2
explains the shape and decision 4 the reservation.

**Why a key per hook, derived.** A connector that wakes one declared hook
needs a credential that can do exactly that and nothing else. The shared
agent key (``hook_signing.derive``) addresses every hook of the agent through
ordinary ingress, so handing it to a connector would widen that connector's
authority. Deriving the wake key from the installation key, the agent id, the
exact hook name and one stored generation integer keeps the same posture as
``hook_signing``: nothing secret is stored, the current value is reproducible
for the operator, and bumping one generation revokes one hook's key at once.

**Why a separate label and prefix.** ``KEY_LABEL`` separates the derivation
from every other use of the installation key, and ``WAKE_PREFIX`` separates
the signed material from ordinary ``curie.hook.delivery.v2`` and protected
support signing. Reusing the header spellings does not reuse their purpose:
no ordinary or protected signature verifies here, and a wake signature
verifies nowhere else.

**Why the material is framed.** The context ``C`` is compact ASCII JSON for
``[agent_id, hook]`` and its byte length precedes it, so the boundary between
the context and the raw body cannot move under one signature. The digits-only
timestamp and a delivery id without ``.`` fix the two ``.`` delimiters.

**Why the reservation scope is an HMAC.** ``installation_scope`` keeps
reservations of different installations sharing one Valkey apart without
putting the installation key, or anything equivalent to it, in a key name.
A raw SHA256 digest of a key longer than 64 bytes is itself an equivalent HMAC
key, so the scope is an HMAC over a fixed label instead.

Every function here is pure. The route owns the clock and every refusal.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
import uuid

from curie_internal import sandbox_token

from . import hook_signing

# The signed-material prefix. Distinct from ``curie.hook.delivery.v2`` and the
# protected support prefix, so no other route's signature authenticates a wake.
WAKE_PREFIX = b"curie.hook.wake.v1"

# The derivation label. Distinct from ``curie.hook.v1`` (shared agent key) and
# ``curie.hook.source.v1`` (protected source key), so a wake key never equals
# another credential derived from the same installation key.
KEY_LABEL = "curie.hook.wake.key.v1"

# The installation scope label. Distinct from every credential derivation
# label, so the scope can be stored in a key name without being a credential.
SCOPE_LABEL = b"curie.hook.wake.scope.v1"

# The Valkey key namespace for wake reservations, separate from ordinary
# ingress claims (``curie_internal.keyspace.HOOK_KEY_PREFIX``).
DEDUPE_NAMESPACE = "curie:hook-wake:v1"

# The contract bounds the stored generation at 2^63-1 (``1 <= G <= 2^63-1``).
MAX_GENERATION = 2**63 - 1

# Same acceptance window as ordinary ingress, inclusive on both sides.
TOLERANCE_S = hook_signing.TOLERANCE_S

# Extra seconds a reservation outlives its signed timestamp's acceptance
# window. The contract requires the reservation to outlive ``timestamp + 300``
# and permits a skew margin. Acceptance is judged by each API replica's own
# clock while the reservation was sized by the replica that wrote it, so a
# replica whose clock trails the writer's could still accept a replay just
# after an exactly sized reservation expired. Thirty seconds covers ordinary
# NTP-disciplined skew between replicas and Valkey's expiry granularity while
# keeping the extra retention a tenth of the window.
DEDUPE_CLOCK_SKEW_MARGIN_S = 30

# The only request body a wake accepts: exactly the two bytes ``{}``.
MAX_BODY = b"{}"

_TIMESTAMP = re.compile(r"\A[0-9]{1,12}\Z")
_DELIVERY_ID = re.compile(r"\A[A-Za-z0-9_-]{1,128}\Z")
_SIGNATURE = re.compile(r"\Asha256=[0-9a-f]{64}\Z")


def canonical_agent_id(raw: str) -> str | None:
    """``raw`` when it is a canonical lowercase hyphenated UUID, else None.

    ``uuid.UUID`` also accepts braces, a ``urn:uuid:`` prefix, missing hyphens
    and uppercase digits. Each is a second spelling of one agent, and the agent
    id is signed verbatim, so only the one spelling ``str(uuid.UUID(raw))``
    produces is accepted.
    """

    if len(raw) != 36:
        return None
    try:
        parsed = uuid.UUID(raw)
    except ValueError:
        return None
    return raw if str(parsed) == raw else None


def derive_key(api_key: str, *, agent_id: str, hook: str, generation: int) -> str:
    """The wake key for one agent hook at one generation.

    HMAC-SHA256 keyed by the UTF-8 installation key over
    ``curie.hook.wake.key.v1:{agent_id}:{hook}:{generation}``, returned as
    unpadded base64url (``sandbox_token.signature``).

    Raises:
        ValueError: ``generation`` is not an integer in ``[1, MAX_GENERATION]``.
            A bool is refused: ``True`` would sign as ``"True"``, not ``"1"``.
    """

    if type(generation) is not int or not 1 <= generation <= MAX_GENERATION:
        raise ValueError("wake key generation is out of range")
    return sandbox_token.signature(api_key, f"{KEY_LABEL}:{agent_id}:{hook}:{generation}")


def material(*, timestamp: str, delivery_id: str, agent_id: str, hook: str, body: bytes) -> bytes:
    """The exact bytes a wake signature covers, with no final newline."""

    context = json.dumps([agent_id, hook], ensure_ascii=True, separators=(",", ":")).encode("ascii")
    return (
        WAKE_PREFIX
        + b"\n"
        + f"{timestamp}.{delivery_id}.{len(context)}:".encode("ascii")
        + context
        + body
    )


def sign(
    key: str, *, timestamp: str, delivery_id: str, agent_id: str, hook: str, body: bytes
) -> str:
    """The ``X-Curie-Signature-256`` value for one wake under ``key``.

    The HMAC key is the UTF-8 bytes of the derived key string itself, never its
    base64url decoding.
    """

    signing_input = material(
        timestamp=timestamp, delivery_id=delivery_id, agent_id=agent_id, hook=hook, body=body
    )
    return "sha256=" + hmac.new(key.encode("utf-8"), signing_input, hashlib.sha256).hexdigest()


def well_formed(timestamp: str | None, delivery_id: str | None, signature: str | None) -> bool:
    """Whether all three headers are present and have their exact ASCII shape.

    A missing or malformed header gets the same 401 as a wrong key, so this
    only decides whether verification can run at all.
    """

    return (
        timestamp is not None
        and delivery_id is not None
        and signature is not None
        and _TIMESTAMP.fullmatch(timestamp) is not None
        and _DELIVERY_ID.fullmatch(delivery_id) is not None
        and _SIGNATURE.fullmatch(signature) is not None
    )


def in_window(timestamp: str, now: float) -> bool:
    """Whether a well-formed timestamp is within ``TOLERANCE_S`` of ``now``, inclusive."""

    return abs(now - int(timestamp)) <= TOLERANCE_S


def verify(
    key: str,
    *,
    timestamp: str | None,
    delivery_id: str | None,
    agent_id: str,
    hook: str,
    body: bytes,
    header: str | None,
    now: float,
) -> bool:
    """Constant-time check of one wake signature, the timestamp in window.

    Returns:
        True only for well-formed headers, an in-window timestamp and a header
        equal to the signature this key produces over this request.
    """

    if timestamp is None or delivery_id is None or header is None:
        return False
    if not well_formed(timestamp, delivery_id, header) or not in_window(timestamp, now):
        return False
    expected = sign(
        key, timestamp=timestamp, delivery_id=delivery_id, agent_id=agent_id, hook=hook, body=body
    )
    return hmac.compare_digest(expected, header)


def installation_scope(api_key: str) -> str:
    """Lowercase hex HMAC-SHA256 of ``SCOPE_LABEL`` keyed by the installation key."""

    return hmac.new(api_key.encode("utf-8"), SCOPE_LABEL, hashlib.sha256).hexdigest()


def dedupe_key(api_key: str, *, agent_id: str, hook: str, generation: int, delivery_id: str) -> str:
    """The Valkey reservation key of one signed delivery at one generation.

    The key names a delivery id, so it belongs in Valkey only and never in a
    log line, exception message or span attribute.
    """

    scope = installation_scope(api_key)
    return f"{DEDUPE_NAMESPACE}:{scope}:{agent_id}:{hook}:{generation}:{delivery_id}"


def dedupe_ttl_s(timestamp: str, now: float) -> int:
    """Seconds a reservation made at ``now`` must last, margin included.

    ``floor(timestamp + TOLERANCE_S - now) + 1`` is the least integer TTL that
    still covers the inclusive acceptance boundary, including accepted future
    timestamps; ``max(1, ...)`` keeps a reservation made at the boundary alive.
    """

    covering = max(1, math.floor(int(timestamp) + TOLERANCE_S - now) + 1)
    return covering + DEDUPE_CLOCK_SKEW_MARGIN_S
