"""The sealed reply convention, as the worker records it (ACTION-EXECUTOR-9).

@spec ACTION-EXECUTOR-9. A restore-capable write connector replies with
``prior``, ``version`` and ``target``. ``prior`` is an envelope of exactly the
keys ``sealed`` (the constant ``curie.snapshot.v1``), ``kid`` (1 to 64
characters of ``[A-Za-z0-9._-]``) and ``ciphertext`` (standard base64, no line
breaks, 1 to 65536 decoded bytes); ``version`` is 1 to 256 printable ASCII
characters. The worker never seals, opens or inspects the ciphertext.

The API (``curie_api.sealed_snapshot``) and the runner's redactor validate the
same grammar in other images; all three read
``tests/vectors/sealed-snapshot-reply.json`` (the parity seam of
ACTION-EXECUTOR-24).
"""

from __future__ import annotations

import base64
import binascii
import re
from typing import Any, Final

SEALED_CONSTANT: Final = "curie.snapshot.v1"
MAX_CIPHERTEXT_BYTES: Final = 65536
MAX_VERSION_LENGTH: Final = 256
# The shared redaction placeholder prefix. A replay input carrying it holds a
# value the runner replaced, so a restore would write the placeholder.
REDACTION_PLACEHOLDER_PREFIX: Final = "[REDACTED:"

_ENVELOPE_KEYS: Final = frozenset({"sealed", "kid", "ciphertext"})
_PRINTABLE_ASCII = re.compile(r"[\x20-\x7e]*")
_KID = re.compile(r"[A-Za-z0-9._-]{1,64}")
# Standard alphabet with padding only: ``b64decode`` would otherwise skip line
# breaks and, without ``validate``, the URL-safe ``-`` and ``_``.
_STANDARD_BASE64 = re.compile(r"(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?")


def is_sealed_envelope(value: Any) -> bool:
    """Whether ``value`` is a valid sealed envelope. @spec ACTION-EXECUTOR-9."""

    if not isinstance(value, dict) or set(value) != _ENVELOPE_KEYS:
        return False
    if value["sealed"] != SEALED_CONSTANT:
        return False
    kid = value["kid"]
    if not isinstance(kid, str) or _KID.fullmatch(kid) is None:
        return False
    ciphertext = value["ciphertext"]
    if not isinstance(ciphertext, str) or not ciphertext:
        return False
    if REDACTION_PLACEHOLDER_PREFIX in ciphertext:
        return False
    # Bounded from the text length before decoding, so an oversized value is
    # refused without allocating it.
    if len(ciphertext) > 4 * ((MAX_CIPHERTEXT_BYTES + 2) // 3):
        return False
    if _STANDARD_BASE64.fullmatch(ciphertext) is None:
        return False
    try:
        decoded = base64.b64decode(ciphertext, validate=True)
    except (binascii.Error, ValueError):
        return False
    return 0 < len(decoded) <= MAX_CIPHERTEXT_BYTES


def is_post_version(value: Any) -> bool:
    """1 to 256 printable ASCII characters (0x20 to 0x7e), with no placeholder.

    @spec ACTION-EXECUTOR-9. Refused, never truncated: a cut version would be
    compared with ``observe_version`` as if the connector had written it. The
    API's ``ActionComplete`` applies the same rule (HTTP 422).
    """

    return (
        isinstance(value, str)
        and 0 < len(value) <= MAX_VERSION_LENGTH
        and _PRINTABLE_ASCII.fullmatch(value) is not None
        and REDACTION_PLACEHOLDER_PREFIX not in value
    )


def carries_placeholder(value: Any) -> bool:
    """Whether any string anywhere inside ``value`` (keys included) holds the prefix."""

    if isinstance(value, str):
        return REDACTION_PLACEHOLDER_PREFIX in value
    if isinstance(value, dict):
        return any(carries_placeholder(k) or carries_placeholder(v) for k, v in value.items())
    if isinstance(value, list):
        return any(carries_placeholder(item) for item in value)
    return False
