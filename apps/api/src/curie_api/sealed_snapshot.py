"""The sealed snapshot envelope grammar, as the API reads it (ACTION-EXECUTOR-9).

@spec ACTION-EXECUTOR-9. A restore-capable connector replies with ``prior`` as
an envelope of exactly three keys: ``sealed`` (the constant
``curie.snapshot.v1``), ``kid`` (1 to 64 characters of ``[A-Za-z0-9._-]``) and
``ciphertext`` (standard base64, no line breaks, at most 65536 decoded bytes).
The platform never seals, opens or inspects the ciphertext; it only decides
whether a stored ``prior_state`` has this shape. Anything else, a cleartext
prior state above all, is history and never restorable.

The worker's ``_snapshot`` and the runner's redactor validate the same grammar
in other images; this is the API's reader, used by ``undoable``
(ACTION-EXECUTOR-11).
"""

from __future__ import annotations

import base64
import binascii
import re
from typing import Any, Final

SEALED_CONSTANT: Final = "curie.snapshot.v1"
MAX_CIPHERTEXT_BYTES: Final = 65536
# The shared redaction placeholder prefix. Never inside a valid envelope: the
# standard base64 alphabet excludes its brackets and colon, and a ``kid`` that
# carried it would fail the alphabet below.
REDACTION_PLACEHOLDER_PREFIX: Final = "[REDACTED:"

MAX_VERSION_LENGTH: Final = 256
_PRINTABLE_ASCII = re.compile(r"[\x20-\x7e]+")
_ENVELOPE_KEYS: Final = frozenset({"sealed", "kid", "ciphertext"})
_KID = re.compile(r"[A-Za-z0-9._-]{1,64}")
# Standard alphabet only: the URL-safe ``-`` and ``_`` are refused, as are
# whitespace and line breaks, which ``b64decode`` would otherwise skip.
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
    # The decoded size is bounded from the text length before decoding, so an
    # oversized value is refused without allocating it.
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
    """1 to 256 printable ASCII characters, no placeholder. @spec ACTION-EXECUTOR-9.

    Refused rather than truncated: the observation comparison trusts the stored
    version, so a malformed one must never be stored at all. The worker's
    ``_snapshot`` applies the same rule before it reports one.
    """

    return (
        isinstance(value, str)
        and len(value) <= MAX_VERSION_LENGTH
        and _PRINTABLE_ASCII.fullmatch(value) is not None
        and REDACTION_PLACEHOLDER_PREFIX not in value
    )
