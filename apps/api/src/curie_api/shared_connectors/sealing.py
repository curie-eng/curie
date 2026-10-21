"""Sealing to a shared application's recipient key (ADR 0239, the ADR 0094 scheme).

A libsodium sealed box (X25519 + XSalsa20-Poly1305) to the application's
registered X25519 public key, the primitive the worker already opens with
PyNaCl. The API seals only what it generates itself (a bundled role's DSN);
imported credentials arrive already sealed by the CLI, so provider plaintext
never reaches the API.

A recipient ID is the lowercase hex SHA-256 of the raw 32-byte public key, so
the ID names exactly one key and a registration can prove which key it carries.
Every decode here is strict, and no error message carries the value it refused.
"""

from __future__ import annotations

import base64
import binascii
import hashlib

from nacl.exceptions import CryptoError
from nacl.public import PublicKey, SealedBox

ALGORITHM = "x25519-sealed-box"
PUBLIC_KEY_BYTES = 32
# A sealed box prepends a 32-byte ephemeral public key and a 16-byte MAC.
SEAL_OVERHEAD_BYTES = 48
MIN_CIPHERTEXT_BYTES = SEAL_OVERHEAD_BYTES + 1
MAX_CIPHERTEXT_BYTES = 65536
# The longest standard base64 text a maximal ciphertext encodes to.
MAX_CIPHERTEXT_BASE64 = 4 * ((MAX_CIPHERTEXT_BYTES + 2) // 3)


def strict_b64decode(value: str) -> bytes:
    """Decode standard padded base64, refusing whitespace and any other alphabet."""

    try:
        return binascii.a2b_base64(value.encode("ascii"), strict_mode=True)
    except (UnicodeEncodeError, binascii.Error):
        raise ValueError("expected standard padded base64") from None


def b64encode(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def public_key_bytes(value: str) -> bytes:
    """The raw key a registration carries: 32 bytes libsodium can seal to.

    The all-zero point and every other small-order point are refused here, by
    a trial seal, so a key that would later fail DSN provisioning or rotation
    is never stored. The refusal names neither the key nor libsodium's error.
    """

    raw = strict_b64decode(value)
    if len(raw) != PUBLIC_KEY_BYTES or raw == bytes(PUBLIC_KEY_BYTES):
        raise ValueError("expected a 32-byte X25519 public key")
    try:
        SealedBox(PublicKey(raw)).encrypt(b"\x00")
    except (CryptoError, ValueError, TypeError):
        raise ValueError("expected a 32-byte X25519 public key") from None
    return raw


def ciphertext_bytes(value: str) -> bytes:
    """A sealed box: at least one byte of plaintext, at most 64 KiB in all."""

    raw = strict_b64decode(value)
    if not MIN_CIPHERTEXT_BYTES <= len(raw) <= MAX_CIPHERTEXT_BYTES:
        raise ValueError(
            f"expected a sealed box of {MIN_CIPHERTEXT_BYTES} to {MAX_CIPHERTEXT_BYTES} bytes"
        )
    return raw


def recipient_id(public_key: bytes) -> str:
    return hashlib.sha256(public_key).hexdigest()


def seal(public_key: bytes, plaintext: bytes) -> bytes:
    return SealedBox(PublicKey(public_key)).encrypt(plaintext)
