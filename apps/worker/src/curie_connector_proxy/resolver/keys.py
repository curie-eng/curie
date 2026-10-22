"""The application's X25519 key ring (ADR 0239 "Custody and least privilege" 1, 4, 6).

Credentials and the bundled DSN are libsodium sealed boxes to the
application's registered public key, the scheme ``curie_api.shared_connectors
.sealing`` writes. Only the proxy container mounts the private key file. It
holds one standard-base64 32-byte private key per line, current first; earlier
lines are previous recipients kept for a reseal transition ("Custody" 6), so a
row still sealed to an old recipient opens until it is resealed.

A recipient ID is the lowercase hex SHA-256 of the raw public key, as the API
computes it, so ``open`` decrypts with exactly the key a row names. Nothing
raised here carries key text, ciphertext or plaintext, and ``repr`` of a ring
shows only how many keys it holds.
"""

from __future__ import annotations

import binascii
import hashlib

from nacl.exceptions import CryptoError
from nacl.public import PrivateKey, PublicKey, SealedBox

_KEY_BYTES = 32


class KeyUnavailable(Exception):
    """No held key opens this ciphertext. Carries no key or ciphertext text."""


def recipient_id(public_key: bytes) -> str:
    return hashlib.sha256(public_key).hexdigest()


def strict_b64decode(text: str) -> bytes:
    """Standard padded base64 with no whitespace or other alphabet; ValueError otherwise."""

    try:
        return binascii.a2b_base64(text.encode("ascii"), strict_mode=True)
    except (UnicodeEncodeError, binascii.Error):
        raise ValueError("expected standard padded base64") from None


class KeyRing:
    """Private keys by recipient ID, current first."""

    __slots__ = ("_boxes", "_public", "recipient_ids")

    def __init__(self, keys: list[PrivateKey]) -> None:
        if not keys:
            raise ValueError("a key ring needs at least one key")
        boxes: dict[str, SealedBox[PrivateKey]] = {}
        public: dict[str, bytes] = {}
        for key in keys:
            raw_public = bytes(key.public_key)
            identifier = recipient_id(raw_public)
            if identifier in boxes:
                raise ValueError("the key file repeats a key")
            boxes[identifier] = SealedBox(key)
            public[identifier] = raw_public
        self._boxes = boxes
        self._public = public
        self.recipient_ids: tuple[str, ...] = tuple(boxes)

    @classmethod
    def from_text(cls, text: str) -> KeyRing:
        """Parse a key file: one standard-base64 private key per nonblank line."""

        keys: list[PrivateKey] = []
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            raw = strict_b64decode(stripped)
            if len(raw) != _KEY_BYTES:
                raise ValueError("a key file line is not a 32-byte X25519 private key")
            keys.append(PrivateKey(raw))
        if not keys:
            raise ValueError("the key file holds no key")
        return cls(keys)

    def holds(self, recipient: str) -> bool:
        return recipient in self._boxes

    def public_key(self, recipient: str) -> bytes:
        """The raw public key of a held recipient; KeyUnavailable when not held."""

        try:
            return self._public[recipient]
        except KeyError:
            raise KeyUnavailable("the recipient is not held") from None

    def open(self, recipient: str, sealed: bytes) -> bytes:
        box = self._boxes.get(recipient)
        if box is None:
            raise KeyUnavailable("the recipient is not held")
        try:
            return box.decrypt(sealed)
        except (CryptoError, ValueError, TypeError):
            raise KeyUnavailable("the ciphertext does not open") from None

    def open_any(self, sealed: bytes) -> bytes:
        """Try every held key; a sealed DSN names no recipient."""

        for box in self._boxes.values():
            try:
                return box.decrypt(sealed)
            except (CryptoError, ValueError, TypeError):
                continue
        raise KeyUnavailable("no held key opens the ciphertext")

    def __repr__(self) -> str:
        return f"KeyRing(keys={len(self._boxes)})"

    __str__ = __repr__


def seal(public_key: bytes, plaintext: bytes) -> bytes:
    return SealedBox(PublicKey(public_key)).encrypt(plaintext)
