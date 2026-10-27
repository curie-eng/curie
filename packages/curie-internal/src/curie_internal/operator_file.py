"""Protected operator file reading for startup initializers. @spec HTTP-INVOCATION-EXECUTION-20.

One fixed file name is opened relative to an operator-selected directory, so a
Secret volume's ``..data`` link is followed. Every defect is a fixed stage; no
path, value or decoder message leaves this module.
"""

from __future__ import annotations

import json
import os
import stat
from typing import Any, Literal, Never

from curie_internal.sandbox_token import b64url, b64url_decode

MAX_FILE_BYTES = 1_048_576
_FILE_FLAGS = os.O_RDONLY | os.O_NONBLOCK | os.O_CLOEXEC
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NONBLOCK | os.O_CLOEXEC
# No group write and no permission for other users: 0440 and 0400 qualify.
_FORBIDDEN_MODE = stat.S_IWGRP | stat.S_IRWXO
_BASE64URL = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")

Stage = Literal["unconfigured_directory", "unsafe_file", "invalid_document"]


class OperatorFileRefusal(Exception):
    """One fixed refusal stage. @spec HTTP-INVOCATION-EXECUTION-20."""

    def __init__(self, stage: Stage) -> None:
        super().__init__(stage)
        self.stage: Stage = stage


def _refuse(stage: Stage) -> Never:
    """@spec HTTP-INVOCATION-EXECUTION-20."""
    raise OperatorFileRefusal(stage) from None


def _pairs(entries: list[tuple[str, Any]]) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-20: duplicate keys refuse."""
    result: dict[str, Any] = {}
    for key, value in entries:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _constant(value: str) -> Never:
    """@spec HTTP-INVOCATION-EXECUTION-20: NaN and infinities refuse."""
    raise ValueError


def _read(directory: str, name: str) -> bytes:
    """@spec HTTP-INVOCATION-EXECUTION-20."""
    if type(directory) is not str or not directory or type(name) is not str:
        _refuse("unconfigured_directory")
    try:
        directory_fd = os.open(directory, _DIRECTORY_FLAGS)
    except OSError:
        _refuse("unconfigured_directory")
    try:
        try:
            descriptor = os.open(name, _FILE_FLAGS, dir_fd=directory_fd)
        except OSError:
            _refuse("unsafe_file")
        try:
            status = os.fstat(descriptor)
            if (
                not stat.S_ISREG(status.st_mode) or status.st_size > MAX_FILE_BYTES
                or status.st_uid not in {os.geteuid(), 0}
                or stat.S_IMODE(status.st_mode) & _FORBIDDEN_MODE
            ):
                _refuse("unsafe_file")
            chunks: list[bytes] = []
            total = 0
            while chunk := os.read(descriptor, MAX_FILE_BYTES + 1 - total):
                chunks.append(chunk)
                total += len(chunk)
                if total > MAX_FILE_BYTES:
                    _refuse("unsafe_file")
            return b"".join(chunks)
        except OSError:
            _refuse("unsafe_file")
        finally:
            os.close(descriptor)
    finally:
        os.close(directory_fd)


def read_operator_json(directory: str, name: str) -> Any:
    """Strict JSON from one protected file. @spec HTTP-INVOCATION-EXECUTION-20."""
    raw = _read(directory, name)
    try:
        text = raw.decode("utf-8")
        # Reject lone surrogates that JSON escapes would otherwise smuggle in.
        value = json.loads(text, object_pairs_hook=_pairs, parse_constant=_constant)
        json.dumps(value, ensure_ascii=False).encode("utf-8")
        return value
    except (ValueError, TypeError, RecursionError, UnicodeError):
        _refuse("invalid_document")


def decode_key_material(value: object) -> bytes:
    """Exactly 32 bytes from unpadded canonical base64url. @spec HTTP-INVOCATION-EXECUTION-20."""
    try:
        if type(value) is not str or len(value) != 43 or not _BASE64URL.issuperset(value):
            raise ValueError
        material = b64url_decode(value)
        if len(material) != 32 or b64url(material) != value:
            raise ValueError
        return material
    except Exception:  # noqa: BLE001 - key material never reaches an error.
        raise ValueError("invalid_key_material") from None
