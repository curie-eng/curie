"""The resolver's configuration, read from the frozen render's environment (ADR 0239).

``plugin_format.shared_connector_render.render_application`` gives the proxy
container exactly these names, and the resolver reads nothing else. In
particular it never reads ``DATABASE_URL`` or any other API or worker DSN: its
only database identity is the per-application role whose DSN the API sealed
to the application key ("Custody and least privilege" 3 and 4), decrypted here
from the mounted file.

Two names must be absent. ``CURIE_SHARED_SNAPSHOT_SEAL_FILE`` belongs to the
server container alone and ``CURIE_CONNECTOR_CALLER_SIGNING_KEY`` is the
worker's Ed25519 signer, which no connector Pod may hold ("Custody" 2, "Bundle"
2). Either one here means a mis-rendered Pod, so the resolver refuses to start.

Every refusal is a ValueError naming the variable and never its value, and
``repr`` of a config hides the key ring and the DSN.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from .keys import KeyRing, KeyUnavailable, strict_b64decode
from .wire import SOCKET_ENV

INSTALLATION_ENV = "CURIE_SHARED_INSTALLATION"
APPLICATION_ENV = "CURIE_SHARED_APPLICATION"
DSN_ENCODING_ENV = "CURIE_SHARED_DSN_ENCODING"
PROVIDER_ORIGINS_ENV = "CURIE_SHARED_PROVIDER_ORIGINS"
CREDENTIAL_SLOTS_ENV = "CURIE_SHARED_CREDENTIAL_SLOTS"
ENCRYPTION_KEY_FILE_ENV = "CURIE_SHARED_ENCRYPTION_KEY_FILE"
DATABASE_DSN_FILE_ENV = "CURIE_SHARED_DATABASE_DSN_FILE"
# Present only in a mis-rendered Pod; the resolver refuses to start beside either.
FORBIDDEN_ENV = ("CURIE_SHARED_SNAPSHOT_SEAL_FILE", "CURIE_CONNECTOR_CALLER_SIGNING_KEY")

DSN_ENCODINGS = frozenset({"bundled-sealed", "managed-raw"})
_DSN_SCHEMES = ("postgresql://", "postgres://")
# sockaddr_un.sun_path is 108 bytes including the terminating NUL.
_MAX_SOCKET_PATH_BYTES = 107
_MAX_INSTALLATION_BYTES = 128
# Far above a sealed DSN or a key file; bounds what a mis-mounted file can cost.
_MAX_FILE_BYTES = 65536
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_SLOT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")


def _required(env: Mapping[str, str], name: str) -> str:
    value = env.get(name)
    if not value:
        raise ValueError(f"{name} is required")
    return value


def _installation(env: Mapping[str, str]) -> str:
    value = _required(env, INSTALLATION_ENV)
    if len(value.encode("utf-8")) > _MAX_INSTALLATION_BYTES or any(
        char.isspace() or not char.isprintable() for char in value
    ):
        raise ValueError(f"{INSTALLATION_ENV} is not a valid installation ID")
    return value


def _socket_path(env: Mapping[str, str]) -> str:
    value = _required(env, SOCKET_ENV)
    path = Path(value)
    if (
        not path.is_absolute()
        or os.path.normpath(value) != value
        or path.parent == Path("/")
        or len(os.fsencode(value)) > _MAX_SOCKET_PATH_BYTES
    ):
        raise ValueError(
            f"{SOCKET_ENV} must be a canonical absolute path of at most "
            f"{_MAX_SOCKET_PATH_BYTES} bytes in its own directory"
        )
    if not path.parent.is_dir():
        raise ValueError(f"{SOCKET_ENV} names a directory that does not exist")
    return value


def _string_list(env: Mapping[str, str], name: str, *, allow_empty: bool) -> tuple[str, ...]:
    try:
        parsed = json.loads(_required(env, name))
    except ValueError:
        raise ValueError(f"{name} is not a JSON list of strings") from None
    if (
        not isinstance(parsed, list)
        or not all(isinstance(item, str) and item for item in parsed)
        or len(set(parsed)) != len(parsed)
        or (not parsed and not allow_empty)
    ):
        raise ValueError(f"{name} is not a JSON list of distinct nonblank strings")
    return tuple(parsed)


def _read_file(env: Mapping[str, str], name: str) -> str:
    path = _required(env, name)
    try:
        with open(path, "rb") as handle:
            raw = handle.read(_MAX_FILE_BYTES + 1)
    except OSError:
        raise ValueError(f"{name} names a file that cannot be read") from None
    if len(raw) > _MAX_FILE_BYTES:
        raise ValueError(f"{name} names a file that is too large")
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        raise ValueError(f"{name} names a file that is not UTF-8 text") from None


def _dsn(text: str, encoding: str, keys: KeyRing) -> str:
    """The plaintext DSN. Every failure names the file variable only."""

    stripped = "".join(text.split())
    if encoding == "bundled-sealed":
        try:
            dsn = keys.open_any(strict_b64decode(stripped)).decode("utf-8")
        except (ValueError, KeyUnavailable):
            raise ValueError(
                f"{DATABASE_DSN_FILE_ENV} does not hold a DSN sealed to a held key"
            ) from None
    else:
        dsn = text.strip()
    if not dsn.startswith(_DSN_SCHEMES) or any(char.isspace() for char in dsn):
        raise ValueError(f"{DATABASE_DSN_FILE_ENV} does not hold a postgresql:// DSN")
    return dsn


@dataclass(frozen=True)
class ResolverConfig:
    installation: str
    application: str
    socket_path: str
    dsn_encoding: str
    provider_origins: tuple[str, ...]
    # Empty for connector custody, whose only accepted slot is "" (identity).
    credential_slots: tuple[str, ...]
    keys: KeyRing = field(repr=False)
    dsn: str = field(repr=False)

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> ResolverConfig:
        for name in FORBIDDEN_ENV:
            if name in env:
                raise ValueError(f"{name} must not be present in the resolver's environment")
        installation = _installation(env)
        application = _required(env, APPLICATION_ENV)
        if _UUID.fullmatch(application) is None:
            raise ValueError(f"{APPLICATION_ENV} is not a canonical lowercase UUID")
        socket_path = _socket_path(env)
        encoding = _required(env, DSN_ENCODING_ENV)
        if encoding not in DSN_ENCODINGS:
            raise ValueError(f"{DSN_ENCODING_ENV} is not bundled-sealed or managed-raw")
        origins = _string_list(env, PROVIDER_ORIGINS_ENV, allow_empty=True)
        slots = _string_list(env, CREDENTIAL_SLOTS_ENV, allow_empty=True)
        if any(_SLOT.fullmatch(slot) is None for slot in slots):
            raise ValueError(f"{CREDENTIAL_SLOTS_ENV} holds a slot that is not an identifier")
        try:
            keys = KeyRing.from_text(_read_file(env, ENCRYPTION_KEY_FILE_ENV))
        except ValueError:
            raise ValueError(f"{ENCRYPTION_KEY_FILE_ENV} does not hold a valid key file") from None
        dsn = _dsn(_read_file(env, DATABASE_DSN_FILE_ENV), encoding, keys)
        return cls(
            installation=installation,
            application=application,
            socket_path=socket_path,
            dsn_encoding=encoding,
            provider_origins=origins,
            credential_slots=slots,
            keys=keys,
            dsn=dsn,
        )
