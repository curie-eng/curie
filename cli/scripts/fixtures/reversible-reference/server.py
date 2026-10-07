"""The reference reversible connector: a hosted MCP fixture that can undo itself.

@spec ACTION-EXECUTOR-9 @spec ACTION-EXECUTOR-13 @spec ACTION-EXECUTOR-15
@spec ACTION-EXECUTOR-16. Three tools over streamable HTTP at ``/mcp``:

* ``scale {target, replicas}`` writes, sealing the prior state into the
  ``prior`` envelope of the sealed reply convention (``curie.snapshot.v1``) and
  naming the ``version`` the write left.
* ``observe_version {target}`` (read-only) reports the live version.
* ``restore {target, prior_state, expected_version?}`` opens the envelope and
  writes the prior state back, compare-and-swapping on ``expected_version``.

Refusals are structured replies ``{"ok": false, "refused": <code>}``, never
tool errors, so the worker maps each to its closed code. ``restore`` checks in
a fixed order: envelope grammar (``snapshot_unopenable``), the kid is held
(``sealing_key_unavailable``), the ciphertext opens under that key with the
target as associated data (``snapshot_unopenable``), the version still matches
(``version_conflict``), and only then writes.

Key custody (AE-16): ``SNAPSHOT_SEALING_KEY=<kid>:<base64 of 32 bytes>`` seals
and opens; ``SNAPSHOT_SEALING_KEYS_RETAINED`` (comma separated, same form) only
opens. With no current key a forward write refuses ``sealing_key_unavailable``
and writes nothing; there is no unsealed fallback. The cipher is AES-256-GCM:
``ciphertext = base64(nonce[12] || ct || tag)``, associated data the canonical
JSON of ``target`` (sorted keys, no spaces), plaintext ``{"replicas": n}``.

The disposable resource is the JSON record store named by
``REFERENCE_STORE_PATH`` (``{"<namespace>/<name>": {"replicas", "version"}}``).
Scaling a real ``Deployment`` (the cluster tier of plan task 15) is not
implemented yet; without a store path the process refuses to start.

``REFERENCE_CONNECTOR_VARIANT`` selects a deliberately non-conforming build for
the tests: ``unpaired`` advertises no ``observe_version`` (AE-13 not capable)
and ``ignores_expected_version`` declares ``expected_version`` and ignores it.

Nothing here logs argument values, ciphertext, versions or key material.
"""

# NOTE: no `from __future__ import annotations`, deliberately, as in the example
# connectors: MCPServer introspects tool signatures and string annotations break
# that at import time.

import base64
import binascii
import json
import logging
import os
import re
import secrets
import sys
import tempfile
import threading
from pathlib import Path
from typing import Any, Final

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

log = logging.getLogger("reversible-reference")

# --------------------------------------------------------------------------- #
# The sealed envelope grammar (ACTION-EXECUTOR-9). A copy of the worker's
# ``curie_worker.sealed_snapshot.is_sealed_envelope``: this fixture ships in its
# own image and cannot import the worker. The tests hold both to the shared
# vector ``tests/vectors/sealed-snapshot-reply.json``.
# --------------------------------------------------------------------------- #

SEALED_CONSTANT: Final = "curie.snapshot.v1"
MAX_CIPHERTEXT_BYTES: Final = 65536
REDACTION_PLACEHOLDER_PREFIX: Final = "[REDACTED:"
NONCE_BYTES: Final = 12
KEY_BYTES: Final = 32

_ENVELOPE_KEYS: Final = frozenset({"sealed", "kid", "ciphertext"})
_KID = re.compile(r"[A-Za-z0-9._-]{1,64}")
_STANDARD_BASE64 = re.compile(r"(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?")

# Connector refusal codes during ``call`` (ACTION-EXECUTOR-20).
SEALING_KEY_UNAVAILABLE: Final = "sealing_key_unavailable"
SNAPSHOT_UNOPENABLE: Final = "snapshot_unopenable"
VERSION_CONFLICT: Final = "version_conflict"

VARIANTS: Final = frozenset({"conforming", "unpaired", "ignores_expected_version"})


def _envelope_ciphertext(value: Any) -> bytes | None:
    """The decoded ciphertext of a grammatical envelope, else ``None``."""

    if not isinstance(value, dict) or set(value) != _ENVELOPE_KEYS:
        return None
    if value["sealed"] != SEALED_CONSTANT:
        return None
    kid = value["kid"]
    if not isinstance(kid, str) or _KID.fullmatch(kid) is None:
        return None
    ciphertext = value["ciphertext"]
    if not isinstance(ciphertext, str) or not ciphertext:
        return None
    if REDACTION_PLACEHOLDER_PREFIX in ciphertext:
        return None
    if len(ciphertext) > 4 * ((MAX_CIPHERTEXT_BYTES + 2) // 3):
        return None
    if _STANDARD_BASE64.fullmatch(ciphertext) is None:
        return None
    try:
        decoded = base64.b64decode(ciphertext, validate=True)
    except (binascii.Error, ValueError):
        return None
    if not 0 < len(decoded) <= MAX_CIPHERTEXT_BYTES:
        return None
    return decoded


# --------------------------------------------------------------------------- #
# Keys (ACTION-EXECUTOR-16).
# --------------------------------------------------------------------------- #


class KeyConfigError(ValueError):
    """A sealing key variable is malformed. The message never carries key text."""


def _parse_key(entry: str, name: str) -> tuple[str, AESGCM]:
    kid, sep, encoded = entry.strip().partition(":")
    if not sep or _KID.fullmatch(kid) is None:
        raise KeyConfigError(f"{name}: an entry is not <kid>:<base64 of 32 bytes>")
    try:
        raw = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError):
        raise KeyConfigError(f"{name}: key for kid {kid} is not standard base64") from None
    if len(raw) != KEY_BYTES:
        raise KeyConfigError(f"{name}: key for kid {kid} is not {KEY_BYTES} bytes")
    return kid, AESGCM(raw)


class Keyring:
    """The current sealing key, if any, and every key that may open."""

    def __init__(self, current: str, retained: str) -> None:
        self.current: tuple[str, AESGCM] | None = None
        self.openers: dict[str, AESGCM] = {}
        if current.strip():
            self.current = _parse_key(current, "SNAPSHOT_SEALING_KEY")
            self.openers[self.current[0]] = self.current[1]
        for entry in retained.split(","):
            if entry.strip():
                kid, aead = _parse_key(entry, "SNAPSHOT_SEALING_KEYS_RETAINED")
                self.openers.setdefault(kid, aead)

    @classmethod
    def from_env(cls) -> "Keyring":
        return cls(
            os.environ.get("SNAPSHOT_SEALING_KEY", ""),
            os.environ.get("SNAPSHOT_SEALING_KEYS_RETAINED", ""),
        )


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _seal(current: tuple[str, AESGCM], target: dict[str, Any], replicas: int) -> dict[str, str]:
    kid, aead = current
    nonce = os.urandom(NONCE_BYTES)
    body = aead.encrypt(nonce, _canonical({"replicas": replicas}), _canonical(target))
    return {
        "sealed": SEALED_CONSTANT,
        "kid": kid,
        "ciphertext": base64.b64encode(nonce + body).decode("ascii"),
    }


# --------------------------------------------------------------------------- #
# The disposable resource: a JSON record store.
# --------------------------------------------------------------------------- #


class Store:
    """``{"<namespace>/<name>": {"replicas": int, "version": str}}`` on disk.

    Every write is a whole-file atomic replace under one lock, and every write
    mints a version never produced before, so a version names one write.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock = threading.Lock()

    def load(self) -> dict[str, Any]:
        loaded = json.loads(self.path.read_text("utf-8"))
        if not isinstance(loaded, dict):
            raise ToolError("the record store is not a JSON object")
        return loaded

    def save(self, records: dict[str, Any]) -> None:
        fd, tmp = tempfile.mkstemp(dir=self.path.parent, prefix=".store-")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(records, handle)
            os.replace(tmp, self.path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    @staticmethod
    def new_version() -> str:
        return f"rv-{secrets.token_hex(12)}"


def _record_key(target: dict[str, Any]) -> str:
    namespace, name = target.get("namespace"), target.get("name")
    if not (isinstance(namespace, str) and namespace and isinstance(name, str) and name):
        raise ToolError("target needs a non-empty namespace and name")
    return f"{namespace}/{name}"


def _record(records: dict[str, Any], key: str) -> dict[str, Any]:
    record = records.get(key)
    if not isinstance(record, dict):
        raise ToolError("unknown target")
    return record


def _refused(code: str) -> dict[str, Any]:
    return {"ok": False, "refused": code}


# --------------------------------------------------------------------------- #
# The server.
# --------------------------------------------------------------------------- #

WRITE = ToolAnnotations(
    read_only_hint=False, destructive_hint=True, idempotent_hint=False, open_world_hint=False
)
READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, open_world_hint=False)


def build(store: Store, keys: Keyring, variant: str) -> MCPServer:
    """The MCP server for one store, keyring and variant."""

    mcp = MCPServer("reversible-reference")
    checks_expected_version = variant != "ignores_expected_version"

    @mcp.tool(annotations=WRITE)
    def scale(target: dict[str, Any], replicas: int) -> dict[str, Any]:
        """Set the replica count of ``target``; reply with the sealed prior state.

        @spec ACTION-EXECUTOR-9. Refuses ``sealing_key_unavailable`` and writes
        nothing when no current sealing key is held.
        """

        if replicas < 0:
            raise ToolError("replicas must be zero or more")
        key = _record_key(target)
        if keys.current is None:
            log.info("scale refused: %s", SEALING_KEY_UNAVAILABLE)
            return _refused(SEALING_KEY_UNAVAILABLE)
        with store.lock:
            records = store.load()
            record = _record(records, key)
            prior = _seal(keys.current, target, int(record["replicas"]))
            version = store.new_version()
            records[key] = {"replicas": replicas, "version": version}
            store.save(records)
        log.info("scale wrote")
        return {"ok": True, "prior": prior, "version": version, "target": target}

    if variant != "unpaired":

        @mcp.tool(annotations=READ_ONLY)
        def observe_version(target: dict[str, Any]) -> dict[str, Any]:
            """Report the live version of ``target``. @spec ACTION-EXECUTOR-15."""

            key = _record_key(target)
            with store.lock:
                record = _record(store.load(), key)
            return {"version": record["version"]}

    @mcp.tool(annotations=WRITE)
    def restore(
        target: dict[str, Any],
        prior_state: dict[str, Any],
        expected_version: str | None = None,
    ) -> dict[str, Any]:
        """Write the sealed ``prior_state`` back to ``target``.

        @spec ACTION-EXECUTOR-15 @spec ACTION-EXECUTOR-16. ``expected_version``,
        when given, must equal the live version or the call refuses
        ``version_conflict`` and writes nothing.
        """

        key = _record_key(target)
        sealed = _envelope_ciphertext(prior_state)
        if sealed is None:
            log.info("restore refused: %s", SNAPSHOT_UNOPENABLE)
            return _refused(SNAPSHOT_UNOPENABLE)
        aead = keys.openers.get(prior_state["kid"])
        if aead is None:
            log.info("restore refused: %s", SEALING_KEY_UNAVAILABLE)
            return _refused(SEALING_KEY_UNAVAILABLE)
        try:
            opened = json.loads(
                aead.decrypt(sealed[:NONCE_BYTES], sealed[NONCE_BYTES:], _canonical(target))
            )
        except (InvalidTag, ValueError):
            opened = None
        replicas = opened.get("replicas") if isinstance(opened, dict) else None
        if not isinstance(replicas, int) or isinstance(replicas, bool) or replicas < 0:
            log.info("restore refused: %s", SNAPSHOT_UNOPENABLE)
            return _refused(SNAPSHOT_UNOPENABLE)
        with store.lock:
            records = store.load()
            record = _record(records, key)
            if (
                checks_expected_version
                and expected_version is not None
                and record["version"] != expected_version
            ):
                log.info("restore refused: %s", VERSION_CONFLICT)
                return _refused(VERSION_CONFLICT)
            version = store.new_version()
            records[key] = {"replicas": replicas, "version": version}
            store.save(records)
        log.info("restore wrote")
        return {"ok": True, "version": version, "target": target}

    return mcp


def main() -> int:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), stream=sys.stderr)
    variant = os.environ.get("REFERENCE_CONNECTOR_VARIANT", "").strip() or "conforming"
    if variant not in VARIANTS:
        log.error("refusing to start: unknown REFERENCE_CONNECTOR_VARIANT")
        return 1
    store_path = os.environ.get("REFERENCE_STORE_PATH", "").strip()
    if not store_path:
        # The cluster tier (a real Deployment) is not implemented; refuse rather
        # than serve tools that fail on every call.
        log.error("refusing to start: REFERENCE_STORE_PATH is not set")
        return 1
    try:
        keys = Keyring.from_env()
    except KeyConfigError as exc:
        log.error("refusing to start: %s", exc)
        return 1
    log.info(
        "reversible reference connector: variant %s, sealing key %s, %d opening key(s)",
        variant,
        "held" if keys.current is not None else "absent",
        len(keys.openers),
    )
    build(Store(Path(store_path)), keys, variant).run(
        transport="streamable-http",
        host=os.environ.get("BIND_ADDRESS", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
        streamable_http_path="/mcp",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
