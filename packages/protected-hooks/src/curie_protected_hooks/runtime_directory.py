"""Shared SOURCE-9 runtime directory grammar, @spec PROTECTED-HOOK-SOURCE-11.

The out of band provisioner writes ``manifest.json``, ``ca.pem`` and
``bootstrap.json`` and, per role, ``source_writer.json`` and ``enqueue.json``
into the directory named by ``CURIE_PROTECTED_RUNTIME_DIR``. This module is
the one definition the API reader and the provisioner writer validate against:
the file names, the size bound, the strict JSON rule, three parsers over file
bytes and the canonical encoding a writer emits. It opens no file and writes
nothing; the API loader owns descriptor reads and the provisioner writer owns
publication. Every defect is one ``RuntimeDirectoryInvalid`` without detail.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

from .authority_records import Manifest, parse_manifest
from .broker_transport import (
    EnqueueCredential,
    MetadataReaderCredential,
    SourceWriterCredential,
    trusted_ca_pem,
)

# @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-SOURCE-11
MANIFEST_FILE = "manifest.json"
CA_FILE = "ca.pem"
BOOTSTRAP_FILE = "bootstrap.json"
# @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-11
SOURCE_WRITER_FILE = "source_writer.json"
ENQUEUE_FILE = "enqueue.json"
MAX_FILE_BYTES = 65536
MAX_READINESS_MS = 9007199254740991
_POSITIVE_DECIMAL = re.compile(r"[1-9][0-9]{0,15}", re.ASCII)
_BOOTSTRAP_FIELDS = frozenset({"schema_version", "max_readiness_ms", "control_reader"})
_PRINCIPAL_MEMBERS = frozenset({"username", "password"})
_WRITER_FIELDS = frozenset({"schema_version", "source_writer"})
_ENQUEUE_FIELDS = frozenset({"schema_version", "credential_ref", "enqueue"})
_REFERENCE_MEMBERS = frozenset({"id", "generation"})


class RuntimeDirectoryInvalid(Exception):
    """A runtime file the grammar refuses, @spec PROTECTED-HOOK-SOURCE-11."""


@dataclass(frozen=True)
class RuntimeBootstrap:
    """Parsed provisioner bootstrap; never echoed, @spec PROTECTED-HOOK-SOURCE-9."""

    manifest: Manifest = field(repr=False)
    ca_pem: str = field(repr=False)
    credential: MetadataReaderCredential = field(repr=False)
    max_readiness_ms: int = field(repr=False)


def _require(condition: bool) -> None:
    """@spec PROTECTED-HOOK-SOURCE-11."""
    if not condition:
        raise RuntimeDirectoryInvalid()


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result)
        result[key] = value
    return result


def _reject_number(_value: str) -> Any:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    raise RuntimeDirectoryInvalid()


def _strict_json(raw: bytes) -> Any:
    """Duplicate members, floats and constants refused, @spec PROTECTED-HOOK-SOURCE-6/9."""
    _require(type(raw) is bytes and len(raw) <= MAX_FILE_BYTES)
    return json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=_unique_pairs,
        parse_float=_reject_number,
        parse_constant=_reject_number,
    )


def _encode(value: dict[str, Any]) -> bytes:
    """Canonical writer bytes, @spec PROTECTED-HOOK-SOURCE-11."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")


def _parse_bootstrap(manifest_raw: bytes, ca_raw: bytes, bootstrap_raw: bytes) -> RuntimeBootstrap:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    _require(all(type(raw) is bytes for raw in (manifest_raw, ca_raw, bootstrap_raw)))
    _require(len(manifest_raw) <= MAX_FILE_BYTES and len(ca_raw) <= MAX_FILE_BYTES)
    manifest = parse_manifest(manifest_raw)
    ca_pem = trusted_ca_pem(ca_raw.decode("ascii"))
    config = _strict_json(bootstrap_raw)
    _require(type(config) is dict and config.keys() == _BOOTSTRAP_FIELDS)
    _require(type(config["schema_version"]) is int and config["schema_version"] == 1)
    maximum = config["max_readiness_ms"]
    _require(type(maximum) is str and _POSITIVE_DECIMAL.fullmatch(maximum) is not None)
    _require(int(maximum) <= MAX_READINESS_MS)
    reader = config["control_reader"]
    _require(type(reader) is dict and reader.keys() == _PRINCIPAL_MEMBERS)
    credential = MetadataReaderCredential(reader["username"], reader["password"])
    return RuntimeBootstrap(manifest, ca_pem, credential, int(maximum))


def parse_bootstrap(manifest_raw: bytes, ca_raw: bytes, bootstrap_raw: bytes) -> RuntimeBootstrap:
    """The three bootstrap files' grammar, @spec PROTECTED-HOOK-SOURCE-9/11."""
    try:
        return _parse_bootstrap(manifest_raw, ca_raw, bootstrap_raw)
    except Exception:  # noqa: BLE001  Any parse failure is the one invalid runtime refusal.
        raise RuntimeDirectoryInvalid() from None


def _parse_source_writer(raw: bytes, reader: MetadataReaderCredential) -> SourceWriterCredential:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    config = _strict_json(raw)
    _require(type(config) is dict and config.keys() == _WRITER_FIELDS)
    _require(type(config["schema_version"]) is int and config["schema_version"] == 1)
    writer = config["source_writer"]
    _require(type(writer) is dict and writer.keys() == _PRINCIPAL_MEMBERS)
    username, password = writer["username"], writer["password"]
    _require(type(username) is str and type(password) is str)
    _require(username != reader.username)
    return SourceWriterCredential(username, password)


def parse_source_writer(raw: bytes, reader: MetadataReaderCredential) -> SourceWriterCredential:
    """``{schema_version: 1, source_writer: {username, password}}`` exactly.

    The username must be nonempty, not ``default`` and not the control
    reader's. @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-11.
    """
    try:
        return _parse_source_writer(raw, reader)
    except Exception:  # noqa: BLE001  Any parse failure is the one invalid runtime refusal.
        raise RuntimeDirectoryInvalid() from None


def _parse_enqueue(raw: bytes, bootstrap: RuntimeBootstrap) -> EnqueueCredential:
    """@spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-LANE-3."""
    config = _strict_json(raw)
    _require(type(config) is dict and config.keys() == _ENQUEUE_FIELDS)
    _require(type(config["schema_version"]) is int and config["schema_version"] == 1)
    reference = config["credential_ref"]
    _require(type(reference) is dict and reference.keys() == _REFERENCE_MEMBERS)
    _require(all(type(value) is str for value in reference.values()))
    _require(reference == bootstrap.manifest.as_dict()["credential_refs"]["enqueue"])
    enqueue = config["enqueue"]
    _require(type(enqueue) is dict and enqueue.keys() == _PRINCIPAL_MEMBERS)
    username, password = enqueue["username"], enqueue["password"]
    _require(type(username) is str and type(password) is str)
    _require(username != bootstrap.credential.username)
    return EnqueueCredential(username, password)


def parse_enqueue(raw: bytes, bootstrap: RuntimeBootstrap) -> EnqueueCredential:
    """``{schema_version: 1, credential_ref: {id, generation}, enqueue: {username, password}}``.

    ``credential_ref`` equals the manifest's ``credential_refs.enqueue``
    exactly, so a stale file after a provisioner rotation is refused. The
    username must be nonempty, not ``default`` and not the control reader's.
    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-LANE-3 @spec PROTECTED-HOOK-SOURCE-11.
    """
    try:
        return _parse_enqueue(raw, bootstrap)
    except Exception:  # noqa: BLE001  Any parse failure is the one invalid runtime refusal.
        raise RuntimeDirectoryInvalid() from None


def _principal(credential: Any, kind: type) -> dict[str, str]:
    """@spec PROTECTED-HOOK-SOURCE-11."""
    _require(type(credential) is kind)
    return {"username": credential.username, "password": credential.password}


def encode_bootstrap(max_readiness_ms: int, control_reader: MetadataReaderCredential) -> bytes:
    """Canonical ``bootstrap.json``, @spec PROTECTED-HOOK-SOURCE-11."""
    _require(type(max_readiness_ms) is int and 0 < max_readiness_ms <= MAX_READINESS_MS)
    return _encode(
        {
            "schema_version": 1,
            "max_readiness_ms": str(max_readiness_ms),
            "control_reader": _principal(control_reader, MetadataReaderCredential),
        }
    )


def encode_source_writer(source_writer: SourceWriterCredential) -> bytes:
    """Canonical ``source_writer.json``, @spec PROTECTED-HOOK-SOURCE-11."""
    return _encode(
        {
            "schema_version": 1,
            "source_writer": _principal(source_writer, SourceWriterCredential),
        }
    )


def encode_enqueue(manifest: Manifest, enqueue: EnqueueCredential) -> bytes:
    """Canonical ``enqueue.json`` bound to the manifest's enqueue reference.

    @spec PROTECTED-HOOK-SOURCE-11 @spec PROTECTED-HOOK-SOURCE-6.
    """
    _require(type(manifest) is Manifest)
    return _encode(
        {
            "schema_version": 1,
            "credential_ref": manifest.as_dict()["credential_refs"]["enqueue"],
            "enqueue": _principal(enqueue, EnqueueCredential),
        }
    )
