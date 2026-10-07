"""Provisioner runtime directory loading, @spec PROTECTED-HOOK-SOURCE-6/9/10.

The setting ``CURIE_PROTECTED_RUNTIME_DIR`` names a directory that only the out
of band provisioner writes and mounts read only into the API. It holds
``manifest.json``, ``ca.pem``, ``bootstrap.json`` and, for administration only,
``source_writer.json`` and, for ingress only, ``enqueue.json``. The support
probe, GET and the secret route read the first three through
``load_bootstrap``; only a source mutation reads the writer file, through
``load_administration``; only ``load_ingress`` reads the enqueue file, and it
never opens the writer file. Nothing here creates, returns or logs
the directory or its contents, and every defect is one refusal.

This module imports no source service or probe module, so the probe, the admin
service and the coordinator composition can all import it without a cycle. The
bootstrap grammar moved here verbatim from the probe module; #4076 later moves
it into the shared package.
"""

from __future__ import annotations

import json
import os
import re
import stat
from dataclasses import dataclass, field
from typing import Any

from curie_protected_hooks.authority_records import Manifest, parse_manifest
from curie_protected_hooks.broker_transport import (
    EnqueueCredential,
    MetadataReaderCredential,
    SourceWriterCredential,
    trusted_ca_pem,
)

# @spec PROTECTED-HOOK-SOURCE-9
_MANIFEST_FILE = "manifest.json"
_CA_FILE = "ca.pem"
_BOOTSTRAP_FILE = "bootstrap.json"
_MAX_FILE_BYTES = 65536
_MAX_MILLISECOND = 9007199254740991
_POSITIVE_DECIMAL = re.compile(r"[1-9][0-9]{0,15}", re.ASCII)
_BOOTSTRAP_FIELDS = frozenset({"schema_version", "max_readiness_ms", "control_reader"})
_READER_FIELDS = frozenset({"username", "password"})
_FILE_FLAGS = os.O_RDONLY | os.O_NONBLOCK | os.O_CLOEXEC
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NONBLOCK | os.O_CLOEXEC
# @spec PROTECTED-HOOK-SOURCE-6
_WRITER_FILE = "source_writer.json"
_WRITER_FIELDS = frozenset({"schema_version", "source_writer"})
_WRITER_MEMBERS = frozenset({"username", "password"})
# @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-LANE-3
_ENQUEUE_FILE = "enqueue.json"
_ENQUEUE_FIELDS = frozenset({"schema_version", "credential_ref", "enqueue"})
_ENQUEUE_MEMBERS = frozenset({"username", "password"})
_REFERENCE_MEMBERS = frozenset({"id", "generation"})


class RuntimeFilesInvalid(Exception):
    """A missing, unreadable or invalid runtime file, @spec PROTECTED-HOOK-SOURCE-9."""


@dataclass(frozen=True)
class RuntimeBootstrap:
    """Parsed provisioner bootstrap; never echoed, @spec PROTECTED-HOOK-SOURCE-9."""

    manifest: Manifest = field(repr=False)
    ca_pem: str = field(repr=False)
    credential: MetadataReaderCredential = field(repr=False)
    max_readiness_ms: int = field(repr=False)


@dataclass(frozen=True)
class AdministrationRuntime:
    """The bootstrap plus the source writer principal, @spec PROTECTED-HOOK-SOURCE-6."""

    bootstrap: RuntimeBootstrap = field(repr=False)
    writer: SourceWriterCredential = field(repr=False)


@dataclass(frozen=True)
class IngressRuntime:
    """The bootstrap plus the enqueue principal, @spec PROTECTED-HOOK-SOURCE-6."""

    bootstrap: RuntimeBootstrap = field(repr=False)
    enqueue: EnqueueCredential = field(repr=False)


def _require(condition: bool) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    if not condition:
        raise RuntimeFilesInvalid()


def _read_file(directory_fd: int, name: str) -> bytes:
    """One bounded regular bootstrap file relative to the opened directory.

    Symlinks are followed (a secret volume links through ``..data``); the
    opened file must be regular, so a FIFO, device or directory is refused
    without blocking. @spec PROTECTED-HOOK-SOURCE-9.
    """
    descriptor = os.open(name, _FILE_FLAGS, dir_fd=directory_fd)
    try:
        status = os.fstat(descriptor)
        _require(stat.S_ISREG(status.st_mode) and status.st_size <= _MAX_FILE_BYTES)
        chunks: list[bytes] = []
        total = 0
        while chunk := os.read(descriptor, _MAX_FILE_BYTES + 1 - total):
            chunks.append(chunk)
            total += len(chunk)
            _require(total <= _MAX_FILE_BYTES)
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result)
        result[key] = value
    return result


def _reject_number(_value: str) -> Any:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    raise RuntimeFilesInvalid()


def _strict_json(raw: bytes) -> Any:
    """Duplicate members, floats and constants refused, @spec PROTECTED-HOOK-SOURCE-6/9."""
    return json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=_unique_pairs,
        parse_float=_reject_number,
        parse_constant=_reject_number,
    )


def _parse_bootstrap(manifest_raw: bytes, ca_raw: bytes, bootstrap_raw: bytes) -> RuntimeBootstrap:
    """The three bootstrap files' grammar, @spec PROTECTED-HOOK-SOURCE-9."""
    manifest = parse_manifest(manifest_raw)
    ca_pem = trusted_ca_pem(ca_raw.decode("ascii"))
    config = _strict_json(bootstrap_raw)
    _require(type(config) is dict and config.keys() == _BOOTSTRAP_FIELDS)
    _require(type(config["schema_version"]) is int and config["schema_version"] == 1)
    maximum = config["max_readiness_ms"]
    _require(type(maximum) is str and _POSITIVE_DECIMAL.fullmatch(maximum) is not None)
    _require(int(maximum) <= _MAX_MILLISECOND)
    reader = config["control_reader"]
    _require(type(reader) is dict and reader.keys() == _READER_FIELDS)
    credential = MetadataReaderCredential(reader["username"], reader["password"])
    return RuntimeBootstrap(manifest, ca_pem, credential, int(maximum))


def _parse_writer(raw: bytes, reader: MetadataReaderCredential) -> SourceWriterCredential:
    """``{schema_version: 1, source_writer: {username, password}}`` exactly.

    The username must be nonempty, not ``default`` and not the control
    reader's. @spec PROTECTED-HOOK-SOURCE-6.
    """
    config = _strict_json(raw)
    _require(type(config) is dict and config.keys() == _WRITER_FIELDS)
    _require(type(config["schema_version"]) is int and config["schema_version"] == 1)
    writer = config["source_writer"]
    _require(type(writer) is dict and writer.keys() == _WRITER_MEMBERS)
    username, password = writer["username"], writer["password"]
    _require(type(username) is str and type(password) is str)
    _require(username != reader.username)
    return SourceWriterCredential(username, password)


def _parse_enqueue(raw: bytes, bootstrap: RuntimeBootstrap) -> EnqueueCredential:
    """``{schema_version: 1, credential_ref: {id, generation}, enqueue: {username, password}}``.

    ``credential_ref`` equals the manifest's ``credential_refs.enqueue``
    exactly, so a stale file after a provisioner rotation is refused. The
    username must be nonempty, not ``default`` and not the control reader's.
    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-LANE-3.
    """
    config = _strict_json(raw)
    _require(type(config) is dict and config.keys() == _ENQUEUE_FIELDS)
    _require(type(config["schema_version"]) is int and config["schema_version"] == 1)
    reference = config["credential_ref"]
    _require(type(reference) is dict and reference.keys() == _REFERENCE_MEMBERS)
    _require(all(type(value) is str for value in reference.values()))
    _require(reference == bootstrap.manifest.as_dict()["credential_refs"]["enqueue"])
    enqueue = config["enqueue"]
    _require(type(enqueue) is dict and enqueue.keys() == _ENQUEUE_MEMBERS)
    username, password = enqueue["username"], enqueue["password"]
    _require(type(username) is str and type(password) is str)
    _require(username != bootstrap.credential.username)
    return EnqueueCredential(username, password)


def load_bootstrap(directory: str) -> RuntimeBootstrap:
    """Strict bootstrap files, read afresh; any defect is one refusal.

    A credential the reader would refuse before connecting (``default`` among
    them) makes the bootstrap invalid. Never opens the writer file.
    @spec PROTECTED-HOOK-SOURCE-9.
    """
    try:
        directory_fd = os.open(directory, _DIRECTORY_FLAGS)
        try:
            manifest_raw = _read_file(directory_fd, _MANIFEST_FILE)
            ca_raw = _read_file(directory_fd, _CA_FILE)
            bootstrap_raw = _read_file(directory_fd, _BOOTSTRAP_FILE)
        finally:
            os.close(directory_fd)
        return _parse_bootstrap(manifest_raw, ca_raw, bootstrap_raw)
    except Exception:  # noqa: BLE001  Any read or parse failure is the one invalid runtime refusal.
        raise RuntimeFilesInvalid() from None


def load_administration(directory: str) -> AdministrationRuntime:
    """All four runtime files under one opened directory, for one mutation.

    Blocking; the caller runs it on its administrative slot.
    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-10.
    """
    try:
        directory_fd = os.open(directory, _DIRECTORY_FLAGS)
        try:
            manifest_raw = _read_file(directory_fd, _MANIFEST_FILE)
            ca_raw = _read_file(directory_fd, _CA_FILE)
            bootstrap_raw = _read_file(directory_fd, _BOOTSTRAP_FILE)
            writer_raw = _read_file(directory_fd, _WRITER_FILE)
        finally:
            os.close(directory_fd)
        bootstrap = _parse_bootstrap(manifest_raw, ca_raw, bootstrap_raw)
        return AdministrationRuntime(bootstrap, _parse_writer(writer_raw, bootstrap.credential))
    except Exception:  # noqa: BLE001  Any read or parse failure is the one invalid runtime refusal.
        raise RuntimeFilesInvalid() from None


def load_ingress(directory: str) -> IngressRuntime:
    """The bootstrap files and ``enqueue.json`` under one opened directory.

    Never opens the writer file. Blocking; the caller runs it off the event
    loop. @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-9.
    """
    try:
        directory_fd = os.open(directory, _DIRECTORY_FLAGS)
        try:
            manifest_raw = _read_file(directory_fd, _MANIFEST_FILE)
            ca_raw = _read_file(directory_fd, _CA_FILE)
            bootstrap_raw = _read_file(directory_fd, _BOOTSTRAP_FILE)
            enqueue_raw = _read_file(directory_fd, _ENQUEUE_FILE)
        finally:
            os.close(directory_fd)
        bootstrap = _parse_bootstrap(manifest_raw, ca_raw, bootstrap_raw)
        return IngressRuntime(bootstrap, _parse_enqueue(enqueue_raw, bootstrap))
    except Exception:  # noqa: BLE001  Any read or parse failure is the one invalid runtime refusal.
        raise RuntimeFilesInvalid() from None
