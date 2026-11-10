"""Provisioner runtime directory loading, @spec PROTECTED-HOOK-SOURCE-6/9/10/11.

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
file grammar is the shared ``curie_protected_hooks.runtime_directory``
(PROTECTED-HOOK-SOURCE-11), which the provisioner writer validates against
too; this module owns only the descriptor reads and the single refusal.
"""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass, field

from curie_protected_hooks.broker_transport import EnqueueCredential, SourceWriterCredential
from curie_protected_hooks.runtime_directory import (
    BOOTSTRAP_FILE,
    CA_FILE,
    ENQUEUE_FILE,
    MANIFEST_FILE,
    MAX_FILE_BYTES,
    SOURCE_WRITER_FILE,
    parse_bootstrap,
    parse_enqueue,
    parse_source_writer,
)
from curie_protected_hooks.runtime_directory import RuntimeBootstrap as RuntimeBootstrap

# @spec PROTECTED-HOOK-SOURCE-9
_FILE_FLAGS = os.O_RDONLY | os.O_NONBLOCK | os.O_CLOEXEC
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NONBLOCK | os.O_CLOEXEC


class RuntimeFilesInvalid(Exception):
    """A missing, unreadable or invalid runtime file, @spec PROTECTED-HOOK-SOURCE-9."""


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
        _require(stat.S_ISREG(status.st_mode) and status.st_size <= MAX_FILE_BYTES)
        chunks: list[bytes] = []
        total = 0
        while chunk := os.read(descriptor, MAX_FILE_BYTES + 1 - total):
            chunks.append(chunk)
            total += len(chunk)
            _require(total <= MAX_FILE_BYTES)
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def load_bootstrap(directory: str) -> RuntimeBootstrap:
    """Strict bootstrap files, read afresh; any defect is one refusal.

    A credential the reader would refuse before connecting (``default`` among
    them) makes the bootstrap invalid. Never opens the writer file.
    @spec PROTECTED-HOOK-SOURCE-9.
    """
    try:
        directory_fd = os.open(directory, _DIRECTORY_FLAGS)
        try:
            manifest_raw = _read_file(directory_fd, MANIFEST_FILE)
            ca_raw = _read_file(directory_fd, CA_FILE)
            bootstrap_raw = _read_file(directory_fd, BOOTSTRAP_FILE)
        finally:
            os.close(directory_fd)
        return parse_bootstrap(manifest_raw, ca_raw, bootstrap_raw)
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
            manifest_raw = _read_file(directory_fd, MANIFEST_FILE)
            ca_raw = _read_file(directory_fd, CA_FILE)
            bootstrap_raw = _read_file(directory_fd, BOOTSTRAP_FILE)
            writer_raw = _read_file(directory_fd, SOURCE_WRITER_FILE)
        finally:
            os.close(directory_fd)
        bootstrap = parse_bootstrap(manifest_raw, ca_raw, bootstrap_raw)
        writer = parse_source_writer(writer_raw, bootstrap.credential)
        return AdministrationRuntime(bootstrap, writer)
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
            manifest_raw = _read_file(directory_fd, MANIFEST_FILE)
            ca_raw = _read_file(directory_fd, CA_FILE)
            bootstrap_raw = _read_file(directory_fd, BOOTSTRAP_FILE)
            enqueue_raw = _read_file(directory_fd, ENQUEUE_FILE)
        finally:
            os.close(directory_fd)
        bootstrap = parse_bootstrap(manifest_raw, ca_raw, bootstrap_raw)
        return IngressRuntime(bootstrap, parse_enqueue(enqueue_raw, bootstrap))
    except Exception:  # noqa: BLE001  Any read or parse failure is the one invalid runtime refusal.
        raise RuntimeFilesInvalid() from None
