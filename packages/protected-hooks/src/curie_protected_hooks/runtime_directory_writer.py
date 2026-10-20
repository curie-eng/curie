"""Provisioner writer of the SOURCE-9 runtime directory, @spec PROTECTED-HOOK-SOURCE-11.

Only provisioner composition imports this module; an import-linter contract
keeps it out of the HTTP application and every ordinary process composition.
``render_runtime_files`` returns the file name to bytes mapping a Kubernetes
provisioner places in a Secret it owns. ``publish_runtime_directory`` writes a
host directory for a Docker substrate: a new owner-only version directory per
publication, swapped in by one atomic rename of the ``current`` symlink, the
layout a Secret volume presents through ``..data``. Both parse their own
output with the shared parsers first, so any shape the API reader refuses is
refused here before a byte is written. Mounting the directory read only into
the API alone remains LANE-8 work.
"""

from __future__ import annotations

import os
import re
import secrets
import stat
from dataclasses import dataclass, field

from .authority_records import Manifest
from .broker_transport import EnqueueCredential, MetadataReaderCredential, SourceWriterCredential
from .runtime_directory import (
    BOOTSTRAP_FILE,
    CA_FILE,
    ENQUEUE_FILE,
    MANIFEST_FILE,
    SOURCE_WRITER_FILE,
    RuntimeDirectoryInvalid,
    encode_bootstrap,
    encode_enqueue,
    encode_source_writer,
    parse_bootstrap,
    parse_enqueue,
    parse_source_writer,
)

# @spec PROTECTED-HOOK-SOURCE-11
CURRENT = "current"
_VERSION = re.compile(r"v\.[0-9a-f]{32}", re.ASCII)
_ROOT_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_FILE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
_VERSION_MODE = 0o700
_FILE_MODE = 0o400


@dataclass(frozen=True, slots=True)
class RuntimeDirectoryContents:
    """Typed writer inputs; never echoed, @spec PROTECTED-HOOK-SOURCE-11."""

    manifest: Manifest = field(repr=False)
    ca_pem: str = field(repr=False)
    max_readiness_ms: int = field(repr=False)
    control_reader: MetadataReaderCredential = field(repr=False)
    source_writer: SourceWriterCredential | None = field(default=None, repr=False)
    enqueue: EnqueueCredential | None = field(default=None, repr=False)


def _render(contents: RuntimeDirectoryContents) -> dict[str, bytes]:
    """@spec PROTECTED-HOOK-SOURCE-11."""
    if type(contents) is not RuntimeDirectoryContents or type(contents.manifest) is not Manifest:
        raise RuntimeDirectoryInvalid()
    if type(contents.ca_pem) is not str:
        raise RuntimeDirectoryInvalid()
    files = {
        MANIFEST_FILE: contents.manifest.canonical_bytes,
        CA_FILE: contents.ca_pem.encode("ascii"),
        BOOTSTRAP_FILE: encode_bootstrap(contents.max_readiness_ms, contents.control_reader),
    }
    bootstrap = parse_bootstrap(files[MANIFEST_FILE], files[CA_FILE], files[BOOTSTRAP_FILE])
    if contents.source_writer is not None:
        files[SOURCE_WRITER_FILE] = encode_source_writer(contents.source_writer)
        parse_source_writer(files[SOURCE_WRITER_FILE], bootstrap.credential)
    if contents.enqueue is not None:
        files[ENQUEUE_FILE] = encode_enqueue(contents.manifest, contents.enqueue)
        parse_enqueue(files[ENQUEUE_FILE], bootstrap)
    return files


def render_runtime_files(contents: RuntimeDirectoryContents) -> dict[str, bytes]:
    """The validated file set, refused whole by the shared parsers first.

    @spec PROTECTED-HOOK-SOURCE-11.
    """
    try:
        return _render(contents)
    except Exception:  # noqa: BLE001  Any defect is the one invalid runtime refusal.
        raise RuntimeDirectoryInvalid() from None


def _open_root(root: str) -> int:
    """A provisioner-owned root no other user can write, @spec PROTECTED-HOOK-SOURCE-11."""
    descriptor = os.open(root, _ROOT_FLAGS)
    try:
        status = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(status.st_mode)
            or status.st_uid != os.geteuid()
            or status.st_mode & 0o022
        ):
            raise RuntimeDirectoryInvalid()
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _current_version(root_fd: int) -> str | None:
    """The version ``current`` names, refusing anything but our symlink.

    @spec PROTECTED-HOOK-SOURCE-11.
    """
    try:
        status = os.stat(CURRENT, dir_fd=root_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if not stat.S_ISLNK(status.st_mode):
        raise RuntimeDirectoryInvalid()
    target = os.readlink(CURRENT, dir_fd=root_fd)
    return target if _VERSION.fullmatch(target) else None


def _write_version(root_fd: int, name: str, files: dict[str, bytes]) -> None:
    """Owner-only files flushed with their directory, @spec PROTECTED-HOOK-SOURCE-11."""
    os.mkdir(name, _VERSION_MODE, dir_fd=root_fd)
    version_fd = os.open(name, _ROOT_FLAGS, dir_fd=root_fd)
    try:
        os.fchmod(version_fd, _VERSION_MODE)
        for file_name, payload in files.items():
            descriptor = os.open(file_name, _FILE_FLAGS, _FILE_MODE, dir_fd=version_fd)
            try:
                os.fchmod(descriptor, _FILE_MODE)
                view = memoryview(payload)
                while view:
                    view = view[os.write(descriptor, view) :]
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        os.fsync(version_fd)
    finally:
        os.close(version_fd)


def _remove_version(root_fd: int, name: str) -> None:
    """Remove one writer-created version and its files, @spec PROTECTED-HOOK-SOURCE-11."""
    try:
        version_fd = os.open(name, _ROOT_FLAGS, dir_fd=root_fd)
    except FileNotFoundError:
        return
    try:
        for entry in os.listdir(version_fd):
            os.unlink(entry, dir_fd=version_fd)
    finally:
        os.close(version_fd)
    os.rmdir(name, dir_fd=root_fd)


def _unlink_quietly(root_fd: int, name: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-11."""
    try:
        os.unlink(name, dir_fd=root_fd)
    except FileNotFoundError:
        pass


def _publish(root_fd: int, files: dict[str, bytes]) -> None:
    """Write a version, swap ``current`` atomically, prune older versions.

    @spec PROTECTED-HOOK-SOURCE-11.
    """
    previous = _current_version(root_fd)
    version = f"v.{secrets.token_hex(16)}"
    link = f".current.{secrets.token_hex(16)}"
    try:
        _write_version(root_fd, version, files)
        os.symlink(version, link, dir_fd=root_fd)
        os.replace(link, CURRENT, src_dir_fd=root_fd, dst_dir_fd=root_fd)
    except BaseException:
        _unlink_quietly(root_fd, link)
        _remove_version(root_fd, version)
        raise
    os.fsync(root_fd)
    # The swap is the publication; a stale version that cannot be pruned now
    # is retried by the next publication rather than reported as a failure.
    for entry in os.listdir(root_fd):
        if _VERSION.fullmatch(entry) and entry not in (version, previous):
            try:
                _remove_version(root_fd, entry)
            except OSError:
                pass


def publish_runtime_directory(root: str, contents: RuntimeDirectoryContents) -> str:
    """Publish the file set under ``root`` and return the ``current`` path.

    ``CURIE_PROTECTED_RUNTIME_DIR`` names the returned path. A refusal or a
    failed publication leaves ``current`` unchanged. @spec PROTECTED-HOOK-SOURCE-11.
    """
    try:
        files = _render(contents)
        root_fd = _open_root(root)
        try:
            _publish(root_fd, files)
        finally:
            os.close(root_fd)
    except Exception:  # noqa: BLE001  Any defect is the one invalid runtime refusal.
        raise RuntimeDirectoryInvalid() from None
    return os.path.join(root, CURRENT)
