"""Read the explicitly provisioned worker role and immutable execution profile.

The caller must independently establish mount ownership. These structural
inputs grant no execution authority and do not activate an ordinary worker.
No source-writer, enqueue, model or history credential file is opened.
@spec PROTECTED-HOOK-LANE-3/6/7.
"""

from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass, field
from typing import Any, NoReturn

from curie_protected_hooks.artifact_measurement import _fingerprint, _open_root
from curie_protected_hooks.authority_records import Manifest, parse_manifest
from curie_protected_hooks.worker_transport import WorkerCredential

from .protected_configuration import ProtectedExecutionConfig, parse_execution_config

_MAX_BYTES = 65536


class WorkerBootstrapInvalid(ValueError):
    """Fixed diagnostic without paths, input values or nested exceptions."""


def _refuse() -> NoReturn:
    error = WorkerBootstrapInvalid("protected worker bootstrap is invalid")
    try:
        raise error from None
    finally:
        error.__context__ = None
        error.__cause__ = None


def _require(condition: bool) -> None:
    if not condition:
        _refuse()


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, member in pairs:
        _require(key not in value)
        value[key] = member
    return value


def _reject(_raw: str) -> NoReturn:
    _refuse()


def _read(root: int, filename: str) -> bytes:
    descriptor = os.open(
        filename, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=root
    )
    try:
        before = os.fstat(descriptor)
        _require(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= _MAX_BYTES)
        chunks: list[bytes] = []
        size = 0
        while size <= _MAX_BYTES:
            chunk = os.read(descriptor, _MAX_BYTES + 1 - size)
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
        _require(size <= _MAX_BYTES)
        after = os.fstat(descriptor)
        selected = os.stat(filename, dir_fd=root, follow_symlinks=False)
        _require(
            stat.S_ISREG(selected.st_mode)
            and _fingerprint(before) == _fingerprint(after) == _fingerprint(selected)
        )
        return b"".join(chunks)
    finally:
        os.close(descriptor)


@dataclass(frozen=True, slots=True, repr=False)
class ProtectedWorkerBootstrap:
    manifest: Manifest = field(repr=False)
    execution: ProtectedExecutionConfig = field(repr=False)
    credential: WorkerCredential = field(repr=False)

    def __repr__(self) -> str:
        return "ProtectedWorkerBootstrap()"


def load_worker_bootstrap(directory: str) -> ProtectedWorkerBootstrap:
    """Read only three bounded regular files from one non-symlink root.

    This loader deliberately refuses Secret-volume symlink layouts. Provision
    regular immutable files, with no symlink component in the selected root.
    Broker authentication and artifact selection remain separate live gates.
    """
    try:
        root = _open_root(directory)
        try:
            manifest = parse_manifest(_read(root, "manifest.json"))
            execution = parse_execution_config(
                _read(root, "execution_config.json"), manifest=manifest
            )
            value = json.loads(
                _read(root, "worker.json").decode("utf-8", errors="strict"),
                object_pairs_hook=_pairs, parse_float=_reject, parse_constant=_reject,
            )
        finally:
            os.close(root)
        _require(
            type(value) is dict
            and value.keys() == {"schema_version", "credential_ref", "worker"}
        )
        _require(type(value["schema_version"]) is int and value["schema_version"] == 1)
        reference = value["credential_ref"]
        _require(type(reference) is dict and reference.keys() == {"id", "generation"})
        _require(reference == manifest.as_dict()["credential_refs"]["worker"])
        worker = value["worker"]
        _require(type(worker) is dict and worker.keys() == {"username", "password"})
        _require(type(worker["username"]) is str and type(worker["password"]) is str)
        credential = WorkerCredential(worker["username"], worker["password"])
        return ProtectedWorkerBootstrap(manifest, execution, credential)
    except Exception:  # noqa: BLE001  Never expose private mount or credential diagnostics.
        pass
    _refuse()
