"""Writer and API reader agreement over real files, @spec PROTECTED-HOOK-SOURCE-11.

The provisioner writer publishes a runtime directory and the unchanged API
loaders read it back. Every shape the reader refuses is checked twice: the
hand-written bytes are refused by the matching loader, and the writer refuses
the same shape first and publishes nothing. @spec PROTECTED-HOOK-SOURCE-9
@spec PROTECTED-HOOK-SOURCE-6.
"""

from __future__ import annotations

import importlib
import json
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from curie_protected_hooks.authority_records import parse_manifest
from curie_protected_hooks.broker_transport import (
    BrokerMetadataUnavailable,
    EnqueueCredential,
    MetadataReaderCredential,
    SourceWriterCredential,
)
from curie_protected_hooks.runtime_directory import RuntimeDirectoryInvalid
from curie_protected_hooks.runtime_directory_writer import (
    RuntimeDirectoryContents,
    publish_runtime_directory,
    render_runtime_files,
)
from test_hook_source_support_broker import manifest_value, standalone_ca_pem

loader = importlib.import_module("curie_api.protected_runtime_files")

READER = "reader-fixture"
READER_SECRET = "reader-fixture-secret-0123456789"
WRITER = "writer-fixture"
WRITER_SECRET = "writer-fixture-secret-0123456789"
ENQUEUE = "enqueue-fixture"
ENQUEUE_SECRET = "enqueue-fixture-secret-0123456789"
CA_PEM = standalone_ca_pem()


def canonical(value: Any) -> bytes:
    """@spec PROTECTED-HOOK-SOURCE-11."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def make_contents(**overrides: Any) -> RuntimeDirectoryContents:
    """Full writer inputs with field overrides, @spec PROTECTED-HOOK-SOURCE-11."""
    fields: dict[str, Any] = {
        "manifest": parse_manifest(
            canonical(manifest_value(port=6379, pin="a" * 64, run_id="b" * 40))
        ),
        "ca_pem": CA_PEM,
        "max_readiness_ms": 60000,
        "control_reader": MetadataReaderCredential(READER, READER_SECRET),
        "source_writer": SourceWriterCredential(WRITER, WRITER_SECRET),
        "enqueue": EnqueueCredential(ENQUEUE, ENQUEUE_SECRET),
    }
    fields.update(overrides)
    return RuntimeDirectoryContents(**fields)


def safe_root(tmp_path: Path, name: str = "root") -> Path:
    """A provisioner owned root with mode 0700, @spec PROTECTED-HOOK-SOURCE-11."""
    root = tmp_path / name
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    return root


def hand_written(tmp_path: Path, overrides: dict[str, bytes]) -> str:
    """A fresh directory of valid files with some bytes replaced, @spec PROTECTED-HOOK-SOURCE-9."""
    directory = tmp_path / "hand"
    directory.mkdir(mode=0o700)
    files = {**render_runtime_files(make_contents()), **overrides}
    for name, payload in files.items():
        path = directory / name
        path.write_bytes(payload)
        path.chmod(0o600)
    return str(directory)


def bootstrap_bytes(**changes: Any) -> bytes:
    """Hand-written bootstrap.json with member changes, @spec PROTECTED-HOOK-SOURCE-9."""
    value: dict[str, Any] = {
        "schema_version": 1,
        "max_readiness_ms": "60000",
        "control_reader": {"username": READER, "password": READER_SECRET},
    }
    value.update(changes)
    return json.dumps(value).encode()


def test_full_roundtrip(tmp_path: Path) -> None:
    """All three loaders read back what the writer published, @spec PROTECTED-HOOK-SOURCE-11."""
    root = safe_root(tmp_path)
    contents = make_contents()
    current = publish_runtime_directory(str(root), contents)
    boot = loader.load_bootstrap(current)
    assert boot.manifest.canonical_bytes == contents.manifest.canonical_bytes
    assert boot.ca_pem == CA_PEM
    assert boot.max_readiness_ms == 60000
    assert (boot.credential.username, boot.credential.password) == (READER, READER_SECRET)
    admin = loader.load_administration(current)
    assert admin.bootstrap.manifest.canonical_bytes == contents.manifest.canonical_bytes
    assert (admin.writer.username, admin.writer.password) == (WRITER, WRITER_SECRET)
    ingress = loader.load_ingress(current)
    assert ingress.bootstrap.ca_pem == CA_PEM
    assert ingress.bootstrap.max_readiness_ms == 60000
    assert (ingress.enqueue.username, ingress.enqueue.password) == (ENQUEUE, ENQUEUE_SECRET)


def test_bootstrap_only_roundtrip(tmp_path: Path) -> None:
    """Bootstrap-only output loads, the others refuse, @spec PROTECTED-HOOK-SOURCE-11."""
    root = safe_root(tmp_path)
    current = publish_runtime_directory(str(root), make_contents(source_writer=None, enqueue=None))
    assert loader.load_bootstrap(current).max_readiness_ms == 60000
    with pytest.raises(loader.RuntimeFilesInvalid):
        loader.load_administration(current)
    with pytest.raises(loader.RuntimeFilesInvalid):
        loader.load_ingress(current)


def test_rotated_directory_reads_new_set(tmp_path: Path) -> None:
    """A second publication is what the loaders then read, @spec PROTECTED-HOOK-SOURCE-11."""
    root = safe_root(tmp_path)
    publish_runtime_directory(str(root), make_contents(max_readiness_ms=1000))
    current = publish_runtime_directory(str(root), make_contents(max_readiness_ms=2000))
    assert loader.load_bootstrap(current).max_readiness_ms == 2000


def _huge_reader() -> MetadataReaderCredential:
    """A reader password pushing bootstrap.json over the bound, @spec PROTECTED-HOOK-SOURCE-9."""
    return MetadataReaderCredential(READER, "p" * 70000)


def _same_name_writer() -> dict[str, Any]:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    return {"source_writer": SourceWriterCredential(READER, WRITER_SECRET)}


def _same_name_enqueue() -> dict[str, Any]:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    return {"enqueue": EnqueueCredential(READER, ENQUEUE_SECRET)}


# (id, loader name, hand-written overrides, writer overrides)
REFUSED: list[tuple[str, str, Callable[[], dict[str, bytes]], Callable[[], dict[str, Any]]]] = [
    (
        "readiness-zero",
        "load_bootstrap",
        lambda: {"bootstrap.json": bootstrap_bytes(max_readiness_ms="0")},
        lambda: {"max_readiness_ms": 0},
    ),
    (
        "readiness-negative",
        "load_bootstrap",
        lambda: {"bootstrap.json": bootstrap_bytes(max_readiness_ms="-1")},
        lambda: {"max_readiness_ms": -1},
    ),
    (
        "readiness-over-2-53",
        "load_bootstrap",
        lambda: {"bootstrap.json": bootstrap_bytes(max_readiness_ms="9007199254740992")},
        lambda: {"max_readiness_ms": 9007199254740992},
    ),
    (
        "readiness-bool",
        "load_bootstrap",
        lambda: {"bootstrap.json": bootstrap_bytes(max_readiness_ms=True)},
        lambda: {"max_readiness_ms": True},
    ),
    (
        "readiness-not-decimal-string",
        "load_bootstrap",
        lambda: {"bootstrap.json": bootstrap_bytes(max_readiness_ms=60000)},
        lambda: {"max_readiness_ms": "60000"},
    ),
    (
        "bootstrap-over-bound",
        "load_bootstrap",
        lambda: {
            "bootstrap.json": bootstrap_bytes(
                control_reader={"username": READER, "password": "p" * 70000}
            )
        },
        lambda: {"control_reader": _huge_reader()},
    ),
    (
        "ca-not-pem",
        "load_bootstrap",
        lambda: {"ca.pem": b"not a certificate"},
        lambda: {"ca_pem": "not a certificate"},
    ),
    (
        "ca-non-ascii",
        "load_bootstrap",
        lambda: {"ca.pem": ("café\n" + CA_PEM).encode("utf-8")},
        lambda: {"ca_pem": "café\n" + CA_PEM},
    ),
    (
        "ca-over-bound",
        "load_bootstrap",
        lambda: {"ca.pem": (CA_PEM + "#" * 70000).encode("ascii")},
        lambda: {"ca_pem": CA_PEM + "#" * 70000},
    ),
    (
        "writer-username-is-control-reader",
        "load_administration",
        lambda: {
            "source_writer.json": canonical(
                {
                    "schema_version": 1,
                    "source_writer": {"username": READER, "password": WRITER_SECRET},
                }
            )
        },
        _same_name_writer,
    ),
    (
        "enqueue-username-is-control-reader",
        "load_ingress",
        lambda: {
            "enqueue.json": canonical(
                {
                    "schema_version": 1,
                    "credential_ref": {"id": "credential/example-enqueue", "generation": "1"},
                    "enqueue": {"username": READER, "password": ENQUEUE_SECRET},
                }
            )
        },
        _same_name_enqueue,
    ),
]


@pytest.mark.parametrize(
    ("case", "loader_name", "hand", "writer"), REFUSED, ids=[r[0] for r in REFUSED]
)
def test_reader_refused_shapes_are_refused_by_reader_and_writer(
    tmp_path: Path,
    case: str,
    loader_name: str,
    hand: Callable[[], dict[str, bytes]],
    writer: Callable[[], dict[str, Any]],
) -> None:
    """Loader and writer both refuse the shape, @spec PROTECTED-HOOK-SOURCE-11."""
    directory = hand_written(tmp_path, hand())
    with pytest.raises(loader.RuntimeFilesInvalid):
        getattr(loader, loader_name)(directory)

    root = safe_root(tmp_path, "writer-root")
    with pytest.raises(RuntimeDirectoryInvalid):
        render_runtime_files(make_contents(**writer()))
    with pytest.raises(RuntimeDirectoryInvalid):
        publish_runtime_directory(str(root), make_contents(**writer()))
    assert os.listdir(root) == []


@pytest.mark.parametrize("which", ["bootstrap", "writer", "enqueue"])
def test_default_username_refused_everywhere(tmp_path: Path, which: str) -> None:
    """A ``default`` username is refused and unconstructible, @spec PROTECTED-HOOK-SOURCE-6."""
    if which == "bootstrap":
        hand = {
            "bootstrap.json": bootstrap_bytes(
                control_reader={"username": "default", "password": READER_SECRET}
            )
        }
        name, credential = "load_bootstrap", MetadataReaderCredential
    elif which == "writer":
        hand = {
            "source_writer.json": canonical(
                {
                    "schema_version": 1,
                    "source_writer": {"username": "default", "password": WRITER_SECRET},
                }
            )
        }
        name, credential = "load_administration", SourceWriterCredential
    else:
        hand = {
            "enqueue.json": canonical(
                {
                    "schema_version": 1,
                    "credential_ref": {"id": "credential/example-enqueue", "generation": "1"},
                    "enqueue": {"username": "default", "password": ENQUEUE_SECRET},
                }
            )
        }
        name, credential = "load_ingress", EnqueueCredential
    directory = hand_written(tmp_path, hand)
    with pytest.raises(loader.RuntimeFilesInvalid):
        getattr(loader, name)(directory)
    with pytest.raises(BrokerMetadataUnavailable):
        credential("default", "secret-0123456789")
