"""Provisioner runtime directory loader, @spec PROTECTED-HOOK-SOURCE-6/9/10.

Unit tests of ``curie_api.protected_runtime_files`` over real private files:
the ``source_writer.json`` grammar, the shared descriptor, regular file, size
and duplicate member rules, redaction, the probe and GET path never opening
the writer file, and an import graph without a cycle. No broker or database:
the loader is file parsing only.
"""

from __future__ import annotations

import importlib
import json
import os
import stat
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from curie_protected_hooks.broker_transport import SourceWriterCredential
from test_hook_source_support_broker import manifest_value, standalone_ca_pem

loader = importlib.import_module("curie_api.protected_runtime_files")

READER = "reader-fixture"
WRITER = "writer-fixture"
READER_SECRET = "reader-fixture-secret-0123456789"
WRITER_SECRET = "writer-fixture-secret-0123456789"


def canonical(value: Any) -> bytes:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


class Directory:
    """One private provisioner directory, @spec PROTECTED-HOOK-SOURCE-6/9."""

    def __init__(self, root: Path) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6/9."""
        self.path = root / "runtime"
        self.path.mkdir(mode=0o700)
        self.ca_pem = standalone_ca_pem()
        self.files: dict[str, bytes] = {
            "manifest.json": canonical(manifest_value(port=6379, pin="a" * 64, run_id="b" * 40)),
            "ca.pem": self.ca_pem.encode("ascii"),
            "bootstrap.json": json.dumps(
                {
                    "schema_version": 1,
                    "max_readiness_ms": "60000",
                    "control_reader": {"username": READER, "password": READER_SECRET},
                }
            ).encode(),
            "source_writer.json": json.dumps(
                {
                    "schema_version": 1,
                    "source_writer": {"username": WRITER, "password": WRITER_SECRET},
                }
            ).encode(),
        }
        self.write()

    def write(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6/9."""
        for name, payload in self.files.items():
            path = self.path / name
            path.write_bytes(payload)
            path.chmod(0o600)

    def unblock(self) -> None:
        """Unblock owned FIFOs so a blocked reader sees EOF, @spec PROTECTED-HOOK-SOURCE-6."""
        for path in self.path.iterdir():
            if stat.S_ISFIFO(path.lstat().st_mode):
                for flags in (os.O_WRONLY | os.O_NONBLOCK, os.O_RDWR | os.O_NONBLOCK):
                    try:
                        os.close(os.open(path, flags))
                        break
                    except OSError:
                        continue


@pytest.fixture
def runtime(tmp_path: Path) -> Any:
    """@spec PROTECTED-HOOK-SOURCE-6/9."""
    directory = Directory(tmp_path)
    try:
        yield directory
    finally:
        directory.unblock()


def writer_bytes(value: Any) -> bytes:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    return value if isinstance(value, bytes) else json.dumps(value).encode()


def valid_writer() -> dict[str, Any]:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    return {"schema_version": 1, "source_writer": {"username": WRITER, "password": WRITER_SECRET}}


def _drop(*path: str) -> dict[str, Any]:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    value = valid_writer()
    target = value
    for key in path[:-1]:
        target = target[key]
    del target[path[-1]]
    return value


def _with(**changes: Any) -> dict[str, Any]:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    value = valid_writer()
    for key, change in changes.items():
        if key in ("username", "password"):
            value["source_writer"][key] = change
        else:
            value[key] = change
    return value


INVALID: list[tuple[str, Any]] = [
    ("extra-member", _with(extra=True)),
    ("missing-version", _drop("schema_version")),
    ("missing-writer", _drop("source_writer")),
    ("other-version", _with(schema_version=2)),
    ("string-version", _with(schema_version="1")),
    ("boolean-version", _with(schema_version=True)),
    ("float-version", b'{"schema_version":1.0,"source_writer":{"username":"w","password":"p"}}'),
    ("nan", b'{"schema_version":NaN,"source_writer":{"username":"w","password":"p"}}'),
    (
        "duplicate-member",
        b'{"schema_version":1,"schema_version":1,"source_writer":{"username":"w","password":"p"}}',
    ),
    (
        "duplicate-inner",
        b'{"schema_version":1,"source_writer":{"username":"w","username":"x","password":"p"}}',
    ),
    ("writer-array", _with(source_writer=["w", "p"])),
    (
        "writer-extra",
        {**valid_writer(), "source_writer": {**valid_writer()["source_writer"], "x": 1}},
    ),
    ("missing-username", _drop("source_writer", "username")),
    ("missing-password", _drop("source_writer", "password")),
    ("empty-username", _with(username="")),
    ("empty-password", _with(password="")),
    ("numeric-username", _with(username=7)),
    ("null-password", _with(password=None)),
    ("default-username", _with(username="default")),
    ("reader-username", _with(username=READER)),
    ("top-level-array", b"[]"),
    ("not-json", b"schema_version=1"),
    ("not-utf8", b'{"schema_version":1,"source_writer":"\xff"}'),
    ("empty-file", b""),
]


@pytest.mark.parametrize("payload", [c[1] for c in INVALID], ids=[c[0] for c in INVALID])
def test_invalid_writer_file_is_one_safe_refusal(runtime: Any, payload: Any) -> None:
    """Strict ``{schema_version: 1, source_writer: {username, password}}`` only.

    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-10.
    """
    runtime.files["source_writer.json"] = writer_bytes(payload)
    runtime.write()
    with pytest.raises(loader.RuntimeFilesInvalid) as caught:
        loader.load_administration(str(runtime.path))
    error = caught.value
    assert error.__cause__ is None
    assert error.__context__ is None or error.__suppress_context__
    for value in (WRITER_SECRET, READER_SECRET, str(runtime.path)):
        assert value not in repr(error) and value not in str(error)
    # The probe and GET path never read the writer file, so it stays valid.
    assert loader.load_bootstrap(str(runtime.path)).credential.username == READER


def _fifo(directory: Path) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    (directory / "source_writer.json").unlink()
    os.mkfifo(directory / "source_writer.json", 0o600)


def _directory(directory: Path) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    (directory / "source_writer.json").unlink()
    (directory / "source_writer.json").mkdir(mode=0o700)


def _symlink_to_fifo(directory: Path) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    os.mkfifo(directory / "target-fifo", 0o600)
    (directory / "source_writer.json").unlink()
    (directory / "source_writer.json").symlink_to("target-fifo")


def _symlink_to_directory(directory: Path) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    (directory / "target-directory").mkdir(mode=0o700)
    (directory / "source_writer.json").unlink()
    (directory / "source_writer.json").symlink_to("target-directory")


def _oversize(directory: Path) -> None:
    """Valid JSON padded past the shared 64 KiB bound, @spec PROTECTED-HOOK-SOURCE-6."""
    path = directory / "source_writer.json"
    path.write_bytes(path.read_bytes() + b" " * (65536 + 1))


def _missing(directory: Path) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6/10."""
    (directory / "source_writer.json").unlink()


def _unreadable(directory: Path) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    (directory / "source_writer.json").chmod(0)


SPECIAL: list[tuple[str, Callable[[Path], None]]] = [
    ("fifo", _fifo),
    ("directory", _directory),
    ("symlink-to-fifo", _symlink_to_fifo),
    ("symlink-to-directory", _symlink_to_directory),
    ("oversize", _oversize),
    ("missing", _missing),
    ("unreadable", _unreadable),
]


@pytest.mark.parametrize("special", [c[1] for c in SPECIAL], ids=[c[0] for c in SPECIAL])
def test_special_writer_files_refuse_promptly_and_bootstrap_ignores_them(
    runtime: Any, special: Callable[[Path], None]
) -> None:
    """Regular file after symlinks, bounded, opened without blocking; probe path unaffected.

    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-SOURCE-10.
    """
    special(runtime.path)
    started = time.monotonic()
    with pytest.raises(loader.RuntimeFilesInvalid):
        loader.load_administration(str(runtime.path))
    bootstrap = loader.load_bootstrap(str(runtime.path))
    assert time.monotonic() - started < 2, "a special writer file blocked the loader"
    assert bootstrap.credential.username == READER


def test_exact_size_bound_and_symlinked_secret_volume_layout_are_accepted(runtime: Any) -> None:
    """A file of exactly 64 KiB and the ``..data`` symlink layout both load.

    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-9.
    """
    raw = json.dumps(valid_writer()).encode()
    runtime.files["source_writer.json"] = raw + b" " * (65536 - len(raw))
    runtime.write()
    assert len((runtime.path / "source_writer.json").read_bytes()) == 65536
    assert loader.load_administration(str(runtime.path)).writer.username == WRITER
    data = runtime.path / "..data"
    data.mkdir(mode=0o700)
    for name in list(runtime.files):
        (runtime.path / name).rename(data / name)
        (runtime.path / name).symlink_to(Path("..data") / name)
    loaded = loader.load_administration(str(runtime.path))
    assert loaded.writer.username == WRITER and loaded.bootstrap.credential.username == READER


def test_valid_files_parse_and_every_representation_redacts(runtime: Any) -> None:
    """``SourceWriterCredential`` is frozen, slotted and redacted, @spec PROTECTED-HOOK-SOURCE-6."""
    loaded = loader.load_administration(str(runtime.path))
    assert type(loaded.writer) is SourceWriterCredential
    assert (loaded.writer.username, loaded.writer.password) == (WRITER, WRITER_SECRET)
    assert loaded.bootstrap.manifest.as_dict()["runtime_id"]
    assert not hasattr(loaded.writer, "__dict__")
    with pytest.raises((AttributeError, TypeError)):
        loaded.writer.password = "changed"  # type: ignore[misc]
    for value in (loaded, loaded.writer, loaded.bootstrap):
        text = repr(value) + str(value)
        for secret in (
            WRITER,
            WRITER_SECRET,
            READER,
            READER_SECRET,
            runtime.ca_pem.splitlines()[1],
        ):
            assert secret not in text


def test_bootstrap_loader_never_opens_the_writer_file(runtime: Any) -> None:
    """A FIFO writer file with no peer leaves ``load_bootstrap`` prompt and valid.

    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-9.
    """
    _fifo(runtime.path)
    started = time.monotonic()
    assert loader.load_bootstrap(str(runtime.path)).credential.username == READER
    assert time.monotonic() - started < 1


IMPORT_CHECK = """
import importlib, sys
first = sys.argv[1]
importlib.import_module(first)
for name in (
    "curie_api.protected_runtime_files",
    "curie_api.hook_source_admin",
    "curie_api.hook_source_mutation",
    "curie_api.hook_source_broker",
    "curie_api.protected_support",
    "curie_api.routers.hook_source_policy",
):
    importlib.import_module(name)
"""

LOADER_ALONE = """
import sys
import curie_api.protected_runtime_files
loaded = sorted(
    name for name in (
        "curie_api.hook_source_admin",
        "curie_api.hook_source_mutation",
        "curie_api.hook_source_broker",
        "curie_api.protected_support",
        "curie_api.routers.hook_source_policy",
    )
    if name in sys.modules
)
print(",".join(loaded))
"""


@pytest.mark.parametrize(
    "first",
    [
        "curie_api.protected_runtime_files",
        "curie_api.hook_source_admin",
        "curie_api.hook_source_mutation",
        "curie_api.hook_source_broker",
        "curie_api.protected_support",
        "curie_api.routers.hook_source_policy",
    ],
)
def test_module_graph_has_no_import_cycle(first: str) -> None:
    """Each source module imports first in a fresh interpreter, @spec PROTECTED-HOOK-SOURCE-6."""
    result = subprocess.run(
        [sys.executable, "-c", IMPORT_CHECK, first],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr[-2000:]


def test_loader_imports_no_source_service_or_probe_module() -> None:
    """@spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-9."""
    result = subprocess.run(
        [sys.executable, "-c", LOADER_ALONE], capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stderr[-2000:]
    assert result.stdout.strip() == ""
