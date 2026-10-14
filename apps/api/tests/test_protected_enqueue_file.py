"""Provisioner ``enqueue.json`` loader, @spec PROTECTED-HOOK-SOURCE-6 PROTECTED-HOOK-SOURCE-9
PROTECTED-HOOK-LANE-3.

Unit tests of ``curie_api.protected_runtime_files.load_ingress`` over real
private files. ``enqueue.json`` is a strict object of exactly
``schema_version: 1``, ``credential_ref: {id, generation}`` equal to the
manifest's ``credential_refs.enqueue`` and ``enqueue: {username, password}``,
read under the descriptor, regular file, size and duplicate member rules of
``bootstrap.json``. The username is nonempty, not ``default`` and not the
control reader's. It parses into a frozen, slotted, redacted
``EnqueueCredential``. ``load_ingress`` returns the bootstrap (``bootstrap``)
plus that credential (``enqueue``) and never opens the writer file;
``load_bootstrap`` and ``load_administration`` never open ``enqueue.json``.
No broker or database: the loader is file parsing only.
"""

from __future__ import annotations

import copy
import importlib
import json
import os
import stat
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from test_hook_source_support_broker import manifest_value, standalone_ca_pem

loader = importlib.import_module("curie_api.protected_runtime_files")

READER = "reader-fixture"
WRITER = "writer-fixture"
ENQUEUE = "enqueue-fixture"
READER_SECRET = "reader-fixture-secret-0123456789"
WRITER_SECRET = "writer-fixture-secret-0123456789"
ENQUEUE_SECRET = "enqueue-fixture-secret-0123456789"
ENQUEUE_REF = {"id": "credential/example-enqueue", "generation": "1"}


def canonical(value: Any) -> bytes:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def valid_enqueue() -> dict[str, Any]:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    return {
        "schema_version": 1,
        "credential_ref": dict(ENQUEUE_REF),
        "enqueue": {"username": ENQUEUE, "password": ENQUEUE_SECRET},
    }


class Directory:
    """One private provisioner directory with all five runtime files.

    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-9.
    """

    def __init__(self, root: Path) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6/9."""
        self.path = root / "runtime"
        self.path.mkdir(mode=0o700)
        self.ca_pem = standalone_ca_pem()
        self.manifest = manifest_value(port=6379, pin="a" * 64, run_id="b" * 40)
        self.files: dict[str, bytes] = {
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
            "enqueue.json": json.dumps(valid_enqueue()).encode(),
        }
        self.write()

    def write(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6/9."""
        self.files["manifest.json"] = canonical(self.manifest)
        for name, payload in self.files.items():
            path = self.path / name
            if path.exists() or path.is_symlink():
                path.unlink()
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


def load_ingress(path: Path) -> Any:
    """Missing product loader is an assertion in the test body, @spec PROTECTED-HOOK-SOURCE-6."""
    assert hasattr(loader, "load_ingress"), "ingress runtime loader absent"
    return loader.load_ingress(str(path))


def enqueue_bytes(value: Any) -> bytes:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    return value if isinstance(value, bytes) else json.dumps(value).encode()


def _drop(*path: str) -> dict[str, Any]:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    value = valid_enqueue()
    target = value
    for key in path[:-1]:
        target = target[key]
    del target[path[-1]]
    return value


def _with(**changes: Any) -> dict[str, Any]:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    value = valid_enqueue()
    for key, change in changes.items():
        if key in ("username", "password"):
            value["enqueue"][key] = change
        elif key in ("ref_id", "ref_generation"):
            value["credential_ref"][key.removeprefix("ref_")] = change
        else:
            value[key] = change
    return value


INVALID: list[tuple[str, Any]] = [
    ("extra-member", _with(extra=True)),
    ("missing-version", _drop("schema_version")),
    ("missing-credential-ref", _drop("credential_ref")),
    ("missing-enqueue", _drop("enqueue")),
    ("other-version", _with(schema_version=2)),
    ("string-version", _with(schema_version="1")),
    ("boolean-version", _with(schema_version=True)),
    (
        "float-version",
        b'{"schema_version":1.0,"credential_ref":{"id":"credential/example-enqueue",'
        b'"generation":"1"},"enqueue":{"username":"e","password":"p"}}',
    ),
    (
        "nan",
        b'{"schema_version":NaN,"credential_ref":{"id":"credential/example-enqueue",'
        b'"generation":"1"},"enqueue":{"username":"e","password":"p"}}',
    ),
    (
        "duplicate-member",
        b'{"schema_version":1,"schema_version":1,"credential_ref":{"id":'
        b'"credential/example-enqueue","generation":"1"},"enqueue":{"username":"e","password":"p"}}',
    ),
    (
        "duplicate-enqueue-member",
        b'{"schema_version":1,"credential_ref":{"id":"credential/example-enqueue",'
        b'"generation":"1"},"enqueue":{"username":"e","username":"f","password":"p"}}',
    ),
    (
        "duplicate-reference-member",
        b'{"schema_version":1,"credential_ref":{"id":"credential/example-enqueue",'
        b'"id":"credential/example-enqueue","generation":"1"},'
        b'"enqueue":{"username":"e","password":"p"}}',
    ),
    ("enqueue-array", _with(enqueue=[ENQUEUE, ENQUEUE_SECRET])),
    ("enqueue-extra", {**valid_enqueue(), "enqueue": {**valid_enqueue()["enqueue"], "x": 1}}),
    ("missing-username", _drop("enqueue", "username")),
    ("missing-password", _drop("enqueue", "password")),
    ("empty-username", _with(username="")),
    ("empty-password", _with(password="")),
    ("numeric-username", _with(username=7)),
    ("numeric-password", _with(password=7)),
    ("null-password", _with(password=None)),
    ("default-username", _with(username="default")),
    ("reader-username", _with(username=READER)),
    ("reference-array", _with(credential_ref=["credential/example-enqueue", "1"])),
    ("reference-extra", _with(credential_ref={**ENQUEUE_REF, "role": "enqueue"})),
    ("reference-missing-id", _drop("credential_ref", "id")),
    ("reference-missing-generation", _drop("credential_ref", "generation")),
    ("reference-other-id", _with(ref_id="credential/example-other")),
    ("reference-worker-role", _with(ref_id="credential/example-worker")),
    ("reference-verifier-role", _with(ref_id="credential/example-verifier")),
    ("reference-stale-generation", _with(ref_generation="2")),
    ("reference-numeric-generation", _with(ref_generation=1)),
    ("reference-padded-generation", _with(ref_generation="01")),
    ("top-level-array", b"[]"),
    ("not-json", b"schema_version=1"),
    ("not-utf8", b'{"schema_version":1,"enqueue":"\xff"}'),
    ("empty-file", b""),
]


@pytest.mark.parametrize("payload", [c[1] for c in INVALID], ids=[c[0] for c in INVALID])
def test_invalid_enqueue_file_is_one_safe_refusal(runtime: Any, payload: Any) -> None:
    """Strict ``{schema_version: 1, credential_ref, enqueue: {username, password}}`` only.

    The bootstrap and administrative loaders never read the file, so they stay
    valid. @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-LANE-3.
    """
    runtime.files["enqueue.json"] = enqueue_bytes(payload)
    runtime.write()
    with pytest.raises(loader.RuntimeFilesInvalid) as caught:
        load_ingress(runtime.path)
    error = caught.value
    assert error.__cause__ is None
    assert error.__context__ is None or error.__suppress_context__
    for value in (ENQUEUE_SECRET, READER_SECRET, WRITER_SECRET, str(runtime.path)):
        assert value not in repr(error) and value not in str(error)
    assert loader.load_bootstrap(str(runtime.path)).credential.username == READER
    assert loader.load_administration(str(runtime.path)).writer.username == WRITER


def test_a_provisioner_rotation_makes_a_stale_file_invalid(runtime: Any) -> None:
    """The manifest's enqueue reference moved on; the old file is refused, not used.

    @spec PROTECTED-HOOK-SOURCE-6.
    """
    assert load_ingress(runtime.path).enqueue.username == ENQUEUE
    runtime.manifest = copy.deepcopy(runtime.manifest)
    runtime.manifest["credential_refs"]["enqueue"]["generation"] = "2"
    runtime.write()
    with pytest.raises(loader.RuntimeFilesInvalid):
        load_ingress(runtime.path)
    assert loader.load_bootstrap(str(runtime.path)).credential.username == READER


def test_an_invalid_bootstrap_refuses_ingress_too(runtime: Any) -> None:
    """``load_ingress`` returns the bootstrap, so its defects refuse as one, @spec
    PROTECTED-HOOK-SOURCE-9."""
    (runtime.path / "bootstrap.json").unlink()
    with pytest.raises(loader.RuntimeFilesInvalid):
        load_ingress(runtime.path)


def _fifo(directory: Path, name: str = "enqueue.json") -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    (directory / name).unlink()
    os.mkfifo(directory / name, 0o600)


def _directory(directory: Path) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    (directory / "enqueue.json").unlink()
    (directory / "enqueue.json").mkdir(mode=0o700)


def _symlink_to_fifo(directory: Path) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    os.mkfifo(directory / "target-fifo", 0o600)
    (directory / "enqueue.json").unlink()
    (directory / "enqueue.json").symlink_to("target-fifo")


def _symlink_to_directory(directory: Path) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    (directory / "target-directory").mkdir(mode=0o700)
    (directory / "enqueue.json").unlink()
    (directory / "enqueue.json").symlink_to("target-directory")


def _oversize(directory: Path) -> None:
    """Valid JSON padded past the shared 64 KiB bound, @spec PROTECTED-HOOK-SOURCE-6."""
    path = directory / "enqueue.json"
    path.write_bytes(path.read_bytes() + b" " * (65536 + 1))


def _missing(directory: Path) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    (directory / "enqueue.json").unlink()


def _unreadable(directory: Path) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    (directory / "enqueue.json").chmod(0)


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
def test_special_enqueue_files_refuse_promptly_and_other_loaders_ignore_them(
    runtime: Any, special: Callable[[Path], None]
) -> None:
    """Regular file after symlinks, bounded, opened without blocking.

    ``load_ingress`` opens ``enqueue.json`` and refuses; ``load_bootstrap`` and
    ``load_administration`` never open it and stay prompt and valid.
    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-9.
    """
    special(runtime.path)
    started = time.monotonic()
    with pytest.raises(loader.RuntimeFilesInvalid):
        load_ingress(runtime.path)
    bootstrap = loader.load_bootstrap(str(runtime.path))
    administration = loader.load_administration(str(runtime.path))
    assert time.monotonic() - started < 2, "a special enqueue file blocked a loader"
    assert bootstrap.credential.username == READER
    assert administration.writer.username == WRITER


def test_ingress_loader_never_opens_the_writer_file(runtime: Any) -> None:
    """A FIFO writer file with no peer leaves ``load_ingress`` prompt and valid.

    @spec PROTECTED-HOOK-SOURCE-6.
    """
    _fifo(runtime.path, "source_writer.json")
    started = time.monotonic()
    loaded = load_ingress(runtime.path)
    assert time.monotonic() - started < 1
    assert loaded.enqueue.username == ENQUEUE
    assert not hasattr(loaded, "writer")


def test_ingress_loads_without_a_writer_file(runtime: Any) -> None:
    """A probe or ingress only deployment carries no writer file, @spec PROTECTED-HOOK-SOURCE-6."""
    (runtime.path / "source_writer.json").unlink()
    assert load_ingress(runtime.path).enqueue.username == ENQUEUE


def test_bootstrap_and_administration_never_open_the_enqueue_file(runtime: Any) -> None:
    """A FIFO enqueue file with no peer leaves both loaders prompt and valid.

    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-9.
    """
    _fifo(runtime.path)
    started = time.monotonic()
    assert loader.load_bootstrap(str(runtime.path)).credential.username == READER
    assert loader.load_administration(str(runtime.path)).writer.username == WRITER
    assert time.monotonic() - started < 1


def test_exact_size_bound_and_symlinked_secret_volume_layout_are_accepted(runtime: Any) -> None:
    """A file of exactly 64 KiB and the ``..data`` symlink layout both load.

    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-9.
    """
    raw = json.dumps(valid_enqueue()).encode()
    runtime.files["enqueue.json"] = raw + b" " * (65536 - len(raw))
    runtime.write()
    assert len((runtime.path / "enqueue.json").read_bytes()) == 65536
    assert load_ingress(runtime.path).enqueue.username == ENQUEUE
    data = runtime.path / "..data"
    data.mkdir(mode=0o700)
    for name in list(runtime.files):
        (runtime.path / name).rename(data / name)
        (runtime.path / name).symlink_to(Path("..data") / name)
    loaded = load_ingress(runtime.path)
    assert loaded.enqueue.username == ENQUEUE
    assert loaded.bootstrap.credential.username == READER


def test_valid_file_parses_and_every_representation_redacts(runtime: Any) -> None:
    """``EnqueueCredential`` is frozen, slotted and redacted, @spec PROTECTED-HOOK-SOURCE-6."""
    from curie_protected_hooks import broker_transport

    assert hasattr(broker_transport, "EnqueueCredential"), "enqueue credential absent"
    loaded = load_ingress(runtime.path)
    assert type(loaded.enqueue) is broker_transport.EnqueueCredential
    assert (loaded.enqueue.username, loaded.enqueue.password) == (ENQUEUE, ENQUEUE_SECRET)
    assert type(loaded.bootstrap) is loader.RuntimeBootstrap
    assert loaded.bootstrap.credential.username == READER
    assert loaded.bootstrap.manifest.as_dict()["credential_refs"]["enqueue"] == ENQUEUE_REF
    assert not hasattr(loaded.enqueue, "__dict__")
    with pytest.raises((AttributeError, TypeError)):
        loaded.enqueue.password = "changed"
    with pytest.raises((AttributeError, TypeError)):
        loaded.enqueue = loaded.enqueue
    for value in (loaded, loaded.enqueue, loaded.bootstrap):
        text = repr(value) + str(value)
        for secret in (
            ENQUEUE,
            ENQUEUE_SECRET,
            READER,
            READER_SECRET,
            ENQUEUE_REF["id"],
            runtime.ca_pem.splitlines()[1],
        ):
            assert secret not in text


def test_the_enqueue_username_may_equal_the_writer_username(runtime: Any) -> None:
    """Not checked against the writer file, which ``load_ingress`` never opens.

    @spec PROTECTED-HOOK-SOURCE-6.
    """
    runtime.files["enqueue.json"] = enqueue_bytes(_with(username=WRITER))
    runtime.write()
    assert load_ingress(runtime.path).enqueue.username == WRITER
