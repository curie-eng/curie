"""Provisioner runtime directory writer over real files, @spec PROTECTED-HOOK-SOURCE-11.

Units of ``curie_protected_hooks.runtime_directory_writer``: the rendered file
mapping and its canonical bytes, the host directory publication (modes, relative
``current`` link, atomic rotation, previous version retention), refused roots
and refused inputs that write nothing, and redaction. No mocks.
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import stat
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.x509.oid import NameOID
from curie_protected_hooks import runtime_directory, runtime_directory_writer
from curie_protected_hooks.authority_records import Manifest, parse_manifest
from curie_protected_hooks.broker_transport import (
    EnqueueCredential,
    MetadataReaderCredential,
    SourceWriterCredential,
)
from curie_protected_hooks.runtime_directory import (
    RuntimeDirectoryInvalid,
    parse_bootstrap,
    parse_enqueue,
    parse_source_writer,
)
from curie_protected_hooks.runtime_directory_writer import (
    RuntimeDirectoryContents,
    publish_runtime_directory,
    render_runtime_files,
)

READER = "reader-fixture"
READER_SECRET = "reader-fixture-secret-0123456789"
WRITER = "writer-fixture"
WRITER_SECRET = "writer-fixture-secret-0123456789"
ENQUEUE = "enqueue-fixture"
ENQUEUE_SECRET = "enqueue-fixture-secret-0123456789"
SECRETS = (READER, READER_SECRET, WRITER, WRITER_SECRET, ENQUEUE, ENQUEUE_SECRET)


def ca_pem() -> str:
    """PEM of a fresh self-signed CA, @spec PROTECTED-HOOK-SOURCE-11."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.now(UTC)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "fixture-writer-ca")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return cert.public_bytes(serialization.Encoding.PEM).decode()


def manifest_value() -> dict[str, Any]:
    """Closed v1 manifest for one loopback broker, @spec PROTECTED-HOOK-SOURCE-11."""
    return {
        "schema_version": 1,
        "runtime_id": "33333333-3333-4333-8333-333333333333",
        "runtime_generation": "1",
        "broker_identity": {
            "instance_id": "11111111-1111-4111-8111-111111111111",
            "endpoint": {"host": "127.0.0.1", "port": 6379},
            "tls_server_name": "127.0.0.1",
            "tls_spki_sha256": "a" * 64,
            "run_id": "b" * 40,
            "database": 0,
        },
        "worker_image_digest": "sha256:" + "d" * 64,
        "runner_image_digest": "sha256:" + "e" * 64,
        "bundle_digest": {"sha256": "f" * 64, "object_identity": "bundle/example"},
        "execution_config_digest": "1" * 64,
        "qualification_id": "44444444-4444-4444-8444-444444444444",
        "substrate": {
            "kind": "docker",
            "authority_domain_id": "55555555-5555-4555-8555-555555555555",
            "launch_identity": "launch/example",
            "launch_config_sha256": "2" * 64,
        },
        "guard_identity": {
            "control_id": "22222222-2222-4222-8222-222222222222",
            "revision": "1",
            "config_sha256": "c" * 64,
        },
        "credential_refs": {
            role: {"id": "credential/example-" + role, "generation": "1"}
            for role in ("enqueue", "worker", "verifier")
        },
    }


def make_manifest() -> Manifest:
    """The fixture manifest parsed from canonical bytes, @spec PROTECTED-HOOK-SOURCE-11."""
    raw = json.dumps(manifest_value(), sort_keys=True, separators=(",", ":")).encode()
    return parse_manifest(raw)


def contents(
    *, full: bool = True, max_readiness_ms: int = 60000, ca: str | None = None
) -> RuntimeDirectoryContents:
    """Typed writer inputs; optional principals only when full, @spec PROTECTED-HOOK-SOURCE-11."""
    return RuntimeDirectoryContents(
        manifest=make_manifest(),
        ca_pem=ca if ca is not None else ca_pem(),
        max_readiness_ms=max_readiness_ms,
        control_reader=MetadataReaderCredential(READER, READER_SECRET),
        source_writer=SourceWriterCredential(WRITER, WRITER_SECRET) if full else None,
        enqueue=EnqueueCredential(ENQUEUE, ENQUEUE_SECRET) if full else None,
    )


def safe_root(tmp_path: Path, name: str = "root") -> Path:
    """A provisioner owned root with mode 0700, @spec PROTECTED-HOOK-SOURCE-11."""
    root = tmp_path / name
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    return root


def versions(root: Path) -> list[str]:
    """Version directory names inside the root, @spec PROTECTED-HOOK-SOURCE-11."""
    return sorted(
        entry.name for entry in root.iterdir() if re.fullmatch(r"v\.[0-9a-f]{32}", entry.name)
    )


def snapshot(root: Path) -> list[str]:
    """Every entry name inside the root, @spec PROTECTED-HOOK-SOURCE-11."""
    return sorted(entry.name for entry in root.iterdir())


def read_current(root: Path) -> dict[str, bytes]:
    """Every file under ``root/current``, @spec PROTECTED-HOOK-SOURCE-11."""
    directory = root / "current"
    return {entry.name: entry.read_bytes() for entry in directory.iterdir()}


def test_render_keys_full_and_bootstrap_only() -> None:
    """Three files always, the optional two only when given, @spec PROTECTED-HOOK-SOURCE-11."""
    assert set(render_runtime_files(contents(full=False))) == {
        "manifest.json",
        "ca.pem",
        "bootstrap.json",
    }
    assert set(render_runtime_files(contents())) == {
        "manifest.json",
        "ca.pem",
        "bootstrap.json",
        "source_writer.json",
        "enqueue.json",
    }


def test_render_canonical_bytes() -> None:
    """Sorted compact ASCII JSON with a decimal string readiness, @spec PROTECTED-HOOK-SOURCE-11."""
    value = contents()
    files = render_runtime_files(value)
    assert files["manifest.json"] == value.manifest.canonical_bytes
    assert files["ca.pem"] == value.ca_pem.encode("ascii")
    assert files["bootstrap.json"] == (
        b'{"control_reader":{"password":"' + READER_SECRET.encode() + b'","username":"'
        b"" + READER.encode() + b'"},"max_readiness_ms":"60000","schema_version":1}'
    )
    for name in ("bootstrap.json", "source_writer.json", "enqueue.json"):
        raw = files[name]
        raw.decode("ascii")
        parsed = json.loads(raw)
        assert (
            raw
            == json.dumps(parsed, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
        )
    assert json.loads(files["bootstrap.json"])["max_readiness_ms"] == "60000"
    assert json.loads(files["source_writer.json"]) == {
        "schema_version": 1,
        "source_writer": {"username": WRITER, "password": WRITER_SECRET},
    }


def test_render_enqueue_ref_comes_from_manifest() -> None:
    """The enqueue credential_ref is the manifest's own, @spec PROTECTED-HOOK-SOURCE-11."""
    value = contents()
    parsed = json.loads(render_runtime_files(value)["enqueue.json"])
    assert parsed["credential_ref"] == value.manifest.as_dict()["credential_refs"]["enqueue"]
    assert parsed["enqueue"] == {"username": ENQUEUE, "password": ENQUEUE_SECRET}
    assert parsed["schema_version"] == 1


def test_publish_layout_and_modes(tmp_path: Path) -> None:
    """A relative current link to a 0700 version of 0400 files, @spec PROTECTED-HOOK-SOURCE-11."""
    root = safe_root(tmp_path)
    value = contents()
    result = publish_runtime_directory(str(root), value)
    assert result == f"{root}/current"
    link = root / "current"
    assert link.is_symlink()
    target = os.readlink(link)
    assert not os.path.isabs(target)
    assert target in versions(root)
    version = root / target
    assert stat.S_IMODE(version.stat().st_mode) == 0o700
    expected = render_runtime_files(value)
    assert read_current(root) == expected
    for entry in version.iterdir():
        assert stat.S_IMODE(entry.lstat().st_mode) == 0o400
        assert stat.S_ISREG(entry.lstat().st_mode)
    assert set(snapshot(root)) == {"current", target}


def test_publish_bootstrap_only_writes_three_files(tmp_path: Path) -> None:
    """Missing optional principals write no file, @spec PROTECTED-HOOK-SOURCE-11."""
    root = safe_root(tmp_path)
    publish_runtime_directory(str(root), contents(full=False))
    assert set(read_current(root)) == {"manifest.json", "ca.pem", "bootstrap.json"}


def test_rotation_keeps_previous_and_open_descriptor(tmp_path: Path) -> None:
    """Rotation switches current, keeps one previous version, @spec PROTECTED-HOOK-SOURCE-11."""
    root = safe_root(tmp_path)
    first_contents = contents(max_readiness_ms=1000)
    publish_runtime_directory(str(root), first_contents)
    first = os.readlink(root / "current")
    held = os.open(root / "current", os.O_RDONLY | os.O_DIRECTORY)
    try:
        publish_runtime_directory(str(root), contents(max_readiness_ms=2000))
        second = os.readlink(root / "current")
        assert second != first
        assert set(versions(root)) == {first, second}
        for name, payload in render_runtime_files(first_contents).items():
            descriptor = os.open(name, os.O_RDONLY, dir_fd=held)
            try:
                assert os.read(descriptor, 65537) == payload
            finally:
                os.close(descriptor)
        assert json.loads(read_current(root)["bootstrap.json"])["max_readiness_ms"] == "2000"
        publish_runtime_directory(str(root), contents(max_readiness_ms=3000))
        third = os.readlink(root / "current")
        assert set(versions(root)) == {second, third}
        assert first not in versions(root)
    finally:
        os.close(held)


def test_unknown_entries_never_removed(tmp_path: Path) -> None:
    """Entries the writer did not create stay, @spec PROTECTED-HOOK-SOURCE-11."""
    root = safe_root(tmp_path)
    (root / "keep.txt").write_text("x")
    (root / "v.short").mkdir()
    (root / "other-dir").mkdir()
    for _ in range(4):
        publish_runtime_directory(str(root), contents())
    assert {"keep.txt", "v.short", "other-dir"} <= set(snapshot(root))
    assert len(versions(root)) == 2


def test_refused_publish_changes_nothing(tmp_path: Path) -> None:
    """Invalid input leaves current and entries untouched, @spec PROTECTED-HOOK-SOURCE-11."""
    root = safe_root(tmp_path)
    publish_runtime_directory(str(root), contents())
    before = snapshot(root)
    link = os.readlink(root / "current")
    for bad in (0, -1, 9007199254740992):
        with pytest.raises(RuntimeDirectoryInvalid):
            publish_runtime_directory(str(root), contents(max_readiness_ms=bad))
    with pytest.raises(RuntimeDirectoryInvalid):
        publish_runtime_directory(str(root), contents(ca="not a certificate"))
    assert snapshot(root) == before
    assert os.readlink(root / "current") == link


def test_refused_first_publish_writes_nothing(tmp_path: Path) -> None:
    """A refused first publication leaves an empty root, @spec PROTECTED-HOOK-SOURCE-11."""
    root = safe_root(tmp_path)
    with pytest.raises(RuntimeDirectoryInvalid):
        publish_runtime_directory(str(root), contents(max_readiness_ms=0))
    assert snapshot(root) == []


def test_symlink_root_refused(tmp_path: Path) -> None:
    """A root that is a symlink to a directory is refused, @spec PROTECTED-HOOK-SOURCE-11."""
    real = safe_root(tmp_path, "real")
    link = tmp_path / "link"
    link.symlink_to(real)
    with pytest.raises(RuntimeDirectoryInvalid):
        publish_runtime_directory(str(link), contents())
    assert snapshot(real) == []


@pytest.mark.parametrize("mode", [0o770, 0o707, 0o777])
def test_group_or_world_writable_root_refused(tmp_path: Path, mode: int) -> None:
    """A root writable by group or others is refused, @spec PROTECTED-HOOK-SOURCE-11."""
    root = safe_root(tmp_path)
    root.chmod(mode)
    try:
        with pytest.raises(RuntimeDirectoryInvalid):
            publish_runtime_directory(str(root), contents())
        assert snapshot(root) == []
    finally:
        root.chmod(0o700)


def test_missing_root_refused(tmp_path: Path) -> None:
    """A missing root is refused and not created, @spec PROTECTED-HOOK-SOURCE-11."""
    root = tmp_path / "absent"
    with pytest.raises(RuntimeDirectoryInvalid):
        publish_runtime_directory(str(root), contents())
    assert not root.exists()


def test_current_real_directory_refused(tmp_path: Path) -> None:
    """A real ``current`` directory is never replaced, @spec PROTECTED-HOOK-SOURCE-11."""
    root = safe_root(tmp_path)
    (root / "current").mkdir()
    (root / "current" / "marker").write_text("x")
    with pytest.raises(RuntimeDirectoryInvalid):
        publish_runtime_directory(str(root), contents())
    assert snapshot(root) == ["current"]
    assert (root / "current" / "marker").read_text() == "x"


def test_repr_and_errors_hold_no_secret(tmp_path: Path) -> None:
    """Neither repr nor refusal text carries a credential or CA, @spec PROTECTED-HOOK-SOURCE-11."""
    value = contents()
    text = repr(value)
    for secret in (*SECRETS, value.ca_pem, "BEGIN CERTIFICATE"):
        assert secret not in text
    root = safe_root(tmp_path)
    root.chmod(0o770)
    try:
        with pytest.raises(RuntimeDirectoryInvalid) as raised:
            publish_runtime_directory(str(root), value)
    finally:
        root.chmod(0o700)
    message = str(raised.value) + repr(raised.value)
    for secret in (*SECRETS, value.ca_pem):
        assert secret not in message
    with pytest.raises(RuntimeDirectoryInvalid) as refused:
        render_runtime_files(contents(max_readiness_ms=0))
    for secret in SECRETS:
        assert secret not in str(refused.value) + repr(refused.value)


def small_ec_ca_pem() -> str:
    """PEM of a small EC CA, shorter than the manifest, @spec PROTECTED-HOOK-SOURCE-11."""
    key = ec.generate_private_key(ec.SECP256R1())
    now = datetime.now(UTC)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "ec")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return cert.public_bytes(serialization.Encoding.PEM).decode()


def tree_state(root: Path) -> dict[str, Any]:
    """Names, modes and bytes of everything under root, @spec PROTECTED-HOOK-SOURCE-11."""
    state: dict[str, Any] = {}
    for path in sorted(root.rglob("*")):
        mode = path.lstat().st_mode
        key = str(path.relative_to(root))
        if stat.S_ISLNK(mode):
            state[key] = ("link", os.readlink(path))
        elif stat.S_ISREG(mode):
            state[key] = (stat.S_IMODE(mode), path.read_bytes())
        else:
            state[key] = (stat.S_IMODE(mode), None)
    return state


def test_overlapping_publications_leave_complete_current(tmp_path: Path) -> None:
    """Concurrent publishers always leave a whole file set, @spec PROTECTED-HOOK-SOURCE-11."""
    for round_number in range(5):
        root = safe_root(tmp_path, f"race-{round_number}")
        first, second = contents(max_readiness_ms=1000), contents(max_readiness_ms=2000)
        expected = [render_runtime_files(first), render_runtime_files(second)]
        errors: list[BaseException] = []
        barrier = threading.Barrier(2)

        def work(
            value: RuntimeDirectoryContents,
            root: Path = root,
            barrier: threading.Barrier = barrier,
            errors: list[BaseException] = errors,
        ) -> None:
            """@spec PROTECTED-HOOK-SOURCE-11."""
            try:
                barrier.wait()
                for _ in range(3):
                    publish_runtime_directory(str(root), value)
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        threads = [threading.Thread(target=work, args=(v,)) for v in (first, second)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert errors == []
        assert read_current(root) in expected
        assert len(versions(root)) <= 2
        assert not [name for name in snapshot(root) if name.startswith(".current.")]


def failing_once(target: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Make ``os.<target>`` raise OSError on its next call only, @spec PROTECTED-HOOK-SOURCE-11."""
    real = getattr(os, target)
    state = {"armed": True}

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        """@spec PROTECTED-HOOK-SOURCE-11."""
        if state["armed"]:
            state["armed"] = False
            raise OSError("injected")
        return real(*args, **kwargs)

    monkeypatch.setattr(runtime_directory_writer.os, target, wrapper)


@pytest.mark.parametrize("target", ["replace", "symlink"])
def test_failure_while_publishing_leaves_current(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, target: str
) -> None:
    """A failed second publication changes nothing, @spec PROTECTED-HOOK-SOURCE-11."""
    root = safe_root(tmp_path)
    publish_runtime_directory(str(root), contents(max_readiness_ms=1000))
    first = os.readlink(root / "current")
    before = tree_state(root)
    failing_once(target, monkeypatch)
    with pytest.raises(RuntimeDirectoryInvalid):
        publish_runtime_directory(str(root), contents(max_readiness_ms=2000))
    assert os.readlink(root / "current") == first
    assert tree_state(root) == before
    assert versions(root) == [first]
    assert not [name for name in snapshot(root) if name.startswith(".current.")]


def test_relative_and_unnormalized_roots_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Relative and unnormalized roots are refused, @spec PROTECTED-HOOK-SOURCE-11."""
    root = safe_root(tmp_path)
    other = safe_root(tmp_path, "other")
    monkeypatch.chdir(tmp_path)
    for text in ("root", f"{root}/", f"{root}/.", f"{other}/../root", f"{tmp_path}/./root"):
        with pytest.raises(RuntimeDirectoryInvalid):
            publish_runtime_directory(text, contents())
    assert snapshot(root) == []


def test_symlink_ancestor_refused(tmp_path: Path) -> None:
    """A symlink anywhere in the root path is refused, @spec PROTECTED-HOOK-SOURCE-11."""
    real = safe_root(tmp_path, "real")
    inner = real / "root"
    inner.mkdir(mode=0o700)
    (tmp_path / "link").symlink_to(real)
    with pytest.raises(RuntimeDirectoryInvalid):
        publish_runtime_directory(str(tmp_path / "link" / "root"), contents())
    assert snapshot(inner) == []


def test_ancestor_writability(tmp_path: Path) -> None:
    """A world-writable ancestor needs the sticky bit, @spec PROTECTED-HOOK-SOURCE-11."""
    ancestor = safe_root(tmp_path, "shared")
    root = ancestor / "root"
    root.mkdir(mode=0o700)
    try:
        ancestor.chmod(0o777)
        with pytest.raises(RuntimeDirectoryInvalid):
            publish_runtime_directory(str(root), contents())
        assert snapshot(root) == []
        ancestor.chmod(0o1777)
        assert publish_runtime_directory(str(root), contents()) == f"{root}/current"
    finally:
        ancestor.chmod(0o700)


def test_tmp_path_itself_is_accepted(tmp_path: Path) -> None:
    """The test temp directory is a valid root, @spec PROTECTED-HOOK-SOURCE-11."""
    assert publish_runtime_directory(str(tmp_path), contents()) == f"{tmp_path}/current"


@pytest.mark.parametrize("target", ["elsewhere", "/etc"])
def test_foreign_current_symlink_refused(tmp_path: Path, target: str) -> None:
    """A current link not naming a version is refused untouched, @spec PROTECTED-HOOK-SOURCE-11."""
    root = safe_root(tmp_path)
    (root / "current").symlink_to(target)
    before = tree_state(root)
    with pytest.raises(RuntimeDirectoryInvalid):
        publish_runtime_directory(str(root), contents())
    assert tree_state(root) == before


def test_lookalike_versions_survive_and_stale_temp_links_go(tmp_path: Path) -> None:
    """Lookalike versions survive, stale temp links go, @spec PROTECTED-HOOK-SOURCE-11."""
    root = safe_root(tmp_path)
    nested = root / ("v." + "1" * 32)
    nested.mkdir(mode=0o700)
    (nested / "manifest.json").write_bytes(b"x")
    (nested / "sub").mkdir()
    unknown = root / ("v." + "2" * 32)
    unknown.mkdir(mode=0o700)
    (unknown / "notes.txt").write_bytes(b"x")
    loose = root / ("v." + "3" * 32)
    loose.mkdir()
    (loose / "manifest.json").write_bytes(b"x")
    (loose / "manifest.json").chmod(0o400)
    loose.chmod(0o755)
    stale = root / (".current." + "4" * 32)
    stale.symlink_to("v." + "5" * 32)
    shapes = {name: tree_state(root / name) for name in (nested.name, unknown.name, loose.name)}
    for _ in range(4):
        publish_runtime_directory(str(root), contents())
    for name, state in shapes.items():
        assert tree_state(root / name) == state
    assert not stale.is_symlink()
    assert len(versions(root)) == 5


class AfterSwap:
    """Fault injection that arms once ``os.replace`` has run, @spec PROTECTED-HOOK-SOURCE-11."""

    def __init__(
        self, monkeypatch: pytest.MonkeyPatch, root: Path, fault: Any, target: str
    ) -> None:
        """@spec PROTECTED-HOOK-SOURCE-11."""
        self.swapped = False
        self.hits = 0
        self.root_inode = root.stat().st_ino
        self.fault = fault
        real_replace, real_fsync = os.replace, os.fsync
        real_fcntl, real_listdir = fcntl.fcntl, os.listdir

        def replace(*args: Any, **kwargs: Any) -> Any:
            """@spec PROTECTED-HOOK-SOURCE-11."""
            result = real_replace(*args, **kwargs)
            self.swapped = True
            return result

        def trip() -> None:
            """@spec PROTECTED-HOOK-SOURCE-11."""
            if self.swapped:
                self.hits += 1
                if self.hits == 1:
                    raise self.fault

        def fsync(descriptor: int) -> None:
            """@spec PROTECTED-HOOK-SOURCE-11."""
            if target == "flush":
                trip()
            real_fsync(descriptor)

        def fcntl_call(descriptor: int, command: int, *rest: Any) -> Any:
            """@spec PROTECTED-HOOK-SOURCE-11."""
            if target == "flush" and command == getattr(fcntl, "F_FULLFSYNC", None):
                trip()
            return real_fcntl(descriptor, command, *rest)

        def listdir(path: Any = ".") -> Any:
            """@spec PROTECTED-HOOK-SOURCE-11."""
            if target == "listdir" and isinstance(path, int):
                if os.fstat(path).st_ino == self.root_inode:
                    trip()
            return real_listdir(path)

        monkeypatch.setattr(os, "replace", replace)
        monkeypatch.setattr(os, "fsync", fsync)
        monkeypatch.setattr(fcntl, "fcntl", fcntl_call)
        monkeypatch.setattr(os, "listdir", listdir)


def second_publication(root: Path) -> tuple[str, dict[str, bytes]]:
    """Publish once and return the first version name, @spec PROTECTED-HOOK-SOURCE-11."""
    publish_runtime_directory(str(root), contents(max_readiness_ms=1000))
    return os.readlink(root / "current"), read_current(root)


@pytest.mark.parametrize("target", ["flush", "listdir"])
def test_failure_after_swap_is_best_effort(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, target: str
) -> None:
    """A failed flush or listing after the swap is ignored, @spec PROTECTED-HOOK-SOURCE-11."""
    root = safe_root(tmp_path)
    first, _ = second_publication(root)
    injected = AfterSwap(monkeypatch, root, OSError("injected"), target)
    publish_runtime_directory(str(root), contents(max_readiness_ms=2000))
    assert injected.hits >= 1
    assert os.readlink(root / "current") != first
    assert json.loads(read_current(root)["bootstrap.json"])["max_readiness_ms"] == "2000"


@pytest.mark.parametrize("target", ["flush", "listdir"])
def test_interrupt_after_swap_keeps_new_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, target: str
) -> None:
    """An interrupt after the swap keeps the new version, @spec PROTECTED-HOOK-SOURCE-11."""
    root = safe_root(tmp_path)
    first, _ = second_publication(root)
    injected = AfterSwap(monkeypatch, root, KeyboardInterrupt(), target)
    with pytest.raises(KeyboardInterrupt):
        publish_runtime_directory(str(root), contents(max_readiness_ms=2000))
    assert injected.hits == 1
    assert os.readlink(root / "current") != first
    expected = render_runtime_files(contents(max_readiness_ms=2000))
    assert set(read_current(root)) == set(expected)
    assert json.loads(read_current(root)["bootstrap.json"])["max_readiness_ms"] == "2000"


def test_failure_while_writing_version_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A write failing on the second file changes nothing, @spec PROTECTED-HOOK-SOURCE-11."""
    root = safe_root(tmp_path)
    first, first_bytes = second_publication(root)
    before = tree_state(root)
    real_write = os.write
    calls = {"count": 0}

    def write(descriptor: int, data: Any) -> int:
        """@spec PROTECTED-HOOK-SOURCE-11."""
        calls["count"] += 1
        if calls["count"] == 2:
            raise OSError("injected")
        return real_write(descriptor, data)

    monkeypatch.setattr(os, "write", write)
    with pytest.raises(RuntimeDirectoryInvalid):
        publish_runtime_directory(str(root), contents(max_readiness_ms=2000))
    monkeypatch.undo()
    assert calls["count"] >= 2
    assert os.readlink(root / "current") == first
    assert read_current(root) == first_bytes
    assert tree_state(root) == before
    assert versions(root) == [first]
    assert not [name for name in snapshot(root) if name.startswith(".current.")]


def test_refusals_carry_no_exception_context(tmp_path: Path) -> None:
    """Refusals have neither cause nor context, @spec PROTECTED-HOOK-SOURCE-11."""
    root = safe_root(tmp_path)
    root.chmod(0o770)
    try:
        with pytest.raises(RuntimeDirectoryInvalid) as bad_root:
            publish_runtime_directory(str(root), contents())
    finally:
        root.chmod(0o700)
    with pytest.raises(RuntimeDirectoryInvalid) as bad_input:
        publish_runtime_directory(str(root), contents(max_readiness_ms=0))
    with pytest.raises(RuntimeDirectoryInvalid) as bad_render:
        render_runtime_files(contents(max_readiness_ms=0))
    files = render_runtime_files(contents())
    reader = MetadataReaderCredential(READER, READER_SECRET)
    bootstrap = parse_bootstrap(files["manifest.json"], files["ca.pem"], files["bootstrap.json"])
    with pytest.raises(RuntimeDirectoryInvalid) as bad_bootstrap:
        parse_bootstrap(b"{", b"x", b"{")
    with pytest.raises(RuntimeDirectoryInvalid) as bad_writer:
        parse_source_writer(b"{", reader)
    with pytest.raises(RuntimeDirectoryInvalid) as bad_enqueue:
        parse_enqueue(b"{", bootstrap)
    for raised in (bad_root, bad_input, bad_render, bad_bootstrap, bad_writer, bad_enqueue):
        assert raised.value.__context__ is None
        assert raised.value.__cause__ is None


def test_real_size_bound_on_ca_and_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    """The byte bound itself refuses a valid CA or manifest, @spec PROTECTED-HOOK-SOURCE-11."""
    big = contents(ca=ca_pem() + ca_pem())
    big_files = render_runtime_files(big)
    small = contents(ca=small_ec_ca_pem())
    small_files = render_runtime_files(small)
    names = ("manifest.json", "ca.pem", "bootstrap.json")
    cases = [
        (big_files, len(big_files["ca.pem"]) - 1),
        (small_files, len(small_files["manifest.json"]) - 1),
    ]
    assert len(big_files["ca.pem"]) > max(
        len(big_files[n]) for n in ("manifest.json", "bootstrap.json")
    )
    assert len(small_files["ca.pem"]) < len(small_files["manifest.json"]) - 1
    assert len(small_files["bootstrap.json"]) < len(small_files["manifest.json"]) - 1
    for files, bound in cases:
        parse_bootstrap(*(files[n] for n in names))
        with monkeypatch.context() as patch:
            patch.setattr(runtime_directory, "MAX_FILE_BYTES", bound)
            with pytest.raises(RuntimeDirectoryInvalid):
                parse_bootstrap(*(files[n] for n in names))
        parse_bootstrap(*(files[n] for n in names))
