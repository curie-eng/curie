"""The commissioning directory reader is strict, @spec PROTECTED-COMMISSIONING-2.

The provisioner writes the commissioning directory itself; it is not a Secret
volume. No path component may be a symlink, and the directory and each file
must be owned by the expected user and writable by neither group nor others.
These cases need no broker: they drive the reader on placeholder bytes.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from curie_commissioning_ingress import directory as reader

OWNER = os.geteuid()


def provisioned(root: Path) -> Path:
    root.mkdir(mode=0o700)
    path = root / "file.json"
    path.write_bytes(b"{}")
    path.chmod(0o600)
    return root


def read(root: Path, owner: int = OWNER) -> bytes:
    descriptor = reader._open_directory(str(root), owner)
    try:
        return reader._read_file(descriptor, "file.json", owner)
    finally:
        os.close(descriptor)


def test_an_owned_private_regular_file_reads(tmp_path: Path) -> None:
    assert read(provisioned(tmp_path / "root")) == b"{}"
    # Read only is fine; only shared write is refused.
    (tmp_path / "root" / "file.json").chmod(0o444)
    (tmp_path / "root").chmod(0o755)
    assert read(tmp_path / "root") == b"{}"


def test_a_symlinked_file_refuses(tmp_path: Path) -> None:
    root = provisioned(tmp_path / "root")
    (root / "file.json").rename(tmp_path / "elsewhere.json")
    (root / "file.json").symlink_to(tmp_path / "elsewhere.json")
    with pytest.raises(OSError):
        read(root)


def test_a_symlink_at_any_directory_component_refuses(tmp_path: Path) -> None:
    root = provisioned(tmp_path / "root")
    (tmp_path / "alias").symlink_to(tmp_path)
    for path in (tmp_path / "alias" / "root", tmp_path / "link"):
        if path.name == "link":
            path.symlink_to(root)
        with pytest.raises(Exception):  # noqa: B017  The reader refuses without detail.
            read(path)


@pytest.mark.parametrize("mode", [0o620, 0o602, 0o666])
def test_a_shared_writable_file_refuses(tmp_path: Path, mode: int) -> None:
    root = provisioned(tmp_path / "root")
    (root / "file.json").chmod(mode)
    with pytest.raises(reader.CommissioningDirectoryInvalid):
        read(root)


@pytest.mark.parametrize("mode", [0o720, 0o702, 0o777])
def test_a_shared_writable_directory_refuses(tmp_path: Path, mode: int) -> None:
    root = provisioned(tmp_path / "root")
    root.chmod(mode)
    try:
        with pytest.raises(reader.CommissioningDirectoryInvalid):
            read(root)
    finally:
        root.chmod(0o700)


def test_another_owner_refuses(tmp_path: Path) -> None:
    with pytest.raises(reader.CommissioningDirectoryInvalid):
        read(provisioned(tmp_path / "root"), owner=OWNER + 1)


def test_a_non_regular_or_oversized_file_refuses(tmp_path: Path) -> None:
    root = provisioned(tmp_path / "root")
    (root / "file.json").unlink()
    os.mkfifo(root / "file.json", 0o600)
    with pytest.raises(reader.CommissioningDirectoryInvalid):
        read(root)
    (root / "file.json").unlink()
    (root / "file.json").write_bytes(b" " * (reader.MAX_FILE_BYTES + 1))
    (root / "file.json").chmod(0o600)
    with pytest.raises(reader.CommissioningDirectoryInvalid):
        read(root)


def test_the_loader_refuses_a_shared_writable_directory(tmp_path: Path) -> None:
    root = provisioned(tmp_path / "root")
    root.chmod(0o777)
    try:
        with pytest.raises(reader.CommissioningDirectoryInvalid):
            reader.load_commissioning_directory(str(root), owner=OWNER)
    finally:
        root.chmod(0o700)
