"""The isolation baseline: what a claim may write, and the proof it left nothing.

These run unprivileged against a scratch tree that stands in for a root
filesystem. The runner child is another uid and group, so, as with a
root-owned image, only world-writable modes let it write. The built image is
checked as root by ``test_shell_container.py``.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from curie_shell_supervisor import isolation
from curie_shell_supervisor.isolation import Baseline, IpcObject, IsolationError, Writer

WRITER = Writer(uid=os.getuid() + 4242, gids=frozenset({os.getgid() + 4242}))


def _system(tmp_path: Path, *, mqueue: bool = True) -> tuple[Path, Path, Path, Path]:
    root = tmp_path / "fs"
    (root / "usr" / "lib").mkdir(parents=True)
    (root / "usr" / "lib" / "module.py").write_text("x")
    for scratch in ("tmp", "dev/shm", "dev/mqueue"):
        (root / scratch).mkdir(parents=True)
        (root / scratch).chmod(0o1777)
    (root / "proc").mkdir()
    # A kernel view the walk must not descend into.
    (root / "proc" / "anything").mkdir()
    (root / "proc" / "anything").chmod(0o777)
    mountinfo = tmp_path / "mountinfo"
    lines = [f"20 1 0:5 / {root / 'proc'} rw - proc proc rw"]
    if mqueue:
        lines.append(f"21 1 0:6 / {root / 'dev' / 'mqueue'} rw - mqueue mqueue rw")
    mountinfo.write_text("\n".join(lines) + "\n")
    sysvipc = tmp_path / "sysvipc"
    sysvipc.mkdir()
    for kind, column in isolation.IPC_KINDS:
        (sysvipc / kind).write_text(f"       key      {column} perms uid\n")
    return root, mountinfo, sysvipc, root / "tmp"


def _take(root: Path, mountinfo: Path, sysvipc: Path) -> Baseline:
    return Baseline.take(
        writer=WRITER,
        scratch_roots=[root / "tmp", root / "dev" / "shm", root / "dev" / "mqueue"],
        root=root,
        mountinfo=mountinfo,
        sysvipc=sysvipc,
    )


def test_a_path_the_child_could_write_outside_the_scratch_roots_refuses_the_shell(
    tmp_path: Path,
) -> None:
    root, mountinfo, sysvipc, _ = _system(tmp_path)
    (root / "run" / "lock").mkdir(parents=True)
    (root / "run" / "lock").chmod(0o1777)
    with pytest.raises(IsolationError, match="run/lock"):
        _take(root, mountinfo, sysvipc)


def test_without_an_mqueue_mount_the_shell_refuses(tmp_path: Path) -> None:
    root, mountinfo, sysvipc, _ = _system(tmp_path, mqueue=False)
    with pytest.raises(IsolationError, match="mqueue"):
        _take(root, mountinfo, sysvipc)


def test_restore_returns_every_scratch_root_and_proves_it(tmp_path: Path) -> None:
    root, mountinfo, sysvipc, tmp = _system(tmp_path)
    baseline = _take(root, mountinfo, sysvipc)
    nested = tmp / "deep" / "er"
    nested.mkdir(parents=True)
    (nested / "secret").write_text("left by a claim")
    nested.chmod(0o000)
    (tmp / "deep").chmod(0o000)
    (root / "dev" / "shm" / "sem.claim").write_text("posix semaphore")
    (root / "dev" / "mqueue" / "claimq").write_text("posix queue")
    assert baseline.restore(root=root, sysvipc=sysvipc) == []
    assert list(tmp.iterdir()) == []
    assert list((root / "dev" / "shm").iterdir()) == []
    # The scratch root itself keeps its mode: nothing above a removed entry changes.
    assert (tmp.stat().st_mode & 0o7777) == 0o1777


def test_restore_reports_a_path_that_became_writable_outside_the_scratch_roots(
    tmp_path: Path,
) -> None:
    root, mountinfo, sysvipc, _ = _system(tmp_path)
    baseline = _take(root, mountinfo, sysvipc)
    (root / "usr").chmod(0o777)
    problems = baseline.restore(root=root, sysvipc=sysvipc)
    assert any("usr is writable by the runner child" in problem for problem in problems)


def test_restore_removes_ipc_created_since_startup_and_reports_survivors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, mountinfo, sysvipc, _ = _system(tmp_path)
    baseline = _take(root, mountinfo, sysvipc)
    (sysvipc / "shm").write_text("       key      shmid perms uid\n        42      7 600 1000\n")
    (sysvipc / "msg").write_text("       key      msqid perms uid\n        43      9 600 1000\n")
    removed: list[IpcObject] = []

    def remove(objects: object, **_identity: object) -> None:
        for item in objects:  # type: ignore[attr-defined]
            removed.append(item)
            if item.kind == "msg":
                (sysvipc / "msg").write_text("       key      msqid perms uid\n")

    monkeypatch.setattr(isolation, "remove_ipc", remove)
    problems = baseline.restore(root=root, sysvipc=sysvipc)
    assert sorted(removed, key=lambda item: item.kind) == [
        IpcObject("msg", 9),
        IpcObject("shm", 7),
    ]
    # The segment did not go away: the shell must not report itself clean.
    assert problems == ["System V shm 7 remains"]


def test_ipc_present_before_any_claim_refuses_the_shell(tmp_path: Path) -> None:
    root, mountinfo, sysvipc, _ = _system(tmp_path)
    (sysvipc / "sem").write_text("       key      semid perms uid\n        1      3 666 0\n")
    with pytest.raises(IsolationError, match="System V"):
        _take(root, mountinfo, sysvipc)


def test_mount_points_with_spaces_are_read_back() -> None:
    assert isolation._unescape("/a\\040b") == "/a b"
