"""What one claim can leave for the next, and the proof that it left nothing.

An isolated runner child (its own uid and gid, never root) can change state
that outlives it in exactly two kinds of place:

1. Files: any directory or file its uid or gid may write. At startup the
   supervisor walks the whole filesystem (virtual filesystems excepted) and
   refuses to serve unless every such path lies in a scratch root (``/tmp``,
   ``/var/tmp``, ``/dev/shm``, ``/run/lock``, ``/dev/mqueue`` by default; POSIX
   shared memory, semaphores and message queues are files there). The claim's
   own directory is created later and wiped at retirement.
2. System V IPC: shared memory segments, semaphore sets and message queues,
   which no path names. They are listed from ``/proc/sysvipc``.

At retirement, after the child's processes are gone, every scratch root is
returned to the entries it held at startup, every IPC object created since
startup is removed, and then the whole check is run again. Only a shell that
passes it may take another claim.
"""

from __future__ import annotations

import contextlib
import ctypes
import os
import stat
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

# Filesystems with no persistent, child-writable state: walking them would
# only read kernel views.
VIRTUAL_FILESYSTEMS = frozenset(
    {
        "autofs",
        "binfmt_misc",
        "bpf",
        "cgroup",
        "cgroup2",
        "configfs",
        "debugfs",
        "devpts",
        "efivarfs",
        "fusectl",
        "hugetlbfs",
        "nsfs",
        "proc",
        "pstore",
        "rpc_pipefs",
        "securityfs",
        "selinuxfs",
        "sysfs",
        "tracefs",
    }
)
IPC_KINDS = (("shm", "shmid"), ("sem", "semid"), ("msg", "msqid"))
_IPC_RMID = 0
_REPORT_LIMIT = 20


class IsolationError(RuntimeError):
    """The shell cannot keep one claim's state from the next."""


@dataclass(frozen=True)
class Writer:
    """The runner child's identity: what its uid and groups may change."""

    uid: int
    gids: frozenset[int]

    def can_change(self, info: os.stat_result) -> bool:
        if info.st_uid == self.uid:
            # An owner can always chmod its way back to writing.
            return True
        if info.st_gid in self.gids and info.st_mode & stat.S_IWGRP:
            return True
        return bool(info.st_mode & stat.S_IWOTH)


def _unescape(field_value: str) -> str:
    # /proc/self/mountinfo escapes space, tab, newline and backslash as octal.
    for code, char in (("\\040", " "), ("\\011", "\t"), ("\\012", "\n"), ("\\134", "\\")):
        field_value = field_value.replace(code, char)
    return field_value


def mounts(mountinfo: Path = Path("/proc/self/mountinfo")) -> list[tuple[Path, str]]:
    """Every mount point with its filesystem type."""

    found: list[tuple[Path, str]] = []
    for line in mountinfo.read_text().splitlines():
        fields = line.split()
        if "-" not in fields:
            continue
        separator = fields.index("-")
        found.append((Path(_unescape(fields[4])), fields[separator + 1]))
    return found


def _within(path: Path, roots: Iterable[Path]) -> bool:
    return any(path == root or root in path.parents for root in roots)


def changeable_paths(writer: Writer, *, root: Path, skip: Iterable[Path]) -> list[Path]:
    """Every path under ``root`` whose content ``writer`` could change.

    Directories and regular files count by their mode and owner. Device nodes
    never do (``/dev/null`` is writable and holds nothing). A socket, a FIFO
    or a symlink counts when ``writer`` owns it, since only a writer could
    have made it. ``skip`` names mount points not to descend into.
    """

    skipped = frozenset(skip)
    found: list[Path] = []
    stack = [root]
    while stack:
        directory = stack.pop()
        try:
            entries = list(os.scandir(directory))
        except (FileNotFoundError, NotADirectoryError):
            continue
        for entry in entries:
            path = Path(entry.path)
            try:
                info = entry.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue
            kind = stat.S_IFMT(info.st_mode)
            if kind == stat.S_IFDIR:
                if path in skipped:
                    continue
                if writer.can_change(info):
                    found.append(path)
                stack.append(path)
            elif kind == stat.S_IFREG:
                if writer.can_change(info):
                    found.append(path)
            elif kind in (stat.S_IFCHR, stat.S_IFBLK):
                continue
            elif info.st_uid == writer.uid:
                found.append(path)
    return found


def snapshot(roots: Iterable[Path]) -> dict[Path, frozenset[str]]:
    """The entries under each existing scratch root, by path relative to it."""

    taken: dict[Path, frozenset[str]] = {}
    for base in roots:
        if not base.is_dir():
            continue
        names: set[str] = set()
        for parent, dirs, files in os.walk(base):
            for name in (*dirs, *files):
                names.add(os.path.relpath(os.path.join(parent, name), base))
        taken[base] = frozenset(names)
    return taken


def _remove(path: Path) -> None:
    """Remove ``path`` and everything below it, touching nothing above it."""

    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISDIR(info.st_mode):
        path.unlink()
        return
    # The child may have left directories it made unreadable to itself; root
    # still needs to list them, and only modes inside ``path`` change.
    with contextlib.suppress(OSError):
        os.chmod(path, stat.S_IRWXU)
    for entry in os.scandir(path):
        _remove(Path(entry.path))
    path.rmdir()


def reset_scratch(baseline: dict[Path, frozenset[str]]) -> list[Path]:
    """Return every scratch root to its startup entries; list what would not go."""

    stuck: list[Path] = []
    for base, kept in baseline.items():
        if not base.is_dir():
            stuck.append(base)
            continue
        for parent, dirs, files in os.walk(base):
            for name in list(dirs) + files:
                path = Path(parent, name)
                if os.path.relpath(path, base) in kept:
                    continue
                try:
                    _remove(path)
                except OSError:
                    stuck.append(path)
                if name in dirs:
                    dirs.remove(name)
    return stuck


@dataclass(frozen=True)
class IpcObject:
    kind: str
    ident: int


def ipc_objects(proc: Path = Path("/proc/sysvipc")) -> frozenset[IpcObject]:
    """Every System V IPC object in this IPC namespace."""

    found: set[IpcObject] = set()
    for kind, id_column in IPC_KINDS:
        lines = (proc / kind).read_text().splitlines()
        if not lines:
            continue
        header = lines[0].split()
        column = header.index(id_column)
        for line in lines[1:]:
            values = line.split()
            if len(values) > column:
                found.add(IpcObject(kind, int(values[column])))
    return frozenset(found)


def _libc() -> ctypes.CDLL:
    return ctypes.CDLL(None, use_errno=True)


def remove_ipc(objects: Iterable[IpcObject]) -> None:
    """Mark each object for removal (a segment goes once nothing attaches)."""

    libc = _libc()
    for item in objects:
        if item.kind == "shm":
            libc.shmctl(item.ident, _IPC_RMID, None)
        elif item.kind == "sem":
            libc.semctl(item.ident, 0, _IPC_RMID)
        else:
            libc.msgctl(item.ident, _IPC_RMID, None)


@dataclass
class Baseline:
    """What the shell held before its first claim, to return to after each."""

    writer: Writer | None
    scratch: dict[Path, frozenset[str]]
    skip: tuple[Path, ...] = ()
    ipc: frozenset[IpcObject] = field(default_factory=frozenset)

    @classmethod
    def take(
        cls,
        *,
        writer: Writer | None,
        scratch_roots: Iterable[Path],
        root: Path = Path("/"),
        mountinfo: Path = Path("/proc/self/mountinfo"),
        sysvipc: Path = Path("/proc/sysvipc"),
    ) -> Baseline:
        """Record the baseline; refuse a shell that cannot isolate claims.

        ``writer`` is ``None`` only for an unisolated test shell, which keeps
        just the scratch reset.
        """

        roots = tuple(scratch_roots)
        if writer is None:
            return cls(writer=None, scratch=snapshot(roots))
        table = mounts(mountinfo)
        skip = tuple(point for point, fstype in table if fstype in VIRTUAL_FILESYSTEMS)
        mqueue = [point for point, fstype in table if fstype == "mqueue"]
        if mqueue and not any(_within(point, roots) for point in mqueue):
            raise IsolationError(
                "the POSIX message queue mount is not a scratch root: "
                + ", ".join(str(point) for point in mqueue)
            )
        if not mqueue:
            raise IsolationError(
                "no mqueue filesystem is mounted, so POSIX message queues a claim "
                "creates could not be found and removed"
            )
        scratch = snapshot(roots)
        # A scratch root itself is writable by design. Anything already in one
        # at startup is reported too: a claim could change what the next reads.
        outside = [
            path for path in changeable_paths(writer, root=root, skip=skip) if path not in roots
        ]
        if outside:
            listed = ", ".join(str(path) for path in sorted(outside)[:_REPORT_LIMIT])
            raise IsolationError(
                f"{len(outside)} path(s) outside the scratch roots are writable by the "
                f"runner child: {listed}"
            )
        ipc = ipc_objects(sysvipc)
        if ipc:
            raise IsolationError(f"{len(ipc)} System V IPC object(s) exist before any claim ran")
        return cls(writer=writer, scratch=scratch, skip=skip, ipc=ipc)

    def restore(
        self,
        *,
        root: Path = Path("/"),
        sysvipc: Path = Path("/proc/sysvipc"),
    ) -> list[str]:
        """Remove what a claim left, then prove it; return what still differs."""

        problems = [f"could not remove {path}" for path in reset_scratch(self.scratch)]
        if self.writer is None:
            return problems
        remove_ipc(ipc_objects(sysvipc) - self.ipc)
        left = ipc_objects(sysvipc) - self.ipc
        problems += [
            f"System V {item.kind} {item.ident} remains"
            for item in sorted(left, key=lambda item: (item.kind, item.ident))
        ]
        roots = tuple(self.scratch)
        for path in changeable_paths(self.writer, root=root, skip=self.skip):
            if path not in roots:
                problems.append(f"{path} is writable by the runner child")
        for base, entries in snapshot(roots).items():
            if entries != self.scratch[base]:
                problems.append(f"{base} differs from its startup contents")
        return problems[:_REPORT_LIMIT]
