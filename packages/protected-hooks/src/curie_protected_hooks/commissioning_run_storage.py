"""Private descriptor-pinned run custody; no broker writes, @spec COMMISSIONING-SERIES-2."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import secrets
import stat
from pathlib import Path

from .authority_records import _pairs, _reject_number
from .commissioning_local_ownership import OwnershipLedger, _entry, valid_owner
from .commissioning_run_records import RunEvent, RunIntent, RunPlan
from .runtime_directory import RuntimeDirectoryInvalid
from .runtime_directory_writer import _flush, _open_root

_ZERO = "0" * 64
_HEX = re.compile(r"[0-9a-f]{64}")
_DIR = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_READ = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
_WRITE = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
_FORK_EPOCH = 0


class RunCustodyRefused(RuntimeError):
    """Never echo paths, ownership or record contents, @spec COMMISSIONING-SERIES-2."""

    def __init__(self) -> None:
        """A fixed refusal, @spec COMMISSIONING-SERIES-2."""
        super().__init__("Commissioning run custody refused")


def _require(value: bool) -> None:
    if not value:
        raise RunCustodyRefused()


def _after_fork() -> None:
    global _FORK_EPOCH
    _FORK_EPOCH += 1


os.register_at_fork(after_in_child=_after_fork)


def _private(fd: int, *, directory: bool = False) -> os.stat_result:
    value = os.fstat(fd)
    _require(value.st_uid == os.geteuid())
    _require(stat.S_IMODE(value.st_mode) == (0o700 if directory else 0o600))
    _require(stat.S_ISDIR(value.st_mode) if directory else stat.S_ISREG(value.st_mode))
    if not directory:
        _require(value.st_nlink == 1)
    return value


def _identity(value: os.stat_result) -> tuple[int, int]:
    return value.st_dev, value.st_ino


def _read_at(directory: int, name: str, maximum: int = 16384) -> bytes:
    fd = os.open(name, _READ, dir_fd=directory)
    try:
        info = _private(fd)
        _require(0 <= info.st_size <= maximum)
        chunks = bytearray()
        while len(chunks) <= maximum:
            part = os.read(fd, min(65536, maximum + 1 - len(chunks)))
            if not part:
                break
            chunks.extend(part)
        _require(len(chunks) <= maximum and os.fstat(fd).st_size == len(chunks))
        return bytes(chunks)
    finally:
        os.close(fd)


def _new_at(directory: int, name: str, raw: bytes) -> None:
    fd = os.open(name, _WRITE, 0o600, dir_fd=directory)
    try:
        os.fchmod(fd, 0o600)
        view = memoryview(raw)
        while view:
            written = os.write(fd, view)
            _require(written > 0)
            view = view[written:]
        _flush(fd)
    finally:
        os.close(fd)
    os.fsync(directory)


class _CustodyStore:
    """An internal owned FD set. A file pointer never supplies a live handle."""

    def __init__(self, ledger: OwnershipLedger, *, create: bool, intent: RunIntent | None = None):
        self.parent_path = Path(ledger.root)
        self.root = self.parent_path / "commissioning-run"
        self.owner = ledger.owner
        self._pid, self._epoch = os.getpid(), _FORK_EPOCH
        self._closed = False
        self._fds: dict[str, int] = {}
        self._ids: dict[str, tuple[int, int]] = {}
        try:
            _require(type(ledger) is OwnershipLedger and valid_owner(self.owner))
            parent = self._remember("parent", _open_root(str(self.parent_path)), directory=True)
            entries = self._ledger_entries(parent)
            if create:
                _require(
                    type(intent) is RunIntent and all(row["state"] == "intended" for row in entries)
                )
                os.mkdir("commissioning-run", 0o700, dir_fd=parent)
                os.chmod("commissioning-run", 0o700, dir_fd=parent, follow_symlinks=False)
                os.fsync(parent)
            root = self._remember(
                "root", os.open("commissioning-run", _DIR, dir_fd=parent), directory=True
            )
            if create:
                os.mkdir("records", 0o700, dir_fd=root)
                os.chmod("records", 0o700, dir_fd=root, follow_symlinks=False)
                _new_at(root, "lock", b"")
            self._remember("records", os.open("records", _DIR, dir_fd=root), directory=True)
            lock = self._remember(
                "lock",
                os.open(
                    "lock", os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, dir_fd=root
                ),
            )
            _require(os.fstat(lock).st_size == 0)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if create:
                assert intent is not None
                self.put(intent)
                _new_at(root, "location", self._location().encode("ascii"))
                _new_at(root, "intent", intent.digest.encode("ascii"))
                _new_at(root, "head", _ZERO.encode("ascii"))
                os.fsync(root)
                os.fsync(parent)
            self.check()
        except BaseException:
            self.close()
            raise

    def _ledger_entries(self, parent: int) -> list[dict[str, object]]:
        value = json.loads(
            _read_at(parent, "ownership.json", 67108864),
            object_pairs_hook=_pairs,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
        _require(type(value) is dict and value.keys() == {"owner", "entries"})
        _require(value["owner"] == self.owner and type(value["entries"]) is list)
        try:
            return [_entry(row) for row in value["entries"]]
        except TypeError:
            raise RunCustodyRefused() from None

    def _remember(self, name: str, fd: int, *, directory: bool = False) -> int:
        self._fds[name] = fd
        self._ids[name] = _identity(_private(fd, directory=directory))
        return fd

    def same_process(self) -> bool:
        return self._pid == os.getpid() and self._epoch == _FORK_EPOCH

    def _location(self) -> str:
        parent, root = self._ids["parent"], self._ids["root"]
        records, lock = self._ids["records"], self._ids["lock"]
        raw = json.dumps(
            {
                "ledger_owner": self.owner,
                "parent_device": str(parent[0]),
                "parent_inode": str(parent[1]),
                "run_device": str(root[0]),
                "run_inode": str(root[1]),
                "root_path": str(self.root),
                "records_device": str(records[0]),
                "records_inode": str(records[1]),
                "lock_device": str(lock[0]),
                "lock_inode": str(lock[1]),
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        return hashlib.sha256(raw).hexdigest()

    def check(self) -> None:
        _require(not self._closed and self.same_process())
        current = _open_root(str(self.parent_path))
        try:
            _require(_identity(_private(current, directory=True)) == self._ids["parent"])
        finally:
            os.close(current)
        self._ledger_entries(self._fds["parent"])
        for name, fd in self._fds.items():
            _require(_identity(_private(fd, directory=name != "lock")) == self._ids[name])
        for parent, name, target in (
            ("parent", "commissioning-run", "root"),
            ("root", "records", "records"),
            ("root", "lock", "lock"),
        ):
            info = os.stat(name, dir_fd=self._fds[parent], follow_symlinks=False)
            _require(_identity(info) == self._ids[target])
        _require(self.pointer("location") == self._location())
        names = set(os.listdir(self._fds["root"]))
        _require(
            names
            in (
                {"lock", "records", "intent", "head", "location"},
                {"lock", "records", "intent", "head", "location", "plan"},
            )
        )

    def pointer(self, name: str) -> str:
        raw = _read_at(self._fds["root"], name, 64)
        _require(len(raw) == 64 and _HEX.fullmatch(raw.decode("ascii")) is not None)
        return raw.decode("ascii")

    def has_plan(self) -> bool:
        return "plan" in os.listdir(self._fds["root"])

    def record(self, digest: str) -> bytes:
        _require(type(digest) is str and _HEX.fullmatch(digest) is not None)
        raw = _read_at(self._fds["records"], digest)
        _require(hashlib.sha256(raw).hexdigest() == digest)
        return raw

    def record_names(self) -> set[str]:
        names = set(os.listdir(self._fds["records"]))
        _require(len(names) <= 4096 and all(_HEX.fullmatch(name) is not None for name in names))
        return names

    def put(self, record: RunIntent | RunPlan | RunEvent) -> None:
        _require(type(record) in {RunIntent, RunPlan, RunEvent})
        names = self.record_names()
        _require(record.digest not in names and len(names) < 4096)
        size = sum(len(self.record(name)) for name in names)
        _require(size + len(record.canonical_bytes) + 512 <= 67108864)
        _new_at(self._fds["records"], record.digest, record.canonical_bytes)

    def set_plan(self, plan: RunPlan) -> None:
        self.put(plan)
        _new_at(self._fds["root"], "plan", plan.digest.encode("ascii"))

    def append(self, event: RunEvent, expected_head: str) -> None:
        self.check()
        _require(self.pointer("head") == expected_head)
        self.put(event)
        temporary = ".head-" + secrets.token_hex(16)
        _new_at(self._fds["root"], temporary, event.digest.encode("ascii"))
        os.replace(temporary, "head", src_dir_fd=self._fds["root"], dst_dir_fd=self._fds["root"])
        os.fsync(self._fds["root"])

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for name, fd in reversed(list(self._fds.items())):
            try:
                if name not in self._ids or _identity(os.fstat(fd)) == self._ids[name]:
                    # Closing an inherited duplicate must never explicitly unlock
                    # the original process's shared flock open-file description.
                    os.close(fd)
            except OSError:
                pass


_STORAGE_ERRORS = (OSError, ValueError, RecursionError, RuntimeDirectoryInvalid)
