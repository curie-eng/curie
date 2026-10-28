"""Trusted-base checkpoints of the managed repository workspace (ADR 0251).

``POST /v1/checkpoint`` captures every observed change in the managed checkout
against a commit the worker names and trusts: tracked, staged and untracked
(not ignored) files, local commits after the base, deletions and executable
bits. The result is a ``git apply --binary`` patch plus a canonical JSON
manifest of the changed paths. Nothing is stored, published or fetched, and no
credential or authority rides the result.

Limits of the contract:

1. It detects observed changes; it is not an atomic snapshot. The inputs are
   fingerprinted before and after the tree is built, and any difference is a
   ``workspace_changed`` refusal, but a writer that changes a file and changes
   it back between the two passes is not seen.
2. Symlink and gitlink changes, special files, untracked nested repositories,
   unmerged entries and unsafe paths are refused, never represented.
3. The workspace and its ``.git/config`` are agent controlled. Git runs by argv
   with filters, external diff, textconv, fsmonitor and the untracked cache
   off, against a scratch index and object directory, so the real index,
   refs, HEAD and object store are only read. Files are read without following
   symlinks.
4. The whole capture shares one monotonic deadline of at most 60 seconds; a
   git child that outlives it is killed with its process group.
"""

from __future__ import annotations

import base64
import errno
import hashlib
import json
import math
import os
import re
import shutil
import signal
import stat
import subprocess
import tempfile
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import IO

from .subprocess_env import shell_and_hook_env
from .workspace_snapshot import MAX_PATCH_BYTES, WorkspaceSnapshotError, validate_patch

__all__ = [
    "MAX_BUDGET_SECONDS",
    "MAX_MANIFEST_BYTES",
    "MAX_PATCH_BYTES",
    "CheckpointRefusal",
    "CheckpointTimeout",
    "WorkspaceCheckpoint",
    "capture_workspace_checkpoint",
    "parse_checkpoint_request",
]

MAX_MANIFEST_BYTES = 1_048_576
MAX_BUDGET_SECONDS = 60
MANIFEST_VERSION = 1

_SHA_RE = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")
_OID_LENGTH = {"sha1": 40, "sha256": 64}
_REQUEST_KEYS = frozenset({"base_sha", "budget_seconds"})
_CHUNK = 1024 * 1024
_REGULAR_MODES = frozenset({"100644", "100755"})
_SYMLINK_MODE = "120000"
_GITLINK_MODE = "160000"
_SCRATCH_PREFIX = "curie-checkpoint-"
# Every git call: no workspace-configured program or hook runs and nothing
# optional is written back to the real repository. A split index would put its
# shared index in the real git directory even for the scratch index.
_GIT_SAFETY = (
    "--no-pager",
    "-c",
    "core.hooksPath=/dev/null",
    "-c",
    "core.fsmonitor=false",
    "-c",
    "core.untrackedCache=false",
    "-c",
    "core.splitIndex=false",
    "-c",
    "gc.auto=0",
)
_GIT_ENV = {
    "GIT_TERMINAL_PROMPT": "0",
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_OPTIONAL_LOCKS": "0",
    "GIT_NO_REPLACE_OBJECTS": "1",
    "GIT_NO_LAZY_FETCH": "1",
}


class CheckpointRefusal(Exception):
    """A checkpoint request the runner answers with a coded refusal."""

    def __init__(self, code: str, status: int, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.status = status
        self.message = message

    def body(self) -> dict[str, object]:
        return {"error": self.message, "code": self.code}


class CheckpointTimeout(CheckpointRefusal):
    """The capture deadline passed during ``stage`` (``git`` or ``hash``)."""

    def __init__(self, stage: str) -> None:
        super().__init__(
            "deadline_exceeded", 504, f"checkpoint budget exhausted during the {stage} stage"
        )
        self.stage = stage

    def body(self) -> dict[str, object]:
        return {**super().body(), "stage": self.stage}


def _unsafe(message: str) -> CheckpointRefusal:
    return CheckpointRefusal("unsafe_entry", 422, message)


def _unreadable(message: str) -> CheckpointRefusal:
    return CheckpointRefusal("unreadable", 422, message)


def _changed(message: str) -> CheckpointRefusal:
    return CheckpointRefusal("workspace_changed", 409, message)


@dataclass(frozen=True)
class WorkspaceCheckpoint:
    base_sha: str
    patch: bytes
    changed_paths: tuple[str, ...]
    manifest: bytes

    def to_json(self) -> dict[str, object]:
        """The JSON boundary form; raw bytes travel only as base64."""

        return {
            "base_sha": self.base_sha,
            "patch_base64": base64.b64encode(self.patch).decode("ascii"),
            "patch_size_bytes": len(self.patch),
            "patch_sha256": hashlib.sha256(self.patch).hexdigest(),
            "changed_paths": list(self.changed_paths),
            "manifest_base64": base64.b64encode(self.manifest).decode("ascii"),
            "manifest_size_bytes": len(self.manifest),
            "manifest_sha256": hashlib.sha256(self.manifest).hexdigest(),
        }


def parse_checkpoint_request(body: object) -> tuple[str, float]:
    """Validate a decoded request body into ``(base_sha, budget_seconds)``."""

    if not isinstance(body, dict) or set(body) != _REQUEST_KEYS:
        raise CheckpointRefusal(
            "invalid_request", 400, "body must be an object with base_sha and budget_seconds"
        )
    base_sha = body["base_sha"]
    if not isinstance(base_sha, str) or not _SHA_RE.fullmatch(base_sha):
        raise CheckpointRefusal(
            "invalid_request", 400, "base_sha must be a full lowercase hex object id"
        )
    budget = body["budget_seconds"]
    if (
        isinstance(budget, bool)
        or not isinstance(budget, int | float)
        or not math.isfinite(budget)
        or not 0 < budget <= MAX_BUDGET_SECONDS
    ):
        raise CheckpointRefusal(
            "invalid_request",
            400,
            f"budget_seconds must be a number in (0, {MAX_BUDGET_SECONDS}]",
        )
    return base_sha, float(budget)


class _Deadline:
    def __init__(self, budget_seconds: float) -> None:
        self._at = time.monotonic() + budget_seconds

    def remaining(self, stage: str) -> float:
        left = self._at - time.monotonic()
        if left <= 0:
            raise CheckpointTimeout(stage)
        return left


class _Git:
    """Git by argv in the workspace, bounded by the shared deadline."""

    def __init__(self, workspace: Path, deadline: _Deadline) -> None:
        self.workspace = workspace
        self.deadline = deadline
        inherited = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
        self._env = shell_and_hook_env(inherited, extra=_GIT_ENV)

    def run(
        self,
        *args: str,
        env: Mapping[str, str] | None = None,
        input: bytes | None = None,
        stdout: IO[bytes] | None = None,
        check: bool = True,
    ) -> tuple[int, bytes]:
        remaining = self.deadline.remaining("git")
        try:
            proc = subprocess.Popen(
                ["git", *_GIT_SAFETY, *args],
                cwd=self.workspace,
                stdin=subprocess.DEVNULL if input is None else subprocess.PIPE,
                stdout=subprocess.PIPE if stdout is None else stdout,
                stderr=subprocess.DEVNULL,
                env={**self._env, **(env or {})},
                start_new_session=True,
            )
        except OSError as exc:
            raise _unreadable("git is not available in the runner image") from exc
        try:
            out, _ = proc.communicate(input=input, timeout=remaining)
        except subprocess.TimeoutExpired:
            _kill_group(proc)
            raise CheckpointTimeout("git") from None
        except BaseException:
            _kill_group(proc)
            raise
        if check and proc.returncode != 0:
            raise _unreadable(f"git {args[0]} failed with exit status {proc.returncode}")
        return proc.returncode, out or b""


def _kill_group(proc: subprocess.Popen[bytes]) -> None:
    """SIGKILL the child's whole process group, then reap the child.

    The pipes are closed rather than drained: a descendant that left the group
    could hold them open, and reaping must not wait on it.
    """

    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    for stream in (proc.stdin, proc.stdout):
        if stream is not None:
            stream.close()
    proc.wait()


# A fingerprint record per path: ("absent",), ("directory",), ("gitlink", oid),
# ("symlink", oid, sha256) or ("file", mode, oid, sha256, size).
_Record = tuple[str | int, ...]


@dataclass(frozen=True)
class _Fingerprint:
    digest: str
    records: dict[str, _Record]
    index: dict[str, tuple[str, str]]


def _safe_path(raw: bytes) -> str:
    try:
        path = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise _unsafe("workspace contains a non-UTF-8 path") from None
    pure = PurePosixPath(path)
    if (
        not path
        or pure.is_absolute()
        or pure.as_posix() != path
        or "\n" in path
        or any(part in ("", ".", "..") or part.casefold() == ".git" for part in pure.parts)
    ):
        raise _unsafe("workspace contains an unsafe path")
    return path


def _git_hasher(object_format: str) -> hashlib._Hash:
    return hashlib.new(object_format, usedforsecurity=False)


def _open_parent(root_fd: int, parts: Iterable[str]) -> int | None:
    """Open the parent directory without following symlinks, or None if absent."""

    fd = os.dup(root_fd)
    for part in parts:
        try:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
        except OSError as exc:
            if exc.errno in (errno.ENOENT, errno.ENOTDIR, errno.ELOOP):
                return None
            raise
        finally:
            os.close(fd)
        fd = child
    return fd


def _hash_file(
    parent_fd: int, name: str, object_format: str, deadline: _Deadline
) -> tuple[str, str, str, int]:
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_NOCTTY, dir_fd=parent_fd)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise _changed("a workspace file changed type during the checkpoint")
        size = info.st_size
        content = hashlib.sha256()
        oid = _git_hasher(object_format)
        oid.update(b"blob %d\0" % size)
        read = 0
        while True:
            deadline.remaining("hash")
            chunk = os.read(fd, _CHUNK)
            if not chunk:
                break
            read += len(chunk)
            content.update(chunk)
            oid.update(chunk)
        if read != size:
            raise _changed("a workspace file changed size during the checkpoint")
        mode = "100755" if info.st_mode & stat.S_IXUSR else "100644"
        return mode, oid.hexdigest(), content.hexdigest(), size
    finally:
        os.close(fd)


def _record(
    root_fd: int,
    path: str,
    index_entry: tuple[str, str] | None,
    object_format: str,
    deadline: _Deadline,
) -> _Record:
    deadline.remaining("hash")
    parts = path.split("/")
    try:
        parent = _open_parent(root_fd, parts[:-1])
    except OSError:
        raise _unreadable("a workspace directory could not be read") from None
    if parent is None:
        return ("absent",)
    name = parts[-1]
    try:
        try:
            info = os.stat(name, dir_fd=parent, follow_symlinks=False)
        except FileNotFoundError:
            return ("absent",)
        if (
            index_entry is not None
            and index_entry[0] == _GITLINK_MODE
            and stat.S_ISDIR(info.st_mode)
        ):
            # A submodule directory is identified by its index entry, never
            # descended into; anything else at that path is its real type.
            return ("gitlink", index_entry[1])
        if stat.S_ISLNK(info.st_mode):
            target = os.readlink(os.fsencode(name), dir_fd=parent)
            oid = _git_hasher(object_format)
            oid.update(b"blob %d\0" % len(target) + target)
            return ("symlink", oid.hexdigest(), hashlib.sha256(target).hexdigest())
        if stat.S_ISDIR(info.st_mode):
            return ("directory",)
        if stat.S_ISREG(info.st_mode):
            mode, oid_hex, sha256, size = _hash_file(parent, name, object_format, deadline)
            return ("file", mode, oid_hex, sha256, size)
        raise _unsafe("workspace contains a special file")
    except OSError:
        raise _unreadable("a workspace file could not be read") from None
    finally:
        os.close(parent)


def _fingerprint(git: _Git, object_format: str) -> _Fingerprint:
    """Hash HEAD, the index listing and every candidate path's current content."""

    _, head = git.run("rev-parse", "--verify", "--quiet", "HEAD", check=False)
    _, staged = git.run("ls-files", "-s", "-z")
    index: dict[str, tuple[str, str]] = {}
    for item in staged.split(b"\0"):
        if not item:
            continue
        meta, _, raw_path = item.partition(b"\t")
        mode, oid, stage = meta.decode("ascii").split(" ")
        if stage != "0":
            raise _unsafe("workspace index has an unmerged entry")
        index[_safe_path(raw_path)] = (mode, oid)
    _, others = git.run("ls-files", "-z", "--others", "--exclude-standard")
    paths = set(index)
    for item in others.split(b"\0"):
        if not item:
            continue
        if item.endswith(b"/"):
            raise _unsafe("workspace contains an untracked nested repository")
        paths.add(_safe_path(item))

    try:
        root_fd = os.open(git.workspace, os.O_RDONLY | os.O_DIRECTORY)
    except OSError:
        raise _unreadable("managed workspace directory is missing") from None
    try:
        records = {
            path: _record(root_fd, path, index.get(path), object_format, git.deadline)
            for path in sorted(paths)
        }
    finally:
        os.close(root_fd)
    canonical = json.dumps(
        {
            "head": head.decode("ascii", errors="replace").strip(),
            "index": hashlib.sha256(staged).hexdigest(),
            "records": records,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return _Fingerprint(hashlib.sha256(canonical.encode()).hexdigest(), records, index)


def _desired_entries(records: Mapping[str, _Record]) -> dict[str, tuple[str, str]]:
    """The tree the observed workspace describes: path to (mode, oid)."""

    desired: dict[str, tuple[str, str]] = {}
    for path, record in records.items():
        kind = record[0]
        if kind == "file":
            desired[path] = (str(record[1]), str(record[2]))
        elif kind == "symlink":
            desired[path] = (_SYMLINK_MODE, str(record[1]))
        elif kind == "gitlink":
            desired[path] = (_GITLINK_MODE, str(record[1]))
    return desired


def _base_entries(git: _Git, base_sha: str) -> dict[str, tuple[str, str]]:
    _, listing = git.run("ls-tree", "-r", "-z", "--full-tree", base_sha)
    entries: dict[str, tuple[str, str]] = {}
    for item in listing.split(b"\0"):
        if not item:
            continue
        meta, _, raw_path = item.partition(b"\t")
        mode, _, oid = meta.decode("ascii").split(" ")
        try:
            path = raw_path.decode("utf-8")
        except UnicodeDecodeError:
            raise _unsafe("base commit contains a non-UTF-8 path") from None
        entries[path] = (mode, oid)
    return entries


def _validate_repository(git: _Git, base_sha: str) -> tuple[str, Path]:
    """Check the checkout and the base; return the object format and objects dir."""

    if not git.workspace.is_dir():
        raise _unreadable("managed workspace directory is missing")
    code, toplevel = git.run("rev-parse", "--show-toplevel", check=False)
    if code != 0 or os.path.realpath(toplevel.decode("utf-8", "replace").strip()) != (
        os.path.realpath(git.workspace)
    ):
        raise _unreadable("managed workspace is not the top of a git work tree")
    _, raw_format = git.run("rev-parse", "--show-object-format")
    object_format = raw_format.decode("ascii", "replace").strip()
    if object_format not in _OID_LENGTH:
        raise _unreadable("repository object format is not supported")
    if len(base_sha) != _OID_LENGTH[object_format]:
        raise CheckpointRefusal(
            "invalid_request", 400, f"base_sha is not a {object_format} object id"
        )
    code, resolved = git.run(
        "rev-parse",
        "--verify",
        "--quiet",
        "--end-of-options",
        f"{base_sha}^{{commit}}",
        check=False,
    )
    if code != 0 or resolved.strip().decode("ascii", "replace") != base_sha:
        raise CheckpointRefusal(
            "base_unavailable", 422, "base_sha is not a commit available in the workspace"
        )
    _, objects = git.run("rev-parse", "--git-path", "objects")
    return object_format, git.workspace / objects.decode("utf-8", "replace").strip()


def _manifest(base_sha: str, changed: Iterable[str], records: Mapping[str, _Record]) -> bytes:
    entries: list[dict[str, object]] = []
    for path in changed:
        record = records.get(path, ("absent",))
        if record[0] == "file":
            entries.append(
                {"mode": record[1], "path": path, "sha256": record[3], "size": record[4]}
            )
        else:
            entries.append({"deleted": True, "path": path})
    document = {"base_sha": base_sha, "entries": entries, "version": MANIFEST_VERSION}
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )


def capture_workspace_checkpoint(
    workspace: Path, *, base_sha: str, budget_seconds: float
) -> WorkspaceCheckpoint:
    """Capture every observed change in ``workspace`` against ``base_sha``.

    Raises ``CheckpointRefusal`` (``CheckpointTimeout`` when the budget runs
    out); the message never carries workspace content.
    """

    git = _Git(workspace, _Deadline(budget_seconds))
    object_format, objects_dir = _validate_repository(git, base_sha)
    base = _base_entries(git, base_sha)
    first = _fingerprint(git, object_format)
    desired = _desired_entries(first.records)

    changed = sorted(
        path for path in set(base) | set(desired) if base.get(path) != desired.get(path)
    )
    for path in changed:
        modes = {entry[0] for entry in (base.get(path), desired.get(path)) if entry}
        if modes & {_SYMLINK_MODE, _GITLINK_MODE}:
            raise _unsafe("symlink and gitlink changes cannot be checkpointed")

    scratch = Path(tempfile.mkdtemp(prefix=_SCRATCH_PREFIX, dir="/tmp"))
    try:
        (scratch / "objects").mkdir()
        # Building the tree never consults the real store, so even an existing
        # object is not freshened there; only the diff reads it as an alternate.
        isolated = {
            "GIT_INDEX_FILE": str(scratch / "index"),
            "GIT_OBJECT_DIRECTORY": str(scratch / "objects"),
        }
        with_base = {**isolated, "GIT_ALTERNATE_OBJECT_DIRECTORIES": str(objects_dir)}

        blobs = [
            path
            for path in changed
            if path in desired
            and first.records[path][0] == "file"
            and (path not in base or base[path][1] != desired[path][1])
        ]
        if blobs:
            _, printed = git.run(
                "hash-object",
                "-w",
                "--no-filters",
                "--stdin-paths",
                env=isolated,
                input="".join(f"{path}\n" for path in blobs).encode("utf-8"),
            )
            if printed.decode("ascii", "replace").split() != [desired[p][1] for p in blobs]:
                raise _changed("a workspace file changed during the checkpoint")

        index_info = "".join(
            f"{mode} {oid}\t{path}\0" for path, (mode, oid) in sorted(desired.items())
        )
        git.run("update-index", "-z", "--index-info", env=isolated, input=index_info.encode())
        _, tree = git.run("write-tree", "--missing-ok", env=isolated)
        tree_sha = tree.decode("ascii", "replace").strip()

        _, names = git.run(
            "diff",
            "--no-renames",
            "--no-ext-diff",
            "--no-textconv",
            "--name-status",
            "-z",
            base_sha,
            tree_sha,
            "--",
            env=with_base,
        )
        patch_file = scratch / "patch"
        with patch_file.open("wb") as handle:
            git.run(
                "diff",
                "--binary",
                "--full-index",
                "--no-renames",
                "--no-ext-diff",
                "--no-textconv",
                "--no-color",
                "--src-prefix=a/",
                "--dst-prefix=b/",
                base_sha,
                tree_sha,
                "--",
                env=with_base,
                stdout=handle,
            )
        second = _fingerprint(git, object_format)
        if second.digest != first.digest:
            raise _changed("the workspace changed during the checkpoint")

        listed = names.split(b"\0")
        diff_paths = sorted(raw.decode("utf-8", "replace") for raw in listed[1::2] if raw)
        if diff_paths != changed:
            raise _changed("the captured tree disagrees with the observed workspace")
        if patch_file.stat().st_size > MAX_PATCH_BYTES:
            raise CheckpointRefusal(
                "patch_too_large", 422, f"checkpoint patch exceeds {MAX_PATCH_BYTES} bytes"
            )
        patch = patch_file.read_bytes()
    finally:
        shutil.rmtree(scratch, ignore_errors=True)

    if len(patch) > MAX_PATCH_BYTES:
        raise CheckpointRefusal(
            "patch_too_large", 422, f"checkpoint patch exceeds {MAX_PATCH_BYTES} bytes"
        )
    try:
        validate_patch(patch)
    except WorkspaceSnapshotError:
        raise _unsafe("checkpoint patch failed path and mode validation") from None
    headers = sum(1 for line in patch.split(b"\n") if line.startswith(b"diff --git "))
    if headers != len(changed):
        raise _changed("checkpoint patch headers disagree with the changed paths")
    manifest = _manifest(base_sha, changed, first.records)
    if len(manifest) > MAX_MANIFEST_BYTES:
        raise CheckpointRefusal(
            "manifest_too_large", 422, f"checkpoint manifest exceeds {MAX_MANIFEST_BYTES} bytes"
        )
    # A result finished after the budget is discarded, not returned late.
    git.deadline.remaining("hash")
    return WorkspaceCheckpoint(
        base_sha=base_sha, patch=patch, changed_paths=tuple(changed), manifest=manifest
    )
