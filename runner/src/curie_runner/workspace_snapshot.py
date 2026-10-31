"""Bounded, credential-free snapshots of the managed repository workspace.

The worker names the prepared commit P it retained for the thread. The runner
proves the checkout's HEAD is P or a bounded local descendant of P, then
captures the net change from P to the final working tree files: committed,
staged, unstaged and untracked (not ignored) changes together. Local commits
never move the base.

Files are read without following symlinks and hashed without git filters into
a scratch index and object directory, so the real index, refs, HEAD and object
store are only read. The workspace is observed before and after the patch is
built, and a difference refuses the capture.

The runner exposes the result only to its per-sandbox bearer-token holder. It
does not publish anything: the worker validates the patch against its own copy
of P and durably stores it before the sandbox is suspended, and a separate
platform Job applies it after approval.
"""

from __future__ import annotations

import base64
import errno
import hashlib
import os
import re
import shlex
import shutil
import stat
import subprocess
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import IO
from urllib.parse import urlsplit, urlunsplit

from .subprocess_env import shell_and_hook_env

MAX_PATCH_BYTES = 900_000
# Bounds on the local history between the prepared base and HEAD.
MAX_ANCESTRY_COMMITS = 256
MAX_ANCESTRY_BYTES = 1_048_576
_GIT_TIMEOUT_SECONDS = 30
# A full lowercase sha1 or sha256 object id.
_OID_RE = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?")
_OID_LENGTH = {"sha1": 40, "sha256": 64}
_SYMLINK_MODE = "120000"
_GITLINK_MODE = "160000"
_READ_CHUNK = 1024 * 1024
_CHANGED = "the workspace changed during snapshot capture; retry once writes stop"
_BASE_UNAVAILABLE = "the prepared base commit is not available in the workspace"
_NOT_DESCENDANT = (
    "the workspace HEAD is not the prepared base commit or a local commit on top of it"
)
_UNREADABLE_HISTORY = "a local commit on top of the prepared base is missing or unreadable"
# Every git call: no workspace-configured hook or monitor runs and nothing
# optional is written back to the real repository. A split index would put its
# shared index in the real git directory even for the scratch index.
_GIT_CONFIG = (
    "-c",
    "core.hooksPath=/dev/null",
    "-c",
    "core.fsmonitor=false",
    "-c",
    "core.untrackedCache=false",
    "-c",
    "core.splitIndex=false",
    # Unescaped UTF-8 paths in patch headers.
    "-c",
    "core.quotePath=false",
    "-c",
    "gc.auto=0",
)
_GIT_ENV = {
    "GIT_TERMINAL_PROMPT": "0",
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_NO_REPLACE_OBJECTS": "1",
    "GIT_NO_LAZY_FETCH": "1",
    "GIT_OPTIONAL_LOCKS": "0",
}
# One segment of a declared repository path (ADR 0197): GitLab nests groups, so
# a code host other than GitHub has any number of these. Dot segments are
# refused separately, since a URL would resolve them away.
_PATH_SEGMENT = re.compile(r"[A-Za-z0-9_][A-Za-z0-9._~-]*")


class WorkspaceSnapshotError(RuntimeError):
    """The workspace cannot be represented as a safe publication patch."""


class WorkspaceHistoryError(WorkspaceSnapshotError):
    """HEAD is not P or a verified local descendant of P, or P is unavailable."""


@dataclass(frozen=True)
class WorkspaceSnapshot:
    repo_full_name: str
    base_sha: str
    patch: bytes
    changed_paths: tuple[str, ...]
    contains_workflow_files: bool
    publication_title: str | None = None
    publication_body: str | None = None

    def to_json(self) -> dict[str, object]:
        """Return the JSON boundary form; raw binary is never interpolated."""

        return {
            "repo_full_name": self.repo_full_name,
            "base_sha": self.base_sha,
            "patch_base64": base64.b64encode(self.patch).decode("ascii"),
            "changed_paths": list(self.changed_paths),
            "contains_workflow_files": self.contains_workflow_files,
            "patch_size_bytes": len(self.patch),
            "publication_title": self.publication_title,
            "publication_body": self.publication_body,
        }


def enforce_patch_cap(patch: bytes) -> bytes:
    if len(patch) > MAX_PATCH_BYTES:
        raise WorkspaceSnapshotError(
            f"publication patch exceeds the {MAX_PATCH_BYTES} raw-byte limit"
        )
    return patch


def _safe_repository_path(raw: str) -> str:
    """Validate a repository-relative path without rewriting its first component."""

    pure = PurePosixPath(raw)
    if (
        not raw
        or raw.startswith("/")
        or "\\" in raw
        or pure.is_absolute()
        or any(part in ("", ".", "..") for part in pure.parts)
        or pure.parts[0] == ".git"
    ):
        raise WorkspaceSnapshotError(f"unsafe patch path {raw!r}")
    return pure.as_posix()


def _safe_diff_path(raw: str, *, prefix: str) -> str:
    expected = f"{prefix}/"
    if not raw.startswith(expected):
        raise WorkspaceSnapshotError(f"patch header path lacks {expected!r} prefix")
    return _safe_repository_path(raw[len(expected) :])


def _diff_header_paths(line: str) -> tuple[str, str]:
    payload = line.removeprefix("diff --git ")
    if payload.startswith(('"', "'")):
        try:
            parts = shlex.split(payload)
        except ValueError as exc:
            raise WorkspaceSnapshotError("malformed git patch header") from exc
        if len(parts) != 2:
            raise WorkspaceSnapshotError("malformed git patch header")
        return parts[0], parts[1]

    # Git does not quote ordinary spaces in diff --git headers. Enumerate the
    # known ` b/` boundary and accept it only when it gives one unambiguous pair
    # of safe a/ and b/ paths. This preserves names such as `a/read me.md`
    # without weakening traversal or metadata checks.
    candidates: list[tuple[str, str]] = []
    offset = 0
    while True:
        boundary = payload.find(" b/", offset)
        if boundary < 0:
            break
        left = payload[:boundary]
        right = payload[boundary + 1 :]
        try:
            _safe_diff_path(left, prefix="a")
            _safe_diff_path(right, prefix="b")
        except WorkspaceSnapshotError:
            pass
        else:
            candidates.append((left, right))
        offset = boundary + 1
    if len(candidates) != 1:
        raise WorkspaceSnapshotError("malformed or ambiguous git patch header")
    return candidates[0]


def validate_patch(patch: bytes) -> bytes:
    """Reject paths and file modes that can escape or alter git metadata."""

    enforce_patch_cap(patch)
    text = patch.decode("utf-8", errors="surrogateescape")
    saw_header = False
    for line in text.splitlines():
        if line.startswith("diff --git "):
            saw_header = True
            left, right = _diff_header_paths(line)
            _safe_diff_path(left, prefix="a")
            _safe_diff_path(right, prefix="b")
        if line.startswith(("new file mode ", "old mode ", "new mode ")):
            mode = line.rsplit(" ", 1)[-1]
            if mode not in {"100644", "100755"}:
                raise WorkspaceSnapshotError(
                    f"publication patch contains unsupported file mode {mode}"
                )
    if patch and not saw_header:
        raise WorkspaceSnapshotError("publication patch contains no git diff header")
    return patch


def _git(
    repo: Path,
    *args: str,
    check: bool = True,
    stdin: bytes | None = None,
    env: Mapping[str, str] | None = None,
    stdout: IO[bytes] | None = None,
) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            ["git", *_GIT_CONFIG, *args],
            cwd=repo,
            input=b"" if stdin is None else stdin,
            stdout=subprocess.PIPE if stdout is None else stdout,
            stderr=subprocess.PIPE,
            timeout=_GIT_TIMEOUT_SECONDS,
            check=check,
            env=shell_and_hook_env(os.environ, extra={**_GIT_ENV, **(env or {})}),
        )
    except FileNotFoundError as exc:
        raise WorkspaceSnapshotError(
            "git is not installed in the runner image; repository publication is unavailable"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise WorkspaceSnapshotError("git snapshot stage exceeded 30 seconds") from exc
    except subprocess.CalledProcessError as exc:
        diagnostic = exc.stderr.decode("utf-8", errors="replace").strip()
        raise WorkspaceSnapshotError(f"git snapshot failed: {diagnostic}") from exc


def _trusted_base(trusted_origin: str) -> str:
    """The boot env's code host origin (ADR 0197) as a URL base, or refuse it.

    The same credential-free HTTPS rule the workspace origin meets: a trusted
    origin carrying userinfo, a query or a fragment is a broken boot, so the
    snapshot fails closed rather than trusting any part of it.
    """

    trusted = urlsplit(trusted_origin)
    _ = trusted.port
    if (
        trusted.scheme != "https"
        or not trusted.hostname
        or trusted.username is not None
        or trusted.password is not None
        or trusted.query
        or trusted.fragment
    ):
        raise ValueError("trusted repository origin is not credential-free HTTPS")
    return urlunsplit((trusted.scheme, trusted.netloc, trusted.path.rstrip("/"), "", ""))


def _valid_repository_path(path: str) -> bool:
    """A repository path of two or more segments, as the boot env declared it."""

    segments = path.split("/")
    return len(segments) >= 2 and all(
        _PATH_SEGMENT.fullmatch(segment) and segment not in {".", ".."} for segment in segments
    )


def _canonical_repo(
    origin: str, *, trusted_origin: str | None, repository_path: str | None
) -> str:
    if trusted_origin is None or repository_path is None:
        # The worker sets both on every workspace claim (ADR 0197); without
        # them the runner has no trusted origin to check against.
        raise WorkspaceSnapshotError(
            "the boot env names no trusted repository origin and path for the workspace"
        )
    try:
        html_base = _trusted_base(trusted_origin)
        parsed = urlsplit(origin)
        _ = parsed.port
    except ValueError as exc:
        raise WorkspaceSnapshotError("workspace repository origin is invalid") from exc
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or not origin.startswith(f"{html_base}/")
    ):
        raise WorkspaceSnapshotError(
            "workspace repository origin is not credential-free trusted code host HTTPS"
        )
    repo = origin[len(html_base) + 1 :].removesuffix(".git")
    if (
        not _valid_repository_path(repository_path)
        or repo.casefold() != repository_path.casefold()
        or origin != f"{html_base}/{repo}.git"
    ):
        # A declared path of any depth is accepted, but only that one.
        raise WorkspaceSnapshotError(
            "workspace repository origin is not the declared repository path"
        )
    return repo


def _display(raw: bytes) -> str:
    return raw.decode("utf-8", errors="backslashreplace")


def _object_id(object_format: str, kind: bytes, data: bytes) -> str:
    header = b"%s %d\0" % (kind, len(data))
    return hashlib.new(object_format, header + data, usedforsecurity=False).hexdigest()


def _verify_base(repo: Path, base_sha: str) -> str:
    """Return the object format once ``base_sha`` names a commit in the workspace."""

    if not isinstance(base_sha, str) or not _OID_RE.fullmatch(base_sha):
        raise WorkspaceSnapshotError("the prepared base commit is not a full lowercase object id")
    object_format = _git(repo, "rev-parse", "--show-object-format").stdout.decode(
        "ascii", errors="replace"
    ).strip()
    if object_format not in _OID_LENGTH:
        raise WorkspaceSnapshotError("the workspace object format is not supported")
    if len(base_sha) != _OID_LENGTH[object_format]:
        raise WorkspaceHistoryError(_BASE_UNAVAILABLE)
    resolved = _git(
        repo,
        "rev-parse",
        "--verify",
        "--quiet",
        "--end-of-options",
        f"{base_sha}^{{commit}}",
        check=False,
    )
    if resolved.returncode != 0 or resolved.stdout.decode("ascii", errors="replace").strip() != (
        base_sha
    ):
        raise WorkspaceHistoryError(_BASE_UNAVAILABLE)
    return object_format


def _head(repo: Path, length: int) -> str | None:
    result = _git(repo, "rev-parse", "--verify", "--quiet", "HEAD", check=False)
    head = result.stdout.decode("ascii", errors="replace").strip()
    if result.returncode != 0 or len(head) != length or not _OID_RE.fullmatch(head):
        return None
    return head


def _commit_parents(body: bytes, length: int) -> list[str]:
    """Parse parents as git does: the lines directly after the tree line."""

    lines = body.split(b"\n")
    if not lines[0].startswith(b"tree "):
        raise WorkspaceHistoryError(_UNREADABLE_HISTORY)
    parents: list[str] = []
    for line in lines[1:]:
        if not line.startswith(b"parent "):
            break
        parent = line.removeprefix(b"parent ").decode("ascii", errors="replace")
        if len(parent) != length or not _OID_RE.fullmatch(parent):
            raise WorkspaceHistoryError(_UNREADABLE_HISTORY)
        parents.append(parent)
    return parents


def _prove_descendant(repo: Path, head: str, base_sha: str, object_format: str) -> None:
    """Refuse unless ``head`` is ``base_sha`` or a bounded local descendant of it.

    rev-list only proposes the commits. Their sizes, bodies and object ids are
    checked here and their parents parsed here, so a graft, shallow file or
    commit-graph cannot decide the answer. Nothing is fetched.
    """

    if head == base_sha:
        return
    listed = _git(
        repo,
        "rev-list",
        f"--max-count={MAX_ANCESTRY_COMMITS + 1}",
        head,
        f"^{base_sha}",
        "--",
        check=False,
    )
    if listed.returncode != 0:
        raise WorkspaceHistoryError(_NOT_DESCENDANT)
    commits = listed.stdout.decode("ascii", errors="replace").split()
    if len(commits) > MAX_ANCESTRY_COMMITS:
        raise WorkspaceHistoryError(
            "the local history on top of the prepared base exceeds "
            f"{MAX_ANCESTRY_COMMITS} commits"
        )
    if (
        head not in commits
        or len(set(commits)) != len(commits)
        or any(len(commit) != len(base_sha) or not _OID_RE.fullmatch(commit) for commit in commits)
    ):
        raise WorkspaceHistoryError(_NOT_DESCENDANT)

    request = "".join(f"{commit}\n" for commit in commits).encode("ascii")
    checked = _git(repo, "cat-file", "--batch-check", stdin=request).stdout.splitlines()
    if len(checked) != len(commits):
        raise WorkspaceHistoryError(_UNREADABLE_HISTORY)
    sizes: list[int] = []
    for commit, line in zip(commits, checked, strict=True):
        fields = line.decode("ascii", errors="replace").split(" ")
        if (
            len(fields) != 3
            or fields[0] != commit
            or fields[1] != "commit"
            or not fields[2].isdigit()
        ):
            raise WorkspaceHistoryError(_UNREADABLE_HISTORY)
        sizes.append(int(fields[2]))
        if sum(sizes) > MAX_ANCESTRY_BYTES:
            raise WorkspaceHistoryError(
                "the local history on top of the prepared base exceeds 1 MiB of commit objects"
            )

    bodies = _git(repo, "cat-file", "--batch", stdin=request).stdout
    known = set(commits)
    offset = 0
    for commit, size in zip(commits, sizes, strict=True):
        header = f"{commit} commit {size}\n".encode("ascii")
        start = offset + len(header)
        if bodies[offset:start] != header or bodies[start + size : start + size + 1] != b"\n":
            raise WorkspaceHistoryError(_UNREADABLE_HISTORY)
        body = bodies[start : start + size]
        offset = start + size + 1
        if _object_id(object_format, b"commit", body) != commit:
            raise WorkspaceHistoryError(
                "a local commit on top of the prepared base does not match its object id"
            )
        parents = _commit_parents(body, len(base_sha))
        # A root commit or a parent outside the listed set is history that
        # does not end at the prepared base.
        if not parents or any(parent != base_sha and parent not in known for parent in parents):
            raise WorkspaceHistoryError(_NOT_DESCENDANT)


def _tree_entries(repo: Path, base_sha: str) -> dict[bytes, tuple[str, str]]:
    """Every path of the prepared base's tree, as (mode, object id)."""

    entries: dict[bytes, tuple[str, str]] = {}
    for item in _git(repo, "ls-tree", "-r", "-z", "--full-tree", base_sha).stdout.split(b"\0"):
        if not item:
            continue
        meta, separator, path = item.partition(b"\t")
        fields = meta.decode("ascii", errors="replace").split(" ")
        if not separator or len(fields) != 3:
            raise WorkspaceSnapshotError("git listed a malformed tree entry")
        entries[path] = (fields[0], fields[2])
    return entries


# One path as observed: ("absent",), ("directory",), ("gitlink", oid),
# ("symlink", oid) or ("file", mode, oid) from the filesystem, or ("kept", mode,
# oid) for P's own entry left in place (see ``_observe``).
_Record = tuple[str, ...]
_ABSENT: _Record = ("absent",)
# Index flag tags (``git ls-files -v``) of a skip-worktree entry; the lowercase
# form also has assume-unchanged set.
_SKIP_WORKTREE_TAGS = frozenset({"S", "s"})


@dataclass(frozen=True)
class _Observation:
    """One read of the index listing, the untracked listing and every candidate path."""

    index: Mapping[bytes, tuple[str, str]]
    untracked: tuple[bytes, ...]
    records: Mapping[bytes, _Record]


def _require_readable(path: bytes) -> None:
    # Checked before any read, so a crafted index entry cannot reach outside
    # the workspace or into git metadata.
    if any(part in (b"", b".", b"..") or part.lower() == b".git" for part in path.split(b"/")):
        raise WorkspaceSnapshotError(f"unsafe patch path {_display(path)!r}")


def _open_parent(root_fd: int, parts: list[bytes]) -> int | None:
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


def _hash_file(parent_fd: int, name: bytes, object_format: str) -> _Record:
    try:
        fd = os.open(
            name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_NOCTTY, dir_fd=parent_fd
        )
    except OSError as exc:
        if exc.errno in (errno.ENOENT, errno.ELOOP):
            raise WorkspaceSnapshotError(_CHANGED) from exc
        raise
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise WorkspaceSnapshotError(_CHANGED)
        digest = hashlib.new(object_format, usedforsecurity=False)
        digest.update(b"blob %d\0" % info.st_size)
        read = 0
        while chunk := os.read(fd, _READ_CHUNK):
            read += len(chunk)
            digest.update(chunk)
        if read != info.st_size:
            raise WorkspaceSnapshotError(_CHANGED)
        mode = "100755" if info.st_mode & stat.S_IXUSR else "100644"
        return ("file", mode, digest.hexdigest())
    finally:
        os.close(fd)


def _record(root_fd: int, path: bytes, gitlink: str | None, object_format: str) -> _Record:
    parts = path.split(b"/")
    try:
        parent = _open_parent(root_fd, parts[:-1])
    except OSError as exc:
        raise WorkspaceSnapshotError(
            f"workspace path {_display(path)!r} could not be read"
        ) from exc
    if parent is None:
        return ("absent",)
    name = parts[-1]
    try:
        info = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if gitlink is not None and stat.S_ISDIR(info.st_mode):
            # A submodule directory is identified by its recorded commit and
            # never descended into.
            return ("gitlink", gitlink)
        if stat.S_ISLNK(info.st_mode):
            target = os.readlink(name, dir_fd=parent)
            return ("symlink", _object_id(object_format, b"blob", target))
        if stat.S_ISDIR(info.st_mode):
            # Its untracked contents are listed as paths of their own.
            return ("directory",)
        if stat.S_ISREG(info.st_mode):
            return _hash_file(parent, name, object_format)
        raise WorkspaceSnapshotError(f"workspace path {_display(path)!r} is not a regular file")
    except FileNotFoundError:
        return ("absent",)
    except OSError as exc:
        raise WorkspaceSnapshotError(
            f"workspace path {_display(path)!r} could not be read"
        ) from exc
    finally:
        os.close(parent)


def _gitlink(
    path: bytes,
    index: Mapping[bytes, tuple[str, str]],
    base: Mapping[bytes, tuple[str, str]],
) -> str | None:
    for entry in (index.get(path), base.get(path)):
        if entry is not None and entry[0] == _GITLINK_MODE:
            return entry[1]
    return None


def _observe(
    repo: Path, base: Mapping[bytes, tuple[str, str]], object_format: str
) -> _Observation:
    """Read every path of P's tree, the index and the untracked listing.

    The filesystem decides every path, with one exception: a skip-worktree index
    entry equal to P's entry whose path is absent keeps P's entry. That is the
    state worker preparation leaves when it unlinks a committed symlink that
    escapes the checkout, so it is not a deletion the agent made. Because the
    kept entry is part of the records, the second observation compares it too.
    """

    index: dict[bytes, tuple[str, str]] = {}
    skip_worktree: set[bytes] = set()
    for item in _git(repo, "ls-files", "-s", "-v", "-z").stdout.split(b"\0"):
        if not item:
            continue
        meta, separator, path = item.partition(b"\t")
        fields = meta.decode("ascii", errors="replace").split(" ")
        if not separator or len(fields) != 4 or len(fields[0]) != 1:
            raise WorkspaceSnapshotError("git listed a malformed index entry")
        tag, mode, oid, stage = fields
        if stage != "0":
            raise WorkspaceSnapshotError(
                f"workspace path {_display(path)!r} has an unmerged index entry"
            )
        index[path] = (mode, oid)
        if tag in _SKIP_WORKTREE_TAGS:
            skip_worktree.add(path)
    untracked: list[bytes] = []
    for item in _git(repo, "ls-files", "-z", "--others", "--exclude-standard").stdout.split(
        b"\0"
    ):
        if not item:
            continue
        if item.endswith(b"/"):
            raise WorkspaceSnapshotError(
                f"workspace path {_display(item.rstrip(b'/'))!r} is an untracked nested "
                "repository"
            )
        untracked.append(item)

    candidates = sorted({*base, *index, *untracked})
    for path in candidates:
        _require_readable(path)
    try:
        root_fd = os.open(repo, os.O_RDONLY | os.O_DIRECTORY)
    except OSError as exc:
        raise WorkspaceSnapshotError("managed workspace directory is missing") from exc
    try:
        records: dict[bytes, _Record] = {}
        for path in candidates:
            record = _record(root_fd, path, _gitlink(path, index, base), object_format)
            kept = base.get(path)
            if (
                record == _ABSENT
                and path in skip_worktree
                and kept is not None
                and index.get(path) == kept
            ):
                record = ("kept", *kept)
            records[path] = record
    finally:
        os.close(root_fd)
    return _Observation(index=index, untracked=tuple(untracked), records=records)


def _desired_entries(records: Mapping[bytes, _Record]) -> dict[bytes, tuple[str, str]]:
    """The tree the observed files describe: path to (mode, object id)."""

    desired: dict[bytes, tuple[str, str]] = {}
    for path, record in records.items():
        if record[0] in ("file", "kept"):
            desired[path] = (record[1], record[2])
        elif record[0] == "symlink":
            desired[path] = (_SYMLINK_MODE, record[1])
        elif record[0] == "gitlink":
            desired[path] = (_GITLINK_MODE, record[1])
    return desired


def _publishable_path(
    raw: bytes, entries: tuple[tuple[str, str] | None, tuple[str, str] | None]
) -> str:
    try:
        path = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise WorkspaceSnapshotError("workspace contains a non-UTF-8 path") from exc
    if _safe_repository_path(path) != path or "\n" in path:
        raise WorkspaceSnapshotError(f"unsafe patch path {path!r}")
    if any(entry is not None and entry[0] in (_SYMLINK_MODE, _GITLINK_MODE) for entry in entries):
        raise WorkspaceSnapshotError(f"workspace path {path!r} is not a regular file")
    return path


def _stdin_path(path: bytes) -> bytes:
    """A path as ``hash-object --stdin-paths`` reads it: a leading quote means C quoting."""

    if not path.startswith(b'"'):
        return path
    return b'"' + path.replace(b"\\", b"\\\\").replace(b'"', b'\\"') + b'"'


def _build_patch(
    repo: Path,
    base_sha: str,
    desired: Mapping[bytes, tuple[str, str]],
    blobs: list[bytes],
    changed: list[bytes],
) -> bytes:
    """Build tree C in a scratch index and object directory and diff it against P."""

    objects_path = _git(repo, "rev-parse", "--git-path", "objects").stdout.decode(
        "utf-8", errors="surrogateescape"
    ).strip()
    real_objects = (repo / objects_path).absolute()
    scratch = Path(tempfile.mkdtemp(prefix="curie-snapshot-"))
    try:
        (scratch / "objects").mkdir()
        # Building C never consults the real store; only the diff reads it.
        isolated = {
            "GIT_INDEX_FILE": str(scratch / "index"),
            "GIT_OBJECT_DIRECTORY": str(scratch / "objects"),
        }
        if blobs:
            printed = _git(
                repo,
                "hash-object",
                "-w",
                "--no-filters",
                "--stdin-paths",
                stdin=b"".join(_stdin_path(path) + b"\n" for path in blobs),
                env=isolated,
            ).stdout
            if printed.split() != [desired[path][1].encode("ascii") for path in blobs]:
                raise WorkspaceSnapshotError(_CHANGED)
        _git(
            repo,
            "update-index",
            "-z",
            "--index-info",
            stdin=b"".join(
                b"%s %s\t%s\0" % (mode.encode("ascii"), oid.encode("ascii"), path)
                for path, (mode, oid) in sorted(desired.items())
            ),
            env=isolated,
        )
        tree = _git(repo, "write-tree", "--missing-ok", env=isolated).stdout.decode(
            "ascii", errors="replace"
        ).strip()
        if len(tree) != len(base_sha) or not _OID_RE.fullmatch(tree):
            raise WorkspaceSnapshotError("git wrote an invalid snapshot tree id")
        with_base = {**isolated, "GIT_ALTERNATE_OBJECT_DIRECTORIES": str(real_objects)}
        names = _git(
            repo, "diff", "--no-renames", "--name-only", "-z", base_sha, tree, "--", env=with_base
        ).stdout
        if sorted(name for name in names.split(b"\0") if name) != changed:
            raise WorkspaceSnapshotError("the snapshot tree does not match the observed workspace")
        patch_file = scratch / "patch"
        with patch_file.open("wb") as handle:
            # Explicitly disabling rename detection represents a rename as one
            # deletion plus one addition, so both paths are disclosed.
            _git(
                repo,
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
                tree,
                "--",
                env=with_base,
                stdout=handle,
            )
        with patch_file.open("rb") as handle:
            return enforce_patch_cap(handle.read(MAX_PATCH_BYTES + 1))
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


def capture_workspace_snapshot(
    workspace: str | Path = "/workspace",
    *,
    base_sha: str,
    expected_repo: str | None = None,
    trusted_origin: str | None = None,
    repository_path: str | None = None,
    publication_title: str | None = None,
    publication_body: str | None = None,
) -> WorkspaceSnapshot:
    """Capture the net change from ``base_sha`` to the final working tree files.

    ``base_sha`` is the prepared commit P the worker retained for the thread;
    the result is always anchored there. HEAD must be P or a local descendant
    proven within ``MAX_ANCESTRY_COMMITS`` and ``MAX_ANCESTRY_BYTES``, else
    ``WorkspaceHistoryError``.

    ``trusted_origin`` and ``repository_path`` are the boot env's
    ``CURIE_REPO_ORIGIN`` and ``CURIE_REPO_PATH`` (ADR 0197), which the worker
    sets on every workspace claim. Without them the snapshot is refused. The
    path is accepted at any depth, but the workspace origin must be exactly
    that path under the trusted origin.
    """

    repo = Path(workspace)
    if not repo.is_dir():
        raise WorkspaceSnapshotError("managed workspace directory is missing")
    inside = _git(repo, "rev-parse", "--is-inside-work-tree").stdout.strip()
    if inside != b"true":
        raise WorkspaceSnapshotError("managed workspace is not a git checkout")

    origin = _git(repo, "remote", "get-url", "origin").stdout.decode().strip()
    actual_repo = _canonical_repo(
        origin, trusted_origin=trusted_origin, repository_path=repository_path
    )
    if expected_repo is not None and (
        actual_repo.casefold() != expected_repo.strip().removesuffix(".git").casefold()
    ):
        raise WorkspaceSnapshotError(
            f"workspace repository {actual_repo!r} does not match configured repository"
        )

    object_format = _verify_base(repo, base_sha)
    head = _head(repo, len(base_sha))
    if head is None:
        raise WorkspaceHistoryError(_NOT_DESCENDANT)
    _prove_descendant(repo, head, base_sha, object_format)

    base = _tree_entries(repo, base_sha)
    first = _observe(repo, base, object_format)
    desired = _desired_entries(first.records)
    changed = sorted(path for path in {*base, *desired} if base.get(path) != desired.get(path))
    changed_paths = tuple(
        _publishable_path(path, (base.get(path), desired.get(path))) for path in changed
    )
    blobs = [path for path in changed if first.records[path][0] == "file"]
    patch = _build_patch(repo, base_sha, desired, blobs, changed)
    # Capture is several reads with no isolation between them. A write landing
    # in between can patch content the disclosed list omits, so observe again
    # and refuse a moved workspace.
    if _head(repo, len(base_sha)) != head or _observe(repo, base, object_format) != first:
        raise WorkspaceSnapshotError(_CHANGED)
    validate_patch(patch)
    return WorkspaceSnapshot(
        repo_full_name=actual_repo,
        base_sha=base_sha,
        patch=patch,
        changed_paths=changed_paths,
        contains_workflow_files=any(
            path.startswith(".github/workflows/") for path in changed_paths
        ),
        publication_title=publication_title,
        publication_body=publication_body,
    )


def workspace_snapshotter(
    workspace: Path,
    *,
    trusted_origin: str | None,
    repository_path: str | None,
    proposal: Callable[[], tuple[str | None, str | None]],
) -> Callable[[str], WorkspaceSnapshot]:
    """The ``/v1/snapshot`` capture of the mounted workspace.

    The route supplies the worker's prepared base. ``proposal`` returns the
    approval gate's publication title and body at capture time.
    """

    def capture(base_sha: str) -> WorkspaceSnapshot:
        title, body = proposal()
        return capture_workspace_snapshot(
            workspace,
            base_sha=base_sha,
            trusted_origin=trusted_origin,
            repository_path=repository_path,
            publication_title=title,
            publication_body=body,
        )

    return capture
