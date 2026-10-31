"""Bounded, credential-free snapshots of the managed repository workspace.

The worker names the prepared commit P it retained for the thread. The runner
proves the checkout's HEAD is P or a bounded local descendant of P, then
captures the net change from P to the final working tree files: committed,
staged, unstaged and untracked (not ignored) changes together. Local commits
never move the base.

The net change is the canonical candidate C: the final publishable contents,
built by ``build_candidate``, the one construction path that both this capture
and the dark factory review gate use. Every path of P's tree, the index and the
untracked listing is read from disk without following symlinks; git's stat
cache and git diff listings are never trusted. A file git reads (an index
entry not flagged skip-worktree or assume-unchanged) whose bytes differ from
P's entry is published as git's canonical clean form of those bytes, with the
attributes and line endings git applies; no clean or process filter program
runs, so such a path with a ``filter`` attribute is refused. One equal to P's
entry is unchanged. Paths git does not read (flagged index entries, P's paths
missing from the index, and untracked files) are published as their bytes on
disk, unconverted. Every object is written to a scratch index and object
directory, so the real index, refs, HEAD and object store are only read. The
workspace is observed before and after C is built, and a difference refuses
it.

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
import posixpath
import re
import shlex
import shutil
import stat
import subprocess
import tempfile
import time
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
_BUDGET = "workspace candidate construction exceeded its time budget"
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
# Every diff between P and C: no workspace config moves the paths, drops the
# a/ and b/ prefixes, or reads an order file.
_DIFF_CONFIG = (
    "-c",
    "diff.relative=false",
    "-c",
    "diff.noprefix=false",
    "-c",
    "diff.orderFile=/dev/null",
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


class UnsupportedFilterError(WorkspaceSnapshotError):
    """A changed path git would convert has a ``filter`` attribute.

    The candidate is built without running repository- or user-configured
    filter programs, so the change to that path cannot be published.
    """


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
    stdin: bytes | IO[bytes] | None = None,
    env: Mapping[str, str] | None = None,
    stdout: IO[bytes] | None = None,
    deadline: float | None = None,
) -> subprocess.CompletedProcess[bytes]:
    # Bytes are sent as git's input; an open file becomes its stdin directly.
    data = stdin if isinstance(stdin, bytes) else None
    handle = None if stdin is None or isinstance(stdin, bytes) else stdin
    # ``deadline`` (a time.monotonic() value) caps the call below the 30 s limit.
    timeout = float(_GIT_TIMEOUT_SECONDS)
    if deadline is not None:
        timeout = min(timeout, deadline - time.monotonic())
        if timeout <= 0:
            raise WorkspaceSnapshotError(_BUDGET)
    try:
        return subprocess.run(
            ["git", *_GIT_CONFIG, *args],
            cwd=repo,
            input=None if handle is not None else (data or b""),
            stdin=handle,
            stdout=subprocess.PIPE if stdout is None else stdout,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=check,
            env=shell_and_hook_env(os.environ, extra={**_GIT_ENV, **(env or {})}),
        )
    except FileNotFoundError as exc:
        raise WorkspaceSnapshotError(
            "git is not installed in the runner image; repository publication is unavailable"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        if deadline is not None and time.monotonic() >= deadline:
            raise WorkspaceSnapshotError(_BUDGET) from exc
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


def _verify_base(repo: Path, base_sha: str, *, deadline: float | None = None) -> str:
    """Return the object format once ``base_sha`` names a commit in the workspace."""

    if not isinstance(base_sha, str) or not _OID_RE.fullmatch(base_sha):
        raise WorkspaceSnapshotError("the prepared base commit is not a full lowercase object id")
    object_format = (
        _git(repo, "rev-parse", "--show-object-format", deadline=deadline)
        .stdout.decode("ascii", errors="replace")
        .strip()
    )
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
        deadline=deadline,
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


def _tree_entries(
    repo: Path, base_sha: str, deadline: float | None = None
) -> dict[bytes, tuple[str, str]]:
    """Every path of the prepared base's tree, as (mode, object id)."""

    entries: dict[bytes, tuple[str, str]] = {}
    listed = _git(repo, "ls-tree", "-r", "-z", "--full-tree", base_sha, deadline=deadline)
    for item in listed.stdout.split(b"\0"):
        if not item:
            continue
        meta, separator, path = item.partition(b"\t")
        fields = meta.decode("ascii", errors="replace").split(" ")
        if not separator or len(fields) != 3:
            raise WorkspaceSnapshotError("git listed a malformed tree entry")
        entries[path] = (fields[0], fields[2])
    return entries


# One path as observed on disk: ("absent",), ("directory",), ("gitlink", oid),
# ("symlink", oid) or ("file", mode, oid) with the raw bytes' blob id, or
# ("kept", mode, oid) for P's own entry left in place (see ``_observe``).
_Record = tuple[str, ...]
_ABSENT: _Record = ("absent",)
# Index flag tags (``git ls-files -v``) of a skip-worktree entry; the lowercase
# form also has assume-unchanged set.
_SKIP_WORKTREE_TAGS = frozenset({"S", "s"})


def _flagged(tag: str) -> bool:
    """git does not read the file of an entry flagged skip-worktree or assume-unchanged."""

    return tag in _SKIP_WORKTREE_TAGS or tag.islower()


def _check_deadline(deadline: float | None) -> None:
    if deadline is not None and time.monotonic() >= deadline:
        raise WorkspaceSnapshotError(_BUDGET)


@dataclass(frozen=True)
class _Observation:
    """One read of the index with its flags, the untracked listing and every candidate path."""

    index: Mapping[bytes, tuple[str, str]]
    tags: Mapping[bytes, str]
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


def _hash_file(parent_fd: int, name: bytes, object_format: str, deadline: float | None) -> _Record:
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
            _check_deadline(deadline)
            read += len(chunk)
            digest.update(chunk)
        if read != info.st_size:
            raise WorkspaceSnapshotError(_CHANGED)
        mode = "100755" if info.st_mode & stat.S_IXUSR else "100644"
        return ("file", mode, digest.hexdigest())
    finally:
        os.close(fd)


def _record(
    root_fd: int, path: bytes, gitlink: str | None, object_format: str, deadline: float | None
) -> _Record:
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
            return _hash_file(parent, name, object_format, deadline)
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
    repo: Path, base: Mapping[bytes, tuple[str, str]], object_format: str, deadline: float | None
) -> _Observation:
    """Read every path of P's tree, the index with its flags and the untracked listing.

    Every candidate path is read from the filesystem, never from git's stat
    cache or a git diff listing, with one exception: a skip-worktree index
    entry equal to P's entry whose path is absent keeps P's entry. That is the
    state worker preparation leaves when it unlinks a committed symlink that
    escapes the checkout, so it is not a deletion the agent made. Because the
    kept entry and the flags are part of the observation, the second
    observation compares them too.
    """

    index: dict[bytes, tuple[str, str]] = {}
    tags: dict[bytes, str] = {}
    listed = _git(repo, "ls-files", "-s", "-v", "-z", deadline=deadline).stdout
    for item in listed.split(b"\0"):
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
        tags[path] = tag
    untracked: list[bytes] = []
    others = _git(repo, "ls-files", "-z", "--others", "--exclude-standard", deadline=deadline)
    for item in others.stdout.split(b"\0"):
        if not item:
            continue
        if item.endswith(b"/"):
            raise WorkspaceSnapshotError(
                f"workspace path {_display(item.rstrip(b'/'))!r} is an untracked nested repository"
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
            _check_deadline(deadline)
            record = _record(root_fd, path, _gitlink(path, index, base), object_format, deadline)
            kept = base.get(path)
            if (
                record == _ABSENT
                and tags.get(path) in _SKIP_WORKTREE_TAGS
                and kept is not None
                and index.get(path) == kept
            ):
                record = ("kept", *kept)
            records[path] = record
    finally:
        os.close(root_fd)
    return _Observation(index=index, tags=tags, untracked=tuple(untracked), records=records)


def _desired_entries(records: Mapping[bytes, _Record]) -> dict[bytes, tuple[str, str]]:
    """The tree the observed files describe: path to (mode, object id).

    A regular file carries the blob id of its raw bytes here; ``build_candidate``
    replaces it with git's clean form where git reads the file.
    """

    desired: dict[bytes, tuple[str, str]] = {}
    for path, record in records.items():
        if record[0] in ("file", "kept"):
            desired[path] = (record[1], record[2])
        elif record[0] == "symlink":
            desired[path] = (_SYMLINK_MODE, record[1])
        elif record[0] == "gitlink":
            desired[path] = (_GITLINK_MODE, record[1])
    return desired


def _candidate_path(raw: bytes) -> str:
    """A changed path as text, refused unless it is UTF-8 and a safe repository path."""

    try:
        path = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise WorkspaceSnapshotError("workspace contains a non-UTF-8 path") from exc
    if _safe_repository_path(path) != path or "\n" in path:
        raise WorkspaceSnapshotError(f"unsafe patch path {path!r}")
    return path


def _regular_change(entries: tuple[tuple[str, str] | None, tuple[str, str] | None]) -> bool:
    """Whether a change at a path involves only regular files (or their absence)."""

    return not any(
        entry is not None and entry[0] in (_SYMLINK_MODE, _GITLINK_MODE) for entry in entries
    )


def _write_symlink_blob(
    repo: Path, path: bytes, oid: str, object_format: str, scratch: Path, deadline: float | None
) -> None:
    """Write a changed symlink's target to the scratch store, so C's patch can show it."""

    parts = path.split(b"/")
    try:
        root_fd = os.open(repo, os.O_RDONLY | os.O_DIRECTORY)
    except OSError as exc:
        raise WorkspaceSnapshotError("managed workspace directory is missing") from exc
    try:
        parent = _open_parent(root_fd, parts[:-1])
        if parent is None:
            raise WorkspaceSnapshotError(_CHANGED)
        try:
            target = os.readlink(parts[-1], dir_fd=parent)
        finally:
            os.close(parent)
    except OSError as exc:
        raise WorkspaceSnapshotError(_CHANGED) from exc
    finally:
        os.close(root_fd)
    if _object_id(object_format, b"blob", target) != oid:
        raise WorkspaceSnapshotError(_CHANGED)
    written = _git(
        repo,
        "hash-object",
        "-w",
        "--no-filters",
        "--stdin",
        stdin=target,
        env=_scratch_env(scratch),
        deadline=deadline,
    )
    if written.stdout.decode("ascii", errors="replace").strip() != oid:
        raise WorkspaceSnapshotError(_CHANGED)


def _stdin_path(path: bytes) -> bytes:
    """A path as ``hash-object --stdin-paths`` reads it: a leading quote means C quoting."""

    if b"\n" in path:
        raise WorkspaceSnapshotError(f"unsafe patch path {_display(path)!r}")
    if not path.startswith(b'"'):
        return path
    return b'"' + path.replace(b"\\", b"\\\\").replace(b'"', b'\\"') + b'"'


def _scratch_env(scratch: Path) -> dict[str, str]:
    """git's environment for the scratch index and object directory."""

    return {
        "GIT_INDEX_FILE": str(scratch / "index"),
        "GIT_OBJECT_DIRECTORY": str(scratch / "objects"),
    }


def _open_file(root_fd: int, path: bytes) -> int | None:
    """Open a workspace file without following symlinks.

    ``None`` when the path is absent or is not a regular file.
    """

    parts = path.split(b"/")
    try:
        parent = _open_parent(root_fd, parts[:-1])
        if parent is None:
            return None
        try:
            fd = os.open(
                parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_NOCTTY, dir_fd=parent
            )
        finally:
            os.close(parent)
    except OSError as exc:
        if exc.errno in (errno.ENOENT, errno.ENOTDIR, errno.ELOOP, errno.ENXIO):
            return None
        raise WorkspaceSnapshotError(
            f"workspace path {_display(path)!r} could not be read"
        ) from exc
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        return None
    return fd


def _copy_file(fd: int, target: Path, object_format: str, deadline: float | None) -> str:
    """Copy an open workspace file to ``target``; the blob id of the bytes copied."""

    size = os.fstat(fd).st_size
    digest = hashlib.new(object_format, usedforsecurity=False)
    digest.update(b"blob %d\0" % size)
    copied = 0
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("xb") as out:
        while chunk := os.read(fd, _READ_CHUNK):
            _check_deadline(deadline)
            copied += len(chunk)
            digest.update(chunk)
            out.write(chunk)
    if copied != size:
        raise WorkspaceSnapshotError(_CHANGED)
    return digest.hexdigest()


def _refuse_filters(
    worktree: Path, paths: list[bytes], env: Mapping[str, str], deadline: float | None
) -> None:
    """Raise ``UnsupportedFilterError`` for a path with any ``filter`` attribute.

    This is git's own attribute lookup with the index and work tree ``git
    add`` uses, so the work tree and index-retained ``.gitattributes``, nested
    files, ``info/attributes`` and ``core.attributesFile`` all apply.
    """

    listed = _git(
        worktree,
        "check-attr",
        "--all",
        "-z",
        "--stdin",
        stdin=b"".join(path + b"\0" for path in paths),
        env=env,
        deadline=deadline,
    ).stdout
    fields = listed.split(b"\0")
    if fields[-1] != b"" or (len(fields) - 1) % 3:
        raise WorkspaceSnapshotError("git listed malformed path attributes")
    requested = set(paths)
    for index in range(0, len(fields) - 1, 3):
        path, attribute = fields[index], fields[index + 1]
        if path not in requested:
            raise WorkspaceSnapshotError("git listed malformed path attributes")
        # Any listed form refuses. git check-attr prints a driver literally
        # named unspecified, set or unset exactly like those states, and
        # --all omits only attributes no line mentions, so refusing every
        # listed filter is the sound boundary. An explicit -filter is refused
        # too, a conservative limitation.
        if attribute == b"filter":
            raise UnsupportedFilterError(
                f"workspace path {_display(path)!r} has a filter attribute; the candidate "
                "is built without running filter programs, so a change to this path "
                "cannot be published. Remove the filter attribute for this path, or "
                "leave the file as it is in the prepared base"
            )


def _clean_blobs(
    repo: Path,
    git_dir: str,
    real_objects: Path,
    paths: list[bytes],
    records: Mapping[bytes, _Record],
    object_format: str,
    scratch: Path,
    deadline: float | None,
) -> list[str]:
    """git's canonical clean form of each path's final bytes on disk, written to scratch.

    Each file is copied into ``<scratch>/worktree`` without following symlinks
    and must still be the bytes observed. Every ``.gitattributes`` that is a
    regular file on disk in the root or an ancestor directory of those paths is
    copied too; git skips any other kind. A relative ``core.attributesFile``,
    which git resolves against the work tree root, is copied the same way, and
    one that leaves the workspace is refused. One ``git add --renormalize`` then
    runs against the real git directory with the scratch copy as its work tree
    and a scratch copy of the real index as its index, so git applies the
    attributes and built-in conversions (text, eol, ident,
    working-tree-encoding) it applies in ``git diff``, including its fallback
    to a ``.gitattributes`` kept in the index (``git hash-object`` never reads
    the index, so it would miss that fallback). Objects are written to the
    scratch object directory; the real index and objects are only read.

    No clean or process filter program runs. Before the add, ``git
    check-attr`` resolves every path's attributes with that same index and
    work tree, and a path with any ``filter`` attribute raises
    ``UnsupportedFilterError``.
    """

    worktree = scratch / "worktree"
    worktree.mkdir()
    attributes: dict[bytes, None] = {}
    for path in paths:
        parts = path.split(b"/")
        for depth in range(len(parts)):
            attributes[b"/".join([*parts[:depth], b".gitattributes"])] = None
    # git resolves a relative core.attributesFile against the work tree root,
    # so in the scratch work tree it is absent unless copied to the same
    # relative path, and a filter it sets would be missed. An absolute value
    # (``--path`` has already expanded ``~``) or the unset default resolves
    # identically from the scratch work tree.
    configured = _git(
        repo, "config", "--path", "--get", "core.attributesFile", check=False, deadline=deadline
    )
    if configured.returncode == 0:
        attributes_file = configured.stdout.removesuffix(b"\n")
    elif configured.returncode == 1 and not configured.stdout:
        attributes_file = b""
    else:
        raise WorkspaceSnapshotError("the workspace git config could not be read")
    relative: bytes | None = None
    if attributes_file and not os.path.isabs(os.fsdecode(attributes_file)):
        # A .. segment is refused before normalizing: git resolves it through
        # any symlink on the way, which a lexical normpath cannot reproduce.
        relative = posixpath.normpath(attributes_file)
        if b".." in attributes_file.split(b"/") or relative == b".":
            raise WorkspaceSnapshotError(
                f"core.attributesFile {_display(attributes_file)!r} is a relative path with a "
                ".. segment or outside the workspace; the candidate cannot resolve its attributes"
            )
        attributes[relative] = None
    converted = set(paths)
    try:
        root_fd = os.open(repo, os.O_RDONLY | os.O_DIRECTORY)
    except OSError as exc:
        raise WorkspaceSnapshotError("managed workspace directory is missing") from exc
    try:
        for path in [*paths, *(name for name in attributes if name not in converted)]:
            _check_deadline(deadline)
            fd = _open_file(root_fd, path)
            if fd is None:
                if path in converted:
                    raise WorkspaceSnapshotError(_CHANGED)
                # git follows symlinks to the attributes file; the copy does
                # not, so one present by any other route is refused.
                if path == relative and os.path.lexists(os.path.join(repo, os.fsdecode(path))):
                    raise WorkspaceSnapshotError(
                        f"core.attributesFile {_display(attributes_file)!r} is not a regular "
                        "file reachable without symlinks; the candidate cannot resolve its "
                        "attributes"
                    )
                continue
            try:
                copied = _copy_file(fd, worktree / os.fsdecode(path), object_format, deadline)
            except OSError as exc:
                raise WorkspaceSnapshotError(
                    f"workspace path {_display(path)!r} could not be read"
                ) from exc
            finally:
                os.close(fd)
            if path in converted and copied != records[path][2]:
                raise WorkspaceSnapshotError(_CHANGED)
    finally:
        os.close(root_fd)
    real_index = repo / os.fsdecode(
        _git(repo, "rev-parse", "--git-path", "index", deadline=deadline).stdout.rstrip(b"\n")
    )
    clean_index = scratch / "clean-index"
    try:
        shutil.copyfile(real_index, clean_index)
    except OSError as exc:
        raise WorkspaceSnapshotError("the workspace index could not be read") from exc
    env = {
        "GIT_DIR": git_dir,
        "GIT_WORK_TREE": str(worktree),
        "GIT_INDEX_FILE": str(clean_index),
        "GIT_OBJECT_DIRECTORY": str(scratch / "objects"),
        # The index's .gitattributes blobs are read from the real store.
        "GIT_ALTERNATE_OBJECT_DIRECTORIES": str(real_objects),
        "GIT_LITERAL_PATHSPECS": "1",
    }
    _refuse_filters(worktree, paths, env, deadline)
    # core.safecrlf only decides whether git refuses a conversion it would make;
    # git diff never refuses, so the candidate must not either.
    _git(
        worktree,
        "-c",
        "core.safecrlf=false",
        "add",
        "--renormalize",
        "--pathspec-from-file=-",
        "--pathspec-file-nul",
        stdin=b"".join(path + b"\0" for path in paths),
        env=env,
        deadline=deadline,
    )
    staged: dict[bytes, str] = {}
    listed = _git(worktree, "ls-files", "-s", "-z", env=env, deadline=deadline).stdout
    for item in filter(None, listed.split(b"\0")):
        meta, separator, name = item.partition(b"\t")
        fields = meta.decode("ascii", errors="replace").split(" ")
        if not separator or len(fields) != 3:
            raise WorkspaceSnapshotError("git listed a malformed index entry")
        if name in converted:
            staged[name] = fields[1]
    blobs = [staged.get(path, "") for path in paths]
    if any(
        len(blob) != _OID_LENGTH[object_format] or not _OID_RE.fullmatch(blob) for blob in blobs
    ):
        raise WorkspaceSnapshotError("git wrote an invalid blob id")
    return blobs


def _write_tree(
    repo: Path,
    base_sha: str,
    desired: Mapping[bytes, tuple[str, str]],
    blobs: list[bytes],
    changed: list[bytes],
    scratch: Path,
    real_objects: Path,
    deadline: float | None,
) -> str:
    """Build tree C in the scratch index and object directory; its id.

    ``blobs`` are the changed files published as their raw bytes; every clean
    form is already in the scratch object directory.
    """

    # Building C never consults the real store; only the diff reads it.
    isolated = _scratch_env(scratch)
    if blobs:
        printed = _git(
            repo,
            "hash-object",
            "-w",
            "--no-filters",
            "--stdin-paths",
            stdin=b"".join(_stdin_path(path) + b"\n" for path in blobs),
            env=isolated,
            deadline=deadline,
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
        deadline=deadline,
    )
    tree = (
        _git(repo, "write-tree", "--missing-ok", env=isolated, deadline=deadline)
        .stdout.decode("ascii", errors="replace")
        .strip()
    )
    if len(tree) != len(base_sha) or not _OID_RE.fullmatch(tree):
        raise WorkspaceSnapshotError("git wrote an invalid snapshot tree id")
    with_base = {**isolated, "GIT_ALTERNATE_OBJECT_DIRECTORIES": str(real_objects)}
    names = _git(
        repo,
        *_DIFF_CONFIG,
        "diff",
        "--no-renames",
        "--name-only",
        "-z",
        base_sha,
        tree,
        "--",
        env=with_base,
        deadline=deadline,
    ).stdout
    if sorted(name for name in names.split(b"\0") if name) != changed:
        raise WorkspaceSnapshotError("the snapshot tree does not match the observed workspace")
    return tree


def _render_patch(
    repo: Path, base_sha: str, tree: str, scratch: Path, real_objects: Path, deadline: float | None
) -> bytes:
    """The binary patch from P to tree C, within ``MAX_PATCH_BYTES``."""

    with_base = {**_scratch_env(scratch), "GIT_ALTERNATE_OBJECT_DIRECTORIES": str(real_objects)}
    patch_file = scratch / "patch"
    with patch_file.open("wb") as handle:
        # Explicitly disabling rename detection represents a rename as one
        # deletion plus one addition, so both paths are disclosed.
        _git(
            repo,
            *_DIFF_CONFIG,
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
            deadline=deadline,
        )
    with patch_file.open("rb") as handle:
        return enforce_patch_cap(handle.read(MAX_PATCH_BYTES + 1))


@dataclass(frozen=True)
class WorkspaceCandidate:
    """The canonical candidate C: the final publishable contents against P."""

    base_sha: str
    tree: str
    changed_paths: tuple[str, ...]
    patch: bytes | None
    # Changed paths whose change involves a symlink or a gitlink; C shows them,
    # and the publication capture refuses them.
    unpublishable: tuple[str, ...] = ()


def build_candidate(
    workspace: str | Path,
    base_sha: str,
    *,
    render_patch: bool = False,
    deadline: float | None = None,
) -> WorkspaceCandidate:
    """Build the canonical candidate C for ``base_sha`` from the checkout's final files.

    This is the one construction path: the runner capture publishes C and the
    review gate binds review and approval to it. Every candidate path (P's
    tree, the index and the untracked listing) is read from disk without
    following symlinks; git's stat cache and git diff listings are never
    trusted. An index entry git reads (not flagged skip-worktree or
    assume-unchanged, not a gitlink) whose file differs from P's entry
    publishes git's canonical clean form of the final bytes on disk; flagged
    entries, P's paths missing from the index and untracked paths publish their
    bytes on disk, unconverted. No clean or process filter program runs: a
    converted path with a ``filter`` attribute raises
    ``UnsupportedFilterError``. ``tree`` is C's tree id, ``changed_paths`` the
    paths that differ from P, and ``patch`` the rendered binary patch when
    ``render_patch`` is true, else ``None``.

    ``deadline`` is a ``time.monotonic()`` value bounding every git call and
    file read. Running out raises ``WorkspaceSnapshotError``, as does every
    other failure; there is no partial result. No origin, ancestry or HEAD
    check runs here.
    """

    repo = Path(workspace)
    if not repo.is_dir():
        raise WorkspaceSnapshotError("managed workspace directory is missing")
    object_format = _verify_base(repo, base_sha, deadline=deadline)
    located = _git(
        repo, "rev-parse", "--show-toplevel", "--absolute-git-dir", deadline=deadline
    ).stdout.split(b"\n")
    if len(located) != 3 or located[2] or not located[0] or not located[1]:
        raise WorkspaceSnapshotError("managed workspace is not a git checkout")
    if os.path.realpath(os.fsdecode(located[0])) != os.path.realpath(repo):
        raise WorkspaceSnapshotError("managed workspace is not the root of its git checkout")
    git_dir = os.fsdecode(located[1])
    objects_path = (
        _git(repo, "rev-parse", "--git-path", "objects", deadline=deadline)
        .stdout.decode("utf-8", errors="surrogateescape")
        .strip()
    )
    real_objects = (repo / objects_path).absolute()

    base = _tree_entries(repo, base_sha, deadline)
    first = _observe(repo, base, object_format, deadline)
    scratch = Path(tempfile.mkdtemp(prefix="curie-snapshot-"))
    try:
        (scratch / "objects").mkdir()
        desired = _desired_entries(first.records)
        # A file git reads whose raw bytes are not P's entry publishes git's
        # clean form of them; one equal to P's entry is unchanged as it is.
        convert = [
            path
            for path, record in first.records.items()
            if record[0] == "file"
            and path in first.index
            and first.index[path][0] != _GITLINK_MODE
            and not _flagged(first.tags[path])
            and base.get(path) != desired[path]
        ]
        if convert:
            clean = _clean_blobs(
                repo,
                git_dir,
                real_objects,
                convert,
                first.records,
                object_format,
                scratch,
                deadline,
            )
            for path, blob in zip(convert, clean, strict=True):
                desired[path] = (desired[path][0], blob)
        converted = set(convert)
        changed = sorted(path for path in {*base, *desired} if base.get(path) != desired.get(path))
        changed_paths = tuple(_candidate_path(path) for path in changed)
        unpublishable = tuple(
            text
            for path, text in zip(changed, changed_paths, strict=True)
            if not _regular_change((base.get(path), desired.get(path)))
        )
        for path in changed:
            entry = desired.get(path)
            if entry is not None and entry[0] == _SYMLINK_MODE:
                _write_symlink_blob(repo, path, entry[1], object_format, scratch, deadline)
        blobs = [
            path for path in changed if first.records[path][0] == "file" and path not in converted
        ]
        tree = _write_tree(repo, base_sha, desired, blobs, changed, scratch, real_objects, deadline)
        patch = (
            _render_patch(repo, base_sha, tree, scratch, real_objects, deadline)
            if render_patch
            else None
        )
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    # Construction is several reads with no isolation between them. A write
    # landing in between can change content the disclosed list omits, so
    # observe again and refuse a moved workspace.
    if _observe(repo, base, object_format, deadline) != first:
        raise WorkspaceSnapshotError(_CHANGED)
    return WorkspaceCandidate(
        base_sha=base_sha,
        tree=tree,
        changed_paths=changed_paths,
        patch=patch,
        unpublishable=unpublishable,
    )


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
    ``WorkspaceHistoryError``. The patch is the canonical candidate C that
    ``build_candidate`` renders.

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

    candidate = build_candidate(repo, base_sha, render_patch=True)
    # HEAD must not have moved while C was built.
    if _head(repo, len(base_sha)) != head:
        raise WorkspaceSnapshotError(_CHANGED)
    if candidate.unpublishable:
        raise WorkspaceSnapshotError(
            f"workspace path {candidate.unpublishable[0]!r} is not a regular file"
        )
    if candidate.patch is None:
        raise WorkspaceSnapshotError("the workspace candidate has no rendered patch")
    patch = validate_patch(candidate.patch)
    return WorkspaceSnapshot(
        repo_full_name=actual_repo,
        base_sha=base_sha,
        patch=patch,
        changed_paths=candidate.changed_paths,
        contains_workflow_files=any(
            path.startswith(".github/workflows/") for path in candidate.changed_paths
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
