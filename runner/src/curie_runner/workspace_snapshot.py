"""Bounded, credential-free snapshots of the managed repository workspace.

The runner exposes the result only to its per-sandbox bearer-token holder.  It
does not publish anything: the worker validates and durably stores the patch
before the sandbox is suspended, and a separate platform Job applies it after
approval.
"""

from __future__ import annotations

import base64
import os
import re
import shlex
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit, urlunsplit

from .subprocess_env import shell_and_hook_env

MAX_PATCH_BYTES = 900_000
_GIT_TIMEOUT_SECONDS = 30
_SHA_RE = re.compile(r"^[0-9a-f]{40,64}$")
# One segment of a declared repository path (ADR 0197): GitLab nests groups, so
# a code host other than GitHub has any number of these. Dot segments are
# refused separately, since a URL would resolve them away.
_PATH_SEGMENT = re.compile(r"[A-Za-z0-9_][A-Za-z0-9._~-]*")


class WorkspaceSnapshotError(RuntimeError):
    """The workspace cannot be represented as a safe publication patch."""


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


def _git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            ["git", *args],
            cwd=repo,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=_GIT_TIMEOUT_SECONDS,
            check=check,
            env=shell_and_hook_env(
                os.environ,
                extra={"GIT_TERMINAL_PROMPT": "0", "GIT_CONFIG_NOSYSTEM": "1"},
            ),
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


def _changed_paths(repo: Path) -> tuple[str, ...]:
    # Keep path enumeration identical to patch capture. Explicitly disabling
    # rename detection represents a rename as one deletion plus one addition,
    # so both paths are validated and disclosed to the approver.
    tracked = _git(repo, "diff", "--no-renames", "--name-only", "-z", "HEAD", "--").stdout
    untracked = _git(repo, "ls-files", "--others", "--exclude-standard", "-z").stdout
    paths: set[str] = set()
    for item in (tracked + untracked).split(b"\0"):
        if not item:
            continue
        try:
            raw = item.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise WorkspaceSnapshotError("workspace contains a non-UTF-8 path") from exc
        safe = _safe_repository_path(raw)
        candidate = repo / safe
        try:
            mode = candidate.lstat().st_mode
        except FileNotFoundError:
            # Deleted tracked paths are represented by their patch and have no
            # filesystem object to validate.
            paths.add(safe)
            continue
        if not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
            raise WorkspaceSnapshotError(f"workspace path {safe!r} is not a regular file")
        paths.add(safe)
    return tuple(sorted(paths))


def _untracked_patch(repo: Path, paths: tuple[str, ...]) -> bytes:
    chunks: list[bytes] = []
    for path in paths:
        tracked = _git(repo, "ls-files", "--error-unmatch", "--", path, check=False)
        if tracked.returncode == 0:
            continue
        result = _git(
            repo,
            "diff",
            "--no-index",
            "--binary",
            "--",
            "/dev/null",
            path,
            check=False,
        )
        if result.returncode not in (0, 1):
            diagnostic = result.stderr.decode("utf-8", errors="replace").strip()
            raise WorkspaceSnapshotError(f"git could not capture untracked path: {diagnostic}")
        chunks.append(result.stdout)
    return b"".join(chunks)


def capture_workspace_snapshot(
    workspace: str | Path = "/workspace",
    *,
    expected_repo: str | None = None,
    trusted_origin: str | None = None,
    repository_path: str | None = None,
    publication_title: str | None = None,
    publication_body: str | None = None,
) -> WorkspaceSnapshot:
    """Capture the workspace as a publication patch, refusing an untrusted origin.

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

    base_sha = _git(repo, "rev-parse", "HEAD").stdout.decode().strip().lower()
    if not _SHA_RE.fullmatch(base_sha):
        raise WorkspaceSnapshotError("workspace base commit is not a valid object id")

    changed_paths = _changed_paths(repo)
    patch = _git(
        repo,
        "diff",
        "--no-renames",
        "--binary",
        "--full-index",
        "HEAD",
        "--",
    ).stdout
    patch += _untracked_patch(repo, changed_paths)
    validate_patch(patch)
    return WorkspaceSnapshot(
        repo_full_name=actual_repo,
        base_sha=base_sha,
        patch=enforce_patch_cap(patch),
        changed_paths=changed_paths,
        contains_workflow_files=any(
            path.startswith(".github/workflows/") for path in changed_paths
        ),
        publication_title=publication_title,
        publication_body=publication_body,
    )
