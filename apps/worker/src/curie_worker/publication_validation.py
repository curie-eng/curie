"""Trusted worker validation of a runner snapshot against its private base."""

from __future__ import annotations

import os
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path, PurePosixPath

from .runner_client import RunnerWorkspaceSnapshot
from .workspace import (
    WorkspaceClaimCoordinator,
    WorkspacePreparationError,
    scrubbed_git_environment,
)

MAX_PATCH_BYTES = 900_000
_SAFE_GIT_MODES = {"000000", "100644", "100755"}
GITHUB_WORKFLOW_REFUSAL = "GitHub workflow changes cannot be published by this capability"
GITHUB_METADATA_REFUSAL = "GitHub metadata changes cannot be published by this capability"
OPERATOR_PROTECTED_REFUSAL = (
    "operator-protected path changes cannot be published by this capability"
)
UNSAFE_PATH_REFUSAL = "snapshot contains an unsafe repository path"
_REFUSAL_PRIORITY = (
    GITHUB_WORKFLOW_REFUSAL,
    GITHUB_METADATA_REFUSAL,
    OPERATOR_PROTECTED_REFUSAL,
    UNSAFE_PATH_REFUSAL,
)


def publication_git_environment(home: Path) -> dict[str, str]:
    """Return a credential-free, configuration-free Git subprocess environment."""

    return scrubbed_git_environment(
        {
            "HOME": str(home),
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_SYSTEM": os.devnull,
            "GIT_TERMINAL_PROMPT": "0",
        }
    )


def normalize_protected_publication_paths(paths: tuple[str, ...]) -> tuple[str, ...]:
    """Reject protected-path entries a repository-relative check cannot apply."""

    normalized: list[str] = []
    for path in paths:
        pure = PurePosixPath(path)
        if (
            not path
            or path.startswith("/")
            or "\\" in path
            or pure.is_absolute()
            or any(part in ("", ".", "..") for part in pure.parts)
        ):
            raise ValueError("protected publication paths must be repository-relative paths")
        normalized.append("/".join(pure.parts))
    return tuple(normalized)


def publication_path_refusal(path: str, protected_paths: tuple[str, ...]) -> str | None:
    """Return the named refusal for one changed path, or None when it may publish."""

    pure = PurePosixPath(path)
    structurally_safe = bool(
        path
        and not path.startswith("/")
        and "\\" not in path
        and not pure.is_absolute()
        and all(part not in ("", ".", "..") for part in pure.parts)
        and pure.parts[0].casefold() != ".git"
    )
    if not structurally_safe:
        return UNSAFE_PATH_REFUSAL
    folded = tuple(part.casefold() for part in pure.parts)
    if folded[:2] == (".github", "workflows"):
        return GITHUB_WORKFLOW_REFUSAL
    if folded[0] == ".github":
        return GITHUB_METADATA_REFUSAL
    for entry in protected_paths:
        entry_parts = tuple(part.casefold() for part in PurePosixPath(entry).parts)
        if folded[: len(entry_parts)] == entry_parts:
            return OPERATOR_PROTECTED_REFUSAL
    return None


def _safe_changed_path(path: str, protected_paths: tuple[str, ...]) -> bool:
    return publication_path_refusal(path, protected_paths) is None


def _validate_changed_paths(paths: tuple[str, ...], protected_paths: tuple[str, ...]) -> None:
    refusals = [publication_path_refusal(path, protected_paths) for path in paths]
    for reason in _REFUSAL_PRIORITY:
        if reason in refusals:
            raise WorkspacePreparationError("publication-validation", reason)


def validate_snapshot_against_base(
    coordinator: WorkspaceClaimCoordinator,
    *,
    thread_key: str,
    snapshot: RunnerWorkspaceSnapshot,
    max_patch_bytes: int = MAX_PATCH_BYTES,
    scratch_root: Path | None = None,
    git_timeout_seconds: int = 30,
    protected_paths: tuple[str, ...],
) -> None:
    """Rehash the private base and prove the binary patch applies to it."""

    prepared = coordinator.current(thread_key)
    if prepared is None:
        raise WorkspacePreparationError(
            "publication-validation", "thread has no retained sanitized base"
        )
    if snapshot.repo_full_name.casefold() != prepared.repo_full_name.casefold():
        raise WorkspacePreparationError(
            "publication-validation", "snapshot repository does not match sanitized base"
        )
    if snapshot.base_sha != prepared.base_sha:
        # #4121: name both commits so the factory run's details line is actionable.
        raise WorkspacePreparationError(
            "publication-validation",
            f"snapshot commit {snapshot.base_sha[:12]} does not match "
            f"sanitized base {prepared.base_sha[:12]}",
        )
    if len(snapshot.patch) > max_patch_bytes:
        raise WorkspacePreparationError(
            "publication-validation", f"patch exceeds {max_patch_bytes} raw bytes"
        )
    if not snapshot.changed_paths:
        if snapshot.patch:
            raise WorkspacePreparationError(
                "publication-validation", "snapshot patch has no declared changed paths"
            )
        return
    if not snapshot.patch:
        raise WorkspacePreparationError(
            "publication-validation", "snapshot changed paths have no patch"
        )
    protected_paths = normalize_protected_publication_paths(protected_paths)
    _validate_changed_paths(snapshot.changed_paths, protected_paths)

    scratch = Path(
        tempfile.mkdtemp(
            prefix="publication-validate-",
            dir=str(scratch_root) if scratch_root is not None else None,
        )
    )
    try:
        archive = scratch / "base.tar.gz"
        total = 0
        with archive.open("wb") as output:
            for chunk in coordinator.stream_current_base(thread_key):
                total += len(chunk)
                if total > coordinator.preparer.limits.max_archive_bytes:
                    raise WorkspacePreparationError(
                        "publication-validation", "private base archive exceeds configured limit"
                    )
                output.write(chunk)
        checkout = scratch / "checkout"
        checkout.mkdir(mode=0o700)
        try:
            with tarfile.open(archive, mode="r:gz") as source:
                source.extractall(checkout, filter="data")
        except (tarfile.TarError, OSError) as exc:
            raise WorkspacePreparationError(
                "publication-validation", "private base archive could not be extracted"
            ) from exc
        patch_file = scratch / "changes.patch"
        patch_file.write_bytes(snapshot.patch)
        git_env = publication_git_environment(scratch)
        try:
            subprocess.run(
                ["git", "init", "--quiet"],
                cwd=checkout,
                env=git_env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                timeout=git_timeout_seconds,
                check=True,
            )
            subprocess.run(
                ["git", "add", "-A"],
                cwd=checkout,
                env=git_env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                timeout=git_timeout_seconds,
                check=True,
            )
            base_tree = (
                subprocess.run(
                    ["git", "write-tree"],
                    cwd=checkout,
                    env=git_env,
                    stdin=subprocess.DEVNULL,
                    capture_output=True,
                    timeout=git_timeout_seconds,
                    check=True,
                )
                .stdout.decode("ascii")
                .strip()
            )
            subprocess.run(
                ["git", "apply", "--check", "--binary", str(patch_file)],
                cwd=checkout,
                env=git_env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                timeout=git_timeout_seconds,
                check=True,
            )
            subprocess.run(
                ["git", "apply", "--binary", str(patch_file)],
                cwd=checkout,
                env=git_env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                timeout=git_timeout_seconds,
                check=True,
            )
            subprocess.run(
                ["git", "add", "-A"],
                cwd=checkout,
                env=git_env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                timeout=git_timeout_seconds,
                check=True,
            )
            derived_raw = subprocess.run(
                ["git", "diff", "--cached", "--name-only", "-z", "--no-renames", base_tree],
                cwd=checkout,
                env=git_env,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=git_timeout_seconds,
                check=True,
            ).stdout
            derived_paths = tuple(
                sorted(path for path in derived_raw.decode("utf-8").split("\0") if path)
            )
            _validate_changed_paths(derived_paths, protected_paths)
            declared_paths = tuple(sorted(snapshot.changed_paths))
            if len(set(declared_paths)) != len(declared_paths) or declared_paths != derived_paths:
                raise WorkspacePreparationError(
                    "publication-validation",
                    "snapshot changed_paths do not exactly match the validated patch",
                )
            raw_diff = (
                subprocess.run(
                    ["git", "diff", "--cached", "--raw", "-z", "--no-renames", base_tree],
                    cwd=checkout,
                    env=git_env,
                    stdin=subprocess.DEVNULL,
                    capture_output=True,
                    timeout=git_timeout_seconds,
                    check=True,
                )
                .stdout.decode("utf-8", errors="strict")
                .split("\0")
            )
            for index in range(0, len(raw_diff) - 1, 2):
                header = raw_diff[index].split()
                if len(header) < 5:
                    raise WorkspacePreparationError(
                        "publication-validation", "patch produced unreadable Git metadata"
                    )
                old_mode = header[0].removeprefix(":")
                new_mode = header[1]
                if old_mode not in _SAFE_GIT_MODES or new_mode not in _SAFE_GIT_MODES:
                    raise WorkspacePreparationError(
                        "publication-validation",
                        "patch contains a symlink, submodule, or unsafe file mode",
                    )
        except FileNotFoundError as exc:
            raise WorkspacePreparationError(
                "publication-validation", "worker image does not contain git"
            ) from exc
        except subprocess.TimeoutExpired as exc:
            raise WorkspacePreparationError(
                "publication-validation",
                f"publication git validation exceeded {git_timeout_seconds} seconds",
            ) from exc
        except subprocess.CalledProcessError as exc:
            raise WorkspacePreparationError(
                "publication-validation", "patch does not apply to sanitized base"
            ) from exc
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
