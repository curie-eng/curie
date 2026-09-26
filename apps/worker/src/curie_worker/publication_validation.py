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


def _safe_changed_path(path: str) -> bool:
    pure = PurePosixPath(path)
    return bool(
        path
        and not path.startswith("/")
        and "\\" not in path
        and not pure.is_absolute()
        and all(part not in ("", ".", "..") for part in pure.parts)
        and pure.parts[0].casefold() != ".git"
        and tuple(part.casefold() for part in pure.parts[:2])
        != (".github", "workflows")
    )


def _validate_changed_paths(paths: tuple[str, ...]) -> None:
    if any(
        tuple(part.casefold() for part in PurePosixPath(path).parts[:2])
        == (".github", "workflows")
        for path in paths
    ):
        raise WorkspacePreparationError(
            "publication-validation",
            "GitHub workflow changes cannot be published by this capability",
        )
    if not all(_safe_changed_path(path) for path in paths):
        raise WorkspacePreparationError(
            "publication-validation", "snapshot contains an unsafe repository path"
        )


def validate_snapshot_against_base(
    coordinator: WorkspaceClaimCoordinator,
    *,
    thread_key: str,
    snapshot: RunnerWorkspaceSnapshot,
    max_patch_bytes: int = MAX_PATCH_BYTES,
    scratch_root: Path | None = None,
    git_timeout_seconds: int = 30,
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
        raise WorkspacePreparationError(
            "publication-validation", "snapshot commit does not match sanitized base"
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
    _validate_changed_paths(snapshot.changed_paths)

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
            base_tree = subprocess.run(
                ["git", "write-tree"],
                cwd=checkout,
                env=git_env,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=git_timeout_seconds,
                check=True,
            ).stdout.decode("ascii").strip()
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
            declared_paths = tuple(sorted(snapshot.changed_paths))
            if len(set(declared_paths)) != len(declared_paths) or declared_paths != derived_paths:
                raise WorkspacePreparationError(
                    "publication-validation",
                    "snapshot changed_paths do not exactly match the validated patch",
                )
            raw_diff = subprocess.run(
                ["git", "diff", "--cached", "--raw", "-z", "--no-renames", base_tree],
                cwd=checkout,
                env=git_env,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=git_timeout_seconds,
                check=True,
            ).stdout.decode("utf-8", errors="strict").split("\0")
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
