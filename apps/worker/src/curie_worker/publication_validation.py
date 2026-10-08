"""Trusted worker validation of a runner snapshot against its private base."""

from __future__ import annotations

import json
import os
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
import tomllib
from pathlib import Path, PurePosixPath
from typing import cast

import yaml
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

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
DEPENDENCY_ADDITION_REFUSAL = "dependency additions cannot be published by this capability"
UNSAFE_PATH_REFUSAL = "snapshot contains an unsafe repository path"
_REFUSAL_PRIORITY = (
    GITHUB_WORKFLOW_REFUSAL,
    GITHUB_METADATA_REFUSAL,
    OPERATOR_PROTECTED_REFUSAL,
    DEPENDENCY_ADDITION_REFUSAL,
    UNSAFE_PATH_REFUSAL,
)
_DEPENDENCY_MANIFESTS = {"pyproject.toml", "Cargo.toml", "package.json"}
_DEPENDENCY_FILES = _DEPENDENCY_MANIFESTS | {
    "uv.lock",
    "Cargo.lock",
    "pnpm-lock.yaml",
    "package-lock.json",
}
_JS_DEPENDENCY_SECTIONS = (
    "dependencies",
    "devDependencies",
    "optionalDependencies",
    "peerDependencies",
)
_CARGO_DEPENDENCY_SECTIONS = ("dependencies", "dev-dependencies", "build-dependencies")
_LOCAL_SPECIFIERS = ("link:", "file:", "workspace:")


def _dependency_table(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError("dependency table is malformed")
    return cast(dict[str, object], value)


def _dependency_list(value: object) -> list[object]:
    if not isinstance(value, list):
        raise ValueError("dependency list is malformed")
    return cast(list[object], value)


def _dependency_string(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("dependency string is malformed")
    return value


def _local_dependency_specifier(value: str) -> bool:
    return value.startswith((*_LOCAL_SPECIFIERS, "./", "../", "/")) or value in (".", "..")


def _python_source_is_local(value: object) -> bool:
    sources = _dependency_list(value) if isinstance(value, list) else [value]
    if not sources:
        raise ValueError("dependency source is malformed")
    local = True
    for value in sources:
        source = _dependency_table(value)
        remote = any(key in source for key in ("git", "registry", "url", "index"))
        for key in ("git", "registry", "url", "index", "path", "directory"):
            if key in source:
                _dependency_string(source[key])
        if "workspace" in source and not isinstance(source["workspace"], (bool, str)):
            raise ValueError("dependency source is malformed")
        if "editable" in source and not isinstance(source["editable"], (bool, str)):
            raise ValueError("dependency source is malformed")
        if "virtual" in source and not isinstance(source["virtual"], (bool, str)):
            raise ValueError("dependency source is malformed")
        internal = any(key in source for key in ("path", "directory")) or bool(
            isinstance(source.get("editable"), str)
            and source["editable"]
            or source.get("virtual")
            or source.get("workspace")
        )
        if not remote and not internal:
            raise ValueError("dependency source is malformed")
        local = local and internal and not remote
    return local


def _python_manifest_names(document: dict[str, object]) -> set[str]:
    project = _dependency_table(document.get("project", {}))
    uv = _dependency_table(_dependency_table(document.get("tool", {})).get("uv", {}))
    sources = {
        canonicalize_name(name, validate=True): _python_source_is_local(source)
        for name, source in _dependency_table(uv.get("sources", {})).items()
    }
    requirements = _dependency_list(project.get("dependencies", []))[:]
    for group in _dependency_table(project.get("optional-dependencies", {})).values():
        requirements.extend(_dependency_list(group))
    for group in _dependency_table(document.get("dependency-groups", {})).values():
        for requirement in _dependency_list(group):
            if isinstance(requirement, dict):
                included = _dependency_table(requirement)
                if set(included) != {"include-group"}:
                    raise ValueError("dependency group is malformed")
                _dependency_string(included["include-group"])
            else:
                requirements.append(requirement)
    requirements.extend(_dependency_list(uv.get("dev-dependencies", [])))
    build = _dependency_table(document.get("build-system", {}))
    requirements.extend(_dependency_list(build.get("requires", [])))
    names: set[str] = set()
    for value in requirements:
        requirement = Requirement(_dependency_string(value))
        name = canonicalize_name(requirement.name)
        if sources.get(name, False):
            continue
        if requirement.url is not None and _local_dependency_specifier(requirement.url):
            continue
        names.add(name)
    return names


def _cargo_name(value: object) -> str:
    name = _dependency_string(value)
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", name) is None:
        raise ValueError("dependency name is malformed")
    return name


def _cargo_manifest_names(document: dict[str, object]) -> set[str]:
    tables = [document, _dependency_table(document.get("workspace", {}))]
    tables.extend(
        _dependency_table(target)
        for target in _dependency_table(document.get("target", {})).values()
    )
    names: set[str] = set()
    for table in tables:
        for section in _CARGO_DEPENDENCY_SECTIONS:
            for alias, value in _dependency_table(table.get(section, {})).items():
                name = _cargo_name(alias)
                if isinstance(value, str):
                    _dependency_string(value)
                else:
                    dependency = _dependency_table(value)
                    if "package" in dependency:
                        name = _cargo_name(dependency["package"])
                    for key in ("path", "version", "git", "registry", "registry-index"):
                        if key in dependency:
                            _dependency_string(dependency[key])
                    if "workspace" in dependency and not isinstance(dependency["workspace"], bool):
                        raise ValueError("dependency source is malformed")
                    remote = any(
                        key in dependency
                        for key in ("version", "git", "registry", "registry-index")
                    )
                    if not remote:
                        if "path" in dependency or dependency.get("workspace") is True:
                            continue
                        raise ValueError("dependency source is malformed")
                names.add(name)
    return names


def _js_name(value: object) -> str:
    name = _dependency_string(value)
    if re.fullmatch(r"(?:@[A-Za-z0-9_.-]+/)?[A-Za-z0-9_][A-Za-z0-9_.-]*", name) is None:
        raise ValueError("dependency name is malformed")
    return name.lower()


def _js_manifest_names(document: dict[str, object]) -> set[str]:
    names: set[str] = set()
    for section in _JS_DEPENDENCY_SECTIONS:
        for name, value in _dependency_table(document.get(section, {})).items():
            name = _js_name(name)
            if not _local_dependency_specifier(_dependency_string(value)):
                names.add(name)
    return names


def _toml_lock_names(document: dict[str, object], *, python: bool) -> set[str]:
    names: set[str] = set()
    for value in _dependency_list(document.get("package", [])):
        package = _dependency_table(value)
        raw_name = _dependency_string(package.get("name"))
        name = canonicalize_name(raw_name, validate=True) if python else _cargo_name(raw_name)
        if "source" not in package:
            continue
        source = package["source"]
        if python:
            if _python_source_is_local(source):
                continue
        elif _local_dependency_specifier(_dependency_string(source)):
            continue
        names.add(name)
    return names


def _npm_lock_names(document: dict[str, object]) -> set[str]:
    names: set[str] = set()
    for path, value in _dependency_table(document.get("packages", {})).items():
        package = _dependency_table(value)
        if "name" in package:
            _js_name(package["name"])
        if "link" in package and not isinstance(package["link"], bool):
            raise ValueError("dependency source is malformed")
        resolved = _dependency_string(package["resolved"]) if "resolved" in package else ""
        if "node_modules/" not in path or package.get("link") is True:
            continue
        if _local_dependency_specifier(resolved):
            continue
        # Nested installs and scoped packages retain the suffix after the last
        # node_modules segment. The root and workspace directory entries are local.
        # Metadata cannot hide a new installed entry, nor can an existing alias
        # hide a newly named package. Compare both identities when present.
        names.add(_js_name(path.rsplit("node_modules/", 1)[1]))
        if "name" in package:
            names.add(_js_name(package["name"]))
    return names


def _pnpm_lock_names(document: dict[str, object]) -> set[str]:
    names: set[str] = set()
    for key, value in _dependency_table(document.get("packages", {})).items():
        package = _dependency_table(value)
        resolution = _dependency_table(package.get("resolution", {}))
        for field in ("directory", "tarball", "repo", "type"):
            if field in resolution:
                _dependency_string(resolution[field])
        key = key.removeprefix("/")
        # pnpm uses name@version in current lockfiles and name/version in older
        # lockfile versions. The name ends after two segments for scoped entries.
        if key.startswith("@"):
            scope, separator, remainder = key.partition("/")
            if not separator:
                raise ValueError("dependency entry is malformed")
            match = re.fullmatch(r"([^@/]+)([@/])(.+)", remainder)
            name = f"{scope}/{match[1]}" if match is not None else ""
        else:
            match = re.fullmatch(r"([^@/]+)([@/])(.+)", key)
            name = match[1] if match is not None else ""
        specifier = match[3] if match is not None else ""
        if (
            match is not None
            and match[2] == "/"
            and not specifier[:1].isdigit()
            and not _local_dependency_specifier(specifier)
        ):
            # A Git locator is not a package identity. Such entries need their
            # explicit name rather than collapsing different repos to one host.
            name = ""
        remote = (
            resolution.get("type") == "git"
            or "repo" in resolution
            or (
                "tarball" in resolution
                and not _local_dependency_specifier(_dependency_string(resolution["tarball"]))
            )
        )
        if not remote and (
            _local_dependency_specifier(specifier)
            or "directory" in resolution
            or resolution.get("type") == "directory"
            or (
                "tarball" in resolution
                and _local_dependency_specifier(_dependency_string(resolution["tarball"]))
            )
        ):
            continue
        if name:
            names.add(_js_name(name))
        elif "name" not in package:
            raise ValueError("dependency entry is malformed")
        if "name" in package:
            names.add(_js_name(package["name"]))
    return names


def _dependency_names(basename: str, contents: bytes) -> set[str]:
    text = contents.decode("utf-8")
    if basename in ("package.json", "package-lock.json"):
        document = _dependency_table(json.loads(text))
        return (
            _js_manifest_names(document)
            if basename == "package.json"
            else _npm_lock_names(document)
        )
    if basename == "pnpm-lock.yaml":
        return _pnpm_lock_names(_dependency_table(yaml.safe_load(text)))
    document = _dependency_table(tomllib.loads(text))
    if basename == "pyproject.toml":
        return _python_manifest_names(document)
    if basename == "Cargo.toml":
        return _cargo_manifest_names(document)
    return _toml_lock_names(document, python=basename == "uv.lock")


def _validate_dependency_additions(
    checkout: Path,
    base_tree: str,
    file_modes: dict[str, tuple[str, str]],
    git_env: dict[str, str],
    git_timeout_seconds: int,
) -> None:
    paths = [path for path in file_modes if PurePosixPath(path).name in _DEPENDENCY_FILES]
    # Report a manifest first when its accompanying lockfile also adds names.
    paths.sort(key=lambda path: (PurePosixPath(path).name not in _DEPENDENCY_MANIFESTS, path))
    for path in paths:
        old_mode, new_mode = file_modes[path]
        if new_mode == "000000":
            continue
        try:
            candidate = checkout / path
            if not stat.S_ISREG(candidate.lstat().st_mode):
                raise ValueError("dependency file is not regular")
            after = _dependency_names(candidate.name, candidate.read_bytes())
            before: set[str] = set()
            if old_mode != "000000":
                contents = subprocess.run(
                    ["git", "show", f"{base_tree}:{path}"],
                    cwd=checkout,
                    env=git_env,
                    stdin=subprocess.DEVNULL,
                    capture_output=True,
                    timeout=git_timeout_seconds,
                    check=True,
                ).stdout
                before = _dependency_names(candidate.name, contents)
            if after - before:
                raise ValueError("dependency set gained names")
        except (
            ValueError,
            OSError,
            yaml.YAMLError,
            RecursionError,
            subprocess.CalledProcessError,
        ) as exc:
            # Parser diagnostics and package identities are untrusted patch
            # content. Publish only the fixed reason and validated relative path.
            raise WorkspacePreparationError(
                "publication-validation", f"{DEPENDENCY_ADDITION_REFUSAL}: {path}"
            ) from exc


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
    allow_dependency_additions: bool,
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
            file_modes: dict[str, tuple[str, str]] = {}
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
                file_modes[raw_diff[index + 1]] = (old_mode, new_mode)
            if not allow_dependency_additions:
                _validate_dependency_additions(
                    checkout, base_tree, file_modes, git_env, git_timeout_seconds
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
