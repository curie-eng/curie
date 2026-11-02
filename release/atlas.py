#!/usr/bin/env python3
"""Require a tag-pinned architecture-atlas snapshot before release builds start."""

import argparse
import json
import subprocess
import sys
from pathlib import Path


class AtlasError(Exception):
    """The release has no trustworthy architecture-atlas snapshot."""


VERSION_ONLY_PATHS = frozenset(
    {
        "charts/curie/Chart.yaml",
        "cli/Cargo.lock",
        "cli/Cargo.toml",
        "docs/architecture-atlas/versions.json",
    }
)


# The schema-window catalog `curie dev bump-version` edits in the same release
# preparation commit. Registering the release's own window there is version
# identity; any other change to it is not (ADR-0247, ADR-0257).
SCHEMA_WINDOWS_PATH = "cli/src/application_schema_windows.json"


def version_only_paths(version: str) -> frozenset[str]:
    """Paths a release bump for `version` may change without changing behavior.

    The shared set plus that release's own atlas snapshot. The CI selector and
    the release tag gate both read this one definition (#3858).
    """
    return VERSION_ONLY_PATHS | {f"docs/architecture-atlas/snapshots/{version}.json"}


def _schema_windows_at(commit: str, *, cwd: Path | None) -> object:
    """The parsed schema-window catalog at `commit`, or None if unreadable.

    A missing file, an unknown commit or malformed JSON is not an error here:
    it only means the delta cannot be a registration, so callers refuse on
    their own terms. A document with a duplicate object key at any level is
    treated the same way: json.loads would silently keep the last member, but
    the Rust consumer rejects such documents.
    """
    try:
        result = subprocess.run(
            ["git", "show", f"{commit}:{SCHEMA_WINDOWS_PATH}"],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise AtlasError(f"could not read {SCHEMA_WINDOWS_PATH} at {commit}: {exc}") from exc
    if result.returncode != 0:
        return None
    try:
        return json.loads(result.stdout, object_pairs_hook=_reject_duplicate_keys)
    except (json.JSONDecodeError, ValueError):
        return None


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate key {key!r}")
        result[key] = value
    return result


def _canonical(value: object) -> str:
    """Type-sensitive form of parsed JSON: True and 1 and 1.0 stay distinct."""
    return json.dumps(value, sort_keys=True)


def is_schema_window_registration(
    base: str, head: str, window_key: str, *, cwd: Path | None = None
) -> bool:
    """Is the catalog change from `base` to `head` only registering `window_key`?

    True only when both documents are objects with an object `windows`,
    `window_key` is a new key in `windows` whose value equals `base`'s
    `candidate` (what bump-version promotes), and every other top-level key
    and every other window is unchanged (ADR-0247, issue #4862).
    """
    old = _schema_windows_at(base, cwd=cwd)
    new = _schema_windows_at(head, cwd=cwd)
    if not isinstance(old, dict) or not isinstance(new, dict):
        return False
    old_windows = old.get("windows")
    new_windows = new.get("windows")
    if not isinstance(old_windows, dict) or not isinstance(new_windows, dict):
        return False
    if window_key in old_windows or window_key not in new_windows:
        return False
    if "candidate" not in old or _canonical(new_windows[window_key]) != _canonical(
        old["candidate"]
    ):
        return False
    if _canonical({k: v for k, v in new_windows.items() if k != window_key}) != (
        _canonical(old_windows)
    ):
        return False
    return _canonical({k: v for k, v in new.items() if k != "windows"}) == (
        _canonical({k: v for k, v in old.items() if k != "windows"})
    )


def version_only_extra(
    changed_paths: list[str] | set[str],
    version: str,
    base: str,
    head: str,
    *,
    cwd: Path | None = None,
) -> list[str]:
    """Paths in the `base`..`head` delta that are not version identity for `version`.

    `version` is the release tag with its leading `v`. A path in
    `version_only_paths(version)` is version identity, and so is
    SCHEMA_WINDOWS_PATH when its only change registers this release's own
    window as the base's candidate (ADR-0257). The release tag gate, the
    authorizer's proof walk and the CI selector all decide with this one
    function, so a release preparation commit made by `curie dev
    bump-version` is version-only everywhere and nothing has to name a
    release before it is cut. An empty result means version-only.
    """
    extra = sorted(set(changed_paths) - version_only_paths(version))
    if extra == [SCHEMA_WINDOWS_PATH] and is_schema_window_registration(
        base, head, version.removeprefix("v"), cwd=cwd
    ):
        return []
    return extra


def require_release_snapshot(
    atlas_dir: Path,
    version: str,
    commit: str,
    *,
    changed_paths: list[str] | None = None,
    repo_root: Path | None = None,
) -> None:
    """Require `version` to resolve to a snapshot pinned to `commit`.

    `changed_paths` is the delta from the pinned commit to `commit`; it may
    carry only version identity, read in `repo_root` (ADR-0257).
    """
    atlas_dir = atlas_dir.resolve()
    manifest_path = atlas_dir / "versions.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AtlasError(f"cannot read architecture atlas manifest {manifest_path}: {exc}") from exc

    matches = [entry for entry in manifest.get("versions", []) if entry.get("id") == version]
    if len(matches) != 1:
        raise AtlasError(
            f"release {version} must have exactly one registered architecture atlas "
            f"snapshot in {manifest_path}; found {len(matches)}. Generate it from the "
            "release commit before tagging."
        )

    entry = matches[0]
    snapshot_commit = entry.get("commit")
    if snapshot_commit != commit:
        if changed_paths is None:
            raise AtlasError(
                f"architecture atlas {version} names commit {snapshot_commit!r}, not tag "
                f"commit {commit}, and no release-delta evidence was supplied"
            )
        disallowed = version_only_extra(
            changed_paths, version, snapshot_commit, commit, cwd=repo_root
        )
        if disallowed:
            raise AtlasError(
                "architecture changed after the release snapshot pin: "
                f"{', '.join(disallowed)}. Regenerate {version} from the final "
                "architecture commit before tagging."
            )

    relative_snapshot = entry.get("file")
    if not isinstance(relative_snapshot, str):
        raise AtlasError(f"architecture atlas {version} has no snapshot file")
    snapshot_path = (atlas_dir / relative_snapshot).resolve()
    if atlas_dir not in snapshot_path.parents:
        raise AtlasError(
            f"architecture atlas snapshot path escapes its directory: {relative_snapshot}"
        )

    try:
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AtlasError(f"cannot read architecture atlas snapshot {snapshot_path}: {exc}") from exc

    repository = snapshot.get("repository")
    if not isinstance(repository, dict):
        raise AtlasError(f"architecture atlas {version} snapshot has no repository metadata")
    if repository.get("release") != version:
        raise AtlasError(
            f"architecture atlas {version} snapshot repository release is "
            f"{repository.get('release')!r}"
        )
    if repository.get("commit") != snapshot_commit:
        raise AtlasError(
            f"architecture atlas {version} snapshot repository commit is "
            f"{repository.get('commit')!r}, not manifest commit {snapshot_commit}"
        )


def registered_commit(atlas_dir: Path, version: str) -> str:
    """Return the commit registered for `version`, failing closed on bad input."""
    manifest_path = atlas_dir.resolve() / "versions.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AtlasError(f"cannot read architecture atlas manifest {manifest_path}: {exc}") from exc
    matches = [entry for entry in manifest.get("versions", []) if entry.get("id") == version]
    if len(matches) != 1 or not isinstance(matches[0].get("commit"), str):
        raise AtlasError(f"release {version} has no unique registered architecture commit")
    commit = matches[0]["commit"]
    assert isinstance(commit, str)
    return commit


def release_delta(repo_root: Path, snapshot_commit: str, tag_commit: str) -> list[str]:
    """Return the paths after the pin, requiring the pin to be tag ancestry."""
    ancestor = subprocess.run(
        ["git", "merge-base", "--is-ancestor", snapshot_commit, tag_commit],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    if ancestor.returncode != 0:
        raise AtlasError(
            f"architecture atlas commit {snapshot_commit} is not an ancestor of tag commit "
            f"{tag_commit}"
        )
    diff = subprocess.run(
        ["git", "diff", "--name-only", f"{snapshot_commit}..{tag_commit}"],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    if diff.returncode != 0:
        raise AtlasError(f"cannot inspect release delta: {diff.stderr.strip()}")
    return [line for line in diff.stdout.splitlines() if line]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--atlas-dir", type=Path, default=Path("docs/architecture-atlas")
    )
    parser.add_argument("--version", required=True, help="release tag, including leading v")
    parser.add_argument("--commit", required=True, help="full commit named by the release tag")
    args = parser.parse_args(argv)

    try:
        snapshot_commit = registered_commit(args.atlas_dir, args.version)
        changed_paths = None
        repo_root = args.atlas_dir.resolve().parents[1]
        if snapshot_commit != args.commit:
            changed_paths = release_delta(repo_root, snapshot_commit, args.commit)
        require_release_snapshot(
            args.atlas_dir,
            args.version,
            args.commit,
            changed_paths=changed_paths,
            repo_root=repo_root,
        )
    except AtlasError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"OK: architecture atlas {args.version} is pinned to {args.commit}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
