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


def version_only_paths(version: str) -> frozenset[str]:
    """Paths a release bump for `version` may change without changing behavior.

    The shared set plus that release's own atlas snapshot. The CI selector and
    the release tag gate both read this one definition (#3858).
    """
    return VERSION_ONLY_PATHS | {f"docs/architecture-atlas/snapshots/{version}.json"}


def require_release_snapshot(
    atlas_dir: Path,
    version: str,
    commit: str,
    *,
    changed_paths: list[str] | None = None,
) -> None:
    """Require `version` to resolve to a snapshot pinned to `commit`."""
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
        disallowed = sorted(set(changed_paths) - version_only_paths(version))
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
        if snapshot_commit != args.commit:
            repo_root = args.atlas_dir.resolve().parents[1]
            changed_paths = release_delta(repo_root, snapshot_commit, args.commit)
        require_release_snapshot(
            args.atlas_dir,
            args.version,
            args.commit,
            changed_paths=changed_paths,
        )
    except AtlasError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"OK: architecture atlas {args.version} is pinned to {args.commit}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
