"""Contract tests for the release architecture-atlas gate."""

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "release" / "atlas.py"
RELEASE_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "release.yaml"


def load_module():
    spec = importlib.util.spec_from_file_location("release_atlas", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_atlas(tmp_path: Path, *, version: str = "v1.2.3", commit: str = "a" * 40) -> Path:
    atlas_dir = tmp_path / "docs" / "architecture-atlas"
    snapshots = atlas_dir / "snapshots"
    snapshots.mkdir(parents=True)
    (atlas_dir / "versions.json").write_text(
        json.dumps(
            {
                "schemaVersion": "1.0",
                "defaultVersion": version,
                "versions": [
                    {
                        "id": version,
                        "label": version,
                        "file": f"snapshots/{version}.json",
                        "date": "2026-08-28",
                        "branch": "main",
                        "commit": commit,
                    }
                ],
            }
        )
    )
    (snapshots / f"{version}.json").write_text(
        json.dumps({"repository": {"release": version, "commit": commit}})
    )
    return atlas_dir


def test_accepts_a_registered_snapshot_pinned_to_the_release_commit(tmp_path):
    module = load_module()
    atlas_dir = write_atlas(tmp_path)

    module.require_release_snapshot(atlas_dir, "v1.2.3", "a" * 40)


def test_refuses_a_release_with_no_registered_snapshot(tmp_path):
    module = load_module()
    atlas_dir = write_atlas(tmp_path)

    with pytest.raises(module.AtlasError, match="v1.2.4"):
        module.require_release_snapshot(atlas_dir, "v1.2.4", "a" * 40)


def test_accepts_a_snapshot_pinned_before_version_only_release_changes(tmp_path):
    module = load_module()
    atlas_dir = write_atlas(tmp_path, commit="b" * 40)

    module.require_release_snapshot(
        atlas_dir,
        "v1.2.3",
        "a" * 40,
        changed_paths=[
            "docs/architecture-atlas/versions.json",
            "docs/architecture-atlas/snapshots/v1.2.3.json",
            "cli/Cargo.toml",
        ],
    )


def test_refuses_runtime_changes_after_the_snapshot_pin(tmp_path):
    module = load_module()
    atlas_dir = write_atlas(tmp_path, commit="b" * 40)

    with pytest.raises(module.AtlasError, match="apps/api"):
        module.require_release_snapshot(
            atlas_dir,
            "v1.2.3",
            "a" * 40,
            changed_paths=["apps/api/src/curie_api/main.py"],
        )


def test_version_only_paths_are_the_shared_set_plus_that_releases_snapshot():
    """#3858: the selector and the tag gate share this one definition."""
    module = load_module()

    assert module.version_only_paths("v0.11.2") == frozenset(
        {
            "charts/curie/Chart.yaml",
            "cli/Cargo.lock",
            "cli/Cargo.toml",
            "docs/architecture-atlas/versions.json",
            "docs/architecture-atlas/snapshots/v0.11.2.json",
        }
    )
    assert module.version_only_paths("v0.11.2") == module.VERSION_ONLY_PATHS | {
        "docs/architecture-atlas/snapshots/v0.11.2.json"
    }
    assert "docs/architecture-atlas/snapshots/v0.11.2.json" not in module.version_only_paths(
        "v0.12.0-rc.1"
    )


def test_refuses_another_releases_snapshot_after_the_pin(tmp_path):
    """#3858: only the release's own snapshot is version-only, not any snapshot."""
    module = load_module()
    atlas_dir = write_atlas(tmp_path, commit="b" * 40)

    with pytest.raises(module.AtlasError, match="v9.9.9"):
        module.require_release_snapshot(
            atlas_dir,
            "v1.2.3",
            "a" * 40,
            changed_paths=[
                "cli/Cargo.toml",
                "docs/architecture-atlas/snapshots/v9.9.9.json",
            ],
        )


def test_refuses_a_snapshot_whose_metadata_does_not_match_the_manifest(tmp_path):
    module = load_module()
    atlas_dir = write_atlas(tmp_path)
    snapshot = atlas_dir / "snapshots" / "v1.2.3.json"
    snapshot.write_text(json.dumps({"repository": {"release": "v1.2.3", "commit": "b" * 40}}))

    with pytest.raises(module.AtlasError, match="snapshot repository commit"):
        module.require_release_snapshot(atlas_dir, "v1.2.3", "a" * 40)


def test_refuses_a_snapshot_path_that_escapes_the_atlas(tmp_path):
    module = load_module()
    atlas_dir = write_atlas(tmp_path)
    manifest = json.loads((atlas_dir / "versions.json").read_text())
    manifest["versions"][0]["file"] = "../../outside.json"
    (atlas_dir / "versions.json").write_text(json.dumps(manifest))

    with pytest.raises(module.AtlasError, match="escapes"):
        module.require_release_snapshot(atlas_dir, "v1.2.3", "a" * 40)


def test_cli_fails_loud_when_the_release_snapshot_is_missing(tmp_path):
    atlas_dir = write_atlas(tmp_path)

    done = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--atlas-dir",
            str(atlas_dir),
            "--version",
            "v1.2.4",
            "--commit",
            "a" * 40,
        ],
        capture_output=True,
        text=True,
    )

    assert done.returncode == 1
    assert "v1.2.4" in done.stderr


def run_git(repo: Path, *args: str) -> str:
    done = subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True, check=True)
    return done.stdout.strip()


@pytest.mark.parametrize(
    ("late_path", "expected_code"),
    [("cli/Cargo.toml", 0), ("apps/api/src/curie_api/main.py", 1)],
)
def test_cli_checks_the_real_git_delta_after_the_snapshot_pin(tmp_path, late_path, expected_code):
    repo = tmp_path / "repo"
    repo.mkdir()
    run_git(repo, "init", "-q", "-b", "main")
    run_git(repo, "config", "user.email", "test@example.com")
    run_git(repo, "config", "user.name", "Test")
    (repo / "architecture.txt").write_text("final architecture")
    run_git(repo, "add", "architecture.txt")
    run_git(repo, "commit", "-qm", "finish architecture")
    snapshot_commit = run_git(repo, "rev-parse", "HEAD")

    atlas_dir = write_atlas(repo, commit=snapshot_commit)
    changed = repo / late_path
    changed.parent.mkdir(parents=True, exist_ok=True)
    changed.write_text("release change")
    run_git(repo, "add", ".")
    run_git(repo, "commit", "-qm", "prepare release")
    tag_commit = run_git(repo, "rev-parse", "HEAD")

    done = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--atlas-dir",
            str(atlas_dir),
            "--version",
            "v1.2.3",
            "--commit",
            tag_commit,
        ],
        capture_output=True,
        text=True,
    )

    assert done.returncode == expected_code, done.stdout + done.stderr
    if expected_code == 1:
        assert late_path in done.stderr


@pytest.mark.parametrize("late_behavior_change", [False, True])
def test_cli_checks_combined_preparation_pr_merge(tmp_path, late_behavior_change):
    """A pin on the preparation branch includes final fixes before the bump."""
    repo = tmp_path / "repo"
    repo.mkdir()
    run_git(repo, "init", "-q", "-b", "main")
    run_git(repo, "config", "user.email", "test@example.com")
    run_git(repo, "config", "user.name", "Test")
    behavior_path = "apps/api/src/curie_api/main.py"
    behavior_file = repo / behavior_path
    behavior_file.parent.mkdir(parents=True)
    behavior_file.write_text("original behavior")
    run_git(repo, "add", ".")
    run_git(repo, "commit", "-qm", "initial behavior")
    main_commit = run_git(repo, "rev-parse", "HEAD")

    run_git(repo, "checkout", "-qb", "task/prepare-release")
    behavior_file.write_text("final fix")
    run_git(repo, "add", behavior_path)
    run_git(repo, "commit", "-qm", "finish release fix")
    snapshot_commit = run_git(repo, "rev-parse", "HEAD")
    atlas_dir = write_atlas(repo, commit=snapshot_commit)
    manifest = repo / "cli" / "Cargo.toml"
    manifest.parent.mkdir()
    manifest.write_text('[package]\nversion = "1.2.3"\n')
    run_git(repo, "add", ".")
    run_git(repo, "commit", "-qm", "prepare release version")
    if late_behavior_change:
        behavior_file.write_text("unrecorded late behavior")
        run_git(repo, "add", behavior_path)
        run_git(repo, "commit", "-qm", "change behavior after snapshot")
    preparation_head = run_git(repo, "rev-parse", "HEAD")

    run_git(repo, "checkout", "-q", "main")
    run_git(repo, "merge", "--no-ff", "-qm", "merge preparation", preparation_head)
    merge_commit = run_git(repo, "rev-parse", "HEAD")
    assert run_git(repo, "rev-parse", "HEAD^1") == main_commit
    assert run_git(repo, "rev-parse", "HEAD^2") == preparation_head

    done = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--atlas-dir",
            str(atlas_dir),
            "--version",
            "v1.2.3",
            "--commit",
            merge_commit,
        ],
        capture_output=True,
        text=True,
    )

    assert done.returncode == (1 if late_behavior_change else 0), done.stdout + done.stderr
    if late_behavior_change:
        assert behavior_path in done.stderr
    else:
        assert merge_commit in done.stdout


def test_authorize_release_runs_the_atlas_gate_before_builds():
    workflow = yaml.safe_load(RELEASE_WORKFLOW.read_text())
    steps = workflow["jobs"]["authorize-release"]["steps"]
    commands = [step.get("run", "") for step in steps]

    assert any("release/atlas.py" in command for command in commands)
