"""Cross-train lineage checks read committed migrations without executing them."""

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
CHECKER = REPO_ROOT / "scripts/check-alembic-revisions.py"
OTHER_TRAIN_REF = "other-train"


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    directory = tmp_path / "repo"
    directory.mkdir()
    _git(directory, "init", "--initial-branch=main")
    _git(directory, "config", "user.name", "Example Author")
    _git(directory, "config", "user.email", "author@example.com")
    return directory


def _write_revision(
    repo: Path,
    filename: str,
    revision: str,
    down_revision: str | tuple[str, ...] | None,
    *,
    annotated: bool = False,
    extra_source: str = "",
) -> Path:
    versions = repo / "apps/api/alembic/versions"
    versions.mkdir(parents=True, exist_ok=True)
    path = versions / filename
    revision_annotation = ": str" if annotated else ""
    parent_annotation = ": str | tuple[str, ...] | None" if annotated else ""
    path.write_text(
        f"revision{revision_annotation} = {revision!r}\n"
        f"down_revision{parent_annotation} = {down_revision!r}\n"
        "branch_labels = None\n"
        "depends_on = None\n"
        f"{extra_source}",
        encoding="utf-8",
    )
    return path


def _record_other_train(repo: Path) -> None:
    _git(repo, "add", "apps/api/alembic/versions")
    _git(repo, "commit", "-m", "Record example migration lineage")
    _git(repo, "branch", OTHER_TRAIN_REF)


def _run_gate(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(CHECKER), "--repo", str(repo), *args],
        cwd=repo.parent,
        check=False,
        capture_output=True,
        text=True,
    )


def _write_merge_graph(repo: Path, *, annotated: bool = False) -> None:
    _write_revision(repo, "0001_base.py", "base", None, annotated=annotated)
    _write_revision(repo, "0002_left.py", "left", "base", annotated=annotated)
    _write_revision(repo, "0003_right.py", "right", "base", annotated=annotated)
    _write_revision(
        repo, "0004_merge.py", "merged", ("left", "right"), annotated=annotated
    )


def test_shared_revision_filename_mismatch_below_head_fails(repo: Path) -> None:
    _write_revision(repo, "0001_base.py", "base", None)
    old_path = _write_revision(repo, "0002_shared.py", "shared", "base")
    _write_revision(repo, "0004_head.py", "same_head", "shared")
    _record_other_train(repo)
    old_path.rename(old_path.with_name("0003_renamed.py"))

    result = _run_gate(repo, "--other-train-ref", OTHER_TRAIN_REF)

    assert result.returncode == 1, result.stderr
    assert "lineage" in result.stderr.lower()
    assert OTHER_TRAIN_REF in result.stderr
    assert "shared" in result.stderr
    assert "0002_shared.py" in result.stderr
    assert "0003_renamed.py" in result.stderr
    assert result.stderr.count("base") >= 2


def test_shared_revision_parent_mismatch_names_both_lineages(repo: Path) -> None:
    _write_revision(repo, "0001_base.py", "base", None)
    _write_revision(repo, "0002_prior.py", "prior", "base")
    _write_revision(repo, "0003_shared.py", "shared", "prior")
    _record_other_train(repo)
    _write_revision(repo, "0003_shared.py", "shared", "base")
    _write_revision(repo, "0004_merge.py", "local_head", ("prior", "shared"))

    result = _run_gate(repo, "--other-train-ref", OTHER_TRAIN_REF)

    assert result.returncode == 1, result.stderr
    assert "lineage" in result.stderr.lower()
    assert OTHER_TRAIN_REF in result.stderr
    assert "shared" in result.stderr
    assert result.stderr.count("0003_shared.py") >= 2
    assert "base" in result.stderr
    assert "prior" in result.stderr


def test_extra_revisions_on_both_trains_do_not_block_shared_merge(repo: Path) -> None:
    _write_merge_graph(repo)
    remote_extra = _write_revision(repo, "0005_other.py", "other_extra", "merged")
    _record_other_train(repo)
    remote_extra.unlink()
    _write_revision(repo, "0006_local.py", "local_extra", "merged")

    result = _run_gate(repo, "--other-train-ref", OTHER_TRAIN_REF)

    assert result.returncode == 0, result.stderr
    assert "local_extra" in result.stdout


def test_reordered_merge_parent_tuple_has_the_same_lineage(repo: Path) -> None:
    _write_merge_graph(repo)
    _record_other_train(repo)
    _write_revision(repo, "0004_merge.py", "merged", ("right", "left"))

    result = _run_gate(repo, "--other-train-ref", OTHER_TRAIN_REF)

    assert result.returncode == 0, result.stderr
    assert "merged" in result.stdout


def test_annotated_module_assignments_are_compared(repo: Path) -> None:
    _write_merge_graph(repo, annotated=True)
    _record_other_train(repo)
    _write_revision(repo, "0004_merge.py", "merged", "left", annotated=True)
    _write_revision(repo, "0005_merge.py", "local_head", ("merged", "right"))

    result = _run_gate(repo, "--other-train-ref", OTHER_TRAIN_REF)

    assert result.returncode == 1, result.stderr
    assert "merged" in result.stderr
    assert result.stderr.count("0004_merge.py") >= 2
    assert "left" in result.stderr
    assert "right" in result.stderr


def test_remote_migration_module_is_not_executed(repo: Path) -> None:
    _write_revision(
        repo,
        "0001_base.py",
        "base",
        None,
        extra_source="raise RuntimeError('remote migration module was executed')\n",
    )
    _record_other_train(repo)
    _write_revision(repo, "0001_base.py", "base", None)

    result = _run_gate(repo, "--other-train-ref", OTHER_TRAIN_REF)

    assert result.returncode == 0, result.stderr
    assert "base" in result.stdout


def test_unavailable_other_train_ref_fails_with_ref_diagnostic(repo: Path) -> None:
    _write_revision(repo, "0001_base.py", "base", None)
    _record_other_train(repo)

    result = _run_gate(repo, "--other-train-ref", "missing-train")

    assert result.returncode == 1, result.stderr
    assert "missing-train" in result.stderr
    assert "lineage" in result.stderr.lower()


@pytest.mark.parametrize("use_repo", [False, True])
def test_without_other_train_flag_checks_only_the_local_graph(
    repo: Path, use_repo: bool
) -> None:
    _write_revision(repo, "0001_base.py", "base", None)
    old_path = _write_revision(repo, "0002_shared.py", "shared", "base")
    _record_other_train(repo)
    old_path.rename(old_path.with_name("0003_renamed.py"))
    args = (
        ["--repo", str(repo)]
        if use_repo
        else ["--script-location", str(repo / "apps/api/alembic")]
    )

    result = subprocess.run(
        [sys.executable, str(CHECKER), *args],
        cwd=repo.parent,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "shared" in result.stdout


def test_explicit_script_location_overrides_repo_default(repo: Path) -> None:
    _write_revision(repo, "0001_base.py", "base", None)
    _record_other_train(repo)
    separate_tree = repo.parent / "separate/alembic"
    (separate_tree / "versions").mkdir(parents=True)
    (separate_tree / "versions/0001_base.py").write_text(
        "revision = 'base'\ndown_revision = None\n", encoding="utf-8"
    )
    (repo / "apps/api/alembic/versions/0001_base.py").unlink()

    result = _run_gate(
        repo,
        "--script-location",
        str(separate_tree),
        "--other-train-ref",
        OTHER_TRAIN_REF,
    )

    assert result.returncode == 0, result.stderr
    assert "base" in result.stdout
