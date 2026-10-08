import json
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import yaml
from _migration_support import run_script
from alembic.script import ScriptDirectory

REPO_ROOT = Path(__file__).resolve().parents[3]
CHECKER = REPO_ROOT / "scripts" / "check-alembic-revisions.py"
ALEMBIC_TREE = REPO_ROOT / "apps" / "api" / "alembic"
CHECK_COMMAND = "uv run python scripts/check-alembic-revisions.py"


def _write_revision(
    script_location: Path,
    filename: str,
    revision: str,
    down_revision: str | tuple[str, ...] | None,
) -> None:
    versions = script_location / "versions"
    versions.mkdir(parents=True, exist_ok=True)
    (versions / filename).write_text(
        "\n".join(
            [
                f"revision = {revision!r}",
                f"down_revision = {down_revision!r}",
                "branch_labels = None",
                "depends_on = None",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _run_gate(script_location: Path | None = None) -> subprocess.CompletedProcess[str]:
    args = [] if script_location is None else ["--script-location", str(script_location)]
    return run_script(CHECKER, *args)


def test_real_migration_tree_has_one_reported_head() -> None:
    """The one real CLI run; every other case drives the gate in-process."""
    expected_head = ScriptDirectory(str(ALEMBIC_TREE)).get_current_head()

    result = subprocess.run(
        [sys.executable, str(CHECKER)],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert expected_head is not None
    assert result.returncode == 0, result.stderr
    assert expected_head in result.stdout


def test_feature_chain_can_join_stable_execution_request_revisions(tmp_path: Path) -> None:
    """Stable and feature migrations must retain distinct identities at the join."""
    # Stable graph pinned at public commit 15ebb08a7. These declarations model
    # graph identity only; the migration tests separately exercise real DDL.
    tree = tmp_path / "alembic"
    shutil.copytree(ALEMBIC_TREE, tree, ignore=shutil.ignore_patterns("__pycache__"))
    _write_revision(tree, "0091_execution_request_owner_lost_retry.py", "0091", "0081")
    _write_revision(tree, "0092_execution_request_start_deferrals.py", "0092", "0091")
    _write_revision(tree, "0093_factory_status_sync_lease.py", "0093", "0092")
    # Do not load the feature graph before the checker: a reversed rename can
    # leave a missing parent, and the gate must diagnose collisions first.
    feature_head = json.loads(
        (REPO_ROOT / "cli/src/application_schema_windows.json").read_text()
    )["candidate"]["schema_head"]
    _write_revision(tree, "0098a_probe_merge.py", "0098a", ("0093", feature_head))

    result = _run_gate(tree)

    assert result.returncode == 0, result.stderr
    assert "head 0098a" in result.stdout


def test_duplicate_numeric_filename_prefix_fails_before_graph_validation(
    tmp_path: Path,
) -> None:
    _write_revision(tmp_path, "0001_base.py", "base", None)
    _write_revision(tmp_path, "0001_collision.py", "different_revision", "base")

    result = _run_gate(tmp_path)

    assert result.returncode == 1, result.stderr
    assert "duplicate numeric revision" in result.stderr.lower()
    assert "0001" in result.stderr
    assert "0001_base.py" in result.stderr
    assert "0001_collision.py" in result.stderr


def test_unrecognized_migration_filename_fails_before_graph_validation(
    tmp_path: Path,
) -> None:
    _write_revision(tmp_path, "custom_base.py", "self_cycle", "self_cycle")

    result = _run_gate(tmp_path)

    assert result.returncode == 1, result.stderr
    assert "unrecognized migration filename" in result.stderr.lower()
    assert "custom_base.py" in result.stderr
    assert "expected exactly one alembic head" not in result.stderr.lower()


def test_non_migration_files_are_ignored_when_valid_migration_passes(
    tmp_path: Path,
) -> None:
    _write_revision(tmp_path, "0001_base.py", "base", None)
    versions = tmp_path / "versions"
    (versions / "__init__.py").write_text("", encoding="utf-8")
    (versions / "migration-notes.txt").write_text("notes", encoding="utf-8")

    result = _run_gate(tmp_path)

    assert result.returncode == 0, result.stderr
    assert "base" in result.stdout


def test_one_letter_suffixed_revision_in_valid_chain_passes(tmp_path: Path) -> None:
    _write_revision(tmp_path, "0001_base.py", "base", None)
    _write_revision(tmp_path, "0001a_reconcile.py", "reconciled", "base")

    result = _run_gate(tmp_path)

    assert result.returncode == 0, result.stderr
    assert "reconciled" in result.stdout


def test_duplicate_identical_suffixed_revision_tokens_fail(tmp_path: Path) -> None:
    _write_revision(tmp_path, "0001_base.py", "base", None)
    _write_revision(tmp_path, "0001a_first.py", "first", "base")
    _write_revision(tmp_path, "0001a_second.py", "second", "base")

    result = _run_gate(tmp_path)

    assert result.returncode == 1, result.stderr
    assert "duplicate numeric revision" in result.stderr.lower()
    assert "0001a" in result.stderr
    assert "0001a_first.py" in result.stderr
    assert "0001a_second.py" in result.stderr


def test_different_suffixed_revision_tokens_are_distinct(tmp_path: Path) -> None:
    _write_revision(tmp_path, "0001_base.py", "base", None)
    _write_revision(tmp_path, "0001a_reconcile.py", "reconciled", "base")
    _write_revision(tmp_path, "0001b_followup.py", "followup", "reconciled")

    result = _run_gate(tmp_path)

    assert result.returncode == 0, result.stderr
    assert "followup" in result.stdout


def test_multi_letter_suffixed_revision_filename_is_rejected(tmp_path: Path) -> None:
    _write_revision(tmp_path, "0001ab_bad.py", "bad", None)

    result = _run_gate(tmp_path)

    assert result.returncode == 1, result.stderr
    assert "unrecognized migration filename" in result.stderr.lower()
    assert "0001ab_bad.py" in result.stderr


def test_duplicate_revision_ids_fail_and_name_the_colliding_files(
    tmp_path: Path,
) -> None:
    _write_revision(tmp_path, "0001_base.py", "base", None)
    _write_revision(tmp_path, "0002_first.py", "collision", "base")
    _write_revision(tmp_path, "0003_second.py", "collision", "base")

    result = _run_gate(tmp_path)

    assert result.returncode == 1, result.stderr
    assert "duplicate revision id" in result.stderr.lower()
    assert "collision" in result.stderr
    assert "0002_first.py" in result.stderr
    assert "0003_second.py" in result.stderr


def test_duplicate_revision_ids_fail_before_graph_validation(tmp_path: Path) -> None:
    _write_revision(tmp_path, "0001_base.py", "base", None)
    _write_revision(tmp_path, "0002_first.py", "collision", "base")
    _write_revision(tmp_path, "0003_second.py", "collision", "base")

    result = _run_gate(tmp_path)

    assert result.returncode == 1, result.stderr
    assert "expected exactly one alembic head" not in result.stderr.lower()


def test_unique_numbered_sibling_revisions_fail_with_both_heads(tmp_path: Path) -> None:
    _write_revision(tmp_path, "0001_base.py", "base", None)
    _write_revision(tmp_path, "0002_left.py", "left_branch", "base")
    _write_revision(tmp_path, "0003_right.py", "right_branch", "base")

    result = _run_gate(tmp_path)

    assert result.returncode == 1, result.stderr
    assert "expected exactly one alembic head" in result.stderr.lower()
    assert "left_branch" in result.stderr
    assert "right_branch" in result.stderr


def test_merge_revision_restores_one_head(tmp_path: Path) -> None:
    _write_revision(tmp_path, "0001_base.py", "base", None)
    _write_revision(tmp_path, "0002_left.py", "left_branch", "base")
    _write_revision(tmp_path, "0003_right.py", "right_branch", "base")
    _write_revision(
        tmp_path,
        "0004_merge.py",
        "merged_head",
        ("left_branch", "right_branch"),
    )

    result = _run_gate(tmp_path)

    assert result.returncode == 0, result.stderr
    assert "merged_head" in result.stdout


def test_python_ci_job_runs_exact_gate_before_dev_stack() -> None:
    workflow = yaml.safe_load((REPO_ROOT / ".github/workflows/ci.yaml").read_text())
    steps = workflow["jobs"]["python"]["steps"]

    matching_steps = [step for step in steps if step.get("run") == CHECK_COMMAND]
    assert len(matching_steps) == 1
    assert matching_steps[0]["name"] == "Alembic revision gate"

    # The dev stack boots only in the pytest shards, so the gate never waits on
    # one in the required Python job.
    assert not any(step.get("name") == "Start dev stack" for step in steps)


def test_python_ci_runs_released_upgrade_after_stack_ready_and_before_fresh_install(
) -> None:
    workflow = yaml.safe_load((REPO_ROOT / ".github/workflows/ci.yaml").read_text())
    # Every pytest shard boots its own stack and runs the gate against it.
    steps = workflow["jobs"]["python-pytest"]["steps"]

    checkout_steps = [
        step for step in steps if step.get("uses") == "actions/checkout@v7"
    ]
    assert len(checkout_steps) == 1
    assert checkout_steps[0]["with"]["fetch-depth"] == 0

    released_upgrade_steps = [
        step for step in steps if step.get("name") == "Released database upgrade gate"
    ]
    assert len(released_upgrade_steps) == 1
    released_upgrade_step = released_upgrade_steps[0]
    released_upgrade_commands = [
        line.strip()
        for line in released_upgrade_step["run"].splitlines()
        if line.strip().startswith("uv run python scripts/check-released-upgrade.py")
    ]
    assert released_upgrade_commands == [
        "uv run python scripts/check-released-upgrade.py --self-test",
        "uv run python scripts/check-released-upgrade.py",
    ]

    stack_index = next(
        index for index, step in enumerate(steps) if step.get("name") == "Start dev stack"
    )
    readiness_index = next(
        index
        for index, step in enumerate(steps)
        if step.get("name") == "Wait for Langfuse to serve"
    )
    released_upgrade_index = steps.index(released_upgrade_step)
    fresh_install_steps = [
        step for step in steps if step.get("name") == "Migrate the shared database"
    ]
    assert len(fresh_install_steps) == 1
    assert fresh_install_steps[0]["run"] == "uv run alembic upgrade head"
    fresh_install_index = steps.index(fresh_install_steps[0])

    assert stack_index < readiness_index < released_upgrade_index < fresh_install_index


def test_baseline_fetch_selects_the_branch_when_a_tag_has_the_same_name(tmp_path: Path) -> None:
    """Execute the documented fetch against conflicting local branch and tag refs."""
    def git(directory: Path, *arguments: str) -> str:
        result = subprocess.run(
            ["git", "-c", "user.name=Test", "-c", "user.email=test@example.com", *arguments],
            cwd=directory,
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    remote, seed, consumer = (tmp_path / name for name in ("remote", "seed", "consumer"))
    for directory in (remote, seed, consumer):
        directory.mkdir()
    git(remote, "init", "--bare")
    git(seed, "init", "--initial-branch=main")
    git(seed, "commit", "--allow-empty", "-m", "tag target")
    tag_sha = git(seed, "rev-parse", "HEAD")
    git(seed, "tag", "main")
    git(seed, "commit", "--allow-empty", "-m", "branch target")
    branch_sha = git(seed, "rev-parse", "HEAD")
    git(seed, "push", str(remote), "refs/heads/main", "refs/tags/main")
    git(consumer, "init")
    git(consumer, "remote", "add", "origin", str(remote))
    command = next(
        line.strip()
        for line in (REPO_ROOT / "AGENTS.md").read_text(encoding="utf-8").splitlines()
        if line.strip().startswith("git fetch") and "refs/remotes/origin/main" in line
    )
    git(consumer, *shlex.split(command)[1:])
    fetched = git(consumer, "rev-parse", "refs/remotes/origin/main")
    assert fetched == branch_sha
    assert fetched != tag_sha
