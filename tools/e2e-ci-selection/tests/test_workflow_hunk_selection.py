"""Git-backed coverage for released-upgrade workflow hunk selection."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SELECTOR = REPO_ROOT / "tools" / "e2e-ci-selection" / "select_tiers.py"
REGISTRY = REPO_ROOT / ".github" / "e2e-selection.yaml"
WORKFLOW_PATH = Path(".github/workflows/ci.yaml")
TARGETS = (
    "e2e-released-upgrade",
    "e2e-released-upgrade-negative",
    "e2e-cluster-upgrade-matrix-shards",
    "e2e-cluster-upgrade-matrix",
)
JOBS = ("before", *TARGETS, "after")


def _selector() -> ModuleType:
    spec = importlib.util.spec_from_file_location("select_tiers_workflow_test", SELECTOR)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


SELECTOR_MODULE = _selector()


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd=repo, capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def _workflow(jobs: tuple[str, ...] = JOBS) -> str:
    return "name: Example CI\njobs:\n" + "".join(
        f"  {job}:\n    runs-on: ubuntu-latest\n    steps:\n"
        f"      - run: echo {job}\n"
        for job in jobs
    )


@pytest.fixture
def git_repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Example Tester")
    _git(repo, "config", "user.email", "tester@example.com")
    path = repo / WORKFLOW_PATH
    path.parent.mkdir(parents=True)
    path.write_text(_workflow(), encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "base")
    monkeypatch.chdir(repo)
    return repo, _git(repo, "rev-parse", "HEAD")


def _commit_workflow(repo: Path, content: str) -> str:
    (repo / WORKFLOW_PATH).write_text(content, encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "candidate")
    return _git(repo, "rev-parse", "HEAD")


def _invoke_selector(
    repo: Path, tmp_path: Path, *options: str
) -> tuple[subprocess.CompletedProcess[str], dict[str, str]]:
    output_path = tmp_path / "selector-output"
    result = subprocess.run(
        [sys.executable, str(SELECTOR), "--registry", str(REGISTRY), *options],
        cwd=repo,
        env={**os.environ, "GITHUB_OUTPUT": str(output_path)},
        capture_output=True,
        text=True,
        check=False,
    )
    output = dict(
        line.split("=", 1)
        for line in output_path.read_text(encoding="utf-8").splitlines()
    ) if output_path.exists() else {}
    return result, output


@pytest.mark.parametrize("target", TARGETS)
def test_each_upgrade_job_edit_selects_full_upgrade(
    git_repo: tuple[Path, str], target: str
) -> None:
    repo, base = git_repo
    head = _commit_workflow(repo, _workflow().replace(f"echo {target}", "echo changed"))

    assert SELECTOR_MODULE._changes_upgrade_workflow_jobs(base, head) is True


def test_old_side_deletion_selects_upgrade(git_repo: tuple[Path, str]) -> None:
    repo, base = git_repo
    head = _commit_workflow(
        repo, _workflow().replace("      - run: echo e2e-released-upgrade\n", "")
    )

    assert SELECTOR_MODULE._changes_upgrade_workflow_jobs(base, head) is True


def test_deleted_upgrade_job_selects_upgrade(git_repo: tuple[Path, str]) -> None:
    repo, base = git_repo
    head = _commit_workflow(repo, _workflow(tuple(j for j in JOBS if j != TARGETS[0])))

    assert SELECTOR_MODULE._changes_upgrade_workflow_jobs(base, head) is True


@pytest.mark.parametrize("neighbor", ("before", "after"))
def test_adjacent_non_upgrade_job_edit_does_not_select(
    git_repo: tuple[Path, str], neighbor: str
) -> None:
    repo, base = git_repo
    head = _commit_workflow(repo, _workflow().replace(f"echo {neighbor}", "echo changed"))

    assert SELECTOR_MODULE._changes_upgrade_workflow_jobs(base, head) is False


def test_insertion_before_first_upgrade_header_is_not_inside_it(
    git_repo: tuple[Path, str],
) -> None:
    repo, base = git_repo
    head = _commit_workflow(
        repo, _workflow().replace(
            "  e2e-released-upgrade:\n", "  # before upgrade header\n  e2e-released-upgrade:\n", 1
        )
    )

    assert SELECTOR_MODULE._changes_upgrade_workflow_jobs(base, head) is False


def test_insertion_after_upgrade_header_selects_upgrade(
    git_repo: tuple[Path, str],
) -> None:
    repo, base = git_repo
    head = _commit_workflow(
        repo, _workflow().replace(
            "  e2e-released-upgrade:\n",
            "  e2e-released-upgrade:\n    # inside upgrade job\n",
            1,
        )
    )

    assert SELECTOR_MODULE._changes_upgrade_workflow_jobs(base, head) is True


def test_insertion_before_next_job_header_belongs_to_preceding_upgrade(
    git_repo: tuple[Path, str],
) -> None:
    repo, base = git_repo
    head = _commit_workflow(
        repo, _workflow().replace("  after:\n", "  # before next header\n  after:\n", 1)
    )

    assert SELECTOR_MODULE._changes_upgrade_workflow_jobs(base, head) is True


def test_insertion_after_next_job_header_does_not_select_upgrade(
    git_repo: tuple[Path, str],
) -> None:
    repo, base = git_repo
    head = _commit_workflow(
        repo, _workflow().replace("  after:\n", "  after:\n    # inside after\n", 1)
    )

    assert SELECTOR_MODULE._changes_upgrade_workflow_jobs(base, head) is False


@pytest.mark.parametrize(
    "changed",
    (
        "name: [invalid\njobs:\n",
        _workflow() + "  e2e-released-upgrade:\n    runs-on: ubuntu-latest\n",
        _workflow(tuple(j for j in JOBS if j != TARGETS[0])).replace(
            "jobs:\n", "other:\n  e2e-released-upgrade:\n    runs-on: ubuntu-latest\njobs:\n"
        ),
    ),
)
def test_malformed_duplicate_or_moved_anchor_fails_closed(
    git_repo: tuple[Path, str], changed: str
) -> None:
    repo, base = git_repo
    head = _commit_workflow(repo, changed)

    with pytest.raises(SELECTOR_MODULE.RegistryError):
        SELECTOR_MODULE._changes_upgrade_workflow_jobs(base, head)


def test_renamed_upgrade_job_still_selects_old_side(
    git_repo: tuple[Path, str],
) -> None:
    repo, base = git_repo
    head = _commit_workflow(
        repo, _workflow().replace("  e2e-released-upgrade:\n", "  renamed-upgrade:\n", 1)
    )

    assert SELECTOR_MODULE._changes_upgrade_workflow_jobs(base, head) is True


def test_workflow_change_without_text_hunk_fails_closed(
    git_repo: tuple[Path, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, base = git_repo
    head = _commit_workflow(repo, _workflow().replace("Example CI", "Changed CI"))
    real_run = SELECTOR_MODULE.subprocess.run

    def no_hunks(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if command[:3] == ["git", "diff", "--no-renames"]:
            return subprocess.CompletedProcess(command, 0, "diff --git a/ci.yaml b/ci.yaml\n", "")
        return real_run(command, **kwargs)

    monkeypatch.setattr(SELECTOR_MODULE.subprocess, "run", no_hunks)
    with pytest.raises(SELECTOR_MODULE.RegistryError):
        SELECTOR_MODULE._changes_upgrade_workflow_jobs(base, head)


def test_single_line_flow_job_edit_selects_upgrade(
    git_repo: tuple[Path, str],
) -> None:
    repo, base = git_repo
    flow_job = "  e2e-released-upgrade: {runs-on: ubuntu-latest, steps: [{run: echo old}]}\n"
    block_job = (
        "  e2e-released-upgrade:\n"
        "    runs-on: ubuntu-latest\n"
        "    steps:\n"
        "      - run: echo e2e-released-upgrade\n"
    )
    # Both commits use the valid compact representation; only its command changes.
    (repo / WORKFLOW_PATH).write_text(
        _workflow().replace(block_job, flow_job), encoding="utf-8"
    )
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "flow base")
    base = _git(repo, "rev-parse", "HEAD")
    head = _commit_workflow(
        repo, _workflow().replace(block_job, flow_job.replace("echo old", "echo new"))
    )

    assert SELECTOR_MODULE._changes_upgrade_workflow_jobs(base, head) is True


def test_multiline_flow_job_final_line_selects_upgrade(
    git_repo: tuple[Path, str],
) -> None:
    repo, _ = git_repo
    block_job = (
        "  e2e-released-upgrade:\n"
        "    runs-on: ubuntu-latest\n"
        "    steps:\n"
        "      - run: echo e2e-released-upgrade\n"
    )
    flow_job = (
        "  e2e-released-upgrade: {runs-on: ubuntu-latest,\n"
        "    steps: [{run: echo old}]}\n"
    )
    (repo / WORKFLOW_PATH).write_text(
        _workflow().replace(block_job, flow_job), encoding="utf-8"
    )
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "flow base")
    base = _git(repo, "rev-parse", "HEAD")
    head = _commit_workflow(
        repo, _workflow().replace(block_job, flow_job.replace("echo old", "echo new"))
    )

    assert SELECTOR_MODULE._changes_upgrade_workflow_jobs(base, head) is True


@pytest.mark.parametrize("change", ("retarget", "anchor edit"))
def test_aliased_upgrade_job_fails_closed(
    git_repo: tuple[Path, str], change: str
) -> None:
    repo, _ = git_repo
    base_text = _workflow(("before", "second", *TARGETS, "after")).replace(
        "  before:\n",
        "  before: &first\n",
        1,
    ).replace("  second:\n", "  second: &second\n", 1)
    block_job = (
        "  e2e-released-upgrade:\n"
        "    runs-on: ubuntu-latest\n"
        "    steps:\n"
        "      - run: echo e2e-released-upgrade\n"
    )
    base_text = base_text.replace(block_job, "  e2e-released-upgrade: *first\n")
    (repo / WORKFLOW_PATH).write_text(base_text, encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "alias base")
    base = _git(repo, "rev-parse", "HEAD")
    if change == "retarget":
        changed = base_text.replace("e2e-released-upgrade: *first", "e2e-released-upgrade: *second")
    else:
        changed = base_text.replace("echo before", "echo changed")
    head = _commit_workflow(repo, changed)

    with pytest.raises(SELECTOR_MODULE.RegistryError):
        SELECTOR_MODULE._changes_upgrade_workflow_jobs(base, head)


@pytest.mark.parametrize("omit_kind", (False, True), ids=("main", "next"))
def test_relevant_workflow_edit_runs_full_upgrade_before_merge(
    git_repo: tuple[Path, str], tmp_path: Path, omit_kind: bool
) -> None:
    repo, base = git_repo
    head = _commit_workflow(
        repo, _workflow().replace("echo e2e-released-upgrade", "echo changed", 1)
    )
    options = ("--base", base, "--head", head)
    if omit_kind:
        options += ("--omit-kind",)

    result, output = _invoke_selector(repo, tmp_path, *options)

    assert result.returncode == 0, result.stderr
    assert output["released_upgrade"] == "true"
    assert output["released_upgrade_full"] == "true"
    assert output["cluster"] == "false"


def test_unrelated_workflow_edit_keeps_upgrade_tier_off(
    git_repo: tuple[Path, str], tmp_path: Path
) -> None:
    repo, base = git_repo
    head = _commit_workflow(repo, _workflow().replace("echo before", "echo changed"))

    result, output = _invoke_selector(repo, tmp_path, "--base", base, "--head", head)

    assert result.returncode == 0, result.stderr
    assert output["released_upgrade"] == "false"
    assert output["released_upgrade_full"] == "false"


def test_ordinary_upgrade_code_edit_remains_smoke_only(
    git_repo: tuple[Path, str], tmp_path: Path
) -> None:
    repo, base = git_repo
    chart = repo / "charts/curie/values.yaml"
    chart.parent.mkdir(parents=True)
    chart.write_text("example: true\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "chart edit")
    head = _git(repo, "rev-parse", "HEAD")

    result, output = _invoke_selector(repo, tmp_path, "--base", base, "--head", head)

    assert result.returncode == 0, result.stderr
    assert output["released_upgrade"] == "true"
    assert output["released_upgrade_full"] == "false"


def test_path_only_workflow_selection_has_no_hunks_to_select(tmp_path: Path) -> None:
    result, output = _invoke_selector(
        tmp_path, tmp_path, "--path", WORKFLOW_PATH.as_posix()
    )

    assert result.returncode == 0, result.stderr
    assert output["released_upgrade"] == "false"
    assert output["released_upgrade_full"] == "false"


def test_ordinary_next_pr_still_omits_kind(
    git_repo: tuple[Path, str], tmp_path: Path
) -> None:
    repo, base = git_repo
    head = _commit_workflow(repo, _workflow().replace("echo before", "echo changed"))

    result, output = _invoke_selector(
        repo, tmp_path, "--base", base, "--head", head, "--omit-kind"
    )

    assert result.returncode == 0, result.stderr
    assert output["cluster"] == "false"
    assert output["released_upgrade"] == "false"
    assert output["released_upgrade_full"] == "false"
