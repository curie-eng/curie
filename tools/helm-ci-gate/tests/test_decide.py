"""Executable contract for the helm-ci chart path gate.

Loaded by file path, not as a top-level `select` module. Python puts a
script's directory first on sys.path, and a module named `select` shadows the
stdlib one (subprocess imports selectors, which imports select). See the same
note in tools/e2e-ci-selection/tests/test_e2e_ci_selection.py.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
DECIDE_PATH = REPO_ROOT / "tools" / "helm-ci-gate" / "decide.py"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "helm-ci.yaml"

# GitHub path filters match the whole path from the repo root. `*` does not
# match `/`. `**` matches any character including `/`. Filter pattern cheat
# sheet: https://docs.github.com/en/actions/using-workflows/
# workflow-syntax-for-github-actions#filter-pattern-cheat-sheet
CHART_PATHS = (
    "charts/curie/**",
    "examples/sre-bot/**",
    ".github/workflows/helm-ci.yaml",
    ".github/workflows/ci.yaml",
    ".github/workflows/release.yaml",
    "cli/**",
    "apps/api/**",
    "apps/worker/**",
    "apps/dispatcher/**",
    "packages/**",
    "scripts/**",
    "uv.lock",
    "pyproject.toml",
    "compose.yaml",
    "compose.dev.yaml",
    "cli/src/ops/upgrade.rs",
    "cli/tests/data/upgrade-driver.py",
    "packages/aci-protocol/src/aci_protocol/slack_identities.py",
    "packages/aci-protocol/src/aci_protocol/turn.py",
    "apps/worker/src/curie_worker/sandbox/types.py",
    "compose/**",
)

POSITIVE = (
    "charts/curie/Chart.yaml",
    "charts/curie/templates/a.yaml",
    "examples/sre-bot/plugin.json",
    ".github/workflows/helm-ci.yaml",
    ".github/workflows/ci.yaml",
    ".github/workflows/release.yaml",
    "cli/src/main.rs",
    "apps/api/src/x.py",
    "apps/worker/src/x.py",
    "apps/dispatcher/src/x.py",
    "packages/aci-protocol/src/x.py",
    "scripts/check-docs.sh",
    "uv.lock",
    "pyproject.toml",
    "compose.yaml",
    "compose.dev.yaml",
    "cli/src/ops/upgrade.rs",
    "cli/tests/data/upgrade-driver.py",
    "packages/aci-protocol/src/aci_protocol/slack_identities.py",
    "packages/aci-protocol/src/aci_protocol/turn.py",
    "apps/worker/src/curie_worker/sandbox/types.py",
    "compose/generate_release_compose.py",
)

NEGATIVE = (
    "README.md",
    "apps/ui/src/App.tsx",
    "apps/mail-adapter/src/x.py",
    "docs/adr/README.md",
    "charts/other/Chart.yaml",
)

GATED_JOBS = (
    "chart-lint-and-assertions-1",
    "chart-assertions-2",
    "chart-assertions-retained-values",
    "reserved-env-upgrade",
)
GATED_IF = "${{ needs.chart-changes.outputs.chart == 'true' }}"
HELM_IF = "${{ !cancelled() }}"
SHARD_NAMES = ("lint", "render", "retained")


def _load_decide():
    spec = importlib.util.spec_from_file_location("helm_ci_gate_decide", DECIDE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _shards(status: str) -> dict[str, str]:
    return {name: status for name in SHARD_NAMES}


def _workflow() -> dict:
    doc = yaml.safe_load(WORKFLOW.read_text())
    assert isinstance(doc, dict)
    return doc


def _run_aggregate(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(DECIDE_PATH), "aggregate"],
        cwd=REPO_ROOT,
        env={**os.environ, **env},
        capture_output=True,
        text=True,
        check=False,
    )


def test_chart_paths_match_the_historical_filter():
    decide = _load_decide()
    assert decide.CHART_PATHS == CHART_PATHS


@pytest.mark.parametrize("path", POSITIVE)
def test_chart_relevant_accepts_historical_chart_inputs(path: str):
    decide = _load_decide()
    assert decide.chart_relevant([path]) is True


@pytest.mark.parametrize("path", NEGATIVE)
def test_chart_relevant_rejects_unrelated_paths(path: str):
    decide = _load_decide()
    assert decide.chart_relevant([path]) is False


def test_select_chart_push_is_always_on():
    decide = _load_decide()
    assert decide.select_chart("push", ["README.md"]) is True


def test_select_chart_pull_request_ignores_unrelated_files():
    decide = _load_decide()
    assert decide.select_chart("pull_request", ["README.md", "apps/ui/src/App.tsx"]) is False


def test_select_chart_pull_request_selects_a_cli_change():
    decide = _load_decide()
    assert decide.select_chart("pull_request", ["cli/src/main.rs"]) is True


def test_select_chart_rejects_other_events():
    decide = _load_decide()
    with pytest.raises(SystemExit):
        decide.select_chart("workflow_dispatch", [])


def test_aggregate_accepts_a_selected_success():
    decide = _load_decide()
    assert decide.aggregate_ok("success", "true", _shards("success")) is True


def test_aggregate_accepts_a_skipped_chart():
    decide = _load_decide()
    assert decide.aggregate_ok("success", "false", _shards("skipped")) is True


@pytest.mark.parametrize("status", ("failure", "cancelled", "skipped"))
@pytest.mark.parametrize("shard", SHARD_NAMES)
def test_aggregate_refuses_a_bad_shard_when_the_chart_ran(status: str, shard: str):
    decide = _load_decide()
    shards = _shards("success")
    shards[shard] = status
    assert decide.aggregate_ok("success", "true", shards) is False


@pytest.mark.parametrize("shard", SHARD_NAMES)
def test_aggregate_refuses_a_success_shard_when_the_chart_was_skipped(shard: str):
    decide = _load_decide()
    shards = _shards("skipped")
    shards[shard] = "success"
    assert decide.aggregate_ok("success", "false", shards) is False


def test_aggregate_refuses_a_failed_changes_result():
    decide = _load_decide()
    assert decide.aggregate_ok("failure", "true", _shards("success")) is False


def test_aggregate_refuses_a_non_boolean_chart_flag():
    decide = _load_decide()
    assert decide.aggregate_ok("success", "yes", _shards("success")) is False


@pytest.mark.parametrize(
    "env",
    (
        {
            "CHANGES_RESULT": "success",
            "CHART": "true",
            "SHARD_LINT": "success",
            "SHARD_RENDER": "success",
            "SHARD_RETAINED": "success",
        },
        {
            "CHANGES_RESULT": "success",
            "CHART": "false",
            "SHARD_LINT": "skipped",
            "SHARD_RENDER": "skipped",
            "SHARD_RETAINED": "skipped",
        },
    ),
)
def test_aggregate_cli_liveness_exits_zero(env: dict[str, str]):
    result = _run_aggregate(env)
    assert result.returncode == 0
    assert "negative control passed" in result.stdout + result.stderr


def test_aggregate_cli_failure_exits_one():
    result = _run_aggregate(
        {
            "CHANGES_RESULT": "failure",
            "CHART": "true",
            "SHARD_LINT": "success",
            "SHARD_RENDER": "success",
            "SHARD_RETAINED": "success",
        }
    )
    assert result.returncode == 1


def test_workflow_triggers_are_unfiltered_on_both_releasable_branches():
    triggers = _workflow()[True]
    for event in ("push", "pull_request"):
        assert triggers[event]["branches"] == ["main", "next"]
        assert "paths" not in triggers[event]
        assert "paths-ignore" not in triggers[event]


def test_chart_changes_selects_through_decide():
    job = _workflow()["jobs"]["chart-changes"]
    assert "if" not in job
    assert job["outputs"]["chart"] == "${{ steps.filter.outputs.chart }}"
    runs = [
        step.get("run", "")
        for step in job["steps"]
        if step.get("id") == "filter"
    ]
    assert any("python3 tools/helm-ci-gate/decide.py select" in run for run in runs)


def test_gated_chart_jobs_follow_the_chart_output():
    jobs = _workflow()["jobs"]
    for name in GATED_JOBS:
        assert jobs[name]["needs"] == ["chart-changes"]
        assert jobs[name]["if"] == GATED_IF


def test_helm_aggregates_the_chart_shards():
    job = _workflow()["jobs"]["helm"]
    assert job["name"] == "Chart (lint + template + kubeconform)"
    assert job["if"] == HELM_IF
    assert job["needs"] == [
        "chart-changes",
        "chart-lint-and-assertions-1",
        "chart-assertions-2",
        "chart-assertions-retained-values",
    ]
    matched = [
        step
        for step in job["steps"]
        if "python3 tools/helm-ci-gate/decide.py aggregate" in (step.get("run") or "")
    ]
    assert len(matched) == 1
    env = matched[0]["env"]
    assert env["CHANGES_RESULT"] == "${{ needs.chart-changes.result }}"
    assert env["CHART"] == "${{ needs.chart-changes.outputs.chart }}"
    assert env["SHARD_LINT"] == "${{ needs.chart-lint-and-assertions-1.result }}"
    assert env["SHARD_RENDER"] == "${{ needs.chart-assertions-2.result }}"
    assert env["SHARD_RETAINED"] == "${{ needs.chart-assertions-retained-values.result }}"


def test_no_other_job_has_a_job_level_if():
    allowed = {"helm": HELM_IF, **{name: GATED_IF for name in GATED_JOBS}}
    for job_id, job in _workflow()["jobs"].items():
        if job_id in allowed:
            assert job["if"] == allowed[job_id]
        else:
            assert "if" not in job


def _git(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        env={
            **os.environ,
            "GIT_AUTHOR_NAME": "Helm Gate",
            "GIT_AUTHOR_EMAIL": "helm-gate@example.com",
            "GIT_COMMITTER_NAME": "Helm Gate",
            "GIT_COMMITTER_EMAIL": "helm-gate@example.com",
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return result


def _work_repo(tmp_path: Path) -> Path:
    """Bare origin plus a work clone on branch pr. quotePath stays default."""
    base = tmp_path / "base"
    origin = tmp_path / "origin.git"
    work = tmp_path / "work"
    base.mkdir()
    _git(base, "init", "-b", "main")
    _git(base, "config", "user.email", "helm-gate@example.com")
    _git(base, "config", "user.name", "Helm Gate")
    (base / "README.md").write_text("base\n", encoding="utf-8")
    _git(base, "add", "README.md")
    _git(base, "commit", "-m", "init")
    # A bare repo whose HEAD is still master, while only main was pushed,
    # clones as an unborn branch and the three-dot diff has no merge base.
    _git(tmp_path, "init", "--bare", "-b", "main", str(origin))
    _git(base, "remote", "add", "origin", str(origin))
    _git(base, "push", "origin", "main")
    _git(tmp_path, "clone", str(origin), str(work))
    _git(work, "config", "user.email", "helm-gate@example.com")
    _git(work, "config", "user.name", "Helm Gate")
    quoted = subprocess.run(
        ["git", "config", "--get", "core.quotePath"],
        cwd=work,
        capture_output=True,
        text=True,
        check=False,
    )
    assert quoted.returncode != 0
    _git(work, "checkout", "-b", "pr")
    return work


def _commit_path(work: Path, relative: str, body: str) -> None:
    path = work.joinpath(*relative.split("/"))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    _git(work, "add", "--", relative)
    _git(work, "commit", "-m", "change")


def _run_select(work: Path, output: Path, event: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "EVENT_NAME": event, "GITHUB_OUTPUT": str(output)}
    if event == "pull_request":
        env["BASE_REF"] = "main"
    return subprocess.run(
        [sys.executable, str(DECIDE_PATH), "select"],
        cwd=work,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_select_cli_pull_request_keeps_a_quoted_unicode_chart_path(tmp_path: Path):
    work = _work_repo(tmp_path)
    _commit_path(work, "charts/curie/templates/café.yaml", "kind: ConfigMap\n")
    output = tmp_path / "github_output"
    result = _run_select(work, output, "pull_request")
    assert result.returncode == 0
    assert "chart=true" in output.read_text(encoding="utf-8")


def test_select_cli_pull_request_keeps_a_quoted_space_chart_path(tmp_path: Path):
    work = _work_repo(tmp_path)
    _commit_path(work, "charts/curie/my file.yaml", "kind: ConfigMap\n")
    output = tmp_path / "github_output"
    result = _run_select(work, output, "pull_request")
    assert result.returncode == 0
    assert "chart=true" in output.read_text(encoding="utf-8")


def test_select_cli_pull_request_skips_an_unrelated_readme(tmp_path: Path):
    work = _work_repo(tmp_path)
    _commit_path(work, "README.md", "unrelated\n")
    output = tmp_path / "github_output"
    result = _run_select(work, output, "pull_request")
    assert result.returncode == 0
    assert "chart=false" in output.read_text(encoding="utf-8")


def test_select_cli_push_selects_the_chart(tmp_path: Path):
    work = _work_repo(tmp_path)
    output = tmp_path / "github_output"
    result = _run_select(work, output, "push")
    assert result.returncode == 0
    assert "chart=true" in output.read_text(encoding="utf-8")
