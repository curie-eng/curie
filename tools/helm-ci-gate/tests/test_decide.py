"""Executable contract for the helm-ci chart path gate.

Loaded by file path, not as a top-level `select` module. Python puts a
script's directory first on sys.path, and a module named `select` shadows the
stdlib one (subprocess imports selectors, which imports select). See the same
note in tools/e2e-ci-selection/tests/test_e2e_ci_selection.py.
"""

from __future__ import annotations

import importlib.util
import json
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

SHARD_JOBS = (
    "chart-lint-and-assertions-1",
    "chart-assertions-2",
    "chart-assertions-retained-values",
)
# The kind jobs are deep tiers: an unlabeled pull request defers them to the
# merge queue, which runs them on the exact merge combination.
KIND_JOBS = (
    "reserved-env-upgrade",
    "automatic-thread-isolation",
)
SHARD_IF = "${{ needs.chart-changes.outputs.chart == 'true' }}"
KIND_IF = (
    "${{ needs.chart-changes.outputs.chart == 'true' && "
    "needs.chart-changes.outputs.deep == 'true' }}"
)
HELM_IF = "${{ !cancelled() }}"
DEEP_LABEL_EXPRESSION = "${{ contains(github.event.pull_request.labels.*.name, 'ci:deep') }}"
SHARD_NAMES = ("lint", "render", "retained")
KIND_NAMES = ("reserved_env", "thread_isolation")
RESULT_NAMES = SHARD_NAMES + KIND_NAMES
KIND_DISPLAY_NAMES = {
    "Reject legacy extraEnv before upgrade mutation",
    "Automatic thread isolation (kind)",
}
DEFERRED = "Deferred to the merge queue: "
RESULT_ENV = {
    "lint": "SHARD_LINT",
    "render": "SHARD_RENDER",
    "retained": "SHARD_RETAINED",
    "reserved_env": "KIND_RESERVED_ENV",
    "thread_isolation": "KIND_THREAD_ISOLATION",
}


def _load_decide():
    spec = importlib.util.spec_from_file_location("helm_ci_gate_decide", DECIDE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _results(shards: str, kind: str | None = None) -> dict[str, str]:
    """Every aggregate key: the three shards, then the two kind jobs."""
    return {
        **{name: shards for name in SHARD_NAMES},
        **{name: shards if kind is None else kind for name in KIND_NAMES},
    }


def _aggregate_env(
    event: str, chart: str, deep: str, results: dict[str, str], summary: Path,
) -> dict[str, str]:
    return {
        "CHANGES_RESULT": "success",
        "EVENT_NAME": event,
        "CHART": chart,
        "DEEP": deep,
        "GITHUB_STEP_SUMMARY": str(summary),
        **{RESULT_ENV[name]: value for name, value in results.items()},
    }


def _deferred_lines(summary: Path) -> list[str]:
    if not summary.exists():
        return []
    return [line for line in summary.read_text().splitlines() if line.startswith(DEFERRED)]


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
    results = _results("success")
    assert decide.aggregate_ok("success", "merge_group", "true", "true", results) is True


def test_aggregate_accepts_a_skipped_chart():
    decide = _load_decide()
    results = _results("skipped")
    assert decide.aggregate_ok("success", "merge_group", "false", "true", results) is True


@pytest.mark.parametrize("status", ("failure", "cancelled", "skipped"))
@pytest.mark.parametrize("name", RESULT_NAMES)
def test_aggregate_refuses_a_bad_result_when_the_chart_ran(status: str, name: str):
    decide = _load_decide()
    results = _results("success")
    results[name] = status
    assert decide.aggregate_ok("success", "merge_group", "true", "true", results) is False


@pytest.mark.parametrize("name", RESULT_NAMES)
def test_aggregate_refuses_a_success_result_when_the_chart_was_skipped(name: str):
    decide = _load_decide()
    results = _results("skipped")
    results[name] = "success"
    assert decide.aggregate_ok("success", "merge_group", "false", "true", results) is False


def test_aggregate_refuses_a_failed_changes_result():
    decide = _load_decide()
    results = _results("success")
    assert decide.aggregate_ok("failure", "merge_group", "true", "true", results) is False


def test_aggregate_refuses_a_non_boolean_chart_flag():
    decide = _load_decide()
    results = _results("success")
    assert decide.aggregate_ok("success", "merge_group", "yes", "true", results) is False


@pytest.mark.parametrize("missing", RESULT_NAMES)
def test_aggregate_refuses_a_missing_result_key(missing: str):
    decide = _load_decide()
    results = _results("success")
    del results[missing]
    assert decide.aggregate_ok("success", "merge_group", "true", "true", results) is False


def test_deep_label_unlabeled_pull_request_defers_only_the_kind_jobs():
    decide = _load_decide()
    # Liveness: the realistic unlabeled chart PR runs the shards and skips kind.
    assert decide.aggregate_ok(
        "success", "pull_request", "true", "false", _results("success", "skipped")
    ) is True
    # The shards stay required on the pull request.
    for name in SHARD_NAMES:
        results = _results("success", "skipped")
        results[name] = "skipped"
        assert decide.aggregate_ok("success", "pull_request", "true", "false", results) is False
    # A deferred kind job that ran anyway is not the selected outcome.
    for name in KIND_NAMES:
        results = _results("success", "skipped")
        results[name] = "success"
        assert decide.aggregate_ok("success", "pull_request", "true", "false", results) is False


def test_deep_label_labeled_pull_request_requires_the_kind_jobs():
    decide = _load_decide()
    results = _results("success")
    assert decide.aggregate_ok("success", "pull_request", "true", "true", results) is True
    assert decide.aggregate_ok(
        "success", "pull_request", "true", "true", _results("success", "skipped")
    ) is False


@pytest.mark.parametrize("event", ("merge_group", "push"))
@pytest.mark.parametrize(
    ("chart", "results"),
    (
        ("true", _results("success", "skipped")),
        ("true", _results("success")),
        ("false", _results("skipped")),
    ),
)
def test_deep_aggregate_refuses_deep_false_outside_pull_requests(
    event: str, chart: str, results: dict[str, str],
):
    decide = _load_decide()
    assert decide.aggregate_ok("success", event, chart, "false", results) is False


@pytest.mark.parametrize("deep", ("", "yes", "True"))
@pytest.mark.parametrize("event", ("pull_request", "merge_group"))
def test_deep_aggregate_refuses_a_non_boolean_deep_flag(event: str, deep: str):
    decide = _load_decide()
    assert decide.aggregate_ok("success", event, "true", deep, _results("success")) is False
    assert decide.aggregate_ok(
        "success", event, "true", deep, _results("success", "skipped")
    ) is False


def test_deep_deferred_names_the_kind_jobs_only_when_selected_and_not_deep():
    decide = _load_decide()
    names = list(decide.deferred("true", "false"))
    assert len(names) == 2
    assert set(names) == KIND_DISPLAY_NAMES
    assert list(decide.deferred("true", "true")) == []
    assert list(decide.deferred("false", "false")) == []
    assert list(decide.deferred("false", "true")) == []


def test_deep_deferred_names_match_the_workflow_job_names():
    jobs = _workflow()["jobs"]
    assert {jobs[name]["name"] for name in KIND_JOBS} == KIND_DISPLAY_NAMES


@pytest.mark.parametrize(
    "env",
    (
        {
            "CHANGES_RESULT": "success",
            "EVENT_NAME": "merge_group",
            "CHART": "true",
            "DEEP": "true",
            "SHARD_LINT": "success",
            "SHARD_RENDER": "success",
            "SHARD_RETAINED": "success",
            "KIND_RESERVED_ENV": "success",
            "KIND_THREAD_ISOLATION": "success",
        },
        {
            "CHANGES_RESULT": "success",
            "EVENT_NAME": "merge_group",
            "CHART": "false",
            "DEEP": "true",
            "SHARD_LINT": "skipped",
            "SHARD_RENDER": "skipped",
            "SHARD_RETAINED": "skipped",
            "KIND_RESERVED_ENV": "skipped",
            "KIND_THREAD_ISOLATION": "skipped",
        },
    ),
)
def test_aggregate_cli_liveness_exits_zero(env: dict[str, str], tmp_path: Path):
    summary = tmp_path / "summary"
    result = _run_aggregate({**env, "GITHUB_STEP_SUMMARY": str(summary)})
    assert result.returncode == 0, result.stdout + result.stderr
    assert "negative control passed" in result.stdout + result.stderr
    assert _deferred_lines(summary) == []


def test_aggregate_cli_failure_exits_one():
    result = _run_aggregate(
        {
            "CHANGES_RESULT": "failure",
            "EVENT_NAME": "merge_group",
            "CHART": "true",
            "DEEP": "true",
            "SHARD_LINT": "success",
            "SHARD_RENDER": "success",
            "SHARD_RETAINED": "success",
            "KIND_RESERVED_ENV": "success",
            "KIND_THREAD_ISOLATION": "success",
        }
    )
    assert result.returncode == 1


def test_deep_label_aggregate_cli_defers_kind_on_an_unlabeled_pull_request(tmp_path: Path):
    summary = tmp_path / "summary"
    env = _aggregate_env("pull_request", "true", "false", _results("success", "skipped"), summary)
    result = _run_aggregate(env)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "negative control passed" in result.stdout + result.stderr
    lines = _deferred_lines(summary)
    assert sorted(lines) == sorted(DEFERRED + name for name in KIND_DISPLAY_NAMES)
    for name in KIND_DISPLAY_NAMES:
        assert name in result.stdout


def test_deep_label_aggregate_cli_does_not_defer_an_unselected_chart(tmp_path: Path):
    summary = tmp_path / "summary"
    env = _aggregate_env("pull_request", "false", "false", _results("skipped"), summary)
    result = _run_aggregate(env)
    assert result.returncode == 0, result.stdout + result.stderr
    assert _deferred_lines(summary) == []


@pytest.mark.parametrize(
    ("event", "deep", "results"),
    (
        ("pull_request", "false", _results("success")),
        ("merge_group", "true", _results("success", "skipped")),
        ("merge_group", "true", _results("success", "failure")),
        ("merge_group", "true", {**_results("success"), "thread_isolation": "skipped"}),
        ("merge_group", "true", {**_results("success"), "reserved_env": ""}),
        ("merge_group", "false", _results("success", "skipped")),
        ("merge_group", "false", _results("skipped")),
        ("merge_group", "", _results("success")),
        ("push", "false", _results("success", "skipped")),
    ),
)
def test_deep_aggregate_cli_refuses_a_wrong_kind_outcome(
    tmp_path: Path, event: str, deep: str, results: dict[str, str],
):
    summary = tmp_path / "summary"
    chart = "false" if all(value == "skipped" for value in results.values()) else "true"
    result = _run_aggregate(_aggregate_env(event, chart, deep, results, summary))
    assert result.returncode == 1, result.stdout + result.stderr
    assert "negative control passed" not in result.stdout
    # A refused run defers nothing: no Deferred line may make it look intentional.
    assert _deferred_lines(summary) == []


def test_workflow_triggers_are_unfiltered_on_both_releasable_branches():
    triggers = _workflow()[True]
    for event in ("push", "pull_request"):
        assert triggers[event]["branches"] == ["main", "next"]
        assert "paths" not in triggers[event]
        assert "paths-ignore" not in triggers[event]
    assert triggers["merge_group"] == {"types": ["checks_requested"]}


def test_chart_changes_selects_through_decide():
    job = _workflow()["jobs"]["chart-changes"]
    assert "if" not in job
    assert job["outputs"]["chart"] == "${{ steps.filter.outputs.chart }}"
    assert job["outputs"]["deep"] == "${{ steps.filter.outputs.deep }}"
    steps = [step for step in job["steps"] if step.get("id") == "filter"]
    assert len(steps) == 1
    assert "python3 tools/helm-ci-gate/decide.py select" in steps[0].get("run", "")
    # The label is read from the event payload, so it applies on the next push.
    assert steps[0]["env"]["DEEP_LABEL"] == DEEP_LABEL_EXPRESSION
    assert steps[0]["env"]["EVENT_NAME"] == "${{ github.event_name }}"


def test_gated_chart_jobs_follow_the_chart_output():
    jobs = _workflow()["jobs"]
    for name in SHARD_JOBS:
        assert jobs[name]["needs"] == ["chart-changes"]
        assert jobs[name]["if"] == SHARD_IF
    for name in KIND_JOBS:
        assert jobs[name]["needs"] == ["chart-changes"]
        assert jobs[name]["if"] == KIND_IF


def test_helm_aggregates_the_chart_shards():
    job = _workflow()["jobs"]["helm"]
    assert job["name"] == "Chart (lint + template + kubeconform)"
    assert job["if"] == HELM_IF
    assert set(job["needs"]) == {"chart-changes", *SHARD_JOBS, *KIND_JOBS}
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
    assert env["EVENT_NAME"] == "${{ github.event_name }}"
    assert env["DEEP"] == "${{ needs.chart-changes.outputs.deep }}"
    assert env["KIND_RESERVED_ENV"] == "${{ needs.reserved-env-upgrade.result }}"
    assert env["KIND_THREAD_ISOLATION"] == "${{ needs.automatic-thread-isolation.result }}"


def test_no_other_job_has_a_job_level_if():
    allowed = {
        "helm": HELM_IF,
        **{name: SHARD_IF for name in SHARD_JOBS},
        **{name: KIND_IF for name in KIND_JOBS},
    }
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


def _run_select(
    work: Path, output: Path, event: str, label: str | None = None,
) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "EVENT_NAME": event, "GITHUB_OUTPUT": str(output)}
    env.pop("DEEP_LABEL", None)
    if label is not None:
        env["DEEP_LABEL"] = label
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


@pytest.mark.parametrize(
    "changed", ["README.md", "charts/curie/templates/café.yaml", "cli/src/main.rs"],
)
def test_merge_group_uses_payload_bounds_and_matches_pr(tmp_path: Path, changed: str):
    work = _work_repo(tmp_path)
    base = _git(work, "rev-parse", "HEAD").stdout.strip()
    _commit_path(work, changed, "example\n")
    head = _git(work, "rev-parse", "HEAD").stdout.strip()
    payload = tmp_path / "event.json"
    payload.write_text(json.dumps({"merge_group": {"base_sha": base, "head_sha": head}}))
    # A ci:deep labeled pull request decides exactly what the queue decides.
    pr_output = tmp_path / "pr-output"
    pr_result = _run_select(work, pr_output, "pull_request", label="true")
    assert pr_result.returncode == 0, pr_result.stderr
    # A later checkout commit is deliberately outside the queued SHA bounds.
    _commit_path(work, "charts/curie/outside.yaml", "outside\n")
    queue_output = tmp_path / "queue-output"
    result = subprocess.run(
        [sys.executable, str(DECIDE_PATH), "select"], cwd=work,
        env={**os.environ, "EVENT_NAME": "merge_group", "GITHUB_EVENT_PATH": str(payload),
             "GITHUB_OUTPUT": str(queue_output)}, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    assert queue_output.read_text() == pr_output.read_text()
    assert "deep=true" in queue_output.read_text().splitlines()


@pytest.mark.parametrize(
    "payload", [{}, {"merge_group": {}}, {"merge_group": {"base_sha": "bad", "head_sha": "bad"}}],
)
def test_merge_group_rejects_missing_or_invalid_bounds(tmp_path: Path, payload: dict):
    event = tmp_path / "event.json"
    event.write_text(json.dumps(payload))
    output = tmp_path / "output"
    result = subprocess.run(
        [sys.executable, str(DECIDE_PATH), "select"], cwd=REPO_ROOT,
        env={**os.environ, "EVENT_NAME": "merge_group", "GITHUB_EVENT_PATH": str(event),
             "GITHUB_OUTPUT": str(output)}, capture_output=True, text=True,
    )
    assert result.returncode != 0
    assert not output.exists()


def _outputs(output: Path) -> dict[str, str]:
    return dict(line.split("=", 1) for line in output.read_text(encoding="utf-8").splitlines())


@pytest.mark.parametrize(
    ("label", "expected"),
    ((None, "false"), ("false", "false"), ("", "false"), ("true", "true")),
)
def test_deep_label_select_cli_pull_request_writes_deep(
    tmp_path: Path, label: str | None, expected: str,
):
    work = _work_repo(tmp_path)
    _commit_path(work, "charts/curie/templates/a.yaml", "kind: ConfigMap\n")
    output = tmp_path / "github_output"
    result = _run_select(work, output, "pull_request", label=label)
    assert result.returncode == 0, result.stderr
    # deep never masks the path selection: the chart is still selected.
    assert _outputs(output) == {"chart": "true", "deep": expected}


def test_deep_label_select_cli_unrelated_pull_request_still_writes_deep(tmp_path: Path):
    work = _work_repo(tmp_path)
    _commit_path(work, "README.md", "unrelated\n")
    output = tmp_path / "github_output"
    result = _run_select(work, output, "pull_request")
    assert result.returncode == 0, result.stderr
    assert _outputs(output) == {"chart": "false", "deep": "false"}


@pytest.mark.parametrize("label", (None, "false", "true"))
def test_deep_label_select_cli_push_is_always_deep(tmp_path: Path, label: str | None):
    work = _work_repo(tmp_path)
    output = tmp_path / "github_output"
    result = _run_select(work, output, "push", label=label)
    assert result.returncode == 0, result.stderr
    assert _outputs(output) == {"chart": "true", "deep": "true"}


@pytest.mark.parametrize("label", (None, "false"))
def test_deep_label_select_cli_merge_group_is_always_deep(tmp_path: Path, label: str | None):
    work = _work_repo(tmp_path)
    base = _git(work, "rev-parse", "HEAD").stdout.strip()
    _commit_path(work, "README.md", "unrelated\n")
    head = _git(work, "rev-parse", "HEAD").stdout.strip()
    payload = tmp_path / "event.json"
    payload.write_text(json.dumps({"merge_group": {"base_sha": base, "head_sha": head}}))
    output = tmp_path / "github_output"
    env = {**os.environ, "EVENT_NAME": "merge_group", "GITHUB_EVENT_PATH": str(payload),
           "GITHUB_OUTPUT": str(output)}
    env.pop("DEEP_LABEL", None)
    if label is not None:
        env["DEEP_LABEL"] = label
    result = subprocess.run(
        [sys.executable, str(DECIDE_PATH), "select"], cwd=work, env=env,
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    assert _outputs(output) == {"chart": "false", "deep": "true"}
