"""Deep tiers defer from pull requests to the merge queue, never silently.

A pull request runs the fast tiers. The kind, upgrade and convergence tiers
(the deep tiers) run on a pull request only when it carries the `ci:deep`
label, and always in the merge queue, which tests the exact merge combination
before anything lands. The two required aggregates this file drives, `E2E
required` and `Rust (fmt + clippy + test)`, are what make that safe:

1. On an unlabeled pull request a selected deep tier must be skipped, and the
   aggregate names it in the step summary as deferred to the merge queue.
2. Everywhere else (the queue, a push, a dispatch, a labeled pull request) a
   selected deep tier must have succeeded, and a run whose `deep` output is not
   exactly "true" off a pull request fails outright, so the queue can never
   defer.

Nothing here is restated: the selection comes from the real selector, `deep`
from the real `changes` step script, each job result from the real job `if:`
conditions (through the dry evaluator in the ci-workflow-paths tests), and the
aggregate verdict from the real `run` script with the env its step declares.
"""

from __future__ import annotations

import functools
import importlib.util
import json
import os
import re
import subprocess
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yaml"
WORKFLOW_PATHS_TESTS = (
    REPO_ROOT / "tools" / "ci-workflow-paths" / "tests" / "test_workflow_paths.py"
)
REQUIRED_STEP = "Require exact selected outcomes"
DEFERRED = "Deferred to the merge queue: "
RUNTIME_DEFERRED = DEFERRED + "changed runtime assertion receipts"
CONVERGENCE_DEFERRED = DEFERRED + "Rust convergence regressions (kind)"

# The deep jobs whose results E2E required reads.
E2E_DEEP_JOBS = (
    "e2e-ladder-cluster",
    "e2e-scripted-factory",
    "factory-admitted-agent",
    "e2e-cluster-chart-regressions",
    "e2e-cluster-approval-resume-restarts",
    "e2e-released-upgrade-negative",
    "e2e-cluster-upgrade-matrix",
)
CONVERGENCE_JOB = "rust-convergence"
# One diff that selects every deep tier: chart, factory, approval resume, and
# a changed runtime assertion.
EVERY_DEEP_TIER = (
    "charts/curie/templates/_helpers.tpl",
    "tools/factory-e2e/scripted_kind.py",
    "apps/worker/src/curie_worker/approvals.py",
    "charts/curie/ci/runtime/connector-readiness-runtime.sh",
)
NO_DEEP_TIER = ("README.md",)
RUNTIME_PREFIX = "charts/curie/ci/runtime/"
EXPRESSION = re.compile(r"\$\{\{\s*(.*?)\s*\}\}")


@functools.cache
def _workflow_paths() -> ModuleType:
    """The dry job condition evaluator, loaded under a name of its own."""
    spec = importlib.util.spec_from_file_location(
        "deep_tier_aggregates_workflow_paths", WORKFLOW_PATHS_TESTS
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@functools.cache
def _jobs() -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW.read_text())["jobs"]


def _required_step() -> dict[str, Any]:
    steps = [step for step in _jobs()["e2e-required"]["steps"] if step.get("name") == REQUIRED_STEP]
    assert len(steps) == 1
    return steps[0]


def _rust_step() -> dict[str, Any]:
    steps = [step for step in _jobs()["rust"]["steps"] if isinstance(step.get("run"), str)]
    assert len(steps) == 1
    return steps[0]


def _deep_output(tmp_path: Path, event: str, label: str) -> str:
    returncode, output = _workflow_paths()._run_deep_step(tmp_path, event, label)
    assert returncode == 0
    lines = output.splitlines()
    assert len(lines) == 1 and lines[0].startswith("deep="), output
    return lines[0].removeprefix("deep=")


def _scenario(
    tmp_path: Path, paths: tuple[str, ...], event: str, label: str = "false",
    deep: str | None = None,
) -> dict[str, Any]:
    """Bindings and job results for one diff on one event.

    `deep` overrides the real deep step only for refusal cases that model a
    broken or tampered selection; every other case takes it from the step.
    """
    paths_module = _workflow_paths()
    bindings = paths_module._diff_bindings(tmp_path, paths)
    if deep is None:
        deep = _deep_output(tmp_path, event, label)
    runtime = [path for path in paths if path.startswith(RUNTIME_PREFIX)]
    bindings.update({
        "needs.changes.outputs.deep": deep,
        "needs.rust-select.outputs.deep": deep,
        "needs.chart-changes.outputs.deep": deep,
        "needs.changes.outputs.runtime_assertions": json.dumps(runtime),
    })
    results = paths_module._queue_decisions(_jobs(), bindings, event)
    return {"event": event, "bindings": bindings, "results": results}


def _environment(step: dict[str, Any], scenario: dict[str, Any]) -> dict[str, str]:
    values: dict[str, str] = {
        **{key: str(value) for key, value in scenario["bindings"].items()},
        **{f"needs.{job}.result": result for job, result in scenario["results"].items()},
        "github.event_name": scenario["event"],
        # Only a workflow_dispatch input; empty on every other event.
        "inputs.nightly": "",
    }

    def substitute(match: re.Match[str]) -> str:
        expression = match[1]
        assert expression in values, f"{REQUIRED_STEP}: unbound env expression {expression!r}"
        return values[expression]

    return {name: EXPRESSION.sub(substitute, str(value)) for name, value in step["env"].items()}


def _run(
    step: dict[str, Any], scenario: dict[str, Any], summary: Path,
) -> subprocess.CompletedProcess[str]:
    environment = {
        "PATH": os.environ["PATH"],
        "GITHUB_STEP_SUMMARY": str(summary),
        **_environment(step, scenario),
    }
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", step["run"]],
        cwd=REPO_ROOT, env=environment, capture_output=True, text=True, check=False,
    )


def _deferred_lines(summary: Path) -> list[str]:
    if not summary.exists():
        return []
    return [line for line in summary.read_text().splitlines() if line.startswith(DEFERRED)]


def _title(job_id: str) -> str:
    """A job's display name up to its parenthetical, where matrix values live."""
    return str(_jobs()[job_id]["name"]).split(" (", 1)[0]


def _assert_names_each_deferred_job(lines: list[str], jobs: tuple[str, ...]) -> None:
    remaining = list(lines)
    for job_id in jobs:
        matches = [line for line in remaining if line.startswith(DEFERRED + _title(job_id))]
        assert len(matches) == 1, (job_id, lines)
        remaining.remove(matches[0])
    assert remaining == [], remaining


def _with(scenario: dict[str, Any], **results: str) -> dict[str, Any]:
    return {**scenario, "results": {**scenario["results"], **results}}


# E2E required


def test_every_deep_tier_diff_selects_each_deep_job_in_the_queue(tmp_path: Path) -> None:
    queue = _scenario(tmp_path, EVERY_DEEP_TIER, "merge_group")
    for job_id in (*E2E_DEEP_JOBS, CONVERGENCE_JOB):
        assert queue["results"][job_id] == "success", job_id
    assert queue["bindings"]["needs.changes.outputs.runtime_assertions"] != "[]"


def test_unlabeled_pull_request_defers_every_selected_deep_tier(tmp_path: Path) -> None:
    scenario = _scenario(tmp_path, EVERY_DEEP_TIER, "pull_request")
    assert scenario["bindings"]["needs.changes.outputs.deep"] == "false"
    for job_id in E2E_DEEP_JOBS:
        assert scenario["results"][job_id] == "skipped", job_id
    summary = tmp_path / "summary"
    result = _run(_required_step(), scenario, summary)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "E2E required passed" in result.stdout
    lines = _deferred_lines(summary)
    assert RUNTIME_DEFERRED in lines
    lines.remove(RUNTIME_DEFERRED)
    _assert_names_each_deferred_job(lines, E2E_DEEP_JOBS)
    # The run log says it too, not only the summary.
    assert "Deferred to the merge queue" in result.stdout


def test_unlabeled_pull_request_without_deep_selection_defers_nothing(tmp_path: Path) -> None:
    scenario = _scenario(tmp_path, NO_DEEP_TIER, "pull_request")
    summary = tmp_path / "summary"
    result = _run(_required_step(), scenario, summary)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "E2E required passed" in result.stdout
    assert _deferred_lines(summary) == []


@pytest.mark.parametrize("job_id", E2E_DEEP_JOBS)
def test_unlabeled_pull_request_refuses_a_deep_tier_that_ran(tmp_path: Path, job_id: str) -> None:
    scenario = _with(_scenario(tmp_path, EVERY_DEEP_TIER, "pull_request"), **{job_id: "success"})
    summary = tmp_path / "summary"
    result = _run(_required_step(), scenario, summary)
    assert result.returncode != 0, result.stdout + result.stderr
    assert "E2E required passed" not in result.stdout
    assert _deferred_lines(summary) == []


def test_merge_group_with_every_deep_tier_passing_defers_nothing(tmp_path: Path) -> None:
    scenario = _scenario(tmp_path, EVERY_DEEP_TIER, "merge_group")
    assert scenario["bindings"]["needs.changes.outputs.deep"] == "true"
    summary = tmp_path / "summary"
    result = _run(_required_step(), scenario, summary)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "E2E required passed" in result.stdout
    assert _deferred_lines(summary) == []


@pytest.mark.parametrize("outcome", ["failure", "skipped", "cancelled", ""])
@pytest.mark.parametrize("job_id", E2E_DEEP_JOBS)
def test_merge_group_refuses_a_selected_deep_tier_that_did_not_pass(
    tmp_path: Path, job_id: str, outcome: str,
) -> None:
    scenario = _with(_scenario(tmp_path, EVERY_DEEP_TIER, "merge_group"), **{job_id: outcome})
    summary = tmp_path / "summary"
    result = _run(_required_step(), scenario, summary)
    assert result.returncode != 0, result.stdout + result.stderr
    assert "E2E required passed" not in result.stdout
    assert _deferred_lines(summary) == []


@pytest.mark.parametrize("event", ["merge_group", "push", "workflow_dispatch"])
@pytest.mark.parametrize("paths", [EVERY_DEEP_TIER, NO_DEEP_TIER], ids=["deep", "docs"])
@pytest.mark.parametrize("deep", ["false", "", "yes"])
def test_off_pull_request_refuses_any_deep_other_than_true(
    tmp_path: Path, event: str, paths: tuple[str, ...], deep: str,
) -> None:
    # The deep jobs skip because deep is not "true"; every result is otherwise
    # exactly what those conditions produce, so only the deep check can fail.
    scenario = _scenario(tmp_path, paths, event, deep=deep)
    summary = tmp_path / "summary"
    result = _run(_required_step(), scenario, summary)
    assert result.returncode != 0, result.stdout + result.stderr
    assert "E2E required passed" not in result.stdout
    assert _deferred_lines(summary) == []


@pytest.mark.parametrize("deep", ["", "yes", "True"])
def test_pull_request_refuses_a_malformed_deep(tmp_path: Path, deep: str) -> None:
    scenario = _scenario(tmp_path, EVERY_DEEP_TIER, "pull_request", deep=deep)
    result = _run(_required_step(), scenario, tmp_path / "summary")
    assert result.returncode != 0, result.stdout + result.stderr
    assert _deferred_lines(tmp_path / "summary") == []


def test_labeled_pull_request_runs_and_requires_every_deep_tier(tmp_path: Path) -> None:
    scenario = _scenario(tmp_path, EVERY_DEEP_TIER, "pull_request", label="true")
    assert scenario["bindings"]["needs.changes.outputs.deep"] == "true"
    queue = _scenario(tmp_path, EVERY_DEEP_TIER, "merge_group")
    assert scenario["results"] == queue["results"]
    summary = tmp_path / "summary"
    passing = _run(_required_step(), scenario, summary)
    assert passing.returncode == 0, passing.stdout + passing.stderr
    assert _deferred_lines(summary) == []
    for job_id in E2E_DEEP_JOBS:
        skipped = _run(_required_step(), _with(scenario, **{job_id: "skipped"}), tmp_path / job_id)
        assert skipped.returncode != 0, (job_id, skipped.stdout + skipped.stderr)
        assert _deferred_lines(tmp_path / job_id) == []


# Rust (fmt + clippy + test)


def test_rust_unlabeled_pull_request_defers_convergence(tmp_path: Path) -> None:
    scenario = _scenario(tmp_path, EVERY_DEEP_TIER, "pull_request")
    assert scenario["bindings"]["needs.rust-select.outputs.tests"] == "true"
    assert scenario["results"][CONVERGENCE_JOB] == "skipped"
    assert scenario["results"]["rust-test"] == "success"
    summary = tmp_path / "summary"
    result = _run(_rust_step(), scenario, summary)
    assert result.returncode == 0, result.stdout + result.stderr
    assert _deferred_lines(summary) == [CONVERGENCE_DEFERRED]
    assert CONVERGENCE_DEFERRED in result.stdout


def test_rust_unlabeled_pull_request_without_rust_tests_defers_nothing(tmp_path: Path) -> None:
    scenario = _scenario(tmp_path, NO_DEEP_TIER, "pull_request")
    assert scenario["bindings"]["needs.rust-select.outputs.tests"] == "false"
    summary = tmp_path / "summary"
    result = _run(_rust_step(), scenario, summary)
    assert result.returncode == 0, result.stdout + result.stderr
    assert _deferred_lines(summary) == []


def test_rust_unlabeled_pull_request_refuses_convergence_that_ran(tmp_path: Path) -> None:
    scenario = _with(
        _scenario(tmp_path, EVERY_DEEP_TIER, "pull_request"), **{CONVERGENCE_JOB: "success"}
    )
    summary = tmp_path / "summary"
    result = _run(_rust_step(), scenario, summary)
    assert result.returncode != 0, result.stdout + result.stderr
    assert _deferred_lines(summary) == []


def test_rust_merge_group_with_convergence_passing_defers_nothing(tmp_path: Path) -> None:
    scenario = _scenario(tmp_path, EVERY_DEEP_TIER, "merge_group")
    assert scenario["results"][CONVERGENCE_JOB] == "success"
    summary = tmp_path / "summary"
    result = _run(_rust_step(), scenario, summary)
    assert result.returncode == 0, result.stdout + result.stderr
    assert _deferred_lines(summary) == []


@pytest.mark.parametrize("outcome", ["failure", "skipped", "cancelled", ""])
def test_rust_merge_group_refuses_convergence_that_did_not_pass(
    tmp_path: Path, outcome: str,
) -> None:
    scenario = _with(
        _scenario(tmp_path, EVERY_DEEP_TIER, "merge_group"), **{CONVERGENCE_JOB: outcome}
    )
    summary = tmp_path / "summary"
    result = _run(_rust_step(), scenario, summary)
    assert result.returncode != 0, result.stdout + result.stderr
    assert _deferred_lines(summary) == []


@pytest.mark.parametrize("event", ["merge_group", "push", "workflow_dispatch"])
@pytest.mark.parametrize("paths", [EVERY_DEEP_TIER, NO_DEEP_TIER], ids=["deep", "docs"])
@pytest.mark.parametrize("deep", ["false", "", "yes"])
def test_rust_off_pull_request_refuses_any_deep_other_than_true(
    tmp_path: Path, event: str, paths: tuple[str, ...], deep: str,
) -> None:
    scenario = _scenario(tmp_path, paths, event, deep=deep)
    summary = tmp_path / "summary"
    result = _run(_rust_step(), scenario, summary)
    assert result.returncode != 0, result.stdout + result.stderr
    assert _deferred_lines(summary) == []


@pytest.mark.parametrize("deep", ["", "yes", "True"])
def test_rust_pull_request_refuses_a_malformed_deep(tmp_path: Path, deep: str) -> None:
    scenario = _scenario(tmp_path, EVERY_DEEP_TIER, "pull_request", deep=deep)
    result = _run(_rust_step(), scenario, tmp_path / "summary")
    assert result.returncode != 0, result.stdout + result.stderr


def test_rust_labeled_pull_request_requires_convergence(tmp_path: Path) -> None:
    scenario = _scenario(tmp_path, EVERY_DEEP_TIER, "pull_request", label="true")
    assert scenario["results"][CONVERGENCE_JOB] == "success"
    passing = _run(_rust_step(), scenario, tmp_path / "summary")
    assert passing.returncode == 0, passing.stdout + passing.stderr
    assert _deferred_lines(tmp_path / "summary") == []
    skipped = _run(_rust_step(), _with(scenario, **{CONVERGENCE_JOB: "skipped"}), tmp_path / "skip")
    assert skipped.returncode != 0, skipped.stdout + skipped.stderr
