"""Docs-only Python jobs skip compose+pytest without dropping the required check."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
SELECTOR = REPO_ROOT / "tools" / "e2e-ci-selection" / "select_tiers.py"
REGISTRY = REPO_ROOT / ".github" / "e2e-selection.yaml"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yaml"


def _invoke_selector(
    tmp_path: Path,
    *paths: str,
    push: bool = False,
    base: str | None = None,
    head: str | None = None,
    cwd: Path | None = None,
) -> tuple[subprocess.CompletedProcess[str], str]:
    output_path = tmp_path / f"github-output-{len(list(tmp_path.glob('github-output-*')))}"
    command = [sys.executable, str(SELECTOR), "--registry", str(REGISTRY)]
    if push:
        command.append("--push")
    if base is not None:
        command.extend(("--base", base))
    if head is not None:
        command.extend(("--head", head))
    for path in paths:
        command.extend(("--path", path))
    environment = os.environ.copy()
    environment["GITHUB_OUTPUT"] = str(output_path)
    completed = subprocess.run(
        command,
        cwd=cwd or tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    output = output_path.read_text() if output_path.exists() else ""
    return completed, output


PYTEST_SELECTED = "steps.python-runtime.outputs.pytest == 'true'"
ALWAYS_RUN_PYTHON_STEPS = (
    "Alembic revision gate",
    "Ruff",
    "Mypy",
    "Docs gate (catalog drift + agent contract + citations)",
    "Wire tolerance gate (_AciModel model_validate call sites)",
    "ACI wire-lock base gate",
)
GATED_RUNTIME_STEPS = (
    "Start dev stack",
    "Wait for Langfuse to serve",
    "Released database upgrade gate",
    "Migrate the shared database",
    "Pytest",
)
PYTEST_AGGREGATE_EXPRESSIONS = {
    "pytest_selected": "${{ steps.python-runtime.outputs.pytest }}",
    "pytest_result": "${{ steps.pytest.outcome }}",
    "stack_result": "${{ steps.dev-stack.outcome }}",
}


def _python_job() -> dict[str, Any]:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    job = workflow["jobs"]["python"]
    assert isinstance(job, dict)
    return job


def _named_steps() -> dict[str, dict[str, Any]]:
    steps = _python_job()["steps"]
    return {
        step["name"]: step
        for step in steps
        if isinstance(step, dict) and isinstance(step.get("name"), str)
    }


def _string(step: dict[str, Any], key: str) -> str:
    value = step.get(key)
    return value if isinstance(value, str) else ""


def _outputs(output: str) -> dict[str, str]:
    return dict(line.split("=", maxsplit=1) for line in output.splitlines() if line)


@pytest.mark.parametrize(
    "path",
    [
        "docs/guides/getting-started.md",
        "docs/example.md",
        "ARCHITECTURE.md",
        "README.md",
        "llms.txt",
    ],
)
def test_docs_only_path_skips_pytest(tmp_path: Path, path: str) -> None:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    assert _outputs(output)["pytest"] == "false"


@pytest.mark.parametrize(
    "path",
    [
        "packages/aci-protocol/src/aci_protocol/wire.py",
        "apps/api/src/curie_api/main.py",
        "apps/worker/src/curie_worker/binding.py",
        "apps/dispatcher/src/curie_dispatcher/app.py",
        "runner/src/curie_runner/session.py",
        "examples/tests/test_example.py",
        "cli/src/main.rs",
        "uv.lock",
        "pyproject.toml",
        "packages/plugin-format/pyproject.toml",
    ],
)
def test_python_or_runtime_path_still_selects_pytest(tmp_path: Path, path: str) -> None:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    assert _outputs(output)["pytest"] == "true"


def test_mixed_docs_and_packages_still_selects_pytest(tmp_path: Path) -> None:
    completed, output = _invoke_selector(
        tmp_path,
        "docs/guides/getting-started.md",
        "packages/aci-protocol/src/aci_protocol/wire.py",
    )
    assert completed.returncode == 0, completed.stderr
    assert _outputs(output)["pytest"] == "true"


def test_push_selects_pytest(tmp_path: Path) -> None:
    completed, output = _invoke_selector(tmp_path, push=True)
    assert completed.returncode == 0, completed.stderr
    outputs = _outputs(output)
    assert outputs["pytest"] == "true"
    for tier in ("skill", "local", "local_release", "cluster", "released_upgrade"):
        assert outputs[tier] == "true"


def test_empty_revision_diff_fails_closed_to_pytest(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    subprocess.run(
        ["git", "init", "--initial-branch", "main"],
        cwd=repository,
        capture_output=True,
        text=True,
        check=True,
    )
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repository, check=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=repository, check=True)
    (repository / "docs").mkdir()
    (repository / "docs" / "guides.md").write_text("one sentence.\n")
    subprocess.run(["git", "add", "."], cwd=repository, check=True)
    subprocess.run(["git", "commit", "-m", "base"], cwd=repository, check=True)
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    completed, output = _invoke_selector(
        tmp_path,
        base=head,
        head=head,
        cwd=repository,
    )
    assert completed.returncode == 0, completed.stderr
    assert _outputs(output)["pytest"] == "true"


def test_deleting_a_docs_guides_sentence_does_not_select_pytest(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    subprocess.run(
        ["git", "init", "--initial-branch", "main"],
        cwd=repository,
        capture_output=True,
        text=True,
        check=True,
    )
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repository, check=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=repository, check=True)
    guides = repository / "docs" / "guides"
    guides.mkdir(parents=True)
    guides.joinpath("getting-started.md").write_text(
        "First sentence.\nSecond sentence that a docs-only edit can delete.\n"
    )
    subprocess.run(["git", "add", "."], cwd=repository, check=True)
    subprocess.run(["git", "commit", "-m", "base"], cwd=repository, check=True)
    base = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    guides.joinpath("getting-started.md").write_text("First sentence.\n")
    subprocess.run(["git", "commit", "-am", "delete one sentence"], cwd=repository, check=True)
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    completed, output = _invoke_selector(
        tmp_path,
        base=base,
        head=head,
        cwd=repository,
    )
    assert completed.returncode == 0, completed.stderr
    assert _outputs(output)["pytest"] == "false"


def test_required_python_job_keeps_ruleset_name_and_is_not_skippable() -> None:
    job = _python_job()
    assert job["name"] == "Python (ruff + mypy + pytest)"
    assert "needs" not in job
    assert "if" not in job


def test_python_job_gates_compose_and_pytest_on_selector_output() -> None:
    named = _named_steps()
    decision = named["Decide whether compose and pytest are needed"]
    assert decision["id"] == "python-runtime"
    assert "tools/e2e-ci-selection/select_tiers.py" in _string(decision, "run")
    assert "--registry .github/e2e-selection.yaml" in _string(decision, "run")

    for name in GATED_RUNTIME_STEPS:
        step = named[name]
        assert _string(step, "if") == PYTEST_SELECTED, name

    stack = named["Start dev stack"]
    assert stack["id"] == "dev-stack"
    assert "postgres" in _string(stack, "run")

    pytest_step = named["Pytest"]
    assert pytest_step["id"] == "pytest"
    assert _string(pytest_step, "run").strip().startswith("uv run pytest -q")


def test_python_static_gates_stay_unconditional() -> None:
    named = _named_steps()
    for name in ALWAYS_RUN_PYTHON_STEPS:
        step = named[name]
        assert "if" not in step, name
        assert PYTEST_SELECTED not in _string(step, "run")

    docs = named["Docs gate (catalog drift + agent contract + citations)"]
    assert "scripts/check-docs.sh" in _string(docs, "run")


def test_dump_logs_do_not_run_when_the_stack_never_started() -> None:
    named = _named_steps()
    dump = named["Dump dev stack logs on failure"]
    condition = _string(dump, "if")
    assert "failure()" in condition
    assert "steps.python-runtime.outputs.pytest == 'true'" in condition


def test_cargo_guard_if_is_unchanged() -> None:
    named = _named_steps()
    probe = named["Decide whether the current curie binary is needed"]
    assert probe["id"] == "fix-pin-curie"
    assert _string(probe, "if") == "github.event_name == 'pull_request'"

    cargo = named["Build the current curie binary for fix pin verification"]
    helm = named["Install Helm for fix pin verification"]
    needed = "steps.fix-pin-curie.outputs.needed == 'true'"
    for step in (cargo, helm):
        condition = _string(step, "if")
        assert "github.event_name == 'pull_request'" in condition
        assert needed in condition
        assert "Fix pin:" not in condition
        assert PYTEST_SELECTED not in condition

    gate = named["Require declared fixes to be pinned by a changed test"]
    assert _string(gate, "if") == "github.event_name == 'pull_request'"
    assert needed not in _string(gate, "if")


def _pytest_aggregate_contract() -> tuple[str, dict[str, str]]:
    named = _named_steps()
    step = named["Require pytest outcome to match selection"]
    assert _string(step, "if") == "success()"
    bindings: dict[str, str] = {}
    environment = step.get("env")
    assert isinstance(environment, dict)
    for semantic_name, expression in PYTEST_AGGREGATE_EXPRESSIONS.items():
        environment_names = [name for name, value in environment.items() if value == expression]
        assert len(environment_names) == 1, expression
        bindings[semantic_name] = environment_names[0]
    return _string(step, "run"), bindings


def _run_pytest_aggregate(
    *,
    script_transform: Callable[[str], str] | None = None,
    **overrides: str,
) -> subprocess.CompletedProcess[str]:
    script, bindings = _pytest_aggregate_contract()
    if script_transform is not None:
        script = script_transform(script)
    state = {
        "pytest_selected": "false",
        "pytest_result": "skipped",
        "stack_result": "skipped",
    }
    state.update(overrides)
    environment = os.environ.copy()
    environment.update({bindings[name]: value for name, value in state.items()})
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", script],
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def test_pytest_aggregator_accepts_docs_only_skips(tmp_path: Path) -> None:
    completed, output = _invoke_selector(tmp_path, "docs/guides/getting-started.md")
    assert completed.returncode == 0, completed.stderr
    selected = _outputs(output)["pytest"]
    result = _run_pytest_aggregate(
        pytest_selected=selected,
        pytest_result="skipped",
        stack_result="skipped",
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_pytest_aggregator_accepts_selected_success(tmp_path: Path) -> None:
    completed, output = _invoke_selector(tmp_path, "packages/aci-protocol/src/aci_protocol/wire.py")
    assert completed.returncode == 0, completed.stderr
    selected = _outputs(output)["pytest"]
    result = _run_pytest_aggregate(
        pytest_selected=selected,
        pytest_result="success",
        stack_result="success",
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_pytest_aggregator_rejects_selected_skip() -> None:
    result = _run_pytest_aggregate(
        pytest_selected="true",
        pytest_result="skipped",
        stack_result="skipped",
    )
    assert result.returncode != 0


def test_pytest_aggregator_rejects_unselected_success() -> None:
    result = _run_pytest_aggregate(
        pytest_selected="false",
        pytest_result="success",
        stack_result="success",
    )
    assert result.returncode != 0
