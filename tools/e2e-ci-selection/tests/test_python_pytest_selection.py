"""Docs-only Python jobs skip compose+pytest without dropping the required check."""

from __future__ import annotations

import os
import subprocess
import sys
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
SHARD_COUNT = 3
ALWAYS_RUN_PYTHON_STEPS = (
    "Alembic revision gate",
    "Ruff",
    "Mypy",
    "Docs gate (catalog drift + agent contract + citations)",
    "Wire tolerance gate (_AciModel model_validate call sites)",
    "ACI wire-lock base gate",
)
# These run in every pytest shard, each on its own runner with its own stack.
GATED_RUNTIME_STEPS = (
    "Start dev stack",
    "Wait for Langfuse to serve",
    "Released database upgrade gate",
    "Migrate the shared database",
    "Pytest",
)
PYTEST_AGGREGATE_EXPRESSIONS = {
    "pytest_selected": "${{ steps.python-runtime.outputs.pytest }}",
    "shards_result": "${{ needs.python-pytest.result }}",
}
SHARD_RECORD_EXPRESSIONS = {
    "shard": "${{ matrix.shard }}",
    "pytest_selected": "${{ steps.python-runtime.outputs.pytest }}",
    "pytest_result": "${{ steps.pytest.outcome }}",
    "stack_result": "${{ steps.dev-stack.outcome }}",
}


def _python_job(job_id: str = "python") -> dict[str, Any]:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    job = workflow["jobs"][job_id]
    assert isinstance(job, dict)
    return job


def _named_steps(job_id: str = "python") -> dict[str, dict[str, Any]]:
    steps = _python_job(job_id)["steps"]
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
    # It waits for the shards only to aggregate them; always() keeps it
    # reporting the required check whatever the shards did.
    assert job["needs"] == "python-pytest"
    assert job["if"] == "always()"

    shards = _python_job("python-pytest")
    assert "needs" not in shards
    assert "if" not in shards
    assert shards["strategy"]["fail-fast"] is False
    assert shards["strategy"]["matrix"]["shard"] == list(range(1, SHARD_COUNT + 1))


def test_python_shards_require_postgres_and_valkey_tests() -> None:
    # The matrix job covers every shard, so job-level env reaches all of them.
    env = _python_job("python-pytest")["env"]
    assert env["CI_REQUIRE_POSTGRES_TESTS"] == "1"
    assert env["CI_REQUIRE_VALKEY_TESTS"] == "1"


def test_python_job_gates_compose_and_pytest_on_selector_output() -> None:
    named = _named_steps("python-pytest")
    decision = named["Decide whether compose and pytest are needed"]
    assert decision["id"] == "python-runtime"
    assert "tools/e2e-ci-selection/select_tiers.py" in _string(decision, "run")
    assert "--registry .github/e2e-selection.yaml" in _string(decision, "run")

    for name in GATED_RUNTIME_STEPS:
        step = named[name]
        assert _string(step, "if") == PYTEST_SELECTED, name

    stack = named["Start dev stack"]
    assert stack["id"] == "dev-stack"
    assert _string(stack, "run").strip() == "python3 scripts/wait-for-langfuse.py --start"
    assert (
        _string(named["Wait for Langfuse to serve"], "run").strip()
        == "python3 scripts/wait-for-langfuse.py"
    )

    pytest_step = named["Pytest"]
    assert pytest_step["id"] == "pytest"
    command = _string(pytest_step, "run").strip()
    assert command.startswith("uv run pytest -q")
    assert f"--ci-shard ${{{{ matrix.shard }}}}/{SHARD_COUNT}" in command
    # One automatic rerun, and every rerun test listed in the summary.
    assert "--reruns 1" in command
    # pytest keeps only the last -r option, so one token must carry both the
    # rerun (R) and skip (s) report characters.
    report_tokens = [token for token in command.split() if token.startswith("-r")]
    assert len(report_tokens) == 1, report_tokens
    assert {"R", "s"} <= set(report_tokens[0][2:])

    # The required job boots no stack and runs no suite of its own.
    required = _named_steps()
    for name in GATED_RUNTIME_STEPS:
        assert name not in required, name
    assert (
        named["Decide whether compose and pytest are needed"]["run"]
        == (required["Decide whether compose and pytest are needed"]["run"])
    )


def test_python_static_gates_stay_unconditional() -> None:
    named = _named_steps()
    for name in ALWAYS_RUN_PYTHON_STEPS:
        step = named[name]
        assert "if" not in step, name
        assert PYTEST_SELECTED not in _string(step, "run")

    docs = named["Docs gate (catalog drift + agent contract + citations)"]
    assert "scripts/check-docs.sh" in _string(docs, "run")


def test_dump_logs_do_not_run_when_the_stack_never_started() -> None:
    named = _named_steps("python-pytest")
    dump = named["Dump dev stack logs on failure"]
    condition = _string(dump, "if")
    assert "failure()" in condition
    assert "steps.python-runtime.outputs.pytest == 'true'" in condition


def _bindings(step: dict[str, Any], expressions: dict[str, str]) -> dict[str, str]:
    environment = step.get("env")
    assert isinstance(environment, dict)
    bindings: dict[str, str] = {}
    for semantic_name, expression in expressions.items():
        environment_names = [name for name, value in environment.items() if value == expression]
        assert len(environment_names) == 1, expression
        bindings[semantic_name] = environment_names[0]
    return bindings


def _run_bash(
    script: str, environment: dict[str, str], cwd: Path
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", script],
        cwd=cwd,
        env={**os.environ, **environment},
        capture_output=True,
        text=True,
        check=False,
    )


def _record_shard(tmp_path: Path, shard: int, selected: str, outcome: str, stack: str) -> None:
    """Run the shard job's real record step, as the shard would."""
    step = _named_steps("python-pytest")["Record shard result"]
    assert _string(step, "if") == "always()"
    bindings = _bindings(step, SHARD_RECORD_EXPRESSIONS)
    state = {
        "shard": str(shard),
        "pytest_selected": selected,
        "pytest_result": outcome,
        "stack_result": stack,
    }
    workdir = tmp_path / f"shard-{shard}"
    workdir.mkdir()
    result = _run_bash(_string(step, "run"), {bindings[k]: v for k, v in state.items()}, workdir)
    assert result.returncode == 0, result.stderr
    upload = _named_steps("python-pytest")["Upload shard result"]
    source = workdir / upload["with"]["path"]
    download = _named_steps()["Download shard results"]
    target = tmp_path / download["with"]["path"]
    target.mkdir(exist_ok=True)
    for record in source.iterdir():
        (target / record.name).write_text(record.read_text())


def _run_pytest_aggregate(
    tmp_path: Path,
    *,
    selected: str,
    shards: dict[int, tuple[str, str, str]] | None = None,
    shards_result: str = "success",
) -> subprocess.CompletedProcess[str]:
    """Record each shard, then run the required job's real aggregate step.

    ``shards`` maps a shard index to (selected, pytest, stack) as that shard
    saw them; an index left out never reported.
    """
    step = _named_steps()["Require pytest outcome to match selection"]
    assert _string(step, "if") == "success()"
    assert step["env"]["SHARD_COUNT"] == str(SHARD_COUNT)
    download = _named_steps()["Download shard results"]
    assert step["env"]["SHARD_RESULTS_DIR"] == download["with"]["path"]
    (tmp_path / download["with"]["path"]).mkdir(parents=True)
    for shard, (shard_selected, outcome, stack) in (shards or {}).items():
        _record_shard(tmp_path, shard, shard_selected, outcome, stack)
    bindings = _bindings(step, PYTEST_AGGREGATE_EXPRESSIONS)
    state = {"pytest_selected": selected, "shards_result": shards_result}
    literals = {k: v for k, v in step["env"].items() if "${{" not in v}
    environment = literals | {bindings[k]: v for k, v in state.items()}
    return _run_bash(_string(step, "run"), environment, tmp_path)


def _all_shards(selected: str, outcome: str) -> dict[int, tuple[str, str, str]]:
    return {shard: (selected, outcome, outcome) for shard in range(1, SHARD_COUNT + 1)}


def test_pytest_aggregator_accepts_docs_only_skips(tmp_path: Path) -> None:
    completed, output = _invoke_selector(tmp_path, "docs/guides/getting-started.md")
    assert completed.returncode == 0, completed.stderr
    selected = _outputs(output)["pytest"]
    result = _run_pytest_aggregate(
        tmp_path / "run", selected=selected, shards=_all_shards(selected, "skipped")
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_pytest_aggregator_accepts_selected_success(tmp_path: Path) -> None:
    completed, output = _invoke_selector(tmp_path, "packages/aci-protocol/src/aci_protocol/wire.py")
    assert completed.returncode == 0, completed.stderr
    selected = _outputs(output)["pytest"]
    result = _run_pytest_aggregate(
        tmp_path / "run", selected=selected, shards=_all_shards(selected, "success")
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_pytest_aggregator_rejects_selected_skip(tmp_path: Path) -> None:
    result = _run_pytest_aggregate(tmp_path, selected="true", shards=_all_shards("true", "skipped"))
    assert result.returncode != 0


def test_pytest_aggregator_rejects_unselected_success(tmp_path: Path) -> None:
    result = _run_pytest_aggregate(
        tmp_path, selected="false", shards=_all_shards("false", "success")
    )
    assert result.returncode != 0


def test_pytest_aggregator_rejects_a_failed_shard(tmp_path: Path) -> None:
    shards = _all_shards("true", "success")
    shards[2] = ("true", "failure", "success")
    result = _run_pytest_aggregate(
        tmp_path, selected="true", shards=shards, shards_result="failure"
    )
    assert result.returncode != 0
    assert "failure" in result.stderr


@pytest.mark.parametrize("shards_result", ["failure", "cancelled", "skipped"])
def test_pytest_aggregator_rejects_shard_jobs_that_did_not_succeed(
    tmp_path: Path, shards_result: str
) -> None:
    result = _run_pytest_aggregate(
        tmp_path,
        selected="true",
        shards=_all_shards("true", "success"),
        shards_result=shards_result,
    )
    assert result.returncode != 0


def test_pytest_aggregator_rejects_a_shard_that_never_reported(tmp_path: Path) -> None:
    shards = _all_shards("true", "success")
    del shards[3]
    result = _run_pytest_aggregate(tmp_path, selected="true", shards=shards)
    assert result.returncode != 0
    assert "shard 3" in result.stderr


def test_pytest_aggregator_rejects_a_shard_that_selected_differently(tmp_path: Path) -> None:
    shards = _all_shards("true", "success")
    shards[1] = ("false", "skipped", "skipped")
    result = _run_pytest_aggregate(tmp_path, selected="true", shards=shards)
    assert result.returncode != 0


def test_shards_partition_the_unfiltered_collection(tmp_path: Path) -> None:
    """Every collected test lands in exactly one shard, and groups stay whole."""
    target = "packages/aci-protocol/tests"

    def collect(*extra: str) -> list[str]:
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "--collect-only",
                "-q",
                "-p",
                "no:cacheprovider",
                target,
                *extra,
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert completed.returncode == 0, completed.stdout + completed.stderr
        return [line for line in completed.stdout.splitlines() if "::" in line]

    full = collect()
    shards = [
        collect("--ci-shard", f"{index}/{SHARD_COUNT}") for index in range(1, SHARD_COUNT + 1)
    ]
    union = [node for shard in shards for node in shard]
    assert len(union) == len(set(union))
    assert sorted(union) == sorted(full)
    assert all(shards), "every shard must run something at this size"
    files = [{node.split("::")[0] for node in shard} for shard in shards]
    for index, owned in enumerate(files):
        for other in files[index + 1 :]:
            assert not owned & other, "a file group straddled two shards"
