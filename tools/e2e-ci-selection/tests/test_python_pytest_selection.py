"""Docs-only Python jobs skip compose+pytest without dropping the required check."""

from __future__ import annotations

import ast
import os
import re
import shlex
import shutil
import subprocess
import sys
import tomllib
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
SELECTOR = REPO_ROOT / "tools" / "e2e-ci-selection" / "select_tiers.py"
REGISTRY = REPO_ROOT / ".github" / "e2e-selection.yaml"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yaml"
ADMITTED_AGENT_RUNTIME = "tools/factory-e2e/tests/test_c4_admitted_agent.py"
SCRIPTED_FACTORY_JOB = "e2e-scripted-factory"
ADMITTED_AGENT_JOB = "factory-admitted-agent"
ADMITTED_AGENT_SHARDS = [1, 2, 3, 4, 5]
RELABEL_REGRESSION = (
    "test_two_worker_ci_preserves_successor_identity_and_rejects_stale_predecessor"
)


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
        cwd=cwd or REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    output = output_path.read_text() if output_path.exists() else ""
    return completed, output


PYTEST_SELECTED = "steps.python-runtime.outputs.pytest == 'true'"
STATIC_JOB = "python-static"
STATIC_REQUIREMENT_STEP = "Require static gates to pass"
# #4471: the gates that moved from the required job into STATIC_JOB, in order.
MOVED_STATIC_STEPS = (
    "Lockfile is up to date",
    "Alembic revision gate",
    "Alembic cross-train lineage",
    "Schema window gate",
    "Ruff",
    "Mypy",
    "Import boundaries (harness SDK containment)",
    "Docs gate (catalog drift + agent contract + citations)",
    "Wire tolerance gate (_AciModel model_validate call sites)",
    "Shellcheck sre-bot platform-upgrade script",
    "Release tests",
    "ACI wire-lock base gate",
)
SHARD_COUNT = 6
CONTAINER_JOB = "python-container-fixtures"
CI_POLICY_DIRS = (
    "tools/ci-retry-gate",
    "tools/ci-workflow-paths",
    "tools/ci-release-target-parity",
    "tools/e2e-ci-selection",
    "tools/helm-ci-gate",
    "tools/fix-pin-ci",
    "tools/p0-closes-ci",
    "tools/runtime-assertion-gate",
    "tools/dependency-audit-paths",
    "tools/close-on-next",
)
CONTAINER_FILES = (
    "tools/factory-e2e/tests/test_c4_tracker_scope.py",
    "examples/tests/test_sre_bot_observability_runtime.py",
)
DRY_RUN_JOB = "python-dry-run-fixture"
DRY_RUN_FILE = "tools/factory-e2e/tests/test_c4_dry_run.py"
SHARD_NEIGHBOR = "tools/factory-e2e/tests/test_c4_reinstall_publication.py"
PRIVATE_IMPORT_STEP = "API private import boundaries"
ALWAYS_RUN_PYTHON_STEPS = (
    "Alembic revision gate",
    "Ruff",
    "Mypy",
    "Docs gate (catalog drift + agent contract + citations)",
    "Wire tolerance gate (_AciModel model_validate call sites)",
    "ACI wire-lock base gate",
)
# These run in every selected shard, each with its own stack.
GATED_RUNTIME_STEPS = (
    "Start dev stack",
    "Wait for Langfuse to serve",
    "Migrate the shared database",
    "Pytest",
)
PYTEST_AGGREGATE_EXPRESSIONS = {
    "pytest_selected": "${{ needs.changes.outputs.pytest }}",
    "pytest_deselect": "${{ needs.changes.outputs.pytest_deselect }}",
    "pytest_scope": "${{ needs.changes.outputs.pytest_scope }}",
    "shards_result": "${{ needs.python-pytest.result }}",
    "changes_result": "${{ needs.changes.result }}",
    "container_result": "${{ needs.python-container-fixtures.result }}",
    "ci_policy_selected": "${{ needs.changes.outputs.ci_policy }}",
    "ci_policy_result": "${{ needs.ci-policy.result }}",
    "dry_run_result": "${{ needs.python-dry-run-fixture.result }}",
}
PYTEST_STEP_EXPRESSIONS = {
    "shard": "${{ matrix.shard }}",
    "pytest_deselect": "${{ steps.python-runtime.outputs.pytest_deselect }}",
    "pytest_scope": "${{ steps.python-runtime.outputs.pytest_scope }}",
}
SHARD_RECORD_EXPRESSIONS = {
    "shard": "${{ matrix.shard }}",
    "pytest_selected": "${{ steps.python-runtime.outputs.pytest }}",
    "pytest_result": "${{ steps.pytest.outcome }}",
    "stack_result": "${{ steps.dev-stack.outcome }}",
    "pytest_deselect": "${{ steps.python-runtime.outputs.pytest_deselect }}",
    "pytest_scope": "${{ steps.python-runtime.outputs.pytest_scope }}",
}


@lru_cache(maxsize=1)
def _parse_workflow(contents: str) -> Any:
    return yaml.safe_load(contents)


def _python_job(job_id: str = "python") -> dict[str, Any]:
    workflow = _parse_workflow(WORKFLOW.read_text())
    job = workflow["jobs"][job_id]
    assert isinstance(job, dict)
    return deepcopy(job)


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


def _needs(job: dict[str, Any]) -> list[str]:
    needs = job.get("needs", [])
    return [needs] if isinstance(needs, str) else list(needs)


SHARDS_SELECTED = "${{ needs.changes.outputs.pytest == 'true' }}"
PYTEST_NOT_SELECTED = "steps.python-runtime.outputs.pytest == 'false'"
DECISION_STEP = "Decide whether compose and pytest are needed"


def test_python_job_sees_workflow_updates_and_isolates_mutations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workflow = tmp_path / "ci.yaml"
    contents = (
        "jobs:\n"
        "  python:\n"
        "    steps:\n"
        "      - name: initial\n"
        "        env:\n"
        "          VALUE: initial\n"
    )
    workflow.write_text(contents)
    monkeypatch.setattr(sys.modules[__name__], "WORKFLOW", workflow)

    job = _python_job()
    assert job == {"steps": [{"name": "initial", "env": {"VALUE": "initial"}}]}
    job["steps"][0]["env"]["VALUE"] = "mutated"
    job["steps"].append({"name": "extra"})
    assert _python_job() == {"steps": [{"name": "initial", "env": {"VALUE": "initial"}}]}

    workflow.write_text(contents.replace("initial", "updated"))
    updated = _python_job()
    assert updated == {"steps": [{"name": "updated", "env": {"VALUE": "updated"}}]}
    updated["steps"][0]["name"] = "mutated"
    assert _python_job() == {"steps": [{"name": "updated", "env": {"VALUE": "updated"}}]}


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
    workflow = repository / ".github/workflows/ci.yaml"
    workflow.parent.mkdir(parents=True)
    workflow.write_text(WORKFLOW.read_text())
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
    workflow = repository / ".github/workflows/ci.yaml"
    workflow.parent.mkdir(parents=True)
    workflow.write_text(WORKFLOW.read_text())
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
    assert job["needs"] == [
        "changes", "python-pytest", CONTAINER_JOB, DRY_RUN_JOB, "python-static", "ci-policy",
    ]
    assert job["if"] == "always()"

    # #4471: the static gates run in their own job, parallel to the shards.
    static = _python_job(STATIC_JOB)
    assert static["name"] == "Python static gates"
    assert _needs(static) == ["changes"]
    assert "if" not in static

    # #3858: the shards no longer boot runners just to skip. They wait for the
    # `changes` selection and run only when it selected pytest; the required
    # job above still reports, and its aggregate step accepts the skip.
    shards = _python_job("python-pytest")
    assert "changes" in _needs(shards)
    assert shards["if"] == SHARDS_SELECTED
    assert shards["strategy"]["fail-fast"] is False
    assert shards["strategy"]["matrix"]["shard"] == list(range(1, SHARD_COUNT + 1))
    assert shards["name"] == "Python pytest (shard ${{ matrix.shard }}/6)"


def test_python_shards_require_postgres_and_valkey_tests() -> None:
    # The matrix job covers every shard, so job-level env reaches all of them.
    env = _python_job("python-pytest")["env"]
    assert env["CI_REQUIRE_POSTGRES_TESTS"] == "1"
    assert env["CI_REQUIRE_VALKEY_TESTS"] == "1"


def test_python_job_gates_compose_and_pytest_on_selector_output(tmp_path: Path) -> None:
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
    assert (
        _string(stack, "run").strip()
        == "python3 scripts/wait-for-langfuse.py --start --timeout-seconds 480"
    )
    assert (
        _string(named["Wait for Langfuse to serve"], "run").strip()
        == "python3 scripts/wait-for-langfuse.py"
    )

    pytest_step = named["Pytest"]
    assert pytest_step["id"] == "pytest"
    command = ["uv", *_run_pytest_step(tmp_path, shard=2, deselect="")]
    assert command[:4] == ["uv", "run", "pytest", "-q"]
    assert command[command.index("--ci-shard") + 1] == f"2/{SHARD_COUNT}"
    # One automatic rerun, and every rerun test listed in the summary.
    assert command[command.index("--reruns") + 1] == "1"
    # pytest keeps only the last -r option, so one token must carry both the
    # rerun (R) and skip (s) report characters.
    report_tokens = [token for token in command if token.startswith("-r")]
    assert len(report_tokens) == 1, report_tokens
    assert {"R", "s"} <= set(report_tokens[0][2:])

    # The required job boots no stack and runs no suite of its own, and
    # neither does the static job.
    required = _named_steps()
    static = _named_steps(STATIC_JOB)
    for name in GATED_RUNTIME_STEPS:
        assert name not in required, name
        assert name not in static, name
    assert DECISION_STEP not in required
    assert "Sync workspace" not in required
    for step in _python_job()["steps"]:
        assert "astral-sh/setup-uv@" not in _string(step, "uses")
        assert "uv sync" not in _string(step, "run")
        assert "select_tiers.py" not in _string(step, "run")


def test_python_static_gates_stay_unconditional() -> None:
    named = _named_steps(STATIC_JOB)
    for name in ALWAYS_RUN_PYTHON_STEPS:
        step = named[name]
        assert "if" not in step, name
        assert PYTEST_SELECTED not in _string(step, "run")

    docs = named["Docs gate (catalog drift + agent contract + citations)"]
    assert "scripts/check-docs.sh" in _string(docs, "run")


def test_static_job_holds_exactly_the_moved_gates_in_order() -> None:
    """The static gates preserve order, with private imports immediately after sync."""
    static_names = [
        step["name"]
        for step in _python_job(STATIC_JOB)["steps"]
        if isinstance(step, dict) and isinstance(step.get("name"), str)
    ]
    expected = [
        "Install uv",
        "Back off before retrying Install uv",
        "Install uv (retry)",
        "Lockfile is up to date",
        "Sync workspace",
        PRIVATE_IMPORT_STEP,
        *MOVED_STATIC_STEPS[1:4],
        # ADR 0232 (#4610) added this gate after the move.
        "Contract migration timing gate",
        *MOVED_STATIC_STEPS[4:],
    ]
    assert static_names == expected

    required = _named_steps()
    for name in MOVED_STATIC_STEPS:
        assert name not in required, name
    assert "Sync workspace" not in required


def _assert_gate_placements(jobs: dict[str, Any]) -> None:
    for script, owner, commands in (
        ("check-released-upgrade.py", "python-pytest", [
            "uv run python scripts/check-released-upgrade.py --self-test",
            "uv run python scripts/check-released-upgrade.py",
        ]),
        ("check-private-imports.py", STATIC_JOB, [
            "uv run python3 scripts/check-private-imports.py --self-test",
            "uv run python3 scripts/check-private-imports.py",
        ]),
    ):
        matches = [
            (job_id, step)
            for job_id in ("python-pytest", "python", STATIC_JOB, CONTAINER_JOB)
            for step in jobs[job_id]["steps"]
            if script in _string(step, "run")
        ]
        assert len(matches) == 1, script
        job_id, step = matches[0]
        assert job_id == owner, script
        assert _string(step, "run").strip().splitlines() == commands
        steps = jobs[owner]["steps"]
        names = [item.get("name") for item in steps]
        if owner == STATIC_JOB:
            assert "if" not in step
            assert names.index("Sync workspace") + 1 == steps.index(step)
        else:
            condition = _string(step, "if").removeprefix("${{").removesuffix("}}").strip()
            assert condition == f"{PYTEST_SELECTED} && matrix.shard == 1"
            assert names.index("Wait for Langfuse to serve") < steps.index(step)
            assert steps.index(step) < names.index("Migrate the shared database")


def test_database_and_private_import_gates_have_one_owner() -> None:
    _assert_gate_placements(yaml.safe_load(WORKFLOW.read_text())["jobs"])


@pytest.mark.parametrize("mutation", ["missing-condition", "all-shards", "static-owner"])
def test_gate_placement_guard_rejects_execution_outside_shard_one(mutation: str) -> None:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    _assert_gate_placements(jobs)
    steps = jobs["python-pytest"]["steps"]
    gate = next(step for step in steps if step.get("name") == "Released database upgrade gate")
    if mutation == "missing-condition":
        del gate["if"]
    elif mutation == "all-shards":
        gate["if"] = PYTEST_SELECTED
    else:
        steps.remove(gate)
        jobs[STATIC_JOB]["steps"].append(gate)
    with pytest.raises(AssertionError):
        _assert_gate_placements(jobs)


def test_aci_wire_lock_base_gate_compares_against_the_event_branch() -> None:
    script = _string(_named_steps(STATIC_JOB)["ACI wire-lock base gate"], "run")
    branch = "${{ github.base_ref || github.ref_name }}"
    assert script.count(branch) == 2
    assert f'git fetch --no-tags --depth=1 origin "{branch}"' in script
    assert f'git show "origin/{branch}:packages/aci-protocol/schema/wire.lock"' in script
    assert "'main'" not in script


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


def _record_uv_step(
    tmp_path: Path,
    step: dict[str, Any],
    *,
    environment: dict[str, str],
    exit_code: int = 0,
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "uv-argv"
    stub = bin_dir / "uv"
    stub.write_text(
        '#!/usr/bin/env bash\nprintf "%s\\0" "$@" >> "$UV_ARGV_LOG"\n'
        f"exit {exit_code}\n"
    )
    stub.chmod(0o755)
    result = _run_bash(
        _string(step, "run"),
        {
            **environment,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "UV_ARGV_LOG": str(log),
        },
        tmp_path,
    )
    assert log.exists(), result.stdout + result.stderr
    recorded = log.read_bytes().split(b"\0")
    assert recorded.pop() == b""
    return result, [argument.decode() for argument in recorded]


def _without_runtime_exclusion(arguments: list[str], runtime: str) -> list[str]:
    remaining = []
    excluded = []
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        option, separator, value = argument.partition("=")
        if option in {"--ignore", "--ignore-glob", "--deselect"}:
            if not separator and index + 1 < len(arguments):
                value = arguments[index + 1]
            if value == runtime:
                excluded.append(value)
                index += 1 if separator else 2
                continue
        remaining.append(argument)
        index += 1
    assert excluded == [runtime], arguments
    return remaining


def _without_owned_runtime_exclusions(arguments: list[str]) -> list[str]:
    expected = {
        ("--deselect", ADMITTED_AGENT_RUNTIME),
        *(("--ignore", runtime) for runtime in (*CONTAINER_FILES, DRY_RUN_FILE)),
    }
    remaining = []
    excluded = []
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        option, separator, value = argument.partition("=")
        if option in {"--ignore", "--deselect"}:
            if not separator:
                assert index + 1 < len(arguments), arguments
                value = arguments[index + 1]
            if (option, value) in expected:
                excluded.append((option, value))
            else:
                remaining.append(argument)
                if not separator:
                    remaining.append(value)
            index += 1 if separator else 2
            continue
        remaining.append(argument)
        index += 1
    assert sorted(excluded) == sorted(expected), arguments
    return remaining


def _run_pytest_step(
    tmp_path: Path,
    *,
    shard: int,
    deselect: str,
    scope: str = "full",
    cli_files: tuple[str, ...] | None = None,
) -> list[str]:
    """Execute the real workflow Bash and record each argument at the uv boundary."""
    step = _named_steps("python-pytest")["Pytest"]
    script = _string(step, "run")
    assert "${{" not in script
    bindings = _bindings(step, PYTEST_STEP_EXPRESSIONS)
    if scope == "cli":
        relative = "tools/e2e-ci-selection/cli_pytest_files.txt"
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if cli_files is None:
            shutil.copyfile(REPO_ROOT / relative, destination)
        else:
            destination.write_text("\n".join(cli_files) + "\n")
    state = {"shard": str(shard), "pytest_deselect": deselect, "pytest_scope": scope}
    result, recorded = _record_uv_step(
        tmp_path, step, environment={bindings[key]: value for key, value in state.items()}
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return recorded


def _selection_expression(expression: str, selected: str, deselect: str) -> bool:
    """Evaluate the Boolean and string operations used by the actual job expressions."""
    # https://docs.github.com/en/actions/reference/workflows-and-actions/expressions
    source = expression.removeprefix("${{").removesuffix("}}").strip()
    for name, value in (("pytest_deselect", deselect), ("pytest", selected)):
        source = source.replace(f"needs.changes.outputs.{name}", repr(value))
    source = re.sub(r"!(?!=)", "not ", source).replace("&&", " and ").replace("||", " or ")

    def evaluate(node: ast.AST) -> Any:
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.BoolOp):
            values = [bool(evaluate(value)) for value in node.values]
            return all(values) if isinstance(node.op, ast.And) else any(values)
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            return not evaluate(node.operand)
        if isinstance(node, ast.Compare):
            assert len(node.ops) == 1 and isinstance(node.ops[0], (ast.Eq, ast.NotEq))
            equal = evaluate(node.left) == evaluate(node.comparators[0])
            return equal if isinstance(node.ops[0], ast.Eq) else not equal
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            args = [evaluate(argument) for argument in node.args]
            if node.func.id == "contains":
                return args[1].casefold() in args[0].casefold()
            if node.func.id == "format":
                return args[0].format(*args[1:])
        raise AssertionError(f"unsupported workflow selection expression: {ast.dump(node)}")

    return bool(evaluate(ast.parse(f"({source})", mode="eval").body))


CONTAINER_SELECTION_CASES = (
    ("", CONTAINER_FILES),
    (CONTAINER_FILES[0], (CONTAINER_FILES[1],)),
    (CONTAINER_FILES[1], (CONTAINER_FILES[0],)),
    (",".join(CONTAINER_FILES), ()),
    (",".join(reversed(CONTAINER_FILES)), ()),
    ("other.py," + CONTAINER_FILES[0] + ",another.py", (CONTAINER_FILES[1],)),
    ("other.py," + ",".join(CONTAINER_FILES) + ",another.py", ()),
    (",".join(path + "::test_example" for path in CONTAINER_FILES), CONTAINER_FILES),
    (",".join("prefix/" + path for path in CONTAINER_FILES), CONTAINER_FILES),
    (",".join(path + ".neighbor" for path in CONTAINER_FILES), CONTAINER_FILES),
    (",".join(path[:-3] for path in CONTAINER_FILES), CONTAINER_FILES),
)


def test_container_job_has_its_own_setup_without_a_shared_stack() -> None:
    job = _python_job(CONTAINER_JOB)
    assert job["name"] == "Python container fixtures (pytest)"
    assert _needs(job) == ["changes"]
    assert not job.get("continue-on-error", False)
    assert any(_string(step, "uses") == "actions/checkout@v7" for step in job["steps"])
    named = _named_steps(CONTAINER_JOB)
    generic = _named_steps("python-pytest")
    for name in (
        "Pull Docker Hub images through the mirror", "Install uv",
        "Back off before retrying Install uv", "Install uv (retry)", "Sync workspace",
    ):
        assert named[name] == generic[name], name
    scripts = "\n".join(_string(step, "run") for step in job["steps"])
    assert "wait-for-langfuse.py" not in scripts
    assert "alembic upgrade" not in scripts
    assert DECISION_STEP not in named
    assert not named["Pytest"].get("continue-on-error", False)
    assert named["Pytest"]["env"] == {
        "PYTEST_DESELECT": "${{ needs.changes.outputs.pytest_deselect }}",
        "PYTEST_SCOPE": "${{ needs.changes.outputs.pytest_scope }}",
    }
    manifest = (REPO_ROOT / "tools/e2e-ci-selection/cli_pytest_files.txt").read_text().splitlines()
    assert set(CONTAINER_FILES) <= set(manifest)


@pytest.mark.parametrize(("deselect", "files"), CONTAINER_SELECTION_CASES)
@pytest.mark.parametrize("selected", ["true", "false"])
def test_container_job_and_aggregate_use_complete_deselect_tokens(
    deselect: str, files: tuple[str, ...], selected: str
) -> None:
    expressions = (
        _python_job(CONTAINER_JOB)["if"],
        _named_steps()["Require pytest outcome to match selection"]["env"]["CONTAINER_SELECTED"],
    )
    for expression in expressions:
        assert _selection_expression(expression, selected, deselect) == (
            selected == "true" and bool(files)
        )


@pytest.mark.parametrize("copy", ["job", "aggregate"])
def test_container_selection_guard_rejects_substring_membership(copy: str) -> None:
    expression = (
        _python_job(CONTAINER_JOB)["if"] if copy == "job" else
        _named_steps()["Require pytest outcome to match selection"]["env"]["CONTAINER_SELECTED"]
    )
    assert _selection_expression(expression, "true", ",".join(CONTAINER_FILES)) is False
    mutated = re.sub(
        r"format\(\s*',\{0\},'\s*,\s*needs\.changes\.outputs\.pytest_deselect\s*\)",
        "needs.changes.outputs.pytest_deselect",
        expression,
    )
    for path in CONTAINER_FILES:
        mutated = mutated.replace(f"',{path},'", f"'{path}'")
    assert mutated != expression
    near_matches = ",".join(path + "::test_example" for path in CONTAINER_FILES)
    assert _selection_expression(expression, "true", near_matches) is True
    with pytest.raises(AssertionError):
        assert _selection_expression(mutated, "true", near_matches) is True


@pytest.mark.parametrize("scope", ["full", "cli"])
@pytest.mark.parametrize(
    ("deselect", "files"), [case for case in CONTAINER_SELECTION_CASES if case[1]]
)
def test_container_pytest_runs_only_selected_files(
    tmp_path: Path, scope: str, deselect: str, files: tuple[str, ...]
) -> None:
    step = _named_steps(CONTAINER_JOB)["Pytest"]
    assert "${{" not in _string(step, "run")
    result, argv = _record_uv_step(
        tmp_path, step, environment={"PYTEST_SCOPE": scope, "PYTEST_DESELECT": deselect}
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert argv == [
        "run", "pytest", "-q", "-n", "2", "--dist", "loadgroup",
        "--reruns", "1", "-rsR", "--durations=25", *files,
    ]


@pytest.mark.parametrize(("scope", "deselect"), [
    ("", ""), ("unknown", ""), ("FULL", ""),
    ("full", ",".join(CONTAINER_FILES)), ("cli", ",".join(reversed(CONTAINER_FILES))),
])
def test_container_pytest_rejects_invalid_scope_or_no_files_before_uv(
    tmp_path: Path, scope: str, deselect: str
) -> None:
    result = _run_bash(
        _string(_named_steps(CONTAINER_JOB)["Pytest"], "run"),
        {
            "PYTEST_SCOPE": scope, "PYTEST_DESELECT": deselect,
            "BASH_FUNC_uv%%": "() { echo unexpected-uv-invocation >&2; exit 99; }",
        },
        tmp_path,
    )
    assert result.returncode not in (0, 99), result.stdout + result.stderr
    assert "unexpected-uv-invocation" not in result.stdout + result.stderr
    assert result.stdout + result.stderr


@pytest.mark.parametrize("exit_code", [1, 5])
def test_container_pytest_propagates_failure(tmp_path: Path, exit_code: int) -> None:
    result, _ = _record_uv_step(
        tmp_path, _named_steps(CONTAINER_JOB)["Pytest"],
        environment={"PYTEST_SCOPE": "full", "PYTEST_DESELECT": ""}, exit_code=exit_code,
    )
    assert result.returncode == exit_code, result.stdout + result.stderr


@pytest.mark.parametrize(("event", "paths", "scope", "files"), [
    ("pull_request", (".github/workflows/ci.yaml",), "full", CONTAINER_FILES),
    ("pull_request", ("cli/src/main.rs",), "cli", (CONTAINER_FILES[0],)),
    ("pull_request", ("docs/guides/getting-started.md",), "full", ()),
    ("push", (), "full", CONTAINER_FILES),
    ("workflow_dispatch", (), "full", CONTAINER_FILES),
])
def test_container_caller_follows_real_selection_for_pr_push_and_dispatch(
    tmp_path: Path, event: str, paths: tuple[str, ...], scope: str, files: tuple[str, ...]
) -> None:
    if event != "pull_request":
        decision = tmp_path / "decision"
        decision.mkdir()
        calls = _run_decision_step(decision, "python-pytest", event)
        assert any("--push" in call.split() for call in calls)
    completed, output = _invoke_selector(tmp_path, *paths, push=event != "pull_request")
    assert completed.returncode == 0, completed.stderr
    selection = _outputs(output)
    assert selection["pytest_scope"] == scope
    for expression in (
        _python_job(CONTAINER_JOB)["if"],
        _named_steps()["Require pytest outcome to match selection"]["env"]["CONTAINER_SELECTED"],
    ):
        assert _selection_expression(
            expression, selection["pytest"], selection["pytest_deselect"]
        ) == bool(files)
    if files:
        caller = tmp_path / "caller"
        caller.mkdir()
        result, argv = _record_uv_step(
            caller, _named_steps(CONTAINER_JOB)["Pytest"],
            environment={"PYTEST_SCOPE": scope, "PYTEST_DESELECT": selection["pytest_deselect"]},
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert argv[-len(files):] == list(files)
        assert [path for path in argv if path in CONTAINER_FILES] == list(files)


def test_dry_run_job_matches_container_setup_and_runs_only_its_file() -> None:
    job = _python_job(DRY_RUN_JOB)
    assert job["name"] == "Python dry run fixture (pytest)"
    assert _needs(job) == ["changes"]
    assert "python-dry-run-fixture" in _needs(_python_job())
    named = _named_steps(DRY_RUN_JOB)
    container = _named_steps(CONTAINER_JOB)
    for name, step in container.items():
        if name == "Pytest":
            continue
        assert named[name] == step, name
    pytest_step = named["Pytest"]
    assert pytest_step["env"] == {
        "PYTEST_SCOPE": "${{ needs.changes.outputs.pytest_scope }}",
    }
    script = _string(pytest_step, "run")
    assert "uv run pytest -q -n 0 --reruns 1 -rsR --durations=25 " + DRY_RUN_FILE in script
    assert "wait-for-langfuse.py" not in script
    manifest = (REPO_ROOT / "tools/e2e-ci-selection/cli_pytest_files.txt").read_text().splitlines()
    assert DRY_RUN_FILE in manifest
    assert SHARD_NEIGHBOR in manifest
    assert SHARD_NEIGHBOR not in (*CONTAINER_FILES, DRY_RUN_FILE)


@pytest.mark.parametrize("copy", ["job", "aggregate"])
def test_dry_run_selection_rejects_a_substring_deselect_token(copy: str) -> None:
    expression = (
        _python_job(DRY_RUN_JOB)["if"] if copy == "job" else
        _named_steps()["Require pytest outcome to match selection"]["env"]["DRY_RUN_SELECTED"]
    )
    assert _selection_expression(expression, "true", "") is True
    assert _selection_expression(expression, "true", DRY_RUN_FILE) is False
    for near in (DRY_RUN_FILE + "::test_x", "prefix/" + DRY_RUN_FILE):
        assert _selection_expression(expression, "true", near) is True
    mutated = re.sub(
        r"format\(\s*',\{0\},'\s*,\s*needs\.changes\.outputs\.pytest_deselect\s*\)",
        "needs.changes.outputs.pytest_deselect",
        expression,
    )
    mutated = mutated.replace(f"',{DRY_RUN_FILE},'", f"'{DRY_RUN_FILE}'")
    assert mutated != expression
    with pytest.raises(AssertionError):
        assert _selection_expression(mutated, "true", DRY_RUN_FILE + "::test_x") is True


@pytest.mark.parametrize(("event", "paths", "selected"), [
    ("pull_request", (".github/workflows/ci.yaml",), True),
    ("pull_request", ("docs/guides/getting-started.md",), False),
    ("push", (), True),
    ("workflow_dispatch", (), True),
])
def test_dry_run_caller_follows_pr_push_and_dispatch_selection(
    tmp_path: Path, event: str, paths: tuple[str, ...], selected: bool
) -> None:
    completed, output = _invoke_selector(tmp_path, *paths, push=event != "pull_request")
    assert completed.returncode == 0, completed.stderr
    selection = _outputs(output)
    for expression in (
        _python_job(DRY_RUN_JOB)["if"],
        _named_steps()["Require pytest outcome to match selection"]["env"]["DRY_RUN_SELECTED"],
    ):
        assert _selection_expression(
            expression, selection["pytest"], selection["pytest_deselect"]
        ) is selected


@pytest.mark.parametrize("scope", ["full", "cli"])
def test_aggregate_rejects_dry_run_skipped_while_selected_or_run_while_deselected(
    tmp_path: Path, scope: str
) -> None:
    skipped = _run_pytest_aggregate(
        tmp_path / "skipped",
        ci_policy_selected="false",
        ci_policy_result="skipped",
        selected="true",
        container_result="success",
        dry_run_result="skipped",
        scope=scope,
        shards=_all_shards("true", "success", scope=scope),
    )
    assert skipped.returncode != 0, skipped.stdout + skipped.stderr
    assert "dry run" in (skipped.stdout + skipped.stderr).lower()
    ran = _run_pytest_aggregate(
        tmp_path / "ran",
        ci_policy_selected="false",
        ci_policy_result="skipped",
        selected="true",
        container_result="success",
        dry_run_result="success",
        deselect=DRY_RUN_FILE,
        scope=scope,
        shards=_all_shards("true", "success", DRY_RUN_FILE, scope),
    )
    assert ran.returncode != 0, ran.stdout + ran.stderr
    assert "dry run" in (ran.stdout + ran.stderr).lower()
    healthy = _run_pytest_aggregate(
        tmp_path / "healthy",
        ci_policy_selected="false",
        ci_policy_result="skipped",
        selected="true",
        container_result="success",
        dry_run_result="skipped",
        deselect=DRY_RUN_FILE,
        scope=scope,
        shards=_all_shards("true", "success", DRY_RUN_FILE, scope),
    )
    assert healthy.returncode == 0, healthy.stdout + healthy.stderr


def _admitted_agent_runtime_owner() -> tuple[dict[str, Any], dict[str, Any]]:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    owners = []
    for job_id, job in jobs.items():
        for step in job.get("steps", []):
            script = _string(step, "run")
            if ADMITTED_AGENT_RUNTIME not in script:
                continue
            arguments = shlex.split(script)
            if "pytest" not in arguments:
                continue
            for index, argument in enumerate(arguments):
                if argument.split("::", 1)[0] != ADMITTED_AGENT_RUNTIME:
                    continue
                if index and arguments[index - 1] in {"--ignore", "--deselect"}:
                    continue
                owners.append((job_id, step))
    assert [job_id for job_id, _ in owners] == [ADMITTED_AGENT_JOB], owners
    return jobs[ADMITTED_AGENT_JOB], owners[0][1]


def test_admitted_agent_runtime_has_one_required_provisioned_factory_owner() -> None:
    job, step = _admitted_agent_runtime_owner()
    assert job["if"] == "${{ needs.changes.outputs.factory == 'true' }}"
    assert "changes" in set(_needs(job))
    assert not job.get("continue-on-error", False)
    # Every leg must run to completion: one red leg cannot cancel the others,
    # and the matrix result is failure unless every leg passed.
    assert job["strategy"]["fail-fast"] is False
    assert job["strategy"]["matrix"] == {"shard": ADMITTED_AGENT_SHARDS}
    assert "if" not in step
    assert not step.get("continue-on-error", False)
    assert step["env"]["CASE_SHARD"] == (
        "${{ matrix.shard }}/" + str(len(ADMITTED_AGENT_SHARDS))
    )
    assert ADMITTED_AGENT_JOB in _needs(_python_job("e2e-required"))
    prior_steps = job["steps"][:job["steps"].index(step)]
    uses = [prior.get("uses", "") for prior in prior_steps]
    for action in ("astral-sh/setup-uv@", "azure/setup-helm@", "helm/kind-action@"):
        assert any(value.startswith(action) for value in uses), action
    assert any("/usr/share/dotnet" in prior.get("run", "") for prior in prior_steps)


@pytest.mark.parametrize("exit_code", [0, 1, 5])
def test_admitted_agent_factory_step_runs_whole_module_and_propagates_failure(
    tmp_path: Path, exit_code: int
) -> None:
    _, step = _admitted_agent_runtime_owner()
    completed, arguments = _record_uv_step(
        tmp_path, step, environment={"CASE_SHARD": "3/5"}, exit_code=exit_code
    )
    assert arguments[:2] == ["run", "pytest"]
    assert arguments.count(ADMITTED_AGENT_RUNTIME) == 1
    assert arguments[arguments.index("--case-shard") + 1] == "3/5"
    assert arguments[arguments.index("-n") + 1] == "0"
    assert all(
        argument in {
            "run", "pytest", "-q", "-v", "-vv", "-n", "0", "--case-shard", "3/5",
            ADMITTED_AGENT_RUNTIME,
        }
        for argument in arguments
    ), arguments
    assert completed.returncode == exit_code, completed.stdout + completed.stderr


def test_admitted_agent_shards_partition_the_module_in_collection_order() -> None:
    def collect(*extra: str) -> list[str]:
        completed = subprocess.run(
            [sys.executable, "-m", "pytest", "--collect-only", "-q", ADMITTED_AGENT_RUNTIME,
             *extra],
            cwd=REPO_ROOT, capture_output=True, text=True, check=False,
        )
        assert completed.returncode == 0, completed.stdout + completed.stderr
        return [line for line in completed.stdout.splitlines() if "::" in line]

    whole = collect()
    count = len(ADMITTED_AGENT_SHARDS)
    shards = [collect("--case-shard", f"{index}/{count}") for index in ADMITTED_AGENT_SHARDS]
    assert all(shards)
    assert [case for shard in shards for case in shard] == whole
    # The successor regression keeps the continuation cases that precede it
    # today in the same process, so its shard reproduces their order.
    relabel = f"{ADMITTED_AGENT_RUNTIME}::{RELABEL_REGRESSION}"
    [owner] = [shard for shard in shards if relabel in shard]
    position = owner.index(relabel)
    assert position >= 6
    assert all(
        "test_continuation_revalidates_current_authority" in case
        for case in owner[position - 6:position]
    )


@pytest.mark.parametrize("shard", range(1, SHARD_COUNT + 1))
@pytest.mark.parametrize("scope", ["full", "cli"])
@pytest.mark.parametrize("runtime", [ADMITTED_AGENT_RUNTIME, *CONTAINER_FILES, DRY_RUN_FILE])
def test_dedicated_runtime_is_excluded_from_every_generic_shard(
    tmp_path: Path, shard: int, scope: str, runtime: str
) -> None:
    neighbor = SHARD_NEIGHBOR
    arguments = _run_pytest_step(
        tmp_path,
        shard=shard,
        deselect="",
        scope=scope,
        cli_files=(runtime, neighbor),
    )
    without_exclusion = _without_runtime_exclusion(arguments, runtime)
    if scope == "cli":
        assert neighbor in without_exclusion
        if runtime in (*CONTAINER_FILES, DRY_RUN_FILE):
            assert runtime not in without_exclusion
            without_exclusion.append(runtime)
        else:
            assert runtime in without_exclusion

    for path in (runtime, neighbor):
        destination = tmp_path / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text("def test_collection_probe():\n    pass\n")
    (tmp_path / "examples/tests").mkdir(parents=True, exist_ok=True)
    config = tmp_path / "pytest.ini"
    config.write_text("[pytest]\ntestpaths = tools/factory-e2e/tests examples/tests\n")
    expected_neighbor = f"{neighbor}::test_collection_probe"
    expected_runtime = f"{runtime}::test_collection_probe"

    def collected(argv: list[str]) -> set[str]:
        # Retain every selection argument; omit only CI scheduling and retry
        # options so the real pytest collector runs once without extra plugins.
        selection = []
        index = 2
        while index < len(argv):
            argument = argv[index]
            if argument in {"-n", "--dist", "--ci-shard", "--reruns"}:
                index += 2
                continue
            selection.append(argument)
            index += 1
        result = subprocess.run(
            [
                sys.executable, "-m", "pytest", "-c", str(config),
                "--collect-only", *selection,
            ],
            cwd=tmp_path,
            env={
                **os.environ,
                "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
                "PYTEST_ADDOPTS": "",
                "PYTEST_PLUGINS": "",
            },
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return {
            line.strip()
            for line in result.stdout.splitlines()
            if line.strip().endswith("::test_collection_probe")
        }

    assert collected(arguments) == {expected_neighbor}
    assert collected(without_exclusion) == {expected_neighbor, expected_runtime}
    if scope == "cli":
        ignored = [*without_exclusion, "--ignore", runtime]
        assert collected(ignored) == {expected_neighbor, expected_runtime}


def _assert_owned_kind_modules_excluded(argv: list[str]) -> None:
    separate = [index + 1 for index, argument in enumerate(argv) if argument == "--ignore"]
    option_values = {
        index + 1
        for index, argument in enumerate(argv)
        if argument in {"--ignore", "--deselect"}
    }
    ignored = [
        argument.split("=", 1)[1] for argument in argv if argument.startswith("--ignore=")
    ]
    ignored += [argv[index] for index in separate]
    for module in (
        "tools/factory-e2e/tests/test_c4_initial_binding.py",
        "tools/factory-e2e/tests/test_c4_static_token_rotation.py",
        "tools/factory-e2e/tests/test_c4_upgrade_migration.py",
        "tools/factory-e2e/tests/test_c4_held_cancellation.py",
        "tools/factory-e2e/tests/test_c4_cluster_up.py",
        "tools/factory-e2e/tests/test_c4_forge_instances.py",
        *CONTAINER_FILES,
        DRY_RUN_FILE,
    ):
        assert ignored.count(module) == 1, module
        assert all(
            index in option_values for index, argument in enumerate(argv) if argument == module
        ), f"{module} must not be selected as a positional pytest file"


@pytest.mark.parametrize("shard", range(1, SHARD_COUNT + 1))
@pytest.mark.parametrize("scope", ("full", "cli"))
@pytest.mark.parametrize("deselect", ("", *CONTAINER_FILES, ",".join(CONTAINER_FILES)))
def test_ordinary_python_shards_exclude_the_owned_kind_modules(
    tmp_path: Path,
    shard: int,
    scope: str,
    deselect: str,
) -> None:
    argv = _run_pytest_step(tmp_path, shard=shard, deselect=deselect, scope=scope)
    _assert_owned_kind_modules_excluded(argv)
    assert argv[argv.index("--ci-shard") + 1] == f"{shard}/{SHARD_COUNT}"


@pytest.mark.parametrize("scope", ("full", "cli"))
@pytest.mark.parametrize("module", CONTAINER_FILES)
def test_owned_kind_module_guard_rejects_positional_files(
    tmp_path: Path, scope: str, module: str
) -> None:
    argv = _run_pytest_step(tmp_path, shard=1, deselect=module, scope=scope)
    _assert_owned_kind_modules_excluded(argv)
    with pytest.raises(AssertionError, match="must not be selected as a positional pytest file"):
        _assert_owned_kind_modules_excluded([*argv, module])


def _record_shard(
    tmp_path: Path, shard: int, selected: str, outcome: str, stack: str, deselect: str, scope: str
) -> None:
    """Run the shard job's real record step, as the shard would."""
    step = _named_steps("python-pytest")["Record shard result"]
    assert _string(step, "if") == "always()"
    bindings = _bindings(step, SHARD_RECORD_EXPRESSIONS)
    state = {
        "shard": str(shard),
        "pytest_selected": selected,
        "pytest_result": outcome,
        "stack_result": stack,
        "pytest_deselect": deselect,
        "pytest_scope": scope,
    }
    workdir = tmp_path / f"shard-{shard}"
    workdir.mkdir()
    result = _run_bash(_string(step, "run"), {bindings[k]: v for k, v in state.items()}, workdir)
    assert result.returncode == 0, result.stderr
    upload = _named_steps("python-pytest")["Upload shard result"]
    assert _string(upload, "if") == "always()"
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
    container_result: str,
    ci_policy_selected: str,
    ci_policy_result: str,
    dry_run_result: str | None = None,
    shards: dict[int, tuple[str, str, str, str, str]] | None = None,
    shards_result: str = "success",
    results_dir: bool = True,
    changes_result: str = "success",
    deselect: str = "",
    scope: str = "full",
) -> subprocess.CompletedProcess[str]:
    """Record each shard, then run the required job's real aggregate step.

    ``shards`` maps a shard index to (selected, pytest, stack, deselect, scope) as that shard
    saw them; an index left out never reported. ``results_dir=False`` leaves
    the download directory absent, as it is when the download step is skipped
    (#3858). Selection inputs come only from ``changes``. Container selection
    is evaluated from the real aggregate expression, independently of its result.
    """
    step = _named_steps()["Require pytest outcome to match selection"]
    assert _string(step, "if") == "success()"
    assert step["env"]["SHARD_COUNT"] == str(SHARD_COUNT)
    download = _named_steps()["Download shard results"]
    assert step["env"]["SHARD_RESULTS_DIR"] == download["with"]["path"]
    tmp_path.mkdir(parents=True, exist_ok=True)
    if results_dir or shards:
        (tmp_path / download["with"]["path"]).mkdir(parents=True, exist_ok=True)
    for shard, (shard_selected, outcome, stack, shard_deselect, shard_scope) in (
        shards or {}
    ).items():
        _record_shard(tmp_path, shard, shard_selected, outcome, stack, shard_deselect, shard_scope)
    bindings = _bindings(step, PYTEST_AGGREGATE_EXPRESSIONS)
    assert bindings == {name: name.upper() for name in PYTEST_AGGREGATE_EXPRESSIONS}
    assert not any(name.startswith("CHANGES_") and name != "CHANGES_RESULT" for name in step["env"])
    state = {
        "pytest_selected": selected,
        "shards_result": shards_result,
        "changes_result": changes_result,
        "pytest_deselect": deselect,
        "pytest_scope": scope,
        "container_result": container_result,
        "ci_policy_selected": ci_policy_selected,
        "ci_policy_result": ci_policy_result,
        "dry_run_result": "placeholder",
    }
    literals = {k: v for k, v in step["env"].items() if "${{" not in v}
    environment = literals | {bindings[k]: v for k, v in state.items()}
    environment["CONTAINER_SELECTED"] = str(
        _selection_expression(step["env"]["CONTAINER_SELECTED"], selected, deselect)
    ).lower()
    dry_run_selected = _selection_expression(
        step["env"]["DRY_RUN_SELECTED"], selected, deselect
    )
    environment["DRY_RUN_SELECTED"] = str(dry_run_selected).lower()
    environment["DRY_RUN_RESULT"] = (
        ("success" if dry_run_selected else "skipped")
        if dry_run_result is None
        else dry_run_result
    )
    return _run_bash(_string(step, "run"), environment, tmp_path)


def _all_shards(
    selected: str, outcome: str, deselect: str = "", scope: str = "full"
) -> dict[int, tuple[str, str, str, str, str]]:
    return {
        shard: (selected, outcome, outcome, deselect, scope) for shard in range(1, SHARD_COUNT + 1)
    }


def test_ci_policy_job_runs_after_changes_without_services() -> None:
    job = _python_job("ci-policy")
    assert job["name"] == "CI policy (workflow meta-tests)"
    assert job["needs"] == ["changes"]
    assert job["if"] == "${{ needs.changes.outputs.ci_policy == 'true' }}"
    assert "services" not in job
    assert not job.get("continue-on-error", False)
    steps = job["steps"]
    assert len(steps) == 6
    assert steps[0]["uses"] == "actions/checkout@v7"
    assert steps[0]["with"] == {"fetch-depth": 0, "persist-credentials": False}
    assert [step["name"] for step in steps[1:]] == [
        "Install uv", "Back off before retrying Install uv", "Install uv (retry)",
        "Sync workspace", "Pytest",
    ]
    install, backoff, retry, sync, tests = steps[1:]
    uv_action = "astral-sh/setup-uv@1c37ad07a6a961277cf70c0d37d6f313000f5884"
    assert install["uses"] == retry["uses"] == uv_action
    assert install["with"] == retry["with"] == {"python-version": "3.13"}
    assert install["id"] == "install_uv"
    assert install["continue-on-error"] is True
    assert backoff["if"] == retry["if"] == "steps.install_uv.outcome == 'failure'"
    assert "sleep 15" in backoff["run"]
    assert sync["run"] == "uv sync"
    assert not sync.get("continue-on-error", False)
    assert not tests.get("continue-on-error", False)
    assert "if" not in tests
    assert _bindings(tests, {"files": "${{ needs.changes.outputs.ci_policy_files }}"})
    assert "${{" not in tests["run"]
    outputs = _python_job("changes")["outputs"]
    for key in ("ci_policy", "ci_policy_files"):
        assert outputs[key] == "${{ steps.filter.outputs." + key + " }}"


@pytest.mark.parametrize("exit_code", [0, 1, 5])
def test_ci_policy_pytest_passes_exact_files_and_propagates_failure(
    tmp_path: Path, exit_code: int
) -> None:
    files = (
        "tools/ci-retry-gate/tests/test_retry_eligibility.py",
        "tools/e2e-ci-selection/tests/test_e2e_ci_selection.py",
    )
    for path in files:
        destination = tmp_path / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text("def test_policy_probe():\n    pass\n")
    step = _named_steps("ci-policy")["Pytest"]
    bindings = _bindings(step, {"files": "${{ needs.changes.outputs.ci_policy_files }}"})
    result, arguments = _record_uv_step(
        tmp_path, step, environment={bindings["files"]: ",".join(files)}, exit_code=exit_code
    )
    assert arguments == ["run", "pytest", "-q", "-n", "4", "-rsR", *files]
    assert result.returncode == exit_code, result.stdout + result.stderr


@pytest.mark.parametrize("files", [
    "", ",", ",tools/sample/tests/test_policy.py", "tools/sample/tests/test_policy.py,",
    "tools/sample/tests/test_policy.py,,tools/sample/tests/test_policy.py",
    "tools/sample/tests/test_policy.py tools/sample/tests/test_policy.py",
    "tools/sample/tests/test_policy.py\n", "--help", "/tmp/test_policy.py",
    "../test_policy.py", "tools/sample/tests/../tests/test_policy.py",
    "tools/sample/tests/test_*.py", "tools/sample/tests/missing.py", "tools/sample/tests",
])
def test_ci_policy_pytest_rejects_bad_file_lists_before_uv(tmp_path: Path, files: str) -> None:
    destination = tmp_path / "tools/sample/tests/test_policy.py"
    destination.parent.mkdir(parents=True)
    destination.write_text("def test_policy_probe():\n    pass\n")
    step = _named_steps("ci-policy")["Pytest"]
    bindings = _bindings(step, {"files": "${{ needs.changes.outputs.ci_policy_files }}"})
    bin_dir, log = _recording_stubs(tmp_path, "uv")
    result = _run_bash(
        _string(step, "run"),
        {"PATH": f"{bin_dir}:{os.environ['PATH']}", bindings["files"]: files},
        tmp_path,
    )
    assert result.returncode != 0, result.stdout + result.stderr
    assert not log.exists(), "invalid policy files reached pytest"


@pytest.mark.parametrize("event", ["push", "workflow_dispatch"])
def test_ci_policy_changes_routes_non_pr_events_through_push(tmp_path: Path, event: str) -> None:
    step = next(step for step in _python_job("changes")["steps"] if step.get("id") == "filter")
    script = _substitute_expressions(step["run"], {
        "${{ github.event_name }}": event,
        "${{ github.ref }}": "refs/heads/next",
        "${{ github.base_ref }}": "",
    })
    bin_dir, log = _recording_stubs(tmp_path, "uv", "git")
    result = _run_bash(script, {"PATH": f"{bin_dir}:{os.environ['PATH']}"}, tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    calls = log.read_text().splitlines()
    assert len(calls) == 1, calls
    command = shlex.split(calls[0])
    assert command[:7] == [
        "uv", "run", "--no-project", "--with", "pyyaml==6.0.3", "python",
        "tools/e2e-ci-selection/select_tiers.py",
    ]
    assert "--push" in command
    assert ("--omit-kind" in command) == (event == "push")
    completed, output = _invoke_selector(tmp_path, push=True)
    assert completed.returncode == 0, completed.stderr
    entries = yaml.safe_load(REGISTRY.read_text())["ci_policy_tests"]
    assert _outputs(output)["ci_policy"] == "true"
    assert _outputs(output)["ci_policy_files"] == ",".join(sorted(entries))


@pytest.mark.parametrize("shard", range(1, SHARD_COUNT + 1))
@pytest.mark.parametrize("scope", ["full", "cli"])
def test_ci_policy_exclusions_collect_only_neighbors_on_every_shard(
    tmp_path: Path, shard: int, scope: str
) -> None:
    entries = yaml.safe_load(REGISTRY.read_text())["ci_policy_tests"]
    neighbors = ("tools/ci-retry-gate-other/tests/test_neighbor.py",)
    for path in (*entries, *neighbors):
        destination = tmp_path / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text("def test_policy_collection_probe():\n    pass\n")
    arguments = _run_pytest_step(
        tmp_path, shard=shard, scope=scope, deselect="",
        cli_files=(*sorted(entries), *neighbors),
    )
    for directory in CI_POLICY_DIRS:
        for option, value in (
            ("--ignore", directory), ("--ignore-glob", f"{directory}/tests/*"),
        ):
            assert arguments.count(value) == 1, arguments
            assert arguments[arguments.index(value) - 1] == option, arguments
    if scope == "cli":
        assert not set(entries).intersection(arguments), arguments
        assert all(path in arguments for path in neighbors)
    config = tmp_path / "pytest.ini"
    roots = [f"{directory}/tests" for directory in CI_POLICY_DIRS]
    roots += [str(Path(path).parent) for path in neighbors]
    config.write_text(
        "[pytest]\ntestpaths =\n" + "".join(f"    {root}\n" for root in roots)
        + "addopts = --import-mode=importlib\n"
    )

    def collected(argv: list[str]) -> set[str]:
        selection = []
        index = 2
        while index < len(argv):
            if argv[index] in {"-n", "--dist", "--ci-shard", "--reruns"}:
                index += 2
                continue
            selection.append(argv[index])
            index += 1
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "-c", str(config), "--collect-only", *selection],
            cwd=tmp_path,
            env={
                **os.environ, "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
                "PYTEST_ADDOPTS": "", "PYTEST_PLUGINS": "",
            },
            capture_output=True, text=True, check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return {
            line.strip() for line in result.stdout.splitlines()
            if line.strip().endswith("::test_policy_collection_probe")
        }

    expected = {path + "::test_policy_collection_probe" for path in neighbors}
    assert collected(arguments) == expected
    directory = "tools/e2e-ci-selection"
    targets = sorted(path for path in entries if path.startswith(directory + "/"))
    assert targets
    if scope == "full":
        mutated = _without_runtime_exclusion(arguments, f"{directory}/tests/*")
        exposed = targets
    else:
        exposed = targets[:1]
        mutated = [*arguments, *exposed]
    assert collected(mutated) == expected | {
        path + "::test_policy_collection_probe" for path in exposed
    }


@pytest.mark.parametrize("selected", ["true", "false"])
@pytest.mark.parametrize("policy_selected", ["true", "false", "", "TRUE", "unknown"])
@pytest.mark.parametrize("policy_result", ["success", "skipped", "failure", "cancelled", ""])
def test_ci_policy_aggregate_accepts_only_the_selected_policy_outcome(
    tmp_path: Path, selected: str, policy_selected: str, policy_result: str
) -> None:
    result = _run_pytest_aggregate(
        tmp_path, ci_policy_selected=policy_selected, ci_policy_result=policy_result,
        selected=selected, container_result="success" if selected == "true" else "skipped",
        shards=_all_shards("true", "success") if selected == "true" else None,
        shards_result="success" if selected == "true" else "skipped",
        results_dir=selected == "true",
    )
    accepted = (policy_selected, policy_result) in {("true", "success"), ("false", "skipped")}
    assert (result.returncode == 0) == accepted, result.stdout + result.stderr
    if not accepted:
        assert "policy" in (result.stdout + result.stderr).lower()


def _assert_policy_failure_rejected(tmp_path: Path, selected: str) -> None:
    result = _run_pytest_aggregate(
        tmp_path,
        ci_policy_selected="true",
        ci_policy_result="failure",
        selected=selected,
        container_result="success" if selected == "true" else "skipped",
        shards=_all_shards("true", "success") if selected == "true" else None,
        shards_result="success" if selected == "true" else "skipped",
        results_dir=selected == "true",
    )
    assert result.returncode != 0, "required Python check accepted failed CI policy"


@pytest.mark.parametrize("selected", ["true", "false"])
def test_ci_policy_aggregate_mutation_exposes_bypassed_result_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, selected: str
) -> None:
    _assert_policy_failure_rejected(tmp_path / "enforced", selected)
    named_steps = _named_steps

    def bypassed(job_id: str = "python") -> dict[str, dict[str, Any]]:
        steps = named_steps(job_id)
        if job_id == "python":
            step = steps["Require pytest outcome to match selection"]
            step["run"] = "CI_POLICY_RESULT=success\n" + step["run"]
        return steps

    monkeypatch.setattr(sys.modules[__name__], "_named_steps", bypassed)
    with pytest.raises(AssertionError, match="accepted failed CI policy"):
        _assert_policy_failure_rejected(tmp_path / "bypassed", selected)


@pytest.mark.parametrize("scope", ["full", "cli"])
@pytest.mark.parametrize(("selected", "deselect", "container_selected"), [
    ("true", "", True),
    ("true", CONTAINER_FILES[0], True),
    ("true", CONTAINER_FILES[1], True),
    ("true", ",".join(CONTAINER_FILES), False),
    ("false", "", False),
])
@pytest.mark.parametrize("container_result", ["success", "skipped", "failure", "cancelled", ""])
def test_aggregate_requires_the_selected_container_outcome_before_any_skip_exit(
    tmp_path: Path, scope: str, selected: str, deselect: str,
    container_selected: bool, container_result: str,
) -> None:
    result = _run_pytest_aggregate(
        tmp_path, ci_policy_selected="false", ci_policy_result="skipped",
        selected=selected,
        container_result=container_result,
        deselect=deselect,
        scope=scope,
        shards=_all_shards("true", "success", deselect, scope) if selected == "true" else None,
        shards_result="success" if selected == "true" else "skipped",
        results_dir=selected == "true",
    )
    expected = "success" if container_selected else "skipped"
    assert (result.returncode == 0) == (container_result == expected), result.stdout + result.stderr
    if container_result != expected:
        assert "container" in (result.stdout + result.stderr).lower()


@pytest.mark.parametrize("selected", ["", "TRUE", "unknown"])
def test_aggregate_rejects_invalid_changes_selection(tmp_path: Path, selected: str) -> None:
    result = _run_pytest_aggregate(
        tmp_path,
        ci_policy_selected="false",
        ci_policy_result="skipped",
        selected=selected,
        container_result="skipped",
        shards_result="skipped",
        results_dir=False,
    )
    assert result.returncode != 0, result.stdout + result.stderr


@pytest.mark.parametrize("selected", ["true", "false"])
@pytest.mark.parametrize("scope", ["", "unknown"])
def test_aggregate_rejects_invalid_changes_scope(
    tmp_path: Path, selected: str, scope: str
) -> None:
    result = _run_pytest_aggregate(
        tmp_path, ci_policy_selected="false", ci_policy_result="skipped", selected=selected,
        container_result="success" if selected == "true" else "skipped",
        scope=scope,
        shards=_all_shards("true", "success", scope=scope) if selected == "true" else None,
        shards_result="success" if selected == "true" else "skipped",
    )
    assert result.returncode != 0, result.stdout + result.stderr
    assert "scope" in (result.stdout + result.stderr).lower()


@pytest.mark.parametrize("field", ["pytest", "stack"])
@pytest.mark.parametrize("outcome", ["failure", "cancelled", "skipped", ""])
def test_aggregate_rejects_unsuccessful_record_even_when_matrix_succeeded(
    tmp_path: Path, field: str, outcome: str
) -> None:
    shards = _all_shards("true", "success")
    shards[6] = (
        "true", outcome if field == "pytest" else "success",
        outcome if field == "stack" else "success", "", "full",
    )
    result = _run_pytest_aggregate(
        tmp_path,
        ci_policy_selected="false",
        ci_policy_result="skipped",
        selected="true",
        container_result="success",
        shards=shards,
    )
    assert result.returncode != 0, result.stdout + result.stderr
    assert "shard 6" in result.stderr


def test_aggregate_rejects_record_with_the_wrong_shard_identity(tmp_path: Path) -> None:
    healthy = _run_pytest_aggregate(
        tmp_path,
        ci_policy_selected="false",
        ci_policy_result="skipped",
        selected="true",
        container_result="success",
        shards=_all_shards("true", "success"),
    )
    assert healthy.returncode == 0, healthy.stdout + healthy.stderr
    directory = _named_steps()["Download shard results"]["with"]["path"]
    record = tmp_path / directory / "shard-6.txt"
    record.write_text(record.read_text().replace("shard=6 ", "shard=5 "))
    result = _run_pytest_aggregate(
        tmp_path,
        ci_policy_selected="false",
        ci_policy_result="skipped",
        selected="true",
        container_result="success",
    )
    assert result.returncode != 0, result.stdout + result.stderr
    assert "shard 6" in result.stderr


def test_aggregate_rejects_mutated_selected_container_skip(tmp_path: Path) -> None:
    healthy = _run_pytest_aggregate(
        tmp_path,
        ci_policy_selected="false",
        ci_policy_result="skipped",
        selected="true",
        container_result="success",
        shards=_all_shards("true", "success"),
    )
    assert healthy.returncode == 0, healthy.stdout + healthy.stderr
    skipped = _run_pytest_aggregate(
        tmp_path,
        ci_policy_selected="false",
        ci_policy_result="skipped",
        selected="true",
        container_result="skipped",
    )
    assert skipped.returncode != 0, skipped.stdout + skipped.stderr
    assert "container" in (skipped.stdout + skipped.stderr).lower()
    restored = _run_pytest_aggregate(
        tmp_path,
        ci_policy_selected="false",
        ci_policy_result="skipped",
        selected="true",
        container_result="success",
    )
    assert restored.returncode == 0, restored.stdout + restored.stderr


def test_pytest_aggregator_accepts_matching_deselect(tmp_path: Path) -> None:
    deselect = "examples/tests/test_sre_bot_observability_runtime.py"
    result = _run_pytest_aggregate(
        tmp_path, ci_policy_selected="false", ci_policy_result="skipped",
        container_result="success",
        selected="true",
        deselect=deselect,
        shards=_all_shards("true", "success", deselect),
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("shard", range(1, SHARD_COUNT + 1))
def test_pytest_aggregator_rejects_independent_shard_deselect_disagreement(
    tmp_path: Path, shard: int
) -> None:
    deselect = "examples/tests/test_sre_bot_observability_runtime.py"
    shards = _all_shards("true", "success")
    shards[shard] = ("true", "success", "success", deselect, "full")
    result = _run_pytest_aggregate(
        tmp_path, ci_policy_selected="false", ci_policy_result="skipped",
        container_result="success",
        selected="true",
        shards=shards,
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert f"shard {shard}" in result.stderr


def test_pytest_aggregator_rejects_shard_deselect_disagreement(tmp_path: Path) -> None:
    deselect = "examples/tests/test_sre_bot_observability_runtime.py"
    shards = _all_shards("true", "success", deselect)
    shards[2] = ("true", "success", "success", "", "full")
    result = _run_pytest_aggregate(
        tmp_path,
        ci_policy_selected="false",
        ci_policy_result="skipped",
        container_result="success",
        selected="true",
        deselect=deselect,
        shards=shards,
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert "shard 2" in result.stderr


@pytest.mark.parametrize("results_dir", [True, False])
def test_pytest_aggregator_accepts_docs_only_skips(tmp_path: Path, results_dir: bool) -> None:
    """#3858: an unselected run skips the shard job, so no shard reports."""
    completed, output = _invoke_selector(tmp_path, "docs/guides/getting-started.md")
    assert completed.returncode == 0, completed.stderr
    selected = _outputs(output)["pytest"]
    assert selected == "false"
    result = _run_pytest_aggregate(
        tmp_path / "run", ci_policy_selected="false", ci_policy_result="skipped",
        container_result="skipped",
        selected=selected,
        shards_result="skipped",
        results_dir=results_dir,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_pytest_aggregator_accepts_a_version_only_release_prep(tmp_path: Path) -> None:
    """#3858: the five #3845 paths skip pytest and the required check passes."""
    cargo = tomllib.loads((REPO_ROOT / "cli" / "Cargo.toml").read_text())
    version = f"v{cargo['package']['version']}"
    completed, output = _invoke_selector(
        tmp_path,
        "charts/curie/Chart.yaml",
        "cli/Cargo.toml",
        "cli/Cargo.lock",
        "docs/architecture-atlas/versions.json",
        f"docs/architecture-atlas/snapshots/{version}.json",
    )
    assert completed.returncode == 0, completed.stderr
    outputs = _outputs(output)
    assert outputs["version_only"] == "true"
    assert outputs["pytest"] == "false"
    assert not _selection_expression(
        _python_job(CONTAINER_JOB)["if"], outputs["pytest"], outputs["pytest_deselect"]
    )
    result = _run_pytest_aggregate(
        tmp_path / "run",
        ci_policy_selected="false",
        ci_policy_result="skipped",
        container_result="skipped",
        selected=outputs["pytest"],
        shards_result="skipped",
        results_dir=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_pytest_aggregator_rejects_unselected_shards_that_succeeded(tmp_path: Path) -> None:
    """#3858: an unselected run must not show the shard job as having run."""
    result = _run_pytest_aggregate(
        tmp_path,
        ci_policy_selected="false",
        ci_policy_result="skipped",
        container_result="skipped",
        selected="false",
        shards_result="success",
    )
    assert result.returncode != 0


def test_pytest_aggregator_rejects_an_unselected_skip_with_a_stray_record(
    tmp_path: Path,
) -> None:
    """#3858: a shard record on an unselected run means the selections disagree."""
    result = _run_pytest_aggregate(
        tmp_path, ci_policy_selected="false", ci_policy_result="skipped",
        container_result="skipped",
        selected="false",
        shards={1: ("false", "skipped", "skipped", "", "full")},
        shards_result="skipped",
    )
    assert result.returncode != 0


@pytest.mark.parametrize("changes_result", ["failure", "cancelled", "skipped"])
def test_pytest_aggregator_rejects_a_changes_job_that_did_not_succeed(
    tmp_path: Path, changes_result: str
) -> None:
    """#3858: a failed `changes` job skips the shards; that skip must not go green."""
    result = _run_pytest_aggregate(
        tmp_path, ci_policy_selected="false", ci_policy_result="skipped",
        container_result="skipped",
        selected="false",
        shards_result="skipped",
        results_dir=False,
        changes_result=changes_result,
    )
    assert result.returncode != 0


def test_pytest_aggregator_rejects_selected_changes_with_unselected_records(
    tmp_path: Path,
) -> None:
    """A shard cannot skip tests selected by changes."""
    result = _run_pytest_aggregate(
        tmp_path, ci_policy_selected="false", ci_policy_result="skipped",
        container_result="success",
        selected="true",
        shards=_all_shards("false", "skipped"),
        shards_result="success",
    )
    assert result.returncode != 0


def test_pytest_aggregator_rejects_unselected_changes_with_selected_records(
    tmp_path: Path,
) -> None:
    """Successful shard records cannot override a skip selected by changes."""
    result = _run_pytest_aggregate(
        tmp_path, ci_policy_selected="false", ci_policy_result="skipped",
        container_result="skipped",
        selected="false",
        shards=_all_shards("true", "success"),
        shards_result="skipped",
    )
    assert result.returncode != 0


def test_pytest_aggregator_accepts_unselected_when_changes_agrees(tmp_path: Path) -> None:
    """#3858 liveness: a healthy docs-only run still goes green."""
    result = _run_pytest_aggregate(
        tmp_path, ci_policy_selected="false", ci_policy_result="skipped",
        container_result="skipped",
        selected="false",
        shards_result="skipped",
        results_dir=False,
        changes_result="success",
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_pytest_aggregator_rejects_selected_with_shards_skipped(tmp_path: Path) -> None:
    """#3858: a selected run whose shard job was skipped never ran pytest."""
    result = _run_pytest_aggregate(
        tmp_path,
        ci_policy_selected="false",
        ci_policy_result="skipped",
        container_result="success",
        selected="true",
        shards_result="skipped",
        results_dir=False,
    )
    assert result.returncode != 0


def test_shard_download_runs_only_when_pytest_was_selected() -> None:
    """#3858: an unselected run has no shard artifacts to download."""
    download = _named_steps()["Download shard results"]
    assert _string(download, "if") == "needs.changes.outputs.pytest == 'true'"


def test_release_tests_run_in_the_static_job_when_pytest_is_skipped() -> None:
    """#3858: a version-only diff still runs the release tests on the new commit.

    The shards cover release/tests whenever pytest is selected, so the step
    runs only on the skip path. #4471: it lives in the static job, so it reads
    the selection from `changes` rather than from a step in the same job.
    """
    step = _named_steps(STATIC_JOB)["Release tests"]
    assert _string(step, "if") == "needs.changes.outputs.pytest == 'false'"
    assert _string(step, "run").strip() == "uv run pytest -q release/tests"
    assert "Release tests" not in _named_steps()


def test_static_gate_requirement_step_sits_before_the_shard_download() -> None:
    """#4471: the python job's step order puts the static check first."""
    names = [
        step["name"]
        for step in _python_job()["steps"]
        if isinstance(step, dict) and isinstance(step.get("name"), str)
    ]
    assert names.index(STATIC_REQUIREMENT_STEP) + 1 == names.index("Download shard results")


@pytest.mark.parametrize("static_result", ["success", "failure", "skipped", "cancelled"])
def test_required_job_passes_only_when_the_static_job_succeeded(
    tmp_path: Path, static_result: str
) -> None:
    """#4471: run the real step; only a success result lets the required job pass."""
    step = _named_steps()[STATIC_REQUIREMENT_STEP]
    # The default success() condition is what makes this step fail the job, so it
    # must not be gated or made non-fatal.
    assert "if" not in step
    assert "continue-on-error" not in step
    bindings = _bindings(step, {"static_result": "${{ needs.python-static.result }}"})
    assert bindings == {"static_result": "STATIC_RESULT"}
    result = _run_bash(_string(step, "run"), {"STATIC_RESULT": static_result}, tmp_path)
    if static_result == "success":
        assert result.returncode == 0, result.stdout + result.stderr
    else:
        assert result.returncode != 0
        assert static_result in result.stdout + result.stderr


def test_changes_job_exports_pytest_and_version_only() -> None:
    """#3858: downstream jobs read the selection from `changes`."""
    outputs = _python_job("changes")["outputs"]
    assert outputs["pytest"] == "${{ steps.filter.outputs.pytest }}"
    assert outputs["version_only"] == "${{ steps.filter.outputs.version_only }}"


def _substitute_expressions(script: str, values: dict[str, str]) -> str:
    for expression, value in values.items():
        script = script.replace(expression, value)
    assert "${{" not in script, script
    return script


def _recording_stubs(tmp_path: Path, *names: str) -> tuple[Path, Path]:
    """Put stub executables first on PATH; each appends its argv to a log."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "calls.log"
    for name in names:
        stub = bin_dir / name
        stub.write_text(f'#!/usr/bin/env bash\necho "{name} $*" >> "{log}"\n')
        stub.chmod(0o755)
    return bin_dir, log


def _run_decision_step(tmp_path: Path, job_id: str, event: str) -> list[str]:
    script = _substitute_expressions(
        _string(_named_steps(job_id)[DECISION_STEP], "run"),
        {
            "${{ github.event_name }}": event,
            "${{ github.base_ref || 'main' }}": "main",
        },
    )
    bin_dir, log = _recording_stubs(tmp_path, "uv", "git")
    result = _run_bash(script, {"PATH": f"{bin_dir}:{os.environ['PATH']}"}, tmp_path)
    assert result.returncode == 0, result.stderr
    return log.read_text().splitlines() if log.exists() else []


@pytest.mark.parametrize("job_id", ["python-pytest"])
@pytest.mark.parametrize("event", ["workflow_dispatch", "push"])
def test_decision_steps_select_everything_for_every_non_pull_request_event(
    tmp_path: Path, job_id: str, event: str
) -> None:
    """#3858: a dispatch agrees with `changes`, which treats it as a push."""
    calls = _run_decision_step(tmp_path, job_id, event)
    uv_calls = [call for call in calls if call.startswith("uv ")]
    assert len(uv_calls) == 1, calls
    assert "--push" in uv_calls[0].split()
    assert "--base" not in uv_calls[0].split()
    assert not [call for call in calls if call.startswith("git ")], calls


@pytest.mark.parametrize("job_id", ["python-pytest"])
def test_decision_steps_diff_against_the_base_on_pull_request(tmp_path: Path, job_id: str) -> None:
    calls = _run_decision_step(tmp_path, job_id, "pull_request")
    uv_calls = [call for call in calls if call.startswith("uv ")]
    assert len(uv_calls) == 1, calls
    assert "--push" not in uv_calls[0].split()
    assert "--base origin/main" in uv_calls[0]


def _rust_select_step() -> dict[str, Any]:
    return next(step for step in _python_job("rust-select")["steps"] if step.get("id") == "select")


def test_rust_select_waits_for_the_version_only_selection() -> None:
    """#3858: a version-only pull request skips the Rust partitions."""
    job = _python_job("rust-select")
    assert "changes" in _needs(job)
    env = _rust_select_step()["env"]
    assert env["VERSION_ONLY"] == "${{ needs.changes.outputs.version_only }}"


@pytest.mark.parametrize(
    ("event", "version_only", "expected"),
    [
        ("pull_request", "true", "tests=false"),
        ("push", "true", "tests=true"),
        ("push", "false", "tests=true"),
        ("workflow_dispatch", "true", "tests=true"),
    ],
)
def test_rust_select_script_honors_version_only_without_git(
    tmp_path: Path, event: str, version_only: str, expected: str
) -> None:
    """#3858: run the real select script; a version-only PR needs no diff."""
    step = _rust_select_step()
    bin_dir, log = _recording_stubs(tmp_path, "git")
    output = tmp_path / "github-output"
    runner_temp = tmp_path / "runner-temp"
    runner_temp.mkdir()
    environment = {
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "EVENT": event,
        "VERSION_ONLY": version_only,
        "BASE_REF": "main",
        "GITHUB_OUTPUT": str(output),
        "RUNNER_TEMP": str(runner_temp),
    }
    result = _run_bash(_string(step, "run"), environment, tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    assert [line for line in output.read_text().splitlines() if line] == [expected]
    assert not log.exists(), log.read_text()


def test_pytest_aggregator_accepts_selected_success(tmp_path: Path) -> None:
    completed, output = _invoke_selector(tmp_path, "packages/aci-protocol/src/aci_protocol/wire.py")
    assert completed.returncode == 0, completed.stderr
    selected = _outputs(output)["pytest"]
    result = _run_pytest_aggregate(
        tmp_path / "run",
        ci_policy_selected="false",
        ci_policy_result="skipped",
        container_result="success",
        selected=selected,
        shards=_all_shards(selected, "success"),
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_pytest_aggregator_rejects_selected_skip(tmp_path: Path) -> None:
    result = _run_pytest_aggregate(
        tmp_path,
        ci_policy_selected="false",
        ci_policy_result="skipped",
        container_result="success",
        selected="true",
        shards=_all_shards("true", "skipped"),
    )
    assert result.returncode != 0


def test_pytest_aggregator_rejects_unselected_success(tmp_path: Path) -> None:
    result = _run_pytest_aggregate(
        tmp_path,
        ci_policy_selected="false",
        ci_policy_result="skipped",
        container_result="skipped",
        selected="false",
        shards=_all_shards("false", "success"),
    )
    assert result.returncode != 0


def test_pytest_aggregator_rejects_a_failed_shard(tmp_path: Path) -> None:
    shards = _all_shards("true", "success")
    shards[2] = ("true", "failure", "success", "", "full")
    result = _run_pytest_aggregate(
        tmp_path,
        ci_policy_selected="false",
        ci_policy_result="skipped",
        container_result="success",
        selected="true",
        shards=shards,
        shards_result="failure",
    )
    assert result.returncode != 0
    assert "failure" in result.stderr


@pytest.mark.parametrize("shards_result", ["failure", "cancelled", "skipped"])
def test_pytest_aggregator_rejects_shard_jobs_that_did_not_succeed(
    tmp_path: Path, shards_result: str
) -> None:
    result = _run_pytest_aggregate(
        tmp_path, ci_policy_selected="false", ci_policy_result="skipped",
        container_result="success",
        selected="true",
        shards=_all_shards("true", "success"),
        shards_result=shards_result,
    )
    assert result.returncode != 0


@pytest.mark.parametrize("missing", range(1, SHARD_COUNT + 1))
def test_pytest_aggregator_rejects_a_shard_that_never_reported(
    tmp_path: Path, missing: int
) -> None:
    healthy = _run_pytest_aggregate(
        tmp_path,
        ci_policy_selected="false",
        ci_policy_result="skipped",
        selected="true",
        container_result="success",
        shards=_all_shards("true", "success"),
    )
    assert healthy.returncode == 0, healthy.stdout + healthy.stderr
    directory = _named_steps()["Download shard results"]["with"]["path"]
    record = tmp_path / directory / f"shard-{missing}.txt"
    original = record.read_text()
    record.unlink()
    result = _run_pytest_aggregate(
        tmp_path,
        ci_policy_selected="false",
        ci_policy_result="skipped",
        container_result="success",
        selected="true",
    )
    assert result.returncode != 0
    assert f"shard {missing}" in result.stderr
    record.write_text(original)
    restored = _run_pytest_aggregate(
        tmp_path,
        ci_policy_selected="false",
        ci_policy_result="skipped",
        selected="true",
        container_result="success",
    )
    assert restored.returncode == 0, restored.stdout + restored.stderr


def test_pytest_aggregator_rejects_a_shard_that_selected_differently(tmp_path: Path) -> None:
    shards = _all_shards("true", "success")
    shards[1] = ("false", "skipped", "skipped", "", "full")
    result = _run_pytest_aggregate(
        tmp_path,
        ci_policy_selected="false",
        ci_policy_result="skipped",
        container_result="success",
        selected="true",
        shards=shards,
    )
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
