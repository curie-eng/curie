"""Executable contract for the required fix pin pull request gate.

The helper is intentionally exercised as a subprocess. Its observable contract
is a pull request body, an event action, and an argv call to the already built
``curie`` binary. The workflow assertions read CI itself because a helper that
is never called cannot protect a merge.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
CHECKER = REPO_ROOT / "tools" / "fix-pin-ci" / "check.py"
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yaml"
FIX_PIN_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "fix-pin.yaml"
REQUIRED_WORKFLOWS = (
    CI_WORKFLOW,
    FIX_PIN_WORKFLOW,
    REPO_ROOT / ".github" / "workflows" / "codeql.yaml",
    REPO_ROOT / ".github" / "workflows" / "dependency-audit.yaml",
    REPO_ROOT / ".github" / "workflows" / "gitleaks.yaml",
    REPO_ROOT / ".github" / "workflows" / "pr-body.yaml",
)
PR_TEMPLATE = REPO_ROOT / ".github" / "PULL_REQUEST_TEMPLATE.md"
BUG_REPORT = REPO_ROOT / ".github" / "ISSUE_TEMPLATE" / "bug_report.yml"
PYPROJECT = REPO_ROOT / "pyproject.toml"
VERIFY_FIX_PIN = REPO_ROOT / "cli" / "scripts" / "verify-fix-pin.sh"

VALID_SELECTOR = "apps/api/tests/test_fix_pin_ci_gate.py::test_exact_declaration"
FACTORY_SELECTOR = (
    "tools/factory-e2e/tests/test_scripted_driver.py::"
    "test_scripted_driver_records_the_curie_bin_it_used"
)
INITIAL_BINDING_SELECTOR = (
    "tools/factory-e2e/tests/test_c4_initial_binding.py::"
    "test_second_factory_agent_uses_initial_tracker_binding"
)
ADMITTED_AGENT_SELECTOR = (
    "tools/factory-e2e/tests/test_c4_admitted_agent.py::"
    "test_labelled_agents_execute_as_admitted_agent"
)
STATIC_TOKEN_ROTATION_SELECTOR = (
    "tools/factory-e2e/tests/test_c4_static_token_rotation.py::"
    "test_secret_replacement_reports_rejection_and_recovers"
)
HELD_CANCELLATION_SELECTOR = (
    "tools/factory-e2e/tests/test_c4_held_cancellation.py::"
    "test_held_run_cancels_after_worker_replacement"
)
FACTORY_UPGRADE_SELECTOR = (
    "tools/factory-e2e/tests/test_c4_upgrade_migration.py::"
    "test_v0125_factory_upgrade_uses_supported_flow"
)
FACTORY_UPGRADE_NEAR_MISS_SELECTOR = (
    "tools/factory-e2e/tests/test_c4_upgrade_migration_extra.py::"
    "test_v0125_factory_upgrade_uses_supported_flow"
)
KIND_MODULES = (
    "tools/factory-e2e/tests/test_c4_initial_binding.py",
    "tools/factory-e2e/tests/test_c4_static_token_rotation.py",
    "tools/factory-e2e/tests/test_c4_upgrade_migration.py",
    "tools/factory-e2e/tests/test_c4_held_cancellation.py",
    "tools/factory-e2e/tests/test_c4_cluster_up.py",
    "tools/factory-e2e/tests/test_c4_forge_instances.py",
)
FORGE_INSTANCES_SELECTOR = (
    "tools/factory-e2e/tests/test_c4_forge_instances.py::test_forge_instances_survive_upgrade"
)
LOCAL_SELECTOR = "cli/tests/local/test_deploy.py::test_deploy"
LOCAL_LIVE_SELECTOR = "cli/tests/local/test_live.py::test_live"
LIVE_SELECTOR = "runner/tests/test_live.py::test_example"
CHART_SELECTOR = "charts/curie/ci/render-assertions.sh"
NOTES_RELEASE_TARGET_SELECTOR = "charts/curie/ci/notes-release-target-assertions.sh"
REPOSITORY = "curie-eng/curie"
STACK_BRANCH = "task/local-deployment-fix-pin"
BASE_SHA = "a" * 40


def _write_event(
    tmp_path: Path,
    body: str | None,
    *,
    action: str = "opened",
    include_body: bool = True,
    base_ref: str = "next",
    base_sha: object = BASE_SHA,
    include_base_sha: bool = True,
    repository: object = REPOSITORY,
    base_repository: object = REPOSITORY,
) -> Path:
    base: dict[str, object] = {
        "ref": base_ref,
        "repo": {"full_name": base_repository},
    }
    if include_base_sha:
        base["sha"] = base_sha
    pull_request: dict[str, object] = {"base": base}
    if include_body:
        pull_request["body"] = body
    event_path = tmp_path / "event.json"
    event_path.write_text(
        json.dumps(
            {
                "action": action,
                "repository": {"full_name": repository},
                "pull_request": pull_request,
            }
        ),
        encoding="utf-8",
    )
    return event_path


def _write_fake_binary(
    tmp_path: Path,
    name: str,
    *,
    call_log_environment: str,
    output_environment: str,
    exit_environment: str,
    subdirectory: str | None = None,
    default_output: str = "",
    print_end: str = "\n",
) -> tuple[Path, Path]:
    """Write a fake executable that logs its argv and echoes a scripted result.

    Returns ``(directory_or_binary_path, call_log_path)``: the binary's own path
    when ``subdirectory`` is ``None``, otherwise the directory it was written into.
    """
    directory = tmp_path / subdirectory if subdirectory is not None else tmp_path
    directory.mkdir(exist_ok=True)
    call_log = tmp_path / f"{name}-argv.json"
    fake_binary = directory / name
    fake_binary.write_text(
        "\n".join(
            [
                f"#!{sys.executable}",
                "import json",
                "import os",
                "from pathlib import Path",
                "import sys",
                f"Path(os.environ['{call_log_environment}']).write_text(json.dumps(sys.argv[1:]))",
                f"print(os.environ.get('{output_environment}', {default_output!r}), "
                f"end={print_end!r})",
                f"raise SystemExit(int(os.environ.get('{exit_environment}', '0')))",
                "",
            ]
        ),
        encoding="utf-8",
    )
    fake_binary.chmod(0o755)
    return (fake_binary if subdirectory is None else directory), call_log


def _write_fake_curie(tmp_path: Path) -> tuple[Path, Path]:
    return _write_fake_binary(
        tmp_path,
        "curie",
        call_log_environment="FIX_PIN_CALL_LOG",
        output_environment="FIX_PIN_OUTPUT",
        exit_environment="FIX_PIN_EXIT",
        default_output="",
        print_end="",
    )


def _gh_issue_payload(
    gh_labels: str, gh_body: str = "", gh_milestone: str | None = "v0.9.0"
) -> str:
    """Return issue data, including milestone state that the gate must ignore."""
    parsed = json.loads(gh_labels)
    if isinstance(parsed, list):
        return json.dumps({"labels": parsed, "body": gh_body, "milestone": gh_milestone})
    return gh_labels


def _write_fake_gh(tmp_path: Path) -> tuple[Path, Path]:
    """Install a fake ``gh`` in its own directory so PATH shadows nothing else."""
    directory = tmp_path / "gh-bin"
    directory.mkdir(exist_ok=True)
    call_log = tmp_path / "gh-argv.json"
    fake_binary = directory / "gh"
    fake_binary.write_text(
        "\n".join(
            [
                f"#!{sys.executable}",
                "import json",
                "import os",
                "from pathlib import Path",
                "import sys",
                "arguments = sys.argv[1:]",
                "with Path(os.environ['FIX_PIN_GH_CALL_LOG']).open('a') as stream:",
                "    stream.write(json.dumps(arguments) + '\\n')",
                "if any(argument.endswith('/pulls') for argument in arguments):",
                "    print(os.environ.get('FIX_PIN_GH_PULLS', '[]'))",
                "    raise SystemExit(int(os.environ.get('FIX_PIN_GH_PULLS_EXIT', '0')))",
                "print(os.environ.get('FIX_PIN_GH_LABELS', '[]'))",
                "raise SystemExit(int(os.environ.get('FIX_PIN_GH_EXIT', '0')))",
                "",
            ]
        ),
        encoding="utf-8",
    )
    fake_binary.chmod(0o755)
    return directory, call_log


def _run_checker(
    tmp_path: Path,
    body: str | None,
    *,
    action: str = "opened",
    include_body: bool = True,
    verifier_exit: int = 0,
    verifier_stdout: str = "PINNED\n",
    timeout: float | None = None,
    gh_labels: str = "[]",
    gh_body: str = "",
    gh_milestone: str | None = "v0.9.0",
    gh_exit: int = 0,
    gh_pulls: str = "[]",
    gh_pulls_exit: int = 0,
    gh_on_path: bool = True,
    base_ref: str = "next",
    base_sha: object = BASE_SHA,
    include_base_sha: bool = True,
    repository: object = REPOSITORY,
    base_repository: object = REPOSITORY,
) -> tuple[subprocess.CompletedProcess[str], Path]:
    event_path = _write_event(
        tmp_path,
        body,
        action=action,
        include_body=include_body,
        base_ref=base_ref,
        base_sha=base_sha,
        include_base_sha=include_base_sha,
        repository=repository,
        base_repository=base_repository,
    )
    curie, call_log = _write_fake_curie(tmp_path)
    binaries, gh_call_log = _write_fake_gh(tmp_path)
    environment = {
        **os.environ,
        "FIX_PIN_CALL_LOG": str(call_log),
        "FIX_PIN_EXIT": str(verifier_exit),
        "FIX_PIN_OUTPUT": verifier_stdout,
        "FIX_PIN_GH_CALL_LOG": str(gh_call_log),
        "FIX_PIN_GH_LABELS": _gh_issue_payload(gh_labels, gh_body, gh_milestone),
        "FIX_PIN_GH_EXIT": str(gh_exit),
        "FIX_PIN_GH_PULLS": gh_pulls,
        "FIX_PIN_GH_PULLS_EXIT": str(gh_pulls_exit),
        "GITHUB_REPOSITORY": REPOSITORY,
        # An empty PATH is how "gh is not installed" is expressed; the checker
        # reaches curie by absolute path, so nothing else needs PATH here.
        "PATH": f"{binaries}{os.pathsep}{os.environ.get('PATH', '')}" if gh_on_path else "",
    }
    completed = subprocess.run(
        [
            sys.executable,
            str(CHECKER),
            "--event",
            str(event_path),
            "--curie",
            str(curie),
            "--ref",
            "HEAD",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        check=False,
        env=environment,
        text=True,
        timeout=timeout,
    )
    return completed, call_log


def _gh_call_log(tmp_path: Path) -> Path:
    return tmp_path / "gh-argv.json"


def _gh_calls(tmp_path: Path) -> list[list[str]]:
    if not _gh_call_log(tmp_path).exists():
        return []
    return [
        json.loads(line) for line in _gh_call_log(tmp_path).read_text(encoding="utf-8").splitlines()
    ]


def _pull_lookup_calls(tmp_path: Path) -> list[list[str]]:
    return [
        call
        for call in _gh_calls(tmp_path)
        if any(argument.endswith("/pulls") for argument in call)
    ]


def _prerequisite(
    *,
    state: object = "open",
    head_ref: object = STACK_BRANCH,
    head_sha: object = BASE_SHA,
    head_repository: object = REPOSITORY,
    base_ref: object = "main",
    base_repository: object = REPOSITORY,
) -> dict[str, Any]:
    return {
        "state": state,
        "head": {
            "ref": head_ref,
            "sha": head_sha,
            "repo": {"full_name": head_repository},
        },
        "base": {
            "ref": base_ref,
            "repo": {"full_name": base_repository},
        },
    }


def _assert_exact_pull_lookup(tmp_path: Path) -> None:
    # GitHub documents read permission plus the head and state filters here:
    # https://docs.github.com/en/rest/pulls/pulls#list-pull-requests
    calls = _pull_lookup_calls(tmp_path)
    assert len(calls) == 1, f"expected one direct prerequisite lookup, got {calls!r}"
    call = calls[0]
    assert call[:2] == ["api", f"repos/{REPOSITORY}/pulls"]
    method_index = call.index("--method")
    assert call[method_index + 1] == "GET"
    fields: list[str] = []
    for index, argument in enumerate(call[:-1]):
        if argument in {"-f", "--raw-field"}:
            fields.append(call[index + 1])
    assert fields == [f"head=curie-eng:{STACK_BRANCH}", "state=open"]


def _github_output(tmp_path: Path) -> Path:
    return tmp_path / "github-output.txt"


def _run_needs_curie(
    tmp_path: Path,
    body: str | None,
    *,
    include_body: bool = True,
    github_output: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Invoke the cargo-guard probe. It must reuse the declaration parser."""
    event_path = _write_event(tmp_path, body, include_body=include_body)
    environment = {**os.environ}
    if github_output:
        environment["GITHUB_OUTPUT"] = str(_github_output(tmp_path))
    else:
        environment.pop("GITHUB_OUTPUT", None)
    return subprocess.run(
        [
            sys.executable,
            str(CHECKER),
            "--event",
            str(event_path),
            "--needs-curie",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        check=False,
        env=environment,
        text=True,
    )


def _needed_output(tmp_path: Path) -> str:
    outputs = _probe_outputs(tmp_path)
    return f"needed={outputs['needed']}\n"


def _probe_outputs(tmp_path: Path) -> dict[str, str]:
    lines = _github_output(tmp_path).read_text(encoding="utf-8").splitlines()
    outputs = dict(line.split("=", 1) for line in lines)
    assert len(lines) == len(outputs)
    assert set(outputs) == {"needed", "needs_stack", "needs_kind", "needs_factory_upgrade"}
    return outputs


def _is_fix_pin_gate(step: dict[str, Any]) -> bool:
    run = _string(step, "run")
    return "tools/fix-pin-ci/check.py" in run and "--needs-curie" not in run


def _is_fix_pin_probe(step: dict[str, Any]) -> bool:
    run = _string(step, "run")
    return "tools/fix-pin-ci/check.py" in run and "--needs-curie" in run


@pytest.mark.parametrize(
    ("body", "include_body"),
    [
        (None, True),
        (None, False),
        ("<!-- Fix pin: apps/api/tests/test_example.py::test_example -->", True),
        ("<!-- Fix pin: apps/api/tests/test_example.py::test_example -->\r\n", True),
    ],
)
def test_non_fix_pull_requests_skip_without_calling_the_verifier(
    tmp_path: Path, body: str | None, include_body: bool
) -> None:
    completed, call_log = _run_checker(tmp_path, body, include_body=include_body)

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "SKIPPED: no Fix pin declaration"
    assert not call_log.exists(), "a non fix pull request must not run curie"


def test_exact_declaration_calls_the_verifier_with_one_argv_selector(tmp_path: Path) -> None:
    completed, call_log = _run_checker(tmp_path, f"Fix pin: {VALID_SELECTOR}\n")

    assert completed.returncode == 0, completed.stderr
    assert json.loads(call_log.read_text(encoding="utf-8")) == [
        "dev",
        "verify-fix-pin",
        "HEAD",
        VALID_SELECTOR,
    ]


@pytest.mark.parametrize(
    "selector",
    [
        FACTORY_SELECTOR,
        INITIAL_BINDING_SELECTOR,
        FACTORY_UPGRADE_SELECTOR,
        FACTORY_UPGRADE_NEAR_MISS_SELECTOR,
    ],
)
def test_factory_python_selector_calls_the_verifier_at_the_ci_boundary(
    tmp_path: Path, selector: str
) -> None:
    completed, call_log = _run_checker(tmp_path, f"Fix pin: {selector}\n")

    assert completed.returncode == 0, completed.stderr
    assert json.loads(call_log.read_text(encoding="utf-8")) == [
        "dev",
        "verify-fix-pin",
        "HEAD",
        selector,
    ]


@pytest.mark.parametrize(
    "selector",
    [
        "apps/worker/tests/kernel/test_consumer.py::test_consumes_stream_entry_end_to_end_and_acks",
        "runner/tests/test_history.py::test_example",
        "runner/tests/history/test_history.py::test_example",
        (
            "apps/api/tests/test_config_parity.py::"
            "TestResumeDeadLetterStreamCoherence::"
            "test_empty_resume_override_falls_back_to_the_shared_graveyard"
        ),
        (
            "apps/worker/tests/reconcile/test_connector_drift.py::"
            "test_no_single_kind_reports_drift_on_its_own[Service]"
        ),
    ],
)
def test_supported_nested_python_selectors_call_the_verifier(tmp_path: Path, selector: str) -> None:
    completed, call_log = _run_checker(tmp_path, f"Fix pin: {selector}")

    assert completed.returncode == 0, completed.stderr
    assert json.loads(call_log.read_text(encoding="utf-8")) == [
        "dev",
        "verify-fix-pin",
        "HEAD",
        selector,
    ]


@pytest.mark.parametrize("selector", [LOCAL_SELECTOR, LOCAL_LIVE_SELECTOR])
def test_local_python_selector_calls_the_verifier_at_the_ci_boundary(
    tmp_path: Path, selector: str
) -> None:
    completed, call_log = _run_checker(tmp_path, f"Fix pin: {selector}\n")

    assert completed.returncode == 0, completed.stderr
    assert json.loads(call_log.read_text(encoding="utf-8")) == [
        "dev",
        "verify-fix-pin",
        "HEAD",
        selector,
    ]


def test_ordinary_fix_pin_prose_skips_without_calling_the_verifier(tmp_path: Path) -> None:
    completed, call_log = _run_checker(tmp_path, "This closes the fix pin-related enforcement gap")

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "SKIPPED: no Fix pin declaration"
    assert not call_log.exists(), "ordinary prose must not run curie"


def test_long_decoration_prefix_in_ordinary_prose_skips_within_timeout(
    tmp_path: Path,
) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        "#" * 24 + " ordinary prose",
        timeout=1.0,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "SKIPPED: no Fix pin declaration"
    assert not call_log.exists(), "ordinary prose must not run curie"


@pytest.mark.parametrize(
    "body",
    [
        "Fix pin:",
        f" Fix pin: {VALID_SELECTOR}",
        f"Fix pin: {VALID_SELECTOR} with explanation",
        f"Fix pin: {VALID_SELECTOR}\nFix pin: cli/tests/verify_fix_pin.rs::ci_gate_pins",
    ],
)
def test_invalid_or_duplicate_declarations_fail_closed_without_calling_curie(
    tmp_path: Path, body: str
) -> None:
    completed, call_log = _run_checker(tmp_path, body)

    assert completed.returncode != 0
    assert "Fix pin declaration" in f"{completed.stdout}\n{completed.stderr}"
    assert not call_log.exists(), "invalid policy input must not reach curie"


@pytest.mark.parametrize(
    "selector",
    [
        "--help",
        "-h",
        "cli/tests/verify_fix_pin.rs::--no-run",
        "tools/fix-pin-ci/tests/test_fix_pin_ci.py::test_not_supported",
        "tools/other-tool/tests/test_pin.py::test_pin",
        "tools/factory-e2e/test_pin.py::test_pin",
        "tools/factory-e2e/tests/../test_pin.py::test_pin",
        "tools/factory-e2e/tests/test_pin.py::helper",
        "apps/api/tests/test_fix_pin_ci_gate.py",
        "cli/tests/test_pin.py::test_pin",
        "cli/tests/local/nested/test_pin.py::test_pin",
        "cli/tests/local/../test_pin.py::test_pin",
        "cli/tests/local/pin.py::test_pin",
        "cli/tests/local/test_pin.py::helper",
        "charts/curie/ci/../render-assertions.sh",
        "charts/curie/ci/runtime/",
        "charts/curie/ci/runtime/name.py",
    ],
)
def test_unsupported_selectors_fail_closed_without_calling_curie(
    tmp_path: Path, selector: str
) -> None:
    completed, call_log = _run_checker(tmp_path, f"Fix pin: {selector}")

    assert completed.returncode != 0
    assert "Fix pin declaration" in f"{completed.stdout}\n{completed.stderr}"
    assert not call_log.exists(), "unsupported selectors must not reach curie"


@pytest.mark.parametrize(
    "body",
    [
        f"Fix Pin: {VALID_SELECTOR}",
        f"fix pin: {VALID_SELECTOR}",
        f"Fix-pin: {VALID_SELECTOR}",
        f"Fix pin : {VALID_SELECTOR}",
    ],
)
def test_near_miss_declaration_markers_fail_closed_without_calling_curie(
    tmp_path: Path, body: str
) -> None:
    completed, call_log = _run_checker(tmp_path, body)

    assert completed.returncode != 0
    assert "Fix pin declaration" in f"{completed.stdout}\n{completed.stderr}"
    assert not call_log.exists(), "near miss declarations must not reach curie"


@pytest.mark.parametrize(
    "body",
    [
        f"- Fix pin: {VALID_SELECTOR}",
        f"- [ ] Fix pin: {VALID_SELECTOR}",
        f"* [x] Fix pin: {VALID_SELECTOR}",
        f"> Fix pin: {VALID_SELECTOR}",
        f"**Fix pin: {VALID_SELECTOR}**",
        f"# Fix pin: {VALID_SELECTOR}",
        f"1. Fix pin: {VALID_SELECTOR}",
    ],
)
def test_markdown_decorated_declarations_fail_closed_without_calling_curie(
    tmp_path: Path, body: str
) -> None:
    completed, call_log = _run_checker(tmp_path, body)

    assert completed.returncode != 0
    assert "Fix pin declaration" in f"{completed.stdout}\n{completed.stderr}"
    assert not call_log.exists(), "decorated declarations must not reach curie"


def test_shell_metacharacters_fail_before_the_verifier_runs(tmp_path: Path) -> None:
    marker = tmp_path / "shell-was-run"
    selector = f"apps/api/tests/test_fix_pin_gate.py::test_safe;touch${{IFS}}{marker}"
    completed, call_log = _run_checker(tmp_path, f"Fix pin: {selector}", verifier_exit=97)

    assert completed.returncode != 0
    assert not call_log.exists(), "invalid selectors must not reach curie"
    assert not marker.exists(), "the declaration must never be interpolated into a shell command"


def test_failed_verification_says_rerunning_unchanged_will_not_help(
    tmp_path: Path,
) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Fix pin: {VALID_SELECTOR}",
        verifier_exit=97,
        verifier_stdout="UNPINNED\n",
    )

    output = f"{completed.stdout}\n{completed.stderr}"
    assert completed.returncode == 97
    assert call_log.exists(), "a valid declaration must reach curie"
    assert "This failure is deterministic." in output
    assert "Edit the PR body's `Fix pin:` declaration or add a changed test." in output
    assert "Rerunning unchanged will not help." in output


def test_verifier_error_does_not_claim_a_deterministic_pin_failure(tmp_path: Path) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Fix pin: {VALID_SELECTOR}",
        verifier_exit=97,
        verifier_stdout="not inside a git repository\n",
    )

    assert completed.returncode == 97
    assert call_log.exists()
    assert "This failure is deterministic." not in completed.stderr


@pytest.mark.parametrize("verifier_stdout", ["", "NOT PINNED\n", "PINNED extra\n"])
def test_verifier_exit_zero_requires_an_exact_pinned_marker(
    tmp_path: Path, verifier_stdout: str
) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Fix pin: {VALID_SELECTOR}",
        verifier_stdout=verifier_stdout,
    )

    assert completed.returncode != 0
    assert call_log.exists(), "a valid declaration must reach curie"


_PYTHON_PIN_PYPROJECT = """[project]
name = "pin-fixture"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = []

[dependency-groups]
dev = ["pytest>=8.3"]

[tool.uv]
package = false

[tool.pytest.ini_options]
pythonpath = ["."]
"""
_PYTHON_PIN_SELECTOR = "apps/api/tests/test_pin.py::test_selected"
_GIT = ["git", "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null"]


def _commit_files(repository: Path, files: dict[str, str], message: str) -> None:
    for relative, content in files.items():
        path = repository / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    for arguments in (["add", "."], ["commit", "-q", "-m", message]):
        subprocess.run([*_GIT, *arguments], cwd=repository, check=True)


def _verify_python_fix_pin(
    tmp_path: Path, baseline: dict[str, str], fix: dict[str, str]
) -> tuple[subprocess.CompletedProcess[str], str]:
    """Commit ``baseline`` then ``fix`` in a scratch repo and run the verifier."""
    repository = tmp_path / "repository"
    repository.mkdir()
    for arguments in (
        ["init", "-q"],
        ["config", "user.name", "Curie Test"],
        ["config", "user.email", "curie@example.com"],
    ):
        subprocess.run([*_GIT, *arguments], cwd=repository, check=True)
    _commit_files(
        repository,
        {
            "pyproject.toml": _PYTHON_PIN_PYPROJECT,
            "apps/__init__.py": "",
            "apps/api/__init__.py": "",
            "apps/api/tests/__init__.py": "",
            **baseline,
        },
        "Add Python fixture",
    )
    _commit_files(repository, fix, "Fix Python behavior")
    fix_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        capture_output=True,
        check=True,
        text=True,
    ).stdout.strip()

    completed = subprocess.run(
        ["bash", str(VERIFY_FIX_PIN), fix_commit, _PYTHON_PIN_SELECTOR],
        cwd=repository,
        capture_output=True,
        check=False,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        text=True,
    )
    return completed, f"{completed.stdout}\n{completed.stderr}"


def test_changed_selected_python_test_is_pinned_by_real_pytest_junit_failure(
    tmp_path: Path,
) -> None:
    completed, shown = _verify_python_fix_pin(
        tmp_path,
        {
            "apps/api/pin_fixture.py": "def value():\n    return 1\n",
            "apps/api/tests/test_pin.py": """from apps.api.pin_fixture import value


def test_selected():
    assert value() == 1
""",
        },
        {
            "apps/api/pin_fixture.py": "def value():\n    return 2\n",
            "apps/api/tests/test_pin.py": """from apps.api.pin_fixture import value


def test_selected():
    assert value() == 2
""",
        },
    )

    assert completed.returncode == 0, shown
    assert "PINNED" in completed.stdout.splitlines(), shown
    assert "1 failed" in shown, shown


_PIN_FIXTURE_BASELINE = "def value():\n    return 1\n"
_PIN_FIXTURE_WITH_ADDED = "def value():\n    return 1\n\n\nAdded = 2\n"
_TEST_PIN_BASELINE = """from apps.api.pin_fixture import value


def test_selected():
    assert value() == 1
"""


@pytest.mark.parametrize(
    "fix",
    [
        pytest.param(
            {
                "apps/api/pin_fixture.py": _PIN_FIXTURE_WITH_ADDED,
                "apps/api/tests/test_pin.py": """from apps.api.pin_fixture import Added


def test_selected():
    assert Added == 2
""",
            },
            id="test-module-imports-added-symbol",
        ),
        pytest.param(
            {
                "apps/api/pin_fixture.py": _PIN_FIXTURE_WITH_ADDED,
                "apps/api/tests/conftest.py": """import pytest

from apps.api.pin_fixture import Added


@pytest.fixture
def added():
    return Added
""",
                "apps/api/tests/test_pin.py": """def test_selected(added):
    assert added == 2
""",
            },
            id="adjacent-conftest-imports-added-symbol",
        ),
        pytest.param(
            {
                "apps/api/added_module.py": "Added = 2\n",
                "apps/api/tests/test_pin.py": """from apps.api.added_module import Added


def test_selected():
    assert Added == 2
""",
            },
            id="test-module-imports-added-module",
        ),
    ],
)
def test_import_error_from_selected_module_or_its_conftest_counts_as_pinned(
    tmp_path: Path, fix: dict[str, str]
) -> None:
    """#3577: a symbol the fix added is missing under revert, so the selected
    test module (or its conftest) fails to import. That is the pinned test
    failing, not an unattributed collection error."""
    completed, shown = _verify_python_fix_pin(
        tmp_path,
        {
            "apps/api/pin_fixture.py": _PIN_FIXTURE_BASELINE,
            "apps/api/tests/test_pin.py": _TEST_PIN_BASELINE,
        },
        fix,
    )

    assert completed.returncode == 0, shown
    assert "PINNED" in completed.stdout.splitlines(), shown
    assert "ImportError" in shown or "ModuleNotFoundError" in shown, shown


# ``other.py`` imports ``Added`` at both commits, so the fix only restores the
# symbol it needs. Under revert the import error is raised inside other.py,
# a module the selected test merely depends on.
_OTHER_IMPORTS_ADDED = "from apps.api.pin_fixture import Added\n\nOTHER = Added\n"


@pytest.mark.parametrize(
    "fix",
    [
        pytest.param(
            {
                "apps/api/pin_fixture.py": _PIN_FIXTURE_WITH_ADDED,
                "apps/api/tests/test_pin.py": """from apps.api import other
from apps.api.pin_fixture import value


def test_selected():
    assert value() == 1
    assert other.OTHER == 2
""",
            },
            id="test-module-imports-failing-unrelated-module",
        ),
        pytest.param(
            {
                "apps/api/pin_fixture.py": _PIN_FIXTURE_WITH_ADDED,
                "apps/api/tests/conftest.py": """import pytest

from apps.api import other


@pytest.fixture
def other_value():
    return other.OTHER
""",
                "apps/api/tests/test_pin.py": """from apps.api.pin_fixture import value


def test_selected(other_value):
    assert value() == 1
    assert other_value == 2
""",
            },
            id="conftest-imports-failing-unrelated-module",
        ),
    ],
)
def test_import_error_raised_in_an_unrelated_module_is_not_attributed(
    tmp_path: Path, fix: dict[str, str]
) -> None:
    completed, shown = _verify_python_fix_pin(
        tmp_path,
        {
            "apps/api/pin_fixture.py": _PIN_FIXTURE_BASELINE,
            "apps/api/other.py": _OTHER_IMPORTS_ADDED,
            "apps/api/tests/test_pin.py": _TEST_PIN_BASELINE,
        },
        fix,
    )

    assert completed.returncode != 0, shown
    assert "reversed failure was not attributed" in shown, shown
    assert "PINNED" not in completed.stdout.splitlines(), shown
    assert "ImportError" in shown, shown


# ``load`` imports ``Added`` only when called, so other.py itself always
# imports. Under revert the ImportError is raised in ``load``'s frame, which
# the selected module (or its conftest) merely called at import time.
_OTHER_LOADS_ADDED = """def load():
    from apps.api.pin_fixture import Added

    return Added
"""
# A non-import exception whose message quotes an ImportError line. pytest
# renders every message line as its own ``E`` line.
_FORGED_IMPORT_ERROR = """from apps.api import pin_fixture

if not hasattr(pin_fixture, "Added"):
    raise RuntimeError("service unavailable\\nImportError: quoted diagnostic")
"""


@pytest.mark.parametrize(
    "fix",
    [
        pytest.param(
            {
                "apps/api/pin_fixture.py": _PIN_FIXTURE_WITH_ADDED,
                "apps/api/tests/test_pin.py": """from apps.api import other

LOADED = other.load()


def test_selected():
    assert LOADED == 2
""",
            },
            id="test-module-calls-function-that-raises-import-error",
        ),
        pytest.param(
            {
                "apps/api/pin_fixture.py": _PIN_FIXTURE_WITH_ADDED,
                "apps/api/tests/conftest.py": """import pytest

from apps.api import other

LOADED = other.load()


@pytest.fixture
def loaded():
    return LOADED
""",
                "apps/api/tests/test_pin.py": """def test_selected(loaded):
    assert loaded == 2
""",
            },
            id="conftest-calls-function-that-raises-import-error",
        ),
        pytest.param(
            {
                "apps/api/pin_fixture.py": _PIN_FIXTURE_WITH_ADDED,
                "apps/api/tests/test_pin.py": _FORGED_IMPORT_ERROR
                + """

def test_selected():
    assert pin_fixture.Added == 2
""",
            },
            id="test-module-raises-runtime-error-quoting-import-error",
        ),
        pytest.param(
            {
                "apps/api/pin_fixture.py": _PIN_FIXTURE_WITH_ADDED,
                "apps/api/tests/conftest.py": _FORGED_IMPORT_ERROR,
                "apps/api/tests/test_pin.py": """from apps.api.pin_fixture import value


def test_selected():
    assert value() == 2 - 1
""",
            },
            id="conftest-raises-runtime-error-quoting-import-error",
        ),
    ],
)
def test_non_import_failure_in_selected_module_or_conftest_is_not_attributed(
    tmp_path: Path, fix: dict[str, str]
) -> None:
    """Only an ImportError raised by the selected module's (or conftest's) own
    ``<module>`` frame is attributable: not one raised in a function it calls,
    and not a different exception whose message quotes an ImportError line."""
    completed, shown = _verify_python_fix_pin(
        tmp_path,
        {
            "apps/api/pin_fixture.py": _PIN_FIXTURE_BASELINE,
            "apps/api/other.py": _OTHER_LOADS_ADDED,
            "apps/api/tests/test_pin.py": _TEST_PIN_BASELINE,
        },
        fix,
    )

    assert completed.returncode != 0, shown
    assert "reversed failure was not attributed" in shown, shown
    assert "PINNED" not in completed.stdout.splitlines(), shown


def test_committed_pull_request_template_skips_without_calling_curie(tmp_path: Path) -> None:
    completed, call_log = _run_checker(tmp_path, PR_TEMPLATE.read_text(encoding="utf-8"))

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "SKIPPED: no Fix pin declaration"
    assert not call_log.exists(), "the template instruction must not activate the verifier"


def _load_workflow(path: Path) -> dict[str, Any]:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(document, dict), f"{path.name} must be a YAML mapping"
    return document


def _load_ci() -> dict[str, Any]:
    return _load_workflow(CI_WORKFLOW)


def _workflow_trigger(document: dict[str, Any]) -> dict[str, Any]:
    trigger = document.get("on", cast(dict[object, Any], document).get(True))
    assert isinstance(trigger, dict), "ci.yaml must declare an on mapping"
    return trigger


def test_required_workflows_reach_one_task_branch_without_widening_pushes() -> None:
    for path in REQUIRED_WORKFLOWS:
        trigger = _workflow_trigger(_load_workflow(path))
        pull_request = trigger.get("pull_request")
        assert isinstance(pull_request, dict), f"{path.name} must run for pull requests"
        assert pull_request.get("branches") == ["main", "next", "task/**"], path.name

        push = trigger.get("push")
        if path.name in {"pr-body.yaml", "fix-pin.yaml"}:
            assert push is None, f"{path.name} must remain pull request only"
        else:
            assert isinstance(push, dict), f"{path.name} must retain its push trigger"
            assert push.get("branches") == ["main", "next"], path.name


def _python_job(
    document: dict[str, Any], job_id: str = "python"
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    jobs = document.get("jobs")
    assert isinstance(jobs, dict), "ci.yaml must declare jobs"
    job = jobs.get(job_id)
    assert isinstance(job, dict), f"ci.yaml must retain the {job_id} job"
    steps = job.get("steps")
    assert isinstance(steps, list), "the required Python job must retain steps"
    return job, [step for step in steps if isinstance(step, dict)]


def _single_step_index(
    steps: list[dict[str, Any]], predicate: Callable[[dict[str, Any]], bool], description: str
) -> int:
    matches = [index for index, step in enumerate(steps) if predicate(step)]
    assert len(matches) == 1, f"expected exactly one {description}, found {len(matches)}"
    return matches[0]


def _string(step: dict[str, Any], key: str) -> str:
    value = step.get(key)
    return value if isinstance(value, str) else ""


def _fix_pin_job() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    document = _load_workflow(FIX_PIN_WORKFLOW)
    jobs = document.get("jobs")
    assert isinstance(jobs, dict), "fix-pin.yaml must declare jobs"
    job = jobs.get("fix-pin")
    assert isinstance(job, dict), "fix-pin.yaml must retain the fix-pin job"
    steps = job.get("steps")
    assert isinstance(steps, list), "the fix pin job must retain steps"
    return job, [step for step in steps if isinstance(step, dict)]


def test_ci_keeps_the_required_python_status_and_keeps_the_fix_pin_gate_off_it() -> None:
    """The suite and the gate are both required, and no longer in series.

    They were one job. #2230 measured that job as the whole critical path, and
    the gate sat behind the suite it does not depend on: a cold 251s cargo
    build plus a 127s verification appended after 1048s of pytest. This asserts
    the split, in both directions. The Python job must still be the unskippable
    required suite, and it must no longer carry the gate; the gate must be its
    own unskippable required job.
    """
    document = _load_ci()
    trigger = _workflow_trigger(document)
    pull_request = trigger.get("pull_request")
    assert isinstance(pull_request, dict), "CI must run for pull requests"
    actions = pull_request.get("types")
    assert isinstance(actions, list), "CI must declare its pull request actions"
    assert len(actions) == 3 and set(actions) == {
        "opened",
        "synchronize",
        "reopened",
    }, "CI must rerun required checks on new commits, not on title or body edits"

    job, required_steps = _python_job(document)
    assert job.get("name") == "Python (ruff + mypy + pytest)"
    # The suite runs in the python-pytest shards and the static gates in
    # python-static; the required job only waits for them to aggregate their
    # results, and always() keeps it from skipping.
    assert job.get("needs") == [
        "changes",
        "python-pytest",
        "python-container-fixtures",
        "python-dry-run-fixture",
        "python-static",
        "ci-policy",
    ]
    assert job.get("if") == "always()", "the required Python check must not be skippable"
    shard_job, steps = _python_job(document, "python-pytest")
    # #3858: the shards wait for the `changes` selection and run only when it
    # selected pytest; the required job above still reports either way.
    shard_needs = shard_job.get("needs")
    shard_needs = [shard_needs] if isinstance(shard_needs, str) else list(shard_needs or [])
    assert "changes" in shard_needs
    assert shard_job.get("if") == "${{ needs.changes.outputs.pytest == 'true' }}"

    permissions = job.get("permissions")
    assert isinstance(permissions, dict), "the Python job must declare job level permissions"
    assert permissions.get("contents") == "read"
    # The Issues scope followed the gate out. It was only ever there so the gate
    # could read closed issue labels, and the suite has no use for it.
    assert "issues" not in permissions, (
        "the Python job must not keep the Issues scope the fix pin gate took with it"
    )

    checkout_index = _single_step_index(
        steps,
        lambda step: _string(step, "uses") == "actions/checkout@v7",
        "Python checkout",
    )
    checkout = steps[checkout_index]
    checkout_with = checkout.get("with")
    assert isinstance(checkout_with, dict)
    assert checkout_with.get("fetch-depth") == 0
    assert checkout_with.get("persist-credentials") is False

    stack_index = _single_step_index(
        steps,
        lambda step: (
            _string(step, "run").strip()
            == "python3 scripts/wait-for-langfuse.py --start --timeout-seconds 480"
        ),
        "dev stack startup",
    )
    migration_index = _single_step_index(
        steps,
        lambda step: "uv run alembic upgrade head" in _string(step, "run"),
        "shared database migration",
    )
    # Dedicated kind runtime modules run through their own CI callers.
    pytest_index = _single_step_index(
        steps,
        lambda step: step.get("id") == "pytest",
        "normal Python suite",
    )
    pytest_step = steps[pytest_index]
    pytest_script = _string(pytest_step, "run")
    assert "${{" not in pytest_script
    pytest_env = pytest_step.get("env")
    assert isinstance(pytest_env, dict)
    bindings = {}
    for meaning, expression in {
        "shard": "${{ matrix.shard }}",
        "deselect": "${{ steps.python-runtime.outputs.pytest_deselect }}",
        "scope": "${{ steps.python-runtime.outputs.pytest_scope }}",
    }.items():
        names = [name for name, value in pytest_env.items() if value == expression]
        assert len(names) == 1, expression
        bindings[meaning] = names[0]
    runtime_exclusions = {("--ignore", module) for module in KIND_MODULES} | {
        ("--deselect", ADMITTED_AGENT_SELECTOR.split("::", 1)[0]),
        ("--ignore", "tools/factory-e2e/tests/test_c4_tracker_scope.py"),
        ("--ignore", "tools/factory-e2e/tests/test_c4_dynamic_base.py"),
        ("--ignore", "examples/tests/test_sre_bot_observability_runtime.py"),
        ("--ignore", "tools/factory-e2e/tests/test_c4_dry_run.py"),
        ("--ignore", "tools/ci-retry-gate"),
        ("--ignore", "tools/ci-workflow-paths"),
        ("--ignore", "tools/ci-release-target-parity"),
        ("--ignore", "tools/e2e-ci-selection"),
        ("--ignore", "tools/helm-ci-gate"),
        ("--ignore", "tools/fix-pin-ci"),
        ("--ignore", "tools/p0-closes-ci"),
        ("--ignore", "tools/runtime-assertion-gate"),
        ("--ignore", "tools/dependency-audit-paths"),
        ("--ignore", "tools/close-on-next"),
    }
    runtime_exclusions |= {
        ("--ignore-glob", f"{path}/tests/*")
        for option, path in runtime_exclusions
        if option == "--ignore" and not path.endswith(".py")
    }

    def assert_full_suite(command: list[str], shard: int, deselect: str) -> None:
        assert command[:3] == ["run", "pytest", "-q"]
        if deselect:
            assert command.count("--deselect") == 1
            assert command[-2:] == ["--deselect", deselect]
            command = command[:-2]
        distribution = [
            "-n",
            "4",
            "--dist",
            "loadgroup",
            "--ci-shard",
            f"{shard}/6",
            "--reruns",
            "1",
        ]
        extra = command[3:]
        assert extra[: len(distribution)] == distribution
        arguments = iter(extra[len(distribution) :])
        excluded = []
        reporting = []
        for argument in arguments:
            if argument in {"--ignore", "--ignore-glob", "--deselect"}:
                path = next(arguments, None)
                assert path is not None, f"{argument} requires a path"
                excluded.append((argument, path))
            elif argument.startswith(("--ignore=", "--ignore-glob=", "--deselect=")):
                option, path = argument.split("=", 1)
                excluded.append((option, path))
            else:
                reporting.append(argument)
        assert len(excluded) == len(runtime_exclusions) and set(excluded) == runtime_exclusions, (
            "the Python suite must use exactly the required runtime and CI policy exclusions, "
            f"got {excluded!r}"
        )
        report_chars = set("fEsxXpPaAR")
        assert all(
            argument.startswith("--durations")
            or (
                argument.startswith("-r")
                and len(argument) > 2
                and set(argument[2:]) <= report_chars
            )
            for argument in reporting
        ), f"the full Python suite permits only reporting flags, got {command!r}"

    # Run the unmodified Bash with a recording uv function at its command boundary.
    for shard in range(1, 7):
        for deselect in ("", "examples/tests/test_sre_bot_observability_runtime.py"):
            environment = {
                **os.environ,
                "BASH_FUNC_uv%%": "() { printf '%s\\0' \"$@\"; }",
                bindings["shard"]: str(shard),
                bindings["deselect"]: deselect,
                bindings["scope"]: "full",
            }
            result = subprocess.run(
                ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", pytest_script],
                env=environment,
                capture_output=True,
                text=True,
                check=False,
            )
            assert result.returncode == 0, result.stdout + result.stderr
            pytest_command = result.stdout.split("\0")
            assert pytest_command.pop() == ""
            assert_full_suite(pytest_command, shard, deselect)
            if shard == 1 and not deselect:
                for mutation in (
                    ["--ignore", "apps/api/tests"],
                    ["--ignore=apps/api/tests"],
                    ["--ignore-glob=*"],
                    ["--deselect=apps/api/tests"],
                    ["apps/api/tests/test_config.py"],
                    list(sorted(runtime_exclusions)[0]),
                ):
                    with pytest.raises(AssertionError):
                        assert_full_suite([*pytest_command, *mutation], shard, deselect)
                for missing_option, missing_path in runtime_exclusions:
                    reduced: list[str] = []
                    arguments = iter(pytest_command)
                    for argument in arguments:
                        if argument in {"--ignore", "--ignore-glob", "--deselect"}:
                            path = next(arguments)
                            if (argument, path) != (missing_option, missing_path):
                                reduced.extend((argument, path))
                        elif argument != f"{missing_option}={missing_path}":
                            reduced.append(argument)
                    with pytest.raises(AssertionError):
                        assert_full_suite(reduced, shard, deselect)
                    replaced = [
                        *reduced,
                        "--ignore" if missing_option == "--deselect" else "--deselect",
                        missing_path,
                    ]
                    with pytest.raises(AssertionError):
                        assert_full_suite(replaced, shard, deselect)
    assert stack_index < migration_index < pytest_index

    # The half that keeps the win. Nothing about the gate may drift back into
    # the job whose length is the critical path.
    steps = steps + required_steps
    assert not any(_is_fix_pin_gate(step) for step in steps), (
        "the fix pin gate must not run inside the Python job again"
    )
    assert not any(_is_fix_pin_probe(step) for step in steps), (
        "the fix pin probe must not run inside the Python job again"
    )
    assert not any("cargo" in _string(step, "run") for step in steps), (
        "the Python job must not build Rust behind the suite"
    )

    diagnostic_index = _single_step_index(
        steps,
        lambda step: "docker compose -f compose.dev.yaml logs" in _string(step, "run"),
        "failure stack diagnostic",
    )
    diagnostic_if = _string(steps[diagnostic_index], "if")
    assert "failure()" in diagnostic_if
    assert "steps.python-runtime.outputs.pytest == 'true'" in diagnostic_if
    assert pytest_index < diagnostic_index


def test_the_fix_pin_job_is_required_and_carries_the_whole_gate() -> None:
    """Everything the gate needs must be in the job that now runs it."""
    job, steps = _fix_pin_job()
    assert "needs" not in job, "the required fix pin check must not be skippable"
    assert not _string(job, "if"), "the job must run for every configured pull request event"
    trigger = _workflow_trigger(_load_workflow(FIX_PIN_WORKFLOW))["pull_request"]
    assert set(trigger["types"]) == {"opened", "synchronize", "reopened", "edited"}

    permissions = job.get("permissions")
    assert isinstance(permissions, dict), "the fix pin job must declare job level permissions"
    assert permissions == {
        "contents": "read",
        "issues": "read",
        "pull-requests": "read",
    }

    checkout = steps[
        _single_step_index(
            steps,
            lambda step: _string(step, "uses") == "actions/checkout@v7",
            "fix pin checkout",
        )
    ]
    checkout_with = checkout.get("with")
    assert isinstance(checkout_with, dict)
    # verify-fix-pin.sh resolves HEAD and adds a detached worktree at it, and
    # the gate diffs against the base.
    assert checkout_with.get("fetch-depth") == 0
    assert checkout_with.get("persist-credentials") is False

    probe_index = _single_step_index(steps, _is_fix_pin_probe, "fix pin cargo probe")
    gate_index = _single_step_index(steps, _is_fix_pin_gate, "fix pin caller")

    probe = steps[probe_index]
    assert probe.get("id") == "fix-pin-curie"
    assert not _string(probe, "if"), (
        "the probe must run for every pull request, including bodies with no "
        "live selector; gating it on its own output would skip the decision"
    )
    assert shlex.split(_string(probe, "run")) == [
        "python3",
        "tools/fix-pin-ci/check.py",
        "--event",
        "$GITHUB_EVENT_PATH",
        "--needs-curie",
    ]
    assert '--event "$GITHUB_EVENT_PATH"' in _string(probe, "run")

    gate = steps[gate_index]
    gate_environment = gate.get("env")
    assert isinstance(gate_environment, dict)
    assert gate_environment.get("CARGO_TARGET_DIR") == "${{ github.workspace }}/cli/target"
    assert gate_environment.get("GH_TOKEN"), "the gate needs a token to read issue labels"
    assert shlex.split(_string(gate, "run")) == [
        "python3",
        "tools/fix-pin-ci/check.py",
        "--event",
        "$GITHUB_EVENT_PATH",
        "--curie",
        "cli/target/release/curie",
        "--ref",
        "HEAD",
    ]
    assert '--event "$GITHUB_EVENT_PATH"' in _string(gate, "run")

    release_build_index = _single_step_index(
        steps,
        lambda step: (
            _string(step, "run").strip()
            == "cargo build --release --locked --manifest-path cli/Cargo.toml"
        ),
        "direct current release build",
    )
    helm_index = _single_step_index(
        steps,
        lambda step: (
            _string(step, "uses") == "azure/setup-helm@9bc31f4ebc9c6b171d7bfbaa5d006ae7abdb4310"
            and (step.get("with") or {}).get("version") == "v3.16.4"
        ),
        "pinned Helm setup",
    )
    cache_index = _single_step_index(
        steps,
        lambda step: _string(step, "uses").startswith("Swatinem/rust-cache@"),
        "fix pin Cargo cache",
    )
    cache = steps[cache_index]
    cache_action = "Swatinem/rust-cache@d1527c8f67ef9af357290475d4af4b0fab8f12e0"
    assert cache.get("name") == "Cargo cache"
    assert cache.get("uses") == cache_action
    assert _string(cache, "if") == CARGO_NEEDED_GUARD
    assert cache.get("with") == {
        "workspaces": "cli",
        "shared-key": "curie-cli-release",
        "cache-on-failure": True,
    }
    assert probe_index < cache_index < release_build_index < helm_index < gate_index

    # Both jobs restore the same dependency cache and still build curie directly.
    rust_build_steps = _load_ci()["jobs"]["rust-build"]["steps"]
    assert isinstance(rust_build_steps, list)
    rust_build_cache = rust_build_steps[
        _single_step_index(
            rust_build_steps,
            lambda step: _string(step, "uses").startswith("Swatinem/rust-cache@"),
            "rust-build Cargo cache",
        )
    ]
    assert rust_build_cache.get("uses") == cache_action
    assert rust_build_cache.get("with", {}).get("shared-key") == "curie-cli-release"
    assert not any(
        _string(step, "uses").startswith("actions/download-artifact") for step in steps
    ), "the fix pin job must not download its curie binary"
    assert not any("chmod +x cli/target/release/curie" in _string(step, "run") for step in steps), (
        "the direct Cargo build creates the executable"
    )

    # Inside the Python job the gate inherited a booted stack. A `python`
    # selector naming an integration test dials Postgres and Valkey through
    # `uv run pytest <selector>`, so the split job has to boot its own or those
    # selectors fail for the environment rather than for the pin.
    sync_index = _single_step_index(
        steps,
        lambda step: _string(step, "run").strip() == "uv sync",
        "workspace sync",
    )
    stack_index = _single_step_index(
        steps,
        lambda step: (
            _string(step, "run").strip()
            == "python3 scripts/wait-for-langfuse.py --start --timeout-seconds 480"
        ),
        "dev stack startup",
    )
    migration_index = _single_step_index(
        steps,
        lambda step: "uv run alembic upgrade head" in _string(step, "run"),
        "shared database migration",
    )
    assert sync_index < probe_index
    assert helm_index < stack_index < migration_index < gate_index
    for index in (stack_index, migration_index):
        assert STACK_NEEDED_GUARD in _string(steps[index], "if"), (
            "a body with no live selector never reaches the verifier, so it "
            "must not pay for a compose stack either"
        )

    diagnostic_index = _single_step_index(
        steps,
        lambda step: "compose.dev.yaml logs" in _string(step, "run"),
        "failure stack diagnostic",
    )
    diagnostic_if = _string(steps[diagnostic_index], "if")
    assert "failure()" in diagnostic_if
    assert STACK_NEEDED_GUARD in diagnostic_if
    assert gate_index < diagnostic_index

    readiness_index = _single_step_index(
        steps,
        lambda step: _string(step, "run").strip() == "python3 scripts/wait-for-langfuse.py",
        "dev stack readiness",
    )
    assert STACK_NEEDED_GUARD in _string(steps[readiness_index], "if")
    assert stack_index < readiness_index < migration_index
    kind_index = _single_step_index(
        steps,
        lambda step: (
            _string(step, "uses").startswith("helm/kind-action@")
            and _string(step, "if") == KIND_NEEDED_GUARD
        ),
        "kind tools installation for the isolated registration pin",
    )
    assert steps[kind_index]["with"]["install_only"] is True
    assert probe_index < kind_index < gate_index
    disk_index = _single_step_index(
        steps,
        lambda step: "/usr/local/lib/android" in _string(step, "run"),
        "disk reclaim for the four candidate images",
    )
    assert _string(steps[disk_index], "if") == KIND_NEEDED_GUARD
    assert probe_index < disk_index < kind_index


CARGO_NEEDED_GUARD = "steps.fix-pin-curie.outputs.needed == 'true'"
STACK_NEEDED_GUARD = "steps.fix-pin-curie.outputs.needs_stack == 'true'"
KIND_NEEDED_GUARD = "steps.fix-pin-curie.outputs.needs_kind == 'true'"
FACTORY_UPGRADE_NEEDED_GUARD = "steps.fix-pin-curie.outputs.needs_factory_upgrade == 'true'"
SUBSTRING_GUARD = "contains(github.event.pull_request.body, 'Fix pin:')"


def _factory_upgrade_setup_steps() -> tuple[list[dict[str, Any]], int, int]:
    _, steps = _fix_pin_job()
    prepare_index = _single_step_index(
        steps,
        lambda step: (
            "TMPDIR=" in _string(step, "run") and "CURIE_C4_RELEASE_ASSETS=" in _string(step, "run")
        ),
        "source-local factory upgrade preparation",
    )
    download_index = _single_step_index(
        steps,
        lambda step: "releases/download/v0.12.5" in _string(step, "run"),
        "released factory upgrade asset download",
    )
    return steps, prepare_index, download_index


def _run_factory_upgrade_setup_step(
    step: dict[str, Any], environment: dict[str, str]
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", _string(step, "run")],
        cwd=environment["GITHUB_WORKSPACE"],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )


def _prepare_factory_upgrade_environment(tmp_path: Path) -> dict[str, str]:
    workspace = tmp_path / "source checkout"
    workspace.mkdir()
    runner_temp = tmp_path / "runner temp"
    runner_temp.mkdir()
    github_env = runner_temp / "github-env"
    github_env.touch()
    environment = {
        **os.environ,
        "GITHUB_WORKSPACE": str(workspace),
        "GITHUB_ENV": str(github_env),
        "RUNNER_TEMP": str(runner_temp),
    }
    for name in ("TMPDIR", "CURIE_C4_RELEASE_ASSETS"):
        environment.pop(name, None)
    steps, prepare_index, _ = _factory_upgrade_setup_steps()
    completed = _run_factory_upgrade_setup_step(steps[prepare_index], environment)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    exported = dict(
        line.split("=", 1) for line in github_env.read_text(encoding="utf-8").splitlines()
    )
    assert {"TMPDIR", "CURIE_C4_RELEASE_ASSETS"} <= exported.keys()
    return {**environment, **exported}


def test_factory_upgrade_workflow_prepares_assets_before_verification() -> None:
    steps, prepare_index, download_index = _factory_upgrade_setup_steps()
    probe_index = _single_step_index(steps, _is_fix_pin_probe, "fix pin requirements probe")
    gate_index = _single_step_index(steps, _is_fix_pin_gate, "fix pin verification")
    assert probe_index < prepare_index < download_index < gate_index
    for index in (prepare_index, download_index):
        step = steps[index]
        assert _string(step, "if") == FACTORY_UPGRADE_NEEDED_GUARD
        assert step.get("continue-on-error", False) is False
    assert not _string(steps[gate_index], "if"), "every declaration must reach the policy gate"


def test_factory_upgrade_setup_exports_source_local_paths(tmp_path: Path) -> None:
    environment = _prepare_factory_upgrade_environment(tmp_path)
    workspace = Path(environment["GITHUB_WORKSPACE"]).resolve()
    for name in ("TMPDIR", "CURIE_C4_RELEASE_ASSETS"):
        directory = Path(environment[name])
        assert directory.is_absolute()
        assert directory.is_dir(), f"{name} must exist before verification"
        assert directory.resolve().is_relative_to(workspace / ".projects"), name

    completed = subprocess.run(
        ["mktemp", "-d"],
        cwd=workspace,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert completed.returncode == 0, completed.stderr
    temporary_root = Path(completed.stdout.strip()).resolve()
    assert temporary_root.is_dir()
    assert temporary_root.parent == Path(environment["TMPDIR"]).resolve()
    assert (temporary_root / "worktree").is_relative_to(workspace / ".projects")


@pytest.mark.parametrize(
    ("corrupt_asset", "manifest_entries"),
    [
        pytest.param(None, "complete", id="verified-assets"),
        pytest.param("curie-0.12.5.tgz", "complete", id="corrupt-chart"),
        pytest.param("curie-x86_64-unknown-linux-gnu", "complete", id="corrupt-cli"),
        pytest.param(None, "missing-cli", id="missing-checksum"),
        pytest.param(None, "duplicate-cli", id="duplicate-checksum"),
    ],
)
def test_factory_upgrade_downloads_only_verified_release_assets(
    tmp_path: Path, corrupt_asset: str | None, manifest_entries: str
) -> None:
    environment = _prepare_factory_upgrade_environment(tmp_path)
    release_url = "https://github.com/curie-eng/curie/releases/download/v0.12.5"
    assets = {
        "curie-0.12.5.tgz": "released chart fixture\n",
        "curie-x86_64-unknown-linux-gnu": "released CLI fixture\n",
    }
    checksum_lines = [
        f"{hashlib.sha256(content.encode()).hexdigest()}  {name}\n"
        for name, content in assets.items()
    ]
    if manifest_entries == "missing-cli":
        checksum_lines.pop()
    elif manifest_entries == "duplicate-cli":
        checksum_lines.append(checksum_lines[-1])
    responses = {f"{release_url}/{name}": content for name, content in assets.items()}
    responses[f"{release_url}/checksums.txt"] = "".join(checksum_lines)
    if corrupt_asset is not None:
        responses[f"{release_url}/{corrupt_asset}"] += "corrupted download\n"

    # The public release serves these assets and their sha256sum manifest:
    # https://github.com/curie-eng/curie/releases/tag/v0.12.5
    # Replace GitHub responses while running the workflow's real checksum tools.
    download_boundary = "\n".join(
        [
            "import json, os, sys",
            "from pathlib import Path",
            "arguments = sys.argv[1:]",
            "urls = [value for value in arguments if value.startswith('https://')]",
            "assert len(urls) == 1, arguments",
            "url = urls[0]",
            "output_flag = '-o' if '-o' in arguments else '--output'",
            "destination = Path(arguments[arguments.index(output_flag) + 1])",
            "responses = json.loads(os.environ['FIX_PIN_RELEASE_RESPONSES'])",
            "assert url in responses, url",
            "print('FIX_PIN_DOWNLOAD ' + json.dumps("
            "{'url': url, 'destination': str(destination)}))",
            "destination.write_text(responses[url], encoding='utf-8')",
        ]
    )
    environment.update(
        {
            "FIX_PIN_RELEASE_RESPONSES": json.dumps(responses),
            "BASH_FUNC_curl%%": (
                f'() {{ {shlex.quote(sys.executable)} -c {shlex.quote(download_boundary)} "$@"; }}'
            ),
        }
    )
    steps, _, download_index = _factory_upgrade_setup_steps()
    completed = _run_factory_upgrade_setup_step(steps[download_index], environment)
    calls = [
        json.loads(line.removeprefix("FIX_PIN_DOWNLOAD "))
        for line in completed.stdout.splitlines()
        if line.startswith("FIX_PIN_DOWNLOAD ")
    ]
    assert calls, completed.stdout + completed.stderr
    assert calls[0]["url"] == f"{release_url}/checksums.txt"
    asset_directory = Path(environment["CURIE_C4_RELEASE_ASSETS"])
    for call in calls:
        filename = call["url"].rsplit("/", 1)[-1]
        assert Path(call["destination"]).resolve() == (asset_directory / filename).resolve()

    if corrupt_asset is not None or manifest_entries != "complete":
        assert completed.returncode != 0, completed.stdout + completed.stderr
        if corrupt_asset is not None:
            assert f"{corrupt_asset}: FAILED" in completed.stdout + completed.stderr
        return

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert len(calls) == 3
    assert {call["url"] for call in calls} == set(responses)
    for name, content in assets.items():
        assert (asset_directory / name).read_text(encoding="utf-8") == content


def test_selector_tooling_builds_only_when_the_parser_would_invoke_curie() -> None:
    """The cargo build must be gated on the declaration parser, not a substring.

    check.py reaches the binary at exactly one place, the `curie dev
    verify-fix-pin` call, and only once `declaration.selector` is set. `n/a`, no
    declaration, a near-miss marker, HTML-comment examples in the pull request
    template, and the bug-without-declaration rejection all return first.

    `#2228` gated cargo with `contains(..., 'Fix pin:')`. The default template
    embeds those exact bytes inside `<!-- -->`, so the 3.5 min release build
    still ran on every pull request that kept the template. The probe must
    reuse `_declaration` so a commented example cannot trigger the build, and
    a live `Fix pin: <selector>` line still cannot skip it.
    """
    job, steps = _fix_pin_job()

    probe = steps[_single_step_index(steps, _is_fix_pin_probe, "fix pin cargo probe")]
    assert probe.get("id") == "fix-pin-curie"
    assert "--needs-curie" in shlex.split(_string(probe, "run"))

    for description, predicate in (
        (
            "direct current release build",
            lambda step: (
                _string(step, "run").strip()
                == "cargo build --release --locked --manifest-path cli/Cargo.toml"
            ),
        ),
        (
            "pinned Helm setup",
            lambda step: (
                _string(step, "uses") == "azure/setup-helm@9bc31f4ebc9c6b171d7bfbaa5d006ae7abdb4310"
            ),
        ),
    ):
        step = steps[_single_step_index(steps, predicate, description)]
        condition = _string(step, "if")
        # The workflow runs only for pull requests, so these steps need only
        # the parser's answer.
        assert not _string(job, "if"), f"{description} must run on edited pull requests"
        assert CARGO_NEEDED_GUARD in condition, (
            f"{description} must build only when the parser says the binary "
            f"is needed: {condition!r}"
        )
        assert SUBSTRING_GUARD not in condition, (
            f"{description} must not use the template-matching substring: {condition!r}"
        )

    # The gate itself must NOT carry the cargo guard. It is the step that
    # decides a missing declaration is acceptable, and a body-shaped `if` on it
    # would turn the "closes a bug with no declaration" rejection into a
    # skipped step.
    gate = steps[_single_step_index(steps, _is_fix_pin_gate, "fix pin caller")]
    assert CARGO_NEEDED_GUARD not in _string(gate, "if"), (
        "the gate must still run for a body with no declaration, or the "
        "bug-without-declaration rejection can never fire"
    )
    assert SUBSTRING_GUARD not in _string(gate, "if")


def test_default_template_body_does_not_need_curie(tmp_path: Path) -> None:
    """The template's commented `Fix pin:` examples must not trigger cargo."""
    completed = _run_needs_curie(tmp_path, PR_TEMPLATE.read_text(encoding="utf-8"))

    assert completed.returncode == 0, completed.stderr
    assert _needed_output(tmp_path) == "needed=false\n"


def test_real_selector_line_needs_curie(tmp_path: Path) -> None:
    completed = _run_needs_curie(tmp_path, f"Fix pin: {VALID_SELECTOR}\n")

    assert completed.returncode == 0, completed.stderr
    assert _needed_output(tmp_path) == "needed=true\n"


def test_template_plus_real_selector_still_needs_curie(tmp_path: Path) -> None:
    body = PR_TEMPLATE.read_text(encoding="utf-8") + f"\nFix pin: {VALID_SELECTOR}\n"
    completed = _run_needs_curie(tmp_path, body)

    assert completed.returncode == 0, completed.stderr
    assert _needed_output(tmp_path) == "needed=true\n"


def test_not_applicable_does_not_need_curie(tmp_path: Path) -> None:
    """`n/a` skips the verifier today, so cargo is extra work, not a requirement.

    `#2228` documented building on `n/a` as a safe extra because `contains()`
    could not tell it from a selector. The parser can, and the binary is not
    invoked, so the probe reports needed=false.
    """
    completed = _run_needs_curie(
        tmp_path, "Fix pin: n/a - the fix is a chart template with no test surface\n"
    )

    assert completed.returncode == 0, completed.stderr
    assert _needed_output(tmp_path) == "needed=false\n"


def test_html_comment_selector_does_not_need_curie(tmp_path: Path) -> None:
    completed = _run_needs_curie(tmp_path, f"<!-- Fix pin: {VALID_SELECTOR} -->\n")

    assert completed.returncode == 0, completed.stderr
    assert _needed_output(tmp_path) == "needed=false\n"


def test_malformed_declaration_does_not_need_curie(tmp_path: Path) -> None:
    """A declaration error fails the gate without calling curie."""
    completed = _run_needs_curie(tmp_path, f" Fix pin: {VALID_SELECTOR}\n")

    assert completed.returncode == 0, completed.stderr
    assert _needed_output(tmp_path) == "needed=false\n"


def test_needs_curie_fails_closed_without_github_output(tmp_path: Path) -> None:
    completed = _run_needs_curie(tmp_path, f"Fix pin: {VALID_SELECTOR}\n", github_output=False)

    assert completed.returncode != 0
    assert "GITHUB_OUTPUT" in completed.stderr


@pytest.mark.parametrize(
    ("body", "needed", "stack", "kind", "factory_upgrade"),
    [
        (f"Fix pin: {ADMITTED_AGENT_SELECTOR}", True, False, True, False),
        (
            "Fix pin: tools/factory-e2e/tests/test_c4_admitted_agent.py::"
            "test_execute_revalidates_current_authority[agent_tenant-factory_tenant_mismatch]",
            True,
            False,
            True,
            False,
        ),
        (f"Fix pin: {INITIAL_BINDING_SELECTOR}", True, False, True, False),
        (f"Fix pin: {STATIC_TOKEN_ROTATION_SELECTOR}", True, False, True, False),
        (f"Fix pin: {HELD_CANCELLATION_SELECTOR}", True, False, True, False),
        (
            "Fix pin: tools/factory-e2e/tests/test_c4_initial_binding.py::"
            "test_quickstarts_are_idempotent_and_binding_guards_hold[gitlab]",
            True,
            False,
            True,
            False,
        ),
        (f"Fix pin: {FORGE_INSTANCES_SELECTOR}", True, False, True, False),
        (
            "Fix pin: tools/factory-e2e/tests/test_c4_forge_instances_extra.py::test_example",
            True,
            True,
            False,
            False,
        ),
        (f"Fix pin: {FACTORY_UPGRADE_SELECTOR}", True, False, True, True),
        (f"Fix pin: {FACTORY_UPGRADE_SELECTOR}[secondary]", True, False, True, True),
        (
            "Fix pin: tools/factory-e2e/tests/test_c4_upgrade_migration.py::"
            "TestUpgrade::test_secondary_path",
            True,
            False,
            True,
            True,
        ),
        (f"Fix pin: {VALID_SELECTOR}", True, True, False, False),
        (f"Fix pin: {FACTORY_SELECTOR}", True, True, False, False),
        (f"Fix pin: {LOCAL_SELECTOR}", True, True, False, False),
        (f"Fix pin: {CHART_SELECTOR}", True, True, False, False),
        (f"Fix pin: {NOTES_RELEASE_TARGET_SELECTOR}", True, False, True, False),
        (
            "Fix pin: charts/curie/ci/notes-release-target-extra-assertions.sh",
            True,
            True,
            False,
            False,
        ),
        (
            "Fix pin: charts/curie/ci/runtime/notes-release-target-assertions.sh",
            True,
            True,
            False,
            False,
        ),
        ("Fix pin: cli/tests/verify_fix_pin.rs::ci_gate_pins", True, True, False, False),
        (
            "Fix pin: tools/factory-e2e/tests/test_c4_initial_binding_extra.py::test_example",
            True,
            True,
            False,
            False,
        ),
        (
            "Fix pin: tools/factory-e2e/tests/test_c4_admitted_agent_extra.py::test_example",
            True,
            True,
            False,
            False,
        ),
        (
            "Fix pin: tools/factory-e2e/tests/test_c4_static_token_rotation_extra.py::test_example",
            True,
            True,
            False,
            False,
        ),
        (
            "Fix pin: tools/factory-e2e/tests/test_c4_held_cancellation_extra.py::test_example",
            True,
            True,
            False,
            False,
        ),
        (f"Fix pin: {FACTORY_UPGRADE_NEAR_MISS_SELECTOR}", True, True, False, False),
        (
            "Fix pin: tools/factory-e2e/tests/nested/test_c4_upgrade_migration.py::test_example",
            True,
            True,
            False,
            False,
        ),
        (None, False, False, False, False),
        ("", False, False, False, False),
        ("Fix pin: n/a - documentation only", False, False, False, False),
        (f"<!-- Fix pin: {ADMITTED_AGENT_SELECTOR} -->", False, False, False, False),
        (f" Fix pin: {ADMITTED_AGENT_SELECTOR}", False, False, False, False),
        (f"Verified with Fix pin: {ADMITTED_AGENT_SELECTOR}", False, False, False, False),
        (
            "Fix pin: tools/factory-e2e/tests/test_c4_admitted_agent.py::",
            False,
            False,
            False,
            False,
        ),
        (f"<!-- Fix pin: {INITIAL_BINDING_SELECTOR} -->", False, False, False, False),
        (f"<!-- Fix pin: {FACTORY_UPGRADE_SELECTOR} -->", False, False, False, False),
        (f" Fix pin: {INITIAL_BINDING_SELECTOR}", False, False, False, False),
        (f" Fix pin: {FACTORY_UPGRADE_SELECTOR}", False, False, False, False),
        (
            f"Fix pin: {INITIAL_BINDING_SELECTOR}\nFix pin: {VALID_SELECTOR}",
            False,
            False,
            False,
            False,
        ),
        (
            f"Fix pin: {FACTORY_UPGRADE_SELECTOR}\nFix pin: {VALID_SELECTOR}",
            False,
            False,
            False,
            False,
        ),
        ("Fix pin: --help", False, False, False, False),
    ],
)
def test_probe_selects_infrastructure_from_the_parsed_pin(
    tmp_path: Path,
    body: str | None,
    needed: bool,
    stack: bool,
    kind: bool,
    factory_upgrade: bool,
) -> None:
    completed = _run_needs_curie(tmp_path, body)
    assert completed.returncode == 0, completed.stderr
    expected = {
        "needed": str(needed).lower(),
        "needs_stack": str(stack).lower(),
        "needs_kind": str(kind).lower(),
        "needs_factory_upgrade": str(factory_upgrade).lower(),
    }
    assert _probe_outputs(tmp_path) == expected
    assert dict(line.split("=", 1) for line in completed.stdout.splitlines()) == expected


def test_pull_request_template_documents_the_required_declaration() -> None:
    template = PR_TEMPLATE.read_text(encoding="utf-8")

    assert "## Fix pin verification" in template
    assert "Fix pin: <supported selector>" in template
    for selector_shape in (
        "apps/*/tests/*.py::test",
        "packages/*/tests/*.py::test",
        "runner/tests/*.py::test",
        "cli/tests/local/test_*.py::test",
        "cli/tests/name.rs::test",
        "charts/curie/ci/name.sh",
    ):
        assert selector_shape in template
    assert re.search(
        r"non.fix.*does not close.*bug.*leave.*empty", template, flags=re.IGNORECASE | re.DOTALL
    )
    assert re.search(r"one.*selector.*changed", template, flags=re.IGNORECASE | re.DOTALL)
    assert re.search(r"REQUIRED.*closes.*bug", template, flags=re.IGNORECASE | re.DOTALL)
    assert "Fix pin: n/a - <reason>" in template
    # The template must state that the reason is mandatory, not merely mention
    # the word "reason" somewhere.
    assert re.search(r"non.empty\s+reason", template, flags=re.IGNORECASE)


BUG_LABELS = '["bug"]'
FOUND_LIVE_LABELS = '["bug", "found:live"]'
FOUND_UNIT_LABELS = '["bug", "found:unit"]'
FOUND_LOCAL_LABELS = '["bug", "found:local"]'
FOUND_CLUSTER_LABELS = '["bug", "found:cluster"]'

# Split across the slash so this repo's gitleaks `cross-repo-issue-ref` rule does not read the
# fixture as a real cross-repo citation. The checker must still see the joined form, because
# rejecting an owner-qualified reference is exactly what this case asserts.
CROSS_REPO_REFERENCE = "Closes another-owner" + "/some-repo#12"


def test_closing_a_bug_issue_without_a_declaration_fails_and_names_the_issue(
    tmp_path: Path,
) -> None:
    completed, call_log = _run_checker(
        tmp_path, "Fixes a crash.\n\nCloses #12\n", gh_labels=BUG_LABELS
    )
    shown = f"{completed.stdout}\n{completed.stderr}"

    assert completed.returncode != 0, shown
    assert "#12" in completed.stderr, shown
    assert "Fix pin" in completed.stderr, shown
    assert "SKIPPED: no Fix pin declaration" not in completed.stdout, shown
    assert not call_log.exists(), "a missing declaration must not reach curie"
    expected = [
        "api",
        "repos/curie-eng/curie/issues/12",
        "--jq",
        "{labels:[.labels[].name],body:.body}",
    ]
    calls = _gh_calls(tmp_path)
    assert calls
    assert all(call == expected for call in calls)


def test_closing_a_bug_issue_with_an_explicit_not_applicable_reason_passes(
    tmp_path: Path,
) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        "Closes #12\n\nFix pin: n/a - the fix is a chart template with no test surface\n",
        gh_labels=BUG_LABELS,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.startswith("SKIPPED: Fix pin declared not applicable")
    assert "chart template with no test surface" in completed.stdout
    assert not call_log.exists(), "an excused declaration must not run curie"


@pytest.mark.parametrize("dash", ["\u2014", "\u2013", "-"])
def test_not_applicable_accepts_every_supported_dash(tmp_path: Path, dash: str) -> None:
    completed, call_log = _run_checker(
        tmp_path, f"Closes #12\n\nFix pin: n/A {dash} documentation only\n", gh_labels=BUG_LABELS
    )

    assert completed.returncode == 0, completed.stderr
    assert "documentation only" in completed.stdout
    assert not call_log.exists(), "an excused declaration must not run curie"


@pytest.mark.parametrize(
    "declaration",
    ["Fix pin: n/a", "Fix pin: n/a \u2014", "Fix pin: n/a \u2014 "],
)
def test_not_applicable_without_a_reason_fails_closed_without_calling_curie(
    tmp_path: Path, declaration: str
) -> None:
    completed, call_log = _run_checker(
        tmp_path, f"Closes #12\n\n{declaration}\n", gh_labels=BUG_LABELS
    )

    assert completed.returncode != 0
    assert "Fix pin declaration error" in completed.stderr
    assert not call_log.exists(), "an unexplained escape must not reach curie"


def test_bug_label_is_found_anywhere_in_the_label_list(tmp_path: Path) -> None:
    """The gate tests membership, not the first or only label."""
    completed, call_log = _run_checker(tmp_path, "Closes #12\n", gh_labels='["priority", "bug"]')

    assert completed.returncode != 0, f"{completed.stdout}\n{completed.stderr}"
    assert "#12" in completed.stderr
    assert not call_log.exists(), "a missing declaration must not reach curie"


def test_not_applicable_reason_without_a_dash_fails_closed_without_calling_curie(
    tmp_path: Path,
) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        "Closes #12\n\nFix pin: n/a documentation only\n",
        gh_labels=BUG_LABELS,
    )

    assert completed.returncode != 0, f"{completed.stdout}\n{completed.stderr}"
    assert "Fix pin declaration error" in completed.stderr
    assert not call_log.exists(), "a dashless escape must not reach curie"


def test_closing_a_non_bug_issue_without_a_declaration_keeps_the_existing_skip(
    tmp_path: Path,
) -> None:
    completed, call_log = _run_checker(tmp_path, "Closes #12\n", gh_labels='["enhancement"]')

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "SKIPPED: no Fix pin declaration"
    assert not call_log.exists(), "a non bug pull request must not run curie"


@pytest.mark.parametrize(
    "body",
    [
        "Closes #12",
        "closes #12",
        "Close #12",
        "Closed #12",
        "Closes: #12",
        "Fixes #12",
        "Fixed #12",
        "Fix #12",
        "Resolves #12",
        "Resolve #12",
        "Resolved #12",
        "Closes #12, #13",
        "Closes #12 and #13",
    ],
)
def test_closing_keyword_variants_all_require_a_declaration(tmp_path: Path, body: str) -> None:
    completed, call_log = _run_checker(tmp_path, body, gh_labels=BUG_LABELS)

    assert completed.returncode != 0, f"{completed.stdout}\n{completed.stderr}"
    assert "#12" in completed.stderr
    assert _gh_call_log(tmp_path).exists(), "a same repository closure must be looked up"
    assert not call_log.exists(), "a missing declaration must not reach curie"


@pytest.mark.parametrize(
    "body",
    [
        CROSS_REPO_REFERENCE,
        "Closes https://github.com/owner/repo/issues/12",
        "See #12 for background",
        "<!-- Closes #12 -->",
        "Closes #",
    ],
)
def test_non_closing_references_are_not_looked_up_and_still_skip(tmp_path: Path, body: str) -> None:
    completed, call_log = _run_checker(tmp_path, body, gh_labels=BUG_LABELS)

    assert completed.returncode == 0, f"{completed.stdout}\n{completed.stderr}"
    assert completed.stdout.strip() == "SKIPPED: no Fix pin declaration"
    assert not _gh_call_log(tmp_path).exists(), "only same repository closures may be looked up"
    assert not call_log.exists(), "a non closing reference must not run curie"


def test_label_lookup_failure_fails_closed_without_calling_curie(tmp_path: Path) -> None:
    completed, call_log = _run_checker(tmp_path, "Closes #12", gh_exit=1)

    assert completed.returncode != 0
    assert "#12" in completed.stderr
    assert not call_log.exists(), "an unreadable label API must not open the gate"


def test_missing_gh_fails_closed_without_calling_curie(tmp_path: Path) -> None:
    completed, call_log = _run_checker(tmp_path, "Closes #12", gh_on_path=False)

    assert completed.returncode != 0
    assert "#12" in completed.stderr
    assert not call_log.exists(), "an unavailable label API must not open the gate"


def test_unit_pin_for_a_found_live_issue_fails_without_a_waiver(tmp_path: Path) -> None:
    """A unit selector cannot pin a defect found on a live surface (#2243)."""
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {VALID_SELECTOR}\n",
        gh_labels=FOUND_LIVE_LABELS,
    )
    shown = f"{completed.stdout}\n{completed.stderr}"

    assert completed.returncode != 0, shown
    assert "found:live" in completed.stderr, shown
    assert "unit" in completed.stderr, shown
    assert "Fix pin waiver:" in completed.stderr, shown
    assert not call_log.exists(), "a pin below the discovery surface must not reach curie"


def test_unit_pin_for_a_found_live_issue_passes_with_a_waiver(tmp_path: Path) -> None:
    """The explicit waiver is the only way a below-surface pin may proceed."""
    completed, call_log = _run_checker(
        tmp_path,
        (
            f"Closes #12\n\nFix pin: {VALID_SELECTOR}\n"
            "Fix pin waiver: live retest is manual; the unit pin covers the regression\n"
        ),
        gh_labels=FOUND_LIVE_LABELS,
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(call_log.read_text(encoding="utf-8")) == [
        "dev",
        "verify-fix-pin",
        "HEAD",
        VALID_SELECTOR,
    ]


def test_live_pin_for_a_found_live_issue_passes_without_a_waiver(tmp_path: Path) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {LIVE_SELECTOR}\n",
        gh_labels=FOUND_LIVE_LABELS,
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(call_log.read_text(encoding="utf-8")) == [
        "dev",
        "verify-fix-pin",
        "HEAD",
        LIVE_SELECTOR,
    ]


def test_helm_render_pin_for_a_found_live_issue_fails_without_a_waiver(
    tmp_path: Path,
) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {CHART_SELECTOR}\n",
        gh_labels=FOUND_LIVE_LABELS,
    )
    shown = f"{completed.stdout}\n{completed.stderr}"

    assert completed.returncode != 0, shown
    assert "found:live" in completed.stderr, shown
    assert "cluster" in completed.stderr, shown
    assert not call_log.exists(), "a helm-render pin must not pass for a live-found issue"


def test_unit_pin_for_a_found_unit_issue_passes_without_a_waiver(tmp_path: Path) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {VALID_SELECTOR}\n",
        gh_labels=FOUND_UNIT_LABELS,
    )

    assert completed.returncode == 0, completed.stderr
    assert call_log.exists(), "an at-surface pin must still be verified"


def test_unit_pin_for_a_found_local_issue_fails_without_a_waiver(tmp_path: Path) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {VALID_SELECTOR}\n",
        gh_labels=FOUND_LOCAL_LABELS,
    )
    shown = f"{completed.stdout}\n{completed.stderr}"

    assert completed.returncode != 0, shown
    assert "found:local" in completed.stderr, shown
    assert not call_log.exists(), "a unit pin must not pass for a local-found issue"


@pytest.mark.parametrize("gh_labels", [FOUND_UNIT_LABELS, FOUND_LOCAL_LABELS])
def test_local_pin_satisfies_unit_and_local_discovery_surfaces(
    tmp_path: Path, gh_labels: str
) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {LOCAL_SELECTOR}\n",
        gh_labels=gh_labels,
    )

    assert completed.returncode == 0, completed.stderr
    assert call_log.exists(), "an at surface local pin must be verified"


@pytest.mark.parametrize("gh_labels", [FOUND_CLUSTER_LABELS, FOUND_LIVE_LABELS])
def test_local_pin_refuses_cluster_and_live_discovery_surfaces_without_a_waiver(
    tmp_path: Path, gh_labels: str
) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {LOCAL_SELECTOR}\n",
        gh_labels=gh_labels,
    )
    shown = f"{completed.stdout}\n{completed.stderr}"

    assert completed.returncode != 0, shown
    assert "local" in completed.stderr, shown
    assert not call_log.exists(), "a below surface local pin must not reach curie"


def test_local_test_live_filename_is_still_a_local_pin(tmp_path: Path) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {LOCAL_LIVE_SELECTOR}\n",
        gh_labels=FOUND_CLUSTER_LABELS,
    )
    shown = f"{completed.stdout}\n{completed.stderr}"

    assert completed.returncode != 0, shown
    assert "is a local pin" in completed.stderr, shown
    assert not call_log.exists(), "a local path named test_live remains below cluster"


def test_helm_render_pin_for_a_found_cluster_issue_passes_without_a_waiver(
    tmp_path: Path,
) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {CHART_SELECTOR}\n",
        gh_labels=FOUND_CLUSTER_LABELS,
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(call_log.read_text(encoding="utf-8")) == [
        "dev",
        "verify-fix-pin",
        "HEAD",
        CHART_SELECTOR,
    ]


def test_nested_chart_runtime_selector_meets_the_cluster_floor(tmp_path: Path) -> None:
    """charts/curie/ci/runtime/ scripts are cluster pins like top-level ones (#4367)."""
    selector = "charts/curie/ci/runtime/workspace-dependency-diagnostic-runtime.sh"
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {selector}\n",
        gh_labels=FOUND_CLUSTER_LABELS,
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(call_log.read_text(encoding="utf-8")) == [
        "dev",
        "verify-fix-pin",
        "HEAD",
        selector,
    ]


def test_unlabeled_bug_with_a_unit_pin_keeps_no_tier_floor(tmp_path: Path) -> None:
    """Existing bugs without a found:* label keep the pre-#2243 behaviour."""
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {VALID_SELECTOR}\n",
        gh_labels=BUG_LABELS,
    )

    assert completed.returncode == 0, completed.stderr
    assert call_log.exists(), "an unlabeled bug must still verify a declared selector"


def test_waiver_without_a_reason_fails_closed_without_calling_curie(tmp_path: Path) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {VALID_SELECTOR}\nFix pin waiver:\n",
        gh_labels=FOUND_LIVE_LABELS,
    )

    assert completed.returncode != 0, f"{completed.stdout}\n{completed.stderr}"
    assert "Fix pin waiver" in completed.stderr
    assert not call_log.exists(), "an unexplained waiver must not reach curie"


def test_duplicate_waiver_lines_fail_closed_without_calling_curie(tmp_path: Path) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        (
            f"Closes #12\n\nFix pin: {VALID_SELECTOR}\n"
            "Fix pin waiver: first\n"
            "Fix pin waiver: second\n"
        ),
        gh_labels=FOUND_LIVE_LABELS,
    )

    assert completed.returncode != 0, f"{completed.stdout}\n{completed.stderr}"
    assert "Fix pin waiver" in completed.stderr
    assert not call_log.exists(), "duplicate waivers must not reach curie"


def test_pin_tier_comes_from_selector_location_not_from_prose(tmp_path: Path) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        (f"Closes #12\n\nThis pin is a live Slack test.\nFix pin: {VALID_SELECTOR}\n"),
        gh_labels=FOUND_LIVE_LABELS,
    )
    shown = f"{completed.stdout}\n{completed.stderr}"

    assert completed.returncode != 0, shown
    assert "unit" in completed.stderr, shown
    assert not call_log.exists(), "prose claiming a higher tier must not raise the pin"


def test_found_live_in_the_issue_body_without_the_label_still_raises_the_floor(
    tmp_path: Path,
) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {VALID_SELECTOR}\n",
        gh_labels=BUG_LABELS,
        gh_body="### Discovery surface\n\nfound:live\n",
    )
    shown = f"{completed.stdout}\n{completed.stderr}"

    assert completed.returncode != 0, shown
    assert "found:live" in completed.stderr, shown
    assert not call_log.exists()


def test_found_live_mentioned_in_issue_prose_does_not_raise_the_floor(
    tmp_path: Path,
) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {VALID_SELECTOR}\n",
        gh_labels=BUG_LABELS,
        gh_body=(
            "### Discovery surface\n\nfound:unit\n\n"
            "### What happened\n\nThis was not found:live; it failed in pytest.\n"
        ),
    )

    assert completed.returncode == 0, completed.stderr
    assert call_log.exists(), "narrative found:* mentions must not raise the floor"


def test_strictest_found_label_across_closed_bugs_is_the_floor(tmp_path: Path) -> None:
    """A fake gh returns one label list; the gate must still fail on found:live."""
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12, #13\n\nFix pin: {VALID_SELECTOR}\n",
        gh_labels='["bug", "found:unit", "found:live"]',
    )
    shown = f"{completed.stdout}\n{completed.stderr}"

    assert completed.returncode != 0, shown
    assert "found:live" in completed.stderr, shown
    assert not call_log.exists()


def test_python_suite_collects_the_fix_pin_gate_tests() -> None:
    text = PYPROJECT.read_text(encoding="utf-8")
    assert '"tools/fix-pin-ci/tests"' in text


def test_bug_report_template_records_the_discovery_surface() -> None:
    template = BUG_REPORT.read_text(encoding="utf-8")
    for label in ("found:unit", "found:local", "found:cluster", "found:live"):
        assert label in template, f"bug template must name {label}"


def test_pull_request_template_documents_the_tier_waiver() -> None:
    template = PR_TEMPLATE.read_text(encoding="utf-8")
    assert "Fix pin waiver: <reason>" in template
    assert "found:live" in template


MAIN_MILESTONE = "v0.8.5"


@pytest.mark.parametrize(
    ("gh_milestone", "base_ref"),
    [
        ("v0.8.5", "next"),
        ("v0.8.6", "main"),
        (None, "next"),
        ("v9.9.9", "next"),
    ],
    ids=["patch_on_next", "feature_on_main", "missing", "unknown"],
)
def test_valid_fix_pin_ignores_closed_issue_milestone(
    tmp_path: Path, gh_milestone: str | None, base_ref: str
) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {VALID_SELECTOR}\n",
        gh_labels=BUG_LABELS,
        gh_milestone=gh_milestone,
        base_ref=base_ref,
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(call_log.read_text(encoding="utf-8")) == [
        "dev",
        "verify-fix-pin",
        "HEAD",
        VALID_SELECTOR,
    ]
    assert not _pull_lookup_calls(tmp_path), "a direct train needs no prerequisite lookup"


def test_not_applicable_fix_pin_ignores_issue_milestone_on_main(tmp_path: Path) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        "Closes #12\n\nFix pin: n/a - milestone does not change this test surface\n",
        gh_labels=BUG_LABELS,
        gh_milestone="v0.9.2",
        base_ref="main",
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.startswith("SKIPPED: Fix pin declared not applicable")
    assert _gh_call_log(tmp_path).exists(), "a closed issue must be looked up even when excused"
    assert not _pull_lookup_calls(tmp_path), "a direct main pull request needs no stack lookup"
    assert not call_log.exists(), "an excused declaration must not run curie"


def test_one_exact_same_repository_prerequisite_is_accepted_before_fix_pin_verification(
    tmp_path: Path,
) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {LOCAL_SELECTOR}\n",
        gh_labels=BUG_LABELS,
        gh_milestone=MAIN_MILESTONE,
        base_ref=STACK_BRANCH,
        gh_pulls=json.dumps([_prerequisite()]),
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(call_log.read_text(encoding="utf-8")) == [
        "dev",
        "verify-fix-pin",
        "HEAD",
        LOCAL_SELECTOR,
    ]
    _assert_exact_pull_lookup(tmp_path)


def _invalid_prerequisite_payload(case: str) -> str:
    prerequisite = _prerequisite()
    if case == "none":
        return "[]"
    if case == "ambiguous":
        return json.dumps([prerequisite, _prerequisite()])
    if case == "closed":
        prerequisite["state"] = "closed"
    elif case == "head fork":
        prerequisite["head"]["repo"]["full_name"] = "acme-corp/acme-bot"
    elif case == "base fork":
        prerequisite["base"]["repo"]["full_name"] = "acme-corp/acme-bot"
    elif case == "head ref":
        prerequisite["head"]["ref"] = "task/other"
    elif case == "stale sha":
        prerequisite["head"]["sha"] = "b" * 40
    elif case == "missing sha":
        del prerequisite["head"]["sha"]
    elif case == "malformed sha":
        prerequisite["head"]["sha"] = "not a commit sha"
    elif case == "deeper chain":
        prerequisite["base"]["ref"] = "task/earlier"
    elif case == "malformed result":
        return json.dumps([{"state": "open"}])
    elif case == "not an array":
        return json.dumps({"state": "open"})
    elif case == "invalid json":
        return "{"
    else:
        raise AssertionError(f"unknown prerequisite case: {case}")
    return json.dumps([prerequisite])


@pytest.mark.parametrize(
    "case",
    [
        "none",
        "ambiguous",
        "closed",
        "head fork",
        "base fork",
        "head ref",
        "stale sha",
        "missing sha",
        "malformed sha",
        "deeper chain",
        "malformed result",
        "not an array",
        "invalid json",
    ],
)
def test_invalid_direct_prerequisite_refuses_before_curie(tmp_path: Path, case: str) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {LOCAL_SELECTOR}\n",
        gh_labels=BUG_LABELS,
        gh_milestone=MAIN_MILESTONE,
        base_ref=STACK_BRANCH,
        gh_pulls=_invalid_prerequisite_payload(case),
    )

    assert completed.returncode != 0, f"{completed.stdout}\n{completed.stderr}"
    assert not call_log.exists(), f"{case} must refuse before curie"
    _assert_exact_pull_lookup(tmp_path)


@pytest.mark.parametrize(
    ("base_sha", "include_base_sha"),
    [(None, True), ("not a commit sha", True), (BASE_SHA, False)],
)
def test_missing_or_malformed_event_base_sha_refuses_before_lookup_and_curie(
    tmp_path: Path, base_sha: object, include_base_sha: bool
) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {LOCAL_SELECTOR}\n",
        gh_labels=BUG_LABELS,
        gh_milestone=MAIN_MILESTONE,
        base_ref=STACK_BRANCH,
        base_sha=base_sha,
        include_base_sha=include_base_sha,
        gh_pulls=json.dumps([_prerequisite()]),
    )

    assert completed.returncode != 0, f"{completed.stdout}\n{completed.stderr}"
    assert not call_log.exists(), "an invalid event base sha must refuse before curie"
    assert not _pull_lookup_calls(tmp_path), "an invalid event base sha must refuse before API"


def test_dependent_base_repository_must_match_the_event_repository(tmp_path: Path) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {LOCAL_SELECTOR}\n",
        gh_labels=BUG_LABELS,
        gh_milestone=MAIN_MILESTONE,
        base_ref=STACK_BRANCH,
        base_repository="other/curie",
        gh_pulls=json.dumps([_prerequisite()]),
    )

    assert completed.returncode != 0, f"{completed.stdout}\n{completed.stderr}"
    assert not call_log.exists(), "a cross repository dependent must refuse before curie"
    assert not _pull_lookup_calls(tmp_path), "a cross repository dependent must refuse before API"


def test_non_task_base_ref_is_not_treated_as_a_prerequisite_branch(tmp_path: Path) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {LOCAL_SELECTOR}\n",
        gh_labels=BUG_LABELS,
        gh_milestone=MAIN_MILESTONE,
        base_ref="feature/not-a-task-branch",
        gh_pulls=json.dumps([_prerequisite(head_ref="feature/not-a-task-branch")]),
    )

    assert completed.returncode != 0, f"{completed.stdout}\n{completed.stderr}"
    assert not call_log.exists(), "an unsupported base branch must refuse before curie"
    assert not _pull_lookup_calls(tmp_path), "only task branches may resolve a prerequisite"


def test_prerequisite_api_failure_refuses_before_curie(tmp_path: Path) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {LOCAL_SELECTOR}\n",
        gh_labels=BUG_LABELS,
        gh_milestone=MAIN_MILESTONE,
        base_ref=STACK_BRANCH,
        gh_pulls_exit=1,
    )

    assert completed.returncode != 0, f"{completed.stdout}\n{completed.stderr}"
    assert not call_log.exists(), "an unreadable prerequisite API must refuse before curie"
    _assert_exact_pull_lookup(tmp_path)


def test_stacked_pull_request_without_gh_refuses_before_curie(tmp_path: Path) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {LOCAL_SELECTOR}\n",
        gh_labels=BUG_LABELS,
        gh_milestone=MAIN_MILESTONE,
        base_ref=STACK_BRANCH,
        gh_on_path=False,
    )

    assert completed.returncode != 0, f"{completed.stdout}\n{completed.stderr}"
    assert not call_log.exists(), "missing gh must refuse before curie"


def test_valid_prerequisite_does_not_compare_issue_milestone_to_parent_train(
    tmp_path: Path,
) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {LOCAL_SELECTOR}\n",
        gh_labels=BUG_LABELS,
        gh_milestone="v0.8.6",
        base_ref=STACK_BRANCH,
        gh_pulls=json.dumps([_prerequisite(base_ref="main")]),
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads(call_log.read_text(encoding="utf-8")) == [
        "dev",
        "verify-fix-pin",
        "HEAD",
        LOCAL_SELECTOR,
    ]
    _assert_exact_pull_lookup(tmp_path)


def test_agents_md_does_not_describe_milestones_as_a_merge_gate() -> None:
    agents = (REPO_ROOT / "AGENTS.md").read_text(encoding="utf-8")
    heading = "## Release train, branch, and commit conventions"
    start = agents.index(heading)
    window = agents[start : start + 2500]

    assert "milestone-trains.json" not in window
    assert "milestone belongs to the other train fails" not in window
    assert "`bug` issue with no milestone fails" not in window
    assert "`main`" in window and "`next`" in window
