"""Executable contract for the required fix pin pull request gate.

The helper is intentionally exercised as a subprocess. Its observable contract
is a pull request body, an event action, and an argv call to the already built
``curie`` binary. The workflow assertions read CI itself because a helper that
is never called cannot protect a merge.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
CHECKER = REPO_ROOT / "tools" / "fix-pin-ci" / "check.py"
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yaml"
PR_TEMPLATE = REPO_ROOT / ".github" / "PULL_REQUEST_TEMPLATE.md"
BUG_REPORT = REPO_ROOT / ".github" / "ISSUE_TEMPLATE" / "bug_report.yml"
PYPROJECT = REPO_ROOT / "pyproject.toml"
VERIFY_FIX_PIN = REPO_ROOT / "cli" / "scripts" / "verify-fix-pin.sh"

VALID_SELECTOR = "apps/api/tests/test_fix_pin_ci_gate.py::test_exact_declaration"
LIVE_SELECTOR = "runner/tests/test_live.py::test_example"
CHART_SELECTOR = "charts/curie/ci/render-assertions.sh"
PR_CONDITION = re.compile(r"github\.event_name\s*==\s*['\"]pull_request['\"]")


def _write_event(
    tmp_path: Path,
    body: str | None,
    *,
    action: str = "opened",
    include_body: bool = True,
    base_ref: str = "next",
) -> Path:
    pull_request: dict[str, object] = {"base": {"ref": base_ref}}
    if include_body:
        pull_request["body"] = body
    event_path = tmp_path / "event.json"
    event_path.write_text(
        json.dumps({"action": action, "pull_request": pull_request}), encoding="utf-8"
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
                f"Path(os.environ['{call_log_environment}']).write_text("
                "json.dumps(sys.argv[1:]))",
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
    """The gate reads `{labels, body, milestone}` so found:* and train mapping work."""
    parsed = json.loads(gh_labels)
    if isinstance(parsed, list):
        return json.dumps(
            {"labels": parsed, "body": gh_body, "milestone": gh_milestone}
        )
    return gh_labels


def _write_fake_gh(tmp_path: Path) -> tuple[Path, Path]:
    """Install a fake ``gh`` in its own directory so PATH shadows nothing else."""
    return _write_fake_binary(
        tmp_path,
        "gh",
        call_log_environment="FIX_PIN_GH_CALL_LOG",
        output_environment="FIX_PIN_GH_LABELS",
        exit_environment="FIX_PIN_GH_EXIT",
        subdirectory="gh-bin",
        default_output="[]",
        print_end="\n",
    )


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
    gh_on_path: bool = True,
    base_ref: str = "next",
) -> tuple[subprocess.CompletedProcess[str], Path]:
    event_path = _write_event(
        tmp_path, body, action=action, include_body=include_body, base_ref=base_ref
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
        "GITHUB_REPOSITORY": "curie-eng/curie",
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
    return _github_output(tmp_path).read_text(encoding="utf-8")


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
def test_supported_nested_python_selectors_call_the_verifier(
    tmp_path: Path, selector: str
) -> None:
    completed, call_log = _run_checker(tmp_path, f"Fix pin: {selector}")

    assert completed.returncode == 0, completed.stderr
    assert json.loads(call_log.read_text(encoding="utf-8")) == [
        "dev",
        "verify-fix-pin",
        "HEAD",
        selector,
    ]


def test_ordinary_fix_pin_prose_skips_without_calling_the_verifier(tmp_path: Path) -> None:
    completed, call_log = _run_checker(
        tmp_path, "This closes the fix pin-related enforcement gap"
    )

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
        "apps/api/tests/test_fix_pin_ci_gate.py",
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


def test_changed_selected_python_test_is_pinned_by_real_pytest_junit_failure(
    tmp_path: Path,
) -> None:
    repository = tmp_path / "repository"
    test_path = repository / "apps" / "api" / "tests" / "test_pin.py"
    source_path = repository / "apps" / "api" / "pin_fixture.py"
    test_path.parent.mkdir(parents=True)
    (repository / "pyproject.toml").write_text(
        """[project]
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
""",
        encoding="utf-8",
    )
    for package in (
        repository / "apps" / "__init__.py",
        repository / "apps" / "api" / "__init__.py",
        repository / "apps" / "api" / "tests" / "__init__.py",
    ):
        package.write_text("", encoding="utf-8")
    source_path.write_text("def value():\n    return 1\n", encoding="utf-8")
    test_path.write_text(
        """from apps.api.pin_fixture import value


def test_selected():
    assert value() == 1
""",
        encoding="utf-8",
    )

    git_command = [
        "git",
        "-c",
        "commit.gpgsign=false",
        "-c",
        "core.hooksPath=/dev/null",
    ]
    for arguments in (
        ["init", "-q"],
        ["config", "user.name", "Curie Test"],
        ["config", "user.email", "curie@example.com"],
        ["add", "."],
        ["commit", "-q", "-m", "Add Python fixture"],
    ):
        subprocess.run(
            [*git_command, *arguments],
            cwd=repository,
            check=True,
        )

    source_path.write_text("def value():\n    return 2\n", encoding="utf-8")
    test_path.write_text(
        """from apps.api.pin_fixture import value


def test_selected():
    assert value() == 2
""",
        encoding="utf-8",
    )
    for arguments in (
        ["add", "."],
        ["commit", "-q", "-m", "Fix Python behavior"],
    ):
        subprocess.run(
            [*git_command, *arguments],
            cwd=repository,
            check=True,
        )
    fix_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        capture_output=True,
        check=True,
        text=True,
    ).stdout.strip()

    completed = subprocess.run(
        [
            "bash",
            str(VERIFY_FIX_PIN),
            fix_commit,
            "apps/api/tests/test_pin.py::test_selected",
        ],
        cwd=repository,
        capture_output=True,
        check=False,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        text=True,
    )
    shown = f"{completed.stdout}\n{completed.stderr}"

    assert completed.returncode == 0, shown
    assert "PINNED" in completed.stdout.splitlines(), shown
    assert "1 failed" in shown, shown


def test_committed_pull_request_template_skips_without_calling_curie(tmp_path: Path) -> None:
    completed, call_log = _run_checker(tmp_path, PR_TEMPLATE.read_text(encoding="utf-8"))

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "SKIPPED: no Fix pin declaration"
    assert not call_log.exists(), "the template instruction must not activate the verifier"


def _load_ci() -> dict[str, Any]:
    document = yaml.safe_load(CI_WORKFLOW.read_text(encoding="utf-8"))
    assert isinstance(document, dict), "ci.yaml must be a YAML mapping"
    return document


def _workflow_trigger(document: dict[str, Any]) -> dict[str, Any]:
    trigger = document.get("on", document.get(True))
    assert isinstance(trigger, dict), "ci.yaml must declare an on mapping"
    return trigger


def _python_job(document: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    jobs = document.get("jobs")
    assert isinstance(jobs, dict), "ci.yaml must declare jobs"
    job = jobs.get("python")
    assert isinstance(job, dict), "ci.yaml must retain the python job"
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


def _pull_request_only(step: dict[str, Any]) -> bool:
    return bool(PR_CONDITION.search(_string(step, "if")))


def test_ci_keeps_the_required_python_status_and_calls_fix_pin_after_pytest() -> None:
    document = _load_ci()
    trigger = _workflow_trigger(document)
    pull_request = trigger.get("pull_request")
    assert isinstance(pull_request, dict), "CI must run for pull requests"
    actions = pull_request.get("types")
    assert isinstance(actions, list), "CI must declare its pull request actions"
    assert len(actions) == 4 and set(actions) == {
        "opened",
        "synchronize",
        "reopened",
        "edited",
    }, "CI must rerun required checks when code or the pull request body changes"

    job, steps = _python_job(document)
    assert job.get("name") == "Python (ruff + mypy + pytest)"
    assert "needs" not in job, "the required Python check must not be skippable"

    # A job-level permissions block replaces the workflow-level one, so the job
    # must grant both the checkout scope and the Issues scope the gate reads
    # closed issue labels with.
    permissions = job.get("permissions")
    assert isinstance(permissions, dict), "the Python job must declare job level permissions"
    assert permissions.get("contents") == "read"
    assert permissions.get("issues") == "read"

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
        lambda step: "docker compose -f compose.dev.yaml up -d" in _string(step, "run"),
        "dev stack startup",
    )
    migration_index = _single_step_index(
        steps,
        lambda step: "uv run alembic upgrade head" in _string(step, "run"),
        "shared database migration",
    )
    # Matched on the prefix, not on equality. What this assertion is for is that
    # the normal suite runs, unfiltered, before the gate; reporting flags like
    # --durations do not bear on that, and pinning the exact string made a
    # profiling flag look like a contract change.
    pytest_index = _single_step_index(
        steps,
        lambda step: _string(step, "run").strip().startswith("uv run pytest -q"),
        "normal Python suite",
    )
    pytest_command = shlex.split(_string(steps[pytest_index], "run").strip())
    assert pytest_command[:4] == ["uv", "run", "pytest", "-q"]
    assert all(
        argument.startswith("--durations") for argument in pytest_command[4:]
    ), (
        "the Python suite must run unfiltered: only reporting flags may be added "
        f"to `uv run pytest -q`, got {pytest_command!r}"
    )
    probe_index = _single_step_index(steps, _is_fix_pin_probe, "fix pin cargo probe")
    gate_index = _single_step_index(steps, _is_fix_pin_gate, "fix pin caller")
    gate = steps[gate_index]
    assert _pull_request_only(gate), "the verifier must not run for pushes"
    assert stack_index < migration_index < pytest_index < gate_index

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

    probe = steps[probe_index]
    assert _string(probe, "if") == "github.event_name == 'pull_request'", (
        "the cargo probe must run for every pull request, including bodies with "
        "no live selector; gating it on its own output would skip the decision"
    )
    assert probe.get("id") == "fix-pin-curie"
    assert shlex.split(_string(probe, "run")) == [
        "python3",
        "tools/fix-pin-ci/check.py",
        "--event",
        "$GITHUB_EVENT_PATH",
        "--needs-curie",
    ]
    assert '--event "$GITHUB_EVENT_PATH"' in _string(probe, "run")

    release_build_index = _single_step_index(
        steps,
        lambda step: _string(step, "run").strip()
        == "cargo build --release --locked --manifest-path cli/Cargo.toml",
        "direct current release build",
    )
    helm_index = _single_step_index(
        steps,
        lambda step: _string(step, "uses")
        == "azure/setup-helm@9bc31f4ebc9c6b171d7bfbaa5d006ae7abdb4310"
        and (step.get("with") or {}).get("version") == "v3.16.4",
        "pinned Helm setup",
    )
    for index in (release_build_index, helm_index):
        assert _pull_request_only(steps[index]), "selector tooling must not affect push runs"
        assert pytest_index < index < gate_index
    assert pytest_index < probe_index < release_build_index < helm_index < gate_index
    assert not any(
        _string(step, "uses") == "Swatinem/rust-cache@v2" for step in steps
    ), "the Python job must build directly without a Cargo cache dependency"
    assert not any(
        _string(step, "uses") == "actions/download-artifact@v8" for step in steps
    ), "the Python job must not download its curie binary"
    assert not any(
        "chmod +x cli/target/release/curie" in _string(step, "run") for step in steps
    ), "the direct Cargo build creates the executable"

    diagnostic_index = _single_step_index(
        steps,
        lambda step: "compose.dev.yaml logs" in _string(step, "run"),
        "failure stack diagnostic",
    )
    diagnostic_if = _string(steps[diagnostic_index], "if")
    assert "failure()" in diagnostic_if
    assert "steps.python-runtime.outputs.pytest == 'true'" in diagnostic_if
    assert gate_index < diagnostic_index


CARGO_NEEDED_GUARD = "steps.fix-pin-curie.outputs.needed == 'true'"
SUBSTRING_GUARD = "contains(github.event.pull_request.body, 'Fix pin:')"


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
    document = _load_ci()
    _, steps = _python_job(document)

    probe = steps[_single_step_index(steps, _is_fix_pin_probe, "fix pin cargo probe")]
    assert probe.get("id") == "fix-pin-curie"
    assert "--needs-curie" in shlex.split(_string(probe, "run"))

    for description, predicate in (
        (
            "direct current release build",
            lambda step: _string(step, "run").strip()
            == "cargo build --release --locked --manifest-path cli/Cargo.toml",
        ),
        (
            "pinned Helm setup",
            lambda step: _string(step, "uses")
            == "azure/setup-helm@9bc31f4ebc9c6b171d7bfbaa5d006ae7abdb4310",
        ),
    ):
        step = steps[_single_step_index(steps, predicate, description)]
        condition = _string(step, "if")
        assert PR_CONDITION.search(condition), f"{description} must stay pull-request only"
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


def test_pull_request_template_documents_the_required_declaration() -> None:
    template = PR_TEMPLATE.read_text(encoding="utf-8")

    assert "## Fix pin verification" in template
    assert "Fix pin: <supported selector>" in template
    for selector_shape in (
        "apps/*/tests/*.py::test",
        "packages/*/tests/*.py::test",
        "runner/tests/*.py::test",
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
    assert json.loads(_gh_call_log(tmp_path).read_text(encoding="utf-8")) == [
        "api",
        "repos/curie-eng/curie/issues/12",
        "--jq",
        "{labels:[.labels[].name],body:.body,milestone:.milestone.title}",
    ]


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
    completed, call_log = _run_checker(
        tmp_path, "Closes #12\n", gh_labels='["priority", "bug"]'
    )

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
    completed, call_log = _run_checker(
        tmp_path, "Closes #12\n", gh_labels='["enhancement"]'
    )

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
def test_non_closing_references_are_not_looked_up_and_still_skip(
    tmp_path: Path, body: str
) -> None:
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
        (
            f"Closes #12\n\nThis pin is a live Slack test.\n"
            f"Fix pin: {VALID_SELECTOR}\n"
        ),
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


FEATURE_MILESTONE = "v0.8.6"
PATCH_MILESTONE = "v0.8.5"
MAPPING_PATH = REPO_ROOT / "tools" / "fix-pin-ci" / "milestone-trains.json"
NA_BODY = "Closes #12\n\nFix pin: n/a - the fix is a chart template with no test surface\n"


def test_matching_milestone_train_passes(tmp_path: Path) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        NA_BODY,
        gh_labels=BUG_LABELS,
        gh_milestone=FEATURE_MILESTONE,
        base_ref="next",
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.startswith("SKIPPED: Fix pin declared not applicable")
    assert _gh_call_log(tmp_path).exists(), "a closed issue must be looked up even when excused"
    assert not call_log.exists(), "an excused declaration must not run curie"


def test_matching_patch_milestone_on_main_passes(tmp_path: Path) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        NA_BODY,
        gh_labels=BUG_LABELS,
        gh_milestone=PATCH_MILESTONE,
        base_ref="main",
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.startswith("SKIPPED: Fix pin declared not applicable")
    assert _gh_call_log(tmp_path).exists(), "a closed issue must be looked up even when excused"
    assert not call_log.exists(), "an excused declaration must not run curie"


def test_mismatched_milestone_train_fails(tmp_path: Path) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        NA_BODY,
        gh_labels=BUG_LABELS,
        gh_milestone=PATCH_MILESTONE,
        base_ref="next",
    )
    shown = f"{completed.stdout}\n{completed.stderr}"

    assert completed.returncode != 0, shown
    assert "#12" in completed.stderr, shown
    assert PATCH_MILESTONE in completed.stderr, shown
    assert "main" in completed.stderr, shown
    assert "next" in completed.stderr, shown
    assert not call_log.exists(), "a train mismatch must not reach curie"


def test_missing_milestone_on_a_bug_fails(tmp_path: Path) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        NA_BODY,
        gh_labels=BUG_LABELS,
        gh_milestone=None,
        base_ref="next",
    )
    shown = f"{completed.stdout}\n{completed.stderr}"

    assert completed.returncode != 0, shown
    assert "#12" in completed.stderr, shown
    assert "milestone" in completed.stderr.lower(), shown
    assert not call_log.exists(), "a bug without a milestone must not reach curie"


def test_missing_milestone_on_a_non_bug_is_allowed(tmp_path: Path) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        "Closes #12\n",
        gh_labels='["enhancement"]',
        gh_milestone=None,
        base_ref="next",
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "SKIPPED: no Fix pin declaration"
    assert not call_log.exists(), "a non bug without a milestone must not run curie"


def test_mismatched_train_fails_even_when_a_selector_is_declared(tmp_path: Path) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        f"Closes #12\n\nFix pin: {VALID_SELECTOR}\n",
        gh_labels=BUG_LABELS,
        gh_milestone=PATCH_MILESTONE,
        base_ref="next",
    )
    shown = f"{completed.stdout}\n{completed.stderr}"

    assert completed.returncode != 0, shown
    assert PATCH_MILESTONE in completed.stderr, shown
    assert not call_log.exists(), "a train mismatch must not reach the verifier"


def test_unknown_milestone_fails_closed(tmp_path: Path) -> None:
    completed, call_log = _run_checker(
        tmp_path,
        NA_BODY,
        gh_labels=BUG_LABELS,
        gh_milestone="v9.9.9",
        base_ref="next",
    )
    shown = f"{completed.stdout}\n{completed.stderr}"

    assert completed.returncode != 0, shown
    assert "v9.9.9" in completed.stderr, shown
    assert not call_log.exists(), "an unmapped milestone must not open the gate"


def test_milestone_mapping_sends_patch_to_main_and_feature_to_next() -> None:
    mapping = json.loads(MAPPING_PATH.read_text(encoding="utf-8"))
    trains = mapping["trains"]
    milestones = mapping["milestones"]

    assert trains == {"patch": "main", "feature": "next"}
    assert milestones[FEATURE_MILESTONE] == "feature"
    assert milestones[PATCH_MILESTONE] == "patch"
    assert set(trains.values()) == {"main", "next"}
    assert set(milestones.values()) <= {"patch", "feature"}


def test_agents_md_cites_the_mapping_next_to_the_release_train_table() -> None:
    agents = (REPO_ROOT / "AGENTS.md").read_text(encoding="utf-8")
    heading = "## Release train, branch, and commit conventions"
    start = agents.index(heading)
    window = agents[start : start + 2500]

    assert "milestone-trains.json" in window
    assert "`main`" in window and "`next`" in window
