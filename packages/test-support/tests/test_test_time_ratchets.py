"""Behavior checks for generated test-time baselines and the pytest budget."""

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pytest
from curie_test_support.duration_budget import budget_failure
from curie_test_support.test_ratchets import canonical_nodeid

ROOT = Path(__file__).resolve().parents[3]
BASELINE = "tests/baselines/test-time.json"


@pytest.fixture(autouse=True)
def private_step_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)


def command(root: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "curie_test_support.test_ratchets", "--root", str(root), *arguments],
        text=True,
        capture_output=True,
        check=False,
    )


def source(root: Path, path: str, text: str) -> None:
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text)


def generate(root: Path) -> dict[str, Any]:
    result = command(root, "--regenerate")
    assert result.returncode == 0, result.stdout + result.stderr
    return cast(dict[str, Any], json.loads((root / BASELINE).read_text()))


@pytest.mark.parametrize("argument", ["1", "delay", "-1", "seconds=1"])
def test_sleep_new_violation_fails(tmp_path: Path, argument: str) -> None:
    source(tmp_path, "tests/test_case.py", "import time\ndef test_case(): pass\n")
    generate(tmp_path)
    source(
        tmp_path, "tests/test_case.py", f"import time\ndef test_case(): time.sleep({argument})\n"
    )
    result = command(tmp_path)
    assert result.returncode == 1
    assert "sleeps" in result.stdout


def test_sleep_removed_baseline_entry_is_reported_and_pruned(tmp_path: Path) -> None:
    source(tmp_path, "tests/test_case.py", "import time\ntime.sleep(1)\n")
    assert generate(tmp_path)["sleeps"] == {"tests/test_case.py": 1}
    source(tmp_path, "tests/test_case.py", "import time\ntime.sleep(0)\n")
    result = command(tmp_path)
    assert result.returncode == 0
    assert "stale" in result.stdout
    generate(tmp_path)
    assert command(tmp_path).returncode == 0


def test_unchanged_sleeps_and_zero_yields_pass(tmp_path: Path) -> None:
    source(
        tmp_path,
        "tests/test_case.py",
        """import time as clock
from asyncio import sleep as yield_for
clock.sleep(0)
yield_for(0.0)
clock.sleep(0.1)
yield_for(delay)
""",
    )
    assert generate(tmp_path)["sleeps"] == {"tests/test_case.py": 2}
    assert command(tmp_path).returncode == 0


FIXTURE = """import pytest
from helpers import Provisioner
@pytest.fixture{scope}
def cluster():
    provisioner = Provisioner()
    provisioner.setup()
"""
HELPER = """import subprocess
class Provisioner:
    def setup(self): self.create()
    def create(self): subprocess.run(["kind", "create", "cluster"])
"""


def test_new_per_case_fixture_fails_through_imported_helper(tmp_path: Path) -> None:
    source(tmp_path, "helpers.py", HELPER)
    source(tmp_path, "tests/test_case.py", FIXTURE.format(scope='(scope="session")'))
    assert generate(tmp_path)["fixtures"] == []
    source(tmp_path, "tests/test_case.py", FIXTURE.format(scope=""))
    result = command(tmp_path)
    assert result.returncode == 1
    assert "tests/test_case.py::cluster" in result.stdout


@pytest.mark.parametrize("scope", ['(scope="module")', '(scope="session")'])
def test_removed_fixture_baseline_entry_is_reported_and_pruned(tmp_path: Path, scope: str) -> None:
    source(tmp_path, "helpers.py", HELPER)
    source(tmp_path, "tests/test_case.py", FIXTURE.format(scope=""))
    assert generate(tmp_path)["fixtures"] == ["tests/test_case.py::cluster"]
    assert command(tmp_path).returncode == 0
    source(tmp_path, "tests/test_case.py", FIXTURE.format(scope=scope))
    assert "stale" in command(tmp_path).stdout
    assert command(tmp_path).returncode == 0
    generate(tmp_path)
    assert command(tmp_path).returncode == 0


def test_platform_install_and_fixture_dependency_are_detected(tmp_path: Path) -> None:
    source(
        tmp_path,
        "tests/conftest.py",
        """from pytest import fixture as resource
import subprocess
@resource
def install():
    subprocess.run([curie_binary, "cluster", "up", "--install-platform"])
@resource
def wrapper(install): return install
""",
    )
    assert generate(tmp_path)["fixtures"] == [
        "tests/conftest.py::install",
        "tests/conftest.py::wrapper",
    ]


def test_deleted_test_file_is_stale(tmp_path: Path) -> None:
    source(tmp_path, "tests/test_case.py", "import asyncio\nasyncio.sleep(delay)\n")
    generate(tmp_path)
    (tmp_path / "tests/test_case.py").unlink()
    assert command(tmp_path).returncode == 0
    assert "stale" in command(tmp_path).stdout
    assert generate(tmp_path)["sleeps"] == {}


@pytest.mark.parametrize("contents", ["{}", "{", '{"sleeps": [], "fixtures": [], "durations": []}'])
def test_malformed_baseline_fails_closed(tmp_path: Path, contents: str) -> None:
    source(tmp_path, BASELINE, contents)
    assert command(tmp_path).returncode != 0


def test_missing_baseline_fails_closed(tmp_path: Path) -> None:
    assert command(tmp_path).returncode != 0


def test_duration_baseline_uses_call_phase_and_normalizes_xdist(tmp_path: Path) -> None:
    artifact = tmp_path / "durations.log"
    artifact.write_text("""30.00s setup tests/test_case.py::test_setup
31.00s call tests/test_case.py::test_slow@tests/test_case.py
19.99s call tests/test_case.py::test_fast
40.00s teardown tests/test_case.py::test_teardown
""")
    source(tmp_path, "tests/test_case.py", "def test_slow(): pass\n")
    artifact2 = tmp_path / "second.log"
    artifact2.write_text(artifact.read_text())
    result = command(
        tmp_path, "--regenerate", "--durations", str(artifact), "--durations", str(artifact2)
    )
    assert result.returncode == 0, result.stdout + result.stderr
    baseline = json.loads((tmp_path / BASELINE).read_text())
    assert baseline["durations"] == ["tests/test_case.py::test_slow"]


def test_duration_new_violation_fails() -> None:
    assert "exceeds" in (budget_failure("tests/test_case.py::test_slow", 30.01, set()) or "")


def test_fast_exempt_duration_never_fails_ci() -> None:
    assert (
        budget_failure("tests/test_case.py::test_slow", 0.1, {"tests/test_case.py::test_slow"})
        is None
    )


def test_duration_unchanged_state_passes() -> None:
    assert (
        budget_failure("tests/test_case.py::test_slow", 31, {"tests/test_case.py::test_slow"})
        is None
    )
    assert budget_failure("tests/test_case.py::test_fast", 30, set()) is None
    assert (
        canonical_nodeid("tests/test_case.py::test_slow@group") == "tests/test_case.py::test_slow"
    )


def test_regeneration_cannot_grow_existing_baseline(tmp_path: Path) -> None:
    source(tmp_path, "tests/test_case.py", "import time\ntime.sleep(1)\n")
    generate(tmp_path)
    source(tmp_path, "tests/test_case.py", "import time\ntime.sleep(1)\ntime.sleep(2)\n")
    result = command(tmp_path, "--regenerate")
    assert result.returncode == 1
    assert "grow" in result.stdout


def test_duration_plugin_drives_real_pytest_reports(tmp_path: Path) -> None:
    source(
        tmp_path,
        "test_case.py",
        """import time
import pytest
@pytest.fixture
def setup_delay(): time.sleep(0.01)
def test_fast(setup_delay): pass
""",
    )
    source(tmp_path, "baseline.json", json.dumps({"sleeps": {}, "fixtures": [], "durations": []}))
    argv = [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "-p",
        "curie_test_support.duration_budget",
        "--test-duration-budget",
        "--test-time-baseline",
        str(tmp_path / "baseline.json"),
    ]
    env = {key: value for key, value in os.environ.items() if key != "PYTEST_ADDOPTS"}
    env["GITHUB_STEP_SUMMARY"] = str(tmp_path / "summary.md")
    result = subprocess.run(argv, cwd=tmp_path, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    source(
        tmp_path,
        "baseline.json",
        json.dumps({"sleeps": {}, "fixtures": [], "durations": ["test_case.py::test_fast"]}),
    )
    result = subprocess.run(argv, cwd=tmp_path, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "test_case.py::test_fast: maximum call" in (tmp_path / "summary.md").read_text()


@pytest.mark.parametrize(
    "invocation",
    [
        'run("kind", "create", "cluster")',
        'argv = ["kind", "create", "cluster"]; run(argv)',
        'run(curie, "cluster", "up")',
    ],
)
def test_provisioning_command_shapes(tmp_path: Path, invocation: str) -> None:
    source(
        tmp_path,
        "tests/test_case.py",
        "import pytest\n@pytest.fixture\ndef cluster(): " + invocation + "\n",
    )
    assert generate(tmp_path)["fixtures"] == ["tests/test_case.py::cluster"]


def test_duration_parameter_identity_is_preserved() -> None:
    assert (
        canonical_nodeid("tests/test_case.py::test_user[a@example.com]")
        == "tests/test_case.py::test_user[a@example.com]"
    )


def duration_artifacts(root: Path, *seconds: float) -> list[str]:
    arguments = []
    for index, duration in enumerate(seconds):
        artifact = root / f"run-{index}.log"
        artifact.write_text(f"{duration}s call tests/test_case.py::test_case\n")
        arguments.extend(["--durations", str(artifact)])
    return arguments


@pytest.mark.parametrize(
    ("first", "second", "retained"), [(8, 9, False), (8, 10, True), (8, 31, True)]
)
def test_duration_regeneration_uses_maximum_and_hysteresis(
    tmp_path: Path, first: float, second: float, retained: bool
) -> None:
    source(tmp_path, "tests/test_case.py", "def test_case(): pass\n")
    assert command(tmp_path, "--regenerate", *duration_artifacts(tmp_path, 31, 32)).returncode == 0
    result = command(tmp_path, "--regenerate", *duration_artifacts(tmp_path, first, second))
    assert result.returncode == 0, result.stdout + result.stderr
    assert bool(json.loads((tmp_path / BASELINE).read_text())["durations"]) is retained


def test_duration_regeneration_requires_two_runs(tmp_path: Path) -> None:
    assert command(tmp_path, "--regenerate", *duration_artifacts(tmp_path, 31)).returncode != 0


def test_deleted_duration_test_is_reported_then_pruned(tmp_path: Path) -> None:
    source(tmp_path, "tests/test_case.py", "def test_case(): pass\n")
    assert command(tmp_path, "--regenerate", *duration_artifacts(tmp_path, 31, 32)).returncode == 0
    source(tmp_path, "tests/test_case.py", "def test_other(): pass\n")
    assert command(tmp_path).returncode == 0
    assert "stale" in command(tmp_path).stdout
    assert generate(tmp_path)["durations"] == []


def test_removed_duration_parameter_is_ignored_at_collection(tmp_path: Path) -> None:
    source(
        tmp_path,
        "test_case.py",
        'import pytest\n@pytest.mark.parametrize("value", ["current"])\n'
        "def test_case(value): pass\n",
    )
    source(
        tmp_path,
        "baseline.json",
        json.dumps(
            {"sleeps": {}, "fixtures": [], "durations": ["test_case.py::test_case[removed]"]}
        ),
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-p",
            "curie_test_support.duration_budget",
            "--test-duration-budget",
            "--test-time-baseline",
            str(tmp_path / "baseline.json"),
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    source(tmp_path, BASELINE, (tmp_path / "baseline.json").read_text())
    for index in range(2):
        source(tmp_path, f"run-{index}.log", result.stdout)
    regenerated = command(
        tmp_path,
        "--regenerate",
        "--durations",
        str(tmp_path / "run-0.log"),
        "--durations",
        str(tmp_path / "run-1.log"),
    )
    assert regenerated.returncode == 0, regenerated.stdout + regenerated.stderr
    assert json.loads((tmp_path / BASELINE).read_text())["durations"] == []


@pytest.mark.parametrize(
    "path",
    [
        "tests/queue_fixtures.py",
        "tools/factory-e2e/tests/c4_admitted_kind.py",
        "packages/test-support/src/helpers.py",
    ],
)
def test_helpers_anyio_and_assignment_aliases_are_scanned(tmp_path: Path, path: str) -> None:
    source(
        tmp_path, path, "import anyio as io\nwait = io.sleep\nalias = wait\nalias(1)\nalias(0)\n"
    )
    assert generate(tmp_path)["sleeps"] == {path: 1}


def git(root: Path, *argv: str) -> str:
    result = subprocess.run(["git", *argv], cwd=root, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout.strip()


def test_ci_ratchets_compare_merge_base_not_advanced_tip(tmp_path: Path) -> None:
    git(tmp_path, "init", "-q", "-b", "main")
    git(tmp_path, "config", "user.name", "Example")
    git(tmp_path, "config", "user.email", "test@example.com")
    git(tmp_path, "config", "core.hooksPath", os.devnull)
    source(tmp_path, "tests/test_case.py", "import time\ntime.sleep(1)\n")
    generate(tmp_path)
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-qm", "Initial test costs")
    git(tmp_path, "update-ref", "refs/remotes/origin/main", "HEAD")
    git(tmp_path, "branch", "feature")
    source(tmp_path, "tests/test_case.py", "import time\ntime.sleep(0)\n")
    generate(tmp_path)
    git(tmp_path, "commit", "-qam", "Remove a sleep")
    git(tmp_path, "update-ref", "refs/remotes/origin/main", "HEAD")
    git(tmp_path, "checkout", "-q", "feature")
    assert command(tmp_path).returncode == 0
    source(tmp_path, "tests/queue_fixtures.py", "import anyio\nanyio.sleep(1)\n")
    assert command(tmp_path).returncode == 1


@pytest.mark.parametrize(
    ("event", "base", "ref", "expected"),
    [
        ("pull_request", "next", "123/merge", "origin/next"),
        ("push", "", "next", "origin/next"),
        ("workflow_dispatch", "", "main", "origin/main"),
        ("schedule", "", "next", "origin/next"),
        ("merge_group", "", "gh-readonly-queue/next/test", "origin/next"),
    ],
)
def test_ci_event_base_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, event: str, base: str, ref: str, expected: str
) -> None:
    from curie_test_support.test_ratchets import base_reference

    monkeypatch.setenv("GITHUB_EVENT_NAME", event)
    monkeypatch.setenv("GITHUB_BASE_REF", base)
    monkeypatch.setenv("GITHUB_REF_NAME", ref)
    payload = tmp_path / "event.json"
    payload.write_text(json.dumps({"merge_group": {"base_ref": "refs/heads/next"}}))
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(payload))
    assert base_reference() == expected


def test_stale_sleep_is_written_to_step_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source(tmp_path, "tests/queue_fixtures.py", "import time\ntime.sleep(1)\n")
    generate(tmp_path)
    source(tmp_path, "tests/queue_fixtures.py", "import time\ntime.sleep(0)\n")
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    assert command(tmp_path).returncode == 0
    assert "stale tests/queue_fixtures.py" in summary.read_text()


def test_new_exemption_cannot_bypass_merge_base_budget(tmp_path: Path) -> None:
    git(tmp_path, "init", "-q", "-b", "main")
    git(tmp_path, "config", "user.name", "Example")
    git(tmp_path, "config", "user.email", "test@example.com")
    git(tmp_path, "config", "core.hooksPath", os.devnull)
    source(tmp_path, "tests/test_case.py", "def test_case(): pass\n")
    generate(tmp_path)
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-qm", "Initial budget")
    git(tmp_path, "update-ref", "refs/remotes/origin/main", "HEAD")
    # Editing the working exemption file must not admit a larger baseline.
    result = command(tmp_path, "--regenerate", *duration_artifacts(tmp_path, 31, 32))
    assert result.returncode == 1
    assert "durations: new violation" in result.stdout


def test_xdist_transports_duration_collection_observations(tmp_path: Path) -> None:
    source(tmp_path, "test_case.py", "def test_case(): pass\n")
    source(
        tmp_path,
        "baseline.json",
        json.dumps({"sleeps": {}, "fixtures": [], "durations": ["test_case.py::test_case"]}),
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-n",
            "2",
            "-p",
            "curie_test_support.duration_budget",
            "--test-duration-budget",
            "--test-time-baseline",
            str(tmp_path / "baseline.json"),
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert 'test-time collected: {"test_case.py": ["test_case.py::test_case"]}' in result.stdout
    assert "s call test_case.py::test_case" in result.stdout
