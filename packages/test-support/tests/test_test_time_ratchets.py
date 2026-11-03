"""Behavior checks for generated test-time baselines and the pytest budget."""

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pytest
from curie_test_support.duration_budget import budget_failure
from curie_test_support.ratchets import canonical_nodeid

ROOT = Path(__file__).resolve().parents[3]
BASELINE = "tests/baselines/test-time.json"


@pytest.fixture(autouse=True)
def private_step_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)


def pin_main_pr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_EVENT_NAME", "pull_request")
    monkeypatch.setenv("GITHUB_BASE_REF", "main")
    monkeypatch.setenv("GITHUB_REF_NAME", "task/example")


def command(root: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "curie_test_support.ratchets", "--root", str(root), *arguments],
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
    artifact.write_text("""test-time source run: 12345
30.00s setup tests/test_case.py::test_setup
31.00s call tests/test_case.py::test_slow@tests/test_case.py
19.99s call tests/test_case.py::test_fast
40.00s teardown tests/test_case.py::test_teardown
""")
    source(tmp_path, "tests/test_case.py", "def test_slow(): pass\n")
    artifact2 = tmp_path / "second.log"
    artifact2.write_text(artifact.read_text().replace("12345", "12346"))
    result = command(
        tmp_path, "--regenerate", "--durations", str(artifact), "--durations", str(artifact2)
    )
    assert result.returncode == 0, result.stdout + result.stderr
    baseline = json.loads((tmp_path / BASELINE).read_text())
    assert baseline["durations"] == ["tests/test_case.py::test_slow"]


def test_duration_new_violation_fails() -> None:
    assert "exceeds" in (
        budget_failure("tests/test_case.py::test_slow", 40.01, {"tests/test_case.py::test_slow"})
        or ""
    )


def test_under_budget_duration_never_fails_ci() -> None:
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
    source(
        tmp_path,
        "baseline.json",
        json.dumps({"sleeps": {}, "fixtures": [], "durations": []}),
    )
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
        json.dumps(
            {
                "sleeps": {},
                "fixtures": [],
                "durations": ["test_case.py::test_fast"],
            }
        ),
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
        artifact.write_text(
            f"test-time source run: {12345 + index}\n"
            f"{duration}s call tests/test_case.py::test_case\n"
        )
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
            {
                "sleeps": {},
                "fixtures": [],
                "durations": ["test_case.py::test_case[removed]"],
            }
        ),
    )
    for index in range(2):
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
            env={**os.environ, "GITHUB_RUN_ID": str(12345 + index)},
            text=True,
            capture_output=True,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        source(tmp_path, f"run-{index}.log", result.stdout)
    source(tmp_path, BASELINE, (tmp_path / "baseline.json").read_text())
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


@pytest.mark.parametrize("bootstrap", [False, True])
def test_train_promotion_accepts_existing_debt_and_refuses_new_growth(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bootstrap: bool
) -> None:
    pin_main_pr(monkeypatch)
    git(tmp_path, "init", "-q", "-b", "main")
    git(tmp_path, "config", "user.name", "Example")
    git(tmp_path, "config", "user.email", "test@example.com")
    git(tmp_path, "config", "core.hooksPath", os.devnull)
    source(tmp_path, "tests/test_main.py", "import time\ntime.sleep(1)\n")
    main_baseline = generate(tmp_path)
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-qm", "Accepted main debt")
    git(tmp_path, "update-ref", "refs/remotes/origin/main", "HEAD")
    git(tmp_path, "checkout", "-qb", "next")
    source(tmp_path, "tests/test_next.py", "import time\ntime.sleep(1)\n")
    next_baseline = {**main_baseline, "sleeps": {**main_baseline["sleeps"], "tests/test_next.py": 1}}
    if bootstrap:
        (tmp_path / BASELINE).unlink()
    else:
        source(tmp_path, BASELINE, json.dumps(next_baseline))
    git(tmp_path, "add", "-A")
    git(tmp_path, "commit", "-qm", "Accepted next debt")
    git(tmp_path, "update-ref", "refs/remotes/origin/next", "HEAD")
    git(tmp_path, "checkout", "-qb", "task/promotion")
    base = "origin/next" if bootstrap else "origin/main"
    if bootstrap:
        source(tmp_path, BASELINE, json.dumps(next_baseline))
    result = command(tmp_path, "--base-ref", base)
    assert result.returncode == 0, result.stdout + result.stderr
    source(tmp_path, "tests/test_new.py", "import time\ntime.sleep(1)\n")
    result = command(tmp_path, "--base-ref", base)
    assert result.returncode == 1, result.stdout + result.stderr
    assert "tests/test_new.py" in result.stdout
    if not bootstrap:
        next_baseline["sleeps"]["tests/test_new.py"] = 1
        source(tmp_path, BASELINE, json.dumps(next_baseline))
        assert command(tmp_path, "--base-ref", base).returncode == 1


def test_ci_ratchets_compare_merge_base_not_advanced_tip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pin_main_pr(monkeypatch)
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
    from curie_test_support.ratchets import base_reference

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


def test_slow_observations_can_be_recorded_without_growing_static_costs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pin_main_pr(monkeypatch)
    git(tmp_path, "init", "-q", "-b", "main")
    git(tmp_path, "config", "user.name", "Example")
    git(tmp_path, "config", "user.email", "test@example.com")
    git(tmp_path, "config", "core.hooksPath", os.devnull)
    source(tmp_path, "tests/test_case.py", "def test_case(): pass\n")
    generate(tmp_path)
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-qm", "Initial budget")
    git(tmp_path, "update-ref", "refs/remotes/origin/main", "HEAD")
    result = command(tmp_path, "--regenerate", *duration_artifacts(tmp_path, 31, 32))
    assert result.returncode == 0, result.stdout
    assert json.loads((tmp_path / BASELINE).read_text())["durations"] == [
        "tests/test_case.py::test_case"
    ]


def test_xdist_transports_duration_collection_observations(tmp_path: Path) -> None:
    source(tmp_path, "test_case.py", "def test_case(): pass\n")
    source(
        tmp_path,
        "baseline.json",
        json.dumps(
            {
                "sleeps": {},
                "fixtures": [],
                "durations": ["test_case.py::test_case"],
            }
        ),
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


@pytest.mark.parametrize("kind", ["sleeps", "fixtures"])
def test_committed_baseline_cannot_undercount_the_scan(
    tmp_path: Path, kind: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    pin_main_pr(monkeypatch)
    git(tmp_path, "init", "-q", "-b", "main")
    git(tmp_path, "config", "user.name", "Example")
    git(tmp_path, "config", "user.email", "test@example.com")
    git(tmp_path, "config", "core.hooksPath", os.devnull)
    code = (
        "import time\nimport pytest\n@pytest.fixture\n"
        "def cluster(): run('kind', 'create', 'cluster')\n"
        "def test_case(): time.sleep(1); time.sleep(2)\n"
    )
    source(tmp_path, "tests/test_case.py", code)
    baseline = generate(tmp_path)
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-qm", "Initial test costs")
    git(tmp_path, "update-ref", "refs/remotes/origin/main", "HEAD")
    assert command(tmp_path).returncode == 0
    if kind == "sleeps":
        baseline[kind]["tests/test_case.py"] -= 1
    else:
        baseline[kind] = []
    source(tmp_path, BASELINE, json.dumps(baseline))
    git(tmp_path, "commit", "-qam", "Undercount the committed baseline")
    before_merge = command(tmp_path)
    git(tmp_path, "update-ref", "refs/remotes/origin/main", "HEAD")
    after_merge = command(tmp_path)
    assert before_merge.returncode == after_merge.returncode == 1, before_merge.stdout
    fixed = (
        code.replace("; time.sleep(2)", "")
        if kind == "sleeps"
        else code.replace("@pytest.fixture", '@pytest.fixture(scope="session")')
    )
    source(tmp_path, "tests/test_case.py", fixed)
    assert command(tmp_path).returncode == 0


def test_six_shards_from_one_ci_run_do_not_satisfy_two_run_rule(tmp_path: Path) -> None:
    source(tmp_path, "tests/test_case.py", "def test_case(): pass\n")
    arguments = []
    for shard in range(6):
        path = tmp_path / f"shard-{shard}.log"
        path.write_text("test-time source run: 12345\n31s call tests/test_case.py::test_case\n")
        arguments.extend(["--durations", str(path)])
    result = command(tmp_path, "--regenerate", *arguments)
    assert result.returncode == 2, result.stdout
    assert "two" in result.stdout


@pytest.mark.parametrize("run_id", ["", "local", "0"])
def test_unidentified_logs_do_not_satisfy_two_run_rule(tmp_path: Path, run_id: str) -> None:
    arguments = []
    for index in range(2):
        path = tmp_path / f"unidentified-{index}.log"
        path.write_text(f"test-time source run: {run_id}\n31s call tests/test_case.py::test_case\n")
        arguments.extend(["--durations", str(path)])
    result = command(tmp_path, "--regenerate", *arguments)
    assert result.returncode == 2, result.stdout
    assert "two CI runs" in result.stdout


def test_combined_artifact_with_two_ci_run_ids_is_accepted(tmp_path: Path) -> None:
    source(tmp_path, "tests/test_case.py", "def test_case(): pass\n")
    path = tmp_path / "combined.log"
    path.write_text(
        "test-time source run: 12345\n31s call tests/test_case.py::test_case\n"
        "test-time source run: 12346\n32s call tests/test_case.py::test_case\n"
    )
    result = command(tmp_path, "--regenerate", "--durations", str(path))
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads((tmp_path / BASELINE).read_text())["durations"] == [
        "tests/test_case.py::test_case"
    ]


def test_removing_a_current_observation_stops_reporting_it_when_fast(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pin_main_pr(monkeypatch)
    git(tmp_path, "init", "-q", "-b", "main")
    git(tmp_path, "config", "user.name", "Example")
    git(tmp_path, "config", "user.email", "test@example.com")
    git(tmp_path, "config", "core.hooksPath", os.devnull)
    source(tmp_path, "tests/test_case.py", "def test_case(): pass\n")
    source(tmp_path, "conftest.py", (ROOT / "conftest.py").read_text())
    assert command(tmp_path, "--regenerate", *duration_artifacts(tmp_path, 31, 32)).returncode == 0
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-qm", "Initial duration exemption")
    git(tmp_path, "update-ref", "refs/remotes/origin/main", "HEAD")
    baseline = json.loads((tmp_path / BASELINE).read_text())
    baseline["durations"] = []
    source(tmp_path, BASELINE, json.dumps(baseline))
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in {"PYTEST_ADDOPTS", "GITHUB_EVENT_NAME", "GITHUB_BASE_REF"}
    }
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "--ci-shard", "1/1"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "1 passed" in result.stdout
    assert "s call tests/test_case.py::test_case" not in result.stdout


@pytest.mark.parametrize("seconds", [31.462, 42.426, 34.827, 120])
def test_unrelated_existing_slow_test_cannot_fail_the_shard(seconds: float) -> None:
    assert budget_failure("tests/test_case.py::test_unrelated", seconds, set()) is None


def test_changed_test_has_ten_seconds_of_budget_headroom() -> None:
    nodeid = "tests/test_case.py::test_changed"
    assert budget_failure(nodeid, 40, {nodeid}) is None
    assert "exceeds" in (budget_failure(nodeid, 40.01, {nodeid}) or "")


@pytest.mark.parametrize(
    "nodeid", ["tests/test_case.py::test_case[a]", "tests/test_case.py::test_case[a@example.com]"]
)
def test_independent_xdist_group_is_removed_from_parameterized_nodeid(nodeid: str) -> None:
    assert canonical_nodeid(nodeid + "@" + nodeid) == nodeid


def duration_history(root: Path, monkeypatch: pytest.MonkeyPatch, before: str, after: str) -> str:
    pin_main_pr(monkeypatch)
    git(root, "init", "-q", "-b", "main")
    git(root, "config", "user.name", "Example")
    git(root, "config", "user.email", "test@example.com")
    git(root, "config", "core.hooksPath", os.devnull)
    file = "tests/test_case.py"
    source(root, file, before)
    git(root, "add", ".")
    git(root, "commit", "-qm", "Initial test cases")
    git(root, "update-ref", "refs/remotes/origin/main", "HEAD")
    source(root, file, after)
    source(root, "README.md", "Changed example documentation\n")
    git(root, "add", ".")
    git(root, "commit", "-qm", "Update test inputs")
    return file


def test_budget_scope_uses_changed_definition_ranges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_test_support.duration_budget import budget_targets

    original = (
        "def test_existing(): pass\n\n"
        "def test_changed():\n    return 1\n\n"
        "def test_neighbor(): pass\n"
    )
    file = duration_history(
        tmp_path, monkeypatch, original, original.replace("return 1", "return 2")
    )
    source(
        tmp_path,
        "tests/test_added.py",
        "if True:\n    def test_added(): pass\ndef helper(): pass\ntest_alias = helper\n",
    )
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-qm", "Add a test file")
    collected = [
        file + "::test_existing",
        file + "::test_existing[new]",
        file + "::test_changed",
        file + "::test_neighbor",
        "tests/test_added.py::test_added",
        "tests/test_added.py::test_alias",
    ]
    enforced = budget_targets(tmp_path, collected)
    assert enforced == {
        file + "::test_changed",
        "tests/test_added.py::test_added",
        "tests/test_added.py::test_alias",
    }
    assert budget_failure(file + "::test_existing", 120, enforced) is None
    assert budget_failure(file + "::test_existing[new]", 120, enforced) is None
    assert budget_failure(file + "::test_neighbor", 120, enforced) is None
    assert "exceeds" in (budget_failure(file + "::test_changed", 120, enforced) or "")


@pytest.mark.parametrize(
    "event", ["pull_request", "push", "workflow_dispatch", "schedule", "merge_group", ""]
)
def test_unchanged_slow_case_never_fails_any_event(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, event: str
) -> None:
    from curie_test_support.duration_budget import budget_targets

    file = duration_history(
        tmp_path, monkeypatch, "def test_existing(): pass\n", "def test_existing(): pass\n"
    )
    monkeypatch.setenv("GITHUB_EVENT_NAME", event)
    monkeypatch.setenv("GITHUB_REF_NAME", "next")
    nodeid = file + "::test_existing"
    assert budget_targets(tmp_path, [nodeid]) == set()
    assert budget_failure(nodeid, 120, budget_targets(tmp_path, [nodeid])) is None


@pytest.mark.parametrize("event", ["push", "workflow_dispatch", "schedule", "merge_group"])
def test_non_pr_events_never_enforce_even_with_changed_tests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, event: str
) -> None:
    from curie_test_support.duration_budget import budget_targets

    file = duration_history(
        tmp_path, monkeypatch, "def test_case(): return 1\n", "def test_case(): return 2\n"
    )
    monkeypatch.setenv("GITHUB_EVENT_NAME", event)
    monkeypatch.setenv("GITHUB_REF_NAME", "next")
    assert budget_targets(tmp_path, [file + "::test_case"]) == set()


def test_new_parametrize_decorator_case_is_enforced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_test_support.duration_budget import budget_targets

    before = (
        'import pytest\n@pytest.mark.parametrize("value", ["old"])\ndef test_case(value): pass\n'
    )
    file = duration_history(
        tmp_path, monkeypatch, before, before.replace('["old"]', '["old", "new"]')
    )
    collected = [file + "::test_case[old]", file + "::test_case[new]"]
    assert budget_targets(tmp_path, collected) == set(collected)


def test_changed_function_under_if_is_enforced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_test_support.duration_budget import budget_targets

    before = "if True:\n    def test_case():\n        return 1\n"
    file = duration_history(tmp_path, monkeypatch, before, before.replace("return 1", "return 2"))
    assert budget_targets(tmp_path, [file + "::test_case"]) == {file + "::test_case"}


@pytest.mark.parametrize("removed", ["\n", "# removed comment\n"])
def test_deleting_trivia_does_not_mark_neighbor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, removed: str
) -> None:
    from curie_test_support.duration_budget import budget_targets

    before = "# header\ndef test_neighbor(): pass\n" + removed + "def test_later(): pass\n"
    after = "# header\ndef test_neighbor(): pass\ndef test_later(): pass\n"
    file = duration_history(tmp_path, monkeypatch, before, after)
    assert budget_targets(tmp_path, [file + "::test_neighbor", file + "::test_later"]) == set()


def test_deleted_statement_marks_its_owner_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_test_support.duration_budget import budget_targets

    before = "def test_changed():\n    assert True\n    assert 1\ndef test_neighbor(): pass\n"
    file = duration_history(tmp_path, monkeypatch, before, before.replace("    assert 1\n", ""))
    assert budget_targets(tmp_path, [file + "::test_changed", file + "::test_neighbor"]) == {
        file + "::test_changed"
    }


def test_changed_nested_helper_cannot_select_an_unchanged_global_case(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_test_support.duration_budget import budget_targets

    before = (
        "def test_existing(): pass\n"
        "def helper():\n"
        "    def test_existing():\n"
        "        return 1\n"
        "    return test_existing\n"
    )
    file = duration_history(tmp_path, monkeypatch, before, before.replace("return 1", "return 2"))
    targets = budget_targets(tmp_path, [file + "::test_existing"])
    assert targets == set()
    assert budget_failure(file + "::test_existing", 120, targets) is None


@pytest.mark.parametrize("invalid", [b"broken = (\n", b"# \xff\n"])
@pytest.mark.parametrize("version", ["current", "base", "added"])
def test_unparseable_changed_file_does_not_interrupt_other_budget_targets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, invalid: bytes, version: str
) -> None:
    from curie_test_support.duration_budget import budget_targets

    valid = "def test_case(): pass\n"
    file = duration_history(tmp_path, monkeypatch, valid, valid)
    if version == "added":
        file = "tests/test_added.py"
    (tmp_path / file).write_bytes(valid.encode() + invalid)
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-qm", "Add unparseable source")
    if version == "base":
        git(tmp_path, "update-ref", "refs/remotes/origin/main", "HEAD")
        source(tmp_path, file, valid)
    source(tmp_path, "tests/test_valid.py", "def test_valid(): pass\n")
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-qm", "Add a valid test")
    valid_nodeid = "tests/test_valid.py::test_valid"
    assert budget_targets(tmp_path, [file + "::test_case", valid_nodeid]) == {valid_nodeid}


@pytest.mark.parametrize("edit", ["replace", "delete"])
def test_each_conditional_definition_can_select_the_same_test_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, edit: str
) -> None:
    from curie_test_support.duration_budget import budget_targets

    before = (
        "if True:\n"
        "    def test_case():\n"
        "        assert True\n"
        "        assert 1\n"
        "else:\n"
        "    def test_case(): pass\n"
    )
    replacement = "        assert 2\n" if edit == "replace" else ""
    file = duration_history(
        tmp_path, monkeypatch, before, before.replace("        assert 1\n", replacement)
    )
    nodeid = file + "::test_case"
    assert budget_targets(tmp_path, [nodeid]) == {nodeid}
