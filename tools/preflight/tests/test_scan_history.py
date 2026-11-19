"""Run the pinned scanner against candidate Git history without host mounts."""

from __future__ import annotations

import json
import os
import runpy
import shlex
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
TOOL = ROOT / "tools/preflight/scan_history.py"
LABEL = "io.curie.preflight.scan"
MARKER = "TRANSPORT_CONTROL_VIOLATION"


def test_scanner_detect_arguments_match_the_workflow() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/gitleaks.yaml").read_text())
    step = next(
        step for step in workflow["jobs"]["gitleaks"]["steps"] if step.get("name") == "Run gitleaks"
    )
    command = shlex.split(step["run"].replace("\\\n", ""))
    expected = command[command.index("detect") :]
    expansion = "${GITLEAKS_LOG_OPTS:+$GITLEAKS_LOG_OPTS}"
    expected[expected.index(expansion)] = "--log-opts=base..head"
    arguments = runpy.run_path(str(TOOL))["detect_arguments"]("base", "head")
    config_index = arguments.index("--config")
    assert arguments[config_index + 1] == "/repo/.gitleaks.toml"
    assert arguments[:config_index] + arguments[config_index + 2 :] == expected


def run(root: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        args,
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"},
    )
    if check and result.returncode:
        raise RuntimeError(f"{args[0]} failed: {result.stderr[-1000:]}")
    return result


def git(root: Path, *args: str) -> str:
    return run(root, "git", *args).stdout.strip()


@pytest.fixture(scope="session")
def scanner_image() -> str:
    run(ROOT, "docker", "info", "--format", "{{.ServerVersion}}")
    workflow = yaml.safe_load((ROOT / ".github/workflows/gitleaks.yaml").read_text())
    return str(workflow["jobs"]["gitleaks"]["env"]["GITLEAKS_IMAGE"])


@pytest.mark.parametrize("invalid", ["missing", ".gitleaks.toml", ".gitleaksignore"])
def test_scan_refuses_missing_or_symlinked_configuration(tmp_path: Path, invalid: str) -> None:
    git(tmp_path, "init", "-q", "-b", "task/scan-probe")
    git(tmp_path, "config", "core.hooksPath", os.devnull)
    git(tmp_path, "config", "user.name", "Example")
    git(tmp_path, "config", "user.email", "scanner@example.com")
    (tmp_path / "config.toml").write_text("[extend]\nuseDefault = true\n")
    if invalid != "missing":
        (tmp_path / ".gitleaks.toml").write_text("[extend]\nuseDefault = true\n")
        path = tmp_path / invalid
        path.unlink(missing_ok=True)
        path.symlink_to("config.toml")
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-qm", "Add invalid scanner configuration")
    git(tmp_path, "update-ref", "refs/remotes/origin/main", "HEAD")
    result = run(
        tmp_path,
        sys.executable,
        str(TOOL),
        "--base",
        "main",
        "--image",
        "unused:configuration-rejected-before-docker",
        check=False,
    )
    assert result.returncode == 1
    name = ".gitleaks.toml" if invalid == "missing" else invalid
    assert f"{name} must be a committed regular file" in result.stderr
    assert not (tmp_path / "gitleaks-report.json").exists()


@pytest.mark.parametrize("shallow", [False, True])
@pytest.mark.parametrize("violation", [False, True])
def test_scan_preserves_range_detects_violation_and_removes_container(
    tmp_path: Path,
    scanner_image: str,
    shallow: bool,
    violation: bool,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    git(source, "init", "-q", "-b", "task/scan-probe")
    git(source, "config", "core.hooksPath", os.devnull)
    git(source, "config", "user.name", "Example")
    git(source, "config", "user.email", "scanner@example.com")
    (source / ".gitleaks.toml").write_text(
        '[[rules]]\nid = "transport-control"\ndescription = "Transport control"\n'
        f"regex = '''{MARKER}'''\n"
    )
    (source / "base.txt").write_text(MARKER + "\n")
    git(source, "add", ".")
    git(source, "commit", "-qm", "Add scanner baseline")
    git(source, "update-ref", "refs/remotes/origin/main", "HEAD")
    if shallow:
        candidate = tmp_path / "candidate"
        run(tmp_path, "git", "clone", "-q", "--depth", "1", source.as_uri(), str(candidate))
        git(candidate, "config", "core.hooksPath", os.devnull)
        git(candidate, "config", "user.name", "Example")
        git(candidate, "config", "user.email", "scanner@example.com")
        git(candidate, "update-ref", "refs/remotes/origin/main", "HEAD")
    else:
        candidate = source
    (candidate / "candidate.txt").write_text(MARKER + "\n" if violation else "Clean change\n")
    git(candidate, "add", "candidate.txt")
    git(candidate, "commit", "-qm", "Add candidate input")
    if violation:
        (candidate / "candidate.txt").write_text("Clean uncommitted replacement\n")
        (candidate / ".gitleaks.toml").write_text("[allowlist]\nregexes = ['.*']\n")
    outside = tmp_path / "outside-report.json"
    outside.write_text("Leave this file alone\n")
    (candidate / "gitleaks-report.json").symlink_to(outside)
    owner = uuid.uuid4().hex
    result = run(
        candidate,
        sys.executable,
        str(TOOL),
        "--base",
        "main",
        "--image",
        scanner_image,
        "--owner",
        owner,
        check=False,
    )
    try:
        assert result.returncode == (1 if violation else 0), result.stdout + result.stderr
        assert outside.read_text() == "Leave this file alone\n"
        assert not (candidate / "gitleaks-report.json").is_symlink()
        assert (candidate / "gitleaks-report.json").stat().st_mode & 0o777 == 0o600
        report = json.loads((candidate / "gitleaks-report.json").read_text())
        if violation:
            assert {finding["RuleID"] for finding in report} == {"transport-control"}
            assert {finding["File"] for finding in report} == {"candidate.txt"}
            assert {finding["Commit"] for finding in report} == {
                git(candidate, "rev-parse", "HEAD")
            }
        else:
            assert report == []
        assert (
            run(candidate, "docker", "ps", "-aq", "--filter", f"label={LABEL}={owner}").stdout == ""
        )
    finally:
        remaining = run(
            candidate, "docker", "ps", "-aq", "--filter", f"label={LABEL}={owner}"
        ).stdout.split()
        if remaining:
            run(candidate, "docker", "rm", "-f", "-v", *remaining)
