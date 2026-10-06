"""Keep Dependency Audit's PR trigger aligned with its audited inputs (#4045)."""

from __future__ import annotations

import copy
import posixpath
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
WORKFLOW_PATH = REPO_ROOT / ".github/workflows/dependency-audit.yaml"
EXPECTED_PATHS = [
    "cli/Cargo.lock",
    "cli/Cargo.toml",
    "packages/aci-protocol/generated/rust/Cargo.lock",
    "packages/aci-protocol/generated/rust/Cargo.toml",
    "uv.lock",
    "**/pyproject.toml",
    "apps/ui/pnpm-lock.yaml",
    "apps/ui/package.json",
    ".github/workflows/dependency-audit.yaml",
]


def _workflow() -> dict[str, Any]:
    # BaseLoader preserves GitHub Actions' `on` key instead of treating it as
    # a YAML 1.1 boolean. All values relevant to these tests are strings.
    return yaml.load(WORKFLOW_PATH.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def _audited_lockfiles(workflow: dict[str, Any]) -> set[str]:
    lockfiles: set[str] = set()
    workflow_directory = workflow.get("defaults", {}).get("run", {}).get("working-directory", ".")
    for job in workflow["jobs"].values():
        job_directory = (
            job.get("defaults", {}).get("run", {}).get("working-directory", workflow_directory)
        )
        for step in job["steps"]:
            run = step.get("run", "").replace("\\\n", " ")
            directory = step.get("working-directory", job_directory)
            if re.search(r"(?m)^\s*cargo\s+audit(?:\s|$)", run):
                lockfiles.add(posixpath.normpath(posixpath.join(directory, "Cargo.lock")))
            if re.search(r"(?m)^\s*uv\s+export\b[^\n]*\s--frozen(?:\s|$)", run):
                lockfiles.add(posixpath.normpath(posixpath.join(directory, "uv.lock")))
            if re.search(r"(?m)^\s*pnpm\s+install\b[^\n]*\s--frozen-lockfile(?:\s|$)", run):
                lockfiles.add(posixpath.normpath(posixpath.join(directory, "pnpm-lock.yaml")))
    return lockfiles


def _assert_lockfiles_covered(workflow: dict[str, Any]) -> None:
    # Every lockfile in the agreed filter has an exact path entry, so checking
    # these paths also prevents a future audit from silently losing PR coverage.
    missing = _audited_lockfiles(workflow) - set(workflow["on"]["pull_request"]["paths"])
    assert not missing, f"Audited lockfiles missing from pull_request.paths: {sorted(missing)}"


def test_pull_request_filters_exact_dependency_inputs_and_preserves_other_triggers() -> None:
    triggers = _workflow()["on"]
    assert triggers == {
        "push": {"branches": ["main", "next"]},
        "pull_request": {
            "branches": ["main", "next", "task/**"],
            "paths": EXPECTED_PATHS,
        },
        "schedule": [{"cron": "0 6 * * 2"}],
        "workflow_dispatch": "",
    }


def test_every_audited_lockfile_is_covered() -> None:
    workflow = _workflow()
    assert _audited_lockfiles(workflow) == {
        "cli/Cargo.lock",
        "packages/aci-protocol/generated/rust/Cargo.lock",
        "uv.lock",
        "apps/ui/pnpm-lock.yaml",
    }
    _assert_lockfiles_covered(workflow)


@pytest.mark.parametrize(
    ("command", "lockfile"),
    [
        ("cargo audit", "Cargo.lock"),
        ("uv export --frozen --all-packages", "uv.lock"),
        ("pnpm install --frozen-lockfile", "pnpm-lock.yaml"),
    ],
)
@pytest.mark.parametrize("directory_source", ["step", "job"])
def test_new_audited_lockfile_requires_filter_update(
    command: str, lockfile: str, directory_source: str
) -> None:
    workflow = copy.deepcopy(_workflow())
    directory = "tools/additional-audit"
    step: dict[str, Any] = {"run": command}
    job: dict[str, Any] = {"steps": [step]}
    if directory_source == "step":
        step["working-directory"] = directory
    else:
        job["defaults"] = {"run": {"working-directory": directory}}
    workflow["jobs"]["additional-audit"] = job

    with pytest.raises(AssertionError, match=re.escape(f"{directory}/{lockfile}")):
        _assert_lockfiles_covered(workflow)

    workflow["on"]["pull_request"]["paths"].append(f"{directory}/{lockfile}")
    _assert_lockfiles_covered(workflow)
