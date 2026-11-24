"""Keep both Kubernetes admission read behavior suites in the Python CI plan.

A new or changed inventory read needs regressions at the affected boundary for
transient HTTP failure, deadline expiry, and a slow successful read that permits
independent maintenance. The consumer and admission suites cover those cases
for #5544 and #5630.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
REGISTRY = REPO_ROOT / ".github/e2e-selection.yaml"
SELECTOR = REPO_ROOT / "tools/e2e-ci-selection/select_tiers.py"
POLICY_TEST = Path(__file__).resolve().relative_to(REPO_ROOT).as_posix()
BOUNDARIES = (
    "apps/worker/src/curie_worker/sandbox/k8s.py",
    "apps/worker/src/curie_worker/consumer.py",
    "apps/worker/src/curie_worker/kernel/work_items.py",
)
BEHAVIOR_MODULES = (
    "apps/worker/tests/kernel/test_consumer.py",
    "apps/worker/tests/sandbox/test_admission_k8s.py",
)


def _select(
    tmp_path: Path, path: str, *, registry: Path = REGISTRY, full: bool = False
) -> tuple[dict[str, str], list[str]]:
    output = tmp_path / "github-output"
    targets = tmp_path / "pytest-targets.txt"
    command = [
        sys.executable,
        str(SELECTOR),
        "--registry",
        str(registry),
        "--path",
        path,
        "--pytest-targets-out",
        str(targets),
        "--omit-kind",
    ]
    if full:
        command.append("--full-pytest")
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        env={**os.environ, "GITHUB_OUTPUT": str(output)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    selection = dict(line.split("=", 1) for line in output.read_text().splitlines())
    return selection, targets.read_text().splitlines()


def _require_policy_test(selection: dict[str, str], path: str) -> None:
    assert POLICY_TEST in selection["ci_policy_files"].split(","), f"Missing policy trigger: {path}"
    assert selection["ci_policy"] == "true"


def _under_any(path: str, prefixes: list[str]) -> bool:
    return any(path == prefix or path.startswith(f"{prefix}/") for prefix in prefixes)


def _require_behavior_modules(selection: dict[str, str], targets: list[str]) -> None:
    assert selection["pytest"] == "true", "Python pytest is disabled"
    scope = selection["pytest_scope"]
    assert scope in {"impact", "full"}, f"Unexpected Python pytest scope: {scope}"
    excluded = yaml.safe_load(REGISTRY.read_text())["pytest_impact"]["excluded"]
    deselected = selection["pytest_deselect"].split(",")
    if scope == "full":
        config = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
        collection_roots = config["tool"]["pytest"]["ini_options"]["testpaths"]

    for module in BEHAVIOR_MODULES:
        assert not _under_any(module, excluded + deselected), f"Excluded behavior module: {module}"
        if scope == "impact":
            assert module in targets, f"Missing behavior module: {module}"
        else:
            assert _under_any(module, collection_roots) and (REPO_ROOT / module).is_file(), (
                f"Full plan will not collect behavior module: {module}"
            )


@pytest.mark.parametrize("path", BOUNDARIES)
def test_admission_boundary_selects_policy_and_behavior(tmp_path: Path, path: str) -> None:
    selection, targets = _select(tmp_path, path)
    _require_policy_test(selection, path)
    _require_behavior_modules(selection, targets)


def test_full_plan_keeps_both_behavior_modules_eligible(tmp_path: Path) -> None:
    selection, targets = _select(tmp_path, BOUNDARIES[0], full=True)
    assert selection["pytest_scope"] == "full"
    _require_behavior_modules(selection, targets)


@pytest.mark.parametrize("missing_module", BEHAVIOR_MODULES)
def test_impact_plan_missing_a_behavior_module_is_rejected(missing_module: str) -> None:
    selection = {"pytest": "true", "pytest_scope": "impact", "pytest_deselect": ""}
    targets = [module for module in BEHAVIOR_MODULES if module != missing_module]
    message = f"Missing behavior module: {re.escape(missing_module)}"
    with pytest.raises(AssertionError, match=message):
        _require_behavior_modules(selection, targets)


@pytest.mark.parametrize("missing_module", BEHAVIOR_MODULES)
def test_full_plan_deselecting_a_behavior_module_is_rejected(missing_module: str) -> None:
    selection = {"pytest": "true", "pytest_scope": "full", "pytest_deselect": missing_module}
    message = f"Excluded behavior module: {re.escape(missing_module)}"
    with pytest.raises(AssertionError, match=message):
        _require_behavior_modules(selection, [])


@pytest.mark.parametrize("missing_path", BOUNDARIES)
def test_registry_missing_a_boundary_trigger_is_rejected(tmp_path: Path, missing_path: str) -> None:
    data = yaml.safe_load(REGISTRY.read_text())
    data["ci_policy_tests"][POLICY_TEST].remove(missing_path)
    registry = tmp_path / "selection.yaml"
    registry.write_text(yaml.safe_dump(data))

    selection, _ = _select(tmp_path, missing_path, registry=registry)
    with pytest.raises(AssertionError, match=f"Missing policy trigger: {re.escape(missing_path)}"):
        _require_policy_test(selection, missing_path)


def test_unrelated_documentation_does_not_select_admission_policy(tmp_path: Path) -> None:
    selection, _ = _select(tmp_path, "docs/unrelated-policy-fixture.md")
    assert POLICY_TEST not in selection["ci_policy_files"].split(",")
