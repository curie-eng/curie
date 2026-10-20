"""Executable contract for deterministic end to end CI selection."""

from __future__ import annotations

import ast
import copy
import functools
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import tomllib
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any, Literal, TypedDict

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
# NOT `select.py`. Python puts a script's own directory first on sys.path, so a
# module named `select` shadows the stdlib one -- and this script imports
# subprocess, which imports selectors, which imports select. That made every
# test here fail on macOS while CI stayed green (#1878).
SELECTOR = REPO_ROOT / "tools" / "e2e-ci-selection" / "select_tiers.py"
REGISTRY = REPO_ROOT / ".github" / "e2e-selection.yaml"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yaml"
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
INITIAL_BINDING_MODULE = "tools/factory-e2e/tests/test_c4_initial_binding.py"
CLUSTER_UP_MODULE = "tools/factory-e2e/tests/test_c4_cluster_up.py"
INITIAL_BINDING_INPUTS = (
    "cli/src/factory_quickstart/github.rs",
    "cli/src/factory_quickstart/gitlab/plan.rs",
    "cli/src/factory_quickstart/jira_bitbucket.rs",
    "cli/src/factory_intake.rs",
    "cli/src/api.rs",
    "cli/src/api_requests.rs",
    "apps/api/src/curie_api/routers/agents.py",
    "apps/api/src/curie_api/schemas/agents.py",
    "apps/api/src/curie_api/schemas/channels.py",
    "apps/api/src/curie_api/crud/channels.py",
    "apps/api/src/curie_api/forges/surfaces.py",
    INITIAL_BINDING_MODULE,
)
STATIC_TOKEN_ROTATION_MODULE = "tools/factory-e2e/tests/test_c4_static_token_rotation.py"
FORGE_INSTANCE_JOB = "forge-instance-regression"
ADMITTED_AGENT_JOB = "factory-admitted-agent"
FACTORY_SIBLING_RESULTS = {
    "admitted_agent_result": "ADMITTED_AGENT_RESULT",
}
NIGHTLY_DISPATCH = (
    "github.event_name == 'workflow_dispatch' && inputs.nightly == true"
)
FORGE_INSTANCE_MODULE = "tools/factory-e2e/tests/test_c4_forge_instances.py"
FORGE_INSTANCE_LEGS = {
    "second-gitlab": "test_second_gitlab_registration_preserves_first",
    "jira-sites": "test_jira_sites_coexist_and_conflicts_preserve_all_routes",
    "stale-observation": "test_registration_stale_observation_refuses",
    "interrupted-resources": (
        "test_interrupted_owned_resources_are_reused_and_foreign_resources_refuse"
    ),
}
SRE_RUNTIME = "examples/tests/test_sre_bot_observability_runtime.py"
FACTORY_UPGRADE_RUNTIME = "tools/factory-e2e/tests/test_c4_upgrade_migration.py"
SRE_RUNTIME_INPUTS = (
    "examples/sre-bot",
    SRE_RUNTIME,
    "runner",
    "packages/aci-protocol",
    "packages/plugin-format",
    "packages/telemetry",
    "packages/telemetry-schema",
    "otel/collector-config.yaml",
    "charts/curie/values.yaml",
    "uv.lock",
    "pyproject.toml",
    "conftest.py",
    "cli/scripts/configure-docker-hub-mirror.sh",
    ".github/workflows/ci.yaml",
    ".github/e2e-selection.yaml",
    "tools/e2e-ci-selection",
)
C4_TRACKER_SCOPE = "tools/factory-e2e/tests/test_c4_tracker_scope.py"
C4_TRACKER_SCOPE_INPUTS = (
    "charts/curie",
    "cli/src",
    "cli/Cargo.toml",
    "cli/Cargo.lock",
    "apps/api/src/curie_api/forges",
    "apps/api/Dockerfile",
    "tests/vectors/forge-tracker-scopes.json",
    C4_TRACKER_SCOPE,
    "tools/factory-e2e/tests/conftest.py",
    "uv.lock",
    "pyproject.toml",
    "conftest.py",
    ".github/workflows/ci.yaml",
    ".github/e2e-selection.yaml",
    "tools/e2e-ci-selection",
)
C4_DRY_RUN = "tools/factory-e2e/tests/test_c4_dry_run.py"
C4_DRY_RUN_INPUTS = (
    "charts/curie",
    "cli/src",
    "cli/Cargo.toml",
    "cli/Cargo.lock",
    "apps/api/Dockerfile",
    C4_DRY_RUN,
    "tools/factory-e2e/tests/conftest.py",
    "uv.lock",
    "pyproject.toml",
    "conftest.py",
    ".github/workflows/ci.yaml",
    ".github/e2e-selection.yaml",
    "tools/e2e-ci-selection",
)
# Every conditional_pytest_files entry of the registry, with its inputs.
CONDITIONAL_PYTEST_FILES = {
    SRE_RUNTIME: SRE_RUNTIME_INPUTS,
    C4_TRACKER_SCOPE: C4_TRACKER_SCOPE_INPUTS,
    C4_DRY_RUN: C4_DRY_RUN_INPUTS,
}
# What a fully classified diff touching no conditional input deselects.
ALL_CONDITIONAL = ",".join(sorted(CONDITIONAL_PYTEST_FILES))
UPGRADE_MUTANT_BUILDER = REPO_ROOT / "charts" / "curie" / "ci" / "make-upgrade-mutants.py"
UPGRADE_MATRIX = REPO_ROOT / "cli" / "scripts" / "cluster-upgrade-matrix.sh"
# Directories the upgrade jobs and their helpers read whole, with excluded
# subdirectories. The chart package leaves out its helmignored CI scripts.
UPGRADE_MATRIX_DIRECTORY_INPUTS = {
    "charts/curie": ("charts/curie/ci",),
    "charts/curie/crds": (),
    "examples/dark-factory": (),
    "tools/factory-e2e/fixtures/unitconv": (),
}

TIERS = ("skill", "local", "local-release", "cluster", "released-upgrade")
BASE_TIERS = TIERS[:-1]
OUTPUT_KEYS = {
    "skill": "skill",
    "local": "local",
    "local-release": "local_release",
    "cluster": "cluster",
    "released-upgrade": "released_upgrade",
}
APPROVED_ROOT_DOCS = (
    "AGENTS.md",
    "ARCHITECTURE.md",
    "CLAUDE.md",
    "CODE_OF_CONDUCT.md",
    "CONTRIBUTING.md",
    "LICENSE",
    "NOTICE",
    "QUICKSTART.md",
    "README.md",
    "SECURITY.md",
    "SUPPORT.md",
    "TRADEMARKS.md",
    "llms.txt",
)
CHART_JOB = "e2e-cluster-chart-regressions"
CHART_JOB_IF = "${{ needs.changes.outputs.chart_scenarios != '[]' }}"
CHART_SHARED_INPUTS = (
    "charts/curie/templates",
    "charts/curie/values",
    "charts/curie/Chart.yaml",
    "charts/curie/crds",
    "charts/curie/files",
    "charts/curie/.helmignore",
    ".github/workflows/ci.yaml",
    ".github/e2e-selection.yaml",
    ".github/actions/load-ci-images",
    "tools/runtime-assertion-gate/run.sh",
    "cli/scripts/configure-docker-hub-mirror.sh",
    "cli/scripts/ensure-kind-registry.sh",
)
CHART_IMAGE_INPUTS = (
    "apps/api",
    "apps/worker",
    "runner",
    "cli",
    "packages",
    "pyproject.toml",
    "uv.lock",
)


class _ChartScenario(TypedDict):
    step: str
    inputs: list[str]
    leg: str


CHART_SCENARIOS: dict[str, _ChartScenario] = {
    "failed-first-install-retry": {
        "step": "Failed first install retry recreates data volumes",
        "inputs": [
            "charts/curie/ci/runtime/failed-first-install-retry-runtime.sh",
            "cli/src/ops/up.rs",
        ],
        "leg": "workload",
    },
    "installation-identity": {
        "step": "Installation identity Secret lifecycle",
        "inputs": ["charts/curie/ci/runtime/upgrade-drain-installation-runtime.sh"],
        "leg": "chart",
    },
    "connector-identity": {
        "step": "End to end connector identity is denied outside its scope",
        "inputs": ["charts/curie/ci/runtime/e2e-connector-identity-runtime.sh"],
        "leg": "chart",
    },
    "runner-resources-render": {
        "step": "Per-agent runner resource override renders",
        "inputs": ["charts/curie/ci/runtime/runner-resources-assertions.sh"],
        "leg": "chart",
    },
    "langfuse-postgres-readiness": {
        "step": "Langfuse web waits for delayed Postgres without restarting",
        "inputs": [
            "charts/curie/ci/runtime/langfuse-postgres-readiness-runtime.sh",
            "charts/curie/ci/postgres-readiness-delay.Dockerfile",
        ],
        "leg": "chart",
    },
    "connector-readiness": {
        "step": "Connector that never listens stays not-Ready until it binds",
        "inputs": [
            "charts/curie/ci/runtime/connector-readiness-runtime.sh",
            "packages/plugin-format",
            "apps/worker/src/curie_worker/connector_k8s.py",
            "pyproject.toml",
            "uv.lock",
        ],
        "leg": "chart",
    },
    "runner-template-admission": {
        "step": "Runner template admission refuses host escapes, token mounts, and unlisted images",
        "inputs": ["charts/curie/ci/runtime/runner-template-admission-runtime.sh"],
        "leg": "chart",
    },
    "runner-byo-egress": {
        "step": "Runner BYO egress enforces (not just rendered)",
        "inputs": ["scripts/check-runner-byo-egress.sh"],
        "leg": "chart",
    },
    "controller-preflight": {
        "step": "Controller preflight accepts a serving upgrade and refuses broken RBAC",
        "inputs": [
            "charts/curie/ci/scenarios/controller-preflight-kind.sh",
            "pyproject.toml",
            "uv.lock",
        ],
        "leg": "chart",
    },
    "workspace-diagnostic": {
        "step": "Workspace dependency failure reaches the terminal diagnostic",
        "inputs": [
            "charts/curie/ci/runtime/workspace-dependency-diagnostic-runtime.sh",
            *CHART_IMAGE_INPUTS,
        ],
        "leg": "workload",
    },
    "quota-progress": {
        "step": "Quota progress regression",
        "inputs": [
            "charts/curie/ci/runtime/idle-capacity-reclamation-runtime.sh",
            *CHART_IMAGE_INPUTS,
        ],
        "leg": "workload",
    },
}
CHART_IDS = tuple(sorted(CHART_SCENARIOS))


CHART_LEGS = ("chart", "workload")
WORKLOAD_SCENARIOS = frozenset(
    {"workspace-diagnostic", "quota-progress", "failed-first-install-retry"}
)


def _chart_leg_clause(leg: str) -> str:
    return "matrix.leg == '" + leg + "'"


def _chart_step_if(scenario: str, leg: str | None = None) -> str:
    return (
        "${{ "
        + _chart_leg_clause(CHART_SCENARIOS[scenario]["leg"] if leg is None else leg)
        + " && contains(fromJSON(needs.changes.outputs.chart_scenarios), '"
        + scenario
        + "') }}"
    )


def _expected_chart_legs(scenarios: tuple[str, ...] | list[str]) -> list[str]:
    # An unregistered step carries no leg condition, so it selects every leg.
    if "unregistered" in scenarios:
        return list(CHART_LEGS)
    return sorted({CHART_SCENARIOS[scenario]["leg"] for scenario in scenarios})


def _matches_chart_input(path: str, value: str) -> bool:
    return (
        path == value
        or path.startswith(value + "/")
        or (not value.endswith((".py", ".sh")) and path.startswith(value))
    )


def _expected_chart_for_path(path: str) -> tuple[str, ...]:
    if any(_matches_chart_input(path, value) for value in CHART_SHARED_INPUTS):
        return CHART_IDS
    matched = tuple(
        scenario
        for scenario in CHART_IDS
        if any(_matches_chart_input(path, value) for value in CHART_SCENARIOS[scenario]["inputs"])
    )
    if matched:
        return matched
    # An input outside the tier registry takes fallback and proves every scenario.
    rules = yaml.safe_load(REGISTRY.read_text())["rules"]
    if path in rules["exact"] or any(
        path == prefix or path.startswith(prefix + "/") for prefix in rules["prefixes"]
    ):
        return ()
    return CHART_IDS


def _invoke_selector(
    tmp_path: Path,
    *paths: str,
    registry: Path = REGISTRY,
    push: bool = False,
    omit_kind: bool = False,
    base: str | None = None,
    head: str | None = None,
    cwd: Path | None = None,
) -> tuple[subprocess.CompletedProcess[str], str]:
    output_path = tmp_path / f"github-output-{len(list(tmp_path.glob('github-output-*')))}"
    command = [sys.executable, str(SELECTOR), "--registry", str(registry)]
    if push:
        command.append("--push")
    if omit_kind:
        command.append("--omit-kind")
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


def _load_selector_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("select_tiers", SELECTOR)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Registered first: the module's dataclasses resolve their own module.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


# Checks that sweep many paths call the selector's own per-path function in
# process; one interpreter per path cost seconds per test. The CLI boundary
# (GITHUB_OUTPUT, --push, --omit-kind, revisions) keeps its subprocess tests
# through `_invoke_selector`.
_SELECTOR_MODULE = _load_selector_module()


@functools.cache
def _loaded_registry(registry: Path) -> Any:
    return _SELECTOR_MODULE._load_registry(registry)


def _selects_released_upgrade(path: str, registry: Path = REGISTRY) -> bool:
    selected = _SELECTOR_MODULE._select_path(_loaded_registry(registry), path)
    return "released-upgrade" in selected


def _path_needs_images(path: str) -> bool:
    name = path.rsplit("/", 1)[-1]
    if path in {"uv.lock", "pyproject.toml"} or name in {"uv.lock", "pyproject.toml"}:
        return True
    return "Dockerfile" in name or name.endswith(".dockerfile")


def _path_needs_cli_release(path: str) -> bool:
    return path == "cli" or path.startswith("cli/")


def _expected_conditional_deselect(*paths: str, pytest_needed: bool = True) -> str:
    if not pytest_needed or not paths:
        return ""
    rules = yaml.safe_load(REGISTRY.read_text())["rules"]
    if any(
        path not in rules["exact"]
        and path not in rules["ignored_exact"]
        and not any(
            path == prefix or path.startswith(prefix + "/")
            for collection in ("prefixes", "ignored_prefixes")
            for prefix in rules[collection]
        )
        for path in paths
    ):
        return ""
    return ",".join(
        sorted(
            test_file
            for test_file, inputs in CONDITIONAL_PYTEST_FILES.items()
            if not any(
                path == value or path.startswith(value + "/") for path in paths for value in inputs
            )
        )
    )


def _expected_ci_policy_files(
    *paths: str, registry: Path = REGISTRY, push: bool = False
) -> tuple[str, ...]:
    entries = yaml.safe_load(registry.read_text())["ci_policy_tests"]
    return tuple(sorted(
        module for module, inputs in entries.items()
        if push or any(
            path == prefix or path.startswith(prefix + "/")
            for path in paths for prefix in inputs
        )
    ))


def _assert_ci_policy_registry(root: Path, entries: dict[str, list[str]]) -> None:
    root = root.resolve()
    discovered = {
        path.relative_to(root).as_posix()
        for directory in CI_POLICY_DIRS
        for path in (root / directory / "tests").rglob("test_*.py")
        if path.is_file()
    }
    assert set(entries) == discovered, (
        f"missing modules: {sorted(discovered - set(entries))}; "
        f"stale modules: {sorted(set(entries) - discovered)}"
    )
    for module in sorted(discovered):
        inputs = entries[module]
        owner = next(
            directory for directory in CI_POLICY_DIRS if module.startswith(directory + "/")
        )
        assert ".github" in inputs, f"{module}: missing .github input"
        assert owner in inputs, f"{module}: missing {owner} input"
        assert not {"", ".", "./"}.intersection(inputs), module
        try:
            tree = ast.parse((root / module).read_text(), filename=module)
        except (OSError, UnicodeError, SyntaxError) as error:
            raise AssertionError(f"{module}: cannot parse required source: {error}") from error
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            literal = node.value
            if "/" not in literal or Path(literal).is_absolute():
                continue
            try:
                target = (root / literal).resolve()
                relative = target.relative_to(root).as_posix()
                exists = target.exists()
            except (OSError, ValueError):
                continue
            if relative == "." or not exists:
                continue
            assert any(
                relative == prefix or relative.startswith(prefix + "/") for prefix in inputs
            ), f"{module}: undeclared repository path {relative}"


def test_ci_policy_registry_covers_modules_and_existing_path_literals() -> None:
    _assert_ci_policy_registry(REPO_ROOT, yaml.safe_load(REGISTRY.read_text())["ci_policy_tests"])


def _policy_guard_fixture(tmp_path: Path) -> dict[str, list[str]]:
    entries: dict[str, list[str]] = yaml.safe_load(REGISTRY.read_text())["ci_policy_tests"]
    for directory in CI_POLICY_DIRS:
        for source in (REPO_ROOT / directory / "tests").rglob("test_*.py"):
            destination = tmp_path / source.relative_to(REPO_ROOT)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
    return entries


def test_ci_policy_guard_covers_existing_ignored_build_directories(tmp_path: Path) -> None:
    entries = _policy_guard_fixture(tmp_path)
    for directory in ("target", "dist"):
        (tmp_path / "cli" / directory).mkdir(parents=True, exist_ok=True)
    _assert_ci_policy_registry(tmp_path, entries)

    module = "tools/ci-workflow-paths/tests/test_workflow_paths.py"
    entries[module].remove("cli")
    with pytest.raises(
        AssertionError, match=re.escape(module) + r": undeclared repository path cli/(target|dist)"
    ):
        _assert_ci_policy_registry(tmp_path, entries)
    entries[module].append("cli")
    _assert_ci_policy_registry(tmp_path, entries)


def test_ci_policy_guard_rejects_missing_entry(tmp_path: Path) -> None:
    entries = _policy_guard_fixture(tmp_path)
    _assert_ci_policy_registry(tmp_path, entries)
    module = "tools/ci-release-target-parity/tests/test_release_target_parity.py"
    del entries[module]
    with pytest.raises(AssertionError, match=re.escape(module)):
        _assert_ci_policy_registry(tmp_path, entries)


def test_ci_policy_guard_rejects_undeclared_literal_and_accepts_covered_literal(
    tmp_path: Path,
) -> None:
    entries = _policy_guard_fixture(tmp_path)
    module = "tools/ci-release-target-parity/tests/test_release_target_parity.py"
    literal = "/".join(("inputs", "undeclared.txt"))
    target = tmp_path / literal
    target.parent.mkdir()
    target.write_text("input\n")
    source = tmp_path / module
    source.write_text(source.read_text() + f"\nNEW_INPUT = {literal!r}\n")
    diagnostic = module + ": undeclared repository path " + literal
    with pytest.raises(AssertionError, match=re.escape(diagnostic)):
        _assert_ci_policy_registry(tmp_path, entries)
    entries[module].append("inputs")
    _assert_ci_policy_registry(tmp_path, entries)


def test_ci_policy_guard_accepts_registered_new_nested_module(tmp_path: Path) -> None:
    entries = _policy_guard_fixture(tmp_path)
    module = "tools/ci-release-target-parity/tests/nested/test_added_policy.py"
    source = tmp_path / module
    source.parent.mkdir()
    source.write_text("INPUT = './tools/ci-release-target-parity/tests/../tests'\n")
    with pytest.raises(AssertionError, match=re.escape(module)):
        _assert_ci_policy_registry(tmp_path, entries)
    entries[module] = [".github", "tools/ci-release-target-parity"]
    _assert_ci_policy_registry(tmp_path, entries)


@pytest.mark.parametrize(
    "mutation", ["missing", "unreadable", "syntax", "stale", "github", "owner"]
)
def test_ci_policy_guard_fails_closed_on_required_source_and_inputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation: str
) -> None:
    entries = _policy_guard_fixture(tmp_path)
    module = "tools/ci-release-target-parity/tests/test_release_target_parity.py"
    source = tmp_path / module
    if mutation == "missing":
        source.unlink()
    elif mutation == "unreadable":
        read_text = Path.read_text

        def read(path: Path, *args: Any, **kwargs: Any) -> str:
            if path == source:
                raise PermissionError("required source is unreadable")
            return read_text(path, *args, **kwargs)

        monkeypatch.setattr(Path, "read_text", read)
    elif mutation == "syntax":
        source.write_text("def broken(\n")
    elif mutation == "stale":
        entries[module + ".missing"] = entries[module]
    else:
        prefix = ".github" if mutation == "github" else "tools/ci-release-target-parity"
        entries[module].remove(prefix)
    with pytest.raises(AssertionError, match=re.escape(module)):
        _assert_ci_policy_registry(tmp_path, entries)


def _assert_policy_output(output: str, expected: tuple[str, ...]) -> None:
    values = _outputs_of(output)
    assert values["ci_policy"] == str(bool(expected)).lower()
    assert values["ci_policy_files"] == ",".join(expected)
    assert output.count("ci_policy=") == output.count("ci_policy_files=") == 1


def test_ci_policy_unrelated_path_is_checked_against_registry(tmp_path: Path) -> None:
    path = "README.md"
    assert (REPO_ROOT / path).is_file()
    assert _expected_ci_policy_files(path) == ()
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    _assert_policy_output(output, ())


@pytest.mark.parametrize("directory", CI_POLICY_DIRS)
def test_ci_policy_tool_change_selects_its_modules_and_guard(
    tmp_path: Path, directory: str
) -> None:
    paths = [
        path.relative_to(REPO_ROOT).as_posix()
        for path in (REPO_ROOT / directory / "tests").rglob("test_*.py")
    ]
    assert paths
    expected = _expected_ci_policy_files(paths[0])
    assert set(paths) <= set(expected)
    assert Path(__file__).relative_to(REPO_ROOT).as_posix() in expected
    completed, output = _invoke_selector(tmp_path, paths[0])
    assert completed.returncode == 0, completed.stderr
    _assert_policy_output(output, expected)


@pytest.mark.parametrize("push,omit_kind", [(False, False), (True, False), (True, True)])
def test_ci_policy_workflow_and_push_select_all_modules(
    tmp_path: Path, push: bool, omit_kind: bool
) -> None:
    path = ".github/workflows/ci.yaml"
    entries = yaml.safe_load(REGISTRY.read_text())["ci_policy_tests"]
    assert all(".github" in inputs for inputs in entries.values())
    completed, output = _invoke_selector(
        tmp_path, *(() if push else (path,)), push=push, omit_kind=omit_kind
    )
    assert completed.returncode == 0, completed.stderr
    _assert_policy_output(output, tuple(sorted(entries)))


def test_ci_policy_selective_inputs_union_and_order_are_deterministic(tmp_path: Path) -> None:
    path = "scripts/check-pr-body.sh"
    expected = _expected_ci_policy_files(path)
    assert 0 < len(expected) < len(_expected_ci_policy_files(push=True))
    for paths in ((path,), (path, "README.md", path), ("README.md", path)):
        completed, output = _invoke_selector(tmp_path, *paths)
        assert completed.returncode == 0, completed.stderr
        _assert_policy_output(output, expected)
    second = ".gitleaks.toml"
    union = tuple(sorted(set(expected) | set(_expected_ci_policy_files(second))))
    assert set(expected) < set(union)
    for paths in ((path, second, path), (second, path)):
        completed, output = _invoke_selector(tmp_path, *paths)
        assert completed.returncode == 0, completed.stderr
        _assert_policy_output(output, union)


@pytest.mark.parametrize(
    "path", ["tools/close-on-next-other/tests/test_new.py", "scripts/check-pr-body.sh.backup"]
)
def test_ci_policy_prefix_near_misses_and_unknown_paths_have_no_fallback(
    tmp_path: Path, path: str
) -> None:
    expected = _expected_ci_policy_files(path)
    target = (
        "tools/close-on-next/tests/test_close_on_next.py" if path.startswith("tools/")
        else "tools/ci-retry-gate/tests/test_pr_body.py"
    )
    assert target not in expected
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    _assert_policy_output(output, expected)


def test_ci_policy_revision_selection_matches_paths_and_empty_diff(tmp_path: Path) -> None:
    repository = _new_repository(tmp_path)
    base = _commit_files(repository, {"README.md": "base\n"}, "base")
    completed, output = _invoke_selector(tmp_path, base=base, head=base, cwd=repository)
    assert completed.returncode == 0, completed.stderr
    _assert_policy_output(output, ())
    assert _outputs_of(output)["pytest"] == "true"
    path = "scripts/check-pr-body.sh"
    head = _commit_files(repository, {path: "changed\n"}, "policy input")
    completed, output = _invoke_selector(tmp_path, base=base, head=head, cwd=repository)
    assert completed.returncode == 0, completed.stderr
    _assert_policy_output(output, _expected_ci_policy_files(path))
    path_mode, path_output = _invoke_selector(tmp_path, path)
    assert path_mode.returncode == 0, path_mode.stderr
    assert output == path_output


@pytest.mark.parametrize("mutation", [
    "absent", "not-mapping", "empty", "nonstring-key", "absolute", "traversal", "trailing-slash",
    "not-test", "comma", "newline", "space", "empty-inputs", "nonlist-inputs", "duplicate-inputs",
    "absolute-input", "traversal-input", "duplicate-key",
])
def test_ci_policy_registry_rejects_malformed_entries(tmp_path: Path, mutation: str) -> None:
    data = yaml.safe_load(VALID_REGISTRY)
    module = "tools/e2e-ci-selection/tests/test_e2e_ci_selection.py"
    entries = data["ci_policy_tests"]
    if mutation == "absent":
        del data["ci_policy_tests"]
    elif mutation in {"not-mapping", "empty"}:
        data["ci_policy_tests"] = [] if mutation == "not-mapping" else {}
    elif mutation in {
        "empty-inputs", "nonlist-inputs", "duplicate-inputs", "absolute-input", "traversal-input"
    }:
        entries[module] = {
            "empty-inputs": [], "nonlist-inputs": ".github",
            "duplicate-inputs": [".github", ".github"],
            "absolute-input": ["/tmp/input"], "traversal-input": ["../input"],
        }[mutation]
    elif mutation != "duplicate-key":
        key = {
            "nonstring-key": 1, "absolute": "/" + module, "traversal": "../" + module,
            "trailing-slash": module + "/", "not-test": "tools/e2e-ci-selection/readme.py",
            "comma": module + ",other.py", "newline": module + "\nother.py",
            "space": module + " other.py",
        }[mutation]
        data["ci_policy_tests"] = {key: entries[module]}
    contents = yaml.safe_dump(data)
    if mutation == "duplicate-key":
        contents = VALID_REGISTRY.replace(
            "ci_policy_tests:\n", "ci_policy_tests:\n  " + module + ": [.github]\n"
        )
    registry = tmp_path / "registry.yaml"
    registry.write_text(contents)
    completed, output = _invoke_selector(tmp_path, "README.md", registry=registry)
    assert completed.returncode != 0, completed.stdout + completed.stderr
    assert output == ""


def test_ci_policy_valid_synthetic_registry_remains_live(tmp_path: Path) -> None:
    registry = tmp_path / "registry.yaml"
    registry.write_text(VALID_REGISTRY)
    completed, output = _invoke_selector(tmp_path, ".github/workflows/ci.yaml", registry=registry)
    assert completed.returncode == 0, completed.stderr
    _assert_policy_output(
        output, _expected_ci_policy_files(".github/workflows/ci.yaml", registry=registry)
    )


def _expected_output(
    *selected: str,
    ci_policy_files: tuple[str, ...],
    pytest_needed: bool = True,
    pytest_deselect: str = "",
    pytest_scope: str = "full",
    images_needed: bool = False,
    cli_release_needed: bool = False,
    released_upgrade_full: bool = False,
    version_only: bool = False,
    factory: bool = False,
    approval_resume: bool = False,
    chart_scenarios: tuple[str, ...] | None = None,
) -> str:
    selected_tiers = set(selected)
    lines = [
        f"{OUTPUT_KEYS[tier]}={'true' if tier in selected_tiers else 'false'}" for tier in TIERS
    ]
    skill_local = ",".join(tier for tier in TIERS[:2] if tier in selected_tiers)
    lines.append(f"skill_local_tiers={skill_local}")
    lines.append(f"pytest={'true' if pytest_needed else 'false'}")
    lines.append(f"pytest_deselect={pytest_deselect}")
    lines.append(f"pytest_scope={pytest_scope}")
    lines.append(f"ci_policy={str(bool(ci_policy_files)).lower()}")
    lines.append("ci_policy_files=" + ",".join(ci_policy_files))
    lines.append(f"images={'true' if images_needed else 'false'}")
    lines.append(f"cli_release={'true' if cli_release_needed else 'false'}")
    lines.append(f"released_upgrade_full={'true' if released_upgrade_full else 'false'}")
    lines.append(f"version_only={'true' if version_only else 'false'}")
    lines.append(f"factory={'true' if factory else 'false'}")
    lines.append(f"approval_resume={'true' if approval_resume else 'false'}")
    scenarios = CHART_IDS if chart_scenarios is None else chart_scenarios
    rendered = list(scenarios) if "cluster" in selected_tiers else []
    lines.append("chart_scenarios=" + json.dumps(rendered, separators=(",", ":")))
    lines.append("chart_legs=" + json.dumps(_expected_chart_legs(rendered), separators=(",", ":")))
    return "\n".join(lines) + "\n"


def _assert_selection(
    tmp_path: Path,
    path: str,
    selected: tuple[str, ...],
    *,
    pytest_needed: bool = True,
    images_needed: bool | None = None,
    cli_release_needed: bool | None = None,
) -> None:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    assert output == _expected_output(
        *selected,
        pytest_needed=pytest_needed,
        pytest_deselect=_expected_conditional_deselect(path, pytest_needed=pytest_needed),
        pytest_scope=(
            "cli"
            if pytest_needed
            and path.startswith("cli/")
            and not path.startswith("cli/scripts/")
            and (not path.endswith(".py") or path.startswith("cli/tests/"))
            else "full"
        ),
        images_needed=_path_needs_images(path) if images_needed is None else images_needed,
        cli_release_needed=(
            _path_needs_cli_release(path) if cli_release_needed is None else cli_release_needed
        ),
        released_upgrade_full=False,
        chart_scenarios=_expected_chart_for_path(path),
        ci_policy_files=_expected_ci_policy_files(path),
    )


@pytest.mark.parametrize(
    ("path", "selected"),
    [
        ("runner/example.py", BASE_TIERS),
        ("compose.dev.yaml", ("local",)),
        ("compose/generated.py", ("local-release",)),
        ("charts/curie/values.yaml", ("cluster", "released-upgrade")),
        ("apps/api/example.py", ("local", "local-release", "cluster")),
        ("apps/worker/example.py", ("local", "local-release", "cluster")),
        ("otel/collector.yaml", ("local", "local-release")),
        ("cli/example.rs", BASE_TIERS),
        ("cli/src/main.rs", BASE_TIERS),
        ("cli/src/ops/upgrade.rs", TIERS),
        ("cli/scripts/cluster-upgrade-matrix.sh", TIERS),
        ("cli/scripts/gnu-process.py", TIERS),
        ("cli/src/application_schema_windows.json", TIERS),
        (
            "apps/api/src/curie_api/schema_compat.json",
            ("local", "local-release", "cluster", "released-upgrade"),
        ),
        ("packages/example.py", BASE_TIERS),
        ("packages/aci-protocol/src/aci_protocol/wire.py", BASE_TIERS),
        ("packages/plugin-format/src/plugin_format/manifest.py", BASE_TIERS),
        ("pyproject.toml", BASE_TIERS),
        ("uv.lock", BASE_TIERS),
    ],
)
def test_registry_maps_each_known_surface(
    tmp_path: Path,
    path: str,
    selected: tuple[str, ...],
) -> None:
    _assert_selection(tmp_path, path, selected)


@pytest.mark.parametrize(
    "path",
    [
        ".github/e2e-selection.yaml",
        ".github/workflows/ci.yaml",
        "tools/e2e-ci-selection/select_tiers.py",
    ],
)
def test_selector_and_workflow_paths_do_not_boot_kind(tmp_path: Path, path: str) -> None:
    _assert_selection(tmp_path, path, (), pytest_needed=True)


@pytest.mark.parametrize(
    "path",
    [
        "scripts/check-netpol-enforcement.sh",
        "scripts/check-connector-caller-enforcement.sh",
        "scripts/check-mail-adapter-otel-egress.sh",
        "scripts/check-runner-byo-egress.sh",
        "tools/runtime-assertion-gate/run.sh",
    ],
)
def test_cluster_job_scripts_select_cluster(tmp_path: Path, path: str) -> None:
    _assert_selection(tmp_path, path, ("cluster",))


def test_ui_dockerfile_selects_images_without_e2e(tmp_path: Path) -> None:
    _assert_selection(
        tmp_path,
        "apps/ui/Dockerfile",
        (),
        pytest_needed=True,
        images_needed=True,
        cli_release_needed=False,
    )


def test_weather_fixture_does_not_select_released_upgrade(tmp_path: Path) -> None:
    _assert_selection(tmp_path, "examples/weather/evals/cases.json", BASE_TIERS)


@pytest.mark.parametrize(
    "path",
    [
        "charts/curie/values.yaml",
        "charts/curie/templates/secrets.yaml",
        "apps/api/src/curie_api/migrations/versions/example.py",
        "apps/worker/src/curie_worker/config.py",
        "apps/worker/src/curie_worker/approval_cards.py",
        "apps/worker/src/curie_worker/consumer_liveness.py",
        "apps/worker/src/curie_worker/workspace.py",
        "cli/src/ops/upgrade.rs",
        "cli/src/ops/convergence.rs",
        "cli/scripts/cluster-upgrade-matrix.sh",
        "cli/tests/data/upgrade-driver.py",
    ],
)
def test_released_upgrade_selects_upgrade_state_owners(
    tmp_path: Path,
    path: str,
) -> None:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    outputs = dict(line.split("=", maxsplit=1) for line in output.splitlines())
    assert outputs["released_upgrade"] == "true"


@pytest.mark.parametrize(
    "path",
    [
        "apps/api/src/curie_api/routers/agents.py",
        "apps/worker/src/curie_worker/binding.py",
        "apps/worker/src/curie_worker/state/config.py",
        "apps/ui/src/main.tsx",
        "cli/src/main.rs",
        "docs/guides/getting-started.md",
    ],
)
def test_released_upgrade_does_not_select_unrelated_paths(
    tmp_path: Path,
    path: str,
) -> None:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    outputs = dict(line.split("=", maxsplit=1) for line in output.splitlines())
    assert outputs["released_upgrade"] == "false"


def _repo_root_references(script: str) -> list[str]:
    references = re.findall(r"\$\{?REPO_ROOT\}?\"?/([\w.][\w./-]*)", script)
    for match in re.finditer(r"\b(?:bash|sh|python3)[ \t]+(['\"]?)([\w./-]+)\1", script):
        path = match.group(2)
        if "/" in path and not path.startswith("/"):
            references.append(path)
    # Workflow steps run from the checkout root. Match tracked path tokens
    # independently of the command that reads them, including root files and
    # newly added top-level directories. Redirection targets are outputs.
    code_lines = "\n".join(
        line for line in script.splitlines() if not line.lstrip().startswith("#")
    )
    code_lines = re.sub(r"(?<![<>])>{1,2}[ \t]*['\"]?[\w./-]+['\"]?", "", code_lines)
    tracked = _tracked_repo_paths(REPO_ROOT)
    references.extend(
        token
        for token in re.findall(
            r"(?<![\w./-])(?:[\w.-]+/)+[\w.-]+|(?<![\w./-])[\w.-]+\.[\w.-]+",
            code_lines,
        )
        if token in tracked
    )
    references.extend(
        re.findall(
            r"\b(?:open|Path)\([ \t]*['\"]([\w.-]+(?:/[\w.-]+)+)['\"]",
            script,
        )
    )
    return sorted({reference.rstrip("/") for reference in references})


@functools.cache
def _tracked_repo_paths(root: Path) -> frozenset[str]:
    completed = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    paths = set(completed.stdout.split("\0")) - {""}
    return frozenset(
        path
        for file in paths
        for path in (file, *(str(parent) for parent in Path(file).parents[:-1]))
    )


def _python_input_source(source: str) -> str:
    module = ast.parse(source)
    parents = {
        child: parent for parent in ast.walk(module) for child in ast.iter_child_nodes(parent)
    }

    def scope(node: ast.AST) -> ast.AST:
        while node in parents:
            node = parents[node]
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda):
                return node
        return module

    def names(node: ast.AST) -> set[str]:
        return {child.id for child in ast.walk(node) if isinstance(child, ast.Name)}

    consumed: dict[ast.AST, set[str]] = {}
    assignments: list[tuple[ast.AST, set[str], set[str]]] = []
    for node in ast.walk(module):
        if isinstance(node, ast.Call | ast.BinOp):
            consumed.setdefault(scope(node), set()).update(names(node))
        elif isinstance(node, ast.Assign | ast.AnnAssign) and node.value is not None:
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            assignments.append(
                (
                    scope(node),
                    {name for target in targets for name in names(target)},
                    names(node.value),
                )
            )
    changed = True
    while changed:
        changed = False
        for owner, target_names, values in assignments:
            used = consumed.setdefault(owner, set())
            if target_names & used and not values <= used:
                used.update(values)
                changed = True

    non_inputs: list[ast.expr | ast.stmt] = []
    if module.body and isinstance(module.body[0], ast.Expr):
        docstring = module.body[0]
        if isinstance(docstring.value, ast.Constant) and isinstance(docstring.value.value, str):
            non_inputs.append(docstring)

    def literal_strings(node: ast.AST) -> list[ast.expr]:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return [node]
        if isinstance(node, ast.Dict):
            children = [child for child in (*node.keys, *node.values) if child is not None]
        elif isinstance(node, ast.List | ast.Tuple | ast.Set):
            children = list(node.elts)
        else:
            return []
        return [literal for child in children for literal in literal_strings(child)]

    def output_path_strings(node: ast.AST) -> list[ast.expr]:
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
            return output_path_strings(node.left) + output_path_strings(node.right)
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "Path"
        ):
            return [literal for argument in node.args for literal in literal_strings(argument)]
        return literal_strings(node)

    for node in ast.walk(module):
        # Literal route comparisons and HTTP response data do not read files.
        # Calls and path expressions inside either still belong to the scan.
        if isinstance(node, ast.Compare):
            # Keep compared names that reach calls, including assignment aliases.
            compared_names = {
                name
                for operand in (node.left, *node.comparators)
                if isinstance(operand, ast.Name | ast.Attribute | ast.Subscript)
                for name in names(operand)
            }
            if not compared_names & consumed.get(scope(node), set()):
                for operand in (node.left, *node.comparators):
                    non_inputs.extend(literal_strings(operand))
        elif (
            isinstance(node, ast.Return)
            and isinstance(node.value, ast.Tuple)
            and len(node.value.elts) == 2
            and isinstance(node.value.elts[0], ast.Constant)
            and isinstance(node.value.elts[0].value, int)
            and 100 <= node.value.elts[0].value < 600
        ):
            non_inputs.extend(literal_strings(node.value.elts[1]))
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"write_text", "write_bytes", "touch", "mkdir"}
        ):
            non_inputs.extend(output_path_strings(node.func.value))

    encoded = source.encode("utf-8")
    line_offsets = []
    offset = 0
    for line in encoded.splitlines(keepends=True):
        line_offsets.append(offset)
        offset += len(line)
    result = bytearray(encoded)
    for node in non_inputs:
        assert node.end_lineno is not None and node.end_col_offset is not None
        start = line_offsets[node.lineno - 1] + node.col_offset
        end = line_offsets[node.end_lineno - 1] + node.end_col_offset
        result[start:end] = re.sub(rb"[^\n]", b" ", result[start:end])
    return result.decode("utf-8")


def _unselected_helper_inputs(reference: str, seen: set[str]) -> list[str]:
    if reference in seen:
        return []
    seen.add(reference)
    helper = REPO_ROOT / reference
    if not helper.is_file() or helper.suffix not in {".py", ".sh"}:
        return []
    source = helper.read_text()
    if helper.suffix == ".py":
        source = _python_input_source(source)
    problems = _unselected_matrix_inputs(source)
    for child in _repo_root_references(source):
        problems.extend(_unselected_helper_inputs(child, seen))
    return problems


def _git_ignored(path: str) -> bool:
    completed = subprocess.run(
        ["git", "check-ignore", "--quiet", "--", path],
        cwd=REPO_ROOT,
        check=False,
    )
    assert completed.returncode in {0, 1}, f"git check-ignore failed for {path}"
    return completed.returncode == 0


def _directory_files(directory: str, excluded: tuple[str, ...]) -> tuple[str, ...]:
    # Tracked files only: an untracked leftover such as a mergetool .orig is
    # not in any diff, and .helmignore keeps it out of the package.
    completed = subprocess.run(
        ["git", "ls-files", "-z", "--", directory],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return tuple(
        path
        for path in sorted(set(completed.stdout.split("\0")))
        if path and not any(_matches(path, prefix) for prefix in excluded)
    )


def _matches(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(f"{prefix}/")


def _unselected_matrix_inputs(
    script: str,
    registry: Path = REGISTRY,
) -> list[str]:
    """Name each repository input of the script that skips released-upgrade."""
    problems: list[str] = []
    if re.search(r"\bcd[ \t]+['\"]?\$\{?REPO_ROOT\}?['\"]?(?=\s|$)", script):
        problems.append("cd to REPO_ROOT: relative reads cannot be classified")
    for match in re.finditer(
        r"\b(bash|sh|python3)[ \t]+(?:-[A-Za-z]+[ \t]+)*['\"]?"
        r"(\$\{?[A-Za-z_][A-Za-z0-9_]*\}?)(?:['\"])?(?=\s|$)",
        script,
    ):
        problems.append(f"{match.group(1)} {match.group(2)}: helper path cannot be classified")
    for match in re.finditer(
        r"\b(bash|sh|python3)[ \t]+['\"]?([\w.-]+\.(?:py|sh))['\"]?(?=\s|$)",
        script,
    ):
        problems.append(f"{match.group(1)} {match.group(2)}: helper path cannot be classified")
    for reference in _repo_root_references(script):
        if _git_ignored(reference):
            # Build output or local scratch: never part of a diff.
            continue
        target = REPO_ROOT / reference
        paths: tuple[str, ...]
        if target.is_dir():
            if reference not in UPGRADE_MATRIX_DIRECTORY_INPUTS:
                problems.append(f"{reference}: unlisted directory")
                continue
            paths = _directory_files(reference, UPGRADE_MATRIX_DIRECTORY_INPUTS[reference])
            if not paths:
                problems.append(f"{reference}: directory with no files to check")
                continue
        elif target.is_file():
            paths = (reference,)
        else:
            problems.append(f"{reference}: neither in the checkout nor ignored")
            continue
        for path in paths:
            if not _selects_released_upgrade(path, registry):
                problems.append(f"{path}: does not select released-upgrade")
    return problems


def test_released_upgrade_selects_every_repo_file_the_upgrade_matrix_reads(
    tmp_path: Path,
) -> None:
    script = UPGRADE_MATRIX.read_text()
    # The scan still sees the inputs these rules were written for.
    assert {
        "apps/api/src/curie_api/schema_compat.json",
        "charts/curie",
        "cli/scripts/gnu-process.py",
        "cli/src/application_schema_windows.json",
    } <= set(_repo_root_references(script))
    # The chart's left-out path holds only while helm still drops it.
    helmignore = (REPO_ROOT / "charts" / "curie" / ".helmignore").read_text()
    assert UPGRADE_MATRIX_DIRECTORY_INPUTS["charts/curie"] == ("charts/curie/ci",)
    assert "ci/" in helmignore.splitlines()
    assert _unselected_matrix_inputs(script) == []
    # A helper invoked by the matrix can start reading another repository
    # file without changing the matrix script. Guard that second hop too.
    seen: set[str] = set()
    for reference in _repo_root_references(script):
        assert _unselected_helper_inputs(reference, seen) == [], reference


def test_matrix_input_guard_checks_every_packaged_chart_file(tmp_path: Path) -> None:
    # The selector allows an ignored child under a selected prefix, so a hole
    # can open inside charts/curie without touching the prefix itself.
    text = REGISTRY.read_text()
    anchor = "    docs: []\n"
    assert anchor in text
    registry = tmp_path / "registry.yaml"
    registry.write_text(text.replace(anchor, f"{anchor}    charts/curie/files/agent-sandbox: []\n"))
    assert _unselected_matrix_inputs('helm package "$REPO_ROOT/charts/curie"\n', registry) == [
        "charts/curie/files/agent-sandbox/controller.yaml: does not select released-upgrade",
    ]


@pytest.mark.parametrize(
    ("directory", "dropped"),
    [
        ("charts/curie/crds", "charts/curie/crds/crd-sandboxclaims.yaml"),
        ("examples/dark-factory", "examples/dark-factory/connectors.yaml"),
        (
            "tools/factory-e2e/fixtures/unitconv",
            "tools/factory-e2e/fixtures/unitconv/unitconv/convert.py",
        ),
    ],
)
def test_helper_directory_guard_checks_each_input_file(
    tmp_path: Path, directory: str, dropped: str
) -> None:
    registry = yaml.safe_load(REGISTRY.read_text())
    files = _directory_files(directory, ())
    assert dropped in files
    for path in files:
        registry["rules"]["exact"][path] = ["released-upgrade"]
    selected = tmp_path / "selected.yaml"
    selected.write_text(yaml.safe_dump(registry, sort_keys=False))
    script = f'copytree(REPO_ROOT / "{directory}", destination)\n'
    assert _unselected_matrix_inputs(script, selected) == []

    del registry["rules"]["exact"][dropped]
    registry["rules"]["ignored_exact"][dropped] = []
    refused = tmp_path / "refused.yaml"
    refused.write_text(yaml.safe_dump(registry, sort_keys=False))
    assert _unselected_matrix_inputs(script, refused) == [
        f"{dropped}: does not select released-upgrade"
    ]


def test_matrix_input_guard_names_each_read_the_registry_drops(tmp_path: Path) -> None:
    dropped = (
        "apps/api/src/curie_api/schema_compat.json",
        "cli/scripts/gnu-process.py",
        "cli/src/application_schema_windows.json",
    )
    text = REGISTRY.read_text()
    for path in dropped:
        rule = f"    {path}: [released-upgrade]\n"
        assert rule in text
        text = text.replace(rule, "")
    registry = tmp_path / "registry.yaml"
    registry.write_text(text)
    assert _unselected_matrix_inputs(UPGRADE_MATRIX.read_text(), registry) == [
        f"{path}: does not select released-upgrade" for path in dropped
    ]


@pytest.mark.parametrize(
    ("script", "problems"),
    [
        (
            'BIN="$REPO_ROOT/cli/src/main.rs"\n',
            ["cli/src/main.rs: does not select released-upgrade"],
        ),
        (
            'DIR="${REPO_ROOT}/cli/scripts"\n',
            ["cli/scripts: unlisted directory"],
        ),
        (
            'BIN="$REPO_ROOT"/cli/src/main.rs\n',
            ["cli/src/main.rs: does not select released-upgrade"],
        ),
        (
            'CHARTS="$REPO_ROOT/charts/curie/.."\n',
            ["charts/curie/..: unlisted directory"],
        ),
        (
            'HELPER="$REPO_ROOT/cli/scripts/no-such-helper.py"\n',
            ["cli/scripts/no-such-helper.py: neither in the checkout nor ignored"],
        ),
        ('KUBECONFIG_FILE="$REPO_ROOT/.projects/kubeconfig-example"\n', []),
        ('BIN="$REPO_ROOT/cli/target/release/curie"\n', []),
    ],
)
def test_matrix_input_guard_classifies_each_reference(
    tmp_path: Path,
    script: str,
    problems: list[str],
) -> None:
    assert _unselected_matrix_inputs(script) == problems


@pytest.mark.parametrize(
    ("script", "problems"),
    [
        (
            "python3 cli/src/main.rs\n",
            ["cli/src/main.rs: does not select released-upgrade"],
        ),
        (
            "bash cli/scripts/e2e-ladder.sh\n",
            ["cli/scripts/e2e-ladder.sh: does not select released-upgrade"],
        ),
        (
            'cd "$REPO_ROOT"\ncat cli/src/main.rs\n',
            [
                "cd to REPO_ROOT: relative reads cannot be classified",
                "cli/src/main.rs: does not select released-upgrade",
            ],
        ),
        (
            'bash "$UPGRADE_HELPER" --check\n',
            ["bash $UPGRADE_HELPER: helper path cannot be classified"],
        ),
        (
            'python3 "$UPGRADE_HELPER" --check\n',
            ["python3 $UPGRADE_HELPER: helper path cannot be classified"],
        ),
        (
            'from pathlib import Path\nPath("cli/src/main.rs").read_text()\n',
            ["cli/src/main.rs: does not select released-upgrade"],
        ),
        (
            'open("cli/src/main.rs").read()\n',
            ["cli/src/main.rs: does not select released-upgrade"],
        ),
        (
            "python3 upgrade-helper.py --check\n",
            ["python3 upgrade-helper.py: helper path cannot be classified"],
        ),
        (
            "cat cli/src/main.rs\n",
            ["cli/src/main.rs: does not select released-upgrade"],
        ),
        (
            "jq . cli/src/main.rs\n",
            ["cli/src/main.rs: does not select released-upgrade"],
        ),
        (
            "helm install acme charts/curie -f cli/src/main.rs\n",
            ["cli/src/main.rs: does not select released-upgrade"],
        ),
        (
            "cat .github/e2e-selection.yaml\n",
            [".github/e2e-selection.yaml: does not select released-upgrade"],
        ),
        (
            "cat release/preserve_next.py\n",
            ["release/preserve_next.py: does not select released-upgrade"],
        ),
        (
            "cat pyproject.toml\n",
            ["pyproject.toml: does not select released-upgrade"],
        ),
        (
            'python3 -u "$UPGRADE_HELPER"\n',
            ["python3 $UPGRADE_HELPER: helper path cannot be classified"],
        ),
        (
            'bash -e "$UPGRADE_HELPER"\n',
            ["bash $UPGRADE_HELPER: helper path cannot be classified"],
        ),
        (
            'sh "$UPGRADE_HELPER"\n',
            ["sh $UPGRADE_HELPER: helper path cannot be classified"],
        ),
    ],
)
def test_upgrade_input_guard_rejects_relative_and_unclassifiable_reads(
    script: str,
    problems: list[str],
) -> None:
    assert _unselected_matrix_inputs(script) == problems


def test_released_upgrade_jobs_select_their_inline_repo_inputs() -> None:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    problems: dict[str, list[str]] = {}
    for job_name in (
        "e2e-released-upgrade",
        "e2e-released-upgrade-negative",
        "e2e-cluster-upgrade-matrix",
    ):
        problems[job_name] = []
        seen: set[str] = set()
        for step in jobs[job_name]["steps"]:
            run = step.get("run")
            if not isinstance(run, str):
                continue
            problems[job_name].extend(_unselected_matrix_inputs(run))
            for reference in _repo_root_references(run):
                if reference == "cli/scripts/e2e-ladder.sh":
                    continue
                problems[job_name].extend(_unselected_helper_inputs(reference, seen))
    # The upgraded-install smoke uses the same ladder invocation as the fresh
    # cluster rung. Selecting this one script also boots the negative control
    # and all matrix shards; test_e2e_ladder_script_stays_off_released_upgrade
    # pins the two equivalent invocations and the absence of ladder reads in
    # the other upgrade jobs.
    assert problems == {
        "e2e-released-upgrade": ["cli/scripts/e2e-ladder.sh: does not select released-upgrade"],
        "e2e-released-upgrade-negative": [],
        "e2e-cluster-upgrade-matrix": [],
    }


def test_tier_gated_jobs_select_their_repo_scripts() -> None:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    registry = _loaded_registry(REGISTRY)
    tiers_by_output = {OUTPUT_KEYS[tier]: tier for tier in BASE_TIERS}
    checked_jobs: set[str] = set()
    problems: set[str] = set()
    for job_name, job in jobs.items():
        condition = job.get("if", "")
        if not isinstance(condition, str):
            continue
        outputs = set(
            re.findall(
                r"needs\.changes\.outputs\.(skill|local|local_release|cluster)\b",
                condition,
            )
        )
        if job_name == CHART_JOB:
            assert condition == CHART_JOB_IF
            outputs.add("cluster")
        if not outputs:
            continue
        checked_jobs.add(job_name)
        for step in job["steps"]:
            run = step.get("run")
            if not isinstance(run, str):
                continue
            for reference in _repo_root_references(run):
                if not reference.startswith(("scripts/", "tools/")):
                    continue
                if not (REPO_ROOT / reference).exists():
                    continue
                selected = _SELECTOR_MODULE._select_path(registry, reference)
                for output in outputs:
                    tier = tiers_by_output[output]
                    if tier not in selected:
                        problems.add(f"{job_name}: {reference}: does not select {tier}")
    assert {"e2e-ladder-cluster", "e2e-cluster-chart-regressions"} <= checked_jobs
    assert not problems, "Tier-gated job script selection gaps:\n" + "\n".join(sorted(problems))


def test_inline_upgrade_guard_rejects_new_unselected_helper() -> None:
    assert _unselected_matrix_inputs("python3 cli/src/main.rs --check\n") == [
        "cli/src/main.rs: does not select released-upgrade"
    ]


def test_inline_upgrade_guard_does_not_treat_redirect_output_as_input() -> None:
    assert _unselected_matrix_inputs("cat > cli/src/main.rs <<EOF\nfixture\nEOF\n") == []


def test_inline_upgrade_guard_scans_a_called_helpers_own_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    subprocess.run(["git", "init", "--quiet", str(tmp_path)], check=True)
    helper = tmp_path / "charts/curie/ci/live_manifest_parity.py"
    helper.parent.mkdir(parents=True)
    helper.write_text('from pathlib import Path\nPath("cli/src/main.rs").read_text()\n')
    input_file = tmp_path / "cli/src/main.rs"
    input_file.parent.mkdir(parents=True)
    input_file.write_text("// fixture\n")
    monkeypatch.setattr(sys.modules[__name__], "REPO_ROOT", tmp_path)

    assert _unselected_helper_inputs("charts/curie/ci/live_manifest_parity.py", set()) == [
        "cli/src/main.rs: does not select released-upgrade"
    ]


@pytest.mark.parametrize(
    ("source", "problems"),
    [
        (
            "def fixture(relative):\n"
            '    if relative == ".github/workflows":\n'
            '        return 404, {"message": "Not Found"}\n'
            '    if relative == "README.md":\n'
            '        return 200, [{"path": "README.md", "name": "README.md"}]\n',
            [],
        ),
        (
            "def fixture(relative):\n"
            '    if relative in {".github/workflows", "README.md"}:\n'
            '        return 200, {"path": relative}\n',
            [],
        ),
        (
            'if Path(".github/workflows").is_dir():\n    pass\n',
            [".github/workflows: unlisted directory"],
        ),
        (
            'if Path("cli/src/main.rs").read_text() == "README.md":\n    pass\n',
            ["cli/src/main.rs: does not select released-upgrade"],
        ),
        (
            'def fixture():\n    return 200, {"content": Path("cli/src/main.rs").read_text()}\n',
            ["cli/src/main.rs: does not select released-upgrade"],
        ),
        (
            'input_file = "cli/src/main.rs"\nPath(input_file).read_text()\n',
            ["cli/src/main.rs: does not select released-upgrade"],
        ),
        (
            "def read(candidate):\n"
            '    if candidate == "cli/src/main.rs":\n'
            "        Path(candidate).read_text()\n",
            ["cli/src/main.rs: does not select released-upgrade"],
        ),
        (
            "def read(candidate):\n"
            '    if candidate == ".github/workflows":\n'
            "        selected = candidate\n"
            "        Path(selected).is_dir()\n",
            [".github/workflows: unlisted directory"],
        ),
        ('(seed / "README.md").write_text("Example fixture repository")\n', []),
        (
            '(seed / "README.md").write_text(Path("cli/src/main.rs").read_text())\n',
            ["cli/src/main.rs: does not select released-upgrade"],
        ),
    ],
    ids=[
        "remote-route-and-response",
        "remote-route-membership",
        "local-directory",
        "local-read-in-comparison",
        "local-read-in-response",
        "local-path-variable",
        "compared-local-path",
        "compared-local-path-alias",
        "generated-fixture-output",
        "checkout-read-into-output",
    ],
)
def test_python_helper_guard_distinguishes_remote_data_from_checkout_inputs(
    source: str, problems: list[str]
) -> None:
    assert _unselected_matrix_inputs(_python_input_source(source)) == problems


@pytest.mark.parametrize("python_helper", [False, True], ids=["workflow", "python-helper"])
def test_upgrade_guard_keeps_the_explicit_pytest_input(python_helper: bool) -> None:
    source = (
        f'subprocess.run(["uv", "run", "pytest", "{FACTORY_UPGRADE_RUNTIME}", "-q"])\n'
        if python_helper
        else f"uv run pytest {FACTORY_UPGRADE_RUNTIME} -q\n"
    )
    if python_helper:
        source = _python_input_source(source)
    assert _repo_root_references(source) == [FACTORY_UPGRADE_RUNTIME]


def test_remote_fixture_data_does_not_hide_a_transitive_checkout_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    subprocess.run(["git", "init", "--quiet", str(tmp_path)], check=True)
    sources = {
        "charts/curie/ci/live_manifest_parity.py": (
            "def fixture(relative):\n"
            '    if relative == ".github/workflows":\n'
            '        return 200, {"path": "README.md"}\n'
            'helper = Path("charts/curie/ci/make-upgrade-mutants.py")\n'
        ),
        "charts/curie/ci/make-upgrade-mutants.py": 'Path("cli/src/main.rs").read_text()\n',
        "cli/src/main.rs": "// fixture\n",
        ".github/workflows/ci.yaml": "name: Example\n",
        "README.md": "Example\n",
    }
    for relative, source in sources.items():
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source)
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True)
    monkeypatch.setattr(sys.modules[__name__], "REPO_ROOT", tmp_path)

    assert _unselected_helper_inputs("charts/curie/ci/live_manifest_parity.py", set()) == [
        "cli/src/main.rs: does not select released-upgrade"
    ]


def test_e2e_ladder_script_stays_off_released_upgrade(tmp_path: Path) -> None:
    """A recorded gap, not an oversight.

    e2e-released-upgrade runs the ladder's cluster rung against an upgraded
    install. e2e-ladder-cluster runs the same invocation against a fresh one,
    and the cli prefix selects it for a ladder edit wherever kind runs (next
    omits both). Selecting released-upgrade as well would boot the negative
    control and every matrix shard, none of which read the ladder. A ladder
    edit that breaks only on the upgraded install first fails on push to main
    or a dispatch, both of which select every tier.
    """
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]

    def ladder_tiers(job: str) -> list[str | None]:
        return [
            step.get("env", {}).get("CURIE_E2E_TIERS")
            for step in jobs[job]["steps"]
            if "cli/scripts/e2e-ladder.sh" in str(step.get("run", ""))
        ]

    assert ladder_tiers("e2e-released-upgrade") == ["cluster"]
    assert ladder_tiers("e2e-ladder-cluster") == ["cluster"]
    assert ladder_tiers("e2e-released-upgrade-negative") == []
    assert ladder_tiers("e2e-cluster-upgrade-matrix") == []
    assert "e2e-ladder" not in UPGRADE_MATRIX.read_text()
    _assert_selection(tmp_path, "cli/scripts/e2e-ladder.sh", BASE_TIERS)


@pytest.mark.parametrize(
    "path",
    [*APPROVED_ROOT_DOCS, "docs/example.md", "docs/guides/getting-started.md"],
)
def test_genuine_documentation_only_selects_no_runtime_e2e_tiers(
    tmp_path: Path,
    path: str,
) -> None:
    _assert_selection(tmp_path, path, (), pytest_needed=False)


@pytest.mark.parametrize(
    ("path", "pytest_needed"),
    [
        ("apps/ui/package.json", True),
        ("apps/ui/pnpm-lock.yaml", True),
        ("apps/dispatcher/src/curie_dispatcher/app.py", True),
        ("scripts/README.md", False),
        ("scripts/check-docs.sh", False),
        ("scripts/check-pr-body.sh", False),
        ("tools/fix-pin-ci/check.py", True),
        (".github/workflows/pr-body.yaml", True),
        ("release/authorize.py", True),
        ("runner/tests/test_repo_toolchain_proof_ci.py", True),
        ("packages/test-support/src/curie_test_support/valkey.py", True),
        ("examples/coder/evals/cases.json", False),
    ],
)
def test_known_non_runtime_paths_select_no_e2e_tiers(
    tmp_path: Path,
    path: str,
    pytest_needed: bool,
) -> None:
    _assert_selection(tmp_path, path, (), pytest_needed=pytest_needed)


@pytest.mark.parametrize("root_file", APPROVED_ROOT_DOCS)
def test_root_filename_directory_cannot_bypass_runtime_tiers(
    tmp_path: Path,
    root_file: str,
) -> None:
    # #1954: ignore the root file itself, never a directory with the same name.
    _assert_selection(tmp_path, f"{root_file}/runtime-check.sh", BASE_TIERS)


def test_unapproved_markdown_fallback_selects_all_base_tiers(tmp_path: Path) -> None:
    _assert_selection(tmp_path, "UNAPPROVED.md", BASE_TIERS)


def test_charts_curie_still_selects_cluster(tmp_path: Path) -> None:
    completed, output = _invoke_selector(tmp_path, "charts/curie/values.yaml")
    assert completed.returncode == 0, completed.stderr
    outputs = dict(line.split("=", maxsplit=1) for line in output.splitlines())
    assert outputs["cluster"] == "true"


# `ci/` is helmignored, so `helm package charts/curie` (the upgrade matrix) and
# `helm upgrade ... charts/curie` (the released upgrade) never load it. A change
# there cannot alter an upgrade and must not book fourteen kind shards.
RELEASED_UPGRADE_JOBS = (
    "e2e-released-upgrade",
    "e2e-released-upgrade-negative",
    "e2e-cluster-upgrade-matrix-shards",
    "e2e-cluster-upgrade-matrix",
)
# The scripts those jobs hand the chart to. A charts/curie/ci path either of
# them starts running must stay on released-upgrade too.
RELEASED_UPGRADE_SCRIPTS = (
    "cli/scripts/cluster-upgrade-matrix.sh",
    "cli/tests/data/upgrade-driver.py",
)
CHART_ROOT = "charts/curie"
CHART_CI = "charts/curie/ci"


def _selector_outputs(tmp_path: Path, path: str) -> dict[str, str]:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    return dict(line.split("=", maxsplit=1) for line in output.splitlines())


def _strings(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [text for item in value.values() for text in _strings(item)]
    if isinstance(value, list):
        return [text for item in value for text in _strings(item)]
    return []


def _chart_ci_paths_run_by_released_upgrade_jobs() -> set[str]:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    texts = [text for name in RELEASED_UPGRADE_JOBS for text in _strings(jobs[name])]
    texts.extend((REPO_ROOT / script).read_text() for script in RELEASED_UPGRADE_SCRIPTS)
    return {match for text in texts for match in re.findall(rf"{CHART_CI}/[A-Za-z0-9_./-]+", text)}


@pytest.mark.parametrize(
    "path",
    [
        "charts/curie/ci/runtime/metrics-alerts-runtime.sh",
        "charts/curie/ci/runtime/publication-job-assertions.sh",
        "charts/curie/ci/live-manifest-parity-assertions.sh",
    ],
)
def test_released_upgrade_skips_chart_ci_scripts_the_chart_never_ships(
    tmp_path: Path,
    path: str,
) -> None:
    outputs = _selector_outputs(tmp_path, path)
    assert outputs["released_upgrade"] == "false"
    assert outputs["cluster"] == "true"


def test_released_upgrade_selects_chart_ci_scripts_its_own_jobs_run(
    tmp_path: Path,
) -> None:
    run_by_jobs = _chart_ci_paths_run_by_released_upgrade_jobs()
    # Negative control: the scan must find the two scripts known to be run, or
    # an empty scan would pass this test without checking anything.
    assert {
        "charts/curie/ci/make-upgrade-mutants.py",
        "charts/curie/ci/live_manifest_parity.py",
    } <= run_by_jobs
    for path in sorted(run_by_jobs):
        assert (REPO_ROOT / path).is_file(), path
        assert _selects_released_upgrade(path), path


def test_every_shipped_chart_file_selects_released_upgrade(tmp_path: Path) -> None:
    helmignore = (REPO_ROOT / CHART_ROOT / ".helmignore").read_text().splitlines()
    assert "ci/" in helmignore, "the released-upgrade carve-out assumes ci/ is not shipped"
    tracked = subprocess.run(
        ["git", "ls-files", CHART_ROOT],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    # Every tracked file, not one per entry: a new shipped file, a narrowed
    # rule, or an ignored hole inside templates/ fails here instead of
    # silently dropping out of the upgrade gate.
    shipped = [path for path in tracked if not path.startswith(f"{CHART_CI}/")]
    assert len(shipped) < len(tracked), "expected tracked charts/curie/ci files"
    entries = {path.removeprefix(f"{CHART_ROOT}/").split("/", 1)[0] for path in shipped}
    assert {"Chart.yaml", "templates", "values.yaml"} <= entries
    for path in shipped:
        assert _selects_released_upgrade(path), path


@pytest.mark.parametrize(
    ("path", "selected"),
    [
        (".github/action.yml", BASE_TIERS),
        ("apps/api/README.md", ("local", "local-release", "cluster")),
        ("apps/api/runtime-config.yaml", ("local", "local-release", "cluster")),
        ("packages/plugin-format/README.md", BASE_TIERS),
        ("packages/plugin-format/plugin.yaml", BASE_TIERS),
        ("examples/weather/README.md", BASE_TIERS),
        ("examples/weather/skill-config.yaml", BASE_TIERS),
        ("tests/README.md", BASE_TIERS),
        ("tests/selector-config.yaml", BASE_TIERS),
        ("UNAPPROVED.md", BASE_TIERS),
    ],
)
def test_non_allowlisted_paths_never_bypass_runtime_e2e_selection(
    tmp_path: Path,
    path: str,
    selected: tuple[str, ...],
) -> None:
    _assert_selection(tmp_path, path, selected)


def test_mixed_root_documentation_and_runtime_diff_selects_runtime_tiers(
    tmp_path: Path,
) -> None:
    completed, output = _invoke_selector(tmp_path, "ARCHITECTURE.md", "apps/api/main.py")
    assert completed.returncode == 0, completed.stderr
    assert output == _expected_output(
        "local",
        "local-release",
        "cluster",
        pytest_deselect=ALL_CONDITIONAL,
        chart_scenarios=("quota-progress", "workspace-diagnostic"),
        ci_policy_files=_expected_ci_policy_files("ARCHITECTURE.md", "apps/api/main.py"),
    )


def test_unknown_and_union_selection_are_deterministic(tmp_path: Path) -> None:
    unknown, unknown_output = _invoke_selector(tmp_path, "new-surface/module.py")
    assert unknown.returncode == 0, unknown.stderr
    assert unknown_output == _expected_output(
        *BASE_TIERS,
        ci_policy_files=_expected_ci_policy_files("new-surface/module.py"),
    )

    forward, forward_output = _invoke_selector(
        tmp_path,
        "charts/curie/values.yaml",
        "compose.dev.yaml",
        "otel/collector.yaml",
    )
    reverse, reverse_output = _invoke_selector(
        tmp_path,
        "otel/collector.yaml",
        "compose.dev.yaml",
        "charts/curie/values.yaml",
    )
    assert forward.returncode == 0, forward.stderr
    assert reverse.returncode == 0, reverse.stderr
    assert (
        forward_output
        == reverse_output
        == _expected_output(
            "local",
            "local-release",
            "cluster",
            "released-upgrade",
            ci_policy_files=_expected_ci_policy_files(
                "charts/curie/values.yaml", "compose.dev.yaml", "otel/collector.yaml"
            ),
        )
    )


def test_push_selects_every_tier_without_a_repository(tmp_path: Path) -> None:
    """Dispatches use --push without kind omission."""
    completed, output = _invoke_selector(tmp_path, push=True)
    assert completed.returncode == 0, completed.stderr
    assert output == _expected_output(
        *TIERS,
        images_needed=True,
        cli_release_needed=True,
        released_upgrade_full=True,
        factory=True,
        approval_resume=True,
        ci_policy_files=_expected_ci_policy_files(push=True),
    )


def test_pull_request_released_upgrade_runs_the_smoke_shard_only(
    tmp_path: Path,
) -> None:
    # Chart-only pull requests retain the upgrade matrix smoke shard.
    outputs = _selector_outputs(tmp_path, "charts/curie/values.yaml")
    assert outputs["released_upgrade"] == "true"
    assert outputs["released_upgrade_full"] == "false"
    _assert_upgrade_smoke_shards(tmp_path, outputs)


def test_omit_kind_drops_cluster_tiers_and_keeps_the_rest(tmp_path: Path) -> None:
    kept = tuple(tier for tier in TIERS if tier not in {"cluster", "released-upgrade"})
    completed, output = _invoke_selector(tmp_path, push=True, omit_kind=True)
    assert completed.returncode == 0, completed.stderr
    # Pushes to both trains omit kind, so they run no upgrade job.
    assert output == _expected_output(
        *kept,
        images_needed=True,
        cli_release_needed=True,
        ci_policy_files=_expected_ci_policy_files(push=True),
    )

    chart, chart_output = _invoke_selector(
        tmp_path,
        "charts/curie/values.yaml",
        omit_kind=True,
    )
    assert chart.returncode == 0, chart.stderr
    assert chart_output == _expected_output(
        pytest_needed=True,
        ci_policy_files=_expected_ci_policy_files("charts/curie/values.yaml"),
    )

    untouched, untouched_output = _invoke_selector(
        tmp_path,
        "charts/curie/values.yaml",
    )
    assert untouched.returncode == 0, untouched.stderr
    assert untouched_output == _expected_output(
        "cluster",
        "released-upgrade",
        pytest_needed=True,
        ci_policy_files=_expected_ci_policy_files("charts/curie/values.yaml"),
    )


def test_revisions_select_changed_paths_and_unknown_fallback(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    subprocess.run(
        ["git", "init", "--initial-branch", "main"],
        cwd=repository,
        capture_output=True,
        text=True,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"],
        cwd=repository,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test User"],
        cwd=repository,
        check=True,
    )

    known_path = repository / "compose.dev.yaml"
    known_path.write_text("version: one\n")
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

    known_path.write_text("version: two\n")
    subprocess.run(["git", "commit", "-am", "known"], cwd=repository, check=True)
    known_head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    known, known_output = _invoke_selector(
        tmp_path,
        base=base,
        head=known_head,
        cwd=repository,
    )
    assert known.returncode == 0, known.stderr
    assert known_output == _expected_output(
        "local",
        pytest_deselect=ALL_CONDITIONAL,
        ci_policy_files=_expected_ci_policy_files("compose.dev.yaml"),
    )

    (repository / "new-surface.txt").write_text("new\n")
    subprocess.run(["git", "add", "."], cwd=repository, check=True)
    subprocess.run(["git", "commit", "-m", "unknown"], cwd=repository, check=True)
    unknown_head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    unknown, unknown_output = _invoke_selector(
        tmp_path,
        base=known_head,
        head=unknown_head,
        cwd=repository,
    )
    assert unknown.returncode == 0, unknown.stderr
    assert unknown_output == _expected_output(
        *BASE_TIERS,
        ci_policy_files=_expected_ci_policy_files("new-surface.txt"),
    )


# The exact five files of the real v0.11.2 preparation PR #3845, with the
# snapshot renamed for whatever release this checkout is (#3858).
def _repo_version() -> str:
    cargo = tomllib.loads((REPO_ROOT / "cli" / "Cargo.toml").read_text())
    return f"v{cargo['package']['version']}"


def _version_only_paths(version: str) -> tuple[str, ...]:
    return (
        "charts/curie/Chart.yaml",
        "cli/Cargo.toml",
        "cli/Cargo.lock",
        "docs/architecture-atlas/versions.json",
        f"docs/architecture-atlas/snapshots/{version}.json",
    )


# What #3858 runs for a version-only diff: the local-release rung, which
# exercises version identity on the new commit, and nothing heavier.
def _version_only_selection() -> str:
    return _expected_output(
        "local-release",
        pytest_needed=False,
        cli_release_needed=True,
        version_only=True,
        ci_policy_files=_expected_ci_policy_files(*_version_only_paths(_repo_version())),
    )


# What the same five paths selected before #3858, and still select whenever
# the delta is not version-only.
def _full_prep_selection() -> str:
    return _expected_output(
        *TIERS,
        pytest_deselect=_expected_conditional_deselect(*_version_only_paths(_repo_version())),
        cli_release_needed=True,
        ci_policy_files=_expected_ci_policy_files(*_version_only_paths(_repo_version())),
    )


@pytest.mark.parametrize("omit_kind", [False, True])
def test_version_only_release_prep_selects_only_the_local_release_rung(
    tmp_path: Path, omit_kind: bool
) -> None:
    """#3858: a release bump reuses the proof it already has."""
    completed, output = _invoke_selector(
        tmp_path, *_version_only_paths(_repo_version()), omit_kind=omit_kind
    )
    assert completed.returncode == 0, completed.stderr
    assert output == _version_only_selection()


def test_another_releases_snapshot_is_not_version_only(tmp_path: Path) -> None:
    """#3858: only this checkout's own snapshot is version-only."""
    assert _repo_version() != "v9.9.9"
    completed, output = _invoke_selector(tmp_path, *_version_only_paths("v9.9.9"))
    assert completed.returncode == 0, completed.stderr
    assert output == _full_prep_selection()


@pytest.mark.parametrize("extra", ["cli/src/main.rs", "README.md"])
def test_any_other_path_makes_the_prep_diff_not_version_only(tmp_path: Path, extra: str) -> None:
    """#3858: one path outside the set restores today's selection."""
    completed, output = _invoke_selector(tmp_path, *_version_only_paths(_repo_version()), extra)
    assert completed.returncode == 0, completed.stderr
    assert output == _full_prep_selection()


def test_push_is_never_version_only(tmp_path: Path) -> None:
    """#3858: pushes and dispatches run everything."""
    completed, output = _invoke_selector(tmp_path, push=True)
    assert completed.returncode == 0, completed.stderr
    assert _outputs_of(output)["version_only"] == "false"


def _outputs_of(output: str) -> dict[str, str]:
    return dict(line.split("=", maxsplit=1) for line in output.splitlines())


def _git(repository: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=repository,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def _commit_files(repository: Path, files: dict[str, str], message: str) -> str:
    for path, content in files.items():
        target = repository / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
    _git(repository, "add", "--", *files)
    _git(repository, "commit", "-q", "-m", message)
    return _git(repository, "rev-parse", "HEAD")


def _new_repository(tmp_path: Path) -> Path:
    repository = tmp_path / "repository"
    repository.mkdir()
    _git(repository, "init", "-q", "--initial-branch", "main")
    _git(repository, "config", "user.email", "test@example.com")
    _git(repository, "config", "user.name", "Test User")
    workflow = repository / ".github/workflows/ci.yaml"
    workflow.parent.mkdir(parents=True)
    workflow.write_text(WORKFLOW.read_text())
    _git(repository, "add", ".github/workflows/ci.yaml")
    return repository


def _cargo_toml(version: str) -> str:
    return f'[package]\nname = "curie"\nversion = "{version}"\n'


def test_revision_mode_reads_the_version_from_the_head_commit(tmp_path: Path) -> None:
    """#3858: revision mode names the snapshot from head's cli/Cargo.toml."""
    repository = _new_repository(tmp_path)
    base = _commit_files(
        repository,
        {
            "cli/Cargo.toml": _cargo_toml("1.2.2"),
            "cli/src/main.rs": "fn main() {}\n",
            "README.md": "readme\n",
        },
        "base",
    )
    version_files = {path: f"{path} bumped\n" for path in _version_only_paths("v1.2.3")}
    version_files["cli/Cargo.toml"] = _cargo_toml("1.2.3")
    head = _commit_files(repository, version_files, "prepare v1.2.3")

    completed, output = _invoke_selector(tmp_path, base=base, head=head, cwd=repository)
    assert completed.returncode == 0, completed.stderr
    assert output == _version_only_selection()

    # Negative control: a snapshot named for a version head does not declare
    # is not version-only.
    wrong = {path: f"{path} again\n" for path in _version_only_paths("v1.2.4")}
    wrong["cli/Cargo.toml"] = _cargo_toml("1.2.3")
    wrong_head = _commit_files(repository, wrong, "snapshot for another version")
    completed, output = _invoke_selector(tmp_path, base=head, head=wrong_head, cwd=repository)
    assert completed.returncode == 0, completed.stderr
    assert _outputs_of(output)["version_only"] == "false"
    assert _outputs_of(output)["local"] == "true"


def test_revision_mode_without_a_cargo_manifest_is_not_version_only(
    tmp_path: Path,
) -> None:
    """#3858: an unreadable version fails closed to today's selection."""
    repository = _new_repository(tmp_path)
    base = _commit_files(repository, {"README.md": "readme\n"}, "base")
    paths = [path for path in _version_only_paths("v1.2.3") if not path.startswith("cli/")]
    head = _commit_files(repository, {path: f"{path} bumped\n" for path in paths}, "no manifest")

    completed, output = _invoke_selector(tmp_path, base=base, head=head, cwd=repository)
    assert completed.returncode == 0, completed.stderr
    path_mode, path_output = _invoke_selector(tmp_path, *paths)
    assert path_mode.returncode == 0, path_mode.stderr
    assert _outputs_of(output)["version_only"] == "false"
    assert output == path_output


VALID_REGISTRY = """
version: 1
fallback: [skill, local, local-release, cluster]
pytest_scopes:
  cli:
    prefixes: [cli]
    excluded_prefixes: [cli/scripts]
    python_prefixes: [cli/tests]
    test_files: tools/e2e-ci-selection/cli_pytest_files.txt
conditional_pytest_files: {}
ci_policy_tests:
  tools/e2e-ci-selection/tests/test_e2e_ci_selection.py: [.github, tools/e2e-ci-selection]
rules:
  exact:
    compose.dev.yaml: [local]
    apps/worker/src/curie_worker/config.py: [released-upgrade]
  prefixes:
    charts: [cluster]
    charts/curie: [released-upgrade]
  ignored_exact:
    README.md: []
  ignored_prefixes:
    docs: []
""" + yaml.safe_dump(
    {
        "chart_scenarios": {
            "job": CHART_JOB,
            "shared_inputs": list(CHART_SHARED_INPUTS),
            "scenarios": CHART_SCENARIOS,
        }
    },
    sort_keys=False,
)


def test_exact_path_under_ignored_prefix_is_selected(tmp_path: Path) -> None:
    registry = tmp_path / "registry.yaml"
    registry.write_text(
        VALID_REGISTRY.replace(
            "    compose.dev.yaml: [local]",
            "    compose.dev.yaml: [local]\n    scripts/selected.sh: [cluster]",
        ).replace("    docs: []", "    docs: []\n    scripts: []")
    )
    selected, selected_output = _invoke_selector(tmp_path, "scripts/selected.sh", registry=registry)
    assert selected.returncode == 0, selected.stderr
    assert selected_output == _expected_output(
        "cluster",
        chart_scenarios=(),
        ci_policy_files=_expected_ci_policy_files("scripts/selected.sh", registry=registry),
    )
    for path in ("scripts/other.sh", "scripts/selected.sh/child.sh"):
        ignored, ignored_output = _invoke_selector(tmp_path, path, registry=registry)
        assert ignored.returncode == 0, ignored.stderr
        assert ignored_output == _expected_output(
            pytest_needed=False,
            ci_policy_files=_expected_ci_policy_files(path, registry=registry),
        )


@pytest.mark.parametrize(
    "registry_text",
    [
        VALID_REGISTRY.replace("charts: [cluster]", "charts: [unknown]"),
        VALID_REGISTRY.replace("charts: [cluster]", "charts: [cluster, cluster]"),
        VALID_REGISTRY.replace("compose.dev.yaml: [local]", "compose.dev.yaml: []"),
        VALID_REGISTRY.replace("charts: [cluster]", "charts: []"),
        VALID_REGISTRY.replace("version: 1", "version: true"),
        VALID_REGISTRY.replace(
            "    charts: [cluster]",
            "    charts: [cluster]\n    charts: [local]",
        ),
        VALID_REGISTRY.replace("  exact:\n    compose.dev.yaml: [local]", "  exact: []"),
        VALID_REGISTRY.replace(
            "fallback: [skill, local, local-release, cluster]",
            "fallback: [skill, local]",
        ),
        VALID_REGISTRY.replace("    docs: []", "    charts: []"),
    ],
    ids=(
        "unknown_tier",
        "duplicate_tier",
        "empty_exact_tiers",
        "empty_prefix_tiers",
        "boolean_version",
        "duplicate_rule",
        "malformed_entry",
        "invalid_fallback",
        "ignored_overlap",
    ),
)
def test_selector_rejects_invalid_registries(
    tmp_path: Path,
    registry_text: str,
) -> None:
    registry = tmp_path / "registry.yaml"
    registry.write_text(registry_text)
    completed, _output = _invoke_selector(tmp_path, "charts/example.yaml", registry=registry)
    assert completed.returncode != 0


@pytest.mark.parametrize(
    "entry",
    [
        "README.md: [skill]",
        "README.md/: []",
        "/README.md: []",
        "compose.dev.yaml: []",
        "charts: []",
    ],
)
def test_selector_rejects_invalid_exact_ignores(tmp_path: Path, entry: str) -> None:
    registry = tmp_path / "registry.yaml"
    registry.write_text(VALID_REGISTRY.replace("README.md: []", entry))
    completed, output = _invoke_selector(tmp_path, "README.md", registry=registry)
    assert completed.returncode != 0
    assert not output


def test_exact_ignore_under_selected_prefix_does_not_hide_siblings(tmp_path: Path) -> None:
    registry = tmp_path / "registry.yaml"
    registry.write_text(VALID_REGISTRY.replace("README.md: []", "charts/curie/README.md: []"))
    ignored, ignored_output = _invoke_selector(
        tmp_path, "charts/curie/README.md", registry=registry
    )
    sibling, sibling_output = _invoke_selector(
        tmp_path, "charts/curie/README.md/runtime-check.sh", registry=registry
    )
    assert ignored.returncode == sibling.returncode == 0
    assert ignored_output == _expected_output(
        pytest_needed=False,
        ci_policy_files=_expected_ci_policy_files("charts/curie/README.md", registry=registry),
    )
    assert sibling_output == _expected_output(
        "cluster",
        "released-upgrade",
        chart_scenarios=(),
        ci_policy_files=_expected_ci_policy_files(
            "charts/curie/README.md/runtime-check.sh", registry=registry
        ),
    )


def test_more_specific_ignored_child_of_selected_prefix_is_allowed(tmp_path: Path) -> None:
    registry = tmp_path / "registry.yaml"
    registry.write_text(VALID_REGISTRY.replace("    docs: []", "    docs: []\n    charts/ci: []"))
    ignored, ignored_output = _invoke_selector(tmp_path, "charts/ci/probe.sh", registry=registry)
    assert ignored.returncode == 0, ignored.stderr
    assert ignored_output == _expected_output(
        pytest_needed=False,
        ci_policy_files=_expected_ci_policy_files("charts/ci/probe.sh", registry=registry),
    )

    selected, selected_output = _invoke_selector(
        tmp_path, "charts/curie/values.yaml", registry=registry
    )
    assert selected.returncode == 0, selected.stderr
    assert selected_output == _expected_output(
        "cluster",
        "released-upgrade",
        ci_policy_files=_expected_ci_policy_files("charts/curie/values.yaml", registry=registry),
    )


AGGREGATE_EXPRESSIONS = {
    "changes_result": "${{ needs.changes.result }}",
    "registration_result": "${{ needs.runtime-assertion-registration.result }}",
    "event_name": "${{ github.event_name }}",
    "nightly": "${{ inputs.nightly }}",
    "skill_selected": "${{ needs.changes.outputs.skill }}",
    "local_selected": "${{ needs.changes.outputs.local }}",
    "local_release_selected": "${{ needs.changes.outputs.local_release }}",
    "cluster_selected": "${{ needs.changes.outputs.cluster }}",
    "chart_scenarios": "${{ needs.changes.outputs.chart_scenarios }}",
    "released_upgrade_selected": "${{ needs.changes.outputs.released_upgrade }}",
    "released_upgrade_full": "${{ needs.changes.outputs.released_upgrade_full }}",
    "approval_resume_selected": "${{ needs.changes.outputs.approval_resume }}",
    "factory_selected": "${{ needs.changes.outputs.factory }}",
    "skill_local_result": "${{ needs.e2e-ladder.result }}",
    "oidc_result": "${{ needs.e2e-oidc.result }}",
    "local_release_result": "${{ needs.e2e-ladder-release.result }}",
    "cluster_result": "${{ needs.e2e-ladder-cluster.result }}",
    "cluster_chart_result": "${{ needs.e2e-cluster-chart-regressions.result }}",
    "rollout_recovery_result": "${{ needs.e2e-cluster-rollout-recovery.result }}",
    "approval_resume_result": ("${{ needs.e2e-cluster-approval-resume-restarts.result }}"),
    "factory_result": "${{ needs.e2e-scripted-factory.result }}",
    "forge_instance_result": "${{ needs.forge-instance-regression.result }}",
    "admitted_agent_result": "${{ needs.factory-admitted-agent.result }}",
    "released_upgrade_result": "${{ needs.e2e-released-upgrade.result }}",
    "released_upgrade_negative_result": ("${{ needs.e2e-released-upgrade-negative.result }}"),
    "upgrade_matrix_shards_result": ("${{ needs.e2e-cluster-upgrade-matrix-shards.result }}"),
    "upgrade_matrix_result": "${{ needs.e2e-cluster-upgrade-matrix.result }}",
}


def _changes_filter_script() -> str:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    trigger = workflow[True]
    nightly = trigger["workflow_dispatch"]["inputs"]["nightly"]
    assert nightly["type"] == "boolean"
    assert nightly["default"] is False
    group = workflow["concurrency"]["group"]
    assert group == ("${{ github.workflow }}-${{ github.ref }}-${{ github.event_name }}")
    run = next(
        step["run"] for step in workflow["jobs"]["changes"]["steps"] if step.get("id") == "filter"
    )
    assert isinstance(run, str)
    assert "github.event_name" in run
    assert '"$base_ref" = "next"' in run
    assert run.count("select_tiers.py") == 3
    return run


def _assert_changes_filter_route(script: str, event: str, branch: str) -> list[str]:
    for expression, value in {
        "github.event_name": event,
        "github.ref": f"refs/heads/{branch}",
        "github.base_ref": branch if event == "pull_request" else "",
    }.items():
        script = script.replace("${{ " + expression + " }}", value)
    assert "${{" not in script
    completed = subprocess.run(
        [
            "bash", "--noprofile", "--norc", "-c",
            "git() { printf '%s\\0' git \"$@\" >&2; }\n"
            "uv() { printf '%s\\0' \"$@\"; }\n" + script,
        ],
        cwd=REPO_ROOT, capture_output=True, text=True, check=False,
    )
    assert completed.returncode == 0, completed.stderr
    argv = completed.stdout.removesuffix("\0").split("\0")
    prefix = [
        "run", "--no-project", "--with", "pyyaml==6.0.3", "python",
        "tools/e2e-ci-selection/select_tiers.py", "--registry", ".github/e2e-selection.yaml",
    ]
    options = (
        ["--base", f"origin/{branch}", "--head", "HEAD"]
        if event == "pull_request" else ["--push"]
    )
    if event == "push" or (event == "pull_request" and branch == "next"):
        options.append("--omit-kind")
    assert argv == prefix + options
    fetch = f"git\0fetch\0--quiet\0origin\0{branch}\0" if event == "pull_request" else ""
    assert completed.stderr == fetch
    return argv[len(prefix):]


@pytest.mark.parametrize("event", ("push", "workflow_dispatch", "pull_request"))
@pytest.mark.parametrize("branch", ("main", "next"))
def test_both_trains_omit_kind_on_push_and_keep_full_dispatch(
    tmp_path: Path, event: str, branch: str
) -> None:
    options = _assert_changes_filter_route(_changes_filter_script(), event, branch)
    if event == "pull_request":
        return
    output = tmp_path / "filter-output"
    completed = subprocess.run(
        [sys.executable, str(SELECTOR), "--registry", str(REGISTRY), *options],
        cwd=REPO_ROOT, env={**os.environ, "GITHUB_OUTPUT": str(output)},
        capture_output=True, text=True, check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    full = event == "workflow_dispatch"
    assert output.read_text() == _expected_output(
        *(TIERS if full else TIERS[:3]),
        ci_policy_files=_expected_ci_policy_files(".github/workflows/ci.yaml"),
        images_needed=True, cli_release_needed=True, released_upgrade_full=full,
        factory=full, approval_resume=full,
    )
    if full:
        listed = subprocess.run(
            ["bash", str(UPGRADE_MATRIX), "--list-shards", "--json"],
            cwd=REPO_ROOT, capture_output=True, text=True, check=True,
        )
        expected = [shard["id"] for shard in json.loads(listed.stdout)["shards"]]
        flags = _outputs_of(output.read_text())
        matrix = _list_shards(tmp_path, flags["released_upgrade_full"])
        assert json.loads(matrix.removeprefix("shards=")) == expected


@pytest.mark.parametrize(
    ("mutation", "event", "branch"),
    [
        ("next-only", "push", "main"),
        ("main-only", "push", "next"),
        ("push-with-kind", "push", "main"),
        ("push-with-kind", "push", "next"),
        ("dispatch-without-kind", "workflow_dispatch", "main"),
        ("dispatch-without-kind", "workflow_dispatch", "next"),
    ],
)
def test_changes_filter_rejects_each_push_and_dispatch_route_mutation(
    mutation: str, event: str, branch: str
) -> None:
    script = _changes_filter_script()
    _assert_changes_filter_route(script, event, branch)
    push_route = 'elif [ "$event" = "push" ]; then'
    assert script.count(push_route) == 1
    if mutation in {"next-only", "main-only"}:
        train = mutation.removesuffix("-only")
        script = script.replace(
            push_route,
            'elif [ "$event" = "push" ] && '
            f'[ "${{{{ github.ref }}}}" = "refs/heads/{train}" ]; then',
            1,
        )
    elif mutation == "push-with-kind":
        assert script.count("--omit-kind\n") == 1
        script = script.replace("--omit-kind\n", "\n", 1)
    else:
        before, after = script.rsplit("--push", 1)
        script = before + "--push --omit-kind" + after
    with pytest.raises(AssertionError):
        _assert_changes_filter_route(script, event, branch)


def test_push_runs_complete_while_pull_requests_and_dispatches_supersede() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    concurrency = workflow["concurrency"]
    assert set(concurrency) == {"group", "cancel-in-progress"}
    assert concurrency["group"] == (
        "${{ github.workflow }}-${{ github.ref }}-${{ github.event_name }}"
    )
    # Only push runs complete; pull requests and dispatches supersede.
    assert concurrency["cancel-in-progress"] == "${{ github.event_name != 'push' }}"
    # A new trigger must force a decision about whether its runs cancel.
    assert set(workflow[True]) == {"push", "pull_request", "workflow_dispatch"}


IMAGE_PROOFS_IF = (
    "${{ !cancelled() && needs.changes.result == 'success' && "
    "(needs.ci-images.result == 'success' || needs.ci-images.result == 'skipped') }}"
)
IMAGE_GROUP_IF = "${{ !cancelled() && needs.changes.outputs.images == 'true' }}"
DOCKER_GROUP_IF = "${{ !cancelled() && steps.scope.outputs.run == 'true' }}"
IMAGE_PROOF_GROUPS = {
    "Docker daemon: ": (
        "Checkout",
        "Scope",
        "Toolchain",
        "Cargo cache",
        "Fmt",
        "Clippy",
        "Test",
        "Entrypoint test",
        "Pull Docker Hub images through the mirror",
        "Set up Buildx",
        "Back off before retrying Set up Buildx",
        "Set up Buildx (retry)",
        "Build both release arches (no push)",
        "Build the amd64 candidate",
        "Escape test against the candidate",
    ),
    "Dispatcher: ": (
        "Checkout",
        "Load the dispatcher image built by the ci-images job",
        "Tag the dispatcher image for this smoke test",
        "Entrypoint imports resolve in the built image",
    ),
    "Mail adapter: ": (
        "Checkout",
        "Load the mail-adapter image built by the ci-images job",
        "Tag the mail-adapter image for this smoke test",
        "Entrypoint imports resolve in the built image",
    ),
    "UI: ": (
        "Checkout",
        "Load the ui image built by the ci-images job",
        "Tag the ui image for this smoke test",
        "Start the stub API upstream",
        "Start the UI container",
        "Assert response headers",
        "Tear down the UI container and its API upstream",
    ),
    "Toolchain: ": (
        "Checkout",
        "Install uv",
        "Back off before retrying Install uv",
        "Install uv (retry)",
        "Sync workspace",
        "Required setting is present (negative control)",
        "Absent image fails in required mode (negative control)",
        "Load the runner image built by the ci-images job",
        "Tag the runner image as the harness default (curie-runner)",
        "Repository toolchain proof",
    ),
}


def _assert_image_proofs(workflow: dict[str, Any]) -> None:
    jobs = workflow["jobs"]
    assert not {
        "docker-daemon",
        "dispatcher-image-smoke",
        "mail-adapter-image-smoke",
        "ui-image-smoke",
        "repo-toolchain-proof",
    } & jobs.keys()
    assert "image-proofs" in jobs
    job = jobs["image-proofs"]
    assert job["name"] == "Image proofs (smoke, Docker daemon, toolchain)"
    assert job["runs-on"] == "ubuntu-latest"
    assert job["needs"] == ["changes", "ci-images"]
    assert job["if"] == IMAGE_PROOFS_IF
    assert job["env"] == {"CURIE_REPO_TOOLCHAIN_PROOF": "required"}
    assert not {"defaults", "continue-on-error", "timeout-minutes"} & job.keys()
    steps = job["steps"]
    assert len(steps) == 40
    assert [step["name"] for step in steps] == [
        prefix + name for prefix, names in IMAGE_PROOF_GROUPS.items() for name in names
    ], "all former steps must retain group order, with UI before Toolchain"
    by_name = {step["name"]: step for step in steps}
    assert [
        (step["name"], step["with"]["images"])
        for step in steps
        if step.get("uses") == "./.github/actions/load-ci-images"
    ] == [
        (prefix + f"Load the {image} image built by the ci-images job", image)
        for prefix, image in (
            ("Dispatcher: ", "dispatcher"),
            ("Mail adapter: ", "mail-adapter"),
            ("UI: ", "ui"),
            ("Toolchain: ", "runner"),
        )
    ]
    for prefix in IMAGE_PROOF_GROUPS:
        checkout = by_name[prefix + "Checkout"]
        assert checkout["uses"].startswith("actions/checkout@")
        assert checkout["with"] == (
            {"fetch-depth": 0, "persist-credentials": False}
            if prefix == "Docker daemon: "
            else {"persist-credentials": False}
        )
    assert sum(step.get("uses", "").startswith("actions/checkout@") for step in steps) == 5
    scope = by_name["Docker daemon: Scope"]
    assert scope["id"] == "scope"
    assert scope["env"] == {
        "EVENT_NAME": "${{ github.event_name }}",
        "BASE_REF": "${{ github.base_ref }}",
    }
    assert by_name["Docker daemon: Set up Buildx"]["id"] == "set_up_buildx"
    assert by_name["Toolchain: Install uv"]["id"] == "install_uv"
    assert {step["name"] for step in steps if "continue-on-error" in step} == {
        "Docker daemon: Set up Buildx",
        "Toolchain: Install uv",
    }
    for name in ("Docker daemon: Set up Buildx", "Toolchain: Install uv"):
        assert by_name[name]["continue-on-error"] is True
    inherited = {"Toolchain", "Fmt", "Clippy", "Test", "Back off before retrying Set up Buildx"}
    workspace = {
        "Scope",
        "Entrypoint test",
        "Pull Docker Hub images through the mirror",
        "Escape test against the candidate",
    }
    for step in steps:
        name = step["name"]
        docker = name.startswith("Docker daemon: ")
        gate = DOCKER_GROUP_IF if docker else IMAGE_GROUP_IF
        if name in ("Docker daemon: Checkout", "Docker daemon: Scope"):
            gate = "${{ !cancelled() }}"
        elif name in (
            "Docker daemon: Back off before retrying Set up Buildx",
            "Docker daemon: Set up Buildx (retry)",
            "Toolchain: Back off before retrying Install uv",
            "Toolchain: Install uv (retry)",
        ):
            attempt = "set_up_buildx" if docker else "install_uv"
            gate = gate.removesuffix(" }}") + f" && steps.{attempt}.outcome == 'failure'" + " }}"
        elif name == "UI: Tear down the UI container and its API upstream":
            gate = "${{ !cancelled() && needs.changes.outputs.images == 'true' && always() }}"
        assert step.get("if") == gate, f"step gate: {name}"
        label = name.removeprefix("Docker daemon: ")
        if docker and label in inherited:
            directory = "apps/docker-daemon"
        elif docker and label in workspace:
            directory = "${{ github.workspace }}"
        else:
            directory = None
        assert step.get("working-directory") == directory, name


def test_image_proofs_workflow_contract() -> None:
    _assert_image_proofs(yaml.safe_load(WORKFLOW.read_text()))


@pytest.mark.parametrize(
    "name",
    [
        "Dispatcher: Entrypoint imports resolve in the built image",
        "Mail adapter: Entrypoint imports resolve in the built image",
        "UI: Assert response headers",
        "Toolchain: Repository toolchain proof",
    ],
)
def test_image_proofs_rejects_a_group_losing_its_image_gate(name: str) -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    _assert_image_proofs(workflow)
    step = next(step for step in workflow["jobs"]["image-proofs"]["steps"] if step["name"] == name)
    step["if"] = step["if"].replace(" && needs.changes.outputs.images == 'true'", "")
    with pytest.raises(AssertionError, match="step gate"):
        _assert_image_proofs(workflow)


@pytest.mark.parametrize(
    ("name", "clause"),
    [
        ("Docker daemon: Scope", "!cancelled()"),
        ("Docker daemon: Fmt", "steps.scope.outputs.run == 'true'"),
        (
            "Docker daemon: Back off before retrying Set up Buildx",
            "steps.scope.outputs.run == 'true'",
        ),
        ("Docker daemon: Set up Buildx (retry)", "steps.set_up_buildx.outcome == 'failure'"),
        (
            "Toolchain: Back off before retrying Install uv",
            "needs.changes.outputs.images == 'true'",
        ),
        ("Toolchain: Install uv (retry)", "steps.install_uv.outcome == 'failure'"),
        ("UI: Tear down the UI container and its API upstream", "always()"),
    ],
)
def test_image_proofs_rejects_a_changed_step_gate(name: str, clause: str) -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    _assert_image_proofs(workflow)
    step = next(step for step in workflow["jobs"]["image-proofs"]["steps"] if step["name"] == name)
    step["if"] = step["if"].replace(clause, "true")
    with pytest.raises(AssertionError, match="step gate"):
        _assert_image_proofs(workflow)


@pytest.mark.parametrize(
    "mutation", ["missing-step", "toolchain-first", "union-load", "scoped-bootstrap"]
)
def test_image_proofs_rejects_a_changed_group_contract(mutation: str) -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    _assert_image_proofs(workflow)
    steps = workflow["jobs"]["image-proofs"]["steps"]
    if mutation == "missing-step":
        steps.pop()
    elif mutation == "toolchain-first":
        steps[23:] = steps[30:] + steps[23:30]
    elif mutation == "union-load":
        load = next(
            step for step in steps if step.get("uses") == "./.github/actions/load-ci-images"
        )
        load["with"]["images"] = "dispatcher,mail-adapter,ui,runner"
    else:
        steps[1]["if"] = DOCKER_GROUP_IF
    with pytest.raises(AssertionError):
        _assert_image_proofs(workflow)


def _nightly_if(selection: str) -> str:
    return "${{ " + NIGHTLY_DISPATCH + " && " + selection + " }}"


def _assert_upgrade_job_gates(workflow: dict[str, Any]) -> None:
    jobs = workflow["jobs"]
    positive = jobs["e2e-released-upgrade"]["if"]
    assert positive == _nightly_if("needs.changes.outputs.released_upgrade_full == 'true'")
    negative = jobs["e2e-released-upgrade-negative"]["if"]
    assert negative == "${{ needs.changes.outputs.released_upgrade == 'true' }}"
    assert NIGHTLY_DISPATCH not in negative


@pytest.mark.parametrize(
    ("job", "flag"),
    [
        ("e2e-released-upgrade-negative", "released_upgrade_full"),
        ("e2e-released-upgrade", "released_upgrade"),
    ],
)
def test_upgrade_job_gates_reject_using_the_other_selection(job: str, flag: str) -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    _assert_upgrade_job_gates(workflow)
    workflow["jobs"][job]["if"] = "${{ needs.changes.outputs." + flag + " == 'true' }}"
    with pytest.raises(AssertionError):
        _assert_upgrade_job_gates(workflow)


def test_workflow_consumes_each_selection_output_exactly() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    jobs = workflow["jobs"]
    assert jobs["changes"]["outputs"] == {
        "skill": "${{ steps.filter.outputs.skill }}",
        "local": "${{ steps.filter.outputs.local }}",
        "local_release": "${{ steps.filter.outputs.local_release }}",
        "cluster": "${{ steps.filter.outputs.cluster }}",
        "chart_scenarios": "${{ steps.filter.outputs.chart_scenarios }}",
        "chart_legs": "${{ steps.filter.outputs.chart_legs }}",
        "released_upgrade": "${{ steps.filter.outputs.released_upgrade }}",
        "released_upgrade_full": ("${{ steps.filter.outputs.released_upgrade_full }}"),
        "skill_local_tiers": "${{ steps.filter.outputs.skill_local_tiers }}",
        "images": "${{ steps.filter.outputs.images }}",
        "cli_release": "${{ steps.filter.outputs.cli_release }}",
        # #3858: python-pytest and rust-select read these two.
        "pytest": "${{ steps.filter.outputs.pytest }}",
        "pytest_deselect": "${{ steps.filter.outputs.pytest_deselect }}",
        "pytest_scope": "${{ steps.filter.outputs.pytest_scope }}",
        "ci_policy": "${{ steps.filter.outputs.ci_policy }}",
        "ci_policy_files": "${{ steps.filter.outputs.ci_policy_files }}",
        "version_only": "${{ steps.filter.outputs.version_only }}",
        "factory": "${{ steps.filter.outputs.factory }}",
        "approval_resume": "${{ steps.filter.outputs.approval_resume }}",
        "runtime_assertions": "${{ steps.runtime.outputs.runtime_assertions }}",
    }

    skill_local = jobs["e2e-ladder"]
    assert skill_local["if"] == (
        "${{ needs.changes.outputs.skill == 'true' || needs.changes.outputs.local == 'true' }}"
    )
    ladder_steps = [
        step for step in skill_local["steps"] if step.get("run") == "bash cli/scripts/e2e-ladder.sh"
    ]
    assert len(ladder_steps) == 1
    assert ladder_steps[0]["env"]["CURIE_E2E_TIERS"] == (
        "${{ needs.changes.outputs.skill_local_tiers }}"
    )

    assert jobs["e2e-ladder-release"]["if"] == (
        "${{ needs.changes.outputs.local_release == 'true' }}"
    )
    assert jobs["e2e-ladder-cluster"]["if"] == ("${{ needs.changes.outputs.cluster == 'true' }}")
    _assert_upgrade_job_gates(workflow)
    assert set(jobs["e2e-released-upgrade-negative"]["needs"]) == set(
        jobs["e2e-released-upgrade"]["needs"]
    )
    assert jobs["e2e-cluster-upgrade-matrix"]["if"] == (
        "${{ needs.changes.outputs.released_upgrade == 'true' }}"
    )
    assert "if" not in jobs["e2e-cluster-upgrade-matrix-shards"]
    assert jobs["images"]["if"] == "${{ needs.changes.outputs.images == 'true' }}"
    assert "worker-local-image" not in jobs
    _assert_image_proofs(workflow)
    assert jobs["cli-portability"]["if"] == (
        "${{ github.event_name != 'pull_request' && needs.changes.outputs.cli_release == 'true' }}"
    )
    assert jobs["cli-darwin"]["if"] == jobs["cli-portability"]["if"]
    assert "changes" in jobs["rust-build"]["needs"]
    assert jobs["eval-falsifiability"]["if"] == ("${{ needs.changes.outputs.skill == 'true' }}")


def _connector_image_contexts() -> set[str]:
    return {
        str(server.parent.relative_to(REPO_ROOT))
        for server in REPO_ROOT.glob("examples/*/connectors/*/server.py")
        if (server.parent / "Dockerfile").is_file()
    }


def _assert_connector_image_import_coverage(job: dict[str, Any]) -> None:
    assert job["name"] == "Build ${{ matrix.name }} image (no push)"
    assert job["runs-on"] == "ubuntu-latest"
    assert job["strategy"]["fail-fast"] is False
    rows = job["strategy"]["matrix"]["include"]
    contexts = _connector_image_contexts()
    assert contexts, "the repository must contain Python connector image contexts"
    matrix_contexts = [row["context"] for row in rows]
    assert len(matrix_contexts) == len(set(matrix_contexts)), "matrix contexts must be unique"
    assert contexts <= set(matrix_contexts), "every connector image context needs a matrix row"
    connectors = [row for row in rows if (REPO_ROOT / row["context"] / "server.py").is_file()]
    assert connectors, "the images matrix must cover Python connector servers"
    steps = job["steps"]
    builds = [
        (index, step)
        for index, step in enumerate(steps)
        if str(step.get("uses", "")).startswith("docker/build-push-action@")
    ]
    assert len(builds) == 2, "connectors need both multiarch and native image builds"
    (multiarch_index, multiarch), (native_index, native) = builds
    imports = [
        (index, step)
        for index, step in enumerate(steps)
        if step.get("name") == "Import the connector server"
    ]
    assert len(imports) == 1, "connectors need an import check in their built image"
    import_index, import_step = imports[0]
    assert multiarch_index < native_index < import_index
    assert "if" not in multiarch
    assert native["uses"] == multiarch["uses"]
    condition = "${{ hashFiles(format('{0}/server.py', matrix.context)) != '' }}"
    assert native.get("if") == condition, "native builds must cover every server.py row"
    assert import_step.get("if") == condition, "imports must cover every server.py row"
    for step in (multiarch, native, import_step):
        assert not step.get("continue-on-error")

    def expand(value: str, row: dict[str, Any]) -> str:
        return re.sub(r"\$\{\{\s*matrix\.(\w+)\s*\}\}", lambda m: row[m[1]], value)

    for row in connectors:
        expected_cache = {
            "cache-from": f"type=gha,scope={row['name']}",
            "cache-to": f"type=gha,mode=max,scope={row['name']}",
        }
        expected_inputs = {
            "context": row["context"],
            "file": row["dockerfile"],
            "push": False,
            **expected_cache,
        }
        assert set(row["platforms"].split(",")) == {"linux/amd64", "linux/arm64"}
        for step, expected in (
            (multiarch, {**expected_inputs, "platforms": row["platforms"], "load": False}),
            (
                native,
                {
                    **expected_inputs,
                    "platforms": "linux/amd64",
                    "load": True,
                    "tags": f"curie-ci-connector/{row['name']}:candidate",
                },
            ),
        ):
            actual = {
                key: expand(value, row) if isinstance(value, str) else value
                for key, value in step["with"].items()
            }
            assert actual == expected, row["name"]
        command = expand(import_step["run"], row).strip()
        assert command == (
            "docker run --rm --entrypoint python "
            f'curie-ci-connector/{row["name"]}:candidate -c "import server"'
        ), row["name"]


def test_connector_image_workflow_imports_every_python_server() -> None:
    job = yaml.safe_load(WORKFLOW.read_text())["jobs"]["images"]
    _assert_connector_image_import_coverage(job)


@pytest.mark.parametrize("context", sorted(_connector_image_contexts()))
def test_connector_image_import_coverage_rejects_deleted_row(context: str) -> None:
    job = yaml.safe_load(WORKFLOW.read_text())["jobs"]["images"]
    _assert_connector_image_import_coverage(job)
    mutated = copy.deepcopy(job)
    rows = mutated["strategy"]["matrix"]["include"]
    rows[:] = [row for row in rows if row["context"] != context]
    with pytest.raises(AssertionError):
        _assert_connector_image_import_coverage(mutated)


@pytest.mark.parametrize(
    "step_name", ["Build the native connector image", "Import the connector server"]
)
def test_connector_image_import_coverage_rejects_deleted_step(step_name: str) -> None:
    job = yaml.safe_load(WORKFLOW.read_text())["jobs"]["images"]
    _assert_connector_image_import_coverage(job)
    mutated = copy.deepcopy(job)
    mutated["steps"] = [step for step in mutated["steps"] if step.get("name") != step_name]
    with pytest.raises(AssertionError):
        _assert_connector_image_import_coverage(mutated)


@pytest.mark.parametrize(
    "step_name", ["Build the native connector image", "Import the connector server"]
)
def test_connector_image_import_coverage_rejects_one_skipped_row(step_name: str) -> None:
    job = yaml.safe_load(WORKFLOW.read_text())["jobs"]["images"]
    _assert_connector_image_import_coverage(job)
    for row in job["strategy"]["matrix"]["include"]:
        if not (REPO_ROOT / row["context"] / "server.py").is_file():
            continue
        mutated = copy.deepcopy(job)
        step = next(step for step in mutated["steps"] if step.get("name") == step_name)
        step["if"] = (
            "${{ hashFiles(format('{0}/server.py', matrix.context)) != '' && "
            f"matrix.name != '{row['name']}' }}}}"
        )
        with pytest.raises(AssertionError):
            _assert_connector_image_import_coverage(mutated)


def test_upgrade_matrix_shards_job_gates_coverage_and_lists_shards() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    job = workflow["jobs"]["e2e-cluster-upgrade-matrix-shards"]
    assert job["outputs"]["shards"] == "${{ steps.list.outputs.shards }}"
    runs = [step["run"] for step in job["steps"] if isinstance(step.get("run"), str)]
    assert any("cli/scripts/cluster-upgrade-matrix.sh --self-test" in run for run in runs)
    list_steps = [step for step in job["steps"] if step.get("id") == "list"]
    assert len(list_steps) == 1
    assert "--list-shards --json" in list_steps[0]["run"]
    assert "$GITHUB_OUTPUT" in list_steps[0]["run"]
    assert job["needs"] == ["changes"]
    assert list_steps[0]["env"]["FULL"] == ("${{ needs.changes.outputs.released_upgrade_full }}")


def _list_shards(tmp_path: Path, full: str, smoke: str | None = None) -> str:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    job = workflow["jobs"]["e2e-cluster-upgrade-matrix-shards"]
    step = next(step for step in job["steps"] if step.get("id") == "list")
    output = tmp_path / f"list-{full}-{smoke}"
    environment = os.environ.copy()
    environment.update(step["env"])
    environment.update({"FULL": full, "GITHUB_OUTPUT": str(output)})
    if smoke is not None:
        environment["SMOKE_SHARD"] = smoke
    completed = subprocess.run(
        ["bash", "--noprofile", "--norc", "-c", step["run"]],
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    return output.read_text()


def _assert_upgrade_smoke_shards(tmp_path: Path, output: dict[str, str]) -> None:
    assert output["released_upgrade"] == "true"
    assert output["released_upgrade_full"] == "false"
    assert _list_shards(tmp_path, output["released_upgrade_full"]) == 'shards=["s01"]\n'


def test_upgrade_matrix_lists_every_shard_on_full_runs_and_one_on_prs(
    tmp_path: Path,
) -> None:
    full = _list_shards(tmp_path, "true")
    shards = json.loads(full.removeprefix("shards="))
    assert shards == [
        "s01",
        "s02",
        "s03",
        "s04",
        "s05",
        "s06",
        "s07",
        "s08",
        "s09",
        "s11",
        "s13",
        "s14",
        "s15",
        "s16",
        "s17",
        "s18",
        "s19",
        "s20",
        "s21",
    ]
    assert _list_shards(tmp_path, "false") == 'shards=["s01"]\n'


def test_upgrade_matrix_smoke_shard_must_be_listed(tmp_path: Path) -> None:
    with pytest.raises(AssertionError):
        _list_shards(tmp_path, "false", smoke="s99")


def test_upgrade_matrix_workflow_runs_one_job_per_shard() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    job = workflow["jobs"]["e2e-cluster-upgrade-matrix"]
    needs = job["needs"]
    if isinstance(needs, str):
        needs = [needs]
    assert set(needs) == {
        "rust-build",
        "changes",
        "e2e-cluster-upgrade-matrix-shards",
        "ci-images",
    }
    assert job["if"] == "${{ needs.changes.outputs.released_upgrade == 'true' }}"
    assert job["timeout-minutes"] == 45
    assert job["strategy"]["fail-fast"] is False
    # 14 shards run in one wave on push and dispatch; 7 took two waves.
    assert job["strategy"]["max-parallel"] == 14
    assert job["strategy"]["matrix"] == {
        "shard": ("${{ fromJSON(needs.e2e-cluster-upgrade-matrix-shards.outputs.shards) }}")
    }
    named_steps = {step["name"]: step for step in job["steps"] if isinstance(step.get("name"), str)}
    run_step = named_steps["Run the cluster upgrade matrix"]
    assert run_step["env"]["CURIE_BIN"] == "cli/target/release/curie"
    assert run_step["env"]["CURIE_E2E_CANDIDATE_TAG"] == "matrix-candidate"
    assert run_step["env"]["SHARD"] == "${{ matrix.shard }}"
    assert "cli/scripts/cluster-upgrade-matrix.sh" in run_step["run"]
    assert '--shard "$SHARD"' in run_step["run"]
    assert "--scenario all" not in run_step["run"]
    assert "${{" not in run_step["run"]
    evidence = named_steps["Upload upgrade matrix evidence"]
    assert evidence["if"] == "always()"
    assert evidence["with"]["name"] == "upgrade-matrix-evidence-${{ matrix.shard }}"
    assert evidence["with"]["path"] == ".projects/2590-evidence"
    teardown = named_steps["Tear down the owned upgrade matrix cluster"]
    assert teardown["if"] == "always()"
    assert "kind delete cluster --name curie-upgrade-matrix" in teardown["run"]
    assert "kind delete cluster --name curie-upgrade " not in teardown["run"]

    # #2733: the script retags matrix-candidate from local docker and
    # kind-loads exclusive tags itself, so the workflow-level kind load of
    # matrix-candidate is gone. The candidate images are built once per run by
    # the ci-images job; a shard only loads and tags them.
    assert not any(
        str(step.get("uses", "")).startswith(
            ("docker/build-push-action@", "docker/bake-action@", "docker/setup-buildx-action@")
        )
        for step in job["steps"]
    )
    assert "Load candidate images into the kind cluster" not in named_steps
    assert not any("kind load" in str(step.get("run", "")) for step in job["steps"])
    load = named_steps["Load the images built by the ci-images job"]
    assert load["uses"] == "./.github/actions/load-ci-images"
    assert load["with"]["images"] == "{api,dispatcher,worker,ui,runner}"
    tag_run = named_steps["Tag the images as the matrix candidate"]["run"]
    for component in ("api", "dispatcher", "worker", "ui", "runner"):
        assert (
            f"docker tag curie-ci/{component}:candidate curie-{component}:matrix-candidate"
        ) in tag_run
    step_names = [step.get("name") for step in job["steps"]]
    assert (
        step_names.index(load["name"])
        < step_names.index("Tag the images as the matrix candidate")
        < step_names.index(run_step["name"])
    )


def test_released_upgrade_workflow_pins_issue_2194_runtime_contract() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    jobs = workflow["jobs"]
    job = workflow["jobs"]["e2e-released-upgrade"]
    assert set(job["needs"]) == {"rust-build", "changes", "ci-images"}

    named_steps = {step["name"]: step for step in job["steps"] if isinstance(step.get("name"), str)}
    assert len(named_steps) == sum("name" in step for step in job["steps"])

    candidate_images = {
        "api": ("apps/api/Dockerfile", "curie-api:upgrade-candidate"),
        "dispatcher": (
            "apps/dispatcher/Dockerfile",
            "curie-dispatcher:upgrade-candidate",
        ),
        "worker": ("apps/worker/Dockerfile", "curie-worker:upgrade-candidate"),
        "ui": ("apps/ui/Dockerfile", "curie-ui:upgrade-candidate"),
        "runner": ("runner/Dockerfile", "curie-runner:upgrade-candidate"),
    }
    load_images = named_steps["Load the images built by the ci-images job"]
    assert load_images["uses"] == "./.github/actions/load-ci-images"
    assert load_images["with"]["images"] == "{api,dispatcher,worker,ui,runner}"
    tag_run = named_steps["Tag the images as the upgrade candidate"]["run"]
    for component, (_dockerfile, tag) in candidate_images.items():
        assert f"docker tag curie-ci/{component}:candidate {tag}" in tag_run

    load_run = named_steps["Load candidate images into the kind cluster"]["run"]
    for _component, (_dockerfile, tag) in candidate_images.items():
        assert f"  {tag} \\" in load_run or f"  {tag}; do" in load_run
    assert 'kind load docker-image "$image" --name curie-upgrade' in load_run

    download_run = named_steps["Download and verify the exact public v0.8.2 chart"]["run"]
    assert (
        "https://github.com/curie-eng/curie/releases/download/v0.8.2/curie-0.8.2.tgz"
    ) in download_run
    assert "sha256sum --check --strict" in download_run
    assert 'test "$(helm show chart "$released_chart"' in download_run
    assert ')" = "0.8.2"' in download_run

    fixture_run = named_steps["Write the legacy retained values fixture"]["run"]
    for exact_line in (
        "  placeholderText: legacy-retained",
        "    appToken: xapp-example-upgrade",
        "    botToken: xoxb-example-upgrade",
        '    mode: "off"',
        "placement: null",
    ):
        assert exact_line in fixture_run

    image_overrides = (
        "--set api.image.repository=curie-api --set api.image.tag=upgrade-candidate "
        "--set api.image.digest= --set api.image.pullPolicy=Never",
        "--set dispatcher.image.repository=curie-dispatcher "
        "--set dispatcher.image.tag=upgrade-candidate "
        "--set dispatcher.image.digest= --set dispatcher.image.pullPolicy=Never",
        "--set worker.image.repository=curie-worker "
        "--set worker.image.tag=upgrade-candidate "
        "--set worker.image.digest= --set worker.image.pullPolicy=Never",
        "--set ui.image.repository=curie-ui --set ui.image.tag=upgrade-candidate "
        "--set ui.image.digest= --set ui.image.pullPolicy=Never",
        "--set agentSandbox.runner.image=curie-runner "
        "--set agentSandbox.runner.tag=upgrade-candidate "
        "--set agentSandbox.runner.digest= "
        "--set agentSandbox.runner.imagePullPolicy=Never",
    )
    runner_prewarm_override = "--set agentSandbox.runner.prewarm.imagePullPolicy=Never"
    retained_projection = (
        "placeholderText: .dispatcher.placeholderText",
        "slackAppToken: .dispatcher.slack.appToken",
        "slackBotToken: .dispatcher.slack.botToken",
        "gvisorMode: .security.gvisor.mode",
        "placement: .placement",
    )

    install_run = named_steps["Install the exact public v0.8.2 release"]["run"]
    assert "helm get values curie -n curie -o json" in install_run
    for projection in retained_projection:
        assert projection in install_run
    for predicate in (
        '.placeholderText == "legacy-retained"',
        '.slackAppToken == "xapp-example-upgrade"',
        '.slackBotToken == "xoxb-example-upgrade"',
        '.gvisorMode == "off"',
        ".placement == null",
    ):
        assert predicate in install_run

    first_upgrade = named_steps["Upgrade the legacy release to the candidate chart"]["run"]
    second_upgrade = named_steps["Upgrade a second time and preserve the generated attester"]["run"]
    for upgrade, snapshot in (
        (first_upgrade, "first-retained-values.json"),
        (second_upgrade, "second-retained-values.json"),
    ):
        assert "helm upgrade curie charts/curie -n curie" in upgrade
        assert "--reset-then-reuse-values" in upgrade
        for override in image_overrides:
            assert override in upgrade
        assert runner_prewarm_override in upgrade
        assert "helm get values curie -n curie -o json" in upgrade
        for projection in retained_projection:
            assert projection in upgrade
        assert snapshot in upgrade
        assert (f'cmp "$RUNNER_TEMP/retained-values.json" "$RUNNER_TEMP/{snapshot}"') in upgrade
        assert "kubectl rollout status deploy/curie-api" in upgrade
        assert "kubectl rollout status deploy/curie-dispatcher" in upgrade

    assert (
        "printf '%s' \"$first_attester\" | sha256sum | cut -d' ' -f1 "
        '> "$RUNNER_TEMP/first-attester.sha256"'
    ) in first_upgrade
    assert ('first_attester="$(cat "$RUNNER_TEMP/first-attester.sha256")"') in second_upgrade
    assert (
        'test "$(printf \'%s\' "$second_attester" | sha256sum | '
        'cut -d\' \' -f1)" = "$first_attester"'
    ) in second_upgrade

    verifier_step = named_steps["Write the managed attester verifier"]
    verifier_run = verifier_step["run"]
    assert 'test -n "$attester"' in verifier_run
    assert 'test "$attester" != "$api_key"' in verifier_run
    verifier_call = '"$RUNNER_TEMP/verify-managed-attester.sh"'
    assert verifier_call in first_upgrade

    assert "Nil unsafe helper negative control" not in named_steps
    negative_job = jobs["e2e-released-upgrade-negative"]
    assert negative_job["name"] == (
        "E2E released chart upgrade negative control (nil unsafe helpers)"
    )
    assert negative_job["runs-on"] == "ubuntu-latest"
    assert negative_job["if"] == "${{ needs.changes.outputs.released_upgrade == 'true' }}"
    assert job["if"] == _nightly_if("needs.changes.outputs.released_upgrade_full == 'true'")
    assert set(negative_job["needs"]) == set(job["needs"])
    negative_steps = {
        step["name"]: step for step in negative_job["steps"] if isinstance(step.get("name"), str)
    }
    assert len(negative_steps) == sum("name" in step for step in negative_job["steps"])
    for shared in (
        "Install Helm",
        "Install Calico so NetworkPolicy is enforced",
        "Download and verify the exact public v0.8.2 chart",
        "Write the legacy retained values fixture",
        "Write the managed attester verifier",
    ):
        assert negative_steps[shared] == named_steps[shared], shared
    negative_load = negative_steps["Load the worker image built by the ci-images job"]
    assert negative_load["uses"] == "./.github/actions/load-ci-images"
    assert negative_load["with"]["images"] == "worker"
    assert negative_steps["Tag the worker image as the upgrade candidate"]["run"] == (
        "docker tag curie-ci/worker:candidate curie-worker:upgrade-candidate"
    )
    assert negative_job["steps"][0] == job["steps"][0]
    assert (
        "kind load docker-image curie-worker:upgrade-candidate"
        in negative_steps["Load the candidate worker image into the kind cluster"]["run"]
    )
    negative_names = [step.get("name") for step in negative_job["steps"]]
    assert negative_names.index("Write the managed attester verifier") < (
        negative_names.index("Nil unsafe helper negative control")
    )
    assert negative_steps["Tear down the disposable negative control cluster"]["if"] == "always()"
    assert negative_steps["Dump negative control diagnostics on failure"]["if"] == ("failure()")

    negative_run = negative_steps["Nil unsafe helper negative control"]["run"]
    placement_mutant_setup = '''placement_mutant="$RUNNER_TEMP/nil-unsafe-placement-chart"
cp -a charts/curie "$placement_mutant"'''
    assert placement_mutant_setup in negative_run
    assert "python3 charts/curie/ci/make-upgrade-mutants.py" in negative_run
    mutant_builder = UPGRADE_MUTANT_BUILDER.read_text()
    assert "match = block_re.search(text)" in mutant_builder
    assert 'if match is None or "| default dict" not in match.group(0):' in mutant_builder
    assert ('mutated = match.group(0).replace("| default dict", "", 1)') in mutant_builder
    assert "start_marker = '{{- define \"curie.managedSecret\" -}}'" in mutant_builder
    placement_upgrade = """if helm upgrade curie-negative "$placement_mutant" -n curie-negative \\
    --reset-then-reuse-values --timeout 15m; then
  echo "nil-unsafe placement mutant unexpectedly upgraded legacy placement:null values" >&2
  exit 1
fi"""
    assert placement_upgrade in negative_run
    placement_rejection = (
        'echo "Released-upgrade rung rejected the nil-unsafe placement mutant as expected"'
    )
    assert placement_rejection in negative_run
    managed_secret_mutation = (
        "kubectl patch secret curie-negative-secrets -n curie-negative --type merge"
    )
    assert negative_run.index(placement_upgrade) < negative_run.index(placement_rejection)
    assert negative_run.index(placement_rejection) < negative_run.index(managed_secret_mutation)
    assert verifier_call in negative_run
    assert f"if {verifier_call}" in negative_run
    assert 'test -z "$negative_attester"' in negative_run
    assert "unexpectedly passed" in negative_run

    health_run = named_steps["Require healthy API after the candidate upgrade"]["run"]
    assert "curl -fsS http://127.0.0.1:28000/health" in health_run
    assert 'grep -q \'"status":"ok"\'' in health_run

    smoke = named_steps["Existing cluster rung smoke on the upgraded candidate"]
    assert smoke["env"]["CURIE_E2E_TIERS"] == "cluster"
    assert "bash cli/scripts/e2e-ladder.sh" in smoke["run"]
    for override in image_overrides:
        assert override in smoke["run"]
    assert runner_prewarm_override in smoke["run"]

    existing_cluster_steps = jobs["e2e-ladder-cluster"]["steps"]
    existing_smoke = [
        step
        for step in existing_cluster_steps
        if step.get("run") == "bash cli/scripts/e2e-ladder.sh"
    ]
    assert len(existing_smoke) == 1
    assert existing_smoke[0]["env"]["CURIE_E2E_TIERS"] == "cluster"

    assert named_steps["Tear down the disposable upgrade cluster"]["if"] == "always()"


def test_upgrade_mutants_change_only_their_target_helpers(tmp_path: Path) -> None:
    """The managed secret mutant must keep every later helper (#4466 added
    curie.secretChecksum after it), or the candidate fails to render and the
    negative control passes for the wrong reason."""
    helpers = REPO_ROOT / CHART_ROOT / "templates" / "_helpers.tpl"
    placement = tmp_path / "placement.tpl"
    managed = tmp_path / "managed.tpl"
    placement.write_text(helpers.read_text())
    managed.write_text(helpers.read_text())
    subprocess.run(
        [sys.executable, str(UPGRADE_MUTANT_BUILDER), str(placement), str(managed)],
        check=True,
    )
    defines = re.compile(r'{{-?\s*define\s+"([^"]+)"')
    original = defines.findall(helpers.read_text())
    assert "curie.secretChecksum" in original
    assert defines.findall(placement.read_text()) == original
    assert defines.findall(managed.read_text()) == original
    assert managed.read_text() != helpers.read_text()
    assert "{{- else if hasKey .existingData .key -}}" in managed.read_text()
    assert placement.read_text() != helpers.read_text()


def test_released_upgrade_candidate_images_opt_into_forward_only_migrations() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    steps = workflow["jobs"]["e2e-released-upgrade"]["steps"]
    named_steps = {step["name"]: step for step in steps if isinstance(step.get("name"), str)}

    candidate_upgrade_steps = (
        "Upgrade the legacy release to the candidate chart",
        "Upgrade a second time and preserve the generated attester",
        "Existing cluster rung smoke on the upgraded candidate",
    )
    for name in candidate_upgrade_steps:
        run = named_steps[name]["run"]
        assert "helm upgrade curie charts/curie -n curie" in run
        assert "--set api.image.tag=upgrade-candidate" in run
        assert "--set api.migrate.forwardOnly=true" in run

    migrated_upgrade = named_steps["Upgrade the v0.8.4 release to the candidate chart"]["run"]
    assert "helm upgrade curie charts/curie -n curie" in migrated_upgrade
    assert "api.migrate.forwardOnly=true" in migrated_upgrade
    assert "--set api.image.tag=upgrade-candidate" in migrated_upgrade


def test_released_upgrade_workflow_pins_issue_2097_live_manifest_parity() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    jobs = workflow["jobs"]
    job = jobs["e2e-released-upgrade"]
    named_steps = {step["name"]: step for step in job["steps"] if isinstance(step.get("name"), str)}

    download_run = named_steps["Download and verify the exact public v0.8.4 chart"]["run"]
    assert (
        "https://github.com/curie-eng/curie/releases/download/v0.8.4/curie-0.8.4.tgz"
    ) in download_run
    assert ("fee20ab73c05d7a888165f980fb82d25150fcba509f19218bafc4c187a9044bb") in download_run
    assert "sha256sum --check --strict" in download_run
    assert 'test "$(helm show chart "$released_chart_084"' in download_run
    assert ')" = "0.8.4"' in download_run

    fixture_run = named_steps["Write the v0.8.4 retained timeout values fixture"]["run"]
    for exact_line in (
        "worker:",
        "  extraEnv:",
        "    - name: CURIE_RUNNER_TOTAL_TIMEOUT_S",
        '      value: "1700"',
        '    mode: "off"',
    ):
        assert exact_line in fixture_run

    published_image_pins = (
        "--set api.image.tag=0.8.4 --set api.image.digest="
        "sha256:8804a35d0c96e9bb2ed0d4f6a990015aab9e9343f6c1ae0b5dc7307e37b50aaf",
        "--set dispatcher.image.tag=0.8.4 --set dispatcher.image.digest="
        "sha256:ab6766db1f7d211e86f6bd816bb5c73ebfe6ee6cbcdf3dc9de9f9a26848bef47",
        "--set worker.image.tag=0.8.4 --set worker.image.digest="
        "sha256:c3117c30ac0a4cdd626c1170a8c976911da601f55781c9ffea6f1d85d4f02656",
        "--set ui.image.tag=0.8.4 --set ui.image.digest="
        "sha256:7e22c984e53478df9d70e96ec1fd3c73601396a23b378f7b9483ef8bf498477b",
        "--set agentSandbox.runner.tag=0.8.4 --set agentSandbox.runner.digest="
        "sha256:4e19b285d4161d4667145ce9ea6e3efd11c1a9e22b1f1f2f49167994515b0b88",
    )
    install_run = named_steps["Install the exact public v0.8.4 release"]["run"]
    assert "helm install" in install_run
    for pin in published_image_pins:
        assert pin in install_run
    assert "$RELEASED_V084_VALUES" in install_run

    upgrade_run = named_steps["Upgrade the v0.8.4 release to the candidate chart"]["run"]
    assert "kubectl delete deploy/curie-worker -n curie" in upgrade_run
    assert (
        'helm get values curie -n curie -o json > "$RUNNER_TEMP/v084-retained-values.json"'
    ) in upgrade_run
    assert "jq '" in upgrade_run
    assert "(reduce $extra[] as $item (" in upgrade_run
    assert "($partition.timeouts[0].value | tonumber) as $timeout" in upgrade_run
    assert ".worker.runnerTotalTimeoutSeconds = $timeout" in upgrade_run
    assert ".worker.extraEnv = $partition.kept" in upgrade_run
    assert '.config.schemaVersion = "0.9.0"' in upgrade_run
    assert '.config.migratedFrom = "0.8.4"' in upgrade_run
    assert (
        'helm upgrade curie charts/curie -n curie -f "$RUNNER_TEMP/v084-migrated-values.json"'
    ) in upgrade_run
    assert "--set api.migrate.forwardOnly=true" in upgrade_run
    assert "--reset-then-reuse-values" not in upgrade_run
    assert "--set worker.deliveryBudgetSeconds=1800" in upgrade_run
    assert "--set worker.terminationGracePeriodSeconds=1860" in upgrade_run
    assert (
        "--set worker.image.repository=curie-worker --set worker.image.tag=upgrade-candidate"
    ) in upgrade_run
    assert (
        'helm get values curie -n curie -o json > "$RUNNER_TEMP/v084-effective-values.json"'
    ) in upgrade_run
    for migration_assertion in (
        ".worker.runnerTotalTimeoutSeconds == 1700",
        ".worker.deliveryBudgetSeconds == 1800",
        '.name == "CURIE_RUNNER_TOTAL_TIMEOUT_S"',
        '.config.schemaVersion == "0.9.0"',
        '.config.migratedFrom == "0.8.4"',
    ):
        assert migration_assertion in upgrade_run

    parity_run = named_steps["Verify live-manifest parity and target-version convergence"]["run"]
    assert "charts/curie/ci/live_manifest_parity.py" in parity_run
    assert "helm get manifest" in parity_run
    assert "kubectl get deploy" in parity_run
    assert "helm get metadata" in parity_run
    assert "charts/curie/Chart.yaml" in parity_run
    assert "CURIE_RUNNER_TOTAL_TIMEOUT_S" in parity_run

    smoke = named_steps["Existing cluster rung smoke on the upgraded candidate"]
    assert smoke["env"]["CURIE_E2E_TIERS"] == "cluster"
    assert "bash cli/scripts/e2e-ladder.sh" in smoke["run"]
    install_names = [step.get("name") for step in job["steps"]]
    assert install_names.index("Install the exact public v0.8.4 release") < (
        install_names.index("Existing cluster rung smoke on the upgraded candidate")
    )
    negative_job = jobs["e2e-released-upgrade-negative"]
    assert "Nil unsafe helper negative control" in {
        step.get("name") for step in negative_job["steps"]
    }

    helm_ci = yaml.safe_load((REPO_ROOT / ".github" / "workflows" / "helm-ci.yaml").read_text())
    helm_runs = "\n".join(
        step.get("run", "")
        for job in helm_ci["jobs"].values()
        for step in job.get("steps", [])
        if isinstance(step.get("run"), str)
    )
    assert "charts/curie/ci/live-manifest-parity-assertions.sh" in helm_runs


def _aggregate_contract() -> tuple[str, dict[str, str]]:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    job = workflow["jobs"]["e2e-required"]
    assert job["name"] == "E2E required"
    assert set(job["needs"]) == {
        "changes",
        "runtime-assertion-registration",
        "e2e-ladder",
        "e2e-oidc",
        "e2e-ladder-release",
        "e2e-ladder-cluster",
        "e2e-cluster-chart-regressions",
        "e2e-cluster-rollout-recovery",
        "e2e-cluster-approval-resume-restarts",
        "e2e-scripted-factory",
        "forge-instance-regression",
        "factory-admitted-agent",
        "e2e-released-upgrade",
        "e2e-released-upgrade-negative",
        "e2e-cluster-upgrade-matrix-shards",
        "e2e-cluster-upgrade-matrix",
    }
    assert job["if"] == "${{ !cancelled() }}"

    candidates = [
        step
        for step in job["steps"]
        if isinstance(step, dict)
        and isinstance(step.get("run"), str)
        and isinstance(step.get("env"), dict)
        and AGGREGATE_EXPRESSIONS["changes_result"] in step["env"].values()
    ]
    assert len(candidates) == 1
    step = candidates[0]
    bindings: dict[str, str] = {}
    for semantic_name, expression in AGGREGATE_EXPRESSIONS.items():
        environment_names = [name for name, value in step["env"].items() if value == expression]
        assert len(environment_names) == 1, expression
        bindings[semantic_name] = environment_names[0]
    return step["run"], bindings


class _AggregateOverrides(TypedDict, total=False):
    changes_result: str
    registration_result: str
    event_name: str
    nightly: str
    skill_selected: str
    local_selected: str
    local_release_selected: str
    cluster_selected: str
    chart_scenarios: str
    released_upgrade_selected: str
    released_upgrade_full: str
    approval_resume_selected: str
    factory_selected: str
    skill_local_result: str
    oidc_result: str
    local_release_result: str
    cluster_result: str
    cluster_chart_result: str
    rollout_recovery_result: str
    approval_resume_result: str
    factory_result: str
    forge_instance_result: str
    admitted_agent_result: str
    released_upgrade_result: str
    released_upgrade_negative_result: str
    upgrade_matrix_shards_result: str
    upgrade_matrix_result: str


def _run_aggregate(
    *,
    script_transform: Callable[[str], str] | None = None,
    **overrides: str,
) -> subprocess.CompletedProcess[str]:
    script, bindings = _aggregate_contract()
    if script_transform is not None:
        script = script_transform(script)
    state = {
        "changes_result": "success",
        "registration_result": "success",
        "event_name": "push",
        "nightly": "false",
        "skill_selected": "false",
        "local_selected": "false",
        "local_release_selected": "false",
        "cluster_selected": "false",
        "chart_scenarios": "[]",
        "released_upgrade_selected": "false",
        "released_upgrade_full": "false",
        "approval_resume_selected": "false",
        "factory_selected": "false",
        "skill_local_result": "skipped",
        "oidc_result": "skipped",
        "local_release_result": "skipped",
        "cluster_result": "skipped",
        "cluster_chart_result": "skipped",
        "rollout_recovery_result": "skipped",
        "approval_resume_result": "skipped",
        "factory_result": "skipped",
        "released_upgrade_result": "skipped",
        "released_upgrade_negative_result": "skipped",
        "upgrade_matrix_shards_result": "success",
        "upgrade_matrix_result": "skipped",
    }
    state.update(overrides)
    # Admitted agent stays on the scripted factory gate. Forge runs only on a
    # nightly dispatch, so a test that does not name it gets a skip unless
    # that dispatch selected factory.
    for name in FACTORY_SIBLING_RESULTS:
        if name not in overrides:
            state[name] = state["factory_result"]
    if "forge_instance_result" not in overrides:
        nightly = (
            state["event_name"] == "workflow_dispatch" and state["nightly"] == "true"
        )
        if nightly and state["factory_selected"] == "true":
            state["forge_instance_result"] = state["factory_result"]
        else:
            state["forge_instance_result"] = "skipped"
    if "chart_scenarios" not in overrides and state["cluster_selected"] == "true":
        state["chart_scenarios"] = '["workspace-diagnostic"]'
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


@pytest.mark.parametrize("value", ["failure", "skipped", "cancelled"])
def test_e2e_required_fails_unless_runtime_assertion_registration_succeeds(value: str) -> None:
    passing = _run_aggregate()
    assert passing.returncode == 0, passing.stdout + passing.stderr

    blocked = _run_aggregate(registration_result=value)
    assert blocked.returncode != 0, blocked.stdout + blocked.stderr


def test_e2e_required_validates_docs_only_ladder_skips(tmp_path: Path) -> None:
    selected, output = _invoke_selector(tmp_path, "ARCHITECTURE.md")
    assert selected.returncode == 0, selected.stderr
    assert output == _expected_output(
        pytest_needed=False,
        ci_policy_files=_expected_ci_policy_files("ARCHITECTURE.md"),
    )
    outputs = dict(line.split("=", maxsplit=1) for line in output.splitlines())

    skipped = _run_aggregate(
        skill_selected=outputs["skill"],
        local_selected=outputs["local"],
        local_release_selected=outputs["local_release"],
        cluster_selected=outputs["cluster"],
        released_upgrade_selected=outputs["released_upgrade"],
        factory_selected=outputs["factory"],
        upgrade_matrix_result="skipped",
    )
    assert skipped.returncode == 0, skipped.stdout + skipped.stderr

    unexpected_result = _run_aggregate(
        skill_selected=outputs["skill"],
        local_selected=outputs["local"],
        local_release_selected=outputs["local_release"],
        cluster_selected=outputs["cluster"],
        released_upgrade_selected=outputs["released_upgrade"],
        factory_selected=outputs["factory"],
        skill_local_result="success",
    )
    assert unexpected_result.returncode != 0


def test_e2e_required_accepts_a_version_only_selection(tmp_path: Path) -> None:
    """#3858: the required E2E gate passes on the local-release rung alone."""
    selected, output = _invoke_selector(tmp_path, *_version_only_paths(_repo_version()))
    assert selected.returncode == 0, selected.stderr
    outputs = _outputs_of(output)
    assert outputs["version_only"] == "true"
    selection: _AggregateOverrides = {
        "event_name": "pull_request",
        "skill_selected": outputs["skill"],
        "local_selected": outputs["local"],
        "local_release_selected": outputs["local_release"],
        "cluster_selected": outputs["cluster"],
        "released_upgrade_selected": outputs["released_upgrade"],
        "released_upgrade_full": outputs["released_upgrade_full"],
        "factory_selected": outputs["factory"],
    }

    ran = _run_aggregate(**selection, local_release_result="success")
    assert ran.returncode == 0, ran.stdout + ran.stderr

    missing = _run_aggregate(**selection)
    assert missing.returncode != 0


@pytest.mark.parametrize(
    "state",
    [
        {},
        {"skill_selected": "true", "skill_local_result": "success"},
        {"local_selected": "true", "skill_local_result": "success", "oidc_result": "success"},
        {"factory_selected": "true", "factory_result": "success"},
        {
            "skill_selected": "true",
            "local_selected": "true",
            "local_release_selected": "true",
            "cluster_selected": "true",
            "skill_local_result": "success",
            "oidc_result": "success",
            "local_release_result": "success",
            "cluster_result": "success",
            "cluster_chart_result": "success",
            "rollout_recovery_result": "skipped",
        },
        {
            "event_name": "pull_request",
            "cluster_selected": "true",
            "cluster_result": "success",
            "cluster_chart_result": "success",
            "rollout_recovery_result": "skipped",
        },
        {
            "event_name": "workflow_dispatch",
            "nightly": "true",
            "cluster_selected": "true",
            "cluster_result": "success",
            "cluster_chart_result": "success",
            "rollout_recovery_result": "success",
        },
        {
            "event_name": "workflow_dispatch",
            "nightly": "true",
            "released_upgrade_selected": "true",
            "released_upgrade_full": "true",
            "released_upgrade_result": "success",
            "released_upgrade_negative_result": "success",
            "upgrade_matrix_result": "success",
        },
    ],
)
def test_aggregate_accepts_exact_selected_outcomes(state: _AggregateOverrides) -> None:
    completed = _run_aggregate(**state)
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_aggregate_requires_upgrade_matrix_when_released_upgrade_is_selected() -> None:
    ok = _run_aggregate(
        event_name="workflow_dispatch",
        nightly="true",
        released_upgrade_selected="true",
        released_upgrade_full="true",
        released_upgrade_result="success",
        released_upgrade_negative_result="success",
        upgrade_matrix_result="success",
    )
    assert ok.returncode == 0, ok.stdout + ok.stderr
    for result in ("skipped", "failure", "cancelled"):
        rejected = _run_aggregate(
            event_name="workflow_dispatch",
            nightly="true",
            released_upgrade_selected="true",
            released_upgrade_full="true",
            released_upgrade_result="success",
            released_upgrade_negative_result="success",
            upgrade_matrix_result=result,
        )
        assert rejected.returncode != 0, result


def test_aggregate_requires_upgrade_matrix_skip_when_released_upgrade_is_not_selected() -> None:
    ok = _run_aggregate(upgrade_matrix_result="skipped")
    assert ok.returncode == 0, ok.stdout + ok.stderr
    for result in ("success", "failure", "cancelled"):
        rejected = _run_aggregate(upgrade_matrix_result=result)
        assert rejected.returncode != 0, result


@pytest.mark.parametrize(
    ("result_name", "result"),
    [
        (name, result)
        for name, results in (
            ("upgrade_matrix_result", ("skipped", "failure", "cancelled")),
            ("released_upgrade_negative_result", ("skipped", "failure", "cancelled")),
            ("released_upgrade_result", ("success", "failure", "cancelled")),
        )
        for result in results
    ],
)
def test_aggregate_pull_request_smoke_runs_matrix_and_negative_only(
    result_name: str, result: str
) -> None:
    state = {
        "event_name": "pull_request",
        "released_upgrade_selected": "true",
        "released_upgrade_full": "false",
        "released_upgrade_result": "skipped",
        "released_upgrade_negative_result": "success",
        "upgrade_matrix_result": "success",
    }
    ok = _run_aggregate(
        event_name=state["event_name"],
        released_upgrade_selected=state["released_upgrade_selected"],
        released_upgrade_full=state["released_upgrade_full"],
        released_upgrade_result=state["released_upgrade_result"],
        released_upgrade_negative_result=state["released_upgrade_negative_result"],
        upgrade_matrix_result=state["upgrade_matrix_result"],
    )
    assert ok.returncode == 0, ok.stdout + ok.stderr
    state[result_name] = result
    rejected = _run_aggregate(
        event_name=state["event_name"],
        released_upgrade_selected=state["released_upgrade_selected"],
        released_upgrade_full=state["released_upgrade_full"],
        released_upgrade_result=state["released_upgrade_result"],
        released_upgrade_negative_result=state["released_upgrade_negative_result"],
        upgrade_matrix_result=state["upgrade_matrix_result"],
    )
    assert rejected.returncode != 0, (result_name, result)


@pytest.mark.parametrize(
    "result_name", ("released_upgrade_result", "released_upgrade_negative_result")
)
@pytest.mark.parametrize("result", ("success", "failure", "cancelled"))
def test_aggregate_requires_released_jobs_skip_when_unselected(
    result_name: str, result: str
) -> None:
    ok = _run_aggregate()
    assert ok.returncode == 0, ok.stdout + ok.stderr
    state = {
        "released_upgrade_result": "skipped",
        "released_upgrade_negative_result": "skipped",
    }
    state[result_name] = result
    rejected = _run_aggregate(
        released_upgrade_result=state["released_upgrade_result"],
        released_upgrade_negative_result=state["released_upgrade_negative_result"],
    )
    assert rejected.returncode != 0, (result_name, result)


@pytest.mark.parametrize("mutation", ("omit-negative", "full-only-negative"))
def test_aggregate_rejects_missing_or_full_only_negative_expectation(mutation: str) -> None:
    state = {
        "event_name": "pull_request",
        "released_upgrade_selected": "true",
        "released_upgrade_full": "false",
        "released_upgrade_negative_result": "success",
        "upgrade_matrix_result": "success",
    }
    unmutated = _run_aggregate(
        event_name=state["event_name"],
        released_upgrade_selected=state["released_upgrade_selected"],
        released_upgrade_full=state["released_upgrade_full"],
        released_upgrade_negative_result=state["released_upgrade_negative_result"],
        upgrade_matrix_result=state["upgrade_matrix_result"],
    )
    assert unmutated.returncode == 0, unmutated.stdout + unmutated.stderr
    check = '"$RELEASED_UPGRADE_NEGATIVE_RESULT" != "$released_upgrade_negative_expected" ||'

    def change_negative(script: str) -> str:
        assert script.count(check) == 1
        replacement = (
            "" if mutation == "omit-negative"
            else check.replace("$released_upgrade_negative_expected", "$released_upgrade_expected")
        )
        return script.replace(check, replacement, 1)

    rejected = _run_aggregate(
        script_transform=change_negative,
        event_name=state["event_name"],
        released_upgrade_selected=state["released_upgrade_selected"],
        released_upgrade_full=state["released_upgrade_full"],
        released_upgrade_negative_result=state["released_upgrade_negative_result"],
        upgrade_matrix_result=state["upgrade_matrix_result"],
    )
    output = rejected.stdout + rejected.stderr
    assert rejected.returncode != 0, output
    if mutation == "omit-negative":
        assert (
            "Selected and skipped negative control failed for RELEASED_UPGRADE_NEGATIVE_RESULT"
            in output
        )
    else:
        assert "E2E required failed" in output


@pytest.mark.parametrize("full", ["", "yes"])
def test_aggregate_rejects_malformed_released_upgrade_full(full: str) -> None:
    completed = _run_aggregate(released_upgrade_full=full)
    assert completed.returncode != 0


def test_aggregate_rejects_full_run_without_released_upgrade() -> None:
    completed = _run_aggregate(
        released_upgrade_full="true",
        released_upgrade_result="success",
        released_upgrade_negative_result="success",
    )
    assert completed.returncode != 0


@pytest.mark.parametrize("result", ["skipped", "failure", "cancelled"])
def test_aggregate_requires_upgrade_matrix_shards_success(result: str) -> None:
    completed = _run_aggregate(upgrade_matrix_shards_result=result)
    assert completed.returncode != 0


@pytest.mark.parametrize(
    "state",
    [
        {"changes_result": "failure"},
        {"skill_selected": "true", "skill_local_result": "skipped"},
        {"local_selected": "true", "skill_local_result": "cancelled"},
        {"local_selected": "true", "skill_local_result": "success", "oidc_result": "failure"},
        {"local_selected": "true", "skill_local_result": "success", "oidc_result": "skipped"},
        {"skill_selected": "true", "skill_local_result": "success", "oidc_result": "success"},
        {"local_release_selected": "true", "local_release_result": "failure"},
        {"cluster_selected": "true", "cluster_result": "skipped"},
        {"cluster_selected": "true", "cluster_result": "cancelled"},
        {
            "cluster_selected": "true",
            "cluster_result": "success",
            "cluster_chart_result": "failure",
        },
        {
            "cluster_selected": "true",
            "cluster_result": "success",
            "cluster_chart_result": "skipped",
        },
        {
            "cluster_selected": "true",
            "cluster_result": "failure",
            "cluster_chart_result": "success",
        },
        {"cluster_chart_result": "success"},
        {"skill_local_result": "success"},
        {"local_release_result": "success"},
        {"cluster_result": "success"},
        {"upgrade_matrix_result": "success"},
        {"upgrade_matrix_result": "failure"},
        {"released_upgrade_negative_result": "success"},
        {
            "released_upgrade_selected": "true",
            "released_upgrade_full": "true",
            "released_upgrade_result": "success",
            "released_upgrade_negative_result": "skipped",
        },
        {
            "released_upgrade_selected": "true",
            "released_upgrade_full": "true",
            "released_upgrade_result": "success",
            "released_upgrade_negative_result": "failure",
        },
        {
            "released_upgrade_selected": "true",
            "released_upgrade_full": "true",
            "released_upgrade_result": "skipped",
            "released_upgrade_negative_result": "success",
        },
    ],
)
def test_aggregate_rejects_inconsistent_outcomes(state: _AggregateOverrides) -> None:
    completed = _run_aggregate(**state)
    assert completed.returncode != 0


@pytest.mark.parametrize("result", ["failure", "skipped", "cancelled"])
def test_aggregate_requires_rollout_recovery_success_when_nightly_selected(
    result: str,
) -> None:
    completed = _run_aggregate(
        event_name="workflow_dispatch",
        nightly="true",
        cluster_selected="true",
        cluster_result="success",
        cluster_chart_result="success",
        rollout_recovery_result=result,
    )
    assert completed.returncode != 0, completed.stdout + completed.stderr


@pytest.mark.parametrize("result", ["success", "failure"])
def test_aggregate_rollout_recovery_must_skip_when_not_expected(result: str) -> None:
    on_pull_request = _run_aggregate(
        event_name="pull_request",
        cluster_selected="true",
        cluster_result="success",
        cluster_chart_result="success",
        rollout_recovery_result=result,
    )
    assert on_pull_request.returncode != 0
    unselected = _run_aggregate(
        event_name="push",
        cluster_selected="false",
        rollout_recovery_result=result,
    )
    assert unselected.returncode != 0
    plain_dispatch = _run_aggregate(
        event_name="workflow_dispatch",
        nightly="false",
        cluster_selected="true",
        cluster_result="success",
        cluster_chart_result="success",
        rollout_recovery_result=result,
    )
    assert plain_dispatch.returncode != 0


def test_negative_control_covers_rollout_recovery_result() -> None:
    state: _AggregateOverrides = {
        "event_name": "workflow_dispatch",
        "nightly": "true",
        "cluster_selected": "true",
        "cluster_result": "success",
        "cluster_chart_result": "success",
        "rollout_recovery_result": "success",
    }
    unmutated = _run_aggregate(**state)
    assert unmutated.returncode == 0, unmutated.stdout + unmutated.stderr

    rollout_result_check = '"$ROLLOUT_RECOVERY_RESULT" != "$rollout_recovery_expected" ||'

    def accept_rollout_drift(script: str) -> str:
        assert script.count(rollout_result_check) == 1
        return script.replace(rollout_result_check, "", 1)

    completed = _run_aggregate(script_transform=accept_rollout_drift, **state)
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, output
    assert "Selected and skipped negative control failed" in output


def test_aggregate_negative_control_runs_before_real_results() -> None:
    completed = _run_aggregate()
    output = completed.stdout + completed.stderr
    assert completed.returncode == 0, output
    negative_control = "Selected and skipped negative control passed"
    real_result = "E2E required passed"
    assert negative_control in output
    assert real_result in output
    assert output.index(negative_control) < output.index(real_result)


def test_negative_control_rejects_selected_skipped_outcome_when_validator_mutates() -> None:
    unmutated = _run_aggregate(
        skill_selected="true",
        skill_local_result="success",
    )
    assert unmutated.returncode == 0, unmutated.stdout + unmutated.stderr

    skill_result_check = '"$SKILL_LOCAL_RESULT" != "$skill_local_expected" ||'

    def accept_selected_skipped(script: str) -> str:
        assert script.count(skill_result_check) == 1
        return script.replace(skill_result_check, "", 1)

    completed = _run_aggregate(
        script_transform=accept_selected_skipped,
        skill_selected="true",
        skill_local_result="skipped",
    )
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, output
    assert "Selected and skipped negative control failed" in output


def test_selector_directory_has_no_stdlib_shadowing_modules() -> None:
    # Structural, not behavioral: on Linux `select` is a builtin module (its
    # __file__ is None), so a directly-executed script's own directory on
    # sys.path never wins and the shadow bug cannot be reproduced here. On
    # macOS `select` is a dynamic extension in lib-dynload, which loses to
    # sys.path[0] and crashes. See issue #1878.
    selector_dir = SELECTOR.parent
    for candidate in selector_dir.glob("*.py"):
        assert candidate.stem not in sys.stdlib_module_names, (
            f"{candidate.name} shadows the stdlib module '{candidate.stem}': "
            "Python puts a directly-executed script's own directory first on "
            "sys.path, so this basename shadows the stdlib module for every "
            "import in this process. That's harmless on platforms where the "
            "shadowed module is a builtin (e.g. Linux's `select`), but breaks "
            "the script on platforms where it's a dynamic extension instead "
            "(e.g. macOS's `select`), per issue #1878."
        )


def test_omit_kind_keeps_cluster_for_a_changed_runtime_assertion(tmp_path: Path) -> None:
    # #3391: E2E required refuses a changed runtime assertion without a pass
    # receipt from the cluster rung, so omitting kind must not drop that rung.
    runtime = "charts/curie/ci/runtime/connector-readiness-runtime.sh"
    outputs = _selector_outputs_omit_kind(tmp_path, runtime)
    assert outputs["cluster"] == "true"
    assert outputs["released_upgrade"] == "false"
    # Negative control: a sibling chart CI file is still dropped.
    other = _selector_outputs_omit_kind(tmp_path, "charts/curie/ci/runtime/README.md")
    assert other["cluster"] == "false"


def _selector_outputs_omit_kind(tmp_path: Path, path: str) -> dict[str, str]:
    completed, output = _invoke_selector(tmp_path, path, omit_kind=True)
    assert completed.returncode == 0, completed.stderr
    return dict(line.split("=", maxsplit=1) for line in output.splitlines())


CLUSTER_RUNG_MOVED_PROOFS = {
    "Langfuse web waits for delayed Postgres without restarting": "e2e-cluster-chart-regressions",
    "Connector that never listens stays not-Ready until it binds": "e2e-cluster-chart-regressions",
    "Runner BYO egress enforces (not just rendered)": "e2e-cluster-chart-regressions",
    "Rollout-free first invocation and dead-consumer recovery": "e2e-cluster-rollout-recovery",
}


def test_single_regression_proofs_run_outside_the_cluster_rung() -> None:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    rung_steps = {step.get("name") for step in jobs["e2e-ladder-cluster"]["steps"]}
    for name, job_id in CLUSTER_RUNG_MOVED_PROOFS.items():
        assert name not in rung_steps, name
        steps = [step for step in jobs[job_id]["steps"] if step.get("name") == name]
        assert len(steps) == 1, (job_id, name)
        if job_id == CHART_JOB:
            scenario = next(
                scenario for scenario, entry in CHART_SCENARIOS.items() if entry["step"] == name
            )
            assert steps[0]["if"] == _chart_step_if(scenario)
        else:
            assert "if" not in steps[0]
        assert "continue-on-error" not in steps[0]

    chart = jobs["e2e-cluster-chart-regressions"]
    assert chart["if"] == CHART_JOB_IF
    assert chart["needs"] == ["rust-build", "changes", "ci-images"]
    assert "e2e-cluster-chart-regressions" in jobs["e2e-required"]["needs"]

    rollout = jobs["e2e-cluster-rollout-recovery"]
    assert rollout["if"] == _nightly_if("needs.changes.outputs.cluster == 'true'")
    assert "e2e-cluster-rollout-recovery" in jobs["e2e-required"]["needs"]


WORKLOAD_STAGING_IF = (
    "${{ matrix.leg == 'workload' && "
    "(contains(fromJSON(needs.changes.outputs.chart_scenarios), 'workspace-diagnostic') "
    "|| contains(fromJSON(needs.changes.outputs.chart_scenarios), 'quota-progress') "
    "|| contains(fromJSON(needs.changes.outputs.chart_scenarios), 'failed-first-install-retry')) }}"
)


def test_workspace_diagnostic_reuses_the_chart_cluster_and_candidate_artifacts() -> None:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    chart = jobs["e2e-cluster-chart-regressions"]
    assert chart["needs"] == ["rust-build", "changes", "ci-images"]
    steps = chart["steps"]
    workspace = next(
        step
        for step in steps
        if step.get("name") == "Workspace dependency failure reaches the terminal diagnostic"
    )
    assert workspace["env"] == {
        "CURIE_BIN": "${{ github.workspace }}/cli/target/release/curie",
        "CURIE_SCENARIO_CONTEXT": "kind-curie-e2e",
        "CURIE_SCENARIO_RUN_ID": "${{ github.run_id }}-${{ github.run_attempt }}",
        "CURIE_SCENARIO_API_IMAGE": "curie-api:wsdiag-${{ github.sha }}",
        "CURIE_SCENARIO_WORKER_IMAGE": "curie-worker:wsdiag-${{ github.sha }}",
        "CURIE_SCENARIO_RUNNER_IMAGE": "curie-runner:wsdiag-${{ github.sha }}",
    }
    commands = workspace["run"].replace("\\\n", " ")
    assert [" ".join(line.split()) for line in commands.splitlines() if line.strip()] == [
        "set -euo pipefail",
        "bash tools/runtime-assertion-gate/run.sh "
        "charts/curie/ci/runtime/workspace-dependency-diagnostic-runtime.sh",
    ]
    staging_names = [
        "Download the workspace diagnostic CLI",
        "Load the workspace diagnostic candidate images",
        "Stage the workspace diagnostic images in kind",
    ]
    staging = [step for step in steps if step.get("name") in staging_names]
    assert [step["name"] for step in staging] == staging_names
    calico_index = next(
        index
        for index, step in enumerate(steps)
        if step.get("name") == "Install Calico so NetworkPolicy is actually enforced"
    )
    assert calico_index < steps.index(staging[0]) < steps.index(staging[1])
    assert steps.index(staging[1]) < steps.index(staging[2]) < steps.index(workspace)
    assert workspace["if"] == _chart_step_if("workspace-diagnostic")
    for step in staging:
        assert step["if"] == WORKLOAD_STAGING_IF, step
        assert "continue-on-error" not in step
    download, load, stage = staging
    assert download["uses"].startswith("actions/download-artifact@")
    assert download["with"] == {
        "name": "curie-x86_64-linux-${{ github.sha }}",
        "path": "cli/target/release",
    }
    assert load["uses"] == "./.github/actions/load-ci-images"
    assert load["with"] == {"images": "{api,worker,runner}"}
    stage_run = stage["run"]
    assert "chmod +x cli/target/release/curie" in stage_run
    assert "docker build" not in stage_run and "cargo build" not in stage_run
    for component in ("api", "worker", "runner"):
        assert (
            f"docker tag curie-ci/{component}:candidate "
            f"curie-{component}:wsdiag-${{{{ github.sha }}}}"
        ) in stage_run
    assert stage_run.count("kind load docker-image") == 1
    assert "--name curie-e2e" in stage_run
    image_load = stage_run[stage_run.index("kind load docker-image") :]
    for component in ("api", "worker", "runner"):
        assert f"curie-{component}:wsdiag-${{{{ github.sha }}}}" in image_load


def test_failed_first_install_retry_reuses_the_chart_cluster_and_candidate_artifacts() -> None:
    chart = yaml.safe_load(WORKFLOW.read_text())["jobs"][CHART_JOB]
    steps = chart["steps"]
    matches = [
        step
        for step in steps
        if step.get("name") == CHART_SCENARIOS["failed-first-install-retry"]["step"]
    ]
    assert len(matches) == 1
    retry = matches[0]
    workspace = next(
        step for step in steps if step.get("if") == _chart_step_if("workspace-diagnostic")
    )
    assert retry["if"] == _chart_step_if("failed-first-install-retry")
    assert retry["timeout-minutes"] == 30
    assert retry["env"] == workspace["env"]
    assert "continue-on-error" not in retry
    commands = retry["run"].replace("\\\n", " ")
    assert [" ".join(line.split()) for line in commands.splitlines() if line.strip()] == [
        "set -euo pipefail",
        "bash tools/runtime-assertion-gate/run.sh "
        "charts/curie/ci/runtime/failed-first-install-retry-runtime.sh",
    ]
    stage = next(
        step
        for step in steps
        if step.get("name") == "Stage the workspace diagnostic images in kind"
    )
    assert steps.index(stage) < steps.index(retry)


def test_quota_progress_runs_on_the_workload_leg() -> None:
    chart = yaml.safe_load(WORKFLOW.read_text())["jobs"][CHART_JOB]
    steps = [step for step in chart["steps"] if step.get("name") == "Quota progress regression"]
    assert len(steps) == 1
    quota = steps[0]
    assert quota["if"] == _chart_step_if("quota-progress")
    assert quota["timeout-minutes"] == 30
    assert "continue-on-error" not in quota
    assert quota["env"] == {
        "CURIE_BIN": "${{ github.workspace }}/cli/target/release/curie",
        "CURIE_SCENARIO_CONTEXT": "kind-curie-e2e",
        "CURIE_SCENARIO_RUN_ID": "${{ github.run_id }}-${{ github.run_attempt }}",
        "CURIE_SCENARIO_API_IMAGE": "curie-api:wsdiag-${{ github.sha }}",
        "CURIE_SCENARIO_WORKER_IMAGE": "curie-worker:wsdiag-${{ github.sha }}",
        "CURIE_SCENARIO_RUNNER_IMAGE": "curie-runner:wsdiag-${{ github.sha }}",
    }
    commands = quota["run"].replace("\\\n", " ")
    assert [" ".join(line.split()) for line in commands.splitlines() if line.strip()] == [
        "set -euo pipefail",
        "bash tools/runtime-assertion-gate/run.sh "
        "charts/curie/ci/runtime/idle-capacity-reclamation-runtime.sh",
    ]


# #4016: the approval resume kind scenario runs on pull requests whenever a
# worker or approval path changes, and E2E required holds it to that selection.
APPROVAL_RESUME_JOB = "e2e-cluster-approval-resume-restarts"


@pytest.mark.parametrize(
    "path",
    [
        "apps/worker/src/curie_worker/consumer.py",
        "apps/worker/src/curie_worker/kernel.py",
        "apps/api/src/curie_api/resumequeue.py",
        "apps/api/src/curie_api/resumereconciler.py",
        "apps/api/src/curie_api/sweeper.py",
        "apps/api/src/curie_api/routers/approvals.py",
        "apps/api/src/curie_api/routers/approval_recovery.py",
        "apps/api/src/curie_api/approval_policy.py",
        "runner/src/curie_runner/approval.py",
        "cli/scripts/e2e-cluster-approval-resume-restarts.sh",
    ],
)
def test_approval_resume_selected_for_worker_and_approval_paths(tmp_path: Path, path: str) -> None:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    outputs = _outputs_of(output)
    assert outputs["approval_resume"] == "true"
    # The job needs the images and binary the cluster tier builds.
    assert outputs["cluster"] == "true"


@pytest.mark.parametrize(
    "path",
    [
        "docs/guides/getting-started.md",
        "apps/ui/src/main.tsx",
        "apps/api/src/curie_api/routers/agents.py",
        "apps/api/src/curie_api/resumequeue_helpers/notes.md",
        "runner/src/curie_runner/session.py",
        "cli/scripts/e2e-cluster-rollout-recovery.sh",
        "charts/curie/values.yaml",
    ],
)
def test_approval_resume_not_selected_for_unrelated_paths(tmp_path: Path, path: str) -> None:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    assert _outputs_of(output)["approval_resume"] == "false"


def test_approval_resume_follows_push_and_kind_omission(tmp_path: Path) -> None:
    pushed, pushed_output = _invoke_selector(tmp_path, push=True)
    assert pushed.returncode == 0, pushed.stderr
    assert _outputs_of(pushed_output)["approval_resume"] == "true"

    next_push, next_output = _invoke_selector(tmp_path, push=True, omit_kind=True)
    assert next_push.returncode == 0, next_push.stderr
    assert _outputs_of(next_output)["approval_resume"] == "false"

    next_pr, next_pr_output = _invoke_selector(
        tmp_path, "apps/api/src/curie_api/resumequeue.py", omit_kind=True
    )
    assert next_pr.returncode == 0, next_pr.stderr
    assert _outputs_of(next_pr_output)["approval_resume"] == "false"


@pytest.mark.parametrize("event_name", ["pull_request", "push", "workflow_dispatch"])
def test_aggregate_requires_approval_resume_success_when_selected(
    event_name: str,
) -> None:
    state: _AggregateOverrides = {
        "event_name": event_name,
        "cluster_selected": "true",
        "cluster_result": "success",
        "cluster_chart_result": "success",
        "rollout_recovery_result": "skipped",
        "approval_resume_selected": "true",
    }
    ok = _run_aggregate(**state, approval_resume_result="success")
    assert ok.returncode == 0, ok.stdout + ok.stderr
    for result in ("skipped", "failure", "cancelled"):
        rejected = _run_aggregate(**state, approval_resume_result=result)
        assert rejected.returncode != 0, result


@pytest.mark.parametrize("result", ["success", "failure", "cancelled"])
def test_aggregate_requires_approval_resume_skip_when_not_selected(result: str) -> None:
    ok = _run_aggregate(event_name="pull_request", approval_resume_result="skipped")
    assert ok.returncode == 0, ok.stdout + ok.stderr
    rejected = _run_aggregate(event_name="pull_request", approval_resume_result=result)
    assert rejected.returncode != 0, result


@pytest.mark.parametrize("selected", ["", "yes"])
def test_aggregate_rejects_malformed_approval_resume_selection(selected: str) -> None:
    completed = _run_aggregate(approval_resume_selected=selected)
    assert completed.returncode != 0


def test_negative_control_covers_approval_resume_result() -> None:
    state: _AggregateOverrides = {
        "event_name": "pull_request",
        "approval_resume_selected": "true",
        "approval_resume_result": "success",
    }
    unmutated = _run_aggregate(**state)
    assert unmutated.returncode == 0, unmutated.stdout + unmutated.stderr

    check = '"$APPROVAL_RESUME_RESULT" != "$approval_resume_expected" ||'

    def accept_drift(script: str) -> str:
        assert script.count(check) == 1
        return script.replace(check, "", 1)

    completed = _run_aggregate(script_transform=accept_drift, **state)
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, output
    assert "Selected and skipped negative control failed" in output


def test_approval_resume_job_runs_on_pull_requests_and_gates_e2e_required() -> None:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    job = jobs[APPROVAL_RESUME_JOB]
    assert job["name"] == "E2E approval resume after worker and store restarts (kind)"
    assert job["if"] == "${{ needs.changes.outputs.approval_resume == 'true' }}"
    assert "pull_request" not in job["if"]
    assert job["needs"] == ["rust-build", "changes", "ci-images"]
    assert APPROVAL_RESUME_JOB in jobs["e2e-required"]["needs"]

    scenario = [
        step
        for step in job["steps"]
        if step.get("run") == "bash cli/scripts/e2e-cluster-approval-resume-restarts.sh"
    ]
    assert len(scenario) == 1
    assert scenario[0]["env"] == {"CURIE_BIN": "cli/target/release/curie"}
    assert "if" not in scenario[0]
    assert "continue-on-error" not in scenario[0]

    diagnostics = next(
        step for step in job["steps"] if step.get("name") == "Dump cluster diagnostics on failure"
    )
    assert diagnostics["if"] == "failure()"
    assert "app.kubernetes.io/component=api" in diagnostics["run"]
    assert "app.kubernetes.io/component=runner-sandbox" in diagnostics["run"]

    teardown = next(
        step
        for step in job["steps"]
        if step.get("name") == "Tear down the disposable cluster release"
    )
    assert teardown["if"] == "always()"
    assert "kubectl delete namespace curie" in teardown["run"]
    assert "--ignore-not-found" in teardown["run"]
    assert "kind delete cluster" not in teardown["run"]

    # Same pinned install surface as the rollout recovery sibling.
    sibling = jobs["e2e-cluster-rollout-recovery"]

    def uses(steps: list[dict[str, Any]]) -> list[str]:
        return [step["uses"] for step in steps if "uses" in step]

    assert uses(job["steps"]) == uses(sibling["steps"])


# #4111: factory replay has its own required result, independent of the other
# kind scenarios. The selector's existing factory output owns this selection.
SCRIPTED_FACTORY_JOB = "e2e-scripted-factory"
ADMITTED_AGENT_RUNTIME = "tools/factory-e2e/tests/test_c4_admitted_agent.py"
ADMITTED_AGENT_INPUTS = (
    "apps/api/src/curie_api/workitem_dispatch.py",
    "apps/api/src/curie_api/routers/work_items.py",
    "apps/api/src/curie_api/forges/surfaces.py",
    "apps/api/src/curie_api/forges/trackers.py",
    "apps/api/src/curie_api/forges/registry.py",
    "apps/api/src/curie_api/crud/channels.py",
    "apps/api/src/curie_api/threadkeys.py",
    "packages/aci-protocol/src/aci_protocol/turn.py",
    "apps/worker/src/curie_worker/workitem_dispatch.py",
    "apps/worker/src/curie_worker/binding.py",
    "apps/worker/src/curie_worker/kernel/constants.py",
    "apps/worker/src/curie_worker/kernel/lifecycle.py",
    "apps/worker/src/curie_worker/kernel/work_items.py",
    "apps/worker/src/curie_worker/kernel/claim.py",
    ADMITTED_AGENT_RUNTIME,
)


@pytest.mark.parametrize("path", ADMITTED_AGENT_INPUTS)
def test_admitted_agent_authority_paths_select_scripted_factory(tmp_path: Path, path: str) -> None:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    outputs = _outputs_of(output)
    assert outputs["factory"] == "true", path
    assert outputs["cluster"] == "true", path


@pytest.mark.parametrize("path", ADMITTED_AGENT_INPUTS)
def test_admitted_agent_factory_selection_respects_kind_omission(tmp_path: Path, path: str) -> None:
    completed, output = _invoke_selector(tmp_path, path, omit_kind=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert _outputs_of(output)["factory"] == "false"


@pytest.mark.parametrize(
    "path",
    [
        "apps/api/src/curie_api/routers/state.py",
        "apps/worker/src/curie_worker/state/config.py",
        "apps/api/src/curie_api/workitem_dispatch.py.other",
        "apps/api/src/curie_api/routers/work_items.py.other",
        "apps/api/src/curie_api/forges.py",
        "apps/api/src/curie_api/forges_other/surfaces.py",
        "apps/api/src/curie_api/crud/channels.py.other",
        "apps/api/src/curie_api/threadkeys.py.other",
        "packages/aci-protocol/src/aci_protocol/turn.py.other",
        "apps/worker/src/curie_worker/workitem_dispatch.py.other",
        "apps/worker/src/curie_worker/binding.py.other",
        "apps/worker/src/curie_worker/kernel/lifecycle.py.other",
        "apps/worker/src/curie_worker/kernel/work_items.py.other",
        "apps/worker/src/curie_worker/kernel/claim.py.other",
        "docs/guides/getting-started.md",
    ],
)
def test_unrelated_paths_do_not_select_admitted_agent_factory(tmp_path: Path, path: str) -> None:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert _outputs_of(output)["factory"] == "false", path


@pytest.mark.parametrize("event_name", ["pull_request", "push", "workflow_dispatch"])
@pytest.mark.parametrize("result", ["failure", "skipped", "cancelled", ""])
def test_aggregate_requires_scripted_factory_success_when_selected(
    event_name: str, result: str
) -> None:
    state: _AggregateOverrides = {"event_name": event_name, "factory_selected": "true"}
    ok = _run_aggregate(**state, factory_result="success")
    assert ok.returncode == 0, ok.stdout + ok.stderr

    rejected = _run_aggregate(**state, factory_result=result)
    assert rejected.returncode != 0, rejected.stdout + rejected.stderr


@pytest.mark.parametrize("event_name", ["pull_request", "push", "workflow_dispatch"])
@pytest.mark.parametrize("result", ["success", "failure", "cancelled", ""])
def test_aggregate_requires_scripted_factory_skip_when_not_selected(
    event_name: str, result: str
) -> None:
    ok = _run_aggregate(event_name=event_name, factory_result="skipped")
    assert ok.returncode == 0, ok.stdout + ok.stderr

    rejected = _run_aggregate(event_name=event_name, factory_result=result)
    assert rejected.returncode != 0, rejected.stdout + rejected.stderr


@pytest.mark.parametrize("sibling", sorted(FACTORY_SIBLING_RESULTS))
@pytest.mark.parametrize("event_name", ["pull_request", "push", "workflow_dispatch"])
@pytest.mark.parametrize("selected", ["true", "false"])
def test_aggregate_requires_factory_sibling_results_to_follow_factory_selection(
    event_name: str,
    selected: str,
    sibling: Literal["forge_instance_result", "admitted_agent_result"],
) -> None:
    expected, wrong = ("success", "skipped") if selected == "true" else ("skipped", "success")
    state: _AggregateOverrides = {
        "event_name": event_name,
        "factory_selected": selected,
        "factory_result": expected,
    }
    state[sibling] = expected
    ok = _run_aggregate(**state)
    assert ok.returncode == 0, ok.stdout + ok.stderr
    for result in (wrong, "failure", "cancelled", ""):
        state[sibling] = result
        rejected = _run_aggregate(**state)
        assert rejected.returncode != 0, rejected.stdout + rejected.stderr


@pytest.mark.parametrize("sibling", sorted(FACTORY_SIBLING_RESULTS))
@pytest.mark.parametrize(("selected", "result"), [("true", "success"), ("false", "skipped")])
def test_negative_control_covers_factory_sibling_results(
    selected: str,
    result: str,
    sibling: Literal["forge_instance_result", "admitted_agent_result"],
) -> None:
    variable = FACTORY_SIBLING_RESULTS[sibling]
    state: _AggregateOverrides = {
        "event_name": "pull_request",
        "factory_selected": selected,
        "factory_result": result,
    }
    state[sibling] = result
    check = f'"${variable}" != "$factory_expected" ||'

    def accept_drift(script: str) -> str:
        assert script.count(check) == 1
        return script.replace(check, "", 1)

    completed = _run_aggregate(script_transform=accept_drift, **state)
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, output
    assert f"Selected and skipped negative control failed for {variable}" in output


@pytest.mark.parametrize("selected", ["", "yes"])
def test_aggregate_rejects_malformed_scripted_factory_selection(selected: str) -> None:
    completed = _run_aggregate(factory_selected=selected)
    assert completed.returncode != 0, completed.stdout + completed.stderr


@pytest.mark.parametrize(("selected", "result"), [("true", "success"), ("false", "skipped")])
def test_negative_control_covers_scripted_factory_result(selected: str, result: str) -> None:
    state: _AggregateOverrides = {
        "event_name": "pull_request",
        "factory_selected": selected,
        "factory_result": result,
    }
    unmutated = _run_aggregate(**state)
    assert unmutated.returncode == 0, unmutated.stdout + unmutated.stderr

    check = '"$FACTORY_RESULT" != "$factory_expected" ||'

    def accept_drift(script: str) -> str:
        assert script.count(check) == 1
        return script.replace(check, "", 1)

    completed = _run_aggregate(script_transform=accept_drift, **state)
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, output
    assert "Selected and skipped negative control failed for FACTORY_RESULT" in output


@pytest.mark.parametrize("path", INITIAL_BINDING_INPUTS)
def test_registration_seam_changes_select_the_factory_runtime(tmp_path: Path, path: str) -> None:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    outputs = _outputs_of(output)
    assert outputs["factory"] == "true"
    assert outputs["images"] == "true"
    assert outputs["cli_release"] == "true"


@pytest.mark.parametrize("path", ("cli/src/ops/convergence.rs", CLUSTER_UP_MODULE))
@pytest.mark.parametrize("omit_kind", (False, True))
def test_convergence_changes_select_the_fresh_install_runtime(
    tmp_path: Path,
    path: str,
    omit_kind: bool,
) -> None:
    completed, output = _invoke_selector(tmp_path, path, omit_kind=omit_kind)
    assert completed.returncode == 0, completed.stderr
    outputs = _outputs_of(output)
    expected = "false" if omit_kind else "true"
    assert outputs["factory"] == expected
    assert outputs["cluster"] == expected
    if omit_kind:
        assert outputs["released_upgrade"] == "false"
        if path.startswith("cli/"):
            for key in ("skill", "local", "local_release", "cli_release"):
                assert outputs[key] == "true", key
    else:
        assert outputs["images"] == "true"
        assert outputs["cli_release"] == "true"


@pytest.mark.parametrize(
    "path",
    (
        "apps/api/src/curie_api/forges/gitlab/connection.py",
        "apps/api/src/curie_api/forges/credential_providers.py",
        "apps/api/src/curie_api/forges/surfaces.py.other",
        "apps/api/src/curie_api/forges/trackers.py.other",
        "apps/api/src/curie_api/forges/registry.py.other",
        "apps/api/src/curie_api/forge_credential_health.py",
        "apps/api/src/curie_api/routers/forge_credential_health.py",
        "cli/src/forge_expiry.rs",
        "apps/api/tests/forge_fakes/gitlab.py",
        STATIC_TOKEN_ROTATION_MODULE,
    ),
)
def test_static_token_rotation_inputs_select_the_factory_runtime(tmp_path: Path, path: str) -> None:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    assert _outputs_of(output)["factory"] == "true"


@pytest.mark.parametrize(
    "path",
    (
        "cli/src/output.rs",
        "cli/src/ops/capacity.rs",
        "apps/api/src/curie_api/routers/config.py",
        "docs/guide.md",
    ),
)
def test_unrelated_changes_do_not_select_the_factory_runtime(tmp_path: Path, path: str) -> None:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    assert _outputs_of(output)["factory"] == "false"


FRESH_INSTALL_TEST = "test_fresh_cluster_up_converges_without_rerun"
SCRIPTED_FACTORY_LEGS = (
    ("scenario", lambda run: run == "python3 tools/factory-e2e/factory_e2e.py scripted"),
    ("initial-binding", lambda run: INITIAL_BINDING_MODULE in run),
    (
        "static-token-rotation",
        lambda run: run == f"uv run pytest -q -n 0 {STATIC_TOKEN_ROTATION_MODULE}",
    ),
    (
        "fresh-install",
        lambda run: run == f"uv run pytest -q -n 0 {CLUSTER_UP_MODULE}::{FRESH_INSTALL_TEST}",
    ),
)
SCRIPTED_FACTORY_EVIDENCE_STEP = "Upload scripted factory evidence"


def _assert_scripted_factory_legs_map_one_to_one(job: dict[str, Any]) -> None:
    """Every suite runs in exactly one matrix leg, so none can be silently skipped."""
    legs = [leg for leg, _ in SCRIPTED_FACTORY_LEGS]
    strategy = job.get("strategy", {})
    if strategy.get("fail-fast") is not False:
        raise AssertionError("strategy.fail-fast must be False")
    if set(strategy.get("matrix", {})) != {"suite"} or strategy["matrix"]["suite"] != legs:
        raise AssertionError(f"matrix must be exactly suite: {legs}")
    if job.get("timeout-minutes") != 120:
        raise AssertionError("timeout-minutes must be 120")
    steps = job["steps"]
    claimed: list[dict[str, Any]] = []
    for leg, runs_command in SCRIPTED_FACTORY_LEGS:
        owners = [step for step in steps if step.get("if") == f"matrix.suite == '{leg}'"]
        if len(owners) != 1:
            raise AssertionError(f"leg {leg} needs exactly one step, found {len(owners)}")
        if not runs_command(owners[0].get("run", "")):
            raise AssertionError(f"leg {leg} step runs the wrong command")
        claimed.append(owners[0])
    for step in steps:
        if "matrix.suite" not in json.dumps(step) or any(step is owner for owner in claimed):
            continue
        if not (
            step.get("name") == SCRIPTED_FACTORY_EVIDENCE_STEP
            and step.get("if") == "always() && matrix.suite == 'scenario'"
        ):
            raise AssertionError(f"unexpected matrix.suite use in step {step.get('name')!r}")
    evidence = [s for s in steps if s.get("name") == SCRIPTED_FACTORY_EVIDENCE_STEP]
    if len(evidence) != 1 or evidence[0].get("if") != "always() && matrix.suite == 'scenario'":
        raise AssertionError("evidence upload must run only in the scenario leg")


def _scripted_factory_step(job: dict[str, Any], leg: str) -> dict[str, Any]:
    return next(s for s in job["steps"] if s.get("if") == f"matrix.suite == '{leg}'")


def _drop_leg(job: dict[str, Any]) -> None:
    job["strategy"]["matrix"]["suite"].remove("fresh-install")


def _drop_step_if(job: dict[str, Any]) -> None:
    del _scripted_factory_step(job, "static-token-rotation")["if"]


def _misdirect_step_if(job: dict[str, Any]) -> None:
    _scripted_factory_step(job, "scenario")["if"] = "matrix.suite == 'initial-binding'"


def _enable_fail_fast(job: dict[str, Any]) -> None:
    job["strategy"]["fail-fast"] = True


def _unguard_evidence(job: dict[str, Any]) -> None:
    evidence = next(s for s in job["steps"] if s.get("name") == SCRIPTED_FACTORY_EVIDENCE_STEP)
    evidence["if"] = "always()"


def test_scripted_factory_matrix_maps_each_suite_to_exactly_one_leg() -> None:
    job = yaml.safe_load(WORKFLOW.read_text())["jobs"][SCRIPTED_FACTORY_JOB]
    _assert_scripted_factory_legs_map_one_to_one(job)


@pytest.mark.parametrize(
    "mutate",
    (_drop_leg, _drop_step_if, _misdirect_step_if, _enable_fail_fast, _unguard_evidence),
)
def test_scripted_factory_leg_mapping_rejects_skippable_shapes(
    mutate: Callable[[dict[str, Any]], None],
) -> None:
    job = yaml.safe_load(WORKFLOW.read_text())["jobs"][SCRIPTED_FACTORY_JOB]
    _assert_scripted_factory_legs_map_one_to_one(job)
    mutated = copy.deepcopy(job)
    mutate(mutated)
    with pytest.raises(AssertionError):
        _assert_scripted_factory_legs_map_one_to_one(mutated)


def test_scripted_factory_job_runs_on_pull_requests_and_gates_e2e_required(tmp_path: Path) -> None:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    job = jobs[SCRIPTED_FACTORY_JOB]
    assert job["name"] == "E2E scripted factory (kind, scripted model, ${{ matrix.suite }})"
    _assert_scripted_factory_legs_map_one_to_one(job)
    assert job["if"] == "${{ needs.changes.outputs.factory == 'true' }}"
    assert "pull_request" not in job["if"]
    assert job["needs"] == ["rust-build", "changes", "ci-images"]
    assert SCRIPTED_FACTORY_JOB in jobs["e2e-required"]["needs"]

    scenario = [
        step
        for step in job["steps"]
        if step.get("run") == "python3 tools/factory-e2e/factory_e2e.py scripted"
    ]
    assert len(scenario) == 1
    assert scenario[0]["env"] == {
        "CURIE_BIN": "${{ github.workspace }}/cli/target/release/curie",
        "BUILDX_BUILDER": "default",
    }
    assert "continue-on-error" not in scenario[0]

    steps = job["steps"]
    registrations = [step for step in steps if INITIAL_BINDING_MODULE in step.get("run", "")]
    assert len(registrations) == 1
    registration = registrations[0]
    rotations = [step for step in steps if STATIC_TOKEN_ROTATION_MODULE in step.get("run", "")]
    assert len(rotations) == 1
    rotation = rotations[0]
    assert rotation["run"] == f"uv run pytest -q -n 0 {STATIC_TOKEN_ROTATION_MODULE}"
    assert not rotation.get("continue-on-error")
    assert not registration.get("continue-on-error")
    pytest_steps = [step for step in steps if step.get("run", "").startswith("uv run pytest")]
    assert len(pytest_steps) == 3
    first_pytest = min(steps.index(step) for step in pytest_steps)
    assert any(
        step.get("uses", "").startswith("astral-sh/setup-uv@") for step in steps[:first_pytest]
    )
    binaries = tmp_path / "bin"
    binaries.mkdir()
    log = tmp_path / "registration-argv.json"
    uv = binaries / "uv"
    uv.write_text(
        f"#!{sys.executable}\nimport json, os, sys\nfrom pathlib import Path\n"
        "Path(os.environ['REGISTRATION_ARGV']).write_text(json.dumps(sys.argv[1:]))\n"
    )
    uv.chmod(0o755)
    executed = subprocess.run(
        ["bash", "-e", "-o", "pipefail", "-c", registration["run"]],
        cwd=tmp_path,
        env={
            **os.environ,
            "PATH": f"{binaries}:{os.environ['PATH']}",
            "REGISTRATION_ARGV": str(log),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert executed.returncode == 0, executed.stdout + executed.stderr
    argv = json.loads(log.read_text())
    assert argv[:2] == ["run", "pytest"]
    assert INITIAL_BINDING_MODULE in argv
    assert not any(arg in argv for arg in ("-k", "-m", "--deselect"))
    assert not any(arg.startswith(INITIAL_BINDING_MODULE + "::") for arg in argv)


@pytest.mark.parametrize("exit_code", (0, 17))
def test_fresh_cluster_up_runtime_is_serial_and_propagates_failure(
    tmp_path: Path,
    exit_code: int,
) -> None:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    job = jobs[SCRIPTED_FACTORY_JOB]
    assert not job.get("continue-on-error")
    assert SCRIPTED_FACTORY_JOB in jobs["e2e-required"]["needs"]
    steps = job["steps"]
    runtime_steps = [step for step in steps if CLUSTER_UP_MODULE in step.get("run", "")]
    assert len(runtime_steps) == 1
    runtime = runtime_steps[0]
    _assert_scripted_factory_legs_map_one_to_one(job)
    assert runtime["if"] == "matrix.suite == 'fresh-install'"
    assert not runtime.get("continue-on-error")

    binaries = tmp_path / "bin"
    binaries.mkdir()
    log = tmp_path / "runtime-argv.json"
    uv = binaries / "uv"
    uv.write_text(
        f"#!{sys.executable}\nimport json, os, sys\nfrom pathlib import Path\n"
        "Path(os.environ['RUNTIME_ARGV']).write_text(json.dumps(sys.argv[1:]))\n"
        f"sys.exit({exit_code})\n"
    )
    uv.chmod(0o755)
    executed = subprocess.run(
        ["bash", "-e", "-o", "pipefail", "-c", runtime["run"]],
        cwd=tmp_path,
        env={
            **os.environ,
            "PATH": f"{binaries}:{os.environ['PATH']}",
            "RUNTIME_ARGV": str(log),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert executed.returncode == exit_code, executed.stdout + executed.stderr
    argv = json.loads(log.read_text())
    assert argv[:2] == ["run", "pytest"]
    assert argv[argv.index("-n") + 1] == "0"
    assert any(
        target in argv
        for target in (
            CLUSTER_UP_MODULE,
            CLUSTER_UP_MODULE + "::test_fresh_cluster_up_converges_without_rerun",
        )
    )
    assert not any(arg.startswith(("-k", "-m", "--deselect", "--ignore")) for arg in argv)


@functools.cache
def _collect_forge_instance_cases(*pytest_args: str) -> tuple[str, ...]:
    collected = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "--collect-only",
            *(pytest_args or ("-q", "-n", "0", FORGE_INSTANCE_MODULE)),
        ],
        cwd=REPO_ROOT,
        env={**os.environ, "PYTEST_ADDOPTS": ""},
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    assert collected.returncode == 0, collected.stdout + collected.stderr
    cases = tuple(
        line.strip()
        for line in collected.stdout.splitlines()
        if line.startswith(FORGE_INSTANCE_MODULE + "::")
    )
    assert cases, collected.stdout + collected.stderr
    return cases


def _assert_forge_instance_legs_map_one_to_one(
    job: dict[str, Any],
    collected: tuple[str, ...],
) -> None:
    matrix = job["strategy"]["matrix"]
    assert set(matrix) == {"include"}
    legs = matrix["include"]
    selectors = [f"{FORGE_INSTANCE_MODULE}::{leg['test']}" for leg in legs]
    assert sorted(selectors) == sorted(collected), "each collected case needs exactly one leg"
    assert len({leg["suite"] for leg in legs}) == len(legs), "suite labels must be unique"


def test_forge_instance_job_is_gated_like_the_scripted_factory_and_required() -> None:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    job = jobs[FORGE_INSTANCE_JOB]
    assert job["name"] == "Forge instance regression (kind, ${{ matrix.suite }})"
    assert job["runs-on"] == "ubuntu-latest"
    assert job["timeout-minutes"] == 165
    assert job["strategy"] == {
        "fail-fast": False,
        "matrix": {
            "include": [
                {"suite": suite, "test": test} for suite, test in FORGE_INSTANCE_LEGS.items()
            ]
        },
    }
    assert not job.get("continue-on-error")
    assert job["if"] == _nightly_if("needs.changes.outputs.factory == 'true'")
    assert job["if"] != jobs[SCRIPTED_FACTORY_JOB]["if"]
    assert "needs.changes.outputs.factory == 'true'" in job["if"]
    assert job["needs"] == ["changes"]
    assert FORGE_INSTANCE_JOB in jobs["e2e-required"]["needs"]
    steps = job["steps"]
    runs = [step for step in steps if FORGE_INSTANCE_MODULE in json.dumps(step)]
    assert len(runs) == 1
    run = runs[0]
    assert run["run"] == 'uv run pytest -q -n 0 "$CASE_SELECTOR"'
    assert run["env"] == {"CASE_SELECTOR": FORGE_INSTANCE_MODULE + "::${{ matrix.test }}"}
    assert not run.get("if")
    assert not run.get("continue-on-error")
    before = steps[: steps.index(run)]
    for action in ("astral-sh/setup-uv@", "azure/setup-helm@", "helm/kind-action@"):
        assert any(step.get("uses", "").startswith(action) for step in before), action
    assert any("/usr/share/dotnet" in step.get("run", "") for step in before)
    required = jobs["e2e-required"]["steps"][0]
    assert required["env"]["FORGE_INSTANCE_RESULT"] == (
        "${{ needs.forge-instance-regression.result }}"
    )
    script = required["run"]
    assert '"$FORGE_INSTANCE_RESULT" != "$forge_instance_expected"' in script
    assert "FACTORY_RESULT FORGE_INSTANCE_RESULT ADMITTED_AGENT_RESULT" in script


NIGHTLY_JOBS = {
    FORGE_INSTANCE_JOB: "needs.changes.outputs.factory == 'true'",
    "e2e-cluster-rollout-recovery": "needs.changes.outputs.cluster == 'true'",
    "e2e-released-upgrade": "needs.changes.outputs.released_upgrade_full == 'true'",
}


def test_nightly_jobs_keep_their_selector_and_reject_a_missing_condition() -> None:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    for job_id, selection in NIGHTLY_JOBS.items():
        assert jobs[job_id]["if"] == _nightly_if(selection)
        missing = jobs[job_id]["if"].replace("inputs.nightly == true && ", "")
        assert missing != jobs[job_id]["if"]
        assert "inputs.nightly" not in missing
    for unchanged in ("cli-portability", "cli-darwin", "e2e-released-upgrade-negative"):
        assert "inputs.nightly" not in jobs[unchanged]["if"]
    assert jobs["cli-portability"]["if"] == jobs["cli-darwin"]["if"]


@pytest.mark.parametrize(
    "overrides",
    [
        {
            "event_name": "workflow_dispatch",
            "nightly": "true",
            "factory_selected": "true",
            "factory_result": "success",
            "forge_instance_result": "skipped",
        },
        {
            "event_name": "workflow_dispatch",
            "nightly": "true",
            "cluster_selected": "true",
            "cluster_result": "success",
            "cluster_chart_result": "success",
            "rollout_recovery_result": "skipped",
        },
        {
            "event_name": "workflow_dispatch",
            "nightly": "true",
            "released_upgrade_selected": "true",
            "released_upgrade_full": "true",
            "released_upgrade_result": "skipped",
            "released_upgrade_negative_result": "success",
            "upgrade_matrix_result": "success",
        },
    ],
)
def test_aggregate_rejects_a_skip_on_a_nightly_dispatch(
    overrides: dict[str, str],
) -> None:
    completed = _run_aggregate(**overrides)
    assert completed.returncode != 0, completed.stdout + completed.stderr


@pytest.mark.parametrize("mutation", ("extra-case", "missing-leg", "duplicate-leg"))
def test_forge_instance_leg_mapping_rejects_missing_or_duplicate_coverage(mutation: str) -> None:
    job = yaml.safe_load(WORKFLOW.read_text())["jobs"][FORGE_INSTANCE_JOB]
    collected = _collect_forge_instance_cases()
    _assert_forge_instance_legs_map_one_to_one(job, collected)
    if mutation == "extra-case":
        collected += (FORGE_INSTANCE_MODULE + "::test_new_unmapped_regression",)
    elif mutation == "missing-leg":
        job["strategy"]["matrix"]["include"].pop()
    else:
        job["strategy"]["matrix"]["include"].append(
            copy.deepcopy(job["strategy"]["matrix"]["include"][0])
        )
    with pytest.raises(AssertionError, match="each collected case needs exactly one leg"):
        _assert_forge_instance_legs_map_one_to_one(job, collected)


@pytest.mark.parametrize("failing_suite", (None, *FORGE_INSTANCE_LEGS))
def test_forge_instance_matrix_executes_each_case_and_propagates_failure(
    tmp_path: Path,
    failing_suite: str | None,
) -> None:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    job = jobs[FORGE_INSTANCE_JOB]
    _assert_forge_instance_legs_map_one_to_one(job, _collect_forge_instance_cases())
    runtime = next(step for step in job["steps"] if FORGE_INSTANCE_MODULE in json.dumps(step))
    binaries = tmp_path / "bin"
    binaries.mkdir()
    uv = binaries / "uv"
    uv.write_text(
        f"#!{sys.executable}\nimport json, os, sys\nfrom pathlib import Path\n"
        "Path(os.environ['FORGE_ARGV']).write_text(json.dumps(sys.argv[1:]))\n"
        "sys.exit(int(os.environ['FORGE_EXIT_CODE']))\n"
    )
    uv.chmod(0o755)
    selected: list[str] = []
    results = []
    for leg in job["strategy"]["matrix"]["include"]:
        log = tmp_path / f"{leg['suite']}.json"
        exit_code = 17 if leg["suite"] == failing_suite else 0
        environment = {
            key: value.replace("${{ matrix.test }}", leg["test"])
            for key, value in runtime["env"].items()
        }
        executed = subprocess.run(
            ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", runtime["run"]],
            cwd=tmp_path,
            env={
                **os.environ,
                **environment,
                "PATH": f"{binaries}:{os.environ['PATH']}",
                "FORGE_ARGV": str(log),
                "FORGE_EXIT_CODE": str(exit_code),
            },
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        assert executed.returncode == exit_code, executed.stdout + executed.stderr
        argv = json.loads(log.read_text())
        selector = f"{FORGE_INSTANCE_MODULE}::{leg['test']}"
        assert argv == ["run", "pytest", "-q", "-n", "0", selector]
        cases = _collect_forge_instance_cases(*argv[2:])
        assert cases == (selector,)
        selected.extend(cases)
        results.append(executed.returncode)
    assert sorted(selected) == sorted(_collect_forge_instance_cases())
    aggregate = _run_aggregate(
        event_name="workflow_dispatch",
        nightly="true",
        factory_selected="true",
        factory_result="success",
        forge_instance_result="failure" if any(results) else "success",
    )
    assert (aggregate.returncode == 0) == (failing_suite is None), (
        aggregate.stdout + aggregate.stderr
    )


@pytest.mark.parametrize(
    ("job_id", "variable"),
    [(ADMITTED_AGENT_JOB, "ADMITTED_AGENT_RESULT")],
)
def test_split_factory_jobs_are_gated_like_the_scripted_factory_and_required(
    job_id: str, variable: str
) -> None:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    job = jobs[job_id]
    assert job["runs-on"] == "ubuntu-latest"
    assert job["timeout-minutes"] == 120
    assert job["if"] == jobs[SCRIPTED_FACTORY_JOB]["if"]
    assert job["needs"] == ["changes"]
    assert not job.get("continue-on-error")
    assert job_id in jobs["e2e-required"]["needs"]
    required = jobs["e2e-required"]["steps"][0]
    assert required["env"][variable] == "${{ needs." + job_id + ".result }}"
    assert f'"${variable}" != "$factory_expected"' in required["run"]


def test_controller_preflight_upgrade_scenario_runs_on_chart_pull_requests() -> None:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    job = jobs["e2e-cluster-chart-regressions"]
    name = "Controller preflight accepts a serving upgrade and refuses broken RBAC"
    steps = [step for step in job["steps"] if step.get("name") == name]
    assert len(steps) == 1, "The controller upgrade proof must execute in required PR CI"
    step = steps[0]
    assert step["if"] == _chart_step_if("controller-preflight")
    assert "continue-on-error" not in step
    # This scenario owns its kind cluster; the runtime receipt runner accepts
    # only ci/runtime scripts and would refuse it before executing any proof.
    assert "uv run bash charts/curie/ci/scenarios/controller-preflight-kind.sh" in step["run"]
    assert "tools/runtime-assertion-gate/run.sh" not in step["run"]
    assert "e2e-cluster-chart-regressions" in jobs["e2e-required"]["needs"]


@pytest.mark.parametrize(
    ("paths", "push", "omit_kind", "expected"),
    [
        (
            ("apps/api/src/curie_api/main.py",),
            False,
            False,
            ("quota-progress", "workspace-diagnostic"),
        ),
        (("charts/curie/templates/agent-sandbox.yaml",), False, False, CHART_IDS),
        (
            ("charts/curie/ci/runtime/connector-readiness-runtime.sh",),
            False,
            False,
            ("connector-readiness",),
        ),
        (
            ("charts/curie/ci/scenarios/controller-preflight-kind.sh",),
            False,
            False,
            ("controller-preflight",),
        ),
        (
            ("packages/plugin-format/src/plugin_format/connector_render.py",),
            False,
            False,
            ("connector-readiness", "quota-progress", "workspace-diagnostic"),
        ),
        (
            ("apps/worker/src/curie_worker/connector_k8s.py",),
            False,
            False,
            ("connector-readiness", "quota-progress", "workspace-diagnostic"),
        ),
        (("docs/adr/AGENTS.md",), False, False, ()),
        (("newtoplevel/file.txt",), False, False, CHART_IDS),
        ((), True, False, CHART_IDS),
        ((), True, True, ()),
        (("apps/api/src/curie_api/main.py",), False, True, ()),
        (
            ("charts/curie/ci/runtime/langfuse-postgres-readiness-runtime.sh",),
            False,
            True,
            ("langfuse-postgres-readiness",),
        ),
        (_version_only_paths(_repo_version()), False, False, ()),
    ],
    ids=(
        "api",
        "shared-template",
        "connector-script",
        "controller-script",
        "plugin-format",
        "worker-connector",
        "docs",
        "fallback",
        "push",
        "next-push",
        "next-api",
        "next-runtime-assertion",
        "version-only",
    ),
)
def test_chart_scenarios_follow_changed_inputs_and_final_tiers(
    tmp_path: Path,
    paths: tuple[str, ...],
    push: bool,
    omit_kind: bool,
    expected: tuple[str, ...],
) -> None:
    completed, output = _invoke_selector(tmp_path, *paths, push=push, omit_kind=omit_kind)
    assert completed.returncode == 0, completed.stderr
    rendered = json.dumps(list(expected), separators=(",", ":"))
    assert _outputs_of(output)["chart_scenarios"] == rendered
    assert output.count("chart_scenarios=") == 1
    assert f"chart_scenarios={rendered}" in completed.stdout
    legs = json.dumps(_expected_chart_legs(expected), separators=(",", ":"))
    assert _outputs_of(output)["chart_legs"] == legs
    assert f"chart_legs={legs}" in completed.stdout
    if expected:
        assert _outputs_of(output)["cluster"] == "true"


@pytest.mark.parametrize(
    ("path", "expected"),
    [(entry["inputs"][0], (scenario,)) for scenario, entry in CHART_SCENARIOS.items()]
    + [
        ("charts/curie/values.yaml", CHART_IDS),
        ("charts/curie/values-dev.yaml", CHART_IDS),
        ("charts/curie/values.schema.json", CHART_IDS),
        ("charts/curie/values-custom.yaml", CHART_IDS),
        ("charts/curie/ci/postgres-readiness-delay.Dockerfile", ("langfuse-postgres-readiness",)),
        (
            "apps/worker/src/curie_worker/connector_k8s.py.bak",
            ("quota-progress", "workspace-diagnostic"),
        ),
        ("charts/curie/ci/runtime/connector-readiness-runtime.sh.bak", ()),
        ("charts/curie/ci/runtime/connector-readiness-runtime.sh/child", ("connector-readiness",)),
        ("examples/weather/skills/weather/SKILL.md", ()),
        ("scripts/check-netpol-enforcement.sh", ()),
    ],
)
def test_chart_scenario_input_matching_keeps_the_existing_prefix_rules(
    tmp_path: Path,
    path: str,
    expected: tuple[str, ...],
) -> None:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    assert json.loads(_outputs_of(output)["chart_scenarios"]) == list(expected)


def test_failed_first_install_retry_script_alone_selects_its_scenario(tmp_path: Path) -> None:
    completed, output = _invoke_selector(
        tmp_path, "charts/curie/ci/runtime/failed-first-install-retry-runtime.sh"
    )
    assert completed.returncode == 0, completed.stderr
    selected = _outputs_of(output)
    assert selected["cluster"] == "true"
    assert json.loads(selected["chart_scenarios"]) == ["failed-first-install-retry"]
    assert json.loads(selected["chart_legs"]) == ["workload"]


def test_failed_first_install_retry_tracks_the_production_discard_path(tmp_path: Path) -> None:
    completed, output = _invoke_selector(tmp_path, "cli/src/ops/up.rs")
    assert completed.returncode == 0, completed.stderr
    assert "failed-first-install-retry" in json.loads(_outputs_of(output)["chart_scenarios"])


def test_chart_scenario_union_is_sorted_and_deduplicated(tmp_path: Path) -> None:
    paths = (
        "charts/curie/ci/runtime/connector-readiness-runtime.sh",
        "apps/api/src/curie_api/main.py",
        "charts/curie/ci/runtime/connector-readiness-runtime.sh",
    )
    forward, output = _invoke_selector(tmp_path, *paths)
    reverse, reversed_output = _invoke_selector(tmp_path, *reversed(paths))
    assert forward.returncode == reverse.returncode == 0, forward.stderr + reverse.stderr
    assert output == reversed_output
    assert json.loads(_outputs_of(output)["chart_scenarios"]) == [
        "connector-readiness",
        "quota-progress",
        "workspace-diagnostic",
    ]


def _chart_consistency_problems(workflow: dict[str, Any], registry: dict[str, Any]) -> list[str]:
    section = registry["chart_scenarios"]
    job = workflow["jobs"][section["job"]]
    scenarios = section["scenarios"]
    by_step = {entry["step"]: scenario for scenario, entry in scenarios.items()}
    steps = {step.get("name"): step for step in job["steps"]}
    problems = []
    for name, step in steps.items():
        if not re.search(
            r"(?<![\w./-])(?:charts/curie/ci|scripts)/[^\s'\"\\]+\.sh\b",
            str(step.get("run", "")),
        ):
            continue
        if name not in by_step:
            problems.append(f"unregistered scenario step: {name}")
        elif scenarios[by_step[name]].get("leg") not in CHART_LEGS:
            problems.append(f"scenario without a leg: {by_step[name]}")
        elif step.get("if") != _chart_step_if(by_step[name], scenarios[by_step[name]]["leg"]):
            problems.append(f"noncanonical scenario condition: {name}")
    for entry in scenarios.values():
        if entry["step"] not in steps:
            problems.append(f"missing scenario step: {entry['step']}")
    return problems


def test_chart_scenario_registry_and_real_workflow_are_consistent() -> None:
    registry = yaml.safe_load(REGISTRY.read_text())
    assert registry["chart_scenarios"] == {
        "job": CHART_JOB,
        "shared_inputs": list(CHART_SHARED_INPUTS),
        "scenarios": CHART_SCENARIOS,
    }
    _SELECTOR_MODULE._load_registry(REGISTRY)
    workflow = yaml.safe_load(WORKFLOW.read_text())
    assert workflow["jobs"][CHART_JOB]["if"] == CHART_JOB_IF
    assert not _chart_consistency_problems(workflow, registry)


@pytest.mark.parametrize(
    "mutation", ["unregistered", "orphan", "missing-if", "wrong-if", "missing-leg", "wrong-leg"]
)
def test_chart_consistency_check_rejects_workflow_drift(mutation: str) -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    registry = yaml.safe_load(REGISTRY.read_text())
    steps = workflow["jobs"][CHART_JOB]["steps"]
    entry = registry["chart_scenarios"]["scenarios"]["connector-readiness"]
    step = next(step for step in steps if step.get("name") == entry["step"])
    if mutation == "unregistered":
        del registry["chart_scenarios"]["scenarios"]["connector-readiness"]
    elif mutation == "orphan":
        entry["step"] = "Absent scenario step"
    elif mutation == "missing-if":
        step.pop("if")
    elif mutation == "missing-leg":
        # The pre-#4722 condition: the step would run in both legs.
        step["if"] = (
            "${{ contains(fromJSON(needs.changes.outputs.chart_scenarios), "
            "'connector-readiness') }}"
        )
    elif mutation == "wrong-leg":
        step["if"] = _chart_step_if("connector-readiness", "workload")
    else:
        step["if"] = _chart_step_if("controller-preflight")
    assert _chart_consistency_problems(workflow, registry)


def test_every_chart_scenario_belongs_to_exactly_one_leg() -> None:
    scenarios = yaml.safe_load(REGISTRY.read_text())["chart_scenarios"]["scenarios"]
    assert set(scenarios) == set(CHART_SCENARIOS)
    for scenario, entry in scenarios.items():
        assert isinstance(entry.get("leg"), str), scenario
        assert entry["leg"] in CHART_LEGS, scenario
    workload = {scenario for scenario, entry in scenarios.items() if entry["leg"] == "workload"}
    assert workload == WORKLOAD_SCENARIOS
    loaded = _SELECTOR_MODULE._load_registry(REGISTRY).chart_scenarios.scenarios
    assert {scenario: loaded[scenario].leg for scenario in loaded} == {
        scenario: entry["leg"] for scenario, entry in scenarios.items()
    }


@pytest.mark.parametrize(
    ("paths", "push", "scenarios", "legs"),
    [
        (
            ("charts/curie/ci/runtime/idle-capacity-reclamation-runtime.sh",),
            False,
            ["quota-progress"],
            ["workload"],
        ),
        (
            ("charts/curie/ci/runtime/connector-readiness-runtime.sh",),
            False,
            ["connector-readiness"],
            ["chart"],
        ),
        ((), True, list(CHART_IDS), ["chart", "workload"]),
        (("docs/adr/AGENTS.md",), False, [], []),
    ],
    ids=("quota-only", "chart-only", "push", "docs"),
)
def test_chart_legs_follow_the_selected_scenarios(
    tmp_path: Path,
    paths: tuple[str, ...],
    push: bool,
    scenarios: list[str],
    legs: list[str],
) -> None:
    completed, output = _invoke_selector(tmp_path, *paths, push=push)
    assert completed.returncode == 0, completed.stderr
    outputs = _outputs_of(output)
    assert json.loads(outputs["chart_scenarios"]) == scenarios
    rendered = json.dumps(legs, separators=(",", ":"))
    assert outputs["chart_legs"] == rendered
    assert output.count("chart_legs=") == 1
    # The legs line follows the scenarios line it is derived from.
    keys = [line.split("=", 1)[0] for line in output.splitlines()]
    assert keys.index("chart_legs") == keys.index("chart_scenarios") + 1
    assert f"chart_legs={rendered}" in completed.stdout


@pytest.mark.parametrize("mutation", ["missing-leg", "unknown-leg", "nonstring-leg"])
def test_chart_scenario_registry_rejects_a_scenario_without_a_valid_leg(
    tmp_path: Path, mutation: str
) -> None:
    valid = tmp_path / "valid.yaml"
    valid.write_text(VALID_REGISTRY)
    # Liveness: the unmutated registry, with a leg on every scenario, loads.
    _SELECTOR_MODULE._load_registry(valid)
    registry = yaml.safe_load(VALID_REGISTRY)
    entry = registry["chart_scenarios"]["scenarios"]["connector-readiness"]
    if mutation == "missing-leg":
        del entry["leg"]
    elif mutation == "unknown-leg":
        entry["leg"] = "upgrade"
    else:
        entry["leg"] = ["chart"]
    path = tmp_path / "registry.yaml"
    path.write_text(yaml.safe_dump(registry))
    with pytest.raises(_SELECTOR_MODULE.RegistryError, match="leg"):
        _SELECTOR_MODULE._load_registry(path)


CHART_FIXTURE_STEPS = (
    "Load the Postgres readiness fixture built by the ci-images job",
    "Tag the Postgres readiness fixture",
    "Load the Postgres readiness fixture into the kind cluster",
)
WORKLOAD_STAGING_STEPS = (
    "Download the workspace diagnostic CLI",
    "Load the workspace diagnostic candidate images",
    "Stage the workspace diagnostic images in kind",
)


def test_chart_job_runs_one_leg_per_runner_from_the_selected_legs() -> None:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    chart = jobs[CHART_JOB]
    assert chart["name"] == "E2E chart runtime regressions (kind, Calico, ${{ matrix.leg }})"
    assert chart["strategy"] == {
        "fail-fast": False,
        "matrix": {"leg": "${{ fromJSON(needs.changes.outputs.chart_legs) }}"},
    }
    assert chart["if"] == CHART_JOB_IF
    steps = {step.get("name"): step for step in chart["steps"]}
    for name in CHART_FIXTURE_STEPS:
        assert steps[name].get("if") == "${{ matrix.leg == 'chart' }}", name
    for name in WORKLOAD_STAGING_STEPS:
        assert steps[name].get("if") == WORKLOAD_STAGING_IF, name
    upload = steps["Upload runtime assertion receipts"]
    assert upload["with"]["name"] == (
        "runtime-assertion-receipts-${{ github.job }}-${{ matrix.leg }}"
    )
    # Every step that names a scenario is bound to that scenario's leg.
    for name, step in steps.items():
        condition = str(step.get("if", ""))
        named = [scenario for scenario in CHART_IDS if f"'{scenario}'" in condition]
        if not named:
            continue
        legs = {CHART_SCENARIOS[scenario]["leg"] for scenario in named}
        assert len(legs) == 1, name
        assert condition.startswith("${{ " + _chart_leg_clause(legs.pop()) + " && "), name


def test_a_failing_chart_leg_fails_e2e_required() -> None:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    # A matrix job's result is the aggregate of its legs, so either failing
    # leg reaches E2E required through this one needs entry.
    assert CHART_JOB in jobs["e2e-required"]["needs"]
    _script, bindings = _aggregate_contract()
    assert bindings["cluster_chart_result"] == "CLUSTER_CHART_RESULT"
    state: _AggregateOverrides = {
        "event_name": "pull_request",
        "cluster_selected": "true",
        "cluster_result": "success",
        "chart_scenarios": '["connector-readiness","quota-progress"]',
    }
    passing = _run_aggregate(cluster_chart_result="success", **state)
    assert passing.returncode == 0, passing.stdout + passing.stderr
    failing = _run_aggregate(cluster_chart_result="failure", **state)
    assert failing.returncode != 0, failing.stdout + failing.stderr


@pytest.mark.parametrize(
    "mutation",
    [
        "section-key",
        "entry-key",
        "duplicate-step",
        "empty-inputs",
        "empty-shared-inputs",
        "empty-path",
        "absolute-path",
        "trailing-slash",
        "shared-empty-path",
        "shared-absolute-path",
        "shared-trailing-slash",
        "nonstring-key",
        "inputs-string",
        "shared-inputs-string",
    ],
)
def test_chart_scenario_registry_rejects_malformed_entries(tmp_path: Path, mutation: str) -> None:
    registry = yaml.safe_load(VALID_REGISTRY)
    section = registry["chart_scenarios"]
    entry = section["scenarios"]["connector-readiness"]
    if mutation == "section-key":
        section["unknown"] = []
    elif mutation == "entry-key":
        entry["unknown"] = []
    elif mutation == "duplicate-step":
        section["scenarios"]["controller-preflight"]["step"] = entry["step"]
    elif mutation == "empty-inputs":
        entry["inputs"] = []
    elif mutation == "empty-shared-inputs":
        section["shared_inputs"] = []
    elif mutation == "nonstring-key":
        section["scenarios"][1] = section["scenarios"].pop("connector-readiness")
    elif mutation == "inputs-string":
        entry["inputs"] = entry["inputs"][0]
    elif mutation == "shared-inputs-string":
        section["shared_inputs"] = section["shared_inputs"][0]
    else:
        values = {
            "empty-path": "",
            "absolute-path": "/charts/curie",
            "trailing-slash": "charts/curie/",
        }
        key = mutation.removeprefix("shared-")
        if mutation.startswith("shared-"):
            section["shared_inputs"][0] = values[key]
        else:
            entry["inputs"][0] = values[key]
    path = tmp_path / "registry.yaml"
    path.write_text(yaml.safe_dump(registry))
    with pytest.raises(_SELECTOR_MODULE.RegistryError):
        _SELECTOR_MODULE._load_registry(path)


@pytest.mark.parametrize("mode", ["paths", "push", "revisions"])
def test_unregistered_chart_scenario_fails_closed_in_each_selection_mode(
    tmp_path: Path,
    mode: str,
) -> None:
    repository = _new_repository(tmp_path)
    base = _commit_files(repository, {"README.md": "base\n"}, "base")
    workflow = yaml.safe_load(WORKFLOW.read_text())
    workflow["jobs"][CHART_JOB]["steps"].append(
        {
            "name": "Unregistered chart assertion",
            "run": "bash charts/curie/ci/runtime/unregistered-example-runtime.sh",
        }
    )
    head = _commit_files(
        repository,
        {
            ".github/workflows/ci.yaml": yaml.safe_dump(workflow, sort_keys=False),
            "apps/api/src/curie_api/main.py": "# candidate\n",
        },
        "add an unregistered chart assertion",
    )
    options: dict[str, Any] = {"cwd": repository}
    paths: tuple[str, ...] = ()
    if mode == "revisions":
        # The committed head must win over the clean working-tree workflow.
        (repository / ".github/workflows/ci.yaml").write_text(WORKFLOW.read_text())
        options.update(base=base, head=head)
    elif mode == "push":
        options["push"] = True
    else:
        paths = ("apps/api/src/curie_api/main.py",)
    completed, output = _invoke_selector(tmp_path, *paths, **options)
    assert completed.returncode == 0, completed.stderr
    expected = (
        CHART_IDS if mode in {"push", "revisions"} else ("quota-progress", "workspace-diagnostic")
    )
    assert json.loads(_outputs_of(output)["chart_scenarios"]) == sorted((*expected, "unregistered"))
    # An unregistered step has no leg condition, so it must run in every leg.
    assert json.loads(_outputs_of(output)["chart_legs"]) == ["chart", "workload"]
    docs, docs_output = _invoke_selector(tmp_path, "docs/example.md", cwd=repository)
    assert docs.returncode == 0, docs.stderr
    assert _outputs_of(docs_output)["chart_scenarios"] == "[]"
    assert _outputs_of(docs_output)["chart_legs"] == "[]"


@pytest.mark.parametrize(
    ("chart_scenarios", "chart_result", "expected_code"),
    [
        ("[]", "skipped", 0),
        ('["workspace-diagnostic"]', "success", 0),
        ('["workspace-diagnostic"]', "skipped", 1),
        ("[]", "success", 1),
    ],
)
def test_e2e_required_uses_chart_scenarios_for_the_chart_result(
    chart_scenarios: str,
    chart_result: str,
    expected_code: int,
) -> None:
    completed = _run_aggregate(
        event_name="pull_request",
        cluster_selected="true",
        cluster_result="success",
        chart_scenarios=chart_scenarios,
        cluster_chart_result=chart_result,
    )
    assert (completed.returncode == 0) == (expected_code == 0), completed.stdout + completed.stderr


def test_e2e_required_rejects_chart_scenarios_without_cluster() -> None:
    completed = _run_aggregate(
        chart_scenarios='["workspace-diagnostic"]',
        cluster_chart_result="success",
    )
    assert completed.returncode != 0, completed.stdout + completed.stderr


@pytest.mark.parametrize("chart_scenarios", ["", "true", "[", "]", '["workspace-diagnostic"'])
def test_e2e_required_rejects_chart_scenarios_without_list_delimiters(chart_scenarios: str) -> None:
    completed = _run_aggregate(
        chart_scenarios=chart_scenarios,
        cluster_selected="true",
        cluster_result="success",
        cluster_chart_result="success",
        event_name="pull_request",
    )
    assert completed.returncode != 0, completed.stdout + completed.stderr


def test_negative_control_still_covers_the_chart_job_result() -> None:
    state: _AggregateOverrides = {
        "event_name": "pull_request",
        "cluster_selected": "true",
        "cluster_result": "success",
        "chart_scenarios": '["workspace-diagnostic"]',
        "cluster_chart_result": "success",
    }
    unmutated = _run_aggregate(**state)
    assert unmutated.returncode == 0, unmutated.stdout + unmutated.stderr
    check = '"$CLUSTER_CHART_RESULT" != "$cluster_chart_expected" ||'

    def omit_chart_check(script: str) -> str:
        assert script.count(check) == 1
        return script.replace(check, "", 1)

    mutated = _run_aggregate(script_transform=omit_chart_check, **state)
    assert mutated.returncode != 0, mutated.stdout + mutated.stderr
    assert "Selected and skipped negative control failed" in mutated.stdout + mutated.stderr
