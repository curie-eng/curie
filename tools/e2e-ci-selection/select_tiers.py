"""Select end to end CI tiers from changed repository paths."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import shlex
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import yaml

BASE_TIERS = ("skill", "local", "local-release", "cluster")
TIERS = (*BASE_TIERS, "released-upgrade")
OUTPUT_KEYS = {
    "skill": "skill",
    "local": "local",
    "local-release": "local_release",
    "cluster": "cluster",
    "released-upgrade": "released_upgrade",
}
# Jobs behind these tiers each boot a kind cluster. Callers omit them when
# the run should not pay for that.
KIND_TIERS = frozenset({"cluster", "released-upgrade"})
# The kind-rung factory scenario runs only when one of these paths changes.
FACTORY_PREFIXES = (
    "cli/src/factory_quickstart",
    "cli/src/factory_intake.rs",
    "cli/src/api.rs",
    "cli/src/api_requests.rs",
    "cli/src/ops/convergence.rs",
    "apps/api/src/curie_api/factory_",
    "apps/api/src/curie_api/routers/agents.py",
    "apps/api/src/curie_api/routers/publications",
    "apps/api/src/curie_api/schemas/agents.py",
    "apps/api/src/curie_api/schemas/channels.py",
    "apps/api/src/curie_api/crud/channels.py",
    "apps/api/src/curie_api/workitem_dispatch.py",
    "apps/api/src/curie_api/routers/work_items.py",
    "apps/api/src/curie_api/threadkeys.py",
    "apps/api/src/curie_api/forges/",
    "apps/api/src/curie_api/forge_credential_health.py",
    "apps/api/src/curie_api/routers/forge_credential_health.py",
    "cli/src/forge_expiry.rs",
    "apps/api/tests/forge_fakes/gitlab.py",
    "apps/worker/src/curie_worker/factory",
    "apps/worker/src/curie_worker/workitem_dispatch.py",
    "apps/worker/src/curie_worker/binding.py",
    "apps/worker/src/curie_worker/kernel/lifecycle.py",
    "apps/worker/src/curie_worker/kernel/work_items.py",
    "apps/worker/src/curie_worker/kernel/claim.py",
    "apps/worker/src/curie_worker/kernel/constants.py",
    "packages/aci-protocol/src/aci_protocol/turn.py",
    "runner/src/curie_runner/verification.py",
    "runner/src/curie_runner/preflight",
    "examples/dark-factory",
    "tools/factory-e2e",
    "tools/model-script",
    "tools/github-stub",
)
# The kind approval resume scenario (#4016) runs only when a worker or approval
# path changes. Prefixes without a .py suffix match by raw string prefix, so
# "curie_api/approval" covers every approval*.py module.
APPROVAL_RESUME_PREFIXES = (
    "apps/worker/src/curie_worker",
    "apps/api/src/curie_api/resumequeue.py",
    "apps/api/src/curie_api/resumereconciler.py",
    "apps/api/src/curie_api/sweeper.py",
    "apps/api/src/curie_api/routers/approvals.py",
    "apps/api/src/curie_api/routers/approval_recovery.py",
    "apps/api/src/curie_api/approval",
    "runner/src/curie_runner/approval.py",
    "cli/scripts/e2e-cluster-approval-resume-restarts.sh",
)
UPGRADE_WORKFLOW_JOBS = frozenset(
    {
        "e2e-released-upgrade",
        "e2e-released-upgrade-negative",
        "e2e-cluster-upgrade-matrix-shards",
        "e2e-cluster-upgrade-matrix",
    }
)
WORKFLOW_PATH = ".github/workflows/ci.yaml"
REGISTRY_PATH = ".github/e2e-selection.yaml"
# A ci.yaml hunk outside every job changes the workflow header.
WORKFLOW_HEADER = "<header>"
# Registry sections that never change what pytest runs on a pull request.
NON_PYTEST_REGISTRY_SECTIONS = ("chart_scenarios", "ci_policy_tests")
PYTEST_SHARDS = 6
CHART_JOB = "e2e-cluster-chart-regressions"
CHART_LEGS = ("chart", "workload")
SCENARIO_SCRIPT = re.compile(r"^(?:charts/curie/ci|scripts)/[^\s]+\.sh(?:$|[;|&])")
REPO_ROOT = Path(__file__).resolve().parents[2]
CARGO_MANIFEST = "cli/Cargo.toml"
# A version-only diff (#3858) still runs this rung: it proves the new version
# identity end to end on the new commit.
VERSION_ONLY_TIERS = frozenset({"local-release"})
HUNK_HEADER = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")


class RegistryError(ValueError):
    """The selection registry is malformed or ambiguous."""


class UniqueKeyLoader(yaml.SafeLoader):
    """YAML loader that rejects duplicate mapping keys."""


def _construct_unique_mapping(
    loader: yaml.SafeLoader,
    node: yaml.nodes.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    loader.flatten_mapping(node)
    result: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in result
        except TypeError as exc:
            raise RegistryError("registry contains an unhashable mapping key") from exc
        if duplicate:
            raise RegistryError(f"duplicate registry key: {key}")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


@dataclass(frozen=True)
class ChartScenario:
    step: str
    inputs: tuple[str, ...]
    leg: str
    pr_inputs: tuple[str, ...] | None


@dataclass(frozen=True)
class ChartScenarios:
    job: str
    shared_inputs: tuple[str, ...]
    scenarios: dict[str, ChartScenario]
    fixtures: dict[str, tuple[str, ...]]


@dataclass(frozen=True)
class PytestScope:
    prefixes: tuple[str, ...]
    excluded_prefixes: tuple[str, ...]
    python_prefixes: tuple[str, ...]
    test_files: str


@dataclass(frozen=True)
class Registry:
    fallback: tuple[str, ...]
    exact: dict[str, tuple[str, ...]]
    prefixes: dict[str, tuple[str, ...]]
    ignored_exact: tuple[str, ...]
    ignored_prefixes: tuple[str, ...]
    chart_scenarios: ChartScenarios
    conditional_pytest_files: dict[str, tuple[str, ...]]
    pytest_scopes: dict[str, PytestScope]
    ci_policy_tests: dict[str, tuple[str, ...]]
    # The pytest_impact section (pytest_impact.ImpactConfig). None when the
    # registry has none, which keeps the full or cli scope on six shards.
    pytest_impact: Any = None


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise RegistryError(f"{label} must be a mapping")
    if any(not isinstance(key, str) for key in value):
        raise RegistryError(f"{label} keys must be strings")
    return cast(dict[str, object], value)


def _tiers(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise RegistryError(f"{label} must be a tier list")
    tiers = tuple(item for item in value if isinstance(item, str))
    if len(set(tiers)) != len(tiers):
        raise RegistryError(f"{label} contains a duplicate tier")
    unknown = sorted(set(tiers).difference(TIERS))
    if unknown:
        raise RegistryError(f"{label} contains unknown tiers: {', '.join(unknown)}")
    return tiers


def _tier_rules(value: object, label: str) -> dict[str, tuple[str, ...]]:
    rules = _mapping(value, label)
    selected: dict[str, tuple[str, ...]] = {}
    for path, tiers in rules.items():
        if not path or path.startswith("/") or path.endswith("/"):
            raise RegistryError(f"{label} contains an invalid path")
        selected_tiers = _tiers(tiers, f"{label}.{path}")
        if not selected_tiers:
            raise RegistryError(f"{label}.{path} must select at least one tier")
        selected[path] = selected_tiers
    return selected


def _matches_prefix(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(f"{prefix}/")


def _inputs(value: object, label: str) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or not value
        or any(
            not isinstance(path, str) or not path or path.startswith("/") or path.endswith("/")
            for path in value
        )
    ):
        raise RegistryError(f"{label} must be a nonempty list of valid paths")
    return tuple(value)


def _chart_registry(value: object) -> ChartScenarios:
    section = _mapping(value, "chart_scenarios")
    if set(section) != {"job", "shared_inputs", "scenarios", "fixtures"}:
        raise RegistryError(
            "chart_scenarios must define job, shared_inputs, scenarios, and fixtures"
        )
    if section["job"] != CHART_JOB:
        raise RegistryError(f"chart_scenarios.job must be {CHART_JOB}")
    shared_inputs = _inputs(section["shared_inputs"], "chart_scenarios.shared_inputs")
    entries = _mapping(section["scenarios"], "chart_scenarios.scenarios")
    if not entries:
        raise RegistryError("chart_scenarios.scenarios must not be empty")
    scenarios: dict[str, ChartScenario] = {}
    step_names: set[str] = set()
    for scenario_id, value in entries.items():
        if not re.fullmatch(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*", scenario_id):
            raise RegistryError("chart_scenarios contains an invalid scenario id")
        label = f"chart_scenarios.scenarios.{scenario_id}"
        entry = _mapping(value, label)
        if set(entry) not in ({"step", "inputs", "leg"}, {"step", "inputs", "leg", "pr_inputs"}):
            raise RegistryError(f"{label} must define step, inputs, and leg; pr_inputs is optional")
        step = entry["step"]
        if not isinstance(step, str) or not step:
            raise RegistryError(f"{label}.step must be a nonempty string")
        leg = entry["leg"]
        if not isinstance(leg, str) or leg not in CHART_LEGS:
            raise RegistryError(f"{label}.leg must be one of chart, workload")
        if step in step_names:
            raise RegistryError(f"duplicate chart scenario step: {step}")
        step_names.add(step)
        scenarios[scenario_id] = ChartScenario(
            step, _inputs(entry["inputs"], f"{label}.inputs"), leg,
            _inputs(entry["pr_inputs"], f"{label}.pr_inputs") if "pr_inputs" in entry else None,
        )
    fixtures = _mapping(section["fixtures"], "chart_scenarios.fixtures")
    commands: dict[str, tuple[str, ...]] = {}
    for step, command in fixtures.items():
        if not isinstance(step, str) or not step.strip() or step in step_names:
            raise RegistryError("chart fixture step must be nonempty and distinct from scenarios")
        if not isinstance(command, list) or any(not isinstance(word, str) for word in command):
            raise RegistryError("chart fixture command must be a list of strings")
        if len(command) == 4 and command[:2] == ["bash", "tools/runtime-assertion-gate/run.sh"]:
            script, phase = command[2:]
            valid = phase == "up"
        elif len(command) == 3 and command[0] == "bash":
            script, phase = command[1:]
            valid = phase == "down"
        else:
            raise RegistryError("chart fixture must receipt up or invoke down directly")
        if not valid or not SCENARIO_SCRIPT.fullmatch(script):
            raise RegistryError("chart fixture must name a scenario script and the correct phase")
        if not any(script in scenario.inputs for scenario in scenarios.values()):
            raise RegistryError("chart fixture must be registered as a scenario input")
        commands[step] = tuple(command)
    return ChartScenarios(CHART_JOB, shared_inputs, scenarios, commands)


def _scope_path(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or "\\" in value
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        raise RegistryError(
            f"{label} must be a repository-relative path without slashes at either end"
        )
    return value


def _scope_prefixes(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise RegistryError(f"{label} must be a nonempty list of valid paths")
    return tuple(_scope_path(path, label) for path in value)


def _pytest_scopes(value: object) -> dict[str, PytestScope]:
    scopes = _mapping(value, "pytest_scopes")
    if set(scopes) != {"cli"}:
        raise RegistryError("pytest_scopes must define exactly the cli scope")
    label = "pytest_scopes.cli"
    scope = _mapping(scopes["cli"], label)
    if set(scope) != {"prefixes", "excluded_prefixes", "python_prefixes", "test_files"}:
        raise RegistryError(
            f"{label} must define prefixes, excluded_prefixes, python_prefixes, and test_files"
        )
    prefixes = _scope_prefixes(scope["prefixes"], f"{label}.prefixes")
    excluded_prefixes = _scope_prefixes(scope["excluded_prefixes"], f"{label}.excluded_prefixes")
    python_prefixes = _scope_prefixes(scope["python_prefixes"], f"{label}.python_prefixes")
    test_files = _scope_path(scope["test_files"], f"{label}.test_files")
    try:
        tracked = subprocess.run(
            ["git", "ls-files", "--error-unmatch", "-z", "--", test_files],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.split("\0")
        if test_files not in tracked:
            raise RegistryError(f"{label}.test_files must name a tracked file: {test_files}")
        (REPO_ROOT / test_files).read_text(encoding="utf-8")
    except (OSError, UnicodeError, subprocess.CalledProcessError) as exc:
        raise RegistryError(
            f"{label}.test_files must name a readable tracked file: {test_files}"
        ) from exc
    return {"cli": PytestScope(prefixes, excluded_prefixes, python_prefixes, test_files)}


def _ci_policy_tests(value: object) -> dict[str, tuple[str, ...]]:
    entries = _mapping(value, "ci_policy_tests")
    if not entries:
        raise RegistryError("ci_policy_tests must not be empty")
    tests: dict[str, tuple[str, ...]] = {}
    for test_file, value in entries.items():
        if not re.fullmatch(
            r"tools/[A-Za-z0-9_-]+/tests/(?:[A-Za-z0-9_-]+/)*test_[A-Za-z0-9_-]+\.py",
            test_file,
        ):
            raise RegistryError("ci_policy_tests contains an invalid test file path")
        label = f"ci_policy_tests.{test_file}"
        inputs = _scope_prefixes(value, label)
        if len(set(inputs)) != len(inputs):
            raise RegistryError(f"{label} contains duplicate inputs")
        tests[test_file] = inputs
    return tests


def _matches_pytest_scope(scope: PytestScope, path: str) -> bool:
    return (
        any(path.startswith(f"{prefix}/") for prefix in scope.prefixes)
        and not any(path.startswith(f"{prefix}/") for prefix in scope.excluded_prefixes)
        and (
            not path.endswith(".py")
            or any(path.startswith(f"{prefix}/") for prefix in scope.python_prefixes)
        )
    )


# Fail-closed pytest set. Ignore rules may skip compose+pytest, but never
# for these Python or runtime paths even when a more-specific ignore exists
# (packages/test-support, apps/dispatcher, apps/ui).
MUST_RUN_PYTEST_EXACT = frozenset(
    {
        "uv.lock",
        "pyproject.toml",
        ".github/e2e-selection.yaml",
    }
)
MUST_RUN_PYTEST_PREFIXES = (
    "packages",
    "apps",
    "runner",
    "examples/tests",
    "cli",
    "tools",
    "release",
    ".github/workflows",
)
IMAGE_LOCKFILES = frozenset({"uv.lock", "pyproject.toml"})


def _is_must_run_pytest(path: str) -> bool:
    name = path.rsplit("/", 1)[-1]
    if path in MUST_RUN_PYTEST_EXACT or name in MUST_RUN_PYTEST_EXACT:
        return True
    return any(_matches_prefix(path, prefix) for prefix in MUST_RUN_PYTEST_PREFIXES)


def _needs_pytest(registry: Registry, paths: list[str]) -> bool:
    if not paths:
        return True
    for path in paths:
        if _is_must_run_pytest(path):
            return True
        if not _is_ignored(registry, path):
            return True
    return False


def _needs_images(paths: list[str]) -> bool:
    if not paths:
        return True
    for path in paths:
        name = path.rsplit("/", 1)[-1]
        if path in IMAGE_LOCKFILES or name in IMAGE_LOCKFILES:
            return True
        if "Dockerfile" in name or name.endswith(".dockerfile"):
            return True
        # The shell supervisor is proven only inside the shell image.
        if path.startswith("packages/shell-supervisor/") or path in {
            "runner/src/curie_runner/session.py",
            "runner/src/curie_runner/server.py",
        }:
            return True
    return False


RUNTIME_ASSERTION_DIR = "charts/curie/ci/runtime"


def _is_runtime_assertion(path: str) -> bool:
    parent, _, name = path.rpartition("/")
    return (path == "charts/curie/ci/scenarios/workload-install.sh"
            or parent == RUNTIME_ASSERTION_DIR and name.endswith(".sh"))


def _matches_scenario(path: str, prefixes: tuple[str, ...]) -> bool:
    for prefix in prefixes:
        if path == prefix or path.startswith(f"{prefix}/"):
            return True
        if not prefix.endswith((".py", ".sh", ".rs")) and path.startswith(prefix):
            return True
    return False


def _needs_factory(paths: list[str]) -> bool:
    if not paths:
        return True
    return any(_matches_scenario(path, FACTORY_PREFIXES) for path in paths)


def _needs_approval_resume(paths: list[str]) -> bool:
    if not paths:
        return True
    return any(_matches_scenario(path, APPROVAL_RESUME_PREFIXES) for path in paths)


def _needs_cli_release(paths: list[str]) -> bool:
    if not paths:
        return True
    return any(path == "cli" or path.startswith("cli/") for path in paths)


def _load_registry(path: Path) -> Registry:
    with path.open(encoding="utf-8") as stream:
        document = yaml.load(stream, Loader=UniqueKeyLoader)
    root = _mapping(document, "registry")
    if type(root.get("version")) is not int or root["version"] != 1:
        raise RegistryError("registry version must be 1")

    fallback = _tiers(root.get("fallback"), "fallback")
    if fallback != BASE_TIERS:
        raise RegistryError("fallback must contain every base tier in canonical order")

    rules = _mapping(root.get("rules"), "rules")
    if set(rules) != {"exact", "prefixes", "ignored_exact", "ignored_prefixes"}:
        raise RegistryError(
            "rules must define exact, prefixes, ignored_exact, and ignored_prefixes"
        )
    exact = _tier_rules(rules["exact"], "rules.exact")
    prefixes = _tier_rules(rules["prefixes"], "rules.prefixes")
    ignored_exact_rules = _mapping(rules["ignored_exact"], "rules.ignored_exact")
    ignored_exact: list[str] = []
    for ignored, value in ignored_exact_rules.items():
        if not ignored or ignored.startswith("/") or ignored.endswith("/"):
            raise RegistryError("rules.ignored_exact contains an invalid path")
        if value != []:
            raise RegistryError("ignored exact values must be empty lists")
        if ignored in exact or ignored in prefixes:
            raise RegistryError(f"ignored exact path overlaps a selected rule: {ignored}")
        ignored_exact.append(ignored)

    ignored_rules = _mapping(rules["ignored_prefixes"], "rules.ignored_prefixes")

    ignored_prefixes: list[str] = []
    for ignored, value in ignored_rules.items():
        if not ignored or ignored.startswith("/") or ignored.endswith("/"):
            raise RegistryError("rules.ignored_prefixes contains an invalid path")
        if value != []:
            raise RegistryError("ignored prefix values must be empty lists")
        ignored_prefixes.append(ignored)

    for ignored in ignored_prefixes:
        # Exact paths can opt back in, but selected prefixes cannot be hidden.
        # A more-specific ignored child of a selected prefix is a hole, not an overlap.
        if any(_matches_prefix(prefix, ignored) for prefix in prefixes):
            raise RegistryError(f"ignored prefix overlaps a selected rule: {ignored}")

    chart_scenarios = _chart_registry(root.get("chart_scenarios"))
    conditional_entries = _mapping(root.get("conditional_pytest_files"), "conditional_pytest_files")
    conditional_pytest_files: dict[str, tuple[str, ...]] = {}
    for test_file, value in conditional_entries.items():
        if not test_file or test_file.startswith("/") or test_file.endswith("/"):
            raise RegistryError("conditional_pytest_files contains an invalid path")
        label = f"conditional_pytest_files.{test_file}"
        inputs = _inputs(value, label)
        if len(set(inputs)) != len(inputs):
            raise RegistryError(f"{label} contains duplicate inputs")
        conditional_pytest_files[test_file] = inputs
    pytest_scopes = _pytest_scopes(root.get("pytest_scopes"))
    ci_policy_tests = _ci_policy_tests(root.get("ci_policy_tests"))
    pytest_impact = None
    if "pytest_impact" in root:
        try:
            pytest_impact = _impact().load_config(root)
        except ValueError as exc:
            raise RegistryError(str(exc)) from exc
    return Registry(
        fallback,
        exact,
        prefixes,
        tuple(ignored_exact),
        tuple(ignored_prefixes),
        chart_scenarios,
        conditional_pytest_files,
        pytest_scopes,
        ci_policy_tests,
        pytest_impact,
    )


def _is_ignored(registry: Registry, path: str) -> bool:
    if path in registry.exact:
        return False
    return path in registry.ignored_exact or any(
        _matches_prefix(path, prefix) for prefix in registry.ignored_prefixes
    )


def _select_path(registry: Registry, path: str) -> set[str]:
    if _is_ignored(registry, path):
        return set()

    selected: set[str] = set()
    matched = False
    if path in registry.exact:
        matched = True
        selected.update(registry.exact[path])
    for prefix, tiers in registry.prefixes.items():
        if _matches_prefix(path, prefix):
            matched = True
            selected.update(tiers)
    return selected if matched else set(registry.fallback)


def _chart_steps(job: str, head: str | None) -> list[dict[str, object]]:
    if head is None:
        content = Path(WORKFLOW_PATH).read_text(encoding="utf-8")
    else:
        content = subprocess.run(
            ["git", "show", f"{head}:{WORKFLOW_PATH}"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    try:
        document = yaml.load(content, Loader=UniqueKeyLoader)
    except yaml.YAMLError as exc:
        raise RegistryError("workflow YAML is malformed") from exc
    if not isinstance(document, dict):
        raise RegistryError("workflow root must be a mapping")
    jobs = _mapping(document.get("jobs"), "workflow jobs")
    selected_job = _mapping(jobs.get(job), f"workflow job {job}")
    steps = selected_job.get("steps")
    if not isinstance(steps, list):
        raise RegistryError(f"workflow job {job} must define a steps list")
    return [_mapping(step, f"workflow job {job} step") for step in steps]


def _is_scenario_step(step: dict[str, object]) -> bool:
    run = step.get("run")
    if not isinstance(run, str):
        return False
    for line in run.replace("\\\n", " ").splitlines():
        try:
            words = shlex.split(line, comments=True)
        except ValueError:
            continue
        if any(SCENARIO_SCRIPT.match(word) for word in words):
            return True
    return False


def _select_chart_scenarios(
    registry: Registry,
    paths: list[str],
    selected_tiers: set[str],
    push: bool,
    head: str | None,
) -> list[str]:
    if "cluster" not in selected_tiers:
        return []
    chart = registry.chart_scenarios
    fallback = any(
        not _is_ignored(registry, path)
        and path not in registry.exact
        and not any(_matches_prefix(path, prefix) for prefix in registry.prefixes)
        for path in paths
    )
    selected = {
        scenario_id
        for scenario_id, scenario in chart.scenarios.items()
        if push or fallback or any(
            _matches_scenario(
                path, (*chart.shared_inputs, *scenario.inputs, *(scenario.pr_inputs or ()))
            ) for path in paths
        )
    }
    registered_steps = {scenario.step for scenario in chart.scenarios.values()}
    def registered_fixture(step: dict[str, object]) -> bool:
        command = chart.fixtures.get(str(step.get("name")))
        run = step.get("run")
        if command is None or not isinstance(run, str):
            return False
        try:
            return tuple(shlex.split(run, comments=True)) == command
        except ValueError:
            return False

    if any(
        _is_scenario_step(step) and step.get("name") not in registered_steps
        and not registered_fixture(step)
        for step in _chart_steps(chart.job, head)
    ):
        selected.add("unregistered")
    return sorted(selected)


def _chart_legs(registry: Registry, chart_scenarios: list[str]) -> list[str]:
    # An unregistered step carries no leg condition, so it runs in whichever leg
    # runs. Selecting every leg keeps the fail-closed behavior.
    if "unregistered" in chart_scenarios:
        return list(CHART_LEGS)
    scenarios = registry.chart_scenarios.scenarios
    return sorted({scenarios[item].leg for item in chart_scenarios if item in scenarios})


def _changed_paths(base: str, head: str) -> list[str]:
    completed = subprocess.run(
        ["git", "diff", "--no-renames", "--name-only", f"{base}...{head}"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [line for line in completed.stdout.splitlines() if line]


def _workflow_job_spans(content: str) -> dict[str, tuple[int, int]]:
    """Line spans [beginning, end) of every workflow job, 1-based.

    A job owns its key line through the line before the next job key; the last
    job runs to the end of the file. Malformed workflows and an aliased
    released-upgrade job raise RegistryError.
    """
    try:
        root = yaml.compose(content)
    except yaml.YAMLError as exc:
        raise RegistryError("workflow YAML is malformed") from exc
    if not isinstance(root, yaml.nodes.MappingNode):
        raise RegistryError("workflow root must be a mapping")

    root_keys: set[str] = set()
    jobs_node: yaml.nodes.MappingNode | None = None
    for key, value in root.value:
        if not isinstance(key, yaml.nodes.ScalarNode):
            raise RegistryError("workflow root keys must be strings")
        if key.value in root_keys:
            raise RegistryError(f"duplicate workflow root key: {key.value}")
        root_keys.add(key.value)
        if key.value == "jobs":
            if not isinstance(value, yaml.nodes.MappingNode):
                raise RegistryError("workflow jobs must be a mapping")
            jobs_node = value
        elif isinstance(value, yaml.nodes.MappingNode):
            if any(
                isinstance(nested_key, yaml.nodes.ScalarNode)
                and nested_key.value in UPGRADE_WORKFLOW_JOBS
                for nested_key, _ in value.value
            ):
                raise RegistryError("upgrade job moved outside workflow jobs")
    if jobs_node is None:
        raise RegistryError("workflow jobs mapping is missing")

    spans: dict[str, tuple[int, int]] = {}
    seen_jobs: set[str] = set()
    for index, (key, value) in enumerate(jobs_node.value):
        if not isinstance(key, yaml.nodes.ScalarNode):
            raise RegistryError("workflow job keys must be strings")
        if key.value in seen_jobs:
            raise RegistryError(f"duplicate workflow job: {key.value}")
        seen_jobs.add(key.value)
        if key.value in UPGRADE_WORKFLOW_JOBS and not isinstance(value, yaml.nodes.MappingNode):
            raise RegistryError(f"workflow job {key.value} must be a mapping")
        beginning = key.start_mark.line + 1
        if index + 1 < len(jobs_node.value):
            next_key, _ = jobs_node.value[index + 1]
            end = next_key.start_mark.line + 1
        else:
            end = len(content.splitlines()) + 1
        # Two flow-style job keys can share a physical line. Both own it.
        spans[key.value] = (beginning, max(beginning + 1, end))

    # PyYAML resolves aliases to the anchor's source node and keeps the anchor's
    # marks. Until we track anchor dependencies, refuse an aliased target job
    # instead of silently treating its external definition as unrelated.
    upgrade_spans = [span for job, span in spans.items() if job in UPGRADE_WORKFLOW_JOBS]
    try:
        alias_lines = (
            event.start_mark.line + 1
            for event in yaml.parse(content)
            if isinstance(event, yaml.events.AliasEvent) and event.start_mark is not None
        )
        if any(beginning <= line < end for line in alias_lines for beginning, end in upgrade_spans):
            raise RegistryError("released-upgrade job uses a YAML alias")
    except yaml.YAMLError as exc:
        raise RegistryError("workflow YAML is malformed") from exc
    return spans


def _git_show(revision: str, path: str) -> str:
    return subprocess.run(
        ["git", "show", f"{revision}:{path}"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def _merge_base(base: str, head: str) -> str:
    return subprocess.run(
        ["git", "merge-base", base, head],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def _workflow_changes(base: str, head: str) -> tuple[bool, frozenset[str]]:
    """Does the ci.yaml diff touch a released-upgrade job, and which jobs it touches.

    The job set names every job whose span a hunk overlaps on either side, plus
    WORKFLOW_HEADER when a hunk covers a line outside every job.
    """
    merge_base = _merge_base(base, head)
    old_content = _git_show(merge_base, WORKFLOW_PATH)
    new_content = _git_show(head, WORKFLOW_PATH)
    old_spans = _workflow_job_spans(old_content)
    new_spans = _workflow_job_spans(new_content)
    if not any(job in UPGRADE_WORKFLOW_JOBS for spans in (old_spans, new_spans) for job in spans):
        raise RegistryError("workflow has no released-upgrade job anchors")

    diff = subprocess.run(
        ["git", "diff", "--no-renames", "--unified=0", merge_base, head, "--", WORKFLOW_PATH],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    hunk_lines = [line for line in diff.splitlines() if line.startswith("@@")]
    if not hunk_lines:
        raise RegistryError("changed workflow has no text hunks")
    touched: set[str] = set()
    for line in hunk_lines:
        match = HUNK_HEADER.match(line)
        if match is None:
            raise RegistryError(f"malformed workflow diff hunk: {line}")
        old_start, old_count, new_start, new_count = match.groups()
        for start, count, spans in (
            (int(old_start), int(old_count or "1"), old_spans),
            (int(new_start), int(new_count or "1"), new_spans),
        ):
            if not count:
                continue
            covered = [
                job
                for job, (beginning, end) in spans.items()
                if start < end and start + count > beginning
            ]
            touched.update(covered)
            lines = range(start, start + count)
            if any(
                not any(beginning <= number < end for beginning, end in spans.values())
                for number in lines
            ):
                touched.add(WORKFLOW_HEADER)
    return bool(touched & UPGRADE_WORKFLOW_JOBS), frozenset(touched)


def _changes_upgrade_workflow_jobs(base: str, head: str) -> bool:
    return _workflow_changes(base, head)[0]


def _registry_without_non_pytest_sections(content: str) -> object:
    document = yaml.load(content, Loader=UniqueKeyLoader)
    if isinstance(document, dict):
        for section in NON_PYTEST_REGISTRY_SECTIONS:
            document.pop(section, None)
    return document


def _registry_pytest_changed(base: str, head: str) -> bool:
    """Does the registry diff change anything that decides what pytest runs?

    A base or head that is missing or does not parse counts as changed.
    """
    try:
        merge_base = _merge_base(base, head)
        old = _registry_without_non_pytest_sections(_git_show(merge_base, REGISTRY_PATH))
        new = _registry_without_non_pytest_sections(_git_show(head, REGISTRY_PATH))
    except (subprocess.CalledProcessError, yaml.YAMLError, RegistryError):
        return True
    return old != new


@lru_cache(maxsize=1)
def _impact() -> ModuleType:
    """Load pytest_impact.py by path; it owns the impact graph and shard plan."""
    path = Path(__file__).resolve().with_name("pytest_impact.py")
    name = "e2e_ci_selection_pytest_impact"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Dataclasses resolve their module through sys.modules while the class body runs.
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_atlas() -> ModuleType:
    """Load release/atlas.py by path; it owns the version-only path set."""
    path = REPO_ROOT / "release" / "atlas.py"
    spec = importlib.util.spec_from_file_location("release_atlas", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_atlas = _load_atlas()


def _head_version(head: str | None) -> str | None:
    """The CLI version at `head` (revision mode) or in this checkout (path mode).

    None when the manifest cannot be read, which keeps today's selection.
    """
    if head is None:
        try:
            manifest = (REPO_ROOT / CARGO_MANIFEST).read_text(encoding="utf-8")
        except OSError:
            return None
    else:
        completed = subprocess.run(
            ["git", "show", f"{head}:{CARGO_MANIFEST}"],
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            return None
        manifest = completed.stdout
    try:
        version = tomllib.loads(manifest)["package"]["version"]
    except (tomllib.TOMLDecodeError, KeyError, TypeError):
        return None
    return version if isinstance(version, str) and version else None


def _is_version_only(paths: list[str], base: str | None, head: str | None) -> bool:
    """Does the diff change only version identity for the head's release (#3858)?

    With revisions, a schema-window catalog change that only registers the
    head's release window is version identity too (ADR-0257). Path mode has no
    revisions to compare, so it keeps the plain path set.
    """
    if not paths:
        return False
    version = _head_version(head)
    if version is None:
        return False
    tag = f"v{version}"
    if base is None or head is None:
        return set(paths) <= _atlas.version_only_paths(tag)
    try:
        return not _atlas.version_only_extra(paths, tag, base, head)
    except _atlas.AtlasError:
        return False


@dataclass(frozen=True)
class PytestPlan:
    """The pytest scope, its impact targets, and the shard plan."""

    scope: str
    reason: str
    targets: tuple[str, ...]
    shards: int
    storage_shards: int
    full_shards: int


def _cli_list_files(registry: Registry) -> list[str]:
    text = (REPO_ROOT / registry.pytest_scopes["cli"].test_files).read_text(encoding="utf-8")
    return [
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


@lru_cache(maxsize=1)
def _impact_graph() -> Any:
    # The graph is always this checkout's, wherever the selector runs from.
    return _impact().ImpactGraph.build(REPO_ROOT)


def _pytest_needed(
    registry: Registry,
    paths: list[str],
    *,
    pull_request: bool,
    workflow_jobs_changed: frozenset[str] | None,
    registry_pytest_changed: bool | None,
) -> bool:
    """The path rules' answer, widened on a pull request by impact readers.

    A pull request whose paths the rules skip (documentation, root docs) still
    runs pytest when a test reads one of them or the always-selected set is
    not empty.
    """
    if _needs_pytest(registry, paths):
        return True
    if not pull_request or registry.pytest_impact is None:
        return False
    return _impact().needs_pytest(
        _impact_graph(),
        registry.pytest_impact,
        paths,
        cli_files=_cli_list_files(registry),
        workflow_jobs_changed=workflow_jobs_changed,
        registry_pytest_changed=registry_pytest_changed,
    )


def _pytest_plan(
    registry: Registry,
    paths: list[str],
    scope: str,
    *,
    pull_request: bool,
    pytest_needed: bool,
    workflow_jobs_changed: frozenset[str] | None,
    registry_pytest_changed: bool | None,
) -> PytestPlan:
    """Narrow a pull request's full scope to impact targets and plan its shards.

    Every other event, a run without pytest, or a registry without a
    pytest_impact section keeps `scope` on all six shards at the full tier.
    """
    config = registry.pytest_impact
    if not pull_request:
        reason = "not a pull request"
    elif not pytest_needed:
        reason = "pytest not selected"
    elif config is None:
        reason = "no pytest_impact registry section"
    else:
        reason = ""
    if reason:
        return PytestPlan(scope, reason, (), PYTEST_SHARDS, PYTEST_SHARDS, PYTEST_SHARDS)
    impact = _impact()
    graph = _impact_graph()
    cli_files = _cli_list_files(registry)
    targets: tuple[str, ...] = ()
    if scope == "cli":
        reason = "cli scope"
        files = [path for path in cli_files if path in graph.tracked]
    elif not paths:
        reason = "empty diff"
        files = list(graph.test_files)
    else:
        selection = impact.select(
            graph,
            config,
            paths,
            cli_files=cli_files,
            workflow_jobs_changed=workflow_jobs_changed,
            registry_pytest_changed=registry_pytest_changed,
        )
        reason = selection.reason
        if selection.everything:
            files = list(graph.test_files)
        else:
            scope = "impact"
            targets = selection.targets
            files = list(targets)
    files = [path for path in files if not any(_matches_prefix(path, p) for p in config.excluded)]
    plan = impact.plan_shards(graph, config, files, impact.conftest_groups(REPO_ROOT))
    return PytestPlan(scope, reason, targets, plan.shards, plan.storage_shards, plan.full_shards)


def _render(
    selected: set[str],
    pytest_needed: bool,
    pytest_deselect: list[str],
    pytest_scope: str,
    ci_policy_files: list[str],
    images_needed: bool,
    cli_release_needed: bool,
    released_upgrade_full: bool,
    version_only: bool,
    factory_needed: bool,
    approval_resume_needed: bool,
    chart_scenarios: list[str],
    chart_legs: list[str],
    pytest_plan: PytestPlan,
) -> str:
    lines = [f"{OUTPUT_KEYS[tier]}={'true' if tier in selected else 'false'}" for tier in TIERS]
    skill_local = ",".join(tier for tier in TIERS[:2] if tier in selected)
    lines.append(f"skill_local_tiers={skill_local}")
    lines.append(f"pytest={'true' if pytest_needed else 'false'}")
    lines.append("pytest_deselect=" + ",".join(pytest_deselect))
    lines.append(f"pytest_scope={pytest_scope}")
    lines.append(f"ci_policy={'true' if ci_policy_files else 'false'}")
    lines.append("ci_policy_files=" + ",".join(ci_policy_files))
    lines.append(f"images={'true' if images_needed else 'false'}")
    lines.append(f"cli_release={'true' if cli_release_needed else 'false'}")
    lines.append(f"released_upgrade_full={'true' if released_upgrade_full else 'false'}")
    lines.append(f"version_only={'true' if version_only else 'false'}")
    lines.append(f"factory={'true' if factory_needed else 'false'}")
    lines.append(f"approval_resume={'true' if approval_resume_needed else 'false'}")
    lines.append("chart_scenarios=" + json.dumps(chart_scenarios, separators=(",", ":")))
    lines.append("chart_legs=" + json.dumps(chart_legs, separators=(",", ":")))
    digest = _impact().targets_digest(pytest_plan.targets) if pytest_plan.targets else ""
    lines.append(f"pytest_targets={digest}")
    lines.append(f"pytest_shards={pytest_plan.shards}")
    lines.append(f"pytest_storage_shards={pytest_plan.storage_shards}")
    lines.append(f"pytest_full_shards={pytest_plan.full_shards}")
    return "\n".join(lines) + "\n"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", required=True, type=Path)
    parser.add_argument("--path", action="append", default=[])
    parser.add_argument("--base")
    parser.add_argument("--head")
    parser.add_argument("--push", action="store_true")
    parser.add_argument(
        "--full-pytest",
        action="store_true",
        help=(
            "Select tiers from the revision diff but run pytest on its full scope and "
            "shard plan whenever a pull request would run it, with no CLI scope, "
            "conditional deselection or impact narrowing (the merge queue)."
        ),
    )
    parser.add_argument(
        "--pytest-targets-out",
        type=Path,
        help="Write the impact targets one per line (empty unless the scope is impact).",
    )
    parser.add_argument(
        "--omit-kind",
        action="store_true",
        help="Drop the cluster and released-upgrade tiers.",
    )
    return parser


def _run() -> None:
    args = _parser().parse_args()
    registry = _load_registry(args.registry)

    paths: list[str] = []
    workflow_upgrade_changed = False
    workflow_jobs_changed: frozenset[str] | None = None
    registry_pytest_changed: bool | None = None
    version_only = False
    factory_needed = False
    approval_resume_needed = False
    if args.push:
        if args.path or args.base or args.head:
            raise RegistryError("push cannot be combined with paths or revisions")
        selected = set(TIERS)
        pytest_needed = True
        images_needed = True
        cli_release_needed = True
        factory_needed = True
        approval_resume_needed = True
    else:
        if args.path and (args.base or args.head):
            raise RegistryError("paths cannot be combined with revisions")
        if args.path:
            paths = args.path
        elif args.base and args.head:
            paths = _changed_paths(args.base, args.head)
            workflow_jobs_changed = frozenset()
            if WORKFLOW_PATH in paths:
                workflow_upgrade_changed, workflow_jobs_changed = _workflow_changes(
                    args.base, args.head
                )
            registry_pytest_changed = REGISTRY_PATH in paths and _registry_pytest_changed(
                args.base, args.head
            )
        else:
            raise RegistryError("provide paths, push, or both base and head revisions")
        selected = set().union(*(_select_path(registry, path) for path in paths))
        # The merge queue runs pytest whenever the pull request would, so it
        # keeps the impact readers' widening; it never narrows what runs.
        pytest_needed = _pytest_needed(
            registry,
            paths,
            pull_request=True,
            workflow_jobs_changed=workflow_jobs_changed,
            registry_pytest_changed=registry_pytest_changed,
        )
        images_needed = _needs_images(paths)
        cli_release_needed = _needs_cli_release(paths)
        version_only = _is_version_only(
            paths,
            args.base if not args.path else None,
            args.head if not args.path else None,
        )
        factory_needed = _needs_factory(paths)
        if factory_needed:
            selected.add("cluster")
        approval_resume_needed = _needs_approval_resume(paths)
        if approval_resume_needed:
            selected.add("cluster")

    if args.omit_kind:
        selected.difference_update(KIND_TIERS)
        # An added or changed cluster runtime assertion must run on the enforcing
        # cluster rung before `E2E required` can pass (#3391), so omitting kind
        # never drops the tier that proves it.
        if any(_is_runtime_assertion(path) for path in paths):
            selected.add("cluster")
        factory_needed = False
        approval_resume_needed = False
    if workflow_upgrade_changed:
        selected.add("released-upgrade")
    if version_only:
        # A release bump changes only version identity, so the proof the base
        # already has still holds (#3858). Run the rung that exercises the new
        # version, and leave pytest to the required job's release tests.
        selected = set(VERSION_ONLY_TIERS)
        pytest_needed = False

    if factory_needed:
        images_needed = True
        cli_release_needed = True

    pytest_deselect = []
    if (
        not args.push
        and not args.full_pytest
        and paths
        and pytest_needed
        and all(
            path in registry.exact
            or path in registry.ignored_exact
            or any(
                _matches_prefix(path, prefix)
                for prefix in (*registry.prefixes, *registry.ignored_prefixes)
            )
            for path in paths
        )
    ):
        pytest_deselect = sorted(
            test_file
            for test_file, inputs in registry.conditional_pytest_files.items()
            if not any(_matches_prefix(path, prefix) for path in paths for prefix in inputs)
        )

    pytest_scope = "full"
    if (
        not args.push
        and not args.full_pytest
        and paths
        and pytest_needed
        and all(_matches_pytest_scope(registry.pytest_scopes["cli"], path) for path in paths)
    ):
        pytest_scope = "cli"

    ci_policy_files = sorted(
        test_file
        for test_file, inputs in registry.ci_policy_tests.items()
        if args.push or any(_matches_prefix(path, prefix) for path in paths for prefix in inputs)
    )

    # Dispatch callers use --push without --omit-kind for the full matrix.
    # Pull requests selecting released-upgrade run the smoke shard.
    released_upgrade_full = args.push and not args.omit_kind
    chart_scenarios = _select_chart_scenarios(
        registry, paths, selected, args.push, args.head if not args.path else None
    )
    chart_legs = _chart_legs(registry, chart_scenarios)
    print("chart_scenarios=" + json.dumps(chart_scenarios, separators=(",", ":")))
    print("chart_legs=" + json.dumps(chart_legs, separators=(",", ":")))

    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        raise RegistryError("GITHUB_OUTPUT is required")
    pytest_plan = _pytest_plan(
        registry,
        paths,
        pytest_scope,
        pull_request=not (args.push or args.full_pytest),
        pytest_needed=pytest_needed,
        workflow_jobs_changed=workflow_jobs_changed,
        registry_pytest_changed=registry_pytest_changed,
    )
    print(
        f"pytest selection: scope={pytest_plan.scope} reason={pytest_plan.reason} "
        f"targets={len(pytest_plan.targets)} shards={pytest_plan.shards} "
        f"storage={pytest_plan.storage_shards} full={pytest_plan.full_shards}"
    )
    if args.pytest_targets_out is not None:
        args.pytest_targets_out.write_text(
            "".join(f"{target}\n" for target in pytest_plan.targets), encoding="utf-8"
        )
    with Path(output_path).open("a", encoding="utf-8") as stream:
        stream.write(
            _render(
                selected,
                pytest_needed,
                pytest_deselect,
                pytest_plan.scope,
                ci_policy_files,
                images_needed,
                cli_release_needed,
                released_upgrade_full,
                version_only,
                factory_needed,
                approval_resume_needed,
                chart_scenarios,
                chart_legs,
                pytest_plan,
            )
        )


def main() -> int:
    _run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
