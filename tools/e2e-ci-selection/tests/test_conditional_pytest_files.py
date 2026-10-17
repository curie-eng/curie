"""Issue #4467: conditional files keep collection and shard assignment unchanged."""

from __future__ import annotations

import ast
import importlib.util
import subprocess
import sys
import tomllib
from pathlib import Path
from types import ModuleType

import pytest
import yaml


def _load_test_module(name: str) -> ModuleType:
    path = Path(__file__).resolve().parent / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"conditional_{name}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


SELECTION_TESTS = _load_test_module("test_e2e_ci_selection")
PYTEST_TESTS = _load_test_module("test_python_pytest_selection")
REGISTRY = SELECTION_TESTS.REGISTRY
REPO_ROOT = SELECTION_TESTS.REPO_ROOT
SRE_RUNTIME = SELECTION_TESTS.SRE_RUNTIME
SRE_RUNTIME_INPUTS = SELECTION_TESTS.SRE_RUNTIME_INPUTS
C4_TRACKER_SCOPE = SELECTION_TESTS.C4_TRACKER_SCOPE
C4_DRY_RUN = SELECTION_TESTS.C4_DRY_RUN
CONDITIONAL_PYTEST_FILES = SELECTION_TESTS.CONDITIONAL_PYTEST_FILES
ALL_CONDITIONAL = SELECTION_TESTS.ALL_CONDITIONAL
_invoke_selector = SELECTION_TESTS._invoke_selector
_load_selector_module = SELECTION_TESTS._load_selector_module
_run_pytest_step = PYTEST_TESTS._run_pytest_step


def _selection(tmp_path: Path, *paths: str, **options: object) -> dict[str, str]:
    completed, output = _invoke_selector(tmp_path, *paths, **options)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    lines = output.splitlines()
    assert sum(line.startswith("pytest_deselect=") for line in lines) == 1
    return dict(line.split("=", 1) for line in lines)


@pytest.mark.parametrize(
    "path",
    [
        "apps/api/src/curie_api/main.py",
        "apps/worker/src/curie_worker/kernel.py",
        "cli/src/ops/up.rs",
        "charts/curie/templates/_helpers.tpl",
        "packages/channel-protocol/pyproject.toml",
    ],
)
def test_sre_runtime_deselected_for_unrelated_paths(tmp_path: Path, path: str) -> None:
    output = _selection(tmp_path, path)
    assert output["pytest"] == "true"
    assert "examples/tests/test_sre_bot_observability_runtime.py" in output[
        "pytest_deselect"
    ].split(",")


@pytest.mark.parametrize(
    "paths",
    [
        ("examples/sre-bot/observability/tempo.yaml",),
        ("examples/sre-bot/connectors/tempo/server.py",),
        ("examples/tests/test_sre_bot_observability_runtime.py",),
        ("runner/Dockerfile",),
        ("packages/plugin-format/src/plugin_format/connector_render.py",),
        ("packages/telemetry-schema/pyproject.toml",),
        ("otel/collector-config.yaml",),
        ("charts/curie/values.yaml",),
        ("uv.lock",),
        ("pyproject.toml",),
        ("conftest.py",),
        ("cli/scripts/configure-docker-hub-mirror.sh",),
        (".github/workflows/ci.yaml",),
        (".github/e2e-selection.yaml",),
        ("tools/e2e-ci-selection/select_tiers.py",),
        ("cli/src/ops/up.rs", "runner/Dockerfile"),
    ],
)
def test_sre_runtime_selected_when_an_input_changes(tmp_path: Path, paths: tuple[str, ...]) -> None:
    assert SRE_RUNTIME not in _selection(tmp_path, *paths)["pytest_deselect"].split(",")


@pytest.mark.parametrize(
    "path",
    [
        "apps/api/src/curie_api/main.py",
        "apps/api/src/curie_api/forges-other/config.py",
        "examples/sre-bot/observability/tempo.yaml",
        "runner/Dockerfile",
    ],
)
def test_c4_tracker_scope_deselected_for_unrelated_paths(tmp_path: Path, path: str) -> None:
    assert C4_TRACKER_SCOPE in _selection(tmp_path, path)["pytest_deselect"].split(",")


@pytest.mark.parametrize(
    "path",
    [
        "charts/curie/templates/_helpers.tpl",
        "cli/src/factory_intake.rs",
        "apps/api/src/curie_api/forges/config.py",
        "tests/vectors/forge-tracker-scopes.json",
        C4_TRACKER_SCOPE,
    ],
)
def test_c4_tracker_scope_selected_when_an_input_changes(tmp_path: Path, path: str) -> None:
    assert C4_TRACKER_SCOPE not in _selection(tmp_path, path)["pytest_deselect"].split(",")


@pytest.mark.parametrize(
    "path",
    [
        "apps/api/src/curie_api/main.py",
        "apps/worker/src/curie_worker/main.py",
        "examples/sre-bot/observability/tempo.yaml",
        "runner/Dockerfile",
    ],
)
def test_c4_dry_run_deselected_for_unrelated_paths(tmp_path: Path, path: str) -> None:
    assert C4_DRY_RUN in _selection(tmp_path, path)["pytest_deselect"].split(",")


@pytest.mark.parametrize(
    "path",
    [
        "cli/src/ops/up.rs",
        "cli/src/factory_quickstart/native.rs",
        "charts/curie/files/agent-sandbox/controller.yaml",
        "apps/api/Dockerfile",
        C4_DRY_RUN,
    ],
)
def test_c4_dry_run_selected_when_an_input_changes(tmp_path: Path, path: str) -> None:
    assert C4_DRY_RUN not in _selection(tmp_path, path)["pytest_deselect"].split(",")


@pytest.mark.parametrize(
    "paths",
    [
        ("unclassified-top-level/file.txt",),
        ("cli/src/ops/up.rs", "unclassified-top-level/file.txt"),
    ],
)
def test_unclassified_path_keeps_conditional_files(tmp_path: Path, paths: tuple[str, ...]) -> None:
    output = _selection(tmp_path, *paths)
    assert output["pytest"] == "true"
    assert output["pytest_deselect"] == ""


@pytest.mark.parametrize("omit_kind", [False, True])
def test_push_never_deselects_conditional_files(tmp_path: Path, omit_kind: bool) -> None:
    assert _selection(tmp_path, push=True, omit_kind=omit_kind)["pytest_deselect"] == ""


def test_empty_revision_diff_keeps_conditional_files(tmp_path: Path) -> None:
    output = _selection(tmp_path, base="HEAD", head="HEAD")
    assert output["pytest"] == "true"
    assert output["pytest_deselect"] == ""


@pytest.mark.parametrize("path", ["README.md", "docs/guides/getting-started.md"])
def test_pytest_false_has_no_conditional_deselection(tmp_path: Path, path: str) -> None:
    output = _selection(tmp_path, path)
    assert output["pytest"] == "false"
    assert output["pytest_deselect"] == ""


def test_version_only_prep_has_no_conditional_deselection(tmp_path: Path) -> None:
    cargo = tomllib.loads((REPO_ROOT / "cli" / "Cargo.toml").read_text())
    version = cargo["package"]["version"]
    output = _selection(
        tmp_path,
        "charts/curie/Chart.yaml",
        "cli/Cargo.toml",
        "cli/Cargo.lock",
        "docs/architecture-atlas/versions.json",
        f"docs/architecture-atlas/snapshots/v{version}.json",
    )
    assert output["version_only"] == "true"
    assert output["pytest"] == "false"
    assert output["pytest_deselect"] == ""


@pytest.mark.parametrize("ignored", ["README.md", "docs/example.md", "tools/example.py"])
def test_classified_ignored_path_does_not_keep_an_unrelated_file(
    tmp_path: Path, ignored: str
) -> None:
    output = _selection(tmp_path, "apps/api/src/curie_api/main.py", ignored)
    assert output["pytest"] == "true"
    assert output["pytest_deselect"] == ALL_CONDITIONAL


@pytest.mark.parametrize("path", ["examples/sre-bot-other/file.yaml", "runner-other/file.py"])
def test_conditional_inputs_respect_directory_boundaries(tmp_path: Path, path: str) -> None:
    # Classify the sibling explicitly so fallback cannot mask a prefix-match bug.
    registry = yaml.safe_load(REGISTRY.read_text())
    registry["rules"]["exact"][path] = ["local"]
    registry_path = tmp_path / "registry.yaml"
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False))
    assert _selection(tmp_path, path, registry=registry_path)["pytest_deselect"] == ALL_CONDITIONAL


def test_conditional_deselection_is_sorted_and_matches_exact_inputs(tmp_path: Path) -> None:
    registry = yaml.safe_load(REGISTRY.read_text())
    registry["conditional_pytest_files"] = {
        "examples/tests/test_z.py": ["runner/Dockerfile"],
        "examples/tests/test_a.py": ["packages/plugin-format"],
    }
    registry_path = tmp_path / "registry.yaml"
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False))
    assert _selection(tmp_path, "apps/api/main.py", registry=registry_path)["pytest_deselect"] == (
        "examples/tests/test_a.py,examples/tests/test_z.py"
    )
    assert _selection(tmp_path, "runner/Dockerfile", registry=registry_path)["pytest_deselect"] == (
        "examples/tests/test_a.py"
    )
    assert (
        _selection(tmp_path, "runner/Dockerfile.other", registry=registry_path)["pytest_deselect"]
        == "examples/tests/test_a.py,examples/tests/test_z.py"
    )


@pytest.mark.parametrize("shard", [1, 2, 3])
@pytest.mark.parametrize("deselect", ["", SRE_RUNTIME])
def test_pytest_step_appends_deselect_only_when_selected(
    tmp_path: Path, shard: int, deselect: str
) -> None:
    expected = [
        "run",
        "pytest",
        "-q",
        "-n",
        "4",
        "--dist",
        "loadgroup",
        "--ci-shard",
        f"{shard}/3",
        "--reruns",
        "1",
        "-rsR",
        "--durations=25",
        "--ignore=tools/factory-e2e/tests/test_c4_initial_binding.py",
        "--ignore=tools/factory-e2e/tests/test_c4_static_token_rotation.py",
        "--ignore=tools/factory-e2e/tests/test_c4_held_cancellation.py",
        "--ignore=tools/factory-e2e/tests/test_c4_forge_instances.py",
        "--ignore", SELECTION_TESTS.FACTORY_UPGRADE_RUNTIME,
        "--ignore", SELECTION_TESTS.CLUSTER_UP_MODULE,
    ]
    if deselect:
        expected += ["--deselect", "examples/tests/test_sre_bot_observability_runtime.py"]
    arguments = _run_pytest_step(tmp_path, shard=shard, deselect=deselect)
    assert PYTEST_TESTS._without_admitted_agent_exclusion(arguments) == expected


def test_pytest_step_preserves_each_deselect_argument(tmp_path: Path) -> None:
    paths = ["examples/tests/test_additional file.py", SRE_RUNTIME]
    argv = _run_pytest_step(tmp_path, shard=2, deselect=",".join(paths))
    argv = PYTEST_TESTS._without_admitted_agent_exclusion(argv)
    assert argv[-4:] == ["--deselect", paths[0], "--deselect", paths[1]]


def _covers(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(prefix + "/")


def _tracking_problems(entries: dict[str, list[str]], tracked: set[str]) -> list[str]:
    problems = []
    for test_file, inputs in entries.items():
        if test_file not in tracked:
            problems.append(f"conditional test file is not tracked: {test_file}")
        for value in inputs:
            if not any(_covers(path, value) for path in tracked):
                problems.append(f"conditional input is not tracked: {value}")
    return problems


def test_conditional_inputs_are_tracked() -> None:
    entries = yaml.safe_load(REGISTRY.read_text())["conditional_pytest_files"]
    assert entries == {name: list(inputs) for name, inputs in CONDITIONAL_PYTEST_FILES.items()}
    completed = subprocess.run(
        ["git", "ls-files", "-z"], cwd=REPO_ROOT, capture_output=True, text=True, check=True
    )
    tracked = set(completed.stdout.split("\0")) - {""}
    assert not _tracking_problems(entries, tracked)


def test_conditional_tracking_detects_missing_files_and_inputs() -> None:
    assert _tracking_problems(
        {"examples/tests/test_example.py": ["examples/example", "missing-input"]},
        {"examples/example/config.yaml"},
    ) == [
        "conditional test file is not tracked: examples/tests/test_example.py",
        "conditional input is not tracked: missing-input",
    ]


def _resolve_repo_path(node: ast.AST, names: dict[str, str]) -> str | None:
    if isinstance(node, ast.Name):
        return names.get(node.id)
    if (
        isinstance(node, ast.BinOp)
        and isinstance(node.op, ast.Div)
        and isinstance(node.right, ast.Constant)
        and isinstance(node.right.value, str)
    ):
        base = _resolve_repo_path(node.left, names)
        if base is not None:
            return "/".join(part for part in (base, node.right.value) if part)
    return None


def _repo_paths(source: str) -> set[str]:
    tree = ast.parse(source)
    names = {"REPO_ROOT": ""}
    for statement in tree.body:
        if isinstance(statement, ast.Assign):
            value = _resolve_repo_path(statement.value, names)
            if value is not None:
                for target in statement.targets:
                    if isinstance(target, ast.Name):
                        names[target.id] = value
        elif isinstance(statement, ast.AnnAssign) and isinstance(statement.target, ast.Name):
            if statement.value is not None:
                value = _resolve_repo_path(statement.value, names)
                if value is not None:
                    names[statement.target.id] = value
    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    paths = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and not isinstance(node.ctx, ast.Load):
            continue
        value = _resolve_repo_path(node, names)
        if not value:
            continue
        parent = parents.get(node)
        if isinstance(parent, ast.BinOp) and _resolve_repo_path(parent, names) is not None:
            continue
        paths.add(value)
    return paths


def _uncovered_repo_paths(source: str, inputs: list[str]) -> list[str]:
    return sorted(
        path for path in _repo_paths(source) if not any(_covers(path, value) for value in inputs)
    )


def test_sre_runtime_repo_reads_are_covered() -> None:
    inputs = yaml.safe_load(REGISTRY.read_text())["conditional_pytest_files"][SRE_RUNTIME]
    source = (REPO_ROOT / SRE_RUNTIME).read_text()
    assert _repo_paths(source), "the AST scan must discover repository paths"
    assert not _uncovered_repo_paths(source, inputs)


def test_repo_read_coverage_resolves_constants_and_rejects_missing_input() -> None:
    source = (
        'CONFIG_DIR = REPO_ROOT / "examples" / "acme-bot"\n'
        'CONFIG_FILE = CONFIG_DIR / "config.yaml"\n'
        "content = CONFIG_FILE.read_text()\n"
        'other = (REPO_ROOT / "otel" / "collector-config.yaml").read_text()\n'
    )
    assert _repo_paths(source) == {
        "examples/acme-bot",
        "examples/acme-bot/config.yaml",
        "otel/collector-config.yaml",
    }
    assert _uncovered_repo_paths(source, ["examples/acme-bot"]) == ["otel/collector-config.yaml"]
    assert not _uncovered_repo_paths(source, ["examples/acme-bot", "otel/collector-config.yaml"])


@pytest.mark.parametrize(
    "entries",
    [
        [],
        {SRE_RUNTIME: []},
        {SRE_RUNTIME: ["runner", "runner"]},
        {SRE_RUNTIME: "runner"},
        {SRE_RUNTIME: [""]},
        {SRE_RUNTIME: ["/runner"]},
        {SRE_RUNTIME: ["runner/"]},
        {SRE_RUNTIME: [1]},
        {"": ["runner"]},
        {"/examples/tests/test_example.py": ["runner"]},
        {"examples/tests/test_example.py/": ["runner"]},
        {1: ["runner"]},
    ],
    ids=[
        "not-mapping",
        "empty-inputs",
        "duplicate-input",
        "not-input-list",
        "empty-input",
        "absolute-input",
        "slash-terminated-input",
        "not-string-input",
        "empty-test-path",
        "absolute-test-path",
        "slash-terminated-test-path",
        "not-string-test-path",
    ],
)
def test_registry_rejects_invalid_conditional_entries(tmp_path: Path, entries: object) -> None:
    registry = yaml.safe_load(REGISTRY.read_text())
    registry["conditional_pytest_files"] = entries
    registry_path = tmp_path / "registry.yaml"
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False))
    selector = _load_selector_module()
    with pytest.raises(selector.RegistryError, match="conditional_pytest_files"):
        selector._load_registry(registry_path)
