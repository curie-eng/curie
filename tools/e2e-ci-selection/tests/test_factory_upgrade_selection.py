"""Factory migration upgrades run in their released-upgrade CI caller."""

from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest


def _load_test_module(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        f"factory_upgrade_{name}", Path(__file__).with_name(f"{name}.py")
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


SELECTION_TESTS = _load_test_module("test_e2e_ci_selection")
PYTEST_TESTS = _load_test_module("test_python_pytest_selection")
REPO_ROOT = SELECTION_TESTS.REPO_ROOT
UPGRADE_FILE = SELECTION_TESTS.FACTORY_UPGRADE_RUNTIME
UPGRADE_NODE = "test_v0125_factory_upgrade_uses_supported_flow"
MIGRATION_PATHS = (
    "cli/src/config_migrate.rs",
    "cli/src/factory_config_migrate.rs",
    "cli/src/ops/upgrade.rs",
    UPGRADE_FILE,
)
MIGRATION_DEPENDENCY_PATHS = (
    "charts/curie/files/legacy-factory-keys.yaml",
    "charts/curie/files/legacy-factory-env.yaml",
    "cli/src/factory_app.rs",
    "cli/src/github_app.rs",
    "cli/src/ops/up.rs",
    "tools/factory-e2e/factory_e2e.py",
    "tools/factory-e2e/scripted_kind.py",
    "tools/factory-e2e/scripted_scenario.py",
    "tools/github-stub/github_stub.py",
)


def _selection(tmp_path: Path, *paths: str, **options: object) -> dict[str, str]:
    result, output = SELECTION_TESTS._invoke_selector(tmp_path, *paths, **options)
    assert result.returncode == 0, result.stdout + result.stderr
    return dict(line.split("=", 1) for line in output.splitlines())


@pytest.mark.parametrize("path", MIGRATION_PATHS)
def test_factory_migration_pr_selects_real_released_upgrade(tmp_path: Path, path: str) -> None:
    output = _selection(tmp_path, path)
    assert output["pytest"] == "true"
    assert output["cluster"] == "true"
    assert output["released_upgrade"] == "true"
    assert output["released_upgrade_full"] == "true"


@pytest.mark.parametrize("path", MIGRATION_DEPENDENCY_PATHS)
def test_factory_migration_dependency_pr_selects_real_released_upgrade(
    tmp_path: Path, path: str
) -> None:
    output = _selection(tmp_path, path)
    assert output["pytest"] == "true"
    assert output["released_upgrade"] == "true"
    assert output["released_upgrade_full"] == "true"


@pytest.mark.parametrize("path", (*MIGRATION_PATHS, *MIGRATION_DEPENDENCY_PATHS))
def test_factory_migration_pr_respects_omit_kind(tmp_path: Path, path: str) -> None:
    output = _selection(tmp_path, path, omit_kind=True)
    assert output["pytest"] == "true"
    assert output["cluster"] == "false"
    assert output["released_upgrade"] == "false"
    assert output["released_upgrade_full"] == "false"


@pytest.mark.parametrize(
    "path",
    [
        "charts/curie/values.yaml",
        "charts/curie/templates/_helpers.tpl",
    ],
)
def test_chart_only_pr_retains_upgrade_smoke(tmp_path: Path, path: str) -> None:
    output = _selection(tmp_path, path)
    assert output["released_upgrade"] == "true"
    assert output["released_upgrade_full"] == "false"


@pytest.mark.parametrize(
    "path",
    [
        "cli/src/main.rs",
        "cli/src/config_migrate.rs.bak",
        "cli/src/factory_config_migrate.rs.bak",
        "cli/src/ops/upgrade.rs.bak",
        "tools/factory-e2e/tests/test_factory_e2e.py",
        UPGRADE_FILE + ".bak",
        *(path + ".bak" for path in MIGRATION_DEPENDENCY_PATHS),
    ],
)
def test_unrelated_paths_do_not_promote_full_upgrade(tmp_path: Path, path: str) -> None:
    assert _selection(tmp_path, path)["released_upgrade_full"] == "false"


def test_mixed_chart_and_factory_migration_pr_runs_full_upgrade(tmp_path: Path) -> None:
    output = _selection(
        tmp_path, "charts/curie/values.yaml", "cli/src/config_migrate.rs"
    )
    assert output["released_upgrade"] == "true"
    assert output["released_upgrade_full"] == "true"


def _collection_repository(tmp_path: Path) -> Path:
    root = tmp_path / "collection"
    root.mkdir()
    for relative in ("pyproject.toml", "conftest.py", "tools/factory-e2e/tests/conftest.py"):
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO_ROOT / relative, destination)
    target = root / UPGRADE_FILE
    target.write_text(
        "from pathlib import Path\n"
        "Path(__file__).with_suffix('.collected').touch()\n"
        f"def {UPGRADE_NODE}():\n"
        "    Path(__file__).with_suffix('.executed').touch()\n"
    )
    neighbor = target.with_name("test_factory_e2e.py")
    neighbor.write_text(
        "from pathlib import Path\n"
        "def test_factory_neighbor():\n"
        "    Path(__file__).with_suffix('.executed').touch()\n"
    )
    return root


def _run_collection(root: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment.pop("PYTEST_ADDOPTS", None)
    environment["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    return subprocess.run(
        [
            sys.executable, "-m", "pytest", "-p", "no:cacheprovider",
            "-p", "xdist.plugin", "-p", "pytest_rerunfailures", *arguments,
        ],
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )


@pytest.mark.parametrize(
    ("event", "paths", "scope"),
    [
        pytest.param(
            "pull_request", ("apps/api/src/curie_api/main.py",), "full", id="full-pr"
        ),
        pytest.param(
            "pull_request", ("cli/src/config_migrate.rs",), "cli", id="cli-pr"
        ),
        pytest.param("push", (), "full", id="push"),
        pytest.param("workflow_dispatch", (), "full", id="dispatch"),
    ],
)
def test_python_shard_caller_excludes_upgrade_but_explicit_selector_executes(
    tmp_path: Path, event: str, paths: tuple[str, ...], scope: str
) -> None:
    if event != "pull_request":
        decision = tmp_path / "decision"
        decision.mkdir()
        calls = PYTEST_TESTS._run_decision_step(decision, "python-pytest", event)
        uv_calls = [call.split() for call in calls if call.startswith("uv ")]
        assert len(uv_calls) == 1
        assert "--push" in uv_calls[0]
    output = _selection(tmp_path, *paths, push=event != "pull_request")
    assert output["pytest"] == "true"
    assert output["pytest_scope"] == scope
    caller = tmp_path / "caller"
    caller.mkdir()
    cli_files = (UPGRADE_FILE, "tools/factory-e2e/tests/test_factory_e2e.py")
    argv = PYTEST_TESTS._run_pytest_step(
        caller,
        shard=1,
        deselect=output["pytest_deselect"],
        scope=output["pytest_scope"],
        cli_files=cli_files,
    )
    assert argv[:2] == ["run", "pytest"]
    if scope == "cli":
        manifest = caller / "tools/e2e-ci-selection/cli_pytest_files.txt"
        assert manifest.read_text().splitlines() == list(cli_files)

    root = _collection_repository(tmp_path)
    target = root / UPGRADE_FILE
    result = _run_collection(root, *argv[2:])
    assert result.returncode == 0, result.stdout + result.stderr
    assert not target.with_suffix(".executed").exists(), (
        f"{scope} Python shard executed the excluded upgrade regression: {argv}"
    )
    assert not target.with_suffix(".collected").exists(), (
        f"{scope} Python shard collected the excluded upgrade regression: {argv}"
    )
    assert target.with_name("test_factory_e2e.executed").is_file()

    explicit = _run_collection(root, "-q", f"{UPGRADE_FILE}::{UPGRADE_NODE}")
    assert explicit.returncode == 0, explicit.stdout + explicit.stderr
    assert "1 passed" in explicit.stdout
    assert target.with_suffix(".collected").is_file()
    assert target.with_suffix(".executed").is_file()
