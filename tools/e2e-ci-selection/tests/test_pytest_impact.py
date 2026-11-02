"""Pull request pytest impact selection: graph, select(), shard plan and selector outputs."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import re
import subprocess
import sys
from collections.abc import Iterable, Mapping, Sequence
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
SELECTOR = REPO_ROOT / "tools" / "e2e-ci-selection" / "select_tiers.py"
IMPACT = REPO_ROOT / "tools" / "e2e-ci-selection" / "pytest_impact.py"
REGISTRY = REPO_ROOT / ".github" / "e2e-selection.yaml"
CLI_LIST = REPO_ROOT / "tools" / "e2e-ci-selection" / "cli_pytest_files.txt"
WORKFLOW_PATH = ".github/workflows/ci.yaml"
REGISTRY_PATH = ".github/e2e-selection.yaml"
UPGRADE_JOBS = (
    "e2e-released-upgrade",
    "e2e-released-upgrade-negative",
    "e2e-cluster-upgrade-matrix-shards",
    "e2e-cluster-upgrade-matrix",
)
DIGEST = re.compile(r"[0-9a-f]{64}")


@lru_cache(maxsize=1)
def _impact() -> ModuleType:
    """Load pytest_impact.py by path, the way select_tiers.py loads release/atlas.py."""
    spec = importlib.util.spec_from_file_location("pytest_impact_under_test", IMPACT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _registry_document() -> dict[str, Any]:
    document = yaml.safe_load(REGISTRY.read_text(encoding="utf-8"))
    assert isinstance(document, dict)
    return document


def _impact_section(**overrides: object) -> dict[str, object]:
    section: dict[str, object] = {
        "everything": ["uv.lock", "pyproject.toml", "conftest.py"],
        "no_impact": ["tools/policy/tests"],
        "workflow_jobs": ["python-pytest", "python", "changes"],
        "excluded": ["tools/policy/tests"],
        "always": ["tests"],
        "reader_only": ["docs"],
        "min_shard_share": 24,
        "max_shards": 6,
    }
    section.update(overrides)
    return section


def _config(**overrides: object) -> Any:
    """A synthetic pytest_impact section inside the real registry document.

    The rest of the document supplies rules.ignored_exact and the cli list path.
    """
    document = deepcopy(_registry_document())
    document["pytest_impact"] = _impact_section(**overrides)
    return _impact().load_config(document)


def _under_any(path: str, prefixes: Iterable[str]) -> bool:
    return any(path == prefix or path.startswith(f"{prefix}/") for prefix in prefixes)


def _select(
    graph: Any,
    config: Any,
    *paths: str,
    cli_files: Sequence[str] = (),
    workflow_jobs_changed: frozenset[str] | None = None,
    registry_pytest_changed: bool | None = None,
) -> Any:
    selection = _impact().select(
        graph,
        config,
        list(paths),
        cli_files=list(cli_files),
        workflow_jobs_changed=workflow_jobs_changed,
        registry_pytest_changed=registry_pytest_changed,
    )
    assert list(selection.targets) == sorted(selection.targets)
    if selection.everything:
        assert selection.targets == ()
    return selection


# Synthetic workspaces -------------------------------------------------------


def _module(name: str) -> str:
    return name.replace("-", "_")


def _workspace(
    members: Mapping[str, tuple[str, Sequence[str]]],
    *,
    extra_testpaths: Sequence[str] = (),
    member_tests: bool = True,
) -> dict[str, str]:
    """Files of a uv workspace: root and member pyprojects, src packages, tests."""
    testpaths = [f"{member}/tests" for member in members] + list(extra_testpaths)
    files = {
        "pyproject.toml": (
            '[project]\nname = "synthetic-root"\nversion = "0"\n\n'
            f"[tool.uv.workspace]\nmembers = {json.dumps(list(members))}\n\n"
            f"[tool.pytest.ini_options]\ntestpaths = {json.dumps(testpaths)}\n"
        )
    }
    for member, (name, dependencies) in members.items():
        module = _module(name)
        files[f"{member}/pyproject.toml"] = (
            f'[project]\nname = "{name}"\nversion = "0"\n'
            f"dependencies = {json.dumps(list(dependencies))}\n"
        )
        files[f"{member}/src/{module}/__init__.py"] = '"""Synthetic package."""\n'
        files[f"{member}/src/{module}/core.py"] = "VALUE = 1\n"
        if member_tests:
            files[f"{member}/tests/test_{member.rsplit('/', 1)[-1]}.py"] = (
                f"import {module}\n\n\ndef test_value() -> None:\n    assert {module}.__name__\n"
            )
    return files


def _build(root: Path, files: Mapping[str, str]) -> Any:
    for relative, text in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return _impact().ImpactGraph.build(root, tracked=sorted(files))


GRAPH_MEMBERS: dict[str, tuple[str, Sequence[str]]] = {
    "packages/c": ("pkg-c", ()),
    # pyproject edge only: pkg_b's source never imports pkg_c.
    "packages/b": ("pkg-b", ("pkg-c>=1.0",)),
    # source import edge only: no pyproject dependency on pkg-b.
    "apps/a": ("app-a", ()),
    "packages/z": ("pkg-z", ()),
    "packages/y": ("pkg-y", ()),
    "packages/h": ("pkg-h", ()),
    "packages/r": ("pkg-r", ()),
    "packages/m": ("pkg-m", ()),
}
PATH_ROOT = "from pathlib import Path\n\nROOT = Path(__file__).resolve().parents[3]\n"
GRAPH_EXTRA = {
    "apps/a/src/app_a/core.py": "import pkg_b\n\nVALUE = pkg_b\n",
    # A test file outside every member that imports the top of the chain.
    "examples/tests/test_seam.py": "import app_a\n\n\ndef test_seam() -> None:\n    assert app_a\n",
    # A conftest two directories above the test imports pkg_b.
    "packages/y/tests/sub/conftest.py": "import pkg_b\n\nFIXTURE = pkg_b\n",
    "packages/y/tests/sub/deeper/test_deep.py": "def test_deep() -> None:\n    assert True\n",
    # A local helper module imported by a test carries its own imports.
    "packages/h/tests/helpers.py": "import pkg_c\n\n\ndef build() -> object:\n    return pkg_c\n",
    "packages/h/tests/test_uses_helper.py": (
        "from helpers import build\n\n\ndef test_build() -> None:\n    assert build()\n"
    ),
    # Path references.
    "packages/r/tests/test_div.py": (
        PATH_ROOT + 'VALUES = ROOT / "charts" / "demo" / "values.yaml"\n\n\n'
        "def test_values() -> None:\n    assert VALUES\n"
    ),
    "packages/r/tests/test_slash.py": (
        'COMPOSE = "compose/dev.yaml"\n\n\ndef test_compose() -> None:\n    assert COMPOSE\n'
    ),
    "packages/r/tests/test_join.py": (
        "import os\n\n" + PATH_ROOT + 'GUIDE = os.path.join(ROOT, "docs", "guide.md")\n\n\n'
        "def test_guide() -> None:\n    assert GUIDE\n"
    ),
    "packages/r/tests/test_apps_dir.py": (
        PATH_ROOT + 'APPS = ROOT / "apps"\n\n\ndef test_apps() -> None:\n    assert APPS\n'
    ),
    "packages/r/tests/test_docstring.py": (
        '"""Mentions charts/demo/values.yaml, compose/dev.yaml and docs/guide.md."""\n\n\n'
        "def test_docstring() -> None:\n"
        '    """Also reads charts/demo/values.yaml and compose/dev.yaml in prose."""\n'
        "    assert True\n"
    ),
    # `python -m pkg_c` and a console script named after a project.
    "packages/m/tests/test_module_run.py": (
        "import subprocess\nimport sys\n\n\ndef test_module_run() -> None:\n"
        '    subprocess.run([sys.executable, "-m", "pkg_c"], check=False)\n'
    ),
    "packages/m/tests/test_console_script.py": (
        "import shutil\n\n\ndef test_console_script() -> None:\n"
        '    assert shutil.which("pkg-b") is None or True\n'
    ),
    # Referenced data.
    "charts/demo/values.yaml": "demo: true\n",
    "compose/dev.yaml": "services: {}\n",
    "docs/guide.md": "# Guide\n",
    "notes/todo.md": "nothing references this\n",
}


@pytest.fixture(scope="module")
def graph_repo(tmp_path_factory: pytest.TempPathFactory) -> Any:
    files = _workspace(GRAPH_MEMBERS, extra_testpaths=("examples/tests",))
    files.update(GRAPH_EXTRA)
    return _build(tmp_path_factory.mktemp("graph-repo"), files)


def test_test_universe_follows_testpaths_and_test_name_patterns(graph_repo: Any) -> None:
    files = set(graph_repo.test_files)

    assert "examples/tests/test_seam.py" in files
    assert "packages/y/tests/sub/deeper/test_deep.py" in files
    assert "packages/h/tests/helpers.py" not in files
    assert "packages/y/tests/sub/conftest.py" not in files
    assert "packages/c/src/pkg_c/core.py" not in files


@pytest.mark.parametrize(
    ("path", "region"),
    (
        ("packages/c/src/pkg_c/core.py", "packages/c"),
        ("apps/a/tests/test_a.py", "apps/a"),
        ("examples/tests/test_seam.py", "examples/tests"),
        ("tools/foo/bar/thing.py", "tools/foo"),
        ("charts/demo/values.yaml", "charts"),
        ("notes/todo.md", "notes"),
        ("README.md", "README.md"),
    ),
)
def test_region_of_prefers_members_then_two_then_one_segment(
    graph_repo: Any, path: str, region: str
) -> None:
    assert graph_repo.region_of(path) == region


def test_package_dependents_follow_pyproject_and_import_edges_transitively(
    graph_repo: Any,
) -> None:
    assert graph_repo.package_dependents("packages/c") == frozenset({"packages/b", "apps/a"})
    assert graph_repo.package_dependents("packages/b") == frozenset({"apps/a"})
    assert graph_repo.package_dependents("apps/a") == frozenset()
    assert graph_repo.package_dependents("packages/z") == frozenset()


def test_file_regions_include_the_transitive_package_closure(graph_repo: Any) -> None:
    regions = graph_repo.file_regions("apps/a/tests/test_a.py")

    assert {"apps/a", "packages/b", "packages/c"} <= regions
    assert "packages/z" not in regions


def test_change_to_the_bottom_of_a_chain_selects_every_dependent_test(graph_repo: Any) -> None:
    selection = _select(graph_repo, _config(), "packages/c/src/pkg_c/core.py")
    targets = set(selection.targets)

    assert not selection.everything
    assert selection.reason == "impact"
    expected = {
        "packages/c/tests/test_c.py",
        "packages/b/tests/test_b.py",  # pyproject edge
        "apps/a/tests/test_a.py",  # import edge on top of the pyproject edge
        "examples/tests/test_seam.py",  # imports app_a from outside the members
        "packages/y/tests/sub/deeper/test_deep.py",  # ancestor conftest imports pkg_b
        "packages/h/tests/test_uses_helper.py",  # local helper imports pkg_c
        "packages/m/tests/test_module_run.py",  # python -m pkg_c
        "packages/m/tests/test_console_script.py",  # project name pkg-b
    }
    assert expected <= targets
    unrelated = {
        "packages/z/tests/test_z.py",
        "packages/y/tests/test_y.py",
        "packages/h/tests/test_h.py",
        "packages/m/tests/test_m.py",
        "packages/r/tests/test_r.py",
        "packages/r/tests/test_docstring.py",
    }
    assert not unrelated & targets


def test_unrelated_package_change_selects_only_its_own_tests(graph_repo: Any) -> None:
    selection = _select(graph_repo, _config(), "packages/z/src/pkg_z/core.py")

    assert not selection.everything
    assert selection.targets == ("packages/z/tests/test_z.py",)


def test_ancestor_conftest_import_reaches_only_tests_below_it(graph_repo: Any) -> None:
    targets = set(_select(graph_repo, _config(), "packages/b/src/pkg_b/core.py").targets)

    assert "packages/y/tests/sub/deeper/test_deep.py" in targets
    assert "packages/y/tests/test_y.py" not in targets
    assert "packages/b" in graph_repo.file_regions("packages/y/tests/sub/deeper/test_deep.py")


def test_local_helper_imports_propagate_to_the_importing_test(graph_repo: Any) -> None:
    assert "packages/c" in graph_repo.file_regions("packages/h/tests/test_uses_helper.py")
    assert "packages/c" not in graph_repo.file_regions("packages/h/tests/test_h.py")


def test_python_dash_m_and_project_name_strings_count_as_package_dependencies(
    graph_repo: Any,
) -> None:
    assert "packages/c" in graph_repo.file_regions("packages/m/tests/test_module_run.py")
    assert "packages/b" in graph_repo.file_regions("packages/m/tests/test_console_script.py")
    targets = set(_select(graph_repo, _config(), "packages/z/src/pkg_z/core.py").targets)
    assert "packages/m/tests/test_module_run.py" not in targets


PATH_REFERENCE_TESTS = (
    "packages/r/tests/test_div.py",
    "packages/r/tests/test_slash.py",
    "packages/r/tests/test_join.py",
    "packages/r/tests/test_apps_dir.py",
    "packages/r/tests/test_docstring.py",
    "packages/r/tests/test_r.py",
)


@pytest.mark.parametrize(
    ("changed", "selected"),
    (
        ("charts/demo/values.yaml", {"packages/r/tests/test_div.py"}),
        ("compose/dev.yaml", {"packages/r/tests/test_slash.py"}),
        ("docs/guide.md", {"packages/r/tests/test_join.py"}),
        ("apps/a/src/app_a/core.py", {"packages/r/tests/test_apps_dir.py"}),
    ),
    ids=("path-division", "slash-string", "os-path-join", "directory-prefix"),
)
def test_path_references_select_the_referencing_test_only(
    graph_repo: Any, changed: str, selected: set[str]
) -> None:
    selection = _select(graph_repo, _config(), changed)
    targets = set(selection.targets)

    assert not selection.everything
    assert {path for path in PATH_REFERENCE_TESTS if path in targets} == selected


def test_docstring_path_mentions_create_no_dependency(graph_repo: Any) -> None:
    regions = graph_repo.file_regions("packages/r/tests/test_docstring.py")

    assert not {"charts", "compose", "docs"} & regions


def test_empty_impact_returns_everything(graph_repo: Any) -> None:
    selection = _select(graph_repo, _config(), "docs/unread.md")

    assert selection.everything
    assert selection.reason == "empty impact"
    assert selection.targets == ()


# Property: every transitive dependent's tests are selected -------------------

CHAIN_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "packages/p0": (),
    "packages/p1": ("packages/p0",),
    "packages/p2": ("packages/p1",),
    "apps/p3": ("packages/p0", "packages/p2"),
    "packages/p4": (),
    "packages/p5": ("packages/p4",),
    "apps/p6": ("apps/p3",),
}
# Which edges are declared in pyproject (the rest are source imports).
CHAIN_PYPROJECT_EDGES = {
    ("packages/p1", "packages/p0"),
    ("apps/p3", "packages/p0"),
    ("packages/p5", "packages/p4"),
}


def _chain_name(member: str) -> str:
    return "pkg-" + member.rsplit("/", 1)[-1]


def _chain_closure(member: str) -> set[str]:
    seen = {member}
    stack = [member]
    while stack:
        for dependency in CHAIN_DEPENDENCIES[stack.pop()]:
            if dependency not in seen:
                seen.add(dependency)
                stack.append(dependency)
    return seen


@pytest.fixture(scope="module")
def chain_repo(tmp_path_factory: pytest.TempPathFactory) -> Any:
    members: dict[str, tuple[str, Sequence[str]]] = {}
    for member, dependencies in CHAIN_DEPENDENCIES.items():
        declared = [
            f"{_chain_name(dependency)}[extra]>=2; python_version >= '3.13'"
            for dependency in dependencies
            if (member, dependency) in CHAIN_PYPROJECT_EDGES
        ]
        members[member] = (_chain_name(member), declared)
    files = _workspace(members)
    for member, dependencies in CHAIN_DEPENDENCIES.items():
        module = _module(_chain_name(member))
        imported = [
            _module(_chain_name(dependency))
            for dependency in dependencies
            if (member, dependency) not in CHAIN_PYPROJECT_EDGES
        ]
        files[f"{member}/src/{module}/core.py"] = (
            "".join(f"import {name}\n" for name in imported) + "VALUE = 1\n"
        )
        files[f"{member}/tests/test_more_{member.rsplit('/', 1)[-1]}.py"] = (
            f"from {module} import core\n\n\ndef test_more() -> None:\n    assert core.VALUE\n"
        )
    return _build(tmp_path_factory.mktemp("chain-repo"), files)


def _chain_tests(member: str) -> set[str]:
    leaf = member.rsplit("/", 1)[-1]
    return {f"{member}/tests/test_{leaf}.py", f"{member}/tests/test_more_{leaf}.py"}


@pytest.mark.parametrize("changed", tuple(CHAIN_DEPENDENCIES))
def test_synthetic_diff_selects_every_transitive_dependent_package_test(
    chain_repo: Any, changed: str
) -> None:
    module = _module(_chain_name(changed))
    dependents = {
        member
        for member in CHAIN_DEPENDENCIES
        if member != changed and changed in _chain_closure(member)
    }

    selection = _select(chain_repo, _config(), f"{changed}/src/{module}/core.py")
    targets = set(selection.targets)

    assert not selection.everything
    assert chain_repo.package_dependents(changed) == frozenset(dependents)
    required = set().union(*(_chain_tests(member) for member in {changed, *dependents}))
    assert required <= targets, sorted(required - targets)
    unaffected = set().union(*(_chain_tests(member) for member in CHAIN_DEPENDENCIES)) - required
    assert not unaffected & targets, sorted(unaffected & targets)


# Configuration, always, excluded, repo-wide ---------------------------------


@pytest.mark.parametrize(
    "section",
    (
        "not a mapping",
        _impact_section(everything="uv.lock"),
        _impact_section(min_shard_share="24"),
        _impact_section(max_shards=[6]),
        {key: value for key, value in _impact_section().items() if key != "always"},
    ),
    ids=("not-mapping", "everything-string", "share-string", "max-list", "missing-key"),
)
def test_load_config_rejects_a_malformed_section(section: object) -> None:
    document = deepcopy(_registry_document())
    document["pytest_impact"] = section

    with pytest.raises(ValueError):
        _impact().load_config(document)


def test_load_config_reads_the_section_values() -> None:
    config = _config(max_shards=4, min_shard_share=12)

    assert config.max_shards == 4
    assert config.min_shard_share == 12
    assert tuple(config.workflow_jobs) == ("python-pytest", "python", "changes")
    assert tuple(config.always) == ("tests",)


def test_excluded_paths_never_appear_and_always_paths_always_do(tmp_path: Path) -> None:
    files = _workspace(
        {"packages/c": ("pkg-c", ()), "packages/z": ("pkg-z", ())}, extra_testpaths=("tests",)
    )
    files.update(
        {
            "packages/c/tests/test_skipped.py": "import pkg_c\n\n\ndef test_x() -> None:\n"
            "    assert pkg_c\n",
            "packages/c/tests/slow/test_slow.py": "import pkg_c\n\n\ndef test_y() -> None:\n"
            "    assert pkg_c\n",
            "tests/test_seam_always.py": "def test_seam() -> None:\n    assert True\n",
        }
    )
    graph = _build(tmp_path, files)
    config = _config(
        excluded=["packages/c/tests/test_skipped.py", "packages/c/tests/slow"],
        always=["tests"],
    )

    changed_c = set(_select(graph, config, "packages/c/src/pkg_c/core.py").targets)
    changed_z = set(_select(graph, config, "packages/z/src/pkg_z/core.py").targets)

    assert "tests/test_seam_always.py" in graph.test_files
    assert changed_c == {"packages/c/tests/test_c.py", "tests/test_seam_always.py"}
    assert changed_z == {"packages/z/tests/test_z.py", "tests/test_seam_always.py"}


@pytest.mark.parametrize(
    "body",
    (
        'FILES = sorted(REPO_ROOT.rglob("*.py"))\n',
        'FILES = sorted(ROOT.glob("**/*.yaml"))\n',
        "import os\n\nFILES = list(os.walk(REPOSITORY_ROOT))\n",
        "import subprocess\n\nFILES = subprocess.run(\n"
        '    ["git", "ls-files"], cwd=REPO_ROOT, capture_output=True, check=False\n)\n',
    ),
    ids=("rglob", "glob-star-star", "os-walk", "git-ls-files"),
)
def test_repo_wide_scanners_are_selected_by_every_impact_selection(
    tmp_path: Path, body: str
) -> None:
    files = _workspace({"packages/c": ("pkg-c", ()), "packages/z": ("pkg-z", ())})
    files["packages/c/tests/test_scan.py"] = (
        "from pathlib import Path\n\nREPO_ROOT = ROOT = REPOSITORY_ROOT = "
        "Path(__file__).resolve().parents[3]\n"
        + body
        + "\n\ndef test_scan() -> None:\n    assert FILES is not None\n"
    )
    graph = _build(tmp_path, files)

    targets = set(_select(graph, _config(), "packages/z/src/pkg_z/core.py").targets)

    assert targets == {"packages/c/tests/test_scan.py", "packages/z/tests/test_z.py"}


# Regressions: __file__ anchors, referenced entry points, unclaimed paths ----

ANCHOR_HEADER = "import os\nfrom pathlib import Path\n\n"
ANCHOR_TESTS = {
    # The turn canary shape: a directory beside tests/, then files in it.
    "examples/tests/test_profile_module.py": (
        ANCHOR_HEADER + 'SOURCE = Path(__file__).resolve().parents[1] / "turn-canary"\n'
        'HELPER = SOURCE / "probe_profiles.py"\n\n\n'
        "def test_helper() -> None:\n    assert HELPER\n"
    ),
    "examples/tests/test_runtime.py": (
        ANCHOR_HEADER
        + 'CANARY = Path(__file__).resolve().parents[1] / "turn-canary" / "turn_canary.py"\n\n\n'
        "def test_runtime() -> None:\n    assert CANARY\n"
    ),
    "examples/tests/test_dirname.py": (
        ANCHOR_HEADER + "HERE = os.path.dirname(os.path.abspath(__file__))\n"
        'DATA = os.path.join(HERE, "..", "turn-canary", "probe_profiles.py")\n\n\n'
        "def test_dirname() -> None:\n    assert DATA\n"
    ),
    "examples/tests/test_parent_chain.py": (
        ANCHOR_HEADER + "def test_parent_chain() -> None:\n"
        '    assert (Path(__file__).parent.parent / "turn-canary").joinpath("turn_canary.py")\n'
    ),
    "examples/tests/test_unrelated.py": "def test_unrelated() -> None:\n    assert True\n",
}
UNRESOLVED_ANCHORS = {
    "computed-index": "DEPTH = int(os.environ.get('DEPTH', '1'))\n"
    "BASE = Path(__file__).resolve().parents[DEPTH]\n",
    "above-root": 'BASE = Path(__file__).resolve().parents[7] / "elsewhere"\n',
    "dotdot-above-root": 'BASE = os.path.join(os.path.dirname(__file__), "..", "..", "..", "x")\n',
}


def _anchor_repo(root: Path, extra: Mapping[str, str] | None = None) -> Any:
    files = _workspace({"packages/z": ("pkg-z", ())}, extra_testpaths=("examples/tests",))
    files.update(ANCHOR_TESTS)
    files["examples/turn-canary/probe_profiles.py"] = "class ProbeProfile:\n    pass\n"
    files["examples/turn-canary/turn_canary.py"] = "VALUE = 1\n"
    files.update(extra or {})
    return _build(root, files)


@pytest.mark.parametrize(
    ("changed", "selected"),
    (
        (
            "examples/turn-canary/probe_profiles.py",
            {
                "examples/tests/test_profile_module.py",
                "examples/tests/test_runtime.py",
                "examples/tests/test_dirname.py",
                "examples/tests/test_parent_chain.py",
            },
        ),
        (
            "examples/turn-canary/turn_canary.py",
            {
                "examples/tests/test_profile_module.py",
                "examples/tests/test_runtime.py",
                "examples/tests/test_dirname.py",
                "examples/tests/test_parent_chain.py",
            },
        ),
    ),
)
def test_file_relative_references_resolve_against_the_test_location(
    tmp_path: Path, changed: str, selected: set[str]
) -> None:
    graph = _anchor_repo(tmp_path)

    selection = _select(graph, _config(), changed)

    assert not selection.everything
    assert set(selection.targets) == selected


@pytest.mark.parametrize("body", tuple(UNRESOLVED_ANCHORS.values()), ids=tuple(UNRESOLVED_ANCHORS))
def test_unresolvable_file_relative_reference_is_selected_by_every_diff(
    tmp_path: Path, body: str
) -> None:
    graph = _anchor_repo(
        tmp_path,
        {
            "examples/tests/test_opaque.py": ANCHOR_HEADER
            + body
            + "\n\ndef test_opaque() -> None:\n    assert BASE\n"
        },
    )

    targets = set(_select(graph, _config(), "packages/z/src/pkg_z/core.py").targets)

    assert graph.repo_wide("examples/tests/test_opaque.py")
    assert targets == {"examples/tests/test_opaque.py", "packages/z/tests/test_z.py"}


def test_anchored_repository_root_scan_counts_as_repo_wide(tmp_path: Path) -> None:
    graph = _anchor_repo(
        tmp_path,
        {
            "examples/tests/test_scan_anchor.py": ANCHOR_HEADER
            + 'FILES = sorted(Path(__file__).resolve().parents[2].rglob("*.py"))\n\n\n'
            "def test_scan() -> None:\n    assert FILES is not None\n"
        },
    )

    assert graph.repo_wide("examples/tests/test_scan_anchor.py")
    assert not graph.repo_wide("examples/tests/test_profile_module.py")


ENTRY_MEMBERS: dict[str, tuple[str, Sequence[str]]] = {
    "packages/c": ("pkg-c", ()),
    # The plugin-format shape: the script imports this package, which depends on pkg-c.
    "packages/f": ("pkg-f", ("pkg-c",)),
    "packages/z": ("pkg-z", ()),
}
ENTRY_FILES = {
    "packages/f/src/pkg_f/tool_policy.py": "def classify_tool() -> str:\n    return 'allow'\n",
    # A checker script that imports a workspace package and a sibling helper.
    "tools/contract/check.py": (
        "import sys\n\nfrom pkg_f import tool_policy\nfrom rules import RULES\n\n"
        "print(tool_policy, RULES, file=sys.stderr)\n"
    ),
    "tools/contract/rules.py": "import pkg_z\n\nRULES = pkg_z\n",
    "tools/contract/tests/test_shipped_policy.py": (
        "import subprocess\nimport sys\nfrom pathlib import Path\n\n"
        "ROOT = Path(__file__).resolve().parents[3]\n\n\n"
        "def test_shipped_policy() -> None:\n"
        '    subprocess.run([sys.executable, str(ROOT / "tools/contract/check.py")], check=False)\n'
    ),
    # A shell wrapper that runs another Python entry point.
    "scripts/run-entry.sh": '#!/bin/sh\nexec python3 tools/entry/main.py "$@"\n',
    "tools/entry/main.py": "import pkg_f\n\nprint(pkg_f)\n",
    "tools/contract/tests/test_shell_entry.py": (
        "import subprocess\n\n\ndef test_shell_entry() -> None:\n"
        '    subprocess.run(["sh", "scripts/run-entry.sh"], check=False)\n'
    ),
    "tools/contract/tests/test_unrelated.py": "def test_unrelated() -> None:\n    assert True\n",
}


@pytest.mark.parametrize(
    ("changed", "selected"),
    (
        (
            "packages/f/src/pkg_f/tool_policy.py",
            {
                "tools/contract/tests/test_shipped_policy.py",
                "tools/contract/tests/test_shell_entry.py",
            },
        ),
        # pkg-f depends on pkg-c, so the entry point reaches pkg-c too.
        (
            "packages/c/src/pkg_c/core.py",
            {
                "tools/contract/tests/test_shipped_policy.py",
                "tools/contract/tests/test_shell_entry.py",
            },
        ),
        # The script's sibling helper imports pkg_z.
        ("packages/z/src/pkg_z/core.py", {"tools/contract/tests/test_shipped_policy.py"}),
    ),
)
def test_referenced_python_entry_points_contribute_their_imports(
    tmp_path: Path, changed: str, selected: set[str]
) -> None:
    files = _workspace(ENTRY_MEMBERS, extra_testpaths=("tools/contract/tests",))
    files.update(ENTRY_FILES)
    graph = _build(tmp_path, files)

    targets = set(_select(graph, _config(), changed).targets)

    assert {path for path in targets if path.startswith("tools/contract/")} == selected


UNCLAIMED_FILES: dict[str, str] = {
    "tests/test_always.py": "def test_always() -> None:\n    assert True\n",
    "packages/z/tests/test_data.py": (
        'DATA = "charts/demo/values.yaml"\n\n\ndef test_data() -> None:\n    assert DATA\n'
    ),
    "charts/demo/values.yaml": "demo: true\n",
    "new-dir/input.json": "{}\n",
    "notes/todo.md": "nothing reads this\n",
    "docs/adr/0001-record.md": "# Record\n",
}


@pytest.mark.parametrize(
    ("changed", "everything"),
    (
        ("new-dir/input.json", True),
        ("new-dir/nested/deeper/config.yaml", True),
        # Markdown outside docs/ is no longer exempt.
        ("notes/todo.md", True),
        ("new-dir/SKILL.md", True),
        ("docs/adr/0001-record.md", False),
        ("charts/demo/values.yaml", False),
    ),
)
def test_a_path_no_test_region_claims_selects_everything_unless_ignored(
    tmp_path: Path, changed: str, everything: bool
) -> None:
    files = _workspace({"packages/z": ("pkg-z", ())}, extra_testpaths=("tests",))
    files.update(UNCLAIMED_FILES)
    graph = _build(tmp_path, files)

    selection = _select(graph, _config(), changed)

    assert selection.everything is everything
    if everything:
        assert selection.reason == f"{changed} unclaimed"
    else:
        assert "tests/test_always.py" in selection.targets
    if changed == "charts/demo/values.yaml":
        assert "packages/z/tests/test_data.py" in selection.targets


# select() on the real registry and repository -------------------------------


@pytest.fixture(scope="module")
def real_graph() -> Any:
    return _impact().ImpactGraph.build(REPO_ROOT)


@pytest.fixture(scope="module")
def real_config() -> Any:
    return _impact().load_config(_registry_document())


@pytest.fixture(scope="module")
def cli_files() -> list[str]:
    return [
        line.strip()
        for line in CLI_LIST.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def _root_tests(graph: Any, config: Any) -> set[str]:
    return {
        path
        for path in graph.test_files
        if path.startswith("tests/") and not _under_any(path, config.excluded)
    }


def _nothing_selected(graph: Any, config: Any) -> set[str]:
    """Targets of a change that contributes nothing: always plus repo-wide files."""
    selection = _select(
        graph, config, WORKFLOW_PATH, workflow_jobs_changed=frozenset({"rust-lint"})
    )
    assert not selection.everything
    return set(selection.targets)


@pytest.mark.parametrize(
    "path",
    (
        "uv.lock",
        "pyproject.toml",
        "conftest.py",
        "compose.dev.yaml",
        "packages/test-support/src/curie_test_support/postgres.py",
        "tools/e2e-ci-selection/select_tiers.py",
        "otel/collector-config.yaml",
        "scripts/wait-for-langfuse.py",
        ".gitleaks.toml",
    ),
)
def test_everything_triggers_select_everything(
    real_graph: Any, real_config: Any, path: str
) -> None:
    selection = _select(real_graph, real_config, path, "runner/src/curie_runner/__init__.py")

    assert selection.everything
    assert selection.reason == path
    assert selection.targets == ()


def test_no_impact_carve_out_contributes_nothing(real_graph: Any, real_config: Any) -> None:
    selection = _select(
        real_graph, real_config, "tools/e2e-ci-selection/tests/test_pytest_impact.py"
    )

    assert not selection.everything
    assert set(selection.targets) == _nothing_selected(real_graph, real_config)
    assert not any(path.startswith("tools/e2e-ci-selection/tests/") for path in selection.targets)


def test_cli_list_file_contributes_its_listed_files(
    real_graph: Any, real_config: Any, cli_files: list[str]
) -> None:
    listed = {path for path in cli_files if not _under_any(path, real_config.excluded)}

    selection = _select(
        real_graph,
        real_config,
        "tools/e2e-ci-selection/cli_pytest_files.txt",
        cli_files=cli_files,
    )

    assert not selection.everything
    assert listed <= set(selection.targets)


def test_cli_source_change_contributes_the_cli_list(
    real_graph: Any, real_config: Any, cli_files: list[str]
) -> None:
    listed = {path for path in cli_files if not _under_any(path, real_config.excluded)}

    selection = _select(real_graph, real_config, "cli/src/main.rs", cli_files=cli_files)

    assert not selection.everything
    assert listed <= set(selection.targets)


def test_ignored_root_doc_does_not_force_everything(real_graph: Any, real_config: Any) -> None:
    selection = _select(real_graph, real_config, "README.md")

    assert not selection.everything
    assert _root_tests(real_graph, real_config) <= set(selection.targets)


@pytest.mark.parametrize(
    ("jobs", "everything"),
    (
        (None, True),
        (frozenset({"<header>"}), True),
        (frozenset({"python-pytest"}), True),
        (frozenset({"python"}), True),
        (frozenset({"changes"}), True),
        (frozenset({"rust-lint", "python-pytest"}), True),
        (frozenset({"rust-lint"}), False),
        (frozenset(), False),
    ),
    ids=("path-mode", "header", "python-pytest", "python", "changes", "mixed", "rust", "none"),
)
def test_workflow_hunks_select_everything_only_for_pytest_jobs(
    real_graph: Any, real_config: Any, jobs: frozenset[str] | None, everything: bool
) -> None:
    selection = _select(real_graph, real_config, WORKFLOW_PATH, workflow_jobs_changed=jobs)

    assert selection.everything is everything
    if not everything:
        assert _root_tests(real_graph, real_config) <= set(selection.targets)


@pytest.mark.parametrize(
    ("changed", "everything"),
    ((None, True), (True, True), (False, False)),
    ids=("path-mode", "pytest-section-changed", "other-sections-only"),
)
def test_registry_edit_selects_everything_only_when_pytest_sections_change(
    real_graph: Any, real_config: Any, changed: bool | None, everything: bool
) -> None:
    selection = _select(real_graph, real_config, REGISTRY_PATH, registry_pytest_changed=changed)

    assert selection.everything is everything
    if not everything:
        assert set(selection.targets) == _nothing_selected(real_graph, real_config)


def test_real_upgrade_pause_change_selects_worker_and_api_tests(
    real_graph: Any, real_config: Any
) -> None:
    assert {"apps/worker", "apps/api"} <= real_graph.package_dependents("packages/upgrade-pause")

    selection = _select(
        real_graph, real_config, "packages/upgrade-pause/src/curie_upgrade_pause/__init__.py"
    )
    targets = selection.targets

    assert not selection.everything
    assert any(path.startswith("apps/worker/tests/") for path in targets)
    assert any(path.startswith("apps/api/tests/") for path in targets)
    assert any(path.startswith("packages/upgrade-pause/tests/") for path in targets)


def test_real_api_change_does_not_select_aci_protocol_tests(
    real_graph: Any, real_config: Any
) -> None:
    selection = _select(real_graph, real_config, "apps/api/src/curie_api/auth.py")

    assert not selection.everything
    assert any(path.startswith("apps/api/tests/") for path in selection.targets)
    assert not any(path.startswith("packages/aci-protocol/tests/") for path in selection.targets)


@pytest.mark.parametrize(
    "path",
    (
        "apps/api/src/curie_api/auth.py",
        "packages/upgrade-pause/src/curie_upgrade_pause/__init__.py",
        "runner/src/curie_runner/__init__.py",
        "tools/ci-retry-gate/README.md",
    ),
)
def test_real_impact_selection_keeps_root_tests_and_drops_excluded(
    real_graph: Any, real_config: Any, path: str
) -> None:
    selection = _select(real_graph, real_config, path)
    targets = set(selection.targets)

    assert not selection.everything
    assert "tests/test_repository_facts.py" in real_graph.test_files
    assert _root_tests(real_graph, real_config) <= targets
    assert not any(_under_any(target, real_config.excluded) for target in targets)


COMPUTED_ROOT_READERS = {
    # The re-review shape: an anchored root joined with a runtime value.
    "anchored-environ": (
        ANCHOR_HEADER + "def test_reader() -> None:\n"
        '    path = Path(__file__).resolve().parents[2] / os.environ.get("INPUT_DIR", "new-dir")'
        ' / "SKILL.md"\n'
        "    assert path\n"
    ),
    "anchored-name": (
        ANCHOR_HEADER + "ROOT = Path(__file__).resolve().parents[2]\n\n\n"
        "def test_reader(name: str = 'x') -> None:\n    assert (ROOT / name).name\n"
    ),
    "fixture-root": (
        "def test_reader(repo_root, relative) -> None:\n    assert (repo_root / relative).name\n"
    ),
    "os-path-join": (
        "import os\n\nREPO_ROOT = os.getcwd()\n\n\n"
        "def test_reader(name: str = 'x') -> None:\n    assert os.path.join(REPO_ROOT, name)\n"
    ),
    "joinpath": (
        "from pathlib import Path\n\nROOT = Path.cwd()\n\n\n"
        "def test_reader(name: str = 'x') -> None:\n    assert ROOT.joinpath(name)\n"
    ),
}
DOC_DIFFS = ("docs/adr/0001-record.md", "AGENTS.md", "docs/guide.md")


def _reader_repo(root: Path, reader: str) -> Any:
    files = _workspace({"packages/z": ("pkg-z", ())}, extra_testpaths=("examples/tests",))
    files.update(
        {
            "examples/tests/test_reader.py": reader,
            "examples/tests/test_bystander.py": "def test_bystander() -> None:\n    assert True\n",
            "AGENTS.md": "# Agents\n",
            "docs/guide.md": "# Guide\n",
            "docs/adr/0001-record.md": "# Record\n",
            "new-dir/SKILL.md": "# Skill\n",
        }
    )
    return _build(root, files)


@pytest.mark.parametrize(
    "reader", tuple(COMPUTED_ROOT_READERS.values()), ids=tuple(COMPUTED_ROOT_READERS)
)
@pytest.mark.parametrize("changed", DOC_DIFFS)
def test_computed_root_reader_is_selected_by_documentation_only_diffs(
    tmp_path: Path, reader: str, changed: str
) -> None:
    graph = _reader_repo(tmp_path, reader)
    config = _config(always=[])

    selection = _select(graph, config, changed)

    assert graph.repo_wide("examples/tests/test_reader.py")
    assert not selection.everything
    assert set(selection.targets) == {"examples/tests/test_reader.py"}
    assert _impact().needs_pytest(
        graph,
        config,
        [changed],
        cli_files=[],
        workflow_jobs_changed=None,
        registry_pytest_changed=None,
    )


def test_computed_root_reader_of_an_unclaimed_path_runs_everything(tmp_path: Path) -> None:
    graph = _reader_repo(tmp_path, COMPUTED_ROOT_READERS["anchored-environ"])

    selection = _select(graph, _config(always=[]), "new-dir/SKILL.md")

    assert selection.everything
    assert selection.reason == "new-dir/SKILL.md unclaimed"


DOC_READERS = {
    "examples/tests/test_agents.py": (
        PATH_ROOT.replace("parents[3]", "parents[2]")
        + 'TEXT = (ROOT / "AGENTS.md").read_text()\n\n\n'
        "def test_agents() -> None:\n    assert TEXT\n"
    ),
    "examples/tests/test_guide.py": (
        PATH_ROOT.replace("parents[3]", "parents[2]") + 'GUIDE = ROOT / "docs" / "guide.md"\n\n\n'
        "def test_guide() -> None:\n    assert GUIDE\n"
    ),
    "examples/tests/test_docs_tree.py": (
        PATH_ROOT.replace("parents[3]", "parents[2]")
        + 'PAGES = sorted((ROOT / "docs").glob("*.md"))\n\n\n'
        "def test_pages() -> None:\n    assert PAGES is not None\n"
    ),
    "examples/tests/test_bystander.py": "def test_bystander() -> None:\n    assert True\n",
}


@pytest.mark.parametrize(
    ("changed", "readers"),
    (
        ("AGENTS.md", {"examples/tests/test_agents.py"}),
        ("docs/guide.md", {"examples/tests/test_guide.py", "examples/tests/test_docs_tree.py"}),
        ("docs/adr/0001-record.md", {"examples/tests/test_docs_tree.py"}),
        ("docs/unread/page.md", {"examples/tests/test_docs_tree.py"}),
        ("README.md", set()),
    ),
)
def test_documentation_a_test_reads_selects_its_readers(
    tmp_path: Path, changed: str, readers: set[str]
) -> None:
    files = _workspace({"packages/z": ("pkg-z", ())}, extra_testpaths=("examples/tests",))
    files.update(DOC_READERS)
    files.update(
        {
            "AGENTS.md": "# Agents\n",
            "README.md": "# Readme\n",
            "docs/guide.md": "# Guide\n",
            "docs/adr/0001-record.md": "# Record\n",
        }
    )
    graph = _build(tmp_path, files)
    config = _config(always=[])

    selection = _select(graph, config, changed)
    needed = _impact().needs_pytest(
        graph,
        config,
        [changed],
        cli_files=[],
        workflow_jobs_changed=None,
        registry_pytest_changed=None,
    )

    assert not graph.repo_wide("examples/tests/test_docs_tree.py")
    assert needed is bool(readers)
    if readers:
        assert not selection.everything
        assert set(selection.targets) == readers
    else:
        assert selection.reason == "empty impact"


@pytest.mark.parametrize(
    ("path", "required"),
    (
        # The recipe that used to live in AGENTS.md; the revision gate test reads it.
        ("docs/local-verification.md", "apps/api/tests/test_alembic_revision_gate.py"),
        # Read by no runnable test now, so only the always-selected set runs.
        ("AGENTS.md", "tools/doclint/tests/test_counts.py"),
    ),
)
def test_documentation_only_pull_request_runs_its_readers(
    tmp_path: Path, real_graph: Any, path: str, required: str
) -> None:
    targets = tmp_path / "targets.txt"

    completed, output = _run_selector(tmp_path, "--path", path, targets_out=targets)

    assert completed.returncode == 0, completed.stderr
    assert output["pytest"] == "true"
    assert output["pytest_scope"] == "impact"
    selected = set(targets.read_text(encoding="utf-8").split())
    assert required in selected
    assert real_graph.repo_wide("tools/doclint/tests/test_counts.py")


@pytest.mark.parametrize(
    ("changed", "required"),
    (
        (
            "examples/turn-canary/probe_profiles.py",
            {"examples/tests/test_turn_canary_profile_module.py"},
        ),
        (
            "examples/turn-canary/turn_canary.py",
            {
                "examples/tests/test_turn_canary_profile_module.py",
                "examples/tests/test_turn_canary.py",
            },
        ),
        (
            "packages/plugin-format/src/plugin_format/tool_policy.py",
            {"tools/sre-contract/tests/test_shipped_policy.py"},
        ),
    ),
)
def test_real_review_regressions_select_the_reaching_tests(
    real_graph: Any, real_config: Any, changed: str, required: set[str]
) -> None:
    selection = _select(real_graph, real_config, changed)

    assert not selection.everything
    assert required <= set(selection.targets)


@pytest.mark.parametrize(
    ("changed", "everything"),
    (
        ("new-dir/input.json", True),
        ("new-dir/SKILL.md", True),
        ("docs/adr/0001-record-architecture-decisions.md", False),
    ),
)
def test_real_unclaimed_path_selects_everything_unless_ignored(
    real_graph: Any, real_config: Any, changed: str, everything: bool
) -> None:
    selection = _select(real_graph, real_config, changed)

    assert selection.everything is everything


# plan_shards and conftest groups --------------------------------------------

SHARD_CORE = tuple(f"packages/s/tests/test_core_{index:02d}.py" for index in range(48))
SHARD_STORAGE = "packages/s/tests/test_storage.py"
SHARD_FULL = "packages/s/tests/test_full.py"
SHARD_EMPTY = "packages/s/tests/test_helpers_only.py"
SHARED_GROUP = "shared-live-state"
PROTECTED_GROUP = "protected-source-runner-port"
SYNTHETIC_ROOT_CONFTEST = f'''"""Synthetic root conftest."""

from __future__ import annotations

SHARED_LIVE_STATE_GROUP = "{SHARED_GROUP}"
PROTECTED_RUNNER_GROUP = "{PROTECTED_GROUP}"
PROTECTED_RUNNER_FILES = frozenset(
    {{
        # Share a fixed host port.
        "{SHARD_CORE[1]}",
        "{SHARD_CORE[2]}",
    }}
)

SHARED_LIVE_STATE_FILES = frozenset(
    {{
        "{SHARD_CORE[0]}",
        "{SHARD_FULL}",
    }}
)
'''


@pytest.fixture(scope="module")
def shard_repo(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Any]:
    files = _workspace({"packages/s": ("pkg-s", ())}, member_tests=False)
    for path in SHARD_CORE:
        files[path] = "def test_one() -> None:\n    assert True\n"
    files[SHARD_STORAGE] = (
        'OBJECT_STORE = "http://localhost:29000/rustfs-bundles"\n\n\n'
        "def test_object_store() -> None:\n    assert OBJECT_STORE\n"
    )
    files[SHARD_FULL] = (
        'LANGFUSE_URL = "http://localhost:23000"\n\n\n'
        "def test_langfuse() -> None:\n    assert LANGFUSE_URL\n"
    )
    files[SHARD_EMPTY] = "def helper() -> int:\n    return 1\n"
    files["conftest.py"] = SYNTHETIC_ROOT_CONFTEST
    root = tmp_path_factory.mktemp("shard-repo")
    return root, _build(root, files)


def _plan(
    graph: Any, files: Sequence[str], groups: Mapping[str, str] | None = None, **cfg: Any
) -> Any:
    return _impact().plan_shards(graph, _config(**cfg), list(files), dict(groups or {}))


def _shard_plan(shards: int, storage: int, full: int) -> Any:
    return _impact().ShardPlan(shards=shards, storage_shards=storage, full_shards=full)


def test_shard_fixture_tiers_and_weights(shard_repo: tuple[Path, Any]) -> None:
    _, graph = shard_repo

    assert graph.service_tier(SHARD_CORE[0]) == 0
    assert graph.service_tier(SHARD_STORAGE) == 1
    assert graph.service_tier(SHARD_FULL) == 2
    assert graph.weight(SHARD_CORE[0]) == 1
    assert graph.weight(SHARD_EMPTY) == 0


def test_shard_count_is_one_for_a_tiny_selection(shard_repo: tuple[Path, Any]) -> None:
    _, graph = shard_repo

    assert _plan(graph, [SHARD_CORE[0]]) == _shard_plan(1, 0, 0)


def test_shard_count_grows_with_the_selected_share(shard_repo: tuple[Path, Any]) -> None:
    _, graph = shard_repo

    # ceil(24 * 4 / 50) = 2 and ceil(24 * 10 / 50) = 5.
    assert _plan(graph, SHARD_CORE[:4]).shards == 2
    assert _plan(graph, SHARD_CORE[:10]).shards == 5


def test_shard_count_never_exceeds_the_weighted_groups(shard_repo: tuple[Path, Any]) -> None:
    _, graph = shard_repo
    two_groups = {path: f"group-{index % 2}" for index, path in enumerate(SHARD_CORE[:10])}

    assert _plan(graph, SHARD_CORE[:10], two_groups).shards == 2
    # A zero-weight file is not a group: one weighted file means one shard.
    assert _plan(graph, [SHARD_CORE[0], SHARD_EMPTY], min_shard_share=1000).shards == 1
    for count in range(1, 8):
        assert _plan(graph, SHARD_CORE[:count], min_shard_share=1000).shards == min(6, count)


def test_large_selection_uses_max_shards(shard_repo: tuple[Path, Any]) -> None:
    _, graph = shard_repo
    universe = [*SHARD_CORE, SHARD_STORAGE, SHARD_FULL, SHARD_EMPTY]

    assert _plan(graph, universe).shards == 6
    assert _plan(graph, universe, max_shards=4).shards == 4
    assert _plan(graph, SHARD_CORE).shards == 6


def test_full_shards_are_zero_without_a_tier_two_file(shard_repo: tuple[Path, Any]) -> None:
    _, graph = shard_repo

    core_only = _plan(graph, SHARD_CORE)
    with_storage = _plan(graph, [*SHARD_CORE, SHARD_STORAGE])

    assert core_only == _shard_plan(6, 0, 0)
    assert with_storage.full_shards == 0
    assert with_storage.storage_shards >= 1


def test_selected_tier_two_file_gets_full_shards(shard_repo: tuple[Path, Any]) -> None:
    _, graph = shard_repo

    plan = _plan(graph, [*SHARD_CORE, SHARD_FULL])

    assert plan == _shard_plan(6, 1, 1)
    alone = _plan(graph, [SHARD_FULL])
    assert alone == _shard_plan(1, 1, 1)


@pytest.mark.parametrize(
    "files",
    (
        [SHARD_FULL, SHARD_STORAGE],
        [*SHARD_CORE[:5], SHARD_FULL],
        [*SHARD_CORE[:20], SHARD_STORAGE, SHARD_FULL],
        [*SHARD_CORE, SHARD_STORAGE, SHARD_FULL],
    ),
    ids=("two", "six", "twenty-two", "all"),
)
def test_storage_shards_cover_full_shards(shard_repo: tuple[Path, Any], files: list[str]) -> None:
    _, graph = shard_repo

    plan = _plan(graph, files, min_shard_share=1000)

    assert 1 <= plan.full_shards <= plan.storage_shards <= plan.shards


def test_shard_plan_counts_a_conftest_group_once_with_its_max_tier(
    shard_repo: tuple[Path, Any],
) -> None:
    _, graph = shard_repo
    grouped = {SHARD_CORE[0]: SHARED_GROUP, SHARD_FULL: SHARED_GROUP}

    pair = [SHARD_CORE[0], SHARD_FULL]
    assert _plan(graph, pair, min_shard_share=1000).shards == 2
    assert _plan(graph, pair, grouped, min_shard_share=1000) == _shard_plan(1, 1, 1)

    # Ten core files plus the tier 2 file. Grouped, the tier 2 group weighs 2 of 11:
    # ceil(6 * 2 / 11) = 2 full shards; ungrouped it weighs 1: ceil(6 / 11) = 1.
    eleven = [*SHARD_CORE[:10], SHARD_FULL]
    assert _plan(graph, eleven, min_shard_share=1000) == _shard_plan(6, 1, 1)
    assert _plan(graph, eleven, grouped, min_shard_share=1000) == _shard_plan(6, 2, 2)


def test_shard_groups_come_from_the_root_conftest_frozensets(
    shard_repo: tuple[Path, Any],
) -> None:
    root, graph = shard_repo

    groups = _impact().conftest_groups(root)

    assert groups[SHARD_CORE[0]] == SHARED_GROUP
    assert groups[SHARD_FULL] == SHARED_GROUP
    assert groups[SHARD_CORE[1]] == PROTECTED_GROUP
    assert groups[SHARD_CORE[2]] == PROTECTED_GROUP
    assert groups.get(SHARD_CORE[3], SHARD_CORE[3]) == SHARD_CORE[3]
    assert _plan(graph, [SHARD_CORE[0], SHARD_FULL], groups, min_shard_share=1000) == (
        _shard_plan(1, 1, 1)
    )


def test_shard_groups_parse_the_real_root_conftest() -> None:
    groups = _impact().conftest_groups(REPO_ROOT)

    assert groups["runner/tests/test_otel.py"] == SHARED_GROUP
    assert groups["apps/worker/tests/test_protected_original_cap.py"] == PROTECTED_GROUP


def test_static_weight_counts_module_and_test_class_functions(tmp_path: Path) -> None:
    files = _workspace({"packages/w": ("pkg-w", ())}, member_tests=False)
    files["packages/w/tests/test_weights.py"] = (
        "def helper() -> None:\n"
        "    def test_nested() -> None:\n"
        "        pass\n\n\n"
        "def test_module() -> None:\n    pass\n\n\n"
        "async def test_async() -> None:\n    pass\n\n\n"
        "class TestThing:\n"
        "    def test_a(self) -> None:\n        pass\n\n"
        "    def test_b(self) -> None:\n        pass\n\n"
        "    def helper(self) -> None:\n        pass\n\n\n"
        "class Helper:\n"
        "    def test_not_collected(self) -> None:\n        pass\n"
    )
    graph = _build(tmp_path, files)

    assert graph.weight("packages/w/tests/test_weights.py") == 4


def test_targets_digest_is_sha256_of_newline_joined_targets() -> None:
    targets = ["a/tests/test_a.py", "b/tests/test_b.py"]

    assert (
        _impact().targets_digest(targets) == hashlib.sha256("\n".join(targets).encode()).hexdigest()
    )
    assert _impact().targets_digest([]) == ""


# Selector CLI ---------------------------------------------------------------


def _run_selector(
    tmp_path: Path,
    *options: str,
    cwd: Path = REPO_ROOT,
    registry: Path = REGISTRY,
    targets_out: Path | None = None,
) -> tuple[subprocess.CompletedProcess[str], dict[str, str]]:
    output_path = tmp_path / f"github-output-{len(list(tmp_path.glob('github-output-*')))}"
    command = [sys.executable, str(SELECTOR), "--registry", str(registry), *options]
    if targets_out is not None:
        command.extend(("--pytest-targets-out", str(targets_out)))
    completed = subprocess.run(
        command,
        cwd=cwd,
        env={**os.environ, "GITHUB_OUTPUT": str(output_path)},
        capture_output=True,
        text=True,
        check=False,
    )
    output: dict[str, str] = {}
    if output_path.exists():
        for line in output_path.read_text(encoding="utf-8").splitlines():
            key, _, value = line.partition("=")
            output[key] = value
    return completed, output


def _shard_outputs(output: dict[str, str]) -> tuple[int, int, int]:
    return (
        int(output["pytest_shards"]),
        int(output["pytest_storage_shards"]),
        int(output["pytest_full_shards"]),
    )


def test_push_non_pull_request_event_runs_full_scope_on_six_shards(tmp_path: Path) -> None:
    targets_out = tmp_path / "targets.txt"

    completed, output = _run_selector(tmp_path, "--push", targets_out=targets_out)

    assert completed.returncode == 0, completed.stderr
    assert output["pytest_scope"] == "full"
    assert output["pytest_targets"] == ""
    assert _shard_outputs(output) == (6, 6, 6)
    assert targets_out.read_text(encoding="utf-8").strip() == ""


def test_push_without_targets_out_still_writes_the_shard_outputs(tmp_path: Path) -> None:
    completed, output = _run_selector(tmp_path, "--push", "--omit-kind")

    assert completed.returncode == 0, completed.stderr
    assert output["pytest_scope"] == "full"
    assert output["pytest_targets"] == ""
    assert output["pytest_shards"] == "6"
    assert output["pytest_storage_shards"] == "6"
    assert output["pytest_full_shards"] == "6"


def test_runner_pull_request_writes_impact_targets_and_shard_plan(
    tmp_path: Path, real_graph: Any, real_config: Any
) -> None:
    targets_out = tmp_path / "targets.txt"
    excluded = _registry_document()["pytest_impact"]["excluded"]

    completed, output = _run_selector(
        tmp_path, "--path", "runner/src/curie_runner/__init__.py", targets_out=targets_out
    )

    assert completed.returncode == 0, completed.stderr
    assert output["pytest_scope"] == "impact"
    assert DIGEST.fullmatch(output["pytest_targets"])
    lines = targets_out.read_text(encoding="utf-8").splitlines()
    assert lines and lines == sorted(lines)
    assert len(lines) == len(set(lines))
    assert any(line.startswith("runner/tests/") for line in lines)
    assert _root_tests(real_graph, real_config) <= set(lines)
    assert not [line for line in lines if _under_any(line, excluded)]
    assert output["pytest_targets"] == hashlib.sha256("\n".join(lines).encode()).hexdigest()
    assert output["pytest_targets"] == _impact().targets_digest(lines)
    shards, storage, full = _shard_outputs(output)
    assert 1 <= shards <= 6
    assert 0 <= full <= storage <= shards
    assert "impact" in completed.stdout


@pytest.mark.parametrize("path", (WORKFLOW_PATH, REGISTRY_PATH))
def test_path_mode_workflow_or_registry_change_is_full_scope_on_six_shards(
    tmp_path: Path, path: str
) -> None:
    targets_out = tmp_path / "targets.txt"

    completed, output = _run_selector(tmp_path, "--path", path, targets_out=targets_out)

    assert completed.returncode == 0, completed.stderr
    assert output["pytest_scope"] == "full"
    assert output["pytest_targets"] == ""
    shards, storage, full = _shard_outputs(output)
    assert shards == 6
    assert 1 <= full <= storage <= shards
    assert targets_out.read_text(encoding="utf-8").strip() == ""


def _git(repo: Path, *args: str) -> str:
    completed = subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True, check=True)
    return completed.stdout.strip()


def _workflow(**runs: str) -> str:
    jobs = ("changes", "python-pytest", "rust-lint", *UPGRADE_JOBS)
    # Selection reads the candidate chart job even when the changed hunk is
    # elsewhere, so keep its real steps in this otherwise minimal workflow.
    chart_job = _registry_document()["chart_scenarios"]["job"]
    chart = yaml.safe_load((REPO_ROOT / WORKFLOW_PATH).read_text())["jobs"][chart_job]
    chart_text = yaml.safe_dump({chart_job: chart}, sort_keys=False)
    return (
        "name: Example CI\njobs:\n"
        + "".join("  " + line for line in chart_text.splitlines(keepends=True))
        + "".join(
            f"  {job}:\n    runs-on: ubuntu-latest\n    steps:\n"
            f"      - run: {runs.get(job.replace('-', '_'), f'echo {job}')}\n"
            for job in jobs
        )
    )


def _dump_registry(document: Mapping[str, Any]) -> str:
    return yaml.safe_dump(dict(document), sort_keys=False)


@pytest.fixture
def revision_repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Example Tester")
    _git(repo, "config", "user.email", "tester@example.com")
    (repo / ".github" / "workflows").mkdir(parents=True)
    (repo / WORKFLOW_PATH).write_text(_workflow(), encoding="utf-8")
    (repo / REGISTRY_PATH).write_text(_dump_registry(_registry_document()), encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "base")
    return repo, _git(repo, "rev-parse", "HEAD")


def _commit(repo: Path, relative: str, content: str) -> str:
    (repo / relative).write_text(content, encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "candidate")
    return _git(repo, "rev-parse", "HEAD")


@pytest.mark.parametrize(
    ("runs", "scope"),
    (
        ({"python_pytest": "echo changed"}, "full"),
        ({"rust_lint": "echo changed"}, "impact"),
    ),
    ids=("python-pytest-hunk", "unrelated-job-hunk"),
)
def test_revision_mode_workflow_hunks_decide_the_pytest_scope(
    revision_repo: tuple[Path, str], tmp_path: Path, runs: dict[str, str], scope: str
) -> None:
    repo, base = revision_repo
    head = _commit(repo, WORKFLOW_PATH, _workflow(**runs))

    completed, output = _run_selector(
        tmp_path,
        "--base",
        base,
        "--head",
        head,
        cwd=repo,
        registry=repo / REGISTRY_PATH,
    )

    assert completed.returncode == 0, completed.stderr
    assert output["pytest"] == "true"
    assert output["pytest_scope"] == scope
    if scope == "impact":
        assert DIGEST.fullmatch(output["pytest_targets"])
    else:
        assert output["pytest_targets"] == ""


def _registry_with_chart_input() -> dict[str, Any]:
    document = deepcopy(_registry_document())
    scenario = document["chart_scenarios"]["scenarios"]["runner-resources-render"]
    scenario["inputs"].append("charts/curie/ci/runtime/extra-runtime.sh")
    return document


def _registry_with_pytest_impact_edit() -> dict[str, Any]:
    document = deepcopy(_registry_document())
    document["pytest_impact"]["min_shard_share"] += 1
    return document


@pytest.mark.parametrize(
    ("edit", "scope"),
    (
        (_registry_with_chart_input, "impact"),
        (_registry_with_pytest_impact_edit, "full"),
    ),
    ids=("chart-scenarios-only", "pytest-impact"),
)
def test_revision_mode_registry_edit_forces_full_only_for_pytest_sections(
    revision_repo: tuple[Path, str],
    tmp_path: Path,
    edit: Any,
    scope: str,
) -> None:
    repo, base = revision_repo
    head = _commit(repo, REGISTRY_PATH, _dump_registry(edit()))

    completed, output = _run_selector(
        tmp_path,
        "--base",
        base,
        "--head",
        head,
        cwd=repo,
        registry=repo / REGISTRY_PATH,
    )

    assert completed.returncode == 0, completed.stderr
    assert output["pytest"] == "true"
    assert output["pytest_scope"] == scope
