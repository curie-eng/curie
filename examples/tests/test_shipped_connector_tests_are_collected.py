"""Every ``build:``-declaring connector's own tests are actually collected.

The failure this exists to stop, from the install it actually happened on:

The source-built SRE-bot connectors (``tempo`` and ``self-upgrade``) each ship a
``test_server.py`` next to their Dockerfile. The
tests exist, they are green when run by hand, and CI builds and
``release.yaml`` publishes the images regardless of whether anyone ever runs
them. But ``pyproject.toml``'s ``[tool.pytest.ini_options] testpaths`` never
listed the directory those tests live in, so ``uv run pytest -q`` never
collected a single one of them, on any branch. Nothing reported it -- the
suite was green, coverage looked fine, and a shipped, published connector's
tests simply never ran (#2093).

The check is deliberately shallow and cheap: it reads the declaration and
``pyproject.toml``, not pytest's actual collection output. It cannot tell you
whether a collected test currently passes. It is parametrized per
build-declaring connector, not per test file, so a connector that ships zero
test files fails its own case instead of silently vanishing from the
parametrize list while its siblings keep the suite green -- the same
per-connector requirement its sibling check,
``test_build_connectors_are_published.py``, already holds every build-declaring
connector to. For each connector it tells you whether the connector shipped at
least one test file at all, and whether every one of those files falls under a
configured ``testpaths`` root, which is the failure that stayed invisible.
"""

import tomllib
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
EXAMPLES = REPO / "examples"
PYPROJECT = REPO / "pyproject.toml"


def _build_connector_dirs() -> list[Path]:
    """Directories of every connector declaring ``build:`` in its bundle."""

    found: list[Path] = []
    for declaration in sorted(EXAMPLES.glob("*/connectors.yaml")):
        parsed = yaml.safe_load(declaration.read_text(encoding="utf-8")) or {}
        for name, spec in (parsed.get("connectors") or {}).items():
            if isinstance(spec, dict) and "build" in spec:
                found.append(declaration.parent / "connectors" / str(name))
    return found


def _test_files() -> list[Path]:
    """Every ``test_*.py`` file directly inside a build-declaring connector dir."""

    files: list[Path] = []
    for connector_dir in _build_connector_dirs():
        if connector_dir.is_dir():
            files.extend(sorted(connector_dir.glob("test_*.py")))
    return files


def _testpaths() -> list[str]:
    with PYPROJECT.open("rb") as handle:
        parsed = tomllib.load(handle)
    return list(parsed["tool"]["pytest"]["ini_options"]["testpaths"])


def _is_collected(test_file: Path, testpaths: list[str]) -> bool:
    relative = test_file.relative_to(REPO)
    for root in testpaths:
        root_path = Path(root)
        if relative == root_path or root_path in relative.parents:
            return True
    return False


def test_the_discovery_finds_connectors_and_test_files() -> None:
    # A guard that silently finds nothing passes vacuously, which is the
    # failure mode of every check that reads files by glob.
    assert _build_connector_dirs(), "no build-declaring connectors found: check the glob"
    assert _test_files(), "no connector test files found: check the glob"


@pytest.mark.parametrize(
    "connector_dir",
    _build_connector_dirs(),
    ids=[str(d.relative_to(REPO)) for d in _build_connector_dirs()],
)
def test_a_connectors_tests_are_collected(connector_dir: Path) -> None:
    relative_dir = connector_dir.relative_to(REPO)
    test_files = sorted(connector_dir.glob("test_*.py")) if connector_dir.is_dir() else []
    assert test_files, (
        f"'{relative_dir}' declares build: but ships no test_*.py file. The "
        f"image it produces is still built by CI and published by "
        f"release.yaml with no test signal at all. Add a test_*.py file next "
        f"to the connector's Dockerfile, or drop the connector."
    )

    testpaths = _testpaths()
    for test_file in test_files:
        relative_file = test_file.relative_to(REPO)
        assert _is_collected(test_file, testpaths), (
            f"'{relative_file}' exists but falls under none of "
            f"pyproject.toml's testpaths {testpaths!r}, so `uv run pytest -q` "
            f"never collects it. The image it tests is still built by CI and "
            f"published by release.yaml with no test signal at all. Add the "
            f"connector's directory (or its bundle's connectors root) to "
            f"testpaths."
        )
