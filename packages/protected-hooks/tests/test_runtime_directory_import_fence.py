"""The import contract that fences the runtime directory writer, @spec PROTECTED-HOOK-SOURCE-11.

Reads the real contract from the repository ``pyproject.toml`` and runs
``lint-imports`` over synthetic trees: every listed source that imports the
writer breaks the contract, and a clean tree passes.
"""

from __future__ import annotations

import os
import re
import subprocess
import tomllib
from pathlib import Path

import pytest

CONTRACT = "The runtime directory writer stays out of ordinary compositions"
WRITER = "curie_protected_hooks.runtime_directory_writer"
REQUIRED = {
    "curie_api",
    "curie_worker",
    "curie_dispatcher",
    "curie_runner",
    "curie_internal",
    "curie_upgrade_pause",
}


def repo_root() -> Path:
    """The repository root holding pyproject.toml, @spec PROTECTED-HOOK-SOURCE-11."""
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").is_file() and (parent / "apps").is_dir():
            return parent
    raise AssertionError("repository root not found")


def contract() -> dict[str, list[str]]:
    """The named contract from pyproject.toml, @spec PROTECTED-HOOK-SOURCE-11."""
    data = tomllib.loads((repo_root() / "pyproject.toml").read_text())
    found = [c for c in data["tool"]["importlinter"]["contracts"] if c["name"] == CONTRACT]
    assert len(found) == 1
    return found[0]


def build_tree(tree: Path, sources: list[str], importing: bool) -> None:
    """Synthetic packages; each source imports the writer when ``importing``."""
    packages: dict[str, dict[str, str]] = {"curie_protected_hooks": {"__init__": ""}}
    packages["curie_protected_hooks"]["runtime_directory_writer"] = ""
    line = f"import {WRITER}\n" if importing else "VALUE = 1\n"
    for source in sources:
        top, *rest = source.split(".")
        modules = packages.setdefault(top, {"__init__": ""})
        modules[rest[0] if rest else "__init__"] = line
    for package, modules in packages.items():
        directory = tree / package
        directory.mkdir()
        for name, text in modules.items():
            (directory / f"{name}.py").write_text(text)


def reported(output: str, source: str) -> bool:
    """Whether the report names exactly this violator, @spec PROTECTED-HOOK-SOURCE-11."""
    return re.search(rf"^{re.escape(source)} is not allowed to import", output, re.M) is not None


def run_linter(tree: Path, sources: list[str]) -> subprocess.CompletedProcess[str]:
    """lint-imports over a synthetic tree, @spec PROTECTED-HOOK-SOURCE-11."""
    spec = contract()
    roots = sorted({source.split(".")[0] for source in sources} | {"curie_protected_hooks"})
    config = tree / ".importlinter"
    config.write_text(
        "[importlinter]\n"
        f"root_packages =\n    {chr(10).join('    ' + r for r in roots).strip()}\n\n"
        "[importlinter:contract:1]\n"
        f"name = {CONTRACT}\n"
        "type = forbidden\n"
        "source_modules =\n"
        + "".join(f"    {s}\n" for s in sources)
        + "forbidden_modules =\n"
        + "".join(f"    {m}\n" for m in spec["forbidden_modules"])
    )
    env = {**os.environ, "PYTHONPATH": str(tree)}
    return subprocess.run(
        ["lint-imports", "--config", str(config)],
        cwd=tree,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )


def test_contract_lists_every_ordinary_composition() -> None:
    """The contract names every ordinary composition, @spec PROTECTED-HOOK-SOURCE-11."""
    assert REQUIRED <= set(contract()["source_modules"])
    assert contract()["forbidden_modules"] == [WRITER]


def test_every_listed_source_importing_the_writer_breaks_the_contract(tmp_path: Path) -> None:
    """Each listed source that imports the writer is reported, @spec PROTECTED-HOOK-SOURCE-11."""
    sources = list(contract()["source_modules"])
    build_tree(tmp_path, sources, importing=True)
    result = run_linter(tmp_path, sources)
    output = result.stdout + result.stderr
    assert result.returncode != 0, output
    assert "BROKEN" in output and CONTRACT in output
    for source in sources:
        assert reported(output, source), source


def test_clean_tree_passes(tmp_path: Path) -> None:
    """Without the import the contract is kept, @spec PROTECTED-HOOK-SOURCE-11."""
    sources = list(contract()["source_modules"])
    build_tree(tmp_path, sources, importing=False)
    result = run_linter(tmp_path, sources)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "KEPT" in result.stdout


@pytest.mark.parametrize(
    "source", ["curie_api", "curie_worker.__main__", "curie_protected_hooks.runtime_directory"]
)
def test_single_source_is_reported_alone(tmp_path: Path, source: str) -> None:
    """A lone importing source breaks the contract by itself, @spec PROTECTED-HOOK-SOURCE-11."""
    build_tree(tmp_path, [source], importing=True)
    result = run_linter(tmp_path, [source])
    assert result.returncode != 0
    assert reported(result.stdout, source)
