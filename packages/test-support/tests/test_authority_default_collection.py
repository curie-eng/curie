"""Default authority grammar coverage, @spec PROTECTED-HOOK-LANE-2."""

from __future__ import annotations

import ast
import subprocess
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
AUTHORITY_ROOT = "packages/protected-hooks/tests"
AUTHORITY_FILE = AUTHORITY_ROOT + "/test_authority_records.py"


def test_authority_grammar_is_a_default_collection_root() -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    with (REPO_ROOT / "pyproject.toml").open("rb") as source:
        config = tomllib.load(source)
    assert AUTHORITY_ROOT in config["tool"]["pytest"]["ini_options"]["testpaths"], (
        "PROTECTED-HOOK-LANE-2: authority grammar omitted from default pytest roots"
    )


def test_default_pytest_actually_collects_each_authority_grammar_test() -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    tree = ast.parse((REPO_ROOT / AUTHORITY_FILE).read_text(encoding="utf-8"))
    expected = {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.startswith("test_")
    }
    assert expected, "authority grammar suite must contain executable tests"
    # No path argument: this exercises the repository's real default inventory.
    # The module selector avoids collecting this regression into its own child.
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", "-k", "test_authority_records"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    prefix = AUTHORITY_FILE + "::"
    collected = {
        line[len(prefix) :].split("[", 1)[0]
        for line in result.stdout.splitlines()
        if line.startswith(prefix)
    }
    assert expected <= collected, (
        "PROTECTED-HOOK-LANE-2: default collection omitted authority tests: "
        + ", ".join(sorted(expected - collected))
    )
    assert result.returncode == 0, "default authority collection must finish without errors"
