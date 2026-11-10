"""Default authority grammar coverage, @spec PROTECTED-HOOK-LANE-2."""

from __future__ import annotations

import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
AUTHORITY_ROOT = "packages/protected-hooks/tests"


def test_authority_grammar_is_a_default_collection_root() -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    with (REPO_ROOT / "pyproject.toml").open("rb") as source:
        config = tomllib.load(source)
    assert AUTHORITY_ROOT in config["tool"]["pytest"]["ini_options"]["testpaths"], (
        "PROTECTED-HOOK-LANE-2: authority grammar omitted from default pytest roots"
    )
