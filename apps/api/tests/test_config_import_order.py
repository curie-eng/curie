"""Settings holds a ForgesConfig, so the config modules must import in any order (#4397).

``curie_api.forges.config`` once imported ``valid_base_branch`` from
``curie_api.config``; with ``Settings`` holding a ``ForgesConfig`` that is a
cycle. The branch rule now lives in the leaf ``curie_api.branches``. Each
order runs in a fresh isolated interpreter, because within one test process
every module is already imported and no order can be observed.
"""

from __future__ import annotations

import itertools
import subprocess
import sys
from pathlib import Path

import pytest

MODULES = ("curie_api.config", "curie_api.forges.config", "curie_api.forges.registry")
API_SOURCE = Path(__file__).resolve().parents[1] / "src"


def _run(code: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            f"import sys; sys.path.insert(0, {str(API_SOURCE)!r}); {code}",
        ],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


@pytest.mark.parametrize("order", list(itertools.permutations(MODULES)), ids=" then ".join)
def test_the_config_modules_import_in_every_order(order: tuple[str, ...]) -> None:
    result = _run("; ".join(f"import {module}" for module in order))
    assert result.returncode == 0, result.stderr


def test_the_branch_rule_lives_only_in_its_leaf_module() -> None:
    result = _run(
        "import curie_api.config, curie_api.branches; "
        "assert not hasattr(curie_api.config, 'valid_base_branch'); "
        "assert curie_api.branches.valid_base_branch('main'); "
        "assert not curie_api.branches.valid_base_branch('-x')"
    )
    assert result.returncode == 0, result.stderr


def test_the_branch_module_imports_nothing_from_curie_api() -> None:
    result = _run(
        "import curie_api.branches, sys; "
        "loaded = sorted(m for m in sys.modules if m.startswith('curie_api.') "
        "and m != 'curie_api.branches'); "
        "assert loaded == [], loaded"
    )
    assert result.returncode == 0, result.stderr
