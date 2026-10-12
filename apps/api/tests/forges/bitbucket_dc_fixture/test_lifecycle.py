"""The offline factory lifecycle against real Bitbucket Data Center 9.4.24 (#4401 AC 10).

The same `run_lifecycle` the fakes pass (``test_bitbucket_lifecycle.py``), with
the live fixture as the backend. Select it with ``-k lifecycle``.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def pair(live: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[Any]:
    key_dir = tmp_path / "keys"
    key_dir.mkdir()
    from forge_fakes.bitbucket_lifecycle import bitbucket_pair

    yield from bitbucket_pair(live, monkeypatch, key_dir)


def test_bitbucket_dc_fixture_lifecycle(pair: Any) -> None:
    from forge_fakes.bitbucket_lifecycle import run_lifecycle

    run_lifecycle(pair)
