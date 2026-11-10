"""The offline factory lifecycle against real Bitbucket Data Center 9.4.24 (#4401 AC 10).

The same `run_lifecycle` the fakes pass (``test_bitbucket_lifecycle.py``), with
the live fixture as the backend. Select it with ``-k lifecycle``.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.usefixtures("clean_db")


@pytest.fixture
def pair(live: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[Any]:
    from forge_fakes.bitbucket_lifecycle import BRANCH, bitbucket_pair

    # The lifecycle's branch is the factory's own name for the work item, and a
    # declined pull request still counts as one on it, so it runs once per
    # fixture: refuse a reused one by name instead of failing midway.
    existing = live.helpers._call(
        live.state,
        "admin",
        "GET",
        f"{live.helpers._repo_api(live.state)}/pull-requests",
        None,
        {"at": f"refs/heads/{BRANCH}", "direction": "OUTGOING", "state": "ALL"},
    )["values"]
    if existing:
        pytest.fail(f"the lifecycle needs a fresh fixture: {BRANCH} already has a pull request")
    key_dir = tmp_path / "keys"
    key_dir.mkdir()

    yield from bitbucket_pair(live, monkeypatch, key_dir)


def test_bitbucket_dc_fixture_lifecycle(pair: Any) -> None:
    from forge_fakes.bitbucket_lifecycle import run_lifecycle

    run_lifecycle(pair)
