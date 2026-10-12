"""The offline factory lifecycle on each Bitbucket fake (#4401, AC 10).

`forge_fakes.bitbucket_lifecycle` holds the lifecycle and its seams; this
module runs it once per kind against that kind's stateful fake. The Data
Center fake's backend lives with the Data Center fake and is imported only
for its case. The live Data Center fixture runs the same lifecycle from
``bitbucket_dc_fixture/test_lifecycle.py``.
"""

from __future__ import annotations

import importlib
from collections.abc import Iterator
from pathlib import Path

import pytest
from forge_fakes.bitbucket_lifecycle import Backend, BitbucketPair, bitbucket_pair, run_lifecycle

pytestmark = pytest.mark.usefixtures("clean_db")

BACKENDS = {
    "bitbucket_cloud": ("forge_fakes.bitbucket_cloud", "BitbucketCloudFakeBackend"),
    "bitbucket_dc": ("forge_fakes.bitbucket_dc", "BitbucketDcFakeBackend"),
}


@pytest.fixture(params=sorted(BACKENDS))
def pair(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Iterator[BitbucketPair]:
    module, name = BACKENDS[request.param]
    backend: Backend = getattr(importlib.import_module(module), name)()
    assert backend.kind == request.param
    yield from bitbucket_pair(backend, monkeypatch, tmp_path)


def test_bitbucket_lifecycle_runs_offline(pair: BitbucketPair) -> None:
    run_lifecycle(pair)
