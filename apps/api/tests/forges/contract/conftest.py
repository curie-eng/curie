"""The forge contract suite's adapter registry (ADR 0197, "Two ports" item 5).

Every vector in this directory runs once per entry in ``HARNESSES``. A new
adapter pair joins by appending one ``pytest.param`` here whose factory returns
an `AdapterHarness` (``apps/api/tests/forge_fakes/contract_harness.py``); the
vectors themselves are never edited for an adapter.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from forge_fakes.contract_harness import AdapterHarness, InMemoryHarness

HarnessFactory = Callable[[], AdapterHarness]

HARNESSES: list[object] = [
    pytest.param(lambda: InMemoryHarness(minimal=False), id="memory"),
    pytest.param(lambda: InMemoryHarness(minimal=True), id="memory-minimal"),
]


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(params=HARNESSES)
def harness(request: pytest.FixtureRequest) -> AdapterHarness:
    factory: HarnessFactory = request.param
    return factory()
