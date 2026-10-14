"""The worker reads the action executor switch (ACTION-EXECUTOR-1).

The chart and compose render ``CURIE_ACTION_EXECUTOR_ENABLED`` into the API and
the worker from one value. The API side already reads it; the worker must read
the same name, default off, and must not fall back to a bare field-name env.
"""

from __future__ import annotations

import pytest
from curie_worker.config import WorkerConfig

FLAG = "CURIE_ACTION_EXECUTOR_ENABLED"


def test_executor_is_off_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(FLAG, raising=False)
    monkeypatch.delenv("ACTION_EXECUTOR_ENABLED", raising=False)

    assert WorkerConfig().action_executor_enabled is False


@pytest.mark.parametrize(("raw", "expected"), [("true", True), ("false", False)])
def test_executor_reads_the_shared_env_name(
    monkeypatch: pytest.MonkeyPatch, raw: str, expected: bool
) -> None:
    monkeypatch.setenv(FLAG, raw)

    assert WorkerConfig().action_executor_enabled is expected


def test_executor_ignores_the_bare_field_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(FLAG, raising=False)
    monkeypatch.setenv("ACTION_EXECUTOR_ENABLED", "true")

    assert WorkerConfig().action_executor_enabled is False
