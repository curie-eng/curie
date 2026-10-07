"""The worker reads the remediation switch (AUTOMATED-REMEDIATION-1).

The chart and compose render ``CURIE_REMEDIATION_ENABLED`` into the API and the
worker from one value. The worker must read the same name, default off, must not
fall back to a bare field-name env, and, like the API, refuses remediation on
with the action executor off (enabling it requires ``actionExecutor.enabled``).
"""

from __future__ import annotations

import pytest
from curie_worker.config import WorkerConfig
from pydantic import ValidationError

FLAG = "CURIE_REMEDIATION_ENABLED"
EXECUTOR = "CURIE_ACTION_EXECUTOR_ENABLED"


@pytest.fixture(autouse=True)
def _clear(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (FLAG, EXECUTOR, "REMEDIATION_ENABLED"):
        monkeypatch.delenv(name, raising=False)


def test_remediation_is_off_by_default() -> None:
    """@spec AUTOMATED-REMEDIATION-1"""

    assert WorkerConfig().remediation_enabled is False


@pytest.mark.parametrize(("raw", "expected"), [("true", True), ("false", False)])
def test_remediation_reads_the_shared_env_name(
    monkeypatch: pytest.MonkeyPatch, raw: str, expected: bool
) -> None:
    """@spec AUTOMATED-REMEDIATION-1"""

    monkeypatch.setenv(EXECUTOR, "true")
    monkeypatch.setenv(FLAG, raw)

    assert WorkerConfig().remediation_enabled is expected


def test_remediation_ignores_the_bare_field_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """@spec AUTOMATED-REMEDIATION-1"""

    monkeypatch.setenv(EXECUTOR, "true")
    monkeypatch.setenv("REMEDIATION_ENABLED", "true")

    assert WorkerConfig().remediation_enabled is False


def test_remediation_on_with_the_executor_off_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    """@spec AUTOMATED-REMEDIATION-1: enabling it requires the executor."""

    monkeypatch.setenv(FLAG, "true")

    with pytest.raises(ValidationError):
        WorkerConfig()
