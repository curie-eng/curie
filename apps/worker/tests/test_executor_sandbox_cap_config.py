"""The worker reads the installation-wide executor sandbox cap (executor amendment E9).

@spec AUTOMATED-REMEDIATION-12. The chart and compose render
``CURIE_ACTION_EXECUTOR_MAX_CONCURRENT_SANDBOXES`` into the API and the worker
from one value (default 2, at least 1). The worker's executor loop may run up
to that many executions at once; the API's claim route is the authority.
"""

from __future__ import annotations

import pytest
from curie_worker.config import WorkerConfig
from pydantic import ValidationError

CAP = "CURIE_ACTION_EXECUTOR_MAX_CONCURRENT_SANDBOXES"


@pytest.fixture(autouse=True)
def _clear(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (CAP, "ACTION_EXECUTOR_MAX_CONCURRENT_SANDBOXES"):
        monkeypatch.delenv(name, raising=False)


def test_the_cap_defaults_to_two() -> None:
    """@spec AUTOMATED-REMEDIATION-12"""

    assert WorkerConfig().action_executor_max_concurrent_sandboxes == 2


def test_the_cap_reads_the_shared_env_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """@spec AUTOMATED-REMEDIATION-12"""

    monkeypatch.setenv(CAP, "3")

    assert WorkerConfig().action_executor_max_concurrent_sandboxes == 3


def test_the_cap_ignores_the_bare_field_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """@spec AUTOMATED-REMEDIATION-12"""

    monkeypatch.setenv("ACTION_EXECUTOR_MAX_CONCURRENT_SANDBOXES", "5")

    assert WorkerConfig().action_executor_max_concurrent_sandboxes == 2


@pytest.mark.parametrize("raw", ["0", "-1"])
def test_a_cap_below_one_refuses_to_boot(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    """@spec AUTOMATED-REMEDIATION-12: "at least 1"."""

    monkeypatch.setenv(CAP, raw)

    with pytest.raises(ValidationError):
        WorkerConfig()
