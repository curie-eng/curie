"""The API reads the installation-wide executor sandbox cap (executor amendment E9).

@spec AUTOMATED-REMEDIATION-12. One chart value,
``actionExecutor.maxConcurrentSandboxes`` (default 2, at least 1), and the
matching compose value render ``CURIE_ACTION_EXECUTOR_MAX_CONCURRENT_SANDBOXES``
into the API and the worker. The API's claim route enforces it; the worker
loop may run up to that many at once.

Pure unit tests: ``Settings`` is constructed directly from the environment set
up here.
"""

from __future__ import annotations

import pytest
from curie_api.config import Settings
from pydantic import ValidationError

CAP = "CURIE_ACTION_EXECUTOR_MAX_CONCURRENT_SANDBOXES"


@pytest.fixture(autouse=True)
def _clear(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (CAP, "ACTION_EXECUTOR_MAX_CONCURRENT_SANDBOXES"):
        monkeypatch.delenv(name, raising=False)


def test_the_cap_defaults_to_two() -> None:
    """@spec AUTOMATED-REMEDIATION-12: "default 2"."""

    assert Settings(_env_file=None).action_executor_max_concurrent_sandboxes == 2


@pytest.mark.parametrize("raw", ["1", "3", "8"])
def test_the_cap_reads_the_shared_env_name(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    """@spec AUTOMATED-REMEDIATION-12"""

    monkeypatch.setenv(CAP, raw)

    assert Settings(_env_file=None).action_executor_max_concurrent_sandboxes == int(raw)


@pytest.mark.parametrize("raw", ["0", "-1", "two"])
def test_a_cap_below_one_refuses_to_boot(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    """@spec AUTOMATED-REMEDIATION-12: "at least 1"."""

    monkeypatch.setenv(CAP, raw)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)
