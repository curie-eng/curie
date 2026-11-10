"""The API reads the remediation switch (AUTOMATED-REMEDIATION-1).

One chart value, ``remediation.enabled`` (default off), and the matching compose
value render ``CURIE_REMEDIATION_ENABLED`` into the API and the worker. Enabling
it requires the action executor: the chart render refuses remediation on with
the executor off, and because compose cannot refuse a render, the API refuses to
boot with that combination so a compose install fails closed the same way.

Pure unit tests: ``Settings`` is constructed directly from the environment set
up here.
"""

from __future__ import annotations

import pytest
from curie_api.config import Settings
from pydantic import ValidationError

FLAG = "CURIE_REMEDIATION_ENABLED"
EXECUTOR = "CURIE_ACTION_EXECUTOR_ENABLED"


@pytest.fixture(autouse=True)
def _clear(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (FLAG, EXECUTOR, "REMEDIATION_ENABLED", "remediation_enabled"):
        monkeypatch.delenv(name, raising=False)


def test_remediation_is_off_by_default() -> None:
    """@spec AUTOMATED-REMEDIATION-1: closed by default."""

    assert Settings(_env_file=None).remediation_enabled is False


def test_remediation_reads_the_shared_env_name_with_the_executor_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec AUTOMATED-REMEDIATION-1"""

    monkeypatch.setenv(EXECUTOR, "true")
    monkeypatch.setenv(FLAG, "true")

    settings = Settings(_env_file=None)

    assert settings.remediation_enabled is True
    assert settings.action_executor_enabled is True


def test_remediation_on_with_the_executor_off_refuses_to_boot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec AUTOMATED-REMEDIATION-1: enabling it requires the executor."""

    monkeypatch.setenv(FLAG, "true")
    monkeypatch.setenv(EXECUTOR, "false")

    with pytest.raises(ValidationError) as refused:
        Settings(_env_file=None)
    assert FLAG in str(refused.value) or "remediation" in str(refused.value).lower()


def test_remediation_off_with_the_executor_on_boots(monkeypatch: pytest.MonkeyPatch) -> None:
    """@spec AUTOMATED-REMEDIATION-1: the executor alone does not turn remediation on."""

    monkeypatch.setenv(EXECUTOR, "true")

    assert Settings(_env_file=None).remediation_enabled is False
