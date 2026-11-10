"""Boot-time configuration contract for the resume reconciler (#3725).

``resume_reconciler_interval_seconds`` bounds ``ResumeReconciler.run_forever``'s
sleep: a value of 0 or below makes the loop a busy spin (a graveyard scan plus a
Postgres query per iteration, back to back, for the life of the pod), so boot
refuses it. The reconciler's off-switch stays ``resume_reconciler_enabled`` --
it is not this field, and this field never acts as one.
"""

import pytest
from curie_api.config import Settings
from pydantic import ValidationError

_RECONCILER_ENV = (
    "RESUME_RECONCILER_INTERVAL_SECONDS",
    "RESUME_RECONCILER_ENABLED",
)


@pytest.fixture(autouse=True)
def _clear_reconciler_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep these settings independent of a developer's local reconciler config."""
    for name in _RECONCILER_ENV:
        monkeypatch.delenv(name, raising=False)


def test_reconciler_defaults_to_a_30_second_interval() -> None:
    settings = Settings(_env_file=None)

    assert settings.resume_reconciler_interval_seconds == 30
    assert settings.resume_reconciler_enabled is True


def test_interval_environment_name_reaches_the_setting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RESUME_RECONCILER_INTERVAL_SECONDS", "1")

    settings = Settings(_env_file=None)

    assert settings.resume_reconciler_interval_seconds == 1


@pytest.mark.parametrize("value", ["0", "-5"])
def test_interval_of_zero_or_below_is_refused_at_boot(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("RESUME_RECONCILER_INTERVAL_SECONDS", value)

    with pytest.raises(ValidationError) as exc:
        Settings(_env_file=None)

    # Pydantic names the field (not the env var) in the error, but the field IS
    # what RESUME_RECONCILER_INTERVAL_SECONDS feeds, so this pins the offender.
    assert "resume_reconciler_interval_seconds" in str(exc.value)
    assert "greater than 0" in str(exc.value)


def test_disabled_reconciler_still_boots_with_the_default_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``resume_reconciler_enabled=false`` is the off-switch (#411): it must stay
    accepted, and disabling never loosens the interval bound."""
    monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")

    settings = Settings(_env_file=None)

    assert settings.resume_reconciler_enabled is False
    assert settings.resume_reconciler_interval_seconds == 30
