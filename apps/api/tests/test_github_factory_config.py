"""Boot-time configuration contract for signed factory issue intake (#2574)."""

from typing import Any

import pytest
from curie_api.config import Settings
from pydantic import ValidationError

_FACTORY_ENV = (
    "GITHUB_FACTORY_INGRESS_ENABLED",
    "GITHUB_FACTORY_INTAKE",
    "GITHUB_FACTORY_POLL_INTERVAL_S",
    "GITHUB_FACTORY_LABEL",
    "GITHUB_FACTORY_MENTION",
    "GITHUB_APP_ID",
    "GITHUB_APP_PRIVATE_KEY",
    "GITHUB_WEBHOOK_SECRET",
    "GITHUB_REPO_ALLOWLIST",
)


@pytest.fixture(autouse=True)
def _clear_factory_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _FACTORY_ENV:
        monkeypatch.delenv(name, raising=False)


def _enabled(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "github_factory_ingress_enabled": True,
        "github_factory_label": "factory",
        "github_factory_mention": "curie",
        "github_app_id": "12345",
        "github_app_private_key": "example-private-key",
        "github_webhook_secret": "example-factory-hmac-secret",
        "github_repo_allowlist": ("acme-corp/*",),
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def test_factory_ingress_defaults_off() -> None:
    settings = Settings(_env_file=None)

    assert settings.github_factory_ingress_enabled is False
    assert settings.github_factory_intake == "poll"
    assert settings.github_factory_poll_interval_s == 45.0
    assert settings.github_factory_label == ""
    assert settings.github_factory_mention == ""


def test_disabled_factory_ingress_does_not_require_label_or_credentials() -> None:
    settings = Settings(
        _env_file=None,
        github_factory_ingress_enabled=False,
        github_factory_label="",
        github_factory_mention="",
        github_webhook_secret="dev-webhook-secret",
    )

    assert settings.github_factory_ingress_enabled is False


def test_enabled_factory_ingress_accepts_complete_configuration() -> None:
    settings = _enabled()

    assert settings.github_factory_label == "factory"
    assert settings.github_factory_mention == "curie"


def test_poll_intake_accepts_an_empty_webhook_secret() -> None:
    settings = _enabled(github_webhook_secret="", github_factory_intake="poll")

    assert settings.github_webhook_secret == ""
    assert settings.github_factory_intake == "poll"


_RECONCILE_SETTINGS = (
    "github_factory_reconcile_interval_s",
    "github_factory_reconcile_grace_s",
)
_RECONCILE_ENV = {
    "github_factory_reconcile_interval_s": "GITHUB_FACTORY_RECONCILE_INTERVAL_S",
    "github_factory_reconcile_grace_s": "GITHUB_FACTORY_RECONCILE_GRACE_S",
}


def test_factory_reconcile_settings_default_to_300() -> None:
    settings = Settings(_env_file=None)

    assert settings.github_factory_reconcile_interval_s == 300.0
    assert settings.github_factory_reconcile_grace_s == 300.0


@pytest.mark.parametrize("field", _RECONCILE_SETTINGS)
def test_factory_reconcile_settings_accept_zero(field: str) -> None:
    settings = Settings(_env_file=None, **{field: 0})

    assert getattr(settings, field) == 0.0


@pytest.mark.parametrize("field", _RECONCILE_SETTINGS)
@pytest.mark.parametrize("value", [-5, -600])
def test_factory_reconcile_settings_refuse_negative_values(field: str, value: float) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{field: value})


@pytest.mark.parametrize("field", _RECONCILE_SETTINGS)
@pytest.mark.parametrize("value", [float("nan"), float("inf")])
def test_factory_reconcile_settings_refuse_non_finite_values(field: str, value: float) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{field: value})


@pytest.mark.parametrize("field", _RECONCILE_SETTINGS)
@pytest.mark.parametrize("value", ["-5", "nan", "inf"])
def test_factory_reconcile_settings_refuse_the_same_via_the_environment(
    field: str, value: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(_RECONCILE_ENV[field], value)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


@pytest.mark.parametrize(
    ("overrides", "offender"),
    [
        ({"github_app_id": ""}, "GITHUB_APP_ID"),
        ({"github_app_private_key": ""}, "GITHUB_APP_PRIVATE_KEY"),
        (
            {"github_webhook_secret": "", "github_factory_intake": "webhook"},
            "GITHUB_WEBHOOK_SECRET",
        ),
        (
            {"github_webhook_secret": "dev-webhook-secret", "github_factory_intake": "webhook"},
            "GITHUB_WEBHOOK_SECRET",
        ),
        ({"github_factory_label": ""}, "GITHUB_FACTORY_LABEL"),
        ({"github_factory_label": "factory label"}, "GITHUB_FACTORY_LABEL"),
        ({"github_factory_mention": ""}, "GITHUB_FACTORY_MENTION"),
        ({"github_factory_mention": "curie[bot]"}, "GITHUB_FACTORY_MENTION"),
        ({"github_repo_allowlist": ()}, "GITHUB_REPO_ALLOWLIST"),
    ],
)
def test_enabled_factory_ingress_refuses_incomplete_configuration(
    overrides: dict[str, Any], offender: str
) -> None:
    with pytest.raises(ValidationError) as exc:
        _enabled(**overrides)

    assert offender in str(exc.value)
