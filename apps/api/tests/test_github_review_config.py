"""Boot-time configuration contract for GitHub review-feedback ingress (#2275)."""

from typing import Any

import pytest
from curie_api.config import Settings
from pydantic import ValidationError


_REVIEW_ENV = (
    "GITHUB_REVIEW_INGRESS_ENABLED",
    "GITHUB_REVIEW_RECONCILER_INTERVAL_S",
    "GITHUB_APP_ID",
    "GITHUB_APP_PRIVATE_KEY",
    "GITHUB_WEBHOOK_SECRET",
)


@pytest.fixture(autouse=True)
def _clear_review_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep these settings independent of a developer's local GitHub config."""
    for name in _REVIEW_ENV:
        monkeypatch.delenv(name, raising=False)


def _enabled_settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "github_review_ingress_enabled": True,
        "github_review_reconciler_interval_s": 5.0,
        "github_app_id": "12345",
        "github_app_private_key": "example-private-key",
        "github_webhook_secret": "example-review-hmac-secret",
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def test_review_ingress_defaults_off_with_positive_reconciler_interval() -> None:
    settings = Settings(_env_file=None)

    assert settings.github_review_ingress_enabled is False
    assert settings.github_review_reconciler_interval_s == 5.0


def test_disabled_review_ingress_does_not_require_credentials_or_active_interval() -> None:
    settings = Settings(
        _env_file=None,
        github_review_ingress_enabled=False,
        github_review_reconciler_interval_s=0,
        github_app_id="",
        github_app_private_key="",
        github_webhook_secret="dev-webhook-secret",
    )

    assert settings.github_review_ingress_enabled is False
    assert settings.github_review_reconciler_interval_s == 0


def test_enabled_review_ingress_accepts_complete_configuration() -> None:
    settings = _enabled_settings()

    assert settings.github_review_ingress_enabled is True
    assert settings.github_review_reconciler_interval_s == 5.0


def test_review_ingress_environment_names_reach_boot_validator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_REVIEW_INGRESS_ENABLED", "true")
    monkeypatch.setenv("GITHUB_REVIEW_RECONCILER_INTERVAL_S", "2.5")
    monkeypatch.setenv("GITHUB_APP_ID", "12345")
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", "example-private-key")
    monkeypatch.setenv("GITHUB_WEBHOOK_SECRET", "example-review-hmac-secret")

    settings = Settings(_env_file=None)

    assert settings.github_review_ingress_enabled is True
    assert settings.github_review_reconciler_interval_s == 2.5


@pytest.mark.parametrize(
    ("overrides", "offender"),
    [
        ({"github_app_id": ""}, "GITHUB_APP_ID"),
        ({"github_app_private_key": ""}, "GITHUB_APP_PRIVATE_KEY"),
        ({"github_webhook_secret": ""}, "GITHUB_WEBHOOK_SECRET"),
        ({"github_webhook_secret": "dev-webhook-secret"}, "GITHUB_WEBHOOK_SECRET"),
        ({"github_review_reconciler_interval_s": 0}, "GITHUB_REVIEW_RECONCILER_INTERVAL_S"),
        ({"github_review_reconciler_interval_s": -0.1}, "GITHUB_REVIEW_RECONCILER_INTERVAL_S"),
    ],
)
def test_enabled_review_ingress_refuses_incomplete_or_inert_configuration(
    overrides: dict[str, Any], offender: str
) -> None:
    with pytest.raises(ValidationError) as exc:
        _enabled_settings(**overrides)

    assert offender in str(exc.value)
