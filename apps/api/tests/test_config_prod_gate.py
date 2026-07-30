"""The production boot gate (#57): ENVIRONMENT=prod must refuse dev-default secrets."""

import pytest
from curie_api.config import Settings
from pydantic import ValidationError


def _settings(**overrides: str) -> Settings:
    # Ignore any ambient .env / process env so the test controls every field.
    base = {
        "environment": "prod",
        "api_key": "a-real-key",
        "github_webhook_secret": "a-real-secret",
    }
    base.update(overrides)
    return Settings(_env_file=None, **base)


def test_dev_environment_allows_defaults() -> None:
    # The dev default construction (what every local run uses) must still work.
    s = Settings(_env_file=None, environment="dev")
    assert s.api_key == "curie-dev-key"


def test_prod_with_real_secrets_boots() -> None:
    s = _settings()
    assert s.environment == "prod"


@pytest.mark.parametrize(
    "overrides, offender",
    [
        ({"api_key": "curie-dev-key"}, "API_KEY"),
        ({"api_key": ""}, "API_KEY"),
        ({"github_webhook_secret": "dev-webhook-secret"}, "GITHUB_WEBHOOK_SECRET"),
        ({"github_webhook_secret": ""}, "GITHUB_WEBHOOK_SECRET"),
    ],
)
def test_prod_refuses_dev_default_or_empty_secret(overrides: dict[str, str], offender: str) -> None:
    with pytest.raises(ValidationError) as exc:
        _settings(**overrides)
    assert offender in str(exc.value)


def test_prod_is_case_insensitive() -> None:
    with pytest.raises(ValidationError):
        _settings(environment="PROD", api_key="curie-dev-key")
