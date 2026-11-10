"""The production boot gate (#57): ENVIRONMENT=prod must refuse dev-default secrets."""

import os
import subprocess
import sys
from pathlib import Path

import pytest
from curie_api.config import Settings
from pydantic import ValidationError


def _settings(**overrides: str) -> Settings:
    # Ignore any ambient .env / process env so the test controls every field.
    base = {
        "environment": "prod",
        "api_key": "a-real-key",
        "approval_chat_attester_secret": "a-real-chat-attester-secret",
        "remediation_authority_key": "a-real-remediation-authority-key",
        "github_webhook_secret": "a-real-secret",
        "internal_worker_token": "a-real-worker-token",
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
        ({"internal_worker_token": "curie-dev-worker-token"}, "CURIE_INTERNAL_WORKER_TOKEN"),
        ({"internal_worker_token": ""}, "CURIE_INTERNAL_WORKER_TOKEN"),
        (
            {"approval_chat_attester_secret": "curie-dev-approval-chat-attester"},
            "CURIE_APPROVAL_CHAT_ATTESTER_SECRET",
        ),
        ({"approval_chat_attester_secret": ""}, "CURIE_APPROVAL_CHAT_ATTESTER_SECRET"),
        (
            {"remediation_authority_key": "curie-dev-remediation-authority"},
            "CURIE_REMEDIATION_AUTHORITY_KEY",
        ),
        ({"remediation_authority_key": ""}, "CURIE_REMEDIATION_AUTHORITY_KEY"),
    ],
)
def test_prod_refuses_dev_default_or_empty_secret(overrides: dict[str, str], offender: str) -> None:
    with pytest.raises(ValidationError) as exc:
        _settings(**overrides)
    assert offender in str(exc.value)


def test_prod_is_case_insensitive() -> None:
    with pytest.raises(ValidationError):
        _settings(environment="PROD", api_key="curie-dev-key")


@pytest.mark.parametrize("environment", ["prod", "dev", "test", "staging", "unknown", "PROD"])
@pytest.mark.parametrize("api_key", ["", "   ", "\t\r\n"])
def test_api_key_must_be_nonblank_in_every_environment(environment: str, api_key: str) -> None:
    with pytest.raises(ValidationError) as exc:
        _settings(environment=environment, api_key=api_key)
    assert "API_KEY" in str(exc.value)


@pytest.mark.parametrize("environment", ["prod", "dev", "test", "staging", "unknown"])
@pytest.mark.parametrize("api_key", ["configured-key", "  configured-key  ", "\tconfigured-key\n"])
def test_api_key_preserves_nonblank_bytes(environment: str, api_key: str) -> None:
    assert _settings(environment=environment, api_key=api_key).api_key == api_key


@pytest.mark.parametrize("environment", ["prod", "dev", "test", "staging", "unknown", "PROD"])
@pytest.mark.parametrize("api_key", ["", "   ", "\t\r\n"])
def test_api_app_import_refuses_blank_key_from_environment(
    environment: str, api_key: str, tmp_path: Path
) -> None:
    # Import the deployed ASGI entrypoint with process env, outside any .env file.
    # The unreachable database must never be consulted before config refuses boot.
    result = subprocess.run(
        [sys.executable, "-c", "import curie_api.main"],
        cwd=tmp_path,
        env={
            "PYTHONPATH": os.pathsep.join(path for path in sys.path if path),
            "ENVIRONMENT": environment,
            "API_KEY": api_key,
            "CURIE_APPROVAL_CHAT_ATTESTER_SECRET": "configured-attester-secret",
            "CURIE_REMEDIATION_AUTHORITY_KEY": "configured-remediation-authority",
            "GITHUB_WEBHOOK_SECRET": "configured-webhook-secret",
            "CURIE_INTERNAL_WORKER_TOKEN": "configured-worker-secret",
            "DATABASE_URL": "postgresql+asyncpg://postgres:postgres@127.0.0.1:1/postgres",
        },
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert result.returncode != 0, "The API app imported with a blank API_KEY"
    assert "ValidationError" in result.stderr
    assert "API_KEY" in result.stderr
    assert "ConnectionRefusedError" not in result.stderr


def test_attester_secret_is_non_blank_and_distinct_in_every_environment() -> None:
    for secret in ("   ", "curie-dev-key"):
        with pytest.raises(ValidationError) as exc:
            Settings(
                _env_file=None,
                environment="dev",
                api_key="curie-dev-key",
                approval_chat_attester_secret=secret,
            )
        assert "CURIE_APPROVAL_CHAT_ATTESTER_SECRET" in str(exc.value)


# ---------------------------------------------------------------------------
# ADR 0231: the remediation authority key (#4231)
# ---------------------------------------------------------------------------

REMEDIATION_AUTHORITY_ENV = "CURIE_REMEDIATION_AUTHORITY_KEY"
DEV_DEFAULT_REMEDIATION_AUTHORITY = "curie-dev-remediation-authority"


def test_the_remediation_authority_is_read_from_its_environment_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(REMEDIATION_AUTHORITY_ENV, "an-env-remediation-authority")
    settings = Settings(_env_file=None, environment="dev", api_key="a-real-key")
    assert settings.remediation_authority_key == "an-env-remediation-authority"


def test_the_remediation_authority_is_unset_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(REMEDIATION_AUTHORITY_ENV, raising=False)
    settings = Settings(_env_file=None, environment="dev")
    assert settings.remediation_authority_key == ""


@pytest.mark.parametrize("environment", ["prod", "dev", "test", "staging", "unknown", "PROD"])
@pytest.mark.parametrize("key", ["", " ", "   ", "\t\r\n"])
def test_a_blank_remediation_authority_is_refused_in_every_environment(
    environment: str, key: str
) -> None:
    with pytest.raises(ValidationError) as exc:
        _settings(environment=environment, remediation_authority_key=key)
    assert REMEDIATION_AUTHORITY_ENV in str(exc.value)


@pytest.mark.parametrize("environment", ["prod", "dev", "test", "staging", "unknown", "PROD"])
@pytest.mark.parametrize("key", ["", " ", "   ", "\t\r\n"])
def test_a_blank_remediation_authority_from_the_environment_is_refused_in_every_environment(
    environment: str, key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The variable is provided but blank (including the empty string): refused,
    # unlike an omitted variable, which the tests below cover.
    monkeypatch.setenv(REMEDIATION_AUTHORITY_ENV, key)
    with pytest.raises(ValidationError) as exc:
        Settings(
            _env_file=None,
            environment=environment,
            api_key="a-real-key",
            approval_chat_attester_secret="a-real-chat-attester-secret",
            github_webhook_secret="a-real-secret",
            internal_worker_token="a-real-worker-token",
        )
    assert REMEDIATION_AUTHORITY_ENV in str(exc.value)


@pytest.mark.parametrize(
    "environment, api_key",
    [
        *((env, "a-real-key") for env in ("prod", "dev", "test", "staging", "unknown", "PROD")),
        *((env, "curie-dev-key") for env in ("dev", "test", "staging", "unknown")),
    ],
)
def test_a_remediation_authority_equal_to_the_platform_key_is_refused_in_every_environment(
    environment: str, api_key: str
) -> None:
    # A platform-key holder must not hold the authority (ADR 0231); in dev too,
    # where the dev API key would otherwise be a valid authority.
    with pytest.raises(ValidationError) as exc:
        _settings(environment=environment, api_key=api_key, remediation_authority_key=api_key)
    message = str(exc.value)
    assert REMEDIATION_AUTHORITY_ENV in message
    assert api_key not in message


@pytest.mark.parametrize("key", ["", DEV_DEFAULT_REMEDIATION_AUTHORITY])
def test_prod_refuses_an_unset_or_dev_default_remediation_authority(key: str) -> None:
    with pytest.raises(ValidationError) as exc:
        _settings(remediation_authority_key=key)
    message = str(exc.value)
    assert REMEDIATION_AUTHORITY_ENV in message
    if key:
        # The dev default is a prod-only refusal; a blank key is refused everywhere.
        assert "ENVIRONMENT=prod" in message
        assert key not in message


def test_prod_refuses_an_unset_remediation_authority_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(REMEDIATION_AUTHORITY_ENV, raising=False)
    base = {
        "environment": "prod",
        "api_key": "a-real-key",
        "approval_chat_attester_secret": "a-real-chat-attester-secret",
        "github_webhook_secret": "a-real-secret",
        "internal_worker_token": "a-real-worker-token",
    }
    with pytest.raises(ValidationError) as exc:
        Settings(_env_file=None, **base)
    assert REMEDIATION_AUTHORITY_ENV in str(exc.value)


def test_prod_boots_with_a_distinct_non_default_remediation_authority() -> None:
    # Liveness: the refusals above are the key's, not some other gate's.
    settings = _settings(remediation_authority_key="a-distinct-remediation-authority")
    assert settings.environment == "prod"
    assert settings.remediation_authority_key == "a-distinct-remediation-authority"


@pytest.mark.parametrize("environment", ["dev", "test", "staging", "unknown"])
def test_outside_prod_the_dev_default_and_omitted_remediation_authority_boot(
    environment: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(
        environment=environment, remediation_authority_key=DEV_DEFAULT_REMEDIATION_AUTHORITY
    )
    assert settings.remediation_authority_key == DEV_DEFAULT_REMEDIATION_AUTHORITY

    # Omitted: neither the init kwarg nor the environment variable is provided.
    monkeypatch.delenv(REMEDIATION_AUTHORITY_ENV, raising=False)
    omitted = Settings(_env_file=None, environment=environment, api_key="a-real-key")
    assert omitted.remediation_authority_key == ""


def test_the_remediation_authority_preserves_its_nonblank_bytes() -> None:
    key = "  a-padded-remediation-authority\n"
    assert _settings(remediation_authority_key=key).remediation_authority_key == key
