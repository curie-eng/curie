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
