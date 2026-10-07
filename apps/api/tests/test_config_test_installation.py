"""The test installation declaration at API boot (ADR 0202 decision 1, #4133).

The chart renders ``testInstallation.enabled`` and ``testInstallation.drivers``
as ``CURIE_TEST_INSTALLATION_ENABLED`` and ``CURIE_TEST_INSTALLATION_DRIVERS``.
The API receives both and, with the declaration on, refuses to boot on any
published default secret it holds. The chart cannot see a secret supplied
another way, and a compose stack never renders the chart, so this refusal is
the one every deployment path meets.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from curie_api.config import Settings
from pydantic import ValidationError

DRIVER = {
    "channel_id": "C0EXAMPLE1",
    "bot_id": "B0EXAMPLE1",
    "bot_user_id": "U0EXAMPLE1",
}
SIBLING = {**DRIVER, "bot_id": "B0EXAMPLE2", "bot_user_id": "U0EXAMPLE2", "agent": "acme-tester"}

REAL_SECRETS = {
    "api_key": "a-real-key",
    "approval_chat_attester_secret": "a-real-chat-attester-secret",
    "internal_worker_token": "a-real-worker-token",
}


def _settings(**overrides: object) -> Settings:
    base: dict[str, object] = {"environment": "dev", **REAL_SECRETS}
    base.update(overrides)
    return Settings(_env_file=None, **base)  # type: ignore[arg-type]


def test_the_declaration_is_off_by_default() -> None:
    settings = Settings(_env_file=None)
    assert settings.test_installation_enabled is False
    assert settings.test_installation_drivers == ()


def test_on_with_real_secrets_boots_and_receives_the_drivers() -> None:
    settings = _settings(
        test_installation_enabled=True,
        test_installation_drivers=json.dumps([DRIVER, SIBLING]),
    )
    assert settings.test_installation_enabled is True
    assert [
        (d.channel_id, d.bot_id, d.bot_user_id, d.agent)
        for d in settings.test_installation_drivers
    ] == [
        ("C0EXAMPLE1", "B0EXAMPLE1", "U0EXAMPLE1", None),
        ("C0EXAMPLE1", "B0EXAMPLE2", "U0EXAMPLE2", "acme-tester"),
    ]


@pytest.mark.parametrize(
    "overrides, offender",
    [
        ({"api_key": "curie-dev-key"}, "API_KEY"),
        ({"internal_worker_token": "curie-dev-worker-token"}, "CURIE_INTERNAL_WORKER_TOKEN"),
        (
            {"approval_chat_attester_secret": "curie-dev-approval-chat-attester"},
            "CURIE_APPROVAL_CHAT_ATTESTER_SECRET",
        ),
    ],
)
def test_on_refuses_each_published_default_secret(
    overrides: dict[str, str], offender: str
) -> None:
    with pytest.raises(ValidationError) as exc:
        _settings(test_installation_enabled=True, **overrides)
    message = str(exc.value)
    assert "CURIE_TEST_INSTALLATION_ENABLED" in message
    assert offender in message


def test_off_keeps_the_published_defaults_bootable() -> None:
    # Control: the same defaults boot while the declaration is off, so the
    # refusal above is the declaration's and not some other gate's.
    settings = _settings(
        api_key="curie-dev-key",
        internal_worker_token="curie-dev-worker-token",
        approval_chat_attester_secret="curie-dev-approval-chat-attester",
    )
    assert settings.test_installation_enabled is False


@pytest.mark.parametrize(
    "drivers, fragment",
    [
        ([{k: v for k, v in DRIVER.items() if k != "channel_id"}], "channel_id"),
        ([{**DRIVER, "channel_id": ""}], "channel_id"),
        ([{**DRIVER, "channel_id": "D0EXAMPLE1"}], "channel_id"),
        ([{**DRIVER, "bot_id": "U0EXAMPLE1"}], "bot_id"),
        ([{**DRIVER, "bot_user_id": "B0EXAMPLE1"}], "bot_user_id"),
        ([{**DRIVER, "agent": ""}], "agent"),
        ([{**DRIVER, "note": "x"}], "note"),
        ({"channel_id": "C0EXAMPLE1"}, "list"),
        (None, "list"),
    ],
)
def test_a_malformed_driver_refuses_boot(drivers: object, fragment: str) -> None:
    with pytest.raises(ValidationError) as exc:
        _settings(test_installation_enabled=True, test_installation_drivers=json.dumps(drivers))
    assert fragment in str(exc.value)


def test_api_app_import_refuses_a_published_default_with_the_declaration_on(
    tmp_path: Path,
) -> None:
    # The deployed ASGI entrypoint, from process env, outside any .env file.
    env = {
        "PYTHONPATH": os.pathsep.join(path for path in sys.path if path),
        "ENVIRONMENT": "dev",
        "API_KEY": "curie-dev-key",
        "CURIE_APPROVAL_CHAT_ATTESTER_SECRET": "configured-attester-secret",
        "CURIE_INTERNAL_WORKER_TOKEN": "configured-worker-secret",
        "CURIE_TEST_INSTALLATION_ENABLED": "true",
        "CURIE_TEST_INSTALLATION_DRIVERS": json.dumps([DRIVER]),
        "DATABASE_URL": "postgresql+asyncpg://postgres:postgres@127.0.0.1:1/postgres",
    }
    result = subprocess.run(
        [sys.executable, "-c", "import curie_api.main"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert result.returncode != 0, "the API imported on a published default with the declaration on"
    assert "ValidationError" in result.stderr
    assert "CURIE_TEST_INSTALLATION_ENABLED" in result.stderr
    assert "API_KEY" in result.stderr
    assert "ConnectionRefusedError" not in result.stderr

    # Control: the same process boots past config with the declaration off.
    env["CURIE_TEST_INSTALLATION_ENABLED"] = "false"
    control = subprocess.run(
        [sys.executable, "-c", "import curie_api.main"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert control.returncode == 0, control.stderr
