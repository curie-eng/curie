"""The provider scope derivation and the OpenRouter key read parser (#5426)."""

from __future__ import annotations

import hashlib
import re
from decimal import Decimal
from typing import Any

import pytest
from aci_protocol import BootEnv
from aci_protocol.events import RETRY_SCOPE_KEY_PATTERN
from curie_internal import provider_scope as ps
from curie_internal.provider_scope import (
    AllowanceReading,
    ProviderScope,
    parse_openrouter_key,
    provider_scope,
    scope_key,
)

# Low-entropy placeholders only.
OR_KEY = "sk-or-v1-" + "0" * 64
ANT_KEY = "sk-ant-api03-" + "0" * 64
ANT_OAT = "sk-ant-oat01-" + "0" * 64
PLAIN_TOKEN = "token-" + "0" * 32


def _scope(provider: str, host: str, credential: str) -> ProviderScope:
    return ProviderScope(
        provider=provider, host=host, scope_key=scope_key(provider, host, credential)
    )


def test_boot_env_declared_names_match_aci_protocol() -> None:
    assert BootEnv.env_key("credentials_ref") == ps.CURIE_CREDENTIALS
    assert BootEnv.env_key("model_env_key") == ps.CURIE_MODEL_ENV_KEY
    assert BootEnv.env_key("base_url") == ps.ANTHROPIC_BASE_URL


def test_provider_sets() -> None:
    assert ps.SUPPORTED_PROVIDERS == {"openrouter", "anthropic"}
    assert ps.PROBED_PROVIDERS == {"openrouter"}


@pytest.mark.parametrize(
    ("env", "provider", "host", "credential"),
    [
        pytest.param(
            {"CURIE_CREDENTIALS": ANT_KEY},
            "anthropic",
            "api.anthropic.com",
            ANT_KEY,
            id="sk-ant-api-key",
        ),
        pytest.param(
            {"CURIE_CREDENTIALS": ANT_OAT},
            "anthropic",
            "api.anthropic.com",
            ANT_OAT,
            id="sk-ant-oat-token",
        ),
        pytest.param(
            {"CURIE_CREDENTIALS": PLAIN_TOKEN},
            "anthropic",
            "api.anthropic.com",
            PLAIN_TOKEN,
            id="non-sk-token",
        ),
        pytest.param(
            {"CURIE_CREDENTIALS": OR_KEY}, "openrouter", "openrouter.ai", OR_KEY, id="sk-or-key"
        ),
        pytest.param(
            {"CURIE_CREDENTIALS": OR_KEY, "ANTHROPIC_BASE_URL": "https://example.test/anthropic"},
            "openrouter",
            "openrouter.ai",
            OR_KEY,
            id="sk-or-key-with-other-base-url",
        ),
        pytest.param(
            {"CURIE_CREDENTIALS": ANT_KEY, "ANTHROPIC_BASE_URL": "https://openrouter.ai/api"},
            "openrouter",
            "openrouter.ai",
            ANT_KEY,
            id="openrouter-base-url-non-sk-or-key",
        ),
        pytest.param(
            {"CURIE_CREDENTIALS": PLAIN_TOKEN, "CURIE_MODEL_BASE_URL": "https://OpenRouter.AI/api"},
            "openrouter",
            "openrouter.ai",
            PLAIN_TOKEN,
            id="curie-model-base-url-openrouter-case-folded",
        ),
        pytest.param(
            {
                "CURIE_MODEL_ENV_KEY": '["FIRST_KEY", "SECOND_KEY"]',
                "FIRST_KEY": "",
                "SECOND_KEY": OR_KEY,
            },
            "openrouter",
            "openrouter.ai",
            OR_KEY,
            id="declared-list-first-empty-second-set",
        ),
        pytest.param(
            {"CURIE_MODEL_ENV_KEY": "PROVIDER_KEY", "PROVIDER_KEY": ANT_KEY},
            "anthropic",
            "api.anthropic.com",
            ANT_KEY,
            id="declared-bare-name",
        ),
        pytest.param(
            {"CURIE_MODEL_ENV_KEY": '["", "CURIE_CREDENTIALS"]', "CURIE_CREDENTIALS": OR_KEY},
            "openrouter",
            "openrouter.ai",
            OR_KEY,
            id="declared-list-blank-dropped-curie-credentials-allowed",
        ),
        pytest.param(
            {"CURIE_MODEL_AUTH_MODE": "service", "CURIE_CREDENTIALS": OR_KEY},
            "openrouter",
            "openrouter.ai",
            OR_KEY,
            id="service-mode",
        ),
        pytest.param(
            {"ANTHROPIC_API_KEY": "", "CURIE_CREDENTIALS": OR_KEY},
            "openrouter",
            "openrouter.ai",
            OR_KEY,
            id="empty-sdk-credential-ignored",
        ),
    ],
)
def test_provider_scope_derives(
    env: dict[str, str], provider: str, host: str, credential: str
) -> None:
    assert provider_scope(env) == _scope(provider, host, credential)


@pytest.mark.parametrize(
    "env",
    [
        pytest.param(
            {"CURIE_MODEL_AUTH_MODE": "personal", "CURIE_CREDENTIALS": OR_KEY}, id="personal-mode"
        ),
        pytest.param(
            {"CURIE_MODEL_AUTH_MODE": " Personal ", "CURIE_CREDENTIALS": OR_KEY},
            id="personal-mode-folded",
        ),
        pytest.param(
            {"ANTHROPIC_API_KEY": ANT_KEY, "CURIE_CREDENTIALS": OR_KEY}, id="explicit-api-key"
        ),
        pytest.param(
            {"CLAUDE_CODE_OAUTH_TOKEN": ANT_OAT, "CURIE_CREDENTIALS": OR_KEY},
            id="explicit-oauth-token",
        ),
        pytest.param(
            {"CURIE_MODEL_ENV_KEY": "CURIE_SANDBOX_TOKEN", "CURIE_SANDBOX_TOKEN": OR_KEY},
            id="curie-target",
        ),
        pytest.param(
            {
                "CURIE_MODEL_ENV_KEY": '["PROVIDER_KEY", "CURIE_STATE_TOKEN"]',
                "PROVIDER_KEY": OR_KEY,
            },
            id="curie-target-in-list",
        ),
        pytest.param(
            {"CURIE_MODEL_ENV_KEY": '{"name": "PROVIDER_KEY"}', "PROVIDER_KEY": OR_KEY},
            id="mapping-declaration",
        ),
        pytest.param(
            {"CURIE_MODEL_ENV_KEY": '["PROVIDER_KEY", 1]', "PROVIDER_KEY": OR_KEY},
            id="non-string-member",
        ),
        pytest.param({"CURIE_MODEL_ENV_KEY": "[]", "CURIE_CREDENTIALS": OR_KEY}, id="empty-list"),
        pytest.param(
            {"CURIE_MODEL_ENV_KEY": '["", " "]', "CURIE_CREDENTIALS": OR_KEY}, id="blank-list"
        ),
        pytest.param(
            {"CURIE_MODEL_ENV_KEY": "7", "CURIE_CREDENTIALS": OR_KEY}, id="number-declaration"
        ),
        pytest.param({"CURIE_CREDENTIALS": "sk-" + "0" * 48}, id="bare-sk"),
        pytest.param(
            {
                "CURIE_CREDENTIALS": ANT_KEY,
                "ANTHROPIC_BASE_URL": "https://api.example.test/anthropic",
            },
            id="other-base-url-host",
        ),
        pytest.param(
            {
                "CURIE_CREDENTIALS": ANT_KEY,
                "CURIE_MODEL_BASE_URL": "https://openrouter.ai.example.test/api",
            },
            id="lookalike-base-url-host",
        ),
        pytest.param({"CURIE_CREDENTIALS": ""}, id="empty-credential"),
        pytest.param({}, id="no-credential"),
        pytest.param(
            {"CURIE_MODEL_ENV_KEY": "PROVIDER_KEY", "CURIE_CREDENTIALS": OR_KEY},
            id="declared-name-unset",
        ),
    ],
)
def test_provider_scope_refuses(env: dict[str, str]) -> None:
    assert provider_scope(env) is None


def test_scope_key_is_stable_and_matches_the_wire_pattern() -> None:
    key = scope_key("openrouter", "openrouter.ai", OR_KEY)
    assert key == scope_key("openrouter", "openrouter.ai", OR_KEY)
    assert re.fullmatch(RETRY_SCOPE_KEY_PATTERN, key)
    # Pinned so a change to the canonical document is a deliberate version bump.
    credential_digest = hashlib.sha256(OR_KEY.encode()).hexdigest()
    document = (
        f'{{"credential_sha256":"{credential_digest}",'
        '"host":"openrouter.ai","provider":"openrouter","v":1}'
    )
    assert key == "sha256:" + hashlib.sha256(document.encode()).hexdigest()


def test_scope_key_differs_by_credential_and_host() -> None:
    base = scope_key("openrouter", "openrouter.ai", OR_KEY)
    assert scope_key("openrouter", "openrouter.ai", "sk-or-v1-" + "1" * 64) != base
    assert scope_key("openrouter", "api.anthropic.com", OR_KEY) != base
    assert scope_key("anthropic", "openrouter.ai", OR_KEY) != base


def test_scope_never_holds_the_credential() -> None:
    scope = provider_scope({"CURIE_CREDENTIALS": OR_KEY})
    assert scope is not None
    assert OR_KEY not in repr(scope)
    assert OR_KEY not in str(scope)
    assert all(OR_KEY not in str(value) for value in vars(scope).values())


# The documented 200 shape of GET /api/v1/key:
# https://openrouter.ai/docs/api/api-reference/api-keys/get-current-api-key
_DOCUMENTED_BODY: dict[str, Any] = {
    "data": {
        "label": "sk-or-v1-000...000",
        "limit": 25,
        "limit_remaining": 12.5,
        "limit_reset": "monthly",
        "usage": 12.5,
        "is_free_tier": False,
    }
}


def _with_remaining(value: object) -> dict[str, Any]:
    return {"data": {**_DOCUMENTED_BODY["data"], "limit_remaining": value}}


def _with_limit(value: object) -> dict[str, Any]:
    return {"data": {**_DOCUMENTED_BODY["data"], "limit": value}}


def test_parse_documented_body() -> None:
    # The documented body carries a numeric limit.
    assert parse_openrouter_key(200, _DOCUMENTED_BODY) == AllowanceReading(
        "authenticated", Decimal("12.5"), Decimal("25")
    )


def test_parse_integer_remaining() -> None:
    assert parse_openrouter_key(200, _with_remaining(3)) == AllowanceReading(
        "authenticated", Decimal("3"), Decimal("25")
    )


def test_parse_float_limit() -> None:
    assert parse_openrouter_key(200, _with_limit(30.5)) == AllowanceReading(
        "authenticated", Decimal("12.5"), Decimal("30.5")
    )


def test_parse_null_limit() -> None:
    # A JSON null limit is an unlimited key.
    body = {"data": {**_DOCUMENTED_BODY["data"], "limit": None, "limit_remaining": None}}
    assert parse_openrouter_key(200, body) == AllowanceReading("authenticated", None, None)


def test_parse_null_limit_keeps_a_numeric_remaining() -> None:
    assert parse_openrouter_key(200, _with_limit(None)) == AllowanceReading(
        "authenticated", Decimal("12.5"), None
    )


def test_parse_missing_limit_reads_none() -> None:
    data = {key: value for key, value in _DOCUMENTED_BODY["data"].items() if key != "limit"}
    assert parse_openrouter_key(200, {"data": data}) == AllowanceReading(
        "authenticated", Decimal("12.5"), None
    )


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(True, id="bool"),
        pytest.param("12.5", id="string"),
        pytest.param(float("inf"), id="inf"),
        pytest.param(float("nan"), id="nan"),
    ],
)
def test_parse_non_numeric_remaining_reads_none(value: object) -> None:
    assert parse_openrouter_key(200, _with_remaining(value)) == AllowanceReading(
        "authenticated", None, Decimal("25")
    )


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(True, id="bool"),
        pytest.param("25", id="string"),
        pytest.param(float("inf"), id="inf"),
        pytest.param(float("nan"), id="nan"),
    ],
)
def test_parse_non_numeric_limit_reads_none(value: object) -> None:
    assert parse_openrouter_key(200, _with_limit(value)) == AllowanceReading(
        "authenticated", Decimal("12.5"), None
    )


def test_parse_401_rejected() -> None:
    assert parse_openrouter_key(401, {"error": {"code": 401}}) == AllowanceReading(
        "rejected", None, None
    )


@pytest.mark.parametrize(
    ("status", "body"),
    [
        pytest.param(403, _DOCUMENTED_BODY, id="403"),
        pytest.param(500, None, id="500"),
        pytest.param(200, [_DOCUMENTED_BODY], id="200-list-body"),
        pytest.param(200, {"data": [1]}, id="200-list-data"),
        pytest.param(200, {}, id="200-no-data"),
        pytest.param(None, None, id="transport-none"),
    ],
)
def test_parse_unreadable(status: int | None, body: object) -> None:
    assert parse_openrouter_key(status, body) == AllowanceReading("unreadable", None, None)
