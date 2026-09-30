"""CURIE_CREDENTIALS -> SDK credential env resolution.

All values are fake placeholders; this never touches a real credential.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TypedDict, cast
from urllib.parse import urlparse

import pytest
from curie_runner.sdk_auth import (
    API_BACKEND_ENV,
    API_KEY_ENV,
    AUTH_TOKEN_ENV,
    BASE_URL_ENV,
    CREDENTIALS_ENV,
    DEEPSEEK_BASE_URL,
    DEFAULT_API_BACKEND,
    DEFAULT_CREDENTIAL_ENV_KEYS,
    MODEL_BASE_URL_ENV,
    MODEL_ENV_KEY_ENV,
    MOONSHOT_BASE_URL,
    NO_OP_API_KEY,
    OAUTH_TOKEN_ENV,
    OPENROUTER_BASE_URL,
    PROVIDER_BASE_URLS,
    ZHIPU_BASE_URL,
    ApiBackend,
    InvalidEnvKeyError,
    UnsupportedApiBackendError,
    UnsupportedCredentialError,
    parse_env_keys,
    resolve_credential,
    resolve_credential_env_keys,
    resolve_model_credential,
    resolve_sdk_env,
)

# Custom credential env var NAMES used by the env_key tests. Hoisted to named
# constants because the secrets pre-commit hook false-positives on inline
# *_TOKEN / *_KEY literals; these are variable names, never values.
ALT_TOKEN_KEY = "ANTHROPIC_AUTH_TOKEN_ALT"
MY_CRED_KEY = "MY_CRED"

_PROVIDER_REGISTRY = (
    Path(__file__).resolve().parents[2] / "tests" / "vectors" / "model-provider-registry.json"
)
_PROVIDER_REGISTRY_KEYS = frozenset(
    {"providers", "unknown_provider_names", "rejected_credential_examples"}
)
_PROVIDER_ROW_KEYS = frozenset(
    {
        "name",
        "base_url",
        "inferred_provider",
        "egress_hosts",
        "credential_examples",
    }
)


class _ProviderRow(TypedDict):
    name: str
    base_url: str | None
    inferred_provider: str | None
    egress_hosts: list[str]
    credential_examples: list[str]


class _ProviderRegistryDocument(TypedDict):
    providers: list[_ProviderRow]
    unknown_provider_names: list[str]
    rejected_credential_examples: list[str]


def _load_provider_registry(raw: str | None = None) -> _ProviderRegistryDocument:
    registry: object = json.loads(
        _PROVIDER_REGISTRY.read_text(encoding="utf-8") if raw is None else raw
    )
    assert isinstance(registry, dict)
    assert set(registry) == _PROVIDER_REGISTRY_KEYS

    providers = registry["providers"]
    assert isinstance(providers, list)
    assert providers
    for provider in providers:
        assert isinstance(provider, dict)
        assert set(provider) == _PROVIDER_ROW_KEYS
        assert isinstance(provider["name"], str)
        assert provider["base_url"] is None or isinstance(provider["base_url"], str)
        assert provider["inferred_provider"] is None or isinstance(
            provider["inferred_provider"], str
        )
        assert isinstance(provider["egress_hosts"], list)
        assert provider["egress_hosts"]
        assert all(isinstance(host, str) and host for host in provider["egress_hosts"])
        assert isinstance(provider["credential_examples"], list)
        assert provider["credential_examples"]
        assert all(
            isinstance(example, str) and example for example in provider["credential_examples"]
        )

    for key in ("unknown_provider_names", "rejected_credential_examples"):
        assert isinstance(registry[key], list)
        assert registry[key]
        assert all(isinstance(value, str) for value in registry[key])
    return cast(_ProviderRegistryDocument, registry)


def test_anthropic_api_key_maps_to_api_key_env() -> None:
    env = {CREDENTIALS_ENV: "sk-ant-PLACEHOLDER"}
    resolve_model_credential(env)
    assert env[API_KEY_ENV] == "sk-ant-PLACEHOLDER"
    assert OAUTH_TOKEN_ENV not in env


@pytest.mark.parametrize(
    "token",
    [
        # Claude Code OAuth tokens begin with sk-ant-oat and share the sk-ant-
        # prefix with API keys; they must route to the OAuth var, not the API key.
        pytest.param("sk-ant-oatPLACEHOLDER", id="sk-ant-oat-prefix"),
        # A Claude Code OAuth token is not an sk- key.
        pytest.param("oauth-PLACEHOLDER-token", id="non-sk-token"),
    ],
)
def test_oauth_token_maps_to_oauth_env(token: str) -> None:
    env = {CREDENTIALS_ENV: token}
    resolve_model_credential(env)
    assert env[OAUTH_TOKEN_ENV] == token
    assert API_KEY_ENV not in env


def test_explicit_sdk_credential_wins_over_curie_credentials() -> None:
    env = {OAUTH_TOKEN_ENV: "explicit-PLACEHOLDER", CREDENTIALS_ENV: "sk-ant-PLACEHOLDER"}
    resolve_model_credential(env)
    # The explicit SDK var is untouched; the reference is ignored (no API key set).
    assert env[OAUTH_TOKEN_ENV] == "explicit-PLACEHOLDER"
    assert API_KEY_ENV not in env


@pytest.mark.parametrize("foreign", ["sk-PLACEHOLDER"])
def test_foreign_key_is_rejected_loudly(foreign: str) -> None:
    with pytest.raises(UnsupportedCredentialError) as exc:
        resolve_model_credential({CREDENTIALS_ENV: foreign})
    msg = str(exc.value)
    assert "Anthropic API key" in msg and "Claude Code OAuth token" in msg
    assert foreign not in msg  # the credential value never appears in the error


def test_openrouter_key_maps_to_api_key_not_bearer() -> None:
    env = {CREDENTIALS_ENV: "sk-or-PLACEHOLDER"}
    resolve_model_credential(env)

    assert env[BASE_URL_ENV] == OPENROUTER_BASE_URL
    # The real key goes in ANTHROPIC_API_KEY (the x-api-key header OpenRouter's
    # Anthropic endpoint reads), overriding the NO_OP_API_KEY placeholder.
    assert env[API_KEY_ENV] == "sk-or-PLACEHOLDER"
    # It must NOT land in ANTHROPIC_AUTH_TOKEN (Bearer); that stays blank.
    assert env[AUTH_TOKEN_ENV] == ""
    assert env[OAUTH_TOKEN_ENV] == ""


def test_explicit_sdk_credential_wins_over_openrouter_reference() -> None:
    env = {API_KEY_ENV: "sk-ant-EXPLICIT", CREDENTIALS_ENV: "sk-or-PLACEHOLDER"}
    resolve_model_credential(env)

    assert env[API_KEY_ENV] == "sk-ant-EXPLICIT"
    assert BASE_URL_ENV not in env


def test_no_credential_is_a_noop() -> None:
    env: dict[str, str] = {}
    resolve_model_credential(env)
    assert env == {}


def test_base_url_override_sets_sdk_base_url_and_placeholder_api_key() -> None:
    from curie_runner.sdk_auth import (
        BASE_URL_ENV,
        NO_OP_API_KEY,
        resolve_base_url_override,
    )

    assert BASE_URL_ENV == "ANTHROPIC_BASE_URL"

    override = resolve_base_url_override({BASE_URL_ENV: "http://ollama:11434"})

    assert override is not None
    assert override[BASE_URL_ENV] == "http://ollama:11434"
    # The placeholder API key must be NON-EMPTY: an empty ANTHROPIC_API_KEY makes
    # the bundled Claude CLI report "not logged in" and skip the endpoint call.
    assert override[API_KEY_ENV] == NO_OP_API_KEY
    assert override[API_KEY_ENV] != ""
    # The OAuth token is blanked so an inherited token cannot win over the
    # placeholder + overridden base URL.
    assert override[OAUTH_TOKEN_ENV] == ""
    # The Bearer token is blanked too so an inherited ANTHROPIC_AUTH_TOKEN
    # cannot leak to the overridden (local/third-party) endpoint.
    assert override[AUTH_TOKEN_ENV] == ""


def test_base_url_override_is_absent_when_env_is_missing() -> None:
    from curie_runner.sdk_auth import resolve_base_url_override

    assert resolve_base_url_override({}) is None


def test_base_url_override_is_absent_when_env_is_empty() -> None:
    from curie_runner.sdk_auth import BASE_URL_ENV, resolve_base_url_override

    assert resolve_base_url_override({BASE_URL_ENV: ""}) is None


def test_resolve_sdk_env_ollama_returns_override_dict() -> None:
    # Base URL set, no sk-or- credential: generic override mode. Returns the
    # override dict for ClaudeAgentOptions.env; the Bearer token is blanked.
    env = {BASE_URL_ENV: "http://ollama:11434"}
    override = resolve_sdk_env(env)

    assert override is not None
    assert override[BASE_URL_ENV] == "http://ollama:11434"
    assert override[API_KEY_ENV] == NO_OP_API_KEY
    assert override[AUTH_TOKEN_ENV] == ""


def test_resolve_sdk_env_plain_anthropic_key_mutates_env() -> None:
    # sk-ant- credential, no base URL: returns None, mutates env with the API key.
    env = {CREDENTIALS_ENV: "sk-ant-PLACEHOLDER"}
    assert resolve_sdk_env(env) is None
    assert env[API_KEY_ENV] == "sk-ant-PLACEHOLDER"
    assert BASE_URL_ENV not in env


def test_resolve_sdk_env_openrouter_alone_mutates_env() -> None:
    # sk-or- credential, no preset base URL: returns None; env gets the
    # OpenRouter base URL and the real key in ANTHROPIC_API_KEY (x-api-key).
    env = {CREDENTIALS_ENV: "sk-or-PLACEHOLDER"}
    assert resolve_sdk_env(env) is None
    assert env[BASE_URL_ENV] == OPENROUTER_BASE_URL
    assert env[API_KEY_ENV] == "sk-or-PLACEHOLDER"
    assert env[AUTH_TOKEN_ENV] == ""


def test_resolve_sdk_env_openrouter_with_preset_base_url_sets_api_key() -> None:
    # P2 regression: an operator sets ANTHROPIC_BASE_URL AND passes an sk-or-
    # credential. The sk-or- key must still be routed (into ANTHROPIC_API_KEY, the
    # x-api-key header), not skipped by the generic base-URL override.
    env = {BASE_URL_ENV: "https://openrouter.ai/api", CREDENTIALS_ENV: "sk-or-PLACEHOLDER"}
    assert resolve_sdk_env(env) is None
    assert env[API_KEY_ENV] == "sk-or-PLACEHOLDER"
    assert env[AUTH_TOKEN_ENV] == ""


# --- Provider-native Anthropic-compatible endpoints (#252): Zhipu / Moonshot /
# DeepSeek. Selected by base URL; the provider key is forwarded as x-api-key.


def test_provider_base_urls_stay_on_anthropic_format() -> None:
    # The canonical endpoints keep the Anthropic wire format (SDK appends
    # /v1/messages) so provider automatic prefix caching survives.
    assert PROVIDER_BASE_URLS["zhipu"] == ZHIPU_BASE_URL == "https://api.z.ai/api/anthropic"
    assert PROVIDER_BASE_URLS["moonshot"] == MOONSHOT_BASE_URL
    assert PROVIDER_BASE_URLS["deepseek"] == DEEPSEEK_BASE_URL
    assert PROVIDER_BASE_URLS["openrouter"] == OPENROUTER_BASE_URL


def test_provider_registry_matches_runner_authority() -> None:
    registry = _load_provider_registry()
    providers = registry["providers"]
    by_name = {provider["name"]: provider for provider in providers}

    assert len(by_name) == len(providers)
    assert set(by_name) == {"anthropic", *PROVIDER_BASE_URLS}
    assert [row["name"] for row in providers if row["base_url"] is None] == ["anthropic"]
    assert {
        name: row["base_url"] for name, row in by_name.items() if row["base_url"] is not None
    } == PROVIDER_BASE_URLS

    for provider in providers:
        base_url = provider["base_url"]
        if base_url is not None:
            hostname = urlparse(base_url).hostname
            assert hostname is not None
            assert provider["egress_hosts"] == [hostname]

    assert not set(registry["unknown_provider_names"]) & set(by_name)


def test_provider_registry_inferred_prefixes_match_runtime_routing() -> None:
    registry = _load_provider_registry()

    for provider in registry["providers"]:
        for credential in provider["credential_examples"]:
            env = {CREDENTIALS_ENV: credential}
            try:
                resolve_model_credential(env)
            except UnsupportedCredentialError:
                routed_provider = None
            else:
                if env.get(BASE_URL_ENV) == OPENROUTER_BASE_URL:
                    routed_provider = "openrouter"
                elif credential.startswith("sk-ant-"):
                    routed_provider = "anthropic"
                else:
                    routed_provider = None

            assert routed_provider == provider["inferred_provider"], (
                provider["name"],
                credential,
                routed_provider,
            )


@pytest.mark.parametrize(
    "base_url",
    [ZHIPU_BASE_URL, MOONSHOT_BASE_URL, DEEPSEEK_BASE_URL],
)
def test_provider_native_key_forwarded_as_x_api_key(base_url: str) -> None:
    # Moonshot / DeepSeek use OpenAI-style sk- keys; Zhipu uses a non-sk key.
    # With the provider base URL set, the key must be forwarded to
    # ANTHROPIC_API_KEY (x-api-key), overriding the NO_OP placeholder -- not
    # rejected and not dropped.
    env = {BASE_URL_ENV: base_url, CREDENTIALS_ENV: "sk-PROVIDER-PLACEHOLDER"}
    override = resolve_sdk_env(env)

    assert override is not None
    assert override[BASE_URL_ENV] == base_url
    assert override[API_KEY_ENV] == "sk-PROVIDER-PLACEHOLDER"
    # Inherited OAuth / Bearer tokens stay blanked so they cannot leak to the
    # third-party endpoint.
    assert override[OAUTH_TOKEN_ENV] == ""
    assert override[AUTH_TOKEN_ENV] == ""


def test_zhipu_non_sk_key_forwarded() -> None:
    # Zhipu keys are id.secret shaped (no sk- prefix); still forwarded.
    env = {BASE_URL_ENV: ZHIPU_BASE_URL, CREDENTIALS_ENV: "zhipu-id.PLACEHOLDER"}
    override = resolve_sdk_env(env)
    assert override is not None
    assert override[API_KEY_ENV] == "zhipu-id.PLACEHOLDER"


def test_model_base_url_alias_selects_override() -> None:
    # CURIE_MODEL_BASE_URL (CURIE_-namespaced alias) drives the same seam.
    env = {MODEL_BASE_URL_ENV: DEEPSEEK_BASE_URL, CREDENTIALS_ENV: "sk-DEEPSEEK-PLACEHOLDER"}
    override = resolve_sdk_env(env)
    assert override is not None
    assert override[BASE_URL_ENV] == DEEPSEEK_BASE_URL
    assert override[API_KEY_ENV] == "sk-DEEPSEEK-PLACEHOLDER"


def test_raw_base_url_wins_over_alias() -> None:
    env = {
        BASE_URL_ENV: MOONSHOT_BASE_URL,
        MODEL_BASE_URL_ENV: DEEPSEEK_BASE_URL,
        CREDENTIALS_ENV: "sk-PLACEHOLDER",
    }
    override = resolve_sdk_env(env)
    assert override is not None
    assert override[BASE_URL_ENV] == MOONSHOT_BASE_URL


def test_oauth_token_not_forwarded_to_provider_endpoint() -> None:
    # A Claude Code OAuth token must never be forwarded to a third-party
    # endpoint; the placeholder stays and the token is blanked (hermetic).
    env = {BASE_URL_ENV: ZHIPU_BASE_URL, CREDENTIALS_ENV: "sk-ant-oatPLACEHOLDER"}
    override = resolve_sdk_env(env)
    assert override is not None
    assert override[API_KEY_ENV] == NO_OP_API_KEY
    assert override[OAUTH_TOKEN_ENV] == ""


def test_ollama_without_credential_keeps_placeholder() -> None:
    # No credential (local Ollama): the NO_OP placeholder is retained.
    env = {BASE_URL_ENV: "http://ollama:11434"}
    override = resolve_sdk_env(env)
    assert override is not None
    assert override[API_KEY_ENV] == NO_OP_API_KEY


def test_bare_sk_still_rejected_without_base_url() -> None:
    # The direct-Anthropic path (no base URL) still rejects a bare sk- key: there
    # is no provider endpoint to forward it to.
    with pytest.raises(UnsupportedCredentialError):
        resolve_sdk_env({CREDENTIALS_ENV: "sk-PLACEHOLDER"})


# --- Explicit api_backend wire-protocol enum (#514). Today every endpoint is
# assumed to speak the Anthropic Messages wire format; CURIE_MODEL_API_BACKEND
# makes that assumption declarable, so an OpenAI-shaped endpoint fails loudly
# instead of being silently mis-dialed.


def test_api_backend_defaults_to_messages_when_unset() -> None:
    # Unset must preserve today's behavior: everything is assumed Messages.
    from curie_runner.sdk_auth import resolve_api_backend

    assert resolve_api_backend({}) is ApiBackend.MESSAGES
    assert DEFAULT_API_BACKEND is ApiBackend.MESSAGES


def test_api_backend_defaults_to_messages_when_empty_string() -> None:
    # An empty env var is "not declared", not "declared as empty" -- the same
    # empty-string trap the credential path guards against.
    from curie_runner.sdk_auth import resolve_api_backend

    assert resolve_api_backend({API_BACKEND_ENV: ""}) is ApiBackend.MESSAGES


def test_api_backend_explicit_messages() -> None:
    from curie_runner.sdk_auth import resolve_api_backend

    assert resolve_api_backend({API_BACKEND_ENV: "messages"}) is ApiBackend.MESSAGES


@pytest.mark.parametrize("raw", ["MESSAGES", " Messages "])
def test_api_backend_parsing_is_case_insensitive_and_trims(raw: str) -> None:
    # Config authors hand-write this value; casing and stray whitespace must not
    # turn a valid backend into a hard failure.
    from curie_runner.sdk_auth import resolve_api_backend

    assert resolve_api_backend({API_BACKEND_ENV: raw}) is ApiBackend.MESSAGES


def test_api_backend_chat_completions() -> None:
    from curie_runner.sdk_auth import resolve_api_backend

    assert resolve_api_backend({API_BACKEND_ENV: "chat_completions"}) is ApiBackend.CHAT_COMPLETIONS


def test_api_backend_responses() -> None:
    from curie_runner.sdk_auth import resolve_api_backend

    assert resolve_api_backend({API_BACKEND_ENV: "responses"}) is ApiBackend.RESPONSES


@pytest.mark.parametrize("raw", ["grpc", "chat-completions"])
def test_api_backend_unrecognized_value_raises(raw: str) -> None:
    # "chat-completions" (hyphen) is the near-miss spelling of a real member; an
    # unrecognized value must fail loudly rather than silently defaulting.
    from curie_runner.sdk_auth import resolve_api_backend

    with pytest.raises(UnsupportedApiBackendError):
        resolve_api_backend({API_BACKEND_ENV: raw})


@pytest.mark.parametrize(
    ("backend", "speaks"),
    [
        (ApiBackend.MESSAGES, True),
        (ApiBackend.CHAT_COMPLETIONS, False),
        (ApiBackend.RESPONSES, False),
    ],
)
def test_speaks_anthropic_wire(backend: ApiBackend, speaks: bool) -> None:
    # The deterministic branch point: only the Messages wire format is dialable
    # by claude-agent-sdk.
    assert backend.speaks_anthropic_wire is speaks


def test_api_backend_members_have_expected_wire_values() -> None:
    # The enum is a wire contract; its string values are what config authors put
    # in CURIE_MODEL_API_BACKEND.
    assert ApiBackend.MESSAGES == "messages"
    assert ApiBackend.CHAT_COMPLETIONS == "chat_completions"
    assert ApiBackend.RESPONSES == "responses"
    assert API_BACKEND_ENV == "CURIE_MODEL_API_BACKEND"


@pytest.mark.parametrize("backend", ["chat_completions", "responses"])
def test_resolve_sdk_env_rejects_non_anthropic_backend_with_base_url(backend: str) -> None:
    # With a base URL set (the override path). The runner speaks the Anthropic
    # Messages wire format only; failing loudly beats silently mis-dialing.
    env = {BASE_URL_ENV: ZHIPU_BASE_URL, API_BACKEND_ENV: backend}
    with pytest.raises(UnsupportedApiBackendError):
        resolve_sdk_env(env)


@pytest.mark.parametrize("backend", ["chat_completions", "responses"])
def test_resolve_sdk_env_rejects_non_anthropic_backend_with_plain_key(backend: str) -> None:
    # Same rejection on the plain sk-ant- path with NO base URL. Paired with the
    # test above, this proves the branch is on the backend itself, not a side
    # effect of the base-URL override path.
    env = {CREDENTIALS_ENV: "sk-ant-PLACEHOLDER", API_BACKEND_ENV: backend}
    with pytest.raises(UnsupportedApiBackendError):
        resolve_sdk_env(env)


def test_resolve_sdk_env_explicit_messages_matches_default_plain_key() -> None:
    # Regression net: declaring "messages" explicitly must behave EXACTLY as the
    # undeclared default does today.
    env = {CREDENTIALS_ENV: "sk-ant-PLACEHOLDER", API_BACKEND_ENV: "messages"}
    assert resolve_sdk_env(env) is None
    assert env[API_KEY_ENV] == "sk-ant-PLACEHOLDER"
    assert BASE_URL_ENV not in env


def test_resolve_sdk_env_explicit_messages_matches_default_override() -> None:
    # Same regression net on the base-URL override path.
    env = {BASE_URL_ENV: "http://ollama:11434", API_BACKEND_ENV: "messages"}
    override = resolve_sdk_env(env)

    assert override is not None
    assert override[BASE_URL_ENV] == "http://ollama:11434"
    assert override[API_KEY_ENV] == NO_OP_API_KEY
    assert override[AUTH_TOKEN_ENV] == ""


def test_openrouter_credential_survives_default_backend() -> None:
    # CRITICAL regression guard: the base-URL gate must NOT drop an explicit
    # sk-or- BYO credential. Under the default (Messages) backend the key must
    # still reach OpenRouter's endpoint as the real key, never the placeholder.
    env = {CREDENTIALS_ENV: "sk-or-PLACEHOLDER"}
    assert resolve_sdk_env(env) is None
    assert env[BASE_URL_ENV] == OPENROUTER_BASE_URL
    assert env[API_KEY_ENV] == "sk-or-PLACEHOLDER"
    assert env[API_KEY_ENV] != NO_OP_API_KEY


def test_openrouter_credential_survives_explicit_messages_backend() -> None:
    # The same guard with the backend declared explicitly.
    env = {CREDENTIALS_ENV: "sk-or-PLACEHOLDER", API_BACKEND_ENV: "messages"}
    assert resolve_sdk_env(env) is None
    assert env[BASE_URL_ENV] == OPENROUTER_BASE_URL
    assert env[API_KEY_ENV] == "sk-or-PLACEHOLDER"
    assert env[API_KEY_ENV] != NO_OP_API_KEY


# --- env_key as string-or-array (#514). CURIE_MODEL_ENV_KEY declares which env
# var(s) carry the credential, so a config author is not forced onto the single
# hardcoded CURIE_CREDENTIALS name.


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param('["A","B"]', ("A", "B"), id="json-array-preserves-order"),
        # Hand-written config picks up stray empty entries; they carry no meaning.
        pytest.param('["A","","  ","B"]', ("A", "B"), id="drops-blank-array-entries"),
        # Sourcing from a provider-native var is the whole point of the feature; the
        # issue's own cited example (grok-build) is exactly this pair. Non-CURIE_
        # names must stay allowed or the fence has eaten the feature.
        pytest.param(
            '["ANTHROPIC_AUTH_TOKEN","LC_ANTHROPIC_AUTH_TOKEN"]',
            ("ANTHROPIC_AUTH_TOKEN", "LC_ANTHROPIC_AUTH_TOKEN"),
            id="provider-native-names-allowed",
        ),
        pytest.param("A", ("A",), id="bare-string-is-one-element"),
        pytest.param("MY_PROVIDER_KEY", ("MY_PROVIDER_KEY",), id="bare-non-curie-name-allowed"),
    ],
)
def test_parse_env_keys(raw: str, expected: tuple[str, ...]) -> None:
    assert parse_env_keys(raw) == expected


def test_parse_env_keys_bare_string_that_is_not_json_falls_back() -> None:
    # "MY_KEY" is not valid JSON. The JSON attempt must fall back to the
    # bare-string form rather than raising.
    assert parse_env_keys(MY_CRED_KEY) == (MY_CRED_KEY,)


@pytest.mark.parametrize("raw", ["[1,2]", '{"a":1}', "[]", '["", "  "]', "5"])
def test_parse_env_keys_invalid_raises(raw: str) -> None:
    # Valid JSON of the wrong shape (non-string array members, a mapping, a bare
    # number) and arrays that reduce to nothing are all config errors.
    with pytest.raises(InvalidEnvKeyError):
        parse_env_keys(raw)


def test_resolve_credential_env_keys_defaults_when_unset() -> None:
    assert resolve_credential_env_keys({}) == DEFAULT_CREDENTIAL_ENV_KEYS
    assert DEFAULT_CREDENTIAL_ENV_KEYS == (CREDENTIALS_ENV,)
    assert MODEL_ENV_KEY_ENV == "CURIE_MODEL_ENV_KEY"


def test_resolve_credential_env_keys_defaults_when_empty_string() -> None:
    assert resolve_credential_env_keys({MODEL_ENV_KEY_ENV: ""}) == DEFAULT_CREDENTIAL_ENV_KEYS


def test_resolve_credential_skips_unset_key_and_takes_next() -> None:
    env = {
        MODEL_ENV_KEY_ENV: f'["{ALT_TOKEN_KEY}","{MY_CRED_KEY}"]',
        MY_CRED_KEY: "sk-ant-PLACEHOLDER",
    }
    assert resolve_credential(env) == "sk-ant-PLACEHOLDER"


@pytest.mark.parametrize(
    ("first", "second", "expected"),
    [
        # THE key assertion (the #229 empty-string gotcha, made explicit): a key that
        # is present but empty must be SKIPPED, not win. An empty credential silently
        # beating a real one downstream is exactly the failure this guards.
        pytest.param("", "sk-ant-PLACEHOLDER", "sk-ant-PLACEHOLDER", id="empty-key-skipped"),
        pytest.param(
            "sk-ant-FIRST-PLACEHOLDER",
            "sk-ant-SECOND-PLACEHOLDER",
            "sk-ant-FIRST-PLACEHOLDER",
            id="first-set-key-wins",
        ),
    ],
)
def test_resolve_credential_takes_first_non_empty_key(
    first: str, second: str, expected: str
) -> None:
    env = {
        MODEL_ENV_KEY_ENV: f'["{ALT_TOKEN_KEY}","{MY_CRED_KEY}"]',
        ALT_TOKEN_KEY: first,
        MY_CRED_KEY: second,
    }
    assert resolve_credential(env) == expected


def test_resolve_credential_returns_empty_when_no_key_matches() -> None:
    env = {MODEL_ENV_KEY_ENV: f'["{ALT_TOKEN_KEY}","{MY_CRED_KEY}"]'}
    assert resolve_credential(env) == ""


def test_resolve_credential_defaults_to_curie_credentials() -> None:
    # Byte-identical-to-today guard: with no CURIE_MODEL_ENV_KEY declared, the
    # credential still comes from CURIE_CREDENTIALS.
    assert resolve_credential({CREDENTIALS_ENV: "sk-ant-PLACEHOLDER"}) == "sk-ant-PLACEHOLDER"


def test_resolve_sdk_env_sources_credential_from_custom_env_key_array() -> None:
    # End-to-end: the two features compose. A credential sourced from a custom
    # env_key array routes to ANTHROPIC_API_KEY exactly as it would from
    # CURIE_CREDENTIALS.
    env = {
        MODEL_ENV_KEY_ENV: f'["{ALT_TOKEN_KEY}","{MY_CRED_KEY}"]',
        MY_CRED_KEY: "sk-ant-PLACEHOLDER",
    }
    assert resolve_sdk_env(env) is None
    assert env[API_KEY_ENV] == "sk-ant-PLACEHOLDER"
    assert BASE_URL_ENV not in env


def test_resolve_sdk_env_default_env_key_path_unchanged() -> None:
    # Byte-identical-to-today guard on the full resolution path.
    env = {CREDENTIALS_ENV: "sk-ant-PLACEHOLDER"}
    assert resolve_sdk_env(env) is None
    assert env[API_KEY_ENV] == "sk-ant-PLACEHOLDER"


# --- env_key targeting fence (#514 hardening). resolve_credential walks whatever
# names CURIE_MODEL_ENV_KEY hands it, but the runner boot env is shared: it also
# carries the platform's own CURIE_-namespaced vars, including the scoped HMAC
# state tokens (ADR-0033) the runner authenticates to the state API with. Naming
# one of those as a credential source under a base-URL override would forward it
# to a third-party endpoint as x-api-key -- a secret-exfiltration primitive. The
# platform's own boot vars are never a model credential, so a CURIE_-prefixed
# target is either a mistake or an attack, and the runner refuses either way.
# CURIE_CREDENTIALS is the one exception: it IS the canonical credential name.
# The existing reserved_env fence guards the name a connector secret may DECLARE;
# this one guards the name an env_key may TARGET. Different direction, both needed.

# Platform boot-env var NAMES the fence must refuse. Hoisted to named constants
# because the secrets pre-commit hook false-positives on inline *_TOKEN / *_KEY
# literals; these are variable names, never values.
MEMORY_TOKEN_KEY = "CURIE_MEMORY_TOKEN"
HISTORY_TOKEN_KEY = "CURIE_HISTORY_TOKEN"
RUNNER_TOKEN_KEY = "CURIE_RUNNER_TOKEN"
BUDGET_KEY = "CURIE_BUDGET"
# An obvious fake placeholder standing in for a scoped state token's VALUE.
FAKE_STATE_TOKEN_VALUE = "PLACEHOLDER-not-a-real-state-token"


@pytest.mark.parametrize(
    "name", [MEMORY_TOKEN_KEY, HISTORY_TOKEN_KEY, RUNNER_TOKEN_KEY, BUDGET_KEY]
)
def test_parse_env_keys_rejects_curie_prefixed_target(name: str) -> None:
    # The state tokens are the exfiltration targets; the budget stands in for the
    # rest of the CURIE_ boot namespace (refs, ids) -- none is a credential.
    with pytest.raises(InvalidEnvKeyError):
        parse_env_keys(f'["{name}"]')


def test_parse_env_keys_rejects_curie_prefixed_bare_string_target() -> None:
    # The fence applies to BOTH input forms; the bare-string path must not be a
    # way around the array-form check.
    with pytest.raises(InvalidEnvKeyError):
        parse_env_keys(RUNNER_TOKEN_KEY)


def test_parse_env_keys_allows_curie_credentials_target() -> None:
    # The one allowed CURIE_ name: it is the canonical credential var, and it is
    # already the default, so declaring it explicitly must stay legal.
    assert parse_env_keys(f'["{CREDENTIALS_ENV}"]') == (CREDENTIALS_ENV,)


def test_parse_env_keys_allows_curie_credentials_bare_string() -> None:
    assert parse_env_keys(CREDENTIALS_ENV) == (CREDENTIALS_ENV,)


def test_parse_env_keys_one_bad_name_poisons_the_whole_declaration() -> None:
    # A mixed array raises rather than silently dropping the fenced name and
    # falling through to the good one. Silently dropping would let a typo'd (or
    # deliberate) exfil attempt LOOK like it worked -- the declaration resolves, a
    # credential is found, nothing surfaces. Refusing the whole declaration makes
    # the operator confront what they wrote.
    with pytest.raises(InvalidEnvKeyError):
        parse_env_keys(f'["{MEMORY_TOKEN_KEY}","{MY_CRED_KEY}"]')


def test_resolve_credential_env_keys_rejects_curie_prefixed_target() -> None:
    # The fence is reachable through the env-facing resolver, not only through the
    # parser it delegates to.
    with pytest.raises(InvalidEnvKeyError):
        resolve_credential_env_keys({MODEL_ENV_KEY_ENV: f'["{MEMORY_TOKEN_KEY}"]'})


def test_resolve_sdk_env_refuses_to_exfiltrate_a_state_token_to_an_override() -> None:
    # THE regression net for the actual vulnerability. A base URL points at a
    # third-party endpoint and the declaration names the scoped state token
    # (ADR-0033) that is sitting in the same boot env. Forwarding it would send the
    # platform's own credential to that endpoint as the x-api-key header. It must
    # raise, and the token value must never reach ANTHROPIC_API_KEY.
    env = {
        BASE_URL_ENV: ZHIPU_BASE_URL,
        MODEL_ENV_KEY_ENV: f'["{MEMORY_TOKEN_KEY}"]',
        MEMORY_TOKEN_KEY: FAKE_STATE_TOKEN_VALUE,
    }
    with pytest.raises(InvalidEnvKeyError) as excinfo:
        resolve_sdk_env(env)

    assert env.get(API_KEY_ENV) != FAKE_STATE_TOKEN_VALUE
    assert API_KEY_ENV not in env  # nothing was resolved at all
    # The error may name the offending VAR NAME (that is the actionable part) but
    # must never echo the credential VALUE -- the module's no-echo discipline.
    assert FAKE_STATE_TOKEN_VALUE not in str(excinfo.value)


def test_resolve_sdk_env_refuses_state_token_on_the_plain_path_too() -> None:
    # Same refusal with no base URL set, proving the gate is on the declaration
    # itself rather than a side effect of the override branch.
    env = {
        MODEL_ENV_KEY_ENV: f'["{HISTORY_TOKEN_KEY}"]',
        HISTORY_TOKEN_KEY: FAKE_STATE_TOKEN_VALUE,
    }
    with pytest.raises(InvalidEnvKeyError):
        resolve_sdk_env(env)
    assert API_KEY_ENV not in env
    assert OAUTH_TOKEN_ENV not in env
