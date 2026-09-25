"""Regression tests for DispatcherConfig env-source resolution.

``populate_by_name=True`` lets tests construct the config with field-name
kwargs, but it must NOT make the env source read the bare uppercased field name
as a fallback for a field that carries a ``validation_alias``. An aliased field
must read only its ``CURIE_*`` alias; a stray generic env var (``STREAM``,
``SHIMMER``, ...) in the pod env must be ignored, as it was before the
BaseSettings refactor.
"""

from __future__ import annotations

import json

import pytest
from curie_dispatcher.config import DispatcherConfig
from pydantic import ValidationError

_CHAT_ATTESTER_SECRET = "dispatcher-attester-test-secret"


def _set_valid_chat_attester_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provide the independent credential needed by unrelated config tests."""

    monkeypatch.setenv(
        "CURIE_APPROVAL_CHAT_ATTESTER_SECRET", _CHAT_ATTESTER_SECRET
    )


def _clear_all_config_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Delete every env var the config could read, for a clean-env baseline.

    ``BaseSettings`` reads the environment for every field, so the compose-stack
    Valkey vars (``VALKEY_HOST`` ...) may be set ambiently; strip them all so the
    defaults assertions see only the code defaults.
    """
    for name, field in DispatcherConfig.model_fields.items():
        alias = field.validation_alias
        key = alias if isinstance(alias, str) else name.upper()
        monkeypatch.delenv(key, raising=False)


# Every env var the OLD hand-rolled ``DispatcherConfig.from_env`` (on
# ``origin/main``) read, paired with a distinct sentinel and its expected
# coerced value. Names are the exact old ones -- the override test proves no
# name drifted and no read was dropped.
_DISPATCHER_OVERRIDES: dict[str, tuple[str, str, object]] = {
    # env var name -> (field name, raw env value, expected coerced value)
    "SLACK_APP_TOKEN": ("slack_app_token", "xapp-sentinel", "xapp-sentinel"),
    "SLACK_BOT_TOKEN": ("slack_bot_token", "xoxb-sentinel", "xoxb-sentinel"),
    "SLACK_SIGNING_SECRET": (
        "slack_signing_secret",
        "sign-sentinel",
        "sign-sentinel",
    ),
    "VALKEY_HOST": ("valkey_host", "valkey.host.example", "valkey.host.example"),
    "VALKEY_PORT": ("valkey_port", "6380", 6380),
    "VALKEY_PASSWORD": ("valkey_password", "vk-pass", "vk-pass"),
    "VALKEY_DB": ("valkey_db", "7", 7),
    "CURIE_STREAM": ("stream", "sentinel:runs", "sentinel:runs"),
    "CURIE_DEDUPE_PREFIX": (
        "dedupe_prefix",
        "sentinel:dedupe:",
        "sentinel:dedupe:",
    ),
    "CURIE_DEDUPE_TTL_SECONDS": ("dedupe_ttl_seconds", "7200", 7200),
    "CURIE_PLACEHOLDER_TEXT": (
        "placeholder_text",
        "Sentinel placeholder.",
        "Sentinel placeholder.",
    ),
    "CURIE_BACKOFF_INITIAL_SECONDS": ("backoff_initial_seconds", "2.5", 2.5),
    "CURIE_BACKOFF_MAX_SECONDS": ("backoff_max_seconds", "45.5", 45.5),
    "CURIE_BACKOFF_MULTIPLIER": ("backoff_multiplier", "3.5", 3.5),
}


def test_aliased_field_ignores_bare_field_name_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stray bare-name env var must not leak into an aliased field."""
    _set_valid_chat_attester_secret(monkeypatch)
    monkeypatch.setenv("STREAM", "stray:stream")

    config = DispatcherConfig()

    assert config.stream == "curie:runs"  # the default, not "stray:stream"


def test_aliased_field_reads_its_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    """The intended CURIE_* alias is still read from the env."""
    _set_valid_chat_attester_secret(monkeypatch)
    monkeypatch.setenv("CURIE_STREAM", "intended:stream")

    assert DispatcherConfig().stream == "intended:stream"


def test_alias_wins_over_bare_field_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """With both set, only the alias is read and the bare name is ignored."""
    _set_valid_chat_attester_secret(monkeypatch)
    monkeypatch.setenv("STREAM", "stray:stream")
    monkeypatch.setenv("CURIE_STREAM", "intended:stream")

    assert DispatcherConfig().stream == "intended:stream"


def test_field_name_kwargs_still_populate() -> None:
    """populate_by_name construction (used by tests) is unchanged."""
    config = DispatcherConfig(
        stream="s",
        dedupe_ttl_seconds=99,
        approval_chat_attester_secret=_CHAT_ATTESTER_SECRET,
    )

    assert config.stream == "s"
    assert config.dedupe_ttl_seconds == 99


def test_non_aliased_field_still_reads_plain_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fields without an alias keep reading their uppercased field name."""
    _set_valid_chat_attester_secret(monkeypatch)
    monkeypatch.setenv("VALKEY_HOST", "valkey.internal")

    assert DispatcherConfig().valkey_host == "valkey.internal"


# --- Env-var parity vs the pre-pydantic from_env (review #178) ---------------


def test_defaults_parity_with_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clean env: every field must equal the exact default the old from_env produced.

    Every field is enumerated; a drifted default is a silent prod break.
    """
    _clear_all_config_env(monkeypatch)
    _set_valid_chat_attester_secret(monkeypatch)

    config = DispatcherConfig()

    assert config.slack_app_token == ""
    assert config.slack_bot_token == ""
    assert config.slack_signing_secret == ""
    assert config.slack_identities == ()
    assert config.approval_chat_attester_secret == _CHAT_ATTESTER_SECRET
    assert config.valkey_host == "localhost"
    assert config.valkey_port == 6379
    assert config.valkey_password == ""
    assert config.valkey_db == 0
    assert config.stream == "curie:runs"
    assert config.dedupe_prefix == "curie:dedupe:"
    assert config.dedupe_ttl_seconds == 3600
    assert config.placeholder_text == "On it. Working on your request."
    assert config.backoff_initial_seconds == 1.0
    assert config.backoff_max_seconds == 30.0
    assert config.backoff_multiplier == 2.0


def test_overrides_parity_with_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every env var the old from_env read, set under its EXACT old name, must be
    read into the right field with the right coercion.

    Proves no env-var name drifted and no read was dropped.
    """
    _clear_all_config_env(monkeypatch)
    _set_valid_chat_attester_secret(monkeypatch)
    for env_var, (_field, raw, _expected) in _DISPATCHER_OVERRIDES.items():
        monkeypatch.setenv(env_var, raw)

    config = DispatcherConfig()

    for env_var, (field, _raw, expected) in _DISPATCHER_OVERRIDES.items():
        actual = getattr(config, field)
        assert actual == expected, f"{env_var} -> {field}: {actual!r} != {expected!r}"
        assert type(actual) is type(expected), (
            f"{env_var} -> {field}: type {type(actual)} != {type(expected)}"
        )


# --- Platform API wiring (#442) ---------------------------------------------
#
# #442 proposed changing api_base_url's default off localhost. It was rejected:
# the default is correct for its only real audience (a dispatcher run bare on a
# laptop against a local API), and every containerized context has a manifest
# whose whole job is to declare the wiring. The obvious "fix"
# (http://curie-api:8000) hardcodes a compose service name into application
# code -- wrong in the chart, wrong on a laptop, wrong for any BYO deployment.
# The default also deliberately mirrors the worker's for the same seam (#246).
# These lock that decision, and cover the setting the boot gate reads.


@pytest.mark.parametrize("chat_attester_secret", [None, "", "curie-dev-key"])
def test_approval_chat_attester_secret_is_required_and_independent(
    monkeypatch: pytest.MonkeyPatch, chat_attester_secret: str | None
) -> None:
    """ADR-0106: an API key must never become a Slack identity attester.

    A missing or blank key would make a click unable to prove who Slack
    authenticated. Reusing the platform API key is worse: it lets every holder
    of that widely shared key forge an approval principal. All three are a
    startup error, rather than a button that appears to work but records an
    assertion.
    """

    _clear_all_config_env(monkeypatch)
    if chat_attester_secret is not None:
        monkeypatch.setenv(
            "CURIE_APPROVAL_CHAT_ATTESTER_SECRET", chat_attester_secret
        )

    with pytest.raises(ValidationError):
        DispatcherConfig()


def test_approval_chat_attester_secret_reads_only_its_explicit_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A generic field-name env var cannot quietly become a signing key."""

    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("APPROVAL_CHAT_ATTESTER_SECRET", _CHAT_ATTESTER_SECRET)

    with pytest.raises(ValidationError):
        DispatcherConfig()

    monkeypatch.setenv(
        "CURIE_APPROVAL_CHAT_ATTESTER_SECRET", _CHAT_ATTESTER_SECRET
    )
    assert DispatcherConfig().approval_chat_attester_secret == _CHAT_ATTESTER_SECRET


def test_approval_chat_attester_secret_cannot_equal_a_non_default_platform_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The independence guard compares values, not just the dev default."""

    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_API_KEY", "different-platform-api-test-key")
    monkeypatch.setenv(
        "CURIE_APPROVAL_CHAT_ATTESTER_SECRET", "different-platform-api-test-key"
    )

    with pytest.raises(ValidationError):
        DispatcherConfig()


def test_api_base_url_default_stays_localhost(monkeypatch: pytest.MonkeyPatch) -> None:
    """The code default is unchanged: the fix is manifest wiring, not a new default.

    A dispatcher run bare on a laptop against a local API must keep working with
    no env set at all.
    """
    _clear_all_config_env(monkeypatch)
    _set_valid_chat_attester_secret(monkeypatch)

    assert DispatcherConfig().api_base_url == "http://localhost:8000"


def test_api_preflight_timeout_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """The boot gate's deadline defaults to 30s: long enough to absorb API startup."""
    _clear_all_config_env(monkeypatch)
    _set_valid_chat_attester_secret(monkeypatch)

    assert DispatcherConfig().api_preflight_timeout_s == 30.0


def test_api_preflight_timeout_reads_its_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    """The deadline is tunable via CURIE_API_PREFLIGHT_TIMEOUT_SECONDS, and float-coerced."""
    _clear_all_config_env(monkeypatch)
    _set_valid_chat_attester_secret(monkeypatch)
    monkeypatch.setenv("CURIE_API_PREFLIGHT_TIMEOUT_SECONDS", "5.5")

    config = DispatcherConfig()

    assert config.api_preflight_timeout_s == 5.5
    assert type(config.api_preflight_timeout_s) is float


def test_api_preflight_timeout_ignores_bare_field_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The new aliased field follows the house rule: alias only, no bare-name fallback."""
    _clear_all_config_env(monkeypatch)
    _set_valid_chat_attester_secret(monkeypatch)
    monkeypatch.setenv("API_PREFLIGHT_TIMEOUT_S", "99.0")

    assert DispatcherConfig().api_preflight_timeout_s == 30.0


def test_api_preflight_timeout_rejects_non_positive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-positive deadline is a config error, not a way to switch the gate off.

    The gate is the AC2 requirement, so `0` must fail loudly at boot rather than
    silently skip the probe and hand the operator back the dead-ended Approve
    button the gate exists to prevent.
    """
    _clear_all_config_env(monkeypatch)
    _set_valid_chat_attester_secret(monkeypatch)
    monkeypatch.setenv("CURIE_API_PREFLIGHT_TIMEOUT_SECONDS", "0")

    with pytest.raises(ValidationError):
        DispatcherConfig()


@pytest.mark.parametrize("token", ["inf", "Infinity", "-inf", "nan"])
def test_api_preflight_timeout_rejects_non_finite(
    monkeypatch: pytest.MonkeyPatch, token: str
) -> None:
    """A non-finite deadline defeats the gate as thoroughly as switching it off.

    `gt=0` catches `-inf` and `nan` on its own but passes `inf`, which makes the
    boot probe wait forever: the pod never exits, so it never CrashLoopBackOffs,
    so the operator never sees the misconfiguration. That is the exact silent
    failure AC2 exists to eliminate, so the value is rejected at boot instead.
    """
    _clear_all_config_env(monkeypatch)
    _set_valid_chat_attester_secret(monkeypatch)
    monkeypatch.setenv("CURIE_API_PREFLIGHT_TIMEOUT_SECONDS", token)

    with pytest.raises(ValidationError):
        DispatcherConfig()


# --- CURIE_SHIMMER has exactly one reader (#1312) -----------------------------
#
# This block used to lock the OPPOSITE property. The dispatcher's bool parser
# accepted "on" as truthy and the worker's did not, and a test named
# ``test_bool_on_divergence_between_services`` asserted that split as intended
# behavior -- while both services read CURIE_SHIMMER. So `CURIE_SHIMMER=on`
# really meant "dispatcher raises the caption, worker never lowers it", and every
# turn stranded a shimmer until Slack's own timeout. No operator could have
# guessed that from the env name.
#
# #1312 removes the split at its source rather than aligning two parsers: the
# worker is the only service that reads the flag now, so one string cannot mean
# two things. These lock that there is still exactly one reader.


def test_the_dispatcher_does_not_read_the_shimmer_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One reader, so one token cannot mean two things.

    The regression this locks out: the dispatcher's bool parser accepted `on` as
    truthy and the worker's did not, while BOTH services read CURIE_SHIMMER. So
    `CURIE_SHIMMER=on` meant "dispatcher raises the caption, worker never lowers
    it", and every turn stranded a shimmer until Slack's own timeout. A test
    named ``test_bool_on_divergence_between_services`` asserted that split as
    intended behavior.

    #1312 removed the split at its source rather than aligning two parsers.
    Asserting the ABSENCE of the field is the whole property: a token table here
    could only re-test the worker's own parser (apps/worker/tests/test_config.py
    already does, including the `on` case), and an earlier version of this test
    tried exactly that and asserted `isinstance(..., bool)`, which cannot fail
    for a pydantic bool field and stayed green under an inverted parser (#1394).
    """
    _clear_all_config_env(monkeypatch)
    _set_valid_chat_attester_secret(monkeypatch)
    monkeypatch.setenv("CURIE_SHIMMER", "on")
    monkeypatch.setenv("CURIE_STATUS_TEXT", "is doing something")

    config = DispatcherConfig()

    assert not hasattr(config, "shimmer")
    assert not hasattr(config, "status_text")
    # Not merely unset for this construction -- not declared at all, so no
    # future env plumbing can quietly reintroduce a second reader.
    assert "shimmer" not in type(config).model_fields
    assert "status_text" not in type(config).model_fields


def test_a_malformed_slack_identity_declaration_refuses_dispatcher_boot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The dispatcher parses `CURIE_SLACK_IDENTITIES` with the same shared
    parser as the API and worker (ADR-0168 decision 1), so a declaration the
    chart would never render must refuse boot here too, not only at the API."""

    _set_valid_chat_attester_secret(monkeypatch)
    monkeypatch.setenv(
        "CURIE_SLACK_IDENTITIES",
        json.dumps(
            [
                {
                    "name": "second",
                    "app_token_env": "CURIE_SLACK_APP_TOKEN__0",
                    "bot_token_env": "PATH",
                    "signing_secret_env": None,
                }
            ]
        ),
    )

    with pytest.raises(ValidationError, match="CURIE_SLACK_IDENTITIES"):
        DispatcherConfig()
