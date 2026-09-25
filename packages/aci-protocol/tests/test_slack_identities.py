"""The parsed form of ``CURIE_SLACK_IDENTITIES`` (ADR-0168 decision 1).

The chart renders the declaration; this parser is what the dispatcher, the
worker and the API read it with. Its env-name rule is what makes the worker's
sandbox filter complete: every token an identity names lives under a legacy
``SLACK_*`` name or an indexed ``CURIE_SLACK_*__<n>`` name, and nowhere else.
"""

import json

import pytest
from aci_protocol.slack_identities import (
    SLACK_CREDENTIAL_ENV_PREFIXES,
    SLACK_IDENTITIES_ENV,
    SlackIdentities,
    SlackIdentity,
    declared_slack_identity_names,
)
from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings

DEFAULT = {
    "name": "default",
    "app_token_env": "SLACK_APP_TOKEN",
    "bot_token_env": "SLACK_BOT_TOKEN",
    "signing_secret_env": "SLACK_SIGNING_SECRET",
}
SECOND = {
    "name": "second",
    "app_token_env": "CURIE_SLACK_APP_TOKEN__0",
    "bot_token_env": "CURIE_SLACK_BOT_TOKEN__0",
    "signing_secret_env": None,
}


class _Settings(BaseSettings):
    """The field exactly as the three services declare it."""

    slack_identities: SlackIdentities = Field(default=(), validation_alias=SLACK_IDENTITIES_ENV)


def _parse(monkeypatch: pytest.MonkeyPatch, value: object) -> tuple[SlackIdentity, ...]:
    raw = value if isinstance(value, str) else json.dumps(value)
    monkeypatch.setenv(SLACK_IDENTITIES_ENV, raw)
    return _Settings().slack_identities


def test_an_absent_declaration_is_the_one_default_app(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(SLACK_IDENTITIES_ENV, raising=False)

    assert _Settings().slack_identities == ()
    assert declared_slack_identity_names(()) == frozenset({"default"})


def test_a_blank_declaration_is_the_same_as_an_absent_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert _parse(monkeypatch, "  ") == ()


def test_a_declaration_parses_in_order_and_names_every_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parsed = _parse(monkeypatch, [DEFAULT, SECOND])

    assert parsed == (SlackIdentity(**DEFAULT), SlackIdentity(**SECOND))
    assert declared_slack_identity_names(parsed) == frozenset({"default", "second"})


def test_the_chart_rendering_parses_as_rendered(monkeypatch: pytest.MonkeyPatch) -> None:
    """Go's toJson sorts keys and writes `null`; the parser takes both."""

    rendered = (
        '[{"app_token_env":"SLACK_APP_TOKEN","bot_token_env":"SLACK_BOT_TOKEN",'
        '"name":"default","signing_secret_env":"SLACK_SIGNING_SECRET"},'
        '{"app_token_env":"CURIE_SLACK_APP_TOKEN__0","bot_token_env":"CURIE_SLACK_BOT_TOKEN__0",'
        '"name":"second","signing_secret_env":null}]'
    )

    assert _parse(monkeypatch, rendered) == (SlackIdentity(**DEFAULT), SlackIdentity(**SECOND))


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("null", "must be a JSON list"),
        ("{}", "must be a JSON list"),
        ([SECOND], "declares no 'default' identity"),
        ([DEFAULT, SECOND, SECOND], "repeats identity names \\['second'\\]"),
        ([DEFAULT, {**SECOND, "name": "Second"}], "should match pattern"),
        ([DEFAULT, {**SECOND, "name": "a" * 41}], "at most 40 characters"),
        ([DEFAULT, {**SECOND, "note": "x"}], "Extra inputs are not permitted"),
        ([{**DEFAULT, "bot_token_env": "CURIE_SLACK_BOT_TOKEN__0"}], "'default' must read"),
        ([DEFAULT, {**SECOND, "bot_token_env": "PATH"}], "CURIE_SLACK_BOT_TOKEN__<index>"),
        ([DEFAULT, {**SECOND, "app_token_env": "CURIE_SLACK_APP_TOKEN__01"}], "<index>"),
        (
            [DEFAULT, {**SECOND, "signing_secret_env": "SLACK_SIGNING_SECRET"}],
            "CURIE_SLACK_SIGNING_SECRET__<index>",
        ),
        (
            [
                DEFAULT,
                SECOND,
                {**SECOND, "name": "third", "app_token_env": "CURIE_SLACK_APP_TOKEN__1"},
            ],
            "gives two identities the env names \\['CURIE_SLACK_BOT_TOKEN__0'\\]",
        ),
    ],
)
def test_a_declaration_the_chart_would_never_render_is_refused(
    monkeypatch: pytest.MonkeyPatch, value: object, message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        _parse(monkeypatch, value)


def test_a_forty_character_name_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    parsed = _parse(monkeypatch, [DEFAULT, {**SECOND, "name": "a" * 40}])

    assert parsed[1].name == "a" * 40


def test_every_indexed_env_name_falls_under_a_credential_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The sandbox filter drops by these prefixes, so nothing may escape them."""

    parsed = _parse(
        monkeypatch,
        [DEFAULT, {**SECOND, "signing_secret_env": "CURIE_SLACK_SIGNING_SECRET__0"}],
    )
    indexed = [
        name
        for identity in parsed
        if identity.name != "default"
        for name in (identity.app_token_env, identity.bot_token_env, identity.signing_secret_env)
    ]

    assert indexed and all(
        name is not None and name.startswith(SLACK_CREDENTIAL_ENV_PREFIXES) for name in indexed
    )
