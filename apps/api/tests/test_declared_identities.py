"""The API reads the chart's Slack identities to validate a binding (ADR-0168 decision 1).

Until ADR-0155's `provider_installations` exists (#2909), the declared
identities are exactly what the chart rendered into `CURIE_SLACK_IDENTITIES`.
`get_settings()` is cached, so every case clears the cache after setting the
environment, and the fixture clears it again on the way out so no later test
reads this one's declaration.
"""

import json
from collections.abc import Callable, Iterator

import pytest
from curie_api.config import Settings, get_settings
from curie_api.identities import declared_identities, refuse_undeclared
from curie_api.schemas import ChannelBindingWrite
from pydantic import ValidationError

TWO_IDENTITIES = json.dumps(
    [
        {
            "name": "default",
            "app_token_env": "SLACK_APP_TOKEN",
            "bot_token_env": "SLACK_BOT_TOKEN",
            "signing_secret_env": "SLACK_SIGNING_SECRET",
        },
        {
            "name": "second",
            "app_token_env": "CURIE_SLACK_APP_TOKEN__0",
            "bot_token_env": "CURIE_SLACK_BOT_TOKEN__0",
            "signing_secret_env": None,
        },
    ]
)


@pytest.fixture
def declare(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[[str | None], None]]:
    def _declare(value: str | None) -> None:
        if value is None:
            monkeypatch.delenv("CURIE_SLACK_IDENTITIES", raising=False)
        else:
            monkeypatch.setenv("CURIE_SLACK_IDENTITIES", value)
        get_settings.cache_clear()

    yield _declare
    get_settings.cache_clear()


def test_with_no_declaration_slack_declares_only_the_default_app(
    declare: Callable[[str | None], None],
) -> None:
    declare(None)

    assert get_settings().slack_identities == ()
    assert declared_identities("slack") == frozenset({"default"})


def test_the_declared_names_are_the_slack_identities(
    declare: Callable[[str | None], None],
) -> None:
    declare(TWO_IDENTITIES)

    assert declared_identities("slack") == frozenset({"default", "second"})


def test_a_declared_identity_is_admitted_and_an_undeclared_one_is_refused(
    declare: Callable[[str | None], None],
) -> None:
    declare(TWO_IDENTITIES)

    refuse_undeclared("slack", "second")
    refuse_undeclared("slack", "default")
    with pytest.raises(ValueError, match="'third'.*'default', 'second'"):
        refuse_undeclared("slack", "third")


def test_without_a_declaration_a_second_name_is_still_refused(
    declare: Callable[[str | None], None],
) -> None:
    declare(None)

    with pytest.raises(ValueError, match="'second'.*'default'"):
        refuse_undeclared("slack", "second")


def test_a_binding_write_may_name_a_declared_identity(
    declare: Callable[[str | None], None],
) -> None:
    declare(TWO_IDENTITIES)

    written = ChannelBindingWrite(kind="slack", address="C0EXAMPLE1", adapter="second")

    assert written.adapter == "second"


def test_a_binding_write_naming_an_undeclared_identity_is_refused(
    declare: Callable[[str | None], None],
) -> None:
    declare(TWO_IDENTITIES)

    with pytest.raises(ValidationError, match="'third'"):
        ChannelBindingWrite(kind="slack", address="C0EXAMPLE1", adapter="third")


def test_other_kinds_are_still_not_enumerable(declare: Callable[[str | None], None]) -> None:
    declare(TWO_IDENTITIES)

    assert declared_identities("email") is None


def test_a_malformed_declaration_fails_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """A declaration the chart would never render refuses boot, not a later write."""

    monkeypatch.setenv("CURIE_SLACK_IDENTITIES", json.dumps([{"name": "second"}]))

    with pytest.raises(ValidationError, match="CURIE_SLACK_IDENTITIES"):
        Settings()
