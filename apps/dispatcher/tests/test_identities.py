"""Which Slack identities the dispatcher connects, and what each one stamps.

ADR-0168 decision 2: one Bolt app per declared identity. These are the rules
every app shares -- where an identity's tokens are read from, what a turn from
it carries in `ReplyHandle.adapter`, and the key its deliveries are claimed
under -- tested without Slack or Valkey.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator, Mapping

import pytest
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.identities import (
    SlackBotIds,
    SlackIdentityCredentials,
    bot_ids_from_auth_test,
    default_identity_credentials,
    delivery_key,
    minted_adapter,
    resolve_identity_credentials,
)

_ATTESTER = "dispatcher-attester-test-secret"

_DECLARED = [
    {
        "name": "default",
        "app_token_env": "SLACK_APP_TOKEN",
        "bot_token_env": "SLACK_BOT_TOKEN",
        "signing_secret_env": None,
    },
    {
        "name": "ops-bot",
        "app_token_env": "CURIE_SLACK_APP_TOKEN__1",
        "bot_token_env": "CURIE_SLACK_BOT_TOKEN__1",
        "signing_secret_env": "CURIE_SLACK_SIGNING_SECRET__1",
    },
]


class _RecordingEnv(Mapping[str, str]):
    """An environment that remembers every name it was asked for."""

    def __init__(self, values: Mapping[str, str]) -> None:
        self._values = dict(values)
        self.read: list[str] = []

    def __getitem__(self, key: str) -> str:
        self.read.append(key)
        return self._values[key]

    def get(self, key: str, default: object = None) -> object:  # type: ignore[override]
        self.read.append(key)
        return self._values.get(key, default)

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)


def _config(*, declared: list[dict[str, object]] | None = None) -> DispatcherConfig:
    kwargs: dict[str, object] = {
        "slack_app_token": "xapp-default",
        "slack_bot_token": "xoxb-default",
        "slack_signing_secret": "signing-default",
        "approval_chat_attester_secret": _ATTESTER,
    }
    if declared is not None:
        kwargs["slack_identities"] = json.dumps(declared)
    return DispatcherConfig(**kwargs)  # type: ignore[arg-type]


def test_a_stock_install_connects_default_alone_and_reads_no_other_env() -> None:
    env = _RecordingEnv({"CURIE_SLACK_APP_TOKEN__1": "xapp-stray"})

    resolved = resolve_identity_credentials(
        _config(), logger=logging.getLogger("test-identities"), environ=env
    )

    assert resolved == (
        SlackIdentityCredentials(
            name="default",
            app_token="xapp-default",
            bot_token="xoxb-default",
            signing_secret="signing-default",
        ),
    )
    assert resolved == (default_identity_credentials(_config()),)
    assert env.read == [], "a stock install must read only the SLACK_* settings it always read"


def test_declared_identities_read_their_declared_env_names_in_declared_order() -> None:
    env = _RecordingEnv(
        {
            "CURIE_SLACK_APP_TOKEN__1": "xapp-ops",
            "CURIE_SLACK_BOT_TOKEN__1": "xoxb-ops",
            "CURIE_SLACK_SIGNING_SECRET__1": "signing-ops",
        }
    )

    resolved = resolve_identity_credentials(
        _config(declared=_DECLARED), logger=logging.getLogger("test-identities"), environ=env
    )

    assert [identity.name for identity in resolved] == ["default", "ops-bot"]
    assert resolved[0] == default_identity_credentials(_config())
    assert resolved[1] == SlackIdentityCredentials(
        name="ops-bot",
        app_token="xapp-ops",
        bot_token="xoxb-ops",
        signing_secret="signing-ops",
    )
    assert sorted(env.read) == sorted(
        ["CURIE_SLACK_APP_TOKEN__1", "CURIE_SLACK_BOT_TOKEN__1", "CURIE_SLACK_SIGNING_SECRET__1"]
    )


def test_an_identity_with_a_blank_token_is_named_and_left_out(
    caplog: pytest.LogCaptureFixture,
) -> None:
    env = _RecordingEnv(
        {"CURIE_SLACK_APP_TOKEN__1": "xapp-ops-sentinel", "CURIE_SLACK_BOT_TOKEN__1": "  "}
    )
    logger = logging.getLogger("test-identities-blank")

    with caplog.at_level(logging.ERROR, logger=logger.name):
        resolved = resolve_identity_credentials(
            _config(declared=_DECLARED), logger=logger, environ=env
        )

    assert [identity.name for identity in resolved] == ["default"]
    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "ops-bot" in text and "CURIE_SLACK_BOT_TOKEN__1" in text
    assert "xapp-ops-sentinel" not in text


def test_credentials_never_print_their_tokens() -> None:
    credentials = SlackIdentityCredentials(
        name="ops-bot",
        app_token="xapp-sentinel",
        bot_token="xoxb-sentinel",
        signing_secret="signing-sentinel",
    )

    printed = f"{credentials!r} {credentials}"

    assert "ops-bot" in printed
    for secret in ("xapp-sentinel", "xoxb-sentinel", "signing-sentinel"):
        assert secret not in printed


def test_default_mints_no_adapter_and_any_other_identity_mints_its_name() -> None:
    assert minted_adapter("default") is None
    assert minted_adapter("ops-bot") == "ops-bot"


def test_delivery_key_is_bare_for_default_and_carries_any_other_identity() -> None:
    assert delivery_key("Ev0EXAMPLE1", "default") == "Ev0EXAMPLE1"
    assert delivery_key("action-trig-1", "default") == "action-trig-1"
    assert delivery_key("Ev0EXAMPLE1", "ops-bot") == "Ev0EXAMPLE1:ops-bot"
    assert delivery_key("Ev0EXAMPLE1", "ops-bot") != delivery_key("Ev0EXAMPLE1", "sales-bot")


def test_auth_test_answer_is_read_into_bot_ids() -> None:
    assert bot_ids_from_auth_test(
        {"ok": True, "team_id": "T1", "user_id": "U0OPS", "bot_id": "B0OPS"}
    ) == SlackBotIds(team_id="T1", app_id=None, bot_id="B0OPS", bot_user_id="U0OPS")
    assert bot_ids_from_auth_test(
        {"ok": True, "team_id": "T1", "user_id": "U0OPS", "bot_id": "B0OPS", "app_id": "A0OPS"}
    ) == SlackBotIds(team_id="T1", app_id="A0OPS", bot_id="B0OPS", bot_user_id="U0OPS")
    assert bot_ids_from_auth_test({"ok": False, "error": "invalid_auth"}) is None
    assert bot_ids_from_auth_test(object()) is None
