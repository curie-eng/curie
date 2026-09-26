"""Which bot token each Slack identity replies with (ADR-0168 decision 5).

`slack_bot_tokens` reads the tokens the chart hands the worker, and
`token_identity` names the identity a Slack route's calls authenticate as. No
Slack and no Valkey.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator, Mapping

import pytest
from aci_protocol.turn import CLUSTER_MESSAGE_ADAPTER
from curie_worker.config import WorkerConfig
from curie_worker.slack_tokens import slack_bot_tokens, token_identity

_DEFAULT_TOKEN = "xoxb-default-sentinel"
_OPS_TOKEN = "xoxb-ops-bot-sentinel"

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
        "signing_secret_env": None,
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


def _config(
    *, declared: list[dict[str, object]] | None = None, default_token: str = _DEFAULT_TOKEN
) -> WorkerConfig:
    kwargs: dict[str, object] = {"slack_bot_token": default_token}
    if declared is not None:
        kwargs["slack_identities"] = json.dumps(declared)
    return WorkerConfig(**kwargs)  # type: ignore[arg-type]


def test_a_stock_install_holds_the_default_token_alone_and_reads_no_env() -> None:
    env = _RecordingEnv({"CURIE_SLACK_BOT_TOKEN__1": "xoxb-stray"})

    assert slack_bot_tokens(_config(), environ=env) == {"default": _DEFAULT_TOKEN}
    assert env.read == [], "a stock install must read only the settings it always read"


def test_a_blank_default_token_is_still_the_default_identitys_token() -> None:
    # A mail-only install has no Slack token; it must keep sending what it
    # always sent rather than start refusing a route it never serves.
    assert slack_bot_tokens(_config(default_token=""), environ=_RecordingEnv({})) == {
        "default": ""
    }


def test_a_declared_identity_reads_only_its_own_bot_token() -> None:
    env = _RecordingEnv(
        {"CURIE_SLACK_BOT_TOKEN__1": _OPS_TOKEN, "CURIE_SLACK_APP_TOKEN__1": "xapp-ops"}
    )

    tokens = slack_bot_tokens(_config(declared=_DECLARED), environ=env)

    assert tokens == {"default": _DEFAULT_TOKEN, "ops-bot": _OPS_TOKEN}
    assert env.read == ["CURIE_SLACK_BOT_TOKEN__1"], "the worker holds no app token"


def test_an_identity_with_a_blank_bot_token_is_named_and_left_out(
    caplog: pytest.LogCaptureFixture,
) -> None:
    env = _RecordingEnv({"CURIE_SLACK_BOT_TOKEN__1": "  "})

    with caplog.at_level(logging.ERROR, logger="curie_worker.slack_tokens"):
        tokens = slack_bot_tokens(_config(declared=_DECLARED), environ=env)

    assert tokens == {"default": _DEFAULT_TOKEN}
    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "ops-bot" in text and "CURIE_SLACK_BOT_TOKEN__1" in text
    assert _DEFAULT_TOKEN not in text


@pytest.mark.parametrize(
    ("adapter", "endpoint", "expected"),
    [
        (None, None, "default"),
        ("default", None, "default"),
        ("ops-bot", None, "ops-bot"),
        (CLUSTER_MESSAGE_ADAPTER, None, "default"),
        # A CLI stub turn's per-turn Slack origin does not pick the token.
        ("ops-bot", "http://stub.example/api/", "ops-bot"),
        (None, "http://stub.example/api/", "default"),
    ],
)
def test_a_slack_route_speaks_as_its_resolved_identity(
    adapter: str | None, endpoint: str | None, expected: str
) -> None:
    assert token_identity(adapter, endpoint) == expected
