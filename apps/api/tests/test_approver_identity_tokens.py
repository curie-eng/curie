"""User-group approvers resolve with the approval's own Slack identity.

ADR-0168 decision 5: `usergroups:read` is granted per Slack app, and each bot
is its own app. Slack is the one faked collaborator, behind a real
`SlackUserGroupClient` over `httpx.MockTransport`, so the token is read off the
request that left the process.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterator, Mapping

import httpx
import pytest
from curie_api.approvers import ApproverSet, MembershipVerdict
from curie_api.config import Settings
from curie_api.identities import slack_bot_tokens
from curie_api.models import Approval
from curie_api.slack_approvers import approval_token_identity, build_approver_set_selector

_GROUP = "S0MGRS001"
_APPROVER = "U0APPROV1"
_CARD_CHANNEL = "C0EXAMPLE1"
_BINDING = {"channel": _CARD_CHANNEL, "approvers": {"group": _GROUP}}
_DEFAULT_TOKEN = "xoxb-default-sentinel"
_OPS_TOKEN = "xoxb-ops-bot-sentinel"
_DECLARED = json.dumps(
    [
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
)


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


def _settings(*, default_token: str = _DEFAULT_TOKEN, declared: str | None = None) -> Settings:
    # The chart's reserved name, not the field name: the API reads only it.
    values: dict[str, object] = {"slack_bot_token": default_token}
    if declared is not None:
        values["CURIE_SLACK_IDENTITIES"] = declared
    return Settings(**values)  # type: ignore[arg-type]


def _approval(
    *, kind: str | None = "slack", adapter: str | None = None, endpoint: str | None = None
) -> Approval:
    return Approval(
        conversation_id="th-420",
        author="U0AUTHOR1",
        summary="Discount for ACME",
        reply_kind=kind,
        reply_channel="C0EXAMPLE2",
        reply_placeholder="p-1",
        reply_endpoint=endpoint,
        reply_adapter=adapter,
        dedupe_key="ev-420",
        route="managers",
        card_channel=_CARD_CHANNEL,
    )


def _http(calls: list[httpx.Request]) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"ok": True, "users": [_APPROVER]})

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _contains(approver_set: ApproverSet) -> MembershipVerdict:
    return asyncio.run(approver_set.contains(_APPROVER, _CARD_CHANNEL))


def _tokens(calls: list[httpx.Request]) -> list[str]:
    return [request.headers["Authorization"] for request in calls]


def test_a_stock_install_holds_the_default_token_alone_and_reads_no_env() -> None:
    env = _RecordingEnv({"CURIE_SLACK_BOT_TOKEN__1": "xoxb-stray"})

    assert slack_bot_tokens(_settings(), environ=env) == {"default": _DEFAULT_TOKEN}
    assert slack_bot_tokens(_settings(default_token=""), environ=env) == {}
    assert env.read == []


def test_a_declared_identity_with_a_blank_token_is_named_and_left_out(
    caplog: pytest.LogCaptureFixture,
) -> None:
    env = _RecordingEnv({"CURIE_SLACK_BOT_TOKEN__1": ""})

    with caplog.at_level(logging.ERROR, logger="curie_api.identities"):
        tokens = slack_bot_tokens(_settings(declared=_DECLARED), environ=env)

    assert tokens == {"default": _DEFAULT_TOKEN}
    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "ops-bot" in text and "CURIE_SLACK_BOT_TOKEN__1" in text
    assert _DEFAULT_TOKEN not in text


@pytest.mark.parametrize(
    ("kind", "adapter", "endpoint", "expected"),
    [
        ("slack", None, None, "default"),
        ("slack", "default", None, "default"),
        ("slack", "ops-bot", None, "ops-bot"),
        ("slack", "curie-cluster-message", None, "default"),
        # A Slack approval's endpoint is only a CLI stub turn's per-turn origin:
        # no Slack binding carries a transport since migration 0061, so the
        # custom-transport form this row used to pin no longer exists.
        ("slack", "default", "http://stub.example/api/", "default"),
        # A mail turn's card and its approvers are Slack's default app's.
        ("email", "agentmail-sandbox", "https://adapter.example/hook", "default"),
        (None, None, None, "default"),
    ],
)
def test_an_approval_resolves_through_its_route_identity(
    kind: str | None, adapter: str | None, endpoint: str | None, expected: str
) -> None:
    assert approval_token_identity(_approval(kind=kind, adapter=adapter, endpoint=endpoint)) == (
        expected
    )


def test_each_identitys_approval_asks_slack_with_its_own_token_and_cache() -> None:
    calls: list[httpx.Request] = []
    select = build_approver_set_selector(
        _http(calls),
        _settings(declared=_DECLARED),
        environ={"CURIE_SLACK_BOT_TOKEN__1": _OPS_TOKEN},
    )

    assert _contains(select(_approval(adapter="ops-bot"), _BINDING)).member
    assert _tokens(calls) == [f"Bearer {_OPS_TOKEN}"]
    # A second group lookup under default is its own client and its own cache:
    # the ops-bot answer above must not stand in for it.
    assert _contains(select(_approval(adapter=None), _BINDING)).member
    assert _tokens(calls) == [f"Bearer {_OPS_TOKEN}", f"Bearer {_DEFAULT_TOKEN}"]
    # And each cache still serves its own identity.
    assert _contains(select(_approval(adapter="ops-bot"), _BINDING)).member
    assert len(calls) == 2


def test_an_identity_with_no_token_fails_closed_and_never_borrows_default() -> None:
    calls: list[httpx.Request] = []
    select = build_approver_set_selector(
        _http(calls), _settings(declared=_DECLARED), environ={}
    )

    verdict = _contains(select(_approval(adapter="ops-bot"), _BINDING))

    assert not verdict.member and verdict.undetermined
    assert verdict.evidence is not None and "'ops-bot'" in verdict.evidence["error"]
    assert calls == []


def test_an_approval_naming_an_undeclared_identity_fails_closed() -> None:
    """A stored Slack `reply_adapter` is an identity. One this installation does
    not declare -- custom-transport history the upgrade leaves as stored, whose
    adapter was a credential slug -- has no token to resolve with, so it fails
    closed rather than borrowing default's."""

    calls: list[httpx.Request] = []
    select = build_approver_set_selector(
        _http(calls),
        _settings(declared=_DECLARED),
        environ={"CURIE_SLACK_BOT_TOKEN__1": _OPS_TOKEN},
    )

    verdict = _contains(select(_approval(adapter="agentmail-sandbox"), _BINDING))

    assert not verdict.member and verdict.undetermined
    assert verdict.evidence is not None and "'agentmail-sandbox'" in verdict.evidence["error"]
    assert calls == []


def test_a_stock_install_builds_the_one_client_it_always_built() -> None:
    env = _RecordingEnv({})
    calls: list[httpx.Request] = []
    select = build_approver_set_selector(_http(calls), _settings(), environ=env)

    assert _contains(select(_approval(), _BINDING)).member
    assert _tokens(calls) == [f"Bearer {_DEFAULT_TOKEN}"]
    assert env.read == []


def test_a_slack_free_install_keeps_its_historical_refusal() -> None:
    calls: list[httpx.Request] = []
    select = build_approver_set_selector(
        _http(calls), _settings(default_token=""), environ=_RecordingEnv({})
    )

    verdict = _contains(select(_approval(), _BINDING))

    assert verdict.undetermined
    assert verdict.evidence is not None
    assert verdict.evidence["error"] == "no Slack bot token is configured for the API"
    assert calls == []
