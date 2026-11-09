"""Approval identity routing traverses the real authenticated gateway."""

import asyncio
import json
from contextlib import contextmanager

import httpx
import pytest
from curie_api.config import Settings
from curie_api.models import Approval
from curie_api.slack_approvers import approval_token_identity, build_approver_set_selector
from curie_test_support.channel_gateway import API_TOKEN, slack_gateway

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


@pytest.mark.parametrize(
    ("kind", "adapter", "endpoint", "expected"),
    [
        ("slack", None, None, "default"),
        ("slack", "default", None, "default"),
        ("slack", "ops-bot", None, "ops-bot"),
        ("slack", "curie-cluster-message", None, "default"),
        # A Slack approval's endpoint is only a CLI stub turn's per-turn origin:
        # no Slack binding carries a transport since migration 0070, so the
        # custom-transport form this row used to pin no longer exists.
        ("slack", "default", "http://stub.example/api/", "default"),
        ("slack", "ops-bot", "http://stub.example/api/", "ops-bot"),
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


@contextmanager
def selection(tokens, *, declared=_DECLARED, configured=True):
    calls = []

    def provider(request):
        calls.append(request)
        return httpx.Response(200, json={"ok": True, "users": [_APPROVER]})

    with slack_gateway(provider, tokens) as url:
        settings = Settings(
            CURIE_CHANNEL_GATEWAY_URL=url if configured else "",
            CURIE_CHANNEL_GATEWAY_TOKEN=API_TOKEN if configured else "",
            CURIE_CHANNEL_GATEWAY_ALLOW_INSECURE=True,
            CURIE_SLACK_IDENTITIES=declared,
        )
        yield settings, calls


def test_each_identitys_approval_asks_slack_with_its_own_token_and_cache():
    with selection({"default": _DEFAULT_TOKEN, "ops-bot": _OPS_TOKEN}) as (settings, calls):

        async def scenario():
            async with httpx.AsyncClient() as http:
                select = build_approver_set_selector(http, settings)
                for identity in ("ops-bot", None, "ops-bot"):
                    assert (
                        await select(_approval(adapter=identity), _BINDING).contains(
                            _APPROVER, None
                        )
                    ).member

        asyncio.run(scenario())
        assert [r.headers["Authorization"] for r in calls] == [
            f"Bearer {_OPS_TOKEN}",
            f"Bearer {_DEFAULT_TOKEN}",
        ]


@pytest.mark.parametrize(
    "identity,tokens",
    [
        ("ops-bot", {"default": _DEFAULT_TOKEN}),
        ("unknown", {"default": _DEFAULT_TOKEN, "ops-bot": _OPS_TOKEN}),
    ],
)
def test_unavailable_identity_fails_closed_without_borrowing_default(identity, tokens):
    with selection(tokens) as (settings, calls):

        async def scenario():
            async with httpx.AsyncClient() as http:
                select = build_approver_set_selector(http, settings)
                verdict = await select(_approval(adapter=identity), _BINDING).contains(
                    _APPROVER, None
                )
                assert not verdict.member and verdict.undetermined

        asyncio.run(scenario())
        assert not calls


def test_a_slack_free_install_fails_closed():
    with selection({}, configured=False) as (settings, calls):

        async def scenario():
            async with httpx.AsyncClient() as http:
                verdict = await build_approver_set_selector(http, settings)(
                    _approval(), _BINDING
                ).contains(_APPROVER, None)
                assert not verdict.member and verdict.undetermined

        asyncio.run(scenario())
        assert not calls
