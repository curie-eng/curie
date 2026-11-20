"""A lookup-only Slack identity serves the user email read and nothing else.

@spec GATEWAY-USER-EMAIL-7
"""

import asyncio
import json
import logging
import os
from types import SimpleNamespace

import httpx
import pytest
from aiohttp.test_utils import TestClient, TestServer
from curie_channel_gateway.request_replay import request_headers
from curie_channel_gateway.slack import read_server
from curie_channel_gateway.slack.tokens import SlackProviderConfig
from curie_channel_gateway.user_email_client import GatewayUserEmailSettings

EMAIL = "lookup.secret-sentinel@example.test"
SLACK_BASE = "https://slack.example.test/api/"
AGENT_TOKEN = "xoxb-agent-default-example"
SECOND_AGENT_TOKEN = "xoxb-agent-second-example"
LOOKUP_TOKEN = "xoxb-lookup-only-example"
LOOKUP_NAME = "directory"
CALLERS = {
    "CURIE_CHANNEL_GATEWAY_API_TOKEN": "api-credential",
    "CURIE_CHANNEL_GATEWAY_WORKER_TOKEN": "worker-credential",
    "CURIE_CHANNEL_GATEWAY_IDENTITY_TOKEN": "identity-credential",
}
PROVIDER_CLASSES = (
    "SlackReplyAdapter",
    "SlackUserGroupClient",
    "SlackFileClient",
    "SlackChannelReader",
    "SlackCanvasReader",
    "SlackSenderIdentities",
    "SlackAccountProfiles",
    "SlackUserEmails",
)
DECLARED = json.dumps(
    [
        {"name": "default", "app_token_env": "SLACK_APP_TOKEN", "bot_token_env": "SLACK_BOT_TOKEN"},
        {
            "name": "second",
            "app_token_env": "CURIE_SLACK_APP_TOKEN__1",
            "bot_token_env": "CURIE_SLACK_BOT_TOKEN__1",
        },
    ]
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch, tmp_path):
    for name in list(os.environ):
        if "SLACK" in name.upper() or name.startswith("CURIE_CHANNEL_GATEWAY_"):
            monkeypatch.delenv(name, raising=False)
    for name, value in CALLERS.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("CURIE_CHANNEL_GATEWAY_REPLAY_DB", str(tmp_path / "requests.sqlite3"))
    monkeypatch.setenv("CURIE_CHANNEL_GATEWAY_ALLOW_INSECURE", "true")
    monkeypatch.setenv("SLACK_BOT_TOKEN", AGENT_TOKEN)


@pytest.fixture(autouse=True)
def _no_email_in_logs(caplog):
    caplog.set_level(logging.DEBUG)
    yield
    assert EMAIL not in caplog.text
    assert LOOKUP_TOKEN not in caplog.text


@pytest.fixture
def slack(monkeypatch):
    """Every Slack HTTP call the gateway makes, answered by users.info for one user."""

    calls: list[httpx.Request] = []

    def answer(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        user = {
            "id": "U0EXAMPLE1",
            "team_id": "T0EXAMPLE1",
            "deleted": False,
            "is_bot": False,
            "is_app_user": False,
            "profile": {"email": EMAIL},
        }
        return httpx.Response(200, json={"ok": True, "user": user})

    real = httpx.AsyncClient

    def client(**kwargs):
        return real(transport=httpx.MockTransport(answer), **kwargs)

    monkeypatch.setattr(read_server, "httpx", SimpleNamespace(AsyncClient=client))
    return calls


@pytest.fixture
def constructed(monkeypatch):
    """The arguments every Slack provider in the gateway process was built with."""

    seen: dict[str, list[str]] = {}
    for name in PROVIDER_CLASSES:
        real = getattr(read_server, name)

        def build(*args, _name=name, _real=real, **kwargs):
            seen.setdefault(_name, []).append(repr((args, kwargs)))
            return _real(*args, **kwargs)

        monkeypatch.setattr(read_server, name, build)
    return seen


def _configure_lookup(monkeypatch, name=LOOKUP_NAME, token=LOOKUP_TOKEN):
    if name is not None:
        monkeypatch.setenv("CURIE_CHANNEL_GATEWAY_USER_EMAIL_SLACK_IDENTITY", name)
    if token is not None:
        monkeypatch.setenv("CURIE_CHANNEL_GATEWAY_USER_EMAIL_SLACK_BOT_TOKEN", token)


async def _gateway(scenario):
    config = read_server.GatewayConfig(slack_api_base_url=SLACK_BASE)
    app = await read_server.application(config, SlackProviderConfig())
    async with TestClient(TestServer(app)) as client:
        return await scenario(client)


async def _post(client, caller, identity, operation, body):
    path = f"/v1/gateway/{identity}/{operation}"
    content = json.dumps(body).encode()
    url = str(client.make_url(path))
    response = await client.post(
        path, data=content, headers=request_headers(caller, "POST", url, content)
    )
    return response.status, await response.text()


EMAIL_BODY = {"user_id": "U0EXAMPLE1", "workspace": "T0EXAMPLE1"}


def test_lookup_identity_reads_user_email_with_its_own_token(monkeypatch, slack, constructed):
    _configure_lookup(monkeypatch)

    async def scenario(client):
        identity = CALLERS["CURIE_CHANNEL_GATEWAY_IDENTITY_TOKEN"]
        status, text = await _post(client, identity, LOOKUP_NAME, "user_email", EMAIL_BODY)
        assert status == 200
        assert json.loads(text) == {"user_id": "U0EXAMPLE1", "email": EMAIL}
        assert slack[-1].headers["Authorization"] == f"Bearer {LOOKUP_TOKEN}"
        # The agent's own identity still answers with the agent's token.
        status, _text = await _post(client, identity, "default", "user_email", EMAIL_BODY)
        assert status == 200
        assert slack[-1].headers["Authorization"] == f"Bearer {AGENT_TOKEN}"

    asyncio.run(_gateway(scenario))


def test_no_other_gateway_surface_receives_the_lookup_token(monkeypatch, slack, constructed):
    monkeypatch.setenv("CURIE_SLACK_IDENTITIES", DECLARED)
    monkeypatch.setenv("CURIE_SLACK_BOT_TOKEN__1", SECOND_AGENT_TOKEN)
    _configure_lookup(monkeypatch)

    async def scenario(client):
        api = CALLERS["CURIE_CHANNEL_GATEWAY_API_TOKEN"]
        status, _text = await _post(client, api, LOOKUP_NAME, "members", {"group_id": "S0EXAMPLE1"})
        assert status != 200
        assert all(LOOKUP_TOKEN not in str(call.headers) for call in slack)

    asyncio.run(_gateway(scenario))
    assert set(constructed) == set(PROVIDER_CLASSES)
    for name, calls in constructed.items():
        carries = any(LOOKUP_TOKEN in call for call in calls)
        assert carries == (name == "SlackUserEmails"), name
    # Declared identities keep reaching every surface they reached before.
    assert any(SECOND_AGENT_TOKEN in call for call in constructed["SlackReplyAdapter"])


def test_without_a_lookup_identity_everything_is_unchanged(monkeypatch, slack, constructed):
    async def scenario(client):
        identity = CALLERS["CURIE_CHANNEL_GATEWAY_IDENTITY_TOKEN"]
        status, _text = await _post(client, identity, LOOKUP_NAME, "user_email", EMAIL_BODY)
        assert status == 502
        status, _text = await _post(client, identity, "default", "user_email", EMAIL_BODY)
        assert status == 200
        assert [call.headers["Authorization"] for call in slack] == [f"Bearer {AGENT_TOKEN}"]

    asyncio.run(_gateway(scenario))
    (emails,) = constructed["SlackUserEmails"]
    (reader,) = constructed["SlackChannelReader"]
    assert repr({"default": AGENT_TOKEN}) in emails
    assert repr({"default": AGENT_TOKEN}) in reader


@pytest.mark.parametrize(
    ("name", "token", "declared"),
    [
        (LOOKUP_NAME, None, False),
        (None, LOOKUP_TOKEN, False),
        (LOOKUP_NAME, " ", False),
        ("Directory", LOOKUP_TOKEN, False),
        ("directory_lookup", LOOKUP_TOKEN, False),
        ("d" * 41, LOOKUP_TOKEN, False),
        ("default", LOOKUP_TOKEN, False),
        ("curie-cluster-message", LOOKUP_TOKEN, False),
        ("second", LOOKUP_TOKEN, True),
    ],
)
def test_gateway_refuses_a_malformed_or_colliding_lookup_identity(
    monkeypatch, slack, name, token, declared
):
    if declared:
        monkeypatch.setenv("CURIE_SLACK_IDENTITIES", DECLARED)
        monkeypatch.setenv("CURIE_SLACK_BOT_TOKEN__1", SECOND_AGENT_TOKEN)
    _configure_lookup(monkeypatch, name=name, token=token)

    async def scenario(_client):
        raise AssertionError("gateway started")

    with pytest.raises(ValueError) as error:
        asyncio.run(_gateway(scenario))
    assert LOOKUP_TOKEN not in str(error.value)


def test_gateway_config_hides_the_lookup_token(monkeypatch):
    _configure_lookup(monkeypatch)
    config = read_server.GatewayConfig()
    assert config.user_email_slack_identity == LOOKUP_NAME
    assert config.user_email_slack_bot_token == LOOKUP_TOKEN
    assert LOOKUP_TOKEN not in repr(config)


def test_port_consumers_refuse_the_lookup_token(monkeypatch):
    monkeypatch.delenv("SLACK_BOT_TOKEN")
    monkeypatch.setenv("CURIE_CHANNEL_GATEWAY_URL", "https://gateway.example.test")
    _configure_lookup(monkeypatch, name=None)
    with pytest.raises(ValueError):
        GatewayUserEmailSettings()
