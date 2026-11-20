"""A lookup identity's credential serves directory reads and nothing else.

@spec GATEWAY-USER-EMAIL-7, GATEWAY-USER-EMAIL-8, GATEWAY-USER-EMAIL-9,
GATEWAY-USER-EMAIL-10
"""

import asyncio
import json
import logging
import os
from types import SimpleNamespace

import httpx
import pytest
from aiohttp.test_utils import TestClient, TestServer
from curie_channel_gateway.lookup_identities import LookupIdentities
from curie_channel_gateway.request_replay import request_headers
from curie_channel_gateway.slack import read_server
from curie_channel_gateway.slack.tokens import SlackProviderConfig
from curie_channel_gateway.token_custody import refuse_slack_tokens
from curie_channel_gateway.user_email_client import GatewayUserEmailSettings
from pydantic import TypeAdapter, ValidationError

EMAIL = "lookup.secret-sentinel@example.test"
SLACK_BASE = "https://slack.example.test/api/"
AGENT_TOKEN = "xoxb-agent-default-example"
SECOND_AGENT_TOKEN = "xoxb-agent-second-example"
LOOKUP_TOKEN = "xoxb-lookup-only-example"
PEOPLE_TOKEN = "xoxb-people-lookup-example"
LOOKUP_NAME = "directory"
LOOKUP_PREFIX = "CURIE_CHANNEL_GATEWAY_LOOKUP_TOKEN__"
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
EMAIL_BODY = {"user_id": "U0EXAMPLE1", "workspace": "T0EXAMPLE1"}


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


def _declare(monkeypatch, *entries, tokens=None):
    """Declare lookup identities; each gets LOOKUP_TOKEN unless tokens says otherwise."""

    declared = [
        {"name": name, "kind": kind, "token_env": f"{LOOKUP_PREFIX}{index}"}
        for index, (name, kind) in enumerate(entries)
    ]
    monkeypatch.setenv("CURIE_CHANNEL_GATEWAY_LOOKUP_IDENTITIES", json.dumps(declared))
    values = [LOOKUP_TOKEN] * len(entries) if tokens is None else tokens
    for index, value in enumerate(values):
        if value is not None:
            monkeypatch.setenv(f"{LOOKUP_PREFIX}{index}", value)


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


def _refused():
    async def scenario(_client):
        raise AssertionError("gateway started")

    with pytest.raises(ValueError) as error:
        asyncio.run(_gateway(scenario))
    assert LOOKUP_TOKEN not in str(error.value)
    return str(error.value)


def _parse(value):
    return TypeAdapter(LookupIdentities).validate_python(value)


# Declaration parser (@spec GATEWAY-USER-EMAIL-7, GATEWAY-USER-EMAIL-9)


def test_declaration_parses_name_kind_and_indexed_token_env():
    (identity,) = _parse(
        json.dumps([{"name": "directory", "kind": "slack", "token_env": f"{LOOKUP_PREFIX}0"}])
    )
    assert (identity.name, identity.kind, identity.token_env) == (
        "directory",
        "slack",
        f"{LOOKUP_PREFIX}0",
    )
    assert _parse("[]") == ()
    with pytest.raises(ValidationError):
        identity.name = "other"


def _entry(name="directory", kind="slack", token_env=f"{LOOKUP_PREFIX}0", **extra):
    return {"name": name, "kind": kind, "token_env": token_env, **extra}


@pytest.mark.parametrize(
    "raw",
    [
        json.dumps([_entry(name="Directory")]),
        json.dumps([_entry(name="directory_lookup")]),
        json.dumps([_entry(name="d" * 41)]),
        json.dumps([_entry(name="default")]),
        json.dumps([_entry(name="curie-cluster-message")]),
        json.dumps([_entry(token_env="SLACK_BOT_TOKEN")]),
        json.dumps([_entry(token_env=f"{LOOKUP_PREFIX}01")]),
        json.dumps([{"name": "directory", "kind": "slack"}]),
        json.dumps([_entry(kind="")]),
        json.dumps([_entry(scopes=["users:read"])]),
        json.dumps([_entry(), _entry(token_env=f"{LOOKUP_PREFIX}1")]),
        json.dumps([_entry(), _entry(name="people")]),
        json.dumps(_entry()),
        "not json",
    ],
    ids=[
        "uppercase-name",
        "underscore-name",
        "long-name",
        "default-name",
        "cluster-message-name",
        "unprefixed-token-env",
        "padded-index",
        "missing-token-env",
        "blank-kind",
        "extra-field",
        "duplicate-name",
        "duplicate-token-env",
        "not-a-list",
        "not-json",
    ],
)
def test_declaration_refuses_a_malformed_entry(raw):
    with pytest.raises(ValidationError):
        _parse(raw)


def test_gateway_config_refuses_a_malformed_declaration(monkeypatch):
    monkeypatch.setenv("CURIE_CHANNEL_GATEWAY_LOOKUP_IDENTITIES", '[{"name": "Directory"}]')
    with pytest.raises(ValueError):
        read_server.GatewayConfig()


# Boot refusals (@spec GATEWAY-USER-EMAIL-9)


def test_gateway_refuses_an_unsupported_kind_and_names_the_supported_ones(monkeypatch, slack):
    _declare(monkeypatch, (LOOKUP_NAME, "ldap"))
    message = _refused()
    assert "ldap" in message and "slack" in message


def test_gateway_refuses_a_lookup_identity_named_like_a_declared_identity(monkeypatch, slack):
    monkeypatch.setenv("CURIE_SLACK_IDENTITIES", DECLARED)
    monkeypatch.setenv("CURIE_SLACK_BOT_TOKEN__1", SECOND_AGENT_TOKEN)
    _declare(monkeypatch, ("second", "slack"))
    assert "second" in _refused()


@pytest.mark.parametrize("token", [None, "", "  "], ids=["unset", "empty", "blank"])
def test_gateway_refuses_an_unset_or_blank_lookup_token(monkeypatch, slack, token):
    _declare(monkeypatch, (LOOKUP_NAME, "slack"), tokens=[token])
    assert f"{LOOKUP_PREFIX}0" in _refused()


# Isolation (@spec GATEWAY-USER-EMAIL-8)


def test_lookup_identity_reads_user_email_with_its_own_token(monkeypatch, slack, constructed):
    _declare(monkeypatch, (LOOKUP_NAME, "slack"))

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


def test_two_lookup_identities_each_read_with_their_own_token(monkeypatch, slack, constructed):
    _declare(
        monkeypatch,
        (LOOKUP_NAME, "slack"),
        ("people", "slack"),
        tokens=[LOOKUP_TOKEN, PEOPLE_TOKEN],
    )

    async def scenario(client):
        identity = CALLERS["CURIE_CHANNEL_GATEWAY_IDENTITY_TOKEN"]
        for name, token in ((LOOKUP_NAME, LOOKUP_TOKEN), ("people", PEOPLE_TOKEN)):
            status, _text = await _post(client, identity, name, "user_email", EMAIL_BODY)
            assert status == 200
            assert slack[-1].headers["Authorization"] == f"Bearer {token}"

    asyncio.run(_gateway(scenario))


def test_no_other_gateway_surface_receives_the_lookup_token(monkeypatch, slack, constructed):
    monkeypatch.setenv("CURIE_SLACK_IDENTITIES", DECLARED)
    monkeypatch.setenv("CURIE_SLACK_BOT_TOKEN__1", SECOND_AGENT_TOKEN)
    _declare(monkeypatch, (LOOKUP_NAME, "slack"))

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


@pytest.mark.parametrize("declaration", [None, "[]"], ids=["absent", "empty"])
def test_without_lookup_identities_everything_is_unchanged(
    monkeypatch, slack, constructed, declaration
):
    if declaration is not None:
        monkeypatch.setenv("CURIE_CHANNEL_GATEWAY_LOOKUP_IDENTITIES", declaration)

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


# Custody (@spec GATEWAY-USER-EMAIL-10)


@pytest.mark.parametrize("value", ["opaque-lookup-credential", LOOKUP_TOKEN])
def test_port_consumers_refuse_the_lookup_token_prefix(monkeypatch, value):
    monkeypatch.delenv("SLACK_BOT_TOKEN")
    with pytest.raises(ValueError):
        refuse_slack_tokens({f"{LOOKUP_PREFIX}0": value})
    monkeypatch.setenv(f"{LOOKUP_PREFIX}3", value)
    with pytest.raises(ValueError):
        refuse_slack_tokens({})
    monkeypatch.setenv("CURIE_CHANNEL_GATEWAY_URL", "https://gateway.example.test")
    with pytest.raises(ValueError):
        GatewayUserEmailSettings()
