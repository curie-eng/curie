"""An identity provider resolves a Slack user's email through the gateway.

@spec GATEWAY-USER-EMAIL-1, GATEWAY-USER-EMAIL-2, GATEWAY-USER-EMAIL-3,
GATEWAY-USER-EMAIL-4, GATEWAY-USER-EMAIL-5.
"""

import asyncio
import logging
import os

import httpx
import pytest
from aiohttp.test_utils import TestServer
from curie_channel_gateway.read_transport import GatewayProviders, gateway_application
from curie_channel_gateway.request_replay import RequestReplayStore, request_headers
from curie_channel_gateway.slack.user_emails import SlackUserEmails
from curie_channel_gateway.user_email_client import (
    GatewayUserEmails,
    GatewayUserEmailSettings,
)
from curie_channel_gateway.user_email_models import UserEmailLookupError, UserEmailUnknown

EMAIL = "alice.secret-sentinel@example.test"
SLACK_BASE = "https://slack.example.test/api/"
TOKENS = {
    "api": "api-credential",
    "worker": "worker-credential",
    "identity": "identity-credential",
}


def _user(**overrides):
    user = {
        "id": "U0EXAMPLE1",
        "team_id": "T0EXAMPLE1",
        "deleted": False,
        "is_bot": False,
        "is_app_user": False,
        "profile": {"email": EMAIL},
    }
    user.update(overrides)
    return user


def _ok(user):
    return httpx.Response(200, json={"ok": True, "user": user})


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in list(os.environ):
        if "SLACK" in name.upper() or name.startswith("CURIE_CHANNEL_GATEWAY_"):
            monkeypatch.delenv(name, raising=False)


@pytest.fixture(autouse=True)
def _no_email_in_logs(caplog):
    caplog.set_level(logging.DEBUG)
    yield
    assert EMAIL not in caplog.text
    assert EMAIL.split("@")[0] not in caplog.text


async def _run(slack, scenario, *, gateway_timeout_s=5.0, providers=None, tokens=None):
    calls = []

    async def record(request):
        calls.append(request)
        answer = slack(request)
        if asyncio.iscoroutine(answer):
            answer = await answer
        return answer

    async with httpx.AsyncClient(transport=httpx.MockTransport(record)) as provider:
        emails = SlackUserEmails(
            provider,
            {"default": "xoxb-gateway-only"},
            base_url=SLACK_BASE,
            timeout_s=gateway_timeout_s,
        )
        app = gateway_application(
            replay=RequestReplayStore(":memory:"),
            tokens=tokens or TOKENS,
            providers=providers or GatewayProviders(emails=emails),
            allow_insecure_http=True,
        )
        async with TestServer(app) as server:
            endpoint = str(server.make_url("")).rstrip("/")
            return await scenario(endpoint, calls)


def _settings(monkeypatch, endpoint, **extra):
    monkeypatch.setenv("CURIE_CHANNEL_GATEWAY_URL", endpoint)
    monkeypatch.setenv("CURIE_CHANNEL_GATEWAY_IDENTITY_TOKEN", TOKENS["identity"])
    monkeypatch.setenv("CURIE_CHANNEL_GATEWAY_ALLOW_INSECURE", "true")
    for name, value in extra.items():
        monkeypatch.setenv(name, value)
    return GatewayUserEmailSettings()


def test_provider_configured_for_gateway_lookup_resolves_email_without_a_slack_token(
    monkeypatch,
):
    async def scenario(endpoint, calls):
        settings = _settings(monkeypatch, endpoint)
        assert not any("SLACK" in name.upper() and "TOKEN" in name.upper() for name in os.environ)
        assert "xox" not in repr(settings) and "identity-credential" not in repr(settings)
        async with GatewayUserEmails.from_settings(settings) as emails:
            assert await emails.email_of("U0EXAMPLE1", "T0EXAMPLE1") == EMAIL
            assert await emails.email_of("U0EXAMPLE1", "T0EXAMPLE1") == EMAIL
        assert len(calls) == 2  # fresh on every call, never cached
        request = calls[0]
        assert str(request.url).split("?", 1)[0] == SLACK_BASE + "users.info"
        assert request.url.params["user"] == "U0EXAMPLE1"
        assert request.headers["Authorization"] == "Bearer xoxb-gateway-only"

    asyncio.run(_run(lambda _request: _ok(_user()), scenario))


@pytest.mark.parametrize(
    "user",
    [
        _user(
            enterprise_user={"enterprise_id": "E0EXAMPLE1", "teams": ["T0EXAMPLE9"]},
            team_id="T0EXAMPLE9",
        ),
        _user(
            enterprise_user={"enterprise_id": "E0EXAMPLE9", "teams": ["T0EXAMPLE1"]},
            team_id="T0EXAMPLE9",
        ),
    ],
)
def test_enterprise_membership_satisfies_the_workspace(monkeypatch, user):
    workspace = (
        "E0EXAMPLE1" if user["enterprise_user"]["enterprise_id"] == "E0EXAMPLE1" else ("T0EXAMPLE1")
    )

    async def scenario(endpoint, _calls):
        async with GatewayUserEmails.from_settings(_settings(monkeypatch, endpoint)) as emails:
            assert await emails.email_of("U0EXAMPLE1", workspace) == EMAIL

    asyncio.run(_run(lambda _request: _ok(user), scenario))


def test_settings_refuse_a_slack_token(monkeypatch):
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-must-not-be-here")
    with pytest.raises(ValueError) as error:
        _settings(monkeypatch, "https://gateway.example.test")
    assert "xoxb-must-not-be-here" not in str(error.value)


@pytest.mark.parametrize(
    "answer",
    [
        httpx.Response(200, json={"ok": False, "error": "user_not_found"}),
        httpx.Response(200, json={"ok": False, "error": "user_not_visible"}),
        _ok(_user(team_id="T0EXAMPLE9")),
        _ok(_user(id="U0EXAMPLE9")),
        _ok(_user(deleted=True)),
        _ok(_user(is_bot=True)),
        _ok(_user(is_app_user=True)),
        _ok(_user(profile={})),
        _ok(_user(profile={"email": ""})),
    ],
    ids=[
        "not-found",
        "not-visible",
        "other-workspace",
        "other-user",
        "deleted",
        "bot",
        "app-user",
        "no-email",
        "blank-email",
    ],
)
def test_unknown_users_refuse(monkeypatch, answer):
    async def scenario(endpoint, _calls):
        async with GatewayUserEmails.from_settings(_settings(monkeypatch, endpoint)) as emails:
            with pytest.raises(UserEmailUnknown):
                await emails.email_of("U0EXAMPLE1", "T0EXAMPLE1")

    asyncio.run(_run(lambda _request: answer, scenario))


@pytest.mark.parametrize(
    "answer",
    [
        httpx.Response(500, text="boom " + EMAIL),
        httpx.Response(200, json={"ok": False, "error": "ratelimited"}),
        httpx.Response(200, text="not json " + EMAIL),
        httpx.Response(200, json={"ok": True}),
    ],
    ids=["http-500", "slack-error", "non-json", "no-user"],
)
def test_gateway_errors_refuse_as_lookup_errors_not_unknown(monkeypatch, answer):
    async def scenario(endpoint, _calls):
        async with GatewayUserEmails.from_settings(_settings(monkeypatch, endpoint)) as emails:
            with pytest.raises(UserEmailLookupError) as error:
                await emails.email_of("U0EXAMPLE1", "T0EXAMPLE1")
            assert not isinstance(error.value, UserEmailUnknown)
            assert EMAIL not in str(error.value)

    asyncio.run(_run(lambda _request: answer, scenario))


def test_network_failure_refuses(monkeypatch):
    def slack(request):
        raise httpx.ConnectError("unreachable", request=request)

    async def scenario(endpoint, _calls):
        async with GatewayUserEmails.from_settings(_settings(monkeypatch, endpoint)) as emails:
            with pytest.raises(UserEmailLookupError):
                await emails.email_of("U0EXAMPLE1", "T0EXAMPLE1")

    asyncio.run(_run(slack, scenario))


def test_slow_slack_times_out_at_the_gateway(monkeypatch):
    async def slack(_request):
        await asyncio.Event().wait()  # never answers; the bound must fire
        return _ok(_user())

    async def scenario(endpoint, _calls):
        url = endpoint + "/v1/gateway/default/user_email"
        body = b'{"user_id": "U0EXAMPLE1", "workspace": "T0EXAMPLE1"}'
        async with httpx.AsyncClient() as http:
            raw = await http.post(
                url,
                content=body,
                headers=request_headers(TOKENS["identity"], "POST", url, body),
            )
        assert raw.status_code == 504
        async with GatewayUserEmails.from_settings(_settings(monkeypatch, endpoint)) as emails:
            with pytest.raises(UserEmailLookupError):
                await emails.email_of("U0EXAMPLE1", "T0EXAMPLE1")

    asyncio.run(_run(slack, scenario, gateway_timeout_s=0.1))


def test_client_timeout_refuses(monkeypatch):
    class Slow:
        async def email_of(self, *, identity, user_id, workspace):
            await asyncio.Event().wait()  # never answers; the bound must fire
            return EMAIL

    async def scenario(endpoint, _calls):
        settings = _settings(
            monkeypatch, endpoint, CURIE_CHANNEL_GATEWAY_USER_EMAIL_TIMEOUT_S="0.2"
        )
        async with GatewayUserEmails.from_settings(settings) as emails:
            with pytest.raises(UserEmailLookupError):
                await asyncio.wait_for(emails.email_of("U0EXAMPLE1", "T0EXAMPLE1"), 3)

    asyncio.run(_run(None, scenario, providers=GatewayProviders(emails=Slow())))


def test_unknown_gateway_identity_refuses(monkeypatch):
    async def scenario(endpoint, calls):
        settings = _settings(monkeypatch, endpoint, CURIE_CHANNEL_GATEWAY_SLACK_IDENTITY="missing")
        async with GatewayUserEmails.from_settings(settings) as emails:
            with pytest.raises(UserEmailLookupError) as error:
                await emails.email_of("U0EXAMPLE1", "T0EXAMPLE1")
            assert not isinstance(error.value, UserEmailUnknown)
        assert calls == []

    asyncio.run(_run(lambda _request: _ok(_user()), scenario))


def test_only_the_identity_caller_reaches_user_email(monkeypatch):
    async def scenario(endpoint, calls):
        url = endpoint + "/v1/gateway/default/user_email"
        body = b'{"user_id": "U0EXAMPLE1", "workspace": "T0EXAMPLE1"}'
        members = endpoint + "/v1/gateway/default/members"
        group = b'{"group_id": "S0EXAMPLE1"}'
        async with httpx.AsyncClient() as http:
            for token in ("", "wrong", TOKENS["api"], TOKENS["worker"]):
                response = await http.post(
                    url,
                    content=body,
                    headers=request_headers(token, "POST", url, body) if token else {},
                )
                assert response.status_code in (401, 403)
                assert EMAIL not in response.text
            response = await http.post(
                members,
                content=group,
                headers=request_headers(TOKENS["identity"], "POST", members, group),
            )
            assert response.status_code == 403
        assert calls == []

    asyncio.run(_run(lambda _request: _ok(_user()), scenario))


def test_without_an_identity_credential_no_caller_reaches_user_email():
    async def scenario(endpoint, calls):
        url = endpoint + "/v1/gateway/default/user_email"
        body = b'{"user_id": "U0EXAMPLE1", "workspace": "T0EXAMPLE1"}'
        async with httpx.AsyncClient() as http:
            for token in (TOKENS["identity"], TOKENS["api"], TOKENS["worker"]):
                response = await http.post(
                    url, content=body, headers=request_headers(token, "POST", url, body)
                )
                assert response.status_code in (401, 403)
        assert calls == []

    tokens = {"api": TOKENS["api"], "worker": TOKENS["worker"]}
    asyncio.run(_run(lambda _request: _ok(_user()), scenario, tokens=tokens))


@pytest.mark.parametrize(
    "tokens",
    [
        {**TOKENS, "identity": TOKENS["api"]},
        {**TOKENS, "identity": " "},
        {**TOKENS, "other": "other-credential"},
    ],
)
def test_gateway_requires_a_distinct_identity_credential(tokens):
    with pytest.raises(ValueError):
        gateway_application(
            replay=RequestReplayStore(":memory:"), tokens=tokens, allow_insecure_http=True
        )


def test_malformed_requests_refuse_without_echo(monkeypatch):
    async def scenario(endpoint, calls):
        url = endpoint + "/v1/gateway/default/user_email"
        async with httpx.AsyncClient() as http:
            for body in (
                b'{"user_id": "U0EXAMPLE1"}',
                b'{"user_id": "U0EXAMPLE1", "workspace": "T0EXAMPLE1", "group_id": "S1"}',
                b'{"user_id": "", "workspace": "T0EXAMPLE1"}',
                b"not json U0EXAMPLE1",
            ):
                response = await http.post(
                    url,
                    content=body,
                    headers=request_headers(TOKENS["identity"], "POST", url, body),
                )
                assert response.status_code == 400
                assert "U0EXAMPLE1" not in response.text
        assert calls == []

    asyncio.run(_run(lambda _request: _ok(_user()), scenario))


def test_gateway_process_adds_the_identity_caller_only_when_configured(monkeypatch):
    from curie_channel_gateway.slack.read_server import GatewayConfig, gateway_callers

    monkeypatch.setenv("CURIE_CHANNEL_GATEWAY_API_TOKEN", TOKENS["api"])
    monkeypatch.setenv("CURIE_CHANNEL_GATEWAY_WORKER_TOKEN", TOKENS["worker"])
    assert set(gateway_callers(GatewayConfig())) == {"api", "worker"}
    monkeypatch.setenv("CURIE_CHANNEL_GATEWAY_IDENTITY_TOKEN", TOKENS["identity"])
    monkeypatch.setenv("CURIE_CHANNEL_GATEWAY_USER_EMAIL_TIMEOUT_S", "2.5")
    config = GatewayConfig()
    assert gateway_callers(config) == TOKENS
    assert config.user_email_timeout_s == 2.5
    assert TOKENS["identity"] not in repr(config)


def test_missing_email_scope_refuses_as_an_outage_not_unknown(monkeypatch):
    def slack(_request):
        return httpx.Response(
            200,
            json={"ok": True, "user": _user(profile={})},
            headers={"X-OAuth-Scopes": "chat:write,users:read"},
        )

    async def scenario(endpoint, _calls):
        async with GatewayUserEmails.from_settings(_settings(monkeypatch, endpoint)) as emails:
            with pytest.raises(UserEmailLookupError) as error:
                await emails.email_of("U0EXAMPLE1", "T0EXAMPLE1")
            assert not isinstance(error.value, UserEmailUnknown)

    asyncio.run(_run(slack, scenario))


def test_a_404_without_the_gateway_code_is_unavailable_not_unknown(monkeypatch):
    from aiohttp import web

    async def not_found(_request):
        return web.json_response({"error": "no route"}, status=404)

    async def scenario():
        app = web.Application()
        app.router.add_post("/v1/gateway/default/user_email", not_found)
        async with TestServer(app) as server:
            endpoint = str(server.make_url("")).rstrip("/")
            async with GatewayUserEmails.from_settings(_settings(monkeypatch, endpoint)) as emails:
                with pytest.raises(UserEmailLookupError) as error:
                    await emails.email_of("U0EXAMPLE1", "T0EXAMPLE1")
                assert not isinstance(error.value, UserEmailUnknown)

    asyncio.run(scenario())


def test_client_default_timeout_outlasts_the_gateway_bound(monkeypatch):
    settings = _settings(monkeypatch, "https://gateway.example.test")
    from curie_channel_gateway.slack.read_server import GatewayConfig

    assert settings.timeout_s > GatewayConfig().user_email_timeout_s


def test_identity_caller_cannot_fetch_files_or_reply(monkeypatch):
    async def scenario(endpoint, calls):
        fetch = endpoint + "/v1/gateway/default/fetch/F0EXAMPLE1"
        reply = endpoint + "/v1/gateway/default/reply"
        async with httpx.AsyncClient() as http:
            response = await http.get(
                fetch, headers=request_headers(TOKENS["identity"], "GET", fetch)
            )
            assert response.status_code == 403
            response = await http.post(
                reply,
                content=b"{}",
                headers=request_headers(TOKENS["identity"], "POST", reply, b"{}"),
            )
            assert response.status_code == 403
        assert calls == []

    asyncio.run(_run(lambda _request: _ok(_user()), scenario))
