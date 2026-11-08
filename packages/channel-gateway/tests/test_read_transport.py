"""Authenticated reads traverse the gateway and its real provider ports."""

import asyncio
import json

import httpx
import pytest
from aiohttp.test_utils import TestServer
from curie_api.config import Settings
from curie_api.slack_approvers import SlackUserGroupMembers
from curie_channel_gateway.group_models import UserGroupLookupError
from curie_channel_gateway.read_transport import GatewayClient, gateway_application
from curie_channel_gateway.request_replay import RequestReplayStore, request_headers
from curie_channel_gateway.slack.groups import SlackUserGroupClient
from curie_worker.config import WorkerConfig


@pytest.mark.parametrize("settings", [Settings, WorkerConfig])
@pytest.mark.parametrize("variable", ["SLACK_BOT_TOKEN", "SLACK_APP_TOKEN", "SLACK_USER_TOKEN"])
def test_application_boot_refuses_slack_tokens(monkeypatch, settings, variable):
    sentinel = "private-token-sentinel"
    monkeypatch.setenv(variable, sentinel)
    with pytest.raises(ValueError) as error:
        settings(_env_file=None)
    assert sentinel not in str(error.value)


def test_group_transport_authentication_freshness_and_narrowing():
    async def scenario():
        users = ["U0EXAMPLE1", "U0EXAMPLE2"]
        calls = []

        def slack(request):
            calls.append(request)
            return httpx.Response(200, json={"ok": True, "users": list(users)})

        async with httpx.AsyncClient(transport=httpx.MockTransport(slack)) as provider:
            groups = SlackUserGroupClient(provider, token="provider-credential")
            app = gateway_application(
                replay=RequestReplayStore(":memory:"),
                tokens={"api": "api-credential", "worker": "worker-credential"},
                groups={"default": groups},
                allow_insecure_http=True,
            )
            async with TestServer(app) as server, httpx.AsyncClient() as http:
                endpoint = str(server.make_url(""))
                client = GatewayClient(http, endpoint, "api-credential", allow_insecure_http=True)
                source = client.groups("default")
                first = await source.members("S0EXAMPLE1")
                assert first.users == frozenset(users)
                users.pop()
                assert (await source.members("S0EXAMPLE1")).users == first.users
                assert (await source.fresh_members("S0EXAMPLE1")).users == frozenset(users)
                verdict = await SlackUserGroupMembers("S0EXAMPLE1", source).contains(
                    "U0EXAMPLE9", None
                )
                assert verdict.member is False
                assert len(calls) == 2
                for token in ("", "wrong", "worker-credential"):
                    response = await http.post(
                        endpoint + "/v1/gateway/default/members",
                        headers={"X-Curie-Adapter-Secret": token},
                        json={"group_id": "S0EXAMPLE1"},
                    )
                    assert response.status_code in (401, 403)
                    assert "S0EXAMPLE1" not in response.text
                assert len(calls) == 2
            async with httpx.AsyncClient() as unavailable:
                source = GatewayClient(
                    unavailable, endpoint, "api-credential", allow_insecure_http=True
                ).groups("default")
                verdict = await SlackUserGroupMembers("S0EXAMPLE1", source).contains(
                    "U0EXAMPLE1", None
                )
                assert not verdict.member and verdict.undetermined
                with pytest.raises(UserGroupLookupError):
                    await source.fresh_members("S0EXAMPLE1")

    asyncio.run(scenario())


def test_gateway_requires_tls_and_separate_credentials():
    with pytest.raises(ValueError):
        GatewayClient(httpx.AsyncClient(), "http://example.com", "credential")
    with pytest.raises(ValueError):
        gateway_application(
            replay=RequestReplayStore(":memory:"), tokens={"api": "shared", "worker": "shared"}
        )


def test_gateway_rejects_invalid_request_without_echoing_input():
    async def scenario():
        app = gateway_application(
            replay=RequestReplayStore(":memory:"),
            tokens={"api": "api", "worker": "worker"},
            allow_insecure_http=True,
        )
        async with TestServer(app) as server, httpx.AsyncClient() as http:
            sentinel = "input-must-not-be-echoed"
            response = await http.post(
                str(server.make_url("/v1/gateway/default/members")),
                headers=request_headers(
                    "api",
                    "POST",
                    str(server.make_url("/v1/gateway/default/members")),
                    json.dumps({sentinel: sentinel}).encode(),
                ),
                content=json.dumps({sentinel: sentinel}),
            )
            assert response.status_code == 400
            assert sentinel not in response.text

    asyncio.run(scenario())


def test_file_transport_caps_bytes_and_rejects_the_wrong_caller():
    from curie_channel_gateway.read_transport import RemoteFileSource
    from curie_channel_gateway.slack.files import FileResponse, SlackFileClient, SlackFileError

    calls = []
    chunks_read = []
    payload = [b"content"]

    def provider(**request):
        calls.append(request)
        if "/files.info?" in request["url"]:
            return FileResponse(
                200,
                {},
                iter(
                    [
                        json.dumps(
                            {
                                "ok": True,
                                "file": {
                                    "url_private_download": "https://files.slack.com/files-pri/T0EXAMPLE1-F0EXAMPLE1/report.txt"
                                },
                            }
                        ).encode()
                    ]
                ),
            )

        def chunks():
            for chunk in payload:
                chunks_read.append(chunk)
                yield chunk

        return FileResponse(200, {"content-type": "text/plain"}, chunks())

    async def scenario():
        app = gateway_application(
            replay=RequestReplayStore(":memory:"),
            tokens={"api": "api-credential", "worker": "worker-credential"},
            files={"default": SlackFileClient(token="provider-only-token", transport=provider)},
            allow_insecure_http=True,
            max_file_bytes=16,
        )
        async with TestServer(app) as server, httpx.AsyncClient() as http:
            endpoint = str(server.make_url(""))
            for token in ("", "wrong", "api-credential"):
                response = await http.get(
                    endpoint + "/v1/gateway/default/fetch/F0EXAMPLE1",
                    headers={"X-Curie-Adapter-Secret": token},
                )
                assert response.status_code in (401, 403)
            assert not calls
            source = RemoteFileSource(
                endpoint, "worker-credential", "default", allow_insecure_http=True
            )
            assert (
                await asyncio.to_thread(lambda: b"".join(source.fetch("F0EXAMPLE1"))) == b"content"
            )
            assert all(r["headers"]["Authorization"] == "Bearer provider-only-token" for r in calls)
            payload[:] = [b"x" * 8] * 4
            chunks_read.clear()
            with pytest.raises(SlackFileError):
                await asyncio.to_thread(lambda: b"".join(source.fetch("F0EXAMPLE1")))
            assert len(chunks_read) == 3

    asyncio.run(scenario())


def test_sender_lookup_does_not_block_and_preserves_identity():
    from curie_channel_gateway.read_transport import GatewayProviders
    from curie_channel_gateway.slack.senders import SlackSenderIdentities

    async def scenario():
        async def provider(request):
            return httpx.Response(200, json={"ok": True, "user_id": "U0EXAMPLE1"})

        # auth.test remains an external service; gateway sender caching and HTTP are real.
        from aiohttp import web

        async def auth_test(request):
            assert request.headers["Authorization"] == "Bearer provider-only-token"
            return web.json_response({"ok": True, "user_id": "U0EXAMPLE1"})

        slack = web.Application()
        slack.router.add_post("/api/auth.test", auth_test)
        async with TestServer(slack) as provider_server:
            senders = SlackSenderIdentities(
                {"ops-bot": "provider-only-token"}, base_url=str(provider_server.make_url("/api/"))
            )
            app = gateway_application(
                replay=RequestReplayStore(":memory:"),
                tokens={"api": "api-credential", "worker": "worker-credential"},
                providers=GatewayProviders(senders=senders),
                allow_insecure_http=True,
            )
            async with TestServer(app) as server, httpx.AsyncClient() as http:
                client = GatewayClient(
                    http, str(server.make_url("")), "worker-credential", allow_insecure_http=True
                )
                try:
                    assert await client.identity_of("U0EXAMPLE1") is None
                    # Completion gates, rather than elapsed time, drive both refresh layers.
                    await asyncio.gather(*tuple(client._sender_tasks.values()))
                    if senders._refresh_task is not None:
                        await senders._refresh_task
                    assert await client.identity_of("U0EXAMPLE1") is None
                    await asyncio.gather(*tuple(client._sender_tasks.values()))
                    assert await client.identity_of("U0EXAMPLE1") == "ops-bot"
                finally:
                    await client.aclose()

    asyncio.run(scenario())


def test_https_gateway_verifies_the_server_certificate(tmp_path):
    import ssl
    import subprocess

    from aiohttp import web

    certificate = tmp_path / "server.crt"
    key = tmp_path / "server.key"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
            "-keyout",
            str(key),
            "-out",
            str(certificate),
        ],
        check=True,
        capture_output=True,
    )

    async def scenario():
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(certificate, key)
        client_tls = ssl.create_default_context(cafile=str(certificate))

        def provider(request):
            return httpx.Response(200, json={"ok": True, "users": ["U0EXAMPLE1"]})

        async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as slack:
            app = gateway_application(
                replay=RequestReplayStore(":memory:"),
                tokens={"api": "api-credential", "worker": "worker-credential"},
                groups={"default": SlackUserGroupClient(slack, token="provider-only-token")},
            )
            runner = web.AppRunner(app, access_log=None)
            await runner.setup()
            try:
                await web.TCPSite(runner, "127.0.0.1", 0, ssl_context=tls).start()
                endpoint = f"https://127.0.0.1:{runner.addresses[0][1]}"
                async with httpx.AsyncClient(verify=client_tls) as trusted:
                    client = GatewayClient(trusted, endpoint, "api-credential")
                    assert (
                        await client.groups("default").fresh_members("S0EXAMPLE1")
                    ).users == frozenset({"U0EXAMPLE1"})
                async with httpx.AsyncClient() as untrusted:
                    client = GatewayClient(untrusted, endpoint, "api-credential")
                    with pytest.raises(UserGroupLookupError):
                        await client.groups("default").fresh_members("S0EXAMPLE1")
            finally:
                await runner.cleanup()

    asyncio.run(scenario())


def test_signed_requests_refuse_replay_tampering_and_restart(tmp_path):
    calls = []
    ledger = tmp_path / "requests.sqlite3"

    def provider(request):
        calls.append(request)
        return httpx.Response(200, json={"ok": True, "users": ["U0EXAMPLE1"]})

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as slack:
            groups = {"default": SlackUserGroupClient(slack, token="provider-only-token", ttl_s=0)}
            body = b'{"group_id":"S0EXAMPLE1"}'
            app = gateway_application(
                tokens={"api": "api-credential", "worker": "worker-credential"},
                replay=RequestReplayStore(ledger),
                groups=groups,
                allow_insecure_http=True,
            )
            async with TestServer(app) as server, httpx.AsyncClient() as http:
                url = str(server.make_url("/v1/gateway/default/fresh_members"))
                headers = request_headers("api-credential", "POST", url, body)
                response = await http.post(url, content=body, headers=headers)
                assert response.status_code == 200
                response = await http.post(url, content=body, headers=headers)
                assert response.status_code == 409
                assert response.json() == {"error": "request_replay_refused"}
                changed = request_headers("api-credential", "POST", url, body)
                response = await http.post(
                    url, content=b'{"group_id":"S0OTHER001"}', headers=changed
                )
                assert response.status_code == 409
                for name, value in (
                    ("X-Curie-Best-Effort-Unreachable", "true"),
                    ("X-Curie-Provider-Endpoint", "https://example.com/api/"),
                ):
                    tampered = request_headers("api-credential", "POST", url, body)
                    tampered[name] = value
                    response = await http.post(url, content=body, headers=tampered)
                    assert response.status_code == 409
                assert len(calls) == 1
            restarted = gateway_application(
                tokens={"api": "api-credential", "worker": "worker-credential"},
                replay=RequestReplayStore(ledger),
                groups=groups,
                allow_insecure_http=True,
            )
            async with TestServer(restarted) as server, httpx.AsyncClient() as http:
                response = await http.post(
                    str(server.make_url("/v1/gateway/default/fresh_members")),
                    content=body,
                    headers=headers,
                )
                assert response.status_code == 409
                assert len(calls) == 1

    asyncio.run(scenario())


def test_slack_file_source_never_sends_credentials_to_a_metadata_selected_host():
    from curie_channel_gateway.slack.files import FileResponse, SlackFileClient, SlackFileError

    calls = []

    def provider(**request):
        calls.append(request)
        return FileResponse(
            200,
            {},
            iter(
                [
                    json.dumps(
                        {
                            "ok": True,
                            "file": {
                                "url_private_download": "https://attacker.example.com/private"
                            },
                        }
                    ).encode()
                ]
            ),
        )

    source = SlackFileClient(token="provider-only-token", transport=provider)
    with pytest.raises(SlackFileError):
        source.fetch("F0EXAMPLE1")
    assert len(calls) == 1
    assert calls[0]["url"].startswith("https://slack.com/api/files.info?")


def test_replay_ledger_refuses_stale_requests_and_recovers_capacity(monkeypatch):
    from curie_channel_gateway import request_replay
    from curie_channel_gateway.request_replay import ReplayCapacityError, ReplayRefusedError

    now = [1_800_000_000]
    monkeypatch.setattr(request_replay.time, "time", lambda: now[0])
    monkeypatch.setattr(request_replay, "MAX_REPLAY_ROWS", 1)
    replay = RequestReplayStore(":memory:")
    try:
        url = "https://gateway.example.com/v1/gateway/default/members"
        path = "/v1/gateway/default/members"
        first = request_headers("api-credential", "POST", url, b"{}")
        replay.consume("api", "api-credential", "POST", path, b"{}", first)
        second = request_headers("api-credential", "POST", url, b"{}")
        with pytest.raises(ReplayCapacityError):
            replay.consume("api", "api-credential", "POST", path, b"{}", second)
        now[0] += 601
        with pytest.raises(ReplayRefusedError):
            replay.consume("api", "api-credential", "POST", path, b"{}", first)
        fresh = request_headers("api-credential", "POST", url, b"{}")
        replay.consume("api", "api-credential", "POST", path, b"{}", fresh)
    finally:
        replay.close()


def test_reply_timeout_does_not_retry_or_accept_a_replay(tmp_path):
    from aiohttp import web
    from channel_protocol import OutboundMessage, ReplyPost, ReplyTarget
    from curie_channel_gateway.gateway_reply import GatewayReplyRoute, GatewayReplySink
    from curie_channel_gateway.reply_delivery import ReplyDeliveryStore
    from curie_channel_gateway.reply_errors import IndeterminateReplyDeliveryError
    from curie_channel_gateway.slack.slack_sink import SlackReplyAdapter

    async def scenario():
        accepted = asyncio.Event()
        release = asyncio.Event()
        completed = asyncio.Event()
        posts = []
        requests = []

        async def provider(request):
            posts.append(await request.json())
            accepted.set()
            await release.wait()
            completed.set()
            return web.json_response({"ok": True, "ts": "1800000000.000100"})

        @web.middleware
        async def record_request(request, handler):
            requests.append((request.raw_path, dict(request.headers), await request.read()))
            return await handler(request)

        slack_app = web.Application()
        slack_app.router.add_post("/api/chat.postMessage", provider)
        async with TestServer(slack_app) as slack:
            adapter = SlackReplyAdapter(
                "provider-only-token", base_url=str(slack.make_url("/api/"))
            )
            app = gateway_application(
                tokens={"api": "api-credential", "worker": "worker-credential"},
                replay=RequestReplayStore(":memory:"),
                reply=adapter,
                reply_state=ReplyDeliveryStore(tmp_path / "replies.sqlite3"),
                allow_insecure_http=True,
            )
            app.middlewares.insert(0, record_request)
            async with TestServer(app) as server:
                sink = GatewayReplySink(
                    str(server.make_url("")),
                    "worker-credential",
                    allow_insecure_http=True,
                    timeout_s=0.2,
                )
                event = ReplyPost(
                    version="1.0",
                    event="reply.post",
                    target=ReplyTarget(
                        kind="slack",
                        address="C0EXAMPLE1",
                        conversation_id="1800000000.000001",
                        reply_ref="1800000000.000001",
                    ),
                    message=OutboundMessage(version="1.0", text="one post"),
                    requested_by="U0EXAMPLE1",
                )
                try:
                    with pytest.raises(IndeterminateReplyDeliveryError):
                        await sink.emit(event, route=GatewayReplyRoute())
                    assert accepted.is_set() and len(posts) == 1
                    release.set()
                    await asyncio.wait_for(completed.wait(), 2)
                    path, headers, body = requests[0]
                    async with httpx.AsyncClient() as http:
                        response = await http.post(
                            str(server.make_url(path)), headers=headers, content=body
                        )
                        assert response.status_code == 409
                    assert len(posts) == 1
                    ack = await sink.emit(event, route=GatewayReplyRoute())
                    assert ack.ref == "1800000000.000100"
                    assert len(posts) == 1
                finally:
                    release.set()
                    await sink.aclose()

    asyncio.run(scenario())


def test_gateway_reply_preserves_an_opaque_provider_identity(tmp_path):
    from aiohttp import web
    from channel_protocol import OutboundMessage, ReplyPost, ReplyTarget
    from curie_channel_gateway.gateway_reply import GatewayReplyRoute, GatewayReplySink
    from curie_channel_gateway.reply_delivery import ReplyDeliveryStore
    from curie_channel_gateway.slack.slack_sink import SlackReplyAdapter

    requests = []

    async def scenario():
        async def provider(request):
            assert request.headers["Authorization"] == "Bearer provider-only-token"
            requests.append(await request.json())
            return web.json_response({"ok": True, "ts": "1800000000.000100"})

        slack_app = web.Application()
        slack_app.router.add_post("/api/chat.postMessage", provider)
        async with TestServer(slack_app) as slack:
            adapter = SlackReplyAdapter(
                "",
                identity_tokens={"adapter/example": "provider-only-token"},
                base_url=str(slack.make_url("/api/")),
            )
            app = gateway_application(
                tokens={"api": "api-credential", "worker": "worker-credential"},
                replay=RequestReplayStore(":memory:"),
                reply=adapter,
                reply_state=ReplyDeliveryStore(tmp_path / "replies.sqlite3"),
                allow_insecure_http=True,
            )
            async with TestServer(app) as server:
                sink = GatewayReplySink(
                    str(server.make_url("")),
                    "worker-credential",
                    identities=("adapter/example",),
                    allow_insecure_http=True,
                )
                try:
                    route = GatewayReplyRoute(adapter="adapter/example")
                    assert sink.undeliverable_reason("slack", route) is None
                    await sink.emit(
                        ReplyPost(
                            version="1.0",
                            event="reply.post",
                            target=ReplyTarget(
                                kind="slack",
                                address="C0EXAMPLE1",
                                conversation_id="1800000000.000001",
                                reply_ref="1800000000.000001",
                            ),
                            message=OutboundMessage(version="1.0", text="opaque identity"),
                            requested_by="U0EXAMPLE1",
                        ),
                        route=route,
                    )
                finally:
                    await sink.aclose()

    asyncio.run(scenario())
    assert len(requests) == 1
    assert requests[0]["channel"] == "C0EXAMPLE1"


@pytest.mark.parametrize("max_age_s", [0.0, 60.0])
def test_api_freshness_policy_rejects_revoked_cached_members(max_age_s):
    from curie_api.models import Approval
    from curie_api.slack_approvers import build_approver_set_selector

    members = ["U0EXAMPLE1"]
    calls = []
    now = [0.0]

    def provider(request):
        calls.append(request)
        return httpx.Response(200, json={"ok": True, "users": list(members)})

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as slack:
            source = SlackUserGroupClient(
                slack, token="provider-only-token", ttl_s=3600, mono=lambda: now[0]
            )
            app = gateway_application(
                tokens={"api": "api-credential", "worker": "worker-credential"},
                replay=RequestReplayStore(":memory:"),
                groups={"default": source},
                allow_insecure_http=True,
            )
            async with TestServer(app) as server, httpx.AsyncClient() as http:
                settings = Settings(
                    CURIE_CHANNEL_GATEWAY_URL=str(server.make_url("")),
                    CURIE_CHANNEL_GATEWAY_TOKEN="api-credential",
                    CURIE_CHANNEL_GATEWAY_ALLOW_INSECURE=True,
                    slack_usergroup_cache_ttl_s=max_age_s,
                )
                approval = Approval(
                    reply_kind="slack",
                    reply_channel="C0EXAMPLE1",
                    card_channel="C0EXAMPLE1",
                    route="managers",
                )
                binding = {"channel": "C0EXAMPLE1", "approvers": {"group": "S0EXAMPLE1"}}
                approvers = build_approver_set_selector(http, settings)(approval, binding)
                assert (await approvers.contains("U0EXAMPLE1", None)).member
                members.clear()
                now[0] = 121.0
                verdict = await approvers.contains("U0EXAMPLE1", None)
                assert not verdict.member and not verdict.undetermined
                assert len(calls) == 2
                await server.close()
                verdict = await approvers.contains("U0EXAMPLE1", None)
                assert not verdict.member and verdict.undetermined

    asyncio.run(scenario())
