"""The standalone gateway and its clients authenticate over verified TLS."""

import asyncio
import os
import ssl
import subprocess
import sys
import time
from contextlib import nullcontext

import httpx
import pytest
from curie_channel_gateway.read_transport import GatewayAuthenticationError, GatewayClient
from curie_test_support.static_shell import free_port, make_shell_tls, wait_until
from pydantic import TypeAdapter

API_TOKEN = "test-gateway-api-credential"
WORKER_TOKEN = "test-gateway-worker-credential"


@pytest.mark.parametrize("address", ["U0EXAMPLE1", "D0EXAMPLE1"])
def test_gateway_reply_preserves_the_approver_dm_channel(tmp_path, address):
    from aiohttp import web
    from aiohttp.test_utils import TestServer
    from channel_protocol import Action, ConfirmIntent, OutboundMessage, ReplyPost, ReplyTarget
    from curie_channel_gateway.gateway_reply import GatewayReplyRoute, GatewayReplySink
    from curie_channel_gateway.read_transport import GatewayAuthenticationError
    from curie_channel_gateway.slack.read_server import GatewayConfig, application
    from curie_channel_gateway.slack.tokens import SlackProviderConfig

    async def scenario():
        posts = []

        async def provider(request):
            posts.append(await request.json())
            # https://docs.slack.dev/reference/methods/chat.postMessage/ returns channel and ts.
            return web.json_response({"ok": True, "channel": "D0EXAMPLE1", "ts": "1700.000200"})

        slack = web.Application()
        slack.router.add_post("/chat.postMessage", provider)
        async with TestServer(slack) as external:
            config = GatewayConfig(
                CURIE_CHANNEL_GATEWAY_API_TOKEN=API_TOKEN,
                CURIE_CHANNEL_GATEWAY_WORKER_TOKEN=WORKER_TOKEN,
                CURIE_CHANNEL_GATEWAY_ALLOW_INSECURE=True,
                CURIE_CHANNEL_GATEWAY_REPLAY_DB=str(tmp_path / "dm.sqlite3"),
                slack_api_base_url=str(external.make_url("/")),
            )
            app = await application(config, SlackProviderConfig(slack_bot_token="xoxb-test"))
            async with TestServer(app) as gateway:
                event = ReplyPost(
                    version="1.0",
                    event="reply.post",
                    target=ReplyTarget(
                        kind="slack", address=address, conversation_id=None, reply_ref=None
                    ),
                    message=OutboundMessage(
                        version="1.0",
                        text="Approve the requested operation",
                        interaction=ConfirmIntent(
                            kind="confirm",
                            id="11111111-1111-4111-8111-111111111111",
                            prompt="Approve the requested operation",
                            confirm=Action(label="Approve", value="approve"),
                            cancel=Action(label="Reject", value="reject"),
                        ),
                    ),
                    requested_by="example requester",
                )
                route = GatewayReplyRoute()
                denied = GatewayReplySink(
                    str(gateway.make_url("/")), "wrong-token", allow_insecure_http=True
                )
                try:
                    with pytest.raises(GatewayAuthenticationError):
                        await denied.emit(event, route=route)
                    assert posts == []
                finally:
                    await denied.aclose()
                sink = GatewayReplySink(
                    str(gateway.make_url("/")), WORKER_TOKEN, allow_insecure_http=True
                )
                try:
                    for _ in range(2):
                        ack = await sink.emit(event, route=route)
                        assert ack.ref == "1700.000200"
                        assert ack.channel == "D0EXAMPLE1"
                    assert len(posts) == 1
                    assert posts[0]["channel"] == address
                    assert "thread_ts" not in posts[0]
                    assert posts[0]["blocks"]
                finally:
                    await sink.aclose()

    asyncio.run(scenario())


@pytest.fixture(scope="module", params=(False, True))
def tls_gateway(tmp_path_factory, request):
    directory = tmp_path_factory.mktemp("gateway-tls")
    tls = make_shell_tls(directory)
    port = free_port()
    endpoint = f"https://127.0.0.1:{port}"
    context = ssl.create_default_context(cafile=str(tls.ca))
    env = {
        **os.environ,
        "CURIE_CHANNEL_GATEWAY_HOST": "127.0.0.1",
        "CURIE_CHANNEL_GATEWAY_PORT": str(port),
        "CURIE_CHANNEL_GATEWAY_API_TOKEN": API_TOKEN,
        "CURIE_CHANNEL_GATEWAY_WORKER_TOKEN": WORKER_TOKEN,
        "CURIE_CHANNEL_GATEWAY_TLS_CERT": str(tls.cert),
        "CURIE_CHANNEL_GATEWAY_TLS_KEY": str(tls.key),
        "CURIE_CHANNEL_GATEWAY_ALLOW_INSECURE": str(request.param).lower(),
        "CURIE_CHANNEL_GATEWAY_REPLAY_DB": str(directory / "requests.sqlite3"),
        "CURIE_SLACK_IDENTITIES": "[]",
        "SLACK_BOT_TOKEN": "",
    }
    process = subprocess.Popen(
        [sys.executable, "-m", "curie_channel_gateway.slack.read_server"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        with httpx.Client(verify=context, timeout=0.5) as client:

            def ready():
                assert process.poll() is None, "gateway exited before TLS became ready"
                try:
                    return client.get(endpoint).status_code == 401
                except httpx.ConnectError:
                    return False

            wait_until(ready, 10, "gateway TLS listener did not become ready")
        yield endpoint, context, tls.ca
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
                pytest.fail("gateway did not shut down after termination")
        assert process.poll() is not None, "gateway process survived cleanup"


def test_tls_client_checks_certificate_and_caller_authority(tls_gateway):
    endpoint, context, _ca = tls_gateway

    async def scenario():
        async with httpx.AsyncClient() as untrusted:
            with pytest.raises(httpx.ConnectError):
                await untrusted.get(endpoint)
        async with httpx.AsyncClient(verify=context) as http:
            client = GatewayClient(http, endpoint, WORKER_TOKEN)
            assert (
                await client.call(
                    "default", "identity_of", {"author": "U0EXAMPLE1"}, TypeAdapter(str | None)
                )
                is None
            )
            for token in ("wrong-caller-credential", API_TOKEN):
                refused = GatewayClient(http, endpoint, token)
                with pytest.raises(GatewayAuthenticationError):
                    await refused.call(
                        "default", "identity_of", {"author": "U0EXAMPLE1"}, TypeAdapter(str | None)
                    )

    asyncio.run(scenario())


@pytest.mark.parametrize("scoped", [False, True])
def test_mounted_gateway_ca_reaches_reads_replies_and_file_transport(
    tls_gateway, monkeypatch, scoped
):
    from channel_protocol import ReplyTarget, ReplyUpdate
    from curie_channel_gateway.gateway_reply import GatewayReplyRoute, GatewayReplySink
    from curie_channel_gateway.reply_errors import UnconfiguredSlackIdentityError
    from curie_channel_gateway.request_replay import request_headers
    from curie_channel_gateway.tls import gateway_http_mounts
    from curie_worker.attachments import AttachmentReadScope, _file_transport

    endpoint, _context, ca = tls_gateway
    monkeypatch.setenv("CURIE_CHANNEL_GATEWAY_CA_FILE", str(ca))
    monkeypatch.setenv("CURIE_CHANNEL_GATEWAY_URL", endpoint)

    async def scenario():
        async with httpx.AsyncClient(mounts=gateway_http_mounts(endpoint)) as http:
            client = GatewayClient(http, endpoint, WORKER_TOKEN)
            assert (
                await client.call(
                    "default", "identity_of", {"author": "U0EXAMPLE1"}, TypeAdapter(str | None)
                )
                is None
            )
        sink = GatewayReplySink(endpoint, WORKER_TOKEN)
        try:
            with pytest.raises(UnconfiguredSlackIdentityError):
                await sink.emit(
                    ReplyUpdate(
                        version="1.0",
                        event="reply.update",
                        text="upgrade proof",
                        target=ReplyTarget(
                            kind="slack",
                            address="C0EXAMPLE1",
                            conversation_id="1.000",
                            reply_ref="2.000",
                        ),
                    ),
                    route=GatewayReplyRoute(adapter="unknown"),
                )
        finally:
            await sink.aclose()

    asyncio.run(scenario())
    url = endpoint + "/v1/gateway/default/fetch/F0EXAMPLE1"
    owner = AttachmentReadScope(deadline_epoch=time.time() + 10) if scoped else nullcontext()
    with owner:
        response = _file_transport(
            method="GET",
            url=url,
            headers=request_headers(WORKER_TOKEN, "GET", url),
            chunk_bytes=1024,
        )
        assert response.status == 502
        list(response.chunks)


def test_optional_gateway_ca_uses_system_trust_and_rejects_malformed_ca(monkeypatch, tmp_path):
    from curie_channel_gateway.tls import gateway_http_mounts

    monkeypatch.setenv("CURIE_CHANNEL_GATEWAY_CA_FILE", str(tmp_path / "absent.pem"))
    assert gateway_http_mounts("https://gateway.example.com")
    invalid = tmp_path / "invalid.pem"
    invalid.write_text("not a certificate")
    monkeypatch.setenv("CURIE_CHANNEL_GATEWAY_CA_FILE", str(invalid))
    with pytest.raises(ssl.SSLError):
        gateway_http_mounts("https://gateway.example.com")


def test_undo_recovery_http_client_trusts_gateway_ca(tls_gateway, monkeypatch):
    from curie_api.config import Settings
    from curie_api.remediation_undo_recovery import recovery_http_client

    endpoint, _context, ca = tls_gateway
    monkeypatch.setenv("CURIE_CHANNEL_GATEWAY_CA_FILE", str(ca))
    settings = Settings(CURIE_CHANNEL_GATEWAY_URL=endpoint)

    async def scenario():
        async with recovery_http_client(settings) as client:
            # A real TLS handshake reaches the gateway's authentication boundary.
            assert (await client.get(endpoint)).status_code == 401

    asyncio.run(scenario())
