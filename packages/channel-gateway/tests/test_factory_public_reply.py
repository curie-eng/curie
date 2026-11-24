"""Exercise Slack's public-channel metadata at the signed gateway boundary."""

import asyncio

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from channel_protocol import MESSAGE_VERSION, OutboundMessage
from channel_protocol.reply import PROGRESS_REPLY_WIRE_VERSION, ReplyPost, ReplyTarget
from curie_channel_gateway.gateway_reply import GatewayReplyRoute, GatewayReplySink
from curie_channel_gateway.read_transport import gateway_application
from curie_channel_gateway.reply_delivery import ReplyDeliveryStore
from curie_channel_gateway.reply_errors import PublicChannelUnverifiedError
from curie_channel_gateway.request_replay import RequestReplayStore
from curie_channel_gateway.slack.slack_sink import SlackReplyAdapter


@pytest.mark.parametrize(
    "changed,allowed",
    [
        ({}, True),
        ({"is_shared": True, "is_org_shared": True}, False),
        ({"is_org_shared": True}, False),
        ({"is_org_shared": None}, False),
        ({"is_pending_ext_shared": True}, False),
        ({"is_private": True}, False),
        ({"is_shared": True}, False),
        ({"is_ext_shared": True}, False),
        ({"is_shared": True, "is_org_shared": True, "is_ext_shared": True}, False),
        ({"is_org_shared": True, "is_ext_shared": None}, False),
        ({"is_group": True}, False),
        ({"is_archived": True}, False),
        ({"is_channel": None}, False),
        ({"context_team_id": "T0EXAMPLE2"}, False),
        ({"id": "C0EXAMPLE2"}, False),
        ({"lookup_error": True}, False),
        ({"fallback_private": True}, False),
    ],
)
def test_signed_public_post_checks_speaking_workspace(tmp_path, changed, allowed):
    async def scenario():
        writes = []
        inspected = []
        # https://docs.slack.dev/reference/objects/conversation-object/
        facts = {
            "id": "C0EXAMPLE1",
            "is_channel": True,
            "context_team_id": "T0EXAMPLE1",
            **dict.fromkeys(
                (
                    "is_private",
                    "is_group",
                    "is_im",
                    "is_mpim",
                    "is_archived",
                    "is_shared",
                    "is_ext_shared",
                    "is_org_shared",
                    "is_pending_ext_shared",
                ),
                False,
            ),
            **changed,
        }

        async def slack(request):
            operation = request.path.rsplit("/", 1)[-1]
            if operation == "auth.test":
                return web.json_response({"ok": True, "team_id": "T0EXAMPLE1"})
            if operation == "conversations.info":
                if facts.get("lookup_error"):
                    return web.json_response({"ok": False, "error": "missing_scope"})
                inspected.append(request.match_info["site"])
                private = facts.get("fallback_private") and request.match_info["site"] == "api"
                return web.json_response(
                    {
                        "ok": True,
                        "channel": {
                            **facts,
                            "is_private": True if private else facts["is_private"],
                        },
                    }
                )
            if request.match_info["site"] == "alternate":
                request.transport.abort()
                return web.Response(status=503)
            writes.append(operation)
            return web.json_response({"ok": True, "ts": "1700000000.000001"})

        provider = web.Application()
        provider.router.add_post("/{site}/{operation}", slack)
        provider.router.add_get("/{site}/conversations.info", slack)
        async with TestServer(provider) as slack_server:
            adapter = SlackReplyAdapter("test-token", base_url=str(slack_server.make_url("/api/")))
            app = gateway_application(
                tokens={"api": "api-key", "worker": "worker-key"},
                replay=RequestReplayStore(tmp_path / "requests.db"),
                reply=adapter,
                reply_state=ReplyDeliveryStore(tmp_path / "replies.db"),
                allow_insecure_http=True,
            )
            event = ReplyPost(
                event="reply.post",
                version=PROGRESS_REPLY_WIRE_VERSION,
                target=ReplyTarget(
                    kind="slack", address="C0EXAMPLE1", conversation_id=None, reply_ref=None
                ),
                message=OutboundMessage(version=MESSAGE_VERSION, text="Factory run started."),
                requested_by="",
                delivery_id="00000000-0000-4000-8000-000000000001",
            )
            async with TestServer(app) as gateway:
                sink = GatewayReplySink(
                    str(gateway.make_url("/")), "worker-key", allow_insecure_http=True
                )
                route = GatewayReplyRoute(
                    adapter="default",
                    factory_public=True,
                    endpoint=str(slack_server.make_url("/alternate/"))
                    if changed.get("fallback_private")
                    else None,
                )
                try:
                    if not allowed:
                        with pytest.raises(PublicChannelUnverifiedError):
                            await sink.emit(event, route=route)
                        assert writes == []
                        if changed.get("fallback_private"):
                            assert inspected == ["alternate", "api"]
                    else:
                        assert (await sink.emit(event, route=route)).ref == "1700000000.000001"
                        await sink.emit(event, route=route)
                        assert writes == ["chat.postMessage"]
                finally:
                    await sink.aclose()

    asyncio.run(scenario())
