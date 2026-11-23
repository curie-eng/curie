"""Slack voice reads and draft posts traverse the authenticated gateway."""

import hashlib
import html
import json
from datetime import UTC, datetime
from uuid import UUID

import httpx
import pytest
from aiohttp.test_utils import TestServer
from curie_channel_gateway.read_transport import (
    GatewayClient,
    GatewayProviders,
    SlackVoiceReadError,
    gateway_application,
    post_voice_draft,
    read_voice_clip,
)
from curie_channel_gateway.request_replay import RequestReplayStore
from curie_channel_gateway.slack.files import FileResponse
from curie_channel_gateway.slack.voice import SlackVoiceGateway

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


TEAM = "T0EXAMPLE1"
APP = "A0EXAMPLE1"
BOT = "B0EXAMPLE1"
BOT_USER = "U0EXAMPLE9"
AUTHOR = "U0EXAMPLE1"
CHANNEL = "C0EXAMPLE1"
ROOT = "1700000000.000001"
MESSAGE = "1700000000.000002"
FILE = "F0EXAMPLE1"
TEXT = f"  <@{BOT_USER}>  "
AUDIO = b"example-audio"
TARGET = dict(identity="support", team_id=TEAM, app_id=APP, channel=CHANNEL, root_ts=ROOT)
SOURCE = dict(
    **TARGET,
    message_ts=MESSAGE,
    author=AUTHOR,
    file_id=FILE,
    content_sha256=hashlib.sha256(TEXT.encode()).hexdigest(),
)
CLIP = dict(
    id=FILE,
    user=AUTHOR,
    mode="hosted",
    is_external=False,
    mimetype="audio/mp4",
    subtype="slack_audio",
    size=len(AUDIO),
)


class Slack:
    def __init__(self):
        self.message = dict(
            type="message",
            subtype="file_share",
            thread_ts=ROOT,
            ts=MESSAGE,
            user=AUTHOR,
            text=TEXT,
            files=[dict(CLIP)],
        )
        self.team = TEAM
        self.app = APP
        self.info = dict(CLIP)
        self.calls = []
        self.posts = []
        self.read_error = None

    def web_api(self, request):
        # https://docs.slack.dev/reference/methods/auth.test/
        # https://docs.slack.dev/reference/methods/bots.info/
        # https://docs.slack.dev/reference/methods/conversations.replies/
        method = request.url.path.rsplit("/", 1)[1]
        assert request.headers["Authorization"] == "Bearer example-support"
        self.calls.append(method)
        if method == "auth.test":
            return httpx.Response(
                200, json=dict(ok=True, team_id=self.team, bot_id=BOT, user_id=BOT_USER)
            )
        if method == "bots.info":
            assert request.url.params["bot"] == BOT
            return httpx.Response(200, json=dict(ok=True, bot=dict(id=BOT, app_id=self.app)))
        if method == "conversations.replies":
            assert dict(request.url.params) == dict(
                channel=CHANNEL,
                ts=ROOT,
                oldest=MESSAGE,
                latest=MESSAGE,
                inclusive="true",
                limit="2",
            )
            if self.read_error:
                return httpx.Response(200, json=dict(ok=False, error=self.read_error))
            return httpx.Response(200, json=dict(ok=True, messages=[dict(ts=ROOT), self.message]))
        assert method == "chat.postMessage"
        self.posts.append(json.loads(request.content))
        return httpx.Response(
            200,
            json=dict(
                ok=True, channel=CHANNEL, ts="1700000001.000001", message=dict(thread_ts=ROOT)
            ),
        )

    def file_transport(self, **request):
        # https://docs.slack.dev/reference/objects/file-object/
        assert request["headers"]["Authorization"] == "Bearer example-support"
        url = request["url"]
        download = "https://files.slack.com/example-clip"
        if url.endswith(f"files.info?file={FILE}"):
            self.calls.append("files.info")
            body = json.dumps(dict(ok=True, file={**self.info, "url_private_download": download}))
            return FileResponse(200, {}, iter([body.encode()]))
        assert url == download
        self.calls.append("download")
        return FileResponse(200, {"Content-Type": "audio/mp4"}, iter([AUDIO]))


@pytest.fixture
async def gateway():
    slack = Slack()
    async with httpx.AsyncClient(transport=httpx.MockTransport(slack.web_api)) as provider:
        providers = GatewayProviders(
            voice=SlackVoiceGateway(
                provider,
                {"support": "example-support", "default": "example-default"},
                file_transport=slack.file_transport,
            )
        )
        app = gateway_application(
            tokens={"api": "apicred", "worker": "workcred", "identity": "lookupcred"},
            providers=providers,
            replay=RequestReplayStore(":memory:"),
            allow_insecure_http=True,
        )
        async with TestServer(app) as server, httpx.AsyncClient() as http:

            def client(token="apicred"):
                return GatewayClient(
                    http, str(server.make_url("")), token, allow_insecure_http=True
                )

            yield slack, client


async def test_reads_current_clip_and_bytes_through_the_receiving_identity(gateway):
    slack, client = gateway
    assert await read_voice_clip(client(), **SOURCE) == ("audio/mp4", AUDIO)
    slack.message["text"] = TEXT.strip()
    with pytest.raises(SlackVoiceReadError) as refused:
        await read_voice_clip(client(), **SOURCE)
    assert refused.value.reason == "source_changed"
    assert slack.calls.count("conversations.replies") == 2
    assert slack.calls.count("download") == 1


@pytest.mark.parametrize(
    "change",
    [
        {"thread_ts": MESSAGE},
        {"ts": ROOT},
        {"user": BOT_USER},
        {"bot_id": BOT},
        {"app_id": APP},
        {"channel": "C0EXAMPLE2"},
        {"files": [CLIP, CLIP]},
        {"files": [{**CLIP, "id": "F0EXAMPLE2"}]},
        {"files": [{**CLIP, "subtype": "upload"}]},
        {"files": [{**CLIP, "user": BOT_USER}]},
        {"subtype": "tombstone"},
    ],
)
async def test_changed_source_never_fetches_media(gateway, change):
    slack, client = gateway
    slack.message.update(change)
    with pytest.raises(SlackVoiceReadError) as refused:
        await read_voice_clip(client(), **SOURCE)
    assert refused.value.reason == "source_changed"
    assert "files.info" not in slack.calls


@pytest.mark.parametrize(
    ("provider_error", "reason"),
    [
        ("thread_not_found", "source_changed"),
        ("not_in_channel", "forbidden"),
        ("ratelimited", "unavailable"),
    ],
)
async def test_provider_refusal_never_fetches_media(gateway, provider_error, reason):
    slack, client = gateway
    slack.read_error = provider_error
    with pytest.raises(SlackVoiceReadError) as refused:
        await read_voice_clip(client(), **SOURCE)
    assert refused.value.reason == reason
    assert "files.info" not in slack.calls


@pytest.mark.parametrize("caller", ["workcred", "lookupcred", "unknown"])
async def test_only_api_can_read_voice_or_display_a_draft(gateway, caller):
    slack, client = gateway
    with pytest.raises(SlackVoiceReadError):
        await read_voice_clip(client(caller), **SOURCE)
    with pytest.raises(SlackVoiceReadError):
        await post_voice_draft(
            client(caller), **SOURCE, text="Private", delivery_id=UUID(int=1), expires_at=None
        )
    assert not slack.calls


@pytest.mark.parametrize("field", ["team", "app"])
async def test_different_receiving_installation_cannot_read_or_post(gateway, field):
    slack, client = gateway
    setattr(slack, field, "other")
    with pytest.raises(SlackVoiceReadError) as refused:
        await read_voice_clip(client(), **SOURCE)
    assert refused.value.reason == "forbidden"
    with pytest.raises(SlackVoiceReadError):
        await post_voice_draft(
            client(), **SOURCE, text="Private", delivery_id=UUID(int=1), expires_at=None
        )
    assert not {"conversations.replies", "chat.postMessage"}.intersection(slack.calls)


async def test_file_limit_precedes_download(gateway):
    slack, client = gateway
    slack.info["size"] = 10 * 1024 * 1024 + 1
    with pytest.raises(SlackVoiceReadError) as refused:
        await read_voice_clip(client(), **SOURCE)
    assert refused.value.reason == "media_limit"
    assert "download" not in slack.calls


async def test_draft_posts_complete_literal_text_to_one_thread(gateway):
    # https://docs.slack.dev/reference/methods/chat.postMessage/
    slack, client = gateway
    text = ("private <@U0EXAMPLE1> https://example.com/ " * 100) + "confirm 1234"
    for _ in range(2):
        await post_voice_draft(
            client(), **SOURCE, text=text, delivery_id=UUID(int=1), expires_at=None
        )
    first = slack.posts[: len(slack.posts) // 2]
    assert first == slack.posts[len(first) :]
    assert len(first) >= 2
    assert len({post["client_msg_id"] for post in first}) == len(first)
    assert "".join(html.unescape(post["text"]) for post in first) == text
    assert "".join(post["blocks"][0]["text"]["text"] for post in first) == text
    for post in first:
        assert post["channel"] == CHANNEL and post["thread_ts"] == ROOT
        assert post["blocks"][0]["text"]["type"] == "plain_text"
        assert "<@" not in post["text"]
        assert post["mrkdwn"] is post["reply_broadcast"] is False
        assert post["unfurl_links"] is post["unfurl_media"] is False
        assert not post["link_names"] and post["parse"] == "none"


async def test_draft_with_changed_source_or_elapsed_deadline_never_posts(gateway):
    slack, client = gateway
    with pytest.raises(SlackVoiceReadError) as expired:
        await post_voice_draft(
            client(),
            **SOURCE,
            text="Private",
            delivery_id=UUID(int=1),
            expires_at=datetime(2020, 1, 1, tzinfo=UTC),
        )
    assert expired.value.reason == "expired"
    slack.message["text"] = "replaced"
    with pytest.raises(SlackVoiceReadError) as changed:
        await post_voice_draft(
            client(), **SOURCE, text="Private", delivery_id=UUID(int=1), expires_at=None
        )
    assert changed.value.reason == "source_changed"
    assert not slack.posts


@pytest.mark.parametrize("status", [403, 404, 413])
async def test_proxy_errors_do_not_claim_an_authoritative_provider_refusal(status):
    def proxy_error(_request):
        return httpx.Response(status, content=b"private gateway diagnostic")

    async with httpx.AsyncClient(transport=httpx.MockTransport(proxy_error)) as http:
        client = GatewayClient(http, "https://example.com", "apicred")
        with pytest.raises(SlackVoiceReadError) as refused:
            await read_voice_clip(client, **SOURCE)
        assert refused.value.reason == "unavailable"
