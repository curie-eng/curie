"""A real gateway listener with only the external Slack provider substituted."""

import asyncio
import threading
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from aiohttp import web
from curie_channel_gateway.ports.reply import ReplyEventTransport
from curie_channel_gateway.read_transport import GatewayProviders, gateway_application
from curie_channel_gateway.reply_delivery import ReplyDeliveryStore
from curie_channel_gateway.reply_route import ReplyRoute
from curie_channel_gateway.request_replay import RequestReplayStore
from curie_channel_gateway.slack.canvas import SlackCanvasReader
from curie_channel_gateway.slack.groups import SlackUserGroupClient
from curie_channel_gateway.slack.reads import SlackChannelReader

API_TOKEN = "test-gateway-api-credential"
WORKER_TOKEN = "test-gateway-worker-credential"


@contextmanager
def slack_gateway(
    handler: Callable[[httpx.Request], httpx.Response] | None,
    tokens: Mapping[str, str],
    *,
    reply: ReplyEventTransport[ReplyRoute] | None = None,
) -> Iterator[str]:
    started = threading.Event()
    endpoints: list[str] = []
    errors: list[BaseException] = []
    loops: list[asyncio.AbstractEventLoop] = []
    stops: list[asyncio.Event] = []

    async def serve() -> None:
        loops.append(asyncio.get_running_loop())
        stop = asyncio.Event()
        stops.append(stop)
        with TemporaryDirectory(prefix="curie-gateway-test-") as directory:
            reply_state = ReplyDeliveryStore(Path(directory) / "replies.sqlite3") if reply else None
            await serve_gateway(stop, reply_state)

    async def serve_gateway(stop: asyncio.Event, reply_state: ReplyDeliveryStore | None) -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler) if handler is not None else None
        ) as http:
            app = gateway_application(
                replay=RequestReplayStore(":memory:"),
                tokens={"api": API_TOKEN, "worker": WORKER_TOKEN},
                groups={
                    name: SlackUserGroupClient(http, token=token) for name, token in tokens.items()
                },
                providers=GatewayProviders(
                    channel=SlackChannelReader(http, tokens), canvas=SlackCanvasReader(http, tokens)
                ),
                allow_insecure_http=True,
                reply=reply,
                reply_state=reply_state,
            )
            runner = web.AppRunner(app, access_log=None)
            await runner.setup()
            try:
                site = web.TCPSite(runner, "127.0.0.1", 0)
                await site.start()
                endpoints.append(f"http://127.0.0.1:{runner.addresses[0][1]}")
                started.set()
                await stop.wait()
            finally:
                await runner.cleanup()

    def run() -> None:
        try:
            asyncio.run(serve())
        except BaseException as error:  # noqa: BLE001 - propagate thread startup and shutdown failures
            errors.append(error)
        finally:
            started.set()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        if not started.wait(10) or errors or not endpoints:
            raise RuntimeError("test gateway failed to start")
        yield endpoints[0]
    finally:
        if loops and stops and thread.is_alive():
            loops[0].call_soon_threadsafe(stops[0].set)
        thread.join(timeout=10)
        if thread.is_alive() or errors:
            raise RuntimeError("test gateway did not shut down cleanly")


@contextmanager
def reply_gateway(tokens: Mapping[str, str], base_url: str) -> Iterator[dict[str, object]]:
    """A real gateway between worker composition and an external Slack stub."""
    from aci_protocol.slack_identities import SlackIdentities
    from curie_channel_gateway.slack.slack_sink import SlackReplyAdapter
    from pydantic import TypeAdapter

    declarations = [
        {
            "name": name,
            "app_token_env": "SLACK_APP_TOKEN"
            if name == "default"
            else f"CURIE_SLACK_APP_TOKEN__{index}",
            "bot_token_env": "SLACK_BOT_TOKEN"
            if name == "default"
            else f"CURIE_SLACK_BOT_TOKEN__{index}",
        }
        for index, name in enumerate(dict.fromkeys(["default", *tokens]))
    ]
    provider = SlackReplyAdapter(
        tokens.get("default", ""), identity_tokens=tokens, base_url=base_url
    )
    with slack_gateway(lambda request: httpx.Response(500), {}, reply=provider) as url:
        yield {
            "channel_gateway_url": url,
            "channel_gateway_token": WORKER_TOKEN,
            "channel_gateway_allow_insecure": True,
            "slack_identities": TypeAdapter(SlackIdentities).validate_python(declarations),
        }
