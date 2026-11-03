"""Bounded follow-up: real Bolt, admission API, Valkey and sender provenance.

Uses the fixture namespace lifecycle in steering.py. Slack Web API and Socket
Mode transport are fake. This does not change production routing or contracts.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import socket
import subprocess
import uuid
from contextlib import asynccontextmanager
from typing import Any, cast

import redis
import uvicorn
from aci_protocol.turn import PrincipalRef
from aiohttp import web
from curie_api.config import get_settings
from curie_api.routers import channels, identity
from curie_dispatcher.app import build_app
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.identities import SlackIdentityCredentials
from curie_dispatcher.identity import shutdown_identity_lookups
from curie_dispatcher.queue import enqueue, from_stream_fields
from fastapi import FastAPI
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_bolt.authorization import AuthorizeResult
from slack_sdk.socket_mode import SocketModeClient
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.web import WebClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from steering import ADAPTER, BOT, CHANNEL, CHECKS, TEAM, checked, command, postgres, server


@asynccontextmanager
async def valkey(context: str, namespace: str) -> Any:
    kubectl = ["kubectl", "--context", context, "-n", namespace]
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "valkey"},
        "spec": {
            "restartPolicy": "Never",
            "containers": [
                {
                    "name": "valkey",
                    "image": "valkey/valkey:8.1.10-alpine",
                    "resources": {
                        "requests": {"cpu": "100m", "memory": "64Mi"},
                        "limits": {"cpu": "500m", "memory": "128Mi"},
                    },
                    "readinessProbe": {
                        "exec": {"command": ["valkey-cli", "ping"]},
                        "periodSeconds": 2,
                    },
                }
            ],
        },
    }
    command([*kubectl, "apply", "-f", "-"], input=json.dumps(pod))
    command([*kubectl, "wait", "--for=condition=Ready", "pod/valkey", "--timeout=60s"])
    forward = subprocess.Popen(
        [*kubectl, "port-forward", "pod/valkey", ":6379", "--address=127.0.0.1"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        assert forward.stdout is not None
        line = await asyncio.wait_for(asyncio.to_thread(forward.stdout.readline), timeout=20)
        match = re.search(r"127\.0\.0\.1:(\d+)", line)
        assert match is not None
        client: redis.Redis = redis.Redis(
            host="127.0.0.1", port=int(match[1]), decode_responses=True
        )
        try:
            assert client.ping()
            yield client
        finally:
            client.close()
    finally:
        forward.terminate()
        forward.wait(timeout=10)


@asynccontextmanager
async def api(sessions: async_sessionmaker[AsyncSession]) -> Any:
    app = FastAPI()
    app.state.sessionmaker = sessions
    app.include_router(channels.router)
    app.include_router(identity.router)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    port = listener.getsockname()[1]
    runtime = uvicorn.Server(uvicorn.Config(app, log_level="error", lifespan="off"))
    running = asyncio.create_task(runtime.serve(sockets=[listener]))
    try:
        # Yield until uvicorn owns the listener; no fixed sleep or retry policy.
        async with asyncio.timeout(5):
            while not runtime.started:
                if running.done():
                    await running
                    raise RuntimeError("API failed to start")
                await asyncio.sleep(0)
        yield f"http://127.0.0.1:{port}"
    finally:
        runtime.should_exit = True
        await running
        listener.close()


class Socket:
    def __init__(self) -> None:
        self.logger = logging.getLogger("spike-socket")
        self.acks: list[str] = []

    def send_socket_mode_response(self, response: Any) -> None:
        self.acks.append(response.envelope_id)


async def experiment(database_url: str, store: redis.Redis) -> None:
    os.environ["API_KEY"] = "fixture-platform-key"
    get_settings.cache_clear()
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    posts: list[dict[str, Any]] = []

    async def slack(request: web.Request) -> web.Response:
        if request.match_info["method"] == "conversations.replies":
            return web.json_response(
                {
                    "ok": True,
                    "messages": [
                        {
                            "ts": "1700000000.000001",
                            "user": BOT,
                            "bot_id": "B0EXAMPLE1",
                            "text": "Factory run for fixture issue 42",
                        }
                    ],
                }
            )
        body = await request.json()
        posts.append(body)
        return web.json_response({"ok": True, "ts": "1700000001.000002", "channel": CHANNEL})

    slack_app = web.Application()
    slack_app.router.add_route("*", "/api/{method}", slack)
    try:
        async with server(slack_app) as slack_url, api(sessions) as api_url:
            config = DispatcherConfig(
                api_base_url=api_url,
                api_key="fixture-platform-key",
                approval_chat_attester_secret="fixture-attester-key",
                stream="spike-ingress",
                dedupe_prefix="spike-ingress-dedupe:",
            )
            web_client = WebClient(token="fixture-factory", base_url=f"{slack_url}/api/")

            def authorize(**kwargs: Any) -> AuthorizeResult:
                return AuthorizeResult(
                    enterprise_id=None,
                    team_id=TEAM,
                    bot_token="fixture-factory",
                    bot_id="B0EXAMPLE1",
                    bot_user_id=BOT,
                )

            bolt = build_app(
                config,
                web_client=web_client,
                redis_client=store,
                identity=SlackIdentityCredentials(ADAPTER, "fixture-app", "fixture-factory"),
                authorize=authorize,
            )
            handler = SocketModeHandler(bolt, app_token="fixture-app")
            wire = Socket()
            body = 'Keep the API stable.\nUse "blue" as the label.'

            def request(key: str, **changes: Any) -> SocketModeRequest:
                return SocketModeRequest(
                    type="events_api",
                    envelope_id=f"env-{key}",
                    payload={
                        "type": "event_callback",
                        "team_id": TEAM,
                        "event_id": key,
                        "event": {
                            "type": "app_mention",
                            "channel": CHANNEL,
                            "user": "U0EXAMPLE1",
                            "user_team": TEAM,
                            "ts": "1700000001.000001",
                            "thread_ts": "1700000000.000001",
                            "text": f"<@{BOT}> {body}",
                            **changes,
                        },
                    },
                )

            def drive() -> None:
                for payload in [
                    request("steer"),
                    request("steer"),
                    request("ordinary", type="message", channel_type="channel", text=body),
                    request(
                        "edit", type="message", subtype="message_changed", channel_type="channel"
                    ),
                    request("self", user=BOT, bot_id="B0EXAMPLE1"),
                ]:
                    handler.handle(cast(SocketModeClient, wire), payload)
                bolt.listener_runner.listener_executor.shutdown(wait=True)

            await asyncio.to_thread(drive)
            checked("all_socket_envelopes_acked", len(wire.acks) == 5)
            checked("one_turn_from_mention_and_duplicate_only", store.xlen(config.stream) == 1)
            checked("one_placeholder_for_duplicate_delivery", len(posts) == 1)
            rows = cast(list[tuple[str, dict[str, str]]], store.xrange(config.stream))
            queued = from_stream_fields(rows[0][1])
            assert queued.reply_handle is not None
            checked(
                "root_thread_and_factory_identity_preserved",
                queued.conversation_id == "1700000000.000001"
                and queued.reply_handle.adapter == ADAPTER,
            )
            checked("full_human_body_survives_bolt_and_stream", queued.text.endswith(body))
            checked("bot_root_context_is_added", "Factory run for fixture issue 42" in queued.text)
            checked("current_socket_ingress_omits_principal", queued.author_principal is None)
            checked(
                "current_socket_ingress_omits_sender_namespace",
                "user_team" not in queued.model_dump() and "team_id" not in queued.model_dump(),
            )
            principal = PrincipalRef(principal_id=str(uuid.uuid4()), authorization_version=1)
            enriched = queued.model_copy(
                update={"event_id": "spike-enriched", "author_principal": principal}
            )
            enqueue(store, config, enriched)
            rows = cast(list[tuple[str, dict[str, str]]], store.xrange(config.stream))
            decoded = from_stream_fields(rows[1][1])
            checked(
                "existing_contract_carries_principal_without_schema_change",
                decoded.author_principal == principal,
            )
            print("Real Bolt, real API and real Valkey ingress checks passed", flush=True)
    finally:
        shutdown_identity_lookups()
        await engine.dispose()


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    args = parser.parse_args()
    async with postgres(args.context, args.namespace) as database_url:
        async with valkey(args.context, args.namespace) as store:
            await experiment(database_url, store)
    print(json.dumps({"passed": len(CHECKS), "checks": CHECKS}, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
