"""A turn is answered by the Slack identity it arrived on (ADR-0168 decision 5).

Real kernel, Valkey, substrate and ``build_reply_sink``; a fake runner and an
in-process Slack capture server, so the token behind every call the turn makes
is read off the wire.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus
from aci_protocol.turn import route_identity
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.binding import BUDGET_ENV, BUNDLE_REF_ENV, PLUGIN_DIR_ENV, ResolvedDeployment
from curie_worker.config import WorkerConfig
from curie_worker.reply_sink import ReplySinkRouter, build_reply_sink

DONE = SessionStatus.DONE
_CHANNEL = "C0EXAMPLE1"
_DEFAULT_TOKEN = "xoxb-default-sentinel"
_OPS_TOKEN = "xoxb-ops-bot-sentinel"


class _Capture:
    def __init__(self) -> None:
        self.requests: list[tuple[str, str | None]] = []
        self.app = web.Application()
        self.app.add_routes([web.post("/slack/api/{method}", self._slack)])

    async def _slack(self, request: web.Request) -> web.Response:
        self.requests.append(
            (request.match_info["method"], request.headers.get("Authorization"))
        )
        return web.json_response({"ok": True, "ts": "1720000000.000200"})

    def methods(self) -> list[str]:
        return [method for method, _auth in self.requests]

    def tokens(self) -> set[str | None]:
        return {auth for _method, auth in self.requests}


class _TripleBinding:
    """Canned resolutions keyed on the route triple, like the real resolver."""

    def __init__(self, routes: dict[tuple[str, str | None, str], ResolvedDeployment]) -> None:
        self._routes = routes

    async def resolve(
        self, kind: str, adapter: str | None, address: str
    ) -> ResolvedDeployment | None:
        return self._routes.get((kind, route_identity(kind, adapter), address))

    def boot_env(
        self,
        resolved: ResolvedDeployment,
        thread_key: str,
        *,
        kind: str | None = None,
        address: str | None = None,
        isolate_memory: bool = False,
    ) -> dict[str, str]:
        env = {
            BUDGET_ENV: '{"max_output_tokens_per_run":100000,"max_usd_per_day":10.0}',
            PLUGIN_DIR_ENV: "/bundles/current",
        }
        if resolved.bundle_ref is not None:
            env[BUNDLE_REF_ENV] = resolved.bundle_ref
        return env

    def packs_for(self, resolved: ResolvedDeployment) -> BehaviorPacks:
        return BehaviorPacks.from_config(resolved.behavior_packs)


def _resolved(adapter: str) -> ResolvedDeployment:
    return ResolvedDeployment(
        agent_id=uuid.uuid4(),
        agent_name="test-agent",
        version_id=uuid.uuid4(),
        version_label="v1",
        bundle_ref="bundles/x.zip",
        max_usd_per_day=None,
        max_output_tokens_per_run=None,
        adapter=adapter,
    )


def _qevent(text: str, *, adapter: str | None, thread: str) -> QueuedTurn:
    return QueuedTurn(
        event_id=uuid.uuid4().hex,
        conversation_id=thread,
        author="U1",
        text=text,
        reply_handle=ReplyHandle(
            kind="slack", channel=_CHANNEL, placeholder="1720000000.000100", adapter=adapter
        ),
        received_at="2026-07-05T00:00:00+00:00",
    )


def _sink(port: int) -> ReplySinkRouter:
    return build_reply_sink(
        WorkerConfig(
            slack_bot_token=_DEFAULT_TOKEN,
            slack_api_base_url=f"http://127.0.0.1:{port}/slack/api/",
        ),
        slack_tokens={"default": _DEFAULT_TOKEN, "ops-bot": _OPS_TOKEN},
    )


@pytest.mark.parametrize(
    ("adapter", "identity", "token"),
    [("ops-bot", "ops-bot", _OPS_TOKEN), (None, "default", _DEFAULT_TOKEN)],
)
def test_a_turn_is_answered_with_the_token_of_the_identity_it_arrived_on(
    make_harness, adapter: str | None, identity: str, token: str
) -> None:
    async def go() -> None:
        capture = _Capture()
        server = TestServer(capture.app)
        await server.start_server()
        try:
            port = server.port
            assert port is not None
            binding = _TripleBinding({("slack", identity, _CHANNEL): _resolved(identity)})
            async with make_harness(binding=binding, sink=_sink(port)) as h:
                h.runner.default_script = [Final(text="answer", status=DONE)]
                ev = _qevent("hi", adapter=adapter, thread=f"t-{identity}")
                await h.kernel.process_event(ev)

                assert h.runner.opened == ["hi"]
                assert await h.async_redis.exists(h.config.done_key(ev.event_id))
            assert "chat.update" in capture.methods()
            assert capture.tokens() == {f"Bearer {token}"}
        finally:
            await server.close()

    asyncio.run(go())
