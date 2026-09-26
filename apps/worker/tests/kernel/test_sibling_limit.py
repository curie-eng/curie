"""An exchange between two of an installation's identities stops at the limit.

ADR-0168 decision 6's test: a simulated exchange between two fake identities,
over Slack and over the channel port, in one thread and across
conversations. Real kernel, Valkey, substrate, ``build_reply_sink`` and
sibling limit. The doubles are a fake runner, a Slack capture server that also
answers ``auth.test``, an adapter capture server, and the channel lookup,
keyed as the real one reads.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable, Mapping
from typing import Any

from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus
from aci_protocol.turn import CLUSTER_MESSAGE_ADAPTER, route_identity
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.binding import BUDGET_ENV, BUNDLE_REF_ENV, PLUGIN_DIR_ENV, ResolvedDeployment
from curie_worker.config import WorkerConfig
from curie_worker.reply_sink import (
    ObservedReplySink,
    ReplySinkRouter,
    TargetRoute,
    build_reply_sink,
)
from curie_worker.sibling_turns import (
    SIBLING_LIMIT_NOTICE,
    SIBLING_OPEN_LIMIT,
    SIBLING_TURN_LIMIT,
    SiblingTurnLimit,
    SlackSenderIdentities,
)
from redis.asyncio import Redis as AsyncRedis

DONE = SessionStatus.DONE
_CHANNEL = "C0EXAMPLE1"
_DEFAULT_TOKEN = "xoxb-default-sentinel"
_OPS_TOKEN = "xoxb-ops-bot-sentinel"
_DEFAULT_USER = "U0EXAMPLE1"
_OPS_USER = "U0EXAMPLE2"
_PERSON = "U0EXAMPLE9"
_BOT_USERS = {f"Bearer {_DEFAULT_TOKEN}": _DEFAULT_USER, f"Bearer {_OPS_TOKEN}": _OPS_USER}
_A = "a@example.com"
_B = "b@example.com"


class _SlackCapture:
    def __init__(self) -> None:
        self.requests: list[tuple[str, str | None, dict[str, Any]]] = []
        self.app = web.Application()
        self.app.add_routes([web.post("/slack/api/{method}", self._slack)])

    async def _slack(self, request: web.Request) -> web.Response:
        method = request.match_info["method"]
        auth = request.headers.get("Authorization")
        if request.content_type == "application/json":
            body: dict[str, Any] = await request.json()
        else:
            body = {key: str(value) for key, value in (await request.post()).items()}
        self.requests.append((method, auth, body))
        if method == "auth.test":
            user = _BOT_USERS.get(auth or "")
            if user is None:
                return web.json_response({"ok": False, "error": "invalid_auth"})
            return web.json_response({"ok": True, "user_id": user, "bot_id": "B" + user[1:]})
        return web.json_response({"ok": True, "ts": "1720000000.999999"})

    def notices(self) -> list[tuple[str | None, str]]:
        return [
            (auth, str(body.get("ts")))
            for method, auth, body in self.requests
            if method == "chat.update" and SIBLING_LIMIT_NOTICE in str(body.get("text", ""))
        ]


class _AdapterCapture:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []
        self.app = web.Application()
        self.app.add_routes([web.post("/{adapter}/", self._event)])

    async def _event(self, request: web.Request) -> web.Response:
        self.events.append((request.match_info["adapter"], await request.json()))
        return web.json_response({})

    def for_ref(self, reply_ref: str) -> list[dict[str, Any]]:
        return [
            event
            for _adapter, event in self.events
            if (event.get("target") or {}).get("reply_ref") == reply_ref
        ]


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


class _BoundAddresses:
    """Which identity is bound at an address, compared as the real lookup does."""

    def __init__(self, rows: Mapping[tuple[str, str], str]) -> None:
        self._rows = dict(rows)

    async def identity_for_address(self, kind: str, address: str) -> str | None:
        return self._rows.get((kind, address.lower()))


class _EmitOnly:
    async def emit(self, event: object, *, route: object, best_effort_unreachable: bool = False):
        raise AssertionError("not called")


def _resolved(adapter: str, endpoint: str | None = None) -> ResolvedDeployment:
    return ResolvedDeployment(
        agent_id=uuid.uuid4(),
        agent_name="test-agent",
        version_id=uuid.uuid4(),
        version_label="v1",
        bundle_ref="bundles/x.zip",
        max_usd_per_day=None,
        max_output_tokens_per_run=None,
        endpoint=endpoint,
        adapter=adapter,
    )


def _slack_turn(text: str, *, adapter: str | None, author: str, thread: str, ref: str):
    return QueuedTurn(
        event_id=uuid.uuid4().hex,
        conversation_id=thread,
        author=author,
        text=text,
        reply_handle=ReplyHandle(kind="slack", channel=_CHANNEL, placeholder=ref, adapter=adapter),
        received_at="2026-07-05T00:00:00+00:00",
    )


def _mail_turn(text: str, *, to: str, adapter: str, author: str, thread: str, port: int):
    return QueuedTurn(
        event_id=uuid.uuid4().hex,
        conversation_id=thread,
        author=author,
        text=text,
        reply_handle=ReplyHandle(
            kind="email",
            channel=to,
            placeholder=f"<{uuid.uuid4().hex}@example.com>",
            endpoint=f"http://127.0.0.1:{port}/{adapter}/",
            adapter=adapter,
        ),
        received_at="2026-07-05T00:00:00+00:00",
    )


def _slack_sink(port: int) -> ReplySinkRouter:
    return build_reply_sink(
        WorkerConfig(
            slack_bot_token=_DEFAULT_TOKEN,
            slack_api_base_url=f"http://127.0.0.1:{port}/slack/api/",
        ),
        slack_tokens={"default": _DEFAULT_TOKEN, "ops-bot": _OPS_TOKEN},
    )


def _mail_sink() -> ReplySinkRouter:
    return build_reply_sink(
        WorkerConfig(adapter_credentials={"mail-a": "secret-a", "mail-b": "secret-b"}),
        slack_tokens={"default": ""},
    )


def _slack_limit(port: int) -> Callable[[AsyncRedis, WorkerConfig], SiblingTurnLimit]:
    def build(client: AsyncRedis, config: WorkerConfig) -> SiblingTurnLimit:
        return SiblingTurnLimit(
            client,
            key_prefix=config.key_prefix,
            slack=SlackSenderIdentities(
                {"default": _DEFAULT_TOKEN, "ops-bot": _OPS_TOKEN},
                base_url=f"http://127.0.0.1:{port}/slack/api/",
            ),
        )

    return build


def _mail_limit(client: AsyncRedis, config: WorkerConfig) -> SiblingTurnLimit:
    return SiblingTurnLimit(
        client,
        key_prefix=config.key_prefix,
        channel=_BoundAddresses({("email", _A): "mail-a", ("email", _B): "mail-b"}),
    )


def _slack_binding() -> _TripleBinding:
    return _TripleBinding(
        {
            ("slack", "default", _CHANNEL): _resolved("default"),
            ("slack", "ops-bot", _CHANNEL): _resolved("ops-bot"),
        }
    )


def _mail_binding(port: int) -> _TripleBinding:
    return _TripleBinding(
        {
            ("email", "mail-a", _A): _resolved("mail-a", f"http://127.0.0.1:{port}/mail-a/"),
            ("email", "mail-b", _B): _resolved("mail-b", f"http://127.0.0.1:{port}/mail-b/"),
        }
    )


async def _ran(h: Any, ev: QueuedTurn) -> bool:
    before = len(h.runner.opened)
    await h.kernel.process_event(ev)
    assert await h.async_redis.exists(h.config.done_key(ev.event_id))
    return len(h.runner.opened) > before


async def _warm_slack_identities(h: Any, *authors: str) -> None:
    """Resolve each bot user before a counted burst.

    ``identity_of`` never blocks on ``auth.test`` (finding 4): a turn checked
    while an identity is still unknown fails open and is not counted. Warming
    the cache first mirrors a worker that has already seen this identity
    resolve, so the burst below is counted from its first turn.
    """

    slack = h.kernel._sibling_limit.slack
    for author in authors:
        await slack.identity_of(author)
    task = slack._refresh_task
    if task is not None:
        await task


def test_only_the_slack_adapter_edits_in_place() -> None:
    router = _mail_sink()
    assert router.edits_in_place("slack", TargetRoute()) is True
    assert router.edits_in_place(
        "email", TargetRoute(endpoint="http://127.0.0.1:1/mail-a/", adapter="mail-a")
    ) is False
    assert router.edits_in_place("slack", TargetRoute(adapter=CLUSTER_MESSAGE_ADAPTER)) is False
    assert ObservedReplySink(router).edits_in_place("slack", TargetRoute()) is True
    assert ObservedReplySink(_EmitOnly()).edits_in_place("slack", TargetRoute()) is False


def test_two_slack_identities_answering_each_other_in_one_thread_stop_at_the_limit(
    make_harness,
) -> None:
    async def go() -> None:
        capture = _SlackCapture()
        server = TestServer(capture.app)
        await server.start_server()
        try:
            port = server.port
            assert port is not None
            thread = "1720000000.000100"
            hops = 2 * SIBLING_TURN_LIMIT + 2
            async with make_harness(
                binding=_slack_binding(),
                sink=_slack_sink(port),
                sibling_limit_factory=_slack_limit(port),
            ) as h:
                h.runner.default_script = [Final(text="answer", status=DONE)]
                await _warm_slack_identities(h, _DEFAULT_USER, _OPS_USER)
                turns = [
                    _slack_turn(
                        f"hop {hop}",
                        adapter="ops-bot" if hop % 2 == 0 else None,
                        author=_DEFAULT_USER if hop % 2 == 0 else _OPS_USER,
                        thread=thread,
                        ref=f"1720000000.{hop + 200:06d}",
                    )
                    for hop in range(hops)
                ]
                ran = [await _ran(h, ev) for ev in turns]
                assert ran == [True] * (2 * SIBLING_TURN_LIMIT) + [False, False]

                # A person in the same thread is never limited.
                person = _slack_turn(
                    "a person asks", adapter="ops-bot", author=_PERSON, thread=thread,
                    ref="1720000000.000999",
                )
                assert await _ran(h, person)
            # Each dropped placeholder is edited by the app that posted it.
            assert capture.notices() == [
                (f"Bearer {_OPS_TOKEN}", turns[-2].reply_handle.placeholder),
                (f"Bearer {_DEFAULT_TOKEN}", turns[-1].reply_handle.placeholder),
            ]
            assert [method for method, _a, _b in capture.requests].count("auth.test") == 2
        finally:
            await server.close()

    asyncio.run(go())


def test_one_slack_identity_opening_conversation_after_conversation_stops_at_the_limit(
    make_harness,
) -> None:
    async def go() -> None:
        capture = _SlackCapture()
        server = TestServer(capture.app)
        await server.start_server()
        try:
            port = server.port
            assert port is not None
            async with make_harness(
                binding=_slack_binding(),
                sink=_slack_sink(port),
                sibling_limit_factory=_slack_limit(port),
            ) as h:
                h.runner.default_script = [Final(text="answer", status=DONE)]
                await _warm_slack_identities(h, _DEFAULT_USER, _OPS_USER)
                turns = [
                    _slack_turn(
                        f"new thread {n}",
                        adapter="ops-bot",
                        author=_DEFAULT_USER,
                        thread=f"1720000100.{n:06d}",
                        ref=f"1720000100.{n:06d}",
                    )
                    for n in range(SIBLING_OPEN_LIMIT + 2)
                ]
                ran = [await _ran(h, ev) for ev in turns]
                assert ran == [True] * SIBLING_OPEN_LIMIT + [False, False]
                # The other direction is its own pair.
                back = _slack_turn(
                    "back", adapter=None, author=_OPS_USER, thread="1720000200.000001",
                    ref="1720000200.000001",
                )
                assert await _ran(h, back)
            assert [ts for _auth, ts in capture.notices()] == [
                turn.reply_handle.placeholder for turn in turns[-2:]
            ]
        finally:
            await server.close()

    asyncio.run(go())


def test_two_inboxes_answering_each_other_in_one_thread_stop_at_the_limit(
    make_harness,
) -> None:
    async def go() -> None:
        capture = _AdapterCapture()
        server = TestServer(capture.app)
        await server.start_server()
        try:
            port = server.port
            assert port is not None
            async with make_harness(
                binding=_mail_binding(port),
                sink=_mail_sink(),
                sibling_limit_factory=_mail_limit,
                # The shimmer clear in the kernel's exit path is unconditional
                # on every path, including a pre-binding drop (EB-B6(a)); on
                # Slack that is a no-op, but on this raw capture it would show
                # up as a spurious ``turn.status`` beside the drop's own
                # completion. Off here so the assertion below reads only what
                # ADR-0168 decision 6 is actually responsible for.
                shimmer=False,
            ) as h:
                h.runner.default_script = [Final(text="answer", status=DONE)]
                turns = [
                    _mail_turn(
                        f"hop {hop}",
                        to=_B if hop % 2 == 0 else _A,
                        adapter="mail-b" if hop % 2 == 0 else "mail-a",
                        author=_A if hop % 2 == 0 else _B,
                        thread="thr-ab",
                        port=port,
                    )
                    for hop in range(2 * SIBLING_TURN_LIMIT + 2)
                ]
                ran = [await _ran(h, ev) for ev in turns]
                assert ran == [True] * (2 * SIBLING_TURN_LIMIT) + [False, False]
            for dropped in turns[-2:]:
                # No text: on a buffered channel it would be mailed to the
                # sibling as the next turn of the exchange.
                events = capture.for_ref(dropped.reply_handle.placeholder)
                assert [(e["event"], e.get("outcome")) for e in events] == [
                    ("turn.completed", "dropped")
                ]
        finally:
            await server.close()

    asyncio.run(go())


def test_one_inbox_opening_conversation_after_conversation_stops_at_the_limit(
    make_harness,
) -> None:
    async def go() -> None:
        capture = _AdapterCapture()
        server = TestServer(capture.app)
        await server.start_server()
        try:
            port = server.port
            assert port is not None
            async with make_harness(
                binding=_mail_binding(port), sink=_mail_sink(), sibling_limit_factory=_mail_limit
            ) as h:
                h.runner.default_script = [Final(text="answer", status=DONE)]
                turns = [
                    _mail_turn(
                        f"new thread {n}", to=_B, adapter="mail-b", author=_A,
                        thread=f"thr-{n}", port=port,
                    )
                    for n in range(SIBLING_OPEN_LIMIT + 2)
                ]
                ran = [await _ran(h, ev) for ev in turns]
                assert ran == [True] * SIBLING_OPEN_LIMIT + [False, False]
                person = _mail_turn(
                    "a person writes", to=_B, adapter="mail-b", author="c@example.com",
                    thread="thr-person", port=port,
                )
                assert await _ran(h, person)
        finally:
            await server.close()

    asyncio.run(go())


def test_a_kernel_with_no_sibling_limit_runs_every_turn_and_counts_nothing(
    make_harness,
) -> None:
    async def go() -> None:
        capture = _SlackCapture()
        server = TestServer(capture.app)
        await server.start_server()
        try:
            port = server.port
            assert port is not None
            async with make_harness(binding=_slack_binding(), sink=_slack_sink(port)) as h:
                h.runner.default_script = [Final(text="answer", status=DONE)]
                for hop in range(2 * SIBLING_TURN_LIMIT + 2):
                    ev = _slack_turn(
                        f"hop {hop}",
                        adapter="ops-bot" if hop % 2 == 0 else None,
                        author=_DEFAULT_USER if hop % 2 == 0 else _OPS_USER,
                        thread="1720000300.000100",
                        ref=f"1720000300.{hop + 200:06d}",
                    )
                    assert await _ran(h, ev)
                assert await h.async_redis.keys(f"{h.config.key_prefix}:sibling:*") == []
            assert "auth.test" not in [method for method, _a, _b in capture.requests]
            assert capture.notices() == []
        finally:
            await server.close()

    asyncio.run(go())
