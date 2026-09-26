"""A turn is answered by the Slack identity it arrived on (ADR-0168 decision 5).

Real kernel, Valkey, substrate and ``build_reply_sink``; a fake runner and an
in-process Slack capture server, so the token behind every call the turn makes
is read off the wire.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus, TurnSource
from aci_protocol.turn import route_identity
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_worker.approvals import CreatedApproval
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.binding import BUDGET_ENV, BUNDLE_REF_ENV, PLUGIN_DIR_ENV, ResolvedDeployment
from curie_worker.config import WorkerConfig
from curie_worker.reply_sink import ReplySinkRouter, build_reply_sink
from curie_worker.workitem_dispatch import WorkItemRunning

DONE = SessionStatus.DONE
AWAITING = SessionStatus.AWAITING_APPROVAL
_CHANNEL = "C0EXAMPLE1"
_POLICY_CHANNEL = "C0EXAMPLE2"
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


_POLICY_ROUTE_NAME = "policy"


def _routed_resolved(adapter: str | None) -> ResolvedDeployment:
    """Like ``_resolved``, but the deployment also binds an approval route to a
    channel other than ``_CHANNEL`` -- the policy-card shape finding 1 covers.
    """

    return ResolvedDeployment(
        agent_id=uuid.uuid4(),
        agent_name="test-agent",
        version_id=uuid.uuid4(),
        version_label="v1",
        bundle_ref="bundles/x.zip",
        max_usd_per_day=None,
        max_output_tokens_per_run=None,
        adapter=adapter,
        approval_routes={
            _POLICY_ROUTE_NAME: {"resolution": {"kind": "slack", "address": _POLICY_CHANNEL}}
        },
    )


def _awaiting_routed_script(summary: str) -> list:
    return [
        Final(
            text=summary,
            status=AWAITING,
            approval_summary=summary,
            approval_route=_POLICY_ROUTE_NAME,
        )
    ]


class _RecordingApprovals:
    """An ApprovalCreator fake that records requests and mints stable ids."""

    def __init__(self) -> None:
        self.requests: list[object] = []

    async def create(self, request: object) -> CreatedApproval:
        self.requests.append(request)
        return CreatedApproval(id=f"appr-{len(self.requests)}", status="pending")


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


def test_a_turn_on_an_identity_this_worker_cannot_speak_as_is_dropped_before_it_runs(
    make_harness, caplog: pytest.LogCaptureFixture
) -> None:
    # No reply is possible: only the addressed bot may edit its placeholder,
    # and any other bot's reply is the defect. So nothing runs, nothing is
    # claimed, Slack is never called, and the log names what was refused.
    async def go() -> None:
        capture = _Capture()
        server = TestServer(capture.app)
        await server.start_server()
        try:
            port = server.port
            assert port is not None
            binding = _TripleBinding({("slack", "ghost", _CHANNEL): _resolved("ghost")})
            async with make_harness(binding=binding, sink=_sink(port)) as h:
                h.runner.default_script = [Final(text="answer", status=DONE)]
                ev = _qevent("hi", adapter="ghost", thread="t-ghost")
                with caplog.at_level(logging.ERROR, logger="curie_worker.kernel"):
                    await h.kernel.process_event(ev)

                assert h.runner.opened == []
                assert h.fake_k8s.claims == {}
                assert await h.async_redis.exists(h.config.done_key(ev.event_id))
            assert capture.requests == []
            text = "\n".join(caplog.messages)
            assert ev.event_id in text and "'ghost'" in text
            assert _DEFAULT_TOKEN not in text and _OPS_TOKEN not in text
        finally:
            await server.close()

    asyncio.run(go())


class _CiWorkItems:
    """Names one already-running request, like a CI continuation's dispatch."""

    def __init__(self, running: uuid.UUID) -> None:
        self.running = running
        self.finishes: list[tuple[uuid.UUID, dict[str, object]]] = []

    async def running_for_conversation(self, _conversation_id: str) -> WorkItemRunning:
        return WorkItemRunning(
            request_id=self.running,
            runtime_epoch=1,
            execution_deadline=datetime.now(UTC) + timedelta(minutes=20),
        )

    async def finish(self, request_id: uuid.UUID, **kwargs: object) -> None:
        self.finishes.append((request_id, kwargs))

    def __getattr__(self, name: str):  # type: ignore[no-untyped-def]
        async def record(*_args: object, **_kwargs: object) -> None:
            return None

        return record


def test_a_factory_work_item_turn_runs_on_an_untokened_identity(
    make_harness, caplog: pytest.LogCaptureFixture
) -> None:
    """The gate's factory-work-item exemption, pinned directly rather than only
    documented in its inline comment: a CI-fix-round continuation whose route
    names an identity this worker holds NO token for must still run.

    Its requesting-chat reply is already suppressed end to end (``_reply_for``,
    ``_ThrottledReply``'s ``target=None``), so there is no wrong-bot reply for
    the gate to prevent here, and dropping it would lose a legitimate factory
    continuation instead. The sink is REAL (``build_reply_sink``); its
    ``undeliverable_reason`` WOULD refuse this exact route if the gate did not
    skip factory turns (see the sibling test above, on the same 'ghost'
    identity), so deleting or inverting
    ``not self._is_factory_work_item_turn(event_id)`` fails this test.
    """

    async def go() -> None:
        capture = _Capture()
        server = TestServer(capture.app)
        await server.start_server()
        try:
            port = server.port
            assert port is not None
            request_id = uuid.uuid4()
            binding = _TripleBinding({("slack", "ghost", _CHANNEL): _resolved("ghost")})
            async with make_harness(binding=binding, sink=_sink(port)) as h:
                h.kernel._work_items = _CiWorkItems(request_id)
                h.runner.default_script = [Final(text="fixed it", status=DONE)]
                ev = QueuedTurn(
                    event_id=f"work-item-{request_id}-ci-2",
                    conversation_id=f"work-item-{request_id}",
                    author="github:1:bot",
                    text="fix it",
                    reply_handle=ReplyHandle(
                        kind="slack",
                        channel=_CHANNEL,
                        placeholder="1720000000.000100",
                        adapter="ghost",
                    ),
                    received_at="2026-07-05T00:00:00+00:00",
                    source=TurnSource.WEBHOOK,
                )
                with caplog.at_level(logging.ERROR, logger="curie_worker.kernel"):
                    await h.kernel.process_event(ev)

                assert h.runner.opened == ["fix it"]
                assert not any("dropping event" in message for message in caplog.messages)
            assert capture.requests == []
        finally:
            await server.close()

    asyncio.run(go())


@pytest.mark.parametrize(
    ("adapter", "identity", "token"),
    [("ops-bot", "ops-bot", _OPS_TOKEN), (None, "default", _DEFAULT_TOKEN)],
)
def test_a_policy_routed_approval_card_posts_with_the_turns_identity_token(
    make_harness, adapter: str | None, identity: str, token: str
) -> None:
    """A policy-routed card belongs to no conversation and carries no per-turn
    endpoint or thread of its own (final-review.md finding 1), but it still
    must speak as the identity the turn arrived on: a named identity's turn
    that requests sign-off in a channel bound to a policy route must post that
    card under its OWN token, never ``default``'s -- an install where only
    that identity sits in the policy channel would otherwise mint a card
    nobody can see.
    """

    async def go() -> None:
        capture = _Capture()
        server = TestServer(capture.app)
        await server.start_server()
        try:
            port = server.port
            assert port is not None
            binding = _TripleBinding({("slack", identity, _CHANNEL): _routed_resolved(identity)})
            approvals = _RecordingApprovals()
            async with make_harness(binding=binding, sink=_sink(port), approvals=approvals) as h:
                h.runner.default_script = _awaiting_routed_script("needs sign-off")
                ev = _qevent("please", adapter=adapter, thread=f"t-policy-{identity}")
                await h.kernel.process_event(ev)

                assert len(approvals.requests) == 1
            assert "chat.postMessage" in capture.methods()
            assert capture.tokens() == {f"Bearer {token}"}
        finally:
            await server.close()

    asyncio.run(go())


def test_a_custom_transport_turns_policy_routed_card_still_posts_as_default(
    make_harness,
) -> None:
    """A Slack turn carrying its own ``endpoint`` is the pre-ADR custom-transport
    form (D4.4): its ``adapter`` is a credential slug, not an identity
    (``aci_protocol.turn.slack_speaking_identity``), so a policy card that turn
    triggers must still speak as ``default`` -- it must never borrow that slug
    the way the identity-form fix above does.
    """

    async def go() -> None:
        capture = _Capture()
        server = TestServer(capture.app)
        await server.start_server()
        try:
            port = server.port
            assert port is not None
            endpoint = f"http://127.0.0.1:{port}/slack/api/"
            binding = _TripleBinding(
                {("slack", "some-credential-slug", _CHANNEL): _routed_resolved(
                    "some-credential-slug"
                )}
            )
            approvals = _RecordingApprovals()
            async with make_harness(binding=binding, sink=_sink(port), approvals=approvals) as h:
                h.runner.default_script = _awaiting_routed_script("needs sign-off")
                ev = QueuedTurn(
                    event_id=uuid.uuid4().hex,
                    conversation_id="t-custom-transport",
                    author="U1",
                    text="please",
                    reply_handle=ReplyHandle(
                        kind="slack",
                        channel=_CHANNEL,
                        placeholder="1720000000.000100",
                        adapter="some-credential-slug",
                        endpoint=endpoint,
                    ),
                    received_at="2026-07-05T00:00:00+00:00",
                )
                await h.kernel.process_event(ev)

                assert len(approvals.requests) == 1
            assert "chat.postMessage" in capture.methods()
            assert capture.tokens() == {f"Bearer {_DEFAULT_TOKEN}"}
        finally:
            await server.close()

    asyncio.run(go())
