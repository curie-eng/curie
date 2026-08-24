"""The kernel speaks the neutral reply wire and nothing else.

Stream B, ADR-0096 phase 2. Covers T-B2 (the S1 tracer: one turn through a sink
that can only append, and the same turn through the Slack adapter), T-B3 (the
kernel suite runs with no Slack adapter registered at all, D3), T-B11
(navigation survives the wire, finding 16) and T-B15 (the two pre-resolution
paths: the already-done skip is silent, the prior-side-effect escalate is NOT).

Real Valkey, the real substrate, a fake runner. The Slack halves talk HTTP to an
in-process capture server so "what the adapter actually sent" is read off the
wire rather than off a patched method.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path

from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus, TextDelta
from aiohttp import web
from aiohttp.test_utils import TestServer
from channel_protocol.reply import (
    NavAffordance,
    ReplyAck,
    ReplyEvent,
    ReplyPost,
    ReplyUpdate,
    TurnCompleted,
)
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.binding import (
    BUDGET_ENV,
    BUNDLE_REF_ENV,
    PLUGIN_DIR_ENV,
    ResolvedDeployment,
)
from curie_worker.config import WorkerConfig
from curie_worker.reply_sink import ReplySinkRouter, TargetRoute, build_reply_sink

DONE = SessionStatus.DONE

ADAPTER = "agentmail-sandbox"
SECRET = "secret-for-agentmail-sandbox"
EMAIL_ADDRESS = "agent@example.test"


# --- doubles ------------------------------------------------------------------


class StubBinding:
    """A BindingResolver-shaped stub keyed on the ``(kind, address)`` pair.

    Records every ``resolve`` call so a test can assert a path did NOT look the
    binding up (T-B15(b)): the pre-resolution escalate must build its route from
    the server-minted reply handle, not from a lookup it cannot reach.
    """

    def __init__(self, by_route: dict[tuple[str, str], ResolvedDeployment]) -> None:
        self._by_route = by_route
        self.resolve_calls: list[tuple[str, str]] = []

    async def resolve(self, kind: str, address: str) -> ResolvedDeployment | None:
        self.resolve_calls.append((kind, address))
        return self._by_route.get((kind, address))

    def boot_env(
        self,
        resolved: ResolvedDeployment,
        thread_key: str,
        *,
        kind: str | None = None,
        address: str | None = None,
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


def _resolved(
    *,
    packs: dict[str, object] | None = None,
    endpoint: str | None = None,
    adapter: str | None = None,
) -> ResolvedDeployment:
    return ResolvedDeployment(
        agent_id=uuid.uuid4(),
        agent_name="test-agent",
        version_id=uuid.uuid4(),
        version_label="v1",
        bundle_ref="bundles/x.zip",
        max_usd_per_day=None,
        max_output_tokens_per_run=None,
        behavior_packs=packs,
        endpoint=endpoint,
        adapter=adapter,
    )


class AppendOnlyAdapter:
    """A sink that can only APPEND -- it implements reply.post and turn.completed.

    The S1 tracer's whole point (T-B2): a channel with no edit-in-place
    affordance (email, a transcript, a webhook log) must be able to carry a turn
    through the same seam a Slack workspace does. It has no ``ts`` to mint and no
    message to edit, so every reply lands as a new transcript line.
    """

    def __init__(self) -> None:
        self.transcript: list[str] = []
        self.completions: list[TurnCompleted] = []
        self.routes: list[TargetRoute] = []

    async def emit(
        self,
        event: ReplyEvent,
        *,
        route: TargetRoute,
        best_effort_unreachable: bool = False,
    ) -> ReplyAck:
        self.routes.append(route)
        if isinstance(event, ReplyUpdate) and event.text is not None:
            self.transcript.append(event.text)
        elif isinstance(event, ReplyPost):
            self.transcript.append(event.message.text)
        elif isinstance(event, TurnCompleted):
            self.completions.append(event)
        # No ref: this channel mints no addressable handle at all, which is the
        # opacity D2 buys -- the kernel must not care.
        return ReplyAck(ref=None)


class _Capture:
    """An in-process HTTP endpoint that records what reached it."""

    def __init__(self) -> None:
        self.requests: list[dict[str, str]] = []
        self.app = web.Application()
        self.app.add_routes([web.post("/slack/api/{method}", self._slack)])

    async def _slack(self, request: web.Request) -> web.Response:
        self.requests.append(
            {"path": request.path, "body": await request.text()}
        )
        return web.json_response({"ok": True, "ts": "1720000000.000200"})

    def methods(self) -> list[str]:
        return [r["path"].rsplit("/", 1)[-1] for r in self.requests]


def _qevent(
    text: str,
    *,
    kind: str = "slack",
    channel: str = "C1",
    thread: str = "th-1",
    placeholder: str = "p-1",
    endpoint: str | None = None,
    adapter: str | None = None,
    event_id: str | None = None,
) -> QueuedTurn:
    return QueuedTurn(
        event_id=event_id or uuid.uuid4().hex,
        conversation_id=thread,
        author="U1",
        text=text,
        reply_handle=ReplyHandle(
            kind=kind,
            channel=channel,
            placeholder=placeholder,
            endpoint=endpoint,
            adapter=adapter,
        ),
        received_at="2026-07-05T00:00:00+00:00",
    )


# --- T-B2: the S1 tracer ------------------------------------------------------


def test_a_turn_completes_through_an_append_only_sink(make_harness) -> None:
    # Spike S1's question, answered as a commitment (D3): the kernel holds ONE
    # ``ReplySink`` and never asks what the channel can do. A sink with no edit
    # affordance still carries a turn to a terminal outcome.
    async def go() -> None:
        adapter = AppendOnlyAdapter()
        binding = StubBinding({("email", EMAIL_ADDRESS): _resolved()})
        async with make_harness(binding=binding, sink=adapter) as h:
            h.runner.default_script = [
                TextDelta(text="Hello "),
                Final(text="Hello world", status=DONE),
            ]
            ev = _qevent(
                "hi",
                kind="email",
                channel=EMAIL_ADDRESS,
                thread="tS1",
                endpoint="https://adapter.example/hook",
                adapter=ADAPTER,
            )
            await h.kernel.process_event(ev)

            assert adapter.transcript[-1] == "Hello world"
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))
            assert [c.outcome for c in adapter.completions] == ["delivered"]
            assert adapter.completions[0].event_id == ev.event_id

    asyncio.run(go())


def test_the_same_turn_through_the_slack_adapter_edits_exactly_one_message(
    make_harness,
) -> None:
    # The other half of the tracer: the neutral wire did not cost Slack its
    # edit-in-place reply model. Every update lands as a ``chat.update`` on the
    # ONE placeholder the ingress pre-posted -- no new messages, no second
    # message id.
    async def go() -> None:
        capture = _Capture()
        server = TestServer(capture.app)
        await server.start_server()
        try:
            port = server.port
            assert port is not None
            sink = build_reply_sink(
                WorkerConfig(
                    slack_bot_token="xoxb-test",
                    slack_api_base_url=f"http://127.0.0.1:{port}/slack/api/",
                )
            )
            binding = StubBinding({("slack", "C1"): _resolved()})
            async with make_harness(
                binding=binding, sink=sink, shimmer=False
            ) as h:
                h.runner.default_script = [
                    TextDelta(text="Hello "),
                    Final(text="Hello world", status=DONE),
                ]
                await h.kernel.process_event(_qevent("hi", thread="tS1b"))

            assert set(capture.methods()) == {"chat.update"}
            assert "chat.postMessage" not in capture.methods()
            # One target message, and it is the placeholder the ingress
            # pre-posted -- not merely "one distinct id", which a worker that
            # minted its own single id would also satisfy. slack_sdk sends the
            # call as a JSON body, so the target is read as a key rather than
            # scanned for out of a form-encoded string.
            assert capture.requests
            timestamps = {json.loads(r["body"])["ts"] for r in capture.requests}
            assert timestamps == {"p-1"}
        finally:
            await server.close()

    asyncio.run(go())


# --- T-B3: no Slack adapter registered at all ---------------------------------


def test_a_turn_completes_with_no_slack_adapter_registered(make_harness) -> None:
    # D3's proof, and the reason it is a green suite rather than a grep: the
    # router is the ONLY place ``kind`` is switched on, so a router that has
    # never heard of Slack must still run a turn end to end.
    async def go() -> None:
        adapter = AppendOnlyAdapter()
        router = ReplySinkRouter(adapters={}, default=adapter)
        binding = StubBinding({("email", EMAIL_ADDRESS): _resolved()})
        async with make_harness(binding=binding, sink=router) as h:
            h.runner.default_script = [Final(text="answer", status=DONE)]
            ev = _qevent(
                "hi",
                kind="email",
                channel=EMAIL_ADDRESS,
                thread="tB3",
                endpoint="https://adapter.example/hook",
                adapter=ADAPTER,
            )
            await h.kernel.process_event(ev)

            assert adapter.transcript[-1] == "answer"
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_the_kernel_source_carries_no_kind_branch() -> None:
    # Corroboration only -- the green suite above is the proof. Kept because the
    # regression it guards is textual: a ``kind ==`` re-appearing in the kernel
    # is the seam leaking back upward, and it would pass every behavioral test
    # that only ever exercises one kind.
    source = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "curie_worker"
        / "kernel.py"
    ).read_text(encoding="utf-8")
    assert "kind ==" not in source
    assert '== "slack"' not in source


# --- the two sanctioned route sources, and their seam -------------------------


def test_a_resolved_turn_routes_through_the_binding_rows_endpoint(make_harness) -> None:
    # EB-B2's FIRST sanctioned source, and the normal path: once a turn has
    # resolved, its ``TargetRoute`` comes from the binding row (D4.1 --
    # endpoints are server-controlled). The sibling of T-B15(b), which covers
    # the SECOND source (the server-minted reply handle) on the pre-resolution
    # paths. Naming both here is what keeps the seam visible: a change that
    # collapses them into one source silently re-opens whichever it dropped.
    async def go() -> None:
        resolved = _resolved(
            endpoint="https://row.example/hook", adapter="row-adapter"
        )
        binding = StubBinding({("email", EMAIL_ADDRESS): resolved})
        async with make_harness(binding=binding, shimmer=False) as h:
            h.runner.default_script = [Final(text="answer", status=DONE)]
            await h.kernel.process_event(
                _qevent(
                    "hi",
                    kind="email",
                    channel=EMAIL_ADDRESS,
                    thread="tRow",
                    endpoint="https://stale.example/hook",
                    adapter="stale-adapter",
                )
            )
            route = h.sink.routes_for("reply.update")[-1]
            assert route.endpoint == "https://row.example/hook"
            assert route.adapter == "row-adapter"

    asyncio.run(go())


# --- T-B11: navigation survives the wire --------------------------------------


def test_an_enabled_nav_pack_reaches_the_wire_as_an_affordance(make_harness) -> None:
    # Finding 16: revision 0 said "NavPack moves onto ReplyUpdate" while omitting
    # it from the model list, which would have either inverted the package
    # dependency or silently dropped navigation. The worker's sink layer maps
    # ``NavPack`` -> ``NavAffordance``.
    # Mutation: drop the mapping and this fails.
    async def go() -> None:
        packs = {"nav": {"enabled": True, "hub_label": "Help", "hub_command": "help"}}
        binding = StubBinding({("slack", "C-bound"): _resolved(packs=packs)})
        async with make_harness(binding=binding) as h:
            h.runner.default_script = [Final(text="here you go", status=DONE)]
            await h.kernel.process_event(
                _qevent("hi", channel="C-bound", thread="tNavW")
            )
            assert h.sink.last_nav == NavAffordance(label="Help", command="help")

    asyncio.run(go())


def test_a_disabled_nav_pack_puts_no_affordance_on_the_wire(make_harness) -> None:
    # The sibling half: an agent with nav off must carry NO affordance, not an
    # empty-labelled one. A disabled pack that still crossed as an affordance
    # would render a dead hub button on every reply.
    async def go() -> None:
        binding = StubBinding({("slack", "C-plain"): _resolved(packs={})})
        async with make_harness(binding=binding) as h:
            h.runner.default_script = [Final(text="here you go", status=DONE)]
            await h.kernel.process_event(
                _qevent("hi", channel="C-plain", thread="tNavOff")
            )
            assert h.sink.last_nav is None

    asyncio.run(go())


def test_the_slack_adapter_renders_the_hub_button_from_the_affordance() -> None:
    # The other end of the mapping: the affordance is not decoration on the wire,
    # it is what the Slack adapter renders the no-dead-ends hub button from.
    async def go() -> None:
        capture = _Capture()
        server = TestServer(capture.app)
        await server.start_server()
        try:
            port = server.port
            assert port is not None
            sink = build_reply_sink(
                WorkerConfig(
                    slack_bot_token="xoxb-test",
                    slack_api_base_url=f"http://127.0.0.1:{port}/slack/api/",
                )
            )
            from channel_protocol.reply import REPLY_WIRE_VERSION, ReplyTarget

            structured = (
                "```curie-reply\n"
                '{"text": "here you go", "buttons": [["Details", "details"]]}\n'
                "```"
            )
            await sink.emit(
                ReplyUpdate(
                    version=REPLY_WIRE_VERSION,
                    event="reply.update",
                    target=ReplyTarget(
                        kind="slack",
                        address="C1",
                        conversation_id="t-1",
                        reply_ref="p-1",
                    ),
                    text=structured,
                    nav=NavAffordance(label="Help", command="help"),
                ),
                route=TargetRoute(endpoint=None, adapter=None),
            )
            assert len(capture.requests) == 1
            body = capture.requests[0]["body"]
            assert "Help" in body
            assert "help" in body
        finally:
            await server.close()

    asyncio.run(go())


# --- T-B15: the two pre-resolution paths --------------------------------------


def test_the_already_done_skip_makes_no_sink_call(make_harness) -> None:
    # T-B15(a). The skip at kernel.py:372-374 returns before anything else. With
    # no pending completion record it has nothing to hand off, so it emits
    # nothing at all -- it never had a resolved route and must not invent one.
    #
    # This is the HALF of T-B15 that stays silent, and the distinction is not
    # "pre-resolution paths are silent": the side-effect escalate below is also
    # pre-resolution and DOES emit. The line is whether the path reaches a NEW
    # terminal outcome. The escalate does (it marks done here, now), so it owes
    # both the message and the completion. The already-done skip does not -- the
    # outcome was reached by an earlier delivery, which already emitted or left a
    # record for the sweeper, so emitting here would be a duplicate this kernel
    # invented rather than one the outbox owed.
    async def go() -> None:
        binding = StubBinding({("email", EMAIL_ADDRESS): _resolved()})
        async with make_harness(binding=binding, shimmer=False) as h:
            ev = _qevent(
                "hi",
                kind="email",
                channel=EMAIL_ADDRESS,
                thread="tDone",
                endpoint="https://adapter.example/hook",
                adapter=ADAPTER,
            )
            await h.async_redis.set(h.config.done_key(ev.event_id), "1")

            await h.kernel.process_event(ev)

            assert h.sink.events == []
            assert h.sink.completions == []
            assert h.runner.opened == []
            # No record was written either: nothing to sweep, nothing to re-emit.
            assert (
                await h.async_redis.smembers(h.config.completions_pending_key()) == set()
            )

    asyncio.run(go())


def test_the_already_done_skip_still_lowers_the_shimmer(make_harness) -> None:
    # The deliberate exception, preserved: the terminal ``finally`` clears the
    # caption unconditionally (kernel.py:531-548), including on the already-done
    # path, because clearing a caption that was never raised is a no-op and the
    # alternative is tracking "did we set it" across every early return on the
    # sacred path. Pinned so the neutral rewrite does not quietly drop it.
    async def go() -> None:
        binding = StubBinding({("slack", "C1"): _resolved()})
        async with make_harness(binding=binding, shimmer=True) as h:
            ev = _qevent("hi", thread="tDoneShim")
            await h.async_redis.set(h.config.done_key(ev.event_id), "1")

            await h.kernel.process_event(ev)

            assert [e.event for e, _r, _b in h.sink.events] == ["turn.status"]
            assert h.sink.status_clears == [("C1", "tDoneShim")]
            assert h.sink.completions == []

    asyncio.run(go())


def test_the_prior_side_effect_escalate_emits_through_the_reply_handle_route(
    make_harness,
) -> None:
    # T-B15(b). This is the "a prior attempt started an action before the worker
    # restarted" escalation -- the one message a human most needs to see -- and
    # revision 3's shape suppressed it. It runs BEFORE binding resolution, so its
    # route comes from the server-minted ``reply_handle`` (EB-B6's trust
    # argument: after phase 2 no adapter writes to the stream at all).
    #
    # Driver adjudication (supersedes AC10f's original "suppresses turn.completed"
    # wording): this path calls ``mark_done`` at kernel.py:389, so it IS a durable
    # terminal outcome and owes a completion. Emitting the escalation text without
    # it is only correct for an edit-in-place channel like Slack; a BUFFERED
    # adapter -- email being the whole reason this phase exists -- accumulates the
    # reply and sends on ``turn.completed``, so a suppressed completion means the
    # escalation is written, marked done, and never delivered. That is strictly
    # worse than the duplicate the suppression was avoiding, and it fails silently
    # on exactly the channel with no human watching a thread.
    # Mutation: make this path silent, OR emit the text without the completion,
    # and it fails.
    async def go() -> None:
        binding = StubBinding({("email", EMAIL_ADDRESS): _resolved()})
        async with make_harness(binding=binding, shimmer=False) as h:
            ev = _qevent(
                "hi",
                kind="email",
                channel=EMAIL_ADDRESS,
                thread="tSideFx",
                endpoint="https://adapter.example/hook",
                adapter=ADAPTER,
            )
            await h.async_redis.set(h.config.side_effect_key(ev.event_id), "1")

            await h.kernel.process_event(ev)

            assert len(h.sink.updates) == 1
            assert "prior attempt started an action" in h.sink.updates[0][2]
            # The route is the reply handle's, verbatim -- not a lookup, not a
            # default, and never the Slack credential standing in for a missing
            # adapter (the second mutation named in AC10f).
            route = h.sink.routes_for("reply.update")[0]
            assert route.endpoint == "https://adapter.example/hook"
            assert route.adapter == ADAPTER
            # No binding lookup happened: the resolver was never reached.
            assert binding.resolve_calls == []

            # EXACTLY ONE completion, so a buffered adapter flushes the
            # escalation instead of holding it forever. One, not "at least one":
            # this path runs before the retry loop, so a second completion here
            # would be a duplicate the kernel manufactured rather than one the
            # at-least-once outbox is entitled to.
            assert len(h.sink.completions) == 1
            completed = h.sink.completions[0]
            assert completed.event_id == ev.event_id
            # ``TurnCompleted.outcome`` is where escalation is reported on this
            # wire (reply.py:131-133's Literal). ``SettledOutcome`` is the
            # approval-card shape carried on ``reply.update`` and has no role
            # here -- nothing was decided, the turn escalated.
            assert completed.outcome == "escalated"
            assert completed.target.address == EMAIL_ADDRESS
            # Same reply_handle-derived route as the escalation text: a
            # completion delivered to a different transport than the message it
            # completes would flush a buffer that never received the text.
            assert h.sink.routes_for("turn.completed") == [route]

            assert await h.async_redis.exists(h.config.done_key(ev.event_id))
            # The outbox record was cleared, which only happens after a CONFIRMED
            # emit (EB-B6 step 4) -- the completion really was delivered, not just
            # queued.
            assert (
                await h.async_redis.smembers(h.config.completions_pending_key()) == set()
            )

    asyncio.run(go())


def test_a_side_effect_escalate_never_substitutes_a_route_it_was_not_given(
    make_harness,
) -> None:
    # The mutation guard, stated as its own case: a reply handle carrying no
    # endpoint/adapter must reach the sink AS SUCH. Defaulting to the worker's
    # Slack credential would send the platform bot token to an email adapter's
    # egress, which is D4.4's leak wearing a different hat.
    async def go() -> None:
        binding = StubBinding({("email", EMAIL_ADDRESS): _resolved()})
        async with make_harness(binding=binding, shimmer=False) as h:
            ev = _qevent(
                "hi",
                kind="email",
                channel=EMAIL_ADDRESS,
                thread="tSideFxBare",
            )
            await h.async_redis.set(h.config.side_effect_key(ev.event_id), "1")

            await h.kernel.process_event(ev)

            route = h.sink.routes_for("reply.update")[0]
            assert route.endpoint is None
            assert route.adapter is None

    asyncio.run(go())


def test_the_escalation_target_names_the_turns_own_address(make_harness) -> None:
    # The escalation is delivered to the address the turn arrived on, with the
    # opaque reply_ref the ingress minted -- never a Slack-shaped id the worker
    # invented (D2).
    async def go() -> None:
        binding = StubBinding({("email", EMAIL_ADDRESS): _resolved()})
        async with make_harness(binding=binding, shimmer=False) as h:
            ev = _qevent(
                "hi",
                kind="email",
                channel=EMAIL_ADDRESS,
                thread="tSideFxTarget",
                placeholder="msg_upstream_id",
                endpoint="https://adapter.example/hook",
                adapter=ADAPTER,
            )
            await h.async_redis.set(h.config.side_effect_key(ev.event_id), "1")

            await h.kernel.process_event(ev)

            event, _route, _best_effort = h.sink.events[0]
            assert event.target.kind == "email"
            assert event.target.address == EMAIL_ADDRESS
            assert event.target.conversation_id == "tSideFxTarget"
            assert event.target.reply_ref == "msg_upstream_id"

    asyncio.run(go())


def test_json_dumps_of_a_route_is_not_on_the_wire(make_harness) -> None:
    # A last shape check at the kernel boundary (EB-B2): whatever the kernel
    # emits, the serialized event must not carry the endpoint or the adapter.
    # Asserted here, at the producer, as well as on the model (T-B1), because
    # this is where a well-meaning "include the route for debugging" would land.
    async def go() -> None:
        binding = StubBinding({("email", EMAIL_ADDRESS): _resolved()})
        async with make_harness(binding=binding, shimmer=False) as h:
            h.runner.default_script = [Final(text="answer", status=DONE)]
            await h.kernel.process_event(
                _qevent(
                    "hi",
                    kind="email",
                    channel=EMAIL_ADDRESS,
                    thread="tWire",
                    endpoint="https://adapter.example/hook",
                    adapter=ADAPTER,
                )
            )
            assert h.sink.events
            for event, _route, _best_effort in h.sink.events:
                body = json.loads(event.model_dump_json())
                assert "https://adapter.example/hook" not in json.dumps(body)
                assert ADAPTER not in json.dumps(body)

    asyncio.run(go())
