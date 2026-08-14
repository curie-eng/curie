"""Respond-in-kind holds when ONE agent is reachable on two channels (#1525, AC3).

BASELINE-GREEN, and that is the point. ``kernel._target_for`` is already pure and
derived wholly from the ``QueuedTurn``, so a reply already goes back to the
channel its turn arrived on. This file characterizes that property under the
condition multi-binding creates -- two live turns for the SAME agent whose only
difference is which door they came through -- so that any later attempt to derive
a reply address from the AGENT (its binding row, its "primary" channel, a cached
last-seen address) turns this suite red instead of quietly cross-posting one
customer's answer into another channel.

The two turns are driven CONCURRENTLY and their completion order is deliberately
inverted (the second turn finishes first, while the first is mid-flight), because
sequential turns cannot catch the failure this guards: a per-agent reply target is
correct-by-accident whenever only one turn is in flight.

New file rather than an addition to ``test_kernel.py`` on purpose (the kernel is
the sacred module and that file is under concurrent edit).

Real Valkey, the real substrate, a fake runner; the only doubles are the binding
resolver and one recording sink per kind.
"""

from __future__ import annotations

import asyncio
import itertools
import time
import uuid
from collections.abc import Callable

from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus, TextDelta
from channel_protocol.reply import (
    ReplyAck,
    ReplyEvent,
    ReplyUpdate,
    TurnCompleted,
)
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.binding import BUDGET_ENV, BUNDLE_REF_ENV, ResolvedDeployment
from curie_worker.reply_sink import ReplySinkRouter, TargetRoute

DONE = SessionStatus.DONE

# One agent, two doors. The Slack half is a placeholder channel id; the second
# door is a different KIND as well as a different address, which is what puts a
# separate recording sink behind it (the router is the only switch on kind).
SLACK_ADDRESS = "C0EXAMPLE1"
EMAIL_ADDRESS = "ops@example.com"
EMAIL_ENDPOINT = "https://adapter.example/hook"
EMAIL_ADAPTER = "agentmail-sandbox"

SLACK_ANSWER = "answer for the slack door"
EMAIL_ANSWER = "answer for the email door"


class OneAgentTwoBindings:
    """A ``BindingResolver``-shaped double: two pairs, ONE agent and version.

    The whole hazard lives here. Both lookups return the same
    ``ResolvedDeployment``, so nothing downstream can tell the two turns apart by
    their agent -- only by their own ``reply_handle``. A kernel that reached for
    the agent to decide where to reply would have exactly one answer to give and
    would give it to both turns.
    """

    def __init__(self) -> None:
        self.agent_id = uuid.uuid4()
        self.version_id = uuid.uuid4()
        self.resolve_calls: list[tuple[str, str]] = []

    def _deployment(self, endpoint: str | None, adapter: str | None) -> ResolvedDeployment:
        return ResolvedDeployment(
            agent_id=self.agent_id,
            agent_name="multi-bound-agent",
            version_id=self.version_id,
            version_label="v1",
            bundle_ref="bundles/x.zip",
            max_usd_per_day=None,
            max_output_tokens_per_run=None,
            behavior_packs=None,
            endpoint=endpoint,
            adapter=adapter,
        )

    async def resolve(self, kind: str, address: str) -> ResolvedDeployment | None:
        self.resolve_calls.append((kind, address))
        if (kind, address) == ("slack", SLACK_ADDRESS):
            return self._deployment(None, None)
        if (kind, address) == ("email", EMAIL_ADDRESS):
            return self._deployment(EMAIL_ENDPOINT, EMAIL_ADAPTER)
        return None

    def boot_env(self, resolved: ResolvedDeployment, thread_key: str) -> dict[str, str]:
        return {
            BUDGET_ENV: '{"max_output_tokens_per_run":100000,"max_usd_per_day":10.0}',
            BUNDLE_REF_ENV: resolved.bundle_ref or "",
        }

    def packs_for(self, resolved: ResolvedDeployment) -> BehaviorPacks:
        return BehaviorPacks.from_config(resolved.behavior_packs)


class RecordingSink:
    """Records every event one kind's adapter was handed, in a SHARED order log.

    The shared ``sequence`` counter is what makes the interleaving assertable:
    two per-sink lists can say what each door saw but not which door saw it
    first, and "the second turn completed while the first was still open" is the
    property that distinguishes this from two sequential turns.

    ``gate`` optionally blocks one specific event until an ``asyncio.Event`` is
    set, which is how the completion order is inverted from the sink side rather
    than by pre-sequencing the two ``process_event`` calls.
    """

    def __init__(self, kind: str, sequence: itertools.count, log: list[tuple[int, str, str]]):
        self.kind = kind
        self._sequence = sequence
        self._log = log
        self.events: list[ReplyEvent] = []
        self.routes: list[TargetRoute] = []
        self.texts: list[str] = []
        self.completions: list[TurnCompleted] = []
        # (predicate, event to wait on, seconds); the wait is recorded rather
        # than raising, so a gate that never opens fails an assertion with a
        # readable message instead of dead-lettering the turn.
        self.gate: tuple[Callable[[ReplyEvent], bool], asyncio.Event, float] | None = None
        self.gate_timed_out = False

    async def emit(
        self,
        event: ReplyEvent,
        *,
        route: TargetRoute,
        best_effort_unreachable: bool = False,
    ) -> ReplyAck:
        if self.gate is not None:
            predicate, opened, timeout = self.gate
            if predicate(event):
                try:
                    await asyncio.wait_for(opened.wait(), timeout)
                except TimeoutError:
                    self.gate_timed_out = True
        self.events.append(event)
        self.routes.append(route)
        self._log.append((next(self._sequence), self.kind, event.event))
        if isinstance(event, ReplyUpdate) and event.text is not None:
            self.texts.append(event.text)
        if isinstance(event, TurnCompleted):
            self.completions.append(event)
        return ReplyAck(ref=event.target.reply_ref)


class RefusingSink:
    """The router's default: reached only if a kind lost its own sink."""

    async def emit(
        self,
        event: ReplyEvent,
        *,
        route: TargetRoute,
        best_effort_unreachable: bool = False,
    ) -> ReplyAck:
        raise AssertionError(
            f"event {event.event} for kind {event.target.kind!r} fell through to the default sink"
        )


def _qevent(
    text: str,
    *,
    kind: str,
    channel: str,
    thread: str,
    placeholder: str,
    endpoint: str | None = None,
    adapter: str | None = None,
) -> QueuedTurn:
    return QueuedTurn(
        event_id=uuid.uuid4().hex,
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


async def _wait_until(pred: Callable[[], bool], what: str, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"timed out waiting for: {what}")


def test_two_concurrent_turns_for_one_agent_each_reply_to_their_own_channel(
    make_harness,
) -> None:
    async def go() -> None:
        order: list[tuple[int, str, str]] = []
        sequence = itertools.count()
        slack_sink = RecordingSink("slack", sequence, order)
        email_sink = RecordingSink("email", sequence, order)
        router = ReplySinkRouter(
            adapters={"slack": slack_sink, "email": email_sink}, default=RefusingSink()
        )
        binding = OneAgentTwoBindings()

        async with make_harness(binding=binding, sink=router) as h:
            # Two scripts, consumed FIFO by the fake runner. The Slack turn is
            # started and confirmed open first, so it takes the first script;
            # the email turn then runs alongside it and takes the second.
            h.runner.turn_scripts = [
                [TextDelta(text="slack "), Final(text=SLACK_ANSWER, status=DONE)],
                [TextDelta(text="email "), Final(text=EMAIL_ANSWER, status=DONE)],
            ]

            email_done = asyncio.Event()
            # The interleave: the Slack door cannot deliver its answer (nor
            # complete) until the email door's turn has completed. Both turns are
            # in flight across that window, which is the condition a per-agent
            # reply target would get wrong.
            slack_sink.gate = (
                lambda ev: (isinstance(ev, ReplyUpdate) and ev.text == SLACK_ANSWER)
                or isinstance(ev, TurnCompleted),
                email_done,
                20.0,
            )

            slack_turn = _qevent(
                "hi from slack",
                kind="slack",
                channel=SLACK_ADDRESS,
                thread="th-slack",
                placeholder="ph-slack",
            )
            email_turn = _qevent(
                "hi from email",
                kind="email",
                channel=EMAIL_ADDRESS,
                thread="th-email",
                placeholder="ph-email",
                endpoint=EMAIL_ENDPOINT,
                adapter=EMAIL_ADAPTER,
            )

            slack_task = asyncio.create_task(h.kernel.process_event(slack_turn))
            await _wait_until(
                lambda: "hi from slack" in h.runner.opened, "the slack turn to open"
            )
            email_task = asyncio.create_task(h.kernel.process_event(email_turn))

            await _wait_until(
                lambda: bool(email_sink.completions),
                "the email door's sink to see turn.completed (nothing reached it: a "
                "reply addressed off the AGENT would have sent both turns to one door)",
            )
            assert not slack_sink.completions, (
                "the slack turn completed before the email turn; the interleave "
                "this test depends on did not happen"
            )
            email_done.set()
            await asyncio.gather(slack_task, email_task)

            assert not slack_sink.gate_timed_out, "the slack gate never opened"

            # 1. Neither door fell through to the default sink, and both were used.
            assert slack_sink.events and email_sink.events

            # 2. Every event a sink saw is addressed to ITS OWN turn: kind,
            #    address, conversation and the opaque reply ref, all four.
            for sink, turn in ((slack_sink, slack_turn), (email_sink, email_turn)):
                handle = turn.reply_handle
                for event in sink.events:
                    target = event.target
                    assert target.kind == handle.kind, event
                    assert target.address == handle.channel, event
                    assert target.conversation_id == turn.conversation_id, event
                    assert target.reply_ref == handle.placeholder, event

            # 3. Neither sink saw the other's text.
            assert SLACK_ANSWER in slack_sink.texts
            assert EMAIL_ANSWER in email_sink.texts
            assert EMAIL_ANSWER not in slack_sink.texts
            assert SLACK_ANSWER not in email_sink.texts

            # 4. Each turn completed for its own event id, and the email turn
            #    completed FIRST -- proving the assertions above held while both
            #    turns were open, not merely across two sequential turns.
            assert [c.event_id for c in slack_sink.completions] == [slack_turn.event_id]
            assert [c.event_id for c in email_sink.completions] == [email_turn.event_id]
            completions = [(seq, kind) for seq, kind, name in order if name == "turn.completed"]
            assert [kind for _, kind in completions] == ["email", "slack"]

            # 5. The egress route is the turn's own too: the email door's replies
            #    carry its endpoint and adapter, the Slack door's carry neither.
            assert {(r.endpoint, r.adapter) for r in email_sink.routes} == {
                (EMAIL_ENDPOINT, EMAIL_ADAPTER)
            }
            assert {(r.endpoint, r.adapter) for r in slack_sink.routes} == {(None, None)}

            # 6. Both turns are durably done, and both resolved against their own
            #    pair rather than one lookup being reused for the other.
            assert await h.async_redis.exists(h.config.done_key(slack_turn.event_id))
            assert await h.async_redis.exists(h.config.done_key(email_turn.event_id))
            assert set(binding.resolve_calls) == {
                ("slack", SLACK_ADDRESS),
                ("email", EMAIL_ADDRESS),
            }

    asyncio.run(go())
