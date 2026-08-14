"""Respond-in-kind holds when ONE agent is reachable on two channels (#1525, AC3).

BASELINE-GREEN, and that is the point. ``kernel._target_for`` is already pure and
derived wholly from the ``QueuedTurn``, so a reply already goes back to the
channel its turn arrived on. This file characterizes that property under the
condition multi-binding creates -- two live turns for the SAME agent whose only
difference is which door they came through -- so that any later attempt to derive
a reply address from the AGENT (its binding row, its "primary" channel, a cached
last-seen address) turns this suite red instead of quietly cross-posting one
customer's answer into another channel.

Both doors are Slack channels of the one agent, which is the multi-binding case
AC3 names: the two turns are distinguishable ONLY by their own reply handle, not
by kind, endpoint, or adapter. Nothing below the egress seam can tell them apart,
so one recording sink sees both and the address on each event is the whole
assertion.

The two turns are driven CONCURRENTLY and their completion order is deliberately
inverted (the second turn finishes first, while the first is mid-flight), because
sequential turns cannot catch the failure this guards: a per-agent reply target is
correct-by-accident whenever only one turn is in flight.

New file rather than an addition to ``test_kernel.py`` on purpose (the kernel is
the sacred module and that file is under concurrent edit).

Real Valkey, the real substrate, a fake runner; the only doubles are the binding
resolver and one recording sink.
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
from curie_worker.reply_sink import TargetRoute

DONE = SessionStatus.DONE

# One agent, two doors: two Slack channels, placeholder channel ids.
CHANNEL_A = "C0EXAMPLE1"
CHANNEL_B = "C0EXAMPLE2"

ANSWER_A = "answer for the first channel"
ANSWER_B = "answer for the second channel"


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

    def _deployment(self) -> ResolvedDeployment:
        return ResolvedDeployment(
            agent_id=self.agent_id,
            agent_name="multi-bound-agent",
            version_id=self.version_id,
            version_label="v1",
            bundle_ref="bundles/x.zip",
            max_usd_per_day=None,
            max_output_tokens_per_run=None,
            behavior_packs=None,
            endpoint=None,
            adapter=None,
        )

    async def resolve(self, kind: str, address: str) -> ResolvedDeployment | None:
        self.resolve_calls.append((kind, address))
        if (kind, address) in (("slack", CHANNEL_A), ("slack", CHANNEL_B)):
            return self._deployment()
        return None

    def boot_env(self, resolved: ResolvedDeployment, thread_key: str) -> dict[str, str]:
        return {
            BUDGET_ENV: '{"max_output_tokens_per_run":100000,"max_usd_per_day":10.0}',
            BUNDLE_REF_ENV: resolved.bundle_ref or "",
        }

    def packs_for(self, resolved: ResolvedDeployment) -> BehaviorPacks:
        return BehaviorPacks.from_config(resolved.behavior_packs)


class RecordingSink:
    """Records every event the egress seam was handed, in one ordered log.

    A single sink for both doors is the point: below the seam the two turns are
    the same agent on the same kind, so the only thing separating them is the
    address on each event. The shared ``sequence`` counter is what makes the
    interleaving assertable -- "the second turn completed while the first was
    still open" is the property that distinguishes this from two sequential
    turns.

    ``gate`` optionally blocks one specific event until an ``asyncio.Event`` is
    set, which is how the completion order is inverted from the sink side rather
    than by pre-sequencing the two ``process_event`` calls.
    """

    def __init__(self) -> None:
        self._sequence = itertools.count()
        self.events: list[ReplyEvent] = []
        self.log: list[tuple[int, str, str]] = []
        # (predicate, event to wait on, seconds); the wait is recorded rather
        # than raising, so a gate that never opens fails an assertion with a
        # readable message instead of dead-lettering the turn.
        self.gate: tuple[Callable[[ReplyEvent], bool], asyncio.Event, float] | None = None
        self.gate_timed_out = False

    def events_for(self, address: str) -> list[ReplyEvent]:
        return [e for e in self.events if e.target.address == address]

    def texts_for(self, address: str) -> list[str]:
        return [
            e.text
            for e in self.events_for(address)
            if isinstance(e, ReplyUpdate) and e.text is not None
        ]

    def completions_for(self, address: str) -> list[TurnCompleted]:
        return [e for e in self.events_for(address) if isinstance(e, TurnCompleted)]

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
        self.log.append((next(self._sequence), event.target.address, event.event))
        return ReplyAck(ref=event.target.reply_ref)


def _qevent(text: str, *, channel: str, thread: str, placeholder: str) -> QueuedTurn:
    return QueuedTurn(
        event_id=uuid.uuid4().hex,
        conversation_id=thread,
        author="U1",
        text=text,
        reply_handle=ReplyHandle(
            kind="slack",
            channel=channel,
            placeholder=placeholder,
            endpoint=None,
            adapter=None,
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
        sink = RecordingSink()
        binding = OneAgentTwoBindings()

        async with make_harness(binding=binding, sink=sink) as h:
            # Two scripts, consumed FIFO by the fake runner. Turn A is started
            # and confirmed open first, so it takes the first script; turn B then
            # runs alongside it and takes the second.
            h.runner.turn_scripts = [
                [TextDelta(text="a "), Final(text=ANSWER_A, status=DONE)],
                [TextDelta(text="b "), Final(text=ANSWER_B, status=DONE)],
            ]

            b_done = asyncio.Event()
            # The interleave: channel A cannot deliver its answer (nor complete)
            # until channel B's turn has completed. Both turns are in flight
            # across that window, which is the condition a per-agent reply target
            # would get wrong.
            sink.gate = (
                lambda ev: (
                    ev.target.address == CHANNEL_A
                    and (
                        (isinstance(ev, ReplyUpdate) and ev.text == ANSWER_A)
                        or isinstance(ev, TurnCompleted)
                    )
                ),
                b_done,
                20.0,
            )

            turn_a = _qevent(
                "hi from channel a",
                channel=CHANNEL_A,
                thread="th-a",
                placeholder="ph-a",
            )
            turn_b = _qevent(
                "hi from channel b",
                channel=CHANNEL_B,
                thread="th-b",
                placeholder="ph-b",
            )

            task_a = asyncio.create_task(h.kernel.process_event(turn_a))
            await _wait_until(lambda: "hi from channel a" in h.runner.opened, "turn A to open")
            task_b = asyncio.create_task(h.kernel.process_event(turn_b))

            await _wait_until(
                lambda: bool(sink.completions_for(CHANNEL_B)),
                "channel B to see turn.completed (nothing was addressed to it: a "
                "reply addressed off the AGENT would have sent both turns to one door)",
            )
            assert not sink.completions_for(CHANNEL_A), (
                "turn A completed before turn B; the interleave this test depends on did not happen"
            )
            b_done.set()
            await asyncio.gather(task_a, task_b)

            assert not sink.gate_timed_out, "the channel A gate never opened"

            # 1. Both doors were replied to, and no event was addressed anywhere
            #    else.
            assert sink.events_for(CHANNEL_A) and sink.events_for(CHANNEL_B)
            assert len(sink.events_for(CHANNEL_A)) + len(sink.events_for(CHANNEL_B)) == len(
                sink.events
            )

            # 2. Every event on a door belongs to THAT door's turn: kind,
            #    conversation and the opaque reply ref, all three.
            for turn in (turn_a, turn_b):
                handle = turn.reply_handle
                for event in sink.events_for(handle.channel):
                    target = event.target
                    assert target.kind == handle.kind, event
                    assert target.conversation_id == turn.conversation_id, event
                    assert target.reply_ref == handle.placeholder, event

            # 3. Neither door saw the other's text.
            assert ANSWER_A in sink.texts_for(CHANNEL_A)
            assert ANSWER_B in sink.texts_for(CHANNEL_B)
            assert ANSWER_B not in sink.texts_for(CHANNEL_A)
            assert ANSWER_A not in sink.texts_for(CHANNEL_B)

            # 4. Each turn completed for its own event id, and turn B completed
            #    FIRST -- proving the assertions above held while both turns were
            #    open, not merely across two sequential turns.
            assert [c.event_id for c in sink.completions_for(CHANNEL_A)] == [turn_a.event_id]
            assert [c.event_id for c in sink.completions_for(CHANNEL_B)] == [turn_b.event_id]
            completions = [address for _, address, name in sink.log if name == "turn.completed"]
            assert completions == [CHANNEL_B, CHANNEL_A]

            # 5. Both turns are durably done, and both resolved against their own
            #    pair rather than one lookup being reused for the other.
            assert await h.async_redis.exists(h.config.done_key(turn_a.event_id))
            assert await h.async_redis.exists(h.config.done_key(turn_b.event_id))
            assert set(binding.resolve_calls) == {
                ("slack", CHANNEL_A),
                ("slack", CHANNEL_B),
            }

    asyncio.run(go())
