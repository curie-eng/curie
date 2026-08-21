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
import contextlib
import itertools
import time
import uuid
from collections.abc import AsyncIterator, Callable
from typing import Any

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
        # Set the instant an event is PARKED on the gate, before the wait. A test
        # that needs "the gated turn is now mid-flight" as a precondition cannot
        # infer it from ``events`` (the parked event has not been appended yet),
        # and inferring it from the fake runner's flags is a race.
        self.gate_entered = False

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
                self.gate_entered = True
                try:
                    await asyncio.wait_for(opened.wait(), timeout)
                except TimeoutError:
                    self.gate_timed_out = True
        self.events.append(event)
        self.log.append((next(self._sequence), event.target.address, event.event))
        return ReplyAck(ref=event.target.reply_ref)


class RecordingSubstrate:
    """Delegating proxy over the REAL substrate that records the key it is asked for.

    Session identity is not otherwise observable from outside the kernel: the
    substrate's key is an argument, not a return value, and the sandbox it hands
    back is reachable only through a ``SandboxHandle`` the kernel keeps to
    itself. So this records both halves per call -- the key asked for, and the
    sandbox handed back -- which is exactly the pair that distinguishes "two
    turns, two sessions" from "the second turn adopted the first's sandbox".

    A proxy rather than a fake: every call still runs the real
    ``SandboxSubstrate`` against the real affinity store in Valkey, so an adopt
    is a genuine adopt and not something this double decided.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        # (thread_key asked for, sandbox handed back), in call order.
        self.claims: list[tuple[str, str]] = []
        self.lookups: list[str] = []

    def claim(self, thread_key: str, *, env: dict[str, str] | None = None) -> Any:
        handle = self._inner.claim(thread_key, env=env)
        self.claims.append((thread_key, handle.sandbox_name))
        return handle

    def resume(self, thread_key: str, *, env: dict[str, str] | None = None) -> Any:
        handle = self._inner.resume(thread_key, env=env)
        self.claims.append((thread_key, handle.sandbox_name))
        return handle

    def lookup(self, thread_key: str) -> Any:
        self.lookups.append(thread_key)
        return self._inner.lookup(thread_key)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class RecordingLock:
    """Delegating proxy over the real ``ThreadLock``, recording the keys used.

    The per-thread Valkey lock is held only for the bounded critical section, so
    it is gone from Valkey long before a test could scan for it; the key has to
    be captured as it is taken.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.keys: list[str] = []

    @contextlib.asynccontextmanager
    async def hold(self, key: str) -> AsyncIterator[None]:
        self.keys.append(key)
        async with self._inner.hold(key):
            yield

    async def acquire(self, key: str) -> str:
        self.keys.append(key)
        token: str = await self._inner.acquire(key)
        return token

    async def release(self, key: str, token: str) -> None:
        await self._inner.release(key, token)


def _record_identity(harness: Any) -> tuple[RecordingSubstrate, RecordingLock]:
    """Wrap the harness kernel's substrate and lock so their keys are observable.

    Installed onto the assembled kernel because the substrate cannot be built
    before the harness (it needs the fake runner's port). Nothing in production
    changes: both objects are proxies onto the very collaborators the harness
    already wired in.
    """
    substrate = RecordingSubstrate(harness.kernel._substrate)
    lock = RecordingLock(harness.kernel._lock)
    harness.kernel._substrate = substrate
    harness.kernel._lock = lock
    return substrate, lock


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


# A Slack ``thread_ts`` is unique only WITHIN a channel, and the dispatcher mints
# it as the turn's ``conversation_id`` verbatim. Two channels can therefore hand
# the worker the same conversation id for two unrelated conversations -- which is
# a new possibility only because one agent can now be bound to two channels.
SHARED_THREAD = "1700000000.000100"


def test_one_conversation_id_on_two_channels_is_two_sessions(make_harness) -> None:
    """The same ``thread_ts`` on two channels must not share one session.

    The two turns below are the collision as it reaches the worker: the same
    ``conversation_id``, two different Slack channels, one agent. If the kernel
    keys its internal thread identity on the bare ``conversation_id``, the second
    turn resolves to the FIRST turn's sandbox -- and with the sandbox comes its
    transcript, its bundle, its lock and its approval-card slot. One customer's
    thread would then continue inside another's session.

    The observation is the substrate key: the kernel asks the substrate for a
    sandbox by thread key, so "two distinct sessions" is exactly "two distinct
    keys asked for, two distinct sandboxes handed back". Adoption is a real
    behavior of the real substrate here (the affinity store lives in the real
    Valkey), so a single sandbox across the two turns is the defect itself and
    not a property of a double.

    The second half is the constraint that killed the obvious fix: scoping the id
    at the DISPATCHER cannot work, because ``ReplyTarget.conversation_id`` is
    what the Slack adapter sends back as ``thread_ts``
    (``slack_sink.py``). A scoped id on the wire replies into a thread
    that does not exist. So the reply target must keep carrying the BARE id even
    while the worker's own keys are scoped, and both halves are asserted here so
    neither can be satisfied at the other's expense.

    Concurrent, and deliberately overlapped: turn A is parked at the egress seam
    for the whole of turn B, so B routes while A's session is live. A sequential
    pair would still catch the adoption, but not the shared lock and not a
    "whichever turn ran last wins" reply target.
    """

    async def go() -> None:
        sink = RecordingSink()
        binding = OneAgentTwoBindings()

        async with make_harness(binding=binding, sink=sink) as h:
            substrate, lock = _record_identity(h)
            h.runner.turn_scripts = [
                [TextDelta(text="a "), Final(text=ANSWER_A, status=DONE)],
                [TextDelta(text="b "), Final(text=ANSWER_B, status=DONE)],
            ]

            b_done = asyncio.Event()
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
                thread=SHARED_THREAD,
                placeholder="ph-a",
            )
            turn_b = _qevent(
                "hi from channel b",
                channel=CHANNEL_B,
                thread=SHARED_THREAD,
                placeholder="ph-b",
            )

            task_a = asyncio.create_task(h.kernel.process_event(turn_a))
            # Turn A is started, its model stream is drained, and it is now
            # parked at the gate holding its session open. Waiting for BOTH
            # conditions is what makes the overlap deterministic: parked alone
            # would let B's follow-up land while A's runner still reports a live
            # turn (B would be steered into it, which is correct behavior for a
            # genuine same-thread follow-up and would mask the defect under
            # test), and an idle runner alone does not prove A got that far.
            await _wait_until(
                lambda: sink.gate_entered and not h.runner.turn_active,
                "turn A to be parked mid-flight with its runner stream closed",
            )
            task_b = asyncio.create_task(h.kernel.process_event(turn_b))
            # B releases A rather than a test-side wait on B's completion: if the
            # defect kept B from ever completing, a wait would fail as a 10s
            # timeout instead of as the assertion that names the cause.
            task_b.add_done_callback(lambda _: b_done.set())
            await asyncio.gather(task_a, task_b)

            assert not sink.gate_timed_out, "the channel A gate never opened"

            # 1. Two sessions, not one. The key the kernel asked the substrate
            #    for must differ per channel, and the sandbox it got back with
            #    it -- the second is the one that actually matters, since it is
            #    the sandbox that carries the transcript and the bundle.
            assert len(substrate.claims) == 2, substrate.claims
            asked = [key for key, _ in substrate.claims]
            sandboxes = [name for _, name in substrate.claims]
            assert len(set(asked)) == 2, (
                "both turns were resolved under one thread key "
                f"({asked!r}): a bare conversation_id is not unique across "
                "channels, so channel B's turn keyed into channel A's session"
            )
            assert len(set(sandboxes)) == 2, (
                f"the two turns shared sandbox {sandboxes[0]!r}: channel B "
                "adopted channel A's live sandbox, inheriting its history"
            )

            # 2. The per-thread lock is per SESSION, not per conversation id.
            #    A shared lock serializes two unrelated conversations and, worse,
            #    makes the second turn look like a follow-up to the first.
            assert len(lock.keys) == 2, lock.keys
            assert len(set(lock.keys)) == 2, (
                f"both turns took the same thread lock {lock.keys[0]!r}"
            )

            # 3. The wire is UNCHANGED. Every event still addresses its own
            #    channel and still carries the bare adapter-native conversation
            #    id, because that value is sent to Slack as thread_ts. Scoping
            #    it here is the fix that was already rejected.
            for turn in (turn_a, turn_b):
                handle = turn.reply_handle
                events = sink.events_for(handle.channel)
                assert events, f"nothing was addressed to {handle.channel}"
                for event in events:
                    assert event.target.kind == handle.kind, event
                    assert event.target.conversation_id == SHARED_THREAD, event
                    assert event.target.reply_ref == handle.placeholder, event
            assert len(sink.events_for(CHANNEL_A)) + len(sink.events_for(CHANNEL_B)) == len(
                sink.events
            )

            # 4. Neither conversation saw the other's answer, and each completed
            #    for its own event id.
            assert ANSWER_A in sink.texts_for(CHANNEL_A)
            assert ANSWER_B in sink.texts_for(CHANNEL_B)
            assert ANSWER_B not in sink.texts_for(CHANNEL_A)
            assert ANSWER_A not in sink.texts_for(CHANNEL_B)
            assert [c.event_id for c in sink.completions_for(CHANNEL_A)] == [turn_a.event_id]
            assert [c.event_id for c in sink.completions_for(CHANNEL_B)] == [turn_b.event_id]
            assert await h.async_redis.exists(h.config.done_key(turn_a.event_id))
            assert await h.async_redis.exists(h.config.done_key(turn_b.event_id))

    asyncio.run(go())


def test_a_second_channel_reusing_a_thread_ts_does_not_resume_the_first(
    make_harness,
) -> None:
    """The sequential half: the collision is not only a concurrency hazard.

    Channel A's conversation ends; channel B then sends a turn that happens to
    carry the same ``thread_ts``. Nothing is racing, so a bare-keyed kernel does
    not merely serialize the two -- it hands B the sandbox A finished in, whose
    transcript is still there for the model to read. Same defect, and the one a
    reader is most likely to assume "the lock protects us from".
    """

    async def go() -> None:
        sink = RecordingSink()
        binding = OneAgentTwoBindings()

        async with make_harness(binding=binding, sink=sink) as h:
            substrate, lock = _record_identity(h)
            h.runner.turn_scripts = [
                [Final(text=ANSWER_A, status=DONE)],
                [Final(text=ANSWER_B, status=DONE)],
            ]

            turn_a = _qevent(
                "hi from channel a",
                channel=CHANNEL_A,
                thread=SHARED_THREAD,
                placeholder="ph-a",
            )
            turn_b = _qevent(
                "hi from channel b",
                channel=CHANNEL_B,
                thread=SHARED_THREAD,
                placeholder="ph-b",
            )
            await h.kernel.process_event(turn_a)
            await h.kernel.process_event(turn_b)

            asked = [key for key, _ in substrate.claims]
            sandboxes = [name for _, name in substrate.claims]
            assert len(substrate.claims) == 2, substrate.claims
            assert len(set(asked)) == 2, f"both turns keyed into one session ({asked!r})"
            assert len(set(sandboxes)) == 2, (
                f"channel B resumed channel A's sandbox {sandboxes[0]!r}"
            )
            assert len(set(lock.keys)) == 2, lock.keys

            # The reply wire stays bare on this path too.
            for turn in (turn_a, turn_b):
                for event in sink.events_for(turn.reply_handle.channel):
                    assert event.target.conversation_id == SHARED_THREAD, event
            assert ANSWER_A in sink.texts_for(CHANNEL_A)
            assert ANSWER_B in sink.texts_for(CHANNEL_B)

    asyncio.run(go())
