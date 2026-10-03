"""A hook fans out per partition, and each partition still serializes.

A hook's conversation id may carry a fourth, operator-configured segment --
``hook:<agent-id>:<name>:<partition>`` -- so one firing of a hook that finds N
independent things runs N threads instead of deferring N-1 of them behind the
first. These tests pin both halves of that: partitions of one hook run
CONCURRENTLY, and two deliveries naming the SAME partition still serialize
exactly as an unpartitioned hook does today.

**The harness trap these tests exist to defend against.** The kernel suite's
shared ``FakeK8s`` resolves every sandbox to ``127.0.0.1`` on ONE fleet-wide
runner port, so every claim dials ONE ``FakeRunner`` holding ONE global
``turn_active`` flag. ``Kernel._turn_active`` then reports EVERY thread busy the
moment any thread is live, and a fan-out test written on that fake concludes
that independent partitions serialize against each other -- a passing test of
the wrong thing, and the opposite of the behavior under test.

The fix is ``per_sandbox_runners``: one ``FakeRunner`` behind its own aiohttp
server per sandbox, handed to the substrate through ``SandboxView.port``. So the
positive control below asserts the three handles resolved to three **DISTINCT**
runner ports. That assertion is not decoration -- without it the test cannot
tell real fan-out from the shared-runner trap, because the trap's symptom
(every partition talking to one runner) is invisible in claim counts, steer logs
and turn text alike. The harness's second guard is the fleet-wide fallback port,
which is deliberately CLOSED whenever ``per_sandbox_runners`` is set: if the
per-sandbox port is ever dropped on the floor, every partition falls back to a
dead port, ``_turn_active`` fails closed, and these tests fail loudly instead of
quietly sharing one runner.
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from pathlib import Path

import pytest
from aci_protocol import (
    Final,
    QueuedTurn,
    ReplyHandle,
    SessionStatus,
    TextDelta,
    TurnSource,
)
from curie_worker.kernel.failures import ThreadBusyError

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from queue_fixtures import wait_until as _wait_until  # noqa: E402

DONE = SessionStatus.DONE

# An example agent id and an example Slack channel: this repo is public, so no
# identifier here is real.
AGENT_ID = "00000000-0000-4000-8000-00000000a1b2"
CHANNEL = "C0EXAMPLE1"
HOOK = "prs"


def _conversation_id(hook: str = HOOK, partition: str | None = None) -> str:
    """The id the hook ingress mints, with or without its partition segment.

    Built here rather than imported so these tests state the wire shape they
    depend on instead of inheriting whatever the API happens to produce.
    """
    base = f"hook:{AGENT_ID}:{hook}"
    return base if partition is None else f"{base}:{partition}"


def _hook_event(
    text: str,
    *,
    partition: str | None = None,
    hook: str = HOOK,
    event_id: str | None = None,
) -> QueuedTurn:
    """One webhook delivery, as the hook ingress enqueues it.

    ``source=TurnSource.WEBHOOK`` is load-bearing: ADR-0079 makes a job an
    OUTPUT, so the kernel defers it with ``ThreadBusyError`` on a live thread
    rather than steering into it. A ``TurnSource.SLACK`` turn would steer and
    these tests would assert nothing about deferral. ``placeholder=None`` mirrors
    the ingress, which has no pre-posted message to edit.

    Its own helper rather than ``test_kernel.py``'s ``_qevent``: that one hard
    codes a Slack conversation id and source, and importing across test modules
    is fragile under importlib mode.
    """
    return QueuedTurn(
        event_id=event_id or uuid.uuid4().hex,
        conversation_id=_conversation_id(hook, partition),
        author=f"hook:{hook}",
        text=text,
        reply_handle=ReplyHandle(
            kind="slack", channel=CHANNEL, placeholder=None, endpoint=None
        ),
        received_at="2026-08-29T00:00:00+00:00",
        source=TurnSource.WEBHOOK,
    )


def _park_every_runner(h, hold: asyncio.Event) -> None:
    """Make every per-sandbox runner hang mid-turn until ``hold`` is set.

    A turn that hangs is the only way to observe concurrency at all: without it
    each turn finishes before the next starts and three serialized turns look
    identical to three concurrent ones.
    """
    for runner in h.runners.values():
        runner.hold = hold
        runner.default_script = [TextDelta(text="working")]
        runner.tail = [Final(text="done", status=DONE)]


def _live_ports(h) -> set[int]:
    """Ports whose runner has a turn live RIGHT NOW."""
    return {port for port, runner in h.runners.items() if runner.turn_active}


def _opened_total(h) -> int:
    return sum(len(runner.opened) for runner in h.runners.values())


def test_three_partitions_of_one_hook_run_concurrently(make_harness) -> None:
    """The positive control: fan-out, and the proof the harness can see it.

    Three ids differing ONLY in their partition segment, driven concurrently.
    They must reach three sandboxes, three DISTINCT runner ports, and three
    simultaneously live turns, with no steer and no deferral anywhere.
    """

    async def go() -> None:
        async with make_harness(per_sandbox_runners=3) as h:
            hold = asyncio.Event()
            _park_every_runner(h, hold)

            events = [_hook_event(f"pr {n}", partition=str(n)) for n in (41, 42, 43)]
            # Differing only in the partition segment is the whole premise: if
            # the ids agreed, one thread key would serialize them.
            assert len({e.conversation_id for e in events}) == 3
            assert {e.conversation_id.rsplit(":", 1)[0] for e in events} == {
                _conversation_id()
            }

            tasks = [asyncio.create_task(h.kernel.process_event(e)) for e in events]
            try:
                # All three live AT THE SAME MOMENT, not merely all three
                # eventually having run.
                await _wait_until(lambda: len(_live_ports(h)) == 3)
                live = _live_ports(h)
                claims = set(h.fake_k8s.claims)
                assigned = dict(h.fake_k8s.assigned_ports)
                opened = {port: list(r.opened) for port, r in h.runners.items()}
                steers = {port: list(r.steers) for port, r in h.runners.items()}
                shared_opened = list(h.runner.opened)
            finally:
                hold.set()
                outcomes = await asyncio.gather(*tasks, return_exceptions=True)

            assert [o for o in outcomes if isinstance(o, BaseException)] == [], (
                "a partition was deferred or failed; fan-out did not happen"
            )
            assert len(claims) == 3, f"expected one sandbox per partition, got {claims}"
            # THE load-bearing assertion. Three distinct ports is the only
            # observable difference between real fan-out and the shared-runner
            # trap described in the module docstring.
            assert len(set(assigned.values())) == 3, (
                f"partitions shared a runner port: {assigned}"
            )
            assert live == set(assigned.values()), (
                f"live turns {live} did not match the assigned ports {assigned}"
            )
            assert sum(len(v) for v in opened.values()) == 3
            assert all(len(v) == 1 for v in opened.values()), (
                f"a runner served more than one partition: {opened}"
            )
            assert all(v == [] for v in steers.values()), (
                f"a job steered a live session: {steers}"
            )
            # The fleet-wide runner is on a port no sandbox was given, and with
            # per-sandbox runners the fallback port is closed: anything reaching
            # it would mean the per-sandbox port was ignored.
            assert shared_opened == []

    asyncio.run(go())


def test_two_deliveries_on_one_partition_serialize(make_harness) -> None:
    """Sequential intra-partition serialization survives the fan-out change.

    The second delivery names the SAME partition, so it is the same thread: it
    must defer with ``ThreadBusyError``, never steer into the live turn and
    never open a second one beside it.
    """

    async def go() -> None:
        async with make_harness(per_sandbox_runners=3) as h:
            hold = asyncio.Event()
            _park_every_runner(h, hold)

            first = asyncio.create_task(
                h.kernel.process_event(_hook_event("pr 41 first", partition="41"))
            )
            try:
                await _wait_until(lambda: len(_live_ports(h)) == 1)
                with pytest.raises(ThreadBusyError):
                    await h.kernel.process_event(
                        _hook_event("pr 41 second", partition="41")
                    )
                claims = set(h.fake_k8s.claims)
                opened = _opened_total(h)
                steers = {port: list(r.steers) for port, r in h.runners.items()}
            finally:
                hold.set()
                await asyncio.gather(first, return_exceptions=True)

            assert len(claims) == 1, f"one partition claimed more than one sandbox: {claims}"
            assert opened == 1, "the deferred delivery opened a turn beside the live one"
            assert all(v == [] for v in steers.values()), (
                f"a job steered a live session: {steers}"
            )

    asyncio.run(go())


def test_concurrent_deliveries_on_one_partition_serialize(make_harness) -> None:
    """The same rule with no pre-sequencing: exactly one of the two runs.

    Both deliveries are dispatched as concurrent tasks, so which one opens the
    turn is decided by the kernel's per-thread FIFO lock rather than by the
    test. Whichever loses must defer, not steer, and not fork a second turn.
    """

    async def go() -> None:
        async with make_harness(per_sandbox_runners=3) as h:
            hold = asyncio.Event()
            _park_every_runner(h, hold)

            tasks = [
                asyncio.create_task(
                    h.kernel.process_event(_hook_event(text, partition="41"))
                )
                for text in ("pr 41 a", "pr 41 b")
            ]
            try:
                await _wait_until(
                    lambda: len(_live_ports(h)) == 1
                    and sum(t.done() for t in tasks) == 1
                )
                claims = set(h.fake_k8s.claims)
                opened = _opened_total(h)
                steers = {port: list(r.steers) for port, r in h.runners.items()}
            finally:
                hold.set()
                outcomes = await asyncio.gather(*tasks, return_exceptions=True)

            busy = [o for o in outcomes if isinstance(o, ThreadBusyError)]
            other = [
                o
                for o in outcomes
                if isinstance(o, BaseException) and not isinstance(o, ThreadBusyError)
            ]
            assert other == [], f"unexpected failure on a serialized partition: {other}"
            assert len(busy) == 1, f"expected exactly one deferral, got {outcomes}"
            assert len(claims) == 1, f"one partition claimed more than one sandbox: {claims}"
            assert opened == 1
            assert all(v == [] for v in steers.values()), (
                f"a job steered a live session: {steers}"
            )

    asyncio.run(go())


def test_an_unpartitioned_hook_still_shares_one_thread(make_harness) -> None:
    """The negative control: no partition segment means no fan-out.

    Three deliveries on one three-segment id behave exactly as they do today --
    one sandbox, one turn, two deferrals. This is the test that catches a change
    which makes an UNPARTITIONED hook start fanning out, which would silently
    multiply every existing agent's sandbox count.
    """

    async def go() -> None:
        async with make_harness(per_sandbox_runners=3) as h:
            hold = asyncio.Event()
            _park_every_runner(h, hold)

            unpartitioned = _conversation_id()
            assert unpartitioned.count(":") == 2, "the control id gained a partition"

            first = asyncio.create_task(h.kernel.process_event(_hook_event("sweep 1")))
            try:
                await _wait_until(lambda: len(_live_ports(h)) == 1)
                deferrals = 0
                for text in ("sweep 2", "sweep 3"):
                    with pytest.raises(ThreadBusyError):
                        await h.kernel.process_event(_hook_event(text))
                    deferrals += 1
                claims = set(h.fake_k8s.claims)
                opened = _opened_total(h)
                assigned = dict(h.fake_k8s.assigned_ports)
                steers = {port: list(r.steers) for port, r in h.runners.items()}
            finally:
                hold.set()
                await asyncio.gather(first, return_exceptions=True)

            assert deferrals == 2
            assert len(claims) == 1, f"an unpartitioned hook fanned out: {claims}"
            assert len(assigned) == 1, f"an unpartitioned hook fanned out: {assigned}"
            assert opened == 1
            assert all(v == [] for v in steers.values()), (
                f"a job steered a live session: {steers}"
            )

    asyncio.run(go())
