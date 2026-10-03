"""#3776: the kernel tells the API when a turn's memory write credential is done.

ADR 0188's per-turn credential is a bearer token valid until ``exp``. So that the
API can refuse it once its turn is over, the kernel calls
``BindingResolver.close_turn_memory(agent_id, turn)`` for every turn claim it
minted in an attempt, when that attempt ends, on every outcome: success, a
runner error (and its retry), cancellation, the work-item continuation. A
steered attempt is the exception: the live turn it joined keeps using the
steering credential, so it is closed when that live turn ends. A close that
fails never fails the turn: the credential still expires at the turn's deadline.

Each attempt mints its own turn claim (``<event_id>#<suffix>``) so closing one
attempt's credential cannot refuse the retry's. A steer is handed to the live
runner turn it joined, not to its thread, so a finished turn's close cannot
reach the next turn's steers. The closes run in the background, concurrently and
each under its own timeout, so a slow API never holds up the attempt's end.

Drives the real ``Kernel.process_event`` against real Valkey and the scriptable
runner, with a binding double that mints real credentials and records closes.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest
from aci_protocol import Final, OutboundEvent, SessionStatus, TextDelta
from curie_worker.kernel import memory as kernel_memory

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from queue_fixtures import wait_until as _wait_until  # noqa: E402
from test_memory_token_event import (  # noqa: E402
    AGENT_ID,
    _as,
    _claims,
    _MemoryBinding,
    _qevent,
    _tool,
)
from test_work_item_early_stop import (  # noqa: E402
    ISSUE_PROMPT,
    _PublicationApi,
    _WorkItems,
    _Workspace,
)
from test_work_item_early_stop import _turn as _work_item_turn  # noqa: E402


class _ClosingBinding(_MemoryBinding):
    """Records, in order, every credential minted and every turn closed."""

    def __init__(self, *, memory_writes: bool = True, close_error: bool = False) -> None:
        super().__init__(memory_writes=memory_writes)
        self.log: list[tuple[str, str]] = []
        self.closes: list[tuple[uuid.UUID, str]] = []
        self.close_error = close_error

    def turn_memory_token(self, resolved: Any, **kwargs: Any) -> str | None:
        token = super().turn_memory_token(resolved, **kwargs)
        if token:
            self.log.append(("mint", _claims(token)["turn"]))
        return token

    async def close_turn_memory(self, agent_id: uuid.UUID, turn: str) -> None:
        self.log.append(("close", turn))
        self.closes.append((agent_id, turn))
        if self.close_error:
            raise RuntimeError("the API is unreachable")

    @property
    def minted(self) -> list[str]:
        return [turn for kind, turn in self.log if kind == "mint"]

    @property
    def closed(self) -> list[str]:
        return [turn for _agent, turn in self.closes]


def _turn_of(body: dict[str, Any]) -> str:
    turn = _claims(body["memory_token"])["turn"]
    assert isinstance(turn, str)
    return turn


def test_turn_is_closed_after_a_successful_turn(make_harness) -> None:
    async def go() -> None:
        binding = _ClosingBinding()
        async with make_harness(binding=binding) as h:
            turn = _as(_qevent("remember the window", thread="th-tc-1"), "U0ALICE01")

            await h.kernel.process_event(turn)

            assert h.sink.last_text == "ok"
            minted = _turn_of(h.runner.event_bodies[0])
            assert minted.startswith(f"{turn.event_id}#"), minted
            await _wait_until(lambda: binding.closed == [minted], "the turn to be closed")
            assert binding.closes == [(AGENT_ID, minted)]

    asyncio.run(go())


def test_each_attempt_is_closed_before_the_retry(make_harness) -> None:
    # A runner error ends the attempt: its credential is closed, and the retry
    # mints a credential with its own turn claim, which the close does not refuse.
    async def go() -> None:
        binding = _ClosingBinding()
        async with make_harness(binding=binding, max_attempts=3) as h:
            h.runner.event_fail_times = 1

            await h.kernel.process_event(_as(_qevent("retry me", thread="th-tc-2"), "U0ALICE01"))

            assert len(h.runner.event_bodies) == 2, h.runner.opened
            first, second = (_turn_of(b) for b in h.runner.event_bodies)
            assert first != second
            await _wait_until(
                lambda: sorted(binding.closed) == sorted([first, second]),
                "both attempts to be closed",
            )
            assert binding.log.index(("close", first)) < binding.log.index(("mint", second))

    asyncio.run(go())


def test_turn_is_closed_when_every_attempt_fails(make_harness) -> None:
    async def go() -> None:
        binding = _ClosingBinding()
        async with make_harness(binding=binding, max_attempts=1) as h:
            h.runner.event_fail_times = 5

            await h.kernel.process_event(_as(_qevent("fail", thread="th-tc-3"), "U0ALICE01"))

            assert binding.minted, "no credential was minted"
            await _wait_until(
                lambda: sorted(binding.closed) == sorted(binding.minted),
                "the failed attempt to be closed",
            )

    asyncio.run(go())


def test_turn_is_closed_when_the_turn_is_cancelled(make_harness) -> None:
    async def go() -> None:
        binding = _ClosingBinding()
        async with make_harness(binding=binding) as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=SessionStatus.DONE)]
            task = asyncio.create_task(
                h.kernel.process_event(_as(_qevent("long one", thread="th-tc-4"), "U0ALICE01"))
            )
            try:
                await _wait_until(lambda: h.runner.turn_active, "the turn to be live")
                assert binding.minted and not binding.closed
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            finally:
                hold.set()
                await asyncio.gather(task, return_exceptions=True)

            await _wait_until(
                lambda: binding.closed == binding.minted, "the cancelled turn to be closed"
            )

    asyncio.run(go())


def test_continuation_turns_are_closed(make_harness) -> None:
    # A work-item execute turn re-prompted in the same attempt opens a second
    # runner turn with its own credential; both are closed.
    async def go() -> None:
        binding = _ClosingBinding()
        scripts: list[list[OutboundEvent]] = [
            [
                _tool("mcp__github__get_issue"),
                TextDelta(text="I read the issue."),
                Final(text="I read the issue.", status=SessionStatus.DONE),
            ],
            [Final(text="I will not continue.", status=SessionStatus.DONE)],
        ]
        async with make_harness(
            binding=binding, workspace_factory=_Workspace, publication_creator=_PublicationApi()
        ) as h:
            h.kernel._work_items = _WorkItems()
            h.runner.turn_scripts = [list(s) for s in scripts]
            h.runner.default_script = [Final(text="unexpected", status=SessionStatus.DONE)]
            turn = _work_item_turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT)

            await h.kernel.process_event(turn)

            assert len(h.runner.event_bodies) == 2, h.runner.opened
            turns = [_turn_of(b) for b in h.runner.event_bodies]
            await _wait_until(
                lambda: set(turns) <= set(binding.closed), "both runner turns to be closed"
            )

    asyncio.run(go())


def test_steer_is_closed_when_the_live_turn_ends(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The live turn keeps the steering credential (``MemoryTurn.begin`` on
    # steer), so the steering attempt returning must not close it; the live
    # turn ending does.
    async def go() -> None:
        binding = _ClosingBinding()
        async with make_harness(binding=binding) as h:
            steered: list[Any] = []
            real_steer = h.kernel._runner.steer

            async def spy(base_url: str, event: Any, **kwargs: Any) -> Any:
                steered.append(event)
                return await real_steer(base_url, event, **kwargs)

            monkeypatch.setattr(h.kernel._runner, "steer", spy)
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=SessionStatus.DONE)]
            first = _as(_qevent("first", thread="th-tc-6", placeholder="ph-1"), "U0ALICE01")
            task = asyncio.create_task(h.kernel.process_event(first))
            try:
                await _wait_until(lambda: h.runner.turn_active, "the first turn to be live")
                second = _as(_qevent("and this", thread="th-tc-6", placeholder="ph-2"), "U0BOB0001")
                await h.kernel.process_event(second)

                assert h.runner.steers == ["and this"]
                landed = [e for e in steered if e.text == "and this"]
                assert len(landed) == 1
                steer_turn = _claims(landed[0].memory_token)["turn"]
                live_turn = _turn_of(h.runner.event_bodies[0])
                # Give a wrongly eager close the chance to land.
                await asyncio.sleep(0.2)
                assert steer_turn not in binding.closed, binding.closes
                assert live_turn not in binding.closed, binding.closes
            finally:
                hold.set()
                await asyncio.gather(task, return_exceptions=True)

            await _wait_until(
                lambda: {live_turn, steer_turn} <= set(binding.closed),
                "the live turn and its steer to be closed",
            )

    asyncio.run(go())


def test_a_failed_close_does_not_fail_the_turn(make_harness) -> None:
    async def go() -> None:
        binding = _ClosingBinding(close_error=True)
        async with make_harness(binding=binding, max_attempts=3) as h:
            await h.kernel.process_event(_as(_qevent("hello", thread="th-tc-7"), "U0ALICE01"))

            assert h.sink.last_text == "ok"
            # The close was tried, and its failure did not cause a retry.
            await _wait_until(lambda: bool(binding.closes), "the close to be attempted")
            assert len(h.runner.event_bodies) == 1, h.runner.opened

    asyncio.run(go())


def test_no_credential_means_no_close(make_harness) -> None:
    async def go() -> None:
        binding = _ClosingBinding(memory_writes=False)
        async with make_harness(binding=binding) as h:
            await h.kernel.process_event(_qevent("hello", thread="th-tc-8"))

            assert h.runner.event_bodies[0]["memory_token"] is None
            await asyncio.sleep(0.2)
            assert binding.closes == []

    asyncio.run(go())


def test_a_finished_turns_close_spares_the_next_turns_steer(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # L1: turn N's attempt closes its steers after its runner turn has ended.
    # In that window turn N+1 opens on the same thread and a further event
    # steers into it. N's close must take only the steers that joined N, so the
    # steer into the still-live N+1 keeps its credential until N+1 ends.
    async def go() -> None:
        binding = _ClosingBinding()
        async with make_harness(binding=binding) as h:
            steered: list[Any] = []
            real_steer = h.kernel._runner.steer

            async def spy(base_url: str, event: Any, **kwargs: Any) -> Any:
                steered.append(event)
                return await real_steer(base_url, event, **kwargs)

            monkeypatch.setattr(h.kernel._runner, "steer", spy)
            # Hold turn N's attempt between its runner turn ending and its
            # steers being read, which is where the next turn can open.
            entered = asyncio.Event()
            gate = asyncio.Event()
            real_drain = h.kernel._markers.drain_steer_memory_turns

            async def gated_drain(*args: Any, **kwargs: Any) -> Any:
                entered.set()
                await gate.wait()
                return await real_drain(*args, **kwargs)

            monkeypatch.setattr(h.kernel._markers, "drain_steer_memory_turns", gated_drain)
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=SessionStatus.DONE)]
            hold_n = asyncio.Event()
            hold_next = asyncio.Event()
            h.runner.hold = hold_n
            thread = "th-tc-9"
            first = _as(_qevent("turn n", thread=thread, placeholder="ph-1"), "U0ALICE01")
            task_n = asyncio.create_task(h.kernel.process_event(first))
            task_next: asyncio.Task[Any] | None = None
            try:
                await _wait_until(lambda: h.runner.turn_active, "turn N to be live")
                h.runner.hold = hold_next
                hold_n.set()
                await asyncio.wait_for(entered.wait(), 5)
                await _wait_until(lambda: not h.runner.turn_active, "turn N to end")

                second = _as(_qevent("turn n+1", thread=thread, placeholder="ph-2"), "U0ALICE01")
                task_next = asyncio.create_task(h.kernel.process_event(second))
                await _wait_until(
                    lambda: h.runner.turn_active and len(h.runner.event_bodies) == 2,
                    "turn N+1 to be live",
                )
                third = _as(
                    _qevent("steer into n+1", thread=thread, placeholder="ph-3"), "U0BOB0001"
                )
                await h.kernel.process_event(third)
                assert h.runner.steers == ["steer into n+1"]
                landed = [e for e in steered if e.text == "steer into n+1"]
                assert len(landed) == 1 and landed[0].memory_token
                steer_turn = _claims(landed[0].memory_token)["turn"]
                turn_n = _turn_of(h.runner.event_bodies[0])
                turn_next = _turn_of(h.runner.event_bodies[1])

                # Now let turn N's attempt read its steers and close.
                gate.set()
                await asyncio.wait_for(task_n, 5)
                await _wait_until(lambda: turn_n in binding.closed, "turn N to be closed")
                # Give a wrongly broad close the chance to land.
                await asyncio.sleep(0.2)
                assert steer_turn not in binding.closed, binding.closes
                assert turn_next not in binding.closed, binding.closes
            finally:
                gate.set()
                hold_n.set()
                hold_next.set()
                await asyncio.gather(task_n, return_exceptions=True)
                if task_next is not None:
                    await asyncio.gather(task_next, return_exceptions=True)

            await _wait_until(
                lambda: {turn_next, steer_turn} <= set(binding.closed),
                "turn N+1 and its steer to be closed when N+1 ends",
            )

    asyncio.run(go())


class _HangingCloseBinding(_ClosingBinding):
    """A close call that hangs until released, counting the calls in flight."""

    def __init__(self) -> None:
        super().__init__()
        self.release = asyncio.Event()
        self.entered = asyncio.Event()
        self.in_flight = 0
        self.peak = 0

    async def close_turn_memory(self, agent_id: uuid.UUID, turn: str) -> None:
        self.log.append(("close", turn))
        self.closes.append((agent_id, turn))
        self.in_flight += 1
        self.peak = max(self.peak, self.in_flight)
        self.entered.set()
        try:
            await self.release.wait()
        finally:
            self.in_flight -= 1


def test_a_hung_close_does_not_hold_up_the_attempt(
    make_harness, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # L3: the attempt ends without waiting on the close; the close runs in the
    # background, times out there, and the timeout is logged.
    monkeypatch.setattr(kernel_memory, "_MEMORY_CLOSE_TIMEOUT_S", 1.0, raising=False)
    caplog.set_level(logging.WARNING, logger="curie_worker.kernel")

    async def go() -> None:
        binding = _HangingCloseBinding()
        async with make_harness(binding=binding) as h:
            task = asyncio.create_task(
                h.kernel.process_event(_as(_qevent("hello", thread="th-tc-10"), "U0ALICE01"))
            )
            try:
                await asyncio.wait_for(binding.entered.wait(), 5)
                # The close is hanging; the attempt still finishes promptly.
                await asyncio.wait_for(asyncio.shield(task), 0.5)
                assert h.sink.last_text == "ok"
                minted = binding.minted
                assert len(minted) == 1 and binding.in_flight == 1

                def logged() -> bool:
                    return any(
                        r.levelno >= logging.WARNING
                        and "timed out" in r.getMessage()
                        and minted[0] in r.getMessage()
                        for r in caplog.records
                    )

                await _wait_until(logged, "the close's timeout to be logged")
                await _wait_until(lambda: binding.in_flight == 0, "the hung close to be dropped")
            finally:
                binding.release.set()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(go())


def test_an_attempts_turns_are_closed_concurrently(make_harness) -> None:
    # L3: a work-item attempt that minted two turns closes them at the same
    # time, so one slow close does not delay the other.
    async def go() -> None:
        binding = _HangingCloseBinding()
        scripts: list[list[OutboundEvent]] = [
            [
                _tool("mcp__github__get_issue"),
                TextDelta(text="I read the issue."),
                Final(text="I read the issue.", status=SessionStatus.DONE),
            ],
            [Final(text="I will not continue.", status=SessionStatus.DONE)],
        ]
        async with make_harness(
            binding=binding, workspace_factory=_Workspace, publication_creator=_PublicationApi()
        ) as h:
            h.kernel._work_items = _WorkItems()
            h.runner.turn_scripts = [list(s) for s in scripts]
            h.runner.default_script = [Final(text="unexpected", status=SessionStatus.DONE)]
            turn = _work_item_turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT)
            task = asyncio.create_task(h.kernel.process_event(turn))
            try:
                await _wait_until(
                    lambda: binding.peak >= 2, "both turns' closes to be in flight together"
                )
                assert len(h.runner.event_bodies) == 2, h.runner.opened
                assert set(binding.closed) == {_turn_of(b) for b in h.runner.event_bodies}
            finally:
                binding.release.set()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(go())
