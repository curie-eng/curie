"""The kernel records what a turn did to the world (ADR-0117).

The branch under test is the one that already existed: a ``side_effect_flag``
sets ``saw_side_effect`` and persists the no-retry marker. It now also writes a
record, and the constraints on that are as much about what must NOT change.

`kernel.py` is sacred under ADR-0013, so every test here that is not about the
ledger is about the signal the ledger must not disturb.
"""

from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from aci_protocol import ErrorEvent, Final, SessionStatus, SideEffectFlag
from curie_worker.actions import ActionBackendError, RecordedAction

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from queue_fixtures import qevent as _qevent  # noqa: E402

DONE = SessionStatus.DONE
FAIL = SessionStatus.CLASSIFIED_FAILURE


@dataclass
class FakeRecorder:
    """Records the calls the kernel makes, and can refuse one."""

    fail_on_record: bool = False
    recorded: list[dict[str, Any]] = field(default_factory=list)
    completed: list[tuple[str, SideEffectFlag]] = field(default_factory=list)

    async def record(
        self,
        frame: SideEffectFlag,
        *,
        event_id: str,
        conversation_id: str,
        agent_id: str | None,
        gate_approval_id: str | None = None,
    ) -> RecordedAction:
        if self.fail_on_record:
            raise ActionBackendError("ledger down")
        self.recorded.append(
            {
                "frame": frame,
                "event_id": event_id,
                "conversation_id": conversation_id,
                "agent_id": agent_id,
                "gate_approval_id": gate_approval_id,
            }
        )
        return RecordedAction(id=f"a{len(self.recorded)}", status="pending")

    async def complete(self, action_id: str, frame: SideEffectFlag) -> dict[str, Any]:
        self.completed.append((action_id, frame))
        return {
            "tool": frame.tool,
            "result": frame.result,
            "detail": frame.detail,
            "status": "failed" if frame.failed else "succeeded",
            "undoable": bool(frame.result and frame.result.get("prior")),
        }


def _call(call_id: str, tool: str = "scale_deployment") -> list[SideEffectFlag]:
    """The two frames one side-effecting call produces."""

    return [
        SideEffectFlag(
            tool=tool,
            call_id=call_id,
            arguments={"replicas": 10},
            detail="non-idempotent tool executed",
        ),
        SideEffectFlag(
            tool=tool,
            call_id=call_id,
            failed=False,
            result={"ok": True, "prior": {"spec": {"replicas": 3}}},
            detail="non-idempotent tool completed",
        ),
    ]


def test_each_call_becomes_one_record(make_harness) -> None:
    """Two calls, two records. The turn is not the unit; the action is."""

    async def go() -> None:
        recorder = FakeRecorder()
        async with make_harness(actions=recorder) as h:
            h.runner.default_script = [
                *_call("toolu_01"),
                *_call("toolu_02", tool="restart_deployment"),
                Final(text="done", status=DONE),
            ]
            await h.kernel.process_event(_qevent("scale it"))

            assert [r["frame"].call_id for r in recorder.recorded] == ["toolu_01", "toolu_02"]
            assert [action_id for action_id, _ in recorder.completed] == ["a1", "a2"]

    asyncio.run(go())


def test_a_record_carries_the_turn_it_belongs_to(make_harness) -> None:
    async def go() -> None:
        recorder = FakeRecorder()
        async with make_harness(actions=recorder) as h:
            h.runner.default_script = [*_call("toolu_01"), Final(text="done", status=DONE)]
            event = _qevent("scale it", thread="th-9")
            await h.kernel.process_event(event)

            assert recorder.recorded[0]["conversation_id"] == "th-9"
            assert recorder.recorded[0]["event_id"] == event.event_id

    asyncio.run(go())


def test_the_completion_carries_what_the_connector_reported(make_harness) -> None:
    async def go() -> None:
        recorder = FakeRecorder()
        async with make_harness(actions=recorder) as h:
            h.runner.default_script = [*_call("toolu_01"), Final(text="done", status=DONE)]
            await h.kernel.process_event(_qevent("scale it"))

            _, frame = recorder.completed[0]
            assert frame.result == {"ok": True, "prior": {"spec": {"replicas": 3}}}

    asyncio.run(go())


def test_a_frame_without_a_call_id_records_nothing_and_still_blocks_retry(
    make_harness,
) -> None:
    """A producer that predates ADR-0117 emits the old frame, and must still work.

    ADR-0036's reader policy cuts both ways: the platform tolerates the older
    producer. There is nothing to record without a call id -- two such frames
    cannot be told apart -- but the no-retry rule reads presence, and presence is
    exactly what this frame still carries.
    """

    async def go() -> None:
        recorder = FakeRecorder()
        async with make_harness(actions=recorder) as h:
            h.runner.default_script = [
                SideEffectFlag(tool="deploy"),
                Final(text="done", status=DONE),
            ]
            event = _qevent("deploy")
            await h.kernel.process_event(event)

            assert recorder.recorded == []
            assert await h.async_redis.exists(h.config.side_effect_key(event.event_id))

    asyncio.run(go())


def test_an_unwired_ledger_does_not_break_a_turn(make_harness) -> None:
    """Every existing test builds a kernel with no recorder, and must keep passing."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [*_call("toolu_01"), Final(text="done", status=DONE)]
            await h.kernel.process_event(_qevent("scale it"))

            assert h.sink.last_text == "done"

    asyncio.run(go())


def test_a_ledger_that_refuses_the_write_fails_the_turn(make_harness) -> None:
    """A change to the world the platform has no record of is not a success.

    This same branch already fails the turn when the no-retry marker cannot be
    persisted. Losing the record of WHAT changed is not the lesser failure, and a
    turn that reports success while the ledger silently missed an action is how
    an operator learns to distrust the receipt.
    """

    async def go() -> None:
        recorder = FakeRecorder(fail_on_record=True)
        async with make_harness(actions=recorder) as h:
            h.runner.default_script = [*_call("toolu_01"), Final(text="done", status=DONE)]
            event = _qevent("scale it")
            await h.kernel.process_event(event)

            # Escalated, not completed: the turn does not report the work done
            # when the platform cannot say what it did.
            assert h.sink.last_text is not None
            assert "human" in h.sink.last_text.lower()
            assert "done" not in h.sink.last_text
            # The side effect still happened, so no retry may follow it, and
            # exactly one attempt was made.
            assert await h.async_redis.exists(h.config.side_effect_key(event.event_id))
            assert h.runner.opened == ["scale it"]

    asyncio.run(go())


def test_a_call_that_ran_under_an_approval_records_which_one(make_harness) -> None:
    """ADR-0117 decision 3 needs to know what authorized the forward call.

    A gated tool only ever executes on the resume turn an approval created, and
    that turn's event id is the approval's own deterministic key. So the gate is
    already in the kernel's hand -- it is the same string ``_is_approval_resume``
    reads for card teardown -- and recording it costs a lookup of nothing.
    """

    async def go() -> None:
        recorder = FakeRecorder()
        approval_id = "3f1b9c22-0000-4000-8000-000000000001"
        async with make_harness(actions=recorder) as h:
            h.runner.default_script = [*_call("toolu_01"), Final(text="done", status=DONE)]
            await h.kernel.process_event(
                _qevent("scale it", event_id=f"approval-{approval_id}-resolved")
            )

            assert recorder.recorded[0]["gate_approval_id"] == approval_id

    asyncio.run(go())


def test_an_ordinary_turn_records_no_gate(make_harness) -> None:
    """NULL means ungated, so an ordinary turn must not invent one."""

    async def go() -> None:
        recorder = FakeRecorder()
        async with make_harness(actions=recorder) as h:
            h.runner.default_script = [*_call("toolu_01"), Final(text="done", status=DONE)]
            await h.kernel.process_event(_qevent("scale it"))

            assert recorder.recorded[0]["gate_approval_id"] is None

    asyncio.run(go())


def test_a_turn_that_changed_the_world_says_so_in_its_reply(make_harness) -> None:
    """ADR-0117 decision 7: the person who asked gets an account of what happened.

    Before this, the platform's entire account of an agent changing production
    was prose the model wrote about itself.
    """

    async def go() -> None:
        recorder = FakeRecorder()
        async with make_harness(actions=recorder) as h:
            h.runner.default_script = [*_call("toolu_01"), Final(text="done", status=DONE)]
            await h.kernel.process_event(_qevent("scale it"))

            assert h.sink.last_text is not None
            assert "What I changed" in h.sink.last_text
            assert "can be undone" in h.sink.last_text
            # The model's own answer is still the answer; the receipt is added to
            # it rather than replacing it.
            assert h.sink.last_text.startswith("done")

    asyncio.run(go())


def test_a_read_only_turn_gets_no_receipt(make_harness) -> None:
    """Most turns are reads, and a receipt on every one of them is noise."""

    async def go() -> None:
        recorder = FakeRecorder()
        async with make_harness(actions=recorder) as h:
            h.runner.default_script = [Final(text="nothing to do", status=DONE)]
            await h.kernel.process_event(_qevent("how are things"))

            assert h.sink.last_text == "nothing to do"

    asyncio.run(go())


def test_a_call_that_never_came_back_is_not_on_the_receipt(make_harness) -> None:
    """An open record is an attempt, not an account of what changed.

    Its result never arrived, so there is nothing truthful to say about what it
    did; the row stands in the ledger and a human reads it there.
    """

    async def go() -> None:
        recorder = FakeRecorder()
        async with make_harness(actions=recorder) as h:
            h.runner.default_script = [
                _call("toolu_01")[0],
                Final(text="done", status=DONE),
            ]
            await h.kernel.process_event(_qevent("scale it"))

            assert h.sink.last_text == "done"

    asyncio.run(go())


# --- The install's receipt mode (ADR-0180) ------------------------------------
#
# The mode is read where the reply text is assembled and nowhere else, so every
# test here pairs what the person sees with what the platform still did: the
# ledger rows and the no-retry marker must be the same in every mode.


def _failed_call(call_id: str, tool: str = "file_document") -> list[SideEffectFlag]:
    """The two frames of a call that reported failure."""

    return [
        SideEffectFlag(
            tool=tool,
            call_id=call_id,
            arguments={"name": "acme-invoice.pdf"},
            detail="non-idempotent tool executed",
        ),
        SideEffectFlag(
            tool=tool,
            call_id=call_id,
            failed=True,
            result={"ok": False, "summary": "filed acme-invoice.pdf"},
            detail="non-idempotent tool completed",
        ),
    ]


def _succeeded_and_failed() -> list[object]:
    return [
        *_call("toolu_01"),
        *_failed_call("toolu_02"),
        Final(text="done", status=DONE),
    ]


def test_all_mode_replies_exactly_as_the_default_install_does(make_harness) -> None:
    """An explicit `all` is the default, byte for byte, on the same turn."""

    async def go() -> None:
        replies: list[str | None] = []
        for overrides in ({}, {"turn_receipt": "all"}):
            recorder = FakeRecorder()
            async with make_harness(actions=recorder, **overrides) as h:
                h.runner.default_script = _succeeded_and_failed()
                await h.kernel.process_event(_qevent("file it", thread=f"th-{len(replies)}"))
                replies.append(h.sink.last_text)

        assert replies[0] == replies[1]
        assert replies[0] == (
            "done\n\n"
            "_What I changed:_\n"
            "• called `scale_deployment` — can be undone\n"
            "• filed acme-invoice.pdf — failed — check before retrying"
        )

    asyncio.run(go())


def test_failures_mode_ends_a_turn_with_nothing_failed_on_its_answer(make_harness) -> None:
    """The staging call that filed nothing no longer trails the answer (#3462)."""

    async def go() -> None:
        recorder = FakeRecorder()
        async with make_harness(actions=recorder, turn_receipt="failures") as h:
            h.runner.default_script = [
                *_call("toolu_01", tool="stage_file"),
                Final(text="Nothing was filed.", status=DONE),
            ]
            event = _qevent("stage it")
            await h.kernel.process_event(event)

            assert h.sink.last_text == "Nothing was filed."
            # Recorded and marked exactly as before: only the reply changed.
            assert [r["frame"].call_id for r in recorder.recorded] == ["toolu_01"]
            assert [action_id for action_id, _ in recorder.completed] == ["a1"]
            assert await h.async_redis.exists(h.config.side_effect_key(event.event_id))

    asyncio.run(go())


def test_failures_mode_keeps_only_the_failed_line(make_harness) -> None:
    async def go() -> None:
        recorder = FakeRecorder()
        async with make_harness(actions=recorder, turn_receipt="failures") as h:
            h.runner.default_script = _succeeded_and_failed()
            event = _qevent("file it")
            await h.kernel.process_event(event)

            assert h.sink.last_text == (
                "done\n\n"
                "_What I changed:_\n"
                "• filed acme-invoice.pdf — failed — check before retrying"
            )
            assert [action_id for action_id, _ in recorder.completed] == ["a1", "a2"]
            assert await h.async_redis.exists(h.config.side_effect_key(event.event_id))

    asyncio.run(go())


def test_off_mode_replies_with_the_answer_alone_even_after_a_failed_call(
    make_harness,
) -> None:
    async def go() -> None:
        recorder = FakeRecorder()
        async with make_harness(actions=recorder, turn_receipt="off") as h:
            h.runner.default_script = _succeeded_and_failed()
            event = _qevent("file it")
            await h.kernel.process_event(event)

            assert h.sink.last_text == "done"
            # Both calls are still one record each, both completed.
            assert [r["frame"].call_id for r in recorder.recorded] == ["toolu_01", "toolu_02"]
            assert [action_id for action_id, _ in recorder.completed] == ["a1", "a2"]
            assert await h.async_redis.exists(h.config.side_effect_key(event.event_id))

    asyncio.run(go())


@pytest.mark.parametrize("mode", ["all", "failures", "off"])
def test_a_side_effect_still_blocks_retry_in_every_receipt_mode(make_harness, mode) -> None:
    """ADR-0180 decision 5: the mode never reaches the no-retry rule.

    A normally retryable classification after a side effect escalates to a
    human after exactly one attempt, with the marker persisted, whichever mode
    the install chose.
    """

    async def go() -> None:
        recorder = FakeRecorder()
        async with make_harness(actions=recorder, turn_receipt=mode) as h:
            h.runner.default_script = [
                *_call("toolu_01"),
                ErrorEvent(message="boom", classification="runner-error"),
                Final(text="failed", status=FAIL),
            ]
            event = _qevent("scale it")
            await h.kernel.process_event(event)

            assert h.config.turn_receipt == mode
            assert h.runner.opened == ["scale it"]
            assert h.sink.last_text is not None and "human" in h.sink.last_text.lower()
            assert await h.async_redis.exists(h.config.side_effect_key(event.event_id))
            assert await h.async_redis.exists(h.config.done_key(event.event_id))
            assert [r["frame"].call_id for r in recorder.recorded] == ["toolu_01"]

    asyncio.run(go())
