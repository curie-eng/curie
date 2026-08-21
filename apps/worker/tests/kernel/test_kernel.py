"""Kernel rule tests: routing, steer, finish-race, interrupt, side-effect/retry.

Each rule is provoked against a real Valkey, the real G1 substrate, and a
scriptable in-process fake runner; only Slack and the model are faked.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
import uuid
from collections.abc import Callable

import pytest
from aci_protocol import (
    ErrorEvent,
    Final,
    QueuedTurn,
    ReplyHandle,
    SessionStatus,
    SideEffectFlag,
    TextDelta,
    ToolNote,
    TurnSource,
)
from channel_protocol.reply import ReplyAck, ReplyEvent, ReplyTarget
from curie_worker import kernel as kernel_module
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.kernel import ThreadBusyError
from curie_worker.reply_sink import TargetRoute
from curie_worker.runner_client import RunnerError
from curie_worker.sandbox import QuotaRejection

DONE = SessionStatus.DONE
IDLE = SessionStatus.IDLE_AWAITING_INPUT
FAIL = SessionStatus.CLASSIFIED_FAILURE


def _qevent(
    text: str,
    *,
    thread: str = "th-1",
    event_id: str | None = None,
    placeholder: str | None = "p-1",
    endpoint: str | None = None,
    source: TurnSource = TurnSource.SLACK,
) -> QueuedTurn:
    return QueuedTurn(
        event_id=event_id or uuid.uuid4().hex,
        conversation_id=thread,
        author="U1",
        text=text,
        reply_handle=ReplyHandle(
            kind="slack", channel="C1", placeholder=placeholder, endpoint=endpoint
        ),
        received_at="2026-07-05T00:00:00+00:00",
        source=source,
    )


def _thread_key(thread: str) -> str:
    return f"slack:C1:{thread}"


async def _wait_until(pred: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met within timeout")


def test_new_turn_streams_to_slack_and_acks(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [
                TextDelta(text="Hello "),
                TextDelta(text="world"),
                Final(text="Hello world", status=DONE),
            ]
            ev = _qevent("hi")
            await h.kernel.process_event(ev)

            assert h.runner.opened == ["hi"]
            assert h.sink.last_text == "Hello world"
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_tool_context_streams_immediately_without_polluting_final_reply(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(slack_edit_min_interval_s=60.0) as h:
            h.runner.default_script = [
                TextDelta(text="Answer so far"),
                ToolNote(text="searching...", tool="WebSearch"),
                TextDelta(text=" and more"),
                ToolNote(text="opening result", tool="WebSearch"),
                Final(text="Final answer", status=DONE),
            ]
            event = _qevent("research this")

            await h.kernel.process_event(event)

            texts = [text for _, _, text in h.sink.updates]
            preview = "Answer so far\n  -> [WebSearch] searching..."
            assert preview in texts
            assert texts.index(preview) == texts.index("Answer so far") + 1
            assert [text for text in texts if "  -> " in text] == [preview]
            assert "Answer so far and more" not in texts
            assert all("opening result" not in text for text in texts)
            final_text = h.sink.last_text
            assert final_text == "Final answer"
            assert "WebSearch" not in final_text

    asyncio.run(go())


def test_tool_context_distinguishes_empty_and_absent_tool_names(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(slack_edit_min_interval_s=0.0) as h:
            h.runner.default_script = [
                TextDelta(text="Answer so far"),
                ToolNote(text="empty name", tool=""),
                ToolNote(text="unnamed", tool=None),
                Final(text="Final answer", status=DONE),
            ]

            await h.kernel.process_event(_qevent("research this"))

            texts = [text for _, _, text in h.sink.updates]
            assert "Answer so far\n  -> [] empty name" in texts
            assert "Answer so far\n  -> unnamed" in texts
            assert h.sink.last_text == "Final answer"

    asyncio.run(go())


def test_tool_context_delivery_failure_is_fail_soft(make_harness) -> None:
    async def go() -> None:
        async with make_harness(slack_edit_min_interval_s=60.0) as h:
            h.runner.default_script = [
                TextDelta(text="Partial answer"),
                ToolNote(text="running command", tool="Bash"),
                ToolNote(text="reading output", tool="Bash"),
                Final(text="Completed answer", status=DONE),
            ]
            context_preview = "Partial answer\n  -> [Bash] running command"
            original_emit = h.sink.emit
            context_attempts: list[str] = []

            async def fail_context_emit(
                reply_event: ReplyEvent,
                *,
                route: TargetRoute,
                best_effort_unreachable: bool = False,
            ) -> ReplyAck:
                text = getattr(reply_event, "text", None)
                if isinstance(text, str) and "\n  -> " in text:
                    context_attempts.append(text)
                    raise RuntimeError("injected context delivery failure")
                return await original_emit(
                    reply_event,
                    route=route,
                    best_effort_unreachable=best_effort_unreachable,
                )

            h.sink.emit = fail_context_emit  # type: ignore[method-assign]
            event = _qevent("run it")

            await h.kernel.process_event(event)

            assert context_attempts == [context_preview]
            assert h.sink.last_text == "Completed answer"
            assert await h.async_redis.exists(h.config.done_key(event.event_id))

    asyncio.run(go())


def test_failed_context_edit_does_not_suppress_identical_final_delivery(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(slack_edit_min_interval_s=60.0) as h:
            final_text = "Partial answer\n  -> [Bash] running command"
            h.runner.default_script = [
                TextDelta(text="Partial answer"),
                ToolNote(text="running command", tool="Bash"),
                Final(text=final_text, status=DONE),
            ]
            original_emit = h.sink.emit
            matching_attempts = 0

            async def fail_first_matching_emit(
                reply_event: ReplyEvent,
                *,
                route: TargetRoute,
                best_effort_unreachable: bool = False,
            ) -> ReplyAck:
                nonlocal matching_attempts
                if getattr(reply_event, "text", None) == final_text:
                    matching_attempts += 1
                    if matching_attempts == 1:
                        raise RuntimeError("injected context delivery failure")
                return await original_emit(
                    reply_event,
                    route=route,
                    best_effort_unreachable=best_effort_unreachable,
                )

            h.sink.emit = fail_first_matching_emit  # type: ignore[method-assign]
            event = _qevent("run it")

            await h.kernel.process_event(event)

            assert matching_attempts == 2, (
                "final delivery was not attempted after the failed context edit"
            )
            assert h.sink.last_text == final_text
            assert await h.async_redis.exists(h.config.done_key(event.event_id))

    asyncio.run(go())


def test_message_creating_tool_context_delivery_failure_stays_loud(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [
                ToolNote(text="running command", tool="Bash"),
                Final(text="Completed answer", status=DONE),
            ]
            context_preview = "  -> [Bash] running command"
            booting = h.config.booting_text
            original_emit = h.sink.emit
            failed_updates: list[str] = []

            async def fail_context_emit(
                reply_event: ReplyEvent,
                *,
                route: TargetRoute,
                best_effort_unreachable: bool = False,
            ) -> ReplyAck:
                text = getattr(reply_event, "text", None)
                if text == booting:
                    failed_updates.append(text)
                    raise RuntimeError("injected booting delivery failure")
                if text == context_preview:
                    failed_updates.append(text)
                    raise RuntimeError("injected message creation failure")
                return await original_emit(
                    reply_event,
                    route=route,
                    best_effort_unreachable=best_effort_unreachable,
                )

            h.sink.emit = fail_context_emit  # type: ignore[method-assign]
            event = _qevent("run it", placeholder=None)

            with pytest.raises(RuntimeError, match="message creation failure"):
                await h.kernel.process_event(event)

            assert failed_updates == [booting, context_preview]
            assert not await h.async_redis.exists(h.config.done_key(event.event_id))
            assert h.sink.last_text is None

    asyncio.run(go())


def test_null_placeholder_turn_runs_and_posts_its_own_reply(make_harness) -> None:
    """ADR-0079: a turn with nothing to edit creates its own message.

    This asserts the reverse of what the kernel used to do. Until this change it
    raised on a null placeholder before touching the runner or the sink, which
    left the frozen contract (``placeholder: str | None``) and the runtime
    disagreeing about what the wire permitted.
    """

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="digest ready", status=DONE)]
            event = _qevent("run the digest", placeholder=None)

            await h.kernel.process_event(event)

            assert h.runner.opened == ["run the digest"]
            # It POSTED rather than edited: the delivery minted a ref of its own.
            assert [text for _, _, text in h.sink.text_posts]
            assert h.sink.last_text == "digest ready"
            assert await h.async_redis.exists(h.config.done_key(event.event_id))

    asyncio.run(go())


def test_placeholder_less_turn_posts_once_then_edits_that_message(make_harness) -> None:
    """The minted ref is adopted, so a streamed job does not spam the channel.

    Without adoption every throttled delta would create another message, which is
    the failure a null placeholder would otherwise produce on a chatty turn.
    """

    async def go() -> None:
        async with make_harness(slack_edit_min_interval_s=0.0) as h:
            h.runner.default_script = [
                TextDelta(text="one "),
                TextDelta(text="two "),
                TextDelta(text="three"),
                Final(text="one two three", status=DONE),
            ]

            await h.kernel.process_event(_qevent("go", placeholder=None))

            # Exactly one message was created for the whole turn...
            assert len(h.sink.text_posts) == 1, h.sink.text_posts
            minted = h.sink.text_posts[0][1]
            # ...and every later delivery addressed that same message.
            refs = {ref for _, ref, _ in h.sink.updates}
            assert refs == {minted}, refs
            assert len(h.sink.updates) > 1, "the turn only delivered once; nothing was edited"

    asyncio.run(go())


def test_a_job_never_steers_a_live_session(make_harness) -> None:
    """ADR-0079: jobs are outputs, not steering inputs.

    A person's follow-up on a busy thread steers. A job on the same thread must
    not, because folding a scheduled digest into someone's conversation changes
    what that person's turn says.
    """

    async def go() -> None:
        async with make_harness() as h:
            h.runner.turn_active = True
            event = _qevent(
                "nightly digest", placeholder=None, source=TurnSource.CRON
            )

            for _ in range(5):
                with pytest.raises(ThreadBusyError):
                    await h.kernel.process_event(event)

            assert h.sink.text_posts == [], "a deferred job left a booting notice"
            assert h.runner.steers == [], "a job steered a live session"
            assert h.runner.opened == [], "a job opened a turn beside a live one"

    asyncio.run(go())


def test_a_job_runs_normally_when_the_thread_is_idle(make_harness) -> None:
    """The deferral is conditional. An idle thread runs the job immediately."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.turn_active = False
            h.runner.default_script = [Final(text="digest", status=DONE)]

            await h.kernel.process_event(
                _qevent("nightly digest", placeholder=None, source=TurnSource.CRON)
            )

            assert h.runner.opened == ["nightly digest"]
            assert h.runner.steers == []

    asyncio.run(go())


def test_an_unreadable_session_defers_the_job(make_harness) -> None:
    """The liveness read fails CLOSED.

    A runner that cannot answer is not evidence of an idle thread. Reading the
    failure as idle would open a turn beside one that may already be running,
    which breaks the kernel's one-live-turn-per-thread invariant -- so the job
    defers instead. Added because a mutation flipping this to fail-open left the
    suite green.
    """

    async def go() -> None:
        async with make_harness() as h:
            h.runner.turn_active = False
            h.runner.status_fails = True

            with pytest.raises(ThreadBusyError):
                await h.kernel.process_event(
                    _qevent("digest", placeholder=None, source=TurnSource.CRON)
                )

            assert h.runner.opened == [], "an unreadable session let a job open a turn"

    asyncio.run(go())


def test_a_status_without_turn_active_defers_the_job(make_harness) -> None:
    """A 200 that omits the field is as unreadable as a 500.

    Separate from the 500 case on purpose: a runner answering successfully with a
    shape we cannot interpret is the likelier real-world drift, and reading a
    missing field as False would silently treat every such runner as idle.
    """

    async def go() -> None:
        async with make_harness() as h:
            h.runner.turn_active = False
            h.runner.status_malformed = True

            with pytest.raises(ThreadBusyError):
                await h.kernel.process_event(
                    _qevent("digest", placeholder=None, source=TurnSource.CRON)
                )

            assert h.runner.opened == []

    asyncio.run(go())


def test_streamer_adopts_its_own_minted_ref(make_harness) -> None:
    """A streamer that posts its first delta edits that message thereafter.

    Driven directly rather than through ``process_event`` because the booting
    notice normally mints the ref first, which hides this path: it is reached
    when that delivery failed, and a mutation removing the adoption left the
    end-to-end test green. One message, then edits, is the property.
    """

    async def go() -> None:
        async with make_harness() as h:
            reply = kernel_module._ThrottledReply(
                h.sink,
                target=ReplyTarget(
                    kind="slack", address="C1", conversation_id="th-1", reply_ref=None
                ),
                route=TargetRoute(),
                min_interval_s=0.0,
            )
            await reply.stream("one")
            await reply.stream("one two")
            await reply.finalize("one two three")

            assert len(h.sink.text_posts) == 1, h.sink.text_posts
            minted = h.sink.text_posts[0][1]
            assert {ref for _, ref, _ in h.sink.updates} == {minted}
            assert len(h.sink.updates) == 3

    asyncio.run(go())


def test_a_person_still_steers_a_live_session(make_harness) -> None:
    """The guard is scoped to jobs and leaves the conversational path alone."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.turn_active = True

            await h.kernel.process_event(_qevent("actually, make it shorter"))

            assert h.runner.steers == ["actually, make it shorter"]

    asyncio.run(go())


def test_shimmer_clears_status_when_the_turn_ends(make_harness) -> None:
    # With shimmer on, the kernel clears the assistant-thread status it
    # dispatcher set, on the turn's terminal exit (a plain success here).
    async def go() -> None:
        async with make_harness(shimmer=True) as h:
            h.runner.default_script = [Final(text="done", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tS"))
            assert ("C1", "tS") in h.sink.status_clears

    asyncio.run(go())


def test_no_status_clear_when_shimmer_is_off(make_harness) -> None:
    # With shimmer OFF the kernel never touches the assistant status. Pinned
    # explicitly: shimmer now defaults ON (#1182), so leaning on the default here
    # would silently stop exercising the off path.
    async def go() -> None:
        async with make_harness(shimmer=False) as h:
            h.runner.default_script = [Final(text="done", status=DONE)]
            await h.kernel.process_event(_qevent("hi"))
            assert h.sink.status_clears == []

    asyncio.run(go())


def test_status_is_cleared_by_default(make_harness) -> None:
    # The mirror of the test above, and the reason the default flipped (#1182):
    # the worker shimmers by default, and editing the placeholder does not
    # auto-clear a Slack status, so the worker must clear it on the way out or
    # the caption lingers until Slack's own timeout.
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="done", status=DONE)]
            await h.kernel.process_event(_qevent("hi"))
            assert h.sink.status_clears, "the shipped default must clear the caption"

    asyncio.run(go())


def test_followup_steers_the_live_turn(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=DONE)]

            e1 = _qevent("first", thread="tA")
            t1 = asyncio.create_task(h.kernel.process_event(e1))
            await _wait_until(lambda: h.runner.turn_active)

            # A follow-up on the same thread steers the live turn, not a new one.
            await h.kernel.process_event(_qevent("second", thread="tA"))
            assert h.runner.steers == ["second"]
            assert h.runner.opened == ["first"]

            hold.set()
            await t1

    asyncio.run(go())


def test_finish_race_falls_back_to_a_fresh_turn(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            # First turn completes; the sandbox stays live but idle (no turn).
            h.runner.default_script = [Final(text="one", status=DONE)]
            await h.kernel.process_event(_qevent("first", thread="tB"))

            # Follow-up: the steer hits 409 (no active turn) and the kernel opens
            # a fresh turn on the same idle sandbox.
            h.runner.default_script = [Final(text="two", status=DONE)]
            await h.kernel.process_event(_qevent("second", thread="tB"))

            assert h.runner.steers == []  # steer returned 409, not delivered
            assert h.runner.opened == ["first", "second"]
            assert h.sink.last_text == "two"

    asyncio.run(go())


def test_drop_mid_run_retries_then_succeeds(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            # Attempt 1 streams a delta then the stream ends with no final (a
            # mid-run drop). Attempt 2 completes.
            h.runner.turn_scripts = [
                [TextDelta(text="partial")],
                [TextDelta(text="full"), Final(text="full done", status=DONE)],
            ]
            ev = _qevent("go")
            await h.kernel.process_event(ev)

            assert h.runner.opened == ["go", "go"]  # retried
            assert h.sink.last_text == "full done"

    asyncio.run(go())


def test_side_effect_failure_escalates_without_retry(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            # A normally-retryable classification (runner-error) must NOT retry
            # once a side effect has executed.
            h.runner.default_script = [
                SideEffectFlag(tool="deploy"),
                ErrorEvent(message="boom", classification="runner-error"),
                Final(text="failed", status=FAIL),
            ]
            ev = _qevent("do it")
            await h.kernel.process_event(ev)

            assert h.runner.opened == ["do it"]  # exactly one attempt, no retry
            assert h.sink.last_text is not None and "human" in h.sink.last_text.lower()
            assert await h.async_redis.exists(h.config.side_effect_key(ev.event_id))
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_rate_limit_retries_then_succeeds(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            h.runner.turn_scripts = [
                [
                    ErrorEvent(message="rl", classification="rate-limit"),
                    Final(text="f", status=FAIL),
                ],
                [Final(text="recovered", status=DONE)],
            ]
            await h.kernel.process_event(_qevent("go"))

            assert h.runner.opened == ["go", "go"]
            assert h.sink.last_text == "recovered"

    asyncio.run(go())


def test_turn_start_failure_is_retryable_not_a_stall(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            # The first /v1/event returns 500 (transient runner error / not ready).
            # This must be turned into a bounded retry, not escape and leave the
            # entry pending for the long reclaim window.
            h.runner.event_fail_times = 1
            h.runner.default_script = [Final(text="recovered", status=DONE)]

            await h.kernel.process_event(_qevent("go"))

            assert h.runner.opened == ["go", "go"]  # failed start, then retried
            assert h.sink.last_text == "recovered"

    asyncio.run(go())


def test_budget_exceeded_escalates_without_retry(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [
                ErrorEvent(message="over budget", classification="budget-exceeded"),
                Final(text="f", status=FAIL),
            ]
            await h.kernel.process_event(_qevent("go"))

            assert h.runner.opened == ["go"]  # budget-exceeded is not retryable
            assert h.sink.last_text is not None and "human" in h.sink.last_text.lower()

    asyncio.run(go())


def test_retries_are_bounded_then_escalate(make_harness) -> None:
    async def go() -> None:
        async with make_harness(max_attempts=3) as h:
            # rate-limit every attempt -> retried up to max_attempts, then escalate.
            h.runner.default_script = [
                ErrorEvent(message="rl", classification="rate-limit"),
                Final(text="f", status=FAIL),
            ]
            await h.kernel.process_event(_qevent("go"))

            assert len(h.runner.opened) == 3
            assert h.sink.last_text is not None and "human" in h.sink.last_text.lower()

    asyncio.run(go())


@pytest.mark.parametrize("slack_no_edit_streaming", [False, True])
def test_quota_capacity_is_terminal_without_retry_or_runner_turn(
    make_harness, slack_no_edit_streaming: bool
) -> None:
    async def go() -> None:
        async with make_harness(
            max_attempts=3,
            slack_no_edit_streaming=slack_no_edit_streaming,
            claim_timeout_seconds=0.05,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                resource="limits.cpu",
                requested="2",
                used="7",
                hard="8",
            )
            endpoint = "http://127.0.0.1:43199"
            ev = _qevent("go", endpoint=endpoint)

            await h.kernel.process_event(ev)

            expected = (
                "This agent is at sandbox capacity. ResourceQuota curie-sandbox-quota "
                "rejected limits.cpu: requested 2, observed usage 7, hard limit 8. "
                "Try again after another conversation releases its sandbox."
            )
            expected_updates = [("C1", "p-1", expected)]
            if not slack_no_edit_streaming:
                expected_updates.insert(0, ("C1", "p-1", h.config.booting_text))
            assert h.sink.updates == expected_updates
            assert h.sink.update_endpoints == [endpoint] * len(expected_updates)
            assert len(h.fake_k8s.claim_envs) == 1
            assert h.runner.opened == []
            assert h.kernel._order_locks == {}
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_approval_resume_capacity_retries_then_escalates(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            max_attempts=3,
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
        ) as h:
            thread = "t-approval-capacity"
            await asyncio.to_thread(h.substrate.claim, thread)
            await asyncio.to_thread(h.substrate.suspend, thread, history_ref="history-1")
            h.fake_k8s.claim_envs.clear()
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                resource="limits.cpu",
                requested="1",
                used="8",
                hard="8",
            )
            endpoint = "http://127.0.0.1:43199"
            ev = _qevent(
                "approved continuation",
                thread=thread,
                event_id="approval-example-resolved",
                endpoint=endpoint,
            )

            await h.kernel.process_event(ev)

            assert len(h.fake_k8s.claim_envs) == 3
            assert h.runner.opened == []
            assert h.sink.updates == [
                (
                    "C1",
                    "p-1",
                    "The run failed (runner-error) after 3 attempt(s). Flagging for a human.",
                )
            ]
            assert h.sink.update_endpoints == [endpoint]
            assert h.kernel._order_locks == {}
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_claim_timeout_without_quota_retries_then_escalates(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            max_attempts=3,
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.02,
        ) as h:
            h.fake_k8s.bind_ready = False
            ev = _qevent("go")

            await h.kernel.process_event(ev)

            assert len(h.fake_k8s.claim_envs) == 3
            assert h.runner.opened == []
            assert h.sink.updates == [
                (
                    "C1",
                    "p-1",
                    "The run failed (runner-error) after 3 attempt(s). Flagging for a human.",
                )
            ]
            assert "sandbox capacity" not in h.sink.updates[0][2].lower()
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_interrupt_hard_stops_the_live_turn(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="thinking")]
            h.runner.tail = [Final(text="stopped", status=IDLE)]

            e1 = _qevent("start", thread="tI")
            t1 = asyncio.create_task(h.kernel.process_event(e1))
            await _wait_until(lambda: h.runner.turn_active)

            signalled = await h.kernel.interrupt_thread(_thread_key("tI"), "user stop")
            assert signalled is True
            assert h.runner.interrupts == 1

            await t1
            assert h.sink.last_text == "stopped"

    asyncio.run(go())


def test_interrupt_agent_signals_other_threads_past_a_wedged_runner(
    make_harness, monkeypatch
) -> None:
    """#742: interrupt_agent fans out over an agent's live threads. A single
    wedged runner -- one that accepts the interrupt call and then never
    answers -- must not cost the agent's other threads up to
    `RunnerClient.interrupt`'s own request budget: the kill switch is "the one
    control that is supposed to work when things are broken." Each thread's
    interrupt is individually bounded and the fan-out runs concurrently, so a
    permanently-wedged thread times out (logged, not raised) while the other
    threads are still signalled well inside the test's own generous ceiling.

    The wedge is injected at the runner-client seam with an event that is never
    set, the same deterministic technique #739's release_thread test uses,
    rather than racing real timing against a live HTTP hang."""

    async def go() -> None:
        async with make_harness() as h:
            agent_id = uuid.uuid4()
            h.runner.default_script = [Final(text="hi", status=DONE)]
            threads = ("tKillA", "tKillB", "tKillC")
            for thread in threads:
                await h.kernel.process_event(_qevent("hi", thread=thread))
            h.kernel._active_by_agent[agent_id] = {_thread_key(t) for t in threads}

            monkeypatch.setattr(kernel_module, "_KILL_INTERRUPT_TIMEOUT_S", 0.2)

            wedged = asyncio.Event()  # never set: the first thread's runner hangs forever
            attempted: list[str] = []

            async def maybe_wedge(base_url: str, reason: str, token: str | None = None) -> None:
                attempted.append(reason)
                if len(attempted) == 1:
                    await wedged.wait()

            monkeypatch.setattr(h.kernel._runner, "interrupt", maybe_wedge)

            try:
                signalled = await asyncio.wait_for(h.kernel.interrupt_agent(agent_id), timeout=2.0)
            finally:
                wedged.set()

            assert len(attempted) == 3  # every thread was attempted, none blocked the rest
            assert signalled == 2  # the wedged thread times out; the other two still land

    asyncio.run(go())


def test_duplicate_event_is_idempotent(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="one", status=DONE)]
            ev = _qevent("hi", event_id="dup-1")
            await h.kernel.process_event(ev)
            await h.kernel.process_event(ev)  # same event id

            assert h.runner.opened == ["hi"]  # processed exactly once

    asyncio.run(go())


def test_ordering_preserved_under_concurrent_sends(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="w")]
            h.runner.tail = [Final(text="done", status=DONE)]

            # Both events for the same thread are dispatched concurrently, with no
            # pre-sequencing: the FIFO in-process lock must make the first-created
            # event open the turn and the second steer into it. Without that lock
            # the order (and whether a second turn is forked) would be a race, so
            # this asserts the ordering guarantee, not just that steering works.
            e1 = _qevent("first", thread="tO", event_id="o1")
            e2 = _qevent("second", thread="tO", event_id="o2")
            t1 = asyncio.create_task(h.kernel.process_event(e1))
            t2 = asyncio.create_task(h.kernel.process_event(e2))
            await _wait_until(lambda: h.runner.turn_active and bool(h.runner.steers))

            assert h.runner.opened == ["first"]  # exactly one turn, the first event
            assert h.runner.steers == ["second"]  # the second folded in as a steer

            hold.set()
            await asyncio.gather(t1, t2)

    asyncio.run(go())


def test_prior_side_effect_marker_escalates_without_running(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            ev = _qevent("retry me", event_id="se-1")
            # A prior attempt executed a side effect then the worker crashed: the
            # marker is set but the event never reached done. It must escalate,
            # never re-run the non-idempotent action.
            await h.async_redis.set(h.config.side_effect_key(ev.event_id), "1")

            await h.kernel.process_event(ev)

            assert h.runner.opened == []  # no turn was ever opened
            assert h.sink.last_text is not None and "human" in h.sink.last_text.lower()
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_suspended_thread_is_resumed_not_forked(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="one", status=DONE)]
            await h.kernel.process_event(_qevent("first", thread="tR"))

            # Suspend the thread (records a rehydrate ref on the route).
            await asyncio.to_thread(h.substrate.suspend, _thread_key("tR"), history_ref="hist-1")

            # A new event on a suspended thread must resume (carry the history)
            # rather than silently fork a fresh, history-less session.
            h.runner.default_script = [Final(text="resumed", status=DONE)]
            await h.kernel.process_event(_qevent("second", thread="tR"))

            assert h.runner.opened == ["first", "second"]
            assert h.sink.last_text == "resumed"

    asyncio.run(go())


async def _route_key(async_redis, thread: str) -> str:
    keys = [k async for k in async_redis.scan_iter(match=f"*:route:{thread}")]
    assert len(keys) == 1, f"expected one route key for {thread}, found {keys}"
    return keys[0]


def test_live_route_reuse_refreshes_ttl(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            # First event creates a live route with the substrate's route TTL.
            h.runner.default_script = [Final(text="one", status=DONE)]
            await h.kernel.process_event(_qevent("first", thread="tTTL"))

            route_key = await _route_key(h.async_redis, _thread_key("tTTL"))
            # Simulate time passing by dropping the TTL low.
            await h.async_redis.expire(route_key, 5)
            assert await h.async_redis.ttl(route_key) <= 5

            # A second event reuses the live route; routing through claim() must
            # refresh the TTL (a regression to lookup() would leave it at ~5 and
            # let the reaper delete a busy thread's sandbox).
            h.runner.default_script = [Final(text="two", status=DONE)]
            await h.kernel.process_event(_qevent("second", thread="tTTL"))
            assert await h.async_redis.ttl(route_key) > 5

    asyncio.run(go())


def test_steered_followup_placeholder_is_retired(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="w")]
            h.runner.tail = [Final(text="done", status=DONE)]

            e1 = _qevent("first", thread="tPH", placeholder="ph-1")
            t1 = asyncio.create_task(h.kernel.process_event(e1))
            await _wait_until(lambda: h.runner.turn_active)

            # The follow-up carries its own placeholder; once steered, that
            # placeholder must be retired (not left stuck on "working").
            e2 = _qevent("second", thread="tPH", placeholder="ph-2")
            await h.kernel.process_event(e2)

            folded = [u for u in h.sink.updates if u[1] == "ph-2"]
            assert folded, "the steered follow-up's placeholder was never updated"
            assert "folded" in folded[-1][2].lower()

            hold.set()
            await t1

    asyncio.run(go())


def test_order_lock_map_evicts_after_processing(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="ok", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tEV"))
            # Ref-counted eviction: no per-thread lock entry lingers once the last
            # holder releases (a regression would leak one entry per thread seen).
            assert h.kernel._order_locks == {}

    asyncio.run(go())


# --- Per-sandbox runner token delivery, end-to-end (issue #63) ----------------


class _FakeResolved:
    """The minimal resolved deployment the kernel reads (agent_id plus the
    binding row's egress pair; shimmer off, so packs are never sampled)."""

    def __init__(self, agent_id: uuid.UUID) -> None:
        self.agent_id = agent_id
        # Unset on this binding, so the turn keeps the route the server minted
        # onto its reply handle (ADR-0096 EB-B2).
        self.endpoint: str | None = None
        self.adapter: str | None = None


class _TokenBinding:
    """A binding whose boot_env injects a known runner token into the claim env,
    so the test can assert the exact value the worker delivers as the Bearer
    header. The claim-time minting itself is covered by the binding unit tests;
    this proves the claim->handle->kernel->runner delivery path."""

    def __init__(self, token: str, agent_id: uuid.UUID) -> None:
        self._token = token
        self._agent_id = agent_id

    async def resolve(self, _kind: str, _channel: str) -> _FakeResolved:
        return _FakeResolved(self._agent_id)

    def boot_env(self, _resolved: object, _thread_key: str) -> dict[str, str]:
        return {"CURIE_RUNNER_TOKEN": self._token}

    def packs_for(self, _resolved: object) -> BehaviorPacks:
        return BehaviorPacks()


def test_kernel_delivers_claim_token_as_bearer_header(make_harness) -> None:
    async def go() -> None:
        binding = _TokenBinding("tok-24", uuid.uuid4())
        async with make_harness(binding=binding) as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="w")]
            h.runner.tail = [Final(text="done", status=DONE)]

            e1 = _qevent("first", thread="tTok")
            t1 = asyncio.create_task(h.kernel.process_event(e1))
            await _wait_until(lambda: h.runner.turn_active)

            # Event path: the opening /v1/event carried the claim-minted token.
            assert h.runner.event_headers
            assert h.runner.event_headers[-1].get("Authorization") == "Bearer tok-24"

            # Steer path: a follow-up folded into the live turn carries it too.
            await h.kernel.process_event(_qevent("second", thread="tTok"))
            assert h.runner.steer_headers
            assert h.runner.steer_headers[-1].get("Authorization") == "Bearer tok-24"

            # Interrupt path: the explicit hard stop carries it as well.
            await h.kernel.interrupt_thread(_thread_key("tTok"), "user stop")
            assert h.runner.interrupt_headers
            assert h.runner.interrupt_headers[-1].get("Authorization") == "Bearer tok-24"

            hold.set()
            await t1

    asyncio.run(go())


# --- #31: no-edit streaming mode ----------------------------------------------

_MULTI_DELTA = [
    TextDelta(text="a"),
    ToolNote(text="checking", tool="ExampleTool"),
    TextDelta(text="b"),
    TextDelta(text="c"),
    Final(text="abc final", status=DONE),
]


def test_no_edit_streaming_edits_placeholder_once(make_harness) -> None:
    async def go() -> None:
        async with make_harness(slack_no_edit_streaming=True) as h:
            # Text and tool frames arrive, but no edit mode updates only the final.
            h.runner.default_script = list(_MULTI_DELTA)
            await h.kernel.process_event(_qevent("go"))

            assert len(h.sink.updates) == 1
            assert h.sink.last_text == "abc final"

    asyncio.run(go())


def test_no_edit_streaming_suppresses_tool_context_and_finalizes_once(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(slack_no_edit_streaming=True) as h:
            h.runner.default_script = [
                TextDelta(text="answer in progress"),
                ToolNote(text="checking", tool="ExampleTool"),
                Final(text="final answer", status=DONE),
            ]

            await h.kernel.process_event(_qevent("go"))

            assert h.sink.updates == [("C1", "p-1", "final answer")]

    asyncio.run(go())


def test_default_streaming_edits_more_than_once(make_harness) -> None:
    async def go() -> None:
        # Deletion-test guard: with no-edit OFF (default; conftest sets
        # slack_edit_min_interval_s=0.0) the SAME multi-delta script produces
        # more than one edit, proving the flag actually changes behavior.
        async with make_harness() as h:
            h.runner.default_script = list(_MULTI_DELTA)
            await h.kernel.process_event(_qevent("go"))

            assert len(h.sink.updates) > 1
            assert h.sink.last_text == "abc final"

    asyncio.run(go())


def test_booting_state_edits_placeholder_before_answer(make_harness) -> None:
    # A fresh-claim turn edits the placeholder to the booting caption at the very
    # start of the attempt, before the sandbox-claim wait, so the "booting a
    # runner" state is visible ahead of the streamed answer on the same message.
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [
                TextDelta(text="Hello "),
                TextDelta(text="world"),
                Final(text="Hello world", status=DONE),
            ]
            ev = _qevent("hi", thread="tBOOT", placeholder="ph-boot")
            await h.kernel.process_event(ev)

            booting = h.config.booting_text
            on_ph = [
                (i, u)
                for i, u in enumerate(h.sink.updates)
                if u[0] == ev.reply_handle.channel and u[1] == ev.reply_handle.placeholder
            ]
            booting_idxs = [i for i, u in on_ph if u[2] == booting]
            answer_idxs = [i for i, u in on_ph if u[2] != booting]
            assert booting_idxs, "the booting caption was never edited onto the placeholder"
            assert answer_idxs, "no streamed-answer update landed on the placeholder"
            assert min(booting_idxs) < min(answer_idxs), (
                "the booting caption must precede the first streamed-answer update"
            )

    asyncio.run(go())


def test_reply_endpoint_is_threaded_to_the_sink(make_harness) -> None:
    # Issue #19: a turn carrying a per-turn reply endpoint must route every sink
    # edit for that turn through that endpoint (not the worker default), so a
    # no-Slack CLI stub and a real workspace can coexist on one worker.
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [
                TextDelta(text="working "),
                Final(text="done", status=DONE),
            ]
            await h.kernel.process_event(
                _qevent("hi", thread="tEP", endpoint="http://stub:8155/api/")
            )

            assert h.sink.last_text == "done"
            # Every recorded update for this turn carried the per-turn endpoint.
            assert h.sink.update_endpoints, "no sink update recorded"
            assert set(h.sink.update_endpoints) == {"http://stub:8155/api/"}

    asyncio.run(go())


def test_reply_endpoint_defaults_to_none_for_the_worker_default(make_harness) -> None:
    # A turn with no per-turn endpoint threads None, so the sink uses its worker
    # default (the pre-#19 behavior is preserved for real-Slack ingress).
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="ok", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tEPNONE"))
            assert set(h.sink.update_endpoints) == {None}

    asyncio.run(go())


def test_booting_update_failure_never_fails_the_turn(make_harness) -> None:
    # The booting edit is best-effort: if the Slack update for the booting caption
    # raises, the turn still runs to its normal terminal answer. Inject a failure
    # on the first booting-caption update and prove both that it fired and that the
    # turn completed anyway.
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="all good", status=DONE)]

            booting = h.config.booting_text
            original_emit = h.sink.emit
            fired = {"n": 0}

            async def flaky_emit(
                event: ReplyEvent,
                *,
                route: TargetRoute,
                best_effort_unreachable: bool = False,
            ) -> ReplyAck:
                text = getattr(event, "text", None)
                if text == booting and fired["n"] == 0:
                    fired["n"] += 1
                    raise RuntimeError("injected Slack failure on booting update")
                return await original_emit(
                    event,
                    route=route,
                    best_effort_unreachable=best_effort_unreachable,
                )

            h.sink.emit = flaky_emit  # type: ignore[method-assign]

            ev = _qevent("hi", thread="tBOOTFAIL", placeholder="ph-boot-fail")
            await h.kernel.process_event(ev)

            assert fired["n"] > 0, "the booting update was never attempted"
            assert h.sink.last_text == "all good"
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_release_thread_force_releases_a_live_route(make_harness) -> None:
    """#713: an operator can force-release a thread's sandbox even though it
    has a live (not suspended, not dead) route -- the whole point is to evict
    a sandbox that is up and answering but running stale env, not just one
    that already died on its own (that path -- claim()'s stale-sandbox
    eviction -- already existed)."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tRelease"))
            assert h.substrate.lookup(_thread_key("tRelease")) is not None  # the route is live

            released = await h.kernel.release_thread(_thread_key("tRelease"))
            assert released is True
            assert h.substrate.lookup(_thread_key("tRelease")) is None  # gone: next claim is fresh

    asyncio.run(go())


def test_release_thread_interrupts_a_live_turn_first(make_harness) -> None:
    """Releasing a thread mid-turn interrupts it first rather than yanking the
    claim out from under a running turn silently."""

    async def go() -> None:
        async with make_harness() as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="thinking")]
            h.runner.tail = [Final(text="stopped", status=IDLE)]

            e1 = _qevent("start", thread="tReleaseMidTurn")
            t1 = asyncio.create_task(h.kernel.process_event(e1))
            await _wait_until(lambda: h.runner.turn_active)

            released = await h.kernel.release_thread(_thread_key("tReleaseMidTurn"))
            assert released is True
            assert h.runner.interrupts == 1  # interrupted, not silently abandoned

            hold.set()
            await t1

    asyncio.run(go())


def test_release_thread_releases_when_the_runner_never_answers_the_interrupt(
    make_harness, monkeypatch
) -> None:
    """#739: a WEDGED runner accepts the TCP connect and then never answers
    ``/v1/interrupt``. The interrupt is a courtesy, not a precondition, so the
    release must not be hostage to it: the sandbox is still released and the
    route-existed answer still comes back, bounded to a few seconds rather than
    the runner client's own 600s request timeout. Without the bound the operator
    reset is lost entirely (the substrate release line is never reached) and the
    maintenance tick that drove it stalls for the whole window.

    The hang is injected at the runner-client seam (the external HTTP call) with
    an event that is never set, so the wedge is deterministic rather than timing
    dependent. The generous 10s ceiling below only has to prove the call is
    bounded to seconds, not to pin the exact constant."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tWedged"))
            assert h.substrate.lookup(_thread_key("tWedged")) is not None  # the route is live

            monkeypatch.setattr(kernel_module, "_RESET_INTERRUPT_TIMEOUT_S", 0.2)

            wedged = asyncio.Event()  # never set: the runner answers nothing, ever

            async def never_answers(base_url: str, reason: str, token: str | None = None) -> None:
                await wedged.wait()

            monkeypatch.setattr(h.kernel._runner, "interrupt", never_answers)

            released = await asyncio.wait_for(
                h.kernel.release_thread(_thread_key("tWedged")), timeout=2.0
            )

            assert released is True
            # released despite the wedged runner
            assert h.substrate.lookup(_thread_key("tWedged")) is None

    asyncio.run(go())


def test_release_thread_releases_when_the_interrupt_raises(make_harness, monkeypatch) -> None:
    """#739, the other half of the wedged-runner shape: the runner answers, but
    with a transport error or a non-200. The release is an operator's explicit
    "give me a fresh sandbox", so a failed courtesy interrupt is logged and
    swallowed rather than aborting the release and stranding the stale sandbox."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tInterruptBoom"))
            assert h.substrate.lookup(_thread_key("tInterruptBoom")) is not None

            async def boom(base_url: str, reason: str, token: str | None = None) -> None:
                raise RunnerError("/v1/interrupt -> 500: runner is wedged")

            monkeypatch.setattr(h.kernel._runner, "interrupt", boom)

            released = await h.kernel.release_thread(_thread_key("tInterruptBoom"))

            assert released is True
            assert h.substrate.lookup(_thread_key("tInterruptBoom")) is None

    asyncio.run(go())


def test_release_thread_with_no_route_is_a_noop(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            released = await h.kernel.release_thread("never-seen-thread")
            assert released is False

    asyncio.run(go())


def test_release_serializes_against_a_concurrent_turn_start(make_harness) -> None:
    """#734: the release runs under the same per-thread route lock the turn path
    holds around `_route_and_start`, so a reset and a message arriving for the
    same thread cannot interleave. Without the lock the message could
    `claim()`-adopt the sandbox the reset is tearing down and open a turn on it,
    which the release then yanks mid-run.

    The interleaving is forced deterministically rather than raced: the release
    is gated open (via a threading.Event, because the substrate release runs on
    `asyncio.to_thread`) so it sits IN the critical section, holding the route
    lock, while a new turn for the same thread tries to start. That turn must
    block on the lock -- proven by its `/v1/event` never firing while the
    release is parked -- and, once the release drops the route and frees the
    lock, must cold-create a FRESH sandbox (a new claim) instead of the released
    one, and complete cleanly rather than failing on a torn-down sandbox."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="ok", status=DONE)]

            # Establish a live route with a concrete, idle sandbox.
            await h.kernel.process_event(_qevent("first", thread="tRace"))
            old = h.substrate.lookup(_thread_key("tRace"))
            assert old is not None
            old_claim = old.claim_name

            # Gate the substrate release so it parks inside the critical section
            # (route lock held) until the test lets it proceed.
            real_release = h.substrate.release
            release_entered = threading.Event()
            release_gate = threading.Event()

            def gated_release(thread_key: str) -> bool:
                release_entered.set()
                release_gate.wait(timeout=5.0)
                return real_release(thread_key)

            h.substrate.release = gated_release  # type: ignore[method-assign]

            reset = asyncio.create_task(h.kernel.release_thread(_thread_key("tRace")))
            await _wait_until(release_entered.is_set)  # release now holds the lock

            # A new message for the same thread races the reset. It must block on
            # the route lock the release holds, not adopt the doomed sandbox.
            turn = asyncio.create_task(h.kernel.process_event(_qevent("second", thread="tRace")))
            await asyncio.sleep(0.2)
            assert h.runner.opened == ["first"], "turn started while the reset held the lock"

            # Let the release finish: it drops the route and frees the lock, so
            # the waiting turn now cold-creates a fresh sandbox.
            release_gate.set()
            assert await reset is True
            await turn

            assert h.runner.opened == ["first", "second"]  # the turn did run
            fresh = h.substrate.lookup(_thread_key("tRace"))
            assert fresh is not None
            assert fresh.claim_name != old_claim  # a fresh sandbox, not the released one
            assert old_claim not in h.fake_k8s.claims  # the released claim is gone

    asyncio.run(go())


def test_claim_latency_is_logged(make_harness, caplog) -> None:
    """#718: the claim wait (cold sandbox boot vs. an adopted warm one) is
    logged separately from the model turn's own duration -- the runner's own
    per-turn logging starts only once its process is already up, so it has no
    visibility into how long the worker waited to get it there. Both a fresh
    claim and a steer onto a live turn go through the same timed call, so both
    are covered by one assertion on the log line's presence and shape."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            with caplog.at_level(logging.INFO, logger="curie_worker.kernel"):
                await h.kernel.process_event(_qevent("hi", thread="tLatency"))

            matches = [
                r.getMessage()
                for r in caplog.records
                if f"claim latency for {_thread_key('tLatency')}" in r.getMessage()
            ]
            assert matches, caplog.text
            # "claim latency for tLatency: <N> ms" -- non-negative integer duration.
            ms = int(matches[0].rsplit(":", 1)[1].strip().split()[0])
            assert ms >= 0

    asyncio.run(go())


def test_lock_acquire_timeout_is_a_retryable_turn_start_failure(make_harness) -> None:
    """#849, the sibling of the runner-500 turn-start failure above: the turn
    never starts because the per-thread route lock cannot be taken in time.

    The contention is real -- another holder squats the exact Valkey lock key,
    so `acquire` polls to its deadline -- and the failure must come back as the
    same retryable outcome the other transient turn-start failures do, not
    escape `_attempt` to the consumer. The order lock must be released too, or
    the next same-thread event would never route."""

    async def go() -> None:
        async with make_harness(lock_acquire_timeout_s=0.2) as h:
            thread = "tLockTimeout"
            # A foreign holder of the route lock, outliving the acquire deadline.
            await h.async_redis.set(
                h.config.lock_key(_thread_key(thread)), "another-worker", nx=True, px=60000
            )

            released: list[bool] = []

            def release_order() -> None:
                released.append(True)

            qe = _qevent("go", thread=thread)
            outcome = await h.kernel._attempt(qe, TargetRoute(), release_order)

            assert outcome.terminal_ok is False
            assert outcome.classification == "runner-error"  # retryable
            assert h.runner.opened == []  # the turn was never started
            assert released, "the order lock was not released on the failed start"

    asyncio.run(go())


def test_lock_acquire_timeout_retries_in_process(make_harness) -> None:
    """#849: a route-lock acquire timeout is retried inside `process_event`,
    within `max_attempts`, instead of escaping to the consumer and leaving the
    stream entry pending for the whole reclaim window.

    Attempt 1 finds the lock squatted by a foreign holder and times out without
    ever reaching the runner; the squatter goes away once attempt 2 has begun,
    so attempt 2 opens the turn and completes. `opened == ["go"]` is what
    separates this from the runner-500 shape: the retry was caused by the lock,
    not by a failed turn start at the runner."""

    async def go() -> None:
        async with make_harness(lock_acquire_timeout_s=0.2, max_attempts=3) as h:
            thread = "tLockRetry"
            lock_key = h.config.lock_key(thread)
            await h.async_redis.set(lock_key, "another-worker", nx=True, px=60000)
            h.runner.default_script = [Final(text="recovered", status=DONE)]

            async def unsquat() -> None:
                # Each attempt opens with a "booting" edit before it touches the
                # lock, so a second one means attempt 1 already gave up.
                await _wait_until(lambda: len(h.sink.updates) >= 2)
                await h.async_redis.delete(lock_key)

            freeing = asyncio.create_task(unsquat())
            ev = _qevent("go", thread=thread)
            await h.kernel.process_event(ev)
            await freeing

            assert h.runner.opened == ["go"]  # attempt 1 never reached the runner
            assert h.sink.last_text == "recovered"
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())
