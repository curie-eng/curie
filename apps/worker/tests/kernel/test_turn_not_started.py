"""AC1 (#2433): a delivery whose handler RAISED tells the person, once.

The operator surface for that branch always worked: ``Consumer._handle`` logs
"processing failed for entry %s; left pending" and returns. The person on the
other end of the thread had no surface at all. They sat on the dispatcher's
placeholder, unchanged, until the delivery was redelivered, which before this
ticket meant the 900 second backstop.

Every test here drives the **real** ``Consumer._handle`` against **real
Valkey** and the real ``Kernel``. ``Kernel.notify_turn_not_started`` is the
production method throughout; only the thing that fails is arranged. Two shapes
of arrangement are used, and the difference is deliberate:

* the guard tests replace ``Kernel.process_event`` with a coroutine that
  raises, which is exactly what the incident's handler did;
* the F3 delivered-answer tests keep the REAL ``process_event`` and induce the
  failure DOWNSTREAM of the terminal send, because those guards depend on
  ``process_event``'s own lifecycle (its ``finally`` clears the in-process mark
  only for a non-``Exception`` exit) and a substituted ``process_event`` would
  pass against a broken implementation.

Two invariants every test asserts, because both are ways a plausible fix looks
clean while breaking something load-bearing: the entry is **still pending**
afterwards (acking to tidy the thread throws the turn away, #505 and ADR-0039),
and the notice is **best effort** (a Slack outage may not turn a pending
delivery into a failed one).

``_settle`` here gathers WITHOUT ``return_exceptions``, unlike
``test_delivery_ownership.py``'s: an exception escaping ``_handle`` is the #673
shape and must fail loudly rather than be collected and dropped.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Any

import pytest
from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus, TextDelta
from channel_protocol.reply import (
    REPLY_WIRE_VERSION,
    ReplyAck,
    ReplyEvent,
    ReplyTarget,
    ReplyUpdate,
    TurnCompleted,
)
from curie_dispatcher.queue import to_stream_fields
from curie_worker.config import WorkerConfig
from curie_worker.consumer import Consumer
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.kernel import _route_from_handle
from curie_worker.markers import CompletionRecord
from curie_worker.reply_sink import TargetRoute

from .conftest import _failing_process_event, _pending_rows, _updates_for

DONE = SessionStatus.DONE

# The same compressed lease clocks ``test_delivery_ownership.py`` uses, kept
# here rather than imported so a cross-file import cannot make one suite's
# timing depend on the other's. Every ratio the config validators enforce is
# preserved: TTL (1.0) >= 3 * heartbeat (0.3); the harness reclaim interval
# (0.05) < TTL; the runner ceiling (30) <= the budget (60, its floor).
_TTL_S = 1.0

_LEASE_KNOBS: dict[str, object] = {
    "delivery_budget_s": 60.0,
    "delivery_lease_ttl_s": _TTL_S,
    "delivery_lease_heartbeat_s": 0.3,
    "runner_total_timeout_s": 30.0,
}


def _qevent(
    text: str,
    *,
    thread: str = "th-1",
    event_id: str = "notice-1",
    placeholder: str | None = "p-1",
) -> QueuedTurn:
    return QueuedTurn(
        event_id=event_id,
        conversation_id=thread,
        author="U1",
        text=text,
        reply_handle=ReplyHandle(kind="slack", channel="C1", placeholder=placeholder),
        received_at="2026-07-05T00:00:00+00:00",
    )


async def _settle(consumer: Consumer) -> None:
    """Drain the in-flight handlers, letting any escaped exception fail loudly."""
    await asyncio.gather(*list(consumer._inflight))


async def _read_one(h: Any, consumer_name: str) -> tuple[str, dict[str, str]]:
    """Take the next new entry into ``consumer_name``'s PEL, as the read loop does."""
    rows = await h.async_redis.xreadgroup(
        h.config.consumer_group, consumer_name, {h.config.stream: ">"}, count=1
    )
    assert rows, "expected an entry to read"
    entry_id, fields = rows[0][1][0]
    return entry_id, dict(fields)


async def _deliver_one(
    h: Any, consumer: Consumer, qevent: QueuedTurn
) -> str:
    """XADD, read into this consumer's PEL, dispatch, and drain. Returns the entry id."""
    await h.async_redis.xadd(h.config.stream, to_stream_fields(qevent))
    entry_id, fields = await _read_one(h, h.config.consumer_name)
    await consumer._dispatch(entry_id, fields)
    await _settle(consumer)
    return entry_id


# --- AC1: the notice itself ---------------------------------------------------


def test_a_failed_turn_edits_the_placeholder_to_the_not_started_text(make_harness) -> None:
    """AC1, the reported defect directly: the thread stops being silent.

    Red on reverting EB-12/EB-14: no reply of any kind is delivered and the
    placeholder still reads the dispatcher's own text while the delivery waits
    out its redelivery.

    Exactly ONE edit, of the dispatcher's own placeholder, and no post: a post
    would notify the thread a second time for a turn that has not even started,
    which is a worse experience than the silence it replaces. The twin that
    proves the opposite half is
    ``test_a_placeholderless_turn_gets_no_message_at_all``.
    """

    async def go() -> None:
        async with make_harness() as h:
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()
            attempts = _failing_process_event(h)

            entry_id = await _deliver_one(
                h, consumer, _qevent("hello", thread="nts-1", event_id="nts-1")
            )

            assert attempts == ["nts-1"]
            assert h.sink.updates == [("C1", "p-1", h.config.turn_not_started_text)]
            assert h.sink.text_posts == [], (
                "the notice POSTED a new message instead of editing the placeholder"
            )
            assert entry_id in await _pending_rows(h), (
                "the notice acked the entry: the turn was thrown away, not retried"
            )

    asyncio.run(go())


def test_a_placeholderless_turn_gets_no_message_at_all(make_harness) -> None:
    """AC1's negative, and the reason the placeholder guard is first in the method.

    The CLI, job and hook shape (ADR-0079) carries no placeholder. On that path
    ``_reply_for`` POSTS a new message and adopts the minted ref, so firing the
    notice here would hand every non-Slack failure a message it never had.

    Red on dropping the ``reply_handle.placeholder is None`` early return.
    Zero reply traffic of ANY shape is asserted, not just zero updates: a fix
    that skipped only the edit would still post. Twin:
    ``test_a_failed_turn_edits_the_placeholder_to_the_not_started_text``.
    """

    async def go() -> None:
        async with make_harness() as h:
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()
            attempts = _failing_process_event(h)

            entry_id = await _deliver_one(
                h,
                consumer,
                _qevent("hello", thread="nts-2", event_id="nts-2", placeholder=None),
            )

            assert attempts == ["nts-2"]
            assert h.sink.events == []
            assert h.sink.updates == []
            assert h.sink.posts == []
            assert h.sink.text_posts == []
            assert entry_id in await _pending_rows(h)

    asyncio.run(go())


def test_the_notice_fires_even_under_no_edit_streaming(make_harness) -> None:
    """AC1 under ``slack_no_edit_streaming``.

    That flag's promise is "the placeholder gets exactly one chat.update: the
    final". A turn that never finished emits no final, so honoring the flag here
    would leave the placeholder frozen: the reported bug reproduced inside its
    own fix, and only in the deployments that enabled the flag.

    Red on copying the ``if not self._config.slack_no_edit_streaming`` guard
    from the booting edit into the notice.
    """

    async def go() -> None:
        async with make_harness(slack_no_edit_streaming=True) as h:
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()
            _failing_process_event(h)

            entry_id = await _deliver_one(
                h, consumer, _qevent("hello", thread="nts-3", event_id="nts-3")
            )

            assert h.sink.updates == [("C1", "p-1", h.config.turn_not_started_text)]
            assert entry_id in await _pending_rows(h)

    asyncio.run(go())


def test_a_failing_slack_sink_does_not_change_the_pending_outcome(
    make_harness, caplog: pytest.LogCaptureFixture
) -> None:
    """AC1's best-effort half: a Slack outage may not change a settlement.

    Red twice over. Red on making the notice non-best-effort: the exception
    escapes ``_handle`` past ``_consume``'s per-entry isolation guard, which is
    the #673 shape, and ``_settle`` here re-raises it rather than collecting it.
    Red also on swallowing the failure silently, because the WARNING is the only
    trace an operator has that the person was never told.
    """

    async def go() -> None:
        async with make_harness() as h:
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()
            _failing_process_event(h)
            h.sink.fail_events.add("reply.update")

            with caplog.at_level(logging.WARNING, logger="curie_worker.kernel"):
                entry_id = await _deliver_one(
                    h, consumer, _qevent("hello", thread="nts-4", event_id="nts-4")
                )

            assert h.sink.updates == []
            assert entry_id in await _pending_rows(h), (
                "a failed notice changed the delivery's settlement outcome"
            )
            assert any("nts-4" in message for message in caplog.messages), (
                "the notice failed silently: nothing names the event id"
            )

    asyncio.run(go())


def test_a_turn_that_already_settled_keeps_its_answer(make_harness) -> None:
    """AC1's terminality guard, the DURABLE half.

    ``_complete`` emits the reply BEFORE it settles, so a settle that applies in
    Valkey and then loses its response unwinds into the same ``except`` branch
    with the answer already on the person's screen and the done marker already
    written. Editing then destroys a delivered answer AND promises a retry that
    provably cannot happen: the redelivery reads this same predicate and takes
    the "already done; skipping" path without re-running the turn.

    The answer here is delivered with ``terminal=False`` on purpose, so the
    in-process ``_terminal_reply_attempted`` mark is NOT taken and this test
    isolates the ``Markers.is_terminal`` read. Its twin
    ``test_an_answer_delivered_before_a_failed_settle_is_not_overwritten``
    isolates the other half, where ``is_terminal`` is False and only the mark
    can save the answer.

    Red on dropping the ``is_terminal`` guard, and red on reading a bare done
    marker instead (a DONE outbox record proves the turn finished just as well
    and outlives the marker).
    """

    async def go() -> None:
        async with make_harness() as h:
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()

            async def settled_then_raised(qevent: QueuedTurn, *, lease: Any = None) -> None:
                await h.kernel._reply_for(
                    qevent, _route_from_handle(qevent), "the answer", terminal=False
                )
                await h.kernel._markers.mark_done(qevent.event_id)
                raise ConnectionError("the settle applied and lost its response")

            h.kernel.process_event = settled_then_raised  # type: ignore[method-assign,assignment]

            entry_id = await _deliver_one(
                h, consumer, _qevent("hello", thread="nts-5", event_id="nts-5")
            )

            assert h.sink.updates == [("C1", "p-1", "the answer")], (
                "the notice overwrote a delivered answer for a turn that is "
                "durably terminal and will never rerun"
            )
            assert entry_id in await _pending_rows(h)

    asyncio.run(go())


def test_a_done_outbox_record_without_a_done_marker_keeps_its_answer(
    make_harness, caplog: pytest.LogCaptureFixture
) -> None:
    """AC1's terminality guard, the half that OUTLIVES the done marker.

    ``is_terminal`` is deliberately wider than the done marker: the marker is one
    key with ``idempotency_ttl_s`` (one day) behind it, while a DONE outbox record
    proves the turn finished and is retained for ``completion_max_retention_s``
    (seven days). A turn recovered after a long outage therefore reaches the
    consumer's except branch with a DONE record and no marker at all, and it is
    still just as terminal: editing the placeholder there destroys a delivered
    answer and promises a retry the record itself refuses.

    Its twin ``test_a_turn_that_already_settled_keeps_its_answer`` arms the marker
    and would stay green if the guard were narrowed to a bare done-marker read.
    This one is the test that goes RED on exactly that narrowing, so the pair
    pins both terms of the predicate rather than one.

    The record is written with ``mark_completion_pending`` and ``done=True``,
    which is the state ``settle_fenced`` and ``mark_completion_pending`` plus
    ``mark_done`` both leave behind, minus the marker those two also set.
    """

    async def go() -> None:
        async with make_harness() as h:
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()
            _failing_process_event(h)

            await h.kernel._markers.mark_completion_pending(
                "nts-12",
                CompletionRecord(
                    event_id="nts-12",
                    event=TurnCompleted(
                        version=REPLY_WIRE_VERSION,
                        event="turn.completed",
                        target=ReplyTarget(
                            kind="slack",
                            address="C1",
                            conversation_id="nts-12",
                            reply_ref="p-1",
                        ),
                        event_id="nts-12",
                        outcome="delivered",
                    ),
                    route=TargetRoute(),
                    created_at=time.time(),
                    done=True,
                ),
            )
            assert await h.async_redis.exists(h.config.done_key("nts-12")) == 0, (
                "a done marker was armed, so this test no longer isolates the "
                "outbox half of the predicate"
            )

            with caplog.at_level(logging.WARNING, logger="curie_worker.kernel"):
                entry_id = await _deliver_one(
                    h, consumer, _qevent("hello", thread="nts-12", event_id="nts-12")
                )

            assert any("nts-12" in message for message in caplog.messages), (
                "nothing reached the terminality guard at all, so a green result "
                "here would prove nothing about it"
            )
            assert h.sink.updates == [], (
                "the notice overwrote the answer of a turn whose DONE outbox "
                "record proves it finished, because terminality was read from "
                "the done marker alone"
            )
            assert entry_id in await _pending_rows(h)

    asyncio.run(go())


def test_an_unreadable_terminality_check_leaves_the_placeholder_alone(
    make_harness, caplog: pytest.LogCaptureFixture
) -> None:
    """AC1's terminality guard FAILS CLOSED.

    The two errors are not symmetric. A notice skipped for a turn that did fail
    costs the person the seconds until the lease-expiry reclaim redelivers,
    which is the behavior before this change; a notice sent for a turn that
    succeeded costs them the answer permanently.

    Red on ``except Exception: pass`` around the terminality read (which would
    fall through and edit), and red on swallowing the failure with no WARNING.
    """

    async def go() -> None:
        async with make_harness() as h:
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()
            _failing_process_event(h)

            async def unreadable(event_id: str) -> bool:
                raise ConnectionError("injected marker read failure")

            h.kernel._markers.is_terminal = unreadable  # type: ignore[method-assign,assignment]

            with caplog.at_level(logging.WARNING, logger="curie_worker.kernel"):
                entry_id = await _deliver_one(
                    h, consumer, _qevent("hello", thread="nts-6", event_id="nts-6")
                )

            assert h.sink.updates == []
            assert entry_id in await _pending_rows(h)
            assert any("nts-6" in message for message in caplog.messages)

    asyncio.run(go())


def test_the_not_started_text_names_no_tool_and_no_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#717's prose rule as an assertion, plus AC1's "one sentence" contract.

    An end user talking to an agent must never see curie's own architecture
    vocabulary in a status line, and there is nothing in an internal id a person
    can look up, so naming one only leaks architecture. The one-sentence check
    is AC1's own wording made enforceable rather than described: two sentences
    is what the round-1 copy did.

    Red on any default that reintroduces internal vocabulary, a digit, or a
    second sentence. No Valkey; this is the copy, not the delivery.
    """
    monkeypatch.delenv("CURIE_TURN_NOT_STARTED_TEXT", raising=False)

    text = WorkerConfig().turn_not_started_text

    for word in ("runner", "sandbox", "valkey", "stream", "entry", "id"):
        assert re.search(rf"\b{word}\b", text, re.IGNORECASE) is None, (
            f"the person-facing notice names {word!r}"
        )
    assert re.search(r"\d", text) is None, "the notice carries an identifier"
    assert len(re.findall(r"[.!?]", text)) == 1, "AC1 asks for ONE sentence"
    assert text.rstrip().endswith((".", "!", "?"))


# --- F3: the notice never overwrites a delivered answer -----------------------


def test_an_answer_delivered_before_a_failed_settle_is_not_overwritten(
    make_harness,
) -> None:
    """F3, the ``finalize`` half. The REAL ``process_event`` unwind, deliberately.

    ``is_terminal`` protects a settlement that succeeded and lost its response.
    It does NOT protect one that failed BEFORE writing: the answer reaches the
    person at ``reply.finalize``, and only afterwards does ``_complete`` make its
    first fenced write. With ``settle_fenced`` raising, the exception unwinds
    with the answer on screen and ``is_terminal`` reading False.

    A substituted ``process_event`` would pass against a broken implementation,
    because ``_process_event``'s ``finally`` runs BEFORE the exception reaches
    the consumer and ``process_event``'s ``finally`` is what decides whether the
    mark survives. So the real one runs here, the runner is scripted with a
    Final, and only the fenced write fails.

    Red without EB-12b: the notice overwrites a delivered answer. Twin:
    ``test_a_turn_that_already_settled_keeps_its_answer`` (the durable half).
    """

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()
            h.runner.default_script = [Final(text="the answer", status=DONE)]

            async def refuse(*args: Any, **kwargs: Any) -> str | None:
                raise ConnectionError("the fenced write never landed")

            h.kernel._markers.settle_fenced = refuse  # type: ignore[method-assign,assignment]

            entry_id = await _deliver_one(
                h, consumer, _qevent("hello", thread="nts-7", event_id="nts-7")
            )

            assert _updates_for(h, "nts-7")[-1] == "the answer", (
                "the notice overwrote an answer the person had already read"
            )
            assert h.config.turn_not_started_text not in _updates_for(h, "nts-7")
            assert await h.kernel._markers.is_terminal("nts-7") is False, (
                "the settle wrote a marker, so this test no longer exercises the "
                "in-process mark it exists to pin"
            )
            assert entry_id in await _pending_rows(h)

    asyncio.run(go())


def test_a_terminal_send_that_lands_then_raises_is_treated_as_delivered(
    make_harness, caplog: pytest.LogCaptureFixture
) -> None:
    """AC1's AMBIGUOUS terminal send: the edit landed, the await did not.

    An HTTP edit that applies on the platform and then loses its response is
    indistinguishable, locally, from one that never applied. The answer is on the
    person's screen and the local call raised, so the delivery still unwinds into
    the consumer's except branch. ``is_terminal`` cannot save it (the settle is
    downstream of the send and never ran) and neither can a mark taken on
    SUCCESS, because there was no success to take it on. Only marking BEFORE the
    send covers this shape, and the fail-safe direction is to treat the person as
    already answered: a duplicate notice costs them an answer they can read,
    while a skipped one costs them the seconds until the redelivery.

    The REAL ``process_event`` runs, for the reason the module docstring gives:
    the mark's survival is decided by ``process_event``'s own ``finally``, which a
    substituted one would not have. The sink's ``fail_events`` cannot arm this
    shape because it raises BEFORE recording, which is the unambiguous
    never-landed case, so ``emit`` is wrapped HERE to record first and raise
    after.

    Red if the terminal mark moves from before the send to after a successful
    send, and red if it moves to the top of ``_complete``: both leave this
    delivery unmarked, and the notice replaces a delivered answer with an
    invitation to resend it.
    """

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()
            h.runner.default_script = [Final(text="the answer", status=DONE)]

            landed = h.sink.emit

            async def lands_then_raises(
                event: ReplyEvent,
                *,
                route: TargetRoute,
                best_effort_unreachable: bool = False,
            ) -> ReplyAck:
                ack = await landed(
                    event, route=route, best_effort_unreachable=best_effort_unreachable
                )
                if isinstance(event, ReplyUpdate) and event.text == "the answer":
                    raise ConnectionError("the edit applied and lost its response")
                return ack

            h.sink.emit = lands_then_raises  # type: ignore[method-assign]

            with caplog.at_level(logging.WARNING, logger="curie_worker.kernel"):
                entry_id = await _deliver_one(
                    h, consumer, _qevent("hello", thread="nts-13", event_id="nts-13")
                )

            assert any("nts-13" in message for message in caplog.messages), (
                "the delivery never reached the notice branch, so a green result "
                "here would prove nothing about the mark"
            )
            texts = _updates_for(h, "nts-13")
            assert texts[-1] == "the answer", (
                "the notice overwrote an answer that had already reached the "
                "person, because the send raised after it landed"
            )
            assert h.config.turn_not_started_text not in texts
            assert await h.kernel._markers.is_terminal("nts-13") is False, (
                "the turn settled, so this test no longer exercises the "
                "before-the-send mark it exists to pin"
            )
            assert entry_id in await _pending_rows(h)

    asyncio.run(go())


def test_a_polite_drop_is_not_overwritten_by_the_notice(make_harness) -> None:
    """F3, the ``_drop_with_message`` half: the second delivery path before ``_complete``.

    An unmapped channel is answered politely and then completed. If that
    completion's fenced write fails, the drop text is already on the person's
    screen and ``is_terminal`` still reads False, exactly as in the twin above.
    One test per marking site is why both ``terminal=True`` paths are covered:
    this one goes through ``_reply_for``, the twin through
    ``_ThrottledReply.finalize``.

    Red on marking only at ``finalize``: the drop message is replaced by the
    notice, and the person is told to resend a message the platform already
    answered.
    """

    class _UnmappedBinding:
        """Resolves nothing, which is the polite-drop route."""

        async def resolve(self, kind: str, adapter: str | None, address: str) -> None:
            return None

    async def go() -> None:
        async with make_harness(binding=_UnmappedBinding(), **_LEASE_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()

            async def refuse(*args: Any, **kwargs: Any) -> str | None:
                raise ConnectionError("the fenced write never landed")

            h.kernel._markers.settle_fenced = refuse  # type: ignore[method-assign,assignment]

            entry_id = await _deliver_one(
                h, consumer, _qevent("hello", thread="nts-8", event_id="nts-8")
            )

            drop_texts = _updates_for(h, "nts-8")
            assert drop_texts, "the polite drop never reached the person at all"
            assert "no agent is configured" in drop_texts[-1].lower(), (
                "the notice overwrote the polite drop"
            )
            assert h.config.turn_not_started_text not in drop_texts
            assert entry_id in await _pending_rows(h)

    asyncio.run(go())


def test_a_partially_streamed_turn_still_gets_the_notice(make_harness) -> None:
    """F3's mandatory NEGATIVE, and one of #2433's three named shapes.

    "The turn that streamed partial text and then raised" must still be told
    about. Streaming previews are not a terminal result: the person has a
    fragment, not an answer, and suppressing the notice here would silently
    delete a third of AC1's coverage while the two tests above stayed green.

    The failure is induced at ``_finish``, which is downstream of every streamed
    edit and upstream of ``reply.finalize``, so ``stream`` provably fired and
    ``finalize`` provably did not. ``process_event`` itself is untouched, so the
    whole real lifecycle still runs.

    Red on calling the mark from ``_ThrottledReply.stream`` or
    ``stream_context``. Twin:
    ``test_an_answer_delivered_before_a_failed_settle_is_not_overwritten``.
    """

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()
            h.runner.default_script = [
                TextDelta(text="partial "),
                TextDelta(text="thought"),
            ]

            async def never_finishes(*args: Any, **kwargs: Any) -> Any:
                raise RuntimeError("the turn raised before it could finalize")

            h.kernel._finish = never_finishes  # type: ignore[method-assign,assignment]

            entry_id = await _deliver_one(
                h, consumer, _qevent("hello", thread="nts-9", event_id="nts-9")
            )

            texts = _updates_for(h, "nts-9")
            assert any(text.startswith("partial") for text in texts), (
                "nothing streamed, so this test does not exercise the negative"
            )
            assert texts[-1] == h.config.turn_not_started_text, (
                "a partially streamed turn was treated as delivered and the "
                "person was never told it failed"
            )
            assert entry_id in await _pending_rows(h)

    asyncio.run(go())


# --- The notice may not talk over the current fence holder --------------------


def test_a_notice_is_skipped_when_this_owner_lost_the_fence(
    make_harness, caplog: pytest.LogCaptureFixture
) -> None:
    """The call-site guard (EB-14): terminality is not permission to edit.

    ``Consumer._handle``'s generic ``except`` can run AFTER lease loss and sits
    ahead of the pre-ACK ``raise_if_lost``. A replacement that holds the fence
    now owns this thread and will speak for it, starting with its own booting
    edit; talking over it is worse than saying nothing.

    Red on removing ``lease.raise_if_lost()`` from the call site: this owner
    edits a placeholder it no longer has authority over. Twin:
    ``test_a_notice_is_skipped_when_the_lease_is_lost_during_the_terminality_read``,
    which covers the instant AFTER this check, where only the kernel's own
    re-check can see the loss.
    """

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()

            async def fenced_out(qevent: QueuedTurn, *, lease: Any = None) -> None:
                assert lease is not None, "the kernel was handed no lease"
                lease.lost.set()
                raise RuntimeError("simulated handler failure after lease loss")

            h.kernel.process_event = fenced_out  # type: ignore[method-assign,assignment]

            with caplog.at_level(logging.WARNING, logger="curie_worker.consumer"):
                entry_id = await _deliver_one(
                    h, consumer, _qevent("hello", thread="nts-10", event_id="nts-10")
                )

            assert h.sink.updates == [], (
                "a fenced-out owner talked over the replacement that holds the fence"
            )
            assert entry_id in await _pending_rows(h)
            assert any(
                "not-started notice" in message or "lost the delivery lease" in message
                for message in caplog.messages
            )

    asyncio.run(go())


def test_a_notice_is_skipped_when_the_lease_is_lost_during_the_terminality_read(
    make_harness, caplog: pytest.LogCaptureFixture
) -> None:
    """The kernel's own re-check at the emission boundary (EB-12 step 4).

    A heartbeat can mark the lease lost WHILE the ``is_terminal`` read is
    pending, so a check taken only at the call site is already stale by the time
    the edit happens. Here the call-site check provably passes (the lease is
    still held when the branch is entered) and only the kernel's
    post-``is_terminal`` ``raise_if_lost`` can catch the loss.

    Red on placing that check BEFORE the ``is_terminal`` await instead of after
    it, which is the whole point of the placement. Twin:
    ``test_a_notice_is_skipped_when_this_owner_lost_the_fence``.
    """

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()

            held: list[Any] = []

            async def failing(qevent: QueuedTurn, *, lease: Any = None) -> None:
                assert lease is not None, "the kernel was handed no lease"
                held.append(lease)
                raise RuntimeError("simulated handler failure")

            h.kernel.process_event = failing  # type: ignore[method-assign,assignment]

            async def loses_the_fence_mid_read(event_id: str) -> bool:
                held[0].lost.set()
                return False

            h.kernel._markers.is_terminal = (  # type: ignore[method-assign,assignment]
                loses_the_fence_mid_read
            )

            with caplog.at_level(logging.WARNING, logger="curie_worker.kernel"):
                entry_id = await _deliver_one(
                    h, consumer, _qevent("hello", thread="nts-11", event_id="nts-11")
                )

            assert h.sink.updates == []
            assert entry_id in await _pending_rows(h)
            assert any("nts-11" in message for message in caplog.messages)

    asyncio.run(go())
