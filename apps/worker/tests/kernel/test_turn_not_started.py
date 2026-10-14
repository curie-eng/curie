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
import functools
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any

import pytest
from aci_protocol import Final, QueuedTurn, SessionStatus, TextDelta
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
from curie_worker.kernel.routing import _route_from_handle
from curie_worker.markers import CompletionRecord
from curie_worker.reply_sink import TargetRoute

from .conftest import _failing_process_event, _pending_rows, _updates_for

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from queue_fixtures import qevent  # noqa: E402

_qevent = functools.partial(qevent, event_id="notice-1")

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
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
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
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
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
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
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
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
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
    ``test_an_answer_delivered_before_a_failed_clear_is_not_overwritten``
    isolates the other half, where ``is_terminal`` is False and only the mark
    can save the answer.

    Red on dropping the ``is_terminal`` guard, and red on reading a bare done
    marker instead (a DONE outbox record proves the turn finished just as well
    and outlives the marker).
    """

    async def go() -> None:
        async with make_harness() as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            async def settled_then_raised(qevent: QueuedTurn, *, lease: Any = None) -> None:
                await h.kernel._reply_for(
                    qevent, _route_from_handle(qevent), "the answer", terminal=False
                )
                await h.kernel._markers.mark_done(qevent.event_id, marker_value="1")
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
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
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
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
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


def test_a_settle_that_fails_before_writing_gets_the_notice(make_harness) -> None:
    """F3 under ADR 0226: the answer is stored by the fenced settle before any send.

    A settle that fails before writing means nothing reached the person and
    nothing is owed, so the not-started notice is true and redelivery reruns.
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

            texts = _updates_for(h, "nts-7")
            assert "the answer" not in texts, "an answer was sent before it was stored"
            assert texts[-1] == h.config.turn_not_started_text
            assert await h.kernel._markers.is_terminal("nts-7") is False
            assert await h.kernel._markers.read_completion("nts-7") is None
            assert entry_id in await _pending_rows(h)

    asyncio.run(go())


def test_an_answer_delivered_before_a_failed_clear_is_not_overwritten(
    make_harness,
) -> None:
    """F3, the in-process mark: an answer sent before a later failure keeps it.

    The answer now leaves in ``_deliver_completion``, which marks the turn as
    answered before the send. A failed clear after it must not let the notice
    overwrite the answer; ``is_terminal`` is blinded to isolate that mark.
    """

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()
            h.runner.default_script = [Final(text="the answer", status=DONE)]

            async def refuse(*args: Any, **kwargs: Any) -> None:
                raise ConnectionError("the clear never landed")

            async def unreadable(*args: Any, **kwargs: Any) -> bool:
                # Isolate the in-process half: the durable half would also
                # suppress the notice here, because the settle already wrote.
                return False

            h.kernel._markers.clear_completion = refuse  # type: ignore[method-assign,assignment]
            h.kernel._markers.is_terminal = unreadable  # type: ignore[method-assign,assignment]

            entry_id = await _deliver_one(
                h, consumer, _qevent("hello", thread="nts-7b", event_id="nts-7b")
            )

            texts = _updates_for(h, "nts-7b")
            assert texts[-1] == "the answer", (
                "the notice overwrote an answer the person had already read"
            )
            assert h.config.turn_not_started_text not in texts
            assert entry_id in await _pending_rows(h)

    asyncio.run(go())


def test_a_terminal_send_that_lands_then_raises_stays_owed_and_replays(
    make_harness,
) -> None:
    """AC1's ambiguous terminal send under ADR 0226.

    The settle precedes the send, so a send that lands and then raises leaves
    the answer on screen, the turn terminal and the stored answer owed. The
    replay delivers that stored answer without running the model again.
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
            await _deliver_one(
                h, consumer, _qevent("hello", thread="nts-13", event_id="nts-13")
            )

            texts = _updates_for(h, "nts-13")
            assert texts[-1] == "the answer"
            assert h.config.turn_not_started_text not in texts
            assert await h.kernel._markers.is_terminal("nts-13") is True
            owed = await h.kernel._markers.read_completion("nts-13")
            assert owed is not None and owed.record.answer is not None
            assert owed.record.answer.text == "the answer"

            h.sink.emit = landed  # type: ignore[method-assign]
            await h.kernel._reemit_pending_completion("nts-13")

            assert h.runner.opened == ["hello"], "the replay re-ran the model"
            assert _updates_for(h, "nts-13")[-1] == "the answer"
            assert await h.kernel._markers.read_completion("nts-13") is None

    asyncio.run(go())


def test_a_turn_that_raised_before_its_answer_still_gets_the_notice(
    make_harness,
) -> None:
    """F3's negative: a turn that raised before its answer still gets the notice.

    Provisional text is internal (ADR 0226 decision 1), so nothing streamed
    stands in for an answer.
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
            assert not any("partial" in text for text in texts), (
                "provisional text reached the channel (ADR 0226 decision 1)"
            )
            assert texts[-1] == h.config.turn_not_started_text
            assert entry_id in await _pending_rows(h)

    asyncio.run(go())
