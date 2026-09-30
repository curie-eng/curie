"""The durable completion outbox: `turn.completed` is never lost, never early.

Stream B, ADR-0096 phase 2, EB-B6. Covers T-B8 (completion is emitted at the
durable terminal markers and NOT on a retryable failure) and T-B12 (a)-(j) (the
outbox, its two sweepers, and the emission guard), against real Valkey.

The property this file exists for is a two-sided one, and both sides have
already been got wrong once:

- revision 0 emitted in the terminal ``finally``, which runs for every
  exception while ``consumer.py:196-217`` leaves the entry PENDING for retry --
  so the email sent and the turn then ran again (T-B8);
- revision 1 marked the turn done and then emitted best-effort, so a crash in
  that window suppressed the only ``turn.completed`` that would ever exist and
  the already-done skip made redelivery unable to retry it (T-B12(a)-(c)).

The ordering EB-B6(c) settles on, at every ``mark_done`` call site:

    1. mark_completion_pending(event_id, record)   # durable, BEFORE mark_done
    2. mark_done(event_id)                         # marker + flag, ONE MULTI
    3. emit(TurnCompleted(...), route=record.route)        # may fail
    4. clear_completion(event_id)                          # only on a CONFIRMED emit
"""

from __future__ import annotations

import asyncio
import functools
import logging
import sys
import time
from pathlib import Path

import pytest
from aci_protocol import Final, SessionStatus
from channel_protocol.reply import REPLY_WIRE_VERSION, ReplyTarget, TurnCompleted
from curie_dispatcher.queue import to_stream_fields
from curie_worker.consumer import Consumer
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.markers import CompletionRecord, Markers
from curie_worker.reply_sink import TargetRoute
from curie_worker.runner_client import RunnerClient

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from queue_fixtures import qevent  # noqa: E402
from queue_fixtures import wait_until as _wait_until

DONE = SessionStatus.DONE

ADAPTER = "agentmail-sandbox"
EMAIL_ADDRESS = "agent@example.test"
ENDPOINT = "https://adapter.example/hook"

_qevent = functools.partial(
    qevent,
    kind="email",
    channel=EMAIL_ADDRESS,
    placeholder="msg_upstream",
    endpoint=ENDPOINT,
    adapter=ADAPTER,
)


def _record(
    event_id: str,
    *,
    thread: str = "th-1",
    done: bool = True,
    age_s: float = 0.0,
    outcome: str = "delivered",
) -> CompletionRecord:
    """A self-contained completion record (EB-B6(b)).

    Self-contained is the whole point: it carries the ALREADY-RESOLVED route, so
    any later emitter -- kernel or sweeper -- uses the stored route and never
    re-resolves. A sweeper draining an acked entry has no binding lookup
    available to it, and re-resolving would read a binding an operator may since
    have re-pointed.
    """
    return CompletionRecord(
        event_id=event_id,
        event=TurnCompleted(
            version=REPLY_WIRE_VERSION,
            event="turn.completed",
            target=ReplyTarget(
                kind="email",
                address=EMAIL_ADDRESS,
                conversation_id=thread,
                reply_ref="msg_upstream",
            ),
            event_id=event_id,
            outcome=outcome,  # type: ignore[arg-type]
        ),
        route=TargetRoute(endpoint=ENDPOINT, adapter=ADAPTER),
        created_at=time.time() - age_s,
        done=done,
    )


# --- T-B8: completion happens at the durable markers, and nowhere else --------


@pytest.mark.parametrize(
    "layer",
    ["runner", "sink", "database", "cancelled"],
)
def test_a_retryable_failure_emits_no_completion_and_leaves_the_entry_pending(
    make_harness, monkeypatch: pytest.MonkeyPatch, layer: str
) -> None:
    # Finding 11. Each case raises from a DIFFERENT layer, because the defect
    # revision 0 shipped was structural: an emit in the terminal ``finally``
    # fires for every exception, and the entry is still pending for XAUTOCLAIM.
    # The adapter would have sent the email and the turn would then run again.
    # Mutation: move the emit into the ``finally`` and all four fail.
    async def go() -> None:
        async with make_harness(shimmer=False, reclaim_min_idle_ms=5000) as h:
            h.runner.default_script = [Final(text="answer", status=DONE)]
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            if layer == "sink":
                h.sink.fail_events = {"reply.update"}
            elif layer == "runner":

                async def runner_boom(*_a: object, **_k: object) -> object:
                    raise RuntimeError("runner layer failure")

                monkeypatch.setattr(RunnerClient, "start_turn", runner_boom)
            elif layer == "database":

                async def db_boom(*_a: object, **_k: object) -> object:
                    raise RuntimeError("database layer failure")

                monkeypatch.setattr(Markers, "settle_fenced", db_boom)
            else:

                async def cancelled(*_a: object, **_k: object) -> object:
                    raise asyncio.CancelledError()

                monkeypatch.setattr(RunnerClient, "start_turn", cancelled)

            qe = _qevent(thread=f"tB8-{layer}", event_id=f"b8-{layer}")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))

            task = asyncio.create_task(consumer.run())
            summary: dict[str, object] = {}
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                summary = await h.async_redis.xpending(
                    h.config.stream, h.config.consumer_group
                )
                if summary["pending"]:
                    break
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.2)
            consumer.request_stop()
            await task

            # The entry was never acked: XAUTOCLAIM will reclaim and retry it.
            summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            assert summary["pending"] == 1
            # ...and no completion escaped, so the adapter never acted on a turn
            # that is about to run again.
            assert h.sink.completions == []

    asyncio.run(go())


# --- T-B12: the outbox -------------------------------------------------------


def test_a_clean_turn_emits_exactly_one_completion_and_clears_the_record(
    make_harness,
) -> None:
    # (d) no double-send on the happy path. Neither sweeper may re-emit after a
    # confirmed delivery, or every turn ships a duplicate.
    async def go() -> None:
        async with make_harness(shimmer=False, completion_sweep_grace_s=0.0) as h:
            h.runner.default_script = [Final(text="answer", status=DONE)]
            qe = _qevent(thread="tD", event_id="d1")
            await h.kernel.process_event(qe)

            assert [c.event_id for c in h.sink.completions] == ["d1"]
            assert h.sink.completions[0].outcome == "delivered"
            assert await h.async_redis.exists(h.config.completion_key("d1")) == 0
            assert await h.async_redis.smembers(h.config.completions_pending_key()) == set()

            await h.kernel.sweep_pending_completions()
            assert len(h.sink.completions) == 1

    asyncio.run(go())


def test_the_completion_carries_the_turns_event_id(make_harness) -> None:
    # EB-B6(e): delivery is at-least-once, so the dedupe key must be ON the
    # event. An adapter keyed on the conversation instead (today's shape,
    # adapter.py:209-214) cannot tell a duplicate from a second turn.
    async def go() -> None:
        async with make_harness(shimmer=False) as h:
            h.runner.default_script = [Final(text="answer", status=DONE)]
            qe = _qevent(thread="tEid", event_id="eid-77")
            await h.kernel.process_event(qe)
            assert h.sink.completions[0].event_id == "eid-77"
            assert h.sink.completions[0].target.conversation_id == "tEid"

    asyncio.run(go())


def test_a_failed_emit_leaves_the_record_and_the_sweeper_delivers_it(
    make_harness,
) -> None:
    # (a) the crash window between mark_done and the emit. This is the failure
    # revision 1 could not recover from: the turn is durably done, so redelivery
    # returns at the already-done skip, and a best-effort emit that failed is
    # simply gone.
    # Mutation: revert to mark_done-then-best-effort-emit with no record and this
    # fails, along with (b) and (c).
    async def go() -> None:
        async with make_harness(shimmer=False, completion_sweep_grace_s=0.0) as h:
            h.runner.default_script = [Final(text="answer", status=DONE)]
            h.sink.fail_events = {"turn.completed"}
            qe = _qevent(thread="tA", event_id="a1")

            await h.kernel.process_event(qe)

            # Durably done, and the emit failure did not raise out of the turn:
            # the turn IS complete; re-running it is the harm.
            assert await h.async_redis.exists(h.config.done_key("a1"))
            assert h.sink.completions == []
            # The record survived, so the completion is still owed.
            assert await h.async_redis.exists(h.config.completion_key("a1"))
            assert await h.async_redis.smembers(h.config.completions_pending_key()) == {"a1"}

            h.sink.fail_events = set()
            await h.kernel.sweep_pending_completions()

            assert [c.event_id for c in h.sink.completions] == ["a1"]
            assert await h.async_redis.exists(h.config.completion_key("a1")) == 0
            assert await h.async_redis.smembers(h.config.completions_pending_key()) == set()

    asyncio.run(go())


def test_the_record_is_cleared_only_after_a_confirmed_emit(make_harness) -> None:
    # The ordering half of (a): clearing before the emit returns re-opens the
    # exact loss window the record exists to close.
    # Mutation: clear the record before awaiting the emit and this fails.
    async def go() -> None:
        async with make_harness(shimmer=False) as h:
            h.runner.default_script = [Final(text="answer", status=DONE)]
            h.sink.fail_events = {"turn.completed"}
            await h.kernel.process_event(_qevent(thread="tA2", event_id="a2"))
            assert await h.async_redis.exists(h.config.completion_key("a2"))

    asyncio.run(go())


def test_redelivery_re_emits_from_the_stored_record(make_harness) -> None:
    # (b) the already-done skip hands off. It has no resolved route of its own
    # (T-B15(a)), so it re-emits from the STORED record and never re-resolves.
    async def go() -> None:
        async with make_harness(shimmer=False) as h:
            qe = _qevent(thread="tB", event_id="b1")
            await Markers(h.async_redis, h.config).mark_completion_pending(
                "b1", _record("b1", thread="tB", done=True)
            )
            await h.async_redis.set(h.config.done_key("b1"), "1")

            await h.kernel.process_event(qe)

            assert [c.event_id for c in h.sink.completions] == ["b1"]
            assert await h.async_redis.exists(h.config.completion_key("b1")) == 0
            assert await h.async_redis.smembers(h.config.completions_pending_key()) == set()
            # It really was the stored record: the turn never ran.
            assert h.runner.opened == []

    asyncio.run(go())


def test_the_startup_sweep_delivers_a_record_whose_entry_was_already_acked(
    make_harness,
) -> None:
    # (c) the case redelivery can NEVER reach. Once the stream entry is acked
    # there is nothing left to redeliver, so a redelivery-only sweep would strand
    # the record forever -- which is why the pending index is a SET and not a
    # scan over the keyspace.
    async def go() -> None:
        async with make_harness(shimmer=False, completion_sweep_grace_s=0.0) as h:
            await Markers(h.async_redis, h.config).mark_completion_pending(
                "c1", _record("c1", thread="tC", done=True)
            )
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            task = asyncio.create_task(consumer.run())
            await _wait_until(lambda: bool(h.sink.completions))
            consumer.request_stop()
            await task

            assert [c.event_id for c in h.sink.completions] == ["c1"]
            assert await h.async_redis.smembers(h.config.completions_pending_key()) == set()

    asyncio.run(go())


def test_a_sweeper_is_silent_for_a_record_whose_turn_is_not_yet_done(
    make_harness,
) -> None:
    # (e) the interleaving revision 2 missed: ``_maintenance_loop`` runs
    # CONCURRENTLY with ``_read_loop``, so a sweeper can fire between step 1 and
    # step 2. Emitting there, then crashing before mark_done, would let
    # redelivery rerun the whole turn with the completion already sent.
    # Mutation: drop the done guard from the sweeper and this fails -- this is
    # the test that proves the guard.
    async def go() -> None:
        async with make_harness(shimmer=False, completion_sweep_grace_s=0.0) as h:
            await Markers(h.async_redis, h.config).mark_completion_pending(
                "e1", _record("e1", thread="tE", done=False, age_s=600)
            )

            await h.kernel.sweep_pending_completions()

            assert h.sink.completions == []
            # Nothing cleared either: the record is still owed, and the stream
            # entry is still pending for the rerun that will own it.
            assert await h.async_redis.exists(h.config.completion_key("e1"))
            assert await h.async_redis.smembers(h.config.completions_pending_key()) == {"e1"}

            # The rerun then produces exactly one completion.
            h.runner.default_script = [Final(text="answer", status=DONE)]
            await h.kernel.process_event(_qevent(thread="tE", event_id="e1"))
            assert [c.event_id for c in h.sink.completions] == ["e1"]

    asyncio.run(go())


def test_the_done_flag_survives_the_done_markers_expiry(make_harness) -> None:
    # (i) the marker-TTL mismatch. ``done_key`` expires at idempotency_ttl_s
    # (86400, config.py:352) while the record is retained for 7 days, so a guard
    # reading ``done_key`` alone can never pass after day one and the record is
    # then discarded by the retention sweep -- completion permanently lost after
    # a >24h outage, which is the failure the 7-day retention existed to prevent.
    # Mutation: make the sweeper depend on the done marker alone and this fails.
    async def go() -> None:
        async with make_harness(shimmer=False) as h:
            await Markers(h.async_redis, h.config).mark_completion_pending(
                "i1", _record("i1", thread="tI", done=True, age_s=90000)
            )
            await h.async_redis.delete(h.config.done_key("i1"))

            await h.kernel.sweep_pending_completions()

            assert [c.event_id for c in h.sink.completions] == ["i1"]
            assert await h.async_redis.smembers(h.config.completions_pending_key()) == set()

    asyncio.run(go())


def test_a_set_member_with_no_payload_is_removed_and_emits_nothing(
    make_harness,
) -> None:
    # (j) the concurrent clear. The MULTI keeps set and payload from diverging
    # durably, but a sweeper can still read the member BEFORE the transaction and
    # find the payload gone AFTER it. A missing payload means some emitter
    # confirmed delivery and cleared it, so re-emitting would be a duplicate --
    # and the sweeper cannot reconstruct a route it never had.
    async def go() -> None:
        async with make_harness(shimmer=False, completion_sweep_grace_s=0.0) as h:
            await h.async_redis.sadd(h.config.completions_pending_key(), "j1")

            await h.kernel.sweep_pending_completions()

            assert h.sink.completions == []
            assert await h.async_redis.smembers(h.config.completions_pending_key()) == set()

            # Idempotent: a second pass is a no-op, not an error.
            await h.kernel.sweep_pending_completions()
            assert h.sink.completions == []

    asyncio.run(go())


def test_the_grace_period_keeps_the_sweeper_out_of_the_kernels_emit_window(
    make_harness,
) -> None:
    # (f) the grace period. Without it, the sweeper races the kernel's own emit
    # on every single turn and duplicates become the norm rather than the
    # crash-window exception.
    async def go() -> None:
        async with make_harness(shimmer=False, completion_sweep_grace_s=60.0) as h:
            await Markers(h.async_redis, h.config).mark_completion_pending(
                "f1", _record("f1", thread="tF", done=True, age_s=0)
            )
            await h.kernel.sweep_pending_completions()
            assert h.sink.completions == []
            assert await h.async_redis.exists(h.config.completion_key("f1"))

            # Past the grace period the same record IS emitted -- the guard is a
            # delay, never a suppression.
            await Markers(h.async_redis, h.config).mark_completion_pending(
                "f1", _record("f1", thread="tF", done=True, age_s=120)
            )
            await h.kernel.sweep_pending_completions()
            assert [c.event_id for c in h.sink.completions] == ["f1"]

    asyncio.run(go())


def test_a_record_past_max_retention_is_cleared_loudly_with_its_set_member(
    make_harness, caplog: pytest.LogCaptureFixture
) -> None:
    # (g) loud max retention. Revision 2 put ``ex=idempotency_ttl_s`` on the
    # payload while the SET membership had none, so a >24h outage left a member
    # pointing at an expired payload -- completion permanently lost, silently.
    # Mutation: re-add a payload TTL and the no-TTL assertion below fails.
    async def go() -> None:
        async with make_harness(
            shimmer=False,
            completion_sweep_grace_s=0.0,
            completion_max_retention_s=604800.0,
        ) as h:
            await Markers(h.async_redis, h.config).mark_completion_pending(
                "g1", _record("g1", thread="tG", done=False, age_s=700000)
            )
            # No expiry on the payload: retention is a decision the sweeper makes
            # out loud, never a silent TTL.
            assert await h.async_redis.ttl(h.config.completion_key("g1")) == -1

            with caplog.at_level(logging.WARNING):
                await h.kernel.sweep_pending_completions()

            assert h.sink.completions == []
            # Set and payload go together, in one MULTI: never a member without
            # its payload.
            assert await h.async_redis.exists(h.config.completion_key("g1")) == 0
            assert await h.async_redis.smembers(h.config.completions_pending_key()) == set()
            logged = "\n".join(r.getMessage() for r in caplog.records)
            assert "g1" in logged
            assert ADAPTER in logged
            assert EMAIL_ADDRESS in logged

    asyncio.run(go())


def test_a_crash_before_the_clear_produces_a_duplicate_with_one_event_id(
    make_harness,
) -> None:
    # (h) the unavoidable duplicate. Any at-least-once outbox has one, so the
    # honest answer is to make it identifiable rather than to pretend it away:
    # both completions carry the SAME event_id, which is what lets the adapter
    # absorb the second (pairs with T-D1's "exactly one email").
    async def go() -> None:
        async with make_harness(shimmer=False, completion_sweep_grace_s=0.0) as h:
            h.runner.default_script = [Final(text="answer", status=DONE)]
            await h.kernel.process_event(_qevent(thread="tH", event_id="h1"))
            assert [c.event_id for c in h.sink.completions] == ["h1"]

            # The crash point: the emit landed, the clear never ran.
            await Markers(h.async_redis, h.config).mark_completion_pending(
                "h1", _record("h1", thread="tH", done=True, age_s=120)
            )
            await h.kernel.sweep_pending_completions()

            assert [c.event_id for c in h.sink.completions] == ["h1", "h1"]

    asyncio.run(go())


def test_every_terminal_path_writes_a_completion_record(make_harness) -> None:
    # The sibling sweep across EB-B6(c)'s call sites: a terminal path that marks
    # the event done without an outbox record is a completion that can never be
    # recovered. Escalation is the one operators notice missing.
    async def go() -> None:
        async with make_harness(shimmer=False) as h:
            h.runner.default_script = [Final(text="boom", status=SessionStatus.CLASSIFIED_FAILURE)]
            h.sink.fail_events = {"turn.completed"}
            await h.kernel.process_event(_qevent(thread="tEsc", event_id="esc1"))

            assert await h.async_redis.exists(h.config.done_key("esc1"))
            assert await h.async_redis.smembers(h.config.completions_pending_key()) == {"esc1"}

    asyncio.run(go())


def test_an_escalated_turn_reports_the_escalated_outcome(make_harness) -> None:
    async def go() -> None:
        async with make_harness(shimmer=False, max_attempts=1) as h:
            h.runner.default_script = [Final(text="boom", status=SessionStatus.CLASSIFIED_FAILURE)]
            await h.kernel.process_event(_qevent(thread="tEsc2", event_id="esc2"))
            assert [c.outcome for c in h.sink.completions] == ["escalated"]

    asyncio.run(go())


# --- R6 (ADR-0131): ONE user-visible terminal effect, and the fence that keeps
# --- a stale owner from producing a second one -------------------------------
#
# The named idempotent receiving boundary, per ADR-0131's requirement that tests
# and documentation NAME it before claiming exactly-once terminal effect: the
# ``turn.completed`` event carries this turn's ``event_id``, and the recording
# sink below is the stand-in for a receiving adapter that applies that id
# idempotently (one message per event_id, edited in place). The ADR is explicit
# that exactly-once TRANSPORT is impossible, so everything here asserts one
# user-visible EFFECT at that boundary -- never one network send.

_R6_LEASE_KNOBS: dict[str, object] = {
    "delivery_budget_s": 60.0,
    "delivery_lease_ttl_s": 1.0,
    "delivery_lease_heartbeat_s": 0.3,
    "runner_total_timeout_s": 30.0,
}


async def _dispatch_and_settle(consumer: Consumer, entry_id: str, fields: dict) -> None:
    await consumer._dispatch(entry_id, fields)
    await asyncio.gather(*list(consumer._inflight), return_exceptions=True)


async def _read_one(h, consumer_name: str) -> tuple[str, dict]:
    rows = await h.async_redis.xreadgroup(
        h.config.consumer_group, consumer_name, {h.config.stream: ">"}, count=1
    )
    assert rows, "expected an entry to read"
    entry_id, fields = rows[0][1][0]
    return entry_id, dict(fields)


def test_a_failed_terminal_send_recovers_to_exactly_one_user_visible_effect(
    make_harness,
) -> None:
    """R6, the recovery half, driven through the fenced consumer.

    Three deliveries of the SAME event id: the first turn's terminal send fails,
    the second recovers it from the stored record, and the third adds nothing.
    Exactly ONE ``turn.completed`` reaches the idempotent receiving boundary
    across all three.

    This also pins the settlement of the delivery state on BOTH terminal paths --
    the successful ACK (C2) and the already-done skip (C11). Red on reverting
    either ``settle`` call: a dead-lettered-and-redelivered event id then
    accumulates delivery-state keys until their one-day retention TTL.

    The negative control is the fourth delivery, of a DIFFERENT event id: it
    still produces its own effect, so "no further effect" above is the
    idempotency key doing its job and not a sink that stopped recording.
    """
    # Imported inside the test on purpose: ``delivery_lease`` does not exist
    # until this ticket lands, and a module-level import would fail COLLECTION
    # for this whole file, turning every unrelated test in it red.
    from curie_worker.delivery_lease import DeliveryLeaseStore

    async def go() -> None:
        async with make_harness(
            shimmer=False, completion_sweep_grace_s=0.0, **_R6_LEASE_KNOBS
        ) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()
            h.runner.default_script = [Final(text="answer", status=DONE)]

            # Delivery 1: the terminal send fails. The turn IS complete; the
            # completion is owed and durably recorded.
            h.sink.fail_events = {"turn.completed"}
            await h.async_redis.xadd(
                h.config.stream, to_stream_fields(_qevent(thread="tR6", event_id="r6-1"))
            )
            entry_1, fields_1 = await _read_one(h, h.config.consumer_name)
            await _dispatch_and_settle(consumer, entry_1, fields_1)

            assert h.sink.completions == []
            assert await h.async_redis.exists(h.config.completion_key("r6-1"))
            assert await store.peek(h.config.stream, h.config.consumer_group, entry_1) == {}, (
                "the delivery state outlived a terminal ACK"
            )

            # Delivery 2: the same event id under a new stream entry (a genuine
            # redelivery). The already-done skip re-emits from the STORED record.
            h.sink.fail_events = set()
            await h.async_redis.xadd(
                h.config.stream, to_stream_fields(_qevent(thread="tR6", event_id="r6-1"))
            )
            entry_2, fields_2 = await _read_one(h, h.config.consumer_name)
            await _dispatch_and_settle(consumer, entry_2, fields_2)

            assert [c.event_id for c in h.sink.completions] == ["r6-1"]
            assert await store.peek(h.config.stream, h.config.consumer_group, entry_2) == {}, (
                "the already-done skip left its delivery state behind"
            )

            # Delivery 3 adds nothing at the boundary, and neither does a sweep.
            await h.async_redis.xadd(
                h.config.stream, to_stream_fields(_qevent(thread="tR6", event_id="r6-1"))
            )
            entry_3, fields_3 = await _read_one(h, h.config.consumer_name)
            await _dispatch_and_settle(consumer, entry_3, fields_3)
            await h.kernel.sweep_pending_completions()
            assert [c.event_id for c in h.sink.completions] == ["r6-1"]

            # NEGATIVE CONTROL: a different event id still lands its own effect.
            await h.async_redis.xadd(
                h.config.stream, to_stream_fields(_qevent(thread="tR6", event_id="r6-2"))
            )
            entry_4, fields_4 = await _read_one(h, h.config.consumer_name)
            await _dispatch_and_settle(consumer, entry_4, fields_4)
            assert [c.event_id for c in h.sink.completions] == ["r6-1", "r6-2"]

    asyncio.run(go())


def test_a_stale_generation_owner_writes_no_marker_clears_nothing_and_emits_nothing(
    make_harness,
) -> None:
    """R6, the stale-owner half, and AC4's four refused verbs at the settle point.

    An owner holding generation N runs to a terminal outcome while the delivery
    state already holds N+1 -- the "slow owner whose lease expired and was taken"
    case. ``settle_fenced`` must refuse atomically: no done marker, no write to
    (and no clear of) the outbox record another writer owns, and no terminal
    emit. The refusal to ACK is pinned separately, in
    ``tests/kernel/test_delivery_ownership.py``.

    Red on reverting ``_complete``'s fenced settlement to the two-call
    ``mark_completion_pending`` + ``mark_done`` pair: the loser then writes the
    marker and emits, and the user sees the turn finish twice.

    The positive control is the CURRENT owner running the identical path
    immediately afterwards. Without it this test passes just as well if
    ``settle_fenced`` refuses everybody.
    """
    # Imported inside the test on purpose: ``delivery_lease`` does not exist
    # until this ticket lands, and a module-level import would fail COLLECTION
    # for this whole file, turning every unrelated test in it red.
    from curie_worker.delivery_lease import DeliveryLeaseStore

    async def go() -> None:
        async with make_harness(shimmer=False, **_R6_LEASE_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()
            h.runner.default_script = [Final(text="answer", status=DONE)]

            qe = _qevent(thread="tR6s", event_id="r6-stale")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))
            entry_id, _fields = await _read_one(h, h.config.consumer_name)

            stale = await store.acquire(
                h.config.stream,
                h.config.consumer_group,
                entry_id,
                consumer=h.config.consumer_name,
            )
            assert (
                await store.release(
                    h.config.stream, h.config.consumer_group, entry_id, owner=stale.owner,
                    resume_event_id=None,
                )
                is True
            )
            current = await store.acquire(
                h.config.stream,
                h.config.consumer_group,
                entry_id,
                consumer=h.config.consumer_name,
            )
            assert (stale.generation, current.generation) == (1, 2)

            # An outbox record owned by another writer, deliberately NOT done so
            # the already-done skip does not short-circuit the run under test.
            await Markers(h.async_redis, h.config).mark_completion_pending(
                "r6-stale", _record("r6-stale", thread="tR6s", done=False)
            )
            assert await h.async_redis.smembers(h.config.completions_pending_key()) == {
                "r6-stale"
            }

            await h.kernel.process_event(qe, lease=stale)

            assert h.sink.completions == [], "a fenced-out owner emitted a terminal result"
            assert await h.async_redis.exists(h.config.done_key("r6-stale")) == 0, (
                "a fenced-out owner wrote the done marker"
            )
            assert await h.async_redis.exists(h.config.completion_key("r6-stale")), (
                "a fenced-out owner cleared an outbox record it does not own"
            )
            assert (
                await h.async_redis.hget(h.config.completion_key("r6-stale"), "done") == "0"
            ), "a fenced-out owner flagged another writer's record done"

            # POSITIVE CONTROL: the current owner settles and emits, so the
            # refusal above is the generation check and not a dead settle path.
            await h.kernel.process_event(
                _qevent(thread="tR6s", event_id="r6-stale"), lease=current
            )
            assert [c.event_id for c in h.sink.completions] == ["r6-stale"]
            assert await h.async_redis.exists(h.config.done_key("r6-stale"))
            assert await h.async_redis.exists(h.config.completion_key("r6-stale")) == 0

    asyncio.run(go())
