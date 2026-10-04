from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from aci_protocol import (
    QueuedTurn,
    TurnSource,
)
from channel_protocol.reply import (
    REPLY_WIRE_VERSION,
    ReplyTarget,
    TurnCompleted,
    TurnStatus,
)

from .. import sweep
from ..behaviorpacks import (
    BehaviorPacks,
    sample_load,
    sample_tip,
)
from ..delivery_lease import DeliveryLease
from ..hook_runs import HookRunOutcome
from ..markers import CompletionRecord, DoneMarkerValue, MalformedCompletionError
from ..reply_sink import (
    DeletedReplyTargetError,
    ProviderEgressRefusedError,
    TargetRoute,
)
from ..workitem_dispatch import (
    WorkItemConflict,
    WorkItemTransportError,
    parse_work_item_event_id,
)

if TYPE_CHECKING:
    from .core import Kernel

from . import claim, clock, constants, failures, log, routing
from .log import logger


async def _complete(
    self: Kernel,
    qevent: QueuedTurn,
    route: TargetRoute,
    outcome: str,
    *,
    telemetry_outcome: str,
    lease: DeliveryLease | None = None,
    hook_outcome: HookRunOutcome | None = None,
    turn: failures.TurnOutcome | None = None,
    successor: QueuedTurn | None = None,
) -> None:
    """The terminal ordering, at every durable ``mark_done`` call site.

    ``turn`` is the streamed turn behind a delivered or escalated outcome.
    An escalated factory run finishes with the cause and provider message it
    carries (#3073); a delivered one that did not publish finishes as
    ``early_stop`` or ``no_pull_request`` with the agent's last message
    (#3128).

    For a valid cron run, close its Postgres row before these Valkey steps.

    1. write the outbox record       -- durable, BEFORE the done marker
    2. mark done + flag the record   -- ONE MULTI, so they cannot diverge
    3. emit ``turn.completed``       -- may fail
    4. clear the record              -- ONLY on a confirmed emit

    Step 3 failing is caught and logged, never raised: the turn is already
    durably done, and re-running it is the harm. The record survives so a
    sweeper (or the next redelivery) delivers the completion the adapter is
    still owed.

    The ONLY ``mark_done`` call site: every durable terminal outcome
    completes here, including the pre-resolution prior-side-effect
    escalation, whose route is the server-minted ``reply_handle``'s (EB-B2's
    second sanctioned source -- no adapter can write to the stream, so the
    handle is trustworthy). A terminal path that marked done without a record
    would be a completion nothing could ever recover.
    ``successor`` is the next slice of a long scheduled sweep (ADR-0160,
    #2878), published atomically with this settle by
    ``Markers.settle_fenced_and_publish``; the hook run stays open. A sweep
    that stopped instead gets its coverage notice posted here, and only once
    the settle was won.
    A targetless cron turn (#2963) is the one exception: no adapter is
    waiting, so no completion is owed, and ``_settle_targetless`` marks it
    done with no record.

    Since ADR-0131 this is also the FENCE. When the caller holds a delivery
    lease, steps 1 and 2 fuse into ``Markers.settle_fenced``, one script that
    verifies the lease token and the fencing generation and then performs the
    same two writes. That adds a PRECONDITION; it reorders nothing -- the
    record is still durable before/with the done marker, and it is still
    cleared only after a confirmed emit.

    A caller that fails the fence returns HERE, before ``_deliver_completion``
    is ever reached: ADR-0131 says a stale owner "may not ACK, dead-letter,
    clear an outbox record, or emit a terminal result", and all three of the
    latter live downstream of this point. It also marks the lease lost, so the
    consumer's pre-ACK ``raise_if_lost`` refuses the fourth verb too and the
    entry stays pending for whoever now holds the fence. Without that, a turn
    whose settle was refused would be acked with no completion written at all.
    """
    parsed = parse_work_item_event_id(qevent.event_id)
    run = (
        self._work_item_runs.get(parsed.request_id)
        if parsed is not None and parsed.kind in {"execute"}
        else None
    )
    if run is not None and run.event_id != qevent.event_id:
        run = None
    if run is None and (
        (parsed is not None and parsed.is_ci_fix) or self._is_approval_resume(qevent.event_id)
    ):
        run = self._run_for_event(qevent.event_id)
    if run is not None and run.started and not run.finished:
        try:
            if outcome == "awaiting-approval":
                # Approval is not a terminus. Stop the short lease refresh
                # and hold the request until its execution deadline. The
                # run stays attached so the resume turn can finish it.
                await run.close()
                try:
                    await run.hold_for_approval()
                except (WorkItemConflict, WorkItemTransportError):
                    logger.warning(
                        "work-item approval hold failed for %s",
                        qevent.event_id,
                        exc_info=True,
                    )
                else:
                    run.held = True
            elif outcome == "delivered":
                ci_fix = parsed is not None and parsed.is_ci_fix
                if ci_fix:
                    cause = "ci_fix_unpublished"
                elif turn is None or self._is_approval_resume(qevent.event_id):
                    cause = "no_pull_request"
                else:
                    cause = failures._unpublished_cause(turn.tools_called)
                try:
                    await run.finish(
                        outcome="failed",
                        cause=cause,
                        detail=(
                            None
                            if ci_fix or turn is None
                            else failures._finish_detail(turn.assistant_text)
                        ),
                    )
                except WorkItemConflict as exc:
                    if exc.code != "publication_pending":
                        raise
                    # The publication loop owns the terminus from here; the
                    # execution itself is over, and publication works from
                    # the stored patch, not the sandbox.
                    run.finished = True
            elif outcome == "escalated":
                await run.finish(
                    outcome="failed",
                    cause=failures._escalation_cause(turn),
                    detail=turn.error_message if turn is not None else None,
                )
            else:
                await run.finish(outcome="failed", cause="runner_failed", detail=None)
        except WorkItemConflict as exc:
            logger.warning(
                "work-item finish refused for %s: %s; writing no marker",
                qevent.event_id,
                exc.code,
            )
            if exc.code == "work_item_cancelled":
                # #3208: the request is already settled as cancelled, so
                # nothing will ever accept a marker or a finish. Count the
                # run as settled locally so this delivery releases its
                # sandbox claim now instead of holding quota until the
                # terminate-wake backstop catches up.
                run.finished = True
            return
    elif run is not None and not run.started:
        try:
            await run.defer(f"not_started:{telemetry_outcome}", capacity=False)
        except WorkItemConflict as exc:
            logger.info(
                "work-item unstarted defer refused for %s: %s",
                qevent.event_id,
                exc.code,
            )
    if (
        hook_outcome is None
        and successor is None
        and qevent.source is TurnSource.CRON
        and sweep.parse_continuation(qevent.event_id) is not None
    ):
        # A continuation that ends without an outcome of its own (refused
        # before runner admission, a drop, an already-terminal row) is a
        # sweep stop: close it so the stop is recorded and reported (#2878).
        hook_outcome = "failed"
    if hook_outcome is None and routing._is_targetless(qevent):
        # A targeted terminal that started nothing (an escalation, a refused
        # admission or workspace) leaves the row open for a human reading the
        # reply. A targetless one has no reader and redelivery skips a done
        # event, so every targetless terminal closes the row: "ran" only when
        # an attempt started and ended ok, otherwise "failed" (#2963).
        hook_outcome = "failed"
    hook_carry = constants._HOOK_RUN_CARRY.get()
    notice: str | None = None
    if (
        hook_outcome is not None
        and hook_carry is not None
        and hook_carry.recorder is not None
        and hook_carry.ref is not None
        and not (claim._is_fenced(lease) and lease is not None and lease.lost.is_set())
    ):
        if await hook_carry.recorder.close(hook_carry.ref, hook_outcome):
            # Computed now, posted only once the settle below is won. Kept on
            # the carry until then, so a settle that raises still leaves the
            # notice to the error close (#2878).
            notice = await self._coverage_notice(qevent, hook_outcome)
            hook_carry.notice_pending = notice
    event_id = qevent.event_id
    if routing._is_targetless(qevent):
        await self._settle_targetless(qevent, outcome, telemetry_outcome, lease)
        return
    history_capacity_review = (
        outcome == "escalated"
        and turn is not None
        and turn.review_origin_key == event_id
        and turn.classification == "history-persistence-error"
    )
    marker_value: DoneMarkerValue = "history_capacity" if history_capacity_review else "1"
    record = CompletionRecord(
        event_id=event_id,
        event=TurnCompleted(
            version=REPLY_WIRE_VERSION,
            event="turn.completed",
            target=self._target_for(qevent),
            event_id=event_id,
            outcome=cast("Any", outcome),
        ),
        route=route,
        created_at=clock.time.time(),
        done=False,
    )
    if claim._is_fenced(lease):
        assert lease is not None  # narrowed by _is_fenced
        if successor is not None:
            fenced = await _settle_and_publish(
                self, event_id, record, lease, marker_value=marker_value, successor=successor
            )
        else:
            fenced = await self._markers.settle_fenced(
                event_id,
                record,
                # The delivery this lease was GRANTED for, carried on the lease
                # itself. Not `self._config.stream`: the fence must be checked
                # against the exact keys the lease was acquired on, and the
                # lease is the only thing that knows which entry that was.
                stream=lease.stream,
                group=lease.group,
                entry_id=lease.entry_id,
                owner=lease.owner,
                generation=lease.generation,
                marker_value=marker_value,
            )
        if fenced is None:
            # The single most diagnostic line in this feature: it is the
            # only record that a turn ran to a terminal outcome and then
            # discovered it no longer owned the delivery. Name what we held,
            # so an operator can line it up against the replacement's
            # acquisition rather than guessing which side was stale.
            logger.warning(
                "fenced out of terminal settlement for event %s "
                "(owner=%s generation=%d outcome=%s): another owner holds "
                "this delivery; writing no marker, clearing nothing and "
                "emitting nothing",
                event_id,
                lease.owner,
                lease.generation,
                outcome,
            )
            constants._LIFECYCLE_OUTCOME.set("fenced_out")
            # Authority is not recoverable, so record the loss locally too:
            # the consumer's pre-ACK check is what keeps this owner from
            # acking a delivery it just failed to settle.
            lease.lost.set()
            if hook_carry is not None:
                # The replacement owns this delivery's report.
                hook_carry.notice_pending = None
            return
        generation = fenced
    else:
        generation = await self._markers.mark_completion_pending(event_id, record)
        if successor is not None:
            # Leaseless and NOT atomic: no fence to publish behind. A crash
            # here leaves a successor and a pending predecessor, whose
            # redelivery escalates on its side-effect marker while the
            # successor drops at the terminal-row preamble.
            await self._markers.publish_successor(self._config.stream, successor.model_dump_json())
        await self._markers.mark_done(event_id, marker_value=marker_value)
    constants._LIFECYCLE_OUTCOME.set(telemetry_outcome)
    if notice is not None:
        # Claimed before the post, so a cancellation during or after it can
        # never make the error close post the same notice again.
        if hook_carry is not None:
            hook_carry.notice_pending = None
        await self._post_coverage_notice(qevent, route, notice)
    await self._deliver_completion(record, generation=generation)
    attributes = {
        "service.name": "curie-worker",
        "source": "worker",
        "outcome": telemetry_outcome,
    }
    log.record_metric("curie.turn.completed", attributes=attributes)
    self._record_agent_turn(telemetry_outcome)
    try:
        received = datetime.fromisoformat(qevent.received_at)
        if received.tzinfo is None:
            received = received.replace(tzinfo=UTC)
        duration = max(
            0.0,
            (datetime.now(UTC) - received.astimezone(UTC)).total_seconds(),
        )
    except ValueError:
        duration = 0.0
    log.record_metric("curie.turn.duration", duration, attributes=attributes)


async def _settle_and_publish(
    self: Kernel,
    event_id: str,
    record: CompletionRecord,
    lease: DeliveryLease,
    *,
    marker_value: DoneMarkerValue,
    successor: QueuedTurn,
) -> str | None:
    """The fenced settle that also publishes a sweep's next slice (#2878).

    The record generation is chosen here, so a lost reply can be told apart
    from a script that never ran: the script sets the published marker to it
    beside the successor XADD, so if the marker holds ours, the successor
    exists and this slice is settled. Otherwise the error stands and the carry
    keeps the run open, because the successor may still exist.
    """
    record_generation = uuid.uuid4().hex
    hook_carry = constants._HOOK_RUN_CARRY.get()
    if hook_carry is not None:
        hook_carry.successor_maybe_published = True
    try:
        return await self._markers.settle_fenced_and_publish(
            event_id,
            record,
            stream=lease.stream,
            group=lease.group,
            entry_id=lease.entry_id,
            owner=lease.owner,
            generation=lease.generation,
            marker_value=marker_value,
            successor_stream=lease.stream,
            successor_payload=successor.model_dump_json(),
            record_generation=record_generation,
        )
    except Exception:
        try:
            stored = await asyncio.wait_for(
                self._markers.sweep_published_generation(event_id), timeout=5.0
            )
        except Exception:  # noqa: BLE001 - unknown is not committed; the original stands
            logger.warning("could not read the outcome of sweep slice %s's settle", event_id)
            stored = None
        if stored != record_generation:
            raise
        logger.warning(
            "sweep slice %s settled and published its successor; its reply was lost",
            event_id,
        )
        return record_generation


async def _settle_targetless(
    self: Kernel,
    qevent: QueuedTurn,
    outcome: str,
    telemetry_outcome: str,
    lease: DeliveryLease | None,
) -> None:
    """``_complete``'s terminal write for a targetless turn (#2963).

    Marker only: no adapter is waiting, so no ``turn.completed`` is owed and
    no outbox record is written. The fenced form refuses exactly as
    ``settle_fenced`` does, with the same lease-lost handling.
    """
    event_id = qevent.event_id
    if claim._is_fenced(lease):
        assert lease is not None  # narrowed by _is_fenced
        settled = await self._markers.settle_fenced_without_completion(
            event_id,
            stream=lease.stream,
            group=lease.group,
            entry_id=lease.entry_id,
            owner=lease.owner,
            generation=lease.generation,
        )
        if not settled:
            logger.warning(
                "fenced out of terminal settlement for event %s "
                "(owner=%s generation=%d outcome=%s): another owner holds "
                "this delivery; writing no marker",
                event_id,
                lease.owner,
                lease.generation,
                outcome,
            )
            constants._LIFECYCLE_OUTCOME.set("fenced_out")
            lease.lost.set()
            return
    else:
        await self._markers.mark_done_without_completion(event_id)
    constants._LIFECYCLE_OUTCOME.set(telemetry_outcome)
    log.record_metric(
        "curie.turn.completed",
        attributes={
            "service.name": "curie-worker",
            "source": "worker",
            "outcome": telemetry_outcome,
        },
    )
    self._record_agent_turn(telemetry_outcome)


def _record_agent_turn(self: Kernel, telemetry_outcome: str) -> None:
    """Count the terminal turn under the resolved agent, or ``unbound``.

    The fleet counter stays unlabeled. This series is the one a per-agent
    silence alarm can group on, and the label is capped in ``record_metric``.
    """

    raw = constants._TURN_AGENT.get()
    # Only a missing agent is ``unbound``. A real agent whose name is a
    # reserved label shares ``other`` so it cannot inflate the unresolved series.
    if not raw:
        label = "unbound"
    elif raw in {"other", "unbound"}:
        label = "other"
    else:
        label = raw
    log.record_metric(
        "curie.agent.turn.completed",
        attributes={
            "service.name": "curie-worker",
            "source": "worker",
            "outcome": telemetry_outcome,
            "agent": label,
        },
    )


async def _deliver_completion(self: Kernel, record: CompletionRecord, *, generation: str) -> bool:
    """Emit a stored completion and clear it, or leave it owed. True on send.

    The clear is compare-and-checked against ``generation`` -- the identity of
    the record this caller actually read -- so a pass holding a stale record
    can never delete the fresh one a concurrent retry wrote for the same
    event id.
    """
    try:
        await self._sink.emit(record.event, route=record.route)
    except DeletedReplyTargetError as exc:
        if await self._markers.dead_letter_completion(
            record, generation=generation, reason=exc.reason
        ):
            logger.warning("turn.completed dead-lettered: %s", exc.reason)
        return False
    except ProviderEgressRefusedError:
        try:
            await self._markers.note_provider_egress_refusal(record.event_id, generation=generation)
        except Exception as exc:  # noqa: BLE001 - retry remains owed
            logger.warning(
                "turn.completed refusal cause could not be stored for %s (%s)",
                record.event_id,
                type(exc).__name__,
            )
        logger.warning(
            "turn.completed provider egress refused for %s; the outbox record stands",
            record.event_id,
        )
        return False
    except Exception as exc:  # noqa: BLE001 - the turn is already durably done
        try:
            await self._markers.clear_completion_cause(record.event_id, generation=generation)
        except Exception as cause_exc:  # noqa: BLE001 - retry remains owed
            logger.warning(
                "turn.completed failure cause could not be cleared for %s (%s)",
                record.event_id,
                type(cause_exc).__name__,
            )
        logger.warning(
            "turn.completed delivery failed for %s (%s); the outbox record stands",
            record.event_id,
            exc,
        )
        return False
    await self._markers.clear_completion(record.event_id, generation=generation)
    return True


async def _reemit_pending_completion(self: Kernel, event_id: str) -> None:
    """Re-deliver a completion this event durably owed, if one is pending.

    The already-done skip's only job on the completion plane. It reads a
    STORED record rather than building one, because it never resolved a
    route of its own and must not invent one.
    """
    try:
        stored = await self._markers.read_completion(event_id)
    except MalformedCompletionError as exc:
        await self._quarantine_completion(event_id, exc)
        return
    if stored is None:
        return
    await self._deliver_completion(stored.record, generation=stored.generation)


async def _quarantine_completion(
    self: Kernel, event_id: str, exc: MalformedCompletionError
) -> None:
    """Refuse a malformed outbox record, loudly, and stop re-reading it.

    The outbox is new in this train, so every record in it was written by a
    writer that sets both the done flag and the generation. A record missing
    either is corrupt, and there is no safe reading of it: treating it as
    done would emit a completion for a turn that may still rerun, and
    treating it as clearable would delete a record this pass cannot prove it
    owns. The payload is therefore LEFT IN PLACE for an operator, and only
    its index entry goes, so the sweeper does not spend a delivery attempt
    on it every pass.
    """
    logger.warning(
        "quarantining a malformed completion outbox record: event_id=%s (%s); "
        "the payload is left in place and will never be emitted",
        event_id,
        exc,
    )
    await self._markers.drop_pending_member(event_id)


async def sweep_pending_completions(self: Kernel) -> None:
    """Drain the completion outbox, logging what it recovered.

    A pass that delivers anything is by definition recovering a completion an
    earlier turn owed and never confirmed, so it says so at INFO rather than
    answering with a count both callers threw away. A quiet pass -- the
    normal case -- stays silent.

    Called from the consumer's maintenance loop and once at startup, which
    is the case redelivery can NEVER reach: once a stream entry is acked
    there is nothing left to redeliver, so a redelivery-only sweep would
    strand the record forever.

    A member may be emitted only when BOTH hold:

    1. **the record is flagged done.** ``Markers.mark_done`` sets that flag
       in the same MULTI as the done marker, so a false flag means the
       kernel is mid-flight or crashed before durability -- the sweeper stays
       silent and stream redelivery reruns the turn, correctly. The flag is
       the WHOLE guard, with no marker fallback behind it: ``done_key``
       expires at ``idempotency_ttl_s`` while the record is retained for a
       week, so a guard resting on the marker could never pass after day
       one, and consulting it for a record whose flag says "not yet" lets a
       CONCURRENT retry's done marker authorize this pass to emit and clear
       a record that retry is still mid-flight on. A record with NO flag is
       malformed rather than legacy (the outbox is new in this train) and is
       quarantined, never guessed at.
    2. **the record is older than the grace period**, which keeps the
       sweeper out of the kernel's own emit window so the normal path is not
       racing it on every turn.

    The pass is BOUNDED, in members and in wall time (``completion_sweep_batch``
    / ``completion_sweep_budget_s``). Every delivery attempt is an HTTP call
    with the sink's own timeout, so an unbounded pass over an unreachable
    adapter is measured in hours -- and this same coroutine runs at startup,
    against exactly the backlog an outage left behind. The remainder is
    drained by the next maintenance tick.
    """
    sent = 0
    now = clock.time.time()
    deadline = now + self._config.completion_sweep_budget_s
    batch = await self._markers.pending_completions(self._config.completion_sweep_batch)
    # ONE pipeline for the whole batch's records: the reads are independent
    # of each other and of every delivery decision below, so paying a round
    # trip per member (up to completion_sweep_batch of them, on the same
    # Valkey the kernel's locks live on) bought nothing. The wall-time budget
    # still bounds the part that is actually slow -- the delivery attempts.
    stored_batch = await self._markers.read_completions(sorted(batch))
    for seen, (event_id, stored) in enumerate(stored_batch.items()):
        if clock.time.time() >= deadline:
            logger.info(
                "completion sweep budget (%.0fs) reached after %d record(s); "
                "the rest are left for the next pass",
                self._config.completion_sweep_budget_s,
                seen,
            )
            break
        if isinstance(stored, MalformedCompletionError):
            await self._quarantine_completion(event_id, stored)
            continue
        if stored is None:
            # A member whose payload is gone: some emitter confirmed
            # delivery and cleared it between our read of the set and our
            # read of the key. Re-emitting would be a duplicate with no
            # record to guide it, and no route to reconstruct one from.
            # Only the stale index entry goes: deleting the KEY here would
            # destroy a record a concurrent retry may have just written.
            await self._markers.drop_pending_member(event_id)
            continue
        record = stored.record
        age = now - record.created_at
        if age > self._config.completion_max_retention_s:
            # Never a silent expiry, and never a set member without its
            # payload: both go together, and an operator hears about it.
            logger.warning(
                "discarding an undelivered turn.completed after %.0fs: event_id=%s "
                "adapter=%s address=%s",
                age,
                event_id,
                record.route.adapter,
                record.event.target.address,
            )
            await self._markers.clear_completion(event_id, generation=stored.generation)
            continue
        if not stored.done_flag:
            continue
        if age < self._config.completion_sweep_grace_s:
            continue
        if await self._deliver_completion(record, generation=stored.generation):
            sent += 1
    if sent:
        logger.info("completion sweep delivered %d owed turn.completed event(s)", sent)


async def _set_shimmer(
    self: Kernel,
    qevent: QueuedTurn,
    route: TargetRoute,
    packs: BehaviorPacks,
) -> None:
    """Raise the shimmer for this turn, and own the only side that lowers it.

    The caption is this agent's sampled load line (+ tip), seeded by the thread
    ts, falling back to the operator's generic ``status_text`` when the agent
    enables neither pack. That fallback is why this is unconditional now: the
    dispatcher used to set the generic caption before enqueueing, so a slow
    Slack call delayed the durable ``XADD`` of the turn, and set and clear
    lived in two different processes where a fast turn could clear before the
    set landed and strand a caption until Slack's own timeout (#1312). Both
    halves are on this side now, ordered by the same ``await`` chain, so that
    race cannot be expressed rather than merely being tested for.

    Best-effort: the sink swallows errors, so a workspace without the
    assistant feature costs one debug line and nothing else.
    """
    if routing._is_targetless(qevent):
        return
    load = sample_load(packs, qevent.conversation_id)
    tip = sample_tip(packs, qevent.conversation_id)
    if load and tip:
        caption = f"{load}\n\nTip: {tip}"
    elif load:
        caption = load
    elif tip:
        caption = f"Tip: {tip}"
    else:
        caption = self._config.status_text
    if not caption:
        # An operator who blanks status_text wants no caption at all; setting
        # an empty status would read as a clear, not as a shimmer.
        return
    await self._emit_status(self._target_for(qevent), route, caption)


async def _emit_status(self: Kernel, target: ReplyTarget, route: TargetRoute, status: str) -> None:
    """Raise or lower the channel's liveness caption. Never fails a turn.

    Best-effort is a property of the SHIMMER, not of Slack, so the swallow
    lives here rather than inside one adapter: a channel with no caption
    affordance, an adapter that is down, or a workspace without the
    assistant feature must all cost one debug line and nothing else. An
    EMPTY status is the clear.
    """
    try:
        await self._sink.emit(
            TurnStatus(
                version=REPLY_WIRE_VERSION,
                event="turn.status",
                target=target,
                status=status,
            ),
            route=route,
        )
    except Exception as exc:  # noqa: BLE001 -- the caption never gates a turn
        logger.debug("turn.status %r skipped for %s: %s", status, target.conversation_id, exc)
