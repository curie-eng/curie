"""SessionRunner: owns the model session and turns inbound frames into NDJSON.

One SessionRunner wraps one long-lived ``ModelSession`` (one session per sandbox).
It is the single owner of the SDK generator: a turn is driven by ``query`` +
``receive_turn``, and that iterator is consumed by exactly one ``run_turn`` at a
time (guarded by a turn lock). Steering and interrupt are side-channel injections
into the same live session that surface on the open turn's stream, mirroring the
proven PT-2 pattern rather than opening a second consumer of the generator.

Responsibilities layered on the translation:
- **Budget:** accumulate output tokens per turn; halt with a classified-failure
  final once ``max_output_tokens_per_run`` is crossed.
- **Interrupt:** a requested interrupt reclassifies an otherwise-done final as
  idle-awaiting-input.
- **OTel:** wrap each turn in the gen_ai span tree.
- **Status:** track the last final status (done / idle-awaiting-input /
  classified-failure) for the status endpoint.
"""

from __future__ import annotations

import contextlib
import hmac
import logging
import time
from collections.abc import AsyncGenerator, AsyncIterator, Callable

import anyio
from aci_protocol import (
    ErrorEvent,
    Event,
    Final,
    Interrupt,
    SessionStatus,
    ToolNote,
    parse_ndjson_line,
    to_ndjson_line,
)
from claude_agent_sdk import AssistantMessage, ResultMessage
from curie_telemetry import record_metric
from opentelemetry.context import Context

from .adapter import ModelSession, PartialMessageBoundary, StreamedToolUseBoundary
from .approval import PUBLISH_TOOL_NAME, ApprovalGate
from .budget import BUDGET_CLASSIFICATION, BudgetTracker
from .history import NullTranscriptStore, TranscriptStore, TurnRecord
from .mcp_tool_capability import ConnectorCapabilityFailure
from .memory import (
    ConsolidationResult,
    MemoryRecord,
    MemoryStore,
    NullMemoryStore,
    Provenance,
    consolidate_memory,
    utcnow_iso,
)
from .otel import RunTracer, _GenerationSpan
from .side_effects import SideEffectClassifier
from .translate import TurnState, translate_message

logger = logging.getLogger(__name__)

SessionFactory = Callable[[], ModelSession]

# The SDK surfaces a provider auth rejection (HTTP 401/403 -- e.g. a placeholder,
# revoked, or wrong model key) as an ``AssistantMessage.error`` of this code
# (see ``claude_agent_sdk.types.AssistantMessageError``). Unlike a 5xx or a rate
# limit, a rejected credential is terminal and NON-retryable: retrying it only
# burns wall time (the SDK/CLI otherwise backs off and re-attempts until a ~2min
# timeout, surfacing as a generic hang). The runner fails the turn fast on this
# signal instead of streaming a non-terminal error and continuing to drive the
# session.
_AUTH_REJECTION_SDK_CODE = "authentication_failed"

# Classification carried on the fast-fail error event so consumers (F1's retry
# rules) can tell a rejected credential from a transient failure and NOT retry
# it -- distinct from both a budget halt and a generic runner error.
AUTH_REJECTED_CLASSIFICATION = "model-credential-rejected"

# Classification tagging the OBSERVE-ONLY reconciliation warning (#544, Decision
# A2): a resumed policy-gate turn that armed gates yet took no action -- the
# model was approved and resumed but never re-called the gated tool. This is a
# non-terminal warning frame (the final stays a clean terminal), stable so the
# invisible "approved but never acted" case becomes queryable. It is NOT AC1
# coverage: side_effect_emitted is a proxy for "some tool ran", not "the
# approved action ran", so it false-alarms on a text-only decision and
# false-passes on any incidental tool -- which is why A2 ships observe-only.
APPROVAL_NOT_ACTED_CLASSIFICATION = "approval-not-acted"
# The false-completion warning classification (#517): a turn declared DONE with a
# substantive answer but no tool-call evidence. Rides the free-form
# ErrorEvent.classification field like the markers above, so it is contract-safe.
FALSE_COMPLETION_CLASSIFICATION = "false-completion"
# The fail-closed classification for a publication call the runner observed on
# the stream but could NOT record as a pending approval (#2294): a malformed
# proposal, or a gate that does not carry the publication tool at all. Unlike
# the two observe-only markers above this one is terminal -- it rides an
# error+final pair, because a publication request that produced no approval
# record must never finalize looking like a clean turn.
PUBLICATION_UNRECORDED_CLASSIFICATION = "publication-unrecorded"
# Declared-connector capability/auth failure (#2519). Distinct from a model
# credential rejection: the session can still boot, but the connector's tools
# are not available. Rides ErrorEvent.classification; the Final is DONE so the
# worker posts the diagnosis text instead of overwriting it with escalate copy.
CONNECTOR_CAPABILITY_FAILED = "connector-capability-failed"


def _is_auth_rejection(message: object) -> bool:
    """True when an SDK message reports a provider credential rejection (401/403)."""

    return (
        isinstance(message, AssistantMessage)
        and getattr(message, "error", None) == _AUTH_REJECTION_SDK_CODE
    )


def _apply_approval_override(final: Final, state: TurnState) -> Final:
    """Flip a final to awaiting-approval when a gate fired (ADR-0010, #1852).

    A DONE final is overridden, as it always was. A NON-DONE final is overridden
    only when the runner's own gate requested the halt
    (``approval_halt_requested``) AND nothing else reported a real failure:
    since #1852 a gated deny carries the SDK's turn-stopping flags
    (``PermissionResultDeny.interrupt`` / the hook's ``continue_: False``), so
    the CLI aborts the turn and its terminal ``ResultMessage`` arrives
    ``is_error``-shaped, which ``translate.py::_translate_result`` maps to
    CLASSIFIED_FAILURE. Without honoring the flag, the fix for the hang would
    turn it into a failure carrying no approval record -- a worse outcome than
    the hang, because there would be nothing for a human to approve.

    **Precedence: a failure the runner did not cause outranks the halt.** The
    halt marker is set by ``ApprovalGate.block`` at deny time, BEFORE the turn's
    terminal cause is known, so on its own it cannot tell "the CLI aborted
    because we asked it to" from "the provider fell over a moment later". The
    tiebreaker is ``TurnState.error_classification``, which ``translate.py``
    sets ONLY where the model or transport reported a classified error of its
    own (an ``AssistantMessage.error``, or a rejected ``RateLimitEvent``) and
    deliberately NOT on the bare error-shaped result an abort produces. So:

    - halt marker, no classified error  -> the abort is ours; pause for approval
      (the #1852 case, where the alternative is losing the approval record);
    - halt marker AND a classified error -> the model/transport failed on its
      own; report the failure. Relabelling a provider outage as
      awaiting-approval would hide it behind a human decision that cannot fix
      it, and approving it would resume straight back into the same failure.

    Both branches require ``state.approval_summary``, so a halt recorded with
    no summary cannot flip a final on its own.

    What still outranks a pending approval, unchanged:

    - the **budget halt**, checked before this call in ``_drive_turn`` (a run
      that blew its ceiling has not completed cleanly, and approving it would
      resume straight back into the same halt);
    - a **genuine operator interrupt**, excluded upstream at the
      ``_merge_gate_block`` guard (the operator asked for the turn to stop and
      must get idle-awaiting-input, not a pause behind a decision they did not
      request);
    - an **auth rejection**, which returns before any final exists.

    A DONE final is untouched by the new guard: a turn the model finished
    cleanly is an approval pause regardless of any non-terminal error frame it
    streamed along the way (a recovered rate limit, say).

    The captured summary rides the final so the platform can persist it on the
    durable Approval record.
    """

    runner_halted_the_turn = (
        state.approval_halt_requested and state.error_classification is None
    )
    if state.approval_summary and (
        final.status is SessionStatus.DONE or runner_halted_the_turn
    ):
        return Final(
            text=final.text,
            status=SessionStatus.AWAITING_APPROVAL,
            approval_summary=state.approval_summary,
            approval_route=state.approval_route,
            approval_gate_kind=state.approval_gate_kind,
            approval_granted_tool=state.approval_granted_tool,
        )
    return final


class SessionRunner:
    """Drives one model session, streaming ACI NDJSON for each inbound frame."""

    def __init__(
        self,
        *,
        session_factory: SessionFactory,
        ceiling: int,
        tracer: RunTracer,
        classifier: SideEffectClassifier,
        trace_name: str,
        session_id: str | None = None,
        model: str | None = None,
        memory_store: MemoryStore | None = None,
        history_store: TranscriptStore | None = None,
        approval_gate: ApprovalGate | None = None,
        approval_resumed_kind: str | None = None,
        approval_decision: str | None = None,
        false_completion_check: bool = False,
        connector_failures: tuple[ConnectorCapabilityFailure, ...] = (),
    ) -> None:
        self._factory = session_factory
        self._ceiling = ceiling
        self._tracer = tracer
        self._classifier = classifier
        self._trace_name = trace_name
        self._session_id = session_id
        self._model = model
        # The memory port (#264). Prior memory is loaded at boot and delivered
        # via the system prompt; this store is the write side for learned records
        # (append + provenance). NullMemoryStore when no CURIE_MEMORY_REF.
        self._memory: MemoryStore = memory_store or NullMemoryStore()
        # The conversation-history port (#20). Prior turns are loaded at boot and
        # delivered via the system prompt; this store is the write side, appended
        # once per successful turn so a restarted sandbox rehydrates the thread.
        # NullTranscriptStore when no CURIE_HISTORY_REF.
        self._history: TranscriptStore = history_store or NullTranscriptStore()
        # The permission gate (#245): the can_use_tool callback records a
        # blocked approval-required call here, and the turn's final is flipped
        # to awaiting-approval on the same override the policy gate uses.
        self._approval_gate = approval_gate
        # The authority-free resume marker (#544, Decision A2): 'policy' when
        # this boot is resuming from a policy-gate approval. It confers no
        # capability -- it only arms the observe-only turn-end reconciliation.
        self._approval_resumed_kind = approval_resumed_kind
        # ADR-0076 Stone 3 (#889, epic #512): the resolved terminal decision
        # (approved/rejected/expired) of the approval this resume boot is
        # resuming from, stamped onto the turn's OTel span. Authority-free,
        # like approval_resumed_kind -- it confers no capability.
        self._approval_decision = approval_decision
        # Opt-in, observe-only false-completion check (#517): warn when a turn
        # ends DONE with a substantive answer but zero tool calls. Off by default.
        self._false_completion_check = false_completion_check
        # Declared-connector probe/expansion failures (#2519). Empty means the
        # model runs as usual. Non-empty short-circuits every turn before query
        # so the agent cannot answer from memory with the connector's tools gone.
        self._connector_failures = connector_failures

        self._session: ModelSession | None = None
        # One turn consumes the SDK generator at a time. This MUST be a
        # Semaphore, not an anyio.Lock, to survive a cross-task teardown: if a
        # run_turn generator is ever finalized by the asyncgen GC on a task
        # other than the one that opened it (the client-disconnect race #679),
        # anyio.Lock.release() from that non-owner task raises "current task is
        # not holding this lock" and leaves _owner_task set -- wedging the lock
        # permanently so every future turn blocks forever. A Semaphore's release
        # is owner-agnostic, so it frees cleanly no matter which task closes the
        # generator. The server's contextlib.aclosing (see server.py) is the
        # primary fix -- it keeps finalization on the driving task -- and this is
        # the defense-in-depth that keeps a stray cross-task close from wedging.
        # max_value=1 keeps the loud double-release guard anyio.Lock gave us: a
        # stray unbalanced release raises ValueError instead of silently
        # over-permitting two concurrent turns on the single SDK generator.
        self._turn_lock = anyio.Semaphore(1, max_value=1)
        self._interrupt_requested = False
        # Timeout is deliberately distinct from an ACI/operator interrupt: the
        # former is a failed delivery boundary while the latter is an intentional
        # cancellation. The opaque epoch binds the side-channel request to exactly
        # this handler-owned turn without becoming an ACI field or trace attribute.
        self._timeout_requested = False
        # Set only for an accepted timeout control request. The SDK serializes
        # control and query lines onto its stdin, and the CLI interrupt applies
        # only to the query live when that line is read. The run-turn owner waits
        # for this event before deciding whether the ordered stop was delivered
        # or needs the abandonment safety-net below.
        self._timeout_interrupt_settled: anyio.Event | None = None
        self._timeout_interrupt_delivered = False
        self._turn_epoch: str | None = None
        self._status = SessionStatus.IDLE_AWAITING_INPUT
        self._started = False
        # True only while a turn can still accept a steer: from turn start until
        # the terminal final is produced. It is cleared the instant a turn
        # terminates -- before the lock releases -- so a steer landing in the
        # finish-race window (final produced, lock not yet freed) is rejected
        # instead of writing into an already-terminal stream.
        self._turn_open = False

    @property
    def status(self) -> SessionStatus:
        return self._status

    @property
    def ready(self) -> bool:
        return self._started

    @property
    def turn_active(self) -> bool:
        """True while a turn can still accept a steer (open, pre-terminal)."""

        return self._turn_open

    async def remember(
        self,
        content: str,
        *,
        source_trace_ids: tuple[str, ...] = (),
    ) -> None:
        """Append a learned record to durable memory with provenance (#264).

        Provenance links the entry to the session that produced it and the source
        traces the lesson was distilled from. The write goes to the external
        store, so the record survives suspend/resume and is reloaded at the next
        boot. This is the write side of the memory port; the automatic
        learned-record extraction that calls it is later work (#265/#266/#267).
        """

        record = MemoryRecord(
            content=content,
            provenance=Provenance(
                learned_from_session_id=self._session_id,
                source_trace_ids=source_trace_ids,
                recorded_at=utcnow_iso(),
            ),
        )
        await self._memory.append(record)

    async def _record_turn(self, event: Event, state: TurnState) -> None:
        """Append one completed turn to the durable conversation transcript (#20).

        Only a successful DONE terminal final sets ``state.final_text``; failed,
        budget-halted, auth-halted, awaiting-approval, and idle turns leave it
        None and are not persisted, so the transcript holds the delivered
        exchange, not error stubs. Best-effort:
        a transient store failure is logged and never propagated -- recording
        history must not fail a turn the user already received an answer to.
        """

        if state.final_text is None:
            return
        try:
            await self._history.append(
                TurnRecord(
                    user=event.text,
                    assistant=state.final_text,
                    ts=utcnow_iso(),
                )
            )
        except Exception as exc:  # noqa: BLE001 - best-effort; never fail a completed turn
            logger.warning(
                "history append failed session=%s error_class=%s: %s",
                self._session_id,
                type(exc).__name__,
                exc,
            )

    async def consolidate_memory(self) -> ConsolidationResult:
        """Compact accumulated memory, merging duplicates and unioning provenance.

        The consolidation entry point (#265): loads the append-only memory log,
        collapses equivalent-content records into one while preserving every
        source trace, and writes the compacted set back when the store supports
        it. Safe to call at boot -- it is a no-op when there is no redundancy or
        when the store cannot rewrite (``NullMemoryStore``).
        """

        result = await consolidate_memory(self._memory)
        if result.written:
            logger.info(
                "memory consolidated: %d -> %d records (%d merged)",
                result.before,
                result.after,
                result.removed,
            )
        return result

    async def start(self) -> None:
        """Create and connect the model session (rehydrating if configured)."""

        self._session = self._factory()
        await self._session.connect()
        self._started = True

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
        self._tracer.shutdown()

    async def reset(self) -> None:
        """Discard the conversation and start a fresh model session (#550).

        Eval isolation: the eval driver calls this between cases so each case
        runs in a fresh conversation and cannot answer from an earlier case's
        history instead of actually invoking its tools (a false green for a
        side-effecting agent, and a silent order-dependence in the suite). Reset
        tears down the current SDK session and reconnects a new one from the same
        factory, so the next turn starts with no accumulated conversation; a
        thread with a durable ``CURIE_HISTORY_REF`` still rehydrates its own
        history preamble on reconnect (that is the thread's real history, not a
        cross-case leak), while an eval runner (no history ref) comes up empty.

        This is a deliberate, explicit control -- NOT per-turn session churn. The
        one-long-lived-session-per-process invariant (prompt-cache affinity
        across a thread's turns, ADR-0003) still holds for the message path,
        which never calls reset. Held under the turn lock so it can never race a
        live turn; the server refuses a reset while a turn is active (409) so the
        lock is free the moment this runs.
        """

        async with self._turn_lock:
            if self._session is not None:
                await self._session.close()
            self._session = self._factory()
            await self._session.connect()
            self._interrupt_requested = False
            self._timeout_requested = False
            self._timeout_interrupt_settled = None
            self._timeout_interrupt_delivered = False
            self._turn_epoch = None
            self._turn_open = False
            self._status = SessionStatus.IDLE_AWAITING_INPUT

    async def steer(self, text: str) -> bool:
        """Inject a follow-up message into the live turn without consuming output.

        Returns False when no turn is active (the finish-race boundary F1 owns:
        the caller falls back to opening a fresh turn). The steered output appears
        on the already-open turn's NDJSON stream.
        """

        if self._session is None or not self._turn_open:
            return False
        await self._session.query(text)
        return True

    async def interrupt(self, _reason: str = "") -> None:
        """Request a hard stop; the live turn's final is reclassified to idle."""

        self._interrupt_requested = True
        if self._session is not None:
            await self._session.interrupt()

    async def timeout(self, turn_epoch: str) -> bool:
        """Stop the exact current turn and mark its terminal as a timeout failure.

        The accepted flag is stored before the SDK await so every completion race
        sees timeout precedence. A replay, stale epoch, or call outside an open
        turn is a no-op; in particular it cannot poison the next lock owner.
        """

        current_epoch = self._turn_epoch
        if (
            self._session is None
            or not self._turn_open
            or self._timeout_requested
            or current_epoch is None
            or not hmac.compare_digest(
                turn_epoch.encode("utf-8"), current_epoch.encode("utf-8")
            )
        ):
            return False
        timeout_interrupt_settled = anyio.Event()
        self._timeout_requested = True
        self._timeout_interrupt_settled = timeout_interrupt_settled
        try:
            await self._session.interrupt()
            # A delayed SDK acknowledgement can return after this turn has
            # released ownership and a later turn has installed fresh timeout
            # state. Only the turn that still owns this completion event may
            # suppress its abandonment safety-net interrupt.
            if self._timeout_interrupt_settled is timeout_interrupt_settled:
                self._timeout_interrupt_delivered = True
        finally:
            timeout_interrupt_settled.set()
        return True

    async def run_inbound(self, message: Event | Interrupt) -> AsyncIterator[str]:
        """Produce the NDJSON a compliant runner emits for one inbound frame.

        A bare ``Interrupt`` (no active turn) yields a single idle-awaiting-input
        final, matching the ACI reference behavior; an ``Event`` runs a turn. This
        is the shared entrypoint the conformance producer validates.
        """

        if isinstance(message, Interrupt):
            yield to_ndjson_line(
                Final(text="run interrupted", status=SessionStatus.IDLE_AWAITING_INPUT)
            )
            self._status = SessionStatus.IDLE_AWAITING_INPUT
            return
        async for line in self.run_turn(message):
            yield line

    async def run_turn(
        self,
        event: Event,
        *,
        parent: Context | None = None,
        turn_epoch: str | None = None,
    ) -> AsyncGenerator[str]:
        """Run one turn, streaming ACI NDJSON lines and enforcing the budget.

        Returns an async *generator* (not just an iterator): the server wraps it
        in ``contextlib.aclosing`` so a client disconnect finalizes it on the
        driving task, and ``aclosing`` requires the ``aclose`` a generator has.
        """

        if self._session is None:
            raise RuntimeError("session not started")

        async with self._turn_lock:
            start = time.monotonic()
            logger.info("turn start session=%s user=%s", self._session_id, event.user)
            self._interrupt_requested = False
            self._timeout_requested = False
            self._timeout_interrupt_settled = None
            self._timeout_interrupt_delivered = False
            self._turn_epoch = turn_epoch
            self._turn_open = True
            state = TurnState()
            # A permission-gate block belongs to exactly one turn: clear any
            # prior turn's residue before the model runs (#245).
            if self._approval_gate is not None:
                self._approval_gate.reset()
            tracker = BudgetTracker(ceiling=self._ceiling)
            metric_outcome = "interrupted"
            metric_attributes = {
                "service.name": "curie-runner",
                "source": "runner",
                "outcome": "accepted",
            }
            record_metric("curie.turn.accepted", attributes=metric_attributes)
            metrics_emitted = False

            def emit_completed_metrics() -> None:
                """Emit the terminal metric pair once, synchronously."""

                nonlocal metrics_emitted
                if metrics_emitted:
                    return
                metrics_emitted = True
                completed_attributes = {
                    "service.name": "curie-runner",
                    "source": "runner",
                    "outcome": metric_outcome,
                }
                elapsed = time.monotonic() - start
                record_metric("curie.turn.completed", attributes=completed_attributes)
                record_metric(
                    "curie.turn.duration", elapsed, attributes=completed_attributes
                )

            try:
                with self._tracer.run_span(
                    self._trace_name,
                    self._model,
                    self._session_id,
                    event.user,
                    approval_decision=self._approval_decision,
                    parent=parent,
                ) as gen:
                    try:
                        if self._connector_failures:
                            message = " ".join(
                                failure.caller_message()
                                for failure in self._connector_failures
                            )
                            state.final_text = message
                            for line in self._connector_capability_halt_lines(gen, message):
                                if isinstance(parse_ndjson_line(line), Final):
                                    metric_outcome = self._metric_outcome(tracker)
                                yield line
                        else:
                            async for line in self._drive_turn(
                                event, state, tracker, gen
                            ):
                                if isinstance(parse_ndjson_line(line), Final):
                                    # The terminal decision is authoritative once the
                                    # Final reaches the consumer, even if it closes
                                    # without requesting the generator's next item.
                                    metric_outcome = self._metric_outcome(tracker)
                                yield line
                        logger.info(
                            "turn end session=%s status=%s duration_ms=%d",
                            self._session_id,
                            self._status.value,
                            int((time.monotonic() - start) * 1000),
                        )
                        # Persist the completed turn to the durable transcript so a
                        # restarted sandbox can rehydrate this thread (#20).
                        await self._record_turn(event, state)
                        metric_outcome = self._metric_outcome(tracker)
                    except Exception as exc:  # noqa: BLE001 - the ACI stream must
                        # always terminate in a final; a raised SDK/transport error
                        # becomes a classified failure unless a requested interrupt
                        # released the iterator, in which case it is cancellation.
                        # GeneratorExit (consumer disconnect) is a BaseException and
                        # is intentionally not caught here -- the finally handles
                        # that abandonment case.
                        self._turn_open = False
                        if self._timeout_requested:
                            # The body-boundary timeout is a failure even when the
                            # SDK reports its interrupt as an iterator exception.
                            # The caller may still be reading this direct session
                            # path, so retain a terminal final when it is deliverable.
                            self._status = SessionStatus.CLASSIFIED_FAILURE
                            metric_outcome = self._metric_outcome(tracker)
                            gen.finish_turn(
                                timeout_requested=True,
                                interrupt_requested=self._interrupt_requested,
                                classified_failure=True,
                            )
                            yield to_ndjson_line(
                                Final(
                                    text="run timed out",
                                    status=SessionStatus.CLASSIFIED_FAILURE,
                                )
                            )
                        elif self._interrupt_requested:
                            # Some SDK iterators surface the runner-requested
                            # interrupt as an exception instead of a terminal
                            # ResultMessage. The interrupt remains authoritative:
                            # do not expose or log the implementation exception as
                            # a model failure, and close every active phase as an
                            # intentional cancellation.
                            self._status = SessionStatus.IDLE_AWAITING_INPUT
                            metric_outcome = "interrupted"
                            gen.finish_turn(
                                interrupt_requested=True,
                                classified_failure=False,
                            )
                            yield to_ndjson_line(
                                Final(
                                    text="run interrupted",
                                    status=SessionStatus.IDLE_AWAITING_INPUT,
                                )
                            )
                        else:
                            logger.error(
                                "turn failed session=%s error_class=%s: %s duration_ms=%d",
                                self._session_id,
                                type(exc).__name__,
                                exc,
                                int((time.monotonic() - start) * 1000),
                            )
                            self._status = SessionStatus.CLASSIFIED_FAILURE
                            metric_outcome = self._metric_outcome(tracker)
                            self._set_failed(gen)
                            yield to_ndjson_line(
                                ErrorEvent(
                                    message=f"runner error: {exc}",
                                    classification="runner-error",
                                )
                            )
                            yield to_ndjson_line(
                                Final(
                                    text="run failed",
                                    status=SessionStatus.CLASSIFIED_FAILURE,
                                )
                            )
                    finally:
                        try:
                            # A timeout accepted while the stream was suspended at
                            # a yield reaches this no-yield block via GeneratorExit
                            # (or cancellation). Store its root terminal and metric
                            # pair synchronously, before cleanup can await or be
                            # cancelled; RunTracer's later abandonment fallback is
                            # idempotent.
                            if self._timeout_requested:
                                self._status = SessionStatus.CLASSIFIED_FAILURE
                                gen.finish_turn(
                                    timeout_requested=True,
                                    interrupt_requested=self._interrupt_requested,
                                    classified_failure=True,
                                )
                                metric_outcome = self._metric_outcome(tracker)
                                emit_completed_metrics()
                        finally:
                            # The SDK serializes this turn's stop and any later
                            # query onto one locked stdin stream. Wait until the
                            # timeout attempt settles before cleanup: a normally
                            # returned stop is already ordered ahead of the next
                            # query and the CLI cannot latch it for that later
                            # turn. If the timeout call does not return normally,
                            # the cleanup path below supplies the safety-net while
                            # this turn still owns the lock; cancellation before
                            # the write sends nothing for a later turn to observe.
                            timeout_settled = self._timeout_interrupt_settled
                            if timeout_settled is not None:
                                with anyio.CancelScope(shield=True):
                                    await timeout_settled.wait()
                            # Retire this turn's control token before cleanup can
                            # await the session-global interrupt. A timeout that
                            # arrives during that await is stale and must not
                            # queue a stop that could be consumed by the next
                            # turn after this owner releases the lock.
                            self._turn_epoch = None
                            # If the turn never reached a terminal final (_turn_open
                            # still set), the consumer abandoned the stream mid-run
                            # (client disconnect -> GeneratorExit, or cancellation).
                            # Stop the SDK so it cannot keep executing tools past the
                            # released turn lock and bleed into the next turn.
                            try:
                                if (
                                    self._turn_open
                                    and not self._timeout_interrupt_delivered
                                    and self._session is not None
                                ):
                                    with contextlib.suppress(Exception):
                                        await self._session.interrupt()
                            finally:
                                self._turn_open = False
                                self._turn_epoch = None
            finally:
                try:
                    emit_completed_metrics()
                finally:
                    self._turn_open = False
                    self._turn_epoch = None
                    self._timeout_interrupt_settled = None
                    self._timeout_interrupt_delivered = False

    def _metric_outcome(self, tracker: BudgetTracker) -> str:
        if self._timeout_requested:
            return "classified_failure"
        if self._status is SessionStatus.DONE:
            return "done"
        if self._status is SessionStatus.AWAITING_APPROVAL:
            return "awaiting_approval"
        if self._status is SessionStatus.CLASSIFIED_FAILURE:
            return "budget_halted" if tracker.exceeded else "classified_failure"
        if self._interrupt_requested:
            return "interrupted"
        return "idle"

    def _set_failed(self, gen: _GenerationSpan) -> None:
        """Store timeout first when a generic failure site wins the race."""

        if self._timeout_requested:
            gen.finish_turn(
                timeout_requested=True,
                interrupt_requested=self._interrupt_requested,
                classified_failure=True,
            )
        else:
            gen.set_failed()

    async def _drive_turn(
        self,
        event: Event,
        state: TurnState,
        tracker: BudgetTracker,
        gen: _GenerationSpan,
    ) -> AsyncIterator[str]:
        """Drive one turn to a terminal final (budget/interrupt overrides applied)."""

        assert self._session is not None
        gen.query_observed()
        await self._session.query(event.text)
        async for message in self._session.receive_turn():
            if isinstance(message, StreamedToolUseBoundary):
                gen.record_first_response_boundary()
                gen.streamed_tool_use(
                    message.call_id,
                    message.tool_name,
                    observed_time_ns=message.observed_time_ns,
                )
                continue
            if isinstance(message, PartialMessageBoundary):
                gen.record_first_response_boundary()
                continue
            if _is_auth_rejection(message):
                # A rejected model credential is terminal: stop the live session
                # so the SDK/CLI does not keep retrying with backoff to the wall,
                # then surface a distinct, immediate classified failure. Suppress
                # a failing interrupt (a wedged transport -- the very state a bad
                # credential can cause) so it cannot propagate to the generic
                # retryable ``runner-error`` handler and defeat the fast-fail; the
                # terminal ``model-credential-rejected`` error is emitted regardless.
                with contextlib.suppress(Exception):
                    await self._session.interrupt()
                self._set_failed(gen)
                for line in self._auth_halt_lines():
                    yield line
                return
            usage = getattr(message, "usage", None)
            # The terminal result carries the authoritative turn total; streaming
            # assistant messages carry per-message output. Fold them differently
            # so the same tokens are not counted twice (see BudgetTracker).
            if isinstance(message, ResultMessage):
                tracker.set_total(usage)
            else:
                tracker.add_increment(usage)
            budget_hit = tracker.exceeded
            events = translate_message(message, state, self._classifier, gen)
            # Synchronous, on the very iteration that delivered the block and
            # strictly before any ResultMessage iteration classifies the turn
            # (#2294). Never a task, and nothing is awaited between observing a
            # publication call and classifying the turn that made it.
            self._observe_publication_calls(state)
            decided_result_final: Final | None = None
            if isinstance(message, ResultMessage):
                terminal_reason = getattr(message, "terminal_reason", None)
                cancelled = self._interrupt_requested and not self._timeout_requested
                subtype = message.subtype or ""
                result_failed = self._timeout_requested or budget_hit or (
                    not cancelled and (message.is_error or subtype.startswith("error"))
                )
                if not budget_hit:
                    self._merge_gate_block(state)
                    sdk_final = next(
                        (outbound for outbound in events if isinstance(outbound, Final)),
                        None,
                    )
                    if sdk_final is not None:
                        decided_result_final = _apply_approval_override(
                            self._reclassify(sdk_final), state
                        )
                gen.result_boundary_observed(
                    failed=result_failed,
                    terminal_reason=terminal_reason,
                    approval_paused=decided_result_final is not None
                    and decided_result_final.status
                    is SessionStatus.AWAITING_APPROVAL,
                )

            for outbound in events:
                if isinstance(outbound, ToolNote):
                    logger.info("tool call session=%s tool=%s", self._session_id, outbound.tool)
                if isinstance(outbound, ErrorEvent):
                    logger.error(
                        "model error session=%s classification=%s",
                        self._session_id,
                        outbound.classification,
                    )
                if isinstance(outbound, Final):
                    if budget_hit:
                        self._set_failed(gen)
                        for line in self._budget_halt_lines():
                            yield line
                        return
                    if decided_result_final is None:
                        self._merge_gate_block(state)
                        final = _apply_approval_override(
                            self._reclassify(outbound), state
                        )
                    else:
                        final = decided_result_final
                    unrecorded = self._publication_unrecorded_lines(state, final)
                    if unrecorded:
                        self._set_failed(gen)
                        for line in unrecorded:
                            yield line
                        return
                    self._log_publication_fallback_alone(state)
                    self._log_publication_not_carried(state, final)
                    for line in self._approval_not_acted_lines(state, final):
                        yield line
                    for line in self._false_completion_lines(state, final):
                        yield line
                    # A timeout can land while one of the warning lines above is
                    # suspended at its yield. Re-apply its precedence immediately
                    # before publishing the terminal final.
                    final = self._reclassify(final)
                    self._status = final.status
                    self._turn_open = False
                    gen.finish_turn(
                        timeout_requested=self._timeout_requested,
                        interrupt_requested=self._interrupt_requested,
                        classified_failure=final.status
                        is SessionStatus.CLASSIFIED_FAILURE,
                        approval_paused=final.status
                        is SessionStatus.AWAITING_APPROVAL,
                        completed_without_result=final.status
                        is SessionStatus.AWAITING_APPROVAL,
                    )
                    # Only a clean DONE reply belongs in the conversation
                    # transcript. Classified failures and approval pauses are
                    # terminal delivery outcomes, not assistant answers (#20).
                    if final.status is SessionStatus.DONE:
                        state.final_text = final.text
                    yield to_ndjson_line(final)
                    return
                yield to_ndjson_line(outbound)

            if budget_hit:
                # Budget crossed on a non-terminal message: stop the live run,
                # then emit the same error+final pair.
                await self._session.interrupt()
                self._set_failed(gen)
                for line in self._budget_halt_lines():
                    yield line
                return

        # The turn iterator ended without a terminal result (e.g. an interrupt
        # aborted before the model produced one). Emit a final so the stream
        # always terminates in a final event.
        if self._timeout_requested:
            status = SessionStatus.CLASSIFIED_FAILURE
        elif self._interrupt_requested:
            status = SessionStatus.IDLE_AWAITING_INPUT
        else:
            status = SessionStatus.DONE
        self._merge_gate_block(state)
        final = _apply_approval_override(Final(text="", status=status), state)
        unrecorded = self._publication_unrecorded_lines(state, final)
        if unrecorded:
            self._set_failed(gen)
            for line in unrecorded:
                yield line
            return
        self._log_publication_fallback_alone(state)
        self._log_publication_not_carried(state, final)
        for line in self._approval_not_acted_lines(state, final):
            yield line
        for line in self._false_completion_lines(state, final):
            yield line
        final = self._reclassify(final)
        self._status = final.status
        self._turn_open = False
        gen.finish_turn(
            timeout_requested=self._timeout_requested,
            interrupt_requested=self._interrupt_requested,
            classified_failure=final.status is SessionStatus.CLASSIFIED_FAILURE,
            approval_paused=final.status is SessionStatus.AWAITING_APPROVAL,
            completed_without_result=final.status is SessionStatus.AWAITING_APPROVAL,
        )
        yield to_ndjson_line(final)

    def _observe_publication_calls(self, state: TurnState) -> None:
        """Record every publication call the runner sees on the stream (#2294).

        The runner's own observer, alongside the PreToolUse hook (#1852) and
        ``can_use_tool`` (#245). Against the real SDK it is normally the FIRST
        writer, not a rare backstop: the ``AssistantMessage`` carrying the
        ``ToolUseBlock`` reaches this loop before the CLI dispatches PreToolUse
        (observed live, #2294), so the hook's own ``block`` then finds the
        record already standing and only adds its halt marker. The fake tier is
        the inverted case -- its gate seam runs before the block is delivered,
        so there the observation is the duplicate.

        Either ordering yields exactly one record with identical fields, which
        is the point. What this closes is the case the live run hit: neither SDK
        layer recorded the call at all, the in-process tool body ran and
        returned its defensive ``is_error``, and the turn was about to finalize
        DONE with nothing for a human to approve.

        It can only record or fail closed -- it never allows a call and never
        mints a grant. Called on every message of the turn, it acts only on
        calls it has not seen yet, and it is deliberately synchronous: the
        record must stand before the ResultMessage iteration classifies the
        turn (see the call site in ``_drive_turn``).

        A call it cannot record stores the FIRST such reason in
        ``state.publication_unrecorded``, for the fail-closed error message
        ONLY. It is deliberately NOT sticky: observation continues over every
        later call in the turn, because the gate's own deny text tells the model
        to "Correct it and retry", so a malformed first attempt must not poison
        the corrected record that follows it. Whether the turn actually fails
        closed is decided from the OUTCOME at classification time -- see
        ``_publication_unrecorded_lines``.
        """

        gate = self._approval_gate
        while state.publication_calls_observed < len(state.publication_calls):
            payload = state.publication_calls[state.publication_calls_observed]
            state.publication_calls_observed += 1
            if gate is None or PUBLISH_TOOL_NAME not in gate.required:
                # The model named the publication tool where the platform has no
                # gated checkout to publish from. Recording it anyway would ask a
                # human to authorize an action that cannot exist, so fail closed.
                state.publication_unrecorded = state.publication_unrecorded or (
                    "the session carries no publication approval gate, so the"
                    " request could not be recorded"
                )
                continue
            try:
                recorded = gate.observe_publication(payload)
            except ValueError as exc:
                # First reason wins for the message; the loop carries on so a
                # corrected retry later in the same turn can still record.
                state.publication_unrecorded = state.publication_unrecorded or str(exc)
                continue
            if recorded:
                # Neutral wording, and NOT a warning: against the real SDK this
                # observer is normally the FIRST writer, because the
                # AssistantMessage carrying the ToolUseBlock reaches this loop
                # before the CLI dispatches PreToolUse (observed live, #2294,
                # session=live-publish-gated). Writing first proves nothing about
                # whether a gate layer decided, so claiming "no layer recorded
                # this" here fired on the fully gated path too. The real
                # layer-missed signal is at turn end, in
                # ``_log_publication_fallback_alone``.
                logger.debug(
                    "publication call observed on the stream and recorded session=%s",
                    self._session_id,
                )
            else:
                # A record already stands. On the fake tier that is the gate
                # layer's (it runs before the block is delivered); on the real
                # path it is this observer's own earlier call in the same turn.
                logger.debug(
                    "publication already recorded this turn session=%s",
                    self._session_id,
                )

    def _log_publication_fallback_alone(self, state: TurnState) -> None:
        """Warn when the stream observer was the ONLY layer that decided (#2294).

        The honest "a gate layer missed this call" signal, and it can only be
        computed at turn end. Per-call it is unknowable: against the real SDK the
        stream delivers the ``ToolUseBlock`` to ``_drive_turn`` BEFORE the CLI
        dispatches PreToolUse, so the observer writes first on every real
        publication call, gated or not (observed live, #2294). Only the fake tier
        runs its gate seam ahead of the block, so "the hook got there first"
        describes that tier alone.

        ``pending_halt`` is the discriminator: it is set by ``ApprovalGate.block``
        and by nothing else, so ONLY the PreToolUse hook (#1852) or
        ``can_use_tool`` (#245) can set it. The observer deliberately never does.
        A turn that observed a publication call, ends holding its record, and
        still has ``pending_halt`` False is therefore a turn in which neither SDK
        layer denied the call -- the exact #2294 condition, and the one an
        operator needs to see, because a gate that reports itself armed while
        deciding nothing is the one they trust.

        Called from the two Final emission sites, which are mutually exclusive
        (each returns after yielding), so this logs at most once per turn.
        Skipped on the interrupt/timeout paths: neither ran the gate to a
        decision, so neither says anything about the layers.
        """

        gate = self._approval_gate
        if (
            not state.publication_calls
            or gate is None
            or gate.pending_summary is None
            or gate.pending_granted_tool != PUBLISH_TOOL_NAME
            or gate.pending_halt
            or self._interrupt_requested
            or self._timeout_requested
        ):
            return
        logger.warning(
            "publication stream fallback stood alone session=%s: neither the"
            " PreToolUse hook nor can_use_tool denied this publication call;"
            " the pending approval was recorded from the stream",
            self._session_id,
        )

    def _log_publication_not_carried(self, state: TurnState, final: Final) -> None:
        """Warn when a parked turn carries a DIFFERENT approval than the publish.

        A turn has one pending approval slot, so a publication call can lose it
        to a gated tool denied earlier in the turn or to a policy
        ``request_approval`` (``_merge_gate_block`` never overwrites a standing
        policy summary with a permission block). That is legitimate -- the turn
        parks on a real decision and nothing is lost -- but the publish intent
        does NOT ride the final, so after the human resolves the card the model
        must ask to publish again. Silently dropping that is how a publication
        request disappears without any operator-visible trace, so it is a
        warning rather than a failure: failing the turn would discard the very
        approval a human still has to act on.

        Called from the two mutually exclusive Final emission sites, so it logs
        at most once per turn.
        """

        if (
            not state.publication_calls
            or final.status is not SessionStatus.AWAITING_APPROVAL
            or final.approval_granted_tool == PUBLISH_TOOL_NAME
        ):
            return
        if state.publication_unrecorded is not None:
            # Not slot contention: this request was never recordable in the
            # first place (a malformed proposal, or a gate without the publish
            # tool). Naming only the other approval here would misreport it and
            # send an operator looking for a race that did not happen.
            logger.warning(
                "publication request not carried session=%s: the publication"
                " request itself could not be recorded: %s; the turn is awaiting"
                " a different approval (%s)",
                self._session_id,
                state.publication_unrecorded,
                final.approval_granted_tool or final.approval_gate_kind or "unknown",
            )
            return
        logger.warning(
            "publication request not carried session=%s: the turn is awaiting a"
            " different approval (%s); the model must request publication again"
            " after it resolves",
            self._session_id,
            final.approval_granted_tool or final.approval_gate_kind or "unknown",
        )

    def _publication_unrecorded_lines(self, state: TurnState, final: Final) -> list[str]:
        """The fail-closed error+final pair for an unrecordable publication (#2294).

        Modeled on ``_auth_halt_lines``: a terminal pair, not an observe-only
        warning frame. The runner saw a publication request and the turn is
        ending with no pending approval to show for it, so the only safe report
        is a classified failure carrying NO approval fields -- an approval card
        with no usable proposal is worse than none, and a DONE final would be
        the very silent hole #2294 closed.

        **The decision is keyed on the FINAL about to be emitted, never on gate
        state.** A turn holds exactly ONE pending approval slot
        (``ApprovalGate._record_pending``'s first-block-wins rule, and
        ``_merge_gate_block``'s preference for a standing policy summary), so
        another approval can legitimately own it: a gated ``Bash`` call denied
        before the publish call arrived, or a ``request_approval`` the model
        raised in the same turn. Reading gate state would then rewrite a turn
        that is correctly parked on a real human decision into a classified
        failure, DESTROYING a card a human could have acted on -- a strictly
        worse outcome than the silent DONE #2294 set out to fix. So an
        AWAITING_APPROVAL final is never overridden; the unclaimed publish
        intent is reported by ``_log_publication_not_carried`` instead.

        A sticky "any attempt failed -> fail the turn" rule is wrong for the
        same reason: the gate's deny text tells the model to "Correct it and
        retry", so a malformed attempt followed by a corrected one must end
        awaiting-approval on the record that stands.

        Precedence is unchanged and deliberately narrow: a final that already
        reports a classified failure keeps its own cause, and an operator
        interrupt (or a timeout) outranks this entirely -- a human pressing stop
        gets idle-awaiting-input, not a failure about a request their stop
        already prevented. What is left is the real hole: publish calls were
        observed and the turn is about to report a clean, unparked outcome.
        """

        if (
            not state.publication_calls
            or final.status is SessionStatus.AWAITING_APPROVAL
            or final.status is SessionStatus.CLASSIFIED_FAILURE
            or self._interrupt_requested
            or self._timeout_requested
        ):
            return []
        reason = state.publication_unrecorded or (
            "the publication call was not recorded by any approval gate layer"
        )
        logger.error("publication request not recorded session=%s: %s", self._session_id, reason)
        self._turn_open = False
        self._status = SessionStatus.CLASSIFIED_FAILURE
        return [
            to_ndjson_line(
                ErrorEvent(
                    message=f"publication request was not recorded: {reason}",
                    classification=PUBLICATION_UNRECORDED_CLASSIFICATION,
                )
            ),
            to_ndjson_line(
                Final(
                    text="run failed: publication request could not be recorded",
                    status=SessionStatus.CLASSIFIED_FAILURE,
                )
            ),
        ]

    def _approval_not_acted_lines(self, state: TurnState, final: Final) -> list[str]:
        """The OBSERVE-ONLY reconciliation warning (#544, Decision A2).

        Emits a single non-terminal warning frame -- never a non-clean final --
        when a resumed POLICY turn armed gates yet took no action: the marker
        says the boot is resuming from a policy gate, gates are armed, the turn
        recorded no permission-gate block (no ``approval_summary``) and no
        side-effecting tool (``side_effect_emitted`` False), and it ended on a
        clean DONE final. That is the observed "approved, resumed, but the model
        never re-called the gated tool" case (edge 11b: a budget halt or error
        never reaches here, so only a clean turn end is reconciled).

        The signal is deliberately weak (``side_effect_emitted`` is a proxy for
        "some tool ran", not "the approved action ran"), so this is
        instrumentation, not a control -- it warns and leaves the final clean.
        """

        if (
            self._approval_resumed_kind == "policy"
            and self._approval_gate is not None
            and self._approval_gate.required
            and final.status is SessionStatus.DONE
            and not state.approval_summary
            and not state.side_effect_emitted
        ):
            logger.warning(
                "resumed policy approval not acted on session=%s: the approved "
                "action was never taken this turn",
                self._session_id,
            )
            return [
                to_ndjson_line(
                    ErrorEvent(
                        message=(
                            "resumed policy approval was not acted on this turn: "
                            "the approved action was never taken"
                        ),
                        classification=APPROVAL_NOT_ACTED_CLASSIFICATION,
                    )
                )
            ]
        return []

    def _false_completion_lines(self, state: TurnState, final: Final) -> list[str]:
        """The OBSERVE-ONLY false-completion warning (#517).

        Emits a single non-terminal warning frame -- never a non-clean final --
        when a turn declared DONE with a substantive answer yet made ZERO tool
        calls this turn. That is the runner-observable analog of "declared done
        with no tool-call evidence" (Grok's laziness classifier). It keys on
        ``tool_call_count`` (all tools), not ``side_effect_emitted`` (a proxy for
        "some non-idempotent tool ran"), so a read-only investigation counts as
        evidence and never trips this.

        Opt-in and observe-only, deliberately: the runner cannot tell a
        legitimately-answerable question ("what is 2+2", "summarize our last
        exchange") from a lazy false completion without judging task intent, which
        it does not do. So this instruments -- it warns and leaves the final clean
        -- and, like the approval-not-acted signal, gates on
        ``self._false_completion_check`` until real-trace rates justify any
        promotion to enforce. An approval pause, budget halt, or classified
        failure never reaches the clean-DONE case reconciled here.
        """

        if (
            self._false_completion_check
            and final.status is SessionStatus.DONE
            and state.tool_call_count == 0
            and final.text.strip()
            and not state.approval_summary
        ):
            logger.warning(
                "false completion session=%s: turn declared done with a "
                "substantive answer but made no tool call",
                self._session_id,
            )
            return [
                to_ndjson_line(
                    ErrorEvent(
                        message=(
                            "turn declared done with a substantive answer but made "
                            "no tool call this turn (no evidence backing the claim)"
                        ),
                        classification=FALSE_COMPLETION_CLASSIFICATION,
                    )
                )
            ]
        return []

    def _merge_gate_block(self, state: TurnState) -> None:
        """Fold the gate's recorded outcome (#245/#544) into the turn state.

        Two reconciliations happen here at turn end:

        - **Policy route (#544, Decision B):** the request_approval tool
          validated the model's ``route`` against the manifest. A refusal means
          no approval was created, so any summary translate.py captured off the
          raw block is dropped; an acceptance carries the RESOLVED route (the
          bound sole route or the named valid one) rather than the raw argument.
        - **Permission block (#245):** the can_use_tool callback records a
          blocked call on the shared gate; merging it here (only when no policy
          summary already stands) lets ``_apply_approval_override`` treat both
          trigger types identically, along with the durable provenance
          (#544, Decision C) the worker branches on.
        - **Approval halt (#1852):** a gated deny now asks the CLI to stop the
          turn, so the gate's ``pending_halt`` marker is carried onto the turn
          state for ``_apply_approval_override`` -- but only when the operator
          did not also interrupt. A human pressing stop is an intentional stop,
          not an approval request; reporting it as awaiting-approval would
          suspend the thread behind a decision nobody asked for.
        """

        gate = self._approval_gate
        if gate is None:
            return

        if gate.policy_requested:
            if gate.policy_rejected:
                # The route could not be resolved: no approval exists, so the
                # turn must not end awaiting-approval on it.
                state.approval_summary = None
                state.approval_route = None
                state.approval_gate_kind = None
            else:
                state.approval_route = gate.policy_route
                # #558: an operator-opted grantable gate mints the one-shot grant; the tool
                # comes from the manifest (never the model's summary/route args). A
                # non-grantable route resolves to None -> no grant, preserving #544's default.
                # gate_kind stays 'policy' (stamped in translate.py).
                state.approval_granted_tool = gate.grantable_tool_for_route(gate.policy_route)

        if gate.pending_summary and not state.approval_summary:
            state.approval_summary = gate.pending_summary
            state.approval_route = gate.pending_route
            state.approval_gate_kind = gate.pending_gate_kind
            state.approval_granted_tool = gate.pending_granted_tool

        # See the "Approval halt" bullet above: an operator interrupt outranks a
        # runner-requested one, so the marker is copied only in its absence.
        if (
            gate.pending_halt
            and not self._interrupt_requested
            and not self._timeout_requested
        ):
            state.approval_halt_requested = True

    def _budget_halt_lines(self) -> list[str]:
        """The error+final pair emitted whenever the output-token ceiling trips.

        The error carries the budget classification so downstream retry rules can
        tell a budget halt from any other classified failure.
        """

        logger.warning("budget halt session=%s: output token budget exceeded", self._session_id)
        self._turn_open = False
        self._status = SessionStatus.CLASSIFIED_FAILURE
        return [
            to_ndjson_line(
                ErrorEvent(
                    message="output token budget exceeded",
                    classification=BUDGET_CLASSIFICATION,
                )
            ),
            to_ndjson_line(
                Final(
                    text="run halted: output token budget exceeded",
                    status=SessionStatus.CLASSIFIED_FAILURE,
                )
            ),
        ]

    def _connector_capability_halt_lines(
        self, gen: _GenerationSpan, message: str
    ) -> list[str]:
        """ErrorEvent + DONE so the diagnosis reaches the message caller (#2519).

        Does not query the model. Final is DONE, not classified-failure: the
        worker's escalate path would replace the connector and credential names
        with a generic human-flag. completed_without_result keeps the OTel span
        from being marked abandoned. Logs names only, never values.
        """

        logger.error(
            "declared connector capability failed session=%s connectors=%s credentials=%s",
            self._session_id,
            ",".join(failure.connector for failure in self._connector_failures),
            ",".join(
                name
                for failure in self._connector_failures
                for name in failure.credential_names
            ),
        )
        self._turn_open = False
        self._status = SessionStatus.DONE
        gen.finish_turn(
            interrupt_requested=False,
            classified_failure=False,
            completed_without_result=True,
        )
        return [
            to_ndjson_line(
                ErrorEvent(
                    message=message,
                    classification=CONNECTOR_CAPABILITY_FAILED,
                )
            ),
            to_ndjson_line(Final(text=message, status=SessionStatus.DONE)),
        ]

    def _auth_halt_lines(self) -> list[str]:
        """The error+final pair emitted when the provider rejects the credential.

        Distinct from a budget halt and a generic runner error so downstream
        retry rules do NOT retry a rejected credential (retrying only burns the
        wall). The message names ``CURIE_CREDENTIALS`` since that is the ACI
        reference the operator must fix; it carries no credential value.
        """

        logger.error(
            "auth failure session=%s: model credential rejected by provider", self._session_id
        )
        self._turn_open = False
        self._status = SessionStatus.CLASSIFIED_FAILURE
        return [
            to_ndjson_line(
                ErrorEvent(
                    message="model credential rejected by provider (check CURIE_CREDENTIALS)",
                    classification=AUTH_REJECTED_CLASSIFICATION,
                )
            ),
            to_ndjson_line(
                Final(
                    text="run failed: model credential rejected by provider",
                    status=SessionStatus.CLASSIFIED_FAILURE,
                )
            ),
        ]

    def _reclassify(self, final: Final) -> Final:
        """Apply the interrupt override to a model-produced terminal final.

        A requested interrupt is an intentional stop, so the run is idle-awaiting-
        input regardless of the SDK's terminal subtype (a real interrupt often
        surfaces as an error result). Without the override an intentional stop
        would look like a failure and could trip F1's escalation path.
        """

        if self._timeout_requested:
            return Final(
                text=final.text or "run timed out",
                status=SessionStatus.CLASSIFIED_FAILURE,
            )
        if self._interrupt_requested:
            return Final(
                text=final.text or "run interrupted",
                status=SessionStatus.IDLE_AWAITING_INPUT,
            )
        return final
