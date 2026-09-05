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

from .adapter import (
    ModelSession,
    PartialMessageBoundary,
    StreamedToolUseBoundary,
    model_message_to_conversation,
)
from .approval import ApprovalGate
from .budget import BUDGET_CLASSIFICATION, BudgetTracker
from .history import (
    ApprovalContext,
    ConversationMessage,
    HarnessReplayState,
    NullTranscriptStore,
    TranscriptStore,
    TurnRecord,
    bound_turn_record,
    close_suspended_tool_calls,
)
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
        history_resumed: bool = False,
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
        # reconstructed as the harness's structured prefix; this store is the
        # write side, appended once per terminal turn so a restarted sandbox
        # rehydrates the thread. NullTranscriptStore when no CURIE_HISTORY_REF.
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
        self._history_resumed = history_resumed
        self._resume_cache_metric_recorded = False

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
        # Safe-boundary fence for replacing a runner. A fresh runner has no
        # completed turn to lose. Once a turn begins, only a successful durable
        # transcript append re-authorizes replacement, unless an earlier turn
        # was already lost by this process. Later appends cannot repair that
        # missing prefix, so the loss remains sticky for this runner's lifetime.
        self._history_durable = True
        self._history_loss_observed = False
        self._active_state: TurnState | None = None

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

    @property
    def history_durable(self) -> bool:
        """Whether every completed logical turn is present in durable replay."""

        return self._history_durable

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

        A successful DONE terminal final or an AWAITING_APPROVAL suspension sets
        ``state.final_text``. Failed, budget-halted, auth-halted, and idle turns
        leave it None and are not persisted, so the transcript holds delivered
        exchanges and resumable approval context, not error stubs. Best-effort:
        a transient store failure is logged and never propagated -- recording
        history must not fail a turn the user already received an answer to.
        """

        if state.final_text is None:
            return
        messages = (
            ConversationMessage(role="user", content=event.text),
            *state.history_messages,
        )
        if (
            self._status is SessionStatus.AWAITING_APPROVAL
            and state.approval_gate_kind == "permission"
        ):
            messages = close_suspended_tool_calls(messages)
        harness_replay: HarnessReplayState | None = None
        exporter = getattr(self._session, "export_replay_state", None)
        if callable(exporter):
            try:
                harness_replay = await exporter()
            except Exception as exc:  # noqa: BLE001 - portable replay remains valid
                logger.warning(
                    "harness replay export failed session=%s error_class=%s: %s",
                    self._session_id,
                    type(exc).__name__,
                    exc,
                )
        try:
            record = bound_turn_record(
                TurnRecord(
                    user=event.text,
                    assistant=state.final_text,
                    ts=utcnow_iso(),
                    messages=messages,
                    status=self._status.value,
                    approval=(
                        ApprovalContext(
                            summary=state.approval_summary,
                            route=state.approval_route,
                            gate_kind=state.approval_gate_kind,
                            granted_tool=state.approval_granted_tool,
                            decision=self._approval_decision,
                        )
                        if any(
                            (
                                state.approval_summary,
                                state.approval_route,
                                state.approval_gate_kind,
                                state.approval_granted_tool,
                                self._approval_decision,
                            )
                        )
                        else None
                    ),
                    harness_replay=harness_replay,
                )
            )
            await self._history.append(record)
            self._history_durable = not self._history_loss_observed
        except Exception as exc:  # noqa: BLE001 - best-effort; never fail a completed turn
            self._history_loss_observed = True
            self._history_durable = False
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
        structured replay on reconnect (that is the thread's real history, not
        a cross-case leak), while an eval runner (no history ref) comes up empty.

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
            self._active_state = None
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
        if self._active_state is not None:
            self._active_state.history_messages.append(
                ConversationMessage(role="user", content=text)
            )
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
            self._history_durable = False
            state = TurnState()
            self._active_state = state
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
                        async for line in self._drive_turn(event, state, tracker, gen):
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
                    self._active_state = None
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
            history_message = model_message_to_conversation(message)
            if history_message is not None:
                # Some harness streams echo the submitted user prompt before
                # assistant output. The durable turn already prepends the exact
                # inbound event, so drop only that leading duplicate.
                if not (
                    not state.history_messages
                    and history_message.role == "user"
                    and history_message.content == event.text
                ):
                    state.history_messages.append(history_message)
            usage = getattr(message, "usage", None)
            # The terminal result carries the authoritative turn total; streaming
            # assistant messages carry per-message output. Fold them differently
            # so the same tokens are not counted twice (see BudgetTracker).
            if isinstance(message, ResultMessage):
                tracker.set_total(usage)
                if self._history_resumed and not self._resume_cache_metric_recorded:
                    cache_read = (
                        int(usage.get("cache_read_input_tokens") or 0)
                        if isinstance(usage, dict)
                        else 0
                    )
                    record_metric(
                        "curie.history.resume.cache_read",
                        cache_read,
                        attributes={
                            "service.name": "curie-runner",
                            "source": "runner",
                            "cache_hit": "true" if cache_read > 0 else "false",
                        },
                    )
                    logger.info(
                        "history resume cache observed session=%s cache_read_input_tokens=%d",
                        self._session_id,
                        cache_read,
                    )
                    self._resume_cache_metric_recorded = True
            else:
                tracker.add_increment(usage)
            budget_hit = tracker.exceeded
            events = translate_message(message, state, self._classifier, gen)
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
                    # Persist clean replies and resumable approval suspensions;
                    # classified failures remain delivery outcomes, not history.
                    if final.status in {
                        SessionStatus.DONE,
                        SessionStatus.AWAITING_APPROVAL,
                    }:
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
        # A missing provider ResultMessage is still incomplete for a nominal
        # DONE turn. The one resumable exception is a runner-owned approval
        # halt: its structured tool call and gate context must cross runners.
        if final.status is SessionStatus.AWAITING_APPROVAL:
            state.final_text = final.text or state.assistant_text
        yield to_ndjson_line(final)

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
