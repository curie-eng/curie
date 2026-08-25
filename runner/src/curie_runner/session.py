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

from .adapter import ModelSession
from .approval import ApprovalGate
from .budget import BUDGET_CLASSIFICATION, BudgetTracker
from .history import NullTranscriptStore, TranscriptStore, TurnRecord
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
    """Flip a successful final to awaiting-approval when a gate fired (ADR-0010).

    Only a DONE final is overridden: a failure, budget halt, or intentional
    interrupt outranks a pending approval (the turn did not complete cleanly,
    so suspending on it would strand a broken run behind a human decision).
    The captured summary rides the final so the platform can persist it on the
    durable Approval record.
    """

    if state.approval_summary and final.status is SessionStatus.DONE:
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

        Only a successful terminal final sets ``state.final_text``; a failed,
        budget-halted, or auth-halted turn leaves it None and is not persisted, so
        the transcript holds the delivered exchange, not error stubs. Best-effort:
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
        self, event: Event, *, parent: Context | None = None
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
                        if self._status is SessionStatus.CLASSIFIED_FAILURE:
                            gen.set_failed()
                        else:
                            gen.set_succeeded()
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
                        # (CLI disconnect, auth expiry, model error) becomes a
                        # classified failure rather than a truncated, final-less
                        # stream. GeneratorExit (consumer disconnect) is a
                        # BaseException and is intentionally not caught here -- the
                        # finally handles that abandonment case.
                        logger.error(
                            "turn failed session=%s error_class=%s: %s duration_ms=%d",
                            self._session_id,
                            type(exc).__name__,
                            exc,
                            int((time.monotonic() - start) * 1000),
                        )
                        self._turn_open = False
                        self._status = SessionStatus.CLASSIFIED_FAILURE
                        metric_outcome = self._metric_outcome(tracker)
                        gen.set_failed()
                        yield to_ndjson_line(
                            ErrorEvent(
                                message=f"runner error: {exc}",
                                classification="runner-error",
                            )
                        )
                        yield to_ndjson_line(
                            Final(text="run failed", status=SessionStatus.CLASSIFIED_FAILURE)
                        )
                    finally:
                        # If the turn never reached a terminal final (_turn_open still
                        # set), the consumer abandoned the stream mid-run (client
                        # disconnect -> GeneratorExit, or cancellation). Stop the SDK
                        # so it does not keep executing tools past the released turn
                        # lock and bleed into the next turn. Best-effort.
                        if self._turn_open and self._session is not None:
                            with contextlib.suppress(Exception):
                                await self._session.interrupt()
                        self._turn_open = False
            finally:
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

    def _metric_outcome(self, tracker: BudgetTracker) -> str:
        if self._status is SessionStatus.DONE:
            return "done"
        if self._status is SessionStatus.AWAITING_APPROVAL:
            return "awaiting_approval"
        if self._status is SessionStatus.CLASSIFIED_FAILURE:
            return "budget_halted" if tracker.exceeded else "classified_failure"
        if self._interrupt_requested:
            return "interrupted"
        return "idle"

    async def _drive_turn(
        self,
        event: Event,
        state: TurnState,
        tracker: BudgetTracker,
        gen: _GenerationSpan,
    ) -> AsyncIterator[str]:
        """Drive one turn to a terminal final (budget/interrupt overrides applied)."""

        assert self._session is not None
        await self._session.query(event.text)
        async for message in self._session.receive_turn():
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
                        for line in self._budget_halt_lines():
                            yield line
                        return
                    self._merge_gate_block(state)
                    final = _apply_approval_override(self._reclassify(outbound), state)
                    for line in self._approval_not_acted_lines(state, final):
                        yield line
                    for line in self._false_completion_lines(state, final):
                        yield line
                    self._status = final.status
                    self._turn_open = False
                    # Capture the delivered reply so run_turn can persist the
                    # {user, assistant} pair to the conversation transcript (#20).
                    state.final_text = final.text
                    yield to_ndjson_line(final)
                    return
                yield to_ndjson_line(outbound)

            if budget_hit:
                # Budget crossed on a non-terminal message: stop the live run,
                # then emit the same error+final pair.
                await self._session.interrupt()
                for line in self._budget_halt_lines():
                    yield line
                return

        # The turn iterator ended without a terminal result (e.g. an interrupt
        # aborted before the model produced one). Emit a final so the stream
        # always terminates in a final event.
        status = (
            SessionStatus.IDLE_AWAITING_INPUT
            if self._interrupt_requested
            else SessionStatus.DONE
        )
        self._merge_gate_block(state)
        final = _apply_approval_override(Final(text="", status=status), state)
        for line in self._approval_not_acted_lines(state, final):
            yield line
        # No-op on this empty-text fallback final (the check requires a
        # substantive answer), kept beside its twin for parity.
        for line in self._false_completion_lines(state, final):
            yield line
        self._status = final.status
        self._turn_open = False
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

        if self._interrupt_requested:
            return Final(
                text=final.text or "run interrupted",
                status=SessionStatus.IDLE_AWAITING_INPUT,
            )
        return final
