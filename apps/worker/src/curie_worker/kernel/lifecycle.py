from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from aci_protocol import (
    QueuedTurn,
    SessionStatus,
    TurnSource,
)
from channel_protocol.reply import (
    NavAffordance,
)
from curie_internal import sandbox_token
from opentelemetry.trace import SpanKind, StatusCode

from .. import sweep
from ..behaviorpacks import (
    BehaviorPacks,
)
from ..binding import (
    DECISION_ENV,
    DEFAULT_EXECUTION_DEADLINE_SECONDS,
    GRANT_ARGUMENTS_ENV,
    GRANT_TOOL_ENV,
    ISSUE_READ_TOKEN_ENV,
    ISSUE_READ_URL_ENV,
    MAX_TURNS_ENV,
    PROGRESS_TOKEN_ENV,
    PROGRESS_URL_ENV,
    RESUMED_KIND_ENV,
    SANDBOX_TOKEN_TTL_SECONDS,
    AmbiguousRoute,
    binding_adapter_for_handle,
)
from ..capacity_wait import (
    current_wait,
)
from ..delivery_lease import DeliveryLease
from ..hook_runs import HookRunOutcome, HookRunReason, HookRunRecorderError, retry_expiry
from ..reply_sink import (
    TargetRoute,
)
from ..turn_progress import (
    ELIGIBILITY_ENV,
    plan_turn_progress,
    progress_eligible,
)
from ..workitem_dispatch import (
    WorkItemConflict,
    WorkItemRun,
    WorkItemStartRefused,
    WorkItemTransportError,
    parse_work_item_event_id,
)

if TYPE_CHECKING:
    from .core import Kernel

from . import (
    channel_read,
    claim,
    clock,
    constants,
    delivery,
    failures,
    hooks,
    log,
    memory,
    routing,
    workspace,
)
from .log import logger


async def process_event(
    self: Kernel, qevent: QueuedTurn, *, lease: DeliveryLease | None = None
) -> None:
    """Observe one kernel lifecycle while preserving its ``None`` result.

    ``lease`` is this delivery's ownership fence and overall deadline
    (ADR-0131). Keyword-only and OPTIONAL: the sweeper's re-emit path and
    every direct caller run without one and keep their pre-ADR-0131
    behavior exactly. It is never required, and its absence is never checked
    for -- the leaseless path is a supported caller, not a degraded one.
    """

    routing._check_targetless_shape(qevent)
    error: BaseException | None = None
    hook_token = constants._HOOK_RUN_CARRY.set(hooks._HookRunCarry())
    lease_token = constants._DELIVERY_LEASE.set(lease)
    # A redelivery must never inherit an earlier delivery's terminal-send mark
    # from this process (#2433).
    self._terminal_reply_attempted.discard(qevent.event_id)
    with log.operation_span(
        "curie.turn.process",
        kind=SpanKind.INTERNAL,
        attributes={"service.name": "curie-worker", "source": "worker"},
    ) as span:
        token = constants._LIFECYCLE_SPAN.set(span)
        outcome_token = constants._LIFECYCLE_OUTCOME.set(None)
        agent_token = constants._TURN_AGENT.set(None)
        try:
            if lease is None:
                # A caller with no lease reaches the body exactly as it did
                # before ADR-0131 -- one positional argument, no keyword.
                # The lease is genuinely optional here, not defaulted-away,
                # and the leaseless call shape is part of that contract.
                await self._process_event(qevent)
            else:
                await self._process_event(qevent, lease=lease)
        except asyncio.CancelledError as exc:
            error = exc
            await self._close_hook_run_after_error(
                qevent,
                lease=lease,
                original=exc,
                shield=True,
            )
            constants._LIFECYCLE_OUTCOME.set("interrupted")
            span.add_event(
                "turn.processing.interrupted",
                {"outcome": "interrupted"},
            )
        except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
            error = exc
            if not isinstance(exc, HookRunRecorderError):
                await self._close_hook_run_after_error(
                    qevent,
                    lease=lease,
                    original=exc,
                    shield=False,
                )
            span.add_event(
                "turn.processing.failed",
                {"outcome": "classified_failure", "error.class": type(exc).__name__},
            )
        finally:
            if not isinstance(error, Exception):
                # Success and cooperative cancellation clear the mark; a
                # FAILED delivery deliberately leaves it, because the
                # consumer's except branch has not read it yet.
                # ``CancelledError`` is a ``BaseException``, so it clears here
                # too: a cancelled turn is a shutdown, not a lost answer.
                self._terminal_reply_attempted.discard(qevent.event_id)
                self._factory_work_item_events.discard(qevent.event_id)
            outcome = constants._LIFECYCLE_OUTCOME.get()
            if outcome is None:
                # A normal early return is the already-terminal skip. An
                # exception is classified here rather than exported as the
                # operation_span default OK.
                outcome = "classified_failure" if error is not None else "done"
            if hasattr(span, "set_attribute"):
                span.set_attribute("outcome", outcome)
            if hasattr(span, "set_status"):
                span.set_status(
                    StatusCode.ERROR
                    if outcome in constants._ERROR_LIFECYCLE_OUTCOMES
                    else StatusCode.OK
                )
            span.add_event("turn.processing.completed", {"outcome": outcome})
            constants._LIFECYCLE_OUTCOME.reset(outcome_token)
            constants._TURN_AGENT.reset(agent_token)
            constants._LIFECYCLE_SPAN.reset(token)
            constants._HOOK_RUN_CARRY.reset(hook_token)
            constants._DELIVERY_LEASE.reset(lease_token)
    if error is not None:
        raise error


async def _process_event(
    self: Kernel, qevent: QueuedTurn, *, lease: DeliveryLease | None = None
) -> None:
    """Handle one queued turn to a terminal state (success or escalate).

    Returns normally once the event is terminally handled; the consumer then
    acks it. Raising leaves the entry pending for crash-recovery reclaim.

    A null ``reply_handle.placeholder`` is ACCEPTED here (ADR-0079). This
    method used to reject one outright, which left the contract and the
    runtime disagreeing: the schema had already been widened to
    ``placeholder: str | None`` for the channel port, so every triggered turn
    the wire permitted died on the first line of the kernel. The reply path
    now posts a message when there is none to edit and edits it thereafter.
    """
    routing._check_targetless_shape(qevent)
    targetless = routing._is_targetless(qevent)
    handle = qevent.reply_handle
    event_id = qevent.event_id
    thread_key = routing._thread_key_for(qevent)
    # The turn's route starts as the one the server minted onto the wire. A
    # bound route replaces it either when the turn resolves or, solely for a
    # bound-but-undeployed status reply, when the diagnostic binding lookup
    # supplies the server-controlled endpoint. Other pre-resolution paths
    # can only reach the handle's route, which is why it travels on the wire.
    route = constants._NO_EGRESS_ROUTE if targetless else routing._route_from_handle(qevent)

    # Acquire the per-thread order lock BEFORE any await, so concurrent
    # same-thread events queue in task-arrival order (asyncio.Lock is FIFO and
    # an uncontended acquire does not yield). It is released as soon as this
    # event's turn is started or steered (``_release_order`` in _attempt), so
    # streaming and steering are never blocked; holding it across the marker
    # checks is what keeps those awaits from reordering arrivals.
    entry = self._acquire_order_entry(thread_key)
    await entry.lock.acquire()
    release_state = {"done": False}

    def release_order() -> None:
        if not release_state["done"]:
            release_state["done"] = True
            entry.lock.release()
            self._release_order_entry(thread_key, entry)

    owned_work_item_id: uuid.UUID | None = None
    owned_token = constants._OWNED_WORK_ITEM.set(None)
    publication_token = constants._PUBLICATION_CONTEXT.set(None)
    progress_token = constants._TURN_PROGRESS.set(None)
    try:
        if await self._markers.is_terminal(event_id):
            # ``is_terminal``, not ``is_done``: a DONE outbox record proves
            # this turn finished just as well as the marker does, and it
            # outlives the marker by the retention window. Reading the marker
            # alone reran a completed turn after a >24h outage -- the startup
            # sweep emitted the record and cleared it, and the entry that was
            # never acked was then reclaimed with nothing left to refuse it.
            # Completion emit stays at-least-once; turn side effects stay
            # at-most-once for the whole outbox retention period.
            logger.info("event %s already done; skipping", event_id)
            # The skip holds no resolved route of its own and must not
            # invent one, so it makes no sink call -- except to hand off a
            # completion an earlier delivery durably owed and never
            # confirmed, which it re-emits from the STORED record.
            await self._reemit_pending_completion(event_id)
            return

        if qevent.source is TurnSource.CRON:
            if self._hook_runs is None or qevent.hook_run is None:
                logger.error(
                    "cron event %s has no hook run recorder or key; dropping",
                    event_id,
                )
                await self._complete(
                    qevent,
                    route,
                    "dropped",
                    telemetry_outcome="interrupted",
                    lease=lease,
                )
                return
            try:
                hook_state = await self._hook_runs.get(qevent.hook_run)
            except HookRunRecorderError as exc:
                if exc.code != "invalid_ref":
                    raise
                logger.error(
                    "cron event %s has an invalid hook run key; dropping",
                    event_id,
                )
                await self._complete(
                    qevent,
                    route,
                    "dropped",
                    telemetry_outcome="interrupted",
                    lease=lease,
                )
                return
            if hook_state is None:
                logger.error(
                    "cron event %s has no matching hook run row; dropping",
                    event_id,
                )
                await self._complete(
                    qevent,
                    route,
                    "dropped",
                    telemetry_outcome="interrupted",
                    lease=lease,
                )
                return
            # Renew the claim lease before the turn starts, so time spent
            # queued never lets the hook's next fire reclaim a live run.
            # A run reclaimed since the read above is terminal (#2931). The
            # margin covers a budget-cut sweep slice's interrupt and coverage
            # read before it renews again (#2878).
            if hook_state.outcome is not None or not await self._hook_runs.renew(
                qevent.hook_run,
                self._config.effective_hook_claim_lease_s + sweep.HOOK_LEASE_START_MARGIN_S,
            ):
                logger.info(
                    "cron event %s belongs to an already terminal hook run; dropping",
                    event_id,
                )
                await self._complete(
                    qevent,
                    route,
                    "dropped",
                    telemetry_outcome="interrupted",
                    lease=lease,
                )
                return
            hook_carry = constants._HOOK_RUN_CARRY.get()
            assert hook_carry is not None
            hook_carry.recorder = self._hook_runs
            hook_carry.ref = qevent.hook_run
            hook_carry.agent_id = hook_state.agent_id
            hook_carry.state = hook_state
            expiry = retry_expiry(event_id)
            hook_carry.retry_expires_at = expiry
            if expiry is not None and datetime.now(UTC) >= expiry:
                # A deferred slot's retry that waited in the stream past its
                # catch-up bound does not run late (#2929).
                logger.info("cron retry %s is past its catch-up bound; skipped", event_id)
                await self._complete(
                    qevent,
                    route,
                    "dropped",
                    telemetry_outcome="interrupted",
                    lease=lease,
                    hook_outcome="skipped",
                    hook_reason="deferred_expired",
                )
                return

        parsed_work_item = parse_work_item_event_id(event_id)
        if parsed_work_item is not None and parsed_work_item.kind in {"terminate"}:
            await self._terminate_work_item(qevent, parsed_work_item.request_id)
            return
        if parsed_work_item is not None and parsed_work_item.kind in {"execute"}:
            if self._work_items is None:
                logger.error(
                    "work-item execute %s has no dispatch client; dropping the wake",
                    event_id,
                )
                return
            if qevent.attachments:
                logger.error(
                    "work-item execute %s carried attachments; refusing",
                    event_id,
                )
                return
            assert parsed_work_item.generation is not None
            try:
                grant = await self._work_items.acquire(
                    parsed_work_item.request_id,
                    owner=self._config.consumer_name,
                    generation=parsed_work_item.generation,
                )
            except WorkItemConflict as exc:
                logger.info(
                    "work-item acquire refused for %s: %s",
                    event_id,
                    exc.code,
                )
                return
            self._work_item_runs[parsed_work_item.request_id] = WorkItemRun(
                client=self._work_items,
                request_id=parsed_work_item.request_id,
                owner=self._config.consumer_name,
                grant=grant,
                event_id=event_id,
                thread_key=thread_key,
                on_stop=self._stop_owned_work_item,
                on_stale=self._abandon_stale_work_item,
            )
            owned_work_item_id = parsed_work_item.request_id
        elif parsed_work_item is not None and parsed_work_item.is_ci_fix:
            # A CI fix round continues the SAME running request (#3097): adopt
            # it by conversation the way an approval continuation does, and
            # refuse the turn when the running request is not the one the
            # event names (a relabel replaced it) or the execution ended.
            try:
                adopted = await self._adopt_resumed_work_item(event_id, thread_key)
            except failures._FactoryExecutionEnded:
                self._factory_work_item_events.add(event_id)
                await self._markers.mark_done(event_id, marker_value="1")
                return
            if adopted != parsed_work_item.request_id:
                if adopted is not None:
                    self._work_item_runs.pop(adopted, None)
                logger.info(
                    "work-item CI continuation %s is stale: running request is %s",
                    event_id,
                    adopted,
                )
                await self._markers.mark_done(event_id, marker_value="1")
                return
            owned_work_item_id = adopted
        elif self._is_approval_resume(event_id):
            try:
                owned_work_item_id = await self._adopt_resumed_work_item(event_id, thread_key)
            except failures._FactoryExecutionEnded:
                self._factory_work_item_events.add(event_id)
                await self._markers.mark_done(event_id, marker_value="1")
                return
        constants._OWNED_WORK_ITEM.set(owned_work_item_id)

        # ADR-0168 decision 5: a turn addressed to an identity this worker
        # cannot speak as ends here, before a card, a claim or a model call.
        # Nothing can answer it: only the addressed bot may edit its own
        # placeholder, and a reply from any other bot is the defect.
        if not targetless and not self._is_factory_work_item_turn(event_id):
            assert handle is not None
            refusal = self._sink.undeliverable_reason(handle.kind, route)
            if refusal is not None:
                logger.error("dropping event %s without a reply: %s", event_id, refusal)
                await self._complete(
                    qevent,
                    route,
                    "dropped",
                    telemetry_outcome="interrupted",
                    lease=lease,
                    hook_outcome="failed",
                    hook_reason="reply_undeliverable",
                )
                return

        # If this is an approval resume, settle its live card before running
        # the continuation: expired (#419) or resolved (#1084). Best-effort,
        # and gated on the resume event id so an ordinary turn pays nothing.
        if self._is_approval_resume(qevent.event_id):
            qevent = await self._place_the_resumed_reply(qevent)
            with log.operation_span(
                "curie.approval.resume",
                kind=SpanKind.INTERNAL,
                attributes={
                    "service.name": "curie-worker",
                    "operation": "resume",
                },
            ) as approval_span:
                await self._finalize_settled_card(qevent, route)
                approval_span.add_event("approval.resumed", {"outcome": "resumed"})
            log.record_metric(
                "curie.approval.lifecycle",
                attributes={
                    "service.name": "curie-worker",
                    "operation": "resume",
                    "outcome": "resumed",
                },
            )
        elif not targetless:
            # A targetless turn owns no card: it never paused for approval.
            await self._finalize_settled_card(qevent, route)

        # Crash-safety: a prior attempt executed a side effect but never
        # reached done (worker died mid-run). Do not auto-retry the action.
        if await self._markers.saw_side_effect(event_id):
            # This path is NOT silent: it is the one message a human most
            # needs to see. It runs before binding resolution, so it emits
            # over the server-minted handle's route, verbatim -- never a
            # lookup it cannot reach and never a default standing in for a
            # missing adapter.
            #
            # And it is a DURABLE TERMINAL OUTCOME, so it completes through
            # the same ordering every other terminal path uses (driver
            # adjudication, superseding the earlier "suppress the completion
            # here" wording). Marking done without a completion is only
            # survivable on an edit-in-place channel: a BUFFERED adapter
            # accumulates the reply and flushes on ``turn.completed``, so a
            # suppressed completion writes the escalation, marks the turn
            # done, and never delivers it -- silently, on exactly the channel
            # with nobody watching a thread. The handle-derived route is what
            # the record stores, so a sweeper re-emits over the same
            # transport the escalation text went to.
            await self._escalate(
                qevent,
                route,
                "A prior attempt started an action before the worker restarted; "
                "not retrying automatically. Flagging for a human.",
                failure_class="prior-side-effect",
            )
            await self._complete(
                qevent,
                route,
                "escalated",
                telemetry_outcome="classified_failure",
                lease=lease,
                hook_outcome="failed",
                hook_reason="prior_side_effect",
            )
            return

        # ADR-0168 decision 6: a turn one of this installation's own
        # identities wrote counts against the sibling limit, and past it
        # ends here, before a binding lookup, a shimmer, a claim or a model
        # call. A job never counts: siblings speak through chat.
        if self._sibling_limit is not None and not targetless and not qevent.source.is_job:
            assert handle is not None
            limited = await self._sibling_limit.check(
                kind=handle.kind,
                adapter=handle.adapter,
                author=qevent.author,
                session_key=thread_key,
            )
            if limited is not None:
                await self._drop_sibling_turn(qevent, route, limited, lease=lease)
                return

        # Deployment-to-runtime binding: resolve which agent/version this
        # channel runs, and refuse a killed agent. An unmapped channel is a
        # polite drop, not a crash.
        boot_env: dict[str, str] | None = None
        agent_id: uuid.UUID | None = None
        agent_name: str | None = None
        runner_resources: dict[str, Any] | None = None
        workspace_deployment_id: uuid.UUID | None = None
        nav: NavAffordance | None = None
        packs: BehaviorPacks | None = None
        approval_routes: dict[str, Any] | None = None
        # ADR-0188: a targetless turn names no binding, so it never gets a
        # memory write credential.
        memory_grant: memory.TurnMemoryGrant | None = None
        # ADR 0100: the channel read grant. A targetless hook turn gets one
        # too, with no default channel, so each read names its channel; an
        # eval isolated turn never does.
        channel_read_grant: channel_read.TurnChannelReadGrant | None = None
        if targetless:
            # Routed by the hook run's agent, never a channel binding (#2963).
            # The id is the one the hook row lookup parsed and matched, not
            # the raw wire string. Each refusal of a valid open run closes
            # its row, since no reply exists to carry the reason.
            hook_carry = constants._HOOK_RUN_CARRY.get()
            assert hook_carry is not None and hook_carry.agent_id is not None
            binding = self._binding
            resolved = (
                await binding.resolve_agent(hook_carry.agent_id) if binding is not None else None
            )
            if binding is None or resolved is None:
                logger.error(
                    "targetless cron event %s: agent %s has no active deployment; dropping",
                    event_id,
                    hook_carry.agent_id,
                )
                await self._complete(
                    qevent,
                    route,
                    "dropped",
                    telemetry_outcome="interrupted",
                    lease=lease,
                    hook_outcome="failed",
                    hook_reason="deployment_missing",
                )
                return
            constants._TURN_AGENT.set(resolved.agent_name)
            if self._killswitch is not None and await self._killswitch.is_killed(resolved.agent_id):
                await self._complete(
                    qevent,
                    route,
                    "dropped",
                    telemetry_outcome="interrupted",
                    lease=lease,
                    hook_outcome="blocked",
                    hook_reason="agent_killed",
                )
                return
            agent_id = resolved.agent_id
            agent_name = resolved.agent_name
            reader = getattr(self._binding, "runner_resources_for", None)
            runner_resources = await reader(agent_id) if reader is not None else None
            # No kind/address: there is no binding to scope the state
            # namespace to. No approval grant, resumed kind or decision
            # either: a targetless turn is never a resume.
            boot_env = binding.boot_env(
                resolved,
                thread_key,
                token_ttl_s=self._runner.turn_deadline_s(claim._remaining_budget(lease)),
            )
            workspace_deployment_id = resolved.deployment_id
            if resolved.deployment_id is not None:
                channel_read_grant = channel_read.TurnChannelReadGrant(
                    agent_id=resolved.agent_id,
                    deployment_id=resolved.deployment_id,
                    default=None,
                    thread_key=thread_key,
                    bundle_ref=resolved.bundle_ref,
                )
            packs = binding.packs_for(resolved)
            approval_routes = resolved.approval_routes
        elif self._binding is not None:
            assert handle is not None
            # The routing key is the TRIPLE (ADR-0168 decision 3): the queue
            # wire carries a required `kind` and, for Slack, `adapter` names
            # the bot identity the turn was addressed to. Never the address
            # alone, and never the pair alone -- one address can be bound
            # under two kinds, and one pair can answer to only one identity
            # at a time, so dropping either would answer with somebody
            # else's route.
            # @spec WORKER-CANARY-1 WORKER-CANARY-2: the relay's identity
            # chooses the binding; its adapter stays on the reply route.
            try:
                resolved = await self._binding.resolve(
                    handle.kind, binding_adapter_for_handle(handle), handle.channel
                )
            except AmbiguousRoute as exc:
                await self._drop_ambiguous_route(qevent, route, exc, lease=lease)
                return
            if resolved is None:
                # Binding doubles predate the diagnostic lookup; keep a miss
                # on those doubles on the established polite-drop path.
                undeployed_lookup = getattr(self._binding, "undeployed_binding", None)
                try:
                    undeployed = (
                        # @spec WORKER-CANARY-2 WORKER-CANARY-4: use the
                        # same binding selector for the diagnostic path.
                        await undeployed_lookup(
                            handle.kind, binding_adapter_for_handle(handle), handle.channel
                        )
                        if undeployed_lookup is not None
                        else None
                    )
                except AmbiguousRoute as exc:
                    await self._drop_ambiguous_route(qevent, route, exc, lease=lease)
                    return
                if undeployed is not None:
                    # A bound non-Slack route may carry the only endpoint the
                    # platform can use to deliver this status reply.
                    constants._TURN_AGENT.set(undeployed.agent_name)
                    route = TargetRoute(
                        endpoint=undeployed.endpoint or handle.endpoint,
                        adapter=routing._bound_egress_adapter(undeployed.adapter, handle.adapter),
                    )
                    logger.warning(
                        "undeployed agent turn dropped for agent=%s route=%s:%s",
                        undeployed.agent_name,
                        handle.kind,
                        handle.channel,
                    )
                    await self._drop_with_message(
                        qevent,
                        route,
                        "This agent does not have an active deployment yet. "
                        "Deploy it, then try again.",
                        lease=lease,
                    )
                    return
                # Name BOTH halves: since the kind routes, a kind typo is a
                # newly reachable drop, and a message naming only the address
                # sends an operator hunting a binding that is right there.
                await self._drop_with_message(
                    qevent,
                    route,
                    f"No agent is configured for this {handle.kind} address {handle.channel} yet.",
                    lease=lease,
                )
                return
            # The normal route source: once a turn has resolved, its egress
            # target is the binding row's,
            # because endpoints are server-controlled (D4.1). A row that
            # names none leaves the server-minted handle standing -- the
            # dispatcher and CLI bind no endpoint of their own.
            route = TargetRoute(
                endpoint=resolved.endpoint or handle.endpoint,
                adapter=routing._bound_egress_adapter(resolved.adapter, handle.adapter),
            )
            constants._TURN_AGENT.set(getattr(resolved, "agent_name", None))
            hook_carry = constants._HOOK_RUN_CARRY.get()
            if (
                qevent.source is TurnSource.CRON
                and hook_carry is not None
                and hook_carry.agent_id != resolved.agent_id
            ):
                logger.error(
                    "cron event %s resolved to a different agent than its hook run key; dropping",
                    event_id,
                )
                await self._complete(
                    qevent,
                    route,
                    "dropped",
                    telemetry_outcome="interrupted",
                    lease=lease,
                )
                return
            if self._killswitch is not None and await self._killswitch.is_killed(resolved.agent_id):
                await self._drop_with_message(
                    qevent,
                    route,
                    "This agent is paused by an operator. Try again once it resumes.",
                    lease=lease,
                )
                return
            agent_id = resolved.agent_id
            agent_name = getattr(resolved, "agent_name", None)
            reader = getattr(self._binding, "runner_resources_for", None)
            runner_resources = await reader(agent_id) if reader is not None else None
            # Memory writes (#1461) are read apart from resolution for the
            # same schema reason as runner_resources; boot_env reads them
            # off the resolved deployment. hasattr: binding doubles may not
            # carry the method. A failed read (a DB error, or a schema from
            # before migration 0068) never fails the turn: it runs with
            # memory writes off and a warning, the safe direction.
            memory_writes = False
            if hasattr(self._binding, "memory_writes_for"):
                try:
                    memory_writes = await self._binding.memory_writes_for(agent_id)
                except Exception as exc:  # noqa: BLE001 - degrade to off
                    logger.warning(
                        "memory writes read failed agent=%s error_class=%s;"
                        " running with memory writes off",
                        agent_id,
                        type(exc).__name__,
                    )
            if memory_writes:
                resolved = resolved.model_copy(update={"memory_writes": True})
            # The scoped key, not the bare conversation id: this mints the
            # sandbox's history ref and session id, so two channels sharing
            # a conversation id must not rehydrate one another's transcript.
            boot_env_kwargs: dict[str, Any] = {
                "kind": handle.kind,
                "address": handle.channel,
                "token_ttl_s": self._runner.turn_deadline_s(claim._remaining_budget(lease)),
            }
            # The internal thread key is channel-scoped, so it no longer
            # starts with the eval marker carried by conversation_id.
            # Preserve that isolation intent explicitly without changing
            # the stable sandbox/history identity (#1909 + ADR-0096).
            if qevent.conversation_id.startswith("eval:"):
                boot_env_kwargs["isolate_memory"] = True
            else:
                # ADR-0188: an eval-isolated turn carries no memory, so no
                # write credential either.
                memory_grant = memory.TurnMemoryGrant(
                    resolved=resolved,
                    kind=handle.kind,
                    address=handle.channel,
                    thread_key=thread_key,
                )
                deployment_id = getattr(resolved, "deployment_id", None)
                if isinstance(deployment_id, uuid.UUID):
                    channel_read_grant = channel_read.TurnChannelReadGrant(
                        agent_id=resolved.agent_id,
                        deployment_id=deployment_id,
                        default=(handle.kind, handle.channel),
                        thread_key=thread_key,
                        bundle_ref=getattr(resolved, "bundle_ref", None),
                    )
            caller_for_boot = (
                self._work_item_runs.get(owned_work_item_id)
                if owned_work_item_id is not None
                else None
            )
            if caller_for_boot is not None:
                if caller_for_boot.execution_deadline is not None:
                    caller_ceiling = int(caller_for_boot.execution_deadline.timestamp())
                else:
                    seconds = DEFAULT_EXECUTION_DEADLINE_SECONDS
                    reader = getattr(self._binding, "execution_deadline_seconds_for", None)
                    if reader is not None and agent_id is not None:
                        try:
                            seconds = int(await reader(agent_id))
                        except Exception:  # noqa: BLE001 - a missing column still boots
                            logger.warning(
                                "execution deadline read failed agent=%s; using the default",
                                agent_id,
                            )
                            seconds = DEFAULT_EXECUTION_DEADLINE_SECONDS
                    caller_ceiling = int(clock.time.time()) + seconds
                boot_env_kwargs["caller_run"] = str(caller_for_boot.request_id)
                boot_env_kwargs["caller_work_item"] = str(caller_for_boot.work_item_id)
                boot_env_kwargs["caller_exp_ceiling"] = caller_ceiling
            boot_env = self._binding.boot_env(
                resolved,
                thread_key,
                **boot_env_kwargs,
            )
            # The deployment id is the server-side authority used to select
            # and redeem a repository at initial claim time.  The legacy
            # per-deployment workspace_enabled bit is deliberately not a
            # runtime coding gate: the worker-wide coordinator switch is the
            # operational kill switch, while a missing deployment id simply
            # leaves this turn on the generic claim path.
            workspace_deployment_id = getattr(resolved, "deployment_id", None)
            # One-shot post-approval allowance (#430, ADR-0035): when THIS turn is the
            # resume of a genuinely-approved permission-gate approval, deliver a single
            # gated-tool grant so the approved action completes once; the gate re-arms
            # on the next claim. Server-side and tool-name-scoped; never minted by the
            # sandbox. getattr: binding doubles may not carry the method, like the
            # approval_routes probe below. See docs/interfaces/approval/INTERFACE.md.
            grant_fn = getattr(self._binding, "approval_grant_tool", None)
            # @spec WORKER-TOOL-ACCESS-4: never a grant for a restricted turn.
            grant_tool = (
                await grant_fn(qevent.event_id, resolved.agent_id)
                if grant_fn is not None and qevent.tool_access is None
                else None
            )
            if grant_tool:
                boot_env[GRANT_TOOL_ENV] = grant_tool
                grant_arguments = await self._binding.approval_grant_arguments(
                    qevent.event_id, resolved.agent_id
                )
                if grant_arguments is not None:
                    boot_env[GRANT_ARGUMENTS_ENV] = json.dumps(
                        grant_arguments, sort_keys=True, separators=(",", ":")
                    )
                # A connector live name is mcp__<server>__<tool>. Plugin
                # tools and a resume with no arguments mint no connector grant.
                connector_grant = claim._connector_tool_grant(
                    grant_tool,
                    grant_arguments,
                    agent=agent_name if isinstance(agent_name, str) else None,
                    signing_key_text=self._config.connector_caller_signing_key,
                )
                if connector_grant is not None:
                    boot_env["CURIE_CONNECTOR_TOOL_GRANT"] = connector_grant
            # A factory execution may report its phases for the live status
            # card (#3077). The token is bound to this request and to the
            # work_item.progress scope only; no other turn carries it.
            if owned_work_item_id is not None and self._config.api_key:
                base = self._config.runner_facing_api_base_url.rstrip("/")
                boot_env[PROGRESS_URL_ENV] = f"{base}/v1/work-item-progress/{owned_work_item_id}"
                boot_env[PROGRESS_TOKEN_ENV] = sandbox_token.mint(
                    self._config.api_key,
                    agent=str(owned_work_item_id),
                    scope="work_item.progress",
                    exp=int(clock.time.time()) + SANDBOX_TOKEN_TTL_SECONDS,
                )
            # The bundle reads its issue through the platform (ADR 0187):
            # the API mints a capability naming this execution and its
            # WorkItem's issue, and the runner mounts get_issue only when
            # it is present. A refused or failed mint leaves the tool off
            # and never stops the boot; the bundle states the gap.
            if owned_work_item_id is not None and self._work_items is not None:
                try:
                    issue, capability = await self._work_items.issue_read_context(
                        owned_work_item_id
                    )
                except (WorkItemConflict, WorkItemTransportError) as exc:
                    logger.warning(
                        "issue read capability unavailable for %s: %s",
                        owned_work_item_id,
                        type(exc).__name__,
                    )
                else:
                    base = self._config.runner_facing_api_base_url.rstrip("/")
                    boot_env[ISSUE_READ_URL_ENV] = f"{base}/work-items/issue-read"
                    boot_env[ISSUE_READ_TOKEN_ENV] = capability
                    logger.info(
                        "issue read capability bound for %s to %s",
                        owned_work_item_id,
                        issue,
                    )
            # Decision A2 marker (#544): an authority-free FACT carrying the
            # resumed approval's gate kind (the actual gate_kind column value,
            # e.g. 'policy' or 'permission'). After the approved-only gate in
            # approval_resumed_kind, only a genuinely approved approval injects
            # it at all. The runner's observe-only turn-end reconciliation acts
            # only on 'policy' (warning if the approved business action never
            # ran); a 'permission' marker is inert there. Grants nothing
            # (contrast the grant above); getattr-tolerant of binding doubles
            # that do not carry the method, like the grant.
            resumed_kind_fn = getattr(self._binding, "approval_resumed_kind", None)
            resumed_kind = (
                await resumed_kind_fn(qevent.event_id, resolved.agent_id)
                if resumed_kind_fn is not None
                else None
            )
            if resumed_kind:
                boot_env[RESUMED_KIND_ENV] = resumed_kind
            # ADR-0076 Stone 3 (#889, epic #512): the resolved terminal
            # decision (approved/rejected/expired), authority-free like the
            # marker above, so the runner can stamp it on the turn's OTel
            # span. Reports all three terminal statuses, not just approved
            # (contrast resumed_kind), closing the "did an approval get
            # requested" gap ADR-0038 named open. getattr-tolerant of
            # binding doubles that do not carry the method, like the two above.
            decision_fn = getattr(self._binding, "approval_decision", None)
            decision = (
                await decision_fn(qevent.event_id, resolved.agent_id)
                if decision_fn is not None
                else None
            )
            if decision:
                boot_env[DECISION_ENV] = decision
            # Resolve the agent's packs once here (a pure parse, no I/O) and
            # reuse: the nav pack is threaded to the final render, the same
            # packs feed the shimmer below.
            packs = self._binding.packs_for(resolved)
            # getattr: binding doubles (tests, alternate resolvers) may not
            # carry the routes attribute; absent means unbound (#247).
            approval_routes = getattr(resolved, "approval_routes", None)
            # Mapped HERE, at the worker boundary: ``NavPack`` is
            # worker-local and cannot cross into ``channel_protocol``
            # (finding 16), so the wire carries a ``NavAffordance`` and a
            # disabled pack carries nothing at all.
            nav = routing._nav_affordance(packs.nav)
            # Raise the shimmer (#1312). Deliberately placed HERE, after the
            # binding resolved and before ``_attempt`` claims a sandbox: a
            # channel we are about to refuse never gets a caption that would
            # flicker straight back off, and a cold claim (up to
            # claim_timeout) is spent with the shimmer already lit rather than
            # in silence. Best-effort and outside the concurrency-critical
            # section, like the clear below.
            if self._config.shimmer:
                await self._set_shimmer(qevent, route, packs)

        # Work-item turn budget (#3071). A factory execution runs far more
        # tool turns than a chat reply, so ONLY a work-item delivery raises
        # the runner's turn cap; every other delivery carries no
        # CURIE_MAX_TURNS and keeps the runner default. Placed after every
        # boot-env build site so both the targetless and the bound paths
        # carry it.
        if owned_work_item_id is not None and boot_env is not None:
            boot_env[MAX_TURNS_ENV] = str(self._config.work_item_max_turns)

        # ADR 0130: the tool and its prompt are part of the sandbox's model
        # surface, so only an eligible human Slack sandbox gets the boot
        # marker. _boots_differently fences adoption across this boundary.
        factory_work_item = self._is_factory_work_item_turn(event_id)
        if (
            self._progress is not None
            and self._config.api_key
            and progress_eligible(qevent, factory_work_item=factory_work_item)
        ):
            if boot_env is None:
                boot_env = {}
            boot_env[ELIGIBILITY_ENV] = "1"

        # ADR-0131 reclaim preflight. A delivery that has CHANGED HANDS --
        # generation > 1, a distributed-state fact and never a sniff of the
        # message text, so kernel rule 3 stands -- may not simply route: the
        # ordinary path would STEER into a retained live turn, which is right
        # for a follow-up and wrong for a retry of the same event. Placed
        # deliberately AFTER the side-effect check above (rule 1 of the
        # preflight is that check, and a marker must forbid replay before any
        # interrupt-and-rehydrate is contemplated) and BEFORE the first
        # attempt.
        if claim._is_fenced(lease) and lease is not None and lease.generation > 1:
            wait_scope = current_wait()
            if wait_scope is None:
                await self._preflight_reclaimed_delivery(thread_key, lease)
            else:
                record = await wait_scope[0].get(wait_scope[1])
                if record is not None and record.grant_epoch is not None:
                    await self._quiesce_capacity_epoch(thread_key, record.grant_epoch)

        # Retry carry for the inferred repository announcement (#2659); see
        # _WorkspaceInferenceCarry. Local to this delivery, never kernel state,
        # so it cannot leak into another thread's turn. A reclaimed redelivery
        # starts fresh.
        workspace_inference = workspace._WorkspaceInferenceCarry()
        # ADR 0130: name the progress chain a person's turn reports on. The
        # record is opened only when a turn's stream is consumed, so an
        # event that only steers a live turn opens none.
        if not targetless and self._progress is not None:
            constants._TURN_PROGRESS.set(
                await plan_turn_progress(
                    self._progress,
                    qevent,
                    thread_key,
                    factory_work_item=factory_work_item,
                    resume=self._is_approval_resume(event_id),
                )
            )
        termination_detail: str | None = None
        attempt = 0
        capacity_refusals = 0
        capacity_wait_run: WorkItemRun | None = None
        while True:
            attempt += 1
            hook_carry = constants._HOOK_RUN_CARRY.get()
            if hook_carry is not None:
                hook_carry.this_attempt_started = False
            # The delivery's overall deadline gates every attempt, and the
            # attempts CONSUME it: a retry never restarts it.
            if lease is not None or capacity_wait_run is not None:
                if lease is not None and lease.lost.is_set():
                    # Fenced out between attempts. Start nothing: a
                    # replacement holds this delivery and is entitled to run
                    # it. Returning without completing leaves the entry
                    # pending, and the consumer's pre-ACK check keeps this
                    # owner from acking it away.
                    logger.warning(
                        "delivery lease lost before attempt %d for event %s; "
                        "starting no attempt and settling nothing",
                        attempt,
                        event_id,
                    )
                    return
                remaining = claim._remaining_budget(lease)
                if capacity_wait_run is not None:
                    remaining = capacity_wait_run.bound_remaining_s(remaining)
                if remaining is not None and remaining <= constants._MIN_ATTEMPT_BUDGET_S:
                    if (
                        sweep.parse_continuation(event_id) is not None
                        and qevent.source is TurnSource.CRON
                        and hook_carry is not None
                        and not hook_carry.any_attempt_started
                    ):
                        # A sweep continuation that spent its budget waiting
                        # on the previous slice to wind down ran nothing of
                        # its own: it stops with the coverage notice alone,
                        # not a delivery-deadline escalation (#2878).
                        logger.info(
                            "sweep continuation %s spent its budget before it started",
                            event_id,
                        )
                        await self._complete(
                            qevent,
                            route,
                            "dropped",
                            telemetry_outcome="deadline_halted",
                            lease=lease,
                            hook_outcome="failed",
                            hook_reason="turn_error",
                        )
                        return
                    # DISTINCT from the model-spend ``budget-exceeded``
                    # classification: this is the wall-clock delivery
                    # deadline, and conflating the two would make both
                    # unreadable in telemetry.
                    await self._escalate(
                        qevent,
                        route,
                        "The run exceeded its delivery deadline after "
                        f"{attempt - 1} attempt(s) and was not restarted. "
                        "Flagging for a human.",
                        failure_class="delivery-deadline",
                    )
                    await self._complete(
                        qevent,
                        route,
                        "escalated",
                        telemetry_outcome="deadline_halted",
                        lease=lease,
                        hook_outcome=hooks._hook_failure_outcome(),
                        hook_reason=(
                            "turn_error" if hooks._hook_failure_outcome() == "failed" else None
                        ),
                    )
                    return
            try:
                outcome = await self._attempt(
                    qevent,
                    route,
                    release_order,
                    boot_env,
                    agent_id,
                    nav,
                    packs,
                    workspace_deployment_id,
                    agent_name,
                    runner_resources=runner_resources,
                    remaining_s=claim._remaining_budget(lease),
                    pressure_retried=False,
                    workspace_inference=workspace_inference,
                    memory_grant=memory_grant,
                    channel_read_grant=channel_read_grant,
                )
            except failures._WorkItemDeferred:
                return
            except failures.SweepClaimGone as exc:
                # ADR-0160: a continuation never claims, resumes or hands off
                # a sandbox. The sweep stops; ``_complete`` posts the notice.
                logger.info("sweep continuation %s stopped: %s", event_id, exc)
                await self._complete(
                    qevent,
                    route,
                    "dropped",
                    telemetry_outcome="interrupted",
                    lease=lease,
                    hook_outcome="failed",
                    hook_reason="turn_error",
                )
                return
            except WorkItemStartRefused as exc:
                logger.info(
                    "work-item start refused for %s: %s",
                    event_id,
                    exc.code,
                )
                if exc.code in constants._WORK_ITEM_TERMINAL_START_REFUSALS:
                    # #3208: the request settled before a turn opened, so
                    # this delivery is the only one that can release the
                    # claim it made. A request cancelled from ``waiting``
                    # gets no terminate wake and carries no teardown flag,
                    # so without this the claim holds quota until the
                    # route TTL lapses. Marking the run finished hands the
                    # release to the existing finally block.
                    # ``not_dispatchable`` is deliberately excluded: it can
                    # mean a lapsed acquire lease, where a replacement
                    # re-acquires the same generation and adopts this
                    # thread's route, so the claim must stay standing.
                    run = (
                        self._work_item_runs.get(owned_work_item_id)
                        if owned_work_item_id is not None
                        else None
                    )
                    if run is not None:
                        run.finished = True
                return
            except failures.ThreadBusyError as busy:
                run = (
                    self._work_item_runs.get(owned_work_item_id)
                    if owned_work_item_id is not None
                    else None
                )
                if run is not None and not run.started:
                    try:
                        await run.defer("thread_busy", capacity=False)
                    except WorkItemConflict as exc:
                        logger.info(
                            "work-item thread_busy defer refused for %s: %s",
                            event_id,
                            exc.code,
                        )
                    return
                if (
                    qevent.source is TurnSource.CRON
                    and not targetless
                    and sweep.parse_continuation(event_id) is not None
                ):
                    if isinstance(busy, failures.HookPaused):
                        # Paused (or the run closed) between slices: the
                        # sweep stops and reports, never defers (#2878).
                        await self._complete(
                            qevent,
                            route,
                            "dropped",
                            telemetry_outcome="interrupted",
                            lease=lease,
                            hook_outcome="blocked",
                            hook_reason="hook_paused",
                        )
                        return
                    if isinstance(busy, failures.LiveSessionBusy):
                        # The previous slice's interrupted turn is still
                        # winding down in this same session. Deferring would
                        # restart the sweep later in a fresh session, so wait
                        # inside this delivery's own budget; the loop top
                        # stops it once that is spent. The order lock stays
                        # held: only this hook's deliveries reach its thread.
                        backoff_s = min(self._backoff(attempt), sweep.MAX_BUSY_PROBE_INTERVAL_S)
                        if lease is not None:
                            backoff_s = min(backoff_s, max(0.0, lease.remaining_s()))
                        elif attempt >= self._config.max_attempts:
                            await self._complete(
                                qevent,
                                route,
                                "dropped",
                                telemetry_outcome="interrupted",
                                lease=lease,
                                hook_outcome="failed",
                                hook_reason="turn_error",
                            )
                            return
                        logger.info(
                            "sweep continuation %s waiting for the previous slice to wind down",
                            event_id,
                        )
                        await asyncio.sleep(backoff_s)
                        continue
                if qevent.source is TurnSource.CRON and (
                    isinstance(busy, failures.HookPaused)
                    or (
                        not targetless
                        and isinstance(busy, (failures.LiveSessionBusy, failures.CatchUpExpired))
                    )
                ):
                    # ADR-0099 Concurrency and idle (#2929): the busy read ran
                    # under the per-thread lock. Record the fire deferred and
                    # settle this delivery; the scheduler reopens the slot on
                    # a later tick or ages it out, so stream reclaim never
                    # holds a cron turn. A targetless hook is never deferred.
                    # A retry past its catch-up bound, busy or not, is
                    # skipped rather than deferred again.
                    expiry = retry_expiry(event_id)
                    expired = isinstance(busy, failures.CatchUpExpired) or (
                        expiry is not None and datetime.now(UTC) >= expiry
                    )
                    busy_outcome: HookRunOutcome = "skipped" if expired else "deferred"
                    busy_reason: HookRunReason = (
                        "deferred_expired"
                        if expiry is not None and datetime.now(UTC) >= expiry
                        else "catch_up_expired"
                        if expired
                        else "hook_paused"
                        if isinstance(busy, failures.HookPaused)
                        else "live_session"
                    )
                    logger.info(
                        "cron event %s met a live session or its bound; %s",
                        event_id,
                        "skipped" if expired else "deferred",
                    )
                    await self._complete(
                        qevent,
                        route,
                        "dropped",
                        telemetry_outcome="interrupted",
                        lease=lease,
                        hook_outcome=busy_outcome,
                        hook_reason=busy_reason,
                    )
                    return
                raise

            if outcome.status is SessionStatus.AWAITING_APPROVAL and targetless:
                # A resume needs a reply route this turn does not carry
                # (#2963), so a gate on a targetless turn is a failed run,
                # never a pause and never a grant. The gated tool never ran.
                await self._complete(
                    qevent,
                    route,
                    "escalated",
                    telemetry_outcome="classified_failure",
                    lease=lease,
                    hook_outcome="failed",
                    hook_reason="approval_gate_targetless",
                )
                return

            if outcome.status is SessionStatus.AWAITING_APPROVAL and qevent.tool_access is not None:
                # @spec WORKER-TOOL-ACCESS-4: a restricted turn may not raise
                # a card, whatever its runner reported. No record, no card,
                # no suspend: a failed turn.
                await self._escalate(
                    qevent,
                    route,
                    constants._READ_ONLY_APPROVAL_REFUSAL,
                    failure_class="tool-access-violation",
                )
                await self._complete(
                    qevent,
                    route,
                    "escalated",
                    telemetry_outcome="classified_failure",
                    lease=lease,
                    hook_outcome=hooks._hook_failure_outcome(),
                    hook_reason=(
                        "turn_error" if hooks._hook_failure_outcome() == "failed" else None
                    ),
                )
                return

            if outcome.status is SessionStatus.AWAITING_APPROVAL:
                # A gate fired (ADR-0010): persist the durable record, then
                # suspend the session until a human resolves it. The event
                # is done -- the resolution arrives as its own queued turn.
                pause = await self._pause_for_approval(
                    qevent,
                    route,
                    outcome,
                    agent_id,
                    approval_routes,
                    deployment_id=workspace_deployment_id,
                )
                approval_created = pause.created
                if not approval_created:
                    run = self._run_for_event(qevent.event_id)
                    if run is not None:
                        try:
                            await self._finish_or_settle(
                                run,
                                outcome="failed",
                                cause="approval_create_failed",
                                detail=pause.failure_detail,
                            )
                        except WorkItemConflict as exc:
                            # work_item_cancelled means what it does in
                            # completion._complete (#3208): the request is settled as
                            # cancelled and accepts no finish (#4191).
                            if exc.code not in {"publication_pending", "work_item_cancelled"}:
                                raise
                            run.finished = True
                await self._complete(
                    qevent,
                    route,
                    "awaiting-approval" if approval_created else "escalated",
                    telemetry_outcome=(
                        "awaiting_approval" if approval_created else "classified_failure"
                    ),
                    lease=lease,
                    hook_outcome=hooks._hook_success_outcome(),
                    hook_reason=(
                        "turn_error" if hooks._hook_success_outcome() == "failed" else None
                    ),
                )
                return

            if outcome.terminal_ok:
                await self._complete(
                    qevent,
                    route,
                    "delivered",
                    telemetry_outcome=(
                        "classified_failure"
                        if outcome.start_failed
                        else "idle"
                        if outcome.status is SessionStatus.IDLE_AWAITING_INPUT
                        else "done"
                    ),
                    lease=lease,
                    hook_outcome=hooks._hook_success_outcome(),
                    hook_reason=(
                        "turn_error" if hooks._hook_success_outcome() == "failed" else None
                    ),
                    turn=outcome,
                )
                return

            # ADR-0160: a sweep slice cut by the delivery budget is a slice
            # boundary, checked before the side-effect halt because every real
            # sweep writes memory or runs a tool. With no checkpoint showing
            # progress it falls through to the handling below unchanged.
            if (
                qevent.source is TurnSource.CRON
                and not targetless
                and self._sweep is not None
                and lease is not None
                and outcome.classification in sweep.BUDGET_CUT_CLASSIFICATIONS
                and lease.remaining_s() <= constants._MIN_ATTEMPT_BUDGET_S
                and await self._continue_sweep(qevent, route, lease, outcome)
            ):
                return

            if outcome.saw_side_effect:
                token = failures._display_error_classification(outcome.classification)
                await self._escalate(
                    qevent,
                    route,
                    failures._escalation_text(
                        qevent,
                        lead=failures._with_guidance(
                            f"The run hit an error ({token}) after starting an action; "
                            "not retrying automatically.",
                            token,
                            delivered_max_turns=(boot_env or {}).get(MAX_TURNS_ENV),
                        ),
                        detail=outcome.error_message,
                    ),
                    failure_class=token,
                )
                await self._complete(
                    qevent,
                    route,
                    "escalated",
                    telemetry_outcome="side_effect_halted",
                    lease=lease,
                    hook_outcome=hooks._hook_failure_outcome(),
                    hook_reason=(
                        "turn_error" if hooks._hook_failure_outcome() == "failed" else None
                    ),
                    turn=outcome,
                )
                return

            retryable = outcome.classification in constants.RETRYABLE_CLASSIFICATIONS
            capacity_continuation = None
            if outcome.classification == "sandbox-capacity" and (
                (parsed_work_item is not None and parsed_work_item.is_ci_fix)
                or self._is_approval_resume(event_id)
            ):
                capacity_continuation = self._run_for_event(event_id)
            if capacity_continuation is not None:
                # The execution already started, so SQL defer cannot hold
                # this continuation. Waiting for quota consumes its delivery
                # and execution deadlines, never a runner attempt (#4275).
                capacity_wait_run = capacity_continuation
                capacity_refusals += 1
                attempt -= 1
            if outcome.classification == "sandbox-terminated":
                termination_detail = outcome.error_message
            if (
                qevent.source is TurnSource.CRON
                and retryable
                and lease is not None
                and lease.remaining_s() <= constants._MIN_ATTEMPT_BUDGET_S
            ):
                await self._escalate(
                    qevent,
                    route,
                    "The run exceeded its delivery deadline after "
                    f"{attempt} attempt(s) and was not restarted. "
                    "Flagging for a human.",
                    failure_class="delivery-deadline",
                )
                await self._complete(
                    qevent,
                    route,
                    "escalated",
                    telemetry_outcome="deadline_halted",
                    lease=lease,
                    hook_outcome=hooks._hook_failure_outcome(),
                    hook_reason=(
                        "turn_error" if hooks._hook_failure_outcome() == "failed" else None
                    ),
                )
                return
            retryable = retryable and qevent.source is not TurnSource.CRON
            if not retryable or attempt >= self._config.max_attempts:
                if (
                    outcome.classification in {"runner-error", "sandbox-capacity"}
                    and termination_detail is not None
                ):
                    # Keep the final attempt's classification truthful while
                    # carrying the earlier confirmed pod cause into both
                    # terminal surfaces.
                    outcome.error_message = (f"Earlier attempt: {termination_detail}")[
                        : constants._ESCALATION_DETAIL_MAX
                    ]
                token = failures._display_error_classification(outcome.classification)
                await self._escalate(
                    qevent,
                    route,
                    failures._escalation_text(
                        qevent,
                        lead=failures._with_guidance(
                            f"The run failed ({token}) after {attempt} attempt(s).",
                            token,
                            delivered_max_turns=(boot_env or {}).get(MAX_TURNS_ENV),
                        ),
                        detail=outcome.error_message,
                    ),
                    failure_class=token,
                )
                await self._complete(
                    qevent,
                    route,
                    "escalated",
                    telemetry_outcome=(
                        "budget_halted"
                        if outcome.classification == "budget-exceeded"
                        else "classified_failure"
                    ),
                    lease=lease,
                    hook_outcome=hooks._hook_failure_outcome(),
                    hook_reason=(
                        "budget_exhausted"
                        if outcome.classification == "budget-exceeded"
                        else "turn_error"
                    ),
                    turn=outcome,
                )
                return

            log.record_metric(
                "curie.queue.retry",
                attributes={
                    "service.name": "curie-worker",
                    "source": "worker",
                    "retry_class": cast("str", outcome.classification),
                },
            )
            backoff_s = self._backoff(
                capacity_refusals if capacity_continuation is not None else attempt
            )
            remaining = claim._remaining_budget(lease)
            if capacity_continuation is not None:
                remaining = capacity_continuation.bound_remaining_s(remaining)
            if remaining is not None:
                # The backoff CONSUMES the delivery budget; it never extends
                # it. Clamped, because an unclamped backoff longer than the
                # remaining deadline burns the whole thing asleep and then
                # escalates without ever having retried -- the worst of both.
                backoff_s = min(backoff_s, max(0.0, remaining))
            if capacity_continuation is not None:
                logger.info(
                    "sandbox capacity retry for event %s: refusal=%d backoff=%.3fs",
                    event_id,
                    capacity_refusals,
                    backoff_s,
                )
            await asyncio.sleep(backoff_s)
    finally:
        if owned_work_item_id is not None:
            # A settlement handoff already removed the active entry. Its
            # task exclusively owns heartbeat close and sandbox release.
            owned_run = self._work_item_runs.get(owned_work_item_id)
            if owned_run is not None and owned_run.held:
                self._held_work_items[owned_run.thread_key] = owned_run
                self._work_item_runs.pop(owned_work_item_id, None)
            else:
                owned_run = self._work_item_runs.pop(owned_work_item_id, None)
                if owned_run is not None:
                    await owned_run.close()
                    if owned_run.finished:
                        # The turn has ended and the request is settled, so
                        # nothing on this thread needs the sandbox (#3075).
                        await self._release_work_item_sandbox(owned_run.thread_key)
        constants._OWNED_WORK_ITEM.reset(owned_token)
        constants._PUBLICATION_CONTEXT.reset(publication_token)
        constants._TURN_PROGRESS.reset(progress_token)
        release_order()
        # Lower the assistant-thread "shimmer" raised above, on every exit
        # path (success, escalate, drop, or error). Best-effort and
        # idempotent -- it never repeats an action or blocks the turn, so it
        # is safe outside the concurrency-critical section above.
        #
        # Unconditional on the exit paths that never raised one (an unmapped
        # channel, a paused agent, an already-done event) on purpose: clearing
        # a status that was never set is a no-op on Slack's side, and the
        # alternative is tracking "did we set it" across every early return in
        # this function, which is more state on the sacred path for no gain.
        #
        # ONLY the shimmer clear lives here (EB-B6(a)). ``turn.completed``
        # deliberately does not: this ``finally`` runs for every exception,
        # while a failed entry stays PENDING for reclaim -- so completing
        # here would tell the adapter to deliver a turn that is about to run
        # again. An EMPTY status IS the clear on the neutral wire.
        if self._config.shimmer and not targetless:
            # Rebuilt rather than reusing the ``target`` captured at entry, so
            # a placeholder-less turn clears the status against the message it
            # actually posted. Slack's status call does not read the ref, but a
            # buffered adapter's does, and handing it a stale null would strand
            # the caption on a channel nobody is watching.
            await self._emit_status(self._target_for(qevent), route, "")
        # Drop this turn's minted ref last, after every delivery above has had
        # its chance to read it. Popping earlier would make the shimmer clear
        # address a message the rest of the turn had already adopted.
        self._minted_refs.pop(event_id, None)


def _acquire_order_entry(self: Kernel, thread_key: str) -> delivery._LockEntry:
    entry = self._order_locks.get(thread_key)
    if entry is None:
        entry = delivery._LockEntry(asyncio.Lock())
        self._order_locks[thread_key] = entry
    entry.refs += 1
    return entry


def _release_order_entry(self: Kernel, thread_key: str, entry: delivery._LockEntry) -> None:
    entry.refs -= 1
    if entry.refs == 0 and self._order_locks.get(thread_key) is entry:
        del self._order_locks[thread_key]
