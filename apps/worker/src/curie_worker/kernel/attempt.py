from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import aiohttp
from aci_protocol import (
    TOOL_ACCESS_STATUS_FIELD,
    ErrorEvent,
    Event,
    Final,
    OutboundEvent,
    QueuedTurn,
    SessionStatus,
    SideEffectFlag,
    TextDelta,
    ToolAccess,
    ToolNote,
    TurnSource,
)
from channel_protocol.reply import (
    NavAffordance,
    ReplyAck,
)
from curie_telemetry.redact import redact_text

from ..actions import ActionBackendError
from ..approvals import (
    ApprovalBackendError,
    ReviewAuthorityUnavailable,
    VerifiedReviewFeedback,
)
from ..attachments import (
    AttachmentResolutionError,
)
from ..behaviorpacks import (
    BehaviorPacks,
)
from ..capacity_wait import (
    CapacityWaitRequested,
)
from ..delivery_lease import DeliveryLease
from ..publication_validation import validate_snapshot_against_base
from ..reply_sink import (
    TargetRoute,
)
from ..runner_client import (
    RunnerError,
    RunnerSnapshotReadError,
    RunnerStreamTimeout,
    TurnStream,
)
from ..sandbox.quota import quota_rejection_is_valid
from ..sandbox.types import (
    CapacityExhaustedError,
    MissingAgentPoolError,
    SandboxError,
    SandboxHandle,
    UnschedulableClaimError,
)
from ..turn_progress import (
    ProgressPump,
    deactivate_turn_progress,
    start_progress_pump,
)
from ..workitem_dispatch import (
    WorkItemConflict,
    parse_work_item_event_id,
)
from ..workspace import (
    WorkspacePreparationError,
    WorkspaceSelectionRefused,
)

if TYPE_CHECKING:
    from .core import Kernel

from . import (
    approval,
    channel_read,
    claim,
    clock,
    constants,
    delivery,
    failures,
    memory,
    publication,
    routing,
    workspace,
)
from .log import logger


async def _attempt(
    self: Kernel,
    qevent: QueuedTurn,
    route: TargetRoute,
    release_order: Callable[[], None],
    boot_env: dict[str, str] | None = None,
    agent_id: uuid.UUID | None = None,
    nav: NavAffordance | None = None,
    packs: BehaviorPacks | None = None,
    workspace_deployment_id: uuid.UUID | None = None,
    agent_name: str | None = None,
    *,
    runner_resources: dict[str, Any] | None = None,
    remaining_s: float | None = None,
    pressure_retried: bool,
    workspace_inference: workspace._WorkspaceInferenceCarry,
    memory_grant: memory.TurnMemoryGrant | None = None,
    channel_read_grant: channel_read.TurnChannelReadGrant | None = None,
) -> failures.TurnOutcome:
    """One attempt at a turn, then close its memory write credentials (#3776).

    Every turn claim the attempt minted is reported closed to the API when
    the attempt ends, on every outcome (success, runner error, start
    failure, cancellation), so a credential copied out of the sandbox stops
    writing then rather than at its expiry. A steered attempt hands its
    claim to the live turn it joined instead (``_close_memory_turns``).
    The closes run in the background, so the attempt's end never waits on
    the API (``_settle_memory_turns``).

    ADR 0100: the channel read logical turns the attempt opened are revoked
    the same way, under an owner id unique to this attempt, by the owner
    checked delete in Valkey (``_settle_channel_read``)."""

    record = memory._AttemptMemoryTurns(agent_id=agent_id)
    reset = constants._MEMORY_TURNS.set(record)
    cr_record = channel_read._AttemptChannelRead()
    cr_reset = constants._CHANNEL_READ_TURNS.set(cr_record)
    try:
        return await self._attempt_turn(
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
            remaining_s=remaining_s,
            pressure_retried=pressure_retried,
            workspace_inference=workspace_inference,
            memory_grant=memory_grant,
            channel_read_grant=channel_read_grant,
        )
    finally:
        constants._MEMORY_TURNS.reset(reset)
        constants._CHANNEL_READ_TURNS.reset(cr_reset)
        # The lease heartbeat is a child of this attempt: it stops here, so a
        # lease nothing renews lapses even if the settlement below cannot run.
        if cr_record.heartbeat is not None:
            cr_record.heartbeat.cancel()
        if record.minted or record.live_turns:
            self._settle_memory_turns(record)
        if cr_record.pending():
            self._settle_channel_read(cr_record)


async def _attempt_turn(
    self: Kernel,
    qevent: QueuedTurn,
    route: TargetRoute,
    release_order: Callable[[], None],
    boot_env: dict[str, str] | None = None,
    agent_id: uuid.UUID | None = None,
    nav: NavAffordance | None = None,
    packs: BehaviorPacks | None = None,
    workspace_deployment_id: uuid.UUID | None = None,
    agent_name: str | None = None,
    *,
    runner_resources: dict[str, Any] | None = None,
    remaining_s: float | None = None,
    pressure_retried: bool,
    workspace_inference: workspace._WorkspaceInferenceCarry,
    memory_grant: memory.TurnMemoryGrant | None = None,
    channel_read_grant: channel_read.TurnChannelReadGrant | None = None,
) -> failures.TurnOutcome:
    handle = qevent.reply_handle
    thread_key = routing._thread_key_for(qevent)
    attempt_started = clock.time.monotonic()

    # Surface a booting state on the placeholder so the (up to claim_timeout)
    # cold-boot wait is not silent. Best-effort and outside the per-thread lock:
    # a Slack failure here must never fail the turn, and this must not lengthen
    # the critical section. Fires once per attempt (retries re-affirm it).
    # Suppressed under no edit streaming: with a preposted placeholder, the
    # mode emits one final chat.update. A placeholderless approval is an
    # exception: its approval path posts the request text before persistence,
    # then updates that message with the approval notice.
    # A placeholderless job or review candidate must route first. Otherwise
    # every busy redelivery or authority outage posts a notice for a turn
    # that never started. Reviews publish their receipt after reservation;
    # webhook jobs publish that deferred booting state below after routing
    # succeeds, and a cron turn does not. A cron turn posts once, when the
    # final reply is delivered.
    review_candidate = constants._REVIEW_EVENT_ID_RE.fullmatch(qevent.event_id) is not None
    placeholder = None if handle is None else handle.placeholder
    defer_job_booting = placeholder is None and qevent.source.is_job
    defer_review_booting = placeholder is None and review_candidate
    if (
        handle is not None
        and not self._config.slack_no_edit_streaming
        and not (defer_job_booting or defer_review_booting)
    ):
        try:
            await self._reply_for(qevent, route, self._config.booting_text, terminal=False)
        except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
            logger.warning("booting-state update failed for %s", qevent.event_id)

    event = self._to_event(qevent)
    # ADR-0188: this attempt's memory write credential is minted where the
    # runner call is made, after the sandbox claim, so its expiry is the
    # stream deadline that call uses and a retry gets its own. A steer mints
    # from the same carry, so a steering sender writes under their own name.
    memory_mint = constants._MEMORY_MINT.set(
        None if memory_grant is None else memory._MemoryMint(qevent=qevent, grant=memory_grant)
    )
    # ADR 0100: the channel read capability is minted the same way, where the
    # turn opens; a steer renews the live turn's instead.
    cr_mint = constants._CHANNEL_READ_MINT.set(
        None
        if channel_read_grant is None
        else channel_read._ChannelReadMint(qevent=qevent, grant=channel_read_grant)
    )

    attachment_intent = self._attachments is not None and bool(qevent.attachments)

    # Critical section: decide steer-vs-new-turn and, if new, open the turn so
    # it is active before we release the Valkey lock (rule 1: no two live
    # turns per thread across workers). Then release the order lock so the
    # next same-thread event can route, and release the Valkey lock before
    # streaming so a follow-up can steer.
    routed: routing._RouteResult | None = None
    verified_review: VerifiedReviewFeedback | None = None
    review_receipt: str | None = None

    async def close_routed_turn() -> None:
        # Unregister only a turn this attempt opened. A follow-up that
        # failed during steer or lock acquire never registered; dropping
        # the agent+thread key would hide the original live turn from kill.
        # start_turn's BaseException path unregisters inside _route_and_start
        # before routed is assigned here. Canned and steered leave turn None.
        if routed is not None and routed.turn is not None:
            self._unregister_run(agent_id, thread_key)
            if memory_grant is not None:
                self._turn_deadlines.pop(memory_grant.thread_key, None)
            routed.turn.close()
        progress_plan = constants._TURN_PROGRESS.get()
        if progress_plan is not None and self._progress is not None:
            await deactivate_turn_progress(self._progress, progress_plan)

    def record_reclaimed_retry() -> None:
        if pressure_retried:
            self._record_pressure_outcome("reclaimed")

    async def capacity_response() -> failures.TurnOutcome:
        release_order()
        if qevent.source is TurnSource.SLACK and handle is not None:
            raise CapacityWaitRequested()
        await self._reply_for(qevent, route, constants._CAPACITY_REPLY)
        return failures.TurnOutcome(terminal_ok=True, start_failed=True)

    try:
        try:
            if review_candidate:
                verifier = getattr(self._publication_creator, "verify_review_feedback", None)
                if verifier is None or workspace_deployment_id is None:
                    raise WorkspaceSelectionRefused(
                        "GitHub feedback requires a configured trusted workspace "
                        "verifier; no model turn started."
                    )
                try:
                    verified = await verifier(qevent, workspace_deployment_id)
                except (ApprovalBackendError, TimeoutError):
                    # No runner turn exists yet. Preserve this delivery for
                    # bounded reclaim instead of spending model attempts on
                    # control-plane uncertainty.
                    raise ReviewAuthorityUnavailable(
                        "GitHub feedback verification is temporarily unavailable"
                    ) from None
                if (
                    not isinstance(verified, VerifiedReviewFeedback)
                    or verified.agent_id != agent_id
                    or verified.sender != qevent.author
                    or verified.origin_key != qevent.event_id
                ):
                    raise WorkspaceSelectionRefused(
                        "GitHub feedback no longer belongs to this conversation."
                    )
                verified_review = verified
                review_receipt = verified.receipt
            if attachment_intent:
                routed = await self._route_attachment_and_start(
                    qevent,
                    thread_key,
                    event,
                    boot_env,
                    agent_id,
                    packs,
                    workspace_deployment_id=workspace_deployment_id,
                    agent_name=agent_name,
                    runner_resources=runner_resources,
                    source=qevent.source,
                    remaining_s=remaining_s,
                    verified_review=verified_review,
                    review_turn=qevent if verified_review is not None else None,
                    workspace_inference=workspace_inference,
                )
            else:
                claim_env = dict(boot_env or {}) if self._attachments is not None else boot_env
                async with self._lock.hold(self._config.lock_key(thread_key)):
                    routed = await self._route_and_start(
                        thread_key,
                        event,
                        claim_env,
                        packs,
                        queued_event_id=qevent.event_id,
                        workspace_deployment_id=workspace_deployment_id,
                        agent_name=agent_name,
                        runner_resources=runner_resources,
                        source=qevent.source,
                        remaining_s=remaining_s,
                        agent_id=agent_id,
                        verified_review=verified_review,
                        review_turn=qevent if verified_review is not None else None,
                        workspace_inference=workspace_inference,
                        approval_resume=self._is_approval_resume(qevent.event_id),
                        work_item_repo=self._work_item_repository(qevent.event_id),
                    )
        except BaseException:  # noqa: BLE001 - broad catch kept at a failure boundary
            # start_turn owns a live response as soon as it returns, which
            # is before the route lock's async exit has finished. Preserve
            # cancellation and every existing error policy, but release the
            # response first if lock cleanup itself fails or is cancelled.
            await close_routed_turn()
            raise
        finally:
            constants._MEMORY_MINT.reset(memory_mint)
            constants._CHANNEL_READ_MINT.reset(cr_mint)
    except CapacityExhaustedError as exc:
        rejection = exc.rejection
        logger.warning(
            "sandbox capacity exhausted for event %s: quota=%s requested=%s used=%s hard=%s",
            qevent.event_id,
            rejection.quota_name,
            rejection.requested,
            rejection.used,
            rejection.hard,
        )
        parsed_execute = parse_work_item_event_id(qevent.event_id)
        if parsed_execute is not None and parsed_execute.kind in {"execute"}:
            run = self._work_item_runs.get(parsed_execute.request_id)
            if run is not None and not run.started:
                release_order()
                try:
                    await run.defer("capacity", capacity=True)
                except WorkItemConflict as exc:
                    logger.info(
                        "work-item capacity defer refused for %s: %s",
                        qevent.event_id,
                        exc.code,
                    )
                raise failures._WorkItemDeferred() from None

        async def capacity_refusal() -> failures.TurnOutcome:
            # Started factory continuations wait in the driving loop:
            # their running request cannot use SQL defer (#4275). An
            # interactive approval keeps its bounded retry policy (#3693).
            if (
                parsed_execute is not None
                and parsed_execute.is_ci_fix
                and self._run_for_event(qevent.event_id) is not None
            ) or self._is_approval_resume(qevent.event_id):
                release_order()
                return failures.TurnOutcome(terminal_ok=False, classification="sandbox-capacity")
            return await capacity_response()

        if pressure_retried:
            self._record_pressure_outcome("reclaimed-retry-refused")
            return await capacity_refusal()

        if not quota_rejection_is_valid(rejection):
            self._record_pressure_outcome("refused-invalid-quota")
            return await capacity_refusal()

        pressure_started = clock.time.monotonic()
        current_remaining = (
            None if remaining_s is None else remaining_s - (pressure_started - attempt_started)
        )
        required = constants._PRESSURE_CEILING_S + self._substrate.claim_timeout_seconds
        if current_remaining is None or current_remaining < required:
            self._record_pressure_outcome("refused-no-budget")
            return await capacity_refusal()

        reclaimed = await self._reclaim_idle_route(
            thread_key,
            rejection,
            remaining_s=current_remaining,
        )
        if not reclaimed.reclaimed:
            self._record_pressure_outcome(reclaimed.outcome)
            return await capacity_refusal()

        retry_remaining = current_remaining - (clock.time.monotonic() - pressure_started)
        if retry_remaining <= 0:
            self._record_pressure_outcome("timeout")
            return await capacity_refusal()
        logger.info(
            "idle route reclamation freed sandbox capacity; retrying event %s",
            qevent.event_id,
        )
        return await self._attempt(
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
            remaining_s=retry_remaining,
            pressure_retried=True,
            workspace_inference=workspace_inference,
            memory_grant=memory_grant,
            channel_read_grant=channel_read_grant,
        )
    except failures.PendingPublicationError as exc:
        record_reclaimed_retry()
        release_order()
        logger.info(
            "pending publication refused a new turn for agent=%s deployment=%s thread=%s",
            agent_name,
            workspace_deployment_id,
            thread_key,
        )
        await self._reply_for(qevent, route, exc.public_detail)
        return failures.TurnOutcome(terminal_ok=True)
    except WorkspaceSelectionRefused as exc:
        record_reclaimed_retry()
        release_order()
        # LOG it, not only reply (#2004). This was the one turn-ending branch
        # in this handler that ended a turn silently, and it ends it with no
        # sandbox: `WorkspaceSelectionRefused` subclasses
        # `WorkspacePreparationError`, so this narrower `except` runs first
        # and the turn never reaches the sibling below that logs "turn start
        # failed".
        #
        # The reply covers a person in Slack, who reads the refusal and acts
        # on it. It covers nobody when the turn came from a hook: there is no
        # placeholder to edit and no one watching, so an acknowledged entry
        # with no sandbox and no log is indistinguishable from an idle bot --
        # and the last change anyone made was a boolean they would not
        # connect to it.
        #
        # info rather than warning: a refusal is this feature working as
        # designed, and routing routine policy outcomes to warning is how
        # warnings stop being read. It names the agent because that is the
        # only thing an operator chasing a silent bot has to search on.
        logger.info(
            "workspace selection refused for agent=%s deployment=%s thread=%s: %s",
            agent_name,
            workspace_deployment_id,
            thread_key,
            exc.public_detail,
        )
        await self._reply_for(qevent, route, exc.public_detail)
        return failures.TurnOutcome(terminal_ok=True)
    except WorkspacePreparationError as exc:
        # AFTER the refusal branch above, and it has to stay after it:
        # `WorkspaceSelectionRefused` subclasses this, so ordering these two
        # the other way round would swallow a deliberate policy answer and
        # retry it. What reaches HERE is infrastructure, not policy -- the
        # clone, the archive, the upload, the coordinator wiring. It used to
        # fall into the clause below and answer to the name "runner-error",
        # pointing an operator at a runner that never saw the fault. Retry
        # behavior is deliberately identical (`workspace-error` is
        # retryable); only the name and the log line change.
        record_reclaimed_retry()
        release_order()
        self._log_workspace_start_failure(
            qevent,
            event.text,
            exc,
            agent_id=agent_id,
            agent_name=agent_name,
            workspace_deployment_id=workspace_deployment_id,
        )
        return failures.TurnOutcome(terminal_ok=False, classification="workspace-error")
    except AttachmentResolutionError as exc:
        record_reclaimed_retry()
        release_order()
        reason = redact_text(failures._exception_reason(exc))[: constants._ESCALATION_DETAIL_MAX]
        logger.warning(
            "attachment resolution failed for event %s: stage=%s reason=%s",
            qevent.event_id,
            exc.stage,
            reason,
        )
        await self._reply_for(qevent, route, constants._UNAVAILABLE_ATTACHMENT_REPLY)
        return failures.TurnOutcome(terminal_ok=True, start_failed=True)
    except failures.ToolAccessUnenforced as exc:
        # @spec WORKER-TOOL-ACCESS-2: a failed turn, escalated under its own
        # class and never retried (the class is not retryable); the model
        # was not asked.
        record_reclaimed_retry()
        release_order()
        logger.warning("turn start refused for %s: %s", qevent.event_id, exc)
        return failures.TurnOutcome(
            terminal_ok=False,
            classification=constants.TOOL_ACCESS_UNENFORCED_CLASSIFICATION,
            error_message=exc.public_detail,
        )
    except failures.ChannelReadUnenforced as exc:
        # ADR 0100: a granted turn whose runner does not advertise channel
        # read enforcement fails under its own class and is never retried;
        # the model was not asked. The attempt's settlement revokes the
        # capability already minted.
        record_reclaimed_retry()
        release_order()
        logger.warning("turn start refused for %s: %s", qevent.event_id, exc)
        return failures.TurnOutcome(
            terminal_ok=False,
            classification=constants.CHANNEL_READ_UNENFORCED_CLASSIFICATION,
            error_message=exc.public_detail,
        )
    except MissingAgentPoolError as exc:
        # Before the SandboxError clause below, which would retry it. The
        # pool appears only after an operator changes the release values,
        # so a retry fails the same way; answer once, naming the fix
        # (#2943).
        record_reclaimed_retry()
        release_order()
        logger.warning("turn start refused for %s: %s", qevent.event_id, exc)
        await self._reply_for(
            qevent,
            route,
            f"This agent cannot start: {exc}. An operator has to make that change.",
        )
        return failures.TurnOutcome(terminal_ok=True, start_failed=True)
    except (
        RunnerError,
        RunnerSnapshotReadError,
        aiohttp.ClientError,
        TimeoutError,
        OSError,
        SandboxError,
    ) as exc:
        # The turn was never accepted (transient runner 5xx, runner not ready,
        # claim timeout, route-lock acquire timeout). Convert to a retryable
        # outcome so process_event backs off and retries within max_attempts,
        # instead of letting the entry escape to the consumer and sit pending
        # for the whole reclaim window.
        record_reclaimed_retry()
        release_order()
        if isinstance(exc, UnschedulableClaimError):
            # No node has room for the pod the quota admitted (#3169). A
            # Factory execution and interactive Slack turns wait for
            # capacity exactly as on a quota refusal. Other deliveries
            # keep the retry below.
            parsed_execute = parse_work_item_event_id(qevent.event_id)
            run = (
                self._work_item_runs.get(parsed_execute.request_id)
                if parsed_execute is not None and parsed_execute.kind in {"execute"}
                else None
            )
            if run is not None and not run.started:
                logger.warning(
                    "sandbox unschedulable for event %s; deferring for capacity: %s",
                    qevent.event_id,
                    exc,
                )
                try:
                    await run.defer("capacity", capacity=True)
                except WorkItemConflict as conflict:
                    logger.info(
                        "work-item capacity defer refused for %s: %s",
                        qevent.event_id,
                        conflict.code,
                    )
                raise failures._WorkItemDeferred() from None
            if qevent.source is TurnSource.SLACK and handle is not None:
                logger.warning(
                    "sandbox unschedulable for interactive event %s; waiting for capacity",
                    qevent.event_id,
                )
                raise CapacityWaitRequested() from None
        logger.warning("turn start failed for %s: %r", qevent.event_id, exc)
        return failures.TurnOutcome(terminal_ok=False, classification="runner-error")
    assert routed is not None
    record_reclaimed_retry()
    try:
        release_order()
    except BaseException:  # noqa: BLE001 - broad catch kept at a failure boundary
        await close_routed_turn()
        raise

    if review_receipt is not None:
        try:
            await self._reply_for(qevent, route, review_receipt, terminal=False)
        except asyncio.CancelledError:
            await close_routed_turn()
            raise
        except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
            # The result uses the existing durable completion path. A reply
            # outage here must not start this already-reserved model turn a
            # second time.
            logger.warning("GitHub feedback receipt delivery unavailable")

    if (
        not self._config.slack_no_edit_streaming
        and defer_job_booting
        and qevent.source is not TurnSource.CRON
    ):
        try:
            # Routing succeeded, so this delivery owns a real turn. Adopt the
            # minted ref before streaming so every later update edits it.
            await self._reply_for(qevent, route, self._config.booting_text, terminal=False)
        except asyncio.CancelledError:
            await close_routed_turn()
            raise
        except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
            logger.warning("booting-state update failed for %s", qevent.event_id)

    if routed.canned_reply is not None:
        # An enabled greeting/help pack matched a provably-fresh thread under
        # the route lock (ADR-0018). Deliver the canned reply onto the
        # placeholder and return terminal-ok so process_event marks the event
        # done. No run was registered, no sandbox claimed, no turn started.
        await self._reply_for(qevent, route, routed.canned_reply)
        return failures.TurnOutcome(terminal_ok=True)

    if routed.steered:
        # Delivered into the thread's live turn; that turn streams the output
        # onto its own placeholder. Retire this follow-up's placeholder so it
        # does not sit stuck on "working" in the thread.
        #
        # Steering is best-effort by design (mirror Claude Code, arch 2b rule
        # 3): the follow-up joins the live turn's context. If that owning turn
        # later fails and retries, the retry replays only its own event, so a
        # steer folded into a since-failed turn is not itself replayed. This is
        # the accepted MVP semantic; durable per-steer replay is a deliberate
        # follow-up, flagged to the orchestrator rather than silently assumed.
        memory_turns = constants._MEMORY_TURNS.get()
        if memory_turns is not None:
            # The live turn now holds this attempt's credential (#3776).
            memory_turns.steered = True
        await self._reply_for(qevent, route, "Folded into the in-progress reply above.")
        return failures.TurnOutcome(terminal_ok=True, steered=True)

    assert routed.handle is not None and routed.turn is not None
    hook_carry = constants._HOOK_RUN_CARRY.get()
    if hook_carry is not None:
        hook_carry.this_attempt_started = True
        hook_carry.any_attempt_started = True
    turn = routed.turn
    # Register this owner turn so a kill for its agent interrupts it, then
    # stream; unregister when the turn ends.
    self._register_run(agent_id, thread_key)
    try:
        # Close the precheck-vs-register race: a kill that landed between the
        # is_killed precheck and this registration would have interrupted zero
        # turns. Recheck now that the turn is registered and interrupt it.
        if (
            agent_id is not None
            and self._killswitch is not None
            and await self._killswitch.is_killed(agent_id)
        ):
            await self.interrupt_thread(thread_key, f"agent {agent_id} killed by operator")
        # #2659: this route's own inference wins; otherwise a retry honors the
        # delivery's carried fact while the adopted handle still carries it.
        carried = workspace_inference.repo
        inferred = routed.workspace_inferred_repo or (
            carried if workspace._same_repo(routed.handle.workspace_repo, carried) else None
        )
        async with self._keep_route_alive(thread_key, routed.handle.claim_name):
            outcome = await self._consume(
                qevent,
                route,
                turn,
                nav,
                agent_id,
                handle=routed.handle,
                workspace_inferred_repo=inferred,
            )
            outcome = await self._continue_unpublished(
                qevent,
                route,
                routed.handle,
                outcome,
                nav,
                agent_id,
                inferred,
                workspace_deployment_id=workspace_deployment_id,
                remaining_s=(
                    None
                    if remaining_s is None
                    else remaining_s - (clock.time.monotonic() - attempt_started)
                ),
                memory_grant=memory_grant,
                channel_read_grant=channel_read_grant,
            )
        outcome.workspace_inferred_repo = inferred
        if verified_review is not None:
            outcome.review_origin_key = verified_review.origin_key
        if outcome.status is SessionStatus.AWAITING_APPROVAL and publication._is_publish_provenance(
            outcome.approval_gate_kind,
            outcome.approval_granted_tool,
        ):
            snapshot = None
            snapshot_started = clock.time.monotonic()
            snapshot_budget_s = (
                None if remaining_s is None else remaining_s - (snapshot_started - attempt_started)
            )
            snapshot_attempts = 0
            snapshot_failure_text = "remaining budget is 5 s or less"
            while snapshot_attempts < 3:
                snapshot_remaining_s = (
                    None
                    if snapshot_budget_s is None
                    else snapshot_budget_s - (clock.time.monotonic() - snapshot_started)
                )
                if (
                    snapshot_remaining_s is not None
                    and snapshot_remaining_s <= constants._MIN_ATTEMPT_BUDGET_S
                ):
                    break
                snapshot_attempts += 1
                try:
                    snapshot = await self._runner.snapshot(
                        routed.handle.base_url,
                        token=routed.handle.token or None,
                        remaining_s=snapshot_remaining_s,
                    )
                except (RunnerError, aiohttp.ClientError, TimeoutError) as exc:
                    snapshot_failure_text = str(exc)
                    if not isinstance(exc, RunnerError):
                        snapshot_failure_text = type(exc).__name__ + (
                            f": {snapshot_failure_text}" if snapshot_failure_text else ""
                        )
                    logger.warning(
                        "publication snapshot read failed for %s: attempt %s of 3: %s",
                        qevent.event_id,
                        snapshot_attempts,
                        snapshot_failure_text,
                    )
                    if snapshot_attempts == 3 or not isinstance(
                        exc, (RunnerSnapshotReadError, aiohttp.ClientError, TimeoutError)
                    ):
                        break
                    snapshot_remaining_s = (
                        None
                        if snapshot_budget_s is None
                        else snapshot_budget_s - (clock.time.monotonic() - snapshot_started)
                    )
                    if (
                        snapshot_remaining_s is not None
                        and snapshot_remaining_s
                        <= constants._MIN_ATTEMPT_BUDGET_S + snapshot_attempts
                    ):
                        break
                    await asyncio.sleep(float(snapshot_attempts))
                else:
                    break
            if snapshot is None:
                # A trusted publication request never falls through into an
                # ordinary approval when snapshotting fails. The pause path
                # reports this error and creates neither durable row.
                outcome.publication_snapshot_error = (
                    "publication snapshot could not be read after "
                    f"{snapshot_attempts} attempt(s): {snapshot_failure_text}"
                )
            else:
                try:
                    if self._workspace is None:
                        raise WorkspacePreparationError(
                            "publication-validation",
                            "managed workspace coordinator is unavailable",
                        )
                    await asyncio.to_thread(
                        validate_snapshot_against_base,
                        self._workspace,
                        thread_key=thread_key,
                        snapshot=snapshot,
                        max_patch_bytes=self._config.publication_patch_max_bytes,
                        scratch_root=Path(self._config.workspace_scratch_root),
                        git_timeout_seconds=(self._config.publication_git_command_timeout_seconds),
                        protected_paths=self._config.publication_protected_paths,
                    )
                    outcome.publication_snapshot = snapshot
                except WorkspacePreparationError as exc:
                    outcome.publication_snapshot_error = f"publication snapshot failed: {exc}"
        return outcome
    finally:
        self._unregister_run(agent_id, thread_key)
        # _consume owns bounded post-Final drain and normally releases the
        # response itself. This idempotent backstop covers cancellation or
        # an unexpected failure after start_turn but before _consume enters
        # its response context.
        turn.close()
        progress_plan = constants._TURN_PROGRESS.get()
        if progress_plan is not None and self._progress is not None:
            await deactivate_turn_progress(self._progress, progress_plan)


async def _require_tool_access(
    self: Kernel,
    handle: SandboxHandle,
    access: ToolAccess,
    remaining_s: float | None,
) -> None:
    """Refuse unless THIS runner advertises ``access`` (WORKER-TOOL-ACCESS-2).

    Read from the handle the event is about to be posted to, with its own
    token, so a replaced or different runner cannot answer for it. An
    unreadable status raises the client's own error, which the caller
    retries like any turn the runner did not accept.
    """

    try:
        status = await self._runner.status(
            handle.base_url,
            token=handle.token or None,
            remaining_s=2.0 if remaining_s is None else min(2.0, remaining_s),
        )
    except (ValueError, TypeError) as exc:
        # A body that is not JSON: not accepted, so retried, never run.
        raise RunnerError("runner status was not readable") from exc
    if not isinstance(status, dict):
        raise RunnerError("runner status was not a JSON object")
    advertised = status.get(TOOL_ACCESS_STATUS_FIELD)
    if not isinstance(advertised, list) or access.value not in advertised:
        raise failures.ToolAccessUnenforced(access)


async def _consume(
    self: Kernel,
    qevent: QueuedTurn,
    route: TargetRoute,
    turn: TurnStream,
    nav: NavAffordance | None = None,
    agent_id: uuid.UUID | None = None,
    *,
    handle: SandboxHandle,
    workspace_inferred_repo: str | None,
) -> failures.TurnOutcome:
    acc = delivery._StreamAccumulator(
        workspace_inferred_repo=workspace_inferred_repo,
        receipt_mode=self._config.turn_receipt,
    )
    silent_requesting_chat = self._is_factory_work_item_turn(qevent.event_id)
    reply = delivery._ThrottledReply(
        self._sink,
        target=(
            None
            if routing._is_targetless(qevent) or silent_requesting_chat
            else self._target_for(qevent)
        ),
        route=route,
        min_interval_s=self._config.slack_edit_min_interval_s,
        nav=nav,
        no_edit=(
            self._config.slack_no_edit_streaming
            or qevent.source is TurnSource.CRON
            or silent_requesting_chat
        ),
        # Reply delivery is best-effort ONLY on an approval-resume turn (the
        # granted tool has already executed in the runner): a dead reply
        # endpoint with no default transport completes the turn instead of
        # dead-lettering the resolved approval. Recognized structurally by the
        # resume event_id, the same platform-authored resume signal the #419
        # card teardown keys off (see the marker note above _is_approval_resume).
        # This intentionally covers BOTH resume flavors -- the ``[approval
        # resolved]`` resolve path and the ``[approval expired]`` expiry path --
        # since both carry the ``approval-<id>-resolved`` event_id matched by
        # _is_approval_resume. That shared coverage is deliberate and
        # plan-ratified, not an oversight.
        best_effort=self._is_approval_resume(qevent.event_id),
        on_ref=lambda ref: self._adopt_ref(qevent, ReplyAck(ref=ref)),
        on_final=(
            None
            if silent_requesting_chat
            else lambda: self._terminal_reply_attempted.add(qevent.event_id)
        ),
    )
    pump = await self._start_progress_pump(qevent, route)
    stream_started_at = datetime.now(UTC)
    stream_reading = True
    try:
        # ``async with`` releases the aiohttp response on every exit path
        # (normal end, apply-frame error, or a mid-stream transport drop), so
        # the connection is never leaked.
        async with turn:
            async for frame in turn:
                stream_reading = False
                await self._apply_frame(frame, acc, reply, qevent, agent_id)
                # ``Final`` is the protocol's terminal response event. Stop
                # at that boundary so a late frame from a finishing runner
                # cannot overwrite the outcome and suppress the kernel's
                # bounded retry/escalation decision.
                if isinstance(frame, Final):
                    break
                stream_reading = True
    except (aiohttp.ClientError, TimeoutError) as exc:
        # Stream dropped mid-run (sandbox killed, network fault, or the
        # client's streaming budget expiring). No final.
        #
        # #2011: both halves of this used to lose information. ``str()`` of a
        # bare TimeoutError is the EMPTY STRING, so the operator log read
        # "turn stream dropped for <id>: " with nothing after the colon; and
        # a timeout collapsed into the same "runner-error" a killed sandbox
        # produces. ``_exception_reason`` guarantees a non-empty reason for
        # EVERY exception this clause catches, and a runner timeout gets its
        # own classification. An ErrorEvent-supplied ``acc.classification``
        # still wins: the runner's own account of why the turn failed
        # outranks the transport symptom the worker observed.
        #
        # The classification test is the CONCRETE ``RunnerStreamTimeout``,
        # never the ``TimeoutError`` base class: this clause spans frame
        # application, and ``_apply_frame`` delivers the reply, whose HTTP
        # adapter has its own 30s budget. A stalled reply endpoint raises a
        # plain ``TimeoutError`` while the runner was answering normally, so
        # it is not a runner timeout and must not borrow the name -- it
        # falls back to "runner-error". ``TurnStream.__aiter__`` converts
        # every stream-budget expiry into ``RunnerStreamTimeout``, so a
        # genuine runner timeout still lands here as "runner-timeout".
        logger.warning(
            "turn stream dropped for %s: %s",
            qevent.event_id,
            failures._exception_reason(exc),
        )
        if acc.classification is not None:
            classification = acc.classification
        elif isinstance(exc, RunnerStreamTimeout):
            classification = (
                "runner-timeout"
                if exc.timeout_result == "accepted"
                else "runner-timeout-unconfirmed"
            )
        else:
            classification = "runner-error"
            if stream_reading and isinstance(exc, aiohttp.ClientError):
                try:
                    termination = await asyncio.wait_for(
                        asyncio.to_thread(
                            self._substrate.pod_termination,
                            handle,
                            since=stream_started_at,
                        ),
                        timeout=3.0,
                    )
                except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
                    # Diagnosis is best effort; the original stream warning
                    # above remains the operator record of this failure.
                    pass
                else:
                    if termination is not None:
                        classification = "sandbox-terminated"
                        acc.error_message = failures._sandbox_termination_detail(termination)
        return failures.TurnOutcome(
            terminal_ok=False,
            saw_side_effect=acc.saw_side_effect,
            classification=classification,
            error_message=acc.error_message,
            text=acc.rendered(),
        )
    except ActionBackendError as exc:
        # The ledger refused a write (ADR-0117). The change to the world has
        # already happened and the platform has no account of it, so this
        # ends the turn rather than completing one whose receipt would be a
        # lie. "ledger-error" is deliberately absent from
        # RETRYABLE_CLASSIFICATIONS: a retry would re-execute a side effect,
        # which is the rule ADR-0013 already holds, and escalation puts a
        # human in front of the gap.
        logger.error("action ledger write failed for %s: %s", qevent.event_id, exc)
        return failures.TurnOutcome(
            terminal_ok=False,
            saw_side_effect=acc.saw_side_effect,
            classification="ledger-error",
            error_message=acc.error_message,
            text=acc.rendered(),
        )
    finally:
        # The runner has answered every progress post it made before the
        # stream ended, so the drain sees all of this turn's commands.
        if pump is not None:
            await pump.stop()

    return await self._finish(acc, reply)


async def _start_progress_pump(
    self: Kernel, qevent: QueuedTurn, route: TargetRoute
) -> ProgressPump | None:
    """The pump for this delivery's progress chain, or None (ADR 0130)."""

    plan = constants._TURN_PROGRESS.get()
    if plan is None or self._progress is None:
        return None
    return await start_progress_pump(
        self._progress,
        plan,
        route=route,
        target=lambda: self._target_for(qevent),
        render=self._config.progress_render,
    )


async def _apply_frame(
    self: Kernel,
    frame: OutboundEvent,
    acc: delivery._StreamAccumulator,
    reply: delivery._ThrottledReply,
    qevent: QueuedTurn,
    agent_id: uuid.UUID | None = None,
) -> None:
    if isinstance(frame, TextDelta):
        acc.text_parts.append(frame.text)
        await reply.stream(acc.rendered())
    elif isinstance(frame, ToolNote):
        # Tool notes remain available on the ACI stream for internal
        # consumers, but they are not part of the user-facing reply. The
        # worker reads the name to classify an unpublished factory turn (#3128).
        if frame.tool:
            acc.tools_called.add(frame.tool)
    elif isinstance(frame, SideEffectFlag):
        acc.saw_side_effect = True
        # Persist immediately so a crash before done still blocks auto-retry.
        # Unchanged by ADR-0117: this latches on the FIRST frame and the rule
        # reads presence, so a stream carrying one frame per call rather than
        # one per turn is the same signal to it.
        lease = constants._DELIVERY_LEASE.get()
        if claim._is_fenced(lease):
            assert lease is not None
            await self._mark_side_effect_with_retry(qevent.event_id, acc, lease)
        else:
            await self._markers.mark_side_effect(qevent.event_id)
        await self._record_action(frame, acc, qevent, agent_id)
    elif isinstance(frame, ErrorEvent):
        if frame.classification:
            acc.classification = failures.map_error_classification(frame.classification)
        acc.error_message = frame.message or acc.error_message
    elif isinstance(frame, Final):
        acc.status = frame.status
        acc.final_text = frame.text
        acc.approval_summary = frame.approval_summary
        acc.approval_route = frame.approval_route
        acc.approval_gate_kind = frame.approval_gate_kind
        acc.approval_granted_tool = frame.approval_granted_tool
        acc.approval_granted_arguments = frame.approval_granted_arguments
        acc.approval_display = frame.approval_display


async def _mark_side_effect_with_retry(
    self: Kernel, event_id: str, acc: delivery._StreamAccumulator, lease: DeliveryLease
) -> None:
    """Hold a side-effect frame until its marker is durable (ADR 0207)."""

    backoff_s = 0.5
    while True:
        lease.raise_if_lost()
        remaining_s = lease.local_deadline_monotonic - clock.time.monotonic()
        if remaining_s <= 0:
            break
        try:
            async with asyncio.timeout(remaining_s):
                await self._markers.mark_side_effect(event_id)
        except Exception as exc:  # noqa: BLE001 - ownership-bounded persistence retry
            remaining_s = lease.local_deadline_monotonic - clock.time.monotonic()
            if remaining_s <= 0:
                break
            logger.warning(
                "side-effect marker write for %s raised %s; retrying while ownership is held",
                event_id,
                failures._exception_reason(exc),
            )
            await asyncio.sleep(min(backoff_s, remaining_s))
            backoff_s = min(5.0, backoff_s * 2)
        else:
            lease.raise_if_lost()
            return

    acc.classification = "ownership-store-unavailable"
    raise TimeoutError("ownership store unreachable past the local lease deadline")


async def _record_action(
    self: Kernel,
    frame: SideEffectFlag,
    acc: delivery._StreamAccumulator,
    qevent: QueuedTurn,
    agent_id: uuid.UUID | None,
) -> None:
    """Open a record for a call, or close the one it already opened.

    Deliberately NOT best-effort. A change to the world the platform has no
    record of is not a success, and the branch this sits in already fails the
    turn when the no-retry marker cannot be persisted; losing the account of
    WHAT changed is not the lesser failure. An ActionBackendError propagates
    out of the frame loop exactly as a marker failure does.
    """

    if self._actions is None or frame.call_id is None:
        # No ledger wired, or a producer that predates ADR-0117. The frame
        # still carries presence, which is all the no-retry rule reads; there
        # is nothing to record without a call id, because two such frames
        # cannot be told apart.
        return
    opened = acc.open_actions.get(frame.call_id)
    if opened is None:
        recorded = await self._actions.record(
            frame,
            event_id=qevent.event_id,
            conversation_id=qevent.conversation_id,
            agent_id=str(agent_id) if agent_id is not None else None,
            # A gated tool only ever executes on the resume turn its approval
            # created, and that turn's event id IS the approval's key. So what
            # authorized this call is already in hand -- the same string the
            # card teardown reads -- and an ordinary turn yields None, which
            # is exactly "nothing gated it".
            gate_approval_id=approval._approval_id_from_resume_event(qevent.event_id),
            budget_s=routing._api_write_budget_s(),
        )
        acc.open_actions[frame.call_id] = recorded.id
        return
    completed = await self._actions.complete(opened, frame, budget_s=routing._api_write_budget_s())
    if completed:
        acc.receipt_rows.append(completed)


async def _finish(
    self: Kernel, acc: delivery._StreamAccumulator, reply: delivery._ThrottledReply
) -> failures.TurnOutcome:
    if acc.status in (SessionStatus.DONE, SessionStatus.IDLE_AWAITING_INPUT):
        text = acc.rendered_with_receipt()
        await reply.finalize(text)
        return failures.TurnOutcome(
            terminal_ok=True,
            saw_side_effect=acc.saw_side_effect,
            text=text,
            status=acc.status,
            tools_called=frozenset(acc.tools_called),
            assistant_text=acc.rendered(),
        )
    if acc.status is SessionStatus.AWAITING_APPROVAL:
        # Terminal for this turn, but the placeholder edit is deferred to
        # _pause_for_approval so the pending notice can carry the created
        # record's id (or the escalation, when no backend is wired).
        return failures.TurnOutcome(
            terminal_ok=True,
            saw_side_effect=acc.saw_side_effect,
            text=acc.rendered(),
            status=acc.status,
            approval_summary=acc.approval_summary,
            approval_route=acc.approval_route,
            approval_gate_kind=acc.approval_gate_kind,
            approval_granted_tool=acc.approval_granted_tool,
            approval_granted_arguments=acc.approval_granted_arguments,
            approval_display=acc.approval_display,
            tools_called=frozenset(acc.tools_called),
            assistant_text=acc.rendered(),
        )
    # classified-failure, or the stream ended with no final at all.
    return failures.TurnOutcome(
        terminal_ok=False,
        saw_side_effect=acc.saw_side_effect,
        classification=acc.classification or "runner-error",
        error_message=acc.error_message,
        text=acc.rendered(),
        status=acc.status,
        tools_called=frozenset(acc.tools_called),
    )


async def _escalate(
    self: Kernel,
    qevent: QueuedTurn,
    route: TargetRoute,
    message: str,
    *,
    failure_class: str,
) -> None:
    """Deliver ``message`` as a failed turn, with the class on the first line.

    The class marker is what a consumer that sees only the reply text uses
    to tell this from a successful task reply (#3401). Factory executions
    still skip the channel write; their status comment carries the class.
    """

    text = failures.turn_failure_reply(failure_class, message)
    logger.warning("escalating event %s: %s", qevent.event_id, text)
    await self._reply_for(qevent, route, text)


def _backoff(self: Kernel, attempt: int) -> float:
    # The exponent is clamped: a sweep continuation's busy wait counts
    # attempts without bound, and 2.0 ** 1024 overflows a float (#2878).
    raw: float = self._config.retry_backoff_base_s * (2 ** min(max(attempt - 1, 0), 64))
    return min(self._config.retry_backoff_max_s, raw)


def _to_event(qevent: QueuedTurn) -> Event:
    event_type: Literal["job", "message"] = "job" if qevent.source.is_job else "message"
    return Event(
        type=event_type,
        text=qevent.text,
        user=qevent.author,
        ts=qevent.conversation_id,
        # @spec WORKER-TOOL-ACCESS-1: carried unchanged; None is today's turn.
        tool_access=qevent.tool_access,
    )
