from __future__ import annotations

import uuid

from ..actions import ActionRecorder
from ..approval_cards import ApprovalCardStore
from ..approvals import (
    ApprovalCreator,
    ApprovalReader,
    PublicationCreator,
)
from ..attachments import (
    AttachmentCoordinator,
)
from ..binding import (
    BindingResolver,
)
from ..config import WorkerConfig
from ..hook_runs import HookRunRecorder
from ..killswitch import KillSwitch
from ..markers import Markers
from ..progress import ProgressStore
from ..reply_sink import (
    ObservedReplySink,
    ReplySink,
)
from ..runner_client import (
    RunnerClient,
)
from ..sandbox import SandboxSubstrate
from ..sibling_turns import SiblingTurnLimit
from ..sweep import SweepCoverage
from ..threadlock import ThreadLock
from ..workitem_dispatch import (
    WorkItemDispatchClient,
    WorkItemRun,
)
from ..workspace import (
    WorkspaceClaimCoordinator,
)
from . import (
    approval,
    approval_key,
    attachments,
    attempt,
    capacity,
    claim,
    completion,
    delivery,
    hooks,
    lifecycle,
    memory,
    publication,
    routing,
    sweep_slices,
    work_items,
    workspace,
)


class Kernel:
    """Routes events to runner turns and enforces the concurrency rules."""

    def __init__(
        self,
        *,
        substrate: SandboxSubstrate,
        runner: RunnerClient,
        sink: ReplySink,
        lock: ThreadLock,
        pressure_lock: ThreadLock,
        markers: Markers,
        config: WorkerConfig,
        binding: BindingResolver | None = None,
        workspace: WorkspaceClaimCoordinator | None = None,
        attachments: AttachmentCoordinator | None = None,
        killswitch: KillSwitch | None = None,
        approvals: ApprovalCreator | None = None,
        publication_creator: PublicationCreator | None = None,
        # Separate from ``approvals`` on purpose (#1084): the pause path needs
        # only the create half, and a test fake for it should not have to grow a
        # read method it never calls. In production both are the one
        # ``ApprovalClient``.
        approval_reader: ApprovalReader | None = None,
        actions: ActionRecorder | None = None,
        card_store: ApprovalCardStore | None = None,
        hook_runs: HookRunRecorder | None = None,
        sweep: SweepCoverage | None = None,
        route_ttl_seconds: int = 3600,
        suspended_route_ttl_seconds: int = 86400,
        work_items: WorkItemDispatchClient | None = None,
        sibling_limit: SiblingTurnLimit | None = None,
        progress: ProgressStore | None = None,
    ) -> None:
        self._substrate = substrate
        self._runner = runner
        # Every outbound reply, including platform-authored drops, completion
        # events, escalations, and approval cards, crosses this one observed
        # seam. ``_ThrottledReply`` retains its decorator for direct callers;
        # the reply-sink ContextVar suppresses that nested observation.
        self._sink = ObservedReplySink(sink)
        self._lock = lock
        self._pressure_lock = pressure_lock
        self._markers = markers
        self._config = config
        # Deployment-to-runtime binding and the kill switch are optional: when
        # absent the kernel runs a generic sandbox (the F1 behavior); when present
        # it resolves channel -> agent -> bundle/budget and gates killed agents.
        self._binding = binding
        revoker = getattr(self._substrate, "set_boot_credential_revoker", None)
        poster = (
            getattr(binding, "release_boot_credential_sync", None) if binding is not None else None
        )
        if revoker is not None and poster is not None:
            revoker(poster)
        # The trusted repository preparation lane. It is optional for generic
        # and legacy deployments. A turn that requires a repository refuses when
        # this lane is unavailable instead of booting an empty directory.
        self._workspace = workspace
        # The inbound-attachment lane (#2567), optional on exactly the same
        # terms as the workspace lane above: a deployment that has not wired it
        # runs every turn unchanged rather than discovering a missing attribute.
        # Wired, it resolves a turn's attachment refs into parked objects plus a
        # short-lived one-object capability BEFORE the sandbox is claimed --
        # the claim env is how that capability is delivered -- and its sibling
        # retention ledger is swept from the same reap tick as the workspace's.
        self._attachments = attachments
        self._killswitch = killswitch
        # The approval-record backend (#244). When absent (unwired tests, a
        # deployment without the API), an awaiting-approval run degrades to an
        # escalation instead of suspending a session nothing could ever resume.
        self._approvals = approvals
        self._publication_creator = publication_creator
        self._approval_reader = approval_reader
        # The action ledger (ADR-0117). Optional like the approval backend: an
        # unwired deployment records nothing rather than failing every turn that
        # touches the world. Where it IS wired, a write it refuses fails the turn
        # -- see _apply_frame.
        self._actions = actions
        # Remembers where each suspended thread's approval card was posted so an
        # EXPIRY can disable it (#419); absent (unwired tests) simply skips the
        # card teardown -- the resolve-click path still heals a card on click.
        self._card_store = card_store
        self._hook_runs = hook_runs
        # ADR-0160 (#2878): the coverage reads behind a long scheduled sweep's
        # continuation and its stop notice. None runs every cron turn exactly
        # as before.
        self._sweep = sweep
        self._route_ttl_seconds = route_ttl_seconds
        self._suspended_route_ttl_seconds = suspended_route_ttl_seconds
        self._work_items = work_items
        # ADR-0168 decision 6: counts turns the installation's own identities
        # write to each other. None on an install with no sibling, which then
        # makes no call for it at all.
        self._sibling_limit = sibling_limit
        # Deliberate progress (ADR 0130). None sends no capability to any turn.
        self._progress = progress
        # Keyed by request id, never thread key: a steered follow-up shares the
        # thread and must not see or remove this run.
        self._work_item_runs: dict[uuid.UUID, WorkItemRun] = {}
        self._held_work_items: dict[str, WorkItemRun] = {}
        # Approval resume ids that were mapped to a factory execution.
        # The live run can be removed before a later delivery failure reaches
        # ``notify_turn_not_started``, so the exact event identity survives to
        # that boundary. Successful and cancelled turns clear it in
        # ``process_event``. A failed turn is cleared by the notice path.
        self._factory_work_item_events: set[str] = set()
        # Which threads are running which agent, so a kill interrupts the agent's
        # live turns. Populated while a turn owner streams.
        self._active_by_agent: dict[uuid.UUID, set[str]] = {}
        # ADR-0188: the wall-clock stream deadline of each live turn this worker
        # opened, by the memory grant's thread key. A steer's memory credential
        # is capped at it, because a steer ends when the live turn ends. Entries
        # past their deadline are dropped whenever one is recorded.
        self._turn_deadlines: dict[str, float] = {}
        # In-process per-thread lock over the route/start critical section only.
        # asyncio.Lock is FIFO, so same-thread events from one worker open/steer
        # the runner in arrival order (ordering preserved under concurrent sends).
        # The cross-worker guarantee is the Valkey ThreadLock; this adds
        # deterministic ordering within a process without blocking steering,
        # because it is released before the stream is consumed.
        self._order_locks: dict[str, delivery._LockEntry] = {}
        # Reply refs MINTED during a placeholder-less turn, keyed by event id
        # (ADR-0079). A triggered turn arrives with ``reply_handle.placeholder``
        # null because no ingress preposted anything, so its first delivery
        # creates the message and the adapter hands back its ref. Every later
        # event in the SAME turn has to edit that message instead of posting
        # another one, and the paths that need it -- the booting state, the
        # stream, the final flush, an escalation -- are four different methods
        # that each rebuild the target from the queued turn. Holding the minted
        # ref here is what makes those rebuilds agree.
        #
        # Bounded by construction: an entry is written only for a turn that had
        # no placeholder, and ``process_event`` drops it in a finally. It is
        # deliberately NOT a cache across turns -- a later turn on the same
        # thread gets its own message, exactly as a Slack mention does.
        self._minted_refs: dict[str, str] = {}
        # Event ids whose TERMINAL person-facing send was attempted during THIS
        # delivery (#2433). An attempt counts even if it raised: an ambiguous
        # delivery failure may still have landed remotely, and the fail-safe
        # direction is to treat the person as already answered rather than
        # overwrite the answer with a notice telling them to send it again.
        # Booting edits and streaming previews deliberately never mark: the first
        # is the placeholder saying work started, and a streamed fragment is not
        # an answer. Bounded exactly as ``_minted_refs`` is -- every successful or
        # cancelled turn clears its own entry in ``process_event``'s finally, and
        # a FAILED one is cleared by the notice that reads it.
        self._terminal_reply_attempted: set[str] = set()

    _finalize_settled_card = approval._finalize_settled_card
    _settle_remembered_card = approval._settle_remembered_card
    _settle_if_decided_before_registration = approval._settle_if_decided_before_registration
    _settled_from_record = approval._settled_from_record
    _place_the_resumed_reply = approval._place_the_resumed_reply
    _adopt_remembered_notice_ref = approval._adopt_remembered_notice_ref
    _pause_for_approval = approval._pause_for_approval
    _escalate_unanswerable_email_approval = approval._escalate_unanswerable_email_approval
    _is_approval_resume = staticmethod(approval_key._is_approval_resume)
    _route_attachment_and_start = attachments._route_attachment_and_start
    _discard_prepared_attachments = attachments._discard_prepared_attachments
    _resolve_attachments = attachments._resolve_attachments
    _attempt = attempt._attempt
    _attempt_turn = attempt._attempt_turn
    _require_tool_access = attempt._require_tool_access
    _consume = attempt._consume
    _start_progress_pump = attempt._start_progress_pump
    _apply_frame = attempt._apply_frame
    _record_action = attempt._record_action
    _finish = attempt._finish
    _escalate = attempt._escalate
    _backoff = attempt._backoff
    _to_event = staticmethod(attempt._to_event)
    notify_capacity_queued = capacity.notify_capacity_queued
    expire_capacity_wait = capacity.expire_capacity_wait
    resolve_capacity_grant = capacity.resolve_capacity_grant
    notify_capacity_expired = capacity.notify_capacity_expired
    _record_pressure_outcome = staticmethod(capacity._record_pressure_outcome)
    _pressure_record_is_safe = capacity._pressure_record_is_safe
    _pressure_status_is_safe = staticmethod(capacity._pressure_status_is_safe)
    _reclaim_idle_route = capacity._reclaim_idle_route
    _reclaim_idle_route_before_deadline = capacity._reclaim_idle_route_before_deadline
    _route_and_start = claim._route_and_start
    _log_claim_latency = staticmethod(claim._log_claim_latency)
    _turn_active = claim._turn_active
    _cap_caller_token_to_deadline = claim._cap_caller_token_to_deadline
    _claim_or_resume = claim._claim_or_resume
    _keep_route_alive = claim._keep_route_alive
    _complete = completion._complete
    _settle_targetless = completion._settle_targetless
    _record_agent_turn = completion._record_agent_turn
    _deliver_completion = completion._deliver_completion
    _reemit_pending_completion = completion._reemit_pending_completion
    _quarantine_completion = completion._quarantine_completion
    sweep_pending_completions = completion.sweep_pending_completions
    _set_shimmer = completion._set_shimmer
    _emit_status = completion._emit_status
    notify_turn_not_started = delivery.notify_turn_not_started
    notify_broker_entry_vanished = delivery.notify_broker_entry_vanished
    interrupt_thread = delivery.interrupt_thread
    release_thread = delivery.release_thread
    interrupt_agent = delivery.interrupt_agent
    _close_hook_run_after_error = hooks._close_hook_run_after_error
    _post_notice_after_error = hooks._post_notice_after_error
    _start_turn_under_hook_control = hooks._start_turn_under_hook_control
    _sweep_run = sweep_slices._sweep_run
    _sweep_read = sweep_slices._sweep_read
    _continue_sweep = sweep_slices._continue_sweep
    _coverage_notice = sweep_slices._coverage_notice
    _post_coverage_notice = sweep_slices._post_coverage_notice
    process_event = lifecycle.process_event
    _process_event = lifecycle._process_event
    _acquire_order_entry = lifecycle._acquire_order_entry
    _release_order_entry = lifecycle._release_order_entry
    _with_memory_token = memory._with_memory_token
    _record_turn_deadline = memory._record_turn_deadline
    _settle_memory_turns = memory._settle_memory_turns
    _close_memory_turns = memory._close_memory_turns
    _bind_publication_context = publication._bind_publication_context
    _continue_unpublished = publication._continue_unpublished
    _target_for = routing._kernel_target_for
    _adopt_ref = routing._adopt_ref
    _reply_for = routing._reply_for
    _drop_with_message = routing._drop_with_message
    _drop_ambiguous_route = routing._drop_ambiguous_route
    _drop_sibling_turn = routing._drop_sibling_turn
    _reply = routing._reply
    owns_work_item = work_items.owns_work_item
    _evict_expired_held_work_items = work_items._evict_expired_held_work_items
    _forget_held_work_items = work_items._forget_held_work_items
    _run_for_event = work_items._run_for_event
    _is_factory_work_item_turn = work_items._is_factory_work_item_turn
    _work_item_repository = work_items._work_item_repository
    _adopt_resumed_work_item = work_items._adopt_resumed_work_item
    reap_orphans = work_items.reap_orphans
    _preflight_reclaimed_delivery = work_items._preflight_reclaimed_delivery
    _quiesce_capacity_epoch = work_items._quiesce_capacity_epoch
    _release_work_item_sandbox = work_items._release_work_item_sandbox
    _terminate_work_item = work_items._terminate_work_item
    _stop_owned_work_item = work_items._stop_owned_work_item
    _abandon_stale_work_item = work_items._abandon_stale_work_item
    _forget_held_run = work_items._forget_held_run
    _halt_work_item_runtime = work_items._halt_work_item_runtime
    attach_killswitch = work_items.attach_killswitch
    _register_run = work_items._register_run
    _unregister_run = work_items._unregister_run
    _log_workspace_start_failure = workspace._log_workspace_start_failure
    _cold_handoff_readiness = workspace._cold_handoff_readiness
    _workspace_handoff_ready = workspace._workspace_handoff_ready
    _workspace_candidate_ready = workspace._workspace_candidate_ready
