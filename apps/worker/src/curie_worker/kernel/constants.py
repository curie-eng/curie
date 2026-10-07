from __future__ import annotations

import asyncio
import logging
import re
import uuid
from contextvars import ContextVar
from typing import Any

from aci_protocol import (
    PublicationContext,
)
from plugin_format import PLATFORM_PUBLISH_TOOL_NAME

from ..reply_sink import (
    TargetRoute,
)
from ..turn_progress import (
    TurnProgressPlan,
)
from . import channel_read, hooks, memory

logger = logging.getLogger(__name__)

# Exact runner-stamped permission provenance required before the worker captures
# a patch. The name comes from plugin_format because the worker must not import
# the runner package.
_PUBLISH_PROVENANCE = ("permission", PLATFORM_PUBLISH_TOOL_NAME)

_PUBLICATION_EXPIRES_IN_SECONDS = 24 * 60 * 60

# Session gates use the same 24 hour deadline as publication (#1938).
# Omitting expires_in_seconds stores expires_at NULL, and the sweeper
# only selects rows that have one, so a request nobody resolves never wakes.
_SESSION_APPROVAL_EXPIRES_IN_SECONDS = 24 * 60 * 60

_ATTACHMENT_HANDOFF_PROBE_TIMEOUT_S = 5.0

# ADR 0205: how far past its own deadline a text boot waits for the thread set
# under the route lock before booting without it. The lane checks the deadline
# before every earlier fetch, so this only absorbs one in-flight fetch's tail.
_THREAD_SET_PREPARE_GRACE_S = 5.0

# ADR 0205: the ledger append after install, retries included. It runs after
# the route lock is released but before the turn's stream is consumed.
_THREAD_ATTACHMENT_APPEND_TIMEOUT_S = 10.0

_ACTIVE_ATTACHMENT_REPLY = (
    "I cannot add a file while the current reply is still running. "
    "Please send the whole message again after that reply finishes. "
    "The text of this message was not processed."
)

_UNSAFE_ATTACHMENT_REPLY = (
    "I could not safely add a file to this existing thread. "
    "Please start a new thread with the file attached. "
    "The text of this message was not processed."
)

_CHANGED_ATTACHMENT_REPLY = (
    "I could not add the file because the thread changed while I was fetching it. "
    "Please send the whole message again. "
    "The text of this message was not processed."
)

_WORKSPACE_ATTACHMENT_REPLY = (
    "I can't add a file to this thread because its repository workspace is already open. "
    "Please start a new thread with the file attached. "
    "The text of this message was not processed."
)

_REPOSITORY_ATTACHMENT_REPLY = (
    "I cannot add a file to an existing thread when a repository is selected for it. "
    "Please start a new thread with the file attached. "
    "The text of this message was not processed."
)

_UNAVAILABLE_ATTACHMENT_REPLY = (
    "I could not make that file available to the agent. "
    "Please send the message again with the file attached."
)

_CAPACITY_REPLY = "This agent is at capacity right now. Please try again shortly."

_CAPACITY_EXPIRED_REPLY = (
    "Your request could not start before its capacity wait ended. Please send it again."
)

_CAPACITY_FAILED_REPLY = "Your request started but could not finish. Please send it again."

_CAPACITY_UNKNOWN_REPLY = (
    "Your request may have started, but its result could not be confirmed. "
    "Please check for a reply before sending it again."
)

_CAPACITY_ADMISSION_OBSERVE_S = 20.0

_REVIEW_EVENT_ID_RE = re.compile(
    r"github-feedback-"
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
)

# The API cancels final reserve work at two seconds. This worker-side overall
# deadline includes protocol doubles and ASGI transports whose HTTP timeouts do
# not fire, while leaving the API enough time to return its retryable 503.
_REVIEW_RESERVE_CONTROL_PLANE_TIMEOUT_S = 3.0

_LIFECYCLE_SPAN: ContextVar[Any | None] = ContextVar("curie_worker_lifecycle_span", default=None)

_LIFECYCLE_OUTCOME: ContextVar[str | None] = ContextVar(
    "curie_worker_lifecycle_outcome", default=None
)

# Agent slug for the per-agent turn counter. Unset until binding resolves.
_TURN_AGENT: ContextVar[str | None] = ContextVar("curie_worker_turn_agent", default=None)

_ERROR_LIFECYCLE_OUTCOMES = frozenset(
    {
        "awaiting_approval",
        "budget_halted",
        "classified_failure",
        "side_effect_halted",
        "interrupted",
        # ADR-0131. Deliberately DISTINCT from ``budget_halted``, which means the
        # model spend budget was exhausted: this one means the delivery's
        # wall-clock deadline was. Folding the two together would make both
        # unreadable in telemetry -- an operator seeing a spike could no longer
        # tell "agents are burning money" from "turns are running long".
        "deadline_halted",
        # The turn finished but a replacement already held the fence, so nothing
        # was written and nothing was emitted.
        "fenced_out",
    }
)

# The route a targetless turn threads through signatures that require one. No
# delivery path uses it: every sink call is skipped for a targetless turn, and
# ``_target_for`` still raises for one, so nothing can be addressed with it.
_NO_EGRESS_ROUTE = TargetRoute()

# Failure classifications that are worth retrying (transient). Everything else
# (budget-exceeded, model/server errors) escalates rather than looping.
# ``runner-timeout`` (#2011) is the streaming budget expiring mid-turn. It is
# named separately from ``runner-error`` so an operator can tell "the model ran
# past the budget" from "the sandbox died", but it stays HERE because retry
# semantics are unchanged by that naming: a flag-clean timeout was transient
# before it had a name and still is. The side-effect check in ``_attempt`` runs
# BEFORE retryability is consulted (ADR-0013), so a timeout that arrives after a
# side-effect frame still escalates and is never retried.
# ``workspace-error`` (#2004) is a managed-workspace preparation failure -- a
# clone, an archive, an upload, or a missing coordinator -- raised before the
# turn was ever accepted. It is named separately from ``runner-error`` for the
# same reason ``runner-timeout`` is: an operator reading the escalation must be
# able to tell "this thread's repository could not be prepared" from "the
# sandbox died", and previously the two were indistinguishable. It stays HERE
# because naming it changes nothing about retryability: a clone or internal-API
# blip was transient before it had a name and still is, and the side-effect
# check above still runs first, so a workspace failure that somehow arrives
# after a side-effect frame escalates rather than replaying it.
# ``sandbox-capacity`` (#3693) is a ResourceQuota refusal on an approval resume,
# which retries rather than taking the ordinary capacity reply. It is named
# apart from ``runner-error`` because no runner was reached: the agent was busy.
RETRYABLE_CLASSIFICATIONS = frozenset(
    {
        "rate-limit",
        "runner-error",
        "runner-timeout",
        "sandbox-capacity",
        "sandbox-terminated",
        "workspace-error",
    }
)

#: The class a restricted turn fails under when its runner cannot enforce it.
TOOL_ACCESS_UNENFORCED_CLASSIFICATION = "tool-access-unenforced"

#: The class a channel read granted turn fails under when its runner cannot
#: enforce channel read (ADR 0100, #2877). Not retryable: the same boot refuses again.
CHANNEL_READ_UNENFORCED_CLASSIFICATION = "channel-read-unenforced"

# Platform ErrorEvent.classification vocabulary. Allowlist-constrain only: do
# not synonym-map SDK ``rate_limit`` onto platform ``rate-limit``, which would
# make a currently non-retryable token retryable.
PLATFORM_ERROR_CLASSIFICATIONS = frozenset(
    {
        "rate-limit",
        "runner-error",
        "runner-timeout",
        "workspace-error",
        "budget-exceeded",
        "server-error",
        "ledger-error",
        "model-credential-rejected",
        "model-credit-exhausted",
        "model-usage-limited",
        "approval-not-acted",
        "false-completion",
        "publication-unrecorded",
        "history-persistence-error",
        # #3071: the SDK's turn cap ran out (``error_max_turns``). Not retryable:
        # a retry would spend the same budget and stop at the same place.
        "max-turns",
        # WORKER-TOOL-ACCESS-5: the runner's refusals of a restricted turn
        # (``curie_runner.tool_access``), and the worker's own for a runner that
        # cannot enforce one. Not retryable: the same runner refuses again.
        TOOL_ACCESS_UNENFORCED_CLASSIFICATION,
        "tool-access-refused",
    }
)

UNCLASSIFIED_ERROR_CLASSIFICATION = "unclassified"

WORKER_LOCAL_DISPLAY_CLASSIFICATIONS = frozenset(
    {
        "runner-timeout-unconfirmed",
        "sandbox-capacity",
        "sandbox-terminated",
        # ADR 0100: the kernel's own refusal of a granted turn whose runner
        # does not advertise channel read enforcement. Worker local, so a
        # runner ErrorEvent naming it is not trusted. Not retryable.
        CHANNEL_READ_UNENFORCED_CLASSIFICATION,
    }
)

_ESCALATION_DETAIL_MAX = 300

# The factory terminus cause for a classified escalation (#3073). Each cause has
# its own operator sentence on the issue; anything unnamed stays the generic
# ``runner_escalated``.
_ESCALATION_CAUSES = {
    "model-credit-exhausted": "model_credit_exhausted",
    "model-usage-limited": "model_usage_limited",
    "model-credential-rejected": "model_credential_rejected",
    "rate-limit": "model_rate_limited",
    "server-error": "model_error",
    "budget-exceeded": "budget_exceeded",
    "runner-timeout": "runner_timeout",
    "runner-timeout-unconfirmed": "runner_timeout",
    "sandbox-terminated": "sandbox_terminated",
    "workspace-error": "workspace_error",
    "history-persistence-error": "history_capacity",
    # #3401: max-turns and an unclassified runner failure used to collapse into
    # runner_escalated, so a consumer that only read the terminus cause could
    # not tell them apart. Each keeps its own cause. Anything still unnamed
    # stays runner_escalated.
    "max-turns": "max_turns",
    "unclassified": "unclassified",
}

# Operator guidance appended to an escalation lead for classifications whose
# fix is a known knob (#3071). Keyed by the displayed classification token.
_CLASSIFICATION_GUIDANCE = {
    "history-persistence-error": (
        "Conversation history capacity exceeded. Work already performed may have side "
        "effects; inspect the result. The run can be retried; if one turn is over the "
        "cap, raise api.transcriptMaxThreadBytes (TRANSCRIPT_MAX_THREAD_BYTES)."
    ),
    # Read by the person who decided the approval. One event id resumes an
    # approved, a rejected and an expired approval, so it names no decision.
    # No quota detail (#2434): the worker's "sandbox capacity exhausted"
    # warning carries it for the operator.
    "sandbox-capacity": (
        "The agent was at capacity, so it could not continue after the approval "
        "decision. Send the request again in a few minutes if it is still needed."
    ),
}

# The runner's own turn cap when no CURIE_MAX_TURNS reaches its boot env
# (runner/src/curie_runner/config.py).
_RUNNER_DEFAULT_MAX_TURNS = 20

# First line of a failed turn's delivered reply (#3401). Frozen with the CLI
# reader in tests/vectors/turn-failure-reply.json. One token, no spaces, so a
# model sentence cannot satisfy the parser by mentioning the prefix.
TURN_FAILURE_REPLY_PREFIX = "curie-turn-failure:"

# The floor of remaining DELIVERY budget below which a fresh attempt is not
# started (ADR-0131). The runner cannot claim a sandbox, open a turn and stream a
# final in a couple of seconds, so an attempt started under this floor is
# guaranteed to be cut off mid-flight -- it buys nothing and spends the last of
# the deadline that the escalation and the terminal settle still need.
_MIN_ATTEMPT_BUDGET_S = 5.0

# Start-refusal codes that mean the work item settled terminally, so the
# execution is over and the delivering worker is the only one that can release
# the sandbox claim it just made (#3208). ``not_dispatchable`` is excluded on
# purpose: it also covers a lapsed acquire lease, where a replacement consumer
# re-acquires the same generation and adopts this thread's route, so the claim
# must survive the refusal. The passthrough set is defined by the API's
# ``_map_start_conflict``; keep the two in sync.
_WORK_ITEM_TERMINAL_START_REFUSALS = frozenset(
    {"work_item_cancelled", "waiting_deadline_elapsed", "not_found"}
)

# A factory execute turn that ends done or idle without calling publish_changes
# is re-prompted once in the same session (#3128). The progress tool's canonical
# spelling is runner/src/curie_runner/approval.py ``PROGRESS_TOOL_NAME``; the
# worker cannot import the runner.
_PROGRESS_TOOL_NAME = "mcp__curie__report_progress"

# Tools that only gather context: the runner's declared read-only set
# (runner/src/curie_runner/side_effects.py ``CLAUDE_READONLY_TOOLS``) plus todo
# bookkeeping. MCP tools named get_/list_/search_/read_ count as context too.
_CONTEXT_TOOLS = frozenset(
    {
        "Read",
        "Glob",
        "Grep",
        "LS",
        "NotebookRead",
        "WebFetch",
        "WebSearch",
        "ToolSearch",
        "TodoRead",
        "TodoWrite",
    }
)

_CONTEXT_MCP_PREFIXES = ("get_", "list_", "search_", "read_")

_APPROVAL_TOOL_NAME = "mcp__curie__request_approval"

# The API's ``FinishBody.detail`` max_length.
_FINISH_DETAIL_MAX = 4000

_EARLY_STOP_PROMPT = (
    "Your last turn ended before any work was reported or published. Start the "
    "work on the issue now, report progress as you go, and call publish_changes "
    "only when the change is complete and reviewed. If it cannot be done, post "
    "the skill's `Could not complete:` explanation instead."
)

_UNPUBLISHED_PROMPT = (
    "Your last turn ended before the work was published. Continue from the last "
    "phase and round you reported. Call publish_changes only when the work is "
    "complete and reviewed. If you cannot finish, post the skill's "
    "`Could not complete:` explanation instead."
)

# How long the reclaim preflight waits for a previous owner's runner to go idle
# after the interrupt, and how often it re-reads. Bounded (and further clamped to
# the delivery's own remaining budget) so recovering one transferred delivery can
# never consume the whole deadline; an interrupt that has not landed inside this
# window is treated as an unreadable runner, which fails closed.
_RECLAIM_PREFLIGHT_IDLE_TIMEOUT_S = 5.0

_RECLAIM_PREFLIGHT_POLL_S = 0.05

# ``WorkspaceClaimCoordinator`` performs blocking clone/archive/upload work on
# a worker thread. Its final handoff guard schedules one bounded runner-status
# read back onto the owning asyncio loop, then waits on that thread. The HTTP
# request already has the runner client's delivery-clamped timeout; this small
# grace covers scheduling and returning its result without leaving a worker
# thread blocked forever if the loop stops servicing callbacks.
_HANDOFF_REVALIDATION_BRIDGE_GRACE_S = 1.0

# Capacity pressure is deliberately a one shot, fixed envelope. The async Redis
# client disables retries and optional handshake commands. Cancellation closes
# its actual I/O, so the wall slices below also bound a multi reply pipeline.
# The cold operation value documents the worst configured handshake: connect,
# optional AUTH, optional SELECT, and the command response.
_PRESSURE_REDIS_CONNECT_S = 1.0

_PRESSURE_REDIS_READ_S = 1.0

_PRESSURE_REDIS_COLD_OPERATION_S = _PRESSURE_REDIS_CONNECT_S + 3 * _PRESSURE_REDIS_READ_S

_PRESSURE_SCAN_DEADLINE_S = 2.0

# Affinity uses an approximate SCAN COUNT hint of 8192. Eight pages cover about
# 65,000 database keys. This separate cap counts matching route keys before
# safety filtering, including suspended routes.
_PRESSURE_SCAN_PAGES = 8

_PRESSURE_SCAN_RECORDS = 256

_PRESSURE_CANDIDATES = 4

_PRESSURE_RUNNER_STATUS_S = 1.0

_PRESSURE_DELETE_S = 5.0

_PRESSURE_GONE_WAIT_S = 15.0

# Inventory is async and its timeout cancels and disconnects the Redis I/O.
_PRESSURE_INVENTORY_CEILING_S = _PRESSURE_SCAN_DEADLINE_S

# One candidate permits a cold lock command and cold affinity command, then a
# runner probe and three warm Redis responses for detach, ownership, and
# release. Reconnect churn is still cut off by this hard wall slice.
# A terminal wall cancellation is delivered once. ThreadLock then releases the
# victim lock with one finite operation that can cost one cold Redis bound, four
# seconds. That tail consumes unused claim reserve and never authorizes retry.
_PRESSURE_CANDIDATE_CEILING_S = (
    2 * _PRESSURE_REDIS_COLD_OPERATION_S + _PRESSURE_RUNNER_STATUS_S + 3 * _PRESSURE_REDIS_READ_S
)

_PRESSURE_CLEANUP_CEILING_S = _PRESSURE_DELETE_S + _PRESSURE_GONE_WAIT_S

_PRESSURE_CEILING_S = (
    _PRESSURE_INVENTORY_CEILING_S
    + _PRESSURE_CANDIDATES * _PRESSURE_CANDIDATE_CEILING_S
    + _PRESSURE_CLEANUP_CEILING_S
)

_PRESSURE_OUTCOMES = frozenset(
    {
        "expiry-unsupported",
        "race-lost",
        "reclaimed",
        "reclaimed-retry-refused",
        "refused-invalid-quota",
        "refused-no-budget",
        "refused-no-safe-route",
        "scan-incomplete",
        "timeout",
    }
)

# The platform-authored prefix that marks an approval resume turn as an EXPIRY
# (vs a resolve). Set by ``resumequeue.build_expiry_resume_turn`` on both expiry
# paths (the #412 sweeper and a past-SLA resolve); ``build_resume_turn`` uses
# ``[approval resolved]`` instead. This text marker -- not the turn author -- is
# the expiry discriminator: ``author`` is the authenticated resolver on a
# resolve (``resolved_by``), and "system" is the codebase's reserved
# machine-actor name (e.g. the sweeper's audit rows), so an authenticated
# resolver whose subject is "system" would otherwise get the card wrongly
# stamped expired. The marker is a stable
# platform contract on a platform-authored turn -- not user-intent guessing.
_EXPIRY_RESUME_MARKER = "[approval expired]"

# The pause notice above a card posted in the requester's own thread (ADR-0179
# decision 2). Worded as something that happened, so it stays true once the card
# settles and nothing edits it again.
_IN_THREAD_APPROVAL_NOTICE = "Approval requested. See the card below."

# The only channel kind a POLICY-ROUTED approval RESOLUTION target may name
# (#1460). The explicit kind is the future extension point, but accepting another
# resolver now would bypass the verified Slack identity on which the API's
# authorizer relies. Its route is empty on both halves, which means the worker's
# DEFAULT Slack transport (#451): the card is policy, so its transport is policy
# too, never the trigger's or the notification target's.
POLICY_CARD_KIND = "slack"

_POLICY_CARD_ADDRESS = re.compile(r"^[CDG][A-Z0-9]{7,}$")

_NOTIFICATION_ADDRESS_SHAPES = {POLICY_CARD_KIND: _POLICY_CARD_ADDRESS}

_CHANNEL_SLUG = re.compile(r"^[a-z0-9]+(?:[-_][a-z0-9]+)*$")

_ROUTE_WHITESPACE = re.compile(r"\s")

# ADR-0177 decision 1: the other form of a route's resolution. The card is shown
# in the conversation that asked, on whatever channel that is, exactly as a
# routeless approval's card already is. Mirrors the API's
# ``ApprovalRequestingSurfaceTarget`` (``schemas.approvals.REQUESTING_SURFACE_MODE``).
_REQUESTING_SURFACE = {"mode": "requesting_surface"}

# ADR-0177 amendment: the channels whose approvals are answered from a route's approver
# ``emails``. Only email today: the mail adapter's kind, and the API's
# ``schemas.channels.EMAIL_KIND``. A set rather than a comparison, because the question
# the raise path asks is "does this channel read an email list", not "which
# channel is this" (the reply seam stays kind-free, see test_reply_wire).
_APPROVER_EMAIL_KINDS = frozenset({"email"})

# How long an operator-requested reset waits on the courtesy interrupt before
# giving up and releasing anyway (#739). Deliberately seconds, not minutes: the
# runner client's own request timeout is 600s, and a wedged runner accepts the
# TCP connect then answers nothing, so an unbounded await there blocks the whole
# maintenance tick. A healthy runner answers an interrupt in well under a second.
# Note the coupling this creates with `RunnerClient.connect_timeout_s` (10.0, see
# runner_client.py): on this path 5s always fires first, so a runner that is not
# even accepting connections surfaces as this timeout rather than as the client's
# connect error. That is fine here because the release runs either way, but keep
# the two in mind together -- dropping the connect timeout below this value would
# silently change which error an operator sees in the log.
_RESET_INTERRUPT_TIMEOUT_S = 5.0

# How long a single thread's interrupt gets during the kill switch's fan-out
# over an agent's live threads (#742) before that thread is logged as failed
# and the fan-out moves on. Unlike `release_thread`, there is no fallback
# release to run afterward on this path, so a timeout here is surfaced via
# logging rather than swallowed -- but it must still be a bound, because an
# unbounded await on one wedged thread would otherwise leave the kill switch,
# "the one control that is supposed to work when things are broken," unable to
# signal the agent's other threads for as long as `RunnerClient.interrupt`'s
# own request budget.
_KILL_INTERRUPT_TIMEOUT_S = 5.0

# The substrate release itself (#743) runs on `asyncio.to_thread`, which is not
# cancellable -- a hang in the K8s control plane (as opposed to a wedged
# runner, already bounded above) would otherwise park the maintenance tick on
# this await indefinitely, the same stall shape as an unbounded interrupt.
# Wrapping it in `asyncio.wait_for` cannot stop the underlying thread (the
# executor slot stays occupied until the call actually returns), but it does
# return control to the caller so the tick is not held hostage by it; a timed
# out release surfaces as any other release failure does -- logged by the
# drain loop's per-request handler, with the request already popped so a
# fresh reset request is needed to retry.
_RESET_RELEASE_TIMEOUT_S = 5.0

# The release runs under the same per-thread route lock the turn path holds
# around `_route_and_start` (#734), so a reset and a turn-start on the same
# thread cannot interleave. Acquiring that lock is bounded like the interrupt
# and the release above, and for the same reason: a wedged or slow-cold-claiming
# turn can hold the route lock for up to the substrate's claim timeout, and a
# reset must not park the maintenance tick waiting on it. A lock that cannot be
# taken in time raises, which the drain loop treats as a failed release (left in
# the in-progress set, reported unconfirmed, retried by a fresh operator
# request) rather than falling back to the old, unsafe lock-free release.
_RESET_LOCK_ACQUIRE_TIMEOUT_S = 5.0

#: The escalation a read-only turn gets when its runner nonetheless ends it
#: awaiting approval (WORKER-TOOL-ACCESS-4).
_READ_ONLY_APPROVAL_REFUSAL = (
    "This read-only turn asked for an approval, which it may not do. No approval was created."
)

_HOOK_RUN_CARRY: ContextVar[hooks._HookRunCarry | None] = ContextVar(
    "curie_worker_hook_run_carry", default=None
)

# The WorkItem request this delivery acquired or adopted. Per delivery, never
# kernel-global: concurrent executions each start their own request (#3069).
_OWNED_WORK_ITEM: ContextVar[uuid.UUID | None] = ContextVar(
    "curie_worker_owned_work_item", default=None
)

_PUBLICATION_CONTEXT: ContextVar[PublicationContext | None] = ContextVar(
    "curie_worker_publication_context", default=None
)

# The deliberate progress chain this delivery reports on (ADR 0130), or None.
# Per delivery like the two above; see ``curie_worker.turn_progress``.
_TURN_PROGRESS: ContextVar[TurnProgressPlan | None] = ContextVar(
    "curie_worker_turn_progress", default=None
)

_MEMORY_MINT: ContextVar[memory._MemoryMint | None] = ContextVar(
    "curie_worker_memory_mint", default=None
)

# Per attempt, like the carries above; set and reset by ``Kernel._attempt``.
_MEMORY_TURNS: ContextVar[memory._AttemptMemoryTurns | None] = ContextVar(
    "curie_worker_memory_turns", default=None
)

# Strong references to in-flight closes, so a close scheduled in the background
# is not garbage collected before it finishes.
_PENDING_MEMORY_CLOSES: set[asyncio.Task[None]] = set()

# ADR 0100 (#2877): the channel read grant this attempt mints from where the
# turn opens, and the owner and opened logical turns it revokes when it ends.
# Per attempt, set and reset by ``Kernel._attempt`` like the memory carries.
_CHANNEL_READ_MINT: ContextVar[channel_read._ChannelReadMint | None] = ContextVar(
    "curie_worker_channel_read_mint", default=None
)
_CHANNEL_READ_TURNS: ContextVar[channel_read._AttemptChannelRead | None] = ContextVar(
    "curie_worker_channel_read_turns", default=None
)

# Strong references to in-flight channel read revocations, like the above.
_PENDING_CHANNEL_READ_CLOSES: set[asyncio.Task[None]] = set()
