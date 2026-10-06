from __future__ import annotations

import asyncio
import base64
import binascii
import contextlib
import json
import uuid
from collections.abc import AsyncIterator, Callable, Coroutine, Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from aci_protocol import (
    CHANNEL_READ_STATUS_FIELD,
    Event,
    QueuedTurn,
    TurnSource,
)

from .. import caller_token, sweep
from ..approvals import (
    ApprovalBackendError,
    PublicationLineage,
    ReviewAuthorityUnavailable,
    VerifiedReviewFeedback,
)
from ..attachments import ATTACHMENTS_MANIFEST_ENV, ATTACHMENTS_REF_ENV
from ..behaviorpacks import (
    BehaviorPacks,
    match_greeting,
    match_help,
)
from ..binding import (
    CONNECTOR_CALLER_TOKEN_ENV,
    HISTORY_TOKEN_ENV,
    MAX_TURNS_ENV,
    SANDBOX_TOKEN_TTL_SECONDS,
    boot_token_facts,
)
from ..capacity_wait import (
    CapacityWaitExpired,
    CapacityWaitRefused,
    CapacityWaitRequested,
    current_wait,
)
from ..connector_grant import mint as mint_connector_grant
from ..delivery_lease import DeliveryLease
from ..runner_client import (
    RunnerError,
    TurnStream,
)
from ..sandbox.types import (
    RouteChangedError,
    SandboxHandle,
    SuspendedThreadError,
)
from ..turn_progress import (
    ELIGIBILITY_ENV,
)
from ..workitem_dispatch import (
    WorkItemRun,
    WorkItemStartRefused,
)
from ..workspace import (
    WORKSPACES_DISABLED_REFUSAL,
    WorkspacePreparationError,
    WorkspaceRepositoryNotAllowed,
    WorkspaceSelectionRefused,
    trusted_repository_fact,
    webhook_job_refuses_workspace,
)

if TYPE_CHECKING:
    from .core import Kernel

from . import clock, constants, failures, memory, routing, workspace
from .log import logger


def _is_fenced(lease: DeliveryLease | None) -> bool:
    """Does this lease carry real distributed authority (ADR-0131)?

    Three shapes reach the kernel and only one of them is a fence. A ``None``
    lease is a direct caller (the sweeper's re-emit path, every pre-ADR-0131
    test). ``unfenced_lease()`` -- the permissive sentinel a consumer built with
    NO lease store yields -- has an empty owner and generation 0, and must take
    the leaseless path or a base-only consumer could never settle anything. Only
    a lease acquired from the store, with a real owner token and a generation of
    at least 1, is authority.
    """
    return lease is not None and lease.generation > 0 and bool(lease.owner)


def _remaining_budget(lease: DeliveryLease | None) -> float | None:
    """Seconds of delivery budget left, or ``None`` for a caller with no budget.

    ``None`` is what keeps every leaseless caller byte-identical: it reaches
    ``RunnerClient`` as "no per-request override", so the session default
    applies exactly as it did before ADR-0131.
    """
    return None if lease is None else lease.remaining_s()


def _boots_differently(
    handle: SandboxHandle,
    boot_env: Mapping[str, str] | None,
    *,
    caller_run: str | None = None,
) -> bool:
    """Whether a live runner booted with facts this delivery must not inherit.

    Read once at boot: ``CURIE_MAX_TURNS`` (#3071), the connector caller token
    (ADR-0168 decision 7), and the run that token names (ADR 0178). A runner
    claimed before the install had a caller key, or booted for a different
    run, is replaced on the next new turn.
    """

    env = boot_env or {}
    if handle.max_turns != env.get(MAX_TURNS_ENV):
        return True
    if (ELIGIBILITY_ENV in env) != handle.carries_turn_progress:
        return True
    if handle.caller_run != caller_run:
        return True
    # #3823: replace a warm sandbox whose boot token cannot cover the turn
    # this delivery is about to boot. Comparing with the new token expiry
    # keeps a follow-up from running past the credential and getting 401s.
    # A route with no recorded expiry is left alone.
    if handle.state_token_exp is not None:
        if handle.state_token_exp <= int(clock.time.time()):
            return True
        _agent, _cred, needed = boot_token_facts(env.get(HISTORY_TOKEN_ENV))
        if needed is not None and handle.state_token_exp < needed:
            return True
    return CONNECTOR_CALLER_TOKEN_ENV in env and not handle.carries_caller_token


def _caller_token_exp(token: str) -> int | None:
    """The ``exp`` claim of a caller token, or None when the token is unreadable."""

    parts = token.split(".")
    if len(parts) != 3:
        return None
    try:
        padded = parts[1] + "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded))
    except (ValueError, binascii.Error):
        return None
    exp = payload.get("exp") if isinstance(payload, dict) else None
    if type(exp) is not int:
        return None
    return exp


def _connector_tool_grant(
    tool: object,
    arguments: object,
    *,
    agent: str | None,
    signing_key_text: str,
) -> str | None:
    """Mint a proxy grant for one connector live name, or None when it is not one.

    ``mcp__<connector>__<tool>`` only. A plugin tool (``mcp__plugin_``) and a
    resume that did not carry a dict of arguments mint nothing.
    """

    if not isinstance(tool, str) or not isinstance(arguments, dict):
        return None
    if not tool.startswith("mcp__") or tool.startswith("mcp__plugin_"):
        return None
    connector, separator, upstream = tool.removeprefix("mcp__").partition("__")
    if not separator or not connector or not upstream or not agent or not signing_key_text.strip():
        return None
    return mint_connector_grant(
        signing_key_text,
        agent=agent,
        connector=connector,
        tool=upstream,
        args=json.dumps(arguments, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
        exp=int(clock.time.time()) + SANDBOX_TOKEN_TTL_SECONDS,
        jti=str(uuid.uuid4()),
    )


async def _route_and_start(
    self: Kernel,
    thread_key: str,
    event: Event,
    boot_env: dict[str, str] | None,
    packs: BehaviorPacks | None = None,
    *,
    queued_event_id: str,
    workspace_deployment_id: uuid.UUID | None = None,
    agent_name: str | None = None,
    runner_resources: dict[str, Any] | None = None,
    source: TurnSource = TurnSource.SLACK,
    remaining_s: float | None = None,
    agent_id: uuid.UUID | None = None,
    lineage_branch: str | None = None,
    lineage_head: str | None = None,
    lineage_base_sha: str | None = None,
    publication_visible_outcome_revision: int | None = None,
    force_lineage_replacement: bool = False,
    pending_publication_approval: bool = False,
    verified_review: VerifiedReviewFeedback | None = None,
    review_turn: QueuedTurn | None = None,
    workspace_inference: workspace._WorkspaceInferenceCarry,
    attachment_fresh_only: bool = False,
    approval_resume: bool = False,
    work_item_repo: str | None = None,
) -> routing._RouteResult:
    # A thread that requires a repository must establish (or confirm) it
    # before any platform response path. This deliberately precedes the
    # greeting/help shortcut: a canned reply must not create a thread whose
    # repository remains ambiguous, and a conflicting repository must be
    # refused before an existing sandbox can be adopted or steered. Generic
    # turns keep the normal claim path when the coordinator is disabled.
    # Hoisted so the new-turn return can tell whether THIS message named the
    # repository it attached (#2659); None when no selection ran.
    repo_fact: str | None = None
    workspace_repo: str | None = None
    owned_run_id = constants._OWNED_WORK_ITEM.get()
    owned_run = self._work_item_runs.get(owned_run_id) if owned_run_id is not None else None
    caller_run = str(owned_run.request_id) if owned_run is not None else None
    lineage: PublicationLineage | None = None
    if workspace_deployment_id is not None:
        # The API already bound a verified review to its persisted thread
        # workspace. Links in the untrusted review body are context, not a
        # request to select another repository. Webhook jobs (#2572) likewise
        # never parse a GitHub URL from the payload; the operator map is the
        # only coding target. An approval resume (#2828) is platform text that
        # quotes the gated tool's arguments, so ``apiVersion: batch/v1`` would
        # read as a repository; its repository is the thread's existing
        # selection, which a null request returns.
        #
        # A factory work-item execution (#2992) already knows its
        # repository: the API bound it to the WorkItem from the signed
        # delivery. The issue URL in its objective is the assignment, not a
        # repository choice, so the turn text is never parsed for it.
        if work_item_repo is not None:
            repo_fact = work_item_repo
        else:
            repo_fact = trusted_repository_fact(
                event.text,
                ignore_message=(
                    verified_review is not None or source is TurnSource.WEBHOOK or approval_resume
                ),
            )
        if self._workspace is None:
            # The coordinator is available across the worker, not a condition
            # on generic agent turns. Refuse only an event that requires a
            # workspace: its own repository fact, a carried inference from
            # this delivery, or verified feedback whose authority is bound to
            # a repository. Ambiguity still raises from
            # trusted_repository_fact above before any route is adopted.
            #
            # A bare owner/repo token is a guess (#2947) that only the
            # allowlist can confirm, and with workspaces off there is none.
            # So it names no repository and the turn runs generic (#3671),
            # rather than answering `Swap/Exchange` with chart settings. A
            # github.com URL, the carried inference and verified feedback
            # still refuse.
            if getattr(repo_fact, "bare", False):
                repo_fact = None
            if (
                repo_fact is not None
                or workspace_inference.repo is not None
                or verified_review is not None
            ):
                raise WorkspaceSelectionRefused(WORKSPACES_DISABLED_REFUSAL)
        elif source is TurnSource.WEBHOOK and webhook_job_refuses_workspace(event.text):
            workspace_repo = None
        else:
            try:
                workspace_repo = await asyncio.to_thread(
                    self._workspace.select_repository,
                    thread_key=thread_key,
                    deployment_id=workspace_deployment_id,
                    author=event.user,
                    repo_full_name=repo_fact,
                )
            except WorkspaceRepositoryNotAllowed:
                # A bare owner/repo token is a guess (#2947): `profit/loss`
                # matches. The allowlist is the ground truth for a bare
                # slug, so one outside it means no repository was named,
                # and the turn proceeds as if the message named none. A
                # URL outside the allowlist is still refused.
                if not getattr(repo_fact, "bare", False):
                    raise
                repo_fact = None
                workspace_repo = await asyncio.to_thread(
                    self._workspace.select_repository,
                    thread_key=thread_key,
                    deployment_id=workspace_deployment_id,
                    author=event.user,
                    repo_full_name=None,
                )
        if (
            lineage_branch is None
            and workspace_repo is not None
            and getattr(self, "_publication_creator", None) is not None
        ):
            reader = getattr(self._publication_creator, "get_publication_lineage", None)
            if reader is not None:
                try:
                    lineage = await reader(workspace_deployment_id, thread_key, workspace_repo)
                except (ApprovalBackendError, WorkspaceSelectionRefused):
                    if self._is_factory_work_item_turn(queued_event_id):
                        raise RunnerError("publication lineage is unavailable") from None
                    raise
                if lineage is not None and lineage.state != "open":
                    raise WorkspaceSelectionRefused(
                        "This pull request is already terminal. Start a new thread."
                    )
                if lineage is not None and (
                    lineage.has_pending_revision or lineage.has_pending_outcome
                ):
                    # A first revision has no accepted head yet, but it still
                    # owns the thread's publication boundary. Its terminal
                    # outcome must also reach durable history before a later
                    # turn can observe it. Refuse before lookup/adopt so a
                    # failed suspend cannot reuse the dirty runner across
                    # either boundary.
                    if verified_review is None:
                        raise failures.PendingPublicationError(thread_key)
                    if lineage.has_pending_outcome or verified_review.reservation_id is None:
                        raise failures.ThreadBusyError(
                            f"thread {thread_key} is waiting before its queued review"
                        )
                    # A replay may adopt only its own still-reserved origin.
                    # The final API operation below proves that identity
                    # again under fresh provider truth before model input.
                if lineage is not None and lineage.head_sha is not None:
                    lineage_branch = lineage.branch
                    lineage_head = lineage.head_sha
                if lineage is not None:
                    publication_visible_outcome_revision = lineage.visible_outcome_revision
                    if lineage.head_sha is None and lineage.visible_outcome_revision > 0:
                        lineage_base_sha = lineage.base_sha
    if verified_review is not None and (
        lineage is None
        or lineage.head_sha != verified_review.head_sha
        or lineage.version != verified_review.lineage_version
    ):
        raise WorkspaceSelectionRefused(
            "The pull request changed after GitHub feedback verification; no model turn started."
        )
    if (
        workspace_deployment_id is not None
        and self._workspace is None
        and await asyncio.to_thread(self._substrate.workspace_repository, thread_key) is not None
    ):
        # lookup() intentionally hides suspended routes. The persistent
        # repository record remains authority for this refusal, so an
        # approval resume cannot rebuild a workspace route as generic.
        raise WorkspaceSelectionRefused(WORKSPACES_DISABLED_REFUSAL)
    existing_handle = await asyncio.to_thread(self._substrate.lookup, thread_key)
    # Greeting/help pre-model short-circuit (ADR-0018): under the per-thread
    # route lock, if an enabled greeting/help pack matches the message text AND
    # the thread has no existing route, it is provably a NEW turn (it cannot be
    # a steer -- rule 1 holds by construction, since the lookup and the routing
    # both run under this same lock), so answer canned without claiming a
    # sandbox or starting a model turn. Any existing route falls through to the
    # normal claim -> steer/start_turn path below.
    # Snapshot the route under the same distributed lock that guards the
    # claim and turn-open decision. ``claim`` can evict a stale route or
    # ``_claim_or_resume`` can replace a suspended one, so the handle after
    # claiming is compared with this full snapshot rather than treating the
    # mere presence of an affinity record as proof that a live route was
    # retained.
    materialized_lineage_head = lineage_head or lineage_base_sha
    if materialized_lineage_head is not None or publication_visible_outcome_revision is not None:
        force_lineage_replacement = force_lineage_replacement or (
            existing_handle is None
            or (
                materialized_lineage_head is not None
                and existing_handle.workspace_materialized_head != materialized_lineage_head
            )
            or existing_handle.publication_visible_outcome_revision
            != publication_visible_outcome_revision
        )
    continuation = (
        source is TurnSource.CRON and sweep.parse_continuation(queued_event_id) is not None
    )
    if continuation and (
        existing_handle is None
        or force_lineage_replacement
        or (workspace_repo is not None and existing_handle.workspace_repo != workspace_repo)
        or _boots_differently(existing_handle, boot_env, caller_run=caller_run)
    ):
        # ADR-0160: a sweep continues only on the claim it already holds.
        # Any state that would claim, resume or hand off a sandbox stops it.
        raise failures.SweepClaimGone(
            f"thread {thread_key} sweep claim is gone or would be replaced"
        )
    if (
        force_lineage_replacement
        and existing_handle is not None
        and not await self._workspace_handoff_ready(
            existing_handle,
            remaining_s=remaining_s,
            lineage_reconciliation=True,
            pending_publication_approval=pending_publication_approval,
        )
    ):
        raise failures.ThreadBusyError(
            f"thread {thread_key} has not reached a durable lineage handoff boundary"
        )
    if (
        not force_lineage_replacement
        and workspace_repo is not None
        and existing_handle is not None
        and existing_handle.workspace_repo is not None
        and existing_handle.workspace_repo != workspace_repo
    ):
        raise WorkspacePreparationError(
            "route-fence", "live workspace route does not match sticky repository"
        )
    if (
        not force_lineage_replacement
        and workspace_repo is not None
        and existing_handle is not None
        and existing_handle.workspace_repo is None
        and not await self._workspace_handoff_ready(existing_handle, remaining_s=remaining_s)
    ):
        raise failures.ThreadBusyError(
            f"thread {thread_key} has not reached a durable workspace handoff boundary"
        )
    # Turn budget fence (#3071), generalized to a runner booted without a
    # caller token (ADR-0168 decision 7). CURIE_MAX_TURNS and the caller
    # token both bind only at boot, so a live route booted with a
    # different budget, or before the install had a caller key, is
    # replaced (not adopted) once it reaches the same durable handoff
    # boundary a late workspace acquisition waits for. A steerable
    # message arriving while a turn is live keeps the one-live-session
    # rule instead: it adopts and steers, and the replacement applies
    # from the next new turn.
    turn_budget_replacement = existing_handle is not None and _boots_differently(
        existing_handle, boot_env, caller_run=caller_run
    )
    if (
        turn_budget_replacement
        and existing_handle is not None
        and not source.is_job
        and verified_review is None
        and await self._turn_active(existing_handle, remaining_s=remaining_s)
    ):
        turn_budget_replacement = False
    if (
        turn_budget_replacement
        and existing_handle is not None
        and not await self._workspace_handoff_ready(existing_handle, remaining_s=remaining_s)
    ):
        raise failures.ThreadBusyError(
            f"thread {thread_key} has not reached a durable turn budget handoff boundary"
        )
    # @spec WORKER-TOOL-ACCESS-3: a restricted turn never gets a canned reply.
    if packs is not None and verified_review is None and event.tool_access is None:
        reply = match_greeting(packs, event.text) or match_help(packs, event.text)
        if reply is not None and existing_handle is None:
            return routing._RouteResult(steered=False, canned_reply=reply)
    # claim() adopts the thread's live sandbox and refreshes its route TTL
    # (so a busy thread past route_ttl is not reaped), or claims a warm one /
    # resumes a suspended one. On a fresh claim the boot env binds the agent's
    # bundle + budget; on an adopt the live sandbox is already bound, so the
    # env is ignored. Then try to steer: a live turn takes the follow-up;
    # otherwise (fresh sandbox, or the finish-race 409) we open a new turn.
    #
    # Timed separately from the model turn itself (#718): a cold claim (no
    # warm pool hit, a fresh `docker run`/pod create) and a slow model
    # response present identically to an end user ("it's just slow"), but
    # have completely different fixes (a warm pool vs. a faster/cheaper
    # model). This is the only place that can measure claim latency at
    # all -- the runner's own per-turn logging starts only once its
    # process is already up, so it cannot see the wait that got it there.
    claim_started = clock.time.monotonic()
    if continuation:
        assert existing_handle is not None  # narrowed by the refusal above
        # Refresh the route the way substrate.claim does for a live route, but
        # only while it still names this claim: a continuation adopts, never
        # claims (ADR-0160).
        if not await asyncio.to_thread(
            self._substrate.touch_live, thread_key, existing_handle.claim_name
        ):
            raise failures.SweepClaimGone(f"thread {thread_key} sweep claim moved")
        if existing_handle.workspace_repo is not None and self._workspace is not None:
            await asyncio.to_thread(
                self._workspace.touch,
                thread_key,
                ttl_seconds=self._route_ttl_seconds,
            )
        handle = existing_handle
    else:
        replace_handle = (
            existing_handle
            if existing_handle is not None
            and (
                turn_budget_replacement
                or (
                    workspace_repo is not None
                    and (force_lineage_replacement or existing_handle.workspace_repo is None)
                )
            )
            else None
        )
        # ADR 0205 decision 3: every boot for the thread rebuilds the thread's
        # files. This turn boots when there is no live route to adopt (a fresh
        # claim or a suspended resume) or the live one is being replaced (a
        # turn budget or workspace handoff). An adopt ignores claim env and
        # already holds the files, so it reads nothing. A file turn arrives
        # here with its whole set already prepared (attachment_fresh_only).
        thread_set = None
        rebuild_started = clock.time.monotonic()
        boots = existing_handle is None or replace_handle is not None
        if (
            not boots
            and not attachment_fresh_only
            and getattr(self, "_attachment_ledger", None) is not None
        ):
            # The snapshot above saw a live route, but its sandbox may be gone
            # by now (a removed container, a pod no longer Running). The claim
            # below would then evict the stale route and cold-create, and that
            # fresh boot must carry the thread's files (#4141). Re-read the
            # route with the claim's own liveness test (``lookup``'s
            # get_sandbox Running check) just before claiming: one bounded
            # control-plane read, and only where a ledger is wired. The
            # workspace claim adopts through ``substrate.adopt``, whose
            # readiness test is stricter (claim and sandbox ready), so that
            # path asks it; it evicts exactly what that claim would evict.
            # Same condition ``_claim_or_resume`` uses to take that path.
            if workspace_repo is not None and workspace_deployment_id is not None:
                boots = await asyncio.to_thread(self._substrate.adopt, thread_key) is None
            else:
                boots = await asyncio.to_thread(self._substrate.lookup, thread_key) is None
        if (
            not attachment_fresh_only
            and boots
            and not (
                boot_env
                and (ATTACHMENTS_REF_ENV in boot_env or ATTACHMENTS_MANIFEST_ENV in boot_env)
            )
        ):
            thread_set = await self._prepare_boot_thread_set(thread_key, agent_id, remaining_s)
            if thread_set is not None:
                thread_env = thread_set.claim_env()
                if thread_env:
                    # In place: the caller hands this attempt its own copy, and
                    # the caller-token cap below re-claims with this same env.
                    if boot_env is None:
                        boot_env = {}
                    boot_env.update(thread_env)
        if remaining_s is not None:
            # The rebuild ran on this delivery's clock: the claim and the turn
            # get only what is left of it.
            remaining_s = max(0.0, remaining_s - (clock.time.monotonic() - rebuild_started))
        try:
            handle = await self._claim_or_resume(
                thread_key,
                boot_env,
                workspace_deployment_id=(
                    workspace_deployment_id if workspace_repo is not None else None
                ),
                workspace_repo=workspace_repo,
                replace_handle=replace_handle,
                lineage_branch=lineage_branch,
                lineage_head=lineage_head,
                lineage_base_sha=lineage_base_sha,
                publication_visible_outcome_revision=(publication_visible_outcome_revision or 0),
                force_lineage_replacement=force_lineage_replacement,
                pending_publication_approval=pending_publication_approval,
                agent_name=agent_name,
                runner_resources=runner_resources,
                remaining_s=remaining_s,
                attachment_fresh_only=attachment_fresh_only,
                caller_run=caller_run,
            )
        except Exception:
            if thread_set is not None:
                await self._discard_unclaimed_thread_set(thread_key, thread_set, existing_handle)
            raise
    wait = current_wait()

    async def check_capacity_before_request() -> float | None:
        if wait is None:
            return None
        wait[3].raise_if_lost()
        return await wait[0].remaining_before_request(wait[1], wait[2])

    async def admit_capacity_turn(turn: TurnStream) -> None:
        if wait is None:
            return
        epoch = turn.turn_epoch
        if epoch is None:
            raise CapacityWaitRefused("capacity runner response carried no epoch")

        async def deny() -> None:
            try:
                await self._runner.admit_turn(
                    handle.base_url,
                    epoch,
                    allow=False,
                    token=handle.token or None,
                    remaining_s=2.0,
                )
            except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
                logger.warning(
                    "could not deny unadmitted capacity turn for event %s",
                    wait[1],
                    exc_info=True,
                )

        observe_until = asyncio.get_running_loop().time() + constants._CAPACITY_ADMISSION_OBSERVE_S
        while True:
            wait[3].raise_if_lost()
            try:
                budget_s = await wait[0].remaining_before_request(wait[1], wait[2])
            except (CapacityWaitExpired, CapacityWaitRefused):
                await deny()
                raise
            if asyncio.get_running_loop().time() >= observe_until:
                await deny()
                raise RunnerError("capacity runner admission was not observed")
            try:
                status = await self._runner.capacity_status(
                    handle.base_url,
                    token=handle.token or None,
                    remaining_s=min(1.0, budget_s or 1.0),
                )
            except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
                logger.warning(
                    "capacity runner status unavailable for event %s",
                    wait[1],
                    exc_info=True,
                )
            else:
                if status.get("turn_epoch") == epoch:
                    break
            await asyncio.sleep(0.05)

        wait[3].raise_if_lost()
        state = await wait[0].mark_active(wait[1], wait[2], wait[3], epoch)
        if state != "active":
            await deny()
            if state == "expired":
                raise CapacityWaitExpired()
            raise CapacityWaitRefused("wait generation changed before runner grant")
        try:
            wait[3].raise_if_lost()
            await self._runner.admit_turn(
                handle.base_url,
                epoch,
                allow=True,
                token=handle.token or None,
                remaining_s=min(2.0, wait[3].remaining_s()),
            )
        except asyncio.CancelledError:
            await self._quiesce_capacity_epoch(thread_key, epoch)
            raise
        except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
            # The runner can accept the grant and lose only its HTTP reply.
            # Query the exact epoch before deciding whether work started.
            try:
                status = await self._runner.capacity_status(
                    handle.base_url,
                    epoch=epoch,
                    token=handle.token or None,
                    remaining_s=1.0,
                )
            except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
                status = {}
            if (
                status.get("capacity_admission") is not True
                or status.get("capacity_admission_result") != "granted"
            ):
                try:
                    result = await self._quiesce_capacity_epoch(thread_key, epoch)
                except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
                    await wait[0].mark_grant_unknown(wait[1], wait[2], epoch)
                    raise
                if result == "granted":
                    await wait[0].confirm_grant(wait[1], wait[2], wait[3], epoch)
                elif result == "unknown":
                    await wait[0].mark_grant_unknown(wait[1], wait[2], epoch)
                raise CapacityWaitRefused("capacity runner grant was not confirmed") from None
        if not await wait[0].confirm_grant(wait[1], wait[2], wait[3], epoch):
            await self._quiesce_capacity_epoch(thread_key, epoch)
            raise CapacityWaitRefused("capacity runner grant lost its delivery")

    retained_live_route = existing_handle is not None and handle == existing_handle
    # #2659: announce a repository only when this message named it, the
    # server selected that same repository, and the route snapshot taken
    # under the lock did not already carry it (a fresh claim, a lost route,
    # or the late handoff from a generic route). A sticky follow-up, a
    # repeated URL on a route that already works there, and a verified
    # review (whose repo_fact is None) all announce nothing. Decided right
    # after the attach, before any steer or start_turn, and recorded into the
    # delivery's holder (see _WorkspaceInferenceCarry).
    inferred = (
        workspace_repo
        if workspace._same_repo(repo_fact, workspace_repo)
        and not workspace._same_repo(
            existing_handle.workspace_repo if existing_handle is not None else None,
            workspace_repo,
        )
        else None
    )
    if inferred is not None:
        workspace_inference.repo = inferred
    self._log_claim_latency(thread_key, claim_started)
    if wait is not None:
        # A capacity wake may meet a different live turn on this thread.
        # Keep its original deadline and retry after that turn finishes;
        # steering would inject the message before its admission grant.
        # A grant that already started is not that case. Re-parking it
        # edits the thread back to "queued" after the turn was admitted,
        # which is what a replacement sees when the previous owner's
        # runner is still marked busy or its status read blips.
        wait_record = await wait[0].get(wait[1])
        already_granted = (
            wait_record is not None
            and wait_record.state == "active"
            and wait_record.grant_confirmed
        )
        if not already_granted:
            wait_budget_s = await check_capacity_before_request()
            try:
                capacity_status = await self._runner.capacity_status(
                    handle.base_url,
                    token=handle.token or None,
                    remaining_s=min(1.0, wait_budget_s or 1.0),
                )
            except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
                logger.warning(
                    "capacity runner status unavailable for event %s",
                    wait[1],
                    exc_info=True,
                )
                raise CapacityWaitRequested() from None
            # An older runner does not implement the admission gate. Never
            # submit a capacity event to a runner that would start it directly.
            if (
                capacity_status.get("capacity_admission") is not True
                or capacity_status.get("turn_active") is not False
            ):
                raise CapacityWaitRequested()
    elif source.is_job or verified_review is not None or event.tool_access is not None:
        # ADR-0079: a job is an OUTPUT, not a steering input. A cron digest or
        # a webhook must never fold itself into whatever a person is currently
        # saying. A verified review likewise owns a separately reserved
        # publication revision. Neither path attempts a steer.
        #
        # It also must not simply open a turn and block. The runner serializes
        # turns on a semaphore, so ``start_turn`` against a busy session waits
        # for the live turn to end -- and this call runs while the kernel holds
        # the per-thread lock, so that wait would freeze the very conversation
        # the job is supposed to stay out of. Waiting there would invert the
        # rule rather than implement it.
        #
        # So ask, and defer if the answer is busy. ``turn_active`` is a plain
        # read that neither steers nor queues. There is no TOCTOU gap worth
        # guarding: the per-thread lock held across this critical section is
        # what stops another turn on this thread from opening between the read
        # and the start.
        hook_carry = constants._HOOK_RUN_CARRY.get()
        expiry = hook_carry.retry_expires_at if hook_carry is not None else None
        if expiry is not None and datetime.now(UTC) >= expiry:
            # The claim can outlast what was left of a retry's catch-up
            # bound; checked again here, right before the start (#2929).
            raise failures.CatchUpExpired(f"cron retry on {thread_key} passed its catch-up bound")
        # @spec WORKER-TOOL-ACCESS-3: a restricted turn takes this branch too.
        # Steering it would run its text under the live turn's access, and
        # an ordinary live turn has none of its restriction.
        if await self._turn_active(handle, remaining_s=remaining_s):
            deferred_kind = (
                "review"
                if verified_review is not None
                else "restricted"
                if event.tool_access is not None
                else str(source)
            )
            raise failures.LiveSessionBusy(
                f"thread {thread_key} has a live session; deferring the {deferred_kind} turn"
            )
    else:
        active_before_steer = False
        live_turn_before_steer: str | None = None
        channel_read_supported = False
        if retained_live_route:
            try:
                # NOT given ``remaining_s``: see the note above _turn_active.
                # This read runs UNDER the per-thread lock, so its bound has
                # to be a short control-plane one, not the delivery budget
                # (min(600, a 30-minute budget) is still 600). Routed
                # separately; a committed test also doubles ``status`` with a
                # base_url-only stub here.
                status = await self._runner.status(handle.base_url, token=handle.token or None)
            except Exception as exc:  # noqa: BLE001 -- steering still decides the route
                logger.warning(
                    "could not read pre-steer turn liveness at %s: %r",
                    handle.base_url,
                    exc,
                )
            else:
                active = status.get("turn_active")
                if isinstance(active, bool):
                    active_before_steer = active
                else:
                    logger.warning(
                        "runner pre-steer status carried no usable turn_active: %r",
                        status,
                    )
                # The live turn this steer would join (#3776). The read and
                # the steer both run under the per-thread lock, so no other
                # attempt can open a turn on this thread between them.
                live_turn_before_steer = memory._live_memory_turn(status.get("turn_epoch"))
                # ADR 0100: renew the channel read capability only for a
                # runner that enforces it; any other answer clears it.
                channel_read_supported = status.get(CHANNEL_READ_STATUS_FIELD) is True
        steer_event = event
        mint = constants._MEMORY_MINT.get()
        if mint is not None and retained_live_route and active_before_steer:
            # ADR-0188: a steer ends when the live turn ends, so its
            # credential never outlives that turn's stream deadline. A turn
            # another worker opened has no recorded deadline here; the
            # steer's own bound applies. Minted only when the retained
            # sandbox reports a live turn to fold into: a fresh, replacement
            # or idle runner refuses the probe, and a credential minted for
            # it would be one more claim to close (#3776).
            steer_event = self._with_memory_token(
                event,
                mint.qevent,
                mint.grant,
                remaining_s,
                cap_at=self._turn_deadlines.get(mint.grant.thread_key),
            )
        # ADR 0100: a steer renews the live logical turn's channel read
        # capability, or sends null, which clears the runner's. It never
        # revokes, delivered or not: its generation lives under the opener's
        # owner and dies with the opener's revoke.
        steer_event = await self._steer_channel_read(
            steer_event,
            renew=retained_live_route and active_before_steer and channel_read_supported,
            thread_key=thread_key,
            remaining_s=remaining_s,
        )
        steered = await self._runner.steer(
            handle.base_url, steer_event, token=handle.token or None, remaining_s=remaining_s
        )
        if steered:
            memory_turns = constants._MEMORY_TURNS.get()
            if memory_turns is not None:
                memory_turns.steered_into = live_turn_before_steer
            routing._record_route("steer")
            routing._lifecycle_event("runner.turn.steered", "steer")
            return routing._RouteResult(steered=True)
        # A 409 is a finish race only when the route-lock snapshot and the
        # claim resolve to the same retained live sandbox. A fresh or
        # replacement runner is expected to refuse its first steer probe;
        # counting that bootstrap condition as a race would make every new
        # turn inflate the operator signal.
        if retained_live_route and active_before_steer:
            routing._record_route("finish-race")
            routing._lifecycle_event("runner.finish_race", "finish-race")
        # Turn budget fence (#3071), finish-race side, generalized to a
        # runner booted without a caller token (ADR-0168 decision 7). The
        # route was kept only to steer a live turn; that turn ended (or
        # its liveness was unreadable), and CURIE_MAX_TURNS and the
        # caller token both bind at boot, so a new turn must not open on
        # this runner. Retry: the redelivery finds the turn idle and
        # takes the replacement path above.
        if _boots_differently(handle, boot_env, caller_run=caller_run):
            raise failures.ThreadBusyError(
                f"thread {thread_key} turn ended before its steer; "
                "retrying to replace the runner's turn budget or caller token"
            )
    if verified_review is not None:
        reserver = getattr(self._publication_creator, "reserve_review_feedback", None)
        if (
            review_turn is None
            or workspace_deployment_id is None
            or verified_review.origin_key != review_turn.event_id
            or reserver is None
        ):
            raise WorkspaceSelectionRefused("GitHub feedback revision identity was refused.")
        try:
            # This is deliberately independent of delivery `remaining_s`.
            # The API freshly re-reads GitHub and CAS-reserves the lineage;
            # bound the entire in-lock operation even for transports whose
            # per-request timeout is ineffective.
            async with asyncio.timeout(constants._REVIEW_RESERVE_CONTROL_PLANE_TIMEOUT_S):
                reservation_id = await reserver(
                    review_turn, workspace_deployment_id, verified_review
                )
        except (ApprovalBackendError, TimeoutError):
            raise ReviewAuthorityUnavailable(
                "GitHub review reservation is temporarily unavailable"
            ) from None
        if not isinstance(reservation_id, uuid.UUID) or (
            verified_review.reservation_id is not None
            and reservation_id != verified_review.reservation_id
        ):
            raise WorkspaceSelectionRefused("GitHub feedback revision identity was refused.")
    # The per-request timeout is min(runner_total_timeout_s, remaining
    # delivery budget): the budget can only ever SHORTEN a request, never
    # grant one more time than the delivery has left (ADR-0131).
    # Register before start_turn so a kill during the POST can find this
    # thread. Canned and steered returns above never register. A failed
    # start unregisters so a turn that never opened cannot leak an entry.
    runs = getattr(self, "_work_item_runs", {})
    owned_id = constants._OWNED_WORK_ITEM.get()
    run = runs.get(owned_id) if owned_id is not None else None
    if run is None:
        for candidate in runs.values():
            if candidate.thread_key == thread_key and candidate.started and not candidate.finished:
                run = candidate
                break
    if run is not None and run.finished:
        raise WorkItemStartRefused("work item authority is finished")
    if run is not None and not run.started:
        started = await run.start(
            claim_name=handle.claim_name,
            sandbox_name=handle.sandbox_name,
        )
        remaining_s = (
            started.remaining_s if remaining_s is None else min(remaining_s, started.remaining_s)
        )
        handle = await self._cap_caller_token_to_deadline(
            thread_key,
            boot_env,
            handle,
            run,
            agent_name=agent_name,
            workspace_deployment_id=workspace_deployment_id,
            workspace_repo=workspace_repo,
            lineage_branch=lineage_branch,
            lineage_head=lineage_head,
            lineage_base_sha=lineage_base_sha,
            publication_visible_outcome_revision=publication_visible_outcome_revision,
            runner_resources=runner_resources,
            remaining_s=remaining_s,
        )
    elif run is not None:
        remaining_s = run.bound_remaining_s(remaining_s)
        handle = await self._cap_caller_token_to_deadline(
            thread_key,
            boot_env,
            handle,
            run,
            agent_name=agent_name,
            workspace_deployment_id=workspace_deployment_id,
            workspace_repo=workspace_repo,
            lineage_branch=lineage_branch,
            lineage_head=lineage_head,
            lineage_base_sha=lineage_base_sha,
            publication_visible_outcome_revision=publication_visible_outcome_revision,
            runner_resources=runner_resources,
            remaining_s=remaining_s,
        )
    if run is not None and agent_id is not None:
        # Lets a kill find this run if it later parks for approval (#3564).
        run.agent_id = agent_id
    if agent_id is not None:
        self._register_run(agent_id, thread_key)
    turn: TurnStream | None = None
    try:
        event, remaining_s = await self._bind_publication_context(
            event,
            queued_event_id=queued_event_id,
            workspace_deployment_id=workspace_deployment_id,
            handle=handle,
            run=run,
            remaining_s=remaining_s,
        )
        wait_budget_s = await check_capacity_before_request()
        if wait is None:
            turn = await self._start_turn_under_hook_control(handle, event, remaining_s)
        elif wait_budget_s is None:
            turn = await self._start_turn_under_hook_control(
                handle, event, remaining_s, capacity_admission=True
            )
        else:
            async with asyncio.timeout(wait_budget_s):
                turn = await self._start_turn_under_hook_control(
                    handle, event, remaining_s, capacity_admission=True
                )
        await admit_capacity_turn(turn)
    except BaseException as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
        self._unregister_run(agent_id, thread_key)
        if turn is not None:
            turn.close()
        if wait is not None and isinstance(exc, TimeoutError):
            if turn is not None and turn.turn_epoch is not None:
                await self._quiesce_capacity_epoch(thread_key, turn.turn_epoch)
            if await wait[0].check_delivery(wait[1], wait[2]) == "expired":
                raise CapacityWaitExpired() from None
        raise
    assert turn is not None
    routing._record_route("start")
    routing._lifecycle_event("runner.turn.started", "start")
    # The inference decided at the claim above is this route's own value.
    return routing._RouteResult(
        steered=False, handle=handle, turn=turn, workspace_inferred_repo=inferred
    )


def _log_claim_latency(thread_key: str, claim_started: float) -> None:
    claim_ms = round((clock.time.monotonic() - claim_started) * 1000)
    logger.info("claim latency for %s: %d ms", thread_key, claim_ms)


async def _turn_active(
    self: Kernel, handle: SandboxHandle, *, remaining_s: float | None = None
) -> bool:
    """Is a turn live in this sandbox right now?

    The read a job uses to decide whether to run or defer. It is a plain GET
    that neither steers nor queues, which is the whole reason it exists: the
    two calls that could otherwise answer the question both have side effects
    (``steer`` folds the job into the live conversation, ``start_turn`` blocks
    on the runner's turn semaphore while holding the kernel's thread lock).

    Fails CLOSED. An unreachable runner or an answer without a usable
    ``turn_active`` reports busy, so an unreadable session defers the job
    rather than opening a turn beside one that may already be running. A
    deferred job is redelivered; two live turns on one thread would break the
    kernel's first invariant.

    Args:
        handle: The claimed sandbox for this thread.
        remaining_s: Wall-clock budget the caller has left, or None for a
            caller that holds no budget. It only ever SHORTENS the probe's
            HTTP bound (``min`` with the client's ceiling): without it this
            GET inherits the 600s streaming session total, so one wedged
            runner costs the caller that whole window inside a single call.

    Returns:
        True when a turn is live, or when liveness could not be determined.
    """
    try:
        status = await self._runner.status(
            handle.base_url,
            token=handle.token or None,
            remaining_s=remaining_s,
        )
    except Exception as exc:  # noqa: BLE001 -- any unreadable answer means "assume busy"
        logger.warning("could not read turn liveness at %s: %r", handle.base_url, exc)
        return True
    active = status.get("turn_active")
    if not isinstance(active, bool):
        logger.warning("runner status carried no usable turn_active: %r", status)
        return True
    return active


async def _cap_caller_token_to_deadline(
    self: Kernel,
    thread_key: str,
    boot_env: dict[str, str] | None,
    handle: SandboxHandle,
    run: WorkItemRun,
    *,
    agent_name: str | None,
    workspace_deployment_id: uuid.UUID | None,
    workspace_repo: str | None,
    lineage_branch: str | None,
    lineage_head: str | None,
    lineage_base_sha: str | None,
    publication_visible_outcome_revision: int | None,
    runner_resources: dict[str, Any] | None,
    remaining_s: float | None,
) -> SandboxHandle:
    """Replace a runner whose caller token outlives the committed deadline.

    Writes the corrected token back onto ``boot_env`` so a later claim of
    this same env, including a retry after the request has already started,
    does not restore the provisional expiry.
    """

    if boot_env is None or run.execution_deadline is None:
        return handle
    if not self._config.connector_caller_signing_key.strip():
        return handle
    if not isinstance(agent_name, str) or not agent_name:
        return handle
    token = boot_env.get(CONNECTOR_CALLER_TOKEN_ENV)
    signed_exp = _caller_token_exp(token) if isinstance(token, str) else None
    deadline_unix = int(run.execution_deadline.timestamp())
    if signed_exp is None or signed_exp <= deadline_unix:
        return handle
    boot_env[CONNECTOR_CALLER_TOKEN_ENV] = caller_token.mint(
        self._config.connector_caller_signing_key,
        agent=agent_name,
        exp=min(int(clock.time.time()) + SANDBOX_TOKEN_TTL_SECONDS, deadline_unix),
        run=str(run.request_id),
        work_item=str(run.work_item_id),
    )
    replaced = await self._claim_or_resume(
        thread_key,
        boot_env,
        workspace_deployment_id=(workspace_deployment_id if workspace_repo is not None else None),
        workspace_repo=workspace_repo,
        replace_handle=handle,
        lineage_branch=lineage_branch,
        lineage_head=lineage_head,
        lineage_base_sha=lineage_base_sha,
        publication_visible_outcome_revision=(publication_visible_outcome_revision or 0),
        agent_name=agent_name,
        runner_resources=runner_resources,
        remaining_s=remaining_s,
        caller_run=str(run.request_id),
    )
    run.claim_name = replaced.claim_name
    run.sandbox_name = replaced.sandbox_name
    return replaced


async def _claim_or_resume(
    self: Kernel,
    thread_key: str,
    boot_env: dict[str, str] | None,
    *,
    workspace_deployment_id: uuid.UUID | None = None,
    workspace_repo: str | None = None,
    replace_handle: SandboxHandle | None = None,
    lineage_branch: str | None = None,
    lineage_head: str | None = None,
    lineage_base_sha: str | None = None,
    publication_visible_outcome_revision: int = 0,
    force_lineage_replacement: bool = False,
    pending_publication_approval: bool = False,
    agent_name: str | None = None,
    runner_resources: dict[str, Any] | None = None,
    remaining_s: float | None = None,
    attachment_fresh_only: bool = False,
    caller_run: str | None = None,
) -> SandboxHandle:
    wait = current_wait()
    previous_handle: SandboxHandle | None = None
    if wait is not None:
        wait_state = await wait[0].check_delivery(wait[1], wait[2])
        if wait_state == "expired":
            raise CapacityWaitExpired()
        if wait_state != "ready":
            raise CapacityWaitRefused("wait generation changed before claim")
        previous_handle = await asyncio.to_thread(self._substrate.lookup, thread_key)

    async def validate_wait_claim(handle: SandboxHandle) -> SandboxHandle:
        if wait is None:
            return handle
        state = await wait[0].check_delivery(wait[1], wait[2])
        if state == "ready":
            return handle
        if previous_handle is None:
            try:
                await asyncio.to_thread(self._substrate.release, thread_key)
            except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
                logger.warning(
                    "could not release an unstarted sandbox for event %s",
                    wait[1],
                    exc_info=True,
                )
        if state == "expired":
            raise CapacityWaitExpired()
        raise CapacityWaitRefused("wait generation changed after claim")

    # A live route is an adopt/steer, not a session start. Preparing before
    # this check would clone on every threaded steer and could even replace
    # the base object while the existing sandbox is still using it. The
    # substrate's adopt primitive both validates and touches an existing
    # route without ever cold-creating one; this avoids the old
    # lookup-then-claim gap, where the route could disappear and ``claim``
    # would create a sandbox without a freshly prepared workspace ref.
    if workspace_deployment_id is not None:
        handoff_budget_started = clock.time.monotonic()

        def handoff_remaining() -> float | None:
            if remaining_s is None:
                return None
            return max(
                0.0,
                remaining_s - (clock.time.monotonic() - handoff_budget_started),
            )

        if self._workspace is None:
            raise WorkspacePreparationError(
                "wiring", "selected workspace has no trusted claim-time preparer"
            )
        existing = None
        if not force_lineage_replacement:
            existing = await asyncio.to_thread(self._substrate.adopt, thread_key)
        if attachment_fresh_only and existing is not None:
            # A runner appeared after the attachment lookup; it never saw
            # the staged files, so refuse rather than adopt it (#2739).
            raise RouteChangedError(thread_key)
        if (
            existing is not None
            and existing.workspace_repo == workspace_repo
            # A route the caller fenced for replacement (#3071 turn
            # budget) is never adopted.
            and existing != replace_handle
        ):
            await asyncio.to_thread(
                self._workspace.touch,
                thread_key,
                ttl_seconds=self._route_ttl_seconds,
            )
            return await validate_wait_claim(existing)
        handoff_revalidation: Callable[[], None] | None = None
        candidate_validation: Callable[[SandboxHandle], None] | None = None
        if replace_handle is not None:
            loop = asyncio.get_running_loop()

            def run_status_probe(
                probe_factory: Callable[[float | None], Coroutine[Any, Any, bool]],
                *,
                failure: str,
            ) -> bool:
                """Run one bounded runner probe from the coordinator thread."""

                probe_remaining_s = handoff_remaining()
                probe = asyncio.run_coroutine_threadsafe(probe_factory(probe_remaining_s), loop)
                request_ceiling = self._config.runner_total_timeout_s
                if probe_remaining_s is not None:
                    request_ceiling = max(0.0, min(request_ceiling, probe_remaining_s))
                try:
                    return probe.result(
                        timeout=(request_ceiling + constants._HANDOFF_REVALIDATION_BRIDGE_GRACE_S)
                    )
                except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
                    probe.cancel()
                    raise failures.ThreadBusyError(failure) from exc

            def revalidate_before_handoff() -> None:
                """Bridge the coordinator thread back to the runner's loop.

                The coordinator invokes this after preparation and durable
                ownership staging, immediately before ``substrate.handoff``.
                Blocking here is safe: ``_claim_or_resume`` is awaiting the
                coordinator via ``to_thread``, so the owning event loop is
                free to service the authenticated status request.
                """

                ready = run_status_probe(
                    lambda probe_remaining_s: self._workspace_handoff_ready(
                        replace_handle,
                        remaining_s=probe_remaining_s,
                        lineage_reconciliation=force_lineage_replacement,
                        pending_publication_approval=pending_publication_approval,
                    ),
                    failure=(
                        f"thread {thread_key} workspace handoff fence could not be revalidated"
                    ),
                )
                if not ready:
                    raise failures.ThreadBusyError(
                        f"thread {thread_key} lost its durable workspace "
                        "handoff boundary during preparation"
                    )

            handoff_revalidation = revalidate_before_handoff

            def validate_candidate(candidate: SandboxHandle) -> None:
                """Attest the newly ready runner before the substrate route CAS."""

                ready = run_status_probe(
                    lambda probe_remaining_s: self._workspace_candidate_ready(
                        candidate, remaining_s=probe_remaining_s
                    ),
                    failure=(
                        f"thread {thread_key} workspace handoff candidate could not be attested"
                    ),
                )
                if not ready:
                    raise failures.ThreadBusyError(
                        f"thread {thread_key} workspace handoff candidate "
                        "did not attest the expected managed checkout"
                    )

            candidate_validation = validate_candidate
        # Prepare once, then let the substrate decide cold claim versus
        # suspended-route resume. Either branch materializes the same fresh,
        # verified archive before the runner can start.
        workspace_claim = await asyncio.to_thread(
            self._workspace.claim_or_resume_with_handle,
            thread_key=thread_key,
            deployment_id=workspace_deployment_id,
            env=boot_env,
            agent_name=agent_name,
            runner_resources=runner_resources,
            repo_full_name=workspace_repo,
            replace_handle=replace_handle,
            revalidate_before_handoff=handoff_revalidation,
            validate_candidate=candidate_validation,
            lineage_branch=lineage_branch,
            lineage_head=lineage_head,
            lineage_base_sha=lineage_base_sha,
            publication_visible_outcome_revision=(publication_visible_outcome_revision),
            fresh_only=attachment_fresh_only,
            caller_run=caller_run,
        )
        if not isinstance(workspace_claim.handle, SandboxHandle):
            raise WorkspacePreparationError(
                "claim", "workspace substrate returned an invalid sandbox handle"
            )
        return await validate_wait_claim(workspace_claim.handle)
    if replace_handle is not None:
        # Turn budget fence (#3071): the caller proved the old runner idle
        # with durable history; hand the route to a runner booted with this
        # delivery's env, keeping the session and history identity.
        handle = await asyncio.to_thread(
            self._substrate.handoff,
            thread_key,
            expected=replace_handle,
            env=boot_env or {},
            workspace_repo=None,
            agent_name=agent_name,
            runner_resources=runner_resources,
            caller_run=caller_run,
        )
        return await validate_wait_claim(handle)
    try:
        handle = await asyncio.to_thread(
            self._substrate.claim,
            thread_key,
            env=boot_env,
            agent_name=agent_name,
            runner_resources=runner_resources,
            fresh_only=attachment_fresh_only,
            caller_run=caller_run,
        )
        return await validate_wait_claim(handle)
    except SuspendedThreadError:
        # Resume with the same bound boot env a fresh claim gets (bundle
        # ref, budget, refs): a suspended pod was deleted (ADR-0003), so
        # the replacement boots from env alone; without this it would come
        # up generic, without the agent's bundle.
        handle = await asyncio.to_thread(
            self._substrate.resume,
            thread_key,
            env=boot_env,
            agent_name=agent_name,
            runner_resources=runner_resources,
            caller_run=caller_run,
        )
        return await validate_wait_claim(handle)


@contextlib.asynccontextmanager
async def _keep_route_alive(self: Kernel, thread_key: str, claim_name: str) -> AsyncIterator[None]:
    """Refresh the thread route while a turn streams (#3188).

    The route is written with ``route_ttl_seconds`` on claim/adopt; a turn
    that outlives it lost its route and the reaper deleted the claim
    mid-turn. A failed refresh is logged and retried, never fatal.
    """

    # Strictly inside the TTL for every positive TTL, so the first refresh
    # never races the expiry it exists to prevent.
    interval = self._route_ttl_seconds / 3

    async def _loop() -> None:
        while True:
            await asyncio.sleep(interval)
            try:
                touched = await asyncio.to_thread(
                    self._substrate.touch_live, thread_key, claim_name
                )
                if touched and self._workspace is not None:
                    await asyncio.to_thread(
                        self._workspace.touch,
                        thread_key,
                        ttl_seconds=self._route_ttl_seconds,
                    )
            except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
                logger.warning("route keepalive failed for thread %s", thread_key, exc_info=True)

    task = asyncio.create_task(_loop())
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
