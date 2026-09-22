"""Operator read model for factory work item outcomes (#2577).

The single home of outcome semantics: ``derive_outcome`` is the only place an
outcome ``state`` and ``actionable_cause`` are computed, and the CLI and the
console render those strings verbatim. The module reads canonical WorkItem,
ExecutionRequest, publication-lineage, Publication and Approval rows; it
writes nothing and adds no store.

Output is an allowlist: every view is built from explicit fields below, never
from an ORM row, so a runtime-owner token, a reply transport address, a patch,
a publication body/error or an approval summary cannot reach an operator even
when a new column is added later. CI is observed live on the detail route
only and never persisted; any failure to observe is ``unavailable`` with a
fixed reason code that never carries a response body, header, URL or token.
"""

from __future__ import annotations

import asyncio
import re
import threading
import uuid
from collections import defaultdict
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from typing import Any

import anyio
import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from .config import Settings
from .github_app import GitHubAppError, GitHubInstallationRefused, credentials_for
from .models import (
    Approval,
    ExecutionRequest,
    Publication,
    ThreadPublicationLineage,
    WorkItem,
)
from .repo_full_name import repo_url_path
from .schemas import (
    WorkItemCiOut,
    WorkItemCorrectnessOut,
    WorkItemOutcomeOut,
    WorkItemOutcomeState,
    WorkItemPrOut,
    WorkItemPublicationOut,
    WorkItemRequestOut,
)

OBJECTIVE_LIMIT = 512
CHECK_RUNS_PAGE = 100
# One overall bound on a live CI observation: credential acquisition (lock,
# discovery, token mint) and the check-runs request together.
CI_OBSERVATION_DEADLINE_SECONDS = 10.0
# How many credential mints a live CI observation may have in flight across all
# callers. A caller abandoned at the deadline cannot cancel the mint thread, so
# the slot is released by the thread itself when the mint actually finishes;
# until then further callers are refused ``observation_busy`` without spawning
# work, which is what keeps repeated timeouts from accumulating threads and
# lock contention.
CI_CREDENTIAL_SLOTS = 4
_CI_CREDENTIAL_GUARD = threading.BoundedSemaphore(CI_CREDENTIAL_SLOTS)

_PUBLISHING = frozenset({"approved", "launching", "running"})
_FAILING_CONCLUSIONS = frozenset(
    {"failure", "timed_out", "cancelled", "action_required", "startup_failure"}
)
_PASSING_CONCLUSIONS = frozenset({"success", "neutral", "skipped"})
_SHA_RE = re.compile(r"[0-9a-fA-F]{7,64}")

CiObservation = WorkItemCiOut


# --- derivation (pure) -------------------------------------------------------


def _iso(value: datetime | None) -> str:
    return value.isoformat() if value is not None else "unknown"


def _cause(
    state: WorkItemOutcomeState,
    req: ExecutionRequest | None,
    lineage: ThreadPublicationLineage | None,
    publication: Publication | None,
    approval: Approval | None,
    now: datetime,
) -> str:
    if req is None:
        if state == "cancelled":
            return "issue cancelled before any execution request was recorded"
        return (
            "no execution request recorded yet; an authorized issue mention "
            "admits one"
        )
    cause = req.terminal_cause
    if state == "cancellation_requested":
        reasons = {
            "issue_cancelled": "the issue was cancelled (label removed or issue closed)",
            "execution_deadline": "the 1800 s execution deadline elapsed",
            "owner_lost": "the runtime owner stopped heartbeating",
        }
        return (
            f"cancellation requested: {cause} ({reasons.get(cause or '', 'unknown')}); "
            "termination is awaiting a runtime observation"
        )
    if state == "waiting":
        text = "waiting for sandbox capacity"
        if req.capacity_deferrals:
            text += (
                f"; deferred {req.capacity_deferrals} time(s) for capacity, "
                f"last reason: {req.last_deferral_reason or 'unknown'}"
            )
        if now >= req.wait_deadline:
            return (
                f"{text}; the waiting deadline elapsed at {_iso(req.wait_deadline)} "
                "and expiry is pending the reconciler"
            )
        return f"{text}; waiting deadline {_iso(req.wait_deadline)}"
    if state == "running":
        return (
            f"running since {_iso(req.started_at)}, bounded by the execution "
            f"deadline {_iso(req.execution_deadline)}"
        )
    if state == "cancelled":
        text = "cancelled: the issue label was removed or the issue was closed"
        if lineage is not None and lineage.pr_number is not None:
            text += f"; pull request #{lineage.pr_number} is retained"
        return text
    if state == "expired":
        if cause == "capacity_wait_expired":
            return (
                "expired: capacity_wait_expired, no sandbox capacity before the "
                "waiting deadline"
            )
        return (
            f"expired: {cause}, the execution exceeded 1800 s and termination "
            "was observed"
        )
    if state == "failed":
        text = f"failed: {cause}"
        if cause == "deadline_halted":
            text += (
                "; the run hit the delivery budget, raise "
                "worker.deliveryBudgetSeconds if the work needs longer"
            )
        return text
    if state == "awaiting_approval":
        if approval is not None and approval.status == "pending":
            return (
                "publication approval pending; resolve it with "
                "`curie <local|cluster> approvals <agent> --list`"
            )
        return (
            "a tool approval is pending on this conversation; resolve it with "
            "`curie <local|cluster> approvals <agent> --list`"
        )
    if state == "publishing":
        assert publication is not None
        if lineage is not None and lineage.pr_number is not None:
            return (
                f"publication revision in progress ({publication.status}); "
                f"updating pull request #{lineage.pr_number}"
            )
        return (
            f"publication in progress ({publication.status}); the pull request "
            "has not been opened yet"
        )
    if state == "published":
        assert lineage is not None
        return (
            f"pull request #{lineage.pr_number} opened; CI and review are the "
            "next signals"
        )
    # completed_unpublished
    if publication is None:
        return "execution completed without a publication"
    return f"execution completed; publication {publication.status}, no pull request"


def derive_outcome(
    item: WorkItem,
    requests: Sequence[ExecutionRequest],
    lineage: ThreadPublicationLineage | None,
    publication: Publication | None,
    approval: Approval | None,
    pending_turn_approval: bool,
    now: datetime,
    *,
    issue_base: str,
) -> WorkItemOutcomeOut:
    """Derive one operator view. Pure: no I/O. ``ci`` is left null."""

    ordered = sorted(requests, key=lambda r: r.sequence)
    req = ordered[-1] if ordered else None
    state: WorkItemOutcomeState
    if req is None:
        state = "cancelled" if item.cancelled_at is not None else "waiting"
    elif req.status == "cancellation_requested":
        state = "cancellation_requested"
    elif req.status == "waiting":
        state = "waiting"
    elif req.status == "running" and (
        (approval is not None and approval.status == "pending") or pending_turn_approval
    ):
        # Publication and tool approvals happen while the execution is still
        # running. Completion is refused until a pull request is open.
        state = "awaiting_approval"
    elif (
        req.status == "running"
        and publication is not None
        and publication.status in _PUBLISHING
    ):
        state = "publishing"
    elif (
        req.status == "running"
        and publication is not None
        and publication.status == "denied"
    ):
        state = "completed_unpublished"
    elif req.status == "running":
        state = "running"
    elif req.status == "cancelled" or item.cancelled_at is not None:
        state = "cancelled"
    elif req.status == "expired":
        state = "expired"
    elif req.status == "failed":
        state = "failed"
    elif (approval is not None and approval.status == "pending") or pending_turn_approval:
        state = "awaiting_approval"
    elif publication is not None and publication.status in _PUBLISHING:
        state = "publishing"
    elif lineage is not None and lineage.pr_number is not None:
        state = "published"
    else:
        state = "completed_unpublished"

    snapshot = next((r for r in reversed(ordered) if r.objective is not None), None)
    objective = snapshot.objective if snapshot is not None else None
    truncated = objective is not None and len(objective) > OBJECTIVE_LIMIT
    pr = (
        WorkItemPrOut(
            number=lineage.pr_number, url=lineage.pr_url or "", status=lineage.status
        )
        if lineage is not None and lineage.pr_number is not None
        else None
    )
    return WorkItemOutcomeOut(
        id=item.id,
        agent_id=item.agent_id,
        repo_full_name=item.repo_full_name,
        github_issue_number=item.github_issue_number,
        issue_url=(
            f"{issue_base.rstrip('/')}/{item.repo_full_name}"
            f"/issues/{item.github_issue_number}"
        ),
        cancelled_at=item.cancelled_at,
        created_at=item.created_at,
        updated_at=item.updated_at,
        state=state,
        actionable_cause=_cause(state, req, lineage, publication, approval, now),
        objective=objective[:OBJECTIVE_LIMIT] if objective is not None else None,
        objective_truncated=truncated,
        requester=snapshot.requester if snapshot is not None else None,
        pr=pr,
        publication=(
            WorkItemPublicationOut(
                status=publication.status,
                revision_number=publication.revision_number,
                approval_status=approval.status if approval is not None else None,
            )
            if publication is not None
            else None
        ),
        correctness=WorkItemCorrectnessOut(),
        ci=None,
        requests=[_request_view(r) for r in ordered],
    )


_ABSENT_AT_RE = re.compile(r"(?:^|\s)absent_at=(\S+)")


def _safe_termination(observation: str | None) -> str | None:
    """Project a raw termination observation to a fixed, safe form.

    The raw string carries the observer (the runtime owner) and runtime object
    names; only the fact of the observation and its absence time are exposed.
    """

    if observation is None:
        return None
    match = _ABSENT_AT_RE.search(observation)
    if match is not None:
        try:
            absent_at = datetime.fromisoformat(match.group(1)).isoformat()
        except ValueError:
            absent_at = None
        if absent_at is not None:
            return f"runtime termination observed; absent at {absent_at}"
    return "runtime termination observed"


def _request_view(req: ExecutionRequest) -> WorkItemRequestOut:
    return WorkItemRequestOut(
        sequence=req.sequence,
        status=req.status,
        created_at=req.created_at,
        wait_deadline=req.wait_deadline,
        started_at=req.started_at,
        execution_deadline=req.execution_deadline,
        terminal_at=req.terminal_at,
        terminal_cause=req.terminal_cause,
        termination_observation=_safe_termination(req.termination_observation),
        capacity_deferrals=req.capacity_deferrals,
        last_deferral_reason=req.last_deferral_reason,
    )


# --- loaders -------------------------------------------------------------------


def _pick_lineage(
    item: WorkItem, lineages: Iterable[ThreadPublicationLineage]
) -> ThreadPublicationLineage | None:
    """The linked lineage when set; otherwise the conversation's lineage for the
    same agent and repository, open first, then newest (D2: nothing links
    ``publication_lineage_id`` in production yet)."""

    candidates = list(lineages)
    if item.publication_lineage_id is not None:
        return next((x for x in candidates if x.id == item.publication_lineage_id), None)
    matching = [
        x
        for x in candidates
        if x.agent_id == item.agent_id
        and x.conversation_id == item.conversation_id
        and x.repo_full_name.lower() == item.repo_full_name.lower()
    ]
    if not matching:
        return None
    return max(matching, key=lambda x: (x.status == "open", x.created_at))


async def _views(
    session: AsyncSession, items: Sequence[WorkItem], settings: Settings
) -> list[tuple[WorkItemOutcomeOut, ThreadPublicationLineage | None]]:
    if not items:
        return []
    now = (await session.execute(select(func.now()))).scalar_one()
    ids = [item.id for item in items]
    requests: dict[uuid.UUID, list[ExecutionRequest]] = defaultdict(list)
    for req in (
        await session.scalars(
            select(ExecutionRequest).where(ExecutionRequest.work_item_id.in_(ids))
        )
    ).all():
        requests[req.work_item_id].append(req)

    linked = {i.publication_lineage_id for i in items if i.publication_lineage_id}
    lineage_rows: list[ThreadPublicationLineage] = []
    if linked:
        lineage_rows.extend(
            (
                await session.scalars(
                    select(ThreadPublicationLineage).where(
                        ThreadPublicationLineage.id.in_(linked)
                    )
                )
            ).all()
        )
    unlinked = [i for i in items if i.publication_lineage_id is None]
    if unlinked:
        lineage_rows.extend(
            (
                await session.scalars(
                    select(ThreadPublicationLineage).where(
                        ThreadPublicationLineage.agent_id.in_(
                            {i.agent_id for i in unlinked}
                        ),
                        ThreadPublicationLineage.conversation_id.in_(
                            {i.conversation_id for i in unlinked}
                        ),
                    )
                )
            ).all()
        )
    chosen = {item.id: _pick_lineage(item, lineage_rows) for item in items}

    lineage_ids = {x.id for x in chosen.values() if x is not None}
    latest_pub: dict[uuid.UUID, Publication] = {}
    if lineage_ids:
        for pub in (
            await session.scalars(
                select(Publication).where(Publication.lineage_id.in_(lineage_ids))
            )
        ).all():
            assert pub.lineage_id is not None
            current = latest_pub.get(pub.lineage_id)
            if current is None or (pub.created_at, pub.revision_number or 0) > (
                current.created_at,
                current.revision_number or 0,
            ):
                latest_pub[pub.lineage_id] = pub
    approvals: dict[uuid.UUID, Approval] = {}
    if latest_pub:
        for approval in (
            await session.scalars(
                select(Approval).where(
                    Approval.id.in_({p.approval_id for p in latest_pub.values()})
                )
            )
        ).all():
            approvals[approval.id] = approval

    pending_tuples: set[tuple[uuid.UUID, str, str, str]] = set()
    for approval in (
        await session.scalars(
            select(Approval).where(
                Approval.status == "pending",
                Approval.agent_id.in_({i.agent_id for i in items}),
            )
        )
    ).all():
        assert approval.agent_id is not None
        pending_tuples.add(
            (
                approval.agent_id,
                approval.reply_kind,
                approval.reply_channel,
                approval.conversation_id,
            )
        )

    views = []
    for item in items:
        item_requests = requests.get(item.id, [])
        latest = max(item_requests, key=lambda r: r.sequence, default=None)
        pending_turn = (
            latest is not None
            and latest.reply_kind is not None
            and latest.reply_address is not None
            and latest.reply_conversation_id is not None
            and (
                item.agent_id,
                latest.reply_kind,
                latest.reply_address,
                latest.reply_conversation_id,
            )
            in pending_tuples
        )
        lineage = chosen[item.id]
        chosen_pub = latest_pub.get(lineage.id) if lineage is not None else None
        views.append((
            derive_outcome(
                item,
                item_requests,
                lineage,
                chosen_pub,
                approvals.get(chosen_pub.approval_id) if chosen_pub is not None else None,
                pending_turn,
                now,
                issue_base=settings.github_clone_base,
            ),
            lineage,
        ))
    return views


async def load_outcomes(
    session: AsyncSession,
    *,
    agent_id: uuid.UUID | None,
    limit: int,
    settings: Settings,
) -> tuple[list[WorkItemOutcomeOut], bool]:
    """Newest-updated first; fetches ``limit + 1`` to report truncation."""

    query = select(WorkItem).order_by(WorkItem.updated_at.desc(), WorkItem.id)
    if agent_id is not None:
        query = query.where(WorkItem.agent_id == agent_id)
    items = list((await session.scalars(query.limit(limit + 1))).all())
    truncated = len(items) > limit
    views = await _views(session, items[:limit], settings)
    return [view for view, _ in views], truncated


async def load_outcome(
    session: AsyncSession,
    work_item_id: uuid.UUID,
    *,
    agent_id: uuid.UUID | None,
    settings: Settings,
) -> tuple[WorkItemOutcomeOut, WorkItem, ThreadPublicationLineage | None] | None:
    """None when missing OR scoped to another agent, so both 404 identically."""

    item = await session.get(WorkItem, work_item_id)
    if item is None or (agent_id is not None and item.agent_id != agent_id):
        return None
    ((view, lineage),) = await _views(session, [item], settings)
    return view, item, lineage


# --- live CI observation -------------------------------------------------------


def _unavailable(reason: str, head_sha: str | None = None) -> CiObservation:
    return CiObservation(
        state="unavailable",
        reason=reason,
        head_sha=head_sha,
        observed_at=datetime.now(UTC),
    )


_STATUS_REASONS = {
    401: "github_unauthorized",
    403: "github_forbidden",
    404: "github_not_found",
    429: "github_rate_limited",
}


def _verdict(payload: Any) -> str:
    """A CI state, or an ``unavailable`` reason code."""

    if not isinstance(payload, dict):
        return "malformed_response"
    total = payload.get("total_count")
    runs = payload.get("check_runs")
    if not isinstance(total, int) or isinstance(total, bool) or not isinstance(runs, list):
        return "malformed_response"
    if total > CHECK_RUNS_PAGE or total > len(runs):
        return "too_many_check_runs"
    if not runs:
        return "none"
    failing = pending = False
    for run in runs:
        if not isinstance(run, dict):
            return "malformed_response"
        status, conclusion = run.get("status"), run.get("conclusion")
        if status != "completed":
            pending = True
        elif conclusion in _FAILING_CONCLUSIONS:
            failing = True
        elif conclusion == "stale":
            pending = True
        elif conclusion not in _PASSING_CONCLUSIONS:
            return "malformed_response"
    if failing:
        return "failing"
    return "pending" if pending else "passing"


async def observe_ci(
    lineage: Any, work_item: Any, settings: Settings, client: httpx.AsyncClient
) -> CiObservation:
    """Observe CI for the lineage's published head, live and never persisted.

    The installation token lives only in a local here; every failure maps to a
    fixed reason code, and no response text, header or URL is ever echoed. The
    whole observation is bounded by ``CI_OBSERVATION_DEADLINE_SECONDS``; on
    expiry the caller is released with ``unavailable``/``timeout`` (a blocked
    credential thread is abandoned, not awaited).
    """

    head_sha = getattr(lineage, "head_sha", None) if lineage is not None else None
    try:
        return await asyncio.wait_for(
            _observe_ci(lineage, work_item, settings, client),
            timeout=CI_OBSERVATION_DEADLINE_SECONDS,
        )
    except TimeoutError:
        return _unavailable(
            "timeout",
            head_sha if isinstance(head_sha, str) and _SHA_RE.fullmatch(head_sha) else None,
        )


class _CredentialPermit:
    """One credential slot, released exactly once by whoever claims it.

    Two parties can end up responsible for the same slot: the worker thread,
    which releases when its mint finishes, and the caller, whose deadline may
    cancel ``run_sync`` before the worker body ever runs (thread capacity
    exhausted), leaving no worker ``finally`` to run at all. ``claim`` makes the
    ownership decision atomic, so the slot is released on every path and never
    twice -- the guard is a ``BoundedSemaphore`` and an over-release raises.
    """

    __slots__ = ("_claimed", "_lock")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._claimed = False

    def claim(self) -> bool:
        """True exactly once, for the party that owes the release."""

        with self._lock:
            if self._claimed:
                return False
            self._claimed = True
            return True


def _mint_and_release(
    permit: _CredentialPermit,
    resolver: Any,
    repo_full_name: str,
    installation_id: int | None,
) -> tuple[int, str]:
    """Mint an installation token, returning the bound slot when it finishes."""

    owns = permit.claim()
    try:
        minted: tuple[int, str] = resolver.fresh_installation_token(
            repo_full_name, installation_id
        )
        return minted
    finally:
        if owns:
            _CI_CREDENTIAL_GUARD.release()


async def _observe_ci(
    lineage: Any, work_item: Any, settings: Settings, client: httpx.AsyncClient
) -> CiObservation:
    if lineage is None or lineage.pr_number is None:
        return CiObservation(state="not_applicable", reason="no_pull_request")
    head_sha = lineage.head_sha
    if not isinstance(head_sha, str) or not _SHA_RE.fullmatch(head_sha):
        return _unavailable("no_head_sha")
    resolver = credentials_for(settings)
    if not resolver.app_configured:
        return _unavailable("app_not_configured", head_sha)
    installation_id = lineage.github_installation_id or work_item.github_installation_id
    if not _CI_CREDENTIAL_GUARD.acquire(blocking=False):
        # Every slot is held by a mint that has not finished; refuse now rather
        # than pile another thread onto the repository lock.
        return _unavailable("observation_busy", head_sha)
    permit = _CredentialPermit()
    try:
        # abandon_on_cancel: the overall deadline must release the caller even
        # while the credential thread is blocked on the repository lock. A
        # started mint releases the slot itself when its own work ends, so an
        # abandoned mint still holds it until then; if the deadline fires before
        # the worker body ever runs, the caller releases it instead.
        _, token = await anyio.to_thread.run_sync(
            _mint_and_release,
            permit,
            resolver,
            lineage.repo_full_name,
            installation_id,
            abandon_on_cancel=True,
        )
    except GitHubInstallationRefused:
        return _unavailable("installation_refused", head_sha)
    except (GitHubAppError, ValueError):
        return _unavailable("github_error", head_sha)
    finally:
        if permit.claim():
            _CI_CREDENTIAL_GUARD.release()
    try:
        url = (
            f"{settings.github_api_url.rstrip('/')}/repos/"
            f"{repo_url_path(lineage.repo_full_name)}/commits/{head_sha}/check-runs"
        )
    except ValueError:
        return _unavailable("github_error", head_sha)
    try:
        response = await client.get(
            url,
            params={"per_page": CHECK_RUNS_PAGE},
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=settings.github_app_timeout_seconds,
            follow_redirects=False,
        )
    except httpx.TimeoutException:
        return _unavailable("timeout", head_sha)
    except httpx.HTTPError:
        return _unavailable("github_error", head_sha)
    finally:
        del token
    if response.status_code in _STATUS_REASONS:
        return _unavailable(_STATUS_REASONS[response.status_code], head_sha)
    if response.status_code != 200:
        return _unavailable("github_error", head_sha)
    try:
        payload = response.json()
    except ValueError:
        return _unavailable("malformed_response", head_sha)
    verdict = _verdict(payload)
    if verdict not in ("passing", "failing", "pending", "none"):
        return _unavailable(verdict, head_sha)
    return CiObservation(
        state=verdict,  # type: ignore[arg-type]
        reason=None,
        head_sha=head_sha,
        observed_at=datetime.now(UTC),
    )
