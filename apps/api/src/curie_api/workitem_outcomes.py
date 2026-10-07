"""Operator read model for factory work item outcomes (#2577).

The single home of outcome semantics: ``derive_outcome`` is the only place an
outcome ``state`` and ``actionable_cause`` are computed, and the CLI and the
console render those strings verbatim. The module reads canonical WorkItem,
ExecutionRequest, publication-lineage, Publication, Approval, factory status
comment and execution phase report rows; it writes nothing and adds no store.
The title and progress strip come from the status comment rows and the phase
reports through ``factory_progress.phase_view``, the same derivation the
GitHub status card uses (#4102).

Output is an allowlist: every view is built from explicit fields below, never
from an ORM row, so a runtime-owner token, a reply transport address, a patch,
a publication body/error or an approval summary cannot reach an operator even
when a new column is added later. CI is observed live on the detail route
only and never persisted; any failure to observe is ``unavailable`` with a
fixed reason code that never carries a response body, header, URL or token.
"""

from __future__ import annotations

import re
import uuid
from collections import defaultdict
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api.forges.errors import ForgeError
from curie_api.forges.hosts import issue_url as tracker_issue_url
from curie_api.forges.hosts import repository_ref
from curie_api.forges.ports import CodeHost
from curie_api.forges.types import RollupState
from curie_api.schemas.workitems import (
    WorkItemCiOut,
    WorkItemCiState,
    WorkItemCorrectnessOut,
    WorkItemOutcomeOut,
    WorkItemOutcomeState,
    WorkItemProgressOut,
    WorkItemPrOut,
    WorkItemPublicationOut,
    WorkItemRepositoryOut,
    WorkItemRequestOut,
    WorkItemStageOut,
    WorkItemTrackerOut,
)

from .config import Settings
from .factory_progress import phase_view
from .models import (
    Approval,
    ExecutionRequest,
    ExecutionRequestPhaseReport,
    FactoryStatusComment,
    Publication,
    ThreadPublicationLineage,
    WorkItem,
)

OBJECTIVE_LIMIT = 512
_SHA = re.compile(r"[0-9a-fA-F]{7,64}")
_CI_STATES: dict[RollupState, WorkItemCiState] = {
    RollupState.NONE: "none",
    RollupState.PENDING: "pending",
    RollupState.SUCCESS: "passing",
    RollupState.FAILURE: "failing",
}

_PUBLISHING = frozenset({"approved", "launching", "running"})
_ACTIVE_REQUEST_STATUSES = frozenset({"waiting", "running", "cancellation_requested"})


# --- derivation (pure) -------------------------------------------------------


def _iso(value: datetime | None) -> str:
    return value.isoformat() if value is not None else "unknown"


def _deadline_seconds(req: ExecutionRequest) -> str:
    """The execution deadline this row was started with, in whole seconds (#3071)."""
    if req.started_at is None or req.execution_deadline is None:
        return "unknown"
    return str(int((req.execution_deadline - req.started_at).total_seconds()))


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
    if state == "queued":
        return "revision waiting on the current run"
    if state == "cancellation_requested":
        reasons = {
            "issue_cancelled": "the issue was cancelled (label removed or issue closed)",
            "execution_deadline": (
                f"the {_deadline_seconds(req)} s execution deadline elapsed"
            ),
            "owner_lost": "the runtime owner stopped heartbeating",
        }
        return (
            f"cancellation requested: {cause} ({reasons.get(cause or '', 'unknown')}); "
            "termination is awaiting a runtime observation"
        )
    if state == "waiting":
        deadline = req.wait_deadline
        if deadline is None:
            raise ValueError("a waiting request has no capacity deadline")
        text = "waiting for sandbox capacity"
        if req.capacity_deferrals:
            text += (
                f"; deferred {req.capacity_deferrals} time(s) for capacity, "
                f"last reason: {req.last_deferral_reason or 'unknown'}"
            )
        if now >= deadline:
            return (
                f"{text}; the waiting deadline elapsed at {_iso(deadline)} "
                "and expiry is pending the reconciler"
            )
        return f"{text}; waiting deadline {_iso(deadline)}"
    if state == "running":
        return (
            f"running since {_iso(req.started_at)}, bounded by the execution "
            f"deadline {_iso(req.execution_deadline)}"
        )
    if state == "cancelled":
        if cause == "lineage_closed":
            text = "cancelled: the pull request closed before this revision could start"
        else:
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
            f"expired: {cause}, the execution exceeded {_deadline_seconds(req)} s "
            "and termination "
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


def _current_request(ordered: Sequence[ExecutionRequest]) -> ExecutionRequest | None:
    """Prefer the live run, then the last revision that reached execution.

    A revision refused because its pull request closed never ran, so its
    cancellation must not replace the preceding completed run's outcome.
    """

    active = next(
        (req for req in reversed(ordered) if req.status in _ACTIVE_REQUEST_STATUSES),
        None,
    )
    if active is not None:
        return active
    return next(
        (
            req
            for req in reversed(ordered)
            if not (req.status == "cancelled" and req.terminal_cause == "lineage_closed")
        ),
        ordered[-1] if ordered else None,
    )


def derive_outcome(
    item: WorkItem,
    requests: Sequence[ExecutionRequest],
    lineage: ThreadPublicationLineage | None,
    publication: Publication | None,
    approval: Approval | None,
    pending_turn_approval: bool,
    now: datetime,
    *,
    issue_url: str,
) -> WorkItemOutcomeOut:
    """Derive one operator view. Pure: no I/O. ``ci`` is left null."""

    ordered = sorted(requests, key=lambda r: r.sequence)
    req = _current_request(ordered)
    state: WorkItemOutcomeState
    if req is None:
        state = "cancelled" if item.cancelled_at is not None else "waiting"
    elif req.status == "queued":
        state = "queued"
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

    snapshot = next(
        (
            r
            for r in reversed(ordered)
            if req is not None and r.sequence <= req.sequence and r.objective is not None
        ),
        None,
    )
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
        tracker=WorkItemTrackerOut(
            kind=item.tracker_kind,
            host=item.tracker_host,
            scope_id=item.tracker_scope_id,
            issue_id=item.tracker_issue_id,
            display_key=item.tracker_display_key,
            url=issue_url,
        ),
        repository=WorkItemRepositoryOut(
            code_host_kind=item.code_host_kind,
            host=item.code_host_host,
            project_id=item.repository_project_id,
            path=item.repository_path,
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
    same agent and repository, open first, then newest for older unlinked rows."""

    candidates = list(lineages)
    if item.publication_lineage_id is not None:
        return next((x for x in candidates if x.id == item.publication_lineage_id), None)
    matching = [
        x
        for x in candidates
        if x.agent_id == item.agent_id
        and x.conversation_id == item.conversation_id
        and x.repo_full_name.lower() == item.repository_path.lower()
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

    sequences = {req.id: req.sequence for rows in requests.values() for req in rows}
    comments: dict[uuid.UUID, list[FactoryStatusComment]] = defaultdict(list)
    for comment in (
        await session.scalars(
            select(FactoryStatusComment).where(FactoryStatusComment.work_item_id.in_(ids))
        )
    ).all():
        comments[comment.work_item_id].append(comment)
    latest: dict[uuid.UUID, ExecutionRequest] = {
        item_id: max(rows, key=lambda r: r.sequence) for item_id, rows in requests.items() if rows
    }
    latest_comment: dict[uuid.UUID, FactoryStatusComment] = {}
    for item_id, req in latest.items():
        row = next((c for c in comments[item_id] if c.execution_request_id == req.id), None)
        if row is not None:
            latest_comment[item_id] = row
    reports: dict[uuid.UUID, list[ExecutionRequestPhaseReport]] = defaultdict(list)
    progressed = {latest[item_id].id for item_id in latest_comment}
    if progressed:
        for report in (
            await session.scalars(
                select(ExecutionRequestPhaseReport)
                .where(ExecutionRequestPhaseReport.execution_request_id.in_(progressed))
                .order_by(ExecutionRequestPhaseReport.id)
            )
        ).all():
            reports[report.execution_request_id].append(report)

    views = []
    for item in items:
        item_requests = requests.get(item.id, [])
        current_request = _current_request(sorted(item_requests, key=lambda r: r.sequence))
        pending_turn = (
            current_request is not None
            and current_request.reply_kind is not None
            and current_request.reply_address is not None
            and current_request.reply_conversation_id is not None
            and (
                item.agent_id,
                current_request.reply_kind,
                current_request.reply_address,
                current_request.reply_conversation_id,
            )
            in pending_tuples
        )
        lineage = chosen[item.id]
        chosen_pub = latest_pub.get(lineage.id) if lineage is not None else None
        view = derive_outcome(
            item,
            item_requests,
            lineage,
            chosen_pub,
            approvals.get(chosen_pub.approval_id) if chosen_pub is not None else None,
            pending_turn,
            now,
            issue_url=tracker_issue_url(settings, item.tracker_issue, item.repository),
        )
        view.title = _title(comments[item.id], sequences)
        comment_row = latest_comment.get(item.id)
        if comment_row is not None:
            req = latest[item.id]
            view.progress = _progress(comment_row, reports[req.id], req)
        views.append((view, lineage))
    return views


def _title(rows: Sequence[FactoryStatusComment], sequences: dict[uuid.UUID, int]) -> str | None:
    """The subject title of the work item's most recent status comment row."""

    if not rows:
        return None
    newest = max(
        rows,
        key=lambda row: (row.created_at, sequences.get(row.execution_request_id, 0)),
    )
    return newest.subject_title


def _progress(
    row: FactoryStatusComment,
    reports: Sequence[ExecutionRequestPhaseReport],
    req: ExecutionRequest,
) -> WorkItemProgressOut:
    """The status card's phase view for one request, as explicit fields."""

    view = phase_view(
        row.declaration or {"phases": [], "loops": []},
        reports,
        req.status,
        req.terminal_cause,
    )
    slots = view.stages if view.staged else view.phases
    return WorkItemProgressOut(
        current=view.current,
        note=next((report.note for report in reversed(reports) if report.note), None),
        stages=[
            WorkItemStageOut(
                id=slot.id,
                label=slot.label,
                state=slot.state,
                round_label=slot.round_label,
            )
            for slot in slots
        ],
    )


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


async def observe_ci(
    code_host: CodeHost,
    settings: Settings,
    lineage: ThreadPublicationLineage | None,
    work_item: WorkItem,
) -> WorkItemCiOut:
    """CI on the lineage's published head, live and never persisted.

    Every failure is ``unavailable`` with the code host's fixed reason code,
    which never carries a response body, header, URL or token.
    """

    if lineage is None or lineage.pr_number is None:
        return WorkItemCiOut(state="not_applicable", reason="no_pull_request")
    head_sha = lineage.head_sha
    if not isinstance(head_sha, str) or not _SHA.fullmatch(head_sha):
        return WorkItemCiOut(state="unavailable", reason="no_head_sha", observed_at=_now())
    repository = repository_ref(
        settings,
        path=lineage.repo_full_name,
        project_id=lineage.repository_project_id or work_item.repository_project_id,
    )
    try:
        rollup = await code_host.observe_ci(repository, head_sha)
    except ForgeError as exc:
        return WorkItemCiOut(
            state="unavailable",
            reason=str(exc) or "unavailable",
            head_sha=head_sha,
            observed_at=_now(),
        )
    return WorkItemCiOut(
        state=_CI_STATES[rollup.state], reason=None, head_sha=head_sha, observed_at=_now()
    )


def _now() -> datetime:
    return datetime.now(UTC)
