"""The factory CI gate: a published request waits on its pull request's CI (#3097).

A succeeded publication does not end the request. The reconciler hands each
``completed`` settlement to ``gate``, which observes the checks and commit
statuses on the published head (``workitem_outcomes.observe_ci_detail``) and
decides with the pure ``decide``:

- green, or no checks after the grace period, completes the request;
- a failure below the round cap enqueues ONE continuation turn for the same
  request (``work-item-{id}-ci-{round}``) carrying the failure report;
- a failure on the last round, a timed-out wait, or unreadable CI ends the
  request with a ``Could not complete:`` notice. Unreadable CI is never success.

No network call runs under a row lock. Every write is fenced to the observed
publication and head (``workitems.settle_ci_verdict`` / ``hold_for_ci_fix``),
and a Valkey claim keeps each round to at most one continuation.
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import httpx
import redis.asyncio as redis
from curie_telemetry.redact import redact_text
from sqlalchemy import TIMESTAMP, cast, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from . import factory_progress, workitem_outcomes, workitems
from .config import Settings
from .models import ExecutionRequest, Publication, ThreadPublicationLineage, WorkItem
from .workitem_outcomes import CiDetail

CI_GRACE_SECONDS = 120
CI_POLL_SECONDS = 20
CI_MAX_ROUNDS = 3
CI_OBSERVATIONS_PER_PASS = 4
CI_CLAIM_SECONDS = 60
# Causes the reconciler writes for a request whose pull request already opened;
# they may land after the execution deadline. ``ci_fix_unpublished`` is written
# by the worker and stays bounded by the deadline. ``workitems`` keeps an equal
# literal set (importing this one there would be circular).
CI_CAUSES = frozenset({"ci_failed", "ci_timeout", "ci_unverified"})

# Reason codes that can never become readable by waiting.
PERMANENT_UNREADABLE = frozenset(
    {
        "app_not_configured",
        "installation_refused",
        "github_unauthorized",
        "github_forbidden",
        "github_not_found",
        "malformed_response",
        "too_many_check_runs",
        "no_head_sha",
    }
)
# Reason codes that count as pending until the CI deadline.
TRANSIENT = frozenset({"timeout", "observation_busy", "github_rate_limited", "github_error"})

MARKER = re.compile(r"^Curie wait_ci round ([23]) of 3: ")

_FAILING_CONCLUSIONS = frozenset(
    {"failure", "timed_out", "cancelled", "action_required", "startup_failure"}
)
_PASSING_CONCLUSIONS = frozenset({"success", "neutral", "skipped"})
_FAILING_STATES = frozenset({"error", "failure"})
_REPORT_MAX = 16000
_SUMMARY_MAX = 2000
_ANNOTATIONS_MAX = 10
_TITLE_MAX = 100
_CHECKS_LINE_MAX = 400
_NO_CI_NOTE = f"No CI checks appeared within {CI_GRACE_SECONDS} s."

VerdictKind = Literal["green", "no_ci", "failing", "pending", "timed_out", "unverified"]
GateResult = Literal["settled", "waiting", "fixing", "continued"]


@dataclass(frozen=True)
class Verdict:
    kind: VerdictKind
    failing: list[dict[str, Any]] = field(default_factory=list)
    pending: list[dict[str, Any]] = field(default_factory=list)
    reason: str | None = None
    note: str | None = None


def continuation_event_id(request_id: uuid.UUID, round_: int) -> str:
    """The runs-stream event id of a CI fix turn (worker contract)."""

    return f"work-item-{request_id}-ci-{round_}"


def ci_key(request_id: uuid.UUID, round_: int) -> str:
    """The Valkey key that keeps a round to at most one continuation."""

    return f"curie:work-item:ci:{request_id}:{round_}"


# --- verdict (pure) -----------------------------------------------------------------


def _str(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _fresh_ci_detail(detail: CiDetail, fresh_after: datetime) -> CiDetail:
    """Keep only checks and statuses created for the metadata revision."""

    return replace(
        detail,
        check_runs=[
            run
            for run in detail.check_runs
            if (started := _github_time(run.get("started_at"))) is not None
            and started > fresh_after
        ],
        statuses=[
            item
            for item in detail.statuses
            if (created := _github_time(item.get("created_at"))) is not None
            and created > fresh_after
        ],
    )


_METADATA_EDIT_CHECKS = frozenset({"PR body (real newlines)", "Fix pin verification"})


def _metadata_revision_detail(detail: CiDetail, fresh_after: datetime) -> CiDetail:
    fresh = _fresh_ci_detail(detail, fresh_after)
    fresh_names = {run.get("name") for run in fresh.check_runs}
    fresh_contexts = {item.get("context") for item in fresh.statuses}
    return replace(
        detail,
        check_runs=fresh.check_runs + [
            run for run in detail.check_runs
            if run.get("name") not in fresh_names
            and run.get("name") not in _METADATA_EDIT_CHECKS
        ],
        statuses=fresh.statuses + [
            item for item in detail.statuses
            if item.get("context") not in fresh_contexts
        ],
    )


def decide(
    detail: CiDetail,
    *,
    now: datetime,
    published_at: datetime,
    execution_deadline: datetime,
    ci_wait_seconds: int,
    prior_round_had_checks: bool = False,
    fresh_after: datetime | None = None,
) -> Verdict:
    """The CI verdict for one observation. Pure: time is an argument."""

    ci_deadline = min(published_at + timedelta(seconds=ci_wait_seconds), execution_deadline)
    expired = now >= ci_deadline
    if detail.state != "observed" or detail.reason is not None:
        reason = detail.reason or "github_error"
        if reason in TRANSIENT:
            if expired:
                return Verdict(kind="timed_out", reason=reason)
            return Verdict(kind="pending", reason=reason)
        return Verdict(kind="unverified", reason=reason)
    check_runs = detail.check_runs
    statuses = detail.statuses
    if fresh_after is not None:
        fresh = _fresh_ci_detail(detail, fresh_after)
        fresh_runs, fresh_statuses = fresh.check_runs, fresh.statuses
        fresh_names = {run.get("name") for run in fresh_runs}
        missing_rerun = any(
            run.get("name") in _METADATA_EDIT_CHECKS
            and run.get("name") not in fresh_names
            for run in check_runs
        )
        fresh_failure = any(
            run.get("status") == "completed"
            and run.get("conclusion") in _FAILING_CONCLUSIONS
            for run in fresh_runs
        ) or any(item.get("state") in _FAILING_STATES for item in fresh_statuses)
        unchanged_failure = any(
            run.get("name") not in _METADATA_EDIT_CHECKS
            and run.get("status") == "completed"
            and run.get("conclusion") in _FAILING_CONCLUSIONS
            for run in check_runs
        ) or any(item.get("state") in _FAILING_STATES for item in statuses)
        if (
            not fresh_failure
            and not unchanged_failure
            and (missing_rerun or not fresh_runs and not fresh_statuses)
        ):
            return Verdict(
                kind="unverified" if expired else "pending",
                reason="checks_not_rerun" if expired else "checks_awaiting_metadata_rerun",
            )
        # A PR metadata edit reruns body checks, but it does not rerun the main
        # suite on the unchanged commit. Keep its passing or pending evidence.
        effective = _metadata_revision_detail(detail, fresh_after)
        check_runs, statuses = effective.check_runs, effective.statuses
    failing: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    for run in check_runs:
        name, status, conclusion = _str(run.get("name")), run.get("status"), run.get("conclusion")
        if status != "completed":
            pending.append({"name": name, "status": _str(status)})
        elif conclusion in _FAILING_CONCLUSIONS:
            failing.append({"name": name, "conclusion": _str(conclusion)})
        elif conclusion not in _PASSING_CONCLUSIONS:
            # ``stale`` (and anything unrecognised) waits for a fresh conclusion.
            pending.append({"name": name, "status": _str(conclusion)})
    for status_item in statuses:
        context, state = _str(status_item.get("context")), status_item.get("state")
        if state in _FAILING_STATES:
            failing.append({"context": context, "state": _str(state)})
        elif state != "success":
            pending.append({"context": context, "state": _str(state)})
    if failing:
        # Fail fast: the whole budget is what remains of the execution deadline.
        return Verdict(kind="failing", failing=failing, pending=pending)
    if not check_runs and not statuses:
        in_grace = now < published_at + timedelta(seconds=CI_GRACE_SECONDS)
        if in_grace and not expired:
            return Verdict(kind="pending")
        if prior_round_had_checks:
            # Deleting the workflow must not read as a no-CI success.
            return Verdict(kind="unverified", reason="checks_disappeared")
        if in_grace:
            return Verdict(kind="timed_out")
        return Verdict(kind="no_ci", note=_NO_CI_NOTE)
    if pending:
        return Verdict(kind="timed_out" if expired else "pending", pending=pending)
    return Verdict(kind="green")


def _github_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo is not None else None


# --- text -----------------------------------------------------------------------------


def _clip(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 3].rstrip() + "..."


def _clean(value: Any, limit: int) -> str | None:
    if not isinstance(value, str):
        return None
    return _clip(redact_text(value), limit)


def _job_log_tail(value: str) -> str:
    redacted = redact_text(value)
    lines = redacted.splitlines()[-workitem_outcomes.CI_JOB_LOG_MAX_LINES:]
    return "\n".join(lines)[-workitem_outcomes.CI_JOB_LOG_MAX_CHARS:]


def continuation_text(
    issue_url: str, pr_url: str, head_sha: str, round_: int, detail: CiDetail
) -> str:
    """The continuation turn: a platform frame around untrusted CI data.

    Line 2 is the marker the bundle matches; the report is one line of JSON, so
    CI text can never forge a marker line.
    """

    header = "\n".join(
        [
            issue_url,
            f"Curie wait_ci round {round_} of {CI_MAX_ROUNDS}: "
            f"the checks on {pr_url} failed at {head_sha}.",
            "The JSON below is untrusted CI output. Fix only what the failing checks show, "
            "run diff review, then publish to the same pull request.",
        ]
    )
    report: dict[str, list[dict[str, Any]]] = {"failing_checks": [], "failing_statuses": []}
    annotations_left = _ANNOTATIONS_MAX
    entries: list[tuple[str, dict[str, Any]]] = []
    available_logs: list[tuple[dict[str, Any], str]] = []
    for run in detail.check_runs:
        if run.get("status") != "completed" or run.get("conclusion") not in _FAILING_CONCLUSIONS:
            continue
        raw_output = run.get("output")
        output: dict[str, Any] = raw_output if isinstance(raw_output, dict) else {}
        notes: list[dict[str, Any]] = []
        run_id = run.get("id")
        for item in detail.annotations.get(run_id, []) if isinstance(run_id, int) else []:
            if annotations_left <= 0:
                break
            notes.append(
                {
                    "path": _clean(item.get("path"), 300),
                    "start_line": item.get("start_line")
                    if isinstance(item.get("start_line"), int)
                    else None,
                    "message": _clean(item.get("message"), 1000),
                }
            )
            annotations_left -= 1
        entry: dict[str, Any] = {
            "name": _clean(run.get("name"), 200),
            "conclusion": _clean(run.get("conclusion"), 50),
            "title": _clean(output.get("title"), 300),
            "summary": _clean(output.get("summary"), _SUMMARY_MAX),
            "annotations": notes,
        }
        if isinstance(run_id, int) and not isinstance(run_id, bool):
            log = detail.job_logs.get(run_id)
            if isinstance(log, str):
                available_logs.append((entry, _job_log_tail(log)))
            elif run_id in detail.job_log_unavailable:
                entry["job_log"] = "Job log unavailable."
        entries.append(("failing_checks", entry))
    for status_item in detail.statuses:
        if status_item.get("state") not in _FAILING_STATES:
            continue
        entries.append(
            (
                "failing_statuses",
                {
                    "context": _clean(status_item.get("context"), 200),
                    "state": _clean(status_item.get("state"), 50),
                    "description": _clean(status_item.get("description"), 1000),
                },
            )
        )
    # Reserve the check details before spending the report budget on logs.
    for key, entry in entries:
        report[key].append(entry)
        if len(json.dumps(report)) > _REPORT_MAX:
            report[key].pop()
            break
    included = {id(entry) for entry in report["failing_checks"]}
    logs = [(entry, log) for entry, log in available_logs if id(entry) in included]
    for index, (entry, log) in enumerate(logs):
        # Share the remaining space among logs. A suffix keeps the diagnostic
        # tail and cannot make a marker into a separate prompt line.
        current_size = len(json.dumps(report))
        allowance = (_REPORT_MAX - current_size) // (len(logs) - index)
        target_size = current_size + allowance
        low, high = 1, len(log)
        best: str | None = None
        while low <= high:
            midpoint = (low + high) // 2
            candidate = log[-midpoint:]
            entry["job_log"] = candidate
            if len(json.dumps(report)) <= target_size:
                best = candidate
                low = midpoint + 1
            else:
                high = midpoint - 1
        if best is None:
            entry.pop("job_log", None)
        else:
            entry["job_log"] = best
    return f"{header}\n{json.dumps(report)}"


def _check_list(items: Sequence[dict[str, Any]]) -> str:
    parts = []
    for item in items:
        name = item.get("name") or item.get("context") or "unnamed"
        state = item.get("conclusion") or item.get("state") or item.get("status") or ""
        parts.append(f"{name} ({state})" if state else str(name))
    return _clip(", ".join(parts), _CHECKS_LINE_MAX)


def tried_summary(publications: Iterable[Any], verdict: Verdict, pr_url: str | None) -> str:
    """The final CI notice detail: rounds, what each fix round tried, the checks."""

    ordered = sorted(publications, key=lambda p: int(getattr(p, "revision_number", 0) or 0))
    lines = [f"Rounds: {len(ordered)}"]
    tried = []
    for index, publication in enumerate(ordered, start=1):
        if index < 2:
            continue
        changed = getattr(publication, "changed_paths", None) or []
        paths = [p for p in changed if isinstance(p, str)]
        title = str(getattr(publication, "title", "") or "").replace("\n", " ")[:_TITLE_MAX]
        tried.append(f'round {index}: "{title}" ({len(paths)} files: {", ".join(paths[:3])})')
    if tried:
        lines.append("Tried: " + "; ".join(tried))
    if verdict.failing:
        lines.append(f"Failing checks: {_check_list(verdict.failing)}")
    if verdict.pending:
        lines.append(f"Pending checks: {_check_list(verdict.pending)}")
    if verdict.reason:
        lines.append(f"Reason: {verdict.reason}")
    if verdict.reason == "github_forbidden":
        # A 403 on check runs or commit statuses does not say which permission
        # was missing, so the notice names both reads the wait needs.
        lines.append(
            "GitHub returned 403, so check that the installation grants "
            "Checks: read and Commit statuses: read. The 403 does not say "
            "which one is missing. On the GitHub App, open Permissions and "
            "events, set each missing permission to Read, save, then accept "
            "the permission update on the installation."
        )
    if pr_url:
        lines.append(pr_url)
    return "\n".join(lines)


# --- gate --------------------------------------------------------------------------------


@dataclass(frozen=True)
class _Facts:
    request: ExecutionRequest
    work_item: WorkItem
    lineage: ThreadPublicationLineage
    publications: list[Publication]
    published_at: datetime


async def _load(
    session: AsyncSession, settlement: workitems.PublicationSettlement
) -> _Facts | None:
    request = await session.get(ExecutionRequest, settlement.request_id)
    work_item = await session.get(WorkItem, settlement.work_item_id)
    if (
        request is None
        or work_item is None
        or request.status != "running"
        or request.execution_deadline is None
        or work_item.publication_lineage_id is None
    ):
        return None
    lineage = await session.get(ThreadPublicationLineage, work_item.publication_lineage_id)
    if lineage is None:
        return None
    publications = list(
        (
            await session.scalars(
                select(Publication)
                .where(
                    Publication.execution_request_id == request.id,
                    Publication.status == "succeeded",
                )
                .order_by(Publication.revision_number)
            )
        ).all()
    )
    if not publications:
        return None
    # ``publications.terminal_at`` is a naive timestamp; read it back as an
    # aware one through the session time zone it was written in.
    published_at = await session.scalar(
        select(
            cast(
                func.coalesce(Publication.terminal_at, Publication.created_at),
                TIMESTAMP(timezone=True),
            )
        ).where(Publication.id == publications[-1].id)
    )
    if published_at is None:
        return None
    return _Facts(request, work_item, lineage, publications, published_at)


_RELEASE_CLAIM = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


_CONFIRM_CLAIM = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
  return 1
end
return 0
"""


def round_ttl(request: ExecutionRequest, now: datetime) -> int:
    """Seconds a round's claim and marker outlive the request's execution deadline."""
    assert request.execution_deadline is not None
    return max(int((request.execution_deadline - now).total_seconds()) + 60, CI_CLAIM_SECONDS)


def enqueue_marker(request_id: uuid.UUID, round_: int) -> str:
    """The key set atomically with a round's continuation turn."""

    return f"{ci_key(request_id, round_)}:enqueued"


Dispatch = Callable[[ExecutionRequest, int, str], Awaitable[bool]]


async def gate(
    sessionmaker: async_sessionmaker[AsyncSession],
    valkey: redis.Redis,
    settings: Settings,
    client: httpx.AsyncClient,
    settlement: workitems.PublicationSettlement,
    *,
    owner: str,
    next_poll: dict[uuid.UUID, datetime],
    dispatch: Dispatch,
    may_observe: Callable[[uuid.UUID], bool],
) -> GateResult:
    """Observe one published request's CI and act on the verdict.

    ``dispatch`` enqueues the continuation turn; it runs only after the round's
    claim and the fenced lease hold, and a failure releases the claim.
    ``may_observe`` spends the caller's per-pass observation budget; it is asked
    only when a CI read is actually due, so a request that is not due never
    takes a later request's slot.
    """

    async with sessionmaker() as session:
        facts = await _load(session, settlement)
        now = await workitems._database_now(session)
        # Keep the loaded snapshots; nothing is locked or written here.
        session.expunge_all()
        await session.rollback()
    if facts is None:
        return "waiting"
    request, work_item, lineage = facts.request, facts.work_item, facts.lineage
    assert request.execution_deadline is not None
    round_ = len(facts.publications)
    if await valkey.exists(ci_key(request.id, round_ + 1)):
        return "fixing"
    async with sessionmaker() as session:
        await factory_progress.record_wait_ci(session, request.id)
    due = next_poll.get(request.id)
    if due is not None and now < due:
        return "waiting"
    if not may_observe(request.id):
        return "waiting"
    latest = facts.publications[-1]
    observed_sha = lineage.head_sha
    detail = await workitem_outcomes.observe_ci_detail(lineage, work_item, settings, client)
    metadata_only = not latest.changed_paths and latest.base_sha == observed_sha
    fresh_after = latest.metadata_updated_at if metadata_only else None
    async with sessionmaker() as session:
        now = await workitems._database_now(session)
        await session.rollback()
    head_sha = detail.head_sha or observed_sha or ""
    if metadata_only and fresh_after is None:
        verdict = Verdict(kind="unverified", reason="metadata_update_unverified")
    else:
        verdict = decide(
            detail,
            now=now,
            published_at=facts.published_at,
            execution_deadline=request.execution_deadline,
            ci_wait_seconds=settings.github_factory_ci_wait_s,
            prior_round_had_checks=round_ > 1,
            fresh_after=fresh_after,
        )
    if verdict.kind == "pending":
        next_poll[request.id] = now + timedelta(seconds=CI_POLL_SECONDS)
        return "waiting"
    next_poll.pop(request.id, None)
    pr_url = lineage.pr_url
    if verdict.kind == "failing" and round_ < CI_MAX_ROUNDS:
        return await _continue(
            sessionmaker,
            valkey,
            settings,
            request=request,
            work_item=work_item,
            publication_id=latest.id,
            head_sha=head_sha,
            round_=round_ + 1,
            text=continuation_text(
                _issue_url(settings, work_item),
                pr_url or "",
                head_sha,
                round_ + 1,
                _metadata_revision_detail(detail, fresh_after)
                if fresh_after is not None else detail,
            ),
            owner=owner,
            now=now,
            dispatch=dispatch,
        )
    if verdict.kind in ("green", "no_ci"):
        status: Literal["completed", "failed"] = "completed"
        cause, text = "completed", verdict.note
    else:
        status = "failed"
        cause = {
            "failing": "ci_failed",
            "timed_out": "ci_timeout",
            "unverified": "ci_unverified",
        }[verdict.kind]
        text = tried_summary(facts.publications, verdict, pr_url)
    async with sessionmaker() as session:
        result = await workitems.settle_ci_verdict(
            session,
            work_item_id=settlement.work_item_id,
            request_id=settlement.request_id,
            expected_work_item_version=settlement.work_item_version,
            expected_request_version=settlement.request_version,
            expected_publication_id=latest.id,
            expected_head_sha=head_sha,
            status=status,
            cause=cause,
            detail=text,
        )
    return "settled" if isinstance(result, workitems.WorkItemOutcome) else "waiting"


def _issue_url(settings: Settings, work_item: WorkItem) -> str:
    base = settings.github_clone_base.rstrip("/")
    return f"{base}/{work_item.repo_full_name}/issues/{work_item.github_issue_number}"


async def _continue(
    sessionmaker: async_sessionmaker[AsyncSession],
    valkey: redis.Redis,
    settings: Settings,
    *,
    request: ExecutionRequest,
    work_item: WorkItem,
    publication_id: uuid.UUID,
    head_sha: str,
    round_: int,
    text: str,
    owner: str,
    now: datetime,
    dispatch: Dispatch,
) -> GateResult:
    """Claim the round, hold the lease, publish the turn, then confirm the claim.

    The claim can expire during a slow hold or dispatch, so it alone cannot keep
    a round to one turn. An enqueue marker, set NX atomically with the stream
    write by ``dispatch``, is the idempotency key: a reconciler that finds it set never enqueues the
    round again, and one whose claim was lost does not report the continuation.
    """

    key = ci_key(request.id, round_)
    token = f"claimed:{owner}"
    ttl = round_ttl(request, now)
    if not await valkey.set(key, token, nx=True, ex=CI_CLAIM_SECONDS):
        return "fixing"
    try:
        async with sessionmaker() as session:
            held = await workitems.hold_for_ci_fix(
                session,
                work_item_id=work_item.id,
                request_id=request.id,
                expected_request_version=request.version,
                expected_publication_id=publication_id,
                expected_head_sha=head_sha,
            )
        if not held:
            await valkey.eval(_RELEASE_CLAIM, 1, key, token)
            return "waiting"
        if await valkey.exists(enqueue_marker(request.id, round_)):
            # Another reconciler already enqueued this round.
            await valkey.set(key, "published", ex=ttl)
            return "fixing"
        # ``dispatch`` sets the marker and appends the turn atomically.
        published = await dispatch(request, round_, text)
    except Exception:
        await valkey.eval(_RELEASE_CLAIM, 1, key, token)
        raise
    if not published:
        await valkey.eval(_RELEASE_CLAIM, 1, key, token)
        return "waiting"
    confirmed = await valkey.eval(_CONFIRM_CLAIM, 1, key, token, "published", ttl)
    if not confirmed:
        # The claim expired mid-dispatch; the marker still holds the round.
        await valkey.set(key, "published", ex=ttl)
        return "fixing"
    return "continued"
