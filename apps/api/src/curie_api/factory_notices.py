"""Keep the one live status comment of each factory execution request (#3077).

The status row is inserted with the request at admission; the terminus stages
its cause and detail on it. This module creates the comment, edits it in place
whenever its rendered body changes, finalizes it once the result is shown, and
moves the ``curie-factory:*`` state labels on the WorkItem's issue. There is no
separate final comment. A refused write never rewrites the execution request.

An issue-originated request comments on its issue. A revision asked for from
pull request review feedback (#2798) answers on the pull request: a review
comment gets a reply in its thread, other feedback a linked PR comment, and a
thread reply GitHub refuses with 422 falls back to that linked PR comment.

GitHub issue comments:
https://docs.github.com/en/rest/issues/comments#create-an-issue-comment
https://docs.github.com/en/rest/issues/comments#list-issue-comments
https://docs.github.com/en/rest/issues/comments#update-an-issue-comment
Pull request review comments:
https://docs.github.com/en/rest/pulls/comments#create-a-reply-for-a-review-comment
https://docs.github.com/en/rest/pulls/comments#list-review-comments-on-a-pull-request
https://docs.github.com/en/rest/pulls/comments#update-a-review-comment-for-a-pull-request
Labels:
https://docs.github.com/en/rest/issues/labels#add-labels-to-an-issue
https://docs.github.com/en/rest/issues/labels#remove-a-label-from-an-issue
"""

from __future__ import annotations

import hashlib
import logging
import math
import re
import uuid
from typing import Any
from urllib.parse import quote

import httpx
from sqlalchemy import and_, case, exists, func, literal, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from starlette.concurrency import run_in_threadpool

from .config import Settings
from .factory_comment_text import _redact_factory_comment, marker_for
from .factory_progress import PhaseView, phase_view, pill_for
from .factory_reply_target import github_host, stored_reply_target
from .factory_usage import usage_line, work_item_usage
from .forges.github.marked_comments import (
    _REFUSED_STATUSES,
    _deliver,
    _GitHub,
    _patch,
    _subject_title,
)
from .forges.types import ReplyTarget
from .github_app import GitHubAppError, GitHubInstallationRefused, credentials_for
from .models import (
    ExecutionRequest,
    ExecutionRequestPhaseReport,
    FactoryStatusComment,
    Publication,
    ThreadPublicationLineage,
    WorkItem,
)
from .repo_full_name import repo_url_path

logger = logging.getLogger(__name__)


# The operator-facing sentence for each terminus cause (#3073). The raw code
# still appears on the comment, but never as its headline.
_CAUSE_TEXT = {
    "model_credit_exhausted": (
        "the model provider refused the request because the account has run "
        "out of credits. Add credits or raise the key's limit, then retry."
    ),
    "model_usage_limited": (
        "the model provider's usage limit for this credential was reached; "
        "re-add the label after the limit resets."
    ),
    "model_credential_rejected": (
        "the model provider rejected the configured API key. Check the model "
        "credential, then retry."
    ),
    "model_rate_limited": (
        "the model provider kept rate limiting the request. Retry later or raise "
        "the provider limit."
    ),
    "model_error": "the model provider returned an error the run could not recover from.",
    "budget_exceeded": "the run reached a budget limit before it finished.",
    "runner_timeout": "the run took longer than its time limit.",
    "sandbox_terminated": (
        "the sandbox terminated before the run finished. Check the Kubernetes "
        "reason below, then retry after addressing the sandbox failure."
    ),
    "workspace_error": "the repository workspace could not be prepared for the run.",
    "history_capacity": (
        "conversation history capacity exceeded. Work may have happened. "
        "Inspect the result and retry."
    ),
    "runner_escalated": "the run stopped on an error and was handed to a person.",
    "unclassified": (
        "the run failed and Curie could not name a more specific cause. "
        "Read the worker log for the provider message, then retry or hand it to a person."
    ),
    "max_turns": (
        "the run used its whole turn budget. Raise worker.workItemMaxTurns "
        "(CURIE_WORK_ITEM_MAX_TURNS) to allow more turns, then retry."
    ),
    "runner_failed": "the run ended without a result.",
    "approval_create_failed": (
        "the publication request was refused, so no pull request was opened. "
        "Read the details for the reason, fix it, then retry."
    ),
    "early_stop": "the agent stopped before doing any work on the issue.",
    "no_pull_request": "the run ended without publishing a pull request.",
    "execution_deadline": "the run did not finish before its deadline.",
    "capacity_wait_expired": "no runner capacity came free before the wait expired.",
    "owner_lost": "the worker running this request stopped responding.",
    "issue_cancelled": "the request was cancelled.",
    "publication_denied": "a person denied the request to open the pull request.",
    "publication_expired": (
        "the request to open the pull request expired before anyone approved it."
    ),
    "publication_failed": "the pull request could not be opened.",
    "ci_failed": (
        "the pull request's checks still failed after 3 rounds of fixes. The pull "
        "request stays open for a person."
    ),
    "ci_timeout": (
        "the pull request's checks did not finish before the CI wait ran out. The "
        "pull request stays open."
    ),
    "ci_unverified": (
        "the pull request's checks could not be read, so CI is unverified. The pull "
        "request stays open; check it yourself."
    ),
    "ci_fix_unpublished": (
        "a CI fix round ended without pushing a fix. The pull request stays open."
    ),
}

# Infrastructure and CI causes carry details, not a provider message.
_DETAIL_CAUSES = frozenset(
    {"sandbox_terminated", "ci_failed", "ci_timeout", "ci_unverified", "approval_create_failed"}
)
# A run that ended without publishing carries the agent's own last message
# (#3128). That text is model-authored, so it renders inert inside a code fence.
_AGENT_MESSAGE_CAUSES = frozenset({"early_stop", "no_pull_request"})
# Wire classification a text-only consumer reads off the status comment (#3401).
# Same tokens as the channel reply's ``curie-turn-failure:`` line. Causes with
# no entry stay unlabeled rather than inventing a class.
# Failed runs that still need a person, including the classes that used to
# collapse into runner_escalated (#3401). The status card reads this set.
NEEDS_HUMAN_CAUSES = frozenset(
    {"runner_escalated", "sandbox_terminated", "unclassified", "max_turns", "ci_failed"}
)


def needs_human(status: str, terminal: str | None) -> bool:
    """Whether a failed run should show the needs-human card state."""

    return status == "failed" and terminal in NEEDS_HUMAN_CAUSES


_FAILURE_CLASS_BY_CAUSE = {
    "unclassified": "unclassified",
    "max_turns": "max-turns",
    "history_capacity": "history-persistence-error",
    "model_credit_exhausted": "model-credit-exhausted",
    "model_usage_limited": "model-usage-limited",
    "model_credential_rejected": "model-credential-rejected",
    "model_rate_limited": "rate-limit",
    "model_error": "server-error",
    "budget_exceeded": "budget-exceeded",
    "runner_timeout": "runner-timeout",
    "sandbox_terminated": "sandbox-terminated",
    "workspace_error": "workspace-error",
}
_BACKTICK_RUN = re.compile(r"`+")
_OUTPUT_TOKEN_BUDGET_DETAIL = re.compile(
    r"output token budget exceeded \(max_output_tokens_per_run=([1-9][0-9]{0,19})\)"
)
_USD_BUDGET_DETAIL = re.compile(
    r"USD budget exceeded \(max_usd_per_day=([0-9]+(?:\.[0-9]+)?(?:e[+-]?[0-9]+)?)\)"
)


def cause_text(cause: str) -> str:
    """A plain sentence for a terminus cause; unknown codes get a generic one."""

    return _CAUSE_TEXT.get(cause, "the run stopped for a reason Curie did not recognize.")


FINAL_MARKER = "<!-- curie-status:final -->"


def _fence_for(text: str, *, minimum: int = 1) -> str:
    """A backtick fence longer than any backtick run in ``text``."""

    longest = max((len(run) for run in _BACKTICK_RUN.findall(text)), default=0)
    return "`" * max(minimum, longest + 1)


def code_span(value: str) -> str:
    """``value`` as one Markdown code span, whatever backticks it carries."""

    fence = _fence_for(value)
    # A value carrying backticks is padded so its edge backticks stay literal.
    padded = f" {value} " if len(fence) > 1 else value
    return f"{fence}{padded}{fence}"


def render_base_line(work_item: WorkItem) -> str | None:
    """The status comment's ``Base:`` line (ADR 0186), or None on a legacy row."""

    if work_item.base_branch is None:
        return None
    if work_item.base_source == "label":
        line = (
            f"Base: {code_span(work_item.base_branch)} "
            f"(from label {code_span('base:' + work_item.base_branch)})"
        )
    else:
        line = f"Base: {code_span(work_item.base_branch)} (deployment default)"
    if work_item.base_label_ignored is not None:
        line += (
            f" Label now says {code_span('base:' + work_item.base_label_ignored)};"
            " the recorded base is kept."
        )
    return line


async def mark_status_comment_stale(session: AsyncSession, work_item_id: uuid.UUID) -> None:
    """Let the reconciler edit the latest finalized status comment once more.

    Its body carries the ignored-label note, so a change to that note must reach
    a comment that was already finalized. The comment re-finalizes after the edit.
    """

    latest = (
        select(ExecutionRequest.id)
        .where(ExecutionRequest.work_item_id == work_item_id)
        .order_by(ExecutionRequest.sequence.desc())
        .limit(1)
        .scalar_subquery()
    )
    await session.execute(
        update(FactoryStatusComment)
        .where(
            FactoryStatusComment.work_item_id == work_item_id,
            FactoryStatusComment.execution_request_id == latest,
            FactoryStatusComment.finalized_at.is_not(None),
            FactoryStatusComment.refused_at.is_(None),
        )
        .values(finalized_at=None)
    )


# The four state labels the pass owns (#3077, #3221). A closed set, never a
# prefix match: human labels, including other ``curie:`` labels, are never
# touched.
STATE_LABELS = (
    "curie-factory:queued",
    "curie-factory:running",
    "curie-factory:pr-open",
    "curie-factory:needs-human",
)
LEGACY_STATE_LABELS = ("curie:queued", "curie:running", "curie:pr-open", "curie:needs-human")
_DESIRED_LABEL = {
    "queued": "curie-factory:queued",
    "waiting": "curie-factory:queued",
    "running": "curie-factory:running",
    "cancellation_requested": "curie-factory:running",
    "completed": "curie-factory:pr-open",
    "failed": "curie-factory:needs-human",
    "expired": "curie-factory:needs-human",
    # Cancelled clears the current four and the legacy four; '' records that nothing is applied.
    "cancelled": "",
}
_PUBLISHING_STATUSES = ("pending", "approved", "launching", "running")
_WAITING_FOR_PROGRESS = "_Waiting for the agent to report progress._"
_MARKDOWN_SPECIAL = set("\\`*_[]()#<>!|")
# #3127 AC2: queued work isn't stuck, it's waiting out an upgrade.
_PAUSED_FOR_UPGRADE_LINE = (
    "Paused: this Curie installation is paused for an upgrade. "
    "Queued work starts when the upgrade finishes."
)


def desired_label(status: str) -> str:
    """The state label for a request status; '' means none of the four."""

    return _DESIRED_LABEL.get(status, "")


def result_section(
    cause: str,
    *,
    pr_url: str | None,
    feedback_url: str | None = None,
    detail: str | None = None,
    superseded: bool = False,
) -> str:
    """The terminal result lines of a status comment, without any marker."""

    if cause == "completed":
        if feedback_url is not None:
            text = "The requested revision is pushed to this pull request.\n"
            if detail is not None and detail.strip():
                text += f"Note: {detail.strip()}\n"
        elif isinstance(pr_url, str) and pr_url.strip():
            text = f"Completed: {pr_url.strip()}\n"
            if detail is not None and detail.strip():
                # The CI gate's no-CI note (#3097).
                text += f"Note: {detail.strip()}\n"
        else:
            raise ValueError("a completed issue notice requires its pull request URL")
    elif cause == "issue_cancelled" and superseded:
        text = (
            f"Stopped: the label was added again, so a new run replaced this one.\nCause: {cause}\n"
        )
    elif cause == "issue_cancelled":
        text = (
            "Stopped: this run was cancelled because the factory label was removed "
            "or the issue was closed. Add the label again to start a new run.\n"
            # tools/factory-e2e reads the cause from this line.
            f"Cause: {cause}\n"
        )
    elif cause == "lineage_closed":
        text = (
            "Could not start this revision because its pull request closed "
            "while the earlier run was finishing.\n"
            f"Cause: {cause}\n"
        )
    else:
        sentence = cause_text(cause)
        if cause == "budget_exceeded":
            token_budget = _OUTPUT_TOKEN_BUDGET_DETAIL.fullmatch(detail or "")
            usd_budget = _USD_BUDGET_DETAIL.fullmatch(detail or "")
            if token_budget is not None:
                sentence = (
                    "the run reached its output token limit per run "
                    f"(max_output_tokens_per_run={token_budget[1]}) before it finished. "
                    "To raise the output token limit, run "
                    "`curie cluster budget <agent> --output-tokens <tokens>`, then retry."
                )
            elif (
                usd_budget is not None
                and len(usd_budget[1]) <= 32
                and math.isfinite(usd := float(usd_budget[1]))
                and usd > 0
            ):
                sentence = (
                    "the run reached its USD cap "
                    f"(max_usd_per_day={usd_budget[1]}) before it finished. "
                    "To raise the USD cap, run "
                    "`curie cluster budget <agent> --limit <usd>`, then retry."
                )
            else:
                sentence = (
                    "the run reached a budget limit before it finished, but Curie "
                    "cannot identify which limit from the reported detail. "
                    "Check the agent's configured budget, then retry."
                )
        text = f"Could not complete: {sentence}\n"
        if cause in _AGENT_MESSAGE_CAUSES and detail is not None and detail.strip():
            text += _agent_message_block(detail.strip())
        elif cause == "approval_create_failed" and detail is not None and detail.strip():
            # The API refusal can quote caller-supplied paths (#3617): one line,
            # so it cannot add a ``Cause:`` line, and no HTML comment opener.
            inert = _break_html_comments(" ".join(detail.split()))
            text += f"Details: {inert}\n"
        elif cause != "history_capacity" and detail is not None and detail.strip():
            label = "Details" if cause in _DETAIL_CAUSES else "Provider message"
            text += f"{label}: {detail.strip()}\n"
        text += f"Cause: {cause}\n"
        failure_class = _FAILURE_CLASS_BY_CAUSE.get(cause)
        if failure_class is not None:
            text += f"Failure class: {failure_class}\n"
    if feedback_url is not None:
        text += f"In response to {feedback_url}\n"
    return text


def _break_html_comments(text: str) -> str:
    return text.replace("<!--", "<\u200b!--")


def _agent_message_block(message: str) -> str:
    """The agent's last message, fenced so GitHub renders none of it.

    The fence is longer than any backtick run in the message, so the message
    cannot close it and spoof the ``Cause:`` line. HTML comment openers are
    broken because the marker scan reads the raw body.
    """

    message = _break_html_comments(message)
    fence = _fence_for(message, minimum=3)
    return f"Agent's last message:\n{fence}text\n{message}\n{fence}\n"


def _escape_markdown(value: str) -> str:
    return "".join(f"\\{char}" if char in _MARKDOWN_SPECIAL else char for char in value)


def _checklist(view: PhaseView) -> list[str]:
    lines: list[str] = []
    for slot in view.phases:
        label = _escape_markdown(slot.label)
        if slot.state == "done":
            lines.append(f"- [x] {label}")
        elif slot.state == "current":
            detail = "in progress"
            if slot.round_label is not None and slot.round_label.startswith("round "):
                detail += f", {slot.round_label}"
            lines.append(f"- [ ] **{label}** ({detail})")
        elif slot.state == "redo":
            lines.append(f"- [ ] {label} (redo after review)")
        else:
            lines.append(f"- [ ] {label}")
    return lines


def status_body(
    *,
    request_id: uuid.UUID,
    card_url: str | None,
    pill_label: str,
    phase_view: PhaseView | None,
    result: str | None,
    paused_for_upgrade: bool = False,
    waiting_line: str | None = None,
    base_line: str | None = None,
) -> str:
    """The whole status comment. A ``result`` makes it the final body.

    Model-written notes are never rendered here, only on the card, so model
    text cannot become a Markdown link or a mention on GitHub. The card already
    draws the phases, so with a card the checklist and the waiting placeholder
    are left out; without one the checklist is the fallback (#3125).
    """

    parts: list[str] = []
    if result is not None:
        parts.append(result.rstrip("\n"))
    if card_url:
        parts.append(f"![Curie status]({card_url})")
    elif phase_view is not None and phase_view.phases:
        parts.append("\n".join(_checklist(phase_view)))
    elif result is None:
        parts.append(_WAITING_FOR_PROGRESS)
    parts.append(f"Status: {pill_label}")
    if waiting_line is not None:
        parts.append(waiting_line)
    if paused_for_upgrade and pill_label == "QUEUED":
        parts.append(_PAUSED_FOR_UPGRADE_LINE)
    if base_line is not None:
        parts.append(base_line)
    if result is not None:
        parts.append(FINAL_MARKER)
    parts.append(marker_for(request_id))
    return _redact_factory_comment("\n\n".join(parts) + "\n")


def _digest(body: str) -> str:
    return hashlib.sha256(body.encode()).hexdigest()


def _desired_label_sql() -> Any:
    return case(
        *[
            (ExecutionRequest.status == status, literal(label))
            for status, label in _DESIRED_LABEL.items()
        ],
        else_=literal(""),
    )


async def sync_status_comments(
    session: AsyncSession,
    settings: Settings,
    *,
    limit: int = 20,
    paused_for_upgrade: bool = False,
) -> int:
    """Create, edit, finalize and label every due status comment this pass can lock.

    Returns the number of GitHub writes. The row lock is held across the GitHub
    calls so a second reconciler skips it. A crash before commit leaves the row
    as it was; the next pass finds a created comment by its marker instead of
    posting another.
    """

    later = aliased(ExecutionRequest)
    is_latest = and_(
        ExecutionRequest.status != "queued",
        or_(
            ExecutionRequest.status != "cancelled",
            ExecutionRequest.wait_deadline.is_not(None),
        ),
        ~exists().where(
            later.work_item_id == ExecutionRequest.work_item_id,
            later.sequence > ExecutionRequest.sequence,
            later.status != "queued",
            or_(later.status != "cancelled", later.wait_deadline.is_not(None)),
        ),
    )
    label_due = and_(
        is_latest,
        FactoryStatusComment.applied_label.is_distinct_from(""),
        FactoryStatusComment.applied_label.is_distinct_from(_desired_label_sql()),
    )
    rows = (
        await session.execute(
            select(
                FactoryStatusComment,
                WorkItem,
                ExecutionRequest,
                ThreadPublicationLineage.pr_url,
                is_latest.label("is_latest"),
            )
            .join(WorkItem, WorkItem.id == FactoryStatusComment.work_item_id)
            .join(
                ExecutionRequest,
                ExecutionRequest.id == FactoryStatusComment.execution_request_id,
            )
            .outerjoin(
                ThreadPublicationLineage,
                ThreadPublicationLineage.id == WorkItem.publication_lineage_id,
            )
            .where(
                FactoryStatusComment.refused_at.is_(None),
                ExecutionRequest.objective.is_not(None),
                or_(FactoryStatusComment.finalized_at.is_(None), label_due),
            )
            .order_by(
                FactoryStatusComment.attempts,
                FactoryStatusComment.created_at,
            )
            .limit(limit)
            .with_for_update(skip_locked=True, of=FactoryStatusComment)
        )
    ).all()
    if not rows:
        await session.commit()
        return 0
    writes = 0
    async with httpx.AsyncClient(timeout=settings.github_app_timeout_seconds) as client:
        for row, work_item, request, pr_url, latest in rows:
            writes += await _sync_one(
                session,
                client,
                settings,
                row,
                work_item,
                request,
                pr_url=pr_url,
                latest=bool(latest),
                paused_for_upgrade=paused_for_upgrade,
            )
            row.attempts += 1
    await session.commit()
    return writes


async def _sync_one(
    session: AsyncSession,
    client: httpx.AsyncClient,
    settings: Settings,
    row: FactoryStatusComment,
    work_item: WorkItem,
    request: ExecutionRequest,
    *,
    pr_url: str | None,
    latest: bool,
    paused_for_upgrade: bool = False,
) -> int:
    target = stored_reply_target(request, work_item, github_host(settings.github_html_base))
    try:
        token = await run_in_threadpool(
            credentials_for(settings).token_for_verified_installation,
            work_item.repo_full_name,
            work_item.github_installation_id,
        )
    except (GitHubInstallationRefused, GitHubAppError, ValueError):
        return 0
    github = _GitHub(
        client=client,
        api=settings.github_api_url.rstrip("/"),
        repo_path=f"/repos/{repo_url_path(work_item.repo_full_name)}",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    writes = 0
    if row.finalized_at is None:
        writes += await _sync_comment(
            session,
            github,
            settings,
            row,
            work_item,
            request,
            target,
            pr_url=pr_url,
            paused_for_upgrade=paused_for_upgrade,
        )
    if latest and row.refused_at is None:
        writes += await _sync_labels(github, row, work_item, request.status)
    return writes


async def _sync_comment(
    session: AsyncSession,
    github: _GitHub,
    settings: Settings,
    row: FactoryStatusComment,
    work_item: WorkItem,
    request: ExecutionRequest,
    target: ReplyTarget,
    *,
    pr_url: str | None,
    paused_for_upgrade: bool = False,
) -> int:
    if row.subject_title is None:
        row.subject_title = await _subject_title(github, work_item, target)
    body = await _render(
        session,
        settings,
        row,
        work_item,
        request,
        target,
        pr_url=pr_url,
        paused_for_upgrade=paused_for_upgrade,
    )
    terminal = FINAL_MARKER in body
    writes = 0
    if row.comment_id is None:
        outcome = await _deliver(github, work_item, row, target, body)
        if outcome is None:
            return 0
        now = await _clock(session)
        if outcome[0] == "refused":
            row.refusal = outcome[1]
            row.refused_at = now
            return 0
        _kind, comment_id, comment_list, created = outcome
        row.comment_id = comment_id
        row.comment_list = comment_list
        row.posted_at = now
        # A comment found by its marker carries an unknown body: edit it below.
        row.rendered_digest = _digest(body) if created else None
        writes += int(created)
    if row.rendered_digest != _digest(body):
        edited = await _patch(github, row, body)
        if edited == "edited":
            row.rendered_digest = _digest(body)
            writes += 1
        elif edited == "gone":
            # A person deleted the comment: re-create it next pass, marker scan first.
            row.comment_id = None
            row.comment_list = None
            row.posted_at = None
            row.rendered_digest = None
            row.scan_page = 1
            return writes
        elif edited is not None:
            # The one-outcome check keeps a refused row uncreated.
            row.comment_id = None
            row.comment_list = None
            row.posted_at = None
            row.rendered_digest = None
            row.refusal = edited
            row.refused_at = await _clock(session)
            return writes
        else:
            return writes
    if terminal:
        row.finalized_at = await _clock(session)
    return writes


async def _render(
    session: AsyncSession,
    settings: Settings,
    row: FactoryStatusComment,
    work_item: WorkItem,
    request: ExecutionRequest,
    target: ReplyTarget,
    *,
    pr_url: str | None,
    paused_for_upgrade: bool = False,
) -> str:
    cause = row.terminal_cause or request.terminal_cause
    result: str | None = None
    if request.terminal_at is not None and cause:
        cause = cause.strip()
        # A completed issue run waits for its PR link before it is final.
        if not (
            cause == "completed"
            and target.kind == "issue"
            and (not isinstance(pr_url, str) or not pr_url.strip())
        ):
            result = result_section(
                cause,
                pr_url=pr_url,
                feedback_url=request.reply_target_url,
                detail=row.detail,
                superseded=cause == "issue_cancelled"
                and await _superseded(session, work_item, request),
            )
            # Tokens and estimated cost over every round of the work item
            # (#3223), after the Cause line so its parse is unchanged.
            usage = usage_line(await work_item_usage(session, work_item.id))
            if usage is not None:
                result = result.rstrip("\n") + "\n" + usage + "\n"
    publishing = (
        await session.scalar(
            select(Publication.id)
            .where(
                Publication.execution_request_id == request.id,
                Publication.status.in_(_PUBLISHING_STATUSES),
            )
            .limit(1)
        )
    ) is not None
    view: PhaseView | None = None
    if row.declaration is not None:
        reports = list(
            await session.scalars(
                select(ExecutionRequestPhaseReport)
                .where(ExecutionRequestPhaseReport.execution_request_id == request.id)
                .order_by(ExecutionRequestPhaseReport.id)
            )
        )
        view = phase_view(row.declaration, reports, request.status, cause)
    pill_label, _color, _live = pill_for(request.status, publishing)
    pending_count = await session.scalar(
        select(func.count(ExecutionRequest.id)).where(
            ExecutionRequest.work_item_id == work_item.id,
            ExecutionRequest.status == "queued",
        )
    )
    waiting_line: str | None = None
    if request.status == "queued":
        waiting_line = (
            "This revision is waiting on the current run. It will start when that run finishes."
        )
    elif request.status in {"waiting", "running", "cancellation_requested"} and pending_count:
        word = "revision" if pending_count == 1 else "revisions"
        waiting_line = f"{pending_count} {word} waiting on this run."
    base = settings.github_factory_card_base_url
    return status_body(
        request_id=row.execution_request_id,
        card_url=f"{base}/v1/factory/cards/{row.card_token}.svg" if base else None,
        pill_label=pill_label,
        phase_view=view,
        result=result,
        paused_for_upgrade=paused_for_upgrade,
        waiting_line=waiting_line,
        base_line=render_base_line(work_item),
    )


async def _superseded(
    session: AsyncSession, work_item: WorkItem, request: ExecutionRequest
) -> bool:
    """A relabel replaced this run: a later request exists or is pending."""

    if work_item.readmit_request_id is not None:
        return True
    later = await session.scalar(
        select(ExecutionRequest.id)
        .where(
            ExecutionRequest.work_item_id == work_item.id,
            ExecutionRequest.sequence > request.sequence,
        )
        .limit(1)
    )
    return later is not None


async def _sync_labels(
    github: _GitHub, row: FactoryStatusComment, work_item: WorkItem, status: str
) -> int:
    """Add the desired state label and remove the others, on the issue.

    Only the four state labels are ever written. A refused write is logged and
    given up; any other failure is retried next pass.
    """

    if row.applied_label == "":
        return 0
    desired = desired_label(status)
    if row.applied_label == desired:
        return 0
    labels_path = f"{github.api}{github.repo_path}/issues/{work_item.github_issue_number}/labels"
    writes = 0
    complete = True
    if desired:
        try:
            added = await github.client.post(
                labels_path,
                headers=github.headers,
                json={"labels": [desired]},
                follow_redirects=False,
            )
        except httpx.HTTPError:
            return writes
        writes += 1
        if added.status_code in _REFUSED_STATUSES:
            logger.warning(
                "factory state label refused",
                extra={"work_item_id": str(work_item.id), "status": added.status_code},
            )
        elif added.status_code not in {200, 201}:
            return writes
    # Legacy deletes stay unconditional. A new request starts with
    # applied_label NULL, and the issue may still carry a legacy name.
    for name in (*STATE_LABELS, *LEGACY_STATE_LABELS):
        if name == desired:
            continue
        try:
            removed = await github.client.delete(
                f"{labels_path}/{quote(name, safe=':')}",
                headers=github.headers,
                follow_redirects=False,
            )
        except httpx.HTTPError:
            complete = False
            continue
        writes += 1
        # 404: the label was not on the issue, which is the goal.
        if removed.status_code in {401, 403}:
            logger.warning(
                "factory state label removal refused",
                extra={"work_item_id": str(work_item.id), "status": removed.status_code},
            )
        elif removed.status_code not in {200, 204, 404}:
            complete = False
    if complete:
        row.applied_label = desired
    return writes


async def _clock(session: AsyncSession) -> Any:
    return await session.scalar(select(func.clock_timestamp()))
