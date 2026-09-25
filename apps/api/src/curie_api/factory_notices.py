"""Keep the one live status comment of each factory execution request (#3077).

The status row is inserted with the request at admission; the terminus stages
its cause and detail on it. This module creates the comment, edits it in place
whenever its rendered body changes, finalizes it once the result is shown, and
moves the ``curie:*`` state labels on the WorkItem's issue. There is no
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
import re
import uuid
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import quote

import httpx
from sqlalchemy import and_, case, exists, func, literal, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from starlette.concurrency import run_in_threadpool

from .config import Settings
from .factory_progress import PhaseView, phase_view, pill_for
from .factory_reply_target import ReplyTarget, parse_reply_target
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

_REFUSED_STATUSES = {401, 403, 404}
_PAGES_PER_PASS = 5
_UNPROCESSABLE = ("unprocessable", "http_422")
# A thread target scans two lists with one stored page. Pages of the review
# comment list are stored as-is; once that list is exhausted, the conversation
# list page is stored above this offset.
_SECOND_LIST_OFFSET = 1_000_000


@dataclass(frozen=True)
class _MarkerScan:
    comment_id: int | None = None
    refusal: str | None = None
    unavailable: bool = False
    next_page: int | None = None


# The operator-facing sentence for each terminus cause (#3073). The raw code
# still appears on the comment, but never as its headline.
_CAUSE_TEXT = {
    "model_credit_exhausted": (
        "the model provider refused the request because the account has run "
        "out of credits. Add credits or raise the key's limit, then retry."
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
    "budget_exceeded": "the run used its whole token budget before it finished.",
    "runner_timeout": "the run took longer than its time limit.",
    "workspace_error": "the repository workspace could not be prepared for the run.",
    "runner_escalated": "the run stopped on an error and was handed to a person.",
    "runner_failed": "the run ended without a result.",
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

# The CI gate's causes (#3097) carry their own labelled lines, not a provider message.
_CI_DETAIL_CAUSES = frozenset({"ci_failed", "ci_timeout", "ci_unverified"})
# A run that ended without publishing carries the agent's own last message
# (#3128). That text is model-authored, so it renders inert inside a code fence.
_AGENT_MESSAGE_CAUSES = frozenset({"early_stop", "no_pull_request"})
_BACKTICK_RUN = re.compile(r"`+")


def cause_text(cause: str) -> str:
    """A plain sentence for a terminus cause; unknown codes get a generic one."""

    return _CAUSE_TEXT.get(cause, "the run stopped for a reason Curie did not recognize.")


def marker_for(request_id: uuid.UUID) -> str:
    return f"<!-- curie-execution-request:{request_id} -->"


FINAL_MARKER = "<!-- curie-status:final -->"

# The four state labels the pass owns (#3077). A closed set, never a prefix
# match: human labels, including other ``curie:`` ones, are never touched.
STATE_LABELS = ("curie:queued", "curie:running", "curie:pr-open", "curie:needs-human")
_DESIRED_LABEL = {
    "waiting": "curie:queued",
    "running": "curie:running",
    "cancellation_requested": "curie:running",
    "completed": "curie:pr-open",
    "failed": "curie:needs-human",
    "expired": "curie:needs-human",
    # Cancelled clears all four; '' records that nothing is applied.
    "cancelled": "",
}
_PUBLISHING_STATUSES = ("pending", "approved", "launching", "running")
_WAITING_FOR_PROGRESS = "_Waiting for the agent to report progress._"
_MARKDOWN_SPECIAL = set("\\`*_[]()#<>!|")


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
    else:
        text = f"Could not complete: {cause_text(cause)}\n"
        if cause in _AGENT_MESSAGE_CAUSES and detail is not None and detail.strip():
            text += _agent_message_block(detail.strip())
        elif detail is not None and detail.strip():
            label = "Details" if cause in _CI_DETAIL_CAUSES else "Provider message"
            text += f"{label}: {detail.strip()}\n"
        text += f"Cause: {cause}\n"
    if feedback_url is not None:
        text += f"In response to {feedback_url}\n"
    return text


def _agent_message_block(message: str) -> str:
    """The agent's last message, fenced so GitHub renders none of it.

    The fence is longer than any backtick run in the message, so the message
    cannot close it and spoof the ``Cause:`` line. HTML comment openers are
    broken because the marker scan reads the raw body.
    """

    message = message.replace("<!--", "<\u200b!--")
    longest = max((len(run) for run in _BACKTICK_RUN.findall(message)), default=0)
    fence = "`" * max(3, longest + 1)
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
    if result is not None:
        parts.append(FINAL_MARKER)
    parts.append(marker_for(request_id))
    return "\n\n".join(parts) + "\n"


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
    session: AsyncSession, settings: Settings, *, limit: int = 20
) -> int:
    """Create, edit, finalize and label every due status comment this pass can lock.

    Returns the number of GitHub writes. The row lock is held across the GitHub
    calls so a second reconciler skips it. A crash before commit leaves the row
    as it was; the next pass finds a created comment by its marker instead of
    posting another.
    """

    later = aliased(ExecutionRequest)
    is_latest = ~exists().where(
        later.work_item_id == ExecutionRequest.work_item_id,
        later.sequence > ExecutionRequest.sequence,
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
            )
            row.attempts += 1
    await session.commit()
    return writes


@dataclass(frozen=True)
class _GitHub:
    client: httpx.AsyncClient
    api: str
    repo_path: str
    headers: dict[str, str]


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
) -> int:
    assert request.objective is not None
    target = parse_reply_target(
        request.objective,
        repo_full_name=work_item.repo_full_name,
        clone_base=settings.github_clone_base,
    )
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
            session, github, settings, row, work_item, request, target, pr_url=pr_url
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
) -> int:
    if row.subject_title is None:
        row.subject_title = await _subject_title(github, work_item, target)
    body = await _render(session, settings, row, work_item, request, target, pr_url=pr_url)
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
                feedback_url=target.url,
                detail=row.detail,
                superseded=cause == "issue_cancelled"
                and await _superseded(session, work_item, request),
            )
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
    base = settings.github_factory_card_base_url
    return status_body(
        request_id=row.execution_request_id,
        card_url=f"{base}/v1/factory/cards/{row.card_token}.svg" if base else None,
        pill_label=pill_label,
        phase_view=view,
        result=result,
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


async def _subject_title(github: _GitHub, work_item: WorkItem, target: ReplyTarget) -> str | None:
    """The issue or PR title for the card, read once. A failed read stays NULL."""

    if target.pr_number is None:
        path = f"{github.repo_path}/issues/{work_item.github_issue_number}"
    else:
        path = f"{github.repo_path}/pulls/{target.pr_number}"
    try:
        found = await github.client.get(
            f"{github.api}{path}", headers=github.headers, follow_redirects=False
        )
        payload = found.json() if found.status_code == 200 else None
    except (httpx.HTTPError, ValueError):
        return None
    if isinstance(payload, dict) and isinstance(payload.get("title"), str):
        return str(payload["title"])[:256]
    return None


async def _sync_labels(
    github: _GitHub, row: FactoryStatusComment, work_item: WorkItem, status: str
) -> int:
    """Add the desired state label and remove the other three, on the issue.

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
    for name in STATE_LABELS:
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


CommentList = Literal["issue", "review"]


async def _deliver(
    github: _GitHub,
    work_item: WorkItem,
    row: FactoryStatusComment,
    target: ReplyTarget,
    body: str,
) -> tuple[Literal["refused"], str] | tuple[Literal["posted"], int, CommentList, bool] | None:
    """Create the comment, or find one a lost response already created.

    ``posted`` carries the comment id, the list it lives in, and whether this
    call created it with ``body``.
    """

    api, repo_path, headers, client = github.api, github.repo_path, github.headers, github.client
    number = work_item.github_issue_number if target.pr_number is None else target.pr_number
    comments_path = f"{repo_path}/issues/{number}/comments"
    marker = marker_for(row.execution_request_id)
    stored = max(1, row.scan_page)
    # (path, first page, offset stored for this list's next page, list)
    scans: list[tuple[str, int, int, CommentList]] = [(comments_path, stored, 0, "issue")]
    if target.kind == "thread":
        # A thread reply lands on the review comment list; its 422 fallback on
        # the conversation list. The marker may sit on either.
        review_path = f"{repo_path}/pulls/{number}/comments"
        if stored > _SECOND_LIST_OFFSET:
            scans = [(comments_path, stored - _SECOND_LIST_OFFSET, _SECOND_LIST_OFFSET, "issue")]
        else:
            scans = [
                (review_path, stored, 0, "review"),
                (comments_path, 1, _SECOND_LIST_OFFSET, "issue"),
            ]
    for path, start, offset, listed in scans:
        existing = await _find_marker(client, api, path, headers, marker, start_page=start)
        if existing.refusal is not None:
            return ("refused", existing.refusal)
        if existing.unavailable:
            return None
        if existing.comment_id is not None:
            return ("posted", existing.comment_id, listed, False)
        if existing.next_page is not None:
            row.scan_page = existing.next_page + offset
            return None
    if target.kind == "thread":
        assert target.comment_id is not None
        root = await _thread_root(client, api, repo_path, headers, target.comment_id)
        replied = await _post(
            client,
            f"{api}{repo_path}/pulls/{number}/comments/{root}/replies",
            headers,
            body,
        )
        if replied != _UNPROCESSABLE:
            return _created(row, replied, "review")
    posted = await _post(client, f"{api}{comments_path}", headers, body)
    # A comment GitHub cannot process stays pending, as before #2798.
    if posted == _UNPROCESSABLE:
        return None
    return _created(row, posted, "issue")


def _created(
    row: FactoryStatusComment, outcome: tuple[str, str] | None, listed: CommentList
) -> tuple[Literal["refused"], str] | tuple[Literal["posted"], int, CommentList, bool] | None:
    if outcome is None:
        # Lost response after a possible success: rescan every list next pass.
        row.scan_page = 1
        return None
    if outcome[0] == "refused":
        return ("refused", outcome[1])
    return ("posted", int(outcome[1]), listed, True)


async def _patch(
    github: _GitHub, row: FactoryStatusComment, body: str
) -> Literal["edited", "gone"] | str | None:
    """Edit the comment in place: ``edited``, ``gone`` (404), a refusal, or None."""

    kind = "pulls" if row.comment_list == "review" else "issues"
    url = f"{github.api}{github.repo_path}/{kind}/comments/{row.comment_id}"
    try:
        edited = await github.client.patch(
            url, headers=github.headers, json={"body": body}, follow_redirects=False
        )
    except httpx.HTTPError:
        return None
    if edited.status_code == 200:
        return "edited"
    if edited.status_code == 404:
        return "gone"
    if edited.status_code in {401, 403}:
        return f"http_{edited.status_code}"
    return None


async def _thread_root(
    client: httpx.AsyncClient,
    api: str,
    repo_path: str,
    headers: dict[str, str],
    comment_id: int,
) -> int:
    """Reply to the thread's first comment; replies to replies are refused."""

    try:
        found = await client.get(
            f"{api}{repo_path}/pulls/comments/{comment_id}",
            headers=headers,
            follow_redirects=False,
        )
        payload = found.json() if found.status_code == 200 else None
    except (httpx.HTTPError, ValueError):
        payload = None
    if isinstance(payload, dict):
        root = payload.get("in_reply_to_id")
        if type(root) is int and root > 0:
            return root
    return comment_id


async def _post(
    client: httpx.AsyncClient, url: str, headers: dict[str, str], body: str
) -> tuple[str, str] | None:
    try:
        created = await client.post(
            url, headers=headers, json={"body": body}, follow_redirects=False
        )
    except httpx.HTTPError:
        return None
    if created.status_code == 422:
        return _UNPROCESSABLE
    if created.status_code in _REFUSED_STATUSES:
        return ("refused", f"http_{created.status_code}")
    if created.status_code not in {200, 201}:
        return None
    try:
        payload = created.json()
    except ValueError:
        return None
    if not isinstance(payload, dict) or type(payload.get("id")) is not int:
        return None
    return ("posted", str(payload["id"]))


async def _find_marker(
    client: httpx.AsyncClient,
    api: str,
    path: str,
    headers: dict[str, str],
    marker: str,
    *,
    start_page: int,
) -> _MarkerScan:
    """Find a marker, or remember the next page so a later pass can continue.

    A full page does not mean the marker is absent. Stopping there and posting
    would duplicate a comment that sits further down the list. A short page is
    the end, so a missing marker is safe to post.
    """

    page = start_page
    for _ in range(_PAGES_PER_PASS):
        try:
            listed = await client.get(
                f"{api}{path}",
                headers=headers,
                params={"per_page": 100, "page": page},
                follow_redirects=False,
            )
        except httpx.HTTPError:
            return _MarkerScan(unavailable=True)
        if listed.status_code in _REFUSED_STATUSES:
            return _MarkerScan(refusal=f"http_{listed.status_code}")
        if listed.status_code != 200:
            return _MarkerScan(unavailable=True)
        try:
            payload = listed.json()
        except ValueError:
            return _MarkerScan(unavailable=True)
        found = _comment_id(payload, marker)
        if found is not None:
            return _MarkerScan(comment_id=found)
        if not isinstance(payload, list) or len(payload) < 100:
            return _MarkerScan()
        page += 1
    return _MarkerScan(next_page=page)


def _comment_id(payload: Any, marker: str) -> int | None:
    if not isinstance(payload, list):
        return None
    for item in payload:
        if not isinstance(item, dict):
            continue
        body = item.get("body")
        comment_id = item.get("id")
        if isinstance(body, str) and marker in body and type(comment_id) is int:
            return comment_id
    return None
