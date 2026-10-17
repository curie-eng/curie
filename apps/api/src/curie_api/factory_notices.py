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
State labels move through the tracker port's ``set_state_label``.
"""

from __future__ import annotations

import hashlib
import logging
import math
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import httpx
from sqlalchemy import and_, case, exists, func, literal, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased
from starlette.concurrency import run_in_threadpool

from .code_host_trust import code_host_verify
from .config import Settings
from .factory_comment_text import marker_for, redact_factory_comment, status_marker
from .factory_progress import PhaseView, phase_view, pill_for
from .factory_reply_target import stored_reply_target
from .factory_usage import format_usd, usage_line, work_item_usage
from .forges.capabilities import Operation, Support
from .forges.errors import ForgeError, Unauthorized
from .forges.github.comments import static_token
from .forges.github.marked_comments import (
    GitHubCalls,
    GitHubCommentClient,
    deliver,
    github_calls,
    patch_comment,
    subject_title,
)
from .forges.github.transport import github_headers
from .forges.hosts import factory_credentials, factory_tracker
from .forges.ports import CodeHost, Tracker, comments_for
from .forges.registry import forges_registry
from .forges.trackers import code_host_for_repository, tracker_for_work_item
from .forges.types import GITHUB, PullRequestRef, ReplyTarget, RepositoryRef, TrackerIssueRef
from .github_app import GitHubAppError, GitHubInstallationRefused
from .models import (
    ExecutionRequest,
    ExecutionRequestPhaseReport,
    FactoryStatusComment,
    Publication,
    ThreadPublicationLineage,
    WorkItem,
)
from .publication_truth import conversation_pr_lineage_id
from .repo_full_name import repo_url_path
from .workitems.lifecycle import (
    OWNER_LOST_RETRY_LIMIT,
    owner_lost_streak,
    owner_lost_successor_admitted,
)

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
    "model_unreachable": (
        "the model provider could not be reached. Check the runner's network path "
        "to the model endpoint, then retry."
    ),
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
    "pull_request_not_adopted": (
        "an earlier pull request on this issue could not be continued. "
        "A person should close or merge it, then re-add the label."
    ),
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
    "start_failed": "the sandbox did not start.",
    "owner_lost": "the worker running this request stopped responding.",
    "issue_cancelled": "the request was cancelled.",
    "publication_denied": "a person denied the request to open the pull request.",
    "publication_expired": (
        "the request to open the pull request expired before anyone approved it."
    ),
    "publication_failed": "the pull request could not be opened.",
    "publication_unrecorded": (
        "the agent tried to publish its change, but no valid publication was recorded. "
        "The Details line below gives the reason; the pull request, if any, is unchanged."
    ),
    "ci_failed": (
        "the pull request's checks still failed after 3 rounds of fixes. The pull "
        "request stays open for a person."
    ),
    "ci_timeout": (
        "the pull request's checks did not finish before the CI wait ran out. The "
        "pull request stays open."
    ),
    "ci_unverified": (
        "the pull request's CI could not be verified, so the run did not complete. "
        "The Reason line below says why. The pull "
        "request stays open; check it yourself."
    ),
    "merge_conflict": (
        "the pull request has merge conflicts with its base branch, so GitHub ran "
        "no pull request checks. The pull request stays open; resolve the conflicts "
        "to continue."
    ),
    "ci_fix_unpublished": (
        "a CI fix round ended without pushing a fix. The pull request stays open."
    ),
}

# Infrastructure and CI causes carry details, not a provider message.
_DETAIL_CAUSES = frozenset(
    {
        "sandbox_terminated",
        "ci_failed",
        "ci_timeout",
        "ci_unverified",
        "merge_conflict",
        "approval_create_failed",
        "publication_unrecorded",
    }
)
# A run that ended without publishing carries the agent's own last message
# (#3128). Quote each line and neutralize HTML, image syntax and mentions.
_AGENT_MESSAGE_CAUSES = frozenset({"early_stop", "no_pull_request"})
# Wire classification a text-only consumer reads off the status comment (#3401).
# Same tokens as the channel reply's ``curie-turn-failure:`` line. Causes with
# no entry stay unlabeled rather than inventing a class.
_FAILURE_CLASS_BY_CAUSE = {
    "unclassified": "unclassified",
    "max_turns": "max-turns",
    "history_capacity": "history-persistence-error",
    "model_credit_exhausted": "model-credit-exhausted",
    "model_usage_limited": "model-usage-limited",
    "model_credential_rejected": "model-credential-rejected",
    "model_rate_limited": "rate-limit",
    "model_error": "server-error",
    "model_unreachable": "model-unreachable",
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
# Horizontal space only: a label must not cross into the next publisher line
# and swallow ``Cause:``. Quotes accept JSON escapes so an inner \" does not
# end the value early. Emphasis or backticks may wrap the label, as in
# ``**key_id**:`` or `` `workspace`: ``.
# ``_`` is a word character, so ``\b`` misses ``_key_id_``. Bound the label
# on letters and digits instead, then allow emphasis on either side.
# The name phrase stays case sensitive so lowercase diagnostic prose
# (``failed to prepare the repository``) is not a workspace name. A capitalized
# phrase (``Acme Research Team``) is. A slug still matches in any case.
# ``workspace=acme`` is an assignment, not diagnostic prose, so the value may
# be a plain word. The colon form stays on ``_PROVIDER_WORKSPACE``.


def cause_text(cause: str) -> str:
    """A plain sentence for a terminus cause; unknown codes get a generic one."""

    return _CAUSE_TEXT.get(cause, "the run stopped for a reason Curie did not recognize.")


def start_failed_sentence(attempts: int, reason: str) -> str:
    """The ``start_failed`` sentence naming the attempt count and last reason (#4170)."""

    return f"the sandbox did not start after {attempts} attempts. Last reason: {reason}."


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
    """Let the reconciler edit the latest status comment once more.

    Its body carries the ignored-label note, so a change to that note must reach
    a comment that was already finalized. A sync holding the lease is flagged so
    its writeback leaves the row due. The comment re-finalizes after the edit.
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
            or_(
                FactoryStatusComment.finalized_at.is_not(None),
                FactoryStatusComment.sync_owner.is_not(None),
            ),
            FactoryStatusComment.refused_at.is_(None),
        )
        .values(
            finalized_at=None,
            sync_invalidated=FactoryStatusComment.sync_owner.is_not(None),
        )
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
    unchanged: bool = False,
    superseded: bool = False,
    lost_streak: int = 0,
    lost_retried: bool = False,
    run_cost_usd: Decimal | None = None,
) -> str:
    """The terminal result lines of a status comment, without any marker.

    For ``owner_lost``, ``lost_streak`` is the consecutive losses through
    this request and ``lost_retried`` whether its settlement admitted a
    successor (ADR 0206); together they choose the retry or exhausted sentence.
    ``run_cost_usd`` is this request's recorded estimate, not the WorkItem total.
    """

    if cause == "completed":
        if unchanged:
            if feedback_url is not None:
                text = (
                    "No changes needed: this pull request already covers the requested revision.\n"
                )
            elif isinstance(pr_url, str) and pr_url.strip():
                text = (
                    "No changes needed: the open pull request already covers this request: "
                    f"{pr_url.strip()}\n"
                )
            else:
                raise ValueError("a completed issue notice requires its pull request URL")
            if detail is not None and detail.strip():
                text += _agent_message_block(detail.strip())
        elif feedback_url is not None:
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
        prefix = "Could not complete: "
        if (
            cause == "approval_create_failed"
            and detail is not None
            and detail.startswith("publication snapshot could not be read")
        ):
            sentence = (
                "Curie could not read the finished changes from the sandbox, so no pull request "
                "was opened. This was an infrastructure failure, not a refusal of the change; "
                "retry the run."
            )
        elif cause == "owner_lost" and lost_streak == OWNER_LOST_RETRY_LIMIT:
            sentence = (
                "the worker running this request stopped responding "
                f"{OWNER_LOST_RETRY_LIMIT} times."
            )
        elif cause == "owner_lost" and lost_retried and 1 <= lost_streak < OWNER_LOST_RETRY_LIMIT:
            prefix = "Retrying: "
            sentence = (
                "the worker running this request stopped responding. Curie started "
                "the work again as a new run "
                f"(attempt {lost_streak + 1} of {OWNER_LOST_RETRY_LIMIT})."
            )
        elif cause == "budget_exceeded":
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
                if run_cost_usd is not None and run_cost_usd < Decimal(usd_budget[1]):
                    sentence = (
                        "the run stopped on the agent SDK's own USD estimate, which reached "
                        f"the cap (max_usd_per_day={usd_budget[1]}), while Curie's recorded "
                        f"estimate for this run is {format_usd(run_cost_usd)}. "
                        "The SDK prices this model above its provider price, so raising the "
                        "cap only delays the stop. To raise it anyway, run "
                        "`curie cluster budget <agent> --limit <usd>`, then retry."
                    )
                else:
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
        if cause == "start_failed" and detail is not None and detail.strip():
            # Curie writes this sentence (#4170), but it quotes the worker's
            # deferral reason: one line, and no HTML comment opener.
            sentence = _inert_line(detail)
        text = f"{prefix}{sentence}\n"
        if cause in _AGENT_MESSAGE_CAUSES and detail is not None and detail.strip():
            text += _agent_message_block(detail.strip())
        elif cause == "approval_create_failed" and detail is not None and detail.strip():
            # The API refusal can quote caller-supplied paths (#3617): one line,
            # so it cannot add a ``Cause:`` line, and no HTML comment opener.
            text += f"Details: {_inert_line(detail)}\n"
        elif (
            cause not in {"history_capacity", "start_failed", "pull_request_not_adopted"}
            and detail is not None
            and detail.strip()
        ):
            label = "Details" if cause in _DETAIL_CAUSES else "Provider message"
            text += f"{label}: {detail.strip()}\n"
        text += f"Cause: {cause}\n"
        if cause == "pull_request_not_adopted" and isinstance(pr_url, str) and pr_url.strip():
            text += f"Pull request: {pr_url.strip()}\n"
        failure_class = _FAILURE_CLASS_BY_CAUSE.get(cause)
        if failure_class is not None:
            text += f"Failure class: {failure_class}\n"
    if feedback_url is not None:
        text += f"In response to {feedback_url}\n"
    return text


def _break_html_comments(text: str) -> str:
    return text.replace("<!--", "<\u200b!--")


def _inert_line(text: str) -> str:
    return _break_html_comments(" ".join(text.split()))


def _agent_message_block(message: str) -> str:
    """The agent's last message as a neutralized Markdown blockquote.

    Normalize CRLF and lone CR to LF before quoting every line, so the message
    cannot spoof a top-level ``Cause:`` line. Break HTML comment openers for
    the raw marker scan, then escape ``<`` and ``![`` and insert U+200B after
    every ``@`` to neutralize HTML, images and mentions. A blank line separates
    the quote from Curie's following lines.
    """

    message = message.replace("\r\n", "\n").replace("\r", "\n")
    message = _break_html_comments(message)
    message = message.replace("<", "&lt;").replace("![", "!\\[").replace("@", "@\u200b")
    quoted = "\n".join(f"> {line}" if line else ">" for line in message.split("\n"))
    return f"Agent's last message:\n{quoted}\n\n"


def _escape_markdown(value: str) -> str:
    return "".join(f"\\{char}" if char in _MARKDOWN_SPECIAL else char for char in value)


def _checklist(view: PhaseView, *, final: bool) -> list[str]:
    lines: list[str] = []
    for slot in view.phases:
        label = _escape_markdown(slot.label)
        if slot.state == "done":
            lines.append(f"- [x] {label}")
        elif slot.state == "current":
            detail = "stopped here" if final else "in progress"
            if not final and slot.round_label is not None and slot.round_label.startswith("round "):
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

    Live progress notes stay on the card; a final budget result includes its
    last nonempty note as one inert line. A final result can also include the
    agent's last message as a neutralized Markdown quote. With a card, the checklist
    and waiting placeholder are left out; without one the checklist is the
    fallback (#3125).
    """

    parts: list[str] = []
    if result is not None:
        parts.append(result.rstrip("\n"))
    if card_url:
        parts.append(f"![Curie status]({card_url})")
    elif phase_view is not None and phase_view.phases:
        parts.append("\n".join(_checklist(phase_view, final=result is not None)))
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
    return redact_factory_comment("\n\n".join(parts) + "\n")


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
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    *,
    owner: str,
    limit: int = 20,
    paused_for_upgrade: bool = False,
) -> int:
    """Claim, render, release the session, call GitHub, then persist under the lease.

    Returns the number of GitHub writes. A crashed claimer's lease expires;
    marker recovery finds a comment whose create response was lost.
    """

    writes = 0
    attempted_ids: set[uuid.UUID] = set()
    async with httpx.AsyncClient(
        timeout=settings.github_app_timeout_seconds, verify=code_host_verify(settings)
    ) as client:
        for _ in range(limit):
            async with sessionmaker() as session:
                claim = await _claim_next(
                    session,
                    settings,
                    owner=owner,
                    attempted_ids=attempted_ids,
                    paused_for_upgrade=paused_for_upgrade,
                )
                await session.commit()
            if claim is None:
                break
            attempted_ids.add(claim.row.execution_request_id)
            calls = GitHubCalls()
            token = github_calls.set(calls)
            started = time.monotonic()
            try:
                writes += await _sync_one(client, settings, claim)
                claim.row.attempts += 1
                async with sessionmaker() as session:
                    await _write_back(session, claim, owner=owner)
                    await session.commit()
            finally:
                github_calls.reset(token)
                elapsed_ms = (time.monotonic() - started) * 1000
                logger.info(
                    "factory status sync calls=%d elapsed_ms=%.1f",
                    calls.count,
                    elapsed_ms,
                    extra={"call_count": calls.count, "elapsed_ms": elapsed_ms},
                )
    return writes


@dataclass(frozen=True)
class _StatusClaim:
    row: FactoryStatusComment
    repository_path: str
    repository: RepositoryRef
    code_host_installation_id: int | None
    tracker_issue: TrackerIssueRef
    # The pull request this request opened on the lineage, linked back to the
    # ticket from this request's comment only (#4358 plan D6); None otherwise.
    opened_pr: int | None
    work_item_id: uuid.UUID
    status: str
    target: ReplyTarget
    body: str
    latest: bool
    now: datetime


async def _claim_next(
    session: AsyncSession,
    settings: Settings,
    *,
    owner: str,
    attempted_ids: set[uuid.UUID],
    paused_for_upgrade: bool,
) -> _StatusClaim | None:
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
    selected = (
        await session.execute(
            select(
                FactoryStatusComment,
                WorkItem,
                ExecutionRequest,
                ThreadPublicationLineage.pr_url,
                ThreadPublicationLineage.pr_number,
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
                or_(
                    FactoryStatusComment.sync_owner.is_(None),
                    FactoryStatusComment.sync_lease_expires_at <= func.clock_timestamp(),
                ),
                FactoryStatusComment.execution_request_id.not_in(attempted_ids),
            )
            .order_by(
                FactoryStatusComment.attempts,
                FactoryStatusComment.created_at,
            )
            .limit(1)
            .with_for_update(skip_locked=True, of=FactoryStatusComment)
        )
    ).one_or_none()
    if selected is None:
        return None
    row, work_item, request, pr_url, pr_number, latest = selected
    now = await _clock(session)
    await session.execute(
        update(FactoryStatusComment)
        .where(FactoryStatusComment.execution_request_id == row.execution_request_id)
        .values(
            sync_owner=owner,
            sync_lease_expires_at=func.clock_timestamp() + timedelta(seconds=300),
            sync_invalidated=False,
        )
        .execution_options(synchronize_session=False)
    )
    target = stored_reply_target(request, work_item)
    opened_pr = (
        pr_number
        if pr_number is not None
        and work_item.publication_lineage_id is not None
        and await session.scalar(
            select(Publication.execution_request_id)
            .where(Publication.lineage_id == work_item.publication_lineage_id)
            .order_by(Publication.created_at, Publication.id)
            .limit(1)
        )
        == request.id
        else None
    )
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
    claim = _StatusClaim(
        row=row,
        repository_path=work_item.repository_path,
        repository=work_item.repository,
        code_host_installation_id=work_item.code_host_installation_id,
        tracker_issue=work_item.tracker_issue,
        opened_pr=opened_pr,
        work_item_id=work_item.id,
        status=request.status,
        target=target,
        body=body,
        latest=bool(latest),
        now=now,
    )
    session.expunge(row)
    return claim


async def _write_back(session: AsyncSession, claim: _StatusClaim, *, owner: str) -> bool:
    row = claim.row
    written = await session.scalar(
        update(FactoryStatusComment)
        .where(
            FactoryStatusComment.execution_request_id == row.execution_request_id,
            FactoryStatusComment.sync_owner == owner,
        )
        .values(
            comment_id=row.comment_id,
            comment_list=row.comment_list,
            posted_at=row.posted_at,
            rendered_digest=row.rendered_digest,
            scan_page=row.scan_page,
            subject_title=row.subject_title,
            refusal=row.refusal,
            refused_at=row.refused_at,
            # An invalidation that landed during the claim keeps the row due.
            finalized_at=case(
                (FactoryStatusComment.sync_invalidated, None),
                else_=literal(row.finalized_at, FactoryStatusComment.finalized_at.type),
            ),
            applied_label=row.applied_label,
            attempts=row.attempts,
            sync_owner=None,
            sync_lease_expires_at=None,
            sync_invalidated=False,
        )
        .returning(FactoryStatusComment.execution_request_id)
    )
    if written is None:
        logger.info("factory status sync dropped stale owner writeback")
        return False
    return True


async def _sync_one(
    client: httpx.AsyncClient,
    settings: Settings,
    claim: _StatusClaim,
) -> int:
    row = claim.row
    if claim.tracker_issue.kind != GITHUB:
        return await _sync_through_ports(client, settings, claim)
    if claim.code_host_installation_id is None:
        return 0
    registry = forges_registry(settings)
    try:
        intake = factory_tracker(registry)
        token = await run_in_threadpool(
            factory_credentials(registry).token_for_verified_installation,
            claim.repository_path,
            claim.code_host_installation_id,
        )
    except (ForgeError, GitHubInstallationRefused, GitHubAppError, ValueError):
        return 0
    github = GitHubCommentClient(
        client=client,
        api=intake.config.api_url.rstrip("/"),
        repo_path=f"/repos/{repo_url_path(claim.repository_path)}",
        headers=github_headers(token),
    )
    writes = 0
    if row.finalized_at is None:
        writes += await _sync_comment(github, claim)
    if claim.latest and row.refused_at is None:
        tracker = intake.for_repository(
            client,
            repo_full_name=claim.repository_path,
            repository_id=int(claim.tracker_issue.scope_id),
            token=static_token(token),
        )
        writes += await _sync_labels(tracker, claim)
    return writes


async def _sync_through_ports(
    client: httpx.AsyncClient, settings: Settings, claim: _StatusClaim
) -> int:
    """A non-GitHub tracker's status comment and state label, through the ports
    (#4358 plan D6).

    The tracker and code host are the WorkItem's own: its tracker kind and its
    stored code host entry, never the install's GitHub accessor.
    """

    row = claim.row
    registry = forges_registry(settings)
    installation = claim.code_host_installation_id
    try:
        tracker = tracker_for_work_item(
            registry, client, claim.tracker_issue, claim.repository, installation
        )
        code_host = code_host_for_repository(
            registry,
            client,
            claim.repository,
            installations=None if installation is None else {claim.repository.path: installation},
        )
    except ForgeError:
        return 0
    writes = 0
    if row.finalized_at is None:
        writes += await _sync_port_comment(tracker, code_host, claim)
    if claim.latest and row.refused_at is None:
        writes += await _sync_labels(tracker, claim)
    return writes


async def _sync_port_comment(tracker: Tracker, code_host: CodeHost, claim: _StatusClaim) -> int:
    """Upsert the request's marked comment, then link its pull request once.

    An unchanged rendered body is skipped. The request that opened the
    lineage's pull request links it back to the ticket after its comment is
    current, once, when the tracker supports it; ``rendered_digest`` then
    records the linked number after the body digest, so a later body edit does
    not link again. A failed link leaves the digest unset, so the next pass
    retries both. A refused write is recorded as the row's refusal.
    """

    row, target, body, now = claim.row, claim.target, claim.body, claim.now
    digest = _digest(body)
    pr = claim.opened_pr
    link = pr is not None and (
        tracker.capabilities.get(Operation.LINK_PULL_REQUEST) == Support.SUPPORTED
    )
    linked = f"{digest}:{pr}"
    stored = row.rendered_digest
    if stored == (linked if link else digest):
        return 0
    already = link and stored is not None and stored.endswith(f":{pr}")
    try:
        result = await comments_for(target, tracker, code_host).upsert_marked(
            target, status_marker(row.execution_request_id), body
        )
    except Unauthorized:
        row.comment_id = None
        row.posted_at = None
        row.rendered_digest = None
        row.refusal = "comment_refused"
        row.refused_at = now
        return 0
    except ForgeError:
        return 0
    if row.comment_id is None:
        row.posted_at = now
    row.comment_id = int(result.comment.id)
    row.rendered_digest = None
    if link and not already:
        assert pr is not None
        try:
            pull_request = await code_host.read_pull_request(
                PullRequestRef(claim.repository, str(pr))
            )
            await tracker.link_pull_request(claim.tracker_issue, pull_request)
        except ForgeError:
            return int(result.written)
    row.rendered_digest = linked if link else digest
    if FINAL_MARKER in body:
        row.finalized_at = now
    return int(result.written)


async def _sync_comment(
    github: GitHubCommentClient,
    claim: _StatusClaim,
) -> int:
    row, target, body, now = claim.row, claim.target, claim.body, claim.now
    if row.subject_title is None:
        row.subject_title = await subject_title(github, int(claim.tracker_issue.issue_id), target)
    terminal = FINAL_MARKER in body
    writes = 0
    if row.comment_id is None:
        outcome = await deliver(github, int(claim.tracker_issue.issue_id), row, target, body)
        if outcome is None:
            return 0
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
        edited = await patch_comment(github, row, body)
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
            row.refused_at = now
            return writes
        else:
            return writes
    if terminal:
        row.finalized_at = now
    return writes


def _lineage_pr_link(
    settings: Settings,
    repo_full_name: str,
    pr_number: int,
    stored_url: str | None,
    code_host_kind: str | None,
) -> str | None:
    """The web link of a lineage's pull request.

    A GitHub lineage (or one recorded before typed code hosts) is linked under
    the install's HTML base. Any other code host's link is the one the lineage
    stored, which its advance verified against that code host's own answer.
    """

    if code_host_kind is None or code_host_kind == GITHUB:
        return f"{settings.github_html_base}/{repo_url_path(repo_full_name)}/pull/{pr_number}"
    return stored_url


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
    retrying = False
    if request.terminal_at is not None and cause:
        cause = cause.strip()
        if cause == "pull_request_not_adopted":
            selected_pr = (
                await session.execute(
                    select(
                        ThreadPublicationLineage.repo_full_name,
                        ThreadPublicationLineage.pr_number,
                        ThreadPublicationLineage.pr_url,
                        ThreadPublicationLineage.code_host_kind,
                    ).where(
                        ThreadPublicationLineage.id
                        == conversation_pr_lineage_id(
                            agent_id=work_item.agent_id,
                            conversation_id=work_item.conversation_id,
                            repo_full_name=work_item.repository_path,
                        ).scalar_subquery()
                    )
                )
            ).one_or_none()
            pr_url = None
            if selected_pr is not None:
                lineage_repo, pr_number, stored_url, code_host_kind = selected_pr
                if pr_number is not None and pr_number > 0:
                    pr_url = _lineage_pr_link(
                        settings, lineage_repo, pr_number, stored_url, code_host_kind
                    )
        # A completed issue run waits for its PR link before it is final.
        if not (
            cause == "completed"
            and target.kind == "issue"
            and (not isinstance(pr_url, str) or not pr_url.strip())
        ):
            streak = 0
            retried = False
            if cause == "owner_lost":
                streak = await owner_lost_streak(
                    session, work_item.id, through_sequence=request.sequence
                )
                retried = await owner_lost_successor_admitted(session, request)
                retrying = (
                    request.status == "failed" and retried and 1 <= streak < OWNER_LOST_RETRY_LIMIT
                )
            unchanged = (
                request.status == "completed"
                and (
                    await session.scalar(
                        select(Publication.id)
                        .where(
                            Publication.execution_request_id == request.id,
                            Publication.status == "succeeded",
                        )
                        .limit(1)
                    )
                )
                is None
            )
            usage = await work_item_usage(session, work_item.id)
            run_cost_usd = next(
                (
                    entry.estimated_cost_usd
                    for entry in usage.requests
                    if entry.request_id == request.id
                ),
                None,
            )
            result = result_section(
                cause,
                pr_url=pr_url,
                feedback_url=request.reply_target_url,
                detail=row.detail,
                unchanged=unchanged,
                superseded=cause == "issue_cancelled"
                and await _superseded(session, work_item, request),
                lost_streak=streak,
                lost_retried=retrying,
                run_cost_usd=run_cost_usd,
            )
            # Tokens and estimated cost over every round of the work item
            # (#3223), after the Cause line so its parse is unchanged.
            line = usage_line(usage)
            if line is not None:
                separator = "\n\n" if result.endswith("\n\n") else "\n"
                result = result.rstrip("\n") + separator + line + "\n"
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
    if row.declaration is not None or (result is not None and cause == "budget_exceeded"):
        reports = list(
            await session.scalars(
                select(ExecutionRequestPhaseReport)
                .where(ExecutionRequestPhaseReport.execution_request_id == request.id)
                .order_by(ExecutionRequestPhaseReport.id)
            )
        )
        if row.declaration is not None:
            view = phase_view(row.declaration, reports, request.status, cause)
        if result is not None and cause == "budget_exceeded":
            latest_note = next(
                (report for report in reversed(reports) if report.note and report.note.strip()),
                None,
            )
            if latest_note is not None:
                labels = (
                    {phase.id: phase.label for phase in view.phases} if view is not None else {}
                )
                label = labels.get(latest_note.phase, latest_note.phase)
                progress = _inert_line(f"Last progress note: {label}: {latest_note.note}")[:300]
                result += progress + "\n"
    pill_label, _color, _live = pill_for(request.status, publishing, retrying=retrying)
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
    base = forges_registry(settings).card_base_url
    body = status_body(
        request_id=row.execution_request_id,
        card_url=f"{base}/v1/factory/cards/{row.card_token}.svg" if base else None,
        pill_label=pill_label,
        phase_view=view,
        result=result,
        paused_for_upgrade=paused_for_upgrade,
        waiting_line=waiting_line,
        base_line=render_base_line(work_item),
    )
    if (
        request.status in {"queued", "waiting", "running", "cancellation_requested"}
        and work_item.publication_lineage_id is not None
        and pr_url
        and await session.scalar(
            select(Publication.id)
            .join(ExecutionRequest, ExecutionRequest.id == Publication.execution_request_id)
            .where(
                Publication.lineage_id == work_item.publication_lineage_id,
                ExecutionRequest.work_item_id == work_item.id,
                ExecutionRequest.sequence < request.sequence,
            )
            .limit(1)
        )
        is not None
    ):
        body = redact_factory_comment(
            f"Continuing on the existing pull request: {pr_url}\n\n{body}"
        )
    return body


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


async def _sync_labels(tracker: Tracker, claim: _StatusClaim) -> int:
    """Add the desired state label and remove the others, on the issue.

    Only the four state labels are ever written. Legacy deletes stay
    unconditional: a new request starts with applied_label NULL, and the issue
    may still carry a legacy name. A refused write is logged and given up; any
    other failure is retried next pass.
    """

    row = claim.row
    if row.applied_label == "":
        return 0
    desired = desired_label(claim.status)
    if row.applied_label == desired:
        return 0
    issue = claim.tracker_issue
    remove = [name for name in (*STATE_LABELS, *LEGACY_STATE_LABELS) if name != desired]
    try:
        await tracker.set_state_label(issue, add=desired or None, remove=remove)
    except ForgeError:
        return 0
    row.applied_label = desired
    return 1


async def _clock(session: AsyncSession) -> Any:
    return await session.scalar(select(func.clock_timestamp()))
