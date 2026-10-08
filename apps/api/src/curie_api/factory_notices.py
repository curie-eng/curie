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
https://docs.github.com/en/rest/issues/labels#list-labels-for-an-issue
https://docs.github.com/en/rest/issues/labels#remove-a-label-from-an-issue
"""

from __future__ import annotations

import hashlib
import logging
import math
import re
import time
import uuid
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal
from urllib.parse import quote, urlsplit

import httpx
from curie_telemetry.redact import redact_text
from sqlalchemy import and_, case, exists, func, literal, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased
from starlette.concurrency import run_in_threadpool

from .config import Settings
from .factory_progress import PhaseView, phase_view, pill_for
from .factory_reply_target import ReplyTarget, parse_reply_target
from .factory_usage import usage_line, work_item_usage
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
from .workitems import OWNER_LOST_RETRY_LIMIT, owner_lost_streak, owner_lost_successor_admitted

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
    body: str | None = None
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
    "start_failed": "the sandbox did not start.",
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
    }
)
# A run that ended without publishing carries the agent's own last message
# (#3128). That text is model-authored, so it renders inert inside a code fence.
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
_FACTORY_URL = re.compile(r"https?://[^\s<>\"'`]+", re.IGNORECASE)
_KEY_MANAGEMENT_PATH = re.compile(
    r"/(?:api[-_]?keys|keys|key[-_]management)(?:[/?#]|$)", re.IGNORECASE
)
_PULL_REQUEST_PATH = re.compile(r"/pull/[1-9][0-9]*(?:[/?#]|$)")
# Horizontal space only: a label must not cross into the next publisher line
# and swallow ``Cause:``. Quotes accept JSON escapes so an inner \" does not
# end the value early. Emphasis or backticks may wrap the label, as in
# ``**key_id**:`` or `` `workspace`: ``.
_EMPHASIS = r"(?:[*_`]{1,3})?"
_QUOTED_PROVIDER_VALUE = r"(?:\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|`[^`\n]*`)"
_PROVIDER_TOKEN = r"[^\s,;)}>" + r"\"'`]+"
_PROVIDER_VALUE = r"(?P<value>" + _QUOTED_PROVIDER_VALUE + r"|" + _PROVIDER_TOKEN + r")"
_PROVIDER_LABEL_TAIL = r"[\"']?[ \t]*(?=[:=\"'`])(?:[:=][ \t]*)?"
# ``_`` is a word character, so ``\b`` misses ``_key_id_``. Bound the label
# on letters and digits instead, then allow emphasis on either side.
_LABEL_BEFORE = r"(?<![A-Za-z0-9])"
_LABEL_AFTER = r"(?![A-Za-z0-9])"
_PROVIDER_KEY_ID = re.compile(
    r"(?P<label>"
    + _EMPHASIS
    + _LABEL_BEFORE
    + r"(?:api[ _-]?)?key[ _-]?(?:id|identifier|hash)"
    + _LABEL_AFTER
    + _EMPHASIS
    + _PROVIDER_LABEL_TAIL
    + r")"
    + _PROVIDER_VALUE,
    re.IGNORECASE,
)
# The name phrase stays case sensitive so lowercase diagnostic prose
# (``failed to prepare the repository``) is not a workspace name. A capitalized
# phrase (``Acme Research Team``) is. A slug still matches in any case.
_SLUG_TOKEN = r"(?![a-z]+[ \t]+[a-z])(?=[^\s,;)}>" + r"\"'`]*[0-9_-])" + _PROVIDER_TOKEN
_WORKSPACE_NAME = r"[A-Z][A-Za-z0-9._-]*(?:[ \t]+[A-Z][A-Za-z0-9._-]*)*" r"|" + _SLUG_TOKEN
_PROVIDER_WORKSPACE = re.compile(
    r"(?P<label>"
    + _EMPHASIS
    + _LABEL_BEFORE
    + r"(?i:workspace(?:[ _-]?name)?)"
    + _LABEL_AFTER
    + _EMPHASIS
    + _PROVIDER_LABEL_TAIL
    + r")(?P<value>"
    + _QUOTED_PROVIDER_VALUE
    + r"|"
    + _WORKSPACE_NAME
    + r")"
)
# ``workspace=acme`` is an assignment, not diagnostic prose, so the value may
# be a plain word. The colon form stays on ``_PROVIDER_WORKSPACE``.
_PROVIDER_WORKSPACE_ASSIGN = re.compile(
    r"(?P<label>"
    + _EMPHASIS
    + _LABEL_BEFORE
    + r"(?i:workspace(?:[ _-]?name)?)"
    + _LABEL_AFTER
    + _EMPHASIS
    + r"[ \t]*=[ \t]*)(?P<value>"
    + _QUOTED_PROVIDER_VALUE
    + r"|"
    + _PROVIDER_TOKEN
    + r")"
)


def _redact_factory_comment(body: str) -> str:
    """Redact secrets and contextual provider identifiers before publication."""

    def redact_url(match: re.Match[str]) -> str:
        url = match[0]
        trimmed = url.rstrip(".,;:!)")
        try:
            parsed = urlsplit(trimmed)
            path = parsed.path
            github_pr = parsed.hostname in {"github.com", "www.github.com"} and bool(
                _PULL_REQUEST_PATH.search(path)
            )
        except ValueError:
            path = trimmed
            github_pr = False
        if _KEY_MANAGEMENT_PATH.search(path) and not github_pr:
            return "[REDACTED:provider_key_url]" + url[len(trimmed) :]
        return url

    body = _FACTORY_URL.sub(redact_url, body)
    body = redact_text(body)
    for pattern, label in (
        (_PROVIDER_KEY_ID, "provider_key_id"),
        (_PROVIDER_WORKSPACE_ASSIGN, "provider_workspace"),
        (_PROVIDER_WORKSPACE, "provider_workspace"),
    ):

        def redact_identifier(match: re.Match[str], label: str = label) -> str:
            placeholder = f"[REDACTED:{label}]"
            value = match["value"]
            if value == placeholder or value in {
                f'"{placeholder}"',
                f"'{placeholder}'",
                f"`{placeholder}`",
            }:
                return match[0]
            if re.search(r"[A-Za-z0-9]", value) is None:
                return match[0]
            return f"{match['label']}{placeholder}"

        body = pattern.sub(redact_identifier, body)
    return body


def cause_text(cause: str) -> str:
    """A plain sentence for a terminus cause; unknown codes get a generic one."""

    return _CAUSE_TEXT.get(cause, "the run stopped for a reason Curie did not recognize.")


def start_failed_sentence(attempts: int, reason: str) -> str:
    """The ``start_failed`` sentence naming the attempt count and last reason (#4170)."""

    return f"the sandbox did not start after {attempts} attempts. Last reason: {reason}."


def marker_for(request_id: uuid.UUID) -> str:
    return f"<!-- curie-execution-request:{request_id} -->"


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
) -> str:
    """The terminal result lines of a status comment, without any marker.

    For ``owner_lost``, ``lost_streak`` is the consecutive losses through
    this request and ``lost_retried`` whether its settlement admitted a
    successor (ADR 0206); together they choose the retry or exhausted sentence.
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
            cause not in {"history_capacity", "start_failed"}
            and detail is not None
            and detail.strip()
        ):
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


def _inert_line(text: str) -> str:
    return _break_html_comments(" ".join(text.split()))


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
    async with httpx.AsyncClient(timeout=settings.github_app_timeout_seconds) as client:
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
            calls = _GitHubCalls()
            token = _github_calls.set(calls)
            started = time.monotonic()
            try:
                writes += await _sync_one(client, settings, claim)
                claim.row.attempts += 1
                async with sessionmaker() as session:
                    await _write_back(session, claim, owner=owner)
                    await session.commit()
            finally:
                _github_calls.reset(token)
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
    repo_full_name: str
    github_installation_id: int
    github_issue_number: int
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
    row, work_item, request, pr_url, latest = selected
    assert request.objective is not None
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
    target = parse_reply_target(
        request.objective,
        repo_full_name=work_item.repo_full_name,
        clone_base=settings.github_clone_base,
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
        repo_full_name=work_item.repo_full_name,
        github_installation_id=work_item.github_installation_id,
        github_issue_number=work_item.github_issue_number,
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


@dataclass
class _GitHubCalls:
    count: int = 0


_github_calls: ContextVar[_GitHubCalls | None] = ContextVar("factory_github_calls", default=None)


async def _github_request(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    path_template: str,
    **kwargs: Any,
) -> httpx.Response:
    calls = _github_calls.get()
    if calls is not None:
        calls.count += 1
    started = time.monotonic()
    status: int | str = "http_error"
    try:
        response = await client.request(method, url, **kwargs)
        status = response.status_code
        return response
    finally:
        elapsed_ms = (time.monotonic() - started) * 1000
        failed = not isinstance(status, int) or (status != 404 and not 200 <= status < 300)
        if elapsed_ms > 2000 or failed:
            logger.warning(
                "factory GitHub call method=%s path=%s status=%s elapsed_ms=%.1f",
                method,
                path_template,
                status,
                elapsed_ms,
                extra={
                    "method": method,
                    "path_template": path_template,
                    "status": status,
                    "elapsed_ms": elapsed_ms,
                },
            )


@dataclass(frozen=True)
class _GitHub:
    client: httpx.AsyncClient
    api: str
    repo_path: str
    headers: dict[str, str]


async def _sync_one(
    client: httpx.AsyncClient,
    settings: Settings,
    claim: _StatusClaim,
) -> int:
    row = claim.row
    try:
        token = await run_in_threadpool(
            credentials_for(settings).token_for_verified_installation,
            claim.repo_full_name,
            claim.github_installation_id,
        )
    except (GitHubInstallationRefused, GitHubAppError, ValueError):
        return 0
    github = _GitHub(
        client=client,
        api=settings.github_api_url.rstrip("/"),
        repo_path=f"/repos/{repo_url_path(claim.repo_full_name)}",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    writes = 0
    if row.finalized_at is None:
        writes += await _sync_comment(github, claim)
    if claim.latest and row.refused_at is None:
        writes += await _sync_labels(github, claim)
    return writes


async def _sync_comment(
    github: _GitHub,
    claim: _StatusClaim,
) -> int:
    row, target, body, now = claim.row, claim.target, claim.body, claim.now
    if row.subject_title is None:
        row.subject_title = await _subject_title(github, claim.github_issue_number, target)
    terminal = FINAL_MARKER in body
    writes = 0
    if row.comment_id is None:
        outcome = await _deliver(github, claim.github_issue_number, row, target, body)
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
            row.refused_at = now
            return writes
        else:
            return writes
    if terminal:
        row.finalized_at = now
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
    retrying = False
    if request.terminal_at is not None and cause:
        cause = cause.strip()
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
            result = result_section(
                cause,
                pr_url=pr_url,
                feedback_url=target.url,
                detail=row.detail,
                unchanged=unchanged,
                superseded=cause == "issue_cancelled"
                and await _superseded(session, work_item, request),
                lost_streak=streak,
                lost_retried=retrying,
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


async def _subject_title(github: _GitHub, issue_number: int, target: ReplyTarget) -> str | None:
    """The issue or PR title for the card, read once. A failed read stays NULL."""

    if target.pr_number is None:
        path = f"{github.repo_path}/issues/{issue_number}"
        template = "/repos/{owner}/{repo}/issues/{issue_number}"
    else:
        path = f"{github.repo_path}/pulls/{target.pr_number}"
        template = "/repos/{owner}/{repo}/pulls/{pull_number}"
    try:
        found = await _github_request(
            github.client,
            "GET",
            f"{github.api}{path}",
            path_template=template,
            headers=github.headers,
            follow_redirects=False,
        )
        payload = found.json() if found.status_code == 200 else None
    except (httpx.HTTPError, ValueError):
        return None
    if isinstance(payload, dict) and isinstance(payload.get("title"), str):
        return str(payload["title"])[:256]
    return None


async def _sync_labels(github: _GitHub, claim: _StatusClaim) -> int:
    """Add the desired state label and remove the others, on the issue.

    Only the four state labels are ever written. A refused write is logged and
    given up; any other failure is retried next pass.
    """

    row = claim.row
    if row.applied_label == "":
        return 0
    desired = desired_label(claim.status)
    if row.applied_label == desired:
        return 0
    labels_path = f"{github.api}{github.repo_path}/issues/{claim.github_issue_number}/labels"
    template = "/repos/{owner}/{repo}/issues/{issue_number}/labels"
    resource = httpx.URL(labels_path)
    url = labels_path
    params: dict[str, int] | None = {"per_page": 100}
    present: set[str] = set()
    seen: set[str] = set()
    # GitHub labels are paginated. Read every page before changing anything:
    # https://docs.github.com/en/rest/issues/labels#list-labels-for-an-issue
    while True:
        try:
            listed = await _github_request(
                github.client,
                "GET",
                url,
                path_template=template,
                headers=github.headers,
                params=params,
                follow_redirects=False,
            )
            if listed.status_code != 200:
                return 0
            payload = listed.json()
        except (httpx.HTTPError, ValueError):
            return 0
        if not isinstance(payload, list) or any(
            not isinstance(label, dict) or not isinstance(label.get("name"), str)
            for label in payload
        ):
            return 0
        present.update(label["name"] for label in payload)
        seen.add(str(listed.request.url))
        next_link = listed.links.get("next")
        if next_link is None:
            break
        next_url = next_link.get("url")
        if not isinstance(next_url, str):
            return 0
        try:
            next_resource = httpx.URL(next_url)
        except httpx.InvalidURL:
            return 0
        if (next_resource.scheme, next_resource.host, next_resource.port, next_resource.path) != (
            resource.scheme,
            resource.host,
            resource.port,
            resource.path,
        ) or str(next_resource) in seen:
            return 0
        url = str(next_resource)
        params = None
    writes = 0
    complete = True
    if desired and desired not in present:
        try:
            added = await _github_request(
                github.client,
                "POST",
                labels_path,
                path_template=template,
                headers=github.headers,
                json={"labels": [desired]},
                follow_redirects=False,
            )
        except httpx.HTTPError:
            return writes
        writes += 1
        if added.status_code in _REFUSED_STATUSES:
            logger.info(
                "factory state label refused",
                extra={"work_item_id": str(claim.work_item_id), "status": added.status_code},
            )
        elif added.status_code not in {200, 201}:
            return writes
    for name in (*STATE_LABELS, *LEGACY_STATE_LABELS):
        if name == desired or name not in present:
            continue
        try:
            removed = await _github_request(
                github.client,
                "DELETE",
                f"{labels_path}/{quote(name, safe=':')}",
                path_template=template + "/{label}",
                headers=github.headers,
                follow_redirects=False,
            )
        except httpx.HTTPError:
            complete = False
            continue
        writes += 1
        # 404: the label was not on the issue, which is the goal.
        if removed.status_code in {401, 403}:
            logger.info(
                "factory state label removal refused",
                extra={"work_item_id": str(claim.work_item_id), "status": removed.status_code},
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
    issue_number: int,
    row: FactoryStatusComment,
    target: ReplyTarget,
    body: str,
) -> tuple[Literal["refused"], str] | tuple[Literal["posted"], int, CommentList, bool] | None:
    """Create the comment, or find one a lost response already created.

    ``posted`` carries the comment id, the list it lives in, and whether this
    call created it with ``body``.
    """

    api, repo_path, headers, client = github.api, github.repo_path, github.headers, github.client
    number = issue_number if target.pr_number is None else target.pr_number
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
        existing = await _find_marker(
            client,
            api,
            path,
            headers,
            marker,
            start_page=start,
        )
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
            path_template="/repos/{owner}/{repo}/pulls/{pull_number}/comments/{comment_id}/replies",
        )
        if replied != _UNPROCESSABLE:
            return _created(row, replied, "review")
    posted = await _post(
        client,
        f"{api}{comments_path}",
        headers,
        body,
        path_template="/repos/{owner}/{repo}/issues/{issue_number}/comments",
    )
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
        edited = await _github_request(
            github.client,
            "PATCH",
            url,
            path_template=f"/repos/{{owner}}/{{repo}}/{kind}/comments/{{comment_id}}",
            headers=github.headers,
            json={"body": _redact_factory_comment(body)},
            follow_redirects=False,
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
        found = await _github_request(
            client,
            "GET",
            f"{api}{repo_path}/pulls/comments/{comment_id}",
            path_template="/repos/{owner}/{repo}/pulls/comments/{comment_id}",
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
    client: httpx.AsyncClient,
    url: str,
    headers: dict[str, str],
    body: str,
    *,
    path_template: str,
) -> tuple[str, str] | None:
    try:
        created = await _github_request(
            client,
            "POST",
            url,
            path_template=path_template,
            headers=headers,
            json={"body": _redact_factory_comment(body)},
            follow_redirects=False,
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
    app_id: str | None = None,
) -> _MarkerScan:
    """Find a marker, or remember the next page so a later pass can continue.

    A full page does not mean the marker is absent. Stopping there and posting
    would duplicate a comment that sits further down the list. A short page is
    the end, so a missing marker is safe to post.
    """

    page = start_page
    for _ in range(_PAGES_PER_PASS):
        try:
            listed = await _github_request(
                client,
                "GET",
                f"{api}{path}",
                path_template="/repos/{owner}/{repo}/{comment_resource}/{subject_number}/comments",
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
        found = _marked_comment(payload, marker, app_id=app_id)
        if found is not None:
            return _MarkerScan(comment_id=found[0], body=found[1])
        if not isinstance(payload, list) or len(payload) < 100:
            return _MarkerScan()
        page += 1
    return _MarkerScan(next_page=page)


def _marked_comment(
    payload: Any, marker: str, *, app_id: str | None = None
) -> tuple[int, str] | None:
    """The first comment carrying ``marker``.

    ``app_id`` None reads every author. Otherwise only comments the app with
    that id posted count, and an empty id matches none.
    """

    if not isinstance(payload, list):
        return None
    for item in payload:
        if not isinstance(item, dict):
            continue
        if app_id is not None:
            via = item.get("performed_via_github_app")
            if not app_id or not isinstance(via, dict) or str(via.get("id")) != app_id:
                continue
        body = item.get("body")
        comment_id = item.get("id")
        if isinstance(body, str) and marker in body and type(comment_id) is int:
            return comment_id, body
    return None


async def upsert_issue_notice(
    client: httpx.AsyncClient,
    *,
    api: str,
    repo_path: str,
    headers: dict[str, str],
    issue_number: int,
    marker: str,
    body: str,
    app_id: str = "",
) -> Literal["written", "unchanged", "unavailable"]:
    """Keep one marked issue comment carrying ``body``: post it, edit it, or leave it.

    The marker scan is the same one the status comment uses. A scan that could
    not reach the end of the list is ``unavailable``, never a second post.
    """

    body = _redact_factory_comment(body)
    comments_path = f"{repo_path}/issues/{issue_number}/comments"
    found = await _find_marker(
        client,
        api,
        comments_path,
        headers,
        marker,
        start_page=1,
        app_id=app_id.strip(),
    )
    if found.unavailable or found.refusal is not None or found.next_page is not None:
        return "unavailable"
    if found.comment_id is not None:
        if found.body == body:
            return "unchanged"
        try:
            edited = await _github_request(
                client,
                "PATCH",
                f"{api}{repo_path}/issues/comments/{found.comment_id}",
                path_template="/repos/{owner}/{repo}/issues/comments/{comment_id}",
                headers=headers,
                json={"body": body},
                follow_redirects=False,
            )
        except httpx.HTTPError:
            return "unavailable"
        return "written" if edited.status_code == 200 else "unavailable"
    posted = await _post(
        client,
        f"{api}{comments_path}",
        headers,
        body,
        path_template="/repos/{owner}/{repo}/issues/{issue_number}/comments",
    )
    if posted is None or posted[0] != "posted":
        return "unavailable"
    return "written"
