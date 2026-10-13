"""Review feedback on a non-GitHub code host asks for one more revision (#4399).

GitHub review feedback arrives by its webhook or its own poll
(`github_factory_review`). Every other code host has neither, so this pass reads
each open factory pull request on it through its `CodeHost` port: the feedback
since the lineage's stored cursor (`CodeHost.list_review_feedback`), kept only
when it mentions the tracker's account and is still current from an author who
may have it acted on (`forges.authority.feedback_actionable`). Each kept piece
admits one revision on the owning WorkItem (`workitem_dispatch.admit_revision`)
under a request id derived from the feedback itself, so a replayed listing
admits nothing new. The revision replies in the feedback's own discussion,
through the same status note sync every port-driven reply uses.

The cursor lives in Valkey per lineage. Losing it only re-lists feedback whose
revisions already exist. It advances only after every listed piece was handled.
Logs carry ids and codes only, never a feedback body or a provider answer.
"""

from __future__ import annotations

import json
import logging
import uuid

import httpx
from curie_internal.keyspace import WORK_ITEM_REVIEW_FEEDBACK_PREFIX
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from . import workitem_dispatch
from .config import Settings
from .factory_tracker_intake import take_due
from .forges.authority import feedback_actionable, feedback_allowlist
from .forges.config import BindingConfig, TrackerConfig
from .forges.errors import ForgeError
from .forges.hosts import forge_code_host
from .forges.identity import revision_request_id
from .forges.ports import CodeHost
from .forges.registry import ForgeRegistry, forges_registry
from .forges.trackers import binding_for_scope, reply_route
from .forges.types import GITHUB, FeedbackKind, PullRequestRef, ReplyTarget, ReviewFeedback
from .github_factory import lock_issue
from .github_factory_events import mentions_login
from .models import ThreadPublicationLineage, WorkItem
from .workitem_dispatch import Admission

logger = logging.getLogger(__name__)

_MAX_OBJECTIVE = 65536
_CURSOR_TTL_SECONDS = 30 * 24 * 3600


def cursor_key(lineage_id: uuid.UUID) -> str:
    """The Valkey key of one lineage's review feedback cursor."""

    return f"{WORK_ITEM_REVIEW_FEEDBACK_PREFIX}:{lineage_id}"


async def poll_review_feedback(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    valkey: Redis,
    *,
    due: dict[str, float],
    clock: float,
) -> None:
    """One pass over every open factory pull request on a non-GitHub code host.

    Each lineage is read at most once per its binding tracker's poll interval
    (``due`` on the caller's monotonic ``clock``). A code host that cannot
    answer skips that lineage until its next due time; the others still run.
    """

    registry = forges_registry(settings)
    async with sessionmaker() as session:
        rows = (
            await session.execute(
                select(WorkItem, ThreadPublicationLineage)
                .join(
                    ThreadPublicationLineage,
                    ThreadPublicationLineage.id == WorkItem.publication_lineage_id,
                )
                .where(
                    ThreadPublicationLineage.status == "open",
                    ThreadPublicationLineage.code_host_kind.is_not(None),
                    ThreadPublicationLineage.code_host_kind != GITHUB,
                    ThreadPublicationLineage.pr_number.is_not(None),
                    WorkItem.cancelled_at.is_(None),
                )
                .order_by(ThreadPublicationLineage.created_at)
            )
        ).all()
    for work_item, lineage in rows:
        binding = binding_for_scope(registry, work_item.tracker_issue)
        if binding is None:
            continue
        tracker = registry.config.tracker(binding.tracker)
        if not take_due(due, f"review:{lineage.id}", clock, tracker.poll_interval_s):
            continue
        try:
            await _poll_lineage(
                sessionmaker, settings, registry, client, valkey, binding, tracker, lineage
            )
        except ForgeError as exc:
            logger.warning(
                "review feedback on lineage %s could not be read: %s",
                lineage.id,
                type(exc).__name__,
            )


async def _poll_lineage(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    registry: ForgeRegistry,
    client: httpx.AsyncClient,
    valkey: Redis,
    binding: BindingConfig,
    tracker: TrackerConfig,
    lineage: ThreadPublicationLineage,
) -> None:
    repository = lineage.repository
    assert repository is not None and lineage.pr_number is not None
    repo = next(
        (
            entry
            for entry in binding.repos
            if entry.project_id == repository.project_id
            and registry.config.code_host(entry.code_host).kind == repository.kind
            and registry.config.code_host(entry.code_host).host == repository.host
        ),
        None,
    )
    if repo is None:
        return
    allowlist = feedback_allowlist(binding, repo)
    code_host = forge_code_host(settings, client, repository)
    key = cursor_key(lineage.id)
    stored = await valkey.get(key)
    cursor = stored.decode() if isinstance(stored, bytes) else stored
    page = await code_host.list_review_feedback(
        PullRequestRef(repository, str(lineage.pr_number)), cursor
    )
    for feedback in page.items:
        if not mentions_login(feedback.body, tracker.mention):
            continue
        if not await feedback_actionable(code_host, feedback, allowlist):
            continue
        async with sessionmaker() as session:
            await _admit(session, registry, code_host, lineage.id, feedback)
    await valkey.set(key, page.cursor, ex=_CURSOR_TTL_SECONDS)


def _objective(feedback: ReviewFeedback, url: str, code_host: CodeHost) -> str:
    # The pull request URL opens the objective for the agent's benefit only;
    # the reply target is stored typed on the request.
    provenance = {
        "kind": feedback.kind.value,
        "url": url,
        "sender": feedback.author.login,
        "body": feedback.body,
        "head_sha": feedback.head_sha,
    }
    return (
        f"{url}\n\n"
        f"Human {code_host.kind} review feedback on this work item's pull request follows as "
        "JSON. Use its body as the reviewer's requested changes and revise the same pull "
        "request.\n" + json.dumps(provenance, ensure_ascii=False)
    )


def _reply_target(feedback: ReviewFeedback) -> ReplyTarget:
    """A threaded note is answered in its discussion; any other feedback in the
    pull request conversation."""

    if feedback.kind is FeedbackKind.REVIEW_COMMENT and feedback.thread_id:
        return ReplyTarget.on_thread(feedback.pull_request, feedback.thread_id)
    return ReplyTarget.on_pull_request(feedback.pull_request)


async def _admit(
    session: AsyncSession,
    registry: ForgeRegistry,
    code_host: CodeHost,
    lineage_id: uuid.UUID,
    feedback: ReviewFeedback,
) -> None:
    """Admit one actionable piece of feedback on the WorkItem owning the lineage.

    The WorkItem and lineage are re-read under the issue lock: a cancelled
    WorkItem, a closed lineage, or one the WorkItem no longer owns, or whose
    pull request is no longer the feedback's, admits nothing.
    """

    work_item = await session.scalar(
        select(WorkItem).where(WorkItem.publication_lineage_id == lineage_id)
    )
    if work_item is None:
        return
    await lock_issue(session, work_item.tracker_issue)
    work_item = await session.scalar(
        select(WorkItem)
        .where(WorkItem.id == work_item.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    lineage = await session.scalar(
        select(ThreadPublicationLineage)
        .where(ThreadPublicationLineage.id == lineage_id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    if (
        work_item is None
        or lineage is None
        or work_item.publication_lineage_id != lineage.id
        or work_item.cancelled_at is not None
        or lineage.status != "open"
        or lineage.repository != feedback.pull_request.repository
        or str(lineage.pr_number) != feedback.pull_request.number
        or lineage.pr_url is None
    ):
        await session.rollback()
        return
    objective = _objective(feedback, lineage.pr_url, code_host)
    if len(objective) > _MAX_OBJECTIVE:
        await session.rollback()
        logger.info("review feedback %s on lineage %s is too large", feedback.id, lineage.id)
        return
    kind, address, conversation = reply_route(
        registry, work_item.tracker_issue, work_item.repository_path
    )
    facts = Admission(
        agent_id=work_item.agent_id,
        kind=kind,
        address=address,
        reply_conversation_id=conversation,
        issue=work_item.tracker_issue,
        repository=work_item.repository,
        code_host_installation_id=work_item.code_host_installation_id,
        objective=objective,
        requester=f"{code_host.kind}:{feedback.author.id}:{feedback.author.login}",
        request_id=revision_request_id(
            feedback.pull_request.repository, feedback.kind, feedback.id
        ),
        reply_target=_reply_target(feedback),
        reply_url=lineage.pr_url,
    )
    result = await workitem_dispatch.admit_revision(session, facts)
    await session.commit()
    logger.info(
        "review feedback %s on lineage %s: %s",
        feedback.id,
        lineage.id,
        type(result).__name__,
    )
