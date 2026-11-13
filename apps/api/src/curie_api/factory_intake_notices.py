"""Durable, App-owned answers for refused trigger-label additions (#5576)."""

from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import timedelta
from typing import Any

import httpx
from curie_telemetry.metrics import record_metric
from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.concurrency import run_in_threadpool

from .code_host_trust import code_host_verify
from .config import Settings
from .factory_comment_text import redact_factory_comment
from .factory_notices import body_digest, code_span, lease_available
from .forges.errors import ForgeError
from .forges.github.comments import static_token
from .forges.github.review_polling import label_actor
from .forges.hosts import factory_credentials, factory_tracker
from .forges.registry import forges_registry
from .forges.types import ReplyTarget, TrackerIssueRef
from .github_app import GitHubAppError, GitHubInstallationRefused
from .github_factory_events import FactoryNotice
from .github_review_events import FeedbackIgnored, FeedbackUnavailable
from .models import FactoryIntakeNotice

METRIC = "curie.factory.intake.notice.oldest_owed_age_seconds"


def observe_label(notice: FactoryNotice, event: dict[str, Any], *, app_id: str) -> FactoryNotice:
    """Preserve refused actors as observations, before deciding their disposition."""
    identity = event.get("id")
    if type(identity) is not int or identity <= 0:
        raise FeedbackUnavailable("label_events_unavailable")
    reason = None
    try:
        sender_id, login = label_actor(event, app_id=app_id)
    except FeedbackIgnored as exc:
        reason = exc.code
        actor = event.get("actor")
        raw_login = actor.get("login") if isinstance(actor, dict) else None
        login = raw_login if isinstance(raw_login, str) else ""
        sender_id = 0
    return replace(
        notice,
        sender_id=sender_id,
        sender_login=login,
        label_event_id=identity,
        actor_refusal=reason,
    )


async def delegated_output_before_label(
    session: AsyncSession, issue: TrackerIssueRef, notice: FactoryNotice
) -> bool:
    """The triage handoff supplies recorded provenance here; no source exists yet."""
    return False


def event_key(issue: TrackerIssueRef, event_id: int) -> tuple[Any, ...]:
    return (
        FactoryIntakeNotice.tracker_host == issue.host,
        FactoryIntakeNotice.tracker_scope_id == issue.scope_id,
        FactoryIntakeNotice.tracker_issue_id == issue.issue_id,
        FactoryIntakeNotice.label_event_id == event_id,
    )


async def answered(session: AsyncSession, issue: TrackerIssueRef, event_id: int) -> bool:
    return (
        await session.scalar(select(FactoryIntakeNotice.id).where(*event_key(issue, event_id)))
        is not None
    )


async def record_notice(
    session: AsyncSession,
    notice: FactoryNotice,
    issue: TrackerIssueRef,
    *,
    settings: Settings,
    reason: str,
    agent_id: uuid.UUID | None,
    publication_url: str | None = None,
) -> None:
    if notice.label_event_id is None:
        raise FeedbackUnavailable("label_events_unavailable")
    tracker = factory_tracker(forges_registry(settings))
    web_base = tracker.html_base.rstrip("/")
    await session.execute(
        insert(FactoryIntakeNotice)
        .values(
            tracker_kind=issue.kind,
            tracker_host=issue.host,
            tracker_scope_id=issue.scope_id,
            tracker_issue_id=issue.issue_id,
            label_event_id=notice.label_event_id,
            repository_path=notice.repo_full_name,
            issue_url=f"{web_base}/{notice.repo_full_name}/issues/{notice.issue_number}",
            label=notice.label or "",
            reason=reason,
            agent_id=agent_id,
            actor_login=notice.sender_login or None,
            publication_url=publication_url,
            label_event_at=notice.label_event_at,
        )
        .on_conflict_do_nothing(constraint="factory_intake_notices_event_key")
    )


def marker_for(row: FactoryIntakeNotice) -> str:
    identity = f"{row.tracker_host}/{row.tracker_scope_id}/{row.tracker_issue_id}"
    digest = body_digest(identity)[:16]
    return f"curie-factory-intake-{row.label_event_id}-{digest} reason={row.reason}"


def notice_body(row: FactoryIntakeNotice) -> str:
    repo = code_span(" ".join(row.repository_path.splitlines()))
    login = code_span(" ".join((row.actor_login or "This person").splitlines()))
    label = code_span(" ".join(row.label.splitlines()))
    sentences = {
        "actor_bot": (
            "This label was applied by a bot account, and Curie only starts work "
            f"for a person with write access to {repo}.",
            "To start this ticket, a person with write access removes the label "
            "and applies it again.",
        ),
        "actor_own_app": (
            "This label was applied through Curie's own GitHub App, "
            "and Curie never starts work from its own activity.",
            "Apply the label directly in GitHub or through another tool.",
        ),
        "actor_no_permission": (
            f"{login} applied the label but does not have write access to {repo}.",
            "Ask a maintainer to apply it.",
        ),
        "actor_unverifiable": (
            "Curie could not verify who applied this label.",
            f"A person with write access to {repo} applies it again.",
        ),
        "personal_owner_required": (
            f"This factory runs on personal model accounts and {login} is not linked to one.",
            f"The operator links {login}, or a linked person applies the label.",
        ),
        "binding_ambiguous": (
            f"More than one factory agent serves the label {label} on {repo}, "
            "and Curie refuses to choose between them.",
            "The operator removes the overlapping binding, then the label is applied again.",
        ),
        "publication_completed": (
            "This issue already has a completed factory pull request, listed below.",
            "To run it again, remove the label and apply it again.",
        ),
        "installation_changed": (
            "Earlier factory work on this issue was recorded under a previous installation "
            "of the GitHub App, and Curie will not move it to the current one.",
            "The factory operator recovers or closes that earlier work; "
            "after that, apply the label again.",
        ),
    }
    body = "Not started.\n\n" + " ".join(sentences[row.reason])
    if row.reason.startswith("actor_"):
        body += "\n\nNothing was queued and nothing was spent."
    if row.publication_url:
        body += f"\n\n1. <{row.publication_url}>"
    return redact_factory_comment(body)


async def oldest_owed_age(session: AsyncSession) -> float:
    age = await session.scalar(
        select(
            func.extract(
                "epoch", func.clock_timestamp() - func.min(FactoryIntakeNotice.observed_at)
            )
        ).where(FactoryIntakeNotice.posted_at.is_(None))
    )
    return max(0.0, float(age or 0))


async def sync_intake_notices(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    *,
    owner: str,
    client: httpx.AsyncClient | None = None,
    limit: int = 20,
) -> int:
    if client is None:
        async with httpx.AsyncClient(
            timeout=settings.github_app_timeout_seconds, verify=code_host_verify(settings)
        ) as owned_client:
            return await sync_intake_notices(
                sessionmaker, settings, owner=owner, client=owned_client, limit=limit
            )
    writes = 0
    for _ in range(limit):
        async with sessionmaker() as session:
            row = await session.scalar(
                select(FactoryIntakeNotice)
                .where(
                    FactoryIntakeNotice.posted_at.is_(None),
                    or_(
                        FactoryIntakeNotice.not_before.is_(None),
                        FactoryIntakeNotice.not_before <= func.clock_timestamp(),
                    ),
                    lease_available(
                        FactoryIntakeNotice.lease_owner, FactoryIntakeNotice.lease_expires_at
                    ),
                )
                .order_by(FactoryIntakeNotice.observed_at, FactoryIntakeNotice.id)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if row is None:
                break
            lease = f"{owner}:{uuid.uuid4().hex}"
            row.lease_owner = lease
            row.lease_expires_at = func.clock_timestamp() + timedelta(seconds=300)
            row.attempts += 1
            await session.commit()
        comment_id = None
        error = None
        try:
            registry = forges_registry(settings)
            _, token = await run_in_threadpool(
                factory_credentials(registry).fresh_installation_token, row.repository_path, None
            )
            connection = factory_tracker(registry)
            if (
                connection.config.host != row.tracker_host
                or connection.config.kind != row.tracker_kind
            ):
                raise ValueError("tracker_identity_changed")
            tracker = connection.for_repository(
                client,
                repo_full_name=row.repository_path,
                repository_id=int(row.tracker_scope_id),
                token=static_token(token),
            )
            written = await tracker.marked_comments.upsert_marked(
                ReplyTarget.on_issue(tracker.issue(int(row.tracker_issue_id))),
                marker_for(row),
                notice_body(row),
            )
            comment_id = int(written.comment.id)
            writes += int(written.written)
        except (ForgeError, GitHubAppError, GitHubInstallationRefused, httpx.HTTPError, ValueError):
            error = "delivery_unavailable"
        async with sessionmaker() as session:
            await session.execute(
                update(FactoryIntakeNotice)
                .where(
                    FactoryIntakeNotice.id == row.id,
                    FactoryIntakeNotice.lease_owner == lease,
                    FactoryIntakeNotice.lease_expires_at > func.clock_timestamp(),
                )
                .values(
                    comment_id=comment_id,
                    posted_at=func.clock_timestamp() if comment_id else None,
                    last_error=error,
                    lease_owner=None,
                    lease_expires_at=None,
                    not_before=(
                        func.clock_timestamp()
                        + timedelta(seconds=min(3600, 10 * 2 ** min(row.attempts - 1, 9)))
                    )
                    if error
                    else None,
                )
            )
            await session.commit()
    async with sessionmaker() as session:
        record_metric(METRIC, await oldest_owed_age(session))
    return writes
