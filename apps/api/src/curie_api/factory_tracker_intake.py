"""The port-driven intake pass for every non-GitHub tracker (#4358 plan D1, D2).

GitHub keeps its webhook, ETag poll and missed-label reconciler. Every other
configured tracker binding is polled here through `Tracker.poll_marked`, and
each notice goes through the same admission the GitHub path applies:

1. ``verify_current``, then the binding's start authority for a label add or
   a mention (an actor without it is ignored, as GitHub ignores a non-writer);
2. the surface the label serves on the binding (none or two refuse
   ``binding_missing``), and the label refusal rules of #4361 decision 9;
3. the repository (`forges.resolution`) and base (`factory_base`, with the
   chosen binding's policy) choice, each refused with one marked comment;
4. the shared WorkItem dispatch: ``readmit``, ``admit_revision`` or a cancel.

A running WorkItem of a configured tracker whose scope no binding serves any
more (the binding was removed or rescoped) can no longer be polled, admitted or
cancelled through a tracker, so each poll of that tracker cancels it.

The opaque ``poll_marked`` cursor is kept per label in the tracker scope's
``factory_poll_cursors`` row, under ``etags["marked:<label>"]``, and saved only
after every notice of a page was applied or deliberately refused. A notice
whose tracker or code host could not answer holds only its own issue (#4621):
the issue's later notices in the page wait behind it, it gets one marked hold
comment, the cursor stays, and every other issue in the page is still applied.
The next poll replays the page, and an already applied notice replays under
its request id. A refused admission is not held: every
``factory_reconcile_interval_s`` a sweep re-reads the latest label add of every
labelled issue and applies it again under the original event's request id, so
a repaired ticket admits and an admitted one replays.

The pass holds ``pg_try_advisory_lock(3745, 197)`` on one connection, so only
one replica polls. A tracker's credential and responses stay in the API: logs
name the tracker and binding, never a token.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from . import factory_base, factory_rediscovery, workitem_dispatch
from .config import Settings
from .factory_admission import LABEL_REFUSAL_MARKER, cancel_work_item, label_refusal
from .forges.authority import may_start_with
from .forges.config import BindingConfig, RepositoryBindingConfig, TrackerConfig
from .forges.connections import CodeHostConnection
from .forges.errors import CredentialUnreadable, ForgeError, NotFound, Unauthorized, Unavailable
from .forges.hosts import resolve_stored
from .forges.identity import notice_request_id
from .forges.ports import CodeHost, Tracker
from .forges.registry import ForgeRegistry, forges_registry
from .forges.resolution import REFUSAL_MARKER as REPO_REFUSAL_MARKER
from .forges.resolution import Refusal, refusal_body, resolve_repository
from .forges.surfaces import agent_binding, binding_location, bound_labels
from .forges.trackers import (
    binding_for_scope,
    issue_link,
    mention_comment_id,
    mention_objective,
    reply_route,
    tracker_for_binding,
)
from .forges.types import (
    GITHUB,
    Disposition,
    MarkedNotice,
    ReplyTarget,
    RepositoryRef,
    TicketLabels,
    TrackerIssueRef,
)
from .github_factory import lock_issue, work_item_for
from .github_review_events import FeedbackUnavailable
from .models import FactoryPollCursor, ThreadPublicationLineage, WorkItem
from .workitem_dispatch import Admission

logger = logging.getLogger(__name__)

_LOCK = text("SELECT pg_try_advisory_lock(CAST(:classid AS integer), CAST(:objid AS integer))")
_UNLOCK = text("SELECT pg_advisory_unlock(CAST(:classid AS integer), CAST(:objid AS integer))")
_LOCK_ARGS = {"classid": 3745, "objid": 197}

# The tracker or a code host could not answer now, or refused the credential.
# On a page read the label's pass stops; on one notice only its issue is held.
# Either way the label's cursor stays.
_DEFERRED = (Unavailable, Unauthorized, FeedbackUnavailable)

HOLD_MARKER = "curie-factory-intake-hold"
_HOLD_BODY = (
    "Curie could not start this issue yet: a tracker or code host it needs did not "
    "answer. Curie retries on its next poll; nothing is needed unless this persists."
)


@dataclass(frozen=True)
class _Scope:
    """One binding of one tracker, as the pass walks it."""

    registry: ForgeRegistry
    settings: Settings
    client: httpx.AsyncClient
    tracker: TrackerConfig
    binding: BindingConfig
    surface_kind: str
    location: str

    @property
    def cursor_key(self) -> tuple[str, str, str]:
        return (self.tracker.kind, self.tracker.host, self.binding.tracker_scope_id)


def _engine(sessionmaker: async_sessionmaker[AsyncSession]) -> Any:
    bind = sessionmaker.kw.get("bind")
    if bind is None:
        raise RuntimeError("tracker intake sessionmaker has no bind")
    return bind


async def poll_trackers(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    *,
    due: dict[str, float],
    clock: float,
) -> None:
    """One locked pass over every non-GitHub tracker binding.

    ``due`` holds the caller's next due time per poll and sweep key, on the
    caller's monotonic ``clock``; a key not yet in it is due now. Returns
    without a tracker read when another replica holds the lock.
    """

    async with _engine(sessionmaker).connect() as connection:
        locked = (await connection.execute(_LOCK, _LOCK_ARGS)).scalar()
        await connection.commit()
        if not locked:
            return
        try:
            await _poll_locked(sessionmaker, settings, client, due=due, clock=clock)
        finally:
            await connection.execute(_UNLOCK, _LOCK_ARGS)
            await connection.commit()


def take_due(due: dict[str, float], key: str, clock: float, interval: float) -> bool:
    """Whether ``key`` is due at ``clock``; if so, schedule it ``interval`` later."""

    if interval <= 0 or clock < due.get(key, clock):
        return False
    due[key] = clock + interval
    return True


async def _poll_locked(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    *,
    due: dict[str, float],
    clock: float,
) -> None:
    registry = forges_registry(settings)
    config = registry.config
    for tracker in config.trackers:
        if tracker.kind == GITHUB:
            continue
        polling = take_due(due, f"poll:{tracker.name}", clock, tracker.poll_interval_s)
        if polling:
            await _cancel_orphans(sessionmaker, registry, tracker)
        for binding in config.bindings:
            if binding.tracker != tracker.name:
                continue
            surface_kind, location = binding_location(binding, tracker)
            scope = _Scope(registry, settings, client, tracker, binding, surface_kind, location)
            running = await _running(sessionmaker, scope)
            labels = await _labels(sessionmaker, scope, running)
            sweeps = {
                label
                for label in labels
                if take_due(
                    due,
                    f"sweep:{tracker.name}:{binding.name}:{label}",
                    clock,
                    settings.factory_reconcile_interval_s,
                )
            }
            if polling or sweeps:
                await _poll_binding(
                    sessionmaker,
                    scope,
                    running,
                    labels if polling else sorted(sweeps),
                    polling=polling,
                    sweeps=sweeps,
                )


async def _cancel_orphans(
    sessionmaker: async_sessionmaker[AsyncSession],
    registry: ForgeRegistry,
    tracker: TrackerConfig,
) -> None:
    """Cancel the tracker's running WorkItems whose scope no binding serves."""

    async with sessionmaker() as session:
        rows = await session.scalars(
            select(WorkItem).where(
                WorkItem.tracker_kind == tracker.kind,
                WorkItem.tracker_host == tracker.host,
                WorkItem.cancelled_at.is_(None),
            )
        )
        orphans = [
            item.tracker_issue
            for item in rows
            if binding_for_scope(registry, item.tracker_issue) is None
        ]
    for issue in orphans:
        async with sessionmaker() as session:
            await lock_issue(session, issue)
            existing = await work_item_for(session, issue)
            if existing is None or existing.cancelled_at is not None:
                continue
            result = await cancel_work_item(session, existing)
            await session.commit()
        logger.warning(
            "tracker intake cancelled work item %s on tracker %s: no binding serves its scope (%s)",
            existing.id,
            tracker.name,
            result.status,
        )


async def _labels(
    sessionmaker: async_sessionmaker[AsyncSession],
    scope: _Scope,
    running: dict[str, list[TrackerIssueRef]],
) -> list[str]:
    """The labels bound on the binding's surface plus those its open work runs under."""

    async with sessionmaker() as session:
        bound = await bound_labels(session, scope.registry, scope.surface_kind, scope.location)
    return sorted(set(bound) | set(running))


async def _running(
    sessionmaker: async_sessionmaker[AsyncSession], scope: _Scope
) -> dict[str, list[TrackerIssueRef]]:
    """The binding scope's WorkItems not cancelled, by the label that admitted them."""

    async with sessionmaker() as session:
        rows = await session.scalars(
            select(WorkItem).where(
                WorkItem.tracker_kind == scope.tracker.kind,
                WorkItem.tracker_host == scope.tracker.host,
                WorkItem.tracker_scope_id == scope.binding.tracker_scope_id,
                WorkItem.cancelled_at.is_(None),
            )
        )
        grouped: dict[str, list[TrackerIssueRef]] = {}
        for item in rows:
            label = item.trigger_label or scope.tracker.label
            grouped.setdefault(label, []).append(item.tracker_issue)
    return grouped


async def _poll_binding(
    sessionmaker: async_sessionmaker[AsyncSession],
    scope: _Scope,
    running: dict[str, list[TrackerIssueRef]],
    labels: list[str],
    *,
    polling: bool,
    sweeps: set[str],
) -> None:
    for label in labels:
        try:
            tracker = tracker_for_binding(scope.registry, scope.client, scope.binding, label=label)
        except ForgeError:
            logger.warning(
                "tracker intake skipped binding %s: tracker %s cannot be built",
                scope.binding.name,
                scope.tracker.name,
            )
            return
        # A label whose page could not be read is not swept in the same pass:
        # the tracker could not answer it now. A held issue does not stop the
        # sweep, which skips that issue until the next pass.
        held: set[TrackerIssueRef] = set()
        polled = not polling or await _poll_label(
            sessionmaker, scope, tracker, label, running.get(label, []), held
        )
        if polled and label in sweeps:
            await _sweep_label(sessionmaker, scope, tracker, label, held)


def _deferred(scope: _Scope, label: str, what: str, exc: Exception) -> None:
    """Log a deferred pass. A refused or unreadable credential is a WARNING with
    a fixed classification and never the exception's text (#4630), so an
    operator sees a rotation gone wrong; any other deferral stays INFO."""

    classification = (
        "credential unreadable"
        if isinstance(exc, CredentialUnreadable)
        else "authentication rejected"
        if isinstance(exc, Unauthorized)
        else None
    )
    if classification is not None:
        logger.warning(
            "tracker intake %s for tracker %s binding %s label %r deferred: %s",
            what,
            scope.tracker.name,
            scope.binding.name,
            label,
            classification,
        )
        return
    # A port error carries only its code, never forge text (ADR 0197).
    logger.info(
        "tracker intake %s for tracker %s binding %s label %r deferred: %s %s",
        what,
        scope.tracker.name,
        scope.binding.name,
        label,
        type(exc).__name__,
        exc,
    )


async def _hold(
    scope: _Scope, tracker: Tracker, label: str, issue: TrackerIssueRef, exc: Exception
) -> None:
    """Log one issue's deferral and say so on the issue once (upserted)."""

    _deferred(scope, label, "notice", exc)
    try:
        await tracker.marked_comments.upsert_marked(
            ReplyTarget.on_issue(issue), HOLD_MARKER, _HOLD_BODY
        )
    except ForgeError as failed:
        # The retained cursor is the retry authority; the hold is a courtesy.
        logger.info(
            "tracker intake for binding %s could not post a hold: %s",
            scope.binding.name,
            type(failed).__name__,
        )


async def _poll_label(
    sessionmaker: async_sessionmaker[AsyncSession],
    scope: _Scope,
    tracker: Tracker,
    label: str,
    running: list[TrackerIssueRef],
    held: set[TrackerIssueRef],
) -> bool:
    """Poll one label from its stored cursor; False when the page was not read.

    A deferred notice adds its issue to ``held``: that issue's later notices
    wait, the others are applied, and the cursor stays for the next poll to
    replay.
    """

    key = f"marked:{label}"
    stored = await _load_cursor(sessionmaker, scope, key)
    try:
        page = await tracker.poll_marked(stored, running=tuple(running))
    except _DEFERRED as exc:
        _deferred(scope, label, "poll", exc)
        return False
    except ValueError:
        # A stored cursor this adapter cannot read is left as it is: resetting
        # it to a first poll would drop the adds since.
        logger.warning(
            "tracker intake for tracker %s binding %s label %r has an unreadable cursor",
            scope.tracker.name,
            scope.binding.name,
            label,
        )
        return False
    for notice in page.notices:
        if notice.issue in held:
            continue
        try:
            await _apply(sessionmaker, scope, tracker, label, notice)
        except _DEFERRED as exc:
            held.add(notice.issue)
            await _hold(scope, tracker, label, notice.issue, exc)
    if not held:
        await _save_cursor(sessionmaker, scope, key, page.cursor)
    return True


async def _sweep_label(
    sessionmaker: async_sessionmaker[AsyncSession],
    scope: _Scope,
    tracker: Tracker,
    label: str,
    held: set[TrackerIssueRef],
) -> None:
    """Apply the latest label add of every labelled issue again (plan D2).

    No cursor is read or written. An admitted issue replays under its request
    id; a refused one is judged again on its current ticket. An issue in
    ``held`` is skipped, and one that defers here is held.
    """

    try:
        page = await tracker.poll_marked(None, running=())
    except _DEFERRED as exc:
        _deferred(scope, label, "sweep", exc)
        return
    for notice in page.notices:
        if notice.disposition is not Disposition.ADMIT or notice.issue in held:
            continue
        try:
            await _apply(sessionmaker, scope, tracker, label, notice)
        except _DEFERRED as exc:
            held.add(notice.issue)
            await _hold(scope, tracker, label, notice.issue, exc)


async def _load_cursor(
    sessionmaker: async_sessionmaker[AsyncSession], scope: _Scope, key: str
) -> str | None:
    async with sessionmaker() as session:
        row = await session.get(FactoryPollCursor, scope.cursor_key)
    if row is None or not isinstance(row.etags, dict):
        return None
    value = row.etags.get(key)
    return value if isinstance(value, str) else None


async def _save_cursor(
    sessionmaker: async_sessionmaker[AsyncSession], scope: _Scope, key: str, cursor: str
) -> None:
    async with sessionmaker() as session:
        row = await session.get(FactoryPollCursor, scope.cursor_key)
        if row is None:
            kind, host, scope_id = scope.cursor_key
            session.add(
                FactoryPollCursor(
                    tracker_kind=kind,
                    tracker_host=host,
                    tracker_scope_id=scope_id,
                    scope_path=scope.binding.name,
                    etags={key: cursor},
                    updated_at=datetime.now(UTC),
                )
            )
        else:
            etags = dict(row.etags) if isinstance(row.etags, dict) else {}
            etags[key] = cursor
            row.etags = etags
            row.scope_path = scope.binding.name
            row.updated_at = datetime.now(UTC)
        await session.commit()


async def _apply(
    sessionmaker: async_sessionmaker[AsyncSession],
    scope: _Scope,
    tracker: Tracker,
    label: str,
    notice: MarkedNotice,
) -> None:
    """Apply one notice under its issue lock, committing what it decided.

    A deliberate refusal returns; a deferral raises one of ``_DEFERRED`` so the
    notice's issue is held and the label's cursor stays.
    """

    async with sessionmaker() as session:
        await lock_issue(session, notice.issue)
        try:
            if not await tracker.verify_current(notice):
                return
            if notice.disposition is Disposition.ADMIT:
                await _admit(session, scope, tracker, label, notice)
            elif notice.disposition is Disposition.MENTION:
                await _mention(session, scope, tracker, notice)
            else:
                await _cancel(session, scope, label, notice)
        except NotFound:
            # The issue or a repository it names is gone: nothing to retry.
            await session.rollback()
            logger.info(
                "tracker intake for binding %s skipped a notice on a missing issue",
                scope.binding.name,
            )
            return
        except BaseException:
            await session.rollback()
            raise
        await session.commit()


async def _comment(tracker: Tracker, issue: TrackerIssueRef, marker: str, body: str) -> None:
    try:
        await tracker.marked_comments.upsert_marked(ReplyTarget.on_issue(issue), marker, body)
    except ForgeError:
        raise FeedbackUnavailable("refusal_comment_unavailable") from None


async def _has_open_pr(session: AsyncSession, item: WorkItem) -> bool:
    if item.publication_lineage_id is None:
        return False
    status = await session.scalar(
        select(ThreadPublicationLineage.status).where(
            ThreadPublicationLineage.id == item.publication_lineage_id
        )
    )
    return status == "open"


async def _admit(
    session: AsyncSession,
    scope: _Scope,
    tracker: Tracker,
    label: str,
    notice: MarkedNotice,
) -> None:
    issue = notice.issue
    binding = scope.binding
    if not await may_start_with(binding.start_authority, issue, notice.actor, tracker=tracker):
        logger.info(
            "tracker intake for binding %s ignored a label by an account without start authority",
            binding.name,
        )
        return
    bound = await bound_labels(session, scope.registry, scope.surface_kind, scope.location)
    matches = bound.get(label, [])
    if len(matches) != 1:
        if matches:
            logger.warning(
                "tracker intake refused binding %s label %r: agents %s all serve it",
                binding.name,
                label,
                ", ".join(str(row.agent_id) for row in matches),
            )
        else:
            logger.info(
                "tracker intake refused binding %s label %r: binding_missing", binding.name, label
            )
        return
    surface = matches[0]
    existing = await work_item_for(session, issue)
    ticket = await tracker.ticket_labels(issue)
    refusal = label_refusal(
        label=label,
        owner_label=None if existing is None else existing.trigger_label or scope.tracker.label,
        other_agent=existing is not None and existing.agent_id != surface.agent_id,
        present=sorted(name for name in bound if name in ticket.labels),
    )
    if refusal is not None:
        await _comment(tracker, issue, LABEL_REFUSAL_MARKER, refusal[1])
        return
    chosen = resolve_repository(
        binding, tracker=scope.tracker, labels=ticket.labels, components=ticket.components
    )
    if isinstance(chosen, Refusal):
        await _comment(tracker, issue, REPO_REFUSAL_MARKER, refusal_body(chosen))
        return
    connection, code_host, repository = await _chosen_repository(scope, chosen)
    if existing is None and await _rediscovered(scope, tracker, notice, repository):
        return
    admission = await _admission(
        tracker,
        scope,
        label,
        notice,
        surface.agent_id,
        chosen,
        connection,
        code_host,
        repository,
        existing is not None and await _has_open_pr(session, existing),
        ticket,
    )
    if admission is None:
        return
    async with AsyncSession(
        bind=await session.connection(),
        join_transaction_mode="create_savepoint",
        expire_on_commit=False,
    ) as dispatch:
        result = await workitem_dispatch.readmit(dispatch, admission)
    _log_dispatch(scope, result)


async def _chosen_repository(
    scope: _Scope, repo: RepositoryBindingConfig
) -> tuple[CodeHostConnection, CodeHost, RepositoryRef]:
    """The chosen repository's code host entry, its adapter, and the repository
    read now by its configured path; the answer's immutable id must be the
    configured one, or the repository is refused as moved."""

    connection = scope.registry.code_host(repo.code_host)
    code_host = connection.adapter(scope.client)
    repository = await resolve_stored(
        code_host,
        RepositoryRef(connection.config.kind, connection.config.host, repo.project_id, repo.path),
    )
    return connection, code_host, repository


async def _rediscovered(
    scope: _Scope, tracker: Tracker, notice: MarkedNotice, repository: RepositoryRef
) -> bool:
    """True when the code host already has this issue's factory publication,
    so an admission with no local WorkItem is refused (#4629).

    An open, ambiguous or unconfirmed (Bitbucket Cloud, #4747) one is named
    on the issue; a completed one
    whose trigger this notice replays is only logged. Evidence that cannot be
    read completely defers (one of ``_DEFERRED``), so the cursor stays. A
    `NotFound` from the listing defers too: the repository was just resolved, so
    a missing answer is incomplete evidence, not a missing issue.
    """

    code_host = factory_rediscovery.publishing_code_host(scope.settings, scope.client, repository)
    try:
        found = await factory_rediscovery.rediscover(
            tracker, code_host, notice.issue, repository, event_at=notice.occurred_at
        )
    except (Unavailable, Unauthorized):
        raise
    except ForgeError as exc:
        raise Unavailable("publication_listing") from exc
    if found.verdict == "clear":
        return False
    try:
        await factory_rediscovery.report(tracker, notice.issue, found)
    except ForgeError:
        raise FeedbackUnavailable("publication_report_unavailable") from None
    logger.info(
        "tracker intake for binding %s refused an admission: publication_exists (%s)",
        scope.binding.name,
        found.verdict,
    )
    return True


async def _admission(
    tracker: Tracker,
    scope: _Scope,
    label: str,
    notice: MarkedNotice,
    agent_id: uuid.UUID,
    repo: RepositoryBindingConfig,
    connection: CodeHostConnection,
    code_host: CodeHost,
    repository: RepositoryRef,
    has_open_pr: bool,
    ticket: TicketLabels,
) -> Admission | None:
    """The admission facts on the chosen repository, or None after a base refusal."""

    installation = await connection.installation_for(repository)
    base: factory_base.ResolvedBase | None = None
    if not has_open_pr:
        resolved = await factory_base.resolve_base(
            policy=repo,
            labels=set(ticket.labels),
            default_branch=repository.default_branch,
            code_host=code_host,
            repository=repository,
        )
        if isinstance(resolved, factory_base.BaseRefusal):
            await factory_base.comment_refusal(tracker.marked_comments, notice.issue, resolved)
            return None
        base = resolved
    kind, address, conversation = reply_route(scope.registry, notice.issue, repository.path)
    return Admission(
        agent_id=agent_id,
        kind=kind,
        address=address,
        reply_conversation_id=conversation,
        issue=notice.issue,
        repository=repository,
        code_host_installation_id=installation,
        objective=issue_link(scope.registry, notice.issue, repository),
        requester=f"{tracker.kind}:{notice.actor.id}:{notice.actor.login}",
        request_id=notice_request_id(notice),
        trigger_label=label,
        base=base,
    )


async def _mention(
    session: AsyncSession, scope: _Scope, tracker: Tracker, notice: MarkedNotice
) -> None:
    issue = notice.issue
    existing = await work_item_for(session, issue)
    if existing is None:
        return
    binding = scope.binding
    if not await may_start_with(binding.start_authority, issue, notice.actor, tracker=tracker):
        logger.info(
            "tracker intake for binding %s ignored a mention by an account without start authority",
            binding.name,
        )
        return
    surface = await agent_binding(session, existing.agent_id, scope.surface_kind, scope.location)
    if surface is None:
        return
    repository = existing.repository
    kind, address, conversation = reply_route(scope.registry, issue, repository.path)
    link = issue_link(scope.registry, issue, repository)
    admission = Admission(
        agent_id=existing.agent_id,
        kind=kind,
        address=address,
        reply_conversation_id=conversation,
        issue=issue,
        repository=repository,
        code_host_installation_id=existing.code_host_installation_id,
        objective=mention_objective(
            link, issue.kind, mention_comment_id(issue.kind, notice.event_id)
        ),
        requester=f"{tracker.kind}:{notice.actor.id}:{notice.actor.login}",
        request_id=notice_request_id(notice),
    )
    async with AsyncSession(
        bind=await session.connection(),
        join_transaction_mode="create_savepoint",
        expire_on_commit=False,
    ) as dispatch:
        result = await workitem_dispatch.admit_revision(dispatch, admission)
    _log_dispatch(scope, result)


async def _cancel(session: AsyncSession, scope: _Scope, label: str, notice: MarkedNotice) -> None:
    """Removing the label that admitted the WorkItem, or closing the issue, cancels it."""

    existing = await work_item_for(session, notice.issue)
    if existing is None:
        return
    if (existing.trigger_label or scope.tracker.label) != label:
        return
    await cancel_work_item(session, existing)


def _log_dispatch(scope: _Scope, result: Any) -> None:
    code = getattr(result, "code", None)
    if code is not None:
        logger.info("tracker intake dispatch for binding %s refused: %s", scope.binding.name, code)
