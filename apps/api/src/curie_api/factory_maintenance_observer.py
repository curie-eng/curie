"""Verify live factory maintenance observations before reserving a successor."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime
from uuid import UUID

import httpx
import redis.asyncio as redis
from channel_protocol.work_item_events import CI_MAX_ROUNDS
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from . import factory_ci
from .config import Settings
from .factory_base_reconcile import base_selection_unchanged
from .factory_ci_recovery import settled_ci_facts
from .factory_conflict import conflict_graph_refusal
from .factory_maintenance import (
    MaintenanceObservation,
    MaintenanceRefusal,
    MaintenanceRepair,
    RefusalReason,
)
from .factory_maintenance_progress import record_event
from .factory_maintenance_store import commit_observation, replay
from .forges.errors import ForgeError, Unsupported
from .forges.hosts import issue_url
from .forges.ports import CodeHost
from .forges.registry import forges_registry
from .forges.trackers import code_host_for_repository, tracker_for_work_item
from .forges.types import (
    Actor,
    CheckState,
    CredentialScope,
    Disposition,
    MarkedNotice,
    PullRequest,
    PullRequestRef,
    PullRequestState,
    RepositoryRef,
    RollupState,
)
from .github_review_events import FeedbackUnavailable
from .models import (
    ExecutionRequest,
    FactoryMaintenanceAdmission,
    FactoryMaintenanceLineage,
    ThreadPublicationLineage,
    WorkItem,
)
from .repository_access import issue_stored_credential
from .workitems import lifecycle


def pull_refusal(
    lineage: ThreadPublicationLineage, pull: PullRequest, head_sha: str, base_ref: str
) -> RefusalReason | None:
    pr_id = pull.node_id if pull.ref.repository.kind == "github" else pull.ref.number
    if (
        not lineage.code_host_pr_id
        or lineage.code_host_pr_id != pr_id
        or lineage.repository_project_id != pull.ref.repository.project_id
        or str(lineage.pr_number) != pull.ref.number
        or pull.head_ref != lineage.branch
        or pull.url.casefold() != (lineage.pr_url or "").casefold()
    ):
        return "identity_unverified"
    if pull.state is not PullRequestState.OPEN:
        return "pr_closed"
    if pull.head_sha != head_sha:
        return "foreign_head"
    if pull.base_ref != base_ref:
        return "base_changed"
    return None


async def base_refusal(
    host: CodeHost, repository: RepositoryRef, base_ref: str, pinned: str
) -> tuple[RefusalReason | None, str | None]:
    tip = await host.branch_head(repository, base_ref)
    if not tip:
        return "evidence_unavailable", None
    if tip != pinned and not await host.is_ancestor(repository, pinned, tip):
        return "base_changed", tip
    return None, tip


class MaintenanceObserver:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        settings: Settings,
        client: httpx.AsyncClient,
        valkey: redis.Redis,
    ) -> None:
        self.sessions = sessions
        self.settings = settings
        self.client = client
        self.valkey = valkey

    def _host(self, item: WorkItem, lineage: ThreadPublicationLineage) -> CodeHost:
        return code_host_for_repository(
            forges_registry(self.settings),
            self.client,
            item.repository,
            installations=factory_ci.stored_installation(lineage, item),
        )

    async def poll(self, lineage_id: UUID, pull: PullRequest | None) -> None:
        # The cheap open-lineage pass selects the stimulus; admission re-reads
        # authority and evidence. Ordinary green polls produce no refusal.
        if pull is None or pull.state is PullRequestState.MERGED:
            return
        async with self.sessions() as session:
            row = (
                await session.execute(
                    select(
                        WorkItem,
                        ExecutionRequest,
                        ThreadPublicationLineage,
                        FactoryMaintenanceLineage,
                    )
                    .join(ExecutionRequest, ExecutionRequest.work_item_id == WorkItem.id)
                    .join(
                        ThreadPublicationLineage,
                        WorkItem.publication_lineage_id == ThreadPublicationLineage.id,
                    )
                    .join(
                        FactoryMaintenanceLineage,
                        FactoryMaintenanceLineage.lineage_id == ThreadPublicationLineage.id,
                    )
                    .where(
                        ThreadPublicationLineage.id == lineage_id,
                        FactoryMaintenanceLineage.final_poll_at.is_(None),
                    )
                    .order_by(ExecutionRequest.sequence.desc())
                    .limit(1)
                )
            ).one_or_none()
            if row is None:
                return
            item, request, lineage, window = row
            if request.status != "completed" or not lineage.head_sha or not lineage.base_ref:
                return
            observation = MaintenanceObservation(
                work_item_id=item.id,
                request_id=request.id,
                lineage_id=lineage.id,
                work_item_version=item.version,
                request_version=request.version,
                lineage_version=lineage.version,
                head_sha=lineage.head_sha,
                base_ref=lineage.base_ref,
                base_sha=lineage.base_sha,
                reason="base_conflict" if pull.conflicted else "late_ci_red",
            )
            now = await lifecycle.database_now(session)
            final_poll = now >= window.expires_at
            session.expunge_all()
        tip = None
        try:
            async with asyncio.timeout(15):
                host = self._host(item, lineage)
                foreign = pull.head_sha != observation.head_sha
                actionable = (
                    foreign
                    or pull.conflicted
                    or (await host.observe_ci(item.repository, observation.head_sha)).state
                    is RollupState.FAILURE
                )
                if actionable:
                    tip = await host.branch_head(item.repository, observation.base_ref)
        except (ForgeError, TimeoutError) as error:
            if pull.conflicted and not final_poll and not isinstance(error, Unsupported):
                await self._refuse_poll(observation, "evidence_unavailable")
            raise
        if actionable:
            if tip is not None:
                observation = replace(observation, base_sha=tip)
            if final_poll:
                await self._refuse_poll(observation, "window_expired")
            elif pull.conflicted and tip is None:
                await self._refuse_poll(observation, "evidence_unavailable")
            else:
                await self(observation)
        if final_poll:
            async with self.sessions() as session:
                await session.execute(
                    update(FactoryMaintenanceLineage)
                    .where(
                        FactoryMaintenanceLineage.lineage_id == lineage_id,
                        FactoryMaintenanceLineage.final_poll_at.is_(None),
                    )
                    .values(final_poll_at=now)
                )
                await session.commit()

    async def _refuse_poll(
        self, observation: MaintenanceObservation, refusal: RefusalReason
    ) -> None:
        async with self.sessions() as session:
            await commit_observation(session, observation, objective=None, refusal=refusal)

    async def __call__(
        self, observation: MaintenanceObservation, /
    ) -> MaintenanceRepair | MaintenanceRefusal:
        async with self.sessions() as session:
            receipt = await replay(session, observation)
            if receipt is not None:
                return receipt
            item = await session.get(WorkItem, observation.work_item_id)
            request = await session.get(ExecutionRequest, observation.request_id)
            lineage = await session.get(ThreadPublicationLineage, observation.lineage_id)
            origin = await session.get(FactoryMaintenanceLineage, observation.lineage_id)
            facts = (
                await settled_ci_facts(session, item, request, lineage, request_status="completed")
                if item
                and request
                and lineage
                and request.work_item_id == item.id
                and item.publication_lineage_id == lineage.id
                else None
            )
            session.expunge_all()
        objective = None
        refusal: RefusalReason | None
        if not self.settings.factory_maintenance_repair_enabled:
            refusal = "repair_disabled"
        elif facts is None:
            refusal = "evidence_unavailable"
        else:
            try:
                async with asyncio.timeout(45):
                    objective, refusal = await self._verify(
                        observation,
                        facts,
                        origin.latest_green_at
                        if origin and origin.latest_green_head_sha == observation.head_sha
                        else None,
                    )
            except (
                ForgeError,
                FeedbackUnavailable,
                TimeoutError,
                httpx.HTTPError,
                redis.RedisError,
            ):
                refusal = "evidence_unavailable"
        async with self.sessions() as session:
            return await commit_observation(
                session, observation, objective=objective, refusal=refusal
            )

    async def _verify(
        self, obs: MaintenanceObservation, facts: factory_ci.CiFacts, green_at: datetime | None
    ) -> tuple[str | None, RefusalReason | None]:
        item, lineage = facts.work_item, facts.lineage
        registry = forges_registry(self.settings)
        host = self._host(item, lineage)
        repository = await host.resolve_repository(item.repository.project_id)
        if (
            repository != item.repository
            or repository.path.casefold() != item.repository.path.casefold()
        ):
            return None, "identity_unverified"
        pull = await host.read_pull_request(PullRequestRef(repository, str(lineage.pr_number)))
        refusal = pull_refusal(lineage, pull, obs.head_sha, obs.base_ref)
        if refusal:
            return None, refusal
        refusal, current_base = await base_refusal(host, repository, obs.base_ref, obs.base_sha)
        if refusal:
            return None, refusal
        tracker = tracker_for_work_item(
            registry,
            self.client,
            item.tracker_issue,
            repository,
            item.code_host_installation_id,
            label=item.trigger_label,
        )
        marker = item.trigger_label
        if marker is not None:
            notice = MarkedNotice(
                item.tracker_issue,
                marker,
                Actor("observation", "observation"),
                "maintenance",
                Disposition.CANCEL,
                "",
                None,
            )
            if await tracker.verify_current(notice):
                return None, "work_item_cancelled"
        ticket = await tracker.ticket_labels(item.tracker_issue)
        if ticket.verified_open_issue is False or (
            marker is not None and marker not in ticket.labels
        ):
            return None, "work_item_cancelled"
        if marker is None and ticket.verified_open_issue is not True:
            return None, "evidence_unavailable"
        async with self.sessions() as session:
            if not await base_selection_unchanged(
                session,
                item,
                labels=set(ticket.labels),
                default_branch=repository.default_branch,
                settings=self.settings,
            ):
                return None, "retarget_pending"
            now = await lifecycle.database_now(session)
        issue = issue_url(self.settings, item.tracker_issue, repository)
        verdict, view, _, _, now = await factory_ci.inspect_ci(
            self.sessions,
            self.valkey,
            self.settings,
            host,
            repository,
            facts,
            now,
            may_observe=lambda: True,
            require_complete_evidence=True,
        )
        if view is None or verdict is None or view.reason or view.pull_head_sha is None:
            return None, "evidence_unavailable"
        if view.closed_unmerged or view.merged:
            return None, "pr_closed"
        if view.pull_head_sha != obs.head_sha:
            return None, "foreign_head"
        if obs.reason == "base_conflict":
            if verdict.kind != "merge_conflict" or not view.conflicted:
                return None, "no_current_conflict"
            credential = await issue_stored_credential(
                self.settings, self.client, repository=repository, scope=CredentialScope.CLONE
            )
            graph_refusal = await conflict_graph_refusal(obs, lineage, credential, now)
            if graph_refusal is not None:
                return None, graph_refusal
            return issue, None
        if green_at is None:
            return None, "prior_green_missing"
        if (
            view.conflicted is not False
            or not current_base
            or view.compared_base_sha != current_base
        ):
            return None, "evidence_unavailable"
        if verdict.kind != "failing":
            return None, "failure_not_attributable"
        failures = [c for c in view.checks if c.key in verdict.failing_keys]
        if not failures or any(
            c.head_sha != obs.head_sha
            or c.state is not CheckState.FAILURE
            or c.started_at is None
            or c.started_at <= green_at
            or c.started_at < facts.published_at
            or c.started_at > now
            for c in failures
        ):
            return None, "failure_not_attributable"
        return factory_ci.continuation_text(
            issue,
            pull.url,
            obs.head_sha,
            CI_MAX_ROUNDS,
            replace(view, checks=tuple(failures)),
            diagnostics=True,
        ), None


async def repair_refusal(
    session: AsyncSession,
    request_id: UUID | None,
    settings: Settings,
    client: httpx.AsyncClient,
    *,
    record: bool = False,
) -> RefusalReason | None:
    """Recheck the stored admission before acquisition or publication."""
    if request_id is None:
        return None
    row = (
        await session.execute(
            select(FactoryMaintenanceAdmission, ThreadPublicationLineage, WorkItem)
            .join(
                ThreadPublicationLineage,
                ThreadPublicationLineage.id == FactoryMaintenanceAdmission.lineage_id,
            )
            .join(WorkItem, WorkItem.id == FactoryMaintenanceAdmission.work_item_id)
            .where(FactoryMaintenanceAdmission.repair_request_id == request_id)
        )
    ).one_or_none()
    if row is None:
        return None
    admission, lineage, item = row
    refusal = await _current_refusal(admission, lineage, item, settings, client)
    if refusal and record:
        await lifecycle.lock_work_item(session, item.id)
        await record_event(
            session, admission, refusal, repair_request_id=admission.repair_request_id
        )
    return refusal


async def _current_refusal(
    admission: FactoryMaintenanceAdmission,
    lineage: ThreadPublicationLineage,
    item: WorkItem,
    settings: Settings,
    client: httpx.AsyncClient,
) -> RefusalReason | None:
    if not settings.factory_maintenance_repair_enabled:
        return "repair_disabled"
    if item.cancelled_at is not None:
        return "work_item_cancelled"
    if lineage.status != "open":
        return "pr_closed"
    if lineage.head_sha != admission.head_sha:
        return "foreign_head"
    if item.publication_lineage_id != lineage.id or item.repository != lineage.repository:
        return "identity_unverified"
    if item.base_branch != admission.base_ref or lineage.base_ref != admission.base_ref:
        return "base_changed"
    try:
        async with asyncio.timeout(15):
            host = code_host_for_repository(
                forges_registry(settings),
                client,
                item.repository,
                installations=factory_ci.stored_installation(lineage, item),
            )
            pull = await host.read_pull_request(
                PullRequestRef(item.repository, str(lineage.pr_number))
            )
            refusal = pull_refusal(lineage, pull, admission.head_sha, admission.base_ref)
            if refusal:
                return refusal
            refusal, _ = await base_refusal(
                host, item.repository, admission.base_ref, admission.base_sha
            )
            return refusal
    except (ForgeError, httpx.HTTPError, TimeoutError):
        return "evidence_unavailable"
