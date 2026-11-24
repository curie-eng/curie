"""Record a factory pull request merged or closed on its lineage (#3831).

A publication lineage used to become merged or closed only when the worker's
publication path happened to read the pull request. A factory WorkItem whose
pull request is merged by a human therefore stayed ``open`` until the next
publication, so the WorkItem never observed the merge.

Each pass reads, through the code host, the pull request of every open lineage
a factory WorkItem is bound to, and records a merged or closed one with the
same compare-and-set the worker path uses
(`curie_api.crud.lineages.mark_publication_lineage_terminal`). It is
forge-neutral: it asks only `CodeHost.read_pull_request`.

1. An open pull request leaves the lineage untouched.
2. A lineage with a publication in flight is skipped; the publication's own
   post-push read records the outcome (ADR 0197 consequence 6).
3. A code host that cannot answer leaves the lineage open for the next pass.
   A rate-limited answer ends the pass, so the budget is not spent further.
4. Recording is idempotent: a lineage already terminal is no longer selected,
   and a concurrent writer's identical terminal state is accepted.
5. After the reads, the pass settles the learned rule candidates of a bounded
   batch of merged or closed lineages that still hold some (ADR 0287, #5796).
   Every terminal writer settles in its own transaction, but a replica from
   before learned rules records a terminal state without settling; this sweep
   settles those once a current replica runs. Each lineage settles in its own
   short transaction, holding its agent row first; the candidate row locks make
   two replicas' sweeps settle a lineage exactly once.
6. The sweep does not depend on the forge pass or on any switch. Every API
   replica also runs it as its own task (`start_rule_settlement`), every
   ``memory_rule_settlement_interval_s``, whether the WorkItem reconcilers,
   the factory or this forge pass are on or off: an older replica's
   publication advance can record a merge with no forge pass at all, and
   candidates outlive a factory switch-off.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass

import httpx
import redis.asyncio as redis
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from curie_api.crud import lineages as crud_lineages
from curie_api.crud.errors import PublicationLineageConflict
from curie_api.forges.errors import ForgeError, Unavailable
from curie_api.forges.ports import CodeHost
from curie_api.forges.types import PullRequestRef, PullRequestState, RepositoryRef

from .config import Settings
from .factory_maintenance_observer import MaintenanceObserver
from .forges.registry import forges_registry
from .forges.trackers import code_host_for_repository, stored_repository
from .memory_rule_lifecycle import lock_rule_owner, settle_lineage_rule_candidates
from .models import MemoryRule, ThreadPublicationLineage, WorkItem

logger = logging.getLogger(__name__)

RATE_LIMITED = "github_rate_limited"
_TERMINAL = {PullRequestState.MERGED: "merged", PullRequestState.CLOSED: "closed"}


@dataclass(frozen=True)
class _Open:
    lineage_id: uuid.UUID
    version: int
    head_sha: str
    branch: str
    pr_url: str
    pull_request: PullRequestRef


@dataclass
class PassResult:
    read: int = 0
    merged: int = 0
    closed: int = 0
    skipped: int = 0
    rate_limited: bool = False
    # Rule candidates changed by the settlement sweep (item 5).
    settled: int = 0


class LineageReconciler:
    """Observe merged and closed factory pull requests on a fixed cadence."""

    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        settings: Settings,
        code_host_for: Callable[[RepositoryRef], CodeHost],
        *,
        interval_seconds: float,
        batch_limit: int = 50,
        maintenance: MaintenanceObserver | None = None,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("the lineage reconcile interval must be positive")
        if batch_limit <= 0:
            raise ValueError("the lineage reconcile batch limit must be positive")
        self._sessionmaker = sessionmaker
        self._settings = settings
        self._maintenance = maintenance
        self._code_host_for = code_host_for
        self._interval = interval_seconds
        self._batch_limit = batch_limit
        # Lineage ids are read in id order from just past this one, wrapping,
        # so a long list of open pull requests is read fairly across passes.
        self._after: uuid.UUID | None = None
        # The same wrapping cursor for the settlement sweep.
        self._settle_after: uuid.UUID | None = None

    async def _batch(self) -> list[_Open]:
        async with self._sessionmaker() as session:
            query = (
                select(
                    ThreadPublicationLineage,
                    WorkItem.repository_project_id,
                    WorkItem.code_host_kind,
                    WorkItem.code_host_host,
                )
                .join(WorkItem, WorkItem.publication_lineage_id == ThreadPublicationLineage.id)
                .where(
                    ThreadPublicationLineage.status == "open",
                    ThreadPublicationLineage.pr_number.is_not(None),
                    ThreadPublicationLineage.pr_url.is_not(None),
                    ThreadPublicationLineage.head_sha.is_not(None),
                )
                .order_by(ThreadPublicationLineage.id)
                .limit(self._batch_limit)
            )
            rows = list(
                await session.execute(
                    query.where(ThreadPublicationLineage.id > self._after)
                    if self._after is not None
                    else query
                )
            )
            if not rows and self._after is not None:
                self._after = None
                rows = list(await session.execute(query))
            # Snapshot before the rollback, which expires every loaded row.
            found: list[_Open] = []
            seen: set[uuid.UUID] = set()
            for lineage, work_item_project_id, work_item_kind, work_item_host in rows:
                if lineage.id in seen:
                    continue
                seen.add(lineage.id)
                assert lineage.pr_number is not None and lineage.pr_url is not None
                assert lineage.head_sha is not None
                found.append(
                    _Open(
                        lineage_id=lineage.id,
                        version=lineage.version,
                        head_sha=lineage.head_sha,
                        branch=lineage.branch,
                        pr_url=lineage.pr_url,
                        # The code host entry the lineage (or its WorkItem)
                        # recorded (#4358 plan D14).
                        pull_request=PullRequestRef(
                            stored_repository(
                                self._settings,
                                kind=lineage.code_host_kind or work_item_kind,
                                host=lineage.code_host_host or work_item_host,
                                path=lineage.repo_full_name,
                                project_id=lineage.repository_project_id or work_item_project_id,
                            ),
                            str(lineage.pr_number),
                        ),
                    )
                )
            await session.rollback()
        if found:
            self._after = found[-1].lineage_id
        return found

    async def _record(self, item: _Open, state: str) -> bool:
        async with self._sessionmaker() as session:
            lineage = await session.get(ThreadPublicationLineage, item.lineage_id)
            if (
                lineage is None
                or lineage.status != "open"
                or lineage.version != item.version
                or await crud_lineages.publication_lineage_has_inflight_push(session, lineage)
            ):
                await session.rollback()
                return False
            try:
                await crud_lineages.mark_publication_lineage_terminal(
                    session,
                    lineage,
                    expected_version=item.version,
                    expected_head_sha=item.head_sha,
                    state=state,
                )
            except PublicationLineageConflict:
                return False
        return True

    async def _unsettled(self) -> list[tuple[uuid.UUID, uuid.UUID]]:
        """A bounded batch of terminal lineages still holding a rule candidate."""

        async with self._sessionmaker() as session:
            query = (
                select(ThreadPublicationLineage.id, ThreadPublicationLineage.agent_id)
                .where(
                    ThreadPublicationLineage.status.in_(("merged", "closed")),
                    ThreadPublicationLineage.id.in_(
                        select(MemoryRule.source_lineage_id).where(
                            MemoryRule.state == "candidate",
                            MemoryRule.source_lineage_id.is_not(None),
                        )
                    ),
                )
                .order_by(ThreadPublicationLineage.id)
                .limit(self._batch_limit)
            )
            after = self._settle_after
            rows = [
                (row[0], row[1])
                for row in await session.execute(
                    query.where(ThreadPublicationLineage.id > after) if after is not None else query
                )
            ]
            if not rows and after is not None:
                rows = [(row[0], row[1]) for row in await session.execute(query)]
            await session.rollback()
        self._settle_after = rows[-1][0] if rows else None
        return rows

    async def _settle(self, lineage_id: uuid.UUID, agent_id: uuid.UUID) -> int:
        async with self._sessionmaker() as session:
            # The agent row first, as every rule settlement (memory_rule_lifecycle).
            await lock_rule_owner(session, agent_id)
            state = await session.scalar(
                select(ThreadPublicationLineage.status).where(
                    ThreadPublicationLineage.id == lineage_id
                )
            )
            if state not in ("merged", "closed"):
                await session.rollback()
                return 0
            changed = await settle_lineage_rule_candidates(session, lineage_id, state)
            await session.commit()
        return changed

    async def settle_terminal_lineages(self) -> int:
        """Settle the candidates a terminal writer left (module item 5)."""

        settled = 0
        for lineage_id, agent_id in await self._unsettled():
            try:
                settled += await self._settle(lineage_id, agent_id)
            except SQLAlchemyError:
                logger.exception("lineage %s rule candidates could not be settled", lineage_id)
        return settled

    async def run_once(self) -> PassResult:
        """One bounded pass over the open factory lineages, then the settlement sweep."""

        result = PassResult()
        # One code host per stored (kind, host) entry within a pass.
        hosts: dict[tuple[str, str], CodeHost] = {}
        for item in await self._batch():
            repository = item.pull_request.repository
            key = (repository.kind, repository.host)
            try:
                code_host = hosts.get(key)
                if code_host is None:
                    code_host = hosts[key] = self._code_host_for(repository)
                pull = await code_host.read_pull_request(item.pull_request)
            except ForgeError as exc:
                result.skipped += 1
                if isinstance(exc, Unavailable) and str(exc) == RATE_LIMITED:
                    result.rate_limited = True
                    break
                if not isinstance(exc, Unavailable):
                    logger.warning(
                        "lineage %s pull request could not be read: %s", item.lineage_id, exc
                    )
                continue
            result.read += 1
            if self._maintenance is not None:
                try:
                    await self._maintenance.poll(item.lineage_id, pull)
                except (ForgeError, TimeoutError, SQLAlchemyError) as exc:
                    if str(exc) == RATE_LIMITED:
                        result.rate_limited = True
                        break
                    logger.info("maintenance observation deferred for lineage %s", item.lineage_id)
            state = _TERMINAL.get(pull.state)
            if state is None:
                continue
            if pull.url.casefold() != item.pr_url.casefold() or pull.head_ref != item.branch:
                result.skipped += 1
                logger.warning(
                    "lineage %s pull request no longer matches its stored identity",
                    item.lineage_id,
                )
                continue
            if not await self._record(item, state):
                result.skipped += 1
                continue
            if state == "merged":
                result.merged += 1
            else:
                result.closed += 1
        result.settled = await self.settle_terminal_lineages()
        return result

    async def run_forever(self) -> None:
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - a failed pass must not end the loop
                logger.exception("lineage reconcile pass failed")
            await asyncio.sleep(self._interval)

    async def run_settlement_forever(self) -> None:
        """Only the settlement sweep, every interval (item 6).

        Waits one interval before each sweep, so a process that just started
        touches nothing at once. A failed sweep is logged and the loop
        continues; cancellation ends it.
        """

        while True:
            await asyncio.sleep(self._interval)
            try:
                await self.settle_terminal_lineages()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - a failed sweep must not end the loop
                logger.exception("lineage rule settlement sweep failed")


def start_lineage_reconciler(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    valkey: redis.Redis,
) -> asyncio.Task[None] | None:
    """Start the pass when the factory runs and the interval is positive.

    The learned rule settlement sweep does not wait on this: it also runs as
    its own task (`start_rule_settlement`, module item 6).
    """

    interval = settings.factory_lineage_reconcile_interval_s
    if not (
        settings.factory_enabled
        and settings.work_item_reconciler_enabled
        and interval > 0
    ):
        return None
    reconciler = LineageReconciler(
        sessionmaker,
        settings,
        lambda repository: code_host_for_repository(forges_registry(settings), client, repository),
        interval_seconds=interval,
        maintenance=(
            MaintenanceObserver(sessionmaker, settings, client, valkey)
            if settings.factory_maintenance_repair_enabled
            else None
        ),
    )
    return asyncio.create_task(reconciler.run_forever())


def _no_code_host(_repository: RepositoryRef) -> CodeHost:
    raise RuntimeError("the rule settlement sweep reads no code host")


def start_rule_settlement(
    sessionmaker: async_sessionmaker[AsyncSession], settings: Settings
) -> asyncio.Task[None]:
    """Start the learned rule settlement sweep (module item 6, ADR 0287).

    Always started, whatever ``work_item_reconciler_enabled``, the factory or
    the forge pass interval say: no switch may strand a candidate whose pull
    request an older replica recorded merged or closed.
    """

    reconciler = LineageReconciler(
        sessionmaker,
        settings,
        _no_code_host,
        interval_seconds=settings.memory_rule_settlement_interval_s,
    )
    return asyncio.create_task(reconciler.run_settlement_forever())
