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
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from curie_api.crud import lineages as crud_lineages
from curie_api.crud.errors import PublicationLineageConflict
from curie_api.forges.errors import ForgeError, Unavailable
from curie_api.forges.ports import CodeHost
from curie_api.forges.types import PullRequestRef, PullRequestState

from .config import Settings
from .forges.hosts import code_host_for, pull_request_ref
from .models import ThreadPublicationLineage, WorkItem

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


class LineageReconciler:
    """Observe merged and closed factory pull requests on a fixed cadence."""

    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        settings: Settings,
        code_host: Callable[[], CodeHost],
        *,
        interval_seconds: float,
        batch_limit: int = 50,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("the lineage reconcile interval must be positive")
        if batch_limit <= 0:
            raise ValueError("the lineage reconcile batch limit must be positive")
        self._sessionmaker = sessionmaker
        self._settings = settings
        self._code_host = code_host
        self._interval = interval_seconds
        self._batch_limit = batch_limit
        # Lineage ids are read in id order from just past this one, wrapping,
        # so a long list of open pull requests is read fairly across passes.
        self._after: uuid.UUID | None = None

    async def _batch(self) -> list[_Open]:
        async with self._sessionmaker() as session:
            query = (
                select(ThreadPublicationLineage, WorkItem.repository_project_id)
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
            for lineage, work_item_project_id in rows:
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
                        pull_request=pull_request_ref(
                            self._settings,
                            path=lineage.repo_full_name,
                            project_id=lineage.repository_project_id or work_item_project_id,
                            number=lineage.pr_number,
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

    async def run_once(self) -> PassResult:
        """One bounded pass over the open factory lineages."""

        result = PassResult()
        code_host = self._code_host()
        for item in await self._batch():
            try:
                pull = await code_host.read_pull_request(item.pull_request)
            except Unavailable as exc:
                result.skipped += 1
                if str(exc) == RATE_LIMITED:
                    result.rate_limited = True
                    break
                continue
            except ForgeError as exc:
                result.skipped += 1
                logger.warning(
                    "lineage %s pull request could not be read: %s", item.lineage_id, exc
                )
                continue
            result.read += 1
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


def start_lineage_reconciler(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
) -> asyncio.Task[None] | None:
    """Start the pass when the factory runs and the interval is positive."""

    interval = settings.factory_lineage_reconcile_interval_s
    if not (
        settings.github_factory_ingress_enabled
        and settings.work_item_reconciler_enabled
        and interval > 0
    ):
        return None
    reconciler = LineageReconciler(
        sessionmaker,
        settings,
        lambda: code_host_for(settings, client),
        interval_seconds=interval,
    )
    return asyncio.create_task(reconciler.run_forever())
