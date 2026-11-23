"""Verify conflict maintenance through the reconciler poll and a real repository."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import httpx
import personal_execution_support as support
import pytest
import redis.asyncio as redis
from curie_api import workitem_dispatch
from curie_api.config import Settings
from curie_api.factory_base import ResolvedBase
from curie_api.factory_ci import CI_GRACE_SECONDS
from curie_api.factory_maintenance_observer import MaintenanceObserver
from curie_api.forges.errors import Unavailable
from curie_api.forges.github.code_host import GitHubCodeHost
from curie_api.forges.types import Credential, CredentialScope, PullRequestRef, RepositoryRef
from curie_api.models import (
    DEFAULT_TENANT_ID,
    FactoryMaintenanceAdmission,
    FactoryMaintenanceEvent,
    Publication,
    ThreadPublicationLineage,
    WorkItem,
)
from curie_api.workitems import lifecycle
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from test_factory_maintenance_observer import MaintenanceForge
from test_factory_maintenance_observer import maintenance_settings as maintenance_settings
from test_factory_maintenance_store import _counts, _settle, seed_completed
from test_factory_rebase import BRANCH, GitGraph, git_http
from test_factory_rebase import graph as graph
from test_factory_rebase import same_base_conflict as same_base_conflict
from test_workitems import _grant_opened_pull_request, _start


@pytest.mark.parametrize("provider_error", [False, True])
def test_poll_records_unavailable_base_without_a_second_read(
    clean_db: None, maintenance_settings: Settings, provider_error: bool
) -> None:
    async def scenario(session: AsyncSession) -> None:
        source = await seed_completed(session)
        item = await session.get(WorkItem, source.work_item_id)
        lineage = await session.get(ThreadPublicationLineage, source.lineage_id)
        assert item and lineage
        forge = MaintenanceForge(lineage)
        forge.mergeable = False
        del forge.branches["main"]
        base_path = "/git/ref/heads/main"
        if provider_error:
            forge.fail_reads(base_path, 503)
        sessions = async_sessionmaker(session.bind, expire_on_commit=False)
        async with (
            httpx.AsyncClient(transport=httpx.MockTransport(forge.handle)) as client,
            redis.Redis.from_url(maintenance_settings.valkey_dsn()) as valkey,
        ):
            observer = MaintenanceObserver(sessions, maintenance_settings, client, valkey)
            host = observer._host(item, lineage)
            pull = await host.read_pull_request(PullRequestRef(item.repository, str(forge.number)))
            if provider_error:
                with pytest.raises(Unavailable):
                    await observer.poll(lineage.id, pull)
            else:
                await observer.poll(lineage.id, pull)
        assert sum(req.url.path.endswith(base_path) for req in forge.http_requests) == 1
        assert await _counts(session) == (1, 0, 0)
        outcomes = await session.scalars(
            select(FactoryMaintenanceEvent.outcome).where(
                FactoryMaintenanceEvent.reason == "base_conflict"
            )
        )
        assert outcomes.all() == ["evidence_unavailable"]
        assert forge.writes == 0

    support.with_session(scenario)


def test_poll_admits_one_successor_only_for_a_verified_conflict(
    clean_db: None,
    maintenance_settings: Settings,
    same_base_conflict: GitGraph,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = same_base_conflict

    async def scenario(session: AsyncSession) -> None:
        agent = await support._insert_agent(session, DEFAULT_TENANT_ID, address=support.ADDRESS)
        facts = replace(
            support.facts(agent, issue_number=2573),
            base=ResolvedBase("main", "default", repo.old_base),
        )
        admitted = await workitem_dispatch.admit(session, facts)
        assert isinstance(admitted, lifecycle.WorkItemOutcome)
        running = await _start(session, admitted)
        linked = await _grant_opened_pull_request(session, running, pr=123)
        assert linked.request and linked.work_item.publication_lineage_id
        item = await session.get(WorkItem, linked.work_item.id)
        lineage = await session.get(
            ThreadPublicationLineage, linked.work_item.publication_lineage_id
        )
        assert item and lineage and lineage.code_host_pr_id
        lineage.branch, lineage.base_sha, lineage.head_sha = BRANCH, repo.old_base, repo.source
        publication = (
            await session.scalars(
                select(Publication).where(Publication.execution_request_id == linked.request.id)
            )
        ).one()
        publication.base_sha = repo.old_base
        await session.commit()

        forge = MaintenanceForge(lineage)
        forge.mergeable = False
        forge.branches["main"] = repo.target
        sessions = async_sessionmaker(session.bind, expire_on_commit=False)
        async with (
            httpx.AsyncClient(transport=httpx.MockTransport(forge.handle)) as client,
            redis.Redis.from_url(maintenance_settings.valkey_dsn()) as valkey,
        ):
            observer = MaintenanceObserver(sessions, maintenance_settings, client, valkey)
            host = observer._host(item, lineage)
            ref = PullRequestRef(item.repository, str(forge.number))

            async def poll() -> None:
                await observer.poll(lineage.id, await host.read_pull_request(ref))

            async def outcomes() -> list[str]:
                return list(
                    await session.scalars(
                        select(FactoryMaintenanceEvent.outcome)
                        .where(FactoryMaintenanceEvent.reason == "base_conflict")
                        .order_by(FactoryMaintenanceEvent.id)
                    )
                )

            await poll()
            assert await _counts(session) == (1, 0, None)
            assert await outcomes() == []
            source = await _settle(session, linked, green=False)
            await poll()
            assert await outcomes() == ["no_current_conflict"]
            assert await _counts(session) == (1, 0, 0)

            assert publication.terminal_at is not None
            publication.terminal_at -= timedelta(seconds=CI_GRACE_SECONDS + 1)
            await session.commit()
            for forge.mergeable in (None, True):
                await poll()
            assert await outcomes() == ["no_current_conflict"]

            forge.mergeable = False
            forge.branches["main"] = repo.old_base
            await poll()
            forge.branches["main"] = repo.target
            forge.branches[BRANCH] = "c" * 40
            await poll()
            forge.branches[BRANCH] = repo.source
            forge.node_id = "PR_other"
            await poll()
            forge.node_id = lineage.code_host_pr_id
            forge.pulls[forge.number]["base"] = "release"
            await poll()
            forge.pulls[forge.number]["base"] = "main"
            forge.set_pull_state(forge.number, merged=True)
            await poll()
            assert (await outcomes())[-1] == "base_changed"
            forge.set_pull_state(forge.number, merged=False)
            await poll()
            forge.pulls[forge.number]["state"] = "open"
            assert await outcomes() == [
                "no_current_conflict",
                "no_current_conflict",
                "foreign_head",
                "identity_unverified",
                "base_changed",
                "pr_closed",
            ]
            assert await _counts(session) == (1, 0, 0)
            assert server.requests == []

            # Hold every eligibility fact fixed and move only the publication clock.
            now = (await lifecycle.database_now(session)).replace(tzinfo=None)
            publication.terminal_at = now - timedelta(seconds=90)
            await session.commit()
            await poll()
            assert (await outcomes())[-1] == "no_current_conflict"
            assert await _counts(session) == (1, 0, 0)
            assert server.requests == [] and forge.writes == 0
            publication.terminal_at = now - timedelta(seconds=121)
            await session.commit()
            await poll()
            await poll()
            assert (await outcomes())[-1] == "admitted"
            assert await _counts(session) == (2, 1, 1)
            receipt = (await session.scalars(select(FactoryMaintenanceAdmission))).one()
            assert (receipt.reason, receipt.mode, receipt.source_request_id) == (
                "base_conflict",
                "successor",
                source.request_id,
            )
            assert (receipt.head_sha, receipt.base_ref, receipt.base_sha) == (
                repo.source,
                "main",
                repo.target,
            )
            assert (receipt.work_item_id, receipt.lineage_id, receipt.code_host_pr_id) == (
                item.id,
                lineage.id,
                lineage.code_host_pr_id,
            )
            claims = await workitem_dispatch.claim_due(
                session, owner="reconciler", limit=10, lease_s=15
            )
            assert [claim.request_id for claim in claims] == [receipt.repair_request_id]
            assert server.requests and server.pushes == forge.writes == 0
            assert repo.head() == repo.source

    remote = repo.root / f"{support.REPO}.git"
    remote.parent.mkdir()
    remote.symlink_to(repo.origin, target_is_directory=True)
    issue_credential = GitHubCodeHost.credential
    with git_http(repo) as server:

        async def local_credential(
            host: GitHubCodeHost, repository: RepositoryRef, scope: CredentialScope
        ) -> Credential:
            assert repository.path == support.REPO and scope is CredentialScope.CLONE
            credential = await issue_credential(host, repository, scope)
            return replace(credential, origin=server.origin)

        monkeypatch.setattr(GitHubCodeHost, "credential", local_credential)
        support.with_session(scenario)
