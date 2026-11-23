"""Replay GitHub REST observations against the reconciler and its PostgreSQL store."""

from __future__ import annotations

from pathlib import Path

import httpx
import personal_execution_support as support
import pytest
import redis.asyncio as redis
from curie_api.config import get_settings
from curie_api.factory_maintenance import MaintenanceRefusal, MaintenanceRepair
from curie_api.factory_maintenance_observer import MaintenanceObserver, repair_refusal
from curie_api.forges.types import PullRequestRef
from curie_api.models import (
    ExecutionRequest,
    FactoryMaintenanceEvent,
    ThreadPublicationLineage,
    WorkItem,
)
from curie_api.workitems.lifecycle import database_now
from forge_fakes import github
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from test_factory_maintenance_store import seed_completed


def test_observer_rechecks_forge_and_restored_head_before_one_successor(
    clean_db: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    for key, value in github.factory_env(
        tmp_path, bindings=[github.github_repository_binding(repo_id=101)]
    ).items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("FACTORY_MAINTENANCE_REPAIR_ENABLED", "true")
    monkeypatch.setattr(github, "REPO_ID", 101)
    monkeypatch.setattr(github, "INSTALLATION_ID", 202)
    github.fake_installation_tokens(monkeypatch)
    get_settings.cache_clear()

    async def scenario(session: AsyncSession) -> None:
        observation = await seed_completed(session)
        item = await session.get(WorkItem, observation.work_item_id)
        lineage = await session.get(ThreadPublicationLineage, observation.lineage_id)
        assert item and lineage and lineage.pr_number
        forge = github.GitHubRepositoryFake()
        forge.branches = {"main": observation.base_sha, lineage.branch: observation.head_sha}
        number = forge.seed_pull(lineage.branch, "main")
        forge.pulls[lineage.pr_number] = forge.pulls.pop(number)
        forge.issues[2573] = {"number": 2573, "state": "open", "labels": []}
        mergeable: bool | None = True

        # GitHub REST pull payloads carry node_id and nullable mergeability.
        # https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request
        def handle(request: httpx.Request) -> httpx.Response:
            response = forge.handle(request)
            if request.url.path.endswith(f"/pulls/{lineage.pr_number}"):
                return httpx.Response(
                    200,
                    json={
                        **response.json(),
                        "mergeable": mergeable,
                        "node_id": lineage.code_host_pr_id,
                    },
                )
            return response

        settings = get_settings()
        sessions = async_sessionmaker(session.bind, expire_on_commit=False)
        async with (
            httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client,
            redis.Redis.from_url(settings.valkey_dsn()) as valkey,
        ):
            observer = MaintenanceObserver(sessions, settings, client, valkey)
            host = observer._host(item, lineage)
            ref = PullRequestRef(item.repository, str(lineage.pr_number))

            async def poll() -> None:
                await observer.poll(lineage.id, await host.read_pull_request(ref))

            async def history() -> list[str]:
                return list(
                    await session.scalars(
                        select(FactoryMaintenanceEvent.outcome).order_by(FactoryMaintenanceEvent.id)
                    )
                )

            forge.report_check(observation.head_sha, "unit", "success")
            await poll()
            assert await history() == ["green"]
            forge.report_check(observation.head_sha, "unit", "failure")
            forge.check_runs[observation.head_sha]["unit"]["started_at"] = (
                await database_now(session)
            ).isoformat()
            forge.report_check(observation.base_sha, "unit", "failure")
            await poll()
            assert await history() == ["green", "failure_not_attributable"]
            forge.report_check(observation.base_sha, "unit", "success")
            mergeable = None
            assert await observer(observation) == MaintenanceRefusal(reason="evidence_unavailable")
            mergeable = True
            forge.branches[lineage.branch] = "c" * 40
            await poll()
            await poll()
            assert await history() == [
                "green",
                "failure_not_attributable",
                "evidence_unavailable",
                "foreign_head",
            ]
            assert await session.scalar(select(func.count()).select_from(ExecutionRequest)) == 1
            assert forge.writes == 0

            forge.branches[lineage.branch] = observation.head_sha
            await poll()
            assert (await history())[-1] == "admitted"
            reads = len(forge.http_requests)
            restarted = MaintenanceObserver(sessions, settings, client, valkey)
            receipt = await restarted(observation)
            assert isinstance(receipt, MaintenanceRepair) and receipt.status == "replayed"
            assert len(forge.http_requests) == reads
            assert await session.scalar(select(func.count()).select_from(ExecutionRequest)) == 2
            forge.seed_commit(observation.base_sha, parents=[], message="original base")
            advanced = forge.push_head("main")
            assert await repair_refusal(session, receipt.request_id, settings, client) is None
            forge.branches["main"] = "d" * 40
            assert (
                await repair_refusal(session, receipt.request_id, settings, client)
                == "evidence_unavailable"
            )
            forge.seed_commit("d" * 40, parents=[], message="unrelated base")
            assert (
                await repair_refusal(session, receipt.request_id, settings, client)
                == "base_changed"
            )
            forge.branches["main"] = advanced
            forge.branches[lineage.branch] = "c" * 40
            assert (
                await repair_refusal(session, receipt.request_id, settings, client, record=True)
                == "foreign_head"
            )
            await session.commit()
            assert (await history())[-2:] == ["admitted", "foreign_head"]
            assert forge.writes == 0

    try:
        support.with_session(scenario)
    finally:
        get_settings.cache_clear()
