"""Replay GitHub REST observations against the reconciler and its PostgreSQL store."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal

import httpx
import personal_execution_support as support
import pytest
import redis.asyncio as redis
from curie_api.config import Settings, get_settings
from curie_api.factory_maintenance import (
    MaintenanceObservation,
    MaintenanceRefusal,
    MaintenanceRepair,
    RefusalReason,
)
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
from test_factory_ci_wait import _clock_offset
from test_factory_maintenance_store import seed_completed


@pytest.fixture
def maintenance_settings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[Settings]:
    for key, value in github.factory_env(
        tmp_path, bindings=[github.github_repository_binding(repo_id=101)]
    ).items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("FACTORY_MAINTENANCE_REPAIR_ENABLED", "true")
    monkeypatch.setattr(github, "REPO_ID", 101)
    monkeypatch.setattr(github, "INSTALLATION_ID", 202)
    github.fake_installation_tokens(monkeypatch)
    get_settings.cache_clear()
    yield get_settings()
    get_settings.cache_clear()


class MaintenanceForge(github.GitHubRepositoryFake):
    def __init__(self, lineage: ThreadPublicationLineage) -> None:
        super().__init__()
        assert lineage.pr_number and lineage.head_sha and lineage.base_ref
        self.branches = {lineage.base_ref: lineage.base_sha, lineage.branch: lineage.head_sha}
        seeded = self.seed_pull(lineage.branch, lineage.base_ref)
        self.number = lineage.pr_number
        self.pulls[self.number] = self.pulls.pop(seeded)
        self.issues[2573] = {"number": 2573, "state": "open", "labels": []}
        self.mergeable: bool | None = True
        self.node_id = lineage.code_host_pr_id

    def handle(self, request: httpx.Request) -> httpx.Response:
        response = super().handle(request)
        if response.status_code == 200 and request.url.path.endswith(f"/pulls/{self.number}"):
            # https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request
            return httpx.Response(
                200,
                json={
                    **response.json(),
                    "mergeable": self.mergeable,
                    "mergeable_state": "dirty" if self.mergeable is False else "unknown",
                    "node_id": self.node_id,
                },
            )
        return response


@asynccontextmanager
async def _observer(
    session: AsyncSession, observation: MaintenanceObservation
) -> AsyncIterator[tuple[MaintenanceObserver, MaintenanceForge]]:
    lineage = await session.get(ThreadPublicationLineage, observation.lineage_id)
    assert lineage
    forge = MaintenanceForge(lineage)
    settings = get_settings()
    sessions = async_sessionmaker(session.bind, expire_on_commit=False)
    async with (
        httpx.AsyncClient(transport=httpx.MockTransport(forge.handle)) as client,
        redis.Redis.from_url(settings.valkey_dsn()) as valkey,
    ):
        yield MaintenanceObserver(sessions, settings, client, valkey), forge


@pytest.mark.parametrize("expired_state", [None, "success", "failure", "conflict"])
def test_observer_rechecks_forge_and_restored_head_before_one_successor(
    clean_db: None, maintenance_settings: Settings, expired_state: str | None
) -> None:
    async def scenario(session: AsyncSession) -> None:
        observation = await seed_completed(session)
        item = await session.get(WorkItem, observation.work_item_id)
        lineage = await session.get(ThreadPublicationLineage, observation.lineage_id)
        assert item and lineage and lineage.pr_number
        async with _observer(session, observation) as (observer, forge):
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
            await observer.poll(lineage.id, None)
            forge.set_pull_state(lineage.pr_number, merged=True)
            await poll()
            assert await history() == ["green"]
            forge.pulls[lineage.pr_number].update(state="open", merged_at=None)
            if expired_state is not None:
                if expired_state == "conflict":
                    forge.mergeable = False
                else:
                    forge.report_check(observation.head_sha, "unit", expired_state)
                with _clock_offset(timedelta(days=1, minutes=1).total_seconds()):
                    await poll()
                    expected = (
                        ["green", "window_expired"]
                        if expired_state in {"failure", "conflict"}
                        else ["green"]
                    )
                    assert await history() == expected
                    reads = len(forge.http_requests)
                    restarted = MaintenanceObserver(
                        observer.sessions, observer.settings, observer.client, observer.valkey
                    )
                    await restarted.poll(lineage.id, await host.read_pull_request(ref))
                    assert len(forge.http_requests) == reads + 1
                assert await session.scalar(select(func.count()).select_from(ExecutionRequest)) == 1
                return
            forge.report_check(observation.head_sha, "unit", "failure")
            forge.check_runs[observation.head_sha]["unit"]["started_at"] = (
                await database_now(session)
            ).isoformat()
            forge.report_check(observation.base_sha, "unit", "failure")
            await poll()
            assert await history() == ["green", "failure_not_attributable"]
            forge.report_check(observation.base_sha, "unit", "success")
            forge.mergeable = None
            assert await observer(observation) == MaintenanceRefusal(reason="evidence_unavailable")
            forge.mergeable = True
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
            restarted = MaintenanceObserver(
                observer.sessions, observer.settings, observer.client, observer.valkey
            )
            receipt = await restarted(observation)
            assert isinstance(receipt, MaintenanceRepair) and receipt.status == "replayed"
            assert len(forge.http_requests) == reads
            assert await session.scalar(select(func.count()).select_from(ExecutionRequest)) == 2
            forge.seed_commit(observation.base_sha, parents=[], message="original base")
            advanced = forge.push_head("main")
            settings, client = observer.settings, observer.client
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

    support.with_session(scenario)


@pytest.mark.parametrize(
    ("marker", "issue_overrides", "expected"),
    [
        ("curie", None, "work_item_cancelled"),
        ("curie", {"state": "closed"}, "work_item_cancelled"),
        ("curie", {"pull_request": {}}, "work_item_cancelled"),
        ("curie", {"labels": []}, "work_item_cancelled"),
        ("curie", {}, "admitted"),
        (None, None, "evidence_unavailable"),
        (None, {"number": 2574}, "evidence_unavailable"),
        (None, {"state": "closed"}, "work_item_cancelled"),
        (None, {"pull_request": {}}, "evidence_unavailable"),
    ],
)
def test_observer_requires_a_current_open_tracker_issue(
    clean_db: None,
    maintenance_settings: Settings,
    marker: str | None,
    issue_overrides: dict[str, Any] | None,
    expected: RefusalReason | Literal["admitted"],
) -> None:
    async def scenario(session: AsyncSession) -> None:
        observation = await seed_completed(session, trigger_label=marker)
        async with _observer(session, observation) as (observer, forge):
            if issue_overrides is None:
                del forge.issues[2573]
            else:
                if marker is not None:
                    forge.issues[2573].update(labels=[{"name": marker}])
                forge.issues[2573].update(issue_overrides)
            forge.report_check(observation.head_sha, "unit", "failure")
            forge.check_runs[observation.head_sha]["unit"]["started_at"] = (
                await database_now(session)
            ).isoformat()
            result = await observer(observation)
            if expected == "admitted":
                assert isinstance(result, MaintenanceRepair)
                assert result.status == "admitted"
                assert await session.scalar(select(func.count()).select_from(ExecutionRequest)) == 2
            else:
                assert result == MaintenanceRefusal(reason=expected)
                assert await session.scalar(select(func.count()).select_from(ExecutionRequest)) == 1
            assert forge.writes == 0

    support.with_session(scenario)
