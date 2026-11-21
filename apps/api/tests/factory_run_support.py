"""A labelled issue admitted and run as the worker runs it, for the factory route tests.

The issue is labelled through the signed GitHub webhook against create_app(),
its request acquired and started through `workitem_dispatch.acquire` and
`start`, and the issue read capability minted at the internal route the
worker calls at sandbox boot. Only the WorkItem's tracker is substituted: the
in-memory adapter, through `completion_contract.work_item_tracker`.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import pytest
from curie_api import completion_contract
from curie_api.config import get_settings
from curie_api.forges.memory import InMemoryTracker
from curie_api.forges.types import TrackerIssueRef
from curie_api.workitem_dispatch import DispatchConflict, acquire, start
from forge_fakes import factory_wake
from forge_fakes.github import LABEL, REPO_ID, GitHubAPI, _issue_event, _post
from forge_fakes.github_comments import _rows
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

WORKER_HEADERS = {"X-Curie-Worker-Token": "factory-terminus-worker"}
MINT_URL = "/v1/internal/work-items/issue-read/context"
SOURCE_TITLE = "Export drops a row"
SOURCE_BODY = "Reported symptom."


def _run[T](work: Callable[[async_sessionmaker[AsyncSession]], Awaitable[T]]) -> T:
    """Run service code on this thread's own loop and engine."""

    async def go() -> T:
        engine = create_async_engine(get_settings().database_url)
        try:
            return await work(async_sessionmaker(engine, expire_on_commit=False))
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _exec(statement: str, params: dict[str, Any] | None = None) -> None:
    async def work(maker: async_sessionmaker[AsyncSession]) -> None:
        async with maker() as session:
            await session.execute(text(statement), params or {})
            await session.commit()

    _run(work)


def _label(client: Any, github: GitHubAPI, number: int) -> None:
    github.issue_number = number
    github.advance_label_event(number)
    response = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "factory_admitted"


@dataclass
class _Run:
    client: Any
    request_id: uuid.UUID
    work_item_id: uuid.UUID
    tracker: InMemoryTracker
    issue: TrackerIssueRef
    capability: str
    context: Any
    epoch: int = 0


def _admit_run(
    admitted: Any,
    monkeypatch: pytest.MonkeyPatch,
    number: int,
    *,
    labels: tuple[str, ...] = (),
) -> _Run:
    """A labelled issue admitted through the webhook and acquired by a worker,
    with the capability the worker mints for its sandbox. Not started yet."""

    client, github, _sink = admitted
    _label(client, github, number)
    (row,) = _rows(
        "SELECT r.id AS request_id, w.id AS work_item_id, w.tracker_kind, w.tracker_host, "
        "w.tracker_scope_id, w.tracker_issue_id FROM curie.execution_requests r "
        "JOIN curie.work_items w ON w.id = r.work_item_id "
        "WHERE w.tracker_scope_id = :scope AND w.tracker_issue_id = :number",
        {"scope": str(REPO_ID), "number": str(number)},
    )
    tracker = InMemoryTracker(
        kind=row["tracker_kind"], host=row["tracker_host"], scope_id=row["tracker_scope_id"]
    )
    issue = tracker.seed_issue(
        SOURCE_TITLE, SOURCE_BODY, issue_id=row["tracker_issue_id"], labels=labels
    )
    monkeypatch.setattr(
        completion_contract,
        "work_item_tracker",
        lambda _settings, _client, _work_item: tracker,
    )
    request_id = row["request_id"]

    async def go() -> Any:
        async with client.app.state.sessionmaker() as session:
            acquired = await acquire(
                session,
                request_id,
                owner="factory-owner",
                generation=1,
                wake=await factory_wake(session, request_id),
                context=None,
                client=client.app.state.http_client,
            )
            assert not isinstance(acquired, DispatchConflict), acquired
            return acquired.context

    context = client.portal.call(go)
    minted = client.post(
        MINT_URL,
        headers=WORKER_HEADERS,
        json={"execution_request_id": str(request_id), "publication_recovery": False},
    )
    assert minted.status_code == 200, minted.text
    return _Run(
        client=client,
        request_id=request_id,
        work_item_id=row["work_item_id"],
        tracker=tracker,
        issue=issue,
        capability=minted.json()["capability"],
        context=context,
    )


def _begin(job: _Run) -> None:
    async def go() -> int:
        async with job.client.app.state.sessionmaker() as session:
            started = await start(
                session,
                job.request_id,
                owner="factory-owner",
                generation=1,
                claim_name=f"claim-{job.request_id.hex[:12]}",
                sandbox_name=f"sbx-{job.request_id.hex[:12]}",
                context=job.context,
                delivered_limits={
                    "memory_bytes": None,
                    "ephemeral_storage_bytes": None,
                    "scratch_bytes": {},
                    "max_turns": None,
                },
            )
            assert not isinstance(started, DispatchConflict), started
            return int(started.runtime_epoch)

    job.epoch = int(job.client.portal.call(go))


def _finish(job: _Run, outcome: str, cause: str) -> Any:
    from curie_api.workitem_dispatch import finish

    async def go() -> Any:
        async with job.client.app.state.sessionmaker() as session:
            return await finish(
                session,
                job.request_id,
                runtime_epoch=job.epoch,
                outcome=outcome,  # type: ignore[arg-type]
                cause=cause,
                detail=None,
                ci_fix_round=None,
                retry_safety="uncertain",
                retry_condition=None,
                exhausted_volume=None,
            )

    return job.client.portal.call(go)
