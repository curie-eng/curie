"""The forge-neutral lineage reconciler records merged and closed pull requests (#3831).

Each case seeds a factory WorkItem whose publication opened a pull request,
through the same admission, publication and lineage routes the outcome tests
use, then runs one reconciler pass against a code host fake that answers
`CodeHost.read_pull_request`. The operator read surface is the observation.
"""

from __future__ import annotations

import asyncio
import uuid
from types import SimpleNamespace
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_api.forges.errors import ForgeError, Unavailable
from curie_api.forges.types import PullRequest, PullRequestRef, PullRequestState, RepositoryRef
from curie_api.lineage_reconciler import LineageReconciler, PassResult
from curie_api.models import ThreadPublicationLineage
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from apps.api.tests.test_workitem_outcomes import (  # noqa: F401 - the stack fixture
    PR_NUMBER,
    PR_URL,
    REPO,
    _agent,
    _complete,
    _completed,
    _detail,
    _open_pr,
    _publish,
    _resolve,
    stack,
)


class _CodeHost:
    """Answers `read_pull_request` with one scripted state or failure."""

    def __init__(self, branch: str, answer: PullRequestState | ForgeError) -> None:
        self.branch = branch
        self.answer = answer
        self.reads: list[PullRequestRef] = []

    async def read_pull_request(self, pull_request: PullRequestRef) -> PullRequest:
        self.reads.append(pull_request)
        if isinstance(self.answer, ForgeError):
            raise self.answer
        return PullRequest(
            ref=pull_request,
            head_sha="f" * 40,
            head_ref=self.branch,
            base_ref="main",
            state=self.answer,
            url=PR_URL,
        )


@pytest.fixture
def client(stack: TestClient) -> TestClient:  # noqa: F811
    """The outcome suite's API stack: real Postgres, Valkey and routes."""

    return stack


def _seed_open_pr(client: TestClient, auth_headers: dict[str, str]) -> tuple[Any, uuid.UUID]:
    agent = _agent(client, auth_headers)
    seeded = _completed(client, agent)
    publication = _publish(client, agent["deployment_id"])
    _resolve(client, auth_headers, publication["approval_id"])
    _open_pr(client, publication["id"])
    _complete(seeded)
    return seeded, uuid.UUID(publication["lineage_id"])


def _lineage(lineage_id: uuid.UUID) -> SimpleNamespace:
    async def read() -> SimpleNamespace:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with async_sessionmaker(engine, expire_on_commit=False)() as session:
                lineage = await session.get(ThreadPublicationLineage, lineage_id)
                assert lineage is not None
                return SimpleNamespace(
                    status=lineage.status,
                    version=lineage.version,
                    branch=lineage.branch,
                    head_sha=lineage.head_sha,
                )
        finally:
            await engine.dispose()

    return asyncio.run(read())


def _passes(code_host: _CodeHost, count: int = 1) -> list[PassResult]:
    async def run() -> list[PassResult]:
        engine = create_async_engine(get_settings().database_url)
        try:
            reconciler = LineageReconciler(
                async_sessionmaker(engine, expire_on_commit=False),
                get_settings(),
                # One code host per stored (kind, host) pair (#4358 plan S2.28).
                lambda _repository: code_host,  # type: ignore[arg-type, return-value]
                interval_seconds=60,
            )
            return [await reconciler.run_once() for _ in range(count)]
        finally:
            await engine.dispose()

    return asyncio.run(run())


def test_an_open_pull_request_leaves_the_lineage_open(
    client: TestClient, auth_headers: dict[str, str]
) -> None:
    seeded, lineage_id = _seed_open_pr(client, auth_headers)
    before = _lineage(lineage_id)
    code_host = _CodeHost(before.branch, PullRequestState.OPEN)

    [result] = _passes(code_host)

    assert (result.read, result.merged, result.closed, result.skipped) == (1, 0, 0, 0)
    assert [(ref.repository.path, ref.number) for ref in code_host.reads] == [
        (REPO, str(PR_NUMBER))
    ]
    after = _lineage(lineage_id)
    assert (after.status, after.version) == ("open", before.version)
    assert _detail(client, auth_headers, seeded.work_item_id)["pr"]["status"] == "open"


@pytest.mark.parametrize(
    ("state", "recorded"),
    [(PullRequestState.MERGED, "merged"), (PullRequestState.CLOSED, "closed")],
)
def test_a_terminal_pull_request_is_recorded_on_the_lineage(
    client: TestClient,
    auth_headers: dict[str, str],
    state: PullRequestState,
    recorded: str,
) -> None:
    seeded, lineage_id = _seed_open_pr(client, auth_headers)
    before = _lineage(lineage_id)

    [result] = _passes(_CodeHost(before.branch, state))

    assert (result.merged, result.closed) == ((1, 0) if recorded == "merged" else (0, 1))
    after = _lineage(lineage_id)
    assert after.status == recorded
    assert after.version == before.version + 1
    assert after.head_sha == before.head_sha
    assert _detail(client, auth_headers, seeded.work_item_id)["pr"] == {
        "number": PR_NUMBER,
        "url": PR_URL,
        "status": recorded,
    }


@pytest.mark.parametrize("reason", ["timeout", "github_rate_limited"])
def test_an_unavailable_code_host_leaves_the_lineage_open(
    client: TestClient, auth_headers: dict[str, str], reason: str
) -> None:
    _seeded, lineage_id = _seed_open_pr(client, auth_headers)
    before = _lineage(lineage_id)

    [result] = _passes(_CodeHost(before.branch, Unavailable(reason)))

    assert (result.read, result.skipped) == (0, 1)
    assert result.rate_limited is (reason == "github_rate_limited")
    after = _lineage(lineage_id)
    assert (after.status, after.version) == ("open", before.version)


def test_a_pull_request_on_another_branch_is_not_recorded(
    client: TestClient, auth_headers: dict[str, str]
) -> None:
    _seeded, lineage_id = _seed_open_pr(client, auth_headers)

    [result] = _passes(_CodeHost("someone/else", PullRequestState.MERGED))

    assert (result.merged, result.skipped) == (0, 1)
    assert _lineage(lineage_id).status == "open"


def test_a_second_pass_is_idempotent(client: TestClient, auth_headers: dict[str, str]) -> None:
    _seeded, lineage_id = _seed_open_pr(client, auth_headers)
    before = _lineage(lineage_id)
    code_host = _CodeHost(before.branch, PullRequestState.MERGED)

    first, second = _passes(code_host, count=2)

    assert first.merged == 1
    assert (second.read, second.merged, second.closed, second.skipped) == (0, 0, 0, 0)
    assert len(code_host.reads) == 1
    after = _lineage(lineage_id)
    assert (after.status, after.version) == ("merged", before.version + 1)


def _rewrite_stored_identity(statement: str, params: dict[str, Any]) -> None:
    """Rewrite a column the work item identity trigger guards, as a row an
    earlier admission wrote that way: triggers are off for this one transaction."""

    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("SET LOCAL session_replication_role = replica"))
                await conn.execute(text(statement), params)
        finally:
            await engine.dispose()

    asyncio.run(run())


# --- Each lineage reads its own stored code host (#4358 plan S2.28, r2 finding 1) --


def _move_to_host(lineage_id: uuid.UUID, host: str) -> None:
    """Record the lineage's repository on another code host entry, as a WorkItem
    admitted on that entry stores it."""

    _rewrite_stored_identity(
        "UPDATE curie.thread_publication_lineages SET code_host_host = :host "
        "WHERE id = :id AND code_host_kind IS NOT NULL",
        {"host": host, "id": lineage_id},
    )
    _rewrite_stored_identity(
        "UPDATE curie.work_items SET code_host_host = :host WHERE publication_lineage_id = :id",
        {"host": host, "id": lineage_id},
    )


@pytest.mark.parametrize(
    ("stored_host", "expected"),
    [(None, "merged"), ("ghe.example", "closed"), ("unconfigured.example", "open")],
    ids=["install_github", "another_entry", "unconfigured"],
)
def test_each_lineage_reads_its_own_code_host_entry(
    client: TestClient,
    auth_headers: dict[str, str],
    stored_host: str | None,
    expected: str,
) -> None:
    _seeded, lineage_id = _seed_open_pr(client, auth_headers)
    if stored_host is not None:
        _move_to_host(lineage_id, stored_host)
    branch = _lineage(lineage_id).branch
    # Each code host entry answers differently, so the recorded state names
    # the entry that was read.
    github = _CodeHost(branch, PullRequestState.MERGED)
    ghe = _CodeHost(branch, PullRequestState.CLOSED)
    asked: list[RepositoryRef] = []

    def code_host_for(repository: RepositoryRef) -> _CodeHost:
        asked.append(repository)
        if repository.host == "ghe.example":
            return ghe
        if repository.host == "unconfigured.example":
            raise Unavailable("code_host_unconfigured")
        return github

    async def run() -> PassResult:
        engine = create_async_engine(get_settings().database_url)
        try:
            reconciler = LineageReconciler(
                async_sessionmaker(engine, expire_on_commit=False),
                get_settings(),
                code_host_for,  # type: ignore[arg-type]
                interval_seconds=60,
            )
            return await reconciler.run_once()
        finally:
            await engine.dispose()

    result = asyncio.run(run())

    assert [repository.path for repository in asked] == [REPO]
    if stored_host is not None:
        assert [repository.host for repository in asked] == [stored_host]
    assert _lineage(lineage_id).status == expected
    if expected == "open":
        # An unconfigured entry is skipped like a rate limit, never read elsewhere.
        assert (result.read, result.skipped, result.rate_limited) == (0, 1, False)
        assert github.reads == [] and ghe.reads == []
    elif expected == "closed":
        assert github.reads == [] and len(ghe.reads) == 1
    else:
        assert ghe.reads == [] and len(github.reads) == 1
