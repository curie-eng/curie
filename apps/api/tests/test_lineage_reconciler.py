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
from curie_api.forges.types import PullRequest, PullRequestRef, PullRequestState
from curie_api.lineage_reconciler import LineageReconciler, PassResult
from curie_api.models import ThreadPublicationLineage
from fastapi.testclient import TestClient
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
                lambda: code_host,  # type: ignore[arg-type, return-value]
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
