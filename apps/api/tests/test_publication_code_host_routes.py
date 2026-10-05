"""The worker's pull request, branch and commit calls, answered by the API (#3831).

ADR 0197 "Two ports" item 6: the worker names one stored publication and the
API acts through its code host. A publication with an open pull request is
seeded through the real admission, publication and lineage routes; only the
code host is a fake, so these tests pin the routes' authority and contract.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from curie_api.forges.errors import Unavailable
from curie_api.forges.types import (
    Commit,
    PullRequest,
    PullRequestRef,
    PullRequestState,
    RepositoryRef,
)
from curie_api.routers import publication_code_host
from fastapi.testclient import TestClient
from sqlalchemy import text

from apps.api.tests.test_workitem_outcomes import (  # noqa: F401 - the stack fixture
    HEAD_SHA,
    PR_NUMBER,
    PR_URL,
    REPO,
    WORKER_HEADERS,
    _agent,
    _completed,
    _open_pr,
    _publish,
    _resolve,
    stack,
    with_session,
)

ROUTES = (
    ("GET", "pull-request?pr_number=1", None),
    ("GET", "branch-head", None),
    (
        "POST",
        "revision-commit",
        {"commit_sha": "b" * 40, "revision_id": str(uuid.uuid4()), "expected_parent": "a" * 40},
    ),
    ("POST", "pull-request", {"expected_head_sha": "b" * 40}),
)


class _CodeHost:
    def __init__(self) -> None:
        self.fail: Exception | None = None
        self.branch: str | None = None
        self.head: str | None = HEAD_SHA
        self.existing: PullRequest | None = None
        self.opened: list[dict[str, Any]] = []
        self.commit = Commit(sha=HEAD_SHA, parents=("0" * 40,), message="unmarked")

    def _pull(self, repository: RepositoryRef, **overrides: Any) -> PullRequest:
        values: dict[str, Any] = {
            "ref": PullRequestRef(repository, str(PR_NUMBER)),
            "head_sha": HEAD_SHA,
            "head_ref": self.branch or "",
            "base_ref": "main",
            "state": PullRequestState.OPEN,
            "url": PR_URL,
        }
        values.update(overrides)
        return PullRequest(**values)

    def _maybe_fail(self) -> None:
        if self.fail is not None:
            raise self.fail

    async def read_pull_request(self, pull_request: PullRequestRef) -> PullRequest:
        self._maybe_fail()
        return self._pull(pull_request.repository, state=PullRequestState.MERGED)

    async def branch_head(self, repository: RepositoryRef, branch: str) -> str | None:
        self._maybe_fail()
        assert branch == self.branch
        return self.head

    async def read_commit(self, repository: RepositoryRef, sha: str) -> Commit:
        self._maybe_fail()
        return self.commit

    async def resolve_repository(self, project_id: str) -> RepositoryRef:
        assert project_id == f"path:{REPO}"
        return RepositoryRef("github", "github.com", "101", REPO, default_branch="main")

    async def find_pull_request(
        self, repository: RepositoryRef, *, head_ref: str
    ) -> PullRequest | None:
        self._maybe_fail()
        return self.existing

    async def open_pull_request(self, repository: RepositoryRef, **fields: Any) -> PullRequest:
        self.opened.append(fields)
        return self._pull(
            repository,
            title=fields["title"],
            body=fields["body"],
            base_ref=fields["base_ref"],
            draft=fields["draft"],
        )


Seeded = tuple[TestClient, dict[str, Any], _CodeHost]


def _seed(
    client: TestClient,
    auth_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    *,
    open_pr: bool,
) -> Seeded:
    agent = _agent(client, auth_headers)
    _completed(client, agent)
    publication = _publish(client, agent["deployment_id"])
    _resolve(client, auth_headers, publication["approval_id"])
    if open_pr:
        _open_pr(client, publication["id"])
    code_host = _CodeHost()
    code_host.branch = with_session(
        lambda session: session.scalar(
            text("SELECT branch FROM curie.thread_publication_lineages WHERE id = :id"),
            {"id": uuid.UUID(publication["lineage_id"])},
        )
    )
    monkeypatch.setattr(publication_code_host, "code_host_for", lambda _s, _c: code_host)
    return client, publication, code_host


@pytest.fixture
def seeded(
    stack: TestClient,  # noqa: F811
    auth_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> Seeded:
    """An approved publication whose pull request is open on its lineage."""

    return _seed(stack, auth_headers, monkeypatch, open_pr=True)


@pytest.fixture
def approved(
    stack: TestClient,  # noqa: F811
    auth_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> Seeded:
    """An approved publication whose pull request was never recorded: the recovery case."""

    return _seed(stack, auth_headers, monkeypatch, open_pr=False)


def _url(publication_id: object, suffix: str) -> str:
    return f"/v1/internal/publications/{publication_id}/{suffix}"


@pytest.mark.parametrize(("method", "suffix", "body"), ROUTES)
def test_every_route_requires_the_internal_worker_token(
    stack: TestClient,  # noqa: F811
    method: str,
    suffix: str,
    body: dict[str, str] | None,
) -> None:
    for headers in ({}, {"X-Curie-Worker-Token": "wrong-token"}):
        response = stack.request(method, _url(uuid.uuid4(), suffix), json=body, headers=headers)
        assert response.status_code == 401, response.text


@pytest.mark.parametrize(("method", "suffix", "body"), ROUTES)
def test_an_unknown_publication_is_not_found(
    stack: TestClient,  # noqa: F811
    method: str,
    suffix: str,
    body: dict[str, str] | None,
) -> None:
    response = stack.request(method, _url(uuid.uuid4(), suffix), json=body, headers=WORKER_HEADERS)

    assert response.status_code == 404, response.text
    assert response.headers["Cache-Control"] == "no-store"


def test_the_stored_pull_request_is_read_only_by_its_stored_number(
    seeded: Seeded,
) -> None:
    client, publication, _code_host = seeded

    stale = client.get(
        _url(publication["id"], "pull-request"),
        params={"pr_number": PR_NUMBER + 1},
        headers=WORKER_HEADERS,
    )
    read = client.get(
        _url(publication["id"], "pull-request"),
        params={"pr_number": PR_NUMBER},
        headers=WORKER_HEADERS,
    )

    assert stale.status_code == 409
    assert stale.json()["detail"]["code"] == "publication.lineage_stale"
    assert read.status_code == 200, read.text
    assert read.json() == {
        "number": PR_NUMBER,
        "url": PR_URL,
        "state": "merged",
        "head_sha": HEAD_SHA,
        "head_ref": _code_host.branch,
    }


def test_the_branch_head_is_read_for_the_lineage_branch(
    seeded: Seeded,
) -> None:
    client, publication, code_host = seeded
    code_host.head = None

    response = client.get(_url(publication["id"], "branch-head"), headers=WORKER_HEADERS)

    assert response.status_code == 200, response.text
    assert response.json() == {"head_sha": None}


def test_a_revision_is_verified_only_for_this_publication_and_its_marker(
    seeded: Seeded,
) -> None:
    client, publication, _code_host = seeded

    other = client.post(
        _url(publication["id"], "revision-commit"),
        json={
            "commit_sha": HEAD_SHA,
            "revision_id": str(uuid.uuid4()),
            "expected_parent": "0" * 40,
        },
        headers=WORKER_HEADERS,
    )

    assert other.status_code == 409
    assert other.json()["detail"]["code"] == "publication.revision_mismatch"


def test_a_code_host_outage_is_a_payload_free_503(
    seeded: Seeded,
) -> None:
    client, publication, code_host = seeded
    code_host.fail = Unavailable("github_rate_limited")

    response = client.get(_url(publication["id"], "branch-head"), headers=WORKER_HEADERS)

    assert response.status_code == 503
    assert response.json()["detail"] == {
        "code": "publication.code_host_unavailable",
        "message": "the code host answered github_rate_limited",
    }


def test_recovery_opens_the_stored_contract_once_and_adopts_it(
    approved: Seeded,
) -> None:
    client, publication, code_host = approved

    response = client.post(
        _url(publication["id"], "pull-request"),
        json={"expected_head_sha": HEAD_SHA},
        headers=WORKER_HEADERS,
    )

    assert response.status_code == 200, response.text
    assert response.json()["number"] == PR_NUMBER
    [opened] = code_host.opened
    assert opened["head_ref"] == code_host.branch
    assert opened["base_ref"] == "main"


def test_recovery_of_a_moved_branch_is_refused(
    approved: Seeded,
) -> None:
    client, publication, code_host = approved
    code_host.head = "c" * 40

    response = client.post(
        _url(publication["id"], "pull-request"),
        json={"expected_head_sha": HEAD_SHA},
        headers=WORKER_HEADERS,
    )

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "publication.branch_moved"
    assert code_host.opened == []


def test_recovery_of_an_absent_branch_is_no_content(
    approved: Seeded,
) -> None:
    client, publication, code_host = approved
    code_host.head = None

    response = client.post(
        _url(publication["id"], "pull-request"),
        json={"expected_head_sha": HEAD_SHA},
        headers=WORKER_HEADERS,
    )

    assert response.status_code == 204
    assert code_host.opened == []
