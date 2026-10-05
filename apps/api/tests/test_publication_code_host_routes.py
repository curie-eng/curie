"""The worker's pull request, branch and commit calls, answered by the API (#3831).

ADR 0197 "Two ports" item 6: the worker names one stored publication and the
API acts through its code host. A publication with an open pull request is
seeded through the real admission, publication and lineage routes; only the
code host is a fake, so these tests pin the routes' authority and contract.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime
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
    _execute,
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
    ("POST", "pull-request/metadata", None),
)
UPDATED_AT = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)


class _CodeHost:
    def __init__(self) -> None:
        self.fail: Exception | None = None
        self.branch: str | None = None
        self.head: str | None = HEAD_SHA
        self.existing: PullRequest | None = None
        self.opened: list[dict[str, Any]] = []
        self.commit = Commit(sha=HEAD_SHA, parents=("0" * 40,), message="unmarked")
        self.read_state = PullRequestState.MERGED
        self.title = "Old title"
        self.body = "Old body"
        self.updated: list[dict[str, Any]] = []

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
        return self._pull(
            pull_request.repository, state=self.read_state, title=self.title, body=self.body
        )

    async def update_pull_request(
        self, pull_request: PullRequestRef, *, title: str | None, body: str | None
    ) -> PullRequest:
        self.updated.append({"title": title, "body": body})
        self.title = title if title is not None else self.title
        self.body = body if body is not None else self.body
        return self._pull(
            pull_request.repository, title=self.title, body=self.body, updated_at=UPDATED_AT
        )

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
        "updated_at": None,
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


def _metadata_only(
    publication: dict[str, Any], *, observed_title: str, observed_body: str
) -> dict[str, str]:
    """Turn the seeded revision into a running metadata-only one on the open pull request."""

    _execute(
        "UPDATE curie.publications SET status = 'running', patch_bytes = ''::bytea, "
        "changed_paths = '[]'::jsonb, base_sha = :head, "
        "observed_title_sha256 = :title, observed_body_sha256 = :body "
        "WHERE id = :id",
        {
            "id": uuid.UUID(publication["id"]),
            "head": HEAD_SHA,
            "title": hashlib.sha256(observed_title.encode()).hexdigest(),
            "body": hashlib.sha256(observed_body.encode()).hexdigest(),
        },
    )

    async def proposal(session: Any) -> dict[str, str]:
        row = (
            (
                await session.execute(
                    text("SELECT title, body FROM curie.publications WHERE id = :id"),
                    {"id": uuid.UUID(publication["id"])},
                )
            )
            .mappings()
            .one()
        )
        return dict(row)

    return with_session(proposal)


def test_a_publication_with_a_patch_is_pushed_not_applied_as_metadata(
    seeded: Seeded,
) -> None:
    client, publication, code_host = seeded
    # The seeded revision is the next one, still running, on the open lineage.
    _execute(
        "UPDATE curie.publications SET status = 'running' WHERE id = :id",
        {"id": uuid.UUID(publication["id"])},
    )

    response = client.post(_url(publication["id"], "pull-request/metadata"), headers=WORKER_HEADERS)

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "publication.not_metadata_only"
    assert code_host.updated == []


def test_a_metadata_only_revision_updates_the_stored_pull_request_through_the_code_host(
    seeded: Seeded,
) -> None:
    client, publication, code_host = seeded
    code_host.read_state = PullRequestState.OPEN
    proposal = _metadata_only(publication, observed_title="Old title", observed_body="Old body")

    response = client.post(_url(publication["id"], "pull-request/metadata"), headers=WORKER_HEADERS)

    assert response.status_code == 200, response.text
    assert response.json()["state"] == "open"
    assert response.json()["head_sha"] == HEAD_SHA
    assert datetime.fromisoformat(response.json()["updated_at"]) == UPDATED_AT
    assert code_host.updated == [{"title": proposal["title"], "body": proposal["body"]}]


def test_a_metadata_update_refuses_an_edit_made_after_approval(
    seeded: Seeded,
) -> None:
    client, publication, code_host = seeded
    code_host.read_state = PullRequestState.OPEN
    _metadata_only(publication, observed_title="Old title", observed_body="Another body")

    response = client.post(_url(publication["id"], "pull-request/metadata"), headers=WORKER_HEADERS)

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "publication.metadata_changed"
    assert code_host.updated == []


def test_a_merged_pull_request_is_answered_unchanged_for_the_worker_to_record(
    seeded: Seeded,
) -> None:
    client, publication, code_host = seeded
    _metadata_only(publication, observed_title="Old title", observed_body="Old body")

    response = client.post(_url(publication["id"], "pull-request/metadata"), headers=WORKER_HEADERS)

    assert response.status_code == 200, response.text
    assert response.json()["state"] == "merged"
    assert code_host.updated == []


def test_recovery_opens_against_the_work_items_recorded_base(
    approved: Seeded,
) -> None:
    """The base the publication Job used to read from BASE_REF is the API's now (#3095)."""

    client, publication, code_host = approved
    _execute(
        "UPDATE curie.work_items SET base_branch = 'next', base_commit = :commit, "
        "base_source = 'label' WHERE id = ("
        "SELECT r.work_item_id FROM curie.execution_requests r "
        "JOIN curie.publications p ON p.execution_request_id = r.id WHERE p.id = :id)",
        {"id": uuid.UUID(publication["id"]), "commit": "e" * 40},
    )

    response = client.post(
        _url(publication["id"], "pull-request"),
        json={"expected_head_sha": HEAD_SHA},
        headers=WORKER_HEADERS,
    )

    assert response.status_code == 200, response.text
    [opened] = code_host.opened
    assert opened["base_ref"] == "next"
