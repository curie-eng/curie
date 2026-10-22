"""The worker's pull request, branch and commit calls, answered by the API (#3831).

ADR 0197 "Two ports" item 6: the worker names one stored publication and the
API acts through its code host. A publication with an open pull request is
seeded through the real admission, publication and lineage routes; only the
code host is a fake, so these tests pin the routes' authority and contract.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from curie_api.forges.errors import NotFound, Unavailable
from curie_api.forges.types import (
    Commit,
    FactoryPublication,
    PullRequest,
    PullRequestRef,
    PullRequestState,
    RepositoryRef,
)
from curie_api.routers import publication_code_host
from fastapi.testclient import TestClient
from sqlalchemy import text

from apps.api.tests.test_workitem_outcomes import (  # noqa: F401 - the stack fixture
    BASE_SHA,
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
        self.commits: dict[str, Commit] = {}
        self.base_head: str | None = BASE_SHA
        self.default_branch = "main"
        self.reads: list[str] = []
        self.branch_reads: list[str] = []
        self.read_delay = 0.0
        self.on_read: Any = None
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
        self.branch_reads.append(branch)
        return self.head if branch == self.branch else self.base_head

    async def read_commit(self, repository: RepositoryRef, sha: str) -> Commit:
        self._maybe_fail()
        self.reads.append(sha)
        if self.on_read is not None:
            callback, self.on_read = self.on_read, None
            await callback()
        if self.read_delay and sha != HEAD_SHA:
            await asyncio.sleep(self.read_delay)
        if sha not in self.commits:
            raise NotFound("commit_not_found")
        return self.commits[sha]

    async def resolve_repository(self, project_id: str) -> RepositoryRef:
        assert project_id == f"path:{REPO}"
        return RepositoryRef(
            "github", "github.com", "101", REPO, default_branch=self.default_branch
        )

    async def find_pull_request(
        self, repository: RepositoryRef, *, head_ref: str
    ) -> PullRequest | None:
        self._maybe_fail()
        return self.existing

    async def list_factory_publications(
        self, repository: RepositoryRef
    ) -> tuple[FactoryPublication, ...]:
        self._maybe_fail()
        return ()

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
    trigger_label: str | None = None,
) -> Seeded:
    agent = _agent(client, auth_headers)
    _completed(client, agent, trigger_label=trigger_label)
    publication = _publish(client, agent["deployment_id"])
    _resolve(client, auth_headers, publication["approval_id"])
    if open_pr:
        _open_pr(client, publication["id"])
    code_host = _CodeHost()
    code_host.commits = {
        BASE_SHA: Commit(sha=BASE_SHA, parents=(), message="Original base"),
        HEAD_SHA: Commit(
            sha=HEAD_SHA,
            parents=(BASE_SHA,),
            message=f"Publish\n\nCurie-Revision: {publication['id']}\n",
        ),
    }
    code_host.branch = with_session(
        lambda session: session.scalar(
            text("SELECT branch FROM curie.thread_publication_lineages WHERE id = :id"),
            {"id": uuid.UUID(publication["lineage_id"])},
        )
    )
    monkeypatch.setattr(publication_code_host, "publishing_code_host", lambda _s, _c, _r: code_host)
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


@pytest.fixture
def factory_approved(
    stack: TestClient,  # noqa: F811
    auth_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> Seeded:
    return _seed(stack, auth_headers, monkeypatch, open_pr=False, trigger_label="curie:implement")


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
    assert response.json() == {"head_sha": None, "base_branch": "main"}


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


def _sha(number: int) -> str:
    return f"{number:040x}"


def _graph_commit(code_host: _CodeHost, sha: str, *parents: str) -> str:
    code_host.commits[sha] = Commit(sha=sha, parents=parents, message="Base history")
    return sha


def _published_parent(publication: dict[str, Any], code_host: _CodeHost, parent: str) -> None:
    code_host.commits[HEAD_SHA] = Commit(
        sha=HEAD_SHA,
        parents=(parent,),
        message=f"Approved patch\n\nCurie-Revision: {publication['id']}\n",
    )


def _verify(client: TestClient, publication: dict[str, Any]) -> Any:
    return client.post(
        _url(publication["id"], "revision-commit"),
        json={
            "commit_sha": HEAD_SHA,
            "revision_id": publication["id"],
            "expected_parent": BASE_SHA,
        },
        headers=WORKER_HEADERS,
    )


def _stored_state(publication: dict[str, Any]) -> dict[str, Any]:
    async def read(session: Any) -> dict[str, Any]:
        result = await session.execute(
            text(
                "SELECT p.base_sha AS original_base, p.expected_prior_head, p.status, "
                "p.version AS publication_version, p.patch_bytes, p.lease_owner, "
                "l.base_sha, l.head_sha, l.pr_number, l.version AS lineage_version "
                "FROM curie.publications p JOIN curie.thread_publication_lineages l "
                "ON l.id = p.lineage_id WHERE p.id = :id"
            ),
            {"id": uuid.UUID(publication["id"])},
        )
        return dict(result.mappings().one())

    return with_session(read)


@pytest.mark.parametrize("branch", ["main", "next", "release/stable"])
def test_branch_context_uses_the_admitted_branch(approved: Seeded, branch: str) -> None:
    client, publication, code_host = approved
    _execute(
        "UPDATE curie.work_items SET base_branch = :branch WHERE id = ("
        "SELECT r.work_item_id FROM curie.execution_requests r "
        "JOIN curie.publications p ON p.execution_request_id = r.id WHERE p.id = :id)",
        {"id": uuid.UUID(publication["id"]), "branch": branch},
    )
    code_host.head = None

    response = client.get(_url(publication["id"], "branch-head"), headers=WORKER_HEADERS)

    assert response.status_code == 200, response.text
    assert response.headers["Cache-Control"] == "no-store"
    assert response.json() == {"head_sha": None, "base_branch": branch}


def test_branch_context_without_execution_uses_repository_default(approved: Seeded) -> None:
    client, publication, code_host = approved
    _execute(
        "UPDATE curie.publications SET execution_request_id = NULL WHERE id = :id",
        {"id": uuid.UUID(publication["id"])},
    )
    code_host.default_branch = "trunk"

    response = client.get(_url(publication["id"], "branch-head"), headers=WORKER_HEADERS)

    assert response.status_code == 200, response.text
    assert response.json() == {"head_sha": HEAD_SHA, "base_branch": "trunk"}


def test_branch_context_preserves_verified_lineage_base(seeded: Seeded) -> None:
    client, publication, code_host = seeded
    _execute(
        "UPDATE curie.thread_publication_lineages SET base_ref = 'release/stable' WHERE id = :id",
        {"id": uuid.UUID(publication["lineage_id"])},
    )
    code_host.default_branch = "unrelated-default"

    response = client.get(_url(publication["id"], "branch-head"), headers=WORKER_HEADERS)

    assert response.status_code == 200, response.text
    assert response.json()["base_branch"] == "release/stable"


@pytest.mark.parametrize("branch", [None, "", "refs/heads/main", "bad..branch", "x" * 1025])
def test_missing_or_malformed_admitted_base_cannot_use_repository_default(
    factory_approved: Seeded, branch: str | None
) -> None:
    client, publication, code_host = factory_approved
    _execute(
        "UPDATE curie.work_items SET base_branch = :branch, "
        "base_source = CASE WHEN CAST(:branch AS text) IS NULL THEN NULL ELSE base_source END, "
        "base_commit = CASE WHEN CAST(:branch AS text) IS NULL THEN NULL ELSE base_commit END "
        "WHERE id = ("
        "SELECT r.work_item_id FROM curie.execution_requests r "
        "JOIN curie.publications p ON p.execution_request_id = r.id WHERE p.id = :id)",
        {"id": uuid.UUID(publication["id"]), "branch": branch},
    )
    before = _stored_state(publication)

    response = client.get(_url(publication["id"], "branch-head"), headers=WORKER_HEADERS)

    assert response.status_code in (409, 503), response.text
    assert code_host.opened == []
    assert _stored_state(publication) == before

    _execute(
        "UPDATE curie.work_items SET base_branch = 'main', base_source = 'default', "
        "base_commit = :base WHERE id = ("
        "SELECT r.work_item_id FROM curie.execution_requests r "
        "JOIN curie.publications p ON p.execution_request_id = r.id WHERE p.id = :id)",
        {"id": uuid.UUID(publication["id"]), "base": BASE_SHA},
    )
    healthy = client.get(_url(publication["id"], "branch-head"), headers=WORKER_HEADERS)
    assert healthy.status_code == 200, healthy.text
    assert healthy.json()["base_branch"] == "main"
    verified = _verify(client, publication)
    assert verified.status_code == 200, verified.text


@pytest.mark.parametrize("failure", ["empty", "malformed", "outage"])
def test_repository_default_base_failure_can_retry(approved: Seeded, failure: str) -> None:
    client, publication, code_host = approved
    _execute(
        "UPDATE curie.work_items SET base_branch = NULL, base_source = NULL, "
        "base_commit = NULL WHERE id = ("
        "SELECT r.work_item_id FROM curie.execution_requests r "
        "JOIN curie.publications p ON p.execution_request_id = r.id WHERE p.id = :id)",
        {"id": uuid.UUID(publication["id"])},
    )
    code_host.default_branch = {"empty": "", "malformed": "bad..branch", "outage": "trunk"}[failure]
    if failure == "outage":
        code_host.fail = Unavailable("github_unavailable")
    before = _stored_state(publication)

    refused = client.get(_url(publication["id"], "branch-head"), headers=WORKER_HEADERS)

    assert refused.status_code == 503, refused.text
    assert code_host.opened == []
    assert _stored_state(publication) == before
    code_host.default_branch, code_host.fail = "trunk", None
    healthy = client.get(_url(publication["id"], "branch-head"), headers=WORKER_HEADERS)
    assert healthy.status_code == 200, healthy.text
    assert healthy.json() == {"head_sha": HEAD_SHA, "base_branch": "trunk"}
    verified = _verify(client, publication)
    assert verified.status_code == 200, verified.text


@pytest.mark.parametrize("missing", ["execution-request", "work-item"])
def test_missing_execution_relation_cannot_use_repository_default(
    approved: Seeded, missing: str
) -> None:
    client, publication, code_host = approved
    original = with_session(
        lambda session: session.scalar(
            text("SELECT execution_request_id FROM curie.publications WHERE id = :id"),
            {"id": uuid.UUID(publication["id"])},
        )
    )
    assert original is not None
    query = (
        "UPDATE curie.publications SET execution_request_id = :missing WHERE id = :id"
        if missing == "execution-request"
        else "UPDATE curie.execution_requests SET work_item_id = :missing WHERE id = :request"
    )
    work_item_id = with_session(
        lambda session: session.scalar(
            text("SELECT work_item_id FROM curie.execution_requests WHERE id = :id"),
            {"id": original},
        )
    )

    async def break_relation(session: Any) -> None:
        # Inject a dangling persisted relation, without replacing the reader (#4943).
        await session.execute(text("SET LOCAL session_replication_role = replica"))
        await session.execute(
            text(query),
            {"id": uuid.UUID(publication["id"]), "request": original, "missing": uuid.uuid4()},
        )
        await session.commit()

    with_session(break_relation)
    before = _stored_state(publication)
    try:
        response = client.get(_url(publication["id"], "branch-head"), headers=WORKER_HEADERS)
        assert response.status_code in (409, 503), response.text
        assert code_host.opened == []
        assert _stored_state(publication) == before
    finally:
        if missing == "execution-request":
            _execute(
                "UPDATE curie.publications SET execution_request_id = :request WHERE id = :id",
                {"id": uuid.UUID(publication["id"]), "request": original},
            )
        else:
            _execute(
                "UPDATE curie.execution_requests SET work_item_id = :item WHERE id = :request",
                {"item": work_item_id, "request": original},
            )
    healthy = client.get(_url(publication["id"], "branch-head"), headers=WORKER_HEADERS)
    assert healthy.status_code == 200, healthy.text


@pytest.mark.parametrize("scenario", ["unchanged", "moved", "moved-again", "merge-parent"])
def test_first_publication_proves_both_ancestry_relations(approved: Seeded, scenario: str) -> None:
    client, publication, code_host = approved
    parent = BASE_SHA
    if scenario != "unchanged":
        if scenario == "merge-parent":
            other = _graph_commit(code_host, _sha(21))
            parent = _graph_commit(code_host, _sha(22), other, BASE_SHA)
            # Membership also needs an all-parent path, independently of O <= P.
            code_host.base_head = _graph_commit(code_host, _sha(23), other, parent)
        else:
            parent = _graph_commit(code_host, _sha(24), BASE_SHA)
            code_host.base_head = parent
            if scenario == "moved-again":
                code_host.base_head = _graph_commit(code_host, _sha(25), parent)
    _published_parent(publication, code_host, parent)
    before = _stored_state(publication)

    response = _verify(client, publication)

    assert response.status_code == 200, response.text
    assert response.headers["Cache-Control"] == "no-store"
    assert response.json() == {"commit_sha": HEAD_SHA, "parent_sha": parent}
    assert len(code_host.reads) == len(set(code_host.reads))
    assert _stored_state(publication) == before


def test_merge_heavy_first_parent_history_stays_within_a_small_read_count(approved: Seeded) -> None:
    client, publication, code_host = approved
    mainline = BASE_SHA
    parent = BASE_SHA
    for merge in range(24):
        side = mainline
        for commit in range(80):
            side = _graph_commit(code_host, _sha(1000 + merge * 100 + commit), side)
        mainline = _graph_commit(code_host, _sha(5000 + merge), mainline, side)
        if merge == 11:
            parent = mainline
    code_host.base_head = mainline
    _published_parent(publication, code_host, parent)

    response = _verify(client, publication)

    assert response.status_code == 200, response.text
    assert response.json() == {"commit_sha": HEAD_SHA, "parent_sha": parent}
    assert len(code_host.reads) <= 30
    assert len(code_host.reads) == len(set(code_host.reads))


@pytest.mark.parametrize("scenario", ["off-branch", "unrelated", "rewound"])
def test_first_publication_rejects_a_parent_outside_the_authoritative_base(
    approved: Seeded, scenario: str
) -> None:
    client, publication, code_host = approved
    parent = _graph_commit(code_host, _sha(31), BASE_SHA)
    code_host.base_head = _graph_commit(code_host, _sha(32), BASE_SHA)
    if scenario == "unrelated":
        parent = _graph_commit(code_host, _sha(33))
        code_host.base_head = parent
    elif scenario == "rewound":
        code_host.base_head = BASE_SHA
    _published_parent(publication, code_host, parent)
    before = _stored_state(publication)

    for suffix in ("revision-commit", "pull-request"):
        response = (
            _verify(client, publication)
            if suffix == "revision-commit"
            else client.post(
                _url(publication["id"], suffix),
                json={"expected_head_sha": HEAD_SHA},
                headers=WORKER_HEADERS,
            )
        )
        assert response.status_code == 409, response.text
        assert response.json()["detail"]["code"] == "publication.base_ancestry_mismatch"
        assert code_host.opened == []
        assert _stored_state(publication) == before


@pytest.mark.parametrize("defect", ["sha", "marker", "inline-marker", "no-parent", "merge"])
def test_first_publication_still_requires_exact_marker_sha_and_single_parent(
    approved: Seeded, defect: str
) -> None:
    client, publication, code_host = approved
    values: dict[str, Any] = {
        "sha": HEAD_SHA,
        "parents": (BASE_SHA,),
        "message": f"Publish\n\nCurie-Revision: {publication['id']}\n",
    }
    if defect == "sha":
        values["sha"] = _sha(40)
    elif defect == "marker":
        values["message"] = f"Curie-Revision: {uuid.uuid4()}\n"
    elif defect == "inline-marker":
        values["message"] = f"Quoted Curie-Revision: {publication['id']} inline\n"
    else:
        values["parents"] = () if defect == "no-parent" else (BASE_SHA, _sha(41))
    code_host.commits[HEAD_SHA] = Commit(**values)
    before = _stored_state(publication)

    response = _verify(client, publication)

    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == "publication.revision_mismatch"
    assert _stored_state(publication) == before
    assert code_host.opened == []
    recovery = client.post(
        _url(publication["id"], "pull-request"),
        json={"expected_head_sha": HEAD_SHA},
        headers=WORKER_HEADERS,
    )
    assert recovery.status_code == 409, recovery.text
    assert recovery.json()["detail"]["code"] == "publication.revision_mismatch"
    assert code_host.opened == []
    assert _stored_state(publication) == before
    _published_parent(publication, code_host, BASE_SHA)
    assert _verify(client, publication).status_code == 200


@pytest.mark.parametrize("defect", ["missing-branch", "missing-commit", "wrong-sha", "bad-parent"])
def test_unreadable_or_malformed_ancestry_fails_closed_and_healthy_retry_progresses(
    approved: Seeded, defect: str
) -> None:
    client, publication, code_host = approved
    parent = _graph_commit(code_host, _sha(51), BASE_SHA)
    code_host.base_head = parent
    _published_parent(publication, code_host, parent)
    if defect == "missing-branch":
        code_host.base_head = None
    elif defect == "missing-commit":
        del code_host.commits[parent]
    elif defect == "wrong-sha":
        code_host.commits[parent] = Commit(sha=_sha(52), parents=(BASE_SHA,), message="wrong")
    else:
        code_host.commits[parent] = Commit(sha=parent, parents=("not-a-sha",), message="bad")
    before = _stored_state(publication)

    refused = _verify(client, publication)

    assert refused.status_code == 503, refused.text
    assert refused.json()["detail"]["code"] == "publication.code_host_unavailable"
    assert "publication_ancestry_unproved" in refused.json()["detail"]["message"]
    assert _stored_state(publication) == before
    assert code_host.opened == []
    recovery = client.post(
        _url(publication["id"], "pull-request"),
        json={"expected_head_sha": HEAD_SHA},
        headers=WORKER_HEADERS,
    )
    assert recovery.status_code == 503, recovery.text
    assert "publication_ancestry_unproved" in recovery.json()["detail"]["message"]
    assert code_host.opened == []
    assert _stored_state(publication) == before
    _graph_commit(code_host, parent, BASE_SHA)
    code_host.base_head = parent
    healthy = _verify(client, publication)
    assert healthy.status_code == 200, healthy.text
    assert healthy.json()["parent_sha"] == parent


def test_ancestry_read_budget_is_shared_by_both_proofs(approved: Seeded) -> None:
    client, publication, code_host = approved
    cursor = BASE_SHA
    for index in range(1100):
        cursor = _graph_commit(code_host, _sha(10000 + index), cursor)
        if index == 599:
            _published_parent(publication, code_host, cursor)
    code_host.base_head = cursor
    before = _stored_state(publication)

    response = _verify(client, publication)

    assert response.status_code == 503, response.text
    assert "publication_ancestry_unproved" in response.json()["detail"]["message"]
    assert len(set(code_host.reads) - {HEAD_SHA}) <= 1024
    assert _stored_state(publication) == before
    assert code_host.opened == []
    code_host.base_head = BASE_SHA
    _published_parent(publication, code_host, BASE_SHA)
    assert _verify(client, publication).status_code == 200


def test_ancestry_has_a_whole_operation_deadline_and_can_retry(approved: Seeded) -> None:
    client, publication, code_host = approved
    intermediate = _graph_commit(code_host, _sha(59), BASE_SHA)
    parent = _graph_commit(code_host, _sha(60), intermediate)
    code_host.base_head = parent
    _published_parent(publication, code_host, parent)
    code_host.read_delay = 6.0
    before = _stored_state(publication)
    started = time.monotonic()

    response = _verify(client, publication)

    assert response.status_code == 503, response.text
    assert time.monotonic() - started < 11.5
    assert "publication_ancestry_unproved" in response.json()["detail"]["message"]
    assert _stored_state(publication) == before
    code_host.read_delay = 0
    assert _verify(client, publication).status_code == 200


def test_provider_outage_does_not_settle_or_open_and_retry_is_live(approved: Seeded) -> None:
    client, publication, code_host = approved
    parent = _graph_commit(code_host, _sha(69), BASE_SHA)
    code_host.base_head = parent
    _published_parent(publication, code_host, parent)
    before = _stored_state(publication)

    async def unavailable_after_revision_read() -> None:
        code_host.fail = Unavailable("provider_unavailable")

    code_host.on_read = unavailable_after_revision_read

    response = _verify(client, publication)

    assert response.status_code == 503, response.text
    assert "publication_ancestry_unproved" in response.json()["detail"]["message"]
    assert _stored_state(publication) == before
    assert code_host.opened == []
    code_host.fail = None
    assert _verify(client, publication).status_code == 200


def test_later_revision_never_uses_first_publication_ancestry_relaxation(seeded: Seeded) -> None:
    client, publication, code_host = seeded
    parent = _graph_commit(code_host, _sha(70), BASE_SHA)
    code_host.base_head = parent
    _published_parent(publication, code_host, parent)
    before = _stored_state(publication)

    refused = _verify(client, publication)

    assert refused.status_code == 409, refused.text
    assert refused.json()["detail"]["code"] == "publication.revision_mismatch"
    assert _stored_state(publication) == before
    _published_parent(publication, code_host, BASE_SHA)
    accepted = _verify(client, publication)
    assert accepted.status_code == 200, accepted.text
    assert accepted.json() == {"commit_sha": HEAD_SHA, "parent_sha": BASE_SHA}


def test_base_can_advance_after_push_before_opening_without_changing_actual_parent(
    approved: Seeded,
) -> None:
    client, publication, code_host = approved
    parent = _graph_commit(code_host, _sha(80), BASE_SHA)
    _published_parent(publication, code_host, parent)
    code_host.base_head = parent
    assert _verify(client, publication).json()["parent_sha"] == parent
    code_host.base_head = _graph_commit(code_host, _sha(81), parent)

    opened = client.post(
        _url(publication["id"], "pull-request"),
        json={"expected_head_sha": HEAD_SHA},
        headers=WORKER_HEADERS,
    )

    assert opened.status_code == 200, opened.text
    assert len(code_host.opened) == 1
    assert code_host.opened[0]["base_ref"] == "main"
    assert _verify(client, publication).json()["parent_sha"] == parent


def test_headless_lineage_with_a_later_revision_number_is_still_a_first_publication(
    approved: Seeded,
) -> None:
    client, publication, code_host = approved
    _execute(
        "UPDATE curie.publications SET revision_number = 2 WHERE id = :id",
        {"id": uuid.UUID(publication["id"])},
    )
    _execute(
        "UPDATE curie.thread_publication_lineages SET latest_revision = 2 WHERE id = :id",
        {"id": uuid.UUID(publication["lineage_id"])},
    )
    parent = _graph_commit(code_host, _sha(90), BASE_SHA)
    code_host.base_head = parent
    _published_parent(publication, code_host, parent)

    response = _verify(client, publication)

    assert response.status_code == 200, response.text
    assert response.json()["parent_sha"] == parent


@pytest.mark.parametrize("partial", ["head-only", "pr-only"])
def test_partial_lineage_identity_cannot_be_classified_as_first_publication(
    approved: Seeded, partial: str
) -> None:
    client, publication, code_host = approved
    if partial == "head-only":
        assignment = "head_sha = :head"
        params: dict[str, Any] = {"head": HEAD_SHA}
    else:
        assignment = "pr_number = :number, pr_url = :url"
        params = {"number": PR_NUMBER, "url": PR_URL}
    _execute(
        f"UPDATE curie.thread_publication_lineages SET {assignment} WHERE id = :id",
        {**params, "id": uuid.UUID(publication["lineage_id"])},
    )
    parent = _graph_commit(code_host, _sha(91), BASE_SHA)
    code_host.base_head = parent
    _published_parent(publication, code_host, parent)
    before = _stored_state(publication)

    response = _verify(client, publication)

    assert response.status_code == 409, response.text
    assert _stored_state(publication) == before
    assert code_host.opened == []


def test_worker_cannot_choose_the_parent_that_the_api_should_trust(approved: Seeded) -> None:
    client, publication, code_host = approved
    parent = _graph_commit(code_host, _sha(92), BASE_SHA)
    code_host.base_head = parent
    _published_parent(publication, code_host, parent)

    response = client.post(
        _url(publication["id"], "revision-commit"),
        json={"commit_sha": HEAD_SHA, "revision_id": publication["id"], "expected_parent": parent},
        headers=WORKER_HEADERS,
    )

    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == "publication.revision_mismatch"
    assert code_host.reads == []
    assert _verify(client, publication).status_code == 200


def test_first_publication_requires_the_stored_expected_parent_to_equal_its_approved_base(
    approved: Seeded,
) -> None:
    client, publication, code_host = approved
    parent = _graph_commit(code_host, _sha(93), BASE_SHA)
    code_host.base_head = parent
    _published_parent(publication, code_host, parent)
    _execute(
        "UPDATE curie.publications SET expected_prior_head = :parent WHERE id = :id",
        {"id": uuid.UUID(publication["id"]), "parent": parent},
    )
    before = _stored_state(publication)

    response = client.post(
        _url(publication["id"], "revision-commit"),
        json={"commit_sha": HEAD_SHA, "revision_id": publication["id"], "expected_parent": parent},
        headers=WORKER_HEADERS,
    )

    assert response.status_code == 409, response.text
    assert _stored_state(publication) == before
    assert code_host.opened == []


@pytest.mark.parametrize("base", [None, "", "x" * 1025])
def test_branch_response_requires_a_bounded_nonempty_base_branch(base: str | None) -> None:
    from curie_api.schemas.publications import PublicationBranchHeadOut
    from pydantic import ValidationError

    payload = {"head_sha": None, **({"base_branch": base} if base is not None else {})}
    with pytest.raises(ValidationError):
        PublicationBranchHeadOut.model_validate(payload)
    assert (
        PublicationBranchHeadOut.model_validate(
            {
                "head_sha": None,
                "base_branch": "release/stable",
            }
        ).base_branch
        == "release/stable"
    )


@pytest.mark.parametrize("parent", [None, "", "invalid", "A" * 40, "a" * 39, "a" * 65])
@pytest.mark.parametrize("field", ["commit_sha", "parent_sha"])
def test_revision_response_requires_full_lowercase_commit_ids(
    parent: str | None, field: str
) -> None:
    from curie_api.schemas.publications import PublicationRevisionOut
    from pydantic import ValidationError

    payload = {"commit_sha": HEAD_SHA, "parent_sha": BASE_SHA}
    if parent is None:
        del payload[field]
    else:
        payload[field] = parent
    with pytest.raises(ValidationError):
        PublicationRevisionOut.model_validate(payload)
    assert (
        PublicationRevisionOut.model_validate(
            {
                "commit_sha": HEAD_SHA,
                "parent_sha": BASE_SHA,
            }
        ).parent_sha
        == BASE_SHA
    )
