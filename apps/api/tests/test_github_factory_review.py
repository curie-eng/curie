"""Review feedback on a factory-owned pull request asks for one more revision (#2798).

A WorkItem owns its pull request through ``work_items.publication_lineage_id``.
Review feedback on that pull request must reach the factory, not the
Slack-bound review arm, and must pass the same authority checks.

Payload shapes follow GitHub's webhook catalog:
https://docs.github.com/en/webhooks/webhook-events-and-payloads#issue_comment
https://docs.github.com/en/webhooks/webhook-events-and-payloads#pull_request_review_comment
https://docs.github.com/en/webhooks/webhook-events-and-payloads#pull_request_review
Provider reads made by the truth verifier:
https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request
https://docs.github.com/en/rest/issues/comments#get-an-issue-comment
https://docs.github.com/en/rest/pulls/comments#get-a-review-comment-for-a-pull-request
https://docs.github.com/en/rest/pulls/reviews#get-a-review-for-a-pull-request
https://docs.github.com/en/rest/collaborators/collaborators#get-repository-permissions-for-a-user
The reply side is covered in test_factory_terminus.py:
https://docs.github.com/en/rest/pulls/comments#create-a-reply-for-a-review-comment
https://docs.github.com/en/rest/issues/comments#create-an-issue-comment

Machine fixtures drive the events. They are not human-authored GitHub proof.
"""

from __future__ import annotations

import asyncio
import copy
import importlib
import itertools
import sys
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest
import redis.asyncio as redis
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.workitem_reconciler import WorkItemReconciler
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

sys.path.insert(0, str(Path(__file__).resolve().parent))

from forge_fakes.github import (  # noqa: E402
    _ENV,
    INSTALLATION_ID,
    LABEL,
    MENTION,
    REPO,
    REPO_ID,
    SENDER,
    SENDER_ID,
    GitHubAPI,
    _Credentials,
    _issue_event,
    _post,
    _sender,
)
from test_github_factory_ingress import _code, _mark_running, _rows  # noqa: E402

HEAD = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
BASE_REF = "main"
_ISSUES = itertools.count(9400)
_PULLS = itertools.count(310)
_FEEDBACK = itertools.count(88001)
EVENTS = ("issue_comment", "pull_request_review_comment", "pull_request_review")
FRAGMENTS = {
    "issue_comment": "issuecomment-{id}",
    "pull_request_review_comment": "discussion_r{id}",
    "pull_request_review": "pullrequestreview-{id}",
}


def _branch(pr: int) -> str:
    return f"curie/publication-acme-{pr}"


def _pr_url(pr: int) -> str:
    return f"https://github.com/{REPO}/pull/{pr}"


class ReviewGitHubAPI(GitHubAPI):
    """Adds the pull request and feedback reads the review verifier makes."""

    def __init__(self) -> None:
        super().__init__()
        self.pulls: dict[int, dict[str, Any]] = {}
        self.feedback: dict[str, dict[str, Any]] = {}

    def open_pull(self, pr: int) -> None:
        repo = {"id": REPO_ID, "full_name": REPO}
        self.pulls[pr] = {
            "number": pr,
            "state": "open",
            "merged": False,
            "node_id": f"PR_acme_{pr}",
            "html_url": _pr_url(pr),
            "head": {"sha": HEAD, "ref": _branch(pr), "repo": dict(repo)},
            "base": {"ref": BASE_REF, "repo": dict(repo)},
        }

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path in self.feedback:
            return httpx.Response(200, json=self.feedback[path])
        prefix = f"/repos/{REPO}/pulls/"
        if path.startswith(prefix) and path[len(prefix) :].isdigit():
            pr = int(path[len(prefix) :])
            if pr in self.pulls:
                return httpx.Response(200, json=self.pulls[pr])
            return httpx.Response(404, json={"message": "missing fixture"})
        return super().handle(request)


def _feedback_event(
    api: ReviewGitHubAPI,
    event: str,
    pr: int,
    body: str,
    *,
    feedback_id: int | None = None,
    user: dict[str, Any] | None = None,
    app: dict[str, Any] | None = None,
    installation_id: int = INSTALLATION_ID,
    state: str = "open",
) -> tuple[dict[str, Any], int]:
    """Build one signed-payload shape and register the provider's current copy."""

    fid = feedback_id if feedback_id is not None else next(_FEEDBACK)
    author = user if user is not None else _sender()
    feedback: dict[str, Any] = {
        "id": fid,
        "body": body,
        "user": dict(author),
        "created_at": "2026-09-20T10:00:00Z",
        "updated_at": "2026-09-20T10:00:00Z",
        "performed_via_github_app": app,
        "author_association": "MEMBER",
        "html_url": f"{_pr_url(pr)}#{FRAGMENTS[event].format(id=fid)}",
    }
    pull = copy.deepcopy(api.pulls.get(pr)) if pr in api.pulls else None
    if pull is None:
        api.open_pull(pr)
        pull = copy.deepcopy(api.pulls[pr])
    pull["state"] = state
    payload: dict[str, Any] = {
        "action": "created",
        "installation": {"id": installation_id},
        "repository": {"id": REPO_ID, "full_name": REPO},
        "sender": dict(author),
    }
    api_base = get_settings().github_api_url.rstrip("/")
    current = copy.deepcopy(feedback)
    if event == "issue_comment":
        payload["issue"] = {
            "number": pr,
            "state": state,
            "pull_request": {
                "html_url": _pr_url(pr),
                "url": f"{api_base}/repos/{REPO}/pulls/{pr}",
            },
        }
        payload["comment"] = feedback
        current["issue_url"] = f"{api_base}/repos/{REPO}/issues/{pr}"
        api.feedback[f"/repos/{REPO}/issues/comments/{fid}"] = current
    elif event == "pull_request_review_comment":
        feedback.update(
            {
                "commit_id": HEAD,
                "path": "src/acme/helper.py",
                "line": 12,
                "pull_request_review_id": fid + 500000,
            }
        )
        current = copy.deepcopy(feedback)
        current["pull_request_url"] = f"{api_base}/repos/{REPO}/pulls/{pr}"
        payload.update({"pull_request": pull, "comment": feedback})
        api.feedback[f"/repos/{REPO}/pulls/comments/{fid}"] = current
    else:
        feedback.update(
            {
                "commit_id": HEAD,
                "state": "changes_requested",
                "submitted_at": "2026-09-20T10:00:00Z",
            }
        )
        current = copy.deepcopy(feedback)
        payload.update({"action": "submitted", "pull_request": pull, "review": feedback})
        api.feedback[f"/repos/{REPO}/pulls/{pr}/reviews/{fid}"] = current
    return payload, fid


def _patch_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "curie_api.github_factory",
        "curie_api.github_review_truth",
        "curie_api.github_factory_review",
        "curie_api.factory_notices",
        "curie_api.repository_auth",
    ):
        try:
            module = importlib.import_module(name)
        except ImportError:
            continue
        monkeypatch.setattr(
            module, "credentials_for", lambda _settings: _Credentials(), raising=False
        )


def _boot(monkeypatch: pytest.MonkeyPatch, api: ReviewGitHubAPI, env: dict[str, str]) -> Any:
    for key, value in {**_ENV, **env}.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    _patch_credentials(monkeypatch)
    return api


@pytest.fixture
def review_api() -> ReviewGitHubAPI:
    return ReviewGitHubAPI()


def _client_with_agent(api: ReviewGitHubAPI) -> Any:
    client_cm = TestClient(create_app())
    client = client_cm.__enter__()
    external = httpx.AsyncClient(transport=httpx.MockTransport(api.handle))
    client.app.state.http_client = external
    created = client.post(
        "/agents",
        headers={"X-API-Key": get_settings().api_key},
        json={
            "name": f"acme-factory-{uuid.uuid4().hex[:8]}",
            "repo_full_name": REPO,
            "channel": {"kind": "github", "address": REPO},
        },
    )
    assert created.status_code == 201, created.text
    return client_cm, client, external


@pytest.fixture
def factory(monkeypatch: pytest.MonkeyPatch, clean_db: None, review_api: ReviewGitHubAPI) -> Any:
    _boot(monkeypatch, review_api, {})
    client_cm, client, external = _client_with_agent(review_api)
    try:
        yield client, review_api
    finally:
        client.portal.call(external.aclose)
        client_cm.__exit__(None, None, None)
        get_settings.cache_clear()


def _execute(statement: str, params: dict[str, Any]) -> None:
    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(text(statement), params)
        finally:
            await engine.dispose()

    asyncio.run(go())


def _admit_issue(client: TestClient, api: ReviewGitHubAPI) -> tuple[int, dict[str, Any]]:
    number = next(_ISSUES)
    api.issue_number = number
    response = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "factory_admitted"
    rows = _requests(number)
    assert len(rows) == 1
    return number, rows[0]


def _own_pull_request(
    work_item_id: uuid.UUID, pr: int, *, lineage_status: str = "open"
) -> uuid.UUID:
    """Seed the lineage a factory publication leaves behind, with GitHub identity."""

    async def go() -> uuid.UUID:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                item = (
                    (
                        await conn.execute(
                            text(
                                "SELECT agent_id, conversation_id, version "
                                "FROM curie.work_items WHERE id = :id"
                            ),
                            {"id": work_item_id},
                        )
                    )
                    .mappings()
                    .one()
                )
                version_id, deployment_id, lineage_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
                await conn.execute(
                    text(
                        "INSERT INTO curie.agent_versions "
                        "(id, agent_id, version_label, created_by) "
                        "VALUES (:id, :agent, 'v1', 'fixture')"
                    ),
                    {"id": version_id, "agent": item["agent_id"]},
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.deployments "
                        "(id, agent_id, version_id, environment, status) VALUES "
                        "(:id, :agent, :version, CAST('dev' AS curie.environment), 'active')"
                    ),
                    {"id": deployment_id, "agent": item["agent_id"], "version": version_id},
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.thread_publication_lineages "
                        "(id, agent_id, deployment_id, conversation_id, repo_full_name, "
                        "base_sha, branch, pr_number, pr_url, head_sha, status, version, "
                        "latest_revision, code_host_kind, code_host_host, "
                        "repository_project_id, code_host_installation_id, "
                        "code_host_pr_id, base_ref) VALUES "
                        "(:id, :agent, :deployment, :conversation, :repo, :base, :branch, "
                        ":pr, :url, :head, :status, 1, 1, 'github', 'github.com', :repo_id, "
                        ":installation, :node, :base_ref)"
                    ),
                    {
                        "id": lineage_id,
                        "agent": item["agent_id"],
                        "deployment": deployment_id,
                        "conversation": item["conversation_id"],
                        "repo": REPO,
                        "base": "0123456789abcdef0123456789abcdef01234567",
                        "branch": _branch(pr),
                        "pr": pr,
                        "url": _pr_url(pr),
                        "head": HEAD,
                        "status": lineage_status,
                        "repo_id": str(REPO_ID),
                        "installation": INSTALLATION_ID,
                        "node": f"PR_acme_{pr}",
                        "base_ref": BASE_REF,
                    },
                )
                await conn.execute(
                    text(
                        "UPDATE curie.work_items SET publication_lineage_id = :lineage, "
                        "version = version + 1 WHERE id = :id"
                    ),
                    {"lineage": lineage_id, "id": work_item_id},
                )
                return lineage_id
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _complete(request_id: uuid.UUID) -> None:
    _mark_running(request_id)
    _execute(
        "UPDATE curie.execution_requests SET status = 'completed', "
        "terminal_at = clock_timestamp(), terminal_cause = 'completed', "
        "version = version + 1 WHERE id = :id AND status = 'running'",
        {"id": request_id},
    )


def _requests(number: int) -> list[dict[str, Any]]:
    return _rows(
        "SELECT r.id, r.sequence, r.status, r.terminal_cause, "
        "r.objective, r.requester, r.reply_kind, "
        "r.reply_address, r.reply_conversation_id, r.reply_target_kind, "
        "r.reply_target_pr_number, r.reply_target_comment_id, r.reply_target_url, "
        "w.id AS work_item_id, "
        "w.conversation_id AS work_item_conversation "
        "FROM curie.execution_requests r "
        "JOIN curie.work_items w ON w.id = r.work_item_id "
        "WHERE w.tracker_scope_id = :repo AND w.tracker_issue_id = :number "
        "ORDER BY r.sequence",
        {"repo": str(REPO_ID), "number": str(number)},
    )


def _all_requests() -> int:
    return len(_rows("SELECT id FROM curie.execution_requests"))


def _owned(client: TestClient, api: ReviewGitHubAPI) -> tuple[int, int, dict[str, Any]]:
    """An issue admitted, published as a PR and completed once."""

    number, first = _admit_issue(client, api)
    pr = next(_PULLS)
    api.open_pull(pr)
    _own_pull_request(first["work_item_id"], pr)
    _complete(first["id"])
    return number, pr, first


def _reply_target(event: str, pr: int, fid: int) -> dict[str, Any]:
    """Where the revision's status reply lands, as admission stores it (#3831)."""

    thread = event == "pull_request_review_comment"
    return {
        "reply_target_kind": "review_thread" if thread else "pull_request",
        "reply_target_pr_number": str(pr),
        "reply_target_comment_id": str(fid) if thread else None,
        "reply_target_url": f"{_pr_url(pr)}#{FRAGMENTS[event].format(id=fid)}",
    }


def _stored_reply_target(row: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if key.startswith("reply_target_")}


def _mention() -> str:
    return f"@{MENTION} please rename the helper before merge"


@pytest.mark.parametrize("event", EVENTS)
def test_authorized_mention_admits_the_next_request_on_the_same_work_item(
    factory: tuple[TestClient, ReviewGitHubAPI], event: str
) -> None:
    client, api = factory
    number, pr, first = _owned(client, api)
    payload, fid = _feedback_event(api, event, pr, _mention())

    response = _post(client, event, payload)

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "factory_admitted", response.text
    rows = _requests(number)
    assert [row["sequence"] for row in rows] == [1, 2]
    revision = rows[1]
    assert revision["work_item_id"] == first["work_item_id"]
    assert revision["status"] == "waiting"
    objective = revision["objective"]
    assert objective.splitlines()[0] == f"{_pr_url(pr)}#{FRAGMENTS[event].format(id=fid)}"
    assert _mention() in objective
    assert revision["reply_kind"] == "github"
    assert revision["reply_address"] == REPO
    assert revision["reply_conversation_id"] == f"issue-{number}"
    assert revision["requester"] == f"github:{SENDER_ID}:{SENDER}"
    assert _stored_reply_target(revision) == _reply_target(event, pr, fid)
    assert _stored_reply_target(rows[0]) == {
        "reply_target_kind": "issue",
        "reply_target_pr_number": None,
        "reply_target_comment_id": None,
        "reply_target_url": None,
    }


def test_redelivery_of_the_same_comment_is_a_duplicate(
    factory: tuple[TestClient, ReviewGitHubAPI],
) -> None:
    client, api = factory
    number, pr, _first = _owned(client, api)
    payload, _fid = _feedback_event(api, "pull_request_review_comment", pr, _mention())
    delivery = str(uuid.uuid4())

    first = _post(client, "pull_request_review_comment", payload, delivery=delivery)
    same = _post(client, "pull_request_review_comment", payload, delivery=delivery)
    fresh = _post(client, "pull_request_review_comment", payload, delivery=str(uuid.uuid4()))

    assert first.json()["status"] == "factory_admitted", first.text
    assert same.json()["status"] == "factory_duplicate", same.text
    assert fresh.json()["status"] == "factory_duplicate", fresh.text
    assert len(_requests(number)) == 2


def _refused(
    client: TestClient, number: int, event: str, payload: dict[str, Any], code: str
) -> None:
    before = _all_requests()
    response = _post(client, event, payload)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "factory_ignored", response.text
    assert _code(response) == code
    assert _all_requests() == before
    assert len(_requests(number)) == 1


def test_ordinary_comment_without_the_mention_is_ignored(
    factory: tuple[TestClient, ReviewGitHubAPI],
) -> None:
    client, api = factory
    number, pr, _first = _owned(client, api)
    payload, _ = _feedback_event(api, "issue_comment", pr, "looks fine to me, thanks")
    _refused(client, number, "issue_comment", payload, "ordinary_comment")


def test_bot_sender_is_refused(factory: tuple[TestClient, ReviewGitHubAPI]) -> None:
    client, api = factory
    number, pr, _first = _owned(client, api)
    bot = {"id": 4242, "login": "acme-helper", "type": "Bot"}
    payload, _ = _feedback_event(api, "issue_comment", pr, _mention(), user=bot)
    _refused(client, number, "issue_comment", payload, "non_human_sender")


def test_app_performed_comment_is_refused(factory: tuple[TestClient, ReviewGitHubAPI]) -> None:
    client, api = factory
    number, pr, _first = _owned(client, api)
    payload, _ = _feedback_event(api, "pull_request_review_comment", pr, _mention(), app={"id": 51})
    _refused(client, number, "pull_request_review_comment", payload, "app_authored")


def test_other_installation_is_refused(factory: tuple[TestClient, ReviewGitHubAPI]) -> None:
    client, api = factory
    number, pr, _first = _owned(client, api)
    payload, _ = _feedback_event(
        api, "issue_comment", pr, _mention(), installation_id=INSTALLATION_ID + 7
    )
    _refused(client, number, "issue_comment", payload, "installation_mismatch")


@pytest.mark.parametrize("permission", ["read", "none"])
def test_sender_without_write_permission_is_refused(
    factory: tuple[TestClient, ReviewGitHubAPI], permission: str
) -> None:
    client, api = factory
    number, pr, _first = _owned(client, api)
    api.permission = permission
    payload, _ = _feedback_event(api, "pull_request_review", pr, _mention())
    _refused(client, number, "pull_request_review", payload, "sender_permission_refused")


def test_closed_lineage_is_refused(factory: tuple[TestClient, ReviewGitHubAPI]) -> None:
    client, api = factory
    number, first = _admit_issue(client, api)
    pr = next(_PULLS)
    api.open_pull(pr)
    _own_pull_request(first["work_item_id"], pr, lineage_status="closed")
    _complete(first["id"])
    payload, _ = _feedback_event(api, "issue_comment", pr, _mention())
    _refused(client, number, "issue_comment", payload, "lineage_closed")


def test_cancelled_work_item_is_refused(factory: tuple[TestClient, ReviewGitHubAPI]) -> None:
    client, api = factory
    number, pr, first = _owned(client, api)
    _execute(
        "UPDATE curie.work_items SET cancelled_at = clock_timestamp(), "
        "version = version + 1 WHERE id = :id",
        {"id": first["work_item_id"]},
    )
    payload, _ = _feedback_event(api, "issue_comment", pr, _mention())
    _refused(client, number, "issue_comment", payload, "work_item_cancelled")


@pytest.mark.parametrize("event", ["issue_comment", "pull_request_review_comment"])
def test_closed_pull_request_is_refused(
    factory: tuple[TestClient, ReviewGitHubAPI], event: str
) -> None:
    client, api = factory
    number, pr, _first = _owned(client, api)
    payload, _ = _feedback_event(api, event, pr, _mention(), state="closed")
    _refused(client, number, event, payload, "terminal_pull_request")


def test_open_pull_request_without_an_owning_work_item_is_unbound(
    factory: tuple[TestClient, ReviewGitHubAPI],
) -> None:
    client, api = factory
    number, _pr, _first = _owned(client, api)
    stranger = next(_PULLS)
    api.open_pull(stranger)
    payload, _ = _feedback_event(api, "issue_comment", stranger, _mention())
    _refused(client, number, "issue_comment", payload, "lineage_unbound")


def _reconcile_queued_revisions(monkeypatch: pytest.MonkeyPatch) -> None:
    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        valkey = redis.Redis(
            host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None
        )
        reconciler = WorkItemReconciler(maker, valkey, get_settings())

        async def skip_github_io() -> None:
            # GitHub reads and status delivery are covered by separate fixtures.
            return None

        monkeypatch.setattr(reconciler, "_reconcile_missed_labels", skip_github_io)
        monkeypatch.setattr(reconciler, "_sync_status_comments", skip_github_io)
        try:
            await reconciler.run_once()
        finally:
            await valkey.aclose()
            await engine.dispose()

    asyncio.run(go())


@pytest.mark.parametrize("event", EVENTS)
def test_review_mention_during_live_run_waits_then_becomes_next_request(
    factory: tuple[TestClient, ReviewGitHubAPI], monkeypatch: pytest.MonkeyPatch, event: str
) -> None:
    client, api = factory
    number, first = _admit_issue(client, api)
    pr = next(_PULLS)
    api.open_pull(pr)
    _own_pull_request(first["work_item_id"], pr)
    _mark_running(first["id"])
    payload, fid = _feedback_event(api, event, pr, _mention())
    delivery = str(uuid.uuid4())

    admitted = _post(client, event, payload, delivery=delivery)
    same_delivery = _post(client, event, payload, delivery=delivery)
    fresh_delivery = _post(client, event, payload, delivery=str(uuid.uuid4()))

    assert admitted.status_code == 200, admitted.text
    assert admitted.json()["status"] == "factory_queued", admitted.text
    assert same_delivery.json()["status"] == "factory_duplicate", same_delivery.text
    assert fresh_delivery.json()["status"] == "factory_duplicate", fresh_delivery.text
    rows = _requests(number)
    assert [row["sequence"] for row in rows] == [1, 2]
    assert rows[0]["status"] == "running"
    revision = rows[1]
    assert revision["status"] == "queued"
    assert revision["work_item_id"] == first["work_item_id"]
    assert revision["objective"].splitlines()[0] == (
        f"{_pr_url(pr)}#{FRAGMENTS[event].format(id=fid)}"
    )
    assert _stored_reply_target(revision) == _reply_target(event, pr, fid)
    assert len(
        _rows(
            "SELECT execution_request_id FROM curie.factory_terminal_notices "
            "WHERE execution_request_id = :id",
            {"id": revision["id"]},
        )
    ) == 1

    _reconcile_queued_revisions(monkeypatch)
    assert _requests(number)[1]["status"] == "queued"

    _execute(
        "UPDATE curie.execution_requests SET status = 'completed', "
        "terminal_at = clock_timestamp(), terminal_cause = 'completed', "
        "version = version + 1 WHERE id = :id AND status = 'running'",
        {"id": first["id"]},
    )
    _reconcile_queued_revisions(monkeypatch)
    rows = _requests(number)
    assert [row["sequence"] for row in rows] == [1, 2]
    assert [row["status"] for row in rows] == ["completed", "waiting"]
    assert rows[1]["id"] == revision["id"]


def test_distinct_review_mentions_wait_in_order_until_each_live_run_ends(
    factory: tuple[TestClient, ReviewGitHubAPI], monkeypatch: pytest.MonkeyPatch
) -> None:
    client, api = factory
    number, first = _admit_issue(client, api)
    pr = next(_PULLS)
    api.open_pull(pr)
    _own_pull_request(first["work_item_id"], pr)
    _mark_running(first["id"])
    first_payload, first_fid = _feedback_event(
        api, "issue_comment", pr, f"@{MENTION} please change the first helper"
    )
    second_payload, second_fid = _feedback_event(
        api, "issue_comment", pr, f"@{MENTION} please change the second helper"
    )

    first_reply = _post(client, "issue_comment", first_payload)
    second_reply = _post(client, "issue_comment", second_payload)

    assert first_reply.json()["status"] == "factory_queued", first_reply.text
    assert second_reply.json()["status"] == "factory_queued", second_reply.text
    rows = _requests(number)
    assert [row["sequence"] for row in rows] == [1, 2, 3]
    assert [row["status"] for row in rows] == ["running", "queued", "queued"]
    assert [row["work_item_id"] for row in rows] == [first["work_item_id"]] * 3
    assert rows[1]["objective"].splitlines()[0].endswith(f"#issuecomment-{first_fid}")
    assert rows[2]["objective"].splitlines()[0].endswith(f"#issuecomment-{second_fid}")
    first_revision_id, second_revision_id = rows[1]["id"], rows[2]["id"]

    _execute(
        "UPDATE curie.execution_requests SET status = 'completed', "
        "terminal_at = clock_timestamp(), terminal_cause = 'completed', "
        "version = version + 1 WHERE id = :id AND status = 'running'",
        {"id": first["id"]},
    )
    _reconcile_queued_revisions(monkeypatch)
    rows = _requests(number)
    assert [row["status"] for row in rows] == ["completed", "waiting", "queued"]
    assert [row["id"] for row in rows[1:]] == [first_revision_id, second_revision_id]

    _mark_running(first_revision_id)
    _reconcile_queued_revisions(monkeypatch)
    assert [row["status"] for row in _requests(number)] == [
        "completed", "running", "queued"
    ]
    _execute(
        "UPDATE curie.execution_requests SET status = 'completed', "
        "terminal_at = clock_timestamp(), terminal_cause = 'completed', "
        "version = version + 1 WHERE id = :id AND status = 'running'",
        {"id": first_revision_id},
    )
    _reconcile_queued_revisions(monkeypatch)
    rows = _requests(number)
    assert [row["status"] for row in rows] == ["completed", "completed", "waiting"]
    assert rows[2]["id"] == second_revision_id


def test_review_admission_survives_work_item_version_change_during_verification(
    factory: tuple[TestClient, ReviewGitHubAPI], monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_api import github_factory_review

    client, api = factory
    number, first = _admit_issue(client, api)
    pr = next(_PULLS)
    api.open_pull(pr)
    _own_pull_request(first["work_item_id"], pr)
    _mark_running(first["id"])
    payload, fid = _feedback_event(api, "issue_comment", pr, _mention())
    verify = github_factory_review.verify_feedback_truth
    bumped = False

    async def verify_with_version_change(*args: Any, **kwargs: Any) -> None:
        nonlocal bumped
        await verify(*args, **kwargs)
        if bumped:
            return
        bumped = True
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE curie.work_items SET version = version + 1 "
                        "WHERE id = :id"
                    ),
                    {"id": first["work_item_id"]},
                )
        finally:
            await engine.dispose()

    monkeypatch.setattr(
        github_factory_review, "verify_feedback_truth", verify_with_version_change
    )

    response = _post(client, "issue_comment", payload)
    replay = _post(client, "issue_comment", payload, delivery=str(uuid.uuid4()))

    assert bumped
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "factory_queued", response.text
    assert replay.json()["status"] == "factory_duplicate", replay.text
    rows = _requests(number)
    assert [row["status"] for row in rows] == ["running", "queued"]
    assert rows[1]["objective"].splitlines()[0].endswith(f"#issuecomment-{fid}")
    assert rows[1]["work_item_id"] == first["work_item_id"]


def test_unlabel_cancels_queued_review_revision_before_it_can_run(
    factory: tuple[TestClient, ReviewGitHubAPI], monkeypatch: pytest.MonkeyPatch
) -> None:
    client, api = factory
    number, first = _admit_issue(client, api)
    pr = next(_PULLS)
    api.open_pull(pr)
    _own_pull_request(first["work_item_id"], pr)
    _mark_running(first["id"])
    payload, _ = _feedback_event(api, "issue_comment", pr, _mention())
    queued = _post(client, "issue_comment", payload)
    assert queued.json()["status"] == "factory_queued", queued.text
    revision_id = _requests(number)[1]["id"]

    api.labels = []
    removed = _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))

    assert removed.json()["status"] == "factory_cancellation_requested", removed.text
    _reconcile_queued_revisions(monkeypatch)
    rows = _requests(number)
    assert rows[0]["status"] == "cancellation_requested"
    assert rows[1]["id"] == revision_id
    assert rows[1]["status"] == "cancelled"
    assert rows[1]["terminal_cause"] == "issue_cancelled"
    _reconcile_queued_revisions(monkeypatch)
    assert _requests(number)[1]["status"] == "cancelled"


def test_closed_lineage_cancels_queued_review_revision_after_live_run_ends(
    factory: tuple[TestClient, ReviewGitHubAPI], monkeypatch: pytest.MonkeyPatch
) -> None:
    client, api = factory
    number, first = _admit_issue(client, api)
    pr = next(_PULLS)
    api.open_pull(pr)
    lineage_id = _own_pull_request(first["work_item_id"], pr)
    _mark_running(first["id"])
    payload, _ = _feedback_event(api, "pull_request_review_comment", pr, _mention())
    queued = _post(client, "pull_request_review_comment", payload)
    assert queued.json()["status"] == "factory_queued", queued.text
    revision_id = _requests(number)[1]["id"]

    _execute(
        "UPDATE curie.thread_publication_lineages SET status = 'closed', "
        "version = version + 1 WHERE id = :id",
        {"id": lineage_id},
    )
    _execute(
        "UPDATE curie.execution_requests SET status = 'completed', "
        "terminal_at = clock_timestamp(), terminal_cause = 'completed', "
        "version = version + 1 WHERE id = :id AND status = 'running'",
        {"id": first["id"]},
    )
    _reconcile_queued_revisions(monkeypatch)
    rows = _requests(number)
    assert rows[0]["status"] == "completed"
    assert rows[1]["id"] == revision_id
    assert rows[1]["status"] == "cancelled"
    assert rows[1]["terminal_cause"] == "lineage_closed"
    _reconcile_queued_revisions(monkeypatch)
    assert _requests(number)[1]["status"] == "cancelled"


@pytest.fixture
def review_key() -> str:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()


def test_review_ingress_still_owns_pull_requests_no_work_item_owns(
    monkeypatch: pytest.MonkeyPatch,
    clean_db: None,
    review_api: ReviewGitHubAPI,
    review_key: str,
) -> None:
    stream = f"test:curie:factory-review:{uuid.uuid4().hex}"
    _boot(
        monkeypatch,
        review_api,
        {
            "GITHUB_REVIEW_INGRESS_ENABLED": "true",
            "GITHUB_APP_PRIVATE_KEY": review_key,
            "GITHUB_REVIEW_RECONCILER_INTERVAL_S": "3600",
            "RUNS_STREAM": stream,
            "KEY_PREFIX": f"{stream}:worker",
        },
    )
    client_cm, client, external = _client_with_agent(review_api)
    try:
        pr = next(_PULLS)
        review_api.open_pull(pr)
        payload, _ = _feedback_event(review_api, "issue_comment", pr, _mention())
        before = _all_requests()

        response = _post(client, "issue_comment", payload)

        status_value = response.json().get("status", "")
        assert status_value.startswith("feedback_"), response.text
        assert not status_value.startswith("factory_"), response.text
        assert _all_requests() == before
    finally:
        client.portal.call(external.aclose)
        client_cm.__exit__(None, None, None)
        get_settings.cache_clear()
