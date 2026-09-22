"""A labelled issue ends as a pull request or exactly one comment.

GitHub issue comments follow
https://docs.github.com/en/rest/issues/comments#create-an-issue-comment
and
https://docs.github.com/en/rest/issues/comments#list-issue-comments
Admission is a signed issues webhook against create_app(). Work items are not
inserted by this file.

A revision asked for from pull request review feedback (#2798) answers on the
pull request instead:
https://docs.github.com/en/rest/pulls/comments#create-a-reply-for-a-review-comment
https://docs.github.com/en/rest/pulls/comments#list-review-comments-on-a-pull-request
and a pull request's conversation comments use the issue comment endpoints with
the pull request number. Those revision requests are inserted directly with the
objective the review ingress writes; machine fixtures drive the events, not
human-authored GitHub proof.
"""

from __future__ import annotations

import asyncio
import json
import sys
import threading
import uuid
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api.config import get_settings
from curie_api.factory_notices import comment_body, marker_for
from curie_api.workitem_dispatch import DispatchConflict, acquire, start
from curie_api.workitem_reconciler import WorkItemReconciler
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from test_github_factory_ingress import (
    INSTALLATION_ID,
    LABEL,
    REPO,
    REPO_ID,
    GitHubAPI,
    _issue_event,
    _post,
)

pytestmark = pytest.mark.usefixtures("clean_db")


class _Credentials:
    def token_for_verified_installation(self, repo: str, installation_id: int) -> str:
        if repo != REPO or installation_id != INSTALLATION_ID:
            raise RuntimeError("unexpected installation")
        return "ghs_factory_terminus_fixture"


class _GitHubComments(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: object) -> None:
        return

    def _send(self, status: int, payload: object) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        server = self.server
        assert isinstance(server, _CommentServer)
        path = self.path.split("?", 1)[0]
        server.requests.append(("GET", path, None))
        if server.by_path:
            self._send(200, list(server.lists.get(path, [])))
            return
        self._send(200, list(server.comments))

    def do_POST(self) -> None:  # noqa: N802
        server = self.server
        assert isinstance(server, _CommentServer)
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length) or b"{}")
        path = self.path.split("?", 1)[0]
        server.requests.append(("POST", path, payload.get("body", "")))
        if server.by_path:
            refused = server.refuse_paths.get(path)
            server.posts += 1
            if refused is not None:
                self._send(refused, {"message": "refused"})
                return
            listed = path.rsplit("/", 2)[0] if path.endswith("/replies") else path
            comment = {"id": 8000 + server.posts, "body": payload.get("body", "")}
            server.lists.setdefault(listed, []).append(comment)
            self._send(201, comment)
            return
        if server.refuse_status is not None:
            server.posts += 1
            self._send(server.refuse_status, {"message": "refused"})
            return
        comment = {"id": 7000 + len(server.comments) + 1, "body": payload.get("body", "")}
        server.comments.append(comment)
        server.posts += 1
        self._send(201, comment)


class _CommentServer(ThreadingHTTPServer):
    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), _GitHubComments)
        self.comments: list[dict[str, Any]] = []
        self.posts = 0
        self.refuse_status: int | None = None
        # Path-aware mode for pull request replies. Each list endpoint keeps its
        # own comments, and a review-thread reply lands in the PR review list.
        self.by_path = False
        self.lists: dict[str, list[dict[str, Any]]] = {}
        self.refuse_paths: dict[str, int] = {}
        self.requests: list[tuple[str, str, str | None]] = []


@pytest.fixture
def comments(monkeypatch: pytest.MonkeyPatch) -> Any:
    server = _CommentServer()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    monkeypatch.setenv("GITHUB_API_URL", f"http://{host}:{port}")
    monkeypatch.setenv("CURIE_WORK_ITEM_WAIT_BUDGET_SECONDS", "30")
    monkeypatch.setenv("CURIE_WORK_ITEM_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("APPROVAL_SWEEP_INTERVAL_S", "0")
    monkeypatch.setenv("DEAD_LETTER_WATCH_INTERVAL_S", "0")
    monkeypatch.setenv("GITHUB_FACTORY_INGRESS_ENABLED", "true")
    monkeypatch.setenv("GITHUB_FACTORY_LABEL", LABEL)
    monkeypatch.setenv("GITHUB_FACTORY_MENTION", "curie")
    monkeypatch.setenv("GITHUB_REVIEW_INGRESS_ENABLED", "false")
    monkeypatch.setenv("GITHUB_APP_ID", "51")
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", "example-private-key")
    monkeypatch.setenv("GITHUB_WEBHOOK_SECRET", "example-factory-hmac-secret")
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    monkeypatch.setenv("GITHUB_TOKEN", "")
    monkeypatch.setenv("INTERNAL_WORKER_TOKEN", "factory-terminus-worker")
    monkeypatch.setenv("RUNS_STREAM", f"test:curie:terminus:{uuid.uuid4().hex}")
    get_settings.cache_clear()
    monkeypatch.setattr(
        "curie_api.factory_notices.credentials_for",
        lambda _settings: _Credentials(),
    )
    monkeypatch.setattr(
        "curie_api.github_factory.credentials_for",
        lambda _settings: _Credentials(),
    )
    yield server
    server.shutdown()
    thread.join(timeout=5)
    get_settings.cache_clear()


@pytest.fixture
def admitted(comments: _CommentServer) -> Any:
    from curie_api.main import create_app
    from fastapi.testclient import TestClient

    github = GitHubAPI()
    with TestClient(create_app()) as client:
        import httpx

        external = httpx.AsyncClient(transport=httpx.MockTransport(github.handle))
        client.app.state.http_client = external
        headers = {"X-API-Key": get_settings().api_key}
        created = client.post(
            "/agents",
            headers=headers,
            json={
                "name": f"acme-factory-{uuid.uuid4().hex[:8]}",
                "repo_full_name": REPO,
                "channel": {"kind": "github", "address": REPO},
            },
        )
        assert created.status_code == 201, created.text
        yield client, github, comments
        client.portal.call(external.aclose)


def _rows(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def go() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as connection:
                result = await connection.execute(text(statement), params or {})
                return [dict(row) for row in result.mappings()]
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _request(number: int) -> dict[str, Any]:
    rows = _rows(
        "SELECT r.id, r.status, r.terminal_cause, r.version, w.id AS work_item_id, "
        "w.version AS work_version "
        "FROM curie.execution_requests r "
        "JOIN curie.work_items w ON w.id = r.work_item_id "
        "WHERE w.github_repository_id = :repo AND w.github_issue_number = :number",
        {"repo": REPO_ID, "number": number},
    )
    assert len(rows) == 1, rows
    return rows[0]


def _notices(request_id: uuid.UUID) -> list[dict[str, Any]]:
    return _rows(
        "SELECT execution_request_id, terminal_cause, attempts, posted_at, "
        "comment_id, refused_at, refusal "
        "FROM curie.factory_terminal_notices WHERE execution_request_id = :id",
        {"id": request_id},
    )


def _label(client: Any, github: GitHubAPI, number: int) -> None:
    github.issue_number = number
    response = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "factory_admitted"


def _reconcile() -> None:
    async def go() -> None:
        import redis.asyncio as aioredis

        engine = create_async_engine(get_settings().database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        client = aioredis.Redis(
            host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None
        )
        reconciler = WorkItemReconciler(maker, client, get_settings())
        try:
            await reconciler.run_once()
        finally:
            await client.aclose()
            await engine.dispose()

    asyncio.run(go())


def _reconcile_later(seconds: int) -> None:
    """Advance only the reconciler clock. Stored deadlines stay write-once."""

    import curie_api.workitems as workitems

    original = workitems._database_now

    async def later(session: AsyncSession) -> Any:
        real = await original(session)
        return real + timedelta(seconds=seconds)

    workitems._database_now = later
    try:
        _reconcile()
    finally:
        workitems._database_now = original


def _observe_termination(client: Any, request_id: uuid.UUID) -> None:
    headers = {"X-Curie-Worker-Token": "factory-terminus-worker"}
    claimed = client.post(
        f"/v1/internal/work-items/requests/{request_id}/termination/claim",
        headers=headers,
        json={"owner": "factory-owner"},
    )
    assert claimed.status_code == 200, claimed.text
    recorded = client.post(
        f"/v1/internal/work-items/requests/{request_id}/termination",
        headers=headers,
        json={
            "runtime_epoch": claimed.json()["runtime_epoch"],
            "observation": "runtime stopped",
        },
    )
    assert recorded.status_code == 200, recorded.text


def _start_running(request_id: uuid.UUID) -> int:
    async def go() -> int:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                acquired = await acquire(
                    session, request_id, owner="factory-owner", generation=1
                )
                assert not isinstance(acquired, DispatchConflict), acquired
                started = await start(
                    session,
                    request_id,
                    owner="factory-owner",
                    generation=1,
                    claim_name="claim-factory",
                    sandbox_name="sbx-factory",
                )
                assert not isinstance(started, DispatchConflict), started
                return int(started.runtime_epoch)
        finally:
            await engine.dispose()

    return asyncio.run(go())


def test_capacity_wait_expiry_posts_one_comment(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9201
    _label(client, github, number)
    _reconcile_later(31)
    row = _request(number)
    assert (row["status"], row["terminal_cause"]) == ("expired", "capacity_wait_expired")
    notices = _notices(row["id"])
    assert len(notices) == 1
    assert notices[0]["terminal_cause"] == "capacity_wait_expired"
    assert notices[0]["posted_at"] is not None
    assert sink.posts == 1
    assert marker_for(row["id"]) in sink.comments[0]["body"]
    assert "capacity_wait_expired" in sink.comments[0]["body"]
    _reconcile()
    assert sink.posts == 1
    assert len(_notices(row["id"])) == 1


def test_label_removal_posts_one_comment(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9202
    _label(client, github, number)
    github.labels = []
    removed = _post(
        client, "issues", _issue_event("unlabeled", number, label={"name": LABEL})
    )
    assert removed.json()["status"] == "factory_cancelled"
    row = _request(number)
    assert (row["status"], row["terminal_cause"]) == ("cancelled", "issue_cancelled")
    assert _notices(row["id"])[0]["posted_at"] is None
    _reconcile()
    assert sink.posts == 1
    assert "issue_cancelled" in sink.comments[0]["body"]
    assert marker_for(row["id"]) in sink.comments[0]["body"]


def test_runner_escalation_posts_one_comment_and_completed_needs_a_pull_request(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 9203
    _label(client, github, number)
    row = _request(number)
    epoch = _start_running(row["id"])
    headers = {"X-Curie-Worker-Token": "factory-terminus-worker"}
    refused = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/finish",
        headers=headers,
        json={"runtime_epoch": epoch, "outcome": "completed", "cause": "completed"},
    )
    assert refused.status_code == 409, refused.text
    assert _request(number)["status"] == "running"
    failed = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/finish",
        headers=headers,
        json={"runtime_epoch": epoch, "outcome": "failed", "cause": "runner_escalated"},
    )
    assert failed.status_code == 200, failed.text
    terminal = _request(number)
    assert (terminal["status"], terminal["terminal_cause"]) == ("failed", "runner_escalated")
    version = terminal["version"]
    _reconcile()
    assert sink.posts == 1
    assert "runner_escalated" in sink.comments[0]["body"]
    assert _request(number)["version"] == version


def test_a_refused_post_leaves_the_terminal_row_unchanged(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9204
    _label(client, github, number)
    github.labels = []
    _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    row = _request(number)
    version = row["version"]
    sink.refuse_status = 403
    _reconcile()
    assert sink.posts == 1
    notices = _notices(row["id"])
    assert notices[0]["posted_at"] is None
    assert notices[0]["refused_at"] is not None
    assert notices[0]["refusal"] == "http_403"
    again = _request(number)
    assert again["version"] == version
    assert (again["status"], again["terminal_cause"]) == ("cancelled", "issue_cancelled")


def test_a_crash_between_commit_and_post_still_posts_once(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9205
    _label(client, github, number)
    github.labels = []
    _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    row = _request(number)
    sink.comments.append(
        {"id": 7444, "body": comment_body(row["id"], "issue_cancelled")}
    )
    _reconcile()
    assert sink.posts == 0
    notices = _notices(row["id"])
    assert notices[0]["posted_at"] is not None
    assert notices[0]["comment_id"] == 7444
    _reconcile()
    assert sink.posts == 0
    assert len(_notices(row["id"])) == 1


def test_a_running_cancellation_comments_only_after_the_runtime_is_observed(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 9207
    _label(client, github, number)
    row = _request(number)
    _start_running(row["id"])
    github.labels = []
    removed = _post(
        client, "issues", _issue_event("unlabeled", number, label={"name": LABEL})
    )
    assert removed.json()["status"] == "factory_cancellation_requested"
    requested = _request(number)
    assert requested["status"] == "cancellation_requested"
    assert _notices(row["id"]) == []
    headers = {"X-Curie-Worker-Token": "factory-terminus-worker"}
    claimed = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/termination/claim",
        headers=headers,
        json={"owner": "factory-owner"},
    )
    assert claimed.status_code == 200, claimed.text
    recorded = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/termination",
        headers=headers,
        json={
            "runtime_epoch": claimed.json()["runtime_epoch"],
            "observation": "runtime stopped after the issue was unlabelled",
        },
    )
    assert recorded.status_code == 200, recorded.text
    terminal = _request(number)
    assert (terminal["status"], terminal["terminal_cause"]) == ("cancelled", "issue_cancelled")
    _reconcile()
    assert sink.posts == 1
    assert "issue_cancelled" in sink.comments[0]["body"]
    assert marker_for(row["id"]) in sink.comments[0]["body"]


def test_execution_deadline_posts_one_comment(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9210
    _label(client, github, number)
    row = _request(number)
    _start_running(row["id"])
    _reconcile_later(1900)
    requested = _request(number)
    assert (requested["status"], requested["terminal_cause"]) == (
        "cancellation_requested",
        "execution_deadline",
    )
    assert _notices(row["id"]) == []
    _observe_termination(client, row["id"])
    terminal = _request(number)
    assert (terminal["status"], terminal["terminal_cause"]) == (
        "expired",
        "execution_deadline",
    )
    _reconcile()
    assert sink.posts == 1
    assert "execution_deadline" in sink.comments[0]["body"]
    assert marker_for(row["id"]) in sink.comments[0]["body"]
    _reconcile()
    assert sink.posts == 1


def test_owner_lost_posts_one_comment(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9211
    _label(client, github, number)
    row = _request(number)
    _start_running(row["id"])

    async def lapse_heartbeat() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                changed = await connection.execute(
                    text(
                        "UPDATE curie.execution_requests "
                        "SET runtime_heartbeat_expires_at = clock_timestamp() "
                        "- interval '1 second' "
                        "WHERE id = :id AND status = 'running'"
                    ),
                    {"id": row["id"]},
                )
                assert changed.rowcount == 1
        finally:
            await engine.dispose()

    asyncio.run(lapse_heartbeat())
    _reconcile()
    requested = _request(number)
    assert (requested["status"], requested["terminal_cause"]) == (
        "cancellation_requested",
        "owner_lost",
    )
    assert _notices(row["id"]) == []
    _observe_termination(client, row["id"])
    terminal = _request(number)
    assert (terminal["status"], terminal["terminal_cause"]) == ("failed", "owner_lost")
    _reconcile()
    assert sink.posts == 1
    assert "owner_lost" in sink.comments[0]["body"]
    assert marker_for(row["id"]) in sink.comments[0]["body"]


def test_runner_failure_posts_one_comment(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9212
    _label(client, github, number)
    row = _request(number)
    epoch = _start_running(row["id"])
    failed = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/finish",
        headers={"X-Curie-Worker-Token": "factory-terminus-worker"},
        json={"runtime_epoch": epoch, "outcome": "failed", "cause": "runner_failed"},
    )
    assert failed.status_code == 200, failed.text
    terminal = _request(number)
    assert (terminal["status"], terminal["terminal_cause"]) == ("failed", "runner_failed")
    _reconcile()
    assert sink.posts == 1
    assert "runner_failed" in sink.comments[0]["body"]
    assert marker_for(row["id"]) in sink.comments[0]["body"]


def test_publication_expiry_and_failure_each_post_one_comment(admitted: Any) -> None:
    client, github, sink = admitted
    cases = ((9213, "expired", "publication_expired"), (9214, "failed", "publication_failed"))
    for number, status, _cause in cases:
        _label(client, github, number)
        row = _request(number)
        _start_running(row["id"])
        _attach_publication(row["work_item_id"], status=status, pr=None)
    _reconcile()
    assert sink.posts == 2
    bodies = [comment["body"] for comment in sink.comments]
    for number, _status, cause in cases:
        row = _request(number)
        assert (row["status"], row["terminal_cause"]) == ("failed", cause)
        assert sum(cause in body for body in bodies) == 1
        assert len(_notices(row["id"])) == 1


def test_a_denied_publication_fails_and_a_pull_request_completes_without_a_comment(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    denied_number = 9208
    opened_number = 9209
    _label(client, github, denied_number)
    _label(client, github, opened_number)
    denied = _request(denied_number)
    opened = _request(opened_number)
    _start_running(denied["id"])
    _start_running(opened["id"])
    _attach_publication(denied["work_item_id"], status="denied", pr=None)
    _attach_publication(opened["work_item_id"], status="succeeded", pr=77)
    _reconcile()
    denied_row = _request(denied_number)
    opened_row = _request(opened_number)
    assert (denied_row["status"], denied_row["terminal_cause"]) == (
        "failed",
        "publication_denied",
    )
    assert (opened_row["status"], opened_row["terminal_cause"]) == ("completed", "completed")
    assert sink.posts == 1
    assert "publication_denied" in sink.comments[0]["body"]
    assert _notices(opened_row["id"]) == []
    assert len(_notices(denied_row["id"])) == 1


def _attach_publication(work_item_id: uuid.UUID, *, status: str, pr: int | None) -> None:
    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                item = (
                    await conn.execute(
                        text(
                            "SELECT agent_id, conversation_id, repo_full_name, "
                            "github_repository_id, github_installation_id, version "
                            "FROM curie.work_items WHERE id = :id"
                        ),
                        {"id": work_item_id},
                    )
                ).mappings().one()
                request = (
                    await conn.execute(
                        text(
                            "SELECT id, version FROM curie.execution_requests "
                            "WHERE work_item_id = :id AND status = 'running'"
                        ),
                        {"id": work_item_id},
                    )
                ).mappings().one()
                version_id, deployment_id, lineage_id = (
                    uuid.uuid4(),
                    uuid.uuid4(),
                    uuid.uuid4(),
                )
                approval_id, publication_id = uuid.uuid4(), uuid.uuid4()
                pr_url = (
                    None
                    if pr is None
                    else f"https://github.com/{item['repo_full_name']}/pull/{pr}"
                )
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
                    {
                        "id": deployment_id,
                        "agent": item["agent_id"],
                        "version": version_id,
                    },
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.thread_publication_lineages "
                        "(id, agent_id, deployment_id, conversation_id, repo_full_name, "
                        "base_sha, branch, pr_number, pr_url, status, version, "
                        "latest_revision) VALUES "
                        "(:id, :agent, :deployment, :conversation, :repo, :base, "
                        ":branch, :pr, :url, 'open', 1, 1)"
                    ),
                    {
                        "id": lineage_id,
                        "agent": item["agent_id"],
                        "deployment": deployment_id,
                        "conversation": item["conversation_id"],
                        "repo": item["repo_full_name"],
                        "base": "0123456789abcdef0123456789abcdef01234567",
                        "branch": f"curie/publication-{lineage_id.hex}",
                        "pr": pr,
                        "url": pr_url,
                    },
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.approvals "
                        "(id, agent_id, conversation_id, author, summary, reply_kind, "
                        "reply_channel, dedupe_key, status, purpose) VALUES "
                        "(:id, :agent, :conversation, 'U0REQUEST1', "
                        "'Publish repository changes', 'github', :channel, :dedupe, "
                        "'approved', 'publication')"
                    ),
                    {
                        "id": approval_id,
                        "agent": item["agent_id"],
                        "conversation": item["conversation_id"],
                        "channel": item["repo_full_name"],
                        "dedupe": f"terminus-{publication_id.hex}",
                    },
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.publications "
                        "(id, approval_id, deployment_id, workspace_conversation_id, "
                        "lineage_id, execution_request_id, revision_number, repo_full_name, "
                        "status, base_sha, changed_paths, title, body, reply_kind, "
                        "reply_channel, result_url) "
                        "VALUES (:id, :approval, :deployment, :conversation, :lineage, "
                        ":request_id, 1, :repo, :status, :base, "
                        "CAST('[\"README.md\"]' AS jsonb), "
                        "'Update README', 'Approved platform publication.', 'github', "
                        ":channel, :result)"
                    ),
                    {
                        "id": publication_id,
                        "approval": approval_id,
                        "deployment": deployment_id,
                        "conversation": item["conversation_id"],
                        "lineage": lineage_id,
                        "request_id": request["id"],
                        "repo": item["repo_full_name"],
                        "status": status,
                        "base": "0123456789abcdef0123456789abcdef01234567",
                        "channel": item["repo_full_name"],
                        "result": pr_url,
                    },
                )
                changed = await conn.execute(
                    text(
                        "UPDATE curie.work_items SET publication_lineage_id = :lineage, "
                        "version = version + 1 WHERE id = :id AND version = :version"
                    ),
                    {
                        "lineage": lineage_id,
                        "id": work_item_id,
                        "version": item["version"],
                    },
                )
                if changed.rowcount != 1:
                    raise AssertionError(f"work item {work_item_id} was not linked")
                if request["id"] is None:
                    raise AssertionError("running request is missing")
        finally:
            await engine.dispose()

    asyncio.run(go())


def test_concurrent_reconcilers_post_one_comment(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9206
    _label(client, github, number)
    github.labels = []
    _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    row = _request(number)

    async def both() -> None:
        import redis.asyncio as aioredis

        engine = create_async_engine(get_settings().database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        clients = [
            aioredis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
            for _ in range(2)
        ]
        reconcilers = [WorkItemReconciler(maker, client, get_settings()) for client in clients]
        try:
            await asyncio.gather(*(item._post_terminal_notices() for item in reconcilers))
        finally:
            for client in clients:
                await client.aclose()
            await engine.dispose()

    asyncio.run(both())
    assert sink.posts == 1
    assert len(_notices(row["id"])) == 1
    assert _notices(row["id"])[0]["posted_at"] is not None


# --- #2798: a revision asked for on the pull request answers on the pull request.

_REVISION_PR = iter(range(501, 600))


def _revision_objective(pr: int, fragment: str) -> str:
    """Line 1 is the canonical URL. Notice routing reads only that line."""

    url = f"https://github.com/{REPO}/pull/{pr}#{fragment}"
    provenance = json.dumps(
        {
            "event": "pull_request_review_comment",
            "url": url,
            "sender": "octocat",
            "body": "@curie please rename the helper",
        }
    )
    return f"{url}\n\nReview feedback asked for another revision.\n{provenance}"


def _work_item_row(work_item_id: uuid.UUID) -> dict[str, Any]:
    return _rows(
        "SELECT w.id, w.conversation_id, w.agent_id, w.github_issue_number, "
        "w.publication_lineage_id, l.deployment_id, l.pr_number "
        "FROM curie.work_items w "
        "JOIN curie.thread_publication_lineages l ON l.id = w.publication_lineage_id "
        "WHERE w.id = :id",
        {"id": work_item_id},
    )[0]


def _insert_revision(work_item_id: uuid.UUID, number: int, objective: str) -> uuid.UUID:
    request_id = uuid.uuid4()

    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO curie.execution_requests "
                        "(id, work_item_id, sequence, status, wait_deadline, objective, "
                        "requester, reply_kind, reply_address, reply_conversation_id) "
                        "VALUES (:id, :work_item, 2, 'waiting', "
                        "clock_timestamp() + interval '30 seconds', :objective, "
                        "'github:6601:octocat', 'github', :repo, :conversation)"
                    ),
                    {
                        "id": request_id,
                        "work_item": work_item_id,
                        "objective": objective,
                        "repo": REPO,
                        "conversation": f"issue-{number}",
                    },
                )
                await conn.execute(
                    text(
                        "UPDATE curie.work_items SET next_sequence = 3, "
                        "version = version + 1 WHERE id = :id"
                    ),
                    {"id": work_item_id},
                )
        finally:
            await engine.dispose()

    asyncio.run(go())
    return request_id


def _attach_revision_publication(work_item_id: uuid.UUID, request_id: uuid.UUID) -> None:
    item = _work_item_row(work_item_id)
    approval_id, publication_id = uuid.uuid4(), uuid.uuid4()
    pr_url = f"https://github.com/{REPO}/pull/{item['pr_number']}"

    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO curie.approvals "
                        "(id, agent_id, conversation_id, author, summary, reply_kind, "
                        "reply_channel, dedupe_key, status, purpose) VALUES "
                        "(:id, :agent, :conversation, 'U0REQUEST1', "
                        "'Publish repository changes', 'github', :channel, :dedupe, "
                        "'approved', 'publication')"
                    ),
                    {
                        "id": approval_id,
                        "agent": item["agent_id"],
                        "conversation": item["conversation_id"],
                        "channel": REPO,
                        "dedupe": f"terminus-revision-{publication_id.hex}",
                    },
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.publications "
                        "(id, approval_id, deployment_id, workspace_conversation_id, "
                        "lineage_id, execution_request_id, revision_number, repo_full_name, "
                        "status, base_sha, changed_paths, title, body, reply_kind, "
                        "reply_channel, result_url) "
                        "VALUES (:id, :approval, :deployment, :conversation, :lineage, "
                        ":request_id, 2, :repo, 'succeeded', :base, "
                        "CAST('[\"README.md\"]' AS jsonb), "
                        "'Rename the helper', 'Approved platform publication.', 'github', "
                        ":channel, :result)"
                    ),
                    {
                        "id": publication_id,
                        "approval": approval_id,
                        "deployment": item["deployment_id"],
                        "conversation": item["conversation_id"],
                        "lineage": item["publication_lineage_id"],
                        "request_id": request_id,
                        "repo": REPO,
                        "base": "0123456789abcdef0123456789abcdef01234567",
                        "channel": REPO,
                        "result": pr_url,
                    },
                )
                await conn.execute(
                    text(
                        "UPDATE curie.thread_publication_lineages SET latest_revision = 2 "
                        "WHERE id = :id"
                    ),
                    {"id": item["publication_lineage_id"]},
                )
        finally:
            await engine.dispose()

    asyncio.run(go())


def _published_issue(client: Any, github: GitHubAPI, sink: _CommentServer) -> tuple[int, int, Any]:
    """An issue whose first run opened a PR and completed without a comment."""

    number = next(_REVISION_ISSUES)
    pr = next(_REVISION_PR)
    _label(client, github, number)
    first = _request(number)
    _start_running(first["id"])
    _attach_publication(first["work_item_id"], status="succeeded", pr=pr)
    _reconcile()
    done = _request(number)
    assert (done["status"], done["terminal_cause"]) == ("completed", "completed")
    assert _notices(first["id"]) == []
    assert sink.posts == 0
    return number, pr, first


_REVISION_ISSUES = iter(range(9301, 9399))


def _complete_revision(
    client: Any, github: GitHubAPI, sink: _CommentServer, fragment: str
) -> tuple[int, int, uuid.UUID]:
    number, pr, first = _published_issue(client, github, sink)
    objective = _revision_objective(pr, fragment)
    revision = _insert_revision(first["work_item_id"], number, objective)
    _start_running(revision)
    _attach_revision_publication(first["work_item_id"], revision)
    _reconcile()
    status = _rows(
        "SELECT status, terminal_cause FROM curie.execution_requests WHERE id = :id",
        {"id": revision},
    )[0]
    assert (status["status"], status["terminal_cause"]) == ("completed", "completed")
    return number, pr, revision


def _posts(sink: _CommentServer) -> list[tuple[str, str | None]]:
    return [(path, body) for method, path, body in sink.requests if method == "POST"]


def test_a_completed_first_request_still_owes_no_comment(admitted: Any) -> None:
    client, github, sink = admitted
    sink.by_path = True
    _published_issue(client, github, sink)
    assert _posts(sink) == []


def test_a_completed_revision_queues_exactly_one_notice(admitted: Any) -> None:
    client, github, sink = admitted
    sink.by_path = True
    _number, _pr, revision = _complete_revision(client, github, sink, "discussion_r88101")
    notices = _notices(revision)
    assert len(notices) == 1
    assert notices[0]["terminal_cause"] == "completed"
    _reconcile()
    assert len(_notices(revision)) == 1


def test_a_review_comment_revision_replies_in_its_thread(admitted: Any) -> None:
    client, github, sink = admitted
    sink.by_path = True
    _number, pr, revision = _complete_revision(client, github, sink, "discussion_r88102")
    posts = _posts(sink)
    assert [path for path, _ in posts] == [
        f"/repos/{REPO}/pulls/{pr}/comments/88102/replies"
    ]
    assert marker_for(revision) in (posts[0][1] or "")
    assert _notices(revision)[0]["posted_at"] is not None
    _reconcile()
    assert len(_posts(sink)) == 1


@pytest.mark.parametrize(
    "fragment", ["issuecomment-88103", "pullrequestreview-88104"]
)
def test_conversation_and_review_revisions_comment_on_the_pull_request(
    admitted: Any, fragment: str
) -> None:
    client, github, sink = admitted
    sink.by_path = True
    number, pr, revision = _complete_revision(client, github, sink, fragment)
    posts = _posts(sink)
    assert [path for path, _ in posts] == [f"/repos/{REPO}/issues/{pr}/comments"]
    assert pr != number
    body = posts[0][1] or ""
    assert f"https://github.com/{REPO}/pull/{pr}#{fragment}" in body
    assert marker_for(revision) in body
    assert _notices(revision)[0]["posted_at"] is not None


def test_a_refused_thread_reply_falls_back_to_a_pull_request_comment(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    sink.by_path = True
    reply = None
    # The PR number is only known once the first run publishes, so refuse every
    # reply path this test could produce.
    for pr in range(501, 600):
        reply = f"/repos/{REPO}/pulls/{pr}/comments/88105/replies"
        sink.refuse_paths[reply] = 422
    _number, pr, revision = _complete_revision(client, github, sink, "discussion_r88105")
    posts = [path for path, _ in _posts(sink)]
    assert posts == [
        f"/repos/{REPO}/pulls/{pr}/comments/88105/replies",
        f"/repos/{REPO}/issues/{pr}/comments",
    ]
    fallback = sink.lists[f"/repos/{REPO}/issues/{pr}/comments"][0]["body"]
    assert marker_for(revision) in fallback
    assert f"https://github.com/{REPO}/pull/{pr}#discussion_r88105" in fallback
    assert _notices(revision)[0]["posted_at"] is not None


@pytest.mark.parametrize(
    ("fragment", "listed"),
    [
        ("discussion_r88106", "pulls/{pr}/comments"),
        ("discussion_r88107", "issues/{pr}/comments"),
        ("issuecomment-88108", "issues/{pr}/comments"),
    ],
)
def test_a_marker_already_on_the_pull_request_is_not_posted_again(
    admitted: Any, fragment: str, listed: str
) -> None:
    """A crash after the post leaves the marker on either PR list. No second post."""

    client, github, sink = admitted
    sink.by_path = True
    number, pr, first = _published_issue(client, github, sink)
    revision = _insert_revision(
        first["work_item_id"], number, _revision_objective(pr, fragment)
    )
    sink.lists[f"/repos/{REPO}/{listed.format(pr=pr)}"] = [
        {"id": 7555, "body": comment_body(revision, "completed")}
    ]
    _start_running(revision)
    _attach_revision_publication(first["work_item_id"], revision)
    _reconcile()
    assert _posts(sink) == []
    notices = _notices(revision)
    assert len(notices) == 1
    assert notices[0]["comment_id"] == 7555
    assert notices[0]["posted_at"] is not None


def test_an_issue_originated_notice_still_comments_on_the_issue(admitted: Any) -> None:
    client, github, sink = admitted
    sink.by_path = True
    number = 9398
    _label(client, github, number)
    github.labels = []
    _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    row = _request(number)
    _reconcile()
    posts = _posts(sink)
    assert [path for path, _ in posts] == [f"/repos/{REPO}/issues/{number}/comments"]
    assert marker_for(row["id"]) in (posts[0][1] or "")
