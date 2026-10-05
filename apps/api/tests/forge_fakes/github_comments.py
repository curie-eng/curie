"""A threaded GitHub REST fake for status comments, labels and CI, with its fixtures."""

from __future__ import annotations

import asyncio
import json
import re
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

import pytest
from curie_api.config import get_settings
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from forge_fakes.github import INSTALLATION_ID, LABEL, REPO, GitHubAPI

# --- #3097: a scripted fake of the GitHub Checks and Statuses APIs.
#
# https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference
# https://docs.github.com/en/rest/commits/statuses#get-the-combined-status-for-a-specific-reference
# https://docs.github.com/en/rest/checks/runs#list-check-run-annotations
#
# Each observation (one check-runs read) pops the next script entry for the
# head SHA it names; the last entry repeats. The combined-status read answers
# from the entry the latest check-runs read popped.

HEAD_A = "a1" * 20
HEAD_B = "b2" * 20
HEAD_C = "c3" * 20
_CHECK_IDS = iter(range(9001, 99999))

CiEntry = tuple[int, Any, int, Any]


def check_run(
    name: str,
    *,
    status: str = "completed",
    conclusion: str | None = "success",
    title: str | None = None,
    summary: str | None = None,
    run_id: int | None = None,
    started_at: str | None = None,
) -> dict[str, Any]:
    return {
        "id": run_id if run_id is not None else next(_CHECK_IDS),
        "name": name,
        "status": status,
        "conclusion": conclusion if status == "completed" else None,
        "started_at": started_at,
        "output": {"title": title, "summary": summary},
    }


def commit_status(context: str, state: str, description: str = "") -> dict[str, Any]:
    return {"context": context, "state": state, "description": description}


def ci_entry(
    *runs: dict[str, Any],
    statuses: tuple[dict[str, Any], ...] = (),
    total: int | None = None,
    check_status: int = 200,
    status_status: int = 200,
) -> CiEntry:
    # The combined state reads "pending" when no status exists; only the
    # statuses list is meaningful (the trap the gate must not fall into).
    combined = "pending" if not statuses else statuses[0]["state"]
    return (
        check_status,
        {"total_count": len(runs) if total is None else total, "check_runs": list(runs)},
        status_status,
        {"state": combined, "statuses": list(statuses), "total_count": len(statuses)},
    )


def ci_green() -> CiEntry:
    return ci_entry(check_run("build"))


def ci_pending() -> CiEntry:
    return ci_entry(check_run("build", status="in_progress"))


def ci_failing(name: str = "unit-tests", *, run_id: int | None = None) -> CiEntry:
    return ci_entry(
        check_run(
            name,
            conclusion="failure",
            title="1 test failed",
            summary="test_widget_parses_input failed: expected 2, got 1",
            run_id=run_id,
        ),
        check_run("lint"),
    )


def ci_empty() -> CiEntry:
    return ci_entry()


class _Credentials:
    app_configured = True

    def token_for_verified_installation(self, repo: str, installation_id: int) -> str:
        if repo != REPO or installation_id != INSTALLATION_ID:
            raise RuntimeError("unexpected installation")
        return "ghs_factory_terminus_fixture"

    def fresh_installation_token(
        self, repo: str, installation_id: int | None = None
    ) -> tuple[int, str]:
        if repo != REPO:
            raise RuntimeError("unexpected repository")
        return 0, "fixture"


_ISSUE_OR_PR = re.compile(r"^/repos/[^/]+/[^/]+/(?:issues|pulls)/(\d+)$")
_COMMENT = re.compile(r"^/repos/[^/]+/[^/]+/(issues|pulls)/comments/(\d+)$")
_LABELS = re.compile(r"^/repos/[^/]+/[^/]+/issues/(\d+)/labels(?:/(.+))?$")


class _GitHubComments(BaseHTTPRequestHandler):
    """Threaded GitHub REST fake for everything the status comment pass calls.

    Comments: create (issue list, thread reply), list, and edit in place
    https://docs.github.com/en/rest/issues/comments#update-an-issue-comment
    https://docs.github.com/en/rest/pulls/comments#update-a-review-comment-for-a-pull-request
    Labels (#3077):
    https://docs.github.com/en/rest/issues/labels#add-labels-to-an-issue
    https://docs.github.com/en/rest/issues/labels#remove-a-label-from-an-issue
    Issue and pull request reads return a title.
    """

    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: object) -> None:
        return

    def _send(self, status: int, payload: object) -> None:
        body = payload.encode() if isinstance(payload, str) else json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _payload(self) -> Any:
        length = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(length) or b"{}")

    def do_GET(self) -> None:  # noqa: N802
        server = self.server
        assert isinstance(server, _CommentServer)
        parsed = urlsplit(self.path)
        path = parsed.path
        if self._ci_get(server, path):
            return
        server.requests.append(("GET", path, None))
        subject = _ISSUE_OR_PR.match(path)
        if subject is not None:
            number = int(subject.group(1))
            self._send(
                200,
                {"number": number, "title": server.titles.get(number, f"Issue {number}")},
            )
            return
        single = _COMMENT.match(path)
        if single is not None:
            found = server.find(int(single.group(2)))
            if found is None:
                self._send(404, {"message": "Not Found"})
            else:
                self._send(200, found)
            return
        if server.by_path:
            items = list(server.lists.get(path, []))
            # Real GitHub pages; a small fixture list is unaffected since page 1
            # at per_page 100 already covers it.
            params = parse_qs(parsed.query)
            page = int(params.get("page", ["1"])[0])
            per_page = int(params.get("per_page", ["100"])[0])
            start = (page - 1) * per_page
            self._send(200, items[start : start + per_page])
            return
        self._send(200, list(server.comments))

    def _ci_get(self, server: _CommentServer, path: str) -> bool:
        repo = re.escape(REPO)
        commit = re.fullmatch(rf"/repos/{repo}/commits/([0-9a-f]{{40}})/(check-runs|status)", path)
        if commit is not None:
            sha, kind = commit.group(1), commit.group(2)
            script = server.ci_scripts.get(sha) or server.ci_script
            if kind == "check-runs":
                index = min(server.ci_cursor.get(sha, -1) + 1, len(script) - 1)
                server.ci_cursor[sha] = index
                server.ci_observations.append(sha)
                self._send(script[index][0], script[index][1])
            else:
                index = max(server.ci_cursor.get(sha, 0), 0)
                self._send(script[index][2], script[index][3])
            return True
        annotations = re.fullmatch(rf"/repos/{repo}/check-runs/([0-9]+)/annotations", path)
        if annotations is not None:
            self._send(200, server.annotations.get(int(annotations.group(1)), []))
            return True
        branch = re.fullmatch(rf"/repos/{repo}/branches/(.+)", path)
        if branch is not None:
            # Get a branch; the base head's checks are then served by ci_scripts.
            # https://docs.github.com/en/rest/branches/branches#get-a-branch
            name = unquote(branch.group(1))
            server.requests.append(("GET", path, None))
            sha = server.branches.get(name)
            if sha is None:
                self._send(404, {"message": "Branch not found"})
            else:
                self._send(200, {"name": name, "commit": {"sha": sha}})
            return True
        return False

    def do_PATCH(self) -> None:  # noqa: N802
        server = self.server
        assert isinstance(server, _CommentServer)
        payload = self._payload()
        path = self.path.split("?", 1)[0]
        server.requests.append(("PATCH", path, payload.get("body", "")))
        edit = _COMMENT.match(path)
        if edit is None:
            self._send(404, {"message": "Not Found"})
            return
        if server.patch_statuses:
            self._send(server.patch_statuses.pop(0), {"message": "injected"})
            return
        found = server.find(int(edit.group(2)))
        if found is None:
            self._send(404, {"message": "Not Found"})
            return
        found["body"] = payload.get("body", "")
        self._send(200, found)

    def do_DELETE(self) -> None:  # noqa: N802
        server = self.server
        assert isinstance(server, _CommentServer)
        path = self.path.split("?", 1)[0]
        server.requests.append(("DELETE", path, None))
        label = _LABELS.match(path)
        if label is None or label.group(2) is None:
            self._send(404, {"message": "Not Found"})
            return
        number, name = int(label.group(1)), unquote(label.group(2))
        present = server.issue_labels.setdefault(number, set())
        if name not in present:
            self._send(404, {"message": "Label does not exist"})
            return
        present.discard(name)
        self._send(200, [{"name": value} for value in sorted(present)])

    def do_POST(self) -> None:  # noqa: N802
        server = self.server
        assert isinstance(server, _CommentServer)
        payload = self._payload()
        path = self.path.split("?", 1)[0]
        label = _LABELS.match(path)
        if label is not None and label.group(2) is None:
            names = [str(name) for name in payload.get("labels", [])]
            server.requests.append(("POST", path, json.dumps(names)))
            present = server.issue_labels.setdefault(int(label.group(1)), set())
            present.update(names)
            self._send(200, [{"name": value} for value in sorted(present)])
            return
        rerun = re.fullmatch(
            rf"/repos/{re.escape(REPO)}/actions/runs/([0-9]+)/rerun-failed-jobs", path
        )
        if rerun is not None:
            # Re-run failed jobs in one workflow run. 201 Created on success.
            # Default 403 so a suite that does not opt in keeps today's path.
            # https://docs.github.com/en/rest/actions/workflow-runs#re-run-failed-jobs-from-a-workflow-run
            run_id = int(rerun.group(1))
            server.reruns.append(run_id)
            server.requests.append(("POST", path, None))
            if path in server.lost_response_paths:
                server.lost_response_paths.discard(path)
                self.close_connection = True
                return
            status = (
                server.rerun_statuses.pop(0) if server.rerun_statuses else server.rerun_status
            )
            self._send(status, {} if status == 201 else {"message": "refused"})
            return
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
            if path in server.lost_response_paths:
                # The comment lands, but the caller never sees the response: drop
                # the connection instead of sending a status line, once.
                server.lost_response_paths.discard(path)
                self.close_connection = True
                return
            self._send(201, comment)
            return
        if server.refuse_status is not None:
            server.posts += 1
            self._send(server.refuse_status, {"message": "refused"})
            return
        server.next_comment_id += 1
        comment = {"id": server.next_comment_id, "body": payload.get("body", "")}
        server.comments.append(comment)
        server.posts += 1
        self._send(201, comment)


class _CommentServer(ThreadingHTTPServer):
    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), _GitHubComments)
        self.comments: list[dict[str, Any]] = []
        self.next_comment_id = 7000
        self.posts = 0
        self.refuse_status: int | None = None
        # Path-aware mode for pull request replies. Each list endpoint keeps its
        # own comments, and a review-thread reply lands in the PR review list.
        self.by_path = False
        self.lists: dict[str, list[dict[str, Any]]] = {}
        self.refuse_paths: dict[str, int] = {}
        self.requests: list[tuple[str, str, str | None]] = []
        # Paths whose next POST response is dropped after the comment is
        # recorded, simulating a lost response to a successful post.
        self.lost_response_paths: set[str] = set()
        # #3097 CI fake. The default is one green check run, so every suite that
        # opens a pull request stays on the success path.
        self.ci_script: list[CiEntry] = [ci_green()]
        self.ci_scripts: dict[str, list[CiEntry]] = {}
        self.ci_cursor: dict[str, int] = {}
        self.ci_observations: list[str] = []
        self.annotations: dict[int, list[dict[str, Any]]] = {}
        # #4105. Branch name -> the sha it points to; empty means every base read 404s.
        self.branches: dict[str, str] = {}
        # #3741. 403 matches a token with no Actions write permission.
        self.rerun_status = 403
        self.rerun_statuses: list[int] = []
        self.reruns: list[int] = []

        # Statuses the next PATCHes answer with instead of editing (#3077).
        self.patch_statuses: list[int] = []
        # Issue number -> label names currently on it.
        self.issue_labels: dict[int, set[str]] = {}
        # Issue or pull request number -> title the subject read returns.
        self.titles: dict[int, str] = {}

    def find(self, comment_id: int) -> dict[str, Any] | None:
        for comment in self.comments:
            if comment.get("id") == comment_id:
                return comment
        for listed in self.lists.values():
            for comment in listed:
                if comment.get("id") == comment_id:
                    return comment
        return None

    def delete_comment(self, comment_id: int) -> None:
        """A human deletes the comment on GitHub."""

        self.comments[:] = [c for c in self.comments if c.get("id") != comment_id]
        for listed in self.lists.values():
            listed[:] = [c for c in listed if c.get("id") != comment_id]


@pytest.fixture
def comments(monkeypatch: pytest.MonkeyPatch, clean_db: None) -> Any:
    """@spec apps/api/README.md#factory-test-isolation"""
    server = _CommentServer()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        monkeypatch.setenv("GITHUB_API_URL", f"http://{host}:{port}")
        monkeypatch.setenv("CURIE_WORK_ITEM_WAIT_BUDGET_SECONDS", "30")
        monkeypatch.setenv("CURIE_WORK_ITEM_RECONCILER_ENABLED", "false")
        monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")
        monkeypatch.setenv("APPROVAL_SWEEP_INTERVAL_S", "0")
        monkeypatch.setenv("DEAD_LETTER_WATCH_INTERVAL_S", "0")
        monkeypatch.setenv("GITHUB_FACTORY_INGRESS_ENABLED", "true")
        # These tests drive signed deliveries. Poll mode would also read the
        # GitHub stand in on every reconciler pass.
        monkeypatch.setenv("GITHUB_FACTORY_INTAKE", "webhook")
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
        monkeypatch.setattr(
            "curie_api.forges.github.ci.credentials_for",
            lambda _settings: _Credentials(),
        )
        yield server
    finally:
        try:
            _clear_ci_keys()
        finally:
            try:
                server.shutdown()
                thread.join(timeout=5)
            finally:
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


def _clear_ci_keys() -> None:
    """@spec apps/api/README.md#factory-test-isolation

    The disposable database records this fixture's requests. Other workers
    share Valkey, so only these exact request namespaces belong to cleanup.
    """

    import redis

    request_ids = [str(row["id"]) for row in _rows("SELECT id FROM curie.execution_requests")]
    if not request_ids:
        return
    client = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
    try:
        for request_id in request_ids:
            for prefix in ("curie:work-item:ci", "curie:work-item:ci-rerun"):
                keys = list(client.scan_iter(f"{prefix}:{request_id}:*"))
                if keys:
                    client.delete(*keys)
    finally:
        client.close()
