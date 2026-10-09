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
import re
import sys
import threading
import uuid
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api.config import get_settings
from curie_api.factory_notices import FINAL_MARKER, cause_text, marker_for, result_section
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


def test_completed_issue_comment_body_requires_pull_request_url() -> None:
    with pytest.raises(ValueError, match="requires its pull request URL"):
        result_section("completed", pr_url=None)


def test_failed_comment_leads_with_a_plain_sentence_not_the_cause_code() -> None:
    body = result_section(
        "model_credit_exhausted",
        pr_url=None,
        detail="API Error: 402 This request requires more credits",
    )
    headline = body.splitlines()[0]
    assert headline.startswith("Could not complete: the model provider refused")
    assert "run out of credits" in headline
    assert "model_credit_exhausted" not in headline
    assert "Provider message: API Error: 402 This request requires more credits" in body
    assert "Cause: model_credit_exhausted" in body


@pytest.mark.parametrize(
    "cause",
    [
        "model_credit_exhausted",
        "model_usage_limited",
        "model_credential_rejected",
        "model_rate_limited",
        "model_error",
        "model_unreachable",
        "budget_exceeded",
        "runner_timeout",
        "workspace_error",
        "runner_escalated",
        "pull_request_not_adopted",
        "unclassified",
        "max_turns",
        "runner_failed",
        "approval_create_failed",
        "no_pull_request",
        "execution_deadline",
        "capacity_wait_expired",
        "owner_lost",
        "issue_cancelled",
        "publication_denied",
        "publication_expired",
        "publication_failed",
        "ci_failed",
        "ci_timeout",
        "ci_unverified",
        "merge_conflict",
        "ci_fix_unpublished",
    ],
)
def test_every_terminus_cause_has_its_own_plain_sentence(cause: str) -> None:
    assert cause_text(cause) != cause_text("not-a-cause")
    assert cause not in cause_text(cause)


_MODEL_UNREACHABLE_SENTENCE = (
    "the model provider could not be reached. Check the runner's network path to the "
    "model endpoint, then retry."
)


def test_model_unreachable_result_section_names_the_network_path_and_its_class() -> None:
    body = result_section(
        "model_unreachable",
        pr_url=None,
        detail="model error: server_error: API Error: Connection refused (ECONNREFUSED)",
    )
    headline = body.splitlines()[0]
    assert headline == f"Could not complete: {_MODEL_UNREACHABLE_SENTENCE}"
    assert (
        "Provider message: model error: server_error: API Error: Connection refused "
        "(ECONNREFUSED)" in body
    )
    assert "Cause: model_unreachable" in body
    assert "Failure class: model-unreachable" in body


def test_the_result_section_carries_no_marker() -> None:
    """The marker belongs to the whole status comment, not to its result lines."""

    body = result_section("runner_failed", pr_url=None)
    assert "curie-execution-request" not in body
    assert FINAL_MARKER not in body


def test_superseded_cancel_says_a_new_run_replaced_this_one() -> None:
    body = result_section("issue_cancelled", pr_url=None, superseded=True)
    assert body == (
        "Stopped: the label was added again, so a new run replaced this one.\n"
        "Cause: issue_cancelled\n"
    )


def test_unknown_cause_still_gets_a_sentence_and_its_code() -> None:
    body = result_section("something_new", pr_url=None)
    assert body.splitlines()[0] == (
        "Could not complete: the run stopped for a reason Curie did not recognize."
    )
    assert "Provider message" not in body
    assert "Cause: something_new" in body


def test_ci_failed_notice_labels_its_details_not_a_provider_message() -> None:
    detail = (
        'Rounds: 3\nTried: round 2: "Fix the test" (1 files: src/app.py)\n'
        "Failing checks: unit-tests (failure)"
    )
    body = result_section("ci_failed", pr_url=None, detail=detail)
    assert body.startswith("Could not complete:")
    assert "3 rounds" in body.splitlines()[0]
    assert "Provider message:" not in body
    assert "Details: Rounds: 3" in body
    assert "Failing checks: unit-tests (failure)" in body
    assert "Cause: ci_failed" in body


def test_ci_unverified_notice_says_it_is_not_a_success() -> None:
    body = result_section("ci_unverified", pr_url=None, detail="Reason: github_forbidden")
    assert body.startswith("Could not complete:")
    assert "CI could not be verified" in body.splitlines()[0]
    assert "Reason: github_forbidden" in body
    assert "Completed:" not in body


def test_completed_notice_carries_the_no_ci_note() -> None:
    url = f"https://github.com/{REPO}/pull/77"
    body = result_section("completed", pr_url=url, detail="No CI checks appeared within 120 s.")
    assert body.startswith(f"Completed: {url}")
    assert "Note: No CI checks appeared within 120 s." in body


def test_revision_completed_notice_carries_the_no_ci_note() -> None:
    url = f"https://github.com/{REPO}/pull/77"
    body = result_section(
        "completed",
        pr_url=url,
        feedback_url=f"{url}#issuecomment-1",
        detail="No CI checks appeared within 120 s.",
    )
    assert body.startswith("The requested revision is pushed")
    assert "Note: No CI checks appeared within 120 s." in body


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

    def _send(self, status: int, payload: object, *, headers: dict[str, str] | None = None) -> None:
        body = payload.encode() if isinstance(payload, str) else json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # Cancelling a lifespan task can abandon an in-flight HTTP read.
            return

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
        barrier = server.get_barrier
        if barrier is not None and path == barrier[0]:
            # The HTTP fake waits on the test's event loop, keeping real HTTP
            # and database operations in flight until the test releases it.
            server.get_barrier = None
            _, loop, entered, released = barrier
            loop.call_soon_threadsafe(entered.set)
            asyncio.run_coroutine_threadsafe(released.wait(), loop).result(timeout=15)
        label = _LABELS.match(path)
        if label is not None and label.group(2) is None:
            # List labels for an issue:
            # https://docs.github.com/en/rest/issues/labels#list-labels-for-an-issue
            # Pagination uses the provider's Link relation:
            # https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api
            number = int(label.group(1))
            params = parse_qs(parsed.query)
            page = int(params.get("page", ["1"])[0])
            per_page = int(params.get("per_page", ["30"])[0])
            server.label_pages.append((number, page))
            status = server.label_page_statuses.get((number, page), 200)
            if status != 200:
                self._send(status, {"message": "injected label list refusal"})
                return
            names = sorted(server.issue_labels.get(number, set()))
            start = (page - 1) * per_page
            headers = {}
            if start + per_page < len(names):
                host, port = server.server_address
                next_page = f"http://{host}:{port}{path}?per_page={per_page}&page={page + 1}"
                headers["Link"] = f'<{next_page}>; rel="next"'
            self._send(
                200,
                [{"name": value} for value in names[start : start + per_page]],
                headers=headers,
            )
            return
        subject = _ISSUE_OR_PR.match(path)
        if subject is not None:
            number = int(subject.group(1))
            payload: dict[str, Any] = {
                "number": number,
                "title": server.titles.get(number, f"Issue {number}"),
            }
            if "/pulls/" in path:
                # GitHub computes mergeability asynchronously and returns null:
                # https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request
                index = min(len(server.pull_observations), len(server.pull_script) - 1)
                server.pull_observations.append(number)
                payload.update(server.pull_script[index])
            self._send(
                200,
                payload,
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
            status = server.rerun_statuses.pop(0) if server.rerun_statuses else server.rerun_status
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
        # #4263. Pull reads default to a mergeable head; scripts can replay the
        # provider's null computation window or a dirty head without checks.
        self.pull_script: list[dict[str, Any]] = [
            {"mergeable": True, "mergeable_state": "clean", "merged": False}
        ]
        self.pull_observations: list[int] = []
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
        self.label_pages: list[tuple[int, int]] = []
        self.label_page_statuses: dict[tuple[int, int], int] = {}
        # Issue or pull request number -> title the subject read returns.
        self.titles: dict[int, str] = {}
        self.get_barrier: (
            tuple[str, asyncio.AbstractEventLoop, asyncio.Event, asyncio.Event] | None
        ) = None

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
            "curie_api.workitem_outcomes.credentials_for",
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
        "comment_id, comment_list, finalized_at, card_token, applied_label, "
        "refused_at, refusal, detail "
        "FROM curie.factory_terminal_notices WHERE execution_request_id = :id",
        {"id": request_id},
    )


def _label(client: Any, github: GitHubAPI, number: int) -> None:
    github.issue_number = number
    github.advance_label_event(number)
    response = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "factory_admitted"


def _reconcile() -> None:
    async def go() -> None:
        import redis.asyncio as aioredis

        engine = create_async_engine(get_settings().database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        client = aioredis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
        reconciler = WorkItemReconciler(maker, client, get_settings())
        try:
            await reconciler.run_once()
            await reconciler._sync_status_comments()
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
                acquired = await acquire(session, request_id, owner="factory-owner", generation=1)
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
    body = sink.comments[0]["body"]
    assert body.startswith("Could not complete:")
    assert "Cause: capacity_wait_expired" in body
    assert marker_for(row["id"]) in body
    assert FINAL_MARKER in body
    assert notices[0]["finalized_at"] is not None
    _reconcile()
    assert sink.posts == 1
    assert len(_notices(row["id"])) == 1


def test_label_removal_posts_one_comment(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9202
    _label(client, github, number)
    github.labels = []
    removed = _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    assert removed.json()["status"] == "factory_cancelled"
    row = _request(number)
    assert (row["status"], row["terminal_cause"]) == ("cancelled", "issue_cancelled")
    assert _notices(row["id"])[0]["posted_at"] is None
    _reconcile()
    assert sink.posts == 1
    assert "Stopped:" in sink.comments[0]["body"]
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
    _assert_one_final_comment([c["body"] for c in sink.comments], row["id"])
    assert _request(number)["version"] == version


def _unadopted_pr_lineages(work_item_id: uuid.UUID, *, open_pr: bool, html_base: str) -> uuid.UUID:
    """An earlier request's PR is deliberately unlinked from the work item."""

    async def go() -> uuid.UUID:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                item = (
                    (
                        await conn.execute(
                            text(
                                "SELECT agent_id, conversation_id, repo_full_name "
                                "FROM curie.work_items WHERE id = :id"
                            ),
                            {"id": work_item_id},
                        )
                    )
                    .mappings()
                    .one()
                )
                version_id, deployment_id = uuid.uuid4(), uuid.uuid4()
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
                # All offsets are distinct, so newest never depends on insertion
                # order. A no-PR lineage and other scopes are newer than every
                # eligible PR; neither can become the actionable link.
                cases = [
                    (71, "closed", 4, item["conversation_id"], item["repo_full_name"]),
                    (
                        72,
                        "open" if open_pr else "merged",
                        3,
                        item["conversation_id"],
                        item["repo_full_name"],
                    ),
                    (73, "closed", 2, item["conversation_id"], item["repo_full_name"]),
                    (None, "closed", 1, item["conversation_id"], item["repo_full_name"]),
                    (74, "open", 0, f"other-{uuid.uuid4().hex}", item["repo_full_name"]),
                    (75, "open", 0, item["conversation_id"], "acme-corp/other"),
                ]
                for pr, status, age, conversation, repo in cases:
                    lineage_id = uuid.uuid4()
                    await conn.execute(
                        text(
                            "INSERT INTO curie.thread_publication_lineages "
                            "(id, agent_id, deployment_id, conversation_id, repo_full_name, "
                            "base_sha, branch, pr_number, pr_url, head_sha, status, "
                            "version, latest_revision, created_at) VALUES "
                            "(:id, :agent, :deployment, :conversation, :repo, :base, "
                            ":branch, :pr, :url, :head, :status, 1, 1, "
                            "clock_timestamp() - :age * interval '1 day')"
                        ),
                        {
                            "id": lineage_id,
                            "agent": item["agent_id"],
                            "deployment": deployment_id,
                            "conversation": conversation,
                            "repo": repo,
                            "base": "a" * 40,
                            "branch": f"curie/publication-{lineage_id.hex}",
                            "pr": pr,
                            "url": None if pr is None else f"{html_base}/{repo}/pull/{pr}",
                            "head": None if pr is None else HEAD_A,
                            "status": status,
                            "age": age,
                        },
                    )
                return deployment_id
        finally:
            await engine.dispose()

    return asyncio.run(go())


@pytest.mark.parametrize(("open_pr", "selected_pr"), [(True, 72), (False, 73)])
def test_an_unadopted_pr_notice_names_the_database_selected_conversation_pr(
    admitted: Any, open_pr: bool, selected_pr: int
) -> None:
    client, github, sink = admitted
    number = 9300
    _label(client, github, number)
    row = _request(number)
    epoch = _start_running(row["id"])
    # The comments fixture configures this HTTP server as the forge API. Its
    # canonical HTML origin is that same configured origin, as required by
    # #3562, rather than public github.com.
    host, port = sink.server_address
    html_base = f"http://{host}:{port}"
    deployment_id = _unadopted_pr_lineages(
        row["work_item_id"], open_pr=open_pr, html_base=html_base
    )
    unlinked = _rows(
        "SELECT publication_lineage_id FROM curie.work_items WHERE id = :id",
        {"id": row["work_item_id"]},
    )
    assert unlinked == [{"publication_lineage_id": None}]
    headers = {"X-Curie-Worker-Token": "factory-terminus-worker"}
    refused = client.post(
        "/v1/internal/publications/precheck/context",
        headers=headers,
        json={
            "deployment_id": str(deployment_id),
            "work_item_id": str(row["work_item_id"]),
            "execution_request_id": str(row["id"]),
            "runtime_epoch": epoch,
            "queued_event_id": f"work-item-{row['id']}-execute-1",
        },
    )
    assert refused.status_code == 409, refused.text
    assert refused.json()["detail"]["code"] == "pull_request_not_adopted"
    worker_url = "https://attacker.example.com/pull/999"
    failed = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/finish",
        headers=headers,
        json={
            "runtime_epoch": epoch,
            "outcome": "failed",
            "cause": "pull_request_not_adopted",
            "detail": f"Continue this pull request: {worker_url}",
        },
    )
    assert failed.status_code == 200, failed.text

    _reconcile()
    _reconcile()

    assert (_request(number)["status"], _request(number)["terminal_cause"]) == (
        "failed",
        "pull_request_not_adopted",
    )
    body = _assert_one_final_comment([comment["body"] for comment in sink.comments], row["id"])
    assert f"{html_base}/{REPO}/pull/{selected_pr}" in body
    for other_pr in {71, 72, 73, 74} - {selected_pr}:
        assert f"{html_base}/{REPO}/pull/{other_pr}" not in body
    assert f"{html_base}/acme-corp/other/pull/75" not in body
    assert "Cause: pull_request_not_adopted" in body
    assert "earlier pull request" in body
    assert "close or merge" in body
    assert "label" in body
    # Supplied detail may be omitted or fenced as inert text, but must never
    # become a link in the public status comment. The selected link is DB truth.
    actionable = re.sub(r"(?ms)^`{3,}[^\n]*\n.*?^`{3,}\s*$", "", body)
    assert worker_url not in actionable
    assert _notices(row["id"])[0]["finalized_at"] is not None
    assert sink.posts == 1


def test_approval_create_failure_posts_terminal_issue_notice_and_clears_running_label(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 9294
    _label(client, github, number)
    row = _request(number)
    epoch = _start_running(row["id"])
    sink.issue_labels.setdefault(number, set()).add("curie:running")

    failed = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/finish",
        headers={"X-Curie-Worker-Token": "factory-terminus-worker"},
        json={
            "runtime_epoch": epoch,
            "outcome": "failed",
            "cause": "approval_create_failed",
        },
    )
    assert failed.status_code == 200, failed.text
    _reconcile()

    assert _request(number)["terminal_cause"] == "approval_create_failed"
    assert sink.posts == 1
    assert sink.comments[0]["body"].startswith("Could not complete:")
    assert "curie:running" not in sink.issue_labels[number]
    assert "curie-factory:needs-human" in sink.issue_labels[number]


def test_approval_create_failure_after_first_publication_ends_ci_fix_round(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 9295
    _label(client, github, number)
    row = _request(number)
    epoch = _start_running(row["id"])
    _attach_publication(row["work_item_id"], status="succeeded", pr=4295)
    sink.issue_labels.setdefault(number, set()).add("curie:running")

    failed = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/finish",
        headers={"X-Curie-Worker-Token": "factory-terminus-worker"},
        json={
            "runtime_epoch": epoch,
            "outcome": "failed",
            "cause": "approval_create_failed",
        },
    )
    assert failed.status_code == 200, failed.text
    _reconcile()

    assert _request(number)["terminal_cause"] == "approval_create_failed"
    assert sink.comments[0]["body"].startswith("Could not complete:")
    assert "curie:running" not in sink.issue_labels[number]


def test_credit_exhausted_finish_comments_the_redacted_provider_message(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 9213
    _label(client, github, number)
    row = _request(number)
    epoch = _start_running(row["id"])
    key = "sk-or-v1-" + "0123456789abcdef" * 4
    failed = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/finish",
        headers={"X-Curie-Worker-Token": "factory-terminus-worker"},
        json={
            "runtime_epoch": epoch,
            "outcome": "failed",
            "cause": "model_credit_exhausted",
            "detail": f"model error: unknown: API Error: 402 requires more credits {key}",
        },
    )
    assert failed.status_code == 200, failed.text
    notice = _notices(row["id"])[0]
    assert key not in notice["detail"]
    assert "[REDACTED" in notice["detail"]
    _reconcile()
    assert sink.posts == 1
    body = sink.comments[0]["body"]
    assert body.startswith("Could not complete: the model provider refused")
    assert "Provider message: model error: unknown: API Error: 402 requires more credits" in body
    assert key not in body


@pytest.mark.parametrize(("number", "cause"), [(9291, "early_stop"), (9292, "no_pull_request")])
def test_an_unpublished_finish_comments_the_agents_redacted_last_message(
    admitted: Any, number: int, cause: str
) -> None:
    """#3128: the agent's final message survives on the notice row and the comment."""

    client, github, sink = admitted
    _label(client, github, number)
    row = _request(number)
    epoch = _start_running(row["id"])
    key = "sk-or-v1-" + "0123456789abcdef" * 4
    detail = f"I read the issue and stopped. <!-- curie-status:final --> @octocat {key}"
    finished = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/finish",
        headers={"X-Curie-Worker-Token": "factory-terminus-worker"},
        json={"runtime_epoch": epoch, "outcome": "failed", "cause": cause, "detail": detail},
    )
    assert finished.status_code == 200, finished.text
    terminal = _request(number)
    assert (terminal["status"], terminal["terminal_cause"]) == ("failed", cause)
    notice = _notices(row["id"])[0]
    assert notice["detail"] is not None
    assert "I read the issue and stopped." in notice["detail"]
    assert key not in notice["detail"]
    assert "[REDACTED" in notice["detail"]
    _reconcile()
    assert sink.posts == 1
    body = sink.comments[0]["body"]
    assert body.startswith("Could not complete:")
    assert "Agent's last message:" in body
    assert "I read the issue and stopped." in body
    assert key not in body
    assert f"Cause: {cause}" in body
    # The model's copy of the final marker is broken; the platform's own is the
    # only one, so the comment is still recognised as exactly one final notice.
    assert body.count(FINAL_MARKER) == 1
    _assert_one_final_comment([c["body"] for c in sink.comments], row["id"])


@pytest.mark.parametrize("cause", ["early_stop", "approval_create_failed"])
def test_an_early_stop_finish_defers_to_an_in_flight_publication(admitted: Any, cause: str) -> None:
    """#3128: like ``no_pull_request``, publication owns the terminus."""

    client, github, sink = admitted
    number = 9293
    _label(client, github, number)
    row = _request(number)
    epoch = _start_running(row["id"])
    _attach_publication(row["work_item_id"], status="pending", pr=None)

    finished = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/finish",
        headers={"X-Curie-Worker-Token": "factory-terminus-worker"},
        json={
            "runtime_epoch": epoch,
            "outcome": "failed",
            "cause": cause,
            "detail": "stopping",
        },
    )

    assert finished.status_code == 409, finished.text
    assert "publication_pending" in finished.text
    assert _request(number)["status"] == "running"


def _requests(number: int) -> list[dict[str, Any]]:
    return _rows(
        "SELECT r.id, r.status, r.terminal_cause, w.id AS work_item_id "
        "FROM curie.execution_requests r "
        "JOIN curie.work_items w ON w.id = r.work_item_id "
        "WHERE w.github_repository_id = :repo AND w.github_issue_number = :number "
        "ORDER BY r.created_at",
        {"repo": REPO_ID, "number": number},
    )


def _relabelled_after_publication(client: Any, github: GitHubAPI, number: int) -> dict[str, Any]:
    """#4158: request 1 published a PR and was cancelled; request 2 is running."""

    _label(client, github, number)
    first = _request(number)
    _start_running(first["id"])
    _attach_publication(first["work_item_id"], status="succeeded", pr=number)
    github.labels = []
    removed = _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    assert removed.json()["status"] == "factory_cancellation_requested"
    _observe_termination(client, first["id"])
    github.labels = [LABEL]
    _label(client, github, number)
    first_row, second = _requests(number)
    assert first_row["id"] == first["id"]
    assert (first_row["status"], first_row["terminal_cause"]) == ("cancelled", "issue_cancelled")
    _start_running(second["id"])
    return second


def _finish_unpublished(
    client: Any,
    request_id: uuid.UUID,
    *,
    cause: str,
    detail: str,
) -> Any:
    epoch = _rows(
        "SELECT runtime_epoch FROM curie.execution_requests WHERE id = :id", {"id": request_id}
    )[0]["runtime_epoch"]
    return client.post(
        f"/v1/internal/work-items/requests/{request_id}/finish",
        headers={"X-Curie-Worker-Token": "factory-terminus-worker"},
        json={
            "runtime_epoch": epoch,
            "outcome": "failed",
            "cause": cause,
            "detail": detail,
        },
    )


def test_a_relabelled_request_without_its_own_publication_ends_failed(
    admitted: Any,
) -> None:
    """#4158: an earlier request's settled PR does not own this request's terminus."""

    client, github, sink = admitted
    number = 9296
    second = _relabelled_after_publication(client, github, number)

    finished = _finish_unpublished(
        client,
        second["id"],
        cause="no_pull_request",
        detail="I read the issue and stopped.",
    )

    assert finished.status_code == 200, finished.text
    rows = {row["id"]: row for row in _requests(number)}
    assert (rows[second["id"]]["status"], rows[second["id"]]["terminal_cause"]) == (
        "failed",
        "no_pull_request",
    )
    notices = _notices(second["id"])
    assert len(notices) == 1
    assert notices[0]["terminal_cause"] == "no_pull_request"
    assert "I read the issue and stopped." in notices[0]["detail"]


def test_a_relabelled_request_defers_to_its_own_in_flight_publication(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 9297
    second = _relabelled_after_publication(client, github, number)
    _attach_revision_publication(second["work_item_id"], second["id"], status="pending")

    finished = _finish_unpublished(
        client,
        second["id"],
        cause="no_pull_request",
        detail="I read the issue and stopped.",
    )

    assert finished.status_code == 409, finished.text
    assert "publication_pending" in finished.text
    rows = {row["id"]: row for row in _requests(number)}
    assert rows[second["id"]]["status"] == "running"


def test_a_relabelled_request_defers_to_an_earlier_in_flight_publication_on_its_lineage(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 9299
    second = _relabelled_after_publication(client, github, number)
    first_id = _requests(number)[0]["id"]

    async def reopen() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                changed = await conn.execute(
                    text(
                        "UPDATE curie.publications SET status = 'running', terminal_at = NULL "
                        "WHERE execution_request_id = :id"
                    ),
                    {"id": first_id},
                )
                assert changed.rowcount == 1
        finally:
            await engine.dispose()

    asyncio.run(reopen())

    finished = _finish_unpublished(
        client,
        second["id"],
        cause="no_pull_request",
        detail="I read the issue and stopped.",
    )

    assert finished.status_code == 409, finished.text
    assert "publication_pending" in finished.text
    rows = {row["id"]: row for row in _requests(number)}
    assert rows[second["id"]]["status"] == "running"


def test_a_request_whose_own_publication_succeeded_defers_an_unpublished_finish(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 9298
    _label(client, github, number)
    row = _request(number)
    _start_running(row["id"])
    _attach_publication(row["work_item_id"], status="succeeded", pr=number)

    finished = _finish_unpublished(
        client,
        row["id"],
        cause="no_pull_request",
        detail="I read the issue and stopped.",
    )

    assert finished.status_code == 409, finished.text
    assert "publication_pending" in finished.text
    assert _request(number)["status"] == "running"


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
        {
            "id": 7444,
            "body": result_section("issue_cancelled", pr_url=None)
            + f"\n{FINAL_MARKER}\n{marker_for(row['id'])}\n",
        }
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
    removed = _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    assert removed.json()["status"] == "factory_cancellation_requested"
    requested = _request(number)
    assert requested["status"] == "cancellation_requested"
    # The status row exists from admission; no terminal detail is staged yet.
    assert _notices(row["id"])[0]["terminal_cause"] is None
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
    assert "Stopped:" in sink.comments[0]["body"]
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
    assert _notices(row["id"])[0]["terminal_cause"] is None
    assert FINAL_MARKER not in sink.comments[0]["body"]
    _observe_termination(client, row["id"])
    terminal = _request(number)
    assert (terminal["status"], terminal["terminal_cause"]) == (
        "expired",
        "execution_deadline",
    )
    _reconcile()
    assert sink.posts == 1
    assert "execution_deadline" in sink.comments[0]["body"]
    _assert_one_final_comment([c["body"] for c in sink.comments], row["id"])
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
                        "- CAST(:elapsed AS interval) "
                        "WHERE id = :id AND status = 'running'"
                    ),
                    {
                        "id": row["id"],
                        "elapsed": timedelta(
                            seconds=get_settings().work_item_runtime_ttl_seconds + 5
                        ),
                    },
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
    assert _notices(row["id"])[0]["terminal_cause"] is None
    _observe_termination(client, row["id"])
    lost = [r for r in _requests(number) if r["id"] == row["id"]]
    assert len(lost) == 1, lost
    assert (lost[0]["status"], lost[0]["terminal_cause"]) == ("failed", "owner_lost")
    successors = [r for r in _requests(number) if r["id"] != row["id"]]
    assert len(successors) == 1, successors
    assert successors[0]["status"] == "waiting"
    _reconcile()
    final = _assert_one_final_comment([c["body"] for c in sink.comments], row["id"])
    assert "Cause: owner_lost" in final
    assert "(attempt 2 of 3)" in final


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


def test_failure_and_opened_pull_request_each_post_one_final_comment(
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
    assert sink.posts == 2
    bodies = [comment["body"] for comment in sink.comments]
    assert sum("Cause: publication_denied" in body for body in bodies) == 1
    opened_url = f"https://github.com/{REPO}/pull/77"
    assert sum(opened_url in body for body in bodies) == 1
    _assert_one_final_comment(bodies, opened_row["id"])
    _assert_one_final_comment(bodies, denied_row["id"])
    assert len(_notices(opened_row["id"])) == 1
    assert len(_notices(denied_row["id"])) == 1


def _attach_publication(
    work_item_id: uuid.UUID, *, status: str, pr: int | None, head_sha: str = HEAD_A
) -> None:
    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                item = (
                    (
                        await conn.execute(
                            text(
                                "SELECT agent_id, conversation_id, repo_full_name, "
                                "github_repository_id, github_installation_id, version "
                                "FROM curie.work_items WHERE id = :id"
                            ),
                            {"id": work_item_id},
                        )
                    )
                    .mappings()
                    .one()
                )
                request = (
                    (
                        await conn.execute(
                            text(
                                "SELECT id, version FROM curie.execution_requests "
                                "WHERE work_item_id = :id AND status = 'running'"
                            ),
                            {"id": work_item_id},
                        )
                    )
                    .mappings()
                    .one()
                )
                version_id, deployment_id, lineage_id = (
                    uuid.uuid4(),
                    uuid.uuid4(),
                    uuid.uuid4(),
                )
                approval_id, publication_id = uuid.uuid4(), uuid.uuid4()
                pr_url = (
                    None if pr is None else f"https://github.com/{item['repo_full_name']}/pull/{pr}"
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
                        "base_sha, branch, pr_number, pr_url, head_sha, status, version, "
                        "latest_revision) VALUES "
                        "(:id, :agent, :deployment, :conversation, :repo, :base, "
                        ":branch, :pr, :url, :head, 'open', 1, 1)"
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
                        "head": head_sha if pr is not None else None,
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
                        "reply_channel, result_url, terminal_at) "
                        "VALUES (:id, :approval, :deployment, :conversation, :lineage, "
                        ":request_id, 1, :repo, :status, :base, "
                        "CAST('[\"README.md\"]' AS jsonb), "
                        "'Update README', 'Approved platform publication.', 'github', "
                        ":channel, :result, clock_timestamp())"
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


def _set_base_ref(work_item_id: uuid.UUID, base_ref: str) -> None:
    """Give the work item's lineage a PR base branch (#4105).

    The identity check constraint wants the repository id, installation id, PR
    node id and base ref all set together, so they are written in one UPDATE.
    """

    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                changed = await conn.execute(
                    text(
                        "UPDATE curie.thread_publication_lineages SET "
                        "github_repository_id = :repo_id, "
                        "github_installation_id = :installation_id, "
                        "github_pr_node_id = :node_id, base_ref = :base_ref "
                        "WHERE id = (SELECT publication_lineage_id FROM curie.work_items "
                        "WHERE id = :id)"
                    ),
                    {
                        "repo_id": REPO_ID,
                        "installation_id": INSTALLATION_ID,
                        "node_id": f"PR_fixture_{work_item_id.hex}",
                        "base_ref": base_ref,
                        "id": work_item_id,
                    },
                )
                if changed.rowcount != 1:
                    raise AssertionError(f"work item {work_item_id} has no lineage")
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
            await asyncio.gather(*(item._sync_status_comments() for item in reconcilers))
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


def _attach_revision_publication(
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    *,
    head_sha: str = HEAD_B,
    status: str = "succeeded",
) -> None:
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
                        "reply_channel, result_url, terminal_at) "
                        "VALUES (:id, :approval, :deployment, :conversation, :lineage, "
                        ":request_id, 2, :repo, :status, :base, "
                        "CAST('[\"README.md\"]' AS jsonb), "
                        "'Rename the helper', 'Approved platform publication.', 'github', "
                        ":channel, :result, clock_timestamp())"
                    ),
                    {
                        "id": publication_id,
                        "approval": approval_id,
                        "deployment": item["deployment_id"],
                        "conversation": item["conversation_id"],
                        "lineage": item["publication_lineage_id"],
                        "request_id": request_id,
                        "repo": REPO,
                        "status": status,
                        "base": "0123456789abcdef0123456789abcdef01234567",
                        "channel": REPO,
                        "result": pr_url,
                    },
                )
                await conn.execute(
                    text(
                        "UPDATE curie.thread_publication_lineages SET latest_revision = 2, "
                        "head_sha = :head WHERE id = :id"
                    ),
                    {"id": item["publication_lineage_id"], "head": head_sha},
                )
        finally:
            await engine.dispose()

    asyncio.run(go())


def _published_issue(client: Any, github: GitHubAPI, sink: _CommentServer) -> tuple[int, int, Any]:
    """An issue whose first run opened a PR and posted its one final comment."""

    number = next(_REVISION_ISSUES)
    pr = next(_REVISION_PR)
    _label(client, github, number)
    first = _request(number)
    _start_running(first["id"])
    _attach_publication(first["work_item_id"], status="succeeded", pr=pr)
    _reconcile()
    done = _request(number)
    assert (done["status"], done["terminal_cause"]) == ("completed", "completed")
    notices = _notices(first["id"])
    assert len(notices) == 1
    assert notices[0]["posted_at"] is not None
    assert sink.posts == 1
    path = f"/repos/{REPO}/issues/{number}/comments"
    body = sink.lists[path][0]["body"]
    assert f"https://github.com/{REPO}/pull/{pr}" in body
    assert marker_for(first["id"]) in body
    return number, pr, first


_REVISION_ISSUES = iter(range(9301, 9399))


def _complete_revision(
    client: Any, github: GitHubAPI, sink: _CommentServer, fragment: str
) -> tuple[int, int, uuid.UUID]:
    number, pr, first = _published_issue(client, github, sink)
    sink.requests.clear()
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
    """Comment creations only; label writes are asserted separately."""

    return [
        (path, body)
        for method, path, body in sink.requests
        if method == "POST" and not _LABELS.match(path)
    ]


def _patches(sink: _CommentServer) -> list[tuple[str, str | None]]:
    return [(path, body) for method, path, body in sink.requests if method == "PATCH"]


def _assert_one_final_comment(bodies: list[str], request_id: uuid.UUID) -> str:
    """Exactly one comment carries this request's marker, and it is final."""

    marked = [body for body in bodies if marker_for(request_id) in body]
    assert len(marked) == 1, marked
    assert FINAL_MARKER in marked[0]
    return marked[0]


@pytest.mark.parametrize("cause", ["no_pull_request", "early_stop"])
@pytest.mark.parametrize("padding", ["", " \n"])
def test_a_mention_with_no_changes_completes_naming_the_open_pull_request(
    admitted: Any, cause: str, padding: str
) -> None:
    """#4297 AC1/AC3: finish, notice and labels agree on an unchanged open PR."""

    client, github, sink = admitted
    sink.by_path = True
    number, pr, first = _published_issue(client, github, sink)
    sink.requests.clear()
    sink.ci_observations.clear()
    objective = f"https://github.com/{REPO}/issues/{number}#issuecomment-88120"
    revision = _insert_revision(first["work_item_id"], number, objective)
    _start_running(revision)
    detail = f"{padding}No changes needed: the existing tests already cover this request."

    finished = _finish_unpublished(client, revision, cause=cause, detail=detail)

    assert finished.status_code == 200, finished.text
    terminal = {row["id"]: row for row in _requests(number)}[revision]
    assert (terminal["status"], terminal["terminal_cause"]) == ("completed", "completed")
    notices = _notices(revision)
    assert len(notices) == 1
    assert notices[0]["terminal_cause"] == "completed"
    assert notices[0]["detail"].strip() == detail.strip()
    assert (
        _rows(
            "SELECT count(*) AS count FROM curie.publications WHERE execution_request_id = :id",
            {"id": revision},
        )[0]["count"]
        == 0
    )

    _reconcile()
    posts = _posts(sink)
    path = f"/repos/{REPO}/issues/{number}/comments"
    assert [posted_path for posted_path, _body in posts] == [path]
    body = _assert_one_final_comment([comment["body"] for comment in sink.lists[path]], revision)
    assert "Status: SUCCEEDED" in body
    assert (
        "No changes needed: the open pull request already covers this request: "
        f"https://github.com/{REPO}/pull/{pr}"
    ) in body
    assert detail.strip() in body
    assert "Could not complete:" not in body
    assert "NEEDS HUMAN" not in body
    assert "Cause:" not in body
    assert "curie-factory:pr-open" in sink.issue_labels[number]
    assert "curie-factory:needs-human" not in sink.issue_labels[number]
    assert _notices(revision)[0]["posted_at"] is not None
    assert sink.ci_observations == []
    _reconcile()
    assert len(_posts(sink)) == 1
    assert len(_notices(revision)) == 1
    assert sink.ci_observations == []


@pytest.mark.parametrize("cause", ["no_pull_request", "early_stop"])
def test_a_review_revision_with_no_changes_completes_in_its_thread(
    admitted: Any, cause: str
) -> None:
    """#4297 AC2: the unchanged result reaches the original review thread."""

    client, github, sink = admitted
    sink.by_path = True
    number, pr, first = _published_issue(client, github, sink)
    sink.requests.clear()
    objective = _revision_objective(pr, "discussion_r88121")
    revision = _insert_revision(first["work_item_id"], number, objective)
    _start_running(revision)
    detail = "No changes needed: this helper already has the requested name."

    finished = _finish_unpublished(client, revision, cause=cause, detail=detail)

    assert finished.status_code == 200, finished.text
    terminal = {row["id"]: row for row in _requests(number)}[revision]
    assert (terminal["status"], terminal["terminal_cause"]) == ("completed", "completed")
    _reconcile()
    posts = _posts(sink)
    path = f"/repos/{REPO}/pulls/{pr}/comments/88121/replies"
    assert [posted_path for posted_path, _body in posts] == [path]
    body = posts[0][1] or ""
    assert "Status: SUCCEEDED" in body
    assert "No changes needed: this pull request already covers the requested revision." in body
    assert f"In response to https://github.com/{REPO}/pull/{pr}#discussion_r88121" in body
    assert "Agent's last message:" in body
    assert detail in body
    assert "Could not complete:" not in body
    assert "NEEDS HUMAN" not in body
    assert "Cause:" not in body
    assert _notices(revision)[0]["comment_list"] == "review"
    assert _notices(revision)[0]["posted_at"] is not None
    _reconcile()
    assert len(_posts(sink)) == 1


@pytest.mark.parametrize("cause", ["no_pull_request", "early_stop"])
@pytest.mark.parametrize(
    "case",
    [
        "could_not_complete",
        "lowercase_marker",
        "marker_after_prose",
        "missing_colon",
        "merged",
        "closed",
        "first_request",
        "relabelled",
        "missing_pr_url",
        "empty_pr_url",
        "mismatched_pr",
    ],
)
def test_a_no_change_reply_outside_an_open_follow_up_still_fails(
    admitted: Any, cause: str, case: str
) -> None:
    """#4297 AC4: only the exact marker and an eligible open follow-up succeed."""

    client, github, sink = admitted
    sink.by_path = True
    detail = "No changes needed: the request is already covered."
    if case == "first_request":
        number = next(_REVISION_ISSUES)
        _label(client, github, number)
        first = _request(number)
        request_id = first["id"]
    else:
        number, pr, first = _published_issue(client, github, sink)
        objective = f"https://github.com/{REPO}/issues/{number}#issuecomment-88122"
        if case == "relabelled":
            objective = f"https://github.com/{REPO}/issues/{number}"
        elif case == "mismatched_pr":
            objective = _revision_objective(pr + 1, "discussion_r88122")
        request_id = _insert_revision(first["work_item_id"], number, objective)
    _start_running(request_id)
    sink.requests.clear()
    if case == "could_not_complete":
        detail = "Could not complete: the requested change needs a design decision."
    elif case == "lowercase_marker":
        detail = "no changes needed: the request is already covered."
    elif case == "marker_after_prose":
        detail = "I checked the request. No changes needed: it is already covered."
    elif case == "missing_colon":
        detail = "No changes needed because the request is already covered."

    if case in {"merged", "closed", "missing_pr_url", "empty_pr_url"}:

        async def change_lineage() -> None:
            engine = create_async_engine(get_settings().database_url)
            try:
                async with engine.begin() as conn:
                    assignment = {
                        "merged": "status = 'merged'",
                        "closed": "status = 'closed'",
                        "missing_pr_url": "pr_number = NULL, pr_url = NULL",
                        "empty_pr_url": "pr_url = '   '",
                    }[case]
                    changed = await conn.execute(
                        text(
                            f"UPDATE curie.thread_publication_lineages SET {assignment} "
                            "WHERE id = (SELECT publication_lineage_id "
                            "FROM curie.work_items WHERE id = :id)"
                        ),
                        {"id": first["work_item_id"]},
                    )
                    assert changed.rowcount == 1
            finally:
                await engine.dispose()

        asyncio.run(change_lineage())

    finished = _finish_unpublished(client, request_id, cause=cause, detail=detail)

    assert finished.status_code == 200, finished.text
    terminal = {row["id"]: row for row in _requests(number)}[request_id]
    assert (terminal["status"], terminal["terminal_cause"]) == ("failed", cause)
    notices = _notices(request_id)
    assert len(notices) == 1
    assert notices[0]["terminal_cause"] == cause
    _reconcile()
    bodies = [body or "" for _path, body in _posts(sink)]
    body = _assert_one_final_comment(bodies, request_id)
    assert "Status: NEEDS HUMAN" in body
    assert "Could not complete:" in body
    assert f"Cause: {cause}" in body
    assert "curie-factory:needs-human" in sink.issue_labels[number]
    assert "curie-factory:pr-open" not in sink.issue_labels[number]


@pytest.mark.parametrize("cause", ["no_pull_request", "early_stop"])
def test_a_no_change_follow_up_defers_to_its_in_flight_publication(
    admitted: Any, cause: str
) -> None:
    """#4297 AC5: the marker cannot bypass an unfinished revision publication."""

    client, github, sink = admitted
    sink.by_path = True
    number, _pr, first = _published_issue(client, github, sink)
    objective = f"https://github.com/{REPO}/issues/{number}#issuecomment-88123"
    revision = _insert_revision(first["work_item_id"], number, objective)
    _start_running(revision)
    _attach_revision_publication(first["work_item_id"], revision, status="pending")

    finished = _finish_unpublished(
        client, revision, cause=cause, detail="No changes needed: the request is already covered."
    )

    assert finished.status_code == 409, finished.text
    assert "publication_pending" in finished.text
    terminal = {row["id"]: row for row in _requests(number)}[revision]
    assert (terminal["status"], terminal["terminal_cause"]) == ("running", None)
    assert all(notice["terminal_cause"] is None for notice in _notices(revision))


@pytest.mark.parametrize("cause", ["no_pull_request", "early_stop"])
def test_a_no_change_follow_up_cannot_complete_after_its_execution_deadline(
    admitted: Any, cause: str
) -> None:
    """The old PR does not grant an unpublished revision the opened-PR deadline exception."""

    client, github, sink = admitted
    sink.by_path = True
    number, _pr, first = _published_issue(client, github, sink)
    objective = f"https://github.com/{REPO}/issues/{number}#issuecomment-88124"
    revision = _insert_revision(first["work_item_id"], number, objective)

    async def expired_runtime() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                # Write the deadline once while building an already-expired
                # runtime. The live owner lease isolates the deadline guard.
                changed = await conn.execute(
                    text(
                        "UPDATE curie.execution_requests SET status = 'running', "
                        "started_at = clock_timestamp() - interval '2 seconds', "
                        "execution_deadline = clock_timestamp() - interval '1 second', "
                        "execution_attempts = 1, runtime_epoch = 1, "
                        "runtime_owner = 'factory-owner', "
                        "runtime_heartbeat_expires_at = clock_timestamp() + interval '60 seconds' "
                        "WHERE id = :id"
                    ),
                    {"id": revision},
                )
                assert changed.rowcount == 1
        finally:
            await engine.dispose()

    asyncio.run(expired_runtime())
    finished = _finish_unpublished(
        client, revision, cause=cause, detail="No changes needed: the request is already covered."
    )

    assert finished.status_code == 409, finished.text
    assert "not_running" in finished.text
    terminal = {row["id"]: row for row in _requests(number)}[revision]
    assert (terminal["status"], terminal["terminal_cause"]) == ("running", None)
    assert all(notice["terminal_cause"] is None for notice in _notices(revision))


@pytest.mark.parametrize("stale", ["work_item", "request"])
def test_a_no_change_follow_up_preserves_stale_version_fencing(admitted: Any, stale: str) -> None:
    """Both optimistic versions fence the unchanged completion at the state boundary."""

    from curie_api.workitems import WorkItemConflict, _terminalize_execution

    client, github, sink = admitted
    sink.by_path = True
    number, _pr, first = _published_issue(client, github, sink)
    objective = f"https://github.com/{REPO}/issues/{number}#issuecomment-88125"
    revision = _insert_revision(first["work_item_id"], number, objective)
    _start_running(revision)
    versions = _rows(
        "SELECT w.version AS work_version, r.version AS request_version "
        "FROM curie.work_items w JOIN curie.execution_requests r "
        "ON r.work_item_id = w.id WHERE r.id = :id",
        {"id": revision},
    )[0]

    async def stale_finish() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                result = await _terminalize_execution(
                    session,
                    work_item_id=first["work_item_id"],
                    request_id=revision,
                    expected_work_item_version=versions["work_version"] - (stale == "work_item"),
                    expected_request_version=versions["request_version"] - (stale == "request"),
                    status="failed",
                    cause="no_pull_request",
                    detail="No changes needed: the request is already covered.",
                    ci_fix_round=None,
                    extra_where=(),
                )
                assert isinstance(result, WorkItemConflict), result
                assert result.code == "stale_version"
        finally:
            await engine.dispose()

    asyncio.run(stale_finish())
    terminal = {row["id"]: row for row in _requests(number)}[revision]
    assert (terminal["status"], terminal["terminal_cause"]) == ("running", None)
    assert all(notice["terminal_cause"] is None for notice in _notices(revision))


def test_a_completed_first_request_posts_one_comment_naming_its_pull_request(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    sink.by_path = True
    number, pr, _first = _published_issue(client, github, sink)
    posts = _posts(sink)
    assert [path for path, _body in posts] == [f"/repos/{REPO}/issues/{number}/comments"]
    assert f"https://github.com/{REPO}/pull/{pr}" in (posts[0][1] or "")
    _reconcile()
    assert len(_posts(sink)) == 1


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
    assert [path for path, _ in posts] == [f"/repos/{REPO}/pulls/{pr}/comments/88102/replies"]
    assert marker_for(revision) in (posts[0][1] or "")
    assert _notices(revision)[0]["comment_list"] == "review"
    assert _notices(revision)[0]["posted_at"] is not None
    _reconcile()
    assert len(_posts(sink)) == 1


@pytest.mark.parametrize("fragment", ["issuecomment-88103", "pullrequestreview-88104"])
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
    sink.requests.clear()
    revision = _insert_revision(first["work_item_id"], number, _revision_objective(pr, fragment))
    sink.lists[f"/repos/{REPO}/{listed.format(pr=pr)}"] = [
        {
            "id": 7555,
            "body": result_section(
                "completed",
                pr_url=None,
                feedback_url=_revision_objective(pr, fragment),
            )
            + f"\n{marker_for(revision)}\n",
        }
    ]
    _start_running(revision)
    _attach_revision_publication(first["work_item_id"], revision)
    _reconcile()
    assert _posts(sink) == []
    notices = _notices(revision)
    assert len(notices) == 1
    assert notices[0]["comment_id"] == 7555
    assert notices[0]["posted_at"] is not None


def _scan_page(request_id: uuid.UUID) -> int:
    return _rows(
        "SELECT scan_page FROM curie.factory_terminal_notices WHERE execution_request_id = :id",
        {"id": request_id},
    )[0]["scan_page"]


def test_a_lost_thread_reply_response_rescans_every_list(admitted: Any) -> None:
    """A thread reply that posts but whose response is lost must not double-post.

    The conversation list holds more than 500 comments, so the first pass
    exhausts the review list, then advances the cursor past the conversation
    list's offset without finishing it. A second pass finishes the conversation
    list and attempts the thread reply, whose response is then lost.
    """

    client, github, sink = admitted
    sink.by_path = True
    number, pr, first = _published_issue(client, github, sink)
    sink.requests.clear()
    fragment = "discussion_r88109"
    revision = _insert_revision(first["work_item_id"], number, _revision_objective(pr, fragment))
    sink.lists[f"/repos/{REPO}/issues/{pr}/comments"] = [
        {"id": 9000 + i, "body": f"unrelated comment {i}"} for i in range(550)
    ]
    reply_path = f"/repos/{REPO}/pulls/{pr}/comments/88109/replies"
    sink.lost_response_paths.add(reply_path)
    _start_running(revision)
    _attach_revision_publication(first["work_item_id"], revision)

    _reconcile()
    assert _posts(sink) == []
    assert _notices(revision)[0]["posted_at"] is None
    assert _scan_page(revision) > 1_000_000

    _reconcile()
    assert [path for path, _ in _posts(sink)] == [reply_path]
    assert _notices(revision)[0]["posted_at"] is None
    assert _scan_page(revision) == 1

    _reconcile()
    assert [path for path, _ in _posts(sink)] == [reply_path]
    notices = _notices(revision)
    assert notices[0]["posted_at"] is not None
    assert notices[0]["comment_id"] == 8002


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


def test_factory_notices_usage_limited_finish_posts_the_reset_remedy_once(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 9214
    _label(client, github, number)
    row = _request(number)
    epoch = _start_running(row["id"])
    finished = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/finish",
        headers={"X-Curie-Worker-Token": "factory-terminus-worker"},
        json={
            "runtime_epoch": epoch,
            "outcome": "failed",
            "cause": "model_usage_limited",
            "detail": "You've hit your session limit · resets 3pm (UTC)",
        },
    )
    assert finished.status_code == 200, finished.text
    assert _request(number)["terminal_cause"] == "model_usage_limited"
    _reconcile()
    _reconcile()
    assert sink.posts == 1
    body = sink.comments[0]["body"]
    assert "the model provider's usage limit for this credential was reached" in body
    assert "re-add the label after the limit resets" in body
    assert "add credits" not in body
    assert "Cause: model_usage_limited" in body
    assert "Failure class: model-usage-limited" in body


def test_factory_notices_model_unreachable_finish_comments_the_network_remedy(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 9215
    _label(client, github, number)
    row = _request(number)
    epoch = _start_running(row["id"])
    finished = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/finish",
        headers={"X-Curie-Worker-Token": "factory-terminus-worker"},
        json={
            "runtime_epoch": epoch,
            "outcome": "failed",
            "cause": "model_unreachable",
            "detail": "model error: server_error: API Error: Connection refused (ECONNREFUSED)",
        },
    )
    assert finished.status_code == 200, finished.text
    assert _request(number)["terminal_cause"] == "model_unreachable"
    _reconcile()
    assert sink.posts == 1
    body = sink.comments[0]["body"]
    assert _MODEL_UNREACHABLE_SENTENCE in body
    assert "Connection refused (ECONNREFUSED)" in body
    assert "Cause: model_unreachable" in body
    assert "Failure class: model-unreachable" in body
