"""Polling is the default factory intake (#3745).

These tests drive ``WorkItemReconciler.run_once``, ``Settings``, and
``POST /github/webhook``. They do not call the poll helper. GitHub is an
in-process fixture. List and conditional reads follow:

https://docs.github.com/en/rest/issues/issues#list-repository-issues
https://docs.github.com/en/rest/issues/events#list-issue-events
https://docs.github.com/en/rest/issues/comments#list-issue-comments-for-a-repository
https://docs.github.com/en/rest/pulls/comments#list-review-comments-in-a-repository
https://docs.github.com/en/rest/pulls/reviews#list-reviews-for-a-pull-request
https://docs.github.com/en/rest/branches/branches#get-a-branch
https://docs.github.com/en/rest/issues/comments#create-an-issue-comment
https://docs.github.com/en/rest/issues/comments#update-an-issue-comment
https://docs.github.com/en/rest/using-the-rest-api/getting-started-with-the-rest-api#conditional-requests

Machine fixtures stand in for GitHub. They are not human-authored GitHub proof.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import importlib
import itertools
import json
import sys
import threading
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg
import httpx
import pytest
import redis.asyncio as aioredis
from curie_api.config import Settings, get_settings
from curie_api.main import create_app
from curie_api.workitem_reconciler import WorkItemReconciler
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from fastapi.testclient import TestClient
from sqlalchemy import make_url
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
    _comment_event,
    _issue_event,
    _post,
)
from test_github_factory_ingress import _rows  # noqa: E402
from test_github_factory_review import (  # noqa: E402
    BASE_REF,
    HEAD,
    _branch,
    _complete,
    _execute,
    _own_pull_request,
)

_ISSUES = itertools.count(9800)
_PULLS = itertools.count(410)
_MAIN_SHA = "1" * 40
_NEXT_SHA = "2" * 40
_POLL_LOCK = (3745, 187)
_CREDENTIAL_MODULES = (
    "curie_api.factory_label_reconcile",
    "curie_api.github_factory",
    "curie_api.github_review_truth",
    "curie_api.github_factory_review",
    "curie_api.factory_notices",
    "curie_api.factory_poll_intake",
)


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _user() -> dict[str, Any]:
    return {"id": SENDER_ID, "login": SENDER, "type": "User"}


def _label_request_id(number: int, event_id: int) -> uuid.UUID:
    return uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"https://github.com/factory/label/{REPO_ID}/{number}/{event_id}",
    )


def _mention_request_id(comment_id: int) -> uuid.UUID:
    return uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"https://github.com/factory/mention/{REPO_ID}/{comment_id}",
    )


def _feedback_request_id(event: str, feedback_id: int) -> uuid.UUID:
    identity = f"{REPO_ID}:{event}:{feedback_id}"
    event_id = f"github-feedback-{uuid.uuid5(uuid.NAMESPACE_URL, identity)}"
    return uuid.uuid5(uuid.NAMESPACE_URL, event_id)


def _body_digest(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload).encode()).hexdigest()


def _public_review(review: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in review.items() if key != "pr"}


@dataclass
class _PlantedIssue:
    number: int
    state: str
    labeled: bool
    labeled_at: datetime
    event_id: int
    closed_event_id: int | None = None
    base_labels: tuple[str, ...] = ()


@dataclass
class PollGitHub:
    """Records REST calls and answers the lists a poll pass reads."""

    calls: list[httpx.Request] = field(default_factory=list)
    saw_304: bool = False
    failures: dict[str, int] = field(default_factory=dict)
    permission_failures: set[int] = field(default_factory=set)
    failed_permissions: list[int] = field(default_factory=list)
    _permission_issue: int | None = None
    issues: dict[int, _PlantedIssue] = field(default_factory=dict)
    comments: dict[int, dict[str, Any]] = field(default_factory=dict)
    review_comments: dict[int, dict[str, Any]] = field(default_factory=dict)
    reviews: dict[int, dict[str, Any]] = field(default_factory=dict)
    pulls: dict[int, dict[str, Any]] = field(default_factory=dict)
    listed_pulls: set[int] = field(default_factory=set)
    permission: str = "write"
    default_branch: str = "main"
    branches: dict[str, str] = field(
        default_factory=lambda: {"main": _MAIN_SHA, "next": _NEXT_SHA}
    )
    _comment_ids: Iterator[int] = field(default_factory=lambda: itertools.count(880000))
    _etags: dict[str, str] = field(
        default_factory=lambda: {
            "issues": '"factory-issues"',
            "events": '"factory-events"',
            "issue-comments": '"factory-issue-comments"',
            "review-comments": '"factory-review-comments"',
            "reviews": '"factory-reviews"',
        }
    )

    def plant_issue(
        self,
        *,
        age: timedelta = timedelta(seconds=5),
        base_labels: tuple[str, ...] = (),
    ) -> int:
        number = next(_ISSUES)
        self.issues[number] = _PlantedIssue(
            number=number,
            state="open",
            labeled=True,
            labeled_at=datetime.now(UTC) - age,
            event_id=810000 + number,
            base_labels=base_labels,
        )
        return number

    def close_issue(self, number: int) -> None:
        issue = self.issues[number]
        issue.state = "closed"
        issue.closed_event_id = 820000 + number

    def plant_pull(self) -> int:
        number = next(_PULLS)
        repo = {"id": REPO_ID, "full_name": REPO}
        self.pulls[number] = {
            "number": number,
            "state": "open",
            "merged": False,
            "node_id": f"PR_acme_{number}",
            "html_url": f"https://github.com/{REPO}/pull/{number}",
            "head": {"sha": HEAD, "ref": _branch(number), "repo": dict(repo)},
            "base": {"ref": BASE_REF, "repo": dict(repo)},
        }
        self.listed_pulls.add(number)
        return number

    def add_issue_comment(self, number: int, comment_id: int, body: str) -> dict[str, Any]:
        stamp = _iso(datetime.now(UTC))
        comment = {
            "id": comment_id,
            "body": body,
            "user": _user(),
            "created_at": stamp,
            "updated_at": stamp,
            "performed_via_github_app": None,
            "author_association": "MEMBER",
            "html_url": f"https://github.com/{REPO}/issues/{number}#issuecomment-{comment_id}",
            "issue_url": f"https://api.github.com/repos/{REPO}/issues/{number}",
        }
        self.comments[comment_id] = comment
        return comment

    def add_review_comment(self, pr: int, comment_id: int, body: str) -> dict[str, Any]:
        stamp = _iso(datetime.now(UTC))
        comment = {
            "id": comment_id,
            "body": body,
            "user": _user(),
            "created_at": stamp,
            "updated_at": stamp,
            "performed_via_github_app": None,
            "author_association": "MEMBER",
            "commit_id": HEAD,
            "path": "src/acme/helper.py",
            "line": 12,
            "pull_request_review_id": comment_id + 500000,
            "html_url": f"https://github.com/{REPO}/pull/{pr}#discussion_r{comment_id}",
            "pull_request_url": f"https://api.github.com/repos/{REPO}/pulls/{pr}",
        }
        self.review_comments[comment_id] = comment
        return comment

    def add_review(self, pr: int, review_id: int, body: str) -> dict[str, Any]:
        review = {
            "id": review_id,
            "pr": pr,
            "body": body,
            "user": _user(),
            "commit_id": HEAD,
            "state": "CHANGES_REQUESTED",
            "submitted_at": _iso(datetime.now(UTC)),
            "html_url": f"https://github.com/{REPO}/pull/{pr}#pullrequestreview-{review_id}",
            "author_association": "MEMBER",
            "performed_via_github_app": None,
        }
        self.reviews[review_id] = review
        return review

    def _not_modified(self, request: httpx.Request, family: str) -> httpx.Response | None:
        etag = self._etags[family]
        if request.headers.get("if-none-match") == etag:
            self.saw_304 = True
            return httpx.Response(304, headers={"ETag": etag})
        return None

    def _page(self, request: httpx.Request, family: str, items: list[Any]) -> httpx.Response:
        cached = self._not_modified(request, family)
        if cached is not None:
            return cached
        per_page = int(request.url.params.get("per_page", "30"))
        page = int(request.url.params.get("page", "1"))
        start = (page - 1) * per_page
        return httpx.Response(
            200,
            json=items[start : start + per_page],
            headers={"ETag": self._etags[family]},
        )

    @staticmethod
    def _since(request: httpx.Request) -> datetime | None:
        raw = request.url.params.get("since")
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else None

    def _recent(self, request: httpx.Request, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        since = self._since(request)
        if since is None:
            return items
        kept = []
        for item in items:
            raw = item.get("created_at") or item.get("submitted_at")
            if not isinstance(raw, str):
                continue
            moment = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if moment >= since:
                kept.append(item)
        return kept

    def _events(self, issue: _PlantedIssue) -> list[dict[str, Any]]:
        human = _user()
        events: list[dict[str, Any]] = [
            {
                "id": issue.event_id,
                "event": "labeled",
                "label": {"name": LABEL},
                "actor": human,
                "performed_via_github_app": None,
                "created_at": _iso(issue.labeled_at),
            }
        ]
        if issue.closed_event_id is not None:
            events.append(
                {
                    "id": issue.closed_event_id,
                    "event": "closed",
                    "actor": human,
                    "performed_via_github_app": None,
                    "created_at": _iso(datetime.now(UTC)),
                }
            )
        return events

    def _issue_body(self, issue: _PlantedIssue) -> dict[str, Any]:
        return {
            "number": issue.number,
            "state": issue.state,
            "labels": [
                {"name": name}
                for name in ((LABEL,) if issue.labeled else ()) + issue.base_labels
            ],
            "user": _user(),
        }

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        path = request.url.path
        if path in self.failures:
            return httpx.Response(self.failures[path], json={"message": "Unavailable fixture"})
        root = f"/repos/{REPO}"
        if path == root:
            return httpx.Response(
                200,
                json={
                    "id": REPO_ID,
                    "full_name": REPO,
                    "default_branch": self.default_branch,
                },
            )
        branch_prefix = f"{root}/branches/"
        if path.startswith(branch_prefix):
            branch = path.removeprefix(branch_prefix)
            sha = self.branches.get(branch)
            if sha is None:
                return httpx.Response(404, json={"message": "Branch not found"})
            return httpx.Response(200, json={"name": branch, "commit": {"sha": sha}})
        if path == f"{root}/issues":
            listed = [
                self._issue_body(issue)
                for issue in self.issues.values()
                if issue.state == "open" and issue.labeled
            ]
            for number in sorted(self.listed_pulls):
                listed.append(
                    {
                        "number": number,
                        "state": "open",
                        "labels": [{"name": LABEL}],
                        "pull_request": {
                            "url": f"https://api.github.com/repos/{REPO}/pulls/{number}",
                        },
                    }
                )
            return self._page(request, "issues", listed)
        if path == f"{root}/issues/comments":
            comments = self._recent(request, list(self.comments.values()))
            return self._page(request, "issue-comments", comments)
        if path == f"{root}/pulls/comments":
            return self._page(
                request,
                "review-comments",
                self._recent(request, list(self.review_comments.values())),
            )
        if path.startswith(f"{root}/issues/comments/"):
            comment_id = int(path.rsplit("/", 1)[1])
            comment = self.comments.get(comment_id)
            if comment is None:
                return httpx.Response(404, json={"message": "missing fixture"})
            if request.method == "PATCH":
                comment["body"] = json.loads(request.content)["body"]
            return httpx.Response(200, json=comment)
        if path.startswith(f"{root}/pulls/comments/"):
            comment_id = int(path.rsplit("/", 1)[1])
            comment = self.review_comments.get(comment_id)
            if comment is None:
                return httpx.Response(404, json={"message": "missing fixture"})
            return httpx.Response(200, json=comment)
        if path.startswith(f"{root}/issues/") and path.endswith("/comments"):
            number = int(path.split("/")[-2])
            if request.method == "POST":
                comment = self.add_issue_comment(
                    number, next(self._comment_ids), json.loads(request.content)["body"]
                )
                comment["user"] = {"id": 99, "login": "curie[bot]", "type": "Bot"}
                comment["performed_via_github_app"] = {"id": 51, "slug": "curie"}
                return httpx.Response(201, json=comment)
            rows = [
                comment
                for comment in self.comments.values()
                if str(comment.get("issue_url", "")).endswith(f"/issues/{number}")
            ]
            return httpx.Response(200, json=rows)
        if path.startswith(f"{root}/issues/") and path.endswith("/events"):
            number = int(path.split("/")[-2])
            issue = self.issues.get(number)
            if issue is None:
                return httpx.Response(200, json=[])
            return self._page(request, "events", self._events(issue))
        reviews_prefix = f"{root}/pulls/"
        if path.startswith(reviews_prefix) and "/reviews" in path:
            tail = path[len(reviews_prefix) :]
            pr_text, _, review_tail = tail.partition("/reviews")
            pr = int(pr_text)
            if review_tail in ("", "/"):
                # GitHub lists reviews with pagination and no since filter:
                # https://docs.github.com/en/rest/pulls/reviews#list-reviews-for-a-pull-request
                assert "since" not in request.url.params
                owned = [
                    _public_review(review)
                    for review in self.reviews.values()
                    if review.get("pr") == pr
                ]
                return self._page(request, "reviews", owned)
            review_id = int(review_tail.strip("/"))
            review = self.reviews.get(review_id)
            if review is None or review.get("pr") != pr:
                return httpx.Response(404, json={"message": "missing fixture"})
            return httpx.Response(200, json=_public_review(review))
        if path.startswith(f"{root}/pulls/"):
            tail = path.rsplit("/", 1)[1]
            if not tail.isdigit():
                return httpx.Response(404, json={"message": "missing fixture"})
            number = int(tail)
            pull = self.pulls.get(number)
            if pull is None:
                return httpx.Response(404, json={"message": "missing fixture"})
            return httpx.Response(200, json=pull)
        if path.startswith(f"{root}/issues/"):
            tail = path.rsplit("/", 1)[1]
            if not tail.isdigit():
                return httpx.Response(404, json={"message": "missing fixture"})
            number = int(tail)
            if number in self.pulls and number not in self.issues:
                return httpx.Response(
                    200,
                    json={
                        "number": number,
                        "state": "open",
                        "labels": [{"name": LABEL}],
                        "pull_request": {
                            "url": f"https://api.github.com/repos/{REPO}/pulls/{number}",
                        },
                    },
                )
            issue = self.issues.get(number)
            if issue is None:
                return httpx.Response(404, json={"message": "missing fixture"})
            self._permission_issue = number
            body = self._issue_body(issue)
            etag = f'"{hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()}"'
            if request.headers.get("if-none-match") == etag:
                self.saw_304 = True
                return httpx.Response(304, headers={"ETag": etag})
            return httpx.Response(200, json=body, headers={"ETag": etag})
        if path == f"{root}/collaborators/{SENDER}/permission":
            number, self._permission_issue = self._permission_issue, None
            if number in self.permission_failures:
                assert number is not None
                self.failed_permissions.append(number)
                return httpx.Response(500, json={"message": "Permission unavailable"})
            return httpx.Response(
                200,
                json={"permission": self.permission, "user": {"id": SENDER_ID, "login": SENDER}},
            )
        return httpx.Response(404, json={"message": "missing fixture"})


class _Credentials:
    def fresh_installation_token(
        self, repo: str, expected_installation_id: int | None = None
    ) -> tuple[int, str]:
        if repo != REPO or expected_installation_id not in (None, INSTALLATION_ID):
            raise RuntimeError("installation was not rediscovered")
        return INSTALLATION_ID, "fixture-installation-token"

    def token_for_verified_installation(self, repo: str, installation_id: int) -> str:
        return self.fresh_installation_token(repo, installation_id)[1]


def _patch_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    creds = _Credentials()

    def credentials_for(_settings: Settings) -> _Credentials:
        return creds

    for name in _CREDENTIAL_MODULES:
        try:
            module = importlib.import_module(name)
        except ImportError:
            continue
        monkeypatch.setattr(module, "credentials_for", credentials_for, raising=False)


def _client_type(github: PollGitHub) -> type[httpx.AsyncClient]:
    transport = httpx.MockTransport(github.handle)

    class _Client(httpx.AsyncClient):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs["transport"] = transport
            super().__init__(*args, **kwargs)

    return _Client


def _poll_env(intake: str) -> dict[str, str]:
    env = dict(_ENV)
    env["GITHUB_FACTORY_INTAKE"] = intake
    env["GITHUB_FACTORY_BASES"] = "{}"
    if intake == "poll":
        env["GITHUB_FACTORY_POLL_INTERVAL_S"] = "30"
    return env


@pytest.fixture
def poll_factory(monkeypatch: pytest.MonkeyPatch, clean_db: None, valkey: object) -> Any:
    yield from _factory(monkeypatch, "poll")


@pytest.fixture
def webhook_factory(monkeypatch: pytest.MonkeyPatch, clean_db: None, valkey: object) -> Any:
    yield from _factory(monkeypatch, "webhook")


def _factory(monkeypatch: pytest.MonkeyPatch, intake: str) -> Any:
    for key, value in _poll_env(intake).items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    _patch_credentials(monkeypatch)
    github = PollGitHub()
    original = httpx.AsyncClient
    httpx.AsyncClient = _client_type(github)  # type: ignore[misc]
    try:
        with TestClient(create_app()) as client:
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
            yield client, github
    finally:
        httpx.AsyncClient = original  # type: ignore[misc]
        get_settings.cache_clear()


def _run_once(github: PollGitHub) -> None:
    asyncio.run(_run_once_async(github))


async def _run_once_async(github: PollGitHub) -> None:
    engine = create_async_engine(get_settings().database_url)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    client = aioredis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
    try:
        await WorkItemReconciler(maker, client, get_settings()).run_once()
    finally:
        await client.aclose()
        await engine.dispose()


def _requests(number: int) -> list[dict[str, Any]]:
    return _rows(
        "SELECT r.id, r.sequence, r.status, r.terminal_cause, r.objective, "
        "r.requester, w.id AS work_item_id, w.cancelled_at, "
        "w.base_branch, w.base_source, w.base_commit, w.base_label_ignored "
        "FROM curie.work_items w "
        "LEFT JOIN curie.execution_requests r ON r.work_item_id = w.id "
        "WHERE w.github_repository_id = :repo AND w.github_issue_number = :number "
        "ORDER BY r.sequence",
        {"repo": REPO_ID, "number": number},
    )


def _delivery_ids() -> set[uuid.UUID]:
    rows = _rows("SELECT delivery_id FROM curie.github_review_deliveries")
    return {row["delivery_id"] for row in rows}


def _assert_receipt(delivery: str, payload: dict[str, Any], event: str) -> None:
    rows = _rows(
        "SELECT delivery_id, body_sha256, event_kind FROM curie.github_review_deliveries "
        "WHERE delivery_id = :id",
        {"id": uuid.UUID(delivery)},
    )
    assert len(rows) == 1
    assert rows[0]["event_kind"] == event
    assert rows[0]["body_sha256"] == _body_digest(payload)


def _admit_label(client: TestClient, github: PollGitHub) -> tuple[int, dict[str, Any]]:
    number = github.plant_issue()
    payload = _issue_event("labeled", number, label={"name": LABEL})
    response = _post(client, "issues", payload)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "factory_admitted", response.text
    rows = _requests(number)
    assert len(rows) == 1
    return number, rows[0]


def test_poll_admits_a_young_labeled_issue_once(
    poll_factory: tuple[TestClient, PollGitHub],
) -> None:
    _client, github = poll_factory
    number = github.plant_issue()
    pull = github.plant_pull()

    _run_once(github)

    rows = _requests(number)
    assert len(rows) == 1
    assert rows[0]["id"] == _label_request_id(number, github.issues[number].event_id)
    assert rows[0]["status"] == "waiting"
    assert rows[0]["objective"] == f"https://github.com/{REPO}/issues/{number}"
    assert rows[0]["requester"] == f"github:{SENDER_ID}:{SENDER}"
    assert _requests(pull) == []
    assert _delivery_ids() == set()

    _run_once(github)

    assert [row["id"] for row in _requests(number)] == [rows[0]["id"]]


@pytest.mark.parametrize(
    ("entry", "labels", "branch", "source", "commit"),
    [
        (None, (), "main", "default", _MAIN_SHA),
        (
            {"bases": ["main", "next"], "default_base": "next"},
            (),
            "next",
            "default",
            _NEXT_SHA,
        ),
        (
            {"bases": ["main", "next"], "default_base": "main"},
            ("base:next",),
            "next",
            "label",
            _NEXT_SHA,
        ),
    ],
)
def test_poll_records_the_verified_selected_base(
    poll_factory: tuple[TestClient, PollGitHub],
    monkeypatch: pytest.MonkeyPatch,
    entry: dict[str, Any] | None,
    labels: tuple[str, ...],
    branch: str,
    source: str,
    commit: str,
) -> None:
    _client, github = poll_factory
    if entry is not None:
        monkeypatch.setenv("GITHUB_FACTORY_BASES", json.dumps({REPO: entry}))
        get_settings.cache_clear()
    number = github.plant_issue(base_labels=labels)

    _run_once(github)

    rows = _requests(number)
    assert len(rows) == 1
    assert (rows[0]["base_branch"], rows[0]["base_source"], rows[0]["base_commit"]) == (
        branch,
        source,
        commit,
    )
    branch_reads = [
        request.url.path
        for request in github.calls
        if request.url.path.startswith(f"/repos/{REPO}/branches/")
    ]
    assert branch_reads == [f"/repos/{REPO}/branches/{branch}"]


@pytest.mark.parametrize(
    ("labels", "allowed", "reason"),
    [
        (("base:main", "base:next"), ["main", "next"], "more than one base label"),
        (("base:next",), ["main"], "not an allowed base"),
        (("base:missing",), ["main", "missing"], "does not exist"),
    ],
)
def test_poll_refuses_an_invalid_base_and_admits_after_correction(
    poll_factory: tuple[TestClient, PollGitHub],
    monkeypatch: pytest.MonkeyPatch,
    labels: tuple[str, ...],
    allowed: list[str],
    reason: str,
) -> None:
    _client, github = poll_factory
    monkeypatch.setenv(
        "GITHUB_FACTORY_BASES",
        json.dumps({REPO: {"bases": allowed, "default_base": "main"}}),
    )
    get_settings.cache_clear()
    number = github.plant_issue(base_labels=labels)

    _run_once(github)

    assert _requests(number) == []
    comments = list(github.comments.values())
    assert len(comments) == 1
    assert "<!-- curie-factory-base-refusal -->" in comments[0]["body"]
    assert reason in comments[0]["body"]

    github._etags["issues"] = f'"retry-refusal-{number}"'
    _run_once(github)
    assert _requests(number) == []
    assert list(github.comments.values()) == comments
    assert len(
        [
            request
            for request in github.calls
            if request.method == "POST"
            and request.url.path == f"/repos/{REPO}/issues/{number}/comments"
        ]
    ) == 1

    github.issues[number].base_labels = ()
    github._etags["issues"] = f'"corrected-base-{number}"'
    _run_once(github)

    rows = _requests(number)
    assert len(rows) == 1
    assert (rows[0]["base_branch"], rows[0]["base_source"], rows[0]["base_commit"]) == (
        "main",
        "default",
        _MAIN_SHA,
    )


def test_poll_records_later_base_labels_without_readmitting_or_moving_the_base(
    poll_factory: tuple[TestClient, PollGitHub], monkeypatch: pytest.MonkeyPatch
) -> None:
    _client, github = poll_factory
    monkeypatch.setenv(
        "GITHUB_FACTORY_BASES",
        json.dumps({REPO: {"bases": ["main", "next"], "default_base": "main"}}),
    )
    get_settings.cache_clear()
    number = github.plant_issue(base_labels=("base:next",))
    _run_once(github)
    first = _requests(number)
    assert len(first) == 1
    assert first[0]["base_branch"] == "next"
    assert first[0]["base_label_ignored"] is None
    github.calls.clear()

    github.issues[number].base_labels = ("base:main",)
    github._etags["issues"] = f'"changed-base-{number}"'
    _run_once(github)

    after = _requests(number)
    assert [row["id"] for row in after] == [first[0]["id"]]
    assert (after[0]["base_branch"], after[0]["base_commit"]) == ("next", _NEXT_SHA)
    assert after[0]["base_label_ignored"] == "main"
    assert after[0]["status"] == first[0]["status"]
    assert not any("/branches/" in request.url.path for request in github.calls)

    github.issues[number].base_labels = ("base:next",)
    github._etags["issues"] = f'"restored-base-{number}"'
    _run_once(github)

    restored = _requests(number)
    assert [row["id"] for row in restored] == [first[0]["id"]]
    assert (restored[0]["base_branch"], restored[0]["base_commit"]) == ("next", _NEXT_SHA)
    assert restored[0]["base_label_ignored"] is None
    assert not any("/branches/" in request.url.path for request in github.calls)


def test_poll_cancels_a_closed_labeled_issue(
    poll_factory: tuple[TestClient, PollGitHub],
) -> None:
    client, github = poll_factory
    number, _row = _admit_label(client, github)
    github.close_issue(number)

    _run_once(github)

    rows = _requests(number)
    assert rows[0]["cancelled_at"] is not None
    assert rows[0]["status"] == "cancelled"
    assert rows[0]["terminal_cause"] == "issue_cancelled"


@pytest.mark.parametrize(
    ("endpoint", "status"),
    [("issue", 404), ("issue", 301), ("events", 500), ("events", 301)],
)
def test_poll_defers_one_unreadable_issue_and_continues_other_intake(
    poll_factory: tuple[TestClient, PollGitHub], endpoint: str, status: int
) -> None:
    client, github = poll_factory
    unavailable, unavailable_request = _admit_label(client, github)
    closed, _closed_request = _admit_label(client, github)
    mentioned, _mentioned_request = _admit_label(client, github)
    reviewed, reviewed_request = _admit_label(client, github)
    _run_once(github)

    # A GitHub 404 can conceal permissions; it cannot prove an issue was closed.
    # https://docs.github.com/en/rest/issues/issues#get-an-issue
    # https://docs.github.com/en/rest/using-the-rest-api/troubleshooting-the-rest-api#404-not-found-for-an-existing-resource
    path = f"/repos/{REPO}/issues/{unavailable}"
    if endpoint == "events":
        github.close_issue(unavailable)
        path += "/events"
    github.failures[path] = status
    github.close_issue(closed)
    comment_id, review_id = 73501, 74501
    github.add_issue_comment(mentioned, comment_id, f"@{MENTION} please revise the helper")
    github._etags["issue-comments"] = '"healthy-mention-after-unavailable-issue"'
    pull = github.plant_pull()
    _own_pull_request(reviewed_request["work_item_id"], pull)
    _complete(reviewed_request["id"])
    github.add_review(pull, review_id, f"@{MENTION} please rename the helper")
    github.calls.clear()

    _run_once(github)

    assert any(request.url.path == path for request in github.calls)
    unreadable = _requests(unavailable)
    assert len(unreadable) == 1
    assert unreadable[0]["id"] == unavailable_request["id"]
    assert unreadable[0]["cancelled_at"] is None
    assert unreadable[0]["status"] != "cancelled"
    cancelled = _requests(closed)
    assert cancelled[0]["cancelled_at"] is not None
    assert cancelled[0]["terminal_cause"] == "issue_cancelled"
    assert _mention_request_id(comment_id) in {row["id"] for row in _requests(mentioned)}
    assert _feedback_request_id("pull_request_review", review_id) in {
        row["id"] for row in _requests(reviewed)
    }
    del github.failures[path]
    github.close_issue(unavailable)

    _run_once(github)

    recovered = _requests(unavailable)
    assert recovered[0]["cancelled_at"] is not None
    assert recovered[0]["terminal_cause"] == "issue_cancelled"


def test_poll_retries_a_new_label_after_event_read_failure_without_blocking_other_intake(
    poll_factory: tuple[TestClient, PollGitHub],
) -> None:
    client, github = poll_factory
    mentioned, _mentioned_request = _admit_label(client, github)
    unavailable = github.plant_issue()
    healthy = github.plant_issue()
    path = f"/repos/{REPO}/issues/{unavailable}/events"
    github.failures[path] = 500
    comment_id = 73502
    github.add_issue_comment(mentioned, comment_id, f"@{MENTION} please revise the helper")

    _run_once(github)

    assert _requests(unavailable) == []
    healthy_rows = _requests(healthy)
    assert len(healthy_rows) == 1
    assert healthy_rows[0]["id"] == _label_request_id(healthy, github.issues[healthy].event_id)
    mentioned_rows = _requests(mentioned)
    assert _mention_request_id(comment_id) in {row["id"] for row in mentioned_rows}
    del github.failures[path]
    github.calls.clear()

    _run_once(github)

    listings = [
        request for request in github.calls if request.url.path == f"/repos/{REPO}/issues"
    ]
    assert len(listings) == 1
    assert "if-none-match" not in listings[0].headers
    recovered = _requests(unavailable)
    assert len(recovered) == 1
    assert recovered[0]["id"] == _label_request_id(unavailable, github.issues[unavailable].event_id)
    assert [row["id"] for row in _requests(healthy)] == [row["id"] for row in healthy_rows]
    assert [row["id"] for row in _requests(mentioned)] == [row["id"] for row in mentioned_rows]


@pytest.mark.parametrize("lane", ["labeled", "stale"])
def test_poll_retries_permission_failure_after_events_and_continues_other_intake(
    poll_factory: tuple[TestClient, PollGitHub], lane: str
) -> None:
    client, github = poll_factory
    mentioned, _mentioned_request = _admit_label(client, github)
    reviewed, reviewed_request = _admit_label(client, github)
    if lane == "labeled":
        unavailable = github.plant_issue()
    else:
        unavailable, _unavailable_request = _admit_label(client, github)
        github.close_issue(unavailable)
    healthy = github.plant_issue()
    github.permission_failures.add(unavailable)
    comment_id, review_id = 73503, 74503
    github.add_issue_comment(mentioned, comment_id, f"@{MENTION} please revise the helper")
    pull = github.plant_pull()
    _own_pull_request(reviewed_request["work_item_id"], pull)
    _complete(reviewed_request["id"])
    github.add_review(pull, review_id, f"@{MENTION} please rename the helper")
    github.calls.clear()

    _run_once(github)

    assert unavailable in github.failed_permissions
    events_path = f"/repos/{REPO}/issues/{unavailable}/events"
    event_index = next(
        index for index, request in enumerate(github.calls) if request.url.path == events_path
    )
    assert any(
        request.url.path == f"/repos/{REPO}/collaborators/{SENDER}/permission"
        for request in github.calls[event_index + 1 :]
    )
    unavailable_rows = _requests(unavailable)
    if lane == "labeled":
        assert unavailable_rows == []
    else:
        assert len(unavailable_rows) == 1
        assert unavailable_rows[0]["cancelled_at"] is None
        assert unavailable_rows[0]["status"] != "cancelled"
    healthy_rows = _requests(healthy)
    assert len(healthy_rows) == 1
    assert healthy_rows[0]["id"] == _label_request_id(healthy, github.issues[healthy].event_id)
    mentioned_rows = _requests(mentioned)
    reviewed_rows = _requests(reviewed)
    assert _mention_request_id(comment_id) in {row["id"] for row in mentioned_rows}
    assert _feedback_request_id("pull_request_review", review_id) in {
        row["id"] for row in reviewed_rows
    }
    github.permission_failures.remove(unavailable)

    _run_once(github)
    _run_once(github)

    recovered = _requests(unavailable)
    assert len(recovered) == 1
    if lane == "labeled":
        assert recovered[0]["id"] == _label_request_id(
            unavailable, github.issues[unavailable].event_id
        )
    else:
        assert recovered[0]["cancelled_at"] is not None
        assert recovered[0]["terminal_cause"] == "issue_cancelled"
    assert [row["id"] for row in _requests(healthy)] == [row["id"] for row in healthy_rows]
    assert [row["id"] for row in _requests(mentioned)] == [row["id"] for row in mentioned_rows]
    assert [row["id"] for row in _requests(reviewed)] == [row["id"] for row in reviewed_rows]


def test_poll_prunes_persisted_etags_for_cancelled_issues_and_closed_lineages(
    poll_factory: tuple[TestClient, PollGitHub],
) -> None:
    client, github = poll_factory
    cancelled, _cancelled_request = _admit_label(client, github)
    retired, retired_request = _admit_label(client, github)
    healthy, healthy_request = _admit_label(client, github)
    retired_pull, healthy_pull = github.plant_pull(), github.plant_pull()
    retired_lineage = _own_pull_request(retired_request["work_item_id"], retired_pull)
    _own_pull_request(healthy_request["work_item_id"], healthy_pull)
    _complete(healthy_request["id"])
    _run_once(github)
    before = _rows(
        "SELECT etags FROM curie.factory_poll_cursors WHERE repo_full_name = :repo",
        {"repo": REPO},
    )
    assert len(before) == 1
    cancelled_key = f"issue:{cancelled}:{LABEL}"
    retired_key, healthy_key = f"reviews:{retired_pull}", f"reviews:{healthy_pull}"
    assert cancelled_key in before[0]["etags"]
    assert retired_key in before[0]["etags"]
    assert healthy_key in before[0]["etags"]
    github.close_issue(cancelled)
    github.pulls[retired_pull]["state"] = "closed"
    _execute(
        "UPDATE curie.thread_publication_lineages SET status = 'closed', "
        "version = version + 1 WHERE id = :id",
        {"id": retired_lineage},
    )
    comment_id, review_id = 73504, 74504
    github.add_issue_comment(healthy, comment_id, f"@{MENTION} please revise the helper")
    github.add_review(healthy_pull, review_id, f"@{MENTION} please rename the helper")
    github._etags["issue-comments"] = '"healthy-mention-after-etag-pruning"'
    github._etags["reviews"] = '"healthy-review-after-etag-pruning"'

    _run_once(github)
    _run_once(github)

    assert _requests(cancelled)[0]["cancelled_at"] is not None
    after = _rows(
        "SELECT etags FROM curie.factory_poll_cursors WHERE repo_full_name = :repo",
        {"repo": REPO},
    )
    assert len(after) == 1
    assert cancelled_key not in after[0]["etags"]
    assert retired_key not in after[0]["etags"]
    assert after[0]["etags"][healthy_key] == github._etags["reviews"]
    assert f"issue:{healthy}:{LABEL}" in after[0]["etags"]
    healthy_rows = _requests(healthy)
    assert len(healthy_rows) == 3
    assert _mention_request_id(comment_id) in {row["id"] for row in healthy_rows}
    assert _feedback_request_id("pull_request_review", review_id) in {
        row["id"] for row in healthy_rows
    }


def test_stale_issue_conditional_read_still_cancels_after_an_issue_changes(
    poll_factory: tuple[TestClient, PollGitHub],
) -> None:
    client, github = poll_factory
    number, _row = _admit_label(client, github)
    _run_once(github)
    body = github._issue_body(github.issues[number])
    etag = f'"{hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()}"'
    github.calls.clear()
    github.saw_304 = False

    _run_once(github)

    reads = [
        request for request in github.calls if request.url.path == f"/repos/{REPO}/issues/{number}"
    ]
    # Current authority and status title reads may use the same issue route.
    # The stale scan itself must make exactly one authenticated conditional read.
    conditional = [request for request in reads if "if-none-match" in request.headers]
    assert len(conditional) == 1
    assert conditional[0].headers.get("authorization") == "Bearer fixture-installation-token"
    assert conditional[0].headers.get("if-none-match") == etag
    assert github.saw_304
    assert _requests(number)[0]["cancelled_at"] is None
    github.close_issue(number)
    github.calls.clear()

    _run_once(github)

    reads = [
        request for request in github.calls if request.url.path == f"/repos/{REPO}/issues/{number}"
    ]
    conditional = [request for request in reads if "if-none-match" in request.headers]
    assert len(conditional) == 1
    assert conditional[0].headers.get("if-none-match") == etag
    rows = _requests(number)
    assert rows[0]["cancelled_at"] is not None
    assert rows[0]["terminal_cause"] == "issue_cancelled"


def test_stale_issue_cache_does_not_cross_a_configured_label_change(
    poll_factory: tuple[TestClient, PollGitHub], monkeypatch: pytest.MonkeyPatch
) -> None:
    client, github = poll_factory
    number, _row = _admit_label(client, github)
    _run_once(github)
    monkeypatch.setenv("GITHUB_FACTORY_LABEL", f"{LABEL}-changed")
    get_settings.cache_clear()
    github.calls.clear()

    _run_once(github)

    reads = [
        request for request in github.calls if request.url.path == f"/repos/{REPO}/issues/{number}"
    ]
    assert reads
    assert all("if-none-match" not in request.headers for request in reads)
    # The changed label has no authoritative unlabeled event, so a fresh read
    # alone must not infer cancellation.
    assert _requests(number)[0]["cancelled_at"] is None


def test_poll_admits_a_mention_once(poll_factory: tuple[TestClient, PollGitHub]) -> None:
    client, github = poll_factory
    number, _row = _admit_label(client, github)
    comment_id = 73001
    body = f"@{MENTION} please revise the helper"
    github.add_issue_comment(number, comment_id, body)
    before = _delivery_ids()

    _run_once(github)

    rows = _requests(number)
    assert len(rows) == 2
    assert rows[1]["id"] == _mention_request_id(comment_id)
    assert rows[1]["status"] == "queued"
    assert _delivery_ids() == before

    _run_once(github)

    assert [row["id"] for row in _requests(number)] == [row["id"] for row in rows]


def test_poll_admits_review_feedback_once(
    poll_factory: tuple[TestClient, PollGitHub],
) -> None:
    client, github = poll_factory
    number, first = _admit_label(client, github)
    pull = github.plant_pull()
    _own_pull_request(first["work_item_id"], pull)
    _complete(first["id"])
    comment_body = f"@{MENTION} please rename the helper"
    review_body = f"@{MENTION} please rename the helper before merge"
    comment_id = 73101
    review_id = 74101
    github.add_review_comment(pull, comment_id, comment_body)
    github.add_review(pull, review_id, review_body)

    _run_once(github)

    rows = _requests(number)
    assert len(rows) == 3
    found = {row["id"] for row in rows}
    assert _feedback_request_id("pull_request_review_comment", comment_id) in found
    assert _feedback_request_id("pull_request_review", review_id) in found
    assert _requests(pull) == []

    _run_once(github)

    assert [row["id"] for row in _requests(number)] == [row["id"] for row in rows]


def test_webhook_of_a_polled_label_stays_one_request(
    poll_factory: tuple[TestClient, PollGitHub],
) -> None:
    client, github = poll_factory
    number = github.plant_issue()
    _run_once(github)
    rows = _requests(number)
    assert len(rows) == 1
    assert rows[0]["id"] == _label_request_id(number, github.issues[number].event_id)
    assert _delivery_ids() == set()
    payload = _issue_event("labeled", number, label={"name": LABEL})
    delivery = str(uuid.uuid4())

    response = _post(client, "issues", payload, delivery=delivery)

    assert response.status_code == 200, response.text
    assert [row["id"] for row in _requests(number)] == [rows[0]["id"]]
    _assert_receipt(delivery, payload, "issues")
    assert _delivery_ids() == {uuid.UUID(delivery)}


def test_webhook_of_a_polled_mention_stays_one_revision(
    poll_factory: tuple[TestClient, PollGitHub],
) -> None:
    client, github = poll_factory
    number, _row = _admit_label(client, github)
    comment_id = 73011
    body = f"@{MENTION} please revise the helper"
    github.add_issue_comment(number, comment_id, body)
    _run_once(github)
    rows = _requests(number)
    assert len(rows) == 2
    assert rows[1]["id"] == _mention_request_id(comment_id)
    before = _delivery_ids()
    payload = _comment_event(number, body, comment_id)
    delivery = str(uuid.uuid4())

    response = _post(client, "issue_comment", payload, delivery=delivery)

    assert response.status_code == 200, response.text
    assert len(_requests(number)) == 2
    assert _delivery_ids() == before | {uuid.UUID(delivery)}
    _assert_receipt(delivery, payload, "issue_comment")


def test_webhook_of_a_polled_review_stays_one_request(
    poll_factory: tuple[TestClient, PollGitHub],
) -> None:
    client, github = poll_factory
    number, first = _admit_label(client, github)
    pull = github.plant_pull()
    _own_pull_request(first["work_item_id"], pull)
    _complete(first["id"])
    review_id = 74111
    review = github.add_review(pull, review_id, f"@{MENTION} please rename the helper")
    _run_once(github)
    assert _feedback_request_id("pull_request_review", review_id) in {
        row["id"] for row in _requests(number)
    }
    count = len(_requests(number))
    before = _delivery_ids()
    payload = {
        "action": "submitted",
        "installation": {"id": INSTALLATION_ID},
        "repository": {"id": REPO_ID, "full_name": REPO},
        "sender": _user(),
        "pull_request": github.pulls[pull],
        "review": _public_review(review),
    }
    delivery = str(uuid.uuid4())

    response = _post(client, "pull_request_review", payload, delivery=delivery)

    assert response.status_code == 200, response.text
    assert len(_requests(number)) == count
    assert _delivery_ids() == before | {uuid.UUID(delivery)}
    _assert_receipt(delivery, payload, "pull_request_review")


def test_webhook_label_then_poll_does_not_add_a_request(
    poll_factory: tuple[TestClient, PollGitHub],
) -> None:
    client, github = poll_factory
    number = github.plant_issue()
    payload = _issue_event("labeled", number, label={"name": LABEL})
    delivery = str(uuid.uuid4())

    response = _post(client, "issues", payload, delivery=delivery)

    assert response.status_code == 200, response.text
    rows = _requests(number)
    assert len(rows) == 1
    assert rows[0]["id"] == _label_request_id(number, github.issues[number].event_id)
    _assert_receipt(delivery, payload, "issues")

    _run_once(github)

    assert [row["id"] for row in _requests(number)] == [rows[0]["id"]]


def test_second_poll_sends_a_conditional_request(
    poll_factory: tuple[TestClient, PollGitHub],
) -> None:
    _client, github = poll_factory
    number = github.plant_issue()

    _run_once(github)

    assert len(_requests(number)) == 1
    github.calls.clear()
    github.saw_304 = False

    _run_once(github)

    issue_listings = [
        request for request in github.calls if request.url.path == f"/repos/{REPO}/issues"
    ]
    assert len(issue_listings) == 1
    assert issue_listings[0].headers.get("if-none-match") == github._etags["issues"]
    assert github.saw_304
    assert len(_requests(number)) == 1


def test_locked_poll_makes_no_github_request(
    poll_factory: tuple[TestClient, PollGitHub],
) -> None:
    _client, github = poll_factory
    github.plant_issue()
    held = threading.Event()
    release = threading.Event()
    errors: list[BaseException] = []

    def hold() -> None:
        async def inner() -> None:
            url = make_url(get_settings().database_url).set(drivername="postgresql")
            connection = await asyncpg.connect(url.render_as_string(hide_password=False))
            try:
                locked = await connection.fetchval(
                    "SELECT pg_try_advisory_lock($1, $2)", *_POLL_LOCK
                )
                if locked is not True:
                    raise RuntimeError("poll lock was not acquired")
                held.set()
                while not release.wait(0.1):
                    pass
                await connection.execute("SELECT pg_advisory_unlock($1, $2)", *_POLL_LOCK)
            finally:
                await connection.close()

        try:
            asyncio.run(inner())
        except BaseException as exc:  # noqa: BLE001 - existing broad catch retained
            errors.append(exc)
            held.set()

    holder = threading.Thread(target=hold)
    holder.start()
    assert held.wait(10)
    try:
        if errors:
            raise errors[0]
        github.calls.clear()
        outcome: list[BaseException] = []

        def once() -> None:
            try:
                _run_once(github)
            except BaseException as exc:  # noqa: BLE001 - existing broad catch retained
                outcome.append(exc)

        runner = threading.Thread(target=once)
        runner.start()
        runner.join(20)
        assert not runner.is_alive(), "run_once blocked while the poll lock was held"
        if outcome:
            raise outcome[0]
        assert github.calls == []
    finally:
        release.set()
        holder.join(10)
        if errors:
            raise errors[0]


def test_webhook_mode_does_not_admit_inside_the_grace_period(
    webhook_factory: tuple[TestClient, PollGitHub],
) -> None:
    _client, github = webhook_factory
    number = github.plant_issue(age=timedelta(seconds=5))

    _run_once(github)

    assert any(request.url.path == f"/repos/{REPO}/issues" for request in github.calls)
    assert _requests(number) == []


def _prod_settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "environment": "prod",
        "api_key": "a-real-key",
        "approval_chat_attester_secret": "a-real-chat-attester-secret",
        "internal_worker_token": "a-real-worker-token",
        "github_webhook_secret": "",
        "github_factory_ingress_enabled": True,
        "github_factory_label": LABEL,
        "github_factory_mention": MENTION,
        "github_review_ingress_enabled": False,
        "github_app_id": "51",
        "github_app_private_key": "example-private-key",
        "github_repo_allowlist": ("acme-corp/*",),
        "github_factory_intake": "poll",
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def test_prod_poll_intake_boots_with_an_empty_webhook_secret() -> None:
    settings = _prod_settings()

    assert settings.github_factory_ingress_enabled is True
    assert settings.github_webhook_secret == ""
    assert settings.github_review_ingress_enabled is False


def test_prod_webhook_intake_refuses_an_empty_webhook_secret() -> None:
    with pytest.raises(ValueError, match="GITHUB_WEBHOOK_SECRET"):
        _prod_settings(github_factory_intake="webhook")


def test_poll_intake_with_review_ingress_still_requires_a_webhook_secret() -> None:
    with pytest.raises(ValueError, match="GITHUB_WEBHOOK_SECRET"):
        _prod_settings(github_review_ingress_enabled=True)


def _signed_webhook(client: TestClient, secret: str) -> httpx.Response:
    body = b'{"action":"labeled"}'
    signature = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return client.post(
        "/github/webhook",
        content=body,
        headers={
            "X-GitHub-Delivery": str(uuid.uuid4()),
            "X-GitHub-Event": "issues",
            "X-Hub-Signature-256": signature,
            "Content-Type": "application/json",
        },
    )


def test_empty_webhook_secret_rejects_its_own_hmac(
    monkeypatch: pytest.MonkeyPatch, clean_db: None, valkey: object
) -> None:
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("GITHUB_FACTORY_INGRESS_ENABLED", "false")
    monkeypatch.setenv("GITHUB_REVIEW_INGRESS_ENABLED", "false")
    monkeypatch.setenv("GITHUB_WEBHOOK_SECRET", "")
    monkeypatch.setenv("CURIE_WORK_ITEM_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("APPROVAL_SWEEP_INTERVAL_S", "0")
    monkeypatch.setenv("DEAD_LETTER_WATCH_INTERVAL_S", "0")
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as client:
            response = _signed_webhook(client, "")
    finally:
        get_settings.cache_clear()

    assert response.status_code == 401, response.text


def test_configured_webhook_secret_accepts_a_valid_signature(
    monkeypatch: pytest.MonkeyPatch, clean_db: None, valkey: object
) -> None:
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("GITHUB_FACTORY_INGRESS_ENABLED", "false")
    monkeypatch.setenv("GITHUB_REVIEW_INGRESS_ENABLED", "false")
    monkeypatch.setenv("GITHUB_WEBHOOK_SECRET", "example-factory-hmac-secret")
    monkeypatch.setenv("CURIE_WORK_ITEM_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("APPROVAL_SWEEP_INTERVAL_S", "0")
    monkeypatch.setenv("DEAD_LETTER_WATCH_INTERVAL_S", "0")
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as client:
            response = _signed_webhook(client, "example-factory-hmac-secret")
    finally:
        get_settings.cache_clear()

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "ignored"
