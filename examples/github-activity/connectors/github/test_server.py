"""Tests for the GitHub activity connector.

These cover the parts that fail SILENTLY in production. A window boundary off by
one, a pagination loop that stops at page one, a closed PR counted as merged, or
a page cap that quietly drops the tail all leave a connector that starts, passes
a health check, and returns a plausible weekly summary. Nothing crashes; someone
just reads a wrong changelog. Everything here is a property that would otherwise
only be noticed that way.

The fakes replay GitHub REST shapes, including fields the connector ignores:
  https://docs.github.com/en/rest/issues/issues#list-repository-issues
  https://docs.github.com/en/rest/issues/events#list-issue-events-for-a-repository
  https://docs.github.com/en/rest/issues/milestones#list-milestones
Pagination is the documented Link header with rel="next" carrying an absolute URL:
  https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api
"""

import importlib.util
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import anyio
import httpx
import pytest
from mcp import Client
from mcp.server.mcpserver.exceptions import ToolError

# Load server.py BY PATH under a unique module name. Several bundles ship a file
# called server.py; importing it as `server` lets whichever suite runs first win
# the name, and the other silently tests the wrong module.
_MODULE_NAME = "github_activity_github_server"
_SERVER_PY = Path(__file__).parent / "server.py"

API = "https://api.github.com"
TOKEN = "ghp_testtoken_do_not_echo"
_ENV_KEYS = (
    "GITHUB_TOKEN",
    "GITHUB_REPOSITORIES",
    "GITHUB_API_URL",
    "GITHUB_MAX_PAGES",
    "GITHUB_TIMEOUT_SECONDS",
)

SINCE = "2026-09-01T00:00:00Z"
UNTIL = "2026-09-08T00:00:00Z"
SINCE_DT = datetime(2026, 9, 1, tzinfo=UTC)
UNTIL_DT = datetime(2026, 9, 8, tzinfo=UTC)

REPO_IDS = {"acme/api": 101, "acme/web": 202, "acme/docs": 303}


def _load(monkeypatch, **env):
    """Import server.py with a specific environment.

    Module-level config is read at import time, so each case needs a fresh
    import. Env goes through monkeypatch so a token never leaks into another
    suite in the same pytest process. Pass a key as None to leave it unset.
    """

    base = {"GITHUB_TOKEN": TOKEN, "GITHUB_REPOSITORIES": "acme/api,acme/web"}
    base.update(env)
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    for key, value in base.items():
        if value is not None:
            monkeypatch.setenv(key, value)

    sys.modules.pop(_MODULE_NAME, None)
    spec = importlib.util.spec_from_file_location(_MODULE_NAME, _SERVER_PY)
    module = importlib.util.module_from_spec(spec)
    sys.modules[_MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------------------- #
# GitHub REST payload builders. Realistic enough that a connector keyed on the
# wrong field (url instead of html_url, closed_at instead of merged_at, the
# `user` object instead of its login) gets a visibly wrong answer.
# --------------------------------------------------------------------------- #
def _user(login):
    return {
        "login": login,
        "id": abs(hash(login)) % 100000,
        "node_id": "MDQ6VXNlcjE=",
        "avatar_url": f"https://avatars.githubusercontent.com/{login}",
        "html_url": f"https://github.com/{login}",
        "type": "User",
        "site_admin": False,
    }


def _milestone(repo, number, title, *, state="open", created_at, closed_at=None, due_on=None):
    return {
        "url": f"{API}/repos/{repo}/milestones/{number}",
        "html_url": f"https://github.com/{repo}/milestone/{number}",
        "labels_url": f"{API}/repos/{repo}/milestones/{number}/labels",
        "id": 9000 + number,
        "node_id": "MDk6TWlsZXN0b25lMTAwMjYwNA==",
        "number": number,
        "state": state,
        "title": title,
        "description": "Tracking milestone",
        "creator": _user("pm-bot"),
        "open_issues": 4,
        "closed_issues": 8,
        "created_at": created_at,
        "updated_at": closed_at or created_at,
        "closed_at": closed_at,
        "due_on": due_on,
    }


def _issue(
    repo,
    number,
    title,
    *,
    user,
    created_at,
    updated_at,
    closed_at=None,
    state_reason=None,
    milestone=None,
    pr=False,
    merged_at=None,
):
    """One row of GET /repos/{o}/{r}/issues. PR rows carry a `pull_request` object."""

    kind = "pull" if pr else "issues"
    row = {
        "url": f"{API}/repos/{repo}/issues/{number}",
        "repository_url": f"{API}/repos/{repo}",
        "labels_url": f"{API}/repos/{repo}/issues/{number}/labels{{/name}}",
        "comments_url": f"{API}/repos/{repo}/issues/{number}/comments",
        "events_url": f"{API}/repos/{repo}/issues/{number}/events",
        "html_url": f"https://github.com/{repo}/{kind}/{number}",
        "id": 500000 + number,
        "node_id": "I_kwDOABCD",
        "number": number,
        "title": title,
        "user": _user(user),
        "labels": [{"id": 1, "name": "bug", "color": "d73a4a", "default": True}],
        "state": "closed" if closed_at else "open",
        "locked": False,
        "assignee": None,
        "assignees": [],
        "milestone": milestone,
        "comments": 3,
        "created_at": created_at,
        "updated_at": updated_at,
        "closed_at": closed_at,
        "author_association": "MEMBER",
        "body": "Some body text the connector should never return.",
        "reactions": {"total_count": 0, "+1": 0},
        "state_reason": state_reason,
    }
    if pr:
        row["draft"] = False
        row["pull_request"] = {
            "url": f"{API}/repos/{repo}/pulls/{number}",
            "html_url": f"https://github.com/{repo}/pull/{number}",
            "diff_url": f"https://github.com/{repo}/pull/{number}.diff",
            "patch_url": f"https://github.com/{repo}/pull/{number}.patch",
            "merged_at": merged_at,
        }
    return row


def _event(repo, event_id, event, *, actor, created_at, issue, milestone_title=None):
    """One row of GET /repos/{o}/{r}/issues/events."""

    row = {
        "id": event_id,
        "node_id": "MEE6RXZlbnQx",
        "url": f"{API}/repos/{repo}/issues/events/{event_id}",
        "actor": _user(actor),
        "event": event,
        "commit_id": None,
        "commit_url": None,
        "created_at": created_at,
        "performed_via_github_app": None,
        "issue": issue,
    }
    if milestone_title is not None:
        row["milestone"] = {"title": milestone_title}
    if event == "labeled":
        row["label"] = {"name": "bug", "color": "d73a4a"}
    return row


# --------------------------------------------------------------------------- #
# The fake GitHub. Every route is an exact URL. A request to anything else, or
# any non-GET httpx call, fails the test with pytest.fail, which raises a
# BaseException so a broad `except Exception` in the server cannot swallow it
# and turn it into a plausible ToolError.
# --------------------------------------------------------------------------- #
class FakeGitHub:
    def __init__(self):
        self.routes = {}
        self.calls = []

    def serve(self, url, status=200, body=None, link=None, headers=None, text=None, error=None):
        """Register one exact URL.

        `body` may be a callable taking the request params, for a route that has
        to honor a query filter the way GitHub does. `text` sends a raw non JSON
        body. `error` is an httpx exception raised instead of any response.
        """

        merged = dict(headers or {})
        if link:
            merged["Link"] = link
        self.routes[url] = (status, body, merged, text, error)

    def serve_pages(self, repo, endpoint, pages):
        """Register `pages` for one endpoint, chained by Link rel="next".

        Page 1 is the plain endpoint URL the connector builds. Later pages are
        the absolute URLs GitHub hands back, which use the numeric repository
        id rather than owner/name, so a connector that rebuilds the URL itself
        instead of following the link lands on an unrouted URL.
        """

        first = f"{API}/repos/{repo}/{endpoint}"
        rid = REPO_IDS[repo]
        urls = [first] + [
            f"{API}/repositories/{rid}/{endpoint}?per_page=100&page={n}"
            for n in range(2, len(pages) + 1)
        ]
        last = urls[-1]
        for i, page in enumerate(pages):
            link = None
            if i + 1 < len(pages):
                link = f'<{urls[i + 1]}>; rel="next", <{last}>; rel="last"'
            self.serve(urls[i], body=page, link=link)
        return urls

    def get(self, url, *args, **kwargs):
        if args:
            pytest.fail(
                f"httpx.get called with positional args {args!r}; pass params/headers by keyword"
            )
        self.calls.append(
            {
                "url": url,
                "params": kwargs.get("params"),
                "headers": kwargs.get("headers") or {},
                "timeout": kwargs.get("timeout"),
            }
        )
        if url not in self.routes:
            pytest.fail(f"unexpected GitHub request: {url}")
        status, body, headers, text, error = self.routes[url]
        request = httpx.Request("GET", url)
        if error is not None:
            raise error
        if text is not None:
            return httpx.Response(status, text=text, headers=headers, request=request)
        if callable(body):
            body = body(kwargs.get("params") or {})
        if body is None:
            return httpx.Response(status, headers=headers, request=request)
        return httpx.Response(status, json=body, headers=headers, request=request)

    def urls(self):
        return [c["url"] for c in self.calls]


def _install(monkeypatch, srv, fake):
    """Route srv.httpx.get to the fake and make every write-capable path fatal."""

    monkeypatch.setattr(srv.httpx, "get", fake.get)

    def forbidden(name):
        def _fail(*args, **kwargs):
            pytest.fail(f"connector called httpx.{name}; only httpx.get is allowed")

        return _fail

    for name in ("request", "post", "put", "patch", "delete", "stream", "Client", "AsyncClient"):
        monkeypatch.setattr(srv.httpx, name, forbidden(name))


def _empty_repo(fake, repo):
    for endpoint in ("issues", "issues/events", "milestones"):
        fake.serve_pages(repo, endpoint, [[]])


def _by_repo(result):
    return {entry["repository"]: entry for entry in result["repositories"]}


def _numbers(items):
    return sorted(item["number"] for item in items)


def _params(call):
    return {k: str(v) for k, v in (call["params"] or {}).items()}


# --------------------------------------------------------------------------- #
# The two-repo world used by the main case.
# --------------------------------------------------------------------------- #
def _seed_two_repos(fake):
    api = "acme/api"
    v11 = _milestone(
        api,
        11,
        "v1.1",
        state="closed",
        created_at="2026-06-01T00:00:00Z",
        closed_at="2026-09-02T12:00:00Z",
    )
    v12 = _milestone(
        api, 12, "v1.2", created_at="2026-08-15T00:00:00Z", due_on="2026-09-30T07:00:00Z"
    )
    v13 = _milestone(
        api, 13, "v1.3", created_at="2026-09-06T08:00:00Z", due_on="2026-10-01T07:00:00Z"
    )
    v10 = _milestone(
        api,
        10,
        "v1.0",
        state="closed",
        created_at="2026-05-01T00:00:00Z",
        closed_at="2026-06-01T00:00:00Z",
    )

    pr42 = _issue(
        api,
        42,
        "Add retry budget",
        user="alice",
        pr=True,
        milestone=v12,
        created_at="2026-09-02T10:00:00Z",
        updated_at="2026-09-04T15:00:00Z",
        closed_at="2026-09-04T15:00:00Z",
        merged_at="2026-09-04T15:00:00Z",
    )
    pr43 = _issue(
        api,
        43,
        "Abandoned refactor",
        user="bob",
        pr=True,
        created_at="2026-09-02T11:00:00Z",
        updated_at="2026-09-05T09:00:00Z",
        closed_at="2026-09-05T09:00:00Z",
        merged_at=None,
    )
    pr40 = _issue(
        api,
        40,
        "Merged last month",
        user="carol",
        pr=True,
        created_at="2026-08-20T10:00:00Z",
        updated_at="2026-09-02T09:00:00Z",
        closed_at="2026-08-30T10:00:00Z",
        merged_at="2026-08-30T10:00:00Z",
    )
    pr44 = _issue(
        api,
        44,
        "Merged at the until boundary",
        user="dave",
        pr=True,
        created_at="2026-09-03T10:00:00Z",
        updated_at="2026-09-08T00:00:00Z",
        closed_at="2026-09-08T00:00:00Z",
        merged_at="2026-09-08T00:00:00Z",
    )
    issue45 = _issue(
        api,
        45,
        "Login page 500s",
        user="bob",
        created_at="2026-09-05T08:30:00Z",
        updated_at="2026-09-05T09:00:00Z",
    )
    issue30 = _issue(
        api,
        30,
        "Old bug finally fixed",
        user="erin",
        milestone=v12,
        created_at="2026-07-01T00:00:00Z",
        updated_at="2026-09-06T10:00:00Z",
        closed_at="2026-09-06T10:00:00Z",
        state_reason="completed",
    )
    issue31 = _issue(
        api,
        31,
        "Old issue with a new comment",
        user="erin",
        created_at="2026-07-02T00:00:00Z",
        updated_at="2026-09-03T00:00:00Z",
    )
    issue46 = _issue(
        api,
        46,
        "Opened at the since boundary",
        user="frank",
        created_at="2026-09-01T00:00:00Z",
        updated_at="2026-09-02T00:00:00Z",
        closed_at="2026-09-02T00:00:00Z",
        state_reason="not_planned",
    )
    issue47 = _issue(
        api,
        47,
        "Opened at the until boundary",
        user="frank",
        created_at="2026-09-08T00:00:00Z",
        updated_at="2026-09-08T00:00:00Z",
    )
    fake.serve_pages(
        api, "issues", [[pr44, issue47, issue30, issue45, pr43, pr42, issue31, pr40, issue46]]
    )

    fake.serve_pages(
        api,
        "issues/events",
        [
            [
                _event(
                    api,
                    7006,
                    "milestoned",
                    actor="alice",
                    created_at="2026-09-08T01:00:00Z",
                    issue=issue45,
                    milestone_title="v1.3",
                ),
                _event(
                    api,
                    7005,
                    "labeled",
                    actor="bob",
                    created_at="2026-09-05T09:00:00Z",
                    issue=issue45,
                ),
                _event(
                    api,
                    7004,
                    "milestoned",
                    actor="alice",
                    created_at="2026-09-04T09:00:00Z",
                    issue=pr42,
                    milestone_title="v1.2",
                ),
                _event(
                    api,
                    7003,
                    "subscribed",
                    actor="carol",
                    created_at="2026-09-03T00:00:00Z",
                    issue=issue31,
                ),
                _event(
                    api,
                    7002,
                    "demilestoned",
                    actor="carol",
                    created_at="2026-09-02T12:00:00Z",
                    issue=issue30,
                    milestone_title="v1.1",
                ),
                _event(
                    api,
                    7001,
                    "milestoned",
                    actor="carol",
                    created_at="2026-08-31T23:59:59Z",
                    issue=issue30,
                    milestone_title="v1.1",
                ),
            ]
        ],
    )
    fake.serve_pages(api, "milestones", [[v13, v12, v11, v10]])

    web = "acme/web"
    w3 = _milestone(
        web,
        3,
        "Q3 launch",
        state="closed",
        created_at="2026-07-01T00:00:00Z",
        closed_at="2026-09-07T23:59:59Z",
    )
    pr7 = _issue(
        web,
        7,
        "New landing page",
        user="gina",
        pr=True,
        milestone=w3,
        created_at="2026-08-28T00:00:00Z",
        updated_at="2026-09-03T12:00:00Z",
        closed_at="2026-09-03T12:00:00Z",
        merged_at="2026-09-03T12:00:00Z",
    )
    issue8 = _issue(
        web,
        8,
        "Footer overlaps on mobile",
        user="hank",
        created_at="2026-09-04T00:00:00Z",
        updated_at="2026-09-04T00:00:00Z",
    )
    fake.serve_pages(web, "issues", [[issue8, pr7]])
    fake.serve_pages(
        web,
        "issues/events",
        [
            [
                _event(
                    web,
                    8801,
                    "milestoned",
                    actor="gina",
                    created_at="2026-09-03T11:00:00Z",
                    issue=pr7,
                    milestone_title="Q3 launch",
                ),
                _event(
                    web, 8800, "closed", actor="gina", created_at="2026-08-01T00:00:00Z", issue=pr7
                ),
            ]
        ],
    )
    fake.serve_pages(web, "milestones", [[w3]])


# --------------------------------------------------------------------------- #
# AC1 + AC2. One call, two repositories, every list populated, and nothing from
# one repo leaking into the other.
# --------------------------------------------------------------------------- #
def test_two_repositories_in_one_call_fill_every_list_per_repo(monkeypatch):
    srv = _load(monkeypatch)
    fake = FakeGitHub()
    _seed_two_repos(fake)
    _install(monkeypatch, srv, fake)

    result = srv.repository_activity(SINCE, UNTIL)

    assert datetime.fromisoformat(result["window"]["since"]) == SINCE_DT
    assert datetime.fromisoformat(result["window"]["until"]) == UNTIL_DT
    repos = _by_repo(result)
    assert set(repos) == {"acme/api", "acme/web"}
    api, web = repos["acme/api"], repos["acme/web"]

    # Merged means merged_at in [since, until). #40 merged before since, #44
    # merged exactly at until (half-open), #43 closed without merging.
    assert _numbers(api["merged_pull_requests"]) == [42]
    assert api["merged_pull_requests"][0] == {
        "number": 42,
        "title": "Add retry budget",
        "author": "alice",
        "merged_at": "2026-09-04T15:00:00Z",
        "url": "https://github.com/acme/api/pull/42",
        "milestone": "v1.2",
    }
    # Opened: #46 at the since boundary is in, #47 at until is out, #31 is old
    # but updated in window (the issues endpoint filters on updated_at, so this
    # row always comes back), and PRs #42/#43 were opened in window but are
    # never issues.
    assert _numbers(api["opened_issues"]) == [45, 46]
    assert next(i for i in api["opened_issues"] if i["number"] == 45) == {
        "number": 45,
        "title": "Login page 500s",
        "author": "bob",
        "created_at": "2026-09-05T08:30:00Z",
        "url": "https://github.com/acme/api/issues/45",
        "milestone": None,
    }
    # Closed: #30 is old but closed in window; PRs #42/#43 closed in window and
    # must not appear.
    assert _numbers(api["closed_issues"]) == [30, 46]
    assert next(i for i in api["closed_issues"] if i["number"] == 30) == {
        "number": 30,
        "title": "Old bug finally fixed",
        "author": "erin",
        "closed_at": "2026-09-06T10:00:00Z",
        "state_reason": "completed",
        "url": "https://github.com/acme/api/issues/30",
        "milestone": "v1.2",
    }
    assert (
        next(i for i in api["closed_issues"] if i["number"] == 46)["state_reason"] == "not_planned"
    )

    # Milestone movement: labeled and subscribed are other event types, one
    # milestoned is after until, one is before since.
    changes = sorted(api["milestone_changes"], key=lambda c: c["at"])
    assert changes == [
        {
            "number": 30,
            "kind": "issue",
            "title": "Old bug finally fixed",
            "event": "demilestoned",
            "milestone": "v1.1",
            "actor": "carol",
            "at": "2026-09-02T12:00:00Z",
        },
        {
            "number": 42,
            "kind": "pull_request",
            "title": "Add retry budget",
            "event": "milestoned",
            "milestone": "v1.2",
            "actor": "alice",
            "at": "2026-09-04T09:00:00Z",
        },
    ]

    # Milestones created or closed in window; v1.2 (open, older) and v1.0
    # (closed long ago) are out.
    milestones = {m["title"]: m for m in api["milestones"]}
    assert set(milestones) == {"v1.1", "v1.3"}
    assert milestones["v1.1"] == {
        "number": 11,
        "title": "v1.1",
        "state": "closed",
        "created_at": "2026-06-01T00:00:00Z",
        "closed_at": "2026-09-02T12:00:00Z",
        "due_on": None,
        "change": "closed",
    }
    assert milestones["v1.3"]["change"] == "created"
    assert milestones["v1.3"]["due_on"] == "2026-10-01T07:00:00Z"

    # The second repo carries only its own rows.
    assert _numbers(web["merged_pull_requests"]) == [7]
    assert web["merged_pull_requests"][0]["milestone"] == "Q3 launch"
    assert _numbers(web["opened_issues"]) == [8]
    assert web["closed_issues"] == []
    assert [(c["number"], c["kind"], c["event"]) for c in web["milestone_changes"]] == [
        (7, "pull_request", "milestoned")
    ]
    assert [(m["title"], m["change"]) for m in web["milestones"]] == [("Q3 launch", "closed")]

    # A complete read is not marked truncated.
    assert api["truncated"] == []
    assert web["truncated"] == []

    # The token is a credential, not data: it is in no returned value.
    assert TOKEN not in json.dumps(result)


def test_list_endpoints_are_called_with_the_documented_parameters(monkeypatch):
    # `since` on the issues endpoint is what keeps a busy repo from paging its
    # whole history; state=all is what makes closed rows visible at all.
    srv = _load(monkeypatch)
    fake = FakeGitHub()
    _seed_two_repos(fake)
    _install(monkeypatch, srv, fake)

    srv.repository_activity(SINCE, UNTIL)

    first_calls = {c["url"]: c for c in fake.calls}
    for repo in ("acme/api", "acme/web"):
        issues = _params(first_calls[f"{API}/repos/{repo}/issues"])
        # GitHub's `since` means updated AFTER, so the query starts one second
        # early and the local filter keeps the boundary inclusive.
        assert issues.pop("since") == "2026-08-31T23:59:59Z"
        assert issues == {"state": "all", "sort": "updated", "direction": "desc", "per_page": "100"}
        assert _params(first_calls[f"{API}/repos/{repo}/issues/events"]) == {"per_page": "100"}
        assert _params(first_calls[f"{API}/repos/{repo}/milestones"]) == {
            "state": "all",
            "per_page": "100",
        }


# --------------------------------------------------------------------------- #
# The inclusive lower boundary. GitHub's issues `since` returns rows updated
# AFTER the timestamp, so a row touched exactly at `since` and never again is
# dropped upstream before the local filter can keep it.
#   https://docs.github.com/en/rest/issues/issues#list-repository-issues
# --------------------------------------------------------------------------- #
def _updated_after(rows):
    """An issues route that filters the way GitHub does: updated_at > since."""

    def body(params):
        since = params.get("since")
        if since is None:
            return rows
        floor = datetime.fromisoformat(str(since))
        return [row for row in rows if datetime.fromisoformat(row["updated_at"]) > floor]

    return body


def test_issue_touched_exactly_at_since_survives_githubs_exclusive_filter(monkeypatch):
    srv = _load(monkeypatch, GITHUB_REPOSITORIES="acme/api")
    fake = FakeGitHub()
    repo = "acme/api"
    at_since = _issue(
        repo,
        100,
        "Opened at since and never touched again",
        user="ann",
        created_at=SINCE,
        updated_at=SINCE,
    )
    before = _issue(
        repo,
        101,
        "Last touched before the window",
        user="ann",
        created_at="2026-08-20T00:00:00Z",
        updated_at="2026-08-31T23:59:59Z",
    )
    fake.serve(f"{API}/repos/{repo}/issues", body=_updated_after([at_since, before]))
    fake.serve_pages(repo, "issues/events", [[]])
    fake.serve_pages(repo, "milestones", [[]])
    _install(monkeypatch, srv, fake)

    result = _by_repo(srv.repository_activity(SINCE, UNTIL))[repo]

    assert _numbers(result["opened_issues"]) == [100]


# --------------------------------------------------------------------------- #
# Historical closures. The issues endpoint shows CURRENT state, so an issue
# closed in the window and reopened since reads as never closed. The events
# feed keeps the closure, and is what the window actually asks about.
# --------------------------------------------------------------------------- #
def _events_issue(row):
    """The issue object embedded in an issue event, per the documented shape."""

    keep = ("number", "title", "user", "html_url", "state", "state_reason", "milestone")
    issue = {key: row[key] for key in keep}
    if "pull_request" in row:
        issue["pull_request"] = row["pull_request"]
    return issue


def _closure_world(fake, repo, issues, events):
    fake.serve_pages(repo, "issues", [issues])
    # One event older than since ends the events read as a covered window.
    anchor = _issue(
        repo, 1, "Ancient", user="ann", created_at="2026-01-01T00:00:00Z", updated_at=SINCE
    )
    old = _event(repo, 1, "labeled", actor="ann", created_at="2026-08-01T00:00:00Z", issue=anchor)
    fake.serve_pages(repo, "issues/events", [[*events, old]])
    fake.serve_pages(repo, "milestones", [[]])


def test_issue_closed_in_window_then_reopened_is_still_a_closure(monkeypatch):
    srv = _load(monkeypatch, GITHUB_REPOSITORIES="acme/api")
    fake = FakeGitHub()
    repo = "acme/api"
    reopened = _issue(
        repo,
        110,
        "Closed then reopened",
        user="bob",
        created_at="2026-08-01T00:00:00Z",
        updated_at="2026-09-10T00:00:00Z",
    )
    assert reopened["state"] == "open" and reopened["closed_at"] is None
    embedded = _events_issue(reopened)
    _closure_world(
        fake,
        repo,
        [reopened],
        [
            _event(
                repo, 3, "reopened", actor="bob", created_at="2026-09-10T00:00:00Z", issue=embedded
            ),
            _event(
                repo, 2, "closed", actor="bob", created_at="2026-09-03T12:00:00Z", issue=embedded
            ),
        ],
    )
    _install(monkeypatch, srv, fake)

    result = _by_repo(srv.repository_activity(SINCE, UNTIL))[repo]

    assert [(i["number"], i["closed_at"]) for i in result["closed_issues"]] == [
        (110, "2026-09-03T12:00:00Z")
    ]
    closure = result["closed_issues"][0]
    assert closure["title"] == "Closed then reopened"
    assert closure["author"] == "bob"
    assert closure["url"] == "https://github.com/acme/api/issues/110"


def test_closure_in_window_survives_a_later_second_closure(monkeypatch):
    # The current row's closed_at is the SECOND closure, after until. Reading it
    # alone loses the in-window closure entirely.
    srv = _load(monkeypatch, GITHUB_REPOSITORIES="acme/api")
    fake = FakeGitHub()
    repo = "acme/api"
    twice = _issue(
        repo,
        111,
        "Closed twice",
        user="bob",
        created_at="2026-08-01T00:00:00Z",
        updated_at="2026-09-12T00:00:00Z",
        closed_at="2026-09-12T00:00:00Z",
        state_reason="completed",
    )
    embedded = _events_issue(twice)
    _closure_world(
        fake,
        repo,
        [twice],
        [
            _event(
                repo, 4, "closed", actor="bob", created_at="2026-09-12T00:00:00Z", issue=embedded
            ),
            _event(
                repo, 3, "reopened", actor="bob", created_at="2026-09-09T00:00:00Z", issue=embedded
            ),
            _event(
                repo, 2, "closed", actor="bob", created_at="2026-09-02T08:00:00Z", issue=embedded
            ),
        ],
    )
    _install(monkeypatch, srv, fake)

    result = _by_repo(srv.repository_activity(SINCE, UNTIL))[repo]

    assert [(i["number"], i["closed_at"]) for i in result["closed_issues"]] == [
        (111, "2026-09-02T08:00:00Z")
    ]


def test_closure_seen_on_the_row_and_in_events_is_reported_once(monkeypatch):
    srv = _load(monkeypatch, GITHUB_REPOSITORIES="acme/api")
    fake = FakeGitHub()
    repo = "acme/api"
    closed = _issue(
        repo,
        112,
        "Closed once",
        user="bob",
        created_at="2026-08-01T00:00:00Z",
        updated_at="2026-09-04T00:00:00Z",
        closed_at="2026-09-04T00:00:00Z",
        state_reason="completed",
    )
    _closure_world(
        fake,
        repo,
        [closed],
        [
            _event(
                repo,
                2,
                "closed",
                actor="bob",
                created_at="2026-09-04T00:00:00Z",
                issue=_events_issue(closed),
            ),
        ],
    )
    _install(monkeypatch, srv, fake)

    result = _by_repo(srv.repository_activity(SINCE, UNTIL))[repo]

    assert [(i["number"], i["closed_at"]) for i in result["closed_issues"]] == [
        (112, "2026-09-04T00:00:00Z")
    ]
    assert result["closed_issues"][0]["state_reason"] == "completed"


def test_closed_event_for_a_pull_request_is_not_an_issue_closure(monkeypatch):
    srv = _load(monkeypatch, GITHUB_REPOSITORIES="acme/api")
    fake = FakeGitHub()
    repo = "acme/api"
    pr = _issue(
        repo,
        113,
        "Closed without merging",
        user="bob",
        pr=True,
        created_at="2026-08-01T00:00:00Z",
        updated_at="2026-09-04T00:00:00Z",
        closed_at="2026-09-04T00:00:00Z",
    )
    _closure_world(
        fake,
        repo,
        [pr],
        [
            _event(
                repo,
                2,
                "closed",
                actor="bob",
                created_at="2026-09-04T00:00:00Z",
                issue=_events_issue(pr),
            ),
        ],
    )
    _install(monkeypatch, srv, fake)

    result = _by_repo(srv.repository_activity(SINCE, UNTIL))[repo]

    assert result["closed_issues"] == []
    assert result["merged_pull_requests"] == []


# --------------------------------------------------------------------------- #
# Pagination. A connector that reads page one only returns a short, plausible
# list on any busy repo, which is exactly the repo someone asks about.
# --------------------------------------------------------------------------- #
def test_issues_follow_the_link_header_to_the_next_page(monkeypatch):
    srv = _load(monkeypatch, GITHUB_REPOSITORIES="acme/api")
    fake = FakeGitHub()
    repo = "acme/api"
    page1 = [
        _issue(
            repo,
            50,
            "Fresh issue",
            user="ann",
            created_at="2026-09-06T00:00:00Z",
            updated_at="2026-09-06T00:00:00Z",
        ),
    ]
    page2 = [
        _issue(
            repo,
            51,
            "Merged on page two",
            user="ben",
            pr=True,
            created_at="2026-09-02T00:00:00Z",
            updated_at="2026-09-03T00:00:00Z",
            closed_at="2026-09-03T00:00:00Z",
            merged_at="2026-09-03T00:00:00Z",
        ),
    ]
    urls = fake.serve_pages(repo, "issues", [page1, page2])
    fake.serve_pages(repo, "issues/events", [[]])
    fake.serve_pages(repo, "milestones", [[]])
    _install(monkeypatch, srv, fake)

    result = _by_repo(srv.repository_activity(SINCE, UNTIL))[repo]

    assert _numbers(result["opened_issues"]) == [50]
    assert _numbers(result["merged_pull_requests"]) == [51]
    assert [u for u in fake.urls() if "issues" in u and "events" not in u] == urls
    assert result["truncated"] == []


@pytest.mark.parametrize(
    "foreign",
    [
        "https://evil.example.com/repos/x/y/issues?page=2",
        # A lookalike host that shares the configured URL as a prefix.
        "https://api.github.com.evil.example.com/repos/x/y/issues?page=2",
        # Same host, plain HTTP: the token would cross the wire in the clear.
        "http://api.github.com/repos/acme/api/issues?page=2",
    ],
)
def test_foreign_pagination_link_is_refused_before_any_request_reaches_it(monkeypatch, foreign):
    # A truncated silent read would be worse than a refusal, so this is a
    # ToolError, and the token never travels to the linked host.
    srv = _load(monkeypatch, GITHUB_REPOSITORIES="acme/api")
    fake = FakeGitHub()
    repo = "acme/api"
    page1 = [
        _issue(
            repo,
            120,
            "Page one",
            user="ann",
            created_at="2026-09-02T00:00:00Z",
            updated_at="2026-09-02T00:00:00Z",
        )
    ]
    fake.serve(f"{API}/repos/{repo}/issues", body=page1, link=f'<{foreign}>; rel="next"')
    fake.serve(foreign, body=[])
    fake.serve_pages(repo, "issues/events", [[]])
    fake.serve_pages(repo, "milestones", [[]])
    _install(monkeypatch, srv, fake)

    with pytest.raises(ToolError):
        srv.repository_activity(SINCE, UNTIL)
    assert foreign not in fake.urls()
    assert all(c["url"].startswith(f"{API}/") for c in fake.calls)
    sent_to = [c for c in fake.calls if "evil.example.com" in c["url"] or c["url"] == foreign]
    assert sent_to == []


def test_events_paging_stops_once_an_event_is_older_than_since(monkeypatch):
    # The repository events feed has no `since` filter and runs back to the
    # repo's creation, so the stop condition is the only thing bounding it.
    srv = _load(monkeypatch, GITHUB_REPOSITORIES="acme/api")
    fake = FakeGitHub()
    repo = "acme/api"
    issue = _issue(
        repo,
        60,
        "Tracked work",
        user="ann",
        created_at="2026-08-01T00:00:00Z",
        updated_at="2026-09-05T00:00:00Z",
    )
    page1 = [
        _event(
            repo,
            9003,
            "milestoned",
            actor="ann",
            created_at="2026-09-05T00:00:00Z",
            issue=issue,
            milestone_title="v2",
        ),
    ]
    page2 = [
        _event(
            repo,
            9002,
            "demilestoned",
            actor="ann",
            created_at="2026-09-01T00:00:01Z",
            issue=issue,
            milestone_title="v1",
        ),
        _event(
            repo,
            9001,
            "milestoned",
            actor="ann",
            created_at="2026-08-31T00:00:00Z",
            issue=issue,
            milestone_title="v1",
        ),
    ]
    page3 = [
        _event(
            repo,
            9000,
            "milestoned",
            actor="ann",
            created_at="2026-08-01T00:00:00Z",
            issue=issue,
            milestone_title="v0",
        ),
    ]
    urls = fake.serve_pages(repo, "issues/events", [page1, page2, page3])
    fake.serve_pages(repo, "issues", [[]])
    fake.serve_pages(repo, "milestones", [[]])
    _install(monkeypatch, srv, fake)

    result = _by_repo(srv.repository_activity(SINCE, UNTIL))[repo]

    assert sorted((c["event"], c["milestone"]) for c in result["milestone_changes"]) == [
        ("demilestoned", "v1"),
        ("milestoned", "v2"),
    ]
    assert urls[0] in fake.urls()
    assert urls[1] in fake.urls()
    assert urls[2] not in fake.urls()
    # Stopping because the window was covered is a complete read, not a cap.
    assert result["truncated"] == []


@pytest.mark.parametrize(
    "endpoint,name",
    [("issues", "issues"), ("issues/events", "events"), ("milestones", "milestones")],
)
def test_page_cap_names_the_truncated_endpoint(monkeypatch, endpoint, name):
    # Hitting the cap must be visible. A silent cap is a short list that reads
    # as "quiet week".
    srv = _load(monkeypatch, GITHUB_REPOSITORIES="acme/api", GITHUB_MAX_PAGES="2")
    fake = FakeGitHub()
    repo = "acme/api"
    issue = _issue(
        repo,
        70,
        "Busy",
        user="ann",
        created_at="2026-09-02T00:00:00Z",
        updated_at="2026-09-02T00:00:00Z",
    )
    rows = {
        "issues": lambda n: [issue | {"number": 1000 + n}],
        "issues/events": lambda n: [
            _event(
                repo,
                20000 + n,
                "milestoned",
                actor="ann",
                created_at="2026-09-05T00:00:00Z",
                issue=issue,
                milestone_title="v9",
            )
        ],
        "milestones": lambda n: [
            _milestone(repo, 300 + n, f"m{n}", created_at="2026-09-03T00:00:00Z")
        ],
    }
    for other in ("issues", "issues/events", "milestones"):
        if other != endpoint:
            fake.serve_pages(repo, other, [[]])
    urls = fake.serve_pages(repo, endpoint, [rows[endpoint](n) for n in range(5)])
    _install(monkeypatch, srv, fake)

    result = _by_repo(srv.repository_activity(SINCE, UNTIL))[repo]

    assert result["truncated"] == [name]
    assert [u for u in fake.urls() if u in urls] == urls[:2]


def test_reading_exactly_the_page_cap_with_no_next_link_is_not_truncated(monkeypatch):
    # The other direction: a flag raised whenever the page count equals the cap
    # cries wolf on every repo whose history happens to be that size.
    srv = _load(monkeypatch, GITHUB_REPOSITORIES="acme/api", GITHUB_MAX_PAGES="2")
    fake = FakeGitHub()
    repo = "acme/api"
    issue = _issue(
        repo,
        71,
        "Fits",
        user="ann",
        created_at="2026-09-02T00:00:00Z",
        updated_at="2026-09-02T00:00:00Z",
    )
    fake.serve_pages(repo, "issues", [[issue], [issue | {"number": 72}]])
    fake.serve_pages(repo, "issues/events", [[]])
    fake.serve_pages(repo, "milestones", [[]])
    _install(monkeypatch, srv, fake)

    result = _by_repo(srv.repository_activity(SINCE, UNTIL))[repo]

    assert result["truncated"] == []
    assert _numbers(result["opened_issues"]) == [71, 72]


def test_default_page_cap_is_ten(monkeypatch):
    srv = _load(monkeypatch, GITHUB_REPOSITORIES="acme/api")
    fake = FakeGitHub()
    repo = "acme/api"
    issue = _issue(
        repo,
        80,
        "Busy",
        user="ann",
        created_at="2026-09-02T00:00:00Z",
        updated_at="2026-09-02T00:00:00Z",
    )
    urls = fake.serve_pages(repo, "issues", [[issue | {"number": 2000 + n}] for n in range(12)])
    fake.serve_pages(repo, "issues/events", [[]])
    fake.serve_pages(repo, "milestones", [[]])
    _install(monkeypatch, srv, fake)

    result = _by_repo(srv.repository_activity(SINCE, UNTIL))[repo]

    assert [u for u in fake.urls() if u in urls] == urls[:10]
    assert result["truncated"] == ["issues"]


# --------------------------------------------------------------------------- #
# AC3 + AC4. Read only. The credential is the real boundary; these pin that the
# connector itself never sends anything but a GET and offers nothing to write.
# --------------------------------------------------------------------------- #
def test_every_request_is_a_get_carrying_the_token_only_in_authorization(monkeypatch):
    srv = _load(monkeypatch, GITHUB_TIMEOUT_SECONDS="7")
    fake = FakeGitHub()
    _seed_two_repos(fake)
    # _install makes httpx.request/post/put/patch/delete/stream/Client fatal, so
    # reaching the assertions below means only httpx.get was used.
    _install(monkeypatch, srv, fake)

    srv.repository_activity(SINCE, UNTIL)

    assert len(fake.calls) == 6
    for call in fake.calls:
        assert call["headers"]["Authorization"] == f"Bearer {TOKEN}"
        assert TOKEN not in call["url"]
        assert TOKEN not in json.dumps(call["params"] or {})
        # An unbounded read hangs the agent's turn on a slow GitHub.
        assert call["timeout"] == 7


def test_only_the_read_only_activity_tool_is_registered(monkeypatch):
    # readOnlyHint is not the boundary, the token is; but a write-shaped tool
    # should never reach a prompt-injectable agent's context, and the bundle's
    # toolPolicy allows exactly this one name.
    srv = _load(monkeypatch)
    tools = anyio.run(srv.mcp.list_tools)
    assert {tool.name for tool in tools} == {"repository_activity"}
    for tool in tools:
        assert tool.annotations is not None
        assert tool.annotations.read_only_hint is True
        assert tool.annotations.destructive_hint is False


def _client_call(srv, name, arguments):
    """Call one tool through a real MCP client session, in process."""

    async def go():
        async with Client(srv.mcp) as client:
            return await client.call_tool(name, arguments)

    return anyio.run(go)


def _result_payload(result):
    if isinstance(result.structured_content, dict) and "repositories" in result.structured_content:
        return result.structured_content
    return json.loads(result.content[0].text)


@pytest.mark.parametrize(
    "name,arguments",
    [
        ("create_issue", {"repository": "acme/api", "title": "pwned", "body": "x"}),
        ("add_issue_comment", {"repository": "acme/api", "issue_number": 1, "body": "x"}),
    ],
)
def test_write_through_the_mcp_client_is_refused_and_a_read_still_works(
    monkeypatch, name, arguments
):
    # The consumer path is an MCP client, not a Python call. A write name is
    # refused there with nothing sent upstream, and the same configuration
    # still answers a two repository read through that same path.
    srv = _load(monkeypatch)
    fake = FakeGitHub()
    _seed_two_repos(fake)
    _install(monkeypatch, srv, fake)

    refused = _client_call(srv, name, arguments)
    assert refused.is_error is True
    assert fake.calls == []

    read = _client_call(srv, "repository_activity", {"since": SINCE, "until": UNTIL})
    assert read.is_error is False
    payload = _result_payload(read)
    assert {r["repository"] for r in payload["repositories"]} == {"acme/api", "acme/web"}
    assert _numbers(_by_repo(payload)["acme/api"]["merged_pull_requests"]) == [42]
    assert TOKEN not in json.dumps(payload)


def test_streamable_http_is_mounted_at_curie_connector_path(monkeypatch):
    srv = _load(monkeypatch)
    assert [route.path for route in srv.mcp.streamable_http_app().routes] == ["/mcp"]


def test_custom_api_url_is_used_without_a_double_slash(monkeypatch):
    # GitHub Enterprise bases end in /api/v3; a trailing slash that becomes
    # //repos is a 404 that reads as "repository not found".
    srv = _load(
        monkeypatch,
        GITHUB_API_URL="https://ghe.example.com/api/v3/",
        GITHUB_REPOSITORIES="acme/api",
    )
    fake = FakeGitHub()
    for endpoint in ("issues", "issues/events", "milestones"):
        fake.serve(f"https://ghe.example.com/api/v3/repos/acme/api/{endpoint}", body=[])
    _install(monkeypatch, srv, fake)

    srv.repository_activity(SINCE, UNTIL)

    assert sorted(fake.urls()) == [
        "https://ghe.example.com/api/v3/repos/acme/api/issues",
        "https://ghe.example.com/api/v3/repos/acme/api/issues/events",
        "https://ghe.example.com/api/v3/repos/acme/api/milestones",
    ]


# --------------------------------------------------------------------------- #
# Repository scope. GITHUB_REPOSITORIES is both the default set and an
# allowlist, so a prompt cannot steer the token at a repo the operator did not
# name. Refusals happen before any request.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("asked", [["evil/repo"], ["acme/api", "evil/repo"]])
def test_repository_outside_the_allowlist_is_refused_before_any_request(monkeypatch, asked):
    srv = _load(monkeypatch)
    fake = FakeGitHub()
    _seed_two_repos(fake)
    _install(monkeypatch, srv, fake)

    with pytest.raises(ToolError) as excinfo:
        srv.repository_activity(SINCE, UNTIL, repositories=asked)
    assert "evil/repo" in str(excinfo.value)
    assert fake.calls == []


def test_repository_inside_the_allowlist_is_read_alone(monkeypatch):
    # Liveness for the refusal above: a listed repo is read, and only it.
    srv = _load(monkeypatch)
    fake = FakeGitHub()
    _seed_two_repos(fake)
    _install(monkeypatch, srv, fake)

    result = srv.repository_activity(SINCE, UNTIL, repositories=["acme/web"])

    assert [r["repository"] for r in result["repositories"]] == ["acme/web"]
    assert all("/repos/acme/web/" in url for url in fake.urls())


def test_no_allowlist_and_no_repositories_argument_is_refused(monkeypatch):
    srv = _load(monkeypatch, GITHUB_REPOSITORIES=None)
    fake = FakeGitHub()
    _install(monkeypatch, srv, fake)

    with pytest.raises(ToolError):
        srv.repository_activity(SINCE, UNTIL)
    assert fake.calls == []


def test_no_allowlist_reads_the_repositories_named_in_the_call(monkeypatch):
    srv = _load(monkeypatch, GITHUB_REPOSITORIES=None)
    fake = FakeGitHub()
    _empty_repo(fake, "acme/docs")
    _install(monkeypatch, srv, fake)

    result = srv.repository_activity(SINCE, UNTIL, repositories=["acme/docs"])

    assert [r["repository"] for r in result["repositories"]] == ["acme/docs"]


@pytest.mark.parametrize("bad", ["not-a-repo", "acme/api/issues", "acme/", "/api"])
def test_malformed_repository_name_is_refused_before_any_request(monkeypatch, bad):
    # Unvalidated, "acme/api/issues" builds a URL to a different endpoint.
    srv = _load(monkeypatch, GITHUB_REPOSITORIES=None)
    fake = FakeGitHub()
    _install(monkeypatch, srv, fake)

    with pytest.raises(ToolError):
        srv.repository_activity(SINCE, UNTIL, repositories=[bad])
    assert fake.calls == []


# --------------------------------------------------------------------------- #
# The window. A since that silently parses as "now" or "epoch" produces either
# an empty week or the whole history, both plausible.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "since,until",
    [
        ("yesterday", UNTIL),
        ("", UNTIL),
        (SINCE, "next week"),
        (SINCE, SINCE),
        (UNTIL, SINCE),
    ],
)
def test_invalid_or_empty_window_is_refused_before_any_request(monkeypatch, since, until):
    srv = _load(monkeypatch)
    fake = FakeGitHub()
    _install(monkeypatch, srv, fake)

    with pytest.raises(ToolError):
        srv.repository_activity(since, until)
    assert fake.calls == []


def test_valid_since_with_empty_until_reads_up_to_now(monkeypatch):
    srv = _load(monkeypatch, GITHUB_REPOSITORIES="acme/api")
    fake = FakeGitHub()
    repo = "acme/api"
    fake.serve_pages(
        repo,
        "issues",
        [
            [
                _issue(
                    repo,
                    90,
                    "Opened after the old until",
                    user="ann",
                    created_at="2026-09-20T00:00:00Z",
                    updated_at="2026-09-20T00:00:00Z",
                ),
            ]
        ],
    )
    fake.serve_pages(repo, "issues/events", [[]])
    fake.serve_pages(repo, "milestones", [[]])
    _install(monkeypatch, srv, fake)

    before = datetime.now(UTC)
    result = srv.repository_activity(SINCE)
    after = datetime.now(UTC)

    until = datetime.fromisoformat(result["window"]["until"])
    assert before - timedelta(seconds=1) <= until <= after + timedelta(seconds=1)
    assert _numbers(_by_repo(result)[repo]["opened_issues"]) == [90]


def test_offset_timestamps_mean_the_same_instant_as_z(monkeypatch):
    # 2026-08-31T20:00-04:00 is SINCE. Treating the offset as local or dropping
    # it shifts the window by hours and moves boundary rows in or out.
    srv = _load(monkeypatch)
    fake = FakeGitHub()
    _seed_two_repos(fake)
    _install(monkeypatch, srv, fake)

    result = srv.repository_activity("2026-08-31T20:00:00-04:00", "2026-09-07T20:00:00-04:00")

    assert datetime.fromisoformat(result["window"]["since"]) == SINCE_DT
    assert datetime.fromisoformat(result["window"]["until"]) == UNTIL_DT
    api = _by_repo(result)["acme/api"]
    assert _numbers(api["opened_issues"]) == [45, 46]
    assert _numbers(api["merged_pull_requests"]) == [42]


# --------------------------------------------------------------------------- #
# Errors. Every one is a ToolError carrying a sentence, never the token.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("status", [401, 403])
def test_auth_failures_name_the_credential_without_echoing_it(monkeypatch, status):
    srv = _load(monkeypatch, GITHUB_REPOSITORIES="acme/api")
    fake = FakeGitHub()
    body = {
        "message": "Bad credentials" if status == 401 else "Resource not accessible by integration",
        "documentation_url": "https://docs.github.com/rest",
        "status": str(status),
    }
    for endpoint in ("issues", "issues/events", "milestones"):
        fake.serve(f"{API}/repos/acme/api/{endpoint}", status=status, body=body)
    _install(monkeypatch, srv, fake)

    with pytest.raises(ToolError) as excinfo:
        srv.repository_activity(SINCE, UNTIL)
    message = str(excinfo.value)
    assert "token" in message.lower() or "credential" in message.lower()
    assert TOKEN not in message


def test_not_found_names_the_repository(monkeypatch):
    # GitHub answers 404, not 403, for a private repo the token cannot see, so
    # the repo name is what lets someone fix the right thing.
    srv = _load(monkeypatch, GITHUB_REPOSITORIES="acme/api")
    fake = FakeGitHub()
    body = {
        "message": "Not Found",
        "documentation_url": "https://docs.github.com/rest",
        "status": "404",
    }
    for endpoint in ("issues", "issues/events", "milestones"):
        fake.serve(f"{API}/repos/acme/api/{endpoint}", status=404, body=body)
    _install(monkeypatch, srv, fake)

    with pytest.raises(ToolError) as excinfo:
        srv.repository_activity(SINCE, UNTIL)
    assert "acme/api" in str(excinfo.value)
    assert TOKEN not in str(excinfo.value)


def _one_failing_repo(fake, repo, **route):
    fake.serve(f"{API}/repos/{repo}/issues", **route)
    fake.serve_pages(repo, "issues/events", [[]])
    fake.serve_pages(repo, "milestones", [[]])


@pytest.mark.parametrize(
    "route",
    [
        pytest.param(
            {"status": 500, "body": {"message": f"upstream echoed Bearer {TOKEN}"}},
            id="500-json",
        ),
        pytest.param({"status": 500, "text": f"<html>proxy saw {TOKEN}</html>"}, id="500-text"),
        pytest.param({"status": 200, "text": f"not json {TOKEN}"}, id="200-non-json"),
        pytest.param(
            {"error": httpx.ConnectError(f"proxy refused Authorization: Bearer {TOKEN}")},
            id="transport",
        ),
    ],
)
def test_upstream_diagnostics_never_carry_the_token(monkeypatch, route):
    # GitHub, a proxy, or an httpx error string can all echo the credential.
    # Whatever reaches the agent as an error must not.
    srv = _load(monkeypatch, GITHUB_REPOSITORIES="acme/api")
    fake = FakeGitHub()
    _one_failing_repo(fake, "acme/api", **route)
    _install(monkeypatch, srv, fake)

    with pytest.raises(ToolError) as excinfo:
        srv.repository_activity(SINCE, UNTIL)
    assert TOKEN not in str(excinfo.value)
    assert TOKEN not in repr(excinfo.value.args)


_SECONDARY = {
    "message": "You have exceeded a secondary rate limit. Please wait a few minutes before "
    "you try again.",
    "documentation_url": "https://docs.github.com/rest/using-the-rest-api/rate-limits-for-the-rest-api",
}


def test_secondary_rate_limit_403_is_reported_as_throttling(monkeypatch):
    # Secondary limits answer 403 with quota still remaining and a Retry-After.
    # Calling that a missing permission sends someone to re-mint a good token.
    srv = _load(monkeypatch, GITHUB_REPOSITORIES="acme/api")
    fake = FakeGitHub()
    _one_failing_repo(
        fake,
        "acme/api",
        status=403,
        body=_SECONDARY,
        headers={"x-ratelimit-remaining": "4999", "Retry-After": "60"},
    )
    _install(monkeypatch, srv, fake)

    with pytest.raises(ToolError) as excinfo:
        srv.repository_activity(SINCE, UNTIL)
    message = str(excinfo.value).lower()
    assert "rate limit" in message
    assert "lacks" not in message
    assert "will not fix itself" not in message


@pytest.mark.parametrize(
    "headers",
    [
        {"x-ratelimit-remaining": "4999"},
        # Throttling needs real evidence (429, x-ratelimit-remaining "0", or a
        # rate limit message in the body); Retry-After alone does not turn a
        # permission refusal into a throttle. See
        # https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api#exceeding-the-rate-limit
        {"x-ratelimit-remaining": "4999", "Retry-After": "60"},
    ],
    ids=["no-retry-after", "with-retry-after"],
)
def test_genuine_403_still_says_the_credential_lacks_access(monkeypatch, headers):
    srv = _load(monkeypatch, GITHUB_REPOSITORIES="acme/api")
    fake = FakeGitHub()
    _one_failing_repo(
        fake,
        "acme/api",
        status=403,
        body={"message": "Resource not accessible by integration"},
        headers=headers,
    )
    _install(monkeypatch, srv, fake)

    with pytest.raises(ToolError) as excinfo:
        srv.repository_activity(SINCE, UNTIL)
    message = str(excinfo.value).lower()
    assert "lacks" in message
    assert "rate limit" not in message


def test_read_with_rate_limit_headers_on_a_200_succeeds(monkeypatch):
    # Liveness for the two above: the headers alone are not a refusal.
    srv = _load(monkeypatch, GITHUB_REPOSITORIES="acme/api")
    fake = FakeGitHub()
    repo = "acme/api"
    row = _issue(
        repo,
        130,
        "Read fine",
        user="ann",
        created_at="2026-09-02T00:00:00Z",
        updated_at="2026-09-02T00:00:00Z",
    )
    _one_failing_repo(
        fake,
        repo,
        status=200,
        body=[row],
        headers={"x-ratelimit-remaining": "4999", "Retry-After": "60"},
    )
    _install(monkeypatch, srv, fake)

    result = _by_repo(srv.repository_activity(SINCE, UNTIL))[repo]

    assert _numbers(result["opened_issues"]) == [130]


# --------------------------------------------------------------------------- #
# Startup. A connector that answers "not configured" to every call looks healthy
# to Kubernetes and is useless to the agent, so main() exits non-zero.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("token", [None, ""])
def test_main_refuses_to_start_without_a_token(monkeypatch, token):
    srv = _load(monkeypatch, GITHUB_TOKEN=token)
    assert srv.main() == 1


def test_main_with_a_token_serves_streamable_http_at_mcp(monkeypatch):
    # The pair to the refusal above: a configured connector actually starts.
    srv = _load(monkeypatch, PORT="8123", BIND_ADDRESS="127.0.0.1")
    seen = {}

    def run(**kwargs):
        seen.update(kwargs)

    monkeypatch.setattr(srv.mcp, "run", run)

    assert srv.main() == 0
    assert seen["transport"] == "streamable-http"
    assert seen["streamable_http_path"] == "/mcp"
    assert seen["port"] == 8123
    assert seen["host"] == "127.0.0.1"
