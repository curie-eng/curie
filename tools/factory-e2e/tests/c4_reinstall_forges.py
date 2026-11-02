"""External HTTP fixtures for the #4629 reinstall regression (test_c4_reinstall_publication.py).

Nothing in here is product code. Every class stands in for a system outside
Curie, answering the requests the production adapters make in the shapes the
providers document, with placeholder identities only:

1. `GitHubFake`: one GitHub with one repository, its App, installation and
   people. It authenticates the App JWT with the App's public key, mints
   installation tokens, and serves the issue, event, comment, permission, git
   ref, commit and pull request reads and writes. Each endpoint cites the REST
   reference it replays.
2. `C4GitLab`: `forge_fakes.gitlab.GitLabAPI` (unchanged) plus the
   ``author_id`` filter of the merge request listing and the fault knobs the
   rediscovery listing needs; `GitLabHost` routes several such projects on one
   host.
3. `Router`: the one transport every outbound ``httpx`` client of the API gets,
   by host. Jira is `C4Jira` (`forge_fakes.jira.JiraFake`) served through ASGI.
   `SharedClock` is the one wall clock a Jira site and the GitLab it publishes
   to share, so a Jira changelog time and a merge request's ``closed_at`` or
   ``merged_at`` order the way they happened.
4. `JobCluster`: the worker's `PublicationCluster` seam (the Kubernetes Job).
   ``apply`` performs the Job's external effect, the commit pushed to the
   publication branch on the fake forge, and ``observe`` reports the Job log's
   ``CURIE_COMMIT_SHA=<sha>`` marker, exactly as the publish script prints it.
"""

from __future__ import annotations

import base64
import hashlib
import itertools
import json
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote, unquote, urlencode

import httpx
import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from forge_fakes import gitlab as gitlab_fake
from forge_fakes import jira as jira_fake

GITHUB_API = "https://api.github.com"
GITHUB_WEB = "https://github.com"
GITHUB_HOST = "api.github.com"
APP_ID = 51
APP_SLUG = "acme-factory"
BOT_LOGIN = f"{APP_SLUG}[bot]"
BOT_USER_ID = 9100
INSTALLATION_ID = 5501
OWNER = "acme-corp"
REPO = f"{OWNER}/acme-bot"
REPO_ID = 4401
HUMAN = {"login": "octocat", "id": 6601, "type": "User"}
DEFAULT_BRANCH = "main"
PUBLICATION_BRANCH = re.compile(r"(?:^|/)publication-[0-9a-f]{32}$")


def generate_app_key() -> tuple[str, Any]:
    """A fresh RSA App key: its PEM (the install's secret) and its public key."""

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    ).decode()
    return pem, key.public_key()


def iso(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json(status: int, payload: Any, headers: dict[str, str] | None = None) -> httpx.Response:
    return httpx.Response(status, json=payload, headers=headers or {})


def _github_error(status: int, message: str) -> httpx.Response:
    # https://docs.github.com/en/rest/using-the-rest-api/troubleshooting-the-rest-api
    return _json(
        status,
        {
            "message": message,
            "documentation_url": "https://docs.github.com/rest",
            "status": str(status),
        },
    )


@dataclass
class GhIssue:
    number: int
    title: str
    state: str = "open"
    labels: list[str] = field(default_factory=list)
    events: list[dict[str, Any]] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class GhPull:
    number: int
    id: int
    head: str
    base: str
    title: str
    body: str
    user: dict[str, Any]
    created_at: datetime
    updated_at: datetime
    state: str = "open"
    draft: bool = False
    closed_at: datetime | None = None
    merged_at: datetime | None = None
    frozen_sha: str | None = None


class GitHubFake:
    """One GitHub repository, the factory App installed on it, and its people."""

    def __init__(self, public_key: Any) -> None:
        self.public_key = public_key
        self.lock = threading.Lock()
        self.requests: list[httpx.Request] = []
        self.issues: dict[int, GhIssue] = {}
        self.pulls: dict[int, GhPull] = {}
        self.comments: list[dict[str, Any]] = []
        self.commits: dict[str, dict[str, Any]] = {}
        self.branches: dict[str, str] = {}
        self.tokens: set[str] = set()
        self.permissions = {HUMAN["login"]: "write"}
        self._numbers = itertools.count(7)
        self._ids = itertools.count(880001)
        self._shas = itertools.count(1)
        # Fault knobs for the repository-wide pull request listing (no ``head``).
        self.listing_status: int | None = None
        self.listing_fail_page: int | None = None
        self.malformed_author: set[int] = set()
        # Pull request number -> "missing" (no ``body`` key) or "not_text" (an
        # object where the documented ``string or null`` is).
        self.body_fault: dict[int, str] = {}
        self._etags: dict[str, str] = {}
        self.push(DEFAULT_BRANCH, "initial", parent=None)

    # Seeding -----------------------------------------------------------------

    def push(self, branch: str, message: str, *, parent: str | None | type = ...) -> str:
        """A commit on ``branch`` whose parent is ``parent`` (default: the branch head,
        else the default branch head)."""

        if parent is ...:
            parent = self.branches.get(branch) or self.branches.get(DEFAULT_BRANCH)
        sha = hashlib.sha1(f"{next(self._shas)}:{message}:{parent}".encode()).hexdigest()
        self.commits[sha] = {"message": message, "parents": [parent] if parent else []}
        self.branches[branch] = sha
        return sha

    def add_issue(
        self,
        title: str,
        *,
        labels: tuple[str, ...] = (),
        labeled_at: datetime,
        actor: dict[str, Any] = HUMAN,
        extra_labels: tuple[str, ...] = (),
    ) -> int:
        number = next(self._numbers)
        issue = GhIssue(number, title, created_at=labeled_at - timedelta(hours=1))
        self.issues[number] = issue
        for label in labels:
            self.label(number, label, actor, at=labeled_at)
        for label in extra_labels:
            self.label(number, label, self.bot_user(), at=labeled_at + timedelta(minutes=5))
        return number

    def label(
        self, number: int, label: str, actor: dict[str, Any], *, at: datetime, add: bool = True
    ) -> None:
        issue = self.issues[number]
        if add and label not in issue.labels:
            issue.labels.append(label)
        if not add and label in issue.labels:
            issue.labels.remove(label)
        app = actor.get("type") == "Bot"
        issue.events.append(
            {
                "id": next(self._ids),
                "node_id": f"LE_{next(self._ids)}",
                "url": f"{GITHUB_API}/repos/{REPO}/issues/events/{len(issue.events) + 1}",
                "actor": dict(actor),
                "event": "labeled" if add else "unlabeled",
                "commit_id": None,
                "commit_url": None,
                "created_at": iso(at),
                "label": {"name": label, "color": "0e8a16"},
                "performed_via_github_app": self.app_summary() if app else None,
            }
        )
        issue.updated_at = max(issue.updated_at, at)

    def add_pull(
        self,
        *,
        body: str,
        author: dict[str, Any],
        head: str,
        created_at: datetime,
        title: str = "Fix the widget parser",
        state: str = "open",
        merged: bool = False,
        closed_at: datetime | None = None,
    ) -> int:
        if head not in self.branches:
            self.push(head, f"{title}\n\nCurie-Revision: 00000000-0000-4000-8000-000000000000")
        number = next(self._numbers)
        pull = GhPull(
            number,
            next(self._ids),
            head,
            DEFAULT_BRANCH,
            title,
            body,
            dict(author),
            created_at,
            created_at,
        )
        self.pulls[number] = pull
        if state != "open":
            self.close_pull(number, merged=merged, at=closed_at or created_at + timedelta(hours=1))
        return number

    def close_pull(self, number: int, *, merged: bool, at: datetime) -> None:
        pull = self.pulls[number]
        pull.state = "closed"
        pull.frozen_sha = self.branches[pull.head]
        pull.closed_at = at
        pull.merged_at = at if merged else None
        pull.updated_at = at

    def bot_user(self) -> dict[str, Any]:
        return {"login": BOT_LOGIN, "id": BOT_USER_ID, "type": "Bot"}

    def app_summary(self) -> dict[str, Any]:
        return {"id": APP_ID, "slug": APP_SLUG, "name": APP_SLUG}

    # Observations -------------------------------------------------------------

    def open_pulls(self) -> dict[int, tuple[int, str]]:
        """Open pull requests: number -> (immutable id, head branch)."""

        return {n: (p.id, p.head) for n, p in self.pulls.items() if p.state == "open"}

    def pull_url(self, number: int) -> str:
        return f"{GITHUB_WEB}/{REPO}/pull/{number}"

    def bot_comments(self, number: int) -> list[str]:
        return [
            c["body"]
            for c in self.comments
            if c["_number"] == number and c["user"]["login"] == BOT_LOGIN
        ]

    def repository_listings(self) -> list[httpx.Request]:
        """Requests for the repository-wide pull request listing (no ``head``)."""

        return [
            r
            for r in self.requests
            if r.method == "GET"
            and r.url.path == f"/repos/{REPO}/pulls"
            and "head" not in r.url.params
        ]

    # Rendering ----------------------------------------------------------------

    def _repo(self) -> dict[str, Any]:
        # https://docs.github.com/en/rest/repos/repos#get-a-repository
        return {
            "id": REPO_ID,
            "node_id": "R_kgDOacme",
            "name": REPO.split("/")[1],
            "full_name": REPO,
            "private": True,
            "owner": {"login": OWNER, "id": 4400, "type": "Organization"},
            "html_url": f"{GITHUB_WEB}/{REPO}",
            "url": f"{GITHUB_API}/repos/{REPO}",
            "default_branch": DEFAULT_BRANCH,
            "fork": False,
        }

    def _issue(self, issue: GhIssue) -> dict[str, Any]:
        # https://docs.github.com/en/rest/issues/issues#get-an-issue
        return {
            "id": 300000 + issue.number,
            "node_id": f"I_kwacme{issue.number}",
            "url": f"{GITHUB_API}/repos/{REPO}/issues/{issue.number}",
            "html_url": f"{GITHUB_WEB}/{REPO}/issues/{issue.number}",
            "number": issue.number,
            "state": issue.state,
            "title": issue.title,
            "body": "The widget parser drops the last field of a line.",
            "user": dict(HUMAN),
            "labels": [{"id": 700 + i, "name": name} for i, name in enumerate(issue.labels)],
            "assignees": [],
            "comments": sum(1 for c in self.comments if c["_number"] == issue.number),
            "created_at": iso(issue.created_at),
            "updated_at": iso(issue.updated_at),
            "closed_at": None,
            "author_association": "MEMBER",
        }

    def _side(self, ref: str, sha: str) -> dict[str, Any]:
        return {
            "label": f"{OWNER}:{ref}",
            "ref": ref,
            "sha": sha,
            "user": {"login": OWNER, "id": 4400, "type": "Organization"},
            "repo": self._repo(),
        }

    def _pull(self, pull: GhPull, *, single: bool) -> dict[str, Any]:
        # https://docs.github.com/en/rest/pulls/pulls#list-pull-requests
        # https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request (adds merged,
        # mergeable and mergeable_state to the listing's fields)
        head_sha = pull.frozen_sha or self.branches[pull.head]
        user: Any = dict(pull.user)
        if pull.number in self.malformed_author:
            user = pull.user["login"]
        payload: dict[str, Any] = {
            "url": f"{GITHUB_API}/repos/{REPO}/pulls/{pull.number}",
            "id": pull.id,
            "node_id": f"PR_kwacme{pull.id}",
            "html_url": self.pull_url(pull.number),
            "number": pull.number,
            "state": pull.state,
            "locked": False,
            "title": pull.title,
            "user": user,
            "body": pull.body,
            "labels": [],
            "created_at": iso(pull.created_at),
            "updated_at": iso(pull.updated_at),
            "closed_at": iso(pull.closed_at) if pull.closed_at else None,
            "merged_at": iso(pull.merged_at) if pull.merged_at else None,
            "merge_commit_sha": None,
            "draft": pull.draft,
            "head": self._side(pull.head, head_sha),
            "base": self._side(pull.base, self.branches[pull.base]),
            "author_association": "NONE" if user == self.bot_user() else "MEMBER",
        }
        fault = self.body_fault.get(pull.number)
        if fault == "missing":
            del payload["body"]
        elif fault == "not_text":
            payload["body"] = {"raw": pull.body}
        if single:
            payload.update(
                {
                    "merged": pull.merged_at is not None,
                    "mergeable": True if pull.state == "open" else None,
                    "mergeable_state": "clean",
                }
            )
        return payload

    def _comment(self, comment: dict[str, Any]) -> dict[str, Any]:
        # https://docs.github.com/en/rest/issues/comments#get-an-issue-comment
        return {key: value for key, value in comment.items() if not key.startswith("_")}

    # Transport ----------------------------------------------------------------

    def handle(self, request: httpx.Request) -> httpx.Response:
        with self.lock:
            self.requests.append(request)
            return self._answer(request)

    def _caller(self, request: httpx.Request) -> str:
        """``jwt`` for the App, ``installation`` for an installation token, else ``none``."""

        header = request.headers.get("authorization", "")
        scheme, _, value = header.partition(" ")
        if scheme.lower() == "basic":
            try:
                user, _, token = base64.b64decode(value).decode().partition(":")
            except ValueError:
                return "none"
            return "installation" if user == "x-access-token" and token in self.tokens else "none"
        if scheme.lower() not in ("bearer", "token"):
            return "none"
        if value in self.tokens:
            return "installation"
        try:
            # https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-a-json-web-token-jwt-for-a-github-app
            claims = jwt.decode(value, self.public_key, algorithms=["RS256"])
        except jwt.PyJWTError:
            return "none"
        return "jwt" if str(claims.get("iss")) == str(APP_ID) else "none"

    def _answer(self, request: httpx.Request) -> httpx.Response:
        caller = self._caller(request)
        path = request.url.path.rstrip("/")
        method = request.method
        if caller == "none":
            return _github_error(401, "Bad credentials")
        if caller == "jwt":
            return self._as_app(request, method, path)
        if path == "/user":
            # An installation token is not a user: GET /user answers 403.
            # https://docs.github.com/en/rest/users/users#get-the-authenticated-user
            return _github_error(403, "Resource not accessible by integration")
        repo = f"/repos/{REPO}"
        if path in (repo, f"/repositories/{REPO_ID}"):
            return _json(200, self._repo())
        if not path.startswith(repo + "/"):
            return _github_error(404, "Not Found")
        tail = path[len(repo) + 1 :]
        body = json.loads(request.content) if request.content else {}
        return self._repository(request, method, tail, body)

    def _as_app(self, request: httpx.Request, method: str, path: str) -> httpx.Response:
        if path == "/app" and method == "GET":
            # https://docs.github.com/en/rest/apps/apps#get-the-authenticated-app
            return _json(
                200,
                {
                    "id": APP_ID,
                    "slug": APP_SLUG,
                    "node_id": "A_kwacme51",
                    "name": APP_SLUG,
                    "owner": {"login": OWNER, "id": 4400, "type": "Organization"},
                    "html_url": f"{GITHUB_WEB}/apps/{APP_SLUG}",
                    "permissions": {"issues": "write", "pull_requests": "write"},
                    "events": ["issues"],
                },
            )
        if path == f"/repos/{REPO}/installation" and method == "GET":
            # https://docs.github.com/en/rest/apps/apps#get-a-repository-installation-for-the-authenticated-app
            return _json(
                200,
                {
                    "id": INSTALLATION_ID,
                    "app_id": APP_ID,
                    "app_slug": APP_SLUG,
                    "account": {"login": OWNER, "id": 4400, "type": "Organization"},
                    "target_type": "Organization",
                    "repository_selection": "selected",
                },
            )
        if path == f"/app/installations/{INSTALLATION_ID}/access_tokens" and method == "POST":
            # https://docs.github.com/en/rest/apps/apps#create-an-installation-access-token-for-an-app
            token = f"ghs_fixture{next(self._ids)}"
            self.tokens.add(token)
            expires = datetime.now(UTC) + timedelta(hours=1)
            return _json(
                201,
                {
                    "token": token,
                    "expires_at": iso(expires),
                    "permissions": {"issues": "write", "pull_requests": "write"},
                    "repository_selection": "selected",
                },
            )
        return _github_error(404, "Not Found")

    def _page(
        self,
        request: httpx.Request,
        items: list[dict[str, Any]],
        *,
        etag_key: str | None = None,
        fail_page: int | None = None,
    ) -> httpx.Response:
        """One page, with ``Link`` relations and optional conditional ETag.

        https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api
        https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api#use-conditional-requests-if-appropriate
        """

        params = request.url.params
        per_page = max(1, min(int(params.get("per_page", "30")), 100))
        page = max(1, int(params.get("page", "1")))
        if fail_page is not None and page == fail_page:
            return _github_error(502, "Server Error")
        start = (page - 1) * per_page
        values = items[start : start + per_page]
        last = max(1, -(-len(items) // per_page))
        headers: dict[str, str] = {}
        relations = []
        base = {k: v for k, v in params.multi_items() if k != "page"}
        url = f"{GITHUB_API}{request.url.path}"
        if page < last:
            relations.append(f'<{url}?{urlencode({**base, "page": page + 1})}>; rel="next"')
            relations.append(f'<{url}?{urlencode({**base, "page": last})}>; rel="last"')
        if page > 1:
            relations.append(f'<{url}?{urlencode({**base, "page": page - 1})}>; rel="prev"')
            relations.append(f'<{url}?{urlencode({**base, "page": 1})}>; rel="first"')
        if relations:
            headers["Link"] = ", ".join(relations)
        if etag_key is not None:
            digest = hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()
            etag = f'"{digest[:32]}"'
            if request.headers.get("if-none-match") == etag:
                return httpx.Response(304, headers={"ETag": etag})
            headers["ETag"] = etag
        return _json(200, values, headers)

    def _repository(
        self, request: httpx.Request, method: str, tail: str, body: dict[str, Any]
    ) -> httpx.Response:
        parts = tail.split("/")
        params = request.url.params
        if parts == ["issues"] and method == "GET":
            # https://docs.github.com/en/rest/issues/issues#list-repository-issues
            wanted = [x for x in params.get("labels", "").split(",") if x]
            state = params.get("state", "open")
            listed = [
                self._issue(i)
                for i in sorted(self.issues.values(), key=lambda i: -i.number)
                if (state == "all" or i.state == state) and all(w in i.labels for w in wanted)
            ]
            return self._page(request, listed, etag_key="issues")
        if parts == ["issues", "comments"] and method == "GET":
            # https://docs.github.com/en/rest/issues/comments#list-issue-comments-for-a-repository
            return self._page(
                request, [self._comment(c) for c in self.comments], etag_key="comments"
            )
        if len(parts) == 3 and parts[:2] == ["issues", "comments"]:
            found = [c for c in self.comments if str(c["id"]) == parts[2]]
            if not found:
                return _github_error(404, "Not Found")
            if method == "PATCH":
                # https://docs.github.com/en/rest/issues/comments#update-an-issue-comment
                found[0]["body"] = body.get("body", found[0]["body"])
                found[0]["updated_at"] = iso(datetime.now(UTC))
            return _json(200, self._comment(found[0]))
        if len(parts) >= 2 and parts[0] == "issues" and parts[1].isdigit():
            return self._one_issue(request, method, int(parts[1]), parts[2:], body)
        if len(parts) == 3 and parts[0] == "collaborators" and parts[2] == "permission":
            # https://docs.github.com/en/rest/collaborators/collaborators#get-repository-permissions-for-a-user
            login = unquote(parts[1])
            permission = self.permissions.get(login, "none")
            return _json(
                200,
                {
                    "permission": permission,
                    "role_name": permission,
                    "user": {**HUMAN, "login": login} if login == HUMAN["login"] else None,
                },
            )
        if parts[:3] == ["git", "ref", "heads"]:
            # https://docs.github.com/en/rest/git/refs#get-a-reference
            branch = unquote("/".join(parts[3:]))
            sha = self.branches.get(branch)
            if sha is None:
                return _github_error(404, "Not Found")
            return _json(
                200,
                {
                    "ref": f"refs/heads/{branch}",
                    "node_id": "REF_kwacme",
                    "url": f"{GITHUB_API}/repos/{REPO}/git/refs/heads/{quote(branch)}",
                    "object": {"sha": sha, "type": "commit", "url": ""},
                },
            )
        if parts[:3] == ["git", "matching-refs", "heads"]:
            # https://docs.github.com/en/rest/git/refs#list-matching-references
            prefix = unquote("/".join(parts[3:]))
            return _json(
                200,
                [
                    {"ref": f"refs/heads/{branch}", "object": {"sha": sha, "type": "commit"}}
                    for branch, sha in self.branches.items()
                    if branch.startswith(prefix)
                ],
            )
        if parts[:2] == ["git", "commits"] and len(parts) == 3:
            # https://docs.github.com/en/rest/git/commits#get-a-commit-object
            commit = self.commits.get(parts[2])
            if commit is None:
                return _github_error(404, "Not Found")
            return _json(
                200,
                {
                    "sha": parts[2],
                    "node_id": f"C_{parts[2][:12]}",
                    "message": commit["message"],
                    "parents": [{"sha": p, "url": "", "html_url": ""} for p in commit["parents"]],
                    "tree": {"sha": "f" * 40, "url": ""},
                    "author": {"name": "Curie Publisher", "email": "publisher@example.test"},
                    "committer": {"name": "Curie Publisher", "email": "publisher@example.test"},
                    "verification": {"verified": False, "reason": "unsigned"},
                },
            )
        if parts == ["pulls"] and method == "GET":
            return self._list_pulls(request)
        if parts == ["pulls"] and method == "POST":
            return self._open_pull(body)
        if parts == ["pulls", "comments"] and method == "GET":
            # https://docs.github.com/en/rest/pulls/comments#list-review-comments-in-a-repository
            return self._page(request, [], etag_key="review-comments")
        if len(parts) >= 2 and parts[0] == "pulls" and parts[1].isdigit():
            pull = self.pulls.get(int(parts[1]))
            if pull is None:
                return _github_error(404, "Not Found")
            if len(parts) == 2 and method == "PATCH":
                # https://docs.github.com/en/rest/pulls/pulls#update-a-pull-request
                pull.title = body.get("title", pull.title)
                pull.body = body.get("body", pull.body)
                pull.updated_at = datetime.now(UTC)
                return _json(200, self._pull(pull, single=True))
            if len(parts) == 2:
                return _json(200, self._pull(pull, single=True))
            if parts[2] in ("reviews", "comments"):
                # https://docs.github.com/en/rest/pulls/reviews#list-reviews-for-a-pull-request
                return self._page(request, [])
        if len(parts) == 3 and parts[0] == "commits" and parts[2] == "check-runs":
            # https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference
            return _json(200, {"total_count": 0, "check_runs": []})
        if len(parts) == 3 and parts[0] == "commits" and parts[2] == "status":
            # https://docs.github.com/en/rest/commits/statuses#get-the-combined-status-for-a-specific-reference
            return _json(200, {"state": "pending", "statuses": [], "total_count": 0})
        return _github_error(404, "Not Found")

    def _one_issue(
        self,
        request: httpx.Request,
        method: str,
        number: int,
        rest: list[str],
        body: dict[str, Any],
    ) -> httpx.Response:
        issue = self.issues.get(number)
        if issue is None and number in self.pulls and not rest:
            return _json(
                200,
                {
                    "number": number,
                    "state": self.pulls[number].state,
                    "labels": [],
                    "pull_request": {"url": f"{GITHUB_API}/repos/{REPO}/pulls/{number}"},
                },
            )
        if issue is None:
            return _github_error(404, "Not Found")
        if not rest:
            payload = self._issue(issue)
            etag = (
                '"'
                + hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:32]
                + '"'
            )
            if request.headers.get("if-none-match") == etag:
                return httpx.Response(304, headers={"ETag": etag})
            return _json(200, payload, {"ETag": etag})
        if rest == ["events"]:
            # https://docs.github.com/en/rest/issues/events#list-issue-events
            return self._page(request, list(issue.events))
        if rest == ["comments"] and method == "POST":
            # https://docs.github.com/en/rest/issues/comments#create-an-issue-comment
            # The installation's token writes as the App's bot user.
            comment_id = next(self._ids)
            now = iso(datetime.now(UTC))
            comment = {
                "id": comment_id,
                "node_id": f"IC_kwacme{comment_id}",
                "url": f"{GITHUB_API}/repos/{REPO}/issues/comments/{comment_id}",
                "html_url": f"{GITHUB_WEB}/{REPO}/issues/{number}#issuecomment-{comment_id}",
                "issue_url": f"{GITHUB_API}/repos/{REPO}/issues/{number}",
                "body": body.get("body", ""),
                "user": self.bot_user(),
                "created_at": now,
                "updated_at": now,
                "author_association": "NONE",
                "performed_via_github_app": self.app_summary(),
                "_number": number,
            }
            self.comments.append(comment)
            return _json(201, self._comment(comment))
        if rest == ["comments"]:
            # https://docs.github.com/en/rest/issues/comments#list-issue-comments
            return self._page(
                request, [self._comment(c) for c in self.comments if c["_number"] == number]
            )
        if rest == ["labels"] and method == "GET":
            # https://docs.github.com/en/rest/issues/labels#list-labels-for-an-issue
            return self._page(request, [{"name": n} for n in issue.labels])
        if rest == ["labels"] and method == "POST":
            # https://docs.github.com/en/rest/issues/labels#add-labels-to-an-issue
            for name in body.get("labels", []):
                self.label(number, name, self.bot_user(), at=datetime.now(UTC))
            return _json(200, [{"name": n} for n in issue.labels])
        if len(rest) == 2 and rest[0] == "labels" and method == "DELETE":
            # https://docs.github.com/en/rest/issues/labels#remove-a-label-from-an-issue
            name = unquote(rest[1])
            if name not in issue.labels:
                return _github_error(404, "Label does not exist")
            self.label(number, name, self.bot_user(), at=datetime.now(UTC), add=False)
            return _json(200, [{"name": n} for n in issue.labels])
        return _github_error(404, "Not Found")

    def _list_pulls(self, request: httpx.Request) -> httpx.Response:
        # https://docs.github.com/en/rest/pulls/pulls#list-pull-requests
        params = request.url.params
        head = params.get("head")
        state = params.get("state", "open")
        found = [
            p
            for p in sorted(self.pulls.values(), key=lambda p: -p.number)
            if state == "all" or p.state == state
        ]
        if head is not None:
            owner, _, branch = head.partition(":")
            if owner != OWNER:
                found = []
            found = [p for p in found if p.head == branch]
        elif self.listing_status is not None:
            # A 404 is GitHub's answer for a repository the token cannot see.
            message = "Not Found" if self.listing_status == 404 else "Server Error"
            return _github_error(self.listing_status, message)
        return self._page(
            request,
            [self._pull(p, single=False) for p in found],
            fail_page=None if head is not None else self.listing_fail_page,
        )

    def _open_pull(self, body: dict[str, Any]) -> httpx.Response:
        # https://docs.github.com/en/rest/pulls/pulls#create-a-pull-request
        head, base = body.get("head", ""), body.get("base", "")
        if head not in self.branches or base not in self.branches:
            return _json(422, {"message": "Validation Failed", "errors": [{"field": "head"}]})
        if any(p.head == head and p.state == "open" for p in self.pulls.values()):
            return _json(
                422,
                {
                    "message": "Validation Failed",
                    "errors": [{"message": f"A pull request already exists for {OWNER}:{head}."}],
                },
            )
        now = datetime.now(UTC)
        number = next(self._numbers)
        pull = GhPull(
            number,
            next(self._ids),
            head,
            base,
            body.get("title", ""),
            body.get("body") or "",
            self.bot_user(),
            now,
            now,
            draft=bool(body.get("draft", False)),
        )
        self.pulls[number] = pull
        return _json(201, self._pull(pull, single=True))


class SharedClock:
    """One wall clock for every fake that shares it, moving only forward.

    A Jira site publishing to a GitLab project needs it: a Jira changelog
    ``created`` and a merge request's ``closed_at`` or ``merged_at`` are
    compared by the factory, so both fakes must stamp from the same time line.
    Each fake's own seeding step advances it.
    """

    def __init__(self, start: datetime) -> None:
        self.now = start

    def move_to(self, moment: datetime) -> None:
        if moment > self.now:
            self.now = moment


class C4GitLab(gitlab_fake.GitLabAPI):
    """`forge_fakes.gitlab.GitLabAPI` plus the merge request listing's ``author_id``
    filter and the faults the repository-wide listing is tested under.

    https://docs.gitlab.com/api/merge_requests/#list-project-merge-requests
    (``author_id``: "Returns merge requests created by the given user id").
    A listing without ``source_branch`` is the repository-wide one; a lookup by
    branch never sees these faults. With a ``clock``, the fake's clock is that
    `SharedClock`.
    """

    def __init__(self, *, clock: SharedClock | None = None, **kwargs: Any) -> None:
        self._clock = clock
        super().__init__(**kwargs)
        self.listing_status: int | None = None
        self.listing_fail_page: int | None = None
        self.listing_page_size: int | None = None
        self.malformed_author: set[int] = set()
        # Merge request iid -> "missing" (no ``description`` key) or "not_text".
        self.body_fault: dict[int, str] = {}

    @property  # type: ignore[override]
    def now(self) -> datetime:
        return self._clock.now if self._clock is not None else self._own_now

    @now.setter
    def now(self, value: datetime) -> None:
        if self._clock is not None:
            self._clock.move_to(value)
        else:
            self._own_now = value

    def _list_mrs(
        self, request: httpx.Request, params: dict[str, str], query: list[tuple[str, str]]
    ) -> httpx.Response:
        repository_wide = "source_branch" not in params
        if repository_wide and self.listing_status is not None:
            # https://docs.gitlab.com/api/rest/troubleshooting/#status-codes
            text = "Not Found" if self.listing_status == 404 else "Bad Gateway"
            return self._error(request, self.listing_status, text)
        page = int(params.get("page", "1") or "1")
        if repository_wide and self.listing_fail_page == page:
            return self._error(request, 502, "Bad Gateway")
        found = list(self.merge_requests.values())
        state = params.get("state", "all")
        if state != "all":
            found = [m for m in found if m.state == state]
        for key in ("source_branch", "target_branch"):
            if key in params:
                found = [m for m in found if getattr(m, key) == params[key]]
        if "author_id" in params:
            found = [m for m in found if str(m.author) == params["author_id"]]
        iids = {int(v) for k, v in query if k == "iids[]"}
        if iids:
            found = [m for m in found if m.iid in iids]
        found.sort(key=lambda m: m.id, reverse=params.get("sort", "desc") == "desc")
        saved = self.page_size
        if repository_wide and self.listing_page_size is not None:
            self.page_size = self.listing_page_size
        try:
            return self._page(request, [self._mr(m) for m in found])
        finally:
            self.page_size = saved

    def _mr(self, mr: Any) -> dict[str, Any]:
        payload = super()._mr(mr)
        if mr.iid in self.malformed_author:
            payload["author"] = None
        fault = self.body_fault.get(mr.iid)
        if fault == "missing":
            payload.pop("description", None)
        elif fault == "not_text":
            payload["description"] = ["not", "text"]
        return payload

    def repository_listings(self) -> list[httpx.Request]:
        return [
            r
            for r in self.requests
            if r.method == "GET"
            and self.api_path(r).endswith("/merge_requests")
            and "source_branch" not in self.params(r)
        ]

    def open_mrs(self) -> dict[int, tuple[int, str]]:
        return {
            iid: (mr.id, mr.source_branch)
            for iid, mr in self.merge_requests.items()
            if mr.state == "opened"
        }

    def mr_url(self, iid: int) -> str:
        return f"{self.web_base}/{self.project_path}/-/merge_requests/{iid}"

    def bot_notes(self, iid: int) -> list[str]:
        return [n.body for n in self.issue_notes(iid) if n.author == gitlab_fake.BOT_ID]


class GitLabHost:
    """Several GitLab projects on one host: a project path goes to its project's
    fake, every other path (``/user``, ``/version``, tokens) to the first."""

    def __init__(self, *projects: C4GitLab) -> None:
        self.projects = projects

    def handle(self, request: httpx.Request) -> httpx.Response:
        raw = request.url.raw_path.decode().split("?", 1)[0]
        prefix = "/api/v4/projects/"
        if raw.startswith(prefix):
            key = unquote(raw[len(prefix) :].split("/", 1)[0])
            for fake in self.projects:
                if key in (str(fake.project_id), fake.project_path):
                    return fake.handle(request)
        return self.projects[0].handle(request)


class C4Jira(jira_fake.JiraFake):
    """`forge_fakes.jira.JiraFake` whose clock (``now_ms``) is a `SharedClock`.

    Jira reports changelog times as ``created`` on each history
    (https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/#api-rest-api-3-issue-issueidorkey-changelog-get);
    sharing the clock with the GitLab fake keeps them on GitLab's time line.
    """

    def __init__(self, clock: SharedClock, **kwargs: Any) -> None:
        self._clock = clock
        super().__init__(**kwargs)

    @property  # type: ignore[override]
    def now_ms(self) -> int:
        return int(self._clock.now.timestamp() * 1000)

    @now_ms.setter
    def now_ms(self, value: int) -> None:
        self._clock.move_to(datetime.fromtimestamp(value / 1000, tz=UTC))


class Router(httpx.BaseTransport, httpx.AsyncBaseTransport):
    """Every outbound request of the API, by host, to its fake. An unknown host
    is a 404 so nothing leaves the process."""

    def __init__(self) -> None:
        self.handlers: dict[str, Callable[[httpx.Request], httpx.Response]] = {}
        self.asgi: dict[str, httpx.ASGITransport] = {}
        self.unknown: list[str] = []

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        request.read()
        handler = self.handlers.get(request.url.host)
        if handler is None:
            self.unknown.append(str(request.url))
            return httpx.Response(404, text="no fixture for this host")
        return handler(request)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        asgi = self.asgi.get(request.url.host)
        if asgi is not None:
            return await asgi.handle_async_request(request)
        await request.aread()
        handler = self.handlers.get(request.url.host)
        if handler is None:
            self.unknown.append(str(request.url))
            return httpx.Response(404, text="no fixture for this host")
        return handler(request)


@dataclass
class _Job:
    sha: str
    origin: str
    header_form: str


class JobCluster:
    """The worker's `PublicationCluster` seam: one Kubernetes namespace of Jobs.

    ``apply`` runs the Job: it reads the container environment the worker built
    (`publication_k8s.build_publication_resources`) and performs the publish
    script's one external effect, a commit ``COMMIT_TITLE`` + ``Curie-Revision:``
    trailer on ``BASE_SHA`` pushed to ``BRANCH``. ``observe`` then reports the
    Job succeeded with the script's ``CURIE_COMMIT_SHA=<sha>`` log line.
    """

    def __init__(self, push: Callable[[dict[str, str]], str]) -> None:
        self._push = push
        self.jobs: dict[str, _Job] = {}
        self.applied: list[dict[str, str]] = []
        self.after_push: Callable[[dict[str, str], str], None] | None = None

    def observe(self, job_name: str) -> Any:
        from curie_worker.publication_k8s import PublicationTransport
        from curie_worker.publication_loop import PublicationJobObservation

        job = self.jobs.get(job_name)
        if job is None:
            return PublicationJobObservation(
                phase="pending", commit_sha=None, logs="", error=None, exists=False
            )
        return PublicationJobObservation(
            phase="succeeded",
            logs=f"Pushed the publication branch\nCURIE_COMMIT_SHA={job.sha}\n",
            transport=PublicationTransport(
                origin=job.origin, header_form=job.header_form, ca_bundle_ref=None
            ),
        )

    def apply(self, resources: Any) -> None:
        containers = resources.job["spec"]["template"]["spec"]["containers"]
        env = {
            item["name"]: item["value"]
            for container in containers
            for item in container.get("env", [])
            if "value" in item
        }
        self.applied.append(env)
        sha = self._push(env)
        if self.after_push is not None:
            self.after_push(env, sha)
        self.jobs[resources.names.job] = _Job(
            sha, env["CODE_HOST_ORIGIN"], env["CODE_HOST_HEADER_FORM"]
        )

    def validate_existing(self, _resources: Any) -> None:
        return None

    def cleanup_credentials(self, _names: Any) -> None:
        return None

    def cleanup_terminal(self, _names: Any) -> None:
        return None


def commit_message(env: dict[str, str]) -> str:
    """The publish script's ``git commit -m "$COMMIT_TITLE" -m "Curie-Revision: ..."``."""

    return f"{env['COMMIT_TITLE']}\n\nCurie-Revision: {env['REVISION_ID']}"
