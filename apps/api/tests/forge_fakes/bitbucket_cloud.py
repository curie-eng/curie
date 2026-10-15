"""A stateful Bitbucket Cloud the Cloud adapter talks to over HTTP (#4401).

`BitbucketCloudFake` is a Starlette app holding one repository: branches with
commit chains, pull requests, comments and build statuses, and the accounts
behind each token. The adapter reaches it through
``httpx.AsyncClient(transport=httpx.ASGITransport(app=fake.app))`` on the real
API origin, so every absolute ``next`` link it returns names the same origin
the adapter was configured with.

Payload shapes and query parameters follow the Bitbucket Cloud REST API 2.0
reference, and the committed recordings (``bitbucket_cloud_recordings/``)
where they exist:

1. Pagination (``values``, ``page``, ``pagelen``, ``size``, absolute ``next``)
   and BBQL filtering (``q``, ``sort``):
   https://developer.atlassian.com/cloud/bitbucket/rest/intro/#pagination and
   https://developer.atlassian.com/cloud/bitbucket/rest/intro/#filtering
2. Repository: https://developer.atlassian.com/cloud/bitbucket/rest/api-group-repositories/
3. Branches: https://developer.atlassian.com/cloud/bitbucket/rest/api-group-refs/
4. Commits and build statuses:
   https://developer.atlassian.com/cloud/bitbucket/rest/api-group-commits/ and
   https://developer.atlassian.com/cloud/bitbucket/rest/api-group-commit-statuses/
5. Pull requests and their comments:
   https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/
6. Pull request mergeability checks (#4624), re-read 2026-10-15 from the
   reference page
   https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-mergeability-checks-get
   and the OpenAPI document
   https://dac-static.atlassian.com/cloud/bitbucket/swagger.v3.json
   (``pullrequest_mergeability_checks``, ``pullrequest_mergeability_check``,
   ``git_mergeability_check``). These are the documented shapes, not
   recordings: no recording of this endpoint exists yet.

Seeding helpers change state directly and count no write; `writes` counts only
mutating requests the app accepted, which is what the contract harness reports
as port writes. Placeholder identifiers only.
"""

from __future__ import annotations

import hashlib
import html
import itertools
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote, urlencode, urlsplit

import httpx
from curie_api.forges import types
from curie_api.forges.types import Actor, RepositoryRef
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Mount, Route

API_URL = "https://api.bitbucket.org/2.0"
HTML_BASE = "https://bitbucket.org"
HOST = "bitbucket.org"
WORKSPACE = "example-workspace"
SLUG = "example-repo"
REPO_UUID = "{00000000-0000-4000-8000-000000000001}"
WORKSPACE_UUID = "{00000000-0000-4000-8000-000000000002}"
PROJECT_UUID = "{00000000-0000-4000-8000-000000000003}"
MAIN = "main"

# The factory's repository access token and the bot account it acts as.
FACTORY_TOKEN = "fixture-factory-token"
FACTORY = Actor("placeholder-account-factory", "factory-bot")
# The two accounts a lifecycle comments as.
REVIEWER_TOKEN = "fixture-reviewer-token"
REVIEWER = Actor("placeholder-account-reviewer", "reviewer-bot")
OUTSIDER_TOKEN = "fixture-outsider-token"
OUTSIDER = Actor("placeholder-account-outsider", "drive-by")

# A changed line an inline comment anchors on.
INLINE = {"path": "src/widget.txt", "from": None, "to": 12}

# Bitbucket Cloud build status states (commit-statuses reference).
STATUS_STATES = frozenset({"SUCCESSFUL", "FAILED", "INPROGRESS", "STOPPED", "CANCELLED"})
PULL_STATES = frozenset({"OPEN", "MERGED", "DECLINED", "SUPERSEDED"})
# Bitbucket's own default page length on these listings.
DEFAULT_PAGELEN = 10
MAX_PAGELEN = 100

_SOURCE_BRANCH_Q = re.compile(r'\s*source\.branch\.name\s*=\s*"((?:[^"\\]|\\.)*)"\s*')
_CREATED_ON_Q = re.compile(r'\s*created_on\s*>=\s*"?([0-9T:.+\-Z]+)"?\s*')
_MARKER_LINE = re.compile(r"^\[//\]: # \(.*\)$")
# The only two filters the mergeability checks endpoint accepts, joined by
# ``AND`` in either order; anything else is a 400 (swagger, re-read 2026-10-15).
_SKIP_GIT_Q = re.compile(r'\s*type\s*!=\s*"git_mergeability_check"\s*')
_SKIP_BEHIND_Q = re.compile(r'\s*check\.kind\s*!=\s*"maximum_commits_behind"\s*')


def _stamp(moment: datetime) -> str:
    """Bitbucket's timestamp form: ISO 8601 with microseconds and an offset."""

    return moment.astimezone(UTC).isoformat(timespec="microseconds")


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _render(raw: str) -> str:
    """A stand-in for Bitbucket's markdown rendering: a link reference
    definition (the marker line) renders to nothing, every other line is text."""

    kept = [line for line in raw.splitlines() if not _MARKER_LINE.fullmatch(line.strip())]
    paragraphs = "\n".join(kept).strip()
    return f"<p>{html.escape(paragraphs)}</p>" if paragraphs else ""


def _number(text: str) -> int:
    """A stable twelve-digit number for ``text``, for placeholder UUIDs."""

    return int(hashlib.sha1(text.encode()).hexdigest(), 16) % 10**12


def _mergeability_skips(query: str | None) -> frozenset[str] | None:
    """The checks a mergeability ``q`` excludes, or None when Bitbucket
    refuses it: ``git`` for ``type!="git_mergeability_check"``, ``behind``
    for ``check.kind!="maximum_commits_behind"``, each at most once."""

    if query is None:
        return frozenset()
    skips: list[str] = []
    for clause in re.split(r"\s+AND\s+", query):
        if _SKIP_GIT_Q.fullmatch(clause):
            skips.append("git")
        elif _SKIP_BEHIND_Q.fullmatch(clause):
            skips.append("behind")
        else:
            return None
    return frozenset(skips) if len(skips) == len(set(skips)) else None


def _skipped(check: dict[str, Any], skips: frozenset[str]) -> bool:
    if "git" in skips and check.get("type") == "git_mergeability_check":
        return True
    kind = check.get("check")
    return (
        "behind" in skips
        and check.get("type") == "standard_merge_check"
        and isinstance(kind, dict)
        and kind.get("kind") == "maximum_commits_behind"
    )


def _error(status: int, message: str) -> JSONResponse:
    return JSONResponse({"type": "error", "error": {"message": message}}, status_code=status)


@dataclass(frozen=True)
class Seen:
    """One request the app received, for header and query assertions."""

    method: str
    path: str
    query: tuple[tuple[str, str], ...]
    headers: dict[str, str]
    body: bytes


@dataclass
class _Commit:
    sha: str
    message: str
    parents: tuple[str, ...]
    date: datetime


@dataclass
class _Pull:
    id: int
    title: str
    description: str
    source: str
    destination: str
    author: Actor
    draft: bool
    close_source_branch: bool
    created_on: datetime
    updated_on: datetime
    state: str = "OPEN"
    # The source head when the pull request left OPEN; it follows the branch until then.
    frozen_head: str | None = None


@dataclass
class _Comment:
    id: int
    pull: int
    author: Actor
    raw: str
    created_on: datetime
    updated_on: datetime
    inline: dict[str, Any] | None = None
    parent: int | None = None
    deleted: bool = False


@dataclass
class _Status:
    uuid: str
    key: str
    state: str
    name: str
    url: str | None
    description: str
    created_on: datetime
    updated_on: datetime


@dataclass
class _Scripted:
    status: int
    headers: dict[str, str]
    body: Any


@dataclass
class BitbucketCloudFake:
    """One Bitbucket Cloud repository behind its REST API."""

    workspace: str = WORKSPACE
    slug: str = SLUG
    repo_uuid: str = REPO_UUID
    factory: Actor = FACTORY
    api_url: str = API_URL
    mainbranch: str = MAIN
    tokens: dict[str, Actor] = field(default_factory=dict)
    # Accounts the repository's permission configuration grants write; the
    # adapter must never read it (decision 11), the fake only offers it.
    writers: set[str] = field(default_factory=set)
    writes: int = 0
    requests: list[Seen] = field(default_factory=list)
    # Called with the pull request id inside every mergeability read, after the
    # request is accepted and before the checks are evaluated: a test pushes to
    # the source or destination branch here to move a head mid-read.
    on_mergeability_read: Callable[[int], None] | None = None
    # Pull requests whose payload carries no source commit hash. Bitbucket is
    # not known to answer this way; it is an unexpected shape, offered so a test
    # can show the adapter proves nothing about a pull request it cannot pin.
    hashless_sources: set[int] = field(default_factory=set)

    def __post_init__(self) -> None:
        self.tokens.setdefault(FACTORY_TOKEN, self.factory)
        self.users: dict[str, Actor] = {self.factory.id: self.factory}
        for actor in self.tokens.values():
            self.users[actor.id] = actor
        self.commits: dict[str, _Commit] = {}
        self.branches: dict[str, str] = {}
        self.pulls: dict[int, _Pull] = {}
        self.comments: dict[int, _Comment] = {}
        # Per commit, the latest status per key (Bitbucket keeps one per key).
        self.statuses: dict[str, dict[str, _Status]] = {}
        self._clock = datetime(2026, 10, 1, 9, 0, tzinfo=UTC)
        self._ids = itertools.count(1)
        self._commit_ids = itertools.count(1)
        self._uuid_ids = itertools.count(100)
        self._page_countdown: int | None = None
        self._scripted: list[_Scripted] = []
        # Per pull request: the seeded Git check, or a whole seeded check list.
        self._git_checks: dict[int, tuple[str, str | None, bool]] = {}
        self._mergeability_values: dict[int, list[dict[str, Any]]] = {}
        # An answer every mergeability read gets instead of the checks.
        self._mergeability_answer: _Scripted | None = None
        self.push(self.mainbranch, "Initial commit")
        prefix = urlsplit(self.api_url).path.rstrip("/")
        self.app = Starlette(routes=[Mount(prefix, routes=self._routes())])

    # Seeding and observation ------------------------------------------------

    @property
    def full_name(self) -> str:
        return f"{self.workspace}/{self.slug}"

    def repository(self) -> RepositoryRef:
        return RepositoryRef(
            types.BITBUCKET_CLOUD, HOST, self.repo_uuid, self.full_name, default_branch=MAIN
        )

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app))

    def add_user(self, actor: Actor, token: str | None = None) -> None:
        """Make ``actor`` an account; with ``token``, one that can call the API."""

        self.users[actor.id] = actor
        if token is not None:
            self.tokens[token] = actor

    def now(self) -> datetime:
        """The fake's clock: one second later on every read, so ordering holds."""

        self._clock += timedelta(seconds=1)
        return self._clock

    def push(self, branch: str, message: str) -> str:
        """One commit on ``branch``, from the main branch when it is new."""

        parent = self.branches.get(branch, self.branches.get(self.mainbranch))
        sha = hashlib.sha1(f"commit-{next(self._commit_ids)}".encode()).hexdigest()
        self.commits[sha] = _Commit(sha, message, (parent,) if parent else (), self.now())
        self.branches[branch] = sha
        return sha

    def post_status(
        self, sha: str, key: str, state: str, *, name: str | None = None, url: str | None = None
    ) -> None:
        """A build status on ``sha`` in Bitbucket's own state words."""

        if state not in STATUS_STATES:
            raise ValueError(f"{state!r} is not a Bitbucket Cloud build state")
        self._set_status(sha, key, state, name or key, url, "")

    def comment(
        self,
        pull: int,
        author: Actor,
        raw: str,
        *,
        inline: dict[str, Any] | None = None,
        parent: int | None = None,
    ) -> int:
        """``author`` comments on ``pull``: inline, on the conversation, or as
        a reply to ``parent``. Returns the comment id."""

        self.users.setdefault(author.id, author)
        return self._add_comment(pull, author, raw, inline=inline, parent=parent)

    def delete_comment(self, comment_id: int) -> None:
        """Bitbucket keeps a deleted comment, flagged and with its text gone."""

        found = self.comments[comment_id]
        found.deleted, found.raw, found.updated_on = True, "", self.now()

    def set_state(self, pull: int, state: str) -> None:
        """Someone else merges or declines ``pull``."""

        self._set_state(self.pulls[pull], state)

    def thread_root(self, comment_id: int) -> int:
        found = self.comments[comment_id]
        while found.parent is not None:
            found = self.comments[found.parent]
        return found.id

    def thread_replies(self, root: int) -> list[_Comment]:
        """Live replies anywhere under ``root``, oldest first."""

        return [
            c
            for c in self.comments.values()
            if c.id != root and not c.deleted and self.thread_root(c.id) == root
        ]

    def conversation(self, pull: int) -> list[_Comment]:
        """Live top-level comments on ``pull`` that anchor on no line."""

        return [
            c
            for c in self.comments.values()
            if c.pull == pull and c.parent is None and c.inline is None and not c.deleted
        ]

    def fail_next_page(self, *, after: int = 0) -> None:
        """The listing page read after ``after`` more page reads answers 500 once."""

        self._page_countdown = after

    def answer_next(
        self, status: int, headers: dict[str, str] | None = None, body: Any = None
    ) -> None:
        """The next request, whatever it is, gets this answer instead."""

        self._scripted.append(_Scripted(status, dict(headers or {}), body))

    def set_git_mergeability(
        self, pull: int, status: str, reason: str | None, *, blocking: bool | None = None
    ) -> None:
        """The ``git_mergeability_check`` ``pull``'s mergeability read reports.

        ``reason`` None omits the field, as Bitbucket does for ``UNKNOWN``.
        ``blocking`` defaults to whether ``status`` is ``FAILED``; a blocking
        check carries the ``links.details`` link to the pull request's
        ``conflicts`` endpoint.
        """

        if pull not in self.pulls:
            raise KeyError(pull)
        self._git_checks[pull] = (
            status,
            reason,
            status == "FAILED" if blocking is None else blocking,
        )

    def set_mergeability_checks(self, pull: int, values: list[dict[str, Any]] | None) -> None:
        """Answer ``pull``'s mergeability read with exactly ``values``, for
        shapes the default does not produce; None restores the default."""

        if pull not in self.pulls:
            raise KeyError(pull)
        if values is None:
            self._mergeability_values.pop(pull, None)
        else:
            self._mergeability_values[pull] = [dict(v) for v in values]

    def answer_mergeability(
        self,
        status: int | None,
        *,
        headers: dict[str, str] | None = None,
        body: Any = None,
    ) -> None:
        """Every mergeability read answers ``status`` (with ``body``) until
        called with None. Other routes answer normally."""

        self._mergeability_answer = (
            None if status is None else _Scripted(status, dict(headers or {}), body)
        )

    # State changes shared by seeding and the API ----------------------------

    def _add_comment(
        self,
        pull: int,
        author: Actor,
        raw: str,
        *,
        inline: dict[str, Any] | None,
        parent: int | None,
    ) -> int:
        if pull not in self.pulls:
            raise KeyError(pull)
        if parent is not None and self.comments[parent].pull != pull:
            raise KeyError(parent)
        moment = self.now()
        comment_id = next(self._ids)
        self.comments[comment_id] = _Comment(
            comment_id, pull, author, raw, moment, moment, inline=inline, parent=parent
        )
        return comment_id

    def _set_status(
        self, sha: str, key: str, state: str, name: str, url: str | None, description: str
    ) -> None:
        moment = self.now()
        on_commit = self.statuses.setdefault(sha, {})
        previous = on_commit.get(key)
        on_commit[key] = _Status(
            previous.uuid if previous else self._uuid(),
            key,
            state,
            name,
            url,
            description,
            previous.created_on if previous else moment,
            moment,
        )

    def _set_state(self, pull: _Pull, state: str) -> None:
        if pull.state == "OPEN" and state != "OPEN":
            pull.frozen_head = self.branches.get(pull.source)
        pull.state, pull.updated_on = state, self.now()

    def _uuid(self) -> str:
        return f"{{00000000-0000-4000-8000-{next(self._uuid_ids):012d}}}"

    # Payloads ---------------------------------------------------------------

    @property
    def _repo_api(self) -> str:
        return f"{self.api_url}/repositories/{self.full_name}"

    def _user(self, actor: Actor) -> dict[str, Any]:
        return {
            "type": "user",
            "account_id": actor.id,
            "nickname": actor.login,
            "display_name": actor.login,
            "uuid": f"{{00000000-0000-4000-8000-{_number(actor.id):012d}}}",
            "links": {"avatar": {"href": "https://example.invalid/avatar.png"}},
        }

    def _repo_min(self) -> dict[str, Any]:
        return {
            "type": "repository",
            "full_name": self.full_name,
            "name": self.slug,
            "uuid": self.repo_uuid,
            "links": {"html": {"href": f"{HTML_BASE}/{self.full_name}"}},
        }

    def _repository(self) -> dict[str, Any]:
        avatar = f"{HTML_BASE}/workspaces/{self.workspace}/avatar/?ts=1760000000"
        return {
            **self._repo_min(),
            "slug": self.slug,
            "is_private": True,
            "scm": "git",
            "mainbranch": {"type": "branch", "name": self.mainbranch},
            # Shape observed on a team workspace on 2026-10-12: the owner's display name
            # and username are the workspace slug, its avatar is under
            # /account/<slug>/avatar/; the workspace avatar is under
            # /workspaces/<slug>/avatar/; the project links are under
            # /<slug>/workspace/projects/<KEY>.
            "owner": {
                "type": "team",
                "display_name": self.workspace,
                "username": self.workspace,
                "uuid": WORKSPACE_UUID,
                "links": {"avatar": {"href": f"{HTML_BASE}/account/{self.workspace}/avatar/"}},
            },
            "workspace": {
                "type": "workspace",
                "slug": self.workspace,
                "name": self.workspace,
                "uuid": WORKSPACE_UUID,
                "links": {"avatar": {"href": avatar}},
            },
            "project": {
                "type": "project",
                "key": "PROJ",
                "name": "Example Project",
                "uuid": PROJECT_UUID,
                "links": {
                    "html": {"href": f"{HTML_BASE}/{self.workspace}/workspace/projects/PROJ"},
                    "avatar": {
                        "href": f"{HTML_BASE}/{self.workspace}/workspace/projects/PROJ"
                        "/avatar/32?ts=1760000000"
                    },
                },
            },
            "links": {
                "self": {"href": self._repo_api},
                "html": {"href": f"{HTML_BASE}/{self.full_name}"},
                "clone": [
                    {"name": "https", "href": f"https://x-token-auth@{HOST}/{self.full_name}.git"}
                ],
            },
        }

    def _commit(self, commit: _Commit) -> dict[str, Any]:
        return {
            "type": "commit",
            "hash": commit.sha,
            "message": commit.message,
            "date": _stamp(commit.date),
            "author": {"type": "author", "raw": "Placeholder User <placeholder@example.invalid>"},
            "parents": [
                {"type": "commit", "hash": p, "links": {"self": {"href": self._commit_url(p)}}}
                for p in commit.parents
            ],
            "links": {"self": {"href": self._commit_url(commit.sha)}},
            "repository": self._repo_min(),
        }

    def _commit_url(self, sha: str) -> str:
        return f"{self._repo_api}/commit/{sha}"

    def _head(self, pull: _Pull) -> str:
        return pull.frozen_head or self.branches[pull.source]

    def _pull(self, pull: _Pull) -> dict[str, Any]:
        url = f"{self._repo_api}/pullrequests/{pull.id}"
        return {
            "type": "pullrequest",
            "id": pull.id,
            "title": pull.title,
            "description": pull.description,
            "summary": {
                "type": "rendered",
                "raw": pull.description,
                "markup": "markdown",
                "html": _render(pull.description),
            },
            "state": pull.state,
            "draft": pull.draft,
            "author": self._user(pull.author),
            # Bitbucket abbreviates both commit hashes to twelve characters.
            "source": {
                "branch": {"name": pull.source},
                "commit": (
                    {"type": "commit"}
                    if pull.id in self.hashless_sources
                    else {"type": "commit", "hash": self._head(pull)[:12]}
                ),
                "repository": self._repo_min(),
            },
            "destination": {
                "branch": {"name": pull.destination},
                "commit": {"type": "commit", "hash": self.branches[pull.destination][:12]},
                "repository": self._repo_min(),
            },
            "merge_commit": None,
            "close_source_branch": pull.close_source_branch,
            "reason": "",
            "comment_count": sum(1 for c in self.comments.values() if c.pull == pull.id),
            "task_count": 0,
            "created_on": _stamp(pull.created_on),
            "updated_on": _stamp(pull.updated_on),
            "links": {
                "self": {"href": url},
                "html": {"href": f"{HTML_BASE}/{self.full_name}/pull-requests/{pull.id}"},
                "comments": {"href": f"{url}/comments"},
                "statuses": {"href": f"{url}/statuses"},
            },
        }

    def _comment(self, comment: _Comment) -> dict[str, Any]:
        url = f"{self._repo_api}/pullrequests/{comment.pull}/comments/{comment.id}"
        payload: dict[str, Any] = {
            "type": "pullrequest_comment",
            "id": comment.id,
            "created_on": _stamp(comment.created_on),
            "updated_on": _stamp(comment.updated_on),
            "content": {
                "type": "rendered",
                "raw": comment.raw,
                "markup": "markdown",
                "html": _render(comment.raw),
            },
            "user": self._user(comment.author),
            "deleted": comment.deleted,
            "pending": False,
            "pullrequest": {
                "type": "pullrequest",
                "id": comment.pull,
                "links": {"self": {"href": f"{self._repo_api}/pullrequests/{comment.pull}"}},
            },
            "links": {
                "self": {"href": url},
                "html": {
                    "href": f"{HTML_BASE}/{self.full_name}/pull-requests/{comment.pull}"
                    f"#comment-{comment.id}"
                },
            },
        }
        if comment.inline is not None:
            payload["inline"] = dict(comment.inline)
        if comment.parent is not None:
            payload["parent"] = {
                "id": comment.parent,
                "links": {
                    "self": {
                        "href": f"{self._repo_api}/pullrequests/{comment.pull}"
                        f"/comments/{comment.parent}"
                    }
                },
            }
        return payload

    def _mergeability(self, pull: _Pull) -> list[dict[str, Any]]:
        """The checks Bitbucket lists for ``pull``: state, permission and Git.

        A pull request that is not OPEN fails the blocking state check and has
        no Git check evaluated. No branch restriction, custom check or merge
        queue is configured, so those checks are omitted, as Bitbucket omits
        them.
        """

        seeded = self._mergeability_values.get(pull.id)
        if seeded is not None:
            return [dict(v) for v in seeded]
        if pull.state != "OPEN":
            return [
                {
                    "type": "pullrequest_state_check",
                    "status": "FAILED",
                    "required": True,
                    "blocking": True,
                }
            ]
        status, reason, blocking = self._git_checks.get(pull.id, ("PASSED", "clean", False))
        git: dict[str, Any] = {
            "type": "git_mergeability_check",
            "status": status,
            "required": True,
            "blocking": blocking,
        }
        if reason is not None:
            git["reason"] = reason
        if blocking:
            conflicts = f"{self._repo_api}/pullrequests/{pull.id}/conflicts"
            git["links"] = {"details": {"href": conflicts}}
        return [
            {
                "type": "pullrequest_state_check",
                "status": "PASSED",
                "required": True,
                "blocking": False,
            },
            {
                "type": "current_user_permission_check",
                "status": "PASSED",
                "required": True,
                "blocking": False,
            },
            git,
        ]

    def _status(self, sha: str, status: _Status) -> dict[str, Any]:
        return {
            "type": "build",
            "uuid": status.uuid,
            "key": status.key,
            "name": status.name,
            "state": status.state,
            "url": status.url,
            "description": status.description,
            "refname": None,
            "created_on": _stamp(status.created_on),
            "updated_on": _stamp(status.updated_on),
            "commit": {
                "type": "commit",
                "hash": sha,
                "links": {"self": {"href": self._commit_url(sha)}},
            },
            "links": {
                "self": {"href": f"{self._commit_url(sha)}/statuses/build/{quote(status.key)}"},
                "commit": {"href": self._commit_url(sha)},
            },
        }

    # HTTP --------------------------------------------------------------------

    def _routes(self) -> list[Route]:
        repo = "/repositories/{workspace}/{slug}"
        return [
            Route(repo, self._get_repository, methods=["GET"]),
            Route(
                repo + "/permissions-config/users/{account_id}",
                self._get_permission,
                methods=["GET"],
            ),
            Route(repo + "/refs/branches/{name:path}", self._branch, methods=["GET", "DELETE"]),
            Route(repo + "/commit/{sha}/statuses", self._list_statuses, methods=["GET"]),
            Route(repo + "/commit/{sha}/statuses/build", self._post_status, methods=["POST"]),
            Route(repo + "/commit/{sha}", self._get_commit, methods=["GET"]),
            Route(repo + "/pullrequests", self._pulls, methods=["GET", "POST"]),
            Route(repo + "/pullrequests/{id:int}", self._one_pull, methods=["GET", "PUT"]),
            Route(repo + "/pullrequests/{id:int}/decline", self._decline, methods=["POST"]),
            Route(repo + "/pullrequests/{id:int}/merge", self._merge, methods=["POST"]),
            Route(
                repo + "/pullrequests/{id:int}/mergeability/checks",
                self._mergeability_checks,
                methods=["GET"],
            ),
            Route(
                repo + "/pullrequests/{id:int}/comments", self._comments, methods=["GET", "POST"]
            ),
            Route(
                repo + "/pullrequests/{id:int}/comments/{comment_id:int}",
                self._one_comment,
                methods=["GET", "PUT"],
            ),
        ]

    async def _enter(self, request: Request) -> tuple[Actor | None, Response | None]:
        """Record the request; answer a scripted reply or a refused token."""

        self.requests.append(
            Seen(
                request.method,
                request.url.path,
                tuple(request.query_params.multi_items()),
                {k.lower(): v for k, v in request.headers.items()},
                await request.body(),
            )
        )
        if self._scripted:
            scripted = self._scripted.pop(0)
            if isinstance(scripted.body, (dict, list)):
                return None, JSONResponse(
                    scripted.body, status_code=scripted.status, headers=scripted.headers
                )
            return None, Response(
                scripted.body or "", status_code=scripted.status, headers=scripted.headers
            )
        scheme, _, token = request.headers.get("authorization", "").partition(" ")
        actor = self.tokens.get(token) if scheme == "Bearer" else None
        if actor is None:
            return None, _error(401, "Access token expired or invalid")
        return actor, None

    def _repo_matches(self, request: Request) -> bool:
        workspace, slug = request.path_params["workspace"], request.path_params["slug"]
        # By slug, or by UUID under the literal `{}` workspace.
        return (workspace, slug) == (self.workspace, self.slug) or (
            workspace == "{}" and slug == self.repo_uuid
        )

    def _page(self, request: Request, values: list[dict[str, Any]]) -> Response:
        """One page of a paged listing, failing when a page fault is due."""

        if self._page_countdown is not None:
            if self._page_countdown == 0:
                self._page_countdown = None
                return _error(500, "Something went wrong")
            self._page_countdown -= 1
        try:
            pagelen = int(request.query_params.get("pagelen", DEFAULT_PAGELEN))
            page = int(request.query_params.get("page", "1"))
        except ValueError:
            return _error(400, "Invalid pagelen or page")
        if not 1 <= pagelen <= MAX_PAGELEN or page < 1:
            return _error(400, "Invalid pagelen or page")
        start = (page - 1) * pagelen
        body: dict[str, Any] = {
            "size": len(values),
            "page": page,
            "pagelen": pagelen,
            "values": values[start : start + pagelen],
        }
        if start + pagelen < len(values):
            query = [(k, v) for k, v in request.query_params.multi_items() if k != "page"]
            body["next"] = f"{self.api_url}{self._path(request)}?" + urlencode(
                [*query, ("page", str(page + 1))]
            )
        return JSONResponse(body)

    def _path(self, request: Request) -> str:
        """The request path below the API prefix, encoded as a ``next`` link carries it."""

        prefix = urlsplit(self.api_url).path.rstrip("/")
        return quote(request.url.path.removeprefix(prefix), safe="/")

    async def _get_repository(self, request: Request) -> Response:
        _, refused = await self._enter(request)
        if refused is not None:
            return refused
        if not self._repo_matches(request):
            return _error(404, "Repository not found")
        return JSONResponse(self._repository())

    async def _get_permission(self, request: Request) -> Response:
        _, refused = await self._enter(request)
        if refused is not None:
            return refused
        if not self._repo_matches(request):
            return _error(404, "Repository not found")
        account = request.path_params["account_id"]
        if account not in self.users:
            return _error(404, "User not found")
        permission = "write" if account in self.writers else "read"
        return JSONResponse({"type": "repository_user_permission", "permission": permission})

    async def _branch(self, request: Request) -> Response:
        _, refused = await self._enter(request)
        if refused is not None:
            return refused
        name = request.path_params["name"]
        if not self._repo_matches(request) or name not in self.branches:
            return _error(404, f"Branch not found: {name}")
        if request.method == "DELETE":
            del self.branches[name]
            self.writes += 1
            return Response(status_code=204)
        target = self.commits[self.branches[name]]
        return JSONResponse(
            {
                "type": "branch",
                "name": name,
                "target": self._commit(target),
                "links": {"self": {"href": f"{self._repo_api}/refs/branches/{quote(name)}"}},
            }
        )

    def _resolve_sha(self, prefix: str) -> str | None:
        if len(prefix) < 7:
            return None
        matched = [sha for sha in self.commits if sha.startswith(prefix.lower())]
        return matched[0] if len(matched) == 1 else None

    async def _get_commit(self, request: Request) -> Response:
        _, refused = await self._enter(request)
        if refused is not None:
            return refused
        sha = self._resolve_sha(request.path_params["sha"])
        if not self._repo_matches(request) or sha is None:
            return _error(404, "Commit not found")
        return JSONResponse(self._commit(self.commits[sha]))

    async def _list_statuses(self, request: Request) -> Response:
        _, refused = await self._enter(request)
        if refused is not None:
            return refused
        sha = self._resolve_sha(request.path_params["sha"])
        if not self._repo_matches(request) or sha is None:
            return _error(404, "Commit not found")
        on_commit = sorted(self.statuses.get(sha, {}).values(), key=lambda s: s.created_on)
        return self._page(request, [self._status(sha, s) for s in on_commit])

    async def _post_status(self, request: Request) -> Response:
        _, refused = await self._enter(request)
        if refused is not None:
            return refused
        sha = self._resolve_sha(request.path_params["sha"])
        if not self._repo_matches(request) or sha is None:
            return _error(404, "Commit not found")
        body = await request.json()
        key, state = body.get("key"), body.get("state")
        if not isinstance(key, str) or not key or state not in STATUS_STATES:
            return _error(400, "key and a valid state are required")
        self._set_status(
            sha, key, state, body.get("name") or key, body.get("url"), body.get("description", "")
        )
        self.writes += 1
        return JSONResponse(self._status(sha, self.statuses[sha][key]), status_code=201)

    async def _pulls(self, request: Request) -> Response:
        actor, refused = await self._enter(request)
        if refused is not None:
            return refused
        if not self._repo_matches(request):
            return _error(404, "Repository not found")
        if request.method == "POST":
            assert actor is not None
            return await self._open(request, actor)
        states = request.query_params.getlist("state") or ["OPEN"]
        if not set(states) <= PULL_STATES:
            return _error(400, "Invalid state")
        listed = [p for p in self.pulls.values() if p.state in states]
        query = request.query_params.get("q")
        if query is not None:
            matched = _SOURCE_BRANCH_Q.fullmatch(query)
            if matched is None:
                return _error(400, f"Unsupported query: {query}")
            branch = re.sub(r"\\(.)", r"\1", matched[1])
            listed = [p for p in listed if p.source == branch]
        return self._page(request, [self._pull(p) for p in sorted(listed, key=lambda p: p.id)])

    async def _open(self, request: Request, actor: Actor) -> Response:
        body = await request.json()
        source = ((body.get("source") or {}).get("branch") or {}).get("name")
        destination = ((body.get("destination") or {}).get("branch") or {}).get(
            "name", self.mainbranch
        )
        if source not in self.branches or destination not in self.branches:
            return _error(400, "source or destination branch not found")
        if not isinstance(body.get("title"), str) or not body["title"]:
            return _error(400, "title is required")
        moment = self.now()
        pull = _Pull(
            id=max(self.pulls, default=0) + 1,
            title=body["title"],
            description=body.get("description", ""),
            source=source,
            destination=destination,
            author=actor,
            draft=bool(body.get("draft", False)),
            close_source_branch=bool(body.get("close_source_branch", False)),
            created_on=moment,
            updated_on=moment,
        )
        self.pulls[pull.id] = pull
        self.writes += 1
        return JSONResponse(self._pull(pull), status_code=201)

    async def _one_pull(self, request: Request) -> Response:
        _, refused = await self._enter(request)
        if refused is not None:
            return refused
        pull = self.pulls.get(request.path_params["id"])
        if not self._repo_matches(request) or pull is None:
            return _error(404, "Pull request not found")
        if request.method == "PUT":
            body = await request.json()
            if "title" in body:
                pull.title = body["title"]
            if "description" in body:
                pull.description = body["description"]
            pull.updated_on = self.now()
            self.writes += 1
        return JSONResponse(self._pull(pull))

    async def _transition(self, request: Request, state: str) -> Response:
        _, refused = await self._enter(request)
        if refused is not None:
            return refused
        pull = self.pulls.get(request.path_params["id"])
        if not self._repo_matches(request) or pull is None:
            return _error(404, "Pull request not found")
        if pull.state != "OPEN":
            return _error(400, "Pull request is not open")
        self._set_state(pull, state)
        self.writes += 1
        return JSONResponse(self._pull(pull))

    async def _decline(self, request: Request) -> Response:
        return await self._transition(request, "DECLINED")

    async def _merge(self, request: Request) -> Response:
        return await self._transition(request, "MERGED")

    async def _mergeability_checks(self, request: Request) -> Response:
        _, refused = await self._enter(request)
        if refused is not None:
            return refused
        pull = self.pulls.get(request.path_params["id"])
        if not self._repo_matches(request) or pull is None:
            return _error(404, "Pull request not found")
        # Only a limited BBQL subset is accepted here; the adapter sends none.
        query = request.query_params.get("q")
        skips = _mergeability_skips(query)
        if skips is None:
            return _error(400, f"Unsupported query: {query}")
        if self.on_mergeability_read is not None:
            self.on_mergeability_read(pull.id)
        answer = self._mergeability_answer
        if answer is not None:
            if isinstance(answer.body, (dict, list)):
                return JSONResponse(answer.body, status_code=answer.status, headers=answer.headers)
            return Response(answer.body or "", status_code=answer.status, headers=answer.headers)
        values = [v for v in self._mergeability(pull) if not _skipped(v, skips)]
        return JSONResponse({"size": len(values), "values": values})

    async def _comments(self, request: Request) -> Response:
        actor, refused = await self._enter(request)
        if refused is not None:
            return refused
        pull_id = request.path_params["id"]
        if not self._repo_matches(request) or pull_id not in self.pulls:
            return _error(404, "Pull request not found")
        if request.method == "POST":
            assert actor is not None
            body = await request.json()
            raw = (body.get("content") or {}).get("raw")
            if not isinstance(raw, str) or not raw:
                return _error(400, "content.raw is required")
            parent = (body.get("parent") or {}).get("id")
            if parent is not None and (
                parent not in self.comments or self.comments[parent].pull != pull_id
            ):
                return _error(400, "parent comment not found")
            comment_id = self._add_comment(
                pull_id, actor, raw, inline=body.get("inline"), parent=parent
            )
            self.writes += 1
            return JSONResponse(self._comment(self.comments[comment_id]), status_code=201)
        listed = [c for c in self.comments.values() if c.pull == pull_id]
        query = request.query_params.get("q")
        if query is not None:
            matched = _CREATED_ON_Q.fullmatch(query)
            if matched is None:
                return _error(400, f"Unsupported query: {query}")
            since = _parse(matched[1])
            listed = [c for c in listed if c.created_on >= since]
        sort = request.query_params.get("sort", "id")
        if sort not in {"id", "-id", "created_on", "-created_on"}:
            return _error(400, f"Unsupported sort: {sort}")
        listed.sort(
            key=lambda c: (c.created_on, c.id) if sort.endswith("created_on") else (c.id,),
            reverse=sort.startswith("-"),
        )
        return self._page(request, [self._comment(c) for c in listed])

    async def _one_comment(self, request: Request) -> Response:
        actor, refused = await self._enter(request)
        if refused is not None:
            return refused
        found = self.comments.get(request.path_params["comment_id"])
        if (
            not self._repo_matches(request)
            or found is None
            or found.pull != request.path_params["id"]
        ):
            return _error(404, "Comment not found")
        if request.method == "PUT":
            if actor != found.author:
                return _error(403, "You can only edit your own comments")
            raw = ((await request.json()).get("content") or {}).get("raw")
            if not isinstance(raw, str) or not raw:
                return _error(400, "content.raw is required")
            found.raw, found.updated_on = raw, self.now()
            self.writes += 1
        return JSONResponse(self._comment(found))


class BitbucketCloudFakeBackend:
    """The fake as the shared Bitbucket lifecycle drives it
    (`forge_fakes.bitbucket_lifecycle.Backend`)."""

    kind = types.BITBUCKET_CLOUD
    code_host_name = "bitbucket-cloud"

    def __init__(self, fake: BitbucketCloudFake | None = None) -> None:
        self.fake = fake or BitbucketCloudFake()
        self.fake.add_user(REVIEWER, REVIEWER_TOKEN)
        self.fake.add_user(OUTSIDER, OUTSIDER_TOKEN)

    def forges_document(self) -> dict[str, Any]:
        from forge_fakes.bitbucket_lifecycle import bitbucket_binding, jira_tracker_entry

        return {
            "code_hosts": [
                {
                    "name": self.code_host_name,
                    "kind": types.BITBUCKET_CLOUD,
                    "host": HOST,
                    "api_url": self.fake.api_url,
                    "credential": {"type": "static_token", "token": FACTORY_TOKEN},
                }
            ],
            "trackers": [jira_tracker_entry()],
            "bindings": [bitbucket_binding(self.code_host_name, self.repository(), REVIEWER)],
        }

    def adapter_client(self) -> httpx.AsyncClient:
        return self.fake.client()

    def repository(self) -> RepositoryRef:
        return self.fake.repository()

    def push(self, branch: str, message: str) -> str:
        return self.fake.push(branch, message)

    def post_status(self, sha: str, key: str, state: str) -> None:
        self.fake.post_status(sha, key, state)

    def comment_as(
        self, who: str, pr: str, body: str, *, inline: bool, parent: str | None = None
    ) -> str:
        return str(
            self.fake.comment(
                int(pr),
                self.actor(who),
                body,
                inline=dict(INLINE) if inline and parent is None else None,
                parent=None if parent is None else int(parent),
            )
        )

    def actor(self, who: str) -> Actor:
        return {"reviewer": REVIEWER, "outsider": OUTSIDER}[who]

    def thread_replies(self, pr: str, root: str) -> list[tuple[str, str]]:
        return [
            (c.author.id, c.raw) for c in self.fake.thread_replies(int(root)) if c.pull == int(pr)
        ]
