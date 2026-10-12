"""A stateful Bitbucket Data Center 9.4 the DC adapter talks to over HTTP (#4401).

`BitbucketDcFake` is a Starlette app holding one project, its repositories,
branches with commit chains, pull requests, comments with nested replies,
build statuses and users. Payloads follow the anonymized shapes in
``bitbucket_dc_captures/`` (Atlassian's Bitbucket Data Center 9.4 REST
reference, https://developer.atlassian.com/server/bitbucket/rest/v904/intro/);
``test_bitbucket_dc_fake.py`` fails when a payload drifts from them.

The adapter reaches the app through `fake.transport()`, which records every
request (method, path, query, headers, JSON body and the timeout the client
sent) and can inject a transport error or a scripted answer before the app
sees the request. Seeding helpers change state directly and count no write;
`writes` counts only mutating requests the app accepted.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl

import httpx
from curie_api.forges import types
from curie_api.forges.credential_providers import StaticTokenProvider
from curie_api.forges.types import Actor, CredentialHeader, RepositoryRef
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

CAPTURES = Path(__file__).resolve().parent / "bitbucket_dc_captures"

HOST = "bbdc.example.invalid"
# A context path, so every adapter URL is proven to keep it (decision 5.2).
CONTEXT = "/bitbucket"
API_URL = f"https://{HOST}{CONTEXT}"
PROJECT_KEY = "FAC"
SLUG = "demo"
REPO_ID = 1
REPO_PATH = f"{PROJECT_KEY}/{SLUG}"
# Placeholders, never real credentials.
FACTORY_TOKEN = "fixture-dc-factory-token"
REVIEWER_TOKEN = "fixture-dc-reviewer-token"
OUTSIDER_TOKEN = "fixture-dc-outsider-token"
# The repository HTTP access token acts as its own service user.
FACTORY = Actor(id="101", login="curie-factory-bot")
REVIEWER = Actor(id="2", login="reviewer")
OUTSIDER = Actor(id="3", login="outsider")
CODE_HOST_NAME = "bitbucket-dc"

# The marker line a markdown renderer drops (decision 12.1).
_REFERENCE_DEFINITION = re.compile(r"^\[//\]: # \(.*\)$")
_START_MS = 1_760_000_000_000


def capture(name: str) -> Any:
    """One anonymized response body from ``bitbucket_dc_captures/``."""

    return json.loads((CAPTURES / name).read_text())


def _errors(message: str, exception: str | None = None, **extra: Any) -> dict[str, Any]:
    return {"errors": [{"context": None, "message": message, "exceptionName": exception, **extra}]}


@dataclass
class RecordedRequest:
    method: str
    path: str
    query: list[tuple[str, str]]
    headers: dict[str, str]
    body: Any
    timeout: dict[str, Any] | None

    def param(self, name: str) -> str | None:
        return next((value for key, value in self.query if key == name), None)


@dataclass
class _Answer:
    status: int
    headers: dict[str, str]
    body: Any
    when: str | None


@dataclass
class _Comment:
    id: int
    pull: int
    author: int
    text: str
    created: int
    version: int = 0
    updated: int = 0
    parent: int | None = None
    anchor: dict[str, Any] | None = None
    deleted: bool = False


@dataclass
class _Pull:
    id: int
    title: str
    description: str
    from_branch: str
    to_branch: str
    author: int
    created: int
    draft: bool = False
    state: str = "OPEN"
    version: int = 0
    updated: int = 0
    # The head a closed pull request keeps; an open one follows its branch.
    frozen_head: str | None = None
    merge: dict[str, Any] | None = field(
        default_factory=lambda: {"canMerge": True, "conflicted": False, "outcome": "CLEAN"}
    )


@dataclass
class _Activity:
    id: int
    pull: int
    created: int
    user: int
    action: str
    comment: int | None = None
    comment_action: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)
    # The comment as it read when deleted; a deleted comment is gone otherwise.
    snapshot: dict[str, Any] | None = None


class _Recorder(httpx.AsyncBaseTransport):
    def __init__(self, fake: BitbucketDcFake) -> None:
        self._fake = fake
        self._inner = httpx.ASGITransport(app=fake.app)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        fake = self._fake
        body: Any = None
        content = await request.aread()
        if content:
            try:
                body = json.loads(content)
            except ValueError:
                body = content.decode(errors="replace")
        recorded = RecordedRequest(
            method=request.method,
            path=request.url.path,
            query=parse_qsl(request.url.query.decode(), keep_blank_values=True),
            headers={key.lower(): value for key, value in request.headers.items()},
            body=body,
            timeout=request.extensions.get("timeout"),
        )
        fake.requests.append(recorded)
        if fake._transport_errors:
            fake._transport_errors -= 1
            raise httpx.ConnectError("connection refused", request=request)
        for index, answer in enumerate(fake._answers):
            if answer.when is None or answer.when in recorded.path:
                del fake._answers[index]
                return httpx.Response(
                    answer.status, headers=answer.headers, json=answer.body, request=request
                )
        return await self._inner.handle_async_request(request)


class BitbucketDcFake:
    """One Bitbucket Data Center with project ``FAC`` and repository ``demo``."""

    def __init__(
        self,
        *,
        version: str = "9.4.24",
        project_key: str = PROJECT_KEY,
        slug: str = SLUG,
        repo_id: int = REPO_ID,
        base_url: str = API_URL,
    ) -> None:
        self.version = version
        self.project_key = project_key
        self.slug = slug
        self.repo_id = repo_id
        self.base_url = base_url.rstrip("/")
        self.context = httpx.URL(self.base_url).path.rstrip("/")
        self.writes = 0
        self.requests: list[RecordedRequest] = []
        # When set, an authentication failure quotes the presented token, so a
        # test proves the adapter never repeats response text.
        self.echo_token_in_errors = False
        self.users: dict[int, dict[str, Any]] = {}
        self.tokens: dict[str, int] = {}
        self.permissions: dict[int, str] = {}
        self._repos: list[dict[str, Any]] = []
        self.commits: dict[str, dict[str, Any]] = {}
        self.branches: dict[str, str] = {}
        self.default_branch = "main"
        self.pulls: dict[int, _Pull] = {}
        self.comments: dict[int, _Comment] = {}
        self.activities: list[_Activity] = []
        # sha -> key -> build
        self.builds: dict[str, dict[str, dict[str, Any]]] = {}
        self._ids = itertools.count(1)
        self._ms = itertools.count(_START_MS, 1000)
        self._commit_seq = itertools.count(1)
        self._fail_countdown: int | None = None
        self._transport_errors = 0
        self._answers: list[_Answer] = []
        self._bump_on_read: set[int] = set()
        self.add_user(FACTORY, token=FACTORY_TOKEN, service=True)
        self.add_user(REVIEWER, token=REVIEWER_TOKEN)
        self.add_user(OUTSIDER, token=OUTSIDER_TOKEN)
        self.permissions[int(REVIEWER.id)] = "REPO_WRITE"
        self.permissions[int(OUTSIDER.id)] = "REPO_WRITE"
        self._repos.append(self._repo_payload(repo_id, project_key, slug))
        self.push(self.default_branch, "initial")
        self.app = Starlette(routes=self._routes())

    # Clients ---------------------------------------------------------------

    def transport(self) -> httpx.AsyncBaseTransport:
        return _Recorder(self)

    def client(self) -> httpx.AsyncClient:
        """A client with no timeout of its own: the adapter must send one."""

        return httpx.AsyncClient(transport=self.transport())

    def repository(self) -> RepositoryRef:
        return RepositoryRef(
            types.BITBUCKET_DC,
            HOST,
            str(self.repo_id),
            f"{self.project_key}/{self.slug}",
            default_branch=self.default_branch,
        )

    # Seeding ---------------------------------------------------------------

    def now(self) -> int:
        return next(self._ms)

    def add_user(self, actor: Actor, *, token: str | None = None, service: bool = False) -> None:
        uid = int(actor.id)
        self.users[uid] = {
            "name": actor.login,
            "emailAddress": f"{actor.login}@example.invalid",
            "active": True,
            "displayName": f"placeholder-user-{uid}",
            "id": uid,
            "slug": actor.login,
            "type": "SERVICE" if service else "NORMAL",
            "links": {"self": [{"href": f"{self.base_url}/users/{actor.login}"}]},
        }
        if token is not None:
            self.tokens[token] = uid

    def pad_repos(self, count: int) -> None:
        """``count`` other repositories listed before ``demo``, as an access
        token with a wider scope would see them."""

        start = 1000 + len(self._repos)
        padding = [
            self._repo_payload(start + n, self.project_key, f"pad-{start + n}")
            for n in range(count)
        ]
        self._repos[:0] = padding

    def rename_repository(self, slug: str) -> None:
        for repo in self._repos:
            if repo["id"] == self.repo_id:
                repo.update(self._repo_payload(self.repo_id, self.project_key, slug))
        self.slug = slug

    def set_version(self, version: str) -> None:
        self.version = version

    def push(self, branch: str, message: str = "change") -> str:
        """A new commit on ``branch``; returns its 40-hex sha."""

        parent = self.branches.get(branch) or self.branches.get(self.default_branch)
        seq = next(self._commit_seq)
        sha = hashlib.sha1(f"{branch}:{seq}:{message}".encode()).hexdigest()
        stamp = self.now()
        author = {
            "name": "placeholder-user-1",
            "emailAddress": "placeholder-user-1@example.invalid",
        }
        self.commits[sha] = {
            "id": sha,
            "displayId": sha[:11],
            "author": author,
            "authorTimestamp": stamp,
            "committer": author,
            "committerTimestamp": stamp,
            "message": message,
            "parents": [] if parent is None else [{"id": parent, "displayId": parent[:11]}],
        }
        self.branches[branch] = sha
        return sha

    def post_status(
        self,
        sha: str,
        key: str,
        state: str,
        *,
        name: str | None = None,
        url: str | None = None,
        description: str | None = None,
    ) -> None:
        """A build status on ``sha`` in Bitbucket's own words (SUCCESSFUL, FAILED,
        INPROGRESS, CANCELLED, UNKNOWN). The latest status per key wins. Stored
        in the build status listing's shape."""

        self.builds.setdefault(sha, {})[key] = {
            "state": state,
            "key": key,
            "name": name or key,
            "url": url or f"https://ci.example.invalid/{key}/{sha[:8]}",
            "description": description or f"{key} {state.lower()}",
            "dateAdded": self.now(),
        }

    def open_pull(
        self, head: str, base: str = "main", *, author: Actor = FACTORY, title: str = "Change"
    ) -> int:
        """A pull request opened outside the adapter."""

        return self._create_pull(int(author.id), head, base, title, "", draft=False)

    def comment(
        self,
        pull: int,
        author: Actor,
        text: str,
        *,
        anchor: dict[str, Any] | None = None,
        parent: int | None = None,
        created: int | None = None,
    ) -> int:
        """``author`` comments on ``pull``: top level, inline (``anchor``), or as a
        reply under ``parent``. ``created`` back-dates it (epoch ms)."""

        if parent is not None and parent not in self.comments:
            raise KeyError(parent)
        stamp = self.now() if created is None else created
        cid = next(self._ids)
        self.comments[cid] = _Comment(
            id=cid,
            pull=pull,
            author=int(author.id),
            text=text,
            created=stamp,
            updated=stamp,
            parent=parent,
            anchor=anchor,
        )
        self._activity(
            pull,
            int(author.id),
            "COMMENTED",
            comment=cid,
            comment_action="ADDED" if parent is None else "REPLIED",
            created=stamp,
        )
        return cid

    def inline_anchor(self, pull: int) -> dict[str, Any]:
        record = self.pulls[pull]
        return {
            "fromHash": self.branches[record.to_branch],
            "toHash": self._head(record),
            "line": 1,
            "lineType": "ADDED",
            "fileType": "TO",
            "path": "README.md",
            "diffType": "EFFECTIVE",
            "orphaned": False,
        }

    def edit_comment(self, cid: int, text: str) -> None:
        found = self.comments[cid]
        found.text, found.version, found.updated = text, found.version + 1, self.now()
        self._activity(found.pull, found.author, "COMMENTED", comment=cid, comment_action="EDITED")

    def delete_comment(self, cid: int) -> None:
        found = self.comments[cid]
        snapshot = self._comment_payload(found, markup=False)
        found.deleted = True
        self._activity(
            found.pull,
            found.author,
            "COMMENTED",
            comment=cid,
            comment_action="DELETED",
            snapshot=snapshot,
        )

    def approve(self, pull: int, user: Actor) -> None:
        self._activity(pull, int(user.id), "APPROVED")

    def needs_work(self, pull: int, user: Actor) -> None:
        self._activity(
            pull,
            int(user.id),
            "REVIEWED",
            extra={"fromStatus": "UNAPPROVED", "toStatus": "NEEDS_WORK"},
        )

    def set_state(self, pull: int, state: str) -> None:
        """Close ``pull`` as MERGED or DECLINED, or reopen it."""

        record = self.pulls[pull]
        record.frozen_head = None if state == "OPEN" else self._head(record)
        record.state, record.version, record.updated = state, record.version + 1, self.now()

    def set_merge_status(self, pull: int, status: dict[str, Any] | None) -> None:
        """What ``GET .../merge`` answers; None answers 409, as DC does while
        it cannot compute the merge."""

        self.pulls[pull].merge = status

    def bump_version_on_next_read(self, pull: int) -> None:
        """Someone else edits ``pull`` right after the next read of it."""

        self._bump_on_read.add(pull)

    # Faults ----------------------------------------------------------------

    def fail_next_page(self, *, after: int = 0) -> None:
        """The listing page read after ``after`` more page reads answers 500 once."""

        self._fail_countdown = after

    def answer_next(
        self,
        status: int,
        headers: dict[str, str] | None = None,
        body: Any = None,
        *,
        when: str | None = None,
    ) -> None:
        """The next request (whose path contains ``when``) gets this answer instead."""

        self._answers.append(
            _Answer(status, headers or {}, body or _errors(f"scripted {status}"), when)
        )

    def fail_next_transport(self, count: int = 1) -> None:
        self._transport_errors += count

    # Observation -----------------------------------------------------------

    def thread_replies(self, root: int) -> list[_Comment]:
        """Every live reply under ``root``, at any depth, oldest first."""

        found = [c for c in self.comments.values() if not c.deleted and c.parent is not None]
        return sorted(
            (c for c in found if self._root_of(c) == root and c.id != root),
            key=lambda c: (c.created, c.id),
        )

    def top_level(self, pull: int) -> list[_Comment]:
        return [
            c
            for c in self.comments.values()
            if c.pull == pull and c.parent is None and c.anchor is None and not c.deleted
        ]

    def actor(self, uid: int) -> Actor:
        return Actor(str(uid), self.users[uid]["slug"])

    @contextmanager
    def recording(self) -> Iterator[list[RecordedRequest]]:
        """The requests made inside the block."""

        start = len(self.requests)
        window: list[RecordedRequest] = []
        try:
            yield window
        finally:
            window.extend(self.requests[start:])

    # Payloads --------------------------------------------------------------

    def _repo_payload(self, repo_id: int, key: str, slug: str) -> dict[str, Any]:
        base = self.base_url
        return {
            "slug": slug,
            "id": repo_id,
            "name": slug,
            "hierarchyId": hashlib.sha1(f"repo:{repo_id}".encode()).hexdigest()[:20],
            "scmId": "git",
            "state": "AVAILABLE",
            "statusMessage": "Available",
            "forkable": True,
            "project": {
                "key": key,
                "id": 1,
                "name": "Factory",
                "public": False,
                "type": "NORMAL",
                "links": {"self": [{"href": f"{base}/projects/{key}"}]},
            },
            "public": False,
            "archived": False,
            "links": {
                "clone": [{"href": f"{base}/scm/{key.lower()}/{slug}.git", "name": "http"}],
                "self": [{"href": f"{base}/projects/{key}/repos/{slug}/browse"}],
            },
        }

    def _target_repo(self) -> dict[str, Any]:
        return next(repo for repo in self._repos if repo["id"] == self.repo_id)

    def _branch_payload(self, name: str) -> dict[str, Any]:
        sha = self.branches[name]
        return {
            "id": f"refs/heads/{name}",
            "displayId": name,
            "type": "BRANCH",
            "latestCommit": sha,
            "latestChangeset": sha,
            "isDefault": name == self.default_branch,
        }

    def _head(self, record: _Pull) -> str:
        return record.frozen_head or self.branches[record.from_branch]

    def _ref(self, name: str, sha: str) -> dict[str, Any]:
        return {
            "id": f"refs/heads/{name}",
            "displayId": name,
            "latestCommit": sha,
            "type": "BRANCH",
            "repository": self._target_repo(),
        }

    def _pull_payload(self, record: _Pull) -> dict[str, Any]:
        base = self.base_url
        return {
            "id": record.id,
            "version": record.version,
            "title": record.title,
            "description": record.description,
            "state": record.state,
            "open": record.state == "OPEN",
            "closed": record.state != "OPEN",
            "draft": record.draft,
            "createdDate": record.created,
            "updatedDate": record.updated,
            "fromRef": self._ref(record.from_branch, self._head(record)),
            "toRef": self._ref(record.to_branch, self.branches[record.to_branch]),
            "locked": False,
            "author": {
                "user": self.users[record.author],
                "role": "AUTHOR",
                "approved": False,
                "status": "UNAPPROVED",
            },
            "reviewers": [],
            "participants": [],
            "links": {
                "self": [
                    {
                        "href": f"{base}/projects/{self.project_key}/repos/{self.slug}"
                        f"/pull-requests/{record.id}"
                    }
                ]
            },
        }

    def _root_of(self, comment: _Comment) -> int:
        while comment.parent is not None:
            comment = self.comments[comment.parent]
        return comment.id

    @staticmethod
    def render_html(text: str) -> str:
        """Bitbucket's markdown rendering, as far as the marker line goes: a
        link reference definition renders to nothing."""

        kept = [line for line in text.splitlines() if not _REFERENCE_DEFINITION.match(line)]
        paragraphs = [p.strip() for p in "\n".join(kept).split("\n\n") if p.strip()]
        return "".join(f"<p>{p}</p>" for p in paragraphs)

    def _comment_payload(self, comment: _Comment, *, markup: bool) -> dict[str, Any]:
        own = comment.author == FACTORY_UID
        replies = sorted(
            (c for c in self.comments.values() if c.parent == comment.id and not c.deleted),
            key=lambda c: (c.created, c.id),
        )
        payload: dict[str, Any] = {
            "properties": {"repositoryId": self.repo_id},
            "id": comment.id,
            "version": comment.version,
            "text": comment.text,
            "author": self.users[comment.author],
            "createdDate": comment.created,
            "updatedDate": comment.updated,
            "comments": [self._comment_payload(reply, markup=markup) for reply in replies],
            "tasks": [],
            "severity": "NORMAL",
            "state": "OPEN",
            "threadResolved": False,
            "permittedOperations": {"editable": own, "transitionable": True, "deletable": own},
        }
        if comment.anchor is not None:
            payload["anchor"] = comment.anchor
        if markup:
            payload["html"] = self.render_html(comment.text)
        return payload

    def _activity_payload(self, activity: _Activity) -> dict[str, Any] | None:
        payload: dict[str, Any] = {
            "id": activity.id,
            "createdDate": activity.created,
            "user": self.users[activity.user],
            "action": activity.action,
            **activity.extra,
        }
        if activity.comment is not None:
            found = self.comments[activity.comment]
            if activity.comment_action == "DELETED":
                payload["comment"] = activity.snapshot
            elif found.deleted:
                # A deleted comment vanishes from the activities that added it.
                return None
            else:
                payload["comment"] = self._comment_payload(found, markup=False)
                if found.anchor is not None and found.parent is None:
                    payload["commentAnchor"] = found.anchor
            payload["commentAction"] = activity.comment_action
        return payload

    def _activity(
        self,
        pull: int,
        user: int,
        action: str,
        *,
        comment: int | None = None,
        comment_action: str | None = None,
        created: int | None = None,
        extra: dict[str, Any] | None = None,
        snapshot: dict[str, Any] | None = None,
    ) -> None:
        self.activities.append(
            _Activity(
                id=next(self._ids),
                pull=pull,
                created=self.now() if created is None else created,
                user=user,
                action=action,
                comment=comment,
                comment_action=comment_action,
                extra=extra or {},
                snapshot=snapshot,
            )
        )

    def _create_pull(
        self, author: int, head: str, base: str, title: str, description: str, *, draft: bool
    ) -> int:
        stamp = self.now()
        pid = max(self.pulls, default=0) + 1
        self.pulls[pid] = _Pull(
            id=pid,
            title=title,
            description=description,
            from_branch=head,
            to_branch=base,
            author=author,
            created=stamp,
            updated=stamp,
            draft=draft,
        )
        self._activity(pid, author, "OPENED", created=stamp)
        return pid

    # HTTP ------------------------------------------------------------------

    def _routes(self) -> list[Route]:
        api = f"{self.context}/rest/api/latest"
        repo = f"{api}/projects/{{key}}/repos/{{slug}}"
        pr = f"{repo}/pull-requests/{{pid:int}}"
        return [
            Route(f"{api}/application-properties", self._properties),
            Route(
                f"{self.context}/rest/build-status/latest/commits/{{sha}}",
                self._build_status_listing,
            ),
            Route(f"{api}/repos", self._list_repos),
            Route(repo, self._get_repo),
            Route(f"{repo}/default-branch", self._get_default_branch),
            Route(f"{repo}/branches", self._list_branches),
            Route(f"{repo}/commits/{{sha}}/builds", self._builds, methods=["GET", "POST"]),
            Route(f"{repo}/commits/{{sha}}", self._get_commit),
            Route(f"{repo}/pull-requests", self._pulls, methods=["GET", "POST"]),
            Route(pr, self._pull, methods=["GET", "PUT"]),
            Route(f"{pr}/merge", self._merge, methods=["GET", "POST"]),
            Route(f"{pr}/decline", self._decline, methods=["POST"]),
            Route(f"{pr}/activities", self._list_activities),
            Route(f"{pr}/comments", self._post_comment, methods=["POST"]),
            Route(f"{pr}/comments/{{cid:int}}", self._one_comment, methods=["GET", "PUT"]),
        ]

    def _user(self, request: Request) -> int | Response:
        header = request.headers.get("authorization", "")
        token = header.removeprefix("Bearer ") if header.startswith("Bearer ") else ""
        uid = self.tokens.get(token)
        if uid is not None:
            return uid
        body = capture("error-401.json")
        if self.echo_token_in_errors:
            body["errors"][0]["message"] += f" (presented {token or header})"
        return JSONResponse(body, status_code=401)

    def _repo_or_404(self, request: Request) -> Response | None:
        key, slug = request.path_params["key"], request.path_params["slug"]
        if (key, slug) != (self.project_key, self.slug):
            return JSONResponse(
                _errors(
                    f"Repository {key}/{slug} does not exist.",
                    "com.atlassian.bitbucket.repository.NoSuchRepositoryException",
                ),
                status_code=404,
            )
        return None

    def _page(self, request: Request, items: list[Any]) -> Response:
        if self._fail_countdown is not None:
            if self._fail_countdown == 0:
                self._fail_countdown = None
                return JSONResponse(_errors("page failed"), status_code=500)
            self._fail_countdown -= 1
        start = int(request.query_params.get("start", "0"))
        limit = int(request.query_params.get("limit", "25"))
        values = items[start : start + limit]
        last = start + limit >= len(items)
        payload: dict[str, Any] = {
            "size": len(values),
            "limit": limit,
            "isLastPage": last,
            "values": values,
            "start": start,
        }
        if not last:
            payload["nextPageStart"] = start + limit
        return JSONResponse(payload)

    async def _guard(self, request: Request) -> int | Response:
        uid = self._user(request)
        if isinstance(uid, Response):
            return uid
        if "key" in request.path_params:
            missing = self._repo_or_404(request)
            if missing is not None:
                return missing
        return uid

    async def _properties(self, request: Request) -> Response:
        uid = await self._guard(request)
        if isinstance(uid, Response):
            return uid
        return JSONResponse(
            {
                "version": self.version,
                "buildNumber": "".join(f"{int(p):03d}" for p in self.version.split(".")[:3]),
                "buildDate": "1730000000000",
                "displayName": "Bitbucket",
            }
        )

    async def _list_repos(self, request: Request) -> Response:
        uid = await self._guard(request)
        if isinstance(uid, Response):
            return uid
        return self._page(request, list(self._repos))

    async def _get_repo(self, request: Request) -> Response:
        uid = await self._guard(request)
        if isinstance(uid, Response):
            return uid
        return JSONResponse(self._target_repo())

    async def _get_default_branch(self, request: Request) -> Response:
        uid = await self._guard(request)
        if isinstance(uid, Response):
            return uid
        return JSONResponse(self._branch_payload(self.default_branch))

    async def _list_branches(self, request: Request) -> Response:
        uid = await self._guard(request)
        if isinstance(uid, Response):
            return uid
        text = request.query_params.get("filterText", "")
        names = sorted(name for name in self.branches if text in name)
        return self._page(request, [self._branch_payload(name) for name in names])

    async def _get_commit(self, request: Request) -> Response:
        uid = await self._guard(request)
        if isinstance(uid, Response):
            return uid
        sha = request.path_params["sha"]
        found = self.commits.get(sha)
        if found is None:
            return JSONResponse(
                _errors(
                    f"Commit '{sha}' does not exist in repository '{self.slug}'.",
                    "com.atlassian.bitbucket.commit.NoSuchCommitException",
                ),
                status_code=404,
            )
        return JSONResponse(found)

    async def _builds(self, request: Request) -> Response:
        # As Data Center 9.4.24 answers it on the live fixture: the repository
        # scoped read needs ``key`` (400 without) and returns that one status;
        # an unknown key is a 404 with no body.
        uid = await self._guard(request)
        if isinstance(uid, Response):
            return uid
        sha = request.path_params["sha"]
        if request.method == "POST":
            body = await request.json()
            self.post_status(
                sha,
                body["key"],
                body["state"],
                name=body.get("name"),
                url=body.get("url"),
                description=body.get("description"),
            )
            self.writes += 1
            return Response(status_code=204)
        key = request.query_params.get("key", "")
        if not key.strip():
            return JSONResponse(capture("error-400-build-key.json"), status_code=400)
        build = self.builds.get(sha, {}).get(key)
        if build is None:
            return Response(status_code=404)
        return JSONResponse(self._keyed_build(sha, build))

    def _keyed_build(self, sha: str, build: dict[str, Any]) -> dict[str, Any]:
        commit = self.commits.get(sha, {"id": sha, "displayId": sha[:11]})
        return {
            "createdDate": build["dateAdded"] + 1,
            **build,
            "updatedDate": build["dateAdded"],
            "commit": commit,
            "projectKey": self.project_key,
            "repositorySlug": self.slug,
        }

    async def _build_status_listing(self, request: Request) -> Response:
        # ``rest/build-status/latest/commits/{sha}``: every key posted on the
        # commit, from any repository, newest first, paged (live fixture).
        uid = await self._guard(request)
        if isinstance(uid, Response):
            return uid
        builds = self.builds.get(request.path_params["sha"], {}).values()
        newest = sorted(builds, key=lambda b: b["dateAdded"], reverse=True)
        return self._page(request, newest)

    async def _pulls(self, request: Request) -> Response:
        uid = await self._guard(request)
        if isinstance(uid, Response):
            return uid
        if request.method == "POST":
            return await self._open_pull(request, uid)
        state = request.query_params.get("state", "OPEN")
        direction = request.query_params.get("direction", "INCOMING")
        at = request.query_params.get("at")
        found = []
        for record in sorted(self.pulls.values(), key=lambda p: -p.id):
            if state != "ALL" and record.state != state:
                continue
            branch = record.from_branch if direction == "OUTGOING" else record.to_branch
            if at is not None and at != f"refs/heads/{branch}":
                continue
            found.append(self._pull_payload(record))
        return self._page(request, found)

    async def _open_pull(self, request: Request, uid: int) -> Response:
        body = await request.json()
        head = body["fromRef"]["id"].removeprefix("refs/heads/")
        base = body["toRef"]["id"].removeprefix("refs/heads/")
        if head not in self.branches or base not in self.branches:
            return JSONResponse(_errors("Branch does not exist"), status_code=404)
        duplicate = any(
            p.state == "OPEN" and (p.from_branch, p.to_branch) == (head, base)
            for p in self.pulls.values()
        )
        if duplicate:
            return JSONResponse(
                _errors(
                    "Only one pull request may be open for a given source and target branch",
                    "com.atlassian.bitbucket.pull.DuplicatePullRequestException",
                ),
                status_code=409,
            )
        pid = self._create_pull(
            uid,
            head,
            base,
            body["title"],
            body.get("description", ""),
            draft=bool(body.get("draft", False)),
        )
        self.writes += 1
        return JSONResponse(self._pull_payload(self.pulls[pid]), status_code=201)

    def _pull_or_404(self, request: Request) -> _Pull | Response:
        record = self.pulls.get(request.path_params["pid"])
        if record is None:
            return JSONResponse(
                _errors(
                    "Pull request does not exist.",
                    "com.atlassian.bitbucket.pull.NoSuchPullRequestException",
                ),
                status_code=404,
            )
        return record

    def _stale(self, record: _Pull, version: Any) -> Response | None:
        if version == record.version:
            return None
        body = capture("error-409-pull-request-out-of-date.json")
        body["errors"][0]["expectedVersion"] = version
        body["errors"][0]["pullRequest"] = self._pull_payload(record)
        return JSONResponse(body, status_code=409)

    async def _pull(self, request: Request) -> Response:
        uid = await self._guard(request)
        if isinstance(uid, Response):
            return uid
        record = self._pull_or_404(request)
        if isinstance(record, Response):
            return record
        if request.method == "GET":
            payload = self._pull_payload(record)
            if record.id in self._bump_on_read:
                self._bump_on_read.discard(record.id)
                record.version += 1
            return JSONResponse(payload)
        body = await request.json()
        stale = self._stale(record, body.get("version"))
        if stale is not None:
            return stale
        if "title" in body:
            record.title = body["title"]
        if "description" in body:
            record.description = body["description"]
        record.version, record.updated = record.version + 1, self.now()
        self.writes += 1
        return JSONResponse(self._pull_payload(record))

    async def _merge(self, request: Request) -> Response:
        uid = await self._guard(request)
        if isinstance(uid, Response):
            return uid
        record = self._pull_or_404(request)
        if isinstance(record, Response):
            return record
        if request.method == "GET":
            if record.merge is None:
                return JSONResponse(_errors("merge status is not available"), status_code=409)
            return JSONResponse({"vetoes": [], **record.merge})
        stale = self._stale(record, _int(request.query_params.get("version")))
        if stale is not None:
            return stale
        self.set_state(record.id, "MERGED")
        self.writes += 1
        return JSONResponse(self._pull_payload(record))

    async def _decline(self, request: Request) -> Response:
        uid = await self._guard(request)
        if isinstance(uid, Response):
            return uid
        record = self._pull_or_404(request)
        if isinstance(record, Response):
            return record
        stale = self._stale(record, _int(request.query_params.get("version")))
        if stale is not None:
            return stale
        self.set_state(record.id, "DECLINED")
        self.writes += 1
        return JSONResponse(self._pull_payload(record))

    async def _list_activities(self, request: Request) -> Response:
        uid = await self._guard(request)
        if isinstance(uid, Response):
            return uid
        record = self._pull_or_404(request)
        if isinstance(record, Response):
            return record
        newest_first = sorted(
            (a for a in self.activities if a.pull == record.id),
            key=lambda a: (a.created, a.id),
            reverse=True,
        )
        rendered = [self._activity_payload(a) for a in newest_first]
        return self._page(request, [a for a in rendered if a is not None])

    async def _post_comment(self, request: Request) -> Response:
        uid = await self._guard(request)
        if isinstance(uid, Response):
            return uid
        record = self._pull_or_404(request)
        if isinstance(record, Response):
            return record
        body = await request.json()
        parent = body.get("parent", {}).get("id")
        if parent is not None:
            found = self.comments.get(int(parent))
            if found is None or found.deleted or found.pull != record.id:
                return JSONResponse(_errors("Parent comment does not exist"), status_code=404)
        cid = self.comment(
            record.id,
            self.actor(uid),
            body["text"],
            anchor=body.get("anchor"),
            parent=None if parent is None else int(parent),
        )
        self.writes += 1
        return JSONResponse(self._comment_payload(self.comments[cid], markup=False), 201)

    async def _one_comment(self, request: Request) -> Response:
        uid = await self._guard(request)
        if isinstance(uid, Response):
            return uid
        record = self._pull_or_404(request)
        if isinstance(record, Response):
            return record
        found = self.comments.get(request.path_params["cid"])
        if found is None or found.deleted or found.pull != record.id:
            return JSONResponse(
                _errors(
                    "Comment does not exist.",
                    "com.atlassian.bitbucket.comment.NoSuchCommentException",
                ),
                status_code=404,
            )
        markup = request.query_params.get("markup") == "true"
        if request.method == "GET":
            return JSONResponse(self._comment_payload(found, markup=markup))
        if found.author != uid:
            return JSONResponse(capture("error-403.json"), status_code=403)
        body = await request.json()
        if body.get("version") != found.version:
            stale = capture("error-409-comment-out-of-date.json")
            stale["errors"][0].update(
                currentVersion=found.version, expectedVersion=body.get("version")
            )
            return JSONResponse(stale, status_code=409)
        found.text, found.version, found.updated = body["text"], found.version + 1, self.now()
        self._activity(record.id, uid, "COMMENTED", comment=found.id, comment_action="EDITED")
        self.writes += 1
        return JSONResponse(self._comment_payload(found, markup=markup))


FACTORY_UID = int(FACTORY.id)


def _int(value: str | None) -> int | None:
    return None if value is None else int(value)


def provider(token: str = FACTORY_TOKEN, *, base_url: str = API_URL) -> StaticTokenProvider:
    """The Bearer credential provider the DC connection builds (decision 4.2)."""

    return StaticTokenProvider(token, origin=base_url, header=CredentialHeader.AUTHORIZATION_BEARER)


def dc_adapter(
    fake: BitbucketDcFake,
    *,
    page_size: int = 2,
    token: str = FACTORY_TOKEN,
    client: httpx.AsyncClient | None = None,
) -> Any:
    """The DC adapter over ``fake``, built as `BitbucketDcConnection.adapter` builds it.

    Imported here, not at module load, so the fake and its tests collect while
    the adapter is missing.
    """

    from curie_api.forges.bitbucket_dc.code_host import BitbucketDcCodeHost

    return BitbucketDcCodeHost(
        client or fake.client(),
        api_url=fake.base_url,
        host=HOST,
        token=token,
        credentials=provider(token, base_url=fake.base_url),
        page_size=page_size,
    )


def code_host_entry(
    *,
    name: str = CODE_HOST_NAME,
    api_url: str = API_URL,
    host: str = HOST,
    token: str = FACTORY_TOKEN,
    **credential: Any,
) -> dict[str, Any]:
    """A ``codeHosts`` entry for a DC code host with a repository access token."""

    return {
        "name": name,
        "kind": types.BITBUCKET_DC,
        "host": host,
        "api_url": api_url,
        "credential": {"type": "static_token", "token": token, **credential},
    }


# The Jira side of a Bitbucket binding. Fictional ids, as in `config_samples`.
JIRA_TRACKER_NAME = "jira"
JIRA_SITE_ID = "a1b2c3d4-0000-4000-8000-000000000001"
JIRA_STARTER = Actor(id="557058:aaaaaaaa-0000-4000-8000-000000000001", login="starter")
JIRA_OUTSIDER = Actor(id="557058:aaaaaaaa-0000-4000-8000-000000000009", login="drive-by")
BINDING_NAME = "fac-demo"


def forges_document(
    *,
    code_host: dict[str, Any] | None = None,
    project_id: str = str(REPO_ID),
    path: str = REPO_PATH,
    reviewers: tuple[str, ...] = (REVIEWER.id,),
) -> dict[str, Any]:
    """A DC code host, a Jira Cloud tracker and the binding joining them.

    The start authority is an allowlist of `JIRA_STARTER`; the review feedback
    allowlist names ``reviewers`` under the DC code host's name (decision 11).
    """

    host = code_host or code_host_entry()
    return {
        "code_hosts": [host],
        "trackers": [
            {
                "name": JIRA_TRACKER_NAME,
                "kind": types.JIRA_CLOUD,
                "host": "example.atlassian.invalid",
                "api_url": "https://api.atlassian.invalid/ex/jira/site",
                "credential": {"type": "static_token", "token": "fixture-jira-token"},
                "label": "factory",
                "mention": "712020:bbbbbbbb-0000-4000-8000-000000000002",
            }
        ],
        "bindings": [
            {
                "name": BINDING_NAME,
                "tracker": JIRA_TRACKER_NAME,
                "tracker_scope_id": JIRA_SITE_ID,
                "repos": [
                    {
                        "alias": path.rsplit("/", 1)[-1],
                        "code_host": host["name"],
                        "project_id": project_id,
                        "path": path,
                    }
                ],
                "default_repo": path.rsplit("/", 1)[-1],
                "start_authority": {"type": "allowlist", "accounts": [JIRA_STARTER.id]},
                "review_feedback_allowlist": {host["name"]: list(reviewers)},
            }
        ],
    }


class BitbucketDcFakeBackend:
    """The fake as the shared Bitbucket lifecycle drives it
    (`forge_fakes.bitbucket_lifecycle.Backend`)."""

    kind = types.BITBUCKET_DC
    code_host_name = CODE_HOST_NAME

    def __init__(self, fake: BitbucketDcFake | None = None) -> None:
        self.fake = fake or BitbucketDcFake()

    def forges_document(self) -> dict[str, Any]:
        return forges_document()

    def adapter_client(self) -> httpx.AsyncClient:
        return self.fake.client()

    def repository(self) -> RepositoryRef:
        return self.fake.repository()

    def push(self, branch: str, message: str) -> str:
        return self.fake.push(branch, message)

    def post_status(self, sha: str, key: str, state: str) -> None:
        self.fake.post_status(sha, key, state)

    def actor(self, who: str) -> Actor:
        return {"reviewer": REVIEWER, "outsider": OUTSIDER}[who]

    def comment_as(
        self, who: str, pr: str, body: str, *, inline: bool, parent: str | None = None
    ) -> str:
        pull = int(pr)
        anchor = self.fake.inline_anchor(pull) if inline and parent is None else None
        return str(
            self.fake.comment(
                pull,
                self.actor(who),
                body,
                anchor=anchor,
                parent=None if parent is None else int(parent),
            )
        )

    def thread_replies(self, pr: str, root: str) -> list[tuple[str, str]]:
        replies = self.fake.thread_replies(int(root))
        return [(str(c.author), c.text) for c in replies if c.pull == int(pr)]
