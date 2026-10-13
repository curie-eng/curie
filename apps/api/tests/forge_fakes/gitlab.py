"""A stateful GitLab REST API v4 the GitLab adapter talks to over HTTP (#4399 decision 25).

`GitLabAPI` holds one self-managed GitLab with one project: users and their
membership, issues with labels, resource label and state events, notes,
merge requests with discussions, branches with commit chains, pipelines with
jobs and job traces, and external commit statuses. `GitLabAPI.handle` answers
one `httpx.Request`, so ``httpx.MockTransport(fake.handle)`` puts it behind a
real client; `GitLabAPI.app` is the same fake as a Starlette app for a test that
serves it over a socket.

Shapes follow GitLab's REST v4 reference (https://docs.gitlab.com/api/rest/),
section by section as each handler cites, with placeholder names and hosts
only. A later stream re-records payloads from the live CE 19.4.1 fixture; the
fields here are the ones GitLab returns, so an adapter that reads a field
GitLab does not send fails here first. Things the fake enforces rather than
records:

1. authentication by ``PRIVATE-TOKEN`` (or ``Authorization: Bearer``) with a
   registered, active token, 401 otherwise; ``Sudo: <username or id>`` acts as
   that user only for a token whose user is an admin and whose scopes include
   ``sudo`` (https://docs.gitlab.com/api/rest/authentication/#sudo), 403
   otherwise;
2. offset pagination (https://docs.gitlab.com/api/rest/#offset-based-pagination):
   ``page`` and ``per_page`` (default 20, max 100), capped again at
   ``page_size`` so a harness can page at two while the adapter asks for 100,
   with ``x-page``, ``x-per-page``, ``x-next-page`` (empty on the last page),
   ``x-prev-page``, ``x-total``, ``x-total-pages`` and ``Link``;
3. rate limit headers on every answer and a scripted 429 with ``Retry-After``
   (https://docs.gitlab.com/administration/settings/user_and_ip_rate_limits/
   #response-headers);
4. path segments addressed as GitLab addresses them: a project by numeric id or
   URL-encoded path, a branch by URL-encoded name
   (https://docs.gitlab.com/api/rest/#namespaced-paths), routed on the raw,
   still encoded request path;
5. label events, state events and notes draw ids from three separate sequences
   that start at the same number, as three GitLab tables do, so a bare id can
   name a label event and a note on one issue at once;
6. a note can be edited only by its author, as on GitLab (403 otherwise).

Every request is appended to ``requests``. ``writes`` counts mutating requests
the fake accepted; seeding helpers change state directly and count none.
Times are GitLab's ``YYYY-MM-DDTHH:MM:SS.mmmZ`` in UTC, from a fake clock that
each seeding helper advances by ``step_s`` and every answer reports in its
``Date`` header. Nothing here is a real host, account or token.
"""

from __future__ import annotations

import base64
import hashlib
import itertools
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from typing import Any
from urllib.parse import parse_qsl, quote, unquote

import httpx
from curie_api.forges import types
from curie_api.forges.types import Actor, RepositoryRef, TrackerIssueRef

HOST = "gitlab.example"
WEB = f"https://{HOST}"
API = f"{WEB}/api/v4"
PROJECT_ID = 4242
# Three segments: a group, a subgroup and the project (decision 4).
PROJECT_PATH = "acme/platform/widgets"
DEFAULT_BRANCH = "main"
# Placeholders, never real credentials (the recordings hygiene scan reads these).
TOKEN = "fixture-gitlab-bot-token"
ROOT_TOKEN = "fixture-gitlab-root-token"
# The token's own user: the factory's identity on GitLab.
BOT = Actor(id="900", login="curie-bot")
ROOT = Actor(id="1", login="root")
TRACKER_NAME = "gitlab"
CODE_HOST_NAME = "gitlab"
BINDING_NAME = "widgets"
LABEL = "factory"
_START = datetime(2026, 10, 1, 12, 0, 0, tzinfo=UTC)
# GitLab's own ceiling on ``per_page`` (https://docs.gitlab.com/api/rest/#pagination).
MAX_PER_PAGE = 100
_LISTING_DEFAULT = 20


def iso(moment: datetime) -> str:
    """A time as GitLab renders it: UTC, milliseconds, ``Z``."""

    moment = moment.astimezone(UTC)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def parse_iso(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)


def _message(status: int, text: str) -> dict[str, Any]:
    # GitLab's error body: {"message": "404 Project Not Found"}
    # (https://docs.gitlab.com/api/rest/troubleshooting/#status-codes).
    return {"message": f"{status} {text}"}


@dataclass
class _Token:
    user: int
    scopes: tuple[str, ...]
    active: bool = True
    revoked: bool = False
    expires_at: str | None = "2026-11-12"
    name: str = "curie-factory"
    id: int = 0


@dataclass
class _Issue:
    id: int
    iid: int
    title: str
    description: str
    author: int
    created_at: datetime
    updated_at: datetime
    state: str = "opened"
    labels: list[str] = field(default_factory=list)
    closed_at: datetime | None = None
    closed_by: int | None = None


@dataclass
class _LabelEvent:
    id: int
    issue: int
    user: int
    label: str
    action: str
    created_at: datetime


@dataclass
class _StateEvent:
    id: int
    issue: int
    user: int
    state: str
    created_at: datetime


@dataclass
class _Note:
    id: int
    body: str
    author: int
    created_at: datetime
    updated_at: datetime
    noteable_type: str
    noteable_iid: int
    system: bool = False
    type: str | None = None
    position: dict[str, Any] | None = None
    discussion: str | None = None


@dataclass
class _Discussion:
    id: str
    mr: int
    individual_note: bool
    notes: list[int] = field(default_factory=list)


@dataclass
class _MergeRequest:
    id: int
    iid: int
    title: str
    description: str
    source_branch: str
    target_branch: str
    author: int
    created_at: datetime
    updated_at: datetime
    state: str = "opened"
    has_conflicts: bool = False
    remove_source_branch: bool | None = None
    # The head a merged or closed merge request keeps; an open one follows
    # its source branch.
    frozen_sha: str | None = None
    merge_commit_sha: str | None = None


@dataclass
class _Pipeline:
    id: int
    iid: int
    sha: str
    ref: str
    source: str
    created_at: datetime


@dataclass
class _Job:
    id: int
    pipeline: int
    name: str
    status: str
    created_at: datetime
    allow_failure: bool = False
    retried: bool = False
    stage: str = "test"


@dataclass
class _Status:
    """An external commit status (``POST /projects/:id/statuses/:sha``)."""

    id: int
    pipeline: int
    sha: str
    ref: str
    name: str
    status: str
    author: int
    created_at: datetime
    target_url: str | None = None
    description: str | None = None


@dataclass
class _Scripted:
    status: int
    body: Any
    headers: dict[str, str]
    path: str | None
    method: str | None


class GitLabAPI:
    """One GitLab with project `PROJECT_PATH` (id `PROJECT_ID`) and its bot user."""

    def __init__(
        self,
        *,
        version: str = "19.4.1",
        page_size: int = MAX_PER_PAGE,
        base_url: str = API,
        project_id: int = PROJECT_ID,
        project_path: str = PROJECT_PATH,
        step_s: float = 600.0,
    ) -> None:
        self.version = version
        self.page_size = page_size
        self.base_url = base_url.rstrip("/")
        parts = httpx.URL(self.base_url)
        self._api_path = parts.raw_path.decode().rstrip("/")
        self.web_base = self.base_url.removesuffix("/api/v4")
        self.project_id = project_id
        self.project_path = project_path
        self.default_branch = DEFAULT_BRANCH
        self.http_url_to_repo: str | None = None
        self.step = timedelta(seconds=step_s)
        self.now = _START
        self.requests: list[httpx.Request] = []
        self.writes = 0
        # When set, an error body quotes the presented token, so a test proves
        # the adapter never repeats response text.
        self.echo_token_in_errors = False
        self.users: dict[int, dict[str, Any]] = {}
        self.admins: set[int] = set()
        self.tokens: dict[str, _Token] = {}
        # user id -> (access_level, user state) on the project, inherited included.
        self.members: dict[int, tuple[int, str]] = {}
        self.issues: dict[int, _Issue] = {}
        self.label_events: list[_LabelEvent] = []
        self.state_events: list[_StateEvent] = []
        self.notes: dict[int, _Note] = {}
        self.discussions: dict[str, _Discussion] = {}
        self.merge_requests: dict[int, _MergeRequest] = {}
        self.commits: dict[str, dict[str, Any]] = {}
        self.branches: dict[str, str] = {}
        self.pipelines: dict[int, _Pipeline] = {}
        self.jobs: dict[int, _Job] = {}
        self.traces: dict[int, str] = {}
        self.statuses: list[_Status] = []
        self._label_ids: dict[str, int] = {}
        self._issue_ids = itertools.count(3001)
        self._issue_iids = itertools.count(1)
        # Three tables, three sequences, one starting number (fake rule 5).
        self._label_event_ids = itertools.count(501)
        self._state_event_ids = itertools.count(501)
        self._note_ids = itertools.count(501)
        self._mr_ids = itertools.count(7001)
        self._mr_iids = itertools.count(1)
        self._pipeline_ids = itertools.count(9001)
        self._pipeline_iids = itertools.count(1)
        # Jobs and commit statuses share one table on GitLab (ci_builds).
        self._build_ids = itertools.count(12001)
        self._commit_seq = itertools.count(1)
        self._fail_countdown: int | None = None
        self._failed_pages: dict[tuple[str, int], int] = {}
        self._transport_errors = 0
        self._scripted: list[_Scripted] = []
        self._throttle: dict[str, Any] | None = None
        self.add_user(ROOT, admin=True, token=ROOT_TOKEN, scopes=("api", "sudo"))
        self.add_user(BOT, token=TOKEN, scopes=("api", "write_repository"))
        self.set_member(BOT, 30)
        self.push(self.default_branch, "initial", author=ROOT)

    # Clients ---------------------------------------------------------------

    def client(self) -> httpx.AsyncClient:
        """A client with no timeout of its own worth trusting: the adapter must
        send its 15 second timeout on every request."""

        return httpx.AsyncClient(transport=httpx.MockTransport(self.handle))

    @property
    def app(self) -> Any:
        """The fake as a Starlette app, for a test that serves it over a socket."""

        from starlette.applications import Starlette
        from starlette.requests import Request
        from starlette.responses import Response
        from starlette.routing import Route

        async def endpoint(request: Request) -> Response:
            built = httpx.Request(
                request.method,
                str(request.url),
                headers=list(request.headers.items()),
                content=await request.body(),
            )
            answer = self.handle(built)
            return Response(answer.content, answer.status_code, headers=dict(answer.headers))

        methods = ["GET", "POST", "PUT", "DELETE"]
        return Starlette(routes=[Route("/{path:path}", endpoint, methods=methods)])

    def repository(self) -> RepositoryRef:
        return RepositoryRef(
            types.GITLAB,
            HOST,
            str(self.project_id),
            self.project_path,
            default_branch=self.default_branch,
        )

    def issue_ref(self, iid: int) -> TrackerIssueRef:
        return TrackerIssueRef(
            kind=types.GITLAB,
            host=HOST,
            scope_id=str(self.project_id),
            issue_id=str(iid),
            display_key=f"{self.project_path}#{iid}",
        )

    # Request log -----------------------------------------------------------

    def api_path(self, request: httpx.Request) -> str:
        """The request's raw (still encoded) path below the API root."""

        raw = request.url.raw_path.decode().split("?", 1)[0]
        return raw.removeprefix(self._api_path)

    def calls(self, method: str | None = None, path: str | None = None) -> list[httpx.Request]:
        """Requests by method and API path (exact, or a prefix ending in ``*``)."""

        def matches(request: httpx.Request) -> bool:
            if method is not None and request.method != method:
                return False
            if path is None:
                return True
            own = self.api_path(request)
            return own.startswith(path[:-1]) if path.endswith("*") else own == path

        return [request for request in self.requests if matches(request)]

    @staticmethod
    def params(request: httpx.Request) -> dict[str, str]:
        return dict(parse_qsl(request.url.query.decode(), keep_blank_values=True))

    @staticmethod
    def body(request: httpx.Request) -> Any:
        content = request.content
        if not content:
            return None
        try:
            return json.loads(content)
        except ValueError:
            return dict(parse_qsl(content.decode(), keep_blank_values=True))

    # Clock -----------------------------------------------------------------

    def tick(self, at: datetime | None = None) -> datetime:
        """Advance the clock one step (or to ``at``) and return the new time."""

        self.now = at if at is not None else self.now + self.step
        return self.now

    # Knobs -----------------------------------------------------------------

    def fail_next_page(self, *, after: int = 0) -> None:
        """The listing page read after ``after`` more successful listing reads
        answers 502 once (the contract harness's fault)."""

        self._fail_countdown = after

    def fail_page(self, path: str, page: int, status: int = 502) -> None:
        """Page ``page`` of the listing at API ``path`` answers ``status`` once."""

        self._failed_pages[(path, page)] = status

    def transport_errors(self, count: int = 1) -> None:
        self._transport_errors = count

    def script(
        self,
        status: int,
        *,
        path: str | None = None,
        method: str | None = None,
        body: Any = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """The next request matching ``method`` and API ``path`` (a prefix ending
        in ``*``, or exact) gets this answer instead of the fake's."""

        default = _message(status, "Scripted")
        self._scripted.append(
            _Scripted(status, default if body is None else body, dict(headers or {}), path, method)
        )

    def throttle(
        self,
        *,
        retry_after: int | None = None,
        remaining_zero_reset: float | None = None,
        reset_epoch: float | None = None,
    ) -> None:
        """Throttle the next request.

        With ``remaining_zero_reset`` the request is answered normally but
        carries ``RateLimit-Remaining: 0`` and ``RateLimit-Reset`` at that epoch
        second. Otherwise it is a 429 carrying ``Retry-After`` when
        ``retry_after`` is set (absent when None) and ``RateLimit-Reset`` when
        ``reset_epoch`` is set, as GitLab's throttle answers
        (https://docs.gitlab.com/administration/settings/user_and_ip_rate_limits/).
        """

        self._throttle = {
            "retry_after": retry_after,
            "remaining_zero_reset": remaining_zero_reset,
            "reset_epoch": reset_epoch,
        }

    def rename_project(self, path: str) -> None:
        """Rename (move) the project; its id stays."""

        self.project_path = path

    def set_clone_url(self, url: str | None) -> None:
        """Override ``http_url_to_repo`` (None restores the derived one)."""

        self.http_url_to_repo = url

    # Seeding: users -------------------------------------------------------

    def add_user(
        self,
        actor: Actor,
        *,
        bot: bool = False,
        admin: bool = False,
        state: str = "active",
        token: str | None = None,
        scopes: tuple[str, ...] = ("api", "write_repository"),
        expires_at: str | None = "2026-11-12",
    ) -> None:
        uid = int(actor.id)
        self.users[uid] = {
            "id": uid,
            "username": actor.login,
            "name": f"Placeholder {actor.login}",
            "state": state,
            "locked": False,
            "avatar_url": f"{self.web_base}/uploads/-/system/user/avatar/{uid}/avatar.png",
            "web_url": f"{self.web_base}/{actor.login}",
            "bot": bot,
        }
        if admin:
            self.admins.add(uid)
        if token is not None:
            self.tokens[token] = _Token(uid, scopes, expires_at=expires_at, id=len(self.tokens) + 1)

    def set_member(self, actor: Actor, access_level: int | None, *, state: str = "active") -> None:
        """``actor``'s access level on the project (None removes the member)."""

        uid = int(actor.id)
        if access_level is None:
            self.members.pop(uid, None)
        else:
            self.members[uid] = (access_level, state)

    # Seeding: issues -------------------------------------------------------

    def add_issue(
        self,
        title: str,
        description: str = "",
        *,
        author: Actor | None = None,
        labels: Iterable[str] = (),
        at: datetime | None = None,
    ) -> int:
        """A new open issue; returns its iid."""

        stamp = self.tick(at)
        iid = next(self._issue_iids)
        self.issues[iid] = _Issue(
            id=next(self._issue_ids),
            iid=iid,
            title=title,
            description=description,
            author=int((author or ROOT).id),
            created_at=stamp,
            updated_at=stamp,
            labels=list(labels),
        )
        return iid

    def label(
        self,
        iid: int,
        label: str,
        actor: Actor,
        *,
        add: bool = True,
        at: datetime | None = None,
    ) -> int:
        """``actor`` adds (or removes) ``label``; returns the label event id."""

        return self._label(self.issues[iid], label, int(actor.id), add, self.tick(at))

    def set_issue_state(
        self, iid: int, state: str, actor: Actor, *, at: datetime | None = None
    ) -> int:
        """Close (``closed``) or reopen (``reopened``) the issue; returns the state event id."""

        return self._set_issue_state(self.issues[iid], state, int(actor.id), self.tick(at))

    def issue_note(
        self,
        iid: int,
        author: Actor,
        body: str,
        *,
        system: bool = False,
        at: datetime | None = None,
    ) -> int:
        stamp = self.tick(at)
        issue = self.issues[iid]
        note = self._note(body, int(author.id), stamp, "Issue", iid, system=system)
        issue.updated_at = max(issue.updated_at, stamp)
        return note.id

    def delete_issue(self, iid: int) -> None:
        del self.issues[iid]

    # Seeding: repository ---------------------------------------------------

    def add_commit(self, parents: tuple[str, ...], message: str = "change") -> str:
        seq = next(self._commit_seq)
        sha = hashlib.sha1(f"{seq}:{message}:{','.join(parents)}".encode()).hexdigest()
        self.commits[sha] = {
            "message": message,
            "parent_ids": list(parents),
            "created_at": self.now,
            "author": "Placeholder Author",
        }
        return sha

    def push(self, branch: str, message: str = "change", *, author: Actor | None = None) -> str:
        """A new commit on ``branch`` (created from the default branch when new);
        returns its 40-hex sha. An open merge request from the branch follows it
        and gets GitLab's ``added 1 commit`` system note."""

        stamp = self.tick()
        parent = self.branches.get(branch) or self.branches.get(self.default_branch)
        sha = self.add_commit(() if parent is None else (parent,), message)
        self.branches[branch] = sha
        for mr in self.merge_requests.values():
            if mr.source_branch == branch and mr.state == "opened":
                mr.updated_at = stamp
                self._mr_note(mr.iid, int((author or BOT).id), "added 1 commit", stamp, system=True)
        return sha

    # Seeding: merge requests ----------------------------------------------

    def add_merge_request(
        self,
        source_branch: str,
        *,
        target_branch: str = DEFAULT_BRANCH,
        title: str = "Change",
        description: str = "",
        author: Actor | None = None,
        state: str = "opened",
    ) -> int:
        if source_branch not in self.branches:
            self.push(source_branch)
        mr = self._create_mr(
            source_branch, target_branch, title, description, int((author or BOT).id)
        )
        if state != "opened":
            self.set_mr_state(mr.iid, state)
        return mr.iid

    def set_mr_state(self, iid: int, state: str) -> None:
        """``opened``, ``closed``, ``merged`` or ``locked``."""

        mr = self.merge_requests[iid]
        if state in ("closed", "merged") and mr.frozen_sha is None:
            mr.frozen_sha = self.branches.get(mr.source_branch)
        if state == "merged":
            mr.merge_commit_sha = self.add_commit(
                (self.branches[mr.target_branch], mr.frozen_sha or ""), "merge"
            )
        if state == "opened":
            mr.frozen_sha = None
        mr.state = state
        mr.updated_at = self.tick()

    def set_conflicts(self, iid: int, conflicted: bool) -> None:
        self.merge_requests[iid].has_conflicts = conflicted

    def mr_note(self, iid: int, author: Actor, body: str, *, system: bool = False) -> int:
        """A top-level merge request comment: an individual-note discussion."""

        return self._mr_note(iid, int(author.id), body, self.tick(), system=system).id

    def mr_discussion(self, iid: int, author: Actor, body: str, *, diff: bool = True) -> str:
        """A thread a person opens; on the diff when ``diff``. Returns its id."""

        stamp = self.tick()
        position = self._position(iid) if diff else None
        discussion = self._discussion(iid, individual=False)
        self._note(
            body,
            int(author.id),
            stamp,
            "MergeRequest",
            iid,
            type="DiffNote" if diff else "DiscussionNote",
            position=position,
            discussion=discussion.id,
        )
        self.merge_requests[iid].updated_at = stamp
        return discussion.id

    def discussion_reply(self, iid: int, discussion_id: str, author: Actor, body: str) -> int:
        stamp = self.tick()
        return self._reply(self.discussions[discussion_id], int(author.id), body, stamp).id

    def edit_note(self, note_id: int, body: str) -> None:
        note = self.notes[note_id]
        note.body = body
        note.updated_at = self.tick()

    def delete_note(self, note_id: int) -> None:
        note = self.notes.pop(note_id)
        if note.discussion is not None:
            self.discussions[note.discussion].notes.remove(note_id)

    def thread_notes(self, discussion_id: str) -> list[_Note]:
        return [self.notes[n] for n in self.discussions[discussion_id].notes]

    def top_level_notes(self, iid: int) -> list[_Note]:
        """Non-system notes in the merge request's individual-note discussions."""

        return [
            self.notes[n]
            for d in self.discussions.values()
            if d.mr == iid and d.individual_note
            for n in d.notes
            if not self.notes[n].system
        ]

    def issue_notes(self, iid: int) -> list[_Note]:
        return [
            n
            for n in self.notes.values()
            if n.noteable_type == "Issue" and n.noteable_iid == iid and not n.system
        ]

    def actor(self, user_id: int) -> Actor:
        return Actor(str(user_id), self.users[user_id]["username"])

    # Seeding: CI -----------------------------------------------------------

    def add_pipeline(
        self,
        sha: str,
        *,
        ref: str | None = None,
        source: str = "push",
        jobs: Mapping[str, str] | None = None,
    ) -> int:
        """A pipeline on ``sha`` with one job per ``jobs`` name and status."""

        stamp = self.tick()
        ref = ref or self._branch_of(sha) or self.default_branch
        pipeline = _Pipeline(
            next(self._pipeline_ids), next(self._pipeline_iids), sha, ref, source, stamp
        )
        self.pipelines[pipeline.id] = pipeline
        for name, status in (jobs or {}).items():
            self.add_job(pipeline.id, name, status)
        return pipeline.id

    def add_merged_results_pipeline(
        self, iid: int, *, parents: tuple[str, ...], jobs: Mapping[str, str]
    ) -> tuple[int, str]:
        """A merged results pipeline (Premium) on a merge commit with ``parents``,
        on ``refs/merge-requests/<iid>/merge``
        (https://docs.gitlab.com/ci/pipelines/merged_results_pipelines/).
        Returns the pipeline id and the merge commit sha."""

        merge_sha = self.add_commit(parents, f"Merge into {DEFAULT_BRANCH}")
        pipeline = self.add_pipeline(
            merge_sha,
            ref=f"refs/merge-requests/{iid}/merge",
            source="merge_request_event",
            jobs=jobs,
        )
        return pipeline, merge_sha

    def add_job(
        self,
        pipeline: int,
        name: str,
        status: str,
        *,
        allow_failure: bool = False,
        trace: str | None = None,
    ) -> int:
        stamp = self.tick()
        for job in self.jobs.values():
            if job.pipeline == pipeline and job.name == name and not job.retried:
                job.retried = True
        job = _Job(next(self._build_ids), pipeline, name, status, stamp, allow_failure)
        self.jobs[job.id] = job
        self.traces[job.id] = trace if trace is not None else f"Running {name}\n$ sh test.sh\n"
        return job.id

    def report_job(self, sha: str, name: str, status: str, *, allow_failure: bool = False) -> int:
        """A job ``name`` on the push pipeline of ``sha`` (created when missing)."""

        pipeline = next(
            (p.id for p in self.pipelines.values() if p.sha == sha and p.source == "push"),
            None,
        )
        if pipeline is None:
            pipeline = self.add_pipeline(sha)
        return self.add_job(pipeline, name, status, allow_failure=allow_failure)

    def add_status(self, sha: str, name: str, state: str, *, author: Actor | None = None) -> int:
        """An external commit status, as ``POST /projects/:id/statuses/:sha`` posts it."""

        return self._post_status(sha, name, state, int((author or ROOT).id), None, None).id

    # Internals: state changes -------------------------------------------------

    def _label_id(self, name: str) -> int:
        return self._label_ids.setdefault(name, 100 + len(self._label_ids))

    def _label(self, issue: _Issue, label: str, user: int, add: bool, stamp: datetime) -> int:
        if add and label not in issue.labels:
            issue.labels.append(label)
        elif not add and label in issue.labels:
            issue.labels.remove(label)
        else:
            return 0
        event = _LabelEvent(
            next(self._label_event_ids),
            issue.iid,
            user,
            label,
            "add" if add else "remove",
            stamp,
        )
        self.label_events.append(event)
        issue.updated_at = stamp
        return event.id

    def _set_issue_state(self, issue: _Issue, state: str, user: int, stamp: datetime) -> int:
        issue.state = "closed" if state == "closed" else "opened"
        issue.closed_at = stamp if state == "closed" else None
        issue.closed_by = user if state == "closed" else None
        issue.updated_at = stamp
        event = _StateEvent(next(self._state_event_ids), issue.iid, user, state, stamp)
        self.state_events.append(event)
        return event.id

    def _note(
        self,
        body: str,
        author: int,
        stamp: datetime,
        noteable_type: str,
        iid: int,
        *,
        system: bool = False,
        type: str | None = None,
        position: dict[str, Any] | None = None,
        discussion: str | None = None,
    ) -> _Note:
        note = _Note(
            next(self._note_ids),
            body,
            author,
            stamp,
            stamp,
            noteable_type,
            iid,
            system=system,
            type=type,
            position=position,
            discussion=discussion,
        )
        self.notes[note.id] = note
        if discussion is not None:
            self.discussions[discussion].notes.append(note.id)
        return note

    def _discussion(self, iid: int, *, individual: bool) -> _Discussion:
        seed = f"{iid}:{len(self.discussions)}:{self.now.isoformat()}"
        discussion = _Discussion(hashlib.sha1(seed.encode()).hexdigest(), iid, individual)
        self.discussions[discussion.id] = discussion
        return discussion

    def _mr_note(
        self, iid: int, author: int, body: str, stamp: datetime, *, system: bool = False
    ) -> _Note:
        discussion = self._discussion(iid, individual=True)
        note = self._note(
            body, author, stamp, "MergeRequest", iid, system=system, discussion=discussion.id
        )
        self.merge_requests[iid].updated_at = stamp
        return note

    def _reply(self, discussion: _Discussion, author: int, body: str, stamp: datetime) -> _Note:
        first = self.notes[discussion.notes[0]]
        # Replying to a single comment turns it into a thread
        # (https://docs.gitlab.com/api/discussions/#add-note-to-existing-merge-request-thread).
        discussion.individual_note = False
        note = self._note(
            body,
            author,
            stamp,
            "MergeRequest",
            discussion.mr,
            type="DiffNote" if first.position is not None else "DiscussionNote",
            position=first.position,
            discussion=discussion.id,
        )
        self.merge_requests[discussion.mr].updated_at = stamp
        return note

    def _position(self, iid: int) -> dict[str, Any]:
        mr = self.merge_requests[iid]
        head = self._mr_sha(mr)
        return {
            "base_sha": self.branches.get(mr.target_branch),
            "start_sha": self.branches.get(mr.target_branch),
            "head_sha": head,
            "old_path": "test.sh",
            "new_path": "test.sh",
            "position_type": "text",
            "old_line": None,
            "new_line": 1,
        }

    def _create_mr(
        self, source: str, target: str, title: str, description: str, author: int
    ) -> _MergeRequest:
        stamp = self.tick()
        mr = _MergeRequest(
            next(self._mr_ids),
            next(self._mr_iids),
            title,
            description,
            source,
            target,
            author,
            stamp,
            stamp,
        )
        self.merge_requests[mr.iid] = mr
        return mr

    def _mr_sha(self, mr: _MergeRequest) -> str | None:
        if mr.frozen_sha is not None:
            return mr.frozen_sha
        return self.branches.get(mr.source_branch)

    def _branch_of(self, sha: str) -> str | None:
        return next((name for name, head in self.branches.items() if head == sha), None)

    def _history(self, sha: str | None) -> set[str]:
        seen: set[str] = set()
        stack = [sha] if sha else []
        while stack:
            current = stack.pop()
            if current in seen or current not in self.commits:
                continue
            seen.add(current)
            stack.extend(self.commits[current]["parent_ids"])
        return seen

    def _external_pipeline(self, sha: str, ref: str) -> _Pipeline:
        found = next(
            (p for p in self.pipelines.values() if p.sha == sha and p.source == "external"),
            None,
        )
        if found is None:
            found = self.pipelines[self.add_pipeline(sha, ref=ref, source="external")]
        return found

    def _post_status(
        self,
        sha: str,
        name: str,
        state: str,
        author: int,
        target_url: str | None,
        description: str | None,
    ) -> _Status:
        ref = self._branch_of(sha) or self.default_branch
        pipeline = self._external_pipeline(sha, ref)
        status = _Status(
            next(self._build_ids),
            pipeline.id,
            sha,
            ref,
            name,
            state,
            author,
            self.tick(),
            target_url,
            description,
        )
        self.statuses.append(status)
        return status

    # Payloads ----------------------------------------------------------------

    def _user_basic(self, uid: int) -> dict[str, Any]:
        # The user as GitLab embeds it in author and user fields
        # (https://docs.gitlab.com/api/users/, "basic" representation).
        user = self.users[uid]
        return {key: user[key] for key in ("id", "username", "name", "state", "locked")} | {
            "avatar_url": user["avatar_url"],
            "web_url": user["web_url"],
        }

    def _user_full(self, uid: int, *, own: bool) -> dict[str, Any]:
        # https://docs.gitlab.com/api/users/#get-a-single-user and #get-the-current-user
        payload = self._user_basic(uid) | {
            "bot": self.users[uid]["bot"],
            "created_at": iso(_START - timedelta(days=30)),
            "bio": "",
            "location": "",
            "public_email": None,
            "linkedin": "",
            "twitter": "",
            "website_url": "",
            "organization": "",
            "job_title": "",
            "pronouns": None,
            "work_information": None,
            "followers": 0,
            "following": 0,
            "local_time": None,
            "is_followed": False,
        }
        if own:
            payload |= {
                "email": f"{self.users[uid]['username']}@{HOST}",
                "is_admin": uid in self.admins,
                "can_create_project": True,
                "two_factor_enabled": False,
                "external": False,
                "private_profile": False,
            }
        return payload

    def _clone_url(self) -> str:
        return self.http_url_to_repo or f"{self.web_base}/{self.project_path}.git"

    def _project(self) -> dict[str, Any]:
        # https://docs.gitlab.com/api/projects/#get-a-single-project
        namespace, _, name = self.project_path.rpartition("/")
        return {
            "id": self.project_id,
            "description": "",
            "name": name,
            "name_with_namespace": " / ".join(self.project_path.split("/")),
            "path": name,
            "path_with_namespace": self.project_path,
            "created_at": iso(_START - timedelta(days=30)),
            "default_branch": self.default_branch,
            "tag_list": [],
            "topics": [],
            "ssh_url_to_repo": f"git@{HOST}:{self.project_path}.git",
            "http_url_to_repo": self._clone_url(),
            "web_url": f"{self.web_base}/{self.project_path}",
            "readme_url": None,
            "visibility": "private",
            "archived": False,
            "issues_enabled": True,
            "merge_requests_enabled": True,
            "jobs_enabled": True,
            "issues_access_level": "enabled",
            "namespace": {
                "id": 77,
                "name": namespace.rsplit("/", 1)[-1],
                "path": namespace.rsplit("/", 1)[-1],
                "kind": "group",
                "full_path": namespace,
                "parent_id": 76,
                "web_url": f"{self.web_base}/groups/{namespace}",
            },
            "last_activity_at": iso(self.now),
        }

    def _issue(self, issue: _Issue) -> dict[str, Any]:
        # https://docs.gitlab.com/api/issues/#single-project-issue
        return {
            "id": issue.id,
            "iid": issue.iid,
            "project_id": self.project_id,
            "title": issue.title,
            "description": issue.description,
            "state": issue.state,
            "created_at": iso(issue.created_at),
            "updated_at": iso(issue.updated_at),
            "closed_at": None if issue.closed_at is None else iso(issue.closed_at),
            "closed_by": None if issue.closed_by is None else self._user_basic(issue.closed_by),
            "labels": list(issue.labels),
            "milestone": None,
            "assignees": [],
            "assignee": None,
            "author": self._user_basic(issue.author),
            "type": "ISSUE",
            "issue_type": "issue",
            "user_notes_count": len(self.issue_notes(issue.iid)),
            "merge_requests_count": 0,
            "upvotes": 0,
            "downvotes": 0,
            "due_date": None,
            "confidential": False,
            "discussion_locked": None,
            "web_url": f"{self.web_base}/{self.project_path}/-/issues/{issue.iid}",
            "references": {
                "short": f"#{issue.iid}",
                "relative": f"#{issue.iid}",
                "full": f"{self.project_path}#{issue.iid}",
            },
            "severity": "UNKNOWN",
            "task_completion_status": {"count": 0, "completed_count": 0},
        }

    def _label_event(self, event: _LabelEvent) -> dict[str, Any]:
        # https://docs.gitlab.com/api/resource_label_events/
        return {
            "id": event.id,
            "user": self._user_basic(event.user),
            "created_at": iso(event.created_at),
            "resource_type": "Issue",
            "resource_id": self.issues[event.issue].id if event.issue in self.issues else 0,
            "label": {
                "id": self._label_id(event.label),
                "name": event.label,
                "color": "#0033CC",
                "description": None,
            },
            "action": event.action,
        }

    def _state_event(self, event: _StateEvent) -> dict[str, Any]:
        # https://docs.gitlab.com/api/resource_state_events/
        return {
            "id": event.id,
            "user": self._user_basic(event.user),
            "created_at": iso(event.created_at),
            "resource_type": "Issue",
            "resource_id": self.issues[event.issue].id if event.issue in self.issues else 0,
            "state": event.state,
        }

    def _note_payload(self, note: _Note) -> dict[str, Any]:
        # https://docs.gitlab.com/api/notes/ and, for DiffNote, /api/discussions/
        payload: dict[str, Any] = {
            "id": note.id,
            "type": note.type,
            "body": note.body,
            "attachment": None,
            "author": self._user_basic(note.author),
            "created_at": iso(note.created_at),
            "updated_at": iso(note.updated_at),
            "system": note.system,
            "noteable_id": (
                self.issues[note.noteable_iid].id
                if note.noteable_type == "Issue" and note.noteable_iid in self.issues
                else self.merge_requests[note.noteable_iid].id
                if note.noteable_type == "MergeRequest"
                else 0
            ),
            "noteable_type": note.noteable_type,
            "project_id": self.project_id,
            "noteable_iid": note.noteable_iid,
            "resolvable": note.type in ("DiffNote", "DiscussionNote"),
            "confidential": False,
            "internal": False,
            "imported": False,
            "imported_from": "none",
            "commands_changes": {},
        }
        if payload["resolvable"]:
            payload |= {"resolved": False, "resolved_by": None, "resolved_at": None}
        if note.position is not None:
            payload["position"] = dict(note.position)
        return payload

    def _discussion_payload(self, discussion: _Discussion) -> dict[str, Any]:
        # https://docs.gitlab.com/api/discussions/#list-project-merge-request-discussion-items
        return {
            "id": discussion.id,
            "individual_note": discussion.individual_note,
            "notes": [self._note_payload(self.notes[n]) for n in discussion.notes],
        }

    def _mr(self, mr: _MergeRequest) -> dict[str, Any]:
        # https://docs.gitlab.com/api/merge_requests/#get-single-mr
        sha = self._mr_sha(mr)
        draft = bool(re.match(r"^\s*(\[draft\]|\(draft\)|draft:)", mr.title, re.IGNORECASE))
        return {
            "id": mr.id,
            "iid": mr.iid,
            "project_id": self.project_id,
            "title": mr.title,
            "description": mr.description,
            "state": mr.state,
            "created_at": iso(mr.created_at),
            "updated_at": iso(mr.updated_at),
            "merged_by": None,
            "merge_user": None,
            "merged_at": iso(mr.updated_at) if mr.state == "merged" else None,
            "closed_by": None,
            "closed_at": iso(mr.updated_at) if mr.state == "closed" else None,
            "target_branch": mr.target_branch,
            "source_branch": mr.source_branch,
            "user_notes_count": len(self.top_level_notes(mr.iid)),
            "upvotes": 0,
            "downvotes": 0,
            "author": self._user_basic(mr.author),
            "assignees": [],
            "assignee": None,
            "reviewers": [],
            "source_project_id": self.project_id,
            "target_project_id": self.project_id,
            "labels": [],
            "draft": draft,
            "work_in_progress": draft,
            "milestone": None,
            "merge_when_pipeline_succeeds": False,
            "merge_status": "cannot_be_merged" if mr.has_conflicts else "can_be_merged",
            "detailed_merge_status": "conflict" if mr.has_conflicts else "mergeable",
            "sha": sha,
            "merge_commit_sha": mr.merge_commit_sha,
            "squash_commit_sha": None,
            "discussion_locked": None,
            "should_remove_source_branch": None,
            "force_remove_source_branch": mr.remove_source_branch,
            "reference": f"!{mr.iid}",
            "references": {
                "short": f"!{mr.iid}",
                "relative": f"!{mr.iid}",
                "full": f"{self.project_path}!{mr.iid}",
            },
            "web_url": f"{self.web_base}/{self.project_path}/-/merge_requests/{mr.iid}",
            "squash": False,
            "has_conflicts": mr.has_conflicts,
            "blocking_discussions_resolved": True,
        }

    def _commit(self, sha: str) -> dict[str, Any]:
        # https://docs.gitlab.com/api/commits/#get-a-single-commit
        commit = self.commits[sha]
        stamp = iso(commit["created_at"])
        title = commit["message"].split("\n", 1)[0]
        return {
            "id": sha,
            "short_id": sha[:8],
            "title": title,
            "message": commit["message"],
            "parent_ids": list(commit["parent_ids"]),
            "author_name": commit["author"],
            "author_email": f"author@{HOST}",
            "authored_date": stamp,
            "committer_name": commit["author"],
            "committer_email": f"author@{HOST}",
            "committed_date": stamp,
            "created_at": stamp,
            "trailers": {},
            "extended_trailers": {},
            "web_url": f"{self.web_base}/{self.project_path}/-/commit/{sha}",
        }

    def _pipeline(self, pipeline: _Pipeline) -> dict[str, Any]:
        # https://docs.gitlab.com/api/pipelines/#list-project-pipelines
        return {
            "id": pipeline.id,
            "iid": pipeline.iid,
            "project_id": self.project_id,
            "sha": pipeline.sha,
            "ref": pipeline.ref,
            "status": self._pipeline_status(pipeline),
            "source": pipeline.source,
            "created_at": iso(pipeline.created_at),
            "updated_at": iso(pipeline.created_at),
            "web_url": f"{self.web_base}/{self.project_path}/-/pipelines/{pipeline.id}",
        }

    def _pipeline_status(self, pipeline: _Pipeline) -> str:
        states = [
            j.status
            for j in self.jobs.values()
            if j.pipeline == pipeline.id
            and not j.retried
            and not (j.allow_failure and j.status == "failed")
        ] + [s.status for s in self.statuses if s.pipeline == pipeline.id]
        for word in ("failed", "running", "pending", "canceled"):
            if word in states:
                return word
        return "success" if states else "created"

    def _job(self, job: _Job) -> dict[str, Any]:
        # https://docs.gitlab.com/api/jobs/#list-pipeline-jobs
        pipeline = self.pipelines[job.pipeline]
        started = None if job.status in ("created", "pending", "manual", "scheduled") else job
        finished = job.status in ("success", "failed", "canceled", "skipped")
        commit = (
            self._commit(pipeline.sha) if pipeline.sha in self.commits else {"id": pipeline.sha}
        )
        return {
            "id": job.id,
            "status": job.status,
            "stage": job.stage,
            "name": job.name,
            "ref": pipeline.ref,
            "tag": False,
            "coverage": None,
            "allow_failure": job.allow_failure,
            "created_at": iso(job.created_at),
            "started_at": None if started is None else iso(job.created_at),
            "finished_at": iso(job.created_at + timedelta(seconds=30)) if finished else None,
            "erased_at": None,
            "duration": 30.0 if finished else None,
            "queued_duration": 1.0,
            "user": self._user_basic(BOT_ID),
            "commit": commit,
            "pipeline": {
                "id": pipeline.id,
                "project_id": self.project_id,
                "ref": pipeline.ref,
                "sha": pipeline.sha,
                "status": self._pipeline_status(pipeline),
            },
            "web_url": f"{self.web_base}/{self.project_path}/-/jobs/{job.id}",
            "project": {"ci_job_token_scope_enabled": False},
            "artifacts": [],
            "runner": None,
            "failure_reason": "script_failure" if job.status == "failed" else None,
            "tag_list": [],
            "archived": False,
            "retried": job.retried,
        }

    def _status_payload(self, status: _Status) -> dict[str, Any]:
        # https://docs.gitlab.com/api/commits/#list-the-statuses-of-a-commit
        return {
            "id": status.id,
            "sha": status.sha,
            "ref": status.ref,
            "status": status.status,
            "name": status.name,
            "target_url": status.target_url,
            "description": status.description,
            "created_at": iso(status.created_at),
            "started_at": iso(status.created_at),
            "finished_at": iso(status.created_at),
            "allow_failure": False,
            "coverage": None,
            "pipeline_id": status.pipeline,
            "author": self._user_basic(status.author),
        }

    def _job_as_status(self, job: _Job) -> dict[str, Any]:
        # Every job is a commit status too, listed beside external ones.
        pipeline = self.pipelines[job.pipeline]
        return {
            "id": job.id,
            "sha": pipeline.sha,
            "ref": pipeline.ref,
            "status": job.status,
            "name": job.name,
            "target_url": f"{self.web_base}/{self.project_path}/-/jobs/{job.id}",
            "description": None,
            "created_at": iso(job.created_at),
            "started_at": iso(job.created_at),
            "finished_at": None,
            "allow_failure": job.allow_failure,
            "coverage": None,
            "pipeline_id": job.pipeline,
            "author": self._user_basic(BOT_ID),
        }

    def _branch(self, name: str) -> dict[str, Any]:
        # https://docs.gitlab.com/api/branches/#get-single-repository-branch
        return {
            "name": name,
            "merged": False,
            "protected": name == self.default_branch,
            "default": name == self.default_branch,
            "developers_can_push": False,
            "developers_can_merge": False,
            "can_push": True,
            "web_url": f"{self.web_base}/{self.project_path}/-/tree/{quote(name, safe='')}",
            "commit": self._commit(self.branches[name]),
        }

    # HTTP --------------------------------------------------------------------

    def handle(self, request: httpx.Request) -> httpx.Response:
        """Answer one request (the ``httpx.MockTransport`` handler)."""

        self.requests.append(request)
        if self._transport_errors:
            self._transport_errors -= 1
            raise httpx.ConnectError("connection refused", request=request)
        throttle, self._throttle = self._throttle, None
        if throttle is not None and throttle["remaining_zero_reset"] is None:
            return self._too_many(request, throttle)
        response = self._answer(request)
        headers = self._rate_headers(throttle)
        headers["date"] = format_datetime(self.now, usegmt=True)
        response.headers.update(headers)
        return response

    def _rate_headers(self, throttle: dict[str, Any] | None) -> dict[str, str]:
        if throttle is not None and throttle["remaining_zero_reset"] is not None:
            reset = int(throttle["remaining_zero_reset"])
            remaining = 0
        else:
            reset = int((self.now + timedelta(seconds=60)).timestamp())
            remaining = 1999
        return {
            "ratelimit-name": "throttle_authenticated_api",
            "ratelimit-limit": "2000",
            "ratelimit-observed": str(2000 - remaining),
            "ratelimit-remaining": str(remaining),
            "ratelimit-reset": str(reset),
            "ratelimit-resettime": format_datetime(
                datetime.fromtimestamp(reset, tz=UTC), usegmt=True
            ),
        }

    def _too_many(self, request: httpx.Request, throttle: dict[str, Any]) -> httpx.Response:
        headers = {"date": format_datetime(self.now, usegmt=True), "content-type": "text/plain"}
        if throttle["retry_after"] is not None:
            headers["retry-after"] = str(throttle["retry_after"])
        if throttle["reset_epoch"] is not None:
            reset = int(throttle["reset_epoch"])
            headers |= {
                "ratelimit-name": "throttle_authenticated_api",
                "ratelimit-limit": "5",
                "ratelimit-observed": "6",
                "ratelimit-remaining": "0",
                "ratelimit-reset": str(reset),
            }
        return httpx.Response(429, headers=headers, text="Retry later\n", request=request)

    def _error(self, request: httpx.Request, status: int, text: str) -> httpx.Response:
        body = _message(status, text)
        if self.echo_token_in_errors:
            presented = request.headers.get("private-token") or request.headers.get(
                "authorization", ""
            )
            body["message"] += f" (token {presented}, url {request.url})"
        return httpx.Response(status, json=body, request=request)

    def _acting_user(self, request: httpx.Request) -> int | httpx.Response:
        token = request.headers.get("private-token")
        if token is None:
            header = request.headers.get("authorization", "")
            token = header.removeprefix("Bearer ") if header.startswith("Bearer ") else ""
        record = self.tokens.get(token)
        if record is None or not record.active or record.revoked:
            return self._error(request, 401, "Unauthorized")
        uid = record.user
        sudo = request.headers.get("sudo")
        if sudo is None:
            return uid
        if uid not in self.admins or "sudo" not in record.scopes:
            return self._error(request, 403, "Forbidden - Must be admin to use sudo")
        target = next(
            (u["id"] for u in self.users.values() if sudo in (str(u["id"]), u["username"])),
            None,
        )
        if target is None:
            return self._error(request, 404, "User Not Found")
        return target

    def _answer(self, request: httpx.Request) -> httpx.Response:
        own = self.api_path(request)
        for index, scripted in enumerate(self._scripted):
            if scripted.method not in (None, request.method):
                continue
            if scripted.path is not None and not (
                own.startswith(scripted.path[:-1])
                if scripted.path.endswith("*")
                else own == scripted.path
            ):
                continue
            del self._scripted[index]
            return httpx.Response(
                scripted.status, json=scripted.body, headers=scripted.headers, request=request
            )
        raw = request.url.raw_path.decode().split("?", 1)[0]
        if not raw.startswith(self._api_path + "/"):
            return httpx.Response(404, text="Not Found", request=request)
        user = self._acting_user(request)
        if isinstance(user, httpx.Response):
            return user
        segments = own.strip("/").split("/")
        query = parse_qsl(request.url.query.decode(), keep_blank_values=True)
        answer = self._route(request, user, segments, query)
        if request.method != "GET" and answer.status_code < 300:
            self.writes += 1
        return answer

    def _route(
        self,
        request: httpx.Request,
        user: int,
        segments: list[str],
        query: list[tuple[str, str]],
    ) -> httpx.Response:
        method = request.method
        params = dict(query)
        head, rest = segments[0], segments[1:]
        if head == "version" and not rest:
            # https://docs.gitlab.com/api/version/
            return self._json(
                request,
                {
                    "version": self.version,
                    "revision": "0a1b2c3d",
                    "kas": {"enabled": False, "externalUrl": None, "version": None},
                    "enterprise": False,
                },
            )
        if head == "user" and not rest:
            return self._json(request, self._user_full(user, own=True))
        if head == "users" and len(rest) == 1:
            uid = int(rest[0]) if rest[0].isdigit() else -1
            if uid not in self.users:
                return self._error(request, 404, "User Not Found")
            return self._json(request, self._user_full(uid, own=False))
        if head == "personal_access_tokens" and rest == ["self"]:
            return self._token_self(request)
        if head == "projects" and rest:
            if unquote(rest[0]) not in (str(self.project_id), self.project_path):
                return self._error(request, 404, "Project Not Found")
            return self._project_route(request, user, rest[1:], params, query, method)
        return self._error(request, 404, "Not found")

    def _token_self(self, request: httpx.Request) -> httpx.Response:
        # https://docs.gitlab.com/api/personal_access_tokens/#get-single-personal-access-token
        token = request.headers.get("private-token", "")
        record = self.tokens[token]
        return self._json(
            request,
            {
                "id": record.id,
                "name": record.name,
                "revoked": record.revoked,
                "created_at": iso(_START - timedelta(days=1)),
                "description": "",
                "scopes": list(record.scopes),
                "user_id": record.user,
                "last_used_at": iso(self.now),
                "active": record.active,
                "expires_at": record.expires_at,
            },
        )

    def _project_route(
        self,
        request: httpx.Request,
        user: int,
        rest: list[str],
        params: dict[str, str],
        query: list[tuple[str, str]],
        method: str,
    ) -> httpx.Response:
        if not rest:
            return self._json(request, self._project())
        kind, tail = rest[0], rest[1:]
        if kind == "members" and len(tail) == 2 and tail[0] == "all":
            return self._member(request, tail[1])
        if kind == "issues":
            return self._issues_route(request, user, tail, params, query, method)
        if kind == "merge_requests":
            return self._mrs_route(request, user, tail, params, query, method)
        if kind == "repository":
            return self._repository_route(request, tail, params)
        if kind == "pipelines":
            return self._pipelines_route(request, tail, params)
        if kind == "jobs" and tail:
            return self._jobs_route(request, user, tail, method)
        if kind == "statuses" and len(tail) == 1 and method == "POST":
            return self._post_status_route(request, user, unquote(tail[0]))
        return self._error(request, 404, "Not found")

    def _member(self, request: httpx.Request, raw_id: str) -> httpx.Response:
        # https://docs.gitlab.com/api/members/#get-a-member-of-a-group-or-project-including-inherited-and-invited-members
        uid = int(raw_id) if raw_id.isdigit() else -1
        if uid not in self.members or uid not in self.users:
            return self._error(request, 404, "Not found")
        level, state = self.members[uid]
        return self._json(
            request,
            self._user_basic(uid)
            | {
                "state": state,
                "access_level": level,
                "created_at": iso(_START - timedelta(days=7)),
                "created_by": self._user_basic(ROOT_ID),
                "expires_at": None,
                "membership_state": "active",
            },
        )

    # Issues ------------------------------------------------------------------

    def _issues_route(
        self,
        request: httpx.Request,
        user: int,
        tail: list[str],
        params: dict[str, str],
        query: list[tuple[str, str]],
        method: str,
    ) -> httpx.Response:
        if not tail:
            if method == "POST":
                body = self.body(request) or {}
                iid = self.add_issue(body.get("title", ""), body.get("description", ""))
                self.issues[iid].author = user
                return self._json(request, self._issue(self.issues[iid]), status=201)
            return self._list_issues(request, params, query)
        iid = int(tail[0]) if tail[0].isdigit() else -1
        issue = self.issues.get(iid)
        if issue is None:
            return self._error(request, 404, "Not found")
        sub = tail[1:]
        if not sub:
            if method == "PUT":
                return self._edit_issue(request, user, issue)
            return self._json(request, self._issue(issue))
        if sub == ["resource_label_events"]:
            events = [self._label_event(e) for e in self.label_events if e.issue == iid]
            return self._page(request, events)
        if sub == ["resource_state_events"]:
            events = [self._state_event(e) for e in self.state_events if e.issue == iid]
            return self._page(request, events)
        if sub[0] == "notes":
            return self._notes_route(request, user, "Issue", iid, sub[1:], params, method)
        return self._error(request, 404, "Not found")

    def _list_issues(
        self, request: httpx.Request, params: dict[str, str], query: list[tuple[str, str]]
    ) -> httpx.Response:
        # https://docs.gitlab.com/api/issues/#list-project-issues
        found = list(self.issues.values())
        state = params.get("state", "all")
        if state != "all":
            found = [i for i in found if i.state == state]
        if params.get("labels"):
            wanted = params["labels"].split(",")
            found = [i for i in found if all(label in i.labels for label in wanted)]
        if "updated_after" in params:
            since = parse_iso(params["updated_after"])
            found = [i for i in found if i.updated_at >= since]
        if "updated_before" in params:
            until = parse_iso(params["updated_before"])
            found = [i for i in found if i.updated_at <= until]
        iids = {int(v) for k, v in query if k == "iids[]"}
        if iids:
            found = [i for i in found if i.iid in iids]
        order = params.get("order_by", "created_at")
        found.sort(
            key=lambda i: (getattr(i, order if order in ("updated_at",) else "created_at"), i.id),
            reverse=params.get("sort", "desc") == "desc",
        )
        return self._page(request, [self._issue(i) for i in found])

    def _edit_issue(self, request: httpx.Request, user: int, issue: _Issue) -> httpx.Response:
        # https://docs.gitlab.com/api/issues/#edit-an-issue
        body = self.body(request) or {}
        stamp = self.tick()
        for name in _labels(body.get("add_labels")):
            self._label(issue, name, user, True, stamp)
        for name in _labels(body.get("remove_labels")):
            self._label(issue, name, user, False, stamp)
        if "labels" in body:
            wanted = _labels(body["labels"])
            for name in [n for n in issue.labels if n not in wanted]:
                self._label(issue, name, user, False, stamp)
            for name in wanted:
                self._label(issue, name, user, True, stamp)
        if body.get("state_event") in ("close", "reopen"):
            state = "closed" if body["state_event"] == "close" else "reopened"
            self._set_issue_state(issue, state, user, stamp)
        for key in ("title", "description"):
            if key in body:
                setattr(issue, key, body[key])
                issue.updated_at = stamp
        return self._json(request, self._issue(issue))

    def _notes_route(
        self,
        request: httpx.Request,
        user: int,
        noteable: str,
        iid: int,
        sub: list[str],
        params: dict[str, str],
        method: str,
    ) -> httpx.Response:
        # https://docs.gitlab.com/api/notes/
        if not sub:
            if method == "POST":
                text = (self.body(request) or {}).get("body", "")
                stamp = self.tick()
                if noteable == "Issue":
                    note = self._note(text, user, stamp, "Issue", iid)
                    self.issues[iid].updated_at = stamp
                else:
                    note = self._mr_note(iid, user, text, stamp)
                return self._json(request, self._note_payload(note), status=201)
            listed = [
                n
                for n in self.notes.values()
                if n.noteable_type == noteable and n.noteable_iid == iid
            ]
            order = params.get("order_by", "created_at")
            listed.sort(
                key=lambda n: (n.updated_at if order == "updated_at" else n.created_at, n.id),
                reverse=params.get("sort", "desc") == "desc",
            )
            return self._page(request, [self._note_payload(n) for n in listed])
        note_id = int(sub[0]) if sub[0].isdigit() else -1
        note = self.notes.get(note_id)
        if note is None or note.noteable_type != noteable or note.noteable_iid != iid:
            return self._error(request, 404, "Note Not Found")
        if method == "PUT":
            return self._edit_note(request, user, note)
        return self._json(request, self._note_payload(note))

    def _edit_note(self, request: httpx.Request, user: int, note: _Note) -> httpx.Response:
        if note.author != user or note.system:
            return self._error(request, 403, "Forbidden")
        note.body = (self.body(request) or {}).get("body", note.body)
        note.updated_at = self.tick()
        return self._json(request, self._note_payload(note))

    # Merge requests -----------------------------------------------------------

    def _mrs_route(
        self,
        request: httpx.Request,
        user: int,
        tail: list[str],
        params: dict[str, str],
        query: list[tuple[str, str]],
        method: str,
    ) -> httpx.Response:
        if not tail:
            if method == "POST":
                return self._open_mr(request, user)
            return self._list_mrs(request, params, query)
        iid = int(tail[0]) if tail[0].isdigit() else -1
        mr = self.merge_requests.get(iid)
        if mr is None:
            return self._error(request, 404, "Not found")
        sub = tail[1:]
        if not sub:
            if method == "PUT":
                return self._edit_mr(request, mr)
            return self._json(request, self._mr(mr))
        if sub[0] == "notes":
            return self._notes_route(request, user, "MergeRequest", iid, sub[1:], params, method)
        if sub[0] == "discussions":
            return self._discussions_route(request, user, mr, sub[1:], method)
        if sub == ["pipelines"]:
            return self._page(request, [self._pipeline(p) for p in self._mr_pipelines(mr)])
        return self._error(request, 404, "Not found")

    def _list_mrs(
        self, request: httpx.Request, params: dict[str, str], query: list[tuple[str, str]]
    ) -> httpx.Response:
        # https://docs.gitlab.com/api/merge_requests/#list-project-merge-requests
        found = list(self.merge_requests.values())
        state = params.get("state", "all")
        if state != "all":
            found = [m for m in found if m.state == state]
        for key in ("source_branch", "target_branch"):
            if key in params:
                found = [m for m in found if getattr(m, key) == params[key]]
        iids = {int(v) for k, v in query if k == "iids[]"}
        if iids:
            found = [m for m in found if m.iid in iids]
        found.sort(key=lambda m: m.id, reverse=params.get("sort", "desc") == "desc")
        return self._page(request, [self._mr(m) for m in found])

    def _open_mr(self, request: httpx.Request, user: int) -> httpx.Response:
        # https://docs.gitlab.com/api/merge_requests/#create-mr
        body = self.body(request) or {}
        source, target = body.get("source_branch"), body.get("target_branch")
        if not source or not target or not body.get("title"):
            return httpx.Response(
                400,
                json={"error": "source_branch, target_branch, title are missing"},
                request=request,
            )
        if source not in self.branches or target not in self.branches:
            return httpx.Response(
                400,
                json={"message": {"source_branch": ["is invalid"]}},
                request=request,
            )
        existing = next(
            (
                m
                for m in self.merge_requests.values()
                if m.source_branch == source and m.state == "opened"
            ),
            None,
        )
        if existing is not None:
            return httpx.Response(
                409,
                json={
                    "message": [
                        "Another open merge request already exists for this source branch:"
                        f" !{existing.iid}"
                    ]
                },
                request=request,
            )
        mr = self._create_mr(source, target, body["title"], body.get("description", ""), user)
        if "remove_source_branch" in body:
            mr.remove_source_branch = _truthy(body["remove_source_branch"])
        return self._json(request, self._mr(mr), status=201)

    def _edit_mr(self, request: httpx.Request, mr: _MergeRequest) -> httpx.Response:
        # https://docs.gitlab.com/api/merge_requests/#update-mr
        body = self.body(request) or {}
        for key in ("title", "description", "target_branch"):
            if key in body:
                setattr(mr, key, body[key])
        if body.get("state_event") == "close":
            self.set_mr_state(mr.iid, "closed")
        elif body.get("state_event") == "reopen":
            self.set_mr_state(mr.iid, "opened")
        mr.updated_at = self.tick()
        return self._json(request, self._mr(mr))

    def _discussions_route(
        self,
        request: httpx.Request,
        user: int,
        mr: _MergeRequest,
        sub: list[str],
        method: str,
    ) -> httpx.Response:
        # https://docs.gitlab.com/api/discussions/#merge-requests
        if not sub:
            if method == "POST":
                body = self.body(request) or {}
                stamp = self.tick()
                discussion = self._discussion(mr.iid, individual=False)
                position = body.get("position")
                self._note(
                    body.get("body", ""),
                    user,
                    stamp,
                    "MergeRequest",
                    mr.iid,
                    type="DiffNote" if position else "DiscussionNote",
                    position=position,
                    discussion=discussion.id,
                )
                mr.updated_at = stamp
                return self._json(request, self._discussion_payload(discussion), status=201)
            listed = [d for d in self.discussions.values() if d.mr == mr.iid and d.notes]
            listed.sort(key=lambda d: self.notes[d.notes[0]].created_at)
            return self._page(request, [self._discussion_payload(d) for d in listed])
        discussion = self.discussions.get(sub[0])
        if discussion is None or discussion.mr != mr.iid:
            return self._error(request, 404, "Not found")
        rest = sub[1:]
        if not rest:
            return self._json(request, self._discussion_payload(discussion))
        if rest == ["notes"] and method == "POST":
            text = (self.body(request) or {}).get("body", "")
            note = self._reply(discussion, user, text, self.tick())
            return self._json(request, self._note_payload(note), status=201)
        if len(rest) == 2 and rest[0] == "notes" and rest[1].isdigit():
            note = self.notes.get(int(rest[1]))
            if note is None or note.discussion != discussion.id:
                return self._error(request, 404, "Not found")
            if method == "PUT":
                return self._edit_note(request, user, note)
            return self._json(request, self._note_payload(note))
        return self._error(request, 404, "Not found")

    def _mr_pipelines(self, mr: _MergeRequest) -> list[_Pipeline]:
        # https://docs.gitlab.com/api/merge_requests/#list-mr-pipelines
        refs = {f"refs/merge-requests/{mr.iid}/head", f"refs/merge-requests/{mr.iid}/merge"}
        history = self._history(self._mr_sha(mr))
        found = [
            p
            for p in self.pipelines.values()
            if p.ref in refs or (p.ref == mr.source_branch and p.sha in history)
        ]
        return sorted(found, key=lambda p: p.id, reverse=True)

    # Repository ---------------------------------------------------------------

    def _repository_route(
        self, request: httpx.Request, tail: list[str], params: dict[str, str]
    ) -> httpx.Response:
        if len(tail) == 2 and tail[0] == "branches":
            name = unquote(tail[1])
            if name not in self.branches:
                return self._error(request, 404, "Branch Not Found")
            return self._json(request, self._branch(name))
        if len(tail) >= 2 and tail[0] == "commits":
            sha = unquote(tail[1])
            if sha not in self.commits:
                sha = self.branches.get(sha, sha)
            if sha not in self.commits:
                return self._error(request, 404, "Commit Not Found")
            if len(tail) == 2:
                return self._json(request, self._commit(sha))
            if tail[2:] == ["merge_requests"]:
                # https://docs.gitlab.com/api/commits/#list-merge-requests-associated-with-a-commit
                found = [
                    m
                    for m in self.merge_requests.values()
                    if sha in self._history(self._mr_sha(m))
                    and sha not in self._history(self.branches.get(m.target_branch))
                ]
                return self._page(request, [self._mr(m) for m in found])
            if tail[2:] == ["statuses"]:
                return self._page(request, self._commit_statuses(sha, params))
        return self._error(request, 404, "Not found")

    def _commit_statuses(self, sha: str, params: dict[str, str]) -> list[dict[str, Any]]:
        listed: list[tuple[datetime, int, dict[str, Any]]] = []
        everything = _truthy(params.get("all", "false"))
        for job in self.jobs.values():
            if self.pipelines[job.pipeline].sha == sha and (everything or not job.retried):
                listed.append((job.created_at, job.id, self._job_as_status(job)))
        for status in self.statuses:
            if status.sha == sha:
                listed.append((status.created_at, status.id, self._status_payload(status)))
        if not everything:
            latest: dict[str, tuple[datetime, int, dict[str, Any]]] = {}
            for entry in listed:
                name = entry[2]["name"]
                if name not in latest or entry[:2] > latest[name][:2]:
                    latest[name] = entry
            listed = list(latest.values())
        listed.sort(key=lambda entry: entry[:2])
        return [payload for _, _, payload in listed]

    def _post_status_route(self, request: httpx.Request, user: int, sha: str) -> httpx.Response:
        # https://docs.gitlab.com/api/commits/#set-the-pipeline-status-of-a-commit
        if sha not in self.commits:
            return self._error(request, 404, "Commit Not Found")
        body = self.body(request) or {}
        state = body.get("state")
        if state not in ("pending", "running", "success", "failed", "canceled", "skipped"):
            return httpx.Response(
                400, json={"error": "state does not have a valid value"}, request=request
            )
        name = body.get("name") or body.get("context") or "default"
        status = self._post_status(
            sha, name, state, user, body.get("target_url"), body.get("description")
        )
        return self._json(request, self._status_payload(status), status=201)

    # Pipelines and jobs --------------------------------------------------------

    def _pipelines_route(
        self, request: httpx.Request, tail: list[str], params: dict[str, str]
    ) -> httpx.Response:
        if not tail:
            found = list(self.pipelines.values())
            for key in ("sha", "ref", "source"):
                if key in params:
                    found = [p for p in found if getattr(p, key) == params[key]]
            found.sort(key=lambda p: p.id, reverse=params.get("sort", "desc") == "desc")
            return self._page(request, [self._pipeline(p) for p in found])
        pid = int(tail[0]) if tail[0].isdigit() else -1
        pipeline = self.pipelines.get(pid)
        if pipeline is None:
            return self._error(request, 404, "Not found")
        if len(tail) == 1:
            return self._json(request, self._pipeline(pipeline))
        if tail[1:] == ["jobs"]:
            include_retried = _truthy(params.get("include_retried", "false"))
            jobs = [
                j
                for j in self.jobs.values()
                if j.pipeline == pid and (include_retried or not j.retried)
            ]
            jobs.sort(key=lambda j: j.id, reverse=True)
            return self._page(request, [self._job(j) for j in jobs])
        return self._error(request, 404, "Not found")

    def _jobs_route(
        self, request: httpx.Request, user: int, tail: list[str], method: str
    ) -> httpx.Response:
        job = self.jobs.get(int(tail[0]) if tail[0].isdigit() else -1)
        if job is None:
            return self._error(request, 404, "Not found")
        if tail[1:] == ["trace"]:
            # https://docs.gitlab.com/api/jobs/#get-a-log-file
            return httpx.Response(
                200,
                text=self.traces.get(job.id, ""),
                headers={"content-type": "text/plain"},
                request=request,
            )
        if tail[1:] == ["retry"] and method == "POST":
            # https://docs.gitlab.com/api/jobs/#retry-a-job
            if job.status not in ("failed", "canceled", "success") or job.retried:
                return self._error(request, 403, "Forbidden - Job is not retryable")
            new = self.add_job(job.pipeline, job.name, "pending", allow_failure=job.allow_failure)
            return self._json(request, self._job(self.jobs[new]), status=201)
        if len(tail) == 1:
            return self._json(request, self._job(job))
        return self._error(request, 404, "Not found")

    # Responses -----------------------------------------------------------------

    def _json(self, request: httpx.Request, payload: Any, *, status: int = 200) -> httpx.Response:
        return httpx.Response(status, json=payload, request=request)

    def _page(self, request: httpx.Request, items: list[Any]) -> httpx.Response:
        """One page of a listing, as GitLab's offset pagination answers it."""

        if self._fail_countdown is not None:
            if self._fail_countdown == 0:
                self._fail_countdown = None
                return self._error(request, 502, "Bad Gateway")
            self._fail_countdown -= 1
        params = self.params(request)
        page = max(1, int(params.get("page", "1") or "1"))
        failed = self._failed_pages.pop((self.api_path(request), page), None)
        if failed is not None:
            return self._error(request, failed, "Bad Gateway")
        asked = int(params.get("per_page", str(_LISTING_DEFAULT)) or _LISTING_DEFAULT)
        per_page = max(1, min(asked, MAX_PER_PAGE, self.page_size))
        start = (page - 1) * per_page
        values = items[start : start + per_page]
        total_pages = max(1, -(-len(items) // per_page))
        next_page = page + 1 if page < total_pages else None
        headers = {
            "x-page": str(page),
            "x-per-page": str(per_page),
            "x-next-page": "" if next_page is None else str(next_page),
            "x-prev-page": "" if page == 1 else str(page - 1),
            "x-total": str(len(items)),
            "x-total-pages": str(total_pages),
        }
        links = []
        base = str(request.url.copy_remove_param("page"))
        if next_page is not None:
            links.append(f'<{httpx.URL(base).copy_add_param("page", next_page)}>; rel="next"')
        links.append(f'<{httpx.URL(base).copy_add_param("page", 1)}>; rel="first"')
        links.append(f'<{httpx.URL(base).copy_add_param("page", total_pages)}>; rel="last"')
        headers["link"] = ", ".join(links)
        return httpx.Response(200, json=values, headers=headers, request=request)


BOT_ID = int(BOT.id)
ROOT_ID = int(ROOT.id)


def _labels(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value if str(v)]
    return [part for part in str(value).split(",") if part]


def _truthy(value: Any) -> bool:
    return value is True or str(value).lower() in ("true", "1", "yes")


def basic_oauth2_header(token: str) -> str:
    """The git header GitLab accepts for every token type (decision 16)."""

    pair = base64.b64encode(f"oauth2:{token}".encode()).decode()
    return f"Authorization: Basic {pair}"


def code_host_entry(
    *,
    name: str = CODE_HOST_NAME,
    api_url: str = API,
    host: str = HOST,
    token: str = TOKEN,
    **credential: Any,
) -> dict[str, Any]:
    """A ``codeHosts`` entry for a GitLab code host with a static token."""

    return {
        "name": name,
        "kind": types.GITLAB,
        "host": host,
        "api_url": api_url,
        "credential": {"type": "static_token", "token": token, **credential},
    }


def tracker_entry(
    *,
    name: str = TRACKER_NAME,
    api_url: str = API,
    host: str = HOST,
    token: str = TOKEN,
    label: str = LABEL,
    mention: str = BOT.login,
    **credential: Any,
) -> dict[str, Any]:
    """A ``trackers`` entry for a GitLab tracker with a static token."""

    return {
        "name": name,
        "kind": types.GITLAB,
        "host": host,
        "api_url": api_url,
        "credential": {"type": "static_token", "token": token, **credential},
        "label": label,
        "mention": mention,
        "poll_interval_s": 45,
    }


def forges_document(
    *,
    api_url: str = API,
    host: str = HOST,
    token: str = TOKEN,
    label: str = LABEL,
    project_id: int = PROJECT_ID,
    path: str = PROJECT_PATH,
    **credential: Any,
) -> dict[str, Any]:
    """A GitLab tracker and code host on one host and the binding joining them
    to the one project (a native pair, ADR 0197 identity 3)."""

    return {
        "code_hosts": [code_host_entry(api_url=api_url, host=host, token=token, **credential)],
        "trackers": [
            tracker_entry(api_url=api_url, host=host, token=token, label=label, **credential)
        ],
        "bindings": [
            {
                "name": BINDING_NAME,
                "tracker": TRACKER_NAME,
                "tracker_scope_id": str(project_id),
                "repos": [
                    {
                        "alias": BINDING_NAME,
                        "code_host": CODE_HOST_NAME,
                        "project_id": str(project_id),
                        "path": path,
                    }
                ],
            }
        ],
    }


def gitlab_registry(**document: Any) -> Any:
    """`build_registry` over `forges_document` (keyword arguments pass through)."""

    from curie_api.forges.config import ForgesConfig
    from curie_api.forges.registry import build_registry

    return build_registry(ForgesConfig.model_validate(forges_document(**document)))


def gitlab_tracker(
    fake: GitLabAPI,
    *,
    label: str | None = None,
    client: httpx.AsyncClient | None = None,
    **document: Any,
) -> Any:
    """The GitLab tracker the production builder makes for the fixture binding:
    `build_registry` -> `binding_tracker` -> `for_binding`, against ``fake``."""

    document.setdefault("api_url", fake.base_url)
    registry = gitlab_registry(**document)
    connection = registry.binding_tracker(TRACKER_NAME)
    return connection.for_binding(
        client or fake.client(), registry.binding(BINDING_NAME), label=label
    )


def gitlab_code_host(
    fake: GitLabAPI, *, client: httpx.AsyncClient | None = None, **document: Any
) -> Any:
    """The GitLab code host adapter `build_registry` builds, against ``fake``."""

    document.setdefault("api_url", fake.base_url)
    registry = gitlab_registry(**document)
    return registry.code_host(CODE_HOST_NAME).adapter(
        client or fake.client(), repository=fake.repository()
    )
