"""The Bitbucket Data Center CodeHost adapter (ADR 0197, #4401).

It speaks Bitbucket Data Center REST under ``<api_url>/rest/api/latest`` with
one project or repository HTTP access token, sent as a Bearer header on every
API call and on git (decision 4.2). Its first call reads the server version;
below 9.4.0 every port call raises `ServerTooOld` (decision 7).

A repository is addressed by its stored ``PROJECT/slug`` path; only
`resolve_repository` and `credential` go by the numeric id, which Data Center
cannot read directly, so they page through ``GET /repos`` (decision 6.2).
Review feedback is pull request comments only, read from the activity stream
(decision 10). Build keys on a commit come from the build status API's
commit listing, the only listing 9.4 has, and each key is read back from the
repository-scoped builds API (decision 9). ``ci_diagnostics``,
``rerun_failed`` and ``user_can_write`` are unsupported (decision 3); review
authority falls back to the binding's allowlist (decision 11).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

import httpx

from curie_api.forges import types
from curie_api.forges.bitbucket_dc.comments import BitbucketDcMarkedComments
from curie_api.forges.bitbucket_dc.transport import BUILD_STATUS_API, DcTransport, object_of
from curie_api.forges.bitbucket_dc.version import MINIMUM, ServerTooOld, parse_version
from curie_api.forges.capabilities import CODE_HOST_OPERATIONS, Operation, Support
from curie_api.forges.credential_providers import StaticTokenProvider, TokenSnapshot
from curie_api.forges.errors import Ambiguous, NotFound, Unavailable, Unsupported
from curie_api.forges.paths import valid_repository_path
from curie_api.forges.retarget import retarget_checked
from curie_api.forges.types import (
    Actor,
    CheckSource,
    CheckState,
    CiReport,
    CiRollup,
    Commit,
    Credential,
    CredentialScope,
    FactoryPublication,
    FeedbackKind,
    FeedbackPage,
    NormalizedCheck,
    PullRequest,
    PullRequestRef,
    PullRequestState,
    RepositoryRef,
    RerunJob,
    RerunObserver,
    RerunRecord,
    ReviewFeedback,
    publication_body,
    publication_url,
    status_key,
)
from curie_api.publication_policy import is_publication_branch

logger = logging.getLogger(__name__)

CAPABILITIES: Mapping[Operation, Support] = {
    **dict.fromkeys(CODE_HOST_OPERATIONS, Support.SUPPORTED),
    Operation.CI_DIAGNOSTICS: Support.UNSUPPORTED,
    Operation.RERUN_FAILED: Support.UNSUPPORTED,
    Operation.USER_CAN_WRITE: Support.UNSUPPORTED,
    Operation.REQUIRED_CHECKS: Support.UNSUPPORTED,
}
# A feedback listing re-reads this far behind its cursor and drops what it
# already returned, so a comment stamped late is still listed once (decision 10.5).
OVERLAP = timedelta(seconds=120)
_OVERLAP_MS = round(OVERLAP.total_seconds() * 1000)

_PULL_STATES = {
    "OPEN": PullRequestState.OPEN,
    "MERGED": PullRequestState.MERGED,
    "DECLINED": PullRequestState.CLOSED,
    "SUPERSEDED": PullRequestState.CLOSED,
}
_BUILD_STATES = {
    "SUCCESSFUL": CheckState.SUCCESS,
    "FAILED": CheckState.FAILURE,
    "INPROGRESS": CheckState.PENDING,
    "UNKNOWN": CheckState.PENDING,
    "CANCELLED": CheckState.CANCELLED,
    "STOPPED": CheckState.CANCELLED,
}


def _ms_time(value: Any) -> datetime | None:
    if type(value) is not int:
        return None
    return datetime.fromtimestamp(value / 1000, UTC)


def _ms(stamp: datetime) -> int:
    return round(stamp.timestamp() * 1000)


def _text(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _int(value: Any) -> int:
    return value if type(value) is int else 0


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _user_id(user: Any) -> str | None:
    if isinstance(user, dict) and type(user.get("id")) is int:
        return str(user["id"])
    return None


def _build_check(key: str, build: Mapping[str, Any], head_sha: str) -> NormalizedCheck:
    reported = _text(build.get("state"))
    state = _BUILD_STATES.get(reported)
    if state is None:
        logger.warning("Bitbucket Data Center build state %r read as pending", reported)
        state = CheckState.PENDING
    return NormalizedCheck(
        key=status_key(key),
        state=state,
        head_sha=head_sha,
        name=_text(build.get("name")) or key,
        url=_text(build.get("url")) or None,
        source=CheckSource.STATUS,
        reported_state=reported,
        started_at=_ms_time(build.get("dateAdded")),
    )


@dataclass(frozen=True)
class _Cursor:
    """Where a feedback listing stopped: the newest creation time listed and
    the ids created within `OVERLAP` of it."""

    since: datetime
    seen: frozenset[str]

    @classmethod
    def parse(cls, cursor: str | None) -> _Cursor | None:
        if not cursor:
            return None
        loaded = json.loads(cursor)
        since = loaded.get("since") if isinstance(loaded, dict) else None
        seen = loaded.get("seen", []) if isinstance(loaded, dict) else None
        if not isinstance(since, str) or not isinstance(seen, list):
            raise ValueError("a Bitbucket feedback cursor carries an ISO-8601 'since' and 'seen'")
        stamp = datetime.fromisoformat(since)
        if stamp.tzinfo is None or not all(isinstance(item, str) for item in seen):
            raise ValueError("a Bitbucket feedback cursor carries an aware 'since' and string ids")
        return cls(stamp, frozenset(seen))

    def dump(self) -> str:
        return json.dumps({"since": self.since.isoformat(), "seen": sorted(self.seen)})


@dataclass(frozen=True)
class ThreadComment:
    """One live comment on a pull request as the activity stream shows it."""

    id: str
    author: Actor
    text: str
    created: int
    version: int
    parent: str | None
    anchored: bool


@dataclass(frozen=True)
class CommentTree:
    """Every live comment on one pull request, with its thread structure."""

    comments: Mapping[str, ThreadComment]

    def root_of(self, comment: ThreadComment) -> ThreadComment:
        seen = {comment.id}
        while comment.parent is not None and comment.parent in self.comments:
            comment = self.comments[comment.parent]
            if comment.id in seen:
                break
            seen.add(comment.id)
        return comment


@dataclass(frozen=True)
class _PullRead:
    payload: dict[str, Any]

    @property
    def author_id(self) -> str | None:
        author = self.payload.get("author")
        return _user_id(author.get("user")) if isinstance(author, dict) else None

    @property
    def state(self) -> PullRequestState:
        return _PULL_STATES.get(_text(self.payload.get("state")), PullRequestState.CLOSED)

    @property
    def head_sha(self) -> str:
        ref = self.payload.get("fromRef")
        return _text(ref.get("latestCommit")) if isinstance(ref, dict) else ""


class BitbucketDcCodeHost:
    """One Bitbucket Data Center as one access token."""

    kind = types.BITBUCKET_DC
    capabilities: Mapping[Operation, Support] = CAPABILITIES

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        entry: str,
        api_url: str,
        host: str,
        token: TokenSnapshot,
        credentials: StaticTokenProvider,
        page_size: int = 100,
    ) -> None:
        self.host = host
        self._http = DcTransport(client, entry=entry, api_url=api_url, token=token)
        self._token = token
        self._credentials = credentials
        self._page_size = page_size
        # The server's version as it reported it, and parsed.
        self._version: tuple[str, tuple[int, int, int]] | None = None
        # The numeric user id the access token acts as, once read.
        self._own_id: int | None = None
        self._comments = BitbucketDcMarkedComments(self)

    def __repr__(self) -> str:
        return f"BitbucketDcCodeHost(host={self.host!r}, api_url={self._http.root!r})"

    @property
    def marked_comments(self) -> BitbucketDcMarkedComments:
        return self._comments

    # Version floor and addressing ---------------------------------------------

    async def ensure_version(self, operation: Operation) -> None:
        """Read the server version once; refuse every call on a server older
        than 9.4.0. A failed read is not cached."""

        if self._version is None:
            payload = await self._http.json("GET", "/application-properties", what="version")
            reported = payload.get("version")
            if not isinstance(reported, str) or (parsed := parse_version(reported)) is None:
                raise Unavailable("version")
            self._version = (reported, parsed)
        text, parsed = self._version
        if parsed < MINIMUM:
            raise ServerTooOld(operation, text)

    def repo_path(self, repository: RepositoryRef) -> str:
        """``/projects/{KEY}/repos/{slug}`` of a repository on this server."""

        if repository.kind != self.kind or repository.host != self.host:
            raise NotFound("repository")
        if not valid_repository_path(self.kind, repository.path):
            raise NotFound("repository")
        parts = repository.path.split("/")
        if len(parts) != 2:
            raise NotFound("repository")
        key, slug = parts
        return f"/projects/{quote(key, safe='')}/repos/{quote(slug, safe='')}"

    def pull_path(self, pull_request: PullRequestRef) -> str:
        if not pull_request.number.isdigit():
            raise NotFound("pull_request")
        return f"{self.repo_path(pull_request.repository)}/pull-requests/{pull_request.number}"

    @property
    def transport(self) -> DcTransport:
        return self._http

    # Repositories ------------------------------------------------------------

    async def resolve_repository(self, project_id: str) -> RepositoryRef:
        await self.ensure_version(Operation.RESOLVE_REPOSITORY)
        return await self._resolve(project_id)

    async def _resolve(self, project_id: str) -> RepositoryRef:
        ref = RepositoryRef(self.kind, self.host, project_id, await self._path_of(project_id))
        return replace(ref, default_branch=await self._default_branch(self.repo_path(ref)))

    async def _path_of(self, project_id: str) -> str:
        """The current ``KEY/slug`` of the repository with id ``project_id``."""

        found: dict[str, Any] | None = None
        async for page in self._http.iter_pages("/repos", what="repository", limit=self._page_size):
            found = next((r for r in page if str(r.get("id")) == project_id), None)
            if found is not None:
                break
        if found is None:
            raise NotFound("repository")
        project = found.get("project")
        key = project.get("key") if isinstance(project, dict) else None
        slug = found.get("slug")
        if not isinstance(key, str) or not isinstance(slug, str):
            raise Unavailable("repository")
        return f"{key}/{slug}"

    async def _default_branch(self, repo: str) -> str | None:
        try:
            branch = await self._http.json("GET", f"{repo}/default-branch", what="default_branch")
        except NotFound:
            # An empty repository has no default branch yet.
            return None
        return _text(branch.get("displayId")) or None

    async def credential(self, repository: RepositoryRef, scope: CredentialScope) -> Credential:
        """The Bearer credential for git on ``<api_url>/scm/<KEY>/<slug>.git``,
        refused when the repository was renamed since it was stored."""

        await self.ensure_version(Operation.CREDENTIAL)
        self.repo_path(repository)
        if await self._path_of(repository.project_id) != repository.path:
            raise NotFound("repository_renamed")
        # The token this adapter's requests sent, not a fresh read (#4630).
        return self._credentials.credential(self._token, scope)

    async def branch_head(self, repository: RepositoryRef, branch: str) -> str | None:
        await self.ensure_version(Operation.BRANCH_HEAD)
        repo = self.repo_path(repository)
        listed = await self._http.pages(
            f"{repo}/branches",
            what="branch",
            limit=self._page_size,
            params={"filterText": branch},
        )
        for item in listed:
            if item.get("displayId") == branch:
                return _text(item.get("latestCommit")) or None
        return None

    async def read_commit(self, repository: RepositoryRef, sha: str) -> Commit:
        await self.ensure_version(Operation.READ_COMMIT)
        repo = self.repo_path(repository)
        payload = await self._http.json(
            "GET", f"{repo}/commits/{quote(sha, safe='')}", what="commit"
        )
        parents = payload.get("parents")
        return Commit(
            sha=_text(payload.get("id")),
            parents=tuple(
                _text(parent.get("id"))
                for parent in (parents if isinstance(parents, list) else [])
                if isinstance(parent, dict)
            ),
            message=_text(payload.get("message")),
        )

    # Pull requests -----------------------------------------------------------

    async def read_pull(self, pull_request: PullRequestRef) -> _PullRead:
        """The pull request as Data Center answers it, without the merge read."""

        return _PullRead(
            await self._http.json("GET", self.pull_path(pull_request), what="pull_request")
        )

    async def _conflicted(self, pull_request: PullRequestRef) -> bool | None:
        response = await self._http.request(
            "GET", f"{self.pull_path(pull_request)}/merge", what="merge_status", allow=(409,)
        )
        if response.status_code == 409:
            # Data Center cannot compute the merge right now (or the pull
            # request is closed): unknown, not clean.
            return None
        conflicted = object_of(response, "merge_status").get("conflicted")
        return conflicted if isinstance(conflicted, bool) else None

    async def _pull(self, repository: RepositoryRef, payload: dict[str, Any]) -> PullRequest:
        number = payload.get("id")
        if type(number) is not int:
            raise Unavailable("pull_request")
        ref = PullRequestRef(repository, str(number))
        read = _PullRead(payload)
        source, target = payload.get("fromRef"), payload.get("toRef")
        links = payload.get("links")
        own = links.get("self") if isinstance(links, dict) else None
        first = own[0] if isinstance(own, list) and own else None
        url = first.get("href") if isinstance(first, dict) else ""
        return PullRequest(
            ref=ref,
            head_sha=read.head_sha,
            head_ref=_text(source.get("displayId")) if isinstance(source, dict) else "",
            base_ref=_text(target.get("displayId")) if isinstance(target, dict) else "",
            state=read.state,
            url=_text(url),
            title=_text(payload.get("title")),
            body=_text(payload.get("description")),
            draft=payload.get("draft") is True,
            conflicted=await self._conflicted(ref),
            updated_at=_ms_time(payload.get("updatedDate")),
        )

    async def find_pull_request(
        self, repository: RepositoryRef, *, head_ref: str
    ) -> PullRequest | None:
        await self.ensure_version(Operation.FIND_PULL_REQUEST)
        repo = self.repo_path(repository)
        ref_id = f"refs/heads/{head_ref}"
        listed = await self._http.pages(
            f"{repo}/pull-requests",
            what="pull_request",
            limit=self._page_size,
            params={"at": ref_id, "direction": "OUTGOING", "state": "ALL"},
        )
        matches = [
            item
            for item in listed
            if isinstance(item.get("fromRef"), dict) and item["fromRef"].get("id") == ref_id
        ]
        if not matches:
            return None
        if len(matches) > 1:
            raise Ambiguous("pull_request")
        return await self._pull(repository, matches[0])

    async def own_user_id(self) -> int | None:
        """The numeric id of the user the access token acts as (its bot user),
        or None when the server does not say.

        The REST API (rest/api/latest) has no current-user resource. Bitbucket
        Server and Data Center name the authenticated user of an authenticated
        answer in the ``X-AUSERID`` response header (and ``X-AUSERNAME``), as
        Atlassian's server products do; it is not a documented REST contract,
        and a proxy may strip it. Read from ``GET /application-properties``;
        a numeric id is cached. An absent header is None, so ownership degrades
        to unverified, which rediscovery ignores (`list_factory_publications`). A
        present header that is not a numeric id is `Unavailable` and is not cached, so the listing
        fails closed rather than guessing ownership.
        """

        if self._own_id is None:
            response = await self._http.request(
                "GET", "/application-properties", what="own_identity"
            )
            raw = response.headers.get("x-auserid")
            if raw is None:
                return None
            raw = raw.strip()
            if not raw.isascii() or not raw.isdigit():
                raise Unavailable("own_identity")
            self._own_id = int(raw)
        return self._own_id

    async def list_factory_publications(
        self, repository: RepositoryRef
    ) -> tuple[FactoryPublication, ...]:
        """Every pull request from a publication branch of ``repository``
        itself, in any state, opened by the token's own user when the server
        names it (#4629).

        ``GET /projects/{KEY}/repos/{slug}/pull-requests?state=ALL``
        (https://developer.atlassian.com/server/bitbucket/rest/v904/api-group-pull-requests/#api-api-latest-projects-projectkey-repos-repositoryslug-pull-requests-get)
        read to its last page: the pull requests whose target is this
        repository. One counts when both ``fromRef`` and ``toRef`` name this
        repository's id and, when `own_user_id` is known, ``author.user.id`` is
        it (``owner_verified=True``). Without the ``X-AUSERID`` header every
        such pull request is listed with ``owner_verified=False``, and
        rediscovery ignores unverified publications on Data Center, so nothing
        is suppressed there and a reinstall can still open one duplicate pull
        request. An item, author, branch, state, description or URL
        that cannot be read fails the listing; a null or absent description
        is an empty body, and the ``links.self`` URL must be on the configured server
        (`types.publication_url`). ``closed_at`` is ``closedDate``, else
        ``updatedDate``: never earlier than the real close, so a doubt keeps
        an old trigger suppressed.
        """

        await self.ensure_version(Operation.LIST_FACTORY_PUBLICATIONS)
        repo = self.repo_path(repository)
        own = await self.own_user_id()
        listed = await self._http.pages(
            f"{repo}/pull-requests",
            what="pull_request",
            limit=self._page_size,
            params={"state": "ALL"},
            strict=True,
        )
        found: list[FactoryPublication] = []
        for item in listed:
            number, state = item.get("id"), item.get("state")
            author = _dict(item.get("author"))
            source, target = item.get("fromRef"), item.get("toRef")
            user_id = _user_id(author.get("user"))
            if (
                type(number) is not int
                or not isinstance(state, str)
                or state not in _PULL_STATES
                or (own is not None and user_id is None)
                or not isinstance(source, dict)
                or not isinstance(target, dict)
            ):
                raise Unavailable("pull_request")
            branch = source.get("displayId")
            if not isinstance(branch, str) or not branch:
                raise Unavailable("pull_request")
            if (own is not None and user_id != str(own)) or not is_publication_branch(branch):
                continue
            if not all(
                str(_dict(ref.get("repository")).get("id")) == repository.project_id
                for ref in (source, target)
            ):
                # A fork's branch is never ours.
                continue
            links = _dict(item.get("links")).get("self")
            first = links[0] if isinstance(links, list) and links else None
            url = first.get("href") if isinstance(first, dict) else None
            pull_state = _PULL_STATES[state]
            closed_at = None
            if pull_state is not PullRequestState.OPEN:
                closed_at = _ms_time(item.get("closedDate")) or _ms_time(item.get("updatedDate"))
            # Data Center omits an empty ``description`` from the JSON entirely.
            body = publication_body(item, "description") if "description" in item else ""
            found.append(
                FactoryPublication(
                    ref=PullRequestRef(repository, str(number)),
                    url=publication_url(url, self._http.root),
                    head_ref=branch,
                    state=pull_state,
                    body=body,
                    closed_at=closed_at,
                    owner_verified=own is not None,
                )
            )
        return tuple(found)

    async def open_pull_request(
        self,
        repository: RepositoryRef,
        *,
        head_ref: str,
        base_ref: str,
        title: str,
        body: str,
        draft: bool = False,
    ) -> PullRequest:
        await self.ensure_version(Operation.OPEN_PULL_REQUEST)
        repo = self.repo_path(repository)
        key, slug = repository.path.split("/")
        located = {"slug": slug, "project": {"key": key}}
        payload = await self._http.json(
            "POST",
            f"{repo}/pull-requests",
            what="pull_request",
            json={
                "title": title,
                "description": body,
                "draft": draft,
                "fromRef": {"id": f"refs/heads/{head_ref}", "repository": located},
                "toRef": {"id": f"refs/heads/{base_ref}", "repository": located},
            },
        )
        return await self._pull(repository, payload)

    async def update_pull_request(
        self, pull_request: PullRequestRef, *, title: str | None, body: str | None
    ) -> PullRequest:
        """Edit the title and or description, sending the version just read
        (decision 8.5). A version someone else moved on answers 409, raised as
        `Unavailable` so the caller retries with a fresh read."""

        await self.ensure_version(Operation.UPDATE_PULL_REQUEST)
        read = await self.read_pull(pull_request)
        if title is None and body is None:
            return await self._pull(pull_request.repository, read.payload)
        change: dict[str, Any] = {"version": read.payload.get("version")}
        if title is not None:
            change["title"] = title
        if body is not None:
            change["description"] = body
        updated = await self._http.put_versioned(
            self.pull_path(pull_request), what="pull_request", json=change
        )
        return await self._pull(pull_request.repository, updated)

    async def retarget_pull_request(
        self,
        pull_request: PullRequestRef,
        *,
        base_ref: str,
        expected_head_sha: str,
    ) -> PullRequest:
        await self.ensure_version(Operation.RETARGET_PULL_REQUEST)
        repository = pull_request.repository
        path = self.pull_path(pull_request)
        version: int | None = None

        async def parse(payload: dict[str, Any]) -> PullRequest:
            if type(payload.get("id")) is not int or str(payload["id"]) != pull_request.number:
                raise Unavailable("pull_request_mismatch")
            for side in ("fromRef", "toRef"):
                branch = _dict(payload.get(side))
                if (
                    str(_dict(branch.get("repository")).get("id")) != repository.project_id
                    or not isinstance(branch.get("displayId"), str)
                    or branch.get("id") != f"refs/heads/{branch['displayId']}"
                ):
                    raise Unavailable("pull_request_mismatch")
            return await self._pull(repository, payload)

        async def read() -> PullRequest:
            nonlocal version
            current = await self.read_pull(pull_request)
            reported = current.payload.get("version")
            if type(reported) is not int or reported < 0:
                raise Unavailable("pull_request_version")
            version = reported
            return await parse(current.payload)

        async def update() -> PullRequest:
            updated = await self._http.put_versioned(
                path,
                what="pull_request",
                json={"version": version, "toRef": {"id": f"refs/heads/{base_ref}"}},
            )
            return await parse(updated)

        return await retarget_checked(
            pull_request,
            base_ref=base_ref,
            expected_head_sha=expected_head_sha,
            read=read,
            update=update,
        )

    async def read_pull_request(self, pull_request: PullRequestRef) -> PullRequest:
        await self.ensure_version(Operation.READ_PULL_REQUEST)
        read = await self.read_pull(pull_request)
        return await self._pull(pull_request.repository, read.payload)

    # CI ----------------------------------------------------------------------

    async def observe_ci(self, repository: RepositoryRef, head_sha: str) -> CiRollup:
        """The build statuses posted on exactly ``head_sha`` in this repository
        (decision 9).

        Data Center 9.4's repository-scoped builds read answers 400 without a
        ``key`` and one status with it, so the keys come from the build status
        commit listing, which is server-wide. Each key's status is then read
        from the repository-scoped API Required Builds reads; a key that read
        does not answer was posted by another repository on the same commit
        and is not reported.
        """

        await self.ensure_version(Operation.OBSERVE_CI)
        builds = f"{self.repo_path(repository)}/commits/{quote(head_sha, safe='')}/builds"
        listed = await self._http.pages(
            f"/commits/{quote(head_sha, safe='')}",
            what="builds",
            limit=self._page_size,
            api=BUILD_STATUS_API,
        )
        keys = dict.fromkeys(
            key for build in listed if isinstance(key := build.get("key"), str) and key
        )
        checks: list[NormalizedCheck] = []
        for key in keys:
            try:
                build = await self._http.json("GET", builds, what="build", params={"key": key})
            except NotFound:
                continue
            checks.append(_build_check(key, build, head_sha))
        return CiRollup.on_head(head_sha, tuple(checks))

    async def required_checks(self, repository: RepositoryRef, branch: str) -> frozenset[str]:
        await self.ensure_version(Operation.REQUIRED_CHECKS)
        raise Unsupported(Operation.REQUIRED_CHECKS)

    async def ci_diagnostics(
        self, repository: RepositoryRef, head_sha: str, *, base_ref: str | None
    ) -> CiReport:
        await self.ensure_version(Operation.CI_DIAGNOSTICS)
        raise Unsupported(Operation.CI_DIAGNOSTICS)

    async def rerun_failed(
        self,
        repository: RepositoryRef,
        jobs: Sequence[RerunJob],
        *,
        settled: Collection[str] = (),
        on_attempt: RerunObserver | None = None,
    ) -> RerunRecord:
        await self.ensure_version(Operation.RERUN_FAILED)
        raise Unsupported(Operation.RERUN_FAILED)

    async def user_can_write(self, repository: RepositoryRef, actor: Actor) -> bool:
        # Data Center reports a user's effective permission only to an admin,
        # and omits inherited grants even then (decision 11).
        await self.ensure_version(Operation.USER_CAN_WRITE)
        raise Unsupported(Operation.USER_CAN_WRITE)

    # Review feedback ---------------------------------------------------------

    async def comment_tree(self, pull_request: PullRequestRef) -> CommentTree:
        """Every live comment on the pull request, read from every page of its
        activities. A thread's root appears, as it reads now, in the activity
        that added it, with its replies nested at any depth (9.4.24 adds no
        activity of its own for a reply or an edit); a comment that does have
        one is kept once. A deleted comment is gone from all but its
        ``DELETED`` activity."""

        activities = await self._http.pages(
            f"{self.pull_path(pull_request)}/activities",
            what="activities",
            limit=self._page_size,
        )
        deleted = {
            str(activity["comment"].get("id"))
            for activity in activities
            if activity.get("commentAction") == "DELETED"
            and isinstance(activity.get("comment"), dict)
        }
        found: dict[str, ThreadComment] = {}
        parents: dict[str, str] = {}

        def walk(comment: Any, parent: str | None) -> None:
            if not isinstance(comment, dict) or type(comment.get("id")) is not int:
                return
            cid = str(comment["id"])
            author = comment.get("author")
            author_id = _user_id(author)
            if parent is not None:
                parents[cid] = parent
            if cid not in found and cid not in deleted and author_id is not None:
                assert isinstance(author, dict)
                found[cid] = ThreadComment(
                    id=cid,
                    author=Actor(author_id, _text(author.get("slug"))),
                    text=_text(comment.get("text")),
                    created=_int(comment.get("createdDate")),
                    version=_int(comment.get("version")),
                    parent=None,
                    anchored=isinstance(comment.get("anchor"), dict),
                )
            for reply in comment.get("comments") or []:
                walk(reply, cid)

        for activity in activities:
            if activity.get("action") == "COMMENTED" and activity.get("commentAction") != "DELETED":
                walk(activity.get("comment"), None)
        return CommentTree(
            {cid: replace(comment, parent=parents.get(cid)) for cid, comment in found.items()}
        )

    async def list_review_feedback(
        self, pull_request: PullRequestRef, cursor: str | None
    ) -> FeedbackPage:
        """Comments by anyone but the pull request's author, newer than
        ``cursor`` less `OVERLAP`, minus the ids the cursor already returned.

        An unanchored top-level comment and its replies are ``COMMENT``; an
        inline comment and every reply under it are ``REVIEW_COMMENT`` with
        the inline root as ``thread_id``. Approvals and needs-work verdicts
        carry no text and are not feedback (decision 10).
        """

        await self.ensure_version(Operation.LIST_REVIEW_FEEDBACK)
        previous = _Cursor.parse(cursor)
        read = await self.read_pull(pull_request)
        tree = await self.comment_tree(pull_request)
        start = _ms(previous.since) - _OVERLAP_MS if previous is not None else None
        candidates = [
            comment
            for comment in tree.comments.values()
            if comment.author.id != read.author_id and (start is None or comment.created >= start)
        ]
        candidates.sort(key=lambda c: (c.created, int(c.id)))
        seen = previous.seen if previous is not None else frozenset()
        items: list[ReviewFeedback] = []
        for comment in candidates:
            if comment.id in seen:
                continue
            root = tree.root_of(comment)
            inline = root.anchored
            items.append(
                ReviewFeedback(
                    id=comment.id,
                    author=comment.author,
                    body=comment.text,
                    pull_request=pull_request,
                    head_sha=read.head_sha,
                    kind=FeedbackKind.REVIEW_COMMENT if inline else FeedbackKind.COMMENT,
                    thread_id=root.id if inline else None,
                )
            )
        stamps = [c.created for c in candidates]
        if previous is not None:
            stamps.append(_ms(previous.since))
        if not stamps:
            return FeedbackPage(tuple(items), "")
        newest = max(stamps)
        window = newest - _OVERLAP_MS
        following = _Cursor(
            datetime.fromtimestamp(newest / 1000, UTC),
            frozenset(c.id for c in candidates if c.created >= window),
        )
        return FeedbackPage(tuple(items), following.dump())

    async def verify_feedback(self, feedback: ReviewFeedback) -> bool:
        await self.ensure_version(Operation.VERIFY_FEEDBACK)
        pull_request = feedback.pull_request
        if not feedback.id.isdigit():
            return False
        try:
            read = await self.read_pull(pull_request)
            if read.state is not PullRequestState.OPEN:
                return False
            current = await self._http.json(
                "GET", f"{self.pull_path(pull_request)}/comments/{feedback.id}", what="comment"
            )
        except NotFound:
            return False
        if _user_id(current.get("author")) != feedback.author.id:
            return False
        return _text(current.get("text")) == feedback.body
