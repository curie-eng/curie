"""The Bitbucket Cloud CodeHost adapter (ADR 0197, #4401).

It speaks Bitbucket Cloud REST 2.0 with one repository, project or workspace
access token, sent as a Bearer header on every API call (decision 4.1).

A repository is addressed by its stored ``workspace/slug`` path; only
`resolve_repository` and `credential` go by the repository UUID, read under the
literal ``{}`` workspace (decision 6.1). Pull request payloads abbreviate the
head hash, so every pull request read expands it (decision 8.4). Review
feedback is pull request comments only (decision 10). ``ci_diagnostics``,
``rerun_failed`` and ``user_can_write`` are unsupported (decision 3); review
authority falls back to the binding's allowlist (decision 11).

``read_pull_request`` reads the mergeability checks around two pull request
reads and never merges (#4624).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import quote

import httpx

from curie_api.forges import types
from curie_api.forges.bitbucket_cloud.comments import BitbucketCloudMarkedComments
from curie_api.forges.bitbucket_cloud.connection import HOST
from curie_api.forges.bitbucket_cloud.transport import CloudTransport
from curie_api.forges.capabilities import CODE_HOST_OPERATIONS, Operation, Support
from curie_api.forges.credential_providers import StaticTokenProvider
from curie_api.forges.errors import Ambiguous, ForgeError, NotFound, Unavailable, Unsupported
from curie_api.forges.paths import valid_repository_path
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
}
# A feedback listing re-reads this far behind its cursor and drops what it
# already returned, so a comment stamped late is still listed once (decision 10.5).
OVERLAP = timedelta(seconds=120)
# Bitbucket Cloud's page length ceiling on pull request listings; most others allow 100.
_PULL_PAGELEN = 50
_FULL_HASH = 40
# Where Bitbucket Cloud's pull request pages live; a listed URL must be on it.
_WEB_BASE = f"https://{HOST}"

_PULL_STATES = {
    "OPEN": PullRequestState.OPEN,
    "MERGED": PullRequestState.MERGED,
    "DECLINED": PullRequestState.CLOSED,
    "SUPERSEDED": PullRequestState.CLOSED,
}
_STATUS_STATES = {
    "SUCCESSFUL": CheckState.SUCCESS,
    "FAILED": CheckState.FAILURE,
    "INPROGRESS": CheckState.PENDING,
    "STOPPED": CheckState.CANCELLED,
    "CANCELLED": CheckState.CANCELLED,
}
# The comment author types that are an account, a person or a token's bot user.
# Anything else (a deleted or anonymous author) is never feedback.
_ACCOUNT_TYPES = frozenset({"user", "app_user"})


def _text(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return stamp if stamp.tzinfo is not None else None


def _bbql_string(value: str) -> str:
    """``value`` as a BBQL string literal."""

    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _git_conflicted(payload: Mapping[str, Any]) -> bool | None:
    """Whether the pull request has Git conflicts, from its mergeability checks.

    Per the Bitbucket Cloud reference for ``pullrequests/{id}/mergeability/checks``,
    only the ``git_mergeability_check`` entry speaks to conflicts, and its UNKNOWN
    status does not imply the pull request is conflict-free. Anything but a clean
    pass or a conflict failure is None.
    """

    values = payload.get("values")
    if not isinstance(values, list):
        return None
    git = [
        item
        for item in values
        if isinstance(item, dict) and item.get("type") == "git_mergeability_check"
    ]
    if len(git) != 1:
        return None
    verdict = (git[0].get("status"), git[0].get("reason"))
    if verdict == ("PASSED", "clean"):
        return False
    if verdict == ("FAILED", "conflicts"):
        return True
    return None


def _commits(payload: Mapping[str, Any]) -> tuple[str, str]:
    """The raw source and destination commit hashes of a pull request payload."""

    return (
        _text(_dict(_dict(payload.get("source")).get("commit")).get("hash")),
        _text(_dict(_dict(payload.get("destination")).get("commit")).get("hash")),
    )


def account(user: Any) -> Actor | None:
    """The account behind a ``user`` object: its ``account_id`` and nickname."""

    found = _dict(user)
    account_id = found.get("account_id")
    if not isinstance(account_id, str) or not account_id:
        return None
    return Actor(account_id, _text(found.get("nickname")) or _text(found.get("display_name")))


@dataclass(frozen=True)
class CloudComment:
    """One pull request comment as Bitbucket Cloud lists it."""

    id: str
    author: Actor | None
    author_type: str
    raw: str
    created: datetime
    parent: str | None
    inline: bool
    deleted: bool

    @classmethod
    def of(cls, payload: Mapping[str, Any]) -> CloudComment | None:
        cid, created = payload.get("id"), _time(payload.get("created_on"))
        if type(cid) is not int or created is None:
            return None
        parent = _dict(payload.get("parent")).get("id")
        user = _dict(payload.get("user"))
        return cls(
            id=str(cid),
            author=account(user),
            author_type=_text(user.get("type")),
            raw=_text(_dict(payload.get("content")).get("raw")),
            created=created,
            parent=str(parent) if type(parent) is int else None,
            inline=isinstance(payload.get("inline"), dict),
            deleted=payload.get("deleted") is True,
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


class BitbucketCloudCodeHost:
    """Bitbucket Cloud as one access token."""

    kind = types.BITBUCKET_CLOUD
    host = HOST
    capabilities: Mapping[Operation, Support] = CAPABILITIES

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        entry: str,
        api_url: str,
        token: str,
        credentials: StaticTokenProvider,
        page_size: int = 100,
    ) -> None:
        self._http = CloudTransport(client, entry=entry, api_url=api_url, token=token)
        self._credentials = credentials
        self._page_size = page_size
        # Abbreviated hash -> full hash, per repository; a commit never changes.
        self._hashes: dict[tuple[str, str], str] = {}
        # A pull request's author, our own identity on it (decision 12.2).
        self._authors: dict[PullRequestRef, str | None] = {}
        self._comments = BitbucketCloudMarkedComments(self)

    def __repr__(self) -> str:
        return f"BitbucketCloudCodeHost(api_url={self._http.root!r})"

    @property
    def marked_comments(self) -> BitbucketCloudMarkedComments:
        return self._comments

    @property
    def transport(self) -> CloudTransport:
        return self._http

    # Addressing ----------------------------------------------------------------

    def repo_url(self, repository: RepositoryRef) -> str:
        """``<api>/repositories/{workspace}/{slug}`` of a Bitbucket Cloud repository."""

        if repository.kind != self.kind or repository.host != self.host:
            raise NotFound("repository")
        if not valid_repository_path(self.kind, repository.path):
            raise NotFound("repository")
        parts = repository.path.split("/")
        if len(parts) != 2:
            raise NotFound("repository")
        workspace, slug = parts
        return f"{self._http.root}/repositories/{quote(workspace, safe='')}/{quote(slug, safe='')}"

    def pull_url(self, pull_request: PullRequestRef) -> str:
        if not pull_request.number.isdigit():
            raise NotFound("pull_request")
        return f"{self.repo_url(pull_request.repository)}/pullrequests/{pull_request.number}"

    def pagelen(self, ceiling: int = 100) -> str:
        return str(min(self._page_size, ceiling))

    # Repositories ------------------------------------------------------------

    async def resolve_repository(self, project_id: str) -> RepositoryRef:
        url = f"{self._http.root}/repositories/{quote('{}', safe='')}/{quote(project_id, safe='')}"
        payload = await self._http.json("GET", url, what="repository")
        uuid, full_name = payload.get("uuid"), payload.get("full_name")
        if not isinstance(uuid, str) or not isinstance(full_name, str):
            raise Unavailable("repository")
        default = _text(_dict(payload.get("mainbranch")).get("name")) or None
        return RepositoryRef(self.kind, self.host, uuid, full_name, default_branch=default)

    async def credential(self, repository: RepositoryRef, scope: CredentialScope) -> Credential:
        """Git's basic ``x-token-auth`` credential for ``https://bitbucket.org``,
        refused when the repository was renamed since it was stored."""

        self.repo_url(repository)
        current = await self.resolve_repository(repository.project_id)
        if current.path != repository.path:
            raise NotFound("repository_renamed")
        return await self._credentials.get(scope)

    async def branch_head(self, repository: RepositoryRef, branch: str) -> str | None:
        url = f"{self.repo_url(repository)}/refs/branches/{quote(branch, safe='')}"
        response = await self._http.request("GET", url, what="branch", allow=(404,))
        if response.status_code == 404:
            return None
        target = _dict(response.json()).get("target")
        return _text(_dict(target).get("hash")) or None

    async def read_commit(self, repository: RepositoryRef, sha: str) -> Commit:
        payload = await self._read_commit(self.repo_url(repository), sha)
        parents = payload.get("parents")
        return Commit(
            sha=_text(payload.get("hash")),
            parents=tuple(
                _text(parent.get("hash"))
                for parent in (parents if isinstance(parents, list) else [])
                if isinstance(parent, dict)
            ),
            message=_text(payload.get("message")),
        )

    async def _read_commit(self, repo: str, sha: str) -> dict[str, Any]:
        return await self._http.json("GET", f"{repo}/commit/{quote(sha, safe='')}", what="commit")

    async def _full_hash(self, repo: str, sha: str) -> str:
        """``sha`` as 40 characters; Bitbucket abbreviates pull request heads."""

        if len(sha) >= _FULL_HASH or not sha:
            return sha
        known = self._hashes.get((repo, sha))
        if known is None:
            known = _text((await self._read_commit(repo, sha)).get("hash"))
            if len(known) != _FULL_HASH or not known.startswith(sha):
                raise Unavailable("commit")
            self._hashes[(repo, sha)] = known
        return known

    # Pull requests -----------------------------------------------------------

    async def read_pull(self, pull_request: PullRequestRef) -> dict[str, Any]:
        """The pull request as Bitbucket answers it, its author remembered."""

        payload = await self._http.json("GET", self.pull_url(pull_request), what="pull_request")
        self._remember_author(pull_request, payload)
        return payload

    def _remember_author(self, pull_request: PullRequestRef, payload: Mapping[str, Any]) -> None:
        author = account(payload.get("author"))
        self._authors[pull_request] = author.id if author is not None else None

    async def author_of(self, pull_request: PullRequestRef) -> str | None:
        """The account that opened the pull request: our own identity on it."""

        if pull_request not in self._authors:
            await self.read_pull(pull_request)
        return self._authors[pull_request]

    async def _pull(
        self,
        repository: RepositoryRef,
        payload: Mapping[str, Any],
        *,
        conflicted: bool | None,
    ) -> PullRequest:
        number = payload.get("id")
        if type(number) is not int:
            raise Unavailable("pull_request")
        ref = PullRequestRef(repository, str(number))
        self._remember_author(ref, payload)
        source, target = _dict(payload.get("source")), _dict(payload.get("destination"))
        head = _text(_dict(source.get("commit")).get("hash"))
        return PullRequest(
            ref=ref,
            head_sha=await self._full_hash(self.repo_url(repository), head),
            head_ref=_text(_dict(source.get("branch")).get("name")),
            base_ref=_text(_dict(target.get("branch")).get("name")),
            state=_PULL_STATES.get(_text(payload.get("state")), PullRequestState.CLOSED),
            url=_text(_dict(_dict(payload.get("links")).get("html")).get("href")),
            title=_text(payload.get("title")),
            body=_text(payload.get("description")),
            draft=payload.get("draft") is True,
            conflicted=conflicted,
            updated_at=_time(payload.get("updated_on")),
        )

    async def find_pull_request(
        self, repository: RepositoryRef, *, head_ref: str
    ) -> PullRequest | None:
        listed = await self._http.pages(
            f"{self.repo_url(repository)}/pullrequests",
            what="pull_request",
            params=[
                ("q", f"source.branch.name = {_bbql_string(head_ref)}"),
                *(("state", state) for state in _PULL_STATES),
                ("pagelen", self.pagelen(_PULL_PAGELEN)),
            ],
        )
        matches = [
            item
            for item in listed
            if _dict(_dict(item.get("source")).get("branch")).get("name") == head_ref
        ]
        if not matches:
            return None
        if len(matches) > 1:
            raise Ambiguous("pull_request")
        return await self._pull(repository, matches[0], conflicted=None)

    async def list_factory_publications(
        self, repository: RepositoryRef
    ) -> tuple[FactoryPublication, ...]:
        """Every pull request from a publication branch of ``repository``
        itself, in any state, each with ``owner_verified=False`` (#4629).

        Bounded limitation: an access token cannot read its own account
        (``GET /2.0/user`` answers 403, see `bitbucket_cloud.comments`), so
        ownership cannot be checked here. Every same-repository pull request
        whose branch is publication shaped is listed, a writer's lookalike
        included, and rediscovery drops every unverified publication, so it never treats one
        as evidence and does not suppress a reinstall on Bitbucket Cloud.

        ``GET /repositories/{workspace}/{slug}/pullrequests`` with every
        ``state`` (https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-get)
        read to its last page. A pull request counts when its source and
        destination repository are this one (by UUID). An item, branch, state,
        description or URL that cannot be read fails the listing; a null
        description is an empty body, and the ``links.html.href`` must be on
        ``https://bitbucket.org`` (`types.publication_url`). Bitbucket Cloud reports
        no close time, so ``closed_at`` of a merged or declined one is its
        ``updated_on``: never earlier than the real close.
        """

        listed = await self._http.pages(
            f"{self.repo_url(repository)}/pullrequests",
            what="pull_request",
            params=[
                *(("state", state) for state in _PULL_STATES),
                ("pagelen", self.pagelen(_PULL_PAGELEN)),
            ],
            strict=True,
        )
        found: list[FactoryPublication] = []
        for item in listed:
            number, state = item.get("id"), item.get("state")
            source, target = item.get("source"), item.get("destination")
            if (
                type(number) is not int
                or not isinstance(state, str)
                or state not in _PULL_STATES
                or not isinstance(source, dict)
                or not isinstance(target, dict)
            ):
                raise Unavailable("pull_request")
            branch = _dict(source.get("branch")).get("name")
            if not isinstance(branch, str) or not branch:
                raise Unavailable("pull_request")
            if not is_publication_branch(branch):
                continue
            if not all(
                _dict(side.get("repository")).get("uuid") == repository.project_id
                for side in (source, target)
            ):
                # A fork's branch is never ours.
                continue
            url = _dict(_dict(item.get("links")).get("html")).get("href")
            pull_state = _PULL_STATES[state]
            found.append(
                FactoryPublication(
                    ref=PullRequestRef(repository, str(number)),
                    url=publication_url(url, _WEB_BASE),
                    head_ref=branch,
                    state=pull_state,
                    body=publication_body(item, "description"),
                    closed_at=(
                        None
                        if pull_state is PullRequestState.OPEN
                        else _time(item.get("updated_on"))
                    ),
                    owner_verified=False,
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
        payload = await self._http.json(
            "POST",
            f"{self.repo_url(repository)}/pullrequests",
            what="pull_request",
            json={
                "title": title,
                "description": body,
                "source": {"branch": {"name": head_ref}},
                "destination": {"branch": {"name": base_ref}},
                "draft": draft,
                "close_source_branch": False,
            },
        )
        return await self._pull(repository, payload, conflicted=None)

    async def update_pull_request(
        self, pull_request: PullRequestRef, *, title: str | None, body: str | None
    ) -> PullRequest:
        if title is None and body is None:
            return await self.read_pull_request(pull_request)
        change: dict[str, str] = {}
        if title is not None:
            change["title"] = title
        if body is not None:
            change["description"] = body
        payload = await self._http.json(
            "PUT", self.pull_url(pull_request), what="pull_request", json=change
        )
        return await self._pull(pull_request.repository, payload, conflicted=None)

    async def read_pull_request(self, pull_request: PullRequestRef) -> PullRequest:
        """The pull request with its Git mergeability, proven only when the
        source and destination commits did not move around the checks read."""

        before = await self.read_pull(pull_request)
        if _text(before.get("state")) != "OPEN":
            return await self._pull(pull_request.repository, before, conflicted=None)
        try:
            checks: dict[str, Any] | None = await self._http.json(
                "GET", f"{self.pull_url(pull_request)}/mergeability/checks", what="mergeability"
            )
        except ForgeError:
            checks = None
        after = await self.read_pull(pull_request)
        conflicted: bool | None = None
        pinned = _commits(before)
        if checks is not None and all(pinned) and pinned == _commits(after):
            conflicted = _git_conflicted(checks)
        return await self._pull(pull_request.repository, after, conflicted=conflicted)

    # CI ----------------------------------------------------------------------

    async def observe_ci(self, repository: RepositoryRef, head_sha: str) -> CiRollup:
        """The build statuses posted on exactly ``head_sha``, Pipelines results
        included (decision 9). One check per status key."""

        listed = await self._http.pages(
            f"{self.repo_url(repository)}/commit/{quote(head_sha, safe='')}/statuses",
            what="statuses",
            params=[("pagelen", self.pagelen())],
        )
        latest: dict[str, dict[str, Any]] = {}
        for status in listed:
            key = status.get("key")
            if not isinstance(key, str) or not key:
                continue
            previous = latest.get(key)
            if previous is None or _text(status.get("updated_on")) >= _text(
                previous.get("updated_on")
            ):
                latest[key] = status
        checks = tuple(_status_check(key, status, head_sha) for key, status in latest.items())
        return CiRollup.on_head(head_sha, checks)

    async def ci_diagnostics(
        self, repository: RepositoryRef, head_sha: str, *, base_ref: str | None
    ) -> CiReport:
        raise Unsupported(Operation.CI_DIAGNOSTICS)

    async def rerun_failed(
        self,
        repository: RepositoryRef,
        jobs: Sequence[RerunJob],
        *,
        settled: Collection[str] = (),
        on_attempt: RerunObserver | None = None,
    ) -> RerunRecord:
        raise Unsupported(Operation.RERUN_FAILED)

    async def user_can_write(self, repository: RepositoryRef, actor: Actor) -> bool:
        # A write-scoped access token cannot read the repository's permission
        # configuration; that needs repository:admin (decision 11).
        raise Unsupported(Operation.USER_CAN_WRITE)

    # Comments and review feedback ---------------------------------------------

    async def list_comments(
        self, pull_request: PullRequestRef, *, since: datetime | None = None
    ) -> list[CloudComment]:
        """Every comment on the pull request, oldest first; with ``since``,
        only those created at or after it."""

        params: list[tuple[str, str]] = []
        if since is not None:
            params.append(("q", f"created_on >= {_bbql_string(since.isoformat())}"))
        params += [("sort", "created_on"), ("pagelen", self.pagelen())]
        listed = await self._http.pages(
            f"{self.pull_url(pull_request)}/comments", what="comments", params=params
        )
        comments = [c for c in (CloudComment.of(item) for item in listed) if c is not None]
        comments.sort(key=lambda c: (c.created, int(c.id)))
        return comments

    async def read_comment(self, pull_request: PullRequestRef, comment_id: str) -> CloudComment:
        if not comment_id.isdigit():
            raise NotFound("comment")
        payload = await self._http.json(
            "GET", f"{self.pull_url(pull_request)}/comments/{comment_id}", what="comment"
        )
        found = CloudComment.of(payload)
        if found is None:
            raise Unavailable("comment")
        return found

    async def root_of(
        self,
        pull_request: PullRequestRef,
        comment: CloudComment,
        known: dict[str, CloudComment],
    ) -> CloudComment:
        """The root of ``comment``'s thread, walking ``parent`` through
        ``known`` and reading a parent outside it once."""

        visited = {comment.id}
        while comment.parent is not None:
            parent = known.get(comment.parent)
            if parent is None:
                try:
                    parent = await self.read_comment(pull_request, comment.parent)
                except NotFound:
                    break
                known[parent.id] = parent
            if parent.id in visited:
                break
            visited.add(parent.id)
            comment = parent
        return comment

    async def list_review_feedback(
        self, pull_request: PullRequestRef, cursor: str | None
    ) -> FeedbackPage:
        """Comments by anyone but the pull request's author, created at or
        after ``cursor`` less `OVERLAP`, minus the ids the cursor already
        returned.

        A comment with no inline anchor in a thread with none is ``COMMENT``;
        an inline comment and every reply under it are ``REVIEW_COMMENT`` with
        the inline root as ``thread_id``. Approvals and change requests carry
        no text and are not comments (decision 10).
        """

        previous = _Cursor.parse(cursor)
        # Only the head is needed here, so no mergeability read.
        pull = await self._pull(
            pull_request.repository, await self.read_pull(pull_request), conflicted=None
        )
        own = self._authors.get(pull_request)
        listed = await self.list_comments(
            pull_request, since=previous.since - OVERLAP if previous is not None else None
        )
        known = {c.id: c for c in listed}
        candidates = [
            c
            for c in listed
            if not c.deleted
            and c.author is not None
            and c.author_type in _ACCOUNT_TYPES
            and c.author.id != own
        ]
        seen = previous.seen if previous is not None else frozenset()
        items: list[ReviewFeedback] = []
        for comment in candidates:
            if comment.id in seen:
                continue
            assert comment.author is not None
            root = await self.root_of(pull_request, comment, known)
            inline = comment.inline or root.inline
            items.append(
                ReviewFeedback(
                    id=comment.id,
                    author=comment.author,
                    body=comment.raw,
                    pull_request=pull_request,
                    head_sha=pull.head_sha,
                    kind=FeedbackKind.REVIEW_COMMENT if inline else FeedbackKind.COMMENT,
                    thread_id=root.id if inline else None,
                )
            )
        stamps = [c.created for c in candidates]
        if previous is not None:
            stamps.append(previous.since)
        if not stamps:
            return FeedbackPage(tuple(items), "")
        newest = max(stamps)
        following = _Cursor(
            newest, frozenset(c.id for c in candidates if c.created >= newest - OVERLAP)
        )
        return FeedbackPage(tuple(items), following.dump())

    async def verify_feedback(self, feedback: ReviewFeedback) -> bool:
        pull_request = feedback.pull_request
        try:
            payload = await self.read_pull(pull_request)
            if _PULL_STATES.get(_text(payload.get("state"))) is not PullRequestState.OPEN:
                return False
            current = await self.read_comment(pull_request, feedback.id)
        except NotFound:
            return False
        if current.deleted or current.author is None or current.author.id != feedback.author.id:
            return False
        return current.raw == feedback.body


def _status_check(key: str, status: Mapping[str, Any], head_sha: str) -> NormalizedCheck:
    reported = _text(status.get("state"))
    state = _STATUS_STATES.get(reported)
    if state is None:
        logger.warning("Bitbucket Cloud build state %r read as pending", reported)
        state = CheckState.PENDING
    on = _text(_dict(status.get("commit")).get("hash"))
    uuid = status.get("uuid")
    return NormalizedCheck(
        key=status_key(key),
        state=state,
        # Bitbucket may abbreviate the hash; a prefix of the head is the head.
        head_sha=head_sha if not on or head_sha.startswith(on) else on,
        name=_text(status.get("name")) or key,
        url=_text(status.get("url")) or None,
        source=CheckSource.STATUS,
        reported_state=reported,
        started_at=_time(status.get("created_on")),
        check_id=uuid if isinstance(uuid, str) else None,
    )
