"""The GitLab CodeHost adapter (ADR 0197, #4399 decisions 4, 11 and 15 to 20).

It speaks REST v4 under the configured ``api_url`` with one static token, and
addresses every repository by its numeric project id, never its path
(decision 4). Git credentials are Basic ``oauth2:<token>`` on the web base
(decision 16), handed out only after the project's current path and clone URL
are checked. CI lives in `curie_api.forges.gitlab.ci`, review feedback in
`curie_api.forges.gitlab.review`, status comments in
`curie_api.forges.gitlab.comments`.
"""

from __future__ import annotations

import re
from collections.abc import Collection, Mapping, Sequence
from typing import Any

from curie_api.forges import types
from curie_api.forges.capabilities import CODE_HOST_OPERATIONS, Operation, Support
from curie_api.forges.credential_providers import StaticTokenProvider
from curie_api.forges.errors import Ambiguous, ForgeError, NotFound, Unauthorized, Unavailable
from curie_api.forges.gitlab import review
from curie_api.forges.gitlab.ci import GitLabCi
from curie_api.forges.gitlab.comments import GitLabMarkedComments
from curie_api.forges.gitlab.transport import GitLabTransport, encode_segment, parse_time
from curie_api.forges.types import (
    Actor,
    CiReport,
    CiRollup,
    Commit,
    Credential,
    CredentialScope,
    FeedbackPage,
    PullRequest,
    PullRequestRef,
    PullRequestState,
    RepositoryRef,
    RerunJob,
    RerunObserver,
    RerunRecord,
    ReviewFeedback,
)

CODE_HOST_CAPABILITIES: Mapping[Operation, Support] = dict.fromkeys(
    CODE_HOST_OPERATIONS, Support.SUPPORTED
)
_STATES = {
    "opened": PullRequestState.OPEN,
    "merged": PullRequestState.MERGED,
    "closed": PullRequestState.CLOSED,
    "locked": PullRequestState.CLOSED,
}
# The title prefixes GitLab reads as a draft; the factory writes ``Draft: ``.
_DRAFT_PREFIX = re.compile(r"^\s*(?:draft:|\[draft\]|\(draft\))\s*", re.IGNORECASE)
DRAFT_PREFIX = "Draft: "


def _text(value: Any) -> str:
    return value if isinstance(value, str) else ""


class GitLabCodeHost:
    """One GitLab as one static token."""

    kind = types.GITLAB
    capabilities: Mapping[Operation, Support] = CODE_HOST_CAPABILITIES

    def __init__(
        self,
        transport: GitLabTransport,
        *,
        host: str,
        web_base: str,
        credentials: StaticTokenProvider,
        repository: RepositoryRef | None = None,
    ) -> None:
        self._transport = transport
        self.host = host
        self._web_base = web_base
        self._credentials = credentials
        # The marked comments address the project a reply target names; with
        # ``repository`` they refuse a target on any other project.
        scope = repository.project_id if repository is not None else None
        self._comments = GitLabMarkedComments(transport, host=host, project_id=scope)

    def __repr__(self) -> str:
        return f"GitLabCodeHost(host={self.host!r}, web_base={self._web_base!r})"

    @property
    def marked_comments(self) -> GitLabMarkedComments:
        return self._comments

    # Addressing ----------------------------------------------------------------

    def _project(self, repository: RepositoryRef) -> str:
        if (
            repository.kind != self.kind
            or repository.host != self.host
            or not repository.project_id.isdigit()
        ):
            raise NotFound("repository")
        return f"/projects/{repository.project_id}"

    def _merge_request(self, pull_request: PullRequestRef) -> str:
        if not pull_request.number.isdigit():
            raise NotFound("pull_request")
        return f"{self._project(pull_request.repository)}/merge_requests/{pull_request.number}"

    def _ci(self, repository: RepositoryRef) -> GitLabCi:
        self._project(repository)
        return GitLabCi(self._transport, project_id=repository.project_id)

    # Repositories ----------------------------------------------------------------

    async def resolve_repository(self, project_id: str) -> RepositoryRef:
        if not project_id.isdigit():
            raise NotFound("repository")
        project = await self._transport.get_object(f"/projects/{project_id}")
        path = project.get("path_with_namespace")
        if type(project.get("id")) is not int or not isinstance(path, str) or not path:
            raise Unavailable("project")
        return RepositoryRef(
            self.kind,
            self.host,
            str(project["id"]),
            path,
            default_branch=_text(project.get("default_branch")) or None,
        )

    async def credential(self, repository: RepositoryRef, scope: CredentialScope) -> Credential:
        """Decision 16: refused when the project was renamed since it was stored,
        or when its clone URL is not on the configured web base."""

        project = await self._transport.get_object(self._project(repository))
        if project.get("path_with_namespace") != repository.path:
            raise NotFound("repository_renamed")
        clone_url = _text(project.get("http_url_to_repo"))
        if not clone_url.startswith(f"{self._web_base}/"):
            raise Unauthorized("origin_mismatch")
        return await self._credentials.get(scope)

    async def branch_head(self, repository: RepositoryRef, branch: str) -> str | None:
        path = f"{self._project(repository)}/repository/branches/{encode_segment(branch)}"
        try:
            found = await self._transport.get_object(path)
        except NotFound:
            return None
        commit = found.get("commit")
        sha = commit.get("id") if isinstance(commit, dict) else None
        return sha if isinstance(sha, str) and sha else None

    async def read_commit(self, repository: RepositoryRef, sha: str) -> Commit:
        commit = await self._transport.get_object(
            f"{self._project(repository)}/repository/commits/{encode_segment(sha)}"
        )
        parents = commit.get("parent_ids")
        return Commit(
            sha=_text(commit.get("id")),
            parents=tuple(p for p in (parents if isinstance(parents, list) else []) if p),
            message=_text(commit.get("message")),
        )

    # Merge requests ----------------------------------------------------------------

    def _pull(self, repository: RepositoryRef, payload: Mapping[str, Any]) -> PullRequest:
        iid = payload.get("iid")
        if type(iid) is not int:
            raise Unavailable("merge_request")
        title = _text(payload.get("title"))
        draft = payload.get("draft") is True
        if draft:
            title = _DRAFT_PREFIX.sub("", title, count=1)
        conflicted = payload.get("has_conflicts")
        return PullRequest(
            ref=PullRequestRef(repository, str(iid)),
            head_sha=_text(payload.get("sha")),
            head_ref=_text(payload.get("source_branch")),
            base_ref=_text(payload.get("target_branch")),
            state=_STATES.get(_text(payload.get("state")), PullRequestState.CLOSED),
            url=_text(payload.get("web_url")),
            title=title,
            body=_text(payload.get("description")),
            draft=draft,
            conflicted=conflicted if isinstance(conflicted, bool) else None,
            updated_at=parse_time(payload.get("updated_at")),
        )

    async def find_pull_request(
        self, repository: RepositoryRef, *, head_ref: str
    ) -> PullRequest | None:
        """Decision 17: every state, by source branch; more than one is `Ambiguous`."""

        listed = await self._transport.list_all(
            f"{self._project(repository)}/merge_requests",
            {"source_branch": head_ref, "state": "all"},
        )
        own = int(repository.project_id)
        matches = [
            item
            for item in listed
            if item.get("source_branch") == head_ref and item.get("source_project_id", own) == own
        ]
        if not matches:
            return None
        if len(matches) > 1:
            raise Ambiguous("merge_requests")
        return self._pull(repository, matches[0])

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
        created = await self._transport.write(
            "POST",
            f"{self._project(repository)}/merge_requests",
            json={
                "source_branch": head_ref,
                "target_branch": base_ref,
                "title": f"{DRAFT_PREFIX}{title}" if draft else title,
                "description": body,
                "remove_source_branch": False,
            },
        )
        return self._pull(repository, created)

    async def update_pull_request(
        self, pull_request: PullRequestRef, *, title: str | None, body: str | None
    ) -> PullRequest:
        """Send only what changes. A draft keeps its prefix when retitled, since
        titles are read back without it."""

        path = self._merge_request(pull_request)
        if title is None and body is None:
            return await self.read_pull_request(pull_request)
        change: dict[str, str] = {}
        if title is not None:
            current = await self._transport.get_object(path)
            change["title"] = f"{DRAFT_PREFIX}{title}" if current.get("draft") is True else title
        if body is not None:
            change["description"] = body
        updated = await self._transport.write("PUT", path, json=change)
        return self._pull(pull_request.repository, updated)

    async def read_pull_request(self, pull_request: PullRequestRef) -> PullRequest:
        payload = await self._transport.get_object(self._merge_request(pull_request))
        return self._pull(pull_request.repository, payload)

    # CI ----------------------------------------------------------------------------

    async def observe_ci(self, repository: RepositoryRef, head_sha: str) -> CiRollup:
        return await self._ci(repository).observe(head_sha)

    async def ci_diagnostics(
        self, repository: RepositoryRef, head_sha: str, *, base_ref: str | None
    ) -> CiReport:
        """Decision 19: each failing job's trace excerpt and, when a head check
        fails and ``base_ref`` is named, the checks on the base branch's head.
        Reading the base never fails the head's report (#4105)."""

        ci = self._ci(repository)
        rollup = await ci.observe(head_sha)
        diagnostics = await ci.diagnostics(rollup)
        base = None
        if base_ref is not None and rollup.failing_keys:
            try:
                base_head = await self.branch_head(repository, base_ref)
                if base_head is not None:
                    base = await ci.observe(base_head)
            except ForgeError:
                base = None
        return CiReport(rollup=rollup, diagnostics=diagnostics, base=base)

    async def rerun_failed(
        self,
        repository: RepositoryRef,
        jobs: Sequence[RerunJob],
        *,
        settled: Collection[str] = (),
        on_attempt: RerunObserver | None = None,
    ) -> RerunRecord:
        return await self._ci(repository).rerun(jobs, settled=settled, on_attempt=on_attempt)

    async def user_can_write(self, repository: RepositoryRef, actor: Actor) -> bool:
        """Decision 11: Developer or higher and an active user."""

        self._project(repository)
        return await self._transport.can_write(repository.project_id, actor.id)

    # Review feedback ---------------------------------------------------------------

    async def list_review_feedback(
        self, pull_request: PullRequestRef, cursor: str | None
    ) -> FeedbackPage:
        return await review.list_review_feedback(
            self._transport, self._merge_request(pull_request), pull_request, cursor
        )

    async def verify_feedback(self, feedback: ReviewFeedback) -> bool:
        return await review.verify_feedback(
            self._transport, self._merge_request(feedback.pull_request), feedback
        )
