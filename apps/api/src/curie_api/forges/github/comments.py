"""GitHub marked status comments behind the MarkedComments port (ADR 0197).

The core owns the marker text. GitHub carries it as an HTML comment on its own
line at the end of the body, which is how every factory comment has carried it.
Only comments the configured App posted (``performed_via_github_app.id``) count
as our own, so a person pasting the marker is neither found nor overwritten.

One class serves both sides: the tracker's issue comments and the code host's
pull request conversation and review threads.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping

import httpx

from curie_api.forges.capabilities import MARKED_COMMENT_OPERATIONS, Operation, Support
from curie_api.forges.errors import NotFound, Unauthorized, Unavailable
from curie_api.forges.github.marked_comments import (
    MarkerWrite,
    find_own_marker,
    upsert_marker,
)
from curie_api.forges.github.transport import github_headers
from curie_api.forges.types import GITHUB, MarkedComment, ReplyTarget, UpsertResult
from curie_api.repo_full_name import repo_url_path

TokenSource = Callable[[], Awaitable[str]]


def static_token(token: str) -> TokenSource:
    """A token source for a token the caller already minted."""

    async def source() -> str:
        return token

    return source


def _refusal(status: int | None, what: str) -> Exception:
    if status == 401 or status == 403:
        return Unauthorized(what)
    if status == 404:
        return NotFound(what)
    return Unavailable(what)


class GitHubMarkedComments:
    """Marked comments on one GitHub repository's issues and pull requests."""

    kind = GITHUB
    capabilities: Mapping[Operation, Support] = dict.fromkeys(
        MARKED_COMMENT_OPERATIONS, Support.SUPPORTED
    )

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        api: str,
        host: str,
        repo_full_name: str,
        repository_id: int,
        token: TokenSource,
        app_id: str,
    ) -> None:
        self._client = client
        self._api = api.rstrip("/")
        self.host = host
        self._repo_path = f"/repos/{repo_url_path(repo_full_name)}"
        self._repository_id = str(repository_id)
        self._token = token
        self._app_id = app_id

    @staticmethod
    def embed(marker: str, body: str) -> str:
        """``body`` carrying ``marker`` exactly as this adapter posts it."""

        return f"{body}\n\n<!-- {marker} -->\n"

    def _address(self, target: ReplyTarget) -> tuple[int, int | None]:
        """The issue or pull request number and the thread's comment id."""

        if target.issue is not None:
            if target.issue.kind != GITHUB or target.issue.scope_id != self._repository_id:
                raise NotFound("issue")
            return int(target.issue.issue_id), None
        assert target.pull_request is not None
        repository = target.pull_request.repository
        if repository.kind != GITHUB or repository.project_id != self._repository_id:
            raise NotFound("pull request")
        thread = int(target.thread_id) if target.thread_id is not None else None
        return int(target.pull_request.number), thread

    async def find_marked(self, target: ReplyTarget, marker: str) -> MarkedComment | None:
        number, thread = self._address(target)
        found = await find_own_marker(
            self._client,
            api=self._api,
            repo_path=self._repo_path,
            headers=github_headers(await self._token()),
            number=number,
            thread_id=thread,
            marker=f"<!-- {marker} -->",
            app_id=self._app_id,
        )
        if isinstance(found, MarkerWrite):
            raise _refusal(found.status, "comments")
        if found.comment_id is None or found.body is None:
            return None
        return MarkedComment(str(found.comment_id), target, found.body)

    async def upsert_marked(self, target: ReplyTarget, marker: str, body: str) -> UpsertResult:
        number, thread = self._address(target)
        written = await upsert_marker(
            self._client,
            api=self._api,
            repo_path=self._repo_path,
            headers=github_headers(await self._token()),
            number=number,
            thread_id=thread,
            marker=f"<!-- {marker} -->",
            body=self.embed(marker, body),
            app_id=self._app_id,
        )
        if written.outcome == "unavailable" or written.comment_id is None:
            raise _refusal(written.status, "comment")
        comment = MarkedComment(str(written.comment_id), target, written.body)
        return UpsertResult(comment, written=written.outcome == "written")
