"""Bitbucket Cloud marked status comments (#4401 decision 12).

The marker is the comment's last line, a markdown link reference definition
that Bitbucket renders to nothing. Our own identity is the author of the pull
request the target names: an access token cannot ask Bitbucket Cloud who it
is (``GET /2.0/user`` answers 403), and the factory opens every pull request
it comments on with the same token, so a person pasting the marker is never
found or edited.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from curie_api.forges import types
from curie_api.forges.bitbucket_cloud.transport import object_of
from curie_api.forges.capabilities import MARKED_COMMENT_OPERATIONS, Operation, Support
from curie_api.forges.errors import NotFound, Unavailable
from curie_api.forges.types import MarkedComment, PullRequestRef, ReplyTarget, UpsertResult

if TYPE_CHECKING:
    from curie_api.forges.bitbucket_cloud.code_host import BitbucketCloudCodeHost, CloudComment

COMMENT_CAPABILITIES: Mapping[Operation, Support] = dict.fromkeys(
    MARKED_COMMENT_OPERATIONS, Support.SUPPORTED
)


def _marker_line(marker: str) -> str:
    return f"[//]: # ({marker})"


def carries(text: str, marker: str) -> bool:
    """Whether ``text`` ends with ``marker``'s line, as `embed` writes it."""

    lines = [line for line in text.splitlines() if line.strip()]
    return bool(lines) and lines[-1].strip() == _marker_line(marker)


class BitbucketCloudMarkedComments:
    """Marked comments on Bitbucket Cloud pull requests."""

    kind = types.BITBUCKET_CLOUD
    capabilities: Mapping[Operation, Support] = COMMENT_CAPABILITIES

    def __init__(self, code_host: BitbucketCloudCodeHost) -> None:
        self._code_host = code_host

    def __repr__(self) -> str:
        return f"BitbucketCloudMarkedComments(host={self.host!r})"

    @property
    def host(self) -> str:
        return self._code_host.host

    @staticmethod
    def embed(marker: str, body: str) -> str:
        """``body`` carrying ``marker`` exactly as this adapter posts it."""

        return f"{body}\n\n{_marker_line(marker)}"

    @staticmethod
    def _pull_request(target: ReplyTarget) -> PullRequestRef:
        if target.pull_request is None:
            raise NotFound("issue")
        return target.pull_request

    async def _find(self, target: ReplyTarget, marker: str) -> CloudComment | None:
        pull_request = self._pull_request(target)
        code_host = self._code_host
        own = await code_host.author_of(pull_request)
        listed = await code_host.list_comments(pull_request)
        mine = [
            c
            for c in listed
            if not c.deleted
            and c.author is not None
            and c.author.id == own
            and carries(c.raw, marker)
        ]
        if target.thread_id is not None:
            known = {c.id: c for c in listed}
            for comment in mine:
                if comment.id == target.thread_id:
                    continue
                if (await code_host.root_of(pull_request, comment, known)).id == target.thread_id:
                    return comment
        # A thread reply the forge refused may have landed on the conversation.
        top_level = [c for c in mine if c.parent is None and not c.inline]
        return top_level[0] if top_level else None

    async def find_marked(self, target: ReplyTarget, marker: str) -> MarkedComment | None:
        found = await self._find(target, marker)
        return None if found is None else MarkedComment(found.id, target, found.raw)

    async def upsert_marked(self, target: ReplyTarget, marker: str, body: str) -> UpsertResult:
        code_host = self._code_host
        pull_request = self._pull_request(target)
        text = self.embed(marker, body)
        found = await self._find(target, marker)
        if found is not None and found.raw == text:
            return UpsertResult(MarkedComment(found.id, target, found.raw), written=False)
        comments = f"{code_host.pull_url(pull_request)}/comments"
        content = {"content": {"raw": text}}
        if found is not None:
            response = await code_host.transport.request(
                "PUT", f"{comments}/{found.id}", what="comment", json=content
            )
        else:
            posted: dict[str, object] = dict(content)
            if target.thread_id is not None:
                if not target.thread_id.isdigit():
                    raise NotFound("thread")
                posted["parent"] = {"id": int(target.thread_id)}
            response = await code_host.transport.request(
                "POST", comments, what="comment", json=posted
            )
        stored = object_of(response, "comment")
        cid = stored.get("id")
        if type(cid) is not int:
            raise Unavailable("comment")
        raw = stored.get("content")
        stored_raw = raw.get("raw") if isinstance(raw, dict) else None
        comment = MarkedComment(
            str(cid), target, stored_raw if isinstance(stored_raw, str) else text
        )
        return UpsertResult(comment, written=True)
