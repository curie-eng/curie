"""Bitbucket Data Center marked status comments (#4401 decision 12).

The marker is the comment's last line, a markdown link reference definition
that Bitbucket renders to nothing. Our own identity is the author of the pull
request the target names: the factory opens every pull request it comments on
with the same token, so a person pasting the marker is never found or edited.
An edit sends the comment's version, as Data Center requires.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from curie_api.forges import types
from curie_api.forges.bitbucket_dc.transport import object_of
from curie_api.forges.capabilities import MARKED_COMMENT_OPERATIONS, Operation, Support
from curie_api.forges.errors import NotFound, Unavailable
from curie_api.forges.types import MarkedComment, PullRequestRef, ReplyTarget, UpsertResult

if TYPE_CHECKING:
    from curie_api.forges.bitbucket_dc.code_host import BitbucketDcCodeHost, ThreadComment

COMMENT_CAPABILITIES: Mapping[Operation, Support] = dict.fromkeys(
    MARKED_COMMENT_OPERATIONS, Support.SUPPORTED
)


def _marker_line(marker: str) -> str:
    return f"[//]: # ({marker})"


def carries(text: str, marker: str) -> bool:
    """Whether ``text`` ends with ``marker``'s line, as `embed` writes it."""

    lines = [line for line in text.splitlines() if line.strip()]
    return bool(lines) and lines[-1].strip() == _marker_line(marker)


class BitbucketDcMarkedComments:
    """Marked comments on the pull requests of one Bitbucket Data Center."""

    kind = types.BITBUCKET_DC
    capabilities: Mapping[Operation, Support] = COMMENT_CAPABILITIES

    def __init__(self, code_host: BitbucketDcCodeHost) -> None:
        self._code_host = code_host

    def __repr__(self) -> str:
        return f"BitbucketDcMarkedComments(host={self.host!r})"

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

    async def _find(self, target: ReplyTarget, marker: str) -> ThreadComment | None:
        pull_request = self._pull_request(target)
        code_host = self._code_host
        own = (await code_host.read_pull(pull_request)).author_id
        tree = await code_host.comment_tree(pull_request)
        mine = sorted(
            (c for c in tree.comments.values() if c.author.id == own and carries(c.text, marker)),
            key=lambda c: (c.created, int(c.id)),
        )
        if target.thread_id is not None:
            in_thread = [
                c
                for c in mine
                if c.id != target.thread_id and tree.root_of(c).id == target.thread_id
            ]
            if in_thread:
                return in_thread[0]
        # A thread reply the server refused may have landed on the conversation.
        top_level = [c for c in mine if c.parent is None and not c.anchored]
        return top_level[0] if top_level else None

    async def find_marked(self, target: ReplyTarget, marker: str) -> MarkedComment | None:
        await self._code_host.ensure_version(Operation.FIND_MARKED)
        found = await self._find(target, marker)
        return None if found is None else MarkedComment(found.id, target, found.text)

    async def upsert_marked(self, target: ReplyTarget, marker: str, body: str) -> UpsertResult:
        code_host = self._code_host
        await code_host.ensure_version(Operation.UPSERT_MARKED)
        pull_request = self._pull_request(target)
        text = self.embed(marker, body)
        found = await self._find(target, marker)
        if found is not None and found.text == text:
            return UpsertResult(MarkedComment(found.id, target, found.text), written=False)
        comments = f"{code_host.pull_path(pull_request)}/comments"
        if found is not None:
            response = await code_host.transport.request(
                "PUT",
                f"{comments}/{found.id}",
                what="comment",
                json={"text": text, "version": found.version},
                allow=(409,),
            )
            if response.status_code == 409:
                raise Unavailable("stale_version")
        else:
            posted: dict[str, object] = {"text": text}
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
        stored_text = stored.get("text")
        comment = MarkedComment(
            str(cid), target, stored_text if isinstance(stored_text, str) else text
        )
        return UpsertResult(comment, written=True)
