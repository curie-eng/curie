"""GitLab marked status comments (#4399 decision 15).

The marker is an HTML comment on the body's last line, which GitLab does not
render. Ownership is proven by author id: only non-system notes the token's own
user wrote count, so a person pasting the marker is never found or edited.

Targets: an issue's notes, a merge request's top-level notes (individual-note
discussions), and the replies in one merge request discussion. A thread reply
is posted into its discussion and found there; one the server refused may have
landed on the merge request conversation, so a thread's marker is looked for
there too. An edit is ``PUT .../notes/:id``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from curie_api.forges import types
from curie_api.forges.capabilities import MARKED_COMMENT_OPERATIONS, Operation, Support
from curie_api.forges.errors import NotFound, Unavailable
from curie_api.forges.gitlab.transport import GitLabTransport, encode_segment, user_id
from curie_api.forges.types import MarkedComment, ReplyTarget, UpsertResult

COMMENT_CAPABILITIES: Mapping[Operation, Support] = dict.fromkeys(
    MARKED_COMMENT_OPERATIONS, Support.SUPPORTED
)


def _marker_line(marker: str) -> str:
    return f"<!-- {marker} -->"


def carries(text: str, marker: str) -> bool:
    """Whether ``text``'s last non-empty line is ``marker``'s, as `embed` writes it."""

    lines = [line for line in text.splitlines() if line.strip()]
    return bool(lines) and lines[-1].strip() == _marker_line(marker)


class GitLabMarkedComments:
    """Marked comments on one GitLab project's issues and merge requests."""

    kind = types.GITLAB
    capabilities: Mapping[Operation, Support] = COMMENT_CAPABILITIES

    def __init__(self, transport: GitLabTransport, *, host: str, project_id: str | None) -> None:
        self._transport = transport
        self.host = host
        self._project_id = project_id

    def __repr__(self) -> str:
        return f"GitLabMarkedComments(host={self.host!r}, project_id={self._project_id!r})"

    @staticmethod
    def embed(marker: str, body: str) -> str:
        """``body`` carrying ``marker`` exactly as this adapter posts it."""

        return f"{body}\n\n{_marker_line(marker)}"

    # Addressing ---------------------------------------------------------------

    def _project(self, project_id: str) -> str:
        """A target's project: numeric, and this side's project when it has one."""

        if not project_id.isdigit() or self._project_id not in (None, project_id):
            raise NotFound("project")
        return f"/projects/{project_id}"

    def _issue_path(self, target: ReplyTarget) -> str:
        issue = target.issue
        if (
            issue is None
            or issue.kind != types.GITLAB
            or issue.host != self.host
            or not issue.issue_id.isdigit()
        ):
            raise NotFound("issue")
        return f"{self._project(issue.scope_id)}/issues/{issue.issue_id}"

    def _merge_request_path(self, target: ReplyTarget) -> str:
        pull = target.pull_request
        if (
            pull is None
            or pull.repository.kind != types.GITLAB
            or pull.repository.host != self.host
            or not pull.number.isdigit()
        ):
            raise NotFound("pull_request")
        return f"{self._project(pull.repository.project_id)}/merge_requests/{pull.number}"

    # Finding -------------------------------------------------------------------

    def _own_marked(
        self, notes: list[dict[str, Any]], own: str, marker: str
    ) -> list[dict[str, Any]]:
        return sorted(
            (
                note
                for note in notes
                if user_id(note.get("author")) == own
                and note.get("system") is not True
                and type(note.get("id")) is int
                and isinstance(note.get("body"), str)
                and carries(note["body"], marker)
            ),
            key=lambda note: note["id"],
        )

    async def _find(self, target: ReplyTarget, marker: str) -> dict[str, Any] | None:
        own = (await self._transport.own_user()).id
        if target.kind == "issue":
            notes = await self._transport.list_all(f"{self._issue_path(target)}/notes")
            found = self._own_marked(notes, own, marker)
            return found[0] if found else None
        discussions = await self._transport.list_all(
            f"{self._merge_request_path(target)}/discussions"
        )
        if target.thread_id is not None:
            for discussion in discussions:
                if discussion.get("id") == target.thread_id:
                    found = self._own_marked(_notes(discussion), own, marker)
                    if found:
                        return found[0]
        top_level = [
            note
            for discussion in discussions
            if discussion.get("individual_note") is True
            for note in _notes(discussion)
        ]
        found = self._own_marked(top_level, own, marker)
        return found[0] if found else None

    async def find_marked(self, target: ReplyTarget, marker: str) -> MarkedComment | None:
        found = await self._find(target, marker)
        return None if found is None else MarkedComment(str(found["id"]), target, found["body"])

    async def upsert_marked(self, target: ReplyTarget, marker: str, body: str) -> UpsertResult:
        text = self.embed(marker, body)
        found = await self._find(target, marker)
        if found is not None and found["body"].strip() == text.strip():
            return UpsertResult(MarkedComment(str(found["id"]), target, found["body"]), False)
        base = (
            self._issue_path(target) if target.kind == "issue" else self._merge_request_path(target)
        )
        if found is not None:
            stored = await self._transport.write(
                "PUT", f"{base}/notes/{found['id']}", json={"body": text}
            )
        elif target.thread_id is not None:
            thread = encode_segment(target.thread_id)
            stored = await self._transport.write(
                "POST", f"{base}/discussions/{thread}/notes", json={"body": text}
            )
        else:
            stored = await self._transport.write("POST", f"{base}/notes", json={"body": text})
        cid = stored.get("id")
        if type(cid) is not int:
            raise Unavailable("note")
        stored_body = stored.get("body")
        comment = MarkedComment(
            str(cid), target, stored_body if isinstance(stored_body, str) else text
        )
        return UpsertResult(comment, written=True)


def _notes(discussion: Mapping[str, Any]) -> list[dict[str, Any]]:
    notes = discussion.get("notes")
    if not isinstance(notes, list):
        raise Unavailable("discussion")
    return [note for note in notes if isinstance(note, dict)]
