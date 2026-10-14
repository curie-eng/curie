"""GitLab review feedback (#4399 decision 20).

Feedback is every non-system note in a merge request's discussions by a person
other than the token's own user. A note in a discussion with
``individual_note: false`` (a thread, on the diff or not) is a
``REVIEW_COMMENT`` whose ``thread_id`` is the discussion id; an individual note
is a ``COMMENT``. GitLab has no review with a body, so there is no ``REVIEW``
feedback. The cursor is the largest note id seen: note ids grow, so a later
reply in an old thread is still listed once.
"""

from __future__ import annotations

from typing import Any

from curie_api.forges.errors import NotFound, Unavailable
from curie_api.forges.gitlab.transport import GitLabTransport, actor_of
from curie_api.forges.types import (
    FeedbackKind,
    FeedbackPage,
    PullRequestRef,
    ReviewFeedback,
)


def _cursor(cursor: str | None) -> int:
    if not cursor:
        return 0
    if not cursor.isdigit():
        raise ValueError("a gitlab feedback cursor is a note id")
    return int(cursor)


async def list_review_feedback(
    transport: GitLabTransport, path: str, pull_request: PullRequestRef, cursor: str | None
) -> FeedbackPage:
    """Human notes newer than ``cursor`` on the merge request at ``path``. A
    partial listing raises `Unavailable` and yields no cursor."""

    after = _cursor(cursor)
    merge_request = await transport.get_object(path)
    head_sha = merge_request.get("sha")
    if not isinstance(head_sha, str):
        raise Unavailable("merge_request")
    discussions = await transport.list_all(f"{path}/discussions")
    newest = after
    found: list[tuple[int, ReviewFeedback]] = []
    for discussion in discussions:
        notes = discussion.get("notes")
        thread = discussion.get("id")
        if not isinstance(notes, list) or not isinstance(thread, str):
            raise Unavailable("discussions")
        in_thread = discussion.get("individual_note") is False
        for note in notes:
            note_id = note.get("id") if isinstance(note, dict) else None
            if type(note_id) is not int:
                continue
            newest = max(newest, note_id)
            body = note.get("body")
            if note_id <= after or note.get("system") is True or not isinstance(body, str):
                continue
            author = await transport.is_person(note.get("author"))
            if author is None:
                continue
            found.append(
                (
                    note_id,
                    ReviewFeedback(
                        id=str(note_id),
                        author=author,
                        body=body,
                        pull_request=pull_request,
                        head_sha=head_sha,
                        kind=FeedbackKind.REVIEW_COMMENT if in_thread else FeedbackKind.COMMENT,
                        thread_id=thread if in_thread else None,
                    ),
                )
            )
    found.sort(key=lambda item: item[0])
    return FeedbackPage(tuple(item for _, item in found), str(newest))


async def verify_feedback(transport: GitLabTransport, path: str, feedback: ReviewFeedback) -> bool:
    """The note still exists with the same body and author on an open merge request."""

    if not feedback.id.isdigit():
        return False
    try:
        merge_request = await transport.get_object(path)
        if merge_request.get("state") != "opened":
            return False
        note: dict[str, Any] = await transport.get_object(f"{path}/notes/{feedback.id}")
    except NotFound:
        return False
    return (
        note.get("system") is not True
        and actor_of(note.get("author")) == feedback.author
        and note.get("body") == feedback.body
    )
