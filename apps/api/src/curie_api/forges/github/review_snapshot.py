"""One submitted GitHub review, read whole with the factory App's token (#5794).

GitHub creates the inline comments of a review in a pending review before the
reviewer submits it, then delivers the review and each comment as separate
events. The factory admits review feedback once per submitted review: it reads
the review, refuses it until it is submitted, then lists every comment of the
review, so the objective holds the whole batch whichever piece arrived first.

Shapes follow GitHub's REST documentation:
https://docs.github.com/en/rest/pulls/reviews#get-a-review-for-a-pull-request
https://docs.github.com/en/rest/pulls/reviews#list-comments-for-a-pull-request-review
A pending review is visible only to its author, so the App's read is a 404.
Every reader here raises only `FeedbackIgnored` or `FeedbackUnavailable`.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import httpx

from curie_api.github_review_events import FeedbackIgnored, FeedbackUnavailable, feedback_event_id
from curie_api.github_review_truth import VerifiedRepository

from .transport import github_headers, parse_time

ACTIONABLE_STATES = frozenset({"COMMENTED", "CHANGES_REQUESTED"})
NON_ACTIONABLE_STATES = frozenset({"APPROVED", "DISMISSED"})
# Submitting a pending review bumps each inline comment's updated_at to the
# submission time without an edit (#2794). A comment updated later than this
# allowance after submission was edited after the reviewer submitted it.
SUBMISSION_SKEW = timedelta(seconds=5)
_PER_PAGE = 100
# A review with more comments than these pages hold is not trusted to be whole.
_MAX_PAGES = 10
_SHA = re.compile(r"[0-9a-fA-F]{40}")


def review_request_id(repository_id: int, pr_number: int, review_id: int) -> uuid.UUID:
    """The one request id every piece of one submitted review admits under."""

    return uuid.uuid5(uuid.NAMESPACE_URL, f"curie-review:{repository_id}:{pr_number}:{review_id}")


def event_request_id(repository_id: int, event: str, feedback_id: int) -> uuid.UUID:
    """The per-event request id one piece of feedback admits under on its own.

    Before #5794 every review and inline comment that mentioned the factory was
    admitted under this id, so a review answered then names no review key.
    """

    return uuid.uuid5(uuid.NAMESPACE_URL, feedback_event_id(repository_id, event, feedback_id))


@dataclass(frozen=True)
class ReviewComment:
    """One inline comment of a submitted review, as the App listed it."""

    id: int
    path: str
    line: int | None
    body: str


@dataclass(frozen=True)
class SubmittedReview:
    """A review GitHub reports as submitted; ``state`` is upper case."""

    id: int
    state: str
    author_id: int
    body: str
    submitted_at: datetime
    commit_id: str

    @property
    def actionable(self) -> bool:
        return self.state in ACTIONABLE_STATES


def _positive(value: Any) -> int | None:
    if type(value) is not int or value <= 0:
        return None
    return value


def _user_id(value: Any) -> int | None:
    return _positive(value.get("id")) if isinstance(value, dict) else None


def _pull_url(verified: VerifiedRepository, repo_path: str, pr_number: int) -> str:
    return f"{verified.api}{repo_path}/pulls/{pr_number}"


async def _get(
    client: httpx.AsyncClient, url: str, token: str, params: dict[str, Any] | None, code: str
) -> httpx.Response:
    try:
        return await client.get(
            url, params=params, headers=github_headers(token), follow_redirects=False
        )
    except httpx.HTTPError:
        raise FeedbackUnavailable(code) from None


async def read_submitted_review(
    client: httpx.AsyncClient,
    verified: VerifiedRepository,
    *,
    repo_path: str,
    pr_number: int,
    review_id: int,
    author_id: int,
) -> SubmittedReview:
    """The review, or ``review_not_submitted`` while the App cannot see it submitted.

    A 404 is a pending review (visible only to its author) and is refused with
    no receipt, so the submitted review's own event or a later poll decides.
    """

    response = await _get(
        client,
        f"{_pull_url(verified, repo_path, pr_number)}/reviews/{review_id}",
        verified.token,
        None,
        "review_unavailable",
    )
    if response.status_code == 404:
        raise FeedbackIgnored("review_not_submitted")
    if response.status_code in {401, 403, 429} or response.status_code >= 500:
        raise FeedbackUnavailable("review_unavailable")
    if response.status_code != 200:
        raise FeedbackIgnored("review_unavailable")
    try:
        review = response.json()
    except ValueError:
        raise FeedbackIgnored("review_unavailable") from None
    if not isinstance(review, dict):
        raise FeedbackIgnored("review_unavailable")
    if (
        review.get("id") != review_id
        or _user_id(review.get("user")) != author_id
        or review.get("pull_request_url") != _pull_url(verified, repo_path, pr_number)
    ):
        raise FeedbackIgnored("feedback_target_mismatch")
    state = review.get("state")
    if not isinstance(state, str):
        raise FeedbackIgnored("review_unavailable")
    state = state.upper()
    submitted_at = parse_time(review.get("submitted_at"))
    if state == "PENDING" or submitted_at is None:
        raise FeedbackIgnored("review_not_submitted")
    if state not in ACTIONABLE_STATES | NON_ACTIONABLE_STATES:
        raise FeedbackIgnored("non_actionable_review")
    body = review.get("body")
    if body is None:
        body = ""
    commit_id = review.get("commit_id")
    if not isinstance(body, str) or not isinstance(commit_id, str) or not _SHA.fullmatch(commit_id):
        raise FeedbackIgnored("review_unavailable")
    # A review submitted with no text has an empty body; its comments carry it.
    return SubmittedReview(
        review_id,
        state,
        author_id,
        body if body.strip() else "",
        submitted_at,
        commit_id.lower(),
    )


def _comment(
    item: Any,
    review: SubmittedReview,
    *,
    pull_url: str,
    head_sha: str,
) -> ReviewComment:
    if not isinstance(item, dict):
        raise FeedbackIgnored("review_unavailable")
    if (
        item.get("pull_request_review_id") != review.id
        or item.get("pull_request_url") != pull_url
        or _user_id(item.get("user")) != review.author_id
    ):
        raise FeedbackIgnored("feedback_target_mismatch")
    comment_id = _positive(item.get("id"))
    body, path, line = item.get("body"), item.get("path"), item.get("line")
    commit_id = item.get("commit_id")
    if (
        comment_id is None
        or not isinstance(body, str)
        or not isinstance(path, str)
        or not path
        or (line is not None and _positive(line) is None)
        or not isinstance(commit_id, str)
    ):
        raise FeedbackIgnored("review_unavailable")
    if commit_id.lower() != head_sha:
        raise FeedbackIgnored("stale_feedback_head")
    updated_at = parse_time(item.get("updated_at"))
    if updated_at is None:
        raise FeedbackIgnored("invalid_feedback_time")
    if updated_at > review.submitted_at + SUBMISSION_SKEW:
        raise FeedbackIgnored("edited_feedback")
    return ReviewComment(comment_id, path, line, body)


async def _review_items(
    client: httpx.AsyncClient, verified: VerifiedRepository, *, pull_url: str, review_id: int
) -> list[Any]:
    """Every item of the review's comment listing, unchecked."""

    items: list[Any] = []
    for page in range(1, _MAX_PAGES + 1):
        response = await _get(
            client,
            f"{pull_url}/reviews/{review_id}/comments",
            verified.token,
            {"per_page": _PER_PAGE, "page": page},
            "review_comments_unavailable",
        )
        if response.status_code != 200:
            raise FeedbackUnavailable("review_comments_unavailable")
        try:
            listed = response.json()
        except ValueError:
            raise FeedbackUnavailable("review_comments_unavailable") from None
        if not isinstance(listed, list):
            raise FeedbackUnavailable("review_comments_unavailable")
        items.extend(listed)
        if len(listed) < _PER_PAGE:
            return items
    raise FeedbackIgnored("feedback_too_large")


async def read_review_comments(
    client: httpx.AsyncClient,
    verified: VerifiedRepository,
    *,
    repo_path: str,
    pr_number: int,
    review: SubmittedReview,
    head_sha: str,
) -> tuple[ReviewComment, ...]:
    """Every comment of ``review``, in id order, or a refusal for the whole review.

    Each comment must belong to this review and pull request, be written by the
    review's author on ``head_sha``, and be unedited since submission.
    """

    pull_url = _pull_url(verified, repo_path, pr_number)
    items = await _review_items(client, verified, pull_url=pull_url, review_id=review.id)
    comments = [_comment(item, review, pull_url=pull_url, head_sha=head_sha) for item in items]
    return tuple(sorted(comments, key=lambda comment: comment.id))


async def read_review_comment(
    client: httpx.AsyncClient,
    verified: VerifiedRepository,
    *,
    repo_path: str,
    pr_number: int,
    review: SubmittedReview,
    comment_id: int,
    head_sha: str,
) -> ReviewComment:
    """Comment ``comment_id`` of ``review``, held to the same checks alone.

    An inline comment admitted on its own (a mention in an approved review) is
    refused ``edited_feedback`` when it was edited after submission; the other
    comments of its review are not its feedback.
    """

    pull_url = _pull_url(verified, repo_path, pr_number)
    for item in await _review_items(client, verified, pull_url=pull_url, review_id=review.id):
        if isinstance(item, dict) and item.get("id") == comment_id:
            return _comment(item, review, pull_url=pull_url, head_sha=head_sha)
    raise FeedbackUnavailable("review_comments_unavailable")
