"""Request ids, feedback event ids and issue lock keys from typed references.

ADR 0197 identity rule 4: GitHub keeps its existing derivations byte for byte,
so rows and advisory locks written before the port still match. Every other
kind derives from the full (kind, host, scope, id) identity under a
``curie-forge`` URL, which cannot collide with a GitHub derivation. A new forge
therefore needs no edit here.

The golden values both schemes must keep are pinned in
``apps/api/tests/forges/test_identity_golden.py``.
"""

from __future__ import annotations

import hashlib
import uuid
from urllib.parse import quote

from curie_api.forges.github import identity as github_identity
from curie_api.forges.types import (
    GITHUB,
    Disposition,
    FeedbackKind,
    MarkedNotice,
    RepositoryRef,
    ReviewFeedback,
    TrackerIssueRef,
)

# The GitHub webhook event each feedback kind arrived as; the feedback event id
# has always been keyed on the event name.
_GITHUB_FEEDBACK_EVENTS = {
    FeedbackKind.COMMENT: "issue_comment",
    FeedbackKind.REVIEW_COMMENT: "pull_request_review_comment",
    FeedbackKind.REVIEW: "pull_request_review",
}


def _github_number(value: str, name: str) -> int:
    if not value.isascii() or not value.isdigit() or value.startswith("0"):
        raise ValueError(f"a GitHub {name} is a positive decimal integer")
    return int(value)


def _segments(*parts: str) -> str:
    return "/".join(quote(part, safe="") for part in parts)


def _generic_url(namespace: str, *parts: str) -> str:
    return f"curie-forge://{namespace}/{_segments(*parts)}"


def notice_request_id(notice: MarkedNotice) -> uuid.UUID:
    """The execution request id one marked notice admits or cancels under.

    A mention is keyed on its comment; an admission or cancellation on the
    issue and the label event (or, for a webhook without a timeline event, the
    delivery id), so a relabel is a new request and a redelivery is not.
    """

    issue = notice.issue
    namespace = "mention" if notice.disposition is Disposition.MENTION else "label"
    if issue.kind == GITHUB:
        repository_id = _github_number(issue.scope_id, "repository id")
        if notice.disposition is Disposition.MENTION:
            identity = f"https://github.com/factory/mention/{repository_id}/{notice.event_id}"
        else:
            issue_number = _github_number(issue.issue_id, "issue number")
            identity = (
                f"https://github.com/factory/label/{repository_id}/{issue_number}/{notice.event_id}"
            )
        return uuid.uuid5(uuid.NAMESPACE_URL, identity)
    parts: tuple[str, ...]
    if notice.disposition is Disposition.MENTION:
        parts = (issue.kind, issue.host, issue.scope_id, notice.event_id)
    else:
        parts = (issue.kind, issue.host, issue.scope_id, issue.issue_id, notice.event_id)
    return uuid.uuid5(uuid.NAMESPACE_URL, _generic_url(f"factory/{namespace}", *parts))


def reconcile_delivery_id(issue: TrackerIssueRef, event_id: str) -> uuid.UUID:
    """A stable stand-in delivery id for one polled label event."""

    if issue.kind == GITHUB:
        return github_identity.label_event_delivery_id(
            _github_number(issue.scope_id, "repository id"),
            _github_number(issue.issue_id, "issue number"),
            _github_number(event_id, "event id"),
        )
    return uuid.uuid5(
        uuid.NAMESPACE_URL,
        _generic_url(
            "factory/reconcile", issue.kind, issue.host, issue.scope_id, issue.issue_id, event_id
        ),
    )


def feedback_event_id(repository: RepositoryRef, kind: FeedbackKind, feedback_id: str) -> str:
    """The idempotency key of one piece of review feedback.

    Installation or credential identity is provenance, not comment identity,
    so it never takes part: reinstalling must not replay an old comment.
    """

    if repository.kind == GITHUB:
        repository_id = _github_number(repository.project_id, "repository id")
        number = _github_number(feedback_id, "feedback id")
        identity = f"{repository_id}:{_GITHUB_FEEDBACK_EVENTS[kind]}:{number}"
        return f"github-feedback-{uuid.uuid5(uuid.NAMESPACE_URL, identity)}"
    url = _generic_url(
        "feedback", repository.kind, repository.host, repository.project_id, kind, feedback_id
    )
    return f"{repository.kind}-feedback-{uuid.uuid5(uuid.NAMESPACE_URL, url)}"


def feedback_event_id_of(feedback: ReviewFeedback) -> str:
    return feedback_event_id(feedback.pull_request.repository, feedback.kind, feedback.id)


def revision_request_id(
    repository: RepositoryRef, kind: FeedbackKind, feedback_id: str
) -> uuid.UUID:
    """The execution request id a revision asked for by this feedback runs under."""

    return uuid.uuid5(uuid.NAMESPACE_URL, feedback_event_id(repository, kind, feedback_id))


def issue_lock_keys(issue: TrackerIssueRef) -> tuple[int, int]:
    """The two-int advisory lock key serializing work on one tracker issue."""

    if issue.kind == GITHUB:
        return github_identity.issue_lock_keys_for(
            _github_number(issue.scope_id, "repository id"),
            _github_number(issue.issue_id, "issue number"),
        )
    material = f"curie-factory:{_segments(issue.kind, issue.host, issue.scope_id, issue.issue_id)}"
    digest = hashlib.sha256(material.encode()).digest()
    return (
        int.from_bytes(digest[:4], "big", signed=True),
        int.from_bytes(digest[4:8], "big", signed=True),
    )
