"""The non-GitHub follow-up objective answers from the pull request before it revises."""

from __future__ import annotations

import json
from types import SimpleNamespace

from curie_api.factory_review_feedback import _objective
from curie_api.forges.types import (
    Actor,
    FeedbackKind,
    PullRequestRef,
    RepositoryRef,
    ReviewFeedback,
)


def test_feedback_objective_answers_from_decisions_before_a_revision() -> None:
    feedback = ReviewFeedback(
        id="note-1",
        author=Actor(id="7", login="octocat"),
        body="why this approach?",
        pull_request=PullRequestRef(
            RepositoryRef(kind="gitlab", host="gitlab.example", project_id="9", path="acme/bot"),
            "4",
        ),
        head_sha="abc123",
        kind=FeedbackKind.COMMENT,
    )
    objective = _objective(
        feedback,
        "https://gitlab.example/acme/bot/-/merge_requests/4#note_1",
        SimpleNamespace(kind="gitlab"),
        {"pull_request_body": "## Decisions\n1. Use the existing parser."},
    )

    prose, _separator, last_line = objective.rpartition("\n")
    assert (
        "Answer from the pull request body's Decisions section and the current diff first." in prose
    )
    assert "Start the revision pipeline only when the comment asks for a change." in prose
    assert "The feedback may ask a question or request a change." in prose
    provenance = json.loads(last_line)
    assert provenance["body"] == "why this approach?"
    assert provenance["pull_request_body"] == "## Decisions\n1. Use the existing parser."
    assert provenance["kind"] == "comment"
    assert "review feedback on this work item's pull request" in prose
