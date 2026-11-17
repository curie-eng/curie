"""Every pull-request follow-up objective is detected by the review gate.

The gate cannot import the API, so it keeps its own copy of the marker. This
builds each objective with the production function that admits that follow-up,
GitHub review feedback, other-forge review feedback, and Slack steering, frames
it the way the runner does, and checks the gate's own detector. A hardcoded
phrase would stay green when a builder drifted.
"""

from __future__ import annotations

import runpy
import uuid
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from curie_api.factory_follow_up import PULL_REQUEST_FOLLOW_UP_MARKER
from curie_api.factory_review_feedback import _objective as forge_objective
from curie_api.factory_slack import objective as slack_objective
from curie_api.forges.types import (
    Actor,
    FeedbackKind,
    PullRequestRef,
    RepositoryRef,
    ReviewFeedback,
)
from curie_api.github_factory_review import _objective as github_objective
from curie_api.github_review_events import UnverifiedFeedback
from curie_runner.sender_frame import frame_user_turn

HOOK = Path(__file__).resolve().parents[1] / "dark-factory" / "hooks" / "review_gate.py"
_GITHUB_URL = "https://github.com/acme/bot/pull/7#discussion_r1"
_FORGE_URL = "https://gitlab.example/acme/bot/-/merge_requests/4#note_1"
_SLACK_URL = "https://github.com/acme/bot/pull/7"
_OLD_PHRASE = (
    f"{_GITHUB_URL}\n\n"
    "Human GitHub review feedback on this work item's pull request follows as JSON. "
    "Answer from the pull request body's Decisions section and the current diff first.\n"
    '{"body":"why this approach?"}'
)


def production_follow_up_objectives() -> dict[str, str]:
    """Each pull-request follow-up objective, from the function that admits it."""

    feedback = UnverifiedFeedback(
        delivery_id=uuid.UUID("00000000-0000-4000-8000-000000000001"),
        event="pull_request_review_comment",
        installation_id=1,
        repository_id=2,
        repo_full_name="acme/bot",
        pr_number=7,
        feedback_id=1,
        sender_id=3,
        sender_login="octocat",
        body="why this approach?",
        url=_GITHUB_URL,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        head_sha="abc123",
        commit_sha="abc123",
        author_association="MEMBER",
    )
    forge_feedback = ReviewFeedback(
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
    return {
        "github": github_objective(
            feedback, _GITHUB_URL, {"pull_request_body": "## Decisions\nNone."}
        ),
        "forge": forge_objective(
            forge_feedback,
            _FORGE_URL,
            SimpleNamespace(kind="gitlab"),
            {"pull_request_body": "## Decisions\nNone."},
        ),
        "slack": slack_objective(_SLACK_URL, "fix/example", "octocat", "why this approach?"),
    }


def _gate() -> dict[str, Any]:
    return runpy.run_path(str(HOOK))


def _framed(text: str, user: str = "octocat") -> str:
    return frame_user_turn("message", user, text, "github")


def test_the_gate_marker_matches_the_api_constant() -> None:
    assert _gate()["_PULL_REQUEST_FOLLOW_UP_MARKER"] == PULL_REQUEST_FOLLOW_UP_MARKER


@pytest.mark.parametrize("name", ["github", "forge", "slack"])
def test_each_pull_request_follow_up_builder_is_detected(name: str) -> None:
    objective = production_follow_up_objectives()[name]
    lines = objective.splitlines()
    assert lines[0].startswith("https://")
    assert lines[1] == ""
    assert lines[2] == PULL_REQUEST_FOLLOW_UP_MARKER
    assert _gate()["_pull_request_follow_up"](_framed(objective)) is True


def test_the_old_review_phrase_without_the_marker_is_not_a_follow_up() -> None:
    assert PULL_REQUEST_FOLLOW_UP_MARKER not in _OLD_PHRASE
    assert _gate()["_pull_request_follow_up"](_framed(_OLD_PHRASE)) is False


def test_a_marker_only_in_the_sender_frame_header_is_not_a_follow_up() -> None:
    body = f"{_SLACK_URL}\n\nwhy this approach?"
    framed = _framed(body, user=PULL_REQUEST_FOLLOW_UP_MARKER)
    assert PULL_REQUEST_FOLLOW_UP_MARKER in framed
    assert _gate()["_pull_request_follow_up"](framed) is False
