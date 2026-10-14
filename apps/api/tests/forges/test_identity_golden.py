"""GitHub identities stay byte for byte across the port (ADR 0197 identity rule 4).

Every literal below was recorded once from the legacy GitHub derivations at
origin/next 08683759d (moved, unchanged, into ``curie_api.forges.github`` by
the first #3831 commit). They must never change: request ids, feedback event
ids and lock keys written before the port are matched against them. Each test
asserts that the legacy derivation and the forge-neutral derivation from typed
references both equal the recorded value.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from curie_api.forges.github.identity import issue_lock_keys_for, label_event_delivery_id
from curie_api.forges.identity import (
    feedback_event_id,
    issue_lock_keys,
    notice_request_id,
    reconcile_delivery_id,
    revision_request_id,
)
from curie_api.forges.types import (
    GITHUB,
    Actor,
    Disposition,
    FeedbackKind,
    MarkedNotice,
    RepositoryRef,
    TrackerIssueRef,
)
from curie_api.github_factory_events import FactoryNotice
from curie_api.github_review_events import UnverifiedFeedback

REPOSITORY_ID = 4401
ISSUE_NUMBER = 12
LABEL_EVENT_ID = 987654321
COMMENT_ID = 2233445566
FEEDBACK_ID = 3344556677
DELIVERY = uuid.UUID("2b1c6a3e-5f4d-4c2b-9a8e-7d6c5b4a3f21")

ISSUE = TrackerIssueRef(GITHUB, "github.com", str(REPOSITORY_ID), str(ISSUE_NUMBER), "#12")
REPOSITORY = RepositoryRef(GITHUB, "github.com", str(REPOSITORY_ID), "acme-corp/acme-bot")
SENDER = Actor("6601", "octocat")

# Recorded from origin/next 08683759d. Never edit.
GOLDEN_LABEL_EVENT_REQUEST = "0632a3d4-7d07-54f5-8eb0-790f63041ca1"
GOLDEN_LABEL_DELIVERY_REQUEST = "2a68b82d-ab6b-5593-a5a2-d2525abc830d"
GOLDEN_MENTION_REQUEST = "1cc218d8-84d8-533e-86ab-ef1118409951"
GOLDEN_RECONCILE_DELIVERY = "d47e61e7-bca7-5500-96c8-8b50cee7e27a"
GOLDEN_FEEDBACK = {
    FeedbackKind.COMMENT: (
        "issue_comment",
        "github-feedback-f4abaf11-840e-582b-83b6-7b077215b8cb",
        "a582b77b-21a8-52c3-b7fa-27070e8bded8",
    ),
    FeedbackKind.REVIEW_COMMENT: (
        "pull_request_review_comment",
        "github-feedback-9cb26b67-4f31-57ce-94c0-2a0f9ae97d09",
        "9f8cddc9-00a7-5918-b14b-9599126c3419",
    ),
    FeedbackKind.REVIEW: (
        "pull_request_review",
        "github-feedback-f57c227e-c607-5a5d-ae4b-f4eecb583a22",
        "95ea378c-e096-55cc-b6be-b2941aef5f88",
    ),
}
GOLDEN_LOCK_KEYS = {(4401, 12): (-1010687396, -691935689), (1, 1): (-1180730903, -751862053)}


def _legacy_notice(**changes: object) -> FactoryNotice:
    fields: dict[str, object] = {
        "delivery_id": DELIVERY,
        "event": "issues",
        "action": "labeled",
        "disposition": "admit",
        "installation_id": 5501,
        "repository_id": REPOSITORY_ID,
        "repo_full_name": "acme-corp/acme-bot",
        "issue_number": ISSUE_NUMBER,
        "sender_id": 6601,
        "sender_login": "octocat",
        "label": "factory",
    }
    fields.update(changes)
    return FactoryNotice(**fields)  # type: ignore[arg-type]


def _notice(disposition: Disposition, event_id: str) -> MarkedNotice:
    return MarkedNotice(ISSUE, "factory", SENDER, event_id, disposition, cursor="1")


def test_label_admission_with_a_timeline_event() -> None:
    legacy = _legacy_notice(label_event_id=LABEL_EVENT_ID).request_id
    new = notice_request_id(_notice(Disposition.ADMIT, str(LABEL_EVENT_ID)))
    assert str(legacy) == str(new) == GOLDEN_LABEL_EVENT_REQUEST


def test_label_admission_without_a_timeline_event_uses_the_delivery() -> None:
    legacy = _legacy_notice().request_id
    new = notice_request_id(_notice(Disposition.ADMIT, str(DELIVERY)))
    assert str(legacy) == str(new) == GOLDEN_LABEL_DELIVERY_REQUEST


def test_cancellation_shares_the_label_namespace() -> None:
    legacy = _legacy_notice(action="unlabeled", disposition="cancel").request_id
    new = notice_request_id(_notice(Disposition.CANCEL, str(DELIVERY)))
    assert str(legacy) == str(new) == GOLDEN_LABEL_DELIVERY_REQUEST


def test_mention() -> None:
    legacy = _legacy_notice(
        event="issue_comment",
        action="created",
        disposition="mention",
        label=None,
        comment_id=COMMENT_ID,
    ).request_id
    new = notice_request_id(_notice(Disposition.MENTION, str(COMMENT_ID)))
    assert str(legacy) == str(new) == GOLDEN_MENTION_REQUEST


@pytest.mark.parametrize("kind", list(FeedbackKind))
def test_feedback_event_id_and_revision_request_id(kind: FeedbackKind) -> None:
    event, golden_event_id, golden_request = GOLDEN_FEEDBACK[kind]
    legacy = UnverifiedFeedback(
        delivery_id=DELIVERY,
        event=event,
        installation_id=5501,
        repository_id=REPOSITORY_ID,
        repo_full_name="acme-corp/acme-bot",
        pr_number=34,
        feedback_id=FEEDBACK_ID,
        sender_id=6601,
        sender_login="octocat",
        body="Please rename it.",
        url="https://github.com/acme-corp/acme-bot/pull/34",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        head_sha=None,
        commit_sha=None,
        author_association="MEMBER",
    ).event_id
    assert legacy == feedback_event_id(REPOSITORY, kind, str(FEEDBACK_ID)) == golden_event_id
    # github_factory_review.py derives the revision request id inline this way.
    legacy_request = uuid.uuid5(uuid.NAMESPACE_URL, legacy)
    new_request = revision_request_id(REPOSITORY, kind, str(FEEDBACK_ID))
    assert str(legacy_request) == str(new_request) == golden_request


def test_reconcile_label_event_delivery_id() -> None:
    legacy = label_event_delivery_id(REPOSITORY_ID, ISSUE_NUMBER, LABEL_EVENT_ID)
    new = reconcile_delivery_id(ISSUE, str(LABEL_EVENT_ID))
    assert str(legacy) == str(new) == GOLDEN_RECONCILE_DELIVERY


@pytest.mark.parametrize(("repository_id", "issue_number"), sorted(GOLDEN_LOCK_KEYS))
def test_issue_lock_keys(repository_id: int, issue_number: int) -> None:
    issue = TrackerIssueRef(GITHUB, "github.com", str(repository_id), str(issue_number))
    golden = GOLDEN_LOCK_KEYS[(repository_id, issue_number)]
    assert issue_lock_keys_for(repository_id, issue_number) == issue_lock_keys(issue) == golden


def test_the_display_key_and_host_take_no_part_in_a_github_identity() -> None:
    moved = TrackerIssueRef(GITHUB, "github.enterprise.example", "4401", "12", "other")
    assert issue_lock_keys(moved) == GOLDEN_LOCK_KEYS[(4401, 12)]


def test_other_kinds_derive_distinct_identities_from_the_full_key() -> None:
    gitlab = TrackerIssueRef("gitlab", "gitlab.example", "4401", "12")
    other_host = TrackerIssueRef("gitlab", "gitlab.other.example", "4401", "12")
    assert issue_lock_keys(gitlab) not in {
        issue_lock_keys(other_host),
        GOLDEN_LOCK_KEYS[(4401, 12)],
    }
    admit = MarkedNotice(gitlab, "factory", SENDER, str(LABEL_EVENT_ID), Disposition.ADMIT, "1")
    assert str(notice_request_id(admit)) != GOLDEN_LABEL_EVENT_REQUEST
    assert notice_request_id(admit) == notice_request_id(admit)


@pytest.mark.parametrize("bad", ["012", "-4", "4401a", "", "٤٤"])
def test_a_github_identity_refuses_a_non_canonical_number(bad: str) -> None:
    with pytest.raises(ValueError):
        issue_lock_keys(TrackerIssueRef(GITHUB, "github.com", bad or "x", "12"))
