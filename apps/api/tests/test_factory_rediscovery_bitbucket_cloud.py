"""Bitbucket Cloud rediscovery fails closed on an unconfirmed publication (#4747).

A Bitbucket Cloud access token cannot read its own account, so
`BitbucketCloudCodeHost.list_factory_publications` lists every publication with
``owner_verified=False``. Before #4747 rediscovery dropped all of them, so after
a loss of factory storage a reinstall admitted the issue again and opened a
second pull request. Now, on a Bitbucket Cloud repository only, an OPEN pull
request from a factory publication branch that names the issue refuses
admission as ``unconfirmed`` with one issue comment asking the operator to
confirm, and the publication route's ``before_open`` check refuses too.

Everything runs on the offline Bitbucket lifecycle harness
(`forge_fakes.bitbucket_lifecycle`) over the Bitbucket Cloud fake: admission is
the real tracker intake pass (`factory_tracker_intake.poll_trackers`, its label
poll and its sweep), and a pull request opens only through the worker's
publication routes (``POST /v1/internal/publications`` and
``POST /v1/internal/publications/{id}/pull-request``). Storage loss is the
factory's database state truncated (`_lose_storage`), while the code host and
the tracker keep what the lost install made, as after a reinstall on an empty
database. Placeholder identifiers only.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from curie_api.factory_rediscovery import EXISTING_PUBLICATION_MARKER, issue_marker
from curie_api.forges.types import ReplyTarget, TrackerIssueRef
from forge_fakes.bitbucket_cloud import BitbucketCloudFake, BitbucketCloudFakeBackend
from forge_fakes.bitbucket_lifecycle import LABEL, BitbucketPair, _work_items, bitbucket_pair
from forge_fakes.github_comments import _rows

pytestmark = pytest.mark.usefixtures("clean_db")

CONFIRM = "Please confirm whether it is the factory's pull request."
LOOKALIKE = "curie/publication-" + "b" * 32


@pytest.fixture
def fake() -> BitbucketCloudFake:
    return BitbucketCloudFake()


@pytest.fixture
def pair(
    fake: BitbucketCloudFake, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Iterator[BitbucketPair]:
    yield from bitbucket_pair(BitbucketCloudFakeBackend(fake), monkeypatch, tmp_path)


def _lose_storage() -> None:
    """The factory's database state is gone: every WorkItem, execution
    request, publication, lineage and intake cursor. The agent and its
    binding stay, as a reinstall configures them again."""

    from test_factory_ci_wait import _execute

    _execute(
        "TRUNCATE curie.publications, curie.thread_publication_lineages, "
        "curie.execution_requests, curie.work_items, curie.factory_poll_cursors CASCADE",
        {},
    )
    assert _rows("SELECT id FROM curie.agents")


def _requests() -> list[dict[str, object]]:
    return _rows("SELECT id, work_item_id FROM curie.execution_requests")


def _confirmations(pair: BitbucketPair, issue: TrackerIssueRef) -> list[str]:
    """The factory's own rediscovery comments on ``issue``."""

    return [
        body
        for author, body in pair.tracker.marked_comments.comments(ReplyTarget.on_issue(issue))
        if author == pair.tracker.identity and EXISTING_PUBLICATION_MARKER in body
    ]


def _admit(pair: BitbucketPair, issue: TrackerIssueRef) -> None:
    """The starter labels ``issue`` and the real intake pass admits it."""

    pair.tracker.apply_label(issue, LABEL, pair.starter)
    pair.intake()
    assert len(_work_items(issue)) == 1


def _published(
    pair: BitbucketPair, fake: BitbucketCloudFake, issue: TrackerIssueRef
) -> tuple[int, str]:
    """``issue``'s publication opens its own pull request through the route;
    returns its number and URL."""

    before = set(fake.pulls)
    opened = pair.open_publication(pair.publish(issue))
    assert opened.status_code == 200, opened.text
    (number,) = set(fake.pulls) - before
    assert opened.json()["number"] == number
    assert issue_marker(issue) in fake.pulls[number].description.splitlines()
    return number, str(opened.json()["url"])


# --- the regression: a reinstall never opens a second pull request ---------------------


def test_reinstall_with_an_open_publication_refuses_with_one_confirmation_comment(
    pair: BitbucketPair, fake: BitbucketCloudFake
) -> None:
    _admit(pair, pair.issue)
    pr, url = _published(pair, fake, pair.issue)

    _lose_storage()
    assert _work_items(pair.issue) == []
    pair.intake()
    pair.intake()

    assert _work_items(pair.issue) == []
    assert _requests() == []
    assert list(fake.pulls) == [pr]
    (comment,) = _confirmations(pair, pair.issue)
    assert CONFIRM in comment
    assert f"1. <{url}>" in comment.splitlines()


def test_confirming_by_removing_the_label_stops_admission_attempts(
    pair: BitbucketPair, fake: BitbucketCloudFake
) -> None:
    _admit(pair, pair.issue)
    pr, _ = _published(pair, fake, pair.issue)
    _lose_storage()
    pair.intake()

    pair.tracker.remove_label(pair.issue, LABEL, pair.starter)
    fake.set_state(pr, "MERGED")
    pair.intake()

    assert _work_items(pair.issue) == []
    assert _requests() == []
    assert list(fake.pulls) == [pr]
    assert len(_confirmations(pair, pair.issue)) == 1


def test_declining_the_unconfirmed_pull_request_admits_the_issue_on_a_later_pass(
    pair: BitbucketPair, fake: BitbucketCloudFake
) -> None:
    _admit(pair, pair.issue)
    pr, _ = _published(pair, fake, pair.issue)
    _lose_storage()
    pair.intake()
    assert _work_items(pair.issue) == []

    fake.set_state(pr, "DECLINED")
    pair.intake()

    (item,) = _work_items(pair.issue)
    assert [r["work_item_id"] for r in _requests()] == [item["id"]]


# --- what does not block ------------------------------------------------------------


def test_a_labelled_issue_with_no_publication_is_admitted_and_publishes_once(
    pair: BitbucketPair, fake: BitbucketCloudFake
) -> None:
    _admit(pair, pair.issue)
    (item,) = _work_items(pair.issue)
    assert [r["work_item_id"] for r in _requests()] == [item["id"]]

    pr, _ = _published(pair, fake, pair.issue)

    assert list(fake.pulls) == [pr]
    assert _confirmations(pair, pair.issue) == []


def test_an_open_publication_for_another_issue_does_not_block(
    pair: BitbucketPair, fake: BitbucketCloudFake
) -> None:
    other = pair.tracker.seed_issue("Gadget parser drops a quoted field", "Quoted commas split.")
    _admit(pair, other)
    theirs, _ = _published(pair, fake, other)

    _admit(pair, pair.issue)
    ours, _ = _published(pair, fake, pair.issue)

    assert sorted(fake.pulls) == sorted({theirs, ours})
    assert _confirmations(pair, pair.issue) == []


@pytest.mark.parametrize("state", ["MERGED", "DECLINED"])
def test_a_completed_publication_for_the_issue_does_not_trigger_the_refusal(
    pair: BitbucketPair, fake: BitbucketCloudFake, state: str
) -> None:
    _admit(pair, pair.issue)
    pr, _ = _published(pair, fake, pair.issue)
    fake.set_state(pr, state)
    _lose_storage()

    pair.intake()

    (item,) = _work_items(pair.issue)
    assert [r["work_item_id"] for r in _requests()] == [item["id"]]
    assert _confirmations(pair, pair.issue) == []


# --- the publication route's own check ----------------------------------------------


def test_publication_refuses_when_a_lookalike_open_pull_request_names_the_issue(
    pair: BitbucketPair, fake: BitbucketCloudFake
) -> None:
    _admit(pair, pair.issue)
    lookalike = fake.open_pull(
        LOOKALIKE, title="Widget", description=f"Adds the widget.\n\n{issue_marker(pair.issue)}\n"
    )

    refused = pair.open_publication(pair.publish(pair.issue))

    assert refused.status_code == 409, refused.text
    assert refused.json()["detail"]["code"] == "publication.publication_exists"
    assert list(fake.pulls) == [lookalike]


def test_the_lineage_branchs_own_pull_request_is_adopted_beside_a_lookalike(
    pair: BitbucketPair, fake: BitbucketCloudFake
) -> None:
    _admit(pair, pair.issue)
    publication = pair.publish(pair.issue)
    opened = pair.open_publication(publication)
    assert opened.status_code == 200, opened.text
    lookalike = fake.open_pull(
        LOOKALIKE, title="Widget", description=f"Adds the widget.\n\n{issue_marker(pair.issue)}\n"
    )

    adopted = pair.open_publication(publication)

    assert adopted.status_code == 200, adopted.text
    assert adopted.json()["number"] == opened.json()["number"]
    assert sorted(fake.pulls) == sorted({opened.json()["number"], lookalike})
