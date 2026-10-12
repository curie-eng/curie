"""Running Jira issues are re-read and cancelled (#4358 AC 4, decision 12).

For each running issue `poll_marked` reads the issue: the label gone gives one
CANCEL carrying the id of the changelog item that removed it; the status
category ``done`` gives one CANCEL carrying the status change's id; a 404 gives
nothing, as on GitHub. `verify_current` re-reads the issue for every
disposition, and a relabel is a new admission with a new request id.
"""

from __future__ import annotations

import pytest
from curie_api.forges.identity import notice_request_id
from curie_api.forges.types import Disposition, MarkedNotice, NoticePage
from forge_fakes.jira import LABEL, JiraFake, JiraUser, jira_tracker


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _human(fake: JiraFake) -> JiraUser:
    return fake.add_user("712020:00000000-0000-4000-8000-000000000101", "Human 1")


def _of(page: NoticePage, disposition: Disposition) -> list[MarkedNotice]:
    return [n for n in page.notices if n.disposition is disposition]


@pytest.mark.anyio
async def test_jira_cancel_a_removed_label_cancels_with_the_removing_history_id() -> None:
    fake = JiraFake()
    human = _human(fake)
    issue = fake.seed_issue("Ticket")
    fake.add_label(issue.id, LABEL, human)
    tracker = jira_tracker(fake)
    admitted = await tracker.poll_marked(None, running=())
    (admit,) = admitted.notices
    assert await tracker.verify_current(admit) is True

    removed = fake.remove_label(issue.id, LABEL, human)
    page = await tracker.poll_marked(admitted.cursor, running=(fake.ref(issue),))

    (cancel,) = page.notices
    assert cancel.disposition is Disposition.CANCEL
    assert (cancel.issue, cancel.event_id, cancel.actor) == (fake.ref(issue), removed, human.actor)
    assert await tracker.verify_current(cancel) is True
    assert await tracker.verify_current(admit) is False


@pytest.mark.anyio
async def test_jira_cancel_a_done_status_cancels_with_the_status_change_id() -> None:
    fake = JiraFake()
    human = _human(fake)
    issue = fake.seed_issue("Ticket", status="In Progress")
    fake.add_label(issue.id, LABEL, human)
    tracker = jira_tracker(fake)
    admitted = await tracker.poll_marked(None, running=())
    (admit,) = admitted.notices

    done = fake.set_status(issue.id, "Done", human)
    page = await tracker.poll_marked(admitted.cursor, running=(fake.ref(issue),))

    (cancel,) = page.notices
    assert cancel.disposition is Disposition.CANCEL
    assert (cancel.issue, cancel.event_id) == (fake.ref(issue), done)
    assert await tracker.verify_current(cancel) is True
    # The label is still on the issue; done alone ends the admission.
    assert LABEL in fake.issue(issue.id).labels
    assert await tracker.verify_current(admit) is False


@pytest.mark.anyio
async def test_jira_cancel_a_running_issue_that_keeps_its_label_is_not_cancelled() -> None:
    """Liveness for the two cancellations above: a running issue still labelled
    and not done, in a status change that is not done, yields nothing."""

    fake = JiraFake()
    human = _human(fake)
    issue = fake.seed_issue("Ticket")
    fake.add_label(issue.id, LABEL, human)
    tracker = jira_tracker(fake)
    admitted = await tracker.poll_marked(None, running=())

    fake.set_status(issue.id, "In Progress", human)
    page = await tracker.poll_marked(admitted.cursor, running=(fake.ref(issue),))

    assert page.notices == ()
    assert fake.requests_of("issue"), "a running issue is read on every poll"


@pytest.mark.anyio
async def test_jira_cancel_a_running_issue_that_answers_404_yields_no_notice() -> None:
    fake = JiraFake()
    human = _human(fake)
    issue = fake.seed_issue("Ticket")
    fake.add_label(issue.id, LABEL, human)
    tracker = jira_tracker(fake)
    admitted = await tracker.poll_marked(None, running=())

    gone = fake.ref(issue)
    fake.delete_issue(issue.id)
    page = await tracker.poll_marked(admitted.cursor, running=(gone,))

    assert page.notices == ()


@pytest.mark.anyio
async def test_jira_cancel_a_relabel_after_cancel_is_a_new_admission() -> None:
    fake = JiraFake()
    human = _human(fake)
    issue = fake.seed_issue("Ticket")
    fake.add_label(issue.id, LABEL, human)
    tracker = jira_tracker(fake)
    admitted = await tracker.poll_marked(None, running=())
    (first,) = admitted.notices

    fake.remove_label(issue.id, LABEL, human)
    cancelled = await tracker.poll_marked(admitted.cursor, running=(fake.ref(issue),))
    assert [n.disposition for n in cancelled.notices] == [Disposition.CANCEL]

    fake.advance(30)
    relabelled = fake.add_label(issue.id, LABEL, human)
    page = await tracker.poll_marked(cancelled.cursor, running=())

    (second,) = _of(page, Disposition.ADMIT)
    assert (second.issue, second.event_id) == (fake.ref(issue), relabelled)
    assert notice_request_id(second) != notice_request_id(first)
    assert await tracker.verify_current(second) is True
    assert await tracker.verify_current(first) is False


@pytest.mark.anyio
async def test_jira_cancel_the_removal_is_found_when_it_predates_the_cursor() -> None:
    fake = JiraFake()
    human = _human(fake)
    issue = fake.seed_issue("Ticket")
    fake.add_label(issue.id, LABEL, human)
    removed = fake.remove_label(issue.id, LABEL, human)
    tracker = jira_tracker(fake)
    caught_up = await tracker.poll_marked(None, running=())
    assert caught_up.notices == ()

    fake.advance(3600)
    page = await tracker.poll_marked(caught_up.cursor, running=(fake.ref(issue),))

    assert [(n.disposition, n.event_id) for n in page.notices] == [(Disposition.CANCEL, removed)]
