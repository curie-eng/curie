"""Removing the marker cancels a running WorkItem, even before the cursor."""

from __future__ import annotations

import pytest
from curie_api.forges.types import Disposition
from forge_fakes.contract_harness import AdapterHarness


@pytest.mark.anyio
async def test_removing_the_label_yields_a_current_cancellation(harness: AdapterHarness) -> None:
    issue = harness.seed_issue("Ticket", "Do the thing.")
    harness.apply_label(issue, harness.writer)
    admitted = await harness.tracker.poll_marked(None, running=())
    (admit,) = admitted.notices
    assert await harness.tracker.verify_current(admit) is True

    harness.remove_label(issue, harness.writer)
    page = await harness.tracker.poll_marked(admitted.cursor, running=(issue,))

    cancels = [n for n in page.notices if n.disposition is Disposition.CANCEL]
    assert [n.issue for n in cancels] == [issue]
    assert await harness.tracker.verify_current(cancels[0]) is True
    # The admission no longer describes the issue.
    assert await harness.tracker.verify_current(admit) is False


@pytest.mark.anyio
async def test_a_running_issue_is_re_read_when_the_removal_predates_the_cursor(
    harness: AdapterHarness,
) -> None:
    issue = harness.seed_issue("Ticket", "Do the thing.")
    harness.apply_label(issue, harness.writer)
    harness.remove_label(issue, harness.writer)
    caught_up = await harness.tracker.poll_marked(None, running=())

    page = await harness.tracker.poll_marked(caught_up.cursor, running=(issue,))
    assert [(n.issue, n.disposition) for n in page.notices] == [(issue, Disposition.CANCEL)]


@pytest.mark.anyio
async def test_a_running_issue_that_keeps_its_label_is_not_cancelled(
    harness: AdapterHarness,
) -> None:
    issue = harness.seed_issue("Ticket", "Do the thing.")
    harness.apply_label(issue, harness.writer)
    caught_up = await harness.tracker.poll_marked(None, running=())

    page = await harness.tracker.poll_marked(caught_up.cursor, running=(issue,))
    assert page.notices == ()
