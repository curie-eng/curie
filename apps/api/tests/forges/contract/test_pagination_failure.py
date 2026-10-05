"""A listing that fails part way raises Unavailable and moves no cursor."""

from __future__ import annotations

import pytest
from curie_api.forges.errors import Unavailable
from curie_api.forges.types import Disposition, FeedbackKind
from forge_fakes.contract_harness import AdapterHarness, open_pull


@pytest.mark.anyio
async def test_a_failing_second_page_of_the_poll_yields_no_cursor(
    harness: AdapterHarness,
) -> None:
    start = await harness.tracker.poll_marked(None, running=())
    issues = [harness.seed_issue(f"Ticket {n}", "Do the thing.") for n in range(3)]
    for issue in issues:
        harness.apply_label(issue, harness.writer)

    harness.fail_next_page(after=1)
    with pytest.raises(Unavailable):
        await harness.tracker.poll_marked(start.cursor, running=())

    # The caller kept start.cursor; the retry sees every marking, including
    # the ones on the page that was read before the failure.
    retry = await harness.tracker.poll_marked(start.cursor, running=())
    admitted = {n.issue for n in retry.notices if n.disposition is Disposition.ADMIT}
    assert admitted == set(issues)
    assert retry.cursor != start.cursor


@pytest.mark.anyio
async def test_a_failing_first_page_of_the_poll_is_unavailable_too(
    harness: AdapterHarness,
) -> None:
    issue = harness.seed_issue("Ticket", "Do the thing.")
    harness.apply_label(issue, harness.writer)
    harness.fail_next_page()
    with pytest.raises(Unavailable):
        await harness.tracker.poll_marked(None, running=())
    page = await harness.tracker.poll_marked(None, running=())
    assert [n.issue for n in page.notices] == [issue]


@pytest.mark.anyio
async def test_a_failing_page_of_review_feedback_yields_no_cursor(
    harness: AdapterHarness,
) -> None:
    pull = await open_pull(harness, "factory/paged")
    for n in range(3):
        harness.add_review_feedback(pull.ref, harness.writer, f"note {n}", FeedbackKind.COMMENT)

    harness.fail_next_page(after=1)
    with pytest.raises(Unavailable):
        await harness.code_host.list_review_feedback(pull.ref, None)

    retry = await harness.code_host.list_review_feedback(pull.ref, None)
    assert [item.body for item in retry.items] == ["note 0", "note 1", "note 2"]
    after = await harness.code_host.list_review_feedback(pull.ref, retry.cursor)
    assert after.items == ()
