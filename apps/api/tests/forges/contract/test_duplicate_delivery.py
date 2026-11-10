"""The same marking, seen twice, is one request identity."""

from __future__ import annotations

import pytest
from curie_api.forges.identity import notice_request_id
from curie_api.forges.types import Disposition
from forge_fakes.contract_harness import AdapterHarness


@pytest.mark.anyio
async def test_polling_one_marking_twice_yields_one_request_id(harness: AdapterHarness) -> None:
    start = await harness.tracker.poll_marked(None, running=())
    issue = harness.seed_issue("Ticket", "Do the thing.")
    harness.apply_label(issue, harness.writer)

    first = await harness.tracker.poll_marked(start.cursor, running=())
    again = await harness.tracker.poll_marked(start.cursor, running=())

    ids = {notice_request_id(n) for n in (*first.notices, *again.notices)}
    assert len(first.notices) == 1
    assert first.notices == again.notices
    assert len(ids) == 1
    # Polling on from the returned cursor does not deliver it a third time.
    later = await harness.tracker.poll_marked(first.cursor, running=())
    assert later.notices == ()


@pytest.mark.anyio
async def test_a_relabel_is_a_new_request_id(harness: AdapterHarness) -> None:
    start = await harness.tracker.poll_marked(None, running=())
    issue = harness.seed_issue("Ticket", "Do the thing.")
    harness.apply_label(issue, harness.writer)
    harness.remove_label(issue, harness.writer)
    harness.apply_label(issue, harness.writer)

    page = await harness.tracker.poll_marked(start.cursor, running=())
    admits = [n for n in page.notices if n.disposition is Disposition.ADMIT]
    assert len(admits) == 2
    assert len({notice_request_id(n) for n in admits}) == 2
