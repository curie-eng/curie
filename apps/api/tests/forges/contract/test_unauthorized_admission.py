"""A marking by an actor without authority may not start a run."""

from __future__ import annotations

import pytest
from forge_fakes.contract_harness import AdapterHarness


@pytest.mark.anyio
async def test_an_actor_without_authority_may_not_start(harness: AdapterHarness) -> None:
    harness.set_write_access(harness.writer, True)
    issue = harness.seed_issue("Ticket", "Do the thing.")
    harness.apply_label(issue, harness.outsider)

    page = await harness.tracker.poll_marked(None, running=())
    (notice,) = [n for n in page.notices if n.issue == issue]
    assert notice.actor == harness.outsider
    assert await harness.tracker.marking_actor(issue, harness.label) == harness.outsider
    assert await harness.tracker.may_start(issue, harness.outsider) is False


@pytest.mark.anyio
async def test_an_actor_with_authority_may_start(harness: AdapterHarness) -> None:
    harness.set_write_access(harness.writer, True)
    issue = harness.seed_issue("Ticket", "Do the thing.")
    harness.apply_label(issue, harness.writer)

    assert await harness.tracker.marking_actor(issue, harness.label) == harness.writer
    assert await harness.tracker.may_start(issue, harness.writer) is True


@pytest.mark.anyio
async def test_revoked_authority_may_no_longer_start(harness: AdapterHarness) -> None:
    harness.set_write_access(harness.writer, True)
    issue = harness.seed_issue("Ticket", "Do the thing.")
    harness.apply_label(issue, harness.writer)
    harness.set_write_access(harness.writer, False)

    assert await harness.tracker.may_start(issue, harness.writer) is False
