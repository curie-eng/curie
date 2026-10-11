"""Work item links come from the tracker (ADR 0197, #3831).

`Tracker.issue_url` is the one place a link to a tracker issue is built. It
makes no request, so it writes nothing, and it refuses an issue that is not on
this tracker rather than producing a link to somewhere else.
"""

from __future__ import annotations

from dataclasses import replace

import pytest
from curie_api.forges.errors import NotFound
from forge_fakes.contract_harness import AdapterHarness


def test_an_issue_link_names_the_issue_on_the_tracker_host(harness: AdapterHarness) -> None:
    issue = harness.seed_issue("Ticket", "Do the thing.")
    writes = harness.write_count()

    url = harness.tracker.issue_url(issue)

    assert url.startswith("https://")
    assert harness.tracker.host in url
    assert url.endswith(f"/issues/{issue.issue_id}")
    assert harness.tracker.issue_url(issue) == url
    assert harness.write_count() == writes


def test_an_issue_on_another_scope_has_no_link(harness: AdapterHarness) -> None:
    issue = harness.seed_issue("Ticket", "Do the thing.")
    foreign = replace(issue, scope_id=issue.scope_id + "9")

    with pytest.raises(NotFound):
        harness.tracker.issue_url(foreign)
