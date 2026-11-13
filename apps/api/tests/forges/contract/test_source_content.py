"""`Tracker.read_source_content` (ADR 0277 decision 4, issue #5586).

A tracker result run holds a digest of its source issue's title and body. The
read answers the issue's own title and description fields only, so a new
comment, the factory's own status and result comments included, never moves
it; it makes no write and names no issue outside the tracker. A vector reads
results only through the port and the `AdapterHarness` hooks.
"""

from __future__ import annotations

from dataclasses import replace

import pytest
from curie_api.forges.errors import NotFound
from curie_api.forges.types import ReplyTarget
from forge_fakes.contract_harness import AdapterHarness

TITLE = "Parser drops the last field"
BODY = "Parsing a three field line yields two fields."


@pytest.mark.anyio
async def test_source_content_is_the_title_and_description_and_ignores_comments(
    harness: AdapterHarness,
) -> None:
    issue = harness.seed_issue(TITLE, BODY)
    writes = harness.write_count()

    content = await harness.tracker.read_source_content(issue)

    assert content.title == TITLE
    assert BODY in content.body
    assert await harness.tracker.read_source_content(issue) == content
    # Neither a person's comment nor one carrying a factory marker moves it.
    target = ReplyTarget.on_issue(issue)
    harness.add_foreign_comment(target, harness.outsider, "curie-status:contract", "A comment.")
    harness.add_foreign_comment(target, harness.writer, "curie-tracker-result:x", "Findings.")
    assert await harness.tracker.read_source_content(issue) == content
    assert harness.write_count() == writes


@pytest.mark.anyio
async def test_an_issue_on_another_scope_has_no_source_content(harness: AdapterHarness) -> None:
    issue = harness.seed_issue(TITLE, BODY)
    foreign = replace(issue, scope_id=issue.scope_id + "9")

    with pytest.raises(NotFound):
        await harness.tracker.read_source_content(foreign)
