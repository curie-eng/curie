"""`Tracker.create_linked_issue` (ADR 0277, issue #5585 decision 6, criterion 4).

Runs on every pair whose tracker declares the operation supported: the
in-memory tracker and the GitHub tracker with only its HTTP API substituted
(`forge_fakes.github.GitHubRepositoryFake`). Every other pair declares it
unsupported and is skipped here; ``test_capabilities.py`` proves those raise
`Unsupported`. A vector reads results only through the port and the
`LinkedIssueHarness` hooks.
"""

from __future__ import annotations

import hashlib
import re

import pytest
from curie_api.forges.capabilities import Operation, supports
from curie_api.forges.errors import Ambiguous, NotFound, Unavailable
from curie_api.forges.types import LinkedIssue, TrackerIssueRef
from forge_fakes.contract_harness import (
    AdapterHarness,
    GitHubHarness,
    InMemoryHarness,
    LinkedIssueHarness,
)

OUTPUT = "exec-0001:triage"
OTHER_OUTPUT = "exec-0002:triage"
TITLE = "Parser drops the last field"
BODY = "The parser drops the last field.\r\n\r\nReproduce with the fixture.\n\n"
LABELS = ("factory-ready", "bug")
# A closing keyword followed by a reference to the source in any form GitHub
# or the in-memory tracker would read as closing it.
_CLOSING = r"(?i)\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\b:?\s+\S*(?:#|/issues/){number}\b"


def _linked(harness: AdapterHarness) -> LinkedIssueHarness:
    if not supports(harness.tracker.capabilities, Operation.CREATE_LINKED_ISSUE):
        pytest.skip(f"{harness.name} declares create_linked_issue unsupported")
    assert isinstance(harness, InMemoryHarness | GitHubHarness), harness.name
    return harness


async def _create(
    harness: LinkedIssueHarness, source: TrackerIssueRef, output: str = OUTPUT
) -> LinkedIssue:
    return await harness.tracker.create_linked_issue(
        source, output_identity=output, title=TITLE, body=BODY, labels=LABELS
    )


def _lines(body: str) -> list[str]:
    return [line.strip() for line in body.splitlines() if line.strip()]


@pytest.mark.anyio
async def test_the_first_call_creates_one_linked_issue(harness: AdapterHarness) -> None:
    linked = _linked(harness)
    source = linked.seed_issue("Source", "Triage this.")
    before = linked.write_count()

    result = await _create(linked, source)

    assert result.created is True
    assert result.issue != source
    assert (result.issue.kind, result.issue.host, result.issue.scope_id) == (
        source.kind,
        source.host,
        source.scope_id,
    )
    assert result.url == linked.tracker.issue_url(result.issue)
    assert result.labels == frozenset(LABELS)
    # Exactly one write, the create: no relation, label or edit beside it.
    assert linked.write_count() == before + 1
    assert linked.marked_issue_count(OUTPUT) == 1
    stored = linked.issue_body(result.issue)
    lines = _lines(stored)
    assert lines[-1] == linked.embed_linked_marker(OUTPUT)
    assert lines[-2].startswith("Related to ")
    assert re.search(rf"(?:#|/issues/){source.issue_id}\b", lines[-2])
    assert lines[:-2] == ["The parser drops the last field.", "Reproduce with the fixture."]
    assert re.search(_CLOSING.format(number=source.issue_id), stored) is None
    assert await linked.tracker.dependencies(result.issue) == ()
    sent = linked.sent_issue_bodies()
    assert len(sent) == 1
    digest = hashlib.sha256(sent[-1].encode()).hexdigest()
    assert result.body_sha256 == result.sent_body_sha256 == digest
    assert hashlib.sha256(stored.encode()).hexdigest() == digest


@pytest.mark.anyio
async def test_a_repeat_after_a_lost_response_returns_the_same_issue(
    harness: AdapterHarness,
) -> None:
    linked = _linked(harness)
    source = linked.seed_issue("Source", "Triage this.")
    linked.lose_next_create_response()

    # The create lands either way. An adapter that relists after an uncertain
    # answer (GitHub) reconciles inside the call; one whose answer is simply
    # lost (in-memory) raises, and the caller's repeat reconciles.
    first: LinkedIssue | None
    try:
        first = await _create(linked, source)
    except Unavailable:
        first = None
    writes = linked.write_count()
    second = await _create(linked, source)

    assert second.created is False
    if first is not None:
        assert first.issue == second.issue
        assert first.created is False
    assert linked.marked_issue_count(OUTPUT) == 1
    assert len(linked.sent_issue_bodies()) == 1
    assert linked.write_count() == writes
    assert second.body_sha256 == second.sent_body_sha256


@pytest.mark.anyio
async def test_a_repeat_after_success_writes_nothing(harness: AdapterHarness) -> None:
    linked = _linked(harness)
    source = linked.seed_issue("Source", "Triage this.")
    first = await _create(linked, source)
    writes = linked.write_count()

    again = await _create(linked, source)

    assert again.issue == first.issue and again.created is False
    assert again.labels == frozenset(LABELS)
    assert linked.write_count() == writes
    assert linked.marked_issue_count(OUTPUT) == 1


@pytest.mark.anyio
async def test_a_marker_in_a_person_authored_issue_is_ignored(harness: AdapterHarness) -> None:
    linked = _linked(harness)
    source = linked.seed_issue("Source", "Triage this.")
    pasted = f"Copied from the factory.\n\n{linked.embed_linked_marker(OUTPUT)}\n"
    planted = linked.add_issue(linked.writer, pasted)

    result = await _create(linked, source)

    assert result.created is True
    assert result.issue not in {planted, source}
    assert linked.issue_body(planted) == pasted
    assert linked.marked_issue_count(OUTPUT) == 1


@pytest.mark.anyio
async def test_two_own_matches_are_ambiguous_and_create_nothing(harness: AdapterHarness) -> None:
    linked = _linked(harness)
    source = linked.seed_issue("Source", "Triage this.")
    marked = f"Earlier output.\n\n{linked.embed_linked_marker(OUTPUT)}\n"
    linked.add_issue(None, marked)
    linked.add_issue(None, marked)
    writes = linked.write_count()

    with pytest.raises(Ambiguous):
        await _create(linked, source)

    assert linked.write_count() == writes
    assert linked.marked_issue_count(OUTPUT) == 2
    assert linked.sent_issue_bodies() == []


@pytest.mark.anyio
async def test_a_marker_that_is_not_the_last_line_or_is_another_outputs_does_not_match(
    harness: AdapterHarness,
) -> None:
    linked = _linked(harness)
    source = linked.seed_issue("Source", "Triage this.")
    mid_body = linked.add_issue(
        None, f"Quoted:\n\n{linked.embed_linked_marker(OUTPUT)}\n\nMore text after it.\n"
    )
    other = linked.add_issue(None, f"Other.\n\n{linked.embed_linked_marker(OTHER_OUTPUT)}\n")

    result = await _create(linked, source)

    assert result.created is True
    assert result.issue not in {mid_body, other}
    assert linked.marked_issue_count(OUTPUT) == 1
    assert linked.marked_issue_count(OTHER_OUTPUT) == 1


@pytest.mark.anyio
@pytest.mark.parametrize("after", [0, 1])
async def test_a_listing_that_cannot_be_read_to_its_end_creates_nothing(
    harness: AdapterHarness, after: int
) -> None:
    linked = _linked(harness)
    source = linked.seed_issue("Source", "Triage this.")
    # Three own issues span two pages at two per page; ``after`` 1 fails the second.
    for index in range(3):
        linked.add_issue(None, f"Unrelated own issue {index}.")
    writes = linked.write_count()
    linked.fail_next_page(after=after)

    with pytest.raises(Unavailable):
        await _create(linked, source)

    assert linked.write_count() == writes
    assert linked.sent_issue_bodies() == []
    assert linked.marked_issue_count(OUTPUT) == 0


@pytest.mark.anyio
@pytest.mark.parametrize("output", ["", " spaced", "-leading", "a" * 129, "x-->", "two\nlines"])
async def test_an_invalid_output_identity_is_refused_before_any_write(
    harness: AdapterHarness, output: str
) -> None:
    linked = _linked(harness)
    source = linked.seed_issue("Source", "Triage this.")
    writes = linked.write_count()

    with pytest.raises(ValueError):
        await _create(linked, source, output)

    assert linked.write_count() == writes
    assert linked.sent_issue_bodies() == []


@pytest.mark.anyio
async def test_a_source_outside_the_tracker_is_not_found(harness: AdapterHarness) -> None:
    linked = _linked(harness)
    source = linked.seed_issue("Source", "Triage this.")
    elsewhere = TrackerIssueRef(source.kind, source.host, "999999", source.issue_id)
    writes = linked.write_count()

    with pytest.raises(NotFound):
        await _create(linked, elsewhere)

    assert linked.write_count() == writes
    assert linked.sent_issue_bodies() == []


@pytest.mark.anyio
async def test_a_retry_naming_the_recorded_issue_returns_it_without_creating(
    harness: AdapterHarness,
) -> None:
    linked = _linked(harness)
    source = linked.seed_issue("Source", "Triage this.")
    recorded = await _create(linked, source)
    writes, count, listings = linked.write_count(), linked.issue_count(), linked.listing_count()

    again = await linked.tracker.create_linked_issue(
        source,
        output_identity=OUTPUT,
        title=TITLE,
        body=BODY,
        labels=LABELS,
        expected=recorded.issue,
    )

    assert again.issue == recorded.issue and again.created is False
    # The recorded issue is read directly: the listing can lag a create.
    assert linked.listing_count() == listings
    assert again.body_sha256 == again.sent_body_sha256 == recorded.sent_body_sha256
    assert linked.write_count() == writes and linked.issue_count() == count
    assert len(linked.sent_issue_bodies()) == 1


@pytest.mark.anyio
async def test_a_retry_naming_an_issue_whose_marker_was_removed_never_creates(
    harness: AdapterHarness,
) -> None:
    linked = _linked(harness)
    source = linked.seed_issue("Source", "Triage this.")
    recorded = await _create(linked, source)
    linked.edit_issue_body(recorded.issue, "Someone rewrote this issue.")
    writes, count, listings = linked.write_count(), linked.issue_count(), linked.listing_count()

    with pytest.raises(NotFound, match="linked_issue"):
        await linked.tracker.create_linked_issue(
            source,
            output_identity=OUTPUT,
            title=TITLE,
            body=BODY,
            labels=LABELS,
            expected=recorded.issue,
        )

    assert linked.write_count() == writes and linked.issue_count() == count
    assert linked.listing_count() == listings
    assert len(linked.sent_issue_bodies()) == 1
    assert linked.marked_issue_count(OUTPUT) == 0


@pytest.mark.anyio
async def test_a_retry_naming_an_issue_another_identity_created_is_not_found(
    harness: AdapterHarness,
) -> None:
    linked = _linked(harness)
    source = linked.seed_issue("Source", "Triage this.")
    await _create(linked, source)
    # A person's issue carrying the marker as its last line is still not ours.
    pasted = linked.add_issue(linked.writer, f"Copied.\n\n{linked.embed_linked_marker(OUTPUT)}\n")
    writes, count, listings = linked.write_count(), linked.issue_count(), linked.listing_count()

    with pytest.raises(NotFound, match="linked_issue"):
        await linked.tracker.create_linked_issue(
            source,
            output_identity=OUTPUT,
            title=TITLE,
            body=BODY,
            labels=LABELS,
            expected=pasted,
        )

    assert linked.write_count() == writes and linked.issue_count() == count
    assert linked.listing_count() == listings
