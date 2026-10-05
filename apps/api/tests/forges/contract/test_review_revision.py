"""Review feedback is acted on only when current and by someone with authority."""

from __future__ import annotations

import pytest
from curie_api.forges.authority import feedback_actionable
from curie_api.forges.types import FeedbackKind
from forge_fakes.contract_harness import AdapterHarness, open_pull


@pytest.mark.anyio
@pytest.mark.parametrize("kind", list(FeedbackKind))
async def test_feedback_by_a_writer_is_listed_and_actionable(
    harness: AdapterHarness, kind: FeedbackKind
) -> None:
    harness.set_write_access(harness.writer, True)
    pull = await open_pull(harness, "factory/review")
    harness.add_review_feedback(pull.ref, harness.writer, "Please rename it.", kind)

    page = await harness.code_host.list_review_feedback(pull.ref, None)
    (feedback,) = page.items
    assert (feedback.author, feedback.body, feedback.kind) == (
        harness.writer,
        "Please rename it.",
        kind,
    )
    assert feedback.head_sha == pull.head_sha
    assert await harness.code_host.verify_feedback(feedback) is True
    assert (
        await feedback_actionable(harness.code_host, feedback, harness.feedback_allowlist()) is True
    )


@pytest.mark.anyio
async def test_feedback_by_a_non_writer_is_listed_but_not_actionable(
    harness: AdapterHarness,
) -> None:
    harness.set_write_access(harness.writer, True)
    pull = await open_pull(harness, "factory/review")
    harness.add_review_feedback(pull.ref, harness.outsider, "Delete it all.", FeedbackKind.COMMENT)

    (feedback,) = (await harness.code_host.list_review_feedback(pull.ref, None)).items
    assert feedback.author == harness.outsider
    assert (
        await feedback_actionable(harness.code_host, feedback, harness.feedback_allowlist())
        is False
    )


@pytest.mark.anyio
async def test_listing_from_the_returned_cursor_yields_only_newer_feedback(
    harness: AdapterHarness,
) -> None:
    pull = await open_pull(harness, "factory/review")
    harness.add_review_feedback(pull.ref, harness.writer, "first", FeedbackKind.COMMENT)
    first = await harness.code_host.list_review_feedback(pull.ref, None)
    harness.add_review_feedback(pull.ref, harness.writer, "second", FeedbackKind.REVIEW)

    later = await harness.code_host.list_review_feedback(pull.ref, first.cursor)
    assert [item.body for item in later.items] == ["second"]
