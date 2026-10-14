"""The channel-neutral text of a progress card or milestone (ADR-0130).

A card edit carries no ``message``, so a channel with nothing richer than text
needs the text from somewhere other than the body. ``progress_text`` is that
one place, and ``progress_heading`` is the part of it a richer renderer shows
above the summary, so Slack's card and Discord's text read the same.
"""

from __future__ import annotations

import pytest
from channel_protocol.progress import (
    TERMINAL_PROGRESS_STATES,
    MilestoneClass,
    ProgressCard,
    ProgressMilestone,
    ProgressState,
    progress_heading,
    progress_text,
)


def _card(
    state: ProgressState, summary: str = "Reading the ledger", revision: int = 2
) -> ProgressCard:
    return ProgressCard(
        kind="card",
        state=state,
        summary=summary,
        revision=revision,
        terminal=state in TERMINAL_PROGRESS_STATES,
    )


def _milestone(
    milestone: MilestoneClass, summary: str = "Found the failing migration"
) -> ProgressMilestone:
    return ProgressMilestone(kind="milestone", milestone=milestone, summary=summary, ordinal=1)


@pytest.mark.parametrize(
    ("state", "heading"),
    [
        (ProgressState.QUEUED, "Task status: Queued"),
        (ProgressState.INVESTIGATING, "Task status: Investigating"),
        (ProgressState.AWAITING_APPROVAL, "Task status: Waiting for approval"),
        (ProgressState.PREPARING_WORKSPACE, "Task status: Preparing the workspace"),
        (ProgressState.TESTING, "Task status: Testing"),
        (ProgressState.PUBLISHING, "Task status: Publishing"),
        (ProgressState.COMPLETE, "Task complete"),
        (ProgressState.FAILED, "Task failed"),
        (ProgressState.CANCELLED, "Task cancelled"),
    ],
)
def test_a_card_names_its_state_in_plain_words(state: ProgressState, heading: str) -> None:
    """@spec ADR-0130 d2: every state reads as words, and a closed card says it is closed."""

    card = _card(state)

    assert progress_heading(card) == heading
    assert progress_text(card) == f"{heading}. Reading the ledger"


@pytest.mark.parametrize(
    ("milestone", "heading"),
    [
        (MilestoneClass.EVIDENCE, "Milestone: Evidence acquired"),
        (MilestoneClass.SCOPE, "Milestone: Scope changed"),
        (MilestoneClass.VERIFICATION, "Milestone: Verification result"),
    ],
)
def test_a_milestone_names_why_it_interrupts(milestone: MilestoneClass, heading: str) -> None:
    """@spec ADR-0130 d3: the class is the reason for the interruption."""

    item = _milestone(milestone)

    assert progress_heading(item) == heading
    assert progress_text(item) == f"{heading}. Found the failing migration"


def test_every_state_and_class_has_a_heading() -> None:
    """A state added to the closed set without words fails here, not in a channel."""

    for state in ProgressState:
        assert progress_heading(_card(state)).strip()
    for milestone in MilestoneClass:
        assert progress_heading(_milestone(milestone)).strip()


def test_the_text_is_plain_ascii_words_with_no_emoji() -> None:
    """@spec ADR-0130 d5: progress is short task state, not decoration."""

    for state in ProgressState:
        assert progress_heading(_card(state)).isascii()
    for milestone in MilestoneClass:
        assert progress_heading(_milestone(milestone)).isascii()


def test_the_summary_is_carried_verbatim() -> None:
    """Escaping is each channel's job, so the neutral text does not do it.

    A Slack fallback escapes this text and Discord disables mentions on the
    send; either would be wrong if the shared text had already changed it.
    """

    summary = "<!channel> & <@U0EXAMPLE1> *not bold* :tada:"

    assert progress_text(_card(ProgressState.TESTING, summary)).endswith(summary)
    assert progress_text(_milestone(MilestoneClass.SCOPE, summary)).endswith(summary)


def test_the_text_does_not_depend_on_the_revision() -> None:
    """Two revisions of one state and summary read alike, whichever arrives."""

    assert progress_text(_card(ProgressState.TESTING, revision=2)) == progress_text(
        _card(ProgressState.TESTING, revision=7)
    )
