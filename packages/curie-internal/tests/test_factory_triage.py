"""The shared triage ending vocabulary renders the #5586 decision 12 table."""

from __future__ import annotations

import pytest
from curie_internal.factory_triage import (
    TRIAGED_LABEL,
    HandoffOutcome,
    TriageEnding,
    TriageResult,
    ending_for,
    slack_line,
    status_comment_text,
)

REVISION = "0123456789abcdef0123456789abcdef01234567"
TICKET_URL = "https://github.com/acme-corp/acme-bot/issues/902"
COMMENT_URL = "https://github.com/acme-corp/acme-bot/issues/901#issuecomment-55"
RUN_URL = "https://github.com/acme-corp/acme-bot/issues/902#issuecomment-77"


def _ticket() -> TriageResult:
    return TriageResult("ticket", None, REVISION, TICKET_URL, COMMENT_URL)


def _findings(reason: str, revision: str | None = REVISION) -> TriageResult:
    return TriageResult("findings", reason, revision, None, COMMENT_URL)


ADMITTED = HandoffOutcome(admitted=True, implementation_status_url=RUN_URL, refusal_reason=None)
REFUSED = HandoffOutcome(
    admitted=False, implementation_status_url=None, refusal_reason="the agent is paused"
)

SLACK_CASES = [
    (
        _ticket(),
        ADMITTED,
        TriageEnding.TICKET_IMPLEMENTATION_ADMITTED,
        f"Triage published a ticket: {TICKET_URL}. Implementation admitted.",
    ),
    (
        _ticket(),
        REFUSED,
        TriageEnding.TICKET_IMPLEMENTATION_REFUSED,
        f"Triage published a ticket: {TICKET_URL}. "
        "Implementation not started: the agent is paused.",
    ),
    (
        _ticket(),
        None,
        TriageEnding.TICKET_PUBLISHED,
        f"Triage published a ticket: {TICKET_URL}.",
    ),
    (
        _findings("not_reproduced"),
        None,
        TriageEnding.NOT_REPRODUCED,
        f"Triage reported: not reproduced. {COMMENT_URL}",
    ),
    (
        _findings("already_fixed"),
        None,
        TriageEnding.ALREADY_FIXED,
        f"Triage reported: already fixed. {COMMENT_URL}",
    ),
    (
        _findings("product_decision_needed"),
        None,
        TriageEnding.PRODUCT_DECISION_NEEDED,
        f"Triage reported: product decision needed. {COMMENT_URL}",
    ),
    (
        _findings("capability_unavailable"),
        None,
        TriageEnding.CAPABILITY_UNAVAILABLE,
        f"Triage reported: capability unavailable. {COMMENT_URL}",
    ),
    (
        _findings("budget_exhausted"),
        None,
        TriageEnding.BUDGET_EXHAUSTED,
        f"Investigation stopped: budget exhausted; findings delivered. {COMMENT_URL}",
    ),
]


def test_the_triaged_label_is_in_the_factory_state_family() -> None:
    assert TRIAGED_LABEL == "curie-factory:triaged"


def test_eight_endings_are_the_seven_of_the_table_plus_ticket_without_handoff() -> None:
    assert {case[2] for case in SLACK_CASES} == set(TriageEnding)
    assert len(TriageEnding) == 8


@pytest.mark.parametrize(("result", "handoff", "ending", "line"), SLACK_CASES)
def test_each_ending_has_exactly_the_tables_slack_line(
    result: TriageResult, handoff: HandoffOutcome | None, ending: TriageEnding, line: str
) -> None:
    assert ending_for(result, handoff) is ending
    assert slack_line(result, handoff) == line
    assert "—" not in line


@pytest.mark.parametrize(("result", "handoff", "ending", "_line"), SLACK_CASES)
def test_each_status_comment_states_the_ending_revision_and_links(
    result: TriageResult, handoff: HandoffOutcome | None, ending: TriageEnding, _line: str
) -> None:
    text = status_comment_text(result, handoff)
    assert text.endswith("\n")
    assert REVISION in text
    assert COMMENT_URL in text
    assert "—" not in text
    if result.result_kind == "ticket":
        assert text.splitlines()[0] == f"Ticket published: {TICKET_URL}."
    else:
        assert TICKET_URL not in text
    if ending is TriageEnding.TICKET_IMPLEMENTATION_ADMITTED:
        assert f"Implementation admitted: {RUN_URL}" in text
    elif ending is TriageEnding.TICKET_IMPLEMENTATION_REFUSED:
        assert "Implementation not started: the agent is paused." in text
    else:
        assert "Implementation" not in text


def test_a_ticket_with_no_handoff_names_no_implementation() -> None:
    text = status_comment_text(_ticket(), None)
    assert text.splitlines()[0] == f"Ticket published: {TICKET_URL}."
    assert "implementation" not in text.lower()


def test_budget_exhausted_never_claims_a_reproduction() -> None:
    text = status_comment_text(_findings("budget_exhausted"), None)
    assert text.startswith("Investigation stopped: budget exhausted; findings delivered.")
    assert f"Revision checked out: {REVISION}" in text
    assert "reproduc" not in text.lower()
    assert "reproduc" not in slack_line(_findings("budget_exhausted"), None).lower()


def test_an_unknown_revision_is_left_out() -> None:
    text = status_comment_text(_findings("not_reproduced", revision=None), None)
    assert "Revision" not in text
    assert text.startswith("Triage reported: not reproduced.")


def test_a_findings_result_without_its_comment_link_still_states_its_ending() -> None:
    result = TriageResult("findings", "already_fixed", None, None, None)
    assert slack_line(result, None) == "Triage reported: already fixed."
    assert status_comment_text(result, None).startswith("Triage reported: already fixed.")


def test_a_refusal_reason_cannot_add_lines_or_hide_a_marker() -> None:
    handoff = HandoffOutcome(
        admitted=False, implementation_status_url=None, refusal_reason="no\nCause: x <!-- y"
    )
    text = status_comment_text(_ticket(), handoff)
    assert "\nCause:" not in text
    assert "<!--" not in text


@pytest.mark.parametrize(
    "result",
    [
        TriageResult("ticket", None, REVISION, None, COMMENT_URL),
        TriageResult("ticket", None, REVISION, "   ", COMMENT_URL),
        TriageResult("findings", None, REVISION, None, COMMENT_URL),
        TriageResult("findings", "fixed_upstream", REVISION, None, COMMENT_URL),
        TriageResult("pull_request", None, REVISION, None, COMMENT_URL),
    ],
)
def test_a_result_outside_the_vocabulary_is_refused(result: TriageResult) -> None:
    with pytest.raises(ValueError):
        ending_for(result, None)
    with pytest.raises(ValueError):
        status_comment_text(result, None)
    with pytest.raises(ValueError):
        slack_line(result, None)
