"""status_body renders the card or the checklist, never both (#3125)."""

from __future__ import annotations

import uuid
from typing import Any

from curie_api.factory_notices import FINAL_MARKER, marker_for, status_body
from curie_api.factory_progress import PhaseSlot, PhaseView, phase_view
from curie_api.models import ExecutionRequestPhaseReport

REQUEST = uuid.UUID("00000000-0000-0000-0000-000000003125")
CARD = "https://curie.example.com/v1/factory/cards/abc.svg"
WAITING = "_Waiting for the agent to report progress._"


def _view() -> PhaseView:
    return PhaseView(
        phases=(
            PhaseSlot(id="read_issue", label="Read issue", state="done", round_label=None),
            PhaseSlot(id="plan", label="Plan", state="current", round_label="round 1 of 3"),
            PhaseSlot(id="ci", label="Wait for CI", state="pending", round_label=None),
        ),
        loops=(),
        current="plan",
    )


def test_a_card_url_emits_no_checklist_while_running() -> None:
    body = status_body(
        request_id=REQUEST, card_url=CARD, pill_label="RUNNING", phase_view=_view(), result=None
    )
    assert body == f"![Curie status]({CARD})\n\nStatus: RUNNING\n\n{marker_for(REQUEST)}\n"


def test_a_card_url_emits_no_waiting_placeholder() -> None:
    body = status_body(
        request_id=REQUEST, card_url=CARD, pill_label="QUEUED", phase_view=None, result=None
    )
    assert WAITING not in body
    assert body == f"![Curie status]({CARD})\n\nStatus: QUEUED\n\n{marker_for(REQUEST)}\n"


def test_a_card_url_final_body_keeps_the_result_and_card_only() -> None:
    body = status_body(
        request_id=REQUEST,
        card_url=CARD,
        pill_label="PR OPEN",
        phase_view=_view(),
        result="Opened https://github.com/acme/fixture/pull/7\n",
    )
    assert body == (
        "Opened https://github.com/acme/fixture/pull/7\n\n"
        f"![Curie status]({CARD})\n\nStatus: PR OPEN\n\n{FINAL_MARKER}\n\n{marker_for(REQUEST)}\n"
    )


def test_without_a_card_url_the_checklist_is_the_fallback() -> None:
    body = status_body(
        request_id=REQUEST, card_url=None, pill_label="RUNNING", phase_view=_view(), result=None
    )
    assert body == (
        "- [x] Read issue\n- [ ] **Plan** (in progress, round 1 of 3)\n- [ ] Wait for CI\n\n"
        f"Status: RUNNING\n\n{marker_for(REQUEST)}\n"
    )


def test_without_a_card_url_the_waiting_placeholder_stays() -> None:
    body = status_body(
        request_id=REQUEST, card_url=None, pill_label="QUEUED", phase_view=None, result=None
    )
    assert body == f"{WAITING}\n\nStatus: QUEUED\n\n{marker_for(REQUEST)}\n"


# --- #3179: the platform records the CI wait itself --------------------------------

DECLARATION: dict[str, Any] = {
    "phases": [
        {"id": "read_issue", "label": "Read issue"},
        {"id": "pin_criteria", "label": "Pin acceptance criteria"},
        {"id": "plan", "label": "Plan"},
        {"id": "plan_review", "label": "Plan review"},
        {"id": "failing_test", "label": "Failing test"},
        {"id": "implement", "label": "Implement"},
        {"id": "review_diff", "label": "Review diff"},
        {"id": "publish", "label": "Publish PR"},
        {"id": "wait_ci", "label": "Wait for CI"},
    ],
    "loops": [
        {"start": "plan", "review": "plan_review", "cap": 3},
        {"start": "implement", "review": "review_diff", "cap": 3},
    ],
}


def _reports_of(*entries: tuple[str, int | None]) -> list[ExecutionRequestPhaseReport]:
    request_id = uuid.uuid4()
    return [
        ExecutionRequestPhaseReport(
            id=index + 1,
            execution_request_id=request_id,
            phase=phase,
            loop_round=loop_round,
        )
        for index, (phase, loop_round) in enumerate(entries)
    ]


def _states(view: PhaseView) -> dict[str, str]:
    return {phase.id: phase.state for phase in view.phases}


def _labels(view: PhaseView) -> dict[str, str | None]:
    return {phase.id: phase.round_label for phase in view.phases}


_THROUGH_PUBLISH: tuple[tuple[str, int | None], ...] = (
    ("read_issue", None),
    ("pin_criteria", None),
    ("plan", 1),
    ("plan_review", 1),
    ("failing_test", None),
    ("implement", 1),
    ("review_diff", 1),
    ("publish", None),
)
"""The agent's reports up to the turn's end at the publish call."""


def test_the_platform_wait_ci_report_shows_publish_done_and_wait_ci_current() -> None:
    """A succeeded publication and a pending CI: the platform's own report (#3179).

    The agent's turn ends at the publish call, so it can never report the CI
    wait itself; the platform records the wait_ci phase report.
    """

    reports = _reports_of(*_THROUGH_PUBLISH, ("wait_ci", None))
    view = phase_view(DECLARATION, reports, "running", None)
    assert view.current == "wait_ci"
    states = _states(view)
    assert states["publish"] == "done"
    assert states["wait_ci"] == "current"
    assert _labels(view)["review_diff"] == "approved, 1 round"


def test_the_ci_wait_body_shows_publish_checked_and_wait_ci_in_progress() -> None:
    """The status body for a request whose publication succeeded and whose CI is pending."""

    reports = _reports_of(*_THROUGH_PUBLISH, ("wait_ci", None))
    body = status_body(
        request_id=REQUEST,
        card_url=None,
        pill_label="RUNNING",
        phase_view=phase_view(DECLARATION, reports, "running", None),
        result=None,
    )
    assert "- [x] Publish PR\n" in body
    assert "- [ ] **Wait for CI** (in progress)\n" in body


def test_a_fix_round_after_the_wait_ci_report_keeps_its_implement_round() -> None:
    """A CI failure that resumes the run still shows the agent's own loop back."""

    reports = _reports_of(*_THROUGH_PUBLISH, ("wait_ci", None), ("implement", 2))
    view = phase_view(DECLARATION, reports, "running", None)
    assert view.current == "implement"
    states = _states(view)
    assert states["implement"] == "current"
    assert states["review_diff"] == "redo"
    assert states["wait_ci"] == "pending"
    assert _labels(view)["implement"] == "round 2 of 3"


def test_a_second_wait_after_the_fix_returns_the_view_to_wait_ci() -> None:
    """The fix's publish ends the turn again, so the platform reports the wait again."""

    reports = _reports_of(
        *_THROUGH_PUBLISH, ("wait_ci", None), ("implement", 2), ("publish", None), ("wait_ci", None)
    )
    view = phase_view(DECLARATION, reports, "running", None)
    assert view.current == "wait_ci"
    states = _states(view)
    assert states["implement"] == "done"
    assert states["publish"] == "done"
    assert states["wait_ci"] == "current"
