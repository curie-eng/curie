"""status_body renders the card or the checklist, never both (#3125)."""

from __future__ import annotations

import uuid

from curie_api.factory_notices import FINAL_MARKER, marker_for, status_body
from curie_api.factory_progress import PhaseSlot, PhaseView

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
