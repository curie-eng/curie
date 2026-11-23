"""phase_view over the dark-factory bundle's eight declared stages (#5715).

The status comment, the SVG card and the Slack progress card all draw their
steps from this view, so these cases load the bundle's real
``progress/phases.json`` and check each step's state through the review loops.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from curie_internal.factory_phase_view import PhaseView, phase_view

PHASES_JSON = (
    Path(__file__).resolve().parents[3] / "examples" / "dark-factory" / "progress" / "phases.json"
)
STAGE_IDS = [
    "read_issue",
    "investigate",
    "plan",
    "plan_review",
    "implement",
    "review_diff",
    "publish",
    "wait_ci",
]
FULL_RUN = (
    "read_issue",
    "investigate",
    "pin_criteria",
    "plan",
    "plan_review",
    "failing_test",
    "implement",
    "review_diff",
    "publish",
    "wait_ci",
)


@dataclass(frozen=True)
class Report:
    id: int | None
    phase: str
    loop_round: int | None


def _declaration() -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(PHASES_JSON.read_text())
    return loaded


def _view(status: str, *phases: str | tuple[str, int]) -> PhaseView:
    reports = []
    for index, entry in enumerate(phases):
        phase, loop_round = (entry, None) if isinstance(entry, str) else entry
        reports.append(Report(id=index + 1, phase=phase, loop_round=loop_round))
    return phase_view(_declaration(), reports, status, None)


def _stages(view: PhaseView) -> dict[str, str]:
    return {slot.id: slot.state for slot in view.stages}


def _round_labels(view: PhaseView) -> dict[str, str | None]:
    return {slot.id: slot.round_label for slot in view.stages}


def test_questions_only_run_ticks_only_reported_phases() -> None:
    reported = ("read_issue", "investigate", "pin_criteria", "plan")
    reports = [Report(index + 1, phase, None) for index, phase in enumerate(reported)]
    view = phase_view(_declaration(), reports, "completed", "questions_asked")

    states = {slot.id: slot.state for slot in view.phases}
    for phase in reported:
        assert states[phase] == "done"
    for phase in ("plan_review", "failing_test", "implement", "review_diff", "publish", "wait_ci"):
        assert states[phase] == "pending"
    assert view.current is None
    stages = _stages(view)
    assert stages["read_issue"] == "done"
    assert stages["investigate"] == "done"
    assert stages["plan"] == "done"
    assert stages["implement"] == "pending"
    assert stages["publish"] == "pending"
    assert stages["wait_ci"] == "pending"
    ordinary = _view("completed", *reported)
    assert {slot.state for slot in ordinary.phases} == {"done"}


def test_phase_view_declares_the_eight_bundle_stages_in_order() -> None:
    view = _view("running")

    assert [slot.id for slot in view.stages] == STAGE_IDS
    assert [slot.label for slot in view.stages] == [
        "Reading the issue",
        "Investigating the ticket",
        "Planning the change",
        "Reviewing the plan",
        "Writing the change",
        "Reviewing the diff",
        "Opening the pull request",
        "Waiting for CI",
    ]
    assert view.staged
    assert set(_stages(view).values()) == {"pending"}


def test_phase_view_completed_run_marks_all_eight_stages_done() -> None:
    view = _view("completed", *FULL_RUN)

    assert _stages(view) == dict.fromkeys(STAGE_IDS, "done")
    labels = _round_labels(view)
    assert labels["plan_review"] == "completed · 1 round"
    assert labels["review_diff"] == "completed · 1 round"
    assert labels["wait_ci"] == "approved · 1 round"


def test_phase_view_plan_round_two_redoes_the_plan_review() -> None:
    view = _view(
        "running",
        "read_issue",
        "investigate",
        "pin_criteria",
        ("plan", 1),
        ("plan_review", 1),
        ("plan", 2),
    )

    assert _stages(view) == {
        "read_issue": "done",
        "investigate": "done",
        "plan": "current",
        "plan_review": "redo",
        "implement": "pending",
        "review_diff": "pending",
        "publish": "pending",
        "wait_ci": "pending",
    }
    plan_loop = next(loop for loop in view.loops if loop.review == "plan_review")
    assert (plan_loop.stage_start, plan_loop.stage_review) == ("plan", "plan_review")
    assert plan_loop.kickbacks == 1
    assert plan_loop.active


def test_phase_view_second_diff_review_round_is_current_with_its_round() -> None:
    view = _view(
        "running",
        *FULL_RUN[:7],
        ("review_diff", 1),
        ("implement", 2),
        ("review_diff", 2),
    )

    assert _stages(view) == {
        "read_issue": "done",
        "investigate": "done",
        "plan": "done",
        "plan_review": "done",
        "implement": "done",
        "review_diff": "current",
        "publish": "pending",
        "wait_ci": "pending",
    }
    labels = _round_labels(view)
    assert labels["implement"] == labels["review_diff"] == "round 2 of 5"
    assert labels["publish"] is None
    diff_loop = next(loop for loop in view.loops if loop.review == "review_diff")
    assert (diff_loop.stage_start, diff_loop.stage_review) == ("implement", "review_diff")
    assert diff_loop.kickbacks == 1


def test_phase_view_diff_kickback_redoes_the_diff_review() -> None:
    view = _view("running", *FULL_RUN[:7], ("review_diff", 1), ("implement", 2))

    stages = _stages(view)
    assert stages["implement"] == "current"
    assert stages["review_diff"] == "redo"
    assert stages["publish"] == stages["wait_ci"] == "pending"


def test_phase_view_ci_failure_returns_to_implement() -> None:
    view = _view("running", *FULL_RUN, "implement")

    assert _stages(view) == {
        "read_issue": "done",
        "investigate": "done",
        "plan": "done",
        "plan_review": "done",
        "implement": "current",
        "review_diff": "done",
        "publish": "pending",
        "wait_ci": "redo",
    }
    labels = _round_labels(view)
    assert labels["wait_ci"] == "round 2 of 3"
    assert labels["review_diff"] == "completed · 1 round"
    ci_loop = next(loop for loop in view.loops if loop.review == "wait_ci")
    assert (ci_loop.stage_start, ci_loop.stage_review) == ("implement", "wait_ci")
    assert ci_loop.active
    assert ci_loop.kickbacks == 1


def test_phase_view_waiting_on_ci_has_the_seven_stages_before_it_done() -> None:
    view = _view("running", *FULL_RUN)

    stages = _stages(view)
    assert stages["wait_ci"] == "current"
    assert [stages[stage] for stage in STAGE_IDS[:-1]] == ["done"] * 7
    assert _round_labels(view)["wait_ci"] == "round 1 of 3"


@pytest.mark.parametrize("status", ["failed", "expired"])
def test_phase_view_failed_run_blocks_the_current_stage(status: str) -> None:
    view = _view(status, *FULL_RUN[:7])

    assert _stages(view) == {
        "read_issue": "done",
        "investigate": "done",
        "plan": "done",
        "plan_review": "done",
        "implement": "blocked",
        "review_diff": "pending",
        "publish": "pending",
        "wait_ci": "pending",
    }


def test_phase_view_never_shows_verification_preflight_as_a_stage() -> None:
    view = _view("running", "read_issue", "verification_preflight", "investigate")

    assert [slot.id for slot in view.stages] == STAGE_IDS
    assert all("verification_preflight" not in slot.phase_ids for slot in view.stages)
    assert view.current == "investigate"
    assert _stages(view)["investigate"] == "current"
    assert [state for state in _stages(view).values() if state != "pending"] == [
        "done",
        "current",
    ]


def test_phase_view_verification_preflight_last_keeps_the_prior_stage_current() -> None:
    view = _view("running", *FULL_RUN[:6], "verification_preflight")

    assert view.current == "failing_test"
    assert _stages(view)["implement"] == "current"
