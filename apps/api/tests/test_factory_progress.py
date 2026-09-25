"""Phase reports from the sandbox's ``report_progress`` tool (#3077).

The runner POSTs ``/v1/work-item-progress/{request_id}`` with a request-bound
``work_item.progress`` sandbox token. The api validates the report against the
declaration the bundle sent, stores it on the WorkItem's active request, and
derives the phase view the status comment and the card render from.

Admission is a signed issues webhook against create_app(), as in
test_factory_terminus.py. Machine fixtures drive the events.
"""

from __future__ import annotations

import re
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api import sandbox_token
from curie_api.config import get_settings
from curie_api.factory_progress import phase_view, pill_for
from curie_api.models import ExecutionRequestPhaseReport
from test_factory_terminus import (  # noqa: F401  (fixtures)
    _label,
    _request,
    _rows,
    _start_running,
    admitted,
    comments,
)
from test_github_factory_ingress import LABEL, _issue_event, _post

pytestmark = pytest.mark.usefixtures("clean_db")

MODELS = Path(__file__).resolve().parents[1] / "src" / "curie_api" / "models.py"
WORKER = {"X-Curie-Worker-Token": "factory-terminus-worker"}

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
ACTIVITY: dict[str, Any] = {
    "model": "glm-5.3-flash",
    "turns": 14,
    "tool_calls": 37,
    "last_tool": "Bash",
}


def progress_token(
    request_id: uuid.UUID, *, scope: str = "work_item.progress", exp: int | None = None
) -> str:
    return sandbox_token.mint(
        get_settings().api_key,
        agent=str(request_id),
        scope=scope,
        exp=exp if exp is not None else int(time.time()) + 3600,
    )


def report(
    client: Any,
    request_id: uuid.UUID,
    phase: str,
    *,
    round: int | None = None,  # noqa: A002 (wire field name)
    note: str | None = None,
    declaration: dict[str, Any] | None = None,
    activity: dict[str, Any] | None = None,
    token: str | None = None,
    path_id: uuid.UUID | None = None,
    extra: dict[str, Any] | None = None,
) -> Any:
    body: dict[str, Any] = {
        "phase": phase,
        "declaration": declaration or DECLARATION,
        "activity": activity or ACTIVITY,
    }
    if round is not None:
        body["round"] = round
    if note is not None:
        body["note"] = note
    body.update(extra or {})
    return client.post(
        f"/v1/work-item-progress/{path_id or request_id}",
        headers={"X-API-Key": token if token is not None else progress_token(request_id)},
        json=body,
    )


def _reports(request_id: uuid.UUID) -> list[dict[str, Any]]:
    return _rows(
        "SELECT phase, note, loop_round FROM curie.execution_request_phase_reports "
        "WHERE execution_request_id = :id ORDER BY id",
        {"id": request_id},
    )


def _status_row(request_id: uuid.UUID) -> dict[str, Any]:
    rows = _rows(
        "SELECT declaration, activity, card_token FROM curie.factory_terminal_notices "
        "WHERE execution_request_id = :id",
        {"id": request_id},
    )
    assert len(rows) == 1, rows
    return rows[0]


def _all_requests(number: int) -> list[dict[str, Any]]:
    return _rows(
        "SELECT r.id, r.status FROM curie.execution_requests r "
        "JOIN curie.work_items w ON w.id = r.work_item_id "
        "WHERE w.github_issue_number = :number ORDER BY r.sequence",
        {"number": number},
    )


# --- 1: storage ---------------------------------------------------------------


def test_a_valid_report_is_stored_on_its_request(admitted: Any) -> None:  # noqa: F811
    client, github, _sink = admitted
    number = 9701
    _label(client, github, number)
    request_id = _request(number)["id"]

    response = report(client, request_id, "plan", round=2, note="Drafting the plan")

    assert response.status_code == 201, response.text
    assert response.json() == {"recorded": True, "request_id": str(request_id)}
    assert _reports(request_id) == [
        {"phase": "plan", "note": "Drafting the plan", "loop_round": 2}
    ]
    row = _status_row(request_id)
    assert row["declaration"] == DECLARATION
    assert row["activity"] == ACTIVITY


def test_a_report_without_note_or_round_stores_nulls_and_the_latest_activity_wins(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, _sink = admitted
    number = 9702
    _label(client, github, number)
    request_id = _request(number)["id"]
    assert report(client, request_id, "read_issue").status_code == 201
    later = {**ACTIVITY, "turns": 20, "tool_calls": 51, "last_tool": "get_issue"}
    assert report(client, request_id, "pin_criteria", activity=later).status_code == 201
    assert _reports(request_id) == [
        {"phase": "read_issue", "note": None, "loop_round": None},
        {"phase": "pin_criteria", "note": None, "loop_round": None},
    ]
    assert _status_row(request_id)["activity"] == later


# --- 2: authentication ----------------------------------------------------------


def test_every_wrong_credential_is_401_and_stores_nothing(admitted: Any) -> None:  # noqa: F811
    client, github, _sink = admitted
    number = 9703
    _label(client, github, number)
    request_id = _request(number)["id"]
    good = progress_token(request_id)
    tampered = good[:-2] + ("AA" if not good.endswith("AA") else "BB")
    wrong = {
        "bad signature": tampered,
        "state.app scope": progress_token(request_id, scope="state.app"),
        "state scope": progress_token(request_id, scope="state"),
        "another request": progress_token(uuid.uuid4()),
        "expired": progress_token(request_id, exp=int(time.time()) - 60),
        "platform key": get_settings().api_key,
        "empty": "",
    }
    for name, token in wrong.items():
        response = report(client, request_id, "read_issue", token=token)
        assert response.status_code == 401, (name, response.text)
    missing = client.post(
        f"/v1/work-item-progress/{request_id}",
        json={"phase": "read_issue", "declaration": DECLARATION},
    )
    assert missing.status_code == 401, missing.text
    assert _reports(request_id) == []


def test_a_progress_token_does_not_authenticate_on_the_state_router(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, _sink = admitted
    number = 9704
    _label(client, github, number)
    request_id = _request(number)["id"]
    agent_id = _rows(
        "SELECT w.agent_id FROM curie.work_items w JOIN curie.execution_requests r "
        "ON r.work_item_id = w.id WHERE r.id = :id",
        {"id": request_id},
    )[0]["agent_id"]
    for agent in (agent_id, request_id):
        response = client.get(
            f"/agents/{agent}/state/workflow/step",
            headers={"X-API-Key": progress_token(agent)},
        )
        assert response.status_code in {401, 403}, response.text


def test_a_token_for_a_request_that_does_not_exist_is_404(admitted: Any) -> None:  # noqa: F811
    client, _github, _sink = admitted
    ghost = uuid.uuid4()
    response = report(client, ghost, "read_issue")
    assert response.status_code == 404, response.text
    assert response.json()["code"] == "request_not_found"


# --- 3: validation ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("phase", "kwargs"),
    [
        ("explore_repo", {}),
        ("Read-Issue", {}),
        ("read_issue", {"round": 1}),
        ("plan", {"round": 4}),
        ("plan", {"round": 0}),
        ("plan", {"note": "x" * 281}),
        ("plan", {"note": "   "}),
        ("plan", {"extra": {"unexpected": True}}),
        ("plan", {"activity": {**ACTIVITY, "turns": -1}}),
        ("plan", {"activity": {**ACTIVITY, "tool_calls": 1_000_001}}),
        ("plan", {"activity": {**ACTIVITY, "model": "m" * 121}}),
        ("plan", {"activity": {**ACTIVITY, "surprise": 1}}),
        ("plan", {"declaration": {"phases": []}}),
        (
            "a",
            {
                "declaration": {
                    "phases": [{"id": "a", "label": "A"}, {"id": "a", "label": "Again"}]
                }
            },
        ),
        ("a", {"declaration": {"phases": [{"id": "a", "label": "x" * 41}]}}),
        (
            "p0",
            {"declaration": {"phases": [{"id": f"p{i}", "label": "P"} for i in range(13)]}},
        ),
        (
            "a",
            {
                "declaration": {
                    "phases": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}],
                    "loops": [{"start": "b", "review": "a", "cap": 3}],
                }
            },
        ),
        (
            "a",
            {
                "declaration": {
                    "phases": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}],
                    "loops": [{"start": "a", "review": "b", "cap": 6}],
                }
            },
        ),
        (
            "a",
            {
                "declaration": {
                    "phases": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}],
                    "loops": [{"start": "a", "review": "zzz", "cap": 3}],
                }
            },
        ),
    ],
)
def test_an_invalid_report_is_422_and_stores_nothing(
    admitted: Any, phase: str, kwargs: dict[str, Any]  # noqa: F811
) -> None:
    client, github, _sink = admitted
    number = 9705
    _label(client, github, number)
    request_id = _request(number)["id"]
    response = report(client, request_id, phase, **kwargs)
    assert response.status_code == 422, response.text
    assert _reports(request_id) == []


def test_a_note_is_stripped_before_it_is_stored(admitted: Any) -> None:  # noqa: F811
    client, github, _sink = admitted
    number = 9706
    _label(client, github, number)
    request_id = _request(number)["id"]
    assert report(client, request_id, "plan", round=1, note="  Plan it  ").status_code == 201
    assert _reports(request_id)[0]["note"] == "Plan it"


# --- 4: the active request of the token's WorkItem ------------------------------------


def _fail(client: Any, request_id: uuid.UUID) -> None:
    epoch = _start_running(request_id)
    failed = client.post(
        f"/v1/internal/work-items/requests/{request_id}/finish",
        headers=WORKER,
        json={"runtime_epoch": epoch, "outcome": "failed", "cause": "runner_failed"},
    )
    assert failed.status_code == 200, failed.text


def test_a_failed_requests_token_cannot_write_onto_the_run_that_replaced_it(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, _sink = admitted
    number = 9707
    _label(client, github, number)
    first = _request(number)["id"]
    _fail(client, first)
    github.labels = [LABEL]
    again = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))
    assert again.json()["status"] == "factory_admitted", again.text
    rows = _all_requests(number)
    assert [r["status"] for r in rows] == ["failed", "waiting"]
    second = rows[1]["id"]

    response = report(client, first, "read_issue", token=progress_token(first))

    assert response.status_code == 409, response.text
    assert response.json()["code"] == "no_active_request"
    assert _reports(first) == []
    assert _reports(second) == []


def test_a_token_whose_work_item_has_no_active_request_is_409(admitted: Any) -> None:  # noqa: F811
    client, github, _sink = admitted
    number = 9708
    _label(client, github, number)
    request_id = _request(number)["id"]
    _fail(client, request_id)
    response = report(client, request_id, "read_issue")
    assert response.status_code == 409, response.text
    assert response.json()["code"] == "no_active_request"
    assert _reports(request_id) == []


# --- 5 and 6: declaration pinning and the report limit -----------------------------------


def test_a_changed_declaration_is_409(admitted: Any) -> None:  # noqa: F811
    client, github, _sink = admitted
    number = 9709
    _label(client, github, number)
    request_id = _request(number)["id"]
    assert report(client, request_id, "read_issue").status_code == 201
    changed = {**DECLARATION, "phases": [*DECLARATION["phases"][:-1]]}
    response = report(client, request_id, "read_issue", declaration=changed)
    assert response.status_code == 409, response.text
    assert response.json()["code"] == "declaration_changed"
    assert len(_reports(request_id)) == 1
    assert _status_row(request_id)["declaration"] == DECLARATION


def test_the_report_after_two_hundred_is_429(admitted: Any) -> None:  # noqa: F811
    import asyncio

    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    client, github, _sink = admitted
    number = 9710
    _label(client, github, number)
    request_id = _request(number)["id"]
    assert report(client, request_id, "read_issue").status_code == 201

    async def fill() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "INSERT INTO curie.execution_request_phase_reports "
                        "(execution_request_id, phase) "
                        "SELECT :id, 'read_issue' FROM generate_series(1, 199)"
                    ),
                    {"id": request_id},
                )
        finally:
            await engine.dispose()

    asyncio.run(fill())
    response = report(client, request_id, "pin_criteria")
    assert response.status_code == 429, response.text
    assert response.json()["code"] == "report_limit"
    assert len(_reports(request_id)) == 200


# --- 7: the phase view (pure) -----------------------------------------------------------


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


def _states(view: Any) -> dict[str, str]:
    return {phase.id: phase.state for phase in view.phases}


def _labels(view: Any) -> dict[str, str | None]:
    return {phase.id: phase.round_label for phase in view.phases}


def _loop(view: Any, start: str) -> Any:
    (loop,) = [loop for loop in view.loops if loop.start == start]
    return loop


def test_no_reports_means_no_current_phase_and_nothing_done() -> None:
    view = phase_view(DECLARATION, [], "running", None)
    assert view.current is None
    assert set(_states(view).values()) == {"pending"}
    assert all(loop.kickbacks == 0 for loop in view.loops)


def test_linear_progress_ticks_every_earlier_phase_even_a_skipped_one() -> None:
    view = phase_view(
        DECLARATION,
        _reports_of(("read_issue", None), ("plan", 1), ("plan_review", 1), ("failing_test", None)),
        "running",
        None,
    )
    assert view.current == "failing_test"
    states = _states(view)
    assert [states[p] for p in ("read_issue", "pin_criteria", "plan", "plan_review")] == [
        "done"
    ] * 4
    assert states["failing_test"] == "current"
    assert [states[p] for p in ("implement", "review_diff", "publish", "wait_ci")] == [
        "pending"
    ] * 4
    plan = _loop(view, "plan")
    assert (plan.kickbacks, plan.approved, plan.active) == (0, True, False)
    assert _labels(view)["plan_review"] == "approved, 1 round"


def test_a_plan_kickback_marks_the_review_redo_and_labels_round_two() -> None:
    view = phase_view(
        DECLARATION,
        _reports_of(("read_issue", None), ("plan", 1), ("plan_review", 1), ("plan", 2)),
        "running",
        None,
    )
    states = _states(view)
    assert states["plan"] == "current"
    assert states["plan_review"] == "redo"
    plan = _loop(view, "plan")
    assert (plan.round, plan.kickbacks, plan.active, plan.approved) == (2, 1, True, False)
    labels = _labels(view)
    assert labels["plan"] == labels["plan_review"] == "round 2 of 3"


def test_a_diff_review_approved_after_three_rounds_has_two_kickbacks() -> None:
    view = phase_view(
        DECLARATION,
        _reports_of(
            ("plan", 1),
            ("plan_review", 1),
            ("implement", 1),
            ("review_diff", 1),
            ("implement", 2),
            ("review_diff", 2),
            ("implement", 3),
            ("review_diff", 3),
            ("publish", None),
        ),
        "running",
        None,
    )
    loop = _loop(view, "implement")
    assert (loop.round, loop.kickbacks, loop.approved, loop.active) == (3, 2, True, False)
    assert _labels(view)["review_diff"] == "approved, 3 rounds"
    assert _states(view)["review_diff"] == "done"


def test_a_review_approved_in_one_round_draws_no_arc() -> None:
    view = phase_view(
        DECLARATION,
        _reports_of(("plan", 1), ("plan_review", 1), ("failing_test", None)),
        "running",
        None,
    )
    loop = _loop(view, "plan")
    assert loop.kickbacks == 0
    assert loop.approved is True
    assert _labels(view)["plan_review"] == "approved, 1 round"


def test_a_completed_request_is_all_done_with_no_current_marker() -> None:
    view = phase_view(
        DECLARATION, _reports_of(("read_issue", None), ("publish", None)), "completed", "completed"
    )
    assert view.current is None
    assert set(_states(view).values()) == {"done"}


def test_a_failed_request_keeps_its_current_phase_and_adds_no_ticks() -> None:
    view = phase_view(
        DECLARATION,
        _reports_of(("read_issue", None), ("plan", 1), ("plan_review", 1), ("implement", 1)),
        "failed",
        "runner_escalated",
    )
    assert view.current == "implement"
    states = _states(view)
    assert states["implement"] == "current"
    assert states["plan_review"] == "done"
    assert [states[p] for p in ("review_diff", "publish", "wait_ci")] == ["pending"] * 3


def test_an_out_of_order_report_takes_the_latest_as_current() -> None:
    view = phase_view(
        DECLARATION,
        _reports_of(("implement", 1), ("plan", 1)),
        "running",
        None,
    )
    states = _states(view)
    assert states["plan"] == "current"
    assert states["implement"] == "pending"


# --- 8: every request status has a pill ---------------------------------------------------


def _constraint_statuses() -> list[str]:
    source = MODELS.read_text()
    match = re.search(
        r'"status IS NOT NULL AND status IN "\s*((?:"[^"]*"\s*)+)\s*,?\s*'
        r'name="execution_requests_status_ck"',
        source,
    )
    assert match, "execution_requests_status_ck not found in models.py"
    joined = "".join(re.findall(r'"([^"]*)"', match.group(1)))
    statuses = re.findall(r"'([a-z_]+)'", joined)
    assert "cancellation_requested" in statuses
    return statuses


PILLS = {
    "waiting": ("QUEUED", "#9a6700", False),
    "running": ("RUNNING", "#2f81f7", True),
    "cancellation_requested": ("STOPPING", "#bc4c00", True),
    "completed": ("SUCCEEDED", "#1a7f37", False),
    "failed": ("FAILED", "#cf222e", False),
    "expired": ("EXPIRED", "#953800", False),
    "cancelled": ("CANCELLED", "#6e7781", False),
}


@pytest.mark.parametrize("status", _constraint_statuses())
def test_every_constraint_status_has_its_pill(status: str) -> None:
    assert status in PILLS, f"new status {status!r} needs a pill"
    assert pill_for(status, False) == PILLS[status]


def test_a_running_request_that_is_publishing_shows_publishing() -> None:
    assert pill_for("running", True) == ("PUBLISHING", "#8250df", True)


def test_an_unknown_status_shows_its_raw_value_in_grey() -> None:
    label, color, _live = pill_for("paused_for_audit", False)
    assert (label, color) == ("PAUSED_FOR_AUDIT", "#6e7781")
