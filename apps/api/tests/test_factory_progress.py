"""Phase reports from the sandbox's ``report_progress`` tool (#3077).

The runner POSTs ``/v1/work-item-progress/{request_id}`` with a request-bound
``work_item.progress`` sandbox token. The api validates the report against the
declaration the bundle sent, stores it on the WorkItem's active request, and
derives the phase view the status comment and the card render from.

Admission is a signed issues webhook against create_app(), as in
test_factory_terminus.py. Machine fixtures drive the events.
"""

from __future__ import annotations

import json
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
STAGED_DECLARATION: dict[str, Any] = {
    **DECLARATION,
    "reviewer_model": "anthropic/claude-opus-5.5",
    "stages": [
        {"id": "plan", "label": "Plan", "phases": ["read_issue", "pin_criteria", "plan"]},
        {"id": "plan_review", "label": "Plan review", "phases": ["plan_review"]},
        {"id": "implement", "label": "Implement", "phases": ["failing_test", "implement"]},
        {"id": "review_diff", "label": "Review diff", "phases": ["review_diff", "publish"]},
        {"id": "wait_ci", "label": "Wait for CI", "phases": ["wait_ci"]},
    ],
    "loops": [
        *DECLARATION["loops"],
        {"start": "implement", "review": "wait_ci", "cap": 3},
    ],
}
ACTIVITY: dict[str, Any] = {
    "model": "glm-5.3-flash",
    "turns": 14,
    "tool_calls": 37,
    "last_tool": "Bash",
}
PYTHON_COMMAND = "uv run pytest unitconv/tests -q"
NOT_DECLARED: dict[str, Any] = {
    "check": None,
    "command": None,
    "outcome": "not_declared",
    "exit_status": None,
    "missing_binaries": [],
    "blocked_services": [],
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


def verification(
    client: Any,
    request_id: uuid.UUID,
    body: dict[str, Any],
    *,
    token: str | None = None,
    path_id: uuid.UUID | None = None,
) -> Any:
    return client.post(
        f"/v1/work-item-progress/{path_id or request_id}/verification",
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
        (
            "a",
            {
                "declaration": {
                    "phases": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}],
                    "stages": [{"id": "only", "label": "Only", "phases": ["a"]}],
                }
            },
        ),
        (
            "a",
            {
                "declaration": {
                    "phases": [{"id": "a", "label": "A"}],
                    "stages": [{"id": "only", "label": "Only", "phases": ["a", "a"]}],
                }
            },
        ),
        (
            "a",
            {
                "declaration": {
                    "phases": [{"id": "a", "label": "A"}],
                    "stages": [{"id": "only", "label": "Only", "phases": ["unknown"]}],
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
    github.advance_label_event(number)
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


# --- 5: runner verification preflight -----------------------------------------------


@pytest.mark.parametrize(
    "observation",
    [
        {
            "check": "python",
            "command": PYTHON_COMMAND,
            "outcome": "passed",
            "exit_status": 0,
            "missing_binaries": [],
            "blocked_services": [],
        },
        {
            "check": "python",
            "command": PYTHON_COMMAND,
            "outcome": "unavailable",
            "exit_status": None,
            "missing_binaries": ["uv"],
            "blocked_services": [],
        },
        {
            "check": "python",
            "command": PYTHON_COMMAND,
            "outcome": "failed",
            "exit_status": 1,
            "missing_binaries": [],
            "blocked_services": [],
        },
        {
            "check": "python",
            "command": "uv run pytest -q",
            "outcome": "passed",
            "exit_status": 0,
            "missing_binaries": [],
            "blocked_services": [],
        },
        {
            "check": "rust",
            "command": "cargo test --locked",
            "outcome": "unavailable",
            "exit_status": None,
            "missing_binaries": [],
            "blocked_services": ["package_registry"],
        },
        NOT_DECLARED,
    ],
    ids=[
        "python-passed",
        "python-unavailable",
        "python-failed",
        "other-declared-command",
        "rust-declared-command",
        "not-declared",
    ],
)
def test_runner_verification_is_stored_as_structured_evidence_before_model_progress(
    admitted: Any, observation: dict[str, Any]  # noqa: F811
) -> None:
    client, github, _sink = admitted
    number = 9711
    _label(client, github, number)
    request_id = _request(number)["id"]

    response = verification(client, request_id, observation)

    assert response.status_code == 201, response.text
    assert response.json() == {"recorded": True, "request_id": str(request_id)}
    expected_note = json.dumps(observation, sort_keys=True, separators=(",", ":"))
    assert len(expected_note.encode("utf-8")) <= 280
    assert _reports(request_id) == [
        {
            "phase": "verification_preflight",
            "note": expected_note,
            "loop_round": None,
        }
    ]

    progress_response = report(client, request_id, "implement")
    assert progress_response.status_code == 201, progress_response.text
    assert _reports(request_id) == [
        {
            "phase": "verification_preflight",
            "note": expected_note,
            "loop_round": None,
        },
        {"phase": "implement", "note": None, "loop_round": None},
    ]


def test_verification_token_is_bound_to_the_path_request(admitted: Any) -> None:  # noqa: F811
    client, github, _sink = admitted
    first_number = 9712
    second_number = 9713
    _label(client, github, first_number)
    first = _request(first_number)["id"]
    _label(client, github, second_number)
    second = _request(second_number)["id"]
    observation = {
        "check": "python",
        "command": PYTHON_COMMAND,
        "outcome": "passed",
        "exit_status": 0,
        "missing_binaries": [],
        "blocked_services": [],
    }

    response = verification(
        client, first, observation, token=progress_token(first), path_id=second
    )

    assert response.status_code == 401, response.text
    assert _reports(first) == []
    assert _reports(second) == []


@pytest.mark.parametrize(
    "observation",
    [
        {
            "check": "python",
            "outcome": "passed",
            "exit_status": 0,
            "missing_binaries": [],
            "blocked_services": [],
        },
        {
            "check": "python",
            "command": "   ",
            "outcome": "passed",
            "exit_status": 0,
            "missing_binaries": [],
            "blocked_services": [],
        },
        {
            "check": "python",
            "command": PYTHON_COMMAND,
            "outcome": "unknown",
            "exit_status": None,
            "missing_binaries": [],
            "blocked_services": [],
        },
        {
            "check": "python",
            "command": PYTHON_COMMAND,
            "outcome": "passed",
            "exit_status": 1,
            "missing_binaries": [],
            "blocked_services": [],
        },
        {
            "check": "python",
            "command": PYTHON_COMMAND,
            "outcome": "passed",
            "exit_status": 0,
            "missing_binaries": ["uv"],
            "blocked_services": [],
        },
        {
            "check": "python",
            "command": PYTHON_COMMAND,
            "outcome": "unavailable",
            "exit_status": 127,
            "missing_binaries": ["uv"],
            "blocked_services": [],
        },
        {
            "check": "python",
            "command": PYTHON_COMMAND,
            "outcome": "failed",
            "exit_status": None,
            "missing_binaries": [],
            "blocked_services": [],
        },
        {
            "check": "python",
            "command": PYTHON_COMMAND,
            "outcome": "failed",
            "exit_status": 0,
            "missing_binaries": [],
            "blocked_services": [],
        },
        {
            "check": "python",
            "command": PYTHON_COMMAND,
            "outcome": "unavailable",
            "exit_status": None,
            "missing_binaries": ["uv", "uv"],
            "blocked_services": [],
        },
        {
            "check": "python",
            "command": PYTHON_COMMAND,
            "outcome": "unavailable",
            "exit_status": None,
            "missing_binaries": [],
            "blocked_services": ["postgres", "postgres"],
        },
        {
            "check": "python",
            "command": PYTHON_COMMAND,
            "outcome": "passed",
            "exit_status": 0,
            "missing_binaries": [],
            "blocked_services": [],
            "model_result": "pretend success",
        },
        {
            "check": "python",
            "command": PYTHON_COMMAND,
            "outcome": "passed",
            "missing_binaries": [],
            "blocked_services": [],
        },
        {
            "check": "python",
            "command": PYTHON_COMMAND,
            "outcome": "passed",
            "exit_status": 0,
            "blocked_services": [],
        },
        {
            "check": "python",
            "command": PYTHON_COMMAND,
            "outcome": "passed",
            "exit_status": 0,
            "missing_binaries": [],
        },
        # #3521: the declared check id and the not_declared shape.
        {
            "command": PYTHON_COMMAND,
            "outcome": "passed",
            "exit_status": 0,
            "missing_binaries": [],
            "blocked_services": [],
        },
        {
            "check": None,
            "command": PYTHON_COMMAND,
            "outcome": "unavailable",
            "exit_status": None,
            "missing_binaries": ["uv"],
            "blocked_services": [],
        },
        {
            "check": "python",
            "command": None,
            "outcome": "passed",
            "exit_status": 0,
            "missing_binaries": [],
            "blocked_services": [],
        },
        {
            "check": "Python",
            "command": PYTHON_COMMAND,
            "outcome": "passed",
            "exit_status": 0,
            "missing_binaries": [],
            "blocked_services": [],
        },
        {
            "check": "1python",
            "command": PYTHON_COMMAND,
            "outcome": "passed",
            "exit_status": 0,
            "missing_binaries": [],
            "blocked_services": [],
        },
        {
            "check": "python",
            "command": "x" * 181,
            "outcome": "passed",
            "exit_status": 0,
            "missing_binaries": [],
            "blocked_services": [],
        },
        {**NOT_DECLARED, "command": PYTHON_COMMAND},
        {**NOT_DECLARED, "check": "python"},
        {**NOT_DECLARED, "exit_status": 0},
        {**NOT_DECLARED, "missing_binaries": ["uv"]},
        {**NOT_DECLARED, "blocked_services": ["postgres"]},
        {**NOT_DECLARED, "outcome": "unavailable"},
    ],
)
def test_malformed_verification_is_rejected_without_persisting_evidence(
    admitted: Any, observation: dict[str, Any]  # noqa: F811
) -> None:
    client, github, _sink = admitted
    number = 9714
    _label(client, github, number)
    request_id = _request(number)["id"]

    response = verification(client, request_id, observation)

    assert response.status_code == 422, response.text
    assert _reports(request_id) == []


def test_duplicate_verification_does_not_replace_the_first_observation(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, _sink = admitted
    number = 9715
    _label(client, github, number)
    request_id = _request(number)["id"]
    first_observation = {
        "check": "python",
        "command": PYTHON_COMMAND,
        "outcome": "unavailable",
        "exit_status": None,
        "missing_binaries": ["uv"],
        "blocked_services": [],
    }
    second_observation = {
        "check": "python",
        "command": PYTHON_COMMAND,
        "outcome": "passed",
        "exit_status": 0,
        "missing_binaries": [],
        "blocked_services": [],
    }

    first = verification(client, request_id, first_observation)
    duplicate = verification(client, request_id, second_observation)

    assert first.status_code == 201, first.text
    assert duplicate.status_code == 409, duplicate.text
    assert _reports(request_id) == [
        {
            "phase": "verification_preflight",
            "note": json.dumps(first_observation, sort_keys=True, separators=(",", ":")),
            "loop_round": None,
        }
    ]


def _declared(check: str, command: str) -> dict[str, Any]:
    return {
        "check": check,
        "command": command,
        "outcome": "passed",
        "exit_status": 0,
        "missing_binaries": [],
        "blocked_services": [],
    }


def _note(observation: dict[str, Any]) -> str:
    return json.dumps(observation, sort_keys=True, separators=(",", ":"))


def test_python_check_id_is_the_api_python_gate_key() -> None:
    from curie_api.factory_progress import PYTHON_CHECK_ID

    assert PYTHON_CHECK_ID == "python"


def test_two_declared_checks_are_recorded_on_one_request(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, _sink = admitted
    number = 9721
    _label(client, github, number)
    request_id = _request(number)["id"]
    python = _declared("python", PYTHON_COMMAND)
    rust = _declared("rust", "cargo test --locked")

    first = verification(client, request_id, python)
    second = verification(client, request_id, rust)

    assert first.status_code == 201, first.text
    assert second.status_code == 201, second.text
    assert _reports(request_id) == [
        {"phase": "verification_preflight", "note": _note(python), "loop_round": None},
        {"phase": "verification_preflight", "note": _note(rust), "loop_round": None},
    ]


@pytest.mark.parametrize(
    ("first_observation", "second_observation"),
    [
        (
            _declared("python", PYTHON_COMMAND),
            _declared("python", "uv run pytest -q"),
        ),
        (_declared("python", PYTHON_COMMAND), NOT_DECLARED),
        (NOT_DECLARED, _declared("python", PYTHON_COMMAND)),
        (NOT_DECLARED, NOT_DECLARED),
    ],
    ids=[
        "duplicate-check-id",
        "not-declared-after-a-check",
        "check-after-not-declared",
        "second-not-declared",
    ],
)
def test_conflicting_second_verification_is_rejected_like_a_duplicate(
    admitted: Any,  # noqa: F811
    first_observation: dict[str, Any],
    second_observation: dict[str, Any],
) -> None:
    client, github, _sink = admitted
    number = 9722
    _label(client, github, number)
    request_id = _request(number)["id"]

    first = verification(client, request_id, first_observation)
    conflicting = verification(client, request_id, second_observation)

    assert first.status_code == 201, first.text
    assert conflicting.status_code == 409, conflicting.text
    assert conflicting.json()["code"] == "verification_exists"
    assert _reports(request_id) == [
        {
            "phase": "verification_preflight",
            "note": _note(first_observation),
            "loop_round": None,
        }
    ]


def test_a_stale_request_cannot_record_verification_for_its_replacement(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, _sink = admitted
    number = 9716
    _label(client, github, number)
    first = _request(number)["id"]
    _fail(client, first)
    github.labels = [LABEL]
    github.advance_label_event(number)
    again = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))
    assert again.json()["status"] == "factory_admitted", again.text
    rows = _all_requests(number)
    assert [row["status"] for row in rows] == ["failed", "waiting"]
    second = rows[1]["id"]
    observation = {
        "check": "python",
        "command": PYTHON_COMMAND,
        "outcome": "unavailable",
        "exit_status": None,
        "missing_binaries": ["uv"],
        "blocked_services": [],
    }

    response = verification(client, first, observation)

    assert response.status_code == 409, response.text
    assert response.json()["code"] == "no_active_request"
    assert _reports(first) == []
    assert _reports(second) == []


def test_model_progress_cannot_spoof_the_reserved_verification_phase(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, _sink = admitted
    number = 9717
    _label(client, github, number)
    request_id = _request(number)["id"]
    declaration = {
        "phases": [{"id": "verification_preflight", "label": "Verification"}],
        "loops": [],
    }

    response = report(
        client,
        request_id,
        "verification_preflight",
        declaration=declaration,
    )

    assert response.status_code == 422, response.text
    assert _reports(request_id) == []


# --- 5b: delegated_to and the verification route (#3873) -----------------------------------

DELEGATED: dict[str, Any] = {
    "check": "integration",
    "command": "make integration",
    "outcome": "unavailable",
    "exit_status": None,
    "missing_binaries": [],
    "blocked_services": ["postgres"],
    "delegated_to": "integration-tests",
}
UNDELEGATED: dict[str, Any] = {k: v for k, v in DELEGATED.items() if k != "delegated_to"}


def _read_observations(request_id: uuid.UUID) -> Any:
    """Run the real reader in a fresh session; returns its list or raises its error."""

    import asyncio

    from curie_api.factory_progress import read_verification_observations
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

    async def go() -> Any:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                return await read_verification_observations(session, request_id)
        finally:
            await engine.dispose()

    return asyncio.run(go())


def test_a_delegated_unavailable_check_is_stored_canonically_and_routes_to_required_ci(
    admitted: Any,  # noqa: F811
) -> None:
    from curie_api.factory_progress import verification_route

    client, github, _sink = admitted
    number = 9731
    _label(client, github, number)
    request_id = _request(number)["id"]

    response = verification(client, request_id, DELEGATED)

    assert response.status_code == 201, response.text
    expected_note = json.dumps(DELEGATED, sort_keys=True, separators=(",", ":"))
    assert '"delegated_to":"integration-tests"' in expected_note
    assert len(expected_note.encode("utf-8")) <= 280
    assert _reports(request_id) == [
        {"phase": "verification_preflight", "note": expected_note, "loop_round": None}
    ]
    (observation,) = _read_observations(request_id)
    assert observation.delegated_to == "integration-tests"
    assert verification_route(observation) == "required_ci"


def test_an_undelegated_note_omits_the_key_and_a_null_key_is_not_canonical(
    admitted: Any,  # noqa: F811
) -> None:
    from curie_api.factory_progress import verification_route

    client, github, _sink = admitted
    number = 9732
    _label(client, github, number)
    request_id = _request(number)["id"]

    response = verification(client, request_id, UNDELEGATED)

    assert response.status_code == 201, response.text
    (stored,) = _reports(request_id)
    assert stored["note"] == json.dumps(UNDELEGATED, sort_keys=True, separators=(",", ":"))
    assert "delegated_to" not in stored["note"]
    (observation,) = _read_observations(request_id)
    assert observation.delegated_to is None
    assert verification_route(observation) == "blocked"

    # A local import: test_factory_status_comment imports this module at load time.
    from test_factory_status_comment import _execute

    noncanonical = json.dumps(
        {**UNDELEGATED, "delegated_to": None}, sort_keys=True, separators=(",", ":")
    )
    _execute(
        "DELETE FROM curie.execution_request_phase_reports WHERE execution_request_id = :id",
        {"id": request_id},
    )
    _execute(
        "INSERT INTO curie.execution_request_phase_reports "
        "(execution_request_id, phase, note) VALUES (:id, 'verification_preflight', :note)",
        {"id": request_id, "note": noncanonical},
    )
    with pytest.raises(ValueError, match="not canonical"):
        _read_observations(request_id)


@pytest.mark.parametrize(
    "observation",
    [
        {**NOT_DECLARED, "delegated_to": "integration-tests"},
        {**DELEGATED, "delegated_to": "x" * 65},
        {**DELEGATED, "delegated_to": "integration`tests"},
        {**DELEGATED, "delegated_to": " integration-tests"},
        {**DELEGATED, "delegated_to": "integration-tests "},
        {**DELEGATED, "delegated_to": ""},
        {**DELEGATED, "delegated_to": "integration\ntests"},
    ],
    ids=[
        "not-declared",
        "65-characters",
        "backtick",
        "leading-space",
        "trailing-space",
        "empty",
        "control-character",
    ],
)
def test_a_bad_delegated_to_is_422_and_stores_nothing(
    admitted: Any, observation: dict[str, Any]  # noqa: F811
) -> None:
    client, github, _sink = admitted
    number = 9734
    _label(client, github, number)
    request_id = _request(number)["id"]

    response = verification(client, request_id, observation)

    assert response.status_code == 422, response.text
    assert _reports(request_id) == []


def test_a_64_character_delegated_to_is_accepted(admitted: Any) -> None:  # noqa: F811
    client, github, _sink = admitted
    number = 9735
    _label(client, github, number)
    request_id = _request(number)["id"]

    response = verification(client, request_id, {**DELEGATED, "delegated_to": "x" * 64})

    assert response.status_code == 201, response.text
    (observation,) = _read_observations(request_id)
    assert observation.delegated_to == "x" * 64


@pytest.mark.parametrize(
    ("observation", "route"),
    [
        (_declared("python", PYTHON_COMMAND), "sandbox"),
        (
            {**_declared("python", PYTHON_COMMAND), "outcome": "failed", "exit_status": 1},
            "sandbox",
        ),
        (DELEGATED, "required_ci"),
        (UNDELEGATED, "blocked"),
        (NOT_DECLARED, None),
    ],
    ids=["passed", "failed", "unavailable-delegated", "unavailable-undelegated", "not-declared"],
)
def test_verification_route_table(observation: dict[str, Any], route: str | None) -> None:
    from curie_api.factory_progress import VerificationObservation, verification_route

    assert verification_route(VerificationObservation.model_validate(observation)) == route


# --- 6 and 7: declaration pinning and the report limit -----------------------------------


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


# --- 8: the phase view (pure) -----------------------------------------------------------


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


def _stage_states(view: Any) -> dict[str, str]:
    return {stage.id: stage.state for stage in view.stages}


def _labels(view: Any) -> dict[str, str | None]:
    return {phase.id: phase.round_label for phase in view.phases}


def _loop(view: Any, start: str, review: str | None = None) -> Any:
    (loop,) = [
        loop
        for loop in view.loops
        if loop.start == start and (review is None or loop.review == review)
    ]
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


def test_an_out_of_order_report_keeps_the_furthest_phase_current() -> None:
    view = phase_view(
        DECLARATION,
        _reports_of(("implement", 1), ("plan", 1)),
        "running",
        None,
    )
    states = _states(view)
    assert view.current == "implement"
    assert states["plan"] == "done"
    assert states["implement"] == "current"


@pytest.mark.parametrize("declaration", [DECLARATION, STAGED_DECLARATION])
@pytest.mark.parametrize("status", ["running", "failed"])
def test_renewed_plan_review_preserves_completed_plan_and_diff_review_progress(
    declaration: dict[str, Any], status: str
) -> None:
    view = phase_view(
        declaration,
        _reports_of(
            ("plan", 1),
            ("plan_review", 1),
            ("failing_test", None),
            ("implement", 1),
            ("review_diff", 1),
            ("plan_review", 2),
        ),
        status,
        "runner_escalated" if status == "failed" else None,
    )

    assert view.current == "review_diff"
    states = _states(view)
    assert all(
        states[phase] == "done"
        for phase in (
            "read_issue", "pin_criteria", "plan", "plan_review", "failing_test", "implement"
        )
    )
    assert states["review_diff"] == "current"
    assert states["publish"] == states["wait_ci"] == "pending"
    assert _loop(view, "plan").approved
    assert not _loop(view, "plan").active
    stages = _stage_states(view)
    assert stages["plan"] == stages["plan_review"] == stages["implement"] == "done"
    assert stages["review_diff"] == ("blocked" if status == "failed" else "current")


@pytest.mark.parametrize("status", ["failed", "expired", "cancelled"])
@pytest.mark.parametrize(
    ("entries", "returned_phase"),
    [
        ((("plan", 1), ("plan_review", 1), ("plan", 2)), "plan"),
        ((("implement", 1), ("review_diff", 1), ("implement", 2)), "implement"),
        (
            (
                ("implement", 1),
                ("review_diff", 1),
                ("publish", None),
                ("wait_ci", None),
                ("implement", 2),
            ),
            "implement",
        ),
    ],
)
def test_terminal_loop_return_keeps_the_returned_phase_selected(
    status: str, entries: tuple[tuple[str, int | None], ...], returned_phase: str
) -> None:
    view = phase_view(STAGED_DECLARATION, _reports_of(*entries), status, None)

    assert view.current == returned_phase
    assert _states(view)[returned_phase] == "current"
    assert _stage_states(view)[returned_phase] == (
        "blocked" if status in {"failed", "expired"} else "current"
    )


@pytest.mark.parametrize(
    ("phase", "stage"),
    [
        ("read_issue", "plan"),
        ("pin_criteria", "plan"),
        ("plan", "plan"),
        ("plan_review", "plan_review"),
        ("failing_test", "implement"),
        ("implement", "implement"),
        ("review_diff", "review_diff"),
        ("publish", "review_diff"),
        ("wait_ci", "wait_ci"),
    ],
)
def test_each_reported_phase_selects_its_declared_stage(phase: str, stage: str) -> None:
    view = phase_view(STAGED_DECLARATION, _reports_of((phase, None)), "running", None)
    assert view.current == phase
    assert [slot.id for slot in view.stages if slot.state == "current"] == [stage]


def test_a_declaration_without_stages_keeps_one_stage_per_phase() -> None:
    view = phase_view(DECLARATION, _reports_of(("pin_criteria", None)), "running", None)
    assert [(stage.id, stage.phase_ids) for stage in view.stages] == [
        (phase["id"], (phase["id"],)) for phase in DECLARATION["phases"]
    ]
    assert _stage_states(view)["pin_criteria"] == "current"


def test_second_plan_round_marks_plan_current_and_review_redo() -> None:
    view = phase_view(
        STAGED_DECLARATION,
        _reports_of(("plan", 1), ("plan_review", 1), ("plan", 2)),
        "running",
        None,
    )
    assert _stage_states(view) == {
        "plan": "current",
        "plan_review": "redo",
        "implement": "pending",
        "review_diff": "pending",
        "wait_ci": "pending",
    }
    assert (_loop(view, "plan").kickbacks, _loop(view, "plan").active) == (1, True)
    assert {stage.id: stage.round_label for stage in view.stages}["plan_review"] == "round 2 of 3"


def test_third_diff_round_keeps_plan_approved_and_badges_two_review_kickbacks() -> None:
    view = phase_view(
        STAGED_DECLARATION,
        _reports_of(
            ("plan", 1),
            ("plan_review", 1),
            ("plan", 2),
            ("plan_review", 2),
            ("implement", 1),
            ("review_diff", 1),
            ("implement", 2),
            ("review_diff", 2),
            ("implement", 3),
        ),
        "running",
        None,
    )
    assert _stage_states(view) == {
        "plan": "done",
        "plan_review": "done",
        "implement": "current",
        "review_diff": "redo",
        "wait_ci": "pending",
    }
    assert (_loop(view, "plan").approved, _loop(view, "plan").kickbacks) == (True, 1)
    assert (
        _loop(view, "implement", "review_diff").active,
        _loop(view, "implement", "review_diff").kickbacks,
    ) == (
        True,
        2,
    )
    assert {stage.id: stage.round_label for stage in view.stages}["plan_review"] == (
        "approved · 2 rounds"
    )


def test_ci_retry_waits_on_ci_without_counting_a_diff_review_kickback() -> None:
    view = phase_view(
        STAGED_DECLARATION,
        _reports_of(
            ("implement", 1),
            ("review_diff", 1),
            ("publish", None),
            ("wait_ci", None),
            ("implement", 2),
            ("review_diff", 2),
            ("publish", None),
            ("wait_ci", None),
        ),
        "running",
        None,
    )
    assert _stage_states(view)["wait_ci"] == "current"
    assert _stage_states(view)["review_diff"] == "done"
    assert _loop(view, "implement", "wait_ci").kickbacks == 1
    assert _loop(view, "implement", "review_diff").kickbacks == 0
    assert {stage.id: stage.round_label for stage in view.stages}["wait_ci"] == "round 2 of 3"


def test_diff_review_after_a_ci_retry_uses_the_diff_round_on_implement() -> None:
    view = phase_view(
        STAGED_DECLARATION,
        _reports_of(
            ("implement", 1),
            ("review_diff", 1),
            ("publish", None),
            ("wait_ci", None),
            ("implement", 2),
            ("review_diff", 2),
        ),
        "running",
        None,
    )
    labels = {stage.id: stage.round_label for stage in view.stages}
    assert view.current == "review_diff"
    assert _loop(view, "implement", "review_diff").active
    assert _loop(view, "implement", "wait_ci").kickbacks == 1
    assert labels["review_diff"] == "round 1 of 3"
    assert labels["implement"] == labels["review_diff"]


def test_a_ci_fix_keeps_diff_approved_while_ci_loop_is_live() -> None:
    view = phase_view(
        STAGED_DECLARATION,
        _reports_of(
            ("implement", 1),
            ("review_diff", 1),
            ("implement", 2),
            ("review_diff", 2),
            ("publish", None),
            ("wait_ci", None),
            ("implement", 3),
        ),
        "running",
        None,
    )
    diff = _loop(view, "implement", "review_diff")
    ci = _loop(view, "implement", "wait_ci")
    labels = {stage.id: stage.round_label for stage in view.stages}
    assert view.current == "implement"
    assert (diff.approved, diff.active, diff.kickbacks) == (True, False, 1)
    assert _stage_states(view)["review_diff"] == "done"
    assert labels["review_diff"] == "approved · 2 rounds"
    assert (ci.active, ci.kickbacks, ci.round) == (True, 1, 2)
    assert labels["wait_ci"] == "round 2 of 3"


def test_success_marks_every_declared_stage_done_and_approves_all_loops() -> None:
    view = phase_view(
        STAGED_DECLARATION,
        _reports_of(("plan", 1), ("plan_review", 1), ("implement", 1), ("wait_ci", None)),
        "completed",
        "completed",
    )
    assert view.current is None
    assert set(_stage_states(view).values()) == {"done"}
    assert all(loop.approved for loop in view.loops)


@pytest.mark.parametrize("status", ["failed", "expired"])
def test_terminal_failure_marks_the_current_stage_blocked(status: str) -> None:
    view = phase_view(STAGED_DECLARATION, _reports_of(("implement", 1)), status, "x")
    assert _stage_states(view)["implement"] == "blocked"
    assert _stage_states(view)["review_diff"] == "pending"


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
    "queued": ("QUEUED", "#9a6700", False),
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
