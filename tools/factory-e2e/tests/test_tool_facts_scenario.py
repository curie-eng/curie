"""Independent controls for the executable selected-image scenario assertions."""

from __future__ import annotations

from typing import Any

import pytest
from tool_facts_scenario import assert_tools


def sample(expected: dict[str, str]) -> dict[str, Any]:
    tools = [
        {
            "name": name,
            "state": state,
            "reason": "unreadable" if state == "unknown" else None,
            "basis": None if state == "unknown" else "selected_image_probe",
        }
        for name, state in expected.items()
    ]
    tools.append(
        {
            "name": "acme-outside-catalog",
            "state": "unknown",
            "reason": "outside_catalog",
            "basis": None,
        }
    )
    return {"executables": tools, "admission": "not_requested"}


@pytest.mark.parametrize("state", ["present", "absent", "unknown"])
def test_tools_accepts_the_observed_positive_negative_and_tag_control(state: str) -> None:
    expected = {"jq": state, "sh": "unknown" if state == "unknown" else "present"}
    assert_tools(sample(expected), expected)


@pytest.mark.parametrize("violation", ["state", "basis", "reason", "outside", "missing", "run"])
def test_tools_rejects_a_violating_answer(violation: str) -> None:
    expected = {"jq": "absent", "sh": "present"}
    answer = sample(expected)
    if violation == "state":
        answer["executables"][0]["state"] = "present"
    elif violation == "basis":
        answer["executables"][0]["basis"] = None
    elif violation == "reason":
        answer["executables"][0]["reason"] = "unreadable"
    elif violation == "outside":
        answer["executables"][-1]["state"] = "absent"
    elif violation == "missing":
        answer["executables"].pop()
    else:
        answer["admission"] = "requested"
    with pytest.raises(RuntimeError):
        assert_tools(answer, expected)
