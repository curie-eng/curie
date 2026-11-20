"""The runner and API share one bounded run-handoff contract."""

import pytest
from curie_internal.factory_run_handoff import RunHandoff
from pydantic import ValidationError


def test_verification_requires_a_command_and_acceptance_criterion() -> None:
    verification = {
        "check": "Python tests",
        "command": "uv run pytest",
        "state": "passed",
        "source": "agent_reported",
        "acceptance_criteria": ["AC1"],
    }
    payload = {
        "next_owner": "agent",
        "next_action": "Review the result",
        "verification": [verification],
    }
    assert RunHandoff.model_validate(payload).verification[0].command == "uv run pytest"
    for change in (
        {key: value for key, value in verification.items() if key != "command"},
        {key: value for key, value in verification.items() if key != "acceptance_criteria"},
        {**verification, "acceptance_criteria": []},
        {**verification, "acceptance_criteria": ["AC1", "AC1"]},
    ):
        with pytest.raises(ValidationError):
            RunHandoff.model_validate({**payload, "verification": [change]})
