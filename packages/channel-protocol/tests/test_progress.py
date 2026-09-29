"""The progress vocabulary and its three closed models (ADR-0130).

``ProgressCommand`` is what a model may submit through the platform's
``curie_progress`` tool. ``ProgressCard`` and ``ProgressMilestone`` are the
rendering-free payloads the platform sends adapters. Every refusal below is
proved by feeding the violating input to the model that will parse it, never by
reading its field list alone.
"""

from __future__ import annotations

import json
import sys
from functools import cache
from typing import Any

import pytest
from channel_protocol.progress import (
    MAX_PROGRESS_MILESTONES,
    PROGRESS_COMMAND_VERSION,
    TERMINAL_PROGRESS_STATES,
    MilestoneClass,
    ProgressCard,
    ProgressCommand,
    ProgressMilestone,
    ProgressState,
)
from pydantic import ValidationError

_ADR_STATES = [
    "queued",
    "investigating",
    "awaiting-approval",
    "preparing-workspace",
    "testing",
    "publishing",
    "complete",
    "failed",
    "cancelled",
]


def _command(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "version": "1.0",
        "update_id": "tests-started",
        "state": "testing",
        "summary": "Running the integration suite",
    }
    body.update(overrides)
    return body


def _only_error(exc: pytest.ExceptionInfo[ValidationError]) -> dict[str, Any]:
    errors = exc.value.errors()
    assert len(errors) == 1, errors
    return dict(errors[0])


# --- the vocabulary ---------------------------------------------------------


def test_the_states_are_exactly_the_adrs_nine_in_its_order() -> None:
    assert [state.value for state in ProgressState] == _ADR_STATES


def test_there_are_exactly_three_milestone_classes() -> None:
    # One per ADR-0130 section 3 class: intake or material evidence acquired,
    # a material hypothesis or scope change, a verification result.
    assert [klass.value for klass in MilestoneClass] == ["evidence", "scope", "verification"]


def test_the_terminal_states_are_complete_failed_and_cancelled() -> None:
    assert TERMINAL_PROGRESS_STATES == frozenset(
        {ProgressState.COMPLETE, ProgressState.FAILED, ProgressState.CANCELLED}
    )


def test_a_chain_gets_at_most_three_milestones() -> None:
    assert MAX_PROGRESS_MILESTONES == 3


# --- ProgressCommand --------------------------------------------------------


def test_a_command_round_trips() -> None:
    command = ProgressCommand.model_validate(_command(milestone="verification"))
    assert command.state is ProgressState.TESTING
    assert command.milestone is MilestoneClass.VERIFICATION
    assert ProgressCommand.model_validate_json(command.model_dump_json()) == command


def test_a_command_needs_no_milestone() -> None:
    assert ProgressCommand.model_validate(_command()).milestone is None


def test_the_command_version_is_pinned_and_required() -> None:
    assert PROGRESS_COMMAND_VERSION == "1.0"
    body = _command()
    del body["version"]
    with pytest.raises(ValidationError) as caught:
        ProgressCommand.model_validate(body)
    assert _only_error(caught)["type"] == "missing"
    with pytest.raises(ValidationError) as caught:
        ProgressCommand.model_validate(_command(version="2.0"))
    assert _only_error(caught)["type"] == "literal_error"


def test_the_command_carries_exactly_its_five_fields() -> None:
    assert set(ProgressCommand.model_fields) == {
        "version",
        "update_id",
        "state",
        "summary",
        "milestone",
    }
    schema = ProgressCommand.model_json_schema()
    assert schema["additionalProperties"] is False
    assert set(schema["properties"]) == set(ProgressCommand.model_fields)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        # ADR-0130 section 1: none of these is the model's to name. The worker
        # resolves the already-authorized reply target and owns every one.
        pytest.param("kind", "slack", id="channel-kind"),
        pytest.param("channel", "slack", id="channel"),
        pytest.param("address", "C0EXAMPLE1", id="channel-address"),
        pytest.param("conversation_id", "1720000000.000100", id="conversation"),
        pytest.param("target", {"kind": "slack"}, id="reply-target"),
        pytest.param("reply_ref", "1720000000.000200", id="reply-ref"),
        pytest.param("endpoint", "https://attacker.example.com/replies", id="endpoint"),
        pytest.param("adapter", "discord", id="adapter"),
        pytest.param("credential", "not-a-real-credential", id="credential"),
        pytest.param("secret", "not-a-real-secret", id="secret"),
        pytest.param("token", "not-a-real-token", id="token"),
        pytest.param("payload", {"blocks": []}, id="adapter-payload"),
        pytest.param("blocks", [{"type": "section"}], id="adapter-blocks"),
        pytest.param("delivery_id", "00000000-0000-4000-8000-000000000001", id="delivery-id"),
        pytest.param("progress_id", "prg-1", id="progress-record"),
        pytest.param("milestone_budget", 10, id="milestone-budget"),
        pytest.param("budget", 10, id="budget"),
        pytest.param("ordinal", 1, id="milestone-slot"),
    ],
)
def test_a_command_refuses_every_routing_credential_delivery_and_budget_field(
    field: str, value: object
) -> None:
    with pytest.raises(ValidationError) as caught:
        ProgressCommand.model_validate(_command(**{field: value}))
    error = _only_error(caught)
    assert error["type"] == "extra_forbidden"
    assert error["loc"] == (field,)


def test_a_command_refuses_a_forbidden_field_arriving_as_json() -> None:
    # The tool handler will hand the ingress JSON, not a dict, so the refusal
    # must hold on that path too.
    with pytest.raises(ValidationError) as caught:
        ProgressCommand.model_validate_json(
            json.dumps(_command(endpoint="https://attacker.example.com"))
        )
    assert _only_error(caught)["type"] == "extra_forbidden"


@pytest.mark.parametrize("state", ["done", "paused", "running", "COMPLETE", ""])
def test_a_command_refuses_a_state_outside_the_closed_set(state: str) -> None:
    with pytest.raises(ValidationError) as caught:
        ProgressCommand.model_validate(_command(state=state))
    assert _only_error(caught)["type"] == "enum"


@pytest.mark.parametrize("milestone", ["hypothesis", "intake", "approval", ""])
def test_a_command_refuses_a_milestone_class_outside_the_closed_set(milestone: str) -> None:
    with pytest.raises(ValidationError) as caught:
        ProgressCommand.model_validate(_command(milestone=milestone))
    assert _only_error(caught)["type"] == "enum"


@pytest.mark.parametrize(
    "update_id",
    ["a", "tests-started", "phase:2.retry_1", "0b9f5a2e-6c1d-4f3a-9e7b-2d8c4a1f6e30", "x" * 64],
)
def test_a_command_accepts_a_bounded_update_id(update_id: str) -> None:
    assert ProgressCommand.model_validate(_command(update_id=update_id)).update_id == update_id


@pytest.mark.parametrize(
    ("update_id", "error_type"),
    [
        pytest.param("", "string_pattern_mismatch", id="empty"),
        pytest.param("x" * 65, "string_pattern_mismatch", id="too-long"),
        pytest.param("tests started", "string_pattern_mismatch", id="space"),
        pytest.param("-leading", "string_pattern_mismatch", id="leading-punctuation"),
        pytest.param("tests/started", "string_pattern_mismatch", id="slash"),
        pytest.param("tests\nstarted", "string_pattern_mismatch", id="line-break"),
    ],
)
def test_a_command_refuses_an_unbounded_update_id(update_id: str, error_type: str) -> None:
    with pytest.raises(ValidationError) as caught:
        ProgressCommand.model_validate(_command(update_id=update_id))
    assert _only_error(caught)["type"] == error_type


# --- the summary, shared by all three models -----------------------------------


def test_a_summary_is_stripped() -> None:
    command = ProgressCommand.model_validate(_command(summary="  Running the suite \t"))
    assert command.summary == "Running the suite"


def test_a_summary_of_exactly_200_characters_is_accepted_after_stripping() -> None:
    command = ProgressCommand.model_validate(_command(summary=" " + "x" * 200 + " "))
    assert command.summary == "x" * 200


@pytest.mark.parametrize(
    ("summary", "error_type"),
    [
        pytest.param("", "string_too_short", id="empty"),
        pytest.param(" \t ", "string_too_short", id="blank"),
        pytest.param("x" * 201, "string_too_long", id="201-characters"),
    ],
)
def test_a_summary_is_1_to_200_characters(summary: str, error_type: str) -> None:
    with pytest.raises(ValidationError) as caught:
        ProgressCommand.model_validate(_command(summary=summary))
    assert _only_error(caught)["type"] == error_type


@cache
def _line_boundaries() -> list[str]:
    """Every character Python itself splits a line on, found by execution."""
    return [
        chr(code)
        for code in range(sys.maxunicode + 1)
        if len(f"a{chr(code)}b".splitlines()) > 1
    ]


def test_the_line_boundary_scan_finds_the_known_boundaries() -> None:
    # Pins the scan below: an empty list would make that test vacuous.
    assert {"\n", "\r", " ", " ", "\x85"} <= set(_line_boundaries())


@pytest.mark.parametrize("boundary", _line_boundaries(), ids=lambda ch: f"U+{ord(ch):04X}")
def test_a_summary_with_any_interior_line_boundary_is_refused(boundary: str) -> None:
    with pytest.raises(ValidationError) as caught:
        ProgressCommand.model_validate(_command(summary=f"Running tests{boundary}then publishing"))
    assert _only_error(caught)["type"] == "string_pattern_mismatch"


# --- ProgressCard -----------------------------------------------------------


def _card(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "kind": "card",
        "state": "testing",
        "summary": "Running the integration suite",
        "revision": 3,
        "terminal": False,
    }
    body.update(overrides)
    return body


def test_a_card_round_trips() -> None:
    card = ProgressCard.model_validate(_card())
    assert card.state is ProgressState.TESTING
    assert ProgressCard.model_validate_json(card.model_dump_json()) == card


@pytest.mark.parametrize("state", _ADR_STATES)
def test_a_cards_terminal_flag_must_match_its_state(state: str) -> None:
    terminal = ProgressState(state) in TERMINAL_PROGRESS_STATES
    assert ProgressCard.model_validate(_card(state=state, terminal=terminal)).terminal is terminal
    with pytest.raises(ValidationError) as caught:
        ProgressCard.model_validate(_card(state=state, terminal=not terminal))
    error = _only_error(caught)
    assert error["type"] == "value_error"
    assert "terminal must be true exactly when" in str(error["msg"])


@pytest.mark.parametrize("revision", [0, -1])
def test_a_card_revision_counts_from_one(revision: int) -> None:
    with pytest.raises(ValidationError) as caught:
        ProgressCard.model_validate(_card(revision=revision))
    assert _only_error(caught)["type"] == "greater_than_equal"


@pytest.mark.parametrize("field", ["kind", "state", "summary", "revision", "terminal"])
def test_every_card_field_is_required(field: str) -> None:
    body = _card()
    del body[field]
    with pytest.raises(ValidationError) as caught:
        ProgressCard.model_validate(body)
    assert "missing" in {error["type"] for error in caught.value.errors()}


def test_a_card_refuses_an_unmodelled_field() -> None:
    with pytest.raises(ValidationError) as caught:
        ProgressCard.model_validate(_card(blocks=[{"type": "section"}]))
    assert _only_error(caught)["type"] == "extra_forbidden"


def test_a_card_is_not_a_milestone() -> None:
    with pytest.raises(ValidationError) as caught:
        ProgressCard.model_validate(_card(kind="milestone"))
    assert _only_error(caught)["type"] == "literal_error"


# --- ProgressMilestone ------------------------------------------------------


def _milestone(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "kind": "milestone",
        "milestone": "evidence",
        "summary": "Found the failing migration in the deploy log",
        "ordinal": 1,
    }
    body.update(overrides)
    return body


@pytest.mark.parametrize("ordinal", [1, 2, 3])
def test_a_milestone_takes_one_of_the_three_slots(ordinal: int) -> None:
    milestone = ProgressMilestone.model_validate(_milestone(ordinal=ordinal))
    assert milestone.ordinal == ordinal
    assert ProgressMilestone.model_validate_json(milestone.model_dump_json()) == milestone


@pytest.mark.parametrize(
    ("ordinal", "error_type"),
    [(0, "greater_than_equal"), (4, "less_than_equal"), (-1, "greater_than_equal")],
)
def test_a_fourth_milestone_cannot_be_expressed(ordinal: int, error_type: str) -> None:
    with pytest.raises(ValidationError) as caught:
        ProgressMilestone.model_validate(_milestone(ordinal=ordinal))
    assert _only_error(caught)["type"] == error_type


@pytest.mark.parametrize("klass", ["evidence", "scope", "verification"])
def test_a_milestone_carries_one_class(klass: str) -> None:
    assert ProgressMilestone.model_validate(_milestone(milestone=klass)).milestone == klass


@pytest.mark.parametrize("field", ["kind", "milestone", "summary", "ordinal"])
def test_every_milestone_field_is_required(field: str) -> None:
    body = _milestone()
    del body[field]
    with pytest.raises(ValidationError) as caught:
        ProgressMilestone.model_validate(body)
    assert "missing" in {error["type"] for error in caught.value.errors()}


def test_a_milestone_refuses_an_unmodelled_field() -> None:
    with pytest.raises(ValidationError) as caught:
        ProgressMilestone.model_validate(_milestone(state="testing"))
    assert _only_error(caught)["type"] == "extra_forbidden"


def test_a_milestone_summary_follows_the_same_single_line_rule() -> None:
    with pytest.raises(ValidationError) as caught:
        ProgressMilestone.model_validate(_milestone(summary="Found it\nand fixed it"))
    assert _only_error(caught)["type"] == "string_pattern_mismatch"
