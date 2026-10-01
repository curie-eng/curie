"""The work item event id grammar shared by the API producers and the worker (#3563)."""

from __future__ import annotations

import uuid

import pytest
from channel_protocol import work_item_events
from channel_protocol.work_item_events import (
    CI_FIRST_FIX_ROUND,
    CI_MAX_ROUNDS,
    CI_ROUND_KEY_PREFIX,
    WorkItemEventId,
    ci_event_id,
    ci_round_key,
    execute_event_id,
    parse_work_item_event_id,
    terminate_event_id,
)

CI_ROUNDS = range(CI_FIRST_FIX_ROUND, CI_MAX_ROUNDS + 1)


@pytest.mark.parametrize("generation", [1, 7, 12])
def test_execute_ids_round_trip(generation: int) -> None:
    request_id = uuid.uuid4()
    event_id = execute_event_id(request_id, generation)
    assert event_id == f"work-item-{request_id}-execute-{generation}"
    assert parse_work_item_event_id(event_id) == WorkItemEventId(request_id, "execute", generation)


def test_terminate_id_round_trips() -> None:
    request_id = uuid.uuid4()
    event_id = terminate_event_id(request_id)
    assert event_id == f"work-item-{request_id}-terminate"
    assert parse_work_item_event_id(event_id) == WorkItemEventId(request_id, "terminate", None)


@pytest.mark.parametrize("round_", CI_ROUNDS)
def test_ci_ids_round_trip_for_every_round_in_range(round_: int) -> None:
    request_id = uuid.uuid4()
    event_id = ci_event_id(request_id, round_)
    assert event_id == f"work-item-{request_id}-ci-{round_}"
    assert parse_work_item_event_id(event_id) == WorkItemEventId(request_id, "ci", round_)


def test_uppercase_uuid_hex_parses_to_the_same_uuid() -> None:
    request_id = uuid.uuid4()
    upper = str(request_id).upper()
    parsed = parse_work_item_event_id(f"work-item-{upper}-ci-{CI_FIRST_FIX_ROUND}")
    assert parsed == WorkItemEventId(request_id, "ci", CI_FIRST_FIX_ROUND)
    parsed = parse_work_item_event_id(f"work-item-{upper}-execute-1")
    assert parsed == WorkItemEventId(request_id, "execute", 1)


@pytest.mark.parametrize(
    "suffix",
    [
        f"ci-{CI_FIRST_FIX_ROUND - 1}",
        f"ci-{CI_MAX_ROUNDS + 1}",
        "ci-02",
        "ci-",
        "ci-2x",
        "ci",
        "cI-2",
        "execute-0",
        "execute-01",
        "execute-",
        "terminate-1",
    ],
)
def test_the_parser_rejects_out_of_range_or_malformed_suffixes(suffix: str) -> None:
    assert parse_work_item_event_id(f"work-item-{uuid.uuid4()}-{suffix}") is None


@pytest.mark.parametrize(
    "event_id",
    [
        "work-item-not-a-uuid-ci-2",
        "work-item-not-a-uuid-execute-1",
        "work-item-not-a-uuid-terminate",
        "slack-123",
        "",
        "github-1-ci-2",
    ],
)
def test_the_parser_rejects_other_namespaces_and_malformed_uuids(event_id: str) -> None:
    assert parse_work_item_event_id(event_id) is None


def test_the_parser_rejects_surrounding_whitespace() -> None:
    event_id = ci_event_id(uuid.uuid4(), CI_FIRST_FIX_ROUND)
    assert parse_work_item_event_id(f" {event_id}") is None
    assert parse_work_item_event_id(f"{event_id} ") is None
    assert parse_work_item_event_id(f"{event_id}\n") is None


def test_execute_builder_refuses_a_generation_below_one() -> None:
    with pytest.raises(ValueError):
        execute_event_id(uuid.uuid4(), 0)


@pytest.mark.parametrize("round_", [CI_FIRST_FIX_ROUND - 1, CI_MAX_ROUNDS + 1])
def test_the_ci_event_builder_refuses_a_round_outside_the_range(round_: int) -> None:
    with pytest.raises(ValueError):
        ci_event_id(uuid.uuid4(), round_)


def test_the_round_key_refuses_a_round_before_the_first_fix() -> None:
    with pytest.raises(ValueError):
        ci_round_key(uuid.uuid4(), CI_FIRST_FIX_ROUND - 1)


def test_the_round_key_accepts_the_lookahead_past_the_last_round() -> None:
    """The CI gate asks whether round N+1 is claimed even after the last round."""

    request_id = uuid.uuid4()
    key = ci_round_key(request_id, CI_MAX_ROUNDS + 1)
    assert key == f"curie:work-item:ci:{request_id}:{CI_MAX_ROUNDS + 1}"


@pytest.mark.parametrize("kind", ["execute", "ci"])
def test_an_oversized_number_is_rejected_not_raised(kind: str) -> None:
    assert parse_work_item_event_id(f"work-item-{uuid.uuid4()}-{kind}-{'9' * 5000}") is None
    assert parse_work_item_event_id(f"work-item-{uuid.uuid4()}-{kind}-{'1' * 19}") is None


@pytest.mark.parametrize("round_", CI_ROUNDS)
def test_ci_round_key_shape(round_: int) -> None:
    request_id = uuid.uuid4()
    key = ci_round_key(request_id, round_)
    assert key == f"curie:work-item:ci:{request_id}:{round_}"
    assert key.startswith(CI_ROUND_KEY_PREFIX)


def test_raising_the_bound_extends_builder_and_parser(monkeypatch: pytest.MonkeyPatch) -> None:
    request_id = uuid.uuid4()
    raised = CI_MAX_ROUNDS + 2
    with pytest.raises(ValueError):
        ci_event_id(request_id, raised)
    assert parse_work_item_event_id(f"work-item-{request_id}-ci-{raised}") is None

    monkeypatch.setattr(work_item_events, "CI_MAX_ROUNDS", raised)

    event_id = ci_event_id(request_id, raised)
    assert event_id == f"work-item-{request_id}-ci-{raised}"
    assert parse_work_item_event_id(event_id) == WorkItemEventId(request_id, "ci", raised)
    assert parse_work_item_event_id(f"work-item-{request_id}-ci-{raised + 1}") is None
    with pytest.raises(ValueError):
        ci_event_id(request_id, raised + 1)
