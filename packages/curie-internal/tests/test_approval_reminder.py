"""Reminder metadata is closed, versioned and identifies one due value."""

import json
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from curie_internal.approval_reminder import ApprovalReminderV1
from pydantic import ValidationError


def event(**changes):
    return {
        "version": 1,
        "approval_id": "00000000-0000-0000-0000-000000000001",
        "claimed_due_at": "2026-11-08T01:00:00Z",
        **changes,
    }


@pytest.mark.parametrize(
    "changes",
    [
        {"version": True},
        {"version": 2},
        {"version": "1"},
        {"destination": "untrusted"},
        {"approval_id": "not-a-uuid"},
        {"claimed_due_at": "2026-11-08T01:00:00"},
        {"claimed_due_at": 1},
    ],
)
def test_invalid_reminder_metadata_is_refused(changes):
    with pytest.raises(ValidationError):
        ApprovalReminderV1.model_validate_json(json.dumps(event(**changes)))


def test_due_identity_survives_serialization_and_changes_each_interval():
    first = ApprovalReminderV1.model_validate_json(json.dumps(event()))
    assert uuid.UUID(first.delivery_id)
    assert (
        ApprovalReminderV1.model_validate_json(first.model_dump_json()).delivery_id
        == first.delivery_id
    )
    later = first.model_copy(
        update={"claimed_due_at": first.claimed_due_at + timedelta(seconds=60)}
    )
    assert later.delivery_id != first.delivery_id
    offset = ApprovalReminderV1.model_validate_json(
        json.dumps(event(claimed_due_at="2026-11-08T02:00:00+01:00"))
    )
    assert offset.delivery_id == first.delivery_id
    assert first.claimed_due_at == datetime(2026, 11, 8, 1, tzinfo=UTC)
