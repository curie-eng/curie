"""Reminder-enabled routes reach the existing approval target parser."""

import json
from pathlib import Path

import pytest
from curie_worker.kernel.approval import _parse_approval_targets

VECTORS = json.loads(
    (
        Path(__file__).resolve().parents[3] / "tests/vectors/approval-route-reminders.json"
    ).read_text()
)


@pytest.mark.parametrize("interval", VECTORS["valid"])
def test_reminder_route_remains_bound(interval: int | None) -> None:
    assert _parse_approval_targets(
        {
            "resolution": {"kind": "slack", "address": "C0EXAMPLE1"},
            "remind_every_s": interval,
        }
    ) == (("slack", "C0EXAMPLE1"), None)


@pytest.mark.parametrize("interval", VECTORS["invalid"])
def test_malformed_reminder_route_is_refused(interval: object) -> None:
    assert (
        _parse_approval_targets(
            {
                "resolution": {"mode": "requesting_surface"},
                "remind_every_s": interval,
            }
        )
        is None
    )
