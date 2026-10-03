"""The worker's CI round bound is the shared one, not a local literal (#3563)."""

from __future__ import annotations

import uuid

import pytest
from curie_worker.workitem_dispatch import parse_work_item_event_id


def test_raising_the_shared_round_bound_extends_the_worker_parser(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_id = uuid.uuid4()
    assert parse_work_item_event_id(f"work-item-{request_id}-ci-5") is None

    monkeypatch.setattr("channel_protocol.work_item_events.CI_MAX_ROUNDS", 5)

    parsed = parse_work_item_event_id(f"work-item-{request_id}-ci-5")
    assert parsed is not None
    assert parsed.kind == "ci"
    assert parsed.request_id == request_id
    assert parsed.generation == 5
    assert parse_work_item_event_id(f"work-item-{request_id}-ci-6") is None
