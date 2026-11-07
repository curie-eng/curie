"""Webhook is a hook-run source on the fire record, not on the schedule list."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

from curie_api.schemas.schedules import HookFireOut

ROOT = Path(__file__).resolve().parents[3]
FIRED_AT = datetime(2026, 11, 7, 16, 0, tzinfo=UTC)
SOURCE_DOCUMENTS = (
    ROOT / "apps/api/openapi.json",
    ROOT / "cli/schema/hook-fire.schema.json",
    ROOT / "cli/schema/baseline/hook-fire.schema.json",
)


def _source_enum(path: Path) -> list[str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if path.name == "openapi.json":
        enum = payload["components"]["schemas"]["HookFireOut"]["properties"]["source"]["enum"]
    else:
        enum = payload["$defs"]["record"]["properties"]["source"]["enum"]
    assert isinstance(enum, list), path
    return enum


def test_hook_fire_out_accepts_webhook_source() -> None:
    # ended_at is required on the model and stays null while the run is open.
    fired = HookFireOut(
        id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        agent="acme-bot",
        name="queue-ready",
        trigger="webhook",
        source="webhook",
        slot_utc=FIRED_AT,
        outcome=None,
        started_at=FIRED_AT,
        ended_at=None,
    )
    assert fired.source == "webhook"


def test_published_hook_fire_source_enums_include_webhook() -> None:
    for path in SOURCE_DOCUMENTS:
        assert "webhook" in _source_enum(path), path


def test_schedule_history_stays_schedule_and_manual() -> None:
    source = (ROOT / "apps/api/src/curie_api/routers/schedules.py").read_text(encoding="utf-8")
    assert 'latest.get((name, "schedule"))' in source
    assert 'latest.get((name, "manual"))' in source
    assert "webhook" not in source
