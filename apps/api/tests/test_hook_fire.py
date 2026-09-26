"""POST /agents/{agent}/hooks/{name}/fire claims a run and queues the turn (#2932)."""

from __future__ import annotations

import asyncio
import io
import json
import tarfile
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import redis.asyncio as redis
from aci_protocol import STREAM_PAYLOAD_FIELD
from curie_api.config import get_settings
from curie_api.killswitch import kill_key
from curie_api.models import HookRun
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


def _cron(name: str, schedule: str) -> dict[str, str]:
    return {
        "type": "cron",
        "name": name,
        "schedule": schedule,
        "prompt": "Run the scheduled check.",
    }


def _bundle(root: Path, triggers: list[dict[str, Any]]) -> Path:
    inner = root / "b"
    (inner / ".claude-plugin").mkdir(parents=True, exist_ok=True)
    (inner / ".claude-plugin" / "plugin.json").write_text(
        json.dumps(
            {
                "name": "acme-bot",
                "version": "0.1.0",
                "description": "t",
                "triggers": triggers,
            }
        ),
        encoding="utf-8",
    )
    (inner / "skills" / "acme-bot").mkdir(parents=True, exist_ok=True)
    (inner / "skills" / "acme-bot" / "SKILL.md").write_text(
        "---\nname: acme-bot\ndescription: t\n---\nhi\n", encoding="utf-8"
    )
    return root


def _archive(root: Path) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as archive:
        archive.add(root / "b", arcname="acme-bot")
    return buf.getvalue()


def _publish(
    client: Any, headers: dict[str, str], archive: bytes, agent_name: str, *, channel: str
) -> tuple[str, str]:
    agent = client.post(
        "/agents",
        json={"name": agent_name, "channel": {"kind": "slack", "address": channel}},
        headers=headers,
    )
    assert agent.status_code == 201, agent.text
    agent_id = str(agent.json()["id"])
    version = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": "v1", "created_by": "acme"},
        headers=headers,
    )
    assert version.status_code == 201, version.text
    version_id = str(version.json()["id"])
    upload = client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        files={"file": ("acme-bot.tar.gz", archive)},
        headers=headers,
    )
    assert upload.status_code == 201, upload.text
    return agent_id, version_id


def _deploy(
    client: Any, headers: dict[str, str], agent_id: str, version_id: str, environment: str
) -> None:
    response = client.post(
        "/deployments",
        json={"agent_id": agent_id, "version_id": version_id, "environment": environment},
        headers=headers,
    )
    assert response.status_code == 201, response.text


def _insert_run(agent_id: str, version_id: str, name: str, slot: datetime) -> None:
    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            sessions = async_sessionmaker(engine, expire_on_commit=False)
            async with sessions() as session:
                session.add(
                    HookRun(
                        id=uuid.uuid4(),
                        agent_id=uuid.UUID(agent_id),
                        name=name,
                        slot_utc=slot,
                        version_id=uuid.UUID(version_id),
                        outcome=None,
                        started_at=slot,
                        ended_at=None,
                    )
                )
                await session.commit()
        finally:
            await engine.dispose()

    asyncio.run(run())


def _fire(client: Any, headers: dict[str, str], agent: str, name: str) -> Any:
    return client.post(f"/agents/{agent}/hooks/{name}/fire", headers=headers)


def _stream_payloads() -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        settings = get_settings()
        client = redis.from_url(settings.valkey_dsn())
        try:
            rows = await client.xrevrange(settings.runs_stream, count=30)
        finally:
            await client.aclose()
        found: list[dict[str, Any]] = []
        for _entry_id, fields in rows:
            raw = fields.get(STREAM_PAYLOAD_FIELD) or fields.get(b"payload")
            if raw is None:
                continue
            if isinstance(raw, bytes):
                raw = raw.decode()
            found.append(json.loads(raw))
        return found

    return asyncio.run(run())


def _payloads_for(name: str) -> list[dict[str, Any]]:
    return [
        item
        for item in _stream_payloads()
        if isinstance(item.get("hook_run"), dict) and item["hook_run"].get("name") == name
    ]


def test_fire_queues_one_turn_and_a_second_fire_is_skipped(
    tmp_path: Any, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    root = _bundle(tmp_path, [_cron("nightly-cleanup", "0 9 * * *")])
    agent_id, version_id = _publish(
        client, auth_headers, _archive(root), "acme-fire", channel="C0EXAMPLE1"
    )
    _deploy(client, auth_headers, agent_id, version_id, "dev")

    first = _fire(client, auth_headers, "acme-fire", "nightly-cleanup")
    assert first.status_code == 200, first.text
    body = first.json()
    assert body["outcome"] is None
    assert body["name"] == "nightly-cleanup"
    assert body["trigger"] == "cron"
    queued = _payloads_for("nightly-cleanup")
    assert len(queued) == 1
    assert queued[0]["source"] == "cron"
    assert queued[0]["text"] == "Run the scheduled check."
    assert queued[0]["hook_run"]["slot_utc"]

    second = _fire(client, auth_headers, "acme-fire", "nightly-cleanup")
    assert second.status_code == 200, second.text
    assert second.json()["outcome"] == "skipped"
    assert len(_payloads_for("nightly-cleanup")) == 1

    fetched = client.get(
        f"/agents/acme-fire/hooks/nightly-cleanup/runs/{body['id']}",
        headers=auth_headers,
    )
    assert fetched.status_code == 200, fetched.text
    assert fetched.json()["outcome"] is None


def test_unknown_hook_and_in_flight_and_kill_do_not_queue(
    tmp_path: Any, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    root = _bundle(tmp_path, [_cron("nightly-cleanup", "0 9 * * *")])
    agent_id, version_id = _publish(
        client, auth_headers, _archive(root), "acme-fire-neg", channel="C0EXAMPLE1"
    )
    _deploy(client, auth_headers, agent_id, version_id, "dev")

    missing = _fire(client, auth_headers, "acme-fire-neg", "not-a-hook")
    assert missing.status_code == 404, missing.text

    before = len(_payloads_for("nightly-cleanup"))

    async def kill() -> None:
        settings = get_settings()
        connection = redis.from_url(settings.valkey_dsn())
        try:
            await connection.set(kill_key(uuid.UUID(agent_id)), "1")
        finally:
            await connection.aclose()

    asyncio.run(kill())
    blocked = _fire(client, auth_headers, "acme-fire-neg", "nightly-cleanup")
    assert blocked.status_code == 200, blocked.text
    assert blocked.json()["outcome"] == "blocked"
    assert len(_payloads_for("nightly-cleanup")) == before


def test_open_row_skips_without_a_new_turn(
    tmp_path: Any, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    root = _bundle(tmp_path, [_cron("nightly-cleanup", "0 9 * * *")])
    agent_id, version_id = _publish(
        client, auth_headers, _archive(root), "acme-fire-open", channel="C0EXAMPLE1"
    )
    _deploy(client, auth_headers, agent_id, version_id, "dev")
    _insert_run(
        agent_id,
        version_id,
        "nightly-cleanup",
        datetime(2026, 9, 25, 9, 0, tzinfo=UTC),
    )
    before = len(_payloads_for("nightly-cleanup"))
    fired = _fire(client, auth_headers, "acme-fire-open", "nightly-cleanup")
    assert fired.status_code == 200, fired.text
    assert fired.json()["outcome"] == "skipped"
    assert len(_payloads_for("nightly-cleanup")) == before
