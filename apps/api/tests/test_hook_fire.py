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

import pytest
import redis.asyncio as redis
from aci_protocol import STREAM_PAYLOAD_FIELD
from curie_api.config import get_settings
from curie_api.models import HookRun
from curie_internal.keyspace import kill_key
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


@pytest.fixture(autouse=True)
def _owned_runs_stream(valkey: Any) -> None:
    """Give each test its own runs stream, deleted on teardown.

    The assertions count queued turns by hook name, so a shared stream would
    also count turns left by earlier runs. ``valkey`` pulls in the conftest
    ``runs_stream`` fixture. Autouse fixtures are set up before ``client``,
    so the app reads the per-test stream name.
    """


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


def _insert_run(agent_id: str, version_id: str, name: str, slot: datetime) -> uuid.UUID:
    run_id = uuid.uuid4()

    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            sessions = async_sessionmaker(engine, expire_on_commit=False)
            async with sessions() as session:
                session.add(
                    HookRun(
                        id=run_id,
                        agent_id=uuid.UUID(agent_id),
                        name=name,
                        slot_utc=slot,
                        version_id=uuid.UUID(version_id),
                        source="schedule",
                        outcome=None,
                        started_at=slot,
                        ended_at=None,
                    )
                )
                await session.commit()
        finally:
            await engine.dispose()

    asyncio.run(run())
    return run_id


def _stored_source(run_id: str) -> str:
    async def read() -> str:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as connection:
                return str(
                    (
                        await connection.execute(
                            text("SELECT source FROM curie.hook_runs WHERE id = :id"),
                            {"id": uuid.UUID(run_id)},
                        )
                    ).scalar_one()
                )
        finally:
            await engine.dispose()

    return asyncio.run(read())


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
    assert body["source"] == "manual"
    assert _stored_source(body["id"]) == "manual"
    queued = _payloads_for("nightly-cleanup")
    assert len(queued) == 1
    assert queued[0]["source"] == "cron"
    assert queued[0]["text"] == "Run the scheduled check."
    assert queued[0]["hook_run"]["slot_utc"]

    second = _fire(client, auth_headers, "acme-fire", "nightly-cleanup")
    assert second.status_code == 200, second.text
    assert second.json()["outcome"] == "skipped"
    assert second.json()["reason"] == "run_in_flight"
    assert second.json()["source"] == "manual"
    assert _stored_source(second.json()["id"]) == "manual"
    assert body["reason"] is None
    assert len(_payloads_for("nightly-cleanup")) == 1

    fetched = client.get(
        f"/agents/acme-fire/hooks/nightly-cleanup/runs/{body['id']}",
        headers=auth_headers,
    )
    assert fetched.status_code == 200, fetched.text
    assert fetched.json()["outcome"] is None
    assert fetched.json()["source"] == "manual"


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
    assert blocked.json()["reason"] == "agent_killed"
    assert blocked.json()["source"] == "manual"
    assert _stored_source(blocked.json()["id"]) == "manual"
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
    assert fired.json()["reason"] == "run_in_flight"
    assert len(_payloads_for("nightly-cleanup")) == before


def _bind(agent_id: str, address: str, identity: str) -> None:
    """A Slack binding under a named identity, written below the API: this
    installation declares only the default identity, and the route key is the
    triple (ADR-0168 decision 3), so one address can carry several."""

    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO curie.agent_channels (id, agent_id, kind, address, adapter) "
                        "VALUES (:id, :agent, 'slack', :address, :identity)"
                    ),
                    {
                        "id": uuid.uuid4(),
                        "agent": uuid.UUID(agent_id),
                        "address": address,
                        "identity": identity,
                    },
                )
        finally:
            await engine.dispose()

    asyncio.run(run())


def test_a_target_bound_under_several_identities_fires_as_the_default_one(
    tmp_path: Any, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """A trigger names an address, never an identity, as the cron loop reads it."""
    root = _bundle(tmp_path, [{**_cron("identity-check", "0 9 * * *"), "target": "C0EXAMPLE1"}])
    agent_id, version_id = _publish(
        client, auth_headers, _archive(root), "acme-fire-identities", channel="C0EXAMPLE1"
    )
    _deploy(client, auth_headers, agent_id, version_id, "dev")
    _bind(agent_id, "C0EXAMPLE1", "second-bot")

    fired = _fire(client, auth_headers, "acme-fire-identities", "identity-check")
    assert fired.status_code == 200, fired.text
    assert fired.json()["outcome"] is None
    # This agent's own turn: the runs stream outlives a test, so an earlier
    # run's turn for the same hook name can still be on it.
    [queued] = [
        item
        for item in _payloads_for("identity-check")
        if item["event_id"].startswith(f"cron:{agent_id}:")
    ]
    handle = queued["reply_handle"]
    assert (handle["kind"], handle["channel"], handle["adapter"]) == (
        "slack",
        "C0EXAMPLE1",
        "default",
    )


def test_a_target_bound_under_several_identities_none_default_fails(
    tmp_path: Any, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Without the default identity among them, several routes are ambiguous."""
    root = _bundle(tmp_path, [{**_cron("ambiguous-check", "0 9 * * *"), "target": "C0EXAMPLE2"}])
    agent_id, version_id = _publish(
        client, auth_headers, _archive(root), "acme-fire-no-default", channel="C0EXAMPLE1"
    )
    _deploy(client, auth_headers, agent_id, version_id, "dev")
    _bind(agent_id, "C0EXAMPLE2", "second-bot")
    _bind(agent_id, "C0EXAMPLE2", "third-bot")

    fired = _fire(client, auth_headers, "acme-fire-no-default", "ambiguous-check")
    assert fired.status_code == 200, fired.text
    assert fired.json()["outcome"] == "failed"
    assert fired.json()["reason"] == "target_unbound"
    assert [
        item
        for item in _payloads_for("ambiguous-check")
        if item["event_id"].startswith(f"cron:{agent_id}:")
    ] == []


def _bind_email(agent_id: str, address: str) -> None:
    """A route-less email binding on the same address string, below the API."""

    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO curie.agent_channels (id, agent_id, kind, address) "
                        "VALUES (:id, :agent, 'email', :address)"
                    ),
                    {"id": uuid.uuid4(), "agent": uuid.UUID(agent_id), "address": address},
                )
        finally:
            await engine.dispose()

    asyncio.run(run())


def test_a_target_bound_under_two_kinds_fails_even_with_a_default_identity(
    tmp_path: Any, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The default-identity reading narrows several Slack identities only; an
    address bound under another kind too stays ambiguous, as the cron loop reads it."""
    root = _bundle(tmp_path, [{**_cron("kinds-check", "0 9 * * *"), "target": "C0EXAMPLE3"}])
    agent_id, version_id = _publish(
        client, auth_headers, _archive(root), "acme-fire-two-kinds", channel="C0EXAMPLE3"
    )
    _deploy(client, auth_headers, agent_id, version_id, "dev")
    _bind(agent_id, "C0EXAMPLE3", "second-bot")
    _bind_email(agent_id, "C0EXAMPLE3")

    fired = _fire(client, auth_headers, "acme-fire-two-kinds", "kinds-check")
    assert fired.status_code == 200, fired.text
    assert fired.json()["outcome"] == "failed"
    assert fired.json()["reason"] == "target_unbound"
    assert _payloads_for("kinds-check") == []


def test_spent_budget_records_blocked_budget_exhausted(
    tmp_path: Any, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    root = _bundle(tmp_path, [_cron("nightly-cleanup", "0 9 * * *")])
    agent_id, version_id = _publish(
        client, auth_headers, _archive(root), "acme-fire-budget", channel="C0EXAMPLE1"
    )
    _deploy(client, auth_headers, agent_id, version_id, "dev")

    async def spend() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text("UPDATE curie.agents SET max_usd_per_day = 0 WHERE id = :id"),
                    {"id": uuid.UUID(agent_id)},
                )
        finally:
            await engine.dispose()

    asyncio.run(spend())
    fired = _fire(client, auth_headers, "acme-fire-budget", "nightly-cleanup")
    assert fired.status_code == 200, fired.text
    assert fired.json()["outcome"] == "blocked"
    assert fired.json()["reason"] == "budget_exhausted"
    assert [
        item
        for item in _payloads_for("nightly-cleanup")
        if item["event_id"].startswith(f"cron:{agent_id}:")
    ] == []


def test_unbound_target_records_failed_target_unbound(
    tmp_path: Any, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    root = _bundle(tmp_path, [{**_cron("nightly-cleanup", "0 9 * * *"), "target": "C0EXAMPLE9"}])
    agent_id, version_id = _publish(
        client, auth_headers, _archive(root), "acme-fire-unbound", channel="C0EXAMPLE1"
    )
    _deploy(client, auth_headers, agent_id, version_id, "dev")
    fired = _fire(client, auth_headers, "acme-fire-unbound", "nightly-cleanup")
    assert fired.status_code == 200, fired.text
    assert fired.json()["outcome"] == "failed"
    assert fired.json()["reason"] == "target_unbound"
    assert [
        item
        for item in _payloads_for("nightly-cleanup")
        if item["event_id"].startswith(f"cron:{agent_id}:")
    ] == []


def test_reading_a_scheduled_run_preserves_its_source(
    tmp_path: Path, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    root = _bundle(tmp_path, [_cron("nightly-cleanup", "0 9 * * *")])
    agent_id, version_id = _publish(
        client, auth_headers, _archive(root), "acme-scheduled-record", channel="C0EXAMPLE1"
    )
    run_id = _insert_run(
        agent_id, version_id, "nightly-cleanup", datetime(2026, 9, 25, 9, 0, tzinfo=UTC)
    )
    fetched = client.get(
        f"/agents/acme-scheduled-record/hooks/nightly-cleanup/runs/{run_id}",
        headers=auth_headers,
    )
    assert fetched.status_code == 200, fetched.text
    assert fetched.json()["id"] == str(run_id)
    assert fetched.json()["source"] == "schedule"
    assert fetched.json()["outcome"] is None
