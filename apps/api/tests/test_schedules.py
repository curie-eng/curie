"""GET /schedules lists each in-force cron hook and its newest slot (#2933)."""

from __future__ import annotations

import asyncio
import io
import json
import tarfile
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from curie_api.config import get_settings
from curie_api.models import HookRun
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

CHANNEL = "C0EXAMPLE1"
NIGHTLY = datetime(2026, 9, 25, 9, 0, tzinfo=UTC)
NIGHTLY_OLDER = (
    datetime(2026, 9, 22, 9, 0, tzinfo=UTC),
    datetime(2026, 9, 23, 9, 0, tzinfo=UTC),
    datetime(2026, 9, 24, 9, 0, tzinfo=UTC),
)
WEEKLY = datetime(2026, 9, 25, 20, 0, tzinfo=UTC)


def _cron(
    name: str,
    schedule: str,
    *,
    timezone: str | None = None,
) -> dict[str, str]:
    item = {
        "type": "cron",
        "name": name,
        "schedule": schedule,
        "prompt": "Run the scheduled check.",
    }
    if timezone is not None:
        item["timezone"] = timezone
    return item


def _bundle(root: Path, triggers: list[dict[str, Any]]) -> Path:
    inner = root / "b"
    (inner / ".claude-plugin").mkdir(parents=True, exist_ok=True)
    manifest = {
        "name": "acme-bot",
        "version": "0.1.0",
        "description": "t",
        "triggers": triggers,
    }
    (inner / ".claude-plugin" / "plugin.json").write_text(
        json.dumps(manifest), encoding="utf-8"
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
    client: Any,
    headers: dict[str, str],
    archive: bytes,
    agent_name: str,
    *,
    agent_id: str | None = None,
    version_label: str = "v1",
    channel: str = CHANNEL,
) -> tuple[str, str]:
    if agent_id is None:
        agent = client.post(
            "/agents",
            json={
                "name": agent_name,
                "channel": {"kind": "slack", "address": channel},
            },
            headers=headers,
        )
        assert agent.status_code == 201, agent.text
        agent_id = str(agent.json()["id"])
    version = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": version_label, "created_by": "acme"},
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
    client: Any,
    headers: dict[str, str],
    agent_id: str,
    version_id: str,
    environment: str,
) -> None:
    response = client.post(
        "/deployments",
        json={
            "agent_id": agent_id,
            "version_id": version_id,
            "environment": environment,
        },
        headers=headers,
    )
    assert response.status_code == 201, response.text


def _insert_run(
    agent_id: str,
    version_id: str,
    name: str,
    slot: datetime,
    outcome: str | None,
) -> None:
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
                        outcome=outcome,
                        started_at=slot,
                        ended_at=None if outcome is None else slot,
                    )
                )
                await session.commit()
        finally:
            await engine.dispose()

    asyncio.run(run())


def _set_bundle_ref(version_id: str, bundle_ref: str) -> None:
    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE curie.agent_versions SET bundle_ref = :bundle_ref "
                        "WHERE id = :version_id"
                    ),
                    {"bundle_ref": bundle_ref, "version_id": uuid.UUID(version_id)},
                )
        finally:
            await engine.dispose()

    asyncio.run(run())


def _schedules(client: Any, headers: dict[str, str], agent: str | None = None) -> Any:
    params = {} if agent is None else {"agent": agent}
    return client.get("/schedules", params=params, headers=headers)


def _schedule_action(
    client: Any,
    headers: dict[str, str],
    agent_id: str,
    hook_name: str,
    action: str,
) -> Any:
    return client.post(
        f"/schedules/{agent_id}/{hook_name}/{action}", headers=headers
    )


def _body(response: Any) -> dict[str, Any]:
    assert response.status_code == 200, response.text
    body = response.json()
    assert isinstance(body, dict)
    return body


def _agent(body: dict[str, Any], name: str) -> dict[str, Any]:
    matches = [row for row in body["schedules"] if row["agent"] == name]
    assert len(matches) == 1, body
    return matches[0]


def _hook(row: dict[str, Any], name: str) -> dict[str, Any]:
    matches = [hook for hook in row["hooks"] if hook["name"] == name]
    assert len(matches) == 1, row
    return matches[0]


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def test_three_failed_nights_are_the_newest_outcome(
    tmp_path: Path, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    triggers = [
        _cron("nightly-cleanup", "0 9 * * *"),
        _cron("weekly-report", "0 16 * * FRI", timezone="America/New_York"),
    ]
    agent_id, version_id = _publish(
        client,
        auth_headers,
        _archive(_bundle(tmp_path, triggers)),
        "acme-nights",
    )
    _deploy(client, auth_headers, agent_id, version_id, "dev")
    _insert_run(agent_id, version_id, "nightly-cleanup", NIGHTLY_OLDER[0], "ran")
    for slot in (*NIGHTLY_OLDER[1:], NIGHTLY):
        _insert_run(agent_id, version_id, "nightly-cleanup", slot, "failed")
    _insert_run(agent_id, version_id, "weekly-report", WEEKLY, "ran")

    body = _body(_schedules(client, auth_headers))
    row = _agent(body, "acme-nights")
    assert row["agent_id"] == agent_id
    assert row["bundle_error"] is None
    assert [hook["name"] for hook in row["hooks"]] == [
        "nightly-cleanup",
        "weekly-report",
    ]
    nightly = _hook(row, "nightly-cleanup")
    assert nightly["trigger"] == "cron"
    assert nightly["schedule"] == "0 9 * * *"
    assert nightly["zone"] == "UTC"
    assert _instant(nightly["last_fire_at"]) == NIGHTLY
    assert nightly["last_outcome"] == "failed"
    weekly = _hook(row, "weekly-report")
    assert weekly["zone"] == "America/New_York"
    assert weekly["schedule"] == "0 16 * * FRI"
    assert _instant(weekly["last_fire_at"]) == WEEKLY
    assert weekly["last_outcome"] == "ran"


def test_a_hook_with_no_rows_is_listed_without_an_outcome(
    tmp_path: Path, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, version_id = _publish(
        client,
        auth_headers,
        _archive(_bundle(tmp_path, [_cron("nightly-cleanup", "0 9 * * *")])),
        "acme-quiet",
    )
    _deploy(client, auth_headers, agent_id, version_id, "dev")

    hook = _hook(_agent(_body(_schedules(client, auth_headers)), "acme-quiet"), "nightly-cleanup")
    assert hook["last_fire_at"] is None
    assert hook["last_outcome"] is None
    assert hook["zone"] == "UTC"


def test_a_webhook_beside_a_cron_hook_is_not_a_schedule(
    tmp_path: Path, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    triggers = [
        _cron("nightly-cleanup", "0 9 * * *"),
        {"type": "webhook", "name": "inbound", "path": "/inbound", "prompt": "Take the event."},
    ]
    agent_id, version_id = _publish(
        client,
        auth_headers,
        _archive(_bundle(tmp_path, triggers)),
        "acme-mixed",
    )
    _deploy(client, auth_headers, agent_id, version_id, "dev")

    row = _agent(_body(_schedules(client, auth_headers)), "acme-mixed")
    assert [hook["name"] for hook in row["hooks"]] == ["nightly-cleanup"]


def test_prod_deployment_hides_the_dev_hook(
    tmp_path: Path, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, dev_version = _publish(
        client,
        auth_headers,
        _archive(_bundle(tmp_path / "dev", [_cron("dev-only", "0 1 * * *")])),
        "acme-ranked",
        version_label="v-dev",
    )
    _prod_version = _publish(
        client,
        auth_headers,
        _archive(_bundle(tmp_path / "prod", [_cron("prod-nightly", "0 9 * * *")])),
        "acme-ranked",
        agent_id=agent_id,
        version_label="v-prod",
    )[1]
    _deploy(client, auth_headers, agent_id, dev_version, "dev")
    _deploy(client, auth_headers, agent_id, _prod_version, "prod")

    row = _agent(_body(_schedules(client, auth_headers)), "acme-ranked")
    assert [hook["name"] for hook in row["hooks"]] == ["prod-nightly"]


def test_agents_are_ordered_by_name(
    tmp_path: Path, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    for name, hook, channel in (
        ("acme-midden", "hook-m", "C0EXAMPLE1"),
        ("acme-alpha", "hook-a", "C0EXAMPLE2"),
    ):
        agent_id, version_id = _publish(
            client,
            auth_headers,
            _archive(_bundle(tmp_path / name, [_cron(hook, "0 9 * * *")])),
            name,
            channel=channel,
        )
        _deploy(client, auth_headers, agent_id, version_id, "dev")

    body = _body(_schedules(client, auth_headers))
    assert [row["agent"] for row in body["schedules"]] == ["acme-alpha", "acme-midden"]


def test_unknown_agent_is_404_and_an_undeployed_agent_has_no_hooks(
    tmp_path: Path, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    missing = _schedules(client, auth_headers, "missing-bot")
    assert missing.status_code == 404, missing.text

    _publish(
        client,
        auth_headers,
        _archive(_bundle(tmp_path, [_cron("nightly-cleanup", "0 9 * * *")])),
        "acme-undeployed",
    )
    row = _agent(_body(_schedules(client, auth_headers, "acme-undeployed")), "acme-undeployed")
    assert row["hooks"] == []
    assert row["bundle_error"] is None


def test_missing_api_key_matches_the_agents_list(client: Any) -> None:
    agents = client.get("/agents")
    schedules = client.get("/schedules")
    assert schedules.status_code == agents.status_code
    assert schedules.status_code != 200


def test_one_unreadable_bundle_does_not_hide_another_agents_failure(
    tmp_path: Path, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    shown_id, shown_version = _publish(
        client,
        auth_headers,
        _archive(_bundle(tmp_path / "shown", [_cron("nightly-cleanup", "0 9 * * *")])),
        "acme-shown",
    )
    _deploy(client, auth_headers, shown_id, shown_version, "dev")
    _insert_run(shown_id, shown_version, "nightly-cleanup", NIGHTLY, "failed")
    broken_id, broken_version = _publish(
        client,
        auth_headers,
        _archive(_bundle(tmp_path / "broken", [_cron("nightly-cleanup", "0 9 * * *")])),
        "acme-broken",
        channel="C0EXAMPLE2",
    )
    _deploy(client, auth_headers, broken_id, broken_version, "dev")
    _set_bundle_ref(broken_version, "missing/not-a-real-key")

    body = _body(_schedules(client, auth_headers))
    shown = _agent(body, "acme-shown")
    assert _hook(shown, "nightly-cleanup")["last_outcome"] == "failed"
    broken = _agent(body, "acme-broken")
    assert broken["bundle_error"] == "stored bundle could not be read"
    assert broken["hooks"] == []


def test_a_run_for_a_hook_the_bundle_no_longer_declares_is_omitted(
    tmp_path: Path, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, version_id = _publish(
        client,
        auth_headers,
        _archive(_bundle(tmp_path, [_cron("kept", "0 9 * * *")])),
        "acme-renamed",
    )
    _deploy(client, auth_headers, agent_id, version_id, "dev")
    _insert_run(agent_id, version_id, "retired", NIGHTLY, "failed")
    _insert_run(agent_id, version_id, "kept", NIGHTLY_OLDER[0], "ran")

    row = _agent(_body(_schedules(client, auth_headers)), "acme-renamed")
    assert [hook["name"] for hook in row["hooks"]] == ["kept"]
    assert _hook(row, "kept")["last_outcome"] == "ran"


def test_pause_and_resume_only_change_the_named_hook_for_the_named_agent(
    tmp_path: Path,
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    owner_id, owner_version = _publish(
        client,
        auth_headers,
        _archive(
            _bundle(
                tmp_path / "owner",
                [
                    _cron("nightly-cleanup", "0 9 * * *"),
                    _cron("weekly-report", "0 16 * * FRI"),
                ],
            )
        ),
        "acme-pause-owner",
        channel="C0EXAMPLE3",
    )
    _deploy(client, auth_headers, owner_id, owner_version, "dev")
    other_id, other_version = _publish(
        client,
        auth_headers,
        _archive(_bundle(tmp_path / "other", [_cron("nightly-cleanup", "0 8 * * *")])),
        "acme-pause-other",
        channel="C0EXAMPLE4",
    )
    _deploy(client, auth_headers, other_id, other_version, "dev")

    unauthenticated = _schedule_action(client, {}, owner_id, "nightly-cleanup", "pause")
    assert unauthenticated.status_code != 200

    paused = _schedule_action(
        client, auth_headers, owner_id, "nightly-cleanup", "pause"
    )
    assert paused.status_code == 200, paused.text
    assert paused.json()["paused"] is True

    body = _body(_schedules(client, auth_headers))
    owner = _agent(body, "acme-pause-owner")
    other = _agent(body, "acme-pause-other")
    assert _hook(owner, "nightly-cleanup")["paused"] is True
    assert _hook(owner, "weekly-report")["paused"] is False
    assert _hook(other, "nightly-cleanup")["paused"] is False

    resumed = _schedule_action(
        client, auth_headers, owner_id, "nightly-cleanup", "resume"
    )
    assert resumed.status_code == 200, resumed.text
    assert resumed.json()["paused"] is False

    body = _body(_schedules(client, auth_headers))
    owner = _agent(body, "acme-pause-owner")
    other = _agent(body, "acme-pause-other")
    assert _hook(owner, "nightly-cleanup")["paused"] is False
    assert _hook(owner, "weekly-report")["paused"] is False
    assert _hook(other, "nightly-cleanup")["paused"] is False
