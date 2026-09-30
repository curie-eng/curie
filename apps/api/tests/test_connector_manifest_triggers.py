"""The connectors route returns plugin.json triggers as stored."""

from __future__ import annotations

import io
import json
import tarfile
from pathlib import Path
from typing import Any

from curie_api.bundles import read_manifest_triggers

RELEASE = "acme-rel"
NAMESPACE = "acme-ns"
APP_NAME = "curie"
CHANNEL = "C0EXAMPLE1"

DAILY_DIGEST = [
    {
        "type": "cron",
        "name": "daily-digest",
        "schedule": "0 9 * * 1-5",
        "timezone": "America/Los_Angeles",
        "target": CHANNEL,
        "prompt": "Summarize overnight work.",
    }
]

TWO_FIELD_CRON = [{"type": "cron", "schedule": "0 9 * * 1-5"}]


def _bundle(root: Path, triggers: list[dict[str, Any]] | None = None) -> Path:
    inner = root / "b"
    (inner / ".claude-plugin").mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {
        "name": "acme-bot",
        "version": "0.1.0",
        "description": "t",
    }
    if triggers is not None:
        manifest["triggers"] = triggers
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
    client: Any, headers: dict[str, str], archive: bytes, agent_name: str
) -> tuple[str, str]:
    agent = client.post(
        "/agents",
        json={
            "name": agent_name,
            "channel": {"kind": "slack", "address": CHANNEL},
        },
        headers=headers,
    )
    assert agent.status_code == 201, agent.text
    agent_id = agent.json()["id"]
    version = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": "v1", "created_by": "acme"},
        headers=headers,
    )
    assert version.status_code == 201, version.text
    version_id = version.json()["id"]
    upload = client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        files={"file": ("acme-bot.tar.gz", archive)},
        headers=headers,
    )
    assert upload.status_code == 201, upload.text
    return str(agent_id), str(version_id)


def _connectors(
    client: Any, headers: dict[str, str], agent_id: str, version_id: str
) -> dict[str, Any]:
    response = client.get(
        f"/agents/{agent_id}/versions/{version_id}/connectors",
        params={"release": RELEASE, "namespace": NAMESPACE, "app_name": APP_NAME},
        headers=headers,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert isinstance(body, dict)
    return body


def test_connectors_returns_the_stored_trigger_and_version_id(
    tmp_path: Path, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, version_id = _publish(
        client, auth_headers, _archive(_bundle(tmp_path, DAILY_DIGEST)), "acme-bot"
    )
    body = _connectors(client, auth_headers, agent_id, version_id)
    assert body["triggers"] == DAILY_DIGEST
    assert body["version_id"] == version_id
    assert "manifests" in body


def test_connectors_returns_an_empty_trigger_list_when_the_key_is_absent(
    tmp_path: Path, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    root = _bundle(tmp_path)
    manifest = json.loads((root / "b" / ".claude-plugin" / "plugin.json").read_text())
    assert "triggers" not in manifest
    agent_id, version_id = _publish(
        client, auth_headers, _archive(root), "acme-bot-empty"
    )
    body = _connectors(client, auth_headers, agent_id, version_id)
    assert body["triggers"] == []
    assert body["version_id"] == version_id


def test_two_field_cron_is_rejected_at_upload_and_unread_on_disk(
    tmp_path: Path, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Upload rejects the legacy shape. The reader still returns that list
    # unchanged when the file is already on disk, and does not add fields.
    root = _bundle(tmp_path, TWO_FIELD_CRON)
    assert read_manifest_triggers(root / "b") == TWO_FIELD_CRON
    agent = client.post(
        "/agents",
        json={
            "name": "acme-bot-legacy",
            "channel": {"kind": "slack", "address": CHANNEL},
        },
        headers=auth_headers,
    )
    assert agent.status_code == 201, agent.text
    agent_id = agent.json()["id"]
    version = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": "v1", "created_by": "acme"},
        headers=auth_headers,
    )
    assert version.status_code == 201, version.text
    version_id = version.json()["id"]
    upload = client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        files={"file": ("acme-bot.tar.gz", _archive(root))},
        headers=auth_headers,
    )
    assert upload.status_code == 422, upload.text
    detail = upload.json()["detail"]
    assert detail["detail"] == "bundle failed validation"
    codes = {item["code"] for item in detail["errors"]}
    assert "triggers.cron_missing_name" in codes
    assert "triggers.cron_missing_prompt" in codes
