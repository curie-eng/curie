"""Migration 0040 preserves honest publication workspace identity history."""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import Any

from alembic import command
from alembic.config import Config
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"


def _config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(ALEMBIC_DIR))
    return config


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                result = await connection.execute(text(statement), params or {})
                if not result.returns_rows:
                    return []
                return [dict(row) for row in result.mappings().all()]
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _seed_legacy_publication() -> uuid.UUID:
    agent_id = uuid.uuid4()
    version_id = uuid.uuid4()
    deployment_id = uuid.uuid4()
    approval_id = uuid.uuid4()
    publication_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"migration-0040-{agent_id.hex}"},
    )
    _sql(
        "INSERT INTO curie.agent_versions "
        "(id, agent_id, version_label, bundle_ref, created_by) "
        "VALUES (:id, :agent_id, 'migration-0040', NULL, 'test')",
        {"id": version_id, "agent_id": agent_id},
    )
    _sql(
        "INSERT INTO curie.deployments "
        "(id, agent_id, version_id, environment, status) "
        "VALUES (:id, :agent_id, :version_id, "
        "CAST('prod' AS curie.environment), 'active')",
        {
            "id": deployment_id,
            "agent_id": agent_id,
            "version_id": version_id,
        },
    )
    _sql(
        "INSERT INTO curie.approvals "
        "(id, agent_id, conversation_id, author, summary, reply_kind, "
        "reply_channel, reply_placeholder, dedupe_key, purpose) "
        "VALUES (:id, :agent_id, '1700000000.000100', 'U0REQUEST1', "
        "'Publish changes?', 'slack', 'C0EXAMPLE1', '1700000000.000001', "
        ":dedupe_key, 'publication')",
        {
            "id": approval_id,
            "agent_id": agent_id,
            "dedupe_key": f"migration-0040-{approval_id.hex}",
        },
    )
    _sql(
        "INSERT INTO curie.publications "
        "(id, approval_id, deployment_id, repo_full_name, base_sha, patch_bytes, "
        "changed_paths, title, body, reply_kind, reply_channel, reply_placeholder) "
        "VALUES (:id, :approval_id, :deployment_id, 'acme-corp/acme-bot', "
        ":base_sha, :patch, CAST('[\"README.md\"]' AS jsonb), 'Update README', "
        "'Prepared by migration test.', 'slack', 'C0EXAMPLE1', "
        "'1700000000.000001')",
        {
            "id": publication_id,
            "approval_id": approval_id,
            "deployment_id": deployment_id,
            "base_sha": "a" * 40,
            "patch": b"diff --git a/README.md b/README.md\n",
        },
    )
    return publication_id


def test_0040_adds_nullable_identity_without_backfill_and_round_trips(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, "0039")
    publication_id = _seed_legacy_publication()

    command.upgrade(config, "0040")

    assert _sql(
        "SELECT is_nullable FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'publications' "
        "AND column_name = 'workspace_conversation_id'"
    ) == [{"is_nullable": "YES"}]
    assert _sql(
        "SELECT workspace_conversation_id FROM curie.publications WHERE id = :id",
        {"id": publication_id},
    ) == [{"workspace_conversation_id": None}]

    canonical = "slack:C0EXAMPLE1:1700000000.000100"
    _sql(
        "UPDATE curie.publications SET workspace_conversation_id = :identity "
        "WHERE id = :id",
        {"identity": canonical, "id": publication_id},
    )
    assert _sql(
        "SELECT workspace_conversation_id FROM curie.publications WHERE id = :id",
        {"id": publication_id},
    ) == [{"workspace_conversation_id": canonical}]

    command.downgrade(config, "0039")
    assert _sql(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'publications' "
        "AND column_name = 'workspace_conversation_id'"
    ) == []

    command.upgrade(config, "0040")
    assert _sql(
        "SELECT workspace_conversation_id FROM curie.publications WHERE id = :id",
        {"id": publication_id},
    ) == [{"workspace_conversation_id": None}]
