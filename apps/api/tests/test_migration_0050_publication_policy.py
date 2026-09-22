"""Migration 0050 adds the per-agent publication policy and keeps approve as the default."""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
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


def test_0050_defaults_existing_agents_to_approve_and_downgrades(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, "0049")
    agent_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"acme-bot-{agent_id.hex[:8]}"},
    )
    command.upgrade(config, "0050")
    row = _sql(
        "SELECT publication_policy, publication_policy_version, publication_draft, "
        "publication_branch_prefix FROM curie.agents WHERE id = :id",
        {"id": agent_id},
    )[0]
    assert row == {
        "publication_policy": "approve",
        "publication_policy_version": 1,
        "publication_draft": False,
        "publication_branch_prefix": None,
    }
    approval_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.approvals "
        "(id, agent_id, conversation_id, author, summary, reply_kind, reply_channel, "
        "reply_placeholder, dedupe_key, gate_kind, granted_tool, purpose) "
        "VALUES (:id, :agent_id, 'thread-policy', 'author', 'summary', 'thread', "
        "'C0EXAMPLE1', 'placeholder', :dedupe, 'permission', 'tool', 'publication')",
        {
            "id": approval_id,
            "agent_id": agent_id,
            "dedupe": f"policy-{approval_id.hex}",
        },
    )
    with pytest.raises(IntegrityError) as excinfo:
        _sql(
            "INSERT INTO curie.approval_audit_entries "
            "(id, approval_id, action, actor, decision, authorizer, authorized, "
            "principal_kind) "
            "VALUES (:id, :approval_id, 'resolved', 'x', 'approved', 'x', true, 'human')",
            {"id": uuid.uuid4(), "approval_id": approval_id},
        )
    assert "approval_audit_principal_kind_ck" in str(excinfo.value)
    with pytest.raises(IntegrityError) as prefix_error:
        _sql(
            "UPDATE curie.agents SET publication_branch_prefix = 'factory.lock/' "
            "WHERE id = :id",
            {"id": agent_id},
        )
    assert "agents_publication_branch_prefix_ck" in str(prefix_error.value)
    command.downgrade(config, "0049")
    missing = _sql(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'agents' "
        "AND column_name = 'publication_policy'"
    )
    assert missing == []
    command.upgrade(config, "head")
