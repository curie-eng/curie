"""Migration 0052 copies transcripts out of the capped state store (ADR-0170)."""

from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from typing import Any

from alembic import command
from alembic.config import Config
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"
THREAD = "slack:C0EXAMPLE1:1700000000.000100"


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


def _state_rows(agent_id: uuid.UUID) -> list[tuple[str, str]]:
    rows = _sql(
        "SELECT namespace, key FROM curie.workflow_state_entries WHERE agent_id = :a "
        "ORDER BY namespace, key",
        {"a": agent_id},
    )
    return [(row["namespace"], row["key"]) for row in rows]


def test_0052_moves_transcripts_and_downgrade_moves_them_back(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, "0051")
    agent_id = uuid.uuid4()
    transcript = [{"role": "user", "content": "fix the flaky test"}]
    try:
        _sql(
            "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
            {"id": agent_id, "name": f"acme-bot-{agent_id.hex[:8]}"},
        )
        for namespace, key, value, version in (
            ("transcript", THREAD, transcript, 3),
            ("memory", "facts", {"n": 1}, 1),
            ("workflow", "step", {"n": 2}, 1),
        ):
            _sql(
                "INSERT INTO curie.workflow_state_entries "
                "(id, agent_id, namespace, key, value, version) "
                "VALUES (:id, :a, :ns, :k, CAST(:v AS jsonb), :ver)",
                {
                    "id": uuid.uuid4(),
                    "a": agent_id,
                    "ns": namespace,
                    "k": key,
                    "v": json.dumps(value),
                    "ver": version,
                },
            )

        command.upgrade(config, "0052")
        moved = _sql(
            "SELECT thread_key, value, version, binding_scope FROM curie.thread_transcripts "
            "WHERE agent_id = :a",
            {"a": agent_id},
        )
        assert moved == [
            {"thread_key": THREAD, "value": transcript, "version": 3, "binding_scope": None}
        ]
        # Expand: the legacy row stays for an older API instance mid-rollout.
        assert _state_rows(agent_id) == [
            ("memory", "facts"),
            ("transcript", THREAD),
            ("workflow", "step"),
        ]
        # The new API then adopts the thread and deletes the legacy row; a
        # downgrade writes the adopted transcript back.
        _sql(
            "DELETE FROM curie.workflow_state_entries WHERE agent_id = :a "
            "AND namespace = 'transcript'",
            {"a": agent_id},
        )

        command.downgrade(config, "0051")
        assert _sql("SELECT to_regclass('curie.thread_transcripts') AS name")[0]["name"] is None
        assert _state_rows(agent_id) == [
            ("memory", "facts"),
            ("transcript", THREAD),
            ("workflow", "step"),
        ]
    finally:
        # A failed assertion must not leave this private database below head.
        command.upgrade(config, "head")
