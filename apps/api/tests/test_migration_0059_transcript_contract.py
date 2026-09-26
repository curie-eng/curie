"""Migration 0059 reconciles and removes legacy transcript state rows."""

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
SCOPE = "slack:C0EXAMPLE1"


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


def _seed_state(
    agent_id: uuid.UUID,
    scope: str | None,
    namespace: str,
    key: str,
    value: Any,
    version: int,
    age_hours: int,
) -> None:
    _sql(
        "INSERT INTO curie.workflow_state_entries "
        "(id, agent_id, binding_scope, namespace, key, value, version, updated_at) "
        "VALUES (:id, :agent, :scope, :namespace, :key, CAST(:value AS jsonb), "
        ":version, clock_timestamp() - make_interval(hours => :age))",
        {
            "id": uuid.uuid4(),
            "agent": agent_id,
            "scope": scope,
            "namespace": namespace,
            "key": key,
            "value": json.dumps(value),
            "version": version,
            "age": age_hours,
        },
    )


def _seed_copy(
    agent_id: uuid.UUID,
    scope: str | None,
    key: str,
    value: Any,
    version: int,
    age_hours: int,
) -> None:
    _sql(
        "INSERT INTO curie.thread_transcripts "
        "(id, agent_id, binding_scope, thread_key, value, version, expires_at, updated_at) "
        "VALUES (:id, :agent, :scope, :key, CAST(:value AS jsonb), :version, "
        "clock_timestamp() + interval '10 days', "
        "clock_timestamp() - make_interval(hours => :age))",
        {
            "id": uuid.uuid4(),
            "agent": agent_id,
            "scope": scope,
            "key": key,
            "value": json.dumps(value),
            "version": version,
            "age": age_hours,
        },
    )


def test_0059_reconciles_by_scope_and_removes_only_legacy_transcripts(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, "0058")
    agent_id = uuid.uuid4()
    try:
        _sql(
            "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
            {"id": agent_id, "name": f"acme-bot-{agent_id.hex[:8]}"},
        )
        _seed_state(agent_id, None, "transcript", "older-copy", [{"text": "legacy"}], 7, 1)
        _seed_copy(agent_id, None, "older-copy", [{"text": "copy"}], 3, 2)
        _seed_state(
            agent_id, SCOPE, "transcript", "older-copy", [{"text": "scoped old"}], 6, 3
        )
        _seed_copy(agent_id, SCOPE, "older-copy", [{"text": "scoped copy"}], 9, 2)
        _seed_state(agent_id, SCOPE, "transcript", "newer-copy", [{"text": "old"}], 4, 3)
        _seed_copy(agent_id, SCOPE, "newer-copy", [{"text": "current"}], 8, 2)
        _seed_state(agent_id, SCOPE, "transcript", "missing-copy", [{"text": "adopt"}], 5, 1)
        _seed_state(agent_id, None, "memory", "facts", {"source": "memory"}, 2, 1)
        _seed_state(agent_id, SCOPE, "workflow", "step", {"source": "workflow"}, 3, 1)

        command.upgrade(config, "0059")

        transcripts = _sql(
            "SELECT binding_scope, thread_key, value, version, "
            "expires_at > now() AS expires_in_future "
            "FROM curie.thread_transcripts WHERE agent_id = :agent "
            "ORDER BY thread_key, binding_scope NULLS FIRST",
            {"agent": agent_id},
        )
        assert transcripts == [
            {
                "binding_scope": SCOPE,
                "thread_key": "missing-copy",
                "value": [{"text": "adopt"}],
                "version": 5,
                "expires_in_future": True,
            },
            {
                "binding_scope": SCOPE,
                "thread_key": "newer-copy",
                "value": [{"text": "current"}],
                "version": 8,
                "expires_in_future": True,
            },
            {
                "binding_scope": None,
                "thread_key": "older-copy",
                "value": [{"text": "legacy"}],
                "version": 7,
                "expires_in_future": True,
            },
            {
                "binding_scope": SCOPE,
                "thread_key": "older-copy",
                "value": [{"text": "scoped copy"}],
                "version": 9,
                "expires_in_future": True,
            },
        ]
        remaining = _sql(
            "SELECT binding_scope, namespace, key, value, version "
            "FROM curie.workflow_state_entries WHERE agent_id = :agent "
            "ORDER BY namespace, key",
            {"agent": agent_id},
        )
        assert remaining == [
            {
                "binding_scope": None,
                "namespace": "memory",
                "key": "facts",
                "value": {"source": "memory"},
                "version": 2,
            },
            {
                "binding_scope": SCOPE,
                "namespace": "workflow",
                "key": "step",
                "value": {"source": "workflow"},
                "version": 3,
            },
        ]
    finally:
        command.upgrade(config, "head")
