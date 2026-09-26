"""Migration 0059 reconciles and removes legacy transcript state rows."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from curie_api.config import get_settings
from curie_api.schema_compat import KIND_CONTRACT, load_kinds, load_window, plan_upgrade
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
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


def test_0059_contract_requires_forward_only() -> None:
    window = load_window()
    kinds = load_kinds()
    assert window.schema_min == "0059"
    assert kinds["0059"] == KIND_CONTRACT

    refused = plan_upgrade(
        current_revision="0058",
        window=window,
        kinds=kinds,
        pending=("0059",),
        forward_only=False,
    )
    assert refused.action == "refuse"
    assert refused.rollback_compatible is False
    assert refused.pending[0].kind == KIND_CONTRACT

    allowed = plan_upgrade(
        current_revision="0058",
        window=window,
        kinds=kinds,
        pending=("0059",),
        forward_only=True,
    )
    assert allowed.action == "apply"
    assert allowed.rollback_compatible is False
    assert allowed.pending[0].kind == KIND_CONTRACT


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
        legacy_low_version = 2
        copy_high_version = 11
        _seed_state(
            agent_id,
            SCOPE,
            "transcript",
            "conflict-copy",
            [{"text": "newer legacy"}],
            legacy_low_version,
            1,
        )
        _seed_copy(
            agent_id, SCOPE, "conflict-copy", [{"text": "older copy"}], copy_high_version, 2
        )
        _seed_state(agent_id, None, "transcript", "older-copy", [{"text": "legacy"}], 7, 1)
        _seed_copy(agent_id, None, "older-copy", [{"text": "copy"}], 3, 2)
        _seed_state(
            agent_id, SCOPE, "transcript", "older-copy", [{"text": "scoped old"}], 6, 3
        )
        _seed_copy(agent_id, SCOPE, "older-copy", [{"text": "scoped copy"}], 9, 2)
        _seed_state(agent_id, SCOPE, "transcript", "newer-copy", [{"text": "old"}], 4, 3)
        _seed_copy(agent_id, SCOPE, "newer-copy", [{"text": "current"}], 8, 2)
        _seed_state(agent_id, SCOPE, "transcript", "tie-copy", [{"text": "old"}], 10, 2)
        _seed_copy(agent_id, SCOPE, "tie-copy", [{"text": "current"}], 4, 2)
        tie_at = datetime(2026, 1, 1)
        _sql(
            "UPDATE curie.workflow_state_entries SET updated_at = :tie_at "
            "WHERE agent_id = :agent AND binding_scope = :scope "
            "AND namespace = 'transcript' AND key = 'tie-copy'",
            {"tie_at": tie_at, "agent": agent_id, "scope": SCOPE},
        )
        _sql(
            "UPDATE curie.thread_transcripts SET updated_at = :tie_at "
            "WHERE agent_id = :agent AND binding_scope = :scope AND thread_key = 'tie-copy'",
            {"tie_at": tie_at, "agent": agent_id, "scope": SCOPE},
        )
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
                "thread_key": "conflict-copy",
                "value": [{"text": "newer legacy"}],
                "version": 12,
                "expires_in_future": True,
            },
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
                "version": 8,
                "expires_in_future": True,
            },
            {
                "binding_scope": SCOPE,
                "thread_key": "older-copy",
                "value": [{"text": "scoped copy"}],
                "version": 9,
                "expires_in_future": True,
            },
            {
                "binding_scope": SCOPE,
                "thread_key": "tie-copy",
                "value": [{"text": "current"}],
                "version": 4,
                "expires_in_future": True,
            },
        ]
        adopted = next(row for row in transcripts if row["thread_key"] == "conflict-copy")
        assert adopted["version"] > legacy_low_version
        assert adopted["version"] > copy_high_version
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

        with pytest.raises(IntegrityError) as rejected:
            _seed_state(agent_id, None, "transcript", "late-write", [{"text": "old"}], 1, 0)
        assert getattr(rejected.value.orig, "sqlstate", None) == "23514"
        _seed_state(agent_id, None, "memory", "late-write", {"source": "new"}, 1, 0)
        writable = _sql(
            "SELECT value FROM curie.workflow_state_entries "
            "WHERE agent_id = :agent AND namespace = 'memory' AND key = 'late-write'",
            {"agent": agent_id},
        )
        assert writable == [{"value": {"source": "new"}}]
    finally:
        command.upgrade(config, "head")
