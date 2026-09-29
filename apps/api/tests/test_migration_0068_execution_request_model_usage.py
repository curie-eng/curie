"""Migration 0068 adds curie.execution_request_model_usage (#3223).

Pinned: revision "0068", down_revision "0067". Runs on a private database
(``isolated_migration_db``), never the shared one, per apps/api/CLAUDE.md.
"""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"
REPO = "acme-corp/acme-bot"


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


def _table() -> str | None:
    return _sql(
        "SELECT to_regclass('curie.execution_request_model_usage') AS name"
    )[0]["name"]


def _seed_request() -> uuid.UUID:
    agent_id, work_item_id, request_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    _sql(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"acme-bot-{agent_id.hex[:8]}"},
    )
    _sql(
        "INSERT INTO curie.work_items "
        "(id, github_repository_id, github_issue_number, github_installation_id, "
        "agent_id, repo_full_name, conversation_id, next_sequence) "
        "VALUES (:id, 4401, 9801, 5501, :agent, :repo, 'issue-9801', 2)",
        {"id": work_item_id, "agent": agent_id, "repo": REPO},
    )
    _sql(
        "INSERT INTO curie.execution_requests "
        "(id, work_item_id, sequence, status, wait_deadline, objective, "
        "requester, reply_kind, reply_address, reply_conversation_id) "
        "VALUES (:id, :work_item, 1, 'waiting', "
        "clock_timestamp() + interval '30 seconds', 'objective', "
        "'github:6601:octocat', 'github', :repo, 'issue-9801')",
        {"id": request_id, "work_item": work_item_id, "repo": REPO},
    )
    return request_id


def _insert(request_id: uuid.UUID, **overrides: Any) -> None:
    row: dict[str, Any] = {
        "id": request_id,
        "turn": "turn-1",
        "model": "example-org/model-PLACEHOLDER",
        "role": "implementer",
        "inp": 10,
        "out": 5,
        "cost": None,
        "source": None,
    }
    row.update(overrides)
    _sql(
        "INSERT INTO curie.execution_request_model_usage "
        "(execution_request_id, turn_id, model, role, input_tokens, cached_input_tokens, "
        "cache_write_tokens, output_tokens, estimated_cost_usd, price_source, price_as_of) "
        "VALUES (:id, :turn, :model, :role, :inp, 0, 0, :out, :cost, :source, "
        "CASE WHEN CAST(:source AS text) IS NULL THEN NULL ELSE clock_timestamp() END)",
        row,
    )


def test_0068_follows_0067() -> None:
    script = ScriptDirectory.from_config(_config()).get_revision("0068")
    assert script is not None
    assert script.down_revision == "0067"


def test_0068_creates_the_usage_table_with_its_guards_and_downgrades(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, "0067")
    try:
        assert _table() is None
        command.upgrade(config, "0068")
        assert _table() is not None

        request_id = _seed_request()
        _insert(request_id)
        _insert(request_id, model="example-org/reviewer-PLACEHOLDER", role="reviewer",
                cost=0.25, source="https://prices.example.com/api/v1/models")
        with pytest.raises(Exception):  # noqa: B017 (unique request, turn, model)
            _insert(request_id)
        with pytest.raises(Exception):  # noqa: B017 (role check)
            _insert(request_id, turn="turn-2", role="observer")
        with pytest.raises(Exception):  # noqa: B017 (tokens are non-negative)
            _insert(request_id, turn="turn-3", inp=-1)
        with pytest.raises(Exception):  # noqa: B017 (cost without a source)
            _insert(request_id, turn="turn-4", cost=1.0, source=None)

        _sql("DELETE FROM curie.execution_requests WHERE id = :id", {"id": request_id})
        assert _sql("SELECT count(*) AS n FROM curie.execution_request_model_usage")[0]["n"] == 0

        command.downgrade(config, "0067")
        assert _table() is None
    finally:
        command.upgrade(config, "head")
