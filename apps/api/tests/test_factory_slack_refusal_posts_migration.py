"""Migration 0170 keeps refusal posts across an upgrade and refuses a lossy downgrade."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from _migration_support import alembic_config
from alembic import command
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def go() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                result = await connection.execute(text(statement), params or {})
                return [dict(row) for row in result.mappings()] if result.returns_rows else []
        finally:
            await engine.dispose()

    return asyncio.run(go())


def test_refusal_post_migration_refuses_lossy_downgrade(isolated_migration_db: Any) -> None:
    isolated_migration_db.at("0168")
    config = alembic_config()
    command.upgrade(config, "0170")
    _sql(
        "INSERT INTO curie.factory_intake_notices "
        "(tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, label_event_id, "
        "repository_path, issue_url, label, reason) VALUES "
        "('github', 'github.com', '4401', '1', 1, 'acme-corp/acme-bot', "
        "'https://github.com/acme-corp/acme-bot/issues/1', 'factory', 'actor_bot')"
    )
    _sql(
        "INSERT INTO curie.factory_slack_refusal_posts "
        "(notice_id, channel_identity, channel, payload) "
        "SELECT id, 'acme-factory', 'C0EXAMPLE1', '{}'::jsonb "
        "FROM curie.factory_intake_notices"
    )
    with pytest.raises(IntegrityError):
        _sql(
            "INSERT INTO curie.factory_slack_refusal_posts "
            "(notice_id, channel_identity, channel, payload) "
            "SELECT id, 'acme-factory', 'C0EXAMPLE1', '{}'::jsonb "
            "FROM curie.factory_intake_notices"
        )
    assert _sql(
        "SELECT conname FROM pg_constraint WHERE conname = 'factory_slack_refusal_posts_notice_key'"
    ) == [{"conname": "factory_slack_refusal_posts_notice_key"}]
    with pytest.raises(RuntimeError, match="refusal posts exist"):
        command.downgrade(config, "0168")
    assert _sql("SELECT channel FROM curie.factory_slack_refusal_posts") == [
        {"channel": "C0EXAMPLE1"}
    ]
    _sql("DELETE FROM curie.factory_slack_refusal_posts")
    command.downgrade(config, "0168")
    assert _sql("SELECT to_regclass('curie.factory_slack_refusal_posts') AS table_name") == [
        {"table_name": None}
    ]
    command.upgrade(config, "0170")
    assert _sql("SELECT to_regclass('curie.factory_slack_refusal_posts') AS table_name") == [
        {"table_name": "curie.factory_slack_refusal_posts"}
    ]
