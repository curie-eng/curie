"""Migration 0042 preserves uncertainty for historical publication lineages."""

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
from sqlalchemy.ext.asyncio import create_async_engine

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"
BELOW = "0041"
REVISION = "0042"
REPO = "acme-corp/acme-bot"
HEAD_SHA = "1123456789abcdef0123456789abcdef01234567"


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
                return [dict(row) for row in result.mappings().all()] if result.returns_rows else []
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _seed_historical_lineage() -> uuid.UUID:
    agent_id = uuid.uuid4()
    version_id = uuid.uuid4()
    deployment_id = uuid.uuid4()
    lineage_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"authority-migration-{agent_id.hex[:8]}"},
    )
    _sql(
        "INSERT INTO curie.agent_versions "
        "(id, agent_id, version_label, bundle_ref, created_by) "
        "VALUES (:id, :agent_id, 'v1', NULL, 'migration-test')",
        {"id": version_id, "agent_id": agent_id},
    )
    _sql(
        "INSERT INTO curie.deployments "
        "(id, agent_id, version_id, environment, status) "
        "VALUES (:id, :agent_id, :version_id, CAST('dev' AS curie.environment), 'active')",
        {"id": deployment_id, "agent_id": agent_id, "version_id": version_id},
    )
    _sql(
        "INSERT INTO curie.thread_publication_lineages "
        "(id, agent_id, deployment_id, conversation_id, repo_full_name, base_sha, branch, "
        "pr_number, pr_url, head_sha, status, version, latest_revision) VALUES "
        "(:id, :agent_id, :deployment_id, :conversation_id, :repo, :base_sha, :branch, "
        ":pr_number, :pr_url, :head_sha, 'open', 2, 1)",
        {
            "id": lineage_id,
            "agent_id": agent_id,
            "deployment_id": deployment_id,
            "conversation_id": "slack:C0EXAMPLE1:1700000000.000100",
            "repo": REPO,
            "base_sha": "0123456789abcdef0123456789abcdef01234567",
            "branch": f"curie/publication-{lineage_id.hex}",
            "pr_number": 123,
            "pr_url": f"https://github.com/{REPO}/pull/123",
            "head_sha": HEAD_SHA,
        },
    )
    return lineage_id


def test_0042_does_not_reconstruct_authority_for_historical_pull_requests(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, BELOW)
    lineage_id = _seed_historical_lineage()

    command.upgrade(config, REVISION)

    assert _sql(
        "SELECT binding_id, binding_generation, reply_conversation_id, "
        "github_repository_id, github_installation_id, github_pr_node_id, base_ref "
        "FROM curie.thread_publication_lineages WHERE id = :id",
        {"id": lineage_id},
    ) == [
        {
            "binding_id": None,
            "binding_generation": None,
            "reply_conversation_id": None,
            "github_repository_id": None,
            "github_installation_id": None,
            "github_pr_node_id": None,
            "base_ref": None,
        }
    ]
    assert _sql("SELECT count(*) AS count FROM curie.publication_review_reservations") == [
        {"count": 0}
    ]


def test_0042_downgrade_refuses_an_active_review_reservation(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, BELOW)
    lineage_id = _seed_historical_lineage()
    command.upgrade(config, REVISION)
    reservation_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.publication_review_reservations "
        "(id, origin_key, lineage_id, lineage_version, expected_head_sha, revision_number, "
        "binding_id, binding_generation, status, version) VALUES "
        "(:id, :origin_key, :lineage_id, 2, :head_sha, 2, :binding_id, 0, 'reserved', 1)",
        {
            "id": reservation_id,
            "origin_key": "review:retain-on-rollback",
            "lineage_id": lineage_id,
            "head_sha": HEAD_SHA,
            "binding_id": uuid.uuid4(),
        },
    )

    with pytest.raises(RuntimeError, match="revisions are active"):
        command.downgrade(config, BELOW)

    assert _sql("SELECT version_num FROM curie.alembic_version") == [{"version_num": REVISION}]
    assert _sql(
        "SELECT status, version FROM curie.publication_review_reservations WHERE id = :id",
        {"id": reservation_id},
    ) == [{"status": "reserved", "version": 1}]
