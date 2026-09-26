"""Migration 0058 widens the factory state label check (#3221).

Runs against a private database (``isolated_migration_db``), never the shared
one, per apps/api/CLAUDE.md. Upgrade to head before seeding, because later
revisions add columns.
"""

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
REPO = "acme-corp/acme-bot"
LEGACY = "curie:queued"
NEW = "curie-factory:queued"


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


def _revision() -> str:
    return str(_sql("SELECT version_num FROM curie.alembic_version")[0]["version_num"])


def _seed_request(number: int) -> tuple[uuid.UUID, uuid.UUID]:
    agent_id, work_item_id, request_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    _sql(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"acme-bot-{agent_id.hex[:8]}"},
    )
    _sql(
        "INSERT INTO curie.work_items "
        "(id, github_repository_id, github_issue_number, github_installation_id, "
        "agent_id, repo_full_name, conversation_id) "
        "VALUES (:id, 4401, :number, 5501, :agent, :repo, :conversation)",
        {
            "id": work_item_id,
            "number": number,
            "agent": agent_id,
            "repo": REPO,
            "conversation": f"issue-{number}",
        },
    )
    _sql(
        "INSERT INTO curie.execution_requests "
        "(id, work_item_id, sequence, status, wait_deadline, objective, "
        "requester, reply_kind, reply_address, reply_conversation_id) "
        "VALUES (:id, :work_item, 1, 'waiting', "
        "clock_timestamp() + interval '30 seconds', :objective, "
        "'github:6601:octocat', 'github', :repo, :conversation)",
        {
            "id": request_id,
            "work_item": work_item_id,
            "objective": f"https://github.com/{REPO}/issues/{number}\n\nLabelled.",
            "repo": REPO,
            "conversation": f"issue-{number}",
        },
    )
    _sql("UPDATE curie.work_items SET next_sequence = 2 WHERE id = :id", {"id": work_item_id})
    return work_item_id, request_id


def _seed_notice(work_item_id: uuid.UUID, request_id: uuid.UUID, applied_label: str) -> None:
    _sql(
        "INSERT INTO curie.factory_terminal_notices "
        "(execution_request_id, work_item_id, card_token, applied_label) "
        "VALUES (:id, :work_item, :token, :label)",
        {
            "id": request_id,
            "work_item": work_item_id,
            "token": uuid.uuid4().hex + uuid.uuid4().hex,
            "label": applied_label,
        },
    )


def _applied(request_id: uuid.UUID) -> str | None:
    rows = _sql(
        "SELECT applied_label FROM curie.factory_terminal_notices WHERE execution_request_id = :id",
        {"id": request_id},
    )
    assert len(rows) == 1, rows
    return rows[0]["applied_label"]


def _set_applied(request_id: uuid.UUID, label: str) -> None:
    _sql(
        "UPDATE curie.factory_terminal_notices SET applied_label = :label "
        "WHERE execution_request_id = :id",
        {"id": request_id, "label": label},
    )


def test_0058_accepts_legacy_and_new_labels_and_downgrades_clean_rows(
    isolated_migration_db: None,
) -> None:
    """N-1 can still write a legacy name, and the new name is a successful write."""

    config = _config()
    command.upgrade(config, "head")
    try:
        work_item_id, legacy_id = _seed_request(9101)
        _seed_notice(work_item_id, legacy_id, LEGACY)
        assert _applied(legacy_id) == LEGACY
        _set_applied(legacy_id, NEW)
        assert _applied(legacy_id) == NEW
        with pytest.raises(IntegrityError):
            _set_applied(legacy_id, "curie:custom")
        assert _applied(legacy_id) == NEW

        _set_applied(legacy_id, LEGACY)
        empty_item, empty_id = _seed_request(9102)
        _seed_notice(empty_item, empty_id, "")
        assert _applied(legacy_id) == LEGACY
        assert _applied(empty_id) == ""

        command.downgrade(config, "0057")

        assert _revision() == "0057"
        assert _applied(legacy_id) == LEGACY
        assert _applied(empty_id) == ""
        with pytest.raises(IntegrityError):
            _set_applied(legacy_id, NEW)
        assert _applied(legacy_id) == LEGACY
    finally:
        command.upgrade(config, "head")


def test_0058_downgrade_refuses_a_stored_new_state_label(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, "head")
    try:
        work_item_id, request_id = _seed_request(9103)
        _seed_notice(work_item_id, request_id, NEW)
        assert _applied(request_id) == NEW
        with pytest.raises(Exception):  # noqa: B017 (the migration raises on purpose)
            command.downgrade(config, "0057")
        # Later expand revisions can roll back. 0058 itself stays applied
        # because its downgrade refuses a stored new state label.
        assert _revision() == "0058"
        # Move off the stored value so the check runs, then write the new name again.
        _set_applied(request_id, "")
        _set_applied(request_id, NEW)
        assert _applied(request_id) == NEW
    finally:
        command.upgrade(config, "head")
