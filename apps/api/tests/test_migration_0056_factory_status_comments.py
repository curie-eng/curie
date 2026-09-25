"""Migration 0056 turns terminal notices into status comments (#3077).

Runs against a private database (``isolated_migration_db``), never the shared
one, per apps/api/CLAUDE.md.
"""

from __future__ import annotations

import asyncio
import re
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


def _seed_request(number: int, objective: str) -> tuple[uuid.UUID, uuid.UUID]:
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
            "objective": objective,
            "repo": REPO,
            "conversation": f"issue-{number}",
        },
    )
    _sql("UPDATE curie.work_items SET next_sequence = 2 WHERE id = :id", {"id": work_item_id})
    return work_item_id, request_id


def _seed_notice(
    work_item_id: uuid.UUID, request_id: uuid.UUID, *, comment_id: int | None
) -> None:
    posted = comment_id is not None
    _sql(
        "INSERT INTO curie.factory_terminal_notices "
        "(execution_request_id, work_item_id, terminal_cause, posted_at, comment_id) "
        "VALUES (:id, :work_item, 'runner_failed', "
        + ("clock_timestamp()" if posted else "NULL")
        + ", :comment)",
        {"id": request_id, "work_item": work_item_id, "comment": comment_id},
    )


def _status_rows() -> dict[uuid.UUID, dict[str, Any]]:
    rows = _sql(
        "SELECT execution_request_id, terminal_cause, posted_at, finalized_at, "
        "card_token, comment_list, comment_id, applied_label "
        "FROM curie.factory_terminal_notices"
    )
    return {row["execution_request_id"]: row for row in rows}


def _table(name: str) -> str | None:
    return _sql("SELECT to_regclass(:name) AS name", {"name": f"curie.{name}"})[0]["name"]


_NEW_COLUMNS = {
    "card_token",
    "comment_list",
    "rendered_digest",
    "finalized_at",
    "subject_title",
    "applied_label",
    "declaration",
    "activity",
    "updated_at",
}


def _columns() -> set[str]:
    rows = _sql(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'factory_terminal_notices'"
    )
    return {row["column_name"] for row in rows}


def test_0056_keeps_posted_notices_final_and_downgrades_clean_data(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, "0055")
    try:
        issue_item, issue_posted = _seed_request(
            9001, f"https://github.com/{REPO}/issues/9001\n\nLabelled."
        )
        _seed_notice(issue_item, issue_posted, comment_id=7001)
        thread_item, thread_posted = _seed_request(
            9002, f"https://github.com/{REPO}/pull/501#discussion_r88001\n\nReview."
        )
        _seed_notice(thread_item, thread_posted, comment_id=7002)
        pending_item, pending = _seed_request(
            9003, f"https://github.com/{REPO}/issues/9003\n\nLabelled."
        )
        _seed_notice(pending_item, pending, comment_id=None)

        command.upgrade(config, "0056")

        assert _table("factory_status_comments") is None
        assert _NEW_COLUMNS <= _columns()
        assert _table("execution_request_phase_reports") is not None
        rows = _status_rows()
        tokens = [row["card_token"] for row in rows.values()]
        assert all(re.fullmatch(r"[0-9a-f]{64}", token) for token in tokens)
        assert len(set(tokens)) == 3
        posted = rows[issue_posted]
        assert posted["finalized_at"] == posted["posted_at"]
        assert posted["comment_list"] == "issue"
        assert posted["applied_label"] == ""
        assert rows[thread_posted]["comment_list"] == "review"
        assert rows[thread_posted]["finalized_at"] is not None
        unposted = rows[pending]
        assert unposted["finalized_at"] is None
        assert unposted["comment_list"] is None
        assert unposted["applied_label"] == ""
        with pytest.raises(Exception):  # noqa: B017 (the unique index refuses it)
            _sql(
                "UPDATE curie.factory_terminal_notices SET card_token = :t "
                "WHERE execution_request_id = :id",
                {"t": posted["card_token"], "id": pending},
            )

        command.downgrade(config, "0055")

        assert _table("factory_terminal_notices") is not None
        assert not (_NEW_COLUMNS & _columns())
        assert _table("execution_request_phase_reports") is None
        restored = _sql(
            "SELECT execution_request_id, terminal_cause, comment_id "
            "FROM curie.factory_terminal_notices ORDER BY comment_id NULLS LAST"
        )
        assert [(r["execution_request_id"], r["comment_id"]) for r in restored] == [
            (issue_posted, 7001),
            (thread_posted, 7002),
            (pending, None),
        ]
    finally:
        command.upgrade(config, "head")


def test_0056_downgrade_refuses_a_row_with_no_terminal_cause(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, "0056")
    try:
        work_item_id, request_id = _seed_request(
            9004, f"https://github.com/{REPO}/issues/9004\n\nLabelled."
        )
        _sql(
            "INSERT INTO curie.factory_terminal_notices "
            "(execution_request_id, work_item_id, card_token) VALUES (:id, :w, :t)",
            {"id": request_id, "w": work_item_id, "t": uuid.uuid4().hex + uuid.uuid4().hex},
        )
        with pytest.raises(Exception):  # noqa: B017 (the migration raises on purpose)
            command.downgrade(config, "0055")
        assert "card_token" in _columns()
    finally:
        command.upgrade(config, "head")


def test_0056_accepts_the_rows_application_n_minus_1_writes(
    isolated_migration_db: None,
) -> None:
    """0056 is an expand: origin/next's _queue_notice insert and post still work."""

    config = _config()
    command.upgrade(config, "0056")
    try:
        work_item_id, request_id = _seed_request(
            9005, f"https://github.com/{REPO}/issues/9005\n\nLabelled."
        )
        _sql(
            "INSERT INTO curie.factory_terminal_notices "
            "(execution_request_id, work_item_id, terminal_cause, detail) "
            "VALUES (:id, :w, 'runner_failed', NULL)",
            {"id": request_id, "w": work_item_id},
        )
        pending = _sql(
            "SELECT execution_request_id FROM curie.factory_terminal_notices "
            "WHERE posted_at IS NULL AND refused_at IS NULL"
        )
        assert [row["execution_request_id"] for row in pending] == [request_id]
        # N-1 posts without naming a comment list.
        _sql(
            "UPDATE curie.factory_terminal_notices "
            "SET posted_at = clock_timestamp(), comment_id = 7005, attempts = 1 "
            "WHERE execution_request_id = :id",
            {"id": request_id},
        )
        row = _status_rows()[request_id]
        assert re.fullmatch(r"[0-9a-f]{64}", row["card_token"])
        assert row["comment_id"] == 7005
        assert row["comment_list"] is None
        assert _table("ix_factory_terminal_notices_pending") is not None
    finally:
        command.upgrade(config, "head")
