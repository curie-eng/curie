"""Revision 0072 defaults success notices off and creates the retry outbox."""

from __future__ import annotations

import uuid

from _migration_support import alembic_config, column_names, sql_rows
from alembic import command
from alembic.script import ScriptDirectory


def test_0072_follows_0071() -> None:
    revision = ScriptDirectory.from_config(alembic_config()).get_revision("0072")
    assert revision is not None
    assert revision.down_revision == "0071"


def test_0072_upgrades_old_agent_and_outbox_then_downgrades(
    isolated_migration_db: None,
) -> None:
    config = alembic_config()
    command.upgrade(config, "0071")
    assert "deploy_notifications" not in column_names("agents")
    assert column_names("deploy_notice_outbox") == set()
    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": "acme-dev"},
    )
    try:
        command.upgrade(config, "0072")
        assert sql_rows(
            "SELECT deploy_notifications FROM curie.agents WHERE id = :id", {"id": agent_id}
        )[0][0] is False
        assert column_names("deploy_notice_outbox") == {
            "key", "stream", "payload", "attempts", "created_at", "enqueued_at"
        }
        sql_rows(
            "INSERT INTO curie.deploy_notice_outbox (key, stream, payload) "
            "VALUES (:key, :stream, :payload)",
            {"key": "a" * 64, "stream": "curie:runs:deploy-notices", "payload": "{}"},
        )
        assert sql_rows(
            "SELECT attempts, enqueued_at IS NULL FROM curie.deploy_notice_outbox "
            "WHERE key = :key",
            {"key": "a" * 64},
        ) == [(0, True)]

        command.downgrade(config, "0071")
        assert "deploy_notifications" not in column_names("agents")
        assert column_names("deploy_notice_outbox") == set()
    finally:
        command.upgrade(config, "head")
