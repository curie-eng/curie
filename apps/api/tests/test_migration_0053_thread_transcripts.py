"""Migration 0053 copies transcripts out of the capped state store (ADR-0170)."""

from __future__ import annotations

import json
import uuid

from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts
from alembic import command

THREAD = "slack:C0EXAMPLE1:1700000000.000100"


def _state_rows(agent_id: uuid.UUID) -> list[tuple[str, str]]:
    rows = sql_dicts(
        "SELECT namespace, key FROM curie.workflow_state_entries WHERE agent_id = :a "
        "ORDER BY namespace, key",
        {"a": agent_id},
    )
    return [(row["namespace"], row["key"]) for row in rows]


def test_0053_moves_transcripts_and_downgrade_moves_them_back(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at("0052")
    agent_id = uuid.uuid4()
    transcript = [{"role": "user", "content": "fix the flaky test"}]
    try:
        sql_dicts(
            "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
            {"id": agent_id, "name": f"acme-bot-{agent_id.hex[:8]}"},
        )
        for namespace, key, value, version in (
            ("transcript", THREAD, transcript, 3),
            ("memory", "facts", {"n": 1}, 1),
            ("workflow", "step", {"n": 2}, 1),
        ):
            sql_dicts(
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

        command.upgrade(config, "0053")
        moved = sql_dicts(
            "SELECT thread_key, value, version, binding_scope FROM curie.thread_transcripts "
            "WHERE agent_id = :a",
            {"a": agent_id},
        )
        expiry = sql_dicts(
            "SELECT expires_at > now() + interval '29 days' AS idle_window "
            "FROM curie.thread_transcripts WHERE agent_id = :a",
            {"a": agent_id},
        )
        assert expiry == [{"idle_window": True}]
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
        sql_dicts(
            "DELETE FROM curie.workflow_state_entries WHERE agent_id = :a "
            "AND namespace = 'transcript'",
            {"a": agent_id},
        )

        command.downgrade(config, "0052")
        rows = sql_dicts("SELECT to_regclass('curie.thread_transcripts') AS name")
        assert rows[0]["name"] is None
        assert _state_rows(agent_id) == [
            ("memory", "facts"),
            ("transcript", THREAD),
            ("workflow", "step"),
        ]
    finally:
        # A failed assertion must not leave this private database below head.
        command.upgrade(config, "head")
