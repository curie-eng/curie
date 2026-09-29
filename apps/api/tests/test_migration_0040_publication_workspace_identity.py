"""Migration 0040 preserves honest publication workspace identity history."""

from __future__ import annotations

import uuid

from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts
from alembic import command


def _seed_legacy_publication() -> uuid.UUID:
    agent_id = uuid.uuid4()
    version_id = uuid.uuid4()
    deployment_id = uuid.uuid4()
    approval_id = uuid.uuid4()
    publication_id = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"migration-0040-{agent_id.hex}"},
    )
    sql_dicts(
        "INSERT INTO curie.agent_versions "
        "(id, agent_id, version_label, bundle_ref, created_by) "
        "VALUES (:id, :agent_id, 'migration-0040', NULL, 'test')",
        {"id": version_id, "agent_id": agent_id},
    )
    sql_dicts(
        "INSERT INTO curie.deployments "
        "(id, agent_id, version_id, environment, status) "
        "VALUES (:id, :agent_id, :version_id, "
        "CAST('prod' AS curie.environment), 'active')",
        {
            "id": deployment_id,
            "agent_id": agent_id,
            "version_id": version_id,
        },
    )
    sql_dicts(
        "INSERT INTO curie.approvals "
        "(id, agent_id, conversation_id, author, summary, reply_kind, "
        "reply_channel, reply_placeholder, dedupe_key, purpose) "
        "VALUES (:id, :agent_id, '1700000000.000100', 'U0REQUEST1', "
        "'Publish changes?', 'slack', 'C0EXAMPLE1', '1700000000.000001', "
        ":dedupe_key, 'publication')",
        {
            "id": approval_id,
            "agent_id": agent_id,
            "dedupe_key": f"migration-0040-{approval_id.hex}",
        },
    )
    sql_dicts(
        "INSERT INTO curie.publications "
        "(id, approval_id, deployment_id, repo_full_name, base_sha, patch_bytes, "
        "changed_paths, title, body, reply_kind, reply_channel, reply_placeholder) "
        "VALUES (:id, :approval_id, :deployment_id, 'acme-corp/acme-bot', "
        ":base_sha, :patch, CAST('[\"README.md\"]' AS jsonb), 'Update README', "
        "'Prepared by migration test.', 'slack', 'C0EXAMPLE1', "
        "'1700000000.000001')",
        {
            "id": publication_id,
            "approval_id": approval_id,
            "deployment_id": deployment_id,
            "base_sha": "a" * 40,
            "patch": b"diff --git a/README.md b/README.md\n",
        },
    )
    return publication_id


def test_0040_adds_nullable_identity_without_backfill_and_round_trips(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at("0039")
    publication_id = _seed_legacy_publication()

    command.upgrade(config, "0040")

    assert sql_dicts(
        "SELECT is_nullable FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'publications' "
        "AND column_name = 'workspace_conversation_id'"
    ) == [{"is_nullable": "YES"}]
    assert sql_dicts(
        "SELECT workspace_conversation_id FROM curie.publications WHERE id = :id",
        {"id": publication_id},
    ) == [{"workspace_conversation_id": None}]

    canonical = "slack:C0EXAMPLE1:1700000000.000100"
    sql_dicts(
        "UPDATE curie.publications SET workspace_conversation_id = :identity "
        "WHERE id = :id",
        {"identity": canonical, "id": publication_id},
    )
    assert sql_dicts(
        "SELECT workspace_conversation_id FROM curie.publications WHERE id = :id",
        {"id": publication_id},
    ) == [{"workspace_conversation_id": canonical}]

    command.downgrade(config, "0039")
    assert sql_dicts(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'publications' "
        "AND column_name = 'workspace_conversation_id'"
    ) == []

    command.upgrade(config, "0040")
    assert sql_dicts(
        "SELECT workspace_conversation_id FROM curie.publications WHERE id = :id",
        {"id": publication_id},
    ) == [{"workspace_conversation_id": None}]
