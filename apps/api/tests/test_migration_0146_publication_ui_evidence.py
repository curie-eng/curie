"""Migration 0146 adds the publication UI evidence tables (plan AC5).

Upgrade creates the three tables with their keys, cascades and checks; the
downgrade to 0145 removes exactly them and leaves the publication tables.
"""

from __future__ import annotations

from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    column_names,
    sql_dicts,
    stamped_revision,
)
from alembic import command

BELOW = "0145"
TABLES = {
    "publication_ui_evidence",
    "publication_ui_evidence_images",
    "publication_lineage_ui_evidence",
}


def _tables() -> set[str]:
    return {
        row["table_name"]
        for row in sql_dicts(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'curie'"
        )
    }


def _constraints(table: str, kind: str) -> list[str]:
    return [
        row["definition"]
        for row in sql_dicts(
            "SELECT pg_get_constraintdef(c.oid) AS definition FROM pg_constraint c "
            "JOIN pg_class t ON t.oid = c.conrelid "
            "JOIN pg_namespace n ON n.oid = t.relnamespace "
            "WHERE n.nspname = 'curie' AND t.relname = :table AND c.contype::text = :kind",
            {"table": table, "kind": kind},
        )
    ]


def _checks(table: str) -> str:
    return "\n".join(_constraints(table, "c"))


def _assert_created() -> None:
    assert TABLES <= _tables()
    assert {
        "publication_id",
        "lineage_id",
        "status",
        "index_sha256",
        "manifest_state",
        "manifest_refusal",
        "manifest_status",
        "reason",
        "base_sha",
        "manifest_base_sha",
        "manifest_head_sha",
        "manifest_candidate_tree",
        "candidate_tree",
        "head_tree",
        "candidate_dirty",
        "warnings",
        "total_bytes",
        "upload_deadline_at",
        "publish_deadline_at",
        "lease_owner",
        "lease_expires_at",
        "attempts",
        "outcome",
        "error",
        "evidence_commit_sha",
        "comment_written_at",
        "version",
        "created_at",
        "updated_at",
        "terminal_at",
    } <= column_names("publication_ui_evidence")
    assert {
        "publication_id",
        "ordinal",
        "path",
        "label",
        "state",
        "size_bytes",
        "sha256",
        "refusal",
        "bound",
        "content",
        "stored_at",
        "url",
    } <= column_names("publication_ui_evidence_images")
    assert {
        "lineage_id",
        "ref_commit_sha",
        "ref_pending",
        "comment_revision",
        "comment_kind",
        "lease_owner",
        "lease_expires_at",
        "version",
        "updated_at",
    } <= column_names("publication_lineage_ui_evidence")

    evidence = _checks("publication_ui_evidence")
    for status in (
        "receiving",
        "ready",
        "publishing",
        "published",
        "unsupported",
        "failed",
        "superseded",
        "abandoned",
    ):
        assert f"'{status}'" in evidence, status
    assert "104857600" in evidence
    images = _checks("publication_ui_evidence_images")
    assert "10485760" in images
    assert "'before'" in images and "'after'" in images
    assert "512" in images and "200" in images
    lineage = _checks("publication_lineage_ui_evidence")
    for kind in ("evidence", "unsupported", "stale", "failed"):
        assert f"'{kind}'" in lineage, kind

    # Every row is owned by its publication or lineage and goes with it.
    foreign = {table: "\n".join(_constraints(table, "f")) for table in TABLES}
    assert "publications(id)" in foreign["publication_ui_evidence"]
    assert "thread_publication_lineages(id)" in foreign["publication_ui_evidence"]
    assert "publication_ui_evidence(publication_id)" in foreign["publication_ui_evidence_images"]
    assert "thread_publication_lineages(id)" in foreign["publication_lineage_ui_evidence"]
    for table, definition in foreign.items():
        assert definition.count("ON DELETE CASCADE") == definition.count("FOREIGN KEY"), table
    primary = "\n".join(_constraints("publication_ui_evidence_images", "p"))
    assert "publication_id" in primary and "ordinal" in primary
    indexes = {
        row["indexname"]
        for row in sql_dicts(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'curie' "
            "AND tablename = 'publication_ui_evidence'"
        )
    }
    assert "ix_publication_ui_evidence_status_lease" in indexes


def test_0146_upgrade_creates_the_evidence_tables_and_downgrade_removes_them(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at(BELOW)
    assert not TABLES & _tables()
    try:
        command.upgrade(config, "0146")
        assert stamped_revision() == "0146"
        _assert_created()

        command.downgrade(config, BELOW)

        assert stamped_revision() == BELOW
        assert not TABLES & _tables()
        # The publication tables the evidence hangs off are untouched.
        assert {"publications", "thread_publication_lineages"} <= _tables()
    finally:
        command.upgrade(config, "head")
