"""Migration 0120 gives every team its directory sync state (#4584, ADR 0220).

Expand-only: seven columns are added to ``curie.teams`` with defaults that
describe a team no directory adapter has synced yet (epoch 0, no members, no
digest, no timestamps), so a database populated at 0108 comes through with
every existing team in that state. The downgrade drops the columns and their
constraints, rows included.

0120 rather than 0109: ``main`` and ``next`` both stop at 0108, and the number
right after a shared head is the one a stable fix takes next.
"""

from __future__ import annotations

import uuid

from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    column_names,
    sql_dicts,
)
from alembic import command
from alembic.script import ScriptDirectory

TENANT = uuid.UUID("00000000-0000-0000-0000-000000000001")
SYNC_COLUMNS = {
    "epoch",
    "member_count",
    "digest",
    "last_synced_at",
    "last_attempt_at",
    "last_error",
    "managed_by",
}
SYNC_CONSTRAINTS = {
    "teams_epoch_ck",
    "teams_member_count_ck",
    "teams_sync_state_ck",
    "teams_managed_by_ck",
    "teams_last_error_ck",
}


def _constraint_names() -> set[str]:
    rows = sql_dicts(
        "SELECT conname FROM pg_constraint WHERE conrelid = to_regclass('curie.teams')"
    )
    return {row["conname"] for row in rows}


def test_0120_revises_0108() -> None:
    script = ScriptDirectory.from_config(alembic_config())
    revision = script.get_revision("0120")
    assert revision is not None
    assert revision.down_revision == "0108"


def test_0120_round_trip_adds_and_drops_sync_state(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at("0120")
    assert SYNC_COLUMNS <= set(column_names("teams"))
    assert SYNC_CONSTRAINTS <= _constraint_names()
    try:
        command.downgrade(config, "0108")
        assert not SYNC_COLUMNS & set(column_names("teams"))
        assert not SYNC_CONSTRAINTS & _constraint_names()
    finally:
        command.upgrade(config, "head")


def test_0120_existing_teams_start_unsynced(isolated_migration_db: IsolatedMigrationDb) -> None:
    isolated_migration_db.at("0108")
    idp_team = uuid.uuid4()
    local_team = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.teams (id, tenant_id, source, external_id, name) VALUES "
        "(:idp, :tenant, 'idp_group', '00gEXAMPLE1', 'finance-requesters'), "
        "(:local, :tenant, 'curie_managed', NULL, 'reviewers')",
        {"idp": idp_team, "local": local_team, "tenant": TENANT},
    )
    command.upgrade(alembic_config(), "0120")
    rows = sql_dicts(
        "SELECT id, epoch, member_count, digest, last_synced_at, last_attempt_at, "
        "last_error, managed_by FROM curie.teams WHERE id IN (:idp, :local)",
        {"idp": idp_team, "local": local_team},
    )
    assert len(rows) == 2
    for row in rows:
        assert row["epoch"] == 0
        assert row["member_count"] == 0
        assert row["digest"] is None
        assert row["last_synced_at"] is None
        assert row["last_attempt_at"] is None
        assert row["last_error"] is None
        assert row["managed_by"] is None
