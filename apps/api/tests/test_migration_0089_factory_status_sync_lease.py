"""Status sync leases expand existing notices and survive a real round trip."""

from __future__ import annotations

from _migration_support import IsolatedMigrationDb, alembic_config, column_names, sql_dicts
from alembic import command
from test_migration_0056_factory_status_comments import _seed_notice, _seed_request


def test_0089_status_sync_lease_upgrades_and_downgrades_without_losing_notices(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at("0081")
    work_item, request = _seed_request(9959, "https://github.com/acme-corp/acme-bot/issues/9959")
    _seed_notice(work_item, request, comment_id=7999)
    original = sql_dicts(
        "SELECT execution_request_id, comment_id, posted_at, terminal_cause "
        "FROM curie.factory_terminal_notices WHERE execution_request_id = :id",
        {"id": request},
    )[0]
    try:
        command.upgrade(config, "0089")
        metadata = sql_dicts(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema = 'curie' AND table_name = 'factory_terminal_notices' "
            "AND column_name IN ('sync_owner', 'sync_lease_expires_at', 'sync_invalidated')"
        )
        assert {row["column_name"]: (row["data_type"], row["is_nullable"]) for row in metadata} == {
            "sync_owner": ("text", "YES"),
            "sync_lease_expires_at": ("timestamp with time zone", "YES"),
            "sync_invalidated": ("boolean", "NO"),
        }
        lease = sql_dicts(
            "SELECT sync_owner, sync_lease_expires_at, sync_invalidated "
            "FROM curie.factory_terminal_notices "
            "WHERE execution_request_id = :id",
            {"id": request},
        )[0]
        assert lease == {
            "sync_owner": None,
            "sync_lease_expires_at": None,
            "sync_invalidated": False,
        }
        sql_dicts(
            "UPDATE curie.factory_terminal_notices SET sync_owner = 'status-owner', "
            "sync_lease_expires_at = clock_timestamp() + interval '300 seconds', "
            "sync_invalidated = true "
            "WHERE execution_request_id = :id",
            {"id": request},
        )
        command.downgrade(config, "0081")
        assert not {"sync_owner", "sync_lease_expires_at", "sync_invalidated"} & column_names(
            "factory_terminal_notices"
        )
        restored = sql_dicts(
            "SELECT execution_request_id, comment_id, posted_at, terminal_cause "
            "FROM curie.factory_terminal_notices WHERE execution_request_id = :id",
            {"id": request},
        )[0]
        assert restored == original
        command.upgrade(config, "0089")
        lease = sql_dicts(
            "SELECT sync_owner, sync_lease_expires_at, sync_invalidated "
            "FROM curie.factory_terminal_notices "
            "WHERE execution_request_id = :id",
            {"id": request},
        )[0]
        assert lease == {
            "sync_owner": None,
            "sync_lease_expires_at": None,
            "sync_invalidated": False,
        }
    finally:
        command.upgrade(config, "head")
