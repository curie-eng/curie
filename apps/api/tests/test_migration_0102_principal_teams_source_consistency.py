"""Migration 0102 adds the principal_teams/teams source-match trigger (#3002)."""

from __future__ import annotations

from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts
from alembic import command


def _trigger_present() -> bool:
    rows = sql_dicts(
        "SELECT 1 FROM pg_trigger WHERE tgname = 'principal_teams_source_matches_team' "
        "AND tgrelid = 'curie.principal_teams'::regclass"
    )
    return bool(rows)


def _function_present() -> bool:
    rows = sql_dicts(
        "SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
        "WHERE n.nspname = 'curie' AND p.proname = 'enforce_principal_teams_source_match'"
    )
    return bool(rows)


def test_0102_round_trip_creates_and_drops_source_match_trigger(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at("head")
    assert _trigger_present() is True
    assert _function_present() is True
    try:
        command.downgrade(config, "0101")
        assert _trigger_present() is False
        assert _function_present() is False
        # principal_teams itself (added by 0057) must outlive this downgrade.
        assert sql_dicts("SELECT to_regclass('curie.principal_teams')::text AS name")[0][
            "name"
        ] == "curie.principal_teams"
    finally:
        # A failed assertion must not leave this private database below head.
        command.upgrade(config, "head")
    assert _trigger_present() is True
    assert _function_present() is True
