"""Migration 0186 counts plain review feedback revisions per pull request (#5794).

Real Postgres. The upgrade runs through Alembic from the revision below it with
a lineage row an earlier release wrote, so the defaults it reads are the ones
an existing install sees, and the downgrade returns to that revision.
"""

from __future__ import annotations

import uuid

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    column_names,
    constraint_exists,
    sql_rows,
    stamped_revision,
)
from alembic import command
from sqlalchemy.exc import IntegrityError
from test_memory_rules_migration import _agent, _lineage

REVISION = "0186"
BELOW = "0185"
TABLE = "thread_publication_lineages"
COUNT_CHECK = "thread_publication_lineages_plain_feedback_revisions_ck"
ADDED = {"plain_feedback_revisions", "plain_feedback_capped_at"}


def _column(column: str) -> tuple[str, str, str | None]:
    row = sql_rows(
        "SELECT data_type, is_nullable, column_default FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = :table AND column_name = :column",
        {"table": TABLE, "column": column},
    )[0]
    return str(row[0]), str(row[1]), row[2]


def _counts(lineage_id: uuid.UUID) -> tuple[int, object]:
    row = sql_rows(
        f"SELECT plain_feedback_revisions, plain_feedback_capped_at FROM curie.{TABLE} "
        "WHERE id = :id",
        {"id": lineage_id},
    )[0]
    return row[0], row[1]


def test_upgrade_from_0185_and_downgrade_back(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(BELOW)
    assert not ADDED & column_names(TABLE)
    existing = _lineage(_agent())

    config = alembic_config()
    command.upgrade(config, REVISION)
    assert stamped_revision() == REVISION
    assert _column("plain_feedback_revisions") == ("integer", "NO", "0")
    assert _column("plain_feedback_capped_at")[:2] == ("timestamp with time zone", "YES")
    assert constraint_exists(COUNT_CHECK)
    # A row written before the upgrade reads an uncapped count of zero.
    assert _counts(existing) == (0, None)
    # The release below keeps inserting without naming either column.
    assert _counts(_lineage(_agent())) == (0, None)

    sql_rows(
        f"UPDATE curie.{TABLE} SET plain_feedback_revisions = 10, "
        "plain_feedback_capped_at = now() WHERE id = :id",
        {"id": existing},
    )
    command.downgrade(config, BELOW)
    assert stamped_revision() == BELOW
    assert not ADDED & column_names(TABLE)
    assert not constraint_exists(COUNT_CHECK)
    # The lineage itself survives the downgrade.
    assert sql_rows(f"SELECT count(*) FROM curie.{TABLE} WHERE id = :id", {"id": existing}) == [
        (1,)
    ]

    command.upgrade(config, REVISION)
    assert _counts(existing) == (0, None)


def test_the_count_is_never_negative(isolated_migration_db: IsolatedMigrationDb) -> None:
    isolated_migration_db.at(REVISION)
    lineage_id = _lineage(_agent())
    with pytest.raises(IntegrityError, match=COUNT_CHECK):
        sql_rows(
            f"UPDATE curie.{TABLE} SET plain_feedback_revisions = -1 WHERE id = :id",
            {"id": lineage_id},
        )
    assert _counts(lineage_id) == (0, None)
