"""Migration 0186 counts plain review feedback revisions per pull request (#5794).

Real Postgres. The upgrade runs through Alembic from the revision below it with
a lineage row an earlier release wrote, so the defaults it reads are the ones
an existing install sees, and the downgrade returns to that revision.
"""

from __future__ import annotations

import uuid
from pathlib import Path

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
from alembic.script import ScriptDirectory
from curie_api.config import get_settings
from sqlalchemy.exc import IntegrityError
from test_github_factory_review import _complete
from test_github_plain_feedback import (
    PATHS,
    Pull,
    _codes,
    _own_pull_request,
    _requests,
    factory_github,
    running_factory,
)
from test_memory_rules_migration import _agent, _lineage

REVISION = "0186"
CURRENT_APP_REVISION = "0189"
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


@pytest.mark.usefixtures("valkey")
@pytest.mark.parametrize("path", PATHS)
def test_a_pull_request_bound_before_the_upgrade_takes_plain_feedback_after_it(
    isolated_migration_db: IsolatedMigrationDb,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    path: str,
) -> None:
    """The upgrade handoff, driven end to end. The release below 0186 binds a
    factory pull request (its lineage insert names neither new column); after
    the upgrade, the first plain comment on that same lineage goes through
    production intake and starts a revision, counted once and uncapped.

    Counterexample it rejects: a backfill that leaves an existing lineage at or
    past the cap (or with a capped time) refuses this first comment.
    """

    # The app under test is the head app, so it runs on the head schema; the
    # downgrade below passes back through 0186 to the release below it.
    isolated_migration_db.at("head")
    github = factory_github(monkeypatch, tmp_path)
    try:
        with running_factory(github, new_agent=True) as factory:
            number, first = factory.work_item()

        config = alembic_config()
        command.downgrade(config, BELOW)
        assert not ADDED & column_names(TABLE)
        branch = f"curie/publication-acme-{number}"
        head = github.push_head(branch)
        pr = github.seed_pull(branch, "main")
        lineage_id = _own_pull_request(
            first["work_item_id"], pr, branch=branch, head=head, status="open"
        )
        command.upgrade(config, "head")
        assert stamped_revision() == ScriptDirectory.from_config(config).get_current_head()

        with running_factory(github, new_agent=False) as factory:
            _complete(factory.client, first["id"])
            pull = Pull(pr, number, first["work_item_id"], lineage_id)
            posted = factory.comment(pull, "Please rename the helper.")
            responses = factory.deliver(path, posted)
            if path == "webhook":
                assert [r.json()["status"] for r in responses] == ["factory_admitted"], _codes(
                    responses
                )
            rows = _requests(number)
            assert len(rows) == 2
            assert rows[1]["work_item_id"] == first["work_item_id"]
            assert _counts(lineage_id) == (1, None)
            assert not factory.notices(pull)
    finally:
        get_settings.cache_clear()
