"""Migration 0021 moves the agent-to-channel binding into its own table.

`0021_agent_channels.py` creates `curie.agent_channels` with `kind` and
`address`, backfills exactly one row per agent as `(kind='slack',
address=agents.slack_channel)`, adds the two named unique constraints, and then
drops `agents_slack_channel_key` and the `agents.slack_channel` column.

Three things make this migration worth a test of its own rather than trusting
the schema diff:

1. **The backfill IS the migration.** Replacing `upgrade()`'s body with the DDL
   alone and no `INSERT ... SELECT` produces a perfectly valid empty table, and
   every existing install silently loses every binding: every agent is deployed,
   healthy-looking, and unroutable. That is #38's silent-shadow failure at
   install scale, and no schema assertion notices it.

2. **The downgrade has to restore a CONTRACT, not just a column.** The old
   column was `NOT NULL` (`0001_initial.py`) and its uniqueness constraint was
   NAMED (`agents_slack_channel_key`, migration 0017), and the API's 409 map is
   keyed on that exact name. A downgrade that recreates the column nullable, or
   lets Postgres generate the constraint name, satisfies a shape check, breaks a
   re-upgrade, and turns the #38 conflict back into an opaque 500.

3. **The new table can hold states the old column cannot represent.** A
   non-`slack` kind and an agent with zero bindings both have no home in a
   single `NOT NULL` string column. Refusing loudly is the only honest
   downgrade; the alternatives are dropping the row's data silently, or failing
   with a bare driver `NOT NULL` violation that names a column instead of the
   agent an operator has to go fix.

Follows `test_migration_0020_blank_overrides.py`: a throwaway database all to
itself via `isolated_migration_db`, real Postgres, no mocking.
"""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from curie_api.config import get_settings
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql import text

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"

# The revision immediately below 0021. Targeted explicitly rather than as a
# relative "-1": a later migration moving head would make "-1" stop short of
# undoing 0021, the backfill would never re-run on the seeded rows, and the test
# would go green while proving nothing (the trap #1391 documents).
BELOW = "0020"

# The constraint name migration 0017 chose. `apps/api/src/curie_api/routers/
# agents.py` keys its 409 message map on this literal, so a downgrade that
# restores the constraint under a Postgres-generated name silently downgrades
# the conflict to a 500.
LEGACY_CONSTRAINT = "agents_slack_channel_key"


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[Any]:
    """Run one statement against the isolated migration database."""

    async def _go() -> list[Any]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                result = await conn.execute(text(statement), params or {})
                return list(result.all()) if result.returns_rows else []
        finally:
            await engine.dispose()

    return asyncio.run(_go())


def _seed_pre_0021_agent(name: str, channel: str) -> uuid.UUID:
    """Insert one agent on the PRE-0021 schema, straight into the old column.

    Args:
        name: agent name (unique).
        channel: the value the old `agents.slack_channel` column holds.

    Returns:
        The new agent's id.
    """
    agent_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.agents (id, name, slack_channel) VALUES (:id, :name, :ch)",
        {"id": agent_id, "name": name, "ch": channel},
    )
    return agent_id


def _alembic_config() -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(ALEMBIC_DIR))
    return cfg


def _column_exists(table: str, column: str) -> bool:
    rows = _sql(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = :t AND column_name = :c",
        {"t": table, "c": column},
    )
    return bool(rows)


def _is_nullable(table: str, column: str) -> bool:
    rows = _sql(
        "SELECT is_nullable FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = :t AND column_name = :c",
        {"t": table, "c": column},
    )
    assert rows, f"curie.{table}.{column} does not exist"
    return rows[0][0] == "YES"


def _constraint_named(name: str) -> bool:
    """Look the constraint up BY NAME in the catalog.

    Deliberately not a shape check: a unique constraint restored under a
    generated name has the right shape and the wrong identity, which is exactly
    the failure this asserts against.
    """
    rows = _sql(
        "SELECT 1 FROM pg_constraint c "
        "JOIN pg_class t ON t.oid = c.conrelid "
        "JOIN pg_namespace n ON n.oid = t.relnamespace "
        "WHERE n.nspname = 'curie' AND c.conname = :name",
        {"name": name},
    )
    return bool(rows)


def test_the_upgrade_backfills_one_binding_per_agent_and_drops_the_column(
    isolated_migration_db: None,
) -> None:
    """T8, upgrade half / AC5. Existing installs migrate without data loss."""

    cfg = _alembic_config()
    command.upgrade(cfg, "head")
    command.downgrade(cfg, BELOW)

    bindings = {
        _seed_pre_0021_agent("mig-a", "C0EXAMPLE1"): "C0EXAMPLE1",
        _seed_pre_0021_agent("mig-b", "C0EXAMPLE2"): "C0EXAMPLE2",
        _seed_pre_0021_agent("mig-c", "C0EXAMPLE3"): "C0EXAMPLE3",
    }

    command.upgrade(cfg, "head")

    rows = _sql(
        "SELECT agent_id, kind, address FROM curie.agent_channels ORDER BY address"
    )
    assert {r[0]: r[2] for r in rows} == bindings
    # Every backfilled row is slack-kind: the old column could only ever have
    # held a Slack channel id, and inventing any other kind here would be a
    # fabricated fact about an existing install.
    assert {r[1] for r in rows} == {"slack"}
    # No agent gains a second binding, and none is left unbound.
    assert len(rows) == len(bindings)

    # The old column is gone, not merely ignored. Leaving it in place beside the
    # new table is the compatibility path this rename exists to avoid: two
    # sources of truth for one fact, silently diverging on the next write.
    assert not _column_exists("agents", "slack_channel")


def test_the_downgrade_restores_the_column_its_not_null_and_its_named_constraint(
    isolated_migration_db: None,
) -> None:
    """T8, downgrade half / AC5. Restoring the values is not enough.

    The three properties asserted here are the ones a "recreate the column and
    copy the data back" downgrade gets wrong, each with a distinct consequence:
    values (data loss), NOT NULL (a re-upgrade's duplicate/NULL pre-flight check
    stops guarding), and the constraint's NAME (the API's 409 map misses and the
    #38 conflict becomes a 500).
    """

    cfg = _alembic_config()
    command.upgrade(cfg, "head")
    command.downgrade(cfg, BELOW)

    bindings = {
        _seed_pre_0021_agent("down-a", "C0EXAMPLE1"): "C0EXAMPLE1",
        _seed_pre_0021_agent("down-b", "C0EXAMPLE2"): "C0EXAMPLE2",
    }

    command.upgrade(cfg, "head")
    command.downgrade(cfg, BELOW)

    restored = _sql("SELECT id, slack_channel FROM curie.agents ORDER BY slack_channel")
    assert {r[0]: r[1] for r in restored} == bindings
    assert not _is_nullable("agents", "slack_channel")
    assert _constraint_named(LEGACY_CONSTRAINT), (
        f"the downgrade must restore the unique constraint as {LEGACY_CONSTRAINT!r}: "
        "the API's 409 message map is keyed on that exact name, and a "
        "Postgres-generated name turns the #38 conflict into an opaque 500"
    )

    # And the round trip closes: re-upgrading finds the restored column and
    # rebuilds the same bindings, which a downgrade that quietly lost NOT NULL
    # or the constraint would not survive.
    command.upgrade(cfg, "head")
    again = _sql("SELECT agent_id, kind, address FROM curie.agent_channels")
    assert {r[0]: r[2] for r in again} == bindings
    assert {r[1] for r in again} == {"slack"}


def test_the_upgrade_is_refused_rather_than_guessed_when_the_column_holds_a_duplicate(
    isolated_migration_db: None,
) -> None:
    """T8's pre-flight half. Migration 0017 already guarantees address
    uniqueness, so the backfill provably cannot collide -- but "provably" rests
    on 0017 having run, and a database restored from a pre-0017 dump has not.
    Assert the guarantee instead of assuming it, in 0017's own actionable style:
    name every offending address rather than emitting a bare duplicate-key error
    that names one.
    """

    cfg = _alembic_config()
    command.upgrade(cfg, "head")
    command.downgrade(cfg, BELOW)

    _seed_pre_0021_agent("dupe-a", "C0EXAMPLE1")
    # Reachable only out of band, which is the point: 0017's constraint is what
    # normally forbids it, and this proves 0021 does not simply inherit that
    # assumption unchecked.
    _sql(f"ALTER TABLE curie.agents DROP CONSTRAINT {LEGACY_CONSTRAINT}")
    _seed_pre_0021_agent("dupe-b", "C0EXAMPLE1")

    with pytest.raises(Exception) as caught:
        command.upgrade(cfg, "head")
    assert "C0EXAMPLE1" in str(caught.value), caught.value


@pytest.mark.parametrize("kind", ["webhook", "teams"])
def test_the_downgrade_refuses_a_non_slack_binding(
    isolated_migration_db: None, kind: str
) -> None:
    """T9 / AC6, first unrepresentable state. A non-`slack` binding has no
    representation in a single `slack_channel` column, so writing its address
    there would silently relabel it as Slack: the agent looks bound, and the
    worker routes a Slack turn to it that it was never meant to serve.
    """

    cfg = _alembic_config()
    command.upgrade(cfg, "head")
    command.downgrade(cfg, BELOW)
    agent_id = _seed_pre_0021_agent("non-slack", "C0EXAMPLE1")
    command.upgrade(cfg, "head")

    _sql(
        # Route-less, as any other kind may be: a Slack row's identity is no
        # route for it (migration 0061's agent_channels_route_ck).
        "UPDATE curie.agent_channels SET kind = :kind, address = :addr, adapter = NULL "
        "WHERE agent_id = :id",
        {"kind": kind, "addr": "acme-room-7", "id": agent_id},
    )

    with pytest.raises(Exception) as caught:
        command.downgrade(cfg, BELOW)
    message = str(caught.value)
    assert "non-slack" in message, message
    assert kind in message, message


def test_the_downgrade_refuses_an_agent_with_no_binding(
    isolated_migration_db: None,
) -> None:
    """T9 / AC6, second unrepresentable state, and the one an implementation is
    most likely to leave to the driver.

    An unbound agent has no value to put in a `NOT NULL` column. Without an
    explicit pre-flight the downgrade fails anyway, but with a bare
    `null value in column "slack_channel" violates not-null constraint`, which
    names a column and leaves the operator to work out which of their agents is
    the problem. The error must name the AGENT.
    """

    cfg = _alembic_config()
    command.upgrade(cfg, "head")
    command.downgrade(cfg, BELOW)
    bound = _seed_pre_0021_agent("still-bound", "C0EXAMPLE1")
    orphan = _seed_pre_0021_agent("unbound-agent", "C0EXAMPLE2")
    command.upgrade(cfg, "head")

    # Reachable only out of band: the API requires a binding on create and
    # rejects an explicit null on PATCH. Out of band is exactly the state a
    # downgrade has to survive.
    _sql("DELETE FROM curie.agent_channels WHERE agent_id = :id", {"id": orphan})

    with pytest.raises(Exception) as caught:
        command.downgrade(cfg, BELOW)
    message = str(caught.value)
    assert "unbound-agent" in message, message
    # The healthy agent is not blamed for its neighbour's state.
    assert "still-bound" not in message, message
    assert str(bound) not in message, message
