"""Migration 0023 widens binding uniqueness from `address` to `(kind, address)`.

`0023_agent_channels_kind_address_unique.py` drops `agent_channels_address_key`
and creates `agent_channels_kind_address_key`. Two things about it are
load-bearing beyond the DDL:

1. **The constraint's NAME is a contract.** `routers/agents.py` keys its 409
   message map on the literal name, so a constraint created under a
   Postgres-generated name satisfies every shape check and turns #38's
   user-facing conflict into an opaque 500 (the same trap migration 0021's test
   documents for `agents_slack_channel_key`).

2. **The downgrade cannot be an unconditional swap.** Once two kinds share an
   address, restoring an address-only constraint has no honest answer -- one of
   the two agents would have to lose its binding. It pre-flights and refuses by
   name, listing BOTH rows, rather than failing with a bare duplicate-key error
   that names one.

Ordering note the plan (section 6.1) makes explicit: this migration is what makes
an OLD address-only worker dangerous rather than merely stale, because it is what
permits two kinds to share an address. The cutover runs it only after proving
zero old worker pods are running; T-A15 is the executable proof of what an old
consumer does with a new turn.

Follows `test_migration_0021_agent_channels.py`: a throwaway database all to
itself via `isolated_migration_db`, real Postgres, no mocking.
"""

from __future__ import annotations

import uuid

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, constraint_exists, sql_rows
from alembic import command
from alembic.config import Config
from sqlalchemy.exc import IntegrityError

# Targeted explicitly, never as a relative "-1": a later migration moving head
# would make "-1" stop short of undoing 0023 and the test would go green while
# proving nothing (#1391).
BELOW = "0022"
REVISION = "0023"

OLD_CONSTRAINT = "agent_channels_address_key"
NEW_CONSTRAINT = "agent_channels_kind_address_key"
# NOT touched by this migration: one agent still binds one channel (ADR-0089).
AGENT_CONSTRAINT = "agent_channels_agent_id_key"


def _seed_binding(name: str, kind: str, address: str) -> uuid.UUID:
    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": name},
    )
    sql_rows(
        "INSERT INTO curie.agent_channels (id, agent_id, kind, address) "
        "VALUES (:id, :agent, :kind, :addr)",
        {"id": uuid.uuid4(), "agent": agent_id, "kind": kind, "addr": address},
    )
    return agent_id


def _at_below(db: IsolatedMigrationDb) -> Config:
    db.at(BELOW)
    return alembic_config()


def test_the_upgrade_lets_two_kinds_share_one_address(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """T-A6 at the database / AC5. The widening asserted as BEHAVIOR: the insert
    that the old constraint refused now succeeds, and the pair itself is still
    unique.

    A schema assertion alone would pass against a constraint created on
    `(address, kind)` in the wrong order or on the wrong table; the two inserts
    are what prove which rows the database now accepts and which it still
    refuses.
    """

    cfg = _at_below(isolated_migration_db)
    _seed_binding("slack-agent", "slack", "shared@example.test")

    command.upgrade(cfg, REVISION)

    _seed_binding("mail-agent", "email", "shared@example.test")
    rows = sql_rows(
        "SELECT kind FROM curie.agent_channels WHERE address = 'shared@example.test' "
        "ORDER BY kind"
    )
    assert [row[0] for row in rows] == ["email", "slack"]

    # The pair is still identity: a THIRD agent on the same pair is refused.
    with pytest.raises(IntegrityError) as caught:
        _seed_binding("mail-agent-2", "email", "shared@example.test")
    assert NEW_CONSTRAINT in str(caught.value), caught.value

    # And the named contract moved, in both directions.
    assert constraint_exists(NEW_CONSTRAINT)
    assert not constraint_exists(OLD_CONSTRAINT)
    # One agent still binds one channel; this migration must not touch it.
    assert constraint_exists(AGENT_CONSTRAINT)


def test_the_downgrade_refuses_and_names_both_rows_sharing_an_address(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """T-A9 / AC5. Two kinds at one address have no representation under an
    address-only constraint, so the only honest downgrade is a refusal that names
    every offending row -- not a bare `duplicate key value violates unique
    constraint` that names one and leaves the operator to find the other.
    """

    cfg = _at_below(isolated_migration_db)
    command.upgrade(cfg, REVISION)
    _seed_binding("slack-agent", "slack", "shared@example.test")
    _seed_binding("mail-agent", "email", "shared@example.test")

    with pytest.raises(Exception) as caught:
        command.downgrade(cfg, BELOW)

    message = str(caught.value)
    assert "shared@example.test" in message, message
    assert "slack" in message, message
    assert "email" in message, message
    # The refusal was total: both bindings survive.
    assert len(sql_rows("SELECT 1 FROM curie.agent_channels")) == 2


def test_the_downgrade_round_trips_when_no_address_is_shared(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """T-A9's positive control, and the round trip 0021's test established as the
    discipline here: a downgrade that refused unconditionally would satisfy the
    refusal test above while being unusable, and a downgrade that restored the
    old constraint under a generated name would break the API's 409 map.
    """

    cfg = _at_below(isolated_migration_db)
    command.upgrade(cfg, REVISION)
    _seed_binding("slack-agent", "slack", "C0EXAMPLE1")
    _seed_binding("mail-agent", "email", "ops@example.test")

    command.downgrade(cfg, BELOW)

    assert constraint_exists(OLD_CONSTRAINT)
    assert not constraint_exists(NEW_CONSTRAINT)
    assert len(sql_rows("SELECT 1 FROM curie.agent_channels")) == 2

    command.upgrade(cfg, REVISION)
    assert constraint_exists(NEW_CONSTRAINT)
