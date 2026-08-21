"""Migration 0028 drops the one-binding-per-agent constraint (ADR-0116, #1459).

`0028_agent_channels_multi_binding.py` drops `agent_channels_agent_id_key`
so one agent may hold more than one row in `agent_channels`. Two things about
it are load-bearing beyond the DDL:

1. **`agent_channels_kind_address_key` (0023) is deliberately untouched.** An
   agent may now hold many bindings, but a `(kind, address)` pair still
   identifies at most one binding -- two agents still cannot claim the same
   channel. This revision widens the agent-side constraint only.

2. **The downgrade cannot be an unconditional restore.** Once an agent holds
   two bindings, collapsing back onto an agent-unique constraint has no
   honest answer -- one of the two bindings would have to be discarded. It
   pre-flights and refuses by name, listing every row for the offending
   agent, rather than failing with a bare duplicate-key error that names one.

Follows `test_migration_0023_agent_channels_kind_address_unique.py`: a
throwaway database all to itself via `isolated_migration_db`, real Postgres,
no mocking.
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
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql import text

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"

# Targeted explicitly, never as a relative "-1": a later migration moving head
# would make "-1" stop short of undoing 0028 and the test would go green
# while proving nothing (#1391).
BELOW = "0027"
REVISION = "0028"

OLD_CONSTRAINT = "agent_channels_agent_id_key"
# NOT touched by this migration: two agents still cannot share one (kind,
# address) pair (0023).
KIND_ADDRESS_CONSTRAINT = "agent_channels_kind_address_key"
# The plain replacement for the dropped constraint's backing index.
AGENT_ID_INDEX = "ix_agent_channels_agent_id"


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[Any]:
    async def _go() -> list[Any]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                result = await conn.execute(text(statement), params or {})
                return list(result.all()) if result.returns_rows else []
        finally:
            await engine.dispose()

    return asyncio.run(_go())


def _alembic_config() -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(ALEMBIC_DIR))
    return cfg


def _constraint_named(name: str) -> bool:
    """Look the constraint up BY NAME in the catalog.

    Deliberately not a shape check: a unique constraint created under a
    generated name has the right shape and the wrong identity, which is the
    failure the API's 409 map trips over (0023's discipline, unchanged here).
    """

    rows = _sql(
        "SELECT 1 FROM pg_constraint c "
        "JOIN pg_class t ON t.oid = c.conrelid "
        "JOIN pg_namespace n ON n.oid = t.relnamespace "
        "WHERE n.nspname = 'curie' AND c.conname = :name",
        {"name": name},
    )
    return bool(rows)


def _agent_id_indexes() -> list[tuple[str, bool]]:
    """Every single-column index on `agent_channels.agent_id`, name + uniqueness.

    Asked as "what indexes this column", not "does this name exist", because
    the property the revision owes is that the column stays indexed at all:
    dropping the unique constraint takes its backing index with it, and that
    index was the only one on `agent_id`.
    """

    rows = _sql(
        """
        SELECT i.relname AS name, ix.indisunique AS is_unique
        FROM pg_index ix
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ix.indkey[0]
        WHERE n.nspname = 'curie'
          AND t.relname = 'agent_channels'
          AND ix.indnatts = 1
          AND a.attname = 'agent_id'
        ORDER BY i.relname
        """
    )
    return [(row.name, row.is_unique) for row in rows]


def _seed_agent(name: str) -> uuid.UUID:
    agent_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": name},
    )
    return agent_id


def _seed_channel(agent_id: uuid.UUID, kind: str, address: str) -> uuid.UUID:
    channel_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.agent_channels (id, agent_id, kind, address) "
        "VALUES (:id, :agent, :kind, :addr)",
        {"id": channel_id, "agent": agent_id, "kind": kind, "addr": address},
    )
    return channel_id


def _seed_binding(name: str, kind: str, address: str) -> uuid.UUID:
    agent_id = _seed_agent(name)
    _seed_channel(agent_id, kind, address)
    return agent_id


def _at_below() -> Config:
    cfg = _alembic_config()
    command.upgrade(cfg, BELOW)
    return cfg


def test_upgrade_drops_only_the_agent_id_constraint(
    isolated_migration_db: None,
) -> None:
    """[FAIL-FIRST] Upgrade must remove ONLY the agent_id uniqueness (ADR-0116:
    an agent may hold more than one binding) and must not disturb the
    kind+address identity constraint 0023 established -- that pair-uniqueness
    is untouched by this revision.

    Checked BY NAME, not by shape: a same-shape constraint created under a
    generated name would satisfy a naive presence check while breaking any
    code that references either constraint literally.
    """

    cfg = _at_below()
    assert _constraint_named(OLD_CONSTRAINT)
    assert _constraint_named(KIND_ADDRESS_CONSTRAINT)

    command.upgrade(cfg, REVISION)

    assert not _constraint_named(OLD_CONSTRAINT)
    assert _constraint_named(KIND_ADDRESS_CONSTRAINT)


def test_agent_id_is_still_indexed_after_the_constraint_is_dropped(
    isolated_migration_db: None,
) -> None:
    """[FAIL-FIRST] Dropping the unique constraint drops its backing index, and
    that index was the ONLY one on `agent_id`. Every binding write locks the
    agent's set first (`crud.lock_agent_bindings` filters and orders by
    `agent_id` under `FOR UPDATE`), so an unindexed column turns each add, move
    and delete into a full scan taken while holding locks -- widening the very
    contention window this revision makes reachable.

    Asserted as "what indexes this column", not "does this name exist": a
    replacement created on the wrong column, or on `(agent_id, kind)` with
    `kind` leading, would satisfy a by-name check and leave the lookup
    unindexed. Uniqueness is asserted too -- a unique replacement would restore
    the one-binding-per-agent restriction this revision exists to remove.
    """

    cfg = _at_below()
    assert _agent_id_indexes() == [(OLD_CONSTRAINT, True)]

    command.upgrade(cfg, REVISION)

    assert _agent_id_indexes() == [(AGENT_ID_INDEX, False)]


def test_downgrade_leaves_exactly_the_pre_0028_indexes(
    isolated_migration_db: None,
) -> None:
    """[FAIL-FIRST] The downgrade's other half: the plain index must go back
    out with the constraint coming back in. The restored unique constraint
    brings its own index on the same column, so a downgrade that only recreated
    the constraint would leave the pre-0028 schema carrying a duplicate index
    it never had -- a write amplification that survives every later revision.
    """

    cfg = _at_below()
    command.upgrade(cfg, REVISION)
    _seed_binding("slack-agent", "slack", "C0EXAMPLE1")

    command.downgrade(cfg, BELOW)

    assert _agent_id_indexes() == [(OLD_CONSTRAINT, True)]


def test_two_bindings_for_one_agent_insert_after_upgrade(
    isolated_migration_db: None,
) -> None:
    """[FAIL-FIRST] The widening asserted as BEHAVIOR: the second insert the
    old constraint refused now succeeds. A schema-only check would pass
    against a migration that dropped the constraint under a different name
    while some other guard still refused the insert; only the actual INSERT
    proves which rows the database accepts post-upgrade.

    Also proves the negative first: against the pre-0028 schema, the second
    insert still raises IntegrityError -- confirming this is genuinely a
    widening this revision performs, not already-true behavior.
    """

    cfg = _at_below()
    agent_id = _seed_agent("multi-channel-agent")
    _seed_channel(agent_id, "slack", "C0EXAMPLE1")

    with pytest.raises(IntegrityError) as caught:
        _seed_channel(agent_id, "email", "ops@example.test")
    assert OLD_CONSTRAINT in str(caught.value), caught.value

    command.upgrade(cfg, REVISION)

    _seed_channel(agent_id, "email", "ops@example.test")
    rows = _sql(
        "SELECT kind FROM curie.agent_channels WHERE agent_id = :agent ORDER BY kind",
        {"agent": agent_id},
    )
    assert [row[0] for row in rows] == ["email", "slack"]


def test_a_duplicate_pair_still_raises_after_upgrade(
    isolated_migration_db: None,
) -> None:
    """[BASELINE-GREEN] The pair-identity constraint 0023 established is not
    this revision's concern (its docstring: `agent_channels_kind_address_key`
    is deliberately untouched) and must still refuse a duplicate (kind,
    address) after 0028 lands -- proven by execution, per AGENTS.md's
    guards-are-outcome-tested discipline, not by reading the DDL.

    Tagged BASELINE-GREEN: the property under test already holds today
    (0023 established it, five revisions before this one). Nothing 0028 adds
    is what makes this pass; only a REGRESSION in 0028 -- a migration that
    touches or drops the wrong constraint -- could break it.
    """

    cfg = _at_below()
    command.upgrade(cfg, REVISION)

    _seed_binding("slack-agent", "slack", "C0EXAMPLE1")
    with pytest.raises(IntegrityError) as caught:
        _seed_binding("slack-agent-2", "slack", "C0EXAMPLE1")
    assert KIND_ADDRESS_CONSTRAINT in str(caught.value), caught.value


def test_downgrade_refuses_by_name_when_an_agent_holds_two_bindings(
    isolated_migration_db: None,
) -> None:
    """[FAIL-FIRST] 0023's downgrade discipline, applied to the agent_id
    constraint this time: once an agent holds two bindings there is no
    honest single row to collapse onto, so the downgrade pre-flights and
    refuses BY NAME, naming the offending agent, its kind(s), and its row
    count, rather than failing with a bare duplicate-key error that names
    one binding and hides which agent it belongs to. The refusal must NOT
    name the bound addresses -- those would otherwise land in deployment
    logs (security).
    """

    cfg = _at_below()
    command.upgrade(cfg, REVISION)
    agent_id = _seed_agent("multi-channel-agent")
    _seed_channel(agent_id, "slack", "C0EXAMPLE1")
    _seed_channel(agent_id, "email", "ops@example.test")

    with pytest.raises(Exception) as caught:
        command.downgrade(cfg, BELOW)

    message = str(caught.value)
    assert str(agent_id) in message, message
    assert "email" in message, message
    assert "slack" in message, message
    assert "rows: 2" in message, message
    # Redaction is pinned: the addresses never appear in the refusal.
    assert "C0EXAMPLE1" not in message, message
    assert "ops@example.test" not in message, message
    # The refusal was total: both bindings survive.
    assert (
        len(
            _sql(
                "SELECT 1 FROM curie.agent_channels WHERE agent_id = :agent",
                {"agent": agent_id},
            )
        )
        == 2
    )
    assert not _constraint_named(OLD_CONSTRAINT)


def test_downgrade_restores_the_constraint_when_every_agent_holds_one(
    isolated_migration_db: None,
) -> None:
    """[FAIL-FIRST] The downgrade's positive control (0023's discipline
    again): a downgrade that refused unconditionally would satisfy the
    refusal test above while being unusable, and one that restored the
    constraint under a generated name would break any code that references
    it by the literal name `agent_channels_agent_id_key`.
    """

    cfg = _at_below()
    command.upgrade(cfg, REVISION)
    _seed_binding("slack-agent", "slack", "C0EXAMPLE1")
    _seed_binding("mail-agent", "email", "ops@example.test")

    command.downgrade(cfg, BELOW)

    assert _constraint_named(OLD_CONSTRAINT)
    assert len(_sql("SELECT 1 FROM curie.agent_channels")) == 2

    command.upgrade(cfg, REVISION)
    assert not _constraint_named(OLD_CONSTRAINT)
