"""Migration 0020 repairs blank operator overrides at rest; this proves it does.

`0020_blank_overrides_to_null.py` collapses `agents.model` and `agents.thinking`
rows holding a blank string to NULL. The predicate IS the migration -- there is
no schema change to fall back on -- and until #1391 nothing exercised it:
replacing `upgrade()`'s body with `return None` was silent across the full API
suite, so the largest possible mutation went unnoticed.

Why the repair matters: `apply_model_env` reads each override as
``override if override is not None else config.<field>``. A blank is not None,
so it wins the ternary, and is then falsy, so NO boot key is emitted -- the agent
silently skips the platform default an operator configured and takes the model's
own built-in instead. #1355 taught the API to refuse a blank on write, but
released images accepted one, so rows written before it hold the defect frozen
in data where no validator will ever see them again.

The seed values are chosen deliberately rather than mirroring the two the
migration's author had in mind, since a test built from the author's own mental
model can only confirm it (#1391 makes exactly this point).
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config
from alembic import command
from curie_api.config import get_settings
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql import text

# Blanks the migration is responsible for. ASCII whitespace an operator or a
# pre-#1355 client could realistically have stored: an empty value, spaces, and
# the three characters a paste or a heredoc drags in.
CLEARED = {
    "empty": "",
    "spaces": "   ",
    "tab": "\t ",
    "newline": "\n",
    "carriage-return": "\r",
}

# Real values, present to prove the predicate only TESTS a value and never
# rewrites one. `enabled:2000` is included because it contains a colon and a
# digit run, which a sloppier predicate might treat as structure.
KEPT = {"model": "kimi-k2", "thinking": "enabled:2000"}

# The measured edge of the SQL predicate, pinned so it is a known boundary
# rather than an unknown gap. See the migration's own note: Postgres's
# `[[:space:]]` is narrower than Python's `str.strip()`. Measured on
# postgres:16-alpine at en_US.utf8, U+00A0 is NOT matched and therefore
# survives. That is accepted, not a defect: no shipped client can write it (the
# console trims, the create modal trims and omits, the CLI refuses a blank), and
# such a row behaves identically before and after #1374. Pinned so a future
# predicate change has to face the decision instead of drifting into it.
SURVIVES_BY_DESIGN = " "


def _seed_agent(name: str, channel: str, model: str | None, thinking: str | None) -> uuid.UUID:
    """Insert one agent row with raw override values, bypassing the API validator.

    The point is to create rows the validator would refuse today, which is
    exactly the state a database written by a released image can be in.

    Still writes `agents.slack_channel`, and that is correct after migration
    0021 removed the column (ADR-0096, #1459): every test in this module
    downgrades to 0019 BEFORE seeding, so the schema under the insert is always
    the pre-0021 one that still has it. Rewriting these inserts to
    `agent_channels` would fail against the schema they actually run on, and
    that is the intermittent, migration-state-dependent failure EB-23 flags.
    The forward path is covered too: `command.upgrade(cfg, "head")` below now
    runs 0021 as well, so 0020's repair is asserted through 0021's backfill.

    Args:
        name: agent name (unique).
        channel: Slack channel id (unique on the pre-0021 schema).
        model: the raw `model` value to store.
        thinking: the raw `thinking` value to store.

    Returns:
        The new agent's id.
    """
    agent_id = uuid.uuid4()

    async def _go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO curie.agents (id, name, slack_channel, model, thinking) "
                        "VALUES (:id, :name, :ch, :model, :thinking)"
                    ),
                    {
                        "id": agent_id,
                        "name": name,
                        "ch": channel,
                        "model": model,
                        "thinking": thinking,
                    },
                )
        finally:
            await engine.dispose()

    asyncio.run(_go())
    return agent_id


def _read_overrides(agent_id: uuid.UUID) -> tuple[str | None, str | None]:
    """Read one agent's stored overrides straight from the table.

    Args:
        agent_id: the row to read.

    Returns:
        `(model, thinking)` exactly as stored, NULL included.
    """

    async def _go() -> tuple[str | None, str | None]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                row = (
                    await conn.execute(
                        text("SELECT model, thinking FROM curie.agents WHERE id = :id"),
                        {"id": agent_id},
                    )
                ).one()
            return (row[0], row[1])
        finally:
            await engine.dispose()

    return asyncio.run(_go())


def test_the_backfill_clears_blank_overrides_and_leaves_real_values_alone(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """The whole substance of 0020, on rows seeded before it runs.

    Targets 0019 explicitly rather than a relative "-1": a later migration moving
    head would make "-1" stop short of undoing 0020, and the backfill would never
    re-run on the seeded rows -- the failure mode that makes a green migration
    test meaningless.
    """
    cfg = alembic_config()
    isolated_migration_db.at("head")
    command.downgrade(cfg, "0019")

    blank_ids: dict[str, uuid.UUID] = {}
    for i, (label, blank) in enumerate(CLEARED.items()):
        # Both columns blank on the same row, so neither is proved only by its
        # sibling's statement having run.
        blank_ids[label] = _seed_agent(f"blank-{label}", f"CMIGBLNK{i}", blank, blank)

    healthy = _seed_agent("healthy", "CMIGHEAL0", KEPT["model"], KEPT["thinking"])
    # One column blank, the other real: the per-column WHERE has to spare the
    # sibling on the SAME row, which a row-level repair would not.
    mixed = _seed_agent("mixed", "CMIGMIXD0", "", KEPT["thinking"])
    already_null = _seed_agent("already-null", "CMIGNULL0", None, None)

    command.upgrade(cfg, "head")

    for label, agent_id in blank_ids.items():
        assert _read_overrides(agent_id) == (None, None), f"{label!r} was not cleared"
    assert _read_overrides(healthy) == (KEPT["model"], KEPT["thinking"])
    assert _read_overrides(mixed) == (None, KEPT["thinking"])
    assert _read_overrides(already_null) == (None, None)


def test_the_backfill_is_idempotent_and_the_revision_round_trips(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """Re-running must be a no-op, and the downgrade must not undo the repair.

    The downgrade is deliberately empty: the upgrade merges `''` and NULL, so
    which NULLs were once blanks is unrecoverable, and re-blanking every
    correctly-NULL row would be strictly worse than leaving the repair standing.
    This pins that as intended rather than as an omission.
    """
    cfg = alembic_config()
    isolated_migration_db.at("head")
    command.downgrade(cfg, "0019")

    blanked = _seed_agent("round-trip", "CMIGRTRP0", "  ", "")
    healthy = _seed_agent("round-trip-ok", "CMIGRTOK0", KEPT["model"], None)

    command.upgrade(cfg, "head")
    assert _read_overrides(blanked) == (None, None)

    command.downgrade(cfg, "0019")
    assert _read_overrides(blanked) == (None, None), "the downgrade must not re-blank"

    command.upgrade(cfg, "head")
    assert _read_overrides(blanked) == (None, None)
    assert _read_overrides(healthy) == (KEPT["model"], None)


def test_a_unicode_blank_survives_and_that_boundary_is_deliberate(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """Pin the measured edge of the predicate instead of leaving it unknown.

    Postgres's `[[:space:]]` is NARROWER than Python's `str.strip()`. Measured on
    postgres:16-alpine at en_US.utf8, `str.strip()` treats all of U+001C..U+001F,
    U+00A0, U+1680 and U+202F as whitespace while the SQL class does not, so a
    row holding one of those is not collapsed.

    Accepted, for a reason that has to keep holding: no shipped client can write
    such a value. If one ever can, this test is where that assumption is written
    down, and it fails loudly the moment someone widens the predicate without
    deciding to.
    """
    cfg = alembic_config()
    isolated_migration_db.at("head")
    command.downgrade(cfg, "0019")

    exotic = _seed_agent("nbsp", "CMIGNBSP0", SURVIVES_BY_DESIGN, SURVIVES_BY_DESIGN)
    # Python agrees it is blank; the SQL predicate does not. That disagreement is
    # the whole point of pinning it.
    assert SURVIVES_BY_DESIGN.strip() == ""

    command.upgrade(cfg, "head")

    assert _read_overrides(exotic) == (SURVIVES_BY_DESIGN, SURVIVES_BY_DESIGN)


@pytest.mark.parametrize("column", ["model", "thinking"])
def test_each_column_is_repaired_independently(
    isolated_migration_db: IsolatedMigrationDb, column: str
) -> None:
    """Both columns, asserted separately, so one statement covering the other is
    not mistaken for both working. 0020 loops over a NAMED list; a third override
    added to the schema and not to that list would slip through, and this is the
    shape that notices."""
    cfg = alembic_config()
    isolated_migration_db.at("head")
    command.downgrade(cfg, "0019")

    values: dict[str, Any] = {"model": None, "thinking": None}
    values[column] = "   "
    agent_id = _seed_agent(f"only-{column}", f"CMIGONLY{column[0].upper()}", **values)

    command.upgrade(cfg, "head")

    assert _read_overrides(agent_id) == (None, None)
