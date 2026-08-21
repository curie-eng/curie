"""A v0.6.x database must be able to reach head (#1705).

Revision id `0021` means `console_sessions` on the v0.6.x line and
`agent_channels` on this one, and both sides declare `down_revision = "0020"`.
So a v0.6.x database arrives already stamped `0021`, this chain concludes
`0021_agent_channels` has run, skips it, and `0022` joins a `curie.agent_channels`
that was never created. `0021a_agent_channels_reconcile` closes that, and `0026`
then has to skip a `console_sessions` table the database already holds.

No gate in this repo saw it, because every tier builds its database from zero.
This test builds one that does not: this tree's own `0001`..`0020` (byte
identical on both release lines), plus the `console_sessions` DDL that shipped
as v0.6.2's `0021`, stamped `0021`. That DDL is written out here rather than
borrowed from `0026`: it is frozen released history, and a fixture that read the
current tree for it would stop describing what operators actually hold the
moment that revision is touched.

Two separate failures are asserted, because the first one hides the second, and
because reaching head is not on its own the property that matters. 0021's own
docstring is explicit that creating `agent_channels` without the
`INSERT ... SELECT` produces a perfectly valid empty table and silently unbinds
every agent in the install, so a reconciliation that skipped the backfill would
satisfy a "reaches head" assertion while being worse than the crash it replaced.

Follows `test_migration_0021_agent_channels.py`: a throwaway database all to
itself via `isolated_migration_db`, real Postgres, no mocking.
"""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import Any

from alembic import command
from alembic.config import Config
from curie_api.config import get_settings
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql import text

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"

# The last revision the two release lines agree on. Above it, `0021` forks.
SHARED = "0020"

# The stamp a v0.6.x database carries: its own `0021_console_sessions`.
V062_HEAD = "0021"

# The DDL v0.6.2's `0021_console_sessions` left behind, transcribed. Renumbered
# to `0026` on this line with its statements unchanged, which is why the upgrade
# path finds the table already there.
V062_CONSOLE_SESSIONS_DDL = """
    CREATE TABLE curie.console_sessions (
        id UUID NOT NULL,
        login_code_hash VARCHAR NOT NULL,
        login_code_expires_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        session_token_hash VARCHAR,
        session_expires_at TIMESTAMP WITHOUT TIME ZONE,
        consumed_at TIMESTAMP WITHOUT TIME ZONE,
        revoked_at TIMESTAMP WITHOUT TIME ZONE,
        created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL,
        PRIMARY KEY (id)
    )
"""
V062_CONSOLE_SESSIONS_INDEXES = (
    "CREATE UNIQUE INDEX ix_console_sessions_login_code_hash "
    "ON curie.console_sessions (login_code_hash)",
    "CREATE UNIQUE INDEX ix_console_sessions_session_token_hash "
    "ON curie.console_sessions (session_token_hash)",
)

BINDINGS = (("acme-bot", "C0EXAMPLE1"), ("acme-ops", "C0EXAMPLE2"))


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


def _alembic_config() -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(ALEMBIC_DIR))
    return cfg


def _seed_v062_database() -> None:
    """Build a database in exactly the shape v0.6.2 leaves behind."""

    command.upgrade(_alembic_config(), SHARED)
    for name, channel in BINDINGS:
        _sql(
            "INSERT INTO curie.agents (id, name, slack_channel) VALUES (:id, :name, :ch)",
            {"id": uuid.uuid4(), "name": name, "ch": channel},
        )
    _sql(V062_CONSOLE_SESSIONS_DDL)
    for statement in V062_CONSOLE_SESSIONS_INDEXES:
        _sql(statement)
    _sql(
        "UPDATE curie.alembic_version SET version_num = :rev",
        {"rev": V062_HEAD},
    )


def _stamped_revision() -> str:
    rows = _sql("SELECT version_num FROM curie.alembic_version")
    assert len(rows) == 1, rows
    revision: str = rows[0][0]
    return revision


def test_v0_6_x_database_reaches_head_with_its_bindings_carried_over(
    isolated_migration_db: None,
) -> None:
    _seed_v062_database()
    assert _stamped_revision() == V062_HEAD

    command.upgrade(_alembic_config(), "head")

    assert _stamped_revision() == "0028"

    # The backfill IS the migration: an empty table here is every agent
    # deployed, healthy looking and unroutable.
    bindings = _sql(
        "SELECT a.name, c.kind, c.address FROM curie.agent_channels c "
        "JOIN curie.agents a ON a.id = c.agent_id ORDER BY a.name"
    )
    assert [tuple(row) for row in bindings] == [
        (name, "slack", channel) for name, channel in BINDINGS
    ]

    # The legacy column and its named constraint went with it, exactly as they
    # do on a fresh install.
    assert (
        _sql(
            "SELECT 1 FROM information_schema.columns WHERE table_schema = 'curie' "
            "AND table_name = 'agents' AND column_name = 'slack_channel'"
        )
        == []
    )

    # Re-running the upgrade against the already-upgraded database is a no-op.
    command.upgrade(_alembic_config(), "head")
    assert _stamped_revision() == "0028"
    assert len(_sql("SELECT 1 FROM curie.agent_channels")) == len(BINDINGS)


def test_v0_6_x_console_sessions_survives_the_upgrade(
    isolated_migration_db: None,
) -> None:
    _seed_v062_database()

    command.upgrade(_alembic_config(), "head")

    indexes = _sql(
        "SELECT indexname FROM pg_indexes WHERE schemaname = 'curie' "
        "AND tablename = 'console_sessions' ORDER BY indexname"
    )
    assert [row[0] for row in indexes] == [
        "console_sessions_pkey",
        "ix_console_sessions_login_code_hash",
        "ix_console_sessions_session_token_hash",
    ]
