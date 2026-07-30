"""Managed-Postgres swap validation (issue #283, part of the #84 relational-DB seam).

The relational-DB seam is deliberately un-abstracted: swapping the app's Postgres
for a managed instance (RDS / Cloud SQL / Neon) is a **connection-string change**
(``DATABASE_URL``), never a code change (see
``docs/interfaces/relational-db/INTERFACE.md`` and its DSN-swap guide).

This suite is the concrete proof of that story. The ``migrated`` fixture provisions
a throwaway database reached *purely by overriding* ``DATABASE_URL`` and runs
``alembic upgrade head`` against it — which is exactly the shape of pointing the app
at a managed Postgres: a different DSN, the same models and migration chain. If that
brings the schema up cleanly here, it brings it up cleanly on RDS/Cloud SQL/Neon
(all standard Postgres speaking the async ``asyncpg`` dialect).

On top of the clean-migration proof, these tests assert that the two documented
Postgres-isms — the only things that make the "just change the DSN" story leak, and
then only for a *non*-Postgres store — actually materialize as expected on any real
Postgres, so a managed-Postgres target is unaffected by them:

  1. ``sqlalchemy.dialects.postgresql.UUID`` primary/foreign keys land as the native
     Postgres ``uuid`` column type.
  2. The schema-scoped native enum ``Enum(Environment, name="environment",
     schema=SCHEMA)`` materializes as a ``CREATE TYPE`` in the ``curie`` schema
     with the expected labels.

Runs against the session's freshly-created, migrated disposable DB (conftest's
``migrated``), which requires the compose Postgres to be up.
"""

import asyncio
from typing import Any

from curie_api.config import get_settings
from curie_api.db import SCHEMA
from curie_api.models import Environment
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


def _query(sql: str, **params: Any) -> list[tuple[Any, ...]]:
    """Run a read query against the (managed-style) DSN-selected database."""

    async def run() -> list[tuple[Any, ...]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(text(sql), params)
                return [tuple(row) for row in result.fetchall()]
        finally:
            await engine.dispose()

    return asyncio.run(run())


# The app's UUID-keyed tables (schema-qualified into `curie`); every primary key
# is a postgresql.UUID column, so each must materialize as native `uuid`.
_UUID_PK_TABLES = ("agents", "agent_versions", "deployments")


def test_uuid_columns_materialize_as_native_uuid(migrated: None) -> None:
    """Ism #1: postgresql.UUID(as_uuid=True) PKs land as native `uuid` columns.

    A managed Postgres speaks this type natively, so the DSN-only swap is unaffected.
    """

    for table in _UUID_PK_TABLES:
        rows = _query(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = :schema AND table_name = :table "
            "AND column_name = 'id'",
            schema=SCHEMA,
            table=table,
        )
        assert rows == [("uuid",)], f"{SCHEMA}.{table}.id -> {rows} (expected uuid)"


def test_environment_enum_is_schema_scoped_native_type(migrated: None) -> None:
    """Ism #2: the native enum materializes as a CREATE TYPE in the `curie` schema.

    Assert the type exists, is an enum (typtype 'e'), lives in the app schema (not
    public), and carries exactly the Environment labels. A managed Postgres creates
    this type from the same migration verbatim.
    """

    rows = _query(
        # typtype is the internal "char" type; cast to text so the driver hands
        # back a str ('e' for enum) rather than a single-byte value.
        "SELECT n.nspname, t.typtype::text "
        "FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace "
        "WHERE t.typname = 'environment'"
    )
    assert rows == [(SCHEMA, "e")], f"environment type -> {rows}"

    labels = _query(
        "SELECT e.enumlabel FROM pg_enum e "
        "JOIN pg_type t ON t.oid = e.enumtypid "
        "JOIN pg_namespace n ON n.oid = t.typnamespace "
        "WHERE t.typname = 'environment' AND n.nspname = :schema "
        "ORDER BY e.enumsortorder",
        schema=SCHEMA,
    )
    got = [row[0] for row in labels]
    assert got == [e.value for e in Environment], got


def test_app_tables_are_schema_qualified(migrated: None) -> None:
    """All app tables land in the `curie` schema, never public.

    Confirms the schema-qualified-tables half of ism #2 end to end: the DSN-only
    swap places the whole app footprint under `curie` on the managed target.
    """

    public = _query("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    assert public == [], f"unexpected public tables: {public}"

    for table in _UUID_PK_TABLES:
        rows = _query(
            "SELECT 1 FROM pg_tables WHERE schemaname = :schema AND tablename = :table",
            schema=SCHEMA,
            table=table,
        )
        assert rows == [(1,)], f"{SCHEMA}.{table} missing after migration"
