"""Alembic environment (async engine, URL and metadata from the app).

The version table lives in the app's own ``curie`` schema, not ``public``.
Alembic defaults ``alembic_version`` to ``public``, which leaves the public
schema non-empty; when Curie shares a Postgres with Langfuse that trips
Langfuse's first-boot Prisma migration (P3005: "the database schema is not
empty"). Pinning ``version_table_schema`` keeps ``public`` untouched so a
co-tenant's migrations still see an empty public schema.
"""

import asyncio

from alembic import context
from curie_api import models  # noqa: F401  (register tables on the metadata)
from curie_api.config import get_settings
from curie_api.db import SCHEMA, Base
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

config = context.config
config.set_main_option("sqlalchemy.url", get_settings().database_url)
target_metadata = Base.metadata


def _relocate_legacy_version_table(connection: Connection) -> None:
    """Move a pre-existing ``public.alembic_version`` into the app schema.

    Dev databases created before this change track their revision in
    ``public.alembic_version``. Switching ``version_table_schema`` without
    carrying that row over would make Alembic see the app schema as unversioned
    and try to re-run every migration against already-existing tables. Relocating
    the table preserves the recorded revision AND empties the public schema, so
    the transition is a no-op for the app and unblocks a Langfuse co-tenant. On a
    fresh database neither table exists yet and this is a no-op.
    """
    public_tbl = connection.exec_driver_sql("SELECT to_regclass('public.alembic_version')").scalar()
    schema_tbl = connection.exec_driver_sql(
        f"SELECT to_regclass('{SCHEMA}.alembic_version')"
    ).scalar()
    if public_tbl is not None and schema_tbl is None:
        connection.exec_driver_sql(f'ALTER TABLE public.alembic_version SET SCHEMA "{SCHEMA}"')


def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        version_table_schema=SCHEMA,
    )
    with context.begin_transaction():
        # The app schema must exist before Alembic manages the version table in
        # it, and any legacy public.alembic_version must be carried over first.
        connection.exec_driver_sql(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
        _relocate_legacy_version_table(connection)
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        version_table_schema=SCHEMA,
    )
    with context.begin_transaction():
        # Emit the schema creation before anything else so the generated SQL
        # creates curie.alembic_version into an existing schema (Alembic writes
        # the version table before migration 0001's CREATE SCHEMA would run).
        context.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
        context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_async_migrations())
