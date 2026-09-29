"""Shared helpers for the alembic migration tests.

The migration tests used to paste the same alembic config, SQL runner and
catalog lookups into every module. They live here once. ``conftest.py`` puts
this directory on ``sys.path`` (the suite runs under ``--import-mode=importlib``,
so test modules cannot import each other), then test modules import from
``_migration_support``.

Every helper reads ``get_settings().database_url`` at call time, so it follows
whichever database ``isolated_migration_db`` (or the session database) points
``DATABASE_URL`` at. Per-revision seed helpers stay in their own modules: each
seeds against the schema at its own revision.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import io
import os
import secrets
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest import mock

import asyncpg
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from curie_api.config import get_settings
from sqlalchemy import Row, text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"

# One engine per database URL. NullPool keeps no connection open between
# statements, so each ``asyncio.run`` loop opens its own and a database can be
# dropped (or cloned as a template) the moment a statement returns.
_ENGINES: dict[str, AsyncEngine] = {}


def alembic_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(ALEMBIC_DIR))
    return config


def _engine() -> AsyncEngine:
    url = get_settings().database_url
    engine = _ENGINES.get(url)
    if engine is None:
        engine = _ENGINES[url] = create_async_engine(url, poolclass=NullPool)
    return engine


def sql_rows(statement: str, params: dict[str, Any] | None = None) -> list[Row[Any]]:
    """Run one statement in its own transaction; rows come back tuple-like."""

    async def run() -> list[Row[Any]]:
        async with _engine().begin() as connection:
            result = await connection.execute(text(statement), params or {})
            return list(result.all()) if result.returns_rows else []

    return asyncio.run(run())


def sql_dicts(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Run one statement in its own transaction; rows come back as dicts."""
    return [dict(row._mapping) for row in sql_rows(statement, params)]


def constraint_exists(name: str) -> bool:
    """Look a ``curie`` constraint up BY NAME in the catalog.

    Deliberately not a shape check: a unique constraint restored under a
    generated name has the right shape and the wrong identity, which is the
    failure the API's 409 map trips over.
    """
    return bool(
        sql_rows(
            "SELECT 1 FROM pg_constraint c "
            "JOIN pg_class t ON t.oid = c.conrelid "
            "JOIN pg_namespace n ON n.oid = t.relnamespace "
            "WHERE n.nspname = 'curie' AND c.conname = :name",
            {"name": name},
        )
    )


def column_names(table: str) -> set[str]:
    rows = sql_rows(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = :table",
        {"table": table},
    )
    return {row[0] for row in rows}


def stamped_revision() -> str | None:
    """The single revision in ``curie.alembic_version``, or None when unstamped."""
    rows = sql_rows("SELECT version_num FROM curie.alembic_version")
    assert len(rows) <= 1, rows
    return str(rows[0][0]) if rows else None


class MigrationTemplates:
    """Session cache of databases migrated to one revision each.

    Replaying the alembic chain from 0001 for every migration test was most of
    their wall time. Instead each requested revision is built once, starting from
    the nearest already-built ancestor, and tests clone it with
    ``CREATE DATABASE ... TEMPLATE``. The clone is byte-for-byte what the replay
    produced: no revision sets database-level state a template would drop.
    """

    def __init__(self, base: URL) -> None:
        self._base = base
        self._script = ScriptDirectory.from_config(alembic_config())
        self._by_revision: dict[str, str] = {}
        self._created: list[str] = []
        # Revisions 0022 and 0024 read CURIE_* variables at upgrade time. A
        # template is shared by every later test, so it is built from the
        # environment the session started with, never the building test's.
        self._environ = dict(os.environ)

    def database_for(self, revision: str) -> str:
        script_rev = self._script.get_revision(revision)
        assert script_rev is not None, revision
        target = script_rev.revision
        if target in self._by_revision:
            return self._by_revision[target]
        ancestor = next(
            (
                self._by_revision[rev.revision]
                for rev in self._script.iterate_revisions(target, "base")
                if rev.revision in self._by_revision
            ),
            None,
        )
        name = f"curie_test_tpl_{target}_{secrets.token_hex(3)}".lower()
        template = f' TEMPLATE "{ancestor}"' if ancestor else ""
        asyncio.run(admin_execute(self._base, f'CREATE DATABASE "{name}"{template}'))
        self._created.append(name)
        environ = {**self._environ, "DATABASE_URL": render_url(self._base.set(database=name))}
        get_settings.cache_clear()
        try:
            with mock.patch.dict(os.environ, environ, clear=True):
                command.upgrade(alembic_config(), target)
        finally:
            get_settings.cache_clear()
        # A clone needs the template to have no other session; closing it to
        # connections keeps a stray one from failing every later clone.
        asyncio.run(
            admin_execute(
                self._base,
                f'ALTER DATABASE "{name}" WITH ALLOW_CONNECTIONS false',
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                f"WHERE datname = '{name}' AND pid <> pg_backend_pid()",
            )
        )
        self._by_revision[target] = name
        return name

    def drop_all(self) -> None:
        for name in self._created:
            asyncio.run(admin_execute(self._base, f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))


class IsolatedMigrationDb:
    """Handle ``isolated_migration_db`` yields: an empty database of the test's own."""

    def __init__(self, base: URL, name: str, templates: MigrationTemplates) -> None:
        self._base = base
        self._name = name
        self._templates = templates

    def at(self, revision: str) -> None:
        """Replace the database with a clone migrated exactly to ``revision``.

        Equivalent to ``command.upgrade(config, revision)`` on the empty
        database, without replaying the chain.
        """
        template = self._templates.database_for(revision)
        asyncio.run(
            admin_execute(
                self._base,
                f'DROP DATABASE IF EXISTS "{self._name}" WITH (FORCE)',
                f'CREATE DATABASE "{self._name}" TEMPLATE "{template}"',
            )
        )


async def admin_execute(base: URL, *statements: str) -> None:
    """Run statements against the `postgres` maintenance DB (CREATE/DROP DATABASE)."""
    conn = await asyncpg.connect(
        user=base.username,
        password=base.password,
        host=base.host,
        port=base.port,
        database="postgres",
    )
    try:
        for statement in statements:
            await conn.execute(statement)
    finally:
        await conn.close()


def render_url(url: URL) -> str:
    return url.render_as_string(hide_password=False)


def run_script(path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """Run a ``scripts/`` checker's ``main()`` in this process, as its CLI would.

    The migration gates are plain scripts (``check-alembic-revisions.py`` has a
    hyphen, so they are not importable by name). Spawning an interpreter per
    case cost a Python and alembic startup each; loading the file fresh per
    call keeps each case as isolated as the subprocess was. ``argparse`` exits
    are folded into the return code the way the shell would see them. Every
    gate keeps one real subprocess smoke test of its CLI.
    """
    module_name = f"_checker_{secrets.token_hex(4)}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None, path
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    stdout, stderr = io.StringIO(), io.StringIO()
    try:
        spec.loader.exec_module(module)
        with (
            mock.patch.object(sys, "argv", [str(path), *args]),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            try:
                returncode = module.main()
            except SystemExit as exit_:
                code = exit_.code
                returncode = 0 if code is None else code if isinstance(code, int) else 1
    finally:
        sys.modules.pop(module_name, None)
    return subprocess.CompletedProcess(
        [str(path), *args], returncode, stdout.getvalue(), stderr.getvalue()
    )
