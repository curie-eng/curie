"""Every worker SQL statement is planned against the actual Alembic head."""

from __future__ import annotations

import ast
import asyncio
import os
import subprocess
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from curie_internal.worker_sql import discover_statements, explain_statements
from sqlalchemy import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def migrated_database_url() -> Iterator[str]:
    base = make_url(
        os.environ.get(
            "TEST_DATABASE_URL",
            "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres",
        )
    )
    name = f"curie_worker_sql_{uuid.uuid4().hex}"
    url = base.set(database=name).render_as_string(hide_password=False)

    async def admin(statement: str) -> None:
        engine = create_async_engine(
            base.set(database="postgres"), isolation_level="AUTOCOMMIT", poolclass=NullPool
        )
        try:
            async with engine.connect() as connection:
                await connection.exec_driver_sql(statement)
        finally:
            await engine.dispose()

    asyncio.run(admin(f'CREATE DATABASE "{name}"'))
    try:
        subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            cwd=ROOT / "apps/api",
            env={**os.environ, "DATABASE_URL": url},
            check=True,
            capture_output=True,
            text=True,
        )
        yield url
    finally:
        asyncio.run(admin(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))


def _write_worker(root: Path, source: str) -> str:
    relative = "apps/worker/src/curie_worker/contract_probe.py"
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")
    return relative


def _explain(url: str, statements: list[tuple[str, str]]) -> None:
    async def run() -> None:
        engine = create_async_engine(url, poolclass=NullPool)
        try:
            async with engine.connect() as connection:
                await explain_statements(connection, statements)
        finally:
            await engine.dispose()

    asyncio.run(run())


def test_every_worker_text_statement_plans_against_migrations(
    migrated_database_url: str,
) -> None:
    statements = discover_statements(ROOT, "curie")
    expected: set[str] = set()
    for path in (ROOT / "apps/worker/src/curie_worker").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        aliases = {
            alias.asname or alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            and node.module in {"sqlalchemy", "sqlalchemy.sql"}
            for alias in node.names
            if alias.name == "text"
        }
        expected.update(
            f"{path.relative_to(ROOT).as_posix()}:{node.lineno}"
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in aliases
        )
    assert expected
    sites = {site for site, _ in statements}
    assert len(statements) >= len(expected)
    assert len(sites) == len(statements)
    assert all(
        any(site == coordinate or site.startswith(f"{coordinate}:") for site in sites)
        for coordinate in expected
    ), expected - sites
    _explain(migrated_database_url, statements)


def test_publication_result_sql_discovers_filtered_and_unfiltered_branches() -> None:
    statements = [
        sql
        for site, sql in discover_statements(ROOT, "curie")
        if "curie_worker/publication_store.py" in site
    ]
    select_variants = [
        sql for sql in statements if "p.result_reported_at IS NULL" in sql
    ]
    update_variants = [
        sql for sql in statements if "approval resolved before card delivery began" in sql
    ]
    assert any("AND p.id = :requested_id" in sql for sql in select_variants)
    assert any("AND p.id = :requested_id" not in sql for sql in select_variants)
    assert any("AND id = :requested_id" in sql for sql in update_variants)
    assert any("AND id = :requested_id" not in sql for sql in update_variants)


@pytest.mark.parametrize(
    ("import_source", "constructor"),
    [
        ("from sqlalchemy import text", "text"),
        ("from sqlalchemy import text as sql_text", "sql_text"),
        ("from sqlalchemy.sql import text", "text"),
        ("import sqlalchemy as sa", "sa.text"),
    ],
)
def test_sql_discovery_accepts_sqlalchemy_constructor_spellings(
    tmp_path: Path, import_source: str, constructor: str
) -> None:
    relative = _write_worker(
        tmp_path,
        f'{import_source}\nstatement = {constructor}("SELECT name FROM curie.agents")\n',
    )
    statements = discover_statements(tmp_path, "curie")
    assert len(statements) == 1
    assert relative in statements[0][0]
    assert statements[0][1] == "SELECT name FROM curie.agents"


def test_unresolved_sql_expression_is_rejected(tmp_path: Path) -> None:
    relative = _write_worker(
        tmp_path,
        "from sqlalchemy import text\n"
        "def statement(sql_from_somewhere):\n    return text(sql_from_somewhere)\n",
    )
    with pytest.raises(ValueError, match=relative):
        discover_statements(tmp_path, "curie")


@pytest.mark.parametrize(
    "binding",
    [
        "for sql in dynamic:",
        "async for sql in dynamic:",
        "with dynamic as sql:",
        "async with dynamic as sql:",
    ],
    ids=["for", "async_for", "with", "async_with"],
)
def test_dynamic_scope_binding_cannot_reuse_an_earlier_sql_string(
    tmp_path: Path, binding: str
) -> None:
    relative = _write_worker(
        tmp_path,
        "from sqlalchemy import text\n"
        "async def statement(dynamic):\n"
        '    sql = "SELECT name FROM curie.agents"\n'
        f"    {binding}\n"
        "        return text(sql)\n",
    )

    with pytest.raises(ValueError, match=relative):
        discover_statements(tmp_path, "curie")


@pytest.mark.parametrize(
    "binding",
    [
        "for item in dynamic:",
        "async for item in dynamic:",
        "with dynamic as item:",
        "async with dynamic as item:",
    ],
    ids=["for", "async_for", "with", "async_with"],
)
def test_unrelated_scope_binding_preserves_a_static_sql_string(
    tmp_path: Path, binding: str
) -> None:
    _write_worker(
        tmp_path,
        "from sqlalchemy import text\n"
        "async def statement(dynamic):\n"
        '    sql = "SELECT name FROM curie.agents"\n'
        f"    {binding}\n"
        "        return text(sql)\n",
    )

    statements = discover_statements(tmp_path, "curie")
    assert len(statements) == 1
    assert statements[0][1] == "SELECT name FROM curie.agents"


def test_new_worker_sql_using_a_missing_column_is_rejected(
    tmp_path: Path, migrated_database_url: str
) -> None:
    _write_worker(
        tmp_path,
        "from sqlalchemy import text\n"
        'statement = text("SELECT missing_contract_column FROM curie.agents")\n',
    )
    statements = discover_statements(tmp_path, "curie")
    with pytest.raises(DBAPIError, match="missing_contract_column"):
        _explain(migrated_database_url, statements)


def test_valid_parameterized_worker_sql_is_accepted(
    tmp_path: Path, migrated_database_url: str
) -> None:
    _write_worker(
        tmp_path,
        "from sqlalchemy import text\n"
        'statement = text("SELECT name FROM curie.agents WHERE id = CAST(:id AS uuid)")\n',
    )
    _explain(migrated_database_url, discover_statements(tmp_path, "curie"))
