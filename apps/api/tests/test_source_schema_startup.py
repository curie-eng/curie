"""Actual API serving prerequisite, @spec PROTECTED-HOOK-SOURCE-2/10."""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import json
import os
import subprocess
import sys
import uuid
from typing import Any

import pytest
from _migration_support import IsolatedMigrationDb, sql_dicts
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.schema_compat import assert_servable
from fastapi.testclient import TestClient
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.fixture
def startup_db(isolated_migration_db: IsolatedMigrationDb) -> str:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    isolated_migration_db.at("head")
    agent = str(uuid.uuid4())
    sql_dicts(
        "INSERT INTO curie.agents(id,name,hook_generation) VALUES (:id,'startup-test',3)",
        {"id": uuid.UUID(agent)},
    )
    return agent


def database_state(agent: str) -> Any:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    return (
        sql_dicts("SELECT * FROM curie.alembic_version ORDER BY version_num"),
        sql_dicts(
            "SELECT id,name,hook_generation FROM curie.agents WHERE id=:id",
            {"id": uuid.UUID(agent)},
        ),
        sql_dicts("SELECT * FROM curie.hook_source_policies"),
    )


def invoke() -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    asyncio.run(asyncio.wait_for(assert_servable(), 35))


@pytest.mark.parametrize("revision", ["0101", "future-expand"])
def test_actual_api_admits_known_or_future_with_readable_source_structure(
    startup_db: str, revision: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    sql_dicts("UPDATE curie.alembic_version SET version_num=:revision", {"revision": revision})
    before = database_state(startup_db)
    invoke()
    assert database_state(startup_db) == before


def test_known_premimum_api_refusal_never_migrates(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    isolated_migration_db.at("0075")
    with pytest.raises(RuntimeError, match="below application min"):
        invoke()
    assert sql_dicts("SELECT version_num FROM curie.alembic_version") == [{"version_num": "0075"}]
    assert sql_dicts("SELECT to_regclass('curie.hook_source_operations')::text AS ledger") == [
        {"ledger": None}
    ]


def test_schema_built_at_0100_is_refused_without_migrating(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """#2911: the Agent ORM and resolver read 0101's columns.

    The admit test above builds head and restamps it, so it cannot notice a
    min that is too low. This one builds a real 0100 schema, the revision right below the min.
    """
    isolated_migration_db.at("0100")
    with pytest.raises(RuntimeError, match="below application min"):
        invoke()
    assert sql_dicts("SELECT version_num FROM curie.alembic_version") == [{"version_num": "0100"}]


@pytest.mark.parametrize("revision", ["0101", "future-expand"])
def test_compatible_stamp_without_actual_ledger_refuses(
    startup_db: str, revision: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    sql_dicts("UPDATE curie.alembic_version SET version_num=:r", {"r": revision})
    sql_dicts("DROP TABLE curie.hook_source_operations")
    before = database_state(startup_db)
    for name, value in {
        "GITHUB_REVIEW_INGRESS_ENABLED": "false",
        "RESUME_RECONCILER_ENABLED": "false",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        "COMMIT_POLL_INTERVAL_S": "0",
        "OTEL_SDK_DISABLED": "true",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "",
    }.items():
        monkeypatch.setenv(name, value)
    get_settings.cache_clear()
    try:
        with pytest.raises(RuntimeError, match="schema_structure_unavailable"):
            with TestClient(create_app()):
                pass
        assert database_state(startup_db) == before
    finally:
        get_settings.cache_clear()


@pytest.mark.parametrize(
    "table,column",
    [("hook_source_policies", "qualification_id"), ("hook_source_operations", "intent_sha256")],
)
def test_required_projection_column_missing_refuses(
    startup_db: str, table: str, column: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    sql_dicts(f"ALTER TABLE curie.{table} RENAME COLUMN {column} TO renamed_required_column")
    with pytest.raises(RuntimeError, match="schema_structure_unavailable"):
        invoke()
    assert sql_dicts("SELECT version_num FROM curie.alembic_version") == [{"version_num": "0102"}]


@pytest.mark.parametrize(
    "table,column,kind",
    [
        ("hook_source_operations", "generation", "numeric"),
        ("hook_source_operations", "attempted_at", "timestamp without time zone"),
        ("agents", "hook_generation", "bigint"),
    ],
)
def test_incompatible_actual_column_type_refuses(
    startup_db: str, table: str, column: str, kind: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    sql_dicts(f"ALTER TABLE curie.{table} ALTER COLUMN {column} TYPE {kind}")
    with pytest.raises(RuntimeError, match="schema_structure_unavailable"):
        invoke()
    assert sql_dicts("SELECT version_num FROM curie.alembic_version") == [{"version_num": "0102"}]


@pytest.mark.parametrize("mode", ["multiple", "empty", "missing"])
def test_actual_version_table_must_have_exactly_one_revision(startup_db: str, mode: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    if mode == "multiple":
        sql_dicts("INSERT INTO curie.alembic_version(version_num) VALUES ('future-expand')")
    elif mode == "empty":
        sql_dicts("DELETE FROM curie.alembic_version")
    else:
        sql_dicts("DROP TABLE curie.alembic_version")
    with pytest.raises(RuntimeError, match="schema_revision_unavailable"):
        invoke()
    assert sql_dicts(
        "SELECT hook_generation FROM curie.agents WHERE id=:id", {"id": uuid.UUID(startup_db)}
    ) == [{"hook_generation": 3}]


def subprocess_probe(*, url: str, schema: str) -> dict[str, Any]:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    program = '''
import asyncio,json
from curie_api.schema_compat import assert_servable
async def probe():
    """@spec PROTECTED-HOOK-SOURCE-2."""
    try:
        await assert_servable()
    except BaseException as error:
        print(json.dumps({'ok':False,'code':getattr(error,'code',None),
                          'safe':str(error),'type':type(error).__name__}))
    else:
        print(json.dumps({'ok':True}))
asyncio.run(probe())
'''
    environment = dict(os.environ, DATABASE_URL=url, DB_SCHEMA=schema, OTEL_SDK_DISABLED="true")
    result = subprocess.run(
        [sys.executable, "-c", program], env=environment, capture_output=True, text=True, timeout=40
    )
    assert result.returncode == 0, "startup probe subprocess failed"
    return json.loads(result.stdout)


def test_configured_metadata_schema_is_distinct_from_actual_curie_source_schema(
    startup_db: str,
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    schema = "metadata_" + uuid.uuid4().hex
    sql_dicts(f"CREATE SCHEMA {schema}")
    sql_dicts(f"CREATE TABLE {schema}.alembic_version(version_num varchar(32) PRIMARY KEY)")
    sql_dicts(f"INSERT INTO {schema}.alembic_version VALUES ('0101')")
    url = get_settings().database_url
    assert subprocess_probe(url=url, schema=schema) == {"ok": True}
    sql_dicts("DROP TABLE curie.hook_source_operations")
    result = subprocess_probe(url=url, schema=schema)
    assert result["ok"] is False and result["code"] == "schema_structure_unavailable"


def test_actual_nonexistent_owned_database_refuses_without_driver_or_dsn_detail(
    startup_db: str,
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    name = "missing_startup_" + uuid.uuid4().hex
    assert not sql_dicts("SELECT datname FROM pg_database WHERE datname=:name", {"name": name})
    url = (
        make_url(get_settings().database_url)
        .set(database=name)
        .render_as_string(hide_password=False)
    )
    result = subprocess_probe(url=url, schema="curie")
    assert result["ok"] is False and result["code"] == "schema_probe_unavailable"
    assert name not in result["safe"] and "postgresql" not in result["safe"].lower()
    assert "password" not in result["safe"].lower()


def test_invalid_metadata_identifier_is_safe_before_sql(startup_db: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    assert importlib.util.find_spec("curie_protected_hooks.schema_serving") is not None, (
        "SOURCE-2 shared startup probe is missing"
    )
    module = importlib.import_module("curie_protected_hooks.schema_serving")
    assert callable(getattr(module, "assert_servable", None)), (
        "SOURCE-2 shared live startup probe is missing"
    )

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        engine = create_async_engine(get_settings().database_url, pool_size=1, max_overflow=0)
        try:
            with pytest.raises(module.SchemaServingUnavailable) as caught:
                await module.assert_servable(engine, metadata_schema="curie; untrusted-input")
            assert "untrusted-input" not in str(caught.value)
            assert engine.pool.checkedout() == 0
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_actual_identity_without_source_select_permission_refuses(startup_db: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    role = "startup_reader_" + uuid.uuid4().hex
    password = uuid.uuid4().hex
    sql_dicts(f"CREATE ROLE {role} LOGIN PASSWORD '{password}'")
    try:
        sql_dicts(f"GRANT USAGE ON SCHEMA curie TO {role}")
        sql_dicts(
            "GRANT SELECT ON curie.alembic_version,curie.agents,curie.hook_source_policies "
            f"TO {role}"
        )
        url = (
            make_url(get_settings().database_url)
            .set(username=role, password=password)
            .render_as_string(hide_password=False)
        )
        result = subprocess_probe(url=url, schema="curie")
        assert result["ok"] is False and result["code"] == "schema_structure_unavailable"
        assert password not in result["safe"] and role not in result["safe"]
    finally:
        sql_dicts(f"DROP OWNED BY {role}")
        sql_dicts(f"DROP ROLE {role}")
