"""Native PostgreSQL type identity, @spec PROTECTED-HOOK-SOURCE-2/10."""

from __future__ import annotations

import asyncio
import uuid

import pytest
from _migration_support import IsolatedMigrationDb, sql_dicts
from curie_api.schema_compat import assert_servable


def test_native_generation_type_cannot_be_impersonated_by_custom_named_enum(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    isolated_migration_db.at("head")
    agent = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.agents(id,name,hook_generation) VALUES (:id,'type-test',3)",
        {"id": agent},
    )
    asyncio.run(asyncio.wait_for(assert_servable(), 35))
    sql_dicts("CREATE TYPE curie.int8 AS ENUM ('1')")
    sql_dicts(
        "ALTER TABLE curie.hook_source_operations "
        "DROP CONSTRAINT hook_source_operations_generation_ck"
    )
    sql_dicts(
        "ALTER TABLE curie.hook_source_operations ALTER COLUMN generation "
        "TYPE curie.int8 USING generation::text::curie.int8"
    )
    actual = sql_dicts(
        "SELECT t.typname,n.nspname,t.typtype::text AS typtype, "
        "a.atttypid='pg_catalog.int8'::regtype::oid AS native "
        "FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid "
        "JOIN pg_namespace n ON n.oid=t.typnamespace "
        "WHERE a.attrelid='curie.hook_source_operations'::regclass "
        "AND a.attname='generation' AND NOT a.attisdropped"
    )
    assert actual == [{"typname": "int8", "nspname": "curie", "typtype": "e", "native": False}]
    before = sql_dicts("SELECT version_num FROM curie.alembic_version")
    with pytest.raises(RuntimeError, match="schema_structure_unavailable"):
        asyncio.run(asyncio.wait_for(assert_servable(), 35))
    assert sql_dicts("SELECT version_num FROM curie.alembic_version") == before
    assert sql_dicts("SELECT hook_generation FROM curie.agents WHERE id=:id", {"id": agent}) == [
        {"hook_generation": 3}
    ]
    assert sql_dicts("SELECT count(*) AS rows FROM curie.hook_source_operations") == [{"rows": 0}]
