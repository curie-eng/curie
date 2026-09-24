"""Migration 0037 preserves legacy shared state and makes its identity singular.

These tests intentionally exercise the migration against a disposable real
Postgres database.  The migration has two jobs that cannot be proved by model
fixtures: restore the runner-visible posture of unambiguously shared pre-0031
state, and ask Postgres to treat NULL as the one shared binding identity.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest
from alembic import command
from alembic.config import Config
from curie_api.config import get_settings
from curie_api.deps import get_session
from curie_api.main import create_app
from curie_api.models import WorkflowStateEntry
from curie_worker.binding import BindingResolver, ResolvedDeployment
from curie_worker.config import WorkerConfig
from fastapi.testclient import TestClient
from sqlalchemy import UniqueConstraint
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import text

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"
BELOW = "0036"
REVISION = "0037"
SCHEMA = "curie"
STATE_URL_ENV = "CURIE_STATE_URL"
KIND = "slack"
ADDRESS = "C0EXAMPLE1"
_UNSET = object()


def _alembic_config() -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(ALEMBIC_DIR))
    return cfg


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[Any]:
    async def go() -> list[Any]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                result = await conn.execute(text(statement), params or {})
                return list(result.all()) if result.returns_rows else []
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _stamped_revision() -> str:
    rows = _sql("SELECT version_num FROM curie.alembic_version")
    assert len(rows) == 1
    revision: str = rows[0][0]
    return revision


def _seed_agent(name: str, *, memory: bool | None = None) -> uuid.UUID:
    agent_id = uuid.uuid4()
    if memory is None:
        _sql(
            "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
            {"id": agent_id, "name": name},
        )
    else:
        _sql(
            "INSERT INTO curie.agents (id, name, memory) VALUES (:id, :name, :memory)",
            {"id": agent_id, "name": name, "memory": memory},
        )
    return agent_id


def _seed_binding(agent_id: uuid.UUID, *, address: str = ADDRESS) -> None:
    _sql(
        "INSERT INTO curie.agent_channels (id, agent_id, kind, address) "
        "VALUES (:id, :agent_id, :kind, :address)",
        {
            "id": uuid.uuid4(),
            "agent_id": agent_id,
            "kind": KIND,
            "address": address,
        },
    )


def _seed_active_deployment(agent_id: uuid.UUID) -> None:
    version_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.agent_versions "
        "(id, agent_id, version_label, bundle_ref, created_by) "
        "VALUES (:id, :agent_id, :label, :bundle_ref, :created_by)",
        {
            "id": version_id,
            "agent_id": agent_id,
            "label": "migration-fixture",
            "bundle_ref": "bundles/migration-fixture.zip",
            "created_by": "test",
        },
    )
    _sql(
        "INSERT INTO curie.deployments "
        "(id, agent_id, version_id, environment, status) "
        "VALUES (:id, :agent_id, :version_id, "
        "CAST('prod' AS curie.environment), 'active')",
        {
            "id": uuid.uuid4(),
            "agent_id": agent_id,
            "version_id": version_id,
        },
    )


def _seed_runnable_agent(
    name: str, *, memory: bool | None = None, address: str = ADDRESS
) -> uuid.UUID:
    agent_id = _seed_agent(name, memory=memory)
    _seed_binding(agent_id, address=address)
    _seed_active_deployment(agent_id)
    return agent_id


def _seed_state(
    agent_id: uuid.UUID,
    namespace: str,
    key: str,
    value: Any,
    *,
    binding_scope: str | None | object = _UNSET,
) -> uuid.UUID:
    entry_id = uuid.uuid4()
    params = {
        "id": entry_id,
        "agent_id": agent_id,
        "namespace": namespace,
        "key": key,
        "value": json.dumps(value),
    }
    if binding_scope is _UNSET:
        _sql(
            "INSERT INTO curie.workflow_state_entries "
            "(id, agent_id, namespace, key, value) "
            "VALUES (:id, :agent_id, :namespace, :key, CAST(:value AS jsonb))",
            params,
        )
    else:
        params["binding_scope"] = binding_scope
        _sql(
            "INSERT INTO curie.workflow_state_entries "
            "(id, agent_id, binding_scope, namespace, key, value) "
            "VALUES (:id, :agent_id, :binding_scope, :namespace, :key, "
            "CAST(:value AS jsonb))",
            params,
        )
    return entry_id


def _memory_by_name() -> dict[str, bool]:
    return {row[0]: row[1] for row in _sql("SELECT name, memory FROM curie.agents")}


def _state_rows(agent_id: uuid.UUID) -> list[tuple[Any, ...]]:
    return [
        tuple(row)
        for row in _sql(
            "SELECT id::text, binding_scope, namespace, key, value::text, version "
            "FROM curie.workflow_state_entries WHERE agent_id = :agent_id "
            "ORDER BY id",
            {"agent_id": agent_id},
        )
    ]


def _runner_state_url(*, kind: str, address: str) -> str:
    async def go() -> str:
        engine = create_async_engine(get_settings().database_url)
        config = WorkerConfig(
            database_url=get_settings().database_url,
            db_schema=SCHEMA,
            api_base_url="http://migration-api.test",
        )
        try:
            resolver = BindingResolver(engine, config)
            # Not `resolver.resolve()`: that is HEAD worker SQL, which reads
            # columns later revisions add (tenant_id, 0054), and head worker code
            # is not required to run below head. This test is at 0036/0037, so
            # the binding is read with SQL valid at that revision and the real
            # `boot_env` still derives the state URL from agent id + memory.
            async with engine.connect() as conn:
                row = (
                    (
                        await conn.execute(
                            text(
                                "SELECT a.id AS agent_id, a.name AS agent_name, "
                                "a.memory AS memory, d.id AS deployment_id, "
                                "v.id AS version_id, v.version_label AS version_label, "
                                "v.bundle_ref AS bundle_ref, "
                                "a.max_usd_per_day AS max_usd_per_day, "
                                "a.max_output_tokens_per_run AS max_output_tokens_per_run "
                                f"FROM {SCHEMA}.agents a "
                                f"JOIN {SCHEMA}.agent_channels c ON c.agent_id = a.id "
                                f"JOIN {SCHEMA}.deployments d "
                                "ON d.agent_id = a.id AND d.status = 'active' "
                                f"JOIN {SCHEMA}.agent_versions v "
                                "ON v.id = d.version_id AND v.agent_id = a.id "
                                "WHERE c.kind = :kind AND c.address = :address"
                            ),
                            {"kind": kind, "address": address},
                        )
                    )
                    .mappings()
                    .all()
                )
            assert len(row) == 1, row
            resolved = ResolvedDeployment.model_validate(dict(row[0]))
            env = resolver.boot_env(
                resolved,
                "migration-thread",
                kind=kind,
                address=address,
            )
            return env[STATE_URL_ENV]
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _api_get(url: str) -> Any:
    """Call the real state router without starting unrelated app resources."""

    engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async def isolated_session() -> AsyncIterator[AsyncSession]:
        async with sessions() as session:
            yield session

    app = create_app()
    app.dependency_overrides[get_session] = isolated_session
    client = TestClient(app)
    try:
        parsed = urlsplit(url)
        return client.get(parsed.path, headers={"X-API-Key": get_settings().api_key})
    finally:
        client.close()
        asyncio.run(engine.dispose())


def test_legacy_state_runner_url_and_value_survive_upgrade(
    isolated_migration_db: None,
) -> None:
    cfg = _alembic_config()
    command.upgrade(cfg, "0030")
    legacy_id = _seed_runnable_agent("legacy-state-owner")
    _seed_agent("fresh-no-state-agent")
    legacy_value = {"step": "approved", "attempts": [1, 2]}
    _seed_state(legacy_id, "workflow", "legacy-key", legacy_value)

    command.upgrade(cfg, BELOW)
    assert _stamped_revision() == BELOW
    assert _memory_by_name() == {
        "fresh-no-state-agent": False,
        "legacy-state-owner": False,
    }

    isolated_url = _runner_state_url(kind=KIND, address=ADDRESS)
    assert isolated_url.endswith(f"/agents/{legacy_id}/state/bindings/{KIND}/{ADDRESS}")
    hidden = _api_get(f"{isolated_url}/workflow/legacy-key")
    assert hidden.status_code == 404
    assert hidden.json() == {"detail": "state entry not found"}
    assert _sql(
        "SELECT binding_scope IS NULL FROM curie.workflow_state_entries "
        "WHERE agent_id = :agent_id AND namespace = 'workflow' AND key = 'legacy-key'",
        {"agent_id": legacy_id},
    ) == [(True,)]

    command.upgrade(cfg, REVISION)
    assert _stamped_revision() == REVISION
    assert _memory_by_name() == {
        "fresh-no-state-agent": False,
        "legacy-state-owner": True,
    }

    shared_url = _runner_state_url(kind=KIND, address=ADDRESS)
    assert shared_url.endswith(f"/agents/{legacy_id}/state")
    assert "/bindings/" not in shared_url
    visible = _api_get(f"{shared_url}/workflow/legacy-key")
    assert visible.status_code == 200, visible.text
    assert visible.json()["value"] == legacy_value


def test_legacy_unscoped_general_state_deliberately_flips_when_provenance_is_ambiguous(
    isolated_migration_db: None,
) -> None:
    cfg = _alembic_config()
    command.upgrade(cfg, BELOW)
    agent_id = _seed_agent("ambiguous-provenance-owner", memory=False)
    original_value = {"policy": "operator-or-legacy", "sequence": [3, 1, 4]}
    _seed_state(
        agent_id,
        "workflow",
        "ambiguous-key",
        original_value,
        binding_scope=None,
    )
    before = _state_rows(agent_id)

    command.upgrade(cfg, REVISION)

    assert _memory_by_name() == {"ambiguous-provenance-owner": True}
    assert _state_rows(agent_id) == before
    visible = _api_get(f"http://migration-api.test/agents/{agent_id}/state/workflow/ambiguous-key")
    assert visible.status_code == 200, visible.text
    assert visible.json()["value"] == original_value


def test_downgrade_keeps_repaired_memory_and_restores_null_distinct_identity(
    isolated_migration_db: None,
) -> None:
    cfg = _alembic_config()
    command.upgrade(cfg, "0030")
    agent_id = _seed_agent("downgrade-repaired-owner")
    original_value = {"survives": "forward-and-back"}
    _seed_state(
        agent_id,
        "workflow",
        "durable-key",
        original_value,
    )
    command.upgrade(cfg, BELOW)
    assert _memory_by_name() == {"downgrade-repaired-owner": False}
    before = _state_rows(agent_id)

    command.upgrade(cfg, REVISION)
    assert _memory_by_name() == {"downgrade-repaired-owner": True}

    command.downgrade(cfg, BELOW)

    assert _stamped_revision() == BELOW
    assert _memory_by_name() == {"downgrade-repaired-owner": True}
    assert _state_rows(agent_id) == before
    assert _sql(
        """
        SELECT backing_index.indnullsnotdistinct
        FROM pg_constraint AS catalog_constraint
        JOIN pg_class AS relation ON relation.oid = catalog_constraint.conrelid
        JOIN pg_index AS backing_index
          ON backing_index.indexrelid = catalog_constraint.conindid
        JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = 'curie'
          AND relation.relname = 'workflow_state_entries'
          AND catalog_constraint.conname = 'uq_state_agent_scope_ns_key'
        """
    ) == [(False,)]
    _seed_state(
        agent_id,
        "workflow",
        "durable-key",
        {"second-null-row": True},
        binding_scope=None,
    )
    assert _sql(
        "SELECT count(*) FROM curie.workflow_state_entries "
        "WHERE agent_id = :agent_id AND binding_scope IS NULL "
        "AND namespace = 'workflow' AND key = 'durable-key'",
        {"agent_id": agent_id},
    ) == [(2,)]


def test_reserved_only_legacy_state_does_not_flip_memory(
    isolated_migration_db: None,
) -> None:
    cfg = _alembic_config()
    command.upgrade(cfg, BELOW)
    agent_id = _seed_agent("reserved-only", memory=False)
    _seed_state(agent_id, "memory", "summary", {"text": "remembered"}, binding_scope=None)
    _seed_state(
        agent_id,
        "transcript",
        "thread-1",
        [{"role": "user", "content": "hello"}],
        binding_scope=None,
    )
    before = _state_rows(agent_id)

    command.upgrade(cfg, REVISION)

    assert _memory_by_name() == {"reserved-only": False}
    assert _state_rows(agent_id) == before


def test_reserved_shared_plus_scoped_general_state_stays_isolated(
    isolated_migration_db: None,
) -> None:
    cfg = _alembic_config()
    command.upgrade(cfg, BELOW)
    agent_id = _seed_agent("healthy-isolated", memory=False)
    _seed_binding(agent_id)
    reserved_value = {"summary": "agent-wide"}
    scoped_value = {"queue": ["ticket-1"]}
    _seed_state(agent_id, "memory", "summary", reserved_value, binding_scope=None)
    _seed_state(
        agent_id,
        "workflow",
        "queue",
        scoped_value,
        binding_scope=f"{KIND}:{ADDRESS}",
    )
    before = _state_rows(agent_id)

    command.upgrade(cfg, REVISION)

    assert _memory_by_name() == {"healthy-isolated": False}
    assert _state_rows(agent_id) == before
    reserved = _api_get(f"http://migration-api.test/agents/{agent_id}/state/memory/summary")
    scoped = _api_get(
        f"http://migration-api.test/agents/{agent_id}/state/bindings/"
        f"{KIND}/{ADDRESS}/workflow/queue"
    )
    assert reserved.status_code == 200, reserved.text
    assert reserved.json()["value"] == reserved_value
    assert scoped.status_code == 200, scoped.text
    assert scoped.json()["value"] == scoped_value


def test_memory_true_mixed_scope_general_state_remains_supported(
    isolated_migration_db: None,
) -> None:
    cfg = _alembic_config()
    command.upgrade(cfg, BELOW)
    agent_id = _seed_runnable_agent("already-shared", memory=True)
    shared_value = {"mode": "shared"}
    scoped_value = {"mode": "old-isolated"}
    _seed_state(
        agent_id,
        "workflow",
        "shared-key",
        shared_value,
        binding_scope=None,
    )
    _seed_state(
        agent_id,
        "workflow",
        "scoped-key",
        scoped_value,
        binding_scope=f"{KIND}:{ADDRESS}",
    )
    before = _state_rows(agent_id)

    command.upgrade(cfg, REVISION)

    assert _memory_by_name() == {"already-shared": True}
    assert _state_rows(agent_id) == before
    shared_url = _runner_state_url(kind=KIND, address=ADDRESS)
    assert shared_url.endswith(f"/agents/{agent_id}/state")
    shared = _api_get(f"{shared_url}/workflow/shared-key")
    scoped = _api_get(
        f"http://migration-api.test/agents/{agent_id}/state/bindings/"
        f"{KIND}/{ADDRESS}/workflow/scoped-key"
    )
    assert shared.status_code == 200, shared.text
    assert shared.json()["value"] == shared_value
    assert scoped.status_code == 200, scoped.text
    assert scoped.json()["value"] == scoped_value


@pytest.mark.parametrize("namespace", ["memory", "workflow"], ids=("reserved", "general"))
def test_duplicate_null_identity_is_refused_atomically(
    namespace: str,
    isolated_migration_db: None,
) -> None:
    cfg = _alembic_config()
    command.upgrade(cfg, BELOW)
    agent_id = _seed_agent(f"duplicate-{namespace}", memory=False)
    _seed_state(agent_id, namespace, "same-key", {"winner": 1}, binding_scope=None)
    _seed_state(agent_id, namespace, "same-key", {"winner": 2}, binding_scope=None)
    before_rows = _state_rows(agent_id)
    before_memory = _memory_by_name()

    with pytest.raises(RuntimeError) as caught:
        command.upgrade(cfg, REVISION)

    message = str(caught.value)
    lowered = message.lower()
    assert "duplicate" in lowered
    assert "null" in lowered
    assert str(agent_id) in message
    assert namespace in message
    assert "same-key" in message
    assert "merge or delete" in lowered
    assert _stamped_revision() == BELOW
    assert _memory_by_name() == before_memory
    assert _state_rows(agent_id) == before_rows


def test_false_mixed_scope_flip_candidate_is_refused_atomically(
    isolated_migration_db: None,
) -> None:
    cfg = _alembic_config()
    command.upgrade(cfg, BELOW)
    agent_id = _seed_agent("ambiguous-false-owner", memory=False)
    _seed_state(
        agent_id,
        "workflow",
        "shared-key",
        {"shape": "shared"},
        binding_scope=None,
    )
    _seed_state(
        agent_id,
        "workflow",
        "scoped-key",
        {"shape": "isolated"},
        binding_scope=f"{KIND}:{ADDRESS}",
    )
    before_rows = _state_rows(agent_id)
    before_memory = _memory_by_name()

    with pytest.raises(RuntimeError) as caught:
        command.upgrade(cfg, REVISION)

    message = str(caught.value)
    lowered = message.lower()
    assert str(agent_id) in message
    assert "memory=false" in lowered
    assert "shared" in lowered
    assert "isolated" in lowered
    assert "move" in lowered
    assert "merge" in lowered
    assert _stamped_revision() == BELOW
    assert _memory_by_name() == before_memory
    assert _state_rows(agent_id) == before_rows


def test_duplicate_null_constraint_catalog_and_scoped_controls(
    isolated_migration_db: None,
) -> None:
    cfg = _alembic_config()
    command.upgrade(cfg, BELOW)
    agent_id = _seed_agent("identity-controls", memory=True)
    command.upgrade(cfg, REVISION)

    assert _sql(
        """
        SELECT backing_index.indnullsnotdistinct
        FROM pg_constraint AS catalog_constraint
        JOIN pg_class AS relation ON relation.oid = catalog_constraint.conrelid
        JOIN pg_index AS backing_index
          ON backing_index.indexrelid = catalog_constraint.conindid
        JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = 'curie'
          AND relation.relname = 'workflow_state_entries'
          AND catalog_constraint.conname = 'uq_state_agent_scope_ns_key'
        """
    ) == [(True,)]

    _seed_state(agent_id, "workflow", "shared-key", {"writer": 1}, binding_scope=None)
    with pytest.raises(IntegrityError) as caught:
        _seed_state(agent_id, "workflow", "shared-key", {"writer": 2}, binding_scope=None)
    assert "uq_state_agent_scope_ns_key" in str(caught.value)
    assert _sql(
        "SELECT count(*) FROM curie.workflow_state_entries "
        "WHERE agent_id = :agent_id AND binding_scope IS NULL "
        "AND namespace = 'workflow' AND key = 'shared-key'",
        {"agent_id": agent_id},
    ) == [(1,)]

    for scope in ("slack:C0EXAMPLE2", "webhook:acme-room-3"):
        _seed_state(
            agent_id,
            "workflow",
            "isolated-key",
            {"scope": scope},
            binding_scope=scope,
        )
    assert _sql(
        "SELECT binding_scope FROM curie.workflow_state_entries "
        "WHERE agent_id = :agent_id AND namespace = 'workflow' "
        "AND key = 'isolated-key' ORDER BY binding_scope",
        {"agent_id": agent_id},
    ) == [("slack:C0EXAMPLE2",), ("webhook:acme-room-3",)]


def test_duplicate_null_orm_metadata_declares_shared_identity() -> None:
    constraint = next(
        candidate
        for candidate in WorkflowStateEntry.__table__.constraints
        if isinstance(candidate, UniqueConstraint)
        and candidate.name == "uq_state_agent_scope_ns_key"
    )

    assert constraint.dialect_options["postgresql"]["nulls_not_distinct"] is True
