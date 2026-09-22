"""Tenant entity: issue #2906's first, no-behavior-change slice.

A ``tenants`` table (and its self-host default row) is the platform's first
tenant concept -- no ``tenant_id`` column exists anywhere else in the schema
yet. ``Tenant.deployment_id`` is an opaque string identifier for the physical
self-host appliance; it is deliberately NOT a foreign key to the existing
``deployments`` table. ``Deployment`` is an unrelated concept -- the
dev/prod binding of an ``AgentVersion`` -- that predates tenants entirely.

These two tests pin the model shape (importable without a database) and the
migration's data step (the auto-provisioned default tenant, against the
disposable per-run DB from conftest's ``migrated`` fixture).
"""

from __future__ import annotations

import asyncio
import inspect
import uuid
from datetime import datetime
from typing import Any

from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


def test_tenant_model_shape() -> None:
    """`Tenant` exists with the columns/types the ADR-2906 slice specifies."""

    from curie_api.models import Tenant

    assert Tenant.__tablename__ == "tenants"

    columns = Tenant.__table__.columns
    expected = {
        "id",
        "deployment_id",
        "idp_config_ref",
        "retention_policy_ref",
        "default_provider_policy_ref",
        "status",
        "created_at",
    }
    assert expected <= set(columns.keys())

    id_col = columns["id"]
    assert id_col.primary_key is True
    assert id_col.type.python_type is uuid.UUID
    assert id_col.default is not None
    # SQLAlchemy wraps the zero-arg callable in a fresh `lambda ctx: fn()` via
    # `functools.update_wrapper`, so `.default.arg` is never `uuid.uuid4` by
    # identity -- unwrap it first.
    assert inspect.unwrap(id_col.default.arg) is uuid.uuid4

    # Opaque physical-appliance identifier -- see module docstring for why this
    # is a plain string, not a FK to `deployments`.
    deployment_id_col = columns["deployment_id"]
    assert deployment_id_col.type.python_type is str
    assert deployment_id_col.nullable is False
    assert not deployment_id_col.foreign_keys

    for nullable_col in (
        "idp_config_ref",
        "retention_policy_ref",
        "default_provider_policy_ref",
    ):
        col = columns[nullable_col]
        assert col.type.python_type is str
        assert col.nullable is True

    status_col = columns["status"]
    assert status_col.type.python_type is str
    assert status_col.nullable is False

    created_at_col = columns["created_at"]
    assert created_at_col.type.python_type is datetime
    assert created_at_col.nullable is False
    assert created_at_col.server_default is not None


def _fetch_tenants() -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    text("SELECT status, deployment_id FROM curie.tenants")
                )
                return [dict(row) for row in result.mappings().all()]
        finally:
            await engine.dispose()

    return asyncio.run(run())


def test_migration_auto_provisions_one_default_tenant(migrated: None) -> None:
    """The self-host case: migrating to head provisions exactly one tenant.

    ``deployment_id`` here names the physical self-host appliance, not a row
    in `deployments` (that table binds an AgentVersion to dev/prod and is an
    unrelated concept -- see module docstring).
    """

    rows = _fetch_tenants()

    assert len(rows) == 1
    (row,) = rows
    assert row["status"] == "active"
    assert row["deployment_id"] is not None
