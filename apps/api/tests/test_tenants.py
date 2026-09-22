"""Tenant entity: first no-behavior-change slice (#2906).

A new ``tenants`` table plus a migration data step that auto-provisions exactly
one default tenant at a FIXED, well-known id
(``00000000-0000-0000-0000-000000000001``) so a later migration can reference
it without a runtime lookup. ``deployment_id`` is an opaque identifier for the
physical self-host appliance -- NOT a foreign key to the existing
``Deployment``/``deployments`` table, which is the unrelated dev/prod binding
of an AgentVersion.
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

DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000001"


def test_tenant_model_shape() -> None:
    from curie_api.models import Tenant

    assert Tenant.__tablename__ == "tenants"

    columns = Tenant.__table__.c

    id_col = columns["id"]
    assert id_col.primary_key is True
    assert id_col.nullable is False
    # SQLAlchemy 2.0.52 wraps a zero-arg callable default in a fresh lambda via
    # functools.update_wrapper, so identity comparison against the bare
    # callable (``id_col.default.arg is uuid.uuid4``) always fails -- even for
    # existing columns in this codebase (e.g. Agent.id). Unwrap it first.
    assert inspect.unwrap(id_col.default.arg) is uuid.uuid4

    deployment_id_col = columns["deployment_id"]
    assert isinstance(deployment_id_col.type.python_type, type)
    assert deployment_id_col.type.python_type is str
    assert deployment_id_col.nullable is False

    idp_config_ref_col = columns["idp_config_ref"]
    assert idp_config_ref_col.type.python_type is str
    assert idp_config_ref_col.nullable is True

    retention_policy_ref_col = columns["retention_policy_ref"]
    assert retention_policy_ref_col.type.python_type is str
    assert retention_policy_ref_col.nullable is True

    default_provider_policy_ref_col = columns["default_provider_policy_ref"]
    assert default_provider_policy_ref_col.type.python_type is str
    assert default_provider_policy_ref_col.nullable is True

    status_col = columns["status"]
    assert status_col.type.python_type is str
    assert status_col.nullable is False
    assert status_col.default is not None
    assert status_col.default.arg == "active"

    created_at_col = columns["created_at"]
    assert created_at_col.type.python_type is datetime
    assert created_at_col.nullable is False


def _select_tenants() -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    text(
                        "SELECT id, deployment_id, status FROM curie.tenants"
                    )
                )
                return [dict(row) for row in result.mappings().all()]
        finally:
            await engine.dispose()

    return asyncio.run(run())


def test_migration_auto_provisions_exactly_one_default_tenant(
    migrated: None,
) -> None:
    rows = _select_tenants()

    assert len(rows) == 1
    (row,) = rows
    assert str(row["id"]) == DEFAULT_TENANT_ID
    assert row["status"] == "active"
    assert row["deployment_id"] is not None
    assert row["deployment_id"] != ""
