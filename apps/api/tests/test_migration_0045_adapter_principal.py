"""Migration 0045 admits the adapter principal kind and adds principal_subject."""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"


def _alembic_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(ALEMBIC_DIR))
    return config


def _execute(sql: str, params: dict[str, Any] | None = None) -> None:
    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(text(sql), params or {})
        finally:
            await engine.dispose()

    asyncio.run(run())


def _rows(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as connection:
                result = await connection.execute(text(sql), params or {})
                return [dict(row) for row in result.mappings().all()]
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _has_subject_column() -> bool:
    return bool(
        _rows(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'curie' AND table_name = 'approval_audit_entries'
              AND column_name = 'principal_subject'
            """
        )
    )


def _insert_audit(approval_id: uuid.UUID, kind: str, subject: str | None) -> uuid.UUID:
    audit_id = uuid.uuid4()
    columns = "principal_kind, authenticated"
    values = ":kind, true"
    params: dict[str, Any] = {"id": audit_id, "approval_id": approval_id, "kind": kind}
    if subject is not None:
        columns += ", principal_subject"
        values += ", :subject"
        params["subject"] = subject
    _execute(
        f"""
        INSERT INTO curie.approval_audit_entries
          (id, approval_id, action, actor, decision, authorizer, authorized, {columns})
        VALUES
          (:id, :approval_id, 'resolved', 'U0EXAMPLE1', 'approved',
           'ExplicitUserListAuthorizer', true, {values})
        """,
        params,
    )
    return audit_id


def test_0045_admits_adapter_kind_and_round_trips(isolated_migration_db: None) -> None:
    config = _alembic_config()
    command.upgrade(config, "0044")

    approval_id = uuid.uuid4()
    _execute(
        """
        INSERT INTO curie.approvals
          (id, conversation_id, author, summary, reply_kind, reply_channel,
           reply_placeholder, dedupe_key)
        VALUES
          (:id, 'th-migration-0045', 'U0EXAMPLE1', 'adapter migration', 'slack',
           'C0EXAMPLE1', 'p-0045', :dedupe)
        """,
        {"id": approval_id, "dedupe": f"migration-0045-{uuid.uuid4()}"},
    )
    assert not _has_subject_column()
    with pytest.raises(IntegrityError):
        _insert_audit(approval_id, "adapter", None)

    command.upgrade(config, "head")

    assert _has_subject_column()
    operator_id = _insert_audit(approval_id, "operator", None)
    adapter_id = _insert_audit(approval_id, "adapter", "mail-adapter")
    assert _rows(
        """
        SELECT id, principal_kind, principal_subject
        FROM curie.approval_audit_entries WHERE id IN (:a, :b) ORDER BY principal_kind
        """,
        {"a": adapter_id, "b": operator_id},
    ) == [
        {"id": adapter_id, "principal_kind": "adapter", "principal_subject": "mail-adapter"},
        {"id": operator_id, "principal_kind": "operator", "principal_subject": None},
    ]
    with pytest.raises(IntegrityError):
        _insert_audit(approval_id, "asserted", None)

    command.downgrade(config, "0044")
    assert not _has_subject_column()
    # The downgrade clears the adapter kind rather than violating the restored check.
    assert _rows(
        "SELECT principal_kind FROM curie.approval_audit_entries WHERE id = :id",
        {"id": adapter_id},
    ) == [{"principal_kind": None}]
    with pytest.raises(IntegrityError):
        _insert_audit(approval_id, "adapter", None)

    command.upgrade(config, "head")
    assert _has_subject_column()
    _insert_audit(approval_id, "adapter", "mail-adapter")
