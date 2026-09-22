"""Migration 0052: OIDC login schema (#2908).

Upgrade adds ``curie.oidc_login_attempts``, ``console_sessions.principal_id``
and ``principals.idp_issuer``, and replaces the principals unique key
``(tenant_id, idp_subject)`` with ``principals_tenant_issuer_subject_key`` on
``(tenant_id, idp_issuer, idp_subject)``. Downgrade to 0051 reverses every piece
and restores the old key. Runs on a private database (``isolated_migration_db``).
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"
NEW_KEY = "principals_tenant_issuer_subject_key"
OLD_KEY = "principals_tenant_idp_subject_key"
ATTEMPT_COLUMNS = {
    "id",
    "state_hash",
    "nonce",
    "code_verifier",
    "expires_at",
    "consumed_at",
    "created_at",
}


def _config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(ALEMBIC_DIR))
    return config


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                result = await connection.execute(text(statement), params or {})
                if not result.returns_rows:
                    return []
                return [dict(row) for row in result.mappings().all()]
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _columns(table: str) -> dict[str, dict[str, Any]]:
    rows = _sql(
        "SELECT column_name, is_nullable, column_default FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = :table",
        {"table": table},
    )
    return {row["column_name"]: row for row in rows}


def _unique_constraints(table: str) -> dict[str, list[str]]:
    """Unique constraint name -> its columns, in key order."""

    rows = _sql(
        "SELECT c.conname, a.attname, k.ord FROM pg_constraint c "
        "JOIN pg_class t ON t.oid = c.conrelid "
        "JOIN pg_namespace n ON n.oid = t.relnamespace "
        "CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord) "
        "JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum "
        "WHERE n.nspname = 'curie' AND t.relname = :table AND c.contype = 'u' "
        "ORDER BY c.conname, k.ord",
        {"table": table},
    )
    out: dict[str, list[str]] = {}
    for row in rows:
        out.setdefault(row["conname"], []).append(row["attname"])
    return out


def _regclass(name: str) -> str | None:
    rows = _sql("SELECT to_regclass(:name)::text AS name", {"name": f"curie.{name}"})
    value = rows[0]["name"]
    return None if value is None else str(value)


def _principal_id_fk_targets() -> list[str]:
    rows = _sql(
        "SELECT confrelid::regclass::text AS target FROM pg_constraint c "
        "JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey) "
        "WHERE c.conrelid = 'curie.console_sessions'::regclass AND c.contype = 'f' "
        "AND a.attname = 'principal_id'"
    )
    return [row["target"] for row in rows]


def _principal_id_indexed() -> bool:
    rows = _sql(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = 'curie' "
        "AND tablename = 'console_sessions'"
    )
    return any("(principal_id)" in row["indexdef"] for row in rows)


def _state_hash_unique() -> bool:
    rows = _sql(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = 'curie' "
        "AND tablename = 'oidc_login_attempts'"
    )
    return any(
        "UNIQUE" in row["indexdef"] and "(state_hash)" in row["indexdef"] for row in rows
    )


def _assert_at_0052() -> None:
    assert _regclass("oidc_login_attempts") is not None
    attempts = _columns("oidc_login_attempts")
    assert set(attempts) == ATTEMPT_COLUMNS
    for name in ("id", "state_hash", "nonce", "code_verifier", "expires_at", "created_at"):
        assert attempts[name]["is_nullable"] == "NO", name
    assert attempts["consumed_at"]["is_nullable"] == "YES"
    assert _state_hash_unique()

    sessions = _columns("console_sessions")
    assert sessions["principal_id"]["is_nullable"] == "YES"
    # The additive login-code subject column is untouched.
    assert sessions["subject"]["is_nullable"] == "YES"
    assert _principal_id_fk_targets() == ["curie.principals"]
    assert _principal_id_indexed()

    principals = _columns("principals")
    assert principals["idp_issuer"]["is_nullable"] == "NO"
    assert principals["idp_issuer"]["column_default"] is not None

    uniques = _unique_constraints("principals")
    assert uniques.get(NEW_KEY) == ["tenant_id", "idp_issuer", "idp_subject"]
    assert OLD_KEY not in uniques


def _assert_at_0051() -> None:
    assert _regclass("oidc_login_attempts") is None
    assert "principal_id" not in _columns("console_sessions")
    assert "idp_issuer" not in _columns("principals")
    uniques = _unique_constraints("principals")
    assert uniques.get(OLD_KEY) == ["tenant_id", "idp_subject"]
    assert NEW_KEY not in uniques
    # The tables 0051 and earlier own survive.
    assert _regclass("principals") is not None
    assert _regclass("console_sessions") is not None


def test_0052_revision_follows_0051() -> None:
    script = ScriptDirectory.from_config(_config())
    revision = script.get_revision("0052")
    assert revision is not None
    assert revision.down_revision == "0051"


def test_0052_round_trip(isolated_migration_db: None) -> None:
    config = _config()
    command.upgrade(config, "head")
    _assert_at_0052()
    try:
        command.downgrade(config, "0051")
        _assert_at_0051()
    finally:
        # A failed assertion must not leave this private database below head.
        command.upgrade(config, "head")
    _assert_at_0052()


def test_0052_backfills_existing_principals_with_blank_issuer(
    isolated_migration_db: None,
) -> None:
    """A principal row that predates 0052 survives the upgrade with idp_issuer ''."""

    config = _config()
    command.upgrade(config, "0051")
    try:
        _sql(
            "INSERT INTO curie.principals (id, tenant_id, idp_subject, type) VALUES "
            "(gen_random_uuid(), '00000000-0000-0000-0000-000000000001', "
            "'pre-0052-sub', 'human')"
        )
        command.upgrade(config, "head")
        rows = _sql(
            "SELECT idp_issuer FROM curie.principals WHERE idp_subject = 'pre-0052-sub'"
        )
        assert rows == [{"idp_issuer": ""}]
    finally:
        command.upgrade(config, "head")


EXPIRES_AT_INDEX = "ix_oidc_login_attempts_expires_at"


def test_head_indexes_login_attempt_expiry(isolated_migration_db: None) -> None:
    """The unauthenticated login start prunes and counts by ``expires_at``.

    Without an index both are a sequential scan of every live attempt, so an
    anonymous flood of ``/console/oidc/login`` makes each request cost more.
    """

    command.upgrade(_config(), "head")
    rows = _sql(
        "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'curie' "
        "AND tablename = 'oidc_login_attempts'"
    )
    by_name = {row["indexname"]: row["indexdef"] for row in rows}
    assert EXPIRES_AT_INDEX in by_name, by_name
    assert by_name[EXPIRES_AT_INDEX].rstrip().endswith("(expires_at)"), by_name
