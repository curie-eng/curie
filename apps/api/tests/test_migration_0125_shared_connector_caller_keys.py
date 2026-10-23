"""Migration 0125 adds the shared proxy's caller verify key tables (ADR 0239, cp5).

Expand only, on 0124. The upgrade creates ``curie.shared_connector_caller_keys``
(installation, public key, ``seen_at``; primary key installation and key) and
``curie.shared_connector_caller_key_heartbeats`` (installation, ``seen_at``),
each with row level security enabled and one SELECT policy scoped to the
installation of the reading role's application. There is no write policy: the
API publishes as the table owner. A ``DO`` block grants SELECT on both tables to
every bundled application role that already exists, so a proxy provisioned
before the upgrade can read its keys. The downgrade drops both tables, which
drops those grants too, so 0123's downgrade can still drop the role.

Every database here is the test's own (``isolated_migration_db``); every role
is registered with ``SharedCleanup`` before it is created.
"""

from __future__ import annotations

import base64
import json
import secrets
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import nacl.signing
import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    render_url,
    sql_dicts,
    stamped_revision,
)
from _shared_connector_support import (
    CALLER_KEY_TABLES,
    SERVER_IMAGE,
    SLOT,
    PgSession,
    Recipient,
    SessionPool,
    SharedCleanup,
    application_spec,
    connect_as,
    expect_sqlstate,
    owner_connect_kwargs,
    quote_ident,
    random_installation,
    regclass,
    role_exists,
    shared_policy,
)
from alembic import command
from alembic.script import ScriptDirectory
from curie_api.config import get_settings
from sqlalchemy.engine import make_url

REVISION_KINDS = Path(__file__).resolve().parents[1] / "src/curie_api/revision_kinds.json"
KEYS = "shared_connector_caller_keys"
HEARTBEATS = "shared_connector_caller_key_heartbeats"
# SQLSTATEs: insufficient_privilege and check_violation.
DENIED = "42501"
CHECK = "23514"


def _verify_key() -> str:
    """A fresh Ed25519 public key in the standard base64 the API publishes."""

    return base64.b64encode(bytes(nacl.signing.SigningKey.generate().verify_key)).decode()


@pytest.fixture
def cleanup(isolated_migration_db: IsolatedMigrationDb) -> Iterator[SharedCleanup]:
    # Runs before the isolated database is dropped, so the role's grants go first.
    tracker = SharedCleanup()
    yield tracker
    tracker.run()


@pytest.fixture
def sessions(cleanup: SharedCleanup) -> Iterator[SessionPool]:
    pool = SessionPool()
    yield pool
    pool.close_all()


def _seed_bundled_role(cleanup: SharedCleanup, installation: str) -> tuple[str, str]:
    """A bundled application role at 0124, granted as the 0123 API grants it.

    Answers the role and its DSN. The role maps to its application through the
    ``shared_connector_database_roles`` row, which is what every 0123 policy and
    the new 0125 policies read.
    """

    role = cleanup.role(f"curie_sca_{secrets.token_hex(20)}")
    password = secrets.token_urlsafe(24)
    owner = PgSession(**owner_connect_kwargs())
    try:
        owner.execute(f"CREATE ROLE {quote_ident(role)} LOGIN PASSWORD '{password}'")
        owner.execute(f"GRANT USAGE ON SCHEMA curie TO {quote_ident(role)}")
        owner.execute(
            "GRANT SELECT ON TABLE curie.shared_connector_applications, "
            "curie.shared_connector_database_roles TO " + quote_ident(role)
        )
        application = uuid.uuid4()
        owner.execute(
            "INSERT INTO curie.shared_connector_applications (id, installation_id, spec, "
            "server_image, credential_slots, provider_origins, snapshot_seal, policy, "
            "pin_digest, custody) VALUES ($1, $2, $3::jsonb, $4, $5::jsonb, $6::jsonb, NULL, "
            "$7::jsonb, $8, 'platform')",
            application,
            installation,
            json.dumps(application_spec(installation, str(application))),
            SERVER_IMAGE,
            json.dumps([SLOT]),
            json.dumps(["https://api.example.com"]),
            json.dumps(shared_policy()),
            "0" * 64,
        )
        recipient = Recipient.generate()
        owner.execute(
            "INSERT INTO curie.shared_connector_recipients "
            "(application_id, recipient_id, algorithm, public_key) "
            "VALUES ($1, $2, 'x25519-sealed-box', $3)",
            application,
            recipient.recipient_id,
            recipient.public_bytes,
        )
        owner.execute(
            "INSERT INTO curie.shared_connector_database_roles (application_id, role_name, "
            "mode, status, sealed_dsn, dsn_recipient_id) "
            "VALUES ($1, $2, 'bundled', 'provisioned', $3, $4)",
            application,
            role,
            recipient.seal(b"postgresql://sealed"),
            recipient.recipient_id,
        )
    finally:
        owner.close()
    dsn = render_url(
        make_url(get_settings().database_url).set(
            drivername="postgresql", username=role, password=password
        )
    )
    return role, dsn


def _privileges(role: str, table: str) -> dict[str, Any]:
    (row,) = sql_dicts(
        "SELECT "
        + ", ".join(
            f"has_table_privilege(CAST(:role AS name), CAST(:table AS text), '{privilege}') "
            f"AS can_{privilege.lower()}"
            for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE")
        ),
        {"role": role, "table": f"curie.{table}"},
    )
    return row


def _tables_present() -> None:
    for table in CALLER_KEY_TABLES:
        assert regclass(table) is not None, table
    rows = sql_dicts(
        "SELECT c.relname, c.relrowsecurity, c.relforcerowsecurity FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'curie' AND c.relname = ANY(:tables)",
        {"tables": list(CALLER_KEY_TABLES)},
    )
    # Enabled, not forced: the owning API publishes through it unaffected.
    assert {
        row["relname"]: (row["relrowsecurity"], row["relforcerowsecurity"]) for row in rows
    } == {table: (True, False) for table in CALLER_KEY_TABLES}
    policies = sql_dicts(
        "SELECT tablename, policyname, cmd FROM pg_policies "
        "WHERE schemaname = 'curie' AND tablename = ANY(:tables)",
        {"tables": list(CALLER_KEY_TABLES)},
    )
    assert sorted((row["tablename"], row["policyname"], row["cmd"]) for row in policies) == sorted(
        (table, f"{table}_select", "SELECT") for table in CALLER_KEY_TABLES
    )
    (primary,) = sql_dicts(
        "SELECT array_agg(a.attname::text ORDER BY a.attname) AS columns "
        "FROM pg_constraint c JOIN pg_attribute a "
        "ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey) "
        "WHERE c.conrelid = to_regclass(:table) AND c.contype = 'p'",
        {"table": f"curie.{KEYS}"},
    )
    assert primary["columns"] == ["installation_id", "public_key"]


def _tables_absent() -> None:
    for table in CALLER_KEY_TABLES:
        assert regclass(table) is None, table


def test_0125_is_an_expand_revision_on_0124() -> None:
    revision = ScriptDirectory.from_config(alembic_config()).get_revision("0125")
    assert revision is not None
    assert revision.down_revision == "0124"
    assert json.loads(REVISION_KINDS.read_text())["0125"] == "expand"


def test_upgrade_grants_an_existing_bundled_role_select_and_the_round_trip_restores_it(
    isolated_migration_db: IsolatedMigrationDb,
    cleanup: SharedCleanup,
    sessions: SessionPool,
) -> None:
    isolated_migration_db.at("0124")
    _tables_absent()
    installation = random_installation()
    role, dsn = _seed_bundled_role(cleanup, installation)
    config = alembic_config()

    command.upgrade(config, "0125")
    assert stamped_revision() == "0125"
    _tables_present()
    for table in CALLER_KEY_TABLES:
        assert _privileges(role, table) == {
            "can_select": True,
            "can_insert": False,
            "can_update": False,
            "can_delete": False,
            "can_truncate": False,
        }, table

    # The owner publishes; the role reads only its own installation's rows.
    mine, theirs = _verify_key(), _verify_key()
    other = random_installation()
    owner = sessions.owner()
    for installation_id, key in ((installation, mine), (other, theirs)):
        owner.execute(
            f"INSERT INTO curie.{KEYS} (installation_id, public_key) VALUES ($1, $2)",
            installation_id,
            key,
        )
        owner.execute(
            f"INSERT INTO curie.{HEARTBEATS} (installation_id) VALUES ($1)", installation_id
        )
    reader = sessions.open(dsn)
    assert reader.fetchval("SELECT current_user") == role
    assert [row[0] for row in reader.fetch(f"SELECT public_key FROM curie.{KEYS}")] == [mine]
    assert [row[0] for row in reader.fetch(f"SELECT installation_id FROM curie.{HEARTBEATS}")] == [
        installation
    ]
    for table in CALLER_KEY_TABLES:
        expect_sqlstate(reader, DENIED, f"DELETE FROM curie.{table}")
        expect_sqlstate(
            reader, DENIED, f"UPDATE curie.{table} SET seen_at = now() - interval '1 day'"
        )
    expect_sqlstate(
        reader,
        DENIED,
        f"INSERT INTO curie.{KEYS} (installation_id, public_key) VALUES ($1, $2)",
        installation,
        _verify_key(),
    )
    reader.close()
    owner.close()

    # Down to 0124: the tables and their grants go, the role stays usable.
    command.downgrade(config, "0124")
    assert stamped_revision() == "0124"
    _tables_absent()
    assert connect_as(dsn) == role

    # Down to 0122: 0123's downgrade still drops the bundled role.
    command.downgrade(config, "0122")
    assert stamped_revision() == "0122"
    _tables_absent()
    assert not role_exists(role)

    command.upgrade(config, "0125")
    assert stamped_revision() == "0125"
    _tables_present()


def test_the_tables_refuse_malformed_installations_and_keys(
    isolated_migration_db: IsolatedMigrationDb, sessions: SessionPool
) -> None:
    isolated_migration_db.at("0125")
    owner = sessions.owner()
    installation, key = random_installation(), _verify_key()
    bad_installations = ("", "with space", "tab\tinside", "x" * 129)
    # Standard base64 of 32 bytes only: urlsafe, unpadded, short and long all refuse.
    raw = base64.b64decode(key)
    bad_keys = (
        key[:-2] + "-=",
        key.rstrip("="),
        base64.b64encode(raw[:31]).decode(),
        base64.b64encode(raw + b"\x00").decode(),
        "not a key",
    )
    for value in bad_installations:
        expect_sqlstate(
            owner,
            CHECK,
            f"INSERT INTO curie.{KEYS} (installation_id, public_key) VALUES ($1, $2)",
            value,
            key,
        )
        expect_sqlstate(
            owner, CHECK, f"INSERT INTO curie.{HEARTBEATS} (installation_id) VALUES ($1)", value
        )
    for value in bad_keys:
        assert value != key
        expect_sqlstate(
            owner,
            CHECK,
            f"INSERT INTO curie.{KEYS} (installation_id, public_key) VALUES ($1, $2)",
            installation,
            value,
        )
    # Liveness: the realistic shapes the publisher writes are accepted, and
    # seen_at defaults to the database clock.
    owner.execute(
        f"INSERT INTO curie.{KEYS} (installation_id, public_key) VALUES ($1, $2)",
        installation,
        key,
    )
    owner.execute(f"INSERT INTO curie.{HEARTBEATS} (installation_id) VALUES ($1)", installation)
    assert owner.fetchval(
        f"SELECT seen_at IS NOT NULL FROM curie.{KEYS} WHERE installation_id = $1", installation
    )
    expect_sqlstate(
        owner,
        "23505",
        f"INSERT INTO curie.{KEYS} (installation_id, public_key) VALUES ($1, $2)",
        installation,
        key,
    )
