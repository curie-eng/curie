"""Publish the shared proxy's caller verify keys (ADR 0239).

A shared application's proxy verifies the caller tokens the worker signs, so it
needs the installation's caller verify keys. The frozen application render has
no input for them, and the operator already configures them for the API
(``CURIE_CONNECTOR_CALLER_PUBLIC_KEY`` and the previous key during a rotation).
Every API process therefore publishes the keys it carries, and the proxy reads
them through its own application role.

``curie.shared_connector_caller_keys`` holds one row per (installation, public
key) with the ``seen_at`` of the newest publication that carried it, and
``curie.shared_connector_caller_key_heartbeats`` one row per installation with
the ``seen_at`` of its newest publication. The proxy accepts the keys seen
within its window of the newest heartbeat, so a key ages out once no live API
process carries it, and an API outage freezes the last set instead of
expiring it.

Both tables enable row level security (not forced, so the owning API publishes
unaffected) with one SELECT policy each: a row is visible when its installation
is the installation of the reading role's application, derived from
``current_user`` through 0123's ``POLICY_APPLICATION``. There is no write
policy. SELECT on both is granted to every bundled application role that
already exists, so a proxy provisioned before this revision can read its keys;
the API grants it to new roles at provisioning.

Expand only: two new tables. The downgrade drops both, which drops their grants
too, so 0123's downgrade can still drop a bundled role.

Revision ID: 0125
Revises: 0124
Create Date: 2026-10-23
"""

import importlib.util
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType

import sqlalchemy as sa
from alembic import op

revision: str = "0125"
down_revision: str | None = "0124"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _load_0123() -> ModuleType:
    """The merged 0123 migration, whose definitions are immutable and so safe to derive from."""

    path = Path(__file__).with_name("0123_shared_connector_store.py")
    spec = importlib.util.spec_from_file_location("_curie_migration_0123", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load migration 0123 from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_CP1 = _load_0123()
SCHEMA: str = _CP1.SCHEMA
APPLICATIONS: str = _CP1.APPLICATIONS
DATABASE_ROLES: str = _CP1.DATABASE_ROLES
BUNDLED_ROLE_NAME: str = _CP1.BUNDLED_ROLE_NAME
SPACE_OR_CONTROL: str = _CP1.SPACE_OR_CONTROL
POLICY_APPLICATION: str = _CP1.POLICY_APPLICATION

KEYS = "shared_connector_caller_keys"
HEARTBEATS = "shared_connector_caller_key_heartbeats"
TABLES = (KEYS, HEARTBEATS)

# The installation of the reading role's application.
POLICY_INSTALLATION = (
    f"(SELECT a.installation_id FROM {SCHEMA}.{APPLICATIONS} a WHERE a.id = {POLICY_APPLICATION})"
)


def _installation_check(table: str) -> sa.CheckConstraint:
    return sa.CheckConstraint(
        "octet_length(installation_id) BETWEEN 1 AND 128 "
        f"AND installation_id !~ '{SPACE_OR_CONTROL}'",
        name=f"{table}_installation_ck",
    )


def _seen_at() -> sa.Column[sa.DateTime]:
    return sa.Column(
        "seen_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def upgrade() -> None:
    op.create_table(
        KEYS,
        sa.Column("installation_id", sa.Text(), nullable=False),
        # Standard base64 of a 32-byte Ed25519 public key, as the chart carries it.
        sa.Column("public_key", sa.Text(), nullable=False),
        _seen_at(),
        _installation_check(KEYS),
        sa.CheckConstraint("public_key ~ '^[A-Za-z0-9+/]{43}=$'", name=f"{KEYS}_public_key_ck"),
        sa.PrimaryKeyConstraint("installation_id", "public_key", name=f"{KEYS}_pkey"),
        schema=SCHEMA,
    )
    op.create_table(
        HEARTBEATS,
        sa.Column("installation_id", sa.Text(), nullable=False),
        _seen_at(),
        _installation_check(HEARTBEATS),
        sa.PrimaryKeyConstraint("installation_id", name=f"{HEARTBEATS}_pkey"),
        schema=SCHEMA,
    )
    for table in TABLES:
        op.execute(f"ALTER TABLE {SCHEMA}.{table} ENABLE ROW LEVEL SECURITY")
        op.execute(
            f"CREATE POLICY {table}_select ON {SCHEMA}.{table} "
            f"FOR SELECT USING (installation_id = {POLICY_INSTALLATION})"
        )
    # Roles provisioned before this revision; only names matching the API's own
    # hash, as 0123's downgrade selects them.
    bundled = [
        str(name)
        for (name,) in op.get_bind().execute(
            sa.text(
                f"SELECT d.role_name FROM {SCHEMA}.{DATABASE_ROLES} d "
                "WHERE d.mode = 'bundled' AND d.role_name ~ :pattern "
                "AND EXISTS (SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname = d.role_name) "
                "ORDER BY d.role_name"
            ),
            {"pattern": BUNDLED_ROLE_NAME},
        )
    ]
    tables = ", ".join(f"{SCHEMA}.{table}" for table in TABLES)
    for name in bundled:
        op.execute(f"GRANT SELECT ON TABLE {tables} TO {_quote_ident(name)}")


def downgrade() -> None:
    # Dropping a table drops its policy and grants with it.
    for table in reversed(TABLES):
        op.drop_table(table, schema=SCHEMA)
