"""Shared connector store (ADR 0239).

Six tables hold explicit shared connector applications: the immutable
application record and its pins, the X25519 recipients that credentials and
DSNs are sealed to, the per-application database role, agent memberships with
alias narrowing, credential bindings (ciphertext only; no plaintext column
exists) and durable OAuth refresh intents ("Custody and least privilege" 1,
"Static sources" 1 and 4).

Each application reads the store through its own non-superuser role. Row level
security is ENABLED, not forced, so the owning API role is unaffected. Every
policy derives the caller's application from ``current_user`` mapped through
``shared_connector_database_roles.role_name``, never from a client-set setting
("Custody and least privilege" 3). Application and binding rows also carry an
UPDATE policy, so the role can lock its own rows FOR SHARE or FOR KEY SHARE
through the non-authority ``lock_guard`` column it alone may update.

Refresh and reseal compare-and-set run in four SECURITY DEFINER functions that
map ``session_user`` (the authenticated login, which an application role cannot
change) to the application, so the role never holds UPDATE on refresh state or
credential columns and cannot reset ``recovery_required`` itself ("Static
sources" 5). Lock order everywhere: application row, binding row, refresh intent
row; the recipient row is a leaf, locked last by trigger.

Triggers refuse changes to pinned columns and decreases of monotonic counters
(SQLSTATE 23514, message ``shared_connector_immutable: ...``); lock the
recipient row FOR SHARE on every write naming a recipient and refuse a retired
or unknown one (SQLSTATE 23503, ``shared_connector_recipient_unknown``), which
serializes retirement against writers; and raise ``policy_generation`` once per
affected application whenever membership rows change, including through the
agent foreign key cascade.

Expand only: six new, empty tables. The downgrade revokes each bundled role's
schema grant, drops the functions and tables that held its other grants, then
drops the role. It never uses ``DROP OWNED BY`` (a PG16 CREATEROLE creator holds
ADMIN without INHERIT or SET, which that command rejects), so it also runs as a
non-superuser owner. Managed role names belong to the operator and survive.

Revision ID: 0109
Revises: 0108
Create Date: 2026-10-21
"""

from collections.abc import Sequence
from typing import Any

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0109"
down_revision: str | None = "0108"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"

APPLICATIONS = "shared_connector_applications"
RECIPIENTS = "shared_connector_recipients"
DATABASE_ROLES = "shared_connector_database_roles"
MEMBERSHIPS = "shared_connector_memberships"
BINDINGS = "shared_connector_bindings"
REFRESH_INTENTS = "shared_connector_refresh_intents"
# Creation order; the downgrade drops in reverse.
TABLES = (APPLICATIONS, RECIPIENTS, DATABASE_ROLES, MEMBERSHIPS, BINDINGS, REFRESH_INTENTS)

# The only names a bundled row may carry, and so the only roles the downgrade drops.
BUNDLED_ROLE_NAME = "^curie_sca_[0-9a-f]{40}$"

# Callable by application roles once the API grants EXECUTE; never by PUBLIC.
DEFINER_FUNCTIONS = (
    "shared_connector_refresh_begin(text, text, integer)",
    "shared_connector_refresh_commit(text, uuid, bigint, text, bytea)",
    "shared_connector_refresh_mark_recovery(text, uuid, bigint)",
    "shared_connector_reseal(uuid, bigint, text, text, bytea)",
)

# SQLAlchemy's text() reads ``:name`` as a bind parameter, which would swallow
# POSIX classes such as ``[:space:]``; ``\:`` is its escape for a literal colon.
SPACE_OR_CONTROL = r"[[\:space\:][\:cntrl\:]]"
CONTROL = r"[[\:cntrl\:]]"

# The caller's application in a row level security policy (plan decision 1).
POLICY_APPLICATION = (
    f"(SELECT r.application_id FROM {SCHEMA}.{DATABASE_ROLES} r "
    "WHERE r.role_name = current_user::text)"
)

# The caller's application inside a SECURITY DEFINER function, where
# current_user is the owner and session_user the authenticated login (decision 2).
CALLER_APPLICATION = f"""
    v_application := (
        SELECT r.application_id FROM {SCHEMA}.{DATABASE_ROLES} r
        WHERE r.role_name = session_user::text
    );
    IF v_application IS NULL THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'shared_refresh_not_authorized';
    END IF;"""

CIPHERTEXT_BOUNDS = """
    IF p_ciphertext IS NULL OR octet_length(p_ciphertext) NOT BETWEEN 49 AND 65536 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'shared_ciphertext_out_of_bounds';
    END IF;"""

# Unlocked: a grant's binding never changes, so it names the rows to lock.
GRANT_BINDING = f"""
    SELECT a.installation_id INTO v_installation
    FROM {SCHEMA}.{APPLICATIONS} a WHERE a.id = v_application;
    SELECT i.binding_id INTO v_binding
    FROM {SCHEMA}.{REFRESH_INTENTS} i
    WHERE i.installation_id = v_installation AND i.grant_id = p_grant_id
      AND i.application_id = v_application;
    IF NOT FOUND THEN
        RETURN NEXT;
        RETURN;
    END IF;"""

DEFINER = """
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp"""


def _timestamp(name: str) -> sa.Column[Any]:
    return sa.Column(
        name, sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )


def _create_tables() -> None:
    op.create_table(
        APPLICATIONS,
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("installation_id", sa.Text(), nullable=False),
        # The frozen SharedApplication, model_dump(mode="json").
        sa.Column("spec", postgresql.JSONB(), nullable=False),
        sa.Column("server_image", sa.Text(), nullable=False),
        sa.Column("credential_slots", postgresql.JSONB(), nullable=False),
        sa.Column("provider_origins", postgresql.JSONB(), nullable=False),
        sa.Column("snapshot_seal", postgresql.JSONB(), nullable=True),
        sa.Column("policy", postgresql.JSONB(), nullable=False),
        sa.Column("pin_digest", sa.Text(), nullable=False),
        sa.Column("custody", sa.Text(), nullable=False),
        sa.Column("policy_generation", sa.BigInteger(), server_default="1", nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("disabled_at", sa.DateTime(timezone=True), nullable=True),
        # The only column an application role may update: it lets the role lock
        # its own row FOR SHARE without holding any authority column.
        sa.Column("lock_guard", sa.SmallInteger(), server_default="0", nullable=False),
        _timestamp("created_at"),
        _timestamp("updated_at"),
        sa.CheckConstraint(
            "octet_length(installation_id) BETWEEN 1 AND 128 "
            f"AND installation_id !~ '{SPACE_OR_CONTROL}'",
            name="shared_connector_applications_installation_ck",
        ),
        sa.CheckConstraint(
            "pin_digest ~ '^[0-9a-f]{64}$'", name="shared_connector_applications_pin_digest_ck"
        ),
        sa.CheckConstraint(
            "custody IN ('platform', 'connector')",
            name="shared_connector_applications_custody_ck",
        ),
        sa.CheckConstraint(
            "policy_generation >= 1", name="shared_connector_applications_policy_generation_ck"
        ),
        sa.CheckConstraint(
            "enabled = (disabled_at IS NULL)", name="shared_connector_applications_disabled_ck"
        ),
        sa.PrimaryKeyConstraint("id", name="shared_connector_applications_pkey"),
        # Targets of the bindings' custody key and the intents' installation key.
        sa.UniqueConstraint("id", "custody", name="shared_connector_applications_custody_key"),
        sa.UniqueConstraint(
            "installation_id", "id", name="shared_connector_applications_installation_key"
        ),
        schema=SCHEMA,
    )

    op.create_table(
        RECIPIENTS,
        sa.Column("application_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("recipient_id", sa.Text(), nullable=False),
        sa.Column("algorithm", sa.Text(), nullable=False),
        sa.Column("public_key", postgresql.BYTEA(), nullable=False),
        # The current recipient is the live row with the highest ordinal.
        sa.Column("ordinal", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        _timestamp("created_at"),
        sa.Column("retired_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "recipient_id ~ '^[0-9a-f]{64}$'", name="shared_connector_recipients_recipient_id_ck"
        ),
        # Plan decision 8: the recipient ID is the SHA-256 of the raw public key.
        sa.CheckConstraint(
            "recipient_id = encode(sha256(public_key), 'hex')",
            name="shared_connector_recipients_key_digest_ck",
        ),
        sa.CheckConstraint(
            "algorithm = 'x25519-sealed-box'", name="shared_connector_recipients_algorithm_ck"
        ),
        sa.CheckConstraint(
            "octet_length(public_key) = 32", name="shared_connector_recipients_public_key_ck"
        ),
        sa.ForeignKeyConstraint(
            ["application_id"],
            [f"{SCHEMA}.{APPLICATIONS}.id"],
            name="shared_connector_recipients_application_id_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "application_id", "recipient_id", name="shared_connector_recipients_pkey"
        ),
        schema=SCHEMA,
    )

    op.create_table(
        DATABASE_ROLES,
        sa.Column("application_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("role_name", sa.Text(), nullable=False),
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("status_reason", sa.Text(), nullable=True),
        sa.Column("sealed_dsn", postgresql.BYTEA(), nullable=True),
        sa.Column("dsn_recipient_id", sa.Text(), nullable=True),
        sa.Column("dsn_revision", sa.BigInteger(), server_default="1", nullable=False),
        sa.Column("dsn_secret_name", sa.Text(), nullable=True),
        sa.Column("dsn_secret_key", sa.Text(), nullable=True),
        _timestamp("created_at"),
        _timestamp("updated_at"),
        sa.CheckConstraint(
            "octet_length(role_name) BETWEEN 1 AND 63",
            name="shared_connector_database_roles_role_name_ck",
        ),
        # A bundled name is the API's own hash; the downgrade drops only those.
        sa.CheckConstraint(
            f"(mode = 'bundled' AND role_name ~ '{BUNDLED_ROLE_NAME}') "
            "OR (mode = 'managed' AND role_name ~ '^[a-z_][a-z0-9_]{0,62}$')",
            name="shared_connector_database_roles_role_name_mode_ck",
        ),
        sa.CheckConstraint(
            "mode IN ('bundled', 'managed')", name="shared_connector_database_roles_mode_ck"
        ),
        sa.CheckConstraint(
            "status IN ('provisioned', 'pending', 'ready', 'failed')",
            name="shared_connector_database_roles_status_ck",
        ),
        sa.CheckConstraint(
            "status_reason ~ '^[a-z][a-z0-9_]{0,63}$'",
            name="shared_connector_database_roles_status_reason_ck",
        ),
        sa.CheckConstraint(
            "dsn_revision >= 1", name="shared_connector_database_roles_dsn_revision_ck"
        ),
        sa.CheckConstraint(
            "(mode = 'bundled' AND sealed_dsn IS NOT NULL AND dsn_recipient_id IS NOT NULL "
            "AND dsn_secret_name IS NULL AND dsn_secret_key IS NULL) "
            "OR (mode = 'managed' AND dsn_secret_name IS NOT NULL AND dsn_secret_key IS NOT NULL "
            "AND sealed_dsn IS NULL AND dsn_recipient_id IS NULL)",
            name="shared_connector_database_roles_mode_shape_ck",
        ),
        sa.ForeignKeyConstraint(
            ["application_id"],
            [f"{SCHEMA}.{APPLICATIONS}.id"],
            name="shared_connector_database_roles_application_id_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["application_id", "dsn_recipient_id"],
            [f"{SCHEMA}.{RECIPIENTS}.application_id", f"{SCHEMA}.{RECIPIENTS}.recipient_id"],
            name="shared_connector_database_roles_dsn_recipient_fkey",
        ),
        sa.PrimaryKeyConstraint("application_id", name="shared_connector_database_roles_pkey"),
        sa.UniqueConstraint("role_name", name="shared_connector_database_roles_role_name_key"),
        schema=SCHEMA,
    )

    op.create_table(
        MEMBERSHIPS,
        sa.Column("application_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("alias", sa.Text(), nullable=False),
        # NULL is every policy tool; otherwise a JSON array narrowing them.
        sa.Column("tools", postgresql.JSONB(), nullable=True),
        _timestamp("created_at"),
        _timestamp("updated_at"),
        sa.CheckConstraint(
            "alias ~ '^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$'",
            name="shared_connector_memberships_alias_ck",
        ),
        sa.CheckConstraint(
            "tools IS NULL OR jsonb_typeof(tools) = 'array'",
            name="shared_connector_memberships_tools_ck",
        ),
        sa.ForeignKeyConstraint(
            ["application_id"],
            [f"{SCHEMA}.{APPLICATIONS}.id"],
            name="shared_connector_memberships_application_id_fkey",
            ondelete="CASCADE",
        ),
        # Plan decision 9: a deleted agent has no reach.
        sa.ForeignKeyConstraint(
            ["agent_id"],
            [f"{SCHEMA}.agents.id"],
            name="shared_connector_memberships_agent_id_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "application_id", "agent_id", "alias", name="shared_connector_memberships_pkey"
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_shared_connector_memberships_agent_id", MEMBERSHIPS, ["agent_id"], schema=SCHEMA
    )

    op.create_table(
        BINDINGS,
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("application_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("provider_account", sa.Text(), nullable=False),
        sa.Column("custody", sa.Text(), nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("disabled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("credential_revision", sa.BigInteger(), nullable=False),
        sa.Column("recipient_id", sa.Text(), nullable=True),
        sa.Column("ciphertext", postgresql.BYTEA(), nullable=True),
        sa.Column("source_kind", sa.Text(), nullable=True),
        sa.Column("source_namespace", sa.Text(), nullable=True),
        sa.Column("source_name", sa.Text(), nullable=True),
        sa.Column("source_uid", sa.Text(), nullable=True),
        sa.Column("source_resource_version", sa.Text(), nullable=True),
        sa.Column("source_fingerprint", sa.Text(), nullable=True),
        sa.Column("lock_guard", sa.SmallInteger(), server_default="0", nullable=False),
        _timestamp("created_at"),
        _timestamp("updated_at"),
        sa.CheckConstraint(
            f"octet_length(provider_account) BETWEEN 1 AND 256 AND provider_account !~ '{CONTROL}'",
            name="shared_connector_bindings_provider_account_ck",
        ),
        sa.CheckConstraint(
            "custody IN ('platform', 'connector')", name="shared_connector_bindings_custody_ck"
        ),
        sa.CheckConstraint(
            "enabled = (disabled_at IS NULL)", name="shared_connector_bindings_disabled_ck"
        ),
        sa.CheckConstraint(
            "credential_revision >= 1", name="shared_connector_bindings_credential_revision_ck"
        ),
        sa.CheckConstraint(
            "octet_length(ciphertext) BETWEEN 49 AND 65536",
            name="shared_connector_bindings_ciphertext_ck",
        ),
        sa.CheckConstraint(
            "source_kind = 'kubernetes-secret'", name="shared_connector_bindings_source_kind_ck"
        ),
        sa.CheckConstraint(
            "source_fingerprint ~ '^hmac-sha256:[0-9a-f]{64}$'",
            name="shared_connector_bindings_source_fingerprint_ck",
        ),
        # Plan decision 13: platform custody holds a sealed credential and its
        # source; connector custody is identity-only and holds neither.
        sa.CheckConstraint(
            "(custody = 'platform' AND recipient_id IS NOT NULL AND ciphertext IS NOT NULL "
            "AND source_kind IS NOT NULL AND source_namespace IS NOT NULL "
            "AND source_name IS NOT NULL AND source_uid IS NOT NULL "
            "AND source_resource_version IS NOT NULL AND source_fingerprint IS NOT NULL) "
            "OR (custody = 'connector' AND recipient_id IS NULL AND ciphertext IS NULL "
            "AND source_kind IS NULL AND source_namespace IS NULL AND source_name IS NULL "
            "AND source_uid IS NULL AND source_resource_version IS NULL "
            "AND source_fingerprint IS NULL AND credential_revision = 1)",
            name="shared_connector_bindings_custody_shape_ck",
        ),
        sa.ForeignKeyConstraint(
            ["application_id"],
            [f"{SCHEMA}.{APPLICATIONS}.id"],
            name="shared_connector_bindings_application_id_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["agent_id"],
            [f"{SCHEMA}.agents.id"],
            name="shared_connector_bindings_agent_id_fkey",
            ondelete="CASCADE",
        ),
        # Cascades like the plain application key, so the two never race over
        # one application delete.
        sa.ForeignKeyConstraint(
            ["application_id", "custody"],
            [f"{SCHEMA}.{APPLICATIONS}.id", f"{SCHEMA}.{APPLICATIONS}.custody"],
            name="shared_connector_bindings_custody_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["application_id", "recipient_id"],
            [f"{SCHEMA}.{RECIPIENTS}.application_id", f"{SCHEMA}.{RECIPIENTS}.recipient_id"],
            name="shared_connector_bindings_recipient_fkey",
        ),
        sa.PrimaryKeyConstraint("id", name="shared_connector_bindings_pkey"),
        # Target of the refresh intents' binding key.
        sa.UniqueConstraint(
            "id", "application_id", name="shared_connector_bindings_application_key"
        ),
        schema=SCHEMA,
    )
    # Plan decision 12: at most one enabled binding per (application, agent).
    op.create_index(
        "shared_connector_bindings_enabled_agent_key",
        BINDINGS,
        ["application_id", "agent_id"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("enabled"),
    )
    op.create_index("ix_shared_connector_bindings_agent_id", BINDINGS, ["agent_id"], schema=SCHEMA)
    # Recipient reference counts at retirement, and the application cascade.
    op.create_index(
        "ix_shared_connector_bindings_application_recipient",
        BINDINGS,
        ["application_id", "recipient_id"],
        schema=SCHEMA,
    )

    op.create_table(
        REFRESH_INTENTS,
        sa.Column("installation_id", sa.Text(), nullable=False),
        sa.Column("grant_id", sa.Text(), nullable=False),
        sa.Column("application_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("binding_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("client_config_fingerprint", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), server_default="ready", nullable=False),
        sa.Column("epoch", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("intent_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("owner", sa.Text(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("recovery_reason", sa.Text(), nullable=True),
        _timestamp("created_at"),
        _timestamp("updated_at"),
        sa.CheckConstraint(
            "grant_id ~ '^[A-Za-z0-9._:-]{1,128}$'",
            name="shared_connector_refresh_intents_grant_id_ck",
        ),
        sa.CheckConstraint(
            "client_config_fingerprint ~ '^sha256:[0-9a-f]{64}$'",
            name="shared_connector_refresh_intents_fingerprint_ck",
        ),
        sa.CheckConstraint(
            "state IN ('ready', 'refreshing', 'recovery_required')",
            name="shared_connector_refresh_intents_state_ck",
        ),
        sa.CheckConstraint("epoch >= 0", name="shared_connector_refresh_intents_epoch_ck"),
        sa.CheckConstraint(
            "octet_length(owner) BETWEEN 1 AND 128",
            name="shared_connector_refresh_intents_owner_ck",
        ),
        sa.CheckConstraint(
            "recovery_reason IN ('uncertain_outcome', 'lease_expired')",
            name="shared_connector_refresh_intents_recovery_reason_ck",
        ),
        # A recovery_required row keeps the intent that was in flight, for audit;
        # ownership is never handed to another caller.
        sa.CheckConstraint(
            "(state = 'ready' AND intent_id IS NULL AND owner IS NULL "
            "AND lease_expires_at IS NULL AND recovery_reason IS NULL) "
            "OR (state = 'refreshing' AND intent_id IS NOT NULL AND owner IS NOT NULL "
            "AND lease_expires_at IS NOT NULL AND recovery_reason IS NULL) "
            "OR (state = 'recovery_required' AND recovery_reason IS NOT NULL)",
            name="shared_connector_refresh_intents_state_shape_ck",
        ),
        sa.ForeignKeyConstraint(
            ["binding_id", "application_id"],
            [f"{SCHEMA}.{BINDINGS}.id", f"{SCHEMA}.{BINDINGS}.application_id"],
            name="shared_connector_refresh_intents_binding_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["installation_id", "application_id"],
            [f"{SCHEMA}.{APPLICATIONS}.installation_id", f"{SCHEMA}.{APPLICATIONS}.id"],
            name="shared_connector_refresh_intents_application_fkey",
            ondelete="CASCADE",
        ),
        # Plan decision 14: a grant ID is unique within its installation.
        sa.PrimaryKeyConstraint(
            "installation_id", "grant_id", name="shared_connector_refresh_intents_pkey"
        ),
        sa.UniqueConstraint("binding_id", name="shared_connector_refresh_intents_binding_id_key"),
        schema=SCHEMA,
    )


def _refuse(table: str, detail: str) -> str:
    return (
        "RAISE EXCEPTION USING ERRCODE = '23514', "
        f"MESSAGE = 'shared_connector_immutable: {table}.{detail}';"
    )


# BEFORE UPDATE refusals per table: (pinned columns, (condition, detail) pairs).
PINNED: dict[str, tuple[tuple[str, ...], tuple[tuple[str, str], ...]]] = {
    APPLICATIONS: (
        (
            "id",
            "installation_id",
            "spec",
            "server_image",
            "credential_slots",
            "provider_origins",
            "snapshot_seal",
            "policy",
            "pin_digest",
            "custody",
            "created_at",
        ),
        (
            ("NEW.policy_generation < OLD.policy_generation", "policy_generation decreased"),
            ("NEW.enabled AND NOT OLD.enabled", "enabled cannot return to true"),
        ),
    ),
    RECIPIENTS: (
        ("application_id", "recipient_id", "algorithm", "public_key", "ordinal", "created_at"),
        (
            (
                "OLD.retired_at IS NOT NULL AND NEW.retired_at IS DISTINCT FROM OLD.retired_at",
                "retired_at is set once",
            ),
        ),
    ),
    DATABASE_ROLES: (
        ("application_id", "role_name", "mode", "created_at"),
        (("NEW.dsn_revision < OLD.dsn_revision", "dsn_revision decreased"),),
    ),
    BINDINGS: (
        ("id", "application_id", "agent_id", "provider_account", "custody", "created_at"),
        (
            ("NEW.credential_revision < OLD.credential_revision", "credential_revision decreased"),
            ("NEW.enabled AND NOT OLD.enabled", "enabled cannot return to true"),
        ),
    ),
    REFRESH_INTENTS: (
        (
            "installation_id",
            "grant_id",
            "application_id",
            "binding_id",
            "client_config_fingerprint",
            "created_at",
        ),
        (("NEW.epoch < OLD.epoch", "epoch decreased"),),
    ),
}


def _pinned_function(table: str) -> str:
    pinned, monotonic = PINNED[table]
    checks = [
        f"IF NEW.{column} IS DISTINCT FROM OLD.{column} THEN\n"
        f"        {_refuse(table, column)}\n    END IF;"
        for column in pinned
    ]
    checks += [
        f"IF {condition} THEN\n        {_refuse(table, detail)}\n    END IF;"
        for condition, detail in monotonic
    ]
    body = "\n    ".join(checks)
    return f"""
CREATE FUNCTION {SCHEMA}.{table}_pinned() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    {body}
    RETURN NEW;
END;
$$"""


def _live_recipient_function(table: str, column: str) -> str:
    """Plan decision 16: a writer holds the recipient FOR SHARE until it commits.

    Retirement locks the same row FOR UPDATE before counting references, so a
    retire waits for an open writer, and a writer that waited on a committed
    retire re-reads the row, finds it retired and refuses.
    """

    return f"""
CREATE FUNCTION {SCHEMA}.{table}_live_recipient() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    IF NEW.{column} IS NOT NULL THEN
        PERFORM 1 FROM {SCHEMA}.{RECIPIENTS} r
        WHERE r.application_id = NEW.application_id
          AND r.recipient_id = NEW.{column}
          AND r.retired_at IS NULL
        FOR SHARE;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING ERRCODE = '23503',
                MESSAGE = 'shared_connector_recipient_unknown';
        END IF;
    END IF;
    RETURN NEW;
END;
$$"""


def _membership_function(event: str, applications: str) -> str:
    """Plan decision 9: raise ``policy_generation`` once per affected application.

    SECURITY DEFINER because an agent-delete cascade fires this statement
    trigger as the outer statement's role; a non-owner would otherwise meet row
    level security on the applications table and silently bump nothing.

    Executing a trigger function needs no EXECUTE privilege at firing time, so
    PUBLIC's EXECUTE is revoked (``_create_triggers``) and a role cannot attach
    this function to a table of its own. The scope check is the second layer: it
    refuses, before reading a transition table, any table but the memberships.
    """

    return f"""
CREATE FUNCTION {SCHEMA}.{MEMBERSHIPS}_{event}() RETURNS trigger
{DEFINER}
AS $$
BEGIN
    IF TG_TABLE_SCHEMA IS DISTINCT FROM '{SCHEMA}'
       OR TG_TABLE_NAME IS DISTINCT FROM '{MEMBERSHIPS}' THEN
        RAISE EXCEPTION USING ERRCODE = '42501',
            MESSAGE = 'shared_connector_trigger_scope';
    END IF;
    UPDATE {SCHEMA}.{APPLICATIONS} a
    SET policy_generation = a.policy_generation + 1
    WHERE a.id IN ({applications});
    RETURN NULL;
END;
$$"""


MEMBERSHIP_TRIGGERS = (
    (
        "inserted",
        "INSERT",
        "NEW TABLE AS inserted_rows",
        "SELECT DISTINCT n.application_id FROM inserted_rows n",
    ),
    (
        "updated",
        "UPDATE",
        "OLD TABLE AS old_rows NEW TABLE AS new_rows",
        "SELECT o.application_id FROM old_rows o UNION SELECT n.application_id FROM new_rows n",
    ),
    (
        "deleted",
        "DELETE",
        "OLD TABLE AS deleted_rows",
        "SELECT DISTINCT o.application_id FROM deleted_rows o",
    ),
)
# (table, the column naming a recipient) for the decision 16 triggers.
LIVE_RECIPIENT_COLUMNS = ((BINDINGS, "recipient_id"), (DATABASE_ROLES, "dsn_recipient_id"))
TRIGGER_FUNCTIONS = (
    *(f"{table}_pinned" for table in PINNED),
    *(f"{table}_live_recipient" for table, _ in LIVE_RECIPIENT_COLUMNS),
    *(f"{MEMBERSHIPS}_{event}" for event, *_ in MEMBERSHIP_TRIGGERS),
)


def _create_triggers() -> None:
    for table in PINNED:
        op.execute(_pinned_function(table))
        op.execute(
            f"CREATE TRIGGER {table}_pinned BEFORE UPDATE ON {SCHEMA}.{table} "
            f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.{table}_pinned()"
        )
    for table, column in LIVE_RECIPIENT_COLUMNS:
        op.execute(_live_recipient_function(table, column))
        op.execute(
            f"CREATE TRIGGER {table}_live_recipient "
            f"BEFORE INSERT OR UPDATE OF {column} ON {SCHEMA}.{table} "
            f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.{table}_live_recipient()"
        )
    # Transition tables allow one event per trigger, hence three.
    for event, operation, referencing, applications in MEMBERSHIP_TRIGGERS:
        op.execute(_membership_function(event, applications))
        op.execute(
            f"CREATE TRIGGER {MEMBERSHIPS}_{event} AFTER {operation} ON {SCHEMA}.{MEMBERSHIPS} "
            f"REFERENCING {referencing} "
            f"FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.{MEMBERSHIPS}_{event}()"
        )
    # A function is executable by PUBLIC unless revoked, and the membership ones
    # run as their owner. Firing a trigger does not check EXECUTE.
    for function in TRIGGER_FUNCTIONS:
        op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{function}() FROM PUBLIC")


# (table, the column naming its application). Every table but database_roles
# gets a SELECT policy through POLICY_APPLICATION; database_roles matches the
# role name itself.
APPLICATION_COLUMNS = (
    (APPLICATIONS, "id"),
    (RECIPIENTS, "application_id"),
    (MEMBERSHIPS, "application_id"),
    (BINDINGS, "application_id"),
    (REFRESH_INTENTS, "application_id"),
)
# SELECT ... FOR SHARE and FOR KEY SHARE also apply the UPDATE policy.
UPDATE_POLICY_TABLES = (APPLICATIONS, BINDINGS)


def _create_policies() -> None:
    for table in TABLES:
        op.execute(f"ALTER TABLE {SCHEMA}.{table} ENABLE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY {DATABASE_ROLES}_select ON {SCHEMA}.{DATABASE_ROLES} "
        "FOR SELECT USING (role_name = current_user::text)"
    )
    for table, column in APPLICATION_COLUMNS:
        op.execute(
            f"CREATE POLICY {table}_select ON {SCHEMA}.{table} "
            f"FOR SELECT USING ({column} = {POLICY_APPLICATION})"
        )
        if table in UPDATE_POLICY_TABLES:
            op.execute(
                f"CREATE POLICY {table}_update ON {SCHEMA}.{table} FOR UPDATE "
                f"USING ({column} = {POLICY_APPLICATION}) "
                f"WITH CHECK ({column} = {POLICY_APPLICATION})"
            )


def _drop_policies() -> None:
    # The policies read database_roles, which the tables' own foreign keys
    # require dropping before applications and recipients, so they go first.
    for table in TABLES:
        op.execute(f"DROP POLICY {table}_select ON {SCHEMA}.{table}")
    for table in UPDATE_POLICY_TABLES:
        op.execute(f"DROP POLICY {table}_update ON {SCHEMA}.{table}")


REFRESH_BEGIN = f"""
CREATE FUNCTION {SCHEMA}.shared_connector_refresh_begin(
    p_grant_id text, p_owner text, p_lease_seconds integer
)
RETURNS TABLE (
    outcome text,
    intent_id uuid,
    epoch bigint,
    credential_revision bigint,
    recipient_id text,
    ciphertext bytea
)
{DEFINER}
AS $$
#variable_conflict use_column
DECLARE
    v_application uuid;
    v_installation text;
    v_binding uuid;
    v_application_enabled boolean;
    v_binding_enabled boolean;
    v_custody text;
    v_revision bigint;
    v_recipient text;
    v_ciphertext bytea;
    v_state text;
    v_lease timestamptz;
    v_intent uuid;
    v_epoch bigint;
BEGIN
{CALLER_APPLICATION}
    IF p_owner IS NULL OR octet_length(p_owner) NOT BETWEEN 1 AND 128 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'shared_refresh_owner_invalid';
    END IF;
    IF p_lease_seconds IS NULL OR p_lease_seconds NOT BETWEEN 1 AND 300 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'shared_refresh_lease_invalid';
    END IF;
    outcome := 'not_found';
{GRANT_BINDING}

    -- Plan decision 15, as separate statements. Application FOR SHARE.
    SELECT a.enabled INTO v_application_enabled
    FROM {SCHEMA}.{APPLICATIONS} a WHERE a.id = v_application
    FOR SHARE;
    IF NOT FOUND THEN
        RETURN NEXT;
        RETURN;
    END IF;
    -- Binding FOR SHARE.
    SELECT b.enabled, b.custody, b.credential_revision, b.recipient_id, b.ciphertext
    INTO v_binding_enabled, v_custody, v_revision, v_recipient, v_ciphertext
    FROM {SCHEMA}.{BINDINGS} b
    WHERE b.id = v_binding AND b.application_id = v_application
    FOR SHARE;
    IF NOT FOUND OR v_custody <> 'platform' THEN
        RETURN NEXT;
        RETURN;
    END IF;
    IF NOT v_application_enabled OR NOT v_binding_enabled THEN
        outcome := 'disabled';
        RETURN NEXT;
        RETURN;
    END IF;
    -- Intent FOR UPDATE.
    SELECT i.state, i.lease_expires_at INTO v_state, v_lease
    FROM {SCHEMA}.{REFRESH_INTENTS} i
    WHERE i.installation_id = v_installation AND i.grant_id = p_grant_id
      AND i.application_id = v_application
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN NEXT;
        RETURN;
    END IF;

    IF v_state = 'recovery_required' THEN
        outcome := 'recovery_required';
        RETURN NEXT;
        RETURN;
    END IF;
    IF v_state = 'refreshing' THEN
        IF v_lease > clock_timestamp() THEN
            outcome := 'busy';
            RETURN NEXT;
            RETURN;
        END IF;
        -- An expired lease does not prove the provider call never happened, so
        -- it becomes recovery and ownership never moves to this caller.
        UPDATE {SCHEMA}.{REFRESH_INTENTS} i
        SET state = 'recovery_required', recovery_reason = 'lease_expired', updated_at = now()
        WHERE i.installation_id = v_installation AND i.grant_id = p_grant_id;
        outcome := 'recovery_required';
        RETURN NEXT;
        RETURN;
    END IF;

    v_intent := gen_random_uuid();
    UPDATE {SCHEMA}.{REFRESH_INTENTS} i
    SET state = 'refreshing',
        epoch = i.epoch + 1,
        intent_id = v_intent,
        owner = p_owner,
        lease_expires_at = clock_timestamp() + make_interval(secs => p_lease_seconds),
        recovery_reason = NULL,
        updated_at = now()
    WHERE i.installation_id = v_installation AND i.grant_id = p_grant_id
    RETURNING i.epoch INTO v_epoch;
    outcome := 'began';
    intent_id := v_intent;
    epoch := v_epoch;
    credential_revision := v_revision;
    recipient_id := v_recipient;
    ciphertext := v_ciphertext;
    RETURN NEXT;
    RETURN;
END;
$$"""


REFRESH_COMMIT = f"""
CREATE FUNCTION {SCHEMA}.shared_connector_refresh_commit(
    p_grant_id text, p_intent_id uuid, p_epoch bigint, p_recipient_id text, p_ciphertext bytea
)
RETURNS TABLE (outcome text, credential_revision bigint)
{DEFINER}
AS $$
#variable_conflict use_column
DECLARE
    v_application uuid;
    v_installation text;
    v_binding uuid;
    v_custody text;
    v_state text;
    v_intent uuid;
    v_epoch bigint;
    v_revision bigint;
BEGIN
{CALLER_APPLICATION}
{CIPHERTEXT_BOUNDS}
    outcome := 'not_found';
{GRANT_BINDING}

    -- Plan decision 15. Binding FOR UPDATE, then intent FOR UPDATE; the binding
    -- trigger locks the recipient last.
    SELECT b.custody INTO v_custody
    FROM {SCHEMA}.{BINDINGS} b
    WHERE b.id = v_binding AND b.application_id = v_application
    FOR UPDATE;
    IF NOT FOUND OR v_custody <> 'platform' THEN
        RETURN NEXT;
        RETURN;
    END IF;
    SELECT i.state, i.intent_id, i.epoch INTO v_state, v_intent, v_epoch
    FROM {SCHEMA}.{REFRESH_INTENTS} i
    WHERE i.installation_id = v_installation AND i.grant_id = p_grant_id
      AND i.application_id = v_application
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN NEXT;
        RETURN;
    END IF;

    -- Plan decision 4. The fence is the intent and epoch, not the clock, so a
    -- commit after lease expiry lands while nobody has moved the row on.
    IF v_state <> 'refreshing' OR v_intent IS DISTINCT FROM p_intent_id
            OR v_epoch IS DISTINCT FROM p_epoch THEN
        outcome := 'stale';
        RETURN NEXT;
        RETURN;
    END IF;
    PERFORM 1 FROM {SCHEMA}.{RECIPIENTS} r
    WHERE r.application_id = v_application AND r.recipient_id = p_recipient_id
      AND r.retired_at IS NULL
    FOR SHARE;
    IF NOT FOUND THEN
        outcome := 'recipient_unknown';
        RETURN NEXT;
        RETURN;
    END IF;

    UPDATE {SCHEMA}.{BINDINGS} b
    SET ciphertext = p_ciphertext,
        recipient_id = p_recipient_id,
        credential_revision = b.credential_revision + 1,
        updated_at = now()
    WHERE b.id = v_binding
    RETURNING b.credential_revision INTO v_revision;
    UPDATE {SCHEMA}.{REFRESH_INTENTS} i
    SET state = 'ready',
        intent_id = NULL,
        owner = NULL,
        lease_expires_at = NULL,
        recovery_reason = NULL,
        updated_at = now()
    WHERE i.installation_id = v_installation AND i.grant_id = p_grant_id;
    outcome := 'committed';
    credential_revision := v_revision;
    RETURN NEXT;
    RETURN;
END;
$$"""


REFRESH_MARK_RECOVERY = f"""
CREATE FUNCTION {SCHEMA}.shared_connector_refresh_mark_recovery(
    p_grant_id text, p_intent_id uuid, p_epoch bigint
)
RETURNS text
{DEFINER}
AS $$
DECLARE
    v_application uuid;
    v_installation text;
    v_state text;
    v_intent uuid;
    v_epoch bigint;
BEGIN
{CALLER_APPLICATION}
    SELECT a.installation_id INTO v_installation
    FROM {SCHEMA}.{APPLICATIONS} a WHERE a.id = v_application;
    -- Plan decision 15. Only the intent is locked.
    SELECT i.state, i.intent_id, i.epoch INTO v_state, v_intent, v_epoch
    FROM {SCHEMA}.{REFRESH_INTENTS} i
    WHERE i.installation_id = v_installation AND i.grant_id = p_grant_id
      AND i.application_id = v_application
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN 'not_found';
    END IF;
    IF v_state <> 'refreshing' OR v_intent IS DISTINCT FROM p_intent_id
            OR v_epoch IS DISTINCT FROM p_epoch THEN
        RETURN 'stale';
    END IF;
    UPDATE {SCHEMA}.{REFRESH_INTENTS} i
    SET state = 'recovery_required', recovery_reason = 'uncertain_outcome', updated_at = now()
    WHERE i.installation_id = v_installation AND i.grant_id = p_grant_id;
    RETURN 'recovery_required';
END;
$$"""


RESEAL = f"""
CREATE FUNCTION {SCHEMA}.shared_connector_reseal(
    p_binding_id uuid,
    p_expected_revision bigint,
    p_from_recipient text,
    p_to_recipient text,
    p_ciphertext bytea
)
RETURNS text
{DEFINER}
AS $$
DECLARE
    v_application uuid;
    v_custody text;
    v_revision bigint;
    v_recipient text;
BEGIN
{CALLER_APPLICATION}
{CIPHERTEXT_BOUNDS}
    -- Plan decision 15. Binding FOR UPDATE; the recipient is the leaf.
    SELECT b.custody, b.credential_revision, b.recipient_id
    INTO v_custody, v_revision, v_recipient
    FROM {SCHEMA}.{BINDINGS} b
    WHERE b.id = p_binding_id AND b.application_id = v_application
    FOR UPDATE;
    IF NOT FOUND OR v_custody <> 'platform' THEN
        RETURN 'not_found';
    END IF;
    IF v_revision IS DISTINCT FROM p_expected_revision
            OR v_recipient IS DISTINCT FROM p_from_recipient THEN
        RETURN 'stale';
    END IF;
    IF p_to_recipient IS NULL OR p_to_recipient = v_recipient THEN
        RETURN 'recipient_unknown';
    END IF;
    PERFORM 1 FROM {SCHEMA}.{RECIPIENTS} r
    WHERE r.application_id = v_application AND r.recipient_id = p_to_recipient
      AND r.retired_at IS NULL
    FOR SHARE;
    IF NOT FOUND THEN
        RETURN 'recipient_unknown';
    END IF;
    -- The credential is unchanged, so its revision is too.
    UPDATE {SCHEMA}.{BINDINGS} b
    SET ciphertext = p_ciphertext, recipient_id = p_to_recipient, updated_at = now()
    WHERE b.id = p_binding_id;
    RETURN 'resealed';
END;
$$"""


def _create_functions() -> None:
    for function in (REFRESH_BEGIN, REFRESH_COMMIT, REFRESH_MARK_RECOVERY, RESEAL):
        op.execute(function)
    for signature in DEFINER_FUNCTIONS:
        op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{signature} FROM PUBLIC")


def upgrade() -> None:
    _create_tables()
    _create_triggers()
    _create_policies()
    _create_functions()
    for table in TABLES:
        op.execute(f"REVOKE ALL ON TABLE {SCHEMA}.{table} FROM PUBLIC")


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def downgrade() -> None:
    # Plan decision 19. Read the bundled roles before their rows go; only names
    # matching the API's own hash are ever dropped.
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
    for name in bundled:
        op.execute(f"REVOKE ALL ON SCHEMA {SCHEMA} FROM {_quote_ident(name)}")
    for signature in DEFINER_FUNCTIONS:
        op.execute(f"DROP FUNCTION {SCHEMA}.{signature}")
    _drop_policies()
    # Dropping a table drops its triggers and grants with it.
    for table in reversed(TABLES):
        op.drop_table(table, schema=SCHEMA)
    for function in TRIGGER_FUNCTIONS:
        op.execute(f"DROP FUNCTION {SCHEMA}.{function}()")
    # Nothing references the roles any more, so no DROP OWNED BY is needed.
    for name in bundled:
        op.execute(f"DROP ROLE IF EXISTS {_quote_ident(name)}")
