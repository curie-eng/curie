"""Identity namespaces and identity links (#2910, ADR 0198 decision 5).

``identity_namespaces`` holds where a provider's user ids are unique, per
tenant: for Slack, one ``slack_workspace`` per team id, scoped by the
receiving installation's ``authority`` (ADR 0198 decision 6, property 3).
``identity_links`` binds one native id inside one namespace to exactly one
principal or one bot. Among ACTIVE links ``(namespace, native id)`` is unique,
whatever the target, so one id means one thing at a time; a revoked link keeps
its row and evidence and stops counting. The namespace, the principal target,
the bot target and the creating principal are composite foreign keys carrying
``tenant_id``, so no link crosses a tenant (the bot key targets
``agents_tenant_id_id_key``, which 0101 added). Every foreign key is NO ACTION: removing a
namespace, principal or agent never silently drops or moves a link (ADR 0198
decision 9).

Expand-only: two new, empty tables, and no existing row is touched.

Revision ID: 0102
Revises: 0101
Create Date: 2026-10-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0102"
down_revision: str | None = "0101"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"

# Frozen copy of 0083's provider vocabulary.
PROVIDER_CK = (
    "provider IN ('slack', 'm365', 'github', 'jira', 'linear', 'confluence', 'quickbooks', 'other')"
)


def upgrade() -> None:
    op.create_table(
        "identity_namespaces",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("provider", sa.String(), nullable=False),
        # The receiving installation's authority, never the event's (ADR 0198
        # decision 6, property 3). NOT NULL with an empty default, like
        # provider_installations.authority, so uniqueness compares it.
        sa.Column("authority", sa.String(), server_default="", nullable=False),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column("key", sa.String(), nullable=False),
        sa.Column("status", sa.String(), server_default="active", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(PROVIDER_CK, name="identity_namespaces_provider_ck"),
        sa.CheckConstraint("kind IN ('slack_workspace')", name="identity_namespaces_kind_ck"),
        sa.CheckConstraint(
            "status IN ('active', 'disabled')", name="identity_namespaces_status_ck"
        ),
        # A Slack workspace is a Slack team id, never an enterprise id or a
        # case variant: a key that cannot match a real team never stores.
        sa.CheckConstraint(
            "kind <> 'slack_workspace' OR (provider = 'slack' AND key ~ '^T[A-Z0-9]{2,}$')",
            name="identity_namespaces_slack_workspace_key_ck",
        ),
        sa.CheckConstraint("length(key) > 0", name="identity_namespaces_key_ck"),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            [f"{SCHEMA}.tenants.id"],
            name="identity_namespaces_tenant_id_fkey",
        ),
        sa.PrimaryKeyConstraint("id", name="identity_namespaces_pkey"),
        sa.UniqueConstraint(
            "tenant_id",
            "provider",
            "authority",
            "kind",
            "key",
            name="identity_namespaces_tenant_provider_authority_kind_key_key",
        ),
        # `id` is already the primary key; a composite foreign key needs this
        # so a link's namespace must belong to the link's own tenant.
        sa.UniqueConstraint("tenant_id", "id", name="identity_namespaces_tenant_id_id_key"),
        schema=SCHEMA,
    )
    op.create_table(
        "identity_links",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("identity_namespace_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("provider_native_id", sa.String(), nullable=False),
        sa.Column("principal_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("bot_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("verification_source", sa.String(), nullable=False),
        # Link creation is the trust boundary (ADR 0198 decision 8), so who
        # made the link is mandatory.
        sa.Column("created_by_principal_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "verified_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "(principal_id IS NULL) <> (bot_id IS NULL)", name="identity_links_target_xor_ck"
        ),
        sa.CheckConstraint(
            "verification_source IN ('provider_event_verified', 'admin_mapped')",
            name="identity_links_verification_source_ck",
        ),
        sa.CheckConstraint("length(provider_native_id) > 0", name="identity_links_native_id_ck"),
        sa.ForeignKeyConstraint(
            ["tenant_id", "identity_namespace_id"],
            [f"{SCHEMA}.identity_namespaces.tenant_id", f"{SCHEMA}.identity_namespaces.id"],
            name="identity_links_namespace_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "principal_id"],
            [f"{SCHEMA}.principals.tenant_id", f"{SCHEMA}.principals.id"],
            name="identity_links_principal_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "created_by_principal_id"],
            [f"{SCHEMA}.principals.tenant_id", f"{SCHEMA}.principals.id"],
            name="identity_links_created_by_fkey",
        ),
        # Carries tenant_id, so a link's bot is an agent of the same tenant.
        sa.ForeignKeyConstraint(
            ["tenant_id", "bot_id"],
            [f"{SCHEMA}.agents.tenant_id", f"{SCHEMA}.agents.id"],
            name="identity_links_bot_fkey",
        ),
        sa.PrimaryKeyConstraint("id", name="identity_links_pkey"),
        schema=SCHEMA,
    )
    # Active-only: a revoked link keeps its row and evidence without blocking
    # the replacement link (ADR 0198 decision 5).
    op.create_index(
        "identity_links_active_native_key",
        "identity_links",
        ["identity_namespace_id", "provider_native_id"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("revoked_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_table("identity_links", schema=SCHEMA)
    op.drop_table("identity_namespaces", schema=SCHEMA)
