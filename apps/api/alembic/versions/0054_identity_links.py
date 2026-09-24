"""Identity links: provider-native ids mapped to principals (#2910, ADR 0155 step 5).

Adds tenant scoped ``identity_links``: one provider-native id (a Slack user id)
on one provider installation, linked to exactly one subject, a principal or a
bot (an Agent). Resolution reads only these rows, by exact equality; it never
derives an identity from a principal's email or display name.

Uniqueness covers principal links only. Two principals cannot claim one native
id on one installation, but a bot link may repeat: today's static Slack app is
one bot user that fronts every bound Agent, so one native id legitimately maps
to many bots.

``provider_installations`` gains ``UNIQUE (tenant_id, id)`` as the target of
the link's tenant-scoped foreign key, as ``principals_tenant_id_id_key`` is for
0052. It cannot fail on existing rows because ``id`` is the primary key.

Revision ID: 0054
Revises: 0053
Create Date: 2026-09-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0054"
down_revision: str | None = "0053"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.create_unique_constraint(
        "provider_installations_tenant_id_id_key",
        "provider_installations",
        ["tenant_id", "id"],
        schema=SCHEMA,
    )

    op.create_table(
        "identity_links",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("principal_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("bot_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("provider_installation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("provider_native_id", sa.String(), nullable=False),
        sa.Column("verification_source", sa.String(), nullable=False),
        sa.Column(
            "verified_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("created_by_principal_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.CheckConstraint(
            "(principal_id IS NULL) <> (bot_id IS NULL)",
            name="identity_links_subject_xor_ck",
        ),
        sa.CheckConstraint(
            "verification_source IN ('provider_event_verified', 'admin_mapped')",
            name="identity_links_verification_source_ck",
        ),
        sa.CheckConstraint(
            "length(provider_native_id) > 0",
            name="identity_links_provider_native_id_ck",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            [f"{SCHEMA}.tenants.id"],
            name="identity_links_tenant_id_fkey",
        ),
        # The tenant-carrying keys keep a link, its principal, its creator and
        # its installation inside one tenant.
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
        sa.ForeignKeyConstraint(
            ["tenant_id", "provider_installation_id"],
            [
                f"{SCHEMA}.provider_installations.tenant_id",
                f"{SCHEMA}.provider_installations.id",
            ],
            name="identity_links_installation_fkey",
        ),
        # agents has no tenant_id yet (#2911), so this key cannot carry one.
        sa.ForeignKeyConstraint(
            ["bot_id"],
            [f"{SCHEMA}.agents.id"],
            name="identity_links_bot_id_fkey",
        ),
        sa.PrimaryKeyConstraint("id", name="identity_links_pkey"),
        schema=SCHEMA,
    )
    op.create_index(
        "identity_links_principal_native_key",
        "identity_links",
        ["provider_installation_id", "provider_native_id"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("principal_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("identity_links_principal_native_key", table_name="identity_links", schema=SCHEMA)
    op.drop_table("identity_links", schema=SCHEMA)
    op.drop_constraint(
        "provider_installations_tenant_id_id_key",
        "provider_installations",
        type_="unique",
        schema=SCHEMA,
    )
