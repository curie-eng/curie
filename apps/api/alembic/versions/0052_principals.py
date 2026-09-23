"""Principal identity: no-behavior-change slice (#2907, ADR 0155 step 2).

Adds tenant scoped ``principals``, ``teams`` and ``principal_teams``. A
principal is keyed on its IdP subject within a tenant; email and display name
are attributes and never the key. ``teams`` and ``principal_teams`` are a
rebuildable projection of the IdP's groups, not a system of record for
membership. Nothing reads or writes these tables yet.

Revision ID: 0052
Revises: 0051
Create Date: 2026-09-22
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0052"
down_revision: str | None = "0051"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.create_table(
        "principals",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("idp_subject", sa.String(), nullable=False),
        sa.Column("type", sa.String(), nullable=False),
        sa.Column(
            "status", sa.String(), server_default="active", nullable=False
        ),
        sa.Column("display_name", sa.String(), nullable=True),
        sa.Column("email", sa.String(), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "authorization_version",
            sa.Integer(),
            server_default="1",
            nullable=False,
        ),
        sa.CheckConstraint(
            "type IN ('human', 'service')", name="principals_type_ck"
        ),
        sa.CheckConstraint(
            "status IN ('active', 'disabled', 'revoked')",
            name="principals_status_ck",
        ),
        sa.CheckConstraint(
            "authorization_version >= 1",
            name="principals_authorization_version_ck",
        ),
        sa.ForeignKeyConstraint(["tenant_id"], [f"{SCHEMA}.tenants.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "tenant_id", "idp_subject", name="principals_tenant_idp_subject_key"
        ),
        schema=SCHEMA,
    )

    op.create_table(
        "teams",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("external_id", sa.String(), nullable=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.CheckConstraint(
            "source IN ('idp_group', 'curie_managed')", name="teams_source_ck"
        ),
        sa.CheckConstraint(
            "source <> 'idp_group' OR external_id IS NOT NULL",
            name="teams_idp_group_external_id_ck",
        ),
        sa.ForeignKeyConstraint(["tenant_id"], [f"{SCHEMA}.tenants.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "tenant_id",
            "source",
            "external_id",
            name="teams_tenant_source_external_id_key",
        ),
        schema=SCHEMA,
    )

    op.create_table(
        "principal_teams",
        sa.Column("principal_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("team_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.Column(
            "synced_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "source IN ('idp_group', 'curie_managed')",
            name="principal_teams_source_ck",
        ),
        sa.CheckConstraint("version >= 1", name="principal_teams_version_ck"),
        sa.ForeignKeyConstraint(
            ["principal_id"], [f"{SCHEMA}.principals.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["team_id"], [f"{SCHEMA}.teams.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("principal_id", "team_id"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_principal_teams_team_id",
        "principal_teams",
        ["team_id"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_principal_teams_team_id", table_name="principal_teams", schema=SCHEMA
    )
    op.drop_table("principal_teams", schema=SCHEMA)
    op.drop_table("teams", schema=SCHEMA)
    op.drop_table("principals", schema=SCHEMA)
