"""Directory authority, identity audit and connect evidence (ADR 0220).

Revision ID: 0122
Revises: 0121
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0122"
down_revision: str | None = "0121"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
S = "curie"
U = postgresql.UUID(as_uuid=True)


def upgrade() -> None:
    op.alter_column("approval_audit_entries", "authorizer", nullable=True, schema=S)
    op.create_table(
        "tenant_identity_providers",
        sa.Column("tenant_id", U, sa.ForeignKey(f"{S}.tenants.id"), primary_key=True),
        sa.Column("issuer", sa.String(), nullable=False),
        sa.Column("governed_domains", postgresql.JSONB(), nullable=False),
        sa.Column("managed_by", sa.String(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.CheckConstraint("version >= 1", name="tenant_identity_providers_version_ck"),
        schema=S,
    )
    op.create_table(
        "identity_audit_events",
        sa.Column("id", U, primary_key=True),
        sa.Column("tenant_id", U, sa.ForeignKey(f"{S}.tenants.id"), nullable=False),
        sa.Column("event_type", sa.String(), nullable=False),
        sa.Column("actor", sa.String(), nullable=False),
        sa.Column("principal_id", U),
        sa.Column("team_id", U),
        sa.Column("details", postgresql.JSONB(), nullable=False, server_default="{}"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "principal_id"],
            [f"{S}.principals.tenant_id", f"{S}.principals.id"],
            name="identity_audit_principal_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "team_id"],
            [f"{S}.teams.tenant_id", f"{S}.teams.id"],
            name="identity_audit_team_fkey",
        ),
        schema=S,
    )
    op.create_table(
        "identity_connect_tokens",
        sa.Column("token_hash", sa.String(), primary_key=True),
        sa.Column("tenant_id", U, sa.ForeignKey(f"{S}.tenants.id"), nullable=False),
        sa.Column("identity_namespace_id", U, nullable=False),
        sa.Column("provider_native_id", sa.String(), nullable=False),
        sa.Column("idp_issuer", sa.String(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("consumed_at", sa.DateTime(timezone=True)),
        sa.Column("browser_proof_hash", sa.String(64)),
        sa.Column("verified_email", sa.String(320)),
        sa.Column("verified_principal_id", U),
        sa.Column("verified_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "identity_namespace_id"],
            [f"{S}.identity_namespaces.tenant_id", f"{S}.identity_namespaces.id"],
            name="identity_connect_namespace_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "verified_principal_id"],
            [f"{S}.principals.tenant_id", f"{S}.principals.id"],
            name="identity_connect_principal_fkey",
        ),
        schema=S,
    )
    op.add_column("identity_links", sa.Column("created_by_directory", sa.String()), schema=S)
    op.alter_column("identity_links", "created_by_principal_id", nullable=True, schema=S)
    op.drop_constraint("identity_links_verification_source_ck", "identity_links", schema=S)
    op.create_check_constraint(
        "identity_links_verification_source_ck",
        "identity_links",
        "verification_source IN ('provider_event_verified', 'admin_mapped', 'idp_directory')",
        schema=S,
    )
    op.create_check_constraint(
        "identity_links_creator_ck",
        "identity_links",
        "(verification_source = 'idp_directory' AND created_by_principal_id IS NULL AND "
        "created_by_directory IS NOT NULL AND length(created_by_directory) > 0) OR "
        "(verification_source <> 'idp_directory' AND created_by_principal_id IS NOT NULL "
        "AND created_by_directory IS NULL)",
        schema=S,
    )
    for name, datatype in [
        ("team_id", U),
        ("team_epoch", sa.Integer()),
        ("team_digest", sa.String()),
    ]:
        op.add_column("approval_audit_entries", sa.Column(name, datatype), schema=S)
    op.drop_constraint("identity_namespaces_provider_ck", "identity_namespaces", schema=S)
    op.create_check_constraint(
        "identity_namespaces_provider_ck",
        "identity_namespaces",
        "provider IN ('slack', 'm365', 'github', 'jira', 'linear', 'confluence', "
        "'quickbooks', 'other', 'email')",
        schema=S,
    )
    op.drop_constraint("identity_namespaces_kind_ck", "identity_namespaces", schema=S)
    op.create_check_constraint(
        "identity_namespaces_kind_ck",
        "identity_namespaces",
        "kind IN ('slack_workspace', 'email_domain')",
        schema=S,
    )
    op.create_check_constraint(
        "identity_namespaces_email_domain_ck",
        "identity_namespaces",
        "kind <> 'email_domain' OR (provider = 'email' AND key = lower(key) AND "
        "position('@' in key) = 0)",
        schema=S,
    )


def downgrade() -> None:
    # Team-only decisions never evaluated a legacy set. Do not invent legacy
    # authority or silently discard that audit history to fit the old schema.
    if op.get_bind().scalar(sa.text(
        "SELECT EXISTS (SELECT 1 FROM curie.approval_audit_entries WHERE authorizer IS NULL)"
    )):
        raise RuntimeError("cannot downgrade while team-only approval audit rows exist")
    op.alter_column("approval_audit_entries", "authorizer", nullable=False, schema=S)
    # No truthful old-format representation exists for directory-authored rows.
    op.execute("DELETE FROM curie.identity_links WHERE verification_source = 'idp_directory'")
    op.drop_table("identity_connect_tokens", schema=S)
    op.execute(
        "DELETE FROM curie.identity_links WHERE identity_namespace_id IN (SELECT id FROM "
        "curie.identity_namespaces WHERE kind = 'email_domain')"
    )
    op.execute("DELETE FROM curie.identity_namespaces WHERE kind = 'email_domain'")
    op.drop_constraint("identity_namespaces_email_domain_ck", "identity_namespaces", schema=S)
    op.drop_constraint("identity_namespaces_kind_ck", "identity_namespaces", schema=S)
    op.create_check_constraint(
        "identity_namespaces_kind_ck",
        "identity_namespaces",
        "kind IN ('slack_workspace')",
        schema=S,
    )
    op.drop_constraint("identity_namespaces_provider_ck", "identity_namespaces", schema=S)
    op.create_check_constraint(
        "identity_namespaces_provider_ck",
        "identity_namespaces",
        "provider IN ('slack', 'm365', 'github', 'jira', 'linear', 'confluence', "
        "'quickbooks', 'other')",
        schema=S,
    )
    for name in ["team_digest", "team_epoch", "team_id"]:
        op.drop_column("approval_audit_entries", name, schema=S)
    op.drop_constraint("identity_links_creator_ck", "identity_links", schema=S)
    op.drop_constraint("identity_links_verification_source_ck", "identity_links", schema=S)
    op.create_check_constraint(
        "identity_links_verification_source_ck",
        "identity_links",
        "verification_source IN ('provider_event_verified', 'admin_mapped')",
        schema=S,
    )
    op.alter_column("identity_links", "created_by_principal_id", nullable=False, schema=S)
    op.drop_column("identity_links", "created_by_directory", schema=S)
    op.drop_table("identity_audit_events", schema=S)
    op.drop_table("tenant_identity_providers", schema=S)
