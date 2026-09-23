"""Provider installations: connected external accounts (#2909, ADR 0155 step 4).

Adds tenant scoped ``provider_installations``: one row per connected external
account, such as one Slack workspace. ``credential_ref`` and
``webhook_verification_ref`` point into the deployment's secret store and are
CHECKed to be references (``env:NAME`` or ``k8s-secret:name/key``) that do
not have the shape of a well-known credential, whoever writes them. The row for today's
static Slack app is created by the API at boot, not here: only the API knows
whether a bot token is configured.

Revision ID: 0053
Revises: 0052
Create Date: 2026-09-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0053"
down_revision: str | None = "0052"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"

# Frozen copies of curie_api.models.PROVIDER_REFERENCE_* at 0053.
REFERENCE_PATTERN = (
    r"^(env:[A-Z_][A-Z0-9_]*"
    r"|k8s-secret:[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?/[-._a-zA-Z0-9]{1,253})$"
)
REFERENCE_MAX_LENGTH = 512
REFERENCE_DENY_PATTERN = (
    r"[:/](xox[a-z]-|xoxe\.|xapp-|gh[pousr]_|github_pat_|sk-|sk_live_|rk_live_|lin_api_|AIza|eyJ)"
    r"|^env:(AKIA|ASIA)[A-Z0-9]{16}$"
    r"|[0-9a-fA-F]{32}"
)


def _reference_check(column: str) -> sa.CheckConstraint:
    return sa.CheckConstraint(
        f"{column} IS NULL OR (length({column}) <= {REFERENCE_MAX_LENGTH} "
        f"AND {column} ~ '{REFERENCE_PATTERN}' "
        f"AND {column} !~ '{REFERENCE_DENY_PATTERN}')",
        name=f"provider_installations_{column}_ck",
    )


def upgrade() -> None:
    op.create_table(
        "provider_installations",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("provider", sa.String(), nullable=False),
        sa.Column("external_account_id", sa.String(), nullable=False),
        sa.Column("display_name", sa.String(), nullable=True),
        sa.Column("credential_ref", sa.String(), nullable=True),
        sa.Column(
            "scopes",
            postgresql.JSONB(),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("webhook_verification_ref", sa.String(), nullable=True),
        sa.Column("status", sa.String(), server_default="connected", nullable=False),
        sa.Column("installed_by_principal_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "installed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("disconnected_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "provider IN ('slack', 'm365', 'github', 'jira', 'linear', "
            "'confluence', 'quickbooks', 'other')",
            name="provider_installations_provider_ck",
        ),
        sa.CheckConstraint(
            "status IN ('connected', 'disconnected', 'degraded')",
            name="provider_installations_status_ck",
        ),
        sa.CheckConstraint(
            "(status = 'disconnected') = (disconnected_at IS NOT NULL)",
            name="provider_installations_disconnected_at_ck",
        ),
        _reference_check("credential_ref"),
        _reference_check("webhook_verification_ref"),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            [f"{SCHEMA}.tenants.id"],
            name="provider_installations_tenant_id_fkey",
        ),
        # Carries tenant_id, so the installer must be a principal of the same tenant.
        sa.ForeignKeyConstraint(
            ["tenant_id", "installed_by_principal_id"],
            [f"{SCHEMA}.principals.tenant_id", f"{SCHEMA}.principals.id"],
            name="provider_installations_installer_fkey",
        ),
        sa.PrimaryKeyConstraint("id", name="provider_installations_pkey"),
        sa.UniqueConstraint(
            "tenant_id",
            "provider",
            "external_account_id",
            name="provider_installations_tenant_provider_external_key",
        ),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_table("provider_installations", schema=SCHEMA)
