"""Provider installations and channel identities (#2909, ADR 0166 step 4).

ADR 0193 supersedes ADR 0168 decision 1 and splits what used to be one table
into two. ``provider_installations`` keeps ADR 0166's meaning: a connected
external account the tenant authorised, now scoped by ``authority`` (the
provider endpoint its ids belong to -- empty for a provider with one global
service, the canonical hostname for a self-hosted or per-host one) so a
self-hosted installation's id cannot collide with the same id on another host.
``channel_identities`` holds what ADR 0168 decision 1 put here: who Curie
speaks as. Its ``name`` is what a binding's ``adapter`` names (ADR 0168
decision 3), unique with ``(tenant_id, provider)``. It attaches to an
installation of its own tenant and provider through a nullable composite
foreign key; a declared identity is created at boot unattached (ADR 0193
decision 4), since only the identity's own credential can later report which
installation it belongs to (#3039). ``credential_ref`` and
``webhook_verification_ref`` point into the deployment's secret store and are
CHECKed to be references (``env:NAME`` or ``k8s-secret:name/key``) that do not
have the shape of a well-known credential, whoever writes them.

Revision ID: 0083
Revises: 0082
Create Date: 2026-09-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0083"
down_revision: str | None = "0082"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"

# Frozen copies of curie_api.models.PROVIDER_REFERENCE_* at 0078.
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

PROVIDER_CK = (
    "provider IN ('slack', 'm365', 'github', 'jira', 'linear', "
    "'confluence', 'quickbooks', 'other')"
)


def _reference_check(table: str, column: str) -> sa.CheckConstraint:
    return sa.CheckConstraint(
        f"{column} IS NULL OR (length({column}) <= {REFERENCE_MAX_LENGTH} "
        f"AND {column} ~ '{REFERENCE_PATTERN}' "
        f"AND {column} !~ '{REFERENCE_DENY_PATTERN}')",
        name=f"{table}_{column}_ck",
    )


def upgrade() -> None:
    op.create_table(
        "provider_installations",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("provider", sa.String(), nullable=False),
        # Empty for a provider with one global service (Slack, Google,
        # Atlassian Cloud); the canonical hostname for a self-hosted or
        # per-host provider (a GHES instance, an Atlassian Data Center base
        # URL). NOT NULL with an empty default, not nullable, so two global
        # installations' authority values compare equal for uniqueness --
        # Postgres never treats two NULLs as equal.
        sa.Column("authority", sa.String(), server_default="", nullable=False),
        sa.Column("external_account_id", sa.String(), nullable=False),
        sa.Column("display_name", sa.String(), nullable=True),
        sa.Column("status", sa.String(), server_default="connected", nullable=False),
        sa.Column("installed_by_principal_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "installed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("disconnected_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(PROVIDER_CK, name="provider_installations_provider_ck"),
        sa.CheckConstraint(
            "status IN ('connected', 'disconnected')",
            name="provider_installations_status_ck",
        ),
        sa.CheckConstraint(
            "(status = 'disconnected') = (disconnected_at IS NOT NULL)",
            name="provider_installations_disconnected_at_ck",
        ),
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
            "authority",
            "external_account_id",
            name="provider_installations_tenant_provider_authority_account_key",
        ),
        # No ordinary row ever needs this on its own -- `id` is already the
        # primary key -- but a composite foreign key can only target a unique
        # constraint or index, and channel_identities.provider_installation_id
        # is declared alongside (tenant_id, provider) so that an identity can
        # attach only to an installation of its own tenant and provider.
        sa.UniqueConstraint(
            "tenant_id",
            "provider",
            "id",
            name="provider_installations_tenant_provider_id_key",
        ),
        schema=SCHEMA,
    )
    op.create_table(
        "channel_identities",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("provider", sa.String(), nullable=False),
        # Unique with (tenant_id, provider); what a binding's `adapter` names
        # (ADR 0168 decision 3). "default" is the one identity an install need
        # not name explicitly.
        sa.Column("name", sa.String(), server_default="default", nullable=False),
        sa.Column("credential_ref", sa.String(), nullable=True),
        sa.Column(
            "scopes",
            postgresql.JSONB(),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("webhook_verification_ref", sa.String(), nullable=True),
        # Provider-specific identity details that don't fit a fixed column --
        # for Slack, the app-token reference alongside `credential_ref`'s
        # bot-token reference.
        sa.Column(
            "attributes",
            postgresql.JSONB(),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("status", sa.String(), server_default="active", nullable=False),
        # Set when a later report (#3039's auth.test) names a different
        # installation than this identity is attached to. Cleared only by
        # fresh matching evidence or an operator reattaching it (ADR 0193
        # decision 4); it does not detach the identity on its own.
        sa.Column(
            "installation_mismatch",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
        sa.Column("provider_installation_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(PROVIDER_CK, name="channel_identities_provider_ck"),
        sa.CheckConstraint(
            "status IN ('active', 'disabled', 'revoked')",
            name="channel_identities_status_ck",
        ),
        _reference_check("channel_identities", "credential_ref"),
        _reference_check("channel_identities", "webhook_verification_ref"),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            [f"{SCHEMA}.tenants.id"],
            name="channel_identities_tenant_id_fkey",
        ),
        # An identity can attach only to an installation of its own tenant and
        # provider (ADR 0193 decision 4); ON DELETE is intentionally omitted
        # (the default NO ACTION), so deleting an installation with attached
        # identities is refused rather than silently detaching them.
        sa.ForeignKeyConstraint(
            ["tenant_id", "provider", "provider_installation_id"],
            [
                f"{SCHEMA}.provider_installations.tenant_id",
                f"{SCHEMA}.provider_installations.provider",
                f"{SCHEMA}.provider_installations.id",
            ],
            name="channel_identities_installation_fkey",
        ),
        sa.PrimaryKeyConstraint("id", name="channel_identities_pkey"),
        sa.UniqueConstraint(
            "tenant_id",
            "provider",
            "name",
            name="channel_identities_tenant_provider_name_key",
        ),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_table("channel_identities", schema=SCHEMA)
    op.drop_table("provider_installations", schema=SCHEMA)
