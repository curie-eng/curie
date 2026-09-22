"""Tenant entity: issue #2906's first, no-behavior-change slice.

A ``tenants`` table is the platform's first tenant concept -- no
``tenant_id`` column exists anywhere else in the schema yet.
``Tenant.deployment_id`` is an opaque string identifier for the physical
self-host appliance; it is deliberately NOT a foreign key to the existing
``deployments`` table. ``Deployment`` is an unrelated concept -- the
dev/prod binding of an ``AgentVersion`` -- that predates tenants entirely.

The data step below auto-provisions the self-host default tenant: exactly
one active row, so a self-host deployment has a tenant to attach future
tenant-scoped data to without any application boot-time provisioning logic.
The row's own id is a fixed, well-known constant (see ``DEFAULT_TENANT_ID``
below), not generated, so it is stable across every fresh ``upgrade head``.
``gen_random_uuid()`` names the appliance (``deployment_id``) the same way
0021 minted ids for its data step -- a Postgres 13+ builtin, so that id
doesn't have to round-trip through Python; a random per-appliance identifier
is the correct semantic there, unlike the tenant's own id.

Revision ID: 0046
Revises: 0045
Create Date: 2026-09-21
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0046"
down_revision: str | None = "0045"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "tenants"

# Stable, well-known id of the self-host default tenant this migration's data
# step provisions. Fixed (not gen_random_uuid()) so the row's id is the same
# across every fresh `upgrade head`, letting a future migration point at "the"
# default tenant without a runtime lookup. Deliberately not the all-zeros nil
# UUID (...0000), since that's commonly reserved as an "unset" sentinel
# elsewhere; ...0001 reads unambiguously as "the first/only well-known row".
DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000001"


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("deployment_id", sa.String(), nullable=False),
        sa.Column("idp_config_ref", sa.String(), nullable=True),
        sa.Column("retention_policy_ref", sa.String(), nullable=True),
        sa.Column("default_provider_policy_ref", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        schema=SCHEMA,
    )

    # Self-host auto-provisioning: exactly one active default tenant, so a
    # freshly migrated deployment already has a tenant to attach
    # tenant-scoped data to. `deployment_id` here names the physical
    # self-host appliance, not a row in `deployments` (see module docstring).
    op.execute(
        sa.text(
            f"""
            INSERT INTO {SCHEMA}.{TABLE} (id, deployment_id, status)
            VALUES ('{DEFAULT_TENANT_ID}', gen_random_uuid()::text, 'active')
            """
        )
    )


def downgrade() -> None:
    op.drop_table(TABLE, schema=SCHEMA)
