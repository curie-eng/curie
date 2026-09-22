"""Tenant entity: first no-behavior-change slice (#2906).

Adds a ``tenants`` table and auto-provisions exactly one default tenant at a
FIXED, well-known id (``DEFAULT_TENANT_ID``) so a later migration can
reference it without a runtime lookup. ``deployment_id`` is an opaque
per-appliance identifier -- NOT a foreign key to the existing
``Deployment``/``deployments`` table, which is the unrelated dev/prod binding
of an AgentVersion. Each self-host install gets its own random
``deployment_id``, generated at migration time; only the tenant's own ``id``
is fixed so it is identical across every fresh upgrade/downgrade/upgrade
cycle.

Revision ID: 0050
Revises: 0049
Create Date: 2026-09-22
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0050"
down_revision: str | None = "0049"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"

# Fixed, well-known id for the single auto-provisioned tenant. Not
# gen_random_uuid() -- later migrations reference this id directly, and it
# must be identical across every fresh upgrade/downgrade/upgrade cycle.
DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000001"


def upgrade() -> None:
    conn = op.get_bind()

    op.create_table(
        "tenants",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("deployment_id", sa.String(), nullable=False),
        sa.Column("idp_config_ref", sa.String(), nullable=True),
        sa.Column("retention_policy_ref", sa.String(), nullable=True),
        sa.Column("default_provider_policy_ref", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        schema=SCHEMA,
    )

    conn.execute(
        sa.text(
            f"""
            INSERT INTO {SCHEMA}.tenants (id, deployment_id, status)
            VALUES (:tenant_id, gen_random_uuid()::text, 'active')
            """
        ),
        {"tenant_id": DEFAULT_TENANT_ID},
    )


def downgrade() -> None:
    op.drop_table("tenants", schema=SCHEMA)
