"""Store each agent's invocation and approver teams (#4585, ADR 0220 section 8).

``agent_access_policies`` holds at most one row per agent. Both team
references are tenant-scoped composite foreign keys, so a policy can only name
a team of the agent's own tenant, and a team a policy names cannot be deleted
from under it (NO ACTION). Deleting the agent deletes its policy. ``version``
is the token a write must name, so concurrent operators conflict instead of
overwriting each other.

Expand-only: one new, empty table. No agent has a policy afterwards, which is
today's behavior.

Revision ID: 0121
Revises: 0120
Create Date: 2026-10-14
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0121"
down_revision: str | None = "0120"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.create_table(
        "agent_access_policies",
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("invocation_team_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("approver_team_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("version >= 1", name="agent_access_policies_version_ck"),
        sa.ForeignKeyConstraint(
            ["tenant_id", "agent_id"],
            [f"{SCHEMA}.agents.tenant_id", f"{SCHEMA}.agents.id"],
            ondelete="CASCADE",
            name="agent_access_policies_agent_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "invocation_team_id"],
            [f"{SCHEMA}.teams.tenant_id", f"{SCHEMA}.teams.id"],
            name="agent_access_policies_invocation_team_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "approver_team_id"],
            [f"{SCHEMA}.teams.tenant_id", f"{SCHEMA}.teams.id"],
            name="agent_access_policies_approver_team_fkey",
        ),
        sa.PrimaryKeyConstraint("agent_id", name="agent_access_policies_pkey"),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_table("agent_access_policies", schema=SCHEMA)
