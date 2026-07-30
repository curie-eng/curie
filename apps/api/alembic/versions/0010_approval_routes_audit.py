"""approval routes + audit log (#247, ADR-0010)

Adds agents.approval_routes (route-name -> workspace binding), the approval's
route/card_channel columns, and the append-only approval_audit_entries table.

Revision ID: 0010
Revises: 0009
Create Date: 2026-07-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.add_column(
        "agents",
        sa.Column("approval_routes", postgresql.JSONB(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column("approvals", sa.Column("route", sa.String(), nullable=True), schema=SCHEMA)
    op.add_column(
        "approvals",
        sa.Column("card_channel", sa.String(), nullable=True),
        schema=SCHEMA,
    )
    op.create_table(
        "approval_audit_entries",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("approval_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("action", sa.String(), nullable=False),
        sa.Column("actor", sa.String(), nullable=False),
        sa.Column("actor_channel", sa.String(), nullable=True),
        sa.Column("decision", sa.String(), nullable=False),
        sa.Column("authorizer", sa.String(), nullable=False),
        sa.Column("authorized", sa.Boolean(), nullable=False),
        sa.Column("reason", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["approval_id"], [f"{SCHEMA}.approvals.id"], ondelete="CASCADE"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_approval_audit_entries_approval_id",
        "approval_audit_entries",
        ["approval_id"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index("ix_approval_audit_entries_approval_id", "approval_audit_entries", schema=SCHEMA)
    op.drop_table("approval_audit_entries", schema=SCHEMA)
    op.drop_column("approvals", "card_channel", schema=SCHEMA)
    op.drop_column("approvals", "route", schema=SCHEMA)
    op.drop_column("agents", "approval_routes", schema=SCHEMA)
