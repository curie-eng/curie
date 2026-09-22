"""Claim table for one agent trigger slot.

Revision ID: 0047
Revises: 0046
Create Date: 2026-09-22
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0047"
down_revision: str | None = "0046"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.create_table(
        "hook_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("slot_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("outcome", sa.String(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "outcome IS NULL OR outcome IN ('ran', 'skipped', 'blocked', 'failed')",
            name="hook_runs_outcome_ck",
        ),
        sa.ForeignKeyConstraint(
            ["agent_id"],
            [f"{SCHEMA}.agents.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["version_id"],
            [f"{SCHEMA}.agent_versions.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "agent_id",
            "name",
            "slot_utc",
            name="hook_runs_agent_name_slot_key",
        ),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_table("hook_runs", schema=SCHEMA)
