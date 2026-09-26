"""Persist operator pause state for one agent and named cron hook (#2937).

Revision ID: 0061
Revises: 0060
Create Date: 2026-09-26
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0061"
down_revision: str | None = "0060"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.create_table(
        "schedule_controls",
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("paused_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resume_from", sa.DateTime(timezone=True), nullable=True),
        sa.Column("generation", sa.BigInteger(), nullable=False, server_default="0"),
        sa.ForeignKeyConstraint(
            ["agent_id"],
            [f"{SCHEMA}.agents.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("agent_id", "name"),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_table("schedule_controls", schema=SCHEMA)
