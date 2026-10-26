"""Daily scheduled spend and consecutive hook failure counts.

Revision ID: selfimp_safety_0001
Revises: 0131
Create Date: 2026-10-20
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "selfimp_safety_0001"
down_revision: str | None = "0131"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.create_table(
        "schedule_daily_spend",
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("utc_day", sa.Date(), nullable=False),
        sa.Column("cost_usd", sa.Numeric(14, 6), nullable=False),
        sa.CheckConstraint("cost_usd >= 0", name="schedule_daily_spend_cost_ck"),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("agent_id", "utc_day"),
        schema=SCHEMA,
    )
    op.create_table(
        "schedule_failure_streaks",
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("consecutive", sa.Integer(), nullable=False),
        sa.CheckConstraint("consecutive >= 0", name="schedule_failure_streaks_count_ck"),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("agent_id", "name"),
        schema=SCHEMA,
    )
    op.create_table(
        "schedule_pause_notices",
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("agent_id", "name", "generation"),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_table("schedule_pause_notices", schema=SCHEMA)
    op.drop_table("schedule_failure_streaks", schema=SCHEMA)
    op.drop_table("schedule_daily_spend", schema=SCHEMA)
