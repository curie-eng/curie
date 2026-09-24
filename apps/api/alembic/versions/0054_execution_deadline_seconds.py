"""Per-agent execution deadline up to 10800 s (#3071).

Adds nullable ``agents.execution_deadline_seconds`` (NULL means the 1800 s
default, otherwise 60..10800) and relaxes ``execution_requests_deadline_ck``
from exactly 1800 s to any deadline after ``started_at`` and at most 10800 s
after it. Downgrade restores the exact-1800 check and drops the column; it
fails if a stored request carries a deadline other than 1800 s.

Revision ID: 0054
Revises: 0053
Create Date: 2026-09-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0054"
down_revision: str | None = "0053"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
DEADLINE_CK = "execution_requests_deadline_ck"
AGENT_CK = "agents_execution_deadline_seconds_ck"


def upgrade() -> None:
    op.add_column(
        "agents",
        sa.Column("execution_deadline_seconds", sa.Integer(), nullable=True),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        AGENT_CK,
        "agents",
        "execution_deadline_seconds IS NULL OR "
        "execution_deadline_seconds BETWEEN 60 AND 10800",
        schema=SCHEMA,
    )
    op.drop_constraint(DEADLINE_CK, "execution_requests", schema=SCHEMA, type_="check")
    op.create_check_constraint(
        DEADLINE_CK,
        "execution_requests",
        "(started_at IS NULL AND execution_deadline IS NULL) OR "
        "(started_at IS NOT NULL AND execution_deadline IS NOT NULL AND "
        "execution_deadline > started_at AND "
        "execution_deadline <= started_at + interval '10800 seconds')",
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_constraint(DEADLINE_CK, "execution_requests", schema=SCHEMA, type_="check")
    op.create_check_constraint(
        DEADLINE_CK,
        "execution_requests",
        "(started_at IS NULL AND execution_deadline IS NULL) OR "
        "(started_at IS NOT NULL AND execution_deadline IS NOT NULL AND "
        "execution_deadline = started_at + interval '1800 seconds')",
        schema=SCHEMA,
    )
    op.drop_constraint(AGENT_CK, "agents", schema=SCHEMA, type_="check")
    op.drop_column("agents", "execution_deadline_seconds", schema=SCHEMA)
