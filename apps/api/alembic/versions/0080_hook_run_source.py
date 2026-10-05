"""Separate scheduled and manual hook run history (#4010).

Scheduled slots are whole minutes. Existing rows with any seconds or
microseconds therefore came from a manual fire. The default keeps scheduled
rows from older workers correct during a rolling upgrade.

Revision ID: 0080
Revises: 0079
Create Date: 2026-10-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0080"
down_revision: str | None = "0079"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.add_column(
        "hook_runs",
        sa.Column("source", sa.Text(), nullable=False, server_default=sa.text("'schedule'")),
        schema=SCHEMA,
    )
    op.execute(
        f"UPDATE {SCHEMA}.hook_runs SET source = 'manual' "
        "WHERE date_trunc('minute', slot_utc) <> slot_utc"
    )
    op.create_check_constraint(
        "hook_runs_source_ck",
        "hook_runs",
        "source IN ('schedule', 'manual')",
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_constraint("hook_runs_source_ck", "hook_runs", schema=SCHEMA, type_="check")
    op.drop_column("hook_runs", "source", schema=SCHEMA)
