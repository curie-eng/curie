"""Nullable reason on a terminal hook run (#4007).

Adds nullable ``hook_runs.reason``. ``ran``, in-flight rows, and rows written
before this revision stay NULL. There is no backfill and no check constraint:
a row closed before the column existed has an outcome and no reason.

Revision ID: 0079
Revises: 0076
Create Date: 2026-10-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0079"
down_revision: str | None = "0076"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.add_column(
        "hook_runs",
        sa.Column("reason", sa.Text(), nullable=True),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_column("hook_runs", "reason", schema=SCHEMA)
