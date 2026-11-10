"""Record an execution request's previous start failure (#4930).

When a factory request's sandbox never starts because workspace preparation
failed, the worker sends the safe stage and reason with its deferral. This
column keeps that diagnostic while the request waits, so later capacity
deferrals and a worker replacement do not erase why the last start failed. A
successful start clears it.

Expand-only: one nullable text column and a check that a stored value is
nonblank and at most 2000 characters. Nothing is backfilled, so every existing
request reads NULL. The number is 0123 rather than a suffixed id because
``main`` and ``next`` share head 0122, and the number right after a shared head
belongs to the next stable fix (the 0120 precedent).

Revision ID: 0123
Revises: 0122
Create Date: 2026-10-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0123"
down_revision: str | None = "0122"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "execution_requests"
COLUMN = "last_start_failure_detail"
CONSTRAINT = "execution_requests_last_start_failure_detail_ck"
# The same bound and nonblank rule as models.LAST_START_FAILURE_DETAIL_CHECK.
CHECK = (
    "last_start_failure_detail IS NULL OR "
    "(last_start_failure_detail ~ '[^[:space:]]' "
    "AND length(last_start_failure_detail) <= 2000)"
)


def upgrade() -> None:
    op.add_column(TABLE, sa.Column(COLUMN, sa.Text(), nullable=True), schema=SCHEMA)
    op.create_check_constraint(CONSTRAINT, TABLE, CHECK, schema=SCHEMA)


def downgrade() -> None:
    op.drop_constraint(CONSTRAINT, TABLE, schema=SCHEMA, type_="check")
    op.drop_column(TABLE, COLUMN, schema=SCHEMA)
