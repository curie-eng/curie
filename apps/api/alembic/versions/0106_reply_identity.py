"""Preserve the named Slack relay selector across approvals and publications (#3683).

Revision ID: 0106
Revises: 0093
Create Date: 2026-10-13
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0106"
down_revision: str | None = "0093"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    for table in ("approvals", "publications"):
        op.add_column(table, sa.Column("reply_identity", sa.String(), nullable=True), schema=SCHEMA)


def downgrade() -> None:
    for table in ("publications", "approvals"):
        op.drop_column(table, "reply_identity", schema=SCHEMA)
