"""Persist canonical denied permission tool arguments (#3255).

Revision ID: 0059
Revises: 0058
Create Date: 2026-09-26
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0059"
down_revision: str | None = "0058"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "approvals"


def upgrade() -> None:
    op.add_column(
        TABLE,
        sa.Column("granted_arguments", postgresql.JSONB(), nullable=True),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_column(TABLE, "granted_arguments", schema=SCHEMA)
