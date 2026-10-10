"""Record an execution request admitted as an owner_lost retry (#4168).

Revision ID: 0091
Revises: 0081
Create Date: 2026-10-07
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0091"
down_revision: str | None = "0081"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.add_column(
        "execution_requests",
        sa.Column(
            "owner_lost_retry",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_column("execution_requests", "owner_lost_retry", schema=SCHEMA)
