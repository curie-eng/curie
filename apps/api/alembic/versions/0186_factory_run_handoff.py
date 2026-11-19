"""Store the optional bounded run handoff on the existing status row (#5771).

Revision ID: 0186
Revises: 0185
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "0186"
down_revision: str = "0185"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "factory_terminal_notices",
        sa.Column("run_handoff", JSONB(), nullable=True),
        schema="curie",
    )


def downgrade() -> None:
    op.drop_column("factory_terminal_notices", "run_handoff", schema="curie")
