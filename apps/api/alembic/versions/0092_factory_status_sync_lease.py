"""Lease factory status sync without holding locks across GitHub calls (#4167).

Revision ID: 0092
Revises: 0091
Create Date: 2026-10-07
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0092"
down_revision: str | None = "0091"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.add_column(
        "factory_terminal_notices",
        sa.Column("sync_owner", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "factory_terminal_notices",
        sa.Column("sync_lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "factory_terminal_notices",
        sa.Column("sync_invalidated", sa.Boolean(), nullable=False, server_default=sa.false()),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_column("factory_terminal_notices", "sync_invalidated", schema=SCHEMA)
    op.drop_column("factory_terminal_notices", "sync_lease_expires_at", schema=SCHEMA)
    op.drop_column("factory_terminal_notices", "sync_owner", schema=SCHEMA)
