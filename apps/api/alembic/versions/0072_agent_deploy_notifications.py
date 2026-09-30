"""opt-in successful git-flow deploy notices per agent

Revision ID: 0072
Revises: 0071
Create Date: 2026-09-29
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0072"
down_revision: str | None = "0071"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "agents",
        sa.Column("deploy_notifications", sa.Boolean(), nullable=False, server_default=sa.false()),
        schema="curie",
    )


def downgrade() -> None:
    op.drop_column("agents", "deploy_notifications", schema="curie")
