"""Index the nullable reminder due cursor.

Revision ID: 0154
Revises: 0153
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0154"
down_revision: str = "0153"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "approvals", sa.Column("next_remind_at", sa.DateTime(), nullable=True), schema="curie"
    )
    op.create_index(
        "ix_approvals_next_remind_at",
        "approvals",
        ["next_remind_at", "id"],
        schema="curie",
        postgresql_where=sa.text("next_remind_at IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_approvals_next_remind_at", table_name="approvals", schema="curie")
    op.drop_column("approvals", "next_remind_at", schema="curie")
