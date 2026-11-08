"""Reserve the nullable approval reminder interval beside frozen deadlines.

Revision ID: 0152
Revises: 0151
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0152"
down_revision: str = "0151"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "approvals", sa.Column("remind_every_s", sa.Integer(), nullable=True), schema="curie"
    )
    op.create_check_constraint(
        "approvals_remind_every_s_positive_ck",
        "approvals",
        "remind_every_s IS NULL OR remind_every_s > 0",
        schema="curie",
    )


def downgrade() -> None:
    op.drop_constraint("approvals_remind_every_s_positive_ck", "approvals", schema="curie")
    op.drop_column("approvals", "remind_every_s", schema="curie")
