"""Bind an answer receipt to tracker content without storing the answer.

Retained receipts have a NULL fingerprint. There is no way to reconstruct
the exact admitted text from a live tracker read, so those receipts convey
no authority. Older writers can still insert receipts during an upgrade.

Revision ID: 0189
Revises: 0188
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0189"
down_revision: str = "0188"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(sa.text("SET LOCAL lock_timeout = '15000ms'"))
    op.add_column(
        "factory_answer_receipts", sa.Column("answer_fingerprint", sa.Text()), schema="curie"
    )


def downgrade() -> None:
    op.execute(sa.text("SET LOCAL lock_timeout = '15000ms'"))
    op.drop_column("factory_answer_receipts", "answer_fingerprint", schema="curie")
