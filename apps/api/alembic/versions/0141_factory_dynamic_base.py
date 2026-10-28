"""Pin factory bases per request after the label settling window.

Revision ID: 0141
Revises: 0139
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0141"
down_revision: str = "0139"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
S = "curie"


def upgrade() -> None:
    op.add_column(
        "work_items",
        sa.Column("base_resolved_request_id", postgresql.UUID(as_uuid=True), nullable=True),
        schema=S,
    )
    op.add_column(
        "work_items", sa.Column("base_selection_fingerprint", sa.Text(), nullable=True), schema=S
    )
    op.add_column(
        "work_items",
        sa.Column("readmit_base_settle_until", sa.DateTime(timezone=True), nullable=True),
        schema=S,
    )
    op.add_column(
        "work_items", sa.Column("base_requested_branch", sa.Text(), nullable=True), schema=S
    )
    op.add_column(
        "work_items", sa.Column("base_requested_source", sa.Text(), nullable=True), schema=S
    )
    op.add_column(
        "execution_requests",
        sa.Column("base_settle_until", sa.DateTime(timezone=True), nullable=True),
        schema=S,
    )


def downgrade() -> None:
    op.drop_column("execution_requests", "base_settle_until", schema=S)
    for name in (
        "base_resolved_request_id",
        "base_selection_fingerprint",
        "readmit_base_settle_until",
        "base_requested_branch",
        "base_requested_source",
    ):
        op.drop_column("work_items", name, schema=S)
