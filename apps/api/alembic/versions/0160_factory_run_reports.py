"""Retain bounded factory execution diagnostics after transcript cleanup (#5601).

Revision ID: 0160
Revises: 0159
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0160"
down_revision: str = "0159"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "factory_run_reports",
        sa.Column("execution_request_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("work_item_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("report", postgresql.JSONB(), nullable=False),
        sa.Column(
            "captured_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.clock_timestamp(),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["execution_request_id"], ["curie.execution_requests.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["work_item_id"], ["curie.work_items.id"], ondelete="CASCADE"),
        schema="curie",
    )
    op.create_index(
        "ix_factory_run_reports_expires_at", "factory_run_reports", ["expires_at"], schema="curie"
    )
    op.create_index(
        "ix_factory_run_reports_work_item_id",
        "factory_run_reports",
        ["work_item_id"],
        schema="curie",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_factory_run_reports_work_item_id", table_name="factory_run_reports", schema="curie"
    )
    op.drop_index(
        "ix_factory_run_reports_expires_at", table_name="factory_run_reports", schema="curie"
    )
    op.drop_table("factory_run_reports", schema="curie")
