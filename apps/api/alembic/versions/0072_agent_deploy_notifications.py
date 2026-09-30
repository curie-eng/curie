"""opt-in successful git-flow deploy notices and durable notice outbox

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
    op.create_table(
        "deploy_notice_outbox",
        sa.Column("key", sa.String(length=64), primary_key=True),
        sa.Column("stream", sa.Text(), nullable=False),
        sa.Column("payload", sa.Text(), nullable=False),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("enqueued_at", sa.DateTime(timezone=True), nullable=True),
        schema="curie",
    )
    op.create_index(
        "ix_deploy_notice_outbox_pending",
        "deploy_notice_outbox",
        ["stream", "created_at"],
        schema="curie",
        postgresql_where=sa.text("enqueued_at IS NULL"),
    )
    op.create_index(
        "ix_deploy_notice_outbox_enqueued_at",
        "deploy_notice_outbox",
        ["enqueued_at"],
        schema="curie",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_deploy_notice_outbox_enqueued_at", table_name="deploy_notice_outbox", schema="curie"
    )
    op.drop_index(
        "ix_deploy_notice_outbox_pending", table_name="deploy_notice_outbox", schema="curie"
    )
    op.drop_table("deploy_notice_outbox", schema="curie")
    op.drop_column("agents", "deploy_notifications", schema="curie")
