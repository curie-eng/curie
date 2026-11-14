"""Outbox for refused trigger-label Slack posts.

Revision ID: 0170
Revises: 0169

A refusal has no WorkItem, so it cannot use factory_slack_posts. One row per
intake notice, with the same lease and acknowledgement columns as that outbox.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0170"
down_revision: str = "0169"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

S = "curie"


def upgrade() -> None:
    op.create_table(
        "factory_slack_refusal_posts",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column(
            "notice_id",
            sa.BigInteger(),
            sa.ForeignKey(f"{S}.factory_intake_notices.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("channel_identity", sa.Text(), nullable=False),
        sa.Column("channel", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("not_before", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_owner", sa.Text(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("posted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("posted_ref", sa.Text(), nullable=True),
        sa.Column("failed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("notice_id", name="factory_slack_refusal_posts_notice_key"),
        schema=S,
    )
    op.create_index(
        "ix_factory_slack_refusal_posts_owed",
        "factory_slack_refusal_posts",
        ["created_at", "id"],
        schema=S,
        postgresql_where=sa.text("posted_at IS NULL AND failed_at IS NULL"),
    )


def downgrade() -> None:
    count = op.get_bind().scalar(sa.text("SELECT count(*) FROM curie.factory_slack_refusal_posts"))
    if count is None or count > 0:
        raise RuntimeError("cannot downgrade while factory Slack refusal posts exist")
    op.drop_index(
        "ix_factory_slack_refusal_posts_owed",
        table_name="factory_slack_refusal_posts",
        schema=S,
    )
    op.drop_table("factory_slack_refusal_posts", schema=S)
