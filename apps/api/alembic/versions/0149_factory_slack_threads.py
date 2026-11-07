"""Factory Slack run threads and their owed progress posts (#5414).

``agents.factory_slack_channel`` names the one Slack channel an agent's factory
runs post progress to, and ``agents.factory_slack_since`` records when it was
last set, so only runs live from then on are posted. ``factory_slack_threads``
binds one WorkItem to one Slack root for its whole life, and
``factory_slack_posts`` is the durable outbox of messages owed to that root.

Revision ID: 0149
Revises: 0148
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0149"
down_revision: str = "0148"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
S = "curie"

POST_KINDS = ("root", "phase", "pull_request", "terminal")


def upgrade() -> None:
    op.add_column("agents", sa.Column("factory_slack_channel", sa.Text(), nullable=True), schema=S)
    op.add_column(
        "agents",
        sa.Column("factory_slack_since", sa.DateTime(timezone=True), nullable=True),
        schema=S,
    )
    op.create_check_constraint(
        "agents_factory_slack_channel_ck",
        "agents",
        "factory_slack_channel IS NULL OR factory_slack_channel ~ '^[CG][A-Z0-9]{6,}$'",
        schema=S,
    )

    op.create_table(
        "factory_slack_threads",
        sa.Column(
            "work_item_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{S}.work_items.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("channel_identity", sa.Text(), nullable=False),
        sa.Column("channel", sa.Text(), nullable=False),
        sa.Column("root_ts", sa.Text(), nullable=True),
        sa.Column("first_sequence", sa.Integer(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint(
            "channel_identity", "channel", "root_ts", name="factory_slack_threads_root_key"
        ),
        schema=S,
    )
    op.create_table(
        "factory_slack_posts",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column(
            "work_item_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{S}.factory_slack_threads.work_item_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("post_key", sa.Text(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column("ordinal", sa.BigInteger(), nullable=False),
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
        sa.CheckConstraint(
            f"kind IN ({', '.join(repr(kind) for kind in POST_KINDS)})",
            name="factory_slack_posts_kind_ck",
        ),
        sa.UniqueConstraint("work_item_id", "post_key", name="factory_slack_posts_post_key_key"),
        schema=S,
    )
    op.create_index(
        "ix_factory_slack_posts_owed",
        "factory_slack_posts",
        ["work_item_id", "sequence", "rank", "ordinal"],
        schema=S,
        postgresql_where=sa.text("posted_at IS NULL AND failed_at IS NULL"),
    )


def downgrade() -> None:
    # Refused while progress state or a destination exists: dropping it would
    # lose owed posts and the roots a re-upgrade needs to resume each thread.
    if op.get_bind().scalar(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM curie.factory_slack_threads)"
            " OR EXISTS (SELECT 1 FROM curie.factory_slack_posts)"
            " OR EXISTS (SELECT 1 FROM curie.agents WHERE factory_slack_channel IS NOT NULL)"
        )
    ):
        raise RuntimeError("factory_slack_progress_state_present")
    op.drop_index("ix_factory_slack_posts_owed", table_name="factory_slack_posts", schema=S)
    op.drop_table("factory_slack_posts", schema=S)
    op.drop_table("factory_slack_threads", schema=S)
    op.drop_constraint("agents_factory_slack_channel_ck", "agents", schema=S)
    op.drop_column("agents", "factory_slack_since", schema=S)
    op.drop_column("agents", "factory_slack_channel", schema=S)
