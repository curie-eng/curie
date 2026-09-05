"""Persist GitHub review feedback and its runs-stream outbox.

Revision ID: 0043
Revises: 0042
Create Date: 2026-09-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0043"
down_revision: str | None = "0042"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "github_review_feedback",
        sa.Column("event_id", sa.String(), primary_key=True),
        sa.Column("delivery_id", postgresql.UUID(as_uuid=True), nullable=False, unique=True),
        sa.Column("lineage_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("binding_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("binding_generation", sa.Integer(), nullable=False),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("lineage_version", sa.Integer(), nullable=False),
        sa.Column("reservation_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("feedback", postgresql.JSONB(), nullable=False),
        sa.Column("turn", postgresql.JSONB(), nullable=False),
        sa.Column("traceparent", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=False, server_default="waiting"),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("enqueue_attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("quota_taken", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("next_attempt_at", sa.DateTime(), nullable=True),
        sa.Column("error_code", sa.String(), nullable=True),
        sa.Column("stream_id", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("queued_at", sa.DateTime(), nullable=True),
        sa.Column("terminal_scan_cursor", sa.String(), nullable=True),
        sa.CheckConstraint(
            "status IN ('waiting', 'queued', 'reserved', 'settled', 'refused', 'dead_lettered')",
            name="github_review_feedback_status_ck",
        ),
        sa.CheckConstraint("version >= 1", name="github_review_feedback_version_ck"),
        sa.CheckConstraint("enqueue_attempts >= 0", name="github_review_feedback_attempts_ck"),
        sa.ForeignKeyConstraint(
            ["lineage_id"],
            ["curie.thread_publication_lineages.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(["binding_id"], ["curie.agent_channels.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["reservation_id"], ["curie.publication_review_reservations.id"], ondelete="SET NULL"
        ),
        schema="curie",
    )
    op.create_index(
        "ix_github_review_feedback_pending",
        "github_review_feedback",
        ["status", "created_at"],
        schema="curie",
    )

    op.create_table(
        "github_review_deliveries",
        sa.Column("delivery_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_kind", sa.String(), nullable=False),
        sa.Column("action", sa.String(), nullable=False),
        sa.Column("body_sha256", sa.String(), nullable=False),
        sa.Column("repository_id", sa.BigInteger(), nullable=True),
        sa.Column("installation_id", sa.BigInteger(), nullable=True),
        sa.Column("pr_number", sa.Integer(), nullable=True),
        sa.Column("source_object_id", sa.BigInteger(), nullable=True),
        sa.Column("sender_id", sa.BigInteger(), nullable=True),
        sa.Column("sender_type", sa.String(), nullable=False),
        sa.Column("sender_login", sa.String(), nullable=True),
        sa.Column("author_association", sa.String(), nullable=False),
        sa.Column(
            "event_id",
            sa.String(),
            sa.ForeignKey("curie.github_review_feedback.event_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("status", sa.String(), nullable=False, server_default="pending"),
        sa.Column("reason", sa.String(), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("replay_conflicts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "status IN ('pending','accepted','ignored','rejected','retryable')",
            name="github_review_deliveries_status_ck",
        ),
        sa.CheckConstraint(
            "version >= 1 AND replay_conflicts >= 0", name="github_review_deliveries_version_ck"
        ),
        sa.CheckConstraint("length(body_sha256) = 64", name="github_review_deliveries_digest_ck"),
        sa.CheckConstraint(
            "status IN ('pending','accepted') OR reason IS NOT NULL",
            name="github_review_deliveries_reason_ck",
        ),
        schema="curie",
    )


def downgrade() -> None:
    active = (
        op.get_bind()
        .execute(
            sa.text(
                "SELECT EXISTS (SELECT 1 FROM curie.github_review_feedback "
                "WHERE status IN ('waiting','queued','reserved')) "
                "OR EXISTS (SELECT 1 FROM curie.github_review_deliveries "
                "WHERE status IN ('pending','retryable'))"
            )
        )
        .scalar_one()
    )
    if active:
        raise RuntimeError("cannot remove GitHub review audit while deliveries are active")
    op.drop_table("github_review_deliveries", schema="curie")
    op.drop_table("github_review_feedback", schema="curie")
