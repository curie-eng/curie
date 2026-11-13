"""Retain ordered factory transcript events independently of replay history (#5608).

Revision ID: 0161
Revises: 0160
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0161"
down_revision: str = "0160"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "execution_requests",
        sa.Column("transcript_expires_at", sa.DateTime(timezone=True), nullable=True),
        schema="curie",
    )
    op.create_table(
        "factory_transcript_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column(
            "execution_request_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("curie.execution_requests.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("boot_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("content", postgresql.JSONB(), nullable=False),
        sa.Column("redacted", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.clock_timestamp(),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "execution_request_id", "event_id", name="uq_factory_transcript_event_id"
        ),
        sa.UniqueConstraint(
            "execution_request_id", "boot_id", "sequence", name="uq_factory_transcript_sequence"
        ),
        schema="curie",
    )
    op.create_index(
        "ix_factory_transcript_request_cursor",
        "factory_transcript_events",
        ["execution_request_id", "id"],
        schema="curie",
    )
    op.create_index(
        "ix_factory_transcript_expires_at",
        "factory_transcript_events",
        ["expires_at"],
        schema="curie",
    )


def downgrade() -> None:
    op.drop_index("ix_factory_transcript_expires_at", table_name="factory_transcript_events", schema="curie")
    op.drop_index(
        "ix_factory_transcript_request_cursor", table_name="factory_transcript_events", schema="curie"
    )
    op.drop_table("factory_transcript_events", schema="curie")
    op.drop_column("execution_requests", "transcript_expires_at", schema="curie")
