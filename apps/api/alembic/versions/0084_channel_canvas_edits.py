"""Channel canvas edit audit (ADR 0200, #3819)

Revision ID: 0084
Revises: 0083
Create Date: 2026-10-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0084"
down_revision: str | None = "0083"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "channel_canvas_edits",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "agent_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("curie.agents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("deployment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("turn", sa.Text(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("channel_address", sa.Text(), nullable=False),
        sa.Column("canvas_id", sa.Text(), nullable=False),
        sa.Column("section_id", sa.Text(), nullable=False),
        sa.Column("before_text", sa.Text(), nullable=False),
        sa.Column("after_text", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("error_code", sa.Text(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('attempted', 'applied', 'failed')",
            name="channel_canvas_edits_status_ck",
        ),
        schema="curie",
    )
    op.create_index(
        "ix_channel_canvas_edits_agent_id", "channel_canvas_edits", ["agent_id"], schema="curie"
    )
    op.create_index(
        "ix_channel_canvas_edits_canvas_created",
        "channel_canvas_edits",
        ["canvas_id", "created_at"],
        schema="curie",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_channel_canvas_edits_canvas_created", table_name="channel_canvas_edits", schema="curie"
    )
    op.drop_index(
        "ix_channel_canvas_edits_agent_id", table_name="channel_canvas_edits", schema="curie"
    )
    op.drop_table("channel_canvas_edits", schema="curie")
