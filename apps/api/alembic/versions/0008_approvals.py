"""add approvals (durable human-approval records, #244 / ADR-0010)

Revision ID: 0008
Revises: 0007
Create Date: 2026-07-14
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.create_table(
        "approvals",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("conversation_id", sa.String(), nullable=False),
        sa.Column("author", sa.String(), nullable=False),
        sa.Column("summary", sa.String(), nullable=False),
        sa.Column("reply_channel", sa.String(), nullable=False),
        sa.Column("reply_placeholder", sa.String(), nullable=False),
        sa.Column("reply_endpoint", sa.String(), nullable=True),
        sa.Column("dedupe_key", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False, server_default="pending"),
        sa.Column("expires_at", sa.DateTime(), nullable=True),
        sa.Column("resolved_by", sa.String(), nullable=True),
        sa.Column("resolution_note", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("resolved_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("dedupe_key", name="uq_approvals_dedupe_key"),
        schema=SCHEMA,
    )
    op.create_index("ix_approvals_agent_id", "approvals", ["agent_id"], schema=SCHEMA)
    op.create_index(
        "ix_approvals_conversation_id",
        "approvals",
        ["conversation_id"],
        schema=SCHEMA,
    )
    op.create_index("ix_approvals_status", "approvals", ["status"], schema=SCHEMA)


def downgrade() -> None:
    op.drop_index("ix_approvals_status", "approvals", schema=SCHEMA)
    op.drop_index("ix_approvals_conversation_id", "approvals", schema=SCHEMA)
    op.drop_index("ix_approvals_agent_id", "approvals", schema=SCHEMA)
    op.drop_table("approvals", schema=SCHEMA)
