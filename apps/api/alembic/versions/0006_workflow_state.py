"""add workflow_state_entries (durable agent-scoped KV store, #23)

Revision ID: 0006
Revises: 0005
Create Date: 2026-07-10
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.create_table(
        "workflow_state_entries",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("namespace", sa.String(), nullable=False),
        sa.Column("key", sa.String(), nullable=False),
        sa.Column("value", postgresql.JSONB(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        # The composite unique constraint's index also serves lookups by
        # agent_id and by (agent_id, namespace) prefix, so no extra index needed.
        sa.UniqueConstraint("agent_id", "namespace", "key", name="uq_state_agent_ns_key"),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_table("workflow_state_entries", schema=SCHEMA)
