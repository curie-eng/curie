"""Move conversation transcripts into their own per-thread table (ADR-0170, #3070)

The reserved ``transcript`` namespace of ``workflow_state_entries`` held every
thread's transcript under one per-(agent, namespace) byte cap, so a factory
agent stopped for good after enough issues. Upgrade creates
``thread_transcripts``, copies every transcript row into it, and deletes the
copied rows from the state store. No operator step is needed.

Downgrade copies the rows back and drops the table. A row larger than the old
state caps is still copied back; the state store then refuses further appends
to it until it is compacted or deleted, which is the pre-0052 behavior.

Revision ID: 0052
Revises: 0051
Create Date: 2026-09-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0052"
down_revision: str | None = "0051"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.create_table(
        "thread_transcripts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("binding_scope", sa.String(), nullable=True),
        sa.Column("thread_key", sa.String(), nullable=False),
        sa.Column("value", postgresql.JSONB(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        sa.UniqueConstraint(
            "agent_id",
            "binding_scope",
            "thread_key",
            name="uq_thread_transcripts_agent_scope_thread",
            postgresql_nulls_not_distinct=True,
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_thread_transcripts_expires_at",
        "thread_transcripts",
        ["expires_at"],
        schema=SCHEMA,
    )
    op.execute(
        f"""
        INSERT INTO {SCHEMA}.thread_transcripts
            (id, agent_id, binding_scope, thread_key, value, version, created_at, updated_at)
        SELECT id, agent_id, binding_scope, key, value, version, created_at, updated_at
        FROM {SCHEMA}.workflow_state_entries
        WHERE namespace = 'transcript'
        """
    )
    op.execute(f"DELETE FROM {SCHEMA}.workflow_state_entries WHERE namespace = 'transcript'")


def downgrade() -> None:
    op.execute(
        f"""
        INSERT INTO {SCHEMA}.workflow_state_entries
            (id, agent_id, binding_scope, namespace, key, value, version, created_at, updated_at)
        SELECT id, agent_id, binding_scope, 'transcript', thread_key, value, version,
               created_at, updated_at
        FROM {SCHEMA}.thread_transcripts
        """
    )
    op.drop_index(
        "ix_thread_transcripts_expires_at", table_name="thread_transcripts", schema=SCHEMA
    )
    op.drop_table("thread_transcripts", schema=SCHEMA)
