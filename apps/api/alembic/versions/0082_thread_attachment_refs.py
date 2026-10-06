"""The thread attachment ledger (ADR 0205, #4079)

Revision ID: 0082
Revises: 0081
Create Date: 2026-10-06

Hand-written and additive. ``thread_attachment_refs`` holds one row per file a
thread's agent was given, keyed like ``thread_transcripts`` (agent, binding
scope, thread key) and removed with it by the API. ``seq`` is the arrival
order. (agent, scope, thread, event, file) is unique so a redelivered append
records nothing twice, and (agent, scope, thread, disk_name) is unique so a
recorded on-disk name is never reused; both treat a NULL scope as equal. The
agent foreign key cascades.

The downgrade drops the table. Its rows are derived thread state: losing them
makes later boots of a thread miss earlier files, as before this revision.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0082"
down_revision: str | None = "0081"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "thread_attachment_refs"


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("seq", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("binding_scope", sa.Text(), nullable=True),
        sa.Column("thread_key", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("file_id", sa.Text(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("disk_name", sa.Text(), nullable=False),
        sa.Column("mime_type", sa.Text(), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("sha256", sa.Text(), nullable=False),
        sa.Column("route_kind", sa.Text(), nullable=False),
        sa.Column("route_adapter", sa.Text(), nullable=True),
        sa.Column("route_identity", sa.Text(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        sa.UniqueConstraint(
            "agent_id",
            "binding_scope",
            "thread_key",
            "event_id",
            "file_id",
            name="uq_thread_attachment_refs_event_file",
            postgresql_nulls_not_distinct=True,
        ),
        sa.UniqueConstraint(
            "agent_id",
            "binding_scope",
            "thread_key",
            "disk_name",
            name="uq_thread_attachment_refs_disk_name",
            postgresql_nulls_not_distinct=True,
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_thread_attachment_refs_thread_seq",
        TABLE,
        ["agent_id", "binding_scope", "thread_key", "seq"],
        schema=SCHEMA,
    )
    op.create_index("ix_thread_attachment_refs_expires_at", TABLE, ["expires_at"], schema=SCHEMA)
    # The orphan sweep runs on every transcript write, scoped to one agent.
    op.create_index(
        "ix_thread_attachment_refs_agent_expires_at",
        TABLE,
        ["agent_id", "expires_at"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index("ix_thread_attachment_refs_agent_expires_at", table_name=TABLE, schema=SCHEMA)
    op.drop_index("ix_thread_attachment_refs_expires_at", table_name=TABLE, schema=SCHEMA)
    op.drop_index("ix_thread_attachment_refs_thread_seq", table_name=TABLE, schema=SCHEMA)
    op.drop_table(TABLE, schema=SCHEMA)
