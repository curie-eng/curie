"""Add the metadata-only attachment reference catalog (ADR 0244).

Revision ID: 0130
Revises: 0122
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0130"
down_revision: str | None = "0122"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "attachment_references"


def upgrade() -> None:
    op.create_table(
        "attachment_catalogs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("thread_key", sa.Text(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("agent_id", "thread_key", name="uq_attachment_catalog_thread"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_attachment_catalog_expiry",
        "attachment_catalogs",
        ["agent_id", "expires_at"],
        schema=SCHEMA,
    )
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
        sa.Column("sha256", sa.Text(), nullable=True),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
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
            name="uq_attachment_references_event_file",
            postgresql_nulls_not_distinct=True,
        ),
        sa.UniqueConstraint(
            "agent_id",
            "binding_scope",
            "thread_key",
            "disk_name",
            name="uq_attachment_references_disk_name",
            postgresql_nulls_not_distinct=True,
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_attachment_references_thread_seq",
        TABLE,
        ["agent_id", "binding_scope", "thread_key", "seq"],
        schema=SCHEMA,
    )
    op.create_index("ix_attachment_references_expires_at", TABLE, ["expires_at"], schema=SCHEMA)
    # The orphan sweep runs on every transcript write, scoped to one agent.
    op.create_index(
        "ix_attachment_references_agent_expires_at",
        TABLE,
        ["agent_id", "expires_at"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_table("attachment_catalogs", schema=SCHEMA)
    op.drop_index("ix_attachment_references_agent_expires_at", table_name=TABLE, schema=SCHEMA)
    op.drop_index("ix_attachment_references_expires_at", table_name=TABLE, schema=SCHEMA)
    op.drop_index("ix_attachment_references_thread_seq", table_name=TABLE, schema=SCHEMA)
    op.drop_table(TABLE, schema=SCHEMA)
