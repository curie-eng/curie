"""One owed issue comment per non-PR factory terminus.

Revision ID: 0049
Revises: 0048
Create Date: 2026-09-22
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0049"
down_revision: str | None = "0048"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.create_table(
        "factory_terminal_notices",
        sa.Column("execution_request_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("work_item_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("terminal_cause", sa.Text(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("scan_page", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("posted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("comment_id", sa.BigInteger(), nullable=True),
        sa.Column("refused_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refusal", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "length(btrim(terminal_cause)) > 0",
            name="factory_terminal_notices_cause_ck",
        ),
        sa.CheckConstraint(
            "attempts >= 0",
            name="factory_terminal_notices_attempts_ck",
        ),
        sa.CheckConstraint(
            "scan_page >= 1",
            name="factory_terminal_notices_scan_page_ck",
        ),
        sa.CheckConstraint(
            "posted_at IS NULL OR refused_at IS NULL",
            name="factory_terminal_notices_one_outcome_ck",
        ),
        sa.CheckConstraint(
            "(comment_id IS NULL) = (posted_at IS NULL)",
            name="factory_terminal_notices_comment_ck",
        ),
        sa.CheckConstraint(
            "(refusal IS NULL) = (refused_at IS NULL)",
            name="factory_terminal_notices_refusal_ck",
        ),
        sa.ForeignKeyConstraint(
            ["execution_request_id"],
            [f"{SCHEMA}.execution_requests.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["work_item_id"],
            [f"{SCHEMA}.work_items.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("execution_request_id"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_factory_terminal_notices_pending",
        "factory_terminal_notices",
        ["created_at"],
        schema=SCHEMA,
        postgresql_where=sa.text("posted_at IS NULL AND refused_at IS NULL"),
    )
    op.add_column(
        "publications",
        sa.Column("execution_request_id", postgresql.UUID(as_uuid=True), nullable=True),
        schema=SCHEMA,
    )
    op.create_foreign_key(
        "publications_execution_request_id_fkey",
        "publications",
        "execution_requests",
        ["execution_request_id"],
        ["id"],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint(
        "publications_execution_request_id_fkey",
        "publications",
        schema=SCHEMA,
        type_="foreignkey",
    )
    op.drop_column("publications", "execution_request_id", schema=SCHEMA)
    op.drop_index(
        "ix_factory_terminal_notices_pending",
        table_name="factory_terminal_notices",
        schema=SCHEMA,
    )
    op.drop_table("factory_terminal_notices", schema=SCHEMA)
