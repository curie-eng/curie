"""Outbox for one Slack DM per rejected personal Claude plan (#5720).

Revision ID: 0183
Revises: 0182

One row per owner and enrollment generation. The lease, attempt, backoff and
acknowledgement columns match factory_slack_posts. The parent is 0182, the
guarded dispatch revision.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0183"
down_revision: str = "0182"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

S = "curie"


def upgrade() -> None:
    op.create_table(
        "factory_grant_notices",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("principal_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("channel_identity", sa.Text(), nullable=True),
        sa.Column("recipient", sa.Text(), nullable=True),
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
        sa.CheckConstraint("generation >= 1", name="factory_grant_notices_generation_ck"),
        sa.CheckConstraint(
            "recipient IS NULL OR channel_identity IS NOT NULL",
            name="factory_grant_notices_recipient_ck",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "principal_id"],
            [f"{S}.principals.tenant_id", f"{S}.principals.id"],
            name="factory_grant_notices_principal_fkey",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "principal_id",
            "generation",
            name="factory_grant_notices_principal_generation_key",
        ),
        schema=S,
    )
    op.create_index(
        "ix_factory_grant_notices_owed",
        "factory_grant_notices",
        ["created_at", "id"],
        schema=S,
        postgresql_where=sa.text("posted_at IS NULL AND failed_at IS NULL"),
    )


def downgrade() -> None:
    count = op.get_bind().scalar(sa.text("SELECT count(*) FROM curie.factory_grant_notices"))
    if count is None or count > 0:
        raise RuntimeError("cannot downgrade while factory grant notices exist")
    op.drop_index(
        "ix_factory_grant_notices_owed",
        table_name="factory_grant_notices",
        schema=S,
    )
    op.drop_table("factory_grant_notices", schema=S)
