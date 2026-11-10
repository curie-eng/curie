"""Factory poll cursors, one row per repository (#3745).

Expand only. Polling stores comment and review ``since`` timestamps and the
ETag of each listing it has applied. ``schema_min`` stays at 0070.

Revision ID: 0073
Revises: 0072
Create Date: 2026-10-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0073"
down_revision: str | None = "0072"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "factory_poll_cursors"


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("repo_full_name", sa.Text(), primary_key=True),
        sa.Column("repository_id", sa.BigInteger(), nullable=True),
        sa.Column("comments_since", sa.DateTime(timezone=True), nullable=True),
        sa.Column("review_comments_since", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reviews_since", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "etags",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "repository_id IS NULL OR repository_id > 0",
            name="factory_poll_cursors_repository_id_ck",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(etags) = 'object'",
            name="factory_poll_cursors_etags_object_ck",
        ),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_table(TABLE, schema=SCHEMA)
