"""Store a private review capacity notice marker and bounded scan cursor.

Revision ID: 0062
Revises: 0061
Create Date: 2026-09-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0062"
down_revision: str | None = "0061"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "github_review_feedback"
SCAN_PAGE_CHECK = "github_review_feedback_notice_scan_page_ck"


def upgrade() -> None:
    op.add_column(
        TABLE,
        sa.Column("notice_marker", postgresql.UUID(as_uuid=True), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        TABLE,
        sa.Column("notice_scan_page", sa.Integer(), nullable=False, server_default="1"),
        schema=SCHEMA,
    )
    op.create_check_constraint(SCAN_PAGE_CHECK, TABLE, "notice_scan_page >= 1", schema=SCHEMA)


def downgrade() -> None:
    op.drop_constraint(SCAN_PAGE_CHECK, TABLE, schema=SCHEMA, type_="check")
    op.drop_column(TABLE, "notice_scan_page", schema=SCHEMA)
    op.drop_column(TABLE, "notice_marker", schema=SCHEMA)
