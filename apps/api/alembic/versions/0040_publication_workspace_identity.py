"""Persist private publication workspace identity (#2272).

Revision ID: 0040
Revises: 0039
Create Date: 2026-09-03
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0040"
down_revision: str | None = "0039"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "publications"


def upgrade() -> None:
    # Historical rows do not prove whether their Approval conversation was a
    # bare adapter id or a scoped workaround. NULL preserves that uncertainty
    # and selects the explicit legacy reader lane.
    op.add_column(
        TABLE,
        sa.Column("workspace_conversation_id", sa.String(), nullable=True),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_column(TABLE, "workspace_conversation_id", schema=SCHEMA)
