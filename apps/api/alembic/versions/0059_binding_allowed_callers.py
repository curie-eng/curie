"""Per-binding caller list (#3241, ADR 0175).

Adds nullable ``agent_channels.allowed_callers``, a JSON array of the exact
caller ids that may start a turn through the binding. NULL is the default and
means everyone, exactly as before this revision, so every existing binding
keeps its behavior. An empty array is refused at the API (422) and here, by a
CHECK, so an out-of-band writer cannot store the value two operators would
read two opposite ways.

Revision ID: 0059
Revises: 0058
Create Date: 2026-09-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0059"
down_revision: str | None = "0058"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
CALLERS_CK = "agent_channels_allowed_callers_ck"


def upgrade() -> None:
    op.add_column(
        "agent_channels",
        sa.Column("allowed_callers", postgresql.JSONB(), nullable=True),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        CALLERS_CK,
        "agent_channels",
        "allowed_callers IS NULL OR ("
        "jsonb_typeof(allowed_callers) = 'array' AND "
        "jsonb_array_length(allowed_callers) BETWEEN 1 AND 100)",
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_constraint(CALLERS_CK, "agent_channels", schema=SCHEMA, type_="check")
    op.drop_column("agent_channels", "allowed_callers", schema=SCHEMA)
