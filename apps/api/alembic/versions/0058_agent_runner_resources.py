"""Per-agent runner resource override (#3209).

Adds nullable ``agents.runner_resources``. NULL means the chart
``agentSandbox.runner.resources`` block. A set value is the requests and
limits block the next sandbox claim copies onto a worker-owned template.
An existing sandbox is not resized.

Revision ID: 0058
Revises: 0057
Create Date: 2026-09-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0058"
down_revision: str | None = "0057"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.add_column(
        "agents",
        sa.Column("runner_resources", postgresql.JSONB(), nullable=True),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_column("agents", "runner_resources", schema=SCHEMA)
