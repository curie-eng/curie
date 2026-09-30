"""Per-agent memory writes switch (#1461, ADR-0167).

Adds ``agents.memory_writes BOOLEAN NOT NULL DEFAULT false``. When true, the
worker hands the runner the binding-scoped channel memory URL and the runner
mounts its remember/update/forget tools. Every existing agent comes out of the
upgrade with it off. Downgrade drops the column.

Revision ID: 0071
Revises: 0070
Create Date: 2026-09-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0071"
down_revision: str | None = "0070"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.add_column(
        "agents",
        sa.Column("memory_writes", sa.Boolean(), server_default=sa.false(), nullable=False),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_column("agents", "memory_writes", schema=SCHEMA)
