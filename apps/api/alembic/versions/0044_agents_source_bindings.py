"""agents.source_bindings and thread_workspaces.revision (#2572)

Operator-controlled workload-to-allowlisted-repository mapping for one
Alertmanager hook source. The map lives on the agent row, like
hook_partitions: the sender supplies workload values only through a field the
operator named, and the coding target is never a URL inside the payload.

thread_workspaces.revision records the deployed source identity the map named.
NULL is every pre-existing selection: those threads had no recorded revision.

Revision ID: 0044
Revises: 0043
Create Date: 2026-09-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0044"
down_revision: str | None = "0043"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.add_column(
        "agents",
        sa.Column(
            "source_bindings",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "thread_workspaces",
        sa.Column("revision", sa.Text(), nullable=True),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_column("thread_workspaces", "revision", schema=SCHEMA)
    op.drop_column("agents", "source_bindings", schema=SCHEMA)
