"""Add a per-agent runner step cap (#4175).

Adds nullable ``agents.max_turns``. NULL keeps the installation default (the
runner's CURIE_MAX_TURNS from ``agentSandbox.runner.extraEnv``, else the
runner's own default); a set value is 1..1000 and the worker forwards it as
CURIE_MAX_TURNS in that agent's sandbox claim. Additive: the downgrade drops
the check and the column, which returns every agent to the installation
default.

Revision ID: 0099
Revises: 0098
Create Date: 2026-10-06
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0099"
down_revision: str | None = "0098"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
AGENT_CK = "agents_max_turns_ck"


def upgrade() -> None:
    op.add_column(
        "agents",
        sa.Column("max_turns", sa.Integer(), nullable=True),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        AGENT_CK,
        "agents",
        "max_turns IS NULL OR max_turns BETWEEN 1 AND 1000",
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_constraint(AGENT_CK, "agents", schema=SCHEMA, type_="check")
    op.drop_column("agents", "max_turns", schema=SCHEMA)
