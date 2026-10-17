"""Pin generic input compatibility declarations and admitted receipts.

Revision ID: 0109
Revises: 0108
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0109"
down_revision = "0108"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "agent_versions",
        sa.Column("input_contracts", postgresql.JSONB(), nullable=True),
        schema="curie",
    )
    op.add_column(
        "execution_requests",
        sa.Column("input_contract_receipt", postgresql.JSONB(), nullable=True),
        schema="curie",
    )


def downgrade() -> None:
    op.drop_column("execution_requests", "input_contract_receipt", schema="curie")
    op.drop_column("agent_versions", "input_contracts", schema="curie")
