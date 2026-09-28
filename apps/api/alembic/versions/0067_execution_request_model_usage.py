"""Per-model token usage and estimated cost per execution request (#3223).

Adds ``curie.execution_request_model_usage``: one row per (request, turn,
model, role) as the runner reports it at turn end. The cost estimate, its price
source, and its price time are all NULL or all set, so an unpriced model keeps
its tokens with no estimate. Rows go with their request.

Revision ID: 0067
Revises: 0066
Create Date: 2026-09-26
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0067"
down_revision: str | None = "0066"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "execution_request_model_usage"


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column(
            "execution_request_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{SCHEMA}.execution_requests.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("turn_id", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column("input_tokens", sa.BigInteger(), nullable=False),
        sa.Column("cached_input_tokens", sa.BigInteger(), nullable=False),
        sa.Column("cache_write_tokens", sa.BigInteger(), nullable=False),
        sa.Column("output_tokens", sa.BigInteger(), nullable=False),
        sa.Column("estimated_cost_usd", sa.Numeric(14, 6), nullable=True),
        sa.Column("price_source", sa.Text(), nullable=True),
        sa.Column("price_as_of", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "role IN ('implementer', 'reviewer')",
            name="execution_request_model_usage_role_ck",
        ),
        sa.CheckConstraint(
            "input_tokens >= 0 AND cached_input_tokens >= 0 "
            "AND cache_write_tokens >= 0 AND output_tokens >= 0",
            name="execution_request_model_usage_tokens_ck",
        ),
        sa.CheckConstraint(
            "(estimated_cost_usd IS NULL AND price_source IS NULL AND price_as_of IS NULL) "
            "OR (estimated_cost_usd IS NOT NULL AND estimated_cost_usd >= 0 "
            "AND price_source IS NOT NULL AND price_as_of IS NOT NULL)",
            name="execution_request_model_usage_price_ck",
        ),
        sa.UniqueConstraint(
            "execution_request_id",
            "turn_id",
            "model",
            "role",
            name="execution_request_model_usage_turn_model_role_key",
        ),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_table(TABLE, schema=SCHEMA)
