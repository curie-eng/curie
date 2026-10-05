"""@spec PROTECTED-HOOK-SOURCE-1.

Revision ID: 0075
Revises: 0073
Create Date: 2026-10-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0075"
down_revision: str | None = "0073"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "hook_source_policies"


def upgrade() -> None:
    """@spec PROTECTED-HOOK-SOURCE-1."""
    op.create_table(
        TABLE,
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("hook", sa.String(63), primary_key=True),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("operation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("mode", sa.String(), nullable=False),
        sa.Column("tool_access", sa.String(), nullable=True),
        sa.Column("runtime_id", sa.String(), nullable=True),
        sa.Column("qualification_id", sa.String(), nullable=True),
        sa.Column("bundle_digest", sa.String(), nullable=True),
        sa.Column("legacy_generation", sa.BigInteger(), nullable=False),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        sa.CheckConstraint("generation > 0", name="hook_source_policies_generation_ck"),
        sa.CheckConstraint(
            "mode IN ('protected', 'ordinary')", name="hook_source_policies_mode_ck"
        ),
        sa.CheckConstraint(
            "(mode = 'protected' AND tool_access IS NOT NULL "
            "AND tool_access = 'read-only' AND runtime_id IS NOT NULL "
            "AND qualification_id IS NOT NULL AND bundle_digest IS NOT NULL) "
            "OR (mode = 'ordinary' AND tool_access IS NULL AND runtime_id IS NULL "
            "AND qualification_id IS NULL AND bundle_digest IS NULL)",
            name="hook_source_policies_policy_ck",
        ),
        schema=SCHEMA,
    )


def downgrade() -> None:
    """@spec PROTECTED-HOOK-SOURCE-1."""
    op.drop_table(TABLE, schema=SCHEMA)
