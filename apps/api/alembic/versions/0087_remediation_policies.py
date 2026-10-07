"""The remediation policy store (AUTOMATED-REMEDIATION-2, ADR 0203)

Revision ID: 0087
Revises: 0086
Create Date: 2026-10-07

Hand-written and additive (ADR 0117 found autogenerate unsafe against the
shared database). ``remediation_policies`` holds one row per bound hook with
its current, always positive generation. ``remediation_policy_generations``
holds one immutable row per generation: the operation that wrote it, the
canonical intent digest, the whole policy document, the armed and active flags
and the operator principal that wrote it (``bound_by``). A trigger refuses any
update of a generation row, and its deletion while the agent exists, like the
one on ``hook_source_operations``; deleting the agent cascades both tables.

The downgrade drops both tables and the trigger function. Nothing else reads
them yet, and with them gone no policy is bound, which is the closed default.

@spec AUTOMATED-REMEDIATION-2
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0087"
down_revision: str | None = "0086"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
POLICIES = "remediation_policies"
GENERATIONS = "remediation_policy_generations"


def upgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-2."""
    op.create_table(
        POLICIES,
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("hook", sa.String(63), primary_key=True),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("operation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("armed", sa.Boolean(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        sa.CheckConstraint("generation > 0", name="remediation_policies_generation_ck"),
        sa.CheckConstraint("active OR NOT armed", name="remediation_policies_armed_ck"),
        schema=SCHEMA,
    )
    op.create_table(
        GENERATIONS,
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("hook", sa.String(63), primary_key=True),
        sa.Column("generation", sa.BigInteger(), primary_key=True),
        sa.Column("operation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("intent_sha256", sa.CHAR(64), nullable=False),
        sa.Column("document", postgresql.JSONB(), nullable=False),
        sa.Column("armed", sa.Boolean(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("bound_by", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        sa.CheckConstraint("generation > 0", name="remediation_policy_generations_generation_ck"),
        sa.CheckConstraint(
            "intent_sha256 ~ '^[0-9a-f]{64}$'", name="remediation_policy_generations_intent_ck"
        ),
        sa.CheckConstraint("active OR NOT armed", name="remediation_policy_generations_armed_ck"),
        sa.CheckConstraint(
            "length(btrim(bound_by)) > 0", name="remediation_policy_generations_bound_by_ck"
        ),
        sa.UniqueConstraint(
            "agent_id", "hook", "operation_id", name="uq_remediation_policy_generation_operation"
        ),
        schema=SCHEMA,
    )
    op.execute("""
        -- @spec AUTOMATED-REMEDIATION-2
        CREATE FUNCTION curie.enforce_remediation_policy_generations_immutable()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                IF EXISTS (SELECT 1 FROM curie.agents WHERE id = OLD.agent_id) THEN
                    RAISE EXCEPTION USING ERRCODE = '23514',
                        MESSAGE = 'remediation policy generations cannot be deleted';
                END IF;
                RETURN OLD;
            END IF;
            RAISE EXCEPTION USING ERRCODE = '23514',
                MESSAGE = 'remediation policy generations are immutable';
        END;
        $$
    """)
    op.execute("""
        -- @spec AUTOMATED-REMEDIATION-2
        CREATE TRIGGER remediation_policy_generations_immutable
        BEFORE UPDATE OR DELETE ON curie.remediation_policy_generations
        FOR EACH ROW EXECUTE FUNCTION curie.enforce_remediation_policy_generations_immutable()
    """)


def downgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-2."""
    op.execute(
        "DROP TRIGGER remediation_policy_generations_immutable "
        "ON curie.remediation_policy_generations"
    )
    op.execute("DROP FUNCTION curie.enforce_remediation_policy_generations_immutable()")
    op.drop_table(GENERATIONS, schema=SCHEMA)
    op.drop_table(POLICIES, schema=SCHEMA)
