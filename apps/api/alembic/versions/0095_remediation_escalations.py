"""Remediation escalations (AUTOMATED-REMEDIATION-19)

Revision ID: 0095
Revises: 0094
Create Date: 2026-10-07

Hand-written and additive (ADR 0117 found autogenerate unsafe against the
shared database).

``remediation_escalations`` holds one row per nomination whose verification
outcome is anything other than ``verified``: the failure report the worker
remediation loop delivers to the policy's route and the delivery's thread. It
names the agent, the nomination (unique: an outcome is written once, so it is
escalated once), the ledger record when the forward left one, the outcome, and
the undo approval when one was offered (a ``reversible`` action whose record is
undoable). It cascades with the agent; the undo approval reference is cleared
if that approval row is ever deleted. The undo approval itself is an existing
``approvals`` row of purpose ``remediation`` (no model wake), told apart from a
forward remediation approval by this table and its ``remediation-undo:`` dedupe
key, so the purpose check is unchanged.

The downgrade deletes the undo approvals this table names, then drops it.

@spec AUTOMATED-REMEDIATION-19
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0095"
down_revision: str | None = "0094"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
APPROVALS = "approvals"
ESCALATIONS = "remediation_escalations"


def upgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-19."""
    op.create_table(
        ESCALATIONS,
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("nomination_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("action_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("undo_approval_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["undo_approval_id"], [f"{SCHEMA}.{APPROVALS}.id"], ondelete="SET NULL"
        ),
        sa.UniqueConstraint("nomination_id", name="uq_remediation_escalations_nomination"),
        sa.UniqueConstraint("undo_approval_id", name="uq_remediation_escalations_undo_approval"),
        sa.CheckConstraint(
            "outcome IN ('not-recovered', 'verifier-unavailable', 'superseded')",
            name="remediation_escalations_outcome_ck",
        ),
        sa.CheckConstraint(
            "undo_approval_id IS NULL OR action_id IS NOT NULL",
            name="remediation_escalations_undo_ck",
        ),
        schema=SCHEMA,
    )
    op.create_index("ix_remediation_escalations_agent", ESCALATIONS, ["agent_id"], schema=SCHEMA)


def downgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-19."""
    op.execute(
        f"DELETE FROM {SCHEMA}.{APPROVALS} WHERE id IN "
        f"(SELECT undo_approval_id FROM {SCHEMA}.{ESCALATIONS} "
        "WHERE undo_approval_id IS NOT NULL)"
    )
    op.drop_index("ix_remediation_escalations_agent", table_name=ESCALATIONS, schema=SCHEMA)
    op.drop_table(ESCALATIONS, schema=SCHEMA)
