"""Remediation admission: breakers, reservations, the approval reason (AR-8 to AR-11)

Revision ID: 0093
Revises: 0092
Create Date: 2026-10-07

Hand-written and additive (ADR 0117 found autogenerate unsafe against the
shared database).

``remediation_breakers`` holds one row per breaker, keyed by agent, connector,
tool and target key (AUTOMATED-REMEDIATION-11). A breaker opens on any
verification outcome other than ``verified`` and is closed only through the
policy's administrative route, which records the operator principal and a
reason; at most one breaker per key is open at a time. It cascades with the
agent.

``remediation_reservations`` holds the check 11 reservation of each nomination
admitted to its precondition read (AUTOMATED-REMEDIATION-10): its policy,
action, target key and turn, when it was taken and when it was released (the
nomination did not execute). Counts are taken from it under the per-agent
admission lock. It cascades with the nomination and the agent.

``remediation_nominations.approval_reason`` names the admission check that sent
a well-formed nomination to approval (AUTOMATED-REMEDIATION-8), one of the
frozen ``approval_reasons`` of ``tests/vectors/remediation-codes.json``.
Existing rows read NULL.

``remediation_nomination_submissions.conversation_id`` keeps the protected
delivery's conversation (the envelope's ``logical_conversation_key``), which an
approval raised after the binding is gone still names. Existing rows read NULL.

The downgrade drops both tables and both columns.

@spec AUTOMATED-REMEDIATION-8 @spec AUTOMATED-REMEDIATION-10 @spec AUTOMATED-REMEDIATION-11
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0093"
down_revision: str | None = "0092"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
BREAKERS = "remediation_breakers"
RESERVATIONS = "remediation_reservations"
NOMINATIONS = "remediation_nominations"
SUBMISSIONS = "remediation_nomination_submissions"
REASON_CHECK = "remediation_nominations_approval_reason_ck"

# tests/vectors/remediation-codes.json ``approval_reasons``, frozen here.
APPROVAL_REASONS = (
    "generation_not_current",
    "policy_disarmed",
    "not_automatic",
    "qualification_missing",
    "qualification_stale",
    "verifier_not_independent",
    "out_of_bounds",
    "not_reversible_now",
    "breaker_open",
    "policy_rate_limit",
    "action_rate_limit",
    "incident_limit",
    "turn_limit",
    "target_live",
    "precondition_not_met",
    "precondition_unavailable",
    "admission_unreadable",
    "policy_changed",
)


def upgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-8 @spec AUTOMATED-REMEDIATION-10
    @spec AUTOMATED-REMEDIATION-11."""
    op.create_table(
        BREAKERS,
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector", sa.Text(), nullable=False),
        sa.Column("tool", sa.Text(), nullable=False),
        sa.Column("target", sa.Text(), nullable=False),
        sa.Column(
            "opened_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("closed_by", sa.Text(), nullable=True),
        sa.Column("close_reason", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        sa.CheckConstraint(
            "(closed_at IS NULL) = (closed_by IS NULL) "
            "AND (closed_at IS NULL) = (close_reason IS NULL)",
            name="remediation_breakers_closed_ck",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "uq_remediation_breakers_open",
        BREAKERS,
        ["agent_id", "connector", "tool", "target"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("closed_at IS NULL"),
    )
    op.create_table(
        RESERVATIONS,
        sa.Column("nomination_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("hook", sa.String(63), nullable=False),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("target", sa.Text(), nullable=False),
        sa.Column("event_id", sa.String(256), nullable=False),
        sa.Column(
            "reserved_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("released_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["nomination_id"], [f"{SCHEMA}.{NOMINATIONS}.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_remediation_reservations_agent",
        RESERVATIONS,
        ["agent_id", "reserved_at"],
        schema=SCHEMA,
    )
    op.add_column(
        NOMINATIONS, sa.Column("approval_reason", sa.Text(), nullable=True), schema=SCHEMA
    )
    reasons = ", ".join(f"'{reason}'" for reason in APPROVAL_REASONS)
    op.create_check_constraint(
        REASON_CHECK,
        NOMINATIONS,
        f"approval_reason IS NULL OR approval_reason IN ({reasons})",
        schema=SCHEMA,
    )
    op.add_column(
        SUBMISSIONS, sa.Column("conversation_id", sa.Text(), nullable=True), schema=SCHEMA
    )


def downgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-8 @spec AUTOMATED-REMEDIATION-10
    @spec AUTOMATED-REMEDIATION-11."""
    op.drop_column(SUBMISSIONS, "conversation_id", schema=SCHEMA)
    op.drop_constraint(REASON_CHECK, NOMINATIONS, type_="check", schema=SCHEMA)
    op.drop_column(NOMINATIONS, "approval_reason", schema=SCHEMA)
    op.drop_index("ix_remediation_reservations_agent", table_name=RESERVATIONS, schema=SCHEMA)
    op.drop_table(RESERVATIONS, schema=SCHEMA)
    op.drop_index("uq_remediation_breakers_open", table_name=BREAKERS, schema=SCHEMA)
    op.drop_table(BREAKERS, schema=SCHEMA)
