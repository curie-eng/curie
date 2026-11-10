"""Remediation qualification records and verifier runs (AUTOMATED-REMEDIATION-22)

Revision ID: 0094
Revises: 0093a
Create Date: 2026-10-07

Hand-written and additive (ADR 0117 found autogenerate unsafe against the
shared database).

``remediation_qualifications`` holds one record per qualification id, for one
``(agent_id, connector, tool, connector_digest, verifier_sha256)``: the
reversibility it was qualified for, the operator principal that recorded it,
the worst case statement (at most 2000 characters, held by a check), the
evidence references the API checked by state and digest when it was written,
and the hook, action and policy generation whose bounds it was evaluated
against. A record is never updated; a connector upgrade makes it stale and the
action is requalified under a new id. It cascades with the agent.

``remediation_qualification_verifier_runs`` holds each verifier evaluation an
operator started for a qualification: the hook, action, policy generation and
verifier declaration digest it ran under, the literal target, the operator
principal, ``started_at`` (the samples' anchor) and the outcome once decided
(``verified``, ``not-recovered`` or ``verifier-unavailable``). Its samples are
``read`` executions with ``authority_kind`` ``qualification`` and the run id as
``authority_ref``. It cascades with the agent.

The downgrade drops both tables.

@spec AUTOMATED-REMEDIATION-22
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0094"
down_revision: str | None = "0093a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
RECORDS = "remediation_qualifications"
RUNS = "remediation_qualification_verifier_runs"


def upgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-22."""
    op.create_table(
        RECORDS,
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector", sa.Text(), nullable=False),
        sa.Column("tool", sa.Text(), nullable=False),
        sa.Column("connector_digest", sa.Text(), nullable=False),
        sa.Column("verifier_sha256", sa.String(64), nullable=False),
        sa.Column("reversibility", sa.Text(), nullable=False),
        sa.Column("recorded_by", sa.Text(), nullable=False),
        sa.Column("worst_case", sa.Text(), nullable=False),
        sa.Column("evidence", postgresql.JSONB(), nullable=False),
        sa.Column("hook", sa.String(63), nullable=True),
        sa.Column("action", sa.Text(), nullable=True),
        sa.Column("generation", sa.BigInteger(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        sa.CheckConstraint(
            "char_length(worst_case) BETWEEN 1 AND 2000",
            name="remediation_qualifications_worst_case_ck",
        ),
        sa.CheckConstraint(
            "verifier_sha256 ~ '^[0-9a-f]{64}$'",
            name="remediation_qualifications_verifier_ck",
        ),
        sa.CheckConstraint(
            "reversibility IN ('reversible', 'idempotent')",
            name="remediation_qualifications_reversibility_ck",
        ),
        schema=SCHEMA,
    )
    op.create_index("ix_remediation_qualifications_agent", RECORDS, ["agent_id"], schema=SCHEMA)
    op.create_table(
        RUNS,
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("qualification_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("hook", sa.String(63), nullable=False),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("verifier_sha256", sa.String(64), nullable=False),
        sa.Column("target", postgresql.JSONB(), nullable=False),
        sa.Column("started_by", sa.Text(), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("outcome", sa.Text(), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        sa.CheckConstraint(
            "outcome IS NULL OR outcome IN ('verified', 'not-recovered', 'verifier-unavailable')",
            name="remediation_qualification_verifier_runs_outcome_ck",
        ),
        sa.CheckConstraint(
            "(outcome IS NULL) = (decided_at IS NULL)",
            name="remediation_qualification_verifier_runs_decided_ck",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_remediation_qualification_verifier_runs_qualification",
        RUNS,
        ["agent_id", "qualification_id"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-22."""
    op.drop_index(
        "ix_remediation_qualification_verifier_runs_qualification",
        table_name=RUNS,
        schema=SCHEMA,
    )
    op.drop_table(RUNS, schema=SCHEMA)
    op.drop_index("ix_remediation_qualifications_agent", table_name=RECORDS, schema=SCHEMA)
    op.drop_table(RECORDS, schema=SCHEMA)
