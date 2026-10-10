"""Remediation approval requests (AUTOMATED-REMEDIATION-15)

Revision ID: 0092a
Revises: 0091a
Create Date: 2026-10-07

Hand-written and additive (ADR 0117 found autogenerate unsafe against the
shared database).

``approvals_purpose_ck`` is replaced in the same revision so ``purpose`` gains
``remediation``: a well-formed nomination that is not admitted raises one
argument-bound approval of that purpose. Existing ``session`` and
``publication`` rows are untouched.

``remediation_approval_requests`` holds one row per remediation approval: the
nomination that raised it, the deduplication fields (agent, hook, action and
``arguments_sha256``), the admission check that failed and the precondition
read's observed value the card names, the count and newest ``event_id`` of the
identical nominations that attached to it while it was pending, and the card
delivery outbox the worker's remediation card loop leases. It cascades with
its approval, its nomination and the agent.

The downgrade drops the table, deletes the remediation approvals (clearing the
nominations' pointer to them) and restores the purpose check without
``remediation``.

@spec AUTOMATED-REMEDIATION-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0092a"
down_revision: str | None = "0091a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
APPROVALS = "approvals"
REQUESTS = "remediation_approval_requests"
PURPOSE_CHECK = "approvals_purpose_ck"

PURPOSES_WITH_REMEDIATION = "purpose IN ('session', 'publication', 'remediation')"
PURPOSES_WITHOUT_REMEDIATION = "purpose IN ('session', 'publication')"


def upgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-15."""
    op.drop_constraint(PURPOSE_CHECK, APPROVALS, type_="check", schema=SCHEMA)
    op.create_check_constraint(PURPOSE_CHECK, APPROVALS, PURPOSES_WITH_REMEDIATION, schema=SCHEMA)
    op.create_table(
        REQUESTS,
        sa.Column("approval_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("nomination_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("hook", sa.String(63), nullable=False),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("arguments_sha256", sa.CHAR(64), nullable=False),
        sa.Column("failed_check", sa.Text(), nullable=False),
        sa.Column("observed", postgresql.JSONB(), nullable=True),
        sa.Column("attached_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("newest_event_id", sa.String(256), nullable=True),
        sa.Column("card_attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("card_version", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("card_lease_owner", sa.Text(), nullable=True),
        sa.Column("card_lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("card_posted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("card_dead_lettered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("card_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["approval_id"], [f"{SCHEMA}.{APPROVALS}.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["nomination_id"], [f"{SCHEMA}.remediation_nominations.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("nomination_id", name="remediation_approval_requests_nomination_key"),
        sa.CheckConstraint(
            "arguments_sha256 ~ '^[0-9a-f]{64}$'",
            name="remediation_approval_requests_digest_ck",
        ),
        sa.CheckConstraint(
            "attached_count >= 0 AND card_attempts >= 0",
            name="remediation_approval_requests_counts_ck",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_remediation_approval_requests_identity",
        REQUESTS,
        ["agent_id", "hook", "action", "arguments_sha256"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-15."""
    op.drop_index(
        "ix_remediation_approval_requests_identity", table_name=REQUESTS, schema=SCHEMA
    )
    op.drop_table(REQUESTS, schema=SCHEMA)
    op.execute(
        f"UPDATE {SCHEMA}.remediation_nominations SET approval_id = NULL "
        f"WHERE approval_id IN (SELECT id FROM {SCHEMA}.{APPROVALS} "
        "WHERE purpose = 'remediation')"
    )
    op.execute(f"DELETE FROM {SCHEMA}.{APPROVALS} WHERE purpose = 'remediation'")
    op.drop_constraint(PURPOSE_CHECK, APPROVALS, type_="check", schema=SCHEMA)
    op.create_check_constraint(
        PURPOSE_CHECK, APPROVALS, PURPOSES_WITHOUT_REMEDIATION, schema=SCHEMA
    )
