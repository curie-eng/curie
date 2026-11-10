"""Remediation nominations and their per-event submissions (AUTOMATED-REMEDIATION-6, -7)

Revision ID: 0088
Revises: 0087
Create Date: 2026-10-07

Hand-written and additive (ADR 0117 found autogenerate unsafe against the
shared database).

``remediation_nomination_submissions`` holds the one accepted submission of a
protected event: keyed on ``event_id`` so the first accepted submission wins,
with the agent and hook resolved from the protected binding and the SHA-256 of
the submitted block's bytes, which decides a byte-identical replay from a
``nomination_conflict``.

``remediation_nominations`` holds one row per entry of that submission (one row
for a malformed block) with the AUTOMATED-REMEDIATION-7 columns. ``state`` and
``refusal_code`` are closed sets (``curie_api.remediation_codes``), a row is
``refused`` exactly when it carries a refusal code, and only a malformed block's
row lacks an action. Both tables cascade with the agent; a nomination cascades
with its submission.

The downgrade drops both tables. Nothing else reads them yet, and with them gone
no nomination exists, which is the closed default.

@spec AUTOMATED-REMEDIATION-6 @spec AUTOMATED-REMEDIATION-7
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0088"
down_revision: str | None = "0087"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
SUBMISSIONS = "remediation_nomination_submissions"
NOMINATIONS = "remediation_nominations"

STATE_CHECK = (
    "state IN ('received', 'refused', 'precondition_pending', 'admitted', "
    "'approval_requested', 'approved', 'rejected', 'expired', 'executing', "
    "'verifying', 'finished')"
)
REFUSAL_CHECK = (
    "refusal_code IS NULL OR refusal_code IN ('nomination_malformed', 'unknown_action', "
    "'nomination_duplicate', 'arguments_schema_mismatch', 'agent_stopped')"
)
REFUSED_CHECK = "(state = 'refused') = (refusal_code IS NOT NULL)"
ACTION_CHECK = (
    "refusal_code = 'nomination_malformed' OR "
    "(action IS NOT NULL AND arguments IS NOT NULL AND arguments_sha256 IS NOT NULL)"
)
KIND_CHECK = "kind IS NULL OR kind IN ('remediate', 'prevent', 'tune')"
OUTCOME_CHECK = (
    "verification_outcome IS NULL OR verification_outcome IN "
    "('verified', 'not-recovered', 'verifier-unavailable', 'superseded')"
)
DIGEST_CHECK = "arguments_sha256 IS NULL OR arguments_sha256 ~ '^[0-9a-f]{64}$'"


def upgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-6 @spec AUTOMATED-REMEDIATION-7."""
    op.create_table(
        SUBMISSIONS,
        sa.Column("event_id", sa.String(256), primary_key=True),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("hook", sa.String(63), nullable=False),
        sa.Column("block_sha256", sa.CHAR(64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        sa.CheckConstraint(
            "block_sha256 ~ '^[0-9a-f]{64}$'", name="remediation_nomination_submissions_digest_ck"
        ),
        schema=SCHEMA,
    )
    op.create_table(
        NOMINATIONS,
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("hook", sa.String(63), nullable=False),
        sa.Column("event_id", sa.String(256), nullable=False),
        sa.Column("admitted_generation", sa.BigInteger(), nullable=True),
        sa.Column("current_generation", sa.BigInteger(), nullable=True),
        sa.Column("action", sa.Text(), nullable=True),
        sa.Column("kind", sa.Text(), nullable=True),
        sa.Column("arguments", sa.Text(), nullable=True),
        sa.Column("arguments_sha256", sa.CHAR(64), nullable=True),
        sa.Column("target", sa.Text(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("refusal_code", sa.Text(), nullable=True),
        sa.Column("approval_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("execution_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("verification_outcome", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["event_id"], [f"{SCHEMA}.{SUBMISSIONS}.event_id"], ondelete="CASCADE"
        ),
        sa.CheckConstraint(STATE_CHECK, name="remediation_nominations_state_ck"),
        sa.CheckConstraint(REFUSAL_CHECK, name="remediation_nominations_refusal_ck"),
        sa.CheckConstraint(REFUSED_CHECK, name="remediation_nominations_refused_ck"),
        sa.CheckConstraint(ACTION_CHECK, name="remediation_nominations_action_ck"),
        sa.CheckConstraint(KIND_CHECK, name="remediation_nominations_kind_ck"),
        sa.CheckConstraint(OUTCOME_CHECK, name="remediation_nominations_outcome_ck"),
        sa.CheckConstraint(DIGEST_CHECK, name="remediation_nominations_digest_ck"),
        sa.CheckConstraint(
            "admitted_generation IS NULL OR admitted_generation > 0",
            name="remediation_nominations_admitted_ck",
        ),
        sa.CheckConstraint(
            "current_generation IS NULL OR current_generation > 0",
            name="remediation_nominations_current_ck",
        ),
        schema=SCHEMA,
    )
    op.create_index("ix_remediation_nominations_event", NOMINATIONS, ["event_id"], schema=SCHEMA)


def downgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-6 @spec AUTOMATED-REMEDIATION-7."""
    op.drop_index("ix_remediation_nominations_event", table_name=NOMINATIONS, schema=SCHEMA)
    op.drop_table(NOMINATIONS, schema=SCHEMA)
    op.drop_table(SUBMISSIONS, schema=SCHEMA)
