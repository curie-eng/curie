"""Remediation receipts (AUTOMATED-REMEDIATION-20)

Revision ID: 0097
Revises: 0096
Create Date: 2026-10-07

Hand-written and additive (ADR 0117 found autogenerate unsafe against the
shared database).

The worker remediation loop posts one thread message per nomination decision
and verification outcome. Two additions carry that:

* ``remediation_delivery_surfaces.reply_conversation``, nullable: the thread
  the receipts post into, recorded by the protected hook ingress beside the
  reply surface it already records. NULL for a surface recorded before this
  revision; the receipt loop then falls back to the submission's conversation.
* ``remediation_receipt_posts``, one row per (nomination, stage) that the loop
  has claimed: the lease, the attempt count and, once the message is out,
  ``posted_at``. The primary key is what makes each stage post once across
  passes, leases and worker replicas. It cascades with its nomination and holds
  no message text, argument or reason.

The downgrade drops the table and the column.

@spec AUTOMATED-REMEDIATION-20
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0097"
down_revision: str | None = "0096"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
SURFACES = "remediation_delivery_surfaces"
NOMINATIONS = "remediation_nominations"
POSTS = "remediation_receipt_posts"
STAGES = (
    "'nominated', 'refused', 'approval_requested', 'executed', 'verified', "
    "'not-recovered', 'verifier-unavailable', 'superseded', 'undo_requested', "
    "'undone', 'escalated'"
)


def upgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-20."""
    op.add_column(
        SURFACES, sa.Column("reply_conversation", sa.Text(), nullable=True), schema=SCHEMA
    )
    op.create_table(
        POSTS,
        sa.Column("nomination_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("stage", sa.Text(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("lease_owner", sa.Text(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("posted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dead_lettered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("nomination_id", "stage", name="pk_remediation_receipt_posts"),
        sa.ForeignKeyConstraint(
            ["nomination_id"], [f"{SCHEMA}.{NOMINATIONS}.id"], ondelete="CASCADE"
        ),
        sa.CheckConstraint(f"stage IN ({STAGES})", name="remediation_receipt_posts_stage_ck"),
        sa.CheckConstraint("attempts >= 0", name="remediation_receipt_posts_attempts_ck"),
        schema=SCHEMA,
    )


def downgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-20."""
    op.drop_table(POSTS, schema=SCHEMA)
    op.drop_column(SURFACES, "reply_conversation", schema=SCHEMA)
