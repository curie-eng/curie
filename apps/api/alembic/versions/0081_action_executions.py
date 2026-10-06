"""Action executions, restore ledger columns and connector capabilities (#4067)

Revision ID: 0081
Revises: 0079
Create Date: 2026-10-06

Hand-written and additive, like every action ledger migration: ADR 0117 found
autogenerate unsafe against the shared database.

* @spec ACTION-EXECUTOR-2: ``action_executions``, one row per restore, forward
  action or capability probe the platform runs without a model. ``kind`` and
  ``state`` are checked, ``idempotency_key`` is unique, and a partial unique
  index allows at most one restore that is not ``refused`` per recorded action.
* @spec ACTION-EXECUTOR-11: ``post_version``, ``connector``,
  ``connector_digest``, ``authority_kind`` and ``authority_ref`` on
  ``agent_actions``. Existing rows are neither migrated nor purged; every new
  column is nullable with no default, so they read back NULL.
* @spec ACTION-EXECUTOR-13: ``connector_capabilities``, keyed on the agent,
  connector and image digest.

The downgrade drops only what the upgrade added. Execution and capability rows
are derived operational state; the ledger rows they name outlive it.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0081"
down_revision: str | None = "0079"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ACTION_COLUMNS = (
    "post_version",
    "connector",
    "connector_digest",
    "authority_kind",
    "authority_ref",
)


def upgrade() -> None:
    for name in _ACTION_COLUMNS:
        op.add_column("agent_actions", sa.Column(name, sa.Text(), nullable=True), schema="curie")

    op.create_table(
        "action_executions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column(
            "agent_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("curie.agents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("connector", sa.Text(), nullable=False),
        sa.Column("tool", sa.Text(), nullable=True),
        sa.Column(
            "subject_action_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("curie.agent_actions.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("arguments_sha256", sa.Text(), nullable=True),
        sa.Column("forward_arguments", postgresql.JSONB(), nullable=True),
        sa.Column("connector_digest", sa.Text(), nullable=False),
        sa.Column("authority_kind", sa.Text(), nullable=False),
        sa.Column("authority_ref", sa.Text(), nullable=False),
        sa.Column("requested_by", sa.Text(), nullable=True),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), server_default="requested", nullable=False),
        sa.Column("refusal_code", sa.Text(), nullable=True),
        sa.Column("failure_code", sa.Text(), nullable=True),
        sa.Column("attempt", sa.Integer(), server_default="0", nullable=False),
        sa.Column("lease_owner", sa.Text(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dispatched_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("outcome", postgresql.JSONB(), nullable=True),
        sa.CheckConstraint(
            "kind IN ('restore', 'forward', 'probe')", name="action_executions_kind_ck"
        ),
        sa.CheckConstraint(
            "state IN ('requested', 'claimed', 'dispatched', 'confirmed', 'failed', "
            "'indeterminate', 'refused')",
            name="action_executions_state_ck",
        ),
        sa.UniqueConstraint("idempotency_key", name="uq_action_executions_idempotency_key"),
        schema="curie",
    )
    op.create_index(
        "ix_action_executions_agent_id", "action_executions", ["agent_id"], schema="curie"
    )
    op.create_index(
        "ix_action_executions_state", "action_executions", ["state"], schema="curie"
    )
    op.create_index(
        "uq_action_executions_live_restore",
        "action_executions",
        ["subject_action_id"],
        unique=True,
        schema="curie",
        postgresql_where=sa.text("kind = 'restore' AND state <> 'refused'"),
    )

    op.create_table(
        "connector_capabilities",
        sa.Column(
            "agent_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("curie.agents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("connector", sa.Text(), nullable=False),
        sa.Column("digest", sa.Text(), nullable=False),
        sa.Column("restore_capable", sa.Boolean(), nullable=False),
        sa.Column(
            "observed_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        # One answer per image: "the row is keyed on the digest".
        sa.PrimaryKeyConstraint("agent_id", "connector", "digest"),
        schema="curie",
    )


def downgrade() -> None:
    op.drop_table("connector_capabilities", schema="curie")
    op.drop_index(
        "uq_action_executions_live_restore", table_name="action_executions", schema="curie"
    )
    op.drop_index("ix_action_executions_state", table_name="action_executions", schema="curie")
    op.drop_index("ix_action_executions_agent_id", table_name="action_executions", schema="curie")
    op.drop_table("action_executions", schema="curie")
    for name in reversed(_ACTION_COLUMNS):
        op.drop_column("agent_actions", name, schema="curie")
